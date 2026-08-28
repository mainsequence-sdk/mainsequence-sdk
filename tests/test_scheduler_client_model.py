import pytest
from pydantic import ValidationError

import mainsequence.client.metatables.core as core_mod

ENVIRONMENT_UID = "58218213-5e4e-43de-a5bd-6757f4e1c8f6"
SCHEDULER_UID = "00000000-0000-4000-8000-000000000001"
UPDATE_NODE_UID = "00000000-0000-4000-8000-000000000002"
OUTPUT_TABLE_UID = "00000000-0000-4000-8000-000000000003"
DATA_SOURCE_UID = "00000000-0000-4000-8000-000000000004"


def _scheduled_node_payload():
    return {
        "uid": UPDATE_NODE_UID,
        "node_type": "time_index_table_update",
        "update_hash": "pricesupdater_0123456789abcdef0123456789abcdef",
        "build_configuration": {
            "configuration_schema_version": 2,
            "table_updater_class_import_path": {
                "module": "tutorial.prices",
                "qualname": "PricesUpdater",
            },
        },
        "output_table_uid": OUTPUT_TABLE_UID,
        "output_table_type": "time_index_meta_table",
        "output_table_physical_table_name": "prices",
        "output_table_identifier": "prices",
        "output_table_data_source_uid": DATA_SOURCE_UID,
    }


def _scheduler_payload():
    return {
        "uid": SCHEDULER_UID,
        "organization_environment_uid": ENVIRONMENT_UID,
        "name": "tutorial",
        "is_running": False,
        "running_process_pid": None,
        "running_in_debug_mode": True,
        "updates_halted": False,
        "host": None,
        "api_address": None,
        "api_port": None,
        "last_heart_beat": None,
        "pre_loads_in_tree": [_scheduled_node_payload()],
        "in_active_tree": [_scheduled_node_payload()],
        "schedules_to": [_scheduled_node_payload()],
    }


class _SchedulerResponse:
    status_code = 200
    text = ""

    @staticmethod
    def json():
        return _scheduler_payload()

    @staticmethod
    def raise_for_status():
        return None


def test_scheduler_strict_model_accepts_environment_projection():
    scheduler = core_mod.Scheduler.model_validate(_scheduler_payload())

    assert scheduler.organization_environment_uid == ENVIRONMENT_UID

    with pytest.raises(ValidationError, match="unexpected_projection"):
        core_mod.Scheduler.model_validate(
            {**_scheduler_payload(), "unexpected_projection": "rejected"}
        )


def test_scheduler_strict_model_accepts_canonical_scheduled_node_projection():
    scheduler = core_mod.Scheduler.model_validate(_scheduler_payload())

    for relation_name in ("pre_loads_in_tree", "in_active_tree", "schedules_to"):
        scheduled_nodes = getattr(scheduler, relation_name)
        assert scheduled_nodes is not None
        assert len(scheduled_nodes) == 1
        scheduled_node = scheduled_nodes[0]
        assert isinstance(scheduled_node, core_mod.ScheduledUpdateNode)
        assert scheduled_node.uid == UPDATE_NODE_UID
        assert scheduled_node.node_type == "time_index_table_update"
        assert scheduled_node.output_table_uid == OUTPUT_TABLE_UID
        assert scheduled_node.output_table_type == "time_index_meta_table"
        assert scheduled_node.output_table_physical_table_name == "prices"
        assert scheduled_node.output_table_identifier == "prices"
        assert scheduled_node.output_table_data_source_uid == DATA_SOURCE_UID


def test_scheduler_strict_model_rejects_unknown_scheduled_node_projection():
    payload = _scheduler_payload()
    payload["schedules_to"][0]["unexpected_node_projection"] = "rejected"

    with pytest.raises(ValidationError, match="unexpected_node_projection"):
        core_mod.Scheduler.model_validate(payload)


def test_scheduler_build_derives_environment_without_request_override(monkeypatch):
    captured = {}

    def _fake_make_request(**kwargs):
        captured.update(kwargs)
        return _SchedulerResponse()

    monkeypatch.setattr(
        core_mod.Scheduler,
        "build_session",
        classmethod(lambda cls: object()),
    )
    monkeypatch.setattr(core_mod, "make_request", _fake_make_request)

    scheduler = core_mod.Scheduler.build_and_assign_to_update_nodes(
        scheduler_name="tutorial",
        update_node_uids=[UPDATE_NODE_UID],
        timeout=11,
    )

    request_body = captured["payload"]["json"]
    assert "organization_environment_uid" not in request_body
    assert "organization_environment_uid" not in request_body["scheduler_kwargs"]
    assert request_body["update_node_uids"] == [UPDATE_NODE_UID]
    assert captured["time_out"] == 11
    assert scheduler.organization_environment_uid == ENVIRONMENT_UID
    assert scheduler.schedules_to is not None
    assert scheduler.schedules_to[0].output_table_uid == OUTPUT_TABLE_UID


def test_scheduler_get_for_update_node_accepts_environment_projection(monkeypatch):
    captured = {}

    def _fake_make_request(**kwargs):
        captured.update(kwargs)
        return _SchedulerResponse()

    monkeypatch.setattr(
        core_mod.Scheduler,
        "build_session",
        classmethod(lambda cls: object()),
    )
    monkeypatch.setattr(core_mod, "make_request", _fake_make_request)

    scheduler = core_mod.Scheduler.get_scheduler_for_update_node(UPDATE_NODE_UID)

    assert captured["r_type"] == "GET"
    assert captured["payload"] == {"params": {"update_node_uid": UPDATE_NODE_UID}}
    assert scheduler.organization_environment_uid == ENVIRONMENT_UID


def test_scheduler_assign_accepts_environment_projection(monkeypatch):
    captured = {}
    scheduler = core_mod.Scheduler.model_validate(_scheduler_payload())

    def _fake_make_request(**kwargs):
        captured.update(kwargs)
        return _SchedulerResponse()

    monkeypatch.setattr(
        core_mod.Scheduler,
        "build_session",
        classmethod(lambda cls: object()),
    )
    monkeypatch.setattr(core_mod, "make_request", _fake_make_request)

    assigned = scheduler.assign_to_scheduler([UPDATE_NODE_UID])

    assert captured["r_type"] == "PATCH"
    assert captured["payload"] == {"json": {"update_node_uids": [UPDATE_NODE_UID]}}
    assert assigned.organization_environment_uid == ENVIRONMENT_UID
