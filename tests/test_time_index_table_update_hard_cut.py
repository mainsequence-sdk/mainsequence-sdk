from __future__ import annotations

import importlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

import mainsequence.client as client
import mainsequence.client.metatables as client_metatables
import mainsequence.meta_tables as meta_tables
from mainsequence.client.base import BaseObjectOrm
from mainsequence.meta_tables import TimeIndexTableRef
from mainsequence.meta_tables.time_index_table_updates import configuration
from mainsequence.meta_tables.time_index_table_updates.updaters import TimeIndexTableUpdater


def _canonical_build_configuration() -> dict[str, object]:
    return {
        "configuration_schema_version": 2,
        "table_updater_class_import_path": {
            "module": "tests.test_time_index_table_update_hard_cut",
            "qualname": "ExampleUpdater",
        },
    }


@pytest.mark.parametrize(
    "removed_name",
    [
        "APIDataNode",
        "APIPersistManager",
        "DataAccessMixin",
        "DataNode",
        "DataNodeConfiguration",
        "PersistManager",
    ],
)
def test_removed_authoring_names_are_not_public(removed_name: str) -> None:
    assert removed_name not in meta_tables.__all__
    with pytest.raises(AttributeError):
        getattr(meta_tables, removed_name)


@pytest.mark.parametrize(
    "removed_name",
    [
        "DataNodeUpdate",
        "DataNodeUpdateDetails",
        "LocalTimeSeriesHistoricalUpdate",
        "RunConfiguration",
    ],
)
def test_removed_client_model_names_are_not_public(removed_name: str) -> None:
    assert removed_name not in client_metatables.__all__
    with pytest.raises(AttributeError):
        getattr(client_metatables, removed_name)
    with pytest.raises(AttributeError):
        getattr(client, removed_name)


def test_removed_module_namespace_cannot_be_imported() -> None:
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("mainsequence.meta_tables.data_nodes")


def test_only_canonical_update_endpoints_are_registered() -> None:
    assert BaseObjectOrm.END_POINTS["TimeIndexTableUpdate"] == "time-index-table-updates"
    assert (
        BaseObjectOrm.END_POINTS["TimeIndexTableUpdateDetails"] == "time-index-table-update-details"
    )
    assert BaseObjectOrm.END_POINTS["TableUpdateRun"] == "table-update-runs"
    assert not any(
        "local-time-series" in endpoint for endpoint in BaseObjectOrm.END_POINTS.values()
    )


@pytest.mark.parametrize(
    "legacy_fragment",
    [
        {"time_series_class_import_path": "example.Legacy"},
        {"storage_table": "table-uid"},
        {"data_node_storage": "table-uid"},
        {"dependency": {"is_api_time_serie_instance": True}},
        {"dependency": {"kind": "data_node"}},
    ],
)
def test_legacy_build_configuration_is_rejected(legacy_fragment: dict[str, object]) -> None:
    payload = _canonical_build_configuration()
    payload.update(legacy_fragment)

    with pytest.raises(ValueError, match="Legacy time-index table update configuration"):
        configuration.validate_canonical_configuration(payload)


@pytest.mark.parametrize("version", [None, 1, 3])
def test_noncanonical_configuration_versions_are_rejected(version: int | None) -> None:
    payload = _canonical_build_configuration()
    if version is None:
        payload.pop("configuration_schema_version")
    else:
        payload["configuration_schema_version"] = version

    with pytest.raises(ValueError, match="configuration_schema_version=2"):
        configuration.validate_canonical_configuration(payload)


def test_canonical_configuration_hash_is_stable_and_versioned() -> None:
    kwargs = {
        **_canonical_build_configuration(),
        "config": {"identifier": "prices"},
    }
    first = configuration.create_config("ExampleUpdater", kwargs)
    second = configuration.create_config("ExampleUpdater", kwargs)

    assert first.update_hash == second.update_hash
    assert first.local_initial_configuration["configuration_schema_version"] == 2
    assert first.local_initial_configuration["table_updater_class_import_path"] == {
        "module": "tests.test_time_index_table_update_hard_cut",
        "qualname": "ExampleUpdater",
    }


def test_offline_migration_golden_mappings_produce_exact_v2_hashes() -> None:
    fixture_path = (
        Path(__file__).parent / "fixtures" / "time_index_table_update_hash_v2_golden.json"
    )
    fixture = json.loads(fixture_path.read_text())

    assert fixture["configuration_schema_version"] == 2
    assert fixture["cases"]

    for case in fixture["cases"]:
        legacy_configuration = case["legacy_configuration"]
        canonical_configuration = case["canonical_configuration"]

        with pytest.raises(ValueError, match="Legacy time-index table update configuration"):
            configuration.validate_canonical_configuration(legacy_configuration)

        configuration.validate_canonical_configuration(canonical_configuration)
        digest, _ = configuration.hash_signature(canonical_configuration)
        prefix = configuration._crop_hash_prefix(
            case["updater_class_name"].lower(),
        )
        update_hash = f"{prefix}_{digest}"

        assert update_hash == case["expected_update_hash"]


def test_reference_serialization_has_only_canonical_identity() -> None:
    table = client_metatables.TimeIndexMetaTable.model_construct(
        uid="table-uid",
        data_source_uid="data-source-uid",
    )
    payload = configuration.serialize_argument(TimeIndexTableRef(output_table=table))

    assert payload == {
        "kind": "time_index_table_ref",
        "time_index_meta_table_uid": "table-uid",
        "data_source_uid": "data-source-uid",
    }
    assert "update_hash" not in payload
    assert not any(key.startswith("is_api") for key in payload)


def test_reference_has_no_updater_execution_surface() -> None:
    forbidden_names = {
        "run",
        "scheduler",
        "set_relation_tree",
        "update",
        "update_hash",
    }
    assert forbidden_names.isdisjoint(dir(TimeIndexTableRef))


def test_reference_dependency_creates_only_table_lineage() -> None:
    table = client_metatables.TimeIndexMetaTable.model_construct(
        uid="table-uid",
        data_source_uid="data-source-uid",
    )
    table_ref = TimeIndexTableRef(output_table=table)
    calls: list[tuple[str, object | None]] = []
    manager = SimpleNamespace(
        table_update=SimpleNamespace(uid="head-update-uid"),
        is_local_relation_tree_set=lambda: False,
        connect_table_dependency=lambda dependency: calls.append(("table", dependency)),
        connect_update_dependency=lambda dependency: calls.append(("update", dependency)),
        set_ogm_dependencies_linked=lambda: calls.append(("linked", None)),
    )
    updater = SimpleNamespace(
        update_manager=manager,
        dependencies=lambda: {"prices": table_ref},
        logger=SimpleNamespace(debug=lambda *_args, **_kwargs: None),
        depth_df=None,
        dependencies_df=None,
    )

    TimeIndexTableUpdater.set_relation_tree(updater)

    assert calls == [("table", table_ref), ("linked", None)]


def test_client_update_rejects_old_output_field() -> None:
    with pytest.raises(ValidationError, match="data_node_storage"):
        client_metatables.TimeIndexTableUpdate(
            uid="update-uid",
            update_hash="update-hash",
            build_configuration=_canonical_build_configuration(),
            output_table="table-uid",
            data_node_storage="table-uid",
        )


def test_get_or_create_rejects_legacy_request_body_before_network() -> None:
    with pytest.raises(ValueError, match="Legacy time-index table update configuration"):
        client_metatables.TimeIndexTableUpdate.get_or_create(
            update_hash="update-hash",
            data_node_storage_uid="table-uid",
            build_configuration=_canonical_build_configuration(),
        )


def test_get_or_create_requires_complete_canonical_request_body() -> None:
    with pytest.raises(ValueError, match="output_table_uid"):
        client_metatables.TimeIndexTableUpdate.get_or_create(
            update_hash="update-hash",
            build_configuration=_canonical_build_configuration(),
        )


def test_cascade_response_rejects_removed_dynamic_table_fields() -> None:
    with pytest.raises(ValidationError):
        client_metatables.MetaTableCascadeDeleteResponse(
            ok=True,
            action="delete_with_cascade",
            root_meta_table_uid="root-uid",
            deleted_meta_tables=[],
            deleted_dynamic_tables=[],
            deleted_meta_table_count=0,
            deleted_dynamic_table_count=0,
            blocking_edges=[],
        )


def test_project_branch_rejects_removed_update_response_key() -> None:
    branch = SimpleNamespace(
        _get_action=lambda action, timeout=None: {"data_node_updates": []},
    )

    with pytest.raises(ValueError, match="time_index_table_updates or results"):
        client.ProjectBranch.get_time_index_table_updates(branch)


def test_dependency_mutations_use_only_canonical_actions(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, object]] = []

    class FakeResponse:
        status_code = 204
        text = ""

    monkeypatch.setattr(
        client_metatables.TimeIndexTableUpdate,
        "build_session",
        classmethod(lambda cls: object()),
    )
    monkeypatch.setattr(
        client_metatables.TimeIndexTableUpdate,
        "get_object_url",
        classmethod(lambda cls, *_args: "https://backend.test/api/v1/time-index-table-updates"),
    )
    monkeypatch.setattr(
        client_metatables.core,
        "make_request",
        lambda **kwargs: calls.append(kwargs) or FakeResponse(),
    )
    update = client_metatables.TimeIndexTableUpdate(
        uid="head-update-uid",
        update_hash="head-update-hash",
        build_configuration=_canonical_build_configuration(),
        output_table="head-table-uid",
    )

    update.connect_update_dependency("upstream-update-uid")
    update.connect_table_dependency("upstream-table-uid", timeout=20)

    assert [(call["url"], call["payload"]) for call in calls] == [
        (
            "https://backend.test/api/v1/time-index-table-updates/"
            "head-update-uid/update-dependencies/",
            {"json": {"upstream_update_uid": "upstream-update-uid"}},
        ),
        (
            "https://backend.test/api/v1/time-index-table-updates/"
            "head-update-uid/table-dependencies/",
            {"json": {"time_index_meta_table_uid": "upstream-table-uid"}},
        ),
    ]


def test_table_update_run_creation_uses_canonical_collection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    class FakeResponse:
        status_code = 201
        text = ""
        url = "https://backend.test/api/v1/table-update-runs/"

        @staticmethod
        def json() -> dict[str, object]:
            return {
                "uid": "table-update-run-uid",
                "table_update_uid": "table-update-uid",
                "update_time_start": "2026-08-27T00:00:00Z",
            }

    monkeypatch.setattr(
        client_metatables.TimeIndexTableUpdate,
        "build_session",
        classmethod(lambda cls: object()),
    )
    monkeypatch.setattr(
        client_metatables.TimeIndexTableUpdate,
        "get_object_url",
        classmethod(
            lambda cls, model_name=None: (
                "https://backend.test/api/v1/table-update-runs"
                if model_name == "TableUpdateRun"
                else "https://backend.test/api/v1/time-index-table-updates"
            )
        ),
    )

    def _make_request(**kwargs):
        captured.update(kwargs)
        return FakeResponse()

    monkeypatch.setattr(client_metatables.core, "make_request", _make_request)

    result = client_metatables.TimeIndexTableUpdate.create_table_update_run(
        table_update_uid="table-update-uid",
        update_time_start="2026-08-27T00:00:00Z",
    )

    assert isinstance(result, client_metatables.TableUpdateRun)
    assert result.uid == "table-update-run-uid"
    assert captured["url"] == "https://backend.test/api/v1/table-update-runs/"
    assert captured["payload"] == {
        "json": {
            "table_update_uid": "table-update-uid",
            "update_time_start": "2026-08-27T00:00:00Z",
        }
    }
