import types

import pytest

import mainsequence.client.models_tdag as models_tdag

PROJECT_UID = "1d0530c0-65d1-4db0-856b-dc29d8260a09"
DATA_SOURCE_UID = "864e7c22-482a-464a-8758-0d3408abd77f"
ORGANIZATION_UID = "56db6c13-235f-4ea4-adb7-f21fd9cebc67"


@pytest.fixture(autouse=True)
def _reset_pod_project_resolution_cache():
    models_tdag._reset_local_pod_project_resolution_cache()
    yield
    models_tdag._reset_local_pod_project_resolution_cache()


def _project_payload_public() -> dict:
    return {
        "uid": PROJECT_UID,
        "project_name": "Markets Repository",
        "data_source": {
            "uid": DATA_SOURCE_UID,
            "related_resource": {
                "uid": DATA_SOURCE_UID,
                "data_source_uid": DATA_SOURCE_UID,
                "display_name": "Remote Timescale",
                "organization_uid": ORGANIZATION_UID,
                "class_type": "timescale_db_remote",
                "status": "AVAILABLE",
            },
            "related_resource_class_type": "timescale_db_remote",
        },
        "git_ssh_url": "git@github.com:mainsequence/markets-repository.git",
        "is_initialized": True,
        "created_by": "user-4",
        "labels": ["markets"],
    }


def test_project_deserializes_public_uid_serializer_payload():
    project = models_tdag.Project(**_project_payload_public())

    assert project.uid == PROJECT_UID
    assert project.created_by == "user-4"
    assert project.data_source is not None
    assert project.data_source.uid == DATA_SOURCE_UID
    assert project.data_source.related_resource.uid == DATA_SOURCE_UID
    assert project.data_source.related_resource.data_source_uid == DATA_SOURCE_UID
    assert project.data_source.related_resource.organization_uid == ORGANIZATION_UID
    assert project.data_source.related_resource.class_type == "timescale_db_remote"


def test_data_node_update_get_or_create_uses_current_project_uid(monkeypatch):
    monkeypatch.setenv("MAIN_SEQUENCE_PROJECT_UID", PROJECT_UID)
    monkeypatch.delenv("MAIN_SEQUENCE_PROJECT_ID", raising=False)

    captured = {}

    monkeypatch.setattr(
        models_tdag.Project,
        "get",
        lambda *args, **kwargs: types.SimpleNamespace(uid=PROJECT_UID, data_source=None),
    )
    monkeypatch.setattr(
        models_tdag.DataNodeUpdate,
        "build_session",
        classmethod(lambda cls: types.SimpleNamespace(headers={})),
    )

    class _Response:
        status_code = 201

        @staticmethod
        def json():
            return {
                "uid": "update-uid-1",
                "update_hash": "abc123",
                "build_configuration": {},
                "orm_class": "DataNodeUpdate",
                "data_node_storage": "storage-1",
                "tags": [],
                "labels": [],
                "description": None,
                "update_details": None,
                "run_configuration": None,
                "open_for_everyone": False,
            }

    def _make_request(**kwargs):
        captured.update(kwargs)
        return _Response()

    monkeypatch.setattr(models_tdag, "make_request", _make_request)

    models_tdag.DataNodeUpdate.get_or_create(update_hash="abc123")

    payload = captured["payload"]["json"]
    assert payload["current_project_uid"] == PROJECT_UID
    assert "current_project_id" not in payload


def test_resolve_local_pod_project_uses_uid_lookup_and_caches(monkeypatch):
    monkeypatch.setenv("MAIN_SEQUENCE_PROJECT_UID", PROJECT_UID)
    monkeypatch.delenv("MAIN_SEQUENCE_PROJECT_ID", raising=False)

    calls = []
    project = types.SimpleNamespace(uid=PROJECT_UID, data_source=None)

    def _project_get(*args, **kwargs):
        calls.append(kwargs)
        return project

    monkeypatch.setattr(models_tdag.Project, "get", _project_get)

    resolution_first = models_tdag._resolve_local_pod_project()
    resolution_second = models_tdag._resolve_local_pod_project()

    assert resolution_first.status == "resolved"
    assert resolution_first.project is project
    assert resolution_second.project is project
    assert calls == [{"pk": PROJECT_UID}]


def test_set_remote_db_warns_once_for_lookup_failure(monkeypatch):
    monkeypatch.setenv("MAIN_SEQUENCE_PROJECT_UID", PROJECT_UID)
    monkeypatch.delenv("MAIN_SEQUENCE_PROJECT_ID", raising=False)

    warnings = []
    debugs = []

    monkeypatch.setattr(models_tdag.logger, "warning", lambda message: warnings.append(message))
    monkeypatch.setattr(models_tdag.logger, "debug", lambda message: debugs.append(message))

    def _project_get(*args, **kwargs):
        raise RuntimeError("contract mismatch")

    monkeypatch.setattr(models_tdag.Project, "get", _project_get)

    pod_data_source = models_tdag.PodDataSource()

    assert pod_data_source.set_remote_db() is None
    assert pod_data_source.set_remote_db() is None

    assert len(warnings) == 1
    assert f"Could not resolve project reference {PROJECT_UID!r}" in warnings[0]
    assert "contract mismatch" in warnings[0]
    assert "Continuing without local pod project attachment." in warnings[0]
    assert debugs == []


def test_data_node_update_get_or_create_requires_local_pod_project(monkeypatch):
    monkeypatch.delenv("MAIN_SEQUENCE_PROJECT_UID", raising=False)
    monkeypatch.delenv("MAIN_SEQUENCE_PROJECT_ID", raising=False)
    monkeypatch.setattr(
        models_tdag.logger,
        "debug",
        lambda message: None,
    )
    monkeypatch.setattr(
        models_tdag.logger,
        "warning",
        lambda message: None,
    )
    monkeypatch.setattr(
        models_tdag,
        "make_request",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("make_request should not run")),
    )

    with pytest.raises(RuntimeError) as exc_info:
        models_tdag.DataNodeUpdate.get_or_create(update_hash="abc123")

    message = str(exc_info.value)
    assert "DataNodeUpdate.get_or_create requires a local pod project." in message
    assert "MAIN_SEQUENCE_PROJECT_UID is not configured." in message
