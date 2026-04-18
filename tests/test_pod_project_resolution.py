import types

import pytest

import mainsequence.client.models_tdag as models_tdag


@pytest.fixture(autouse=True)
def _reset_pod_project_resolution_cache():
    models_tdag._reset_local_pod_project_resolution_cache()
    yield
    models_tdag._reset_local_pod_project_resolution_cache()


def test_resolve_local_pod_project_ignores_invalid_env_without_lookup(monkeypatch):
    monkeypatch.setenv("MAIN_SEQUENCE_PROJECT_ID", "local")

    calls = {"project_get": 0}

    def _project_get(*args, **kwargs):
        calls["project_get"] += 1
        raise AssertionError("Project.get should not be called for an invalid env project id")

    monkeypatch.setattr(models_tdag.Project, "get", _project_get)

    resolution = models_tdag._resolve_local_pod_project()

    assert resolution.status == "invalid"
    assert resolution.project is None
    assert "expected an integer project id" in resolution.detail
    assert calls["project_get"] == 0


def test_set_remote_db_warns_once_for_invalid_env(monkeypatch):
    monkeypatch.setenv("MAIN_SEQUENCE_PROJECT_ID", "local")

    warnings = []
    debugs = []

    monkeypatch.setattr(models_tdag.logger, "warning", lambda message: warnings.append(message))
    monkeypatch.setattr(models_tdag.logger, "debug", lambda message: debugs.append(message))

    pod_data_source = models_tdag.PodDataSource()

    assert pod_data_source.set_remote_db() is None
    assert pod_data_source.set_remote_db() is None

    assert len(warnings) == 1
    assert "MAIN_SEQUENCE_PROJECT_ID='local'" in warnings[0]
    assert "Continuing without local pod project attachment." in warnings[0]
    assert debugs == []


def test_resolve_local_pod_project_uses_pk_lookup_and_caches(monkeypatch):
    monkeypatch.setenv("MAIN_SEQUENCE_PROJECT_ID", "123")

    calls = []
    project = types.SimpleNamespace(id=123, data_source=None)

    def _project_get(*args, **kwargs):
        calls.append(kwargs)
        return project

    monkeypatch.setattr(models_tdag.Project, "get", _project_get)

    resolution_first = models_tdag._resolve_local_pod_project()
    resolution_second = models_tdag._resolve_local_pod_project()

    assert resolution_first.status == "resolved"
    assert resolution_first.project is project
    assert resolution_second.project is project
    assert calls == [{"pk": 123}]


def test_data_node_update_get_or_create_requires_local_pod_project(monkeypatch):
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
    assert "MAIN_SEQUENCE_PROJECT_ID is not configured." in message
