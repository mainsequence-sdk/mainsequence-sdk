import types

import pytest

import mainsequence.client.metatables as models_metatables
import mainsequence.client.models_foundry as models_foundry
from mainsequence.client.exceptions import AuthenticationError
from mainsequence.meta_tables.data_nodes import build_operations

PROJECT_UID = "1d0530c0-65d1-4db0-856b-dc29d8260a09"
PROJECT_BRANCH_UID = "5a28020a-0f1b-47ee-aab8-334286234bea"
DATA_SOURCE_UID = "864e7c22-482a-464a-8758-0d3408abd77f"
ORGANIZATION_UID = "56db6c13-235f-4ea4-adb7-f21fd9cebc67"


@pytest.fixture(autouse=True)
def _reset_pod_project_resolution_cache():
    models_metatables._reset_local_pod_project_resolution_cache()
    yield
    models_metatables._reset_local_pod_project_resolution_cache()


def _project_payload_public() -> dict:
    return {
        "uid": PROJECT_UID,
        "project_name": "Markets Repository",
        "default_metatables_data_source": {
            "uid": DATA_SOURCE_UID,
            "data_source_uid": DATA_SOURCE_UID,
            "display_name": "Remote Timescale",
            "organization_uid": ORGANIZATION_UID,
            "class_type": "timescale_db",
            "status": "AVAILABLE",
            "storage_access_mode": "read_write",
        },
        "default_metatables_data_source_uid": DATA_SOURCE_UID,
        "git_repository_uid": "3c2113e7-40ba-4d8c-ad65-51ca236c3b0c",
        "archived": False,
        "created_by": "user-4",
        "labels": ["markets"],
        "branches": [
            {"uid": PROJECT_BRANCH_UID, "repository_branch": "main"},
        ],
    }


def _project_branch_payload_public() -> dict:
    return {
        "uid": PROJECT_BRANCH_UID,
        "project_uid": PROJECT_UID,
        "project_name": "Markets Repository",
        "repository_branch": "main",
        "project_type": "python",
        "primary_language": "python",
        "framework": "",
        "metatables_data_source": {
            "uid": DATA_SOURCE_UID,
            "data_source_uid": DATA_SOURCE_UID,
            "display_name": "Remote Timescale",
            "organization_uid": ORGANIZATION_UID,
            "class_type": "timescale_db",
            "status": "AVAILABLE",
            "storage_access_mode": "read_write",
        },
        "metatables_data_source_uid": DATA_SOURCE_UID,
        "default_base_image": {
            "uid": "c3ddb792-a3c0-428c-b5cf-31ed99dad10f",
            "title": "Python",
        },
        "sdks": [],
        "git_repository_uid": "3c2113e7-40ba-4d8c-ad65-51ca236c3b0c",
        "latest_git_version": "",
        "is_initialized": True,
        "created_by": "user-4",
    }


def test_project_deserializes_public_uid_serializer_payload():
    project = models_foundry.Project(**_project_payload_public())

    assert project.uid == PROJECT_UID
    assert project.created_by == "user-4"
    assert project.archived is False
    assert project.default_metatables_data_source is not None
    assert project.default_metatables_data_source.uid == DATA_SOURCE_UID
    assert project.default_metatables_data_source_uid == DATA_SOURCE_UID
    assert project.branches[0].uid == PROJECT_BRANCH_UID
    assert project.branches[0].repository_branch == "main"


def test_project_branch_deserializes_public_uid_serializer_payload():
    project_branch = models_foundry.ProjectBranch(**_project_branch_payload_public())

    assert project_branch.uid == PROJECT_BRANCH_UID
    assert project_branch.project_uid == PROJECT_UID
    assert project_branch.metatables_data_source is not None
    assert project_branch.metatables_data_source.uid == DATA_SOURCE_UID
    assert project_branch.metatables_data_source.data_source_uid == DATA_SOURCE_UID
    assert project_branch.metatables_data_source.organization_uid == ORGANIZATION_UID
    assert project_branch.metatables_data_source.class_type == "timescale_db"
    assert project_branch.metatables_data_source.storage_access_mode == "read_write"
    assert project_branch.metatables_data_source.allows_runtime_reads is True
    assert project_branch.metatables_data_source.allows_runtime_writes is True


def test_project_branch_deserializes_nullable_data_source_fields():
    payload = _project_branch_payload_public()
    payload["metatables_data_source"]["storage_access_mode"] = None

    project = models_foundry.ProjectBranch(**payload)

    assert project.metatables_data_source is not None
    assert project.metatables_data_source.storage_access_mode is None
    assert project.metatables_data_source.allows_runtime_reads is False
    assert project.metatables_data_source.allows_runtime_writes is False


def test_project_branch_deserializes_null_metatables_data_source():
    payload = _project_branch_payload_public()
    payload["metatables_data_source"] = None
    payload["metatables_data_source_uid"] = None

    project = models_foundry.ProjectBranch(**payload)

    assert project.metatables_data_source is None


def test_data_node_update_get_or_create_uses_current_project_branch_uid(monkeypatch):
    monkeypatch.setenv("MAIN_SEQUENCE_PROJECT_UID", PROJECT_UID)
    monkeypatch.delenv("MAIN_SEQUENCE_PROJECT_ID", raising=False)
    monkeypatch.setattr(models_metatables, "_current_repository_branch", lambda: "main")

    captured = {}

    monkeypatch.setattr(
        models_foundry.Project,
        "get",
        lambda *args, **kwargs: types.SimpleNamespace(
            uid=PROJECT_UID,
            branches=[types.SimpleNamespace(uid=PROJECT_BRANCH_UID, repository_branch="main")],
        ),
    )
    monkeypatch.setattr(
        models_foundry.ProjectBranch,
        "get",
        lambda *args, **kwargs: types.SimpleNamespace(
            uid=PROJECT_BRANCH_UID,
            metatables_data_source=None,
        ),
    )
    monkeypatch.setattr(
        models_metatables.DataNodeUpdate,
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

    monkeypatch.setattr(models_metatables, "make_request", _make_request)

    models_metatables.DataNodeUpdate.get_or_create(update_hash="abc123")

    payload = captured["payload"]["json"]
    assert payload["current_project_branch_uid"] == PROJECT_BRANCH_UID
    assert "current_project_uid" not in payload
    assert "current_project_id" not in payload


def test_resolve_local_pod_project_uses_uid_lookup_and_caches(monkeypatch):
    monkeypatch.setenv("MAIN_SEQUENCE_PROJECT_UID", PROJECT_UID)
    monkeypatch.delenv("MAIN_SEQUENCE_PROJECT_ID", raising=False)

    monkeypatch.setattr(models_metatables, "_current_repository_branch", lambda: "main")
    project_calls = []
    branch_calls = []
    project_branch = types.SimpleNamespace(uid=PROJECT_BRANCH_UID, metatables_data_source=None)
    project = types.SimpleNamespace(
        uid=PROJECT_UID,
        branches=[types.SimpleNamespace(uid=PROJECT_BRANCH_UID, repository_branch="main")],
    )

    def _project_get(*args, **kwargs):
        project_calls.append(kwargs)
        return project

    def _project_branch_get(*args, **kwargs):
        branch_calls.append(kwargs)
        return project_branch

    monkeypatch.setattr(models_foundry.Project, "get", _project_get)
    monkeypatch.setattr(models_foundry.ProjectBranch, "get", _project_branch_get)

    resolution_first = models_metatables._resolve_local_pod_project()
    resolution_second = models_metatables._resolve_local_pod_project()

    assert resolution_first.status == "resolved"
    assert resolution_first.project is project
    assert resolution_first.project_branch is project_branch
    assert resolution_first.repository_branch == "main"
    assert resolution_second.project is project
    assert project_calls == [{"pk": PROJECT_UID}]
    assert branch_calls == [{"pk": PROJECT_BRANCH_UID}]


def test_set_remote_db_warns_once_for_lookup_failure(monkeypatch):
    monkeypatch.setenv("MAIN_SEQUENCE_PROJECT_UID", PROJECT_UID)
    monkeypatch.delenv("MAIN_SEQUENCE_PROJECT_ID", raising=False)
    monkeypatch.setattr(models_metatables, "_current_repository_branch", lambda: "main")

    warnings = []
    debugs = []

    monkeypatch.setattr(
        models_metatables.logger, "warning", lambda message: warnings.append(message)
    )
    monkeypatch.setattr(models_metatables.logger, "debug", lambda message: debugs.append(message))

    def _project_get(*args, **kwargs):
        raise RuntimeError("contract mismatch")

    monkeypatch.setattr(models_foundry.Project, "get", _project_get)

    pod_data_source = models_metatables.PodDataSource()

    assert pod_data_source.set_remote_db() is None
    assert pod_data_source.set_remote_db() is None

    assert len(warnings) == 1
    assert f"Could not resolve project reference {PROJECT_UID!r}" in warnings[0]
    assert "contract mismatch" in warnings[0]
    assert "Continuing without local pod project attachment." in warnings[0]
    assert debugs == []


def test_set_remote_db_uses_project_default_for_unregistered_git_branch(monkeypatch):
    monkeypatch.setenv("MAIN_SEQUENCE_PROJECT_UID", PROJECT_UID)
    monkeypatch.setattr(
        models_metatables,
        "_current_repository_branch",
        lambda: "feature/local-only",
    )
    data_source = types.SimpleNamespace(
        uid=DATA_SOURCE_UID,
        status=models_metatables.DataSource.STATUS_AVAILABLE,
    )
    project = types.SimpleNamespace(
        uid=PROJECT_UID,
        branches=[],
        default_metatables_data_source=data_source,
    )
    monkeypatch.setattr(models_foundry.Project, "get", lambda **kwargs: project)

    pod_data_source = models_metatables.PodDataSource()

    assert pod_data_source.set_remote_db() is data_source
    assert pod_data_source.data_source is data_source
    resolution = models_metatables._resolve_local_pod_project()
    assert resolution.status == "branch_not_registered"
    assert resolution.project_branch is None


def test_get_session_data_source_tracks_registered_git_branch_switch(monkeypatch):
    feature_branch_uid = "5b28020a-0f1b-47ee-aab8-334286234bea"
    current_branch = {"name": "main"}
    main_data_source = types.SimpleNamespace(
        uid="main-data-source",
        status=models_metatables.DataSource.STATUS_AVAILABLE,
    )
    feature_data_source = types.SimpleNamespace(
        uid="feature-data-source",
        status=models_metatables.DataSource.STATUS_AVAILABLE,
    )
    project = types.SimpleNamespace(
        uid=PROJECT_UID,
        branches=[
            types.SimpleNamespace(uid=PROJECT_BRANCH_UID, repository_branch="main"),
            types.SimpleNamespace(uid=feature_branch_uid, repository_branch="feature/runtime"),
        ],
        default_metatables_data_source=main_data_source,
    )
    project_branches = {
        PROJECT_BRANCH_UID: types.SimpleNamespace(
            uid=PROJECT_BRANCH_UID,
            metatables_data_source=main_data_source,
        ),
        feature_branch_uid: types.SimpleNamespace(
            uid=feature_branch_uid,
            metatables_data_source=feature_data_source,
        ),
    }

    monkeypatch.setenv("MAIN_SEQUENCE_PROJECT_UID", PROJECT_UID)
    monkeypatch.setattr(
        models_metatables,
        "_current_repository_branch",
        lambda: current_branch["name"],
    )
    monkeypatch.setattr(models_foundry.Project, "get", lambda **kwargs: project)
    monkeypatch.setattr(
        models_foundry.ProjectBranch,
        "get",
        lambda **kwargs: project_branches[kwargs["pk"]],
    )
    session_data_source = models_metatables.PodDataSource()
    monkeypatch.setattr(models_metatables, "SessionDataSource", session_data_source)

    assert models_metatables.get_session_data_source() is main_data_source

    current_branch["name"] = "feature/runtime"

    assert models_metatables.get_session_data_source() is feature_data_source
    assert session_data_source.data_source is feature_data_source


def test_get_session_data_source_uses_project_default_after_unregistered_git_switch(
    monkeypatch,
):
    current_branch = {"name": "main"}
    branch_data_source = types.SimpleNamespace(
        uid="main-data-source",
        status=models_metatables.DataSource.STATUS_AVAILABLE,
    )
    default_data_source = types.SimpleNamespace(
        uid="project-default-data-source",
        status=models_metatables.DataSource.STATUS_AVAILABLE,
    )
    project = types.SimpleNamespace(
        uid=PROJECT_UID,
        branches=[types.SimpleNamespace(uid=PROJECT_BRANCH_UID, repository_branch="main")],
        default_metatables_data_source=default_data_source,
    )

    monkeypatch.setenv("MAIN_SEQUENCE_PROJECT_UID", PROJECT_UID)
    monkeypatch.setattr(
        models_metatables,
        "_current_repository_branch",
        lambda: current_branch["name"],
    )
    monkeypatch.setattr(models_foundry.Project, "get", lambda **kwargs: project)
    monkeypatch.setattr(
        models_foundry.ProjectBranch,
        "get",
        lambda **kwargs: types.SimpleNamespace(
            uid=PROJECT_BRANCH_UID,
            metatables_data_source=branch_data_source,
        ),
    )
    session_data_source = models_metatables.PodDataSource()
    monkeypatch.setattr(models_metatables, "SessionDataSource", session_data_source)

    assert models_metatables.get_session_data_source() is branch_data_source

    current_branch["name"] = "feature/local-only"

    assert models_metatables.get_session_data_source() is default_data_source
    assert session_data_source.data_source is default_data_source


def test_get_session_data_source_tracks_project_uid_change(monkeypatch):
    second_project_uid = "2d0530c0-65d1-4db0-856b-dc29d8260a09"
    first_data_source = types.SimpleNamespace(
        uid="first-project-data-source",
        status=models_metatables.DataSource.STATUS_AVAILABLE,
    )
    second_data_source = types.SimpleNamespace(
        uid="second-project-data-source",
        status=models_metatables.DataSource.STATUS_AVAILABLE,
    )
    projects = {
        PROJECT_UID: types.SimpleNamespace(
            uid=PROJECT_UID,
            branches=[],
            default_metatables_data_source=first_data_source,
        ),
        second_project_uid: types.SimpleNamespace(
            uid=second_project_uid,
            branches=[],
            default_metatables_data_source=second_data_source,
        ),
    }

    monkeypatch.setenv("MAIN_SEQUENCE_PROJECT_UID", PROJECT_UID)
    monkeypatch.setattr(models_metatables, "_current_repository_branch", lambda: "main")
    monkeypatch.setattr(
        models_foundry.Project,
        "get",
        lambda **kwargs: projects[kwargs["pk"]],
    )
    session_data_source = models_metatables.PodDataSource()
    monkeypatch.setattr(models_metatables, "SessionDataSource", session_data_source)

    assert models_metatables.get_session_data_source() is first_data_source

    monkeypatch.setenv("MAIN_SEQUENCE_PROJECT_UID", second_project_uid)

    assert models_metatables.get_session_data_source() is second_data_source
    assert session_data_source.data_source is second_data_source


def test_get_session_data_source_preserves_explicit_local_override(monkeypatch):
    local_data_source = types.SimpleNamespace(
        uid="local-data-source",
        class_type=models_metatables.DUCK_DB,
        status=models_metatables.DataSource.STATUS_AVAILABLE,
    )
    session_data_source = models_metatables.PodDataSource(data_source=local_data_source)
    monkeypatch.setattr(models_metatables, "SessionDataSource", session_data_source)
    monkeypatch.setattr(
        models_metatables,
        "_resolve_local_pod_project",
        lambda **kwargs: pytest.fail("explicit local data sources must not resolve Git state"),
    )

    assert models_metatables.get_session_data_source() is local_data_source


def test_resolve_local_pod_project_raises_auth_failure(monkeypatch):
    monkeypatch.setenv("MAIN_SEQUENCE_PROJECT_UID", PROJECT_UID)
    monkeypatch.delenv("MAIN_SEQUENCE_PROJECT_ID", raising=False)
    monkeypatch.setattr(models_metatables, "_current_repository_branch", lambda: "main")

    def _project_get(*args, **kwargs):
        raise AuthenticationError("No auth configured.")

    monkeypatch.setattr(models_foundry.Project, "get", _project_get)

    with pytest.raises(RuntimeError) as exc_info:
        models_metatables._resolve_local_pod_project()

    message = str(exc_info.value)
    assert "SDK authentication/authorization failed" in message
    assert f"project {PROJECT_UID!r}" in message
    assert "mainsequence login" in message
    assert "No auth configured." in message


def test_get_session_data_source_reports_missing_project_uid(monkeypatch):
    monkeypatch.delenv("MAIN_SEQUENCE_PROJECT_UID", raising=False)
    monkeypatch.delenv("MAIN_SEQUENCE_PROJECT_ID", raising=False)
    monkeypatch.setattr(models_metatables, "SessionDataSource", models_metatables.PodDataSource())
    monkeypatch.setattr(models_metatables.logger, "debug", lambda message: None)

    with pytest.raises(RuntimeError) as exc_info:
        models_metatables.get_session_data_source()

    message = str(exc_info.value)
    assert "Could not resolve a session default data source." in message
    assert "MAIN_SEQUENCE_PROJECT_UID is not configured." in message
    assert "pass data_source_uid explicitly" in message
    assert "This Pod does not have a default data source" not in message


def test_data_source_create_duckdb_makes_creation_explicit(monkeypatch):
    captured = {}
    monkeypatch.setattr(models_metatables, "bios_uuid", lambda: "host-123")

    def _get_or_create(cls, time_out=None, **kwargs):
        captured["time_out"] = time_out
        captured["kwargs"] = kwargs
        return types.SimpleNamespace(id=7, class_type=models_metatables.DUCK_DB)

    monkeypatch.setattr(
        models_metatables.DataSource,
        "get_or_create_duck_db",
        classmethod(_get_or_create),
    )

    data_source = models_metatables.DataSource.create_duckdb(time_out=15)

    assert data_source.id == 7
    assert captured == {
        "time_out": 15,
        "kwargs": {
            "display_name": "DuckDB_host-123",
            "host_mac_address": "host-123",
        },
    }


def test_data_source_create_sqlite_makes_creation_explicit(monkeypatch):
    captured = {}
    monkeypatch.setattr(models_metatables, "bios_uuid", lambda: "host-456")

    def _get_or_create(cls, time_out=None, **kwargs):
        captured["time_out"] = time_out
        captured["kwargs"] = kwargs
        return types.SimpleNamespace(id=8, class_type=models_metatables.SQLITE)

    monkeypatch.setattr(
        models_metatables.DataSource,
        "get_or_create_sqlite",
        classmethod(_get_or_create),
    )

    data_source = models_metatables.DataSource.create_sqlite(time_out=20)

    assert data_source.id == 8
    assert captured == {
        "time_out": 20,
        "kwargs": {
            "display_name": "SQLite_host-456",
            "host_mac_address": "host-456",
        },
    }


def test_set_local_db_requires_explicit_duckdb_data_source():
    with pytest.raises(ValueError, match="DataSource.create_duckdb"):
        models_metatables.PodDataSource().set_local_db()


def test_set_local_db_uses_explicit_duckdb_source_without_hidden_creation(monkeypatch):
    physical_data_source = types.SimpleNamespace(
        uid="duckdb-data-source",
        display_name="Local DuckDB",
        class_type=models_metatables.DUCK_DB,
        status="AVAILABLE",
    )
    captured = {}

    def _hidden_create(*args, **kwargs):
        raise AssertionError("set_local_db should not create the physical DuckDB DataSource")

    def _filter_storages(cls, **kwargs):
        captured["filter_kwargs"] = kwargs
        return []

    class _DuckDB:
        db_path = "/tmp/duckdb"

        @staticmethod
        def list_tables():
            return []

        @staticmethod
        def drop_table(table):
            raise AssertionError(f"unexpected drop_table({table!r})")

    monkeypatch.setattr(
        models_metatables.DataSource,
        "get_or_create_duck_db",
        classmethod(_hidden_create),
    )
    monkeypatch.setattr(
        models_metatables.TimeIndexMetaTable,
        "filter",
        classmethod(_filter_storages),
    )
    monkeypatch.setattr(models_metatables, "_duckdb_interface", lambda: _DuckDB())

    pod_data_source = models_metatables.PodDataSource()
    pod_data_source.set_local_db(data_source=physical_data_source)

    assert pod_data_source.data_source is physical_data_source
    assert captured["filter_kwargs"] == {
        "data_source__uid": "duckdb-data-source",
        "list_tables": True,
    }


def test_set_local_db_accepts_explicit_sqlite_source_without_hidden_creation(monkeypatch):
    physical_data_source = types.SimpleNamespace(
        uid="sqlite-data-source",
        display_name="Local SQLite",
        class_type=models_metatables.SQLITE,
        status="AVAILABLE",
    )
    captured = {}

    def _hidden_create(*args, **kwargs):
        raise AssertionError("set_local_db should not create the physical SQLite DataSource")

    def _filter_storages(cls, **kwargs):
        captured["filter_kwargs"] = kwargs
        return []

    class _SQLite:
        db_file = "/tmp/mainsequence.sqlite"

        @staticmethod
        def list_tables():
            return []

        @staticmethod
        def drop_table(table):
            raise AssertionError(f"unexpected drop_table({table!r})")

    monkeypatch.setattr(
        models_metatables.DataSource,
        "get_or_create_sqlite",
        classmethod(_hidden_create),
    )
    monkeypatch.setattr(
        models_metatables.TimeIndexMetaTable,
        "filter",
        classmethod(_filter_storages),
    )
    monkeypatch.setattr(models_metatables, "_sqlite_interface", lambda: _SQLite())

    pod_data_source = models_metatables.PodDataSource()
    pod_data_source.set_local_db(data_source=physical_data_source)

    assert pod_data_source.data_source is physical_data_source
    assert captured["filter_kwargs"] == {
        "data_source__uid": "sqlite-data-source",
        "list_tables": True,
    }


def test_delete_table_does_not_create_duckdb_source_to_classify(monkeypatch):
    drops = []
    deletes = []

    def _hidden_create(*args, **kwargs):
        raise AssertionError("delete_table should not create DuckDB data sources")

    monkeypatch.setattr(
        models_metatables.DataSource,
        "get_or_create_duck_db",
        classmethod(_hidden_create),
    )
    monkeypatch.setattr(
        models_metatables,
        "_duckdb_interface",
        lambda: types.SimpleNamespace(drop_table=lambda table: drops.append(table)),
    )
    monkeypatch.setattr(
        models_metatables.TimeIndexMetaTable,
        "delete",
        lambda self: deletes.append(self.physical_table_name),
    )

    storage = models_metatables.TimeIndexMetaTable.model_construct(
        physical_table_name="node-storage",
        data_source=types.SimpleNamespace(class_type=models_metatables.DUCK_DB),
    )

    storage.delete_table()

    assert drops == ["node-storage"]
    assert deletes == ["node-storage"]


def test_delete_table_uses_sqlite_adapter_for_sqlite_storage(monkeypatch):
    drops = []
    deletes = []

    monkeypatch.setattr(
        models_metatables,
        "_sqlite_interface",
        lambda: types.SimpleNamespace(drop_table=lambda table: drops.append(table)),
    )
    monkeypatch.setattr(
        models_metatables.TimeIndexMetaTable,
        "delete",
        lambda self: deletes.append(self.physical_table_name),
    )

    storage = models_metatables.TimeIndexMetaTable.model_construct(
        physical_table_name="node-storage",
        data_source=types.SimpleNamespace(class_type=models_metatables.SQLITE),
    )

    storage.delete_table()

    assert drops == ["node-storage"]
    assert deletes == ["node-storage"]


def test_resolve_local_pod_project_does_not_fall_back_to_project_id(monkeypatch):
    monkeypatch.delenv("MAIN_SEQUENCE_PROJECT_UID", raising=False)
    monkeypatch.setenv("MAIN_SEQUENCE_PROJECT_ID", "123")

    def _project_get(*args, **kwargs):
        raise AssertionError("MAIN_SEQUENCE_PROJECT_ID must not be used for project lookup.")

    monkeypatch.setattr(models_foundry.Project, "get", _project_get)

    resolution = models_metatables._resolve_local_pod_project()

    assert resolution.status == "missing"
    assert "MAIN_SEQUENCE_PROJECT_UID is not configured." in resolution.detail


def test_data_node_update_get_or_create_requires_local_pod_project(monkeypatch):
    monkeypatch.delenv("MAIN_SEQUENCE_PROJECT_UID", raising=False)
    monkeypatch.delenv("MAIN_SEQUENCE_PROJECT_ID", raising=False)
    monkeypatch.setattr(
        models_metatables.logger,
        "debug",
        lambda message: None,
    )
    monkeypatch.setattr(
        models_metatables.logger,
        "warning",
        lambda message: None,
    )
    monkeypatch.setattr(
        models_metatables,
        "make_request",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("make_request should not run")
        ),
    )

    with pytest.raises(RuntimeError) as exc_info:
        models_metatables.DataNodeUpdate.get_or_create(update_hash="abc123")

    message = str(exc_info.value)
    assert "DataNodeUpdate.get_or_create requires a registered active ProjectBranch." in message
    assert "MAIN_SEQUENCE_PROJECT_UID is not configured." in message


def test_build_operations_data_node_reference_serialization_uses_data_source_uid():
    data_node = types.SimpleNamespace(
        update_hash="update-hash-1",
        data_source_uid=DATA_SOURCE_UID,
    )

    payload = build_operations._serialize_timeserie(data_node)

    assert payload == {
        "is_time_serie_instance": True,
        "update_hash": "update-hash-1",
        "data_source_uid": DATA_SOURCE_UID,
    }
    assert "data_source_id" not in payload
    assert "is_time_serie_pickled" not in payload


def test_build_operations_api_node_reference_serialization_uses_data_source_uid():
    api_node = types.SimpleNamespace(
        update_hash="api-update-hash-1",
        data_source_uid=DATA_SOURCE_UID,
    )

    payload = build_operations._serialize_api_timeserie(api_node)

    assert payload == {
        "is_api_time_serie_instance": True,
        "update_hash": "api-update-hash-1",
        "data_source_uid": DATA_SOURCE_UID,
    }
    assert "data_source_id" not in payload
    assert "is_api_time_serie_pickled" not in payload


def test_build_operations_does_not_expose_data_node_cold_rebuild_helpers():
    assert not hasattr(build_operations, "rebuild_from_configuration")
    assert not hasattr(build_operations, "rebuild_and_set_from_update_hash")
