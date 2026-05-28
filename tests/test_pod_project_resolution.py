import types

import pytest

import mainsequence.client.models_metatables as models_metatables
import mainsequence.client.models_tdag as models_tdag
from mainsequence.tdag.data_nodes import build_operations

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


def test_data_source_create_duckdb_makes_creation_explicit(monkeypatch):
    captured = {}
    monkeypatch.setattr(models_metatables, "bios_uuid", lambda: "host-123")

    def _get_or_create(cls, time_out=None, **kwargs):
        captured["time_out"] = time_out
        captured["kwargs"] = kwargs
        return types.SimpleNamespace(id=7, class_type=models_tdag.DUCK_DB)

    monkeypatch.setattr(
        models_tdag.DataSource,
        "get_or_create_duck_db",
        classmethod(_get_or_create),
    )

    data_source = models_tdag.DataSource.create_duckdb(time_out=15)

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
        return types.SimpleNamespace(id=8, class_type=models_tdag.SQLITE)

    monkeypatch.setattr(
        models_tdag.DataSource,
        "get_or_create_sqlite",
        classmethod(_get_or_create),
    )

    data_source = models_tdag.DataSource.create_sqlite(time_out=20)

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
        models_tdag.PodDataSource().set_local_db()


def test_set_local_db_uses_explicit_duckdb_source_without_hidden_creation(monkeypatch):
    physical_data_source = types.SimpleNamespace(
        id=7,
        display_name="Local DuckDB",
        class_type=models_tdag.DUCK_DB,
        status="AVAILABLE",
    )
    dynamic_data_source = types.SimpleNamespace(
        id=42,
        uid="dynamic-duckdb",
        related_resource=physical_data_source,
    )
    captured = {}

    def _hidden_create(*args, **kwargs):
        raise AssertionError("set_local_db should not create the physical DuckDB DataSource")

    def _create_dynamic(cls, *, data_source, **kwargs):
        captured["dynamic_data_source"] = data_source
        captured["dynamic_kwargs"] = kwargs
        return dynamic_data_source

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
        models_tdag.DataSource,
        "get_or_create_duck_db",
        classmethod(_hidden_create),
    )
    monkeypatch.setattr(
        models_tdag.DynamicTableDataSource,
        "create_duckdb",
        classmethod(_create_dynamic),
    )
    monkeypatch.setattr(
        models_tdag.DataNodeStorage,
        "filter",
        classmethod(_filter_storages),
    )
    monkeypatch.setattr(models_tdag, "_duckdb_interface", lambda: _DuckDB())

    pod_data_source = models_tdag.PodDataSource()
    pod_data_source.set_local_db(data_source=physical_data_source)

    assert pod_data_source.data_source is dynamic_data_source
    assert captured["dynamic_data_source"] is physical_data_source
    assert captured["dynamic_kwargs"] == {}
    assert captured["filter_kwargs"] == {"data_source__uid": "dynamic-duckdb", "list_tables": True}


def test_set_local_db_accepts_explicit_sqlite_source_without_hidden_creation(monkeypatch):
    physical_data_source = types.SimpleNamespace(
        id=8,
        display_name="Local SQLite",
        class_type=models_tdag.SQLITE,
        status="AVAILABLE",
    )
    dynamic_data_source = types.SimpleNamespace(
        id=43,
        uid="dynamic-sqlite",
        related_resource=physical_data_source,
    )
    captured = {}

    def _hidden_create(*args, **kwargs):
        raise AssertionError("set_local_db should not create the physical SQLite DataSource")

    def _create_dynamic(cls, *, data_source, **kwargs):
        captured["dynamic_data_source"] = data_source
        captured["dynamic_kwargs"] = kwargs
        return dynamic_data_source

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
        models_tdag.DataSource,
        "get_or_create_sqlite",
        classmethod(_hidden_create),
    )
    monkeypatch.setattr(
        models_tdag.DynamicTableDataSource,
        "create_sqlite",
        classmethod(_create_dynamic),
    )
    monkeypatch.setattr(
        models_tdag.DataNodeStorage,
        "filter",
        classmethod(_filter_storages),
    )
    monkeypatch.setattr(models_tdag, "_sqlite_interface", lambda: _SQLite())

    pod_data_source = models_tdag.PodDataSource()
    pod_data_source.set_local_db(data_source=physical_data_source)

    assert pod_data_source.data_source is dynamic_data_source
    assert captured["dynamic_data_source"] is physical_data_source
    assert captured["dynamic_kwargs"] == {}
    assert captured["filter_kwargs"] == {"data_source__uid": "dynamic-sqlite", "list_tables": True}


def test_delete_table_does_not_create_duckdb_source_to_classify(monkeypatch):
    drops = []
    deletes = []

    def _hidden_create(*args, **kwargs):
        raise AssertionError("delete_table should not create DuckDB data sources")

    monkeypatch.setattr(
        models_tdag.DataSource,
        "get_or_create_duck_db",
        classmethod(_hidden_create),
    )
    monkeypatch.setattr(
        models_tdag.DynamicTableDataSource,
        "get_or_create_duck_db",
        classmethod(_hidden_create),
    )
    monkeypatch.setattr(
        models_tdag,
        "_duckdb_interface",
        lambda: types.SimpleNamespace(drop_table=lambda table: drops.append(table)),
    )
    monkeypatch.setattr(
        models_tdag.DataNodeStorage,
        "delete",
        lambda self: deletes.append(self.storage_hash),
    )

    storage = models_tdag.DataNodeStorage.model_construct(
        storage_hash="node-storage",
        data_source=types.SimpleNamespace(
            related_resource=types.SimpleNamespace(class_type=models_tdag.DUCK_DB),
        ),
    )

    storage.delete_table()

    assert drops == ["node-storage"]
    assert deletes == ["node-storage"]


def test_delete_table_uses_sqlite_adapter_for_sqlite_storage(monkeypatch):
    drops = []
    deletes = []

    monkeypatch.setattr(
        models_tdag,
        "_sqlite_interface",
        lambda: types.SimpleNamespace(drop_table=lambda table: drops.append(table)),
    )
    monkeypatch.setattr(
        models_tdag.DataNodeStorage,
        "delete",
        lambda self: deletes.append(self.storage_hash),
    )

    storage = models_tdag.DataNodeStorage.model_construct(
        storage_hash="node-storage",
        data_source=types.SimpleNamespace(
            related_resource=types.SimpleNamespace(class_type=models_tdag.SQLITE),
        ),
    )

    storage.delete_table()

    assert drops == ["node-storage"]
    assert deletes == ["node-storage"]


def test_resolve_local_pod_project_does_not_fall_back_to_project_id(monkeypatch):
    monkeypatch.delenv("MAIN_SEQUENCE_PROJECT_UID", raising=False)
    monkeypatch.setenv("MAIN_SEQUENCE_PROJECT_ID", "123")

    def _project_get(*args, **kwargs):
        raise AssertionError("MAIN_SEQUENCE_PROJECT_ID must not be used for project lookup.")

    monkeypatch.setattr(models_tdag.Project, "get", _project_get)

    resolution = models_tdag._resolve_local_pod_project()

    assert resolution.status == "missing"
    assert "MAIN_SEQUENCE_PROJECT_UID is not configured." in resolution.detail


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


def test_build_operations_rebuilds_from_configuration_without_pickle(monkeypatch):
    calls = []

    class FakeLogger:
        def debug(self, message):
            calls.append(("debug", message))

    class FakeDataNode:
        logger = FakeLogger()

        def set_relation_tree(self):
            calls.append(("set_relation_tree",))

        def _set_state_with_sessions(
            self,
            *,
            graph_depth,
            graph_depth_limit,
            include_client_objects,
        ):
            calls.append(
                (
                    "set_state_with_sessions",
                    graph_depth,
                    graph_depth_limit,
                    include_client_objects,
                )
            )

    fake_ts = FakeDataNode()

    def fake_rebuild_from_configuration(update_hash, data_source):
        calls.append(("rebuild_from_configuration", update_hash, data_source))
        return fake_ts

    monkeypatch.setattr(
        build_operations,
        "rebuild_from_configuration",
        fake_rebuild_from_configuration,
    )

    result = build_operations.rebuild_and_set_from_update_hash(
        update_hash="update-hash-1",
        data_source_uid=DATA_SOURCE_UID,
        set_dependencies_df=True,
        graph_depth_limit=3,
    )

    assert result is fake_ts
    assert calls == [
        ("rebuild_from_configuration", "update-hash-1", DATA_SOURCE_UID),
        ("set_relation_tree",),
        ("set_state_with_sessions", 0, 3, False),
        ("debug", "ts update-hash-1 rebuilt from configuration"),
    ]
