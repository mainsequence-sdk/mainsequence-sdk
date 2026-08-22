import types

import pytest

import mainsequence.client.metatables as models_metatables
from mainsequence.meta_tables.data_nodes import build_operations

DATA_SOURCE_UID = "864e7c22-482a-464a-8758-0d3408abd77f"


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
