from __future__ import annotations

import datetime
import os
from types import SimpleNamespace
from typing import Annotated

import pytest
from pydantic import Field

os.environ.setdefault("TDAG_ENDPOINT", "http://testserver")
os.environ.setdefault("MAINSEQUENCE_ACCESS_TOKEN", "test-access-token")
os.environ.setdefault("MAINSEQUENCE_REFRESH_TOKEN", "test-refresh-token")

import mainsequence.client.models_simple_tables as client_simple_table_models
from mainsequence.client.utils import API_ENDPOINT
from mainsequence.tdag.simple_tables import (
    Index,
    SimpleTable,
    SimpleTablePersistManager,
    SimpleTableUpdater,
    SimpleTableUpdaterConfiguration,
)


class _FakeDataSource:
    id = 1

    def model_dump(self):
        return {"id": 1}


class OrderRow(SimpleTable):
    id: int
    order_code: Annotated[str, Index(unique=True)] = Field(...)
    created_at: datetime.datetime


class OrderUpdaterConfiguration(SimpleTableUpdaterConfiguration):
    tenant: str = Field(..., json_schema_extra={"update_only": True})


def _build_update(*, update_hash: str = "order_updater_hash") -> client_simple_table_models.SimpleTableUpdate:
    return client_simple_table_models.SimpleTableUpdate(
        id=11,
        update_hash=update_hash,
        simple_table=client_simple_table_models.SimpleTableStorage(
            id=41,
            storage_hash="storage_hash",
            data_source={"id": 1},
            build_configuration={"configuration": {}},
            schema={"model": "tests.test_simple_tables_persistence.OrderRow", "fields": []},
            schema_fingerprint="schema-fingerprint",
            physical_name="t_schema_fingerprint",
            source_class_name="OrderUpdater",
        ),
        build_configuration={"configuration": {}},
        ogm_dependencies_linked=False,
        update_details=None,
        run_configuration=None,
        open_for_everyone=False,
    )


def test_simple_table_update_model_endpoints():
    assert (
        client_simple_table_models.SimpleTableUpdate.get_object_url()
        == f"{API_ENDPOINT}/ts_manager/simple_tables_update"
    )
    assert (
        client_simple_table_models.SimpleTableUpdateHistorical.get_object_url()
        == f"{API_ENDPOINT}/ts_manager/simple_tables_update_historical"
    )
    assert (
        client_simple_table_models.SimpleTableUpdateDetails.get_object_url()
        == f"{API_ENDPOINT}/ts_manager/simple_tables_update_details"
    )


def test_simple_table_persist_manager_routes_through_simple_table_update(monkeypatch):
    captured: dict[str, object] = {}

    def fake_storage_get_or_create(cls, **kwargs):
        captured["storage_get_or_create_kwargs"] = kwargs
        return client_simple_table_models.SimpleTableStorage(
            id=41,
            storage_hash=kwargs["storage_hash"],
            data_source=kwargs["data_source"],
            build_configuration=kwargs["build_configuration"],
            build_configuration_json_schema=kwargs["build_configuration_json_schema"],
            time_serie_source_code_git_hash=kwargs["time_serie_source_code_git_hash"],
            time_serie_source_code=kwargs["time_serie_source_code"],
            schema=kwargs["schema"],
            schema_fingerprint=kwargs["schema_fingerprint"],
            physical_name=kwargs["physical_name"],
            source_class_name=kwargs["source_class_name"],
            open_for_everyone=kwargs["open_to_public"],
        )

    def fake_get_or_create(cls, **kwargs):
        captured["get_or_create_kwargs"] = kwargs
        return _build_update(update_hash=kwargs["update_hash"])

    def fake_insert_data_into_table(cls, *, data_node_update_id, records, overwrite=True, add_insertion_time=False):
        calls = captured.setdefault("insert_calls", [])
        calls.append(
            {
                "data_node_update_id": data_node_update_id,
                "records": records,
                "overwrite": overwrite,
                "add_insertion_time": add_insertion_time,
            }
        )

    monkeypatch.setattr(
        client_simple_table_models.SimpleTableStorage,
        "get_or_create",
        classmethod(fake_storage_get_or_create),
    )
    monkeypatch.setattr(
        client_simple_table_models.SimpleTableUpdate,
        "get_or_create",
        classmethod(fake_get_or_create),
    )
    monkeypatch.setattr(
        client_simple_table_models.SimpleTableUpdate,
        "get_or_none",
        classmethod(lambda cls, **kwargs: None),
    )
    monkeypatch.setattr(
        client_simple_table_models.SimpleTableUpdate,
        "insert_data_into_table",
        classmethod(fake_insert_data_into_table),
    )

    manager = SimpleTablePersistManager(
        update_hash="order_updater_hash",
        class_name="OrderUpdater",
        data_source=_FakeDataSource(),
        simple_table_schema=OrderRow,
        configuration=OrderUpdaterConfiguration(tenant="desk_a"),
    )

    manager.local_persist_exist_set_config(
        storage_hash="storage_hash",
        local_configuration={"configuration": {"tenant": "desk_a"}},
        remote_configuration={"configuration": {"tenant": "desk_a"}},
        data_source=_FakeDataSource(),
        time_serie_source_code_git_hash="git-hash",
        time_serie_source_code="source",
        build_configuration_json_schema={},
        open_to_public=True,
    )

    record = OrderRow(
        id=7,
        order_code="M-7",
        created_at=datetime.datetime(2026, 3, 22, 13, 0, tzinfo=datetime.UTC),
    )

    assert manager.insert_records([record]) == [record]
    assert manager.upsert_records([record]) == [record]

    kwargs = captured["get_or_create_kwargs"]
    storage_kwargs = captured["storage_get_or_create_kwargs"]
    assert storage_kwargs["storage_hash"] == "storage_hash"
    assert storage_kwargs["time_serie_source_code_git_hash"] == "git-hash"
    assert storage_kwargs["time_serie_source_code"] == "source"
    assert storage_kwargs["build_configuration"] == {"configuration": {"tenant": "desk_a"}}
    assert storage_kwargs["build_configuration_json_schema"] == {}
    assert storage_kwargs["open_to_public"] is True
    assert storage_kwargs["schema_fingerprint"] == OrderRow.schema_fingerprint()
    assert storage_kwargs["physical_name"] == OrderRow.physical_name()
    assert storage_kwargs["schema"]["model"].endswith("OrderRow")
    assert kwargs["update_hash"] == "order_updater_hash"
    assert kwargs["open_for_everyone"] is True
    assert kwargs["simple_table"] == 41

    insert_calls = captured["insert_calls"]
    assert insert_calls[0]["data_node_update_id"] == 11
    assert insert_calls[0]["overwrite"] is False
    assert insert_calls[1]["overwrite"] is True


def test_simple_table_updater_uses_persist_manager_after_backend_registration(monkeypatch):
    captured: dict[str, object] = {}

    def fake_storage_get_or_create(cls, **kwargs):
        captured["storage_get_or_create_kwargs"] = kwargs
        return client_simple_table_models.SimpleTableStorage(
            id=41,
            storage_hash=kwargs["storage_hash"],
            data_source=kwargs["data_source"],
            build_configuration=kwargs["build_configuration"],
            build_configuration_json_schema=kwargs["build_configuration_json_schema"],
            time_serie_source_code_git_hash=kwargs["time_serie_source_code_git_hash"],
            time_serie_source_code=kwargs["time_serie_source_code"],
            schema=kwargs["schema"],
            schema_fingerprint=kwargs["schema_fingerprint"],
            physical_name=kwargs["physical_name"],
            source_class_name=kwargs["source_class_name"],
            open_for_everyone=kwargs["open_to_public"],
        )

    def fake_get_or_create(cls, **kwargs):
        captured["get_or_create_kwargs"] = kwargs
        return _build_update(update_hash=kwargs["update_hash"])

    def fake_insert_data_into_table(cls, *, data_node_update_id, records, overwrite=True, add_insertion_time=False):
        calls = captured.setdefault("insert_calls", [])
        calls.append(
            {
                "data_node_update_id": data_node_update_id,
                "records": records,
                "overwrite": overwrite,
            }
        )

    monkeypatch.setattr(
        client_simple_table_models.SimpleTableStorage,
        "get_or_create",
        classmethod(fake_storage_get_or_create),
    )
    monkeypatch.setattr(
        client_simple_table_models.SimpleTableUpdate,
        "get_or_create",
        classmethod(fake_get_or_create),
    )
    monkeypatch.setattr(
        client_simple_table_models.SimpleTableUpdate,
        "get_or_none",
        classmethod(lambda cls, **kwargs: None),
    )
    monkeypatch.setattr(
        client_simple_table_models.SimpleTableUpdate,
        "insert_data_into_table",
        classmethod(fake_insert_data_into_table),
    )

    class OrderUpdater(SimpleTableUpdater):
        SIMPLE_TABLE_SCHEMA = OrderRow

        def set_data_source(self, data_source=None):
            self._data_source = _FakeDataSource()

        def update(self):
            return None

    updater = OrderUpdater(configuration=OrderUpdaterConfiguration(tenant="desk_a"))
    updater.verify_and_build_remote_objects()

    record = OrderRow(
        id=9,
        order_code="UP-9",
        created_at=datetime.datetime(2026, 3, 22, 12, 0, tzinfo=datetime.UTC),
    )

    assert updater.insert_records([record]) == [record]
    assert updater.upsert_records([record]) == [record]

    insert_calls = captured["insert_calls"]
    assert insert_calls[0]["overwrite"] is False
    assert insert_calls[1]["overwrite"] is True
    assert captured["get_or_create_kwargs"]["simple_table"] == 41
    assert captured["storage_get_or_create_kwargs"]["schema"]["model"].endswith("OrderRow")


def test_simple_table_persist_manager_skips_storage_create_when_remote_loaded(monkeypatch):
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        client_simple_table_models.SimpleTableStorage,
        "get_or_create",
        classmethod(lambda cls, **kwargs: captured.setdefault("storage_calls", []).append(kwargs)),
    )
    monkeypatch.setattr(
        client_simple_table_models.SimpleTableUpdate,
        "get_or_none",
        classmethod(lambda cls, **kwargs: None),
    )
    monkeypatch.setattr(
        client_simple_table_models.SimpleTableUpdate,
        "get_or_create",
        classmethod(lambda cls, **kwargs: _build_update(update_hash=kwargs["update_hash"])),
    )

    manager = SimpleTablePersistManager(
        update_hash="order_updater_hash",
        class_name="OrderUpdater",
        data_source=_FakeDataSource(),
        simple_table_schema=OrderRow,
        configuration=OrderUpdaterConfiguration(tenant="desk_a"),
    )
    manager.data_node_storage = client_simple_table_models.SimpleTableStorage(
        id=41,
        storage_hash="existing_storage_hash",
        data_source={"id": 1},
        build_configuration={"configuration": {"tenant": "desk_a"}},
        schema={"model": "tests.test_simple_tables_persistence.OrderRow", "fields": []},
        schema_fingerprint="schema-fingerprint",
        physical_name="t_schema_fingerprint",
        source_class_name="OrderUpdater",
    )

    manager.local_persist_exist_set_config(
        storage_hash="ignored_storage_hash",
        local_configuration={"configuration": {"tenant": "desk_a"}},
        remote_configuration={"configuration": {"tenant": "desk_a"}},
        data_source=_FakeDataSource(),
        time_serie_source_code_git_hash="git-hash",
        time_serie_source_code="source",
        build_configuration_json_schema={},
        open_to_public=True,
    )

    assert "storage_calls" not in captured


def test_simple_table_persist_manager_skips_update_create_when_local_loaded(monkeypatch):
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        client_simple_table_models.SimpleTableStorage,
        "get_or_create",
        classmethod(lambda cls, **kwargs: captured.setdefault("storage_calls", []).append(kwargs)),
    )
    monkeypatch.setattr(
        client_simple_table_models.SimpleTableUpdate,
        "get_or_none",
        classmethod(lambda cls, **kwargs: captured.setdefault("get_or_none_calls", []).append(kwargs)),
    )
    monkeypatch.setattr(
        client_simple_table_models.SimpleTableUpdate,
        "get_or_create",
        classmethod(lambda cls, **kwargs: captured.setdefault("update_create_calls", []).append(kwargs)),
    )

    manager = SimpleTablePersistManager(
        update_hash="order_updater_hash",
        class_name="OrderUpdater",
        data_source=_FakeDataSource(),
        simple_table_schema=OrderRow,
        configuration=OrderUpdaterConfiguration(tenant="desk_a"),
        data_node_update=_build_update(),
    )

    manager.local_persist_exist_set_config(
        storage_hash="ignored_storage_hash",
        local_configuration={"configuration": {"tenant": "desk_a"}},
        remote_configuration={"configuration": {"tenant": "desk_a"}},
        data_source=_FakeDataSource(),
        time_serie_source_code_git_hash="git-hash",
        time_serie_source_code="source",
        build_configuration_json_schema={},
        open_to_public=True,
    )

    assert "get_or_none_calls" not in captured
    assert "update_create_calls" not in captured


def test_simple_table_delete_uses_detail_delete(monkeypatch):
    captured: dict[str, object] = {}

    def fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured["r_type"] = r_type
        captured["url"] = url
        return SimpleNamespace(status_code=204, text="", content=b"", json=lambda: {})

    monkeypatch.setattr(client_simple_table_models, "make_request", fake_make_request)

    row = OrderRow(
        id=9,
        order_code="DEL-9",
        created_at=datetime.datetime(2026, 3, 22, 12, 0, tzinfo=datetime.UTC),
    )
    client_simple_table_models.SimpleTableStorage.delete_record(row)

    assert captured["r_type"] == "DELETE"
    assert captured["url"] == f"{API_ENDPOINT}/ts_manager/simple_tables/9/"


def test_simple_table_updater_requires_class_level_schema():
    class InvalidUpdater(SimpleTableUpdater):
        def set_data_source(self, data_source=None):
            self._data_source = _FakeDataSource()

        def update(self):
            return None

    with pytest.raises(TypeError, match="SIMPLE_TABLE_SCHEMA"):
        InvalidUpdater(configuration=OrderUpdaterConfiguration(tenant="desk_a"))
