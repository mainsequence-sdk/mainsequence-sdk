from __future__ import annotations

import base64
import datetime
import gzip
import json
import os
from types import SimpleNamespace
from typing import Annotated

import pytest
from pydantic import Field

os.environ.setdefault("TDAG_ENDPOINT", "http://testserver")
os.environ.setdefault("MAINSEQUENCE_ACCESS_TOKEN", "test-access-token")
os.environ.setdefault("MAINSEQUENCE_REFRESH_TOKEN", "test-refresh-token")

import mainsequence.client.models_simple_tables as client_simple_table_models
from mainsequence.client.models_tdag import UpdateStatistics
from mainsequence.client.utils import API_ENDPOINT
from mainsequence.tdag.simple_tables import (
    ForeignKey,
    Index,
    Ops,
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


class _HttpResponse:
    def __init__(self, status_code: int, text: str = ""):
        self.status_code = status_code
        self.text = text


def test_insert_records_into_table_raises_before_success_log_on_http_error(monkeypatch):
    info_messages: list[str] = []

    monkeypatch.setattr(
        client_simple_table_models,
        "make_request",
        lambda **kwargs: _HttpResponse(
            500,
            '{"detail":"insert_data_into_table is intentionally not implemented"}',
        ),
    )

    def fake_raise_for_response(response, payload=None):
        raise RuntimeError(response.text)

    monkeypatch.setattr(client_simple_table_models, "raise_for_response", fake_raise_for_response)
    monkeypatch.setattr(
        client_simple_table_models,
        "logger",
        SimpleNamespace(info=info_messages.append, exception=lambda *_args, **_kwargs: None),
    )

    with pytest.raises(RuntimeError, match="intentionally not implemented"):
        client_simple_table_models.SimpleTableUpdate.insert_records_into_table(
            data_node_update_id=1226,
            records=[{"id": 1, "name": "Alice"}],
        )

    assert info_messages == []

def test_insert_records_into_table_logs_success_only_on_success(monkeypatch):
    info_messages: list[str] = []
    raise_calls: list[int] = []

    monkeypatch.setattr(client_simple_table_models, "make_request", lambda **kwargs: _HttpResponse(200))

    def fake_raise_for_response(response, payload=None):
        raise_calls.append(response.status_code)

    monkeypatch.setattr(client_simple_table_models, "raise_for_response", fake_raise_for_response)
    monkeypatch.setattr(
        client_simple_table_models,
        "logger",
        SimpleNamespace(info=info_messages.append, exception=lambda *_args, **_kwargs: None),
    )

    client_simple_table_models.SimpleTableUpdate.insert_records_into_table(
        data_node_update_id=1226,
        records=[{"id": 1, "name": "Alice"}],
    )

    assert raise_calls == []
    assert info_messages == ["Chunk uploaded successfully."]


def _decode_chunk_records(payload: dict[str, object]) -> list[dict[str, object]]:
    compressed_b64 = payload["json"]["data"]
    compressed = base64.b64decode(compressed_b64)
    return json.loads(gzip.decompress(compressed).decode("utf-8"))


def test_insert_records_into_table_sends_record_chunks(monkeypatch):
    requests_payloads: list[dict[str, object]] = []
    info_messages: list[str] = []

    def fake_make_request(**kwargs):
        requests_payloads.append(kwargs["payload"])
        return _HttpResponse(200)

    monkeypatch.setattr(client_simple_table_models, "make_request", fake_make_request)
    monkeypatch.setattr(client_simple_table_models, "raise_for_response", lambda response, payload=None: None)
    monkeypatch.setattr(
        client_simple_table_models,
        "logger",
        SimpleNamespace(
            info=info_messages.append,
            warning=lambda *_args, **_kwargs: None,
            exception=lambda *_args, **_kwargs: None,
        ),
    )

    client_simple_table_models.SimpleTableUpdate.insert_records_into_table(
        data_node_update_id=1226,
        records=[
            {"id": 1, "as_of_date": datetime.date(2026, 3, 24)},
            {"id": 2, "as_of_date": datetime.date(2026, 3, 25)},
            {"id": 3, "as_of_date": datetime.date(2026, 3, 26)},
        ],
        chunk_size=2,
    )

    assert len(requests_payloads) == 2
    assert requests_payloads[0]["json"]["chunk_index"] == 0
    assert requests_payloads[0]["json"]["total_chunks"] == 2
    assert requests_payloads[0]["json"]["chunk_stats"] is None
    assert _decode_chunk_records(requests_payloads[0]) == [
        {"id": 1, "as_of_date": "2026-03-24"},
        {"id": 2, "as_of_date": "2026-03-25"},
    ]
    assert requests_payloads[1]["json"]["chunk_index"] == 1
    assert requests_payloads[1]["json"]["total_chunks"] == 2
    assert requests_payloads[1]["json"]["chunk_stats"] is None
    assert _decode_chunk_records(requests_payloads[1]) == [
        {"id": 3, "as_of_date": "2026-03-26"},
    ]
    assert info_messages == [
        "Chunk uploaded successfully.",
        "Chunk uploaded successfully.",
    ]


def test_insert_records_into_table_splits_413_chunks(monkeypatch):
    requests_payloads: list[dict[str, object]] = []
    statuses = iter([413, 200, 200])
    warning_messages: list[str] = []

    def fake_make_request(**kwargs):
        requests_payloads.append(kwargs["payload"])
        return _HttpResponse(next(statuses))

    monkeypatch.setattr(client_simple_table_models, "make_request", fake_make_request)
    monkeypatch.setattr(
        client_simple_table_models,
        "raise_for_response",
        lambda response, payload=None: None,
    )
    monkeypatch.setattr(
        client_simple_table_models,
        "logger",
        SimpleNamespace(
            info=lambda *_args, **_kwargs: None,
            warning=warning_messages.append,
            exception=lambda *_args, **_kwargs: None,
        ),
    )

    client_simple_table_models.SimpleTableUpdate.insert_records_into_table(
        data_node_update_id=1226,
        records=[{"id": 1}, {"id": 2}, {"id": 3}],
        chunk_size=10,
    )

    assert len(requests_payloads) == 3
    assert requests_payloads[0]["json"]["chunk_index"] == 0
    assert requests_payloads[0]["json"]["total_chunks"] == 1
    assert _decode_chunk_records(requests_payloads[0]) == [{"id": 1}, {"id": 2}, {"id": 3}]
    assert requests_payloads[1]["json"]["chunk_index"] == 0
    assert requests_payloads[1]["json"]["total_chunks"] == 1
    assert _decode_chunk_records(requests_payloads[1]) == [{"id": 1}]
    assert requests_payloads[2]["json"]["chunk_index"] == 0
    assert requests_payloads[2]["json"]["total_chunks"] == 1
    assert _decode_chunk_records(requests_payloads[2]) == [{"id": 2}, {"id": 3}]
    assert len(warning_messages) == 1


def test_simple_table_storage_insert_records_into_table_targets_storage_url(monkeypatch):
    requests_payloads: list[dict[str, object]] = []
    urls: list[str] = []

    def fake_make_request(**kwargs):
        urls.append(kwargs["url"])
        requests_payloads.append(kwargs["payload"])
        return _HttpResponse(200)

    monkeypatch.setattr(client_simple_table_models, "make_request", fake_make_request)
    monkeypatch.setattr(client_simple_table_models, "raise_for_response", lambda response, payload=None: None)
    monkeypatch.setattr(
        client_simple_table_models,
        "logger",
        SimpleNamespace(
            info=lambda *_args, **_kwargs: None,
            warning=lambda *_args, **_kwargs: None,
            exception=lambda *_args, **_kwargs: None,
        ),
    )

    client_simple_table_models.SimpleTableStorage.insert_records_into_table(
        simple_table_id=41,
        records=[{"id": 1, "as_of_date": datetime.date(2026, 3, 24)}],
    )

    assert urls == [f"{client_simple_table_models.SimpleTableStorage.get_object_url()}/41/insert_records_into_table/"]
    assert requests_payloads[0]["json"]["chunk_index"] == 0
    assert requests_payloads[0]["json"]["total_chunks"] == 1
    assert _decode_chunk_records(requests_payloads[0]) == [
        {"id": 1, "as_of_date": "2026-03-24"},
    ]


def test_simple_table_storage_upsert_records_into_table_targets_sparse_upsert_url(monkeypatch):
    captured: dict[str, object] = {}

    def fake_make_request(**kwargs):
        captured["url"] = kwargs["url"]
        captured["payload"] = kwargs["payload"]
        captured["r_type"] = kwargs["r_type"]
        return _HttpResponse(204)

    monkeypatch.setattr(client_simple_table_models, "make_request", fake_make_request)
    monkeypatch.setattr(client_simple_table_models, "raise_for_response", lambda response, payload=None: None)

    client_simple_table_models.SimpleTableStorage.upsert_records_into_table(
        simple_table_id=41,
        records=[
            {"id": 1, "as_of_date": datetime.date(2026, 3, 25)},
            {"id": 2, "balance_usd": 150.0},
        ],
    )

    assert captured["r_type"] == "POST"
    assert captured["url"] == (
        f"{client_simple_table_models.SimpleTableStorage.get_object_url()}/41/upsert_records_into_table/"
    )
    assert captured["payload"].keys() == {"json"}
    assert captured["payload"]["json"].keys() == {"data"}
    assert _decode_chunk_records(captured["payload"]) == [
        {"id": 1, "as_of_date": "2026-03-25"},
        {"id": 2, "balance_usd": 150.0},
    ]


def test_delete_records_from_table_posts_ids(monkeypatch):
    captured: dict[str, object] = {}

    def fake_make_request(**kwargs):
        captured["url"] = kwargs["url"]
        captured["payload"] = kwargs["payload"]
        captured["r_type"] = kwargs["r_type"]
        return _HttpResponse(204)

    monkeypatch.setattr(client_simple_table_models, "make_request", fake_make_request)
    monkeypatch.setattr(
        client_simple_table_models,
        "raise_for_response",
        lambda response, payload=None: None,
    )

    client_simple_table_models.SimpleTableStorage.delete_records_from_table(
        data_node_update_id=1226,
        records_ids=[10, 11, 12],
    )

    assert captured["r_type"] == "POST"
    assert captured["url"].endswith("/1226/delete_records_from_table/")
    assert captured["payload"] == {"json": {"records_ids": [10, 11, 12]}}


def test_delete_records_from_table_raises_on_http_error(monkeypatch):
    monkeypatch.setattr(
        client_simple_table_models,
        "make_request",
        lambda **kwargs: _HttpResponse(500, '{"detail":"delete failed"}'),
    )

    def fake_raise_for_response(response, payload=None):
        raise RuntimeError(response.text)

    monkeypatch.setattr(client_simple_table_models, "raise_for_response", fake_raise_for_response)

    with pytest.raises(RuntimeError, match="delete failed"):
        client_simple_table_models.SimpleTableStorage.delete_records_from_table(
            data_node_update_id=1226,
            records_ids=[10, 11],
        )


def test_simple_table_updater_convenience_methods_delegate_to_persist_manager():
    captured: dict[str, object] = {}

    row = OrderRow(
        id=9,
        order_code="ROW-9",
        created_at=datetime.datetime(2026, 3, 22, 12, 0, tzinfo=datetime.UTC),
    )

    class OrderUpdater(SimpleTableUpdater):
        SIMPLE_TABLE_SCHEMA = OrderRow

        def set_data_source(self, data_source=None):
            self._data_source = _FakeDataSource()

        def update(self):
            return []

    updater = OrderUpdater(configuration=OrderUpdaterConfiguration(tenant="desk_a"))
    updater._local_persist_manager = SimpleNamespace(
        insert_records=lambda records: captured.setdefault("insert_calls", []).append(records),
        upsert_records=lambda records: captured.setdefault("upsert_calls", []).append(records),
        delete=lambda record_or_id, timeout=None: captured.update(
            {"delete_call": {"record_or_id": record_or_id, "timeout": timeout}}
        ),
    )

    assert updater.insert(row) is row
    assert updater.upsert(row) is row
    updater.delete(row, timeout=55)

    assert captured["insert_calls"] == [[row]]
    assert captured["upsert_calls"] == [[row]]
    assert captured["delete_call"] == {"record_or_id": row, "timeout": 55}


def test_simple_table_persist_manager_delete_uses_update_delete_records_endpoint(monkeypatch):
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        client_simple_table_models.SimpleTableStorage,
        "delete_records_from_table",
        classmethod(
            lambda cls, *, data_node_update_id, records_ids, timeout=None: captured.update(
                {
                    "data_node_update_id": data_node_update_id,
                    "records_ids": records_ids,
                    "timeout": timeout,
                }
            )
        ),
    )

    manager = SimpleTablePersistManager(
        update_hash="order_updater_hash",
        class_name="OrderUpdater",
        data_source=_FakeDataSource(),
        simple_table_schema=OrderRow,
        configuration=OrderUpdaterConfiguration(tenant="desk_a"),
        data_node_update=_build_update(),
    )

    row = OrderRow(
        id=9,
        order_code="DEL-9",
        created_at=datetime.datetime(2026, 3, 22, 12, 0, tzinfo=datetime.UTC),
    )

    manager.delete(row, timeout=33)

    assert captured == {
        "data_node_update_id": 11,
        "records_ids": [9],
        "timeout": 33,
    }


def _build_update(*, update_hash: str = "order_updater_hash") -> client_simple_table_models.SimpleTableUpdate:
    return client_simple_table_models.SimpleTableUpdate(
        id=11,
        update_hash=update_hash,
        remote_table=client_simple_table_models.SimpleTableStorage(
            id=41,
            storage_hash="storage_hash",
            data_source={"id": 1},
            build_configuration={"configuration": {}},
            schema={"model": "tests.test_simple_tables_persistence.OrderRow", "fields": []},
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
        == f"{API_ENDPOINT}/{client_simple_table_models.SimpleTableUpdate.ENDPOINT}"
    )
    assert (
        client_simple_table_models.SimpleTableUpdateHistorical.get_object_url()
        == f"{API_ENDPOINT}/{client_simple_table_models.SimpleTableUpdateHistorical.ENDPOINT}"
    )
    assert (
        client_simple_table_models.SimpleTableUpdateDetails.get_object_url()
        == f"{API_ENDPOINT}/{client_simple_table_models.SimpleTableUpdateDetails.ENDPOINT}"
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
            time_serie_source_code_git_hash=kwargs["source_code_git_hash"],
            time_serie_source_code=kwargs["source_code"],
            schema=kwargs["schema"],
            source_class_name=kwargs["source_class_name"],
            open_for_everyone=kwargs["open_to_public"],
        )

    def fake_get_or_create(cls, **kwargs):
        captured["get_or_create_kwargs"] = kwargs
        return _build_update(update_hash=kwargs["update_hash"])

    def fake_insert_records_into_table(
        cls, *, data_node_update_id, records, overwrite=True, add_insertion_time=False
    ):
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
        "insert_records_into_table",
        classmethod(fake_insert_records_into_table),
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
    assert storage_kwargs["source_code_git_hash"] == "git-hash"
    assert storage_kwargs["source_code"] == "source"
    assert storage_kwargs["build_configuration"] == {"configuration": {"tenant": "desk_a"}}
    assert storage_kwargs["build_configuration_json_schema"] == {}
    assert storage_kwargs["open_to_public"] is True
    assert storage_kwargs["schema"]["model"].endswith("OrderRow")
    assert kwargs["update_hash"] == "order_updater_hash"
    assert kwargs["open_for_everyone"] is True
    assert kwargs["remote_table"] == 41

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
            time_serie_source_code_git_hash=kwargs["source_code_git_hash"],
            time_serie_source_code=kwargs["source_code"],
            schema=kwargs["schema"],
            source_class_name=kwargs["source_class_name"],
            open_for_everyone=kwargs["open_to_public"],
        )

    def fake_get_or_create(cls, **kwargs):
        captured["get_or_create_kwargs"] = kwargs
        return _build_update(update_hash=kwargs["update_hash"])

    def fake_insert_records_into_table(
        cls, *, data_node_update_id, records, overwrite=True, add_insertion_time=False
    ):
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
        "insert_records_into_table",
        classmethod(fake_insert_records_into_table),
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
    assert captured["get_or_create_kwargs"]["remote_table"] == 41
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


def test_simple_table_storage_accepts_relations_payloads():
    storage = client_simple_table_models.SimpleTableStorage(
        id=41,
        storage_hash="storage_hash",
        data_source={"id": 1},
        schema={"model": "tests.test_simple_tables_persistence.OrderRow", "fields": []},
        source_class_name="OrderUpdater",
        columns=[
            {
                "id": 9,
                "attr_name": "order_code",
                "column_name": "order_code",
                "db_type": "text",
                "is_pk": False,
                "nullable": False,
                "is_unique": True,
            }
        ],
        foreign_keys=[],
        incoming_fks=[],
        indexes_meta=[
            {
                "id": 5,
                "name": "uq_order_code",
                "columns": ["order_code"],
            }
        ],
    )

    assert storage.columns[0].attr_name == "order_code"
    assert storage.columns[0].is_unique is True
    assert storage.indexes_meta[0].name == "uq_order_code"
    assert storage.foreign_keys == []
    assert storage.incoming_fks == []


def test_simple_table_updater_requires_class_level_schema():
    class InvalidUpdater(SimpleTableUpdater):
        def set_data_source(self, data_source=None):
            self._data_source = _FakeDataSource()

        def update(self):
            return None

    with pytest.raises(TypeError, match="SIMPLE_TABLE_SCHEMA"):
        InvalidUpdater(configuration=OrderUpdaterConfiguration(tenant="desk_a"))


def test_simple_table_updater_persists_returned_schema_instances(monkeypatch):
    captured: dict[str, object] = {}
    record = OrderRow(
        id=15,
        order_code="RUN-15",
        created_at=datetime.datetime(2026, 3, 22, 12, 0, tzinfo=datetime.UTC),
    )

    class OrderUpdater(SimpleTableUpdater):
        SIMPLE_TABLE_SCHEMA = OrderRow

        def set_data_source(self, data_source=None):
            self._data_source = _FakeDataSource()

        def update(self):
            return [record], True

    updater = OrderUpdater(configuration=OrderUpdaterConfiguration(tenant="desk_a"))
    updater._local_persist_manager = SimpleNamespace(
        persist_records=lambda *, records, overwrite: captured.update(
            {"records": records, "overwrite": overwrite}
        )
    )

    result = updater._execute_local_update(historical_update=None)

    assert result == [record]
    assert captured["records"] == [record]
    assert captured["overwrite"] is True


def test_simple_table_updater_rejects_non_schema_update_results():
    class OrderUpdater(SimpleTableUpdater):
        SIMPLE_TABLE_SCHEMA = OrderRow

        def set_data_source(self, data_source=None):
            self._data_source = _FakeDataSource()

        def update(self):
            return [{"id": 1, "order_code": "BAD", "created_at": datetime.datetime.now(datetime.UTC)}]

    updater = OrderUpdater(configuration=OrderUpdaterConfiguration(tenant="desk_a"))
    updater._local_persist_manager = SimpleNamespace(persist_records=lambda **kwargs: None)

    with pytest.raises(TypeError, match="must return OrderRow instances"):
        updater._execute_local_update(historical_update=None)


def test_simple_table_updater_set_update_statistics_is_passthrough():
    class OrderUpdater(SimpleTableUpdater):
        SIMPLE_TABLE_SCHEMA = OrderRow

        def set_data_source(self, data_source=None):
            self._data_source = _FakeDataSource()

        def update(self):
            return []

    updater = OrderUpdater(configuration=OrderUpdaterConfiguration(tenant="desk_a"))
    update_statistics = UpdateStatistics()

    returned = updater._set_update_statistics(update_statistics)

    assert returned is update_statistics
    assert updater.update_statistics is update_statistics


def test_simple_table_historical_update_statistics_is_optional():
    historical_cls = client_simple_table_models.SimpleTableUpdateHistorical

    assert historical_cls.model_fields["update_statistics"].is_required() is False
    assert historical_cls.model_fields["must_update"].is_required() is False
    assert historical_cls.model_fields["direct_dependencies_ids"].is_required() is False

    historical = historical_cls(
        update_time_start=datetime.datetime.now(datetime.UTC),
        related_table=1,
    )

    assert historical.update_statistics is None
    assert historical.must_update is None
    assert historical.direct_dependencies_ids is None


def test_simple_table_updater_resolves_foreign_keys_to_storage_ids(monkeypatch):
    captured_storage_calls: list[dict[str, object]] = []

    class CustomerRow(SimpleTable):
        id: int
        customer_code: Annotated[str, Index(unique=True)] = Field(...)

    class BalanceRow(SimpleTable):
        id: int
        customer_id: Annotated[
            int,
            ForeignKey(CustomerRow, on_delete="cascade"),
            Index(),
            Ops(filter=True),
        ] = Field(...)
        balance_usd: float

    def fake_storage_get_or_create(cls, **kwargs):
        captured_storage_calls.append(kwargs)
        schema_model = kwargs["schema"]["model"]
        storage_id = 101 if schema_model.endswith("CustomerRow") else 202
        return client_simple_table_models.SimpleTableStorage(
            id=storage_id,
            storage_hash=kwargs["storage_hash"],
            data_source=kwargs["data_source"],
            build_configuration=kwargs["build_configuration"],
            build_configuration_json_schema=kwargs["build_configuration_json_schema"],
            time_serie_source_code_git_hash=kwargs["source_code_git_hash"],
            time_serie_source_code=kwargs["source_code"],
            schema=kwargs["schema"],
            source_class_name=kwargs["source_class_name"],
            open_for_everyone=kwargs["open_to_public"],
        )

    monkeypatch.setattr(
        client_simple_table_models.SimpleTableStorage,
        "get_or_create",
        classmethod(fake_storage_get_or_create),
    )

    class BalanceUpdater(SimpleTableUpdater):
        SIMPLE_TABLE_SCHEMA = BalanceRow

        def set_data_source(self, data_source=None):
            self._data_source = _FakeDataSource()

        def update(self):
            return []

    updater = BalanceUpdater(configuration=OrderUpdaterConfiguration(tenant="desk_a"))

    resolved_schema = updater.local_initial_configuration["simple_table_schema"]
    foreign_key = next(
        field["foreign_key"]
        for field in resolved_schema["fields"]
        if field["name"] == "customer_id"
    )

    assert any(
        storage_call["schema"]["model"].endswith("CustomerRow")
        for storage_call in captured_storage_calls
    )
    assert foreign_key == {"target": 101, "on_delete": "cascade"}


def test_simple_table_updater_recursively_resolves_nested_foreign_keys(monkeypatch):
    captured_storage_calls: list[dict[str, object]] = []

    class CountryRow(SimpleTable):
        id: int
        country_code: Annotated[str, Index(unique=True)] = Field(...)

    class CustomerRow(SimpleTable):
        id: int
        country_id: Annotated[
            int,
            ForeignKey(CountryRow, on_delete="restrict"),
            Index(),
        ] = Field(...)
        customer_code: Annotated[str, Index(unique=True)] = Field(...)

    class BalanceRow(SimpleTable):
        id: int
        customer_id: Annotated[
            int,
            ForeignKey(CustomerRow, on_delete="cascade"),
            Index(),
            Ops(filter=True),
        ] = Field(...)
        balance_usd: float

    storage_ids = {
        "CountryRow": 301,
        "CustomerRow": 302,
        "BalanceRow": 303,
    }

    def fake_storage_get_or_create(cls, **kwargs):
        captured_storage_calls.append(kwargs)
        model_name = kwargs["schema"]["model"].split(".")[-1]
        return client_simple_table_models.SimpleTableStorage(
            id=storage_ids[model_name],
            storage_hash=kwargs["storage_hash"],
            data_source=kwargs["data_source"],
            build_configuration=kwargs["build_configuration"],
            build_configuration_json_schema=kwargs["build_configuration_json_schema"],
            time_serie_source_code_git_hash=kwargs["source_code_git_hash"],
            time_serie_source_code=kwargs["source_code"],
            schema=kwargs["schema"],
            source_class_name=kwargs["source_class_name"],
            open_for_everyone=kwargs["open_to_public"],
        )

    monkeypatch.setattr(
        client_simple_table_models.SimpleTableStorage,
        "get_or_create",
        classmethod(fake_storage_get_or_create),
    )

    class BalanceUpdater(SimpleTableUpdater):
        SIMPLE_TABLE_SCHEMA = BalanceRow

        def set_data_source(self, data_source=None):
            self._data_source = _FakeDataSource()

        def update(self):
            return []

    updater = BalanceUpdater(configuration=OrderUpdaterConfiguration(tenant="desk_a"))

    resolved_schema = updater.local_initial_configuration["simple_table_schema"]
    balance_fk = next(
        field["foreign_key"]
        for field in resolved_schema["fields"]
        if field["name"] == "customer_id"
    )
    customer_storage_call = next(
        storage_call
        for storage_call in captured_storage_calls
        if storage_call["schema"]["model"].endswith("CustomerRow")
    )
    customer_fk = next(
        field["foreign_key"]
        for field in customer_storage_call["schema"]["fields"]
        if field["name"] == "country_id"
    )

    assert any(
        storage_call["schema"]["model"].endswith("CountryRow")
        for storage_call in captured_storage_calls
    )
    assert any(
        storage_call["schema"]["model"].endswith("CustomerRow")
        for storage_call in captured_storage_calls
    )
    assert balance_fk == {"target": 302, "on_delete": "cascade"}
    assert customer_fk == {"target": 301, "on_delete": "restrict"}
