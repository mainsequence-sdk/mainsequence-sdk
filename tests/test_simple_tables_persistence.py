from __future__ import annotations

import datetime
import os
from typing import Annotated

import pytest
from pydantic import Field

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


class _FakeResponse:
    def __init__(self, status_code: int, payload=None):
        self.status_code = status_code
        self._payload = payload
        self.text = ""
        self.content = b"" if payload is None else b"ok"

    def json(self):
        return self._payload


class OrderRow(SimpleTable):
    id: int
    order_code: Annotated[str, Index(unique=True)] = Field(...)
    created_at: datetime.datetime


class OrderNodeConfig(SimpleTableUpdaterConfiguration):
    tenant: str = Field(..., json_schema_extra={"update_only": True})


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


def test_simple_table_insert_records_posts_to_model_viewset(monkeypatch):
    captured: dict[str, object] = {}

    def fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured["r_type"] = r_type
        captured["url"] = url
        captured["payload"] = payload
        return _FakeResponse(
            201,
            [
                {
                    "id": 1,
                    "order_code": "A-1",
                    "created_at": "2026-03-22T10:00:00Z",
                }
            ],
        )

    monkeypatch.setattr(client_simple_table_models, "make_request", fake_make_request)

    rows = OrderRow.insert_records(
        [
            OrderRow(
                id=1,
                order_code="A-1",
                created_at=datetime.datetime(2026, 3, 22, 10, 0, tzinfo=datetime.UTC),
            )
        ]
    )

    assert captured["r_type"] == "POST"
    assert captured["url"] == f"{API_ENDPOINT}/ts_manager/simple_tables/"
    assert captured["payload"] == {
        "json": [
            {
                "id": 1,
                "order_code": "A-1",
                "created_at": "2026-03-22T10:00:00Z",
            }
        ]
    }
    assert len(rows) == 1
    assert isinstance(rows[0], OrderRow)
    assert rows[0].order_code == "A-1"


def test_simple_table_upsert_records_uses_upsert_action(monkeypatch):
    captured: dict[str, object] = {}

    def fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured["url"] = url
        return _FakeResponse(
            200,
            [
                {
                    "id": 2,
                    "order_code": "B-2",
                    "created_at": "2026-03-22T11:00:00Z",
                }
            ],
        )

    monkeypatch.setattr(client_simple_table_models, "make_request", fake_make_request)

    rows = OrderRow.upsert_records(
        [
            {
                "id": 2,
                "order_code": "B-2",
                "created_at": datetime.datetime(2026, 3, 22, 11, 0, tzinfo=datetime.UTC),
            }
        ]
    )

    assert captured["url"] == f"{API_ENDPOINT}/ts_manager/simple_tables_update/"
    assert rows[0].id == 2


def test_simple_table_delete_uses_detail_delete(monkeypatch):
    captured: dict[str, object] = {}

    def fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured["r_type"] = r_type
        captured["url"] = url
        return _FakeResponse(204)

    monkeypatch.setattr(client_simple_table_models, "make_request", fake_make_request)

    row = OrderRow(
        id=9,
        order_code="DEL-9",
        created_at=datetime.datetime(2026, 3, 22, 12, 0, tzinfo=datetime.UTC),
    )
    row.delete()

    assert captured["r_type"] == "DELETE"
    assert captured["url"] == f"{API_ENDPOINT}/ts_manager/simple_tables/9/"


def test_simple_table_persist_manager_and_node_delegate_to_schema_methods(monkeypatch):
    inserted: list[list[OrderRow]] = []
    upserted: list[list[OrderRow]] = []
    deleted: list[object] = []

    def fake_insert_records(cls, records, *, timeout=None):
        inserted.append(list(records))
        return list(records)

    def fake_upsert_records(cls, records, *, timeout=None):
        upserted.append(list(records))
        return list(records)

    def fake_delete_by_id(cls, record_id, *, timeout=None):
        deleted.append(record_id)

    monkeypatch.setattr(OrderRow, "insert_records", classmethod(fake_insert_records))
    monkeypatch.setattr(OrderRow, "upsert_records", classmethod(fake_upsert_records))
    monkeypatch.setattr(OrderRow, "delete_by_id", classmethod(fake_delete_by_id))

    manager = SimpleTablePersistManager(simple_table_schema=OrderRow)
    record = OrderRow(
        id=7,
        order_code="M-7",
        created_at=datetime.datetime(2026, 3, 22, 13, 0, tzinfo=datetime.UTC),
    )

    assert manager.insert_records([record]) == [record]
    assert manager.upsert_records([record]) == [record]
    manager.delete(record)
    manager.delete(17)

    class OrderNode(SimpleTableUpdater):
        SIMPLE_TABLE_SCHEMA = OrderRow

        def __init__(self):
            super().__init__(configuration=OrderNodeConfig(tenant="desk_a"))

        def update(self):
            return None

    node = OrderNode()
    assert node.insert_records([record]) == [record]
    assert node.upsert_records([record]) == [record]
    node.delete_record(23)

    assert inserted == [[record], [record]]
    assert upserted == [[record], [record]]
    assert deleted == [7, 17, 23]


def test_simple_table_node_requires_class_level_schema():
    class InvalidNode(SimpleTableUpdater):
        def update(self):
            return None

    with pytest.raises(TypeError, match="SIMPLE_TABLE_SCHEMA"):
        InvalidNode(configuration=OrderNodeConfig(tenant="desk_a"))
