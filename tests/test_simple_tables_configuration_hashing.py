from __future__ import annotations

import os
from typing import Annotated

from pydantic import BaseModel, Field

os.environ.setdefault("TDAG_ENDPOINT", "http://testserver")
os.environ.setdefault("MAINSEQUENCE_ACCESS_TOKEN", "test-access-token")
os.environ.setdefault("MAINSEQUENCE_REFRESH_TOKEN", "test-refresh-token")

from mainsequence.tdag.simple_tables import (
    Index,
    SimpleTable,
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


class OrderRowWithDesk(SimpleTable):
    id: int
    order_code: Annotated[str, Index(unique=True)] = Field(...)
    desk: str


def test_simple_table_runtime_only_value_changes_neither_hash():
    class TableConfig(SimpleTableUpdaterConfiguration):
        identifier: str
        label: str = Field(..., json_schema_extra={"runtime_only": True})

    class OrderUpdater(SimpleTableUpdater):
        SIMPLE_TABLE_SCHEMA = OrderRow

        def set_data_source(self, data_source=None):
            self._data_source = _FakeDataSource()

        def update(self):
            return None

    hashes_a = OrderUpdater(
        configuration=TableConfig(identifier="orders", label="Client Orders")
    ).hashes()
    hashes_b = OrderUpdater(
        configuration=TableConfig(identifier="orders", label="Orders")
    ).hashes()

    assert hashes_a == hashes_b


def test_simple_table_update_only_value_changes_update_hash_but_not_storage_hash():
    class TableConfig(SimpleTableUpdaterConfiguration):
        tenant: str = Field(..., json_schema_extra={"update_only": True})
        identifier: str

    class OrderUpdater(SimpleTableUpdater):
        SIMPLE_TABLE_SCHEMA = OrderRow

        def set_data_source(self, data_source=None):
            self._data_source = _FakeDataSource()

        def update(self):
            return None

    update_hash_a, storage_hash_a = OrderUpdater(
        configuration=TableConfig(tenant="desk_a", identifier="orders")
    ).hashes()
    update_hash_b, storage_hash_b = OrderUpdater(
        configuration=TableConfig(tenant="desk_b", identifier="orders")
    ).hashes()

    assert update_hash_a != update_hash_b
    assert storage_hash_a == storage_hash_b


def test_simple_table_schema_changes_storage_hash():
    class TableConfig(SimpleTableUpdaterConfiguration):
        identifier: str

    class OrderUpdater(SimpleTableUpdater):
        SIMPLE_TABLE_SCHEMA = OrderRow

        def set_data_source(self, data_source=None):
            self._data_source = _FakeDataSource()

        def update(self):
            return None

    class OrderUpdaterV2(SimpleTableUpdater):
        SIMPLE_TABLE_SCHEMA = OrderRowWithDesk

        def set_data_source(self, data_source=None):
            self._data_source = _FakeDataSource()

        def update(self):
            return None

    update_hash_a, storage_hash_a = OrderUpdater(configuration=TableConfig(identifier="orders")).hashes()
    update_hash_b, storage_hash_b = OrderUpdaterV2(
        configuration=TableConfig(identifier="orders")
    ).hashes()

    assert update_hash_a != update_hash_b
    assert storage_hash_a != storage_hash_b


def test_simple_table_nested_runtime_only_fields_are_stripped_from_serialized_config():
    class NestedDefinition(BaseModel):
        column_name: str
        dtype: str
        label: str | None = Field(default=None, json_schema_extra={"runtime_only": True})

    class TableConfig(SimpleTableUpdaterConfiguration):
        nested_definitions: list[NestedDefinition]

    config = TableConfig(
        nested_definitions=[
            NestedDefinition(
                column_name="notional",
                dtype="float64",
                label="Notional",
            )
        ]
    )

    update_configuration = config.update_configuration()
    storage_configuration = config.storage_configuration()

    assert update_configuration["serialized_model"]["nested_definitions"][0]["serialized_model"] == {
        "column_name": "notional",
        "dtype": "float64",
    }
    assert storage_configuration["serialized_model"]["nested_definitions"][0]["serialized_model"] == {
        "column_name": "notional",
        "dtype": "float64",
    }
