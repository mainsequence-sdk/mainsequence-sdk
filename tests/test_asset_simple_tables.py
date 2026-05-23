from __future__ import annotations

import os
from types import SimpleNamespace

import pytest

os.environ.setdefault("MAINSEQUENCE_ACCESS_TOKEN", "test-token")
os.environ.setdefault("MAINSEQUENCE_REFRESH_TOKEN", "test-refresh-token")

from mainsequence.markets.assets.simple_tables import (
    AssetCategoryMembershipSimpleTableUpdater,
    AssetCategorySimpleTable,
)


class _FakeStorage:
    def __init__(self, result: dict | None = None):
        self.result = result or {
            "ok": True,
            "results": [{"deleted_count": 0, "inserted_count": 0}],
        }
        self.calls: list[dict[str, object]] = []

    def run_query(
        self,
        sql: str,
        *,
        max_rows: int | None = None,
        statement_timeout_ms: int | None = None,
        timeout: int | None = None,
    ) -> dict:
        self.calls.append(
            {
                "sql": sql,
                "max_rows": max_rows,
                "statement_timeout_ms": statement_timeout_ms,
                "timeout": timeout,
            }
        )
        return self.result


def _membership_updater(storage: _FakeStorage) -> AssetCategoryMembershipSimpleTableUpdater:
    updater = object.__new__(AssetCategoryMembershipSimpleTableUpdater)
    updater.storage_hash = 'asset_category_memberships"hash'
    updater._local_persist_manager = SimpleNamespace(data_node_storage=storage)
    updater.list_assets_for_category = lambda **kwargs: [{"category_id": kwargs["category_id"]}]
    return updater


def _normalized_sql(sql: str) -> str:
    return " ".join(sql.split())


def test_asset_category_update_assets_replaces_memberships_with_sql():
    storage = _FakeStorage()
    updater = _membership_updater(storage)

    result = updater.update_assets(
        category_id=7,
        asset_ids=[10, 11, 10],
        statement_timeout_ms=1500,
        timeout=30,
    )

    assert result == [{"category_id": 7}]
    assert len(storage.calls) == 1
    call = storage.calls[0]
    sql = _normalized_sql(str(call["sql"]))
    assert 'DELETE FROM "asset_category_memberships""hash"' in sql
    assert 'WHERE "category" = 7' in sql
    assert (
        'INSERT INTO "asset_category_memberships""hash" '
        '("category", "asset") VALUES (7, 10), (7, 11)'
        in sql
    )
    assert call["max_rows"] == 1
    assert call["statement_timeout_ms"] == 1500
    assert call["timeout"] == 30


def test_asset_category_update_assets_can_clear_memberships_with_sql():
    storage = _FakeStorage()
    updater = _membership_updater(storage)

    updater.update_assets(category_id=7, asset_ids=[])

    sql = _normalized_sql(str(storage.calls[0]["sql"]))
    assert 'DELETE FROM "asset_category_memberships""hash"' in sql
    assert 'WHERE "category" = 7' in sql
    assert "INSERT INTO" not in sql
    assert "0 AS inserted_count" in sql


def test_asset_category_row_update_assets_delegates_to_membership_updater():
    category = AssetCategorySimpleTable(
        id=7,
        uid="category-uid",
        unique_identifier="category",
        display_name="Category",
    )
    captured: dict[str, object] = {}

    class _MembershipUpdater:
        def update_assets(self, **kwargs):
            captured.update(kwargs)
            return ["updated"]

    assert category.update_assets(
        _MembershipUpdater(),
        asset_ids=[1, 2],
        statement_timeout_ms=1000,
        timeout=10,
    ) == ["updated"]
    assert captured == {
        "category": category,
        "assets": None,
        "asset_ids": [1, 2],
        "asset_unique_identifiers": None,
        "statement_timeout_ms": 1000,
        "timeout": 10,
    }


def test_asset_category_update_assets_raises_on_sql_error_envelope():
    storage = _FakeStorage(
        {
            "ok": False,
            "error": {"message": "Only SELECT/WITH/EXPLAIN queries are allowed."},
        }
    )
    updater = _membership_updater(storage)

    with pytest.raises(RuntimeError, match="Only SELECT/WITH/EXPLAIN"):
        updater.update_assets(category_id=7, asset_ids=[10])
