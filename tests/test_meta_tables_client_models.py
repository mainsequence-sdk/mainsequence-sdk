from __future__ import annotations

import datetime
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

import mainsequence.client.models_metatables as meta_table_models
from mainsequence.meta_tables.compiled_sql.v1 import build_operation, compile_sqlalchemy_statement


class _Response:
    def __init__(self, payload: dict, status_code: int = 200):
        self._payload = payload
        self.status_code = status_code
        self.content = b"{}"

    def json(self):
        return self._payload


def _meta_table_response(**overrides):
    payload = {
        "uid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "data_source_uid": "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        "storage_hash": "mt_example_assets_asset_80390fee13",
        "identifier": "Asset",
        "namespace": "example.assets",
        "description": None,
        "labels": [],
        "management_mode": "platform_managed",
        "physical_table_name": "mt_example_assets_asset_80390fee13",
        "table_contract": {
            "version": "relational-table.v1",
            "physical": {
                "table_name": "mt_example_assets_asset_80390fee13",
            },
            "columns": [
                {
                    "name": "uid",
                    "data_type": "uuid",
                    "nullable": False,
                    "primary_key": True,
                }
            ],
            "indexes": [],
            "foreign_keys": [],
        },
        "contract_version": "relational-table.v1",
        "introspection_snapshot": {},
        "protect_from_deletion": False,
        "columns": [
            {
                "name": "uid",
                "data_type": "uuid",
                "backend_type": "uuid",
                "nullable": False,
                "primary_key": True,
                "unique": False,
                "ordinal_position": 0,
                "contract_fragment": {},
            }
        ],
        "indexes_meta": [],
        "foreign_keys": [],
        "incoming_fks": [],
        "creation_date": "2026-05-25T08:00:00Z",
        "created_by_user_uid": "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
        "organization_owner_uid": "ffffffff-ffff-4fff-8fff-ffffffffffff",
        "open_for_everyone": False,
    }
    payload.update(overrides)
    return payload


def test_meta_table_register_posts_contract_to_meta_table_endpoint(monkeypatch):
    captured = {}

    def fake_make_request(**kwargs):
        captured.update(kwargs)
        return _Response(
            _meta_table_response(registration={"created": True, "mode": "platform_managed"}),
            status_code=201,
        )

    monkeypatch.setattr(meta_table_models, "make_request", fake_make_request)
    monkeypatch.setattr(
        meta_table_models.MetaTable,
        "build_session",
        classmethod(lambda cls: SimpleNamespace(headers={})),
    )

    table = meta_table_models.MetaTable.register(
        data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        management_mode="platform_managed",
        storage_hash="mt_example_assets_asset_80390fee13",
        identifier="Asset",
        namespace="example.assets",
        table_contract=meta_table_models.MetaTableContract(
            physical=meta_table_models.MetaTablePhysicalContract(
                table_name="mt_example_assets_asset_80390fee13",
            ),
            columns=[
                meta_table_models.MetaTableColumnContract(
                    name="uid",
                    data_type="uuid",
                    nullable=False,
                    primary_key=True,
                ),
                meta_table_models.MetaTableColumnContract(
                    name="account_uid",
                    data_type="uuid",
                    nullable=False,
                )
            ],
            foreign_keys=[
                meta_table_models.MetaTableForeignKeyContract(
                    source_columns=["account_uid"],
                    target_meta_table_uid="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
                    target_columns=["uid"],
                    on_delete="restrict",
                )
            ],
        ),
        provisioning={"create_table": True, "if_not_exists": True},
    )

    assert table.uid == "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    assert table.columns[0].name == "uid"
    assert table.registration == {"created": True, "mode": "platform_managed"}
    assert captured["r_type"] == "POST"
    assert captured["url"].endswith("/ts_manager/meta_table/register/")
    assert captured["payload"]["json"]["table_contract"]["physical"] == {
        "table_name": "mt_example_assets_asset_80390fee13",
    }
    assert captured["payload"]["json"]["table_contract"]["foreign_keys"] == [
        {
            "source_columns": ["account_uid"],
            "target_meta_table_uid": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
            "target_columns": ["uid"],
            "on_delete": "restrict",
        }
    ]
    assert captured["payload"]["json"]["provisioning"] == {
        "create_table": True,
        "if_not_exists": True,
    }


def test_meta_table_execute_operation_serializes_scope_uid(monkeypatch):
    captured = {}

    def fake_make_request(**kwargs):
        captured.update(kwargs)
        return _Response({"ok": True, "rows": [{"symbol": "BTC"}]})

    monkeypatch.setattr(meta_table_models, "make_request", fake_make_request)
    monkeypatch.setattr(
        meta_table_models.MetaTable,
        "build_session",
        classmethod(lambda cls: SimpleNamespace(headers={})),
    )

    result = meta_table_models.MetaTable.execute_operation(
        meta_table_models.MetaTableCompiledSQLOperation(
            operation="select",
            statement=meta_table_models.MetaTableStatementPayload(
                sql=(
                    "SELECT asset.symbol FROM public.asset AS asset "
                    "WHERE asset.symbol ILIKE %(symbol_1)s"
                ),
                parameters={"symbol_1": "%BTC%"},
            ),
            scope=meta_table_models.MetaTableOperationScope(
                tables=[
                    meta_table_models.MetaTableOperationScopeTable(
                        metaTableUid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                        alias="asset",
                    )
                ]
            ),
            limits=meta_table_models.MetaTableOperationLimits(
                max_rows=1000,
                statement_timeout_ms=15000,
            ),
        )
    )

    assert result["ok"] is True
    assert captured["r_type"] == "POST"
    assert captured["url"].endswith("/ts_manager/meta_table/execute-operation/")
    assert captured["payload"]["json"]["scope"]["tables"][0] == {
        "meta_table_uid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "alias": "asset",
        "access": "read",
    }
    assert captured["payload"]["json"]["version"] == meta_table_models.COMPILED_SQL_V1
    assert captured["payload"]["json"]["statement"]["parameters"] == {
        "symbol_1": "%BTC%",
    }


def test_meta_table_apply_migration_posts_registry_row_reference(monkeypatch):
    captured = {}

    def fake_make_request(**kwargs):
        captured.update(kwargs)
        return _Response(
            {
                "ok": True,
                "version": meta_table_models.METATABLE_MIGRATION_V1,
                "dry_run": True,
                "migration_meta_table_uid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                "migration_row_uid": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
                "data_source_uid": "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
                "package": "msm",
                "migration_namespace": "markets",
                "revision": "001",
                "direction": "upgrade",
                "previous_revision": None,
                "applied_revision": "001",
                "executed_statement_count": 0,
                "affected_tables": [
                    {
                        "identifier": "msm.markets.models.Asset",
                        "meta_table_uid": "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
                        "physical_table_name": "public.asset",
                        "action": "planned",
                        "storage_hash": "asset_storage",
                        "previous_contract_hash": "c" * 64,
                        "new_contract_hash": "d" * 64,
                        "introspection": {},
                    }
                ],
                "created_meta_table_uids": [],
                "imported_meta_table_uids": [],
                "refreshed_meta_table_uids": ["cccccccc-cccc-4ccc-8ccc-cccccccccccc"],
                "introspection_snapshots": {"msm.markets.models.Asset": {}},
                "registry_update": {
                    "migration_meta_table_uid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                    "migration_row_uid": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
                    "status": "validated",
                },
                "error": None,
            }
        )

    monkeypatch.setattr(meta_table_models, "make_request", fake_make_request)
    monkeypatch.setattr(
        meta_table_models.MetaTable,
        "build_session",
        classmethod(lambda cls: SimpleNamespace(headers={})),
    )

    result = meta_table_models.MetaTable.apply_migration(
        meta_table_models.MetaTableMigrationOperation(
            migrationMetaTableUid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            migrationRowUid="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
            data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
            package="msm",
            migration_namespace="markets",
            revision="001",
            expected_current_revision=None,
            manifest_sha256="a" * 64,
            sql_sha256="b" * 64,
            affected_tables=[
                {
                    "identifier": "msm.markets.models.Asset",
                    "meta_table_uid": "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
                }
            ],
            old_contract_hashes={"msm.markets.models.Asset": "c" * 64},
            new_contract_hashes={"msm.markets.models.Asset": "d" * 64},
            idempotency_key="migration:001",
            lock_key="dddddddd-dddd-4ddd-8ddd-dddddddddddd:msm:markets",
            dry_run=True,
        )
    )

    assert result.ok is True
    assert result.migration_meta_table_uid == "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    assert result.migration_row_uid == "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
    assert result.registry_update.status == "validated"
    assert result.affected_tables[0].identifier == "msm.markets.models.Asset"
    assert captured["r_type"] == "POST"
    assert captured["url"].endswith("/ts_manager/meta_table/apply-migration/")
    assert captured["payload"]["json"]["version"] == meta_table_models.METATABLE_MIGRATION_V1
    assert captured["payload"]["json"]["migration_meta_table_uid"] == (
        "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    )
    assert captured["payload"]["json"]["migration_row_uid"] == (
        "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
    )
    assert captured["payload"]["json"]["dry_run"] is True


def test_meta_table_migration_operation_requires_lowercase_sha256():
    with pytest.raises(ValidationError):
        meta_table_models.MetaTableMigrationOperation(
            migrationMetaTableUid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            migrationRowUid="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
            data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
            package="msm",
            migration_namespace="markets",
            revision="001",
            manifest_sha256="m" * 64,
            sql_sha256="b" * 64,
            idempotency_key="migration:001",
            lock_key="dddddddd-dddd-4ddd-8ddd-dddddddddddd:msm:markets",
        )


def test_meta_table_get_migration_status_posts_registry_scope(monkeypatch):
    captured = {}

    def fake_make_request(**kwargs):
        captured.update(kwargs)
        return _Response(
            {
                "ok": True,
                "migration_meta_table_uid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                "data_source_uid": "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
                "package": "msm",
                "migration_namespace": "markets",
                "current_revision": "001",
                "latest_successful_revision": "001",
                "latest_attempted_revision": "001",
                "rows": [
                    {
                        "migration_row_uid": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
                        "revision": "001",
                        "down_revision": None,
                        "direction": "upgrade",
                        "status": "applied",
                        "previous_revision": None,
                        "applied_revision": "001",
                        "executed_statement_count": 3,
                        "manifest_sha256": "a" * 64,
                        "sql_sha256": "b" * 64,
                        "started_at": "2026-06-01T00:00:00Z",
                        "finished_at": "2026-06-01T00:00:01Z",
                        "error": None,
                    }
                ],
                "error": None,
            }
        )

    monkeypatch.setattr(meta_table_models, "make_request", fake_make_request)
    monkeypatch.setattr(
        meta_table_models.MetaTable,
        "build_session",
        classmethod(lambda cls: SimpleNamespace(headers={})),
    )

    result = meta_table_models.MetaTable.get_migration_status(
        {
            "migrationMetaTableUid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            "data_source_uid": "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
            "package": "msm",
            "migration_namespace": "markets",
        }
    )

    assert result.current_revision == "001"
    assert result.migration_meta_table_uid == "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    assert result.rows[0].migration_row_uid == "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
    assert captured["r_type"] == "POST"
    assert captured["url"].endswith("/ts_manager/meta_table/migration-status/")
    assert captured["payload"]["json"]["migration_meta_table_uid"] == (
        "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    )


def test_meta_table_get_schema_graph_requests_incoming_edges(monkeypatch):
    captured = {}
    graph_payload = {
        "root_uid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "depth": 2,
        "include_incoming": True,
        "nodes": [],
        "edges": [
            {
                "name": "fk_account_asset",
                "source_uid": "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
                "target_uid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                "source_columns": ["asset_uid"],
                "target_columns": ["uid"],
                "on_delete": "restrict",
                "relationship_type": "meta_table_to_meta_table",
            }
        ],
    }

    def fake_make_request(**kwargs):
        captured.update(kwargs)
        return _Response(graph_payload)

    monkeypatch.setattr(meta_table_models, "make_request", fake_make_request)
    monkeypatch.setattr(
        meta_table_models.MetaTable,
        "build_session",
        classmethod(lambda cls: SimpleNamespace(headers={})),
    )

    table = meta_table_models.MetaTable(**_meta_table_response())
    result = table.get_schema_graph(depth=2, include_incoming=True, timeout=15)

    assert result == graph_payload
    assert captured["r_type"] == "GET"
    assert captured["url"].endswith(
        "/ts_manager/meta_table/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa/schema-graph/"
    )
    assert captured["payload"]["params"] == {
        "depth": 2,
        "include_incoming": True,
    }
    assert captured["time_out"] == 15


def test_compiled_sql_v1_protocol_is_validated_by_pydantic():
    operation = build_operation(
        operation="select",
        sql="SELECT asset.symbol FROM public.asset AS asset",
        parameters={"symbol_1": "%BTC%"},
        scope={
            "tables": [
                {
                    "metaTableUid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                    "alias": "asset",
                }
            ]
        },
        limits={"max_rows": 1000, "statement_timeout_ms": 15000},
    )

    assert isinstance(operation, meta_table_models.MetaTableCompiledSQLOperation)
    assert operation.version == "compiled-sql.v1"
    assert operation.dialect == "postgresql"
    assert operation.statement.paramstyle == "pyformat"
    assert operation.scope.tables[0].meta_table_uid == "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"

    with pytest.raises(ValidationError):
        meta_table_models.MetaTableCompiledSQLOperation(
            operation="select",
            version="compiled-sql.v2",
            statement=meta_table_models.MetaTableStatementPayload(sql="SELECT 1"),
            scope=meta_table_models.MetaTableOperationScope(
                tables=[
                    meta_table_models.MetaTableOperationScopeTable(
                        metaTableUid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                    )
                ]
            ),
        )

    with pytest.raises(ValidationError):
        meta_table_models.MetaTableCompiledSQLOperation(
            operation="select",
            statement=meta_table_models.MetaTableStatementPayload(sql="SELECT 1"),
            scope=meta_table_models.MetaTableOperationScope(tables=[]),
        )


def test_compiled_sql_v1_serializes_typed_temporal_parameters():
    operation = build_operation(
        operation="select",
        sql="SELECT * FROM asset WHERE as_of = %(as_of)s AND seen_at = %(seen_at)s",
        parameters={
            "as_of": datetime.date(2026, 5, 28),
            "seen_at": datetime.datetime(2026, 5, 28, 12, 30, tzinfo=datetime.UTC),
        },
        parameter_types={
            "as_of": "date",
            "seen_at": "datetime64[ns, UTC]",
        },
        scope={
            "tables": [
                {
                    "metaTableUid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                    "alias": "asset",
                }
            ]
        },
    )

    assert operation.statement.parameter_types == {
        "as_of": "date",
        "seen_at": "timestamp with time zone",
    }
    assert operation.statement.parameters == {
        "as_of": "2026-05-28",
        "seen_at": "2026-05-28T12:30:00Z",
    }


def test_compiled_sql_v1_rejects_untyped_temporal_parameters():
    with pytest.raises(ValidationError, match="parameter_types"):
        build_operation(
            operation="select",
            sql="SELECT * FROM asset WHERE as_of = %(as_of)s",
            parameters={"as_of": datetime.date(2026, 5, 28)},
            scope={
                "tables": [
                    {
                        "metaTableUid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                        "alias": "asset",
                    }
                ]
            },
        )


def test_compile_sqlalchemy_statement_emits_temporal_parameter_types():
    sqlalchemy = pytest.importorskip("sqlalchemy")

    statement = sqlalchemy.select(
        sqlalchemy.bindparam(
            "seen_at",
            datetime.datetime(2026, 5, 28, 12, 30, tzinfo=datetime.UTC),
            type_=sqlalchemy.DateTime(timezone=True),
        )
    )

    operation = compile_sqlalchemy_statement(
        statement,
        operation="select",
        scope_tables=[
            {
                "metaTableUid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                "alias": "asset",
            }
        ],
    )

    assert operation.statement.parameter_types == {
        "seen_at": "timestamp with time zone",
    }
    assert operation.statement.parameters["seen_at"] == "2026-05-28T12:30:00Z"


def test_compile_sqlalchemy_statement_rejects_naive_datetime_bind_types():
    sqlalchemy = pytest.importorskip("sqlalchemy")

    statement = sqlalchemy.select(
        sqlalchemy.bindparam(
            "seen_at",
            datetime.datetime(2026, 5, 28, 12, 30),
            type_=sqlalchemy.DateTime(timezone=False),
        )
    )

    with pytest.raises(ValueError, match="Timezone-naive"):
        compile_sqlalchemy_statement(
            statement,
            operation="select",
            scope_tables=[
                {
                    "metaTableUid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                    "alias": "asset",
                }
            ],
        )


def test_meta_table_introspect_updates_local_snapshot(monkeypatch):
    captured = {}
    table = meta_table_models.MetaTable(**_meta_table_response())

    def fake_make_request(**kwargs):
        captured.update(kwargs)
        return _Response(
            {
                "meta_table_uid": table.uid,
                "introspection_snapshot": {
                    "columns": [{"name": "uid", "backend_type": "uuid"}],
                },
            }
        )

    monkeypatch.setattr(meta_table_models, "make_request", fake_make_request)
    monkeypatch.setattr(
        meta_table_models.MetaTable,
        "build_session",
        classmethod(lambda cls: SimpleNamespace(headers={})),
    )

    response = table.introspect()

    assert response["meta_table_uid"] == table.uid
    assert table.introspection_snapshot == {
        "columns": [{"name": "uid", "backend_type": "uuid"}],
    }
    assert captured["url"].endswith(f"/ts_manager/meta_table/{table.uid}/introspect/")


def test_meta_table_is_exported_from_client_package():
    from mainsequence.client import MetaTable

    assert MetaTable is meta_table_models.MetaTable
