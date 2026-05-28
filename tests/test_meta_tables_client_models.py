from __future__ import annotations

import datetime
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

import mainsequence.client.models_metatables as meta_table_models
from mainsequence.tdag.meta_tables import (
    build_compiled_sql_v1_operation,
    compile_sqlalchemy_statement,
)


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


def test_compiled_sql_v1_protocol_is_validated_by_pydantic():
    operation = build_compiled_sql_v1_operation(
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
    operation = build_compiled_sql_v1_operation(
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
        build_compiled_sql_v1_operation(
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
