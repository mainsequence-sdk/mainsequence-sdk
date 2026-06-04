from __future__ import annotations

import datetime
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

import mainsequence.client.metatables as meta_table_models
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
        "creation_date": "2026-05-25T08:00:00Z",
        "created_by_user_uid": "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
        "organization_owner_uid": "ffffffff-ffff-4fff-8fff-ffffffffffff",
    }
    payload.update(overrides)
    return payload


def test_metatable_identifier_field_descriptions_state_org_global_uniqueness():
    expected = "globally unique per organization"

    assert (
        expected
        in meta_table_models.MetaTableRegistrationRequest.model_fields["identifier"].description
    )
    assert expected in meta_table_models.MetaTable.model_fields["identifier"].description
    assert (
        expected
        in meta_table_models.TimeIndexMetaTableRegistrationRequest.model_fields[
            "identifier"
        ].description
    )
    assert expected in meta_table_models.TableMetaData.model_fields["identifier"].description


def test_managed_reservation_response_accepts_backend_contract_shape():
    response = meta_table_models.ManagedMetaTableReservationResponse(
        ok=True,
        version="managed-metatable-reservation.v1",
        tables=[
            {
                "identifier": "mainsequence.examples.Asset",
                "meta_table_uid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                "data_source_uid": "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
                "management_mode": "platform_managed",
                "provisioning_status": "reserved",
                "schema_management_mode": "alembic_managed",
                "migration_package": "msm",
                "migration_namespace": "markets",
                "migration_provider_key": "msm:markets",
                "alembic_version_meta_table_uid": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
                "storage_hash": "asset",
                "physical_table_name": "asset",
                "created": True,
                "matched_by": None,
                "table_contract": {
                    "version": "relational-table.v1",
                    "physical": {"table_name": "asset"},
                    "columns": [],
                },
            }
        ],
    )

    assert response.ok is True
    item = response.tables[0]
    assert item.created is True
    assert item.matched_by is None
    assert item.schema_management_mode == "alembic_managed"
    assert item.migration_provider_key == "msm:markets"
    assert not hasattr(item, "existing")
    assert (
        "TS Manager accepted"
        in meta_table_models.ManagedMetaTableReservationResponse.model_fields["ok"].description
    )
    assert (
        "Physical table name reserved"
        in meta_table_models.ManagedMetaTableReservationItem.model_fields[
            "physical_table_name"
        ].description
    )
    response_without_version = meta_table_models.ManagedMetaTableReservationResponse(
        ok=True,
        tables=[
            {
                "identifier": "mainsequence.examples.Asset",
                "meta_table_uid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                "data_source_uid": "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
                "management_mode": "platform_managed",
                "provisioning_status": "reserved",
                "schema_management_mode": "alembic_managed",
                "migration_package": "msm",
                "migration_namespace": "markets",
                "migration_provider_key": "msm:markets",
                "alembic_version_meta_table_uid": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
                "storage_hash": "asset",
                "physical_table_name": "asset",
                "created": True,
                "matched_by": None,
                "table_contract": {
                    "version": "relational-table.v1",
                    "physical": {"table_name": "asset"},
                    "columns": [],
                },
            }
        ],
    )
    assert response.version == "managed-metatable-reservation.v1"
    assert response_without_version.version is None
    assert "version" not in meta_table_models.ManagedMetaTableReservationRequest.model_fields
    assert (
        "data_source_uid" not in meta_table_models.ManagedMetaTableReservationRequest.model_fields
    )
    assert (
        "migration_package"
        in meta_table_models.ManagedMetaTableReservationRequest.model_fields
    )
    assert (
        "Public UID of the DynamicTableDataSource"
        in meta_table_models.ManagedMetaTableReservationTable.model_fields[
            "data_source_uid"
        ].description
    )
    assert "management_mode" not in meta_table_models.ManagedMetaTableReservationTable.model_fields
    assert "schema_management" not in meta_table_models.ManagedMetaTableReservationTable.model_fields
    assert (
        "protect_from_deletion"
        not in meta_table_models.ManagedMetaTableReservationTable.model_fields
    )
    request = meta_table_models.ManagedMetaTableReservationRequest(
        migration_package="msm",
        migration_namespace="markets",
        migration_provider_key="msm:markets",
        tables=[
            meta_table_models.ManagedMetaTableReservationTable(
                identifier="mainsequence.examples.Asset",
                data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
                storage_hash="asset",
                table_contract={
                    "version": "relational-table.v1",
                    "physical": {},
                    "columns": [],
                },
            )
        ],
    )
    request_payload = request.model_dump(mode="json", exclude_none=True)
    assert "version" not in request_payload
    assert "data_source_uid" not in request_payload
    assert request_payload["migration_package"] == "msm"
    assert request_payload["migration_namespace"] == "markets"
    assert request_payload["migration_provider_key"] == "msm:markets"
    assert request_payload["tables"][0]["data_source_uid"] == "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
    assert "schema_management" not in request_payload["tables"][0]
    assert "protect_from_deletion" not in request_payload["tables"][0]


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
    assert "foreign_keys" not in captured["payload"]["json"]["table_contract"]
    assert "indexes" not in captured["payload"]["json"]["table_contract"]
    assert captured["payload"]["json"]["provisioning"] == {
        "create_table": True,
        "if_not_exists": True,
    }


def test_meta_table_filter_by_body_posts_identifier_filters(monkeypatch):
    captured = {}

    def fake_make_request(**kwargs):
        captured.update(kwargs)
        return _Response(
            {
                "count": 1,
                "next": None,
                "previous": None,
                "results": [_meta_table_response(identifier="Asset")],
            }
        )

    monkeypatch.setattr(meta_table_models, "make_request", fake_make_request)
    monkeypatch.setattr(
        meta_table_models.MetaTable,
        "build_session",
        classmethod(lambda cls: SimpleNamespace(headers={})),
    )

    rows = meta_table_models.MetaTable.filter_by_body(
        identifiers=["mainsequence.examples.Asset"],
        limit=1,
    )

    assert len(rows) == 1
    assert rows[0].identifier == "Asset"
    assert captured["r_type"] == "POST"
    assert captured["url"].endswith("/ts_manager/meta_table/filter/")
    assert captured["payload"]["json"] == {
        "identifiers": ["mainsequence.examples.Asset"],
        "limit": 1,
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


def test_dynamic_table_data_source_issue_migration_connection_posts_scope(monkeypatch):
    captured = {}

    def fake_make_request(**kwargs):
        captured.update(kwargs)
        return _Response(
            {
                "ok": True,
                "data_source_uid": "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
                "dialect": "postgresql",
                "credential_kind": "runtime",
                "role_name": "ms_runtime",
                "owner_role_name": "ms_owner",
                "expires_at": "2026-06-02T12:00:00Z",
                "uri": "postgresql://temporary-secret",
            }
        )

    monkeypatch.setattr(meta_table_models, "make_request", fake_make_request)
    monkeypatch.setattr(
        meta_table_models.DynamicTableDataSource,
        "build_session",
        classmethod(lambda cls: SimpleNamespace(headers={})),
    )

    data_source = meta_table_models.DynamicTableDataSource(
        uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        id=1,
        related_resource={
            "uid": "physical-data-source",
            "data_source_uid": "physical-data-source",
            "id": 10,
            "display_name": "project-db",
            "organization": None,
            "organization_uid": None,
            "class_type": "POSTGRES",
            "status": "AVAILABLE",
        },
        related_resource_class_type="POSTGRES",
    )
    result = data_source.issue_migration_connection(
        meta_table_models.DynamicTableDataSourceMigrationConnectionRequest(
            package="msm",
            migration_namespace="markets",
            meta_table_uids=["aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"],
            ttl_seconds=60,
        )
    )

    assert result.ok is True
    assert result.uri == "postgresql://temporary-secret"
    assert result.owner_role_name == "ms_owner"
    assert captured["r_type"] == "POST"
    assert captured["url"].endswith(
        "/ts_manager/dynamic_table_data_source/"
        "dddddddd-dddd-4ddd-8ddd-dddddddddddd/migration-connection/"
    )
    assert captured["payload"]["json"] == {
        "purpose": "schema_migration",
        "package": "msm",
        "migration_namespace": "markets",
        "meta_table_uids": ["aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"],
        "ttl_seconds": 60,
    }


def test_meta_table_reserve_managed_posts_reservation_payload(monkeypatch):
    captured = {}

    def fake_make_request(**kwargs):
        captured.update(kwargs)
        return _Response(
            {
                "ok": True,
                "version": "managed-metatable-reservation.v1",
                "tables": [
                    {
                        "identifier": "Asset",
                        "namespace": "example.assets",
                        "meta_table_uid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                        "data_source_uid": "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
                        "management_mode": "platform_managed",
                        "provisioning_status": "reserved",
                        "schema_management_mode": "alembic_managed",
                        "migration_package": "msm",
                        "migration_namespace": "markets",
                        "migration_provider_key": "msm:markets",
                        "alembic_version_meta_table_uid": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
                        "storage_hash": "mt_asset_hash",
                        "physical_table_name": "mt_asset_physical",
                        "table_contract": {
                            "version": "relational-table.v1",
                            "physical": {"table_name": "mt_asset_physical"},
                            "columns": [],
                        },
                        "created": True,
                        "matched_by": None,
                    }
                ],
            }
        )

    monkeypatch.setattr(meta_table_models, "make_request", fake_make_request)
    monkeypatch.setattr(
        meta_table_models.MetaTable,
        "build_session",
        classmethod(lambda cls: SimpleNamespace(headers={})),
    )

    result = meta_table_models.MetaTable.reserve_managed(
        meta_table_models.ManagedMetaTableReservationRequest(
            migration_package="msm",
            migration_namespace="markets",
            migration_provider_key="msm:markets",
            tables=[
                meta_table_models.ManagedMetaTableReservationTable(
                    identifier="Asset",
                    namespace="example.assets",
                    data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
                    storage_hash="mt_asset_hash",
                    table_contract={
                        "version": "relational-table.v1",
                        "physical": {},
                        "columns": [],
                    },
                )
            ],
        )
    )

    assert result.tables[0].meta_table_uid == "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    assert result.tables[0].physical_table_name == "mt_asset_physical"
    assert captured["r_type"] == "POST"
    assert captured["url"].endswith("/ts_manager/meta_table/reserve-managed/")
    assert "version" not in captured["payload"]["json"]
    assert "data_source_uid" not in captured["payload"]["json"]
    assert captured["payload"]["json"]["migration_package"] == "msm"
    assert captured["payload"]["json"]["migration_namespace"] == "markets"
    assert captured["payload"]["json"]["migration_provider_key"] == "msm:markets"
    assert (
        captured["payload"]["json"]["tables"][0]["data_source_uid"]
        == "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
    )
    assert "management_mode" not in captured["payload"]["json"]["tables"][0]
    assert "schema_management" not in captured["payload"]["json"]["tables"][0]
    assert "protect_from_deletion" not in captured["payload"]["json"]["tables"][0]
    assert result.tables[0].management_mode == "platform_managed"
    assert result.tables[0].schema_management_mode == "alembic_managed"
    assert result.tables[0].migration_provider_key == "msm:markets"
    assert "indexes" not in captured["payload"]["json"]["tables"][0]["table_contract"]
    assert "foreign_keys" not in captured["payload"]["json"]["tables"][0]["table_contract"]


def test_meta_table_finalize_managed_posts_finalize_payload(monkeypatch):
    captured = {}

    def fake_make_request(**kwargs):
        captured.update(kwargs)
        return _Response(
            {
                "ok": True,
                "finalized_count": 1,
                "active_count": 1,
                "reserved_count": 0,
                "failed_count": 0,
                "tables": [
                    {
                        "meta_table_uid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                        "identifier": "Asset",
                        "storage_hash": "mt_asset_hash",
                        "physical_table_name": "mt_asset_physical",
                        "previous_provisioning_status": "reserved",
                        "provisioning_status": "active",
                        "schema_management_mode": "alembic_managed",
                        "migration_package": "msm",
                        "migration_namespace": "markets",
                        "migration_provider_key": "msm:markets",
                        "alembic_version_meta_table_uid": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
                        "alembic_revision": "0001",
                        "table_kind": "relational",
                        "time_indexed": False,
                        "finalized": True,
                        "physical_table_exists": True,
                    }
                ],
            }
        )

    monkeypatch.setattr(meta_table_models, "make_request", fake_make_request)
    monkeypatch.setattr(
        meta_table_models.MetaTable,
        "build_session",
        classmethod(lambda cls: SimpleNamespace(headers={})),
    )

    response = meta_table_models.MetaTable.finalize_managed(
        meta_table_models.ManagedMetaTableFinalizeRequest(
            meta_table_uids=["aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"],
            migration_package="msm",
            migration_namespace="markets",
            migration_provider_key="msm:markets",
            alembic_version_meta_table_uid="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
            alembic_revision="0001",
        )
    )

    assert response.ok is True
    assert response.tables[0].provisioning_status == "active"
    assert captured["r_type"] == "POST"
    assert captured["url"].endswith("/ts_manager/meta_table/finalize-managed/")
    assert captured["payload"]["json"] == {
        "meta_table_uids": ["aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"],
        "migration_package": "msm",
        "migration_namespace": "markets",
        "migration_provider_key": "msm:markets",
        "alembic_version_meta_table_uid": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        "alembic_revision": "0001",
    }
    for forbidden in (
        "data_source_uid",
        "management_mode",
        "storage_hash",
        "table_contract",
        "labels",
        "provisioning",
    ):
        assert forbidden not in captured["payload"]["json"]


def test_meta_table_finalize_managed_accepts_conflict_response(monkeypatch):
    def fake_make_request(**kwargs):
        return _Response(
            {
                "ok": False,
                "finalized_count": 0,
                "active_count": 0,
                "reserved_count": 1,
                "failed_count": 1,
                "tables": [
                    {
                        "meta_table_uid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                        "identifier": "Asset",
                        "storage_hash": "mt_asset_hash",
                        "physical_table_name": "mt_asset_physical",
                        "previous_provisioning_status": "reserved",
                        "provisioning_status": "reserved",
                        "table_kind": "relational",
                        "time_indexed": False,
                        "finalized": False,
                        "physical_table_exists": False,
                        "error": {"code": "physical_table_missing"},
                    }
                ],
            },
            status_code=409,
        )

    monkeypatch.setattr(meta_table_models, "make_request", fake_make_request)
    monkeypatch.setattr(
        meta_table_models.MetaTable,
        "build_session",
        classmethod(lambda cls: SimpleNamespace(headers={})),
    )

    response = meta_table_models.MetaTable.finalize_managed(
        meta_table_models.ManagedMetaTableFinalizeRequest(
            meta_table_uids=["aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"],
            migration_package="msm",
            migration_namespace="markets",
        )
    )

    assert response.ok is False
    assert response.tables[0].physical_table_exists is False
    assert response.tables[0].error == {"code": "physical_table_missing"}


def test_meta_table_alembic_provider_reset_posts_reset_payload(monkeypatch):
    captured = {}

    def fake_make_request(**kwargs):
        captured.update(kwargs)
        return _Response(
            {
                "ok": True,
                "migration_provider_key": "msm:markets",
                "migration_package": "msm",
                "migration_namespace": "markets",
                "data_source_uid": "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
                "meta_table_uids": ["aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"],
                "dropped_physical_tables": ["mt_asset_physical"],
                "cleared_alembic_version_table": True,
                "deleted_or_reserved_catalog_rows": ["aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"],
                "failed_count": 0,
                "tables": [
                    {
                        "meta_table_uid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                        "identifier": "Asset",
                        "storage_hash": "mt_asset_hash",
                        "physical_table_name": "mt_asset_physical",
                        "previous_provisioning_status": "active",
                        "provisioning_status": "reserved",
                        "physical_table_exists": True,
                        "physical_table_dropped": True,
                    }
                ],
                "errors": [],
            }
        )

    monkeypatch.setattr(meta_table_models, "make_request", fake_make_request)
    monkeypatch.setattr(
        meta_table_models.MetaTable,
        "build_session",
        classmethod(lambda cls: SimpleNamespace(headers={})),
    )

    response = meta_table_models.MetaTable.alembic_provider_reset(
        meta_table_models.AlembicProviderResetRequest(
            migration_package="msm",
            migration_namespace="markets",
            data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
            confirm_reset=True,
        )
    )

    assert response.ok is True
    assert response.cleared_alembic_version_table is True
    assert captured["r_type"] == "POST"
    assert captured["url"].endswith("/ts_manager/meta_table/alembic-provider-reset/")
    assert captured["payload"]["json"]["confirm_reset"] is True
    assert captured["payload"]["json"]["drop_physical_tables"] is True


def test_removed_alembic_artifact_client_models_are_not_public():
    removed_names = [
        "ALEMBIC_MIGRATION_V1",
        "AlembicMigrationApplyResponse",
        "AlembicMigrationOperation",
        "AlembicMigrationStatusRequest",
        "AlembicMigrationStatusResponse",
    ]

    for name in removed_names:
        assert not hasattr(meta_table_models, name)
    assert not hasattr(meta_table_models.MetaTable, "apply_migration")
    assert not hasattr(meta_table_models.MetaTable, "get_migration_status")


def test_migration_connection_request_validates_positive_ttl():
    with pytest.raises(ValidationError):
        meta_table_models.DynamicTableDataSourceMigrationConnectionRequest(
            meta_table_uids=["aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"],
            ttl_seconds=0,
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
                    "reservedPolicy": "reconcile",
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
    assert operation.scope.tables[0].reserved_policy == "reconcile"
    assert (
        operation.model_dump(mode="json", by_alias=True)["scope"]["tables"][0]["reserved_policy"]
        == "reconcile"
    )

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
