from __future__ import annotations

import datetime
import importlib

import pytest
from sqlalchemy import DateTime, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from mainsequence.client.models_metatables import MetaTable
from mainsequence.meta_tables import MigrationManagedTimeIndexMetaData, PlatformManagedMetaTable
from mainsequence.meta_tables.migrations import (
    METATABLE_MIGRATION_V1,
    MIGRATION_MANIFEST_V1,
    MigrationMetaTable,
    apply_migration,
    build_migration_operation,
    build_migration_registry_row,
    build_registry_upsert_operation,
    create_default_migration_registry_model,
    load_packaged_migration,
    migration_lock_key,
    sync_packaged_migration,
)


def _migration_meta_table(**overrides):
    payload = {
        "uid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "data_source_uid": "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        "storage_hash": "mt_migrations_registry",
        "identifier": "metatable_migrations",
        "namespace": "mainsequence.migrations",
        "description": None,
        "labels": [],
        "management_mode": "platform_managed",
        "physical_table_name": "mt_migrations_registry",
        "table_contract": {},
        "contract_version": "relational-table.v1",
        "introspection_snapshot": {},
        "protect_from_deletion": False,
        "columns": [],
        "indexes_meta": [],
        "foreign_keys": [],
        "incoming_fks": [],
        "open_for_everyone": False,
    }
    payload.update(overrides)
    return MetaTable(**payload)


def test_load_packaged_migration_and_build_registry_row(tmp_path, monkeypatch):
    package_name = "sample_migrations_load"
    package_dir = tmp_path / package_name
    migrations_dir = package_dir / "migrations"
    migrations_dir.mkdir(parents=True)
    (package_dir / "__init__.py").write_text("", encoding="utf-8")
    manifest_text = f"""
version: {MIGRATION_MANIFEST_V1}
migration_namespace: markets
revision: "001"
down_revision:
direction: upgrade
expected_current_revision:
sql_path: migrations/001.up.sql
statement_boundaries:
  - statement_index: 0
    start_line: 1
    end_line: 1
affected_tables:
  - identifier: asset
    namespace: msm
"""
    (migrations_dir / "001.yaml").write_text(manifest_text, encoding="utf-8")
    (migrations_dir / "001.up.sql").write_text(
        "CREATE TABLE asset (uid uuid PRIMARY KEY);",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()

    packaged = load_packaged_migration(package_name, "migrations/001.yaml")
    row = build_migration_registry_row(
        packaged,
        data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
    )

    assert packaged.manifest.package == package_name
    assert packaged.manifest.revision == "001"
    assert len(packaged.manifest_sha256) == 64
    assert len(packaged.sql_sha256) == 64
    assert row.version == METATABLE_MIGRATION_V1
    assert row.package == package_name
    assert row.lock_key == migration_lock_key(
        data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        package=package_name,
        migration_namespace="markets",
    )
    assert row.affected_tables == [{"identifier": "asset", "namespace": "msm"}]


def test_contract_hash_rotation_example_builds_old_and_new_hashes():
    from examples.meta_tables.migrations.contract_hash_rotation import (
        ASSET_IDENTIFIER,
        contract_rotation_summary,
        load_asset_status_migration,
    )

    packaged = load_asset_status_migration()
    summary = contract_rotation_summary(packaged)
    row = build_migration_registry_row(
        packaged,
        data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
    )

    assert summary["identifier"] == ASSET_IDENTIFIER
    assert summary["rotated"] is True
    assert row.old_contract_hashes[ASSET_IDENTIFIER] == summary["old_contract_hash"]
    assert row.new_contract_hashes[ASSET_IDENTIFIER] == summary["new_contract_hash"]
    assert row.old_contracts[ASSET_IDENTIFIER]["columns"]
    assert row.new_contracts[ASSET_IDENTIFIER]["columns"]
    assert row.affected_tables == [{"identifier": ASSET_IDENTIFIER, "namespace": "sdk-examples"}]


def test_load_packaged_migration_accepts_operation_plan_without_sql(tmp_path, monkeypatch):
    package_name = "sample_migrations_operations"
    package_dir = tmp_path / package_name
    migrations_dir = package_dir / "migrations"
    migrations_dir.mkdir(parents=True)
    (package_dir / "__init__.py").write_text("", encoding="utf-8")
    (migrations_dir / "001.json").write_text(
        """
{
  "version": "metatable-migration-manifest.v1",
  "migration_namespace": "markets",
  "revision": "001",
  "direction": "upgrade",
  "operations": [
    {
      "op": "add_column",
      "table_identifier": "sample.Asset",
      "column": {"name": "display_name", "data_type": "str", "nullable": true}
    }
  ]
}
""",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()

    packaged = load_packaged_migration(package_name, "migrations/001.json")
    row = build_migration_registry_row(
        packaged,
        data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
    )

    assert packaged.sql == ""
    assert row.operations[0]["op"] == "add_column"
    assert len(packaged.operations_sha256) == 64


def test_load_packaged_migration_rejects_shape_addressed_contract_models(tmp_path, monkeypatch):
    package_name = "sample_migrations_shape_reject"
    package_dir = tmp_path / package_name
    migrations_dir = package_dir / "migrations"
    migrations_dir.mkdir(parents=True)
    (package_dir / "__init__.py").write_text("", encoding="utf-8")
    (migrations_dir / "001.json").write_text(
        """
{
  "version": "metatable-migration-manifest.v1",
  "migration_namespace": "markets",
  "revision": "001",
  "direction": "upgrade",
  "operations": [
    {"op": "add_column", "table_identifier": "sample.Asset", "column": "display_name"}
  ]
}
""",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()

    class Base(DeclarativeBase):
        pass

    class ShapeAddressedAsset(PlatformManagedMetaTable, Base):
        __table_args__ = {"schema": "public"}
        __metatable_namespace__ = "sample"
        __metatable_identifier__ = "sample.Asset"

        uid: Mapped[str] = mapped_column(String(64), primary_key=True)

    with pytest.raises(ValueError, match="MigrationManagedMetaTable"):
        load_packaged_migration(
            package_name,
            "migrations/001.json",
            new_contract_models={"sample.Asset": ShapeAddressedAsset},
        )


def test_load_packaged_migration_accepts_migration_managed_time_index_models(
    tmp_path,
    monkeypatch,
):
    package_name = "sample_migrations_time_index_accept"
    package_dir = tmp_path / package_name
    migrations_dir = package_dir / "migrations"
    migrations_dir.mkdir(parents=True)
    (package_dir / "__init__.py").write_text("", encoding="utf-8")
    (migrations_dir / "001.json").write_text(
        """
{
  "version": "metatable-migration-manifest.v1",
  "migration_namespace": "markets",
  "revision": "001",
  "direction": "upgrade",
  "affected_tables": [{"identifier": "sample.Holdings"}],
  "operations": [
    {"op": "add_column", "table_identifier": "sample.Holdings", "column": "display_name"}
  ]
}
""",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()

    class Base(DeclarativeBase):
        pass

    class Holdings(MigrationManagedTimeIndexMetaData, Base):
        __table_args__ = {"schema": "public"}
        __metatable_namespace__ = "sample"
        __metatable_identifier__ = "sample.Holdings"
        __time_index_name__ = "time_index"
        __index_names__ = ["time_index", "asset_uid"]

        time_index: Mapped[datetime.datetime] = mapped_column(
            DateTime(timezone=True),
            nullable=False,
        )
        asset_uid: Mapped[str] = mapped_column(String(64), nullable=False)
        value: Mapped[str] = mapped_column(String(64), nullable=False)

    packaged = load_packaged_migration(
        package_name,
        "migrations/001.json",
        new_contract_models={"sample.Holdings": Holdings},
    )

    assert "sample.Holdings" in packaged.manifest.new_contracts
    assert packaged.manifest.new_contracts["sample.Holdings"]["table_kind"] == "time_indexed"


def test_create_default_migration_registry_model_builds_metatable_contract(monkeypatch):
    sqlalchemy = pytest.importorskip("sqlalchemy")

    class Base(DeclarativeBase):
        pass

    registry = create_default_migration_registry_model(
        Base,
        namespace="msm",
        identifier="markets_migrations",
    )

    assert issubclass(registry, MigrationMetaTable)
    assert registry.__metatable_namespace__ == "msm"
    assert registry.__metatable_identifier__ == "markets_migrations"

    request = registry.build_registration_request(
        data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd"
    )
    column_names = {column.name for column in request.table_contract.columns}
    assert {"uid", "package", "migration_namespace", "operations", "sql", "sql_sha256"} <= (
        column_names
    )
    assert request.management_mode == "platform_managed"
    assert request.table_contract.physical.table_name is None

    sqlalchemy.inspect(registry)


def test_project_can_extend_migration_metatable_with_extra_columns():
    sqlalchemy = pytest.importorskip("sqlalchemy")

    class Base(DeclarativeBase):
        pass

    class ProjectMigration(MigrationMetaTable, Base):
        __table_args__ = {"schema": "public"}
        __metatable_namespace__ = "msm"
        __metatable_identifier__ = "markets_migrations"
        __metatable_description__ = "Project-specific migration registry."

        release_channel: Mapped[str | None] = mapped_column(String(64), nullable=True)

    request = ProjectMigration.build_registration_request(
        data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd"
    )

    column_names = {column.name for column in request.table_contract.columns}
    assert {"uid", "operations", "sql", "sql_sha256", "release_channel"} <= column_names
    sqlalchemy.inspect(ProjectMigration)


def test_build_registry_upsert_operation_uses_registry_metatable_scope(tmp_path, monkeypatch):
    package_name = "sample_migrations_upsert"
    package_dir = tmp_path / package_name
    migrations_dir = package_dir / "migrations"
    migrations_dir.mkdir(parents=True)
    (package_dir / "__init__.py").write_text("", encoding="utf-8")
    (migrations_dir / "001.json").write_text(
        """
{
  "version": "metatable-migration-manifest.v1",
  "migration_namespace": "markets",
  "revision": "001",
  "direction": "upgrade",
  "sql_path": "migrations/001.up.sql"
}
""",
        encoding="utf-8",
    )
    (migrations_dir / "001.up.sql").write_text("SELECT 1;", encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()

    packaged = load_packaged_migration(package_name, "migrations/001.json")
    row = build_migration_registry_row(
        packaged,
        data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
    )
    operation = build_registry_upsert_operation(_migration_meta_table(), row)

    assert operation.operation == "upsert"
    assert 'INSERT INTO "mt_migrations_registry"' in operation.statement.sql
    assert 'ON CONFLICT ("uid") DO UPDATE SET' in operation.statement.sql
    assert operation.scope.tables[0].meta_table_uid == "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    assert operation.scope.tables[0].access == "write"
    assert operation.statement.parameters["revision"] == "001"


def test_build_and_apply_migration_operation_references_registry_row(monkeypatch):
    migration_meta_table = _migration_meta_table()
    row = {
        "uid": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        "target_data_source_uid": "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        "package": "msm",
        "migration_namespace": "markets",
        "revision": "001",
        "direction": "upgrade",
        "manifest_sha256": "a" * 64,
        "sql_sha256": "b" * 64,
        "affected_tables": [{"identifier": "asset", "namespace": "msm"}],
        "old_contract_hashes": {},
        "new_contract_hashes": {},
        "idempotency_key": "migration:001",
        "lock_key": "dddddddd-dddd-4ddd-8ddd-dddddddddddd:msm:markets",
    }

    operation = build_migration_operation(migration_meta_table, row, dry_run=True)
    assert operation.migration_meta_table_uid == migration_meta_table.uid
    assert operation.migration_row_uid == row["uid"]
    assert operation.dry_run is True

    captured = {}

    def fake_apply(operation_payload, *, timeout=None):
        captured["operation"] = operation_payload
        captured["timeout"] = timeout
        return {"ok": True, "applied_revision": "001"}

    monkeypatch.setattr(MetaTable, "apply_migration", staticmethod(fake_apply))
    result = apply_migration(migration_meta_table, row, dry_run=True, timeout=15)

    assert result["ok"] is True
    assert captured["operation"].migration_row_uid == row["uid"]
    assert captured["operation"].dry_run is True
    assert captured["timeout"] == 15


def test_sync_packaged_migration_registers_registry_and_executes_upsert(tmp_path, monkeypatch):
    package_name = "sample_migrations_sync"
    package_dir = tmp_path / package_name
    migrations_dir = package_dir / "migrations"
    migrations_dir.mkdir(parents=True)
    (package_dir / "__init__.py").write_text("", encoding="utf-8")
    (migrations_dir / "001.json").write_text(
        """
{
  "version": "metatable-migration-manifest.v1",
  "migration_namespace": "markets",
  "revision": "001",
  "direction": "upgrade",
  "sql_path": "migrations/001.up.sql"
}
""",
        encoding="utf-8",
    )
    (migrations_dir / "001.up.sql").write_text("SELECT 1;", encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()

    packaged = load_packaged_migration(package_name, "migrations/001.json")

    class Registry(MigrationMetaTable):
        @classmethod
        def register(cls, *, timeout=None, _registration_stack=()):
            return _migration_meta_table()

    captured = {}

    def fake_execute(operation, *, timeout=None):
        captured["operation"] = operation
        captured["timeout"] = timeout
        return {"ok": True}

    monkeypatch.setattr(MetaTable, "execute_operation", staticmethod(fake_execute))

    result = sync_packaged_migration(
        Registry,
        packaged,
        data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        timeout=10,
    )

    assert result["result"] == {"ok": True}
    assert result["meta_table"].uid == "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    assert result["row"].revision == "001"
    assert captured["operation"].operation == "upsert"
    assert captured["timeout"] == 10
