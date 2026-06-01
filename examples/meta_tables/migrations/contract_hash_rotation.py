"""Preview old/new contract hashes for a changed MetaTable declaration."""

from __future__ import annotations

import uuid

from sqlalchemy import MetaData, String, Uuid
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from examples.meta_tables.common import (
    DEFAULT_TIMEOUT,
    env_flag,
    optional_env,
    print_json,
)
from mainsequence.client import MetaTable
from mainsequence.meta_tables import MigrationManagedMetaTable
from mainsequence.meta_tables.migrations import (
    PackagedMetaTableMigration,
    apply_migration,
    build_migration_operation,
    build_migration_registry_row,
    contract_hashes_from_models,
    load_packaged_migration,
    sync_packaged_migration,
)

from .client_defined_registry import NAMESPACE, ExampleMigrationRegistry

ASSET_IDENTIFIER = "sdk-examples.Asset"

NAMING_CONVENTION = {
    "ix": "%(table_name)s_%(column_0_name)s_idx",
    "pk": "%(table_name)s_pkey",
    "uq": "%(table_name)s_%(column_0_name)s_key",
}


class BeforeBase(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)


class AfterBase(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)


class AssetBeforeMigration(MigrationManagedMetaTable, BeforeBase):
    __table_args__ = {"schema": "public"}
    __metatable_namespace__ = NAMESPACE
    __metatable_identifier__ = ASSET_IDENTIFIER

    uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    symbol: Mapped[str] = mapped_column(String(64), nullable=False)


class AssetAfterMigration(MigrationManagedMetaTable, AfterBase):
    __table_args__ = {"schema": "public"}
    __metatable_namespace__ = NAMESPACE
    __metatable_identifier__ = ASSET_IDENTIFIER

    uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    symbol: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)


# Keep the preview deterministic when the file is executed with ``python -m``.
_STABLE_MODULE = "examples.meta_tables.migrations.contract_hash_rotation"
AssetBeforeMigration.__module__ = _STABLE_MODULE
AssetAfterMigration.__module__ = _STABLE_MODULE


def load_asset_status_migration() -> PackagedMetaTableMigration:
    return load_packaged_migration(
        "examples.meta_tables.migrations",
        "packaged/002_add_asset_status.yaml",
        old_contract_models={ASSET_IDENTIFIER: AssetBeforeMigration},
        new_contract_models={ASSET_IDENTIFIER: AssetAfterMigration},
    )


def contract_rotation_summary(packaged: PackagedMetaTableMigration) -> dict[str, object]:
    old_hashes = contract_hashes_from_models({ASSET_IDENTIFIER: AssetBeforeMigration})
    new_hashes = contract_hashes_from_models({ASSET_IDENTIFIER: AssetAfterMigration})
    return {
        "identifier": ASSET_IDENTIFIER,
        "old_contract_hash": old_hashes[ASSET_IDENTIFIER],
        "new_contract_hash": new_hashes[ASSET_IDENTIFIER],
        "rotated": old_hashes[ASSET_IDENTIFIER] != new_hashes[ASSET_IDENTIFIER],
        "manifest_old_contract_hash": packaged.manifest.old_contract_hashes[ASSET_IDENTIFIER],
        "manifest_new_contract_hash": packaged.manifest.new_contract_hashes[ASSET_IDENTIFIER],
    }


def preview_registry_meta_table(data_source_uid: str) -> MetaTable:
    return MetaTable(
        uid=optional_env(
            "MAINSEQUENCE_META_TABLE_MIGRATION_REGISTRY_UID",
            "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        ),
        data_source_uid=data_source_uid,
        storage_hash="mt_example_migrations",
        identifier="ExampleMigrationRegistry",
        namespace=NAMESPACE,
        management_mode="platform_managed",
        physical_table_name=optional_env(
            "MAINSEQUENCE_META_TABLE_MIGRATION_REGISTRY_TABLE",
            "mt_example_migrations",
        ),
        table_contract={},
    )


def main() -> None:
    data_source_uid = optional_env(
        "MAINSEQUENCE_META_TABLE_MIGRATION_DATA_SOURCE_UID",
        "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
    )
    packaged = load_asset_status_migration()
    row = build_migration_registry_row(packaged, data_source_uid=data_source_uid)

    print_json("Contract hash rotation", contract_rotation_summary(packaged))
    print_json("Migration registry row", row)

    if not env_flag("MAINSEQUENCE_META_TABLE_MIGRATION_SYNC", default=False):
        apply_operation = build_migration_operation(
            preview_registry_meta_table(data_source_uid),
            row,
            dry_run=not env_flag("MAINSEQUENCE_META_TABLE_MIGRATION_APPLY", default=False),
        )
        print_json("Migration apply operation", apply_operation)
        print("\nSet MAINSEQUENCE_META_TABLE_MIGRATION_SYNC=1 to register and write the row.")
        return

    registry_meta_table = ExampleMigrationRegistry.register(
        data_source_uid=data_source_uid,
        timeout=DEFAULT_TIMEOUT,
    )
    sync_result = sync_packaged_migration(
        registry_meta_table,
        packaged,
        timeout=DEFAULT_TIMEOUT,
    )
    print_json("Registry sync result", sync_result["result"])

    if not env_flag("MAINSEQUENCE_META_TABLE_MIGRATION_APPLY", default=False):
        print("\nSet MAINSEQUENCE_META_TABLE_MIGRATION_APPLY=1 to apply the synced row.")
        return

    result = apply_migration(
        sync_result["meta_table"],
        sync_result["row"],
        timeout=DEFAULT_TIMEOUT,
    )
    print_json("Migration apply result", result)


if __name__ == "__main__":
    main()
