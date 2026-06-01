from __future__ import annotations

from sqlalchemy import MetaData, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from examples.meta_tables.common import (
    DEFAULT_SCHEMA,
    DEFAULT_TIMEOUT,
    env_flag,
    optional_env,
    print_json,
)
from mainsequence.meta_tables.migrations import (
    MigrationMetaTable,
    apply_migration,
    build_migration_operation,
    build_migration_registry_row,
    build_registry_upsert_operation,
    load_packaged_migration,
    sync_packaged_migration,
)

NAMESPACE = "sdk-examples"

NAMING_CONVENTION = {
    "ix": "%(table_name)s_%(column_0_name)s_idx",
    "pk": "%(table_name)s_pkey",
    "uq": "%(table_name)s_%(column_0_name)s_key",
}


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)


class ExampleMigrationRegistry(MigrationMetaTable, Base):
    __table_args__ = {"schema": DEFAULT_SCHEMA}

    __metatable_namespace__ = NAMESPACE
    __metatable_identifier__ = "ExampleMigrationRegistry"
    __metatable_description__ = (
        "Example client-defined MetaTable registry for packaged contract migrations."
    )
    __metatable_extra_hash_components__ = {"storage_name": "example_migrations"}

    release_channel: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
        info={
            "label": "Release Channel",
            "description": "Optional package release channel used by the example.",
        },
    )


def main() -> None:
    data_source_uid = optional_env(
        "MAINSEQUENCE_META_TABLE_MIGRATION_DATA_SOURCE_UID",
        "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
    )
    packaged = load_packaged_migration(
        "examples.meta_tables.migrations",
        "packaged/001_create_asset_tag.yaml",
    )
    row = build_migration_registry_row(
        packaged,
        data_source_uid=data_source_uid,
        extension_fields={"release_channel": optional_env("MAINSEQUENCE_RELEASE_CHANNEL", "dev")},
    )
    print_json("Migration registry row", row)

    if not env_flag("MAINSEQUENCE_META_TABLE_MIGRATION_SYNC", default=False):
        registry_meta_table_uid = optional_env(
            "MAINSEQUENCE_META_TABLE_MIGRATION_REGISTRY_UID",
            "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        )
        registry_physical_table_name = optional_env(
            "MAINSEQUENCE_META_TABLE_MIGRATION_REGISTRY_TABLE",
            "mt_example_migrations",
        )
        from mainsequence.client import MetaTable

        registry_meta_table = MetaTable(
            uid=registry_meta_table_uid,
            data_source_uid=data_source_uid,
            storage_hash="mt_example_migrations",
            identifier="ExampleMigrationRegistry",
            namespace=NAMESPACE,
            management_mode="platform_managed",
            physical_table_name=registry_physical_table_name,
            table_contract={},
        )
        upsert_operation = build_registry_upsert_operation(registry_meta_table, row)
        apply_operation = build_migration_operation(
            registry_meta_table,
            row,
            dry_run=not env_flag("MAINSEQUENCE_META_TABLE_MIGRATION_APPLY", default=False),
        )
        print_json("Registry upsert operation", upsert_operation)
        print_json("Migration apply operation", apply_operation)
        print("\nSet MAINSEQUENCE_META_TABLE_MIGRATION_SYNC=1 to register and write the row.")
        return

    sync_result = sync_packaged_migration(
        ExampleMigrationRegistry,
        packaged,
        timeout=DEFAULT_TIMEOUT,
        extension_fields={"release_channel": optional_env("MAINSEQUENCE_RELEASE_CHANNEL", "dev")},
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
