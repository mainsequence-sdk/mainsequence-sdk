from __future__ import annotations

from collections.abc import Callable
from typing import Any


def run_mainsequence_alembic_env(
    *,
    default_provider: Any | None = None,
    included_schema: Callable[[str | None], bool] | None = None,
) -> None:
    """Run the SDK-owned Alembic environment for a MetaTable migration provider."""

    from alembic import context
    from sqlalchemy import engine_from_config, pool

    from mainsequence.meta_tables.migrations import apply_mainsequence_migration_role

    def migration_provider() -> Any:
        provider = context.config.attributes.get("mainsequence_migration_provider")
        if provider is not None:
            return provider
        if default_provider is None:
            raise RuntimeError(
                "No MainSequence MetaTable migration provider is configured. "
                "Pass default_provider from the scaffolded env.py or set "
                "context.config.attributes['mainsequence_migration_provider']."
            )
        return default_provider

    def schema_is_included(schema_name: str | None) -> bool:
        if included_schema is None:
            return True
        return bool(included_schema(schema_name))

    def uses_named_schemas() -> bool:
        migration = migration_provider()
        if migration.version_table_schema is not None:
            return True
        return any(
            getattr(table, "schema", None) is not None
            for table in migration.target_metadata.tables.values()
        )

    def include_name(name: str | None, type_: str, parent_names: dict[str, Any]) -> bool:
        if type_ == "schema":
            return schema_is_included(name)
        schema_name = parent_names.get("schema_name") if parent_names else None
        if not schema_is_included(schema_name):
            return False
        return migration_provider().include_name(name, type_, parent_names)

    def include_object(
        object_: Any,
        name: str | None,
        type_: str,
        reflected: bool,
        compare_to: Any,
    ) -> bool:
        if not schema_is_included(getattr(object_, "schema", None)):
            return False
        return migration_provider().include_object(
            object_,
            name,
            type_,
            reflected,
            compare_to,
        )

    def configure_kwargs() -> dict[str, Any]:
        migration = migration_provider()
        return {
            "target_metadata": migration.target_metadata,
            "version_table": migration.version_table,
            "version_table_schema": migration.version_table_schema,
            "include_schemas": uses_named_schemas(),
            "include_name": include_name,
            "include_object": include_object,
            "compare_type": True,
            "compare_server_default": True,
        }

    def run_migrations_offline() -> None:
        context.configure(
            url=context.config.get_main_option("sqlalchemy.url"),
            **configure_kwargs(),
            literal_binds=True,
        )
        with context.begin_transaction():
            context.run_migrations()

    def run_migrations_online() -> None:
        config = context.config
        connection = config.attributes.get("connection")
        if connection is not None:
            apply_mainsequence_migration_role(connection, config)
            context.configure(connection=connection, **configure_kwargs())
            with context.begin_transaction():
                context.run_migrations()
            return

        connectable = engine_from_config(
            config.get_section(config.config_ini_section) or {},
            prefix="sqlalchemy.",
            poolclass=pool.NullPool,
        )
        with connectable.connect() as connection:
            apply_mainsequence_migration_role(connection, config)
            context.configure(connection=connection, **configure_kwargs())
            with context.begin_transaction():
                context.run_migrations()

    if context.is_offline_mode():
        run_migrations_offline()
    else:
        run_migrations_online()


__all__ = ["run_mainsequence_alembic_env"]
