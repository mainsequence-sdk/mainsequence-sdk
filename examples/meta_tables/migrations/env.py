from __future__ import annotations

from alembic import context
from sqlalchemy import engine_from_config, pool

from examples.meta_tables.migrations import migration as default_migration


def _migration_provider():
    return context.config.attributes.get("mainsequence_migration_provider", default_migration)


def _configure_kwargs(migration):
    return {
        "target_metadata": migration.target_metadata,
        "version_table": migration.version_table,
        "version_table_schema": migration.version_table_schema,
        "include_name": migration.include_name,
        "include_object": migration.include_object,
        "compare_type": True,
        "compare_server_default": True,
    }


def run_migrations_offline() -> None:
    migration = _migration_provider()
    context.configure(
        url=context.config.get_main_option("sqlalchemy.url"),
        literal_binds=True,
        **_configure_kwargs(migration),
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    migration = _migration_provider()
    connectable = engine_from_config(
        context.config.get_section(context.config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, **_configure_kwargs(migration))
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
