from __future__ import annotations

from examples.meta_tables.common import DEFAULT_SCHEMA
from examples.meta_tables.platform_managed.account_asset import Account, Asset, Base
from mainsequence.meta_tables import schema_table_name
from mainsequence.meta_tables.migrations import (
    build_alembic_version_metatable,
    build_metatable_migration_provider,
)

NAMESPACE = "sdk-examples"
PROJECT_NAME = "sdk_examples"


ExampleAlembicVersion = build_alembic_version_metatable(
    class_name="ExampleAlembicVersion",
    namespace=NAMESPACE,
    identifier=f"{PROJECT_NAME}.alembic_version",
    schema=DEFAULT_SCHEMA,
    table_name=schema_table_name(PROJECT_NAME, "alembic_version"),
)


migration = build_metatable_migration_provider(
    package="mainsequence_sdk_examples",
    migration_namespace=NAMESPACE,
    script_location="examples.meta_tables.migrations:",
    version_location_prefix="examples.meta_tables.migrations:versions",
    target_metadata=Base.metadata,
    alembic_registry=ExampleAlembicVersion,
    metatable_models=[
        Account,
        Asset,
    ],
)
