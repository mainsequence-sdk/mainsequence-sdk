from __future__ import annotations

import os

from examples.meta_tables.common import DEFAULT_SCHEMA
from examples.meta_tables.platform_managed.account_asset import Account, Asset, Base
from mainsequence.meta_tables import schema_table_name
from mainsequence.meta_tables.migrations import (
    AlembicMetaTableMigration,
    AlembicVersionMetaTable,
)

NAMESPACE = "sdk-examples"
PROJECT_NAME = "sdk_examples"


class ExampleAlembicVersion(AlembicVersionMetaTable):
    __metatable_namespace__ = NAMESPACE
    __metatable_identifier__ = f"{PROJECT_NAME}.alembic_version"
    __alembic_version_schema__ = DEFAULT_SCHEMA
    __alembic_version_table_name__ = schema_table_name(PROJECT_NAME, "alembic_version")
    __alembic_version_column_name__ = "version_num"
    __metatable_data_source_uid__ = os.getenv("MAINSEQUENCE_META_TABLE_DATA_SOURCE_UID") or None


migration = AlembicMetaTableMigration(
    package="mainsequence_sdk_examples",
    migration_namespace=NAMESPACE,
    script_location="examples.meta_tables:migrations",
    target_metadata=Base.metadata,
    alembic_registry=ExampleAlembicVersion,
    metatable_models=[
        Account,
        Asset,
    ],
)
