# MetaTable Examples

These examples show the Account/Asset flow from the MetaTable ADR using the SDK
helpers.

There are two registration modes:

- `external_registered`: your application creates and migrates the physical
  table. TS Manager registers metadata, permissions, search/discovery, and
  governed execution for that table.
- `platform_managed`: your application sends a neutral table contract. TS
  Manager creates the physical table through the configured
  `DynamicTableDataSource` when the backend supports table DDL.

The examples are dry-run by default. They print the generated contracts unless
you explicitly enable backend mutations.

## Environment

Set the TS Manager data source UID:

```bash
export MAINSEQUENCE_META_TABLE_DATA_SOURCE_UID="<dynamic-table-data-source-uid>"
```

Optional common settings:

```bash
export MAINSEQUENCE_META_TABLE_SCHEMA="public"
export MAINSEQUENCE_META_TABLE_NAMESPACE="examples.meta_tables.demo"
export MAINSEQUENCE_META_TABLE_TIMEOUT="120"
```

## Platform Managed

Use platform-managed mode when TS Manager should create the physical tables and
own the table naming boundary.

Dry run:

```bash
python -m examples.meta_tables.platform_managed_account_asset
```

Register and create tables through TS Manager:

```bash
export MAINSEQUENCE_META_TABLE_REGISTER=1
python -m examples.meta_tables.platform_managed_account_asset
```

The model definitions use:

```python
__tablename__ = metatable_tablename(namespace=NAMESPACE, identifier="Account")
```

That makes the physical table name equal to the same `storage_hash` sent during
registration, which prevents user table-name collisions inside the platform
data-source boundary.

The platform-managed example also derives index and foreign-key names from the
generated table name. That keeps database object names unique when many users
register an `Asset` table in the same physical schema.

## External Registered

Use external-registered mode when your application already owns migrations, for
example through SQLAlchemy plus Alembic.

Dry run:

```bash
python -m examples.meta_tables.external_registered_account_asset
```

For a local demo only, you can ask the example to create the physical tables
directly with SQLAlchemy before registration:

```bash
export MAINSEQUENCE_META_TABLE_CREATE_EXTERNAL_TABLES=1
export MAINSEQUENCE_META_TABLE_EXTERNAL_DATABASE_URL="postgresql+psycopg://..."
export MAINSEQUENCE_META_TABLE_REGISTER=1
python -m examples.meta_tables.external_registered_account_asset
```

In a production application, replace that demo `create_all(...)` step with your
normal migration workflow.

## Compiled Query

After Account and Asset are registered, compile an application-authored
SQLAlchemy statement into the TS Manager execution protocol:

```bash
export MAINSEQUENCE_META_TABLE_ACCOUNT_UID="<account-metatable-uid>"
export MAINSEQUENCE_META_TABLE_ASSET_UID="<asset-metatable-uid>"
export MAINSEQUENCE_META_TABLE_ACCOUNT_ROW_UID="<account-row-uuid>"
python -m examples.meta_tables.compiled_sql_account_asset_query
```

The query example uses platform-managed model definitions by default. To compile
against the external-registered physical names instead:

```bash
export MAINSEQUENCE_META_TABLE_EXAMPLE_MODE="external_registered"
python -m examples.meta_tables.compiled_sql_account_asset_query
```

Execute it through TS Manager:

```bash
export MAINSEQUENCE_META_TABLE_EXECUTE=1
python -m examples.meta_tables.compiled_sql_account_asset_query
```

The compiled SQL payload includes both joined tables in `scope.tables`, because
TS Manager authorizes execution through the declared MetaTable scope.
