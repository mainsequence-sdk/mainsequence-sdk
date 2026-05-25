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

The registration examples call TS Manager by default. The compiled query example
prints the operation by default and executes only when explicitly requested.

## Environment

The platform-managed example resolves the TS Manager data source from the
current Main Sequence project/session, the same way DataNode does.

Optional common settings:

```bash
export MAINSEQUENCE_META_TABLE_SCHEMA="public"
export MAINSEQUENCE_META_TABLE_TIMEOUT="120"
```

## Platform Managed

Use platform-managed mode when TS Manager should create the physical tables and
own the table naming boundary.

Register and create tables through TS Manager:

```bash
python -m examples.meta_tables.platform_managed_account_asset
```

The model definitions use:

```python
class Account(PlatformManagedMetaTable, Base):
    __metatable_namespace__ = NAMESPACE
    __metatable_identifier__ = "Account"
```

That makes the physical table name equal to the same configured `storage_hash`
sent during registration, which prevents user table-name collisions inside the
platform data-source boundary and rotates the name when the table shape changes.
Changing `__metatable_identifier__` changes the backend logical name, not the
configured physical table name.

The platform-managed example uses SQLAlchemy naming conventions for index and
foreign-key names. Those names are generated after the configured table name is
known, avoiding a circular dependency between table-name hashing and database
object names.

Foreign-key targets are resolved by registration order. The example registers
`Account` first; `Asset.register()` then inspects the SQLAlchemy foreign key and
looks up the registered Account MetaTable in the same platform data source.

## External Registered

Use external-registered mode when your application already owns migrations, for
example through SQLAlchemy plus Alembic.

External registration is not tied to the project-managed table lifecycle, so
the example requires an explicit data source UID:

```bash
export MAINSEQUENCE_META_TABLE_DATA_SOURCE_UID="<dynamic-table-data-source-uid>"
```

Register existing externally managed tables:

```bash
python -m examples.meta_tables.external_registered_account_asset
```

For a local demo only, you can ask the example to create the physical tables
directly with SQLAlchemy before registration:

```bash
export MAINSEQUENCE_META_TABLE_CREATE_EXTERNAL_TABLES=1
export MAINSEQUENCE_META_TABLE_EXTERNAL_DATABASE_URL="postgresql+psycopg://..."
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
