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
python -m examples.meta_tables.platform_managed.account_asset
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

## External Managed

Use external-registered mode when your application already owns the physical
tables. This example creates a local SQLite `DataSource`, resolves its
`DynamicTableDataSource`, creates the Account/Asset tables locally with
SQLAlchemy, and registers those tables with TS Manager.

Register existing externally managed tables:

```bash
python -m examples.meta_tables.external_managed.account_asset
```

By default, the SQLite database file is the SDK local SQLite file. Override it
only when you need a different local file:

```bash
export SQLITE_PATH="/tmp/mainsequence-meta-table-example.sqlite"
python -m examples.meta_tables.external_managed.account_asset
```

In a production application, replace the example `create_all(...)` step with
your normal migration workflow.

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
export MAINSEQUENCE_META_TABLE_EXAMPLE_MODE="external_managed"
python -m examples.meta_tables.compiled_sql_account_asset_query
```

Execute it through TS Manager:

```bash
export MAINSEQUENCE_META_TABLE_EXECUTE=1
python -m examples.meta_tables.compiled_sql_account_asset_query
```

The compiled SQL payload includes both joined tables in `scope.tables`, because
TS Manager authorizes execution through the declared MetaTable scope.

## Migrations

The migration example declares a client-owned `MigrationMetaTable` registry,
loads bundled SQL and a manifest, builds the registry upsert operation, and
builds the `metatable-migration.v1` apply operation.

Preview the payloads without touching TS Manager:

```bash
python -m examples.meta_tables.migrations.client_defined_registry
```

Preview a changed MetaTable contract and the old/new contract hashes that will
be sent in the migration row:

```bash
python -m examples.meta_tables.migrations.contract_hash_rotation
```

Register the registry and sync the packaged migration row:

```bash
export MAINSEQUENCE_META_TABLE_MIGRATION_DATA_SOURCE_UID="<dynamic-data-source-uid>"
export MAINSEQUENCE_META_TABLE_MIGRATION_SYNC=1
python -m examples.meta_tables.migrations.client_defined_registry
```

Apply after the backend migration endpoint is available:

```bash
export MAINSEQUENCE_META_TABLE_MIGRATION_APPLY=1
python -m examples.meta_tables.migrations.client_defined_registry
```

Use the same environment variables with
`examples.meta_tables.migrations.contract_hash_rotation` to sync or apply the
contract-rotation example.
