# MetaTable Examples

These examples show the Account/Asset flow from the MetaTable ADR using the SDK
helpers.

There are two registration modes:

- `external_registered`: your application creates and migrates the physical
  table. TS Manager registers metadata, permissions, search/discovery, and
  governed execution for that table.
- `platform_managed`: migration tooling sends the neutral table contract. TS
  Manager reserves catalog rows and backend-owned physical names before Alembic
  runs. Alembic creates or changes the physical application tables, then TS
  Manager finalizes the reserved catalog rows.

The external-registered examples call TS Manager by default. Platform-managed
examples are migration-first and do not call `Model.register()` directly.

## Environment

The platform-managed example resolves the TS Manager data source from the
current Main Sequence project/session, the same way DataNode does.

Optional common settings:

```bash
export MAINSEQUENCE_META_TABLE_SCHEMA="public"
export MAINSEQUENCE_META_TABLE_TIMEOUT="120"
```

## Platform Managed

Use platform-managed mode when TS Manager should own the catalog and naming
boundary while Alembic owns the physical schema migration.

Inspect the model declarations:

```bash
python -m examples.meta_tables.platform_managed.account_asset
```

The model definitions use:

```python
PROJECT_NAME = "sdk_examples"


class Account(PlatformManagedMetaTable, Base):
    __metatable_namespace__ = NAMESPACE
    __metatable_identifier__ = f"{PROJECT_NAME}.Account"
```

The model classes are registered by migration tooling through the provider in
`examples.meta_tables.migrations:migration`. Do not call `Account.register()` or
`Asset.register()` in application/bootstrap code.

The examples use a `PROJECT_NAME` prefix for logical MetaTable identifiers,
external physical table names, and the Alembic version table name. Do not use
bare names like `Account`, `asset`, or `alembic_version` in shared schemas; they
collide easily across projects and providers.

The platform-managed example uses a SQLAlchemy naming convention for indexes.
Platform-managed foreign-key contracts keep logical FK relationships only. The
physical FK constraint names are not managed by the MetaTable backend and should
not be copied into the MetaTable contract.

Foreign-key targets are resolved by the migration workflow. Before Alembic runs,
`migrations upgrade` resolves existing provider-scoped MetaTables by exact
`identifier`, reserves missing rows through `reserve-managed`, and binds
SQLAlchemy models to backend `MetaTable.uid` and storage metadata while
preserving authored table names.
After Alembic succeeds, the CLI calls backend finalization for the prepared
provider MetaTable UIDs.

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

MetaTable schema migrations are provider-based and Alembic-backed. The old SDK
migration examples were removed because the SDK no longer supports separate
artifact tables or custom SDK schema-operation migrations.

The provider example lives at:

```text
examples.meta_tables.migrations:migration
```

It defines:

- `AlembicMetaTableMigration` for provider identity and Alembic configuration
- `ExampleAlembicVersion` as the external catalog binding for Alembic's version table
- `metatable_models=[Account, Asset]` as the provider reservation and finalization scope
- `versions/0001_initial_account_asset.py` as a normal Alembic revision that
  creates and drops the provider-bound SQLAlchemy tables

Run the lifecycle with the provider:

```bash
mainsequence migrations current \
  --provider examples.meta_tables.migrations:migration

mainsequence migrations upgrade \
  --provider examples.meta_tables.migrations:migration \
  head
```

After changing the SQLAlchemy models, generate and review a new Alembic
revision, then apply it:

```bash
mainsequence migrations revision \
  --provider examples.meta_tables.migrations:migration \
  -m "describe schema change"

mainsequence migrations upgrade \
  --provider examples.meta_tables.migrations:migration \
  head
```

The provider's `AlembicVersionMetaTable` binding is registered automatically
by commands that need backend migration state, such as `current` and
`upgrade`. `revision` accepts an optional `-m/--message`; if omitted, the CLI
uses `migration`.

`upgrade` first resolves existing catalog rows by exact `identifier`, reserves
only missing rows, applies Alembic, and then finalizes the provider-scoped
MetaTables listed in `metatable_models`. If a model does not declare
`__metatable_identifier__`, the SDK defaults it to
`<pyproject project name>:<model module>.<model qualname>`.
If a model is renamed or moved, pin the old identifier explicitly to preserve
the same platform identity.

When running the example outside an attached project context, set the data
source binding explicitly:

```bash
export MAINSEQUENCE_META_TABLE_DATA_SOURCE_UID="<dynamic-table-data-source-uid>"
```
