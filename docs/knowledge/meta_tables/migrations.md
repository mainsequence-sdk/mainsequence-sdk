# MetaTable Migrations

MetaTable schema migrations use Alembic. The SDK does not provide a parallel
operation-list migration language or a separate SDK artifact table.

The protocol version for backend execution is:

```text
metatable-migration.v1
```

## Architecture

The migration lifecycle is:

```text
SQLAlchemy models
-> Alembic revision
-> Alembic renders SQL
-> SDK sends the Alembic-rendered SQL artifact to TS Manager
-> TS Manager executes SQL
-> project tooling registers or refreshes MetaTable catalog bindings separately
```

Alembic owns revision files, upgrade/downgrade operations, offline SQL
rendering, and the physical `alembic_version` table.

The SDK owns typed MetaTable registration/catalog bindings and typed backend
request models.

## Alembic Version MetaTable

`AlembicVersionMetaTable` registers a catalog pointer for Alembic's version
table. It uses the minimal known `version_num` contract required by normal
external MetaTable registration; Alembic and PostgreSQL remain responsible for
the physical version table.

```python
from mainsequence.meta_tables.migrations import AlembicVersionMetaTable


request = AlembicVersionMetaTable.build_registration_request(
    data_source_uid=DATA_SOURCE_UID,
    schema="public",
    table_name="alembic_version",
)
```

The generated contract declares the Alembic revision column:

```json
{
  "physical": {"table_name": "alembic_version"},
  "columns": [
    {
      "name": "version_num",
      "data_type": "string",
      "backend_type": "VARCHAR",
      "nullable": false,
      "primary_key": true
    }
  ],
  "authoring": {
    "owner": "alembic",
    "schema": "public",
    "version_table": "alembic_version"
  }
}
```

Use a subclass only to set project/package defaults:

```python
class MarketsAlembicVersion(AlembicVersionMetaTable):
    __metatable_namespace__ = "msm"
    __metatable_identifier__ = "msm.alembic_version"
    __alembic_version_schema__ = "markets"
```

## Backend Apply Payload

The backend apply endpoint receives Alembic-rendered SQL plus manifest metadata.
It does not receive custom SDK operations and it does not read a separate SDK
artifact row.

```json
{
  "version": "metatable-migration.v1",
  "data_source_uid": "uuid",
  "alembic_version_meta_table_uid": "uuid",
  "package": "msm",
  "migration_namespace": "markets",
  "revision": "0001_initial",
  "down_revision": null,
  "direction": "upgrade",
  "expected_current_revision": null,
  "manifest": {},
  "sql": "Alembic-rendered SQL text",
  "statement_boundaries": [],
  "dry_run": false
}
```

The backend checks the current Alembic revision through the registered
`AlembicVersionMetaTable` and runs SQL transactionally when supported. It does
not validate, sign, checksum, or approve Alembic artifacts, and it does not
receive catalog-reconciliation metadata, custom operation plans, retry/lock
metadata, or execution-run rows.

## MetaTable Catalog Binding

MetaTables remain catalog metadata. They do not own migration execution records,
affected-table validation, or contract reconciliation during Alembic apply. After
Alembic creates or changes physical tables, project tooling should register or
refresh the relevant MetaTable bindings as a separate catalog step.

## Removed Path

These are intentionally unsupported:

- SDK-managed migration artifact table models
- packaged migration artifact rows
- `load_packaged_migration`
- `sync_packaged_migration`
- `build_migration_registry_row`
- custom `operations()` migration modules
- legacy SDK schema-migration SQLAlchemy base classes
- operation names such as `add_column` or `create_index`
- SQL-or-operations fallbacks

Use Alembic revisions and Alembic-rendered SQL only.
