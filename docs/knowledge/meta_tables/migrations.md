# MetaTable Migrations

MetaTable schema migrations use Alembic. The SDK does not provide a parallel
operation-list migration language or a separate SDK artifact table.

The protocol version for backend execution is:

```text
metatable-migration.v1
```

## Architecture

The migration lifecycle is provider-based:

```text
AlembicMetaTableMigration provider
-> Alembic revision from provider.target_metadata
-> Alembic renders SQL from provider.script_location
-> SDK sends the Alembic-rendered SQL artifact to TS Manager
-> TS Manager executes SQL
-> project tooling registers or refreshes provider.metatable_models separately
```

Alembic owns revision files, upgrade/downgrade operations, offline SQL
rendering, and the physical `alembic_version` table.

The SDK owns:

- the `AlembicMetaTableMigration` provider contract
- `AlembicVersionMetaTable`
- provider discovery by convention plus explicit `--provider`
- typed backend request/response models
- CLI commands that create revisions, render SQL, dry-run/apply SQL, read current state, register the version table, and optionally register provider-scoped MetaTables

For a walkthrough that evolves the `Account` and `AccountLimit` MetaTables from
the tutorial, see
[Part 2.1: MetaTable Migrations](../../tutorial/metatable_migrations.md).

## Provider Object

Each project defines one selected provider object in an importable Python
module. The simplest conventional location is `mainsequence_migrations.py` at
the project root, next to `pyproject.toml`:

```text
your-project/
  pyproject.toml
  mainsequence_migrations.py
  sdk_examples/
    migrations/
      env.py
      versions/
```

This provider module is not registered with the backend. The CLI imports it to
build Alembic configuration and backend request metadata. The backend
registration step is only for the provider's `AlembicVersionMetaTable` binding.

```python
from mainsequence.meta_tables.migrations import (
    AlembicMetaTableMigration,
    AlembicVersionMetaTable,
)

from sdk_examples.meta_tables.account_limits import Account, AccountLimit, Base


def refresh_project_catalog_from_registered_metatables(registered_metatables):
    ...


class ProjectAlembicVersion(AlembicVersionMetaTable):
    __metatable_namespace__ = "sdk-examples"
    __metatable_identifier__ = "sdk-examples.alembic_version"
    __alembic_version_schema__ = "public"
    __alembic_version_table_name__ = "alembic_version"


migration = AlembicMetaTableMigration(
    package="sdk_examples",
    migration_namespace="sdk-examples",
    script_location="sdk_examples:migrations",
    target_metadata=Base.metadata,
    alembic_registry=ProjectAlembicVersion,
    metatable_models=[Account, AccountLimit],
    after_register_metatables=refresh_project_catalog_from_registered_metatables,
)
```

Provider discovery order:

1. `mainsequence_migrations.py:migration` in the current project root.
2. `<current_package>.migrations:migration` when the CLI can infer the package.
3. `--provider module.path:migration` as an explicit override.

For package-local providers, put the same `migration` object in a package module
such as `sdk_examples/migrations/__init__.py` and pass
`--provider sdk_examples.migrations:migration`.

The provider is the scope boundary. The SDK must not scan every imported model
or every installed package to decide what to migrate.

`after_register_metatables` is optional. Use it only when the project has a
derived catalog that must refresh after the provider-scoped MetaTable models are
registered successfully.

## Alembic Version MetaTable

`AlembicVersionMetaTable` registers a catalog pointer for Alembic's version
table. It uses the minimal known `version_num` contract required by normal
external MetaTable registration; Alembic and PostgreSQL remain responsible for
the physical version table.

```python
request = migration.alembic_registry.build_registration_request(
    data_source_uid=DATA_SOURCE_UID,
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

Register it through the provider:

```python
alembic_version_meta_table = migration.register_alembic_registry(
    data_source_uid=DATA_SOURCE_UID,
)
```

or through the CLI:

```bash
mainsequence migrations register-version-table \
  --provider mainsequence_migrations:migration
```

## CLI Workflow

The provider-based CLI workflow is:

```bash
mainsequence migrations current --provider mainsequence_migrations:migration
mainsequence migrations revision --provider mainsequence_migrations:migration --autogenerate -m "initial"
mainsequence migrations register-version-table --provider mainsequence_migrations:migration
mainsequence migrations render --provider mainsequence_migrations:migration --to head
mainsequence migrations upgrade --provider mainsequence_migrations:migration --to head --dry-run
mainsequence migrations upgrade --provider mainsequence_migrations:migration --to head --apply
```

Use `--register-metatables` on `upgrade --apply` when the project should also
register provider-scoped application MetaTables after SQL execution:

```bash
mainsequence migrations upgrade \
  --provider mainsequence_migrations:migration \
  --to head \
  --apply \
  --register-metatables
```

If the provider defines `after_register_metatables`, the CLI runs that hook only
after the provider-scoped MetaTable registration succeeds. The hook receives the
ordered registered MetaTable objects. It does not run for `current`, `render`,
`revision`, dry-run, or failed apply.

## Backend Apply Payload

The backend apply endpoint receives Alembic-rendered SQL plus manifest metadata.
It does not receive custom SDK operations and it does not read a separate SDK
artifact row.

The SDK builds the payload from the selected provider:

```json
{
  "version": "metatable-migration.v1",
  "data_source_uid": "uuid",
  "alembic_version_meta_table_uid": "uuid",
  "package": "sdk_examples",
  "migration_namespace": "sdk-examples",
  "revision": "0002_add_account_status",
  "down_revision": "0001_initial",
  "direction": "upgrade",
  "expected_current_revision": "0001_initial",
  "manifest": {
    "package": "sdk_examples",
    "migration_namespace": "sdk-examples",
    "revision": "0002_add_account_status",
    "down_revision": "0001_initial",
    "direction": "upgrade",
    "alembic_version_table": "public.alembic_version"
  },
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
affected-table validation, or contract reconciliation during Alembic apply.

After Alembic creates or changes physical tables, project tooling can register
or refresh only the application MetaTable classes listed in
`migration.metatable_models`. Provider scope prevents unrelated imported models
from being registered by migration tooling.

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

Use provider-selected Alembic revisions and Alembic-rendered SQL only.
