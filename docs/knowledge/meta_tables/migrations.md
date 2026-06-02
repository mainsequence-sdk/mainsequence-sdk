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
-> SDK resolves/registers provider.metatable_models and binds physical names
-> Alembic renders SQL from provider.script_location
-> SDK sends the Alembic-rendered SQL artifact to TS Manager
-> TS Manager executes SQL
-> project tooling refreshes provider.metatable_models
```

Alembic owns revision files, upgrade/downgrade operations, offline SQL
rendering, and the physical `alembic_version` table.

The SDK owns:

- the `AlembicMetaTableMigration` provider contract
- `AlembicVersionMetaTable`
- provider discovery by convention plus explicit `--provider`
- typed backend request/response models
- CLI commands that create revisions, render SQL, dry-run/apply SQL, read
  current state, auto-register the version table when needed, and sync
  provider-scoped MetaTables

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
      __init__.py
      env.py
      script.py.mako
      versions/
        __init__.py
```

The Alembic environment must include `script.py.mako`. Alembic uses that
template when `mainsequence migrations revision` writes a new file under
`versions/`; without it, revision generation fails before any migration file is
created.

Minimal `script.py.mako`:

```mako
"""${message}

Revision ID: ${up_revision}
Revises: ${down_revision}
Create Date: ${create_date}
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
${imports if imports else ""}


revision: str = ${repr(up_revision)}
down_revision: Union[str, Sequence[str], None] = ${repr(down_revision)}
branch_labels: Union[str, Sequence[str], None] = ${repr(branch_labels)}
depends_on: Union[str, Sequence[str], None] = ${repr(depends_on)}


def upgrade() -> None:
    ${upgrades if upgrades else "pass"}


def downgrade() -> None:
    ${downgrades if downgrades else "pass"}
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

Application MetaTable catalog sync resolves by exact `identifier` only. Explicit
`__metatable_identifier__` values are used as-is. When omitted, the SDK derives
the identifier from `[project].name` in `pyproject.toml` plus
`<model.__module__>.<model.__qualname__>`. Pin an explicit identifier when a
model moves or is renamed but should keep the same platform identity.

## Alembic Version MetaTable

`AlembicVersionMetaTable` registers a catalog pointer for Alembic's version
table. It uses the minimal known `version_num` contract required by normal
external MetaTable registration; Alembic and PostgreSQL remain responsible for
the physical version table.

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

The CLI registers this binding automatically when a command needs backend
migration state, such as `current` or `upgrade`. Initial registration resolves
the data source through the same resolver used by normal MetaTable
registration; it does not accept a data-source override.

## CLI Workflow

The provider-based CLI workflow is:

```bash
mainsequence migrations current --provider mainsequence_migrations:migration
mainsequence migrations revision --provider mainsequence_migrations:migration
mainsequence migrations render --provider mainsequence_migrations:migration --to head
mainsequence migrations upgrade --provider mainsequence_migrations:migration --to head --dry-run
mainsequence migrations upgrade --provider mainsequence_migrations:migration --to head
```

`revision` accepts an optional `-m/--message`. If it is omitted, the CLI passes
`migration` to Alembic.

The standard `revision` command writes an Alembic revision file for the
provider; it does not call the backend and does not use the registered
`AlembicVersionMetaTable` to invent table operations. The version-table
registry is used by `current` and `upgrade`: those commands auto-register or
resolve the registry MetaTable, send its UID to the backend, and let the
backend resolve the target data source from that registry binding.

`revision --autogenerate` is optional. It requires an explicit
`--sqlalchemy-url` because Alembic must connect to a baseline database to
compute the diff against `migration.target_metadata`.

`render` resolves provider-scoped application MetaTables by identifier before
SQL generation. Existing rows are bound to their backend physical table names;
missing platform-managed rows are created through the existing backend
registration path inside migration tooling. This is why rendered Alembic SQL
targets backend physical table names instead of logical class names.

`upgrade --dry-run` validates the rendered SQL and revision precondition. It
does not execute DDL and does not run post-apply catalog refresh, but it still
performs the same pre-render model resolution described above.

After SQL apply, `upgrade` refreshes the same provider-scoped catalog rows. It
does not pass the Alembic version-table data source into application model
registration. The command succeeds only when both backend SQL execution and
catalog sync succeed.

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

Before Alembic renders SQL, project tooling resolves/registers only the
application MetaTable classes listed in `migration.metatable_models` and binds
them to backend physical table names. After Alembic changes physical tables,
project tooling refreshes the same catalog scope. Provider scope prevents
unrelated imported models from being registered by migration tooling.

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
