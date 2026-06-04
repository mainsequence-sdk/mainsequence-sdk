# MetaTable Migrations

MetaTable schema migrations use Alembic. The SDK adds a thin coordination layer
around Alembic so provider-scoped MetaTables can be bound to backend catalog
state before Alembic runs. The SDK does not provide a parallel operation-list
migration language or a separate SDK artifact table.

## Architecture

The migration lifecycle is provider-based:

```text
AlembicMetaTableMigration provider
-> Alembic revision from provider.target_metadata
-> SDK requests a provider migration database URI
-> SDK reserves provider.metatable_models only for upgrade/downgrade
-> Alembic executes revision/current/upgrade/downgrade directly
-> project tooling refreshes provider.metatable_models
```

Alembic owns revision files, current revision reads, upgrade/downgrade
operations, and the physical `alembic_version` table.

The SDK owns:

- the `AlembicMetaTableMigration` provider contract
- `AlembicVersionMetaTable`
- provider discovery by convention plus explicit `--provider`
- typed reservation and migration-connection request/response models
- CLI commands that prepare providers, request migration credentials, reserve
  and bind MetaTable UID/storage metadata only for schema-mutating commands,
  call Alembic directly, and refresh provider-scoped MetaTables after successful
  upgrade

## SDK Alembic Coordination

The goal is to keep Alembic as the schema migration engine and add only the
small amount of SDK coordination needed for Main Sequence MetaTables. The
`mainsequence migrations ...` commands are thin wrappers around normal Alembic
commands. They do not replace Alembic and they do not interpret a custom SDK
migration format.

Before calling Alembic, the SDK layer does only the platform-specific setup
Alembic cannot know on its own:

- load the selected `AlembicMetaTableMigration` provider
- register or resolve the provider's `AlembicVersionMetaTable` catalog binding
- reserve or resolve the provider-scoped platform-managed MetaTables without
  creating physical application tables for `upgrade` and `downgrade`
- preserve authored SQLAlchemy table names and bind backend MetaTable
  UID/storage metadata into the provider models
- keep the Alembic version MetaTable UID and provider MetaTable UIDs in prepared
  state for finalization and reporting
- request a temporary provider migration credential from the owning
  `DynamicTableDataSource`
- build an Alembic `Config` with `script_location`, `sqlalchemy.url`,
  `version_table`, `version_table_schema`, provider metadata, owner role, and
  CLI output streams
- call Alembic `current`, `revision`, `upgrade`, or `downgrade` directly

After Alembic `upgrade` or `downgrade`, the SDK refreshes only the
provider-scoped MetaTable catalog rows with physical table creation disabled and
then runs `after_register_metatables` when the provider defines that hook.

The SDK layer does not:

- generate SDK operation objects
- parse Alembic revision files
- own Alembic revision state
- create a backend migration artifact row
- send a rendered SQL artifact to the backend for apply
- apply DDL through a custom backend migration endpoint

Alembic executes DDL through the provider migration credential. The backend owns
catalog reservation and credential issuance; it does not own the Alembic
migration lifecycle or FK/index naming.

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
    AlembicMetaTableCatalogRefreshContext,
    AlembicMetaTableMigration,
    AlembicVersionMetaTable,
)
from mainsequence.meta_tables import schema_table_name

from sdk_examples.meta_tables.account_limits import Account, AccountLimit, Base


def refresh_project_catalog_from_registered_metatables(
    context: AlembicMetaTableCatalogRefreshContext,
):
    registered_metatables = context.registered_metatables
    reserved_policy = context.reserved_policy
    ...


class ProjectAlembicVersion(AlembicVersionMetaTable):
    __metatable_namespace__ = "sdk-examples"
    __metatable_identifier__ = "sdk_examples.alembic_version"
    __alembic_version_schema__ = "public"
    __alembic_version_table_name__ = schema_table_name("sdk_examples", "alembic_version")


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

Application MetaTable catalog sync in the Alembic workflow resolves by the
SQLAlchemy table name. `prepare_for_alembic()` sends each provider model's
current `Table.name` as the reservation `identifier`, and uses the same table
name for same-batch FK `target_identifier` references before backend UIDs
exist. `__metatable_identifier__` is not the Alembic migration identity. A model
move or rename keeps the same migration identity when its SQLAlchemy table name
stays stable.

`prepare_for_alembic()` runs for `upgrade` and `downgrade`; revision generation
does not provision provider MetaTables. When it runs, it sends provider model rows through typed
collection-create endpoints. Plain `PlatformManagedMetaTable` rows go to
`POST /orm/api/ts_manager/meta_table/`; `PlatformTimeIndexMetaData` rows go to
`POST /orm/api/ts_manager/dynamic_table/`. The SDK sends raw JSON lists with
`provisioning_status="reserved"` and `is_alembic_managed=true`; it does not
wrap the rows in a reservation request object. Alembic is the authority for
schema changes, and TS Manager is the authoritative owner of MetaTable catalog
rows and migration-scoped credentials.

## Alembic Version MetaTable

`AlembicVersionMetaTable` registers a catalog pointer for Alembic's version
table. It uses the minimal known `version_num` contract required by normal
external MetaTable registration; Alembic and PostgreSQL remain responsible for
the physical version table. Migration commands force this registration
idempotently so the backend `MetaTable` pointer is recreated if it was deleted;
the SDK class cache is not considered authoritative.

The generated contract declares the Alembic revision column:

```json
{
  "physical": {"table_name": "sdk_examples__alembic_version"},
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
    "version_table": "sdk_examples__alembic_version"
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
mainsequence migrations upgrade --provider mainsequence_migrations:migration head
```

`revision` accepts an optional `-m/--message`. If it is omitted, the CLI passes
`migration` to Alembic. The command builds a local Alembic `Config` from the
provider and calls Alembic `revision` directly. Autogenerate is enabled by
default. If `--sqlalchemy-url` is supplied, Alembic reflects that database;
otherwise the CLI requests a provider migration connection without MetaTable
UIDs. `revision` does not register, reserve, or finalize provider MetaTables.

The standard `revision` command writes an Alembic revision file for the
provider. It does not build SDK migration operations and it does not ask the
backend to render or apply SQL. Alembic owns revision generation.

`upgrade` and `downgrade` reserve provider-scoped platform-managed MetaTables,
bind MetaTable UID/storage metadata while preserving authored SQLAlchemy table
names, ask TS Manager for a temporary provider migration URI without MetaTable
UIDs, build a normal Alembic `Config`, and call Alembic directly. `current`
only needs the Alembic version MetaTable binding and a provider migration
credential because it is read-only for application MetaTables.

There is no normal-user `render` or `upgrade --dry-run` path. Alembic is the
execution path. The backend only provides registry reservation and the scoped
database credential; it does not receive an SDK SQL artifact to apply.

After Alembic `upgrade` succeeds, the CLI calls `finalize-managed/` for the
provider-scoped catalog rows. The command succeeds only when both Alembic
execution and backend finalization succeed.

If the provider defines `after_register_metatables`, the CLI runs that hook only
after provider-scoped backend finalization succeeds. The hook receives an
`AlembicMetaTableCatalogRefreshContext`. It does not run for `current`,
`revision`, or failed `upgrade`.

## Backend Coordination

The migration CLI coordinates with backend endpoints around Alembic:

- `POST /orm/api/ts_manager/meta_table/` creates or resolves regular
  platform-managed MetaTable catalog rows without creating physical application
  tables.
- `POST /orm/api/ts_manager/dynamic_table/` creates or resolves time-indexed
  platform-managed MetaTable catalog rows without creating physical application
  tables.
- `POST /orm/api/ts_manager/dynamic_table_data_source/<uid>/migration-connection/`
  issues a temporary provider migration credential. The request does not include
  MetaTable UIDs.
- `POST /orm/api/ts_manager/meta_table/finalize-managed/` activates reserved
  MetaTables after Alembic creates or alters the physical tables.

The returned database URI is a secret and should not be printed, logged, or
stored in project files.

## MetaTable Catalog Binding

MetaTables remain catalog metadata. They do not own migration execution records,
affected-table validation, or contract reconciliation during Alembic apply.

Before Alembic renders SQL, project tooling resolves/reserves only the
application MetaTable classes listed in `migration.metatable_models` and binds
their backend `MetaTable.uid`/storage metadata while preserving authored table
names. After Alembic changes physical tables, project tooling finalizes the same
catalog scope. Provider scope prevents unrelated imported models from being
registered by migration tooling.

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
