# Part 2.1: MetaTable Migrations

This document is only about schema migrations for MetaTables.

It continues from [Part 2: Working With MetaTables](working_with_meta_tables.md),
where the project declared these backend-managed MetaTables:

- `Account`
- `AccountLimit`

Use this page for platform-managed MetaTable creation and evolution. The
migration provider is the normal lifecycle path; users do not call
`Model.register()` directly for platform-managed tables.

## Why Migrations Exist

Initial platform-managed registration still creates a table from a known
contract, but migration tooling is the caller. `migrations upgrade` resolves or
registers provider-scoped models before Alembic SQL is rendered.

For shape-addressed platform-managed tables, the SDK derives `storage_hash` from
storage-relevant SQLAlchemy table attributes. If you add or remove a column in
the class and then call normal registration again, the newly constructed class
can point at a different logical storage identity before the SDK can recover the
previous table. That is not a migration. It is a new table identity.

Alembic migrations preserve a deployed table by changing the physical schema in
place. For a new provider-scoped platform-managed model, migration tooling first
uses the existing backend registration path to create and bind the physical
table, then renders/applies Alembic SQL against that backend physical name.

Use MetaTable migrations when:

- you are creating a brand-new platform-managed logical table
- an existing physical table already contains production or shared data
- a MetaTable UID is already used by governed SQL scopes, APIs, dashboards, or jobs
- a foreign key points at the existing MetaTable
- you need ordered, reviewable, reproducible DDL across environments
- you need the database to track current schema revision

Do not bypass the migration workflow for platform-managed tables. Changes that
intentionally create a separate table identity should still be expressed by the
provider and applied through `migrations upgrade`.

## Provider-Based Lifecycle

The SDK migration entry point is one selected
`AlembicMetaTableMigration` provider. The provider is the boundary between the
host project, Alembic, MetaTable catalog bindings, and backend execution.

The lifecycle is:

```text
provider object
-> Alembic revision from provider.target_metadata
-> Alembic renders SQL from provider.script_location
-> SDK sends the SQL artifact with provider package/namespace identity
-> TS Manager executes SQL
-> project tooling registers or refreshes provider.metatable_models
```

The backend receives SQL rendered by Alembic from installed project code. It
does not receive SDK custom operations such as `add_column`, and it does not
receive an SDK migration artifact row.

The CLI lifecycle intentionally separates four jobs:

- `mainsequence migrations current` asks the backend what Alembic revision is
  actually recorded in the target database. This prevents the client from
  rendering SQL from an assumed revision and lets the backend resolve the target
  data source from the registered Alembic version MetaTable.
- `mainsequence migrations revision` asks Alembic to write a normal revision
  file for the selected provider. The standard path creates a revision file
  that the developer fills with explicit `op.create_table(...)`,
  `op.add_column(...)`, and downgrade operations.
- `mainsequence migrations render` resolves or registers provider-scoped
  platform-managed MetaTables, binds SQLAlchemy models to backend physical
  names, then runs Alembic in offline SQL mode and prints the exact SQL artifact
  that would be sent to TS Manager. This does not apply DDL, but it can create
  missing platform-managed catalog rows because Alembic needs stable backend
  physical names before rendering.
- `mainsequence migrations upgrade` is the only command that sends the rendered
  SQL to the backend. It dry-runs the same artifact first, applies it when not
  run with `--dry-run`, then refreshes provider-scoped MetaTable catalog
  bindings after the database schema has changed.

## 1. Define The Migration Provider

Put this code in a Python module that the CLI can import. The simplest location
is `mainsequence_migrations.py` at the project root, next to `pyproject.toml`:

```text
your-project/
  pyproject.toml
  mainsequence_migrations.py
  sdk_examples/
    meta_tables/
      account_limits.py
    migrations/
      __init__.py
      env.py
      script.py.mako
      versions/
        __init__.py
```

This file is not registered with the backend. It is a local provider module.
The CLI imports `mainsequence_migrations:migration` to know which Alembic
environment, SQLAlchemy metadata, version-table binding, and MetaTable models
belong to this migration stream.

The Alembic version-table binding is registered automatically by commands that
need backend state, such as `mainsequence migrations current` and
`mainsequence migrations upgrade`.

The provider below uses the same `Base`, `Account`, and `AccountLimit` model
classes from Part 2:

```python
from mainsequence.meta_tables.migrations import (
    AlembicMetaTableMigration,
    AlembicVersionMetaTable,
)

from sdk_examples.meta_tables.account_limits import Account, AccountLimit, Base


class TutorialAlembicVersion(AlembicVersionMetaTable):
    __metatable_namespace__ = "sdk-examples"
    __metatable_identifier__ = "sdk-examples.alembic_version"
    __alembic_version_schema__ = "public"
    __alembic_version_table_name__ = "alembic_version"
    __alembic_version_column_name__ = "version_num"


migration = AlembicMetaTableMigration(
    package="sdk_examples",
    migration_namespace="sdk-examples",
    script_location="sdk_examples:migrations",
    target_metadata=Base.metadata,
    alembic_registry=TutorialAlembicVersion,
    metatable_models=[
        Account,
        AccountLimit,
    ],
)
```

The provider supplies:

- `package` and `migration_namespace` for backend request identity
- `script_location` for Alembic revision discovery and rendering
- `target_metadata` for Alembic autogenerate
- `alembic_registry`, the SDK catalog binding for Alembic's physical version table
- `metatable_models`, the exact application MetaTables project tooling may register or refresh after SQL execution
- optional `after_register_metatables`, a project hook for refreshing catalog
  tables after provider-scoped MetaTables register successfully

The SDK must not infer migrations from every imported SQLAlchemy model or every
installed dependency. The selected provider controls the migration scope.

Application MetaTables are resolved by exact `identifier` during catalog sync.
If a model declares `__metatable_identifier__`, that value is used exactly. If
it does not, the SDK derives one from `[project].name` in `pyproject.toml` plus
the model path:

```text
<pyproject project name>:<model.__module__>.<model.__qualname__>
```

If the project name cannot be resolved, set `__metatable_identifier__`
explicitly. If a model is renamed or moved but should keep the same platform
identity, pin the old identifier on the class.

If you do not want a root-level `mainsequence_migrations.py`, put the same
provider object in your package, for example `sdk_examples/migrations/__init__.py`,
and pass it explicitly:

```bash
mainsequence migrations current --provider sdk_examples.migrations:migration
```

## 2. Create The Alembic Environment

Create the full Alembic environment under the provider's `script_location`.
For `script_location="sdk_examples:migrations"`, create exactly these files:

```text
sdk_examples/
  migrations/
    __init__.py
    env.py
    script.py.mako
    versions/
      __init__.py
```

The `__init__.py` files can be empty. The `versions/` directory is where
`mainsequence migrations revision` writes generated Alembic revision files.

`env.py` tells Alembic how to load the selected provider:

```python
from alembic import context

from mainsequence_migrations import migration


def include_name(name, type_, parent_names):
    return migration.include_name(name, type_, parent_names)


def run_migrations_offline():
    context.configure(
        url=context.config.get_main_option("sqlalchemy.url"),
        target_metadata=migration.target_metadata,
        version_table=migration.version_table,
        version_table_schema=migration.version_table_schema,
        include_name=include_name,
        compare_type=True,
        compare_server_default=True,
        literal_binds=True,
    )
    with context.begin_transaction():
        context.run_migrations()


run_migrations_offline()
```

The important parts are:

- Alembic reads `target_metadata` from the selected provider
- the physical version table comes from `migration.alembic_registry`
- `include_name` delegates to the provider so unrelated imported tables stay out of this migration stream

`script.py.mako` is required by `mainsequence migrations revision`. Alembic
uses this template to write each generated file under `versions/`. If it is
missing, revision generation fails with a `FileNotFoundError` for
`script.py.mako`.

Use this minimal template:

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

## 3. Version Table Binding Is Automatic

The provider's `AlembicVersionMetaTable` is registered automatically when a
command needs backend migration state. `current` and `upgrade` call the same
registration path before building status or apply requests.

Initial registration resolves the data source through the same resolver used by
normal MetaTable registration. After registration, the bound
`AlembicVersionMetaTable` supplies the backend-resolved target data source for
status and apply.

The registered UID becomes `alembic_version_meta_table_uid` in backend status
and apply requests. It is the UID of Alembic's version-table catalog binding,
not the UID of `Account` or `AccountLimit`.

## 4. Change The SQLAlchemy Model

Suppose the existing `Account` table now needs a lifecycle status. The Part 2
model had:

```python
class Account(PlatformManagedMetaTable, Base):
    __metatable_namespace__ = NAMESPACE
    __metatable_identifier__ = "Account"
    __metatable_extra_hash_components__ = {"storage_name": "account"}

    uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    account_code: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    region: Mapped[str] = mapped_column(String(32), nullable=False)
```

The new model adds `status`:

```python
class Account(PlatformManagedMetaTable, Base):
    __metatable_namespace__ = NAMESPACE
    __metatable_identifier__ = "Account"
    __metatable_extra_hash_components__ = {"storage_name": "account"}

    uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    account_code: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    region: Mapped[str] = mapped_column(String(32), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, server_default="active")
```

Do not treat a normal shape-based `Account.register()` call as the migration.
Alembic must generate and apply physical DDL first.

## 5. Generate An Alembic Revision

Use the provider-scoped CLI command:

```bash
mainsequence migrations revision \
  --provider mainsequence_migrations:migration
```

This command is needed because the changed SQLAlchemy class is only desired
state. Alembic must turn that desired state into an ordered revision file with
an explicit parent revision, `upgrade()`, and `downgrade()`. Without this step,
there is no durable schema history for review, rollback, or replay across
environments.

`-m/--message` is optional. Use it when you want a descriptive Alembic file
name, for example `-m "add account status"`; otherwise the SDK passes
`migration`.

The Alembic version MetaTable registry is not what generates this file's table
operations. `revision` uses the provider to find `script_location`, the
revision template, and Alembic configuration. The registry is integrated later:
`current` and `upgrade` auto-register or resolve the provider's
`AlembicVersionMetaTable`, and status/apply requests send that registry UID so
the backend can resolve the target data source and read Alembic's physical
version table.

`--autogenerate` is optional and not the standard path. If used, Alembic must
connect to an explicit baseline database through `--sqlalchemy-url` so it can
compare that database with `migration.target_metadata`. For an initial
migration the baseline should be an empty database; for later migrations it
must represent the previous Alembic revision.

The revision file is a normal Alembic revision. It should contain database DDL,
not SDK operation lists:

```python
from alembic import op
import sqlalchemy as sa


revision = "0002_add_account_status"
down_revision = "0001_initial"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "account",
        sa.Column(
            "status",
            sa.String(length=32),
            nullable=False,
            server_default="active",
        ),
        schema="public",
    )


def downgrade() -> None:
    op.drop_column("account", "status", schema="public")
```

For platform-managed physical names, project tooling must render Alembic SQL
against the existing physical table names from the MetaTable catalog. The
provider controls that mapping through its Alembic environment and metadata
setup.

## 6. Check Current Revision

Read current Alembic state through the selected provider:

```bash
mainsequence migrations current \
  --provider mainsequence_migrations:migration
```

This command is needed because the source revision must come from the target
database, not from the local filesystem or from the newest package revision.
The backend reads Alembic state through the registered version-table MetaTable
and reports the revision that SQL rendering must start from.

Programmatically, this is the same status request the CLI builds from the
provider:

```python
from mainsequence.client.metatables import AlembicMigrationStatusRequest, MetaTable
from mainsequence.meta_tables.migrations import load_alembic_metatable_migration_provider


migration = load_alembic_metatable_migration_provider("mainsequence_migrations:migration")
alembic_version_meta_table = migration.ensure_alembic_registry()

status = MetaTable.get_migration_status(
    AlembicMigrationStatusRequest(
        alembic_version_meta_table_uid=migration.alembic_registry.get_meta_table_uid(),
        package=migration.package,
        migration_namespace=migration.migration_namespace,
    )
)
```

Use `status.current_revision` as the source revision for rendering the next
transition.

## 7. Render SQL Without Applying

Render the exact SQL artifact that would be sent to TS Manager:

```bash
mainsequence migrations render \
  --provider mainsequence_migrations:migration \
  --to head
```

This command is needed because Alembic revisions are Python migration code, but
the backend execution contract accepts SQL. Rendering converts the revision
range from `current` to `--to` into a concrete SQL artifact that can be
inspected, logged, dry-run, and reused for apply.

The provider determines:

- revision script location
- package and migration namespace
- Alembic version table path
- target metadata and inclusion hooks

Programmatic rendering uses the provider-aware helper:

```python
from mainsequence.meta_tables.migrations import render_packaged_alembic_migration_for_provider


artifact = render_packaged_alembic_migration_for_provider(
    migration,
    revision="head",
    current_revision=status.current_revision,
)
```

`artifact.sql` is what TS Manager executes. `artifact.manifest` is metadata for
observability and revision checks; it is not a second migration language.

The CLI `render` command resolves or registers missing provider-scoped
platform-managed MetaTables before rendering so Alembic uses backend physical
table names. It does not apply DDL.

## 8. Dry-Run And Apply

Validate the rendered artifact first:

```bash
mainsequence migrations upgrade \
  --provider mainsequence_migrations:migration \
  --to head \
  --dry-run
```

`upgrade --dry-run` is needed because it validates the backend request, current
revision precondition, registry binding, and SQL artifact without executing the
DDL or running post-apply catalog refresh. It still resolves or registers
missing provider-scoped platform-managed MetaTables before validation, because
the SQL artifact must be rendered against backend physical table names.

Apply the same rendered artifact after validation and sync provider-scoped
MetaTables:

```bash
mainsequence migrations upgrade \
  --provider mainsequence_migrations:migration \
  --to head
```

`upgrade` without `--dry-run` is needed because it is the single mutation path:
it sends the Alembic-rendered SQL to TS Manager, waits for backend execution,
then refreshes the SDK MetaTable catalog bindings for the models listed by the
provider.

The backend checks the current Alembic revision, executes the Alembic-rendered
SQL transactionally when supported, and relies on Alembic SQL to update the
physical `alembic_version` table. After SQL apply succeeds, the CLI refreshes
only the MetaTables listed in `migration.metatable_models` by exact
`identifier`; the command succeeds only if both SQL execution and catalog sync
succeed.

## 9. Backend Request Shape For Reference

Users should normally apply migrations with `mainsequence migrations upgrade`.
The CLI builds and sends this backend request shape from the selected provider,
registered Alembic version table, current revision, and rendered Alembic SQL:

```json
{
  "version": "metatable-migration.v1",
  "alembic_version_meta_table_uid": "registered-alembic-version-metatable-uid",
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
  "dry_run": true
}
```

For apply, the CLI sends the same rendered artifact again with `dry_run=false`
after validation. The apply request does not contain
affected-table lists, old/new contract hashes, custom operation names,
idempotency keys, lock keys, or migration-row UIDs.

## 10. Catalog Registration Scope

Migration tooling resolves/registers provider models before SQL rendering and
refreshes only those models after SQL execution:

```python
registered = migration.sync_metatable_catalog()
```

This catalog step resolves by exact `identifier`, binds changed model classes
to existing MetaTable rows when present, registers missing platform-managed
identifiers through the existing backend registration path inside migration
tooling, and fails if an identifier is duplicated. It does not pass the Alembic
version-table data source into application model registration; each model uses
its own normal MetaTable data-source binding. The provider controls scope;
imported-but-unlisted SQLAlchemy/MetaTable classes are ignored by migration
tooling.

Projects that maintain a derived catalog can attach an optional provider hook:

```python
migration = AlembicMetaTableMigration(
    ...,
    metatable_models=[
        Account,
        AccountLimit,
    ],
    after_register_metatables=refresh_example_catalog_from_registered_metatables,
)
```

The hook receives the ordered registered MetaTable objects and runs only after
`upgrade` has successfully applied SQL and synced provider-scoped MetaTables. It
does not run for `current`, `render`, `revision`, dry-run, or failed apply.

If a project migrates `AccountLimit` instead of `Account`, use the same
provider lifecycle. The provider still owns revision discovery, version-table
binding, request identity, and post-apply catalog scope.

## Summary

Keep migrations when a deployed table must evolve in place. The migration files
are the database schema history. The `AlembicMetaTableMigration` provider is the
SDK boundary for selecting that history, Alembic metadata, version-table
binding, and catalog scope. Alembic owns DDL and revision state. TS Manager
executes Alembic-rendered SQL. Project tooling registers or refreshes
provider-scoped catalog bindings after the physical schema has changed.
