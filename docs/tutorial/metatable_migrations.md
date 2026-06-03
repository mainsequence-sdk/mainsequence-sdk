# Part 2.1: MetaTable Migrations

This document is only about schema migrations for MetaTables.

The SDK does not implement a second migration engine. It provides a thin layer
on top of Alembic so MetaTable catalog bindings, backend physical table names, and
scoped database credentials are ready before Alembic runs.

It continues from [Part 2: Working With MetaTables](working_with_meta_tables.md),
where the project declared these backend-managed MetaTables:

- `Account`
- `AccountLimit`

Use this page for platform-managed MetaTable creation and evolution. The
migration provider is the normal lifecycle path; users do not call
`Model.register()` directly for platform-managed tables.

## Why Migrations Exist

Initial platform-managed registration reserves a catalog row and physical table
name, but Alembic creates the physical table. `migrations upgrade` resolves or
reserves provider-scoped models before Alembic SQL is rendered.

For shape-addressed platform-managed tables, the SDK derives `storage_hash` from
storage-relevant SQLAlchemy table attributes. If you add or remove a column in
the class and then call normal registration again, the newly constructed class
can point at a different logical storage identity before the SDK can recover the
previous table. That is not a migration. It is a new table identity.

Alembic migrations preserve a deployed table by changing the physical schema in
place. For a new provider-scoped platform-managed model, migration tooling first
uses the backend reservation path to reserve and bind the physical table name,
then renders/applies Alembic SQL against that name. Prefix explicit table
identifiers, explicit physical table names, and Alembic version table names with
the project or package name to avoid collisions in shared organizations and
schemas.

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
-> SDK reserves provider.metatable_models and binds physical table names
-> SDK obtains a scoped migration database URI
-> Alembic executes current/revision/upgrade directly
-> SDK finalizes reserved provider.metatable_models
```

The backend does not receive SDK custom operations such as `add_column`, and it
does not receive an SDK migration artifact row. It reserves MetaTable catalog
rows and issues a temporary database credential; Alembic owns the migration
execution.

The SDK layer is intentionally thin. Before delegating to Alembic, it:

- imports the selected provider
- registers or resolves the provider's `AlembicVersionMetaTable`
- reserves or resolves the provider-scoped platform-managed MetaTables for
  commands that generate or mutate provider schema, without creating physical
  application tables
- binds backend-reserved physical table names into SQLAlchemy metadata for those
  schema commands
- includes the Alembic version MetaTable UID, and when needed the provider
  MetaTable UIDs, in the migration scope
- requests a temporary table-scoped database URI from the target data source
- builds a normal Alembic `Config` with the provider's `script_location`,
  `target_metadata`, version-table settings, owner role, and output streams
- calls Alembic `current`, `revision`, `upgrade`, or `downgrade` directly

`current` is the cheap read-only path. It registers or resolves only the
Alembic version MetaTable, requests a credential scoped to that registry table,
and asks Alembic for current state. It must not restage provider application
MetaTables.

After Alembic `upgrade` or `downgrade`, the SDK calls TS Manager's
`finalize-managed` endpoint once for the reserved provider MetaTable UIDs. The
backend introspects the physical tables Alembic created and flips the catalog
rows from `reserved` to `active`. If the provider defines
`after_register_metatables`, the hook runs only after finalization reports every
provider MetaTable active.

The SDK layer does not parse revision files, generate SDK operation lists,
store backend migration artifacts, or ask the backend to apply rendered SQL.
Alembic revision files remain the schema history and Alembic executes DDL
through the scoped database credential.

The CLI lifecycle intentionally separates three jobs:

- `mainsequence migrations current` asks Alembic what revision is actually
  recorded in the target database through a backend-issued scoped credential.
- `mainsequence migrations revision` asks Alembic to write a normal revision
  file for the selected provider. Autogenerate runs against metadata after
  backend reservation has bound physical table names.
- `mainsequence migrations upgrade` runs Alembic directly through the scoped
  credential, then finalizes provider-scoped MetaTable catalog bindings after
  the database schema has changed.
- `mainsequence migrations downgrade` runs Alembic directly through the scoped
  credential, then finalizes the same provider-scoped catalog bindings.

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
    __metatable_identifier__ = "sdk_examples.alembic_version"
    __alembic_version_schema__ = "public"
    __alembic_version_table_name__ = "sdk_examples_alembic_version"
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
command needs backend migration state. `current` uses only that registry
MetaTable because it is read-only. `revision`, `upgrade`, and `downgrade` also
reserve or resolve provider application MetaTables before Alembic renders or
applies schema changes.

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
    __metatable_identifier__ = "sdk_examples.Account"
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
    __metatable_identifier__ = "sdk_examples.Account"
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
revision template, and Alembic configuration, then calls Alembic directly. The
registry is part of the thin SDK setup: commands auto-register or resolve the
provider's `AlembicVersionMetaTable`, and scoped-credential requests include
that registry UID so Alembic can read and write its physical version table.

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

For platform-managed physical names, the SDK reservation step binds Alembic's
SQLAlchemy metadata to the existing physical table names from the MetaTable
catalog before Alembic renders or executes SQL.

## 6. Check Current Revision

Read current Alembic state through the selected provider:

```bash
mainsequence migrations current \
  --provider mainsequence_migrations:migration
```

This command is needed because the source revision must come from the target
database, not from the local filesystem or from the newest package revision.
The CLI prepares the provider, asks the backend for a scoped migration
connection, and lets Alembic read the version table directly.

If Alembic prints no current revision, the CLI reports that explicitly. That
means the version table is empty or Alembic found no current revision for the
prepared migration scope.

## 7. Apply With Alembic

Apply the provider migration:

```bash
mainsequence migrations upgrade \
  --provider mainsequence_migrations:migration \
  head
```

`upgrade` is the mutation path. It runs the thin SDK setup, obtains a temporary
scoped database credential, and calls Alembic `upgrade` directly.

After Alembic succeeds, the CLI finalizes only the MetaTables listed in
`migration.metatable_models` by UID. The command succeeds only if Alembic
execution succeeds and TS Manager reports every provider MetaTable as `active`.
If a physical table is still missing, the SDK raises
`AlembicProviderPhysicalStateError` before any project catalog hook runs.

## 8. Backend Coordination For Reference

Users should normally apply migrations with `mainsequence migrations upgrade`.
Before and after Alembic runs, the CLI coordinates with TS Manager through
three request types.

First, it reserves provider-scoped platform-managed MetaTables without creating
physical application tables:

```json
{
  "tables": [
    {
      "identifier": "sdk_examples.Asset",
      "namespace": "sdk_examples",
      "data_source_uid": "dynamic-table-data-source-uid",
      "storage_hash": "logical-storage-hash",
      "schema_management": {
        "mode": "alembic_managed",
        "alembic": {
          "package": "sdk_examples",
          "migration_namespace": "sdk-examples",
          "provider_key": "sdk_examples:sdk-examples",
          "alembic_version_meta_table_uid": "alembic-version-metatable-uid"
        }
      },
      "table_contract": {
        "version": "relational-table.v1",
        "physical": {},
        "columns": [],
        "indexes": [],
        "foreign_keys": []
      }
    }
  ]
}
```

The backend returns reserved physical table names. The SDK binds those table
names into SQLAlchemy metadata before Alembic runs. Index names remain normal
SQLAlchemy/Alembic metadata. Platform-managed foreign-key contracts carry the
logical relationship only; Alembic, SQLAlchemy, and the database own the
physical FK constraint names.

Second, schema-changing commands request a short-lived migration credential
scoped to the Alembic version MetaTable UID plus the reserved provider
MetaTable UIDs:

```json
{
  "purpose": "schema_migration",
  "package": "sdk_examples",
  "migration_namespace": "sdk-examples",
  "meta_table_uids": [
    "alembic-version-metatable-uid",
    "reserved-metatable-uid"
  ],
  "ttl_seconds": 900
}
```

The Alembic version MetaTable is part of the scope because Alembic reads and
writes its version table before it runs application DDL. The returned URI is
secret. The CLI passes it to Alembic and does not print it. Alembic's normal
stdout and offline output buffers are forwarded through the CLI.

For `current`, the credential scope contains only the Alembic version MetaTable
UID because no provider application table should be reserved or changed.

Third, after Alembic succeeds, it finalizes the reserved provider rows:

```json
{
  "meta_table_uids": ["reserved-metatable-uid"],
  "migration_package": "sdk_examples",
  "migration_namespace": "sdk-examples",
  "alembic_version_meta_table_uid": "alembic-version-metatable-uid",
  "alembic_revision": "head"
}
```

This request intentionally does not include `data_source_uid`, `storage_hash`,
`table_contract`, labels, or provisioning options. It is a reconciliation step:
TS Manager verifies that Alembic created the physical tables and then marks the
reserved catalog rows active.

For development repair, use the explicit provider reset path:

```bash
mainsequence migrations reset \
  --provider mainsequence_migrations:migration \
  --confirm-reset
```

Reset is destructive and provider-scoped. It is the supported way to drop or
reserve Alembic-managed provider state when local Alembic revision state and
physical tables have drifted.

## 9. Catalog Registration Scope

Migration tooling reserves provider models before Alembic runs and finalizes
only those models after Alembic execution:

```python
finalize_response = migration.finalize_metatable_catalog(prepared=prepared)
```

This catalog step is not registration. The provider controls scope;
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

The hook receives the ordered finalized MetaTable objects and runs only after
`upgrade` has successfully applied Alembic migrations and finalized
provider-scoped MetaTables. It does not run for `current`, `revision`, or
failed `upgrade`.

If a project migrates `AccountLimit` instead of `Account`, use the same
provider lifecycle. The provider still owns revision discovery, version-table
binding, request identity, and post-apply catalog scope.

## Summary

Keep migrations when a deployed table must evolve in place. The migration files
are the database schema history. The `AlembicMetaTableMigration` provider is the
SDK boundary for selecting that history, Alembic metadata, version-table
binding, and catalog scope. The SDK layer prepares backend bindings and scoped
credentials, then delegates to Alembic. Alembic owns DDL execution and revision
state. Project tooling finalizes provider-scoped catalog bindings after the
physical schema has changed.
