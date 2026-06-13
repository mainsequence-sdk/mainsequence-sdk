# Part 2.1: MetaTable Migrations

This document is only about schema migrations for MetaTables.

The SDK does not implement a second migration engine. It provides a thin layer
on top of Alembic so MetaTable catalog bindings, authored physical table names,
and provider migration credentials are ready before Alembic runs.

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

For platform-managed tables, Alembic owns physical schema evolution while the
MetaTable catalog row is reconciled by backend-supported identities such as
`uid`, logical `identifier`, data source, and physical table name. If you add or
remove a column in the class and then call normal registration directly, you are
bypassing the migration lifecycle. That is not a migration.

Alembic migrations preserve a deployed table by changing the physical schema in
place. For a new provider-scoped platform-managed model, `migrations upgrade`
uses the backend reservation path to resolve or reserve the MetaTable catalog
row for the authored SQLAlchemy table name, then applies Alembic SQL against
that name. Use stable SQLAlchemy table names for provider models, and generate
them with `schema_table_name(project_or_app, concept)`. Prefix Alembic version
table names the same way to avoid collisions in shared organizations and
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
-> SDK obtains a provider migration database URI
-> SDK reserves provider.metatable_models only for upgrade/downgrade
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
  commands that mutate provider schema, without creating physical application
  tables
- preserves authored SQLAlchemy table names and binds backend MetaTable
  UID/storage metadata for those schema commands
- keeps the Alembic version MetaTable UID and provider MetaTable UIDs in
  prepared/finalization state, but does not send them to the migration
  credential endpoint
- requests a temporary provider migration URI from the target data source
- builds a normal Alembic `Config` with the provider's `script_location`,
  `target_metadata`, version-table settings, owner role, and output streams
- calls Alembic `current`, `revision`, `upgrade`, or `downgrade` directly

`current` is the cheap read-only path. It registers or resolves only the
Alembic version MetaTable, requests a provider migration credential, and asks
Alembic for current state. It must not restage provider application MetaTables.

After Alembic `upgrade` or `downgrade`, the SDK calls TS Manager's
`finalize-managed` endpoint once for the reserved provider MetaTable UIDs. The
backend introspects the physical tables Alembic created and flips the catalog
rows from `reserved` to `active`. If the provider defines
`after_register_metatables`, the hook runs only after finalization reports every
provider MetaTable active.

The SDK layer does not parse revision files, generate SDK operation lists,
store backend migration artifacts, or ask the backend to apply rendered SQL.
Alembic revision files remain the schema history and Alembic executes DDL
through the provider migration credential.

The CLI lifecycle intentionally separates three jobs:

- `mainsequence migrations current` asks Alembic what revision is actually
  recorded in the target database through a backend-issued migration credential.
- `mainsequence migrations revision` asks Alembic to write a normal revision
  file for the selected provider. Autogenerate runs against the provider's
  authored SQLAlchemy table names and does not provision or finalize MetaTables.
- `mainsequence migrations upgrade` runs Alembic directly through the migration
  credential, then finalizes provider-scoped MetaTable catalog bindings after
  the database schema has changed.
- `mainsequence migrations downgrade` runs Alembic directly through the migration
  credential, then finalizes the same provider-scoped catalog bindings.

## 1. Define The Migration Provider

Create the migration package with the SDK scaffold command:

```bash
mainsequence migrations scaffold \
  --package sdk_examples \
  --module sdk_examples.migrations \
  --namespace sdk-examples \
  --metadata sdk_examples.meta_tables.account_limits:Base.metadata
```

The command creates the Alembic package, SDK-owned `env.py`, SDK-owned
`script.py.mako`, and a namespace-specific versions directory. Pass
`--force` only when you intentionally want to overwrite changed scaffold files.

The generated provider module is importable as
`sdk_examples.migrations:migration`. The CLI imports that provider to know which
Alembic environment, SQLAlchemy metadata, version-table binding, and MetaTable
models belong to this migration stream.

```text
your-project/
  pyproject.toml
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

The provider module itself is not registered with the backend. The Alembic
version-table binding is registered automatically by commands that need backend
state, such as `mainsequence migrations current` and
`mainsequence migrations upgrade`.

After scaffolding, add the provider-scoped model list. The provider below uses
the same `Base`, `Account`, and `AccountLimit` model classes from Part 2:

```python
from mainsequence.meta_tables.migrations import (
    build_alembic_version_metatable,
    build_metatable_migration_provider,
)
from mainsequence.meta_tables import schema_table_name

from sdk_examples.meta_tables.account_limits import Account, AccountLimit, Base


TutorialAlembicVersion = build_alembic_version_metatable(
    class_name="TutorialAlembicVersion",
    namespace="sdk-examples",
    identifier="sdk_examples.alembic_version",
    schema="public",
    table_name=schema_table_name("sdk_examples", "alembic_version"),
)

migration = build_metatable_migration_provider(
    package="sdk_examples",
    migration_namespace="sdk-examples",
    script_location="sdk_examples.migrations:",
    version_location_prefix="sdk_examples.migrations:versions",
    target_metadata=Base.metadata,
    alembic_registry=TutorialAlembicVersion,
    metatable_models=[
        Account,
        AccountLimit,
    ],
)
```

The scaffolded `registry.py` can also return the model list through
`build_metatable_model_registry(...)` when the project has many model source
functions.

The equivalent hand-written provider shape is:

```python
from mainsequence.meta_tables.migrations import (
    AlembicMetaTableMigration,
    AlembicVersionMetaTable,
)
from mainsequence.meta_tables import schema_table_name

from sdk_examples.meta_tables.account_limits import Account, AccountLimit, Base


class TutorialAlembicVersion(AlembicVersionMetaTable):
    __metatable_namespace__ = "sdk-examples"
    __metatable_identifier__ = "sdk_examples.alembic_version"
    __alembic_version_schema__ = "public"
    __alembic_version_table_name__ = schema_table_name("sdk_examples", "alembic_version")
    __alembic_version_column_name__ = "version_num"


migration = AlembicMetaTableMigration(
    package="sdk_examples",
    migration_namespace="sdk-examples",
    script_location="sdk_examples.migrations:",
    version_locations=["sdk_examples.migrations:versions/sdk_examples"],
    version_path="sdk_examples.migrations:versions/sdk_examples",
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

Application MetaTables in the Alembic workflow are resolved by the SQLAlchemy
table name. `mainsequence migrations` reads each provider model's current
`Table.name`, sends that value as the reservation `identifier`, and uses the
same value for same-batch FK `target_identifier` references before backend UIDs
exist. `__metatable_identifier__` is not the Alembic migration identity.

```text
<sqlalchemy Table.name>
```

If a model is renamed or moved, its migration identity stays stable as long as
the SQLAlchemy table name stays stable.

This tutorial uses the scaffolded package provider, so pass it explicitly:

```bash
mainsequence migrations current --provider sdk_examples.migrations:migration
```

## 2. Create The Alembic Environment

The scaffold command created the Alembic environment under the provider's
`script_location`. For `script_location="sdk_examples.migrations:"`, the files
are:

```text
sdk_examples/
  migrations/
    __init__.py
    env.py
    script.py.mako
    versions/
      __init__.py
```

`env.py` should stay thin and delegate to the SDK-owned Alembic environment:

```python
from __future__ import annotations

from mainsequence.meta_tables.migrations.env import run_mainsequence_alembic_env

from sdk_examples.migrations import migration


run_mainsequence_alembic_env(default_provider=migration)
```

The `versions/` directory is where `mainsequence migrations revision` writes
generated Alembic revision files. `script.py.mako` is required by Alembic and is
copied from the SDK template by the scaffold command.

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
MetaTable because it is read-only. `revision` is local Alembic authoring and
does not register, reserve, or finalize provider application MetaTables.
`upgrade` and `downgrade` reserve or resolve provider application MetaTables
before Alembic applies schema changes.

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
ACCOUNT_TABLE_NAME = schema_table_name("sdk_examples", "account")


class Account(PlatformManagedMetaTable, Base):
    __tablename__ = ACCOUNT_TABLE_NAME
    __metatable_namespace__ = NAMESPACE

    uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    account_code: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    region: Mapped[str] = mapped_column(String(32), nullable=False)
```

The new model adds `status`:

```python
class Account(PlatformManagedMetaTable, Base):
    __tablename__ = ACCOUNT_TABLE_NAME
    __metatable_namespace__ = NAMESPACE

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
  --provider sdk_examples.migrations:migration
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
registry is not touched by `revision`; it is part of backend-scoped commands
such as `current`, `upgrade`, and `downgrade`, where scoped-credential requests
include that registry UID so Alembic can read and write its physical version
table.

Autogenerate is enabled by default. If `--sqlalchemy-url` is supplied, Alembic
reflects that database. Otherwise the CLI requests a provider migration
connection without MetaTable UIDs, so revision generation does not register,
reserve, or finalize provider MetaTables. For an initial migration the baseline
should be an empty database; for later migrations it must represent the
previous Alembic revision.

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

For platform-managed tables, the SDK reservation step resolves the MetaTable
catalog rows by authored SQLAlchemy table name and binds MetaTable UID/storage
metadata before Alembic renders or executes SQL.

## 6. Check Current Revision

Read current Alembic state through the selected provider:

```bash
mainsequence migrations current \
  --provider sdk_examples.migrations:migration
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
  --provider sdk_examples.migrations:migration \
  head
```

`upgrade` is the mutation path. It runs the thin SDK setup, obtains a temporary
provider migration credential, and calls Alembic `upgrade` directly.

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
  "migration_package": "sdk_examples",
  "migration_namespace": "sdk-examples",
  "migration_provider_key": "sdk_examples:sdk-examples",
  "alembic_version_meta_table_uid": "alembic-version-metatable-uid",
  "tables": [
    {
      "identifier": "sdk_examples.Asset",
      "namespace": "sdk_examples",
      "data_source_uid": "dynamic-table-data-source-uid",
      "table_contract": {
        "version": "relational-table.v1",
        "physical": {"table_name": "sdk_examples_asset"},
        "columns": []
      }
    }
  ]
}
```

The endpoint implies Alembic ownership and deletion protection. Provider
identity is request-level metadata, not a per-table schema-management payload.
The backend returns reserved MetaTable UIDs, storage metadata, and flat provider
fields. The SDK binds that catalog metadata to the provider models while
preserving the authored SQLAlchemy table names Alembic will render against.
Index names remain normal SQLAlchemy/Alembic metadata. Foreign keys are also
normal SQLAlchemy/Alembic metadata; Alembic, SQLAlchemy, and the database own
the physical FK constraint names and DDL.

Second, backend-scoped Alembic commands request a short-lived migration
credential for the provider data source. The migration-connection endpoint does
not receive MetaTable UIDs:

```json
{
  "purpose": "schema_migration",
  "package": "sdk_examples",
  "migration_namespace": "sdk-examples",
  "ttl_seconds": 900
}
```

The returned URI is secret. The CLI passes it to Alembic and does not print it.
Alembic's normal stdout and offline output buffers are forwarded through the
CLI. `current` still avoids provider application MetaTable reservation because
reading Alembic version state should not restage provider tables.

Third, after Alembic succeeds, it finalizes the reserved provider rows:

```json
{
  "meta_table_uids": ["reserved-metatable-uid"],
  "migration_package": "sdk_examples",
  "migration_namespace": "sdk-examples",
  "migration_provider_key": "sdk_examples:sdk-examples",
  "alembic_version_meta_table_uid": "alembic-version-metatable-uid",
  "alembic_revision": "head"
}
```

This request intentionally does not include `data_source_uid`, `storage_hash`,
`table_contract`, labels, or provisioning options. It is a reconciliation step:
TS Manager verifies that Alembic created the physical tables and then marks the
reserved catalog rows active.

Existing reserved MetaTables found during prepare are bound and reused; the SDK
does not create them again. If a reserved row exists but cannot be reused safely,
the command must fail with a clear error.

## 9. Catalog Registration Scope

`upgrade` and `downgrade` reserve provider models before Alembic runs and
finalize only those models after Alembic execution:

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

The hook receives the same ordered model scope through
`context.metatable_models` and the matching MetaTable objects through
`context.registered_metatables`. Use those context fields for catalog refresh;
do not import a broader registry inside the hook.

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
