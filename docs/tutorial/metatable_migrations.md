# Part 2.1: MetaTable Migrations

This document is only about schema migrations for MetaTables.

It continues from [Part 2: Working With MetaTables](working_with_meta_tables.md),
where the project created and registered these backend-managed MetaTables:

- `Account`
- `AccountLimit`

Use this page when an already deployed MetaTable must change shape without
losing its existing platform identity, data, permissions, governed SQL scopes,
foreign-key references, dashboards, or jobs.

## Why Migrations Exist

Initial `PlatformManagedMetaTable` registration is for creating a table from a
known contract. It is not an in-place schema migration engine.

For shape-addressed platform-managed tables, the SDK derives `storage_hash` from
storage-relevant SQLAlchemy table attributes. If you add or remove a column in
the class and then call normal registration again, the newly constructed class
can point at a different logical storage identity before the SDK can recover the
previous table. That is not a migration. It is a new table identity.

Alembic migrations preserve a deployed table by changing the physical schema in
place. The existing MetaTable remains the catalog reference, and project tooling
registers or refreshes catalog metadata after SQL execution.

Use Alembic migrations when:

- an existing physical table already contains production or shared data
- a MetaTable UID is already used by governed SQL scopes, APIs, dashboards, or jobs
- a foreign key points at the existing MetaTable
- you need ordered, reviewable, reproducible DDL across environments
- you need the database to track current schema revision

Do not use migrations when:

- you are creating a brand-new logical table
- a development table can be deleted and recreated
- the change intentionally creates a separate table identity
- the change is only catalog text such as labels or description

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
      env.py
      versions/
```

This file is not registered with the backend. It is a local provider module.
The CLI imports `mainsequence_migrations:migration` to know which Alembic
environment, SQLAlchemy metadata, version-table binding, and MetaTable models
belong to this migration stream.

The backend registration step happens later, when
`mainsequence migrations register-version-table` registers
`TutorialAlembicVersion` as the external MetaTable binding for Alembic's
physical `alembic_version` table.

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

## 2. Wire Alembic To The Provider

Create an Alembic `env.py` under the provider's `script_location`, for example
`sdk_examples/migrations/env.py`:

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

## 3. Register The Version Table Binding

Register the provider's Alembic version table as an external MetaTable:

```bash
mainsequence migrations register-version-table \
  --provider mainsequence_migrations:migration
```

If the provider's `AlembicVersionMetaTable` binding is not already bound to a
data source, pass an explicit override once:

```bash
mainsequence migrations register-version-table \
  --provider mainsequence_migrations:migration \
  --data-source-uid "$MAINSEQUENCE_DATA_SOURCE_UID"
```

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
  --provider mainsequence_migrations:migration \
  --autogenerate \
  -m "add account status"
```

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
        data_source_uid=migration.resolve_data_source_uid(),
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

## 8. Dry-Run And Apply

Validate the rendered artifact first:

```bash
mainsequence migrations upgrade \
  --provider mainsequence_migrations:migration \
  --to head \
  --dry-run
```

Apply the same rendered artifact after validation and sync provider-scoped
MetaTables:

```bash
mainsequence migrations upgrade \
  --provider mainsequence_migrations:migration \
  --to head
```

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
  "data_source_uid": "bound-data-source-uid",
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

After SQL execution, project tooling registers or refreshes only the models
listed by the selected provider:

```python
registered = migration.sync_metatable_catalog()
```

This catalog step resolves by exact `identifier`, binds changed model classes
to existing MetaTable rows when present, initial-registers missing identifiers,
and fails if an identifier is duplicated. The provider controls scope;
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
