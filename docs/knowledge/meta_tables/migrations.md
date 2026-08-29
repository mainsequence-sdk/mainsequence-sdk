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
-> CodeRepository tooling refreshes provider.metatable_models
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
- reserve or resolve the provider's platform-managed, Alembic-managed
  `AlembicVersionMetaTable` root
- reserve or resolve the provider-scoped platform-managed MetaTables without
  creating physical application tables for `upgrade` and `downgrade`
- preserve authored SQLAlchemy table names and bind backend MetaTable
  UID/storage metadata into the provider models
- keep the Alembic version MetaTable UID and provider MetaTable UIDs in prepared
  state for finalization and reporting
- request a temporary provider migration credential through the provider's
  Alembic registry `MetaTable`; the server derives its owning `DataSource`
- build an Alembic `Config` with `script_location`, `sqlalchemy.url`,
  `version_table`, `version_table_schema`, provider metadata, owner role, and
  CLI output streams
- call Alembic `current`, `revision`, `upgrade`, or `downgrade` directly

After Alembic `upgrade` or `downgrade`, the SDK finalizes the registry and
provider-scoped MetaTable catalog rows, then runs `after_register_metatables`
when the provider defines that hook.

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

The canonical tutorial CodeRepository contains the runnable beginner migration flow.
This page remains the SDK reference for provider behavior and migration
contracts.

## Provider Object

Each CodeRepository defines one selected provider object in an importable Python
module. The SDK-owned scaffold command creates the normal provider package:

```bash
mainsequence migrations scaffold \
  --package sdk_examples \
  --module sdk_examples.migrations \
  --namespace sdk-examples \
  --metadata sdk_examples.meta_tables.account_limits:Base.metadata
```

```text
your-repository/
  pyproject.toml
  sdk_examples/
    migrations/
      __init__.py
      env.py
      script.py.mako
      versions/
        __init__.py
```

The scaffolded `env.py` delegates to the SDK-owned Alembic environment, and the
scaffolded `script.py.mako` is the SDK-owned Alembic revision template.

The provider module is not registered with the backend. The CLI imports it to
build Alembic configuration and backend request metadata. The backend reserves
the provider's `AlembicVersionMetaTable` root, then reserves application
MetaTables as its children for schema-mutating commands.

```python
from mainsequence.meta_tables.migrations import (
    AlembicMetaTableCatalogRefreshContext,
    build_alembic_version_metatable,
    build_metatable_migration_provider,
)
from mainsequence.meta_tables import schema_table_name

from sdk_examples.meta_tables.account_limits import Account, AccountLimit, Base


def refresh_code_repository_catalog_from_registered_metatables(
    context: AlembicMetaTableCatalogRefreshContext,
):
    models = context.metatable_models
    registered_metatables = context.registered_metatables
    reserved_policy = context.reserved_policy
    ...


CodeRepositoryAlembicVersion = build_alembic_version_metatable(
    class_name="CodeRepositoryAlembicVersion",
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
    alembic_registry=CodeRepositoryAlembicVersion,
    metatable_models=[Account, AccountLimit],
    after_register_metatables=refresh_code_repository_catalog_from_registered_metatables,
)
```

Provider discovery order:

1. `mainsequence_migrations.py:migration` in the current code repository root.
2. `<current_package>.migrations:migration` when the CLI can infer the package.
3. `--provider module.path:migration` as an explicit override.

For package-local providers, put the same `migration` object in a package module
such as `sdk_examples/migrations/__init__.py` and pass
`--provider sdk_examples.migrations:migration`.

The provider is the scope boundary. The SDK must not scan every imported model
or every installed package to decide what to migrate.

`after_register_metatables` is optional. Use it only when the CodeRepository has a
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
`POST /api/v1/meta-tables/`; `PlatformTimeIndexMetaTable` rows go to
`POST /api/v1/time-index-meta-tables/`. The SDK sends raw JSON lists with
`provisioning_status="reserved"` and `is_alembic_managed=true`; it does not
wrap the rows in a reservation request object. Alembic is the authority for
schema changes, and TS Manager is the authoritative owner of MetaTable catalog
rows and migration-scoped credentials.

Raw describes the list-shaped HTTP body, not unchecked SDK input. Before HTTP,
each relational row is validated as `ManagedMetaTableCollectionCreateRow` and
each time-indexed row as `ManagedTimeIndexMetaTableCollectionCreateRow`. These
models fix catalog ownership to `platform_managed`, schema ownership to
Alembic through `is_alembic_managed=true`, and provisioning to `reserved`.

## Alembic Version MetaTable

`AlembicVersionMetaTable` is the platform-managed root MetaTable for the
provider migration stream. The SDK reserves its catalog row with
`schema_management_mode="alembic_managed"`; Alembic and PostgreSQL remain
responsible for the physical version table. Migration commands force this
reservation idempotently. Before creation, the SDK resolves the registry by
its physical identity: DataSource UID, physical schema, and physical table
name. A compatible provider root is reused; a missing row is created; an
ambiguous or incompatible row fails clearly. The SDK class cache is not
considered authoritative, and a create conflict triggers one validated lookup
to handle concurrent CLI processes.

The three lifecycle axes are independent:

| Field | Registry value before migration | Meaning |
| --- | --- | --- |
| `management_mode` | `platform_managed` | The SDK/platform owns the catalog lifecycle. |
| `schema_management_mode` | `alembic_managed` | Alembic owns physical DDL. |
| `provisioning_status` | `reserved` | The catalog row is not usable until its physical table exists. |

After successful managed finalization, only `provisioning_status` changes to
`active`. The registry has no parent `alembic_version_meta_table_uid`; provider
application tables reference the registry UID as their parent.

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

The CLI reserves this binding automatically when a command needs backend
migration state, such as `current` or `upgrade`. Initial reservation resolves
the data source through the same resolver used by normal MetaTable
registration; it does not accept a data-source override.

## CLI Workflow

The provider-based CLI workflow is:

```bash
mainsequence migrations current --provider sdk_examples.migrations:migration
mainsequence migrations revision --provider sdk_examples.migrations:migration
mainsequence migrations upgrade --provider sdk_examples.migrations:migration head
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

- `POST /api/v1/meta-tables/` creates or resolves regular
  platform-managed MetaTable catalog rows without creating physical application
  tables.
- `POST /api/v1/time-index-meta-tables/` creates or resolves time-indexed
  platform-managed MetaTable catalog rows without creating physical application
  tables.
- `POST /api/v1/meta-tables/<meta_table_uid>/migration-connection/`
  issues a temporary provider migration credential through an authoritative
  MetaTable. The server derives the canonical DataSource from that table.
- `POST /api/v1/meta-tables/finalize-managed/` activates reserved
  MetaTables after Alembic creates or alters the physical tables.

The returned database URI is a secret and should not be printed, logged, or
stored in repository files.

## MetaTable Catalog Binding

MetaTables remain catalog metadata. They do not own migration execution records,
affected-table validation, or contract reconciliation during Alembic apply.

Before Alembic renders SQL, CodeRepository tooling resolves/reserves only the
application MetaTable classes listed in `migration.metatable_models` and binds
their backend `MetaTable.uid`/storage metadata while preserving authored table
names. After Alembic changes physical tables, CodeRepository tooling finalizes the same
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
