# ADR 0020: Alembic-Based MetaTable Migrations

Date: 2026-06-01

Status: Superseded

Superseded in part by
[ADR 0021: Migration-First Platform-Managed MetaTables](0021-platform-managed-metatables-migration-first.md)
for platform-managed lifecycle details, by
[ADR 0022: Thin SDK Alembic-Owned MetaTable Migrations](0022-thin-sdk-alembic-owned-metatable-migrations.md)
for the direct-Alembic SDK flow, and by backend ADR 013 for
Alembic-managed finalization and reset semantics. FK/index migration-resolution
policy is superseded by
[ADR 0023: Alembic-Owned Foreign Keys And Indexes](0023-alembic-owned-foreign-keys-and-indexes.md).

## Context

SQLAlchemy MetaTable declarations are the SDK authoring source for table
contracts. They are not a migration engine.

The SDK custom migration operation format duplicated Alembic with a weaker
language: fake schema operation plans and SDK-managed artifact rows. That path
is not maintainable and must be removed.

The intended lifecycle is:

```text
SQLAlchemy models
-> Alembic revision
-> SDK reserves provider MetaTables and obtains a scoped database URI
-> Alembic executes current/revision/upgrade directly
-> SDK calls backend finalize-managed once for the provider MetaTable UIDs
-> project finalizes derived catalog state after backend finalization succeeds
```

Alembic owns schema diffs, revision files, downgrade/upgrade semantics, and the
physical `alembic_version` table. The SDK owns a single migration provider
object, MetaTable registration/catalog bindings, and typed backend requests.

## Decision

Use Alembic as the only schema migration engine.

The SDK will not provide a migration artifact table. There is no SDK
migration-row model and no SDK migration-row upsert path. The only MetaTable
model the SDK provides for Alembic migration state is `AlembicVersionMetaTable`.

Each client project defines one `AlembicMetaTableMigration` provider for now.
That provider is the boundary between the host project, Alembic, SDK catalog
registration, and backend execution. It supplies:

- the package and migration namespace used for request identity
- the Alembic script location
- the SQLAlchemy `target_metadata` Alembic uses for optional autogenerate
- the `AlembicVersionMetaTable` binding for Alembic's physical version table
- the MetaTable model classes that project tooling may register or refresh
  after SQL execution
- the optional post-registration catalog hook

`AlembicVersionMetaTable` is a catalog binding for Alembic's version table. It
registers as a normal `external_registered` MetaTable with the minimal Alembic
revision column the backend needs to read current state:

```json
{
  "version": "relational-table.v1",
  "physical": {"table_name": "msm_alembic_version"},
  "columns": [
    {
      "name": "version_num",
      "data_type": "string",
      "backend_type": "VARCHAR",
      "nullable": false,
      "primary_key": true
    }
  ],
  "constraints": [],
  "indexes": [],
  "foreign_keys": [],
  "authoring": {
    "owner": "alembic",
    "schema": "public",
    "version_table": "msm_alembic_version"
  }
}
```

That contract means:

- Alembic owns the actual version-table DDL.
- The SDK registers a stable pointer for catalog/discovery and current-revision
  reads.
- The `version_num` declaration is not an SDK-owned SQLAlchemy model. It is the
  minimal metadata required by the existing MetaTable registration contract.
- The backend does not use this contract to validate Alembic inserts or DDL.
  PostgreSQL and Alembic remain the authority for the physical version table.
- The binding is external-registered; it must not create or mutate the physical
  Alembic version table.

There is no `[tool.mainsequence.migrations].enabled` provider list yet. The
first implementation uses one conventional provider per host project, with an
explicit `--provider` CLI override when convention cannot find it. Multiple
provider or dependency-package migration selection is a future extension, not
part of this ADR's first implementation.

## Alembic MetaTable Migration Provider

Client packages define one provider object:

```python
from mainsequence.meta_tables.migrations import (
    AlembicMetaTableMigration,
    AlembicVersionMetaTable,
)
from msm.catalog import refresh_markets_catalog_from_registered_metatables
from msm.config import markets_namespace
from msm.models import Base
from msm.models.accounts import AccountTable
from msm.models.assets import AssetTable, AssetTypeTable
from msm.migrations.registry import migration_model_registry


class MarketsAlembicVersion(AlembicVersionMetaTable):
    __metatable_namespace__ = "msm"
    __metatable_identifier__ = "msm.alembic_version"
    __alembic_version_schema__ = "public"
    __alembic_version_table_name__ = "msm_alembic_version"
    __alembic_version_column_name__ = "version_num"


migration = AlembicMetaTableMigration(
    package="msm",
    migration_namespace=markets_namespace(),
    script_location="msm:migrations",
    target_metadata=Base.metadata,
    alembic_registry=MarketsAlembicVersion,
    metatable_models=migration_model_registry(),
    after_register_metatables=refresh_markets_catalog_from_registered_metatables,
)
```

The provider is the only source the CLI needs for migration behavior. The SDK
must not infer migrations from every imported model or every installed
dependency. Imported libraries may expose their own provider objects, but the
host project must choose the provider being operated on.

`after_register_metatables` is an optional project hook for catalog side effects
that must happen after provider-scoped MetaTable registration. The SDK calls it
only after every model in `migration.metatable_models` has registered or
refreshed successfully. The hook receives an
`AlembicMetaTableCatalogRefreshContext` containing the ordered registered
MetaTable objects, the matching ordered provider model classes, provider
identity, and the reserved-table policy for any post-Alembic catalog writes:

```python
def refresh_markets_catalog_from_registered_metatables(context):
    models = context.metatable_models
    registered_metatables = context.registered_metatables
    reserved_policy = context.reserved_policy
    ...
```

Hooks must use the provider-scoped `context.metatable_models`; importing a
separate global registry can compare the current provider against the wrong
model scope.

The hook is client/project tooling. It is not part of the backend apply request,
is not a second migration language, and must not run during `current`,
`revision`, failed `upgrade`, or failed `downgrade`. It should be idempotent
because SQL execution and MetaTable registration may already have succeeded
before a hook failure is retried.

The `alembic_registry` attribute is the source for Alembic's version table
configuration. The SDK must derive Alembic config from it:

```python
context.configure(
    target_metadata=migration.target_metadata,
    version_table=migration.alembic_registry.__alembic_version_table_name__,
    version_table_schema=migration.alembic_registry.__alembic_version_schema__,
    compare_type=True,
    compare_server_default=True,
)
```

For autogenerate, the provider must also support an inclusion filter so Alembic
does not generate migrations for unrelated imported SQLAlchemy tables. The first
implementation may default to tables present in `target_metadata`, but the
provider shape must leave room for an explicit `include_object` or
`include_name` hook.

Recommended provider discovery order:

1. `mainsequence_migrations.py:migration` in the current project root.
2. `<current_package>.migrations:migration` when the CLI can infer the package.
3. `--provider module.path:migration` as an explicit override.

The CLI must not require `data_source_uid` in project config. Initial
`AlembicVersionMetaTable` registration resolves its data source through the
same registration resolver used by other MetaTables. After registration, the
bound `AlembicVersionMetaTable` supplies the target data source for Alembic
status checks and backend SQL execution. Migration provider methods and CLI
commands must not accept a `data_source_uid` override.

That target data source is for Alembic status checks and backend SQL execution.
It must not be copied into application MetaTable registrations during catalog
sync. Each application MetaTable registers through its own normal model
binding/configuration.

## Stable MetaTable Identity For Alembic Catalog Sync

Application MetaTable catalog sync in the Alembic workflow uses the SQLAlchemy
table name as the stable lookup key. During `prepare_for_alembic()`, the SDK
reads each provider model's current `Table.name`, sends that value as the
reservation `identifier`, and emits the same value as same-batch FK
`target_identifier` before backend UIDs exist.

`__metatable_identifier__` is not the Alembic migration identity. A model rename
or module move does not affect migration identity as long as the SQLAlchemy
table name is stable.

Catalog sync after a migration must:

1. Resolve each model's SQLAlchemy table name.
2. Send every provider model to `reserve-managed/` with that table name as the
   reservation `identifier`.
3. Let TS Manager resolve existing catalog rows or create missing reserved
   rows in one batch.
4. Bind the model class to the MetaTable UID, storage hash, and physical table
   name returned by the reservation response.
5. Fail if TS Manager reports that the table-name identity is not unique.

This is the mechanism that lets Alembic change a shape-addressed
`PlatformManagedMetaTable` without losing the original catalog path. The new
SQLAlchemy class shape must not decide which deployed MetaTable is refreshed,
and the SDK must not skip reservation because a previous catalog contract
appears to match.

## Alembic Version MetaTable Registration Contract

The SDK registration request for Alembic's version table is a normal
`external_registered` MetaTable:

```json
{
  "data_source_uid": "uuid",
  "management_mode": "external_registered",
  "storage_hash": "identifier-derived stable hash",
  "identifier": "msm.alembic_version",
  "namespace": "msm",
  "description": "Alembic revision state table.",
  "labels": [],
  "introspect": false,
  "table_contract": {
    "version": "relational-table.v1",
    "physical": {
      "table_name": "msm_alembic_version"
    },
    "columns": [
      {
        "name": "version_num",
        "data_type": "string",
        "backend_type": "VARCHAR",
        "nullable": false,
        "primary_key": true
      }
    ],
    "constraints": [],
    "indexes": [],
    "foreign_keys": [],
    "authoring": {
      "owner": "alembic",
      "schema": "public",
      "version_table": "msm_alembic_version"
    }
  }
}
```

The request uses `introspect=false`, so initial migration setup does not require
the physical `alembic_version` table to exist before Alembic SQL runs. The
backend still enforces normal MetaTable permissions, data-source ownership,
labels, namespace, identifier, physical table binding rules, and non-empty
contract columns.

## Client Workflow

The client workflow has two separate tracks:

1. Register a MetaTable catalog pointer to Alembic's version table.
2. Render and apply Alembic SQL artifacts through the backend migration apply
   endpoint.

The client does not run a migration "on" a MetaTable. The registered
`AlembicVersionMetaTable` only tells the backend where Alembic revision state
lives for a data source. The migration SQL runs on the target data source and
Alembic's own SQL updates the physical `alembic_version` table.

### 1. Register The Alembic Version MetaTable

The selected `AlembicMetaTableMigration` provider must first register its
`alembic_registry` binding. This registration uses the same data-source
resolver as other MetaTable registrations: an explicit class binding when one
exists, otherwise the active Main Sequence project/session data source.

```python
from mainsequence.meta_tables.migrations import AlembicVersionMetaTable


class MarketsAlembicVersion(AlembicVersionMetaTable):
    __metatable_namespace__ = "msm"
    __metatable_identifier__ = "msm.alembic_version"
    __alembic_version_schema__ = "public"
    __alembic_version_table_name__ = "msm_alembic_version"
    __alembic_version_column_name__ = "version_num"


alembic_version_meta_table = migration.ensure_alembic_registry()
```

The registration request generated by the SDK still contains the resolved data
source UID:

```python
request = migration.alembic_registry.build_registration_request(
    data_source_uid=resolved_registration_data_source_uid,
)
```

```json
{
  "data_source_uid": "uuid",
  "management_mode": "external_registered",
  "storage_hash": "computed-by-sdk",
  "identifier": "msm.alembic_version",
  "namespace": "msm",
  "description": "Alembic revision state table.",
  "protect_from_deletion": false,
  "labels": [],
  "introspect": false,
  "table_contract": {
    "version": "relational-table.v1",
    "physical": {
      "table_name": "msm_alembic_version"
    },
    "columns": [
      {
        "name": "version_num",
        "data_type": "string",
        "backend_type": "VARCHAR",
        "nullable": false,
        "primary_key": true
      }
    ],
    "constraints": [],
    "indexes": [],
    "foreign_keys": [],
    "authoring": {
      "owner": "alembic",
      "schema": "public",
      "version_table": "msm_alembic_version"
    }
  }
}
```

The `schema`, `version_table`, and `version_num` column must match the
project's Alembic configuration. For an initial migration, the physical
`alembic_version` table may not exist yet. This registration is still valid
because it is a catalog pointer, not an instruction for TS Manager to create or
validate Alembic's table schema.

### 2. Generate An Alembic Revision

The client project uses Alembic revision generation through the SDK CLI or a
project-local Alembic command wired to the same provider:

```bash
mainsequence migrations revision
```

`-m/--message` is optional; when omitted, the CLI passes `migration` as the
Alembic revision message.

Autogenerate is enabled by default. If `--sqlalchemy-url` is supplied, Alembic
reflects that database. Otherwise the CLI requests a provider migration
connection without MetaTable UIDs.

The Alembic environment must point `target_metadata` at
`migration.target_metadata`, and it must derive `version_table` and
`version_table_schema` from `migration.alembic_registry`. Alembic owns
`op.create_table(...)`, `op.add_column(...)`, downgrade bodies, branch labels,
dependencies, and the physical `alembic_version` table.

Minimal Alembic `env.py` direction:

```python
from alembic import context
from msm.migrations import migration


def include_name(name, type_, parent_names):
    return migration.include_name(name, type_, parent_names)


context.configure(
    target_metadata=migration.target_metadata,
    version_table=migration.alembic_registry.__alembic_version_table_name__,
    version_table_schema=migration.alembic_registry.__alembic_version_schema__,
    include_name=include_name,
    compare_type=True,
    compare_server_default=True,
)
```

The inclusion hook is the extension point that prevents unrelated imported
SQLAlchemy tables from entering this migration provider.

SDK migration files must not define custom `operations()` functions and must not
emit SDK operation lists.

### 3. Current Revision Discovery

ADR 0022 removed the backend migration-status helper. Current SDK tooling reads
the current Alembic revision by configuring Alembic with the provider's
registered version-table binding and calling Alembic `current` through the
migration connection.

The version table remains discoverable because the provider declares an
`AlembicVersionMetaTable`. The backend catalog stores that MetaTable pointer;
Alembic, not a TS Manager status endpoint, reads the row content.

### 4. Superseded: Direct Alembic Execution

ADR 0022 removes the normal-user SQL artifact path. The current SDK CLI calls
Alembic directly through a provider migration connection instead of exposing a
`render` command or backend SQL apply command.

The older design rendered SQL from Alembic without applying it locally. It used
SQL output similar to:

```bash
alembic upgrade <current_revision_or_base>:<target_revision> --sql
```

For the first migration, `<current_revision_or_base>` was `base`. For a normal
upgrade, the older design used a backend status helper that no longer exists.

The SDK packages the rendered SQL with a small manifest that describes the
Alembic revision and the provider's registry table. The backend echoes this
metadata for observability; it does not validate or approve the artifact.

```python
manifest = {
    "package": migration.package,
    "migration_namespace": migration.migration_namespace,
    "revision": "0001_initial",
    "down_revision": None,
    "direction": "upgrade",
    "alembic_version_table": "public.msm_alembic_version",
}
```

`alembic_version_table` is computed from
`migration.alembic_registry.__alembic_version_schema__` and
`migration.alembic_registry.__alembic_version_table_name__`.

`statement_boundaries` is optional diagnostic metadata. It can identify rendered
statement positions for error reporting, but it is not executable migration
logic.

### 5. Superseded: Backend SQL Artifact Apply

ADR 0022 removes the normal-user dry-run/apply artifact path. Current SDK
migration commands do not construct `AlembicMigrationOperation`, do not call
`MetaTable.apply_migration(...)`, and do not send rendered SQL to a backend
apply endpoint. The SDK requests a scoped database connection and calls Alembic
directly.

### 7. Confirm Revision State

After Alembic runs directly, the SDK confirms state through Alembic itself using
the same scoped connection and configured provider version table. There is no
TS Manager migration-status request body in the active contract.

### 8. Register Or Refresh Application MetaTables By Identifier

After Alembic applies the physical schema change, project tooling registers or
refreshes the application MetaTable catalog bindings listed on the selected
provider. This is part of the migration success path, not an optional separate
operation:

```python
registered_metatables = migration.sync_metatable_catalog()
```

That catalog step resolves each model by stable `identifier` first. If the
MetaTable already exists, the SDK binds the model to that existing MetaTable and
refreshes registration metadata for that same catalog path. If it does not
exist, the SDK performs the initial registration for that identifier.

The provider controls the catalog scope; imported-but-unlisted MetaTable
classes are not registered by migration tooling.

If `migration.after_register_metatables` is configured, the SDK calls it after
the provider-scoped catalog registration succeeds:

```python
migration.after_register_metatables(
    AlembicMetaTableCatalogRefreshContext(
        package=migration.package,
        migration_namespace=migration.migration_namespace,
        registered_metatables=registered_metatables,
        reserved_policy="reconcile",
    )
)
```

This hook lets a project update project-specific catalog tables, such as a
markets catalog derived from the registered MetaTable UIDs, without adding
Markets-specific behavior to the SDK or backend.
The `reserved_policy="reconcile"` value is emitted by
`refresh_metatable_catalog()` after Alembic upgrade. Normal runtime repository
operations omit it and keep TS Manager's default reserved-table rejection.

## CLI Workflow

The SDK CLI should expose this workflow through one migration provider:

```bash
mainsequence migrations current --provider module.path:migration
mainsequence migrations revision --provider module.path:migration -m "schema change"
mainsequence migrations upgrade --provider module.path:migration head
mainsequence migrations downgrade --provider module.path:migration <revision>
```

All commands resolve the same provider object by convention or by
`--provider module.path:migration`. Runtime commands request a temporary
provider migration database connection from the target data source. MetaTable
UIDs remain in prepared/finalization state; they are not sent to the
migration-connection endpoint.

`revision` writes a normal Alembic revision file. `current`, `upgrade`, and
`downgrade` call Alembic directly through the migration connection. The
backend does not render or apply SQL artifacts for normal users.

Command responsibilities:

- `current` resolves the provider, registers or resolves the provider's
  `alembic_registry` binding when needed, requests a read-only scoped
  connection, and calls Alembic `current`.
- `revision` creates a normal Alembic revision file under
  `migration.script_location`; project code may keep generated Alembic
  operations or edit them like any normal Alembic project.
- `upgrade` prepares provider-scoped MetaTable reservations, binds backend
  physical table names into SQLAlchemy metadata, requests a scoped
  migration connection, calls Alembic `upgrade`, finalizes the MetaTable catalog
  once, and runs `migration.after_register_metatables` when configured.
- `downgrade` uses the same scoped connection flow and calls Alembic
  `downgrade`.

There is no `--register-metatables` flag in the final workflow. Catalog sync is
part of `upgrade` success. The command exits successfully only when Alembic
execution and provider-scoped catalog finalization succeed.

Existing reserved MetaTables found during prepare are bound and reused; they are
not collection-created again. If reserved state cannot be reused safely, the
client must fail with a clear error.

## Superseded Backend Apply Contract

The earlier `apply-migration/` SQL artifact request and response contract is
removed. The SDK no longer renders Alembic SQL for TS Manager to execute.

The active backend coordination contract is:

```text
reserve-managed/            -> reserve MetaTable rows and physical table names
migration-connection/       -> issue a provider temporary database credential
finalize-managed/           -> activate reserved rows after Alembic runs
```

Alembic reads current revision state and executes upgrade/downgrade directly
through the provider migration credential. TS Manager must not maintain SDK
migration artifact rows, migration-run rows, or a separate SQL apply endpoint.

## SDK Responsibilities

The SDK owns:

- `AlembicMetaTableMigration`
- `AlembicVersionMetaTable`
- provider discovery for one conventional migration provider plus explicit
  `--provider` override
- CLI commands that read current state, generate revisions locally, run Alembic
  upgrade/downgrade directly, reset provider state, register the version table
  for backend-scoped commands, reserve provider MetaTables for apply-time
  commands, and finalize provider MetaTables
- typed reservation and finalization request/response models
- helpers that bind MetaTable UID/storage metadata while preserving authored
  SQLAlchemy table names before Alembic runs
- docs and examples that show Alembic, not SDK operation lists, including the
  dedicated `docs/tutorial/metatable_migrations.md` walkthrough

The SDK does not own:

- custom revision generation outside Alembic
- schema diffing outside Alembic
- a migration artifact table
- artifact sync/upsert helpers
- custom `operations()` migration modules
- execution of DDL locally

## Extensibility Requirements

The first implementation supports one selected provider. It must still be
designed so multiple packages can later expose migration providers without
making imports dangerous.

Required extension points:

- Provider discovery must be explicit and side-effect free. Importing
  `mainsequence` or `msm` must not run migrations or register MetaTables.
- Provider selection must remain user-controlled. Installed dependency packages
  may expose providers, but the host project decides which provider to operate.
- `AlembicMetaTableMigration` must accept `alembic_registry` instead of separate
  loose `version_table` and `version_schema` strings.
- `AlembicMetaTableMigration` must accept `target_metadata`; Alembic
  autogenerate/rendering must never scan every imported SQLAlchemy model.
- `AlembicMetaTableMigration` must accept `metatable_models`; catalog
  registration must be scoped to that list.
- Application MetaTable catalog sync in the Alembic workflow must resolve by
  exact SQLAlchemy table-name identifier.
- Application MetaTable catalog sync must call each provider-scoped model's
  normal `register()` path without passing the Alembic registry data source.
  Each model owns its own data-source binding.
- `__metatable_identifier__` must not be used as Alembic migration identity.
  Normal non-migration MetaTable registration may still expose it as catalog
  metadata.
- `AlembicMetaTableMigration` must accept an optional
  `after_register_metatables` hook for project-specific catalog refresh after
  provider-scoped MetaTable registration.
- The provider should support `include_name` or `include_object` hooks for
  packages that share a SQLAlchemy `MetaData` with unrelated tables.
- The CLI should support `--provider module.path:migration` from day one, even
  if convention-based discovery covers the main project.
- Future multi-provider config may add a selection list, but it must not be
  required for the single-provider workflow.

## Catalog Binding

MetaTables are catalog metadata. The Alembic apply request points only to the
registered `AlembicVersionMetaTable` binding so the backend can locate
Alembic's version table on the target data source. Catalog registration or
refresh for changed application tables is a separate SDK/project tooling step.

## Removal Inventory

Remove these concepts from SDK public behavior:

- SDK-managed migration artifact table models
- default migration artifact table factories
- migration artifact row Pydantic models
- packaged migration artifact row builders
- artifact table upsert SQL helpers
- sync helpers that insert migrations into a MetaTable
- `operations()` functions in migration files
- custom SDK schema-operation names such as `add_column`, `drop_column`,
  `alter_column`, and `create_index`
- legacy SDK schema-migration SQLAlchemy base classes
- operation-only manifests
- SQL-or-operations fallbacks
- custom schema-operation tests, examples, and docs

Remove or rewrite these docs/examples:

- `examples/meta_tables/migrations/*`
- migration sections in `docs/tutorial/working_with_meta_tables.md`
- stale artifact-table content in `docs/knowledge/meta_tables/migrations.md`
- stale artifact-table content in `docs/knowledge/meta_tables/api.md`

There is no compatibility alias and no fallback path.

## Consequences

Alembic is the single source of truth for DDL.

The SDK surface is smaller: one Alembic version MetaTable binding plus typed
backend migration requests.

Initial migrations are valid because the backend runs SQL before affected table
introspection.

Shape-addressed platform-managed tables remain useful for create-new table
patterns, but they are not sufficient for in-place schema migration without a
stable identifier.

## Implementation Tasks

- [x] Make SQLAlchemy a required SDK dependency.
- [x] Remove SDK schema-operation models.
- [x] Remove SDK-managed migration artifact table models.
- [x] Remove default migration artifact table factory.
- [x] Remove migration artifact registry helpers.
- [x] Remove registry upsert and sync helpers.
- [x] Remove custom schema-operation tests.
- [x] Add `AlembicVersionMetaTable`.
- [x] Make `AlembicVersionMetaTable` Alembic-owned with the minimal
  `version_num` contract required for normal external registration.
- [x] Superseded by ADR 0022: remove the normal-user offline SQL render/backend
  apply artifact path. The CLI calls Alembic directly through a scoped
  migration connection.
- [x] Add `AlembicMetaTableMigration` provider object with `package`,
  `migration_namespace`, `script_location`, `target_metadata`,
  `alembic_registry`, `metatable_models`, and optional Alembic inclusion hooks.
- [x] Add provider discovery by convention plus `--provider module.path:migration`.
- [x] Add SDK CLI `mainsequence migrations` command group.
- [x] Add CLI support for `current`, `revision`, `upgrade`, `downgrade`, and
  `reset`.
- [x] Superseded by ADR 0022: CLI migration status/apply requests were removed
  from the normal user path.
- [x] Make revision/autogenerate use `migration.target_metadata`,
  `migration.script_location`, and the Alembic version table declared by
  `migration.alembic_registry`.
- [x] Superseded by ADR 0022: no `render` or `upgrade --dry-run` command exists
  in the current SDK CLI.
- [x] Make `upgrade` perform provider-scoped backend finalization by default
  after Alembic succeeds, with no `--register-metatables` flag.
- [x] Make Alembic migration catalog sync use the SQLAlchemy table name as the
  reservation `identifier` and same-batch FK `target_identifier`.
- [x] Make migration catalog sync bind models to existing catalog rows before
  refresh, reserve missing table-name identifiers, and fail on duplicate
  table-name identifiers.
- [x] Update `docs/tutorial/metatable_migrations.md` so the user-facing flow is
  one `mainsequence migrations upgrade --provider ... head` path with
  table-name catalog sync.
- [x] Update `docs/knowledge/meta_tables/migrations.md` and
  `docs/knowledge/meta_tables/api.md` to remove the `--register-metatables`
  workflow and document table-name catalog sync.
- [x] Update MetaTable examples to use the final upgrade flow and document that
  model renames keep identity when the SQLAlchemy table name remains stable.
- [x] Update the MetaTable skill so migration guidance follows the final ADR
  workflow and does not mention optional `--register-metatables`.
- [x] Add optional `after_register_metatables` provider hook and run it with an
  `AlembicMetaTableCatalogRefreshContext` after successful provider-scoped
  MetaTable registration.
- [x] Add CLI/provider tests covering initial migration, current revision,
  direct Alembic upgrade, and provider-scoped catalog finalization.
- [x] Update tutorials and examples around the provider-based Alembic lifecycle.

## Non-Goals

This ADR does not make the backend run Alembic.

This ADR does not introduce a backend-owned migration artifact registry.

This ADR does not introduce an SDK migration artifact registry MetaTable.

This ADR does not introduce a general-purpose SQL execution endpoint. SQL is
accepted only through the Alembic migration apply contract, tied to a registered
Alembic version MetaTable, provider identity, revision metadata, and current
revision checks. Alembic revisions may render custom SQL, but that SQL is part
of an authorized migration artifact, not an ad hoc SQL request.
