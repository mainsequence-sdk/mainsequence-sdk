# ADR 0020: Alembic-Based MetaTable Migrations

Date: 2026-06-01

Status: Proposed

Superseded in part by
[ADR 0021: Migration-First Platform-Managed MetaTables](0021-platform-managed-metatables-migration-first.md)
for platform-managed lifecycle details.

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
-> Alembic renders SQL
-> SDK sends the Alembic-rendered SQL artifact to the backend
-> backend executes SQL
-> project finalizes MetaTable catalog state
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
  "constraints": [],
  "indexes": [],
  "foreign_keys": [],
  "authoring": {
    "owner": "alembic",
    "schema": "public",
    "version_table": "alembic_version"
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
    __alembic_version_table_name__ = "alembic_version"
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
refreshed successfully. The hook receives the ordered registered MetaTable
objects returned by `migration.sync_metatable_catalog()`:

```python
def refresh_markets_catalog_from_registered_metatables(registered_metatables):
    ...
```

The hook is client/project tooling. It is not part of the backend apply request,
is not a second migration language, and must not run during `current`, `render`,
`revision`, backend dry-run, or failed apply. It should be idempotent because
SQL execution and MetaTable registration may already have succeeded before a
hook failure is retried.

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

## Stable MetaTable Identity For Catalog Sync

Application MetaTable catalog sync uses `identifier` as the only stable lookup
key. It must not resolve application MetaTables by `data_source_uid`,
namespace, storage hash, physical table name, or the newly constructed
SQLAlchemy table shape.

When a model declares `__metatable_identifier__`, that value is used exactly.
The user is then responsible for global uniqueness:

```python
class AssetTypeTable(PlatformManagedMetaTable, Base):
    __metatable_identifier__ = "msm.asset_type"
```

When a model does not declare `__metatable_identifier__`, the SDK derives a
default identifier from the project and the Python model path:

```text
<pyproject project name>:<model.__module__>.<model.__qualname__>
```

Example:

```text
mainsequencemarkets:src.msm.models.assets.AssetTypeTable
```

The project prefix comes from `[project].name` in the nearest project
`pyproject.toml`, normalized only enough to be deterministic. This prevents
different projects that both define `src.Test` from colliding. If the project
name cannot be resolved and the model does not define
`__metatable_identifier__`, the SDK must fail and require an explicit
identifier.

Catalog sync after a migration must:

1. Resolve each model's stable identifier.
2. Fetch the existing `MetaTable` by exact `identifier`.
3. If found, bind the changed model class to that existing MetaTable UID,
   storage hash, and physical table name before registration refresh.
4. If not found, perform initial catalog registration for that identifier.
5. If more than one MetaTable exists for the identifier, fail because the
   identifier is not globally unique.

This is the mechanism that lets Alembic change a shape-addressed
`PlatformManagedMetaTable` without losing the original catalog path. The new
SQLAlchemy class shape must not decide which deployed MetaTable is refreshed.

## Alembic Version MetaTable Registration Contract

The SDK registration request for Alembic's version table is a normal
`external_registered` MetaTable:

```json
{
  "data_source_uid": "uuid",
  "management_mode": "external_registered",
  "storage_hash": "identifier-derived stable hash",
  "identifier": "alembic_version",
  "namespace": "mainsequence.migrations",
  "description": "Alembic revision state table.",
  "labels": [],
  "introspect": false,
  "table_contract": {
    "version": "relational-table.v1",
    "physical": {
      "table_name": "alembic_version"
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
      "version_table": "alembic_version"
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
    __alembic_version_table_name__ = "alembic_version"
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
  "open_for_everyone": false,
  "labels": [],
  "introspect": false,
  "table_contract": {
    "version": "relational-table.v1",
    "physical": {
      "table_name": "alembic_version"
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
      "version_table": "alembic_version"
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

`--autogenerate` is optional and requires an explicit `--sqlalchemy-url` for a
baseline database. The standard path creates a normal Alembic revision file
that project code fills with explicit Alembic operations.

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

### 3. Read Current Revision

Before rendering a transition, client tooling should read the backend's current
Alembic revision through the registered version-table binding:

```python
from mainsequence.client.metatables import AlembicMigrationStatusRequest, MetaTable


status = MetaTable.get_migration_status(
    AlembicMigrationStatusRequest(
        alembic_version_meta_table_uid=alembic_version_meta_table.uid,
        package=migration.package,
        migration_namespace=migration.migration_namespace,
    )
)

current_revision = status.current_revision
```

The returned `current_revision` is the source revision for the next Alembic
render. For a never-migrated database it is `null`.

### 4. Render Alembic SQL Client-Side

Client tooling renders SQL from Alembic without applying it locally. The exact
command can be CLI-based or programmatic, but the output must be the SQL for the
intended transition:

```bash
alembic upgrade <current_revision_or_base>:<target_revision> --sql
```

For the first migration, `<current_revision_or_base>` is `base`. For a normal
upgrade, it is the revision currently reported by
`MetaTable.get_migration_status(...)`.

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
    "alembic_version_table": "public.alembic_version",
}
```

`alembic_version_table` is computed from
`migration.alembic_registry.__alembic_version_schema__` and
`migration.alembic_registry.__alembic_version_table_name__`.

`statement_boundaries` is optional diagnostic metadata. It can identify rendered
statement positions for error reporting, but it is not executable migration
logic.

### 5. Dry-Run The Alembic SQL Artifact

The client sends the rendered artifact to the backend with `dry_run=True` first:

```python
from mainsequence.client.metatables import AlembicMigrationOperation, MetaTable


operation = AlembicMigrationOperation(
    alembic_version_meta_table_uid=alembic_version_meta_table.uid,
    package=migration.package,
    migration_namespace=migration.migration_namespace,
    revision="0001_initial",
    down_revision=None,
    direction="upgrade",
    expected_current_revision=current_revision,
    manifest=manifest,
    sql=rendered_sql,
    statement_boundaries=[],
    dry_run=True,
)

validation = MetaTable.apply_migration(operation)
assert validation.ok
assert validation.status == "validated"
```

The exact JSON sent by the SDK is:

```json
{
  "version": "metatable-migration.v1",
  "alembic_version_meta_table_uid": "uuid",
  "package": "msm",
  "migration_namespace": "mainsequence.examples",
  "revision": "0001_initial",
  "down_revision": null,
  "direction": "upgrade",
  "expected_current_revision": null,
  "manifest": {
    "package": "msm",
    "migration_namespace": "mainsequence.examples",
    "revision": "0001_initial",
    "down_revision": null,
    "direction": "upgrade",
    "alembic_version_table": "public.alembic_version"
  },
  "sql": "Alembic-rendered SQL text",
  "statement_boundaries": [],
  "dry_run": true
}
```

The request must contain only the fields shown above. It must not carry
extra SDK artifact references, affected-table plans, execution-run identifiers, or
other catalog-reconciliation metadata.

### 6. Apply The Same Artifact

After dry-run validation, the client sends the same artifact with
`dry_run=False`:

```python
apply_operation = operation.model_copy(update={"dry_run": False})
result = MetaTable.apply_migration(apply_operation)

assert result.ok
assert result.status == "applied"
assert result.current_revision == "0001_initial"
```

The backend resolves the target data source from
`alembic_version_meta_table_uid` and executes the SQL there. The SQL itself is
responsible for creating or updating the physical Alembic version table
according to Alembic's offline output. The backend must not require application
MetaTables to exist before the SQL runs, which is what makes initial
`op.create_table(...)` migrations valid.

### 7. Confirm Revision State

After apply, the client can read the same version-table binding:

```python
status = MetaTable.get_migration_status(
    {
        "alembic_version_meta_table_uid": alembic_version_meta_table.uid,
        "package": "msm",
        "migration_namespace": "mainsequence.examples",
    }
)

assert status.current_revision == "0001_initial"
```

The exact status request JSON is:

```json
{
  "alembic_version_meta_table_uid": "uuid",
  "package": "msm",
  "migration_namespace": "markets namespace"
}
```

Status is read-only. It does not inspect, create, or refresh application
MetaTables.

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
migration.after_register_metatables(registered_metatables)
```

This hook lets a project update project-specific catalog tables, such as a
markets catalog derived from the registered MetaTable UIDs, without adding
Markets-specific behavior to the SDK or backend.

## CLI Workflow

The SDK CLI should expose this workflow through one migration provider:

```bash
mainsequence migrations current
mainsequence migrations revision
mainsequence migrations render --to head
mainsequence migrations upgrade --to head --dry-run
mainsequence migrations upgrade --to head
```

All commands resolve the same provider object by convention or by
`--provider module.path:migration`. Status and apply requests send the
registered Alembic version MetaTable UID; the backend resolves the target data
source from that registry MetaTable.

`revision` does not call the backend and does not use the
`AlembicVersionMetaTable` registry to produce table operations. The registry is
integrated in `current` and `upgrade`, where the SDK registers or resolves the
provider's version-table MetaTable and the backend uses that registry UID to
read revision state and execute SQL against the correct data source.

Command responsibilities:

- `current` resolves the provider, registers or resolves the provider's
  `alembic_registry` binding when needed, and calls backend migration status.
- `revision` creates a normal Alembic revision file under
  `migration.script_location`; project code fills the revision body with
  explicit Alembic operations. `revision --autogenerate` is optional and must
  require an explicit baseline database URL.
- `render` renders Alembic offline SQL for the requested transition without
  sending it to the backend.
- `upgrade --dry-run` renders SQL and sends the exact backend request with
  `dry_run=true`, then exits without SQL execution or catalog sync.
- `upgrade` first dry-runs, then sends the same rendered artifact with
  `dry_run=false`, reads current revision state, resolves and refreshes
  provider-scoped MetaTables by stable identifier through each model's normal
  registration path, and runs `migration.after_register_metatables` when
  configured.

There is no `--register-metatables` flag in the final workflow. Catalog sync is
part of `upgrade` success. The command exits successfully only when both backend
SQL execution and provider-scoped catalog sync succeed.

## Request Contract

The backend migration apply endpoint receives an Alembic-rendered SQL artifact,
not a custom SDK operation plan and not an SDK artifact-table reference.

Canonical endpoint:

```text
POST /ts_manager/meta_table/apply-migration/
```

Canonical payload:

```json
{
  "version": "metatable-migration.v1",
  "alembic_version_meta_table_uid": "uuid",
  "package": "example-package",
  "migration_namespace": "default",
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

Field rules:

- `version` is literal `metatable-migration.v1`.
- `alembic_version_meta_table_uid` identifies the registered
  `AlembicVersionMetaTable` binding. The backend resolves the target
  `DynamicTableDataSource` from this MetaTable.
- `package` and `migration_namespace` identify the selected Alembic migration
  provider and revision lineage.
- `revision`, `down_revision`, and `direction` come from the Alembic revision.
- `expected_current_revision` is checked against the Alembic version table
  immediately before SQL execution.
- `manifest` is package metadata for the rendered artifact.
- `sql` is generated by Alembic offline rendering from installed package
  revisions.
- `statement_boundaries` is diagnostic metadata; it is not a second migration
  language.
- Affected-table lists, execution-run identifiers, and catalog-reconciliation
  metadata are not part of this request.
- `dry_run` validates only and must not execute SQL.

The backend treats the SQL as an authorized Alembic migration request. It does
not parse, validate, sign, checksum, or approve the artifact.

## Response Contract

Canonical success response:

```json
{
  "ok": true,
  "version": "metatable-migration.v1",
  "dry_run": false,
  "data_source_uid": "uuid",
  "alembic_version_meta_table_uid": "uuid",
  "alembic_version_table": "public.alembic_version",
  "package": "example-package",
  "migration_namespace": "default",
  "revision": "0001_initial",
  "direction": "upgrade",
  "status": "applied",
  "previous_revision": null,
  "previous_revisions": [],
  "current_revision": "0001_initial",
  "current_revisions": ["0001_initial"],
  "executed_statement_count": 3,
  "error": null
}
```

For `dry_run: true`, `executed_statement_count` is `0`, `status` is
`validated`, and no DDL is executed.

For failure, the response keeps the same outer shape with `ok: false` and:

```json
{
  "error": {
    "code": "metatable_migration_sql_failed",
    "message": "Human-readable failure reason.",
    "details": {}
  }
}
```

## Backend Responsibilities

The backend must:

1. Verify caller permission to execute DDL on the target data source.
2. Resolve the `AlembicVersionMetaTable` binding and ensure it belongs to the
   request data source.
3. Read current revision from the Alembic version table.
4. Validate `expected_current_revision`.
5. Execute Alembic-rendered SQL in one transaction when supported.

The backend must not:

- execute SDK custom operation plans
- require affected tables to exist before running initial `create_table` SQL
- treat affected identifiers as proof that a physical table already exists
- mutate Alembic's version table through SDK contract validation
- maintain a separate SDK migration artifact table
- maintain migration-run rows for Alembic execution
- create, import, or refresh MetaTable records as part of SQL execution

## SDK Responsibilities

The SDK owns:

- `AlembicMetaTableMigration`
- `AlembicVersionMetaTable`
- provider discovery for one conventional migration provider plus explicit
  `--provider` override
- CLI commands that generate revisions, render SQL, dry-run, apply, read
  current state, register the version table, and optionally register application
  MetaTables from `migration.metatable_models`
- typed apply/status request and response models
- helpers that package Alembic-rendered SQL into backend apply requests
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
- Application MetaTable catalog sync must resolve by exact `identifier` only.
  It must not use `data_source_uid`, namespace, storage hash, physical table
  name, or the newly constructed SQLAlchemy table shape as the catalog identity.
- Application MetaTable catalog sync must call each provider-scoped model's
  normal `register()` path without passing the Alembic registry data source.
  Each model owns its own data-source binding.
- Models without explicit `__metatable_identifier__` must default to
  `<pyproject project name>:<model.__module__>.<model.__qualname__>`. Missing
  project name means the SDK must require an explicit identifier.
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
- [x] Add package Alembic offline SQL rendering helpers.
- [x] Add apply helper that sends Alembic-rendered SQL plus manifest directly to
  the backend.
- [x] Add backend apply support for direct Alembic SQL artifacts.
- [x] Add `AlembicMetaTableMigration` provider object with `package`,
  `migration_namespace`, `script_location`, `target_metadata`,
  `alembic_registry`, `metatable_models`, and optional Alembic inclusion hooks.
- [x] Add provider discovery by convention plus `--provider module.path:migration`.
- [x] Add SDK CLI `mainsequence migrations` command group.
- [x] Add CLI support for `current`, `revision`, `render`, and `upgrade`.
- [x] Make CLI migration status/apply requests send only the provider's
  `AlembicVersionMetaTable` UID, with no migration-level data-source override.
- [x] Make revision/autogenerate use `migration.target_metadata`,
  `migration.script_location`, and the Alembic version table declared by
  `migration.alembic_registry`.
- [x] Make render/dry-run/apply build the exact backend request shape in this
  ADR and reuse the same rendered SQL artifact between dry-run and apply.
- [x] Make `upgrade` perform provider-scoped catalog sync by default after
  backend SQL apply succeeds, with no `--register-metatables` flag.
- [x] Add stable identifier defaults:
  `<pyproject project name>:<model.__module__>.<model.__qualname__>`.
- [x] Make migration catalog sync resolve existing application MetaTables by
  exact `identifier` only, bind models to existing catalog rows before refresh,
  initial-register missing identifiers, and fail on duplicate identifiers.
- [x] Update `docs/tutorial/metatable_migrations.md` so the user-facing flow is
  one `mainsequence migrations upgrade --provider ... --to head` path with
  identifier-based catalog sync.
- [x] Update `docs/knowledge/meta_tables/migrations.md` and
  `docs/knowledge/meta_tables/api.md` to remove the `--register-metatables`
  workflow and document identifier-based catalog sync.
- [x] Update MetaTable examples to use the final upgrade flow and show default
  identifier behavior plus explicit identifier pinning for renamed/moved model
  classes.
- [x] Update the MetaTable skill so migration guidance follows the final ADR
  workflow and does not mention optional `--register-metatables`.
- [x] Add optional `after_register_metatables` provider hook and run it after
  successful provider-scoped MetaTable registration.
- [x] Add CLI/provider tests covering initial migration, current revision, SQL
  render, dry-run/apply payloads, and provider-scoped catalog registration.
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
