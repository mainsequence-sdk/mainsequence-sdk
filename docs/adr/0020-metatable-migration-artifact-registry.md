# ADR 0020: Client-Defined MetaTable Migration Registry

Date: 2026-06-01

Status: Proposed

Backend alignment: this ADR is implemented against the TS Manager backend
decision in `docs/tdag/ts_manager/adr/adr-011-metatable-migration-execution-ledger.md`.
The SDK migration registry is the artifact source. The backend
`MetaTableMigrationRun` ledger is the execution source of truth.

## Context

SQLAlchemy MetaTable declarations are now the SDK authoring source for
backend-managed table contracts. Registration can create new physical tables,
bind SDK models to backend physical names, and expose governed compiled SQL
operations, but it does not yet provide a controlled way to apply schema
changes after a table exists.

Projects such as `ms-markets` need to evolve MetaTable-backed schemas with DDL
operations such as adding columns, creating indexes, creating new related
tables, importing existing physical tables, and refreshing platform MetaTable
metadata. The platform should support those changes without turning the SDK
into a full migration engine.

The SDK already has the machinery needed to declare and register governed
tables. The migration registry should use that machinery. It should not be a
server-side system table or a separate platform artifact store.

The migration unit is not a single MetaTable. One migration revision can create,
rename, split, or update several physical tables and several platform
MetaTable resources. The revision stream is keyed by:

- `data_source_uid`
- `package`
- `migration_namespace`

The backend should remain the authority for DDL execution, locking,
introspection, and MetaTable resource refresh. The SDK and client package should
own migration packaging, registry table declaration, registry row insertion,
and project-specific metadata.

The migration system also needs a stable way to find the table being migrated
after application code changes. The current shape-addressed
`PlatformManagedMetaTable` storage hash is not sufficient for that. If
`storage_hash` is derived from SQLAlchemy columns, indexes, foreign keys, or
constraints, then adding or removing a column changes the hash before the SDK
can find the existing table. A new model shape cannot recover the previous
shape-derived hash unless the old code, old `storage_hash`, or old `MetaTable`
UID was stored somewhere else.

## Decision

Introduce a dedicated migration operation contract:

```text
metatable-migration.v1
```

Add a small `mainsequence.meta_tables.migrations` module that exposes:

- a SQLAlchemy-compatible `MigrationMetaTable` base class
- a default migration registry table declaration
- Pydantic request and response models
- package manifest and SQL loading helpers
- checksum helpers
- contract hash helpers based on SQLAlchemy MetaTable declarations
- helper functions to sync packaged migration rows into the registry MetaTable
- helper functions to dry-run and apply a migration from a registry row

The client package registers the migration registry as a normal platform-managed
MetaTable. It then stores packaged migrations in that MetaTable using ordinary
SDK MetaTable operation machinery.

The backend apply endpoint does not own or pre-register migration artifacts. It
receives a reference to a migration registry MetaTable row, reads the migration
payload from that row, validates it, locks the target migration stream, executes
the migration when requested, refreshes affected MetaTables, and writes the
authoritative backend migration run ledger.

The main migration resolver is the MetaTable `identifier`. `identifier` is
stable within the backend migration resolution scope:

```text
organization_owner + data_source + management_mode + identifier
```

Migratable tables are identifier-addressed, not shape-addressed.

When a model does not define `__metatable_identifier__`, the SDK derives a
stable default from the importable Python class path:

```text
<module>.<qualname>
```

Example:

```text
msm.markets.models.Asset
```

If distribution metadata is available and stable, SDK tooling may display or
record a distribution-qualified form:

```text
<distribution>:<module>.<qualname>
```

The canonical resolver remains the import path unless the user explicitly sets
`__metatable_identifier__`.

Filesystem paths such as `src/msm/markets/models.py:Asset` are not migration
identity. They are not stable after packaging, Docker builds, editable installs,
or repository layout changes.

Schema, data source, and physical table name are also not public migration
identity:

- schema is SQLAlchemy/backend physical metadata
- `data_source_uid` is the execution target and safety scope
- physical table name is backend-owned

They may appear in validation and execution payloads, but they do not replace
the scoped stable `identifier` resolver.

## Identifier-Based Migration Resolution

In-place migrations require the SDK to resolve a MetaTable from a stable
`identifier`, not from SQLAlchemy table shape.

The resolver rule is:

```text
identifier = explicit __metatable_identifier__
           else f"{model.__module__}.{model.__qualname__}"

storage_hash = f(identifier)
contract_hash = f(columns, indexes, foreign keys, constraints, dtypes, nullability, ...)
```

`identifier` is the public stable pointer. `storage_hash` is the backend storage
identity value derived from that pointer for migration-managed MetaTables.
`contract_hash` is the rotating schema contract pointer. Schema migrations
change `contract_hash`; they must not change the stable `identifier` or the
storage identity derived from it.

The current shape-addressed `PlatformManagedMetaTable` behavior is create-only
for schema evolution. It is valid for tables where changing storage-relevant
shape should produce a new logical table. It is not a valid in-place migration
resolver.

The SDK must therefore add an identifier-addressed registration mode for
migration-managed SQLAlchemy MetaTables. That mode uses the resolver rule above
when constructing the registration payload. It must not call the current
table-shape storage identity builder for migration-managed tables.

Migration-managed tables should use that identifier-addressed registration mode
from their first version. A table already created with a shape-derived
`storage_hash` is not automatically recoverable from the new code after its
shape changes.
Adopting such a table requires an explicit anchor, such as the old
`MetaTable.uid`, old `storage_hash`, or an unambiguous backend lookup by the
scoped `identifier`.

Class moves and renames are handled by pinning the old identifier:

```python
class Asset(PlatformManagedMetaTable, Base):
    __metatable_identifier__ = "msm.markets.models.Asset"
```

Users do not need to write identifiers for every table at initial creation. The
default import path is sufficient until they intentionally move or rename a
model and need to preserve the previous identity.

## MigrationMetaTable

`MigrationMetaTable` is the SDK base class for migration registry tables. A
project can use the default registry model or subclass it for domain-specific
release metadata.

The default registry stores one row per packaged migration revision and
direction. Required columns include:

- `uid`
- `package`
- `migration_namespace`
- `revision`
- `down_revision`
- `direction`
- `version`
- `target_data_source_uid`
- `expected_current_revision`
- `manifest`
- `manifest_sha256`
- `operations`
- `sql`
- `sql_sha256`
- `statement_boundaries`
- `affected_tables`
- `old_contracts`
- `old_contract_hashes`
- `new_contract_hashes`
- `new_contracts`
- `idempotency_key`
- `lock_key`
- `status` as a convenience mirror only
- `previous_revision`
- `applied_revision`
- `executed_statement_count`
- `affected_table_uids`
- `introspection_snapshots`
- `error`
- `created_at`
- `started_at`
- `finished_at`

The registry table can contain migrations for many logical tables. It is keyed
by package and namespace, not by the MetaTables that happen to be affected.

The registry table itself is a MetaTable. It can be discovered, labeled,
queried, and extended like any other platform-managed MetaTable.

## Project Extensions

Project packages may extend `MigrationMetaTable` directly:

```python
class MarketsMigration(MigrationMetaTable, Base):
    __metatable_namespace__ = "msm"
    __metatable_identifier__ = "markets_migrations"
    __metatable_description__ = "Packaged ms-markets schema migrations and run state."

    release_channel: Mapped[str | None] = mapped_column(String(64))
    minimum_sdk_version: Mapped[str | None] = mapped_column(String(64))
    domain_contract_version: Mapped[str | None] = mapped_column(String(64))
```

Extension fields are client/package-owned metadata. They can support release
channels, diagnostics, compatibility checks, rollback notes, or domain-specific
validation. They are not a separate backend approval mechanism and they are not
authoritative execution state.

The only hard requirement is that the subclass preserves the required migration
contract columns. Backend execution logic reads the migration operation through
those required fields.

## Registry Sync

The SDK syncs migrations into the registry before apply:

1. Load the migration manifest from the installed package.
2. Load SQL files from the installed package.
3. Compute `manifest_sha256` and `sql_sha256`.
4. Compute expected old and new contract hashes from SQLAlchemy MetaTable
   declarations.
5. Resolve affected table identifiers from explicit
   `__metatable_identifier__` or canonical class paths.
6. Verify that affected in-place tables use identifier-addressed migration
   registration, not shape-addressed registration.
7. Register the `MigrationMetaTable` subclass if needed.
8. Insert or update the migration row in the registry MetaTable.

The sync path is client-side. It is equivalent to declaring any other
platform-managed MetaTable and writing governed rows into it.

This replaces a backend artifact registration endpoint. There is no separate
server-side artifact registry.

## Migration Application

The SDK applies a migration through the backend MetaTable action endpoint:

```text
POST /ts_manager/meta_table/apply-migration/
```

The canonical wire payload uses `snake_case` field names. SDK helper APIs may
accept selected camelCase aliases for caller convenience, but the backend
contract is `snake_case`.

The request body is:

```json
{
  "version": "metatable-migration.v1",
  "migration_meta_table_uid": "uuid",
  "migration_row_uid": "uuid",
  "data_source_uid": "uuid",
  "package": "msm",
  "migration_namespace": "markets",
  "revision": "001_create_asset_tag",
  "down_revision": null,
  "direction": "upgrade",
  "expected_current_revision": null,
  "manifest_sha256": "64 lowercase hex characters",
  "sql_sha256": "64 lowercase hex characters",
  "affected_tables": [
    {
      "identifier": "msm.markets.models.Asset",
      "namespace": null,
      "meta_table_uid": null,
      "physical_table_name": null
    }
  ],
  "old_contract_hashes": {
    "msm.markets.models.Asset": "64 lowercase hex characters"
  },
  "new_contract_hashes": {
    "msm.markets.models.Asset": "64 lowercase hex characters"
  },
  "idempotency_key": "metatable-migration:<migration_row_uid>:<sql_sha256>",
  "lock_key": "<data_source_uid>:<package>:<migration_namespace>",
  "dry_run": false
}
```

Field meanings:

- `version`: literal `metatable-migration.v1`.
- `migration_meta_table_uid`: UID of the registered `MigrationMetaTable`.
- `migration_row_uid`: UID of the registry row containing the manifest and SQL.
- `data_source_uid`: target `DynamicTableDataSource` for DDL execution.
- `package`: package or migration stream owner.
- `migration_namespace`: package-local revision stream.
- `revision`: target revision for this operation.
- `down_revision`: expected parent revision, or `null` for bootstrap.
- `direction`: `upgrade` or `downgrade`.
- `expected_current_revision`: current revision required before execution, or
  `null` for bootstrap.
- `manifest_sha256`: checksum expected for the registry row manifest payload.
- `sql_sha256`: checksum expected for the registry row SQL payload.
- `affected_tables`: stable logical table identifiers and optional hints.
- `old_contract_hashes`: expected pre-migration contract hashes by identifier.
- `new_contract_hashes`: expected post-migration contract hashes by identifier.
- `idempotency_key`: retry safety key for this exact row and SQL checksum.
- `lock_key`: logical migration lock key.
- `dry_run`: validate-only flag.

The apply request references a row in a registered `MigrationMetaTable`. It must
not contain executable SQL statements, SQL statement arrays, manifest bodies, or
backend-side Python hooks directly in the request body.

The backend must load the registry row and validate that it contains at least:

```json
{
  "uid": "uuid",
  "version": "metatable-migration.v1",
  "package": "msm",
  "migration_namespace": "markets",
  "revision": "001_create_asset_tag",
  "down_revision": null,
  "direction": "upgrade",
  "target_data_source_uid": "uuid",
  "expected_current_revision": null,
  "manifest": {},
  "manifest_sha256": "64 lowercase hex characters",
  "sql": "SQL text loaded from the installed package by the SDK",
  "sql_sha256": "64 lowercase hex characters",
  "statement_boundaries": [],
  "affected_tables": [],
  "old_contract_hashes": {},
  "new_contract_hashes": {},
  "new_contracts": {},
  "idempotency_key": "metatable-migration:<migration_row_uid>:<sql_sha256>",
  "lock_key": "<data_source_uid>:<package>:<migration_namespace>",
  "status": "pending"
}
```

The backend validates the row against the request before it executes anything:

- request `migration_row_uid` equals row `uid`
- request `data_source_uid` equals row `target_data_source_uid`
- request `package`, `migration_namespace`, `revision`, `down_revision`,
  `direction`, and `expected_current_revision` equal the row values
- request `manifest_sha256` equals row `manifest_sha256`
- request `sql_sha256` equals row `sql_sha256`
- recomputed SHA-256 of row `manifest` equals request `manifest_sha256`
- recomputed SHA-256 of row `sql` equals request `sql_sha256`
- request `idempotency_key` equals row `idempotency_key`
- request `lock_key` equals row `lock_key`

The backend must:

1. Resolve `migration_meta_table_uid`.
2. Read the migration row identified by `migration_row_uid`.
3. Verify that the row fields match the request identity and checksums.
4. Recompute `manifest_sha256`, operation checksums, and `sql_sha256` from the
   row content.
5. Acquire a migration lock for
   `(data_source_uid, package, migration_namespace)`.
6. Check the current revision from successful backend ledger rows after the
   lock is acquired.
7. Validate `expected_current_revision`.
8. Resolve affected existing MetaTables by scoped `identifier`.
9. Validate that resolved in-place tables keep the same stable `storage_hash`.
10. Validate old contract hashes for affected existing tables.
11. If `dry_run` is true, stop after validation and return the planned result.
12. Execute the row's SQL in one transaction when the database supports
    transactional DDL.
13. Introspect affected physical tables.
14. Create, import, or refresh affected platform MetaTable resources.
15. Validate new contract hashes using backend-normalized contracts.
16. Update existing MetaTable contract/projection rows without changing the
    stable `storage_hash` for in-place migrated tables.
17. Update the backend `MetaTableMigrationRun` ledger and optionally mirror
    status to the registry row.
18. Release the lock after success or failure.

The apply response body is:

```json
{
  "ok": true,
  "version": "metatable-migration.v1",
  "dry_run": false,
  "migration_meta_table_uid": "uuid",
  "migration_row_uid": "uuid",
  "data_source_uid": "uuid",
  "package": "msm",
  "migration_namespace": "markets",
  "revision": "001_create_asset_tag",
  "direction": "upgrade",
  "migration_run_uid": "uuid",
  "status": "applied",
  "previous_revision": null,
  "applied_revision": "001_create_asset_tag",
  "executed_statement_count": 3,
  "affected_tables": [
    {
      "identifier": "msm.markets.models.Asset",
      "meta_table_uid": "uuid",
      "physical_table_name": "backend.physical_table",
      "action": "refreshed",
      "storage_hash": "stable storage hash",
      "previous_contract_hash": "64 lowercase hex characters",
      "new_contract_hash": "64 lowercase hex characters",
      "introspection": {}
    }
  ],
  "created_meta_table_uids": [],
  "imported_meta_table_uids": [],
  "refreshed_meta_table_uids": ["uuid"],
  "introspection_snapshots": {
    "msm.markets.models.Asset": {}
  },
  "registry_update": {
    "migration_meta_table_uid": "uuid",
    "migration_row_uid": "uuid",
    "status": "applied"
  },
  "error": null
}
```

For `dry_run: true`, `ok` is true when validation succeeds, `dry_run` remains
true, `executed_statement_count` is `0`, `applied_revision` is the planned
revision, affected table `action` values may be `planned`, and
backend ledger status is `validated`.

For validation or execution failure, the backend should return the same shape
with `ok: false`, a backend ledger failure status, and:

```json
{
  "error": {
    "code": "checksum_mismatch",
    "message": "Human-readable failure reason.",
    "details": {}
  }
}
```

The HTTP status should still reflect the failure class. Examples: `400` for
malformed requests, `403` for missing permission, `404` for unknown registry
tables or rows, `409` for revision, lock, idempotency, identifier, storage-hash,
or contract-hash conflicts, and `500` for unexpected execution failures after
the registry row is marked failed.

The migration status endpoint is:

```text
POST /ts_manager/meta_table/migration-status/
```

The status request body is:

```json
{
  "migration_meta_table_uid": "uuid",
  "data_source_uid": "uuid",
  "package": "msm",
  "migration_namespace": "markets"
}
```

`data_source_uid` may be `null` only when the caller wants package/namespace
status across all data sources visible to the caller.

The status response body is backed by the backend migration run ledger:

```json
{
  "ok": true,
  "migration_meta_table_uid": "uuid",
  "data_source_uid": "uuid",
  "package": "msm",
  "migration_namespace": "markets",
  "current_revision": "001_create_asset_tag",
  "latest_successful_revision": "001_create_asset_tag",
  "latest_attempted_revision": "001_create_asset_tag",
  "runs": [
    {
      "migration_run_uid": "uuid",
      "migration_row_uid": "uuid",
      "revision": "001_create_asset_tag",
      "down_revision": null,
      "direction": "upgrade",
      "status": "applied",
      "previous_revision": null,
      "applied_revision": "001_create_asset_tag",
      "executed_statement_count": 3,
      "manifest_sha256": "64 lowercase hex characters",
      "sql_sha256": "64 lowercase hex characters",
      "started_at": "2026-06-01T00:00:00Z",
      "finished_at": "2026-06-01T00:00:01Z",
      "error": null
    }
  ],
  "error": null
}
```

## SDK Responsibilities

The SDK owns packaging, registry management, and local validation:

- define `MigrationMetaTable`
- allow projects to subclass `MigrationMetaTable`
- store migration artifacts in the registry, not authoritative execution state
- define the identifier-addressed registration mode for migration-managed
  MetaTables
- derive default table identifiers from canonical class import paths
- register the migration registry MetaTable
- load migration manifests from installed packages
- load SQL files from installed packages
- compute artifact checksums
- validate revision lineage
- split SQL using explicit statement boundary metadata
- serialize structured operation plans for standard schema changes
- compute old and new contract hashes from SQLAlchemy MetaTable declarations
- include full old and new contracts so the backend can recompute
  authoritative hashes
- reject in-place migration packaging for shape-addressed table declarations
- insert packaged migration rows into the registry MetaTable
- construct typed `metatable-migration.v1` requests
- expose helper methods for registry sync, status, dry run, and apply

The SDK does not:

- autogenerate migrations
- run Alembic environments
- execute backend-side Python hooks
- diff arbitrary database schemas
- require a server-side artifact registry before a migration can be applied

## Backend Responsibilities

The backend owns authority and side effects during apply:

- permission checks for schema migration execution
- backend-owned `MetaTableMigrationRun` execution ledger
- migration locks
- affected table resolution by scoped `identifier`
- current revision checks against backend ledger rows
- idempotency
- transactional execution when supported
- table introspection
- MetaTable create/import/refresh
- stable `storage_hash` preservation for in-place migrated tables
- old and new contract hash validation
- optional registry row status mirroring

The backend must reject apply requests for unknown registry tables, unknown
migration rows, checksum mismatches, revision mismatches, lock conflicts,
idempotency conflicts, identifier ambiguities, attempts to migrate
shape-addressed tables in place, storage hash drift, and contract hash
mismatches.

The backend must not require migrations to be stored in a server-side artifact
table. The registry is supplied by the client as a normal MetaTable.

The backend ledger, not the registry row, is authoritative for current
revision, idempotency, status, recovery state, and audit metadata.

## Trust Boundary

The migration registry is client-defined, but migration execution is still a
privileged backend operation. The backend controls whether the caller may run
DDL against the target data source.

The apply endpoint should not accept direct SQL statements in the request body.
It should execute only SQL loaded from the referenced `MigrationMetaTable` row
after checksum, revision, lock, idempotency, and contract validation.

The apply endpoint is also the only supported path for in-place schema changes
to migratable platform-managed tables. Normal registration must not silently
update an existing table contract when the SQLAlchemy shape changes.

This keeps migration authoring extensible while preserving backend authority
over execution.

## Consequences

All migrations for a data source can live in one project-defined registry
MetaTable. The registry is keyed by package and namespace, so a single table can
track migrations across many logical MetaTables.

The migration registry benefits from the same SQLAlchemy declaration,
registration, search, labels, governed operations, and extension model as any
other MetaTable, while execution truth lives in the backend ledger.

Project-specific extension fields are first-class. A package can extend
`MigrationMetaTable` with release, compatibility, ownership, or diagnostics
metadata without waiting for backend schema changes.

Migratable data tables are resolved by scoped `identifier`, using explicit
`__metatable_identifier__` when present and canonical class import path
otherwise. This makes schema changes recoverable from the new code because the
resolver does not depend on columns, indexes, foreign keys, or constraints.

Shape-addressed `PlatformManagedMetaTable` remains useful for create-new table
patterns, but it is not an in-place migration target. Once the SQLAlchemy shape
changes, the previous shape-derived storage hash is not recoverable from the new
class alone.

The first implementation remains small. It is a packaged SQL migration runner
with a client-defined MetaTable registry and backend DDL execution, not a
general migration engine.

## Implementation Tasks

- [x] Make SQLAlchemy a required SDK dependency.
- [x] Add `mainsequence.meta_tables.migrations`.
- [x] Add a SQLAlchemy-compatible `MigrationMetaTable` base class.
- [x] Add a default registry model with the required migration columns.
- [x] Allow projects to subclass `MigrationMetaTable` with extension columns.
- [x] Add Pydantic models for `metatable-migration.v1` apply requests.
- [x] Add typed Pydantic response models for migration apply/status responses.
- [x] Add backend-ledger status fields to migration apply/status response
  models.
- [x] Add package manifest and SQL loading helpers.
- [x] Add checksum helpers for manifests and SQL artifacts.
- [x] Add structured operation-plan serialization for registry artifacts.
- [x] Add contract hash helpers based on SQLAlchemy MetaTable declarations.
- [x] Include full old/new contracts in registry artifacts.
- [x] Add registry row construction helpers.
- [x] Add registry upsert operation helpers for inserting packaged migrations
  into a registered `MigrationMetaTable`.
- [x] Add SDK migration apply helper that sends a referenced registry row to
  the backend apply endpoint.
- [x] Add `MetaTable.apply_migration(...)`.
- [x] Add SDK migration status helper.
- [x] Add `MetaTable.get_migration_status(...)`.
- [x] Add SDK documentation for client-defined MetaTable migrations.
- [x] Add an SDK example with a registry subclass, bundled manifest, bundled
  SQL, and dry-run apply operation construction.
- [x] Add SDK tests for migration request payloads, registry helpers, package
  loading, checksums, contract hashes, and example operation construction.
- [x] Add an identifier-addressed registration mode for migration-managed
  SQLAlchemy MetaTables so registration resolves by scoped `identifier`, not by
  SQLAlchemy table shape.
- [x] Add canonical class-path identifier derivation for migration-managed
  MetaTables: explicit `__metatable_identifier__`, else `model.__module__ +
  "." + model.__qualname__`.
- [x] Add validation that in-place affected tables use the
  identifier-addressed migration registration mode.
- [ ] Add backend apply endpoint with registry-row loading, locking,
  idempotency, identifier-based table resolution, stable storage-hash
  preservation, transaction handling, introspection, MetaTable refresh, and
  registry status update.
- [ ] Add backend migration-status endpoint or registry status read path.
- [ ] Add an `ms-markets` migration registry subclass using bundled SQL,
  manifests, and SQLAlchemy post-migration contracts.

## Non-Goals

This ADR does not introduce automatic schema diffing.

This ADR does not make Alembic the platform migration runtime.

This ADR does not introduce a server-side migration artifact registry.

This ADR does not prevent projects from extending the migration registry
MetaTable for their own release workflow metadata.

This ADR does not make shape-addressed `PlatformManagedMetaTable` declarations
in-place migratable.
