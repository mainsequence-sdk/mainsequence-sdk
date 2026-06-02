# ADR 0021: Migration-First Platform-Managed MetaTables

Date: 2026-06-02

Status: Proposed

## Context

ADR 0020 moves MetaTable schema migrations to Alembic. That is necessary, but
not sufficient for platform-managed MetaTables.

Platform-managed MetaTables currently have two competing lifecycle paths:

1. direct model registration, where `Model.register()` sends a table contract to
   TS Manager and TS Manager creates the physical table; and
2. migration execution, where Alembic renders SQL and TS Manager applies that
   SQL.

Keeping both as normal user-facing paths creates a broken workflow:

- users must know whether to call `Model.register()` before migrations;
- initial platform-managed tables have no registered MetaTable row, UID, or
  backend physical table name for Alembic to target;
- changed SQLAlchemy models can rotate storage identity before the SDK can
  recover the previous table;
- `migrations upgrade` cannot be trusted as the single command that brings
  database schema and platform catalog metadata into a coherent state.

The provider already contains the right user-facing shape:

```python
migration = AlembicMetaTableMigration(
    package="msm",
    migration_namespace=markets_namespace(),
    script_location="msm:migrations",
    target_metadata=MarketsBase.metadata,
    alembic_registry=MarketsAlembicVersion,
    metatable_models=migration_model_registry(),
    after_register_metatables=refresh_markets_catalog_from_registered_metatables,
)
```

That provider must be authoritative for platform-managed MetaTable lifecycle.

## Decision

All `PlatformManagedMetaTable` and `PlatformTimeIndexMetaData` models are
migration-first.

Users should not call `Model.register()` to bootstrap or evolve
platform-managed tables. The only normal user-facing path is:

```bash
mainsequence migrations revision --provider msm.migrations:migration
mainsequence migrations render --provider msm.migrations:migration --to head
mainsequence migrations upgrade --provider msm.migrations:migration --to head
```

`Model.register()` may continue to exist as an SDK method, but it is no longer a
public workflow for platform-managed models. It may only execute inside the
migration workflow or an explicit internal migration context.

Outside the migration workflow, attempting to use an unregistered
platform-managed MetaTable must fail with a clear error:

```text
Platform-managed MetaTable '<identifier>' is not registered. Run
`mainsequence migrations upgrade --provider <provider> --to head`.
```

No other SDK path should silently auto-register platform-managed MetaTables.

## Migration-First Lifecycle

`migrations upgrade` becomes the single command that guarantees a coherent
schema/catalog state for all provider-scoped platform-managed models.

The required lifecycle is:

```text
1. load AlembicMetaTableMigration provider
2. ensure/register AlembicVersionMetaTable
3. resolve or register provider.metatable_models
4. bind SQLAlchemy models to returned/existing physical table names
5. read current Alembic revision
6. render Alembic SQL against bound physical table names
7. backend dry-run validates revision precondition and SQL artifact
8. backend applies SQL
9. SDK refreshes MetaTable catalog rows
10. SDK runs after_register_metatables(final_metatables)
```

The key change is step 3.

Before SQL rendering, the SDK must resolve every provider-scoped
platform-managed MetaTable. Resolution must:

- resolve models by global `identifier`;
- return an existing MetaTable when one exists;
- call the existing platform-managed `register()` path when one does not exist;
- let the backend create the initial physical table through normal
  platform-managed registration;
- bind the SQLAlchemy model to the returned MetaTable UID, data source UID,
  storage identity, and backend physical table name.

After resolution, Alembic renders SQL against the backend physical table names,
not against temporary logical/hash names.

After SQL apply, catalog refresh must:

- introspect or validate the affected physical tables;
- refresh column, index, foreign-key, and time-index catalog projections;
- preserve the same MetaTable UID for existing tables;
- return the final MetaTable rows to the SDK;
- run `after_register_metatables` with those final rows.

If SQL apply fails after an initial hidden registration, the table exists and
the MetaTable row remains active. That is acceptable because creation still used
the current platform-managed backend authority path. The failed migration can be
retried after fixing the Alembic SQL.

## Backend Contract

ADR 0020's SQL apply contract remains. No new backend prepare endpoint is
required.

Platform-managed initial table creation uses the existing backend
`MetaTable.register()` endpoint. The migration CLI is the only normal
user-facing caller for platform-managed models:

```text
migrations upgrade
-> Model.register() for missing provider-scoped platform-managed models
-> backend creates the physical table and returns physical_table_name
-> SDK binds the model
-> Alembic renders evolution SQL against the bound physical table
```

The apply request remains an Alembic-rendered SQL artifact and still contains no
request-side data source UID.

### Apply Request

```json
{
  "version": "metatable-migration.v1",
  "alembic_version_meta_table_uid": "uuid",
  "package": "msm",
  "migration_namespace": "markets",
  "revision": "0001",
  "down_revision": null,
  "direction": "upgrade",
  "expected_current_revision": null,
  "manifest": {},
  "manifest_sha256": "...",
  "sql": "CREATE TABLE backend_allocated_asset_table (...);",
  "sql_sha256": "...",
  "statement_boundaries": [],
  "affected_tables": [
    {
      "identifier": "mainsequencemarkets:msm.models.Asset",
      "meta_table_uid": "uuid"
    }
  ],
  "dry_run": false
}
```

### Apply Response

The backend response remains the direct SQL apply response. SDK migration
tooling performs catalog refresh after apply and then passes the final
MetaTables to `after_register_metatables`.

```json
{
  "ok": true,
  "previous_revision": null,
  "applied_revision": "0001",
  "executed_statement_count": 4
}
```

## Sequential Alembic Revision IDs

The SDK CLI must not rely on Alembic's random revision IDs for the standard
workflow.

When the user does not pass `--rev-id`, `mainsequence migrations revision`
must generate the next sequential revision ID:

```text
0001
0002
0003
...
```

With the default message, the first generated file should be:

```text
migrations/versions/0001_migration.py
```

Rules:

- scan the provider's `versions/` directory through Alembic's script directory;
- find numeric revision IDs matching `^\d{4,}$`;
- choose max + 1, padded to four digits;
- if no revisions exist, choose `0001`;
- if non-numeric heads or multiple heads exist, fail unless the user supplies
  `--rev-id`;
- preserve explicit `--rev-id` for advanced cases;
- keep `-m/--message` optional, defaulting to `migration`.

This keeps the normal migration history readable and avoids random filenames
such as `c15aca390d02_migration.py`.

## Consequences

### Positive

- Users get one platform-managed workflow: `migrations upgrade`.
- Project code does not need scattered `Model.register()` bootstrap calls.
- Alembic SQL always targets backend-resolved physical table names.
- Initial and later migrations share the same lifecycle.
- Catalog state and schema state are reconciled together.
- Missing MetaTables outside migration fail loudly instead of being silently
  auto-created in arbitrary runtime paths.

### Negative

- Existing direct-registration tutorials and examples must be rewritten.
- SDK runtime paths that used opportunistic registration must become stricter.
- Migration retry behavior must account for hidden initial registrations that
  already created physical tables.

## Required Removals

Remove or restrict these user-facing behaviors:

- direct platform-managed `Model.register()` bootstrap from docs, tutorials,
  examples, and skills;
- automatic platform-managed registration from DataNode runtime paths,
  compiled-SQL helpers, operation builders, or other non-migration paths;
- docs that present platform-managed table creation as a separate step before
  migrations;
- migration examples that require manual `register-version-table`;
- random Alembic revision IDs in the standard CLI workflow.

`MetaTable.register()` and model `register()` may remain as implementation
methods, but platform-managed calls must require an internal migration context
or be treated as private SDK plumbing.

## Implementation Tasks

- [x] Update `AlembicMetaTableMigration` to resolve/register and bind
  `metatable_models` before render/apply using existing registration APIs.
- [x] Make `migrations render` resolve/register and bind models before SQL
  rendering.
- [x] Make `migrations upgrade` resolve/register, dry-run, apply, refresh, and run
  `after_register_metatables` as one workflow.
- [x] Add an internal migration context that allows platform-managed
  `Model.register()` only from migration tooling.
- [x] Remove non-migration automatic registration from DataNode and MetaTable
  runtime paths.
- [x] Raise clear errors when unregistered platform-managed MetaTables are used
  outside migration tooling.
- [x] Generate sequential Alembic revision IDs when `--rev-id` is omitted.
- [x] Update ADR 0020 to defer platform-managed lifecycle details to this ADR.
- [x] Update `docs/knowledge/meta_tables/sqlalchemy.md`.
- [x] Update `docs/knowledge/meta_tables/migrations.md`.
- [x] Update `docs/knowledge/meta_tables/api.md`.
- [x] Update `docs/tutorial/metatable_migrations.md`.
- [x] Update MetaTable examples to be migration-first only.
- [x] Update MetaTable skill guidance to stop recommending direct registration
  for platform-managed models.
- [x] Add tests that `upgrade` resolves/registers and binds missing
  platform-managed models before render.
- [x] Add tests that runtime/non-migration paths fail on unregistered
  platform-managed models.
- [x] Add tests for sequential revision IDs.
