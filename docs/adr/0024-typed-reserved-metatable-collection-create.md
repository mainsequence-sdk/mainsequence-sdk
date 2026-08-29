# ADR 0024: Typed Reserved MetaTable Collection Create

Date: 2026-06-04

Status: Implemented

Supersedes-in-part: ADR 0022 `MetaTable.reserve_managed(...)` reservation
surface.

ADR 0028 supersedes the `storage_hash` request fields shown in the original
decision. Current collection-create rows do not send a storage hash.

Depends on backend ADR: TS Manager ADR-019, "Remove `reserve-managed/` And Use
Typed Collection Create".

## Context

ADR 0022 made the SDK a thin Alembic adapter and introduced
`MetaTable.reserve_managed(...)` as the SDK reservation call before Alembic
provisioning. That backend endpoint was too specialized. It duplicated
registration logic, resolved contracts, handled foreign-key bindings, saved rows
twice, and could spend tens of seconds creating a modest provider catalog.

That shape is wrong for the current migration-first design:

- Alembic owns physical DDL.
- SQLAlchemy metadata owns table, index, and foreign-key declarations.
- TS Manager owns catalog rows and post-DDL finalization.
- reservation before provisioning only needs catalog rows in
  `provisioning_status="reserved"`.

The backend now exposes normal typed collection-create semantics instead of a
special `reserve-managed/` reservation engine:

```text
POST /api/v1/meta-tables/
POST /api/v1/time-index-meta-tables/
```

The SDK must move to that contract.

The critical requirement is that the SDK must call the correct endpoint before
provisioning:

- `PlatformManagedMetaTable` must reserve through the MetaTable endpoint;
- `PlatformTimeIndexMetaTable` must reserve through the time-index MetaTable endpoint.

Calling the MetaTable endpoint for a time-indexed model creates the wrong
backend model: a plain parent `MetaTable` row instead of the concrete
`DynamicTableMetaData` row needed by TS Manager.

## Decision

Remove the SDK use of `MetaTable.reserve_managed(...)` from the Alembic
pre-provisioning path.

`AlembicMetaTableMigration.prepare_for_alembic()` must build ordinary typed
reserved-row create payloads and split them by SDK model type:

```text
PlatformManagedMetaTable     -> POST /api/v1/meta-tables/
PlatformTimeIndexMetaTable    -> POST /api/v1/time-index-meta-tables/
```

Both calls use a raw JSON list body. There is no `{ "items": [...] }` wrapper
and no `/bulk-create/` endpoint.

"Raw JSON list" defines the backend transport shape. It does not mean callers
may submit unchecked lifecycle combinations. The SDK validates relational rows
with `ManagedMetaTableCollectionCreateRow` and time-indexed rows with
`ManagedTimeIndexMetaTableCollectionCreateRow` before serialization. Their
literal fields admit only platform-managed, Alembic-managed, reserved intent.

Each list item must explicitly declare Alembic ownership with:

```json
{
  "is_alembic_managed": true
}
```

The backend maps that request flag to its internal
`schema_management_mode="alembic_managed"` model field and enforces deletion protection.
The SDK must not rely on the endpoint alone to imply Alembic management and must not own
`protect_from_deletion` for this flow.

## Request Shape

These are branch-owned backend wire shapes. Under ADR 0031, the SDK derives and
inserts `project_context` from the frozen Git-native context in both local and
deployed project-code runs; application callers cannot provide or override it.
The backend additionally verifies deployed equality against the authenticated
runtime target.

The relational MetaTable reservation payload must include:

```json
[
  {
    "data_source_uid": "864e7c22-482a-464a-8758-0d3408abd77f",
    "identifier": "mainsequence.examples.Asset",
    "namespace": "mainsequence.examples",
    "description": "",
    "management_mode": "platform_managed",
    "provisioning_status": "reserved",
    "is_alembic_managed": true,
    "migration_package": "msm",
    "migration_namespace": "mainsequence.examples",
    "migration_provider_key": "msm:mainsequence.examples",
    "alembic_version_meta_table_uid": "00000000-0000-4000-8000-000000000000",
    "alembic_revision": null,
    "physical_schema": "public",
    "physical_table_name": "ms_markets__asset__mainsequence_examples",
    "project_context": {
      "code_repository_branch_uid": "00000000-0000-4000-8000-000000000001"
    },
    "table_contract": {
      "version": "relational-table.v1",
      "physical": {
        "schema": "public",
        "table_name": "ms_markets__asset__mainsequence_examples"
      },
      "columns": []
    },
    "labels": []
  }
]
```

The time-indexed reservation payload uses the time-index MetaTable endpoint and
must also include its first-class fields:

```json
[
  {
    "data_source_uid": "864e7c22-482a-464a-8758-0d3408abd77f",
    "identifier": "mainsequence.examples.Prices",
    "namespace": "mainsequence.examples",
    "description": "",
    "management_mode": "platform_managed",
    "provisioning_status": "reserved",
    "is_alembic_managed": true,
    "migration_package": "msm",
    "migration_namespace": "mainsequence.examples",
    "migration_provider_key": "msm:mainsequence.examples",
    "alembic_version_meta_table_uid": "00000000-0000-4000-8000-000000000000",
    "alembic_revision": null,
    "physical_schema": "public",
    "physical_table_name": "ms_markets__prices__mainsequence_examples",
    "project_context": {
      "code_repository_branch_uid": "00000000-0000-4000-8000-000000000001"
    },
    "time_index_name": "time_index",
    "partition_strategy": "backend_default",
    "table_contract": {
      "version": "relational-table.v1",
      "physical": {
        "schema": "public",
        "table_name": "ms_markets__prices__mainsequence_examples"
      },
      "columns": []
    },
    "labels": []
  }
]
```

## Required SDK Changes

Remove these managed-reservation-only client models from the active migration
path and, if they have no remaining callers, delete them:

- `ManagedMetaTableReservationRequest`
- `ManagedMetaTableReservationTable`
- `ManagedMetaTableReservationResponse`
- `ManagedMetaTableReservationItem`

Remove or stop using:

- `MetaTable.reserve_managed(...)`
- SDK code that posts to
  `POST /api/v1/meta-tables/reserve-managed/`

Add or use collection-create methods:

- `MetaTable.bulk_create(...)`, posting a raw list to
  `POST /api/v1/meta-tables/`
- `TimeIndexMetaTable.bulk_create(...)`, posting a raw list to
  `POST /api/v1/time-index-meta-tables/`

The method name may remain `bulk_create(...)` as a client convenience, but the
HTTP contract is normal collection `POST` with a list body.

`AlembicMetaTableMigration.prepare_for_alembic()` must:

1. build reservation rows from provider models;
2. split rows by concrete SDK model class;
3. call `MetaTable.bulk_create(...)` for `PlatformManagedMetaTable`;
4. call `TimeIndexMetaTable.bulk_create(...)` for `PlatformTimeIndexMetaTable`;
5. include `is_alembic_managed=true` on every row;
6. include `provisioning_status="reserved"` on every row;
7. include provider metadata:
   `migration_package`, `migration_namespace`, `migration_provider_key`,
   `alembic_version_meta_table_uid`, and `alembic_revision`;
8. include authored `physical_table_name` before Alembic runs so Alembic renders
   against the correct SQLAlchemy table name;
9. internally include the Git-resolved exact CodeRepositoryBranch UID in
   `project_context` on every managed wire row, including deployed project-code
   runtimes;
10. include `time_index_name` and `partition_strategy` only for the
   time-indexed endpoint;
11. bind returned `MetaTable` and `TimeIndexMetaTable` rows back to the
    SQLAlchemy models that produced each reservation intent;
12. keep `finalize_managed(...)` as the post-Alembic activation call.

## Non-Goals

The SDK must not reintroduce backend contract management during reservation:

- no FK target resolution;
- no index or FK contract serialization;
- no contract-hash comparison;
- no backend physical-table creation request;
- no backend Alembic execution request;
- no compatibility fallback to `reserve-managed/`.

Natural conflicts should come from the backend collection create/database
constraints and be reported as ordinary API errors. The SDK should not recreate
the old reservation conflict-diagnostic engine.

## Implementation Tasks

- [x] Remove `MetaTable.reserve_managed(...)` from the Alembic preparation path.
- [x] Remove managed-reservation-only Pydantic models when no remaining callers
  need them.
- [x] Add or update `MetaTable.bulk_create(...)` to post a raw list to
  `/api/v1/meta-tables/`.
- [x] Add or update `TimeIndexMetaTable.bulk_create(...)` to post a raw list to
  `/api/v1/time-index-meta-tables/`.
- [x] Change `AlembicMetaTableMigration.prepare_for_alembic()` to split
  reservation rows by model type and call the correct typed endpoint before
  provisioning.
- [x] Ensure every reservation row includes `is_alembic_managed=true`.
- [x] Ensure every reservation row includes
  `provisioning_status="reserved"`.
- [x] Ensure `PlatformTimeIndexMetaTable` rows include `time_index_name`,
  optional `cadence`, and `partition_strategy`.
- [x] Bind returned rows to the source SQLAlchemy models in request order per
  endpoint.
- [x] Keep `MetaTable.finalize_managed(...)` and
  `AlembicMetaTableMigration.finalize_metatable_catalog(...)`.
- [x] Reserve the Alembic version MetaTable through the same collection-create
  contract as a platform-managed, Alembic-managed root with no parent registry
  UID.
- [x] Include the registry root UID with provider child UIDs in managed
  finalization.
- [x] Update SDK knowledge/tutorial docs that still mention
  `reserve-managed/`.
- [x] Add focused SDK tests for typed reservation routing:
  `PlatformManagedMetaTable` uses `/api/v1/meta-tables/` and
  `PlatformTimeIndexMetaTable` uses `/api/v1/time-index-meta-tables/`.
- [x] Add focused SDK tests proving the request body is a raw JSON list and
  contains `is_alembic_managed=true`.
- [x] Validate every row through its endpoint-specific typed SDK model before
  HTTP and reject non-managed, non-Alembic, or non-reserved input.
- [x] Make Alembic registry ensure idempotent across CLI processes: resolve by
  DataSource/schema/table physical identity, validate provider-root ownership,
  reuse compatible rows, and create only when missing.
- [x] Recheck and validate registry identity after a collection-create conflict
  so concurrent ensure operations cannot produce duplicate roots.

## Consequences

The SDK preparation step becomes a thin catalog-reservation client. It no longer
asks the backend to reconcile table shape before provisioning. It creates the
right backend model type before Alembic runs, then lets Alembic create physical
tables and lets `finalize-managed/` reconcile and activate catalog rows after
successful provisioning.
