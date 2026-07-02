# Implementation Task: First-Class MetaTable Physical Schema Identity

Date: 2026-07-02

## Context

MetaTable canonical storage currently treats `physical_table_name` as the visible
physical table identity, while schema is inconsistently represented:

- SQLAlchemy model metadata can resolve a schema.
- Time-indexed registration already writes `table_contract.physical.schema` in
  some paths.
- Regular MetaTable registration builds a `MetaTablePhysicalContract` with
  `schema`, but the field is excluded from serialization.
- Alembic reserve explicitly strips schema from `table_contract.physical`.
- Backend lookup and matching paths use `physical_table_name` only.

That leaves a correctness gap for tables that share the same table name across
different schemas. The intended physical identity must be:

```text
(data_source_uid, physical_schema, physical_table_name)
```

`physical_table_name` must remain unqualified. Schema must never be encoded into
the table name as `schema.table`.

## Current Gap And SDK Status

Before this SDK implementation, the SDK could know schema locally but still
treated `physical_table_name` as the practical catalog identity.

SDK gaps addressed by this implementation:

- `MetaTablePhysicalContract.schema_` had `exclude=True`, so regular
  `MetaTable.register(...)` did not send `table_contract.physical.schema`.
- `MetaTable` and `TimeIndexMetaTable` did not expose a top-level
  `physical_schema` field.
- `MetaTable` and `TimeIndexMetaTable` filter sets did not accept
  `physical_schema` or `physical_schema__in`.
- Alembic collection-create rows included `physical_table_name` but not
  `physical_schema`.
- Alembic collection-create stripped schema from `table_contract.physical`.
- Alembic prepare mapped rows by table name only.
- Alembic finalize used table-name fallback before authoritative UID matching.
- `DataNode` storage lookup filtered only by `physical_table_name__in`.
- `APIDataNode.build_from_table_name(...)` is ambiguous because table name alone
  cannot identify storage.

Backend gap:

- The backend must persist, serialize, filter, and enforce `physical_schema` as
  part of MetaTable identity. Without backend support, schema-aware SDK write
  and lookup payloads will only work against compatible backend versions.

## Target Contract

The canonical physical identity for a MetaTable row is:

```text
data_source_uid
physical_schema
physical_table_name
```

Rules:

- `physical_schema` is a first-class top-level MetaTable field.
- `physical_table_name` is always unqualified.
- `table_contract.physical.schema` carries the same schema value in the table
  contract payload.
- For compatibility, omitted schema means the backend-resolved default schema
  for that data source. That default is not globally guaranteed to be `public`.
- The SDK may treat missing backend `physical_schema` as `public` only in legacy
  Postgres/Alembic compatibility paths where the previous behavior was known to
  use the PostgreSQL default schema. New backend responses should be trusted to
  provide `physical_schema`.
- The SDK must not silently mutate deserialized model objects when applying any
  compatibility fallback.
- If both top-level `physical_schema` and `table_contract.physical.schema` are
  provided in a write payload, backend must reject mismatches.

## Backward Compatibility Requirements

This migration must support mixed versions during rollout.

### Old SDK With New Backend

Old SDK clients do not know `physical_schema`.

Backend must remain compatible by:

- defaulting missing write payload schema to the data source's backend-resolved
  default schema;
- continuing to accept existing register/reserve payloads without
  `physical_schema`;
- accepting `table_contract.physical` without `schema`;
- not requiring clients to send `physical_schema` immediately.

Important implication:

- If backend starts returning `physical_schema` before old SDKs are upgraded,
  strict old client models may reject the extra field. Backend should either
  coordinate release timing or expose `physical_schema` only after the SDK model
  accepts it. The safest rollout is SDK model acceptance first, backend response
  field second.

### New SDK With Old Backend

New SDK must not immediately require backend `physical_schema` support.

SDK must remain compatible by:

- allowing backend rows that omit `physical_schema`;
- using a compatibility fallback for missing schema only where the old
  Postgres/Alembic path had a known `public` default;
- keeping registration payloads acceptable to old backend during a compatibility
  window if possible;
- gating schema filters until backend support is available, or using table-name
  lookup plus client-side filtering only when the response contains enough
  schema data.

Important implication:

- Sending `physical_schema__in` to an old backend will likely fail if backend
  filter validation rejects unsupported fields.
- Sending top-level `physical_schema` to old bulk-create/register endpoints may
  fail if serializers reject unknown fields.
- Therefore the strict schema-aware lookup mode must be enabled only after
  backend filter and serializer support exists.

### New SDK With New Backend

This is the target state:

- writes include top-level `physical_schema` and
  `table_contract.physical.schema`;
- reads deserialize `physical_schema`;
- lookups filter by `data_source_uid`, `physical_schema`, and
  `physical_table_name`;
- duplicate detection uses full physical identity, not table name only;
- ambiguous table-only APIs are deprecated or require a schema argument.

## Minimum Safe SDK Implementation

This SDK implementation applies the minimum safe SDK-side change set. Backend
storage, serializer, filter, and uniqueness work remains required for the full
end-to-end contract.

Implemented SDK change set:

- Add `physical_schema` to every relevant client model, request model, result
  model, and projected payload:
  - `MetaTable`;
  - `TimeIndexMetaTable`;
  - `MetaTableRequestFields`;
  - registration request models;
  - managed finalize result models;
  - foreign-key payloads.
- Stop excluding `MetaTablePhysicalContract.schema_` from serialization.
- Stop stripping schema from Alembic `table_contract.physical`.
- Send both top-level `physical_schema` and `table_contract.physical.schema`
  when the backend supports the top-level field.
- Change Alembic prepare lookup and DataNode storage lookup to query and match
  by `(physical_schema, physical_table_name)`, scoped by data source where
  available.
- Replace tests that assert `physical_table_name__in` alone with schema-aware
  assertions once backend filters exist.
- Keep finalize result binding UID-first because reserve/finalize is already
  UID-scoped.

## Backend Implementation Tasks

1. Add `physical_schema` storage to MetaTable catalog rows.

   - Backfill existing rows to the data source's effective default schema.
   - Use `public` only for legacy PostgreSQL rows where that was the actual
     historical default.
   - Keep `physical_table_name` unqualified.
   - Add database constraints or application validation so
     `(data_source_uid, physical_schema, physical_table_name)` is unique for
     rows where a physical table exists.

2. Update backend serializers.

   - Include `physical_schema` in MetaTable responses.
   - Include `physical_schema` in TimeIndexMetaTable responses.
   - Include `physical_schema` in managed finalize table results.
   - Include `target_table_physical_schema` in FK projections.
   - Preserve old fields so old clients still see `physical_table_name`.

3. Update backend write endpoints.

   - Register endpoints accept `physical_schema`.
   - Register endpoints derive `physical_schema` from
     `table_contract.physical.schema` when top-level value is omitted.
   - Bulk collection-create accepts `physical_schema`.
   - Finalize-managed uses schema-aware physical table introspection.
   - Reject mismatches between top-level `physical_schema` and
     `table_contract.physical.schema`.

4. Update backend filters.

   - Support `physical_schema` and `physical_schema__in` for MetaTable.
   - Support the same filters for TimeIndexMetaTable.
   - Keep `physical_table_name` and `physical_table_name__in` working.

5. Add backend compatibility defaults.

   - Missing schema in old payloads resolves to the data source's effective
     default schema.
   - Existing rows without stored schema read as the backfilled/effective schema.
   - Old table-name-only lookup endpoints continue to work but may be ambiguous.

## SDK Implementation Tasks

1. Client models.

   - Add `physical_schema: str | None` to `MetaTable`.
   - Let `TimeIndexMetaTable` inherit the field.
   - Add `physical_schema` to `MetaTableRequestFields` so register payloads can
     carry the top-level identity field.
   - Add `physical_schema` to `MetaTableRegistrationRequest` and
     `TimeIndexMetaTableRegistrationRequest` through the shared request fields
     or equivalent endpoint-specific request model.
   - Add `physical_schema` to `ManagedMetaTableFinalizeTableResult`.
   - Add `target_table_physical_schema` to `MetaTableForeignKeyPayload`.
   - Add `physical_schema` and `physical_schema__in` to filter sets for
     `MetaTable` and `TimeIndexMetaTable`.
   - Add filter normalizers for schema values.

2. Contract serialization.

   - Remove `exclude=True` from `MetaTablePhysicalContract.schema_`.
   - Update the field description to say schema is part of physical identity.
   - Ensure `MetaTableContract.model_dump(..., exclude_none=True)` emits:

     ```json
     {
       "physical": {
         "schema": "resolved_schema",
         "table_name": "example_table"
       }
     }
     ```

   - Keep `physical.table_name` unqualified.
   - Write paths should send both top-level `physical_schema` and
     `table_contract.physical.schema` after backend accepts the top-level field.

3. SQLAlchemy identity helpers.

   Add shared helpers:

   ```python
   _physical_schema_from_table(table) -> str
   _physical_identity_from_model(model) -> tuple[str, str]
   _physical_identity_from_metatable(meta_table) -> tuple[str, str]
   ```

   These helpers should prefer explicit `physical_schema`, fall back to
   `table_contract.physical.schema` when present, and use legacy `public` only
   for old Postgres/Alembic rows that lack both values. They must not mutate the
   source object.

4. Bound model state.

   - Add `__metatable_physical_schema__: ClassVar[str | None] = None`.
   - Set it in `_bind_meta_table(...)`.
   - Add `get_physical_schema()`.
   - Update identity-sensitive code to use schema plus table.
   - Keep `get_physical_table_name()` returning only the unqualified table name.

5. Alembic registry MetaTable.

   - Ensure Alembic version MetaTable registration sends
     `table_contract.physical.schema`.
   - Treat authoring metadata as descriptive only, not physical identity.

6. Alembic prepare flow.

   - Replace table-name maps with physical identity maps.
   - Query backend by `physical_schema__in` and `physical_table_name__in` after
     backend filter support exists.
   - Apply exact client-side filtering by `(schema, table)` to avoid cross-product
     matches.
   - Reject duplicates only when the full physical identity duplicates.

7. Alembic reserve payload.

   - Add `physical_schema` to collection-create rows.
   - Stop removing schema from `table_contract.physical`.
   - Replace `_request_contract_physical_table_name(...)` with a physical
     identity helper.
   - Validate both schema and table are available.

8. Alembic finalize flow.

   - Build `finalized_by_uid = {meta_table_uid: item}` and use it as the
     authoritative finalize result map.
   - Build `finalized_by_identity = {(schema, table): item}` only as a fallback
     for compatibility and diagnostics.
   - Match models by MetaTable UID first. Physical identity fallback must never
     degrade to table-name-only matching.
   - Include schema in finalize failure messages.
   - Include schema in `_metatable_from_finalize_result(...)`.

9. DataNode and APIDataNode lookup.

   - Update registered storage lookup diagnostics to include schema.
   - Use full physical identity once backend schema filters exist.
   - Add `APIDataNode.build_from_physical_identity(schema, table_name, ...)`.
   - Deprecate `APIDataNode.build_from_table_name(...)` as ambiguous, or require
     callers to pass schema explicitly. A temporary `schema="public"` default is
     acceptable only for legacy Postgres compatibility and should warn.

10. Local storage operations.

    - Audit local DuckDB/SQLite interfaces before passing schema through.
    - PostgreSQL identity requires schema; local file-backed engines may not.
    - Do not change local table naming semantics unless those interfaces support
      schema-aware operations explicitly.

## Rollout Plan

Phase 1: SDK accepts future backend responses.

- Add SDK model/request/result fields for `physical_schema`.
- Keep missing schema acceptable.
- Do not switch default lookups to `physical_schema__in` yet.
- Add identity helpers that prefer explicit schema and use `public` only for
  legacy Postgres/Alembic compatibility where no schema is available.

Phase 2: Backend stores and returns schema.

- Add backend field and backfill.
- Accept schema in payloads.
- Return schema in serializers.
- Add filters.
- Enforce uniqueness on `(data_source_uid, physical_schema, physical_table_name)`.

Phase 3: SDK sends schema and uses schema-aware matching.

- Remove `exclude=True` from physical contract schema.
- Stop stripping schema from Alembic reserve payloads.
- Send top-level `physical_schema` where backend accepts it.
- Switch prepare and DataNode lookups to full physical identity.
- Keep finalize binding UID-first, with physical identity only as fallback and
  diagnostics.
- Add table-name-only deprecation warnings.

Phase 4: Strict cleanup.

- Make ambiguous table-name-only APIs error unless `schema` is supplied.
- Remove compatibility fallback code only after supported backend versions always
  return `physical_schema`.

## Test Plan

SDK tests:

- `MetaTablePhysicalContract` serializes `schema`.
- Regular platform-managed registration includes `table_contract.physical.schema`.
- External registered registration includes `table_contract.physical.schema`.
- Time-indexed registration preserves existing schema behavior.
- Register payloads include top-level `physical_schema` when backend-compatible
  schema sending is enabled.
- SDK models accept backend rows with `physical_schema`.
- SDK models accept backend rows without `physical_schema`.
- Filters serialize `physical_schema` and `physical_schema__in`.
- Alembic reserve rows include `physical_schema`.
- Alembic reserve preserves `table_contract.physical.schema`.
- Alembic prepare does not confuse `public.asset` with `analytics.asset`.
- Alembic finalize binds by `meta_table_uid` before physical identity fallback.
- Alembic finalize never falls back to table-name-only matching.
- DataNode storage lookup includes schema once backend filter support is enabled.
- APIDataNode physical identity constructor resolves schema plus table.
- Table-name-only API produces the planned compatibility warning or error.

Backend tests:

- Existing rows backfill/read as the data source's effective default schema.
- Legacy PostgreSQL rows without schema backfill/read as `public`.
- Register without schema still creates the data source's effective
  `physical_schema`.
- Register with `table_contract.physical.schema` stores that schema.
- Register rejects mismatched top-level and contract schema.
- Filter by `physical_schema__in` and `physical_table_name__in` works.
- Duplicate table names in different schemas are allowed.
- Duplicate `(data_source_uid, physical_schema, physical_table_name)` is rejected.
- Finalize-managed introspects by schema plus table.
- FK payloads include target physical schema.

## Release Notes

Document:

- MetaTable physical identity is now
  `(data_source_uid, physical_schema, physical_table_name)`.
- `physical_table_name` remains unqualified.
- Missing schema resolves through the backend data source default. `public` is
  only the legacy Postgres/Alembic compatibility default.
- Table-name-only lookup is deprecated because it is ambiguous across schemas.
- SDK and backend versions must be coordinated for strict schema-aware lookup.
