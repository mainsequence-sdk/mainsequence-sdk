# ADR 0018: Platform-Managed MetaTable Runtime Physical Binding

## Status

Superseded by ADR 0022 for Alembic-managed platform MetaTables and by ADR 0028
for storage-hash identity.

## Context

Platform-managed MetaTables now have two different table identifiers:

- `storage_hash`: the client-side logical storage identity. It is stable across
  registrations and is used to find an existing MetaTable.
- `physical_table_name`: the backend-owned physical table name. For newly
  generated backend tables this is `mt_<meta_table_uid_hex>`.

The SDK previously used the SQLAlchemy declarative table name for both roles,
then retargeted SQLAlchemy metadata to a backend-owned physical table name after
registration. ADR 0022 removes that runtime rebinding for Alembic-managed
platform MetaTables: authored SQLAlchemy `__tablename__` remains the physical
table name Alembic sees, while storage identity is tracked separately.

## Decision

This decision is historical and has been superseded for the migration-first
path. Registration is no longer allowed to retarget SQLAlchemy table names for
Alembic-managed platform MetaTables.

`PlatformManagedMetaTable.register()` and
`PlatformTimeIndexMetaTable.register()` will:

1. Build a registration request using the SQLAlchemy table name as
   `storage_hash`.
2. Omit `table_contract.physical.table_name` from platform-managed payloads.
3. Receive the backend `MetaTable` response.
4. Privately bind the model with `_bind_meta_table(...)`.
5. Retarget the SQLAlchemy `Table.name` to the backend `physical_table_name`.
6. Preserve the logical `storage_hash` separately on the model.

ADR 0022 replaces steps 2 and 5 for Alembic-managed models: the request includes
the authored physical table name, and `_bind_meta_table(...)` binds MetaTable
UID and data-source metadata without mutating SQLAlchemy `Table.name`. ADR 0028
removes `storage_hash` as a first-class MetaTable identity.

The binding method is intentionally private. User code should not bind arbitrary
MetaTable objects into SQLAlchemy models; doing so bypasses the register path and
can make a model compile SQL against a table it did not register.

The old free-function platform-managed registration helper is removed from the
public SDK export surface. Platform-managed SQLAlchemy models must register
through `PlatformManagedMetaTable.register()` or
`PlatformTimeIndexMetaTable.register()` so the backend response can be bound
back onto the declaring model.

## Consequences

After ADR 0028, SDK code that needs a deterministic contract fingerprint must
call `compute_metatable_contract_hash(Model)`. `Model.__table__.name` remains
the authored physical table name.

Foreign-key metadata should use backend MetaTable UID when available and the
authored target physical table name for SQL/Alembic binding. Hashing is only an
explicit contract-fingerprint utility after ADR 0028.

External-registered tables keep the old behavior: their SQLAlchemy table name is
their physical table name and the contract includes `physical.table_name`.

## Implementation Tasks

- Make `bind_meta_table` private as `_bind_meta_table`.
- Remove the public free-function platform-managed registration helper.
- Store MetaTable UID, data-source UID, and physical table name during binding.
- Preserve the authored SQLAlchemy `Table.name`; do not retarget SQLAlchemy
  metadata during binding for Alembic-managed tables.
- Remove the mixin storage-hash convenience method; callers that need a
  fingerprint use `compute_metatable_contract_hash(Model)` explicitly.
- Add `get_physical_table_name()` for explicit physical-name access.
- Include `physical.table_name` in platform-managed registration contracts.
- Resolve platform-managed FK targets by MetaTable UID when bound and by
  authored physical table name for SQL/Alembic binding.
- Cover registration rebinding with SQLAlchemy compile tests.
