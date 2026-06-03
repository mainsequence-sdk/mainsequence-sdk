# ADR 0018: Platform-Managed MetaTable Runtime Physical Binding

## Status

Superseded by ADR 0022 for Alembic-managed platform MetaTables.

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
`PlatformTimeIndexMetaData.register()` will:

1. Build a registration request using the SQLAlchemy table name as
   `storage_hash`.
2. Omit `table_contract.physical.table_name` from platform-managed payloads.
3. Receive the backend `MetaTable` response.
4. Privately bind the model with `_bind_meta_table(...)`.
5. Retarget the SQLAlchemy `Table.name` to the backend `physical_table_name`.
6. Preserve the logical `storage_hash` separately on the model.

ADR 0022 replaces steps 2 and 5 for Alembic-managed models: the request includes
the authored physical table name, and `_bind_meta_table(...)` binds MetaTable
UID/storage metadata without mutating SQLAlchemy `Table.name`.

The binding method is intentionally private. User code should not bind arbitrary
MetaTable objects into SQLAlchemy models; doing so bypasses the register path and
can make a model compile SQL against a table it did not register.

The old free-function platform-managed registration helper is removed from the
public SDK export surface. Platform-managed SQLAlchemy models must register
through `PlatformManagedMetaTable.register()` or
`PlatformTimeIndexMetaData.register()` so the backend response can be bound
back onto the declaring model.

## Consequences

After ADR 0022, ordinary SQLAlchemy statements built from `Model.__table__`
compile against the authored table name. SDK code that needs logical storage
identity must call `Model.get_storage_hash()`, not read `Model.__table__.name`.

Foreign-key auto-resolution for platform-managed registration resolves target
tables by logical `storage_hash`. A fallback lookup by `physical_table_name` is
kept for compatibility with already-bound or older client objects.

External-registered tables keep the old behavior: their SQLAlchemy table name is
their physical table name and the contract includes `physical.table_name`.

## Implementation Tasks

- Make `bind_meta_table` private as `_bind_meta_table`.
- Remove the public free-function platform-managed registration helper.
- Store `__metatable_storage_hash__` and `__metatable_physical_table_name__`
  during binding.
- Retarget SQLAlchemy `Table.name`, `Table.fullname`, and metadata registry keys
  during binding.
- Preserve a logical fullname on the table for SDK storage-hash and FK contract
  generation after the SQLAlchemy runtime name has been rebound.
- Keep `get_storage_hash()` returning logical storage hash after binding.
- Add `get_physical_table_name()` for explicit physical-name access.
- Omit `physical.table_name` from platform-managed registration contracts.
- Resolve platform-managed FK targets by `storage_hash`, with physical-name
  fallback for compatibility.
- Cover registration rebinding with SQLAlchemy compile tests.
