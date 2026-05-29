# ADR 0014: DataNode Records and MetaTable Foreign Keys

Date: 2026-05-26

Status: Superseded

Superseded by ADR 0017: MetaTable Canonical Storage.

## Decision

Foreign-key authoring belongs to the MetaTable contract in the SDK.

DataNode configuration must not define or resolve foreign keys. The SDK should
build storage schema through the platform-managed MetaTable model, where columns,
indexes, and foreign keys are one table contract. Dynamic-table and DataNode
runtime models may reference the storage MetaTable, but they must not expose a
separate foreign-key authoring model or projection.

Current SDK ownership is:

- `MetaTableContract.foreign_keys` carries request contract foreign keys.
- `MetaTable.foreign_keys` carries the returned MetaTable projection.
- SQLAlchemy-backed storage classes derive foreign keys into the MetaTable table
  contract.
- DataNode configuration carries update/runtime configuration, not storage
  foreign-key definitions.

## Consequences

The old DataNode-level foreign-key API is not part of the current SDK design.
There is no DataNode-specific foreign-key model, no time-indexed profile
foreign-key field, and no DynamicTable-specific SDK validation path. Re-register
and comparison behavior should use the MetaTable contract/projection as the
single source of truth.
