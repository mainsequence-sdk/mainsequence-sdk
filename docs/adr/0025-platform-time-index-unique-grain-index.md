# ADR 0025: Platform Time-Index Unique Grain Index

Date: 2026-06-04

Status: Implemented

Depends on: ADR 0023, "Alembic-Owned Foreign Keys And Indexes".

## Context

`PlatformTimeIndexMetaTable` models declare the DataNode storage grain with:

```python
__time_index_name__ = "time_index"
__index_names__ = ["time_index", "asset_identifier"]
```

That tuple is currently used by the SDK/backend as time-indexed storage
metadata. It identifies the logical observation point for update progress,
DataFrame validation, reads, deletes, and upserts.

That is not enough. If the database does not enforce uniqueness for the same
tuple, the storage contract says a row is identified by
`(time_index, asset_identifier)` while the physical table can still contain
duplicate observations for that exact key. That makes `__index_names__`
semantically incomplete.

ADR 0023 says the SDK must not own generic FK/index migration semantics and must
not serialize FK/index contracts to the backend. This ADR does not change that
boundary. The missing piece is that the DataNode grain declaration should
materialize as normal SQLAlchemy metadata before Alembic autogenerate runs.
Alembic still renders and applies the index DDL.

## Decision

`PlatformTimeIndexMetaTable` must automatically add a unique SQLAlchemy index over
its declared full time-index grain.

For:

```python
__time_index_name__ = "time_index"
__index_names__ = ["time_index", "asset_identifier"]
```

the SQLAlchemy table metadata must contain the equivalent of:

```python
Index(None, "time_index", "asset_identifier", unique=True)
```

For higher-dimensional storage:

```python
__index_names__ = ["time_index", "account_uid", "asset_identifier"]
```

the generated unique index is:

```text
UNIQUE (time_index, account_uid, asset_identifier)
```

The SDK must generate this as SQLAlchemy metadata only. It must not send an
index contract to TS Manager, must not ask the backend to create the index, and
must not reconcile index state outside Alembic.

## Naming

The generated index must use the existing SDK schema naming convention.

The SDK creates the index with an explicit name from
`schema_index_name(..., unique=True)`:

```python
Index(
    schema_index_name(table.name, ["time_index", "asset_identifier"], unique=True),
    table.c.time_index,
    table.c.asset_identifier,
    unique=True,
)
```

Using the same helper as `sqlalchemy_naming_convention()` yields:

```text
uix__{table_name}__{time_index_name}__{dimension_1}__{dimension_2}
```

Long names are bounded to PostgreSQL's identifier limit with the existing stable
hash suffix behavior.

Example:

```text
table:
ms_markets__assetpricingdetailsts__mainsequence_examples

columns:
time_index, asset_identifier

index:
uix__ms_markets__assetpricingdetailsts__mainsequence_3f465e3beb
```

Projects can opt out of the generated `uix__...` name by declaring an
equivalent unique index or unique constraint themselves. The SDK detects that
existing uniqueness enforcement, including the same columns in a different
order, and does not add a duplicate generated index.

## Required Behavior

`PlatformTimeIndexMetaTable` table construction must:

1. resolve `__time_index_name__`;
2. resolve `__index_names__`;
3. require `__index_names__` to include `__time_index_name__` exactly once;
4. require `__index_names__` to start with `__time_index_name__`;
5. require every `__index_names__` entry to exist as a SQLAlchemy table column;
6. require every `__index_names__` column to be non-nullable;
7. add a unique SQLAlchemy `Index` over the full ordered tuple when no matching
   unique index or unique constraint already exists;
8. leave user-authored indexes and constraints intact;
9. expose the generated index to Alembic autogenerate like any other SQLAlchemy
   DDL object.

The generated unique grain index is the physical enforcement of the DataNode
storage identity. Additional non-unique performance indexes remain ordinary
user-authored SQLAlchemy metadata.

## Required Migration Behavior

Existing applied Alembic revision files must not be edited. They are database
history.

After this ADR is implemented, projects should create a new Alembic revision.
Autogenerate should emit missing unique grain indexes for existing
`PlatformTimeIndexMetaTable` tables that do not already have an equivalent unique
index or unique constraint.

The SDK migration preparation and finalization path must continue to follow ADR
0023:

- no backend index contract serialization;
- no backend index-name reconciliation;
- no SDK migration-time index resolution;
- no special finalize behavior for indexes.

## Implementation Tasks

- [x] Add SQLAlchemy metadata generation for the unique grain index on
  `PlatformTimeIndexMetaTable`.
- [x] Reuse `schema_index_name(..., unique=True)` so generated names use the
  `uix__...` convention.
- [x] Detect an existing equivalent unique `Index` or `UniqueConstraint` and
  avoid adding a duplicate generated index.
- [x] Keep strict validation for missing, duplicated, nullable, or misordered
  grain columns.
- [x] Add tests proving `Base.metadata` contains the generated unique index.
- [x] Add Alembic/autogenerate-facing tests proving the generated index is
  visible as normal SQLAlchemy metadata.
- [x] Update DataNode docs, tutorial, examples, and skills so users do not
  manually repeat the full `__index_names__` unique index.
- [x] Preserve guidance that additional performance indexes are ordinary
  SQLAlchemy/Alembic declarations.

## Consequences

`__index_names__` becomes a complete storage-grain declaration: it drives
DataNode semantics and creates the database uniqueness metadata needed to enforce
those semantics.

This reduces repeated user boilerplate and prevents duplicate time/dimension
rows by default.

The tradeoff is that adding or changing `__index_names__` now changes physical
DDL and should appear in Alembic revisions. That is correct: the DataNode grain
is part of the physical storage contract.

## Non-Goals

This ADR does not reintroduce backend-managed indexes.

This ADR does not reintroduce SDK index contracts in MetaTable reservation or
finalization payloads.

This ADR does not remove user-authored SQLAlchemy indexes.

This ADR does not edit already-applied migrations. Existing schemas must be
updated with new Alembic revisions.
