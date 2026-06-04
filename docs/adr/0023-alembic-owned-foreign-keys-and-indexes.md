# ADR 0023: Alembic-Owned Foreign Keys And Indexes

Date: 2026-06-04

Status: Accepted

Supersedes: ADR 0019 foreign-key wrapper policy and the FK/index reservation
contract portions of ADR 0022.

## Context

Platform-managed MetaTables now use Alembic as the schema migration authority.
The SDK reserves MetaTable rows and obtains a provider migration connection, but
Alembic reads SQLAlchemy metadata, renders revisions, applies DDL, and records
the version table state.

Under that architecture, foreign keys and indexes are database DDL objects. They
belong to SQLAlchemy/Alembic and the database, not to the MetaTable registry
contract.

The SDK still carries legacy machinery from the previous registry-created-table
design:

- `MetaTableForeignKey(TargetModel, column=...)` wraps `sqlalchemy.ForeignKey`
  and stores SDK metadata on the SQLAlchemy FK object.
- registration code can inspect FK metadata, recursively register FK targets,
  resolve target `MetaTable.uid`, and serialize FK contracts.
- table-contract builders can serialize SQLAlchemy indexes into
  `MetaTableIndexContract`.
- table-contract builders can serialize SQLAlchemy FKs into
  `MetaTableForeignKeyContract`.
- docs and examples teach `MetaTableForeignKey(...)` as the public FK API.

That is now the wrong mental model. It makes assistants and users believe the
SDK owns FK/index resolution, naming, and registry contracts. It also creates
maintenance risk because Alembic autogenerate compares real database DDL against
SQLAlchemy metadata, while the SDK has a second, partial representation of the
same objects.

## Decision

The SDK must not own foreign-key or index migration semantics.

For Alembic-managed platform MetaTables:

1. SQLAlchemy metadata is the only FK/index declaration surface.
2. Alembic is the only FK/index migration generator and executor.
3. The database is the authority for physical FK/index names unless the project
   explicitly authored names in SQLAlchemy.
4. MetaTable reservation, registration, finalization, and migration-scoped
   connection setup must not resolve, serialize, rename, compare, or generate
   FK/index contracts.
5. MetaTable storage identity must include storage-shape semantics from columns,
   primary keys, indexes, and foreign keys. This is a local opaque fingerprint
   input only. It must not reintroduce backend FK/index contracts or SDK
   FK/index name ownership.
6. Project models should use ordinary SQLAlchemy declarations:

```python
from sqlalchemy import ForeignKey, Index


class Account(Base):
    __tablename__ = "ms_markets__account"


class Position(Base):
    __tablename__ = "ms_markets__position"

    account_uid: Mapped[uuid.UUID] = mapped_column(
        Uuid,
        ForeignKey("ms_markets__account.uid", ondelete="RESTRICT"),
        nullable=False,
    )

    __table_args__ = (
        Index("ms_markets__position_account_uid_idx", "account_uid"),
    )
```

Project-authored table names should be project-prefixed, for example
`ms_markets__account`, to avoid collisions in shared schemas.

## Deprecation And Removal

`MetaTableForeignKey` is deprecated for Alembic-managed platform MetaTables and
must be removed from the normal SDK surface. This is not a compatibility branch
that should remain in docs or examples.

The following legacy code must be removed:

- `MetaTableForeignKey`
- `_METATABLE_FOREIGN_KEY_INFO_KEY`
- `_resolve_metatable_foreign_key_target_column`
- `PlatformManagedMetaTable.resolve_foreign_key_targets(...)`
- `_register_metatable_foreign_key_targets(...)`
- `_metatable_foreign_key_target_models(...)`
- `_metatable_foreign_key_metadata(...)`
- `_metatable_foreign_key_contract_target(...)`
- `_foreign_key_contract(...)`
- `_time_indexed_meta_table_foreign_key_contract(...)`
- direct FK target UID/identifier resolution for platform-managed table
  contracts
- recursive target registration driven by FK declarations
- tests asserting FK contract serialization or recursive FK target registration
- docs, tutorials, examples, skills, and ADR text that present
  `MetaTableForeignKey(...)` as the normal FK API

The following legacy contract fields/models must be removed from the migration
path and from the SDK contract surface. If backend request/response schemas
still require any of them, that is a backend alignment task, not a reason to
preserve the SDK-side legacy model:

- `MetaTableIndexContract`
- `MetaTableForeignKeyContract`
- `MetaTableContract.indexes`
- `MetaTableContract.foreign_keys`
- `_index_contract(...)`
- `_default_time_indexed_meta_table_indexes(...)` as a table-contract generator

The SDK must keep normalized FK/index semantics in storage identity because
those objects affect the physical table shape. That hash input must be derived
from SQLAlchemy metadata and must ignore generated FK/index names. The backend
may store the resulting hash as an opaque value, but it must not be asked to
understand or reconcile FK/index structure.

Temporary guard flags such as `include_indexes=False` or
`include_foreign_keys=False` are acceptable only as an intermediate step while
removing the legacy code. The target state is no SDK FK/index contract path, not
a permanent compatibility switch.

## Required Migration Behavior

`AlembicMetaTableMigration.prepare_for_alembic()` must:

- reserve provider models in the provider-declared order, without topological
  sorting through FK targets;
- use table names only for MetaTable reservation identity;
- build reservation table contracts without indexes;
- build reservation table contracts without foreign keys;
- build migration storage identity with normalized index and FK semantics;
- keep generated FK/index names out of storage identity so Alembic/database
  naming does not cause hash churn;
- never call FK target registration or target UID resolution;
- leave SQLAlchemy metadata intact so Alembic can still see indexes and FKs.

`AlembicMetaTableMigration.sync_metatable_catalog()` must follow the same
contract boundary. It may refresh platform catalog metadata after Alembic, but
it must not become an FK/index schema-management path.

## Consequences

The SDK migration layer becomes smaller and less ambiguous:

- FK/index changes appear only as Alembic revision changes.
- FK/index changes also change the SDK storage hash through normalized local
  semantics, without creating a second FK/index contract language.
- Alembic autogenerate no longer competes with SDK FK/index contract metadata.
- backend MetaTable reservations only coordinate table identity and migration
  scope, not physical relational constraints.
- users rely on standard SQLAlchemy declarations, which are the same
  declarations Alembic already understands.

The cost is that projects using `MetaTableForeignKey(...)` must migrate to
ordinary SQLAlchemy `ForeignKey` or `ForeignKeyConstraint`. That is intentional.
Keeping both APIs would preserve the broken architecture.

## Implementation Tasks

- [x] Remove FK/index serialization from Alembic migration reservation and
  catalog-sync paths.
- [x] Keep normalized FK/index semantics in migration storage identity without
  serializing FK/index contracts or generated names.
- [x] Remove FK target discovery from migration provider preparation.
- [x] Remove recursive FK target registration from platform-managed
  registration.
- [x] Remove `MetaTableForeignKey` and its helper metadata key.
- [x] Remove FK contract extraction helpers.
- [x] Remove index contract extraction helpers.
- [x] Remove FK/index contract model usage from platform-managed MetaTable
  request builders and client request/response models.
- [x] Remove temporary `include_indexes` / `include_foreign_keys` compatibility
  switches after the legacy contract path is deleted.
- [x] Remove or supersede tests that assert FK/index SDK contract generation.
- [x] Rewrite examples to use SQLAlchemy `ForeignKey` / `ForeignKeyConstraint`
  and `Index`.
- [x] Rewrite tutorials, knowledge docs, and skills so they do not mention
  `MetaTableForeignKey(...)` as a valid current pattern.
- [x] Mark ADR 0019 as superseded by this ADR.
- [x] Update ADR 0022 language so FK/index reservation contracts are not
  described as part of the thin Alembic path.

## Non-Goals

This ADR does not remove SQLAlchemy foreign keys or indexes from user models.
It removes the SDK's duplicate representation and resolution of those objects.

This ADR does not ask the backend to manage FK or index names. Alembic and the
database remain the owners of physical FK/index DDL.
