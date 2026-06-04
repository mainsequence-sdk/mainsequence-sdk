# ADR 0022: Thin SDK Alembic-Owned MetaTable Migrations

Date: 2026-06-02

Status: Implemented

Superseded-in-part note: ADR 0023 supersedes the FK/index reservation-contract
policy in this ADR. Foreign keys and indexes are Alembic-owned DDL and must not
be resolved or serialized by the SDK migration path.

## Context

ADR 0020 and ADR 0021 moved MetaTable migrations toward Alembic, but the SDK
still contains too much migration machinery:

- `mainsequence.client.metatables.migrations` exposes `AlembicMigrationOperation`
  and binds `MetaTable.apply_migration(...)` / `MetaTable.get_migration_status(...)`
  at import time.
- `mainsequence.meta_tables.migrations` renders packaged Alembic SQL artifacts
  for a backend apply endpoint.
- `AlembicMetaTableMigration.resolve_or_register_metatable_model(...)` creates
  missing platform-managed tables through `model.register()`.
- Foreign keys and indexes are Alembic-owned DDL. ADR 0023 removes SDK
  FK/index contract resolution from the migration path. The project-authored
  SQLAlchemy table names are the physical table names Alembic targets.

This keeps the SDK in the wrong position. Alembic should own revision
generation, database connection configuration, current revision discovery,
upgrade/downgrade execution, and version-table writes. MetaTables should remain
the platform registry/catalog layer.

The backend now supports the missing coordination primitives:

- `POST /orm/api/ts_manager/meta_table/reserve-managed/`
  reserves or resolves platform-managed MetaTable rows without creating physical
  tables, and returns TS Manager-owned physical table names.
- `POST /orm/api/ts_manager/dynamic_table_data_source/<uid>/migration-connection/`
  issues a short-lived, user-scoped migration credential for the reserved table
  scope. It does not expose the stored data-source owner credential.

With those primitives, the SDK no longer needs to package SQL and ask TS Manager
to execute Alembic output.

## Decision

The SDK migration path must become a thin Alembic adapter:

```text
SDK provider -> reserve MetaTables -> bind SQLAlchemy metadata
             -> ask backend for scoped migration URI -> invoke Alembic normally
             -> finalize reserved MetaTables after successful upgrade
```

The SDK must not be the migration executor. It should only:

1. discover/load the project migration provider;
2. register or resolve the provider's `AlembicVersionMetaTable`;
3. reserve/resolve provider-scoped platform-managed MetaTables;
4. bind returned MetaTable UID/storage metadata while preserving authored
   SQLAlchemy table names before Alembic runs;
5. build an Alembic `Config` with the backend-issued migration URI;
6. invoke Alembic command APIs for revision/current/upgrade/downgrade;
7. call the backend batch finalization endpoint after successful Alembic
   execution so reserved MetaTables become active only after their physical
   tables exist.

## Preserved APIs

Keep `AlembicVersionMetaTable`.

It remains the SDK class that registers a catalog pointer to Alembic's version
table. It is not a migration ledger and it does not validate application table
contracts. It exists so the platform can discover and display the Alembic
version table through the MetaTable registry. Migration commands must force an
idempotent backend registration for this pointer before issuing migration
credentials; the SDK's in-memory bound `AlembicVersionMetaTable` object is only
a cache and must not be treated as proof that the backend catalog row exists.

Keep `AlembicMetaTableMigration`, but narrow its responsibility.

It should represent a provider-scoped migration configuration:

```python
migration = AlembicMetaTableMigration(
    package="msm",
    migration_namespace="mainsequence.examples",
    script_location="msm:migrations",
    target_metadata=Base.metadata,
    alembic_registry=MSMAlembicVersion,
    metatable_models=[AssetTable, AssetSnapshotsStorage],
    after_register_metatables=refresh_catalog,
)
```

The provider should expose enough information to prepare Alembic. It should not
produce SQL artifacts for a backend apply endpoint.

Keep `PlatformManagedMetaTable` and `PlatformTimeIndexMetaData`.

They remain the SQLAlchemy authoring/registry surfaces for platform-managed
tables. Their runtime role is to build registry contracts and bind existing or
reserved MetaTable rows to SQLAlchemy table metadata.

## New SDK Client Surface

Add `DynamicTableDataSource.issue_migration_connection(...)`.

This method calls:

```text
POST /orm/api/ts_manager/dynamic_table_data_source/<uid>/migration-connection/
```

with a provider/data-source-scoped request:

```python
class DynamicTableDataSourceMigrationConnectionRequest(BasePydanticModel):
    purpose: Literal["schema_migration"] = "schema_migration"
    package: str = ""
    migration_namespace: str = ""
    ttl_seconds: int = 900
```

and returns a model with at least:

```python
class DynamicTableDataSourceMigrationConnection(BasePydanticModel):
    ok: bool
    data_source_uid: str
    dialect: str
    credential_kind: str
    role_name: str
    owner_role_name: str | None = None
    expires_at: datetime
    uri: str
```

The migration code must use this endpoint instead of reconstructing connection
URIs from foundry data source fields or requesting the stored data-source owner
URI. The returned URI is a temporary credential. The SDK must avoid logging it.

Add `MetaTable.reserve_managed(...)`.

This method calls:

```text
POST /orm/api/ts_manager/meta_table/reserve-managed/
```

and uses request/response models equivalent to:

```python
class MetaTableRequestFields(BasePydanticModel):
    identifier: str | None = Field(None, description="Migration table identity; use authored SQLAlchemy Table.name for Alembic-managed tables.")
    namespace: str | None = Field(None, description="Optional MetaTable namespace.")
    data_source_uid: str = Field(..., description="DynamicTableDataSource UID that owns this MetaTable.")
    storage_hash: str = Field(..., max_length=63, description="Canonical table storage hash.")
    description: str | None = Field(None, description="Optional MetaTable description.")
    labels: list[str] = Field(default_factory=list, description="Labels to apply to the MetaTable.")
    protect_from_deletion: bool = Field(False, description="Whether to protect the MetaTable from deletion.")
    table_contract: MetaTableContract | dict[str, Any] = Field(..., description="Relational table contract.")


class MetaTableRegistrationRequest(MetaTableRequestFields):
    management_mode: MetaTableManagementMode
    provisioning: dict[str, Any] | None = None
    introspect: bool = False


class ManagedMetaTableReservationTable(MetaTableRequestFields):
    physical_table_name: str | None = Field(None, description="Authored SQLAlchemy table name Alembic will render against.")
    time_index_name: str | None = Field(None, description="Optional time-index column name.")
    partition_strategy: str | None = Field(None, description="Optional time-index partition strategy.")


class ManagedMetaTableReservationRequest(BasePydanticModel):
    tables: list[ManagedMetaTableReservationTable] = Field(..., description="Tables to reserve.")


class ManagedMetaTableReservationItem(BasePydanticModel):
    identifier: str | None = Field(None, description="Reserved migration table identity.")
    namespace: str | None = Field(None, description="Resolved namespace when returned.")
    meta_table_uid: str = Field(..., description="Reserved backend MetaTable UID.")
    data_source_uid: str = Field(..., description="DynamicTableDataSource UID that owns the reservation.")
    management_mode: Literal["platform_managed"] = Field(..., description="Backend-confirmed management mode.")
    provisioning_status: Literal["reserved", "active"] = Field(..., description="First-class backend provisioning state.")
    storage_hash: str = Field(..., description="Reserved storage hash.")
    physical_table_name: str = Field(..., description="Backend-confirmed authored physical table name for Alembic rendering.")
    table_contract: dict[str, Any] = Field(..., description="Backend-normalized contract with resolved names.")
    created: bool = Field(..., description="Whether the backend created a new reservation row.")
    matched_by: str | None = Field(None, description="Existing-row match strategy, or null for newly created rows.")


class ManagedMetaTableReservationResponse(BasePydanticModel):
    ok: bool = Field(..., description="Whether TS Manager accepted the reservation request.")
    version: str | None = Field(None, description="Opaque TS Manager-owned reservation response schema version, when returned.")
    tables: list[ManagedMetaTableReservationItem] = Field(..., description="Reserved MetaTable name plans.")
```

## Reservation Binding

Add a provider method equivalent to:

```python
migration.prepare_for_alembic()
```

The method must:

1. resolve the provider's data source from the provider models or session;
2. build reservation table payloads from `metatable_models`;
3. omit SQLAlchemy index/FK contracts from reservation payloads;
4. leave SQLAlchemy metadata intact so Alembic can render index/FK DDL;
5. use each provider model's authored SQLAlchemy `Table.name` as the
   reservation `identifier`;
6. include the authored SQLAlchemy table name in `physical_table_name` so
   Alembic renders against the project-owned table name;
7. never resolve FK targets or emit same-batch FK target identifiers;
8. call `MetaTable.reserve_managed(...)` once with all pending reservation
   tables, not once per model;
9. bind each returned item back to its model by response order;
10. preserve SQLAlchemy `Table.name` exactly as authored;
11. leave SQLAlchemy FK/index names as authored by project metadata;
12. bind `__metatable_uid__`, `__metatable_data_source_uid__`,
   `__metatable_storage_hash__`, and `__metatable_physical_table_name__`.

After this step, Alembic autogenerate and Alembic upgrade must see the authored
SQLAlchemy physical table names. Index names remain normal Alembic/SQLAlchemy
DDL metadata. Platform-managed FK names are physical database details and must
not be carried in MetaTable contracts or copied from backend reservation
responses.

The provider must also expose the prepared migration scope:

```python
prepared.meta_table_uids
prepared.data_source_uid
prepared.owner_role_name
```

The SDK does not send `prepared.meta_table_uids` to the migration-connection
endpoint. MetaTable UIDs remain part of the prepared/finalization state, but the
temporary database credential request is scoped by provider data source and
migration provider identity.

## CLI Lifecycle

The intended CLI flow becomes:

```bash
mainsequence migrations revision --provider msm.migrations:migration -m "add field"
mainsequence migrations current --provider msm.migrations:migration
mainsequence migrations upgrade --provider msm.migrations:migration head
```

`revision` must:

1. load the provider;
2. build a local Alembic `Config` from provider `script_location` and
   `target_metadata`;
3. use an explicit `--sqlalchemy-url` when supplied, otherwise request a
   provider migration connection without MetaTable UIDs;
4. call Alembic `revision(...)` / autogenerate against local provider metadata.

`revision` is local Alembic authoring. It must not register, reserve, finalize,
or otherwise mutate provider MetaTables. Provider application MetaTable
reservation is an apply-time concern, not a revision-file generation concern.

`current` must:

1. load the provider;
2. register/resolve `AlembicVersionMetaTable`;
3. issue a provider migration connection without MetaTable UIDs;
4. call Alembic current/head APIs directly.

`current` is read-only for provider application MetaTables. It must not call
`reserve-managed` for the provider model list because checking Alembic version
state should not mutate or restage application catalog rows.

`upgrade` must:

1. load the provider;
2. register/resolve `AlembicVersionMetaTable`;
3. reserve and bind platform-managed MetaTables;
4. issue a provider migration connection without MetaTable UIDs;
5. call Alembic `upgrade(...)` directly;
6. after success, call `POST /orm/api/ts_manager/meta_table/finalize-managed/`
   once with the provider MetaTable UIDs;
7. call `after_register_metatables` with an
   `AlembicMetaTableCatalogRefreshContext` containing the finalized rows and
   `reserved_policy="reconcile"` for post-Alembic catalog writes.

There should not be separate normal-user commands for "render SQL" and "apply
SQL to backend". Alembic is the apply path.

## Final Catalog Finalization

After Alembic succeeds, the SDK must finalize the reserved MetaTable rows. This
is not registration and must not send labels, storage hashes, full contracts, or
`provisioning.create_table=false` payloads.

```python
response = migration.finalize_metatable_catalog(prepared=prepared)
```

The SDK calls the backend once:

```text
POST /orm/api/ts_manager/meta_table/finalize-managed/
```

The backend introspects the reserved physical names created by Alembic and flips
each row from `reserved` to `active`. If any table is still missing or invalid,
the SDK raises `AlembicProviderPhysicalStateError` and does not run
`after_register_metatables`.

## Explicit Name Policy

Projects own physical table names through authored SQLAlchemy `Table.name`
values:

- explicit table names are passed through as requested physical names;
- FK/index contracts must stay out of reservation payloads;
- omitted or explicit FK/index names are SQLAlchemy/Alembic metadata only;
- backend reservation responses must not be used to fill, rename, compare, or
  normalize FK/index metadata.

The SDK must not clear, map, or synthesize FK/index names in SQLAlchemy
metadata. Alembic sees the project-authored metadata and owns the resulting DDL.

## Deprecated SDK Surface

Deprecate the backend SQL artifact client path:

- `AlembicMigrationOperation`
- `AlembicMigrationApplyResponse`
- `AlembicMigrationStatusRequest`
- `AlembicMigrationStatusResponse`
- `MetaTable.apply_migration(...)`
- `MetaTable.get_migration_status(...)`
- `PackagedAlembicMigrationArtifact`
- `render_packaged_alembic_migration(...)`
- `render_packaged_alembic_migration_for_provider(...)`

These APIs describe the previous design where the SDK rendered Alembic SQL and
TS Manager executed it. They should not be used by the new CLI migration path.

If retained temporarily, they must be documented as legacy and kept out of the
normal tutorial workflow.

## Implementation Tasks

- [x] Add `DynamicTableDataSourceMigrationConnectionRequest`,
  `DynamicTableDataSourceMigrationConnection`, and
  `DynamicTableDataSource.issue_migration_connection(...)`.
- [x] Do not include MetaTable UIDs in migration-connection requests. Keep
  MetaTable UIDs in prepared/finalization state only.
- [x] Treat the returned migration URI as a secret and avoid printing or logging
  it in CLI output.
- [x] Add managed MetaTable reservation request/response models.
- [x] Add `MetaTable.reserve_managed(...)`.
- [x] Add provider-level reservation preparation that binds MetaTable
  UID/storage metadata while preserving authored SQLAlchemy table names.
- [x] Batch provider MetaTable reservations into one backend request without
  FK/index target resolution.
- [x] Change `AlembicMetaTableMigration` so missing provider models are reserved,
  not registered/created, before Alembic runs.
- [x] Add an Alembic config helper that uses the backend-issued scoped
  migration URI and the provider's version table settings.
- [x] Configure Alembic to `SET ROLE` to the backend-provided migration owner
  role when `owner_role_name` is present.
- [x] Update migration CLI commands to call Alembic directly instead of backend
  SQL artifact apply endpoints.
- [x] Finalize reserved provider MetaTables after successful Alembic
  upgrade/downgrade through the backend batch finalize endpoint.
- [x] Add typed SDK request/response models for schema-management,
  finalize-managed, and Alembic provider reset.
- [x] Add `mainsequence migrations reset --confirm-reset` for explicit
  provider-scoped repair workflows.
- [x] Apply ADR 0023 cleanup: remove SDK FK/index contract generation from
  migration resolution and remove the legacy `MetaTableForeignKey(...)` surface.
- [x] Deprecate or remove import-time binding of `MetaTable.apply_migration` and
  `MetaTable.get_migration_status`.
- [x] Remove normal-user documentation that asks users to hand-build or apply
  `AlembicMigrationOperation`.
- [x] Remove previous migration objects/models from the public migration path,
  including backend SQL artifact request/response models and package-rendered
  artifact helpers.
- [x] Rewrite examples, tutorials, knowledge docs, and skills so the normal
  workflow is the thin Alembic-owned path and no longer references the previous
  backend SQL artifact migration model.

## Consequences

The SDK becomes thinner and easier to maintain. Alembic handles migration
semantics, revision graph state, transactional DDL behavior, autogenerate, and
version-table updates. TS Manager remains the authority for MetaTable registry
identity and canonical platform names.

The important constraint is ordering: reservation and metadata binding must
happen before Alembic autogenerate or upgrade. If Alembic sees pre-reservation
logical/hash names, it will generate SQL for the wrong physical tables.

## Required Cleanup

Previous migration objects and models must be removed from the normal SDK
surface, not merely hidden behind new documentation. The backend SQL artifact
path belongs to the old architecture and should not remain as a parallel
concept that assistants or users can accidentally select.

Examples, tutorials, knowledge docs, and skills must be rewritten around the
thin Alembic-owned path. They must not instruct users to create
`AlembicMigrationOperation`, render packaged SQL artifacts, call
`MetaTable.apply_migration(...)`, or use the backend apply endpoint as the
standard migration workflow.
