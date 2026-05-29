# ADR 0017: MetaTable Canonical Storage

Date: 2026-05-28

Status: Proposed

## Related ADRs

- ADR 0002: Multidimensional DataNode Update Contract
- ADR 0006: TDAG Public UID Identifiers
- ADR 0007: Client-Wide UID Public Identity
- ADR 0008: MetaTable CLI Surface
- ADR 0011: DuckDB N-Dimensional Row Identity
- ADR 0014: DataNode Records and Foreign Keys
- ADR 0015: APIDataNode UID Runtime Identity
- ADR 0016: DType Serialization and Parsing Contract

## Context

The SDK currently has two table-storage concepts that overlap:

- `MetaTable`, exposed by `mainsequence.client.models_metatables.MetaTable`,
  is the SDK model for row-oriented relational table storage, table contracts,
  permissions, labels, UID lookup, introspection, and governed SQL operations.
- `TimeIndexMetaData`, exposed by `mainsequence.client.models_metatables.TimeIndexMetaData`,
  is the SDK projection of server `DynamicTableMetaData`. It is the storage
  table for DataNode writes and reads. It carries `storage_hash`, data source,
  labels, permissions, metadata, source-table configuration, and data access
  actions.

The DataNode runtime also computes two identities:

- `storage_hash`, derived from DataNode construction/configuration, identifies
  the table that receives writes.
- `update_hash`, derived from DataNode construction/configuration, identifies
  the updater process writing to that table.

That split solved the original "many updater jobs write into one table" problem,
but it also means the DataNode class owns too much:

- it defines update logic;
- it computes table storage identity;
- it carries table metadata intent;
- it carries structural table records and foreign keys;
- it asks `PersistManager` to create storage;
- it then writes through a `DataNodeUpdate` backend object.

MetaTable has become the stronger storage abstraction. It already owns the
storage-facing concepts that should be canonical for all tables:

- stable `uid`;
- `storage_hash`;
- `data_source_uid`;
- `identifier` and `namespace`;
- physical table name;
- table contract with columns, indexes, and foreign keys;
- labels and sharing;
- introspection and contract validation;
- governed SQL execution.

The next architecture step is to make MetaTable the canonical storage model and
make DataNode a pure update-process abstraction.

## Current Client Architecture

The relevant client models are:

- `MetaTable` in `mainsequence/client/models_metatables.py`
- `TimeIndexMetaData` in `mainsequence/client/models_metatables.py`
- `DataNodeUpdate` in `mainsequence/client/models_metatables.py`
- `DataNode` runtime class in `mainsequence/meta_tables/data_nodes/data_nodes.py`
- `PersistManager` in `mainsequence/meta_tables/data_nodes/persist_managers.py`
- `build_operations.create_config(...)` in
  `mainsequence/meta_tables/data_nodes/build_operations.py`

The pre-migration write path was:

1. A concrete `DataNode` subclass is constructed with a `DataNodeConfiguration`.
2. `DataNode.__init_subclass__` captures constructor arguments and calls
   `build_operations.create_config(...)`.
3. `create_config(...)` returns both `update_hash` and `storage_hash`.
4. `DataNode.verify_and_build_remote_objects()` calls
   `PersistManager.local_persist_exist_set_config(...)`.
5. `PersistManager` creates or resolves `TimeIndexMetaData` with:

   ```python
   TimeIndexMetaData.get_or_create(
       storage_hash=storage_hash,
       data_source_uid=data_source.uid,
       build_configuration_json_schema=...,
       namespace=...,
   )
   ```

6. `PersistManager` creates or resolves `DataNodeUpdate` with:

   ```python
   DataNodeUpdate.get_or_create(
       update_hash=update_hash,
       data_source_uid=data_source.uid,
       remote_table__hash_id=storage_hash,
       build_configuration=local_configuration,
   )
   ```

7. `DataNodeUpdate.upsert_data_into_table(...)` initializes
   `SourceTableConfiguration` on `TimeIndexMetaData`, uploads data, and updates
   progress statistics.

In that old flow, the storage row was a side effect of constructing an updater.

## Problem

Storage identity should not be owned by DataNode update code.

The current design creates these issues:

- A user cannot cleanly say "this update process writes into this existing
  table" without letting the DataNode compute a `storage_hash`.
- Table schema and table metadata are split across
  `DataNodeConfiguration.records`, `DataNodeConfiguration.foreign_keys`,
  `DataNodeConfiguration.node_metadata`, `TimeIndexMetaData`, and
  `SourceTableConfiguration`.
- `TimeIndexMetaData` and `MetaTable` duplicate storage-facing fields, filters,
  labels, sharing, UID identity, and data-source identity.
- The name `DataNode` suggests a data product/storage object, but the runtime
  class is increasingly the update process.
- The existing client model name `DataNodeUpdate` is already used for the
  backend update record, so renaming the runtime class to `DataNodeUpdate`
  requires a deliberate compatibility plan.
- `TimeIndexMetaData`, DataNode update records, and shared data-source models
  now live with `MetaTable` in `models_metatables.py`.
- `models_foundry.py` keeps Project, Secret, Constant, Bucket, Artifact, and
  other non-MetaTable client models. It must not publicly re-export the moved
  MetaTable/update classes.

## Decision

Make `MetaTable` the canonical storage model.

`TimeIndexMetaData` becomes a DataNode-specific compatibility/storage projection
that inherits from the MetaTable client model:

```python
class TimeIndexMetaData(MetaTable):
    ENDPOINT = "ts_manager/dynamic_table"

    source_table_configuration: SourceTableConfiguration | None = None
    source_class_name: str | None = None  # compatibility only
```

`TimeIndexMetaData` remains available during migration because many public SDK,
CLI, tutorial, and backend paths still use that name. Its meaning changes:

- final canonical table fields come from `MetaTable`;
- DataNode-specific update/read extensions remain on `TimeIndexMetaData` only as
  compatibility or typed extension fields;
- new storage APIs should accept and return `MetaTable` wherever possible;
- old `TimeIndexMetaData` read/write helpers delegate to MetaTable-backed routes
  or continue to use legacy dynamic-table routes until the backend endpoint is
  migrated.

The DataNode runtime no longer computes the storage table identity and does not
resolve or register storage. It accepts the target platform-managed storage
model as a first-class constructor argument:

```python
class PricesTable(PlatformTimeIndexMetaData, Base):
    ...

# Done by storage/bootstrap code before the DataNode is constructed.
PricesTable.bind_meta_table(prepared_meta_table)

data_node = PricesUpdate(
    config=PricesUpdateConfiguration(...),
    storage_table=PricesTable,
)
```

This is the runtime direction in this migration. The runtime authoring class
remains `DataNode`; the immediate change is to pass storage explicitly.

## Final Model Boundaries

### MetaTable

`MetaTable` owns table storage identity and table contract:

- `uid`
- `data_source_uid`
- `storage_hash`
- `identifier`
- `namespace`
- `description`
- `labels`
- `management_mode`
- `physical_table_name`
- `table_contract`
- `contract_version`
- projected columns, indexes, foreign keys, incoming foreign keys
- open/shared/protected state
- registration, validation, introspection, and governed SQL operations

For DataNode-produced tables, the physical table is still timestamped and
multi-indexed, but the structural contract belongs to MetaTable.

### TimeIndexMetaData

`TimeIndexMetaData` becomes a specialized MetaTable projection for legacy
DataNode table operations.

It may keep DataNode-specific extension fields:

- `sourcetableconfiguration` / `source_table_configuration`
- update progress statistics
- dynamic-table read helpers such as `get_last_observation(...)`,
  `get_data_between_dates_from_api(...)`, and tail delete
- compatibility fields that existing serializers still send

It should not own generic storage metadata that MetaTable already owns.

Fields that should move away from `TimeIndexMetaData` over time:

- `build_configuration_json_schema`: belongs to the update process, not storage;
- `source_class_name`: belongs to update provenance, not storage;
- table/column/foreign-key structure: belongs to `MetaTable.table_contract`;
- label/share/open state: inherited from MetaTable.

`TimeIndexMetaData.initialize_source_table(...)` and
`TimeIndexMetaData.set_or_update_columns_metadata(...)` are removed from the SDK
surface. New code should put source-table setup, column labels/descriptions,
and structural metadata in MetaTable registration/bootstrap contracts.

### TimeIndexMetaData Inventory Review

Reviewed against `mainsequence/client/models_foundry.py` on 2026-05-29.

This inventory is the current ownership line for the client model. It is not a
new API promise; it is the cleanup checklist for finishing the MetaTable
canonical storage migration.

#### Field Ownership

| Current field surface | Final owner | Direction |
| --- | --- | --- |
| Inherited MetaTable fields: `uid`, `data_source`, `data_source_uid`, `storage_hash`, `identifier`, `namespace`, `description`, `labels`, `management_mode`, `physical_table_name`, `table_contract`, `contract_version`, `introspection_snapshot`, `protect_from_deletion`, `columns`, `indexes_meta`, `foreign_keys`, `incoming_fks`, `creation_date`, `created_by_user_uid`, `organization_owner_uid`, `open_for_everyone`, `registration` | MetaTable | Canonical storage state. Keep on `MetaTable`; `TimeIndexMetaData` may expose them only through inheritance/projection. |
| `ENDPOINT`, `FILTERSET_FIELDS`, `FILTER_VALUE_NORMALIZERS`, `READ_QUERY_PARAMS`, `READ_QUERY_PARAM_DESCRIPTIONS` | Compatibility wrapper | Dynamic-table endpoint and query compatibility. Keep only while backend routes still use `ts_manager/dynamic_table`. |
| `build_configuration_json_schema` | Update process | Move to the update record/build configuration contract. It is not table storage metadata. |
| `data_source_open_for_everyone` | DataSource or access policy | Compatibility projection only. Canonical access lives on DataSource/MetaTable permission fields. |
| `source_class_name` | Update process provenance | Move to the update record or update provenance payload. It is not storage identity. |
| `time_indexed_profile` | DataNode table extension | Keep as a MetaTable-backed timestamped-table profile projection until the backend exposes the equivalent extension directly from MetaTable. |
| `table_index_names` | Compatibility wrapper | Derive from `time_indexed_profile` or `MetaTable.table_contract`; remove as a stored client concern. |
| `compression_policy_config`, `retention_policy_config` | DataNode table extension | Timestamped-table storage policy. Keep out of update-process configuration. |
| `_drop_indices`, `_rebuild_indices` | Update/write option | Do not persist as table metadata. If still needed, pass as an explicit write/bootstrap option. |

Nested `time_indexed_profile` fields have the same ownership split:

| Current profile field surface | Final owner | Direction |
| --- | --- | --- |
| `time_index_name`, `index_names`, `column_dtypes_map`, `storage_layout`, `physical_index_plan` | DataNode table extension backed by MetaTable contract | These describe timestamped table semantics; structural column truth still comes from `MetaTable.table_contract`. |
| `foreign_keys`, `foreign_key_projections`, `columns_metadata` | MetaTable contract/projection | Canonical source is `MetaTable.table_contract`, `MetaTable.foreign_keys`, and `MetaTable.columns`; profile fields are projections. |
| `multi_index_stats`, `multi_index_column_stats`, `last_time_index_value`, `earliest_index_value` | DataNode table extension/update progress projection | Keep as table-profile progress read models, not generic MetaTable fields. |
| `dynamic_table_uid`, `related_table_uid` | Compatibility wrapper | These must resolve to MetaTable UID during transition. Do not introduce a second storage identity. |
| `column_index_names` | Compatibility wrapper | Legacy projection; remove when callers use `index_names`. |

#### Method Ownership

| Current method surface | Classification | Direction |
| --- | --- | --- |
| Inherited `MetaTable` methods: `patch`, `patch_by_hash`, `delete`, `destroy_by_uid`, `validate_contract`, `validate_existing_contract`, `introspect`, `refresh_table_search_index`, `run_query`, `description_search`, `column_search`, `execute_operation` | MetaTable method | Canonical table operations. Prefer `MetaTable` where no DataNode timestamp semantics are required. |
| `register` | DataNode table extension/bootstrap wrapper | Intentional dynamic-table registration while backend routes remain split. It must stay out of DataNode runtime and PersistManager hot paths. |
| `get_or_create` | Compatibility wrapper | Do not use from DataNode runtime. Storage creation belongs to MetaTable registration/bootstrap. |
| `_fill_legacy_dynamic_table_metatable_fields` | Compatibility wrapper | Payload coercion for old dynamic-table responses that do not emit full MetaTable fields. Remove after backend serializers converge. |
| `_time_indexed_dynamic_contract`, `_time_indexed_storage_layout`, `time_index_name`, `index_names`, `column_dtypes_map`, `time_indexed_foreign_keys`, `_require_time_indexed_table_contract` | DataNode table extension | Contract/profile readers for timestamped DataNode tables. Keep as extension helpers or move behind a dedicated profile object. |
| `_date_for_payload`, `_normalize_dimension_range_map`, `_build_dimension_payload` | DataNode table extension | Payload helpers for timestamped read/delete routes. |
| `get_data_updates` | DataNode table extension/update progress projection | Reads update-progress/table-profile stats. Do not make it generic MetaTable state. |
| `delete_after_date` | DataNode table extension | Timestamped tail delete. Keep distinct from generic MetaTable delete/drop operations. |
| `_uses_session_duckdb_data_source`, `_uses_session_local_data_source`, `delete_table` | Compatibility wrapper around MetaTable/local storage operations | Generic table deletion belongs to MetaTable; local-session branching should move out of `TimeIndexMetaData`. |
| `handle_time_indexed_profile_creation` | Compatibility wrapper | Legacy validation shim only. It must not create profile/storage and must not be called from the DataNode write hot path. |
| `_serialize_column_metadata_for_validation`, `_validate_existing_source_table_columns_metadata`, `_normalize_source_table_foreign_key_for_validation`, `_validate_existing_source_table_foreign_keys` | Compatibility wrapper for MetaTable contract validation | Final owner is MetaTable registration/bootstrap validation. These should disappear when DataNode write paths stop carrying schema metadata. |
| `map_columns_to_df`, `get_last_observation`, `_get_data_between_dates_common`, `get_data_between_dates_from_api`, `get_data_between_dates_from_node_identifier` | DataNode table extension/read compatibility | Keep only for timestamped reads. `get_data_between_dates_from_node_identifier` is a compatibility class route; new factories should resolve MetaTable-backed storage first. |

No `TimeIndexMetaData` method should be a final update-process method. Anything
that creates, resolves, mutates, or schedules an update record belongs on
`DataNodeUpdate`, `PersistManager`, or the runtime `DataNode`, not on storage.

### DataNodeUpdate Client Record

The backend update record currently named
`mainsequence.client.models_metatables.DataNodeUpdate` remains the persisted update
process record in this ADR.

A future naming ADR may rename or alias it if runtime naming is revisited.
Candidate names, if needed:

- `DataNodeUpdateRecord`
- `DataNodeUpdateModel`
- `DataNodeUpdateResource`

The final persisted update record should reference storage by MetaTable UID:

```python
meta_table_uid: str
```

but new request payloads should send `meta_table_uid`, not
`remote_table__hash_id`.

### Runtime Naming

The runtime update-process class remains `DataNode` for now.

```python
class DataNode(DataAccessMixin, ABC):
    def __init__(
        self,
        config: DataNodeUpdateConfiguration,
        storage_table: type[PlatformTimeIndexMetaData],
        *,
        hash_namespace: str | None = None,
    ):
        ...
```

Do not introduce a third public runtime class name. A later ADR or implementation
phase may revisit naming, but no runtime or client-resource rename is required
in this ADR.

## Authoring Surface

New code should separate storage creation from update process creation.

Storage authoring:

```python
from mainsequence.meta_tables import PlatformTimeIndexMetaData


class PricesTable(PlatformTimeIndexMetaData, Base):
    __metatable_namespace__ = "market_data"
    __metatable_identifier__ = "prices_daily"
    __time_index_name__ = "time_index"
    __index_names__ = ["time_index", "unique_identifier"]

    ...


PricesTable.register()
```

Storage/bootstrap code may also bind a previously-created backend MetaTable to
the authoring model with `PricesTable.bind_meta_table(prepared_meta_table)`.
`DataNode` must not perform either operation.

Update authoring:

```python
from mainsequence.meta_tables import DataNode, DataNodeUpdateConfiguration
from mainsequence.meta_tables import PlatformTimeIndexMetaData


class PricesUpdateConfiguration(DataNodeUpdateConfiguration):
    shard_id: str


class PricesUpdate(DataNode):
    def __init__(
        self,
        config: PricesUpdateConfiguration,
        storage_table: type[PlatformTimeIndexMetaData],
    ):
        super().__init__(config=config, storage_table=storage_table)

    def dependencies(self):
        return {}

    def update(self):
        ...
```

The user should be able to instantiate:

```python
data_node_update = PricesUpdate(
    config=PricesUpdateConfiguration(shard_id="eu"),
    storage_table=PricesTable,
)
```

## Hashing And Identity

The final identity rules are:

- `storage_hash` is produced by MetaTable registration or MetaTable authoring
  helpers.
- The DataNode update runtime must not compute `storage_hash`.
- The DataNode update runtime should not expose `storage_hash` as a derived
  compatibility property; code that needs storage identity must read the
  first-class `storage_table`.
- `update_hash` is still computed by the update runtime because it identifies
  the updater process.
- `update_hash` is not changed by this migration. It remains scoped to the
  update process configuration until a separate hashing ADR says otherwise.

`hash_namespace` changes meaning:

- It may continue to isolate update process hashes.
- It must not silently create a different storage table.
- If a caller wants namespaced storage, they must pass a namespaced MetaTable.
- `test_node=True` is removed. Tests should use explicit `hash_namespace`
  values and pass the intended storage table explicitly.

## Configuration Split

`DataNodeConfiguration` currently mixes storage and update concerns. In the
canonical model, storage concerns belong only to MetaTable:

- `records`
- `foreign_keys`
- `node_metadata.identifier`
- `node_metadata.description`
- future table indexes and table constraints

Update concerns should remain on the update configuration:

- shard identifiers
- update-only filters
- updater scope
- `offset_start`
- API/source selection when it changes updater behavior but not table schema

The migration should introduce a clearer configuration name:

```python
class DataNodeUpdateConfiguration(BaseConfiguration):
    offset_start: datetime.datetime | None = None
```

`DataNodeConfiguration` can remain as a compatibility alias while docs migrate.

## SourceTableConfiguration

`SourceTableConfiguration` should stop being the structural schema owner.

Final responsibility split:

- `MetaTable.table_contract` owns columns, indexes, foreign keys, and physical
  table identity.
- A DataNode-specific table profile owns timestamped update semantics:
  `time_index_name`, `index_names`, update progress, tail-delete semantics,
  and any server-rendered physical index plan.

This can be represented either as:

- a DataNode extension block in `MetaTable.table_contract["authoring"]`; and/or
- a server-side `SourceTableConfiguration` relation attached to a MetaTable UID.

During compatibility, `TimeIndexMetaData.sourcetableconfiguration` can continue
to expose `SourceTableConfiguration`. The implementation should make it a
projection of MetaTable-backed source-table state instead of a separate schema
source of truth.

## DataNode Storage Creation Removal

The DataNode runtime path must stop creating storage resources.

The following objects must be assumed to already exist before any DataNode
update process runs:

- the canonical storage `MetaTable`;
- the compatibility `TimeIndexMetaData` / dynamic-table projection when legacy
  dynamic-table routes are still used;
- the DataNode table profile exposed as `SourceTableConfiguration` or its
  MetaTable-backed replacement.

DataNode-related code may validate and read these objects, but it must not
create them. This applies to every relation of the DataNode runtime: constructor
setup, persist manager registration, update-record creation, write bootstrap,
DataFrame upsert, local database setup, and foreign-key/schema metadata paths.

The old hot path violated this boundary:

```python
metadata.handle_time_indexed_profile_creation(
    column_dtypes_map=column_dtypes_map,
    index_names=index_names,
    time_index_name=time_index_name,
    data=data,
    overwrite=overwrite,
    columns_metadata=columns_metadata,
    foreign_keys=foreign_keys,
)
```

`handle_time_indexed_profile_creation(...)` could call
`initialize_source_table(...)`, which creates or validates
`TimeIndexedProfile` and may create the physical backing table. In the
canonical model, this is not allowed in the update hot path.

The replacement behavior is:

- `DataNodeUpdate.upsert_data_into_table(...)` assumes the DataNode already has
  the resolved MetaTable/storage profile before the update runs.
- `DataNodeUpdate.upsert_data_into_table(...)` must not call storage/profile
  creation, initialization, mutation, or validation helpers.
- Schema/profile validation belongs to DataNode setup, MetaTable registration,
  or explicit bootstrap commands, not the update hot path.
- `columns_metadata` and `foreign_keys` must not trigger creation or mutation
  from the DataNode write path.
- `TimeIndexMetaData.initialize_source_table(...)` is removed from the SDK
  surface. DataNode runtime, `PersistManager`, and `DataNodeUpdate` hot-path
  code must not call source-table initialization.
- `TimeIndexedProfile.create(...)` and `TimeIndexMetaData.get_or_create(...)`
  must not be used by DataNode runtime relations. Storage creation belongs to
  MetaTable registration or an explicit schema/bootstrap command that runs
  before updates.

## Persist Manager Flow

The write path should become:

1. User prepares a `PlatformTimeIndexMetaData` storage model outside DataNode
   runtime. That bootstrap path registers or binds the backend MetaTable UID.
2. User constructs a DataNode update process with:

   ```python
   PricesUpdate(config=..., storage_table=storage_table)
   ```

3. The runtime computes only `update_hash`.
4. `PersistManager` receives `storage_table` only, then derives MetaTable UID
   and data-source UID from that platform-managed model.
5. `PersistManager` creates or resolves the backend update record with:

   ```python
   DataNodeUpdateRecord.get_or_create(
       update_hash=update_hash,
       build_configuration=local_configuration,
       data_source_uid=storage_table.get_data_source_uid(),
       meta_table_uid=storage_table.get_meta_table_uid(),
   )
   ```

6. The update runner validates the output DataFrame against
   the storage table contract.
7. Writes target the storage table by UID.
8. Update progress is stored on the update record and the DataNode table profile,
   not on generic table metadata.

`PersistManager.local_persist_exist_set_config(...)` should no longer create
storage as part of update registration.

It should also not create or initialize the source-table profile. Its only
storage responsibility is to use the already supplied MetaTable target, verify
that the required profile already exists, and pass storage identity into update
record creation.

## Read And APIDataNode Flow

`APIDataNode` should become a read wrapper over a MetaTable-backed storage table.

Factories should prefer MetaTable identity:

```python
APIDataNode.build_from_table_uid(meta_table_uid)
APIDataNode.build_from_identifier(identifier)
APIDataNode.build_from_meta_table(storage_table)
```

Internally, the read path may still need dynamic-table endpoints until the
backend exposes equivalent MetaTable routes for timestamped table reads.

Compatibility fields:

- `data_source_uid` is derived from the resolved MetaTable.
- integer `data_source_id` remains removed per ADR 0015.

## Client Import And Inheritance Plan

Before `TimeIndexMetaData(MetaTable)` could be implemented, the client model
import cycle had to be removed.

Removed cycle risk:

- `models_metatables.py` previously needed `DynamicTableDataSource` from
  `models_foundry.py`.
- `models_foundry.py` would then have needed `MetaTable` to make
  `TimeIndexMetaData` inherit from it.

Implemented refactor:

1. Move shared data-source client models from `models_foundry.py` into
   `models_metatables.py`, because MetaTable is the parent storage model.
2. Move `TimeIndexMetaData`, registration payloads, DataNode update records,
   scheduler/run-configuration models, historical update models, and update
   statistic payload builders into `models_metatables.py`.
3. Keep public imports stable from `mainsequence.client`, but do not re-export
   the moved classes from `models_foundry.py`.
4. Rebuild Pydantic forward references in a deterministic order.

Do not reintroduce public `models_foundry.py` compatibility exports for the moved
MetaTable/update classes.

## Backend Contract Implications

The backend must converge on the same resource model.

Required backend capabilities:

- `DynamicTableMetaData` is represented as a MetaTable subtype or one-to-one
  extension.
- Dynamic table serializers expose MetaTable fields needed by the client
  superclass: `uid`, `storage_hash`, `data_source_uid`, `identifier`,
  `namespace`, `management_mode`, `physical_table_name`, `table_contract`,
  labels, permissions, and open/protection state.
- DataNode update records are created with `meta_table_uid`.
- Timestamped table write/read endpoints accept MetaTable UID, or dynamic-table
  UID remains identical to MetaTable UID during transition.
- Source-table initialization validates against MetaTable contract rather than
  creating a separate structural schema.
- Search, semantic index refresh, tail delete, latest observation, and
  between-date reads continue to work for DataNode-produced MetaTables.

## Migration Sequence

### Phase 0: Inventory And Contract Lock

- Inventory every `TimeIndexMetaData` field and method and classify it as:
  canonical MetaTable field, DataNode table extension, update-process field, or
  compatibility field.
- Inventory every backend dynamic-table serializer field and confirm what can
  be emitted as MetaTable-compatible payload.
- Confirm whether DataNode-produced table UID and MetaTable UID are the same
  resource UID or whether a mapping layer is required.

### Phase 1: Break Client Model Cycles

- Move shared data-source models from `models_foundry.py` into `models_metatables.py`.
- Keep public imports stable from `mainsequence.client`.
- Add focused import tests for:
  `mainsequence.client.MetaTable`, `mainsequence.client.TimeIndexMetaData`,
  `mainsequence.client.DataNodeUpdate`, and `mainsequence.meta_tables.DataNode`.

### Phase 2: Make TimeIndexMetaData Inherit MetaTable

- Change `TimeIndexMetaData` to inherit from `MetaTable`.
- Override `ENDPOINT` to keep legacy dynamic-table routes during transition.
- Override required MetaTable fields only where legacy backend payloads cannot
  yet provide them.
- Add aliases for `sourcetableconfiguration` and
  `source_table_configuration`.
- Prevent MetaTable registration class methods from accidentally becoming a
  public `TimeIndexMetaData.register(...)` surface unless that behavior is
  intentionally supported.
### Phase 3: Introduce Explicit Storage Table Runtime Argument

- Update the runtime `DataNode` constructor to accept:

  ```python
  __init__(
      config: DataNodeUpdateConfiguration,
      storage_table: type[PlatformTimeIndexMetaData],
      ...
  )
  ```

- Do not add a derived runtime `storage_hash` property; storage identity stays
  on the platform-managed storage model and its bound MetaTable UID.
- Fail explicitly when a DataNode update is run without a prepared
  `storage_table` carrying MetaTable identity.

### Phase 4: Move Storage Creation Out Of PersistManager

- Change `PersistManager` to require a resolved storage table.
- Replace storage creation with storage validation.
- Create update records with `meta_table_uid`.

### Phase 4A: Remove Source-Table Creation From DataNode Runtime

- Replace
  `TimeIndexMetaData.handle_time_indexed_profile_creation(...)` calls from
  `DataNodeUpdate.upsert_data_into_table(...)` without adding replacement
  write-path validation.
- Remove or deprecate `handle_time_indexed_profile_creation(...)` as a
  DataNode write-path helper. If kept temporarily, rename or split it so the
  runtime path cannot call `initialize_source_table(...)`.
- Keep profile existence and compatibility checks in DataNode setup,
  MetaTable registration, or explicit bootstrap flows.
- Remove all DataNode-runtime calls to:
  `TimeIndexMetaData.get_or_create(...)`,
  `TimeIndexedProfile.create(...)`, and
  `TimeIndexMetaData.initialize_source_table(...)`.
- Keep explicit schema/bootstrap commands responsible for MetaTable
  registration/bootstrap. Do not expose direct dynamic-table
  `initialize_source_table(...)` from `TimeIndexMetaData`.
- Add regression tests proving `DataNodeUpdate.upsert_data_into_table(...)`
  does not call `handle_time_indexed_profile_creation(...)` or
  `initialize_source_table(...)`.

### Phase 5: Validate Against MetaTable Contract

- Update output validation to validate against `storage_table.table_contract`.
- Remove DataNode runtime helpers that derive table metadata, column metadata,
  source-table schemas, or FK contracts from DataNode configuration.
- Stop treating `SourceTableConfiguration` as the source of structural column
  truth.

### Phase 6: Migrate Writes And Reads To MetaTable UID

- Update insert/upsert endpoints or client calls to target storage by MetaTable
  UID.
- Update source-table initialization to target storage by MetaTable UID.
- Update latest-observation, between-date reads, search, tail delete, and
  query helpers to accept MetaTable-backed storage.
- Keep `TimeIndexMetaData` methods as compatibility wrappers until backend routes
  converge.

### Phase 7: Keep Runtime Naming Stable

- Keep runtime `DataNode` unchanged in this migration. Runtime renaming is
  deferred.
- Keep the client resource currently named `DataNodeUpdate` unchanged unless a
  separate naming ADR is accepted.
- Update docs to distinguish `mainsequence.meta_tables.DataNode` as the runtime class
  from `mainsequence.client.DataNodeUpdate` as the backend update record.

### Phase 8: CLI And Docs

- Make `mainsequence meta-table ...` the canonical table-storage CLI.
- Keep `mainsequence data-node ...` focused on update processes and
  DataNode-specific read helpers.
- Keep existing `mainsequence data-node ...` storage commands available as
  compatibility commands during the transition, but stop treating them as the
  canonical storage-management surface.
- Update tutorials to teach:

  ```python
  storage_table = PricesTable
  data_node = PricesUpdate(config, storage_table)
  ```

- Tutorial order must introduce `MetaTable` first as the canonical platform
  table abstraction, then introduce `DataNode` as an opinionated
  MetaTable-backed update workflow.
- Replace `update_hash vs storage_hash` explanations with:
  "MetaTable is storage; DataNode is the update process."
- Regenerate reference docs after public signatures stabilize.

### Phase 9: Removal

- Remove automatic storage creation from the DataNode runtime.
- Remove old storage-hash generation from `DataNode` construction.
- Remove deprecated DataNode authoring docs that put table schema in update
  configuration.

## Implementation Tasks

- [x] Inventory `TimeIndexMetaData` fields and classify their final owner.
- [x] Inventory `TimeIndexMetaData` methods and classify as MetaTable method,
      DataNode table extension, update-process method, or compatibility wrapper.
- [x] Move data-source client models from `models_foundry.py` into
      `models_metatables.py` to avoid import cycles.
- [x] Make `TimeIndexMetaData` inherit from `MetaTable`.
- [x] Add first-class `storage_table: PlatformTimeIndexMetaData` runtime argument.
- [x] Change `PersistManager` to validate existing storage instead of creating
      storage.
- [x] Change update record creation to send `meta_table_uid`.
- [x] Validate update output DataFrames against MetaTable contract.
- [x] Remove `DataNodeUpdate.upsert_data_into_table(...)` calls to
      `handle_time_indexed_profile_creation(...)`.
- [x] Remove DataNode-runtime calls to
      `TimeIndexMetaData.initialize_source_table(...)`.
- [x] Remove `TimeIndexMetaData.initialize_source_table(...)` from the SDK
      surface.
- [x] Keep `DataNodeUpdate.upsert_data_into_table(...)` free of replacement
      storage/profile validation.
- [x] Add tests proving the DataNode write path does not call the legacy
      profile creation helper.
- [ ] Add tests proving foreign-key and column metadata in the DataNode write
      path are not used to mutate/create storage metadata.
- [x] Update `APIDataNode` factories to resolve MetaTable-backed storage.
- [x] Update CLI data-node and meta-table command ownership.
- [x] Update DataNode and MetaTable tutorials.
- [ ] Regenerate reference docs.

## Compatibility Policy

Compatibility is important, but it should not preserve the old mental model as
the documented path.

Allowed temporary compatibility:

- `DataNode` remains importable.
- `TimeIndexMetaData` remains importable and can keep dynamic-table read helpers.
Not allowed as final behavior:

- DataNode runtime computes storage identity.
- PersistManager creates storage as a hidden side effect of updater creation.
- DataNode runtime creates or initializes `TimeIndexMetaData`.
- DataNode runtime creates or initializes `TimeIndexedProfile`.
- `DataNodeUpdate.upsert_data_into_table(...)` calls
  `handle_time_indexed_profile_creation(...)` or any helper that can
  create storage/profile state.
- Table schema is split between DataNode config and TimeIndexedProfile.
- New docs teach users to put table storage concerns inside update config.
- Public code requires integer resource IDs.

## Consequences

The SDK will have one canonical table-storage model: MetaTable.

DataNode update code becomes easier to reason about:

- storage exists before the update process;
- update process identity is separate from table identity;
- table permissions and discovery use the same model for DataNode tables and
  other relational tables;
- DataNode-produced tables can participate naturally in MetaTable SQL,
  introspection, labels, sharing, and foreign-key relationships.

This is a breaking conceptual migration. Existing code that relies on DataNode
constructors to implicitly create storage must migrate to explicit storage table
creation before this ADR lands.

## Risks

- Direct `TimeIndexMetaData(MetaTable)` inheritance can break Pydantic validation
  if backend dynamic-table payloads do not include required MetaTable fields.
- Import cycles will occur unless shared data-source/client models are split
  first.
- Inherited MetaTable class methods may expose invalid TimeIndexMetaData command
  surfaces unless they are moved, guarded, or overridden.
- Existing docs and CLI commands heavily use `TimeIndexMetaData`; they must be
  migrated carefully to avoid confusing users.
- Changing update hashing would affect runtime identity compatibility; keep it
  outside this migration unless a separate hashing ADR is accepted.
- Backend routes may continue to distinguish MetaTable and DynamicTableMetaData
  longer than the client model does, requiring compatibility wrappers.

## Acceptance Criteria

The migration is complete when:

- `TimeIndexMetaData` inherits from the MetaTable client model without import
  cycles.
- DataNode-produced storage tables expose canonical MetaTable fields and UID
  identity.
- New DataNode update runtime construction requires an explicit
  `PlatformTimeIndexMetaData` storage argument carrying MetaTable identity.
- DataNode update runtime no longer computes `storage_hash`.
- `PersistManager` does not create storage in the normal path.
- No DataNode runtime relation calls `TimeIndexMetaData.get_or_create(...)`,
  `TimeIndexMetaData.initialize_source_table(...)`, or
  `TimeIndexedProfile.create(...)`.
- `DataNodeUpdate.upsert_data_into_table(...)` does not call storage/profile
  creation, initialization, mutation, or validation helpers.
- Column metadata and foreign-key metadata on the DataNode write path do not
  create or mutate storage/profile schema.
- Update records link to storage by MetaTable UID.
- DataFrame output validation uses the MetaTable table contract.
- SourceTableConfiguration no longer owns structural schema.
- Runtime naming remains `DataNode`; any future rename to `DataNodeUpdate` is
  tracked outside the current implementation sequence.
- DataNode tutorials show:

  ```python
  storage_table = PricesTable
  data_node = PricesUpdate(config, storage_table)
  ```

- Compatibility aliases are covered by tests and emit deprecation warnings where
  practical.
