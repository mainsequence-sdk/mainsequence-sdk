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
- `DataNodeStorage`, exposed by `mainsequence.client.models_tdag.DataNodeStorage`,
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
- `DataNodeStorage` in `mainsequence/client/models_tdag.py`
- `DataNodeUpdate` in `mainsequence/client/models_tdag.py`
- `DataNode` runtime class in `mainsequence/tdag/data_nodes/data_nodes.py`
- `PersistManager` in `mainsequence/tdag/base_persist_managers.py`
- `build_operations.create_config(...)` in
  `mainsequence/tdag/data_nodes/build_operations.py`

The current write path is:

1. A concrete `DataNode` subclass is constructed with a `DataNodeConfiguration`.
2. `DataNode.__init_subclass__` captures constructor arguments and calls
   `build_operations.create_config(...)`.
3. `create_config(...)` returns both `update_hash` and `storage_hash`.
4. `DataNode.verify_and_build_remote_objects()` calls
   `PersistManager.local_persist_exist_set_config(...)`.
5. `PersistManager` creates or resolves `DataNodeStorage` with:

   ```python
   DataNodeStorage.get_or_create(
       storage_hash=storage_hash,
       data_source_uid=data_source.uid,
       build_configuration_json_schema=...,
       open_to_public=...,
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
   `SourceTableConfiguration` on `DataNodeStorage`, uploads data, and updates
   progress statistics.

This means the storage row is a side effect of constructing an updater.

## Problem

Storage identity should not be owned by DataNode update code.

The current design creates these issues:

- A user cannot cleanly say "this update process writes into this existing
  table" without letting the DataNode compute a `storage_hash`.
- Table schema and table metadata are split across
  `DataNodeConfiguration.records`, `DataNodeConfiguration.foreign_keys`,
  `DataNodeConfiguration.node_metadata`, `DataNodeStorage`, and
  `SourceTableConfiguration`.
- `DataNodeStorage` and `MetaTable` duplicate storage-facing fields, filters,
  labels, sharing, UID identity, and data-source identity.
- The name `DataNode` suggests a data product/storage object, but the runtime
  class is increasingly the update process.
- The existing client model name `DataNodeUpdate` is already used for the
  backend update record, so renaming the runtime class to `DataNodeUpdate`
  requires a deliberate compatibility plan.
- `MetaTable` currently imports `DynamicTableDataSource` from `models_tdag.py`.
  Making `DataNodeStorage` inherit from `MetaTable` inside `models_tdag.py`
  creates a direct import-cycle risk unless shared data-source/storage models
  are split first.

## Decision

Make `MetaTable` the canonical storage model.

`DataNodeStorage` becomes a DataNode-specific compatibility/storage projection
that inherits from the MetaTable client model:

```python
class DataNodeStorage(MetaTable):
    ENDPOINT = "ts_manager/dynamic_table"

    source_table_configuration: SourceTableConfiguration | None = None
    source_class_name: str | None = None  # compatibility only
```

`DataNodeStorage` remains available during migration because many public SDK,
CLI, tutorial, and backend paths still use that name. Its meaning changes:

- final canonical table fields come from `MetaTable`;
- DataNode-specific update/read extensions remain on `DataNodeStorage` only as
  compatibility or typed extension fields;
- new storage APIs should accept and return `MetaTable` wherever possible;
- old `DataNodeStorage` read/write helpers delegate to MetaTable-backed routes
  or continue to use legacy dynamic-table routes until the backend endpoint is
  migrated.

The DataNode runtime no longer computes the storage table identity. It accepts
the target storage table as a first-class constructor argument:

```python
storage_table = MetaTable.get(uid=storage_table_uid)
data_node_update = DataNodeUpdate(
    config=PricesUpdateConfiguration(...),
    storage_table=storage_table,
)
```

This is the final public direction. The implementation must rename the current
client resource model called `DataNodeUpdate` to a record/resource name so the
runtime authoring class can take the `DataNodeUpdate` name.

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

### DataNodeStorage

`DataNodeStorage` becomes a specialized MetaTable projection for legacy
DataNode table operations.

It may keep DataNode-specific extension fields:

- `sourcetableconfiguration` / `source_table_configuration`
- update progress statistics
- dynamic-table read helpers such as `get_last_observation(...)`,
  `get_data_between_dates_from_api(...)`, and tail delete
- compatibility fields that existing serializers still send

It should not own generic storage metadata that MetaTable already owns.

Fields that should move away from `DataNodeStorage` over time:

- `build_configuration_json_schema`: belongs to the update process, not storage;
- `source_class_name`: belongs to update provenance, not storage;
- table/column/foreign-key structure: belongs to `MetaTable.table_contract`;
- label/share/open state: inherited from MetaTable.

### DataNodeUpdate Client Record

The backend update record currently named
`mainsequence.client.models_tdag.DataNodeUpdate` remains the persisted update
process record during the first migration phases.

It should eventually be renamed or aliased to avoid conflict with the runtime
authoring class. Candidate final names:

- `DataNodeUpdateRecord`
- `DataNodeUpdateModel`
- `DataNodeUpdateResource`

The final persisted update record should reference storage by MetaTable UID:

```python
storage_table: str | MetaTable
```

Compatibility aliases may continue to expose:

```python
data_node_storage: str | DataNodeStorage
```

but new request payloads should send `storage_table_uid` or
`meta_table_uid`, not `remote_table__hash_id`.

### DataNodeUpdate Runtime Class

The runtime update-process class should be refactored from `DataNode` to
`DataNodeUpdate`. Do not introduce a third public runtime class name.

```python
class DataNodeUpdate(DataAccessMixin, ABC):
    def __init__(
        self,
        config: DataNodeUpdateConfiguration,
        storage_table: MetaTable,
        *,
        hash_namespace: str | None = None,
        test_node: bool = False,
    ):
        ...
```

`DataNode` should remain as a deprecated compatibility alias or subclass:

```python
class DataNode(DataNodeUpdate):
    ...
```

The final public import is:

```python
from mainsequence.tdag import DataNodeUpdate
```

The existing `mainsequence.client.DataNodeUpdate` backend resource must be
renamed first, for example to `DataNodeUpdateRecord`, so the runtime name is not
ambiguous.

## Authoring Surface

New code should separate storage creation from update process creation.

Storage authoring:

```python
from mainsequence.client import MetaTable

storage_table = MetaTable.register(
    data_source_uid=data_source.uid,
    management_mode="platform_managed",
    storage_hash="prices_daily",
    identifier="prices_daily",
    namespace="market_data",
    table_contract={
        "version": "relational-table.v1",
        "physical": {"table_name": "prices_daily"},
        "columns": [
            {"name": "time_index", "data_type": "timestamp with time zone", "nullable": False},
            {"name": "asset_uid", "data_type": "uuid", "nullable": False},
            {"name": "close", "data_type": "double precision", "nullable": False},
        ],
        "indexes": [
            {"name": "prices_daily_identity_idx", "columns": ["time_index", "asset_uid"], "unique": True}
        ],
        "foreign_keys": [],
        "authoring": {
            "data_node": {
                "time_index_name": "time_index",
                "index_names": ["time_index", "asset_uid"],
            }
        },
    },
)
```

Update authoring:

```python
from mainsequence.tdag import DataNodeUpdate, DataNodeUpdateConfiguration


class PricesUpdateConfiguration(DataNodeUpdateConfiguration):
    shard_id: str


class PricesUpdate(DataNodeUpdate):
    def __init__(self, config: PricesUpdateConfiguration, storage_table: MetaTable):
        super().__init__(config=config, storage_table=storage_table)

    def dependencies(self):
        return {}

    def update(self):
        ...
```

The user should be able to instantiate:

```python
storage_table = MetaTable(...)
data_node_update = PricesUpdate(
    config=PricesUpdateConfiguration(shard_id="eu"),
    storage_table=storage_table,
)
```

## Hashing And Identity

The final identity rules are:

- `storage_hash` is produced by MetaTable registration or MetaTable authoring
  helpers.
- The DataNode update runtime must not compute `storage_hash`.
- The update process may expose `self.storage_hash` as a compatibility property
  that returns `self.storage_table.storage_hash`.
- `update_hash` is still computed by the update runtime because it identifies
  the updater process.
- `update_hash` must include the first-class storage table identity to avoid
  collisions when the same updater code/config writes to different tables.

The storage identity used in `update_hash` should be deterministic and portable.
Prefer:

```python
{
    "storage_table": {
        "storage_hash": storage_table.storage_hash,
    }
}
```

Do not require backend integer IDs in any hash.

`data_source_uid` can remain part of update lookup and backend payloads without
being embedded in `update_hash`, matching the current pattern where update
resolution is scoped by data source. Embedding `data_source_uid` or
`MetaTable.uid` in the hash should be avoided unless the implementation
intentionally decides that update hashes are backend-environment-specific. Those
UIDs should still be sent in backend request payloads because they are the
public resource references.

`hash_namespace` changes meaning:

- It may continue to isolate update process hashes.
- It must not silently create a different storage table.
- If a caller wants namespaced storage, they must pass a namespaced MetaTable.
- Legacy `test_node=True` may keep an auto-storage compatibility path during
  migration, but new code should pass the storage table explicitly.

## Configuration Split

`DataNodeConfiguration` currently mixes storage and update concerns.

Storage concerns should move to MetaTable:

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
    offset_start: datetime.datetime | None = Field(
        default=None,
        json_schema_extra={"update_only": True},
    )
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

During compatibility, `DataNodeStorage.sourcetableconfiguration` can continue
to expose `SourceTableConfiguration`. The implementation should make it a
projection of MetaTable-backed source-table state instead of a separate schema
source of truth.

## Persist Manager Flow

The write path should become:

1. User constructs or resolves a `MetaTable`.
2. User constructs a DataNode update process with:

   ```python
   DataNodeUpdate(config=..., storage_table=storage_table)
   ```

3. The runtime computes only `update_hash`.
4. `PersistManager` validates that `storage_table.uid`,
   `storage_table.storage_hash`, and `storage_table.data_source_uid` are
   present.
5. `PersistManager` creates or resolves the backend update record with:

   ```python
   DataNodeUpdateRecord.get_or_create(
       update_hash=update_hash,
       build_configuration=local_configuration,
       data_source_uid=storage_table.data_source_uid,
       storage_table_uid=storage_table.uid,
   )
   ```

6. The update runner validates the output DataFrame against
   `storage_table.table_contract`.
7. Writes target the storage table by UID.
8. Update progress is stored on the update record and the DataNode table profile,
   not on generic table metadata.

`PersistManager.local_persist_exist_set_config(...)` should no longer create
storage as part of update registration. Any storage creation compatibility path
must be isolated and marked deprecated.

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

- `storage_hash` can remain a convenience projection from `storage_table`.
- `data_source_uid` remains required.
- integer `data_source_id` remains removed per ADR 0015.

## Client Import And Inheritance Plan

Before `DataNodeStorage(MetaTable)` can be implemented, the client model import
cycle must be removed.

Current cycle risk:

- `models_metatables.py` imports `DynamicTableDataSource` from `models_tdag.py`.
- `models_tdag.py` would need to import `MetaTable` to make
  `DataNodeStorage` inherit from it.

Implemented refactor:

1. Move shared data-source client models from `models_tdag.py` into
   `models_metatables.py`, because MetaTable is the parent storage model.
2. Make `models_tdag.py` import and re-export those data-source models from
   `models_metatables.py`.
3. Make `models_tdag.py` import `MetaTable` safely.
4. Rebuild Pydantic forward references in a deterministic order.

Do not reintroduce a `models_metatables.py` runtime import from `models_tdag.py`.

## Backend Contract Implications

The backend must converge on the same resource model.

Required backend capabilities:

- `DynamicTableMetaData` is represented as a MetaTable subtype or one-to-one
  extension.
- Dynamic table serializers expose MetaTable fields needed by the client
  superclass: `uid`, `storage_hash`, `data_source_uid`, `identifier`,
  `namespace`, `management_mode`, `physical_table_name`, `table_contract`,
  labels, permissions, and open/protection state.
- DataNode update records can be created with `storage_table_uid` or
  `meta_table_uid`.
- Legacy `remote_table__hash_id` creation remains temporarily but is deprecated.
- Timestamped table write/read endpoints accept MetaTable UID, or dynamic-table
  UID remains identical to MetaTable UID during transition.
- Source-table initialization validates against MetaTable contract rather than
  creating a separate structural schema.
- Search, semantic index refresh, tail delete, latest observation, and
  between-date reads continue to work for DataNode-produced MetaTables.

## Migration Sequence

### Phase 0: Inventory And Contract Lock

- Inventory every `DataNodeStorage` field and method and classify it as:
  canonical MetaTable field, DataNode table extension, update-process field, or
  compatibility field.
- Inventory every backend dynamic-table serializer field and confirm what can
  be emitted as MetaTable-compatible payload.
- Confirm whether DataNode-produced table UID and MetaTable UID are the same
  resource UID or whether a mapping layer is required.
- Decide the final request key: `storage_table_uid` or `meta_table_uid`.

### Phase 1: Break Client Model Cycles

- Move shared data-source models from `models_tdag.py` into `models_metatables.py`.
- Keep public imports stable from `mainsequence.client`.
- Add focused import tests for:
  `mainsequence.client.MetaTable`, `mainsequence.client.DataNodeStorage`,
  `mainsequence.client.DataNodeUpdate`, and `mainsequence.tdag.DataNode`.

### Phase 2: Make DataNodeStorage Inherit MetaTable

- Change `DataNodeStorage` to inherit from `MetaTable`.
- Override `ENDPOINT` to keep legacy dynamic-table routes during transition.
- Override required MetaTable fields only where legacy backend payloads cannot
  yet provide them.
- Add aliases for `sourcetableconfiguration` and
  `source_table_configuration`.
- Prevent MetaTable registration class methods from accidentally becoming a
  public `DataNodeStorage.register(...)` surface unless that behavior is
  intentionally supported.
- Add Pydantic tests proving existing DataNodeStorage payloads still parse.
- Add Pydantic tests proving MetaTable fields are available on DataNodeStorage.

### Phase 3: Refactor DataNode Runtime To DataNodeUpdate

- Rename/refactor the runtime `DataNode` class to `DataNodeUpdate` with:

  ```python
  __init__(config: DataNodeUpdateConfiguration, storage_table: MetaTable, ...)
  ```

- Keep `DataNode` as a compatibility subclass/alias.
- Make `storage_hash` a derived compatibility property from
  `storage_table.storage_hash`.
- Compute `update_hash` from update config plus storage table identity.
- Keep old DataNode constructors working with warnings when possible.
- Add tests proving two update processes with the same config but different
  storage tables get different `update_hash` values.
- Add tests proving changing storage metadata labels/descriptions does not
  change `update_hash`.

### Phase 4: Move Storage Creation Out Of PersistManager

- Change `PersistManager` to require a resolved storage table.
- Replace storage creation with storage validation.
- Create update records with storage table UID.
- Keep a deprecated compatibility path that can create legacy DataNodeStorage
  from old DataNode configuration only for existing users.
- Mark every compatibility path with a searchable `LEGACY_COMPAT` comment.

### Phase 5: Move Schema To MetaTable Contract

- Add helpers to build DataNode-compatible MetaTable contracts from existing
  `RecordDefinition`, FK declarations, `time_index_name`, and `index_names`.
- Update output validation to validate against `MetaTable.table_contract`.
- Keep `DataNodeConfiguration.records` and `foreign_keys` as compatibility
  inputs that build a MetaTable contract when old constructors are used.
- Stop treating `SourceTableConfiguration` as the source of structural column
  truth.

### Phase 6: Migrate Writes And Reads To MetaTable UID

- Update insert/upsert endpoints or client calls to target storage by MetaTable
  UID.
- Update source-table initialization to target storage by MetaTable UID.
- Update latest-observation, between-date reads, search, tail delete, and
  query helpers to accept MetaTable-backed storage.
- Keep `DataNodeStorage` methods as compatibility wrappers until backend routes
  converge.

### Phase 7: Resolve Naming

- Rename the client resource currently named `DataNodeUpdate` to a clearer
  record name such as `DataNodeUpdateRecord`.
- Export compatibility alias `DataNodeUpdate = DataNodeUpdateRecord` from
  `mainsequence.client` with deprecation warnings if feasible.
- Refactor/export the runtime class as `mainsequence.tdag.DataNodeUpdate`.
- Update tutorials and examples to use the new runtime name.

### Phase 8: CLI And Docs

- Make `mainsequence meta-table ...` the canonical table-storage CLI.
- Keep `mainsequence data-node ...` focused on update processes and
  DataNode-specific read helpers.
- Update tutorials to teach:

  ```python
  storage_table = MetaTable(...)
  data_node_update = DataNodeUpdate(config, storage_table)
  ```

- Replace `update_hash vs storage_hash` explanations with:
  "MetaTable is storage; DataNodeUpdate is the update process."
- Regenerate reference docs after public signatures stabilize.

### Phase 9: Removal

- Remove automatic storage creation from the DataNode runtime.
- Remove old storage-hash generation from `DataNode` construction.
- Remove `remote_table__hash_id` from update creation payloads after backend
  compatibility ends.
- Remove deprecated DataNode authoring docs that put table schema in update
  configuration.

## Implementation Tasks

- [ ] Inventory `DataNodeStorage` fields and classify their final owner.
- [ ] Inventory `DataNodeStorage` methods and classify as MetaTable method,
      DataNode table extension, update-process method, or compatibility wrapper.
- [x] Move data-source client models from `models_tdag.py` into
      `models_metatables.py` to avoid import cycles.
- [x] Make `DataNodeStorage` inherit from `MetaTable`.
- [ ] Add compatibility defaults or backend serializer fields for
      `management_mode`, `physical_table_name`, `table_contract`, and
      `contract_version`.
- [ ] Refactor runtime `DataNode` to `DataNodeUpdate`.
- [ ] Add first-class `storage_table: MetaTable` runtime argument.
- [ ] Make runtime `storage_hash` a derived compatibility property.
- [ ] Change update hashing to include storage table identity but not compute
      storage identity.
- [ ] Add tests for update hash behavior with different storage tables.
- [ ] Change `PersistManager` to validate existing storage instead of creating
      storage.
- [ ] Change update record creation to send storage table UID.
- [ ] Keep and test legacy storage creation only as a deprecated compatibility
      path.
- [ ] Move table records, FKs, identifiers, and descriptions into MetaTable
      contract helpers.
- [ ] Validate update output DataFrames against MetaTable contract.
- [ ] Update `APIDataNode` factories to resolve MetaTable-backed storage.
- [ ] Update CLI data-node and meta-table command ownership.
- [ ] Update DataNode and MetaTable tutorials.
- [ ] Regenerate reference docs.

## Compatibility Policy

Compatibility is important, but it should not preserve the old mental model as
the documented path.

Allowed temporary compatibility:

- `DataNode` remains importable.
- `DataNodeConfiguration.records`, `foreign_keys`, and `node_metadata` may build
  a MetaTable contract for old-style nodes.
- `DataNodeStorage` remains importable and can keep dynamic-table read helpers.
- `storage_hash` remains readable on update runtime objects as a derived
  property.
- `remote_table__hash_id` may remain in backend payloads until the server
  accepts storage table UID everywhere.

Not allowed as final behavior:

- DataNode runtime computes storage identity.
- PersistManager creates storage as a hidden side effect of updater creation.
- Table schema is split between DataNode config and SourceTableConfiguration.
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
creation or use a deprecated compatibility path.

## Risks

- Direct `DataNodeStorage(MetaTable)` inheritance can break Pydantic validation
  if backend dynamic-table payloads do not include required MetaTable fields.
- Import cycles will occur unless shared data-source/client models are split
  first.
- Inherited MetaTable class methods may expose invalid DataNodeStorage command
  surfaces unless they are moved, guarded, or overridden.
- Existing docs and CLI commands heavily use `DataNodeStorage`; they must be
  migrated carefully to avoid confusing users.
- Using `MetaTable.uid` in update hashing would make update hashes less portable
  across environments. Prefer storage hash plus data-source UID unless an
  environment-specific hash is explicitly desired.
- Backend routes may continue to distinguish MetaTable and DynamicTableMetaData
  longer than the client model does, requiring compatibility wrappers.

## Acceptance Criteria

The migration is complete when:

- `DataNodeStorage` inherits from the MetaTable client model without import
  cycles.
- DataNode-produced storage tables expose canonical MetaTable fields and UID
  identity.
- New DataNode update runtime construction requires an explicit MetaTable
  storage argument.
- DataNode update runtime no longer computes `storage_hash`.
- `PersistManager` does not create storage in the normal path.
- Update records link to storage by MetaTable UID.
- DataFrame output validation uses the MetaTable table contract.
- SourceTableConfiguration no longer owns structural schema.
- `mainsequence.tdag.DataNodeUpdate` is the documented runtime authoring class,
  or the remaining naming blocker is explicitly tracked.
- DataNode tutorials show:

  ```python
  storage_table = MetaTable(...)
  data_node_update = DataNodeUpdate(config, storage_table)
  ```

- Compatibility aliases are covered by tests and emit deprecation warnings where
  practical.
