# ADR 0014: DataNode Records and Foreign Keys

Date: 2026-05-26

Status: Proposed

## Related ADRs

- ADR 0002: Multidimensional DataNode Update Contract
- ADR 0006: TDAG Public UID Identifiers
- ADR 0011: DuckDB N-Dimensional Row Identity
- ADR 0013: Local SQLite DataSource Support

## Context

The SDK already has a DataNode record declaration:

```python
class RecordDefinition(BaseModel):
    column_name: str
    dtype: str
    label: str | None = None
    description: str | None = None
```

In the current implementation, descriptive metadata is excluded from hashing
through `json_schema_extra={"runtime_only": True}`. That marker name is
misleading for these fields: they are not runtime controls. They are
publication, discovery, and UI metadata. The behavior we need is hash exclusion
for descriptive metadata, not "runtime-only" semantics.

This applies to both column metadata and table metadata:

- `RecordDefinition.label`
- `RecordDefinition.description`
- `DataNodeMetaData.identifier`
- `DataNodeMetaData.description`
- `DataNodeMetaData.data_frequency_id`

And the base DataNode configuration already exposes it:

```python
class DataNodeConfiguration(BaseConfiguration):
    records: list[RecordDefinition] | None = None
```

The base `DataNode.get_column_metadata()` already bridges those records into
backend `ColumnMetaData`:

```python
def get_column_metadata(self) -> list[ColumnMetaData] | None:
    config = self._get_data_node_configuration()
    records = getattr(config, "records", None) if config is not None else None
    if not records:
        return None

    return [
        ColumnMetaData(
            column_name=record.column_name,
            dtype=record.dtype,
            label=record.label or record.column_name,
            description=record.description or "",
        )
        for record in records
    ]
```

So this ADR must not introduce a parallel schema or record model.

The issue is that `records` currently only feed column metadata after the
update flow. They do not initialize `SourceTableConfiguration`, and they do not
provide foreign keys.

Today, the default source table creation path is still DataFrame-first:

1. `DataNode.update()` returns a DataFrame.
2. `BasePersistManager.persist_updated_data(...)` calls
   `DataNodeUpdate.upsert_data_into_table(...)`.
3. `DataNodeUpdate.upsert_data_into_table(...)` calls
   `_break_pandas_dataframe(...)`, which resets the index and infers
   `column_dtypes_map` from pandas dtypes.
4. `DataNodeUpdate.upsert_data_into_table(...)` calls
   `DataNodeStorage.handle_source_table_configuration_creation(...)`.
5. `handle_source_table_configuration_creation(...)` creates
   `SourceTableConfiguration` from the observed DataFrame.

Foreign keys cannot rely on that late inference. A FK-enabled DataNode needs a
stable source table contract before the first write:

- source columns must be declared;
- source dtypes must be declared;
- `time_index_name` and `index_names` must be known;
- target references must be resolved;
- schema/FK conflicts must be caught before data insertion.

The backend already exposes a schema-only initialization endpoint:

```http
POST /orm/api/ts_manager/dynamic_table/<dynamic_table_uid>/initialize-source-table/
```

The client wrapper is `DataNodeStorage.initialize_source_table(...)`. It
currently posts:

```json
{
  "time_index_name": "time_index",
  "index_names": ["time_index", "account_uid", "unique_identifier"],
  "column_dtypes_map": {
    "time_index": "datetime64[ns, UTC]",
    "account_uid": "uuid",
    "unique_identifier": "object"
  }
}
```

That endpoint creates or validates `SourceTableConfiguration` and creates the
physical backing table without inserting a bootstrap row.

## Decision

Use the existing `DataNodeConfiguration.records` as the DataNode source table
record declaration.

Do not add `DataNodeSchema`.
Do not add another record model.

Add foreign keys as an optional field next to `records` on
`DataNodeConfiguration`:

```python
SourceTableForeignKeyTarget = type[PlatformManagedMetaTable] | MetaTable
SourceColumnRef = RecordDefinition
TargetColumnRef = InstrumentedAttribute | Column  # for example Asset.uid


class SourceTableForeignKey(BaseModel):
    target: SourceTableForeignKeyTarget = Field(
        ...,
        description="Authored MetaTable target reference.",
    )
    source_columns: list[SourceColumnRef] = Field(
        ...,
        description="Authored DataNode record references used as FK source columns.",
    )
    target_columns: list[TargetColumnRef] = Field(
        ...,
        description="Authored MetaTable column references used as FK target columns.",
    )
    on_delete: str = Field(
        default="restrict",
        description="Authored delete action.",
    )


class SourceTableForeignKeyContract(BaseModel):
    source_columns: list[str]
    target_meta_table_uid: str
    target_columns: list[str]
    on_delete: str = "restrict"


class DataNodeConfiguration(BaseConfiguration):
    records: list[RecordDefinition] | None = None
    foreign_keys: list[SourceTableForeignKey] | None = None
```

`RecordDefinition.column_name` and `RecordDefinition.dtype` are structural.
They participate in hashing today and should continue to define the storage
contract.

`RecordDefinition.label` and `RecordDefinition.description` are descriptive
metadata. They must not affect either `update_hash` or `storage_hash`, but they
should not be described as runtime-only fields.

The same naming correction applies to `DataNodeMetaData`: identifier,
description, and data frequency are publication/discovery metadata that should
remain hash-excluded, not runtime-only.

Foreign key declarations are structural. Adding, removing, or changing a FK
should affect `storage_hash`.

The public FK declaration is authoring intent, not the backend persistence
payload. Users must declare the relationship columns, but they should not
provide FK names or backend target UIDs. The SDK resolves target UIDs only when
building the serializer contract; the backend derives FK names.

`SourceTableForeignKey` and `SourceTableForeignKeyContract` are different
concepts:

- `SourceTableForeignKey` is the user/config authoring model. It is like
  `RecordDefinition`: it declares structural storage intent and participates
  in `storage_hash`.
- `SourceTableForeignKeyContract` is the resolved serializer request payload
  sent to `initialize-source-table/`. It exists at the backend API boundary and
  does not appear in user config.

`SourceTableForeignKey` hash material is the normalized authored structure:

- source column names resolved from `RecordDefinition` references;
- target MetaTable public `uid`, resolved from the authored `target` reference;
- target column names resolved from MetaTable column references;
- `on_delete`.

The target MetaTable `uid` is the only target-table identity that participates
in the FK hash. This follows the same normalization rule used when hashing
Pydantic/BaseObjectOrm references: hash the stable object identity value, not
the Python object, generated payload, or backend projection.

The hash material must not include generated FK names, backend database primary
keys, source-table FK row UIDs, backend enforcement/projection fields, target
storage hashes, or object/class repr values. The resolved
`target_meta_table_uid` in `SourceTableForeignKeyContract` is allowed because it
is the target MetaTable public `uid` in serializer form, not a generated FK
artifact.

Scope is intentionally narrow for the first implementation: only
`DynamicTableMetaData` / DataNode source tables may declare foreign keys that
target registered MetaTables. DataNode-to-DataNode foreign keys are not allowed
by this ADR and must remain out of the public API until a separate design
defines their target identity and enforcement semantics.

## Authoring Surface

Example:

```python
from pydantic import Field

from mainsequence.tdag import DataNodeConfiguration, RecordDefinition

ASSET_UID = RecordDefinition(
    column_name="asset_uid",
    dtype="uuid",
    label="Asset",
    description="Asset UID.",
)


class PricesConfig(DataNodeConfiguration):
    records: list[RecordDefinition] = Field(
        default_factory=lambda: [
            RecordDefinition(
                column_name="time_index",
                dtype="datetime64[ns, UTC]",
                label="Time",
                description="UTC observation timestamp.",
            ),
            ASSET_UID,
            RecordDefinition(
                column_name="price",
                dtype="float64",
                label="Price",
                description="Observed price.",
            ),
        ]
    )
    foreign_keys: list[SourceTableForeignKey] = Field(
        default_factory=lambda: [
            SourceTableForeignKey(
                target=Asset,
                source_columns=[ASSET_UID],
                target_columns=[Asset.uid],
                on_delete="restrict",
            )
        ]
    )
```

The user declares the source and target columns because those are the semantic
relationship. The SDK resolves `target=Asset` to the backend MetaTable UID and
the backend derives the FK name. The user still does not provide the FK name or
`target_meta_table_uid` at any authoring layer.

Target columns should be declared as target table column references, for
example `Asset.uid`, not strings. Source columns should preferably use the
same `RecordDefinition` object that appears in `records`; string source column
names are only a compatibility fallback for code that cannot hold a record
object reference cleanly.

The DataNode constructor already stores the config before backend verification.
That means `self.config.records` and `self.config.foreign_keys` are available
by the time `verify_and_build_remote_objects()` runs.

## Resolution

For a FK-enabled DataNode, the SDK resolves existing records and configured
foreign keys into the `initialize-source-table/` payload.

Resolution must produce:

- `time_index_name`;
- `index_names`;
- `column_dtypes_map`;
- `columns_metadata`;
- `foreign_keys`.

Resolution rules:

- `foreign_keys` requires `records`;
- every configured `index_names` entry must exist in `records`;
- every FK `source_columns` entry must resolve to a record in `records`;
- each `RecordDefinition.column_name` must be unique;
- `column_dtypes_map` is built from each record's `column_name` and `dtype`;
- `columns_metadata` is built by the existing `get_column_metadata()` bridge;
- each FK `target` must resolve to exactly one registered MetaTable;
- `target_meta_table_uid` is resolved by the SDK from the target MetaTable;
- every FK `target_columns` entry must resolve to a column on the target
  MetaTable;
- every FK must explicitly declare `source_columns` and `target_columns`;
- FK request payloads do not include `name`;
- the backend derives the FK name deterministically from the DataNode storage
  identity, source columns, and target identity, and can return it in a
  projection model;
- unresolved, empty, or ambiguous targets fail before any backend mutation.

Resolution converts `SourceTableForeignKey` authoring declarations into
`SourceTableForeignKeyContract` serializer payloads:

```python
SourceTableForeignKeyContract(
    source_columns=["asset_uid"],
    target_meta_table_uid="meta-table-asset-uid",
    target_columns=["uid"],
    on_delete="restrict",
)
```

That resolved FK contract is attached to source table initialization, not to
`DataNodeUpdate`. The user should not hand-author this serializer shape.

The source for `time_index_name` and `index_names` must be explicit. This ADR
does not introduce a new schema object for those fields. The implementation
should use the existing DataNode table/index configuration surface if one
exists; if one does not exist, add the smallest compatible fields to
`DataNodeConfiguration`, for example:

```python
time_index_name: str = "time_index"
index_names: list[str] | None = None
```

If a FK-enabled DataNode cannot resolve `time_index_name`, `index_names`,
`records`, and `foreign_keys` before the first write, backend verification must
fail with a clear error instead of waiting for DataFrame inference.

## Hot Path

This ADR must not move source table initialization to a new lifecycle point.

The `initialize-source-table/` call stays in the same source-table
initialization path where it is already used. The only change is that, when the
DataNode configuration has `records` and `foreign_keys`, the SDK resolves that
extra information before the existing call and sends a richer payload.

At the existing initialization point, the flow is:

1. Read `self.config.records` and `self.config.foreign_keys`.
2. Build the existing source table fields:
   `time_index_name`, `index_names`, and `column_dtypes_map`.
3. If `records` are present, derive `columns_metadata` through the existing
   `get_column_metadata()` bridge.
4. If `foreign_keys` are present, resolve them into
   `SourceTableForeignKeyContract` serializer payloads.
5. Call the existing endpoint:

   ```http
   POST /orm/api/ts_manager/dynamic_table/<dynamic_table_uid>/initialize-source-table/
   ```

6. The payload is the same existing payload plus optional `columns_metadata`
   and optional `foreign_keys`.

Runtime writes still go through `DataNodeUpdate.upsert_data_into_table(...)`.
This ADR enriches source table initialization; it does not redefine when source
table initialization happens.

## Backend Behavior

The backend `initialize-source-table/` endpoint should:

- continue accepting the existing payload;
- accept optional `columns_metadata`;
- accept optional `foreign_keys`;
- validate every FK `source_columns` entry against `column_dtypes_map`;
- resolve and validate FK targets;
- accept only resolved `SourceTableForeignKeyContract` serializer payloads, not
  `SourceTableForeignKey` user authoring declarations;
- derive FK names server-side and expose them through returned FK projections;
- store declared FK metadata with the source table configuration or adjacent
  source-table FK rows;
- create physical constraints where supported;
- return declared FK metadata and enforcement status;
- return a conflict if an existing source table has incompatible schema or FK
  metadata.

Physical enforcement can be backend-dependent:

- remote SQL-backed tables should create physical constraints when possible;
- SQLite can enforce FKs when both tables are in the same local database;
- DuckDB/Parquet may initially store FK metadata and optionally validate writes;
- all backends must report whether each FK is physically enforced or
  metadata-only.

## Backward Compatibility

This change must be additive.

Existing behavior remains valid:

- source table initialization is called in the same lifecycle location as
  before;
- DataFrame-first schema inference remains supported;
- existing `DataNodeConfiguration.records` behavior remains valid;
- existing `get_column_metadata()` behavior remains valid;
- the existing `initialize_source_table(...)` payload remains valid without
  `columns_metadata` or `foreign_keys`;
- existing `SourceTableConfiguration.create(...)` behavior remains valid for
  inferred schemas;
- existing DataNode pickles and serialized configurations remain readable.

## Documentation Requirements

The documentation must be explicit that first-version DataNode foreign keys are
only:

```text
DynamicTableMetaData/DataNode source table -> MetaTable
```

The docs must also state what is not supported:

- DataNode-to-DataNode foreign keys;
- MetaTable-to-DataNode foreign keys;
- user-authored FK names;
- user-authored `target_meta_table_uid` for normal MetaTable targets.

Examples should declare the MetaTable target and the relationship columns:

```python
ASSET_UID = RecordDefinition(column_name="asset_uid", dtype="uuid")

foreign_keys = [
    SourceTableForeignKey(
        target=Asset,
        source_columns=[ASSET_UID],
        target_columns=[Asset.uid],
    )
]
```

## Non-Negotiable Rules

- Do not introduce `DataNodeSchema`.
- Do not introduce a parallel record declaration.
- Use existing `DataNodeConfiguration.records`.
- Do not require users to provide FK names.
- Do not require users to provide `target_meta_table_uid` for MetaTable
  targets.
- Only support DynamicTableMetaData/DataNode source tables targeting MetaTables
  in this ADR.
- Do not expose DataNode-to-DataNode foreign keys in this ADR.
- Do not remove the DataFrame-inferred fallback.
- Do not require all DataNodes to predefine records.
- Do not special-case `unique_identifier`; row identity remains the full
  configured `index_names` tuple.
- Do not silently ignore schema or FK conflicts on an existing source table.
- Do not claim physical FK enforcement for backends that only store metadata.
- Keep FK metadata attached to source table schema, not to `DataNodeUpdate`.

## Implementation Tasks

- [x] Add `SourceTableForeignKey`.
- [x] Reuse or extend the existing `SourceTableForeignKeyContract` as the
      serializer request contract.
- [x] Add optional `foreign_keys: list[SourceTableForeignKey] | None` to
      `DataNodeConfiguration`.
- [x] Model `SourceTableForeignKey` as hashable authoring intent: target
      MetaTable reference plus explicit source record references, target table
      column references, and delete behavior.
- [x] Ensure `SourceTableForeignKey` hash material includes resolved source
      column names, target MetaTable public `uid`, resolved target column
      names, and `on_delete`.
- [x] Ensure `SourceTableForeignKey` hash material excludes generated FK names,
      backend database primary keys, source-table FK row UIDs, backend
      enforcement/projection fields, target storage hashes, and object/class
      repr values.
- [x] Model `SourceTableForeignKeyContract` as the resolved serializer request
      contract with source column names, resolved target MetaTable UID, target
      column names, and delete behavior.
- [x] Reuse existing `DataNodeConfiguration.records` as the record declaration
      for FK-enabled DataNodes.
- [x] Replace or alias the misleading `runtime_only` marker for descriptive
      metadata fields with a metadata-specific hash exclusion concept.
- [x] Confirm `RecordDefinition.label` and `RecordDefinition.description` do
      not affect `update_hash` or `storage_hash`.
- [x] Confirm `DataNodeMetaData.identifier`,
      `DataNodeMetaData.description`, and
      `DataNodeMetaData.data_frequency_id` do not affect `update_hash` or
      `storage_hash`.
- [x] Identify the existing source for `time_index_name` and `index_names`, or
      add the smallest compatible fields to `DataNodeConfiguration`.
- [x] Add FK target resolution helpers for MetaTable uid/object/model-class
      references.
- [x] Validate source column references against `DataNodeConfiguration.records`.
- [ ] Validate target column references against the resolved MetaTable
      contract.
- [x] Add FK resolution helpers that produce the resolved backend
      `SourceTableForeignKeyContract` payload from existing records and
      configured FK intent.
- [ ] Add source table initialization helpers that produce `time_index_name`,
      `index_names`, `column_dtypes_map`, and `columns_metadata` from existing
      records and table/index configuration.
- [x] Add an optional `columns_metadata` parameter to
      `DataNodeStorage.initialize_source_table(...)` if the backend endpoint
      needs it.
- [x] Add an optional `foreign_keys` parameter to
      `DataNodeStorage.initialize_source_table(...)`.
- [x] Include optional `columns_metadata` and `foreign_keys` in
      `_initialize_source_table_at_url(...)` payload serialization.
- [ ] Extend the backend `initialize-source-table/` endpoint to accept
      `columns_metadata` and `foreign_keys`.
- [ ] Derive deterministic FK names on the backend and return them through
      source table FK projections.
- [ ] Persist declared DataNode FK metadata on or next to
      `SourceTableConfiguration`.
- [ ] Return declared FK metadata and enforcement status from the endpoint.
- [ ] Add backend conflict checks for incompatible existing source table schema
      and FK definitions.
- [x] Preserve the existing call site for source table initialization and
      enrich that existing payload with optional `columns_metadata` and
      `foreign_keys`.
- [x] Keep the same existing behavior when `foreign_keys` is absent.
- [x] Add tests proving the current no-FK behavior is unchanged.
- [x] Add tests proving FK payloads are posted to
      `/initialize-source-table/`.
- [x] Add tests proving users do not need to provide FK names or
      `target_meta_table_uid`.
- [x] Add tests proving FK names are absent from the client request contract
      and returned only by backend projections.
- [ ] Add tests proving target columns can be declared as MetaTable column
      references such as `Asset.uid`.
- [x] Add tests proving source columns can be declared as `RecordDefinition`
      object references.
- [ ] Add tests for missing records, missing index columns, duplicate records,
      missing FK source columns, missing FK target columns, unresolved targets,
      incompatible target columns, generated FK names, and conflicting existing
      FK metadata.
- [ ] Add local backend tests documenting physical enforcement versus
      metadata-only behavior.
- [x] Document DataNode FK declarations and backend FK enforcement
      capabilities.
- [x] Document explicitly that only DynamicTableMetaData/DataNode-to-MetaTable
      foreign keys are supported in this ADR.

## Open Questions

- Should `foreign_keys` live on base `DataNodeConfiguration`, or only on
  concrete config subclasses with the base SDK reading it when present?
- Should metadata-only FK backends validate references during writes, or only
  expose the relationship for discovery and query planning?
