---
name: mainsequence-data-nodes
description: Use this skill when the task is about producing, changing, validating, or reviewing Main Sequence DataNodes. This skill owns DataNode contracts, hashing, namespaces, update logic, metadata, asset-indexed nodes, and DataNode validation. It does not own MetaTable registration, API route contracts, scheduling, or sharing policy.
---

# Main Sequence Data Nodes

## Overview

Use this skill when the task changes a DataNode producer or the published table contract behind it.

This skill is for producer-side table engineering.

## This Skill Can Do

- create a new `DataNode`
- modify an existing `DataNode`
- review whether a DataNode change is breaking or non-breaking
- define or refactor `DataNodeConfiguration`
- classify config fields into dataset meaning, updater scope, and hash-excluded metadata
- implement or review:
  - `dependencies()`
  - `update()`
  - `get_asset_list()`
  - `RecordDefinition` declarations and metadata
  - DataNode source-table foreign key declarations to MetaTables
- design single-index or `(time_index, unique_identifier)` MultiIndex outputs
- define namespace-first validation strategy
- write or review DataNode smoke tests
- decide whether a consumer should use `APIDataNode`

## This Skill Must Not Claim

This skill must not claim ownership of:

- MetaTable registration or governed operation semantics
- HTTP route design or FastAPI response contracts
- workspace/widget layout payloads
- job creation, scheduling, image pinning, or release creation
- RBAC or sharing policy
- domain strategy semantics

If the task depends on one of those areas, route it explicitly instead of guessing.

If the user is still in the discovery process and does not yet know what data exists on the platform, use the exploration skill first and return here after discovery is complete.

## Route Adjacent Work

- discovery-only data inventory before DataNode implementation:
  `.agents/skills/mainsequence/data_access/exploration/SKILL.md`
- MetaTables:
  `.agents/skills/mainsequence/data_publishing/meta_tables/SKILL.md`
- APIs and FastAPI:
  `.agents/skills/mainsequence/application_surfaces/api_surfaces/SKILL.md`
- Command Center workspaces:
  `.agents/skills/mainsequence/command_center/workspace_builder/SKILL.md`
- AppComponents and custom forms:
  `.agents/skills/mainsequence/command_center/app_components/SKILL.md`
- Jobs, images, resources, and releases:
  `.agents/skills/mainsequence/platform_operations/orchestration_and_releases/SKILL.md`
- RBAC and sharing:
  `.agents/skills/mainsequence/platform_operations/access_control_and_sharing/SKILL.md`
## Read First

1. `docs/tutorial/creating_a_simple_data_node.md`
2. `docs/knowledge/data_nodes.md`

## Inputs This Skill Needs

Before changing code, collect or infer:

- dataset meaning
- intended published identifier
- expected index shape
- expected columns and dtypes, declared as `RecordDefinition`
- whether declared columns should reference registered MetaTables through foreign keys
- upstream dependencies
- whether the node is asset-indexed
- first-run or backfill bounds
- whether the change must preserve the existing table contract

If one of these is unknown and changes the contract, stop and resolve it before implementation.

## Required Decisions

For every non-trivial DataNode task, make these decisions explicitly:

1. Is this a new dataset or the same dataset?
2. Is the change changing dataset meaning or only updater scope?
3. Is the identifier collision-safe in this organization?
4. Is the node single-index or MultiIndex?
5. Does the first validation run happen in a namespace?

## Build Rules

### 1. Treat the DataNode as a published product

The following are contract-level decisions:

- identifier
- schema
- index shape
- semantic meaning of the table

Do not change them casually.

### 2. Keep meaning separate from scope

- dataset meaning belongs in table identity
- updater scope belongs in updater identity
- descriptive metadata belongs in `hash_excluded` fields
- runtime knobs belong outside hashed identity

Do not mix these.

### 3. `hash_namespace` is isolation only

Use `hash_namespace(...)` or `test_node=True` for:

- namespaced tests
- isolated experimentation
- shared-backend safety

Do not use namespace to encode business meaning.

### 4. `update()` should be incremental by default

Use `UpdateStatistics`.

Do not fetch or return full history every run unless there is a documented reason.

### 5. Dependencies must be deterministic

Dependencies belong in constructor setup and `dependencies()`.

Do not construct dependency graphs dynamically inside `update()`.

### 6. Asset-indexed nodes must behave like asset-indexed nodes

If the node emits `(time_index, unique_identifier)`:

- `unique_identifier` should represent an Asset identity
- `get_asset_list()` must reflect the effective updater asset scope
- missing assets should be resolved or registered when required by the workflow

### 7. `RecordDefinition` is the canonical schema surface

Every new or materially edited `DataNodeConfiguration` must declare output
records with `RecordDefinition` unless there is a documented compatibility
reason not to. Treat `records` as the canonical table schema declaration, not
as optional UI metadata.

The canonical pattern is:

```python
from pydantic import Field

from mainsequence.tdag import DataNodeConfiguration, RecordDefinition


class PricesConfig(DataNodeConfiguration):
    records: list[RecordDefinition] = Field(
        default_factory=lambda: [
            RecordDefinition(
                column_name="price",
                dtype="float64",
                label="Price",
                description="Observed price.",
            )
        ]
    )
```

Rules:

- `column_name` and `dtype` are structural and define the persisted record contract.
- `label` and `description` are descriptive discovery metadata and must not be treated as runtime controls.
- The DataFrame returned by `update()` must match declared `records`.
- Do not invent another schema object or parallel record declaration.
- Prefer `DataNodeConfiguration.records` over overriding `get_column_metadata()` for normal nodes.

### DataNode table metadata is discovery-critical

Every production `DataNodeConfiguration` should set `node_metadata` with
`DataNodeMetaData`. This is not decorative metadata. The `description` is used
for embedding-based data discovery, so it must be written as a useful dataset
description rather than a vague one-line label.

Good `DataNodeMetaData.description` values should describe:

- what real-world entity or process the dataset represents
- the row grain and identity dimensions
- the important measures or columns
- the time coverage and expected update cadence when known
- the source, assumptions, caveats, and intended analytical use
- common search terms a user might use to find this dataset

Keep `identifier` short and stable. Make `description` rich enough that a user
searching semantically for the dataset can find it through the embedding model.
Do not use the description for runtime controls or configuration.

### 8. Use `SourceTableForeignKey` when a DataNode references a MetaTable

When a DataNode source table has a column that should reference a registered
MetaTable, declare that relationship in `DataNodeConfiguration.foreign_keys`.
Do this only for DataNode source-table to MetaTable relationships. Do not
invent DataNode-to-DataNode or MetaTable-to-DataNode foreign keys.

The canonical pattern is:

```python
from pydantic import Field

from mainsequence.tdag import (
    DataNodeConfiguration,
    RecordDefinition,
    SourceTableForeignKey,
)


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

Rules:

- `SourceTableForeignKey` is the authoring model; do not hand-author
  `SourceTableForeignKeyContract` in DataNode configs.
- `source_columns` should reference the same `RecordDefinition` objects listed
  in `records`.
- `target_columns` should use MetaTable/SQLAlchemy column references such as
  `Asset.uid`, not backend UID strings.
- Do not ask users to provide FK names.
- Do not ask users to provide `target_meta_table_uid`; the SDK resolves the
  target MetaTable public `uid`.
- FK hash material is source column names, target MetaTable public `uid`,
  target column names, and `on_delete`.
- FK hash material must not include generated names, backend database primary
  keys, source-table FK row UIDs, backend projection/enforcement fields, target
  storage hashes, or Python object/class repr values.
- If FK target registration or MetaTable ownership is unclear, route to
  `.agents/skills/mainsequence/data_publishing/meta_tables/SKILL.md`.

### 9. Metadata is still required for production-quality nodes

When the node is not a throwaway example, also provide table metadata through
`DataNodeConfiguration.node_metadata` when a stable published identifier or
description is needed.

Use `json_schema_extra={"hash_excluded": True}` for descriptive metadata that
must not rotate `update_hash` or `storage_hash`. Keep the older `runtime_only`
marker only for legacy compatibility.

## Review Rules

When reviewing an existing DataNode, look for:

- identifier collisions
- accidental schema breaks
- wrong meaning/scope/hash-excluded split
- missing `RecordDefinition` declarations
- missing or incorrectly authored `SourceTableForeignKey` declarations when a
  DataNode column references a MetaTable
- misuse of `hash_namespace`
- non-incremental `update()` behavior
- hidden dependency creation inside `update()`
- invalid asset-indexed output shape
- missing metadata on a production node

## Validation Checklist

Do not claim success until you have checked:

- the relevant docs were read first
- the identifier choice is intentional
- config fields are classified correctly
- `DataNodeConfiguration.records` is present for new or materially edited nodes
- declared `RecordDefinition` names and dtypes match the DataFrame returned by `update()`
- any DataNode-to-MetaTable relationships use `SourceTableForeignKey` with
  source record references and target column references
- `dependencies()` is deterministic
- `update()` is incremental
- the DataFrame shape is valid
- the first validation run is namespaced

For asset-indexed nodes, also check:

- `get_asset_list()` is correct
- no duplicate `(time_index, unique_identifier)` rows are emitted
- assets exist or are registered idempotently when needed

## This Skill Must Stop And Escalate When

- the change may break an existing published table contract and the versioning decision is unclear
- the intended identifier is likely to collide and no naming decision was made
- the node needs asset identities but the asset-resolution strategy is unclear
- the task is actually an API, MetaTable, orchestration, or sharing problem
- docs and code disagree on hashing or runtime behavior

Do not guess through contract changes.
