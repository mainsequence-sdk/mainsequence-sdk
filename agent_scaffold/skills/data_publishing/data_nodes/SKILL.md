---
name: mainsequence-data-nodes
description: Use this skill when the task is about producing, changing, validating, or reviewing Main Sequence DataNode update processes. This skill owns DataNode update configuration, dependencies, update logic, hashing, namespaces, and validation against a PlatformTimeIndexMetaData storage contract. It does not own generic MetaTable governance, API route contracts, scheduling, sharing policy, or storage registration internals.
---

# Main Sequence Data Nodes

## Overview

Use this skill when the task changes a DataNode producer.

A DataNode is an update process. It is not the canonical storage model. Storage
is defined by a registered `PlatformTimeIndexMetaData` SQLAlchemy model.

Canonical workflow:

1. Define a `PlatformTimeIndexMetaData` storage class.
2. Register or bind that storage class before constructing the DataNode.
3. Construct the DataNode with `config=...` and `storage_table=StorageClass`.
4. Return a DataFrame from `update()` that matches the storage class contract.

## This Skill Can Do

- create a new `DataNode` update process
- modify an existing `DataNode` update process
- review whether a DataNode change affects update identity or table contract
- define or refactor `DataNodeConfiguration`
- classify config fields into update identity and hash-excluded descriptive metadata
- implement or review:
  - `dependencies()`
  - `update()`
  - `prepare_update_statistics()`
  - `get_asset_list()` when the update is asset scoped
- design single-index or `(time_index, unique_identifier)` DataFrame outputs
- validate output shape against a `PlatformTimeIndexMetaData` storage contract
- define explicit `hash_namespace(...)` validation strategy
- write or review DataNode smoke tests
- decide whether a consumer should use `APIDataNode`

## This Skill Must Not Claim

This skill must not claim ownership of:

- generic MetaTable registration or governed operation semantics
- storage creation inside `DataNode` or `PersistManager`
- HTTP route design or FastAPI response contracts
- workspace/widget layout payloads
- job creation, scheduling, image pinning, or release creation
- RBAC or sharing policy
- domain strategy semantics

If the task depends on one of those areas, route it explicitly instead of guessing.

## Route Adjacent Work

- MetaTables and storage contracts:
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
3. `docs/knowledge/meta_tables/sqlalchemy.md`

## Inputs This Skill Needs

Before changing code, collect or infer:

- dataset meaning
- registered `PlatformTimeIndexMetaData` storage class or the class to create
- expected time index and identity index shape
- expected columns and dtypes from the storage class
- upstream dependencies
- whether the update is asset scoped
- first-run or backfill bounds
- whether the change must preserve the existing table contract

If one of these is unknown and changes the contract, stop and resolve it before
implementation.

## Required Decisions

For every non-trivial DataNode task, make these decisions explicitly:

1. Is this a new dataset or the same dataset?
2. Is this change storage-contract work or update-process work?
3. Is the storage class already registered or should the MetaTable skill handle it?
4. Is the node single-index or MultiIndex?
5. Does the first validation run happen under an explicit `hash_namespace(...)`?

## Build Rules

### 1. Treat Storage As A PlatformTimeIndexMetaData Contract

The following are storage-contract decisions:

- table namespace and identifier
- SQLAlchemy columns and dtypes
- time index name
- identity index names
- foreign keys
- table description and labels

Do not put those concerns in `DataNodeConfiguration`.

Minimal pattern:

```python
import datetime

from sqlalchemy import DateTime, Float, MetaData
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from mainsequence.meta_tables import PlatformTimeIndexMetaData


class Base(DeclarativeBase):
    metadata = MetaData()


class PricesTable(PlatformTimeIndexMetaData, Base):
    __metatable_namespace__ = "market_data"
    __metatable_identifier__ = "prices_daily"
    __time_index_name__ = "time_index"
    __index_names__ = ["time_index", "unique_identifier"]

    time_index: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    unique_identifier: Mapped[str] = mapped_column(nullable=False)
    close: Mapped[float] = mapped_column(Float, nullable=False)
```

Register or bind storage before constructing the DataNode:

```python
PricesTable.register(data_source_uid=data_source_uid)
```

or:

```python
PricesTable.bind_meta_table(existing_meta_table)
```

### 2. Keep DataNode As Update Logic

The DataNode constructor should accept:

- a `DataNodeConfiguration`
- a registered `storage_table: type[PlatformTimeIndexMetaData]`
- optional `hash_namespace`

Do not accept `test_node`. It has been removed. Use explicit
`hash_namespace(...)` or `hash_namespace="..."`.

Pattern:

```python
from mainsequence.meta_tables import DataNode, DataNodeConfiguration
from mainsequence.meta_tables import PlatformTimeIndexMetaData


class PricesConfig(DataNodeConfiguration):
    shard_id: str


class PricesUpdate(DataNode):
    def __init__(
        self,
        config: PricesConfig,
        storage_table: type[PlatformTimeIndexMetaData],
        *,
        hash_namespace: str | None = None,
    ):
        self.config = config
        super().__init__(
            config=config,
            storage_table=storage_table,
            hash_namespace=hash_namespace,
        )

    def dependencies(self):
        return {}

    def update(self):
        ...
```

### 3. Configuration Is Update-Scoped By Default

Every `DataNodeConfiguration` field participates in `update_hash` by default.

Do not use:

- `json_schema_extra={"update_only": True}`
- `json_schema_extra={"runtime_only": True}`
- `json_schema_extra={"ignore_from_storage_hash": True}`
- `_ARGS_IGNORE_IN_STORAGE_HASH`

Those are removed. The only supported opt-out is:

```python
Field(..., json_schema_extra={"hash_excluded": True})
```

Use `hash_excluded` only for descriptive metadata that must not affect update
identity. If a field changes output values, dependencies, source choice, or
updater scope, it must remain a normal config field.

### 4. `hash_namespace` Is Isolation Only

Use explicit `hash_namespace(...)` for:

- namespaced tests
- isolated experimentation
- shared-backend safety

Do not use namespace to encode business meaning.

Do not use `test_node=True`; it is not supported.

### 5. `update()` Should Be Incremental By Default

Use `UpdateStatistics`.

Do not fetch or return full history every run unless there is a documented
reason.

### 6. Dependencies Must Be Deterministic

Dependencies belong in constructor setup and `dependencies()`.

Do not construct dependency graphs dynamically inside `update()`.

### 7. Asset-Scoped Updates Must Be Explicit

If the node emits `(time_index, unique_identifier)`:

- `unique_identifier` should represent an Asset identity
- asset scope should live in update configuration when it affects the updater
- `get_asset_list()` must reflect the effective updater asset scope when the
  workflow uses that hook
- missing assets should be resolved or registered by the relevant workflow

### 8. Foreign Keys Belong To The Storage Contract

For new code, model foreign keys on the `PlatformTimeIndexMetaData` storage
class or route the storage-contract work to the MetaTable skill.

Do not add DataNode configuration fields just to mutate storage metadata.

### 9. Metadata Belongs To Storage

Production-quality table identifiers, descriptions, labels, column docs, and
foreign-key metadata belong to the storage class/MetaTable registration path.

Do not use `DataNodeMetaData` or `RecordDefinition` as the canonical schema
surface for new DataNode work.

## Review Rules

When reviewing an existing DataNode, look for:

- storage contract hidden in `DataNodeConfiguration`
- old `RecordDefinition` or `DataNodeMetaData` schema patterns
- `update_only`, `runtime_only`, or `ignore_from_storage_hash`
- `test_node=True`
- missing explicit `storage_table`
- accidental storage registration inside the DataNode
- wrong meaning/scope/hash-excluded split
- misuse of `hash_namespace`
- non-incremental `update()` behavior
- hidden dependency creation inside `update()`
- invalid asset-indexed output shape
- DataFrame columns that do not match the `PlatformTimeIndexMetaData` class

## Validation Checklist

Do not claim success until you have checked:

- the relevant docs were read first
- storage is a registered or bound `PlatformTimeIndexMetaData` class
- the DataNode constructor requires `storage_table`
- config fields are updater-scoped by default
- no removed hash metadata markers remain
- no `test_node` usage remains
- `dependencies()` is deterministic
- `update()` is incremental
- the DataFrame shape matches the storage class
- the first validation run uses explicit `hash_namespace(...)` when it touches a shared backend

For asset-scoped updates, also check:

- `get_asset_list()` is correct when used
- no duplicate `(time_index, unique_identifier)` rows are emitted
- assets exist or are registered idempotently when needed

## This Skill Must Stop And Escalate When

- the change may break an existing published table contract and the versioning decision is unclear
- the intended storage class or MetaTable registration path is unclear
- the node needs asset identities but the asset-resolution strategy is unclear
- the task is actually an API, MetaTable, orchestration, or sharing problem
- docs, skill instructions, and code disagree on hashing or runtime behavior

Do not guess through contract changes.
