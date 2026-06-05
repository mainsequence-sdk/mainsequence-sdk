---
name: mainsequence-data-nodes
description: Use this skill when the task is about producing, changing, validating, or reviewing Main Sequence DataNode update processes. This skill owns DataNode update configuration, dependencies, update logic, hashing, namespaces, and validation against a PlatformTimeIndexMetaTable storage contract. It does not own generic MetaTable governance, Alembic migration execution, API route contracts, scheduling, sharing policy, or storage registration internals.
---

# Main Sequence Data Nodes

## Overview

Use this skill when the task changes a DataNode producer.

A DataNode is an update process. It is not the canonical storage model. Storage
is defined by a `PlatformTimeIndexMetaTable` SQLAlchemy model. Physical schema
evolution is handled with Alembic, not a DataNode storage subclass.

Canonical workflow:

1. Define a `PlatformTimeIndexMetaTable` storage class.
2. Construct the DataNode with `config=...` and `storage_table=StorageClass`.
3. Let the SDK register the output storage class automatically when needed.
4. Return a DataFrame from `update()` that matches the storage class contract.

## This Skill Can Do

- create a new `DataNode` update process
- modify an existing `DataNode` update process
- review whether a DataNode change affects update identity or table contract
- define or refactor `DataNodeConfiguration`
- classify values as hashed config fields or non-config class/runtime values
- implement or review:
  - `dependencies()`
  - `update()`
  - `prepare_update_statistics()`
- design single-index or multidimensional time-first DataFrame outputs
- validate output shape against a `PlatformTimeIndexMetaTable` storage contract
- define explicit `hash_namespace(...)` validation strategy
- write or review DataNode smoke tests
- decide whether a consumer should use `APIDataNode`

## This Skill Must Not Claim

This skill must not claim ownership of:

- generic MetaTable registration or governed operation semantics
- storage registration internals beyond the SDK lifecycle call
- HTTP route design or FastAPI response contracts
- workspace/widget layout payloads
- job creation, scheduling, image pinning, or release creation
- RBAC or sharing policy
- domain strategy semantics
- ad hoc storage creation or registration inside `update()`

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

1. `docs/knowledge/data_nodes.md`
2. `docs/knowledge/meta_tables/sqlalchemy.md`

## Inputs This Skill Needs

Before changing code, collect or infer:

- dataset meaning
- `PlatformTimeIndexMetaTable` output storage class, or the class to create
- expected time index and identity index shape
- expected columns and dtypes from the storage class
- upstream dependencies
- first-run or backfill bounds
- whether the change must preserve the existing table contract

If one of these is unknown and changes the contract, stop and resolve it before
implementation.

## Required Decisions

For every non-trivial DataNode task, make these decisions explicitly:

1. Is this a new dataset or the same dataset?
2. Is this change storage-contract work or update-process work?
3. Is the storage class contract correct, or should the MetaTable skill handle it?
4. Is the node single-index or MultiIndex?
5. Does the first validation run happen under an explicit `hash_namespace(...)`?

## Build Rules

### 1. Treat Storage As A TimeIndex MetaTable Contract

The following are storage-contract decisions:

- table namespace and identifier
- SQLAlchemy columns and dtypes
- time index name
- identity index names
- foreign keys
- table description and labels

Every storage class must include `__metatable_description__`. The description
should explain the table's intention, row grain, and downstream use, not only
list columns or schema mechanics.

Every `mapped_column(...)` in a storage class must include
`info={"label": ..., "description": ...}`. The column description must explain
what the value means for this dataset and how downstream users should interpret
it; do not merely repeat the column name or dtype.

Use `__metatable_extra_hash_components__` on storage classes when distinct
DataNode storage tables could otherwise have the same storage-relevant shape.
For example, two one-index daily tables with one float column need a stable
component such as `{"storage_name": "daily_random_number"}` versus
`{"storage_name": "daily_random_addition"}`.

This is storage identity. Changing it creates a different storage table. Do not
use it for labels, descriptions, runtime options, test isolation, backend UIDs,
data-source UIDs, or updater scope.

Do not put those concerns in `DataNodeConfiguration`.

Minimal pattern:

```python
import datetime

from sqlalchemy import DateTime, Float, MetaData
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from mainsequence.meta_tables import PlatformTimeIndexMetaTable, schema_table_name

PROJECT_NAME = "<project_name>"
PRICES_TABLE_NAME = schema_table_name(PROJECT_NAME, "prices")


class Base(DeclarativeBase):
    metadata = MetaData()


class PricesTable(PlatformTimeIndexMetaTable, Base):
    __tablename__ = PRICES_TABLE_NAME
    __metatable_namespace__ = "<domain_namespace>"
    __metatable_identifier__ = "<project_name>.<table_identifier>"
    __metatable_extra_hash_components__ = {"storage_name": "<stable_storage_name>"}
    __metatable_description__ = (
        "Daily close prices keyed by asset unique identifier for portfolio and "
        "risk analytics."
    )
    __time_index_name__ = "time_index"
    __cadence__ = "1d"
    __index_names__ = ["time_index", "unique_identifier"]

    time_index: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        info={
            "label": "Time Index",
            "description": "UTC timestamp for the close-price observation.",
        },
    )
    unique_identifier: Mapped[str] = mapped_column(
        nullable=False,
        info={
            "label": "Unique Identifier",
            "description": "Stable asset identifier for the observed price.",
        },
    )
    close: Mapped[float] = mapped_column(
        Float,
        nullable=False,
        info={
            "label": "Close",
            "description": "Daily close price used by portfolio and risk analytics.",
        },
    )
```

Storage registration is migration-first. Add the storage model to the
MetaTable migration provider and run `mainsequence migrations upgrade --provider
... head`. Do not call `PricesTable.register()` directly and do not rely on
DataNode construction to register storage tables.

`__index_names__` is the full DataNode storage grain. `PlatformTimeIndexMetaTable`
automatically adds a SQLAlchemy unique index over that tuple before Alembic
autogenerate runs. Do not manually repeat the full grain unique index in
`__table_args__`; add ordinary SQLAlchemy `Index(...)` entries only for
additional lookup/performance paths.

When the dataset has a known stable observation interval, declare `__cadence__`
on the `PlatformTimeIndexMetaTable` model, for example `1m`, `5m`, `1h`, `1d`,
`1w`, `1mo`, `1q`, or `1y`. Cadence is table metadata and should be included
whenever possible; do not make it a DataNode runtime configuration field unless
changing it actually changes the produced dataset identity.

`PlatformTimeIndexMetaTable.register()` remains SDK plumbing for the migration
workflow. Do not manually attach an existing UID, reconstruct a generic
`MetaTable`, or use manual bind helpers as an authoring step.

### 2. Keep DataNode As Update Logic

The DataNode constructor should accept:

- a `DataNodeConfiguration`
- a `storage_table: type[PlatformTimeIndexMetaTable]`
- optional `hash_namespace`

The constructor `storage_table` is the output storage contract. Keep it out of
`DataNodeConfiguration`. The storage class must already be registered by the
migration workflow before the node is constructed or run.

If the DataNode needs to select another DataNode's storage table as a
dependency, put that dependency storage reference in the config as
`type[PlatformTimeIndexMetaTable]`. Do not add an extra constructor argument for
dependency storage tables. Config values of this type are hashed by the bound
`TimeIndexMetaTable.uid` from `StorageClass.get_time_index_meta_table()`. If
the class is not yet bound, config serialization must fail and tell the user to
run the migration workflow.

Do not accept `test_node`. It has been removed. Use explicit
`hash_namespace(...)` or `hash_namespace="..."`.

Pattern:

```python
from typing import ClassVar

from pydantic import Field

from mainsequence.meta_tables import DataNode, DataNodeConfiguration
from mainsequence.meta_tables import PlatformTimeIndexMetaTable


class PricesConfig(DataNodeConfiguration):
    shard_id: str = Field(
        ...,
        description="Stable updater partition for this price job.",
        examples=["us_equities_daily"],
    )
    reference_dimension: ClassVar[str] = "unique_identifier"


class PricesUpdate(DataNode):
    def __init__(
        self,
        config: PricesConfig,
        storage_table: type[PlatformTimeIndexMetaTable],
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
Declare values that change output values, dependencies, source choice, or
updater scope as normal config fields.

Every config field must be declared with `Field(...)`. Include a clear
`description` and add `examples=[...]` whenever a realistic example helps. The
description must explain what the value means for the update process, not repeat
the Python type.

Values that must not affect `update_hash` should not be Pydantic config fields.
Use `ClassVar[...]` for class-level invariants and implementation constants, or
keep runtime controls in environment/runtime configuration outside the
DataNode config.

If a value genuinely must remain a Pydantic field while not affecting
`update_hash`, the only supported field-level opt-out is
`json_schema_extra={"hash_excluded": True}`:

```python
from pydantic import Field

from mainsequence.meta_tables import DataNodeConfiguration


class PricesConfig(DataNodeConfiguration):
    shard_id: str = Field(
        ...,
        description="Stable updater partition for this price job.",
        examples=["us_equities_daily"],
    )
    display_label: str | None = Field(
        default=None,
        description="Optional human-facing label for UI display only.",
        examples=["US equities daily prices"],
        json_schema_extra={"hash_excluded": True},
    )
```

Do not invent other metadata-marker exceptions. Scope, dependency, source, and
output-affecting fields must remain hashed config fields.

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

### 6. `time_index` Must Be Nanosecond UTC

Every non-empty DataFrame returned by `update()` must have its first index
level named `time_index` with dtype exactly `datetime64[ns, UTC]`.

Do not rely on `pd.Timestamp.now("UTC").normalize()` or
`pd.Timestamp.now("UTC").floor(...)` alone. Some pandas versions preserve
microsecond resolution and produce `datetime64[us, UTC]`, which the DataNode
runtime rejects.

For a single-index DataFrame, construct the index with an explicit dtype:

```python
time_index = pd.Timestamp.now("UTC").normalize()
return pd.DataFrame(
    {"close": [100.0]},
    index=pd.DatetimeIndex(
        [time_index],
        name="time_index",
        dtype="datetime64[ns, UTC]",
    ),
)
```

For a MultiIndex DataFrame, construct the time level separately:

```python
time_index = pd.DatetimeIndex(
    [row[0] for row in rows],
    dtype="datetime64[ns, UTC]",
)
index = pd.MultiIndex.from_arrays(
    [
        time_index,
        [row[1] for row in rows],
    ],
    names=["time_index", "unique_identifier"],
)
```

The validator error to prevent is:

```text
Time index must be datetime64[ns, UTC]
```

Use Alembic from the first version when a DataNode storage table must support
physical schema evolution. Keep the `PlatformTimeIndexMetaTable` catalog model as
the SDK storage contract, apply Alembic-rendered SQL through the migration
workflow, then register or refresh the MetaTable catalog binding separately.

### 7. Dependencies Must Be Deterministic

Dependencies belong in constructor setup and `dependencies()`.

Dependency storage-table selection belongs in `DataNodeConfiguration`, because
changing it changes the dependency graph and update identity.

Do not construct dependency graphs dynamically inside `update()`.

### 8. Foreign Keys Belong To SQLAlchemy And Alembic

For new code, model foreign keys on the `PlatformTimeIndexMetaTable` storage
class, or route the storage work to the MetaTable skill. When a DataNode storage
table needs a platform-managed FK, use normal SQLAlchemy `ForeignKey(...)` /
`ForeignKeyConstraint(...)` metadata on the storage class. Prefer
project-prefixed SQLAlchemy table names for explicit FK string targets so
projects sharing one schema do not collide. Generate those names with
`schema_table_name(project_or_app, concept)` from `mainsequence.meta_tables`.

Do not ask users to put FK target `MetaTable.uid` values into DataNode config or
MetaTable registration contracts. Alembic, SQLAlchemy, and the database own FK
DDL and physical FK constraint names.

Registration of the storage class follows the MetaTable migration lifecycle.
The migration provider reserves the MetaTable rows, Alembic renders/applies FK
DDL from SQLAlchemy metadata, and catalog finalization refreshes the MetaTable
binding after upgrade.

Do not add DataNode configuration fields just to mutate storage metadata.

### 9. Metadata Belongs To Storage

Production-quality table identifiers, descriptions, labels, column docs, and
foreign-key metadata belong to the storage class/MetaTable registration path.
Prefix explicit table identifiers, explicit physical table names, and Alembic
version table names with the project or package name rather than using bare
names that can collide across projects. Use `schema_table_name(...)` for
authored SQLAlchemy table names, including DataNode storage tables.

Do not put schema or published table metadata on the DataNode configuration.

## Review Rules

When reviewing an existing DataNode, look for:

- output storage contract hidden in `DataNodeConfiguration`
- missing `__metatable_description__` on the storage table
- storage columns without `mapped_column(info={"label": ..., "description": ...})`
- dependency storage table passed as an ad hoc constructor argument
- schema or published table metadata hidden in DataNode configuration
- `test_node=True`
- missing explicit `storage_table`
- manual pre-registration of the output `storage_table` when no returned
  metadata object is needed
- wrong split between hashed config fields and non-config class/runtime values
- misuse of `hash_namespace`
- non-incremental `update()` behavior
- hidden dependency creation inside `update()`
- invalid identity-indexed output shape
- `time_index` dtype that is not exactly `datetime64[ns, UTC]`
- DataFrame columns that do not match the `PlatformTimeIndexMetaTable` class

## Validation Checklist

Do not claim success until you have checked:

- the relevant docs were read first
- output storage is a `PlatformTimeIndexMetaTable` class that the SDK can register
- storage has an intention-rich `__metatable_description__`
- every storage column has an intention-rich `info.description`
- the DataNode constructor requires `storage_table`
- dependency storage-table references live in config and rely on SDK
  registration during config serialization
- config fields are updater-scoped by default
- no removed hash metadata markers remain
- no `test_node` usage remains
- `dependencies()` is deterministic
- `update()` is incremental
- the DataFrame shape matches the storage class
- non-empty outputs have first index level `time_index` with dtype `datetime64[ns, UTC]`
- the first validation run uses explicit `hash_namespace(...)` when it touches a shared backend

## This Skill Must Stop And Escalate When

- the change may break an existing published table contract and the versioning decision is unclear
- the intended storage class or MetaTable registration path is unclear
- the node needs identity dimensions but the coordinate strategy is unclear
- the task is actually an API, MetaTable, orchestration, or sharing problem
- docs, skill instructions, and code disagree on hashing or runtime behavior

Do not guess through contract changes.
