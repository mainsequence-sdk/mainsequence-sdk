# Migrating DataNodes To MetaTable-Driven Storage

This guide is for libraries that still use the old DataNode style where the
node declares its output schema inline. The new SDK model is storage-first:
a `PlatformTimeIndexMetaData` SQLAlchemy model owns the table contract, and the
`DataNode` only owns update logic.

## What Changed

| Old DataNode | New DataNode |
| --- | --- |
| Node declares schema with methods such as `get_table_metadata()` and `get_column_metadata()`. | A `PlatformTimeIndexMetaData` SQLAlchemy model declares the schema. |
| Node metadata mixes table identity, columns, and update behavior. | Storage identity lives on the storage class; update identity lives in `DataNodeConfiguration`. |
| Registration is implicit or manually glued together from table metadata. | Registration is migration-first: add the storage class to the MetaTable migration provider and run `mainsequence migrations upgrade`. |
| Foreign keys are raw SQLAlchemy targets or backend UID maps. | Foreign keys use `MetaTableForeignKey(TargetModel, column=...)`. |
| `storage_table` had to be registered before node construction. | Output `storage_table` must be registered by the MetaTable migration workflow before `DataNode` / `PersistManager` uses it. Config-stored storage classes fail if they are not already bound. |

## The New Mental Model

A modern DataNode has two explicit objects:

- a storage class: `type[PlatformTimeIndexMetaData]`
- an update process: `DataNode`

The storage class answers:

- What table is this?
- Which project/session data source owns it?
- What columns, indexes, foreign keys, and descriptions define it?
- What stable storage identity should downstream users depend on?

The DataNode answers:

- What configuration controls this updater?
- What dependencies does this updater read?
- What rows does this update produce now?

Do not put storage schema, published table metadata, backend UIDs, or data-source
UIDs in `DataNodeConfiguration`.

## Public Registration Rule

For platform-managed SDK storage classes, the public registration path is the
MetaTable migration workflow:

```bash
mainsequence migrations upgrade --provider mainsequence_migrations:migration --to head
```

Do not call `StorageClass.register()` directly in application/bootstrap code.
The method remains SDK plumbing for migration tooling.

Those values are inferred:

- data source: active Main Sequence project/session
- identifier: `__metatable_identifier__`
- namespace: `__metatable_namespace__`
- description: `__metatable_description__`
- labels: `__metatable_labels__`
- provisioning: `__metatable_provisioning__` or SDK default
- time index: `__time_index_name__`
- index grain: `__index_names__`
- stable storage disambiguation: `__metatable_extra_hash_components__`

`introspect` belongs to lower-level externally registered table flows. It is not
part of normal platform-managed DataNode storage registration.

## Migration Pattern

Every migrated node should end up with three declarations.

### Import Surface

Old libraries commonly import `DataNode` from `mainsequence.tdag` or from a
domain-layer compatibility module. SDK-level migrations should import the modern
DataNode surface from `mainsequence.meta_tables`:

```python
from mainsequence.meta_tables import (
    DataNode,
    DataNodeConfiguration,
    PlatformTimeIndexMetaData,
)
```

Domain libraries may wrap these primitives. In those projects, map the same
storage/config/node boundaries onto the library's public wrappers rather than
mixing layers.

### 1. Storage Class

Move table identity and schema out of the node and into a
`PlatformTimeIndexMetaData` model.

```python
import datetime

from sqlalchemy import DateTime, Float, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from mainsequence.meta_tables import PlatformTimeIndexMetaData


class Base(DeclarativeBase):
    pass


class DailyMetricsStorage(PlatformTimeIndexMetaData, Base):
    __metatable_namespace__ = "example-data"
    __metatable_identifier__ = "daily_metrics"
    __metatable_description__ = (
        "Daily metric observations keyed by entity identifier for downstream "
        "analytics."
    )
    __metatable_labels__ = ["metrics", "daily"]
    __metatable_extra_hash_components__ = {"storage_name": "daily_metrics"}

    __time_index_name__ = "time_index"
    __index_names__ = ["time_index", "entity_identifier"]

    time_index: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        info={
            "label": "Time Index",
            "description": "UTC observation timestamp for the daily metric row.",
        },
    )
    entity_identifier: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        info={
            "label": "Entity Identifier",
            "description": "Stable identifier for the observed entity.",
        },
    )
    value: Mapped[float] = mapped_column(
        Float,
        nullable=False,
        info={
            "label": "Value",
            "description": "Daily numeric value produced by the source system.",
        },
    )
```

Rules:

- Always include `__metatable_description__`.
- Describe both schema and intention, not only column names.
- Use SQLAlchemy column `info` for column labels and descriptions.
- Use `__metatable_extra_hash_components__` when two tables could otherwise have
  the same storage-relevant shape.
- Do not use `__metatable_extra_hash_components__` for labels, tests,
  backend UIDs, data-source UIDs, or updater scope.

### 2. Configuration

Keep configuration focused on update identity.

```python
import datetime

from pydantic import Field

from mainsequence.meta_tables import DataNodeConfiguration


class DailyMetricsConfig(DataNodeConfiguration):
    source_scope: str = Field(
        ...,
        description="Stable upstream scope selected by this updater.",
        examples=["default_scope", "regional_scope"],
    )
    offset_start: datetime.datetime | None = Field(
        default=None,
        description="Optional first-run lower bound for this updater.",
        examples=["2024-01-01T00:00:00+00:00"],
    )
```

Use `Field(...)` with descriptions for configuration fields. Add
`examples=[...]` when useful.

If a field is descriptive UI metadata and must not affect update identity, use:

```python
display_label: str | None = Field(
    default=None,
    description="Human-facing label for UI display only.",
    json_schema_extra={"hash_excluded": True},
)
```

Do not use legacy schema extras such as `update_only`, `runtime_only`, or
`ignore_from_storage_hash`. They are not the current SDK contract.

### 3. DataNode

The node receives the config and storage class, then produces rows.

```python
import pandas as pd

from mainsequence.meta_tables import DataNode, PlatformTimeIndexMetaData


class DailyMetricsNode(DataNode):
    def __init__(
        self,
        config: DailyMetricsConfig,
        storage_table: type[PlatformTimeIndexMetaData] = DailyMetricsStorage,
        *,
        hash_namespace: str | None = None,
    ) -> None:
        self.source_scope = config.source_scope
        super().__init__(
            config=config,
            storage_table=storage_table,
            hash_namespace=hash_namespace,
        )

    def dependencies(self) -> dict:
        return {}

    def update(self) -> pd.DataFrame:
        return build_daily_metrics_frame(self.source_scope)
```

The returned frame must match the storage table contract:

- index includes the declared `__index_names__`
- `time_index` is timezone-aware UTC
- value columns match the SQLAlchemy storage columns
- no extra payload columns appear

## Auto-Registration During Construction And Hashing

The SDK now registers storage classes at the places where it needs backend
identity:

- output `storage_table` passed to `DataNode`
- output `storage_table` validated by `PersistManager`
- `type[PlatformTimeIndexMetaData]` values inside `DataNodeConfiguration`

That means this is valid:

```python
node = DailyMetricsNode(
    config=DailyMetricsConfig(source_scope="default_scope"),
    storage_table=DailyMetricsStorage,
)
```

You do not need to pre-register the output storage table just to construct or
run the node.

This is also valid when a dependency storage class is part of config:

```python
class SpreadConfig(DataNodeConfiguration):
    base_storage: type[PlatformTimeIndexMetaData] = Field(
        ...,
        description="Storage table for the base metric series.",
    )


node = DerivedMetricsNode(
    config=SpreadConfig(base_storage=DailyMetricsStorage),
    storage_table=SpreadStorage,
)
```

Before hashing that config, the SDK requires `DailyMetricsStorage` to already be
bound by the migration workflow, then hashes by the bound `TimeIndexMetaData.uid`.
If it is not bound, config serialization fails and tells the user to run
migrations.

## Old Code To Delete

Remove old inline schema declarations from the node:

```python
def get_table_metadata(self):
    ...


def get_column_metadata(self):
    ...
```

Remove hand-built table metadata objects from update classes. The table contract
must be declared once on the SQLAlchemy storage model.

Also remove manual binding or UID plumbing:

```python
Storage.bind_meta_table(...)              # wrong
Storage.__metatable_uid__ = "..."         # wrong
storage_hash = "backend_table_name"       # wrong as config
data_source_uid = "..."                   # wrong as DataNode config
```

## Foreign Keys

Use `MetaTableForeignKey` for platform-managed storage relationships.

```python
import uuid

from sqlalchemy import Uuid
from sqlalchemy.orm import Mapped, mapped_column

from mainsequence.meta_tables import (
    MetaTableForeignKey,
    PlatformManagedMetaTable,
    PlatformTimeIndexMetaData,
)


class Account(PlatformManagedMetaTable, Base):
    __metatable_namespace__ = "accounts"
    __metatable_identifier__ = "account"
    __metatable_description__ = "Account master rows used to scope positions."
    __metatable_extra_hash_components__ = {"storage_name": "account"}

    uid: Mapped[uuid.UUID] = mapped_column(
        Uuid,
        primary_key=True,
        info={
            "label": "Account UID",
            "description": "Stable account identifier referenced by dependent observation tables.",
        },
    )


class AccountPositions(PlatformTimeIndexMetaData, Base):
    __metatable_namespace__ = "positions"
    __metatable_identifier__ = "account_positions"
    __metatable_description__ = "Time-indexed position rows keyed by account."
    __metatable_extra_hash_components__ = {"storage_name": "account_positions"}
    __time_index_name__ = "time_index"
    __index_names__ = ["time_index", "account_uid"]

    account_uid: Mapped[uuid.UUID] = mapped_column(
        Uuid,
        MetaTableForeignKey(Account, column="uid", ondelete="RESTRICT"),
        nullable=False,
        info={
            "label": "Account UID",
            "description": "Account identifier that scopes this position observation.",
        },
    )
```

Do not use table fullnames or `Parent.__table__.c.uid` as the public
declaration. Registration can rebind SQLAlchemy table names to backend physical
names. `MetaTableForeignKey` keeps the target model class as SDK metadata and
lets recursive registration resolve the backend `MetaTable.uid`.

## Parameterized Storage Tables

If a library used one node class for multiple table identities, create one
storage class per identity. Use a memoized factory so repeated calls return the
same SQLAlchemy mapped class object.

```python
import datetime
from functools import lru_cache

from sqlalchemy import DateTime, Float, String
from sqlalchemy.orm import Mapped, mapped_column

from mainsequence.meta_tables import PlatformTimeIndexMetaData


class _ObservationColumns(PlatformTimeIndexMetaData, Base):
    __abstract__ = True
    __time_index_name__ = "time_index"
    __index_names__ = ["time_index", "entity_identifier"]

    time_index: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        info={
            "label": "Time Index",
            "description": "UTC observation timestamp for this dataset variant.",
        },
    )
    entity_identifier: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        info={
            "label": "Entity Identifier",
            "description": "Stable identifier for the observed entity.",
        },
    )
    value: Mapped[float] = mapped_column(
        Float,
        nullable=False,
        info={
            "label": "Value",
            "description": "Numeric observation value for this dataset variant.",
        },
    )


@lru_cache(maxsize=None)
def observation_storage(dataset_variant: str) -> type[_ObservationColumns]:
    normalized = dataset_variant.lower().replace("-", "_")
    identifier = f"{normalized}_observations"
    return type(
        f"ObservationStorage_{normalized}",
        (_ObservationColumns,),
        {
            "__module__": __name__,
            "__qualname__": f"ObservationStorage_{normalized}",
            "__metatable_namespace__": "example-data",
            "__metatable_identifier__": identifier,
            "__metatable_description__": (
                f"{dataset_variant} observations keyed by entity identifier."
            ),
            "__metatable_extra_hash_components__": {"storage_name": identifier},
        },
    )
```

Then pass the concrete class into the node:

```python
class ObservationNode(DataNode):
    def __init__(
        self,
        config: ObservationConfig,
        *,
        hash_namespace: str | None = None,
    ):
        storage_table = observation_storage(config.dataset_variant)
        super().__init__(
            config=config,
            storage_table=storage_table,
            hash_namespace=hash_namespace,
        )
```

Use this when table identity truly changes. If a value changes only updater
scope, keep it in config and write into the same storage table.

## Migration Checklist

For each old DataNode:

1. [ ] Identify the output table contract: index columns, payload columns, dtypes, nullable flags, and foreign keys.
2. [ ] Replace old `mainsequence.tdag` imports with the modern SDK or domain-layer DataNode surface.
3. [ ] Create a `PlatformTimeIndexMetaData` SQLAlchemy storage class.
4. [ ] Move table identifier, namespace, descriptions, labels, and storage disambiguation to class metadata.
5. [ ] Move column labels and descriptions to SQLAlchemy column `info`.
6. [ ] Replace old inline metadata methods with the storage class.
7. [ ] Make the node constructor accept `config` and `storage_table`.
8. [ ] Keep `DataNodeConfiguration` limited to update-scope fields.
9. [ ] Use `Field(...)` descriptions and examples for config fields.
10. [ ] Replace raw foreign keys with `MetaTableForeignKey(TargetModel, column=...)`.
11. [ ] Use a memoized storage-class factory for parameterized table identities.
12. [ ] Delete manual UID binding, manual `data_source_uid` threading, and direct register kwargs.
13. [ ] Add contract tests comparing the storage columns/indexes to the frame returned by `update()`.
14. [ ] Add an import smoke test for the library package.
15. [ ] Run `mainsequence migrations upgrade --provider <provider> --to head` against an authenticated project to verify storage registration, then run one live update to verify row writes.

## Validation Tests To Add

At minimum, add tests for:

- storage class imports without backend access
- storage contract columns match the expected output frame
- index names match the returned DataFrame index names
- `time_index` values are timezone-aware UTC
- parameterized storage factories are memoized
- different table identities produce different storage hashes
- `DataNodeConfiguration` changes alter `update_hash` only when they should
- `hash_excluded` fields do not alter `update_hash`
- `MetaTableForeignKey` targets resolve through model classes, not table names

Offline tests should avoid backend calls by monkeypatching the migration
registration plumbing or using SDK model constructors for returned metadata.
Live tests should run with a real project/session and verify idempotency: a
second migration run should reuse the same registered storage table, and a node
run should append/update rows according to the node logic.

## Common Mistakes

- Keeping `get_table_metadata()` or `get_column_metadata()` on the node.
- Passing `data_source_uid`, `description`, `labels`, or `introspect` into
  platform-managed model `register()`.
- Putting `storage_table` in `DataNodeConfiguration`.
- Putting backend UIDs or data-source UIDs in config.
- Using `hash_namespace` to define real storage identity.
- Using `ForeignKey(f"{Parent.__table__.fullname}.uid")` or
  `ForeignKey(Parent.__table__.c.uid)` for platform-managed storage.
- Creating a new dynamic SQLAlchemy class every time instead of memoizing a
  parameterized storage factory.
- Declaring config fields without `Field(...)` descriptions.
- Using legacy schema extras instead of `json_schema_extra={"hash_excluded": True}`.
