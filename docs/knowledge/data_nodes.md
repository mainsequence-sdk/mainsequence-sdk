# Data Nodes

DataNodes are the core unit of data production in Main Sequence. A good DataNode is not just code that runs - it is a stable data product that other people, jobs, dashboards, and agents can trust.

This guide translates the internal rules into practical, human-friendly guidance.

## 1) Start with the right mental model

A DataNode has two separate objects:

- a registered `PlatformTimeIndexMetaData` storage class, backed by a MetaTable
  UID, that owns the dataset contract
- a DataNode update process, identified by `update_hash`, that writes into that
  registered storage table

This is what allows multiple update processes to write safely into the same
dataset contract.

!!! tip "Rule of thumb"
    If a change modifies what the dataset means, change or migrate the
    `PlatformTimeIndexMetaData` storage contract. If a change only modifies how
    one job updates data, it should affect `update_hash`.

## 2) Guiding principles

1. A DataNode is a product.
2. Incremental updates are the default.
3. Deterministic behavior beats clever behavior.
4. Metadata is required for production-quality tables.

In practice, this means:

- treat `identifier` and schema as API contracts,
- avoid full-history fetches in production,
- avoid hidden global state and implicit time behavior,
- always document table and columns with metadata.

## 3) Naming conventions

Use these conventions consistently:

- Class name: `PascalCase` (`DailyFxRatesECB`)
- File name: `snake_case.py` (`daily_fx_rates_ecb.py`)
- Config model: `<Something>Config` based on `DataNodeConfiguration`
- Published identifier: lowercase `snake_case`, readable, and stable when possible (`fx_ecb_daily_rates`)
- Dependency keys: short and descriptive (`"prices"`, `"rates"`, `"raw"`)

### 3.1 Table identifiers must be unique across the organization

Treat a table `identifier` as an organization-level published name.

That means:

- two teams should not publish different datasets under the same identifier,
- tutorial-style identifiers are likely to collide on shared backends,
- project-specific suffixes are often the safest starting point while learning.

Important architectural point:

- `identifier` is published metadata, not update identity,
- it belongs to the registered MetaTable-backed storage contract,
- you can repoint an identifier from one backing table to another during a migration without rotating update-process identity.

There is no separate `portable_identifier` flag in the current SDK. Portability
is the default meaning of the storage-table identifier.

This is useful when you want a portable public handle like `daily_prices` while
moving the actual storage implementation underneath it.

If you want to inspect existing DataNode table identifiers from the CLI, run:

```bash
mainsequence data-node list
```

The `Identifier` column lists DataNode table identifiers exposed by `TimeIndexMetaData`. It does not list asset `unique_identifier` values.

### 3.2 Labels are organization metadata only

`TimeIndexMetaData` objects can also carry `labels`.

Those labels do not change:

- runtime behavior
- table identity
- hashing
- functionality

Use them only for grouping and discovery.

## 4) Config design: meaning vs scope vs operational knobs

A simple and scalable pattern is:

- one Pydantic config object for update-scope fields,
- one registered `PlatformTimeIndexMetaData` class for storage,
- make the node constructor accept both `config` and `storage_table`,
- operational runtime knobs outside `__init__`.

Preferred constructor shape:

```python
from mainsequence.meta_tables import (
    DataNode,
    DataNodeConfiguration,
    PlatformTimeIndexMetaData,
)


class MyNodeConfig(DataNodeConfiguration):
    ...


class MyNode(DataNode):
    def __init__(
        self,
        config: MyNodeConfig,
        storage_table: type[PlatformTimeIndexMetaData],
    ):
        self.my_field = config.my_field
        super().__init__(config=config, storage_table=storage_table)
```

`DataNode` is strict about this contract. New nodes should not rely on raw
constructor args being reflected back into hashed configuration automatically,
and they should not expect `DataNode` to create storage.

### 4.1 Storage meaning belongs to the storage table

These define the dataset contract and table identity and should be represented
in the `PlatformTimeIndexMetaData` SQLAlchemy model:

Examples:

- frequency (`1m`, `1d`)
- price type (`mid`, `close`)
- transformation type (log return vs simple return)
- source choice when it changes dataset meaning
- index columns and foreign keys

### 4.2 Scope fields affect `update_hash`

These define updater/job partitioning only.

Examples:

- `asset_universe`
- `shard_id`
- partition keys

Legacy `ignore_from_storage_hash` field metadata and the older
`_ARGS_IGNORE_IN_STORAGE_HASH` class attribute are removed. `update_only` is
also removed: all `DataNodeConfiguration` fields are update-scoped by default.

### 4.3 Operational knobs (affect neither hash)

Do not put these in `__init__` args.

Examples:

- batch size
- retries/timeouts/backoff
- debug flags
- secrets/credentials

Keep them in env vars or runtime config read inside `update()`.

If you absolutely must keep a Pydantic field on a config/helper model for
display-only metadata, mark it with:

```python
Field(..., json_schema_extra={"hash_excluded": True})
```

`hash_excluded` fields are excluded from update identity. The older
`runtime_only` marker is removed; descriptive metadata should use
`hash_excluded`.

Use this only for descriptive fields such as labels or long-form documentation.
Do not use `hash_excluded` for anything that changes:

- output values
- dependencies
- table meaning
- table schema

One explicit exception in this SDK is `DataNodeMetaData`, which is
hash-excluded for older config-driven metadata paths. New code should put
published table identity and schema on the registered storage class.

## 5) Hashing rules in plain English

The storage table should change only when downstream users should treat the
result as a different dataset.

`update_hash` can change for job-level differences while still writing to the same table.

Example:

- `PricesBars(frequency="1m")` defines table meaning.
- Updater A writes only BTC.
- Updater B writes only ETH.
- Both should share one registered storage table, but have different `update_hash`.

!!! note "Asset universe is usually scope, not table meaning"
    For standard `(time_index, unique_identifier)` asset tables, do not create a
    new storage table for every asset universe.

## 5.1) Hash namespaces: what they actually do

`DataNode` also supports a separate testing and isolation mechanism called `hash_namespace`.

This is not the same thing as table identity, asset identity, or business meaning. It is an extra namespace added to hashing so you can safely isolate runs on a shared backend.

### When to use it

Use `hash_namespace` when you want:

- integration tests that do not collide with production-like tables
- temporary experimentation on a shared tenant
- parallel test runs that must not write into each other's tables

### How namespace is resolved

The code resolves namespace in this order:

1. explicit `hash_namespace="..."`
2. the active `with hash_namespace("..."):` context manager
3. otherwise, an empty namespace

### What changes when the namespace is non-empty

If the namespace is empty, nothing changes and hashes behave exactly as they do in normal production-style runs.

If the namespace is non-empty, `DataNode` injects `hash_namespace` into the
build configuration. That changes `update_hash`.

Storage identity is not created by `DataNode`. If a test needs isolated
storage, pass a separately registered or bound `PlatformTimeIndexMetaData`
storage class.

### What happens during `run()`

If a node has a non-empty namespace, `run()` re-activates that namespace around the full execution.

That matters because dependencies created inside `dependencies()` will inherit the same namespace automatically during the run.

So the isolation is not only for the top-level node. It propagates across the run tree.

### What to prefer

For tests, prefer:

```python
from mainsequence.meta_tables.data_nodes import hash_namespace

with hash_namespace("pytest_case_123"):
    node = MyNode(...)
    node.run(debug_mode=True, force_update=True)
```

### What not to use it for

Do not use `hash_namespace` to represent business meaning such as:

- frequency
- venue
- asset universe
- transformation logic

Those belong in the actual constructor/config fields and should be reflected through the normal hashing rules.

Think of `hash_namespace` as test and isolation plumbing, not dataset definition.

## 6) Update strategy: incremental by default

Use `UpdateStatistics` to minimize work and control windows intentionally.

### 6.1 Single-index pattern

- first run: start at `config.offset_start` when provided, otherwise legacy `OFFSET_START`
- subsequent runs: start at `last_time + frequency_step`
- daily datasets typically end at yesterday 00:00 UTC

### 6.2 Multidimensional index pattern

- the first index is the UTC `time_index`
- every remaining index is an identity dimension
- uniqueness is enforced across the full index tuple
- compute incremental start points from canonical `UpdateStatistics`
- return only new rows, or as close as practical

For an asset price table, the index is usually:

```python
["time_index", "unique_identifier"]
```

For account holdings, the index includes the account and the asset:

```python
["time_index", "account_uid", "unique_identifier"]
```

The nested `UpdateStatistics.index_progress` shape follows the identity
dimensions in order. For `["time_index", "account_uid", "unique_identifier"]`,
the progress path is:

```text
account_uid -> unique_identifier -> timestamp
```

Use canonical range maps when reading only the needed incremental window:

```python
range_map = update_statistics.get_dimension_range_map_great_or_equal(
    identity_dimensions=["account_uid", "unique_identifier"],
)
last_observation = self.get_df_between_dates(dimension_range_map=range_map)
```

### 6.3 Backfills

Backfills should be explicit and controlled (separate job/updater, intentional overwrite policy).

### 6.4 Do not rely on implicit filtering

Even if runtime filtering removes already persisted rows, you should still:

- avoid fetching full history every run,
- avoid returning full history every run.

This is a cost and performance requirement.

## 7) DataFrame quality rules

Must-have rules:

- `time_index` is UTC-aware datetime index
- `time_index` is the observation point of the row, not just a generic date label
- rows aligned on the same `time_index` should be comparable across series in the dataset
- for bar datasets, `time_index` should usually be the right edge of the bar, so the timestamp represents the completed observation window
- output columns are lowercase and stable
- payload columns may be `date` or timezone-aware datetime when declared in records; `time_index` remains the row observation timestamp
- no duplicate rows for same index keys
- consistent dtypes across runs

Recommended rules:

- keep column names short (<= 63 chars)
- avoid mixed `object` dtype when possible
- replace `inf/-inf` with `NaN`
- keep index sorted ascending

## 8) Dependencies best practices

Do:

- instantiate dependency nodes in `__init__`
- return them in `dependencies()`
- keep dependency graph deterministic

Do not:

- create dependencies inside `update()`
- make dependency construction depend on current time or hidden env state

## 9) Records, foreign keys, and metadata

For production nodes, define the table contract on the registered
`PlatformTimeIndexMetaData` SQLAlchemy model. That is the source of truth for:

- index columns
- value columns
- foreign keys
- table identifier, namespace, labels, and description

Older config-driven metadata fields still exist for compatibility and simple
cases, but new tutorials and examples should prefer the storage model.

For simple config-driven nodes, you can still provide descriptive metadata:

- put table metadata in `DataNodeConfiguration.node_metadata`
- put column metadata in `DataNodeConfiguration.records`

Those config blocks use the SDK models `DataNodeMetaData` and `RecordDefinition`.

The base `DataNode` can build `TableMetaData` and `ColumnMetaData` from those
config blocks when present, but storage validation is performed against the
registered storage table contract.

Use `DataNodeConfiguration.records` when a node has a stable output contract:

```python
from pydantic import Field

from mainsequence.meta_tables import DataNodeConfiguration, RecordDefinition


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
```

When a DataNode source table should reference a registered MetaTable, declare the
relationship on the `PlatformTimeIndexMetaData` storage model. Foreign keys are
part of the MetaTable contract, not `DataNodeConfiguration`.

Log useful operational facts:

- chosen update window,
- number of assets processed,
- rows produced,
- data coverage issues.

Avoid logging secrets.

### 10.1) Searchability and semantic discovery

Good metadata is not just for humans reading code. It also powers search and discovery across published data nodes.

`TimeIndexMetaData` now exposes two complementary search paths:

- `description_search(q, ...)`
- `column_search(q, ...)`

Use them differently:

- `description_search(...)` is for natural-language discovery:
  - "close price"
  - "daily allocation weights"
  - "crypto funding rates"
- `column_search(...)` is for schema-oriented discovery:
  - "close"
  - "unique_identifier"
  - "implied_volatility"

#### `description_search(...)`

This search hits:

- `POST <object_url>/description-search/`

Server behavior:

- if `q_embedding` is omitted, the server generates it from `q`
- results may come back either paginated or non-paginated

The ranking blends two signals:

- trigram similarity
- embedding similarity

That is why the method exposes:

- `trigram_k`
- `embed_k`
- `w_trgm`
- `w_emb`
- `embedding_model`

Use this when the user knows what they want conceptually, but not the exact table name.

Example:

```python
import mainsequence.client as msc

results = msc.TimeIndexMetaData.description_search(
    "daily close price",
    data_source__id=2,
)
```

CLI equivalent:

```bash
mainsequence data_node search "daily close price" --mode description --data-source-id 2
```

#### `column_search(...)`

This search hits:

- `GET <object_url>/column-search/?q=...`

Extra keyword arguments are passed through as normal DRF filters, which makes it useful when you want to constrain the search to a known area such as one data source or one identifier family.

Use this when the user remembers a column name or schema fragment, but not the data node name.

Example:

```python
import mainsequence.client as msc

results = msc.TimeIndexMetaData.column_search(
    "close",
    data_source__id=2,
)
```

CLI equivalent:

```bash
mainsequence data_node search "close" --mode column --data-source-id 2
```

#### Refreshing the search index

`refresh_table_search_index()` exists for the cases where search quality depends on metadata or code that changed after the table was created.

This method:

- joins the table's column definitions with the code used to generate the data node
- builds one consolidated textual description
- embeds that description into the vector representation used for semantic search

It hits:

- `POST /{data_node_storage_uid}/refresh-table-search-index/`

Use it when:

- you improved `get_table_metadata()` or `get_column_metadata()`
- you changed code comments or table-generation logic in a way that should improve discovery
- search results feel stale compared with the current node implementation

Example:

```python
import mainsequence.client as msc

storage = msc.TimeIndexMetaData.get(uid="<DATA_NODE_STORAGE_UID>")
storage.refresh_table_search_index()
```

CLI equivalent:

```bash
mainsequence data-node refresh-search-index <DATA_NODE_STORAGE_UID>
```

#### Running read-only SQL against a dynamic table

`TimeIndexMetaData.run_query(...)` executes a raw SQL query directly against one published dynamic table.

This is for inspection and diagnostics on the storage that already exists. It is not a substitute for building a reusable `DataNode` API contract.

The SDK uses:

- `POST /orm/api/ts_manager/dynamic_table/{data_node_storage_uid}/run_query/`

Request contract:

- the request body is the raw SQL string
- content type is `text/plain`
- do not send JSON like `{"sql": "SELECT ..."}` 
- do not send `X-MS-SYNC-TOKEN`

Example:

```python
import mainsequence.client as msc

storage = msc.TimeIndexMetaData.get(uid="<DATA_NODE_STORAGE_UID>")
result = storage.run_query("SELECT * FROM my_table LIMIT 100")
```

Expected success envelope:

```python
{
    "ok": True,
    "query_id": "abc123",
    "dynamic_table_uid": "<DATA_NODE_STORAGE_UID>",
    "results": [
        {
            "column_a": "value",
            "column_b": 10,
        }
    ],
    "truncated": False,
    "max_rows": 1000,
    "row_count": 1,
    "error": None,
}
```

The method returns the backend query envelope directly. If the backend rejects the SQL with a structured error payload, the SDK still returns that envelope so callers can inspect `error.kind`, `error.message`, and `retryable`.

CLI:

```bash
mainsequence data-node run_query <DATA_NODE_STORAGE_UID> "SELECT 1 AS ok"
mainsequence data-node run_query <DATA_NODE_STORAGE_UID> "SELECT * FROM my_table LIMIT 100"
```

#### Tail deleting rows after a cutoff

`TimeIndexMetaData.delete_after_date(...)` removes the tail of a dynamic table starting at an inclusive cutoff timestamp.

This is not arbitrary range deletion:

- there is no `end_date`
- the cutoff is inclusive
- rows at or after `after_date` are deleted
- the caller must be authenticated and have edit access to the `DynamicTableMetaData`

The SDK uses:

- `POST /orm/api/ts_manager/dynamic_table/{data_node_storage_uid}/delete_after_date/`

Use this for rollback-style cleanup when a bad tail load or backfill needs to be removed.

For a normal table:

```python
import mainsequence.client as msc

storage = msc.TimeIndexMetaData.get(uid="<DATA_NODE_STORAGE_UID>")
result = storage.delete_after_date("2026-04-01T00:00:00Z")
```

For an asset table, scope the delete to one asset identity:

```python
result = storage.delete_after_date(
    "2026-04-01T00:00:00Z",
    dimension_filters={"unique_identifier": ["AAPL"]},
)
```

Or scope it to multiple asset identities:

```python
result = storage.delete_after_date(
    "2026-04-01T00:00:00Z",
    dimension_filters={"unique_identifier": ["AAPL", "MSFT"]},
)
```

For a three-index holdings table, scope the delete to one coordinate:

```python
result = storage.delete_after_date(
    "2026-04-01T00:00:00Z",
    index_coordinates=[
        {
            "account_uid": "00000000-0000-0000-0000-000000000001",
            "unique_identifier": "AAPL",
        }
    ],
)
```

The response contains the authoritative post-delete table state:

```python
{
    "ok": True,
    "dynamic_table_uid": "<DATA_NODE_STORAGE_UID>",
    "deleted_count": 123,
    "table_empty": False,
    "dimension_filters": {"unique_identifier": ["AAPL", "MSFT"]},
    "stats": {
        "last_time_index_value": "2026-03-31T23:59:00Z",
        "earliest_index_value": "2024-01-01T00:00:00Z",
        "multi_index_stats": None,
        "multi_index_column_stats": None,
    },
}
```

Use `stats` to update visible table metadata, or refetch the table detail after the delete.

!!! tip "Search quality depends on metadata quality"
    If you want `description_search(...)` to work well, write table and column metadata for humans, not just for machines. Clear descriptions, stable naming, and meaningful column docs directly improve discovery.

## 11) Testing safely

When tests hit shared backends, isolate update hashes and use the intended
storage table explicitly.

Use `with hash_namespace("..."):` with an explicit, collision-resistant
namespace.

This intentionally changes `update_hash` so tests do not collide with
production-like update records. It does not create or select storage for you.

Keep test runs bounded:

- prefer a narrow `config.offset_start` for test runs,
- or pass controlled update statistics/checkpoints,
- keep integration tests small and deterministic.

### Example: keep integration tests in `tests/` and set `config.offset_start`

In real projects, keep these tests in the `tests/` folder, for example:

- `tests/test_my_node.py`

A practical pattern is to keep the production class unchanged, pass a narrow
`offset_start` in the test config, and run the node inside a namespace:

```python
from mainsequence.meta_tables.data_nodes import hash_namespace

from src.data_nodes.my_node import MyNode, MyNodeConfig, MyNodeStorage


def test_my_node_smoke():
    config = MyNodeConfig(
        ...,
        offset_start="2025-01-01T00:00:00+00:00",
    )

    with hash_namespace("pytest_my_node_smoke"):
        MyNodeStorage.register(
            description="Test storage table for MyNode.",
            labels=["test", "data-node"],
        )
        node = MyNode(config=config, storage_table=MyNodeStorage)
        err, df = node.run(debug_mode=True, force_update=True)

    assert err is False
    assert df is not None
```

Why this pattern works well:

- the namespace isolates update hashes from production-like update records
- the registered storage table keeps the table contract explicit
- the production node stays unchanged
- the narrower `config.offset_start` prevents large first-run backfills during tests

If you already have older nodes that still use class-level `OFFSET_START`, that
fallback remains supported, but config-driven `offset_start` is the preferred
pattern for new code and new documentation.

## 12) Schema evolution policy

Schema is an API contract and should be treated as immutable.

Safe changes:

- metadata text updates,
- tags/documentation improvements,
- bug fixes that do not change schema/meaning.

Breaking changes (create a new table identifier instead):

- adding/removing/renaming columns,
- changing dtypes,
- changing index shape,
- changing meaning of existing fields.

!!! warning "Never make breaking schema changes in place"
    If semantics or schema change, publish a new table identifier and migrate consumers.

## 13) Security basics

- keep credentials in env vars or secret management,
- never hardcode API keys,
- validate external inputs,
- prefer batched calls over excessive network chatter.

## 13.1) Reading an existing table from the client

When you are consuming a table that already exists in the platform, the first question is simple:

"Do I just want to read one known table, or do I need a more flexible query?"

Start with `APIDataNode.build_from_identifier(...)` when you already know the published table identifier.

```python
from mainsequence.meta_tables import APIDataNode

prices = APIDataNode.build_from_identifier("simulated_prices_tutorial")

df = prices.get_df_between_dates(
    start_date="2026-01-01",
    columns=["open", "high", "low", "close"],
    dimension_filters={"unique_identifier": ["NVDA", "AAPL"]},
)
```

This helper resolves the table metadata for you and returns an `APIDataNode` that is ready for normal read methods.

Use it when:

- you are reading one table by its stable identifier,
- the read shape is fairly standard,
- you want the shortest path from published dataset to DataFrame.

This is especially useful in dashboards, notebooks, and integration code where the table already exists and you are not constructing a dependency graph.

## 14) Quick pre-ship checklist

Before shipping a DataNode, verify:

- identifier is stable and globally meaningful,
- config fields are correctly split across meaning/scope/runtime,
- `update()` is incremental and uses `UpdateStatistics`,
- assets are resolved for 2D tables,
- metadata methods are implemented,
- schema is intentional and stable,
- logs are useful and secret-safe,
- integration test runs in an isolated namespace.
