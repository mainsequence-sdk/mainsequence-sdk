# Data Nodes

DataNodes are the core unit of data production in Main Sequence. A good DataNode is not just code that runs - it is a stable data product that other people, jobs, dashboards, and agents can trust.

This guide translates the internal rules into practical, human-friendly guidance.

## 1) Start with the right mental model

A DataNode has two identities:

- `storage_hash`: identifies the table (the dataset contract).
- `update_hash`: identifies the updater job (the process writing into that table).

This is what allows multiple jobs to write safely into the same dataset.

!!! tip "Rule of thumb"
    If a change modifies what the dataset means, it should affect `storage_hash`.
    If a change only modifies how one job updates data, it should affect `update_hash`.

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

- `identifier` is published metadata, not hash identity,
- it is intentionally runtime-only,
- you can repoint an identifier from one backing table to another during a migration without rotating `storage_hash` or `update_hash`.

There is no separate `portable_identifier` flag in the current SDK. Portability is
the default meaning of `DataNodeMetaData.identifier`.

This is useful when you want a portable public handle like `daily_prices` while moving the actual storage implementation underneath it.

If you want to inspect existing DataNode table identifiers from the CLI, run:

```bash
mainsequence data-node list
```

The `Identifier` column lists DataNode table identifiers exposed by `DataNodeStorage`. It does not list asset `unique_identifier` values.

### 3.2 Labels are organization metadata only

`DataNodeStorage` objects can also carry `labels`.

Those labels do not change:

- runtime behavior
- table identity
- hashing
- functionality

Use them only for grouping and discovery.

## 4) Config design: meaning vs scope vs operational knobs

A simple and scalable pattern is:

- one Pydantic config object for dataset and scope fields,
- make the node constructor accept exactly that one config object,
- operational runtime knobs outside `__init__`.

Preferred constructor shape:

```python
from mainsequence.tdag import DataNode, DataNodeConfiguration


class MyNodeConfig(DataNodeConfiguration):
    ...


class MyNode(DataNode):
    def __init__(self, config: MyNodeConfig):
        self.my_field = config.my_field
        super().__init__(config=config)
```

`DataNode` is now strict about this contract. New nodes should not rely on raw
constructor args being reflected back into hashed configuration automatically.

### 4.1 Meaning fields (affect `storage_hash`)

These define the dataset contract and table identity.

Examples:

- frequency (`1m`, `1d`)
- price type (`mid`, `close`)
- transformation type (log return vs simple return)
- source choice when it changes dataset meaning

### 4.2 Scope fields (affect `update_hash`, ignored from `storage_hash`)

These define updater/job partitioning only.

Examples:

- `asset_universe`
- `shard_id`
- partition keys

For Pydantic v2, mark these fields with:

```python
Field(..., json_schema_extra={"update_only": True})
```

Legacy `ignore_from_storage_hash` field metadata and the older
`_ARGS_IGNORE_IN_STORAGE_HASH` class attribute are removed. New code should use
`DataNodeConfiguration` fields plus `update_only` / `runtime_only` explicitly.

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
Field(..., json_schema_extra={"runtime_only": True})
```

`runtime_only` fields are excluded from both `update_hash` and `storage_hash`.

Use this only for descriptive fields such as labels or long-form documentation.
Do not use `runtime_only` for anything that changes:

- output values
- dependencies
- `get_column_metadata()` structure
- table meaning
- table schema

One explicit exception in this SDK is `DataNodeMetaData`, which is also runtime-only.
That is intentional: table metadata is treated as published/discovery information,
not as build identity. In particular, `identifier` is runtime-only so it can serve
as a portable alias across backing tables.

## 5) Hashing rules in plain English

`storage_hash` should change only when downstream users should treat the result as a different dataset.

`update_hash` can change for job-level differences while still writing to the same table.

Example:

- `PricesBars(frequency="1m")` defines table meaning.
- Updater A writes only BTC.
- Updater B writes only ETH.
- Both should share `storage_hash`, but have different `update_hash`.

!!! note "Asset universe is usually scope, not table meaning"
    For standard `(time_index, unique_identifier)` asset tables, do not include asset universe in `storage_hash`.

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
2. `test_node=True`, which becomes the namespace `"test"`
3. the active `with hash_namespace("..."):` context manager
4. otherwise, an empty namespace

### What changes when the namespace is non-empty

If the namespace is empty, nothing changes and hashes behave exactly as they do in normal production-style runs.

If the namespace is non-empty, `DataNode` injects `hash_namespace` into the build configuration. That changes both:

- `storage_hash`
- `update_hash`

That is why namespaced runs are isolated from non-namespaced runs.

### What happens during `run()`

If a node has a non-empty namespace, `run()` re-activates that namespace around the full execution.

That matters because dependencies created inside `dependencies()` will inherit the same namespace automatically during the run.

So the isolation is not only for the top-level node. It propagates across the run tree.

### What to prefer

For tests, prefer:

```python
from mainsequence.tdag.data_nodes import hash_namespace

with hash_namespace("pytest_case_123"):
    node = MyNode(...)
    node.run(debug_mode=True, force_update=True)
```

`test_node=True` is a shortcut and simply uses the namespace `"test"`.

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

### 6.2 MultiIndex asset pattern

- compute per-asset start using each asset's last update
- include lookback when rolling features need history
- batch fetch upstream data once when possible
- return only new rows (or as close as practical)

### 6.3 Backfills

Backfills should be explicit and controlled (separate job/updater, intentional overwrite policy).

### 6.4 Do not rely on implicit filtering

Even if runtime filtering removes already persisted rows, you should still:

- avoid fetching full history every run,
- avoid returning full history every run.

This is a cost and performance requirement.

## 7) Asset discipline for 2D tables

If your index is `(time_index, unique_identifier)`, `unique_identifier` should normally map to `msc.Asset.unique_identifier`.

Business rule:

- do not emit unknown asset identifiers,
- resolve/register assets idempotently,
- prefer doing this in `get_asset_list()`.

Minimal helper pattern:

```python
import mainsequence.client as msc


def ensure_assets_exist(asset_uids: list[str]) -> list[msc.Asset]:
    existing = msc.Asset.filter(unique_identifier__in=asset_uids)
    existing_uids = {a.unique_identifier for a in existing}

    missing = [uid for uid in asset_uids if uid not in existing_uids]
    if missing:
        payload = [
            {"unique_identifier": uid, "snapshot": {"name": uid, "ticker": uid}}
            for uid in missing
        ]
        created = msc.Asset.batch_get_or_register_custom_assets(payload)
        return list(existing) + list(created)

    return list(existing)
```

Optional but recommended for instruments use cases:

- attach pricing details when relevant via
  `asset.add_instrument_pricing_details_from_ms_instrument(...)`.

## 8) DataFrame quality rules

Must-have rules:

- `time_index` is UTC-aware datetime index
- `time_index` is the observation point of the row, not just a generic date label
- rows aligned on the same `time_index` should be comparable across series in the dataset
- for bar datasets, `time_index` should usually be the right edge of the bar, so the timestamp represents the completed observation window
- output columns are lowercase and stable
- no datetime columns in value columns (time goes in index)
- no duplicate rows for same index keys
- consistent dtypes across runs

Recommended rules:

- keep column names short (<= 63 chars)
- avoid mixed `object` dtype when possible
- replace `inf/-inf` with `NaN`
- keep index sorted ascending

## 9) Dependencies best practices

Do:

- instantiate dependency nodes in `__init__`
- return them in `dependencies()`
- keep dependency graph deterministic

Do not:

- create dependencies inside `update()`
- make dependency construction depend on current time or hidden env state

## 10) Metadata and observability

For production nodes, implement:

- `get_table_metadata()`
- `get_column_metadata()`

For simple config-driven nodes, you can often avoid overriding these methods:

- put table metadata in `DataNodeConfiguration.node_metadata`
- put column metadata in `DataNodeConfiguration.records`

Those config blocks use the SDK models `DataNodeMetaData` and `RecordDefinition`.

The base `DataNode` will build `TableMetaData` and `ColumnMetaData` from those
config blocks when present.

Log useful operational facts:

- chosen update window,
- number of assets processed,
- rows produced,
- data coverage issues.

Avoid logging secrets.

### 10.1) Searchability and semantic discovery

Good metadata is not just for humans reading code. It also powers search and discovery across published data nodes.

`DataNodeStorage` now exposes two complementary search paths:

- `description_search(q, ...)`
- `column_search(q, ...)`

Use them differently:

- `description_search(...)` is for natural-language discovery:
  - "close price"
  - "daily portfolio weights"
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

results = msc.DataNodeStorage.description_search(
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

results = msc.DataNodeStorage.column_search(
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

- `POST /{id}/refresh-table-search-index/`

Use it when:

- you improved `get_table_metadata()` or `get_column_metadata()`
- you changed code comments or table-generation logic in a way that should improve discovery
- search results feel stale compared with the current node implementation

Example:

```python
import mainsequence.client as msc

storage = msc.DataNodeStorage.get(pk=123)
storage.refresh_table_search_index()
```

CLI equivalent:

```bash
mainsequence data-node refresh-search-index 123
```

!!! tip "Search quality depends on metadata quality"
    If you want `description_search(...)` to work well, write table and column metadata for humans, not just for machines. Clear descriptions, stable naming, and meaningful column docs directly improve discovery.

## 11) Testing safely

When tests hit shared backends, isolate hashes/tables.

Use:

- `with hash_namespace("..."):` (preferred), or
- `test_node=True` (quick shortcut).

This intentionally changes both `storage_hash` and `update_hash` so tests do not collide with production-like tables.

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
from mainsequence.tdag.data_nodes import hash_namespace

from src.data_nodes.my_node import MyNode, MyNodeConfig


def test_my_node_smoke():
    config = MyNodeConfig(
        ...,
        offset_start="2025-01-01T00:00:00+00:00",
    )

    with hash_namespace("pytest_my_node_smoke"):
        node = MyNode(config=config)
        err, df = node.run(debug_mode=True, force_update=True)

    assert err is False
    assert df is not None
```

Why this pattern works well:

- the namespace isolates hashes from production-like tables
- the production node stays unchanged
- the narrower `config.offset_start` prevents large first-run backfills during tests

If you already have older nodes that still use class-level `OFFSET_START`, that
fallback remains supported, but config-driven `offset_start` is the preferred
pattern for new code and new documentation.

If you want the shortcut form, `test_node=True` is equivalent to using the namespace `"test"`, but explicit namespaces are usually better for parallel test runs.

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
from mainsequence.tdag import APIDataNode

prices = APIDataNode.build_from_identifier("simulated_prices_tutorial")

df = prices.get_df_between_dates(
    start_date="2026-01-01",
    columns=["open", "high", "low", "close"],
    unique_identifier_list=["NVDA", "AAPL"],
)
```

This helper resolves the table metadata for you and returns an `APIDataNode` that is ready for normal read methods.

Use it when:

- you are reading one table by its stable identifier,
- the read shape is fairly standard,
- you want the shortest path from published dataset to DataFrame.

This is especially useful in dashboards, notebooks, and integration code where the table already exists and you are not constructing a dependency graph.

## 14) Client-side filters and joins (B14 summary)

For dynamic table reads, clients send a structured filter payload instead of raw SQL.

Why this matters:

- safer execution (no free-form SQL from clients),
- stable paging (`limit/offset/next_offset`),
- deterministic dtype restoration on the client.

Use this approach when building:

- UI filter builders,
- notebook analysis with ad-hoc slices,
- joins between dynamic tables (for example prices + fundamentals).

The main entry point is `mainsequence.tdag.data_nodes.filters.SearchRequest`, which you submit through `DataNodeStorage.get_data_from_filter(...)`.

```python
import datetime as dt

import mainsequence.client as msc
from mainsequence.tdag.data_nodes.filters import F, SearchRequest, and_

request = SearchRequest(
    node_unique_identifier="simulated_prices_tutorial",
    filter=and_(
        F.between(
            "time_index",
            dt.datetime(2026, 1, 1, tzinfo=dt.UTC),
            dt.datetime(2026, 1, 31, 23, 59, 59, tzinfo=dt.UTC),
        ),
        F.in_("unique_identifier", ["NVDA", "AAPL"]),
    ),
)

df = msc.DataNodeStorage.get_data_from_filter(request)
```

Use `APIDataNode.build_from_identifier(...)` and `get_df_between_dates(...)` when you are reading one table in a fixed way.

Use `SearchRequest` when:

- filters are assembled dynamically by a UI or notebook,
- you need paging and server-side filter execution,
- you need joins between dynamic tables.

They are complementary APIs:

- `APIDataNode.build_from_identifier(...)` is the simple table reader
- `SearchRequest` is the structured query layer for more flexible reads

## 15) Quick pre-ship checklist

Before shipping a DataNode, verify:

- identifier is stable and globally meaningful,
- config fields are correctly split across meaning/scope/runtime,
- `update()` is incremental and uses `UpdateStatistics`,
- assets are resolved for 2D tables,
- metadata methods are implemented,
- schema is intentional and stable,
- logs are useful and secret-safe,
- integration test runs in an isolated namespace.
