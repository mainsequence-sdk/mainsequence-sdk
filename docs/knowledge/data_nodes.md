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
- Config model: `<Something>Config` based on `pydantic.BaseModel`
- Table identifier: lowercase `snake_case`, stable, meaning-based (`fx_ecb_daily_rates`)
- Dependency keys: short and descriptive (`"prices"`, `"rates"`, `"raw"`)

## 4) Config design: meaning vs scope vs operational knobs

A simple and scalable pattern is:

- one Pydantic config object for dataset and scope fields,
- operational runtime knobs outside `__init__`.

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
Field(..., json_schema_extra={"ignore_from_storage_hash": True})
```

### 4.3 Operational knobs (affect neither hash)

Do not put these in `__init__` args.

Examples:

- batch size
- retries/timeouts/backoff
- debug flags
- secrets/credentials

Keep them in env vars or runtime config read inside `update()`.

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

## 5.1) Build metadata (`build_meta_data`)

`build_meta_data` controls backend behavior and is not part of hash identity.

Common example:

- `initialize_with_default_partitions` (defaults to `True`)

Only change build metadata when you understand the storage/layout impact.

## 6) Update strategy: incremental by default

Use `UpdateStatistics` to minimize work and control windows intentionally.

### 6.1 Single-index pattern

- first run: start at `OFFSET_START` (UTC-aware)
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

Log useful operational facts:

- chosen update window,
- number of assets processed,
- rows produced,
- data coverage issues.

Avoid logging secrets.

## 11) Testing safely

When tests hit shared backends, isolate hashes/tables.

Use:

- `with hash_namespace("..."):` (preferred), or
- `test_node=True` (quick shortcut).

This intentionally changes both `storage_hash` and `update_hash` so tests do not collide with production-like tables.

Keep test runs bounded:

- use a narrow `OFFSET_START` for test nodes,
- or pass controlled update statistics/checkpoints,
- keep integration tests small and deterministic.

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
