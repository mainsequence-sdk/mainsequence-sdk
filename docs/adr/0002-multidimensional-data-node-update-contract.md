# ADR 0002: Multidimensional DataNode Update Contract

Date: 2026-05-20

Status: Proposed

## Context

The server-side `ts_manager.data_nodes` model has moved from a two-shape
timestamped-table contract to a general multidimensional identity contract.

The old client model assumes that a timestamped table is either:

- single-index: `time_index`
- asset-indexed: `time_index`, `unique_identifier`

The server now treats every timestamped table as:

```python
index_names = [time_index_name, *identity_dimensions]
identity_dimensions = index_names[1:]
```

Only the first index is special. It is the time index. Every remaining index is
a normal identity dimension used for uniqueness, update progress, reads,
latest-observation lookup, and tail delete.

Example:

```json
["time_index", "account_uid", "unique_identifier"]
```

This is not a VAM-only special case. Account holdings are one example of the
normal shape: a timestamped table with a higher-dimensional identity coordinate.

Server-to-client naming:

- server `DynamicTableMetaData` == client `DataNodeStorage`
- server `SourceTableConfiguration` == client `SourceTableConfiguration`
- server `LocalTimeSerie` == client update model currently named
  `DataNodeUpdate`

This ADR covers the client SDK architecture and implementation plan. It does
not implement the raw model changes.

## Server References

The source of truth for this client work is the server documentation:

- `/Users/jose/code/MainSequenceServerSide/tdag-django/docs/tdag/ts_manager/data_nodes/source_table_configuration.md`
- `/Users/jose/code/MainSequenceServerSide/tdag-django/docs/tdag/ts_manager/data_nodes/dynamic_table_metadata.md`
- `/Users/jose/code/MainSequenceServerSide/tdag-django/docs/tdag/ts_manager/data_nodes/update_process.md`
- `/Users/jose/code/MainSequenceServerSide/tdag-django/docs/tdag/ts_manager/data_nodes/tail_delete.md`
- `/Users/jose/code/MainSequenceServerSide/tdag-django/docs/tdag/ts_manager/data_nodes/legacy_compatibility.md`
- `/Users/jose/code/MainSequenceServerSide/tdag-django/docs/tdag/ts_manager/data_nodes/client_sdk_handoff_raw_model_changes.md`

## Current Client Architecture

The relevant client-side model surface is concentrated in:

- `mainsequence/client/models_tdag.py`
- `mainsequence/tdag/data_nodes/`
- `mainsequence/tdag/base_persist_managers.py`
- `mainsequence/client/data_sources_interfaces/`
- `mainsequence/virtualfundbuilder/`

The update flow today is:

1. `DataNode.run()` builds an `UpdateRunner`.
2. `UpdateRunner.run()` calls `verify_and_build_remote_objects()`.
3. `PersistManager.local_persist_exist_set_config()` creates or resolves
   `DataNodeStorage` and `DataNodeUpdate`.
4. `DataNodeUpdate.set_start_of_execution()` opens a historical update and
   returns update state.
5. `DataNode.update()` returns a DataFrame.
6. `DataNodeUpdate.upsert_data_into_table()` resets the index into columns,
   creates `SourceTableConfiguration`, computes chunk stats, uploads chunks,
   and patches final update stats.
7. `UpdateRunner` syncs metadata, column metadata, table metadata, and
   `set_end_of_execution()`.

The current implementation already has partial multidimensional behavior:

- `SourceTableConfiguration.index_names` is a list.
- `get_chunk_stats()` groups by `index_names[1:]`.
- `UpdateStatistics.filter_df_by_latest_value()` drills through nested
  coordinate stats.
- `UpdateStatistics.filter_assets_by_level()` expects nested stats.

However, the public contract and most helper names are still asset-specific:

- `asset_time_statistics`
- `max_per_asset_symbol`
- `_PER_ASSET_`
- `unique_identifier_list`
- `unique_identifier_range_map`

Those names make `unique_identifier` look like the canonical second dimension
instead of one possible identity dimension.

## Decision

Adopt a multidimensional DataNode update contract in the Python client SDK.

The client SDK will model timestamped table identity as:

```python
time_index_name = index_names[0]
identity_dimensions = index_names[1:]
```

Update progress will be canonicalized around:

- `global_index_progress`
- `index_progress`
- `index_min`
- `multi_index_column_stats`

Query and delete operations will be canonicalized around:

- `dimension_filters`
- `index_coordinates`
- `dimension_range_map`

Legacy asset-specific names may remain only as temporary compatibility paths.
Every retained old-name path must include a `LEGACY_COMPAT` comment so the next
cleanup can find and remove it deliberately.

`table_partition` will not be retained as a typed client field or compatibility
projection. It has been removed from the backend model and public serializer.
Intentional reads should move to:

- `storage_layout`, for logical persisted storage contract
- `physical_index_plan`, for rendered backend index plan

## SourceTableConfiguration Contract

`SourceTableConfiguration` should represent the server response contract:

```python
class SourceTableConfiguration(...):
    related_table: int | DataNodeStorage
    time_index_name: str
    last_time_index_value: datetime.datetime | None
    earliest_index_value: datetime.datetime | None
    storage_layout: dict[str, Any] | None
    physical_index_plan: dict[str, Any] | None
    multi_index_stats: dict[str, Any] | None
    multi_index_column_stats: dict[str, Any] | None
    open_for_everyone: bool
    columns_metadata: list[ColumnMetaData] | None
    column_index_names: list | None
```

`last_time_index_value` and `earliest_index_value` remain compatibility
projections of the canonical `_GLOBAL_` stats:

- `last_time_index_value` mirrors `multi_index_stats["_GLOBAL_"]["max"]`
- `earliest_index_value` mirrors `multi_index_stats["_GLOBAL_"]["min"]`

`table_partition` must be removed from the typed model surface.

### Creation Payload

Normal SDK creation continues to send the existing minimal intent:

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

The server derives:

- `identity_dimensions = index_names[1:]`
- `index_progress.grain = index_names[1:]`
- `tail_delete.scope_dimensions = index_names[1:]`
- `uniqueness.columns = index_names`
- lookup index columns from `index_names[1:]`
- physical indexes from `storage_layout`

The SDK should not compute or submit `storage_layout` or `physical_index_plan`
for normal creation.

### Data Updates

`SourceTableConfiguration.get_data_updates()` must prefer canonical stats:

- `multi_index_stats["_GLOBAL_"]["max"]` ->
  `UpdateStatistics.max_time_index_value`
- `multi_index_stats["_GLOBAL_"]` ->
  `UpdateStatistics.global_index_progress`
- `multi_index_stats["index_progress"]` ->
  `UpdateStatistics.index_progress`
- `multi_index_stats["index_min"]` ->
  `UpdateStatistics.index_min`

Legacy fallback:

- `max_per_asset_symbol` projects to `index_progress`
- `min_per_asset_symbol` projects to `index_min`

The fallback path must be marked `LEGACY_COMPAT`.

## UpdateStatistics Contract

`UpdateStatistics` remains the client model name, but its canonical fields
change.

Canonical fields:

```python
global_index_progress: dict[str, datetime.datetime | None] | None
index_progress: dict[str, Any] | None
index_min: dict[str, Any] | None
max_time_index_value: datetime.datetime | None
multi_index_column_stats: dict[str, Any] | None
asset_list: list | None
limit_update_time: datetime.datetime | None
is_backfill: bool
```

`max_time_index_value` is a scalar projection of
`global_index_progress["max"]`. It is retained because many producers already
use it for single-index update logic.

All nested timestamp leaves in `global_index_progress`, `index_progress`,
`index_min`, and `multi_index_column_stats` must normalize to timezone-aware
UTC `datetime.datetime` values.

One-index example:

```json
{
  "global_index_progress": {
    "max": "2026-05-01T03:00:00Z",
    "min": "2026-05-01T00:00:00Z"
  },
  "index_progress": {},
  "index_min": {}
}
```

Two-index example:

```json
{
  "global_index_progress": {
    "max": "2026-05-01T03:00:00Z",
    "min": "2026-05-01T00:00:00Z"
  },
  "index_progress": {
    "asset-1": "2026-05-01T02:00:00Z",
    "asset-2": "2026-05-01T03:00:00Z"
  },
  "index_min": {
    "asset-1": "2026-05-01T00:00:00Z",
    "asset-2": "2026-05-01T01:00:00Z"
  }
}
```

Three-index example:

```json
{
  "global_index_progress": {
    "max": "2026-05-01T03:00:00Z",
    "min": "2026-05-01T00:00:00Z"
  },
  "index_progress": {
    "account-a": {
      "asset-1": "2026-05-01T02:00:00Z"
    }
  },
  "index_min": {
    "account-a": {
      "asset-1": "2026-05-01T00:00:00Z"
    }
  }
}
```

For `["time_index", "account_uid", "unique_identifier"]`, nested paths follow:

```text
account_uid -> unique_identifier -> timestamp
```

### Compatibility Projection

`asset_time_statistics` is not the canonical typed contract.

If retained temporarily, it must be implemented as a `LEGACY_COMPAT` projection
of `index_progress`, not as the source of truth. New SDK code must not read or
write it.

## LocalTimeSerie Client Contract

The server `LocalTimeSerie` flow maps to the client class currently named
`DataNodeUpdate`.

### `set_start_of_execution()`

Current client behavior reads:

- `last_time_index_value`
- `asset_time_statistics`

Required client behavior reads:

- `global_index_progress`
- `index_progress`
- `index_min`
- `multi_index_column_stats`
- `time_index_name`
- `index_names`

It should construct `UpdateStatistics` from canonical fields and set
`max_time_index_value` from `global_index_progress["max"]`.
The backend response for this method is canonical. SDK code must not read
`last_time_index_value` or `asset_time_statistics` from this response.

### Chunk Stats

Current client helper behavior emits:

```json
{
  "_GLOBAL_": {"min": 1778764800.0, "max": 1779373200.0},
  "_PER_ASSET_": {
    "account-a": {
      "asset-1": {"min": 1778764800.0, "max": 1779373200.0}
    }
  }
}
```

The value under `_PER_ASSET_` is already structurally a generic coordinate map,
but the name is wrong.

New helper behavior should emit:

```json
{
  "_GLOBAL_": {
    "min": "2026-05-01T00:00:00Z",
    "max": "2026-05-01T03:00:00Z"
  },
  "index_progress": {
    "account-a": {
      "asset-1": "2026-05-01T02:00:00Z"
    }
  },
  "index_min": {
    "account-a": {
      "asset-1": "2026-05-01T00:00:00Z"
    }
  }
}
```

Rules:

- one-index tables use empty `index_progress` and `index_min`
- two-index tables use one nested level keyed by `index_names[1]`
- three-index tables use two nested levels keyed by `index_names[1]` and
  `index_names[2]`
- N-index tables use N-1 nested identity levels

Introduce a dimension-aware helper such as:

```python
get_index_progress_chunk_stats(chunk_df, *, time_index_name, index_names)
```

`get_chunk_stats()` may remain temporarily only as a `LEGACY_COMPAT` wrapper if
needed to migrate `SimpleTable` or other downstream imports in a separate step.
It must not remain the canonical helper.

### `set_last_update_index_time_from_update_stats()`

Replace the old method signature:

```python
set_last_update_index_time_from_update_stats(
    last_time_index_value: float,
    max_per_asset_symbol,
    multi_index_column_stats,
)
```

with the canonical keyword-only shape:

```python
set_last_update_index_time_from_update_stats(
    *,
    global_index_progress: dict[str, Any],
    index_progress: dict[str, Any],
    index_min: dict[str, Any],
    multi_index_column_stats: dict[str, Any] | None = None,
)
```

The backend does not accept legacy keys on this write path. The SDK
builder/validator for this method must use `extra="forbid"` or an equivalent
strict-key check and must emit only canonical decoded payload shapes.

Canonical top-level shape:

```json
{
  "global_index_progress": {
    "max": "2026-05-01 03:00:00+00:00",
    "min": "2026-05-01 00:00:00+00:00"
  },
  "index_progress": {
    "account-a": {
      "asset-1": "2026-05-01 02:00:00+00:00"
    }
  },
  "index_min": {
    "account-a": {
      "asset-1": "2026-05-01 00:00:00+00:00"
    }
  },
  "multi_index_column_stats": {}
}
```

Equivalent nested shape:

```json
{
  "multi_index_stats": {
    "_GLOBAL_": {
      "max": "2026-05-01 03:00:00+00:00",
      "min": "2026-05-01 00:00:00+00:00"
    },
    "index_progress": {},
    "index_min": {}
  },
  "multi_index_column_stats": {}
}
```

For multi-identity tables, `index_progress` and `index_min` use the same
nested coordinate shape shown in the top-level example.

Strict allowed keys:

- top-level: `_GLOBAL_`, `global_index_progress`, `index_progress`,
  `index_min`, `multi_index_stats`, `multi_index_column_stats`
- inside `multi_index_stats`: `_GLOBAL_`, `index_progress`, `index_min`

The outbound model must not include old names as explicit forbidden examples in
the model logic. Names such as `last_time_index_value`,
`max_per_asset_symbol`, `min_per_asset_symbol`, and
`asset_time_statistics` are simply unknown keys under the strict contract and
must fail the same way as any other unknown key.

### `upsert_data_into_table()`

`DataNodeUpdate.upsert_data_into_table()` must:

1. Preserve the existing minimal source configuration creation payload.
2. Validate that every `index_names` column exists in `column_dtypes_map`.
3. Detect duplicates over the full `index_names` tuple, not only
   `(time_index, unique_identifier)`.
4. Compute canonical global and per-coordinate progress.
5. Build per-column stats with the same coordinate shape.
6. Call `set_last_update_index_time_from_update_stats()` using canonical
   fields.

## DataNodeStorage Query Contract

Canonical query and mutation inputs are dimension-aware:

```python
dimension_filters: dict[str, list[Any]] | None
index_coordinates: list[dict[str, Any]] | None
dimension_range_map: list[dict[str, Any]] | None
```

Dimension filter example:

```json
{
  "dimension_filters": {
    "account_uid": ["account-a"],
    "unique_identifier": ["BTC", "ETH"]
  }
}
```

Exact coordinate example:

```json
{
  "index_coordinates": [
    {"account_uid": "account-a", "unique_identifier": "BTC"}
  ]
}
```

Per-coordinate range example:

```json
{
  "dimension_range_map": [
    {
      "coordinate": {"account_uid": "account-a", "unique_identifier": "BTC"},
      "start_date": 1778764800,
      "end_date": 1779373200
    }
  ]
}
```

Affected methods:

- `DataNodeStorage.delete_after_date()`
- `DataNodeStorage.get_last_observation()`
- `DataNodeStorage._get_data_between_dates_common()`
- `DataNodeStorage.get_data_between_dates_from_api()`
- `DataNodeStorage.get_data_between_dates_from_node_identifier()`
- `DataSource.get_data_by_time_index()`
- `DynamicTableDataSource.get_data_by_time_index()`
- `TimeScaleDB.get_data_by_time_index()`
- `DataAccessMixin.get_df_between_dates()`

Legacy aliases:

- `unique_identifier_list`
- `unique_identifier_range_map`

These aliases are valid only when `index_names[1:] == ["unique_identifier"]`.
Translation belongs in the client communication layer and high-level wrappers,
before calls reach storage-interface code. Backend and storage-interface code
should receive canonical dimension payloads.

## Tail Delete Contract

`delete_after_date()` remains a suffix trim operation, not an arbitrary range
delete.

Global delete:

```json
{
  "after_date": "2026-04-01T00:00:00Z"
}
```

Coordinate-scoped delete:

```json
{
  "after_date": "2026-04-01T00:00:00Z",
  "index_coordinates": [
    {"account_uid": "account-a", "unique_identifier": "AAPL"}
  ]
}
```

`unique_identifier` and `unique_identifier_list` may remain only as
`LEGACY_COMPAT` communication-layer aliases for two-index
`["time_index", "unique_identifier"]` tables.

After delete, the returned stats are authoritative replacement snapshots. The
client should not merge them as append stats.

## Data Source Interface Contract

DuckDB and Timescale helpers must stop treating `unique_identifier` as the
canonical second key.

DuckDB changes:

- `read()` should accept `dimension_filters` and `dimension_range_map`.
- projected columns should include all configured `index_names`, not just
  `time_index` and `unique_identifier`.
- merge/deduplication should partition by full `index_names`.
- `constrain_read()` should understand `dimension_range_map`.

Timescale changes:

- direct reads should construct predicates from dimension names.
- direct update overwrite/delete conditions should scope by all identity
  dimensions in `grouped_dates`, not only `unique_identifier`.
- direct insertion should continue to write the full reset-index DataFrame.

These adapter changes are part of the same architecture migration because
client-side local execution and direct data source paths must observe the same
coordinate contract as the server.

## SearchRequest And Joins

The existing structured filter API in `mainsequence.tdag.data_nodes.filters`
currently restricts joins to:

```python
["time_index", "unique_identifier"]
```

That is correct only for legacy two-index asset tables. The long-term contract
should allow joins over arbitrary configured index names.

This ADR does not require the first raw-model PR to fully generalize joins, but
the current restriction must be marked as a known follow-up. Any new
three-index table cannot be joined correctly through `SearchRequest` until this
validator and response dtype/index restoration are generalized.

## Compatibility Policy

Backward compatibility means existing two-index asset workflows keep working.
It does not mean the old names remain canonical.

Temporary compatibility projections:

- `asset_time_statistics` -> projection of `index_progress`
- `max_per_asset_symbol` -> projection of `index_progress`
- `min_per_asset_symbol` -> projection of `index_min`
- `last_time_index_value` -> projection of `_GLOBAL_.max`
- `earliest_index_value` -> projection of `_GLOBAL_.min`
- `unique_identifier_list` -> communication-layer alias for
  `dimension_filters={"unique_identifier": values}` only when the only identity
  dimension is `unique_identifier`
- `unique_identifier_range_map` -> communication-layer alias for
  `dimension_range_map` only when the only identity dimension is
  `unique_identifier`

Every retained compatibility path must include:

```python
# LEGACY_COMPAT: ...
```

No compatibility projection:

- `table_partition`

## Consequences

Positive:

- DataNodes can represent timestamped tables with any number of identity
  dimensions.
- Update progress becomes generic and usable for account, portfolio, strategy,
  venue, asset, or other coordinate shapes.
- The uniqueness contract becomes unambiguous: the full time-first index tuple.
- Tail delete and latest-observation lookups can be scoped to full coordinates
  instead of one hardcoded identifier.
- Existing asset tables can keep working through explicit compatibility aliases.

Negative:

- This is a broad SDK migration. Pydantic raw model changes alone are not
  enough.
- Existing downstream code that reads `asset_time_statistics` must be audited.
- Local DuckDB paths and direct Timescale paths need nontrivial query-builder
  changes.
- Generated reference docs will be stale until regenerated.
- Tutorial language that says only `(time_index, unique_identifier)` is allowed
  will become actively wrong.

## Blast Radius

### Primary Raw Model Surface

`mainsequence/client/models_tdag.py` is the largest change point.

Hotspots:

- `SourceTableConfiguration`
- `DataNodeUpdate.set_start_of_execution()`
- `DataNodeUpdate.set_last_update_index_time_from_update_stats()`
- `DataNodeUpdate.upsert_data_into_table()`
- `DataNodeStorage.delete_after_date()`
- `DataNodeStorage.get_last_observation()`
- `DataNodeStorage._get_data_between_dates_common()`
- `DataNodeStorage.get_data_between_dates_from_api()`
- `DataNodeStorage.get_data_between_dates_from_node_identifier()`
- `UpdateStatistics`
- `get_chunk_stats()`

Current search found concentrated legacy terminology in this file, including
`asset_time_statistics`, `max_per_asset_symbol`, `unique_identifier_list`,
`unique_identifier_range_map`, `_PER_ASSET_`, and `table_partition`.

### TDAG Runtime

Affected files:

- `mainsequence/tdag/data_nodes/data_nodes.py`
- `mainsequence/tdag/data_nodes/persist_managers.py`
- `mainsequence/tdag/base_persist_managers.py`
- `mainsequence/tdag/data_nodes/filters.py`

The `DataAccessMixin.get_df_between_dates()` API is user-facing and currently
documents and forwards `unique_identifier_list` and `unique_identifier_range_map`.
It should gain canonical dimension parameters while retaining legacy aliases for
asset tables.

The dependency/update runner orchestration is mostly unaffected because it
passes `UpdateStatistics` around without knowing the dimension shape. The
statistics object and persistence methods are the important runtime changes.

### Data Source Interfaces

Affected files:

- `mainsequence/client/data_sources_interfaces/duckdb.py`
- `mainsequence/client/data_sources_interfaces/timescale.py`

DuckDB currently hardcodes `unique_identifier` in read filters, merge keys,
deduplication, and range maps. Timescale direct paths hardcode
`unique_identifier` in predicates and overwrite delete conditions.

These paths are important because tests and local runs can bypass the server's
canonical normalization. They must not keep translating legacy aliases after the
communication layer.

### SimpleTable Coupling

`mainsequence/client/models_simple_tables.py` imports and uses
`get_chunk_stats()` from `models_tdag.py`.

That means the DataNode helper rename cannot be done blindly. Either:

1. migrate SimpleTable to the new helper in the same PR, or
2. keep `get_chunk_stats()` as a `LEGACY_COMPAT` wrapper until SimpleTable is
   migrated.

### VirtualFundBuilder And Instruments

Affected files include:

- `mainsequence/virtualfundbuilder/portfolio_nodes.py`
- `mainsequence/virtualfundbuilder/resource_factory/signal_factory.py`
- `mainsequence/virtualfundbuilder/contrib/prices/data_nodes.py`
- `mainsequence/virtualfundbuilder/contrib/data_nodes/market_cap.py`
- `mainsequence/virtualfundbuilder/contrib/data_nodes/portfolio_replicator.py`
- `mainsequence/virtualfundbuilder/utils.py`
- `mainsequence/instruments/data_interface/data_interface.py`

These callers currently read `asset_time_statistics` or construct
`unique_identifier_range_map` directly. For two-index market data this remains a
valid legacy shape, but new code should move to generic coordinate helpers.

### CLI And Generated References

Affected CLI surface:

- `mainsequence/cli/cli.py` detail rendering of `table_index_names`
- CLI docs/reference pages after model docstrings/signatures change

Generated docs under `docs/reference/` should be regenerated after code changes
instead of hand-edited.

### Tutorials And Knowledge Docs

Docs currently encode the old two-shape model in user-facing rules:

- `docs/tutorial/creating_a_simple_data_node.md`
- `docs/tutorial/multi_index_columns_working_with_assets.md`
- `docs/knowledge/data_nodes.md`
- `docs/tutorial/dashboards/streamlit/streamlit_integration_1.md`
- `docs/tutorial/markets_tutorial/markets_equities_with_algoseek.md`
- VFB knowledge/tutorial pages that describe signal or portfolio indexes as
  only `(time_index, unique_identifier)`

These need a careful rewrite after SDK behavior lands. The docs should teach:

- first index is always time
- remaining indexes are identity dimensions
- `(time_index, unique_identifier)` is the standard asset table shape, not the
  only valid MultiIndex shape
- generic read/delete APIs use dimensions and coordinates
- legacy `unique_identifier_*` helpers are asset-table convenience aliases

## Minimum Regression Tests

Add tests for three table shapes:

1. one-index table: `["time_index"]`
2. legacy two-index table: `["time_index", "unique_identifier"]`
3. generic three-index table:
   `["time_index", "account_uid", "unique_identifier"]`

Required test coverage:

- `SourceTableConfiguration` parses `storage_layout` and
  `physical_index_plan`.
- `SourceTableConfiguration` rejects or ignores no `table_partition`
  dependency in typed parsing.
- source configuration creation still sends the minimal payload.
- `get_data_updates()` prefers canonical `multi_index_stats`.
- `get_data_updates()` falls back from legacy `max_per_asset_symbol` with
  `LEGACY_COMPAT`.
- `UpdateStatistics` normalizes nested timestamps in `global_index_progress`,
  `index_progress`, `index_min`, and `multi_index_column_stats`.
- `set_start_of_execution()` prefers canonical response fields.
- `set_last_update_index_time_from_update_stats()` sends canonical compressed
  `multi_index_stats`.
- `set_last_update_index_time_from_update_stats()` validates outbound payloads
  with strict allowed keys and rejects unknown keys through `extra="forbid"` or
  an equivalent generic unknown-key mechanism.
- `get_index_progress_chunk_stats()` returns correct global/index progress for
  one-, two-, and three-index frames.
- `upsert_data_into_table()` detects duplicates over full `index_names`.
- `get_last_observation()` can send `dimension_filters` and
  `index_coordinates`.
- `get_data_between_dates_from_api()` can send `dimension_range_map`.
- `delete_after_date()` can send coordinate-scoped `index_coordinates`.
- legacy `unique_identifier_list` works only for two-index asset tables.
- legacy `unique_identifier_range_map` works only for two-index asset tables.
- legacy aliases fail or raise clearly when identity dimensions are not exactly
  `["unique_identifier"]`.

## Implementation Tasks

### Phase 1: Raw Model Contract

- [x] Update `SourceTableConfiguration` in `mainsequence/client/models_tdag.py`.
- [x] Add `storage_layout`, `physical_index_plan`, and optional
   `multi_index_stats`.
- [x] Remove `table_partition` from the typed model.
- [x] Keep `last_time_index_value` and `earliest_index_value` as optional
   compatibility projections.
- [x] Update `SourceTableConfiguration.get_data_updates()` to prefer canonical
   stats and mark legacy fallback paths with `LEGACY_COMPAT`.
- [x] Add tests for parsing canonical source configuration responses.

### Phase 2: UpdateStatistics

- [x] Add `global_index_progress`, `index_progress`, and `index_min`.
- [x] Normalize all nested timestamp leaves to timezone-aware UTC datetimes.
- [x] Keep `max_time_index_value` as a projection of
   `global_index_progress["max"]`.
- [x] Replace internal helper implementations that read `asset_time_statistics`
   with `index_progress`.
- [x] Keep `asset_time_statistics` only as a temporary `LEGACY_COMPAT` projection
   if needed for downstream compatibility.
- [x] Add tests for one-, two-, and three-index update statistics.

### Phase 3: LocalTimeSerie Update Flow

- [x] Update `DataNodeUpdate.set_start_of_execution()` to consume canonical
   response fields.
- [x] Add `get_index_progress_chunk_stats()`.
- [x] Replace `_PER_ASSET_`-based SDK payload construction with canonical
   `index_progress` and `index_min`.
- [x] Update `DataNodeUpdate.upsert_data_into_table()` to compute canonical stats
   and detect duplicates over full `index_names`.
- [x] Replace `set_last_update_index_time_from_update_stats()` with the new
   keyword-only canonical signature.
- [x] Add a strict outbound payload model or builder validation for
   `set_last_update_index_time_from_update_stats()` that allows only canonical
   keys and relies on generic unknown-key rejection for legacy names.
- [x] Add tests that inspect the compressed payload sent to the backend.
- [x] Add tests for both allowed decoded payload shapes and for unknown-key
   rejection at the top level and inside `multi_index_stats`.

### Phase 4: Query And Tail Delete APIs

- [ ] Add canonical `dimension_filters`, `index_coordinates`, and
   `dimension_range_map` parameters to `DataNodeStorage` read, latest, and
   delete methods.
- [ ] Implement legacy alias translation only in the client communication layer.
- [ ] Validate that `unique_identifier_list` and `unique_identifier_range_map`
   aliases are used only for `["time_index", "unique_identifier"]` tables.
- [ ] Update `DataAccessMixin.get_df_between_dates()` and persist managers to pass
   canonical dimension arguments.
- [ ] Add tests for latest-observation, range reads, node-identifier reads, and
   tail delete.

### Phase 5: Data Source Adapters

- [ ] Update DuckDB read filters to use arbitrary dimension filters and coordinate
   ranges.
- [ ] Update DuckDB merge and deduplication keys to use full `index_names`.
- [ ] Update DuckDB row-limit constraining to understand `dimension_range_map`.
- [ ] Update Timescale direct reads to build predicates from dimension names.
- [ ] Update Timescale overwrite delete conditions to scope by all identity
   dimensions.
- [ ] Add focused adapter tests where feasible without requiring a live backend.

### Phase 6: Downstream Runtime Audit

- [ ] Audit and migrate `mainsequence/tdag/data_nodes/`.
- [ ] Audit and migrate `mainsequence/tdag/base_persist_managers.py`.
- [ ] Audit and migrate `mainsequence/virtualfundbuilder/`.
- [ ] Audit and migrate `mainsequence/instruments/`.
- [ ] Audit `mainsequence/client/models_simple_tables.py` for the shared chunk
   stats helper dependency.
- [ ] Replace direct reads of `asset_time_statistics` with canonical helper calls
   or clearly marked `LEGACY_COMPAT` projections.

### Phase 7: Search And Joins

- [ ] Generalize `mainsequence.tdag.data_nodes.filters.JoinKey` beyond the
   hardcoded enum if joins need to support higher-dimensional tables.
- [ ] Relax `SearchRequest` validation so `JoinSpec.on` can match arbitrary
   configured index dimensions.
- [ ] Update client response dtype/index restoration to set indexes according to
   the join keys returned by the server.
- [ ] Add tests for two-index and three-index joins once server support is exposed.

### Phase 8: CLI And Documentation

- [ ] Update CLI detail rendering to show `storage_layout` and
   `physical_index_plan` where useful.
- [ ] Remove `table_partition` from user-facing docs and examples.
- [ ] Regenerate `docs/reference/` after code signatures and docstrings are
   updated.
- [ ] Add migration notes to changelog or release notes.

### Phase 9: Tutorial Fixes

- [ ] Update `docs/tutorial/creating_a_simple_data_node.md` so MultiIndex rules
   say: first level is UTC `time_index`; remaining levels are identity
   dimensions. Explain that `(time_index, unique_identifier)` is the common
   asset-table case, not the only valid MultiIndex shape.
- [ ] Update `docs/tutorial/multi_index_columns_working_with_assets.md` to frame
   asset tables as the standard two-index specialization of the generic
   multidimensional contract.
- [ ] Update `docs/knowledge/data_nodes.md` with a new section on
   multidimensional identity dimensions, canonical update stats, generic
   dimension reads, and coordinate-scoped tail delete.
- [ ] Update dashboard and market tutorials that pass `unique_identifier_list` or
   `unique_identifier_range_map` to mention these are legacy asset-table
   convenience aliases.
- [ ] Update VFB docs that describe portfolio or signal indexes. Keep
   `(time_index, unique_identifier)` where it is still the actual contract, but
   remove language implying it is the only legal DataNode shape.
- [ ] Update examples and generated snippets that say DataNode MultiIndex tables
   can only have two levels.
- [ ] Add at least one tutorial or knowledge example for a three-index table such
   as `["time_index", "account_uid", "unique_identifier"]`.
