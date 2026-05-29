# ADR 0011: DuckDB N-Dimensional Row Identity

Date: 2026-05-25

Status: Accepted

## Related ADRs

- ADR 0002: Multidimensional DataNode Update Contract
- ADR 0010: Remove Legacy Timescale Direct Interface

## Context

DuckDB local-data mode is still a supported SDK path. It is the local
persistence adapter used when the physical `DataSource.class_type` is
`"duck_db"`.

The current DuckDB adapter is still shaped around the legacy asset-table
contract:

```python
["time_index", "unique_identifier"]
```

That is no longer the DataNode identity contract.

The current contract is defined by `SourceTableConfiguration.index_names`:

```python
index_names = [time_index_name, *identity_dimensions]
```

`identity_dimensions` can contain zero, one, or many dimensions. Examples:

```python
["time_index"]
["time_index", "unique_identifier"]
["time_index", "account_uid", "unique_identifier"]
["event_time", "portfolio_uid", "asset_uid", "venue", "scenario"]
```

The DuckDB implementation currently hardcodes `unique_identifier` in the write
and read paths:

- `DataSource.insert_data_into_table(...)` calls `DuckDBInterface.upsert(...)`
  without passing `index_names` or `time_index_name`.
- `DuckDBInterface.upsert(...)` adds a synthetic `unique_identifier` when absent.
- incoming batches are deduplicated on `["time_index", "unique_identifier"]`.
- collision detection joins existing and incoming data on `time_index` and
  `unique_identifier`.
- append anti-join and rewrite merge logic use only those two columns.
- read projection always forces `time_index` and `unique_identifier` when
  `columns` is provided.
- read filtering supports `ids` and `unique_identifier_range_map`, but rejects
  the canonical `dimension_filters`, `index_coordinates`, and
  `dimension_range_map` path in `DataSource.get_data_by_time_index(...)`.

This can corrupt local DuckDB tables for N-dimensional DataNodes. Distinct rows
that share `time_index` and `unique_identifier` but differ on another identity
dimension can be collapsed, skipped, or overwritten.

## Decision

DuckDB row identity must be driven exclusively by
`SourceTableConfiguration.index_names`.

The DuckDB adapter must not infer row identity from `unique_identifier`, and it
must not synthesize `unique_identifier` unless that column is explicitly present
in `index_names` or the source DataFrame columns.

The full configured `index_names` tuple is the primary key shape for local
DuckDB persistence:

```python
primary_key_columns = data_node_storage.sourcetableconfiguration.index_names
time_index_name = data_node_storage.sourcetableconfiguration.time_index_name
identity_dimensions = primary_key_columns without time_index_name
```

`time_index_name` remains the temporal column used for partition pruning and
date range predicates. It is not always literally named `"time_index"`.

`unique_identifier` remains a valid dimension name only for legacy or
asset-shaped tables whose `index_names` explicitly include it. It is not a
canonical special column.

## Required Behavior

### Writes

DuckDB writes must:

- receive `index_names` and `time_index_name` from `DataSource.insert_data_into_table(...)`;
- validate that every `index_names` column exists in the serialized DataFrame;
- validate that `time_index_name` is in `index_names`;
- normalize only `time_index_name` as the temporal column;
- deduplicate incoming rows on the full `index_names` tuple;
- detect existing row collisions by joining on the full `index_names` tuple;
- anti-join existing rows using the full `index_names` tuple;
- rewrite colliding partitions using the full `index_names` tuple;
- order output deterministically by `index_names`;
- preserve all identity dimension columns in Parquet files and DuckDB views.

For a one-dimensional table, `index_names == [time_index_name]` is valid. In
that case, the time column alone is the row identity.

### Reads

DuckDB reads must:

- receive `index_names` and `time_index_name`;
- always include all `index_names` columns in the projection, even when the user
  requests a subset of value columns;
- use `time_index_name` for date range predicates instead of hardcoded
  `"time_index"`;
- support canonical `dimension_filters`;
- support canonical `index_coordinates`;
- support canonical `dimension_range_map`;
- continue to set the DataFrame index with `stc.index_names` after reading;
- reject legacy `ids` or `unique_identifier_range_map` unless
  `identity_dimensions == ["unique_identifier"]`.

The canonical filtering semantics should match the backend API:

- `dimension_filters={dimension: [values...]}` means `dimension IN (...)`.
- `index_coordinates=[{dimension: value, ...}]` means an OR of exact coordinate
  predicates.
- `dimension_range_map` means an OR of coordinate-specific temporal ranges.

### Row Limiting

`DuckDBInterface.constrain_read(...)` must stop accepting only
`ids` and `unique_identifier_range_map`.

It must accept the canonical filter shape and use the adjusted window it returns.
`DataSource.get_data_by_time_index(...)` must pass the adjusted start, end, and
range map into `DuckDBInterface.read(...)`; computing adjusted values and then
discarding them is not sufficient.

### Earliest Value

`DataSource.get_earliest_value(...)` and `DuckDBInterface.time_index_minima(...)`
must not return statistics keyed only by `unique_identifier`.

The local DuckDB earliest-value path must return:

- the global minimum temporal value; and
- per-coordinate minima keyed by the configured identity dimensions.

For a table with `identity_dimensions == ["account_uid", "asset_uid"]`, the
coordinate map must preserve both dimensions. It must not flatten or collapse
onto one column.

## Non-Negotiable Rules

- Do not replace `unique_identifier` with "the last index column"; use the full
  `index_names` tuple.
- Do not add synthetic identity columns for missing dimensions.
- Do not keep any DuckDB read/write merge predicate that hardcodes
  `unique_identifier`.
- Do not allow `columns=[...]` reads to omit configured index columns.
- Do not treat date partition columns (`year`, `month`, `day`) as identity
  columns unless they are explicitly part of `index_names`.
- Do not change the backend API contract while fixing local DuckDB behavior.

## Scope

In scope:

- `mainsequence/client/models_foundry.py`
- `mainsequence/client/data_sources_interfaces/duckdb.py`
- focused DuckDB adapter tests
- focused DataSource dispatch tests
- any ADR 0002 checklist updates needed after implementation

Out of scope:

- removing DuckDB local-data mode
- changing non-DuckDB API-backed persistence
- adding direct Timescale/Postgres behavior back
- changing MetaTable relational storage behavior

## Implementation Tasks

- [x] Pass `index_names` and `time_index_name` from
  `DataSource.insert_data_into_table(...)` into `DuckDBInterface.upsert(...)`.
- [x] Change `DuckDBInterface.upsert(...)` to require `index_names` and
  `time_index_name`.
- [x] Remove synthetic `unique_identifier = ""` insertion from DuckDB upsert.
- [x] Deduplicate incoming DuckDB rows using full `index_names`.
- [x] Build collision join predicates from full `index_names`.
- [x] Build append anti-join predicates from full `index_names`.
- [x] Build rewrite merge predicates from full `index_names`.
- [x] Order written Parquet output by full `index_names`.
- [x] Ensure `_ensure_view(...)` preserves every configured index column.
- [x] Update `DuckDBInterface.read(...)` to accept `index_names`,
  `time_index_name`, `dimension_filters`, `index_coordinates`, and
  `dimension_range_map`.
- [x] Ensure DuckDB reads always project all configured `index_names`.
- [x] Replace hardcoded `time_index` read predicates with `time_index_name`.
- [x] Keep `unique_identifier_range_map` only as a guarded compatibility alias
  for `index_names == [time_index_name, "unique_identifier"]`.
- [x] Update `constrain_read(...)` to accept canonical dimension filters and
  return adjusted canonical filter inputs.
- [x] Use adjusted `constrain_read(...)` outputs in
  `DataSource.get_data_by_time_index(...)`.
- [x] Update `time_index_minima(...)` to group by configured identity
  dimensions, not `unique_identifier`.
- [x] Add tests for a three-index DataNode where two rows share
  `time_index` and one identity dimension but differ on another dimension.
- [x] Add tests proving column-subset reads still return all index columns.
- [x] Add tests for `dimension_filters`, `index_coordinates`, and
  `dimension_range_map` against DuckDB.
- [x] Add tests proving legacy `unique_identifier_range_map` is rejected for
  non-asset-shaped N-dimensional tables.

## Validation

The fix is complete when a local DuckDB table with:

```python
index_names = ["time_index", "account_uid", "asset_uid"]
```

can persist and read both rows below without collapse:

```text
time_index                 account_uid  asset_uid  value
2026-05-25T00:00:00Z       acct-a       asset-1    10
2026-05-25T00:00:00Z       acct-b       asset-1    20
```

The read result must have a MultiIndex over:

```python
["time_index", "account_uid", "asset_uid"]
```

and must preserve both values.

## Compatibility

Existing two-index asset-shaped local DuckDB tables continue to work when:

```python
index_names == [time_index_name, "unique_identifier"]
```

However, that shape is now one specific DataNode layout, not the implicit global
contract.

Any caller relying on DuckDB to synthesize a missing `unique_identifier` column
must migrate to a valid DataNode configuration whose DataFrame contains every
configured index column.
