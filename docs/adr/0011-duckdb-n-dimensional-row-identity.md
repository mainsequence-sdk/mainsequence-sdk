# ADR 0011: DuckDB N-Dimensional Row Identity

Date: 2026-05-25

Status: Accepted

## Context

DuckDB local-data mode is a supported SDK persistence adapter when the physical
`DataSource.class_type` is `"duck_db"`.

DataNode storage identity is defined by the configured time-first index:

```python
index_names = [time_index_name, *identity_dimensions]
identity_dimensions = index_names[1:]
```

`identity_dimensions` can contain zero, one, or many dimensions. Examples:

```python
["time_index"]
["time_index", "unique_identifier"]
["time_index", "account_uid", "unique_identifier"]
["event_time", "portfolio_uid", "asset_uid", "venue", "scenario"]
```

DuckDB must therefore use the full configured `index_names` tuple for row
identity. It must not infer identity from any one domain column.

## Decision

DuckDB row identity is driven exclusively by `index_names` and
`time_index_name`.

- Writes require every configured index column to exist in the serialized
  DataFrame.
- Deduplication, collision detection, append anti-joins, and rewrite predicates
  use the full `index_names` tuple.
- Reads always project every configured index column, even when value columns
  are narrowed.
- Date predicates use `time_index_name`, not a hardcoded column name.
- Read scoping uses `dimension_filters`, `index_coordinates`, and
  `dimension_range_map`.
- Earliest-value calculations return the global temporal minimum and
  per-coordinate minima keyed by the configured identity dimensions.

## Non-Negotiable Rules

- Do not replace identity with the last index column.
- Do not add synthetic identity columns for missing dimensions.
- Do not allow `columns=[...]` reads to omit configured index columns.
- Do not treat date partition columns as identity columns unless they are
  explicitly part of `index_names`.
- Do not change the backend API contract while fixing local DuckDB behavior.

## Implementation Status

- [x] Pass `index_names` and `time_index_name` into DuckDB upserts.
- [x] Require `index_names` and `time_index_name` for DuckDB writes.
- [x] Deduplicate incoming rows using full `index_names`.
- [x] Build collision and append predicates from full `index_names`.
- [x] Order written Parquet output by full `index_names`.
- [x] Preserve every configured index column in DuckDB views.
- [x] Support canonical read filters, coordinates, and range maps.
- [x] Ensure DuckDB reads always project all configured `index_names`.
- [x] Use `time_index_name` for temporal predicates.
- [x] Return per-coordinate minima from `time_index_minima(...)`.
- [x] Add tests for three-index row identity.
- [x] Add tests for column-subset reads with full index projection.
- [x] Add tests for canonical DuckDB read scoping.

## Validation

A local DuckDB table with:

```python
index_names = ["time_index", "account_uid", "asset_uid"]
```

must persist and read both rows below without collapse:

```text
time_index                 account_uid  asset_uid  value
2026-05-25T00:00:00Z       acct-a       asset-1    10
2026-05-25T00:00:00Z       acct-b       asset-1    20
```

The read result must have a MultiIndex over:

```python
["time_index", "account_uid", "asset_uid"]
```
