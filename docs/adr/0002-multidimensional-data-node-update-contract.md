# ADR 0002: Multidimensional DataNode Update Contract

Date: 2026-05-20

Status: Accepted

## Context

DataNode storage is a timestamped table with a time-first index:

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

This is not an asset-specific shape. Higher-dimensional identity coordinates
are a normal timestamped table shape.

## Decision

The SDK runtime uses only the generic multidimensional contract.

- DataFrame validation requires the first index to be a UTC
  `datetime64[ns, UTC]` time index.
- Uniqueness is the full time-first index tuple, not any individual identity
  dimension.
- Update statistics use `global_index_progress`, `index_progress`, and
  `index_min`.
- Chunk statistics are computed with `get_index_progress_chunk_stats()`.
- Data reads and tail deletes use `dimension_filters`, `index_coordinates`, and
  `dimension_range_map`.
- Local DuckDB and SQLite interfaces accept only canonical dimension parameters.
- `DataAccessMixin.get_df_between_dates()` exposes only canonical dimension
  parameters.

## Runtime Contract

`DataNodeUpdate.upsert_data_into_table()` resets a DataFrame index into columns,
computes canonical progress statistics, serializes the DataFrame using the
registered storage contract, uploads chunks, and patches final update stats.

The write path must not derive storage schema from ad hoc DataFrame metadata.
Storage schema comes from the bound `PlatformTimeIndexMetaData` /
`TimeIndexMetaData` contract.

## Read Contract

Dimension scoping has three forms:

- `dimension_filters`: column-to-values filters.
- `index_coordinates`: exact coordinate dictionaries.
- `dimension_range_map`: coordinate dictionaries with per-coordinate time
  windows.

These parameters are generic across any number of identity dimensions.

## Implementation Status

- [x] Replace per-asset update progress with canonical nested coordinate stats.
- [x] Use `get_index_progress_chunk_stats()` as the canonical chunk-stat helper.
- [x] Remove asset-specific read aliases from DataNode and local data-source
      interfaces.
- [x] Validate SQLite and DuckDB reads with canonical dimension filters,
      coordinates, and range maps.
- [x] Reject removed alias parameters through normal Python signature errors.
- [x] Keep update-stat payload validation strict and JSON-safe.
- [x] Normalize update-stat mapping keys for UUID and non-string coordinate
      values before payload validation.

## Consequences

Positive:

- DataNodes can represent timestamped tables with any number of identity
  dimensions.
- Update progress is generic and usable for account, strategy, venue, asset, or
  any other coordinate shape.
- Tail delete and latest-observation lookups can be scoped to full coordinates.
- The uniqueness contract is unambiguous.

Negative:

- Downstream code must use the canonical dimension API.
- Generated reference docs must be regenerated after this migration.
