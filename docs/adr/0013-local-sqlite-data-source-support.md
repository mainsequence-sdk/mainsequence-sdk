# ADR 0013: Local SQLite DataSource Support

Date: 2026-05-25

Status: Accepted

## Related ADRs

- ADR 0011: DuckDB N-Dimensional Row Identity
- ADR 0012: Explicit DuckDB DataSource Selection

## Context

The SDK supports local DataNode persistence through DuckDB. After ADR 0012,
local persistence is selected explicitly:

```python
data_source = DataSource.create_duckdb()
SessionDataSource.set_local_db(data_source=data_source)
```

This is the right public shape, but the implementation is still DuckDB-specific:

- the local physical class type is hardcoded as `"duck_db"`;
- the local session method is named around "local db" but validates only DuckDB;
- adapter dispatch in `DataSource.insert_data_into_table(...)`,
  `DataSource.get_data_by_time_index(...)`, and
  `DataSource.get_earliest_value(...)` branches only on DuckDB;
- the only local data-source interface is
  `mainsequence/client/data_sources_interfaces/duckdb.py`.

SQLite is useful as a second local storage backend because it has a minimal
runtime footprint, is available in the Python standard library, and is suitable
for small or moderate local datasets where Parquet/DuckDB is unnecessary.

## Decision

Add SQLite as a first-class local `DataSource` backend alongside DuckDB.

The public API must mirror the explicit DuckDB flow:

```python
data_source = DataSource.create_sqlite()
SessionDataSource.set_local_db(data_source=data_source)
```

The backend class type for SQLite will be:

```python
SQLITE = "sqlite"
```

`SessionDataSource.set_local_db(...)` must accept any supported local physical
`DataSource`, not just DuckDB. It should reject remote or unsupported class
types with a clear error.

Adapter dispatch must be local-backend aware. DuckDB and SQLite should share a
common client-side contract:

- `upsert(df, table, *, index_names, time_index_name, ...)`
- `read(table, *, index_names, time_index_name, dimension_filters=None,
  index_coordinates=None, dimension_range_map=None, columns=None, ...)`
- `constrain_read(...)` when row limiting is supported
- `time_index_minima(table, *, index_names, time_index_name)`
- `drop_table(table)`
- `list_tables()`

SQLite row identity must follow ADR 0011: full `index_names`, never
`unique_identifier` as a special column.

## Required Behavior

### DataSource Creation

Add:

```python
DataSource.create_sqlite(...)
DynamicTableDataSource.create_sqlite(data_source=...)
```

`DataSource.create_sqlite(...)` should send an explicit local SQLite creation
request, analogous to DuckDB. The body should include at minimum:

```json
{
  "host_mac_address": "<bios_uuid()>",
  "display_name": "SQLite_<bios_uuid()>"
}
```

If the backend needs a different endpoint than DuckDB, add a SQLite-specific
`get_or_create_sqlite(...)` wrapper instead of overloading DuckDB names.

### Local Session Selection

`SessionDataSource.set_local_db(data_source=...)` must:

- require an explicit physical `DataSource`;
- accept `class_type in {"duck_db", "sqlite"}`;
- resolve the matching `DynamicTableDataSource` through the correct backend
  endpoint;
- avoid hidden creation of physical data sources;
- preserve cleanup behavior for local tables.

`SessionDataSource.is_local_duck_db` can remain for compatibility, but local
backend checks should move toward a generic shape:

```python
SessionDataSource.is_local_db
SessionDataSource.local_db_class_type
```

### SQLite Storage Contract

The SQLite adapter must:

- create one SQLite database file for the local SDK data path;
- create one table per `storage_hash`;
- quote identifiers safely;
- create or validate a unique index over full `index_names`;
- normalize `time_index_name` values consistently before writes;
- upsert rows by full `index_names`;
- preserve every configured index column;
- support canonical `dimension_filters`;
- support canonical `index_coordinates`;
- support canonical `dimension_range_map`;
- always include all `index_names` columns when `columns` is provided;
- return DataFrames compatible with
  `DataSource.get_data_by_time_index(...).set_index(stc.index_names)`.

### Type Mapping

SQLite has weaker physical types than DuckDB. The SDK should use a conservative
mapping:

- temporal columns stored as ISO-8601 UTC text or integer epoch nanoseconds;
- strings as `TEXT`;
- integers as `INTEGER`;
- floats as `REAL`;
- booleans as `INTEGER` with conversion on read;
- unsupported complex object columns serialized explicitly or rejected with a
  clear error.

The first implementation should prefer correctness and clear errors over
implicit lossy conversion.

### Row Limiting

SQLite does not have Parquet row-group metadata. Its initial `constrain_read`
implementation may be simpler than DuckDB:

- use indexed temporal predicates and `LIMIT` only where semantics are clear; or
- return the original filters with diagnostics stating row limiting is not
  metadata-constrained for SQLite.

It must not silently apply a row limit that changes coordinate range semantics.

## Non-Negotiable Rules

- Do not hide SQLite `DataSource` creation inside session setup.
- Do not make SQLite a DuckDB mode or reuse `"duck_db"` class type.
- Do not hardcode `["time_index", "unique_identifier"]` in SQLite write, read,
  merge, delete, or minima logic.
- Do not synthesize `unique_identifier`.
- Do not implement SQLite by routing through DuckDB.
- Do not change the backend API contract for existing DuckDB callers.

## Scope

In scope:

- `mainsequence/client/models_foundry.py`
- `mainsequence/client/data_sources_interfaces/sqlite.py`
- `mainsequence/client/data_sources_interfaces/__init__.py`
- focused SQLite adapter tests
- focused local-session selection tests
- reference docs for local data-source setup

Out of scope:

- removing DuckDB support
- changing API-backed Timescale/Postgres persistence
- optimizing SQLite for large analytical scans
- full SQL query execution parity with DuckDB

## Implementation Tasks

- [x] Add `SQLITE = "sqlite"` constant.
- [x] Add lazy SQLite interface loader in
  `mainsequence/client/data_sources_interfaces/__init__.py`.
- [x] Add `mainsequence/client/data_sources_interfaces/sqlite.py`.
- [x] Add `DataSource.create_sqlite(...)`.
- [x] Add `DynamicTableDataSource.create_sqlite(data_source=...)`.
- [x] Generalize `SessionDataSource.set_local_db(data_source=...)` to dispatch
  by local class type.
- [x] Generalize local cleanup to use the selected local adapter.
- [x] Update `DataSource.insert_data_into_table(...)` dispatch for SQLite.
- [x] Update `DataSource.get_data_by_time_index(...)` dispatch for SQLite.
- [x] Update `DataSource.get_earliest_value(...)` dispatch for SQLite.
- [x] Add tests proving SQLite upsert uses full N-dimensional `index_names`.
- [x] Add tests proving SQLite reads support canonical dimension filters,
  coordinates, and range maps.
- [x] Add tests proving local setup does not create SQLite implicitly.
- [x] Document the public setup flow:
  `DataSource.create_sqlite()` plus
  `SessionDataSource.set_local_db(data_source=...)`.

## Validation

The SQLite implementation is complete when this flow works:

```python
data_source = DataSource.create_sqlite()
SessionDataSource.set_local_db(data_source=data_source)
```

and a table with:

```python
index_names = ["time_index", "account_uid", "asset_uid"]
```

can persist and read both rows below without collapse:

```text
time_index                 account_uid  asset_uid  value
2026-05-25T00:00:00Z       acct-a       asset-1    10
2026-05-25T00:00:00Z       acct-b       asset-1    20
```

The read result must set a MultiIndex over:

```python
["time_index", "account_uid", "asset_uid"]
```

and preserve both values.

## Compatibility

DuckDB remains the existing local analytical backend. SQLite is an additional
local backend, not a replacement.

Existing DuckDB callers using:

```python
DataSource.create_duckdb()
SessionDataSource.set_local_db(data_source=data_source)
```

must continue to work unchanged.
