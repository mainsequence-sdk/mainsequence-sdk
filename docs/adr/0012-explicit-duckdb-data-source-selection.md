# ADR 0012: Explicit DuckDB DataSource Selection

Date: 2026-05-25

Status: Accepted

## Context

The SDK local DuckDB mode previously created or resolved the physical DuckDB
`DataSource` inside session helpers. A caller could switch to local DuckDB
without a visible data-source object at the call site.

That made DuckDB persistence look like a global side effect. It also made
storage cleanup paths call `get_or_create_duck_db(...)` only to decide whether a
table belonged to local DuckDB.

This is inconsistent with the rest of the SDK, where persistence targets should
be explicit resources that callers can pass around, inspect, and test.

## Decision

DuckDB local mode must be selected with an explicit physical `DataSource`.

The public caller flow is:

```python
data_source = DataSource.create_duckdb()
SessionDataSource.set_local_db(data_source=data_source)
```

`DataSource.create_duckdb(...)` is the public factory for the physical DuckDB
resource. It may still use the backend idempotent endpoint internally, but that
resolution is no longer hidden inside session setup.

`SessionDataSource.set_local_db(...)` requires the caller-provided DuckDB
`DataSource` and creates/resolves the corresponding `DynamicTableDataSource`
from it.

SDK internals must not create a DuckDB `DataSource` just to classify or clean up
local DuckDB storage.

## Consequences

- Local DuckDB selection is explicit at the call site.
- Tests can assert that session setup does not touch the physical data-source
  factory.
- Existing backend `get_or_create_duck_db(...)` endpoints remain available as
  compatibility internals.
- Calling `SessionDataSource.set_local_db()` without a `data_source` now raises
  a clear error.

## Implementation Tasks

- [x] Add `DataSource.create_duckdb(...)`.
- [x] Add `DynamicTableDataSource.create_duckdb(data_source=...)`.
- [x] Make `PodDataSource.set_local_db(...)` require an explicit DuckDB
  `DataSource`.
- [x] Remove hidden DuckDB creation from `DataNodeStorage.delete_table(...)`.
- [x] Add tests for explicit creation, local setup, and cleanup behavior.
