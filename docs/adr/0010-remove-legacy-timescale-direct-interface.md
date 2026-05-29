# ADR 0010: Remove Legacy Timescale Direct Interface

Date: 2026-05-25

Status: Accepted

## Related ADRs

- ADR 0002: Multidimensional DataNode Update Contract
- ADR 0007: Client-Wide UID Public Identity

## Context

`mainsequence/client/data_sources_interfaces/timescale.py` is a legacy direct
Postgres/Timescale adapter.

The supported Timescale path now goes through the backend API:

- writes route through `DataNodeUpdate.post_data_frame_in_chunks(...)`
- reads route through `DataNodeUpdate.get_data_between_dates_from_api(...)`
- `TimeScaleDB.get_data_by_time_index(...)` normalizes the API response into the
  expected indexed DataFrame

The legacy file is still imported by `mainsequence/client/models_foundry.py` as
`TimeScaleInterface`, but its only live dispatch path is the special
`DynamicTableDataSource` branch for:

```python
related_resource.class_type == "direct"
```

That branch calls `TimeScaleInterface.direct_data_from_db(...)` and bypasses the
backend API. Repository search shows no maintained tests, examples, or public
docs that create or exercise a `"direct"` data source. Existing test fixtures use
`timescale_db`, `timescale_db_remote`, or `duck_db`.

The write helpers in the legacy file are effectively orphaned:

- `process_and_update_table(...)` is not called outside the file
- `direct_table_update(...)` is not called outside the file except by
  `process_and_update_table(...)`
- normal Timescale writes call `DataNodeUpdate.post_data_frame_in_chunks(...)`

The remaining direct implementation also preserves old assumptions that conflict
with current SDK direction:

- raw SQL is assembled with string interpolation
- filters hardcode `unique_identifier` and `asset_symbol`
- overwrite deletion scopes by legacy identity columns instead of arbitrary
  configured index dimensions
- one helper builds a direct backend URL from `data_node_storage.id`
- `psycopg2` is imported lazily but is not declared as part of the normal SDK
  dependency contract
- progress-bar and threaded insert behavior lives outside the maintained
  backend persistence path

ADR 0002 previously listed Timescale direct paths as work to modernize for
multidimensional dimensions. Given the current implementation inventory, that is
the wrong direction. The direct adapter is not a supported public path, and
updating it would extend an untested second persistence contract.

## Decision

Remove `mainsequence/client/data_sources_interfaces/timescale.py` and all SDK
references to it.

The SDK must not keep a special direct Postgres/Timescale read or write path.
All non-DuckDB dynamic table reads and writes must go through the resource API
surface exposed by the related data source.

`DynamicTableDataSource.get_data_by_time_index(...)` must delegate to
`self.related_resource.get_data_by_time_index(...)` for non-DuckDB data sources.
It must not inspect `related_resource.class_type == "direct"` or call a
Timescale direct interface.

If direct database access is needed again in the future, it must be introduced
as a new explicit adapter with:

- a named and documented data source class type
- an optional dependency group for its database driver
- parameterized query construction
- dimension-aware predicates based on `SourceTableConfiguration.index_names`
- UID-based backend routes
- focused tests that do not require a production Timescale instance
- a clear reason it cannot use the backend API path

## Scope

In scope:

- remove `mainsequence/client/data_sources_interfaces/timescale.py`
- remove the `TimeScaleInterface` import from `mainsequence/client/models_foundry.py`
- remove `DynamicTableDataSource.has_direct_postgres_connection(...)`
- remove the direct branch in `DynamicTableDataSource.get_data_by_time_index(...)`
- remove generated reference docs for the legacy module
- remove the `docs/reference/SUMMARY.md` entry for the legacy module
- update ADR 0002 so Timescale direct paths are marked removed, not pending
- update ADR 0007 so the `data_node_storage.id` URL in the deleted file is no
  longer tracked as a remaining migration item
- add or update tests proving non-DuckDB reads delegate to the related resource
  API path

Out of scope:

- changing the backend API
- changing DuckDB local-data behavior
- changing `TimeScaleDB` API-backed read/write behavior
- adding a replacement direct database adapter

## Required Final State

- importing `mainsequence.client.models_foundry` does not import a Timescale direct
  interface module
- `mainsequence/client/data_sources_interfaces/__init__.py` exposes only
  maintained interface entry points
- no code under `mainsequence/` references `TimeScaleInterface`
- no code under `mainsequence/` references `direct_data_from_db(...)`,
  `direct_table_update(...)`, `process_and_update_table(...)`, or
  `filter_by_assets_ranges(...)`
- no public docs or generated reference docs include
  `mainsequence.client.data_sources_interfaces.timescale`
- `DynamicTableDataSource.get_data_by_time_index(...)` has one non-DuckDB path:
  delegate to `related_resource.get_data_by_time_index(...)`
- no public SDK behavior depends on `related_resource.class_type == "direct"`

## Implementation Tasks

- [x] Delete `mainsequence/client/data_sources_interfaces/timescale.py`.
- [x] Remove `from .data_sources_interfaces import timescale as TimeScaleInterface`.
- [x] Delete `DynamicTableDataSource.has_direct_postgres_connection(...)`.
- [x] Simplify `DynamicTableDataSource.get_data_by_time_index(...)` to delegate
  to the related resource for all non-DuckDB sources.
- [x] Remove generated reference file
  `docs/reference/client/data_sources_interfaces/timescale.md`.
- [x] Remove the `timescale` entry from `docs/reference/SUMMARY.md`.
- [x] Update ADR 0002 to replace Timescale direct-path modernization tasks with
  this removal decision.
- [x] Update ADR 0007 to remove the stale finding for
  `timescale.py`'s `data_node_storage.id` URL after the file is deleted.
- [x] Add a focused test for `DynamicTableDataSource.get_data_by_time_index(...)`
  delegation on a non-DuckDB related resource.
- [x] Run the affected test subset and a repository search for the removed
  symbols.

## Compatibility

This is a breaking change only for callers that still rely on a `"direct"`
Timescale/Postgres data source class type through `DynamicTableDataSource`.

That class type is not covered by maintained tests, examples, or public docs in
this repository. The supported compatibility path is to use the backend-managed
Timescale data source API, not direct database credentials from SDK code.

## Consequences

Benefits:

- removes an untested direct SQL path
- removes an undeclared optional database-driver dependency from the maintained
  runtime surface
- avoids modernizing legacy `unique_identifier` and asset-specific assumptions
- reduces public identity migration work by deleting an id-based helper URL
- keeps Timescale behavior behind the backend API contract

Tradeoffs:

- any private user of the `"direct"` data source branch must migrate to an
  API-backed data source
- direct database reads cannot be used as an emergency performance bypass unless
  a new supported adapter is designed and tested

## Validation

Removal is complete when these checks pass:

```bash
rg -n "TimeScaleInterface|direct_data_from_db|direct_table_update|process_and_update_table|filter_by_assets_ranges|data_sources_interfaces/timescale|data_sources_interfaces\\.timescale" mainsequence tests docs examples
```

The command should return no live references except this ADR and any intentional
historical references that explicitly say the legacy interface was removed.
