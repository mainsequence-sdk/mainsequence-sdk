# ADR 0016: DType Serialization and Parsing Contract

Date: 2026-05-28

Status: Proposed

## Related ADRs

- ADR 0002: Multidimensional DataNode Update Contract
- ADR 0011: DuckDB N-Dimensional Row Identity
- ADR 0013: Local SQLite DataSource Support
- ADR 0014: DataNode Records and Foreign Keys
- Server ADR 008: Date And DateTime Write Contracts For MetaTables And
  DynamicTables

## Context

DataNode and MetaTable dtype handling is currently spread across multiple
client, runtime, local-storage, and SQLAlchemy-contract paths.

Temporal columns are now allowed as normal payload columns. The DataNode runtime
must still require a UTC `time_index`, but it must not reject valid `date` or
timezone-aware `datetime` payload columns. Timezone-naive datetime payloads are
local-backend-only unless the remote TS Manager adds an explicit
`TIMESTAMP WITHOUT TIME ZONE` contract later.

The remaining problem is not whether temporal payload columns are allowed. They
are. The problem is that dtype parsing is not centralized:

- DataNode source-table dtypes use pandas-style strings in `column_dtypes_map`.
- DataNode column metadata mirrors record `dtype` strings through
  `columns_metadata`.
- MetaTable contracts use `data_type` plus optional `backend_type`.
- SQLAlchemy MetaTable helpers map SQLAlchemy types to MetaTable logical types.
- Local SQLite and DuckDB interfaces have their own storage mappings.
- Response paths cast returned DataFrames from `column_dtypes_map` with several
  different rules.

This creates drift. For example, `datetime64[ns, UTC]`, `datetime64[ns]`,
`datetime`, and `date` are all valid-looking tokens in different surfaces, but
not all serializers and deserializers can handle them with the same backend
semantics.

## Decision

Add a single dtype codec/parser layer and route all DataNode and MetaTable dtype
serialization/deserialization through it.

The codec must support:

- canonical dtype normalization for DataNode `RecordDefinition.dtype`;
- canonical dtype normalization for `column_dtypes_map`;
- canonical dtype normalization for `ColumnMetaData.dtype`;
- MetaTable `data_type` normalization;
- MetaTable SQLAlchemy type-to-logical-type normalization;
- pandas dtype-to-wire-token serialization;
- wire-token-to-pandas-dtype deserialization;
- backend/local-engine type mapping for SQLite and DuckDB;
- temporal payload columns, including `date`, timezone-aware `datetime`,
  `datetime64[ns, UTC]`, and local-only timezone-naive datetime values where
  the local backend explicitly supports them.

The time index remains special:

- the first DataNode index level must remain `datetime64[ns, UTC]`;
- non-index payload columns may be date/datetime typed;
- date/datetime payload columns must be serialized and restored through the same
  dtype codec as every other dtype.

The remote TS Manager PostgreSQL write contract is narrower than the SDK's
local dtype vocabulary:

- remote date aliases: `date`, `datetime.date`;
- remote timezone-aware datetime aliases: `datetime`, `datetime.datetime`,
  `datetime64[ns, UTC]`, `timestamp with time zone`;
- canonical remote PostgreSQL datetime token: `timestamp with time zone`;
- remote date value wire format: `YYYY-MM-DD`;
- remote datetime value wire format: RFC3339 UTC `Z`;
- remote missing-value wire format: `null`;
- remote rejected temporal forms: timezone-naive `datetime`, `datetime64[ns]`,
  `TIMESTAMP WITHOUT TIME ZONE`, and any datetime string without an explicit
  offset or `Z`.

Local SQLite and DuckDB may retain timezone-naive datetime support, but that is
a local-engine mapping. The SDK must not emit timezone-naive temporal tokens or
values to the remote TS Manager write APIs.

Do not keep adding one-off `str(dtype)`, substring checks, or direct
`astype(c_type)` calls outside the codec.

## Current DType Boundary Inventory

Every item below is a current dtype serialization or deserialization boundary
that must be migrated or explicitly left as a thin wrapper around the new codec.

### DataNode Authoring and Metadata

- `mainsequence/tdag/data_nodes/models.py:28`
  - `RecordDefinition.dtype` is an unconstrained string authored by users.
  - This is a structural dtype declaration and participates in hashing.

- `mainsequence/client/models_tdag.py:167`
  - `BaseColumnMetaData.dtype` is an unconstrained string sent as column
    metadata.

- `mainsequence/client/models_tdag.py:241`
  - `SourceTableConfigurationBase.column_dtypes_map` is typed as
    `dict[str, Any]` and accepts backend-returned dtype strings without parsing.

- `mainsequence/tdag/data_nodes/data_nodes.py:1234`
  - `DataNode.get_column_metadata()` copies `record.dtype` directly into
    `ColumnMetaData.dtype`.

### DataNode Record-to-DType Map Serialization

- `mainsequence/client/models_tdag.py:61`
  - `_records_to_column_dtypes_map()` converts each record dtype with
    `str(dtype)`.

- `mainsequence/tdag/data_nodes/run_operations.py:45`
  - Duplicate `_records_to_column_dtypes_map()` also converts each record dtype
    with `str(dtype)`.

- `mainsequence/tdag/data_nodes/data_nodes.py:1187`
  - `get_source_table_initialization_schema()` builds `column_dtypes_map`
    directly from `str(record.dtype)`.

### DataFrame-to-SourceTable Serialization

- `mainsequence/client/models_tdag.py:940`
  - `_break_pandas_dataframe()` reads `data_frame.attrs[LOGICAL_COLUMN_DTYPES_ATTR]`.

- `mainsequence/client/models_tdag.py:956`
  - `_break_pandas_dataframe()` serializes pandas dtypes as
    `{column: str(dtype)}` from `data_frame.dtypes`.

- `mainsequence/client/models_tdag.py:967`
  - `_break_pandas_dataframe()` normalizes logical dtype attrs with
    `str(key)` and `str(value)`.

- `mainsequence/client/models_tdag.py:998`
  - `_break_pandas_dataframe()` lets logical dtype attrs override inferred
    pandas dtype strings.

- `mainsequence/client/models_tdag.py:999`
  - `_break_pandas_dataframe()` lets declared records override inferred dtype
    strings.

- `mainsequence/client/models_tdag.py:1040`
  - `DataNodeUpdate.upsert_data_into_table()` reads
    `source_table_schema["column_dtypes_map"]`.

- `mainsequence/client/models_tdag.py:1042`
  - `DataNodeUpdate.upsert_data_into_table()` normalizes source-table schema
    dtype entries with `str(column_name)` and `str(dtype)`.

- `mainsequence/client/models_tdag.py:1060`
  - `DataNodeUpdate.upsert_data_into_table()` passes the resulting
    `column_dtypes_map` into source-table creation.

- `mainsequence/tdag/base_persist_managers.py:556`
  - `BasePersistManager.persist_updated_data()` forwards records,
    `columns_metadata`, and `source_table_schema` into
    `DataNodeUpdate.upsert_data_into_table()`.

### SourceTable API Payload Serialization

- `mainsequence/client/models_tdag.py:1462`
  - `TimeIndexMetaData.initialize_source_table()` accepts `column_dtypes_map`.

- `mainsequence/client/models_tdag.py:1512`
  - `_initialize_source_table_at_url()` serializes source-table creation
    payload with `"column_dtypes_map": dict(column_dtypes_map)`.

- `mainsequence/client/models_tdag.py:1688`
  - `TimeIndexMetaData.handle_source_table_configuration_creation()` accepts
    `column_dtypes_map` and routes it to initialization.

- `mainsequence/client/models_tdag.py:1726`
  - `handle_source_table_configuration_creation()` sends `column_dtypes_map`
    through `initialize_source_table()`.

- `mainsequence/client/models_tdag.py:347`
  - `SourceTableConfiguration.set_or_update_columns_metadata()` accepts column
    metadata containing dtype strings.

- `mainsequence/client/models_tdag.py:354`
  - `SourceTableConfiguration.set_or_update_columns_metadata()` serializes
    metadata through `model_dump(...)`.

- `mainsequence/client/models_tdag.py:363`
  - `SourceTableConfiguration.set_or_update_columns_metadata()` sends
    `columns_metadata` to the backend.

- `mainsequence/tdag/base_persist_managers.py:497`
  - `BasePersistManager.set_column_metadata()` decides whether to sync
    `ColumnMetaData` back to the backend.

### DataNode Runtime Validation and Parsing

- `mainsequence/tdag/data_nodes/run_operations.py:120`
  - `_validate_declared_record_dtype()` compares declared dtype strings to
    actual pandas dtype strings.

- `mainsequence/tdag/data_nodes/run_operations.py:127`
  - `_validate_declared_record_dtype()` lowercases declared and actual dtype
    strings but only has special handling for `json/jsonb`, `uuid`, and
    `string/str`.

- `mainsequence/tdag/data_nodes/run_operations.py:381`
  - `UpdateRunner.validate_data_frame()` enforces the first index level as
    `datetime64[ns, UTC]`.

- `mainsequence/tdag/data_nodes/run_operations.py:398`
  - `UpdateRunner.validate_data_frame()` builds a record dtype map for output
    validation.

- `mainsequence/tdag/data_nodes/run_operations.py:416`
  - `UpdateRunner.validate_data_frame()` validates every declared record dtype
    against the DataFrame/index dtype.

### DataNode Response DType Deserialization

- `mainsequence/client/models_tdag.py:1754`
  - `TimeIndexMetaData.map_columns_to_df()` casts columns from
    `column_dtypes_map`.

- `mainsequence/client/models_tdag.py:1764`
  - `map_columns_to_df()` rewrites `"object"` to `"str"` before `astype(...)`.

- `mainsequence/client/models_tdag.py:1803`
  - `TimeIndexMetaData.get_last_observation()` parses the configured time index
    with `pd.to_datetime(...)`.

- `mainsequence/client/models_tdag.py:1807`
  - `get_last_observation()` calls `map_columns_to_df()` with
    `stc.column_dtypes_map`.

- `mainsequence/client/models_tdag.py:1980`
  - `_normalize_dtype_for_pandas()` maps dtype strings to pandas nullable
    dtypes for search responses.

- `mainsequence/client/models_tdag.py:2068`
  - `_search_response_column_dtype()` reads dtype strings from
    source-table metadata.

- `mainsequence/client/models_tdag.py:2081`
  - `_apply_dtypes_from_meta()` applies dtype restoration to DataNode search
    responses.

- `mainsequence/client/models_tdag.py:2119`
  - `_apply_dtypes_from_meta()` treats index keys as temporal when they are time
    indexes or their dtype string contains `"datetime"`.

- `mainsequence/client/models_tdag.py:2160`
  - `_apply_dtypes_from_meta()` treats prefixed payload columns as temporal only
    when their dtype string contains `"datetime"`.

- `mainsequence/client/models_tdag.py:2251`
  - DataNode search response construction calls `_apply_dtypes_from_meta()`.

- `mainsequence/client/models_tdag.py:3577`
  - API DataNode response restoration parses the configured time index with
    `pd.to_datetime(...)`.

- `mainsequence/client/models_tdag.py:3583`
  - API DataNode response restoration casts columns from
    `stc.column_dtypes_map`.

- `mainsequence/client/models_tdag.py:4392`
  - Timescale response restoration parses the configured time index with
    `pd.to_datetime(...)`.

- `mainsequence/client/models_tdag.py:4393`
  - Timescale response restoration casts columns from `stc.column_dtypes_map`.

- `mainsequence/tdag/data_nodes/persist_managers.py:86`
  - `APIPersistManager.get_data_between_dates_from_api()` reads
    `stc.column_dtypes_map` for response restoration.

- `mainsequence/tdag/data_nodes/persist_managers.py:92`
  - `APIPersistManager.get_data_between_dates_from_api()` casts returned
    columns using `astype(c_type)`.

- `mainsequence/client/utils.py:732`
  - `set_types_in_table()` is a standalone dtype-casting helper that rewrites
    `"object"` to `str` and otherwise uses `astype(col_type)`.

### Local Storage DType Mapping

- `mainsequence/client/data_sources_interfaces/sqlite.py:80`
  - SQLite `_sqlite_type()` maps pandas datetime columns to SQLite `TEXT`.

- `mainsequence/client/data_sources_interfaces/sqlite.py:128`
  - SQLite table creation serializes pandas series dtypes into SQLite column
    types through `_sqlite_type()`.

- `mainsequence/client/data_sources_interfaces/sqlite.py:144`
  - SQLite column-addition serializes pandas series dtypes into SQLite column
    types through `_sqlite_type()`.

- `mainsequence/client/data_sources_interfaces/sqlite.py:445`
  - SQLite query responses deserialize only the time index through
    `pd.to_datetime(..., utc=True)`.

- `mainsequence/client/data_sources_interfaces/duckdb.py:425`
  - DuckDB upsert normalizes the time index through
    `pd.to_datetime(..., utc=True)`.

- `mainsequence/client/data_sources_interfaces/duckdb.py:1361`
  - DuckDB read path maps DuckDB physical types to pandas target dtypes through
    `_duck_to_pandas()`.

- `mainsequence/client/data_sources_interfaces/duckdb.py:1367`
  - DuckDB read path restores `datetime64[ns, UTC]`.

- `mainsequence/client/data_sources_interfaces/duckdb.py:1375`
  - DuckDB read path restores `datetime64[ns]`.

- `mainsequence/client/data_sources_interfaces/duckdb.py:1378`
  - DuckDB read path casts non-temporal columns with pandas `astype(...)`.

- `mainsequence/client/data_sources_interfaces/duckdb.py:1450`
  - DuckDB view refresh locks parquet-discovered dtypes by explicit SQL casts.

- `mainsequence/client/data_sources_interfaces/duckdb.py:1529`
  - DuckDB `_pandas_to_duck()` maps pandas dtypes to DuckDB physical types.
    It currently has no in-file call site.

- `mainsequence/client/data_sources_interfaces/duckdb.py:1544`
  - DuckDB `_duck_to_pandas()` maps DuckDB physical types back to pandas dtypes.

- `mainsequence/client/data_sources_interfaces/duckdb.py:1554`
  - `_duck_to_pandas()` maps `TIMESTAMPTZ` to `datetime64[ns, UTC]`.

- `mainsequence/client/data_sources_interfaces/duckdb.py:1558`
  - `_duck_to_pandas()` maps `TIMESTAMP`/`DATETIME` to `datetime64[ns]`.

- `mainsequence/client/data_sources_interfaces/duckdb.py:1565`
  - `_duck_to_pandas()` maps DuckDB `DATE` to `datetime64[ns]`.

### MetaTable DType Contracts

- `mainsequence/client/models_metatables.py:73`
  - `MetaTableColumnContract.data_type` is an unconstrained string.

- `mainsequence/client/models_metatables.py:76`
  - `MetaTableColumnContract.backend_type` is an optional unconstrained string.

- `mainsequence/client/models_metatables.py:148`
  - `MetaTableColumnPayload.data_type` is an unconstrained string from backend
    responses.

- `mainsequence/client/models_metatables.py:155`
  - `MetaTableColumnPayload.backend_type` is an optional backend-returned type
    string.

- `mainsequence/client/models_metatables.py:251`
  - `MetaTableRegistrationRequest.table_contract` accepts either
    `MetaTableContract` or raw `dict`.

- `mainsequence/client/models_metatables.py:266`
  - `MetaTableValidateContractRequest.table_contract` accepts either
    `MetaTableContract` or raw `dict`.

- `mainsequence/client/models_metatables.py:374`
  - `MetaTable.register()` serializes and sends the table contract to the
    backend.

- `mainsequence/client/models_metatables.py:393`
  - `MetaTable.validate_contract()` serializes and sends the table contract to
    the backend.

### MetaTable SQLAlchemy DType Mapping

- `mainsequence/tdag/meta_tables/sqlalchemy_contracts.py:695`
  - `_column_contract()` converts SQLAlchemy columns into
    `MetaTableColumnContract`.

- `mainsequence/tdag/meta_tables/sqlalchemy_contracts.py:721`
  - `_column_type_contract()` reads `column.type` and converts it to
    `data_type` plus `backend_type`.

- `mainsequence/tdag/meta_tables/sqlalchemy_contracts.py:727`
  - `_column_type_contract()` serializes backend type as
    `str(column_type).upper()`.

- `mainsequence/tdag/meta_tables/sqlalchemy_contracts.py:741`
  - `_logical_data_type()` maps SQLAlchemy/backend type names into logical
    dtype strings.

- `mainsequence/tdag/meta_tables/sqlalchemy_contracts.py:769`
  - `_logical_data_type()` maps SQLAlchemy datetime/timestamp types to
    `"datetime"`.

- `mainsequence/tdag/meta_tables/sqlalchemy_contracts.py:771`
  - `_logical_data_type()` maps SQLAlchemy date types to `"date"`.

- `mainsequence/tdag/meta_tables/sqlalchemy_contracts.py:839`
  - `_column_storage_identity()` includes `data_type` and `backend_type` in the
    MetaTable storage hash identity.

## Required New Contract

Introduce one SDK dtype codec module. The exact module name can be chosen during
implementation, but every caller above must use it.

The codec must expose at least these operations:

- `normalize_dtype_token(value) -> DTypeToken`
  - Parses aliases and returns a canonical logical token.

- `pandas_dtype_to_token(dtype) -> DTypeToken`
  - Serializes pandas/numpy extension dtypes into canonical wire tokens.

- `token_to_pandas_dtype(token, *, nullable=True)`
  - Deserializes canonical tokens into pandas dtype objects or conversion
    instructions.

- `token_to_pandas_series(series, token, *, is_time_index=False) -> Series`
  - Applies safe conversions, including temporal conversions.

- `sqlalchemy_type_to_token(column_type) -> DTypeToken`
  - Replaces MetaTable SQLAlchemy dtype mapping.

- `token_to_backend_type(token, backend) -> str`
  - Maps canonical tokens to backend physical types where the SDK owns local
    DDL.

- `backend_type_to_token(backend_type, backend) -> DTypeToken`
  - Maps backend/local physical types back to canonical tokens.

## Temporal Type Rules

- `datetime64[ns, UTC]` must round-trip as timezone-aware UTC pandas datetime.
- `datetime64[ns]` may round-trip as timezone-naive pandas datetime only for
  local backends that explicitly support timezone-naive datetime semantics.
- For remote TS Manager writes, `datetime` is a timezone-aware logical alias.
  It must normalize to the remote canonical `timestamp with time zone` token.
- `date` must be accepted as a logical payload type and must not be treated as
  plain string.
- The DataNode time index remains required to be timezone-aware UTC.
- Payload date/datetime columns are allowed and must not be blocked by runtime
  validation.
- Remote DataNode and MetaTable writes must reject naive datetimes before
  sending a request.
- Remote MetaTable compiled SQL must emit `statement.parameter_types` for
  temporal bind parameters. Date parameters use `date`; timezone-aware datetime
  parameters use `timestamp with time zone`.

## Implementation Tasks

- [ ] Add a canonical dtype codec module shared by DataNode, MetaTable, and
      local data-source interfaces.
- [ ] Define canonical wire tokens and aliases for string/object, numeric,
      boolean, UUID, JSON/JSONB, date, datetime, timezone-aware datetime, and
      local-only timezone-naive datetime.
- [ ] Make the remote PostgreSQL canonical datetime token
      `timestamp with time zone`.
- [ ] Ensure remote write serialization emits date values as `YYYY-MM-DD`.
- [ ] Ensure remote write serialization emits timezone-aware datetime values as
      RFC3339 UTC `Z`.
- [ ] Ensure remote write serialization emits pandas `NaT`, `NaN`, and missing
      values as `null`.
- [ ] Reject timezone-naive datetimes before calling remote DataNode or
      MetaTable write endpoints.
- [ ] Treat `datetime64[ns]` as local-only unless a future remote
      `TIMESTAMP WITHOUT TIME ZONE` contract is added.
- [ ] Replace both `_records_to_column_dtypes_map()` implementations with a
      shared helper that normalizes `RecordDefinition.dtype`.
- [ ] Update `DataNode.get_source_table_initialization_schema()` to normalize
      record dtypes before building `column_dtypes_map`.
- [ ] Update `DataNode.get_column_metadata()` so `ColumnMetaData.dtype` matches
      the normalized dtype used in `column_dtypes_map`.
- [ ] Update `_break_pandas_dataframe()` to serialize inferred pandas dtypes
      through the codec.
- [ ] Update logical dtype attr handling for `LOGICAL_COLUMN_DTYPES_ATTR` to
      normalize through the codec.
- [ ] Update `source_table_schema["column_dtypes_map"]` handling to parse and
      normalize through the codec before sending it to the backend.
- [ ] Update `SourceTableConfiguration.set_or_update_columns_metadata()` to
      normalize metadata dtype values before sending them.
- [ ] Update `_validate_declared_record_dtype()` to compare normalized dtype
      semantics instead of raw strings.
- [ ] Keep the DataNode time-index UTC validation.
- [ ] Add tests proving remote DataNode payload columns may be `date` and
      `datetime64[ns, UTC]`.
- [ ] Add local-backend-only tests for `datetime64[ns]` if SQLite or DuckDB
      keep timezone-naive support.
- [ ] Update every DataNode response restoration path to use
      `token_to_pandas_series(...)` instead of open-coded `astype(...)` or
      substring checks.
- [ ] Update DataNode search response restoration to handle `date` as a
      temporal type, not only strings containing `"datetime"`.
- [ ] Update SQLite local DDL/read restoration to use codec backend mappings.
- [ ] Update DuckDB local read/write restoration to use codec backend mappings.
- [ ] Decide whether unused `DuckDBDataSourceInterface._pandas_to_duck()` should
      be wired through the codec or removed.
- [ ] Update MetaTable `MetaTableColumnContract` and `MetaTableColumnPayload`
      model validators to normalize `data_type`/`backend_type`.
- [ ] Update MetaTable registration and validation request models so raw dict
      contracts are normalized before serialization.
- [ ] Add `statement.parameter_types: dict[str, str] | None` to the MetaTable
      compiled SQL statement model.
- [ ] Emit `parameter_types[name] = "date"` for SQLAlchemy `Date` bind
      parameters.
- [ ] Emit `parameter_types[name] = "timestamp with time zone"` for
      timezone-aware SQLAlchemy `DateTime` bind parameters.
- [ ] Reject `DateTime(timezone=False)` for remote MetaTable compiled SQL unless
      a future timezone-naive remote contract is explicitly added.
- [ ] Replace SQLAlchemy `_logical_data_type()` with codec-backed mapping.
- [ ] Keep `data_type`/`backend_type` in MetaTable storage hash identity, but
      hash their normalized canonical values.
- [ ] Add tests for SQLAlchemy `Date`, `DateTime`, timezone timestamp, and
      backend type aliases.
- [ ] Add tests proving MetaTable compiled SQL emits `parameter_types` and
      canonical temporal JSON values.
- [ ] Add tests proving MetaTable compiled SQL rejects naive datetime bind
      values before request submission.
- [ ] Add round-trip tests from DataFrame -> `column_dtypes_map` ->
      SourceTableConfiguration -> response DataFrame for temporal payload
      columns.
- [ ] Add round-trip tests for MetaTable SQLAlchemy model -> registration
      request -> normalized table contract.
- [ ] Update docs that still imply DataNode payload dates must be stored as raw
      timestamps.

## Acceptance Criteria

- There is one dtype normalization implementation used by all code paths above.
- `column_dtypes_map` and `columns_metadata` cannot disagree for record-derived
  DataNode columns.
- Temporal payload columns are accepted in DataNode output validation.
- Temporal payload columns round-trip through backend/API response restoration.
- Remote writes serialize `date` as `YYYY-MM-DD` and timezone-aware datetime as
  RFC3339 UTC `Z`.
- Remote writes never emit timezone-naive datetime values.
- Remote DataNode writes never send `datetime64[ns]` in `column_dtypes_map`.
- MetaTable SQLAlchemy `Date` and `DateTime` columns serialize consistently.
- MetaTable compiled SQL emits `statement.parameter_types` for temporal bind
  parameters and uses `timestamp with time zone` for timezone-aware datetimes.
- Search/join response restoration handles `date`, `datetime`,
  local-only `datetime64[ns]`, and remote `datetime64[ns, UTC]`.
- Local SQLite and DuckDB paths use the same logical dtype codec as remote
  source tables while preserving explicitly local-only naive datetime semantics.

## Risks

- Normalizing dtype tokens affects storage hashes where dtype declarations are
  structural.
- MetaTable storage hashes include normalized `data_type`/`backend_type`; alias
  canonicalization can change hashes for existing authoring inputs.
- Backend physical table creation may still have its own dtype parser. Client
  normalization must be compatible with the backend parser before rollout.
- Response casting currently swallows some cast failures. Centralizing parsing
  may expose hidden bad data unless compatibility behavior is explicit.
