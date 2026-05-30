# ADR 0016: DType Serialization and Parsing Contract

Date: 2026-05-28

Status: Accepted

## Context

DataNode and MetaTable code both move logical column types between SQLAlchemy,
Pandas, JSON payloads, local SQLite/DuckDB storage, and backend PostgreSQL
storage. The SDK needs one type-normalization layer so every boundary treats the
same logical type the same way.

## Decision

All DataNode and MetaTable dtype serialization and deserialization routes use
the shared dtype codec in `mainsequence.client.dtype_codec`.

The codec owns:

- canonical dtype normalization for direct write records;
- canonical dtype normalization for `column_dtypes_map`;
- canonical dtype normalization for `ColumnMetaData.dtype`;
- MetaTable `data_type` normalization;
- SQLAlchemy type-to-logical-type normalization;
- pandas dtype-to-wire-token serialization;
- wire-token-to-pandas-dtype deserialization;
- backend/local-engine type mapping for SQLite and DuckDB.

## Time Index Rule

The first DataNode index level is the time index and must be
`datetime64[ns, UTC]`.

Payload columns may use supported date/datetime logical types. Remote writes use
explicit timezone-aware datetime values. Local SQLite and DuckDB may support
additional local-engine datetime representations, but the remote write contract
uses explicit timezone-aware values.

## Current Contract

- Storage schema comes from the registered `PlatformTimeIndexMetaData` /
  `TimeIndexMetaData` contract.
- DataNode write payloads are prepared through `prepare_dataframe_for_remote_write`.
- DataNode read payloads are restored through the codec using the storage
  `column_dtypes_map`.
- MetaTable SQLAlchemy helpers derive logical types from SQLAlchemy column
  definitions through the codec.
- Local SQLite and DuckDB interfaces map codec tokens to their engine types.

## Rules

- Do not add direct `str(dtype)` parsing outside the codec.
- Do not add substring checks for dtype behavior outside the codec.
- Do not cast DataFrames with raw backend strings outside codec helpers.
- Do not emit timezone-naive datetime values to remote write APIs.
