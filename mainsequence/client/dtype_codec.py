from __future__ import annotations

import datetime
import math
import re
from collections.abc import Mapping
from typing import Any
from uuid import UUID

import numpy as np
import pandas as pd

DATE = "date"
TIMESTAMP_TZ = "timestamp with time zone"
LOCAL_DATETIME_NAIVE = "datetime64[ns]"
STRING = "string"
UUID_TOKEN = "uuid"
JSON = "json"
JSONB = "jsonb"
BOOL = "bool"
INT16 = "int16"
INT32 = "int32"
INT64 = "int64"
FLOAT32 = "float32"
FLOAT64 = "float64"
NUMERIC = "numeric"

_OFFSET_RE = re.compile(r"(z|[+-]\d{2}:?\d{2})$", re.IGNORECASE)


def is_nullish(value: Any) -> bool:
    if value is None:
        return True
    try:
        if value is pd.NaT:
            return True
        if isinstance(value, float) and math.isnan(value):
            return True
        result = pd.isna(value)
    except (TypeError, ValueError):
        return False
    if isinstance(result, (bool, np.bool_)):
        return bool(result)
    return False


def normalize_dtype_token(
    value: Any,
    *,
    remote: bool = True,
    allow_naive_datetime: bool = False,
) -> str:
    token = str(value or "").strip()
    if not token:
        raise ValueError("DType token is required.")

    lowered = " ".join(token.replace("_", " ").strip().lower().split())
    compact = lowered.replace(" ", "")

    if lowered in {"date", "datetime.date"}:
        return DATE

    if lowered in {
        "datetime",
        "datetime.datetime",
        "datetime64[ns, utc]",
        "timestamp with time zone",
        "timestamptz",
    }:
        return TIMESTAMP_TZ

    if lowered in {
        "datetime64[ns]",
        "timestamp",
        "timestamp without time zone",
        "datetime without time zone",
        "datetime naive",
    }:
        if remote and not allow_naive_datetime:
            raise ValueError(
                "Timezone-naive datetime dtypes are local-backend-only. "
                "Remote writes require 'timestamp with time zone'."
            )
        return LOCAL_DATETIME_NAIVE

    if lowered in {"object", "str", "string", "text", "varchar", "char", "unicode"}:
        return STRING
    if "varchar" in lowered:
        return STRING
    if compact in {"uuid", "uuidtype", "uuid_", "postgresqluuid"}:
        return UUID_TOKEN
    if lowered in {"json"}:
        return JSON
    if lowered in {"jsonb"}:
        return JSONB
    if lowered in {"bool", "boolean"}:
        return BOOL
    if lowered in {"int16", "smallint", "int2"}:
        return INT16
    if lowered in {"int32", "integer", "int", "int4"}:
        return INT32
    if lowered in {"int64", "biginteger", "bigint", "int8"}:
        return INT64
    if lowered in {"uint16", "uint32", "uint64"}:
        return INT64
    if lowered in {"float32", "real", "float4"}:
        return FLOAT32
    if lowered in {"float", "float64", "double", "double precision", "float8"}:
        return FLOAT64
    if lowered == "numeric" or lowered.startswith("numeric") or lowered == "decimal":
        return NUMERIC

    return token


def normalize_column_dtypes_map(
    column_dtypes_map: Mapping[str, Any],
    *,
    remote: bool = True,
    allow_naive_datetime: bool = False,
) -> dict[str, str]:
    return {
        str(column_name): normalize_dtype_token(
            dtype,
            remote=remote,
            allow_naive_datetime=allow_naive_datetime,
        )
        for column_name, dtype in dict(column_dtypes_map).items()
    }


def _record_definition_field(record: Any, field_name: str) -> Any:
    if isinstance(record, Mapping):
        return record.get(field_name)
    return getattr(record, field_name, None)


def record_definitions_to_column_dtypes_map(
    records: Any,
    *,
    remote: bool = True,
    allow_naive_datetime: bool = False,
) -> dict[str, str]:
    if not records:
        return {}

    column_dtypes_map: dict[str, str] = {}
    duplicate_columns: set[str] = set()
    for record in records:
        column_name = _record_definition_field(record, "column_name")
        dtype = _record_definition_field(record, "dtype")
        if not column_name or not dtype:
            raise ValueError(
                "Record definitions must include non-empty 'column_name' and 'dtype' fields."
            )
        column_name = str(column_name)
        if column_name in column_dtypes_map:
            duplicate_columns.add(column_name)
        column_dtypes_map[column_name] = normalize_dtype_token(
            dtype,
            remote=remote,
            allow_naive_datetime=allow_naive_datetime,
        )

    if duplicate_columns:
        raise ValueError(f"Duplicate DataNode record column names: {sorted(duplicate_columns)}")

    return column_dtypes_map


def pandas_dtype_to_token(
    dtype: Any,
    *,
    remote: bool = True,
    allow_naive_datetime: bool = False,
) -> str:
    if isinstance(dtype, pd.DatetimeTZDtype):
        return TIMESTAMP_TZ
    if pd.api.types.is_datetime64_any_dtype(dtype):
        if remote and not allow_naive_datetime:
            raise ValueError(
                "Timezone-naive pandas datetime64[ns] is local-backend-only. "
                "Remote writes require timezone-aware datetime64[ns, UTC]."
            )
        return LOCAL_DATETIME_NAIVE
    if pd.api.types.is_bool_dtype(dtype):
        return BOOL
    if pd.api.types.is_integer_dtype(dtype):
        name = str(dtype).lower()
        if "16" in name:
            return INT16
        if "32" in name:
            return INT32
        return INT64
    if pd.api.types.is_float_dtype(dtype):
        return FLOAT32 if "32" in str(dtype).lower() else FLOAT64
    if pd.api.types.is_string_dtype(dtype):
        return STRING
    return STRING


def pandas_dtypes_to_column_map(
    dtypes: Mapping[str, Any],
    *,
    remote: bool = True,
    allow_naive_datetime: bool = False,
) -> dict[str, str]:
    return {
        str(column_name): pandas_dtype_to_token(
            dtype,
            remote=remote,
            allow_naive_datetime=allow_naive_datetime,
        )
        for column_name, dtype in dtypes.items()
    }


def token_to_pandas_dtype(token: Any, *, nullable: bool = True) -> Any:
    normalized = normalize_dtype_token(token, remote=False, allow_naive_datetime=True)
    if normalized == STRING:
        return "string"
    if normalized == UUID_TOKEN:
        return "string"
    if normalized in {JSON, JSONB}:
        return "object"
    if normalized in {INT16, INT32, INT64}:
        return "Int64" if nullable else normalized
    if normalized == BOOL:
        return "boolean" if nullable else "bool"
    if normalized in {FLOAT32, FLOAT64, NUMERIC}:
        return "float64"
    if normalized == TIMESTAMP_TZ:
        return "datetime64[ns, UTC]"
    if normalized == LOCAL_DATETIME_NAIVE:
        return "datetime64[ns]"
    if normalized == DATE:
        return "object"
    return str(token)


def token_to_pandas_series(
    series: pd.Series,
    token: Any,
    *,
    is_time_index: bool = False,
    nullable: bool = True,
) -> pd.Series:
    normalized = (
        TIMESTAMP_TZ
        if is_time_index
        else normalize_dtype_token(
            token,
            remote=False,
            allow_naive_datetime=True,
        )
    )
    if normalized == TIMESTAMP_TZ:
        return pd.to_datetime(series, errors="coerce", utc=True)
    if normalized == LOCAL_DATETIME_NAIVE:
        parsed = pd.to_datetime(series, errors="coerce")
        try:
            if getattr(parsed.dt, "tz", None) is not None:
                parsed = parsed.dt.tz_convert(None)
        except (AttributeError, TypeError):
            pass
        return parsed
    if normalized == DATE:
        return pd.to_datetime(series, errors="coerce").dt.date
    if normalized in {JSON, JSONB}:
        return series
    pandas_dtype = token_to_pandas_dtype(normalized, nullable=nullable)
    if pandas_dtype == "object":
        return series
    return series.astype(pandas_dtype)


def is_temporal_token(token: Any) -> bool:
    return normalize_dtype_token(token, remote=False, allow_naive_datetime=True) in {
        DATE,
        TIMESTAMP_TZ,
        LOCAL_DATETIME_NAIVE,
    }


def _string_has_explicit_timezone(value: str) -> bool:
    stripped = value.strip()
    if not stripped:
        return True
    return bool(_OFFSET_RE.search(stripped))


def _timestamp_is_timezone_aware(value: Any) -> bool:
    if isinstance(value, pd.Timestamp):
        return value.tzinfo is not None
    if isinstance(value, datetime.datetime):
        return value.tzinfo is not None and value.tzinfo.utcoffset(value) is not None
    return False


def _serialize_date_value(value: Any) -> str | None:
    if is_nullish(value):
        return None
    if isinstance(value, datetime.datetime):
        return value.date().isoformat()
    if isinstance(value, datetime.date):
        return value.isoformat()
    timestamp = pd.to_datetime(value, errors="raise")
    if isinstance(timestamp, pd.Timestamp):
        return timestamp.date().isoformat()
    return str(value)


def _serialize_timestamp_tz_value(value: Any) -> str | None:
    if is_nullish(value):
        return None
    if isinstance(value, np.datetime64):
        raise ValueError(
            "Timezone-naive numpy datetime64 values cannot be sent to remote TS Manager."
        )
    if isinstance(value, str) and not _string_has_explicit_timezone(value):
        raise ValueError(
            f"Remote datetime value {value!r} must include an explicit timezone offset or Z."
        )
    if isinstance(value, (pd.Timestamp, datetime.datetime)) and not _timestamp_is_timezone_aware(
        value
    ):
        raise ValueError(f"Remote datetime value {value!r} must be timezone-aware.")
    timestamp = pd.to_datetime(value, errors="raise", utc=True)
    if not isinstance(timestamp, pd.Timestamp):
        timestamp = pd.Timestamp(timestamp).tz_convert("UTC")
    if timestamp.tzinfo is None:
        raise ValueError(f"Remote datetime value {value!r} must be timezone-aware.")
    timestamp = timestamp.tz_convert("UTC")
    return timestamp.isoformat().replace("+00:00", "Z")


def serialize_remote_value(value: Any, token: Any) -> Any:
    normalized = normalize_dtype_token(token, remote=True)
    if normalized == DATE:
        return _serialize_date_value(value)
    if normalized == TIMESTAMP_TZ:
        return _serialize_timestamp_tz_value(value)
    if is_nullish(value):
        return None
    if isinstance(value, UUID):
        return str(value)
    return value


def serialize_remote_parameters(
    parameters: Mapping[str, Any] | list[Any],
    parameter_types: Mapping[str, Any] | None,
) -> dict[str, Any] | list[Any]:
    if not parameter_types or not isinstance(parameters, Mapping):
        return dict(parameters) if isinstance(parameters, Mapping) else list(parameters)
    normalized_types = normalize_column_dtypes_map(parameter_types, remote=True)
    serialized = dict(parameters)
    for name, token in normalized_types.items():
        if name in serialized:
            serialized[name] = serialize_remote_value(serialized[name], token)
    return serialized


def prepare_dataframe_for_remote_write(
    df: pd.DataFrame,
    *,
    column_dtypes_map: Mapping[str, Any],
    time_index_name: str,
) -> pd.DataFrame:
    normalized_map = normalize_column_dtypes_map(column_dtypes_map, remote=True)
    prepared = df.copy()
    if time_index_name not in normalized_map:
        normalized_map[time_index_name] = TIMESTAMP_TZ
    for column_name, token in normalized_map.items():
        if column_name not in prepared.columns:
            continue
        normalized = normalize_dtype_token(token, remote=True)
        prepared[column_name] = [
            serialize_remote_value(value, normalized) for value in prepared[column_name].tolist()
        ]
    return prepared


def token_to_backend_type(token: Any, backend: str) -> str:
    normalized = normalize_dtype_token(token, remote=False, allow_naive_datetime=True)
    backend_key = backend.strip().lower()
    if backend_key == "sqlite":
        if normalized in {INT16, INT32, INT64, BOOL}:
            return "INTEGER"
        if normalized in {FLOAT32, FLOAT64, NUMERIC}:
            return "REAL"
        return "TEXT"
    if backend_key in {"duckdb", "duck_db"}:
        if normalized == DATE:
            return "DATE"
        if normalized == TIMESTAMP_TZ:
            return "TIMESTAMPTZ"
        if normalized == LOCAL_DATETIME_NAIVE:
            return "TIMESTAMP"
        if normalized in {INT16, INT32, INT64}:
            return "BIGINT"
        if normalized in {FLOAT32, FLOAT64, NUMERIC}:
            return "DOUBLE"
        if normalized == BOOL:
            return "BOOLEAN"
        return "VARCHAR"
    raise ValueError(f"Unsupported dtype backend {backend!r}.")


def backend_type_to_token(
    backend_type: Any,
    backend: str,
    *,
    allow_naive_datetime: bool = True,
) -> str:
    raw = str(backend_type or "").strip().upper()
    backend_key = backend.strip().lower()
    if backend_key == "sqlite":
        if raw == "INTEGER":
            return INT64
        if raw == "REAL":
            return FLOAT64
        return STRING
    if backend_key in {"duckdb", "duck_db"}:
        if raw in {"TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE"}:
            return TIMESTAMP_TZ
        if raw in {"TIMESTAMP", "DATETIME"}:
            return normalize_dtype_token(
                LOCAL_DATETIME_NAIVE,
                remote=False,
                allow_naive_datetime=allow_naive_datetime,
            )
        if raw == "DATE":
            return DATE
        if raw in {"TINYINT", "SMALLINT", "INTEGER", "INT", "BIGINT"}:
            return INT64
        if raw in {"REAL", "FLOAT", "DOUBLE", "DECIMAL"}:
            return FLOAT64
        if raw == "BOOLEAN":
            return BOOL
        return STRING
    return normalize_dtype_token(str(backend_type), remote=False, allow_naive_datetime=True)


def sqlalchemy_type_to_token(column_type: Any, *, remote: bool = True) -> str:
    type_name = type(column_type).__name__.lower()
    backend_type = str(column_type).upper()
    normalized_backend = " ".join(backend_type.lower().split())
    timezone = getattr(column_type, "timezone", None)

    if type_name in {"datetime"} or "timestamp" in normalized_backend:
        if timezone is False or "without time zone" in normalized_backend:
            if remote:
                raise ValueError(
                    "Timezone-naive SQLAlchemy DateTime columns are not supported "
                    "by the remote TS Manager contract."
                )
            return LOCAL_DATETIME_NAIVE
        if (
            timezone is True
            or "with time zone" in normalized_backend
            or "timestamptz" in normalized_backend
        ):
            return TIMESTAMP_TZ
        if remote:
            raise ValueError(
                "SQLAlchemy DateTime columns must set timezone=True for remote MetaTables."
            )
        return LOCAL_DATETIME_NAIVE

    if type_name in {"date"} or normalized_backend == "date":
        return DATE
    if type_name in {"uuid", "uuidtype", "uuid_", "postgresqluuid"} or normalized_backend == "uuid":
        return UUID_TOKEN
    if type_name in {"string", "unicode", "text", "varchar", "char"}:
        return STRING
    if type_name in {"integer", "biginteger"}:
        return INT64
    if type_name in {"smallinteger"}:
        return INT16
    if type_name in {"float", "double", "doubleprecision"}:
        return FLOAT64
    if type_name in {"numeric", "decimal"}:
        return NUMERIC
    if type_name in {"boolean"}:
        return BOOL
    if type_name in {"json", "jsonb"}:
        return JSONB if type_name == "jsonb" else JSON
    return normalize_dtype_token(backend_type, remote=remote)


def sqlalchemy_backend_type(column_type: Any, token: Any) -> str:
    normalized = normalize_dtype_token(token, remote=False, allow_naive_datetime=True)
    if normalized == UUID_TOKEN:
        return "UUID"
    backend_type = str(column_type).upper()
    if normalized == TIMESTAMP_TZ:
        return "TIMESTAMP WITH TIME ZONE"
    if normalized == LOCAL_DATETIME_NAIVE:
        return "TIMESTAMP WITHOUT TIME ZONE"
    if normalized == DATE:
        return "DATE"
    return backend_type or normalized
