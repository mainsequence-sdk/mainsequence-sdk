from __future__ import annotations

import datetime as dt
from typing import Any

import pandas as pd

from mainsequence.client.models_tdag import LOGICAL_COLUMN_DTYPES_ATTR
from mainsequence.markets.markets_data_node import (
    MarketDataNode,
    MarketDataNodeConfiguration,
)
from mainsequence.tdag.data_nodes import (
    DataNode,
    DataNodeMetaData,
    RecordDefinition,
)

EXECUTION_SCHEMA_BOOTSTRAP_TIME_INDEX = dt.datetime(1970, 1, 1, tzinfo=dt.UTC)
EXECUTION_SCHEMA_BOOTSTRAP_IDENTIFIER = "__schema_bootstrap_execution__"
EXECUTION_SCHEMA_BOOTSTRAP_ASSET_IDENTIFIER = "__schema_bootstrap_asset__"

ORDERS_TIME_INDEX_NAME = "order_time"
ORDERS_INDEX_NAMES = [
    "order_time",
    "order_unique_identifier",
    "account_unique_identifier",
    "asset_unique_identifier",
]
ORDERS_COLUMN_DTYPES_MAP = {
    "order_time": "datetime64[ns, UTC]",
    "order_unique_identifier": "string",
    "account_unique_identifier": "string",
    "fund_unique_identifier": "string",
    "order_manager_unique_identifier": "string",
    "asset_unique_identifier": "string",
    "order_remote_id": "string",
    "client_order_id": "string",
    "order_type": "string",
    "order_side": "int64",
    "quantity": "float64",
    "status": "string",
    "filled_quantity": "float64",
    "filled_price": "float64",
    "expires_time": "datetime64[ns, UTC]",
    "limit_price": "float64",
    "time_in_force": "string",
    "comments": "string",
    "venue_metadata": "jsonb",
}

ORDER_EVENTS_TIME_INDEX_NAME = "event_time"
ORDER_EVENTS_INDEX_NAMES = [
    "event_time",
    "order_unique_identifier",
]
ORDER_EVENTS_COLUMN_DTYPES_MAP = {
    "event_time": "datetime64[ns, UTC]",
    "order_unique_identifier": "string",
    "order_status": "string",
    "event_metadata": "jsonb",
}

TRADES_TIME_INDEX_NAME = "trade_time"
TRADES_INDEX_NAMES = [
    "trade_time",
    "trade_unique_identifier",
    "account_unique_identifier",
    "asset_unique_identifier",
]
TRADES_COLUMN_DTYPES_MAP = {
    "trade_time": "datetime64[ns, UTC]",
    "trade_unique_identifier": "string",
    "account_unique_identifier": "string",
    "fund_unique_identifier": "string",
    "order_unique_identifier": "string",
    "asset_unique_identifier": "string",
    "trade_side": "int64",
    "quantity": "float64",
    "price": "float64",
    "commission": "float64",
    "commission_asset_unique_identifier": "string",
    "settlement_cost": "float64",
    "settlement_asset_unique_identifier": "string",
    "comments": "string",
    "venue_metadata": "jsonb",
}

EXECUTION_ERRORS_TIME_INDEX_NAME = "time_recorded"
EXECUTION_ERRORS_INDEX_NAMES = [
    "time_recorded",
    "error_unique_identifier",
]
EXECUTION_ERRORS_COLUMN_DTYPES_MAP = {
    "time_recorded": "datetime64[ns, UTC]",
    "error_unique_identifier": "string",
    "account_unique_identifier": "string",
    "fund_unique_identifier": "string",
    "order_unique_identifier": "string",
    "order_manager_unique_identifier": "string",
    "error_code": "string",
    "error_message": "string",
    "error_traceback": "string",
    "metadata": "jsonb",
}


class ExecutionDataNodeConfiguration(MarketDataNodeConfiguration):
    """Configuration base for SDK-owned execution DataNodes."""

    time_index_name: str
    index_names: list[str]
    records: list[RecordDefinition]

    @property
    def column_dtypes_map(self) -> dict[str, str]:
        return {record.column_name: record.dtype for record in self.records}


class ExecutionDataNode(MarketDataNode):
    """Base DataNode for timestamped execution facts."""

    def __init__(
        self,
        config: ExecutionDataNodeConfiguration | None = None,
        *args,
        **kwargs,
    ):
        resolved_config = self._validate_config(config or self.default_config())
        super().__init__(resolved_config, *args, **kwargs)

    def dependencies(self) -> dict[str, DataNode]:
        return {}

    @classmethod
    def default_config(
        cls,
        *,
        identifier: str | None = None,
        description: str | None = None,
        extra_records: list[RecordDefinition] | None = None,
    ) -> ExecutionDataNodeConfiguration:
        return cls._validate_config(
            ExecutionDataNodeConfiguration(
                time_index_name=cls._required_time_index_name(),
                index_names=cls._required_index_names(),
                records=cls._records_with_extra(extra_records=extra_records),
                node_metadata=DataNodeMetaData(
                    identifier=identifier or cls._default_identifier(),
                    description=description or cls._default_description(),
                ),
            )
        )

    @classmethod
    def _records_with_extra(
        cls,
        *,
        extra_records: list[RecordDefinition] | None = None,
    ) -> list[RecordDefinition]:
        required_records = cls._required_records()
        if not extra_records:
            return list(required_records)
        merged_records = list(required_records)
        existing_dtypes = {
            record.column_name: record.dtype
            for record in required_records
        }
        for record in extra_records:
            existing_dtype = existing_dtypes.get(record.column_name)
            if existing_dtype is not None:
                if existing_dtype != record.dtype:
                    raise ValueError(
                        f"Extra record {record.column_name!r} has dtype "
                        f"{record.dtype!r}, expected {existing_dtype!r}."
                    )
                continue
            merged_records.append(record)
            existing_dtypes[record.column_name] = record.dtype
        return merged_records

    @classmethod
    def _validate_config(
        cls,
        config: ExecutionDataNodeConfiguration,
    ) -> ExecutionDataNodeConfiguration:
        if not isinstance(config, ExecutionDataNodeConfiguration):
            raise TypeError(
                f"{cls.__name__} requires an ExecutionDataNodeConfiguration."
            )
        if config.time_index_name != cls._required_time_index_name():
            raise ValueError(
                f"{cls.__name__} requires time_index_name "
                f"{cls._required_time_index_name()!r}."
            )
        if config.index_names != cls._required_index_names():
            raise ValueError(
                f"{cls.__name__} requires index_names "
                f"{cls._required_index_names()!r}."
            )
        _validate_required_records(
            records=list(config.records),
            required_records=cls._required_records(),
        )
        return config

    @classmethod
    def _default_identifier(cls) -> str:
        raise NotImplementedError

    @classmethod
    def _default_description(cls) -> str:
        raise NotImplementedError

    @classmethod
    def _required_time_index_name(cls) -> str:
        raise NotImplementedError

    @classmethod
    def _required_index_names(cls) -> list[str]:
        raise NotImplementedError

    @classmethod
    def _required_records(cls) -> list[RecordDefinition]:
        raise NotImplementedError

    @classmethod
    def _schema_bootstrap_index_values(cls) -> dict[str, Any]:
        raise NotImplementedError

    def _execution_config(self) -> ExecutionDataNodeConfiguration:
        return self.__class__._validate_config(
            getattr(self, "config", None) or self.default_config()
        )

    def update(self) -> pd.DataFrame:
        return self.validate_execution_frame(
            self.get_execution_frame(),
            config=self._execution_config(),
        )

    def set_frame(self, frame: pd.DataFrame) -> ExecutionDataNode:
        self._execution_data_frame = frame
        return self

    def get_execution_frame(self) -> pd.DataFrame:
        frame = getattr(self, "_execution_data_frame", None)
        if frame is None:
            return self.build_schema_bootstrap_frame(config=self._execution_config())
        return frame

    @classmethod
    def build_initialization_frame(cls, **kwargs) -> pd.DataFrame:
        return cls.build_schema_bootstrap_frame(**kwargs)

    @classmethod
    def build_mock_frame(cls, **kwargs) -> pd.DataFrame:
        return cls.build_schema_bootstrap_frame(**kwargs)

    @classmethod
    def build_schema_bootstrap_frame(
        cls,
        *,
        config: ExecutionDataNodeConfiguration | None = None,
        index_values: dict[str, Any] | None = None,
        time_index: dt.datetime | pd.Timestamp = EXECUTION_SCHEMA_BOOTSTRAP_TIME_INDEX,
    ) -> pd.DataFrame:
        config = cls._validate_config(config or cls.default_config())
        row: dict[str, Any] = {}
        for record in config.records or []:
            row[record.column_name] = _schema_bootstrap_value(
                dtype=record.dtype,
                time_index=time_index,
            )
        row[config.time_index_name] = pd.Timestamp(time_index).isoformat()
        for key, value in {
            **cls._schema_bootstrap_index_values(),
            **(index_values or {}),
        }.items():
            row[key] = value
        frame = pd.DataFrame([row])
        return cls.validate_execution_frame(frame, config=config)

    @classmethod
    def validate_execution_frame(
        cls,
        data_frame: pd.DataFrame,
        *,
        config: ExecutionDataNodeConfiguration | None = None,
    ) -> pd.DataFrame:
        config = cls._validate_config(config or cls.default_config())
        return _validate_execution_frame(data_frame, config=config)

    @classmethod
    def validate_frame(
        cls,
        data_frame: pd.DataFrame,
        *,
        config: ExecutionDataNodeConfiguration | None = None,
    ) -> pd.DataFrame:
        return cls.validate_execution_frame(data_frame, config=config)


class Orders(ExecutionDataNode):
    """Timestamped order records replacing Django Order, MarketOrder, and LimitOrder."""

    @classmethod
    def _default_identifier(cls) -> str:
        return "mainsequence.markets.execution.orders"

    @classmethod
    def _default_description(cls) -> str:
        return "Timestamped execution order records keyed by order_time."

    @classmethod
    def _required_time_index_name(cls) -> str:
        return ORDERS_TIME_INDEX_NAME

    @classmethod
    def _required_index_names(cls) -> list[str]:
        return list(ORDERS_INDEX_NAMES)

    @classmethod
    def _required_records(cls) -> list[RecordDefinition]:
        return _record_definitions_from_dtype_map(ORDERS_COLUMN_DTYPES_MAP)

    @classmethod
    def _schema_bootstrap_index_values(cls) -> dict[str, Any]:
        return {
            "order_unique_identifier": EXECUTION_SCHEMA_BOOTSTRAP_IDENTIFIER,
            "account_unique_identifier": EXECUTION_SCHEMA_BOOTSTRAP_IDENTIFIER,
            "asset_unique_identifier": EXECUTION_SCHEMA_BOOTSTRAP_ASSET_IDENTIFIER,
        }


class OrderEvents(ExecutionDataNode):
    """Timestamped order status events."""

    @classmethod
    def _default_identifier(cls) -> str:
        return "mainsequence.markets.execution.order_events"

    @classmethod
    def _default_description(cls) -> str:
        return "Timestamped order status events keyed by event_time."

    @classmethod
    def _required_time_index_name(cls) -> str:
        return ORDER_EVENTS_TIME_INDEX_NAME

    @classmethod
    def _required_index_names(cls) -> list[str]:
        return list(ORDER_EVENTS_INDEX_NAMES)

    @classmethod
    def _required_records(cls) -> list[RecordDefinition]:
        return _record_definitions_from_dtype_map(ORDER_EVENTS_COLUMN_DTYPES_MAP)

    @classmethod
    def _schema_bootstrap_index_values(cls) -> dict[str, Any]:
        return {"order_unique_identifier": EXECUTION_SCHEMA_BOOTSTRAP_IDENTIFIER}


class Trades(ExecutionDataNode):
    """Timestamped trade execution records."""

    @classmethod
    def _default_identifier(cls) -> str:
        return "mainsequence.markets.execution.trades"

    @classmethod
    def _default_description(cls) -> str:
        return "Timestamped trade executions keyed by trade_time."

    @classmethod
    def _required_time_index_name(cls) -> str:
        return TRADES_TIME_INDEX_NAME

    @classmethod
    def _required_index_names(cls) -> list[str]:
        return list(TRADES_INDEX_NAMES)

    @classmethod
    def _required_records(cls) -> list[RecordDefinition]:
        return _record_definitions_from_dtype_map(TRADES_COLUMN_DTYPES_MAP)

    @classmethod
    def _schema_bootstrap_index_values(cls) -> dict[str, Any]:
        return {
            "trade_unique_identifier": EXECUTION_SCHEMA_BOOTSTRAP_IDENTIFIER,
            "account_unique_identifier": EXECUTION_SCHEMA_BOOTSTRAP_IDENTIFIER,
            "asset_unique_identifier": EXECUTION_SCHEMA_BOOTSTRAP_ASSET_IDENTIFIER,
        }


class ExecutionErrors(ExecutionDataNode):
    """Timestamped execution error records."""

    @classmethod
    def _default_identifier(cls) -> str:
        return "mainsequence.markets.execution.errors"

    @classmethod
    def _default_description(cls) -> str:
        return "Timestamped execution failures keyed by time_recorded."

    @classmethod
    def _required_time_index_name(cls) -> str:
        return EXECUTION_ERRORS_TIME_INDEX_NAME

    @classmethod
    def _required_index_names(cls) -> list[str]:
        return list(EXECUTION_ERRORS_INDEX_NAMES)

    @classmethod
    def _required_records(cls) -> list[RecordDefinition]:
        return _record_definitions_from_dtype_map(EXECUTION_ERRORS_COLUMN_DTYPES_MAP)

    @classmethod
    def _schema_bootstrap_index_values(cls) -> dict[str, Any]:
        return {"error_unique_identifier": EXECUTION_SCHEMA_BOOTSTRAP_IDENTIFIER}


def _record_definitions_from_dtype_map(
    column_dtypes_map: dict[str, str],
) -> list[RecordDefinition]:
    return [
        RecordDefinition(
            column_name=column_name,
            dtype=dtype,
            label=column_name.replace("_", " ").title(),
            description=f"Execution field {column_name}.",
        )
        for column_name, dtype in column_dtypes_map.items()
    ]


def _validate_required_records(
    *,
    records: list[RecordDefinition],
    required_records: list[RecordDefinition],
) -> None:
    dtype_by_column = {record.column_name: record.dtype for record in records}
    errors = []
    for required_record in required_records:
        actual_dtype = dtype_by_column.get(required_record.column_name)
        if actual_dtype != required_record.dtype:
            errors.append(
                f"{required_record.column_name!r} dtype {actual_dtype!r} "
                f"does not match required dtype {required_record.dtype!r}"
            )
    if errors:
        raise ValueError(
            "Execution records must include the required columns: "
            + "; ".join(errors)
        )


def _validate_execution_frame(
    data_frame: pd.DataFrame,
    *,
    config: ExecutionDataNodeConfiguration,
) -> pd.DataFrame:
    frame = data_frame.copy()
    if list(frame.index.names) == config.index_names:
        flat = frame.reset_index()
    elif all(index_name in frame.columns for index_name in config.index_names):
        flat = frame
    else:
        raise ValueError(
            "Execution frame must use index_names "
            f"{config.index_names} or include those columns before validation."
        )

    missing_columns = [
        column_name
        for column_name in config.column_dtypes_map
        if column_name not in flat.columns
    ]
    if missing_columns:
        raise ValueError(
            "Execution frame is missing required columns: "
            f"{', '.join(missing_columns)}."
        )

    flat = _normalize_execution_values(flat, config=config)
    frame = flat[list(config.column_dtypes_map)].set_index(config.index_names)
    if frame.index.has_duplicates:
        raise ValueError(
            "Execution frame contains duplicate rows for index contract "
            f"{config.index_names}."
        )
    frame.attrs[LOGICAL_COLUMN_DTYPES_ATTR] = dict(config.column_dtypes_map)
    return frame.sort_index()


def _normalize_execution_values(
    frame: pd.DataFrame,
    *,
    config: ExecutionDataNodeConfiguration,
) -> pd.DataFrame:
    normalized = frame.copy()
    for column_name, dtype in config.column_dtypes_map.items():
        values = normalized[column_name]
        if dtype == "datetime64[ns, UTC]":
            normalized[column_name] = pd.to_datetime(values, utc=True)
        elif dtype == "string":
            normalized[column_name] = values.fillna("").map(str)
        elif dtype == "float64":
            normalized[column_name] = pd.to_numeric(values, errors="coerce").fillna(0.0)
        elif dtype == "int64":
            normalized[column_name] = pd.to_numeric(values, errors="coerce").fillna(0).astype("int64")
        elif dtype == "bool":
            normalized[column_name] = values.map(bool)
        elif dtype == "jsonb":
            normalized[column_name] = values.map(_normalize_jsonb)
        else:
            raise ValueError(f"Unsupported execution dtype {dtype!r}.")
    return normalized


def _normalize_jsonb(value: Any) -> dict[str, Any] | list[Any]:
    if value is None or (not isinstance(value, (dict, list)) and pd.isna(value)):
        return {}
    if isinstance(value, (dict, list)):
        return value
    raise ValueError(f"Invalid jsonb execution value {value!r}.")


def _schema_bootstrap_value(
    *,
    dtype: str,
    time_index: dt.datetime | pd.Timestamp,
) -> Any:
    if dtype == "datetime64[ns, UTC]":
        return pd.Timestamp(time_index).isoformat()
    if dtype == "float64":
        return 0.0
    if dtype == "int64":
        return 0
    if dtype == "bool":
        return False
    if dtype == "jsonb":
        return {"_mainsequence_reserved": "schema_bootstrap"}
    return ""


__all__ = [
    "EXECUTION_ERRORS_COLUMN_DTYPES_MAP",
    "EXECUTION_ERRORS_INDEX_NAMES",
    "EXECUTION_ERRORS_TIME_INDEX_NAME",
    "ORDER_EVENTS_COLUMN_DTYPES_MAP",
    "ORDER_EVENTS_INDEX_NAMES",
    "ORDER_EVENTS_TIME_INDEX_NAME",
    "ORDERS_COLUMN_DTYPES_MAP",
    "ORDERS_INDEX_NAMES",
    "ORDERS_TIME_INDEX_NAME",
    "TRADES_COLUMN_DTYPES_MAP",
    "TRADES_INDEX_NAMES",
    "TRADES_TIME_INDEX_NAME",
    "ExecutionDataNode",
    "ExecutionDataNodeConfiguration",
    "ExecutionErrors",
    "OrderEvents",
    "Orders",
    "Trades",
]
