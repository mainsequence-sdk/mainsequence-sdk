from __future__ import annotations

from typing import Any

import pandas as pd
from pydantic import Field

from mainsequence.client.models_tdag import LOGICAL_COLUMN_DTYPES_ATTR
from mainsequence.markets.markets_data_node import MarketDataNode
from mainsequence.tdag.data_nodes import (
    DataNode,
    DataNodeConfiguration,
    DataNodeMetaData,
    RecordDefinition,
)

from .constants import *


class PortfolioCanonicalDataNodeConfiguration(DataNodeConfiguration):
    """Configuration base for SDK-created canonical Portfolios data nodes."""

    index_names: list[str]
    records: list[RecordDefinition]

    @property
    def time_index_name(self) -> str:
        return PORTFOLIO_CANONICAL_TIME_INDEX_NAME

    @property
    def identity_index_names(self) -> list[str]:
        return self.index_names[1:]

    @property
    def column_dtypes_map(self) -> dict[str, str]:
        return {record.column_name: record.dtype for record in self.records}


class SignalWeightsConfiguration(PortfolioCanonicalDataNodeConfiguration):
    """Canonical SignalWeights table config plus runtime signal input."""

    signal_configuration: Any | None = Field(
        default=None,
        json_schema_extra={"runtime_only": True},
    )


class PortfolioCanonicalDataNode(DataNode):
    """Base DataNode for canonical shared Portfolios tables."""

    _HASH_NAMESPACE_ALIASES = ("namespace",)

    def __init__(
        self,
        config: PortfolioCanonicalDataNodeConfiguration | None = None,
        *args,
        namespace: str | None = None,
        **kwargs,
    ):
        """Create a canonical Portfolios node.

        ``namespace`` is the market-domain alias for DataNode ``hash_namespace``.
        The DataNode constructor wrapper consumes it before this method runs, so
        it is intentionally not forwarded as a separate config field.
        """
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
    ) -> PortfolioCanonicalDataNodeConfiguration:
        return cls._validate_config(
            PortfolioCanonicalDataNodeConfiguration(
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
        resolved_records = (
            _merge_records(required_records, extra_records)
            if extra_records
            else list(required_records)
        )
        _validate_records(
            records=resolved_records,
            required_records=required_records,
        )
        return resolved_records

    @classmethod
    def _validate_config(
        cls,
        config: PortfolioCanonicalDataNodeConfiguration,
    ) -> PortfolioCanonicalDataNodeConfiguration:
        if not isinstance(config, PortfolioCanonicalDataNodeConfiguration):
            raise TypeError(f"{cls.__name__} requires a PortfolioCanonicalDataNodeConfiguration.")
        if config.index_names != cls._required_index_names():
            raise ValueError(
                f"{cls.__name__} requires index_names " f"{cls._required_index_names()!r}."
            )
        _validate_records(
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
    def _required_index_names(cls) -> list[str]:
        raise NotImplementedError

    @classmethod
    def _required_records(cls) -> list[RecordDefinition]:
        raise NotImplementedError

    @classmethod
    def _schema_bootstrap_index_values(cls) -> dict[str, Any]:
        raise NotImplementedError

    def _canonical_config(self) -> PortfolioCanonicalDataNodeConfiguration:
        return self.__class__._validate_config(
            getattr(self, "config", None) or self.default_config()
        )

    def update(self) -> pd.DataFrame:
        return self.validate_frame(
            self.get_canonical_frame(),
            config=self._canonical_config(),
        )

    def get_canonical_frame(self) -> pd.DataFrame:
        return self.build_schema_bootstrap_frame(config=self._canonical_config())

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
        config: PortfolioCanonicalDataNodeConfiguration | None = None,
        index_values: dict[str, Any] | None = None,
        time_index: dt.datetime | pd.Timestamp = SCHEMA_BOOTSTRAP_TIME_INDEX,
    ) -> pd.DataFrame:
        config = cls._validate_config(config or cls.default_config())
        resolved_index_values = {
            **cls._schema_bootstrap_index_values(),
            **(index_values or {}),
        }
        row: dict[str, Any] = {
            config.time_index_name: time_index,
        }
        for index_name in config.identity_index_names:
            row[index_name] = resolved_index_values[index_name]
        for record in config.records or []:
            if record.column_name not in row:
                row[record.column_name] = _schema_bootstrap_value(
                    dtype=record.dtype,
                    time_index=time_index,
                )
        frame = pd.DataFrame([row])
        frame = frame.set_index(config.index_names)
        return cls.validate_frame(frame, config=config)

    @classmethod
    def validate_frame(
        cls,
        data_frame: pd.DataFrame,
        *,
        config: PortfolioCanonicalDataNodeConfiguration | None = None,
    ) -> pd.DataFrame:
        return _validate_canonical_frame(
            data_frame,
            config=cls._validate_config(config or cls.default_config()),
            frame_name=cls.__name__,
        )

    @classmethod
    def validate_weights_frame(
        cls,
        data_frame: pd.DataFrame,
        *,
        config: PortfolioCanonicalDataNodeConfiguration | None = None,
    ) -> pd.DataFrame:
        return cls.validate_frame(data_frame, config=config)

    def canonical_data_source_uid(self) -> str:
        return self.ensure_storage_ready()

    def canonical_data_source_id(self) -> str:
        return self.canonical_data_source_uid()

    def ensure_storage_ready(self, *, force_update: bool = False) -> str:
        storage = None if force_update else self._ready_storage_or_none()
        if storage is None:
            storage = self._initialize_source_table_storage_or_none()

        if storage is None:
            raise RuntimeError(
                f"{self.__class__.__name__} did not create a ready canonical "
                "Portfolios data node. Initialize the canonical Portfolios storage family "
                "before writing."
            )
        return _coerce_required_uid(storage, field_name="data_node_storage")

    def _initialize_source_table_storage_or_none(self):
        from .storage_initialization import initialize_portfolio_storage_source_tables

        initialize_portfolio_storage_source_tables(anchor_node=self)
        return self._ready_storage_or_none()

    def _ready_storage_or_none(self):
        storage = self.data_node_storage
        if _coerce_optional_uid(storage, field_name="data_node_storage") is None:
            return None

        source_config = _storage_source_config(storage)
        if source_config is None:
            return None

        self._validate_storage_contract(source_config)
        return storage

    def _validate_storage_contract(self, source_config: Any) -> None:
        config = self._canonical_config()
        errors: list[str] = []

        time_index_name = _get_mapping_or_attr(source_config, "time_index_name")
        if time_index_name != config.time_index_name:
            errors.append(
                "time_index_name " f"{time_index_name!r} does not match {config.time_index_name!r}"
            )

        index_names = list(_get_mapping_or_attr(source_config, "index_names") or [])
        if index_names != config.index_names:
            errors.append(f"index_names {index_names!r} do not match {config.index_names!r}")

        column_dtypes_map = dict(_get_mapping_or_attr(source_config, "column_dtypes_map") or {})
        for column_name, expected_dtype in config.column_dtypes_map.items():
            actual_dtype = column_dtypes_map.get(column_name)
            if actual_dtype != expected_dtype:
                errors.append(
                    f"{column_name!r} dtype {actual_dtype!r} does not match " f"{expected_dtype!r}"
                )

        if errors:
            raise ValueError(
                f"{self.__class__.__name__} is bound to an incompatible "
                "canonical Portfolios data node: " + "; ".join(errors)
            )


class AssetScopedPortfolioCanonicalDataNode(PortfolioCanonicalDataNode, MarketDataNode):
    """Canonical Portfolios DataNode whose identity includes asset unique_identifier."""

    def _initialize_configuration(self, init_kwargs: dict) -> None:
        _drop_empty_framework_init_kwargs(init_kwargs)
        super()._initialize_configuration(init_kwargs=init_kwargs)


def _record_definitions_from_dtype_map(
    column_dtypes_map: dict[str, str],
    *,
    labels: dict[str, str],
    descriptions: dict[str, str],
) -> list[RecordDefinition]:
    return [
        RecordDefinition(
            column_name=column_name,
            dtype=dtype,
            label=labels.get(column_name, column_name),
            description=descriptions.get(column_name),
        )
        for column_name, dtype in column_dtypes_map.items()
    ]


def _is_canonical_frame(
    frame: pd.DataFrame,
    *,
    config: PortfolioCanonicalDataNodeConfiguration,
) -> bool:
    return (
        isinstance(frame, pd.DataFrame)
        and isinstance(frame.index, pd.MultiIndex)
        and list(frame.index.names) == config.index_names
    )


def _class_import_path(cls: type) -> dict[str, str]:
    return {
        "module": cls.__module__,
        "qualname": cls.__qualname__,
    }


def _drop_empty_framework_init_kwargs(init_kwargs: dict) -> None:
    if init_kwargs.get("hash_namespace") in (None, ""):
        init_kwargs.pop("hash_namespace", None)
    if init_kwargs.get("test_node") is False:
        init_kwargs.pop("test_node", None)


def _drop_excluded_keys(value: Any, *, excluded_keys: frozenset[str]) -> Any:
    if isinstance(value, list):
        return [_drop_excluded_keys(item, excluded_keys=excluded_keys) for item in value]
    if isinstance(value, tuple):
        return tuple(_drop_excluded_keys(item, excluded_keys=excluded_keys) for item in value)
    if isinstance(value, dict):
        return {
            key: _drop_excluded_keys(item, excluded_keys=excluded_keys)
            for key, item in value.items()
            if key not in excluded_keys
        }
    return value


def _reset_frame_index(frame: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(frame, pd.DataFrame):
        raise TypeError("canonical Portfolios normalizers require a pandas DataFrame.")
    return frame.copy().reset_index()


def _empty_flat_frame(
    *,
    config: PortfolioCanonicalDataNodeConfiguration,
) -> pd.DataFrame:
    return pd.DataFrame(columns=list(config.column_dtypes_map))


def _require_columns(
    frame: pd.DataFrame,
    *,
    required_columns: list[str],
    frame_name: str,
) -> None:
    missing_columns = [
        column_name for column_name in required_columns if column_name not in frame.columns
    ]
    if missing_columns:
        raise ValueError(
            f"{frame_name} frame is missing required canonical columns: "
            f"{', '.join(missing_columns)}."
        )


def _normalize_pivoted_signal_weights(frame: pd.DataFrame) -> pd.DataFrame:
    if PORTFOLIO_CANONICAL_TIME_INDEX_NAME not in frame.columns:
        raise ValueError(
            "SignalWeights frame must include 'signal_weight' rows or a "
            "'time_index' column for pivoted signal weights."
        )

    value_columns = [
        column_name for column_name in frame.columns if column_name != PORTFOLIO_CANONICAL_TIME_INDEX_NAME
    ]
    if not value_columns:
        raise ValueError("Pivoted SignalWeights frame must contain asset columns to unpivot.")

    return frame.melt(
        id_vars=[PORTFOLIO_CANONICAL_TIME_INDEX_NAME],
        value_vars=value_columns,
        var_name=ASSET_UNIQUE_IDENTIFIER,
        value_name="signal_weight",
    )


def _merge_records(
    required_records: list[RecordDefinition],
    extra_records: list[RecordDefinition],
) -> list[RecordDefinition]:
    merged_records = list(required_records)
    existing_dtypes = {record.column_name: record.dtype for record in required_records}
    for record in extra_records:
        existing_dtype = existing_dtypes.get(record.column_name)
        if existing_dtype is not None:
            if existing_dtype != record.dtype:
                raise ValueError(
                    f"Extra record {record.column_name!r} has dtype "
                    f"{record.dtype!r}, but the required dtype is "
                    f"{existing_dtype!r}."
                )
            continue
        merged_records.append(record)
        existing_dtypes[record.column_name] = record.dtype
    return merged_records


def _validate_records(
    *,
    records: list[RecordDefinition],
    required_records: list[RecordDefinition],
) -> None:
    seen_columns: set[str] = set()
    duplicate_columns: list[str] = []
    for record in records:
        if record.column_name in seen_columns:
            duplicate_columns.append(record.column_name)
        seen_columns.add(record.column_name)
    if duplicate_columns:
        raise ValueError(
            "Canonical Portfolios records must be duplicate-free. Duplicate columns: "
            + ", ".join(sorted(set(duplicate_columns)))
            + "."
        )

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
            "Canonical Portfolios records must include the required columns: " + "; ".join(errors)
        )


def _schema_bootstrap_value(
    *,
    dtype: str,
    time_index: dt.datetime | pd.Timestamp,
) -> Any:
    if dtype == "datetime64[ns, UTC]":
        return pd.Timestamp(time_index)
    if dtype == "float64":
        return 0.0
    if dtype == "string":
        return ""
    raise ValueError(f"Unsupported canonical Portfolios dtype {dtype!r}.")


def _validate_canonical_frame(
    data_frame: pd.DataFrame,
    *,
    config: PortfolioCanonicalDataNodeConfiguration,
    frame_name: str,
) -> pd.DataFrame:
    frame = _ensure_config_index(data_frame, config=config, frame_name=frame_name)
    flat = frame.reset_index()
    missing_columns = [
        column_name for column_name in config.column_dtypes_map if column_name not in flat.columns
    ]
    if missing_columns:
        raise ValueError(
            f"{frame_name} frame is missing required columns: " f"{', '.join(missing_columns)}."
        )

    flat = _normalize_config_values(flat, config=config, frame_name=frame_name)
    _validate_identity_values(flat, config=config, frame_name=frame_name)
    frame = flat.set_index(config.index_names).sort_index()
    if frame.index.has_duplicates:
        raise ValueError(
            f"{frame_name} frame contains duplicate rows for index contract "
            f"{config.index_names}."
        )
    return _attach_logical_dtype_contract(frame, config=config)


def _ensure_config_index(
    data_frame: pd.DataFrame,
    *,
    config: PortfolioCanonicalDataNodeConfiguration,
    frame_name: str,
) -> pd.DataFrame:
    expected_index_names = list(config.index_names)
    frame = data_frame.copy()
    if list(frame.index.names) == expected_index_names:
        return frame
    if all(index_name in frame.columns for index_name in expected_index_names):
        return frame.set_index(expected_index_names)
    raise ValueError(
        f"{frame_name} frame must use index_names "
        f"{expected_index_names} or include those columns before validation."
    )


def _normalize_config_values(
    frame: pd.DataFrame,
    *,
    config: PortfolioCanonicalDataNodeConfiguration,
    frame_name: str,
) -> pd.DataFrame:
    normalized = frame.copy()
    for column_name, dtype in config.column_dtypes_map.items():
        values = normalized[column_name]
        if column_name == config.time_index_name:
            normalized[column_name] = _normalize_time_index(values)
        elif dtype == "datetime64[ns, UTC]":
            normalized[column_name] = _normalize_datetime_column(values)
        elif dtype == "float64":
            normalized[column_name] = _normalize_float64(values, column_name=column_name)
        elif dtype == "string":
            normalized[column_name] = _normalize_string(values)
        else:
            raise ValueError(
                f"Unsupported canonical Portfolios dtype {dtype!r} for " f"{frame_name}.{column_name!r}."
            )
    return normalized


def _validate_identity_values(
    frame: pd.DataFrame,
    *,
    config: PortfolioCanonicalDataNodeConfiguration,
    frame_name: str,
) -> None:
    for index_name in config.identity_index_names:
        values = frame[index_name]
        invalid_values = values.isna() | (values.astype(str).str.len() == 0)
        if invalid_values.any():
            raise ValueError(
                f"{frame_name} frame has empty identity values for " f"{index_name!r}."
            )


def _normalize_time_index(values: Any) -> pd.Series:
    return pd.to_datetime(values, utc=True).astype("datetime64[ns, UTC]")


def _normalize_datetime_column(values: Any) -> pd.Series:
    return pd.to_datetime(values, utc=True).astype("datetime64[ns, UTC]")


def _normalize_float64(values: Any, *, column_name: str) -> pd.Series:
    try:
        return pd.to_numeric(values, errors="raise").astype("float64")
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid float64 canonical Portfolios value for {column_name!r}.") from exc


def _normalize_string(values: Any) -> pd.Series:
    return values.map(lambda value: "" if pd.isna(value) else str(value)).astype("string")


def _attach_logical_dtype_contract(
    frame: pd.DataFrame,
    *,
    config: PortfolioCanonicalDataNodeConfiguration,
) -> pd.DataFrame:
    frame.attrs[LOGICAL_COLUMN_DTYPES_ATTR] = dict(config.column_dtypes_map)
    return frame


def _storage_source_config(storage: Any) -> Any | None:
    return (
        _get_mapping_or_attr(storage, "sourcetableconfiguration")
        or _get_mapping_or_attr(storage, "source_table_configuration")
        or _get_mapping_or_attr(storage, "source_table_config")
    )


def _get_mapping_or_attr(value: Any, field_name: str) -> Any:
    if isinstance(value, dict):
        return value.get(field_name)
    return getattr(value, field_name, None)


def _coerce_required_uid(value: Any, *, field_name: str) -> str:
    value_uid = _coerce_optional_uid(value, field_name=field_name)
    if value_uid is None:
        raise ValueError(f"{field_name} must expose a public uid.")
    return value_uid


def _coerce_optional_uid(value: Any, *, field_name: str) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value or None
    value_uid = value.get("uid") if isinstance(value, dict) else getattr(value, "uid", None)
    if value_uid in (None, ""):
        return None
    return str(value_uid)
