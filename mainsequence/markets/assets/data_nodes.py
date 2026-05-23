from __future__ import annotations

import datetime as dt
from typing import Any

import pandas as pd
from pydantic import Field

from mainsequence.tdag.data_nodes import (
    DataNode,
    DataNodeConfiguration,
    DataNodeMetaData,
    RecordDefinition,
)

ASSET_DATA_NODE_TIME_INDEX_NAME = "time_index"
ASSET_DATA_NODE_INDEX_NAMES = ["time_index", "unique_identifier"]
ASSET_DATA_NODE_BOOTSTRAP_UNIQUE_IDENTIFIER = "__schema_bootstrap__"
ASSET_DATA_NODE_BOOTSTRAP_TIME_INDEX = dt.datetime(1970, 1, 1, tzinfo=dt.UTC)

ASSET_SNAPSHOT_COLUMN_DTYPES_MAP = {
    "time_index": "datetime64[ns, UTC]",
    "unique_identifier": "string",
    "name": "string",
    "ticker": "string",
    "exchange_code": "string",
    "asset_ticker_group_id": "string",
    "venue_specific_properties": "jsonb",
}
ASSET_SNAPSHOT_COLUMN_LABELS = {
    "time_index": "Time Index",
    "unique_identifier": "Unique Identifier",
    "name": "Name",
    "ticker": "Ticker",
    "exchange_code": "Exchange Code",
    "asset_ticker_group_id": "Asset Ticker Group ID",
    "venue_specific_properties": "Venue Specific Properties",
}
ASSET_SNAPSHOT_COLUMN_DESCRIPTIONS = {
    "time_index": "UTC timestamp for the asset display snapshot.",
    "unique_identifier": "Asset unique identifier from the selected master-list table.",
    "name": "Security name as recorded by the asset data provider.",
    "ticker": "Ticker or display symbol.",
    "exchange_code": "Exchange or market code.",
    "asset_ticker_group_id": "Highest aggregation level for share-class grouping.",
    "venue_specific_properties": "JSON payload for exchange-specific metadata.",
}

ASSET_PRICING_DETAIL_COLUMN_DTYPES_MAP = {
    "time_index": "datetime64[ns, UTC]",
    "unique_identifier": "string",
    "instrument_dump": "jsonb",
}
ASSET_PRICING_DETAIL_COLUMN_LABELS = {
    "time_index": "Time Index",
    "unique_identifier": "Unique Identifier",
    "instrument_dump": "Instrument Dump",
}
ASSET_PRICING_DETAIL_COLUMN_DESCRIPTIONS = {
    "time_index": "UTC timestamp for the pricing metadata payload.",
    "unique_identifier": "Asset unique identifier from the selected master-list table.",
    "instrument_dump": "Provider-specific pricing instrument payload.",
}


class AssetDataNodeConfiguration(DataNodeConfiguration):
    """Configuration for timestamped asset DataNodes."""

    time_index_name: str = Field(
        ...,
        description="Timestamp column used as the DataNode time index.",
    )
    index_names: list[str] = Field(
        ...,
        description="Canonical DataFrame index columns for the asset DataNode.",
    )
    records: list[RecordDefinition] = Field(
        ...,
        description="Output schema for the asset DataNode.",
    )

    @property
    def column_dtypes_map(self) -> dict[str, str]:
        return {record.column_name: record.dtype for record in self.records}


class AssetTimestampedDataNode(DataNode):
    """Base DataNode for timestamped asset facts keyed by asset unique_identifier."""

    def __init__(
        self,
        config: AssetDataNodeConfiguration | None = None,
        *args: Any,
        **kwargs: Any,
    ):
        super().__init__(config=config or self.default_config(), *args, **kwargs)

    @classmethod
    def default_config(
        cls,
        *,
        identifier: str | None = None,
        description: str | None = None,
        extra_records: list[RecordDefinition] | None = None,
    ) -> AssetDataNodeConfiguration:
        records = cls._records_with_extra(extra_records=extra_records)
        return AssetDataNodeConfiguration(
            time_index_name=ASSET_DATA_NODE_TIME_INDEX_NAME,
            index_names=list(ASSET_DATA_NODE_INDEX_NAMES),
            records=records,
            node_metadata=DataNodeMetaData(
                identifier=identifier or cls._default_identifier(),
                description=description or cls._default_description(),
            ),
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

        by_name = {record.column_name: record for record in required_records}
        for record in extra_records:
            by_name.setdefault(record.column_name, record)
        return list(by_name.values())

    @classmethod
    def _default_identifier(cls) -> str:
        raise NotImplementedError

    @classmethod
    def _default_description(cls) -> str:
        raise NotImplementedError

    @classmethod
    def _required_records(cls) -> list[RecordDefinition]:
        raise NotImplementedError

    def set_frame(self, frame: pd.DataFrame) -> AssetTimestampedDataNode:
        self._asset_data_frame = frame
        return self

    def get_frame(self) -> pd.DataFrame:
        frame = getattr(self, "_asset_data_frame", None)
        if frame is None:
            return self.build_schema_bootstrap_frame(config=self.config)
        return frame

    def update(self) -> pd.DataFrame:
        return _validate_asset_data_frame(self.get_frame(), config=self.config)

    @classmethod
    def build_initialization_frame(
        cls,
        **kwargs: Any,
    ) -> pd.DataFrame:
        return cls.build_schema_bootstrap_frame(**kwargs)

    @classmethod
    def build_schema_bootstrap_frame(
        cls,
        *,
        config: AssetDataNodeConfiguration | None = None,
        unique_identifier: str = ASSET_DATA_NODE_BOOTSTRAP_UNIQUE_IDENTIFIER,
        time_index: dt.datetime | pd.Timestamp = ASSET_DATA_NODE_BOOTSTRAP_TIME_INDEX,
    ) -> pd.DataFrame:
        resolved_config = config or cls.default_config()
        row = {
            resolved_config.time_index_name: time_index,
            "unique_identifier": unique_identifier,
        }
        for record in resolved_config.records:
            if record.column_name not in row:
                row[record.column_name] = _schema_bootstrap_value(record.dtype)
        frame = pd.DataFrame([row])
        return _validate_asset_data_frame(frame, config=resolved_config)

    @classmethod
    def build_mock_frame(cls, **kwargs: Any) -> pd.DataFrame:
        return cls.build_schema_bootstrap_frame(**kwargs)


class AssetSnapshot(AssetTimestampedDataNode):
    """Timestamped asset display snapshots keyed by asset unique_identifier."""

    @classmethod
    def _default_identifier(cls) -> str:
        return "mainsequence.markets.asset_snapshots"

    @classmethod
    def _default_description(cls) -> str:
        return (
            "Timestamped asset display snapshots keyed by time_index and "
            "unique_identifier."
        )

    @classmethod
    def _required_records(cls) -> list[RecordDefinition]:
        return _record_definitions_from_dtype_map(
            ASSET_SNAPSHOT_COLUMN_DTYPES_MAP,
            labels=ASSET_SNAPSHOT_COLUMN_LABELS,
            descriptions=ASSET_SNAPSHOT_COLUMN_DESCRIPTIONS,
        )


class AssetPricingDetail(AssetTimestampedDataNode):
    """Timestamped provider pricing metadata keyed by asset unique_identifier."""

    @classmethod
    def _default_identifier(cls) -> str:
        return "mainsequence.markets.asset_pricing_details"

    @classmethod
    def _default_description(cls) -> str:
        return (
            "Timestamped asset pricing metadata keyed by time_index and "
            "unique_identifier."
        )

    @classmethod
    def _required_records(cls) -> list[RecordDefinition]:
        return _record_definitions_from_dtype_map(
            ASSET_PRICING_DETAIL_COLUMN_DTYPES_MAP,
            labels=ASSET_PRICING_DETAIL_COLUMN_LABELS,
            descriptions=ASSET_PRICING_DETAIL_COLUMN_DESCRIPTIONS,
        )


def _record_definitions_from_dtype_map(
    dtype_map: dict[str, str],
    *,
    labels: dict[str, str] | None = None,
    descriptions: dict[str, str] | None = None,
) -> list[RecordDefinition]:
    return [
        RecordDefinition(
            column_name=column_name,
            dtype=dtype,
            label=(labels or {}).get(column_name),
            description=(descriptions or {}).get(column_name),
        )
        for column_name, dtype in dtype_map.items()
    ]


def _validate_asset_data_frame(
    frame: pd.DataFrame,
    *,
    config: AssetDataNodeConfiguration,
) -> pd.DataFrame:
    if not isinstance(config, AssetDataNodeConfiguration):
        raise TypeError("Asset DataNodes require AssetDataNodeConfiguration.")

    normalized = _reset_frame_index(frame.copy(), index_names=config.index_names)
    required_columns = {record.column_name for record in config.records}
    missing = sorted(required_columns.difference(normalized.columns))
    if missing:
        raise ValueError(f"Asset DataNode frame is missing columns: {missing!r}.")

    normalized[config.time_index_name] = pd.to_datetime(
        normalized[config.time_index_name],
        utc=True,
    )
    normalized["unique_identifier"] = normalized["unique_identifier"].astype("string")
    normalized = normalized[[record.column_name for record in config.records]]
    normalized = normalized.set_index(config.index_names)

    if normalized.index.has_duplicates:
        raise ValueError(
            "Asset DataNode frame contains duplicate rows for "
            f"{config.index_names!r}."
        )
    return normalized.sort_index()


def _reset_frame_index(
    frame: pd.DataFrame,
    *,
    index_names: list[str],
) -> pd.DataFrame:
    missing_index_names = [
        index_name
        for index_name in index_names
        if index_name not in frame.columns
        and index_name not in (frame.index.names or [])
    ]
    if missing_index_names:
        raise ValueError(
            f"Asset DataNode frame is missing index columns: {missing_index_names!r}."
        )
    has_required_index = any(name in index_names for name in frame.index.names)
    return frame.reset_index() if has_required_index else frame


def _schema_bootstrap_value(dtype: str) -> Any:
    if dtype == "datetime64[ns, UTC]":
        return ASSET_DATA_NODE_BOOTSTRAP_TIME_INDEX
    if dtype in {"jsonb", "json"}:
        return {"_mainsequence_reserved": "schema_bootstrap", "semantic": False}
    if dtype in {"float64", "decimal"}:
        return "0"
    if dtype in {"int64", "Int64"}:
        return 0
    if dtype == "bool":
        return False
    return ""


__all__ = [
    "ASSET_DATA_NODE_BOOTSTRAP_TIME_INDEX",
    "ASSET_DATA_NODE_BOOTSTRAP_UNIQUE_IDENTIFIER",
    "ASSET_DATA_NODE_INDEX_NAMES",
    "ASSET_DATA_NODE_TIME_INDEX_NAME",
    "ASSET_PRICING_DETAIL_COLUMN_DESCRIPTIONS",
    "ASSET_PRICING_DETAIL_COLUMN_DTYPES_MAP",
    "ASSET_PRICING_DETAIL_COLUMN_LABELS",
    "ASSET_SNAPSHOT_COLUMN_DESCRIPTIONS",
    "ASSET_SNAPSHOT_COLUMN_DTYPES_MAP",
    "ASSET_SNAPSHOT_COLUMN_LABELS",
    "AssetDataNodeConfiguration",
    "AssetPricingDetail",
    "AssetSnapshot",
    "AssetTimestampedDataNode",
]
