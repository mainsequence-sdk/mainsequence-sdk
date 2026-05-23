from __future__ import annotations

import datetime as dt

PORTFOLIO_CANONICAL_TIME_INDEX_NAME = "time_index"
PORTFOLIO_INDEX_ASSET_UNIQUE_IDENTIFIER = "portfolio_index_asset_unique_identifier"
ASSET_UNIQUE_IDENTIFIER = "unique_identifier"
PORTFOLIO_METADATA_UNIQUE_IDENTIFIER = "unique_identifier"
PORTFOLIO_DESCRIPTION = "description"
SIGNAL_UID = "signal_uid"
SIGNAL_DESCRIPTION = "signal_description"
REBALANCE_STRATEGY_UID = "rebalance_strategy_uid"
REBALANCE_STRATEGY_DESCRIPTION = "rebalance_strategy_description"

PORTFOLIO_WEIGHTS_INDEX_NAMES = [
    PORTFOLIO_CANONICAL_TIME_INDEX_NAME,
    PORTFOLIO_INDEX_ASSET_UNIQUE_IDENTIFIER,
    ASSET_UNIQUE_IDENTIFIER,
]
PORTFOLIO_WEIGHTS_COLUMN_DTYPES_MAP = {
    "time_index": "datetime64[ns, UTC]",
    "portfolio_index_asset_unique_identifier": "string",
    "unique_identifier": "string",
    "weight": "float64",
    "weight_before": "float64",
    "price_current": "float64",
    "price_before": "float64",
    "volume_current": "float64",
    "volume_before": "float64",
}
PORTFOLIO_WEIGHTS_COLUMN_LABELS = {
    "time_index": "Time Index",
    "portfolio_index_asset_unique_identifier": "Portfolio Index Asset Unique Identifier",
    "unique_identifier": "Unique Identifier",
    "weight": "Weight",
    "weight_before": "Weight Before",
    "price_current": "Price Current",
    "price_before": "Price Before",
    "volume_current": "Volume Current",
    "volume_before": "Volume Before",
}
PORTFOLIO_WEIGHTS_COLUMN_DESCRIPTIONS = {
    "time_index": "UTC timestamp for the executed portfolio weight row.",
    "portfolio_index_asset_unique_identifier": (
        "Stable PortfolioIndexAsset unique identifier for the portfolio that "
        "owns this executed weight row."
    ),
    "unique_identifier": "Asset unique identifier for the weighted instrument.",
    "weight": "Executed/current allocation weight for this asset.",
    "weight_before": "Allocation weight before the rebalance execution.",
    "price_current": "Asset price used for the current rebalance calculation.",
    "price_before": "Asset price from the previous rebalance reference.",
    "volume_current": "Asset volume used for the current rebalance calculation.",
    "volume_before": "Asset volume from the previous rebalance reference.",
}

SIGNAL_WEIGHTS_INDEX_NAMES = [
    PORTFOLIO_CANONICAL_TIME_INDEX_NAME,
    SIGNAL_UID,
    ASSET_UNIQUE_IDENTIFIER,
]
SIGNAL_WEIGHTS_COLUMN_DTYPES_MAP = {
    "time_index": "datetime64[ns, UTC]",
    "signal_uid": "string",
    "unique_identifier": "string",
    "signal_weight": "float64",
}
SIGNAL_WEIGHTS_COLUMN_LABELS = {
    "time_index": "Time Index",
    "signal_uid": "Signal UID",
    "unique_identifier": "Unique Identifier",
    "signal_weight": "Signal Weight",
}
SIGNAL_WEIGHTS_COLUMN_DESCRIPTIONS = {
    "time_index": "UTC timestamp for the signal weight row.",
    "signal_uid": (
        "Deterministic hash of the canonical signal configuration that produced "
        "this signal weight row."
    ),
    "unique_identifier": "Asset unique identifier for the signaled instrument.",
    "signal_weight": "Raw signal allocation weight before portfolio execution.",
}

PORTFOLIOS_INDEX_NAMES = [
    PORTFOLIO_CANONICAL_TIME_INDEX_NAME,
    ASSET_UNIQUE_IDENTIFIER,
]
PORTFOLIOS_COLUMN_DTYPES_MAP = {
    "time_index": "datetime64[ns, UTC]",
    "unique_identifier": "string",
    "close": "float64",
    "return": "float64",
    "calculated_close": "float64",
    "close_time": "datetime64[ns, UTC]",
}
PORTFOLIOS_COLUMN_LABELS = {
    "time_index": "Time Index",
    "unique_identifier": "Unique Identifier",
    "close": "Close",
    "return": "Return",
    "calculated_close": "Calculated Close",
    "close_time": "Close Time",
}
PORTFOLIOS_COLUMN_DESCRIPTIONS = {
    "time_index": "UTC timestamp for the portfolio value row.",
    "unique_identifier": (
        "Stable asset unique identifier for the portfolio value series."
    ),
    "close": "Published portfolio close value.",
    "return": "Portfolio period return.",
    "calculated_close": "Internally calculated close before any price override.",
    "close_time": "UTC close timestamp represented by this portfolio value row.",
}

SCHEMA_BOOTSTRAP_TIME_INDEX = dt.datetime(1970, 1, 1, tzinfo=dt.UTC)
SCHEMA_BOOTSTRAP_PORTFOLIO_IDENTIFIER = "__schema_bootstrap_portfolio__"
SCHEMA_BOOTSTRAP_ASSET_IDENTIFIER = "__schema_bootstrap_asset__"
SCHEMA_BOOTSTRAP_SIGNAL_UID = "__schema_bootstrap_signal__"
SIGNAL_UID_EXCLUDED_CONFIGURATION_KEYS = frozenset(
    {
        "hash_namespace",
        "namespace",
        "storage_hash",
        "update_hash",
        "storage_id",
        "storage_uid",
        "update_id",
        "update_uid",
        "data_node_storage_id",
        "data_node_storage_uid",
        "data_node_update_id",
        "data_node_update_uid",
        "data_source_id",
        "portfolio_consumers",
        "portfolio_id",
        "portfolio_uid",
        "portfolio_index_asset_unique_identifier",
        "display_name",
        "display_label",
        "signal_name",
        "signal_description",
        "run_id",
        "run_timestamp",
        "created_at",
        "updated_at",
        "creation_date",
    }
)
REBALANCE_STRATEGY_UID_EXCLUDED_CONFIGURATION_KEYS = frozenset(
    {
        "hash_namespace",
        "namespace",
        "storage_hash",
        "update_hash",
        "storage_id",
        "storage_uid",
        "update_id",
        "update_uid",
        "data_node_storage_id",
        "data_node_storage_uid",
        "data_node_update_id",
        "data_node_update_uid",
        "data_source_id",
        "portfolio_consumers",
        "portfolio_id",
        "portfolio_uid",
        "portfolio_index_asset_unique_identifier",
        "display_name",
        "display_label",
        "rebalance_strategy_name",
        "rebalance_strategy_description",
        "description",
        "run_id",
        "run_timestamp",
        "created_at",
        "updated_at",
        "creation_date",
    }
)
PORTFOLIO_CONFIGURATION_HASH_EXCLUDED_KEYS = frozenset(
    {
        "hash_namespace",
        "namespace",
        "storage_hash",
        "update_hash",
        "storage_id",
        "storage_uid",
        "update_id",
        "update_uid",
        "data_node_storage_id",
        "data_node_storage_uid",
        "data_node_update_id",
        "data_node_update_uid",
        "data_source_id",
        "run_id",
        "run_timestamp",
        "created_at",
        "updated_at",
        "creation_date",
    }
)
PORTFOLIO_WEIGHT_SOURCE_COLUMN_ALIASES = {
    "weights_current": "weight",
    "weights_before": "weight_before",
}
