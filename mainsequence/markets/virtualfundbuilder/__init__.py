__version__ = "0.1.0"

import os
import sys
from pathlib import Path

from .utils import get_vfb_logger

logger = get_vfb_logger()

from mainsequence.markets.virtualfundbuilder.data_nodes import (
    PORTFOLIO_WEIGHTS_COLUMN_DTYPES_MAP,
    PORTFOLIO_WEIGHTS_INDEX_NAMES,
    PORTFOLIOS_COLUMN_DTYPES_MAP,
    PORTFOLIOS_INDEX_NAMES,
    SIGNAL_UID_EXCLUDED_CONFIGURATION_KEYS,
    SIGNAL_WEIGHTS_COLUMN_DTYPES_MAP,
    SIGNAL_WEIGHTS_INDEX_NAMES,
    PortfoliosDataNode,
    PortfolioWeights,
    SignalMetadata,
    SignalsMetadataConfiguration,
    SignalsMetadataUpdater,
    SignalWeights,
    SignalWeightsConfiguration,
    VFBCanonicalDataNode,
    VFBCanonicalDataNodeConfiguration,
    build_signal_metadata,
    canonical_portfolio_configuration,
    canonical_signal_configuration,
    compute_portfolio_configuration_hash,
    compute_signal_uid,
    default_signal_metadata_updater,
    get_or_create_portfolio_index_asset,
    get_signal_metadata,
    normalize_portfolio_values_frame,
    normalize_portfolio_weights_frame,
    normalize_signal_weights_frame,
    upsert_signal_metadata,
)
from mainsequence.markets.virtualfundbuilder.utils import (
    GECKO_SYMBOL_MAPPING,
    TIMEDELTA,
    build_rolling_regression_from_df,
    convert_to_binance_frequency,
    get_last_query_times_per_asset,
    reindex_df,
    runs_in_main_process,
)


def register_default_strategies():
    # Keep this in a function to not clutter the libs namespace
    import mainsequence.markets.virtualfundbuilder.contrib.rebalance_strategies
    import mainsequence.markets.virtualfundbuilder.contrib.signals


RUNS_IN_JOB = os.getenv("JOB_ID", None)
if RUNS_IN_JOB:
    # register_default_strategies() #
    pass

if runs_in_main_process():
    pass
