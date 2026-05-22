from __future__ import annotations

import pandas as pd

from mainsequence.markets.virtualfundbuilder.contrib.rebalance_strategies.rebalance_strategies import (
    ImmediateSignal,
)
from mainsequence.markets.virtualfundbuilder.contrib.signals.market_cap import (
    AUIDWeight,
    FixedWeights,
    FixedWeightsConfig,
)
from mainsequence.markets.virtualfundbuilder.data_nodes import (
    PortfoliosDataNode,
    PortfolioWeights,
    compute_portfolio_configuration_hash,
    normalize_signal_weights_frame,
)
from mainsequence.markets.virtualfundbuilder.enums import PriceTypeNames
from mainsequence.markets.virtualfundbuilder.models import (
    AssetsConfiguration,
    BacktestingWeightsConfig,
    PortfolioBuildConfiguration,
    PortfolioConfiguration,
    PortfolioExecutionConfiguration,
    PortfolioMarketsConfig,
    PricesConfiguration,
)

NAMESPACE = "example.equal_weights"
PORTFOLIO_INDEX_ASSET_UNIQUE_IDENTIFIER = "portfolio_index_asset:equal_weights_demo"
ASSET_UNIQUE_IDENTIFIERS = [
    "BINANCE_SPOT_BTC_USDT",
    "BINANCE_SPOT_ETH_USDT",
    "BINANCE_SPOT_SOL_USDT",
]


def build_assets_configuration() -> AssetsConfiguration:
    return AssetsConfiguration(
        assets_category_unique_id=None,
        price_type=PriceTypeNames.CLOSE,
        prices_configuration=PricesConfiguration(
            bar_frequency_id="1d",
            upsample_frequency_id="1d",
            intraday_bar_interpolation_rule="ffill",
            translation_table_unique_id="prices_translation_table_1d",
        ),
    )


def build_equal_weights_signal() -> FixedWeights:
    weight = 1.0 / len(ASSET_UNIQUE_IDENTIFIERS)
    return FixedWeights.from_signal_configuration(
        FixedWeightsConfig(
            signal_assets_configuration=build_assets_configuration(),
            asset_unique_identifier_weights=[
                AUIDWeight(unique_identifier=asset_uid, weight=weight)
                for asset_uid in ASSET_UNIQUE_IDENTIFIERS
            ],
        ),
        namespace=NAMESPACE,
    )


def build_portfolio_configuration(signal_weights: FixedWeights) -> PortfolioConfiguration:
    assets_configuration = build_assets_configuration()
    return PortfolioConfiguration(
        portfolio_build_configuration=PortfolioBuildConfiguration(
            assets_configuration=assets_configuration,
            portfolio_prices_frequency="1d",
            execution_configuration=PortfolioExecutionConfiguration(commission_fee=0.00018),
            backtesting_weights_configuration=BacktestingWeightsConfig(
                signal_weights_instance=signal_weights,
                rebalance_strategy_instance=ImmediateSignal(calendar_key="24/7"),
            ),
        ),
        portfolio_markets_configuration=PortfolioMarketsConfig(
            portfolio_name="Example Equal Weights Portfolio",
        ),
    )


def build_example_signal_weights_frame(signal_weights: FixedWeights) -> pd.DataFrame:
    time_index = pd.Timestamp("2024-01-01", tz="UTC")
    weight = 1.0 / len(ASSET_UNIQUE_IDENTIFIERS)
    raw_signal_weights = pd.DataFrame(
        {
            "time_index": [time_index] * len(ASSET_UNIQUE_IDENTIFIERS),
            "unique_identifier": ASSET_UNIQUE_IDENTIFIERS,
            "signal_weight": [weight] * len(ASSET_UNIQUE_IDENTIFIERS),
        }
    ).set_index(["time_index", "unique_identifier"])
    return normalize_signal_weights_frame(
        raw_signal_weights,
        signal_uid=signal_weights.signal_uid,
    )


def build_example_portfolio_weights_frame() -> pd.DataFrame:
    time_index = pd.Timestamp("2024-01-01", tz="UTC")
    weight = 1.0 / len(ASSET_UNIQUE_IDENTIFIERS)
    raw_portfolio_weights = pd.DataFrame(
        {
            "time_index": [time_index] * len(ASSET_UNIQUE_IDENTIFIERS),
            "unique_identifier": ASSET_UNIQUE_IDENTIFIERS,
            "weight": [weight] * len(ASSET_UNIQUE_IDENTIFIERS),
            "weight_before": [0.0] * len(ASSET_UNIQUE_IDENTIFIERS),
            "price_current": [100.0, 50.0, 25.0],
            "price_before": [100.0, 50.0, 25.0],
            "volume_current": [1.0, 1.0, 1.0],
            "volume_before": [0.0, 0.0, 0.0],
        }
    ).set_index(["time_index", "unique_identifier"])
    return PortfolioWeights.normalize_weights_frame(
        raw_portfolio_weights,
        portfolio_index_asset_unique_identifier=(PORTFOLIO_INDEX_ASSET_UNIQUE_IDENTIFIER),
    )


def build_example_portfolio_values_frame() -> pd.DataFrame:
    time_index = pd.Timestamp("2024-01-01", tz="UTC")
    raw_portfolio_values = pd.DataFrame(
        {
            "time_index": [time_index],
            "close": [100.0],
            "return": [0.0],
            "calculated_close": [100.0],
            "close_time": [time_index],
        }
    ).set_index("time_index")
    return PortfoliosDataNode.normalize_values_frame(
        raw_portfolio_values,
        portfolio_index_asset_unique_identifier=(PORTFOLIO_INDEX_ASSET_UNIQUE_IDENTIFIER),
    )


def main() -> None:
    signal_weights = build_equal_weights_signal()
    portfolio_configuration = build_portfolio_configuration(signal_weights)
    portfolio_configuration_hash = compute_portfolio_configuration_hash(portfolio_configuration)

    print(f"signal_uid={signal_weights.signal_uid}")
    print(f"portfolio_configuration_hash={portfolio_configuration_hash}")
    print(build_example_signal_weights_frame(signal_weights))
    print(build_example_portfolio_weights_frame())
    print(build_example_portfolio_values_frame())


if __name__ == "__main__":
    main()
