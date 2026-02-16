from __future__ import annotations

from functools import lru_cache
from typing import Annotated, Any

from pydantic import (
    BaseModel,
    ConfigDict,
    WithJsonSchema,
    field_serializer,
)

import mainsequence.client as msc
from mainsequence.client import Asset
from mainsequence.virtualfundbuilder.enums import PriceTypeNames
from mainsequence.virtualfundbuilder.resource_factory.rebalance_factory import RebalanceStrategyBase
from mainsequence.virtualfundbuilder.resource_factory.signal_factory import WeightsBase
from mainsequence.virtualfundbuilder.utils import get_vfb_logger

logger = get_vfb_logger()


class VFBConfigBaseModel(BaseModel):
    class Config:
        arbitrary_types_allowed = True


class MarketsTimeSeries(VFBConfigBaseModel):
    """
    MarketsTimeSeries based on their unique id. Used as the data sources for the prices.
    Values include alpaca_1d_bars, binance_1d_bars etc.

    Attributes:
        unique_identifier (str): Identfier of the MarketsTimeSeries.
    """

    unique_identifier: str = "alpaca_1d_bars"


class PricesConfiguration(VFBConfigBaseModel):
    """
    Configuration for price data handling in a portfolio.

    Attributes:
        bar_frequency_id (str): The frequency of price bars.
        upsample_frequency_id (str): Frequency to upsample intraday data to.
        intraday_bar_interpolation_rule (str): Rule for interpolating missing intraday bars.
        is_live (bool): Boolean flag indicating if the price feed is live.
        translation_table_unique_id (str): The unique identifier of the translation table used to identify the price source.
    """

    bar_frequency_id: str = "1d"
    upsample_frequency_id: str = "1d"  # "15m"
    intraday_bar_interpolation_rule: str = "ffill"
    is_live: bool = False
    translation_table_unique_id: str = "prices_translation_table_1d"
    forward_fill_to_now: bool = False


@lru_cache(maxsize=1028)  # Cache up to 1028 different combinations
def cached_asset_filter(*args, **kwargs):
    tmp_assets = Asset.filter_with_asset_class(*args, **kwargs)
    return tmp_assets


class AssetsConfiguration(VFBConfigBaseModel):
    """
    Configuration for assets included in a portfolio.

    Attributes:
        assets_category_unique_id (str):
            Unique Identifier of assets category
        price_type (PriceTypeNames): Type of price used for backtesting.
        prices_configuration (PricesConfiguration): Configuration for price data handling.
    """

    assets_category_unique_id: str | None=None
    price_type: PriceTypeNames = PriceTypeNames.CLOSE
    prices_configuration: PricesConfiguration

    def get_asset_list(self):
        asset_category = msc.AssetCategory.get(unique_identifier=self.assets_category_unique_id)
        assets = msc.Asset.filter(id__in=asset_category.assets)
        return assets


class BacktestingWeightsConfig(VFBConfigBaseModel):
    """
    Configuration for backtesting weights.

    Attributes:
        rebalance_strategy_name (str): Strategy used for rebalancing.
        rebalance_strategy_configuration (Dict): Placeholder dict for the rebalance strategy configuration.
        signal_weights_name (str): Type of signal weights strategy.
        signal_weights_configuration (Dict): Placeholder dict for the signal weights configuration.
    Private attrs:
        _rebalance_strategy_instance (RebalanceStrategyBase | None)
        _signal_weights_instance (WeightsBase | None)
    """

    model_config = ConfigDict(
        extra="forbid",  # reject unknown fields
        populate_by_name=True,
    )


    rebalance_strategy_instance: RebalanceStrategyBase
    signal_weights_instance: Annotated[
        WeightsBase,#its also a DataNode so we serialize with its exact configuration json schema
        WithJsonSchema({"type": "object"})
    ]

    @field_serializer(
        "signal_weights_instance",
        when_used="json",
        return_type=dict[str, Any],  # <- IMPORTANT for schema; replace with your real output type
    )
    def ser_signal_weights(self, v: WeightsBase) -> dict[str, Any]:
        return v.build_configuration_json_schema








class PortfolioExecutionConfiguration(VFBConfigBaseModel):
    """
    Configuration for portfolio execution.

    Attributes:
        commission_fee (float): Commission fee percentage.
    """

    commission_fee: float = 0.00018


class FrontEndDetails(VFBConfigBaseModel):
    description: str  # required field; must be provided and cannot be None

    signal_name: str | None = None
    signal_description: str | None = None
    rebalance_strategy_name: str | None = None
    rebalance_strategy_description: str | None = None


class PortfolioMarketsConfig(VFBConfigBaseModel):
    """
    Configuration for Virtual Asset Management (VAM) portfolio.

    Attributes:
        portfolio_name (str): Name of the portfolio.
        execution_configuration (VAMExecutionConfiguration): Execution configuration for VAM.
    """

    portfolio_name: str = "Portfolio Strategy Title"
    front_end_details: FrontEndDetails | None  = None





class PortfolioBuildConfiguration(VFBConfigBaseModel):
    """
    Main class for configuring and building a portfolio.

    This class defines the configuration parameters needed for
    building a portfolio, including asset configurations, backtesting
    weights, and execution parameters.

    Attributes:
        assets_configuration (AssetsConfiguration): Configuration details for assets.
        portfolio_prices_frequency (str): Frequency to upsample portoflio. Optional.
        backtesting_weights_configuration (BacktestingWeightsConfig): Weights configuration used for backtesting.
        execution_configuration (PortfolioExecutionConfiguration): Execution settings for the portfolio.
    """

    assets_configuration: AssetsConfiguration
    portfolio_prices_frequency: str | None = "1d"

    execution_configuration: PortfolioExecutionConfiguration

    backtesting_weights_configuration: BacktestingWeightsConfig

    def model_dump(self, **kwargs):
        serialized_asset_config = self.assets_configuration.model_dump(**kwargs)
        data = super().model_dump(**kwargs)
        data["assets_configuration"] = serialized_asset_config

        data["backtesting_weights_configuration"] = (
            self.backtesting_weights_configuration.model_dump(**kwargs)
        )
        return data




class PortfolioConfiguration(VFBConfigBaseModel):
    """
        Configuration for a complete portfolio, including build configuration,
        TDAG updates, and VAM settings.

        This class aggregates different configurations required for the
        management and operation of a portfolio.

    Attributes:
        portfolio_build_configuration (PortfolioBuildConfiguration): Configuration for building the portfolio.
        portfolio_markets_configuration (PortfolioMarketsConfig): VAM execution configuration.
    """

    portfolio_build_configuration: PortfolioBuildConfiguration
    portfolio_markets_configuration: PortfolioMarketsConfig



