from __future__ import annotations

import copy
import json
import os
from functools import lru_cache
from typing import Any

import yaml
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PrivateAttr,
    field_validator,
    model_validator,
    root_validator,
)

import mainsequence.client as msc
from mainsequence.client import MARKETS_CONSTANTS, Asset
from mainsequence.tdag.utils import hash_dict, write_yaml
from mainsequence.virtualfundbuilder.enums import PriceTypeNames
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

    assets_category_unique_id: str
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
    rebalance_strategy_name: str = "ImmediateSignal"
    rebalance_strategy_configuration: dict[str, Any] = Field(default_factory=dict)

    signal_weights_name: str = "MarketCap"
    signal_weights_configuration: dict[str, Any] = Field(default_factory=dict)

    _rebalance_strategy_instance: "RebalanceStrategyBase" | None = PrivateAttr(default=None)
    _signal_weights_instance: "WeightsBase" | None = PrivateAttr(default=None)

    @field_validator("rebalance_strategy_name", "signal_weights_name")
    @classmethod
    def _non_empty_name(cls, v: str) -> str:
        v = (v or "").strip()
        if not v:
            raise ValueError("Names must be non-empty strings.")
        return v

    # --- Builders -----------------------------------------------------------

    @classmethod
    def build_from_rebalance_strategy_and_signal_node(
        cls, rebalance_strategy: "RebalanceStrategyBase", signal_weights_node: "WeightsBase"
    ):

        config = dict(
            rebalance_strategy_name=rebalance_strategy.__class__.__name__,
            rebalance_strategy_configuration=rebalance_strategy.model_dump(),
            signal_weights_name=signal_weights_node.__class__.__name__,
            signal_weights_configuration=signal_weights_node.build_configuration,
        )
        instance = cls(**config)
        instance._rebalance_strategy_instance = rebalance_strategy
        instance._signal_weights_instance = signal_weights_node

        return instance

    def model_dump(self, **kwargs):
        signal_weights_configuration = self.signal_weights_configuration
        data = super().model_dump(**kwargs)
        data["signal_weights_configuration"]["signal_assets_configuration"] = (
            signal_weights_configuration["signal_assets_configuration"].model_dump(**kwargs)
        )

        return data

    def get_signal_weights_instance(self):
        return self._signal_weights_instance
    def get_rebalancer_instance(self):
        return self._rebalance_strategy_instance

    @model_validator(mode="before")
    def parse_signal_weights_configuration(cls, values):
        if isinstance(
            values["signal_weights_configuration"]["signal_assets_configuration"],
            AssetsConfiguration,
        ):
            return values

        asset_configuration = copy.deepcopy(
            values["signal_weights_configuration"]["signal_assets_configuration"]
        )
        if "prices_configuration" not in asset_configuration:
            logger.info("No Price Configuration in Configuration - Use Default Price Configuration")
            asset_configuration["prices_configuration"] = PricesConfiguration()

        values["signal_weights_configuration"]["signal_assets_configuration"] = AssetsConfiguration(
            **asset_configuration
        )
        return values


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


class AssetMixinOverwrite(VFBConfigBaseModel):
    """
    The Asset for evaluating the portfolio.

    Attributes:
        unique_identifier (str): The unique_identifier of the asset.
    """

    unique_identifier: str = MARKETS_CONSTANTS.UNIQUE_IDENTIFIER_USD


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

    @root_validator(pre=True)
    def parse_assets_configuration(cls, values):

        if (
            not isinstance(values["assets_configuration"], AssetsConfiguration)
            and values["assets_configuration"] is not None
        ):
            values["assets_configuration"] = AssetsConfiguration(
                assets_category_unique_id=values["assets_configuration"][
                    "assets_category_unique_id"
                ],
                price_type=PriceTypeNames(values["assets_configuration"]["price_type"]),
                prices_configuration=PricesConfiguration(
                    **values["assets_configuration"]["prices_configuration"]
                ),
            )

        return values


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

    @staticmethod
    def read_portfolio_configuration_from_yaml(yaml_path: str):
        with open(yaml_path) as file:
            return yaml.safe_load(file)

    @staticmethod
    def parse_portfolio_configuration_from_yaml(yaml_path: str, auto_complete=False):
        from mainsequence.virtualfundbuilder.config_handling import configuration_sanitizer

        configuration = PortfolioConfiguration.read_portfolio_configuration_from_yaml(yaml_path)
        return configuration_sanitizer(configuration, auto_complete=auto_complete)

    @staticmethod
    def parse_portfolio_configurations(
        portfolio_build_configuration: dict,
        portfolio_markets_configuration: dict,
    ):
        # Parse the individual components
        backtesting_weights_configuration = BacktestingWeightsConfig(
            rebalance_strategy_name=portfolio_build_configuration[
                "backtesting_weights_configuration"
            ]["rebalance_strategy_name"],
            rebalance_strategy_configuration=portfolio_build_configuration[
                "backtesting_weights_configuration"
            ]["rebalance_strategy_configuration"],
            signal_weights_name=portfolio_build_configuration["backtesting_weights_configuration"][
                "signal_weights_name"
            ],
            signal_weights_configuration=portfolio_build_configuration[
                "backtesting_weights_configuration"
            ]["signal_weights_configuration"],
        )

        execution_configuration = PortfolioExecutionConfiguration(
            commission_fee=portfolio_build_configuration["execution_configuration"][
                "commission_fee"
            ]
        )

        portfolio_build_config = PortfolioBuildConfiguration(
            assets_configuration=portfolio_build_configuration["assets_configuration"],
            backtesting_weights_configuration=backtesting_weights_configuration,
            execution_configuration=execution_configuration,
            portfolio_prices_frequency=portfolio_build_configuration["portfolio_prices_frequency"],
        )

        portfolio_markets_configuration = PortfolioMarketsConfig(**portfolio_markets_configuration)

        # Combine everything into the final PortfolioConfiguration
        portfolio_config = PortfolioConfiguration(
            portfolio_build_configuration=portfolio_build_config,
            portfolio_markets_configuration=portfolio_markets_configuration,
        )

        return portfolio_config

    def build_yaml_configuration_file(self):
        signal_type = (
            self.portfolio_build_configuration.backtesting_weights_configuration.signal_weights_name
        )
        vfb_folder = os.path.join(os.path.expanduser("~"), "VirtualFundBuilder", "configurations")
        vfb_folder = os.path.join(vfb_folder, signal_type)
        if not os.path.exists(vfb_folder):
            os.makedirs(vfb_folder)

        config_hash = hash_dict(self.model_dump_json())
        config_file_name = f"{vfb_folder}/{config_hash}.yaml"

        write_yaml(dict_file=json.loads(self.model_dump_json()), path=config_file_name)
        return config_file_name
