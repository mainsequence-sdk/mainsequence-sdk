from __future__ import annotations

from functools import lru_cache
from typing import Annotated, Any

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
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
    """
    Base class for VFB configuration models.

    Notes
    -----
    VFB configurations often carry non-Pydantic objects (e.g., strategy instances),
    so `arbitrary_types_allowed=True` is required.
    """
    model_config = ConfigDict(arbitrary_types_allowed=True)


class MarketsTimeSeries(VFBConfigBaseModel):
    """
    Reference to a MarketsTimeSeries by unique identifier.

    This is used to point price nodes to a specific upstream time series
    (e.g., "alpaca_1d_bars", "binance_1d_bars") depending on your backend setup.

    Attributes:
        unique_identifier: Identifier of the MarketsTimeSeries.
    """

    unique_identifier: str = Field(
        default="alpaca_1d_bars",
        description=(
            "Unique identifier of the MarketsTimeSeries to use as a price source "
            "(e.g., 'alpaca_1d_bars', 'binance_1d_bars')."
        ),
        examples=["alpaca_1d_bars", "binance_1d_bars"],
    )


class PricesConfiguration(VFBConfigBaseModel):
    """
    Configuration for price handling and interpolation/upsampling.

    This config controls *how VFB fetches and shapes prices*.
    It is distinct from signal-weight validity, which is controlled by
    `WeightsBase.maximum_forward_fill()`.

    Attributes:
        bar_frequency_id: Source bars frequency (e.g., "1d", "5m").
        upsample_frequency_id: Target frequency after interpolation (e.g., "1d", "15m").
        intraday_bar_interpolation_rule: Interpolation rule for gaps ("ffill" or "None").
        is_live: Whether the feed should be treated as live (may be ignored by some nodes).
        translation_table_unique_id: AssetTranslationTable unique id used by WrapperDataNode.
        forward_fill_to_now: If True, extend interpolated prices up to `datetime.now(UTC)` by forward-fill.
    """

    bar_frequency_id: str = Field(
        default="1d",
        description=(
            "Frequency identifier for the *raw* bars coming from the upstream price source. "
            "Examples: '1d', '5m', '15m'. Must contain 'd' (daily) or 'm' (minutes) for current implementations."
        ),
        examples=["1d", "5m", "15m"],
    )

    upsample_frequency_id: str = Field(
        default="1d",
        description=(
            "Frequency identifier for the *interpolated/upsampled* series produced by VFB. "
            "For daily portfolios, this is typically '1d'. For intraday portfolios, e.g. '15m'."
        ),
        examples=["1d", "15m", "30m"],
    )

    intraday_bar_interpolation_rule: str = Field(
        default="ffill",
        description=(
            "Interpolation rule used to fill gaps in intraday (minute-level) bars. "
            "Supported values in VFB code paths are typically 'ffill' or 'None'."
        ),
        examples=["ffill", "None"],
    )

    is_live: bool = Field(
        default=False,
        description=(
            "Marks whether this prices configuration is intended for live/streaming usage. "
            "Some helpers may ignore this flag; it is primarily descriptive/config-level metadata."
        ),
        examples=[False, True],
    )

    translation_table_unique_id: str = Field(
        default="prices_translation_table_1d",
        description=(
            "Unique identifier of the AssetTranslationTable used to map assets into the upstream price source. "
            "This is used by WrapperDataNode when `source_bars_data_node` is not provided."
        ),
        examples=["prices_translation_table_1d", "prices_translation_table_15m"],
    )

    forward_fill_to_now: bool = Field(
        default=False,
        description=(
            "If True, portfolio price interpolation extends the time index up to 'now' (UTC) "
            "and forward-fills the last known price. Useful for live dashboards. "
            "Does NOT make signal weights valid beyond their maximum_forward_fill."
        ),
        examples=[False, True],
    )


@lru_cache(maxsize=1028)
def cached_asset_filter(*args, **kwargs):
    """
    Cached wrapper around `Asset.filter_with_asset_class(...)`.

    This exists to reduce repeated backend calls when the same asset-universe query
    is executed frequently (e.g., across multiple nodes).
    """
    return Asset.filter_with_asset_class(*args, **kwargs)


class AssetsConfiguration(VFBConfigBaseModel):
    """
    Configuration for the asset universe and price field selection.

    Attributes:
        assets_category_unique_id:
            Optional AssetCategory unique id. If provided, VFB will use that category
            as the universe for both prices and portfolio construction.
            If None, the signal node is expected to define its own universe via get_asset_list().
        price_type:
            Which price field to use for portfolio return computation (OPEN/CLOSE/VWAP).
        prices_configuration:
            Pricing configuration used to fetch and interpolate/upsample bars.
    """

    assets_category_unique_id: str | None = Field(
        default=None,
        description=(
            "AssetCategory unique identifier used as the portfolio/signal universe. "
            "If None, the signal strategy is expected to provide assets via `get_asset_list()`."
        ),
        examples=["crypto", "etfs", None],
    )

    price_type: PriceTypeNames = Field(
        default=PriceTypeNames.CLOSE,
        description="Which price field should be used as the valuation/return price for the portfolio.",
        examples=["close", "open", "vwap"],
    )

    prices_configuration: PricesConfiguration = Field(
        ...,
        description=(
            "Price-source and interpolation configuration used by VFB when building/interpolating prices."
        ),
        examples=[
            {
                "bar_frequency_id": "1d",
                "upsample_frequency_id": "1d",
                "intraday_bar_interpolation_rule": "ffill",
                "translation_table_unique_id": "prices_translation_table_1d",
                "forward_fill_to_now": False,
            }
        ],
    )

    def get_asset_list(self):
        """
        Convenience helper: resolve the assets from the configured AssetCategory.

        Note: only valid when `assets_category_unique_id` is provided.
        """
        if not self.assets_category_unique_id:
            raise ValueError(
                "assets_category_unique_id is None. Either set it, or let the signal strategy define the universe."
            )
        asset_category = msc.AssetCategory.get(unique_identifier=self.assets_category_unique_id)
        return msc.Asset.filter(id__in=asset_category.assets)


class BacktestingWeightsConfig(VFBConfigBaseModel):
    """
    Configuration container holding instantiated strategy objects.

    IMPORTANT (VFB design)
    ----------------------
    VFB uses **direct injection** (instances), not string lookups:
    - `signal_weights_instance` is a WeightsBase strategy (usually also a TDAG DataNode)
    - `rebalance_strategy_instance` is a RebalanceStrategyBase strategy (pure pydantic model)

    Attributes:
        rebalance_strategy_instance:
            Rebalance strategy instance controlling how/when weights become executed weights.
        signal_weights_instance:
            Signal strategy instance producing `signal_weight` per asset and time.
            This is typically also a TDAG DataNode and is serialized using its own configuration schema.
    """

    model_config = ConfigDict(
        extra="forbid",          # reject unknown fields
        populate_by_name=True,
        arbitrary_types_allowed=True,
    )

    rebalance_strategy_instance: RebalanceStrategyBase = Field(
        ...,
        description=(
            "Instance of the rebalance strategy (e.g., ImmediateSignal). "
            "Controls how signal weights are turned into executed/portfolio weights."
        ),
        examples=[{"calendar_key": "24/7"}],
    )

    signal_weights_instance: Annotated[
        WeightsBase,
        Field(
            description=(
                "Instance of a signal weights strategy. "
                "This object is typically also a TDAG DataNode and will be serialized "
                "via its own configuration JSON schema."
            ),
            examples=[
                # Example is intentionally schematic; actual fields depend on the strategy class.
                {"strategy": "MarketCap", "num_top_assets": 10, "source_frequency": "1d"}
            ],
        ),
        WithJsonSchema({"type": "object"}),
    ]

    @field_serializer(
        "signal_weights_instance",
        when_used="json",
        return_type=dict[str, Any],
    )
    def ser_signal_weights(self, v: WeightsBase) -> dict[str, Any]:
        """
        Serialize the signal node as its configuration JSON schema.

        Robust behavior:
        - works if `build_configuration_json_schema` is a property (dict)
        - works if it is a method returning dict
        """
        schema_or_callable = v.build_configuration_json_schema
        return schema_or_callable() if callable(schema_or_callable) else schema_or_callable


class PortfolioExecutionConfiguration(VFBConfigBaseModel):
    """
    Configuration for execution / fee model.

    Attributes:
        commission_fee:
            Commission/fee rate applied to turnover (abs(weights_current - weights_before)).
    """

    commission_fee: float = Field(
        default=0.00018,
        ge=0.0,
        description=(
            "Commission fee rate applied as turnover drag. "
            "PortfolioStrategy computes fees as sum(abs(w_current - w_before)) * commission_fee."
        ),
        examples=[0.00018, 0.001, 0.0],
    )


class FrontEndDetails(VFBConfigBaseModel):
    """
    Optional descriptive metadata intended for UI/front-end surfaces.

    Attributes:
        description: Required human-readable description of the portfolio.
        signal_name: Optional display name for the signal strategy.
        signal_description: Optional description of the signal strategy.
        rebalance_strategy_name: Optional display name for the rebalance strategy.
        rebalance_strategy_description: Optional description of the rebalance strategy.
    """

    description: str = Field(
        ...,
        description="Human-readable description of the portfolio shown in UI/front-end contexts.",
        examples=["Market-cap weighted crypto basket with daily rebalance."],
        min_length=1,
    )

    signal_name: str | None = Field(
        default=None,
        description="Optional display name for the signal strategy (overrides class name in UI).",
        examples=["MarketCap Top 10"],
    )

    signal_description: str | None = Field(
        default=None,
        description="Optional longer description of the signal strategy for UI.",
        examples=["Selects the top N assets by market cap and normalizes into weights."],
    )

    rebalance_strategy_name: str | None = Field(
        default=None,
        description="Optional display name for the rebalance strategy (overrides class name in UI).",
        examples=["Immediate Rebalance"],
    )

    rebalance_strategy_description: str | None = Field(
        default=None,
        description="Optional longer description of the rebalance strategy for UI.",
        examples=["Applies signal weights directly as executed weights on each bar."],
    )


class PortfolioMarketsConfig(VFBConfigBaseModel):
    """
    Portfolio metadata/config used when syncing the portfolio into the Markets/VAM backend layer.

    Attributes:
        portfolio_name: Name of the portfolio in the Markets backend.
        front_end_details: Optional bundle of front-end details.
    """

    portfolio_name: str = Field(
        default="Portfolio Strategy Title",
        description="Display name for the portfolio in the Markets/VAM backend.",
        examples=["Fixed Weights BTC/ETH", "Demo VFB Portfolio - Direct Config"],
        min_length=1,
    )

    front_end_details: FrontEndDetails | None = Field(
        default=None,
        description="Optional front-end metadata shown in UI (description + strategy details).",
        examples=[
            {
                "description": "Demo portfolio built with VFB",
                "signal_name": "MarketCap",
                "rebalance_strategy_name": "ImmediateSignal",
            }
        ],
    )


class PortfolioBuildConfiguration(VFBConfigBaseModel):
    """
    Full build configuration for a VFB portfolio.

    This section defines the *behavior* of the portfolio build pipeline:
    - which assets/prices are used
    - how signal weights are generated
    - how rebalancing is applied
    - what fee model to apply
    - what frequency the portfolio series is produced at

    Attributes:
        assets_configuration:
            Asset universe + price configuration used by the portfolio.
        portfolio_prices_frequency:
            Portfolio resampling/valuation frequency (e.g. "1d", "15m").
            Often matches `prices_configuration.upsample_frequency_id`.
        execution_configuration:
            Fee/execution model.
        backtesting_weights_configuration:
            The injected signal + rebalance strategies.
    """

    assets_configuration: AssetsConfiguration = Field(
        ...,
        description="Asset universe definition + price configuration used by the portfolio.",
        examples=[
            {
                "assets_category_unique_id": "crypto",
                "price_type": "close",
                "prices_configuration": {
                    "bar_frequency_id": "1d",
                    "upsample_frequency_id": "1d",
                    "intraday_bar_interpolation_rule": "ffill",
                    "translation_table_unique_id": "prices_translation_table_1d",
                    "forward_fill_to_now": False,
                },
            }
        ],
    )

    portfolio_prices_frequency: str | None = Field(
        default="1d",
        description=(
            "Portfolio output frequency used when resampling the final portfolio series "
            "(e.g. '1d', '15m'). If None, the portfolio may rely on internal defaults."
        ),
        examples=["1d", "15m", None],
    )

    execution_configuration: PortfolioExecutionConfiguration = Field(
        ...,
        description="Execution / fee configuration used by PortfolioStrategy.",
        examples=[{"commission_fee": 0.00018}],
    )

    backtesting_weights_configuration: BacktestingWeightsConfig = Field(
        ...,
        description="Injected signal and rebalance strategy instances used to build the portfolio.",
    )

    def model_dump(self, **kwargs):
        """
        Preserve your existing behavior: explicitly dump nested configs.

        (Pydantic already does this, but keeping your override maintains backward compatibility
        if downstream code expects this exact structure.)
        """
        serialized_asset_config = self.assets_configuration.model_dump(**kwargs)
        data = super().model_dump(**kwargs)
        data["assets_configuration"] = serialized_asset_config
        data["backtesting_weights_configuration"] = (
            self.backtesting_weights_configuration.model_dump(**kwargs)
        )
        return data


class PortfolioConfiguration(VFBConfigBaseModel):
    """
    Top-level configuration object for a VFB portfolio.

    Attributes:
        portfolio_build_configuration:
            Defines the portfolio build behavior (assets/prices/signal/rebalance/fees).
        portfolio_markets_configuration:
            Defines the portfolio metadata used for Markets backend sync.
    """

    portfolio_build_configuration: PortfolioBuildConfiguration = Field(
        ...,
        description="Defines how the portfolio is built (assets/prices/signal/rebalance/fees).",
    )

    portfolio_markets_configuration: PortfolioMarketsConfig = Field(
        ...,
        description="Defines portfolio metadata used when syncing to Markets/VAM backend.",
    )
