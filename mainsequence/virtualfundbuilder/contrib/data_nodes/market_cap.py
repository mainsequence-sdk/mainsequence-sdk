import datetime
from datetime import timedelta
from typing import Union

import numpy as np
import pandas as pd
from pydantic import BaseModel

import mainsequence.client as msc
from mainsequence.client import (
    Asset,
    AssetCategory,
    AssetTranslationTable,
    DoesNotExist,
)
from mainsequence.tdag.data_nodes import APIDataNode, DataNode, WrapperDataNode
from mainsequence.virtualfundbuilder.models import VFBConfigBaseModel
from mainsequence.virtualfundbuilder.resource_factory.signal_factory import (
    WeightsBase,
    register_signal_class,
)
from mainsequence.virtualfundbuilder.utils import TIMEDELTA


class AUIDWeight(VFBConfigBaseModel):
    unique_identifier: str
    weight: float


@register_signal_class(register_in_agent=True)
class FixedWeights(WeightsBase, DataNode):

    def __init__(self, asset_unique_identifier_weights: list[AUIDWeight], *args, **kwargs):
        """
        Args:
            asset_symbol_weights (List[SymbolWeight]): List of SymbolWeights that map asset symbols to weights
        """
        super().__init__(*args, **kwargs)
        self.asset_unique_identifier_weights = asset_unique_identifier_weights

    def maximum_forward_fill(self):
        return timedelta(days=200 * 365)  # Always forward-fill to avoid filling the DB

    def get_explanation(self):
        info = f"<p>{self.__class__.__name__}: Signal uses fixed weights with the following weights:</p>"
        return info


    def get_asset_list(self) -> None | list:
        asset_list = msc.Asset.filter(
            unique_identifier__in=[
                w.unique_identifier for w in self.asset_unique_identifier_weights
            ]
        )
        return asset_list



    def dependencies(self) -> dict[str, Union["DataNode", "APIDataNode"]]:
        return {}

    def update(self) -> pd.DataFrame:



        if not self.get_df_between_dates().empty:
            return pd.DataFrame()  # No need to store more than one constant weight

        df = pd.DataFrame([m.model_dump() for m in self.asset_unique_identifier_weights]).rename(
            columns={"weight": "signal_weight"}
        )
        df = df.set_index(["unique_identifier"])
        #offset 1 day to avoid last filter
        signals_weights = pd.concat([df], axis=0, keys=[self.OFFSET_START+datetime.timedelta(days=1)]).rename_axis(
            ["time_index", "unique_identifier"]
        )

        signals_weights = signals_weights.dropna()
        return signals_weights


class AssetMistMatch(Exception): ...


class VolatilityControlConfiguration(BaseModel):
    target_volatility: float = 0.1
    ewm_span: int = 21
    ann_factor: int = 252


@register_signal_class(register_in_agent=True)
class MarketCap(WeightsBase, DataNode):
    def __init__(
        self,
        volatility_control_configuration: VolatilityControlConfiguration | None,
        minimum_atvr_ratio: float = 0.1,
        rolling_atvr_volume_windows: list[int] | None = None,
        frequency_trading_percent: float = 0.9,
        source_frequency: str = "1d",
        min_number_of_assets: int = 3,
        num_top_assets: int | None = None,
        *args,
        **kwargs,
    ):
        """
        Signal Weights using weighting by Market Capitalization or Equal Weights

        Args:
            source_frequency (str): Frequency of market cap source.
            num_top_assets (Optional[int]): Number of largest assets by market cap to use for signals. Leave empty to include all assets.
        """
        if rolling_atvr_volume_windows is None:
            rolling_atvr_volume_windows=[60, 360]

        super().__init__(*args, **kwargs)
        self.source_frequency = source_frequency
        self.num_top_assets = num_top_assets or 50000
        self.minimum_atvr_ratio = minimum_atvr_ratio
        self.rolling_atvr_volume_windows = rolling_atvr_volume_windows
        self.frequency_trading_percent = frequency_trading_percent
        self.min_number_of_assets = min_number_of_assets

        translation_table = "marketcap_translation_table"
        try:
            # 1) fetch from server
            translation_table = AssetTranslationTable.get(unique_identifier=translation_table)
        except DoesNotExist:
            self.logger.error(f"Translation table {translation_table} does not exist")

        self.historical_market_cap_ts = WrapperDataNode(translation_table=translation_table)
        self.volatility_control_configuration = volatility_control_configuration

    def maximum_forward_fill(self):
        return timedelta(days=1) - TIMEDELTA

    def dependencies(self) -> dict[str, Union["DataNode", "APIDataNode"]]:
        return {"historical_market_cap_ts": self.historical_market_cap_ts}

    def get_explanation(self):
        # Convert the asset universe filter (assumed to be stored in self.asset_universe.asset_filter)
        # to a formatted JSON string for display.

        windows_str = ", ".join(str(window) for window in self.rolling_atvr_volume_windows)
        if self.volatility_control_configuration is not None:
            volatility_details = self.volatility_control_configuration
            vol_message = f"The strategy uses the following volatility target configuration:\n{volatility_details}\n"
        else:
            vol_message = "The strategy does not use volatility control.\n"

        explanation = (
            "### 1. Dynamic Asset Universe Selection\n\n"
            f"This strategy dynamically selects assets using a predefined category {self.assets_configuration.assets_category_unique_id} :\n\n"
            "### 2. Market Capitalization Filtering\n\n"
            f"The strategy retrieves historical market capitalization data and restricts the universe to the top **{self.num_top_assets}** assets. "
            "This ensures that only the largest and most influential market participants are considered.\n\n"
            "### 3. Liquidity Filtering via Annualized Traded Value Ratio (ATVR)\n\n"
            f"Liquidity is assessed using the Annualized Traded Value Ratio (ATVR), which compares an asset's annualized median trading volume to its market capitalization. "
            f"To obtain a robust measure of liquidity, ATVR is computed over multiple rolling windows: **[{windows_str}]** days. "
            f"An asset must achieve an ATVR of at least **{self.minimum_atvr_ratio:.2f}** in each of these windows to be considered liquid enough.\n\n"
            "### 4. Trading Frequency Filter\n\n"
            f"In addition, the strategy applies a trading frequency filter over the longest period defined by the rolling windows. "
            f"Only assets with trading activity on at least **{self.frequency_trading_percent:.2f}** of the days (i.e., {self.frequency_trading_percent * 100:.1f}%) in the longest window are retained.\n\n"
            "### 5. Portfolio Weight Construction\n\n"
            "After filtering based on market capitalization, liquidity, and trading frequency, the market capitalizations of the remaining assets are normalized on a daily basis. "
            "This normalization converts raw market values into portfolio weights, which serve as the signal for trading decisions.\n\n"
            "### 6. Data Source Frequency\n\n"
            f"The strategy uses market data that is updated at a **'{self.source_frequency}'** frequency. This ensures that the signals are generated using the most recent market conditions.\n\n"
            "### 7. Volatility Target\n\n"
            f"{vol_message}\n\n"
            "**Summary:**\n"
            f"This strategy dynamically selects assets using a specific filter, focuses on the top {self.num_top_assets} assets by market capitalization, and evaluates liquidity using ATVR computed over multiple rolling windows ({self.rolling_atvr_volume_windows}). "
            f"Assets must achieve a minimum ATVR of {self.minimum_atvr_ratio:.2f} in each window and meet a trading frequency requirement of at least {self.frequency_trading_percent * 100:.1f}%. "
            f"Finally, the market capitalizations of the filtered assets are normalized into portfolio weights, with market data refreshed at a '{self.source_frequency}' frequency."
        )

        return explanation

    def get_asset_list(self) -> None | list:
        asset_category = AssetCategory.get(
            unique_identifier=self.assets_configuration.assets_category_unique_id
        )

        asset_list = Asset.filter(id__in=asset_category.assets)
        return asset_list

    def update(self):
        """
        Args:
            latest_value (Union[datetime, None]): The timestamp of the most recent data point.

        Returns:
            DataFrame: A DataFrame containing updated signal weights, indexed by time and asset symbol.
        """
        asset_list = self.update_statistics.asset_list
        if len(asset_list) < self.min_number_of_assets:
            raise AssetMistMatch(
                f"only {len(asset_list)} in asset_list minum are {self.min_number_of_assets} "
            )

        unique_identifier_range_market_cap_map = {
            a.unique_identifier: {
                "start_date": self.update_statistics[a.unique_identifier],
                "start_date_operand": ">",
            }
            for a in asset_list
        }
        # Start Loop on unique identifier

        ms_asset_list = Asset.filter_with_asset_class(
            exchange_code=None,
            asset_ticker_group_id__in=[
                a.asset_ticker_group_id for a in self.update_statistics.asset_list
            ],
        )

        ms_asset_list = {a.asset_ticker_group_id: a for a in ms_asset_list}
        asset_list_to_share_class = {
            a.asset_ticker_group_id: a for a in self.update_statistics.asset_list
        }

        market_cap_uid_range_map = {
            ms_asset.get_spot_reference_asset_unique_identifier(): unique_identifier_range_market_cap_map[
                asset_list_to_share_class[ms_share_class].unique_identifier
            ]
            for ms_share_class, ms_asset in ms_asset_list.items()
        }

        market_cap_uid_to_asset_uid = {
            ms_asset.get_spot_reference_asset_unique_identifier(): asset_list_to_share_class[
                ms_share_class
            ].unique_identifier
            for ms_share_class, ms_asset in ms_asset_list.items()
        }

        mc = self.historical_market_cap_ts.get_df_between_dates(
            unique_identifier_range_map=market_cap_uid_range_map,
            great_or_equal=False,
        )
        mc = mc[~mc.index.duplicated(keep="first")]

        if mc.shape[0] == 0:
            self.logger.info("No data in Market Cap historical market cap")
            return pd.DataFrame()

        mc = mc.reset_index("unique_identifier")
        mc["unique_identifier"] = mc["unique_identifier"].map(market_cap_uid_to_asset_uid)
        mc = mc.set_index("unique_identifier", append=True)
        # ends loop on unique identifier
        unique_in_mc = mc.index.get_level_values("unique_identifier").unique().shape[0]

        if unique_in_mc != len(asset_list):
            self.logger.warning(
                "Market Cap and asset_list does not match missing assets will be set to 0"
            )

        # If there is no market cap data, return an empty DataFrame.
        if mc.shape[0] == 0:
            return pd.DataFrame()

        # 3. Pivot the market cap data to get a DataFrame with a datetime index and one column per asset.
        mc_raw = mc.pivot_table(columns="unique_identifier", index="time_index")
        mc_raw = mc_raw.ffill().bfill()

        # 4. Using the prices dataframe, compute a rolling statistic on volume.
        # We assume the "volume" column represents the traded volume.
        # First, pivot prices so that rows are dates and columns are assets.
        dollar_volume_df = mc_raw["volume"] * mc_raw["price"]

        # 5. Compute the rolling ATVR for each window specified in self.rolling_atv_volume_windows.
        #    For each window, compute the median traded volume, annualize it and divide by market cap.
        atvr_dict = {}
        for window in self.rolling_atvr_volume_windows:
            # Compute the rolling median of volume over the window.
            rolling_median = dollar_volume_df.rolling(window=window, min_periods=1).median()

            # Annualize: assume 252 trading days per year.
            annual_factor = 252  # todo fix when prices are not daily
            annualized_traded_value = rolling_median * annual_factor
            # Align with market cap dates.
            annualized_traded_value = annualized_traded_value.reindex(mc_raw.index).ffill().bfill()
            # Compute the ATVR.
            atvr_dict[window] = annualized_traded_value.div(mc_raw["market_cap"])

        # 6. Create a liquidity mask that requires the ATVR to be above the minimum threshold
        #    for every rolling window.
        atvr_masks = [
            atvr_dict[window] >= self.minimum_atvr_ratio
            for window in self.rolling_atvr_volume_windows
        ]
        # Combine the masks elementwise and re-wrap the result as a DataFrame with the same index/columns as mc_raw.
        combined_atvr_mask = pd.DataFrame(
            np.logical_and.reduce([mask.values for mask in atvr_masks]),
            index=mc_raw.index,
            columns=mc_raw.volume.columns,
        )

        # 7. Compute the trading frequency mask.
        #    For frequency we assume that an asset "traded" on a day if its volume is > 0.
        #    We use the longest rolling window (e.g. 360 days) for the frequency computation.
        freq_window = max(self.rolling_atvr_volume_windows)
        trading_flag = dollar_volume_df.fillna(0) > 0
        trading_frequency = trading_flag.rolling(window=freq_window, min_periods=1).mean()

        frequency_mask = trading_frequency >= self.frequency_trading_percent

        # 8. Combine the ATVR and frequency masks.
        liquidity_mask = combined_atvr_mask & frequency_mask

        # 9. (Optional) Select the top assets by market cap.
        #    For each date, rank assets by market cap and flag those outside the top 'self.num_top_assets'.
        assets_excluded = mc_raw["market_cap"].rank(axis=1, ascending=False) > self.num_top_assets

        # 10. Apply both the market cap ranking filter and the liquidity filter.
        filtered_mc = mc_raw["market_cap"].copy()
        filtered_mc[assets_excluded] = 0  # Exclude assets not in the top by market cap.
        filtered_mc[~liquidity_mask] = 0  # Exclude assets that do not meet the liquidity criteria.

        # 11. Compute the final weights by normalizing the surviving market caps.
        weights = filtered_mc.div(filtered_mc.sum(axis=1), axis=0)
        weights = weights.fillna(0)

        if self.volatility_control_configuration is not None:
            log_returns = (np.log(mc_raw["price"])).diff()

            ewm_vol = (log_returns * weights).sum(axis=1).ewm(
                span=self.volatility_control_configuration.ewm_span, adjust=False
            ).std() * np.sqrt(self.volatility_control_configuration.ann_factor)

            scaling_factor = self.volatility_control_configuration.target_volatility / ewm_vol
            scaling_factor = scaling_factor.clip(upper=1.0)
            weights = weights.mul(scaling_factor, axis=0)

        # 12. Reshape the weights to a long-form DataFrame if desired.
        signal_weights = weights.stack().rename("signal_weight").to_frame()

        return signal_weights
