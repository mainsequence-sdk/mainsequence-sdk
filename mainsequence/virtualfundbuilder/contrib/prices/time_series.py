import copy

import pytz
from typing import Union, Dict, List, Literal, Optional
import pandas as pd
import numpy as np
import datetime
import pandas_market_calendars as mcal

from mainsequence.tdag.time_series import TimeSerie, WrapperTimeSerie, APITimeSerie, data_source_pickle_path
from mainsequence.client import (CONSTANTS, LocalTimeSeriesDoesNotExist, LocalTimeSerie, DynamicTableDataSource,
                                  DataUpdates, AssetCategory, AssetTranslationTable, AssetTranslationRule, AssetFilter
                                 )
from mainsequence.client import MARKETS_CONSTANTS, ExecutionVenue
from mainsequence.client import HistoricalBarsSource, DoesNotExist, Asset, MarketsTimeSeriesDetails
from mainsequence.tdag.time_series.time_series import WrapperTimeSerie
from mainsequence.tdag.time_series.utils import (
    string_frequency_to_minutes,
    string_freq_to_time_delta,
)
import os

from tqdm import tqdm
from joblib import Parallel, delayed

from mainsequence.virtualfundbuilder.models import AssetsConfiguration
from mainsequence.virtualfundbuilder.utils import logger, TIMEDELTA
from typing import Optional
FULL_CALENDAR = "24/7"


def get_interpolated_prices_timeseries(assets_configuration: Optional[AssetsConfiguration]=None,
                                     ):
    """
    Creates a Wrapper Timeseries for an asset configuration.
    """
    prices_configuration = copy.deepcopy(assets_configuration).prices_configuration
    prices_configuration_kwargs = prices_configuration.model_dump()
    prices_configuration_kwargs.pop("is_live", None)
    prices_configuration_kwargs.pop("markets_time_series", None)


    return InterpolatedPrices(
        asset_category_unique_id=assets_configuration.assets_category_unique_id,
        **prices_configuration_kwargs
    )

class UpsampleAndInterpolation:
    """
    Handles upsampling and interpolation of bar data.
    """
    TIMESTAMP_COLS = ["first_trade_time", "last_trade_time", "open_time"]

    def __init__(
            self,
            bar_frequency_id: str,
            upsample_frequency_id: str,
            intraday_bar_interpolation_rule: str,
    ):
        self.bar_frequency_id = bar_frequency_id
        self.upsample_frequency_id = upsample_frequency_id
        self.intraday_bar_interpolation_rule = intraday_bar_interpolation_rule

        rows = string_frequency_to_minutes(self.upsample_frequency_id) / string_frequency_to_minutes(
            self.bar_frequency_id)
        assert rows.is_integer()

        if "days" in self.bar_frequency_id:
            assert bar_frequency_id == self.upsample_frequency_id  # Upsampling for daily bars not implemented

        self.upsample_frequency_td = string_freq_to_time_delta(self.upsample_frequency_id)

    @staticmethod
    def upsample_bars(
            bars_df: pd.DataFrame,
            upsample_frequency_obs: int,
            upsample_frequency_td: object,
            calendar: str,
            open_to_close_time_delta: datetime.timedelta,
            is_portfolio: bool = False
    ) -> pd.DataFrame:
        """
        Upsamples the bars dataframe based on the given parameters.
        For example, it can convert 5-minute bars to 1-minute bars.
        Note that this method works on iloc as the underlying data should be already interpolated so should be completed


        Args:
            bars_df (pd.DataFrame): The bars data to be upsampled.
            upsample_frequency_obs (int): Frequency for upsampling.
            upsample_frequency_td (object): Time delta for upsampling.
            calendar (str): Trading calendar to account for trading hours.
            open_to_close_time_delta (datetime.timedelta): Time delta between open and close.
            is_portfolio (bool): Whether the data is for a portfolio or a single asset.

        Returns:
            pd.DataFrame: The upsampled bars dataframe.
        """
        obs = bars_df.shape[0] / upsample_frequency_obs
        assert obs > 1.0

        trading_halts = calendar != FULL_CALENDAR
        calendar = mcal.get_calendar(calendar)

        full_schedule = calendar.schedule(bars_df["trade_day"].min(), bars_df["trade_day"].max()).reset_index()
        full_schedule["index"] = full_schedule["index"].apply(lambda x: x.timestamp())
        full_schedule = full_schedule.set_index("index").to_dict("index")

        all_dfs = []
        for i in tqdm(range(bars_df.shape[0] - upsample_frequency_obs + 1),
                      desc=f"Upsampling from {bars_df['trade_day'].iloc[0]} to {bars_df['trade_day'].iloc[-1]} for assets {bars_df['unique_identifier'].dropna().unique()}"):
            start = i
            end = i + upsample_frequency_obs
            tmp_df = bars_df.iloc[start:end]

            day_schedule = full_schedule[tmp_df["trade_day"].iloc[0].timestamp()]
            first_available_bar = day_schedule["market_open"] + upsample_frequency_td
            last_available_bar = day_schedule["market_close"]

            if trading_halts and tmp_df.index[-1] < first_available_bar:
                # edge case 1market is close should not upsample to the next day
                continue
            elif trading_halts and tmp_df.index[-1] > last_available_bar:
                continue
            else:
                dollar = tmp_df.vwap * tmp_df.volume
                volume = np.nansum(tmp_df.volume.values)
                vwap = np.nansum(dollar.values) / volume
                close = tmp_df.close.iloc[-1]
                vwap = vwap if not np.isnan(vwap) else close
                new_bar = {
                    "open_time": tmp_df.index[0] - open_to_close_time_delta,
                    "time": tmp_df.index[-1],
                    "volume": volume,
                    "vwap": vwap,
                    "open": tmp_df.open.iloc[0],
                    "close": close,
                }
                if not is_portfolio:
                    new_bar.update({
                        "high": np.nanmax(tmp_df.high.values),
                        "low": np.nanmin(tmp_df.low.values),
                    })

            all_dfs.append(new_bar)

        all_dfs = pd.DataFrame(all_dfs)
        all_dfs["unique_identifier"] = bars_df["unique_identifier"].iloc[0]
        all_dfs = all_dfs.set_index("time")

        return all_dfs

    def get_interpolated_upsampled_bars(
            self,
            calendar: str,
            tmp_df: pd.DataFrame,
            last_observation: Union[None, pd.Series] = None
    ) -> pd.DataFrame:
        """
        Gets interpolated and upsampled bars based on the given parameters.
        First interpolates the data to fill any gaps, then upsamples it to the desired frequency.

        Args:
            calendar (str): Trading calendar for interpolation and upsampling.
            tmp_df (pd.DataFrame): Dataframe containing the bars to be processed.
            last_observation (Union[None, pd.Series], optional): Last observed data to fill gaps.

        Returns:
            pd.DataFrame: Interpolated and upsampled bars dataframe.
        """
        for col in self.TIMESTAMP_COLS:
            try:
                if col in tmp_df.columns:
                    tmp_df[col] = pd.to_datetime(tmp_df[col], utc=True)
            except Exception as e:
                raise e

        if "d" in self.bar_frequency_id:
            tmp_df = interpolate_daily_bars(
                bars_df=tmp_df.copy(),
                interpolation_rule=self.intraday_bar_interpolation_rule,
                calendar=calendar,
                last_observation=last_observation,
            )
        elif "m" in self.bar_frequency_id:
            bars_frequency_min = string_frequency_to_minutes(self.bar_frequency_id)

            # Interpolation to fill gaps
            tmp_df = interpolate_intraday_bars(
                bars_df=tmp_df.copy(),
                interpolation_rule=self.intraday_bar_interpolation_rule,
                calendar=calendar,
                bars_frequency_min=bars_frequency_min,
                last_observation=last_observation,
            )

        if len(tmp_df) == 0:
            return tmp_df

        assert tmp_df.isnull().sum()[["close", "open"]].sum() == 0

        # Upsample to the correct frequency
        if "d" in self.bar_frequency_id:
            all_columns = self.TIMESTAMP_COLS
            upsampled_df = tmp_df
        else:
            upsample_freq_obs = string_frequency_to_minutes(self.upsample_frequency_id) // bars_frequency_min

            if upsample_freq_obs > bars_frequency_min:
                upsampled_df = UpsampleAndInterpolation.upsample_bars(
                    bars_df=tmp_df,
                    upsample_frequency_obs=upsample_freq_obs,
                    upsample_frequency_td=self.upsample_frequency_td,
                    calendar=calendar,
                    is_portfolio=False,
                    open_to_close_time_delta=datetime.timedelta(minutes=bars_frequency_min),
                )
            else:
                upsampled_df = tmp_df
            all_columns = self.TIMESTAMP_COLS + ["trade_day"]

        for col in all_columns:
            if col in upsampled_df.columns:
                upsampled_df[col] = pd.to_datetime(upsampled_df[col]).astype(np.int64).values

        return upsampled_df


def interpolate_daily_bars(
        bars_df: pd.DataFrame,
        interpolation_rule: str,
        calendar: str,
        last_observation: Union[None, pd.Series] = None,
):
    try:
        calendar_instance = mcal.get_calendar(calendar.name)
    except Exception as e:
        raise e

    def rebase_with_forward_fill(bars_df, last_observation):
        try:
            if last_observation is not None:
                if "interpolated" in last_observation.columns:
                    last_observation = last_observation.drop(columns="interpolated")

                bars_df = pd.concat([last_observation, bars_df], axis=0).sort_index()
                if "unique_identifier" in bars_df.columns:
                    bars_df.loc[:, ['unique_identifier']] = bars_df[
                        ['unique_identifier']
                    ].bfill().ffill()

            null_index = bars_df.isnull().any(axis=1)
            bars_df["close"] = bars_df["close"].ffill()
            bars_df["open"] = bars_df["open"].where(~null_index, bars_df["close"])
            try:
                bars_df.volume = bars_df.volume.fillna(0)
            except Exception as e:
                raise e
            if "vwap" in bars_df.columns:
                bars_df.vwap = bars_df.vwap.ffill()
            if "trade_count" in bars_df.columns:
                bars_df.trade_count = bars_df.trade_count.fillna(0)

            if len(null_index) > 0:
                if "high" in bars_df.columns:
                    bars_df["high"] = bars_df["high"].where(~null_index, bars_df["close"])
                    bars_df["low"] = bars_df["low"].where(~null_index, bars_df["close"])

                bars_df["interpolated"] = False
                bars_df.loc[null_index, "interpolated"] = True

            else:
                bars_df["interpolated"] = False

            if last_observation is not None:
                bars_df = bars_df.iloc[1:]
        except Exception as e:
            raise e

        return bars_df

    # Restrict to calendar types
    restricted_schedule = None
    full_index = bars_df.index

    restricted_schedule = calendar_instance.schedule(bars_df.index.min(),
                                                     bars_df.index.max())  # This needs to be faster
    restricted_schedule = restricted_schedule.reset_index()
    market_type = "market_close"

    restricted_schedule = restricted_schedule.set_index(market_type)
    full_index = bars_df.index.union(restricted_schedule.index)

    bars_df = bars_df.reindex(full_index)

    if interpolation_rule == "None":
        pass
    elif interpolation_rule == "ffill":
        bars_df = rebase_with_forward_fill(bars_df, last_observation=last_observation)
        if last_observation is None:
            bars_df = bars_df.bfill()
    else:
        raise Exception

    if len(bars_df):
        last_observation = bars_df.iloc[[-1]]

    if len(bars_df) == 0:
        return pd.DataFrame()

    bars_df = bars_df[bars_df.index.isin(restricted_schedule.index)]

    null_index = bars_df[bars_df["open_time"].isnull()].index
    if len(null_index) > 0:
        bars_df.loc[null_index, "open_time"] = restricted_schedule.loc[null_index].index

    return bars_df


def interpolate_intraday_bars(
        bars_df: pd.DataFrame,
        interpolation_rule: str,
        bars_frequency_min: int,
        calendar: str,
        last_observation: Union[None, pd.Series] = None,
) -> pd.DataFrame:
    """
    Interpolates intraday bars based on the given parameters. Fills in missing data points in intraday bar data in case of gaps.
    """
    calendar_instance = mcal.get_calendar(calendar.name)

    def build_daily_range_from_schedule(start, end):
        return pd.date_range(start=start, end=end, freq=f"{bars_frequency_min}min")

    def sanitize_today_update(x: pd.DataFrame, date_range):
        today = datetime.datetime.utcnow()
        if day.date() == today.date():
            x.index.name = None
            date_range = [i for i in date_range if i <= x.index.max()]
        return date_range

    def rebase_withoutnan_fill(x, trade_starts, trade_ends):
        date_range = build_daily_range_from_schedule(trade_starts, trade_ends)
        date_range = sanitize_today_update(x=x, date_range=date_range)
        x = x.reindex(date_range)
        return x

    def rebase_with_forward_fill(x, trade_starts, trade_ends, last_observation):
        is_start_of_day = False
        if (x.shape[0] == 1) and x.index[0].hour == 0:
            is_start_of_day = True
        x["interpolated"] = False
        if not is_start_of_day:
            date_range = build_daily_range_from_schedule(trade_starts, trade_ends)
            date_range = sanitize_today_update(x=x, date_range=date_range)
            try:
                x = x.reindex(date_range)

                if last_observation is not None:
                    if "interpolated" in x.columns:
                        last_observation = last_observation.drop(columns="interpolated")
                    x = pd.concat([last_observation, x], axis=0)

                null_index = x[x["close"].isnull()].index
                x.close = x.close.ffill()
                x.loc[null_index, "open"] = x.loc[null_index, "close"]
                x.volume = x.volume.fillna(0)
                x.vwap = x.vwap.ffill()
                if "trade_count" in x.columns:
                    x.trade_count = x.trade_count.fillna(0)
                x["interpolated"] = False
                if len(null_index) > 0:
                    if "high" in x.columns:
                        x.loc[null_index, "high"] = x.loc[null_index, "close"]
                        x.loc[null_index, "low"] = x.loc[null_index, "close"]

                    x.loc[null_index, "interpolated"] = True

                if last_observation is not None:
                    x = x.iloc[1:]
            except Exception as e:
                raise e

        # interpolate any other columns with 0

        return x

    full_index = bars_df.index

    # because index are closes the greates value should be the open time of the last close to do not extra interpolate
    restricted_schedule = calendar_instance.schedule(bars_df.index.min(),
                                                     bars_df.iloc[-1]["open_time"])  # This needs to be faster

    bars_df = bars_df[~bars_df.index.duplicated(keep='first')]  # todo: remove uncessary with indices.

    full_index = bars_df.index.union(restricted_schedule.set_index("market_open").index).union(
        restricted_schedule.set_index("market_close").index)

    restricted_schedule = restricted_schedule.set_index('market_open')

    bars_df = bars_df.reindex(full_index)

    bars_df["trade_day"] = bars_df.index
    bars_df["trade_day"] = bars_df["trade_day"].apply(lambda x: x.replace(hour=0, minute=0, second=0))

    groups = bars_df.groupby("trade_day")
    interpolated_data = []
    restricted_schedule.index = restricted_schedule.index.map(lambda x: x.timestamp())
    restricted_schedule = restricted_schedule.to_dict()
    for day, group_df in tqdm(groups,
                              desc=f"Interpolating bars from {bars_df.index.min()} to {bars_df.index.max()} for assets {bars_df['unique_identifier'].dropna().unique()}"):
        schedule = calendar_instance.schedule(start_date=day, end_date=day)
        if schedule.shape[0] == 0:
            continue
        try:
            trade_starts = schedule["market_open"].iloc[0]
            trade_ends = schedule["market_close"].iloc[0]
        except Exception as e:
            raise e

        group_df = group_df[group_df.index >= schedule["market_open"].iloc[0]]
        group_df = group_df[group_df.index <= schedule["market_close"].iloc[0]]

        if group_df.dropna().shape[0] == 0:
            continue

        if trade_starts < day:
            trade_starts = day
        next_day = day + datetime.timedelta(days=1)
        if trade_ends >= next_day:
            trade_ends = next_day - datetime.timedelta(minutes=1)

        if interpolation_rule == "None":
            tmp_df = rebase_withoutnan_fill(group_df, trade_starts=trade_starts, trade_ends=trade_ends)
        elif interpolation_rule == "ffill":
            tmp_df = rebase_with_forward_fill(
                group_df,
                trade_starts=trade_starts,
                last_observation=last_observation,
                trade_ends=trade_ends
            )
            if last_observation is None:
                tmp_df = tmp_df.bfill()
        else:
            raise Exception

        if len(tmp_df):
            last_observation = tmp_df.iloc[[-1]]
        interpolated_data.append(tmp_df)

    if len(interpolated_data) == 0:
        return pd.DataFrame()

    interpolated_data = pd.concat(interpolated_data, axis=0)
    interpolated_data["trade_day"] = interpolated_data.index
    interpolated_data["trade_day"] = interpolated_data["trade_day"].apply(
        lambda x: x.replace(hour=0, minute=0, second=0)
    )

    return interpolated_data


class InterpolatedPrices(TimeSerie):
    """
    Handles interpolated prices for assets.
    """
    OFFSET_START = datetime.datetime(2017, 7, 20).replace(tzinfo=pytz.utc)

    def __init__(
            self,

            bar_frequency_id: str,
            intraday_bar_interpolation_rule: str,
            asset_category_unique_id: Optional[str] = None,
            upsample_frequency_id: Optional[str] = None,
            asset_list: List = None, # todo change for asset_filter when asset filter has all the characteristics
            translation_table_unique_id: Optional[str] = None,
            local_kwargs_to_ignore: List[str] = ["asset_category_unique_id","asset_list"],
            *args,
            **kwargs
    ):
        """
        Initializes the InterpolatedPrices object.
        """
        assert "d" in bar_frequency_id or "m" in bar_frequency_id, f"bar_frequency_id={bar_frequency_id} should be 'd for days' or 'm for min'"

        if asset_category_unique_id is None:
            assert asset_list is not None, f"asset_category_unique_id={asset_category_unique_id} should not be None or asset_list should be defined"

        if translation_table_unique_id is None:
            raise Exception(f"Translation table needs to be set")

        self.asset_category_unique_id = asset_category_unique_id
        self.interpolator = UpsampleAndInterpolation(
            bar_frequency_id=bar_frequency_id,
            upsample_frequency_id=upsample_frequency_id,
            intraday_bar_interpolation_rule=intraday_bar_interpolation_rule
        )
        self.constructor_asset_list = asset_list
        bars_frequency_min = string_frequency_to_minutes(bar_frequency_id)
        self.maximum_forward_fill = datetime.timedelta(minutes=bars_frequency_min) - TIMEDELTA

        self.intraday_bar_interpolation_rule = intraday_bar_interpolation_rule
        self.bar_frequency_id = bar_frequency_id
        self.upsample_frequency_id = upsample_frequency_id

        # get the translation rules
        translation_table = AssetTranslationTable.get(unique_identifier=translation_table_unique_id)

        self.bars_ts = WrapperTimeSerie(translation_table=translation_table)
        super().__init__(local_kwargs_to_ignore=local_kwargs_to_ignore,*args, **kwargs)



    def _get_required_cores(self, last_observation_map) -> int:
        """
        Determines the required number of cores for processing.
        """
        if len(last_observation_map) == 0:
            required = 1
        else:
            required = min(len(last_observation_map), 20)

        return required

    def _run_post_update_routines(self, error_on_last_update, update_statistics):
        if not self.persistence.metadata.protect_from_deletion:
            self.local_persist_manager.protect_from_deletion()

    def _transform_raw_data_to_upsampled_df(
            self,
            raw_data_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Transforms raw data into an upsampled dataframe.
        """
        upsampled_df = []
        full_last_observation = self.get_last_observation()
        last_observation_map = {}

        for unique_identifier in raw_data_df["unique_identifier"].unique():
            if full_last_observation is None:
                last_observation_map[unique_identifier] = None
                continue

            if unique_identifier in full_last_observation.index.get_level_values("unique_identifier").to_list():
                last_obs = full_last_observation.loc[(slice(None), unique_identifier), :].reset_index(
                    ["unique_identifier"], drop=True
                )
                last_obs.index.name = None
                if "open_time" in last_obs.columns:
                    last_obs["open_time"] = pd.to_datetime(last_obs["open_time"], utc=True)
                last_observation_map[unique_identifier] = last_obs
            else:
                last_observation_map[unique_identifier] = None

        def multiproc_upsample(calendar, tmp_df, unique_identifier, last_observation, interpolator_kwargs):
            interpolator = UpsampleAndInterpolation(**interpolator_kwargs)
            df = interpolator.get_interpolated_upsampled_bars(
                calendar=calendar,
                tmp_df=tmp_df,
                last_observation=last_observation_map[unique_identifier]
            )
            df["unique_identifier"] = unique_identifier
            return df

        required_cores = self._get_required_cores(last_observation_map=last_observation_map)
        required_cores = 1
        if required_cores == 1:
            # Single-core processing
            for unique_identifier, df in raw_data_df.groupby("unique_identifier"):
                if df.shape[0] > 0:
                    df = self.interpolator.get_interpolated_upsampled_bars(
                        calendar=self.asset_calendar_map[unique_identifier],
                        tmp_df=df,
                        last_observation=last_observation_map[unique_identifier],
                    )
                    df["unique_identifier"] = unique_identifier
                    upsampled_df.append(df)
        else:
            upsampled_df = Parallel(n_jobs=required_cores)(
                delayed(multiproc_upsample)(
                    calendar=self.asset_calendar_map[unique_identifier],
                    tmp_df=tmp_df,
                    unique_identifier=unique_identifier,
                    last_observation=last_observation_map[unique_identifier],
                    interpolator_kwargs=dict(
                        bar_frequency_id=self.bar_frequency_id,
                        upsample_frequency_id=self.upsample_frequency_id,
                        intraday_bar_interpolation_rule=self.intraday_bar_interpolation_rule,
                    )
                )
                for unique_identifier, tmp_df in raw_data_df.groupby("unique_identifier") if tmp_df.shape[0] > 0
            )

        upsampled_df = [d for d in upsampled_df if len(d) > 0]  # Remove empty dataframes
        if len(upsampled_df) == 0:
            return pd.DataFrame()

        max_value_per_asset = {d.index.max(): d.unique_identifier.iloc[0] for d in upsampled_df}
        min_max = min(max_value_per_asset.keys())
        self.logger.info(f"min_max {max_value_per_asset[min_max]} {min_max} max_max {max(max_value_per_asset.keys())}")
        upsampled_df = pd.concat(upsampled_df, axis=0)
        # upsampled_df = upsampled_df[upsampled_df.index <= min_max]
        upsampled_df.volume = upsampled_df.volume.fillna(0)

        upsampled_df.index.name = "time_index"
        upsampled_df = upsampled_df.set_index("unique_identifier", append=True)
        upsampled_df = upsampled_df.sort_index(level=0)

        if upsampled_df.shape[0] == 0:
            upsampled_df = pd.DataFrame()

        return upsampled_df

    def get_upsampled_data(
            self,
            update_statistics: DataUpdates,
    ) -> pd.DataFrame:
        """
        Main method to get upsampled data for prices.
        """
        from mainsequence.virtualfundbuilder.time_series import PortfolioStrategy

        unique_identifier_range_map = {
            unique_identifier: {
                "start_date": last_update,
                "start_date_operand": '>=',
            } for unique_identifier, last_update in update_statistics.update_statistics.items()
        }

        raw_data_df = self.bars_ts.get_df_between_dates(unique_identifier_range_map=unique_identifier_range_map)

        if raw_data_df.empty == True:
            self.logger.info("New new data to interpolate")
            return pd.DataFrame()

        upsampled_df = self._transform_raw_data_to_upsampled_df(
            raw_data_df.reset_index(["unique_identifier"]),
        )
        return upsampled_df

    def _get_asset_list(self):
        """
        Creates mappings from symbols to IDs
        """
        if self.constructor_asset_list is not None:
            asset_list= self.constructor_asset_list
        else:
            asset_category = AssetCategory.get(unique_identifier=self.asset_category_unique_id)
            asset_list = Asset.filter(id__in=asset_category.assets)
        self.asset_calendar_map = {a.unique_identifier: a.get_calendar() for a in asset_list}
        return asset_list

    def update(
            self,
            update_statistics: DataUpdates
    ) -> pd.DataFrame:
        """
        Updates the series from the source based on the latest value.
        """
        prices = self.get_upsampled_data(
            update_statistics=update_statistics,
        )

        if prices.shape[0]==0:
            return pd.DataFrame()

        if update_statistics.is_empty() == False:
            TARGET_COLS = ['open', 'close', 'high', 'low', 'volume', 'open_time']
            assert prices[[c for c in prices.columns if c in TARGET_COLS]].isnull().sum().sum() == 0

        prices = update_statistics.filter_df_by_latest_value(prices)

        duplicates_exist = prices.reset_index().duplicated(subset=["time_index", "unique_identifier"]).any()
        if duplicates_exist:
            raise Exception()

        # adapt to InterpolatedPrices data schema
        if "vwap" not in prices:
            self.logger.warning("vwap not calculated in prices, set to NaN")
            prices["vwap"] = np.nan
        if "trade_count" not in prices:
            self.logger.warning("trade_count not calculated in prices, set to NaN")
            prices["trade_count"] = np.nan

        prices = prices[['open_time', 'open', 'high', 'low', 'close', 'volume', 'trade_count', 'vwap', 'interpolated']]
        return prices

class ExternalPrices(TimeSerie):

    def __init__(
            self,
            artifact_name: str,
            bucket_name: str,
            asset_category_unique_id,
            *args,
            **kwargs
    ):
        self.artifact_name = artifact_name
        self.bucket_name = bucket_name
        self.asset_category_unique_id = asset_category_unique_id
        super().__init__(*args, **kwargs)

    def _get_asset_list(self):
        """
        Creates mappings from symbols to IDs
        """
        asset_category = AssetCategory.get(unique_identifier=self.asset_category_unique_id)
        asset_list = Asset.filter(id__in=asset_category.assets)
        return asset_list

    def update(
            self,
            update_statistics: DataUpdates
    ) -> pd.DataFrame:
        from mainsequence.client.models_tdag import Artifact
        source_artifact = Artifact.get(bucket__name=self.bucket_name, name=self.artifact_name)
        prices_source = pd.read_csv(source_artifact.content)

        expected_cols = [
            "time_index",
            "figi",
            "price",
        ]
        prices_source = prices_source[expected_cols].copy()
        prices_source["time_index"] = pd.to_datetime(
            prices_source["time_index"], utc=True
        )

        # convert figis in source data
        for asset in update_statistics.asset_list:
            prices_source.loc[prices_source["figi"] == asset.figi, "unique_identifier"] = asset.unique_identifier

        prices_source.set_index(["time_index", "unique_identifier"], inplace=True)
        prices = update_statistics.filter_df_by_latest_value(prices_source)

        prices = prices.rename(columns={"price": "open"})[["open"]]
        return prices
