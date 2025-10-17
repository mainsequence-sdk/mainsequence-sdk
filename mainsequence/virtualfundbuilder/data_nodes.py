import ast
import copy
import json
import os
from datetime import datetime
from typing import Union

import numpy as np
import pandas as pd
import pytz

import mainsequence.client as msc
from mainsequence.client import Asset, AssetCategory
from mainsequence.tdag.data_nodes import APIDataNode, DataNode, WrapperDataNode
from mainsequence.virtualfundbuilder.contrib.prices.data_nodes import (
    get_interpolated_prices_timeseries,
)
from mainsequence.virtualfundbuilder.resource_factory.rebalance_factory import RebalanceFactory
from mainsequence.virtualfundbuilder.resource_factory.signal_factory import SignalWeightsFactory

from .. import client as ms_client
from .models import PortfolioBuildConfiguration


def translate_to_pandas_freq(custom_freq):
    """
    Translate custom datetime frequency strings to Pandas frequency strings.

    Args:
        custom_freq (str): Custom frequency string (e.g., '1d', '1m', '1mo').

    Returns:
        str: Pandas frequency string (e.g., 'D', 'T', 'M').
    """
    # Mapping for the custom frequencies to pandas frequencies
    freq_mapping = {
        "d": "D",  # days
        "m": "min",  # minutes
        "mo": "M",  # months
    }

    # Extract the numeric part and the unit part
    import re

    match = re.match(r"(\d+)([a-z]+)", custom_freq)
    if not match:
        raise ValueError(f"Invalid frequency format: {custom_freq}")

    number, unit = match.groups()

    # Map the unit to the corresponding pandas frequency
    if unit not in freq_mapping:
        raise ValueError(f"Unsupported frequency unit: {unit}")

    pandas_freq = freq_mapping[unit]

    # Combine the number with the pandas frequency
    return f"{number}{pandas_freq}"


WEIGHTS_TO_PORTFOLIO_COLUMNS = {
    "rebalance_weights": "weights_current",
    "rebalance_price": "price_current",
    "volume": "volume_current",
    "weights_at_last_rebalance": "weights_before",
    "price_at_last_rebalance": "price_before",
    "volume_at_last_rebalance": "volume_before",
}

All_PORTFOLIO_COLUMNS = []
All_PORTFOLIO_COLUMNS.extend(list(WEIGHTS_TO_PORTFOLIO_COLUMNS.keys()))
All_PORTFOLIO_COLUMNS.extend(["last_rebalance_date", "close", "return"])


class PortfolioFromDF(DataNode):

    def __init__(
        self, portfolio_name: str, calendar_name: str, target_portfolio_about: str, *args, **kwargs
    ):
        self.portfolio_name = portfolio_name
        self.calendar_name = calendar_name
        self.target_portfolio_about = target_portfolio_about
        super().__init__(*args, **kwargs)

    def dependencies(self) -> dict[str, Union["DataNode", "APIDataNode"]]:
        return {}

    def get_portfolio_df(self):
        raise NotImplementedError()

    def update(self):
        df = self.get_portfolio_df()
        if df.empty:
            return pd.DataFrame()

        # Ensure columns are known
        assert all(c in All_PORTFOLIO_COLUMNS for c in df.columns)

        # Optional time filter
        mti = getattr(self.update_statistics, "max_time_index_value", None)
        if mti is not None:
            df = df[df.index >= mti]
            if df.empty:
                return pd.DataFrame()

        # Normalizer: value -> canonical JSON string of a dict
        def _to_json_dict(v, colname):
            # Normalize missing/empty
            if pd.isna(v):
                v = {}
            elif isinstance(v, str):
                s = v.strip()
                if s == "":
                    v = {}
                else:
                    # Try JSON first, then Python literal
                    try:
                        v = json.loads(s)
                    except json.JSONDecodeError:
                        try:
                            v = ast.literal_eval(s)
                        except (ValueError, SyntaxError) as err:
                            # Preserve the original cause for clearer tracebacks
                            raise ValueError(
                                f"Value in '{colname}' is not JSON/dict: {v!r}"
                            ) from err

            if not isinstance(v, dict):
                raise ValueError(
                    f"Value in '{colname}' is not a dict after normalization (got {type(v)})."
                )

            # Canonical JSON + round-trip sanity check
            out = json.dumps(v, ensure_ascii=False, sort_keys=True)
            json.loads(out)
            return out

        # Apply to expected weight columns
        for c in WEIGHTS_TO_PORTFOLIO_COLUMNS.keys():
            if c not in df.columns:
                raise KeyError(f"Missing expected column '{c}' in DataFrame.")
            df[c] = df[c].apply(lambda v, col=c: _to_json_dict(v, col))

        return df


class PortfolioStrategy(DataNode):
    """
    Manages the rebalancing of asset weights within a portfolio over time, considering transaction fees
    and rebalancing strategies. Calculates portfolio values and returns while accounting for execution-specific fees.
    """

    def __init__(self, portfolio_build_configuration: PortfolioBuildConfiguration, *args, **kwargs):
        """
        Initializes the PortfolioStrategy class with the necessary configurations.

        Args:
            portfolio_build_configuration (PortfolioBuildConfiguration): Configuration for building the portfolio,
                including assets, execution parameters, and backtesting weights.
            is_live (bool): Flag indicating whether the strategy is running in live mode.
        """
        self.portfolio_build_configuration=portfolio_build_configuration
        self.execution_configuration = portfolio_build_configuration.execution_configuration
        self.backtesting_weights_config = (
            portfolio_build_configuration.backtesting_weights_configuration
        )

        self.commission_fee = self.execution_configuration.commission_fee

        self.portfolio_prices_frequency = portfolio_build_configuration.portfolio_prices_frequency

        self.assets_configuration = portfolio_build_configuration.assets_configuration

        self.portfolio_frequency = (
            self.assets_configuration.prices_configuration.upsample_frequency_id
        )

        self.full_signal_weight_config = copy.deepcopy(
            self.backtesting_weights_config.signal_weights_configuration
        )

        self.signal_weights_name = self.backtesting_weights_config.signal_weights_name

        self.signal_weights=self.backtesting_weights_config.get_signal_weights_instance()
        if self.signal_weights is None:
            SignalWeightClass = SignalWeightsFactory.get_signal_weights_strategy(
                signal_weights_name=self.signal_weights_name
            )
            self.signal_weights = SignalWeightClass.build_and_parse_from_configuration(
                **self.full_signal_weight_config
            )

        self.rebalance_strategy_name = self.backtesting_weights_config.rebalance_strategy_name

        self.rebalancer=self.backtesting_weights_config.get_rebalancer_instance()
        if self.rebalancer is None:
            RebalanceClass = RebalanceFactory.get_rebalance_strategy(
                rebalance_strategy_name=self.rebalance_strategy_name
            )
            self.rebalancer = RebalanceClass(
                **self.backtesting_weights_config.rebalance_strategy_configuration
            )

        self.rebalancer_explanation = ""  # TODO: Add rebalancer explanation

        asset_list = None
        if not self.assets_configuration.assets_category_unique_id:
            asset_list = self.signal_weights.get_asset_list()
            portfolio_asset_uid = self.signal_weights.get_asset_uid_to_override_portfolio_price()
            if portfolio_asset_uid is not None:
                asset = msc.Asset.get_or_none(unique_identifier=portfolio_asset_uid)
                if asset is None:
                    raise Exception(
                        f"{portfolio_asset_uid} not found. be sure that is on the price transaltion table"
                    )
                asset_list = asset_list + [asset]
                asset_list = list({a.id: a for a in asset_list}.values())

        self.bars_ts = get_interpolated_prices_timeseries(
            copy.deepcopy(self.assets_configuration), asset_list=asset_list
        )

        super().__init__(*args, **kwargs)

    def get_asset_list(self):
        """
        Creates mappings from symbols to IDs
        """
        if self.assets_configuration.assets_category_unique_id:
            asset_category = AssetCategory.get(
                unique_identifier=self.assets_configuration.assets_category_unique_id
            )
            asset_list = Asset.filter(
                id__in=asset_category.assets
            )  # no need for specifics as only symbols are relevant
        else:
            # get all assets of signal
            asset_list = self.signal_weights.get_asset_list()

        return asset_list

    def _calculate_start_end_dates(self):
        """
        Calculates the start and end dates for processing based on the latest value and available data.
        The end date is calcualted to get the end dates of the prices of all assets involved, and using the earliest to ensure that all assets have prices.

        Args:
            latest_value (datetime): The timestamp of the latest available data.

        Returns:
            Tuple[datetime, datetime]: A tuple containing the start date and end date for processing.
        """
        # Get last observations for each exchange
        update_statics_from_dependencies = self.bars_ts.update_statistics
        earliest_last_value = max(update_statics_from_dependencies.asset_time_statistics.values())

        if earliest_last_value is None:
            self.logger.warning(
                f"update_statics_from_dependencies {update_statics_from_dependencies}"
            )
            raise Exception("Prices are empty")

        # Determine the last value where all assets have data
        if self.assets_configuration.prices_configuration.forward_fill_to_now:
            end_date = datetime.now(pytz.utc)
        else:
            end_date = earliest_last_value + self.bars_ts.maximum_forward_fill

        # Handle case when latest_value is None
        start_date = self.update_statistics.max_time_index_value or self.OFFSET_START

        # Adjust end_date based on max time difference variable if set
        max_td_env = os.getenv("MAX_TD_FROM_LATEST_VALUE", None)
        if max_td_env is not None:
            new_end_date = start_date + pd.Timedelta(max_td_env)
            end_date = new_end_date if new_end_date < end_date else end_date

        return start_date, end_date

    def _generate_new_index(self, start_date, end_date, rebalancer_calendar):
        """
        Generates a new index based on frequency and calendar.

        Args:
            start_date (datetime): Latest timestamp in series.
            end_date (datetime): Upper limit for date range.
            rebalancer_calendar: Calendar object from the rebalancer.

        Returns:
            pd.DatetimeIndex: New index for resampling.
        """
        upsample_freq = self.assets_configuration.prices_configuration.upsample_frequency_id

        if "d" in upsample_freq:
            assert upsample_freq == "1d", "Only '1d' frequency is implemented."
            upsample_freq = translate_to_pandas_freq(upsample_freq)
            freq = upsample_freq.replace("days", "d")
            schedule = rebalancer_calendar.schedule(start_date=start_date, end_date=end_date)
            new_index = schedule.set_index("market_close").index
            new_index.name = None
            new_index = new_index[new_index <= end_date]

        else:
            upsample_freq = translate_to_pandas_freq(upsample_freq)
            self.logger.warning("Matching new index with calendar")
            freq = upsample_freq

            new_index = pd.date_range(start=start_date, end=end_date, freq=freq)
        return new_index, freq

    def dependencies(self) -> dict[str, Union["DataNode", "APIDataNode"]]:
        return {"bars_ts": self.bars_ts, "signal_weights": self.signal_weights}

    def _postprocess_weights(self, weights):
        """
        Prepares backtesting weights DataFrame for storage and sends them to VAM if applicable.

        Args:
            weights (pd.DataFrame): DataFrame of backtesting weights.
            latest_value (datetime): Latest timestamp.

        Returns:
            pd.DataFrame: Prepared backtesting weights.
        """
        # Filter for dates after latest_value
        if self.update_statistics.max_time_index_value is not None:
            weights = weights[weights.index > self.update_statistics.max_time_index_value]
        if weights.empty:
            return pd.DataFrame()

        # Reshape and validate the DataFrame
        weights = weights.stack()
        required_columns = ["weights_before", "weights_current", "price_current", "price_before"]
        for col in required_columns:
            assert col in weights.columns, f"Column '{col}' is missing in weights"

        weights = weights.dropna(subset=["weights_current"])
        # Filter again for dates after latest_value
        if self.update_statistics.max_time_index_value is not None:
            weights = weights[
                weights.index.get_level_values("time_index")
                > self.update_statistics.max_time_index_value
            ]

        # Prepare the weights before by using the last weights used for the portfolio and the new weights
        if self.update_statistics.max_time_index_value is not None:
            last_weights = self._get_last_weights()
            weights = pd.concat([last_weights, weights], axis=0).fillna(0)

        return weights

    def get_portfolio_about_text(self):
        """
        Constructs the portfolio about text.

        Returns:
            str: Portfolio description.
        """
        portfolio_about = """Portfolio created with Main Sequence VirtualFundBuilder engine with the following signal and
rebalance details:"""
        return json.dumps(portfolio_about)

    def build_prefix(self):
        reba_strat = self.rebalance_strategy_name
        signa_name = self.signal_weights_name
        return f"{reba_strat}_{signa_name}"

    def _calculate_portfolio_returns(
        self,
        weights: pd.DataFrame,
        prices: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Calculates the returns for the portfolio based on the asset prices and their respective weights,
        including the impact of transaction fees.

        Args:
            weights (pd.DataFrame): DataFrame containing weights of assets at different timestamps.
            prices (pd.DataFrame): DataFrame containing prices of assets.

        Returns:
            pd.DataFrame: DataFrame containing portfolio returns with and without transaction fees.
        """
        weights = weights.reset_index().pivot(
            index="time_index",
            columns=["unique_identifier"],
            values=["price_current", "weights_before", "weights_current"],
        )

        price_current = weights.price_current
        weights_before = weights.weights_before.fillna(0)
        weights_current = weights.weights_current.fillna(0)

        prices = prices[self.assets_configuration.price_type.value].unstack()

        # get the first date for prices
        first_price_date = (
            prices.stack().dropna().index.union(price_current.stack().dropna().index)[0][0]
        )

        prices = (
            price_current.combine_first(prices).sort_index().ffill()
        )  # combine raw prices with signal prices for continous price ts
        prices = prices.reindex(weights.index)

        returns = (prices / prices.shift(1) - 1).fillna(0.0)
        returns.replace([np.inf, -np.inf], 0, inplace=True)

        # Calculate weighted returns per coin: R_c = w_past_c * r_c
        weights_before = weights_before.reindex(returns.index, method="ffill").dropna()
        weights_current = weights_current.reindex(returns.index, method="ffill").dropna()

        weighted_returns = (weights_before * returns).dropna()

        weights_diff = (weights_current - weights_before).fillna(0)
        # Fees = w_diff * fee%
        fees = (weights_diff.abs() * self.commission_fee).sum(axis=1)

        # Sum returns over assets
        portfolio_returns = pd.DataFrame(
            {
                "return": weighted_returns.sum(axis=1) - fees,
            }
        )
        portfolio_returns = portfolio_returns[portfolio_returns.index >= first_price_date]

        return portfolio_returns

    def _calculate_portfolio_values(self, portfolio: pd.DataFrame) -> pd.DataFrame:
        """
        Calculates and applies cumulative returns to get the current portfolio values.
        For re-executions, the last portfolio values are retrieved from the database.

        Args:
            portfolio (pd.DataFrame): DataFrame containing portfolio returns.
            latest_value (datetime): Timestamp indicating the latest data point for starting calculations.

        Returns:
            pd.DataFrame: Updated portfolio values with and without fees and returns.
        """
        last_portfolio = 1
        if self.update_statistics.max_time_index_value is not None:
            last_obs = self.get_df_between_dates(
                start_date=self.update_statistics.max_time_index_value
            )
            last_portfolio = last_obs["close"].iloc[0]

            # Keep only new returns
            portfolio = portfolio[portfolio.index > last_obs.index[0]]

        # Apply cumulative returns
        portfolio["close"] = last_portfolio * np.cumprod(portfolio["return"] + 1)
        return portfolio

    def _add_serialized_weights(self, portfolio, weights):
        # Reset index to get 'time_index' as a column
        weights_reset = weights.reset_index()

        # Identify the data columns to pivot
        data_columns = weights_reset.columns.difference(["time_index", "unique_identifier"])

        # Pivot the DataFrame to get a wide format
        weights_pivot = weights_reset.pivot(
            index="time_index", columns="unique_identifier", values=data_columns
        )

        # calculate close metrics
        rebalance_weights_serialized = pd.DataFrame(index=weights_pivot.index)
        for portfolio_column, weights_column in WEIGHTS_TO_PORTFOLIO_COLUMNS.items():
            rebalance_weights_serialized[portfolio_column] = [
                json.dumps(r) for r in weights_pivot[weights_column].to_dict(orient="records")
            ]

        # Join the serialized weights to the portfolio DataFrame
        portfolio = portfolio.join(rebalance_weights_serialized, how="left")

        # Identify rebalance dates where weights are provided
        is_rebalance_date = portfolio["rebalance_weights"].notnull()
        portfolio.loc[is_rebalance_date, "last_rebalance_date"] = portfolio.index[
            is_rebalance_date
        ].astype(str)

        # Forward-fill the serialized weights and last rebalance dates
        rebalance_columns = list(WEIGHTS_TO_PORTFOLIO_COLUMNS.keys())
        portfolio[rebalance_columns] = portfolio[rebalance_columns].ffill()
        portfolio["last_rebalance_date"] = portfolio["last_rebalance_date"].ffill()

        # Drop rows with any remaining NaN values
        return portfolio.dropna()

    def _get_last_weights(self):
        """Deserialize the last rebalance weights"""

        last_obs = self.get_df_between_dates(start_date=self.update_statistics.max_time_index_value)
        if last_obs is None or last_obs.empty:
            return None

        last_weights = {}
        for portfolio_column, weights_column in WEIGHTS_TO_PORTFOLIO_COLUMNS.items():
            last_weights[weights_column] = json.loads(last_obs[portfolio_column].iloc[0])

        last_weights = pd.DataFrame(last_weights)
        last_weights.index.names = ["unique_identifier"]
        last_weights["time_index"] = last_obs.index[0]
        last_weights = last_weights.set_index("time_index", append=True)
        last_weights.index = last_weights.index.reorder_levels(["time_index", "unique_identifier"])
        return last_weights

    def _interpolate_bars_index(
        self,
        new_index: pd.DatetimeIndex,
        unique_identifier_list: list,
        index_freq: str,
        bars_ts: WrapperDataNode,
    ):
        """
        Get interpolated prices for a time index.
        Optionally forward-fills prices to the present if configured.
        """
        prices_config = self.assets_configuration.prices_configuration

        # Determine the end_date for data fetching
        fetch_end_date = new_index.max()

        # If forward-filling is enabled, we still fetch up to the latest signal date,
        # but we will extend the index later.
        raw_prices = bars_ts.get_df_between_dates(
            start_date=new_index.min() - pd.Timedelta(index_freq),
            end_date=fetch_end_date,
            great_or_equal=True,
            less_or_equal=True,
            unique_identifier_list=unique_identifier_list,
        )

        if len(raw_prices) == 0:
            self.logger.info(
                f"No prices data in index interpolation for node {bars_ts.storage_hash}"
            )
            return pd.DataFrame(), pd.DataFrame()

        raw_prices.sort_values("time_index", inplace=True)

        final_index_for_interpolation = new_index
        if prices_config.forward_fill_to_now:
            fill_end_date = datetime.now(pytz.utc)
            last_ts_in_df = raw_prices.index.get_level_values("time_index").max()

            self.logger.info(f"Forward-filling prices from {last_ts_in_df} to {fill_end_date}")
            # Extend the `new_index` to the current time for the fill operation
            pandas_freq = translate_to_pandas_freq(self.portfolio_prices_frequency)
            final_index_for_interpolation = pd.date_range(
                start=new_index.min(), end=fill_end_date, freq=pandas_freq
            )

        interpolated_prices = raw_prices.unstack(["unique_identifier"])

        # Use the potentially extended index for reindexing
        interpolated_prices = interpolated_prices.reindex(
            final_index_for_interpolation, method="ffill"
        )
        interpolated_prices.index.names = ["time_index"]
        interpolated_prices = interpolated_prices.stack(["unique_identifier"])

        return raw_prices, interpolated_prices

    def update(self):
        """
        Updates the portfolio weights based on the latest available data.

        Args:
            latest_value (datetime): The timestamp of the latest available data.

        Returns:
            pd.DataFrame: Updated portfolio values with and without fees and returns.
        """
        self.logger.debug("Starting update of portfolio weights.")
        start_date, end_date = self._calculate_start_end_dates()
        self.logger.debug(f"Update from {start_date} to {end_date}")

        if start_date is None:
            self.logger.info("Start date is None, no update is done")
            return pd.DataFrame()

        # Generate new index for resampling
        new_index, index_freq = self._generate_new_index(
            start_date, end_date, self.rebalancer.calendar
        )

        if len(new_index) == 0:
            self.logger.info("No new portfolio weights to update")
            return pd.DataFrame()

        # Interpolate signal weights to the new index, times where signal is not valid are nan
        signal_weights = self.signal_weights.interpolate_index(new_index).dropna()

        if len(signal_weights) == 0:
            self.logger.info("No signal weights found, no update is done")
            return pd.DataFrame()

        # limit index to last valid signal_weights value, as new signal_weights might be created afterwards (especially important for backtesting)
        new_index = new_index[
            new_index <= signal_weights.index.max() + self.signal_weights.maximum_forward_fill()
        ]

        # Verify the format of signal_weights columns
        expected_columns = ["unique_identifier"]
        assert (
            signal_weights.columns.names == expected_columns
        ), f"signal_weights must have columns named {expected_columns}"

        # get prices for portfolio and interpolated with new_index
        raw_prices, interpolated_prices = self._interpolate_bars_index(
            new_index=new_index,
            bars_ts=self.bars_ts,
            index_freq=index_freq,
            unique_identifier_list=list(
                signal_weights.columns.get_level_values("unique_identifier")
            ),
        )

        if self.update_statistics.max_time_index_value is not None:
            interpolated_prices = interpolated_prices[
                interpolated_prices.index.get_level_values("time_index")
                > self.update_statistics.max_time_index_value
            ]
            signal_weights = signal_weights[
                signal_weights.index > self.update_statistics.max_time_index_value
            ]

        if interpolated_prices.empty:
            raise ValueError(
                "Interpolated Prices are empty. Check if asset prices exist for time window"
            )

        # Calculate rebalanced weights
        weights = self.rebalancer.apply_rebalance_logic(
            signal_weights=signal_weights,
            start_date=start_date,
            prices_df=interpolated_prices,
            end_date=end_date,
            last_rebalance_weights=self._get_last_weights(),
            price_type=self.assets_configuration.price_type,
        )

        weights = self._postprocess_weights(weights)
        if len(weights) == 0:
            self.logger.info("No portfolio weights to update")
            return pd.DataFrame()

        # Calculate portfolio returns
        portfolio_returns = self._calculate_portfolio_returns(weights, raw_prices)
        portfolio = self._calculate_portfolio_values(portfolio_returns)

        # prepare for storage
        if len(portfolio) > 0 and self.update_statistics.max_time_index_value is not None:
            portfolio = portfolio[portfolio.index > self.update_statistics.max_time_index_value]

        portfolio = self._add_serialized_weights(portfolio, weights)
        portfolio = self._resample_portfolio_with_calendar(portfolio)

        # if price comes forn signal then override
        asset_uid_to_override_portfolio_price = (
            self.signal_weights.get_asset_uid_to_override_portfolio_price()
        )
        if asset_uid_to_override_portfolio_price is not None:
            new_portfolio_price = self.bars_ts.get_ranged_data_per_asset(
                range_descriptor={
                    asset_uid_to_override_portfolio_price: {
                        "start_date": portfolio.index.min(),
                        "start_date_operand": ">=",
                    }
                }
            )
            if new_portfolio_price.empty:
                self.logger.error("No Prices on portfolio target asset")
                return pd.DataFrame()

            new_portfolio_price = new_portfolio_price.reset_index("unique_identifier", drop=True)
            union_index = new_portfolio_price.index.union(portfolio.index.unique()).unique()
            new_portfolio_price = new_portfolio_price.reindex(union_index).ffill().bfill()
            new_portfolio_price = new_portfolio_price.reindex(portfolio.index)
            portfolio["calculated_close"] = portfolio["close"]
            portfolio["close"] = new_portfolio_price["close"]
            portfolio["return"] = (
                portfolio["close"].pct_change().fillna(0.0)
            )  # todo get the correct return from previoyus price

        self.logger.info(f"{len(portfolio)} new portfolio values have been calculated.")
        return portfolio

    def get_table_metadata(self) -> ms_client.TableMetaData | None:
        asset = ms_client.PortfolioIndexAsset.get_or_none(
            reference_portfolio__data_node_update__update_hash=self.data_node_update.update_hash
        )
        if asset is not None:
            identifier = asset.unique_identifier
            return ms_client.TableMetaData(
                identifier=identifier,
                description=f"Portfolio strategy for asset {asset.unique_identifier}",
                data_frequency_id=ms_client.DataFrequency.one_d,
            )

    def _resample_portfolio_with_calendar(self, portfolio: pd.DataFrame) -> pd.DataFrame:
        if len(portfolio) == 0:
            return portfolio

        # calendar_schedule = self.rebalancer.calendar.schedule(
        #     portfolio.index.min(), portfolio.index.max()
        # )
        portfolio.index = pd.to_datetime(portfolio.index)
        portfolio["close_time"] = portfolio.index.strftime("%Y-%m-%d %H:%M:%S")
        portfolio = (
            portfolio.resample(pd.to_timedelta(self.portfolio_frequency_to_pandas())).last().ffill()
        )
        # todo: solve cases of portfolio_frequency
        return portfolio

    def portfolio_frequency_to_pandas(self):
        return translate_to_pandas_freq(self.portfolio_prices_frequency)
