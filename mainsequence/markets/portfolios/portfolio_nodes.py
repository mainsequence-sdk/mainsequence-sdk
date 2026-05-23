import ast
import copy
import json
import os
from datetime import datetime
from typing import Any, Union

import numpy as np
import pandas as pd
import pytz

import mainsequence.client as ms_client
import mainsequence.client as msc
from mainsequence.client import Asset, AssetCategory
from mainsequence.client.models_tdag import UpdateStatistics
from mainsequence.markets.portfolios.contrib.prices.data_nodes import (
    get_interpolated_prices_timeseries,
)
from mainsequence.markets.portfolios.data_nodes import (
    PORTFOLIO_INDEX_ASSET_UNIQUE_IDENTIFIER,
    PortfoliosDataNode,
    PortfolioWeights,
    SignalWeights,
    get_or_create_portfolio_index_asset,
)
from mainsequence.tdag.data_nodes import (
    APIDataNode,
    DataNode,
    DataNodeConfiguration,
    WrapperDataNode,
)

from .models import PortfolioConfiguration


class PortfolioFromDFConfig(DataNodeConfiguration):
    portfolio_name: str
    calendar_name: str
    portfolio_description: str | None = None
    builds_from_target_weights: bool = True


class PortfolioStrategyConfig(DataNodeConfiguration):
    portfolio_configuration: PortfolioConfiguration


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

POSITIONS_PORTFOLIO_COLUMNS = {
    "rebalance_positions": "positions_current",
    "rebalance_price": "price_current",
    "volume": "volume_current",
    "positions_at_last_rebalance": "positions_before",
    "price_at_last_rebalance": "price_before",
    "volume_at_last_rebalance": "volume_before",
}

All_PORTFOLIO_COLUMNS_WEIGHTS, All_PORTFOLIO_COLUMNS_POSITIONS = [], []
All_PORTFOLIO_COLUMNS_WEIGHTS.extend(list(WEIGHTS_TO_PORTFOLIO_COLUMNS.keys()))
All_PORTFOLIO_COLUMNS_WEIGHTS.extend(["last_rebalance_date", "close", "return"])

All_PORTFOLIO_COLUMNS_POSITIONS.extend(list(POSITIONS_PORTFOLIO_COLUMNS.keys()))
All_PORTFOLIO_COLUMNS_POSITIONS.extend(["last_rebalance_date", "close", "return"])


class PortfolioFromDF(DataNode):

    def __init__(self, portfolio_from_df_config: PortfolioFromDFConfig, *args, **kwargs):
        self.portfolio_from_df_config = portfolio_from_df_config
        self.portfolio_name = portfolio_from_df_config.portfolio_name
        self.calendar_name = portfolio_from_df_config.calendar_name
        self.portfolio_description = portfolio_from_df_config.portfolio_description
        self.builds_from_target_weights = portfolio_from_df_config.builds_from_target_weights
        super().__init__(config=portfolio_from_df_config, *args, **kwargs)

    def dependencies(self) -> dict[str, Union["DataNode", "APIDataNode"]]:
        return {}

    def get_portfolio_df(self):
        raise NotImplementedError()

    def update(self):
        df = self.get_portfolio_df()
        if df.empty:
            return pd.DataFrame()

        # Ensure columns are known
        if self.builds_from_target_weights:
            assert all(c in All_PORTFOLIO_COLUMNS_WEIGHTS for c in df.columns)
        else:
            assert all(c in All_PORTFOLIO_COLUMNS_POSITIONS for c in df.columns)

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

        target_dict = (
            WEIGHTS_TO_PORTFOLIO_COLUMNS
            if self.builds_from_target_weights
            else POSITIONS_PORTFOLIO_COLUMNS
        )

        for c in target_dict.keys():
            if c not in df.columns:
                raise KeyError(f"Missing expected column '{c}' in DataFrame.")
            df[c] = df[c].apply(lambda v, col=c: _to_json_dict(v, col))

        return df

    def run(self, *args, add_portfolio_to_markets_backend=False, **kwargs):
        super().run(*args, **kwargs)

        ## manualely
        target_portfolio = msc.Portfolio.get_or_none(data_node_update__id=self.data_node_update.id)
        standard_kwargs = dict(
            portfolio_name=self.portfolio_name,
            data_node_update_id=self.data_node_update.id,
            signal_data_node_update_id=None,
            calendar_name=self.calendar_name,
            backtest_table_price_column_name="close",
        )
        if target_portfolio is None:
            target_portfolio, index_asset = msc.Portfolio.create_from_time_series(
                **standard_kwargs,
                portfolio_description=self.portfolio_description,
            )
        else:
            # patch timeserie of portfolio to guaranteed recreation
            target_portfolio.patch(**standard_kwargs)

            index_asset = msc.PortfolioIndexAsset.get(reference_portfolio__id=target_portfolio.id)
            if self.portfolio_description is not None:
                from mainsequence.markets.portfolios.simple_tables import (
                    upsert_portfolio_metadata,
                )

                upsert_portfolio_metadata(
                    portfolio_index_asset=index_asset,
                    description=self.portfolio_description,
                )

        self.target_portfolio = target_portfolio
        self.index_asset = index_asset


class PortfolioStrategy(DataNode):
    """
    Manages the rebalancing of asset weights within a portfolio over time, considering transaction fees
    and rebalancing strategies. Calculates portfolio values and returns while accounting for execution-specific fees.
    """

    def __init__(self, portfolio_strategy_config: PortfolioStrategyConfig, *args, **kwargs):
        """
        Initializes the PortfolioStrategy class with the necessary configurations.

        Args:
            portfolio_build_configuration (PortfolioBuildConfiguration): Configuration for building the portfolio,
                including assets, execution parameters, and backtesting weights.
            is_live (bool): Flag indicating whether the strategy is running in live mode.
        """
        self.portfolio_strategy_config = portfolio_strategy_config
        portfolio_configuration = portfolio_strategy_config.portfolio_configuration
        self.portfolio_configuration = portfolio_configuration
        portfolio_build_configuration = portfolio_configuration.portfolio_build_configuration
        self.portfolio_build_configuration = portfolio_build_configuration
        self.execution_configuration = portfolio_build_configuration.execution_configuration
        self.backtesting_weights_config = (
            portfolio_build_configuration.backtesting_weights_configuration
        )
        self.portfolio_markets_config = portfolio_configuration.portfolio_markets_configuration
        self.commission_fee = self.execution_configuration.commission_fee

        self.portfolio_prices_frequency = portfolio_build_configuration.portfolio_prices_frequency

        self.assets_configuration = portfolio_build_configuration.assets_configuration

        self.portfolio_frequency = (
            self.assets_configuration.prices_configuration.upsample_frequency_id
        )

        self.signal_weights = self.backtesting_weights_config.signal_weights_instance
        if not isinstance(self.signal_weights, SignalWeights):
            raise TypeError(
                "PortfolioStrategy requires signal_weights_instance to inherit "
                "from SignalWeights."
            )

        self.rebalancer = self.backtesting_weights_config.rebalance_strategy_instance

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

        super().__init__(config=portfolio_strategy_config, *args, **kwargs)

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
        progress_values = update_statics_from_dependencies.get_index_progress_leaf_values()
        earliest_last_value = min(progress_values) if progress_values else None

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
        start_date = self._latest_portfolio_time_index_value() or self.OFFSET_START

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

    def _canonical_namespace(self) -> str | None:
        namespace = getattr(self, "hash_namespace", "") or ""
        return namespace or None

    def _canonical_portfolio_weights_node(self) -> PortfolioWeights:
        node = getattr(self, "_portfolio_weights_node", None)
        if node is None:
            node = PortfolioWeights(namespace=self._canonical_namespace())
            self._portfolio_weights_node = node
        return node

    def _canonical_portfolios_node(self) -> PortfoliosDataNode:
        node = getattr(self, "_portfolios_node", None)
        if node is None:
            node = PortfoliosDataNode(namespace=self._canonical_namespace())
            self._portfolios_node = node
        return node

    def _resolve_portfolio_identity(self) -> tuple[Any, Any]:
        portfolio = getattr(self, "target_portfolio", None)
        index_asset = getattr(self, "index_asset", None)
        if portfolio is not None and getattr(index_asset, "unique_identifier", None):
            return portfolio, index_asset

        portfolio, index_asset = get_or_create_portfolio_index_asset(
            self.portfolio_strategy_config,
        )
        self.target_portfolio = portfolio
        self.index_asset = index_asset
        return portfolio, index_asset

    def _portfolio_index_asset_unique_identifier(self) -> str:
        _portfolio, index_asset = self._resolve_portfolio_identity()
        unique_identifier = getattr(index_asset, "unique_identifier", None)
        if not unique_identifier:
            raise ValueError("PortfolioIndexAsset must expose unique_identifier.")
        return str(unique_identifier)

    def _portfolio_description(self) -> str | None:
        front_end_details = getattr(self.portfolio_markets_config, "front_end_details", None)
        if front_end_details is None:
            return None
        description = getattr(front_end_details, "description", None)
        return None if description is None else str(description)

    def _latest_portfolio_time_index_value(self):
        update_statistics = getattr(self, "update_statistics", None)
        if update_statistics is None:
            return None

        portfolio_uid = getattr(self, "_resolved_portfolio_index_asset_unique_identifier", None)
        if portfolio_uid:
            progress = getattr(update_statistics, "index_progress", None) or {}
            progress_value = progress.get(portfolio_uid)
            if isinstance(progress_value, dict):
                return progress_value.get("max") or progress_value.get("time_index")
            if progress_value is not None:
                return progress_value

        return getattr(update_statistics, "max_time_index_value", None)

    def _portfolio_dimension_range_map(
        self,
        *,
        start_date,
        start_date_operand: str = ">=",
    ) -> list[dict]:
        return [
            {
                "coordinate": {
                    PORTFOLIO_INDEX_ASSET_UNIQUE_IDENTIFIER: (
                        self._portfolio_index_asset_unique_identifier()
                    )
                },
                "start_date": start_date,
                "start_date_operand": start_date_operand,
            }
        ]

    def _postprocess_weights(self, weights):
        """
        Prepares backtesting weights DataFrame for storage and sends them to VAM if applicable.

        Args:
            weights (pd.DataFrame): DataFrame of backtesting weights.
            latest_value (datetime): Latest timestamp.

        Returns:
            pd.DataFrame: Prepared backtesting weights.
        """
        latest_value = self._latest_portfolio_time_index_value()
        if latest_value is not None:
            weights = weights[weights.index > latest_value]
        if weights.empty:
            return pd.DataFrame()

        # Reshape and validate the DataFrame
        weights = weights.stack()
        required_columns = ["weights_before", "weights_current", "price_current", "price_before"]
        for col in required_columns:
            assert col in weights.columns, f"Column '{col}' is missing in weights"

        weights = weights.dropna(subset=["weights_current"])
        # Filter again for dates after latest_value
        if latest_value is not None:
            weights = weights[
                weights.index.get_level_values("time_index") > latest_value
            ]

        # Prepare the weights before by using the last weights used for the portfolio and the new weights
        if latest_value is not None:
            last_weights = self._get_last_weights()
            if last_weights is not None and not last_weights.empty:
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
        latest_value = self._latest_portfolio_time_index_value()
        if latest_value is not None:
            last_obs = self._canonical_portfolios_node().get_df_between_dates(
                dimension_range_map=self._portfolio_dimension_range_map(
                    start_date=latest_value,
                    start_date_operand=">=",
                )
            )
            if last_obs is not None and not last_obs.empty:
                last_obs = last_obs.sort_index()
                latest_time_index = last_obs.index.get_level_values("time_index").max()
                last_obs = last_obs[
                    last_obs.index.get_level_values("time_index") == latest_time_index
                ]
                last_portfolio = last_obs["close"].iloc[0]

                # Keep only new returns
                portfolio = portfolio[portfolio.index > latest_time_index]

        # Apply cumulative returns
        portfolio["close"] = last_portfolio * np.cumprod(portfolio["return"] + 1)
        return portfolio

    def _get_last_weights(self):
        """Read the last executed weights from canonical PortfolioWeights."""
        latest_value = self._latest_portfolio_time_index_value()
        if latest_value is None:
            return None

        portfolio_weights_node = self._canonical_portfolio_weights_node()
        last_obs = portfolio_weights_node.get_df_between_dates(
            dimension_range_map=self._portfolio_dimension_range_map(
                start_date=latest_value,
                start_date_operand=">=",
            )
        )
        if last_obs is None or last_obs.empty:
            return None

        last_obs = last_obs.sort_index()
        latest_time_index = last_obs.index.get_level_values("time_index").max()
        last_weights = last_obs[
            last_obs.index.get_level_values("time_index") == latest_time_index
        ].copy()
        if PORTFOLIO_INDEX_ASSET_UNIQUE_IDENTIFIER in last_weights.index.names:
            last_weights = last_weights.droplevel(PORTFOLIO_INDEX_ASSET_UNIQUE_IDENTIFIER)

        return last_weights.rename(columns={"weight": "weights_current"})

    def _interpolate_bars_index(
        self,
        new_index: pd.DatetimeIndex,
        unique_identifiers: list,
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
            dimension_filters={"unique_identifier": unique_identifiers},
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
        self._last_canonical_weights_frame = pd.DataFrame()
        self._last_canonical_portfolio_values_frame = pd.DataFrame()
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
            unique_identifiers=list(signal_weights.columns.get_level_values("unique_identifier")),
        )

        latest_value = self._latest_portfolio_time_index_value()
        if latest_value is not None:
            interpolated_prices = interpolated_prices[
                interpolated_prices.index.get_level_values("time_index") > latest_value
            ]
            signal_weights = signal_weights[signal_weights.index > latest_value]

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

        # prepare canonical value rows
        if len(portfolio) > 0 and latest_value is not None:
            portfolio = portfolio[portfolio.index > latest_value]

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
        self._last_canonical_weights_frame = weights
        self._last_canonical_portfolio_values_frame = portfolio
        return portfolio

    def get_table_metadata(self) -> ms_client.TableMetaData | None:
        return None

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

    def run(
        self,
        debug_mode: bool = True,
        *,
        update_tree: bool = True,
        force_update: bool = False,
        update_only_tree: bool = False,
        remote_scheduler: object | None = None,
        override_update_stats: UpdateStatistics | None = None,
        add_portfolio_to_markets_backend: bool = False,
    ):
        """
        Run the portfolio workflow through canonical VFB DataNodes.

        `PortfolioStrategy` no longer persists its own per-portfolio output
        table. It orchestrates calculation, then writes executed weights to
        `PortfolioWeights` and portfolio values to `PortfoliosDataNode`.
        """
        if add_portfolio_to_markets_backend:
            raise ValueError(
                "add_portfolio_to_markets_backend is no longer supported. "
                "Canonical VFB creates or resolves the PortfolioIndexAsset from "
                "the portfolio configuration hash."
            )

        if update_tree:
            self.signal_weights.run(
                debug_mode=debug_mode,
                update_tree=True,
                force_update=force_update,
                remote_scheduler=remote_scheduler,
            )
            self.bars_ts.run(
                debug_mode=debug_mode,
                update_tree=True,
                force_update=force_update,
                remote_scheduler=remote_scheduler,
            )

        if update_only_tree:
            return None

        _portfolio, index_asset = self._resolve_portfolio_identity()
        portfolio_uid = str(index_asset.unique_identifier)
        self._resolved_portfolio_index_asset_unique_identifier = portfolio_uid

        portfolio_weights_node = self._canonical_portfolio_weights_node()
        portfolios_node = self._canonical_portfolios_node()

        portfolio_weights_node.ensure_storage_ready()
        portfolios_node.ensure_storage_ready()

        self.update_statistics = override_update_stats or portfolios_node.get_update_statistics()
        if self.update_statistics is None:
            self.update_statistics = UpdateStatistics()

        portfolio_values = self.update()
        weights = getattr(self, "_last_canonical_weights_frame", pd.DataFrame())

        results = {
            "portfolio_weights": None,
            "portfolio_values": None,
        }
        if weights is not None and not weights.empty:
            portfolio_weights_node.set_weights_frame(
                weights,
                portfolio_index_asset_unique_identifier=portfolio_uid,
                portfolio_configuration=self.portfolio_strategy_config,
                portfolio_index_asset=index_asset,
                portfolio_description=self._portfolio_description(),
            )
            results["portfolio_weights"] = portfolio_weights_node.run(
                debug_mode=debug_mode,
                update_tree=False,
                force_update=force_update,
                remote_scheduler=remote_scheduler,
            )

        if portfolio_values is not None and not portfolio_values.empty:
            portfolios_node.set_portfolio_values_frame(
                portfolio_values,
                portfolio_index_asset_unique_identifier=portfolio_uid,
                portfolio_configuration=self.portfolio_strategy_config,
                portfolio_index_asset=index_asset,
                portfolio_description=self._portfolio_description(),
            )
            results["portfolio_values"] = portfolios_node.run(
                debug_mode=debug_mode,
                update_tree=False,
                force_update=force_update,
                remote_scheduler=remote_scheduler,
            )

        return results
