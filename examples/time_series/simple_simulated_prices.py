import pytz
import pandas as pd
import datetime
import numpy as np
np.NaN = np.nan
import dotenv
dotenv.load_dotenv('../../.env')

from mainsequence.tdag import TimeSerie, ModelList
from mainsequence.client.models_tdag import DataUpdates
from typing import Union,Optional

class SimulatedPrices(TimeSerie):
    """
    A basic time series example tracking BTC and ETH price updates.

    Simulation periods:
      - If no update statistics are provided, simulate data from 30 days before now
        until 20 days before now.
      - Otherwise, simulate data per asset from one hour after its last update until
        yesterday at midnight (UTC).
    """
    OFFSET_START = datetime.datetime(2024, 1, 1, tzinfo=pytz.utc)

    @TimeSerie._post_init_routines()
    def __init__(self, asset_list: ModelList, *args, **kwargs):
        """
        Initialize the SimpleCryptoFeature time series.

        Args:
            asset_list (ModelList): List of asset objects.
            *args: Additional positional arguments.
            **kwargs: Additional keyword arguments.
        """
        self.asset_list = asset_list
        self.asset_symbols_filter = [a.unique_identifier for a in asset_list]
        super().__init__(*args, **kwargs)

    @staticmethod
    def _get_last_price(obs_df: pd.DataFrame, unique_id: str, fallback: float) -> float:
        """
        Helper method to retrieve the last price for a given unique_id or return 'fallback'
        if unavailable.

        Args:
            obs_df (pd.DataFrame): A DataFrame with multi-index (time_index, unique_identifier).
            unique_id (str): Asset identifier to look up.
            fallback (float): Value to return if the last price cannot be retrieved.

        Returns:
            float: Last observed price or the fallback value.
        """
        # If there's no historical data at all, return fallback immediately
        if obs_df is None:
            return fallback

        # Try to slice for this asset and get the last 'feature_1' value
        try:
            slice_df = obs_df.xs(unique_id, level="unique_identifier")["feature_1"]
            return slice_df.iloc[-1]
        except (KeyError, IndexError):
            # KeyError if unique_id not present, IndexError if slice is empty
            return fallback
    def update(self, update_statistics: DataUpdates)->pd.DataFrame:
        """
        Mocks price updates for assets with stochastic lognormal returns.

        - If update_statistics is empty, simulate data from (now - 30 days) to (now - 20 days),
          floored to the nearest minute.
        - Otherwise, for each unique identifier in update_statistics.update_statistics,
          simulate new data starting one hour after its last update until yesterday at midnight (UTC),
          using the last observed price as the starting price. The last observation is not duplicated.

        Returns:
            pd.DataFrame: A DataFrame with a multi-index (time_index, unique_identifier)
                          and a single column 'feature_1' containing the simulated prices.
        """
        import numpy as np

        initial_price = 100.0
        mu = 0.0  # drift component for lognormal returns
        sigma = 0.01  # volatility component for lognormal returns

        df_list = []

        # Get the latest historical observations; assumed to be a DataFrame with a multi-index:
        # (time_index, unique_identifier) and a column "feature_1" for the last observed price.
        last_observation = self.get_last_observation()
        # Define simulation end: yesterday at midnight (UTC)
        yesterday_midnight = datetime.datetime.now(pytz.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        ) - datetime.timedelta(days=1)
        # Loop over each unique identifier and its last update timestamp.
        for unique_id, last_update in update_statistics.update_statistics.items():
            # Simulation starts one hour after the last update.
            start_time = last_update + datetime.timedelta(hours=1)
            if start_time > yesterday_midnight:
                continue  # Skip if no simulation period is available.
            time_range = pd.date_range(start=start_time, end=yesterday_midnight, freq='H')
            if len(time_range) == 0:
                continue
            # Use the last observed price for the asset as the starting price.
                # Get or fallback to initial_price
            last_price = self._get_last_price(
                obs_df=last_observation,
                unique_id=unique_id,
                fallback=initial_price
            )

            random_returns = np.random.lognormal(mean=mu, sigma=sigma, size=len(time_range))
            simulated_prices = last_price * np.cumprod(random_returns)
            df_asset = pd.DataFrame({unique_id: simulated_prices}, index=time_range)
            df_list.append(df_asset)

        if df_list:
            data = pd.concat(df_list, axis=1)
        else:
            return pd.DataFrame()

        # Reshape the DataFrame into long format with a multi-index.
        data.index.name = "time_index"
        data = data.melt(ignore_index=False, var_name="unique_identifier", value_name="feature_1")
        data = data.set_index("unique_identifier", append=True)
        return data



    def _get_column_metadata(self):
        from mainsequence.client.models_tdag import ColumnMetaData
        columns_metadata = [ColumnMetaData(column_name="feature_1",
                                           dtype="float",
                                           label="Feature 1",
                                           description=(
                                               "Feature calculated like XXX "
                                           )
                                           ),


                            ]
        return columns_metadata


# Mocking DataUpdates and Running the Test
def test_simple_crypto_feature():
    from mainsequence.client import Asset
    from mainsequence.client import MARKETS_CONSTANTS

    # Filter assets for BTCUSDT and ETHUSDT.
    assets = Asset.filter(
        symbol__in=["BTCUSDT", "ETHUSDT"],
        asset_type=MARKETS_CONSTANTS.ASSET_TYPE_CURRENCY_PAIR,
        execution_venue__symbol=MARKETS_CONSTANTS.BINANCE_EV_SYMBOL
    )
    ts = SimulatedPrices(asset_list=ModelList(assets))

    # CASE 1: Run simulation with an empty DataUpdates instance.
    print("=== Simulation using empty DataUpdates ===")
    data_update_df = ts.update(DataUpdates())
    print(data_update_df)

    # Efficiently extract the latest time per unique_identifier using groupby.
    update_dict = (
        data_update_df
        .reset_index()
        .groupby("unique_identifier")["time_index"]
        .max()
        .to_dict()
    )

    # Build a new DataUpdates instance with the extracted update times.
    updates = DataUpdates(update_statistics=update_dict,
                          max_time_index_value=data_update_df.index.get_level_values("time_index").max())
    # CASE 2: Run simulation using the provided update_statistics instance.
    print("=== Simulation using extracted update_statistics ===")
    df_updates = ts.update(updates)
    print(df_updates)


class TAFeature(TimeSerie):
    """
    A time series that derives a technical analysis (TA) feature from simulated crypto prices.

    This class depends on SimulatedCryptoPrices as its base price simulation. It retrieves
    simulated prices over a defined period using the base feature's get_df_between_dates() method,
    then applies a TA indicator (e.g. SMA, RSI, EMA) using pandas-ta.

    The name of the TA feature (ta_feature) and the lookback period (ta_length) are provided as parameters.
    """
    SIMULATION_OFFSET_START=datetime.timedelta(days=50)
    @TimeSerie._post_init_routines()
    def __init__(self, asset_list: ModelList, ta_feature: str, ta_length: int = 14,
                 local_kwargs_to_ignore=["asset_list"],
                 *args, **kwargs):
        """
        Initialize the TA feature time series.

        Args:
            asset_list (ModelList): List of asset objects.
            ta_feature (str): Name of the TA indicator to calculate ("SMA", "RSI", "EMA", etc.).
            ta_length (int, optional): Lookback period for the indicator. Default is 14.
            *args: Additional positional arguments.
            **kwargs: Additional keyword arguments.
        """
        self.asset_list = asset_list
        self.asset_symbols_filter = [a.unique_identifier for a in asset_list]
        self.ta_feature = ta_feature.upper()  # standardize to uppercase
        self.ta_length = ta_length

        # Instantiate the base price simulation.
        # Replace this import with the actual location of SimulatedCryptoPrices if needed.

        self.prices_time_serie = SimulatedPrices(asset_list=asset_list, *args, **kwargs)
        super().__init__(*args, **kwargs)

    def update(self, update_statistics: DataUpdates):
        """
        Retrieves simulated crypto prices and calculates a TA indicator on those prices.
        It does NOT skip if update_statistics is empty. Instead, for each asset:

          1. Determine the asset's last update from update_statistics. If none exists,
             fallback to (now - SIMULATION_OFFSET_START).
          2. Subtract a rolling window (based on ta_length) from the last update to ensure
             enough history is retrieved for the TA calculation.
          3. Build a unique_identifier_range_map so the base feature only returns data
             after (last_update - rolling window).
          4. Pivot to wide format, call the appropriate pandas_ta function, then pivot back.

        Args:
            update_statistics (DataUpdates): Update statistics containing last updates
                                             per unique identifier. If empty, we still
                                             run a fallback flow.

        Returns:
            pd.DataFrame: A DataFrame with a multi-index ("time_index", "unique_identifier")
                          and a single column "feature_1" for the computed TA feature.
        """
        import datetime
        import pytz
        import pandas as pd
        import pandas_ta as ta

        now = datetime.datetime.now(pytz.utc)

        # 1. For each asset, ensure it exists in update_statistics with a fallback date.
        #    This ensures update_statistics.update_statistics is never None for these assets.
        update_statistics = update_statistics.update_assets(
            self.asset_list,
            init_fallback_date=now - self.SIMULATION_OFFSET_START
        )

        # 2. Construct a unique_identifier_range_map using (last_update - rolling_window).
        #    Here, we interpret self.ta_length as days for a basic rolling window approach.
        #    If you prefer hours or another measure, adjust accordingly.
        rolling_window = datetime.timedelta(days=self.ta_length)
        unique_identifier_range_map = {}

        for unique_id, last_update in update_statistics.update_statistics.items():
            # Subtract rolling_window from last_update to ensure enough history
            # is retrieved for TA calculations.
            start_date_for_asset = last_update - rolling_window

            unique_identifier_range_map[unique_id] = {
                "start_date": start_date_for_asset,
                "start_date_operand": ">"
            }

        # 3. Retrieve price data from the base feature. We do not define an end_date here;
        #    the method can fetch up to the present or whatever the base feature defaults to.
        prices_df = self.prices_time_serie.get_df_between_dates(
            start_date=None,
            end_date=None,
            unique_identifier_list=self.asset_symbols_filter,
            unique_identifier_range_map=unique_identifier_range_map
        )

        # If no data is returned, just return an empty DataFrame.
        if prices_df.empty:
            return pd.DataFrame()

        # Pivot the data so each unique_identifier is a column, index = time_index.
        prices_pivot = (
            prices_df
            .reset_index()
            .pivot(index="time_index", columns="unique_identifier", values="feature_1")
        )

        # 4. Dynamically call the appropriate TA function (e.g., sma, rsi, ema) from pandas_ta.
        ta_func = getattr(ta, self.ta_feature.lower(), None)
        ta_results = pd.DataFrame(index=prices_pivot.index)

        for asset_col in prices_pivot.columns:
            col_series = prices_pivot[asset_col]

            if ta_func is not None:
                # Some pandas_ta functions return a DataFrame (e.g. Bollinger Bands) or a Series.
                result = ta_func(col_series, length=self.ta_length)
                if isinstance(result, pd.DataFrame):
                    # If multiple columns are returned, pick the first or rename them as needed.
                    first_col = result.columns[0]
                    ta_results[asset_col] = result[first_col]
                else:
                    ta_results[asset_col] = result
            else:
                # If no matching TA function is found, just use the raw price.
                ta_results[asset_col] = col_series

        # Drop rows that are entirely NaN (common at the start of TA calculations).
        ta_results.dropna(how="all", inplace=True)
        # If your TA function only returns a single column, "all" might be replaced with "any" if needed.

        # Convert from wide to long format with a multi-index.
        ta_long = (
            ta_results
            .reset_index()
            .melt(id_vars="time_index", var_name="unique_identifier", value_name="feature_1")
            .set_index(["time_index", "unique_identifier"])
        )

        return ta_long


# Example test function for the TA dependent feature.
def test_ta_feature_simulated_crypto_prices():
    """
    Test the TAFeatureSimulatedCryptoPrices time series by:
      - Filtering assets.
      - Creating a base simulation of crypto prices.
      - Calculating a TA feature (e.g., SMA) on the simulated prices.
    """
    from mainsequence.client import AssetCurrencyPair
    from mainsequence.client import MARKETS_CONSTANTS

    # Filter assets for BTCUSDT and ETHUSDT.
    assets = AssetCurrencyPair.filter(
        ticker__in=["BTCUSDT", "ETHUSDT"], exchange_code=None,
        execution_venue__symbol=MARKETS_CONSTANTS.MAIN_SEQUENCE_EV
    )
    ts = TAFeature(asset_list=ModelList(assets), ta_feature="SMA", ta_length=14)

    ts.run(debug_mode=True,
           update_tree=True,
           force_update=True,
           update_only_tree=False,
    )
    print("Two asset time serie updated", ts.local_hash_id)
    assets = AssetCurrencyPair.filter(
        ticker__in=["BTCUSDT"], exchange_code=None,
        execution_venue__symbol=MARKETS_CONSTANTS.MAIN_SEQUENCE_EV
    )
    ts = TAFeature(asset_list=ModelList(assets), ta_feature="SMA", ta_length=14)

    ts.run(debug_mode=True,
           update_tree=True,
           force_update=False,
           update_only_tree=False,
           )
    print("one asset time serie updated", ts.local_hash_id)

if __name__ == "__main__":
    test_ta_feature_simulated_crypto_prices()



