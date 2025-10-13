import copy
import datetime
import json
from typing import Any, Union

# Imports should be at the top of the file
import numpy as np
import pandas as pd
import pytz
from pydantic import BaseModel, Field
from sklearn.linear_model import ElasticNet

import mainsequence.client as msc
from mainsequence.client.models_tdag import ColumnMetaData, UpdateStatistics
from mainsequence.tdag import APIDataNode, DataNode, WrapperDataNode

np.NaN = np.nan  # Fix for a pandas-ta compatibility issue
MARKET_TIME_SERIES_UNIQUE_IDENTIFIER_CATEGORY_PRICES = "simulated_prices_from_category"
TEST_TRANSLATION_TABLE_UID = "test_translation_table"

import base64


class SimulatedPricesManager:

    def __init__(self, owner: DataNode):
        self.owner = owner

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
        if obs_df.empty:
            return fallback

        # Try to slice for this asset and get the last 'close' value
        try:
            slice_df = obs_df.xs(unique_id, level="unique_identifier")["close"]
            return slice_df.iloc[-1]
        except (KeyError, IndexError):
            # KeyError if unique_id not present, IndexError if slice is empty
            return fallback

    def update(self) -> pd.DataFrame:
        """
        Mocks price updates for assets with stochastic lognormal returns.
        For each asset, simulate new data starting one hour after its last update
         until yesterday at 00:00 UTC, using the last observed price as the seed.
         The last observation is not duplicated.
         Returns:
             pd.DataFrame: A DataFrame with a multi-index (time_index, unique_identifier)
                           and a single column 'close' containing the simulated prices.
        """
        import numpy as np

        initial_price = 100.0
        mu = 0.0  # drift component for lognormal returns
        sigma = 0.01  # volatility component for lognormal returns

        df_list = []
        update_statistics = self.owner.update_statistics
        # Get the latest historical observations; assumed to be a DataFrame with a multi-index:
        # (time_index, unique_identifier) and a column "close" for the last observed price.
        range_descriptor = update_statistics.get_update_range_map_great_or_equal()
        last_observation = self.owner.get_ranged_data_per_asset(range_descriptor=range_descriptor)
        # Define simulation end: yesterday at midnight (UTC)
        yesterday_midnight = datetime.datetime.now(pytz.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        ) - datetime.timedelta(days=1)
        # Loop over each unique identifier and its last update timestamp.
        for asset in update_statistics.asset_list:
            # Simulation starts one hour after the last update.
            start_time = update_statistics.get_asset_earliest_multiindex_update(
                asset=asset
            ) + datetime.timedelta(hours=1)
            if start_time > yesterday_midnight:
                continue  # Skip if no simulation period is available.
            time_range = pd.date_range(start=start_time, end=yesterday_midnight, freq="D")
            if len(time_range) == 0:

                continue
            # Use the last observed price for the asset as the starting price (or fallback).
            last_price = self._get_last_price(
                obs_df=last_observation, unique_id=asset.unique_identifier, fallback=initial_price
            )

            random_returns = np.random.lognormal(mean=mu, sigma=sigma, size=len(time_range))
            simulated_prices = last_price * np.cumprod(random_returns)
            df_asset = pd.DataFrame({asset.unique_identifier: simulated_prices}, index=time_range)
            df_list.append(df_asset)

        if df_list:
            data = pd.concat(df_list, axis=1)
        else:
            return pd.DataFrame()

        # Reshape the DataFrame into long format with a multi-index.
        data.index.name = "time_index"
        data = data.melt(ignore_index=False, var_name="unique_identifier", value_name="close")
        data = data.set_index("unique_identifier", append=True)
        return data

    def get_column_metadata(self):
        from mainsequence.client.models_tdag import ColumnMetaData

        columns_metadata = [
            ColumnMetaData(
                column_name="close",
                dtype="float",
                label="Close ",
                description=("Simulated close price"),
            ),
        ]
        return columns_metadata


class SingleIndexTS(DataNode):
    """
    A simple time series with a single index, generating random numbers daily.
    This serves as a basic, non-asset-based example.
    """

    OFFSET_START = datetime.datetime(2024, 1, 1, tzinfo=pytz.utc)

    def dependencies(self) -> dict[str, Union["DataNode", "APIDataNode"]]:
        return {}

    def update(self):
        today_utc = datetime.datetime.now(tz=pytz.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        update_statistics = self.update_statistics
        if update_statistics.max_time_index_value is None:
            start_date = self.OFFSET_START
        else:
            last_time = update_statistics.max_time_index_value
            start_date = last_time + datetime.timedelta(days=1)

        if start_date > today_utc:
            return pd.DataFrame()

        num_days = (today_utc - start_date).days + 1
        time_index = pd.date_range(start=start_date, periods=num_days, freq="D", tz=pytz.utc)
        random_values = np.random.rand(num_days)

        df = pd.DataFrame({"random_number": random_values}, index=time_index)
        df.index.name = "time_index"
        return df

    def get_column_metadata(self):
        """
        Add MetaData information to the DataNode Table
        Returns:

        """
        from mainsequence.client.models_tdag import ColumnMetaData

        columns_metadata = [
            ColumnMetaData(
                column_name="random_number",
                dtype="float",
                label="Random Number ",
                description=("A random number with no meaning whatsoever"),
            ),
        ]
        return columns_metadata

    def get_table_metadata(self) -> msc.TableMetaData:
        """ """
        MARKET_TIME_SERIES_UNIQUE_IDENTIFIER = "simple_time_serie_example"
        mts = msc.TableMetaData(
            unique_identifier=MARKET_TIME_SERIES_UNIQUE_IDENTIFIER,
            data_frequency_id=msc.DataFrequency.one_d,
            description="This is a simulated prices time serie from asset category",
        )

        return mts


class PriceSimulConfig(BaseModel):

    asset_list: list[msc.AssetMixin] = Field(
        ...,
        title="Asset List",
        description="List of assets to simulate",
        ignore_from_storage_hash=True,
    )


class SimulatedPrices(DataNode):
    """
    Simulates price updates for a specific list of assets provided at initialization.
    """

    OFFSET_START = datetime.datetime(2024, 1, 1, tzinfo=pytz.utc)

    def __init__(self, simulation_config: PriceSimulConfig, *args, **kwargs):
        """
        Args:
            asset_list (ModelList): List of asset objects.
            *args: Additional positional arguments.
            **kwargs: Additional keyword arguments.
        """
        self.asset_list = simulation_config.asset_list
        self.asset_symbols_filter = [a.unique_identifier for a in self.asset_list]
        super().__init__(*args, **kwargs)

    def dependencies(self) -> dict[str, Union["DataNode", "APIDataNode"]]:
        return {}

    def update(self):
        update_manager = SimulatedPricesManager(self)
        df = update_manager.update()
        return df

    def get_asset_list(self):
        return self.asset_list

    def get_column_metadata(self):
        """
        Add MetaData information to the DataNode Table
        Returns:

        """
        from mainsequence.client.models_tdag import ColumnMetaData

        columns_metadata = [
            ColumnMetaData(
                column_name="close",
                dtype="float",
                label="Close",
                description=("Simulated Close Price"),
            ),
        ]
        return columns_metadata

    def get_table_metadata(self) -> msc.TableMetaData:
        """
        REturns the market time serie unique identifier, assets to append , or asset to overwrite
        Returns:

        """

        mts = msc.TableMetaData(
            identifier="simulated_prices",
            data_frequency_id=msc.DataFrequency.one_d,
            description="This is a simulated prices time serie from asset category",
        )

        return mts


class Volatility(DataNode):
    _ARGS_IGNORE_IN_STORAGE_HASH = ["asset_list"]

    def __init__(self, asset_list, rolling_window, *args, **kwargs):
        self.asset_list = asset_list
        self.rolling_window = rolling_window
        self.prices = SimulatedPrices(asset_list=self.asset_list)
        super().__init__(*args, **kwargs)

    def dependencies(self) -> dict[str, Union["DataNode", "APIDataNode"]]:
        return {"prices": self.prices}

    def get_asset_list(self):
        return self.asset_list

    def update(self):
        prices = self.prices
        df = prices.get_df_between_dates()
        # Step 1: Pivot to wide
        price_wide = df["close"].unstack(level="unique_identifier")

        # Step 2: Compute percent returns
        returns = price_wide.pct_change()
        # Step 3: 20-period rolling volatility (non-annualized)
        rolling_vol = returns.rolling(window=20).std()

        # Step 4 (optional): Annualize if using hourly data → ~252*24 = 6048 trading hours/year
        rolling_vol_annualized = rolling_vol * np.sqrt(6048)
        rolling_vol_annualized = rolling_vol_annualized.unstack()
        rolling_vol_annualized.index = rolling_vol_annualized.index.swaplevel()
        return rolling_vol_annualized.dropna().to_frame("volatility")


class CategorySimulatedPrices(DataNode):
    """
    Simulates price updates for all assets belonging to a specified category.
    This demonstrates using a hook (`get_asset_list`) to dynamically define the asset universe.
    It also shows a dependency on another DataNode (`SingleIndexTS`).
    """

    OFFSET_START = datetime.datetime(2024, 1, 1, tzinfo=pytz.utc)
    _ARGS_IGNORE_IN_STORAGE_HASH = ["asset_category_id"]

    def __init__(self, asset_category_id: str, *args, **kwargs):
        """
        Initialize the SimpleCryptoFeature time series.

        Args:
            asset_list (ModelList): List of asset objects.
            *args: Additional positional arguments.
            **kwargs: Additional keyword arguments.
        """
        self.asset_category_id = asset_category_id

        # this time serie has a dependency to include dependencies is just enough to declared them in the init method
        self.simple_ts = SingleIndexTS()

        super().__init__(*args, **kwargs)

    def dependencies(self) -> dict[str, Union["DataNode", "APIDataNode"]]:
        return {"simple_ts": self.simple_ts}

    def get_asset_list(self):
        """[Hook] Dynamically fetches the list of assets from a category."""

        asset_category = msc.AssetCategory.get(unique_identifier=self.asset_category_id)
        asset_list = msc.Asset.filter(id__in=asset_category.assets)
        return asset_list

    def update(self):
        """
        Simulates prices and then adds a random number from its dependency,
        demonstrating how to combine data from multiple sources.
        """
        update_manager = SimulatedPricesManager(self)
        data = update_manager.update(self.update_statistics)
        if data.empty:
            return pd.DataFrame()
        # an example of a dependencies calls
        historical_random_data = self.simple_ts.get_df_between_dates(
            start_date=self.update_statistics.max_time_index_value
        )
        number = historical_random_data["random_number"].iloc[-1]
        return data + number

    def get_column_metadata(self):
        """
        Add MetaData information to the DataNode Table
        Returns:

        """
        from mainsequence.client.models_tdag import ColumnMetaData

        columns_metadata = [
            ColumnMetaData(
                column_name="close",
                dtype="float",
                label="Close",
                description=("Simulated Close Price"),
            ),
        ]
        return columns_metadata

    def get_table_metadata(self) -> msc.TableMetaData:
        """ """
        meta = msc.TableMetaData(
            identifier=MARKET_TIME_SERIES_UNIQUE_IDENTIFIER_CATEGORY_PRICES,
            data_frequency_id=msc.DataFrequency.one_d,
            description="This is a simulated prices time serie from asset category",
        )

        return meta

    def run_post_update_routines(self, error_on_last_update: bool) -> None:
        """
        In this example we will use the post init routines to build an  Asset Translation Table that points
        prices to this prices
        Args:
            error_on_last_update:
            update_statistics:

        Returns:

        """
        translation_table = msc.AssetTranslationTable.get_or_none(
            unique_identifier=TEST_TRANSLATION_TABLE_UID
        )

        rules = [
            msc.AssetTranslationRule(
                asset_filter=msc.AssetFilter(
                    execution_venue_symbol=msc.MARKETS_CONSTANTS.MAIN_SEQUENCE_EV,
                    security_type=msc.MARKETS_CONSTANTS.FIGI_SECURITY_TYPE_COMMON_STOCK,
                ),
                markets_time_serie_unique_identifier=MARKET_TIME_SERIES_UNIQUE_IDENTIFIER_CATEGORY_PRICES,
                target_execution_venue_symbol=msc.MARKETS_CONSTANTS.MAIN_SEQUENCE_EV,
            )
        ]
        rules_serialized = [r.model_dump() for r in rules]

        if translation_table is None:
            translation_table = msc.AssetTranslationTable.create(
                unique_identifier=TEST_TRANSLATION_TABLE_UID,
                rules=rules_serialized,
            )
        else:
            translation_table.add_rules(rules)


# Mocking UpdateStatistics and Running the Test


class FeatureStoreTA(DataNode):
    """
    A derived time series that calculates a technical analysis feature from another price series.
    """

    _ARGS_IGNORE_IN_STORAGE_HASH = ["asset_list", "ta_feature_config"]

    def __init__(self, asset_list: list[msc.Asset], ta_feature_config: list[dict], *args, **kwargs):
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
        self.ta_feature_config = ta_feature_config

        # Instantiate the base price simulation.
        # Replace this import with the actual location of SimulatedCryptoPrices if needed.

        self.prices_time_serie = SimulatedPrices(asset_list=asset_list, *args, **kwargs)
        super().__init__(*args, **kwargs)

    def dependencies(self) -> dict[str, Union["DataNode", "APIDataNode"]]:
        return {
            "prices_time_serie": self.prices_time_serie,
        }

    @staticmethod
    def encode_col(orig_dict: str) -> str:
        json_text = json.dumps(orig_dict, sort_keys=True)

        b64 = base64.urlsafe_b64encode(json_text.encode("utf-8")).decode("ascii")
        # strip trailing = padding (optional)
        return "f_" + b64.rstrip("=")

    @staticmethod
    def decode_col(safe: str) -> str:
        b64 = safe.removeprefix("f_")
        pad = len(b64) % 4
        if pad:
            b64 += "=" * (4 - pad)
        json_text = base64.urlsafe_b64decode(b64).decode("utf-8")
        return json.loads(json_text)

    def update(self) -> pd.DataFrame:
        """
        [Core Logic] Calculates a technical analysis feature based on a dependent price series.

        This method serves as a template for creating a derived time series. It follows a
        three-step process:
        1.  **Prepare a request**: It determines the required historical data for each asset,
            ensuring a sufficient lookback window for the TA calculation.
        2.  **Fetch data**: It retrieves the necessary price data from its dependency
            (self.prices_time_serie) using a single, efficient call.
        3.  **Calculate and reshape**: It transforms the data, applies the specified TA
            indicator, and formats the result into the required long-format DataFrame.



        Returns:
            A DataFrame with a ("time_index", "unique_identifier") multi-index and a
            single column containing the calculated TA feature values.
        """
        SIMULATION_OFFSET_START = datetime.timedelta(days=50)
        import pandas_ta as ta

        # when using more than 2 indices in a multiindex table it is extremely importnat to make all the filters
        # accross all levels to keep consistency in the update
        self.update_statistics.filter_assets_by_level(
            level=2, filters=[json.dumps(c) for c in self.ta_feature_config]
        )

        # --- Step 1: Prepare the Data Request ---
        # We need to fetch not just new data, but also a "lookback" window of older data
        # to ensure the TA indicator can be calculated correctly from the very first new point.
        rolling_window = datetime.timedelta(
            days=np.max([a["length"] for a in self.ta_feature_config]).item() + 1
        )  # Fetch the max of the required features
        asset_range_descriptor = {}

        for asset in self.update_statistics.asset_list:
            # For each asset, find its last update time. If it's a new asset,
            # fall back to the global offset defined on the class.
            last_update = (
                self.update_statistics.get_asset_earliest_multiindex_update(asset)
                - SIMULATION_OFFSET_START
            )

            # The start date for our data request is the last update time minus the lookback window.
            start_date_for_asset = last_update - rolling_window
            asset_range_descriptor[asset.unique_identifier] = {
                "start_date": start_date_for_asset,
                "start_date_operand": ">=",  # Use ">=" to include the start of the window
            }

        # --- Step 2: Fetch Data From Dependency ---
        # This is a key pattern for derived features. We use a data access method from the base
        # DataNode class (via the DataAccessMixin) to query our dependency.
        #
        # `get_ranged_data_per_asset` is highly efficient because it takes a dictionary
        # that specifies a *different* start date for each asset, allowing us to get all
        # the data we need in a single call.
        prices_df = self.prices_time_serie.get_ranged_data_per_asset(
            range_descriptor=asset_range_descriptor
        )

        if prices_df.empty:
            self.logger.warning("Base price data was empty for the requested range.")
            return pd.DataFrame()

        # --- Step 3: Calculate the TA Feature and Reshape the Data ---
        # The pandas-ta library expects data in a "wide" format, where each column
        # is a different time series. We pivot the data to create this structure.
        prices_pivot = prices_df.reset_index().pivot(
            index="time_index", columns="unique_identifier", values="close"
        )

        # wide shape: (time × asset) rows × M features columns

        all_features = []
        for feature_config in self.ta_feature_config:
            features_df = pd.DataFrame(index=prices_pivot.stack(dropna=False).index)
            f_conf = copy.deepcopy(feature_config)
            feature_name = json.dumps(f_conf)
            feature_kind = f_conf.pop("kind").lower()
            func = getattr(ta, feature_kind)
            out = prices_pivot.apply(lambda col: func(col, **feature_config))

            features_df[feature_name] = out.stack(dropna=False)
            all_features.append(features_df)

        all_features = pd.concat(all_features, axis=1)
        all_features.columns = [self.encode_col(c) for c in all_features.columns]
        return all_features


from pydantic import BaseModel


class ModelConfiguration(BaseModel):
    """
    Abstract base for any model’s hyperparameter/configuration object.
    Subclasses must implement build_model() to return a fresh model instance.
    """

    def build_model(self) -> Any:
        raise NotImplementedError


class ElasticNetConfiguration(ModelConfiguration):
    """
    Configuration for sklearn.linear_model.ElasticNet.
    All fields map 1:1 to its __init__ parameters.
    """

    alpha: float = 1.0
    l1_ratio: float = 0.5
    fit_intercept: bool = True
    normalize: bool = False
    precompute: bool = False
    max_iter: int = 1000
    tol: float = 1e-4
    selection: str = "cyclic"

    def build_model(self) -> ElasticNet:
        """Instantiate sklearn’s ElasticNet with these hyperparameters."""
        return ElasticNet(
            alpha=self.alpha,
            l1_ratio=self.l1_ratio,
            fit_intercept=self.fit_intercept,
            normalize=self.normalize,
            precompute=self.precompute,
            max_iter=self.max_iter,
            tol=self.tol,
            selection=self.selection,
        )


def _prepare_model_data(
    feature_ts: DataNode, prices_ts: DataNode, update_statistics: UpdateStatistics
) -> pd.DataFrame:
    """
    Helper to fetch, align, and prepare feature and target data for modeling.

    1. Fetches feature and price data from dependencies.
    2. Pivots features from long to wide format.
    3. Computes next-day returns from prices.
    4. Joins features and returns into a single DataFrame.

    Returns:
        A cleaned DataFrame with features and 'return_1d' as columns,
        indexed by ('time_index', 'unique_identifier'). Returns an empty
        DataFrame if prerequisites are not met.
    """
    start = getattr(prices_ts, "OFFSET_START", datetime.datetime(2024, 1, 1, tzinfo=pytz.utc))
    range_descriptor = {
        a.unique_identifier: {"start_date": start, "start_date_operand": ">="}
        for a in update_statistics.asset_list
    }

    # Fetch raw data
    feats_df = feature_ts.get_ranged_data_per_asset(range_descriptor=range_descriptor)
    prices_df = prices_ts.get_ranged_data_per_asset(range_descriptor=range_descriptor)

    if feats_df.empty or prices_df.empty:
        return pd.DataFrame()

    # Compute next-day returns
    price_ser = (
        prices_df.reset_index()
        .rename(columns={"close": "price"})
        .set_index(["time_index", "unique_identifier"])["price"]
    )
    returns = (
        price_ser.groupby(level="unique_identifier")
        .pct_change()
        .shift(-1)  # Align so return_1d at t = (p_{t+1}/p_t - 1)
        .to_frame(name="return_1d")
    )

    # Join features + target and drop rows with missing values
    return feats_df.join(returns, how="inner").dropna()


class ModelTrainTimeSerie(DataNode):
    """
    DataNode dedicated solely to retraining the model at the configured frequency.

    Dependencies:
      - TAFeature
      - SimulatedPrices

    Args:
        asset_list: assets to train on
        ta_feature_config: TA‐indicator specs
        model_config: builds the estimator
        window_size: rows in rolling window
        retrain_config: when/how often to retrain
    """

    def __init__(
        self,
        asset_list: list[msc.Asset],
        ta_feature_config: list[dict[str, Any]],
        model_config: Any,  # ModelConfiguration
        window_size: int,
        retrain_config_days: int,
        *args,
        **kwargs,
    ):
        self.asset_list = asset_list
        self.ta_feature_config = ta_feature_config
        self.model_config = model_config
        self.window_size = window_size
        self.retrain_config_days = retrain_config_days

        # downstream TS for features & prices
        self.feature_ts = FeatureStoreTA(
            *args, asset_list=asset_list, ta_feature_config=ta_feature_config, **kwargs
        )
        self.prices_ts = SimulatedPrices(*args, asset_list=asset_list, **kwargs)

        super().__init__(*args, **kwargs)

    def dependencies(self) -> dict[str, DataNode]:
        return {
            "feature_ts": self.feature_ts,
            "prices_ts": self.prices_ts,
        }

    def update(
        self,
    ) -> pd.DataFrame:
        """
        Retrain once per retrain_frequency.  Returns a DataFrame of
        model coefficients + intercept for each asset at the retrain timestamp.
        """
        data = _prepare_model_data(self.feature_ts, self.prices_ts, self.update_statistics)
        if data.empty:
            return pd.DataFrame()

        feature_cols = [c for c in data.columns if c != "return_1d"]

        now = datetime.datetime.now(pytz.utc).replace(
            microsecond=0, minute=0, second=0, tzinfo=pytz.utc
        )
        records = []
        for uid, grp in data.groupby(level="unique_identifier"):

            last_retrain = self.update_statistics.get_last_update_index_2d(uid)
            if (now - last_retrain).days < self.retrain_config_days:
                continue

            df = grp.droplevel("unique_identifier").sort_index()
            if len(df) < self.window_size + 1:
                continue

            # train on most recent window
            win = df.iloc[-self.window_size :]
            X = win[feature_cols].to_numpy()
            y = win["return_1d"].to_numpy()

            job_id = self._handle_model_train(X, y, uid)
            retrain_time = win.index[-1]  # use last data timestamp, not now
            records.append((retrain_time, uid, job_id))

        if not records:
            return pd.DataFrame()

        # build MultiIndex:
        df = pd.DataFrame(
            records, columns=["last_timestamp_on_train", "unique_identifier", "job_id"]
        )
        df["last_timestamp_on_train"] = df["last_timestamp_on_train"].apply(lambda x: x.timestamp())
        df["time_index"] = now
        df = df.set_index(["time_index", "unique_identifier"])
        return df

    def _handle_model_train(self, X, y, uid):
        """
        Handle Model Train This could be in a different Job in another cloud service
        Ideally we get the information regarding the model via ML Flow
        Args:
            X:
            y:
            uid:

        Returns:

        """
        # model = self.model_config.build_model()
        # model.fit(X, y)

        # record coefficients + intercept
        # Serialize and upload as Artifact

        # with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as tmp:
        #     pickle.dump(model, tmp)
        #     tmp.flush()
        #     artifact = msc.Artifact.upload_file(
        #         filepath=tmp.name,
        #         name=f"{self.__class__.__name__}_{uid}_{now.isoformat()}",
        #         created_by_resource_name=self.__class__.__name__,
        #         bucket_name="ModelArtifacts"
        #     )
        return 1

    def get_table_metadata(
        self,
    ) -> msc.TableMetaData:
        return msc.TableMetaData(
            identifier="model_retrain",
            data_frequency_id=None,
            description="Model coefficients retrained at specified frequency",
        )


class RollingModelPrediction(DataNode):
    """
        Predicts the next-day return via a rolling ElasticNet regression on TA features.

        Dependencies:
          - TAFeature: configured technical indicators.
          - SimulatedPrices: base price series for return calculation.

        Args:
            asset_list: List of msc.Asset to include.
            ta_feature_config: List of dicts, each with keys "kind" and parameters
                               for the TA indicator (e.g. {"kind":"SMA","length":14}).
            elastic_net_config: Dict of parameters to pass into sklearn.linear_model.ElasticNet.
            window_size: Number of past observations to use in each rolling regression.
    g"""

    def __init__(
        self,
        asset_list: list[msc.Asset],
        ta_feature_config: list[dict[str, Any]],
        model_config: ModelConfiguration,
        window_size: int,
        retrain_config_days: int,
        *args,
        **kwargs,
    ):
        # Core configuration
        self.asset_list = asset_list
        self.ta_feature_config = ta_feature_config
        self.model_config = model_config
        self.window_size = window_size

        # Declare dependencies by instantiating them as attributes
        self.feature_ts = FeatureStoreTA(
            *args, asset_list=asset_list, ta_feature_config=ta_feature_config, **kwargs
        )
        self.prices_ts = SimulatedPrices(*args, asset_list=asset_list, **kwargs)

        # even more prices from a DataNode not in the project using APIDataNode

        self.external_prices = APIDataNode.build_from_identifier(
            identifier=MARKET_TIME_SERIES_UNIQUE_IDENTIFIER_CATEGORY_PRICES
        )
        translation_table = msc.AssetTranslationTable.get(
            unique_identifier=TEST_TRANSLATION_TABLE_UID
        )
        self.translated_prices_ts = WrapperDataNode(translation_table=translation_table)
        # retrainer DataNode
        self.model_train_ts = ModelTrainTimeSerie(
            *args,
            asset_list=asset_list,
            ta_feature_config=ta_feature_config,
            model_config=model_config,
            window_size=window_size,
            retrain_config_days=retrain_config_days,
            **kwargs,
        )

        super().__init__(*args, **kwargs)

    def dependencies(self) -> dict[str, Union["DataNode", "APIDataNode"]]:
        return {
            "prices_ts": self.prices_ts,
            "feature_ts": self.feature_ts,
            "api_prices": self.external_prices,
            "translated_prices_ts": self.translated_prices_ts,
            "model_train_ts": self.model_train_ts,
        }

    def update(
        self,
    ) -> pd.DataFrame:
        """
        For each asset, fits a rolling-window ElasticNet to predict the return
        from today to tomorrow. X = today's TA features; y = tomorrow's 1d return.
        """
        # Build range descriptors from the earliest needed date
        data = _prepare_model_data(self.feature_ts, self.prices_ts, self.update_statistics)
        if data.empty:
            return pd.DataFrame()

        # 6) Loop per asset via groupby; drop the UID level so asset_df is purely wide
        trainer = None  # ElasticNetTrainer(model_config=self.model_config)
        records: list = []
        for uid, grp in data.groupby(level="unique_identifier"):
            asset_df = grp.droplevel("unique_identifier").sort_index()
            # asset_df now has columns = [<feature1>, <feature2>, ..., "return_1d"]

            # need at least window_size + 1 rows to train & predict
            if len(asset_df) < self.window_size + 1:
                continue

            feature_cols = [c for c in asset_df.columns if c != "return_1d"]
            for i in range(self.window_size, len(asset_df)):
                train_win = asset_df.iloc[i - self.window_size : i]
                X_train = train_win[feature_cols].to_numpy()  # shape (window_size, n_features)
                y_train = train_win["return_1d"].to_numpy()  # shape (window_size,)

                # Fit ElasticNet with user‐supplied hyperparams
                model = trainer.train(X_train, y_train)

                # Predict using today's features
                X_today = asset_df.iloc[[i]][feature_cols].to_numpy()
                pred = model.predict(X_today)[0]

                ts = asset_df.index[i]
                records.append((ts, uid, pred))

        # 7) Assemble output
        if not records:
            return pd.DataFrame()

        idx = pd.MultiIndex.from_tuples(
            [(t, u) for t, u, _ in records], names=["time_index", "unique_identifier"]
        )
        return pd.DataFrame({"predicted_return": [v for _, _, v in records]}, index=idx)

    def get_column_metadata(self) -> list[ColumnMetaData]:
        return [
            ColumnMetaData(
                column_name="predicted_return",
                dtype="float",
                label="Predicted Next-Day Return",
                description="Return from today to tomorrow predicted by rolling ElasticNet",
            )
        ]

    def get_table_metadata(
        self,
    ) -> msc.TableMetaData:
        return msc.TableMetaData(
            identifier="next_day_elastic_net",
            data_frequency_id=msc.DataFrequency.one_d,
            description="Next-day return via rolling ElasticNet on TA indicators",
        )


class LivePrediction(DataNode):
    def __init__(
        self,
        asset_list: list[msc.Asset],
        ta_feature_config: list[dict[str, Any]],
        model_config: ModelConfiguration,
        window_size: int,
        retrain_config_days: int,
        *args,
        **kwargs,
    ):
        # Core configuration
        self.asset_list = asset_list
        self.ta_feature_config = ta_feature_config
        self.model_config = model_config
        self.window_size = window_size

        # Declare dependencies by instantiating them as attributes
        self.feature_ts = FeatureStoreTA(
            *args, asset_list=asset_list, ta_feature_config=ta_feature_config, **kwargs
        )
        self.prices_ts = SimulatedPrices(*args, asset_list=asset_list, **kwargs)

        # even more prices from a DataNode not in the project using APIDataNode

        self.external_prices = APIDataNode.build_from_identifier(
            identifier=MARKET_TIME_SERIES_UNIQUE_IDENTIFIER_CATEGORY_PRICES
        )
        translation_table = msc.AssetTranslationTable.get(
            unique_identifier=TEST_TRANSLATION_TABLE_UID
        )
        self.translated_prices_ts = WrapperDataNode(translation_table=translation_table)
        # retrainer DataNode
        self.model_train_ts = ModelTrainTimeSerie(
            asset_list=asset_list,
            ta_feature_config=ta_feature_config,
            model_config=model_config,
            window_size=window_size,
            retrain_config_days=retrain_config_days,
            *args,
            **kwargs,
        )

        super().__init__(*args, **kwargs)

    def dependencies(self) -> dict[str, Union["DataNode", "APIDataNode"]]:
        return {
            "prices_ts": self.prices_ts,
            "feature_ts": self.feature_ts,
            "api_prices": self.external_prices,
            "translated_prices_ts": self.translated_prices_ts,
            "model_train_ts": self.model_train_ts,
        }

    def update(self, *args, **kwargs):
        """
        live predictions from a life databatrse
        Args:
            *args:
            **kwargs:

        Returns:

        """
        return pd.DataFrame()


class WorkflowManager(DataNode):
    def __init__(
        self,
        asset_list: list[msc.Asset],
        ta_feature_config: list[dict[str, Any]],
        model_config: ModelConfiguration,
        window_size: int,
        retrain_config_days: int,
        *args,
        **kwargs,
    ):
        self.prediction_back_test = RollingModelPrediction(
            asset_list=asset_list,
            ta_feature_config=ta_feature_config,
            model_config=model_config,
            window_size=window_size,
            retrain_config_days=retrain_config_days,
            *args,
            **kwargs,
        )
        self.live_back_test = LivePrediction(
            asset_list=asset_list,
            ta_feature_config=ta_feature_config,
            model_config=model_config,
            window_size=window_size,
            retrain_config_days=retrain_config_days,
            *args,
            **kwargs,
        )

        super().__init__(*args, **kwargs)

    def dependencies(self) -> dict[str, Union["DataNode", "APIDataNode"]]:
        return {
            "prediction_back_test": self.prediction_back_test,
            "live_back_test": self.live_back_test,
        }

    def update(self, *args, **kwargs):
        pass


def test_features_from_prices_local_storage():
    from mainsequence.client import Asset

    msc.SessionDataSource.set_local_db()
    assets = Asset.filter(ticker__in=["NVDA", "JPM"])
    ts = FeatureStoreTA(
        asset_list=assets,
        ta_feature_config=[
            dict(kind="SMA", length=28),
            dict(kind="SMA", length=21),
            dict(kind="RSI", length=21),
        ],
    )

    ts.run(debug_mode=True, force_update=True)

    a = 5


def test_simulated_prices():
    from mainsequence.client import Asset

    assets = Asset.filter(
        ticker__in=["NVDA", "APPL"],
    )
    config = PriceSimulConfig(
        asset_list=assets,
    )

    batch_2_assets = Asset.filter(
        ticker__in=["JPM", "GS"],
    )
    config_2 = PriceSimulConfig(
        asset_list=batch_2_assets,
    )

    ts = SimulatedPrices(simulation_config=config)
    ts.run(debug_mode=True, force_update=True)

    ts_2 = SimulatedPrices(simulation_config=config_2)
    ts_2.run(debug_mode=True, force_update=True)

    # ts_2=CategorySimulatedPrices(asset_category_id="external_magnificent_7")
    #
    # ts_0=SingleIndexTS()
    # # ts_0.run(debug_mode=True,force_update=True)
    # # msc.SessionDataSource.set_local_db() #run on duck
    # # ts.run(debug_mode=True,force_update=True)
    # ts_2.run(debug_mode=True,force_update=True)
    #
    #
    # ts = TAFeature(asset_list=assets, ta_feature_config=[dict(kind="SMA", length=28),
    #                                                      dict(kind="SMA", length=21),
    #                                                      dict(kind="RSI", length=21)
    #
    #                                                      ]
    #
    #                )
    # # ts.run(debug_mode=True,
    # #        update_tree=True,
    # #        force_update=True,
    # #        )
    #
    #
    # ta_cfg = [{"kind": "RSI", "length": 21}, {"kind": "RSI", "length": 14}]
    # en_cfg = {"alpha": 1.0, "l1_ratio": 0.5, "fit_intercept": True}
    # window = 360  # use the last 30 days for each regression
    #
    # ts = WorkflowManager(
    #     asset_list=assets,
    #     ta_feature_config=ta_cfg,
    #     model_config=ElasticNetConfiguration(),
    #     window_size=window,
    #     retrain_config_days=10
    # )
    #
    #
    # ts.run(debug_mode=True,
    #        update_tree=True,
    #        force_update=True,
    #        )
