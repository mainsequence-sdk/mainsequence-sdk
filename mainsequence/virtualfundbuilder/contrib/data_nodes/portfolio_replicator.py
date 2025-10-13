import copy
from enum import Enum
from typing import Union

import numpy as np
import pandas as pd
from sklearn.linear_model import ElasticNet, Lasso, LinearRegression
from tqdm import tqdm

from mainsequence.client import MARKETS_CONSTANTS, Asset, AssetCategory
from mainsequence.tdag.data_nodes import DataNode
from mainsequence.virtualfundbuilder import TIMEDELTA
from mainsequence.virtualfundbuilder.contrib.prices.data_nodes import (
    get_interpolated_prices_timeseries,
)
from mainsequence.virtualfundbuilder.models import VFBConfigBaseModel
from mainsequence.virtualfundbuilder.resource_factory.signal_factory import (
    WeightsBase,
    register_signal_class,
)


class TrackingStrategy(Enum):
    ELASTIC_NET = "elastic_net"
    LASSO = "lasso"


class TrackingStrategyConfiguration(VFBConfigBaseModel):
    configuration: dict = {"alpha": 0, "l1_ratio": 0}


def rolling_pca_betas(X, window, n_components=5, *args, **kwargs):
    """
    Perform rolling PCA and return the betas (normalized principal component weights).

    Parameters:
        X (pd.DataFrame): DataFrame of stock returns or feature data (rows are time, columns are assets).
        window (int): The size of the rolling window.
        n_components (int, optional): The number of principal components to extract. Defaults to 5.

    Returns:
        np.ndarray: An array of normalized PCA weights for each rolling window.
    """
    from sklearn.decomposition import PCA

    betas = []

    # Loop over each rolling window
    for i in tqdm(range(window, len(X)), desc="Performing rolling PCA"):
        X_window = X.iloc[i - window : i]

        # Perform PCA on the windowed data
        pca = PCA(n_components=n_components)
        try:
            pca.fit(X_window)
        except Exception as e:
            raise e

        # Get the eigenvectors (principal components)
        eigenvectors = pca.components_  # Shape: (n_components, n_assets)

        # Transpose to align weights with assets
        eigenvectors_transposed = eigenvectors.T  # Shape: (n_assets, n_components)

        # Normalize the eigenvectors so that sum of absolute values = 1 for each component
        weights_normalized = eigenvectors_transposed / np.sum(
            np.abs(eigenvectors_transposed), axis=0
        )

        # Append the normalized weights (betas) for this window
        betas.append(weights_normalized)

    return np.array(betas)  # Shape: (num_windows, n_assets, n_components)


def rolling_lasso_regression(y, X, window, alpha=1.0, *args, **kwargs):
    """
    Perform rolling Lasso regression and return the coefficients.

    Parameters:
        y (pd.Series): Target variable.
        X (pd.DataFrame): Feature variables.
        window (int): Size of the rolling window.
        alpha (float, optional): Regularization strength. Defaults to 1.0.

    Returns:
        list: List of DataFrames containing the coefficients for each rolling window.
    """
    betas = []
    if alpha == 0:
        lasso = LinearRegression(fit_intercept=False, positive=True)
    else:
        lasso = Lasso(alpha=alpha, fit_intercept=False, positive=True)

    for i in tqdm(range(window, len(y)), desc="Building Lasso regression"):
        null_xs = X.isnull().sum()
        null_xs = null_xs[null_xs > 0]
        symbols_to_zero = None
        X_window = X.iloc[i - window : i]
        if null_xs.shape[0] > 0:
            symbols_to_zero = null_xs.index.to_list()
            X_window = X_window[[c for c in X_window.columns if c not in symbols_to_zero]]
        y_window = y.iloc[i - window : i]

        # Fit the Lasso model
        try:
            lasso.fit(X_window, y_window)
        except Exception as e:
            raise e

        round_betas = pd.DataFrame(
            lasso.coef_.reshape(1, -1),
            columns=X_window.columns,
            index=[X_window.index[-1]],
        )
        if symbols_to_zero is not None:
            round_betas.loc[:, symbols_to_zero] = 0.0
        # Append the coefficients
        betas.append(round_betas)
    return betas


def rolling_elastic_net(y, X, window, alpha=1.0, l1_ratio=0.5):
    """
    Perform rolling Elastic Net regression and return the coefficients.

    Parameters:
        y (pd.Series): Target variable.
        X (pd.DataFrame): Feature variables.
        window (int): Size of the rolling window.
        alpha (float, optional): Regularization strength. Defaults to 1.0.
        l1_ratio (float, optional): The ElasticNet mixing parameter. Defaults to 0.5.

    Returns:
        np.ndarray: Array of coefficients for each rolling window.
    """
    betas = []
    enet = ElasticNet(alpha=alpha, l1_ratio=l1_ratio, fit_intercept=False)

    for i in tqdm(range(window, len(y)), desc="Building rolling regression"):
        X_window = X.iloc[i - window : i]
        y_window = y.iloc[i - window : i]

        # Fit the ElasticNet model
        enet.fit(X_window, y_window)

        # Save coefficients
        betas.append(enet.coef_)

    return np.array(betas)


@register_signal_class(register_in_agent=True)
class ETFReplicator(WeightsBase, DataNode):
    def __init__(
        self,
        etf_ticker: str,
        tracking_strategy_configuration: TrackingStrategyConfiguration,
        in_window: int = 60,
        tracking_strategy: TrackingStrategy = TrackingStrategy.LASSO,
        *args,
        **kwargs,
    ):
        """
        Initialize the ETFReplicator.

        Args:
            etf_ticker (str): Figi of the etf to replicate.
            tracking_strategy_configuration (TrackingStrategyConfiguration): Configuration parameters for the tracking strategy.
            in_window (int, optional): The size of the rolling window for regression. Defaults to 60.
            tracking_strategy (TrackingStrategy, optional): The regression strategy to use for tracking. Defaults to TrackingStrategy.LASSO.
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """
        super().__init__(*args, **kwargs)

        self.in_window = in_window
        self.bars_ts = get_interpolated_prices_timeseries(copy.deepcopy(self.assets_configuration))
        etf_assets_configuration = copy.deepcopy(self.assets_configuration)
        etf_assets_configuration.assets_category_unique_id = "etfs"
        self.etf_bars_ts = get_interpolated_prices_timeseries(etf_assets_configuration)
        self.etf_ticker = etf_ticker

        self.tracking_strategy = tracking_strategy
        self.tracking_strategy_configuration = tracking_strategy_configuration

    def get_asset_list(self) -> None | list:
        asset_category = AssetCategory.get(
            unique_identifier=self.assets_configuration.assets_category_unique_id
        )
        self.price_assets = Asset.filter(id__in=asset_category.assets)
        self.etf_asset = Asset.get(
            ticker=self.etf_ticker,
            exchange_code="US",
            security_type=MARKETS_CONSTANTS.FIGI_SECURITY_TYPE_ETP,
            security_market_sector=MARKETS_CONSTANTS.FIGI_MARKET_SECTOR_EQUITY,
        )
        return self.price_assets + [self.etf_asset]

    def dependencies(self) -> dict[str, Union["DataNode", "APIDataNode"]]:
        return {
            "bars_ts": self.bars_ts,
            "etf_bars_ts": self.etf_bars_ts,
        }

    def get_explanation(self):
        info = f"""
        <p>{self.__class__.__name__}: Signal aims to replicate {self.etf_asset.ticker} using a data-driven approach.
        This strategy will use {self.tracking_strategy} as approximation function with parameters </p>
        <code>{self.tracking_strategy_configuration}</code>
        """
        return info

    def maximum_forward_fill(self):
        freq = self.assets_configuration.prices_configuration.bar_frequency_id
        return pd.Timedelta(freq) - TIMEDELTA

    def get_tracking_weights(self, prices: pd.DataFrame) -> pd.DataFrame:
        prices = prices[~prices[self.etf_asset.unique_identifier].isnull()]
        prices = prices.pct_change().iloc[1:]
        prices = prices.replace([np.inf, -np.inf], np.nan)

        y = prices[self.etf_asset.unique_identifier]
        X = prices.drop(columns=[self.etf_asset.unique_identifier])

        if self.tracking_strategy == TrackingStrategy.ELASTIC_NET:
            betas = rolling_elastic_net(
                y, X, window=self.in_window, **self.tracking_strategy_configuration.configuration
            )
        elif self.tracking_strategy == TrackingStrategy.LASSO:
            betas = rolling_lasso_regression(
                y, X, window=self.in_window, **self.tracking_strategy_configuration.configuration
            )
        else:
            raise NotImplementedError

        try:
            betas = pd.concat(betas, axis=0)
        except Exception as e:
            raise e
        betas.index.name = "time_index"
        return betas

    def update(self) -> pd.DataFrame:
        if self.update_statistics.max_time_index_value:
            prices_start_date = self.update_statistics.max_time_index_value - pd.Timedelta(
                days=self.in_window
            )
        else:
            prices_start_date = self.OFFSET_START - pd.Timedelta(days=self.in_window)

        prices = self.bars_ts.get_df_between_dates(
            start_date=prices_start_date,
            end_date=None,
            great_or_equal=True,
            less_or_equal=True,
            unique_identifier_list=[a.unique_identifier for a in self.price_assets],
        )
        etf_prices = self.etf_bars_ts.get_df_between_dates(
            start_date=prices_start_date,
            end_date=None,
            great_or_equal=True,
            less_or_equal=True,
            unique_identifier_list=[self.etf_asset.unique_identifier],
        )

        prices = pd.concat([prices, etf_prices])
        prices = prices.reset_index().pivot_table(
            index="time_index",
            columns="unique_identifier",
            values=self.assets_configuration.price_type.value,
        )

        if prices.shape[0] < self.in_window:
            self.logger.warning("Not enough prices to run regression")
            return pd.DataFrame()

        weights = self.get_tracking_weights(prices=prices)
        weights = weights.unstack().to_frame(name="signal_weight")
        weights = weights.swaplevel()
        return weights
