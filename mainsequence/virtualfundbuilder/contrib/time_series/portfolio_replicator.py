import copy
from datetime import datetime
from typing import Union, Dict
from enum import Enum

import numpy as np
import pandas as pd
import pytz
from sklearn.linear_model import ElasticNet, Lasso, LinearRegression
from tqdm import tqdm

from mainsequence.virtualfundbuilder import TIMEDELTA
from mainsequence.virtualfundbuilder.contrib.prices.time_series import get_interpolated_prices_timeseries
from mainsequence.virtualfundbuilder.resource_factory.signal_factory import WeightsBase, register_signal_class
from mainsequence.virtualfundbuilder.models import VFBConfigBaseModel
from mainsequence.tdag.time_series import TimeSerie


class TrackingStrategy(Enum):
    ELASTIC_NET = "elastic_net"
    LASSO = "lasso"

class TrackingStrategyConfiguration(VFBConfigBaseModel):
    configuration: Dict = {"alpha": 0, "l1_ratio": 0}

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
        X_window = X.iloc[i - window:i]

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
        weights_normalized = eigenvectors_transposed / np.sum(np.abs(eigenvectors_transposed), axis=0)

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
        X_window = X.iloc[i - window:i]
        if null_xs.shape[0] > 0:
            symbols_to_zero = null_xs.index.to_list()
            X_window = X_window[[c for c in X_window.columns if c not in symbols_to_zero]]
        y_window = y.iloc[i - window:i]

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
        X_window = X.iloc[i - window:i]
        y_window = y.iloc[i - window:i]

        # Fit the ElasticNet model
        enet.fit(X_window, y_window)

        # Save coefficients
        betas.append(enet.coef_)

    return np.array(betas)

@register_signal_class(register_in_agent=True)
class ETFReplicator(WeightsBase, TimeSerie):
    def __init__(
        self,
        symbol_to_replicate: str,
        tracking_strategy_configuration: TrackingStrategyConfiguration,
        in_window: int = 60,
        tracking_strategy: TrackingStrategy = TrackingStrategy.LASSO,
        *args,
        **kwargs,
    ):
        """
        Initialize the ETFReplicator.

        Args:
            symbol_to_replicate (str): Symbol of the asset to replicate. Must be included in the signals asset universe.
            tracking_strategy_configuration (TrackingStrategyConfiguration): Configuration parameters for the tracking strategy.
            in_window (int, optional): The size of the rolling window for regression. Defaults to 60.
            tracking_strategy (TrackingStrategy, optional): The regression strategy to use for tracking. Defaults to TrackingStrategy.LASSO.
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """
        super().__init__(*args, **kwargs)

        self.in_window = in_window
        self.bars_ts, self.asset_symbols = get_interpolated_prices_timeseries(copy.deepcopy(self.assets_configuration))
        assert len(list(self.asset_symbols.keys())) == 1
        self.symbol_to_replicate = symbol_to_replicate
        self.tracking_strategy = tracking_strategy
        self.tracking_strategy_configuration = tracking_strategy_configuration

    def get_explanation(self):
        info = f"""
        <p>{self.__class__.__name__}: Signal aims to replicate {self.symbol_to_replicate} using a data-driven approach.
        This strategy will use {self.tracking_strategy} as approximation function with parameters </p>
        <code>{self.tracking_strategy_configuration}</code>
        """
        return info

    def maximum_forward_fill(self):
        freq = self.assets_configuration.prices_configuration.bar_frequency_id
        return pd.Timedelta(freq) - TIMEDELTA

    def get_tracking_weights(self, prices: pd.DataFrame) -> pd.DataFrame:
        prices = prices[~prices[self.symbol_to_replicate].isnull()]
        prices = prices.pct_change().iloc[1:]
        prices = prices.replace([np.inf, -np.inf], np.nan)

        y = prices[self.symbol_to_replicate]
        X = prices.drop(columns=[self.symbol_to_replicate])

        if self.tracking_strategy == TrackingStrategy.ELASTIC_NET.value:
            betas = rolling_elastic_net(
                y, X, window=self.in_window, **self.tracking_strategy_configuration.configuration
            )
        elif self.tracking_strategy == TrackingStrategy.LASSO.value:
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

    def update(
        self, latest_value: Union[datetime, None], *args, **kwargs
    ) -> pd.DataFrame:
        if latest_value is None:
            latest_value = datetime(year=2018, month=1, day=1).replace(tzinfo=pytz.utc)

        asset_symbols = [a for assets in self.asset_symbols.values() for a in assets]

        prices_start_date = latest_value - pd.Timedelta(days=self.in_window)
        prices = self.bars_ts.pandas_df_concat_on_rows_by_key_between_dates(
            start_date=prices_start_date,
            end_date=None,
            great_or_equal=True,
            less_or_equal=True,
            asset_symbols=asset_symbols,
        )

        prices = prices.reset_index().pivot_table(
            index="time_index",
            columns="asset_symbol",
            values=self.assets_configuration.price_type.value,
        )

        if prices.shape[0] < self.in_window:
            self.logger.warning("Not enough prices to run regression")
            return pd.DataFrame()

        weights = self.get_tracking_weights(prices=prices)
        weights = weights.unstack().to_frame(name="signal_weight")
        weights = weights.swaplevel()
        weights["execution_venue_symbol"] = list(self.asset_symbols.keys())[0]
        weights = weights.set_index("execution_venue_symbol", append=True)
        return weights
