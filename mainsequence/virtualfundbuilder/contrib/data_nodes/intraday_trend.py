import copy
from datetime import datetime

import numpy as np
import pandas as pd
import pandas_market_calendars as mcal
import pytz

from mainsequence.tdag.data_nodes import DataNode
from mainsequence.virtualfundbuilder import TIMEDELTA
from mainsequence.virtualfundbuilder.contrib.prices.data_nodes import (
    get_interpolated_prices_timeseries,
)
from mainsequence.virtualfundbuilder.resource_factory.signal_factory import (
    WeightsBase,
    register_signal_class,
)


@register_signal_class(register_in_agent=True)
class IntradayTrend(WeightsBase, DataNode):

    def __init__(self, calendar: str, source_frequency: str = "1d", *args, **kwargs):
        """
        Signal Weights

        Arguments
            source_frequency (str): Frequency of market cap source
        """
        super().__init__(*args, **kwargs)

        self.source_frequency = source_frequency
        self.calendar = calendar
        self.bars_ts, self.asset_symbols = get_interpolated_prices_timeseries(
            copy.deepcopy(self.assets_configuration)
        )

    def update(
        self, latest_value: datetime | None, params_for_tree_run=None, *args, **kwargs
    ) -> pd.DataFrame:
        """
        Updates the weights considering rebalance periods and execution frequency.

        Args:
            latest_value Union[datetime, None]: The timestamp of the latest available data.
        """
        asset_symbols = [a for assets in self.asset_symbols.values() for a in assets]
        exchange_per_symbol = {v: k for k, values in self.asset_symbols.items() for v in values}

        max_assets_time = [
            ts.get_last_observation(asset_symbols=self.asset_symbols[exchange])
            for exchange, ts in self.bars_ts.related_time_series.items()
        ]

        top_date_limit = min(
            [i.index.get_level_values("time_index").min() for i in max_assets_time]
        )  # get the minimum available time

        if latest_value is None:
            latest_value = datetime(year=2018, month=1, day=1).replace(tzinfo=pytz.utc)
        else:
            # only when there are prices enough for the upsample
            upper_range = latest_value + pd.Timedelta(self.source_frequency)
            if top_date_limit < upper_range:
                return pd.DataFrame()

        # get last few days for past intraday returns
        prices_start_date = latest_value - pd.Timedelta(days=1)
        prices = self.bars_ts.pandas_df_concat_on_rows_by_key_between_dates(
            start_date=prices_start_date,
            end_date=top_date_limit,
            great_or_equal=True,
            less_or_equal=True,
            asset_symbols=asset_symbols,
        )

        # align signal weight updates with market schedule for each day
        calendar_dates = mcal.get_calendar(self.calendar)
        market_schedule = calendar_dates.schedule(
            start_date=prices_start_date, end_date=top_date_limit
        )
        signal_weights_index = pd.DatetimeIndex(
            market_schedule.apply(
                lambda d: pd.Series(
                    pd.date_range(
                        start=d["market_open"], end=d["market_close"], freq=self.source_frequency
                    )
                ),
                axis=1,
            ).values.flatten()
        )
        signal_prices = prices[
            prices.index.get_level_values("time_index").isin(signal_weights_index)
        ]
        signal_returns = signal_prices.groupby(["asset_symbol"])["close"].pct_change()
        open_close_daily = prices.groupby(
            [
                prices.index.get_level_values("time_index").date,
                prices.index.get_level_values("asset_symbol"),
            ]
        ).agg(open=("open", "first"), close=("close", "last"))
        open_close_daily = open_close_daily.unstack("asset_symbol")

        signal_returns = signal_returns.unstack("asset_symbol").reset_index(
            "execution_venue_symbol", drop=True
        )
        intraday_returns = open_close_daily["close"] / open_close_daily["open"] - 1
        daily_returns = open_close_daily["open"] / open_close_daily["close"].shift(1) - 1

        # Determine daily trade direction:
        # Long (1) if previous intraday return was positive
        # Short (-1) if previous intraday return was negative
        trade_direction = intraday_returns.shift(1).map(lambda r: 1 if r > 0 else -1)

        # prepare signal weights
        signal_weights = pd.DataFrame(index=signal_weights_index)
        signal_weights["date"] = signal_weights.index.date
        signal_weights = signal_weights[signal_weights["date"].isin(trade_direction.index)]
        trade_direction = trade_direction.loc[signal_weights["date"]]
        trade_direction.index = signal_weights.index
        signal_weights = pd.concat(
            [signal_weights, trade_direction, signal_returns],
            axis=1,
            keys=["date", "trade_direction", "signal_returns"],
        )

        # add daily returns from last day
        daily_returns.index = daily_returns.index.map(
            signal_weights["date"].groupby("date").apply(lambda x: x.index[0])
        )
        for asset_name in asset_symbols:
            signal_weights.loc[daily_returns.index, ("signal_returns", asset_name)] = daily_returns[
                asset_name
            ]

        def create_weights(daily_data):
            # start weights each day, grow by 1/trades_per_day and sell at the end
            weights = pd.DataFrame(np.nan, index=daily_data.index, columns=asset_symbols)
            weights.iloc[0] = 0
            for asset_name in asset_symbols:
                trade_size = 1 / len(daily_data)
                daily_trade_direction = daily_data[("trade_direction", asset_name)].iloc[0]

                if daily_trade_direction == 0:
                    return weights[asset_name].fillna(0)

                # Create the indicator based on trade direction
                if daily_trade_direction > 0:
                    indicator = daily_data[("signal_returns", asset_name)] > 0
                else:
                    indicator = daily_data[("signal_returns", asset_name)] < 0

                weights.loc[indicator, asset_name] = (
                    np.cumsum(indicator) * trade_size * daily_trade_direction
                )

            weights.iloc[-1] = 0  # sell at the end of day
            return weights

        # calculate signal weights based on intra-day returns in source_frequency interval
        signal_weights = (
            signal_weights.groupby(signal_weights.index.date)
            .apply(create_weights)
            .reset_index(drop=True, level=0)
            .ffill()
            .dropna()
        )

        # prepare for storage
        signal_weights.index.name = "time_index"
        signal_weights.columns = pd.MultiIndex.from_arrays(
            [[exchange_per_symbol[a] for a in asset_symbols], signal_weights.columns],
            names=("execution_venue_symbol", "asset_symbol"),
        )

        if len(signal_weights) == 0:
            return pd.DataFrame()

        signal_weights = signal_weights[signal_weights.index > latest_value]
        signal_weights = signal_weights.stack().stack().to_frame(name="signal_weight").astype(float)
        return signal_weights

    def maximum_forward_fill(self):
        return pd.Timedelta(self.source_frequency) - TIMEDELTA
