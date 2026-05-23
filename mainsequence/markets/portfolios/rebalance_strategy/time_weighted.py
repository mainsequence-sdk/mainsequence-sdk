import datetime
import re

import numpy as np
import pandas as pd
from pydantic import Field, field_validator, model_validator

from mainsequence.markets.portfolios.enums import (
    PriceTypeNames,
    RebalanceFrequencyStrategyName,
)
from mainsequence.markets.portfolios.rebalance_strategy.base import (
    RebalanceStrategyBase,
)
from mainsequence.markets.portfolios.utils import logger

_TIME_RE = re.compile(r"^(?:[01]?\d|2[0-3]):[0-5]\d$")


class TimeWeighted(RebalanceStrategyBase):
    rebalance_start: str = Field(
        default="9:00",
        description="Start time for rebalancing in 'H:MM' or 'HH:MM' (24h).",
    )
    rebalance_end: str = Field(
        default="23:00",
        description="End time for rebalancing in 'H:MM' or 'HH:MM' (24h).",
    )
    rebalance_frequency_strategy: RebalanceFrequencyStrategyName = Field(
        default=RebalanceFrequencyStrategyName.DAILY,
        description="Rebalance frequency (Enum).",
    )

    @field_validator("rebalance_start", "rebalance_end")
    @classmethod
    def _validate_time_format(cls, v: str) -> str:
        if not _TIME_RE.match(v):
            raise ValueError("Expected 'H:MM' or 'HH:MM' (24-hour), e.g., '9:00' or '09:00'.")
        return v

    @model_validator(mode="after")
    def _check_time_order(self) -> "TimeWeighted":
        start_t = self._parse_time(self.rebalance_start)
        end_t = self._parse_time(self.rebalance_end)
        if start_t >= end_t:
            raise ValueError("rebalance_start must be earlier than rebalance_end.")
        return self

    @staticmethod
    def _parse_time(s: str) -> datetime.time:
        h, m = s.split(":")
        return datetime.time(int(h), int(m))

    @property
    def rebalance_start_time(self) -> datetime.time:
        return self._parse_time(self.rebalance_start)

    @property
    def rebalance_end_time(self) -> datetime.time:
        return self._parse_time(self.rebalance_end)

    def apply_rebalance_logic(
        self,
        last_rebalance_weights: pd.DataFrame,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        signal_weights: pd.DataFrame,
        prices_df: pd.DataFrame,
        price_type: PriceTypeNames,
    ) -> pd.DataFrame:
        """
        Rebalance weights are set at start_time of rebalancing.
        """
        raise NotImplementedError
        asset_list = list(signal_weights.columns)
        start_time, end_time = (
            pd.Timestamp(self.rebalance_start).time(),
            pd.Timestamp(self.rebalance_end).time(),
        )

        get_time_seconds = lambda x: x.hour * 3600 + x.minute * 60 + x.second

        rebalance_dates = self.calculate_rebalance_dates(
            start=start_date,
            end=end_date,
            rebalance_frequency_strategy=self.rebalance_frequency_strategy,
            calendar=self.calendar,
        )

        signal_weights["day"] = signal_weights.index.floor("D")
        rebalance_days = np.intersect1d(signal_weights["day"], rebalance_dates)
        rebalance_weights = signal_weights[signal_weights["day"].isin(rebalance_days)]

        rebalance_weights = rebalance_weights[rebalance_weights.index.time >= start_time]
        rebalance_weights = rebalance_weights[rebalance_weights.index.time <= end_time]

        past_rebalance_weights = rebalance_weights.groupby("day").first().shift()

        if last_rebalance_weights is not None:
            past_rebalance_weight = last_rebalance_weights["weights_current"].reset_index(
                level="time_index", drop=True
            )
        else:
            past_rebalance_weight = pd.Series(0, index=asset_list)

        past_rebalance_weights.index += datetime.timedelta(seconds=get_time_seconds(start_time))
        past_rebalance_weights = past_rebalance_weights.reindex(rebalance_weights.index).ffill()

        time_weight = (rebalance_weights.index - rebalance_weights["day"]).dt.total_seconds()
        time_weight = (time_weight - get_time_seconds(start_time)) / (
            get_time_seconds(end_time) - get_time_seconds(start_time)
        )

        rebalance_weights = rebalance_weights.drop(columns=["day"])

        diff_weights = rebalance_weights - past_rebalance_weights
        rebalance_weights = past_rebalance_weights + diff_weights.multiply(time_weight, axis=0)

        prices_df = (
            prices_df.reset_index()
            .pivot(index="time_index", columns="asset_symbol", values=price_type)
            .ffill()
            .fillna(0)
        )
        valid_columns = rebalance_weights.columns[rebalance_weights.columns.isin(prices_df.columns)]
        if len(valid_columns) != rebalance_weights.shape[1]:
            rebalance_weights = rebalance_weights[valid_columns].copy()
            rebalance_weights = rebalance_weights.divide(rebalance_weights.sum(axis=1), axis=0)

        nan_mask = prices_df.loc[rebalance_weights.index].isna()
        rebalance_weights[nan_mask] = np.nan

        if len(rebalance_weights) == 0:
            logger.info("No new rebalancing weights found - returning empty DataFrame")
            return pd.DataFrame()

        shifted_rebalance_weights = rebalance_weights.shift(1)
        shifted_rebalance_weights.iloc[0] = past_rebalance_weight
        rebalance_weights = pd.concat(
            objs=[shifted_rebalance_weights, rebalance_weights, prices_df, prices_df.shift(1)],
            keys=["weights_before", "weights_current", "price_current", "price_before"],
            axis=1,
        )

        logger.info(f"{len(rebalance_weights)} new rebalancing weights calculated")
        return rebalance_weights
