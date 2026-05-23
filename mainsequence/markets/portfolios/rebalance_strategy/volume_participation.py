import datetime
import re

import numpy as np
import pandas as pd
from pydantic import Field, field_validator, model_validator
from tqdm import tqdm

from mainsequence.markets.portfolios.enums import RebalanceFrequencyStrategyName
from mainsequence.markets.portfolios.rebalance_strategy.base import (
    RebalanceStrategyBase,
)
from mainsequence.markets.portfolios.utils import logger

_TIME_RE = re.compile(r"^(?:[01]?\d|2[0-3]):[0-5]\d$")


class VolumeParticipation(RebalanceStrategyBase):
    """
    This rebalance strategy implies volume participation with no market impact.
    i.e. execution at VWAP and it will never execute more than max_percent_volume_in_bar.
    """

    rebalance_start: str = Field(
        default="9:00",
        description="Start time for rebalancing in 'H:MM' or 'HH:MM' 24-hour format.",
    )
    rebalance_end: str = Field(
        default="23:00",
        description="End time for rebalancing in 'H:MM' or 'HH:MM' 24-hour format.",
    )
    rebalance_frequency_strategy: RebalanceFrequencyStrategyName = Field(
        default=RebalanceFrequencyStrategyName.DAILY,
        description="Rebalance frequency (Enum).",
    )
    max_percent_volume_in_bar: float = Field(
        default=0.01,
        gt=0,
        le=1,
        description="Maximum fraction of volume to trade per bar (0, 1].",
    )
    total_notional: float = Field(
        default=50_000_000,
        gt=0,
        description="Initial notional invested in the strategy (must be > 0).",
    )

    @field_validator("rebalance_start", "rebalance_end")
    @classmethod
    def _validate_time_format(cls, v: str) -> str:
        if not _TIME_RE.match(v):
            raise ValueError("Expected 'H:MM' or 'HH:MM' (24-hour), e.g. '9:00' or '09:00'.")
        return v

    @field_validator("max_percent_volume_in_bar")
    @classmethod
    def _validate_pct(cls, v: float) -> float:
        if not (0 < v <= 1):
            raise ValueError("max_percent_volume_in_bar must be in the (0, 1] range.")
        return v

    @field_validator("total_notional")
    @classmethod
    def _validate_notional(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("total_notional must be positive.")
        return float(v)

    @model_validator(mode="after")
    def _check_time_order(self) -> "VolumeParticipation":
        start_t = self._parse_time(self.rebalance_start)
        end_t = self._parse_time(self.rebalance_end)
        if start_t >= end_t:
            raise ValueError("rebalance_start must be earlier than rebalance_end.")
        return self

    @staticmethod
    def _parse_time(s: str) -> datetime.time:
        h, m = s.split(":")
        return datetime.time(int(h), int(m))

    def apply_rebalance_logic(
        self,
        last_rebalance_weights: pd.DataFrame,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        signal_weights: pd.DataFrame,
        prices_df: pd.DataFrame,
        price_type: str,
    ) -> pd.DataFrame:
        raise NotImplementedError
        asset_list = list(signal_weights.columns)
        start_time, end_time = (
            pd.Timestamp(self.rebalance_start).time(),
            pd.Timestamp(self.rebalance_end).time(),
        )

        rebalance_dates = self.calculate_rebalance_dates(
            start=start_date,
            end=end_date,
            rebalance_frequency_strategy=self.rebalance_frequency_strategy,
            calendar=self.calendar,
        )

        signal_weights["day"] = signal_weights.index.floor("D")

        volume_df = (
            prices_df.reset_index()
            .pivot(index="time_index", columns="asset_symbol", values="volume")
            .fillna(0)
        )
        prices_df = (
            prices_df.reset_index()
            .pivot(index="time_index", columns="asset_symbol", values=price_type)
            .fillna(0)
        )

        rebalance_days = np.intersect1d(signal_weights["day"], rebalance_dates)

        rebalance_weights = signal_weights[signal_weights["day"].isin(rebalance_days)]
        rebalance_weights = rebalance_weights[rebalance_weights.index.time >= start_time]
        rebalance_weights = rebalance_weights[rebalance_weights.index.time <= end_time]

        rebalance_weights = rebalance_weights.set_index("day", append=True)

        max_participation_volume = (volume_df * prices_df) * self.max_percent_volume_in_bar
        max_participation_volume["day"] = max_participation_volume.index.floor("D")
        max_participation_volume = max_participation_volume.set_index("day", append=True)
        max_participation_volume = max_participation_volume[
            max_participation_volume.index.isin(rebalance_weights.index)
        ]

        rebalance_weights = pd.concat(
            objs=[rebalance_weights, max_participation_volume],
            axis=1,
            keys=["weights", "max_dollar_volume"],
        )

        if last_rebalance_weights is not None:
            past_rebalance_weight = last_rebalance_weights["weights_current"].reset_index(
                level="time_index", drop=True
            )
        else:
            past_rebalance_weight = pd.Series(0, index=asset_list)

        past_day_rebalance_weight = past_rebalance_weight
        new_rebalance_weights = []
        for day, day_df in tqdm(
            rebalance_weights.groupby("day"), desc="building volume participation"
        ):
            if (day_df.weights.max() - day_df.weights.min()).sum() != 0.0:
                logger.warning(
                    "Signal weight in time period changes, using weights at rebalancing start"
                )
                day_df.loc[:, "weights"] = day_df["weights"].iloc[0].to_numpy()

            weights_diff = day_df.weights - past_day_rebalance_weight

            target_dollar_volume = np.abs(weights_diff) * self.total_notional
            cumulative_dollar_volume = day_df.max_dollar_volume.fillna(0).cumsum()
            weighted_volume_multiplier = (cumulative_dollar_volume / target_dollar_volume).replace(
                [np.inf], np.nan
            )
            weighted_volume_multiplier = weighted_volume_multiplier.fillna(0).map(
                lambda x: min(1.0, x)
            )

            new_rebalance_weights_day = (
                past_day_rebalance_weight + weights_diff * weighted_volume_multiplier
            )
            new_rebalance_weights.append(new_rebalance_weights_day)
            past_day_rebalance_weight = new_rebalance_weights_day.iloc[-1]

        if len(new_rebalance_weights) == 0:
            logger.info("No new rebalancing weights found - returning empty DataFrame")
            return pd.DataFrame()

        rebalance_weights = pd.concat(new_rebalance_weights, axis=0).reset_index("day", drop=True)
        rebalance_weights_index = rebalance_weights.index

        shifted_rebalance_weights = rebalance_weights.shift(1)
        shifted_rebalance_weights.iloc[0] = past_rebalance_weight
        rebalance_weights = pd.concat(
            objs=[
                rebalance_weights,
                shifted_rebalance_weights,
                prices_df,
                prices_df.shift(1),
                volume_df,
                volume_df.shift(1),
            ],
            keys=[
                "weights_current",
                "weights_before",
                "price_current",
                "price_before",
                "volume_current",
                "volume_before",
            ],
            axis=1,
        )

        rebalance_weights = rebalance_weights.loc[rebalance_weights_index].fillna(0)

        logger.info(f"{len(rebalance_weights)} new rebalancing weights calculated")
        return rebalance_weights
