import pandas as pd

from mainsequence.markets.portfolios.enums import PriceTypeNames
from mainsequence.markets.portfolios.rebalance_strategy.base import (
    RebalanceStrategyBase,
)


class ImmediateSignal(RebalanceStrategyBase):
    def get_explanation(self):
        explanation = """<p> This rebalance strategy 'immediately' rebalances the weights. This is equivalent to just using the signal weights. </p>"""
        return explanation

    def apply_rebalance_logic(
        self,
        last_rebalance_weights: pd.DataFrame,
        signal_weights: pd.DataFrame,
        prices_df,
        price_type: PriceTypeNames,
        *args,
        **kwargs,
    ) -> pd.DataFrame:
        volume_df = prices_df.reset_index().pivot(
            index="time_index", columns=["unique_identifier"], values="volume"
        )
        prices_df = prices_df.reset_index().pivot(
            index="time_index", columns=["unique_identifier"], values=price_type.value
        )

        if last_rebalance_weights is not None:
            # This strategy emits backtest weights, so include the last observation
            # to calculate before/after execution context.
            volume_df = pd.concat(
                [last_rebalance_weights.unstack()["volume_current"], volume_df], axis=0
            )
            prices_df = pd.concat(
                [last_rebalance_weights.unstack()["price_current"], prices_df], axis=0
            )
            signal_weights = pd.concat(
                [last_rebalance_weights.unstack()["weights_current"], signal_weights], axis=0
            )
        rebalance_weights = pd.concat(
            objs=[
                signal_weights,
                signal_weights.shift(1),
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

        if last_rebalance_weights is not None:
            rebalance_weights = rebalance_weights[
                rebalance_weights.index
                > last_rebalance_weights.index.get_level_values("time_index")[0]
            ]

        return rebalance_weights
