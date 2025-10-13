from datetime import datetime

import pandas as pd
import pytz

from mainsequence.tdag.data_nodes import DataNode
from mainsequence.virtualfundbuilder.resource_factory.signal_factory import (
    WeightsBase,
    register_signal_class,
)


@register_signal_class(register_in_agent=True)
class MockSignal(WeightsBase, DataNode):
    """
    Mock Signal to test strategies. Creates a signal with long/short of ETH and BTC in frequency.
    """

    def __init__(self, source_frequency: str = "30min", *args, **kwargs):
        super().__init__(*args, **kwargs)

        asset_mapping = {}
        for tmp_asset_universe in self.asset_universe:
            execution_venue = tmp_asset_universe.execution_venue_symbol
            asset_list = tmp_asset_universe.asset_list
            ev = execution_venue.value
            asset_mapping[ev] = {
                a.get_spot_reference_asset_symbol(): a.unique_identifier for a in asset_list
            }
            self.asset_1 = asset_list[0]
            self.asset_2 = asset_list[1]
        self.asset_mapping = asset_mapping
        self.source_frequency = source_frequency

    def get_explanation(self):
        return f"The signal will switch between {self.asset_1.symbol} and {self.asset_2.symbol} randomly every 30 minutes"

    def maximum_forward_fill(self):
        return self.source_frequency

    def update(self, latest_value: datetime | None, *args, **kwargs) -> pd.DataFrame:
        """
        Args:
            latest_value (Union[datetime, None]): The timestamp of the most recent data point.

        Returns:
            DataFrame: A DataFrame containing updated signal weights, indexed by time and asset symbol.
        """
        if latest_value is None:
            latest_value = datetime(year=2017, month=1, day=1).replace(tzinfo=pytz.utc)

        current_time = datetime.now(pytz.utc)
        if (current_time - latest_value) < pd.Timedelta(self.source_frequency):
            return pd.DataFrame()

        signal_index = pd.date_range(
            start=latest_value + pd.Timedelta(self.source_frequency),
            end=current_time,
            freq=self.source_frequency,
        )
        signal_weights = []
        for ev, asset_map in self.asset_mapping.items():
            tmp_signal = pd.DataFrame(index=signal_index, columns=self.asset_mapping[ev].values())
            tmp_signal = pd.concat([tmp_signal], axis=1, keys=[ev])
            signal_weights.append(tmp_signal)
        signal_weights = pd.concat(signal_weights, axis=1)

        last_observation = self.get_last_observation()
        if last_observation is not None:
            asset_1_weight = -last_observation.query(f"asset_symbol == '{self.asset_1.symbol}'")[
                "signal_weight"
            ].iloc[0]
        else:
            asset_1_weight = 1.0

        signal_weights.loc[:, (self.asset_1.execution_venue.symbol, self.asset_1.symbol)] = [
            asset_1_weight if i % 2 == 0 else -asset_1_weight for i in range(len(signal_weights))
        ]
        signal_weights.loc[:, (self.asset_2.execution_venue.symbol, self.asset_2.symbol)] = (
            -signal_weights.loc[:, (self.asset_1.execution_venue.symbol, self.asset_1.symbol)]
        )

        signal_weights = signal_weights.stack().stack().to_frame(name="signal_weight").astype(float)
        signal_weights.index.set_names(
            ["time_index", "asset_symbol", "execution_venue_symbol"], inplace=True
        )

        return signal_weights
