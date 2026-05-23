import datetime
from datetime import timedelta
from typing import Union

import pandas as pd

import mainsequence.client as msc
from mainsequence.markets.portfolios.data_nodes import (
    SIGNAL_UID,
    SignalWeights,
)
from mainsequence.markets.portfolios.models import AssetsConfiguration, VFBConfigBaseModel
from mainsequence.tdag.data_nodes import APIDataNode, DataNode

SIGNAL_OFFSET_START = datetime.datetime(2018, 1, 1, tzinfo=datetime.UTC)


class AUIDWeight(VFBConfigBaseModel):
    unique_identifier: str
    weight: float


class FixedWeightsConfig(VFBConfigBaseModel):
    signal_assets_configuration: AssetsConfiguration
    asset_unique_identifier_weights: list[AUIDWeight]


class FixedWeights(SignalWeights):
    @property
    def fixed_weights_config(self) -> FixedWeightsConfig:
        if not isinstance(self.signal_configuration, FixedWeightsConfig):
            raise TypeError("FixedWeights requires FixedWeightsConfig as signal_configuration.")
        return self.signal_configuration

    @property
    def assets_configuration(self) -> AssetsConfiguration:
        return self.fixed_weights_config.signal_assets_configuration

    @property
    def asset_unique_identifier_weights(self) -> list[AUIDWeight]:
        return self.fixed_weights_config.asset_unique_identifier_weights

    def maximum_forward_fill(self):
        return timedelta(days=200 * 365)  # Always forward-fill to avoid filling the DB

    def get_explanation(self):
        info = f"<p>{self.__class__.__name__}: Signal uses fixed weights with the following weights:</p>"
        return info

    def get_asset_list(self) -> None | list:
        asset_list = msc.Asset.filter(
            unique_identifier__in=[
                w.unique_identifier for w in self.asset_unique_identifier_weights
            ]
        )
        return asset_list

    def dependencies(self) -> dict[str, Union["DataNode", "APIDataNode"]]:
        return {}

    def _calculate_signal_weights(self) -> pd.DataFrame:
        existing_signal_rows = self.get_df_between_dates(
            dimension_filters={SIGNAL_UID: [self.signal_uid]}
        )
        if not existing_signal_rows.empty:
            return pd.DataFrame(columns=["time_index", "unique_identifier", "signal_weight"])

        df = pd.DataFrame([m.model_dump() for m in self.asset_unique_identifier_weights]).rename(
            columns={"weight": "signal_weight"}
        )
        df = df.set_index(["unique_identifier"])
        # offset 1 day to avoid last filter
        signals_weights = pd.concat(
            [df], axis=0, keys=[SIGNAL_OFFSET_START + datetime.timedelta(days=1)]
        ).rename_axis(["time_index", "unique_identifier"])

        signals_weights = signals_weights.dropna()
        return signals_weights
