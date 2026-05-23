from __future__ import annotations

from datetime import timedelta

import pandas as pd

from mainsequence.client import Asset, AssetCategory
from mainsequence.client.models_tdag import Artifact
from mainsequence.markets.portfolios.data_nodes import SignalWeights
from mainsequence.markets.portfolios.models import (
    AssetsConfiguration,
    PortfolioConfigBaseModel,
)
from mainsequence.markets.portfolios.utils import TIMEDELTA


class ExternalWeightsConfig(PortfolioConfigBaseModel):
    signal_assets_configuration: AssetsConfiguration
    artifact_name: str
    bucket_name: str


class ExternalWeights(SignalWeights):
    @property
    def external_weights_config(self) -> ExternalWeightsConfig:
        if not isinstance(self.signal_configuration, ExternalWeightsConfig):
            raise TypeError(
                "ExternalWeights requires ExternalWeightsConfig as signal_configuration."
            )
        return self.signal_configuration

    @property
    def assets_configuration(self) -> AssetsConfiguration:
        return self.external_weights_config.signal_assets_configuration

    @property
    def artifact_name(self) -> str:
        return self.external_weights_config.artifact_name

    @property
    def bucket_name(self) -> str:
        return self.external_weights_config.bucket_name

    def maximum_forward_fill(self):
        return timedelta(days=1) - TIMEDELTA

    def get_explanation(self):
        explanation = (
            "### External Weights Source\n\n"
            f"This strategy represents weights from an artifact: {self.bucket_name}/{self.artifact_name}\n\n\n"
        )
        return explanation

    def get_asset_list(self) -> None | list:
        asset_category = AssetCategory.get(
            unique_identifier=self.assets_configuration.assets_category_unique_id
        )
        asset_list = Asset.filter(id__in=asset_category.assets)
        return asset_list

    def _calculate_signal_weights(self):
        source_artifact = Artifact.get(bucket__name=self.bucket_name, name=self.artifact_name)
        weights_source = pd.read_csv(source_artifact.content)

        weights_source["time_index"] = pd.to_datetime(weights_source["time_index"], utc=True)

        # convert figis in source data
        for asset in self.update_statistics.asset_list:
            weights_source.loc[weights_source["figi"] == asset.figi, "unique_identifier"] = (
                asset.unique_identifier
            )

        weights = weights_source[["time_index", "unique_identifier", "weight"]]
        weights.rename(columns={"weight": "signal_weight"}, inplace=True)
        weights.set_index(["time_index", "unique_identifier"], inplace=True)

        weights = self.update_statistics.filter_df_by_latest_value(weights)
        return weights
