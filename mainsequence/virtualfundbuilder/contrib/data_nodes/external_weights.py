from datetime import timedelta

import pandas as pd

from mainsequence.client import Asset, AssetCategory
from mainsequence.client.models_tdag import Artifact
from mainsequence.tdag.data_nodes import DataNode
from mainsequence.virtualfundbuilder.resource_factory.signal_factory import (
    WeightsBase,
    register_signal_class,
)
from mainsequence.virtualfundbuilder.utils import TIMEDELTA


@register_signal_class(register_in_agent=False)
class ExternalWeights(WeightsBase, DataNode):
    def __init__(self, artifact_name: str, bucket_name: str, *args, **kwargs):
        self.artifact_name = artifact_name
        self.bucket_name = bucket_name
        super().__init__(*args, **kwargs)

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

    def update(self, update_statistics: "UpdateStatistics"):
        source_artifact = Artifact.get(bucket__name=self.bucket_name, name=self.artifact_name)
        weights_source = pd.read_csv(source_artifact.content)

        weights_source["time_index"] = pd.to_datetime(weights_source["time_index"], utc=True)

        # convert figis in source data
        for asset in update_statistics.asset_list:
            weights_source.loc[weights_source["figi"] == asset.figi, "unique_identifier"] = (
                asset.unique_identifier
            )

        weights = weights_source[["time_index", "unique_identifier", "weight"]]
        weights.rename(columns={"weight": "signal_weight"}, inplace=True)
        weights.set_index(["time_index", "unique_identifier"], inplace=True)

        weights = update_statistics.filter_df_by_latest_value(weights)
        return weights
