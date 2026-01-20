from __future__ import annotations

import datetime

import pandas as pd
import pytz
from pydantic import BaseModel, Field

import mainsequence.client as msc
from mainsequence.tdag import APIDataNode, DataNode

from .curve_codec import compress_curve_to_string
from .registry import (
    DISCOUNT_CURVE_BUILDERS,
    FIXING_RATE_BUILDERS,
)

UTC = pytz.UTC

class CurveConfig(BaseModel):
    curve_const: str = Field(..., description="Constant name, e.g. ZERO_CURVE__VALMER_TIIE_28",
                             ignore_from_storage_hash=True
                             )
    name: str = Field(..., description="Display name",
                      ignore_from_storage_hash=True
                      )
    curve_points_dependency_node_uid: str | None = Field(None, title="Dependecies curve points", description="",
                                                                ignore_from_storage_hash=True
                                                                )

class RateConfig(BaseModel):
    rate_const: str = Field(..., description="Constant name, e.g. REFERENCE_RATE__TIIE_28",
                            ignore_from_storage_hash=True
                            )
    name: str = Field(..., title="asset name", description="string name of curve to create",
                      ignore_from_storage_hash=True
                      )

class FixingRateConfig(BaseModel):
    rates: list[RateConfig] = Field(..., title="Interest rates build",
                                                description="string name of curve to create",
                                                ignore_from_storage_hash=True
                                                )


class DiscountCurvesNode(DataNode):
    OFFSET_START = datetime.datetime(1990, 1, 1, tzinfo=UTC)

    def __init__(self, curve_config: CurveConfig, *args, **kwargs):
        self.curve_config = curve_config

        self.base_node_curve_points = None
        if curve_config.curve_points_dependency_node_uid:
            self.base_node_curve_points = APIDataNode.build_from_identifier(
                identifier=curve_config.curve_points_dependency_node_uid
            )
        super().__init__(*args, **kwargs)

    def dependencies(self) -> dict[str, DataNode | APIDataNode]:
        if self.base_node_curve_points is None:
            return {}
        return {self.curve_config.curve_points_dependency_node_uid: self.base_node_curve_points}

    def get_asset_list(self):
        curve_uid = DISCOUNT_CURVE_BUILDERS.uid(self.curve_config.curve_const)  # VALUE
        payload = [{
            "unique_identifier": curve_uid,
            "snapshot": {"name": self.curve_config.name, "ticker": curve_uid},
        }]
        return msc.Asset.batch_get_or_register_custom_assets(payload)

    def update(self):
        curve_uid = DISCOUNT_CURVE_BUILDERS.uid(self.curve_config.curve_const)
        builder = DISCOUNT_CURVE_BUILDERS.builder_for_const(self.curve_config.curve_const)

        df = builder(
            update_statistics=self.update_statistics,
            curve_unique_identifier=curve_uid,
            base_node_curve_points=self.base_node_curve_points,
        )
        if df.empty:
            return pd.DataFrame()

        # compress curve dict -> string
        df["curve"] = df["curve"].apply(compress_curve_to_string)

        last = self.update_statistics.get_last_update_index_2d(curve_uid)
        df = df[df.index.get_level_values("time_index") > last]
        return df if not df.empty else pd.DataFrame()

    def get_table_metadata(self) -> msc.TableMetaData:
        return msc.TableMetaData(
            identifier="discount_curves",
            data_frequency_id=msc.DataFrequency.one_d,
            description="Collection of Discount Curves",
        )

    def get_column_metadata(self) -> list[msc.ColumnMetaData]:
        return [msc.ColumnMetaData(column_name="curve", dtype="str", label="Compressed Curve", description="Compressed Discount Curve")]

class FixingRatesNode(DataNode):
    OFFSET_START = datetime.datetime(1990, 1, 1, tzinfo=UTC)

    def __init__(self, rates_config: FixingRateConfig, *args, **kwargs):
        self.rates_config = rates_config
        super().__init__(*args, **kwargs)

    def get_asset_list(self):
        payload = []
        for rc in self.rates_config.rates:
            uid = FIXING_RATE_BUILDERS.uid(rc.rate_const)
            payload.append({"unique_identifier": uid, "snapshot": {"name": rc.name, "ticker": uid}})
        return msc.Asset.batch_get_or_register_custom_assets(payload)

    def dependencies(self):
        return {}

    def update(self):
        dfs = []
        for asset in self.update_statistics.asset_list:
            builder = FIXING_RATE_BUILDERS.builder_for_uid(asset.unique_identifier)
            df = builder(update_statistics=self.update_statistics, unique_identifier=asset.unique_identifier)
            if not df.empty:
                dfs.append(df)

        if not dfs:
            return pd.DataFrame()

        out = pd.concat(dfs, axis=0)
        assert out.index.names == ["time_index", "unique_identifier"]
        return out[["rate"]].dropna()

    def get_table_metadata(self) -> msc.TableMetaData:
        return msc.TableMetaData(
            identifier="fixing_rates_1d",
            data_frequency_id=msc.DataFrequency.one_d,
            description="Daily fixing rates ",
        )

    def get_column_metadata(self) -> list[msc.ColumnMetaData]:
        return [
            msc.ColumnMetaData(
                column_name="rate",
                dtype="float",
                label="Fixing Rate (decimal)",
                description="Fixing value normalized to decimal (percentage/100).",
            ),
        ]
