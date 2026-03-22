"""
These data nodes do not serve any practical purpose but only exemplify creation and best practices.



"""

import os
from typing import Union

import numpy as np
import pandas as pd
from pydantic import BaseModel, Field

import mainsequence.client as msc
from mainsequence.tdag.data_nodes import (
    APIDataNode,
    DataNode,
    DataNodeConfiguration,
    hash_namespace,
)

PROJECT_ID = os.getenv("MAIN_SEQUENCE_PROJECT_ID", "local").strip() or "local"


class VolatilityConfig(BaseModel):
    center: float = Field(
        ...,
        title="Standard Deviation",
        description="Standard deviation of the normal distribution (must be > 0).",
        examples=[0.1, 1.0, 2.5],
        gt=0,  # constraint: strictly positive
        le=1e6,  # example upper bound (optional)
        multiple_of=0.0001,  # example precision step (optional)
    )
    skew: bool


class RandomDataNodeConfig(DataNodeConfiguration):
    mean: float = Field(
        ...,
        title="Mean",
        description="Mean for the random normal distribution generator",
    )
    std: VolatilityConfig = Field(
        VolatilityConfig(center=1, skew=True),
        json_schema_extra={"update_only": True},
        title="Vol Config",
        description="Vol Configuration",
    )


class DailyRandomAdditionConfig(DataNodeConfiguration):
    mean: float
    std: float


class DailyRandomAdditionAPIConfig(DataNodeConfiguration):
    mean: float
    std: float
    dependency_identifier: int


class DailyRandomNumber(DataNode):
    """
    Example Data Node that generates one random number every day  every day
    """

    def __init__(self, node_configuration: RandomDataNodeConfig, *args, **kwargs):
        """
        :param mean:  the mean of the probability  distribution
        :param std: the std of the probability  distribution
        :param kwargs:
        """
        self.node_configuration = node_configuration
        self.mean = node_configuration.mean
        self.std = node_configuration.std
        super().__init__(config=node_configuration, *args, **kwargs)

    def get_table_metadata(self) -> msc.TableMetaData:
        TS_ID = f"example_random_number_{PROJECT_ID}_{self.mean}"

        meta = msc.TableMetaData(identifier=TS_ID, description="Example Data Node")
        return meta

    def update(self) -> pd.DataFrame:
        """Draw daily samples from N(mean, std) since last run (UTC days)."""
        today = pd.Timestamp.now("UTC").normalize()
        last = self.update_statistics.max_time_index_value
        if last is not None and last >= today:
            return pd.DataFrame()
        return pd.DataFrame(
            {"random_number": [np.random.normal(self.mean, self.std.center)]},
            index=pd.DatetimeIndex([today], name="time_index", tz="UTC"),
        )

    def dependencies(self) -> dict[str, Union["DataNode", "APIDataNode"]]:
        """
        This nodes does not depend on any other data nodes.
        """
        return {}


class DailyRandomAddition(DataNode):
    def __init__(self, addition_config: DailyRandomAdditionConfig, *args, **kwargs):
        self.addition_config = addition_config
        self.mean = addition_config.mean
        self.std = addition_config.std
        self.daily_random_number_data_node = DailyRandomNumber(
            node_configuration=RandomDataNodeConfig(mean=0.0), *args, **kwargs
        )
        super().__init__(config=addition_config, *args, **kwargs)

    def dependencies(self):
        return {"number_generator": self.daily_random_number_data_node}

    def update(self) -> pd.DataFrame:
        """Draw daily samples from N(mean, std) since last run (UTC days)."""
        today = pd.Timestamp.now("UTC").normalize()
        last = self.update_statistics.max_time_index_value
        if last is not None and last >= today:
            return pd.DataFrame()
        random_number = np.random.normal(self.mean, self.std)
        dependency_noise = self.daily_random_number_data_node.get_df_between_dates(
            start_date=today, great_or_equal=True
        ).iloc[0]["random_number"]
        self.logger.info(f"random_number={random_number} dependency_noise={dependency_noise}")

        return pd.DataFrame(
            {"random_number": [random_number + dependency_noise]},
            index=pd.DatetimeIndex([today], name="time_index", tz="UTC"),
        )


class DailyRandomAdditionAPI(DataNode):
    def __init__(self, addition_api_config: DailyRandomAdditionAPIConfig, *args, **kwargs):
        self.addition_api_config = addition_api_config
        self.mean = addition_api_config.mean
        self.std = addition_api_config.std

        self.daily_random_number_data_node = APIDataNode.build_from_identifier(
            identifier=addition_api_config.dependency_identifier
        )
        super().__init__(config=addition_api_config, *args, **kwargs)

    def dependencies(self):
        return {"number_generator": self.daily_random_number_data_node}

    def update(self) -> pd.DataFrame:
        """Draw daily samples from N(mean, std) since last run (UTC days)."""
        today = pd.Timestamp.now("UTC").normalize()
        last = self.update_statistics.max_time_index_value
        if last is not None and last >= today:
            return pd.DataFrame()
        random_number = np.random.normal(self.mean, self.std)
        dependency_noise = self.daily_random_number_data_node.get_df_between_dates(
            start_date=today, great_or_equal=True
        ).iloc[0]["random_number"]
        self.logger.info(f"random_number={random_number} dependency_noise={dependency_noise}")

        return pd.DataFrame(
            {"random_number": [random_number + dependency_noise]},
            index=pd.DatetimeIndex([today], name="time_index", tz="UTC"),
        )


def run_graph(label: str):
    print(f"\n===== {label} =====")

    # 1) Standalone node
    dn = DailyRandomNumber(node_configuration=RandomDataNodeConfig(mean=0.0))
    print(f"{label} DailyRandomNumber.update_hash  = {dn.update_hash}")
    print(f"{label} DailyRandomNumber.storage_hash = {dn.storage_hash}")
    dn.run(debug_mode=True, force_update=True)

    # This is the identifier we will use for the APIDataNode example.
    # After run(), backend metadata should be patched with get_table_metadata().
    dep_identifier = dn.get_table_metadata().identifier
    print(f"{label} dependency identifier = {dep_identifier}")

    # 2) Node with DataNode dependency (will reuse same underlying table if hashes match)
    add = DailyRandomAddition(addition_config=DailyRandomAdditionConfig(mean=0.0, std=1.0))
    print(f"{label} DailyRandomAddition.update_hash  = {add.update_hash}")
    print(f"{label} DailyRandomAddition.storage_hash = {add.storage_hash}")
    print(f"{label}   dep(update_hash) = {add.daily_random_number_data_node.update_hash}")
    print(f"{label}   dep(storage_hash)= {add.daily_random_number_data_node.storage_hash}")
    add.run(debug_mode=True, force_update=True)

    # 3) Node with API dependency (identifier-based)
    api = DailyRandomAdditionAPI(
        addition_api_config=DailyRandomAdditionAPIConfig(
            mean=0.0,
            std=1.0,
            dependency_identifier=dep_identifier,
        )
    )
    print(f"{label} DailyRandomAdditionAPI.update_hash  = {api.update_hash}")
    print(f"{label} DailyRandomAdditionAPI.storage_hash = {api.storage_hash}")
    api.run(debug_mode=True, force_update=True)


def build_test_time_series():
    # -------------------------
    # A) NORMAL / PRODUCTION
    # -------------------------
    run_graph("PROD(no namespace)")

    # -------------------------
    # B) TEST / NAMESPACED
    # -------------------------
    # Everything created inside gets different update_hash/storage_hash
    # so it goes to different tables.
    with hash_namespace("pytest"):
        run_graph("TEST(namespace=pytest)")


if __name__ == "__main__":
    build_test_time_series()
