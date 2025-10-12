"""
These data nodes do not serve any practical purpose but only exemplify creation and best practices.



"""
from typing import Dict, Union

import pandas as pd

from mainsequence.client import UpdateStatistics
from mainsequence.tdag.data_nodes import DataNode, APIDataNode
import mainsequence.client as msc
import numpy as np
from pydantic import BaseModel,Field


class VolatilityConfig(BaseModel):
    center: float = Field(
        ...,
        title="Standard Deviation",
        description="Standard deviation of the normal distribution (must be > 0).",
        examples=[0.1, 1.0, 2.5],\
        gt=0,  # constraint: strictly positive
        le=1e6,  # example upper bound (optional)
        multiple_of=0.0001,  # example precision step (optional)
    )
    skew: bool


class RandomDataNodeConfig(BaseModel):
    mean: float = Field(..., ignore_from_storage_hash=False, title="Mean",
                        description="Mean for the random normal distribution generator")
    std: VolatilityConfig = Field(VolatilityConfig(center=1, skew=True), ignore_from_storage_hash=True,
                                  title="Vol Config",
                                  description="Vol Configuration")


class DailyRandomNumber(DataNode):
    """
    Example Data Node that generates one random number every day  every day
    """

    def __init__(self,node_configuration:RandomDataNodeConfig, *args, **kwargs):
        """
        :param mean:  the mean of the probability  distribution
        :param std: the std of the probability  distribution
        :param kwargs:
        """
        self.node_configuration=node_configuration
        self.mean=node_configuration.mean
        self.std=node_configuration.std
        super().__init__(*args, **kwargs)

    def get_table_metadata(self)->msc.TableMetaData:
        TS_ID = f"example_random_number_{self.mean}_{self.std}"
        meta = msc.TableMetaData( identifier=TS_ID,
                                        description="Example Data Node")

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

    def dependencies(self) -> Dict[str, Union["DataNode", "APIDataNode"]]:
        """
        This nodes does not depend on any other data nodes.
        """
        return {}


class DailyRandomAddition(DataNode):
    def __init__(self,mean:float,std:float, *args, **kwargs):
        self.mean=mean
        self.std=std
        self.daily_random_number_data_node=DailyRandomNumber(node_configuration=RandomDataNodeConfig(mean=0.0)
                                                             ,*args, **kwargs)
        super().__init__(*args, **kwargs)
    def dependencies(self):
        return {"number_generator":self.daily_random_number_data_node}
    def update(self) -> pd.DataFrame:
        """Draw daily samples from N(mean, std) since last run (UTC days)."""
        today = pd.Timestamp.now("UTC").normalize()
        last = self.update_statistics.max_time_index_value
        if last is not None and last >= today:
            return pd.DataFrame()
        random_number=np.random.normal(self.mean, self.std)
        dependency_noise=self.daily_random_number_data_node.\
                get_df_between_dates(start_date=today, great_or_equal=True).iloc[0]["random_number"]
        self.logger.info(f"random_number={random_number} dependency_noise={dependency_noise}")

        return pd.DataFrame(
            {"random_number": [random_number+dependency_noise]},
            index=pd.DatetimeIndex([today], name="time_index", tz="UTC"),
        )

class DailyRandomAdditionAPI(DataNode):
    def __init__(self,mean:float,std:float,
                 dependency_identifier:int,
                 *args, **kwargs):
        self.mean=mean
        self.std=std

        self.daily_random_number_data_node=APIDataNode.build_from_identifier(identifier=dependency_identifier)
        super().__init__(*args, **kwargs)
    def dependencies(self):
        return {"number_generator":self.daily_random_number_data_node}
    def update(self) -> pd.DataFrame:
        """Draw daily samples from N(mean, std) since last run (UTC days)."""
        today = pd.Timestamp.now("UTC").normalize()
        last = self.update_statistics.max_time_index_value
        if last is not None and last >= today:
            return pd.DataFrame()
        random_number=np.random.normal(self.mean, self.std)
        dependency_noise=self.daily_random_number_data_node.\
                get_df_between_dates(start_date=today, great_or_equal=True).iloc[0]["random_number"]
        self.logger.info(f"random_number={random_number} dependency_noise={dependency_noise}")

        return pd.DataFrame(
            {"random_number": [random_number+dependency_noise]},
            index=pd.DatetimeIndex([today], name="time_index", tz="UTC"),
        )


def build_test_time_series():


    daily_node = DailyRandomNumber(node_configuration=RandomDataNodeConfig(mean=0.0))
    daily_node.run(debug_mode=True, force_update=True)

    daily_node = DailyRandomAddition(mean=0.0, std=1.0)
    daily_node.run(debug_mode=True, force_update=True)


    daily_node = DailyRandomAdditionAPI(mean=0.0, std=1.0,
                                        dependency_identifier=f"example_random_number_0.0_center=1.0 skew=True"
                                        )
    daily_node.run(debug_mode=True, force_update=True)

if __name__ == "__main__":
    build_test_time_series()  # Call your main function