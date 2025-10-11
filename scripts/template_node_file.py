from typing import Dict, Union, List,Optional
from mainsequence.tdag import DataNode, APIDataNode
import mainsequence.client as msc
import pandas as pd

from pydantic import BaseModel, Field


class NodeConfiguration(BaseModel):
    argument_1: float = Field(..., ignore_from_storage_hash=False, title="Argument 1",
                              description="Argument 1 placeholder this argument will be taking in count for update and storage")
    argument_2: float = Field(..., ignore_from_storage_hash=True, title="Argument 2",
                              description="Argument 2 placeholder this argument will not be taking in count for update and storage")


class SampleNode(DataNode):

    def __init__(self, configuration: NodeConfiguration, *args, **kwargs):
        self.node_configuration = configuration
        super().__init__(*args, **kwargs)

    def dependencies(self) -> Dict[str, Union["DataNode", "APIDataNode"]]:
        ...

    def get_table_metadata(self) -> msc.TableMetaData:
        ...

    def get_column_metadata(self) -> List[msc.ColumnMetaData]:
        ...

    def get_table_metadata(self) -> msc.TableMetaData:
        ...
    def update(self) -> pd.DataFrame:
        ...