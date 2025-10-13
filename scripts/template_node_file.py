from typing import Union

import pandas as pd
from pydantic import BaseModel, Field

import mainsequence.client as msc
from mainsequence.tdag import APIDataNode, DataNode


class NodeConfiguration(BaseModel):
    argument_1: float = Field(
        ...,
        ignore_from_storage_hash=False,
        title="Argument 1",
        description="Argument 1 placeholder this argument will be taking in count for update and storage",
    )
    argument_2: float = Field(
        ...,
        ignore_from_storage_hash=True,
        title="Argument 2",
        description="Argument 2 placeholder this argument will not be taking in count for update and storage",
    )


class SampleNode(DataNode):

    def __init__(self, configuration: NodeConfiguration, *args, **kwargs):
        self.node_configuration = configuration
        super().__init__(*args, **kwargs)

    def dependencies(self) -> dict[str, Union["DataNode", "APIDataNode"]]: ...

    def get_table_metadata(self) -> msc.TableMetaData: ...

    def get_column_metadata(self) -> list[msc.ColumnMetaData]: ...

    def get_table_metadata(self) -> msc.TableMetaData: ...
    def update(self) -> pd.DataFrame: ...
