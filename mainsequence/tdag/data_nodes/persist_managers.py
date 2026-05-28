from __future__ import annotations

import threading
from concurrent.futures import Future
from typing import Any

import pandas as pd

from mainsequence.client import TDAG_CONSTANTS as CONSTANTS
from mainsequence.client import DataNodeStorage, DataNodeUpdate, DynamicTableDataSource
from mainsequence.client.models_tdag import DataNodeUpdateDetails
from mainsequence.logconf import logger

from .. import future_registry
from ..base_persist_managers import (
    BasePersistManager,
)


class APIPersistManager:
    """
    Manages persistence for time series data accessed via an API.
    It handles asynchronous fetching of data_node_storage to avoid blocking operations.
    """

    def __init__(
        self,
        *,
        storage_hash: str | None = None,
        data_source_uid: str,
    ):
        if data_source_uid in (None, ""):
            raise ValueError("APIPersistManager requires data_source_uid.")
        self.data_source_uid: str = str(data_source_uid)
        self.storage_hash: str = storage_hash

        logger.debug(f"Initializing Time Serie {self.storage_hash}  as APIDataNode")

        self._data_node_storage_future = Future()
        future_registry.add_future(self._data_node_storage_future)
        thread = threading.Thread(
            target=self._init_data_node_storage,
            name=f"ApiDataNodeStorageThread-{self.storage_hash}",
            daemon=False,
        )
        thread.start()

    @property
    def data_node_storage(self) -> DataNodeStorage:
        if not hasattr(self, "_data_node_storage_cached"):
            self._data_node_storage_cached = self._data_node_storage_future.result()
        return self._data_node_storage_cached

    def _init_data_node_storage(self) -> None:
        try:
            result = DataNodeStorage.get_or_none(
                storage_hash=self.storage_hash,
                data_source__uid=self.data_source_uid,
                include_relations_detail=True,
            )
            self._data_node_storage_future.set_result(result)
        except Exception as exc:
            self._data_node_storage_future.set_exception(exc)
        finally:
            future_registry.remove_future(self._data_node_storage_future)

    def get_last_observation(
        self,
        *,
        dimension_filters: dict[str, list[Any]] | None = None,
        index_coordinates: list[dict[str, Any]] | None = None,
        dimension_range_map: list[dict[str, Any]] | None = None,
    ):
        last_observation = self.data_node_storage.get_last_observation(
            dimension_filters=dimension_filters,
            index_coordinates=index_coordinates,
            dimension_range_map=dimension_range_map,
        )
        return last_observation

    def get_df_between_dates(self, *args, **kwargs) -> pd.DataFrame:
        filtered_data = self.data_node_storage.get_data_between_dates_from_api(*args, **kwargs)
        if filtered_data.empty:
            return filtered_data

        stc = self.data_node_storage.sourcetableconfiguration
        filtered_data[stc.time_index_name] = pd.to_datetime(
            filtered_data[stc.time_index_name], utc=True
        )
        column_filter = kwargs.get("columns") or stc.column_dtypes_map.keys()
        for c in column_filter:
            c_type = stc.column_dtypes_map[c]
            if c != stc.time_index_name:
                if c_type == "object":
                    c_type = "str"
                filtered_data[c] = filtered_data[c].astype(c_type)
        filtered_data = filtered_data.set_index(stc.index_names)

        return filtered_data


class PersistManager(BasePersistManager):
    STORAGE_CLASS = DataNodeStorage
    UPDATE_CLASS = DataNodeUpdate
    UPDATE_DETAILS_CLASS = DataNodeUpdateDetails
    UPDATE_GET_OR_NONE_DATASOURCE_LOOKUP = "remote_table__data_source__uid"
    UPDATE_CREATE_STORAGE_LOOKUP = "remote_table__hash_id"

    @classmethod
    def get_from_data_type(
        cls, data_source: DynamicTableDataSource, *args, **kwargs
    ) -> PersistManager:
        data_type = data_source.related_resource_class_type
        if data_type in CONSTANTS.DATA_SOURCE_TYPE_TIMESCALEDB:
            return TimeScaleLocalPersistManager(data_source=data_source, *args, **kwargs)
        else:
            return TimeScaleLocalPersistManager(data_source=data_source, *args, **kwargs)


class TimeScaleLocalPersistManager(PersistManager):
    """
    Main Controller to interact with backend-backed table storage.
    """

    def get_table_schema(self, _):
        return self.data_node_storage["sourcetableconfiguration"]["column_dtypes_map"]
