from __future__ import annotations

import threading
from concurrent.futures import Future
from typing import Any

import pandas as pd

from mainsequence.client import DataNodeStorage, DataNodeUpdate
from mainsequence.client.dtype_codec import TIMESTAMP_TZ, token_to_pandas_series
from mainsequence.client.models_tdag import DataNodeUpdateDetails
from mainsequence.logconf import logger

from .. import future_registry
from ..base_persist_managers import (
    BasePersistManager,
)


class APIPersistManager:
    """
    Manages persistence for time series data accessed via an API.
    It handles asynchronous fetching of the storage table to avoid blocking operations.
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

        self._storage_table_future = Future()
        future_registry.add_future(self._storage_table_future)
        thread = threading.Thread(
            target=self._init_storage_table,
            name=f"ApiStorageTableThread-{self.storage_hash}",
            daemon=False,
        )
        thread.start()

    @property
    def storage_table(self) -> DataNodeStorage:
        if not hasattr(self, "_storage_table_cached"):
            self._storage_table_cached = self._storage_table_future.result()
        return self._storage_table_cached

    def _init_storage_table(self) -> None:
        try:
            result = DataNodeStorage.get_or_none(
                storage_hash=self.storage_hash,
                data_source__uid=self.data_source_uid,
                include_relations_detail=True,
            )
            self._storage_table_future.set_result(result)
        except Exception as exc:
            self._storage_table_future.set_exception(exc)
        finally:
            future_registry.remove_future(self._storage_table_future)

    def get_last_observation(
        self,
        *,
        dimension_filters: dict[str, list[Any]] | None = None,
        index_coordinates: list[dict[str, Any]] | None = None,
        dimension_range_map: list[dict[str, Any]] | None = None,
    ):
        last_observation = self.storage_table.get_last_observation(
            dimension_filters=dimension_filters,
            index_coordinates=index_coordinates,
            dimension_range_map=dimension_range_map,
        )
        return last_observation

    def get_df_between_dates(self, *args, **kwargs) -> pd.DataFrame:
        filtered_data = self.storage_table.get_data_between_dates_from_api(*args, **kwargs)
        if filtered_data.empty:
            return filtered_data

        stc = self.storage_table.sourcetableconfiguration
        filtered_data[stc.time_index_name] = token_to_pandas_series(
            filtered_data[stc.time_index_name],
            TIMESTAMP_TZ,
            is_time_index=True,
        )
        column_filter = kwargs.get("columns") or stc.column_dtypes_map.keys()
        for c in column_filter:
            c_type = stc.column_dtypes_map[c]
            if c in filtered_data.columns:
                filtered_data[c] = token_to_pandas_series(
                    filtered_data[c],
                    c_type,
                    is_time_index=c == stc.time_index_name,
                )
        filtered_data = filtered_data.set_index(stc.index_names)

        return filtered_data


class PersistManager(BasePersistManager):
    UPDATE_CLASS = DataNodeUpdate
    UPDATE_DETAILS_CLASS = DataNodeUpdateDetails
    UPDATE_GET_OR_NONE_DATASOURCE_LOOKUP = "remote_table__data_source__uid"
    UPDATE_CREATE_STORAGE_LOOKUP = "meta_table_uid"

    @classmethod
    def get_from_storage_table(cls, storage_table: Any | None, *args, **kwargs) -> PersistManager:
        return TimeScaleLocalPersistManager(storage_table=storage_table, *args, **kwargs)


class TimeScaleLocalPersistManager(PersistManager):
    """
    Main Controller to interact with backend-backed table storage.
    """

    def get_table_schema(self, _):
        return self.storage_table["sourcetableconfiguration"]["column_dtypes_map"]
