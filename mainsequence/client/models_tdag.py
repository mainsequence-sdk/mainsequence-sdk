from __future__ import annotations

import base64
import concurrent.futures
import copy
import datetime
import gzip
import json
import math
import os
import time
from threading import RLock
from typing import Any, ClassVar

import numpy as np
import pandas as pd
import pytz
import requests
import yaml
from cachetools import TTLCache, cachedmethod
from pydantic import BaseModel, Field, field_validator

from mainsequence.logconf import logger

from . import exceptions
from .base import TDAG_ENDPOINT, BaseObjectOrm, BasePydanticModel
from .data_sources_interfaces import timescale as TimeScaleInterface
from .data_sources_interfaces.duckdb import DuckDBInterface
from .utils import (
    TDAG_CONSTANTS,
    AuthLoaders,
    DataFrequency,
    DateInfo,
    UniqueIdentifierRangeMap,
    bios_uuid,
    get_network_ip,
    is_process_running,
    make_request,
    request_to_datetime,
    serialize_to_json,
    set_types_in_table,
)

_default_data_source = None  # Module-level cache

JSON_COMPRESSED_PREFIX = ["json_compressed", "jcomp_"]

loaders = AuthLoaders()

# Global executor (or you could define one on your class)
_executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)
DUCK_DB = "duck_db"


class AlreadyExist(Exception):
    pass


def build_session(loaders):
    from requests.adapters import HTTPAdapter, Retry

    s = requests.Session()
    s.headers.update(loaders.auth_headers)
    retries = Retry(
        total=2,
        backoff_factor=2,
    )
    s.mount("http://", HTTPAdapter(max_retries=retries))
    return s


session = build_session(loaders=loaders)


class SchedulerDoesNotExist(Exception):
    pass


class LocalTimeSeriesDoesNotExist(Exception):
    pass


class DynamicTableDoesNotExist(Exception):
    pass


class SourceTableConfigurationDoesNotExist(Exception):
    pass


class ColumnMetaData(BasePydanticModel, BaseObjectOrm):
    source_config_id: int | None = Field(
        None,

        description="Primary key of the related SourceTableConfiguration",
    )
    column_name: str = Field(
        ..., max_length=63, description="Name of the column (must match column_dtypes_map key)"
    )
    dtype: str = Field(
        ...,
        max_length=100,
        description="Data type (will be synced from the configuration’s dtype map)",
    )
    label: str = Field(..., max_length=250, description="Human‐readable label")
    description: str = Field(..., description="Longer description of the column")


class SourceTableConfiguration(BasePydanticModel, BaseObjectOrm):
    id: int | None = Field(None, description="Primary key, auto-incremented ID")
    related_table: int | DataNodeStorage
    time_index_name: str = Field(..., max_length=100, description="Time index name")
    column_dtypes_map: dict[str, Any] = Field(..., description="Column data types map")
    index_names: list
    last_time_index_value: datetime.datetime | None = Field(
        None, description="Last time index value"
    )
    earliest_index_value: datetime.datetime | None = Field(None, description="Earliest index value")

    # multi_index_stats: Optional[Dict[str, Any]] = Field(None, description="Multi-index statistics JSON field")
    # multi_index_column_stats:Optional[Dict[str, Any]] = Field(None, description="Multi-index statistics JSON field column based")

    table_partition: dict[str, Any] = Field(..., description="Table partition settings")
    open_for_everyone: bool = Field(
        default=False, description="Whether the table configuration is open for everyone"
    )
    columns_metadata: list[ColumnMetaData] | None = None

    # todo remove
    column_index_names: list | None = [None]

    def get_data_updates(self):
        max_per_asset = None

        url = self.get_object_url() + f"/{self.related_table}/get_stats/"
        s = self.build_session()
        r = make_request(s=s, loaders=self.LOADERS, r_type="GET", url=url, accept_gzip=True)
        if r.status_code != 200:
            raise Exception(r.text)
        data = r.json()
        multi_index_stats = data["multi_index_stats"]
        multi_index_column_stats = data["multi_index_column_stats"]
        max_time_index_value = self.last_time_index_value
        if multi_index_stats is not None:
            max_per_asset = multi_index_stats["max_per_asset_symbol"]
            max_per_asset = {k: request_to_datetime(v) for k, v in max_per_asset.items()}
            max_time_index_value = np.max(list(max_per_asset.values()))

        du = UpdateStatistics(
            max_time_index_value=max_time_index_value,
            asset_time_statistics=max_per_asset,
            multi_index_column_stats=multi_index_column_stats,
        )

        du._max_time_in_update_statistics = max_time_index_value
        return du

    def get_time_scale_extra_table_indices(self) -> dict:
        url = self.get_object_url() + f"/{self.related_table}/get_time_scale_extra_table_indices/"
        s = self.build_session()
        r = make_request(
            s=s,
            loaders=self.LOADERS,
            r_type="GET",
            url=url,
        )
        if r.status_code != 200:
            raise Exception(r.text)
        return r.json()

    def set_or_update_columns_metadata(
        self, columns_metadata: list[ColumnMetaData], timeout=None
    ) -> None:
        """ """

        columns_metadata = [c.model_dump(exclude={"orm_class"}) for c in columns_metadata]
        url = self.get_object_url() + f"/{self.related_table}/set_or_update_columns_metadata/"
        s = self.build_session()
        r = make_request(
            s=s,
            loaders=self.LOADERS,
            r_type="POST",
            time_out=timeout,
            url=url,
            payload={"json": {"columns_metadata": columns_metadata}},
        )
        if r.status_code not in [200, 201]:
            raise Exception(r.text)


        return r.json()

    def patch(self, *args, **kwargs):
        # related table is the primary key of this model
        if isinstance(self.related_table, int):
            id = self.related_table
        else:
            id = self.related_table.id
        return self.__class__.patch_by_id(id, *args, **kwargs)





class DataNodeUpdate(BasePydanticModel, BaseObjectOrm):
    id: int | None = Field(None, description="Primary key, auto-incremented ID")
    update_hash: str = Field(..., max_length=63, description="Max length of PostgreSQL table name")
    data_node_storage: int | DataNodeStorage
    build_configuration: dict[str, Any] = Field(..., description="Configuration in JSON format")
    build_meta_data: dict[str, Any] | None = Field(None, description="Optional YAML metadata")
    ogm_dependencies_linked: bool = Field(default=False, description="OGM dependencies linked flag")
    tags: list[str] | None = Field(default=[], description="List of tags")
    description: str | None = Field(None, description="Optional HTML description")
    update_details: DataNodeUpdateDetails | int | None = None
    run_configuration: RunConfiguration | None = None
    open_for_everyone: bool = Field(
        default=False, description="Whether the ts is open for everyone"
    )

    @property
    def data_source_id(self):
        if isinstance(self.data_node_storage.data_source, int):
            return self.data_node_storage.data_source
        else:
            return self.data_node_storage.data_source.id

    @classmethod
    def get_or_create(cls, **kwargs):
        url = cls.get_object_url() + "/get_or_create/"
        kwargs = serialize_to_json(kwargs)

        payload = {"json": kwargs}
        s = cls.build_session()
        r = make_request(s=s, loaders=cls.LOADERS, r_type="POST", url=url, payload=payload)
        if r.status_code not in [200, 201]:
            raise Exception(r.text)
        data = r.json()

        return cls(**data)

    def add_tags(self, tags: list, timeout=None):
        base_url = self.get_object_url()
        s = self.build_session()
        payload = {"json": {"tags": tags}}
        # r = self.s.get(, )
        url = f"{base_url}/{self.id}/add_tags/"
        r = make_request(
            s=s, loaders=self.LOADERS, r_type="PATCH", url=url, payload=payload, time_out=timeout
        )
        if r.status_code != 200:
            raise Exception(f"Error in request {r.json()}")
        return r.json()

    @classmethod
    def filter_by_hash_id(cls, local_hash_id_list: list, timeout=None):
        s = cls.build_session()
        base_url = cls.get_object_url()
        url = f"{base_url}/filter_by_hash_id/"
        payload = {
            "json": {"local_hash_id__in": local_hash_id_list},
        }
        r = make_request(
            s=s, loaders=cls.LOADERS, r_type="POST", url=url, payload=payload, time_out=timeout
        )
        if r.status_code != 200:
            raise Exception(f"{r.text}")
        all_data_node_storage = {m["update_hash"]: m for m in r.json()}
        return all_data_node_storage

    def set_start_of_execution(self, **kwargs):
        s = self.build_session()
        base_url = self.get_object_url()
        payload = {"json": kwargs}
        url = f"{base_url}/{self.id}/set_start_of_execution/"
        r = make_request(
            s=s, loaders=self.LOADERS, r_type="PATCH", url=url, payload=payload, accept_gzip=True
        )
        if r.status_code != 201:
            raise Exception(f"Error in request {r.text}")

        def _recurse_to_datetime(node):
            if isinstance(node, dict):
                return {k: _recurse_to_datetime(v) for k, v in node.items()}
            # leaf: assume it’s your timestamp string
            return request_to_datetime(node)

        result = r.json()
        if result["last_time_index_value"] is not None:
            datetime.datetime.fromtimestamp(result["last_time_index_value"], tz=pytz.utc)

        if result["asset_time_statistics"] is not None:
            result["asset_time_statistics"] = _recurse_to_datetime(result["asset_time_statistics"])


        hu = LocalTimeSeriesHistoricalUpdate(
            **result["historical_update"],
            update_statistics=UpdateStatistics(
                asset_time_statistics=result["asset_time_statistics"],
                max_time_index_value=result["last_time_index_value"],
                multi_index_column_stats=result["multi_index_column_stats"],
            ),
            must_update=result["must_update"],
            direct_dependencies_ids=result["direct_dependencies_ids"],
        )
        return hu

    def set_end_of_execution(
        self, historical_update_id: int, timeout=None, threaded_request=True, **kwargs
    ):
        s = self.build_session()
        url = self.get_object_url() + f"/{self.id}/set_end_of_execution/"
        kwargs.update(dict(historical_update_id=historical_update_id))
        payload = {"json": kwargs}

        def _do_request():
            r = make_request(
                s=s,
                loaders=self.LOADERS,
                r_type="PATCH",
                url=url,
                payload=payload,
                time_out=timeout,
            )
            if r.status_code != 200:
                raise Exception("Error in request")
            return r

        if threaded_request:
            # Submit the request to an executor. The returned Future will be non-blocking.
            future = _executor.submit(_do_request)

            # Optionally, attach a callback to log failures. (Exceptions will also be
            # re-raised when someone calls future.result().)
            def _handle_exception(fut):
                try:
                    fut.result()  # This will re-raise any exception caught in _do_request.
                except Exception as e:
                    logger.error("set_end_of_execution: request failed: %s", e)

            future.add_done_callback(_handle_exception)
            return future
        else:
            # Synchronous execution that will raise exceptions inline.
            return _do_request()

    @classmethod
    def batch_set_end_of_execution(cls, update_map: dict, timeout=None):
        s = cls.build_session()
        url = f"{cls.get_object_url()}/batch_set_end_of_execution/"
        payload = {"json": {"update_map": update_map}}
        r = make_request(
            s=s, loaders=cls.LOADERS, r_type="PATCH", url=url, payload=payload, time_out=timeout
        )
        if r.status_code != 200:
            raise Exception("Error in request ")

    @classmethod
    def set_last_update_index_time(cls, data_node_storage, timeout=None):
        s = cls.build_session()
        url = cls.get_object_url() + f"/{data_node_storage['id']}/set_last_update_index_time/"
        r = make_request(s=s, loaders=cls.LOADERS, r_type="GET", url=url, time_out=timeout)

        if r.status_code == 404:
            raise SourceTableConfigurationDoesNotExist

        if r.status_code != 200:
            raise Exception(f"{data_node_storage['update_hash']}{r.text}")
        return r

    def set_last_update_index_time_from_update_stats(
        self,
        last_time_index_value: float,
        max_per_asset_symbol,
        multi_index_column_stats,
        timeout=None,
    ) -> DataNodeUpdate:
        s = self.build_session()
        url = self.get_object_url() + f"/{self.id}/set_last_update_index_time_from_update_stats/"

        data_to_comp = {
            "last_time_index_value": last_time_index_value,
            "max_per_asset_symbol": max_per_asset_symbol,
            "multi_index_column_stats": multi_index_column_stats,
        }
        chunk_json_str = json.dumps(data_to_comp)
        compressed = gzip.compress(chunk_json_str.encode("utf-8"))
        compressed_b64 = base64.b64encode(compressed).decode("utf-8")
        payload = dict(
            json={
                "data": compressed_b64,  # compres
            }
        )

        r = make_request(
            s=s, loaders=self.LOADERS, payload=payload, r_type="POST", url=url, time_out=timeout
        )

        if r.status_code == 404:
            raise SourceTableConfigurationDoesNotExist

        if r.status_code != 200:
            raise Exception(f"{self.update_hash}{r.text}")
        return DataNodeUpdate(**r.json())

    @classmethod
    def create_historical_update(cls, *args, **kwargs):
        s = cls.build_session()
        base_url = cls.ENDPOINT["LocalTimeSerieHistoricalUpdate"]
        data = serialize_to_json(kwargs)
        payload = {
            "json": data,
        }
        r = make_request(
            s=s, loaders=cls.LOADERS, r_type="POST", url=f"{base_url}/", payload=payload
        )
        if r.status_code != 201:
            raise Exception(f"Error in request {r.url} {r.text}")

    @classmethod
    def get_mermaid_dependency_diagram(
        cls, update_hash, data_source_id, desc=True, timeout=None
    ) -> dict:
        s = cls.build_session()
        url = (
            cls.get_object_url("DataNode")
            + f"/{update_hash}/dependencies_graph_mermaid?desc={desc}&data_source_id={data_source_id}"
        )
        r = make_request(s=s, loaders=cls.LOADERS, r_type="GET", url=url, time_out=timeout)
        if r.status_code != 200:
            raise Exception(f"Error in request {r.text}")

        return r.json()

    def get_all_dependencies_update_priority(self, timeout=None) -> pd.DataFrame:
        s = self.build_session()
        url = self.get_object_url() + f"/{self.id}/get_all_dependencies_update_priority/"
        r = make_request(s=s, loaders=self.LOADERS, r_type="GET", url=url, time_out=timeout)
        if r.status_code != 200:
            raise Exception(f"Error in request {r.text}")

        depth_df = pd.DataFrame(r.json())

        if not depth_df.empty:
            # hot fix for compatiblity with backend
            depth_df = depth_df.rename(columns={"local_time_serie_id": "data_node_update_id"})

        return depth_df

    @classmethod
    def get_upstream_nodes(cls, storage_hash, data_source_id, timeout=None):
        s = cls.build_session()
        url = (
            cls.get_object_url("DataNode")
            + f"/{storage_hash}/get_upstream_nodes?data_source_id={data_source_id}"
        )
        r = make_request(s=s, loaders=cls.LOADERS, r_type="GET", url=url, time_out=timeout)
        if r.status_code != 200:
            raise Exception(f"Error in request {r.text}")

        depth_df = pd.DataFrame(r.json())
        return depth_df

    @classmethod
    def create(cls, timeout=None, *args, **kwargs):
        url = cls.get_object_url("DataNode") + "/"
        payload = {"json": serialize_to_json(kwargs)}
        s = cls.build_session()
        r = make_request(
            s=s, loaders=cls.LOADERS, r_type="POST", url=url, payload=payload, time_out=timeout
        )
        if r.status_code != 201:
            raise Exception(f"Error in request {r.text}")
        instance = cls(**r.json())
        return instance

    def verify_if_direct_dependencies_are_updated(self) -> dict:
        """
        Response({
            "error_on_update_dependencies": False,
            "updated": all_success,
        })
        """
        s = self.build_session()
        url = self.get_object_url() + f"/{self.id}/verify_if_direct_dependencies_are_updated/"
        r = make_request(s=s, loaders=None, r_type="GET", url=url)
        if r.status_code != 200:
            raise Exception(f"Error in request: {r.text}")
        return r.json()

    def get_data_between_dates_from_api(self, *args, **kwargs):

        return self.data_node_storage.get_data_between_dates_from_api(*args, **kwargs)

    @classmethod
    def insert_data_into_table(
        cls, data_node_update_id, records: list[dict], overwrite=True, add_insertion_time=False
    ):
        s = cls.build_session()
        url = cls.get_object_url() + f"/{data_node_update_id}/insert_data_into_table/"

        chunk_json_str = json.dumps(records)
        compressed = gzip.compress(chunk_json_str.encode("utf-8"))
        compressed_b64 = base64.b64encode(compressed).decode("utf-8")

        payload = dict(
            json={
                "data": compressed_b64,  # compressed JSON data
                "chunk_stats": None,
                "overwrite": overwrite,
                "chunk_index": 0,
                "total_chunks": 1,
            }
        )

        try:
            r = make_request(
                s=s, loaders=None, payload=payload, r_type="POST", url=url, time_out=60 * 15
            )
            if r.status_code not in [200, 204]:
                logger.warning(f"Error in request: {r.text}")
            logger.info("Chunk uploaded successfully.")
        except requests.exceptions.RequestException as e:
            logger.exception(f"Error uploading chunk : {e}")
            # Optionally, you could retry or break here
            raise e
        if r.status_code not in [200, 204]:
            raise Exception(r.text)

    @classmethod
    def post_data_frame_in_chunks(
        cls,
        serialized_data_frame: pd.DataFrame,
        chunk_size: int = 50_000,
        data_node_update: DataNodeUpdate = None,
        data_source: str = None,
        index_names: list = None,
        time_index_name: str = "timestamp",
        overwrite: bool = False,
    ):
        """
        Sends a large DataFrame to a Django backend in multiple chunks.
        If a chunk is too large (HTTP 413), it's automatically split in half and retried.
        """
        s = cls.build_session()
        url = cls.get_object_url() + f"/{data_node_update.id}/insert_data_into_table/"

        def _send_chunk_recursively(
            df_chunk: pd.DataFrame, chunk_idx: int, total_chunks: int, is_sub_chunk: bool = False
        ):
            """
            Internal helper to send a chunk. If it receives a 413 error, it splits
            the chunk and calls itself on the two halves.
            """
            if df_chunk.empty:
                return

            part_label = (
                f"{chunk_idx + 1}/{total_chunks}"
                if not is_sub_chunk
                else f"sub-chunk of {chunk_idx + 1}"
            )

            # Prepare the payload
            chunk_stats, _ = get_chunk_stats(
                chunk_df=df_chunk, index_names=index_names, time_index_name=time_index_name
            )
            chunk_json_str = df_chunk.to_json(orient="records", date_format="iso")
            compressed = gzip.compress(chunk_json_str.encode("utf-8"))
            compressed_b64 = base64.b64encode(compressed).decode("utf-8")

            # For sub-chunks, we treat it as a new, single-chunk upload.
            payload = dict(
                json={
                    "data": compressed_b64,
                    "chunk_stats": chunk_stats,
                    "overwrite": overwrite,
                    "chunk_index": 0 if is_sub_chunk else chunk_idx,
                    "total_chunks": 1 if is_sub_chunk else total_chunks,
                }
            )

            try:
                r = make_request(
                    s=s, loaders=None, payload=payload, r_type="POST", url=url, time_out=60 * 15
                )

                if r.status_code in [200, 204]:
                    logger.info(f"Chunk {part_label} ({len(df_chunk)} rows) uploaded successfully.")
                    return

                if r.status_code == 413:
                    logger.warning(
                        f"Chunk {part_label} ({len(df_chunk)} rows) is too large (413). "
                        f"Splitting in half and retrying as new uploads."
                    )
                    if len(df_chunk) <= 1:
                        logger.error(
                            f"A single row is too large to upload (from chunk {part_label}). Cannot split further."
                        )
                        raise Exception(
                            f"A single row from chunk {part_label} is too large to upload."
                        )

                    mid_point = len(df_chunk) // 2
                    first_half = df_chunk.iloc[:mid_point]
                    second_half = df_chunk.iloc[mid_point:]

                    # Recursively call for each half, marking them as sub-chunks.
                    _send_chunk_recursively(first_half, chunk_idx, total_chunks, is_sub_chunk=True)
                    _send_chunk_recursively(second_half, chunk_idx, total_chunks, is_sub_chunk=True)
                    return

                logger.warning(f"Error in request for chunk {part_label}: {r.text}")
                raise Exception(r.text)

            except requests.exceptions.RequestException as e:
                logger.exception(f"Network error uploading chunk {part_label}: {e}")
                raise e

        total_rows = len(serialized_data_frame)
        if total_rows == 0:
            logger.info("DataFrame is empty, nothing to upload.")
            return

        total_chunks = math.ceil(total_rows / chunk_size) if chunk_size > 0 else 1
        logger.info(f"Starting upload of {total_rows} rows in {total_chunks} initial chunk(s).")

        for i in range(total_chunks):
            start_idx = i * chunk_size
            end_idx = min((i + 1) * chunk_size, total_rows)
            chunk_df = serialized_data_frame.iloc[start_idx:end_idx]

            _send_chunk_recursively(chunk_df, i, total_chunks)

    @classmethod
    def get_data_nodes_and_set_updates(
        cls, local_time_series_ids: list, update_details_kwargs, update_priority_dict
    ):
        """
        {'local_hash_id__in': [{'update_hash': 'alpacaequitybarstest_97018e7280c1bad321b3f4153cc7e986', 'data_source_id': 1},
        :param local_hash_id__in:
        :param multi_index_asset_symbols_filter:
        :param update_details_kwargs:
        :param update_priority_dict:
        :return:
        """
        base_url = cls.get_object_url()
        s = cls.build_session()
        payload = {
            "json": dict(
                local_time_series_ids=local_time_series_ids,
                update_details_kwargs=update_details_kwargs,
                update_priority_dict=update_priority_dict,
            )
        }
        # r = self.s.post(f"{base_url}/get_metadatas_and_set_updates/", **payload)
        url = f"{base_url}/get_metadatas_and_set_updates/"
        r = make_request(s=s, loaders=cls.LOADERS, r_type="POST", url=url, payload=payload)
        if r.status_code != 200:
            raise Exception(f"Error in request {r.text}")
        r = r.json()
        r["source_table_config_map"] = {
            int(k): SourceTableConfiguration(**v) if v is not None else v
            for k, v in r["source_table_config_map"].items()
        }
        r["state_data"] = {int(k): DataNodeUpdateDetails(**v) for k, v in r["state_data"].items()}
        r["all_index_stats"] = {int(k): v for k, v in r["all_index_stats"].items()}
        r["data_node_updates"] = [DataNodeUpdate(**v) for v in r["local_metadatas"]]
        return r

    def depends_on_connect(self, target_time_serie_id):

        url = self.get_object_url() + f"/{self.id}/depends_on_connect/"
        s = self.build_session()
        payload = dict(
            json={
                "target_time_serie_id": target_time_serie_id,
            }
        )
        r = make_request(s=s, loaders=self.LOADERS, r_type="PATCH", url=url, payload=payload)
        if r.status_code != 204:
            raise Exception(f"Error in request {r.text}")

    def depends_on_connect_to_api_table(self, target_table_id, timeout=None):

        url = self.get_object_url() + f"/{self.id}/depends_on_connect_to_api_table/"
        s = self.build_session()
        payload = dict(
            json={
                "target_table_id": target_table_id,
            }
        )
        r = make_request(
            s=s, loaders=self.LOADERS, r_type="PATCH", url=url, time_out=timeout, payload=payload
        )
        if r.status_code != 204:
            raise Exception(f"Error in request {r.text}")

    @classmethod
    def _break_pandas_dataframe(cls, data_frame: pd.DataFrame, time_index_name: str | None = None):
        if time_index_name is  None:
            time_index_name = data_frame.index.names[0]
            if time_index_name is None:
                time_index_name = "time_index"
                names = [
                    c if i != 0 else time_index_name for i, c in enumerate(data_frame.index.names)
                ]
                data_frame.index.names = names

        time_col_loc = data_frame.index.names.index(time_index_name)
        index_names = data_frame.index.names
        data_frame = data_frame.reset_index()
        data_frame.columns = [str(c) for c in data_frame.columns]
        data_frame = data_frame.rename(columns={data_frame.columns[time_col_loc]: time_index_name})
        column_dtypes_map = {key: str(value) for key, value in data_frame.dtypes.to_dict().items()}

        data_frame = data_frame.replace({np.nan: None})

        return data_frame, index_names, column_dtypes_map, time_index_name

    def upsert_data_into_table(
        self,
        data: pd.DataFrame,
        data_source: DynamicTableDataSource,overwrite:bool
    ):

        overwrite = True  # ALWAYS OVERWRITE
        metadata = self.data_node_storage

        data, index_names, column_dtypes_map, time_index_name = self._break_pandas_dataframe(data)

        # overwrite data origina data frame to release memory
        if not data[time_index_name].is_monotonic_increasing:
            data = data.sort_values(time_index_name)

        metadata.handle_source_table_configuration_creation(
            column_dtypes_map=column_dtypes_map,
            index_names=index_names,
            time_index_name=time_index_name,
            data=data,
            overwrite=overwrite,
        )

        duplicates_exist = data.duplicated(subset=index_names).any()
        if duplicates_exist:
            raise Exception(f"Duplicates found in columns: {index_names}")

        global_stats, grouped_dates = get_chunk_stats(
            chunk_df=data, index_names=index_names, time_index_name=time_index_name
        )
        multi_index_column_stats = {}
        column_names = [c for c in data.columns if c not in index_names]
        for c in column_names:
            multi_index_column_stats[c] = global_stats["_PER_ASSET_"]
        data_source.related_resource.insert_data_into_table(
            serialized_data_frame=data,
            data_node_update=self,
            overwrite=overwrite,
            time_index_name=time_index_name,
            index_names=index_names,
            grouped_dates=grouped_dates,
        )

        _, last_time_index_value = (
            global_stats["_GLOBAL_"]["min"],
            global_stats["_GLOBAL_"]["max"],
        )
        max_per_asset_symbol = None

        def extract_max(node):
            # Leaf case: a dict with 'min' and 'max'
            if isinstance(node, dict) and "min" in node and "max" in node:
                return node["max"]
            # Otherwise recurse
            return {k: extract_max(v) for k, v in node.items()}

        if len(index_names) > 1:
            max_per_asset_symbol = {
                uid: extract_max(stats) for uid, stats in global_stats["_PER_ASSET_"].items()
            }
        data_node_update = self.set_last_update_index_time_from_update_stats(
            max_per_asset_symbol=max_per_asset_symbol,
            last_time_index_value=last_time_index_value,
            multi_index_column_stats=multi_index_column_stats,
        )
        return data_node_update

    def get_node_time_to_wait(self):

        next_update = self.update_details.next_update
        time_to_wait = 0.0
        if next_update is not None:
            time_to_wait = (
                pd.to_datetime(next_update) - datetime.datetime.now(pytz.utc)
            ).total_seconds()
            time_to_wait = max(0, time_to_wait)
        return time_to_wait, next_update

    def wait_for_update_time(        self,    ):

        if self.update_details.error_on_last_update == True or self.update_details.last_update is None:
            return None

        time_to_wait, next_update = self.get_node_time_to_wait()
        if time_to_wait > 0:

            logger.info(f"Scheduler Waiting for ts update time at {next_update} {time_to_wait}")
            time.sleep(time_to_wait)
        else:
            time_to_wait = max(0, 60 - datetime.datetime.now(pytz.utc).second)
            logger.info("Scheduler Waiting for ts update at start of minute")
            time.sleep(time_to_wait)


class DataNodeUpdateDetails(BasePydanticModel, BaseObjectOrm):
    related_table: int | DataNodeUpdate
    active_update: bool = Field(default=False, description="Flag to indicate if update is active")
    update_pid: int = Field(default=0, description="Process ID of the update")
    error_on_last_update: bool = Field(
        default=False, description="Flag to indicate if there was an error in the last update"
    )
    last_update: datetime.datetime | None = Field(None, description="Timestamp of the last update")
    next_update: datetime.datetime | None = Field(None, description="Timestamp of the next update")
    update_statistics: dict[str, Any] | None = Field(
        None, description="JSON field for update statistics"
    )
    active_update_status: str = Field(
        default="Q", max_length=20, description="Current update status"
    )
    active_update_scheduler: int | Scheduler | None = Field(
        None, description="Scheduler  for active update"
    )
    update_priority: int = Field(default=0, description="Priority level of the update")
    last_updated_by_user: int | None = Field(None, description="Foreign key reference to User")

    run_configuration: RunConfiguration | None = None

    @staticmethod
    def _parse_parameters_filter(parameters):
        for key, value in parameters.items():
            if "__in" in key:
                assert isinstance(value, list)
                parameters[key] = ",".join(value)
        return parameters


class TableMetaData(BaseModel):
    identifier: str = None
    description: str | None = None
    data_frequency_id: DataFrequency | None = None


class DataNodeStorage(BasePydanticModel, BaseObjectOrm):
    id: int = Field(None, description="Primary key, auto-incremented ID")
    storage_hash: str = Field(..., max_length=63, description="Max length of PostgreSQL table name")
    table_name: str | None = Field(
        None, max_length=63, description="Max length of PostgreSQL table name"
    )
    creation_date: datetime.datetime = Field(..., description="Creation timestamp")
    created_by_user: int | None = Field(None, description="Foreign key reference to User")
    organization_owner: int = Field(None, description="Foreign key reference to Organization")
    open_for_everyone: bool = Field(
        default=False, description="Whether the table is open for everyone"
    )
    data_source_open_for_everyone: bool = Field(
        default=False, description="Whether the data source is open for everyone"
    )
    build_configuration: dict[str, Any] | None = Field(
        None, description="Configuration in JSON format"
    )
    build_meta_data: dict[str, Any] | None = Field(None, description="Optional YAML metadata")
    time_serie_source_code_git_hash: str | None = Field(
        None, max_length=255, description="Git hash of the time series source code"
    )
    time_serie_source_code: str | None = Field(
        None, description="File path for time series source code"
    )
    protect_from_deletion: bool = Field(
        default=False, description="Flag to protect the record from deletion"
    )
    data_source: int | DynamicTableDataSource
    source_class_name: str
    sourcetableconfiguration: SourceTableConfiguration | None = None
    table_index_names: dict | None = None

    # TS specifi
    compression_policy_config: dict | None = None
    retention_policy_config: dict | None = None

    # MetaData
    identifier: str | None = None
    description: str | None = None
    data_frequency_id: DataFrequency | None = None

    _drop_indices: bool = False  # for direct incertion we can pass this values
    _rebuild_indices: bool = False  # for direct incertion we can pass this values

    def patch(
        self,
        time_out: None | int = None,
        *args,
        **kwargs,
    ):
        url = self.get_object_url() + f"/{self.id}/"
        payload = {"json": serialize_to_json(kwargs)}
        s = self.build_session()
        r = make_request(
            s=s, loaders=self.LOADERS, r_type="PATCH", url=url, payload=payload, time_out=time_out
        )
        if r.status_code != 200:
            data = r.json()  # guaranteed JSON from your backend
            if r.status_code == 409:
                raise exceptions.ConflictError(data["error"])
            raise exceptions.ApiError(data["error"])
        return self.__class__(**r.json())

    @classmethod
    def patch_by_hash(cls, storage_hash: str, *args, **kwargs):
        metadata = cls.get(storage_hash=storage_hash)
        metadata.patch(*args, **kwargs)

    @classmethod
    def get_or_create(cls, **kwargs):
        kwargs = serialize_to_json(kwargs)
        url = cls.get_object_url() + "/get_or_create/"
        payload = {"json": kwargs}
        s = cls.build_session()
        r = make_request(s=s, loaders=cls.LOADERS, r_type="POST", url=url, payload=payload)
        if r.status_code not in [201, 200]:
            raise Exception(r.text)
        data = r.json()
        return cls(**data)

    def build_or_update_update_details(self, *args, **kwargs):
        base_url = self.get_object_url()
        payload = {"json": kwargs}
        s = self.build_session()
        url = f"{base_url}/{self.id}/build_or_update_update_details/"
        r = make_request(
            r_type="PATCH",
            url=url,
            payload=payload,
            s=s,
            loaders=self.LOADERS,
        )
        if r.status_code != 202:
            raise Exception(f"Error in request {r.text}")

    @classmethod
    def patch_build_configuration(
        cls,
        remote_table_patch: dict | None,
        build_meta_data: dict,
        data_source_id: int,
        local_table_patch: dict,
    ):

        logger.warning("TODO Fix Patch Build Configuration")
        # url = cls.get_object_url() + "/patch_build_configuration"
        # payload = {"json": {"remote_table_patch": remote_table_patch, "local_table_patch": local_table_patch,
        #                     "build_meta_data": build_meta_data, "data_source_id": data_source_id,
        #                     }}
        # s = cls.build_session()
        # r = make_request(s=s, loaders=cls.LOADERS, r_type="POST", url=url, payload=payload,
        #
        #                  )
        # if r.status_code != 200:
        #     raise Exception(r.text)

    def delete_table(self):
        data_source = PodDataSource._get_duck_db()
        duckdb_dynamic_data_source = DynamicTableDataSource.get_or_create_duck_db(
            related_resource=data_source.id,
        )
        if (
            isinstance(self.data_source, int)
            and self.data_source.id == duckdb_dynamic_data_source.id
        ) or (
            not isinstance(self.data_source, int)
            and self.data_source.related_resource.class_type == DUCK_DB
        ):
            db_interface = DuckDBInterface()
            db_interface.drop_table(self.table_name)

        self.delete()

    def handle_source_table_configuration_creation(
        self,
        column_dtypes_map: dict,
        index_names: list[str],
        time_index_name,
        data,
        overwrite=False,
    ):
        """
        Handles the creation or retrieval of the source table configuration.

        Parameters:
        ----------
        metadata : dict
            Metadata dictionary containing "sourcetableconfiguration" and "id".
        column_dtypes_map : dict
            Mapping of column names to their data types.
        index_names : list
            List of index names.
        time_index_name : str
            Name of the time index column.

        data : DataFrame
            The input DataFrame.
        overwrite : bool, optional
            Whether to overwrite existing configurations (default is False).

        Returns:
        -------
        dict or None
            Updated metadata with the source table configuration, and potentially filtered data.
        """
        stc = self.sourcetableconfiguration

        if stc is None:
            try:
                stc = SourceTableConfiguration.create(
                    column_dtypes_map=column_dtypes_map,
                    index_names=index_names,
                    time_index_name=time_index_name,
                    metadata_id=self.id,
                )
                self.sourcetableconfiguration = stc
            except AlreadyExist as err:
                if not overwrite:
                    # Feature not implemented yet → make the causal link explicit
                    raise NotImplementedError(
                        "Removing values per asset when overwrite=False is not implemented yet."
                    ) from err
                    # Filter the data based on time_index_name and last_time_index_value

    def get_last_observation(self,unique_identifier_list:List[str],timeout=None):
        base_url = self.get_object_url()
        payload = {"json": {"unique_identifier_list":unique_identifier_list}}
        s = self.build_session()
        url = f"{base_url}/{self.id}/get_last_observation/"
        r = make_request(
            r_type="POST",
            url=url,
            payload=payload,
            s=s,
            loaders=self.LOADERS,
        )
        if r.status_code != 200:
            raise Exception(f"Error in request {r.text}")
        df=pd.DataFrame(r.json())
        stc = self.sourcetableconfiguration
        try:
            df[stc.time_index_name] = pd.to_datetime(df[stc.time_index_name], format="ISO8601")
        except Exception as e:
            raise e
        columns_to_loop =stc.column_dtypes_map.keys()
        for c, c_type in stc.column_dtypes_map.items():
            if c not in columns_to_loop:
                continue
            if c != stc.time_index_name:
                if c_type == "object":
                    c_type = "str"
                df[c] = df[c].astype(c_type)
        df = df.set_index(stc.index_names)
        return df



    def get_data_between_dates_from_api(
        self,
        start_date: datetime.datetime = None,
        end_date: datetime.datetime = None,
        great_or_equal: bool = None,
        less_or_equal: bool = None,
        unique_identifier_list: list = None,
        columns: list = None,
        unique_identifier_range_map: None | UniqueIdentifierRangeMap = None,
        column_range_descriptor: None | UniqueIdentifierRangeMap = None,
    ):
        """Helper function to make a single batch request (or multiple paged requests if next_offset)."""

        def fetch_one_batch(chunk_range_map):
            all_results_chunk = []
            offset = 0
            while True:
                payload = {
                    "json": {
                        "start_date": start_date.timestamp() if start_date else None,
                        "end_date": end_date.timestamp() if end_date else None,
                        "great_or_equal": great_or_equal,
                        "less_or_equal": less_or_equal,
                        "unique_identifier_list": unique_identifier_list,
                        "columns": columns,
                        "offset": offset,  # pagination offset
                        "unique_identifier_range_map": chunk_range_map,
                    }
                }

                # Perform the POST request
                r = make_request(s=s, loaders=self.LOADERS, payload=payload, r_type="POST", url=url)
                if r.status_code != 200:
                    logger.warning(f"Error in request: {r.text}")
                    return []

                response_data = r.json()
                # Accumulate results
                chunk = response_data.get("results", [])
                all_results_chunk.extend(chunk)

                # Retrieve next offset; if None, we've got all the data in this chunk
                next_offset = response_data.get("next_offset")
                if not next_offset:
                    break

                # Update offset for the next iteration
                offset = next_offset

            return all_results_chunk

        s = self.build_session()
        url = self.get_object_url() + f"/{self.id}/get_data_between_dates_from_remote/"

        unique_identifier_range_map = copy.deepcopy(unique_identifier_range_map)
        if unique_identifier_range_map is not None:
            for _, date_info in unique_identifier_range_map.items():
                # Convert start_date if present
                if "start_date" in date_info and isinstance(
                    date_info["start_date"], datetime.datetime
                ):
                    date_info["start_date"] = int(date_info["start_date"].timestamp())

                # Convert end_date if present
                if "end_date" in date_info and isinstance(date_info["end_date"], datetime.datetime):
                    date_info["end_date"] = int(date_info["end_date"].timestamp())

        all_results = []
        if unique_identifier_range_map:
            keys = list(unique_identifier_range_map.keys())
            chunk_size = 100
            for start_idx in range(0, len(keys), chunk_size):
                key_chunk = keys[start_idx : start_idx + chunk_size]

                # Build sub-dictionary for this chunk
                chunk_map = {k: unique_identifier_range_map[k] for k in key_chunk}

                # Fetch data (including any pagination via next_offset)
                chunk_results = fetch_one_batch(chunk_map)
                all_results.extend(chunk_results)
        else:
            # If unique_identifier_range_map is None, do a single batch with offset-based pagination.
            chunk_results = fetch_one_batch(None)
            all_results.extend(chunk_results)

        return pd.DataFrame(all_results)


class Scheduler(BasePydanticModel, BaseObjectOrm):
    id: int | None = None
    name: str
    is_running: bool
    running_process_pid: int | None
    running_in_debug_mode: bool
    updates_halted: bool
    host: str | None
    api_address: str | None
    api_port: int | None
    last_heart_beat: datetime.datetime | None = None
    pre_loads_in_tree: list[DataNodeUpdate] | None = None  # Assuming this is a list of strings
    in_active_tree: list[DataNodeUpdate] | None = None  # Assuming this is a list of strings
    schedules_to: list[DataNodeUpdate] | None = None
    # for heartbeat
    _stop_heart_beat: bool = False
    _executor: object | None = None

    @classmethod
    def get_scheduler_for_ts(cls, ts_id: int):
        """
        GET /schedulers/for-ts/?ts_id=<DataNodeUpdate PK>
        """
        s = cls.build_session()
        url = cls.get_object_url() + "/for-ts/"
        r = make_request(
            s=s,
            r_type="GET",
            url=url,
            payload={"params": {"ts_id": ts_id}},
            loaders=cls.LOADERS,
        )
        if r.status_code == 404:
            raise SchedulerDoesNotExist(r.json().get("detail", r.text))
        r.raise_for_status()
        return cls(**r.json())

    @classmethod
    def initialize_debug_for_ts(
        cls,
        time_serie_id: int,
        name_suffix: str | None = None,
    ):
        """
        POST /schedulers/initialize‑debug/
        body: { time_serie_id, name_suffix? }
        """
        s = cls.build_session()
        url = cls.get_object_url() + "/initialize-debug/"
        payload = {
            "json": {
                "time_serie_id": time_serie_id,
                **({"name_suffix": name_suffix} if name_suffix is not None else {}),
            }
        }
        r = make_request(s=s, r_type="POST", url=url, payload=payload, loaders=cls.LOADERS)
        r.raise_for_status()
        return cls(**r.json())

    @classmethod
    def build_and_assign_to_ts(
        cls,
        scheduler_name: str,
        time_serie_ids: list[int],
        delink_all_ts: bool = False,
        remove_from_other_schedulers: bool = True,
        timeout=None,
        **kwargs,
    ):
        """
        POST /schedulers/build-and-assign/
        body: {
          scheduler_name, time_serie_ids, delink_all_ts?,
          remove_from_other_schedulers?, scheduler_kwargs?
        }
        """
        s = cls.build_session()
        url = cls.get_object_url() + "/build_and_assign_to_ts/"
        payload = {
            "json": {
                "scheduler_name": scheduler_name,
                "time_serie_ids": time_serie_ids,
                "delink_all_ts": delink_all_ts,
                "remove_from_other_schedulers": remove_from_other_schedulers,
                "scheduler_kwargs": kwargs or {},
            }
        }
        r = make_request(
            s=s, r_type="POST", url=url, payload=payload, time_out=timeout, loaders=cls.LOADERS
        )
        if r.status_code not in [200, 201]:
            r.raise_for_status()
        return cls(**r.json())

    def in_active_tree_connect(self, local_time_series_ids: list[int]):
        """
        PATCH /schedulers/{id}/in-active-tree/
        body: { time_serie_ids }
        """
        s = self.build_session()
        url = f"{self.get_object_url()}/{self.id}/in-active-tree/"
        r = make_request(
            s=s,
            r_type="PATCH",
            url=url,
            payload={"json": {"time_serie_ids": local_time_series_ids}},
            loaders=self.LOADERS,
        )
        if r.status_code not in (200, 204):
            raise Exception(f"Error in request {r.text}")

    def assign_to_scheduler(self, time_serie_ids: list[int]):
        """
        PATCH /schedulers/{id}/assign/
        body: { time_serie_ids }
        """
        s = self.build_session()
        url = f"{self.get_object_url()}/{self.id}/assign/"
        r = make_request(
            s=s,
            r_type="PATCH",
            url=url,
            payload={"json": {"time_serie_ids": time_serie_ids}},
            loaders=self.LOADERS,
        )
        r.raise_for_status()
        return Scheduler(**r.json())

    def is_scheduler_running_in_process(self):
        # test call
        if self.is_running and hasattr(self, "api_address"):
            # verify  scheduler host is the same
            if (
                self.api_address == get_network_ip()
                and is_process_running(self.running_process_pid)
            ):
                return True
        return False

    def _heart_beat_patch(self):
        try:
            scheduler = self.patch(
                is_running=True,
                running_process_pid=os.getpid(),
                running_in_debug_mode=self.running_in_debug_mode,
                last_heart_beat=datetime.datetime.utcnow().replace(tzinfo=pytz.utc).timestamp(),
            )
            for field, value in scheduler.__dict__.items():
                setattr(self, field, value)
        except Exception as e:
            logger.error(e)

    def _heartbeat_runner(self, run_interval):
        """
        Runs forever (until the main thread ends),
        calling _scheduler_heart_beat_patch every 30 seconds.
        """
        logger.debug("Heartbeat thread started with interval = %d seconds", run_interval)

        while True:
            self._heart_beat_patch()
            # Sleep in a loop so that if we ever decide to
            # add a cancellation event, we can check it in smaller intervals
            for _ in range(run_interval):
                # could check for a stop event here if not daemon
                if self._stop_heart_beat:
                    return
                time.sleep(1)

    def start_heart_beat(self):
        from concurrent.futures import ThreadPoolExecutor

        if self._executor is None:
            self._executor = ThreadPoolExecutor(max_workers=1)
        run_interval = TDAG_CONSTANTS.SCHEDULER_HEART_BEAT_FREQUENCY_SECONDS
        self._heartbeat_future = self._executor.submit(self._heartbeat_runner, run_interval)

    def stop_heart_beat(self):
        """
        Stop the heartbeat gracefully.
        """
        # Signal the runner loop to exit
        self._stop_heart_beat = True

        # Optionally wait for the future to complete
        if hasattr(self, "heartbeat_future") and self._heartbeat_future:
            logger.info("Waiting for the heartbeat thread to finish...")
            self._heartbeat_future.result()  # or .cancel() if you prefer

        # Shut down the executor if no longer needed
        if self._executor:
            self._executor.shutdown(wait=True)
            self._executor = None

        logger.info("Heartbeat thread stopped.")


class RunConfiguration(BasePydanticModel, BaseObjectOrm):
    local_time_serie_update_details: int | None = None
    retry_on_error: int = 0
    seconds_wait_on_retry: float = 50
    required_cpus: int = 1
    required_gpus: int = 0
    execution_time_out_seconds: float = 50
    update_schedule: str = "*/1 * * * *"

    @classmethod
    @property
    def ROOT_URL(cls):
        return None


class UpdateStatistics(BaseModel):
    """
    This class contains the  update details of the table in the main sequence engine
    """

    asset_time_statistics: dict[str, datetime.datetime | None | dict] | None = None

    max_time_index_value: datetime.datetime | None = None  # does not include filter applicable for 1d index
    asset_list: list | None = None
    limit_update_time: datetime.datetime | None = None  # flag to limit the update of data node

    _max_time_in_update_statistics: datetime.datetime | None = None  # include filter
    _initial_fallback_date: datetime.datetime | None = None


    # when working with DuckDb and column based storage we want to have also stats by  column
    multi_index_column_stats: dict[str, Any] | None = None
    is_backfill: bool = False

    class Config:
        arbitrary_types_allowed = True

    @staticmethod
    def _to_utc_datetime(value: Any):
        # pandas / numpy friendly path first
        if hasattr(value, "to_pydatetime"):  # pandas.Timestamp
            value = value.to_pydatetime()
        # Handle numpy.datetime64 without importing numpy explicitly
        if type(value).__name__ == "datetime64":
            try:
                import pandas as pd  # only if available

                value = pd.to_datetime(value).to_pydatetime()
            except Exception:
                return value

        if isinstance(value, datetime.datetime):
            return (
                value.astimezone(datetime.UTC)
                if value.tzinfo
                else value.replace(tzinfo=datetime.UTC)
            )

        if isinstance(value, (int| float)):
            v = float(value)
            # seconds / ms / µs / ns heuristics by magnitude
            if v > 1e17:  # ns
                v /= 1e9
            elif v > 1e14:  # µs
                v /= 1e6
            elif v > 1e11:  # ms
                v /= 1e3
            return datetime.datetime.fromtimestamp(v, tz=datetime.UTC)

        if isinstance(value, str):
            s = value.strip()
            if s.endswith("Z"):  # ISO Z suffix
                s = s[:-1] + "+00:00"
            try:
                dt = datetime.datetime.fromisoformat(s)
                return (
                    dt.astimezone(datetime.UTC)
                    if dt.tzinfo
                    else dt.replace(tzinfo=datetime.UTC)
                )
            except ValueError:
                return value

        return value

    @classmethod
    def _normalize_nested(cls, obj: Any):
        if obj is None:
            return None
        if isinstance(obj, dict):
            return {k: cls._normalize_nested(v) for k, v in obj.items()}
        return cls._to_utc_datetime(obj)

    @field_validator("multi_index_column_stats", mode="before")
    @classmethod
    def _coerce_multi_index_column_stats(cls, v):
        # Normalize before standard parsing so ints/strings become datetimes
        return cls._normalize_nested(v)

    @classmethod
    def return_empty(cls):
        return cls()

    def pretty_print(self):
        print(f"{self.__class__.__name__} summary:")

        # asset_list
        if self.asset_list is None:
            print("  asset_list: None")
        else:
            print(f"  asset_list: {len(self.asset_list)} assets")

        # DataFrame
        if self.last_observation is None or self.last_observation.empty:
            print("  last_observation: empty DataFrame")
        else:
            rows, cols = self.last_observation.shape
            print(f"  last_observation: DataFrame with {rows} rows × {cols} columns")

        # Other attributes
        print(f"  max_time_index_value: {self.max_time_index_value}")
        print(f"  _max_time_in_update_statistics: {self._max_time_in_update_statistics}")



    def asset_identifier(self):
        return list(self.asset_time_statistics.keys())

    def get_max_time_in_update_statistics(self):
        if not hasattr(self, "_max_time_in_update_statistics") :
            self._max_time_in_update_statistics = (
                self.max_time_index_value or self._initial_fallback_date
            )
        if self._max_time_in_update_statistics is None and self.asset_time_statistics is not None:
            new_update_statistics, _max_time_in_asset_time_statistics = self._get_update_statistics(
                asset_list=None, unique_identifier_list=None
            )
            self._max_time_in_update_statistics = _max_time_in_asset_time_statistics

        return self._max_time_in_update_statistics

    @property
    def is_any_asset_on_fallback_date(self)->bool:
        """"
        return true if any of the assets in asset_time_statistics equals _initial_fallback_date
        """


        for _,v in self.asset_time_statistics.items():
            if v==self._initial_fallback_date:
                return True
        return False
    @property
    def are_all_assets_on_fallback_date(self)->bool:
        """"
             return true if all assets in asset_time_statistics equals _initial_fallback_date
             """
        for _,v in self.asset_time_statistics.items():
            if v!=self._initial_fallback_date:
                return False
        return True


    def get_update_range_map_great_or_equal_columnar(
        self,
        extra_time_delta: datetime.timedelta | None = None,
        column_filter: list[str] | None = None,
    ):
        fallback = {
            c: {
                a.unique_identifier: {
                    "min": self._initial_fallback_date,
                    "max": self._initial_fallback_date,
                }
                for a in self.asset_list
            }
            for c in column_filter
        }

        multi_index_column_stats = self.multi_index_column_stats or {}
        fallback.update(multi_index_column_stats)

        def _start_dt(bounds):
            dt = (bounds or {}).get("max") or self._initial_fallback_date
            if extra_time_delta:
                dt = dt + extra_time_delta
            return dt


        range_map = {
            col: {
                asset_id: DateInfo(
                    {
                        "start_date_operand": ">=",
                        "start_date": _start_dt(bounds),
                    }
                )
                for asset_id, bounds in col_stats.items()
            }
            for col, col_stats in fallback.items()
            if col in column_filter
        }

        return range_map

    def get_update_range_map_great_or_equal(
        self,
        extra_time_delta: datetime.timedelta | None = None,
    ):

        if extra_time_delta is None:
            range_map = {
                k: DateInfo(
                    {"start_date_operand": ">=", "start_date": v or self._initial_fallback_date}
                )
                for k, v in self.asset_time_statistics.items()
            }
        else:
            range_map = {
                k: DateInfo(
                    {
                        "start_date_operand": ">=",
                        "start_date": (v or self._initial_fallback_date) + extra_time_delta,
                    }
                )
                for k, v in self.asset_time_statistics.items()
            }
        return range_map

    def get_last_update_index_2d(self, uid):
        return self.asset_time_statistics[uid] or self._initial_fallback_date

    def get_asset_earliest_multiindex_update(self, asset):
        stats = self.asset_time_statistics.get(asset.unique_identifier)
        if not stats:
            return self._initial_fallback_date

        def _min_in_nested(node):
            # If this is a dict, recurse into its values
            if isinstance(node, dict):
                m = None
                for v in node.values():
                    cand = _min_in_nested(v)
                    if cand is not None and (m is None or cand < m):
                        m = cand
                return m
            # Leaf: assume it’s a timestamp (datetime or numeric)
            return node

        return _min_in_nested(stats)

    def filter_assets_by_level(
        self,
        level: int,
        filters: list,
    ):
        """
        Prune `self.asset_time_statistics` so that at the specified index level
        only the given keys remain.  Works for any depth of nesting.

        Parameters
        ----------
        level_name : str
            The name of the index-level to filter on (must be one of
            self.metadata.sourcetableconfiguration.index_names).
        filters : List
            The allowed values at that level.  Any branches whose key at
            `level_name` is not in this list will be removed.

        Returns
        -------
        self
            (Allows method chaining.)
        """
        # Grab the full list of index names, in order

        # Determine the numeric depth of the target level
        #   0 == unique_identifier, 1 == first nested level, etc.
        target_depth = level - 1

        # Special‐case: filtering on unique_identifier itself
        if target_depth == 0:
            self.asset_time_statistics = {
                asset: stats
                for asset, stats in self.asset_time_statistics.items()
                if asset in filters
            }
            return self

        allowed = set(filters)
        default = self._initial_fallback_date

        def _prune(node: Any, current_depth: int) -> Any:
            # leaf timestamp
            if not isinstance(node, dict):
                return node

            # we've reached the level to filter
            if current_depth == target_depth:
                out: dict[str, Any] = {}
                for key in allowed:
                    if key in node:
                        out[key] = node[key]
                    else:
                        # missing filter → assign fallback date
                        out[key] = default
                return out

            # otherwise recurse deeper
            pruned: dict[str, Any] = {}
            for key, subnode in node.items():
                new_sub = _prune(subnode, current_depth + 1)
                # keep non-empty dicts or valid leaves
                if isinstance(new_sub, dict):
                    if new_sub:
                        pruned[key] = new_sub
                elif new_sub is not None:
                    pruned[key] = new_sub
            return pruned

        new_stats: dict[str, Any] = {}
        # stats dict sits at depth=1 under each asset
        for asset, stats in self.asset_time_statistics.items():
            if stats is None:
                new_stats[asset] = {f: self._initial_fallback_date for f in allowed}
            else:
                pr = _prune(stats, current_depth=1)
                new_stats[asset] = pr or None

        self.asset_time_statistics = new_stats
        return self

    def _get_update_statistics(
        self, asset_list: list | None, unique_identifier_list: list | None, init_fallback_date=None
    ):
        new_update_statistics = {}
        if asset_list is None and unique_identifier_list is None:
            assert self.asset_time_statistics is not None
            unique_identifier_list = list(self.asset_time_statistics.keys())

        else:
            unique_identifier_list = (
                [a.unique_identifier for a in asset_list]
                if unique_identifier_list is None
                else unique_identifier_list
            )

        for unique_identifier in unique_identifier_list:

            if self.asset_time_statistics and unique_identifier in self.asset_time_statistics:
                new_update_statistics[unique_identifier] = self.asset_time_statistics[
                    unique_identifier
                ]
            else:

                new_update_statistics[unique_identifier] = init_fallback_date

        def _max_in_nested(d):
            """
            Recursively find the max leaf value in a nested dict-of-dicts,
            where the leaves are comparable (e.g. datetime objects).
            Returns None if there are no leaves.
            """
            max_val = None
            for v in d.values():
                if isinstance(v, dict):
                    candidate = _max_in_nested(v)
                else:
                    candidate = v
                if candidate is not None and (max_val is None or candidate > max_val):
                    max_val = candidate
            return max_val

        _max_time_in_asset_time_statistics = (
            _max_in_nested(new_update_statistics)
            if len(new_update_statistics) > 0
            else init_fallback_date
        )

        return new_update_statistics, _max_time_in_asset_time_statistics

    def update_assets(
        self,
        asset_list: list | None,
        *,
        init_fallback_date: datetime = None,
        unique_identifier_list: list | None = None,
    ):
        self.asset_list = asset_list
        new_update_statistics = self.asset_time_statistics

        if asset_list is not None or unique_identifier_list is not None:
            new_update_statistics, _max_time_in_asset_time_statistics = self._get_update_statistics(
                unique_identifier_list=unique_identifier_list,
                asset_list=asset_list,
                init_fallback_date=init_fallback_date,
            )

        else:
            _max_time_in_asset_time_statistics = self.max_time_index_value or init_fallback_date

        new_multi_index_column_stats = self.multi_index_column_stats
        if self.max_time_index_value is not None and self.multi_index_column_stats is not None:
            new_multi_index_column_stats = {
                k: v
                for k, v in self.multi_index_column_stats.items()
                if k in new_update_statistics.keys()
            }



        du = UpdateStatistics(
            asset_time_statistics=new_update_statistics,
            max_time_index_value=self.max_time_index_value,
            asset_list=asset_list,
            multi_index_column_stats=new_multi_index_column_stats,
        )
        du._max_time_in_update_statistics = _max_time_in_asset_time_statistics
        du._initial_fallback_date = init_fallback_date
        return du



    def __getitem__(self, key: str) -> Any:
        if self.asset_time_statistics is None:
            raise KeyError(f"{key} not found (asset_time_statistics is None).")
        return self.asset_time_statistics[key]

    def __setitem__(self, key: str, value: Any) -> None:
        if self.asset_time_statistics is None:
            self.asset_time_statistics = {}
        self.asset_time_statistics[key] = value

    def __delitem__(self, key: str) -> None:
        if not self.asset_time_statistics or key not in self.asset_time_statistics:
            raise KeyError(f"{key} not found in asset_time_statistics.")
        del self.asset_time_statistics[key]

    def __iter__(self):
        """Iterate over keys."""
        if self.asset_time_statistics is None:
            return iter([])
        return iter(self.asset_time_statistics)

    def __len__(self) -> int:
        if not self.asset_time_statistics:
            return 0
        return len(self.asset_time_statistics)

    def keys(self):
        if not self.asset_time_statistics:
            return []
        return self.asset_time_statistics.keys()

    def values(self):
        if not self.asset_time_statistics:
            return []
        return self.asset_time_statistics.values()

    def items(self):
        if not self.asset_time_statistics:
            return []
        return self.asset_time_statistics.items()

    def filter_df_by_latest_value(self, df: pd.DataFrame) -> pd.DataFrame:


        # Single-index time series fallback
        if "unique_identifier" not in df.index.names:
            if self.max_time_index_value is not None:
                df = df[df.index > self.max_time_index_value]
                return df
            else:
                return df



        names = df.index.names
        time_level = names[0]

        grouping_levels = [n for n in names if n != time_level]

        # Build a mask by iterating over each row tuple + its timestamp
        mask = []
        for idx_tuple, ts in zip(df.index, df.index.get_level_values(time_level), strict=False):
            # map level names → values
            level_vals = dict(zip(names, idx_tuple, strict=False))
            asset = level_vals["unique_identifier"]

            # fetch this asset’s nested stats
            stats = self.asset_time_statistics.get(asset)
            if stats is None:
                # no prior stats for this asset → keep row
                mask.append(True)
                continue

            # drill into the nested stats for the remaining levels
            nested = stats
            for lvl in grouping_levels[1:]:  # skip 'unique_identifier'
                key = level_vals[lvl]
                if not isinstance(nested, dict) or key not in nested:
                    # no prior stats for this subgroup → keep row
                    nested = None
                    break
                nested = nested[key]

            # if we couldn’t find a prior timestamp, or this ts is newer, keep it
            if nested is None or ts > nested:
                mask.append(True)
            else:
                # ts ≤ last seen → filter out
                mask.append(False)

        # apply the mask
        df = df[mask]

        # drop any exact duplicate multi‐index rows that remain
        dup = df.index.duplicated(keep="first")
        if dup.any():
            n = dup.sum()
            logger.warning(f"Removed {n} duplicated rows after filtering.")
            df = df[~dup]
        return df


def get_chunk_stats(chunk_df, time_index_name, index_names):
    chunk_stats = {
        "_GLOBAL_": {
            "max": chunk_df[time_index_name].max().timestamp(),
            "min": chunk_df[time_index_name].min().timestamp(),
        }
    }
    chunk_stats["_PER_ASSET_"] = {}
    grouped_dates = None
    if len(index_names) > 1:
        grouped_dates = chunk_df.groupby(index_names[1:])[time_index_name].agg(["min", "max"])

        # 2) decompose the grouped index names
        first, *rest = grouped_dates.index.names

        # 3) reset to a flat DataFrame for easy iteration
        df = grouped_dates.reset_index()

        # 4) build the nested dict
        per_asset: dict = {}
        for _, row in df.iterrows():
            uid = row[first]  # e.g. the unique_identifier
            # only one extra level beyond uid?
            if len(rest) == 0:

                per_asset[uid] = {
                    "min": row["min"].timestamp(),
                    "max": row["max"].timestamp(),
                }
            else:
                # multiple extra levels → walk a path of dicts
                keys = [row[level] for level in rest]
                sub = per_asset.setdefault(uid, {})
                for key in keys[:-1]:
                    sub = sub.setdefault(key, {})
                sub[keys[-1]] = {
                    "min": row["min"].timestamp(),
                    "max": row["max"].timestamp(),
                }
        # 5) assign into your stats structure
        chunk_stats["_PER_ASSET_"] = per_asset
    return chunk_stats, grouped_dates


class LocalTimeSeriesHistoricalUpdate(BasePydanticModel, BaseObjectOrm):
    id: int | None = None
    related_table: int  # Assuming you're using the ID of the related table
    update_time_start: datetime.datetime
    update_time_end: datetime.datetime | None = None
    error_on_update: bool = False
    trace_id: str | None = Field(default=None, max_length=255)
    updated_by_user: int | None = None  # Assuming you're using the ID of the user

    last_time_index_value: datetime.datetime | None = None

    # extra fields for local control
    update_statistics: UpdateStatistics | None
    must_update: bool | None
    direct_dependencies_ids: list[int] | None


class DataSource(BasePydanticModel, BaseObjectOrm):
    id: int | None = Field(None, description="The unique identifier of the Local Disk Source Lake")
    display_name: str
    organization: int | None = Field(
        None, description="The unique identifier of the Local Disk Source Lake"
    )
    class_type: str
    status: str
    extra_arguments: dict | None = None

    @classmethod
    def get_or_create_duck_db(cls, time_out=None, *args, **kwargs):
        url = cls.get_object_url() + "/get_or_create_duck_db/"
        payload = {"json": serialize_to_json(kwargs)}
        s = cls.build_session()
        r = make_request(
            s=s, loaders=cls.LOADERS, r_type="POST", url=url, payload=payload, time_out=time_out
        )
        if r.status_code not in [200, 201]:
            raise Exception(f"Error in request {r.text}")
        return cls(**r.json())

    def insert_data_into_table(
        self,
        serialized_data_frame: pd.DataFrame,
        data_node_update: DataNodeUpdate,
        overwrite: bool,
        time_index_name: str,
        index_names: list,
        grouped_dates: dict,
    ):

        if self.class_type == DUCK_DB:
            DuckDBInterface().upsert(
                df=serialized_data_frame, table=data_node_update.data_node_storage.table_name
            )
        else:
            DataNodeUpdate.post_data_frame_in_chunks(
                serialized_data_frame=serialized_data_frame,
                data_node_update=data_node_update,
                data_source=self,
                index_names=index_names,
                time_index_name=time_index_name,
                overwrite=overwrite,
            )

    def insert_data_into_local_table(
        self,
        serialized_data_frame: pd.DataFrame,
        data_node_update: DataNodeUpdate,
        overwrite: bool,
        time_index_name: str,
        index_names: list,
        grouped_dates: dict,
    ):

        # DataNodeUpdate.post_data_frame_in_chunks(
        #     serialized_data_frame=serialized_data_frame,
        #     data_node_update=data_node_update,
        #     data_source=self,
        #     index_names=index_names,
        #     time_index_name=time_index_name,
        #     overwrite=overwrite,
        # )
        raise NotImplementedError

    def get_data_by_time_index(
        self,
        data_node_update: DataNodeUpdate,
        start_date: datetime.datetime | None = None,
        end_date: datetime.datetime | None = None,
        great_or_equal: bool = True,
        less_or_equal: bool = True,
        columns: list[str] | None = None,
        unique_identifier_list: list[str] | None = None,
        unique_identifier_range_map: UniqueIdentifierRangeMap | None = None,
        column_range_descriptor: dict[str, UniqueIdentifierRangeMap] | None = None,
    ) -> pd.DataFrame:

        if self.class_type == DUCK_DB:
            db_interface = DuckDBInterface()
            table_name = data_node_update.data_node_storage.table_name

            adjusted_start, adjusted_end, adjusted_uirm, _ = db_interface.constrain_read(
                table=table_name,
                start=start_date,
                end=end_date,
                ids=unique_identifier_list,
                unique_identifier_range_map=unique_identifier_range_map,
            )
            if unique_identifier_range_map is not None and adjusted_end is not None:
                adjusted_end = datetime.datetime(
                    adjusted_end.year,
                    adjusted_end.month,
                    adjusted_end.day,
                    tzinfo=datetime.UTC,
                )
                for v in unique_identifier_range_map.values():
                    v["end_date"] = adjusted_end
                    v["end_date_operand"] = "<="

            df = db_interface.read(
                table=table_name,
                start=start_date,
                end=end_date,
                great_or_equal=great_or_equal,
                less_or_equal=less_or_equal,
                ids=unique_identifier_list,
                columns=columns,
                unique_identifier_range_map=unique_identifier_range_map,  # Pass range map
            )

        else:
            if column_range_descriptor is not None:
                raise Exception("On this data source do not use column_range_descriptor")
            df = data_node_update.get_data_between_dates_from_api(
                start_date=start_date,
                end_date=end_date,
                great_or_equal=great_or_equal,
                less_or_equal=less_or_equal,
                unique_identifier_list=unique_identifier_list,
                columns=columns,
                unique_identifier_range_map=unique_identifier_range_map,
            )
        if len(df) == 0:
            logger.warning(f"No data returned from remote API for {data_node_update.update_hash}")
            return df

        stc = data_node_update.data_node_storage.sourcetableconfiguration
        try:
            df[stc.time_index_name] = pd.to_datetime(df[stc.time_index_name], format="ISO8601")
        except Exception as e:
            raise e
        columns_to_loop = columns or stc.column_dtypes_map.keys()
        for c, c_type in stc.column_dtypes_map.items():
            if c not in columns_to_loop:
                continue
            if c != stc.time_index_name:
                if c_type == "object":
                    c_type = "str"
                df[c] = df[c].astype(c_type)
        df = df.set_index(stc.index_names)
        return df

    def get_earliest_value(
        self,
        data_node_update: DataNodeUpdate,
    ) -> tuple[pd.Timestamp | None, dict[Any, pd.Timestamp | None]]:
        if self.class_type == DUCK_DB:
            db_interface = DuckDBInterface()
            table_name = data_node_update.data_node_storage.table_name
            return db_interface.time_index_minima(table=table_name)

        else:
            raise NotImplementedError


class DynamicTableDataSource(BasePydanticModel, BaseObjectOrm):
    id: int
    related_resource: DataSource
    related_resource_class_type: str

    class Config:
        use_enum_values = True  # This ensures that enums are stored as their values (e.g., 'TEXT')

    def model_dump_json(self, **json_dumps_kwargs) -> str:
        """
        Dump the current instance to a JSON string,
        ensuring that the dependent `related_resource` is also properly dumped.
        """
        # Obtain the dictionary representation using Pydantic's model_dump
        dump = self.model_dump()
        # Properly dump the dependent resource if it supports model_dump
        dump["related_resource"] = self.related_resource.model_dump()
        # Convert the dict to a JSON string
        return json.dumps(dump, **json_dumps_kwargs)

    @classmethod
    def get_default_data_source_for_token(cls):
        global _default_data_source
        if _default_data_source is not None:
            return _default_data_source  # Return cached result if already set
        url = cls.ROOT_URL + "/get_default_data_source_for_token/"

        s = cls.build_session()
        r = make_request(s=s, loaders=cls.LOADERS, r_type="GET", url=url, payload={})

        if r.status_code != 200:
            raise Exception(f"Error in request {r.text}")

        return cls(**r.json())

    def persist_to_pickle(self, path):
        import cloudpickle

        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as handle:
            cloudpickle.dump(self, handle)

    @classmethod
    def get_or_create_duck_db(cls, *args, **kwargs):
        url = cls.get_object_url() + "/get_or_create_duck_db/"
        s = cls.build_session()
        r = make_request(s=s, loaders=cls.LOADERS, r_type="POST", url=url, payload={"json": kwargs})
        if r.status_code not in [200, 201]:
            raise Exception(f"Error in request {r.text}")
        return cls(**r.json())

    def has_direct_postgres_connection(self):
        return self.related_resource.class_type == "direct"

    def get_data_by_time_index(self, *args, **kwargs):
        if self.has_direct_postgres_connection():
            stc = kwargs["data_node_update"].data_node_storage.sourcetableconfiguration

            df = TimeScaleInterface.direct_data_from_db(
                *args,
                connection_uri=self.related_resource.get_connection_uri(),

                **kwargs,
            )
            df = set_types_in_table(df, stc.column_dtypes_map)
            return df
        else:
            return self.related_resource.get_data_by_time_index(*args, **kwargs)




class Project(BasePydanticModel, BaseObjectOrm):
    id: int
    project_name: str
    data_source: DynamicTableDataSource
    git_ssh_url: str | None = None
    resources_visible_to_light_users: bool

    @classmethod
    def get_user_default_project(cls):
        url = cls.get_object_url() + "/get_user_default_project/"

        s = cls.build_session()
        r = make_request(
            s=s,
            loaders=cls.LOADERS,
            r_type="GET",
            url=url,
        )
        if r.status_code == 404:
            raise Exception(r.text)
        if r.status_code != 200:
            raise Exception(f"Error in request {r.text}")
        return cls(**r.json())

    def __str__(self):
        return yaml.safe_dump(
            self.model_dump(),
            sort_keys=False,
            default_flow_style=False,
        )


class TimeScaleDB(DataSource):
    database_user: str
    password: str
    host: str
    database_name: str
    port: int

    def get_connection_uri(self):
        password = self.password  # Decrypt password if necessary
        return f"postgresql://{self.database_user}:{password}@{self.host}:{self.port}/{self.database_name}"

    def insert_data_into_table(
        self,
        serialized_data_frame: pd.DataFrame,
        data_node_update: DataNodeUpdate,
        overwrite: bool,
        time_index_name: str,
        index_names: list,
        grouped_dates: dict,
    ):

        DataNodeUpdate.post_data_frame_in_chunks(
            serialized_data_frame=serialized_data_frame,
            data_node_update=data_node_update,
            data_source=self,
            index_names=index_names,
            time_index_name=time_index_name,
            overwrite=overwrite,
        )

    def filter_by_assets_ranges(
        self, asset_ranges_map: dict, metadata: dict, update_hash: str, has_direct_connection: bool
    ):
        table_name = metadata.table_name
        index_names = metadata.sourcetableconfiguration.index_names
        column_types = metadata.sourcetableconfiguration.column_dtypes_map
        if has_direct_connection:
            df = TimeScaleInterface.filter_by_assets_ranges(
                table_name=table_name,
                asset_ranges_map=asset_ranges_map,
                index_names=index_names,
                data_source=self,
                column_types=column_types,
            )
        else:
            df = DataNodeUpdate.get_data_between_dates_from_api(
                update_hash=update_hash,
                data_source_id=self.id,
                start_date=None,
                end_date=None,
                great_or_equal=True,
                less_or_equal=True,
                asset_symbols=None,
                columns=None,
                execution_venue_symbols=None,
                symbol_range_map=asset_ranges_map,  # <-- key for applying ranges
            )
        return df

    def get_data_by_time_index(
        self,
        data_node_update: DataNodeUpdate,
        start_date: datetime.datetime | None = None,
        end_date: datetime.datetime | None = None,
        great_or_equal: bool = True,
        less_or_equal: bool = True,
        columns: list[str] | None = None,
        unique_identifier_list: list[str] | None = None,
    ) -> pd.DataFrame:


        df = data_node_update.get_data_between_dates_from_api(
            start_date=start_date,
            end_date=end_date,
            great_or_equal=great_or_equal,
            less_or_equal=less_or_equal,
            unique_identifier_list=unique_identifier_list,
            columns=columns,
        )
        if len(df) == 0:
            if logger:
                logger.warning(
                    f"No data returned from remote API for {data_node_update.update_hash}"
                )
            return df

        stc = data_node_update.data_node_storage.sourcetableconfiguration
        df[stc.time_index_name] = pd.to_datetime(df[stc.time_index_name])
        for c, c_type in stc.column_dtypes_map.items():
            if c != stc.time_index_name:
                if c_type == "object":
                    c_type = "str"
                df[c] = df[c].astype(c_type)
        df = df.set_index(stc.index_names)
        return df


class DynamicResource(BasePydanticModel, BaseObjectOrm):
    id: int | None = None
    name: str
    type: str
    object_signature: dict
    attributes: dict | None

    created_at: datetime.datetime
    updated_at: datetime.datetime
    is_production: bool
    pod: int

    @classmethod
    def create(self,*args, **kwargs):
        return super().create(*args, **kwargs)

def create_configuration_for_strategy(json_payload: dict, timeout=None):
    url = TDAG_ENDPOINT + "/orm/api/tdag-gpt/create_configuration_for_strategy/"
    from requests.adapters import HTTPAdapter, Retry

    s = requests.Session()
    s.headers.update(loaders.auth_headers)
    retries = Retry(total=2, backoff_factor=2)
    s.mount("http://", HTTPAdapter(max_retries=retries))

    r = make_request(
        s=s, r_type="POST", url=url, payload={"json": json_payload}, loaders=loaders, time_out=200
    )
    return r


def query_agent(json_payload: dict, timeout=None):
    url = TDAG_ENDPOINT + "/orm/api/tdag-gpt/query_agent/"
    from requests.adapters import HTTPAdapter, Retry

    s = requests.Session()
    s.headers.update(loaders.auth_headers)
    retries = Retry(total=2, backoff_factor=2)
    s.mount("http://", HTTPAdapter(max_retries=retries))

    r = make_request(
        s=s, r_type="POST", url=url, payload={"json": json_payload}, loaders=loaders, time_out=200
    )
    return r


def add_created_object_to_jobrun(
    model_name: str, app_label: str, object_id: int, timeout: int | None = None
) -> dict:
    """
    Logs a new object that was created by this JobRun instance.

    Args:
        model_name: The string name of the created model (e.g., "Project").
        app_label: The Django app label where the model is defined (e.g., "pod_manager").
        object_id: The primary key of the created object instance.
        timeout: Optional request timeout in seconds.

    Returns:
        A dictionary representing the created record.
    """
    url = TDAG_ENDPOINT + f"/orm/api/pods/job-run/{os.getenv('JOB_RUN_ID')}/add_created_object/"
    s = requests.Session()
    payload = {"json": {"app_label": app_label, "model_name": model_name, "object_id": object_id}}
    r = make_request(
        s=s, loaders=loaders, r_type="POST", url=url, payload=payload, time_out=timeout
    )
    if r.status_code not in [200, 201]:
        raise Exception(f"Failed to add created object: {r.status_code} - {r.text}")
    return r.json()


class Artifact(BasePydanticModel, BaseObjectOrm):
    id: int | None
    name: str
    created_by_resource_name: str
    bucket_name: str
    content: Any
    creation_date: datetime.datetime

    @classmethod
    def upload_file(cls, filepath, name, created_by_resource_name, bucket_name=None):
        bucket_name=bucket_name if bucket_name else "default_bucket"
        return cls.get_or_create(
            filepath=filepath,
            name=name,
            created_by_resource_name=created_by_resource_name,
            bucket_name=bucket_name,
        )

    @classmethod
    def get_or_create(cls, filepath, name, created_by_resource_name, bucket_name):
        url = cls.get_object_url() + "/get_or_create/"
        s = cls.build_session()
        with open(filepath, "rb") as f:
            data = {
                "name": name,
                "created_by_resource_name": created_by_resource_name,
                "bucket_name": bucket_name if bucket_name else "default_bucket",
            }
            files = {"content": (str(filepath), f, "application/pdf")}
            payload = {"json": data, "files": files}
            r = make_request(s=s, loaders=cls.LOADERS, r_type="POST", url=url, payload=payload)

            if r.status_code not in [200, 201]:
                raise Exception(f"Failed to get artifact: {r.status_code} - {r.text}")

            return cls(**r.json())


try:
    POD_PROJECT = Project.get_user_default_project()
except Exception as e:
    POD_PROJECT = None
    logger.exception(f"Could not retrive pod project {e}")
    raise e


class PodDataSource:
    def set_remote_db(self):
        self.data_source = POD_PROJECT.data_source
        logger.info(f"Set remote data source to {self.data_source.related_resource}")

        if self.data_source.related_resource.status != "AVAILABLE":
            raise Exception(f"Project Database {self.data_source} is not available")

    @staticmethod
    def _get_duck_db():
        host_uid = bios_uuid()
        data_source = DataSource.get_or_create_duck_db(
            display_name=f"DuckDB_{host_uid}", host_mac_address=host_uid
        )
        return data_source

    @property
    def is_local_duck_db(self):
        return SessionDataSource.data_source.related_resource.class_type == DUCK_DB

    def set_local_db(self):
        data_source = self._get_duck_db()

        duckdb_dynamic_data_source = DynamicTableDataSource.get_or_create_duck_db(
            related_resource=data_source.id,
        )

        # drop local tables that are not in registered in the backend anymore (probably have been deleted)
        remote_node_storages = DataNodeStorage.filter(
            data_source__id=duckdb_dynamic_data_source.id, list_tables=True
        )
        remote_table_names = [t.table_name for t in remote_node_storages]
        from mainsequence.client.data_sources_interfaces.duckdb import DuckDBInterface

        db_interface = DuckDBInterface()
        local_table_names = db_interface.list_tables()

        tables_to_delete_locally = set(local_table_names) - set(remote_table_names)
        for table_name in tables_to_delete_locally:
            logger.debug(f"Deleting table in local duck db {table_name}")
            db_interface.drop_table(table_name)

        tables_to_delete_remotely = set(remote_table_names) - set(local_table_names)
        for remote_table in remote_node_storages:
            if remote_table.table_name in tables_to_delete_remotely:
                logger.debug(f"Deleting table remotely {remote_table.table_name}")
                if remote_table.protect_from_deletion:
                    remote_table.patch(protect_from_deletion=False)

                remote_table.delete()

        self.data_source = duckdb_dynamic_data_source

        physical_ds = self.data_source.related_resource
        banner = (
            "─" * 40 + "\n"
            f"LOCAL: {physical_ds.display_name} (engine={physical_ds.class_type})\n\n"
            "import duckdb, pathlib\n"
            f"path = pathlib.Path('{db_interface.db_path}') / 'duck_meta.duckdb'\n"
            "conn = duckdb.connect(':memory:')\n"
            "conn.execute(f\"ATTACH '{path}' AS ro (READ_ONLY)\")\n"
            "conn.execute('INSTALL ui; LOAD ui; CALL start_ui();')\n" + "─" * 40
        )
        logger.info(banner)

    def __repr__(self):
        return f"{self.data_source.related_resource}"


def _norm_value(v: Any) -> Any:
    """Normalize values into hashable, deterministic forms for the cache key."""
    # Project objects → their integer IDs (project scoped vs global)
    if Project and isinstance(v, Project):
        return getattr(v, "id", v)

    # Common iterables → sorted tuples to ignore order in queries like name__in
    if isinstance(v, (set| list| tuple)):
        # Convert nested items too, just in case
        return tuple(sorted(_norm_value(x) for x in v))

    # Dicts → sorted (k,v) tuples
    if isinstance(v, dict):
        return tuple(sorted((k, _norm_value(val)) for k, val in v.items()))

    return v  # primitives pass through


def _norm_kwargs(kwargs: dict[str, Any]) -> tuple[tuple[str, Any], ...]:
    """Stable, hashable key from kwargs (order-insensitive)."""
    items = []
    for k, v in kwargs.items():
        # Special-case a big `name__in` so you don’t produce huge keys.
        if k == "name__in" and isinstance(v, (list | tuple | set)):
            items.append((k, tuple(sorted(str(x) for x in v))))
        else:
            items.append((k, _norm_value(v)))
    return tuple(sorted(items))


class Constant(BasePydanticModel, BaseObjectOrm):
    """
    Simple scoped constant.
    - Global when project is None.
    - Project-scoped when project is set.

    Uniqueness (enforced in DB/service layer):
      * Global:      (organization_owner, name)
      * Per-project: (organization_owner, project, name)
    """

    id: int | None

    name: str = Field(
        ...,
        max_length=255,
        description="UPPER_SNAKE_CASE; optional category via double-underscore, e.g. 'CURVE__US_TREASURIES'.",
    )
    value: Any = Field(
        ...,
        description="Small JSON value (string/number/bool/object/array). Keep it small (e.g., <=10KB).",
    )
    project: Project | int | None = Field(None, description="Project ID; None ⇒ global.")
    category: str | None = None

    # Class-level cache & lock (Pydantic ignores ClassVar)
    _filter_cache: ClassVar[TTLCache] = TTLCache(maxsize=512, ttl=600)
    _get_cache: ClassVar[TTLCache] = TTLCache(maxsize=1024, ttl=600)

    _cache_lock: ClassVar[RLock] = RLock()

    model_config = dict(from_attributes=True)  # allows .model_validate(from_orm_obj)

    @classmethod
    @cachedmethod(
        lambda cls: cls._filter_cache,  # <- resolves to the real TTLCache
        lock=lambda cls: cls._cache_lock,
        key=lambda cls, **kw: _norm_kwargs(kw),
    )
    def filter(cls, **kwargs):
        # Delegate to your real filter (API/DB) only on cache miss
        return super().filter(**kwargs)

    @classmethod
    @cachedmethod(
        lambda cls: cls._get_cache,
        lock=lambda cls: cls._cache_lock,
        key=lambda cls, **kw: _norm_kwargs(kw),
    )
    def get(cls, **kwargs):
        # e.g. get(name="CURVE__M_BONOS", project=None)
        return super().get(**kwargs)

    @classmethod
    def get_value(cls, name: str, project_id: int | None = None):
        return cls.get(name=name, project_id=project_id).value

    @classmethod
    def invalidate_filter_cache(cls) -> None:
        cls._filter_cache.clear()

    @classmethod
    def create_constants_if_not_exist(cls, constants_to_create: dict):
        # crete global constants if not exist in  backed

        # constants_to_create=dict(
        # TIIE_28_UID        = "TIIE_28",
        # TIIE_91_UID        = "TIIE_91",
        # TIIE_182_UID       = "TIIE_182",
        # TIIE_OVERNIGHT_UID = "TIIE_OVERNIGHT",
        #
        # CETE_28_UID        = "CETE_28",
        # CETE_91_UID        = "CETE_91",
        # CETE_182_UID       = "CETE_182",
        #
        # # Curve identifiers
        # TIIE_28_ZERO_CURVE = "F_TIIE_28_VALMER",
        # M_BONOS_ZERO_CURVE = "M_BONOS_ZERO_OTR",
        #
        #
        # DISCOUNT_CURVES_TABLE         = "discount_curves",
        # REFERENCE_RATES_FIXING_TABLE  = "fixing_rates_1d",
        # )
        existing_constants = cls.filter(name__in=list(constants_to_create.keys()))
        existing_constants_names = [c.name for c in existing_constants]
        constants_to_register = {
            k: v for k, v in constants_to_create.items() if k not in existing_constants_names
        }
        created_constants = []
        for k, v in constants_to_register.items():
            new_constant = cls.create(name=k, value=v)
            created_constants.append(new_constant)
        return created_constants


SessionDataSource = PodDataSource()
SessionDataSource.set_remote_db()

DataNodeUpdateDetails.model_rebuild()
DataNodeUpdate.model_rebuild()
RunConfiguration.model_rebuild()
SourceTableConfiguration.model_rebuild()
DataNodeStorage.model_rebuild()
DynamicTableDataSource.model_rebuild()
DataSource.model_rebuild()
