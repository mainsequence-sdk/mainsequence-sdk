from importlib.metadata import metadata

import yaml

from .base import BasePydanticModel, BaseObjectOrm, TDAG_ENDPOINT
from .data_sources_interfaces.duckdb import DuckDBInterface
from .utils import (is_process_running, get_network_ip,
                    TDAG_CONSTANTS,
                    DATE_FORMAT, AuthLoaders, make_request, set_types_in_table, request_to_datetime, serialize_to_json, bios_uuid)
import copy
import datetime
import pytz
import requests
import pandas as pd
import json
from typing import Union
import time
import os
from mainsequence.logconf import logger

from pydantic import BaseModel, Field, field_validator
from typing import Optional, List, Dict, Any, TypedDict
from .data_sources_interfaces import timescale as TimeScaleInterface
from functools import wraps
import math
import gzip
import base64
import numpy as np
import concurrent.futures

_default_data_source = None  # Module-level cache


JSON_COMPRESSED_PREFIX = ["json_compressed", "jcomp_"]

loaders = AuthLoaders()


# Global executor (or you could define one on your class)
_executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)


class AlreadyExist(Exception):
    pass

class DateInfo(TypedDict, total=False):
    start_date: Optional[datetime.datetime]
    start_date_operand: Optional[str]
    end_date: Optional[datetime.datetime]
    end_date_operand: Optional[str]

UniqueIdentifierRangeMap = Dict[str, DateInfo]

def build_session(loaders):
    from requests.adapters import HTTPAdapter, Retry
    s = requests.Session()
    s.headers.update(loaders.auth_headers)
    retries = Retry(total=2, backoff_factor=2, )
    s.mount('http://', HTTPAdapter(max_retries=retries))
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

class TimeSerieNode(BasePydanticModel, BaseObjectOrm):
    uid: str
    hash_id: str
    data_source_id: int
    source_class_name: str
    creation_date: datetime.datetime
    relation_tree_frozen: bool

    @classmethod
    def get_all_dependencies(cls, hash_id):
        s = cls.build_session()
        url = cls.get_object_url() + f"/{hash_id}/get_all_dependencies/"
        r = make_request(s=s, loaders=cls.LOADERS, r_type="GET", url=url, )
        if r.status_code != 200:
            raise Exception(f"Error in request {r.text}")

        depth_df = pd.DataFrame(r.json())
        return depth_df

    @classmethod
    def delete_with_relationships(cls, *args, **kwargs):
        s = cls.build_session()
        url = cls.get_object_url() + f"/delete_with_relationships/"
        r = make_request(s=s, loaders=cls.LOADERS, r_type="POST", url=url, payload={"json": kwargs})
        if r.status_code != 200:
            raise Exception(f"Error in request {r.text}")
        return r.json()

    @classmethod
    def get_max_depth(cls, hash_id, timeout=None):
        s = cls.build_session()
        url = cls.get_object_url() + f"/{hash_id}/get_max_depth/"
        r = make_request(s=s, loaders=cls.LOADERS, r_type="GET", url=url, time_out=timeout)
        if r.status_code != 200:
            raise Exception(f"Error in request {r.text}")

        return r.json()["max_depth"]

    @classmethod
    def get_upstream_nodes(cls, hash_id):
        s = cls.build_session()
        url = cls.get_object_url() + f"/{hash_id}/get_upstream_nodes/"
        r = make_request(s=s, loaders=cls.LOADERS, r_type="GET", url=url, )
        if r.status_code != 200:
            raise Exception(f"Error in request {r.text}")

        depth_df = pd.DataFrame(r.json())
        return depth_df

    @classmethod
    def set_policy_for_descendants(cls, hash_id, policy, pol_type, exclude_ids, extend_to_classes):
        s = cls.build_session()
        url = cls.get_object_url() + f"/{hash_id}/set_policy_for_descendants/"
        payload = dict(json={"policy": policy,
                             "pol_type": pol_type,
                             "exclude_ids": exclude_ids,
                             "extend_to_classes": extend_to_classes,
                             })
        r = make_request(s=s, loaders=cls.LOADERS, r_type="PATCH", url=url, payload=payload)
        if r.status_code != 200:
            raise Exception(f"Error in request {r.text}")

    @classmethod
    def remove_head_from_all_schedulers(cls, hash_id):
        url = cls.get_object_url() + f"/{hash_id}/remove_head_from_all_schedulers/"
        s = cls.build_session()
        r = make_request(s=s, loaders=cls.LOADERS, r_type="PATCH", url=url, )
        if r.status_code != 200:
            raise Exception(r.text)

class LocalTimeSerieNode(BasePydanticModel, BaseObjectOrm):
    hash_id: str
    uid: str
    data_source_id: int
    updates_to: TimeSerieNode


class ColumnMetaData(BasePydanticModel,BaseObjectOrm):
    source_config_id: int = Field(
        ...,
        alias="source_config",
        description="Primary key of the related SourceTableConfiguration"
    )
    column_name: str = Field(
        ...,
        max_length=63,
        description="Name of the column (must match column_dtypes_map key)"
    )
    dtype: str = Field(
        ...,
        max_length=100,
        description="Data type (will be synced from the configuration’s dtype map)"
    )
    label: str = Field(
        ...,
        max_length=250,
        description="Human‐readable label"
    )
    description: str = Field(
        ...,
        description="Longer description of the column"
    )

class SourceTableConfiguration(BasePydanticModel, BaseObjectOrm):
    id: Optional[int] = Field(None, description="Primary key, auto-incremented ID")
    related_table: Union[int, "DynamicTableMetaData"]
    time_index_name: str = Field(..., max_length=100, description="Time index name")
    column_dtypes_map: Dict[str, Any] = Field(..., description="Column data types map")
    index_names: List
    column_index_names: List
    last_time_index_value: Optional[datetime.datetime] = Field(None, description="Last time index value")
    earliest_index_value: Optional[datetime.datetime] = Field(None, description="Earliest index value")
    multi_index_stats: Optional[Dict[str, Any]] = Field(None, description="Multi-index statistics JSON field")
    table_partition: Dict[str, Any] = Field(..., description="Table partition settings")
    last_observation: Optional[Dict]
    open_for_everyone: bool = Field(default=False, description="Whether the table configuration is open for everyone")
    columns_metadata:Optional[List[ColumnMetaData]]=None

    def get_data_updates(self):
        max_per_asset = None
        if self.multi_index_stats is not None:
            max_per_asset = self.multi_index_stats["max_per_asset_symbol"]
            max_per_asset = {k: request_to_datetime(v) for k, v in max_per_asset.items()}

        du = DataUpdates(
            max_time_index_value=self.last_time_index_value,
            update_statistics=max_per_asset
        )

        du._max_time_in_update_statistics = self.last_time_index_value
        return du

    def get_time_scale_extra_table_indices(self) -> dict:
        url = self.get_object_url() + f"/{self.related_table}/get_time_scale_extra_table_indices/"
        s = self.build_session()
        r = make_request(s=s, loaders=self.LOADERS, r_type="GET", url=url, )
        if r.status_code != 200:
            raise Exception(r.text)
        return r.json()

    def set_or_update_columns_metadata(self,columns_metadata:List[ColumnMetaData],
                                       timeout=None) -> None:
        """
        """

        columns_metadata = [
            c.model_dump(exclude={'orm_class'})
            for c in columns_metadata
        ]
        url = self.get_object_url() + f"/{self.related_table}/set_or_update_columns_metadata/"
        s = self.build_session()
        r = make_request(s=s, loaders=self.LOADERS, r_type="POST",
                         time_out=timeout,
                         url=url,payload={"json": {"columns_metadata":columns_metadata}} )
        if r.status_code not in [ 200,201]:
            raise Exception(r.text)
        return r.json()

    def patch(self, *args, **kwargs):
        # related table is the primary key of this model
        if isinstance(self.related_table, int):
            id = self.related_table
        else:
            id = self.related_table.id
        return self.__class__.patch_by_id(id, *args, **kwargs)

class ColumnMetaData(BasePydanticModel):
    source_config_id: Optional[int] = Field(None, description="FK to SourceTableConfiguration")
    column_name:str=  Field(...,max_length=63, description="Name of the column")
    dtype:   str=Field(..., max_length=100,description="Data type of the column")
    label:  str=Field(..., max_length=255,description="Human-readable label")
    description: str = Field(..., description="Detailed description")


class LocalTimeSerie(BasePydanticModel, BaseObjectOrm):
    id: Optional[int] = Field(None, description="Primary key, auto-incremented ID")
    local_hash_id: str = Field(..., max_length=63, description="Max length of PostgreSQL table name")
    remote_table: Union[int, "DynamicTableMetaData"]
    build_configuration: Dict[str, Any] = Field(..., description="Configuration in JSON format")
    build_meta_data: Optional[Dict[str, Any]] = Field(None, description="Optional YAML metadata")
    ogm_dependencies_linked: bool = Field(default=False, description="OGM dependencies linked flag")
    tags: Optional[list[str]] = Field(default=[], description="List of tags")
    description: Optional[str] = Field(None, description="Optional HTML description")
    localtimeserieupdatedetails: Optional[Union["LocalTimeSerieUpdateDetails",int]] = None
    run_configuration: Optional["RunConfiguration"]=None
    open_for_everyone: bool = Field(default=False, description="Whether the ts is open for everyone")

    @property
    def data_source_id(self):
        if isinstance(self.remote_table.data_source, int):
            return self.remote_table.data_source
        else:
            return self.remote_table.data_source.id

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
        r = make_request(s=s, loaders=self.LOADERS, r_type="PATCH", url=url,
                         payload=payload,
                         time_out=timeout)
        if r.status_code != 200:
            raise Exception(f"Error in request {r.json()}")
        return r.json()

    def update_details_exist(self, timeout=None):
        base_url = self.get_object_url()
        s = self.build_session()

        url = f"{base_url}/{self.id}/update_details_exist/"
        r = make_request(s=s, loaders=self.LOADERS, r_type="GET", url=url, time_out=timeout)
        if r.status_code != 200:
            raise Exception(f"Error in request {r.json()}")
        return r.json()

    @classmethod
    def filter_by_hash_id(cls, local_hash_id_list: list, timeout=None):
        s = cls.build_session()
        base_url = cls.get_object_url()
        url = f"{base_url}/filter_by_hash_id/"
        payload = {"json": {"local_hash_id__in": local_hash_id_list}, }
        r = make_request(s=s, loaders=cls.LOADERS, r_type="POST", url=url, payload=payload, time_out=timeout)
        if r.status_code != 200:
            raise Exception(f"{r.text}")
        all_metadatas = {m["local_hash_id"]: m for m in r.json()}
        return all_metadatas

    def set_start_of_execution(self, **kwargs):
        s = self.build_session()
        base_url = self.get_object_url()
        payload = {"json": kwargs}
        url = f"{base_url}/{self.id}/set_start_of_execution/"
        r = make_request(s=s, loaders=self.LOADERS, r_type="PATCH", url=url, payload=payload)
        if r.status_code != 201:
            raise Exception(f"Error in request {r.text}")

        result = r.json()
        if result["last_time_index_value"] is not None:
            result["last_time_index_value"] = datetime.datetime.fromtimestamp(result["last_time_index_value"]).replace(
                tzinfo=pytz.utc)

        if result['update_statistics'] is not None:
            result['update_statistics'] = {k: request_to_datetime(v) for k, v in result['update_statistics'].items()}

        last_observation = pd.DataFrame()
        if result["last_observation"] is not None:
            last_observation = pd.DataFrame(result["last_observation"]).T
            last_observation[result["time_index_name"]] = pd.to_datetime(last_observation.loc[:, result["time_index_name"]], unit='s', utc=True)
            if len(result["index_names"]) > 1:
                last_observation.index.name="unique_identifier"
            else:
                raise NotImplementedError
            last_observation = last_observation.reset_index().set_index(result["index_names"])

        hu = LocalTimeSeriesHistoricalUpdate(
            **result["historical_update"],
            update_statistics=DataUpdates(
                update_statistics=result['update_statistics'],
                max_time_index_value=result["last_time_index_value"],
                last_observation=last_observation,
            ),
            must_update=result["must_update"],
            direct_dependencies_ids=result["direct_dependencies_ids"]
        )
        return hu

    def set_end_of_execution(
            self,
            historical_update_id: int,
            timeout=None,threaded_request=True,
            **kwargs
    ):
        s = self.build_session()
        url = self.get_object_url() + f"/{self.id}/set_end_of_execution/"
        kwargs.update(dict(historical_update_id=historical_update_id))
        payload = {"json": kwargs}

        def _do_request():
            r = make_request(s=s, loaders=self.LOADERS, r_type="PATCH", url=url, payload=payload, time_out=timeout)
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
        r = make_request(s=s, loaders=cls.LOADERS, r_type="PATCH", url=url, payload=payload, time_out=timeout)
        if r.status_code != 200:
            raise Exception(f"Error in request ")

    @classmethod
    def set_last_update_index_time(cls, metadata, timeout=None):
        s = cls.build_session()
        url = cls.get_object_url() + f"/{metadata['id']}/set_last_update_index_time/"
        r = make_request(s=s, loaders=cls.LOADERS, r_type="GET", url=url, time_out=timeout)

        if r.status_code == 404:
            raise SourceTableConfigurationDoesNotExist

        if r.status_code != 200:
            raise Exception(f"{metadata['local_hash_id']}{r.text}")
        return r

    def set_last_update_index_time_from_update_stats(
            self,
            last_time_index_value: float,
            max_per_asset_symbol,
            last_observation: dict,
            timeout=None
    ) -> "LocalTimeSerie":
        s = self.build_session()
        url = self.get_object_url() + f"/{self.id}/set_last_update_index_time_from_update_stats/"
        payload = {
            "json": {
                "last_time_index_value": last_time_index_value,
                "max_per_asset_symbol": max_per_asset_symbol,
                "last_observation": last_observation,
            }
        }
        logger.debug(f"Set last update index with {payload['json']}")
        r = make_request(s=s, loaders=self.LOADERS, payload=payload, r_type="POST", url=url, time_out=timeout)

        if r.status_code == 404:
            raise SourceTableConfigurationDoesNotExist

        if r.status_code != 200:
            raise Exception(f"{self.local_hash_id}{r.text}")
        return LocalTimeSerie(**r.json())

    @classmethod
    def create_historical_update(cls, *args, **kwargs):
        s = cls.build_session()
        base_url = cls.ENDPOINT["LocalTimeSerieHistoricalUpdate"]
        data = serialize_to_json(kwargs)
        payload = {"json": data, }
        r = make_request(s=s, loaders=cls.LOADERS, r_type="POST", url=f"{base_url}/", payload=payload)
        if r.status_code != 201:
            raise Exception(f"Error in request {r.url} {r.text}")

    @classmethod
    def get_mermaid_dependency_diagram(cls, local_hash_id, data_source_id, desc=True, timeout=None) -> dict:
        s = cls.build_session()
        url = cls.get_object_url("TimeSerie") + f"/{local_hash_id}/dependencies_graph_mermaid?desc={desc}&data_source_id={data_source_id}"
        r = make_request(s=s, loaders=cls.LOADERS, r_type="GET", url=url,
                         time_out=timeout)
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
        return depth_df

    @classmethod
    def get_upstream_nodes(cls, hash_id, data_source_id, timeout=None):
        s = cls.build_session()
        url = cls.get_object_url("TimeSerie") + f"/{hash_id}/get_upstream_nodes?data_source_id={data_source_id}"
        r = make_request(s=s, loaders=cls.LOADERS, r_type="GET", url=url, time_out=timeout)
        if r.status_code != 200:
            raise Exception(f"Error in request {r.text}")

        depth_df = pd.DataFrame(r.json())
        return depth_df

    @classmethod
    def create(cls, timeout=None, *args, **kwargs):
        url = cls.get_object_url("TimeSerie") + "/"
        payload = {"json": serialize_to_json(kwargs)}
        s = cls.build_session()
        r = make_request(s=s, loaders=cls.LOADERS, r_type="POST", url=url, payload=payload, time_out=timeout)
        if r.status_code != 201:
            raise Exception(f"Error in request {r.text}")
        instance = cls(**r.json())
        return instance

    def set_ogm_dependencies_linked(self):
        s = self.build_session()
        url = self.get_object_url("LocalTimeSerieNodesMethods") + f"/{self.id}/set_ogm_dependencies_linked"
        r = make_request(s=s, loaders=self.LOADERS, r_type="GET", url=url, )
        if r.status_code != 200:
            raise Exception(f"Error in request {r.text}")

        return r

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

    def get_data_between_dates_from_api(
            self,
           *args,**kwargs
    ):

        return self.remote_table.get_data_between_dates_from_api(  *args,**kwargs)


    @classmethod
    def post_data_frame_in_chunks(
            cls,
            serialized_data_frame: pd.DataFrame,
            chunk_size: int = 50_000,
            local_metadata: dict = None,
            data_source: str = None,
            index_names: list = None,
            time_index_name: str = 'timestamp',
            overwrite: bool = False,
    ):
        """
            Sends a large DataFrame to a Django backend in multiple chunks.

            :param serialized_data_frame: The DataFrame to upload.
            :param url: The endpoint URL (e.g. https://yourapi.com/upload-chunk/).
            :param chunk_size: Number of rows per chunk.
            :param local_metadata: General metadata dict you want to send with each chunk.
            :param data_source: Additional info about the source of the data.
            :param index_names: Index columns in the DataFrame.
            :param time_index_name: The column name used for time indexing.
            :param overwrite: Boolean indicating whether existing data should be overwritten.
            """
        s = cls.build_session()
        url = cls.get_object_url() + f"/{local_metadata.id}/insert_data_into_table/"
        total_rows = len(serialized_data_frame)
        total_chunks = math.ceil(total_rows / chunk_size)
        logger.info(f"Starting upload of {total_rows} rows in {total_chunks} chunk(s).")
        for i in range(total_chunks):
            start_idx = i * chunk_size
            end_idx = min((i + 1) * chunk_size, total_rows)

            # Slice the DataFrame for the current chunk
            chunk_df = serialized_data_frame.iloc[start_idx:end_idx]

            # Compute grouped_dates for this chunk
            chunk_stats, grouped_dates, last_chunk_observation = get_chunk_stats(
                chunk_df=chunk_df,
                index_names=index_names,
                time_index_name=time_index_name
            )

            # Convert the chunk to JSON
            chunk_json_str = chunk_df.to_json(orient="records", date_format="iso")

            # (Optional) Compress JSON using gzip then base64-encode
            compressed = gzip.compress(chunk_json_str.encode('utf-8'))
            compressed_b64 = base64.b64encode(compressed).decode('utf-8')

            payload = dict(json={
                "data": compressed_b64,  # compressed JSON data
                "chunk_stats": chunk_stats,
                "overwrite": overwrite,
                "chunk_index": i,
                "total_chunks": total_chunks,
                "last_observation": last_chunk_observation
            })
            try:
                r = make_request(s=s, loaders=None, payload=payload, r_type="POST", url=url, time_out=60 * 15)
                if r.status_code not in [200, 204]:
                    logger.warning(f"Error in request: {r.text}")
                logger.info(f"Chunk {i + 1}/{total_chunks} uploaded successfully.")
            except requests.exceptions.RequestException as e:
                logger.exception(f"Error uploading chunk {i + 1}/{total_chunks}: {e}")
                # Optionally, you could retry or break here
                raise e
            if r.status_code not in [200, 204]:
                raise Exception(r.text)

    @classmethod
    def get_metadatas_and_set_updates(
            cls,
            local_time_series_ids: list,
            update_details_kwargs,
            update_priority_dict
    ):
        """
        {'local_hash_id__in': [{'local_hash_id': 'alpacaequitybarstest_97018e7280c1bad321b3f4153cc7e986', 'data_source_id': 1},
        :param local_hash_id__in:
        :param multi_index_asset_symbols_filter:
        :param update_details_kwargs:
        :param update_priority_dict:
        :return:
        """
        base_url = cls.get_object_url()
        s = cls.build_session()
        payload = {"json": dict(local_time_series_ids=local_time_series_ids,
                                update_details_kwargs=update_details_kwargs,
                                update_priority_dict=update_priority_dict,
                                )}
        # r = self.s.post(f"{base_url}/get_metadatas_and_set_updates/", **payload)
        url = f"{base_url}/get_metadatas_and_set_updates/"
        r = make_request(s=s, loaders=cls.LOADERS, r_type="POST", url=url, payload=payload)
        if r.status_code != 200:
            raise Exception(f"Error in request {r.text}")
        r = r.json()
        r["source_table_config_map"] = {int(k): SourceTableConfiguration(**v) if v is not None else v for k, v in r["source_table_config_map"].items()}
        r["state_data"] = {int(k): LocalTimeSerieUpdateDetails(**v) for k, v in r["state_data"].items()}
        r["all_index_stats"] = {int(k): v for k, v in r["all_index_stats"].items()}
        r["local_metadatas"] = [LocalTimeSerie(**v) for v in r["local_metadatas"]]
        return r

    @classmethod
    def depends_on_connect_remote_table(cls, source_hash_id: str,
                                        source_local_hash_id: str,
                                        source_data_source_id: id,
                                        target_data_source_id: id,
                                        target_local_hash_id: str):
        s = cls.build_session()
        url = cls.get_object_url("TimeSerie") + "/depends_on_connect_remote_table/"
        payload = dict(json={"source_hash_id": source_hash_id,
                             "source_local_hash_id": source_local_hash_id,
                             "source_data_source_id": source_data_source_id,
                             "target_data_source_id": target_data_source_id,
                             "target_local_hash_id": target_local_hash_id,
                             })
        r = make_request(s=s, loaders=cls.LOADERS, r_type="POST", url=url, payload=payload)
        if r.status_code != 201:
            raise Exception(f"Error in request {r.text}")

    @classmethod
    def depends_on_connect(cls, target_class_name: str,
                           source_local_hash_id: str,
                           target_local_hash_id: str,
                           source_data_source_id: id,
                           target_data_source_id: id,
                         ):

        s = cls.build_session()
        url = cls.get_object_url("TimeSerie") + "/depends_on_connect/"
        payload = dict(json={"target_class_name": target_class_name,
                             "source_local_hash_id": source_local_hash_id, "target_local_hash_id": target_local_hash_id,

                             "source_data_source_id": source_data_source_id,
                             "target_data_source_id": target_data_source_id,
                             })
        r = make_request(s=s, loaders=cls.LOADERS, r_type="POST", url=url, payload=payload)
        if r.status_code != 201:
            raise Exception(f"Error in request {r.text}")

class DynamicTableMetaData(BasePydanticModel, BaseObjectOrm):
    id: int = Field(None, description="Primary key, auto-incremented ID")
    hash_id: str = Field(..., max_length=63, description="Max length of PostgreSQL table name")
    table_name: Optional[str] = Field(None, max_length=63, description="Max length of PostgreSQL table name")
    creation_date: datetime.datetime = Field(..., description="Creation timestamp")
    created_by_user: Optional[int] = Field(None, description="Foreign key reference to User")
    organization_owner: int = Field(None, description="Foreign key reference to Organization")
    open_for_everyone: bool = Field(default=False, description="Whether the table is open for everyone")
    data_source_open_for_everyone: bool = Field(default=False,
                                                description="Whether the data source is open for everyone")
    build_configuration: Optional[Dict[str, Any]] = Field(None, description="Configuration in JSON format")
    build_meta_data: Optional[Dict[str, Any]] = Field(None, description="Optional YAML metadata")
    time_serie_source_code_git_hash: Optional[str] = Field(None, max_length=255,
                                                           description="Git hash of the time series source code")
    time_serie_source_code: Optional[str] = Field(None, description="File path for time series source code")
    protect_from_deletion: bool = Field(default=False, description="Flag to protect the record from deletion")
    data_source: Union[int, "DynamicTableDataSource"]
    source_class_name: str
    sourcetableconfiguration: Optional[SourceTableConfiguration] = None
    table_index_names:Optional[Dict] = None

    #TS specifi
    compression_policy_config: Optional[Dict] = None
    retention_policy_config: Optional[Dict] = None

    _drop_indices: bool = False  # for direct incertion we can pass this values
    _rebuild_indices: bool = False  # for direct incertion we can pass this values

    def patch(self, time_out: Union[None, int] = None, *args, **kwargs, ):
        url = self.get_object_url() + f"/{self.id}/"
        payload = {"json": serialize_to_json(kwargs)}
        s = self.build_session()
        r = make_request(s=s, loaders=self.LOADERS, r_type="PATCH", url=url, payload=payload, time_out=time_out)
        if r.status_code != 200:
            raise Exception(f"Error in request {r.text}")
        return self.__class__(**r.json())

    @classmethod
    def patch_by_hash(cls, hash_id: str, *args, **kwargs):
        metadata = cls.get(hash_id=hash_id)
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
        r = make_request(r_type="PATCH", url=url, payload=payload, s=s, loaders=self.LOADERS, )
        if r.status_code != 202:
            raise Exception(f"Error in request {r.text}")


    @classmethod
    def patch_build_configuration(
            cls,
            remote_table_patch: Union[dict, None],
            build_meta_data: dict,
            data_source_id: int,
            local_table_patch: dict,
    ) :
        url = cls.get_object_url("TimeSerie") + "/patch_build_configuration"
        payload = {"json": {"remote_table_patch": remote_table_patch, "local_table_patch": local_table_patch,
                            "build_meta_data": build_meta_data, "data_source_id": data_source_id,
                            }}
        s = cls.build_session()
        r = make_request(s=s, loaders=cls.LOADERS, r_type="POST", url=url, payload=payload,

                         )
        if r.status_code != 200:
            raise Exception(r.text)

    def delete_table(self):
        data_source = PodDataSource._get_duck_db()
        duckdb_dynamic_data_source = DynamicTableDataSource.get_or_create_duck_db(
            related_resource=data_source.id,
        )
        if (isinstance(self.data_source, int) and self.data_source.id == duckdb_dynamic_data_source.id) or \
                (not isinstance(self.data_source, int) and self.data_source.related_resource.class_type == "duck_db"):
            db_interface = DuckDBInterface()
            db_interface.drop_table(self.table_name)

        self.delete()

    def get_data_between_dates_from_api(
            self,
            start_date: datetime.datetime=None,
            end_date: datetime.datetime=None,
            great_or_equal: bool=None,
            less_or_equal: bool=None,
            unique_identifier_list: list=None,
            columns: list=None,
            unique_identifier_range_map: Union[None, UniqueIdentifierRangeMap]=None
        ):
        """ Helper function to make a single batch request (or multiple paged requests if next_offset). """
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
            for unique_identifier, date_info in unique_identifier_range_map.items():
                # Convert start_date if present
                if 'start_date' in date_info and isinstance(date_info['start_date'], datetime.datetime):
                    date_info['start_date'] = int(date_info['start_date'].timestamp())

                # Convert end_date if present
                if 'end_date' in date_info and isinstance(date_info['end_date'], datetime.datetime):
                    date_info['end_date'] = int(date_info['end_date'].timestamp())

        all_results = []
        if unique_identifier_range_map:
            keys = list(unique_identifier_range_map.keys())
            chunk_size = 100
            for start_idx in range(0, len(keys), chunk_size):
                key_chunk = keys[start_idx: start_idx + chunk_size]

                # Build sub-dictionary for this chunk
                chunk_map = {
                    k: unique_identifier_range_map[k] for k in key_chunk
                }

                # Fetch data (including any pagination via next_offset)
                chunk_results = fetch_one_batch(chunk_map)
                all_results.extend(chunk_results)
        else:
            # If unique_identifier_range_map is None, do a single batch with offset-based pagination.
            chunk_results = fetch_one_batch(None)
            all_results.extend(chunk_results)

        return pd.DataFrame(all_results)


class Scheduler(BasePydanticModel, BaseObjectOrm):
    uid: str
    name: str
    is_running: bool
    running_process_pid: Optional[int]
    running_in_debug_mode: bool
    updates_halted: bool
    host: Optional[str]
    api_address: Optional[str]
    api_port: Optional[int]
    pre_loads_in_tree: Optional[List[str]] = None  # Assuming this is a list of strings
    in_active_tree: Optional[List[LocalTimeSerieNode]] = None  # Assuming this is a list of strings
    schedules_to: Optional[List[LocalTimeSerieNode]] = None
    # for heartbeat
    _stop_heart_beat: bool = False
    _executor: Optional[object] = None

    @classmethod
    def get(cls, *args, **kwargs):
        return super().get(*args, **kwargs)

    @classmethod
    def filter(cls, payload: Union[dict, None]):
        return super().filter(payload)

    @classmethod
    def get_scheduler_for_ts(cls, hash_id: str):

        s = cls.build_session()
        url = cls.get_object_url() + "/get_scheduler_for_ts/"
        payload = dict(params={"hash_id": hash_id})
        r = make_request(s=s, loaders=cls.LOADERS, r_type="GET", url=url, payload=payload)
        if r.status_code == 404:
            raise SchedulerDoesNotExist(r.text)
        if r.status_code != 200:
            raise Exception(f"Error in request {r.text}")
        scheduler = cls(**r.json())
        return scheduler

    @classmethod
    def initialize_debug_for_ts(cls, local_hash_id: str,
                                data_source_id: int,
                                name_suffix: Union[str, None] = None, ):


        s = cls.build_session()
        url = cls.get_object_url() + "/initialize_debug_for_ts/"
        payload = dict(json={"local_hash_id": local_hash_id, "name_suffix": name_suffix,
                             "data_source_id": data_source_id
                             })
        r = make_request(s=s, loaders=cls.LOADERS, r_type="POST", url=url, payload=payload)
        if r.status_code != 201:
            raise Exception(f"Error in request {r.text}")
        scheduler = cls(**r.json())
        return scheduler

    @classmethod
    def build_and_assign_to_ts(cls, scheduler_name: str, local_hash_id_list: list, delink_all_ts=False,
                               remove_from_other_schedulers=True, **kwargs):



        s = cls.build_session()
        url = cls.get_object_url() + "/build_and_assign_to_ts/"
        payload = dict(json={
            "scheduler_name": scheduler_name,
            "delink_all_ts": delink_all_ts,
            "hash_id_list": local_hash_id_list,
            "remove_from_other_schedulers": remove_from_other_schedulers,
            "scheduler_kwargs": kwargs
        })
        r = make_request(s=s, loaders=cls.LOADERS, r_type="POST", url=url, payload=payload)
        if r.status_code != 201:
            raise Exception(f"Error in request {r.text}")
        scheduler = cls(**r.json())
        return scheduler

    def in_active_tree_connect(self, local_time_series_ids: list):

        s = self.build_session()
        url = self.get_object_url() + f"/{self.uid}/in_active_tree_connect/"
        payload = dict(json={"local_time_series_ids": local_time_series_ids})
        r = make_request(s=s, loaders=self.LOADERS, r_type="PATCH", url=url, payload=payload)
        if r.status_code != 201:
            raise Exception(f"Error in request {r.text}")

    def assign_to_scheduler(self, hash_id_list: list):

        s = self.build_session()
        url = self.get_object_url() + f"/{self.uid}/assign_to_scheduler/"
        payload = dict(json={"hash_id_list": hash_id_list})
        r = make_request(s=s, loaders=self.LOADERS, r_type="PATCH", url=url, payload=payload)
        if r.status_code != 200:
            raise Exception(f"Error in request {r.text}")
        return Scheduler(**r.json())

    def is_scheduler_running_in_process(self):
        # test call
        if self.is_running == True and hasattr(self, "api_address"):
            # verify  scheduler host is the same
            if self.api_address == get_network_ip() and is_process_running(self.running_process_pid) == True:
                return True
        return False

    def _heart_beat_patch(self):
        try:
            scheduler = self.patch(is_running=True,
                                   running_process_pid=os.getpid(),
                                   running_in_debug_mode=self.running_in_debug_mode,
                                   last_heart_beat=datetime.datetime.utcnow().replace(
                                       tzinfo=pytz.utc).timestamp(),
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
                if self._stop_heart_beat == True:
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

    def patch(self, time_out=None, *args, **kwargs):
        url = self.get_object_url() + f"/{self.uid}/update"
        payload = {"json": serialize_to_json(kwargs)}
        s = self.build_session()
        r = make_request(s=s, loaders=self.LOADERS, r_type="PATCH", url=url, payload=payload, time_out=time_out)
        if r.status_code != 200:
            raise Exception(f"Error in request {r.text}")
        return Scheduler(**r.json())

class RunConfiguration(BasePydanticModel, BaseObjectOrm):
    local_time_serie_update_details: Optional[int] = None
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

class LocalTimeSerieUpdateDetails(BasePydanticModel, BaseObjectOrm):
    related_table: Union[int, LocalTimeSerie]
    active_update: bool = Field(default=False, description="Flag to indicate if update is active")
    update_pid: int = Field(default=0, description="Process ID of the update")
    error_on_last_update: bool = Field(default=False,
                                       description="Flag to indicate if there was an error in the last update")
    last_update: Optional[datetime.datetime] = Field(None, description="Timestamp of the last update")
    next_update: Optional[datetime.datetime] = Field(None, description="Timestamp of the next update")
    update_statistics: Optional[Dict[str, Any]] = Field(None, description="JSON field for update statistics")
    active_update_status: str = Field(default="Q", max_length=20, description="Current update status")
    active_update_scheduler_uid: Optional[str] = Field(None, max_length=100,
                                                       description="Scheduler UID for active update")
    update_priority: int = Field(default=0, description="Priority level of the update")
    direct_dependencies: List = Field(default=[], description="List of direct upstream dependencies IDs")
    last_updated_by_user: Optional[int] = Field(None, description="Foreign key reference to User")

    run_configuration: Optional["RunConfiguration"]=None

    @staticmethod
    def _parse_parameters_filter(parameters):
        for key, value in parameters.items():
            if "__in" in key:
                assert isinstance(value, list)
                parameters[key] = ",".join(value)
        return parameters

class DataUpdates(BaseModel):
    """
    This class contains the  update details of the table in the main sequence engine
    """
    update_statistics: Optional[Dict[str, Union[datetime.datetime, None]]] = None
    max_time_index_value: Optional[datetime.datetime] = None  # does not include fitler
    _max_time_in_update_statistics: Optional[datetime.datetime] = None  # include filter
    last_observation: Optional[pd.DataFrame] = Field(default_factory=pd.DataFrame)

    asset_list:Optional[List]=None


    class Config:
        arbitrary_types_allowed = True

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

    def is_empty(self):
        return self.update_statistics is None and self.max_time_index_value is None

    def get_min_latest_value(self, init_fallback_date: datetime = None):
        if not self.update_statistics:
            return init_fallback_date
        return min(self.update_statistics.values())

    def get_max_latest_value(self, init_fallback_date: datetime = None):
        if not self.update_statistics:
            if self.max_time_index_value:
                return self.max_time_index_value #its a 1 colum index
            return init_fallback_date
        return max(self.update_statistics.values())

    def asset_identifier(self):
        return list(self.update_statistics.keys())

    def get_update_range_map_great_or_equal(self):
        range_map={k:DateInfo({"start_date_operand":">=","start_date":v}) for k,v in self.update_statistics.items()}
        return range_map

    def update_assets(
            self,
            asset_list: Optional[List],
            *,
            init_fallback_date: datetime = None,
            unique_identifier_list: Union[list, None] = None
    ):
        self.asset_list = asset_list
        new_update_statistics = self.update_statistics
        last_observation = self.last_observation
        if asset_list is not None or unique_identifier_list is not None:
            new_update_statistics = {}
            unique_identifier_list = [a.unique_identifier for a in asset_list] if unique_identifier_list is None else unique_identifier_list

            for unique_identifier in unique_identifier_list:

                if self.update_statistics and unique_identifier in self.update_statistics:
                    new_update_statistics[unique_identifier] = self.update_statistics[unique_identifier]
                else:
                    if init_fallback_date is None: raise ValueError(f"No initial start date for {unique_identifier} assets defined")
                    new_update_statistics[unique_identifier] = init_fallback_date

            _max_time_in_update_statistics = max(new_update_statistics.values()) if len(new_update_statistics) > 0 else None
            if not last_observation.empty:
                last_observation = last_observation[last_observation.index.get_level_values("unique_identifier").isin(unique_identifier_list)]
        else:
            _max_time_in_update_statistics = self.max_time_index_value or init_fallback_date

        du = DataUpdates(
            update_statistics=new_update_statistics,
            max_time_index_value=self.max_time_index_value,
            last_observation=last_observation,
            asset_list=asset_list
        )
        du._max_time_in_update_statistics = _max_time_in_update_statistics
        return du

    def is_empty(self):
        return self.max_time_index_value is None

    def __getitem__(self, key: str) -> Any:
        if self.update_statistics is None:
            raise KeyError(f"{key} not found (update_statistics is None).")
        return self.update_statistics[key]

    def __setitem__(self, key: str, value: Any) -> None:
        if self.update_statistics is None:
            self.update_statistics = {}
        self.update_statistics[key] = value

    def __delitem__(self, key: str) -> None:
        if not self.update_statistics or key not in self.update_statistics:
            raise KeyError(f"{key} not found in update_statistics.")
        del self.update_statistics[key]

    def __iter__(self):
        """Iterate over keys."""
        if self.update_statistics is None:
            return iter([])
        return iter(self.update_statistics)

    def __len__(self) -> int:
        if not self.update_statistics:
            return 0
        return len(self.update_statistics)

    def keys(self):
        if not self.update_statistics:
            return []
        return self.update_statistics.keys()

    def values(self):
        if not self.update_statistics:
            return []
        return self.update_statistics.values()

    def items(self):
        if not self.update_statistics:
            return []
        return self.update_statistics.items()

    def filter_df_by_latest_value(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.shape[0] == 0:
            return df

        if not self.is_empty():
            if (self.update_statistics is None or "unique_identifier" not in df.index.names) and self.max_time_index_value is not None:
                # single index time serie
                return df[df.index>=self.max_time_index_value]

            for unique_identifier, last_update in self.update_statistics.items():
                df = df[
                    (
                        (df.index.get_level_values("unique_identifier") == unique_identifier) &
                        (df.index.get_level_values("time_index") > last_update)
                    )
                    |
                    (
                        (df.index.get_level_values("unique_identifier") != unique_identifier)
                    )
                ]
            duplicated = df.index.duplicated(keep='first')

            if duplicated.any():
                num_duplicates = duplicated.sum()
                logger.warning(
                    f"Removed {num_duplicates} duplicated rows for unique_identifier and time_index combinations.")
                df = df[~duplicated]
            return df
        return df

def get_chunk_stats(chunk_df, time_index_name, index_names):
    chunk_stats = {
        "_GLOBAL_": {
            "max": chunk_df[time_index_name].max().timestamp(),
            "min": chunk_df[time_index_name].min().timestamp()
        }
    }

    last_observation = chunk_df.loc[[chunk_df['time_index'].idxmax()]]
    last_observation[time_index_name] = last_observation[time_index_name].apply(lambda x: x.timestamp())
    last_observation = last_observation.set_index(time_index_name).to_dict("index")

    grouped_dates = None
    if len(index_names) > 1:
        grouped_dates = chunk_df.groupby(["unique_identifier"])[
            time_index_name].agg(
            ["min", "max"])
        chunk_stats["_PER_ASSET_"] = {
            row["unique_identifier"]: {
                "max": row["max"].timestamp(),
                "min": row["min"].timestamp(),
            }
            for _, row in grouped_dates.reset_index().iterrows()
        }

        idx = chunk_df.groupby('unique_identifier')[time_index_name].idxmax()
        last_observation = chunk_df.loc[idx].reset_index(drop=True)
        last_observation[time_index_name] = last_observation[time_index_name].apply(lambda x: x.timestamp())
        last_observation = last_observation.set_index("unique_identifier").to_dict("index")

    return chunk_stats, grouped_dates, last_observation

class LocalTimeSeriesHistoricalUpdate(BasePydanticModel, BaseObjectOrm):
    id: Optional[int] = None
    related_table: int  # Assuming you're using the ID of the related table
    update_time_start: datetime.datetime
    update_time_end: Optional[datetime.datetime] = None
    error_on_update: bool = False
    trace_id: Optional[str] = Field(default=None, max_length=255)
    updated_by_user: Optional[int] = None  # Assuming you're using the ID of the user

    last_time_index_value: Optional[datetime.datetime] = None

    # extra fields for local control
    update_statistics: Optional[DataUpdates]
    must_update: Optional[bool]
    direct_dependencies_ids: Optional[List[int]]

class DataSource(BasePydanticModel, BaseObjectOrm):
    id: Optional[int] = Field(None, description="The unique identifier of the Local Disk Source Lake")
    display_name: str
    organization: Optional[int] = Field(None, description="The unique identifier of the Local Disk Source Lake")
    class_type: str
    status: str
    extra_arguments: Optional[Dict] = None

    @classmethod
    def get_or_create_duck_db(cls, time_out=None, *args, **kwargs):
        url = cls.get_object_url() + f"/get_or_create_duck_db/"
        payload = {"json": serialize_to_json(kwargs)}
        s = cls.build_session()
        r = make_request(s=s, loaders=cls.LOADERS, r_type="POST", url=url, payload=payload, time_out=time_out)
        if r.status_code not in [200, 201]:
            raise Exception(f"Error in request {r.text}")
        return cls(**r.json())

    def insert_data_into_table(
            self,
            serialized_data_frame: pd.DataFrame,
            local_metadata: LocalTimeSerie,
            overwrite: bool,
            time_index_name: str,
            index_names: list,
            grouped_dates: dict,
    ):

        if self.class_type == "duck_db":
            DuckDBInterface().upsert(
                df=serialized_data_frame,
                table=local_metadata.remote_table.table_name
            )
        else:
            LocalTimeSerie.post_data_frame_in_chunks(
                serialized_data_frame=serialized_data_frame,
                local_metadata=local_metadata,
                data_source=self,
                index_names=index_names,
                time_index_name=time_index_name,
                overwrite=overwrite,
            )

    def insert_data_into_local_table(
            self,
            serialized_data_frame: pd.DataFrame,
            local_metadata: LocalTimeSerie,
            overwrite: bool,
            time_index_name: str,
            index_names: list,
            grouped_dates: dict,
    ):

        # LocalTimeSerie.post_data_frame_in_chunks(
        #     serialized_data_frame=serialized_data_frame,
        #     local_metadata=local_metadata,
        #     data_source=self,
        #     index_names=index_names,
        #     time_index_name=time_index_name,
        #     overwrite=overwrite,
        # )
        raise NotImplementedError

    def get_data_by_time_index(
            self,
            local_metadata: dict,
            start_date: Optional[datetime.datetime] = None,
            end_date: Optional[datetime.datetime] = None,
            great_or_equal: bool = True,
            less_or_equal: bool = True,
            columns: Optional[List[str]] = None,
            unique_identifier_list: Optional[List[str]] = None,
            unique_identifier_range_map: Optional[UniqueIdentifierRangeMap] = None,
    ) -> pd.DataFrame:

        if self.class_type == "duck_db":
            db_interface = DuckDBInterface()
            table_name = local_metadata.remote_table.table_name

            df = db_interface.read(
                table=table_name,
                start=start_date,
                end=end_date,
                great_or_equal=great_or_equal,
                less_or_equal=less_or_equal,
                ids=unique_identifier_list,
                columns=columns,
                unique_identifier_range_map=unique_identifier_range_map # Pass range map
            )
        else:
            df = local_metadata.get_data_between_dates_from_api(
                start_date=start_date,
                end_date=end_date,
                great_or_equal=great_or_equal,
                less_or_equal=less_or_equal,
                unique_identifier_list=unique_identifier_list,
                columns=columns,
                unique_identifier_range_map=unique_identifier_range_map
            )
        if len(df) == 0:
            logger.warning(
                f"No data returned from remote API for {local_metadata.local_hash_id}"
            )
            return df

        stc = local_metadata.remote_table.sourcetableconfiguration
        try:
            df[stc.time_index_name] = pd.to_datetime(df[stc.time_index_name], format='ISO8601')
        except Exception as e:
            raise e
        for c, c_type in stc.column_dtypes_map.items():
            if c != stc.time_index_name:
                if c_type == "object":
                    c_type = "str"
                df[c] = df[c].astype(c_type)
        df = df.set_index(stc.index_names)
        return df


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
        data = r.json()

        return cls(**r.json())

    def persist_to_pickle(self, path):
        import cloudpickle
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'wb') as handle:
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
        return self.related_resource.class_type == 'direct'

    def get_data_by_time_index(self, *args, **kwargs):
        if self.has_direct_postgres_connection():
            stc = kwargs["local_metadata"].remote_table.sourcetableconfiguration

            df = TimeScaleInterface.direct_data_from_db(
                connection_uri=self.related_resource.get_connection_uri(),
                *args, **kwargs,

            )
            df = set_types_in_table(df, stc.column_dtypes_map)
            return df
        else:
            return self.related_resource.get_data_by_time_index(*args, **kwargs)

    def insert_data_into_table(self, *args, **kwargs):
        if self.has_direct_postgres_connection():
            TimeScaleInterface.process_and_update_table(
                data_source=self.related_resource,
                *args, **kwargs,
            )

        else:
            self.related_resource.insert_data_into_table(*args, **kwargs)

class Project(BasePydanticModel, BaseObjectOrm):
    id: int
    project_name: str
    data_source: DynamicTableDataSource

    @classmethod
    def get_user_default_project(cls):
        url = cls.get_object_url() + "/get_user_default_project/"

        s = cls.build_session()
        r = make_request(s=s, loaders=cls.LOADERS, r_type="GET", url=url, )
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
            local_metadata: dict,
            overwrite: bool,
            time_index_name: str,
            index_names: list,
            grouped_dates: dict,
    ):

        LocalTimeSerie.post_data_frame_in_chunks(
            serialized_data_frame=serialized_data_frame,
            local_metadata=local_metadata,
            data_source=self,
            index_names=index_names,
            time_index_name=time_index_name,
            overwrite=overwrite,
        )

    def filter_by_assets_ranges(
            self,
            asset_ranges_map: dict,
            metadata: dict,
            local_hash_id: str,
            has_direct_connection: bool
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
                column_types=column_types
            )
        else:
            df = LocalTimeSerie.get_data_between_dates_from_api(
                local_hash_id=local_hash_id,
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
            local_metadata: dict,
            start_date: Optional[datetime.datetime] = None,
            end_date: Optional[datetime.datetime] = None,
            great_or_equal: bool = True,
            less_or_equal: bool = True,
            columns: Optional[List[str]] = None,
            unique_identifier_list: Optional[List[str]] = None,

    ) -> pd.DataFrame:

        metadata = local_metadata.remote_table

        df = local_metadata.get_data_between_dates_from_api(

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
                    f"No data returned from remote API for {local_metadata.local_hash_id}"
                )
            return df

        stc = local_metadata.remote_table.sourcetableconfiguration
        df[stc.time_index_name] = pd.to_datetime(df[stc.time_index_name])
        for c, c_type in stc.column_dtypes_map.items():
            if c != stc.time_index_name:
                if c_type == "object":
                    c_type = "str"
                df[c] = df[c].astype(c_type)
        df = df.set_index(stc.index_names)
        return df


class DynamicResource(BasePydanticModel, BaseObjectOrm):
    id:Optional[int]=None
    name: str
    type: str
    object_signature : dict
    markdown_documentation : str
    default_yaml: str
    attributes: Optional[dict]

    created_at:datetime.datetime
    updated_at:datetime.datetime
    is_production:bool
    pod: int

def register_default_configuration(json_payload: dict, timeout=None):
    url = TDAG_ENDPOINT + "/orm/api/tdag-gpt/register_default_configuration/"
    from requests.adapters import HTTPAdapter, Retry
    s = requests.Session()
    s.headers.update(loaders.auth_headers)
    retries = Retry(total=2, backoff_factor=2)
    s.mount('http://', HTTPAdapter(max_retries=retries))

    r = make_request(s=s, r_type="POST", url=url, payload={"json": json_payload},
                     loaders=loaders, time_out=timeout)
    return r

def create_configuration_for_strategy(json_payload: dict, timeout=None):
    url = TDAG_ENDPOINT + "/orm/api/tdag-gpt/create_configuration_for_strategy/"
    from requests.adapters import HTTPAdapter, Retry
    s = requests.Session()
    s.headers.update(loaders.auth_headers)
    retries = Retry(total=2, backoff_factor=2)
    s.mount('http://', HTTPAdapter(max_retries=retries))

    r = make_request(s=s, r_type="POST", url=url, payload={"json": json_payload},
                     loaders=loaders, time_out=200)
    return r


def query_agent(json_payload: dict, timeout=None):
    url = TDAG_ENDPOINT + "/orm/api/tdag-gpt/query_agent/"
    from requests.adapters import HTTPAdapter, Retry
    s = requests.Session()
    s.headers.update(loaders.auth_headers)
    retries = Retry(total=2, backoff_factor=2)
    s.mount('http://', HTTPAdapter(max_retries=retries))

    r = make_request(s=s, r_type="POST", url=url, payload={"json": json_payload},
                     loaders=loaders, time_out=200)
    return r


def add_created_object_to_jobrun(model_name: str, app_label: str, object_id: int, timeout: Optional[int] = None) -> dict:
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
    payload = {
        "json": {
            "app_label": app_label,
            "model_name": model_name,
            "object_id": object_id
        }
    }
    r = make_request(
        s=s,
        loaders=loaders,
        r_type="POST",
        url=url,
        payload=payload,
        time_out=timeout
    )
    if r.status_code not in [200, 201]:
        raise Exception(f"Failed to add created object: {r.status_code} - {r.text}")
    return r.json()


class Artifact(BasePydanticModel, BaseObjectOrm):
    id: Optional[int]
    name: str
    created_by_resource_name: str
    bucket_name: str
    content: Any

    @staticmethod
    def upload_file(filepath, name, created_by_resource_name, bucket_name=None):

        with open(filepath, "rb") as f:
            data = {
                "name": name,
                "created_by_resource_name": created_by_resource_name,
                "bucket_name": bucket_name if bucket_name else "default_bucket",
            }
            files = {"content": (str(filepath), f, "application/pdf")}
            artifact = Artifact.create(files=files, **data)
            return artifact

# TODO can we remove this?? ROOT_URLS does not seem to exist
class DynamicTableHelpers:
    def set_time_series_orm_uri_db_connection(self, uri: str):
        self.time_series_orm_uri_db_connection = uri

    def make_request(
            self,
            r_type: str,
            url: str,
            payload: Union[dict, None] = None,
            timeout: Union[float, None] = None
    ):
        r = make_request(s=self.s, r_type=r_type, url=url, payload=payload,
                         loaders=self.LOADERS, time_out=timeout)
        return r

    @property
    def s(self):
        from requests.adapters import HTTPAdapter, Retry
        s = requests.Session()
        s.headers.update(self.LOADERS.auth_headers)
        retries = Retry(total=2, backoff_factor=2, )
        s.mount('http://', HTTPAdapter(max_retries=retries))
        return s

    @staticmethod
    def _parse_parameters_filter(parameters):

        for key, value in parameters.items():
            if "__in" in key:
                assert isinstance(value, list)
                parameters[key] = ",".join(value)
        return parameters

    @staticmethod
    def request_to_datetime(string_date: str):
        return request_to_datetime(string_date)

    def get_orm_root_from_base_url(self, base_url):
        return base_url + "/orm/api"

    @property
    def root_url(self):
        return self.ROOT_URL + "/dynamic_table"

    @property
    def historical_update_url(self):
        return self.ROOT_URL + "/historical_update"

    @property
    def update_details_url(self):
        return self.ROOT_URL + "/update_details"

    @property
    def local_update_details_url(self):
        return self.ROOT_URL + "/local_update_details"

    @property
    def rest_token_auth_url(self):
        base = self.ROOT_URL.replace("/orm/api", "")
        return f"{base}/auth/rest-token-auth/"

    def patch_update_details(self, *args, **kwargs):
        base_url = self.update_details_url

        data = serialize_to_json(kwargs)
        payload = {"json": data}
        r = self.make_request(r_type="PATCH", url=f"{base_url}/0/", payload=payload)
        if r.status_code != 200:
            raise Exception(f"Error in request {r.text}")

    def patch_local_update_details(self, *args, **kwargs):
        base_url = self.local_update_details_url

        data = serialize_to_json(kwargs)
        payload = {"json": data}
        r = self.make_request(r_type="PATCH", url=f"{base_url}/0/", payload=payload)
        if r.status_code != 200:
            raise Exception(f"Error in request {r.text}")

    def destroy(self, metadata, delete_only_table: bool):
        base_url = self.root_url

        payload = {"json": {"delete_only_table": delete_only_table}, }
        r = self.s.delete(f"{base_url}/{metadata['id']}/", **payload)
        if r.status_code != 200:
            raise Exception(f"Error in request {r.text}")

    def get_all_hash_id(self):
        base_url = self.root_url
        r = self.s.get(f"{base_url}/get_all_hash_id", )
        if r.status_code != 200:
            raise Exception(f"Error in request {r.json()}")
        return r.json()

    def delete_all_data_after_date(self, after_date: str):
        base_url = self.root_url
        data = serialize_to_json({"after_date": after_date})
        payload = {"json": data, }
        r = self.s.patch(f"{base_url}/delete_all_data_after_date/", **payload)
        if r.status_code != 200:
            raise Exception(f"Error in request {r.json()}")

    def delete_after_date(self, metadata: Union[dict, None], after_date: str):

        base_url = self.root_url
        data = serialize_to_json({"after_date": after_date})
        payload = {"json": data, }
        r = self.s.patch(f"{base_url}/{metadata['id']}/delete_after_date/", **payload)
        if r.status_code != 200:
            raise Exception(f"Error in request {r.text}")

    def search(self, key_word: str):
        base_url = self.root_url
        url = f"{base_url}/?search={key_word}"
        # r = self.s.get(url )
        r = self.make_request(r_type="GET", url=url, )

        if r.status_code != 200:
            raise Exception(f"{base_url} Error in request {r.json}")
        else:
            serialized = r.json()

            return serialized, r

    def exist(self, *args, **kwargs):
        base_url = self.root_url
        payload = {"json": kwargs, }
        # r = self.s.patch(, **payload)
        r = self.make_request(r_type="PATCH", url=f"{base_url}/exist/", payload=payload)
        if r.status_code != 200:
            raise Exception(f"Error in request {r.json()}")
        return r.json(), r

    def filter(self, *args, **kwargs):
        instances, r = self.filter_rest(*args, **kwargs)
        return instances, r

    def filter_rest(self, *args, **kwargs):
        base_url = self.root_url
        params = self._parse_parameters_filter(parameters=kwargs)
        url = f"{base_url}/"
        payload = dict(params=params)
        # r = self.s.get(url, params=params)
        r = self.make_request(r_type="GET", url=url, payload=payload)
        if r.status_code == 404:
            raise DynamicTableDoesNotExist
        elif r.status_code != 200:
            raise Exception(f"Error in request {r.url} {r.text}")
        else:
            serialized = r.json()

            return serialized, r

    def get_rest(self, *args, **kwargs):
        instance, r = self.filter_rest(*args, **kwargs)

        if len(instance) > 1:
            raise Exception(f"Get does not return only one instance {r}")
        elif len(instance) == 0:
            return {}, r
        else:
            metadata = instance[0]
            return metadata, r

    def get(self, class_name=None, *args, **kwargs):

        instance, r = self.get_rest(*args, **kwargs)
        return instance

    def get_configuration(self, hash_id: str):
        data, _r = self.get(hash_id=hash_id)
        if len(data) == 0:
            return None, None
        build_configuration, build_meta_data = data["build_configuration"], data["build_meta_data"]

        return build_configuration, build_meta_data

    def create(self, metadata_kwargs: dict):
        metadata_kwargs = serialize_to_json(metadata_kwargs)
        time_serie_node, metadata = TimeSerieNode.create(metadata_kwargs=metadata_kwargs)
        return metadata

    def create_table_from_source_table_configuration(self, source_table_config_id: int, timeout=None):
        base_url = self.source_table_config_url

        r = self.s.post(f"{base_url}/{source_table_config_id}/create_table_from_source_table_configuration/")
        if r.status_code != 201:
            raise Exception(r.text)

    def get_update_statistics(self, hash_id):
        """
        Gets latest value from Hash_id
        """
        r, j = self.get_rest(hash_id=hash_id, class_name=None)
        if len(r) == 0:
            return None
        if r['sourcetableconfiguration'] is None:
            return None
        if r['sourcetableconfiguration']["last_time_index_value"] is None:
            return None

        date = self.request_to_datetime(string_date=r["sourcetableconfiguration"]["last_time_index_value"])

        return date

    @classmethod
    def _break_pandas_dataframe(cls, data_frame: pd.DataFrame, time_index_name: Union[str, None] = None):
        if time_index_name == None:
            time_index_name = data_frame.index.names[0]
            if time_index_name is None:
                time_index_name = "time_index"
                names = [c if i != 0 else time_index_name for i, c in
                         enumerate(data_frame.index.names)]
                data_frame.index.names = names

        time_col_loc = data_frame.index.names.index(time_index_name)
        column_index_names = data_frame.columns.names
        index_names = data_frame.index.names
        data_frame = data_frame.reset_index()
        data_frame.columns = [str(c) for c in data_frame.columns]
        data_frame = data_frame.rename(columns={data_frame.columns[time_col_loc]: time_index_name})
        column_dtypes_map = {key: str(value) for key, value in data_frame.dtypes.to_dict().items()}

        data_frame = data_frame.replace({np.nan: None})

        return data_frame, column_index_names, index_names, column_dtypes_map, time_index_name

    def filter_by_hash_id(self, hash_id_list: list):
        base_url = self.root_url
        url = f"{base_url}/filter_by_hash_id/"
        payload = {"json": {"hash_id__in": hash_id_list}, }
        r = self.make_request(r_type="POST", url=url, payload=payload)
        if r.status_code != 200:
            raise Exception(f"{r.text}")
        all_metadatas = {m["hash_id"]: m for m in r.json()}
        return all_metadatas

    @classmethod
    def _handle_source_table_configuration(cls,
                                           metadata: DynamicTableMetaData,
                                           column_dtypes_map,
                                           index_names,
                                           time_index_name,
                                           column_index_names,
                                           data,
                                           overwrite=False
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
        column_index_names : list
            List of column index names.
        data : DataFrame
            The input DataFrame.
        overwrite : bool, optional
            Whether to overwrite existing configurations (default is False).

        Returns:
        -------
        dict or None
            Updated metadata with the source table configuration, and potentially filtered data.
        """
        stc = metadata.sourcetableconfiguration

        if stc is None:
            try:
                stc = SourceTableConfiguration.create(
                    column_dtypes_map=column_dtypes_map,
                    index_names=index_names,
                    time_index_name=time_index_name,
                    column_index_names=column_index_names,
                    metadata_id=metadata.id
                )
                metadata.sourcetableconfiguration = stc
            except AlreadyExist:

                if not overwrite:
                    raise NotImplementedError("TODO Needs to remove values per asset")
                    # Filter the data based on time_index_name and last_time_index_value
                    data = data[
                        data[time_index_name] > self.request_to_datetime(stc.last_time_index_value)
                        ]
        return metadata, data

    @classmethod
    def upsert_data_into_table(cls,
                               local_metadata: dict,
                               data: pd.DataFrame,
                               data_source: DynamicTableDataSource,
                               ):
        overwrite = True  # ALWAYS OVERWRITE
        metadata = local_metadata.remote_table

        data, column_index_names, index_names, column_dtypes_map, time_index_name = cls._break_pandas_dataframe(
            data)

        # overwrite data origina data frame to release memory
        if not data[time_index_name].is_monotonic_increasing:
            data = data.sort_values(time_index_name)

        metadata, data = (
            result if (
                result := cls._handle_source_table_configuration(
                    metadata=metadata, column_dtypes_map=column_dtypes_map,
                    index_names=index_names,
                    time_index_name=time_index_name,
                    column_index_names=column_index_names, data=data,
                    overwrite=overwrite
                )
            ) is not None
            else (metadata, data)
        )

        duplicates_exist = data.duplicated(subset=index_names).any()
        if duplicates_exist:
            raise Exception(f"Duplicates found in columns: {index_names}")

        global_stats, grouped_dates, global_last_observation = get_chunk_stats(
            chunk_df=data,
            index_names=index_names,
            time_index_name=time_index_name
        )

        data_source.insert_data_into_table(
            serialized_data_frame=data,
            local_metadata=local_metadata,
            overwrite=overwrite,
            time_index_name=time_index_name,
            index_names=index_names,
            grouped_dates=grouped_dates,
        )



        min_d, last_time_index_value = global_stats["_GLOBAL_"]["min"], global_stats["_GLOBAL_"]["max"]
        max_per_asset_symbol = None
        if len(index_names) > 1:
            max_per_asset_symbol = {
                unique_identifier: stats["max"] for unique_identifier, stats in global_stats["_PER_ASSET_"].items()
            }
        local_metadata = local_metadata.set_last_update_index_time_from_update_stats(
            max_per_asset_symbol=max_per_asset_symbol,
            last_time_index_value=last_time_index_value,
            last_observation=global_last_observation
        )
        return local_metadata

    def filter_by_assets_ranges(
            self,
            metadata: dict,
            asset_ranges_map: dict,
            data_source: object,
            local_hash_id: str
    ):
        df = data_source.filter_by_assets_ranges(
            metadata=metadata,
            asset_ranges_map=asset_ranges_map,
            data_source=data_source,
            local_hash_id=local_hash_id,
        )
        return df

    def time_serie_exist_in_db(self, hash_id):
        metadata, _ = self.get(hash_id=hash_id)
        if len(metadata) == 0:
            return False
        else:
            if metadata["sourcetableconfiguration"] is not None:
                if metadata["sourcetableconfiguration"]["last_time_index_value"] is not None:
                    return True
        return False

    def set_compression_policy(self, metadata, interval: str):
        base_url = self.root_url

        payload = {"json": {"interval": interval, }}
        # r = self.s.patch(f"{base_url}/{metadata['id']}/set_compression_policy/", **payload)
        url = f"{base_url}/{metadata['id']}/set_compression_policy/"
        r = self.make_request(r_type="PATCH", url=url, payload=payload)
        if r.status_code != 200:
            raise Exception(f"{metadata['hash_id']} : {r.json()}")

    def set_retention_policy(self, metadata, interval: str):
        base_url = self.root_url

        payload = {"json": {"interval": interval, }}
        # r = self.s.patch(f"{base_url}/{metadata['id']}/set_retention_policy/", **payload)
        url = f"{base_url}/{metadata['id']}/set_retention_policy/"
        r = self.make_request(r_type="PATCH", url=url, payload=payload)
        if r.status_code != 200:
            raise Exception(f"{metadata['hash_id']} : {r.text}")

    def set_policy_for_descendants(self, hash_id, policy, pol_type, exclude_ids, extend_to_classes):
        r = TimeSerieNode.set_policy_for_descendants(hash_id, policy, pol_type, exclude_ids, extend_to_classes)

    def patch(self, metadata, timeout=None, *args, **kwargs):
        base_url = self.root_url

        payload = {"json": kwargs}
        # r = self.s.patch(f"{base_url}/{metadata['id']}/", **payload)
        url = f"{base_url}/{metadata['id']}/"
        r = self.make_request(r_type="PATCH", url=url, payload=payload, timeout=timeout)
        if r.status_code != 200:
            raise Exception(f"Error in request {r.text}")
        return r.json()

    def reset_dependencies_states(self, metadata, **kwargs):
        base_url = self.root_url

        payload = {"json": kwargs}
        # r = self.s.patch(, **payload)
        url = f"{base_url}/{metadata['id']}/reset_dependencies_states/"
        r = self.make_request(r_type="PATCH", url=url, payload=payload)
        if r.status_code != 200:
            raise Exception(f"Error in request ")

    def must_update_by_hash_id(self, hash_id: str):
        base_url = self.root_url

        payload = {"json": {"use_hash_id": True}}
        # r = self.s.patch(, **payload)
        url = f"{base_url}/{hash_id}/must_update/"
        r = self.make_request(r_type="PATCH", url=url, payload=payload)
        if r.status_code != 200:
            raise Exception(f"Error in request ")
        r = r.json()
        return r["must_update"], r["metadata"]

    def _build_table_response(self, data: pd.DataFrame, source_table_config: dict):
        infered_dtypes = {k: str(c) for k, c in data.dtypes.to_dict().items()}
        config_types = {c: source_table_config["column_dtypes_map"][c] for c in infered_dtypes.keys()}
        for c, c_type in config_types.items():
            if c_type != infered_dtypes[c]:
                if data.shape[0] > 0:
                    if c_type == 'datetime64[ns, UTC]':
                        if isinstance(data[c].iloc[0], str):
                            data[c] = pd.to_datetime(data[c])
                        else:
                            data[c] = pd.to_datetime(data[c] * 1e6, utc=True)
                    else:
                        data[c] = data[c].astype(c_type)
        data = data.set_index(source_table_config['index_names'])
        return data

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

    @staticmethod
    def _get_duck_db():
        host_uid = bios_uuid()
        data_source = DataSource.get_or_create_duck_db(
            display_name=f"DuckDB_{host_uid}",
            host_mac_address=host_uid
        )
        return data_source

    def set_local_db(self):
        data_source = self._get_duck_db()

        duckdb_dynamic_data_source = DynamicTableDataSource.get_or_create_duck_db(
            related_resource=data_source.id,
        )

        # drop local tables that are not in registered in the backend anymore (probably have been deleted)
        remote_tables = DynamicTableMetaData.filter(data_source__id=duckdb_dynamic_data_source.id, list_tables=True)
        remote_table_names = [t.table_name for t in remote_tables]
        from mainsequence.client.data_sources_interfaces.duckdb import DuckDBInterface
        db_interface=DuckDBInterface()
        local_table_names = db_interface.list_tables()

        tables_to_delete_locally = set(local_table_names) - set(remote_table_names)
        for table_name in tables_to_delete_locally:
            logger.debug(f"Deleting table in local duck db {table_name}")
            db_interface.drop_table(table_name)

        tables_to_delete_remotely = set(remote_table_names) - set(local_table_names)
        for remote_table in remote_tables:
            if remote_table.table_name in tables_to_delete_remotely:
                logger.debug(f"Deleting table remotely {remote_table.table_name}")
                if remote_table.protect_from_deletion:
                    remote_table.patch(protect_from_deletion=False)

                remote_table.delete()

        self.data_source = duckdb_dynamic_data_source

        physical_ds = self.data_source.related_resource
        banner = (
            f"\n{'-' * 80}\n"
            f"LOCAL data source SET: {physical_ds.display_name}  (engine={physical_ds.class_type})\n"
            f"\nTo launch the GUI, run these commands in your Python environment:\n"
            f"  import duckdb\n"
            f"  conn = duckdb.connect('{db_interface.db_path}')\n"
            f"  conn.execute(\"LOAD ui;\")         # Loads the UI extension\n"
            f"  conn.execute(\"CALL start_ui();\")  # Starts the server and opens the browser\n"
            f"{'-' * 80}\n"
        )
        logger.info(banner)

    def __repr__(self):
        return f"{self.data_source.related_resource}"

SessionDataSource = PodDataSource()
SessionDataSource.set_remote_db()
