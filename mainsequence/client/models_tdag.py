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
import warnings
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from threading import RLock
from typing import TYPE_CHECKING, Any, ClassVar, Generic, TypedDict, TypeVar
from uuid import UUID

import numpy as np
import pandas as pd
import pytz
import requests
import yaml
from cachetools import TTLCache, cachedmethod
from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator, model_validator

from mainsequence.logconf import logger

from . import exceptions
from .base import BaseObjectOrm, BasePydanticModel, LabelableObjectMixin, ShareableObjectMixin
from .data_sources_interfaces import get_duckdb_interface_class, get_sqlite_interface_class
from .dtype_codec import (
    TIMESTAMP_TZ,
    is_temporal_token,
    normalize_column_dtypes_map,
    normalize_dtype_token,
    pandas_dtypes_to_column_map,
    prepare_dataframe_for_remote_write,
    record_definitions_to_column_dtypes_map,
    token_to_pandas_dtype,
    token_to_pandas_series,
)
from .exceptions import raise_for_response
from .utils import (
    MAINSEQUENCE_ENDPOINT,
    TDAG_CONSTANTS,
    DataFrequency,
    DateInfo,
    DoesNotExist,
    UniqueIdentifierRangeMap,
    bios_uuid,
    get_network_ip,
    is_process_running,
    loaders,
    make_request,
    serialize_to_json,
    session,
)

_default_data_source = None  # Module-level cache

JSON_COMPRESSED_PREFIX = ["json_compressed", "jcomp_"]
LOGICAL_COLUMN_DTYPES_ATTR = "mainsequence_column_dtypes_map"


def _warn_legacy_compat(message: str, *, stacklevel: int = 3) -> None:
    warnings.warn(
        f"Deprecated TDAG compatibility path: {message}",
        FutureWarning,
        stacklevel=stacklevel,
    )



# Global executor (or you could define one on your class)
_executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)
DUCK_DB = "duck_db"
SQLITE = "sqlite"
LOCAL_DATA_SOURCE_CLASS_TYPES = {DUCK_DB, SQLITE}

_POD_PROJECT_RESOLUTION_LOCK = RLock()
_POD_PROJECT_RESOLUTION_CACHE = None
_POD_PROJECT_LOGGED_STATES: set[tuple[str, str]] = set()
POD_PROJECT = None

if TYPE_CHECKING:
    from mainsequence.tdag.data_nodes.filters import SearchRequest


class AlreadyExist(Exception):
    pass


def _duckdb_interface():
    return get_duckdb_interface_class()()


def _sqlite_interface():
    return get_sqlite_interface_class()()


def _local_data_interface(class_type: str):
    if class_type == DUCK_DB:
        return _duckdb_interface()
    if class_type == SQLITE:
        return _sqlite_interface()
    raise ValueError(f"Unsupported local data source class_type: {class_type!r}")




class SchedulerDoesNotExist(Exception):
    pass


class LocalTimeSeriesDoesNotExist(Exception):
    pass


class DynamicTableDoesNotExist(Exception):
    pass


class SourceTableConfigurationDoesNotExist(Exception):
    pass


class UpdateNodeRef(TypedDict):
    uid: str
    node_type: str
    update_hash: str
    remote_table_hash_id: str


def _require_public_uid(obj: Any, object_name: str) -> str:
    uid = getattr(obj, "uid", None)
    if uid in (None, ""):
        raise ValueError(f"{object_name} must have a uid before calling this endpoint.")
    return str(uid)


class BaseColumnMetaData(BasePydanticModel):
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

    @field_validator("dtype")
    @classmethod
    def _normalize_dtype(cls, value: str) -> str:
        return normalize_dtype_token(value, remote=False, allow_naive_datetime=True)


class ColumnMetaData(BaseColumnMetaData, BaseObjectOrm):
    source_config_id: int | None = Field(
        None,
        description="Primary key of the related SourceTableConfiguration",
    )


class SourceTableForeignKeyContract(BasePydanticModel):
    source_columns: list[str] = Field(
        default_factory=list,
        description="Ordered DynamicTable source columns",
    )
    target_meta_table_uid: str = Field(
        ...,
        description="Public uid of the target MetaTable",
    )
    target_columns: list[str] = Field(
        default_factory=list,
        description="Ordered target MetaTable columns",
    )
    on_delete: str = Field(
        default="restrict",
        description="Foreign-key delete action",
    )


class SourceTableForeignKeyProjection(SourceTableForeignKeyContract):
    name: str = Field(..., description="Server-derived foreign-key constraint name")
    target_meta_table_storage_hash: str | None = Field(
        None,
        description="Storage hash of the target MetaTable projection",
    )


def _serialize_source_table_foreign_key_contract(
    foreign_key: SourceTableForeignKeyContract | dict[str, Any],
) -> dict[str, Any]:
    if isinstance(foreign_key, BaseModel):
        raw = foreign_key.model_dump(mode="json", exclude_none=True)
    else:
        raw = dict(foreign_key)
    return {
        key: raw[key]
        for key in (
            "source_columns",
            "target_meta_table_uid",
            "target_columns",
            "on_delete",
        )
        if key in raw
    }


def _serialize_source_table_column_metadata(
    column_metadata: BaseColumnMetaData | dict[str, Any],
) -> dict[str, Any]:
    if isinstance(column_metadata, BaseModel):
        raw = column_metadata.model_dump(mode="json", exclude_none=True)
    else:
        raw = dict(column_metadata)
    raw.pop("orm_class", None)
    raw.pop("source_config_id", None)
    if "dtype" in raw:
        raw["dtype"] = normalize_dtype_token(raw["dtype"], remote=True)
    return raw


class SourceTableConfigurationBase:
    column_dtypes_map: dict[str, Any] = Field(..., description="Column data types map")
    index_names: list
    foreign_keys: list[SourceTableForeignKeyContract] = Field(default_factory=list)

    @field_validator("column_dtypes_map")
    @classmethod
    def _normalize_column_dtypes_map(cls, value: dict[str, Any]) -> dict[str, str]:
        return normalize_column_dtypes_map(value, remote=False, allow_naive_datetime=True)

    def get_data_updates(self) -> BaseUpdateStatistics:
        raise NotImplementedError

    def set_or_update_columns_metadata(
        self,
        columns_metadata: list[BaseColumnMetaData],
        timeout: int | float | tuple[float, float] | None = None,
    ) -> Any:
        raise NotImplementedError

class SourceTableConfiguration(SourceTableConfigurationBase, BasePydanticModel, BaseObjectOrm):
    related_table_uid: str | None = Field(
        None, description="Public uid of the related DataNodeStorage"
    )
    time_index_name: str = Field(..., max_length=100, description="Time index name")
    last_time_index_value: datetime.datetime | None = Field(
        None, description="Last time index value"
    )
    earliest_index_value: datetime.datetime | None = Field(None, description="Earliest index value")
    storage_layout: dict[str, Any] | None = Field(
        None, description="Server-derived logical storage layout"
    )
    physical_index_plan: dict[str, Any] | None = Field(
        None, description="Server-rendered physical index plan"
    )
    foreign_key_projections: list[SourceTableForeignKeyProjection] = Field(
        default_factory=list,
        description="Server-resolved DynamicTable-to-MetaTable FK projections",
    )
    multi_index_stats: dict[str, Any] | None = Field(
        None, description="Canonical multi-index progress statistics"
    )
    multi_index_column_stats: dict[str, Any] | None = Field(
        None, description="Column-level multi-index statistics"
    )
    open_for_everyone: bool = Field(
        default=False, description="Whether the table configuration is open for everyone"
    )
    columns_metadata: list[ColumnMetaData] | None = None

    # todo remove
    column_index_names: list | None = [None]

    def _related_table_uid(self) -> str:
        if self.related_table_uid in (None, ""):
            raise ValueError("SourceTableConfiguration must have related_table_uid before calling this endpoint.")
        return str(self.related_table_uid)

    def get_data_updates(self) -> UpdateStatistics:
        url = self.get_object_url() + f"/{self._related_table_uid()}/get_stats/"
        s = self.build_session()
        r = make_request(s=s, loaders=self.LOADERS, r_type="GET", url=url, accept_gzip=True)
        if r.status_code != 200:
            raise_for_response(r)
        data = r.json()
        multi_index_stats = data.get("multi_index_stats")
        multi_index_column_stats = data.get("multi_index_column_stats")
        max_time_index_value = self.last_time_index_value
        global_index_progress = None
        index_progress = None
        index_min = None

        if multi_index_stats is not None:
            global_index_progress = (
                multi_index_stats.get("_GLOBAL_")
                or multi_index_stats.get("global_index_progress")
            )
            global_index_progress = UpdateStatistics._normalize_nested(global_index_progress)
            index_progress = UpdateStatistics._normalize_nested(
                multi_index_stats.get("index_progress")
            )
            index_min = UpdateStatistics._normalize_nested(multi_index_stats.get("index_min"))

            if global_index_progress is not None:
                max_time_index_value = global_index_progress.get("max") or max_time_index_value

        du = UpdateStatistics(
            max_time_index_value=max_time_index_value,
            global_index_progress=global_index_progress,
            index_progress=index_progress,
            index_min=index_min,
            multi_index_column_stats=multi_index_column_stats,
        )

        du._max_time_in_update_statistics = max_time_index_value
        return du

    def get_time_scale_extra_table_indices(self) -> dict:
        url = self.get_object_url() + f"/{self._related_table_uid()}/get_time_scale_extra_table_indices/"
        s = self.build_session()
        r = make_request(
            s=s,
            loaders=self.LOADERS,
            r_type="GET",
            url=url,
        )
        if r.status_code != 200:
            raise_for_response(r)

        return r.json()

    def set_or_update_columns_metadata(
        self,
        columns_metadata: list[BaseColumnMetaData],
        timeout: int | float | tuple[float, float] | None = None,
    ) -> Any:
        """ """

        serialized_columns_metadata = []
        for column_metadata in columns_metadata:
            if isinstance(column_metadata, BaseModel):
                raw = column_metadata.model_dump(exclude={"orm_class"})
            else:
                raw = dict(column_metadata)
            if "dtype" in raw:
                raw["dtype"] = normalize_dtype_token(raw["dtype"], remote=True)
            serialized_columns_metadata.append(raw)
        url = self.get_object_url() + f"/{self._related_table_uid()}/set_or_update_columns_metadata/"
        s = self.build_session()
        r = make_request(
            s=s,
            loaders=self.LOADERS,
            r_type="POST",
            time_out=timeout,
            url=url,
            payload={"json": {"columns_metadata": serialized_columns_metadata}},
        )
        if r.status_code not in [200, 201]:
            raise_for_response(r)

        return r.json()

    def patch(self, *args, **kwargs):
        url = self.get_object_url() + f"/{self._related_table_uid()}/"
        payload = {"json": serialize_to_json(kwargs)}
        r = make_request(
            s=self.build_session(),
            loaders=self.LOADERS,
            r_type="PATCH",
            url=url,
            payload=payload,
        )
        raise_for_response(r)
        return self.__class__(**r.json())





class TableUpdateNode(BasePydanticModel):
    model_config = ConfigDict(extra="allow")

    uid: str | None = Field(None, description="Public uid of this update node")
    update_hash: str = Field(..., max_length=63, description="Max length of PostgreSQL table name")
    build_configuration: dict[str, Any] = Field(..., description="Configuration in JSON format")
    ogm_dependencies_linked: bool = Field(default=False, description="OGM dependencies linked flag")
    downstream_direct_dependencies: list[TableUpdateNode] | None = Field(
        None,
        description="Optional serialized downstream direct dependency payloads.",
    )
    all_dependencies_update_priority: list[dict[str, Any]] | None = Field(
        None,
        description="Optional serialized dependency priority payloads.",
    )

    def _public_uid(self) -> str:
        return _require_public_uid(self, self.__class__.__name__)


class DataNodeUpdate(TableUpdateNode, BaseObjectOrm):
    model_config = ConfigDict(extra="forbid")
    FILTERSET_FIELDS: ClassVar[dict[str, list[str]]] = {
        "uid": ["in", "exact"],
        "update_hash": ["exact"],
        "remote_table__data_source__uid": ["exact", "in"],
        "related_table__namespace": ["contains", "in", "isnull"],
    }
    FILTER_VALUE_NORMALIZERS: ClassVar[dict[str, str]] = {
        "uid": "uid",
        "uid__in": "uid",
        "remote_table__data_source__uid": "uid",
        "remote_table__data_source__uid__in": "uid",
    }
    READ_QUERY_PARAMS: ClassVar[dict[str, str]] = {
        "include_relations_detail": "bool",
    }
    READ_QUERY_PARAM_DESCRIPTIONS: ClassVar[dict[str, str]] = {
        "include_relations_detail": (
            "Expand related objects in the serializer response. "
            "This changes response detail only and does not change which rows are returned."
        ),
    }

    NODE_TYPE: ClassVar[str] = "local_time_serie"

    data_node_storage: str | DataNodeStorage
    tags: list[str] | None = Field(default=[], description="List of tags")
    labels: list[str] = Field(
        default_factory=list,
        description=(
            "Organizational labels  "
            "These are helpers for grouping and discovery only and do not change runtime behavior or functionality."
        ),
    )
    description: str | None = Field(None, description="Optional HTML description")
    update_details: DataNodeUpdateDetails | None = None
    run_configuration: RunConfiguration | None = None
    open_for_everyone: bool = Field(
        default=False, description="Whether the ts is open for everyone"
    )

    @property
    def data_source_uid(self):
        if isinstance(self.data_node_storage, str):
            return None
        data_source = self.data_node_storage.data_source
        if isinstance(data_source, str):
            return data_source
        if isinstance(data_source, dict):
            return data_source.get("uid")
        return getattr(data_source, "uid", None)

    @classmethod
    def get_or_create(cls, **kwargs):
        url = cls.get_object_url() + "/get_or_create/"
        kwargs = serialize_to_json(kwargs)
        pod_project = _require_local_pod_project("DataNodeUpdate.get_or_create")
        project_uid = str(getattr(pod_project, "uid", "") or "").strip()
        if not project_uid:
            raise RuntimeError(
                "DataNodeUpdate.get_or_create requires a local pod project uid, "
                "but the resolved project does not expose one."
            )
        kwargs["current_project_uid"] = project_uid
        payload = {"json": kwargs}
        s = cls.build_session()
        r = make_request(s=s, loaders=cls.LOADERS, r_type="POST", url=url, payload=payload)
        if r.status_code not in [200, 201]:
            raise_for_response(r)

        data = r.json()

        return cls(**data)

    def add_tags(self, tags: list, timeout=None):
        base_url = self.get_object_url()
        s = self.build_session()
        payload = {"json": {"tags": tags}}
        # r = self.s.get(, )
        url = f"{base_url}/{self._public_uid()}/add_tags/"
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
        url = f"{base_url}/{self._public_uid()}/set_start_of_execution/"
        r = make_request(
            s=s, loaders=self.LOADERS, r_type="PATCH", url=url, payload=payload, accept_gzip=True
        )
        if r.status_code != 201:
            raise Exception(f"Error in request {r.text}")

        result = r.json()
        multi_index_stats = result.get("multi_index_stats") or {}
        global_index_progress = (
            result.get("global_index_progress")
            or multi_index_stats.get("_GLOBAL_")
            or multi_index_stats.get("global_index_progress")
        )
        index_progress = result.get("index_progress") or multi_index_stats.get("index_progress")
        index_min = result.get("index_min") or multi_index_stats.get("index_min")

        hu = LocalTimeSeriesHistoricalUpdate(
            **result["historical_update"],
            update_statistics=UpdateStatistics(
                global_index_progress=global_index_progress,
                index_progress=index_progress,
                index_min=index_min,
                multi_index_column_stats=result.get("multi_index_column_stats"),
            ),
            must_update=result["must_update"],
            direct_dependency_uids=result.get("direct_dependency_uids"),
        )
        return hu

    def set_end_of_execution(
        self, historical_update_uid: str, timeout=None, threaded_request=True, **kwargs
    ):
        s = self.build_session()
        url = self.get_object_url() + f"/{self._public_uid()}/set_end_of_execution/"
        if historical_update_uid in (None, ""):
            raise ValueError("Historical update uid is required to end execution.")
        kwargs.update(dict(historical_update_uid=str(historical_update_uid)))
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
        storage_uid = data_node_storage["uid"]
        url = cls.get_object_url() + f"/{storage_uid}/set_last_update_index_time/"
        r = make_request(s=s, loaders=cls.LOADERS, r_type="GET", url=url, time_out=timeout)

        if r.status_code == 404:
            raise SourceTableConfigurationDoesNotExist

        if r.status_code != 200:
            raise Exception(f"{data_node_storage['update_hash']}{r.text}")
        return r

    def set_last_update_index_time_from_update_stats(
        self,
        *,
        global_index_progress: dict[str, Any] | None = None,
        index_progress: dict[str, Any] | None = None,
        index_min: dict[str, Any] | None = None,
        multi_index_stats: dict[str, Any] | None = None,
        multi_index_column_stats: dict[str, Any] | None = None,
        timeout=None,
    ) -> DataNodeUpdate:
        s = self.build_session()
        url = self.get_object_url() + f"/{self._public_uid()}/set_last_update_index_time_from_update_stats/"

        data_to_comp = build_last_update_index_time_payload(
            global_index_progress=global_index_progress,
            index_progress=index_progress,
            index_min=index_min,
            multi_index_stats=multi_index_stats,
            multi_index_column_stats=multi_index_column_stats,
        )
        chunk_json_str = json.dumps(serialize_to_json(data_to_comp))
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
        base_url = cls.END_POINTS["LocalTimeSerieHistoricalUpdate"]
        data = serialize_to_json(kwargs)
        payload = {
            "json": data,
        }
        r = make_request(
            s=s, loaders=cls.LOADERS, r_type="POST", url=f"{base_url}/", payload=payload
        )
        if r.status_code != 201:
            raise Exception(f"Error in request {r.url} {r.text}")



    def get_all_dependencies_update_priority(self, timeout=None) -> pd.DataFrame:
        s = self.build_session()
        url = self.get_object_url() + f"/{self._public_uid()}/get_all_dependencies_update_priority/"
        r = make_request(s=s, loaders=self.LOADERS, r_type="GET", url=url, time_out=timeout)
        if r.status_code != 200:
            raise Exception(f"Error in request {r.text}")

        depth_df = pd.DataFrame(r.json())
        if not depth_df.empty:
            uid_candidates = [
                c
                for c in ["update_node_uid", "local_time_serie_uid", "data_node_update_uid"]
                if c in depth_df.columns
            ]

            if uid_candidates:
                update_node_uid = None
                for col in uid_candidates:
                    series = depth_df[col]
                    if isinstance(series, pd.DataFrame):
                        series = series.iloc[:, 0]
                    update_node_uid = (
                        series if update_node_uid is None else update_node_uid.fillna(series)
                    )

                depth_df = depth_df.drop(
                    columns=["update_node_uid", "local_time_serie_uid", "data_node_update_uid"],
                    errors="ignore",
                )
                depth_df["update_node_uid"] = update_node_uid



        return depth_df

    @classmethod
    def get_upstream_nodes(cls, storage_hash, data_source_uid, timeout=None):
        s = cls.build_session()
        url = (
            cls.get_object_url("DataNode")
            + f"/{storage_hash}/get_upstream_nodes?data_source_uid={data_source_uid}"
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
        url = self.get_object_url() + f"/{self._public_uid()}/verify_if_direct_dependencies_are_updated/"
        r = make_request(s=s, loaders=None, r_type="GET", url=url)
        if r.status_code != 200:
            raise Exception(f"Error in request: {r.text}")
        return r.json()

    def get_data_between_dates_from_api(self, *args, **kwargs):

        return self.data_node_storage.get_data_between_dates_from_api(*args, **kwargs)

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
        column_dtypes_map: Mapping[str, Any] | None = None,
    ):
        """
        Sends a large DataFrame to a Django backend in multiple chunks.
        If a chunk is too large (HTTP 413), it's automatically split in half and retried.
        """
        s = cls.build_session()
        update_uid = _require_public_uid(data_node_update, "DataNodeUpdate")
        url = cls.get_object_url() + f"/{update_uid}/insert_data_into_table/"

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
            chunk_stats, _ = get_index_progress_chunk_stats(
                chunk_df=df_chunk, index_names=index_names, time_index_name=time_index_name
            )
            if column_dtypes_map is not None:
                df_chunk = prepare_dataframe_for_remote_write(
                    df_chunk,
                    column_dtypes_map=column_dtypes_map,
                    time_index_name=time_index_name,
                )
            chunk_json_str = df_chunk.to_json(orient="records", date_format="iso")
            compressed = gzip.compress(chunk_json_str.encode("utf-8"))
            compressed_b64 = base64.b64encode(compressed).decode("utf-8")

            # For sub-chunks, we treat it as a new, single-chunk upload.
            payload = dict(
                json={
                    "data": compressed_b64,
                    "chunk_stats": serialize_to_json(chunk_stats),
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
                raise_for_response(r, )


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
        cls,
        update_nodes: Sequence[UpdateNodeRef],
        update_details_kwargs: Mapping[str, Any],
        update_priority_dict: Mapping[str, int] | None,
    ):
        """
        {'local_hash_id__in': [{'update_hash': 'alpacaequitybarstest_97018e7280c1bad321b3f4153cc7e986', 'data_source_uid': '...'},
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
                update_nodes=list(update_nodes),
                update_details_kwargs=update_details_kwargs,
                update_priority_dict=update_priority_dict,
            )
        }
        # r = self.s.post(f"{base_url}/get_metadatas_and_set_updates/", **payload)
        url = f"{base_url}/get_metadatas_and_set_updates/"
        r = make_request(s=s, loaders=cls.LOADERS, r_type="POST", url=url, payload=payload)
        if r.status_code != 200:
            raise Exception(f"Error in request {r.text}")
        response_json = r.json()
        source_table_config_map = {
            str(k): SourceTableConfiguration(**v) if v is not None else v
            for k, v in response_json["source_table_config_map"].items()
        }
        state_data = {
            str(k): DataNodeUpdateDetails(**v) for k, v in response_json["state_data"].items()
        }
        all_index_stats = {str(k): v for k, v in response_json["all_index_stats"].items()}
        data_node_updates = [DataNodeUpdate(**v) for v in response_json["local_metadatas"]]
        return UpdateBatchResponse[
            DataNodeUpdate,
            DataNodeUpdateDetails,
            SourceTableConfiguration,
        ](
            source_table_config_map=source_table_config_map,
            state_data=state_data,
            all_index_stats=all_index_stats,
            data_node_updates=data_node_updates,
        )

    def depends_on_connect(self, target_update_node_uid):

        url = self.get_object_url() + f"/{self._public_uid()}/depends_on_connect/"
        s = self.build_session()
        payload = dict(
            json={
                "target_update_node_uid": str(target_update_node_uid),
            }
        )
        r = make_request(s=s, loaders=self.LOADERS, r_type="PATCH", url=url, payload=payload)
        if r.status_code != 204:
            raise Exception(f"Error in request {r.text}")

    def depends_on_connect_to_api_table(self, target_table_uid, timeout=None):

        url = self.get_object_url() + f"/{self._public_uid()}/depends_on_connect_to_api_table/"
        s = self.build_session()
        payload = dict(
            json={
                "target_table_uid": str(target_table_uid),
            }
        )
        r = make_request(
            s=s, loaders=self.LOADERS, r_type="PATCH", url=url, time_out=timeout, payload=payload
        )
        if r.status_code != 204:
            raise Exception(f"Error in request {r.text}")

    @classmethod
    def _break_pandas_dataframe(
        cls,
        data_frame: pd.DataFrame,
        time_index_name: str | None = None,
        records: Sequence[Any] | None = None,
        *,
        remote_dtypes: bool = True,
        allow_naive_datetime: bool = False,
    ):
        logical_column_dtypes_map = data_frame.attrs.get(LOGICAL_COLUMN_DTYPES_ATTR)
        record_column_dtypes_map = record_definitions_to_column_dtypes_map(
            records,
            remote=remote_dtypes,
            allow_naive_datetime=allow_naive_datetime,
        )
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
        column_dtypes_map = pandas_dtypes_to_column_map(
            data_frame.dtypes.to_dict(),
            remote=remote_dtypes,
            allow_naive_datetime=allow_naive_datetime,
        )
        missing_record_columns = [
            column_name
            for column_name in record_column_dtypes_map
            if column_name not in data_frame.columns
        ]
        if missing_record_columns:
            raise ValueError(
                "DataNode records declare columns not present in the DataFrame: "
                f"{missing_record_columns}"
            )
        if logical_column_dtypes_map is not None:
            logical_column_dtypes_map = normalize_column_dtypes_map(
                logical_column_dtypes_map,
                remote=remote_dtypes,
                allow_naive_datetime=allow_naive_datetime,
            )
            missing_logical_columns = [
                column_name
                for column_name in logical_column_dtypes_map
                if column_name not in data_frame.columns
            ]
            if missing_logical_columns:
                raise ValueError(
                    "Logical column dtype contract contains columns not present "
                    f"in the DataFrame: {missing_logical_columns}"
                )
            conflicting_declared_dtypes = {
                column_name: {
                    "record_dtype": record_column_dtypes_map[column_name],
                    "logical_dtype": logical_column_dtypes_map[column_name],
                }
                for column_name in logical_column_dtypes_map
                if (
                    column_name in record_column_dtypes_map
                    and logical_column_dtypes_map[column_name]
                    != record_column_dtypes_map[column_name]
                )
            }
            if conflicting_declared_dtypes:
                raise ValueError(
                    "Logical column dtype contract conflicts with DataNode records: "
                    f"{conflicting_declared_dtypes}"
                )
            column_dtypes_map.update(logical_column_dtypes_map)
        column_dtypes_map.update(record_column_dtypes_map)

        data_frame = data_frame.replace({np.nan: None})

        return data_frame, index_names, column_dtypes_map, time_index_name

    def upsert_data_into_table(
        self,
        data: pd.DataFrame,
        data_source: DynamicTableDataSource,
        overwrite: bool,
        columns_metadata: list[BaseColumnMetaData | dict[str, Any]] | None = None,
        foreign_keys: list[SourceTableForeignKeyContract | dict[str, Any]] | None = None,
        records: Sequence[Any] | None = None,
        source_table_schema: Mapping[str, Any] | None = None,
    ):

        overwrite = True  # ALWAYS OVERWRITE
        metadata = self.data_node_storage
        storage_class_type = getattr(getattr(data_source, "related_resource", None), "class_type", None)
        is_local_storage = storage_class_type in LOCAL_DATA_SOURCE_CLASS_TYPES

        schema_time_index_name = (
            str(source_table_schema["time_index_name"])
            if source_table_schema and source_table_schema.get("time_index_name") is not None
            else None
        )
        data, index_names, column_dtypes_map, time_index_name = self._break_pandas_dataframe(
            data,
            time_index_name=schema_time_index_name,
            records=records,
            remote_dtypes=not is_local_storage,
            allow_naive_datetime=is_local_storage,
        )
        inferred_index_names = list(index_names)
        if source_table_schema:
            schema_index_names = source_table_schema.get("index_names")
            if schema_index_names is not None:
                index_names = [str(name) for name in schema_index_names]
                if index_names != inferred_index_names:
                    raise ValueError(
                        "DataFrame index names do not match declared source table "
                        f"index_names. DataFrame: {inferred_index_names}; "
                        f"declared: {index_names}"
                    )
            schema_column_dtypes_map = source_table_schema.get("column_dtypes_map")
            if schema_column_dtypes_map is not None:
                column_dtypes_map = normalize_column_dtypes_map(
                    schema_column_dtypes_map,
                    remote=not is_local_storage,
                    allow_naive_datetime=is_local_storage,
                )
            if columns_metadata is None:
                columns_metadata = source_table_schema.get("columns_metadata")
        index_names = list(index_names)
        missing_index_dtypes = [name for name in index_names if name not in column_dtypes_map]
        if missing_index_dtypes:
            raise ValueError(
                "Every index column must exist in column_dtypes_map. "
                f"Missing: {missing_index_dtypes}"
            )

        # overwrite data origina data frame to release memory
        if not data[time_index_name].is_monotonic_increasing:
            data = data.sort_values(time_index_name)

        metadata.handle_source_table_configuration_creation(
            column_dtypes_map=column_dtypes_map,
            index_names=index_names,
            time_index_name=time_index_name,
            data=data,
            overwrite=overwrite,
            columns_metadata=columns_metadata,
            foreign_keys=foreign_keys,
        )

        duplicates_exist = data.duplicated(subset=index_names).any()
        if duplicates_exist:
            raise Exception(f"Duplicates found in columns: {index_names}")

        index_stats, grouped_dates = get_index_progress_chunk_stats(
            chunk_df=data, index_names=index_names, time_index_name=time_index_name
        )
        index_min_max_stats = combine_index_min_max_stats(
            index_min=index_stats["index_min"],
            index_progress=index_stats["index_progress"],
        )
        multi_index_column_stats = {}
        column_names = [c for c in data.columns if c not in index_names]
        for c in column_names:
            multi_index_column_stats[c] = index_min_max_stats
        data_source.related_resource.insert_data_into_table(
            serialized_data_frame=data,
            data_node_update=self,
            overwrite=overwrite,
            time_index_name=time_index_name,
            index_names=index_names,
            grouped_dates=grouped_dates,
            column_dtypes_map=column_dtypes_map,
        )

        data_node_update = self.set_last_update_index_time_from_update_stats(
            global_index_progress=index_stats["_GLOBAL_"],
            index_progress=index_stats["index_progress"],
            index_min=index_stats["index_min"],
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

        if self.update_details.error_on_last_update or self.update_details.last_update is None:
            return None

        time_to_wait, next_update = self.get_node_time_to_wait()
        if time_to_wait > 0:

            logger.info(f"Scheduler Waiting for ts update time at {next_update} {time_to_wait}")
            time.sleep(time_to_wait)
        else:
            time_to_wait = max(0, 60 - datetime.datetime.now(pytz.utc).second)
            logger.info("Scheduler Waiting for ts update at start of minute")
            time.sleep(time_to_wait)


class BaseUpdateDetails:
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
    active_update_scheduler_uid: str | None = Field(
        None, description="UID reference to the scheduler for active update"
    )
    update_priority: int = Field(default=0, description="Priority level of the update")
    last_updated_by_user_uid: str | None = Field(
        None, description="UID reference to the user that last updated this record"
    )


class DataNodeUpdateDetails(BaseUpdateDetails,BasePydanticModel, BaseObjectOrm):
    related_table_uid: str | None = Field(None, description="Public uid of the related DataNodeUpdate")
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


class AbstractTable:
    uid: str | None = Field(None, description="Public uid of this table storage")
    storage_hash: str = Field(..., max_length=63, description="Max length of PostgreSQL table name")
    namespace: str | None = Field(
        None,
        description="Optional hash namespace used to isolate table identity for tests or experiments.",
    )
    build_configuration_json_schema: dict[str, Any] | None = Field(
        None,
        description="JSON schema describing the build configuration",
    )
    identifier: str | None = None
    protect_from_deletion: bool = Field(
        default=False,
        description="Flag to protect the record from deletion",
    )
    description: str | None = None

    def _public_uid(self) -> str:
        return _require_public_uid(self, self.__class__.__name__)


class DataNodeStorage(AbstractTable, LabelableObjectMixin, ShareableObjectMixin, BasePydanticModel, BaseObjectOrm):
    FILTERSET_FIELDS: ClassVar[dict[str, list[str]]] = {
        "storage_hash": ["in", "exact", "contains"],
        "identifier": ["in", "exact", "contains"],
        "uid": ["in", "exact"],
        "data_source__uid": ["in", "exact"],
        "namespace": ["exact", "contains", "in", "isnull"],
        "labels": ["exact", "in", "contains"],
    }
    FILTER_VALUE_NORMALIZERS: ClassVar[dict[str, str]] = {
        "uid": "uid",
        "uid__in": "uid",
        "data_source__uid": "uid",
        "data_source__uid__in": "uid",
        "labels": "str",
        "labels__in": "str",
        "labels__contains": "str",
    }
    READ_QUERY_PARAMS: ClassVar[dict[str, str]] = {
        "include_relations_detail": "bool",
    }
    READ_QUERY_PARAM_DESCRIPTIONS: ClassVar[dict[str, str]] = {
        "include_relations_detail": (
            "Expand related objects in the serializer response. "
            "This changes response detail only and does not change which rows are returned."
        ),
    }
    DESTROY_QUERY_PARAMS: ClassVar[dict[str, str]] = {
        "full_delete_selected": "bool",
        "full_delete_downstream_tables": "bool",
        "delete_with_no_table": "bool",
        "override_protection": "bool",
    }
    DESTROY_QUERY_PARAM_DESCRIPTIONS: ClassVar[dict[str, str]] = {
        "full_delete_selected": "Fully delete the selected DataNode instance.",
        "full_delete_downstream_tables": (
            "Delete downstream tables/dependencies starting from the selected metadata instance."
        ),
        "delete_with_no_table": (
            "Scan all DataNode rows and fully delete records whose backing DB table does not exist."
        ),
        "override_protection": (
            "Bypass protect_from_deletion. ORG_ADMIN only. Used with full_delete_selected=true."
        ),
    }

    labels: list[str] = Field(
        default_factory=list,
        description=(
            "Organizational labels attached to the data node. "
            "These are helpers for grouping and discovery only and do not change runtime behavior or functionality."
        ),
    )
    creation_date: datetime.datetime = Field(..., description="Creation timestamp")
    created_by_user_uid: str | None = Field(None, description="UID reference to User")
    organization_owner_uid: str | None = Field(None, description="UID reference to Organization")
    open_for_everyone: bool = Field(
        default=False, description="Whether the table is open for everyone"
    )
    data_source_open_for_everyone: bool = Field(
        default=False, description="Whether the data source is open for everyone"
    )
    build_configuration: dict[str, Any] | None = Field(
        None, description="Configuration in JSON format"
    )
    time_serie_source_code_git_hash: str | None = Field(
        None, max_length=255, description="Git hash of the time series source code"
    )
    time_serie_source_code: str | None = Field(
        None, description="File path for time series source code"
    )
    data_source: int | DynamicTableDataSource
    source_class_name: str
    sourcetableconfiguration: SourceTableConfiguration | None = None
    table_index_names: dict | None = None

    # TS specifi
    compression_policy_config: dict | None = None
    retention_policy_config: dict | None = None

    # MetaData
    data_frequency_id: DataFrequency | None = None

    _drop_indices: bool = False  # for direct incertion we can pass this values
    _rebuild_indices: bool = False  # for direct incertion we can pass this values

    @staticmethod
    def _date_for_payload(value: Any) -> Any:
        if isinstance(value, datetime.datetime):
            return int(value.timestamp())
        return value

    @classmethod
    def _normalize_dimension_range_map(
        cls,
        dimension_range_map: list[dict[str, Any]] | None,
    ) -> list[dict[str, Any]] | None:
        if dimension_range_map is None:
            return None

        normalized = copy.deepcopy(dimension_range_map)
        for descriptor in normalized:
            for key in ("start_date", "end_date"):
                if key in descriptor:
                    descriptor[key] = cls._date_for_payload(descriptor[key])
        return normalized

    def _build_dimension_payload(
        self,
        *,
        dimension_filters: dict[str, list[Any]] | None = None,
        index_coordinates: list[dict[str, Any]] | None = None,
        dimension_range_map: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        if dimension_filters is not None:
            payload["dimension_filters"] = dimension_filters
        if index_coordinates is not None:
            payload["index_coordinates"] = index_coordinates
        if dimension_range_map is not None:
            payload["dimension_range_map"] = self._normalize_dimension_range_map(dimension_range_map)
        return payload

    def patch(
        self,
        time_out: None | int = None,
        *args,
        **kwargs,
    ):
        url = self.get_object_url() + f"/{self._public_uid()}/"
        payload = {"json": serialize_to_json(kwargs)}
        s = self.build_session()
        r = make_request(
            s=s, loaders=self.LOADERS, r_type="PATCH", url=url, payload=payload, time_out=time_out
        )
        if r.status_code != 200:
            data = r.json()  # guaranteed JSON from your backend

            error=data.get("error") or data.get("detail")
            if r.status_code == 409:
                raise exceptions.ConflictError(error)
            raise exceptions.ApiError(error)
        return self.__class__(**r.json())

    @classmethod
    def patch_by_hash(cls, storage_hash: str, *args, **kwargs):
        metadata = cls.get(storage_hash=storage_hash)
        metadata.patch(*args, **kwargs)

    @classmethod
    def destroy_by_uid(
        cls,
        uid: str,
        *,
        full_delete_selected: bool = False,
        full_delete_downstream_tables: bool = False,
        delete_with_no_table: bool = False,
        override_protection: bool = False,
        timeout: int | None = None,
    ):
        """Delete a DataNodeStorage row using its public uid."""
        if uid in (None, ""):
            raise ValueError("DataNodeStorage uid is required for deletion.")
        payload = {
            "params": {
                "full_delete_selected": full_delete_selected,
                "full_delete_downstream_tables": full_delete_downstream_tables,
                "delete_with_no_table": delete_with_no_table,
                "override_protection": override_protection,
            }
        }
        r = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="DELETE",
            url=f"{cls.get_object_url()}/{uid}/",
            payload=payload,
            time_out=timeout,
        )
        raise_for_response(r)
        return r.json() if r.content else None

    def delete(
        self,
        *,
        full_delete_selected: bool = False,
        full_delete_downstream_tables: bool = False,
        delete_with_no_table: bool = False,
        override_protection: bool = False,
        timeout: int | None = None,
    ):
        """
        Instance wrapper for `destroy_by_uid()` with the same delete query parameters.
        """
        return type(self).destroy_by_uid(
            self._public_uid(),
            timeout=timeout,
            full_delete_selected=full_delete_selected,
            full_delete_downstream_tables=full_delete_downstream_tables,
            delete_with_no_table=delete_with_no_table,
            override_protection=override_protection,
        )

    @classmethod
    def get_or_create(cls, **kwargs):
        kwargs = serialize_to_json(kwargs)
        url = cls.get_object_url() + "/get_or_create/"
        payload = {"json": kwargs}
        s = cls.build_session()
        r = make_request(s=s, loaders=cls.LOADERS, r_type="POST", url=url, payload=payload)
        if r.status_code not in [201, 200]:
            raise_for_response(r, payload=payload)

        data = r.json()
        return cls(**data)

    def build_or_update_update_details(self, *args, **kwargs):
        base_url = self.get_object_url()
        payload = {"json": kwargs}
        s = self.build_session()
        url = f"{base_url}/{self._public_uid()}/build_or_update_update_details/"
        r = make_request(
            r_type="PATCH",
            url=url,
            payload=payload,
            s=s,
            loaders=self.LOADERS,
        )
        if r.status_code != 202:
            raise Exception(f"Error in request {r.text}")

    def refresh_table_search_index(
        self,
        *,
        timeout: int | None = None,
    ) -> dict[str, Any] | None:
        """
        Refresh the semantic search index for this data node storage.

        The backend joins the table's column definitions with the code used to
        generate the data node, builds a consolidated textual description, and
        embeds that description into a vector representation for smart search.

        This hits:
            POST /{uid}/refresh-table-search-index/

        Parameters
        ----------
        timeout:
            Optional request timeout in seconds.
        """
        if self.uid is None:
            raise ValueError("DataNodeStorage must have a uid before refreshing the table search index.")

        cls = type(self)
        url = f"{cls.get_object_url()}/{self._public_uid()}/refresh-table-search-index/"
        r = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="POST",
            url=url,
            payload={},
            time_out=timeout,
        )
        raise_for_response(r)

        return r.json() if r.content else None

    def initialize_source_table(
        self,
        *,
        time_index_name: str,
        index_names: list[str],
        column_dtypes_map: dict[str, Any],
        storage_layout: dict[str, Any] | None = None,
        foreign_keys: list[SourceTableForeignKeyContract | dict[str, Any]] | None = None,
        open_for_everyone: bool | None = None,
        timeout: int | None = None,
    ) -> dict[str, Any]:
        """
        Initialize this DynamicTableMetaData source table from schema only.

        This calls:
            POST /orm/api/ts_manager/dynamic_table/{uid}/initialize-source-table/

        It creates or validates the SourceTableConfiguration and creates the
        physical backing table without inserting a bootstrap row. Optional
        column metadata and foreign keys are resolved by the caller and sent as
        source-table schema metadata.
        """
        cls = type(self)
        url = f"{cls.get_object_url()}/{self._public_uid()}/initialize-source-table/"
        return self._initialize_source_table_at_url(
            url=url,
            time_index_name=time_index_name,
            index_names=index_names,
            column_dtypes_map=column_dtypes_map,
            storage_layout=storage_layout,
            foreign_keys=foreign_keys,
            open_for_everyone=open_for_everyone,
            timeout=timeout,
        )

    def _initialize_source_table_at_url(
        self,
        *,
        url: str,
        time_index_name: str,
        index_names: list[str],
        column_dtypes_map: dict[str, Any],
        storage_layout: dict[str, Any] | None = None,
        foreign_keys: list[SourceTableForeignKeyContract | dict[str, Any]] | None = None,
        open_for_everyone: bool | None = None,
        timeout: int | None = None,
    ) -> dict[str, Any]:
        if self.uid is None:
            raise ValueError("DataNodeStorage must have a uid before initializing a source table.")
        storage_class_type = getattr(
            getattr(getattr(self, "data_source", None), "related_resource", None),
            "class_type",
            None,
        )
        is_local_storage = storage_class_type in LOCAL_DATA_SOURCE_CLASS_TYPES
        column_dtypes_map = normalize_column_dtypes_map(
            column_dtypes_map,
            remote=not is_local_storage,
            allow_naive_datetime=is_local_storage,
        )

        payload_body: dict[str, Any] = {
            "time_index_name": time_index_name,
            "index_names": list(index_names),
            "column_dtypes_map": dict(column_dtypes_map),
        }
        if storage_layout is not None:
            payload_body["storage_layout"] = storage_layout
        if foreign_keys is not None:
            payload_body["foreign_keys"] = [
                _serialize_source_table_foreign_key_contract(foreign_key)
                for foreign_key in foreign_keys
            ]
        if open_for_everyone is not None:
            payload_body["open_for_everyone"] = open_for_everyone

        cls = type(self)
        response = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="POST",
            url=url,
            payload={"json": serialize_to_json(payload_body)},
            time_out=timeout,
        )
        raise_for_response(response, payload=payload_body)
        data = response.json()
        source_config_data = data.get("source_table_configuration")
        if isinstance(source_config_data, dict):
            self.sourcetableconfiguration = SourceTableConfiguration(**source_config_data)
        return data

    def run_query(
        self,
        sql: str,
        *,
        timeout: int | None = None,
    ) -> dict[str, Any]:
        """
        Execute a raw SQL query against this dynamic table.

        This hits:
            POST /orm/api/ts_manager/dynamic_table/{uid}/run_query/

        Request contract:
        - body is the raw SQL string as `text/plain`
        - do not send JSON like `{"sql": "..."}`

        Response contract:
        - returns the backend query envelope with `ok`, `results`, `row_count`,
          `truncated`, and error fields
        - when the backend returns a structured query envelope with `ok=false`,
          that envelope is returned directly so callers can inspect the backend
          error payload
        """
        if self.uid is None:
            raise ValueError("DataNodeStorage must have a uid before running a query.")

        sql = str(sql or "").strip()
        if not sql:
            raise ValueError("sql is required.")

        cls = type(self)
        url = f"{cls.get_object_url()}/{self._public_uid()}/run_query/"
        session = cls.build_session()
        old_content_type = session.headers.get("Content-Type")
        session.headers["Content-Type"] = "text/plain"
        try:
            response = make_request(
                s=session,
                loaders=cls.LOADERS,
                r_type="POST",
                url=url,
                payload={"data": sql},
                time_out=timeout,
            )
        finally:
            if old_content_type is None:
                session.headers.pop("Content-Type", None)
            else:
                session.headers["Content-Type"] = old_content_type

        try:
            data = response.json()
        except Exception:
            data = None

        if isinstance(data, dict) and "ok" in data:
            return data

        raise_for_response(response, payload={"data": sql})
        return response.json()

    def delete_after_date(
        self,
        after_date: str | datetime.datetime,
        *,
        dimension_filters: dict[str, list[Any]] | None = None,
        index_coordinates: list[dict[str, Any]] | None = None,
        dimension_range_map: list[dict[str, Any]] | None = None,
        timeout: int | None = None,
    ) -> dict[str, Any]:
        """
        Delete rows at or after a cutoff timestamp from this dynamic table.

        This is a backend tail-delete operation:

        - it hits `POST /orm/api/ts_manager/dynamic_table/{uid}/delete_after_date/`
        - `after_date` is the inclusive cutoff
        - there is no `end_date`; this is not arbitrary range deletion
        - for multi-index tables, pass `dimension_filters` or
          `index_coordinates` to scope the tail delete

        The authenticated user must have edit access to this DynamicTableMetaData.

        The returned payload contains the authoritative post-delete table stats,
        including `deleted_count`, `table_empty`, and index metadata. Consumers
        can use those stats to update visible table metadata or refetch the table
        detail after the delete.
        """
        if self.uid is None:
            raise ValueError("DataNodeStorage must have a uid before deleting rows after a date.")

        payload_body: dict[str, Any] = {
            "after_date": after_date.isoformat() if isinstance(after_date, datetime.datetime) else after_date
        }
        payload_body.update(
            self._build_dimension_payload(
                dimension_filters=dimension_filters,
                index_coordinates=index_coordinates,
                dimension_range_map=dimension_range_map,
            )
        )

        cls = type(self)
        url = f"{cls.get_object_url()}/{self._public_uid()}/delete_after_date/"
        r = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="POST",
            url=url,
            payload={"json": payload_body},
            time_out=timeout,
        )
        raise_for_response(r, payload=payload_body)
        return r.json()

    def _uses_session_duckdb_data_source(self) -> bool:
        return self._uses_session_local_data_source()

    def _uses_session_local_data_source(self) -> bool:
        if not isinstance(self.data_source, int):
            related_resource = getattr(self.data_source, "related_resource", None)
            return getattr(related_resource, "class_type", None) in LOCAL_DATA_SOURCE_CLASS_TYPES

        session_dynamic_data_source = getattr(SessionDataSource, "data_source", None)
        related_resource = getattr(session_dynamic_data_source, "related_resource", None)
        return (
            getattr(session_dynamic_data_source, "id", None) == self.data_source
            and getattr(related_resource, "class_type", None) in LOCAL_DATA_SOURCE_CLASS_TYPES
        )

    def delete_table(self):
        if self._uses_session_local_data_source():
            class_type = None
            if not isinstance(self.data_source, int):
                related_resource = getattr(self.data_source, "related_resource", None)
                class_type = getattr(related_resource, "class_type", None)
            else:
                session_dynamic_data_source = getattr(SessionDataSource, "data_source", None)
                related_resource = getattr(session_dynamic_data_source, "related_resource", None)
                class_type = getattr(related_resource, "class_type", None)
            db_interface = _local_data_interface(class_type)
            db_interface.drop_table(self.storage_hash)

        self.delete()

    def handle_source_table_configuration_creation(
        self,
        column_dtypes_map: dict,
        index_names: list[str],
        time_index_name,
        data,
        overwrite=False,
        columns_metadata: list[BaseColumnMetaData | dict[str, Any]] | None = None,
        foreign_keys: list[SourceTableForeignKeyContract | dict[str, Any]] | None = None,
    ):
        """
        Handles the creation or retrieval of the source table configuration.

        Parameters:
        ----------
        metadata : dict
            Metadata dictionary containing "sourcetableconfiguration" and "uid".
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
        storage_class_type = getattr(
            getattr(getattr(self, "data_source", None), "related_resource", None),
            "class_type",
            None,
        )
        is_local_storage = storage_class_type in LOCAL_DATA_SOURCE_CLASS_TYPES
        column_dtypes_map = normalize_column_dtypes_map(
            column_dtypes_map,
            remote=not is_local_storage,
            allow_naive_datetime=is_local_storage,
        )

        if stc is None or foreign_keys is not None:
            try:
                response_data = self.initialize_source_table(
                    column_dtypes_map=column_dtypes_map,
                    index_names=index_names,
                    time_index_name=time_index_name,
                    foreign_keys=foreign_keys,
                    open_for_everyone=self.open_for_everyone,
                )
                stc_data = response_data.get("source_table_configuration")
                if isinstance(stc_data, dict):
                    stc = SourceTableConfiguration(**stc_data)
                    self.sourcetableconfiguration = stc
            except AlreadyExist as err:
                if not overwrite:
                    # Feature not implemented yet → make the causal link explicit
                    raise NotImplementedError(
                        "Removing values per asset when overwrite=False is not implemented yet."
                    ) from err
                    # Filter the data based on time_index_name and last_time_index_value

        stc = self.sourcetableconfiguration
        if columns_metadata is not None:
            if stc is None:
                raise ValueError(
                    "Cannot update columns metadata before SourceTableConfiguration exists."
                )
            stc.set_or_update_columns_metadata(columns_metadata=columns_metadata)


    @staticmethod
    def map_columns_to_df(df,
                          column_dtypes_map:dict,time_index_name:str,
                          index_names:list[str],
                          )->pd.DataFrame:
        columns_to_loop = column_dtypes_map.keys()
        for c, c_type in column_dtypes_map.items():
            if c not in columns_to_loop:
                continue
            if c in df.columns:
                df[c] = token_to_pandas_series(
                    df[c],
                    c_type,
                    is_time_index=c == time_index_name,
                )
        df = df.set_index(index_names)
        return df

    def get_last_observation(
        self,
        *,
        dimension_filters: dict[str, list[Any]] | None = None,
        index_coordinates: list[dict[str, Any]] | None = None,
        dimension_range_map: list[dict[str, Any]] | None = None,
        timeout=None,
    ):
        base_url = self.get_object_url()
        payload = {
            "json": self._build_dimension_payload(
                dimension_filters=dimension_filters,
                index_coordinates=index_coordinates,
                dimension_range_map=dimension_range_map,
            )
        }
        s = self.build_session()
        url = f"{base_url}/{self._public_uid()}/get_last_observation/"
        r = make_request(
            r_type="POST",
            url=url,
            payload=payload,
            s=s,
            loaders=self.LOADERS,
            time_out=timeout,
        )
        if r.status_code != 200:
            raise Exception(f"Error in request {r.text}")
        df=pd.DataFrame(r.json())
        if df.empty:
            return df
        stc = self.sourcetableconfiguration
        try:
            df[stc.time_index_name] = pd.to_datetime(df[stc.time_index_name], format="ISO8601")
        except Exception as e:
            raise e

        df=self.map_columns_to_df(df=df,column_dtypes_map=stc.column_dtypes_map,
                                  time_index_name=stc.time_index_name,
                                  index_names=stc.index_names,
                                  )


        return df

    @classmethod
    def _get_data_between_dates_common(
            cls,
            url: str,
            start_date: datetime.datetime = None,
            end_date: datetime.datetime = None,
            great_or_equal: bool = None,
            less_or_equal: bool = None,
            dimension_filters: dict[str, list[Any]] | None = None,
            index_coordinates: list[dict[str, Any]] | None = None,
            dimension_range_map: list[dict[str, Any]] | None = None,
            columns: list = None,
            column_range_descriptor: None | UniqueIdentifierRangeMap = None,
            node_identifier: str | None = None,
    ) -> pd.DataFrame:
        """Internal shared implementation for fetching data between dates."""
        return_storage_node=False
        if "get_data_between_dates_from_node_identifier" in url:
            return_storage_node=True

        def fetch_one_batch(chunk_dimension_range_map):
            all_results_chunk = []
            offset = 0

            while True:
                payload_json = {
                    "start_date": start_date.timestamp() if start_date else None,
                    "end_date": end_date.timestamp() if end_date else None,
                    "great_or_equal": great_or_equal,
                    "less_or_equal": less_or_equal,
                    "columns": columns,
                    "offset": offset,  # pagination offset
                    # "column_range_descriptor": column_range_descriptor,  # if/when needed
                }
                if dimension_filters is not None:
                    payload_json["dimension_filters"] = dimension_filters
                if index_coordinates is not None:
                    payload_json["index_coordinates"] = index_coordinates
                if chunk_dimension_range_map is not None:
                    payload_json["dimension_range_map"] = chunk_dimension_range_map

                if node_identifier is not None:
                    payload_json["node_identifier"] = node_identifier

                payload = {"json": payload_json}

                # Perform the POST request
                r = make_request(
                    s=s,
                    loaders=cls.LOADERS,
                    payload=payload,
                    r_type="POST",
                    url=url,
                )
                if r.status_code != 200:
                    logger.warning(f"Error in request: {r.text}")
                    return [] ,None

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

            return all_results_chunk,response_data

        s = cls.build_session()

        all_results = []
        response_data = None
        if dimension_range_map:
            chunk_size = 100
            for start_idx in range(0, len(dimension_range_map), chunk_size):
                chunk_range_map = dimension_range_map[start_idx: start_idx + chunk_size]

                # Fetch data (including any pagination via next_offset)
                chunk_results,response_data = fetch_one_batch(chunk_range_map)
                all_results.extend(chunk_results)
        else:
            # If dimension_range_map is None, do a single batch with offset-based pagination.
            chunk_results,response_data = fetch_one_batch(None)
            all_results.extend(chunk_results)
        if not return_storage_node:
            return pd.DataFrame(all_results)
        else:
            storage_node=cls(**response_data['storage_node']) if response_data is not None else None
            return pd.DataFrame(all_results),storage_node

    def get_data_between_dates_from_api(
            self,
            start_date: datetime.datetime = None,
            end_date: datetime.datetime = None,
            great_or_equal: bool = None,
            less_or_equal: bool = None,
            dimension_filters: dict[str, list[Any]] | None = None,
            index_coordinates: list[dict[str, Any]] | None = None,
            dimension_range_map: list[dict[str, Any]] | None = None,
            columns: list = None,
            column_range_descriptor: None | UniqueIdentifierRangeMap = None,
    ):
        """Public helper for /{uid}/get_data_between_dates_from_remote/."""
        url = self.get_object_url() + f"/{self._public_uid()}/get_data_between_dates_from_remote/"
        dimension_payload = self._build_dimension_payload(
            dimension_filters=dimension_filters,
            index_coordinates=index_coordinates,
            dimension_range_map=dimension_range_map,
        )

        return self._get_data_between_dates_common(
            url=url,
            start_date=start_date,
            end_date=end_date,
            great_or_equal=great_or_equal,
            less_or_equal=less_or_equal,
            dimension_filters=dimension_payload.get("dimension_filters"),
            index_coordinates=dimension_payload.get("index_coordinates"),
            dimension_range_map=dimension_payload.get("dimension_range_map"),
            columns=columns,
            column_range_descriptor=column_range_descriptor,
            node_identifier=None,
        )

    @classmethod
    def get_data_between_dates_from_node_identifier(
            cls,
            node_identifier: str,
            start_date: datetime.datetime = None,
            end_date: datetime.datetime = None,
            great_or_equal: bool = None,
            less_or_equal: bool = None,
            dimension_filters: dict[str, list[Any]] | None = None,
            index_coordinates: list[dict[str, Any]] | None = None,
            dimension_range_map: list[dict[str, Any]] | None = None,
            columns: list = None,
            column_range_descriptor: None | UniqueIdentifierRangeMap = None,
    )->[pd.DataFrame,DataNodeStorage]:
        """
        Same behaviour as get_data_between_dates_from_api,
        but calls the node-identifier endpoint and includes node_identifier in payload.
        """
        url = cls.get_object_url() + "/get_data_between_dates_from_node_identifier/"
        dimension_range_map = cls._normalize_dimension_range_map(dimension_range_map)

        return cls._get_data_between_dates_common(
            url=url,
            start_date=start_date,
            end_date=end_date,
            great_or_equal=great_or_equal,
            less_or_equal=less_or_equal,
            dimension_filters=dimension_filters,
            index_coordinates=index_coordinates,
            dimension_range_map=dimension_range_map,
            columns=columns,
            column_range_descriptor=column_range_descriptor,
            node_identifier=node_identifier,
        )

    @staticmethod
    def _normalize_dtype_for_pandas(dtype_str: str) -> str:
        """
        Convert your stc.column_dtypes_map types into pandas dtypes that can hold NULLs.
        """
        return token_to_pandas_dtype(dtype_str)

    @staticmethod
    def _get_search_meta_attr(obj: Any, attr: str, default: Any = None) -> Any:
        if isinstance(obj, Mapping):
            return obj.get(attr, default)
        return getattr(obj, attr, default)

    @staticmethod
    def _coerce_search_index_names(value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, str):
            return [value]
        if isinstance(value, Mapping):
            try:
                items = sorted(value.items(), key=lambda item: int(item[0]))
            except (TypeError, ValueError):
                items = value.items()
            value = [item_value for _, item_value in items]
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
            return [str(item) for item in value if item is not None and str(item).strip()]
        return []

    @classmethod
    def _iter_search_source_configs(cls, data_node_storage_map: dict):
        for meta in (data_node_storage_map or {}).values():
            stc = cls._get_search_meta_attr(meta, "sourcetableconfiguration")
            if stc is None:
                stc = cls._get_search_meta_attr(meta, "source_table_configuration")
            if stc is not None:
                yield stc

    @classmethod
    def _resolve_search_response_index_names(
        cls,
        *,
        data_node_storage_map: dict,
        filter_request: SearchRequest,
        response_index_names: Any = None,
    ) -> list[str]:
        explicit_index_names = cls._coerce_search_index_names(response_index_names)
        if explicit_index_names:
            return explicit_index_names

        join_vectors = [
            cls._coerce_search_index_names(getattr(join, "on", None))
            for join in (getattr(filter_request, "joins", None) or [])
        ]
        join_vectors = [join_on for join_on in join_vectors if join_on]
        if join_vectors:
            first_join_vector = join_vectors[0]
            if all(join_on == first_join_vector for join_on in join_vectors):
                return first_join_vector
            return []

        for stc in cls._iter_search_source_configs(data_node_storage_map):
            index_names = cls._coerce_search_index_names(
                cls._get_search_meta_attr(stc, "index_names")
            )
            if index_names:
                return index_names
        return []

    @classmethod
    def _search_response_column_dtype(
        cls,
        *,
        data_node_storage_map: dict,
        column_name: str,
    ) -> str | None:
        for stc in cls._iter_search_source_configs(data_node_storage_map):
            dtype_map = cls._get_search_meta_attr(stc, "column_dtypes_map", {}) or {}
            if column_name in dtype_map:
                return str(dtype_map[column_name])
        return None

    @classmethod
    def _apply_dtypes_from_meta(
            cls,
            df: pd.DataFrame,
            *,
            data_node_storage_map: dict,
            filter_request: SearchRequest,
            response_index_names: Any = None,
    ) -> pd.DataFrame:
        """
        df columns expected:
          - unprefixed join/index key columns returned by the server
          - base__<col>
          - <join_alias>__<col>
        """
        if df.empty:
            return df

        index_names = cls._resolve_search_response_index_names(
            data_node_storage_map=data_node_storage_map,
            filter_request=filter_request,
            response_index_names=response_index_names,
        )
        time_index_names = {"time_index"}
        for stc in cls._iter_search_source_configs(data_node_storage_map):
            time_index_name = cls._get_search_meta_attr(stc, "time_index_name")
            if time_index_name:
                time_index_names.add(str(time_index_name))

        # 1) Parse unprefixed join/index keys. These exist even in FULL OUTER JOIN.
        key_columns_to_cast = list(dict.fromkeys([*index_names, "time_index", "unique_identifier"]))
        for key_col in key_columns_to_cast:
            if key_col not in df.columns:
                continue
            dtype_str = cls._search_response_column_dtype(
                data_node_storage_map=data_node_storage_map,
                column_name=key_col,
            )
            if key_col in time_index_names:
                df[key_col] = token_to_pandas_series(
                    df[key_col],
                    TIMESTAMP_TZ,
                    is_time_index=True,
                )
                continue
            if dtype_str is not None and is_temporal_token(dtype_str):
                df[key_col] = token_to_pandas_series(df[key_col], dtype_str)
                continue
            try:
                df[key_col] = token_to_pandas_series(df[key_col], dtype_str or "string")
            except Exception:
                pass

        # 2) Cast prefixed columns using each table's SourceTableConfiguration
        for prefix, meta in (data_node_storage_map or {}).items():
            stc = cls._get_search_meta_attr(meta, "sourcetableconfiguration")
            if stc is None:
                stc = cls._get_search_meta_attr(meta, "source_table_configuration")
            if stc is None:
                continue

            stc_index_names = set(
                cls._coerce_search_index_names(cls._get_search_meta_attr(stc, "index_names"))
            )
            response_index_name_set = set(index_names)
            dtype_map = cls._get_search_meta_attr(stc, "column_dtypes_map", {}) or {}

            for col_name, col_type in dtype_map.items():
                # The server returns join/index keys unprefixed; do not look for
                # prefixed copies of any configured index dimension.
                if col_name in stc_index_names or col_name in response_index_name_set:
                    continue

                df_col = f"{prefix}__{col_name}"
                if df_col not in df.columns:
                    continue

                try:
                    df[df_col] = token_to_pandas_series(df[df_col], col_type)
                except Exception:
                    # last resort: leave as object (do not crash on one bad cast)
                    pass

        # 3) Restore the server-declared index vector.
        if index_names and set(index_names).issubset(df.columns):
            df = df.set_index(index_names)

        return df

    @classmethod
    def get_data_from_filter(
            cls,
            filter_request: SearchRequest,
            *,
            batch_limit: int = 14000,
    ) -> pd.DataFrame:
        url = cls.get_object_url() + "/get-data-from-filter/"
        s = cls.build_session()

        offset = int(filter_request.offset or 0)

        all_results: list[dict] = []
        data_node_storage_map_json: dict | None = None
        response_index_names: Any = None

        while True:
            req = filter_request.model_copy(deep=True)
            req.limit = int(batch_limit)
            req.offset = int(offset)

            payload_json = req.model_dump(mode="json", exclude_none=True)
            payload = {"json": payload_json}

            r = make_request(
                s=s,
                loaders=cls.LOADERS,
                payload=payload,
                r_type="POST",
                url=url,
            )

            if r.status_code != 200:
                logger.warning(f"Error in request: {r.text}")
                return pd.DataFrame([])

            response_data = r.json() or {}

            # capture meta map once (same on every page)
            if data_node_storage_map_json is None:
                data_node_storage_map_json = response_data.get("data_node_storage_map") or {}
            if response_index_names is None:
                for key in ("index_names", "join_keys", "join_index_names", "index_columns"):
                    if key in response_data and response_data[key] is not None:
                        response_index_names = response_data[key]
                        break

            chunk = response_data.get("results", []) or []
            all_results.extend(chunk)

            next_offset = response_data.get("next_offset")
            if not next_offset:
                break
            offset = int(next_offset)

        # IMPORTANT: dtype=object prevents pandas from converting big ints to float when NULLs exist
        df = pd.DataFrame(all_results, dtype=object)

        # Build DataNodeStorage objects from serializer payloads
        storage_objs = {}
        for prefix, meta_json in (data_node_storage_map_json or {}).items():
            try:
                storage_objs[prefix] = cls(**meta_json)
            except Exception:
                # if instantiation fails, keep the raw dict so the method still works
                storage_objs[prefix] = meta_json

        # If instantiation failed and we only have dicts, dtype parsing would require dict access.
        # Assuming cls(**meta_json) works in your client models (it should).
        df = cls._apply_dtypes_from_meta(
            df,
            data_node_storage_map=storage_objs,
            filter_request=filter_request,
            response_index_names=response_index_names,
        )

        return df

    @classmethod
    def _deserialize_search_response(cls, data: Any):
        """
        Supports both:
        - paginated DRF responses: {"count": ..., "next": ..., "previous": ..., "results": [...]}
        - non-paginated list responses: [...]
        """
        if isinstance(data, dict) and isinstance(data.get("results"), list):
            hydrated = dict(data)
            hydrated["results"] = [cls(**item) for item in hydrated["results"]]
            return hydrated

        if isinstance(data, list):
            return [cls(**item) for item in data]

        if isinstance(data, dict):
            return cls(**data)

        return data

    @classmethod
    def description_search(
            cls,
            q: str,
            *,
            q_embedding: Sequence[float] | None = None,
            trigram_k: int = 200,
            embed_k: int = 200,
            w_trgm: float = 0.65,
            w_emb: float = 0.35,
            embedding_model: str = "default",
            **filters,
    ):
        """
        Hits:
            POST <object_url>/description-search/

        Server behavior:
        - if q_embedding is omitted, the server generates it from q
        - returns paginated or non-paginated serialized DynamicTableMetaData rows
        """
        q = (q or "").strip()
        if not q:
            raise ValueError("q is required")

        url = cls.get_object_url() + "/description-search/"
        body = {
            "q": q,
            "trigram_k": trigram_k,
            "embed_k": embed_k,
            "w_trgm": w_trgm,
            "w_emb": w_emb,
            "embedding_model": embedding_model,
        }

        if q_embedding is not None:
            body["q_embedding"] = [float(x) for x in q_embedding]

        if filters:
            body.update(filters)

        body = serialize_to_json(body)
        payload = {"json": body}

        s = cls.build_session()
        r = make_request(
            s=s,
            loaders=cls.LOADERS,
            r_type="POST",
            url=url,
            payload=payload,
        )
        if r.status_code != 200:
            raise_for_response(r, payload=payload)

        return cls._deserialize_search_response(r.json())

    @classmethod
    def column_search(cls, q: str, **filters):
        """
        Hits:
            GET <object_url>/column-search/?q=...

        Extra kwargs are passed through as query params so your DRF filters still work
        (e.g. storage_hash=..., identifier=..., data_source__uid=..., page=...).
        """
        q = (q or "").strip()
        if not q:
            raise ValueError("q is required")

        url = cls.get_object_url() + "/column-search/"
        params = {"q": q, **filters}
        params = serialize_to_json(params)
        payload = {"params": params}

        s = cls.build_session()
        r = make_request(
            s=s,
            loaders=cls.LOADERS,
            r_type="GET",
            url=url,
            payload=payload,
        )
        if r.status_code != 200:
            raise_for_response(r, payload=payload)

        return cls._deserialize_search_response(r.json())



class Scheduler(BasePydanticModel, BaseObjectOrm):
    uid: str | None = Field(None, description="Public uid of this scheduler")
    name: str
    is_running: bool
    running_process_pid: int | None
    running_in_debug_mode: bool
    updates_halted: bool
    host: str | None
    api_address: str | None
    api_port: int | None
    last_heart_beat: datetime.datetime | None = None
    pre_loads_in_tree: list[TableUpdateNode] | None = None
    in_active_tree: list[TableUpdateNode] | None = None
    schedules_to: list[TableUpdateNode] | None = None
    # for heartbeat
    _stop_heart_beat: bool = False
    _executor: object | None = None

    def _public_uid(self) -> str:
        return _require_public_uid(self, "Scheduler")

    @classmethod
    def get_scheduler_for_update_node(cls, update_node_uid: str):
        """GET the scheduler assigned to a DataNodeUpdate uid."""
        if update_node_uid in (None, ""):
            raise ValueError("update_node_uid is required.")
        s = cls.build_session()
        url = cls.get_object_url() + "/for-update-node/"
        r = make_request(
            s=s,
            r_type="GET",
            url=url,
            payload={"params": {"update_node_uid": str(update_node_uid)}},
            loaders=cls.LOADERS,
        )
        if r.status_code == 404:
            raise SchedulerDoesNotExist(r.json().get("detail", r.text))
        r.raise_for_status()
        return cls(**r.json())


    @classmethod
    def build_and_assign_to_update_nodes(
        cls,
        scheduler_name: str,
        update_node_uids: list[str],
        delink_all_ts: bool = False,
        remove_from_other_schedulers: bool = True,
        timeout=None,
        **kwargs,
    ):
        """
        POST /schedulers/build_and_assign_to_update_nodes/
        body: {
          scheduler_name, update_node_uids, delink_all_ts?,
          remove_from_other_schedulers?, scheduler_kwargs?
        }
        """
        s = cls.build_session()
        url = cls.get_object_url() + "/build_and_assign_to_update_nodes/"
        request_body = {
            "scheduler_name": scheduler_name,
            "delink_all_update_nodes": delink_all_ts,
            "remove_from_other_schedulers": remove_from_other_schedulers,
            "scheduler_kwargs": kwargs or {},
        }
        request_body["update_node_uids"] = [str(uid) for uid in update_node_uids]
        payload = {"json": request_body}
        r = make_request(
            s=s, r_type="POST", url=url, payload=payload, time_out=timeout, loaders=cls.LOADERS
        )
        if r.status_code not in [200, 201]:
            r.raise_for_status()
        return cls(**r.json())

    def in_active_tree_connect(self, update_node_uids: list[str]):
        """
        PATCH /schedulers/{uid}/in-active-tree/
        body: { update_node_uids }
        """
        s = self.build_session()
        url = f"{self.get_object_url()}/{self._public_uid()}/in-active-tree/"
        r = make_request(
            s=s,
            r_type="PATCH",
            url=url,
            payload={"json": {"update_node_uids": [str(uid) for uid in update_node_uids]}},
            loaders=self.LOADERS,
        )
        if r.status_code not in (200, 204):
            raise Exception(f"Error in request {r.text}")

    def assign_to_scheduler(self, update_node_uids: list[str]):
        """
        PATCH /schedulers/{uid}/assign/
        body: { update_node_uids }
        """
        s = self.build_session()
        url = f"{self.get_object_url()}/{self._public_uid()}/assign/"
        r = make_request(
            s=s,
            r_type="PATCH",
            url=url,
            payload={"json": {"update_node_uids": [str(uid) for uid in update_node_uids]}},
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
    update_schedule: str = "*/1 * * * *"
    local_time_serie_update_details: int | None = None

    @classmethod
    @property
    def ROOT_URL(cls):
        return None


class BaseUpdateStatistics(BaseModel):
    """
    Backend-agnostic update-state envelope shared by updater types.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)


class UpdateStatistics(BaseUpdateStatistics):
    """
    Time-series-specific update statistics used by DataNode updaters.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    global_index_progress: dict[str, datetime.datetime | None] | None = None
    index_progress: dict[str, Any] | None = None
    index_min: dict[str, Any] | None = None

    max_time_index_value: datetime.datetime | None = None  # does not include filter applicable for 1d index
    limit_update_time: datetime.datetime | None = None  # flag to limit the update of data node

    _max_time_in_update_statistics: datetime.datetime | None = None  # include filter
    _initial_fallback_date: datetime.datetime | None = None


    # when working with DuckDb and column based storage we want to have also stats by  column
    multi_index_column_stats: dict[str, Any] | None = None
    is_backfill: bool = False

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
        if isinstance(obj, list):
            return [cls._normalize_nested(v) for v in obj]
        if isinstance(obj, tuple):
            return tuple(cls._normalize_nested(v) for v in obj)
        return cls._to_utc_datetime(obj)

    @field_validator(
        "global_index_progress",
        "index_progress",
        "index_min",
        "multi_index_column_stats",
        mode="before",
    )
    @classmethod
    def _coerce_nested_timestamp_fields(cls, v):
        # Normalize before standard parsing so ints/strings become datetimes
        return cls._normalize_nested(v)

    @field_validator("max_time_index_value", mode="before")
    @classmethod
    def _coerce_max_time_index_value(cls, v):
        return cls._to_utc_datetime(v)

    def model_post_init(self, __context: Any) -> None:
        if self.global_index_progress is not None:
            global_max = self.global_index_progress.get("max")
            if global_max is not None:
                self.max_time_index_value = global_max

        if self._max_time_in_update_statistics is None:
            self._max_time_in_update_statistics = self.max_time_index_value

    def _progress_stats(self) -> dict[str, Any]:
        return self.index_progress or {}

    def _set_progress_stats(self, stats: dict[str, Any] | None) -> None:
        normalized = self._normalize_nested(stats)
        self.index_progress = normalized

    @classmethod
    def return_empty(cls):
        return cls()

    def pretty_print(self):
        print(f"{self.__class__.__name__} summary:")

        # DataFrame
        if self.last_observation is None or self.last_observation.empty:
            print("  last_observation: empty DataFrame")
        else:
            rows, cols = self.last_observation.shape
            print(f"  last_observation: DataFrame with {rows} rows × {cols} columns")

        # Other attributes
        print(f"  max_time_index_value: {self.max_time_index_value}")
        print(f"  _max_time_in_update_statistics: {self._max_time_in_update_statistics}")



    def identity_values(self):
        return list(self._progress_stats().keys())

    def get_max_time_in_update_statistics(self):
        if not hasattr(self, "_max_time_in_update_statistics") :
            self._max_time_in_update_statistics = (
                self.max_time_index_value or self._initial_fallback_date
            )
        if self._max_time_in_update_statistics is None and self.index_progress is not None:
            values = self.get_index_progress_leaf_values()
            self._max_time_in_update_statistics = max(values) if values else None

        return self._max_time_in_update_statistics

    @property
    def is_any_identity_on_fallback_date(self)->bool:
        """Return true if any index progress leaf equals _initial_fallback_date."""
        return any(
            value == self._initial_fallback_date
            for value in self.get_index_progress_leaf_values()
        )

    @property
    def are_all_identities_on_fallback_date(self)->bool:
        """Return true if all index progress leaves equal _initial_fallback_date."""
        return all(
            value == self._initial_fallback_date
            for value in self.get_index_progress_leaf_values()
        )


    def get_columnar_identity_range_map_great_or_equal(
        self,
        extra_time_delta: datetime.timedelta | None = None,
        column_filter: list[str] | None = None,
    ):
        multi_index_column_stats = self.multi_index_column_stats or {}
        if column_filter is None:
            column_filter = list(multi_index_column_stats.keys())
        column_stats_by_column = {
            column_name: copy.deepcopy(multi_index_column_stats.get(column_name, {}))
            for column_name in column_filter
        }

        def _start_dt(bounds):
            dt = (
                (bounds or {}).get("max")
                if isinstance(bounds, dict)
                else bounds
            ) or self._initial_fallback_date
            if extra_time_delta:
                dt = dt + extra_time_delta
            return dt


        range_map = {
            col: {
                identity_value: DateInfo(
                    {
                        "start_date_operand": ">=",
                        "start_date": _start_dt(bounds),
                    }
                )
                for identity_value, bounds in col_stats.items()
            }
            for col, col_stats in column_stats_by_column.items()
            if col in column_filter
        }

        return range_map

    def get_identity_range_map_great_or_equal(
        self,
        extra_time_delta: datetime.timedelta | None = None,
    ):

        if extra_time_delta is None:
            range_map = {
                k: DateInfo(
                    {"start_date_operand": ">=", "start_date": v or self._initial_fallback_date}
                )
                for k, v in self._progress_stats().items()
            }
        else:
            range_map = {
                k: DateInfo(
                    {
                        "start_date_operand": ">=",
                        "start_date": (v or self._initial_fallback_date) + extra_time_delta,
                    }
                )
                for k, v in self._progress_stats().items()
            }
        return range_map

    def iter_index_progress_coordinates(
        self,
        *,
        identity_dimensions: Sequence[str],
    ) -> list[tuple[dict[str, Any], Any]]:
        """
        Return `(coordinate, timestamp)` leaves from canonical `index_progress`.

        `identity_dimensions` must match `index_names[1:]` for multidimensional
        stats. Callers must pass it explicitly; UpdateStatistics does not infer
        identity dimension names.
        """
        progress_stats = self._progress_stats()
        if not progress_stats:
            return []

        dimensions = list(identity_dimensions)
        coordinates: list[tuple[dict[str, Any], Any]] = []

        def _visit(node: Any, depth: int, coordinate: dict[str, Any]) -> None:
            if isinstance(node, dict):
                if depth >= len(dimensions):
                    raise ValueError(
                        "identity_dimensions must include a name for every "
                        "nested index_progress level."
                    )
                dimension_name = dimensions[depth]
                for key, value in node.items():
                    _visit(value, depth + 1, {**coordinate, dimension_name: key})
                return

            coordinates.append((coordinate, node))

        _visit(progress_stats, 0, {})
        return coordinates

    def get_index_progress_leaf_values(self) -> list[Any]:
        values: list[Any] = []

        def _visit(node: Any) -> None:
            if isinstance(node, dict):
                for value in node.values():
                    _visit(value)
                return
            if node is not None:
                values.append(node)

        _visit(self._progress_stats())
        return values

    def get_dimension_range_map_great_or_equal(
        self,
        *,
        identity_dimensions: Sequence[str],
        extra_time_delta: datetime.timedelta | None = None,
    ) -> list[dict[str, Any]]:
        dimension_range_map = []
        for coordinate, value in self.iter_index_progress_coordinates(
            identity_dimensions=identity_dimensions
        ):
            start_date = value or self._initial_fallback_date
            if start_date is not None and extra_time_delta is not None:
                start_date = start_date + extra_time_delta
            dimension_range_map.append(
                {
                    "coordinate": coordinate,
                    "start_date_operand": ">=",
                    "start_date": start_date,
                }
        )
        return dimension_range_map

    def get_last_update_for_identity(self, identity_value):
        return self._progress_stats().get(identity_value)

    def get_earliest_update_for_identity(self, identity_value):
        stats = self._progress_stats().get(identity_value)
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

    def filter_identity_level(
        self,
        level: int,
        filters: list,
    ):
        """
        Prune `self.index_progress` so that at the specified index level
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
        # Determine the numeric depth of the target level
        #   0 == first identity level, 1 == second identity level, etc.
        target_depth = level - 1

        # Special-case: filtering on the first identity level.
        if target_depth == 0:
            self._set_progress_stats({
                identity_value: stats
                for identity_value, stats in self._progress_stats().items()
                if identity_value in filters
            })
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
        # stats dict sits at depth=1 under each top-level identity.
        for identity_value, stats in self._progress_stats().items():
            if stats is None:
                new_stats[identity_value] = {
                    f: self._initial_fallback_date for f in allowed
                }
            else:
                pr = _prune(stats, current_depth=1)
                new_stats[identity_value] = pr or None

        self._set_progress_stats(new_stats)
        return self

    def _get_update_statistics_for_identity_values(
        self,
        identity_values: list | None,
        init_fallback_date=None,
    ):
        new_update_statistics = {}
        progress_stats = self.index_progress
        if identity_values is None:
            identity_values = list((progress_stats or {}).keys())

        for identity_value in identity_values:

            if progress_stats and identity_value in progress_stats:
                new_update_statistics[identity_value] = progress_stats[identity_value]
            else:

                new_update_statistics[identity_value] = init_fallback_date

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

        _max_time_in_identity_statistics = (
            _max_in_nested(new_update_statistics)
            if len(new_update_statistics) > 0
            else init_fallback_date
        )

        return new_update_statistics, _max_time_in_identity_statistics

    def update_identity_scope(
        self,
        identity_values: list | None,
        *,
        init_fallback_date: datetime = None,
    ):
        new_update_statistics = self.index_progress

        if identity_values is not None:
            new_update_statistics, _max_time_in_identity_statistics = (
                self._get_update_statistics_for_identity_values(
                    identity_values=identity_values,
                    init_fallback_date=init_fallback_date,
                )
            )

        else:
            _max_time_in_identity_statistics = self.max_time_index_value or init_fallback_date

        new_multi_index_column_stats = self.multi_index_column_stats
        if (
            new_update_statistics is not None
            and self.max_time_index_value is not None
            and self.multi_index_column_stats is not None
        ):
            new_multi_index_column_stats = {
                k: v
                for k, v in self.multi_index_column_stats.items()
                if k in new_update_statistics.keys()
            }

        du = UpdateStatistics(
            index_progress=new_update_statistics,
            max_time_index_value=self.max_time_index_value,
            multi_index_column_stats=new_multi_index_column_stats,
        )
        du._max_time_in_update_statistics = _max_time_in_identity_statistics
        du._initial_fallback_date = init_fallback_date
        return du

    def __getitem__(self, key: str) -> Any:
        if self.index_progress is None:
            raise KeyError(f"{key} not found (index_progress is None).")
        return self.index_progress[key]

    def __setitem__(self, key: str, value: Any) -> None:
        progress_stats = dict(self._progress_stats())
        progress_stats[key] = self._normalize_nested(value)
        self._set_progress_stats(progress_stats)

    def __delitem__(self, key: str) -> None:
        if not self.index_progress or key not in self.index_progress:
            raise KeyError(f"{key} not found in index_progress.")
        progress_stats = dict(self.index_progress)
        del progress_stats[key]
        self._set_progress_stats(progress_stats)

    def __iter__(self):
        """Iterate over keys."""
        if self.index_progress is None:
            return iter([])
        return iter(self.index_progress)

    def __len__(self) -> int:
        if not self.index_progress:
            return 0
        return len(self.index_progress)

    def keys(self):
        if not self.index_progress:
            return []
        return self.index_progress.keys()

    def values(self):
        if not self.index_progress:
            return []
        return self.index_progress.values()

    def items(self):
        if not self.index_progress:
            return []
        return self.index_progress.items()

    def filter_df_by_latest_value(self, df: pd.DataFrame) -> pd.DataFrame:


        names = list(df.index.names)
        time_level = names[0]
        identity_levels = [n for n in names if n != time_level]

        # Single-index time series fallback
        if not identity_levels:
            if self.max_time_index_value is not None:
                df = df[df.index > self.max_time_index_value]
                return df
            else:
                return df

        # Build a mask by iterating over each row tuple + its timestamp
        mask = []
        progress_stats = self._progress_stats()
        for idx_tuple, ts in zip(df.index, df.index.get_level_values(time_level), strict=False):
            if not isinstance(idx_tuple, tuple):
                idx_tuple = (idx_tuple,)
            # map level names → values
            level_vals = dict(zip(names, idx_tuple, strict=False))

            nested = progress_stats
            for lvl in identity_levels:
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


def _assign_nested_coordinate(root: dict[Any, Any], keys: list[Any], value: Any) -> None:
    if not keys:
        return
    sub = root
    for key in keys[:-1]:
        sub = sub.setdefault(key, {})
    sub[keys[-1]] = value


def get_index_progress_chunk_stats(chunk_df, time_index_name, index_names):
    index_names = list(index_names)
    chunk_stats = {
        "_GLOBAL_": {
            "max": UpdateStatistics._to_utc_datetime(chunk_df[time_index_name].max()),
            "min": UpdateStatistics._to_utc_datetime(chunk_df[time_index_name].min()),
        },
        "index_progress": {},
        "index_min": {},
    }
    grouped_dates = None
    if len(index_names) > 1:
        identity_dimensions = index_names[1:]
        grouped_dates = chunk_df.groupby(identity_dimensions)[time_index_name].agg(["min", "max"])
        df = grouped_dates.reset_index()

        for _, row in df.iterrows():
            keys = [row[level] for level in identity_dimensions]
            _assign_nested_coordinate(
                chunk_stats["index_progress"],
                keys,
                UpdateStatistics._to_utc_datetime(row["max"]),
            )
            _assign_nested_coordinate(
                chunk_stats["index_min"],
                keys,
                UpdateStatistics._to_utc_datetime(row["min"]),
            )
    return chunk_stats, grouped_dates


def combine_index_min_max_stats(index_min: dict[str, Any], index_progress: dict[str, Any]):
    if not isinstance(index_progress, dict):
        return {"min": index_min, "max": index_progress}

    combined = {}
    for key, progress_value in index_progress.items():
        min_value = index_min.get(key) if isinstance(index_min, dict) else None
        if isinstance(progress_value, dict):
            combined[key] = combine_index_min_max_stats(min_value or {}, progress_value)
        else:
            combined[key] = {"min": min_value, "max": progress_value}
    return combined


def _to_timestamp(value: Any):
    value = UpdateStatistics._to_utc_datetime(value)
    if isinstance(value, datetime.datetime):
        return value.timestamp()
    return value


def request_to_datetime(value: Any):
    return UpdateStatistics._to_utc_datetime(value)


def _combine_index_min_max_stats_as_timestamps(index_min: dict[str, Any], index_progress: dict[str, Any]):
    combined = combine_index_min_max_stats(index_min=index_min, index_progress=index_progress)

    def _recurse(node):
        if isinstance(node, dict) and set(node.keys()) == {"min", "max"}:
            return {"min": _to_timestamp(node["min"]), "max": _to_timestamp(node["max"])}
        if isinstance(node, dict):
            return {k: _recurse(v) for k, v in node.items()}
        return _to_timestamp(node)

    return _recurse(combined)


def get_chunk_stats(chunk_df, time_index_name, index_names):
    # LEGACY_COMPAT: older SDK paths still import get_chunk_stats
    # and expect _PER_ASSET_ leaves with min/max timestamps. DataNodeUpdate uses
    # get_index_progress_chunk_stats() as the canonical helper.
    _warn_legacy_compat(
        "get_chunk_stats() returns the legacy '_PER_ASSET_' shape. Use "
        "get_index_progress_chunk_stats() and send '_GLOBAL_', "
        "'index_progress', and 'index_min' instead.",
    )
    canonical_stats, grouped_dates = get_index_progress_chunk_stats(
        chunk_df=chunk_df,
        time_index_name=time_index_name,
        index_names=index_names,
    )
    legacy_stats = {
        "_GLOBAL_": {
            "max": _to_timestamp(canonical_stats["_GLOBAL_"]["max"]),
            "min": _to_timestamp(canonical_stats["_GLOBAL_"]["min"]),
        },
        "_PER_ASSET_": _combine_index_min_max_stats_as_timestamps(
            index_min=canonical_stats["index_min"],
            index_progress=canonical_stats["index_progress"],
        ),
    }
    return legacy_stats, grouped_dates


class LastUpdateMultiIndexStatsPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True, arbitrary_types_allowed=True)

    global_stats: dict[str, Any] = Field(..., alias="_GLOBAL_")
    index_progress: dict[str, Any] = Field(default_factory=dict)
    index_min: dict[str, Any] = Field(default_factory=dict)

    def to_payload(self) -> dict[str, Any]:
        return {
            "_GLOBAL_": self.global_stats,
            "index_progress": self.index_progress,
            "index_min": self.index_min,
        }


class LastUpdateIndexTimePayload(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True, arbitrary_types_allowed=True)

    global_stats: dict[str, Any] | None = Field(default=None, alias="_GLOBAL_")
    global_index_progress: dict[str, Any] | None = None
    index_progress: dict[str, Any] | None = None
    index_min: dict[str, Any] | None = None
    multi_index_stats: LastUpdateMultiIndexStatsPayload | None = None
    multi_index_column_stats: dict[str, Any] | None = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_shape(self):
        top_level_progress_keys = [
            self.global_stats is not None,
            self.global_index_progress is not None,
            self.index_progress is not None,
            self.index_min is not None,
        ]
        has_top_level_shape = any(top_level_progress_keys)
        if self.multi_index_stats is not None and has_top_level_shape:
            raise ValueError("Use either multi_index_stats or top-level progress fields, not both.")

        if self.multi_index_stats is None:
            if self.global_stats is not None and self.global_index_progress is not None:
                raise ValueError("Use either _GLOBAL_ or global_index_progress, not both.")
            if (self.global_stats is None and self.global_index_progress is None) or (
                self.index_progress is None or self.index_min is None
            ):
                raise ValueError(
                    "Top-level payloads require global_index_progress or _GLOBAL_, "
                    "index_progress, and index_min."
                )
        return self

    def to_nested_payload(self) -> dict[str, Any]:
        if self.multi_index_stats is not None:
            multi_index_stats = self.multi_index_stats.to_payload()
        else:
            multi_index_stats = {
                "_GLOBAL_": self.global_index_progress or self.global_stats,
                "index_progress": self.index_progress or {},
                "index_min": self.index_min or {},
            }

        return {
            "multi_index_stats": multi_index_stats,
            "multi_index_column_stats": self.multi_index_column_stats or {},
        }


def build_last_update_index_time_payload(
    *,
    global_index_progress: dict[str, Any] | None = None,
    index_progress: dict[str, Any] | None = None,
    index_min: dict[str, Any] | None = None,
    multi_index_stats: dict[str, Any] | None = None,
    multi_index_column_stats: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if multi_index_stats is not None and any(
        value is not None for value in [global_index_progress, index_progress, index_min]
    ):
        raise ValueError("Use either multi_index_stats or top-level progress fields, not both.")

    raw_payload: dict[str, Any] = {"multi_index_column_stats": multi_index_column_stats or {}}
    if multi_index_stats is not None:
        raw_payload["multi_index_stats"] = multi_index_stats
    else:
        raw_payload.update(
            {
                "global_index_progress": global_index_progress,
                "index_progress": index_progress,
                "index_min": index_min,
            }
        )

    return LastUpdateIndexTimePayload.model_validate(raw_payload).to_nested_payload()



class HistoricalUpdateRecord:
    uid: str | None = Field(None, description="Public uid of this historical update")
    update_time_start: datetime.datetime
    update_time_end: datetime.datetime | None = None
    error_on_update: bool = False
    trace_id: str | None = Field(default=None, max_length=255)
    updated_by_user_uid: str | None = Field(
        None, description="UID reference to the user that updated this record"
    )
    # extra fields for local control
    update_statistics: BaseUpdateStatistics | None = None
    must_update: bool | None = None
    direct_dependency_uids: list[str] | None = None

class LocalTimeSeriesHistoricalUpdate(HistoricalUpdateRecord,BasePydanticModel, BaseObjectOrm):

    related_table_uid: str | None = Field(None, description="Public uid of the related DataNodeUpdate")
    last_time_index_value: datetime.datetime | None = None


UpdateT = TypeVar("UpdateT")
UpdateDetailsT = TypeVar("UpdateDetailsT")
SourceTableConfigurationT = TypeVar("SourceTableConfigurationT")


class UpdateBatchResponse(BaseModel, Generic[UpdateT, UpdateDetailsT, SourceTableConfigurationT]):
    model_config = ConfigDict(extra="forbid")

    source_table_config_map: dict[str, SourceTableConfigurationT | None]
    state_data: dict[str, UpdateDetailsT]
    all_index_stats: dict[str, Any]
    data_node_updates: list[UpdateT]




class DataSource(BasePydanticModel, BaseObjectOrm):
    uid: str | None = Field(
        None,
        description="Public uid of the data source.",
    )
    data_source_uid: str | None = Field(
        None,
        description="Compatibility alias for the public data source uid.",
    )
    id: int | None = Field(None, description="The unique identifier of the Local Disk Source Lake")
    display_name: str
    organization: int | None = Field(
        None, description="The unique identifier of the Local Disk Source Lake"
    )
    organization_uid: str | None = Field(
        None,
        description="Public uid of the owning organization.",
    )
    class_type: str
    status: str
    extra_arguments: dict | None = None

    STATUS_AVAILABLE: ClassVar[str] = "AVAILABLE"

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

    @classmethod
    def get_or_create_sqlite(cls, time_out=None, *args, **kwargs):
        url = cls.get_object_url() + "/get_or_create_sqlite/"
        payload = {"json": serialize_to_json(kwargs)}
        s = cls.build_session()
        r = make_request(
            s=s, loaders=cls.LOADERS, r_type="POST", url=url, payload=payload, time_out=time_out
        )
        if r.status_code not in [200, 201]:
            raise Exception(f"Error in request {r.text}")
        return cls(**r.json())

    @classmethod
    def create_duckdb(
        cls,
        time_out: int | None = None,
        *,
        display_name: str | None = None,
        host_mac_address: str | None = None,
        **kwargs,
    ):
        """
        Explicitly create or resolve the physical DuckDB DataSource for this host.
        """
        host_uid = host_mac_address or bios_uuid()
        payload = dict(kwargs)
        payload.setdefault("host_mac_address", host_uid)
        payload.setdefault("display_name", display_name or f"DuckDB_{host_uid}")
        return cls.get_or_create_duck_db(time_out=time_out, **payload)

    @classmethod
    def create_sqlite(
        cls,
        time_out: int | None = None,
        *,
        display_name: str | None = None,
        host_mac_address: str | None = None,
        **kwargs,
    ):
        """
        Explicitly create or resolve the physical SQLite DataSource for this host.
        """
        host_uid = host_mac_address or bios_uuid()
        payload = dict(kwargs)
        payload.setdefault("host_mac_address", host_uid)
        payload.setdefault("display_name", display_name or f"SQLite_{host_uid}")
        return cls.get_or_create_sqlite(time_out=time_out, **payload)

    def insert_data_into_table(
        self,
        serialized_data_frame: pd.DataFrame,
        data_node_update: DataNodeUpdate,
        overwrite: bool,
        time_index_name: str,
        index_names: list,
        grouped_dates: dict,
        column_dtypes_map: Mapping[str, Any] | None = None,
    ):

        if self.class_type in LOCAL_DATA_SOURCE_CLASS_TYPES:
            _local_data_interface(self.class_type).upsert(
                df=serialized_data_frame,
                table=data_node_update.data_node_storage.storage_hash,
                index_names=index_names,
                time_index_name=time_index_name,
            )
        else:
            DataNodeUpdate.post_data_frame_in_chunks(
                serialized_data_frame=serialized_data_frame,
                data_node_update=data_node_update,
                data_source=self,
                index_names=index_names,
                time_index_name=time_index_name,
                overwrite=overwrite,
                column_dtypes_map=column_dtypes_map,
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
        dimension_filters: dict[str, list[Any]] | None = None,
        index_coordinates: list[dict[str, Any]] | None = None,
        dimension_range_map: list[dict[str, Any]] | None = None,
        column_range_descriptor: dict[str, UniqueIdentifierRangeMap] | None = None,
    ) -> pd.DataFrame:

        if self.class_type in LOCAL_DATA_SOURCE_CLASS_TYPES:
            db_interface = _local_data_interface(self.class_type)
            table_name = data_node_update.data_node_storage.storage_hash
            stc = data_node_update.data_node_storage.sourcetableconfiguration

            adjusted_start, adjusted_end, adjusted_dimension_range_map, _ = (
                db_interface.constrain_read(
                    table=table_name,
                    start=start_date,
                    end=end_date,
                    time_index_name=stc.time_index_name,
                    index_names=stc.index_names,
                    dimension_filters=dimension_filters,
                    index_coordinates=index_coordinates,
                    dimension_range_map=dimension_range_map,
                )
            )

            df = db_interface.read(
                table=table_name,
                start=adjusted_start,
                end=adjusted_end,
                great_or_equal=great_or_equal,
                less_or_equal=less_or_equal,
                index_names=stc.index_names,
                time_index_name=stc.time_index_name,
                dimension_filters=dimension_filters,
                index_coordinates=index_coordinates,
                dimension_range_map=adjusted_dimension_range_map,
                columns=columns,
            )

        else:
            if column_range_descriptor is not None:
                raise Exception("On this data source do not use column_range_descriptor")
            df = data_node_update.get_data_between_dates_from_api(
                start_date=start_date,
                end_date=end_date,
                great_or_equal=great_or_equal,
                less_or_equal=less_or_equal,
                dimension_filters=dimension_filters,
                index_coordinates=index_coordinates,
                dimension_range_map=dimension_range_map,
                columns=columns,
            )
        if len(df) == 0:
            logger.warning(f"No data returned from remote API for {data_node_update.update_hash}")
            return df

        stc = data_node_update.data_node_storage.sourcetableconfiguration
        try:
            df[stc.time_index_name] = token_to_pandas_series(
                df[stc.time_index_name],
                TIMESTAMP_TZ,
                is_time_index=True,
            )
        except Exception as e:
            raise e
        columns_to_loop = set(columns or stc.column_dtypes_map.keys()) | set(stc.index_names)
        for c, c_type in stc.column_dtypes_map.items():
            if c not in columns_to_loop:
                continue
            if c in df.columns:
                df[c] = token_to_pandas_series(
                    df[c],
                    c_type,
                    is_time_index=c == stc.time_index_name,
                )
        df = df.set_index(stc.index_names)
        return df

    def get_earliest_value(
        self,
        data_node_update: DataNodeUpdate,
    ) -> tuple[pd.Timestamp | None, dict[Any, pd.Timestamp | None]]:
        if self.class_type in LOCAL_DATA_SOURCE_CLASS_TYPES:
            db_interface = _local_data_interface(self.class_type)
            storage = data_node_update.data_node_storage
            table_name = getattr(storage, "storage_hash", None) or storage.table_name
            stc = storage.sourcetableconfiguration
            return db_interface.time_index_minima(
                table=table_name,
                index_names=stc.index_names,
                time_index_name=stc.time_index_name,
            )

        else:
            raise NotImplementedError


class DynamicTableDataSource(BasePydanticModel, BaseObjectOrm):
    uid: str | None = Field(
        None,
        description="Public uid of the dynamic table data source.",
    )
    id: int | None = Field(
        None,
        description="Legacy numeric identifier of the dynamic table data source.",
    )
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

    def persist_to_pickle(self, path):
        import cloudpickle

        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as handle:
            cloudpickle.dump(self, handle)

    @classmethod
    def get_or_create_duck_db(cls, time_out=None, *args, **kwargs):
        url = cls.get_object_url() + "/get_or_create_duck_db/"
        s = cls.build_session()
        r = make_request(
            s=s,
            loaders=cls.LOADERS,
            r_type="POST",
            url=url,
            payload={"json": kwargs},
            time_out=time_out,
        )
        if r.status_code not in [200, 201]:
            raise Exception(f"Error in request {r.text}")
        return cls(**r.json())

    @classmethod
    def get_or_create_sqlite(cls, time_out=None, *args, **kwargs):
        url = cls.get_object_url() + "/get_or_create_sqlite/"
        s = cls.build_session()
        r = make_request(
            s=s,
            loaders=cls.LOADERS,
            r_type="POST",
            url=url,
            payload={"json": kwargs},
            time_out=time_out,
        )
        if r.status_code not in [200, 201]:
            raise Exception(f"Error in request {r.text}")
        return cls(**r.json())

    @classmethod
    def create_duckdb(
        cls,
        *,
        data_source: int | DataSource,
        time_out: int | None = None,
        **kwargs,
    ):
        related_resource_id = (
            data_source if isinstance(data_source, int) else getattr(data_source, "id", None)
        )
        if related_resource_id is None:
            raise ValueError("A DuckDB DataSource with an id is required.")

        class_type = None if isinstance(data_source, int) else getattr(data_source, "class_type", None)
        if class_type is not None and class_type != DUCK_DB:
            raise ValueError(
                f"DynamicTableDataSource.create_duckdb requires a {DUCK_DB!r} "
                f"DataSource, got {class_type!r}."
            )

        return cls.get_or_create_duck_db(
            time_out=time_out,
            related_resource=related_resource_id,
            **kwargs,
        )

    @classmethod
    def create_sqlite(
        cls,
        *,
        data_source: int | DataSource,
        time_out: int | None = None,
        **kwargs,
    ):
        related_resource_id = (
            data_source if isinstance(data_source, int) else getattr(data_source, "id", None)
        )
        if related_resource_id is None:
            raise ValueError("A SQLite DataSource with an id is required.")

        class_type = None if isinstance(data_source, int) else getattr(data_source, "class_type", None)
        if class_type is not None and class_type != SQLITE:
            raise ValueError(
                f"DynamicTableDataSource.create_sqlite requires a {SQLITE!r} "
                f"DataSource, got {class_type!r}."
            )

        return cls.get_or_create_sqlite(
            time_out=time_out,
            related_resource=related_resource_id,
            **kwargs,
        )

    def get_data_by_time_index(self, *args, **kwargs):
        return self.related_resource.get_data_by_time_index(*args, **kwargs)




class GithubOrganization(BasePydanticModel, BaseObjectOrm):
    uid: str
    login: str
    display_name: str | None = None

    def __str__(self):
        return yaml.safe_dump(self.model_dump(), sort_keys=False, default_flow_style=False)


class ProjectBaseImage(BasePydanticModel, BaseObjectOrm):
    uid: str
    latest_digest: str | None = None
    description: str
    title: str
    tags: list[str] | None = None

    def __str__(self):
        return yaml.safe_dump(self.model_dump(), sort_keys=False, default_flow_style=False)




class ProjectNameValidationNormalized(BasePydanticModel):
    slugified_project_name: str = Field(
        ...,
        title="Slugified Project Name",
        description="Slug-safe project name generated by the platform for URLs and repository naming.",
        examples=["rates-platform"],
        json_schema_extra={"label": "Slugified Project Name"},
    )
    project_library_name: str = Field(
        ...,
        title="Project Library Name",
        description="Python-safe library/module name derived from the proposed project name.",
        examples=["rates_platform"],
        json_schema_extra={"label": "Project Library Name"},
    )


class ProjectNameValidationResult(BasePydanticModel):
    project_name: str = Field(
        ...,
        title="Project Name",
        description="Original project name submitted for validation.",
        examples=["Rates Platform"],
        json_schema_extra={"label": "Project Name"},
    )
    available: bool = Field(
        ...,
        title="Available",
        description="Whether the project name is currently available for creation in the caller's organization.",
        examples=[False],
        json_schema_extra={"label": "Available"},
    )
    reason: str | None = Field(
        None,
        title="Reason",
        description="Human-readable explanation when the requested project name is unavailable or adjusted.",
        examples=["A project with this name already exists in your organization."],
        json_schema_extra={"label": "Reason"},
    )
    normalized: ProjectNameValidationNormalized = Field(
        ...,
        title="Normalized Names",
        description="Platform-generated normalized forms derived from the submitted project name.",
        json_schema_extra={"label": "Normalized Names"},
    )
    suggestions: list[str] = Field(
        default_factory=list,
        title="Suggestions",
        description="Alternative project names suggested by the platform when the submitted name is unavailable.",
        examples=[["Rates Platform 2", "Rates Platform 3", "Rates Platform 4"]],
        json_schema_extra={"label": "Suggestions"},
    )


class ProjectQuickSearchResult(BasePydanticModel):
    uid: str | None = Field(
        None,
        title="UID",
        description="Public uid of the matching project.",
        json_schema_extra={"label": "UID"},
    )
    project_name: str = Field(
        ...,
        title="Project Name",
        description="Display name of the matching project.",
        json_schema_extra={"label": "Project Name"},
    )
    repository_branch: str | None = Field(
        None,
        title="Repository Branch",
        description="Configured repository branch for the matching project.",
        json_schema_extra={"label": "Repository Branch"},
    )
    cluster_uid: str | None = Field(
        None,
        title="Cluster UID",
        description="Public UID of the cluster associated with the matching project, when present.",
        json_schema_extra={"label": "Cluster UID"},
    )


class Project(LabelableObjectMixin, ShareableObjectMixin, BasePydanticModel, BaseObjectOrm):
    FILTERSET_FIELDS: ClassVar[dict[str, list[str]]] = {
        "project_name": ["in", "exact", "contains"],
        "uid": ["in", "exact"],
        "labels": ["exact", "in", "contains"],
    }
    FILTER_VALUE_NORMALIZERS: ClassVar[dict[str, str]] = {
        "uid": "str",
        "uid__in": "str",
        "project_name": "str",
        "labels": "str",
        "labels__in": "str",
        "labels__contains": "str",
    }
    uid: str | None = Field(
        None,
        title="Project UID",
        description="Public uid of the project.",
        examples=["project-uid-142"],
        json_schema_extra={"label": "Project UID"},
    )
    project_name: str = Field(
        ...,
        title="Project Name",
        description="Human-readable name of the project.",
        examples=["Data Research Pipeline"],
        json_schema_extra={"label": "Project Name"},
    )
    data_source: DynamicTableDataSource | None = Field(
        None,
        title="Data Source",
        description="Default data source associated with the project, when configured.",
        json_schema_extra={"label": "Data Source"},
    )
    git_ssh_url: str | None = Field(
        None,
        title="Git SSH URL",
        description="SSH repository URL used to access the project's source code repository.",
        examples=["git@github.com:mainsequence/data-pipeline.git"],
        json_schema_extra={"label": "Git SSH URL"},
    )
    created_by: str | int | dict[str, Any] | None = Field(
        None,
        title="Created By",
        description="Backend-provided creator metadata for the project.",
        json_schema_extra={"label": "Created By"},
    )

    labels: list[str] = Field(
        default_factory=list,
        title="Labels",
        description=(
            "Organizational labels attached to the project. "
            "These are helpers for grouping and discovery only and do not change runtime behavior or functionality."
        ),
        json_schema_extra={"label": "Labels"},
    )

    is_initialized: bool = Field(
        ...,
        title="Is Initialized",
        description="Whether the project has completed its initial setup and is ready for use.",
        examples=[True],
        json_schema_extra={"label": "Is Initialized"},
    )


    @staticmethod
    def _normalize_env_vars(
        env_vars: dict[str, str] | list[dict[str, str]] | None,
    ) -> list[dict[str, str]] | None:
        """
        Serializer expects: [{"name": "...", "value": "..."}, ...]
        Allow passing a dict for convenience.
        """
        if env_vars is None:
            return None
        if isinstance(env_vars, dict):
            return [{"name": k, "value": v} for k, v in env_vars.items()]
        # assume already list-of-dicts shape
        return env_vars
    @staticmethod
    def _coerce_uid(obj: Any, *, field_name: str) -> str | None:
        """
        Accept:
          - None
          - uid string
          - any object with attribute `.uid`
          - dict-like with key "uid"
        """
        if obj is None:
            return None
        if isinstance(obj, UUID):
            return str(obj)
        if isinstance(obj, str):
            normalized = obj.strip()
            if normalized:
                return normalized
        if hasattr(obj, "uid") and obj.uid not in (None, ""):
            return str(obj.uid).strip()
        if isinstance(obj, dict) and obj.get("uid") not in (None, ""):
            return str(obj["uid"]).strip()
        raise TypeError(
            f"{field_name} must be a uid string, an object with .uid, a dict with 'uid', or None. "
            f"Got: {type(obj)!r}"
        )

    @classmethod
    def create(
        cls,
        *,
        project_name: str,
        data_source_uid: str | DynamicTableDataSource | dict[str, Any] | None = None,
        default_base_image_uid: str | ProjectBaseImage | dict[str, Any] | None = None,
        github_org_uid: str | GithubOrganization | dict[str, Any] | None = None,
        repository_branch: str | None = None,
        env_vars: dict[str, str] | list[dict[str, str]] | None = None,
            timeout:int | None = None,
    ) -> Project:
        """
        POST /projects/

        Sends:
          - project_name
          - repository_branch (optional)
          - data_source_uid (optional; server may auto-pick for individual orgs)
          - default_base_image_uid (optional)
          - github_org_uid (optional)
          - env_vars (optional list of {name,value})
        """
        url = cls.get_object_url() + "/"

        payload: dict[str, Any] = {
            "project_name": project_name,
        }

        if repository_branch:
            payload["repository_branch"] = repository_branch

        ds_uid = cls._coerce_uid(data_source_uid, field_name="data_source_uid")
        if ds_uid is not None:
            payload["data_source_uid"] = ds_uid

        img_uid = cls._coerce_uid(default_base_image_uid, field_name="default_base_image_uid")
        if img_uid is not None:
            payload["default_base_image_uid"] = img_uid

        org_uid = cls._coerce_uid(github_org_uid, field_name="github_org_uid")
        if org_uid is not None:
            payload["github_org_uid"] = org_uid

        env_list = cls._normalize_env_vars(env_vars)
        if env_list is not None:
            payload["env_vars"] = env_list

        s = cls.build_session()
        r = make_request(
            s=s,
            loaders=cls.LOADERS,
            r_type="POST",
            url=url,
            payload={"json":payload},time_out=timeout
        )

        # your helpers already exist; use them consistently
        raise_for_response(r)

        # DRF should return 201 with your detail serializer shape
        return cls(**r.json())


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
            raise_for_response(r,)

        if r.status_code != 200:
            raise Exception(f"Error in request {r.text}")
        return cls(**r.json())

    @classmethod
    def quick_search(
        cls,
        q: str,
        *,
        limit: int = 20,
        timeout: int | None = None,
    ) -> list[ProjectQuickSearchResult]:
        """
        Return project quick-search matches visible to the authenticated user.

        This hits:
            GET /orm/api/pods/projects/quick-search/?q=...

        Parameters
        ----------
        q:
            Search query. Must contain at least 3 non-whitespace characters.
        limit:
            Maximum number of matches to return. Defaults to 20 and is capped at 100.
        timeout:
            Optional request timeout in seconds.
        """
        normalized_query = (q or "").strip()
        if len(normalized_query) < 3:
            raise ValueError("Query must contain at least 3 characters.")

        limit = int(limit)
        if limit < 1 or limit > 100:
            raise ValueError("limit must be between 1 and 100")

        url = cls.get_object_url() + "/quick-search/"
        r = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="GET",
            url=url,
            payload={"params": {"q": normalized_query, "limit": limit}},
            time_out=timeout,
        )
        raise_for_response(r)

        if not r.content:
            return []

        payload = r.json()
        if not isinstance(payload, list):
            raise ValueError(
                f"Unexpected response type for Project.quick_search(): {type(payload)!r}"
            )

        return [ProjectQuickSearchResult.model_validate(item) for item in payload]

    @classmethod
    def validate_name(
        cls,
        *,
        project_name: str,
        timeout: int | None = None,
    ) -> ProjectNameValidationResult:
        """
        Validate whether a project name is available for creation on the platform.

        This hits:
            GET /orm/api/pods/projects/validate-name/?project_name=...

        Parameters
        ----------
        project_name:
            Candidate project name to validate.
        timeout:
            Optional request timeout in seconds.

        Returns
        -------
        ProjectNameValidationResult
            Validation payload including availability, normalized names, and suggested alternatives.
        """
        normalized_project_name = (project_name or "").strip()
        if not normalized_project_name:
            raise ValueError("project_name is required.")

        url = cls.get_object_url() + "/validate-name/"
        r = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="GET",
            url=url,
            payload={"params": {"project_name": normalized_project_name}},
            time_out=timeout,
        )
        raise_for_response(r)

        if not r.content:
            raise ValueError("Project.validate_name() returned an empty response.")

        payload = r.json()
        if not isinstance(payload, dict):
            raise ValueError(
                f"Unexpected response type for Project.validate_name(): {type(payload)!r}"
            )

        return ProjectNameValidationResult.model_validate(payload)

    @classmethod
    def sync_project_after_commit(
        cls,
        project_uid: str | int,
        timeout: int | None = None,
    ) -> Project | dict[str, Any] | None:
        url = f"{cls.get_object_url()}/{project_uid}/sync_project_after_commit/"

        s = cls.build_session()
        r = make_request(
            s=s,
            loaders=cls.LOADERS,
            r_type="POST",
            url=url,
            time_out=timeout,
        )
        raise_for_response(r)

        if not r.content:
            return None

        data = r.json()
        if isinstance(data, dict) and ({"project_name"} <= set(data.keys())):
            return cls(**data)
        return data

    def delete(
            self,
            *,
            delete_repositories: bool = False,
            timeout: int | None = None,
    ) -> dict[str, Any] | None:
        """
        DELETE /projects/{uid}/

        Optional query param:
          - delete_repositories=true
        """
        cls = type(self)
        url = f"{cls.get_object_url()}/{self._public_detail_reference()}/"

        request_payload: dict[str, Any] = {}
        if delete_repositories:
            request_payload["params"] = {"delete_repositories": "true"}

        s = cls.build_session()
        r = make_request(
            s=s,
            loaders=cls.LOADERS,
            r_type="DELETE",
            url=url,
            payload=request_payload,
            time_out=timeout,
        )

        raise_for_response(r)

        return r.json() if r.content else None

    def get_data_nodes_updates(self, *, timeout: int | None = None) -> list[DataNodeUpdate]:
        """
        GET /projects/{uid}/get-data-nodes-updates/

        Returns a list of DataNodeUpdate objects for this project.
        """
        cls = type(self)
        url = f"{cls.get_object_url()}/{self._public_detail_reference()}/get-data-nodes-updates/"

        s = cls.build_session()
        r = make_request(
            s=s,
            loaders=cls.LOADERS,
            r_type="GET",
            url=url,
            time_out=timeout,
        )
        raise_for_response(r)

        payload = r.json()
        if isinstance(payload, list):
            raw_updates = payload
        elif isinstance(payload, dict):
            if isinstance(payload.get("results"), list):
                raw_updates = payload["results"]
            elif isinstance(payload.get("data_node_updates"), list):
                raw_updates = payload["data_node_updates"]
            else:
                raw_updates = []
        else:
            raise ValueError(
                f"Unexpected response type for project data node updates: {type(payload)!r}"
            )

        return [u if isinstance(u, DataNodeUpdate) else DataNodeUpdate(**u) for u in raw_updates]

    def __str__(self):
        return yaml.safe_dump(
            self.model_dump(),
            sort_keys=False,
            default_flow_style=False,
        )



class ProjectImage(BasePydanticModel, BaseObjectOrm):
    """
    Image build from a a project
    """

    FILTERSET_FIELDS: ClassVar[dict[str, list[str]]] = {
        "search": ["exact"],
        "related_project__uid": ["in", "exact"],
        "related_project_uid": ["in", "exact"],
        "project_repo_hash": ["exact", "in"],
    }
    FILTER_VALUE_NORMALIZERS: ClassVar[dict[str, str]] = {
        "related_project__uid": "uid",
        "related_project_uid": "uid",
    }

    uid: str = Field(..., description="Public UID of the project image")
    title: str | None = Field(None, description="Human-readable image title")
    key: str | None = Field(None, description="Stable image key")
    description: str | None = Field(None, description="Image description")
    project_repo_hash: str = Field(..., description="Canonical full commit SHA for the built image")
    related_project_uid: str | None = Field(None, description="Public UID of the owning project")
    base_image: ProjectBaseImage | None = Field(None, description="Persisted parent base image")
    tags: list[str] | None = Field(default=[], description="Observed registry tags for the project image")
    build_error: str | None = Field(None, description="Backend build error, when present")
    is_ready: bool = Field(..., description="Whether the image is ready in Artifact Registry")
    creation_date: datetime.datetime | None = Field(None, description="Creation timestamp")

    @staticmethod
    def _coerce_uid(obj: Any, *, field_name: str) -> str | None:
        if obj is None:
            return None
        if isinstance(obj, UUID):
            return str(obj)
        if isinstance(obj, str):
            normalized = obj.strip()
            if normalized:
                return normalized
        if hasattr(obj, "uid") and obj.uid not in (None, ""):
            return str(obj.uid).strip()
        if isinstance(obj, dict) and obj.get("uid") not in (None, ""):
            return str(obj["uid"]).strip()
        raise TypeError(
            f"{field_name} must be a uid string, an object with .uid, a dict with 'uid', or None. "
            f"Got: {type(obj)!r}"
        )

    @classmethod
    def create(
        cls,
        *,
        project_repo_hash: str,
        related_project_uid: str | Project | dict[str, Any] | None = None,
        base_image_uid: str | ProjectBaseImage | dict[str, Any] | None = None,
        timeout=None,
        files=None,
        **kwargs,
    ) -> ProjectImage:
        """
        Create a project image.
        """
        payload: dict[str, Any] = {"project_repo_hash": project_repo_hash}

        project_uid = cls._coerce_uid(related_project_uid, field_name="related_project_uid")
        if project_uid is not None:
            payload["related_project_uid"] = project_uid

        image_uid = cls._coerce_uid(base_image_uid, field_name="base_image_uid")
        if image_uid is not None:
            payload["base_image_uid"] = image_uid

        payload.update(kwargs)
        data = cls.serialize_for_json(payload)
        request_payload = {"json": data}
        if files:
            request_payload["files"] = files

        r = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="POST",
            url=f"{cls.get_object_url()}/",
            payload=request_payload,
            time_out=timeout,
        )
        if r.status_code not in (200, 201, 202):
            raise_for_response(r, payload=request_payload)
        return cls(**r.json())

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
        column_dtypes_map: Mapping[str, Any] | None = None,
    ):

        DataNodeUpdate.post_data_frame_in_chunks(
            serialized_data_frame=serialized_data_frame,
            data_node_update=data_node_update,
            data_source=self,
            index_names=index_names,
            time_index_name=time_index_name,
            overwrite=overwrite,
            column_dtypes_map=column_dtypes_map,
        )

    def get_data_by_time_index(
        self,
        data_node_update: DataNodeUpdate,
        start_date: datetime.datetime | None = None,
        end_date: datetime.datetime | None = None,
        great_or_equal: bool = True,
        less_or_equal: bool = True,
        columns: list[str] | None = None,
        dimension_filters: dict[str, list[Any]] | None = None,
        index_coordinates: list[dict[str, Any]] | None = None,
        dimension_range_map: list[dict[str, Any]] | None = None,
    ) -> pd.DataFrame:


        df = data_node_update.get_data_between_dates_from_api(
            start_date=start_date,
            end_date=end_date,
            great_or_equal=great_or_equal,
            less_or_equal=less_or_equal,
            dimension_filters=dimension_filters,
            index_coordinates=index_coordinates,
            dimension_range_map=dimension_range_map,
            columns=columns,
        )
        if len(df) == 0:
            if logger:
                logger.warning(
                    f"No data returned from remote API for {data_node_update.update_hash}"
                )
            return df

        stc = data_node_update.data_node_storage.sourcetableconfiguration
        df[stc.time_index_name] = token_to_pandas_series(
            df[stc.time_index_name],
            TIMESTAMP_TZ,
            is_time_index=True,
        )
        for c, c_type in stc.column_dtypes_map.items():
            if c in df.columns:
                df[c] = token_to_pandas_series(
                    df[c],
                    c_type,
                    is_time_index=c == stc.time_index_name,
                )
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



def query_agent(json_payload: dict, timeout=None):
    url = MAINSEQUENCE_ENDPOINT + "/orm/api/tdag-gpt/query_agent/"
    r = make_request(
        s=session,
        r_type="POST",
        url=url,
        payload={"json": json_payload},
        loaders=loaders,
        time_out=(timeout if timeout is not None else 200),
    )
    from .exceptions import raise_for_response
    raise_for_response(r, payload={"json": json_payload})
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
    url = MAINSEQUENCE_ENDPOINT + f"/orm/api/pods/job-run/{os.getenv('JOB_RUN_ID')}/add_created_object/"
    payload = {"json": {"app_label": app_label, "model_name": model_name, "object_id": object_id}}

    r = make_request(
        s=session,
        loaders=loaders,
        r_type="POST",
        url=url,
        payload=payload,
        time_out=timeout,
    )

    from .exceptions import raise_for_response
    raise_for_response(r, payload=payload)
    return r.json()



class Bucket(ShareableObjectMixin, BasePydanticModel, BaseObjectOrm):
    FILTERSET_FIELDS: ClassVar[dict[str, list[str]]] = {
        "uid": ["in", "exact"],
        "name": ["in", "exact"],
    }
    FILTER_VALUE_NORMALIZERS: ClassVar[dict[str, str]] = {
        "uid": "uid",
        "name": "str",
        "name__in": "str",
    }

    uid: str | None = Field(
        None,
        title="Bucket UID",
        description="Public UID of the bucket.",
        examples=["47b8eac1-7630-44f4-bb42-7b4055ec4afe"],
        json_schema_extra={"label": "Bucket UID"},
    )
    name:str= Field(
        ...,
        title="Bucket Name",
        description="Human-readable Bucket name ",
        examples=["daily_positions_report.pdf"],
        json_schema_extra={"label": "Bucket Name"},
    )




class Artifact(ShareableObjectMixin, BasePydanticModel, BaseObjectOrm):
    uid: str | None = Field(
        None,
        title="Artifact UID",
        description="Public UID of the artifact record.",
        examples=["d28c87b6-a784-4f9f-bc2c-f47b43a04274"],
        json_schema_extra={"label": "Artifact UID"},
    )
    name: str = Field(
        ...,
        title="Artifact Name",
        description="Human-readable artifact name used to identify the stored file or payload.",
        examples=["daily_positions_report.pdf"],
        json_schema_extra={"label": "Artifact Name"},
    )

    bucket_name: str = Field(
        ...,
        title="Bucket Name",
        description="Storage bucket where the artifact content is persisted.",
        examples=["default_bucket"],
        json_schema_extra={"label": "Bucket Name"},
    )
    bucket_uid: str | None = Field(
        None,
        title="Bucket UID",
        description="Public UID of the storage bucket where the artifact content is persisted.",
        examples=["47b8eac1-7630-44f4-bb42-7b4055ec4afe"],
        json_schema_extra={"label": "Bucket UID"},
    )
    content: Any = Field(
        ...,
        title="Artifact Content",
         description="signed url pointed to the download link of the content",
        examples=["<binary-or-serialized-artifact-content>"],
        json_schema_extra={"label": "Artifact Content"},
    )
    creation_date: datetime.datetime = Field(
        ...,
        title="Creation Date",
        description="Timestamp when the artifact was created and stored.",
        examples=["2026-03-15T10:30:00Z"],
        json_schema_extra={"label": "Creation Date"},
    )

    @classmethod
    def upload_file(cls, filepath, name,  bucket_name=None):
        bucket_name=bucket_name if bucket_name else "default_bucket"
        return cls.get_or_create(
            filepath=filepath,
            name=name,
            bucket_name=bucket_name,
        )

    @classmethod
    def get_or_create(cls, filepath, name,  bucket_name):
        url = cls.get_object_url() + "/get_or_create/"
        s = cls.build_session()
        with open(filepath, "rb") as f:
            data = {
                "name": name,
                "bucket_name": bucket_name if bucket_name else "default_bucket",
            }
            files = {"content": (str(filepath), f, "application/pdf")}
            payload = {"json": data, "files": files}
            r = make_request(s=s, loaders=cls.LOADERS, r_type="POST", url=url, payload=payload)

            if r.status_code not in [200, 201]:
                raise Exception(f"Failed to get artifact: {r.status_code} - {r.text}")

            return cls(**r.json())


@dataclass(frozen=True)
class _PodProjectResolution:
    project: Project | None
    status: str
    detail: str = ""


def _reset_local_pod_project_resolution_cache() -> None:
    global _POD_PROJECT_RESOLUTION_CACHE
    with _POD_PROJECT_RESOLUTION_LOCK:
        _POD_PROJECT_RESOLUTION_CACHE = None
        _POD_PROJECT_LOGGED_STATES.clear()


def _build_local_pod_project_resolution() -> _PodProjectResolution:
    running_project_uid = (os.environ.get("MAIN_SEQUENCE_PROJECT_UID") or "").strip()
    if not running_project_uid:
        return _PodProjectResolution(
            project=None,
            status="missing",
            detail="MAIN_SEQUENCE_PROJECT_UID is not configured.",
        )
    try:
        project = Project.get(pk=running_project_uid)
    except DoesNotExist:
        return _PodProjectResolution(
            project=None,
            status="not_found",
            detail=f"Project reference {running_project_uid!r} from local runtime env was not found.",
        )
    except Exception as exc:
        return _PodProjectResolution(
            project=None,
            status="lookup_failed",
            detail=(
                "Could not resolve project reference "
                f"{running_project_uid!r} from local runtime env: {exc}"
            ),
        )

    return _PodProjectResolution(project=project, status="resolved")


def _resolve_local_pod_project(*, refresh: bool = False) -> _PodProjectResolution:
    global _POD_PROJECT_RESOLUTION_CACHE, POD_PROJECT

    with _POD_PROJECT_RESOLUTION_LOCK:
        if _POD_PROJECT_RESOLUTION_CACHE is None or refresh:
            _POD_PROJECT_RESOLUTION_CACHE = _build_local_pod_project_resolution()
            POD_PROJECT = _POD_PROJECT_RESOLUTION_CACHE.project
        return _POD_PROJECT_RESOLUTION_CACHE


def _log_local_pod_project_resolution(resolution: _PodProjectResolution) -> None:
    if resolution.status == "resolved":
        return

    cache_key = (resolution.status, resolution.detail)
    with _POD_PROJECT_RESOLUTION_LOCK:
        if cache_key in _POD_PROJECT_LOGGED_STATES:
            return
        _POD_PROJECT_LOGGED_STATES.add(cache_key)

    continuation = " Continuing without local pod project attachment."
    message = (resolution.detail or "No local pod project attached.").strip()
    if not message.endswith("."):
        message += "."
    message += continuation

    if resolution.status == "missing":
        logger.debug(message)
    else:
        logger.warning(message)


def _require_local_pod_project(operation: str) -> Project:
    resolution = _resolve_local_pod_project()
    if resolution.project is not None:
        return resolution.project

    _log_local_pod_project_resolution(resolution)

    detail = (resolution.detail or "No local pod project attached.").strip()
    raise RuntimeError(f"{operation} requires a local pod project. {detail}")


@dataclass
class PodDataSource:
    data_source: Any | None = None

    def set_remote_db(self):
        resolution = _resolve_local_pod_project()
        if resolution.project is None:
            _log_local_pod_project_resolution(resolution)
            return None

        self.data_source = resolution.project.data_source
        logger.debug(f"Set remote data source to {self.data_source.related_resource}")

        if self.data_source.related_resource.status != "AVAILABLE":
            raise Exception(f"Project Database {self.data_source} is not available")

    @property
    def is_local_duck_db(self):
        related_resource = getattr(getattr(self, "data_source", None), "related_resource", None)
        return getattr(related_resource, "class_type", None) == DUCK_DB

    @property
    def is_local_db(self):
        return self.local_db_class_type in LOCAL_DATA_SOURCE_CLASS_TYPES

    @property
    def local_db_class_type(self):
        related_resource = getattr(getattr(self, "data_source", None), "related_resource", None)
        return getattr(related_resource, "class_type", None)

    def set_local_db(self, *, data_source: DataSource | None = None):
        if data_source is None:
            raise ValueError(
                "set_local_db requires an explicit local DataSource. "
                "Create one with DataSource.create_duckdb() or DataSource.create_sqlite() and pass "
                "SessionDataSource.set_local_db(data_source=data_source)."
            )
        class_type = getattr(data_source, "class_type", None)
        if class_type not in LOCAL_DATA_SOURCE_CLASS_TYPES:
            raise ValueError(
                "set_local_db requires a supported local DataSource "
                f"{sorted(LOCAL_DATA_SOURCE_CLASS_TYPES)!r}, got {class_type!r}."
            )
        if getattr(data_source, "id", None) is None:
            raise ValueError("set_local_db requires a persisted local DataSource with an id.")

        if class_type == DUCK_DB:
            local_dynamic_data_source = DynamicTableDataSource.create_duckdb(data_source=data_source)
        elif class_type == SQLITE:
            local_dynamic_data_source = DynamicTableDataSource.create_sqlite(data_source=data_source)
        else:
            raise ValueError(f"Unsupported local DataSource class_type: {class_type!r}")

        # drop local tables that are not in registered in the backend anymore (probably have been deleted)
        remote_node_storages = DataNodeStorage.filter(
            data_source__uid=local_dynamic_data_source.uid,
            list_tables=True,
        )
        remote_table_names = [t.storage_hash for t in remote_node_storages]
        db_interface = _local_data_interface(class_type)
        local_table_names = db_interface.list_tables()

        tables_to_delete_locally = set(local_table_names) - set(remote_table_names)
        for table_name in tables_to_delete_locally:
            logger.debug(f"Deleting table in local {class_type} db {table_name}")
            db_interface.drop_table(table_name)

        tables_to_delete_remotely = set(remote_table_names) - set(local_table_names)
        for remote_table in remote_node_storages:
            if remote_table.storage_hash in tables_to_delete_remotely:
                logger.debug(f"Deleting table remotely {remote_table.storage_hash}")
                if remote_table.protect_from_deletion:
                    remote_table.patch(protect_from_deletion=False)

                remote_table.delete()

        self.data_source = local_dynamic_data_source

        physical_ds = self.data_source.related_resource
        if class_type == DUCK_DB:
            banner = (
                "─" * 40 + "\n"
                f"LOCAL: {physical_ds.display_name} (engine={physical_ds.class_type})\n\n"
                "import duckdb, pathlib\n"
                f"path = pathlib.Path('{db_interface.db_path}') / 'duck_meta.duckdb'\n"
                "conn = duckdb.connect(':memory:')\n"
                "conn.execute(f\"ATTACH '{path}' AS ro (READ_ONLY)\")\n"
                "conn.execute('INSTALL ui; LOAD ui; CALL start_ui();')\n" + "─" * 40
            )
        else:
            banner = (
                "─" * 40 + "\n"
                f"LOCAL: {physical_ds.display_name} (engine={physical_ds.class_type})\n\n"
                f"sqlite file: {db_interface.db_file}\n" + "─" * 40
            )
        logger.info(banner)

    def __repr__(self):
        return f"{self.data_source.related_resource}"


def get_session_data_source() -> Any:
    data_source = getattr(SessionDataSource, "data_source", None)
    if getattr(data_source, "related_resource", None) is None:
        raise Exception("This Pod does not have a default data source")
    return data_source


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

class Secret(ShareableObjectMixin, BasePydanticModel, BaseObjectOrm):
    FILTERSET_FIELDS: ClassVar[dict[str, list[str]]] = {
        "uid": ["in", "exact"],
        "name": ["in", "exact"],
    }
    FILTER_VALUE_NORMALIZERS: ClassVar[dict[str, str]] = {
        "uid": "uid",
        "name": "str",
        "name__in": "str",
    }

    uid: str | None = Field(None, description="Public uid of the secret.")
    name: str = Field(..., description="Secret name")
    value: SecretStr | None = Field(
        None,
        description="Secret value. The create endpoint may omit it in the response.",
        exclude=True,
    )

    @classmethod
    def create(
        cls,
        *,
        name: str,
        value: str,
        timeout: int | None = None,
    ) -> Secret:
        """
        Create a secret.

        Parameters
        ----------
        name:
            Secret name.
        value:
            Secret value. Stored as a string by the backend. The backend create
            response may omit the secret uid and value, so the returned model can
            have `uid=None` and `value=None`.
        timeout:
            Optional request timeout in seconds.
        """
        return super().create(name=name, value=value, timeout=timeout)


class Constant(ShareableObjectMixin, BasePydanticModel, BaseObjectOrm):
    """
    Simple organization-level constant.
    """

    FILTERSET_FIELDS: ClassVar[dict[str, list[str]]] = {
        "uid": ["in", "exact"],
        "name": ["in", "exact"],
    }
    FILTER_VALUE_NORMALIZERS: ClassVar[dict[str, str]] = {
        "uid": "uid",
        "name": "str",
        "name__in": "str",
    }

    uid: str | None = Field(None, description="Public UID of the constant.")

    name: str = Field(
        ...,
        max_length=255,
        description="UPPER_SNAKE_CASE; optional category via double-underscore, e.g. 'CURVE__US_TREASURIES'.",
    )
    value: Any = Field(
        ...,
        description="Small JSON value (string/number/bool/object/array). Keep it small (e.g., <=10KB).",
    )
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
    def get_value(cls, name: str):
        return cls.get(name=name).value

    @classmethod
    def create(
        cls,
        *,
        name: str,
        value: Any,
        timeout: int | None = None,
    ) -> Constant:
        """
        Create a constant.

        Parameters
        ----------
        name:
            Constant name in UPPER_SNAKE_CASE. When the name contains `__`,
            the prefix before the first double underscore is used as the
            display category, for example `ASSETS__MASTER`.
        value:
            Small JSON-serializable value.
        timeout:
            Optional request timeout in seconds.
        """
        return super().create(name=name, value=value, timeout=timeout)

    @classmethod
    def invalidate_filter_cache(cls) -> None:
        cls._filter_cache.clear()

    @classmethod
    def create_constants_if_not_exist(cls, constants_to_create: dict):
        # crete global constants if not exist in  backed


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
