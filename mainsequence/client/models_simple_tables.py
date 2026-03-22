from __future__ import annotations

import base64
import datetime
import gzip
import json
import math
import time
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar

import numpy as np
import pandas as pd
import pytz
import requests
from pydantic import BaseModel, Field

from mainsequence import logger

from .base import BaseObjectOrm, BasePydanticModel
from .exceptions import raise_for_response
from .models_tdag import (
    POD_PROJECT,
    DynamicTableDataSource,
    Scheduler,
    SourceTableConfiguration,
    _executor,
    get_chunk_stats,
    request_to_datetime,
)
from .utils import API_ENDPOINT, loaders, make_request, serialize_to_json, session

if TYPE_CHECKING:
    from mainsequence.tdag.simple_tables.models import SimpleTable


class SimpleTableClientMixin:
    ROOT_URL: ClassVar[str] = API_ENDPOINT
    LOADERS: ClassVar[Any] = loaders
    ENDPOINT: ClassVar[str] = "ts_manager/simple_tables"
    UPSERT_ENDPOINT: ClassVar[str] = "ts_manager/simple_tables_update"

    @classmethod
    def build_session(cls):
        return session

    @classmethod
    def get_object_url(cls) -> str:
        return f"{cls.ROOT_URL.rstrip('/')}/{cls.ENDPOINT.strip('/')}"

    @classmethod
    def get_upsert_url(cls) -> str:
        return f"{cls.ROOT_URL.rstrip('/')}/{cls.UPSERT_ENDPOINT.strip('/')}"

    @classmethod
    def _serialize_record_payload(
        cls,
        record: SimpleTable | dict[str, Any],
    ) -> dict[str, Any]:
        if isinstance(record, cls):
            return serialize_to_json(record.model_dump(mode="python"))
        if isinstance(record, BaseModel):
            return serialize_to_json(record.model_dump(mode="python"))
        if isinstance(record, dict):
            return serialize_to_json(record)
        raise TypeError(f"Unsupported record type for {cls.__name__}: {type(record)!r}")

    @classmethod
    def _validate_record_response_payload(cls, payload: Any) -> list[SimpleTable]:
        if isinstance(payload, dict):
            if isinstance(payload.get("results"), list):
                payload = payload["results"]
            elif isinstance(payload.get("records"), list):
                payload = payload["records"]
            else:
                payload = [payload]

        if not isinstance(payload, list):
            raise TypeError(
                f"Expected list or dict response for {cls.__name__} records, got {type(payload)!r}."
            )

        return [cls.model_validate(item) for item in payload]

    @classmethod
    def insert_records(
        cls,
        records: Sequence[SimpleTable | dict[str, Any]],
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> list[SimpleTable]:
        url = f"{cls.get_object_url().rstrip('/')}/"
        payload = {
            "json": [cls._serialize_record_payload(record) for record in records],
        }
        response = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="POST",
            url=url,
            payload=payload,
            time_out=timeout,
        )
        if response.status_code not in (200, 201):
            raise_for_response(response, payload=payload)
        return cls._validate_record_response_payload(response.json())

    @classmethod
    def upsert_records(
        cls,
        records: Sequence[SimpleTable | dict[str, Any]],
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> list[SimpleTable]:
        url = f"{cls.get_upsert_url().rstrip('/')}/"
        payload = {
            "json": [cls._serialize_record_payload(record) for record in records],
        }
        response = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="POST",
            url=url,
            payload=payload,
            time_out=timeout,
        )
        if response.status_code not in (200, 201):
            raise_for_response(response, payload=payload)
        return cls._validate_record_response_payload(response.json())

    @classmethod
    def delete_by_id(
        cls,
        record_id: Any,
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> None:
        url = f"{cls.get_object_url().rstrip('/')}/{record_id}/"
        response = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="DELETE",
            url=url,
            payload={},
            time_out=timeout,
        )
        if response.status_code != 204:
            raise_for_response(response)

    def delete(
        self,
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> None:
        record_id = getattr(self, "id", None)
        if record_id is None:
            raise ValueError(
                f"{type(self).__name__} must have an id before calling delete()."
            )
        type(self).delete_by_id(record_id, timeout=timeout)



class SimpleTableRunConfiguration(BasePydanticModel, BaseObjectOrm):
    ENDPOINT: ClassVar[str] = "ts_manager/simple_tables_run_configuration"
    update_schedule: str = "*/1 * * * *"

    @classmethod
    @property
    def ROOT_URL(cls):
        return None

class SimpleTableUpdateHistorical(BasePydanticModel, BaseObjectOrm):
    ENDPOINT: ClassVar[str] = "ts_manager/simple_tables_update_historical"
    id: int | None = None
    related_table: int  # Assuming you're using the ID of the related table
    update_time_start: datetime.datetime
    update_time_end: datetime.datetime | None = None
    error_on_update: bool = False
    trace_id: str | None = Field(default=None, max_length=255)
    updated_by_user: int | None = None  # Assuming you're using the ID of the user


    # extra fields for local control
    must_update: bool | None
    direct_dependencies_ids: list[int] | None

class SimpleTableUpdate(BasePydanticModel, BaseObjectOrm):
    ENDPOINT: ClassVar[str] = "ts_manager/simple_tables_update"
    READ_QUERY_PARAMS: ClassVar[dict[str, str]] = {
        "include_relations_detail": "bool",
    }
    READ_QUERY_PARAM_DESCRIPTIONS: ClassVar[dict[str, str]] = {
        "include_relations_detail": (
            "Expand related objects in the serializer response. "
            "This changes response detail only and does not change which rows are returned."
        ),
    }

    id: int | None = Field(None, description="Primary key, auto-incremented ID")
    update_hash: str = Field(..., max_length=63, description="Max length of PostgreSQL table name")
    simple_table: int | SimpleTable = Field(..., description="Simple table")
    build_configuration: dict[str, Any] = Field(..., description="Configuration in JSON format")
    ogm_dependencies_linked: bool = Field(default=False, description="OGM dependencies linked flag")
    tags: list[str] | None = Field(default=[], description="List of tags")
    description: str | None = Field(None, description="Optional HTML description")
    update_details: SimpleTableUpdateDetails | int | None = None
    run_configuration: SimpleTableRunConfiguration | None = None
    open_for_everyone: bool = Field(
        default=False, description="Whether the ts is open for everyone"
    )

    @property
    def data_source_id(self):
        if isinstance(self.simple_table.data_source, int):
            return self.simple_table.data_source
        else:
            return self.simple_table.data_source.id

    @classmethod
    def get_or_create(cls, **kwargs):
        url = cls.get_object_url() + "/get_or_create/"
        kwargs = serialize_to_json(kwargs)
        pod_project=POD_PROJECT
        kwargs["current_project_id"]=pod_project.id
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



        hu = SimpleTableUpdateHistorical(
            **result["historical_update"],

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
    def create_historical_update(cls, *args, **kwargs):
        s = cls.build_session()
        base_url = SimpleTableUpdateHistorical.get_object_url()
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
            SimpleTableClientMixin.get_object_url()
            + f"/{storage_hash}/get_upstream_nodes?data_source_id={data_source_id}"
        )
        r = make_request(s=s, loaders=cls.LOADERS, r_type="GET", url=url, time_out=timeout)
        if r.status_code != 200:
            raise Exception(f"Error in request {r.text}")

        depth_df = pd.DataFrame(r.json())
        return depth_df

    @classmethod
    def create(cls, timeout=None, *args, **kwargs):
        url = SimpleTableClientMixin.get_object_url() + "/"
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
            raise_for_response(r)

    @classmethod
    def post_data_frame_in_chunks(
        cls,
        serialized_data_frame: pd.DataFrame,
        chunk_size: int = 50_000,
        data_node_update: SimpleTableUpdate = None,
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
        r["state_data"] = {int(k): SimpleTableUpdateDetails(**v) for k, v in r["state_data"].items()}
        r["all_index_stats"] = {int(k): v for k, v in r["all_index_stats"].items()}
        r["data_node_updates"] = [SimpleTableUpdate(**v) for v in r["local_metadatas"]]
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


class SimpleTableUpdateDetails(BasePydanticModel, BaseObjectOrm):
    ENDPOINT: ClassVar[str] = "ts_manager/simple_tables_update_details"
    related_table: int | SimpleTable | None = Field(None, description="Related table")
    active_update: bool = Field(default=False, description="Flag to indicate if update is active")
    update_pid: int = Field(default=0, description="Process ID of the update")
    error_on_last_update: bool = Field(
        default=False, description="Flag to indicate if there was an error in the last update"
    )
    last_update: datetime.datetime | None = Field(None, description="Timestamp of the last update")
    next_update: datetime.datetime | None = Field(None, description="Timestamp of the next update")

    active_update_status: str = Field(
        default="Q", max_length=20, description="Current update status"
    )
    active_update_scheduler: int | Scheduler | None = Field(
        None, description="Scheduler  for active update"
    )
    update_priority: int = Field(default=0, description="Priority level of the update")
    last_updated_by_user: int | None = Field(None, description="Foreign key reference to User")

    run_configuration: SimpleTableRunConfiguration | None = None

    @staticmethod
    def _parse_parameters_filter(parameters):
        for key, value in parameters.items():
            if "__in" in key:
                assert isinstance(value, list)
                parameters[key] = ",".join(value)
        return parameters


__all__ = [
    "SimpleTableClientMixin",
    "SimpleTableRunConfiguration",
    "SimpleTableUpdate",
    "SimpleTableUpdateDetails",
    "SimpleTableUpdateHistorical",
]
