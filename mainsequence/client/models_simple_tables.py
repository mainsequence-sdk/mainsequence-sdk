from __future__ import annotations

import base64
import datetime
import gzip
import json
import math
import time
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, ClassVar

import numpy as np
import pandas as pd
import pytz
import requests
from pydantic import AliasChoices, ConfigDict, Field

from mainsequence import logger

from .base import BaseObjectOrm, BasePydanticModel, LabelableObjectMixin
from .exceptions import raise_for_response
from .models_tdag import (
    POD_PROJECT,
    AbstractTable,
    BaseColumnMetaData,
    BaseUpdateDetails,
    BaseUpdateStatistics,
    DynamicTableDataSource,
    HistoricalUpdateRecord,
    SourceTableConfigurationBase,
    TableUpdateNode,
    UpdateBatchResponse,
    UpdateNodeRef,
    _executor,
    get_chunk_stats,
    request_to_datetime,
)
from .utils import make_request, serialize_to_json

if TYPE_CHECKING:
    from mainsequence.tdag.filters import BaseSearchRequest
    from mainsequence.tdag.simple_tables.models import SimpleTable


class SimpleTableColumnPayload(BasePydanticModel):
    id: int | None = Field(
        None,
        description="Primary key of the column metadata row.",
    )
    attr_name: str = Field(
        ...,
        description="Logical attribute name for the column in the simple-table schema.",
    )
    column_name: str = Field(
        ...,
        description="Physical column name stored in the backing table.",
    )
    db_type: str = Field(
        ...,
        description="Declared data type for the column.",
    )
    is_pk: bool = Field(
        False,
        description="Whether the column is part of the primary key.",
    )
    nullable: bool = Field(
        False,
        description="Whether the column accepts null values.",
    )
    is_unique: bool = Field(
        False,
        description="Whether the column has a uniqueness constraint.",
    )


class SimpleTableForeignKeyPayload(BasePydanticModel):
    id: int | None = Field(
        None,
        description="Primary key of the foreign-key metadata row.",
    )
    source_column: str = Field(
        ...,
        description="Column on the source simple table that points to the target table.",
    )
    target_table: int = Field(
        ...,
        description="Primary key of the target simple table referenced by this foreign key.",
    )
    target_column: str = Field(
        ...,
        description="Column on the target simple table that is referenced.",
    )
    on_delete: str = Field(
        ...,
        description="Deletion behavior recorded for the foreign-key relationship.",
    )


class SimpleTableIncomingForeignKeyPayload(SimpleTableForeignKeyPayload):
    pass


class SimpleTableIndexMetaPayload(BasePydanticModel):
    id: int | None = Field(
        None,
        description="Primary key of the index metadata row.",
    )
    name: str = Field(
        ...,
        description="Index name.",
    )
    columns: list[str] = Field(
        default_factory=list,
        description="Ordered list of column names included in the index.",
    )


def _json_default(value: Any) -> Any:
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()
    if hasattr(value, "model_dump"):
        try:
            return value.model_dump(mode="json", exclude_none=True)
        except TypeError:
            return value.model_dump()
    return str(value)


def _serialize_records_for_upload(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    sanitized_records = [
        {
            key: value
            for key, value in record.items()
            if not (key == "id" and value is None)
        }
        for record in records
    ]
    return json.loads(
        json.dumps(
            serialize_to_json({"records": sanitized_records})["records"],
            default=_json_default,
        )
    )


def _compress_records_payload(records: list[dict[str, Any]]) -> str:
    chunk_json_str = json.dumps(records)
    compressed = gzip.compress(chunk_json_str.encode("utf-8"))
    return base64.b64encode(compressed).decode("utf-8")


def _insert_records_in_chunks(
    *,
    owner_label: str,
    url: str,
    records: list[dict],
    overwrite: bool,
    chunk_size: int,
    timeout: int | float | tuple[float, float] | None,
    add_insertion_time: bool = False,
) -> None:
    del add_insertion_time

    serialized_records = _serialize_records_for_upload(records)
    if not serialized_records:
        logger.info("No records to upload.")
        return

    s = SimpleTableStorage.build_session()

    def _build_payload(
        records_chunk: list[dict[str, Any]],
        *,
        chunk_index: int,
        total_chunks: int,
    ) -> dict[str, Any]:
        return {
            "json": {
                "data": _compress_records_payload(records_chunk),
                "chunk_stats": None,
                "overwrite": overwrite,
                "chunk_index": chunk_index,
                "total_chunks": total_chunks,
            }
        }

    def _send_chunk_recursively(
        records_chunk: list[dict[str, Any]],
        chunk_idx: int,
        total_chunks: int,
        *,
        is_sub_chunk: bool = False,
    ) -> None:
        if not records_chunk:
            return

        part_label = (
            f"{chunk_idx + 1}/{total_chunks}"
            if not is_sub_chunk
            else f"sub-chunk of {chunk_idx + 1}"
        )
        payload = _build_payload(
            records_chunk,
            chunk_index=0 if is_sub_chunk else chunk_idx,
            total_chunks=1 if is_sub_chunk else total_chunks,
        )

        try:
            response = make_request(
                s=s,
                loaders=None,
                payload=payload,
                r_type="POST",
                url=url,
                time_out=timeout,
            )
        except requests.exceptions.RequestException as exc:
            logger.exception(f"Error uploading {owner_label} chunk {part_label}: {exc}")
            raise

        if response.status_code in [200, 204]:
            logger.info("Chunk uploaded successfully.")
            return

        if response.status_code == 413:
            logger.warning(
                f"Chunk {part_label} ({len(records_chunk)} rows) is too large (413). "
                "Splitting in half and retrying as new uploads."
            )
            if len(records_chunk) <= 1:
                raise Exception(
                    f"A single row from chunk {part_label} is too large to upload."
                )

            mid_point = len(records_chunk) // 2
            _send_chunk_recursively(
                records_chunk[:mid_point],
                chunk_idx,
                total_chunks,
                is_sub_chunk=True,
            )
            _send_chunk_recursively(
                records_chunk[mid_point:],
                chunk_idx,
                total_chunks,
                is_sub_chunk=True,
            )
            return

        raise_for_response(response, payload=payload)

    total_rows = len(serialized_records)
    effective_chunk_size = chunk_size if chunk_size > 0 else total_rows
    total_chunks = math.ceil(total_rows / effective_chunk_size) if total_rows > 0 else 1

    for chunk_idx in range(total_chunks):
        start_idx = chunk_idx * effective_chunk_size
        end_idx = min((chunk_idx + 1) * effective_chunk_size, total_rows)
        _send_chunk_recursively(
            serialized_records[start_idx:end_idx],
            chunk_idx,
            total_chunks,
        )


class STColumnMetaData(BaseColumnMetaData, BaseObjectOrm):
    source_config_id: int | None = Field(
        None,
        description="Primary key of the related STSourceTableConfiguration",
    )


class SimpleTableStorage(AbstractTable, LabelableObjectMixin, BasePydanticModel, BaseObjectOrm):
    ENDPOINT: ClassVar[str] = "ts_manager/simple_table"
    model_config = ConfigDict(populate_by_name=True)
    FILTERSET_FIELDS: ClassVar[dict[str, list[str]]] = {
        "storage_hash": ["in", "exact", "contains"],
        "identifier": ["in", "exact", "contains"],
        "id": ["in", "exact", "contains"],
        "data_source__id": ["in", "exact"],
        "namespace": ["exact", "contains", "in", "isnull"],
        "labels": ["exact", "in", "contains"],
    }
    FILTER_VALUE_NORMALIZERS: ClassVar[dict[str, str]] = {
        "id": "id",
        "id__in": "id",
        "data_source__id": "id",
        "labels": "str",
        "labels__in": "str",
        "labels__contains": "str",
    }

    id: int | None = Field(None, description="Primary key, auto-incremented ID")
    source_class_name: str | None = None
    data_source: int | DynamicTableDataSource | dict[str, Any] | None = None
    simple_table_schema: dict[str, Any] | None = Field(
        None,
        alias="schema",
        description="Canonical simple-table schema",
    )
    columns: list[SimpleTableColumnPayload] = Field(
        default_factory=list,
        description="Column metadata exposed by the backend for this simple table.",
    )
    foreign_keys: list[SimpleTableForeignKeyPayload] = Field(
        default_factory=list,
        description="Outgoing foreign-key relations exposed by the backend for this simple table.",
    )
    incoming_fks: list[SimpleTableIncomingForeignKeyPayload] = Field(
        default_factory=list,
        description="Incoming foreign-key relations exposed by the backend for this simple table.",
    )
    indexes_meta: list[SimpleTableIndexMetaPayload] = Field(
        default_factory=list,
        description="Index metadata exposed by the backend for this simple table.",
    )
    sourcetableconfiguration: STSourceTableConfiguration | None = None
    build_configuration: dict[str, Any] | None = Field(
        None, description="Storage/build configuration in JSON format"
    )
    time_serie_source_code_git_hash: str | None = Field(
        None, max_length=255, description="Git hash of the simple-table updater source code"
    )
    time_serie_source_code: str | None = Field(
        None, description="Source code for the simple-table updater"
    )
    labels: list[str] = Field(
        default_factory=list,
        description=(
            "Organizational labels attached to the simple table. "
            "These are helpers for grouping and discovery only and do not change runtime behavior or functionality."
        ),
    )

    open_for_everyone: bool = Field(
        default=False, description="Whether the table is open for everyone"
    )
    data_source_open_for_everyone: bool = Field(
        default=False, description="Whether the data source is open for everyone"
    )
    creation_date: datetime.datetime | None = Field(None, description="Creation timestamp")
    created_by_user: int | None = Field(None, description="Foreign key reference to User")
    organization_owner: int | None = Field(None, description="Foreign key reference to Organization")

    @classmethod
    def get_or_create(cls, **kwargs):
        kwargs = serialize_to_json(kwargs)
        url = cls.get_object_url() + "/get_or_create/"
        payload = {"json": kwargs}
        s = cls.build_session()
        r = make_request(s=s, loaders=cls.LOADERS, r_type="POST", url=url, payload=payload)
        if r.status_code not in [201, 200]:
            raise_for_response(r, payload=payload)

        return cls(**r.json())

    @classmethod
    def get_records_url(cls) -> str:
        return f"{cls.ROOT_URL.rstrip('/')}/ts_manager/simple_tables"

    @classmethod
    def get_records_upsert_url(cls) -> str:
        return f"{cls.ROOT_URL.rstrip('/')}/ts_manager/simple_tables/update"

    @classmethod
    def insert_records(
        cls,
        record_model: type[SimpleTable],
        records: Sequence[SimpleTable | dict[str, Any]],
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> list[SimpleTable]:
        url = f"{cls.get_records_url().rstrip('/')}/"
        payload = {
            "json": [
                serialize_to_json(record_model.serialize_record_payload(record))
                for record in records
            ],
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
        return record_model.validate_record_response_payload(response.json())

    @classmethod
    def upsert_records(
        cls,
        record_model: type[SimpleTable],
        records: Sequence[SimpleTable | dict[str, Any]],
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> list[SimpleTable]:
        url = f"{cls.get_records_upsert_url().rstrip('/')}/"
        payload = {
            "json": [
                serialize_to_json(record_model.serialize_record_payload(record))
                for record in records
            ],
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
        return record_model.validate_record_response_payload(response.json())

    @classmethod
    def delete_by_id(
        cls,
        record_id: Any,
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> None:
        url = f"{cls.get_records_url().rstrip('/')}/{record_id}/"
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

    @classmethod
    def delete_record(
        cls,
        record: Any,
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> None:
        record_id = getattr(record, "id", None)
        if record_id is None:
            raise ValueError("delete_record(...) requires a record with an 'id' field value.")
        cls.delete_by_id(record_id, timeout=timeout)

    @classmethod
    def insert_records_into_table(
        cls,
        simple_table_id: int,
        records: list[dict[str, Any]],
        overwrite: bool = True,
        add_insertion_time: bool = False,
        *,
        chunk_size: int = 50_000,
        timeout: int | float | tuple[float, float] | None = 60 * 15,
    ) -> None:
        url = cls.get_object_url() + f"/{simple_table_id}/insert_records_into_table/"
        _insert_records_in_chunks(
            owner_label=f"simple table {simple_table_id}",
            url=url,
            records=records,
            overwrite=overwrite,
            chunk_size=chunk_size,
            timeout=timeout,
            add_insertion_time=add_insertion_time,
        )

    @classmethod
    def upsert_records_into_table(
        cls,
        simple_table_id: int,
        records: list[dict[str, Any]],
        *,
        timeout: int | float | tuple[float, float] | None = 60 * 15,
    ) -> None:
        """Perform sparse upserts into an existing simple table.

        This endpoint is intended for sparse insertion workloads: the table must
        define a primary key, every input record must include all primary-key
        columns, and records may provide different non-primary-key fields.

        The request body is sent as ``{"data": "<base64+gzip encoded JSON list>"}``.
        """
        serialized_records = _serialize_records_for_upload(records)
        if not serialized_records:
            logger.info("No records to upsert.")
            return

        url = cls.get_object_url() + f"/{simple_table_id}/upsert_records_into_table/"
        payload = {
            "json": {
                "data": _compress_records_payload(serialized_records),
            }
        }
        response = make_request(
            s=cls.build_session(),
            loaders=None,
            r_type="POST",
            url=url,
            payload=payload,
            time_out=timeout,
        )
        if response.status_code not in (200, 204):
            raise_for_response(response, payload=payload)

    @classmethod
    def delete_records_from_table(
        cls,
        data_node_storage_id: int,
        records_ids: list[int],
        *,
        timeout: int | float | tuple[float, float] | None = 60 * 15,
    ):
        if not records_ids:
            return

        s = cls.build_session()
        url = cls.get_object_url()  + f"/{data_node_storage_id}/delete_records_from_table/"
        payload = {"json": {"records_ids": records_ids}}

        response = make_request(
            s=s,
            loaders=None,
            payload=payload,
            r_type="POST",
            url=url,
            time_out=timeout,
        )
        if response.status_code not in [200, 204]:
            raise_for_response(response, payload=payload)

    @classmethod
    def get_data_from_filter(
        cls,
        filter_request: BaseSearchRequest,
        *,
        batch_limit: int = 14000,
    ) -> list[dict[str, Any]]:
        url = cls.get_object_url() + "/get-data-from-filter/"
        s = cls.build_session()

        offset = int(filter_request.offset or 0)
        all_results: list[dict[str, Any]] = []

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
                err = r.json().get("error") or {}
                msg=f"[{err.get('kind')}:{err.get('code')}] {err.get('message')}"
                logger.exception(msg)
                raise RuntimeError(
                    msg
                )

            response_data = r.json() or {}
            if isinstance(response_data, list):
                all_results.extend(response_data)
                break

            chunk = response_data.get("results", []) or []
            all_results.extend(chunk)

            next_offset = response_data.get("next_offset")
            if not next_offset:
                break
            offset = int(next_offset)

        return all_results

class STSourceTableConfiguration(SourceTableConfigurationBase,BasePydanticModel, BaseObjectOrm):
    ENDPOINT: ClassVar[str] = "ts_manager/simple_tables/source_table_configuration"

    related_table: int | SimpleTableStorage | None = Field(None, description="Related table")
    columns_metadata: list[STColumnMetaData] | None = None

    def set_or_update_columns_metadata(
        self,
        columns_metadata: list[STColumnMetaData],
        timeout: int | float | tuple[float, float] | None = None,
    ) -> Any:
        del columns_metadata, timeout
        return None

    def get_data_updates(self) -> BaseUpdateStatistics:
        #no op
        return BaseUpdateStatistics()

class SimpleTableRunConfiguration(BasePydanticModel, BaseObjectOrm):
    ENDPOINT: ClassVar[str] = "ts_manager/simple_table/run_configuration"
    update_schedule: str = "*/1 * * * *"

    @classmethod
    @property
    def ROOT_URL(cls):
        return None

class SimpleTableUpdateRecord(HistoricalUpdateRecord,BasePydanticModel, BaseObjectOrm):
    ENDPOINT: ClassVar[str] = "ts_manager/simple_table/update_historical"

    related_table: int | SimpleTableStorage


SimpleTableUpdateHistorical = SimpleTableUpdateRecord


class SimpleTableUpdate(TableUpdateNode, BaseObjectOrm):
    model_config = ConfigDict(extra="forbid")
    ENDPOINT: ClassVar[str] = "ts_manager/simple_table/update"
    FILTERSET_FIELDS: ClassVar[dict[str, list[str]]] = {
        "update_hash": ["exact"],
        "remote_table__data_source__id": ["exact"],
        "related_table__namespace": ["contains", "in", "isnull"],
    }
    FILTER_VALUE_NORMALIZERS: ClassVar[dict[str, str]] = {
        "remote_table__data_source__id": "id",
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
    NODE_TYPE: ClassVar[str] = "simple_table_update"

    remote_table: int | SimpleTableStorage = Field(
        ...,
        validation_alias=AliasChoices("remote_table", "simple_table"),
        description="Simple table storage referenced by this update.",
    )
    tags: list[str] | None = Field(default=[], description="List of tags")
    description: str | None = Field(None, description="Optional HTML description")
    update_details: SimpleTableUpdateDetails | int | None = None
    run_configuration: SimpleTableRunConfiguration | None = None
    open_for_everyone: bool = Field(
        default=False, description="Whether the ts is open for everyone"
    )

    @property
    def data_source_id(self):
        if isinstance(self.remote_table, int):
            return None
        if isinstance(self.remote_table.data_source, dict):
            return self.remote_table.data_source.get("id")
        if isinstance(self.remote_table.data_source, int):
            return self.remote_table.data_source
        if self.remote_table.data_source is None:
            return None
        else:
            return self.remote_table.data_source.id

    @property
    def data_node_storage(self):
        return self.remote_table

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
            id_candidates = [
                c
                for c in ["update_node_id", "local_time_serie_id", "data_node_update_id"]
                if c in depth_df.columns
            ]

            if id_candidates:
                update_node_id = None
                for col in id_candidates:
                    series = depth_df[col]
                    if isinstance(series, pd.DataFrame):
                        series = series.iloc[:, 0]
                    update_node_id = (
                        series if update_node_id is None else update_node_id.fillna(series)
                    )

                depth_df = depth_df.drop(
                    columns=["update_node_id", "local_time_serie_id", "data_node_update_id"],
                    errors="ignore",
                )
                depth_df["update_node_id"] = update_node_id

        return depth_df

    @classmethod
    def get_upstream_nodes(cls, storage_hash, data_source_id, timeout=None):
        s = cls.build_session()
        url = (
            SimpleTableStorage.get_records_url()
            + f"/{storage_hash}/get_upstream_nodes?data_source_id={data_source_id}"
        )
        r = make_request(s=s, loaders=cls.LOADERS, r_type="GET", url=url, time_out=timeout)
        if r.status_code != 200:
            raise Exception(f"Error in request {r.text}")

        depth_df = pd.DataFrame(r.json())
        return depth_df

    @classmethod
    def create(cls, timeout=None, *args, **kwargs):
        url = SimpleTableStorage.get_records_url() + "/"
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
    def insert_records_into_table(
        cls,
        data_node_update_id,
        records: list[dict],
        overwrite: bool = True,
        add_insertion_time: bool = False,
        *,
        chunk_size: int = 50_000,
        timeout: int | float | tuple[float, float] | None = 60 * 15,
    ):
        url = cls.get_object_url() + f"/{data_node_update_id}/insert_records_into_table/"
        _insert_records_in_chunks(
            owner_label=f"simple table update {data_node_update_id}",
            url=url,
            records=records,
            overwrite=overwrite,
            chunk_size=chunk_size,
            timeout=timeout,
            add_insertion_time=add_insertion_time,
        )

    @classmethod
    def insert_data_into_table(
        cls,
        data_node_update_id,
        records: list[dict],
        overwrite: bool = True,
        add_insertion_time: bool = False,
        *,
        chunk_size: int = 50_000,
        timeout: int | float | tuple[float, float] | None = 60 * 15,
    ):
        return cls.insert_records_into_table(
            data_node_update_id=data_node_update_id,
            records=records,
            overwrite=overwrite,
            add_insertion_time=add_insertion_time,
            chunk_size=chunk_size,
            timeout=timeout,
        )

    @classmethod
    def get_data_nodes_and_set_updates(
        cls,
        update_nodes: Sequence[UpdateNodeRef],
        update_details_kwargs: Mapping[str, Any],
        update_priority_dict: Mapping[int, int] | None,
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
            int(k): STSourceTableConfiguration(**v) if v is not None else v
            for k, v in response_json["source_table_config_map"].items()
        }
        state_data = {
            int(k): SimpleTableUpdateDetails(**v) for k, v in response_json["state_data"].items()
        }
        all_index_stats = {int(k): v for k, v in response_json["all_index_stats"].items()}
        data_node_updates = [SimpleTableUpdate(**v) for v in response_json["local_metadatas"]]
        return UpdateBatchResponse[
            SimpleTableUpdate,
            SimpleTableUpdateDetails,
            STSourceTableConfiguration,
        ](
            source_table_config_map=source_table_config_map,
            state_data=state_data,
            all_index_stats=all_index_stats,
            data_node_updates=data_node_updates,
        )

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
        data: Any,
        data_source: DynamicTableDataSource | None = None,
        overwrite: bool = False,
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


class SimpleTableUpdateDetails(BaseUpdateDetails,BasePydanticModel, BaseObjectOrm):
    ENDPOINT: ClassVar[str] = "ts_manager/simple_tables_update_details"
    related_table: int | SimpleTableStorage | None = Field(None, description="Related table")
    run_configuration: SimpleTableRunConfiguration | None = None

    @staticmethod
    def _parse_parameters_filter(parameters):
        for key, value in parameters.items():
            if "__in" in key:
                assert isinstance(value, list)
                parameters[key] = ",".join(value)
        return parameters






__all__ = [
    "SimpleTableStorage",
    "SimpleTableRunConfiguration",
    "SimpleTableUpdate",
    "SimpleTableUpdateDetails",
    "SimpleTableUpdateHistorical",
]
