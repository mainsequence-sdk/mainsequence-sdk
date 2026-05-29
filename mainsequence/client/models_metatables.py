from __future__ import annotations

import datetime
import json
from collections.abc import Mapping, Sequence
from typing import Any, ClassVar, Literal

import pandas as pd
from pydantic import AliasChoices, ConfigDict, Field, field_validator, model_validator

from mainsequence.logconf import logger

from .base import BaseObjectOrm, BasePydanticModel, LabelableObjectMixin, ShareableObjectMixin
from .data_sources_interfaces import get_duckdb_interface_class, get_sqlite_interface_class
from .dtype_codec import (
    DATE,
    TIMESTAMP_TZ,
    normalize_column_dtypes_map,
    normalize_dtype_token,
    serialize_remote_parameters,
    token_to_pandas_series,
)
from .exceptions import raise_for_response
from .utils import UniqueIdentifierRangeMap, bios_uuid, make_request, serialize_to_json

DUCK_DB = "duck_db"
SQLITE = "sqlite"
LOCAL_DATA_SOURCE_CLASS_TYPES = {DUCK_DB, SQLITE}


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


def _storage_time_indexed_contract(storage: Any) -> tuple[str, list[str], dict[str, Any]]:
    if hasattr(storage, "_require_time_indexed_table_contract"):
        return storage._require_time_indexed_table_contract()

    def _field(obj: Any, name: str, default: Any = None) -> Any:
        if isinstance(obj, Mapping):
            return obj.get(name, default)
        return getattr(obj, name, default)

    profile = _field(storage, "time_indexed_profile")
    if profile is not None:
        time_index_name = _field(profile, "time_index_name")
        index_names = _field(profile, "index_names")
        column_dtypes_map = _field(profile, "column_dtypes_map")
        if time_index_name and index_names and column_dtypes_map:
            return str(time_index_name), [str(name) for name in index_names], dict(column_dtypes_map)

    columns = _field(storage, "columns")
    if columns:
        column_dtypes_map = {
            str(_field(column, "name")): _field(column, "data_type")
            for column in columns
            if _field(column, "name")
        }
        table_contract = _field(storage, "table_contract", {})
        dynamic_contract = (
            table_contract.get("dynamic_table") or {}
            if isinstance(table_contract, Mapping)
            else {}
        )
        index_names = dynamic_contract.get("index_names") or []
        time_index_name = dynamic_contract.get("time_index_name") or (index_names[0] if index_names else None)
        if time_index_name and index_names and column_dtypes_map:
            return str(time_index_name), [str(name) for name in index_names], column_dtypes_map

    raise ValueError("Storage is missing its time-indexed table contract.")

MetaTableManagementMode = Literal["external_registered", "platform_managed"]
MetaTableOperation = Literal["select", "insert", "update", "delete", "upsert"]
COMPILED_SQL_V1 = "compiled-sql.v1"
MetaTableCompiledSQLVersion = Literal["compiled-sql.v1"]
MetaTableCompiledSQLDialect = Literal["postgresql"]
MetaTableCompiledSQLParamstyle = Literal["pyformat"]


def _strip_client_metadata(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            key: _strip_client_metadata(item) for key, item in value.items() if key != "orm_class"
        }
    if isinstance(value, list):
        return [_strip_client_metadata(item) for item in value]
    if isinstance(value, tuple):
        return [_strip_client_metadata(item) for item in value]
    return value


def _payload_json(payload: Mapping[str, Any] | BasePydanticModel) -> dict[str, Any]:
    if isinstance(payload, BasePydanticModel):
        return _strip_client_metadata(
            payload.model_dump(mode="json", by_alias=True, exclude_none=True)
        )

    def convert(value: Any) -> Any:
        if isinstance(value, BasePydanticModel):
            return _strip_client_metadata(
                value.model_dump(mode="json", by_alias=True, exclude_none=True)
            )
        if isinstance(value, Mapping):
            return {key: convert(item) for key, item in value.items()}
        if isinstance(value, list):
            return [convert(item) for item in value]
        if isinstance(value, tuple):
            return [convert(item) for item in value]
        return value

    return _strip_client_metadata(
        serialize_to_json({key: convert(value) for key, value in dict(payload).items()})
    )


def _normalize_backend_type(value: str | None) -> str | None:
    if value is None:
        return None
    raw = str(value)
    token = normalize_dtype_token(raw, remote=True)
    if token == TIMESTAMP_TZ:
        return "TIMESTAMP WITH TIME ZONE"
    if token == DATE:
        return "DATE"
    return raw


def _normalize_contract_mapping(contract: Mapping[str, Any]) -> dict[str, Any]:
    normalized = dict(contract)
    columns = normalized.get("columns")
    if isinstance(columns, list):
        normalized_columns = []
        for column in columns:
            if not isinstance(column, Mapping):
                normalized_columns.append(column)
                continue
            column_dict = dict(column)
            if "data_type" in column_dict:
                column_dict["data_type"] = normalize_dtype_token(
                    column_dict["data_type"],
                    remote=True,
                )
            if "backend_type" in column_dict:
                column_dict["backend_type"] = _normalize_backend_type(
                    column_dict.get("backend_type")
                )
            normalized_columns.append(column_dict)
        normalized["columns"] = normalized_columns
    return normalized


def _temporal_parameter_names(parameters: dict[str, Any] | list[Any]) -> set[str]:
    if isinstance(parameters, Mapping):
        items = parameters.items()
    else:
        items = ((str(index), value) for index, value in enumerate(parameters))
    temporal_names: set[str] = set()
    for name, value in items:
        if isinstance(value, (datetime.date, datetime.datetime)):
            temporal_names.add(str(name))
    return temporal_names


class MetaTablePhysicalContract(BasePydanticModel):
    schema_: str | None = Field(
        default=None,
        alias="schema",
        serialization_alias="schema",
        exclude=True,
        description=(
            "Deprecated input-only schema alias. MetaTable uses the data source " "default schema."
        ),
    )
    table_name: str = Field(..., description="Physical database table name.")

    model_config = ConfigDict(populate_by_name=True)


class MetaTableColumnContract(BasePydanticModel):
    name: str = Field(..., description="Physical column name in the MetaTable.")
    data_type: str = Field(..., description="Portable logical column type.")
    backend_type: str | None = Field(
        default=None,
        description=(
            "Optional backend-specific type hint. Logical UUID columns are emitted "
            "as UUID for platform-managed PostgreSQL tables."
        ),
    )
    nullable: bool = Field(
        default=True,
        description="Whether the column allows database NULL values.",
    )
    primary_key: bool = Field(
        default=False,
        description="Whether the column participates in the physical primary key.",
    )
    unique: bool = Field(
        default=False,
        description="Whether the column has a single-column unique constraint.",
    )
    server_default: str | None = Field(
        default=None,
        description=(
            "Portable database default expression for server-generated values. "
            "For platform-managed PostgreSQL UUID primary keys this is "
            "gen_random_uuid()."
        ),
    )
    description: str | None = Field(
        default=None,
        description="Optional human-readable column description.",
    )
    label: str | None = Field(
        default=None,
        description="Optional display label for UI surfaces.",
    )
    logical_name: str | None = Field(
        default=None,
        description="Optional logical or semantic column name.",
    )

    @field_validator("data_type")
    @classmethod
    def _normalize_data_type(cls, value: str) -> str:
        return normalize_dtype_token(value, remote=True)

    @field_validator("backend_type")
    @classmethod
    def _normalize_backend_type(cls, value: str | None) -> str | None:
        return _normalize_backend_type(value)


class MetaTableIndexContract(BasePydanticModel):
    name: str
    columns: list[str] = Field(default_factory=list)
    unique: bool = False
    method: str | None = None
    expression: str | None = None


class MetaTableForeignKeyContract(BasePydanticModel):
    name: str
    source_columns: list[str] = Field(default_factory=list)
    target_meta_table_uid: str = Field(
        ...,
        validation_alias=AliasChoices("target_meta_table_uid", "targetMetaTableUid"),
    )
    target_columns: list[str] = Field(default_factory=list)
    on_delete: str = "restrict"

    model_config = ConfigDict(populate_by_name=True)


class MetaTableContract(BasePydanticModel):
    version: str = "relational-table.v1"
    physical: MetaTablePhysicalContract
    columns: list[MetaTableColumnContract] = Field(default_factory=list)
    constraints: list[dict[str, Any]] = Field(default_factory=list)
    indexes: list[MetaTableIndexContract] = Field(default_factory=list)
    foreign_keys: list[MetaTableForeignKeyContract] = Field(default_factory=list)
    authoring: dict[str, Any] | None = None


class MetaTableColumnPayload(BasePydanticModel):
    name: str = Field(..., description="Physical column name in the MetaTable.")
    logical_name: str | None = Field(
        default=None,
        description="Optional logical or semantic column name.",
    )
    data_type: str = Field(..., description="Portable logical column type.")
    backend_type: str | None = Field(
        default=None,
        description="Optional backend-specific type hint.",
    )
    nullable: bool = Field(
        default=True,
        description="Whether the column allows database NULL values.",
    )
    primary_key: bool = Field(
        default=False,
        description="Whether the column participates in the physical primary key.",
    )
    unique: bool = Field(
        default=False,
        description="Whether the column has a single-column unique constraint.",
    )
    server_default: str | None = Field(
        default=None,
        description=(
            "Portable database default expression for server-generated values. "
            "For platform-managed PostgreSQL UUID primary keys this is "
            "gen_random_uuid()."
        ),
    )
    ordinal_position: int = Field(
        default=0,
        description="Zero-based column position from the normalized contract.",
    )
    description: str | None = Field(
        default=None,
        description="Optional human-readable column description.",
    )
    label: str | None = Field(
        default=None,
        description="Optional display label for UI surfaces.",
    )
    contract_fragment: dict[str, Any] = Field(
        default_factory=dict,
        description="Raw normalized contract fragment for this projected column.",
    )

    @field_validator("data_type")
    @classmethod
    def _normalize_data_type(cls, value: str) -> str:
        return normalize_dtype_token(value, remote=True)

    @field_validator("backend_type")
    @classmethod
    def _normalize_backend_type(cls, value: str | None) -> str | None:
        return _normalize_backend_type(value)


class MetaTableIndexPayload(BasePydanticModel):
    name: str
    columns: list[str] = Field(default_factory=list)
    unique: bool = False
    method: str | None = None
    expression: str | None = None
    contract_fragment: dict[str, Any] = Field(default_factory=dict)


class MetaTableForeignKeyPayload(BasePydanticModel):
    name: str
    source_columns: list[str] = Field(default_factory=list)
    target_table_uid: str | None = None
    target_table_storage_hash: str | None = None
    target_columns: list[str] = Field(default_factory=list)
    on_delete: str = "restrict"
    contract_fragment: dict[str, Any] = Field(default_factory=dict)


class MetaTableStatementPayload(BasePydanticModel):
    sql: str = Field(..., min_length=1)
    parameters: dict[str, Any] | list[Any] = Field(default_factory=dict)
    parameter_types: dict[str, str] | None = None
    paramstyle: MetaTableCompiledSQLParamstyle = "pyformat"

    @model_validator(mode="after")
    def _normalize_parameters(self) -> MetaTableStatementPayload:
        temporal_names = _temporal_parameter_names(self.parameters)
        if temporal_names:
            missing_types = temporal_names - set(self.parameter_types or {})
            if missing_types:
                raise ValueError(
                    "MetaTable compiled SQL temporal parameters require "
                    f"statement.parameter_types entries. Missing: {sorted(missing_types)}"
                )
        if self.parameter_types:
            self.parameter_types = normalize_column_dtypes_map(self.parameter_types, remote=True)
            self.parameters = serialize_remote_parameters(
                self.parameters,
                self.parameter_types,
            )
        return self


class MetaTableOperationScopeTable(BasePydanticModel):
    meta_table_uid: str = Field(
        ...,
        validation_alias=AliasChoices("meta_table_uid", "metaTableUid"),
    )
    alias: str | None = None
    access: Literal["read", "write"] = "read"

    model_config = ConfigDict(populate_by_name=True)


class MetaTableOperationScope(BasePydanticModel):
    tables: list[MetaTableOperationScopeTable] = Field(..., min_length=1)


class MetaTableOperationLimits(BasePydanticModel):
    max_rows: int | None = Field(default=None, ge=1)
    statement_timeout_ms: int | None = Field(default=None, ge=1)


class MetaTableCompiledSQLOperation(BasePydanticModel):
    operation: MetaTableOperation
    version: MetaTableCompiledSQLVersion = COMPILED_SQL_V1
    dialect: MetaTableCompiledSQLDialect = "postgresql"
    statement: MetaTableStatementPayload
    scope: MetaTableOperationScope
    limits: MetaTableOperationLimits | None = None


class MetaTableRegistrationRequest(BasePydanticModel):
    data_source_uid: str
    management_mode: MetaTableManagementMode
    storage_hash: str = Field(..., max_length=63, description="Canonical table storage hash.")
    table_contract: MetaTableContract | dict[str, Any]
    identifier: str | None = None
    namespace: str | None = None
    description: str | None = None
    protect_from_deletion: bool = False
    open_for_everyone: bool = False
    labels: list[str] = Field(default_factory=list)
    provisioning: dict[str, Any] | None = None
    introspect: bool = False

    @model_validator(mode="after")
    def _normalize_table_contract(self) -> MetaTableRegistrationRequest:
        if isinstance(self.table_contract, Mapping):
            self.table_contract = _normalize_contract_mapping(self.table_contract)
        return self


class MetaTableValidateContractRequest(BasePydanticModel):
    table_contract: MetaTableContract | dict[str, Any]
    management_mode: MetaTableManagementMode | None = None
    storage_hash: str | None = None

    @model_validator(mode="after")
    def _normalize_table_contract(self) -> MetaTableValidateContractRequest:
        if isinstance(self.table_contract, Mapping):
            self.table_contract = _normalize_contract_mapping(self.table_contract)
        return self


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
        data_node_update: Any,
        overwrite: bool,
        time_index_name: str,
        index_names: list,
        grouped_dates: dict,
        column_dtypes_map: Mapping[str, Any] | None = None,
    ):
        if self.class_type in LOCAL_DATA_SOURCE_CLASS_TYPES:
            storage = data_node_update.data_node_storage
            _local_data_interface(self.class_type).upsert(
                df=serialized_data_frame,
                table=getattr(storage, "physical_table_name", None) or storage.storage_hash,
                index_names=index_names,
                time_index_name=time_index_name,
            )
        else:
            from .models_tdag import DataNodeUpdate

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
        data_node_update: Any,
        overwrite: bool,
        time_index_name: str,
        index_names: list,
        grouped_dates: dict,
    ):
        raise NotImplementedError

    def get_data_by_time_index(
        self,
        data_node_update: Any,
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
            storage = data_node_update.data_node_storage
            table_name = getattr(storage, "physical_table_name", None) or storage.storage_hash
            time_index_name, index_names, _ = _storage_time_indexed_contract(storage)

            adjusted_start, adjusted_end, adjusted_dimension_range_map, _ = (
                db_interface.constrain_read(
                    table=table_name,
                    start=start_date,
                    end=end_date,
                    time_index_name=time_index_name,
                    index_names=index_names,
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
                index_names=index_names,
                time_index_name=time_index_name,
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

        time_index_name, index_names, column_dtypes_map = _storage_time_indexed_contract(
            data_node_update.data_node_storage
        )
        try:
            df[time_index_name] = token_to_pandas_series(
                df[time_index_name],
                TIMESTAMP_TZ,
                is_time_index=True,
            )
        except Exception as e:
            raise e
        columns_to_loop = set(columns or column_dtypes_map.keys()) | set(index_names)
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

    def get_earliest_value(
        self,
        data_node_update: Any,
    ) -> tuple[pd.Timestamp | None, dict[Any, pd.Timestamp | None]]:
        if self.class_type in LOCAL_DATA_SOURCE_CLASS_TYPES:
            db_interface = _local_data_interface(self.class_type)
            storage = data_node_update.data_node_storage
            table_name = (
                getattr(storage, "physical_table_name", None)
                or getattr(storage, "storage_hash", None)
            )
            time_index_name, index_names, _ = _storage_time_indexed_contract(storage)
            return db_interface.time_index_minima(
                table=table_name,
                index_names=index_names,
                time_index_name=time_index_name,
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
        use_enum_values = True

    def model_dump_json(self, **json_dumps_kwargs) -> str:
        dump = self.model_dump()
        dump["related_resource"] = self.related_resource.model_dump()
        return json.dumps(dump, **json_dumps_kwargs)

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


class MetaTable(BasePydanticModel, LabelableObjectMixin, ShareableObjectMixin, BaseObjectOrm):
    ENDPOINT: ClassVar[str] = "ts_manager/meta_table"
    FILTERSET_FIELDS: ClassVar[dict[str, list[str]]] = {
        "storage_hash": ["in", "exact", "contains"],
        "identifier": ["in", "exact", "contains"],
        "uid": ["in", "exact"],
        "data_source__uid": ["in", "exact"],
        "namespace": ["exact", "contains", "in", "isnull"],
        "management_mode": ["exact", "in"],
        "physical_table_name": ["exact", "contains", "in"],
        "labels": ["exact", "in", "contains"],
    }
    FILTER_VALUE_NORMALIZERS: ClassVar[dict[str, str]] = {
        "uid": "str",
        "uid__in": "str",
        "data_source__uid": "uid",
        "data_source__uid__in": "uid",
        "labels": "str",
        "labels__in": "str",
        "labels__contains": "str",
    }
    DESTROY_QUERY_PARAMS: ClassVar[dict[str, str]] = {
        "full_delete_selected": "bool",
        "full_delete_downstream_tables": "bool",
        "delete_with_no_table": "bool",
        "override_protection": "bool",
    }
    DESTROY_QUERY_PARAM_DESCRIPTIONS: ClassVar[dict[str, str]] = {
        "full_delete_selected": "Fully delete the selected table metadata row.",
        "full_delete_downstream_tables": (
            "Delete downstream table metadata dependencies starting from the selected row."
        ),
        "delete_with_no_table": (
            "Scan table metadata rows and fully delete records whose backing database table "
            "does not exist."
        ),
        "override_protection": (
            "Bypass protect_from_deletion. ORG_ADMIN only. Used with full_delete_selected=true."
        ),
    }

    uid: str | None = Field(None, description="Public uid of this MetaTable.")
    data_source: int | DynamicTableDataSource | dict[str, Any] | None = None
    data_source_uid: str | None = None
    storage_hash: str = Field(..., max_length=63, description="Canonical table storage hash.")
    identifier: str | None = None
    namespace: str | None = None
    description: str | None = None
    labels: list[str] = Field(default_factory=list)
    management_mode: MetaTableManagementMode
    physical_table_name: str
    table_contract: dict[str, Any] = Field(default_factory=dict)
    contract_version: str = "relational-table.v1"
    introspection_snapshot: dict[str, Any] = Field(default_factory=dict)
    protect_from_deletion: bool = False
    columns: list[MetaTableColumnPayload] = Field(default_factory=list)
    indexes_meta: list[MetaTableIndexPayload] = Field(default_factory=list)
    foreign_keys: list[MetaTableForeignKeyPayload] = Field(default_factory=list)
    incoming_fks: list[MetaTableForeignKeyPayload] = Field(default_factory=list)
    creation_date: datetime.datetime | None = None
    created_by_user_uid: str | None = None
    organization_owner_uid: str | None = None
    open_for_everyone: bool = False
    registration: dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(populate_by_name=True)

    def _public_uid(self) -> str:
        if self.uid in (None, ""):
            raise ValueError("MetaTable must have a uid before calling this endpoint.")
        return str(self.uid)

    @classmethod
    def _deserialize_search_response(cls, data: Any):
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
    def _post_action(
        cls,
        action_name: str,
        payload: Mapping[str, Any] | BasePydanticModel,
        *,
        timeout: int | float | tuple[float, float] | None = None,
        expected_statuses: tuple[int, ...] = (200,),
    ) -> dict[str, Any]:
        url = f"{cls.get_object_url().rstrip('/')}/{action_name.strip('/')}/"
        payload_json = _payload_json(payload)
        request_payload = {"json": payload_json}
        response = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="POST",
            url=url,
            payload=request_payload,
            time_out=timeout,
        )
        if response.status_code not in expected_statuses:
            raise_for_response(response, payload=request_payload)
        return response.json()

    def _post_detail_action(
        self,
        action_name: str,
        payload: Mapping[str, Any] | BasePydanticModel | None = None,
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> dict[str, Any]:
        url = f"{type(self).get_object_url().rstrip('/')}/{self._public_uid()}/{action_name.strip('/')}/"
        if payload is None:
            payload_json: dict[str, Any] = {}
        else:
            payload_json = _payload_json(payload)
        request_payload = {"json": payload_json}
        response = make_request(
            s=type(self).build_session(),
            loaders=type(self).LOADERS,
            r_type="POST",
            url=url,
            payload=request_payload,
            time_out=timeout,
        )
        if response.status_code != 200:
            raise_for_response(response, payload=request_payload)
        return response.json()

    def patch(
        self,
        time_out: None | int = None,
        *args,
        **kwargs,
    ):
        url = f"{type(self).get_object_url().rstrip('/')}/{self._public_uid()}/"
        payload = {"json": serialize_to_json(kwargs)}
        response = make_request(
            s=type(self).build_session(),
            loaders=type(self).LOADERS,
            r_type="PATCH",
            url=url,
            payload=payload,
            time_out=time_out,
        )
        if response.status_code != 200:
            raise_for_response(response, payload=payload)
        return type(self)(**response.json())

    @classmethod
    def patch_by_hash(cls, storage_hash: str, *args, **kwargs):
        metadata = cls.get(storage_hash=storage_hash)
        return metadata.patch(*args, **kwargs)

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
        if uid in (None, ""):
            raise ValueError(f"{cls.__name__} uid is required for deletion.")
        payload = {
            "params": {
                "full_delete_selected": full_delete_selected,
                "full_delete_downstream_tables": full_delete_downstream_tables,
                "delete_with_no_table": delete_with_no_table,
                "override_protection": override_protection,
            }
        }
        response = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="DELETE",
            url=f"{cls.get_object_url().rstrip('/')}/{uid}/",
            payload=payload,
            time_out=timeout,
        )
        raise_for_response(response)
        return response.json() if response.content else None

    def delete(
        self,
        *,
        full_delete_selected: bool = False,
        full_delete_downstream_tables: bool = False,
        delete_with_no_table: bool = False,
        override_protection: bool = False,
        timeout: int | None = None,
    ):
        return type(self).destroy_by_uid(
            self._public_uid(),
            timeout=timeout,
            full_delete_selected=full_delete_selected,
            full_delete_downstream_tables=full_delete_downstream_tables,
            delete_with_no_table=delete_with_no_table,
            override_protection=override_protection,
        )

    @classmethod
    def register(
        cls,
        request: MetaTableRegistrationRequest | Mapping[str, Any] | None = None,
        *,
        timeout: int | float | tuple[float, float] | None = None,
        **kwargs: Any,
    ) -> MetaTable:
        if request is not None and kwargs:
            raise ValueError("Pass either request or keyword fields, not both.")
        payload = request if request is not None else MetaTableRegistrationRequest(**kwargs)
        response_json = cls._post_action(
            "register",
            payload,
            timeout=timeout,
            expected_statuses=(200, 201),
        )
        return cls(**response_json)

    @classmethod
    def validate_contract(
        cls,
        request: MetaTableValidateContractRequest | Mapping[str, Any] | None = None,
        *,
        timeout: int | float | tuple[float, float] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        if request is not None and kwargs:
            raise ValueError("Pass either request or keyword fields, not both.")
        payload = request if request is not None else MetaTableValidateContractRequest(**kwargs)
        return cls._post_action("validate-contract", payload, timeout=timeout)

    def validate_existing_contract(
        self,
        request: MetaTableValidateContractRequest | Mapping[str, Any] | None = None,
        *,
        timeout: int | float | tuple[float, float] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        if request is not None and kwargs:
            raise ValueError("Pass either request or keyword fields, not both.")
        payload = request if request is not None else MetaTableValidateContractRequest(**kwargs)
        return self._post_detail_action(
            "validate-contract",
            payload,
            timeout=timeout,
        )

    def introspect(
        self,
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> dict[str, Any]:
        response_json = self._post_detail_action("introspect", timeout=timeout)
        snapshot = response_json.get("introspection_snapshot")
        if isinstance(snapshot, dict):
            self.introspection_snapshot = snapshot
        return response_json

    def refresh_table_search_index(
        self,
        *,
        timeout: int | None = None,
    ) -> dict[str, Any] | None:
        response = make_request(
            s=type(self).build_session(),
            loaders=type(self).LOADERS,
            r_type="POST",
            url=(
                f"{type(self).get_object_url().rstrip('/')}/"
                f"{self._public_uid()}/refresh-table-search-index/"
            ),
            payload={},
            time_out=timeout,
        )
        raise_for_response(response)
        return response.json() if response.content else None

    def run_query(
        self,
        sql: str,
        *,
        timeout: int | None = None,
    ) -> dict[str, Any]:
        if self.uid is None:
            raise ValueError(f"{type(self).__name__} must have a uid before running a query.")

        sql = str(sql or "").strip()
        if not sql:
            raise ValueError("sql is required.")

        cls = type(self)
        url = f"{cls.get_object_url().rstrip('/')}/{self._public_uid()}/run_query/"
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
        q = (q or "").strip()
        if not q:
            raise ValueError("q is required")

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

        payload = {"json": serialize_to_json(body)}
        response = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="POST",
            url=f"{cls.get_object_url().rstrip('/')}/description-search/",
            payload=payload,
        )
        if response.status_code != 200:
            raise_for_response(response, payload=payload)

        return cls._deserialize_search_response(response.json())

    @classmethod
    def column_search(cls, q: str, **filters):
        q = (q or "").strip()
        if not q:
            raise ValueError("q is required")

        payload = {"params": serialize_to_json({"q": q, **filters})}
        response = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="GET",
            url=f"{cls.get_object_url().rstrip('/')}/column-search/",
            payload=payload,
        )
        if response.status_code != 200:
            raise_for_response(response, payload=payload)

        return cls._deserialize_search_response(response.json())

    @classmethod
    def execute_operation(
        cls,
        operation: MetaTableCompiledSQLOperation | Mapping[str, Any],
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> dict[str, Any]:
        payload = (
            operation
            if isinstance(operation, MetaTableCompiledSQLOperation)
            else MetaTableCompiledSQLOperation(**operation)
        )
        return cls._post_action(
            "execute-operation",
            payload,
            timeout=timeout,
            expected_statuses=(200,),
        )


__all__ = [
    "DataSource",
    "DynamicTableDataSource",
    "DUCK_DB",
    "LOCAL_DATA_SOURCE_CLASS_TYPES",
    "MetaTable",
    "MetaTableColumnContract",
    "MetaTableColumnPayload",
    "MetaTableCompiledSQLOperation",
    "MetaTableCompiledSQLDialect",
    "MetaTableCompiledSQLParamstyle",
    "MetaTableCompiledSQLVersion",
    "MetaTableContract",
    "MetaTableForeignKeyContract",
    "MetaTableForeignKeyPayload",
    "MetaTableIndexContract",
    "MetaTableIndexPayload",
    "MetaTableManagementMode",
    "MetaTableOperation",
    "MetaTableOperationLimits",
    "MetaTableOperationScope",
    "MetaTableOperationScopeTable",
    "MetaTablePhysicalContract",
    "MetaTableRegistrationRequest",
    "MetaTableStatementPayload",
    "MetaTableValidateContractRequest",
    "SQLITE",
    "COMPILED_SQL_V1",
]
