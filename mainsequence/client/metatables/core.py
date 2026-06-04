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
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from threading import RLock
from typing import Any, ClassVar, Generic, Literal, TypedDict, TypeVar
from uuid import UUID

import numpy as np
import pandas as pd
import pytz
import requests
from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from mainsequence.logconf import logger

from ..base import BaseObjectOrm, BasePydanticModel, LabelableObjectMixin, ShareableObjectMixin
from ..data_sources_interfaces import get_duckdb_interface_class, get_sqlite_interface_class
from ..dtype_codec import (
    DATE,
    TIMESTAMP_TZ,
    normalize_column_dtypes_map,
    normalize_dtype_token,
    pandas_dtypes_to_column_map,
    prepare_dataframe_for_remote_write,
    record_definitions_to_column_dtypes_map,
    serialize_remote_parameters,
    token_to_pandas_series,
)
from ..exceptions import raise_for_response
from ..utils import (
    TDAG_CONSTANTS,
    DateInfo,
    DoesNotExist,
    bios_uuid,
    get_network_ip,
    is_process_running,
    make_request,
    serialize_to_json,
)

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
            return (
                str(time_index_name),
                [str(name) for name in index_names],
                dict(column_dtypes_map),
            )

    columns = _field(storage, "columns")
    if columns:
        column_dtypes_map = {
            str(_field(column, "name")): _field(column, "data_type")
            for column in columns
            if _field(column, "name")
        }
        table_contract = _field(storage, "table_contract", {})
        dynamic_contract = (
            table_contract.get("dynamic_table") or {} if isinstance(table_contract, Mapping) else {}
        )
        index_names = dynamic_contract.get("index_names") or []
        time_index_name = dynamic_contract.get("time_index_name") or (
            index_names[0] if index_names else None
        )
        if time_index_name and index_names and column_dtypes_map:
            return str(time_index_name), [str(name) for name in index_names], column_dtypes_map

    raise ValueError("Storage is missing its time-indexed table contract.")


MetaTableManagementMode = Literal["external_registered", "platform_managed"]
MetaTableSchemaManagementMode = Literal[
    "backend_managed",
    "alembic_managed",
    "external_registered",
]
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


def _payload_json_sequence(
    payloads: Sequence[Mapping[str, Any] | BasePydanticModel],
) -> list[dict[str, Any]]:
    return [_payload_json(payload) for payload in payloads]


def _normalize_backend_type(value: str | None) -> str | None:
    if value is None:
        return None
    raw = str(value)
    if not raw.strip():
        return None
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
        description=("Input-only schema alias. MetaTable uses the data source default schema."),
    )
    table_name: str | None = Field(
        default=None,
        description=(
            "Physical database table name. Required for external_registered tables; "
            "omitted by platform_managed client requests because the backend owns it."
        ),
    )

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


class MetaTableContract(BasePydanticModel):
    version: str = "relational-table.v1"
    physical: MetaTablePhysicalContract
    columns: list[MetaTableColumnContract] = Field(default_factory=list)
    constraints: list[dict[str, Any]] = Field(default_factory=list)
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
    name: str | None = Field(None, description="Physical or database-assigned index name.")
    columns: list[str] = Field(
        default_factory=list,
        description="Physical column names included in this index.",
    )
    unique: bool = Field(default=False, description="Whether this index is unique.")
    method: str | None = Field(None, description="Backend index method, when known.")
    expression: str | None = Field(
        None,
        description="Backend index expression, when this is an expression index.",
    )
    contract_fragment: dict[str, Any] = Field(
        default_factory=dict,
        description="Raw normalized contract fragment for this projected index.",
    )


class MetaTableForeignKeyPayload(BasePydanticModel):
    name: str | None = Field(None, description="Physical or database-assigned FK name.")
    source_columns: list[str] = Field(
        default_factory=list,
        description="Source MetaTable physical column names.",
    )
    target_table_uid: str | None = Field(
        None,
        description="Public UID of the target MetaTable.",
    )
    target_table_storage_hash: str | None = Field(
        None,
        description="Storage hash of the target MetaTable.",
    )
    target_columns: list[str] = Field(
        default_factory=list,
        description="Target MetaTable physical column names.",
    )
    on_delete: str | None = Field(None, description="Foreign-key delete action.")
    contract_fragment: dict[str, Any] = Field(
        default_factory=dict,
        description="Raw normalized contract fragment for this projected foreign key.",
    )


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
    reserved_policy: Literal["reject", "reconcile"] | None = Field(
        default=None,
        validation_alias=AliasChoices("reserved_policy", "reservedPolicy"),
        description=(
            "Reserved MetaTable policy for this scoped table. Omit for normal "
            "operations. Use 'reconcile' only from post-Alembic catalog refresh "
            "paths after Alembic has created the physical table."
        ),
    )

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


class MetaTableRequestFields(BasePydanticModel):
    data_source_uid: str = Field(
        ...,
        description="Public UID of the DynamicTableDataSource that owns this MetaTable.",
    )
    storage_hash: str = Field(..., max_length=63, description="Canonical table storage hash.")
    table_contract: MetaTableContract | dict[str, Any]
    identifier: str | None = Field(
        default=None,
        description=(
            "Optional logical MetaTable identifier. Non-empty values are globally "
            "unique per organization and are used to resolve migrated MetaTables."
        ),
    )
    namespace: str | None = None
    description: str | None = None
    protect_from_deletion: bool = False
    labels: list[str] = Field(default_factory=list)
    schema_management: SchemaManagementRequest | dict[str, Any] | None = Field(
        None,
        description=(
            "Formal schema lifecycle ownership contract. Use "
            "mode='alembic_managed' for MetaTables whose physical DDL is owned "
            "by an Alembic provider and finalized after Alembic runs."
        ),
    )

    @model_validator(mode="after")
    def _normalize_table_contract(self) -> MetaTableRequestFields:
        if isinstance(self.table_contract, Mapping):
            self.table_contract = _normalize_contract_mapping(self.table_contract)
        return self


class MetaTableRegistrationRequest(MetaTableRequestFields):
    management_mode: MetaTableManagementMode
    provisioning: dict[str, Any] | None = None
    introspect: bool = False


class MetaTableValidateContractRequest(BasePydanticModel):
    table_contract: MetaTableContract | dict[str, Any]
    management_mode: MetaTableManagementMode | None = None
    storage_hash: str | None = None

    @model_validator(mode="after")
    def _normalize_table_contract(self) -> MetaTableValidateContractRequest:
        if isinstance(self.table_contract, Mapping):
            self.table_contract = _normalize_contract_mapping(self.table_contract)
        return self


class AlembicManagementRequest(BasePydanticModel):
    package: str = Field(
        ...,
        description="Client package or migration stream package that owns the Alembic revisions.",
    )
    migration_namespace: str = Field(
        ...,
        description="Provider-scoped migration namespace inside the package.",
    )
    provider_key: str | None = Field(
        None,
        description=(
            "Stable provider key for this Alembic stream. When omitted, TS "
            "Manager derives package:migration_namespace."
        ),
    )
    alembic_version_meta_table_uid: str | None = Field(
        None,
        description=(
            "Optional MetaTable UID for the Alembic version table that tracks this provider stream."
        ),
    )
    revision: str | None = Field(
        None,
        description="Optional last finalized Alembic revision for this MetaTable.",
    )


class SchemaManagementRequest(BasePydanticModel):
    mode: MetaTableSchemaManagementMode = Field(
        ...,
        description=(
            "Schema lifecycle owner. Alembic-managed rows are reserved before "
            "Alembic creates tables and activated through finalize-managed."
        ),
    )
    alembic: AlembicManagementRequest | dict[str, Any] | None = Field(
        None,
        description="Alembic lifecycle metadata. Required when mode is alembic_managed.",
    )


class DynamicTableDataSourceMigrationConnectionRequest(BasePydanticModel):
    purpose: Literal["schema_migration"] = "schema_migration"
    package: str = ""
    migration_namespace: str = ""
    ttl_seconds: int = Field(default=900, ge=1)


class DynamicTableDataSourceMigrationConnection(BasePydanticModel):
    ok: bool
    data_source_uid: str
    dialect: str
    credential_kind: str
    role_name: str
    owner_role_name: str | None = None
    expires_at: datetime.datetime
    uri: str

    model_config = ConfigDict(extra="allow")


class ManagedMetaTableFinalizeRequest(BasePydanticModel):
    meta_table_uids: list[str] = Field(
        ...,
        min_length=1,
        description="Reserved Alembic-managed MetaTable UIDs to reconcile and activate.",
    )
    migration_package: str | None = Field(
        None,
        description="Alembic provider package that owns these MetaTables.",
    )
    migration_namespace: str | None = Field(
        None,
        description="Alembic provider namespace that owns these MetaTables.",
    )
    migration_provider_key: str | None = Field(
        None,
        description="Alembic provider key guard for these MetaTables, when known.",
    )
    alembic_version_meta_table_uid: str | None = Field(
        None,
        description="MetaTable UID for the Alembic version table, when known.",
    )
    alembic_revision: str | None = Field(
        None,
        description="Revision label to store after successful finalization, when known.",
    )


class ManagedMetaTableFinalizeTableResult(BasePydanticModel):
    meta_table_uid: str = Field(..., description="Public UID of the finalized MetaTable.")
    identifier: str | None = Field(
        None,
        description="Organization-global logical MetaTable identifier, when present.",
    )
    storage_hash: str = Field(..., description="Canonical MetaTable storage hash.")
    physical_table_name: str | None = Field(
        None,
        description="Physical table name reconciled against the data source.",
    )
    previous_provisioning_status: str = Field(
        ...,
        description="Provisioning status before the finalize-managed attempt.",
    )
    provisioning_status: str = Field(
        ...,
        description="Provisioning status after the finalize-managed attempt.",
    )
    table_kind: str = Field(..., description="Backend table kind after reconciliation.")
    time_indexed: bool = Field(..., description="Whether the MetaTable is time-indexed.")
    finalized: bool = Field(
        ...,
        description="True when this row was reconciled and activated by this call.",
    )
    physical_table_exists: bool = Field(
        ...,
        description="Whether the backend found the physical table during reconciliation.",
    )
    schema_management_mode: str | None = Field(
        None,
        description="Backend schema management mode implied by the managed endpoint.",
    )
    migration_package: str | None = Field(None, description="Alembic provider package.")
    migration_namespace: str | None = Field(None, description="Alembic provider namespace.")
    migration_provider_key: str | None = Field(None, description="Resolved provider key.")
    alembic_version_meta_table_uid: str | None = Field(
        None,
        description="MetaTable UID for the provider Alembic version table, when known.",
    )
    alembic_revision: str | None = Field(
        None,
        description="Last Alembic revision recorded for this MetaTable, when known.",
    )
    error: dict[str, Any] | None = Field(
        None,
        description="Per-table structured error when finalization did not activate this row.",
    )

    model_config = ConfigDict(extra="ignore")


class ManagedMetaTableFinalizeResponse(BasePydanticModel):
    ok: bool = Field(
        ...,
        description="True only when every requested MetaTable is active after finalization.",
    )
    finalized_count: int = Field(..., description="Number of rows finalized by this call.")
    active_count: int = Field(..., description="Number of requested rows now active.")
    reserved_count: int = Field(..., description="Number of requested rows still reserved.")
    failed_count: int = Field(..., description="Number of requested rows with errors.")
    tables: list[ManagedMetaTableFinalizeTableResult] = Field(
        ...,
        description="Per-MetaTable finalization results.",
    )

    model_config = ConfigDict(extra="allow")


class AlembicProviderResetRequest(BasePydanticModel):
    migration_package: str = Field(
        ...,
        description="Alembic provider package, for example 'msm'.",
    )
    migration_namespace: str = Field(
        ...,
        description="Alembic provider namespace, for example 'mainsequence.examples'.",
    )
    data_source_uid: str = Field(
        ...,
        description="DynamicTableDataSource UID that owns the provider MetaTables.",
    )
    confirm_reset: Literal[True] = Field(
        ...,
        description="Must be true. This endpoint is destructive and provider-scoped.",
    )
    drop_physical_tables: bool = Field(
        True,
        description="Drop provider physical tables before reserving catalog rows.",
    )
    clear_alembic_version_table: bool = Field(
        True,
        description="Clear the provider Alembic version table after physical reset.",
    )
    include_reserved: bool = Field(
        True,
        description="Include already-reserved provider MetaTables in the reset result.",
    )


class AlembicProviderResetTableResult(BasePydanticModel):
    meta_table_uid: str = Field(..., description="Public UID of the reset MetaTable.")
    identifier: str | None = Field(None, description="Logical MetaTable identifier.")
    storage_hash: str = Field(..., description="Canonical MetaTable storage hash.")
    physical_table_name: str | None = Field(None, description="Physical table name.")
    previous_provisioning_status: str = Field(
        ...,
        description="Provisioning status before the reset attempt.",
    )
    provisioning_status: str = Field(
        ...,
        description="Provisioning status after the reset attempt.",
    )
    physical_table_exists: bool = Field(
        ...,
        description="Whether the physical table existed before reset completed.",
    )
    physical_table_dropped: bool = Field(
        ...,
        description="Whether this physical table was dropped by reset.",
    )
    error: dict[str, Any] | None = Field(
        None,
        description="Per-table structured reset error, when present.",
    )

    model_config = ConfigDict(extra="allow")


class AlembicProviderResetResponse(BasePydanticModel):
    ok: bool = Field(..., description="Whether the provider reset completed without errors.")
    migration_provider_key: str = Field(..., description="Resolved provider key.")
    migration_package: str = Field(..., description="Alembic provider package.")
    migration_namespace: str = Field(..., description="Alembic provider namespace.")
    data_source_uid: str = Field(..., description="Provider DynamicTableDataSource UID.")
    meta_table_uids: list[str] = Field(..., description="Provider MetaTable UIDs reset.")
    dropped_physical_tables: list[str] = Field(
        default_factory=list,
        description="Physical table names dropped during reset.",
    )
    cleared_alembic_version_table: bool = Field(
        ...,
        description="Whether the provider Alembic version table was cleared.",
    )
    deleted_or_reserved_catalog_rows: list[str] = Field(
        default_factory=list,
        description="Catalog row UIDs deleted or moved to reserved by reset.",
    )
    failed_count: int = Field(..., description="Number of reset failures.")
    tables: list[AlembicProviderResetTableResult] = Field(
        default_factory=list,
        description="Per-MetaTable reset results.",
    )
    errors: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Provider-level reset errors.",
    )

    model_config = ConfigDict(extra="allow")


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
            table_name = getattr(storage, "physical_table_name", None) or getattr(
                storage, "storage_hash", None
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
        description="Backend numeric row identifier of the time-indexed data source.",
    )
    related_resource: DataSource
    related_resource_class_type: str

    class Config:
        use_enum_values = True

    def model_dump_json(self, **json_dumps_kwargs) -> str:
        dump = self.model_dump()
        dump["related_resource"] = self.related_resource.model_dump()
        return json.dumps(dump, **json_dumps_kwargs)

    def _public_uid(self) -> str:
        if self.uid in (None, ""):
            raise ValueError("DynamicTableDataSource must have a uid before calling this endpoint.")
        return str(self.uid)

    def issue_migration_connection(
        self,
        request: DynamicTableDataSourceMigrationConnectionRequest | Mapping[str, Any] | None = None,
        *,
        timeout: int | float | tuple[float, float] | None = None,
        **kwargs: Any,
    ) -> DynamicTableDataSourceMigrationConnection:
        if request is not None and kwargs:
            raise ValueError("Pass either request or keyword fields, not both.")
        payload = (
            request
            if request is not None
            else DynamicTableDataSourceMigrationConnectionRequest(**kwargs)
        )
        if isinstance(payload, Mapping):
            payload = DynamicTableDataSourceMigrationConnectionRequest(**payload)

        url = (
            f"{type(self).get_object_url().rstrip('/')}/{self._public_uid()}/migration-connection/"
        )
        request_payload = {"json": _payload_json(payload)}
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
        return DynamicTableDataSourceMigrationConnection(**response.json())

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

        class_type = (
            None if isinstance(data_source, int) else getattr(data_source, "class_type", None)
        )
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

        class_type = (
            None if isinstance(data_source, int) else getattr(data_source, "class_type", None)
        )
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
        "provisioning_status": ["exact", "in"],
        "schema_management_mode": ["exact", "in"],
        "migration_provider_key": ["exact", "in"],
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
    identifier: str | None = Field(
        default=None,
        description=(
            "Optional logical MetaTable identifier. Non-empty values are globally "
            "unique per organization and are used to resolve migrated MetaTables."
        ),
    )
    namespace: str | None = None
    description: str | None = None
    labels: list[str] = Field(default_factory=list)
    management_mode: MetaTableManagementMode
    provisioning_status: Literal["reserved", "active"] = "active"
    schema_management_mode: MetaTableSchemaManagementMode = "backend_managed"
    schema_management: dict[str, Any] = Field(default_factory=dict)
    migration_package: str | None = None
    migration_namespace: str | None = None
    migration_provider_key: str | None = None
    alembic_version_meta_table_uid: str | None = None
    alembic_revision: str | None = None
    physical_table_name: str
    table_contract: dict[str, Any] = Field(default_factory=dict)
    contract_version: str = "relational-table.v1"
    table_kind: str | None = None
    time_indexed: bool | None = None
    introspection_snapshot: dict[str, Any] = Field(default_factory=dict)
    protect_from_deletion: bool = False
    columns: list[MetaTableColumnPayload] = Field(default_factory=list)
    indexes_meta: list[MetaTableIndexPayload] = Field(default_factory=list)
    foreign_keys: list[MetaTableForeignKeyPayload] = Field(default_factory=list)
    incoming_fks: list[MetaTableForeignKeyPayload] = Field(default_factory=list)
    creation_date: datetime.datetime | None = None
    created_by_user_uid: str | None = None
    organization_owner_uid: str | None = None
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
        on_status: Callable[[str], Any] | None = None,
    ) -> dict[str, Any]:
        url = f"{cls.get_object_url().rstrip('/')}/{action_name.strip('/')}/"
        if on_status is not None:
            on_status(f"Serializing POST {url} payload...")
        payload_json = _payload_json(payload)
        if on_status is not None:
            payload_size = len(json.dumps(payload_json, default=str))
            on_status(f"Serialized POST {url} payload bytes={payload_size}.")
        request_payload = {"json": payload_json}
        if on_status is not None:
            on_status(f"Building API session for POST {url}...")
        session = cls.build_session()
        if on_status is not None:
            on_status(f"Sending HTTP POST {url}...")
        response = make_request(
            s=session,
            loaders=cls.LOADERS,
            r_type="POST",
            url=url,
            payload=request_payload,
            time_out=timeout,
        )
        if on_status is not None:
            on_status(f"Received HTTP {response.status_code} from POST {url}.")
        if response.status_code not in expected_statuses:
            raise_for_response(response, payload=request_payload)
        return response.json()

    @classmethod
    def filter_by_body(
        cls,
        *,
        timeout: int | float | tuple[float, float] | None = None,
        **filters: Any,
    ) -> list[MetaTable]:
        response_json = cls._post_action(
            "filter",
            filters,
            timeout=timeout,
            expected_statuses=(200,),
        )
        if isinstance(response_json, dict) and isinstance(
            response_json.get("results"),
            list,
        ):
            return [cls(**item) for item in response_json["results"]]
        if isinstance(response_json, list):
            return [cls(**item) for item in response_json]
        raise TypeError("MetaTable.filter_by_body expected a list or paginated response.")

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
    def bulk_create(
        cls,
        rows: Sequence[Mapping[str, Any] | BasePydanticModel],
        *,
        timeout: int | float | tuple[float, float] | None = None,
        on_status: Callable[[str], Any] | None = None,
    ) -> list[MetaTable]:
        url = f"{cls.get_object_url().rstrip('/')}/"
        if on_status is not None:
            on_status(f"Serializing POST {url} payload...")
        payload_json = _payload_json_sequence(rows)
        if on_status is not None:
            payload_size = len(json.dumps(payload_json, default=str))
            on_status(f"Serialized POST {url} payload bytes={payload_size}.")
            on_status(f"Building API session for POST {url}...")
        session = cls.build_session()
        request_payload = {"json": payload_json}
        if on_status is not None:
            on_status(f"Sending HTTP POST {url}...")
        response = make_request(
            s=session,
            loaders=cls.LOADERS,
            r_type="POST",
            url=url,
            payload=request_payload,
            time_out=timeout,
        )
        if on_status is not None:
            on_status(f"Received HTTP {response.status_code} from POST {url}.")
        if response.status_code not in (200, 201):
            raise_for_response(response, payload=request_payload)
        response_json = response.json()
        if not isinstance(response_json, list):
            raise TypeError(f"{cls.__name__}.bulk_create expected a list response.")
        return [cls(**item) for item in response_json]

    @classmethod
    def finalize_managed(
        cls,
        request: ManagedMetaTableFinalizeRequest | Mapping[str, Any] | None = None,
        *,
        timeout: int | float | tuple[float, float] | None = None,
        on_status: Callable[[str], Any] | None = None,
        **kwargs: Any,
    ) -> ManagedMetaTableFinalizeResponse:
        if request is not None and kwargs:
            raise ValueError("Pass either request or keyword fields, not both.")
        payload = request if request is not None else ManagedMetaTableFinalizeRequest(**kwargs)
        response_json = cls._post_action(
            "finalize-managed",
            payload,
            timeout=timeout,
            expected_statuses=(200, 409),
            on_status=on_status,
        )
        return ManagedMetaTableFinalizeResponse(**response_json)

    @classmethod
    def alembic_provider_reset(
        cls,
        request: AlembicProviderResetRequest | Mapping[str, Any] | None = None,
        *,
        timeout: int | float | tuple[float, float] | None = None,
        on_status: Callable[[str], Any] | None = None,
        **kwargs: Any,
    ) -> AlembicProviderResetResponse:
        if request is not None and kwargs:
            raise ValueError("Pass either request or keyword fields, not both.")
        payload = request if request is not None else AlembicProviderResetRequest(**kwargs)
        response_json = cls._post_action(
            "alembic-provider-reset",
            payload,
            timeout=timeout,
            expected_statuses=(200, 409),
            on_status=on_status,
        )
        return AlembicProviderResetResponse(**response_json)

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
        """
        Refresh this MetaTable's physical database shape snapshot.

        This calls the backend ``POST /meta_table/<uid>/introspect/`` action.
        The backend reads the real physical table through the MetaTable's data
        source, reflects columns, indexes, and constraints, stores that data on
        ``MetaTable.introspection_snapshot``, and returns the full response.

        This method is intended for admin, debugging, and reconciliation
        workflows. Use it when a client needs to inspect what the database
        currently has, diagnose catalog/physical drift, or refresh metadata
        after an out-of-band DDL change. It is not required for normal reads,
        writes, registration, or migration-first application startup.

        Returns:
            Backend response containing ``ok``, ``meta_table_uid``, and
            ``introspection_snapshot``. When the snapshot is an object, this
            instance's ``introspection_snapshot`` attribute is updated in place.
        """
        response_json = self._post_detail_action("introspect", timeout=timeout)
        snapshot = response_json.get("introspection_snapshot")
        if isinstance(snapshot, dict):
            self.introspection_snapshot = snapshot
        return response_json

    def get_schema_graph(
        self,
        *,
        depth: int = 1,
        include_incoming: bool = False,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> dict[str, Any]:
        """
        Return the MetaTable foreign-key schema graph rooted at this table.

        The graph is the client API for dependency analysis. Outgoing edges are
        foreign keys declared by this table. When ``include_incoming`` is true,
        the response also includes visible tables that depend on this table.

        Args:
            depth: Relationship traversal depth. The backend clamps this to its
                supported range.
            include_incoming: Include inbound relationships where another
                visible MetaTable has a foreign key targeting this table.
            timeout: Optional request timeout in seconds or ``requests`` timeout
                tuple form.

        Returns:
            A dictionary with ``root_uid``, ``depth``, ``include_incoming``,
            ``nodes``, and ``edges``. Inbound dependencies are edges where
            ``edge["target_uid"] == self.uid``; the dependent table UID is
            ``edge["source_uid"]``.
        """
        url = f"{type(self).get_object_url().rstrip('/')}/{self._public_uid()}/schema-graph/"
        payload = {
            "params": serialize_to_json(
                {
                    "depth": depth,
                    "include_incoming": include_incoming,
                }
            )
        }
        response = make_request(
            s=type(self).build_session(),
            loaders=type(self).LOADERS,
            r_type="GET",
            url=url,
            payload=payload,
            time_out=timeout,
        )
        if response.status_code != 200:
            raise_for_response(response, payload=payload)
        return response.json()

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


# Global executor (or you could define one on your class)
_executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)

_POD_PROJECT_RESOLUTION_LOCK = RLock()
_POD_PROJECT_RESOLUTION_CACHE = None
_POD_PROJECT_LOGGED_STATES: set[tuple[str, str]] = set()
POD_PROJECT = None


def _local_data_interface(class_type: str):
    if class_type == DUCK_DB:
        return _duckdb_interface()
    if class_type == SQLITE:
        return _sqlite_interface()
    raise ValueError(f"Unsupported local data source class_type: {class_type!r}")


class AlreadyExist(Exception):
    pass


class SchedulerDoesNotExist(Exception):
    pass


class LocalTimeSeriesDoesNotExist(Exception):
    pass


class TimeIndexedProfileDoesNotExist(Exception):
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
        ..., max_length=63, description="Name of the column in the TimeIndexMetaTable contract"
    )
    dtype: str = Field(
        ...,
        max_length=100,
        description="Portable data type from the TimeIndexMetaTable contract",
    )
    label: str | None = Field(None, max_length=250, description="Human‐readable label")
    description: str | None = Field(None, description="Longer description of the column")

    @model_validator(mode="before")
    @classmethod
    def _normalize_contract_aliases(cls, value: Any) -> Any:
        if not isinstance(value, Mapping):
            return value
        data = dict(value)
        if data.get("column_name") in (None, "") and data.get("name") not in (None, ""):
            data["column_name"] = data["name"]
        if data.get("dtype") in (None, "") and data.get("data_type") not in (None, ""):
            data["dtype"] = data["data_type"]
        data.pop("name", None)
        data.pop("data_type", None)
        return data

    @field_validator("dtype")
    @classmethod
    def _normalize_dtype(cls, value: str) -> str:
        return normalize_dtype_token(value, remote=False, allow_naive_datetime=True)


class ColumnMetaData(BaseColumnMetaData, BaseObjectOrm):
    source_config_id: int | None = Field(
        None,
        description="Backend primary key for the time-indexed profile projection",
    )


class TimeIndexMetaTableRegistrationRequest(BasePydanticModel):
    model_config = ConfigDict(extra="forbid")

    data_source_uid: str = Field(..., description="Public uid of the storage data source")
    storage_hash: str = Field(
        ...,
        max_length=63,
        description="Canonical logical storage identity for the time-indexed MetaTable",
    )
    identifier: str | None = Field(
        None,
        description=(
            "Optional published storage identifier. Non-empty values are globally "
            "unique per organization."
        ),
    )
    namespace: str | None = Field(None, description="Optional published storage namespace")
    description: str | None = Field(None, description="Optional storage description")
    labels: list[str] = Field(default_factory=list)
    protect_from_deletion: bool = False
    schema_management: SchemaManagementRequest | dict[str, Any] | None = Field(
        None,
        description=(
            "Formal schema lifecycle ownership contract. Use "
            "mode='alembic_managed' for time-indexed MetaTables created by "
            "an Alembic provider."
        ),
    )
    provisioning: dict[str, Any] = Field(
        default_factory=lambda: {"create_table": True, "if_not_exists": True}
    )
    time_index_name: str = Field(..., description="Canonical timestamp column name")
    partition_strategy: str = Field(
        default="backend_default",
        description="Time-indexed MetaTable physical partitioning strategy",
    )
    table_contract: dict[str, Any] = Field(
        ...,
        description="Inherited MetaTable contract; owns table and column identity.",
    )

    @model_validator(mode="before")
    @classmethod
    def _reject_flat_table_shape_fields(cls, value: Any) -> Any:
        if not isinstance(value, Mapping):
            return value
        data = dict(value)
        forbidden = [
            field
            for field in ("columns", "index_names", "foreign_keys", "storage_layout")
            if field in data
        ]
        if forbidden:
            raise ValueError(
                "Time-indexed MetaTable registration uses table_contract for table shape. "
                f"Do not submit flat fields: {forbidden}."
            )
        table_contract = data.get("table_contract")
        if not isinstance(table_contract, Mapping):
            return data
        if "dynamic_table" in table_contract:
            raise ValueError(
                "Time-indexed MetaTable storage fields are first-class fields; do not nest "
                "dynamic_table inside table_contract."
            )
        forbidden_contract_fields = [
            field
            for field in ("index_names", "storage_layout", "physical_index_plan")
            if field in table_contract
        ]
        if forbidden_contract_fields:
            raise ValueError(
                "table_contract must be a MetaTable contract. Remove "
                f"time-indexed storage fields: {forbidden_contract_fields}."
            )
        normalized_contract = dict(table_contract)
        normalized_contract["table_kind"] = "time_indexed"
        normalized_contract.setdefault("version", "relational-table.v1")
        normalized_contract.setdefault("physical", {})
        data["table_contract"] = normalized_contract
        return data


def _payload_get(obj: Any, key: str, default: Any = None) -> Any:
    if isinstance(obj, Mapping):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _dynamic_table_contract_fragment(table_contract: Mapping[str, Any] | None) -> dict[str, Any]:
    if not isinstance(table_contract, Mapping):
        return {}
    dynamic_contract = table_contract.get("dynamic_table")
    if isinstance(dynamic_contract, Mapping):
        return dict(dynamic_contract)
    return {}


def _column_contracts_from_dtype_map(
    column_dtypes_map: Mapping[str, Any],
    *,
    index_names: Sequence[str] | None = None,
    remote: bool,
    allow_naive_datetime: bool,
) -> list[dict[str, Any]]:
    normalized_dtypes = normalize_column_dtypes_map(
        column_dtypes_map,
        remote=remote,
        allow_naive_datetime=allow_naive_datetime,
    )
    index_name_set = {str(name) for name in (index_names or [])}
    return [
        {
            "name": str(column_name),
            "data_type": dtype,
            "nullable": str(column_name) not in index_name_set,
            "primary_key": False,
            "unique": False,
            "ordinal_position": position,
        }
        for position, (column_name, dtype) in enumerate(normalized_dtypes.items())
    ]


def _normalize_time_indexed_column_contracts(
    columns: Sequence[Mapping[str, Any] | Any],
    *,
    remote: bool,
    allow_naive_datetime: bool,
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for position, column in enumerate(columns):
        name = _payload_get(column, "name") or _payload_get(column, "column_name")
        data_type = _payload_get(column, "data_type") or _payload_get(column, "dtype")
        if name in (None, ""):
            raise ValueError("TimeIndexMetaTable column contracts require a non-empty name.")
        if data_type in (None, ""):
            raise ValueError(f"TimeIndexMetaTable column {name!r} requires a data_type.")
        normalized.append(
            {
                "name": str(name),
                "data_type": normalize_dtype_token(
                    str(data_type),
                    remote=remote,
                    allow_naive_datetime=allow_naive_datetime,
                ),
                "backend_type": _payload_get(column, "backend_type"),
                "nullable": bool(_payload_get(column, "nullable", True)),
                "primary_key": bool(_payload_get(column, "primary_key", False)),
                "unique": bool(_payload_get(column, "unique", False)),
                "ordinal_position": int(_payload_get(column, "ordinal_position", position) or 0),
                "description": _payload_get(column, "description"),
                "label": _payload_get(column, "label"),
            }
        )
    return normalized


def _column_dtype_map_from_contracts(
    columns: Sequence[Mapping[str, Any] | Any],
    *,
    remote: bool,
    allow_naive_datetime: bool,
) -> dict[str, str]:
    return {
        column["name"]: column["data_type"]
        for column in _normalize_time_indexed_column_contracts(
            columns,
            remote=remote,
            allow_naive_datetime=allow_naive_datetime,
        )
    }


class TimeIndexedProfileBase:
    column_dtypes_map: dict[str, Any] = Field(
        default_factory=dict,
        description="Derived dtype map projected from canonical MetaTable columns",
    )
    columns: list[MetaTableColumnPayload] = Field(default_factory=list)
    index_names: list

    @model_validator(mode="before")
    @classmethod
    def _derive_column_dtypes_map_from_columns(cls, value: Any) -> Any:
        if not isinstance(value, Mapping):
            return value
        data = dict(value)
        if data.get("column_dtypes_map"):
            return data
        columns = data.get("columns")
        if isinstance(columns, Sequence) and not isinstance(columns, (str, bytes, bytearray)):
            data["column_dtypes_map"] = {
                str(_payload_get(column, "name") or _payload_get(column, "column_name")): (
                    _payload_get(column, "data_type") or _payload_get(column, "dtype")
                )
                for column in columns
                if _payload_get(column, "name") or _payload_get(column, "column_name")
            }
        return data

    @field_validator("column_dtypes_map")
    @classmethod
    def _normalize_column_dtypes_map(cls, value: dict[str, Any]) -> dict[str, str]:
        return normalize_column_dtypes_map(value, remote=False, allow_naive_datetime=True)


class TimeIndexedProfile(TimeIndexedProfileBase, BasePydanticModel):
    """Read-only TimeIndexMetaTable profile.

    This is a value object returned by TimeIndexMetaTable responses. It is not a
    public REST resource; operations such as stats reads and column metadata
    updates are routed through TimeIndexMetaTable.
    """

    dynamic_table_uid: str | None = Field(
        None, description="Backend response alias for the related TimeIndexMetaTable uid"
    )
    related_table_uid: str | None = Field(
        None, description="Public uid of the related TimeIndexMetaTable"
    )
    time_index_name: str = Field(..., max_length=100, description="Time index name")
    partition_strategy: str | None = None
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
    multi_index_stats: dict[str, Any] | None = Field(
        None, description="Canonical multi-index progress statistics"
    )
    multi_index_column_stats: dict[str, Any] | None = Field(
        None, description="Column-level multi-index statistics"
    )
    columns_metadata: list[ColumnMetaData] | None = None

    column_index_names: list | None = [None]


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
        "remote_table__uid": ["exact", "in"],
        "remote_table__data_source__uid": ["exact", "in"],
        "related_table__namespace": ["contains", "in", "isnull"],
    }
    FILTER_VALUE_NORMALIZERS: ClassVar[dict[str, str]] = {
        "uid": "uid",
        "uid__in": "uid",
        "remote_table__uid": "uid",
        "remote_table__uid__in": "uid",
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

    data_node_storage: str | UUID | TimeIndexMetaTable
    tags: list[str] | None = Field(default=[], description="List of tags")
    labels: list[str] = Field(
        default_factory=list,
        description=(
            "Organizational labels  "
            "These are helpers for grouping and discovery only and do not change runtime behavior or functionality."
        ),
    )
    description: str | None = Field(None, description="Optional HTML description")
    time_serie_source_code_git_hash: str | None = None
    time_serie_source_code: str | None = None
    update_details: DataNodeUpdateDetails | None = None
    run_configuration: RunConfiguration | None = None
    open_for_everyone: bool = Field(
        default=False, description="Whether the ts is open for everyone"
    )

    @property
    def data_source_uid(self):
        if isinstance(self.data_node_storage, str | UUID):
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
            raise TimeIndexedProfileDoesNotExist

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
        url = (
            self.get_object_url()
            + f"/{self._public_uid()}/set_last_update_index_time_from_update_stats/"
        )

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
            raise TimeIndexedProfileDoesNotExist

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
        url = (
            self.get_object_url()
            + f"/{self._public_uid()}/verify_if_direct_dependencies_are_updated/"
        )
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
                raise_for_response(
                    r,
                )

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
        time_indexed_profile_map = {
            str(k): TimeIndexedProfile(**v) if v is not None else v
            for k, v in response_json["time_indexed_profile_map"].items()
        }
        state_data = {
            str(k): DataNodeUpdateDetails(**v) for k, v in response_json["state_data"].items()
        }
        all_index_stats = {str(k): v for k, v in response_json["all_index_stats"].items()}
        data_node_updates = [DataNodeUpdate(**v) for v in response_json["local_metadatas"]]
        return UpdateBatchResponse[
            DataNodeUpdate,
            DataNodeUpdateDetails,
            TimeIndexedProfile,
        ](
            time_indexed_profile_map=time_indexed_profile_map,
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
        record_column_dtypes_map = record_definitions_to_column_dtypes_map(
            records,
            remote=remote_dtypes,
            allow_naive_datetime=allow_naive_datetime,
        )
        if time_index_name is None:
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
        column_dtypes_map.update(record_column_dtypes_map)

        data_frame = data_frame.replace({np.nan: None})

        return data_frame, index_names, column_dtypes_map, time_index_name

    def upsert_data_into_table(
        self,
        data: pd.DataFrame,
        data_source: DynamicTableDataSource,
        overwrite: bool,
        records: Sequence[Any] | None = None,
        source_table_schema: Mapping[str, Any] | None = None,
    ):
        overwrite = True  # ALWAYS OVERWRITE
        storage_class_type = getattr(
            getattr(data_source, "related_resource", None), "class_type", None
        )
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
            schema_columns = source_table_schema.get("columns")
            schema_column_dtypes_map = source_table_schema.get("column_dtypes_map")
            if schema_columns is not None:
                column_dtypes_map = _column_dtype_map_from_contracts(
                    schema_columns,
                    remote=not is_local_storage,
                    allow_naive_datetime=is_local_storage,
                )
            elif schema_column_dtypes_map is not None:
                column_contracts = _column_contracts_from_dtype_map(
                    schema_column_dtypes_map,
                    index_names=index_names,
                    remote=not is_local_storage,
                    allow_naive_datetime=is_local_storage,
                )
                column_dtypes_map = _column_dtype_map_from_contracts(
                    column_contracts,
                    remote=not is_local_storage,
                    allow_naive_datetime=is_local_storage,
                )
        index_names = list(index_names)
        missing_index_dtypes = [name for name in index_names if name not in column_dtypes_map]
        if missing_index_dtypes:
            raise ValueError(
                "Every index column must exist in the TimeIndexMetaTable column contract. "
                f"Missing: {missing_index_dtypes}"
            )

        # overwrite data origina data frame to release memory
        if not data[time_index_name].is_monotonic_increasing:
            data = data.sort_values(time_index_name)

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

    def wait_for_update_time(
        self,
    ):
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


class DataNodeUpdateDetails(BaseUpdateDetails, BasePydanticModel, BaseObjectOrm):
    related_table_uid: str | None = Field(
        None, description="Public uid of the related DataNodeUpdate"
    )
    run_configuration: RunConfiguration | None = None

    @classmethod
    def patch_for_data_node_update_uid(
        cls,
        data_node_update_uid: str,
        *,
        timeout: int | None = None,
        **kwargs,
    ) -> DataNodeUpdateDetails:
        if data_node_update_uid in (None, ""):
            raise ValueError("DataNodeUpdate uid is required to patch update details.")

        payload = {"json": serialize_to_json(kwargs)}
        r = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="PATCH",
            url=f"{cls.get_object_url()}/{data_node_update_uid}/",
            payload=payload,
            time_out=timeout,
        )
        if r.status_code != 200:
            raise_for_response(r, payload=payload)
        return cls(**r.json())

    @staticmethod
    def _parse_parameters_filter(parameters):
        for key, value in parameters.items():
            if "__in" in key:
                assert isinstance(value, list)
                parameters[key] = ",".join(value)
        return parameters


class TableMetaData(BaseModel):
    identifier: str | None = Field(
        default=None,
        description=(
            "Optional logical MetaTable identifier. Non-empty values are globally "
            "unique per organization."
        ),
    )
    description: str | None = None


class TimeIndexMetaTable(MetaTable):
    ENDPOINT: ClassVar[str] = "ts_manager/dynamic_table"
    FILTERSET_FIELDS: ClassVar[dict[str, list[str]]] = {
        "storage_hash": ["in", "exact", "contains"],
        "identifier": ["in", "exact", "contains"],
        "uid": ["in", "exact"],
        "data_source__uid": ["in", "exact"],
        "namespace": ["exact", "contains", "in", "isnull"],
        "physical_table_name": ["exact", "contains", "in"],
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
    build_configuration_json_schema: dict[str, Any] | None = Field(
        None,
        description="JSON schema describing the DataNode update build configuration.",
    )
    data_source_open_for_everyone: bool = Field(
        default=False, description="Whether the data source is open for everyone"
    )
    source_class_name: str | None = None
    time_indexed_profile: TimeIndexedProfile | None = None
    table_index_names: dict | None = None

    # TS specifi
    compression_policy_config: dict | None = None
    retention_policy_config: dict | None = None

    _drop_indices: bool = False  # for direct incertion we can pass this values
    _rebuild_indices: bool = False  # for direct incertion we can pass this values

    def _time_indexed_dynamic_contract(self) -> dict[str, Any]:
        return _dynamic_table_contract_fragment(self.table_contract)

    def _time_indexed_storage_layout(self) -> dict[str, Any]:
        profile = self.time_indexed_profile
        if profile is not None and isinstance(profile.storage_layout, Mapping):
            return dict(profile.storage_layout)

        dynamic_contract = self._time_indexed_dynamic_contract()
        storage_layout = dynamic_contract.get("storage_layout")
        if isinstance(storage_layout, Mapping):
            return dict(storage_layout)

        contract_layout = (
            self.table_contract.get("storage_layout")
            if isinstance(self.table_contract, Mapping)
            else None
        )
        if isinstance(contract_layout, Mapping):
            return dict(contract_layout)
        return {}

    @property
    def time_index_name(self) -> str | None:
        profile = self.time_indexed_profile
        if profile is not None and profile.time_index_name:
            return str(profile.time_index_name)

        dynamic_contract = self._time_indexed_dynamic_contract()
        if dynamic_contract.get("time_index_name"):
            return str(dynamic_contract["time_index_name"])

        index_names = dynamic_contract.get("index_names")
        if (
            isinstance(index_names, Sequence)
            and not isinstance(index_names, (str, bytes, bytearray))
            and index_names
        ):
            return str(index_names[0])

        storage_layout = self._time_indexed_storage_layout()
        time_index = storage_layout.get("time_index")
        if isinstance(time_index, Mapping) and time_index.get("name"):
            return str(time_index["name"])
        return None

    @property
    def index_names(self) -> list[str]:
        profile = self.time_indexed_profile
        if profile is not None and profile.index_names:
            return [str(name) for name in profile.index_names]

        dynamic_contract = self._time_indexed_dynamic_contract()
        dynamic_index_names = dynamic_contract.get("index_names")
        if isinstance(dynamic_index_names, Sequence) and not isinstance(
            dynamic_index_names, (str, bytes, bytearray)
        ):
            return [str(name) for name in dynamic_index_names]

        storage_layout = self._time_indexed_storage_layout()
        uniqueness_columns = (storage_layout.get("uniqueness") or {}).get("columns")
        if isinstance(uniqueness_columns, Sequence) and not isinstance(
            uniqueness_columns, (str, bytes, bytearray)
        ):
            return [str(name) for name in uniqueness_columns]

        time_index_name = self.time_index_name
        identity_dimensions = storage_layout.get("identity_dimensions")
        if (
            time_index_name
            and isinstance(identity_dimensions, Sequence)
            and not isinstance(identity_dimensions, (str, bytes, bytearray))
        ):
            return [time_index_name, *[str(name) for name in identity_dimensions]]
        return [time_index_name] if time_index_name else []

    @property
    def column_dtypes_map(self) -> dict[str, str]:
        profile = self.time_indexed_profile
        if profile is not None and profile.column_dtypes_map:
            return normalize_column_dtypes_map(
                profile.column_dtypes_map,
                remote=False,
                allow_naive_datetime=True,
            )

        if self.columns:
            return _column_dtype_map_from_contracts(
                self.columns,
                remote=False,
                allow_naive_datetime=True,
            )

        contract_columns = (
            self.table_contract.get("columns") if isinstance(self.table_contract, Mapping) else None
        )
        if isinstance(contract_columns, Sequence) and not isinstance(
            contract_columns, (str, bytes, bytearray)
        ):
            return _column_dtype_map_from_contracts(
                contract_columns,
                remote=False,
                allow_naive_datetime=True,
            )
        return {}

    def _require_time_indexed_table_contract(self) -> tuple[str, list[str], dict[str, str]]:
        time_index_name = self.time_index_name
        index_names = self.index_names
        column_dtypes_map = self.column_dtypes_map
        if not time_index_name or not index_names or not column_dtypes_map:
            raise ValueError(
                "TimeIndexMetaTable is missing its time-indexed table contract. "
                "Expected canonical MetaTable columns plus a time_indexed_profile projection."
            )
        return time_index_name, index_names, column_dtypes_map

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
            payload["dimension_range_map"] = self._normalize_dimension_range_map(
                dimension_range_map
            )
        return payload

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

    @classmethod
    def register(
        cls,
        request: TimeIndexMetaTableRegistrationRequest | Mapping[str, Any] | None = None,
        *,
        timeout: int | float | tuple[float, float] | None = None,
        **kwargs: Any,
    ) -> TimeIndexMetaTable:
        if request is not None and kwargs:
            raise ValueError("Pass either request or keyword fields, not both.")
        payload = (
            request if request is not None else TimeIndexMetaTableRegistrationRequest(**kwargs)
        )
        if isinstance(payload, BaseModel):
            payload_json = payload.model_dump(mode="json", exclude_none=True)
        else:
            payload_json = dict(payload)
        request_payload = {"json": serialize_to_json(payload_json)}
        response = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="POST",
            url=f"{cls.get_object_url()}/register/",
            payload=request_payload,
            time_out=timeout,
        )
        if response.status_code not in (200, 201):
            raise_for_response(response, payload=request_payload)
        return cls(**response.json())

    def get_data_updates(self, *, timeout: int | None = None) -> UpdateStatistics:
        """Fetch update-progress statistics for this TimeIndexMetaTable table."""
        if self.uid is None:
            raise ValueError("TimeIndexMetaTable must have a uid before fetching update stats.")

        cls = type(self)
        url = f"{cls.get_object_url()}/{self._public_uid()}/get-stats/"
        response = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="GET",
            url=url,
            time_out=timeout,
        )
        raise_for_response(response)
        payload = response.json()
        if "update_statistics" in payload and isinstance(payload["update_statistics"], Mapping):
            payload = payload["update_statistics"]
        multi_index_stats = payload.get("multi_index_stats") or {}
        return UpdateStatistics(
            global_index_progress=(
                payload.get("global_index_progress")
                or multi_index_stats.get("_GLOBAL_")
                or multi_index_stats.get("global_index_progress")
            ),
            index_progress=payload.get("index_progress") or multi_index_stats.get("index_progress"),
            index_min=payload.get("index_min") or multi_index_stats.get("index_min"),
            multi_index_column_stats=payload.get("multi_index_column_stats"),
        )

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
        Delete rows at or after a cutoff timestamp from this TimeIndexMetaTable table.

        This is a backend tail-delete operation:

        - it hits the TimeIndexMetaTable backend delete-after-date route
        - `after_date` is the inclusive cutoff
        - there is no `end_date`; this is not arbitrary range deletion
        - for multi-index tables, pass `dimension_filters` or
          `index_coordinates` to scope the tail delete

        The authenticated user must have edit access to this TimeIndexMetaTable.

        The returned payload contains the authoritative post-delete table stats,
        including `deleted_count`, `table_empty`, and index metadata. Consumers
        can use those stats to update visible table metadata or refetch the table
        detail after the delete.
        """
        if self.uid is None:
            raise ValueError("TimeIndexMetaTable must have a uid before deleting rows after a date.")

        payload_body: dict[str, Any] = {
            "after_date": after_date.isoformat()
            if isinstance(after_date, datetime.datetime)
            else after_date
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

    @staticmethod
    def map_columns_to_df(
        df,
        column_dtypes_map: dict,
        time_index_name: str,
        index_names: list[str],
    ) -> pd.DataFrame:
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
        df = pd.DataFrame(r.json())
        if df.empty:
            return df
        time_index_name, index_names, column_dtypes_map = (
            self._require_time_indexed_table_contract()
        )
        try:
            df[time_index_name] = pd.to_datetime(df[time_index_name], format="ISO8601")
        except Exception as e:
            raise e

        df = self.map_columns_to_df(
            df=df,
            column_dtypes_map=column_dtypes_map,
            time_index_name=time_index_name,
            index_names=index_names,
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
        node_identifier: str | None = None,
    ) -> pd.DataFrame:
        """Internal shared implementation for fetching data between dates."""
        return_storage_node = False
        if "get_data_between_dates_from_node_identifier" in url:
            return_storage_node = True

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
                    return [], None

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

            return all_results_chunk, response_data

        s = cls.build_session()

        all_results = []
        response_data = None
        if dimension_range_map:
            chunk_size = 100
            for start_idx in range(0, len(dimension_range_map), chunk_size):
                chunk_range_map = dimension_range_map[start_idx : start_idx + chunk_size]

                # Fetch data (including any pagination via next_offset)
                chunk_results, response_data = fetch_one_batch(chunk_range_map)
                all_results.extend(chunk_results)
        else:
            # If dimension_range_map is None, do a single batch with offset-based pagination.
            chunk_results, response_data = fetch_one_batch(None)
            all_results.extend(chunk_results)
        if not return_storage_node:
            return pd.DataFrame(all_results)
        else:
            storage_node = (
                cls(**response_data["storage_node"]) if response_data is not None else None
            )
            return pd.DataFrame(all_results), storage_node

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
    ) -> [pd.DataFrame, TimeIndexMetaTable]:
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
            node_identifier=node_identifier,
        )


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
            if self.api_address == get_network_ip() and is_process_running(
                self.running_process_pid
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

    max_time_index_value: datetime.datetime | None = (
        None  # does not include filter applicable for 1d index
    )
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

        if isinstance(value, (int | float)):
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
                return dt.astimezone(datetime.UTC) if dt.tzinfo else dt.replace(tzinfo=datetime.UTC)
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
        if not hasattr(self, "_max_time_in_update_statistics"):
            self._max_time_in_update_statistics = (
                self.max_time_index_value or self._initial_fallback_date
            )
        if self._max_time_in_update_statistics is None and self.index_progress is not None:
            values = self.get_index_progress_leaf_values()
            self._max_time_in_update_statistics = max(values) if values else None

        return self._max_time_in_update_statistics

    @property
    def is_any_identity_on_fallback_date(self) -> bool:
        """Return true if any index progress leaf equals _initial_fallback_date."""
        return any(
            value == self._initial_fallback_date for value in self.get_index_progress_leaf_values()
        )

    @property
    def are_all_identities_on_fallback_date(self) -> bool:
        """Return true if all index progress leaves equal _initial_fallback_date."""
        return all(
            value == self._initial_fallback_date for value in self.get_index_progress_leaf_values()
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
                (bounds or {}).get("max") if isinstance(bounds, dict) else bounds
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
            self.metadata.time_indexed_profile.index_names).
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
            self._set_progress_stats(
                {
                    identity_value: stats
                    for identity_value, stats in self._progress_stats().items()
                    if identity_value in filters
                }
            )
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
                new_stats[identity_value] = {f: self._initial_fallback_date for f in allowed}
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
    normalized_keys = [_normalize_update_stat_key(key) for key in keys]
    sub = root
    for key in normalized_keys[:-1]:
        existing = sub.setdefault(key, {})
        if not isinstance(existing, dict):
            raise ValueError("Update statistics coordinate keys collide after JSON normalization.")
        sub = existing
    final_key = normalized_keys[-1]
    if final_key in sub:
        raise ValueError("Update statistics coordinate keys collide after JSON normalization.")
    sub[final_key] = value


def _normalize_update_stat_key(key: Any) -> str:
    if isinstance(key, str):
        return key
    if isinstance(key, np.generic):
        key = key.item()
    if isinstance(key, datetime.datetime):
        value = serialize_to_json({"_": key})["_"]
        return str(value)
    if isinstance(key, datetime.date):
        return key.isoformat()
    if key is None:
        return "null"
    if isinstance(key, bool):
        return "true" if key else "false"
    if isinstance(key, int | float):
        try:
            return json.dumps(key, allow_nan=False)
        except (TypeError, ValueError):
            return str(key)

    normalized = serialize_to_json({key: None})
    normalized_key = next(iter(normalized.keys()))
    if normalized_key is None:
        return "null"
    if isinstance(normalized_key, bool):
        return "true" if normalized_key else "false"
    if isinstance(normalized_key, int | float):
        try:
            return json.dumps(normalized_key, allow_nan=False)
        except (TypeError, ValueError):
            return str(normalized_key)
    return str(normalized_key)


def _normalize_update_stat_mapping_keys(value: Any) -> Any:
    if isinstance(value, Mapping):
        normalized: dict[str, Any] = {}
        for key, item in value.items():
            normalized_key = _normalize_update_stat_key(key)
            if normalized_key in normalized:
                raise ValueError(
                    "Update statistics coordinate keys collide after JSON normalization: "
                    f"{normalized_key!r}."
                )
            normalized[normalized_key] = _normalize_update_stat_mapping_keys(item)
        return normalized
    if isinstance(value, list):
        return [_normalize_update_stat_mapping_keys(item) for item in value]
    if isinstance(value, tuple):
        return [_normalize_update_stat_mapping_keys(item) for item in value]
    return value


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


def request_to_datetime(value: Any):
    return UpdateStatistics._to_utc_datetime(value)


class LastUpdateMultiIndexStatsPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True, arbitrary_types_allowed=True)

    global_stats: dict[str, Any] = Field(..., alias="_GLOBAL_")
    index_progress: dict[str, Any] = Field(default_factory=dict)
    index_min: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _normalize_mapping_keys(cls, value: Any) -> Any:
        return _normalize_update_stat_mapping_keys(value)

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

    @model_validator(mode="before")
    @classmethod
    def _normalize_mapping_keys(cls, value: Any) -> Any:
        return _normalize_update_stat_mapping_keys(value)

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

    normalized_payload = _normalize_update_stat_mapping_keys(raw_payload)
    return LastUpdateIndexTimePayload.model_validate(normalized_payload).to_nested_payload()


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


class LocalTimeSeriesHistoricalUpdate(HistoricalUpdateRecord, BasePydanticModel, BaseObjectOrm):
    related_table_uid: str | None = Field(
        None, description="Public uid of the related DataNodeUpdate"
    )
    last_time_index_value: datetime.datetime | None = None


UpdateT = TypeVar("UpdateT")
UpdateDetailsT = TypeVar("UpdateDetailsT")
TimeIndexedProfileT = TypeVar("TimeIndexedProfileT")


class UpdateBatchResponse(BaseModel, Generic[UpdateT, UpdateDetailsT, TimeIndexedProfileT]):
    model_config = ConfigDict(extra="forbid")

    time_indexed_profile_map: dict[str, TimeIndexedProfileT | None]
    state_data: dict[str, UpdateDetailsT]
    all_index_stats: dict[str, Any]
    data_node_updates: list[UpdateT]


@dataclass(frozen=True)
class _PodProjectResolution:
    project: Any | None
    status: str
    detail: str = ""


def _reset_local_pod_project_resolution_cache() -> None:
    global _POD_PROJECT_RESOLUTION_CACHE
    with _POD_PROJECT_RESOLUTION_LOCK:
        _POD_PROJECT_RESOLUTION_CACHE = None
        _POD_PROJECT_LOGGED_STATES.clear()


def _build_local_pod_project_resolution() -> _PodProjectResolution:
    from ..models_foundry import Project

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


def _require_local_pod_project(operation: str) -> Any:
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
            local_dynamic_data_source = DynamicTableDataSource.create_duckdb(
                data_source=data_source
            )
        elif class_type == SQLITE:
            local_dynamic_data_source = DynamicTableDataSource.create_sqlite(
                data_source=data_source
            )
        else:
            raise ValueError(f"Unsupported local DataSource class_type: {class_type!r}")

        # drop local tables that are not in registered in the backend anymore (probably have been deleted)
        remote_node_storages = TimeIndexMetaTable.filter(
            data_source__uid=local_dynamic_data_source.uid,
            list_tables=True,
        )
        remote_table_names = [
            getattr(t, "physical_table_name", None) or t.storage_hash for t in remote_node_storages
        ]
        db_interface = _local_data_interface(class_type)
        local_table_names = db_interface.list_tables()

        tables_to_delete_locally = set(local_table_names) - set(remote_table_names)
        for table_name in tables_to_delete_locally:
            logger.debug(f"Deleting table in local {class_type} db {table_name}")
            db_interface.drop_table(table_name)

        tables_to_delete_remotely = set(remote_table_names) - set(local_table_names)
        for remote_table in remote_node_storages:
            remote_physical_table_name = (
                getattr(remote_table, "physical_table_name", None) or remote_table.storage_hash
            )
            if remote_physical_table_name in tables_to_delete_remotely:
                logger.debug(f"Deleting table remotely {remote_physical_table_name}")
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


SessionDataSource = PodDataSource()


def get_session_data_source() -> Any:
    data_source = getattr(SessionDataSource, "data_source", None)
    if getattr(data_source, "related_resource", None) is None:
        SessionDataSource.set_remote_db()
        data_source = getattr(SessionDataSource, "data_source", None)
    if getattr(data_source, "related_resource", None) is None:
        raise Exception("This Pod does not have a default data source")
    return data_source


DataNodeUpdateDetails.model_rebuild()
DataNodeUpdate.model_rebuild()
RunConfiguration.model_rebuild()
TimeIndexedProfile.model_rebuild()
TimeIndexMetaTable.model_rebuild()
DynamicTableDataSource.model_rebuild()
DataSource.model_rebuild()
MetaTableRequestFields.model_rebuild()
MetaTableRegistrationRequest.model_rebuild()
TimeIndexMetaTableRegistrationRequest.model_rebuild()
MetaTable.model_rebuild()


__all__ = [
    "AlembicManagementRequest",
    "AlembicProviderResetRequest",
    "AlembicProviderResetResponse",
    "AlembicProviderResetTableResult",
    "BaseColumnMetaData",
    "BaseUpdateStatistics",
    "ColumnMetaData",
    "DataNodeUpdate",
    "DataNodeUpdateDetails",
    "DataSource",
    "DynamicTableDataSource",
    "DynamicTableDataSourceMigrationConnection",
    "DynamicTableDataSourceMigrationConnectionRequest",
    "DUCK_DB",
    "HistoricalUpdateRecord",
    "LastUpdateIndexTimePayload",
    "LastUpdateMultiIndexStatsPayload",
    "LOCAL_DATA_SOURCE_CLASS_TYPES",
    "LocalTimeSeriesHistoricalUpdate",
    "ManagedMetaTableFinalizeRequest",
    "ManagedMetaTableFinalizeResponse",
    "ManagedMetaTableFinalizeTableResult",
    "MetaTable",
    "MetaTableColumnContract",
    "MetaTableColumnPayload",
    "MetaTableCompiledSQLOperation",
    "MetaTableCompiledSQLDialect",
    "MetaTableCompiledSQLParamstyle",
    "MetaTableCompiledSQLVersion",
    "MetaTableContract",
    "MetaTableForeignKeyPayload",
    "MetaTableIndexPayload",
    "MetaTableManagementMode",
    "MetaTableSchemaManagementMode",
    "MetaTableOperation",
    "MetaTableOperationLimits",
    "MetaTableOperationScope",
    "MetaTableOperationScopeTable",
    "MetaTablePhysicalContract",
    "MetaTableRequestFields",
    "MetaTableRegistrationRequest",
    "SchemaManagementRequest",
    "MetaTableStatementPayload",
    "MetaTableValidateContractRequest",
    "PodDataSource",
    "RunConfiguration",
    "Scheduler",
    "SchedulerDoesNotExist",
    "SessionDataSource",
    "SQLITE",
    "COMPILED_SQL_V1",
    "TableMetaData",
    "TableUpdateNode",
    "TimeIndexMetaTable",
    "TimeIndexedProfile",
    "TimeIndexedProfileBase",
    "TimeIndexedProfileDoesNotExist",
    "TimeIndexMetaTableRegistrationRequest",
    "UpdateBatchResponse",
    "UpdateStatistics",
    "build_last_update_index_time_payload",
    "combine_index_min_max_stats",
    "get_index_progress_chunk_stats",
    "get_session_data_source",
    "request_to_datetime",
]
