from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import Any, ClassVar, Literal

from pydantic import AliasChoices, ConfigDict, Field

from .base import BaseObjectOrm, BasePydanticModel, LabelableObjectMixin, ShareableObjectMixin
from .exceptions import raise_for_response
from .models_tdag import DynamicTableDataSource
from .utils import make_request, serialize_to_json

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


class MetaTablePhysicalContract(BasePydanticModel):
    schema_: str = Field(
        ...,
        alias="schema",
        serialization_alias="schema",
        description="Physical database schema name.",
    )
    table_name: str = Field(..., description="Physical database table name.")

    model_config = ConfigDict(populate_by_name=True)


class MetaTableColumnContract(BasePydanticModel):
    name: str
    data_type: str
    backend_type: str | None = None
    nullable: bool = True
    primary_key: bool = False
    unique: bool = False
    description: str | None = None
    label: str | None = None
    logical_name: str | None = None


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
    name: str
    logical_name: str | None = None
    data_type: str
    backend_type: str | None = None
    nullable: bool = True
    primary_key: bool = False
    unique: bool = False
    ordinal_position: int = 0
    description: str | None = None
    label: str | None = None
    contract_fragment: dict[str, Any] = Field(default_factory=dict)


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
    paramstyle: MetaTableCompiledSQLParamstyle = "pyformat"


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
    storage_hash: str
    table_contract: MetaTableContract | dict[str, Any]
    identifier: str | None = None
    namespace: str | None = None
    description: str | None = None
    protect_from_deletion: bool = False
    open_for_everyone: bool = False
    labels: list[str] = Field(default_factory=list)
    provisioning: dict[str, Any] | None = None
    introspect: bool = False


class MetaTableValidateContractRequest(BasePydanticModel):
    table_contract: MetaTableContract | dict[str, Any]
    management_mode: MetaTableManagementMode | None = None
    storage_hash: str | None = None


class MetaTable(BasePydanticModel, LabelableObjectMixin, ShareableObjectMixin, BaseObjectOrm):
    ENDPOINT: ClassVar[str] = "ts_manager/meta_table"
    FILTERSET_FIELDS: ClassVar[dict[str, list[str]]] = {
        "storage_hash": ["in", "exact", "contains"],
        "identifier": ["in", "exact", "contains"],
        "uid": ["in", "exact"],
        "data_source__id": ["in", "exact"],
        "data_source__uid": ["in", "exact"],
        "namespace": ["exact", "contains", "in", "isnull"],
        "management_mode": ["exact", "in"],
        "physical_schema": ["exact", "in"],
        "physical_table_name": ["exact", "contains", "in"],
        "labels": ["exact", "in", "contains"],
    }
    FILTER_VALUE_NORMALIZERS: ClassVar[dict[str, str]] = {
        "uid": "str",
        "uid__in": "str",
        "data_source__id": "id",
        "data_source__uid": "str",
        "data_source__uid__in": "str",
        "labels": "str",
        "labels__in": "str",
        "labels__contains": "str",
    }

    uid: str | None = Field(None, description="Public uid of this MetaTable.")
    data_source: DynamicTableDataSource | dict[str, Any] | None = None
    data_source_uid: str | None = None
    storage_hash: str
    identifier: str | None = None
    namespace: str | None = None
    description: str | None = None
    labels: list[str] = Field(default_factory=list)
    management_mode: MetaTableManagementMode
    physical_schema: str
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

    @classmethod
    def execute_operation(
        cls,
        operation: MetaTableCompiledSQLOperation | Mapping[str, Any],
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> dict[str, Any]:
        return cls._post_action(
            "execute-operation",
            operation,
            timeout=timeout,
            expected_statuses=(200,),
        )


__all__ = [
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
    "COMPILED_SQL_V1",
]
