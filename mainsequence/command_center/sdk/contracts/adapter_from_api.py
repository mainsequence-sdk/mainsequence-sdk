from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .response_mapping import AdapterResponseMapping

AdapterFromApiMethod = Literal["GET", "POST", "PUT", "PATCH", "DELETE"]
AdapterFromApiOperationKind = Literal["health", "query", "mutation", "action"]
AdapterFromApiVariableType = Literal["string", "number", "boolean", "select", "json"]
AdapterFromApiParameterLocation = Literal["path", "query", "header"]


class AdapterFromApiBaseModel(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class AdapterFromApiAdapterMetadata(AdapterFromApiBaseModel):
    type: Literal["adapter-from-api"] = "adapter-from-api"
    id: str
    title: str
    description: str


class AdapterFromApiOpenApiMetadata(AdapterFromApiBaseModel):
    url: str
    version: str = "3.1.0"
    checksum: str | None = None


class AdapterFromApiConfigVariable(AdapterFromApiBaseModel):
    key: str
    label: str
    type: AdapterFromApiVariableType
    required: bool = False
    description: str | None = None
    default: Any = None
    choices: list[dict[str, Any]] | None = None


class AdapterFromApiSecretInjection(AdapterFromApiBaseModel):
    type: str
    name: str


class AdapterFromApiSecretVariable(AdapterFromApiBaseModel):
    key: str
    label: str
    type: Literal["secret"] = "secret"
    required: bool = True
    description: str | None = None
    injection: AdapterFromApiSecretInjection


class AdapterFromApiHealth(AdapterFromApiBaseModel):
    operation_id: str = Field(alias="operationId")
    expected_status: int = Field(alias="expectedStatus")
    timeout_ms: int = Field(alias="timeoutMs")


class AdapterFromApiOperationCache(AdapterFromApiBaseModel):
    enabled: bool
    ttl_seconds: int | None = Field(default=None, alias="ttlSeconds")


class AdapterFromApiParameter(AdapterFromApiBaseModel):
    name: str
    in_: AdapterFromApiParameterLocation | str = Field(alias="in")
    required: bool = False
    type: AdapterFromApiVariableType | str | None = None
    description: str | None = None
    schema_: dict[str, Any] | None = Field(default=None, alias="schema")


class AdapterFromApiRequestBody(AdapterFromApiBaseModel):
    required: bool = False
    content_types: list[str] = Field(default_factory=lambda: ["application/json"], alias="contentTypes")
    schema_: dict[str, Any] | None = Field(default=None, alias="schema")
    schema_ref: str | None = Field(default=None, alias="schemaRef")


class AdapterFromApiOperation(AdapterFromApiBaseModel):
    operation_id: str = Field(alias="operationId")
    label: str
    description: str
    method: AdapterFromApiMethod | str
    path: str
    kind: AdapterFromApiOperationKind
    capabilities: list[str]
    requires_time_range: bool = Field(alias="requiresTimeRange")
    supports_variables: bool = Field(alias="supportsVariables")
    supports_max_rows: bool = Field(alias="supportsMaxRows")
    parameters: list[AdapterFromApiParameter] = Field(default_factory=list)
    request_body: AdapterFromApiRequestBody | None = Field(default=None, alias="requestBody")
    response_mappings: list[AdapterResponseMapping] = Field(
        default_factory=list,
        alias="responseMappings",
    )
    cache: AdapterFromApiOperationCache
    response_contract: str = Field(alias="responseContract")
    response_model: str | None = Field(default=None, alias="responseModel")

    @field_validator("method", mode="before")
    @classmethod
    def _normalize_method(cls, value: Any) -> Any:
        if isinstance(value, str):
            return value.upper()
        return value

    @field_validator("path")
    @classmethod
    def _validate_path(cls, value: str) -> str:
        if not value.startswith("/"):
            raise ValueError("Adapter from API operation paths must start with /.")
        return value


class AdapterFromApiConnectionContract(AdapterFromApiBaseModel):
    contract_version: int = Field(alias="contractVersion")
    adapter: AdapterFromApiAdapterMetadata
    openapi: AdapterFromApiOpenApiMetadata
    config_variables: list[AdapterFromApiConfigVariable] = Field(
        default_factory=list,
        alias="configVariables",
    )
    secret_variables: list[AdapterFromApiSecretVariable] = Field(
        default_factory=list,
        alias="secretVariables",
    )
    available_operations: list[AdapterFromApiOperation] = Field(
        default_factory=list,
        alias="availableOperations",
    )
    health: AdapterFromApiHealth

    @model_validator(mode="after")
    def _validate_health_operation(self) -> AdapterFromApiConnectionContract:
        operation_ids = {operation.operation_id for operation in self.available_operations}
        if self.health.operation_id not in operation_ids:
            raise ValueError("health.operationId must reference an operation in availableOperations.")
        return self


def make_adapter_from_api_contract(
    *,
    adapter_id: str,
    title: str,
    description: str,
    openapi_url: str,
    operations: list[AdapterFromApiOperation],
    health_operation_id: str,
    config_variables: list[AdapterFromApiConfigVariable] | None = None,
    secret_variables: list[AdapterFromApiSecretVariable] | None = None,
    contract_version: int = 1,
    openapi_version: str = "3.1.0",
    openapi_checksum: str | None = None,
    expected_status: int = 200,
    timeout_ms: int = 5000,
) -> AdapterFromApiConnectionContract:
    """Build the strict well-known Adapter from API discovery contract."""

    return AdapterFromApiConnectionContract(
        contract_version=contract_version,
        adapter=AdapterFromApiAdapterMetadata(
            id=adapter_id,
            title=title,
            description=description,
        ),
        openapi=AdapterFromApiOpenApiMetadata(
            url=openapi_url,
            version=openapi_version,
            checksum=openapi_checksum,
        ),
        config_variables=config_variables or [],
        secret_variables=secret_variables or [],
        available_operations=operations,
        health=AdapterFromApiHealth(
            operation_id=health_operation_id,
            expected_status=expected_status,
            timeout_ms=timeout_ms,
        ),
    )


__all__ = [
    "AdapterFromApiAdapterMetadata",
    "AdapterFromApiBaseModel",
    "AdapterFromApiConfigVariable",
    "AdapterFromApiConnectionContract",
    "AdapterFromApiHealth",
    "AdapterFromApiMethod",
    "AdapterFromApiOpenApiMetadata",
    "AdapterFromApiOperation",
    "AdapterFromApiOperationCache",
    "AdapterFromApiOperationKind",
    "AdapterFromApiParameter",
    "AdapterFromApiParameterLocation",
    "AdapterFromApiRequestBody",
    "AdapterFromApiSecretInjection",
    "AdapterFromApiSecretVariable",
    "AdapterFromApiVariableType",
    "make_adapter_from_api_contract",
]
