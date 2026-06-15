from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .response_mapping import AdapterResponseMapping

AdapterFromApiMethod = Literal["GET", "POST", "PUT", "PATCH", "DELETE"]
AdapterFromApiOperationKind = Literal["query", "action", "health"]
AdapterFromApiVariableType = Literal["string", "number", "boolean", "select", "json"]
AdapterFromApiParameterLocation = Literal["path", "query", "header"]


class AdapterFromApiBaseModel(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class AdapterFromApiAdapterMetadata(AdapterFromApiBaseModel):
    type: Literal["adapter-from-api"] = "adapter-from-api"
    id: str
    title: str
    description: str | None = None


class AdapterFromApiOpenApiMetadata(AdapterFromApiBaseModel):
    url: str
    version: str = "3.1.0"
    checksum: str | None = None


class AdapterFromApiConfigVariable(AdapterFromApiBaseModel):
    key: str
    label: str
    type: AdapterFromApiVariableType = "string"
    required: bool = False
    default: Any = None
    description: str | None = None
    options: list[dict[str, Any]] | None = None


class AdapterFromApiSecretInjection(AdapterFromApiBaseModel):
    type: str
    name: str | None = None
    prefix: str | None = None


class AdapterFromApiSecretVariable(AdapterFromApiBaseModel):
    key: str
    label: str
    type: Literal["secret"] = "secret"
    required: bool = True
    injection: AdapterFromApiSecretInjection
    description: str | None = None


class AdapterFromApiParameter(AdapterFromApiBaseModel):
    name: str
    location: AdapterFromApiParameterLocation = Field(default="query", alias="in")
    label: str | None = None
    description: str | None = None
    type: AdapterFromApiVariableType = "string"
    required: bool = False
    default: Any = None
    enum: list[Any] | None = None


class AdapterFromApiRequestBody(AdapterFromApiBaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True, serialize_by_alias=True)

    required: bool = False
    contentType: str = "application/json"
    schema_: dict[str, Any] | None = Field(default=None, alias="schema")
    description: str | None = None

    @property
    def schema(self) -> dict[str, Any] | None:
        return self.schema_


class AdapterFromApiOperationCache(AdapterFromApiBaseModel):
    enabled: bool = True
    ttlSeconds: int | None = None
    dedupe: bool = True


class AdapterFromApiOperation(AdapterFromApiBaseModel):
    operationId: str
    label: str
    description: str = ""
    method: AdapterFromApiMethod = "GET"
    path: str
    kind: AdapterFromApiOperationKind = "query"
    capabilities: list[str] = Field(default_factory=lambda: ["query"])
    requiresTimeRange: bool = False
    supportsVariables: bool = True
    supportsMaxRows: bool = True
    parameters: list[AdapterFromApiParameter] = Field(default_factory=list)
    requestBody: AdapterFromApiRequestBody | None = None
    responseMappings: list[AdapterResponseMapping] = Field(default_factory=list)
    cache: AdapterFromApiOperationCache = Field(default_factory=AdapterFromApiOperationCache)

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


class AdapterFromApiHealth(AdapterFromApiBaseModel):
    operationId: str = "health"
    expectedStatus: int = 200
    timeoutMs: int = 5000


class AdapterFromApiConnectionContract(AdapterFromApiBaseModel):
    contractVersion: int = 1
    adapter: AdapterFromApiAdapterMetadata
    openapi: AdapterFromApiOpenApiMetadata
    configVariables: list[AdapterFromApiConfigVariable] = Field(default_factory=list)
    secretVariables: list[AdapterFromApiSecretVariable] = Field(default_factory=list)
    availableOperations: list[AdapterFromApiOperation] = Field(default_factory=list)
    health: AdapterFromApiHealth

    @model_validator(mode="after")
    def _validate_health_operation(self) -> AdapterFromApiConnectionContract:
        operation_ids = {operation.operationId for operation in self.availableOperations}
        if self.health.operationId not in operation_ids:
            raise ValueError("health.operationId must reference an operation in availableOperations.")
        return self


def make_adapter_from_api_contract(
    *,
    adapter_id: str,
    title: str,
    openapi_url: str,
    operations: list[AdapterFromApiOperation],
    health_operation_id: str = "health",
    description: str | None = None,
    config_variables: list[AdapterFromApiConfigVariable] | None = None,
    secret_variables: list[AdapterFromApiSecretVariable] | None = None,
    openapi_version: str = "3.1.0",
    openapi_checksum: str | None = None,
) -> AdapterFromApiConnectionContract:
    """Build the strict well-known Adapter from API discovery contract."""

    return AdapterFromApiConnectionContract(
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
        configVariables=config_variables or [],
        secretVariables=secret_variables or [],
        availableOperations=operations,
        health=AdapterFromApiHealth(operationId=health_operation_id),
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
