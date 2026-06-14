from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, ClassVar
from urllib.parse import urljoin, urlparse

from pydantic import ConfigDict, Field, StrictBool, StrictInt, model_validator

from ..base import BasePydanticModel
from .workspace import CommandCenterBaseObjectOrm

CONNECTION_TYPE_ADAPTER_FROM_API = "command_center.adapter_from_api"
ADAPTER_FROM_API_TRANSPORT_BACKEND = "backend"
ADAPTER_FROM_API_TRANSPORT_DIRECT = "direct"
ADAPTER_FROM_API_MIN_TIMEOUT_MS = 1000
ADAPTER_FROM_API_MAX_TIMEOUT_MS = 30000
ADAPTER_FROM_API_MAX_QUERY_CACHE_TTL_MS = 3600000


def _rename_query_params(params: dict[str, Any], aliases: dict[str, str]) -> dict[str, Any]:
    return {aliases.get(key, key): value for key, value in params.items()}


def _normalize_adapter_from_api_base_url(value: Any, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} is required.")
    parsed = urlparse(value.strip().rstrip("/"))
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError(f"{field_name} must be an absolute http or https URL.")
    if parsed.username or parsed.password:
        raise ValueError(f"{field_name} must not include URL credentials.")
    if parsed.query or parsed.fragment:
        raise ValueError(f"{field_name} must not include query strings or fragments.")
    return parsed.geturl().rstrip("/")


def _adapter_from_api_contract_definition_url(api_base_url: str) -> str:
    return urljoin(
        api_base_url.rstrip("/") + "/",
        ".well-known/command-center/connection-contract",
    )


def _adapter_from_api_openapi_url(api_base_url: str) -> str:
    return urljoin(api_base_url.rstrip("/") + "/", "openapi.json")


def _validate_or_set_adapter_from_api_url(
    model: AdapterFromApiConnectionPublicConfig,
    *,
    attr_name: str,
    field_name: str,
    expected: str,
) -> None:
    value = getattr(model, attr_name)
    if value in (None, ""):
        setattr(model, attr_name, expected)
        return
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string.")
    if value.rstrip("/") != expected.rstrip("/"):
        raise ValueError(f"{field_name} must match the URL derived from the API base URL.")


class ConnectionAccessMode(str, Enum):
    PROXY = "proxy"
    BROWSER = "browser"
    SERVER_ONLY = "server-only"


class ConnectionInstanceStatus(str, Enum):
    UNKNOWN = "unknown"
    OK = "ok"
    ERROR = "error"
    DISABLED = "disabled"


class AdapterFromApiTransportMode(str, Enum):
    BACKEND = ADAPTER_FROM_API_TRANSPORT_BACKEND
    DIRECT = ADAPTER_FROM_API_TRANSPORT_DIRECT


class AdapterFromApiQueryCachePolicy(str, Enum):
    DISABLED = "disabled"
    SAFE = "safe"


class AdapterFromApiConnectionPublicConfig(BasePydanticModel):
    """Strict publicConfig payload for command_center.adapter_from_api connections."""

    model_config = ConfigDict(
        populate_by_name=True,
        extra="forbid",
    )

    api_base_url: str | None = Field(
        default=None,
        alias="apiBaseUrl",
        description="Backend-mode upstream API base URL.",
    )
    debug_api_base_url: str | None = Field(
        default=None,
        alias="debugApiBaseUrl",
        description="Direct-mode browser API base URL.",
    )
    transport_mode: AdapterFromApiTransportMode | None = Field(
        default=None,
        alias="transportMode",
        description="Direct debug mode marker. Omit for backend mode.",
    )
    contract_definition_url: str | None = Field(
        default=None,
        alias="contractDefinitionUrl",
        description="Derived Command Center connection contract URL.",
    )
    open_api_url: str | None = Field(
        default=None,
        alias="openApiUrl",
        description="Derived OpenAPI document URL.",
    )
    compiled_contract_source: str | None = Field(
        default=None,
        alias="compiledContractSource",
        description="Direct-mode compiled contract source marker.",
    )
    compiled_contract_source_url: str | None = Field(
        default=None,
        alias="compiledContractSourceUrl",
        description="Direct-mode compiled contract source URL.",
    )
    compiled_contract: dict[str, Any] | None = Field(
        default=None,
        alias="compiledContract",
        description="Optional browser-compiled or backend-discovered contract.",
    )
    config_values: dict[str, Any] = Field(
        default_factory=dict,
        alias="configValues",
        description="Non-secret config variable values declared by the contract.",
    )
    contract_version: str | None = Field(
        default=None,
        alias="contractVersion",
        description="Optional contract version pin.",
    )
    request_timeout_ms: StrictInt | None = Field(
        default=None,
        ge=ADAPTER_FROM_API_MIN_TIMEOUT_MS,
        le=ADAPTER_FROM_API_MAX_TIMEOUT_MS,
        alias="requestTimeoutMs",
        description="Request timeout in milliseconds.",
    )
    query_cache_policy: AdapterFromApiQueryCachePolicy | None = Field(
        default=None,
        alias="queryCachePolicy",
        description="Adapter query cache policy.",
    )
    query_cache_ttl_ms: StrictInt | None = Field(
        default=None,
        ge=0,
        le=ADAPTER_FROM_API_MAX_QUERY_CACHE_TTL_MS,
        alias="queryCacheTtlMs",
        description="Adapter query cache TTL in milliseconds.",
    )
    dedupe_in_flight: StrictBool | None = Field(
        default=None,
        alias="dedupeInFlight",
        description="Whether identical in-flight operations should be deduplicated.",
    )

    @model_validator(mode="after")
    def _validate_adapter_from_api_contract(self):
        if self.transport_mode not in (None, AdapterFromApiTransportMode.DIRECT):
            raise ValueError("transportMode must be omitted unless direct mode is used.")

        if self.transport_mode == AdapterFromApiTransportMode.DIRECT:
            if self.api_base_url is not None:
                raise ValueError("apiBaseUrl is not supported for direct mode.")
            normalized_base_url = _normalize_adapter_from_api_base_url(
                self.debug_api_base_url,
                field_name="debugApiBaseUrl",
            )
            self.debug_api_base_url = normalized_base_url
            self.transport_mode = AdapterFromApiTransportMode.DIRECT
            contract_url = _adapter_from_api_contract_definition_url(normalized_base_url)
            openapi_url = _adapter_from_api_openapi_url(normalized_base_url)
            _validate_or_set_adapter_from_api_url(
                self,
                attr_name="contract_definition_url",
                field_name="contractDefinitionUrl",
                expected=contract_url,
            )
            _validate_or_set_adapter_from_api_url(
                self,
                attr_name="open_api_url",
                field_name="openApiUrl",
                expected=openapi_url,
            )
            _validate_or_set_adapter_from_api_url(
                self,
                attr_name="compiled_contract_source_url",
                field_name="compiledContractSourceUrl",
                expected=contract_url,
            )
            if self.compiled_contract_source in (None, ""):
                self.compiled_contract_source = ADAPTER_FROM_API_TRANSPORT_DIRECT
            elif self.compiled_contract_source != ADAPTER_FROM_API_TRANSPORT_DIRECT:
                raise ValueError("compiledContractSource must be 'direct' for direct mode.")
            if self.config_values and not self.compiled_contract:
                raise ValueError(
                    "Direct mode requires compiledContract when configValues contains fields."
                )
            return self

        if self.debug_api_base_url is not None:
            raise ValueError("debugApiBaseUrl is only supported for direct mode.")
        if self.compiled_contract_source is not None:
            raise ValueError("compiledContractSource is only supported for direct mode.")
        if self.compiled_contract_source_url is not None:
            raise ValueError("compiledContractSourceUrl is only supported for direct mode.")

        normalized_base_url = _normalize_adapter_from_api_base_url(
            self.api_base_url,
            field_name="apiBaseUrl",
        )
        self.api_base_url = normalized_base_url
        _validate_or_set_adapter_from_api_url(
            self,
            attr_name="contract_definition_url",
            field_name="contractDefinitionUrl",
            expected=_adapter_from_api_contract_definition_url(normalized_base_url),
        )
        _validate_or_set_adapter_from_api_url(
            self,
            attr_name="open_api_url",
            field_name="openApiUrl",
            expected=_adapter_from_api_openapi_url(normalized_base_url),
        )
        return self

    @classmethod
    def direct(
        cls,
        *,
        debug_api_base_url: str,
        config_values: dict[str, Any] | None = None,
        compiled_contract: dict[str, Any] | None = None,
        contract_version: str | None = "",
        request_timeout_ms: int | None = ADAPTER_FROM_API_MAX_TIMEOUT_MS,
        query_cache_policy: AdapterFromApiQueryCachePolicy | str | None = (
            AdapterFromApiQueryCachePolicy.SAFE
        ),
        query_cache_ttl_ms: int | None = 300000,
        dedupe_in_flight: bool | None = True,
    ) -> AdapterFromApiConnectionPublicConfig:
        payload: dict[str, Any] = {
            "transportMode": ADAPTER_FROM_API_TRANSPORT_DIRECT,
            "debugApiBaseUrl": debug_api_base_url,
            "configValues": config_values or {},
        }
        optional_values = {
            "compiledContract": compiled_contract,
            "contractVersion": contract_version,
            "requestTimeoutMs": request_timeout_ms,
            "queryCachePolicy": query_cache_policy,
            "queryCacheTtlMs": query_cache_ttl_ms,
            "dedupeInFlight": dedupe_in_flight,
        }
        payload.update({key: value for key, value in optional_values.items() if value is not None})
        return cls.model_validate(payload)

    @classmethod
    def backend(
        cls,
        *,
        api_base_url: str,
        config_values: dict[str, Any] | None = None,
        compiled_contract: dict[str, Any] | None = None,
        contract_version: str | None = None,
        request_timeout_ms: int | None = None,
        query_cache_policy: AdapterFromApiQueryCachePolicy | str | None = None,
        query_cache_ttl_ms: int | None = None,
        dedupe_in_flight: bool | None = None,
    ) -> AdapterFromApiConnectionPublicConfig:
        payload: dict[str, Any] = {
            "apiBaseUrl": api_base_url,
            "configValues": config_values or {},
        }
        optional_values = {
            "compiledContract": compiled_contract,
            "contractVersion": contract_version,
            "requestTimeoutMs": request_timeout_ms,
            "queryCachePolicy": query_cache_policy,
            "queryCacheTtlMs": query_cache_ttl_ms,
            "dedupeInFlight": dedupe_in_flight,
        }
        payload.update({key: value for key, value in optional_values.items() if value is not None})
        return cls.model_validate(payload)

    def to_public_config(self) -> dict[str, Any]:
        return self.model_dump(
            mode="json",
            by_alias=True,
            exclude_none=True,
            exclude={"orm_class"},
        )


def _adapter_from_api_public_config_payload(
    public_config: AdapterFromApiConnectionPublicConfig | dict[str, Any],
) -> dict[str, Any]:
    if isinstance(public_config, AdapterFromApiConnectionPublicConfig):
        config = public_config
    else:
        config = AdapterFromApiConnectionPublicConfig.model_validate(public_config)
    return config.to_public_config()


class ConnectionType(CommandCenterBaseObjectOrm, BasePydanticModel):
    """Command Center connection type catalog entry.

    Connection types are registry-synced catalog definitions. The public API is
    read-only except for the admin-only sync endpoint, so this SDK model is
    intended for discovery and lookup.
    """

    ENDPOINT: ClassVar[str] = "connection-types"
    FILTERSET_FIELDS: ClassVar[dict[str, list[str]]] = {
        "type_id": ["exact"],
        "category": ["exact"],
        "source": ["exact"],
        "access_mode": ["exact"],
        "is_active": ["exact"],
        "include_inactive": ["exact"],
    }
    FILTER_VALUE_NORMALIZERS: ClassVar[dict[str, str]] = {
        "type_id": "str",
        "category": "str",
        "source": "str",
        "access_mode": "str",
        "is_active": "bool",
        "include_inactive": "bool",
    }

    model_config = ConfigDict(
        populate_by_name=True,
        extra="ignore",
    )

    type_id: str = Field(
        alias="id",
        description="Stable connection type identifier.",
    )
    type_version: int = Field(
        alias="version",
        description="Connection type manifest version.",
    )
    title: str = Field(
        description="Human-readable connection type title.",
    )
    description: str = Field(
        default="",
        description="Connection type description.",
    )
    source: str = Field(
        description="Origin of the connection type definition.",
    )
    category: str = Field(
        description="Connection type catalog category.",
    )
    tags: list[str] = Field(
        default_factory=list,
        description="Connection type tags.",
    )
    capabilities: list[str] = Field(
        default_factory=list,
        description="Runtime capabilities advertised by this connection type.",
    )
    access_mode: ConnectionAccessMode | str = Field(
        alias="accessMode",
        description="How the connection is accessed at runtime.",
    )
    public_config_schema: dict[str, Any] = Field(
        default_factory=dict,
        alias="publicConfigSchema",
        description="JSON schema for non-secret connection configuration.",
    )
    secure_config_schema: dict[str, Any] = Field(
        default_factory=dict,
        alias="secureConfigSchema",
        description="JSON schema for secret connection configuration.",
    )
    query_models: list[dict[str, Any]] = Field(
        default_factory=list,
        alias="queryModels",
        description="Query models supported by this connection type.",
    )
    required_permissions: list[str] = Field(
        default_factory=list,
        alias="requiredPermissions",
        description="Permission codes required to use this connection type.",
    )
    usage_guidance: str = Field(
        default="",
        alias="usageGuidance",
        description="Human-readable usage guidance.",
    )
    examples: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Example connection or query payloads.",
    )

    @classmethod
    def _normalize_filter_kwargs(cls, kwargs: dict[str, Any]) -> dict[str, Any]:
        params = super()._normalize_filter_kwargs(kwargs)
        return _rename_query_params(
            params,
            {
                "is_active": "isActive",
                "include_inactive": "includeInactive",
            },
        )

    @classmethod
    def get(cls, pk=None, timeout=None, **filters):
        type_id = filters.pop("type_id", None)
        if pk is None and type_id is not None:
            pk = type_id

        return super().get(pk=pk, timeout=timeout, **filters)


class ConnectionInstance(CommandCenterBaseObjectOrm, BasePydanticModel):
    """Command Center configured connection instance."""

    ENDPOINT: ClassVar[str] = "connections"
    FILTERSET_FIELDS: ClassVar[dict[str, list[str]]] = {
        "type_id": ["exact"],
        "status": ["exact"],
        "workspace_uid": ["exact"],
        "is_default": ["exact"],
        "is_active": ["exact"],
    }
    FILTER_VALUE_NORMALIZERS: ClassVar[dict[str, str]] = {
        "type_id": "str",
        "status": "str",
        "workspace_uid": "uid",
        "is_default": "bool",
        "is_active": "bool",
    }

    model_config = ConfigDict(
        populate_by_name=True,
        extra="ignore",
    )

    uid: str = Field(
        description="Connection instance public UID used for detail routes and actions.",
    )
    type_id: str = Field(
        alias="typeId",
        description="Related connection type identifier.",
    )
    type_version: int = Field(
        alias="typeVersion",
        description="Related connection type version.",
    )
    name: str = Field(
        description="Human-readable connection name.",
    )
    description: str = Field(
        default="",
        description="Connection description.",
    )
    organization_uid: str | None = Field(
        default=None,
        alias="organizationUid",
        description="Owning organization UID.",
    )
    workspace_uid: str | None = Field(
        default=None,
        alias="workspaceUid",
        description="Optional workspace scope UID.",
    )
    public_config: dict[str, Any] = Field(
        default_factory=dict,
        alias="publicConfig",
        description="Non-secret connection configuration.",
    )
    secure_fields: dict[str, bool] = Field(
        default_factory=dict,
        alias="secureFields",
        description="Map of secure config field names to presence flags.",
    )
    status: ConnectionInstanceStatus | str = Field(
        default=ConnectionInstanceStatus.UNKNOWN,
        description="Connection health/status.",
    )
    status_message: str = Field(
        default="",
        alias="statusMessage",
        description="Human-readable status detail.",
    )
    last_health_check_at: datetime | None = Field(
        default=None,
        alias="lastHealthCheckAt",
        description="Timestamp of the last connection health check.",
    )
    is_default: bool = Field(
        default=False,
        alias="isDefault",
        description="Whether this is the default connection for its scope/type.",
    )
    is_system: bool = Field(
        default=False,
        alias="isSystem",
        description="Whether this is a system-managed connection.",
    )
    tags: list[str] = Field(
        default_factory=list,
        description="Connection tags.",
    )
    created_by_user_uid: str | None = Field(
        default=None,
        alias="createdByUserUid",
        description="UID of the user that created the connection.",
    )
    created_at: datetime = Field(
        alias="createdAt",
        description="Timestamp when the connection was created.",
    )
    updated_at: datetime = Field(
        alias="updatedAt",
        description="Timestamp when the connection was last updated.",
    )

    @classmethod
    def _normalize_filter_kwargs(cls, kwargs: dict[str, Any]) -> dict[str, Any]:
        params = super()._normalize_filter_kwargs(kwargs)
        return _rename_query_params(
            params,
            {
                "workspace_uid": "workspaceUid",
                "is_default": "isDefault",
                "is_active": "isActive",
            },
        )

    @classmethod
    def get(cls, pk=None, timeout=None, **filters):
        uid = filters.pop("uid", None)
        if pk is None and uid is not None:
            pk = uid

        return super().get(pk=pk, timeout=timeout, **filters)

    @classmethod
    def create_adapter_from_api(
        cls,
        *,
        name: str,
        public_config: AdapterFromApiConnectionPublicConfig | dict[str, Any],
        description: str = "",
        secure_config: dict[str, Any] | None = None,
        workspace_uid: str | None = None,
        is_default: bool | None = None,
        tags: list[str] | None = None,
        timeout=None,
    ) -> ConnectionInstance:
        payload: dict[str, Any] = {
            "typeId": CONNECTION_TYPE_ADAPTER_FROM_API,
            "name": name,
            "description": description,
            "publicConfig": _adapter_from_api_public_config_payload(public_config),
        }
        if secure_config is not None:
            payload["secureConfig"] = secure_config
        if workspace_uid is not None:
            payload["workspaceUid"] = workspace_uid
        if is_default is not None:
            payload["isDefault"] = is_default
        if tags is not None:
            payload["tags"] = tags

        return cls.create(timeout=timeout, **payload)

    @classmethod
    def create_adapter_from_api_direct(
        cls,
        *,
        name: str,
        debug_api_base_url: str,
        description: str = "",
        workspace_uid: str | None = None,
        is_default: bool | None = None,
        tags: list[str] | None = None,
        config_values: dict[str, Any] | None = None,
        compiled_contract: dict[str, Any] | None = None,
        contract_version: str | None = "",
        request_timeout_ms: int | None = ADAPTER_FROM_API_MAX_TIMEOUT_MS,
        query_cache_policy: AdapterFromApiQueryCachePolicy | str | None = (
            AdapterFromApiQueryCachePolicy.SAFE
        ),
        query_cache_ttl_ms: int | None = 300000,
        dedupe_in_flight: bool | None = True,
        timeout=None,
    ) -> ConnectionInstance:
        public_config = AdapterFromApiConnectionPublicConfig.direct(
            debug_api_base_url=debug_api_base_url,
            config_values=config_values,
            compiled_contract=compiled_contract,
            contract_version=contract_version,
            request_timeout_ms=request_timeout_ms,
            query_cache_policy=query_cache_policy,
            query_cache_ttl_ms=query_cache_ttl_ms,
            dedupe_in_flight=dedupe_in_flight,
        )
        return cls.create_adapter_from_api(
            name=name,
            description=description,
            public_config=public_config,
            workspace_uid=workspace_uid,
            is_default=is_default,
            tags=tags,
            timeout=timeout,
        )

    @classmethod
    def filter_adapter_from_api(cls, timeout=None, **filters) -> list[ConnectionInstance]:
        requested_type_id = filters.pop("type_id", None)
        if requested_type_id not in (None, CONNECTION_TYPE_ADAPTER_FROM_API):
            raise ValueError(
                "filter_adapter_from_api only supports command_center.adapter_from_api."
            )
        return cls.filter(
            type_id=CONNECTION_TYPE_ADAPTER_FROM_API,
            timeout=timeout,
            **filters,
        )

    @classmethod
    def get_adapter_from_api(
        cls,
        uid: str | None = None,
        timeout=None,
        **filters,
    ) -> ConnectionInstance:
        requested_type_id = filters.pop("type_id", None)
        if requested_type_id not in (None, CONNECTION_TYPE_ADAPTER_FROM_API):
            raise ValueError("get_adapter_from_api only supports command_center.adapter_from_api.")

        if uid is not None:
            connection = cls.get(uid=uid, timeout=timeout, **filters)
            if connection.type_id != CONNECTION_TYPE_ADAPTER_FROM_API:
                raise ValueError(
                    f"Connection '{uid}' is type '{connection.type_id}', not "
                    f"'{CONNECTION_TYPE_ADAPTER_FROM_API}'."
                )
            return connection

        return cls.get(
            type_id=CONNECTION_TYPE_ADAPTER_FROM_API,
            timeout=timeout,
            **filters,
        )


__all__ = [
    "ADAPTER_FROM_API_TRANSPORT_BACKEND",
    "ADAPTER_FROM_API_TRANSPORT_DIRECT",
    "AdapterFromApiConnectionPublicConfig",
    "AdapterFromApiQueryCachePolicy",
    "AdapterFromApiTransportMode",
    "ConnectionAccessMode",
    "ConnectionInstance",
    "ConnectionInstanceStatus",
    "ConnectionType",
    "CONNECTION_TYPE_ADAPTER_FROM_API",
]
