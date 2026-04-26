from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, ClassVar

from pydantic import ConfigDict, Field

from ..base import BasePydanticModel
from .workspace import CommandCenterBaseObjectOrm


def _rename_query_params(params: dict[str, Any], aliases: dict[str, str]) -> dict[str, Any]:
    return {aliases.get(key, key): value for key, value in params.items()}


class ConnectionAccessMode(str, Enum):
    PROXY = "proxy"
    BROWSER = "browser"
    SERVER_ONLY = "server-only"


class ConnectionInstanceStatus(str, Enum):
    UNKNOWN = "unknown"
    OK = "ok"
    ERROR = "error"
    DISABLED = "disabled"


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
        "connection_type": ["exact"],
        "type_id": ["exact"],
        "status": ["exact"],
        "workspace_id": ["exact"],
        "is_default": ["exact"],
        "is_active": ["exact"],
    }
    FILTER_VALUE_NORMALIZERS: ClassVar[dict[str, str]] = {
        "connection_type": "id",
        "type_id": "str",
        "status": "str",
        "workspace_id": "str",
        "is_default": "bool",
        "is_active": "bool",
    }

    model_config = ConfigDict(
        populate_by_name=True,
        extra="ignore",
    )

    id: str = Field(
        description="Connection instance primary key.",
    )
    uid: str = Field(
        description="Stable connection instance uid used in detail URLs.",
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
    organization_id: str | None = Field(
        default=None,
        alias="organizationId",
        description="Owning organization id.",
    )
    workspace_id: str | None = Field(
        default=None,
        alias="workspaceId",
        description="Optional workspace scope id.",
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
    created_by: str | None = Field(
        default=None,
        alias="createdBy",
        description="User id that created the connection.",
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


__all__ = [
    "ConnectionAccessMode",
    "ConnectionInstance",
    "ConnectionInstanceStatus",
    "ConnectionType",
]
