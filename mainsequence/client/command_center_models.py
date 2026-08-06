from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime
from enum import Enum
from typing import Any, ClassVar, Literal
from urllib.parse import quote

from pydantic import ConfigDict, Field

from mainsequence.client.base import (
    BaseObjectOrm,
    BasePydanticModel,
    LabelableObjectMixin,
    ShareableObjectMixin,
)
from mainsequence.client.exceptions import ApiError

RegisteredWidgetKind = Literal["kpi", "chart", "table", "feed", "custom"]


class _CommandCenterBaseObjectOrm(BaseObjectOrm):
    """Base client for Command Center backend resources."""

    COMMAND_CENTER_PREFIX: ClassVar[str] = "api/v1/command_center"
    ENDPOINT: ClassVar[str]

    @classmethod
    def _command_center_root(cls) -> str:
        root = str(getattr(cls, "ROOT_URL", BaseObjectOrm.ROOT_URL)).rstrip("/")
        if root.endswith("/orm/api"):
            root = root[: -len("/orm/api")]
        return root

    @classmethod
    def get_object_url(cls, custom_endpoint_name=None):
        endpoint = custom_endpoint_name or getattr(cls, "ENDPOINT", None)
        if not endpoint:
            raise ValueError(f"{cls.__name__} must define ENDPOINT.")

        return (
            f"{cls._command_center_root().rstrip('/')}/"
            f"{cls.COMMAND_CENTER_PREFIX.strip('/')}/"
            f"{endpoint.strip('/')}"
        )


def _rename_query_params(params: dict[str, Any], aliases: dict[str, str]) -> dict[str, Any]:
    return {aliases.get(key, key): value for key, value in params.items()}


def _normalize_uid_csv(value: Any, *, field_name: str) -> str:
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes, dict)):
        return ",".join(
            BaseObjectOrm._coerce_filter_uid(item, field_name=field_name) for item in value
        )
    return BaseObjectOrm._coerce_filter_uid(value, field_name=field_name)


class ConnectionAccessMode(str, Enum):
    PROXY = "proxy"
    BROWSER = "browser"
    SERVER_ONLY = "server-only"


class ConnectionInstanceStatus(str, Enum):
    UNKNOWN = "unknown"
    OK = "ok"
    ERROR = "error"
    DISABLED = "disabled"


class ConnectionType(_CommandCenterBaseObjectOrm, BasePydanticModel):
    """Backend-owned Command Center connection type catalog entry."""

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

    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    type_id: str = Field(alias="id")
    type_version: int = Field(alias="version")
    title: str
    description: str = ""
    source: str
    category: str
    tags: list[str] = Field(default_factory=list)
    capabilities: list[str] = Field(default_factory=list)
    access_mode: ConnectionAccessMode | str = Field(alias="accessMode")
    public_config_schema: dict[str, Any] = Field(default_factory=dict, alias="publicConfigSchema")
    secure_config_schema: dict[str, Any] = Field(default_factory=dict, alias="secureConfigSchema")
    query_models: list[dict[str, Any]] = Field(default_factory=list, alias="queryModels")
    required_permissions: list[str] = Field(default_factory=list, alias="requiredPermissions")
    usage_guidance: str = Field(default="", alias="usageGuidance")
    examples: list[dict[str, Any]] = Field(default_factory=list)

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


class ConnectionInstance(_CommandCenterBaseObjectOrm, BasePydanticModel):
    """Backend-owned Command Center configured connection instance."""

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

    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    uid: str
    type_id: str = Field(alias="typeId")
    type_version: int = Field(alias="typeVersion")
    name: str
    description: str = ""
    organization_uid: str | None = Field(default=None, alias="organizationUid")
    workspace_uid: str | None = Field(default=None, alias="workspaceUid")
    public_config: dict[str, Any] = Field(default_factory=dict, alias="publicConfig")
    secure_fields: dict[str, bool] = Field(default_factory=dict, alias="secureFields")
    status: ConnectionInstanceStatus | str = ConnectionInstanceStatus.UNKNOWN
    status_message: str = Field(default="", alias="statusMessage")
    last_health_check_at: datetime | None = Field(default=None, alias="lastHealthCheckAt")
    is_default: bool = Field(default=False, alias="isDefault")
    is_system: bool = Field(default=False, alias="isSystem")
    tags: list[str] = Field(default_factory=list)
    created_by_user_uid: str | None = Field(default=None, alias="createdByUserUid")
    created_at: datetime = Field(alias="createdAt")
    updated_at: datetime = Field(alias="updatedAt")

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


class WorkspaceLayoutKind(str, Enum):
    CUSTOM = "custom"
    AUTO_GRID = "auto-grid"


class WorkspaceType(str, Enum):
    WORKSPACE = "workspace"
    AGENT_MONITOR = "agent-monitor"
    SLIDE_STUDIO = "slide-studio"


class Workspace(
    LabelableObjectMixin,
    ShareableObjectMixin,
    _CommandCenterBaseObjectOrm,
    BasePydanticModel,
):
    """Backend-owned Command Center workspace resource."""

    ENDPOINT: ClassVar[str] = "workspaces"
    FILTERSET_FIELDS: ClassVar[dict[str, list[str]]] = {
        "uid": ["exact", "in"],
        "exclude_uids": ["exact"],
        "title": ["exact", "contains"],
        "type": ["exact", "in"],
        "source": ["exact", "in"],
        "labels": ["exact", "in", "contains"],
    }
    FILTER_VALUE_NORMALIZERS: ClassVar[dict[str, Any]] = {
        "uid": "uid",
        "uid__in": "uid",
        "exclude_uids": _normalize_uid_csv,
        "title": "str",
        "type": "str",
        "type__in": "str",
        "source": "str",
        "source__in": "str",
        "labels": "str",
        "labels__in": "str",
        "labels__contains": "str",
    }

    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    uid: str
    title: str
    description: str = ""
    type: WorkspaceType = WorkspaceType.WORKSPACE
    public_url: str | None = None
    labels: list[str] = Field(default_factory=list)
    category: str = "Custom"
    source: str = "user"
    schema_version: int = Field(alias="schemaVersion")
    required_permissions: list[str] | None = Field(default=None, alias="requiredPermissions")
    grid: dict[str, Any] = Field(default_factory=dict)
    layout_kind: WorkspaceLayoutKind = Field(alias="layoutKind")
    auto_grid: dict[str, Any] = Field(default_factory=dict, alias="autoGrid")
    companions: list[dict[str, Any]] = Field(default_factory=list)
    controls: dict[str, Any] = Field(default_factory=dict)
    widgets: list[dict[str, Any]] = Field(default_factory=list)
    created_at: datetime = Field(alias="createdAt")
    updated_at: datetime = Field(alias="updatedAt")

    def _get_widget_detail_url(self, widget_instance_id: str) -> str:
        if not widget_instance_id:
            raise ValueError("widget_instance_id is required.")
        encoded_id = quote(str(widget_instance_id), safe="")
        return f"{self.get_detail_url().rstrip('/')}/widgets/{encoded_id}/"

    def _request_widget_action(
        self,
        *,
        r_type: str,
        widget_instance_id: str,
        action_suffix: str = "",
        payload: dict[str, Any] | None = None,
        timeout: int | float | tuple[float, float] | None = None,
        expected_statuses: tuple[int, ...] = (200,),
        empty_response: Any = None,
    ) -> Any:
        from mainsequence.client.exceptions import raise_for_response
        from mainsequence.client.utils import make_request

        request_payload = payload or {}
        url = self._get_widget_detail_url(widget_instance_id)
        if action_suffix:
            url = f"{url.rstrip('/')}/{action_suffix.strip('/')}/"

        response = make_request(
            s=type(self).build_session(),
            loaders=type(self).LOADERS,
            r_type=r_type,
            url=url,
            payload=request_payload,
            time_out=timeout,
        )
        if response.status_code not in expected_statuses:
            raise_for_response(response, payload=request_payload or None)
        if not getattr(response, "content", b""):
            return empty_response
        return response.json()

    def patch_workspace_widget(
        self,
        widget_instance_id: str,
        *,
        widget: dict[str, Any],
        timeout: int | float | tuple[float, float] | None = None,
    ) -> WorkspaceWidgetMutationResult:
        if not isinstance(widget, dict):
            raise TypeError("widget must be a dictionary payload.")

        data = self._request_widget_action(
            r_type="PATCH",
            widget_instance_id=widget_instance_id,
            payload={"json": {"widget": widget}},
            timeout=timeout,
        )
        if not isinstance(data, dict):
            raise ApiError(f"Unexpected workspace widget patch response: {type(data)!r}")
        return WorkspaceWidgetMutationResult.model_validate(data)

    def delete_workspace_widget(
        self,
        widget_instance_id: str,
        *,
        recursive: bool = False,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> None:
        payload: dict[str, Any] = {}
        if recursive:
            payload["params"] = {"recursive": "true"}
        self._request_widget_action(
            r_type="DELETE",
            widget_instance_id=widget_instance_id,
            payload=payload,
            timeout=timeout,
            expected_statuses=(204,),
        )

    def move_workspace_widget(
        self,
        widget_instance_id: str,
        *,
        parent_widget_id: str | None = None,
        index: int | None = None,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> WorkspaceWidgetMutationResult:
        payload_data: dict[str, Any] = {}
        if parent_widget_id is not None:
            payload_data["parentWidgetId"] = str(parent_widget_id)
        if index is not None:
            payload_data["index"] = int(index)

        data = self._request_widget_action(
            r_type="POST",
            widget_instance_id=widget_instance_id,
            action_suffix="move",
            payload={"json": payload_data},
            timeout=timeout,
        )
        if not isinstance(data, dict):
            raise ApiError(f"Unexpected workspace widget move response: {type(data)!r}")
        return WorkspaceWidgetMutationResult.model_validate(data)


class WorkspaceWidgetMutationResult(BasePydanticModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    workspace_uid: str = Field(alias="workspaceUid")
    widget_instance_id: str = Field(alias="widgetInstanceId")
    parent_widget_id: str | None = Field(default=None, alias="parentWidgetId")
    widget: dict[str, Any]
    updated_at: datetime = Field(alias="updatedAt")


class RegisteredWidgetType(_CommandCenterBaseObjectOrm, BasePydanticModel):
    """Backend-owned registered widget catalog entry."""

    ENDPOINT: ClassVar[str] = "widget-types"
    FILTERSET_FIELDS: ClassVar[dict[str, list[str]]] = {
        "widget_id": ["exact"],
        "category": ["exact"],
        "kind": ["exact"],
        "source": ["exact"],
        "is_active": ["exact"],
        "include_inactive": ["exact"],
    }
    FILTER_VALUE_NORMALIZERS: ClassVar[dict[str, Any]] = {
        "widget_id": "str",
        "category": "str",
        "kind": "str",
        "source": "str",
        "is_active": "bool",
        "include_inactive": "bool",
    }

    model_config = ConfigDict(populate_by_name=True, extra="allow")

    widget_id: str = Field(alias="widgetId")
    title: str
    description: str
    category: str
    widget_version: str | None = Field(default=None, alias="widgetVersion")
    kind: RegisteredWidgetKind
    source: str
    tags: list[str] | None = None
    required_permissions: list[str] | None = Field(default=None, alias="requiredPermissions")
    schema_payload: dict[str, Any] | None = Field(default=None, alias="schema")
    io: dict[str, Any] | None = None
    default_presentation: dict[str, Any] | None = Field(default=None, alias="defaultPresentation")
    default_size: dict[str, Any] | None = Field(default=None, alias="defaultSize")
    responsive: dict[str, Any] | list[Any] | None = None
    usage_guidance: dict[str, Any] | list[Any] | str | None = Field(
        default=None,
        alias="usageGuidance",
    )
    capabilities: dict[str, Any] | list[Any] | None = None
    examples: dict[str, Any] | list[Any] | None = None
    is_active: bool = Field(alias="isActive")
    registry_version: str = Field(alias="registryVersion")
    checksum: str
    last_synced_at: datetime | None = Field(default=None, alias="lastSyncedAt")
    created_at: datetime = Field(alias="createdAt")
    updated_at: datetime = Field(alias="updatedAt")

    @classmethod
    def get(cls, pk=None, timeout=None, **filters):
        widget_id = filters.pop("widget_id", None)
        if pk is None and widget_id is not None:
            pk = widget_id
        return super().get(pk=pk, timeout=timeout, **filters)


__all__ = [
    "ConnectionAccessMode",
    "ConnectionInstance",
    "ConnectionInstanceStatus",
    "ConnectionType",
    "RegisteredWidgetKind",
    "RegisteredWidgetType",
    "Workspace",
    "WorkspaceLayoutKind",
    "WorkspaceType",
    "WorkspaceWidgetMutationResult",
]
