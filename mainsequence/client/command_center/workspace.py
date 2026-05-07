from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, ClassVar, Literal
from urllib.parse import quote

from pydantic import ConfigDict, Field

from ..base import BaseObjectOrm, BasePydanticModel, LabelableObjectMixin, ShareableObjectMixin
from ..exceptions import ApiError

RegisteredWidgetKind = Literal["kpi", "chart", "table", "feed", "custom"]


class WorkspaceLayoutKind(str, Enum):
    CUSTOM = "custom"
    AUTO_GRID = "auto-grid"


class CommandCenterBaseObjectOrm(BaseObjectOrm):
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


class WorkspaceType(str, Enum):
    WORKSPACE = "workspace"
    AGENT_MONITOR = "agent-monitor"
    SLIDE_STUDIO = "slide-studio"


class Workspace(LabelableObjectMixin, ShareableObjectMixin, CommandCenterBaseObjectOrm, BasePydanticModel):
    """Command Center shared workspace payload.

    A workspace stores shared dashboard structure such as mounted widgets, layout,
    controls, and metadata. Per-user runtime state is stored separately.

    Use widget-scoped mutation methods when the task only affects one mounted
    widget instance:

    - `patch_workspace_widget(...)`
    - `delete_workspace_widget(...)`
    - `move_workspace_widget(...)`

    Those endpoints mutate one widget in place without requiring a full workspace
    fetch/patch round-trip.
    """

    ENDPOINT: ClassVar[str] = "workspaces"
    FILTERSET_FIELDS: ClassVar[dict[str, list[str]]] = {
        "id": ["exact", "in"],
        "title": ["exact", "contains"],
        "source": ["exact", "in"],
        "labels": ["exact", "in", "contains"],
    }
    FILTER_VALUE_NORMALIZERS: ClassVar[dict[str, str]] = {
        "id": "id",
        "id__in": "id",
        "title": "str",
        "source": "str",
        "labels": "str",
        "labels__in": "str",
        "labels__contains": "str",
    }

    model_config = ConfigDict(
        populate_by_name=True,
        extra="ignore",
    )

    id: int = Field(
        description="Workspace primary key.",
    )
    title: str = Field(
        description="Human-readable workspace title.",
    )
    type:str=Field(
        description="Type of workspace ",
    )
    description: str = Field(
        default="",
        description="Free-form workspace description.",
    )
    type: WorkspaceType = Field(default=WorkspaceType.WORKSPACE)
    public_url:str=Field(description="Public URL for workspace endpoint if exists.", default="")
    created_at:datetime=Field()
    labels: list[str] = Field(
        default_factory=list,
        description=(
            "User-defined workspace labels used only for organization and discovery. "
            "They do not change workspace runtime behavior or functionality."
        ),
    )
    category: str = Field(
        default="Custom",
        description="Workspace category label.",
    )
    source: str = Field(
        default="user",
        description="Origin of the workspace definition.",
    )
    schema_version: int = Field(
        alias="schemaVersion",
        description="Version of the workspace document schema.",
    )
    required_permissions: list[str] | None = Field(
        default=None,
        alias="requiredPermissions",
        description="Optional permission codes required to access or use the workspace.",
    )
    grid: dict[str, Any] = Field(
        default_factory=dict,
        description="Shared grid layout configuration.",
    )
    layout_kind: WorkspaceLayoutKind = Field(
        alias="layoutKind",
        description="Workspace layout mode.",
    )
    auto_grid: dict[str, Any] = Field(
        default_factory=dict,
        alias="autoGrid",
        description="Auto-grid configuration used when the layout kind is auto-grid.",
    )
    companions: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Auxiliary companion panels or companion definitions attached to the workspace.",
    )
    controls: dict[str, Any] = Field(
        default_factory=dict,
        description="Shared control configuration. Per-user selected values are not injected here.",
    )
    widgets: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Shared widget instances mounted in the workspace. Per-user runtime state is not injected here.",
    )
    created_at: datetime = Field(
        alias="createdAt",
        description="Timestamp when the workspace was created.",
    )
    updated_at: datetime = Field(
        alias="updatedAt",
        description="Timestamp when the workspace was last updated.",
    )

    def _get_widget_detail_url(self, widget_instance_id: str) -> str:
        if not widget_instance_id:
            raise ValueError("widget_instance_id is required.")
        return f"{self.get_detail_url().rstrip('/')}/widgets/{quote(str(widget_instance_id), safe='')}/"

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
        from ..exceptions import raise_for_response
        from ..utils import make_request

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
        """Partially update one mounted workspace widget by instance id.

        This endpoint accepts a shallow top-level widget patch under the `widget`
        key and returns the updated widget instance plus parent placement data.

        Important behavior:

        - `widget` is required.
        - The patch is shallow. If you send `props`, `layout`, `bindings`, or
          `row`, that whole top-level value replaces the previous value.
        - `runtimeState` is write-only convenience. It is persisted into the
          current user's workspace user-state, not the shared workspace JSON.
        - If `widget.id` is present, it must match `widget_instance_id`.
        - This endpoint does not allow replacing `widget.row.children`.
        - `widget.widgetId` may be changed, but it must remain a valid registered
          widget type.
        """
        if not isinstance(widget, dict):
            raise TypeError("widget must be a dictionary payload.")

        data = self._request_widget_action(
            r_type="PATCH",
            widget_instance_id=widget_instance_id,
            payload={"json": {"widget": widget}},
            timeout=timeout,
            expected_statuses=(200,),
        )
        if not isinstance(data, dict):
            raise ApiError(
                "Unexpected workspace widget patch response: "
                f"{type(data)!r}"
            )
        return WorkspaceWidgetMutationResult.model_validate(data)

    def delete_workspace_widget(
        self,
        widget_instance_id: str,
        *,
        recursive: bool = False,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> None:
        """Delete one mounted workspace widget by instance id.

        Returns `204 No Content` on success.

        If the target widget still contains `row.children`, pass
        `recursive=True`. Otherwise the backend may reject the delete with
        `409 Conflict`.
        """
        payload: dict[str, Any] = {}
        if recursive:
            payload["params"] = {"recursive": "true"}

        self._request_widget_action(
            r_type="DELETE",
            widget_instance_id=widget_instance_id,
            payload=payload,
            timeout=timeout,
            expected_statuses=(204,),
            empty_response=None,
        )

    def move_workspace_widget(
        self,
        widget_instance_id: str,
        *,
        parent_widget_id: str | None = None,
        index: int | None = None,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> WorkspaceWidgetMutationResult:
        """Move one mounted workspace widget inside the same workspace.

        Use this to reorder a top-level widget or move a widget into an existing
        row/container that supports `row.children`.

        - `parent_widget_id=None` moves the widget to the top level.
        - `index=None` appends to the target list.
        - Runtime state is unchanged.
        - The backend rejects moves into the widget itself or one of its
          descendants.
        """
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
            expected_statuses=(200,),
        )
        if not isinstance(data, dict):
            raise ApiError(
                "Unexpected workspace widget move response: "
                f"{type(data)!r}"
            )
        return WorkspaceWidgetMutationResult.model_validate(data)


class WorkspaceWidgetMutationResult(BasePydanticModel):
    """Result returned by widget-scoped workspace mutation endpoints.

    Shared success response used by:

    - `PATCH /api/v1/command_center/workspaces/{id}/widgets/{widget_instance_id}/`
    - `POST /api/v1/command_center/workspaces/{id}/widgets/{widget_instance_id}/move/`

    It contains the mutated widget instance, its current parent placement, and
    the workspace update timestamp after the mutation.
    """

    model_config = ConfigDict(
        populate_by_name=True,
        extra="ignore",
    )

    workspace_id: int = Field(
        alias="workspaceId",
        description="Workspace primary key.",
    )
    widget_instance_id: str = Field(
        alias="widgetInstanceId",
        description="Widget instance id after the mutation.",
    )
    parent_widget_id: str | None = Field(
        default=None,
        alias="parentWidgetId",
        description="Current parent widget id, or null for top-level widgets.",
    )
    widget: dict[str, Any] = Field(
        description="Widget JSON as stored in `Workspace.widgets` after the mutation.",
    )
    updated_at: datetime = Field(
        alias="updatedAt",
        description="Workspace `updated_at` after the mutation.",
    )


class RegisteredWidgetType(CommandCenterBaseObjectOrm, BasePydanticModel):
    """Registered widget catalog entry."""

    ENDPOINT: ClassVar[str] = "widget-types"
    FILTERSET_FIELDS: ClassVar[dict[str, list[str]]] = {
        "widget_id": ["exact"],
        "category": ["exact"],
        "kind": ["exact"],
        "source": ["exact"],
        "is_active": ["exact"],
        "include_inactive": ["exact"],
    }
    FILTER_VALUE_NORMALIZERS: ClassVar[dict[str, str]] = {
        "widget_id": "str",
        "category": "str",
        "kind": "str",
        "source": "str",
        "is_active": "bool",
        "include_inactive": "bool",
    }

    model_config = ConfigDict(
        populate_by_name=True,
        extra="allow",
    )

    widget_id: str = Field(
        alias="widgetId",
        description="Stable widget type identifier used by workspace widgets.",
    )
    title: str = Field(
        description="Human-readable widget title.",
    )
    description: str = Field(
        description="Widget description shown to users.",
    )
    category: str = Field(
        description="Widget catalog category.",
    )
    widget_version: str | None = Field(
        default=None,
        alias="widgetVersion",
        description="Registered widget definition version.",
    )
    kind: RegisteredWidgetKind = Field(
        description="Widget kind classification.",
    )
    source: str = Field(
        description="Origin of the widget definition.",
    )
    tags: list[str] | None = Field(
        default=None,
        description="Optional list of widget tags.",
    )
    required_permissions: list[str] | None = Field(
        default=None,
        alias="requiredPermissions",
        description="Optional permission codes required to use the widget.",
    )
    schema_payload: dict[str, Any] | None = Field(
        default=None,
        alias="schema",
        description="Optional widget schema payload.",
    )
    io: dict[str, Any] | None = Field(
        default=None,
        description="Optional widget IO contract payload.",
    )
    default_presentation: dict[str, Any] | None = Field(
        default=None,
        alias="defaultPresentation",
        description="Optional default presentation configuration.",
    )
    default_size: dict[str, Any] | None = Field(
        default=None,
        alias="defaultSize",
        description="Optional default widget size configuration.",
    )
    responsive: dict[str, Any] | list[Any] | None = Field(
        default=None,
        description="Optional responsive behavior configuration.",
    )
    usage_guidance: dict[str, Any] | list[Any] | str | None = Field(
        default=None,
        alias="usageGuidance",
        description="Optional guidance for using the widget type.",
    )
    capabilities: dict[str, Any] | list[Any] | None = Field(
        default=None,
        description="Optional widget capability metadata.",
    )
    examples: dict[str, Any] | list[Any] | None = Field(
        default=None,
        description="Optional example widget configurations or payloads.",
    )
    is_active: bool = Field(
        alias="isActive",
        description="Whether the widget type is currently active.",
    )
    registry_version: str = Field(
        alias="registryVersion",
        description="Registry version from the last successful sync.",
    )
    checksum: str = Field(
        description="Registry checksum from the last successful sync.",
    )
    last_synced_at: datetime | None = Field(
        default=None,
        alias="lastSyncedAt",
        description="Timestamp of the last catalog sync.",
    )
    created_at: datetime = Field(
        alias="createdAt",
        description="Timestamp when the widget type row was created.",
    )
    updated_at: datetime = Field(
        alias="updatedAt",
        description="Timestamp when the widget type row was last updated.",
    )

    @classmethod
    def get(cls, pk=None, timeout=None, **filters):
        widget_id = filters.pop("widget_id", None)
        if pk is None and widget_id is not None:
            pk = widget_id

        return super().get(pk=pk, timeout=timeout, **filters)


__all__ = [
    "CommandCenterBaseObjectOrm",
    "RegisteredWidgetKind",
    "RegisteredWidgetType",
    "Workspace",
    "WorkspaceWidgetMutationResult",
    "WorkspaceLayoutKind",
]
