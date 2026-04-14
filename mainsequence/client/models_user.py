
from __future__ import annotations

import datetime
from collections.abc import Mapping
from contextvars import ContextVar
from typing import Any, ClassVar, Literal

from pydantic import Field

from mainsequence.logconf import logger

from .base import (
    BaseObjectOrm,
    BasePydanticModel,
    DetailActionObjectMixin,
    PermissionManagedObjectMixin,
)
from .exceptions import ApiError, raise_for_response
from .utils import (
    make_request,
)

_CURRENT_AUTH_HEADERS: ContextVar[Mapping[str, Any] | None] = ContextVar(
    "_CURRENT_AUTH_HEADERS",
    default=None,
)

_CURRENT_USER: ContextVar[Any | None] = ContextVar(
    "_CURRENT_USER",
    default=None,
)


class UserApiBaseObjectOrm(BaseObjectOrm):
    USER_API_PREFIX: ClassVar[str] = "user/api"
    ENDPOINT: ClassVar[str]

    @classmethod
    def _user_api_root(cls) -> str:
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
            f"{cls._user_api_root().rstrip('/')}/"
            f"{cls.USER_API_PREFIX.strip('/')}/"
            f"{endpoint.strip('/')}"
        )


def _logged_user_header_context(
    headers: Mapping[str, Any] | None,
    *,
    header_source: str,
) -> dict[str, Any]:
    if not headers:
        return {
            "header_source": header_source,
            "header_keys": [],
            "x_user_id": None,
            "authorization_present": False,
            "authorization_scheme": None,
        }

    normalized_headers: dict[str, Any] = {}
    for key, value in headers.items():
        key_str = str(key)
        normalized_headers[key_str] = value
        normalized_headers[key_str.lower()] = value

    authorization = normalized_headers.get("authorization")
    authorization_scheme = None
    if authorization:
        authorization_scheme = str(authorization).split(" ", 1)[0]

    return {
        "header_source": header_source,
        "header_keys": sorted(str(key) for key in headers.keys()),
        "x_user_id": (
            normalized_headers.get("X-User-ID")
            or normalized_headers.get("x-user-id")
            or normalized_headers.get("HTTP_X_USER_ID")
            or normalized_headers.get("http_x_user_id")
        ),
        "authorization_present": bool(authorization),
        "authorization_scheme": authorization_scheme,
    }

class Organization(BasePydanticModel):
    id: int = Field(
        ...,
        title="Organization ID",
        description="Unique identifier of the organization.",
        examples=[12],
    )
    uid: str = Field(
        ...,
        title="Organization UID",
        description="Stable external unique identifier for the organization.",
        examples=["org_01hxyzabcd1234"],
    )
    name: str = Field(
        ...,
        title="Organization Name",
        description="Human-readable organization name.",
        examples=["Main Sequence"],
    )
    url: str | None = Field(
        None,
        title="Organization URL",
        description="Optional public or internal URL for the organization.",
        examples=["https://api.main-sequence.app"],
    )
    organization_domain: str = Field(
        ...,
        title="Organization Domain",
        description="Primary email or identity domain associated with the organization.",
        examples=["main-sequence.io"],
    )
    identity_platform_tenant_id: str | None = Field(
        None,
        title="Identity Platform Tenant ID",
        description="Optional tenant identifier in the external identity platform.",
        examples=["8f5d6b54-2f5e-4a8b-bb10-0b17f3f4c123"],
    )
    has_pending_invoices: bool = Field(
        False,
        title="Has Pending Invoices",
        description="Whether the organization currently has unpaid invoices.",
        examples=[False],
    )


class Group(BasePydanticModel):
    id: int = Field(
        ...,
        title="Group ID",
        description="Unique identifier of the permission group.",
        examples=[3],
    )
    name: str = Field(
        ...,
        title="Group Name",
        description="Human-readable name of the permission group.",
        examples=["Portfolio Managers"],
    )

    normalized_name:str = Field()



class UserSummary(BasePydanticModel):
    id: int = Field(
        ...,
        title="User ID",
        description="Unique identifier of the user.",
        examples=[42],
    )
    first_name: str = Field(
        ...,
        title="First Name",
        description="User's given name.",
        examples=["Jose"],
    )
    last_name: str = Field(
        ...,
        title="Last Name",
        description="User's family name.",
        examples=["Ambrosino"],
    )
    username: str = Field(
        ...,
        title="Username",
        description="Unique username used to identify the user in the platform.",
        examples=["jose@main-sequence.io"],
    )
    email: str = Field(
        ...,
        title="Email",
        description="Primary email address of the user.",
        examples=["jose@main-sequence.io"],
    )
    phone_number: str | None = Field(
        None,
        title="Phone Number",
        description="Optional phone number associated with the user.",
        examples=["+43123456789"],
    )


class ShareableTeamSummary(BasePydanticModel):
    id: int = Field(
        ...,
        title="Team ID",
        description="Unique identifier of the team in shareable-access responses.",
        examples=[9],
    )
    name: str = Field(
        ...,
        title="Team Name",
        description="Human-readable name of the team.",
        examples=["Research"],
    )
    description: str = Field(
        "",
        title="Team Description",
        description="Optional textual description of the team.",
        examples=["Team responsible for model research and validation."],
    )
    member_count: int = Field(
        0,
        title="Member Count",
        description="Number of members currently in the team.",
        examples=[5],
    )


class TeamMembershipUpdateResult(BasePydanticModel):
    team_id: int = Field(
        ...,
        title="Team ID",
        description="Unique identifier of the team whose membership was updated.",
        examples=[11],
    )
    member_count: int = Field(
        ...,
        title="Member Count",
        description="Total number of users in the team after the membership operation completes.",
        examples=[4],
    )
    selected: int = Field(
        ...,
        title="Selected Users",
        description="Number of user ids submitted in the membership update request.",
        examples=[2],
    )
    added: int = Field(
        0,
        title="Added Users",
        description="Number of users added to the team by the operation.",
        examples=[2],
    )
    removed: int = Field(
        0,
        title="Removed Users",
        description="Number of users removed from the team by the operation.",
        examples=[0],
    )
    skipped: int = Field(
        0,
        title="Skipped Users",
        description="Number of submitted users skipped because the requested membership state already existed.",
        examples=[0],
    )


class Team(PermissionManagedObjectMixin, BasePydanticModel, UserApiBaseObjectOrm):
    ENDPOINT: ClassVar[str] = "team"
    FILTERSET_FIELDS: ClassVar[dict[str, list[str]] | None] = {
        "search": ["exact"],
        "is_active": ["exact"],
    }
    FILTER_VALUE_NORMALIZERS: ClassVar[dict[str, str]] = {
        "search": "str",
        "is_active": "bool",
    }

    id: int | None = Field(
        None,
        title="Organization Team ID",
        description="Unique identifier of the organization team.",
        examples=[9],
    )
    organization: Organization | int | None = Field(
        None,
        title="Organization",
        description="Organization that owns this team.",
    )
    name: str = Field(
        ...,
        title="Team Name",
        description="Human-readable name of the team.",
        examples=["Research"],
    )
    description: str = Field(
        "",
        title="Team Description",
        description="Optional textual description of the team.",
        examples=["Team responsible for model research and validation."],
    )
    created_by: UserSummary | int | None = Field(
        None,
        title="Created By",
        description="User who created the team, when available.",
    )
    members: list[UserSummary | int | dict[str, Any]] = Field(
        default_factory=list,
        title="Members",
        description="Users who belong to the team.",
    )
    member_count: int = Field(
        0,
        title="Member Count",
        description="Number of members currently in the team.",
        examples=[5],
    )
    is_active: bool | None = Field(
        None,
        title="Is Active",
        description="Whether the team is active.",
        examples=[True],
    )
    created_at: datetime.datetime | None = Field(
        None,
        title="Created At",
        description="Timestamp when the team was created.",
        examples=["2026-03-15T09:00:00Z"],
    )
    updated_at: datetime.datetime | None = Field(
        None,
        title="Updated At",
        description="Timestamp when the team was last updated.",
        examples=["2026-03-15T10:30:00Z"],
    )

    @classmethod
    def create(
        cls,
        *,
        name: str,
        description: str = "",
        is_active: bool = True,
        timeout: int | None = None,
    ) -> Team:
        return super().create(
            name=name,
            description=description,
            is_active=is_active,
            timeout=timeout,
        )

    def _validate_team_user_list_payload(self, payload: Any, *, action_name: str) -> list[UserSummary]:
        if isinstance(payload, dict):
            payload = payload.get("results", payload.get("users", payload.get("members")))
        if not isinstance(payload, list):
            raise ApiError(
                f"Unexpected Team response for action {action_name!r}: {type(payload)!r}"
            )
        return [UserSummary.model_validate(item) for item in payload]

    def list_members(
        self,
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> list[UserSummary]:
        payload = self._request_detail_action(
            r_type="GET",
            action_name="members",
            payload={},
            timeout=timeout,
            expected_statuses=(200,),
        )
        return self._validate_team_user_list_payload(payload, action_name="members")

    def list_candidate_members(
        self,
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> list[UserSummary]:
        payload = self._request_detail_action(
            r_type="GET",
            action_name="candidate-members",
            payload={},
            timeout=timeout,
            expected_statuses=(200,),
        )
        return self._validate_team_user_list_payload(payload, action_name="candidate-members")

    def manage_members(
        self,
        *,
        action: Literal["add", "remove"],
        user_ids: list[Any],
        timeout: int | float | tuple[float, float] | None = None,
    ) -> TeamMembershipUpdateResult:
        normalized_user_ids = [
            type(self)._coerce_filter_id(user_id, field_name="user_ids")
            for user_id in list(user_ids or [])
        ]
        if not normalized_user_ids:
            raise ValueError("user_ids must contain at least one user id.")

        payload = self._request_detail_action(
            r_type="POST",
            action_name="manage-members",
            payload={"json": {"action": action, "user_ids": normalized_user_ids}},
            timeout=timeout,
            expected_statuses=(200,),
        )
        if not isinstance(payload, dict):
            raise ApiError(
                f"Unexpected Team response for action 'manage-members': {type(payload)!r}"
            )
        return TeamMembershipUpdateResult.model_validate(payload)

    def add_members(
        self,
        user_ids: list[Any],
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> TeamMembershipUpdateResult:
        return self.manage_members(action="add", user_ids=user_ids, timeout=timeout)

    def remove_members(
        self,
        user_ids: list[Any],
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> TeamMembershipUpdateResult:
        return self.manage_members(action="remove", user_ids=user_ids, timeout=timeout)


class OrganizationTeam(Team):
    """
    Backward-compatible alias for the richer organization team model.
    """

    pass


class NotificationBulkActionResult(BasePydanticModel):
    updated_count: int = Field(
        ...,
        title="Updated Count",
        description="Number of notifications updated by the bulk action.",
        examples=[4],
    )


class Notification(DetailActionObjectMixin, BasePydanticModel, UserApiBaseObjectOrm):
    ENDPOINT: ClassVar[str] = "notifications"

    id: int | None = Field(
        None,
        title="Notification ID",
        description="Unique identifier of the notification.",
        examples=[101],
    )
    source: Literal["system", "organization"] | None = Field(
        None,
        title="Notification Source",
        description="Source scope of the notification.",
        examples=["organization"],
    )
    created_by_user: int | dict[str, Any] | UserSummary | None = Field(
        None,
        title="Created By User",
        description="User that created the notification when available.",
    )
    source_organization: int | dict[str, Any] | Organization | None = Field(
        None,
        title="Source Organization",
        description="Organization that owns the notification when the source is organization-scoped.",
    )
    type: Literal["UR", "IM", "IN"] = Field(
        "IN",
        title="Notification Type",
        description="Stored notification priority code.",
        examples=["IN"],
    )
    created_at: datetime.datetime | None = Field(
        None,
        title="Created At",
        description="Timestamp when the notification was created.",
        examples=["2026-04-14T09:00:00Z"],
    )
    title: str = Field(
        ...,
        title="Title",
        description="Short notification title.",
        examples=["Deployment complete"],
    )
    description: str = Field(
        ...,
        title="Description",
        description="Rich-text notification body.",
        examples=["The deployment finished successfully."],
    )
    meta_data: dict[str, Any] | None = Field(
        None,
        title="Metadata",
        description="Optional structured metadata attached to the notification.",
    )
    is_global: bool = Field(
        False,
        title="Is Global",
        description="Whether the notification is broadcast within its source scope.",
        examples=[False],
    )
    target_user: int | dict[str, Any] | UserSummary | None = Field(
        None,
        title="Target User",
        description="Direct target user when the notification is user-targeted.",
    )
    target_team: int | dict[str, Any] | ShareableTeamSummary | Team | None = Field(
        None,
        title="Target Team",
        description="Target organization team when the notification is team-targeted.",
    )
    include_email: bool = Field(
        False,
        title="Include Email",
        description="Whether the backend also sends email for the notification.",
        examples=[False],
    )
    is_read: bool | None = Field(
        None,
        title="Is Read",
        description="Read flag exposed by visible-notification responses.",
        examples=[False],
    )

    @classmethod
    def _normalize_notification_payload(
        cls,
        *,
        type: str,
        title: str,
        description: str,
        meta_data: dict[str, Any] | None = None,
        include_email: bool = False,
        target_user: Any = None,
        target_team: Any = None,
        user_ids: list[Any] | None = None,
        team_ids: list[Any] | None = None,
        is_global: bool | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "type": type,
            "title": title,
            "description": description,
            "meta_data": meta_data,
            "include_email": include_email,
        }

        if target_user is not None:
            payload["target_user"] = cls._coerce_filter_id(target_user, field_name="target_user")
        if target_team is not None:
            payload["target_team"] = cls._coerce_filter_id(target_team, field_name="target_team")
        if user_ids is not None:
            payload["user_ids"] = [
                cls._coerce_filter_id(user_id, field_name="user_ids")
                for user_id in list(user_ids)
            ]
        if team_ids is not None:
            payload["team_ids"] = [
                cls._coerce_filter_id(team_id, field_name="team_ids")
                for team_id in list(team_ids)
            ]
        if is_global is not None:
            payload["is_global"] = bool(is_global)
        return payload

    @classmethod
    def _validate_notification_response(
        cls,
        payload: Any,
        *,
        allow_many: bool = False,
    ) -> Notification | list[Notification]:
        if isinstance(payload, list):
            if not allow_many:
                raise ApiError(
                    f"Unexpected Notification list response: {type(payload)!r}"
                )
            return [cls.model_validate(item) for item in payload]
        if not isinstance(payload, dict):
            raise ApiError(
                f"Unexpected Notification response payload: {type(payload)!r}"
            )
        return cls.model_validate(payload)

    @classmethod
    def send(
        cls,
        *,
        type: str,
        title: str,
        description: str,
        meta_data: dict[str, Any] | None = None,
        include_email: bool = False,
        target_user: Any = None,
        target_team: Any = None,
        user_ids: list[Any] | None = None,
        team_ids: list[Any] | None = None,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> Notification | list[Notification]:
        """
        Create one or more organization-scoped notifications via `POST /user/api/notifications/`.
        """
        base_url = cls.get_object_url().rstrip("/")
        payload = {
            "json": cls._normalize_notification_payload(
                type=type,
                title=title,
                description=description,
                meta_data=meta_data,
                include_email=include_email,
                target_user=target_user,
                target_team=target_team,
                user_ids=user_ids,
                team_ids=team_ids,
            )
        }
        response = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="POST",
            url=f"{base_url}/",
            payload=payload,
            time_out=timeout,
        )
        if response.status_code not in (200, 201):
            raise_for_response(response, payload=payload)
        return cls._validate_notification_response(response.json(), allow_many=True)

    @classmethod
    def send_to_self(
        cls,
        *,
        type: str,
        title: str,
        description: str,
        meta_data: dict[str, Any] | None = None,
        include_email: bool = False,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> Notification:
        """
        Create an organization-scoped self notification via `POST /user/api/notifications/send-self/`.
        """
        base_url = cls.get_object_url().rstrip("/")
        payload = {
            "json": cls._normalize_notification_payload(
                type=type,
                title=title,
                description=description,
                meta_data=meta_data,
                include_email=include_email,
            )
        }
        response = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="POST",
            url=f"{base_url}/send-self/",
            payload=payload,
            time_out=timeout,
        )
        if response.status_code not in (200, 201):
            raise_for_response(response, payload=payload)
        return cls._validate_notification_response(response.json())

    @classmethod
    def send_system(
        cls,
        *,
        type: str,
        title: str,
        description: str,
        meta_data: dict[str, Any] | None = None,
        include_email: bool = False,
        is_global: bool = False,
        target_user: Any = None,
        target_team: Any = None,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> Notification:
        """
        Create a system-scoped notification via `POST /user/api/notifications/send-system/`.
        """
        base_url = cls.get_object_url().rstrip("/")
        payload = {
            "json": cls._normalize_notification_payload(
                type=type,
                title=title,
                description=description,
                meta_data=meta_data,
                include_email=include_email,
                is_global=is_global,
                target_user=target_user,
                target_team=target_team,
            )
        }
        response = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="POST",
            url=f"{base_url}/send-system/",
            payload=payload,
            time_out=timeout,
        )
        if response.status_code not in (200, 201):
            raise_for_response(response, payload=payload)
        return cls._validate_notification_response(response.json())

    def mark_read(
        self,
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> Notification:
        """
        Mark this notification as read via `POST /user/api/notifications/<id>/mark-read/`.
        """
        payload = self._request_detail_action(
            r_type="POST",
            action_name="mark-read",
            payload={},
            timeout=timeout,
            expected_statuses=(200,),
        )
        return type(self)._validate_notification_response(payload)

    def dismiss(
        self,
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> None:
        """
        Dismiss this notification via `POST /user/api/notifications/<id>/dismiss/`.
        """
        self._request_detail_action(
            r_type="POST",
            action_name="dismiss",
            payload={},
            timeout=timeout,
            expected_statuses=(204,),
            empty_response=None,
        )

    @classmethod
    def mark_all_read(
        cls,
        *,
        notification_type: str | None = None,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> NotificationBulkActionResult:
        """
        Mark all visible notifications as read via `POST /user/api/notifications/mark-all-read/`.
        """
        base_url = cls.get_object_url().rstrip("/")
        payload: dict[str, Any] = {}
        if notification_type:
            payload["params"] = {"type": str(notification_type)}
        response = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="POST",
            url=f"{base_url}/mark-all-read/",
            payload=payload,
            time_out=timeout,
        )
        if response.status_code != 200:
            raise_for_response(response, payload=payload or None)
        return NotificationBulkActionResult.model_validate(response.json())

    @classmethod
    def dismiss_all(
        cls,
        *,
        notification_type: str | None = None,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> NotificationBulkActionResult:
        """
        Dismiss all visible notifications via `POST /user/api/notifications/dismiss-all/`.
        """
        base_url = cls.get_object_url().rstrip("/")
        payload: dict[str, Any] = {}
        if notification_type:
            payload["params"] = {"type": str(notification_type)}
        response = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="POST",
            url=f"{base_url}/dismiss-all/",
            payload=payload,
            time_out=timeout,
        )
        if response.status_code != 200:
            raise_for_response(response, payload=payload or None)
        return NotificationBulkActionResult.model_validate(response.json())


class ShareableAccessState(BasePydanticModel):
    object_id: int = Field(
        ...,
        title="Object ID",
        description="Primary key of the shareable object whose access is being described.",
        examples=[123],
    )
    object_type: str = Field(
        ...,
        title="Object Type",
        description="Backend object type identifier for the shareable object.",
        examples=["tdag.constant"],
    )
    access_level: Literal["view", "edit"] = Field(
        ...,
        title="Access Level",
        description="Permission level represented by this access-state payload.",
        examples=["view"],
    )
    users: list[UserSummary] = Field(
        default_factory=list,
        title="Users",
        description="Users with this explicit access level on the object.",
    )
    teams: list[ShareableTeamSummary] = Field(
        default_factory=list,
        title="Teams",
        description="Teams with this access level on the object.",
        examples=[[{"id": 9, "name": "Research", "description": "Research team", "member_count": 5}]],
    )


class User(UserApiBaseObjectOrm, BasePydanticModel):
    ENDPOINT: ClassVar[str] = "user"
    # present on UserSerializer
    id: int | None = Field(
        None,
        title="User ID",
        description="Unique identifier of the user.",
        examples=[42],
    )
    is_verified: bool | None = Field(
        None,
        title="Is Verified",
        description="Whether the user's identity or email has been verified.",
        examples=[True],
    )
    blocked_access: bool | None = Field(
        None,
        title="Blocked Access",
        description="Whether the user is currently blocked from accessing the platform.",
        examples=[False],
    )
    requires_password_change: bool | None = Field(
        None,
        title="Requires Password Change",
        description="Whether the user must change their password before continuing.",
        examples=[False],
    )
    identity_platform_uid: str | None = Field(
        None,
        title="Identity Platform UID",
        description="Unique identifier of the user in the external identity platform.",
        examples=["usr_01hxyzabcd1234"],
    )
    active_plan_type: str | None = Field(
        None,
        title="Active Plan Type",
        description="Name of the active subscription or access plan.",
        examples=["enterprise"],
    )


    date_joined: datetime.datetime = Field(
        ...,
        title="Date Joined",
        description="Timestamp when the user account was created.",
        examples=["2025-01-10T08:15:00Z"],
    )
    is_active: bool = Field(
        ...,
        title="Is Active",
        description="Whether the user account is active.",
        examples=[True],
    )
    username: str = Field(
        ...,
        title="Username",
        description="Unique username used by the platform.",
        examples=["jose@main-sequence.io"],
    )
    email: str = Field(
        ...,
        title="Email",
        description="Primary email address of the user.",
        examples=["jose@main-sequence.io"],
    )
    last_login: datetime.datetime | None = Field(
        None,
        title="Last Login",
        description="Timestamp of the last successful login.",
        examples=["2026-03-15T11:20:00Z"],
    )
    api_request_limit: int = Field(
        ...,
        title="API Request Limit",
        description="Maximum number of API requests available to the user in the current limit window.",
        examples=[10000],
    )
    mfa_enabled: bool = Field(
        ...,
        title="MFA Enabled",
        description="Whether multi-factor authentication is enabled for the user.",
        examples=[True],
    )
    organization: Organization | None = Field(
        None,
        title="Organization",
        description="Organization the user belongs to, when available.",
    )
    phone_number: str | None = Field(
        None,
        title="Phone Number",
        description="Optional phone number of the user.",
        examples=["+43123456789"],
    )

    plan: Any | None = Field(
        None,
        title="Plan",
        description="Optional detailed plan payload returned by the backend.",
        examples=[{"type": "enterprise", "seats": 25}],
    )
    groups: list[Group] = Field(
        default_factory=list,
        title="Groups",
        description="Permission groups assigned to the user.",
    )
    user_permissions: list[int] = Field(
        default_factory=list,
        title="User Permissions",
        description="Direct permission ids assigned to the user.",
        examples=[[101, 202]],
    )
    organization_teams: list[int | Team | dict[str, Any]] = Field(
        default_factory=list,
        title="Organization Teams",
        description="Organization team identifiers or fully-expanded team objects for the user.",
        examples=[[9, 12]],
    )

    @property
    def effective_plan(self) -> Any | None:
        return self.plan if self.plan is not None else self.active_plan_type

    @classmethod
    def get_authenticated_user_details(cls):
        """
        Resolve the authenticated user from the active SDK auth session.

        Use this in standalone authenticated CLI or script code that is not
        running inside a request-bound identity context. This method reads the
        authenticated user through the backend `/user/api/user/get_user_details/`
        endpoint and does not depend on request headers, Streamlit context, or
        `_CURRENT_AUTH_HEADERS`.
        """
        url = f"{cls.get_object_url()}/get_user_details/"
        r = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="GET",
            url=url,
        )
        if r.status_code not in (200, 201):
            raise_for_response(r)

        data = r.json()

        if hasattr(cls, "model_validate"):
            return cls.model_validate(data)

        return cls.parse_obj(data)

    @classmethod
    def get_logged_user(cls) -> User:
        """
        Resolve the current user from request-bound identity context.

        Use this when code is running with request-scoped identity context, such
        as FastAPI middleware, Streamlit, or code that explicitly binds
        `_CURRENT_AUTH_HEADERS`. This method first uses the bound request/user
        context and only falls back to `get_authenticated_user_details()` when
        request headers are present with Bearer auth but no `X-User-ID`.

        For standalone authenticated CLI or script code that is not request-bound,
        prefer `get_authenticated_user_details()`.
        """
        cached_user = _CURRENT_USER.get()
        if cached_user is not None:
            return cached_user

        headers = _CURRENT_AUTH_HEADERS.get()
        header_source = "_CURRENT_AUTH_HEADERS"

        if not headers:
            try:
                import streamlit as st

                headers = st.context.headers
                header_source = "streamlit"
            except Exception:
                headers = None

        if not headers:
            logger.error(
                "User.get_logged_user failed: no auth headers are available; "
                "header_source=%s header_keys=%s",
                header_source,
                [],
            )
            raise RuntimeError(
                "No auth headers are available. "
                "In Streamlit, this requires st.context.headers to be available. "
                "In Agents, you must bind request headers into _CURRENT_AUTH_HEADERS at request entry."
            )

        normalized_headers: dict[str, Any] = {}
        for key, value in headers.items():
            key_str = str(key)
            normalized_headers[key_str] = value
            normalized_headers[key_str.lower()] = value

        user_id_raw = (
            normalized_headers.get("X-User-ID")
            or normalized_headers.get("x-user-id")
            or normalized_headers.get("HTTP_X_USER_ID")
            or normalized_headers.get("http_x_user_id")
        )

        if user_id_raw in (None, ""):
            if normalized_headers.get("authorization") and "Bearer" in normalized_headers.get("authorization"):
                outgoing_authorization = None
                outgoing_authorization_scheme = None
                try:
                    outgoing_headers = cls.LOADERS.auth_headers
                    outgoing_authorization = (
                        outgoing_headers.get("Authorization")
                        or outgoing_headers.get("authorization")
                    )
                    if outgoing_authorization:
                        outgoing_authorization_scheme = str(outgoing_authorization).split(" ", 1)[0]
                except Exception as auth_exc:
                    logger.exception(
                        "User.get_logged_user could not inspect outgoing auth headers "
                        "for /user/api/user/get_user_details/: %s",
                        auth_exc,
                    )
                logger.info(
                    "User.get_logged_user bearer fallback to /user/api/user/get_user_details/ "
                    "outgoing_authorization_present=%s outgoing_authorization_scheme=%r",
                    bool(outgoing_authorization),
                    outgoing_authorization_scheme,
                )
                try:
                    user = cls.get_authenticated_user_details()
                except Exception:
                    context = _logged_user_header_context(headers, header_source=header_source)
                    logger.exception(
                        "User.get_logged_user failed during bearer fallback; "
                        "header_source=%s header_keys=%s X-User-ID=%r "
                        "authorization_present=%s authorization_scheme=%r",
                        context["header_source"],
                        context["header_keys"],
                        context["x_user_id"],
                        context["authorization_present"],
                        context["authorization_scheme"],
                    )
                    raise
                logger.info(
                    "User.get_logged_user resolved user_id=%s via bearer fallback",
                    user.id,
                )
                return user

            context = _logged_user_header_context(headers, header_source=header_source)
            logger.error(
                "User.get_logged_user failed: missing X-User-ID in request headers; "
                "header_source=%s header_keys=%s X-User-ID=%r "
                "authorization_present=%s authorization_scheme=%r",
                context["header_source"],
                context["header_keys"],
                context["x_user_id"],
                context["authorization_present"],
                context["authorization_scheme"],
            )
            raise RuntimeError("Missing X-User-ID in request headers.")

        try:
            user_id = int(str(user_id_raw).strip())
        except (TypeError, ValueError) as exc:
            context = _logged_user_header_context(headers, header_source=header_source)
            logger.exception(
                "User.get_logged_user failed: invalid X-User-ID value; "
                "header_source=%s header_keys=%s X-User-ID=%r "
                "authorization_present=%s authorization_scheme=%r",
                context["header_source"],
                context["header_keys"],
                context["x_user_id"],
                context["authorization_present"],
                context["authorization_scheme"],
            )
            raise RuntimeError(f"Invalid X-User-ID value: {user_id_raw!r}") from exc

        user = cls.get(pk=user_id, serializer="full")
        logger.info(
            "User.get_logged_user resolved user_id=%s via X-User-ID header",
            user.id,
        )
        return user
