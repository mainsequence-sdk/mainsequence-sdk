
from __future__ import annotations

import datetime
from collections.abc import Mapping
from contextvars import ContextVar
from typing import Any, ClassVar, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from mainsequence.defaults import STANDARD_BACKEND_URL

from .base import (
    BaseObjectOrm,
    BasePydanticModel,
    DetailActionObjectMixin,
    PermissionManagedObjectMixin,
)
from .exceptions import ApiError, raise_for_response
from .utils import (
    DEFAULT_TIMEOUT,
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


class RequestIdentityError(RuntimeError):
    """Raised when a request does not carry a valid public user identity."""


class RequestUserIdentity(BaseModel):
    """Minimal identity of the human making the current runtime request."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    uid: str = Field(
        ...,
        title="User UID",
        description="Canonical public UUID of the human making the current request.",
    )
    username: str | None = Field(
        None,
        title="Username",
        description="Optional display name authenticated by the runtime gateway.",
    )

    @field_validator("uid", mode="before")
    @classmethod
    def _canonicalize_uid(cls, value: Any) -> str:
        try:
            return str(UUID(str(value).strip()))
        except (AttributeError, TypeError, ValueError) as exc:
            raise ValueError("Request user UID must be a valid UUID.") from exc

    @field_validator("username", mode="before")
    @classmethod
    def _normalize_username(cls, value: Any) -> str | None:
        if value in (None, ""):
            return None
        normalized = str(value).strip()
        return normalized or None


class UserApiBaseObjectOrm(BaseObjectOrm):
    ENDPOINT: ClassVar[str]

    @classmethod
    def _user_api_root(cls) -> str:
        root = str(getattr(cls, "ROOT_URL", BaseObjectOrm.ROOT_URL)).rstrip("/")
        return root

    @classmethod
    def get_object_url(cls, custom_endpoint_name=None):
        endpoint = custom_endpoint_name or getattr(cls, "ENDPOINT", None)
        if not endpoint:
            raise ValueError(f"{cls.__name__} must define ENDPOINT.")

        return f"{cls._user_api_root().rstrip('/')}/{endpoint.strip('/')}"


def _normalize_request_headers(headers: Mapping[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for key, value in headers.items():
        normalized[str(key).lower()] = value
    return normalized


class Organization(UserApiBaseObjectOrm, BasePydanticModel):
    ENDPOINT: ClassVar[str] = "organizations"

    id: int | None = Field(
        None,
        exclude=True,
        title="Organization ID",
        description="Internal backend row id accepted only for legacy payloads; not a public lookup key.",
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
        examples=[STANDARD_BACKEND_URL],
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
    production_environment_uid: str | None = Field(
        None,
        title="Production Environment UID",
        description=(
            "Stable public UID of the organization's backend-managed production "
            "project environment."
        ),
        examples=["00000000-0000-4000-8000-000000000002"],
    )


class Group(BasePydanticModel):
    model_config = ConfigDict(extra="ignore")

    name: str = Field(
        ...,
        title="Group Name",
        description="Human-readable name of the permission group.",
        examples=["Data Managers"],
    )

    normalized_name: str = Field(
        ...,
        title="Normalized Group Name",
        description="Stable normalized permission group name returned by the user details API.",
        examples=["data_managers"],
    )


class UserSummary(BasePydanticModel):
    uid: str | None = Field(
        None,
        title="User UID",
        description="Stable public unique identifier of the user.",
        examples=["8f5d6b54-2f5e-4a8b-bb10-0b17f3f4c123"],
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

    @model_validator(mode="before")
    @classmethod
    def _drop_internal_id(cls, value: Any) -> Any:
        if isinstance(value, dict):
            value = {key: item for key, item in value.items() if key != "id"}
        return value


class ShareableTeamSummary(BasePydanticModel):
    uid: str | None = Field(
        None,
        title="Team UID",
        description="Stable public unique identifier of the team in shareable-access responses.",
        examples=["3f1cc452-43ec-49cb-b2ba-87dbac164d29"],
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

    @model_validator(mode="before")
    @classmethod
    def _drop_internal_id(cls, value: Any) -> Any:
        if isinstance(value, dict):
            value = {key: item for key, item in value.items() if key != "id"}
        return value


class UserTeamSummary(BasePydanticModel):
    model_config = ConfigDict(extra="ignore")

    uid: str = Field(
        ...,
        title="Organization Team UID",
        description="Stable public unique identifier of the organization team.",
        examples=["3f1cc452-43ec-49cb-b2ba-87dbac164d29"],
    )
    name: str | None = Field(
        None,
        title="Team Name",
        description="Human-readable organization team name when returned by the user details API.",
        examples=["Research"],
    )


class TeamMembershipUpdateResult(BasePydanticModel):
    team_id: int | None = Field(
        None,
        exclude=True,
        title="Team ID",
        description="Internal backend row id for the team whose membership was updated.",
        examples=[11],
    )
    team_uid: str | None = Field(
        None,
        title="Team UID",
        description="Stable public unique identifier of the team whose membership was updated.",
        examples=["3f1cc452-43ec-49cb-b2ba-87dbac164d29"],
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
        description="Number of user UIDs submitted in the membership update request.",
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
    ENDPOINT: ClassVar[str] = "teams"
    FILTERSET_FIELDS: ClassVar[dict[str, list[str]] | None] = {
        "uid": ["exact", "in"],
        "organization_uid": ["exact"],
        "search": ["exact"],
        "is_active": ["exact"],
    }
    FILTER_VALUE_NORMALIZERS: ClassVar[dict[str, str]] = {
        "uid": "uid",
        "uid__in": "uid",
        "organization_uid": "uid",
        "search": "str",
        "is_active": "bool",
    }

    id: int | None = Field(
        None,
        exclude=True,
        title="Organization Team ID",
        description="Internal backend row id for the organization team.",
        examples=[9],
    )
    uid: str | None = Field(
        None,
        title="Organization Team UID",
        description="Stable public unique identifier of the organization team.",
        examples=["3f1cc452-43ec-49cb-b2ba-87dbac164d29"],
    )
    organization: Organization | str | None = Field(
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
    created_by: UserSummary | str | None = Field(
        None,
        title="Created By",
        description="User who created the team, when available.",
    )
    members: list[UserSummary | str] = Field(
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
        organization_uid: Any | None = None,
        timeout: int | None = None,
    ) -> Team:
        payload: dict[str, Any] = {
            "name": name,
            "description": description,
            "is_active": is_active,
        }
        if organization_uid is not None:
            payload["organization_uid"] = cls._coerce_filter_uid(
                organization_uid,
                field_name="organization_uid",
            )
        return super().create(
            **payload,
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
        user_uids: list[Any],
        timeout: int | float | tuple[float, float] | None = None,
    ) -> TeamMembershipUpdateResult:
        normalized_user_uids = [
            type(self)._coerce_filter_uid(user_uid, field_name="user_uids")
            for user_uid in list(user_uids or [])
        ]
        if not normalized_user_uids:
            raise ValueError("user_uids must contain at least one user uid.")

        payload = self._request_detail_action(
            r_type="POST",
            action_name="manage-members",
            payload={"json": {"action": action, "user_uids": normalized_user_uids}},
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
        user_uids: list[Any],
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> TeamMembershipUpdateResult:
        return self.manage_members(action="add", user_uids=user_uids, timeout=timeout)

    def remove_members(
        self,
        user_uids: list[Any],
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> TeamMembershipUpdateResult:
        return self.manage_members(action="remove", user_uids=user_uids, timeout=timeout)


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

    uid: str = Field(
        ...,
        title="Notification UID",
        description="Stable public unique identifier of the notification.",
    )
    source: Literal["system", "organization"] | None = Field(
        None,
        title="Notification Source",
        description="Source scope of the notification.",
        examples=["organization"],
    )
    created_by_user_uid: str | None = Field(
        None,
        title="Created By User UID",
        description="Public UID of the user that created the notification.",
    )
    source_organization_uid: str | None = Field(
        None,
        title="Source Organization UID",
        description="Public UID of the organization that owns the notification.",
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
    target_user_uid: str | None = Field(
        None,
        title="Target User UID",
        description="Public UID of the direct target user.",
    )
    target_team_uid: str | None = Field(
        None,
        title="Target Team UID",
        description="Public UID of the target organization team.",
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

    @model_validator(mode="before")
    @classmethod
    def _drop_internal_relationship_ids(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        public_payload = dict(value)
        for internal_field in (
            "id",
            "created_by_user",
            "source_organization",
            "target_user",
            "target_team",
        ):
            public_payload.pop(internal_field, None)
        return public_payload

    @classmethod
    def _coerce_recipient_uid(cls, value: Any, *, field_name: str) -> str:
        normalized = cls._coerce_filter_uid(value, field_name=field_name)
        try:
            return str(UUID(normalized))
        except ValueError as exc:
            raise ValueError(f"{field_name} must be a valid UUID.") from exc

    @classmethod
    def _normalize_notification_payload(
        cls,
        *,
        type: str,
        title: str,
        description: str,
        meta_data: dict[str, Any] | None = None,
        include_email: bool = False,
        target_user_uid: Any = None,
        target_team_uid: Any = None,
        user_uids: list[Any] | None = None,
        team_uids: list[Any] | None = None,
        is_global: bool | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "type": type,
            "title": title,
            "description": description,
            "meta_data": meta_data,
            "include_email": include_email,
        }

        if target_user_uid is not None:
            payload["target_user_uid"] = cls._coerce_recipient_uid(
                target_user_uid,
                field_name="target_user_uid",
            )
        if target_team_uid is not None:
            payload["target_team_uid"] = cls._coerce_recipient_uid(
                target_team_uid,
                field_name="target_team_uid",
            )
        if user_uids is not None:
            payload["user_uids"] = [
                cls._coerce_recipient_uid(user_uid, field_name="user_uids")
                for user_uid in list(user_uids)
            ]
        if team_uids is not None:
            payload["team_uids"] = [
                cls._coerce_recipient_uid(team_uid, field_name="team_uids")
                for team_uid in list(team_uids)
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
        target_user_uid: Any = None,
        target_team_uid: Any = None,
        user_uids: list[Any] | None = None,
        team_uids: list[Any] | None = None,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> Notification | list[Notification]:
        """
        Create one or more organization-scoped notifications via `POST /api/v1/notifications/`.
        """
        base_url = cls.get_object_url().rstrip("/")
        payload = {
            "json": cls._normalize_notification_payload(
                type=type,
                title=title,
                description=description,
                meta_data=meta_data,
                include_email=include_email,
                target_user_uid=target_user_uid,
                target_team_uid=target_team_uid,
                user_uids=user_uids,
                team_uids=team_uids,
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
        Create an organization-scoped self notification via `POST /api/v1/notifications/send-self/`.
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
        target_user_uid: Any = None,
        target_team_uid: Any = None,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> Notification:
        """
        Create a system-scoped notification via `POST /api/v1/notifications/send-system/`.
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
                target_user_uid=target_user_uid,
                target_team_uid=target_team_uid,
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
        Mark this notification as read via `POST /api/v1/notifications/<uid>/mark-read/`.
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
        Dismiss this notification via `POST /api/v1/notifications/<uid>/dismiss/`.
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
        Mark all visible notifications as read via `POST /api/v1/notifications/mark-all-read/`.
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
        Dismiss all visible notifications via `POST /api/v1/notifications/dismiss-all/`.
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
    object_uid: str | None = Field(
        ...,
        title="Object UID",
        description="Public uid of the shareable object whose access is being described.",
        examples=["24001fc7-098c-40fa-b398-1d2352b7c224"],
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
        examples=[[
            {
                "uid": "3f1cc452-43ec-49cb-b2ba-87dbac164d29",
                "name": "Research",
                "description": "Research team",
                "member_count": 5,
            }
        ]],
    )


class User(UserApiBaseObjectOrm, BasePydanticModel):
    ENDPOINT: ClassVar[str] = "users"
    FILTERSET_FIELDS: ClassVar[dict[str, list[str]] | None] = {
        "uid": ["exact", "in"],
        "email": ["exact", "contains", "in"],
    }
    FILTER_VALUE_NORMALIZERS: ClassVar[dict[str, str]] = {
        "uid": "uid",
        "uid__in": "uid",
        "email": "str",
        "email__contains": "str",
        "email__in": "str",
    }

    id: int | None = Field(
        None,
        exclude=True,
        title="User ID",
        description="Internal backend row id accepted only for legacy/auth payloads; not a public lookup key.",
        examples=[42],
    )
    uid: str | None = Field(
        None,
        title="User UID",
        description="Stable public unique identifier of the user.",
        examples=["8f5d6b54-2f5e-4a8b-bb10-0b17f3f4c123"],
    )
    profile_picture: str | None = Field(
        None,
        title="Profile Picture URL",
        description="Absolute or relative URL of the user's profile picture when one is set.",
        examples=["https://api.example.com/media/user_profile_pictures/avatar.png"],
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
    first_name: str | None = Field(
        None,
        title="First Name",
        description="User's given name when returned by the backend user serializer.",
        examples=["Jose"],
    )
    last_name: str | None = Field(
        None,
        title="Last Name",
        description="User's family name when returned by the backend user serializer.",
        examples=["Ambrosino"],
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
        exclude=True,
        title="User Permissions",
        description="Legacy internal permission ids accepted only for older auth payloads.",
        examples=[[101, 202]],
    )
    organization_teams: list[UserTeamSummary | Team | str] = Field(
        default_factory=list,
        title="Organization Teams",
        description="Organization team UIDs or nested team summaries for the user.",
        examples=[[{"uid": "3f1cc452-43ec-49cb-b2ba-87dbac164d29", "name": "Research"}]],
    )

    @property
    def effective_plan(self) -> Any | None:
        return self.plan if self.plan is not None else self.active_plan_type

    @field_validator("user_permissions", mode="before")
    @classmethod
    def _normalize_legacy_user_permissions(cls, value):
        if value is None:
            return []
        return value

    @field_validator("organization_teams", mode="before")
    @classmethod
    def _normalize_organization_team_references(cls, value):
        if value is None:
            return []
        if not isinstance(value, list):
            return value

        normalized = []
        for item in value:
            if isinstance(item, int):
                continue
            if isinstance(item, dict):
                item = {key: val for key, val in item.items() if key != "id"}
            normalized.append(item)
        return normalized

    @classmethod
    def get_authenticated_user_details(cls):
        """
        Resolve the authenticated user from the active SDK auth session.

        Use this in standalone authenticated CLI or script code that is not
        running inside a request-bound identity context. This method reads the
        authenticated user through the backend `/api/v1/users/me/`
        endpoint and does not depend on request headers, Streamlit context, or
        `_CURRENT_AUTH_HEADERS`.
        """
        url = f"{cls.get_object_url()}/me/"
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
    def _get_request_bound_user(
        cls,
        *,
        authorization: str,
    ) -> User:
        url = f"{cls.get_object_url()}/me/"
        response = cls.build_session().get(
            url,
            headers={"Authorization": authorization},
            params=None,
            timeout=DEFAULT_TIMEOUT,
        )
        raise_for_response(response)

        data = response.json()
        if hasattr(cls, "model_validate"):
            return cls.model_validate(data)

        return cls.parse_obj(data)

    @classmethod
    def get_logged_user(cls) -> RequestUserIdentity:
        """
        Resolve the human making the current request as a UID-only identity.

        A deployed gateway provides trusted `X-User-UID` and optional
        `X-Username` headers. Direct local development provides a Bearer token,
        which is validated through `/api/v1/users/me/`. When both are present,
        their UIDs must match. The removed `X-User-ID` contract is rejected.

        Use `get_authenticated_user_details()` for a full account profile in a
        standalone authenticated CLI or script.
        """
        cached_user = _CURRENT_USER.get()
        if cached_user is not None:
            if not isinstance(cached_user, RequestUserIdentity):
                raise RequestIdentityError("Invalid request user context value.")
            return cached_user

        headers = _CURRENT_AUTH_HEADERS.get()

        if not headers:
            try:
                import streamlit as st

                headers = st.context.headers
            except Exception:
                headers = None

        if not headers:
            raise RequestIdentityError(
                "No request identity is available. Provide an explicitly bound "
                "request-header context or Streamlit request headers. FastAPI "
                "handlers should use request.state.user instead."
            )

        normalized_headers = _normalize_request_headers(headers)
        removed_user_id = normalized_headers.get("x-user-id") or normalized_headers.get(
            "http_x_user_id"
        )
        if removed_user_id not in (None, ""):
            raise RequestIdentityError("X-User-ID is not a supported request identity header.")

        authorization_raw = normalized_headers.get("authorization")
        authorization: str | None = None
        if authorization_raw not in (None, ""):
            authorization = str(authorization_raw).strip()
            scheme, separator, credential = authorization.partition(" ")
            if scheme.lower() != "bearer" or not separator or not credential.strip():
                raise RequestIdentityError("Request authorization must use a Bearer token.")

        header_identity: RequestUserIdentity | None = None
        user_uid_raw = normalized_headers.get("x-user-uid") or normalized_headers.get(
            "http_x_user_uid"
        )
        if user_uid_raw not in (None, ""):
            try:
                header_identity = RequestUserIdentity(
                    uid=user_uid_raw,
                    username=normalized_headers.get("x-username")
                    or normalized_headers.get("http_x_username"),
                )
            except ValueError as exc:
                raise RequestIdentityError("X-User-UID must contain a valid UUID.") from exc

        if authorization is None:
            if header_identity is None:
                raise RequestIdentityError("Missing X-User-UID or Bearer authorization.")
            return header_identity

        user = cls._get_request_bound_user(authorization=authorization)
        if user.uid in (None, ""):
            raise RequestIdentityError("Authenticated user response is missing uid.")
        try:
            bearer_identity = RequestUserIdentity(uid=user.uid, username=user.username)
        except ValueError as exc:
            raise RequestIdentityError(
                "Authenticated user response contains an invalid uid."
            ) from exc

        if header_identity is not None and header_identity.uid != bearer_identity.uid:
            raise RequestIdentityError(
                "Bearer user does not match the trusted request user UID."
            )
        return bearer_identity
