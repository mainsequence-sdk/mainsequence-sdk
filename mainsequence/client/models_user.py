from __future__ import annotations

import datetime
import math
from collections.abc import Iterable
from typing import Any, ClassVar, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, StrictBool, field_validator, model_validator

from mainsequence._request_identity import (
    CALLER_ASSERTION_HEADER,
    RequestIdentityError,
    _caller_read_headers,
    _get_request_identity,
    _get_requester,
    _team_membership_scope,
)
from mainsequence._team_memberships import MAX_TEAMS_PER_CALL
from mainsequence.defaults import STANDARD_BACKEND_URL

from .base import (
    BaseObjectOrm,
    BasePydanticModel,
    DetailActionObjectMixin,
)
from .exceptions import ApiError, raise_for_response
from .object_access import AccessManagedObjectMixin, AccessPrincipal, _canonical_uid
from .utils import (
    make_request,
)
from .value_sets import OpenValueSet


class RequestUserIdentity(BaseModel):
    """Minimal identity of the caller of the current runtime request.

    The caller is a person or a workload identity; ``User.get_by_uid(uid)``
    reads its User. ``User.get_requester()`` returns the same type for the
    requester of a requester-bound request, the person the calling
    application works for, with that person's own ``is_organization_admin``.
    ``in_teams()`` asks the platform which teams either of them belongs to.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    uid: str = Field(
        ...,
        title="User UID",
        description=(
            "Canonical public UUID of the User making the current request, a person or a "
            "workload identity."
        ),
    )
    username: str | None = Field(
        None,
        title="Username",
        description="Optional display name authenticated by the runtime gateway.",
    )
    is_organization_admin: bool = Field(
        False,
        title="Organization admin",
        description=(
            "Whether the platform signed this identity as an admin of the application's "
            "Organization: the caller, or for the requester the person. False in local "
            "mode and on WebSockets, which carry no such facts."
        ),
    )

    @field_validator("uid", mode="before")
    @classmethod
    def _canonicalize_uid(cls, value: Any) -> str:
        try:
            return str(UUID(str(value).strip()))
        except (AttributeError, TypeError, ValueError) as exc:
            raise ValueError("Request user UID must be a valid UUID.") from exc

    def in_teams(
        self, team_uids: Iterable[str | UUID], *, max_age_seconds: float | None = None
    ) -> frozenset[str]:
        """Which of ``team_uids`` this person is an active member of.

        Works on ``User.get_logged_user()`` and ``User.get_requester()`` inside
        an authenticated hosted request, and asks the platform for the request's
        caller or requester only. Returns canonical team UIDs; an empty set
        means none. Teams the platform does not count, such as unknown or
        inactive ones, are left out.

        Reuse is the application's choice (``install_request_identity``):

        - within one request the same question is asked once;
        - across requests an answer is reused while it is younger than
          ``team_membership_cache_seconds``, or than ``max_age_seconds`` when
          given; ``0``, the default, reuses none across requests;
        - ``max_age_seconds=0`` is a fresh check: it always asks the platform,
          skips both kinds of reuse and stores the new answer. Ask for one
          before acting on a membership that must be current.

        Raises ``RequestIdentityError`` outside such a request, in local mode
        and on WebSockets, and the SDK's API error when the platform cannot
        answer; it never answers "not a member" because a call failed.
        """
        return _member_team_uids(self.uid, team_uids, max_age_seconds)

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


class OrganizationEnvironment(AccessManagedObjectMixin, UserApiBaseObjectOrm, BasePydanticModel):
    """Public Environment metadata, scoped by authenticated platform visibility.

    Working in an Environment is a policy. `member` works in it, opens the
    objects there that the person has access to, and creates Projects,
    Buckets, Constants, Secrets, model providers and repositories in it;
    `observer` works in it without creating anything. Both go to people and
    Teams, never to workload identities, and only Organization admins attach
    or detach them. The access methods (`get_access()`, `attach_policy()`, ...)
    read and change them; being admitted never shows objects the person has no
    access to.
    """

    ENDPOINT: ClassVar[str] = "organization-environments"

    uid: str
    name: str
    organization_owner_uid: str
    required_repository_branch: str
    is_production: bool


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
    organization_domain: str | None = Field(
        ...,
        title="Organization Domain",
        description=(
            "Primary email or identity domain associated with the organization. "
            "Null for an organization that has no domain set."
        ),
        examples=["main-sequence.io", None],
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
            "Organization Environment."
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
    """A person or workload identity in a team response.

    Personal fields are ``None`` for workload identities or when not supplied.
    Team responses carry ``member_kind`` to distinguish people from workloads.
    """

    uid: str | None = Field(
        None,
        title="User UID",
        description="Stable public unique identifier of the user.",
        examples=["8f5d6b54-2f5e-4a8b-bb10-0b17f3f4c123"],
    )
    first_name: str | None = Field(
        None,
        title="First Name",
        description="User's given name; None for workloads or when not supplied.",
        examples=["Jose"],
    )
    last_name: str | None = Field(
        None,
        title="Last Name",
        description="User's family name; None for workloads or when not supplied.",
        examples=["Ambrosino"],
    )
    username: str | None = Field(
        None,
        title="Username",
        description="Platform username; None for workloads or when not supplied.",
        examples=["jose@main-sequence.io"],
    )
    email: str | None = Field(
        None,
        title="Email",
        description="Primary email address; None for workloads or when not supplied.",
        examples=["jose@main-sequence.io"],
    )
    member_kind: OpenValueSet[Literal["person", "workload"]] | None = Field(
        None,
        title="Member Kind",
        description="Kind of identity in a team response; None when not supplied.",
        examples=["workload"],
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


class TeamHeldAccess(BasePydanticModel):
    """One policy a Team holds on an object: what joining the Team gives a member."""

    binding_uid: str = Field(
        ...,
        title="Binding UID",
        description="Public UID of the binding that attaches the policy to the Team.",
        examples=["7d3c6a51-0b8e-4c2f-9a14-5e6f7a8b9c0d"],
    )
    resource_type: str = Field(
        ...,
        title="Resource Type",
        description="Resource type key of the object, for example `secrets`.",
        examples=["secrets"],
    )
    object_uid: str = Field(
        ...,
        title="Object UID",
        description="Public UID of the object the policy is held on.",
        examples=["24001fc7-098c-40fa-b398-1d2352b7c224"],
    )
    object_name: str | None = Field(
        None,
        title="Object Name",
        description="The object's name, when it has one.",
        examples=["POLYGON_API_KEY"],
    )
    policy: str = Field(
        ...,
        title="Policy",
        description="Id of the policy the Team holds, `<resource type>.<policy>`.",
        examples=["secrets.reader"],
    )
    source: OpenValueSet[Literal["explicit", "creator", "workflow", "migrated"]] = Field(
        ...,
        title="Source",
        description=(
            "Why the binding exists: `explicit`, `creator`, `workflow` or `migrated`. "
            "The source grants nothing by itself."
        ),
        examples=["explicit"],
    )
    granted_by: AccessPrincipal | None = Field(
        None,
        title="Granted By",
        description="The User who attached the policy, when known.",
    )
    created_at: datetime.datetime | None = Field(
        None,
        title="Created At",
        description="When the binding was created.",
        examples=["2026-10-17T09:00:00Z"],
    )


class _CallerDirectoryReadMixin:
    """Inside ``reads_as_caller()``, ``filter`` and ``get`` reads answer as the
    request's caller (ADR-0036, amended for #198)."""

    @classmethod
    def _read_request_headers(cls, url: str) -> dict[str, str]:
        return _caller_read_headers(url, cls.get_object_url())


class Team(
    _CallerDirectoryReadMixin, AccessManagedObjectMixin, BasePydanticModel, UserApiBaseObjectOrm
):
    """A group of people and workload identities in one Organization.

    Membership is who is in the Team: who receives the policies the Team holds.
    Control is the policies on the Team: `viewer` sees it and its members,
    `membership_manager` adds and removes members, `editor` renames,
    describes, activates or deactivates it and sees its policies, and `owner`
    may do everything, including deleting it and deciding who manages it.
    Every member holds `member`: they see the Team and its members and can
    leave. Controlling a Team does not require being a member, and
    Organization admins hold every action on every Team of their Organization.

    Any person can create a Team; the creator becomes its owner and first
    member. The access methods (`get_access()`, `attach_policy()`,
    `detach_policy()` and the others) read and change the policies on the
    Team.
    """

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
    allowed_actions: list[str] | None = Field(
        None,
        title="Allowed Actions",
        description=(
            "Ids of the actions the caller may perform on this team, for example "
            "`organization-teams.update`. Team detail responses carry it, such as "
            "`Team.get_by_uid()` and `Team.create()`; it is None on listed teams."
        ),
        examples=[["organization-teams.read", "organization-teams.list_members"]],
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
        """Create a Team and return its detail, with `members` and `allowed_actions`.

        Any person can create a Team. The creator becomes its owner and its
        first member.
        """
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

    def _validate_team_user_list_payload(
        self, payload: Any, *, action_name: str
    ) -> list[UserSummary]:
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
        """Return the team's members, people first and then workload identities."""
        payload = self._request_detail_action(
            r_type="GET",
            action_name="members",
            payload={},
            timeout=timeout,
            expected_statuses=(200,),
        )
        return self._validate_team_user_list_payload(payload, action_name="members")

    def add_member(
        self,
        user_uid: Any,
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> UserSummary:
        """Add a person or a workload identity to the team, and return their member row.

        `user_uid` is the User's UID, or an object or dict that carries it as
        `uid`. Adding someone who already is a member changes nothing and
        returns their row. It needs the team's `add_member` action. An
        Organization admin can add any person or workload identity of the
        Organization; anyone else can add people who share an active team with
        them, themselves, and the workload identities they can view. Anyone
        else raises `BadRequestError` with `code` `invalid_user_uids`.
        """
        member_uid = _canonical_uid(user_uid, field_name="user_uid")
        payload = self._request_detail_action(
            r_type="POST",
            action_name="members",
            payload={"json": {"user_uid": member_uid}},
            timeout=timeout,
            expected_statuses=(200, 201),
        )
        if not isinstance(payload, dict):
            raise ApiError(f"Unexpected Team response for action 'members': {type(payload)!r}")
        return UserSummary.model_validate(payload)

    def remove_member(
        self,
        user_uid: Any,
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> None:
        """Remove a member from the team.

        `user_uid` is the member's UID, or an object or dict that carries it as
        `uid`. It needs the team's `remove_member` action, or `leave` to remove
        yourself. Someone who is not a member raises `NotFoundError`.
        """
        member_uid = _canonical_uid(user_uid, field_name="user_uid")
        self._request_detail_action(
            r_type="DELETE",
            action_name=f"members/{member_uid}",
            payload={},
            timeout=timeout,
            expected_statuses=(204,),
        )

    def leave(
        self,
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> None:
        """Leave the team: the caller stops being a member.

        Policies on the team are not membership: leaving keeps any policy the
        caller holds on the team itself, such as `owner`. A caller who is not a
        member raises `BadRequestError` with `code` `not_a_team_member`.
        """
        self._request_detail_action(
            r_type="POST",
            action_name="leave",
            payload={},
            timeout=timeout,
            expected_statuses=(204,),
        )

    def list_member_candidates(
        self,
        *,
        identity_type: Literal["human", "workload"] | None = None,
        search: str | None = None,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> list[UserSummary]:
        """Return who the caller may add to the team and is not a member yet, at most 200 Users.

        `identity_type` is `human` (the platform's default) for people or
        `workload` for workload identities. `search` narrows people by email or
        name. It needs the team's `add_member` action; the built-in
        full-directory team returns an empty list.
        """
        params: dict[str, str] = {}
        if identity_type is not None:
            if identity_type not in ("human", "workload"):
                raise ValueError(
                    f"identity_type must be 'human' or 'workload'. Got: {identity_type!r}"
                )
            params["identity_type"] = identity_type
        if search is not None and str(search).strip():
            params["search"] = str(search).strip()
        payload = self._request_detail_action(
            r_type="GET",
            action_name="member-candidates",
            payload={"params": params} if params else {},
            timeout=timeout,
            expected_statuses=(200,),
        )
        return self._validate_team_user_list_payload(payload, action_name="member-candidates")

    def list_held_access(
        self,
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> list[TeamHeldAccess]:
        """Return the policies the team holds on other objects: what joining it gives.

        Only objects the caller can read are listed.
        """
        payload = self._request_detail_action(
            r_type="GET",
            action_name="held-access",
            payload={},
            timeout=timeout,
            expected_statuses=(200,),
        )
        bindings = payload.get("bindings") if isinstance(payload, dict) else None
        if not isinstance(bindings, list):
            raise ApiError(f"Unexpected Team response for action 'held-access': {type(payload)!r}")
        return [TeamHeldAccess.model_validate(item) for item in bindings]


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
    source: OpenValueSet[Literal["system", "organization"]] | None = Field(
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
    type: OpenValueSet[Literal["UR", "IM", "IN"]] = Field(
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
                raise ApiError(f"Unexpected Notification list response: {type(payload)!r}")
            return [cls.model_validate(item) for item in payload]
        if not isinstance(payload, dict):
            raise ApiError(f"Unexpected Notification response payload: {type(payload)!r}")
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


class User(_CallerDirectoryReadMixin, UserApiBaseObjectOrm, BasePydanticModel):
    """A platform User: a person, or the workload identity a deployed Job,
    FastAPI release or Agent runs as.

    A workload identity is read with ``User.get_by_uid(uid)`` or listed with
    ``User.filter(identity_type="workload")``. Its row carries ``uid``,
    ``identity_type`` (``"workload"``), ``is_active`` and the UID of its
    workload in ``job_uid``, ``resource_release_uid`` or ``agent_uid``, without
    a username, email, join date or person's name. Both listed rows and UID
    lookups carry ``workload_name``, the name of that Job, release or Agent,
    when the caller can view the workload. It is ``None`` for a workload the
    caller cannot view, for a person, or when the response does not carry it.
    Other fields the row does not carry are ``None``, or empty for lists.

    The reverse direction is ``workload_user_uid`` on ``Job``,
    ``ResourceRelease`` and ``Agent``: the UID of the User that workload runs
    as, to pass to ``User.get_by_uid`` and as the ``principal_uid`` of
    ``attach_policy()``.

    Inside ``mainsequence.server.fastapi.reads_as_caller()``, ``User.filter``
    and ``User.get_by_uid`` answer with what the request's caller may see, and
    workload rows carry ``managed_by_caller``.
    """

    # Platform facts, not application authorization policy. None means the
    # connected platform has not supplied the versioned additive contract.
    is_organization_admin: StrictBool | None = None
    active_team_uids: list[UUID] | None = None

    ENDPOINT: ClassVar[str] = "users"
    FILTERSET_FIELDS: ClassVar[dict[str, list[str]] | None] = {
        "uid": ["exact", "in"],
        "email": ["exact", "contains", "in"],
        "identity_type": ["exact"],
        "search": ["exact"],
    }
    FILTER_VALUE_NORMALIZERS: ClassVar[dict[str, str]] = {
        "uid": "uid",
        "uid__in": "uid",
        "email": "str",
        "email__contains": "str",
        "email__in": "str",
        "identity_type": "str",
        "search": "str",
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
    identity_type: (
        OpenValueSet[Literal["human", "service_account", "deleted_user", "workload"]] | None
    ) = Field(
        None,
        title="Identity Type",
        description=(
            "Kind of identity: `workload` for the identity a deployed Job, FastAPI release "
            "or Agent runs as. None when the response does not carry it."
        ),
        examples=["workload"],
    )
    job_uid: str | None = Field(
        None,
        title="Job UID",
        description="UID of the Job a workload identity belongs to; None otherwise.",
        examples=["88888888-8888-4888-8888-888888888888"],
    )
    resource_release_uid: str | None = Field(
        None,
        title="Resource Release UID",
        description="UID of the release a workload identity belongs to; None otherwise.",
        examples=[None],
    )
    agent_uid: str | None = Field(
        None,
        title="Agent UID",
        description="UID of the Agent a workload identity belongs to; None otherwise.",
        examples=[None],
    )
    workload_name: str | None = Field(
        None,
        title="Workload Name",
        description=(
            "Name of the Job, release or Agent a workload identity belongs to, on a "
            "listed row or UID lookup when the caller can view the workload. None "
            "for a workload the caller cannot view, for a person, or when not supplied."
        ),
        examples=["nightly-prices"],
    )
    managed_by_caller: bool | None = Field(
        None,
        title="Managed By Caller",
        description=(
            "On a workload identity read inside `reads_as_caller()`: whether the "
            "request's caller manages that workload. None when the row does not carry it."
        ),
        examples=[True],
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

    date_joined: datetime.datetime | None = Field(
        None,
        title="Date Joined",
        description=(
            "Timestamp when the user account was created. None when the response does not "
            "carry it, as for a workload identity."
        ),
        examples=["2025-01-10T08:15:00Z"],
    )
    is_active: bool = Field(
        ...,
        title="Is Active",
        description="Whether the user account is active.",
        examples=[True],
    )
    username: str | None = Field(
        None,
        title="Username",
        description=(
            "Unique username used by the platform. None when the response does not carry it, "
            "as for a workload identity."
        ),
        examples=["jose@main-sequence.io"],
    )
    email: str | None = Field(
        None,
        title="Email",
        description=(
            "Primary email address of the user. None when the response does not carry it, "
            "as for a workload identity."
        ),
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
    api_request_limit: int | None = Field(
        None,
        title="API Request Limit",
        description=(
            "Maximum number of API requests available to the user in the current limit "
            "window. None when the response does not carry it, as for a workload identity."
        ),
        examples=[10000],
    )
    mfa_enabled: bool | None = Field(
        None,
        title="MFA Enabled",
        description=(
            "Whether multi-factor authentication is enabled for the user. None when the "
            "response does not carry it, as for a workload identity."
        ),
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
        endpoint and does not depend on request headers or
        the request identity context.
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
    def get_logged_user(cls) -> RequestUserIdentity:
        """Return the caller bound by the application's request identity integration."""
        user = _get_request_identity()
        if not isinstance(user, RequestUserIdentity):
            raise RequestIdentityError("Invalid request user context value.")
        return user

    @classmethod
    def get_requester(cls) -> RequestUserIdentity | None:
        """Return the person a requester-bound request works for, or ``None``.

        A requester-bound request comes from another application, for example
        an Agent, while it works for a person: the requester. The platform
        signs that person into the caller assertion, and the request identity
        integration verifies it before any handler or tool runs. The result is
        that checked delegation: the person's own ``uid`` and
        ``is_organization_admin``, as the platform signed them, and
        ``in_teams()`` for the person's teams.
        ``get_logged_user()`` still returns the caller, the acting application.

        Returns ``None`` when the request carries no delegation, including
        local mode and WebSockets. Raises ``RequestIdentityError`` outside an
        authenticated request, as ``get_logged_user()`` does. An invalid
        delegation rejects the whole request before any handler or tool runs;
        it never falls back to the caller.

        Each handler or tool decides whether it needs a person:

        - one that needs a person fails with its own error when this returns
          ``None``;
        - one where the person is optional authorizes against this result when
          present, and against ``get_logged_user()`` otherwise;
        - none parses assertions, headers or MCP ``_meta``, takes a person's
          UID from a request body, header or query parameter, or combines the
          caller's grants with the person's.

        Authorize against the person's ordinary permissions, including
        administrative ones, for the exact object and operation. This getter
        verifies identity, not permission; read access or team membership does
        not authorize a write. The platform checks access on every call and
        limits the work to at most 24 hours after the person's request. A
        model-driven Agent can be steered by prompt injection, so per-operation
        checks remain necessary. This getter does not enable platform
        endpoints or extend its request scope.
        """
        requester = _get_requester()
        if requester is not None and not isinstance(requester, RequestUserIdentity):
            raise RequestIdentityError("Invalid request requester context value.")
        return requester

    @classmethod
    def _team_memberships(cls, person_uid: str, team_uids: list[str], assertion: str) -> set[str]:
        """Which of ``team_uids`` the platform says the person belongs to (#225)."""
        response = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="GET",
            url=f"{cls.get_object_url()}/{person_uid}/team-memberships/",
            payload={
                "params": {"team_uid__in": ",".join(team_uids)},
                "headers": {CALLER_ASSERTION_HEADER: assertion},
            },
        )
        if response.status_code != 200:
            # Never pass the payload: it holds the caller assertion.
            raise_for_response(response)
        answered = response.json().get("team_uids")
        if not isinstance(answered, list):
            raise ApiError("Invalid team membership answer.", response=response)
        return set(_canonical_team_uids(answered)) & set(team_uids)


def _canonical_team_uids(team_uids: Iterable[str | UUID]) -> list[str]:
    if isinstance(team_uids, (str, bytes, UUID)):
        raise TypeError("in_teams() takes a collection of team UIDs, not a single UID.")
    try:
        return sorted({str(UUID(str(team_uid).strip())) for team_uid in team_uids})
    except (AttributeError, TypeError, ValueError) as exc:
        raise ValueError("Team UIDs must be valid UUIDs.") from exc


def _member_team_uids(
    person_uid: str, team_uids: Iterable[str | UUID], max_age_seconds: float | None
) -> frozenset[str]:
    """RequestUserIdentity.in_teams(): reuse answers as allowed, ask for the rest."""
    asked = _canonical_team_uids(team_uids)
    if max_age_seconds is not None and (
        isinstance(max_age_seconds, bool)
        or not isinstance(max_age_seconds, (int, float))
        or not math.isfinite(max_age_seconds)
        or max_age_seconds < 0
    ):
        raise ValueError("max_age_seconds must be None or a non-negative number of seconds.")
    scope = _team_membership_scope(person_uid)
    fresh = max_age_seconds == 0
    lifetime = scope.team_membership_cache_seconds if max_age_seconds is None else max_age_seconds
    cache = scope.team_membership_cache
    answers: dict[str, bool] = {}
    unanswered: list[str] = []
    for team_uid in asked:
        key = (scope.release_uid, scope.environment_uid, person_uid, team_uid)
        reused = None
        if not fresh:
            reused = scope.team_memberships.get(key)
            if reused is None and cache is not None:
                reused = cache.get(key, max_age=lifetime)
        if reused is None:
            unanswered.append(team_uid)
        else:
            answers[team_uid] = scope.team_memberships[key] = reused
    for start in range(0, len(unanswered), MAX_TEAMS_PER_CALL):
        batch = unanswered[start : start + MAX_TEAMS_PER_CALL]
        members = User._team_memberships(person_uid, batch, scope.caller_assertion)
        for team_uid in batch:
            key = (scope.release_uid, scope.environment_uid, person_uid, team_uid)
            answers[team_uid] = scope.team_memberships[key] = team_uid in members
            if cache is not None:
                cache.put(key, answers[team_uid])
    return frozenset(team_uid for team_uid, member in answers.items() if member)
