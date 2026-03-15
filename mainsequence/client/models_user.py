
from __future__ import annotations

import datetime
from collections.abc import Mapping
from contextvars import ContextVar
from typing import Any

from pydantic import Field

from .base import BaseObjectOrm, BasePydanticModel
from .data_filters import *
from .exceptions import raise_for_response
from .utils import (
    make_request,
)

_CURRENT_AUTH_HEADERS: ContextVar[Mapping[str, Any] | None] = ContextVar(
    "_CURRENT_AUTH_HEADERS",
    default=None,
)

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
        examples=["https://main-sequence.app"],
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
    permissions: list[int] = Field(
        default_factory=list,
        title="Permissions",
        description="List of permission ids assigned to the group.",
        examples=[[101, 102, 205]],
    )


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


class OrganizationTeam(BasePydanticModel):
    id: int = Field(
        ...,
        title="Organization Team ID",
        description="Unique identifier of the organization team.",
        examples=[9],
    )
    organization: Organization = Field(
        ...,
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
    created_by: UserSummary | None = Field(
        None,
        title="Created By",
        description="User who created the team, when available.",
    )
    members: list[UserSummary] = Field(
        default_factory=list,
        title="Members",
        description="Users who belong to the team.",
    )
    is_active: bool = Field(
        ...,
        title="Is Active",
        description="Whether the team is active.",
        examples=[True],
    )
    created_at: datetime.datetime = Field(
        ...,
        title="Created At",
        description="Timestamp when the team was created.",
        examples=["2026-03-15T09:00:00Z"],
    )
    updated_at: datetime.datetime = Field(
        ...,
        title="Updated At",
        description="Timestamp when the team was last updated.",
        examples=["2026-03-15T10:30:00Z"],
    )


class User(BaseObjectOrm, BasePydanticModel):
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
    organization_teams: list[int | OrganizationTeam | dict[str, Any]] = Field(
        default_factory=list,
        title="Organization Teams",
        description="Organization team identifiers or fully-expanded team objects for the user.",
        examples=[[9, 12]],
    )

    @property
    def effective_plan(self) -> Any | None:
        return self.plan if self.plan is not None else self.active_plan_type

    @classmethod
    def get_object_url(cls):
        return f"{cls.ROOT_URL.replace('orm/api', 'user/api')}/{cls.END_POINTS[cls.class_name()]}"

    @classmethod
    def get_authenticated_user_details(cls):
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
        headers = _CURRENT_AUTH_HEADERS.get()

        if not headers:
            try:
                import streamlit as st
                headers = st.context.headers
            except Exception:
                headers = None

        if not headers:
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
            raise RuntimeError("Missing X-User-ID in request headers.")

        try:
            user_id = int(str(user_id_raw).strip())
        except (TypeError, ValueError) as exc:
            raise RuntimeError(f"Invalid X-User-ID value: {user_id_raw!r}") from exc

        return cls.get(pk=user_id, serializer="full")
