
from __future__ import annotations

import datetime
from collections.abc import Mapping
from contextvars import ContextVar
from typing import Any

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
    id: int
    uid: str
    name: str
    url: str | None = None
    organization_domain: str
    identity_platform_tenant_id: str | None = None
    has_pending_invoices: bool = False


class Group(BasePydanticModel):
    id: int
    name: str
    permissions: list[int] = Field(default_factory=list)


class UserSummary(BasePydanticModel):
    id: int
    first_name: str
    last_name: str
    username: str
    email: str
    phone_number: str | None = None


class OrganizationTeam(BasePydanticModel):
    id: int
    organization: Organization
    name: str
    description: str = ""
    created_by: UserSummary | None = None
    members: list[UserSummary] = Field(default_factory=list)
    is_active: bool
    created_at: datetime.datetime
    updated_at: datetime.datetime


class User(BaseObjectOrm, BasePydanticModel):
    # present on UserSerializer
    id: int | None = None
    is_verified: bool | None = None
    blocked_access: bool | None = None
    requires_password_change: bool | None = None
    identity_platform_uid: str | None = None
    active_plan_type: str | None = None

    # present on both

    date_joined: datetime.datetime
    is_active: bool
    username: str
    email: str
    last_login: datetime.datetime | None = None
    api_request_limit: int
    mfa_enabled: bool
    organization: Organization | None = None
    phone_number: str | None = None

    # present on FullUserSerializer
    plan: Any | None = None
    groups: list[Group] = Field(default_factory=list)
    user_permissions: list[int] = Field(default_factory=list)
    organization_teams: list[int] = Field(default_factory=list)

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