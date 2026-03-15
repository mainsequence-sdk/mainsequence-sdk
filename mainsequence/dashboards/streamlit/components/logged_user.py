from __future__ import annotations

import datetime as dt
from collections.abc import Mapping
from typing import Any

import streamlit as st

import mainsequence.client as msc

_LOGGED_USER_KEY = "_ms_logged_user"
_LOGGED_USER_ERROR_KEY = "_ms_logged_user_error"
_LOGGED_USER_LOADED_KEY = "_ms_logged_user_loaded"


def _format_datetime(value: dt.datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat(sep=" ", timespec="seconds")


def _format_plan(value: Any | None) -> str | None:
    if value in (None, "", [], {}):
        return None

    if isinstance(value, dict):
        preferred_keys = ("name", "type", "plan_type", "slug", "id")
        for key in preferred_keys:
            candidate = value.get(key)
            if candidate not in (None, "", [], {}):
                return str(candidate)

        return ", ".join(
            f"{key}={candidate}"
            for key, candidate in value.items()
            if candidate not in (None, "", [], {})
        ) or None

    return str(value)


def _status_summary(user: msc.User) -> str:
    parts = ["active" if user.is_active else "inactive"]

    if user.is_verified is True:
        parts.append("verified")
    elif user.is_verified is False:
        parts.append("unverified")

    parts.append("mfa on" if user.mfa_enabled else "mfa off")

    if user.blocked_access is True:
        parts.append("blocked")

    if user.requires_password_change is True:
        parts.append("password change required")

    return " | ".join(parts)


def _team_title(team: int | msc.OrganizationTeam | Mapping[str, Any]) -> str:
    if isinstance(team, int):
        return f"Team #{team}"

    if isinstance(team, Mapping):
        name = team.get("name")
        if name:
            return str(name)

        team_id = team.get("id")
        if team_id is not None:
            return f"Team #{team_id}"

        return "Unknown team"

    if team.name:
        return team.name

    if team.id is not None:
        return f"Team #{team.id}"

    return "Unknown team"


def _team_detail(team: int | msc.OrganizationTeam | Mapping[str, Any]) -> str | None:
    if isinstance(team, int):
        return None

    parts: list[str] = []

    if isinstance(team, Mapping):
        organization = team.get("organization")
        if isinstance(organization, Mapping):
            organization_name = organization.get("name")
        else:
            organization_name = getattr(organization, "name", None)
        description = str(team.get("description") or "").strip()
        members = team.get("members") or []
        is_active = team.get("is_active")
    else:
        organization_name = getattr(getattr(team, "organization", None), "name", None)
        description = (team.description or "").strip()
        members = team.members
        is_active = team.is_active

    if organization_name:
        parts.append(str(organization_name))

    if description:
        parts.append(description)

    if members:
        parts.append(f"{len(members)} member(s)")

    if is_active is False:
        parts.append("inactive")

    return " | ".join(parts) or None


def _group_names(user: msc.User) -> list[str]:
    return [group.name for group in user.groups if getattr(group, "name", None)]


def _team_entries(user: msc.User) -> list[int | msc.OrganizationTeam | Mapping[str, Any]]:
    return list(user.organization_teams or [])


def _detail_rows(user: msc.User) -> list[tuple[str, str]]:
    rows: list[tuple[str, str]] = [("Username", user.username)]

    if user.email and user.email != user.username:
        rows.append(("Email", user.email))

    organization_name = getattr(user.organization, "name", None)
    if organization_name:
        rows.append(("Organization", str(organization_name)))

    organization_domain = getattr(user.organization, "organization_domain", None)
    if organization_domain:
        rows.append(("Organization domain", str(organization_domain)))

    groups = ", ".join(_group_names(user))
    if groups:
        rows.append(("Groups", groups))

    plan = _format_plan(user.effective_plan)
    if plan:
        rows.append(("Plan", plan))

    if user.phone_number:
        rows.append(("Phone", user.phone_number))

    if user.api_request_limit is not None:
        rows.append(("API request limit", str(user.api_request_limit)))

    if user.user_permissions:
        rows.append(("Direct permissions", str(len(user.user_permissions))))

    joined = _format_datetime(user.date_joined)
    if joined:
        rows.append(("Joined", joined))

    last_login = _format_datetime(user.last_login)
    if last_login:
        rows.append(("Last login", last_login))

    return rows


def _get_logged_user(
    *,
    refresh: bool = False,
    session_key: str = _LOGGED_USER_KEY,
) -> tuple[msc.User | None, str | None]:
    loaded_key = f"{session_key}:{_LOGGED_USER_LOADED_KEY}"
    error_key = f"{session_key}:{_LOGGED_USER_ERROR_KEY}"

    if refresh:
        st.session_state.pop(session_key, None)
        st.session_state.pop(error_key, None)
        st.session_state.pop(loaded_key, None)

    if not st.session_state.get(loaded_key, False):
        try:
            st.session_state[session_key] = msc.User.get_logged_user()
            st.session_state.pop(error_key, None)
        except Exception as exc:
            st.session_state[session_key] = None
            st.session_state[error_key] = str(exc)
        finally:
            st.session_state[loaded_key] = True

    return st.session_state.get(session_key), st.session_state.get(error_key)


def render_logged_user_username(
    *,
    label: str = "Logged user",
    show_organization: bool = False,
    show_errors: bool = False,
    refresh: bool = False,
    session_key: str = _LOGGED_USER_KEY,
) -> msc.User | None:
    user, error = _get_logged_user(refresh=refresh, session_key=session_key)

    if user is None:
        if show_errors and error:
            st.caption(f"{label}: unavailable")
            st.caption(error)
        return None

    st.caption(label)

    text = user.username
    organization_name = getattr(user.organization, "name", None)
    if show_organization and organization_name:
        text = f"{text} | {organization_name}"

    st.markdown(f"**{text}**")
    return user


def render_logged_user_details(
    *,
    title: str = "Logged user details",
    expanded: bool = False,
    show_errors: bool = False,
    refresh: bool = False,
    session_key: str = _LOGGED_USER_KEY,
) -> msc.User | None:
    user, error = _get_logged_user(refresh=refresh, session_key=session_key)

    if user is None:
        if show_errors and error:
            st.caption(f"{title}: unavailable")
            st.caption(error)
        return None

    with st.expander(title, expanded=expanded):
        st.caption(_status_summary(user))

        for label, value in _detail_rows(user):
            st.markdown(f"**{label}**")
            st.caption(value)

        teams = _team_entries(user)
        st.markdown("**Teams**")
        if not teams:
            st.caption("No team memberships returned for this user.")
        else:
            for team in teams:
                st.caption(_team_title(team))
                detail = _team_detail(team)
                if detail:
                    st.caption(detail)

    return user


def sidebar_logged_user_username(**kwargs: Any) -> msc.User | None:
    with st.sidebar:
        return render_logged_user_username(**kwargs)


def sidebar_logged_user_details(**kwargs: Any) -> msc.User | None:
    with st.sidebar:
        return render_logged_user_details(**kwargs)


__all__ = [
    "render_logged_user_username",
    "render_logged_user_details",
    "sidebar_logged_user_username",
    "sidebar_logged_user_details",
]
