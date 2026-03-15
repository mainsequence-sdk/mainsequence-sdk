from __future__ import annotations

import datetime as dt
import importlib.util
import pathlib
import sys
import types

import pytest


def _load_logged_user_module():
    repo_root = pathlib.Path(__file__).resolve().parents[1]
    module_path = repo_root / "mainsequence/dashboards/streamlit/components/logged_user.py"

    for name in ("mainsequence", "mainsequence.client", "streamlit", "logged_user_under_test"):
        sys.modules.pop(name, None)

    class _NoopContext:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    streamlit = types.ModuleType("streamlit")
    streamlit.session_state = {}
    streamlit._captions = []
    streamlit._markdown = []
    streamlit.caption = lambda value: streamlit._captions.append(value)
    streamlit.markdown = lambda value: streamlit._markdown.append(value)
    streamlit.expander = lambda *args, **kwargs: _NoopContext()
    streamlit.sidebar = _NoopContext()
    sys.modules["streamlit"] = streamlit

    class _Organization:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    class _OrganizationTeam:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    class _Group:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    class _User:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

        @property
        def effective_plan(self):
            return self.plan if self.plan is not None else self.active_plan_type

    fake_client = types.ModuleType("mainsequence.client")
    fake_client.User = _User
    fake_client.Group = _Group
    fake_client.Organization = _Organization
    fake_client.OrganizationTeam = _OrganizationTeam

    fake_mainsequence = types.ModuleType("mainsequence")
    fake_mainsequence.client = fake_client
    sys.modules["mainsequence"] = fake_mainsequence
    sys.modules["mainsequence.client"] = fake_client

    spec = importlib.util.spec_from_file_location("logged_user_under_test", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module, fake_client, streamlit


def test_logged_user_detail_rows_include_membership_metadata():
    module, fake_client, _ = _load_logged_user_module()

    user = fake_client.User(
        username="jose",
        email="jose@main-sequence.io",
        organization=fake_client.Organization(
            name="Main Sequence",
            organization_domain="main-sequence.io",
        ),
        groups=[fake_client.Group(name="Research"), fake_client.Group(name="Trading")],
        plan={"type": "enterprise"},
        active_plan_type=None,
        phone_number="+43123456789",
        api_request_limit=10000,
        user_permissions=[1, 2, 3],
        date_joined=dt.datetime(2026, 1, 1, 10, 0, 0),
        last_login=dt.datetime(2026, 3, 15, 9, 30, 0),
        organization_teams=[
            fake_client.OrganizationTeam(
                id=7,
                name="Research",
                description="Model validation",
                organization=fake_client.Organization(name="Main Sequence"),
                members=[object(), object()],
                is_active=True,
            )
        ],
        is_active=True,
        is_verified=True,
        mfa_enabled=True,
        blocked_access=False,
        requires_password_change=False,
    )

    rows = dict(module._detail_rows(user))

    assert rows["Username"] == "jose"
    assert rows["Organization"] == "Main Sequence"
    assert rows["Organization domain"] == "main-sequence.io"
    assert rows["Groups"] == "Research, Trading"
    assert rows["Plan"] == "enterprise"
    assert rows["Direct permissions"] == "3"

    team = user.organization_teams[0]
    assert module._team_title(team) == "Research"
    assert module._team_detail(team) == "Main Sequence | Model validation | 2 member(s)"


def test_logged_user_status_summary_and_numeric_team_fallback():
    module, fake_client, _ = _load_logged_user_module()

    user = fake_client.User(
        is_active=False,
        is_verified=False,
        mfa_enabled=False,
        blocked_access=True,
        requires_password_change=True,
    )

    assert module._status_summary(user) == (
        "inactive | unverified | mfa off | blocked | password change required"
    )
    assert module._team_title(12) == "Team #12"
    assert module._team_detail(12) is None
    assert module._team_title({"id": 4, "name": "Trading"}) == "Trading"
    assert module._team_detail(
        {
            "id": 4,
            "name": "Trading",
            "description": "Execution desk",
            "organization": {"name": "Main Sequence"},
            "members": [1],
            "is_active": False,
        }
    ) == "Main Sequence | Execution desk | 1 member(s) | inactive"


def test_render_logged_user_username_calls_client_user_get_logged_user():
    module, fake_client, streamlit = _load_logged_user_module()

    user = fake_client.User(
        username="jose",
        email="jose@main-sequence.io",
        organization=fake_client.Organization(name="Main Sequence"),
        groups=[],
        plan=None,
        active_plan_type=None,
        phone_number=None,
        api_request_limit=10000,
        user_permissions=[],
        date_joined=dt.datetime(2026, 1, 1, 10, 0, 0),
        last_login=None,
        organization_teams=[],
        is_active=True,
        is_verified=True,
        mfa_enabled=True,
        blocked_access=False,
        requires_password_change=False,
    )
    calls = {"count": 0}

    def _get_logged_user(cls):
        calls["count"] += 1
        return user

    fake_client.User.get_logged_user = classmethod(_get_logged_user)

    result = module.render_logged_user_username(show_organization=True)

    assert result is user
    assert calls["count"] == 1
    assert streamlit._captions == [
        "Logged user",
        "Logged user rendered from tokens.",
    ]
    assert streamlit._markdown == ["**jose | Main Sequence**"]


def test_render_logged_user_username_falls_back_to_authenticated_details():
    module, fake_client, streamlit = _load_logged_user_module()

    user = fake_client.User(
        username="jose",
        email="jose@main-sequence.io",
        organization=fake_client.Organization(name="Main Sequence"),
        groups=[],
        plan=None,
        active_plan_type=None,
        phone_number=None,
        api_request_limit=10000,
        user_permissions=[],
        date_joined=dt.datetime(2026, 1, 1, 10, 0, 0),
        last_login=None,
        organization_teams=[],
        is_active=True,
        is_verified=True,
        mfa_enabled=True,
        blocked_access=False,
        requires_password_change=False,
    )
    calls = {"logged": 0, "authenticated": 0}

    def _get_logged_user(cls):
        calls["logged"] += 1
        return None

    def _get_authenticated_user_details(cls):
        calls["authenticated"] += 1
        return user

    fake_client.User.get_logged_user = classmethod(_get_logged_user)
    fake_client.User.get_authenticated_user_details = classmethod(
        _get_authenticated_user_details
    )

    result = module.render_logged_user_username(show_organization=True)

    assert result is user
    assert calls == {"logged": 1, "authenticated": 1}
    assert streamlit._captions == [
        "Logged user",
        "Logged user rendered from tokens.",
    ]
    assert streamlit._markdown == ["**jose | Main Sequence**"]


def test_render_logged_user_details_falls_back_to_authenticated_details():
    module, fake_client, streamlit = _load_logged_user_module()

    user = fake_client.User(
        username="jose",
        email="jose@main-sequence.io",
        organization=fake_client.Organization(name="Main Sequence"),
        groups=[],
        plan=None,
        active_plan_type=None,
        phone_number=None,
        api_request_limit=10000,
        user_permissions=[],
        date_joined=dt.datetime(2026, 1, 1, 10, 0, 0),
        last_login=None,
        organization_teams=[],
        is_active=True,
        is_verified=True,
        mfa_enabled=True,
        blocked_access=False,
        requires_password_change=False,
    )
    calls = {"logged": 0, "authenticated": 0}

    def _get_logged_user(cls):
        calls["logged"] += 1
        return None

    def _get_authenticated_user_details(cls):
        calls["authenticated"] += 1
        return user

    fake_client.User.get_logged_user = classmethod(_get_logged_user)
    fake_client.User.get_authenticated_user_details = classmethod(
        _get_authenticated_user_details
    )

    result = module.render_logged_user_details()

    assert result is user
    assert calls == {"logged": 1, "authenticated": 1}
    assert streamlit._captions[0] == "Logged user rendered from tokens."


def test_render_logged_user_username_propagates_lookup_error():
    module, fake_client, _ = _load_logged_user_module()

    def _get_logged_user(cls):
        raise RuntimeError("missing token user")

    fake_client.User.get_logged_user = classmethod(_get_logged_user)

    with pytest.raises(RuntimeError, match="missing token user"):
        module.render_logged_user_username()


def test_render_logged_user_details_falls_back_on_missing_header_error():
    module, fake_client, _ = _load_logged_user_module()

    user = fake_client.User(
        username="jose",
        email="jose@main-sequence.io",
        organization=fake_client.Organization(name="Main Sequence"),
        groups=[],
        plan=None,
        active_plan_type=None,
        phone_number=None,
        api_request_limit=10000,
        user_permissions=[],
        date_joined=dt.datetime(2026, 1, 1, 10, 0, 0),
        last_login=None,
        organization_teams=[],
        is_active=True,
        is_verified=True,
        mfa_enabled=True,
        blocked_access=False,
        requires_password_change=False,
    )
    calls = {"logged": 0, "authenticated": 0}

    def _get_logged_user(cls):
        calls["logged"] += 1
        raise RuntimeError("Missing X-User-ID in request headers.")

    def _get_authenticated_user_details(cls):
        calls["authenticated"] += 1
        return user

    fake_client.User.get_logged_user = classmethod(_get_logged_user)
    fake_client.User.get_authenticated_user_details = classmethod(
        _get_authenticated_user_details
    )

    result = module.render_logged_user_details()

    assert result is user
    assert calls == {"logged": 1, "authenticated": 1}


def test_render_logged_user_details_raises_when_no_user_can_be_resolved():
    module, fake_client, _ = _load_logged_user_module()

    def _get_logged_user(cls):
        return None

    def _get_authenticated_user_details(cls):
        return None

    fake_client.User.get_logged_user = classmethod(_get_logged_user)
    fake_client.User.get_authenticated_user_details = classmethod(
        _get_authenticated_user_details
    )

    with pytest.raises(RuntimeError, match="No authenticated user available."):
        module.render_logged_user_details()
