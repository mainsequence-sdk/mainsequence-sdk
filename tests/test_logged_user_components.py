from __future__ import annotations

import datetime as dt
import importlib.util
import pathlib
import sys
import types


def _load_logged_user_module():
    repo_root = pathlib.Path(__file__).resolve().parents[1]
    module_path = repo_root / "mainsequence/dashboards/streamlit/components/logged_user.py"

    for name in ("mainsequence", "mainsequence.client", "streamlit", "logged_user_under_test"):
        sys.modules.pop(name, None)

    streamlit = types.ModuleType("streamlit")
    streamlit.session_state = {}
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
    return module, fake_client


def test_logged_user_detail_rows_include_membership_metadata():
    module, fake_client = _load_logged_user_module()

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
    module, fake_client = _load_logged_user_module()

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
