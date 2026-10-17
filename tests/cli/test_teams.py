"""`mainsequence organization teams` membership and access commands.

The commands are tested with the CLI's API helpers replaced; the helpers are
tested with the SDK client replaced; and two commands run the real SDK client
against a mocked `make_request` at the CLI's backend URL.
"""

from __future__ import annotations

import importlib
import json
import types

import pytest

from tests.cli.support import TEAM_UID, USER_UID

OTHER_TEAM_UID = "9a1e2d3c-4b5a-4c6d-8e7f-0a1b2c3d4e5f"
BINDING_UID = "7d3c6a51-0b8e-4c2f-9a14-5e6f7a8b9c0d"


def _member(**overrides):
    member = {
        "uid": USER_UID,
        "username": "ana",
        "email": "ana@example.com",
        "first_name": "Ana",
        "last_name": "Smith",
        "member_kind": "person",
        "phone_number": None,
    }
    member.update(overrides)
    return member


def _binding(**overrides):
    binding = {
        "uid": BINDING_UID,
        "policy": {"id": "organization-teams.viewer", "label": "Viewer"},
        "principal": {
            "type": "user",
            "uid": USER_UID,
            "name": "Ana Smith",
            "email": "ana@example.com",
            "identity_type": "human",
        },
        "source": "explicit",
        "granted_by": {"uid": OTHER_TEAM_UID, "email": "owner@example.com"},
        "workflow_declaration": None,
        "created_at": "2026-10-17T09:00:00Z",
    }
    binding.update(overrides)
    return binding


def _listing():
    return {
        "object": {"resource_type": "organization-teams", "uid": TEAM_UID},
        "bindings": [
            _binding(),
            _binding(
                uid=OTHER_TEAM_UID,
                policy={"id": "organization-teams.owner", "label": "Owner"},
                principal={
                    "type": "team",
                    "uid": OTHER_TEAM_UID,
                    "name": "Platform",
                    "email": None,
                    "identity_type": None,
                },
                source="creator",
            ),
        ],
        "computed": [
            {
                "source": "role",
                "detail": "Organization administrators hold every action.",
                "policy": None,
            },
            {
                "source": "membership",
                "detail": "Members hold the member policy.",
                "policy": "organization-teams.member",
            },
        ],
    }


def _explanation(**overrides):
    explanation = {
        "action": "organization-teams.update",
        "allowed": True,
        "in_scope": True,
        "paths": [
            {
                "source": "team",
                "binding_uid": BINDING_UID,
                "policy": "organization-teams.editor",
                "team_uid": OTHER_TEAM_UID,
            }
        ],
        "blocked_by_rule": False,
        "granting_policies": ["organization-teams.editor", "organization-teams.owner"],
    }
    explanation.update(overrides)
    return explanation


@pytest.fixture()
def logged_in(cli_mod, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})


@pytest.fixture()
def rendered(cli_mod, monkeypatch):
    """Capture tables and panels instead of letting Rich wrap them at 80 columns."""

    captured = {"tables": [], "panels": []}
    monkeypatch.setattr(
        cli_mod,
        "print_table",
        lambda title, columns, rows: captured["tables"].append(
            (title, list(columns), [list(row) for row in rows])
        ),
    )
    monkeypatch.setattr(
        cli_mod,
        "print_kv",
        lambda title, items: captured["panels"].append((title, dict(items))),
    )
    return captured


# --- members ---------------------------------------------------------------------


def test_members_list_prints_each_member(cli_mod, runner, monkeypatch, logged_in, rendered):
    calls = []

    def _list(team_uid, *, timeout=None):
        calls.append((team_uid, timeout))
        return [
            _member(),
            _member(
                uid=OTHER_TEAM_UID,
                username=None,
                email=None,
                first_name=None,
                last_name=None,
                member_kind="workload",
            ),
        ]

    monkeypatch.setattr(cli_mod, "list_organization_team_members", _list)

    result = runner.invoke(
        cli_mod.app, ["organization", "teams", "members", "list", TEAM_UID, "--timeout", "3"]
    )

    assert result.exit_code == 0, result.output
    assert calls == [(TEAM_UID, 3)]
    assert rendered["tables"] == [
        (
            "Team Members",
            ["UID", "Kind", "Email", "Name"],
            [
                [USER_UID, "person", "ana@example.com", "Ana Smith"],
                [OTHER_TEAM_UID, "workload", "-", "-"],
            ],
        )
    ]
    assert "Total team members: 2" in result.output


def test_members_list_json_prints_the_members(cli_mod, runner, monkeypatch, logged_in):
    monkeypatch.setattr(
        cli_mod, "list_organization_team_members", lambda team_uid, *, timeout=None: [_member()]
    )

    result = runner.invoke(
        cli_mod.app, ["organization", "teams", "members", "list", TEAM_UID, "--json"]
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == [_member()]


def test_members_add_reports_the_member(cli_mod, runner, monkeypatch, logged_in, rendered):
    calls = []

    def _add(team_uid, user_uid, *, timeout=None):
        calls.append((team_uid, user_uid, timeout))
        return _member()

    monkeypatch.setattr(cli_mod, "add_organization_team_member", _add)

    result = runner.invoke(
        cli_mod.app, ["organization", "teams", "members", "add", TEAM_UID, USER_UID.upper()]
    )

    assert result.exit_code == 0, result.output
    assert calls == [(TEAM_UID, USER_UID, None)]
    assert f"ana@example.com is a member of team {TEAM_UID}." in result.output
    assert rendered["panels"] == [
        (
            "Team Member",
            {
                "UID": USER_UID,
                "Kind": "person",
                "Username": "ana",
                "Email": "ana@example.com",
                "Name": "Ana Smith",
            },
        )
    ]


def test_members_add_refuses_a_user_uid_that_is_not_a_uuid(cli_mod, runner, monkeypatch, logged_in):
    monkeypatch.setattr(
        cli_mod,
        "add_organization_team_member",
        lambda *args, **kwargs: pytest.fail("no request for a malformed UID"),
    )

    result = runner.invoke(
        cli_mod.app, ["organization", "teams", "members", "add", TEAM_UID, "ana"]
    )

    assert result.exit_code == 2


def test_members_add_reports_a_refusal(cli_mod, runner, monkeypatch, logged_in):
    def _add(team_uid, user_uid, *, timeout=None):
        raise cli_mod.ApiError(
            "400 POST url: This user is outside the people you can add to the Team."
        )

    monkeypatch.setattr(cli_mod, "add_organization_team_member", _add)

    result = runner.invoke(
        cli_mod.app, ["organization", "teams", "members", "add", TEAM_UID, USER_UID]
    )

    assert result.exit_code == 1
    assert "Team member addition failed: 400 POST url: This user is outside" in result.output


def test_members_remove_removes_the_member(cli_mod, runner, monkeypatch, logged_in):
    calls = []
    monkeypatch.setattr(
        cli_mod,
        "remove_organization_team_member",
        lambda team_uid, user_uid, *, timeout=None: calls.append((team_uid, user_uid, timeout)),
    )

    result = runner.invoke(
        cli_mod.app, ["organization", "teams", "members", "remove", TEAM_UID, USER_UID, "--json"]
    )

    assert result.exit_code == 0, result.output
    assert calls == [(TEAM_UID, USER_UID, None)]
    assert json.loads(result.output) == {
        "team_uid": TEAM_UID,
        "user_uid": USER_UID,
        "removed": True,
    }


def test_leave_leaves_the_team(cli_mod, runner, monkeypatch, logged_in):
    calls = []
    monkeypatch.setattr(
        cli_mod,
        "leave_organization_team",
        lambda team_uid, *, timeout=None: calls.append((team_uid, timeout)),
    )

    result = runner.invoke(
        cli_mod.app, ["organization", "teams", "leave", TEAM_UID, "--timeout", "8"]
    )

    assert result.exit_code == 0, result.output
    assert calls == [(TEAM_UID, 8)]
    assert f"You left team {TEAM_UID}." in result.output


def test_leave_reports_a_failure(cli_mod, runner, monkeypatch, logged_in):
    def _leave(team_uid, *, timeout=None):
        raise cli_mod.ApiError("400 POST url: You are not a member of this Team.")

    monkeypatch.setattr(cli_mod, "leave_organization_team", _leave)

    result = runner.invoke(cli_mod.app, ["organization", "teams", "leave", TEAM_UID])

    assert result.exit_code == 1
    assert (
        "Leaving the team failed: 400 POST url: You are not a member of this Team." in result.output
    )


def test_create_says_the_creator_owns_the_team(cli_mod, runner, monkeypatch, logged_in):
    monkeypatch.setattr(
        cli_mod,
        "create_organization_team",
        lambda *, name, description="", timeout=None: {
            "uid": TEAM_UID,
            "name": name,
            "description": description,
            "member_count": 1,
            "members": [_member()],
            "allowed_actions": ["organization-teams.delete"],
            "is_active": True,
        },
    )

    result = runner.invoke(
        cli_mod.app, ["organization", "teams", "create", "Research", "--description", ""]
    )

    assert result.exit_code == 0, result.output
    assert "Organization team created: Research" in result.output
    assert "You are its owner and its first member." in result.output


# --- access ----------------------------------------------------------------------


def test_access_list_prints_the_bindings_and_the_computed_access(
    cli_mod, runner, monkeypatch, logged_in, rendered
):
    calls = []

    def _get(team_uid, *, timeout=None):
        calls.append((team_uid, timeout))
        return _listing()

    monkeypatch.setattr(cli_mod, "get_organization_team_access", _get)

    result = runner.invoke(cli_mod.app, ["organization", "teams", "access", "list", TEAM_UID])

    assert result.exit_code == 0, result.output
    assert calls == [(TEAM_UID, None)]
    assert rendered["tables"] == [
        (
            "Team Policies",
            ["Binding UID", "Policy", "Principal", "Source"],
            [
                [BINDING_UID, "organization-teams.viewer", "ana@example.com", "explicit"],
                [OTHER_TEAM_UID, "organization-teams.owner", "Team Platform", "creator"],
            ],
        ),
        (
            "Computed Access",
            ["Source", "Policy", "Who"],
            [
                ["role", "every action", "Organization administrators hold every action."],
                ["membership", "organization-teams.member", "Members hold the member policy."],
            ],
        ),
    ]
    assert "Total policy bindings: 2" in result.output


def test_access_list_names_a_workload_principal(cli_mod, runner, monkeypatch, logged_in, rendered):
    workload = _binding(
        principal={
            "type": "user",
            "uid": OTHER_TEAM_UID,
            "name": "",
            "email": None,
            "identity_type": "workload",
        }
    )
    monkeypatch.setattr(
        cli_mod,
        "get_organization_team_access",
        lambda team_uid, *, timeout=None: {**_listing(), "bindings": [workload], "computed": []},
    )

    result = runner.invoke(cli_mod.app, ["organization", "teams", "access", "list", TEAM_UID])

    assert result.exit_code == 0, result.output
    assert rendered["tables"][0][2] == [
        [BINDING_UID, "organization-teams.viewer", f"Workload {OTHER_TEAM_UID}", "explicit"]
    ]


def test_access_list_json_prints_the_listing(cli_mod, runner, monkeypatch, logged_in):
    monkeypatch.setattr(
        cli_mod, "get_organization_team_access", lambda team_uid, *, timeout=None: _listing()
    )

    result = runner.invoke(
        cli_mod.app, ["organization", "teams", "access", "list", TEAM_UID, "--json"]
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == _listing()


@pytest.mark.parametrize(
    ("option", "principal_type", "principal_uid"),
    [("--user", "user", USER_UID), ("--team", "team", OTHER_TEAM_UID)],
)
def test_access_grant_attaches_the_policy_to_a_user_or_a_team(
    cli_mod, runner, monkeypatch, logged_in, rendered, option, principal_type, principal_uid
):
    calls = []

    def _attach(team_uid, *, policy, principal_type, principal_uid, timeout=None):
        calls.append((team_uid, policy, principal_type, principal_uid, timeout))
        return _binding()

    monkeypatch.setattr(cli_mod, "attach_organization_team_policy", _attach)

    result = runner.invoke(
        cli_mod.app,
        ["organization", "teams", "access", "grant", TEAM_UID, "viewer", option, principal_uid],
    )

    assert result.exit_code == 0, result.output
    assert calls == [(TEAM_UID, "viewer", principal_type, principal_uid, None)]
    assert "Attached organization-teams.viewer to ana@example.com" in result.output
    assert rendered["panels"][0][1]["Binding UID"] == BINDING_UID
    assert rendered["panels"][0][1]["Granted by"] == "owner@example.com"


@pytest.mark.parametrize(
    "principal_options",
    [[], ["--user", USER_UID, "--team", OTHER_TEAM_UID]],
    ids=["neither", "both"],
)
def test_access_grant_needs_exactly_one_principal(cli_mod, runner, monkeypatch, principal_options):
    monkeypatch.setattr(
        cli_mod, "_require_login", lambda: pytest.fail("no session check for a malformed command")
    )
    monkeypatch.setattr(
        cli_mod,
        "attach_organization_team_policy",
        lambda *args, **kwargs: pytest.fail("no request for a malformed command"),
    )

    result = runner.invoke(
        cli_mod.app,
        ["organization", "teams", "access", "grant", TEAM_UID, "viewer", *principal_options],
    )

    assert result.exit_code == 1
    assert "Pass exactly one of --user or --team." in result.output


def test_access_grant_json_prints_the_binding(cli_mod, runner, monkeypatch, logged_in):
    monkeypatch.setattr(
        cli_mod,
        "attach_organization_team_policy",
        lambda team_uid, *, policy, principal_type, principal_uid, timeout=None: _binding(),
    )

    result = runner.invoke(
        cli_mod.app,
        [
            "organization",
            "teams",
            "access",
            "grant",
            TEAM_UID,
            "viewer",
            "--user",
            USER_UID,
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == _binding()


def test_access_grant_reports_a_refusal_with_the_policies_that_give_it(
    cli_mod, runner, monkeypatch, logged_in
):
    def _attach(team_uid, *, policy, principal_type, principal_uid, timeout=None):
        raise cli_mod.ApiError(
            "403 POST url: This needs the organization-teams.manage_access action. "
            "Policies that give it: organization-teams.owner."
        )

    monkeypatch.setattr(cli_mod, "attach_organization_team_policy", _attach)

    result = runner.invoke(
        cli_mod.app,
        ["organization", "teams", "access", "grant", TEAM_UID, "owner", "--user", USER_UID],
    )

    assert result.exit_code == 1
    assert "Team access grant failed: 403 POST url" in result.output
    assert "Policies that give it: organization-teams.owner." in result.output


def test_access_revoke_detaches_the_binding(cli_mod, runner, monkeypatch, logged_in):
    calls = []
    monkeypatch.setattr(
        cli_mod,
        "detach_organization_team_policy",
        lambda team_uid, binding_uid, *, timeout=None: calls.append(
            (team_uid, binding_uid, timeout)
        ),
    )

    result = runner.invoke(
        cli_mod.app, ["organization", "teams", "access", "revoke", TEAM_UID, BINDING_UID]
    )

    assert result.exit_code == 0, result.output
    assert calls == [(TEAM_UID, BINDING_UID, None)]
    assert f"Detached binding {BINDING_UID} from team {TEAM_UID}." in result.output


def test_access_revoke_json_says_what_was_detached(cli_mod, runner, monkeypatch, logged_in):
    monkeypatch.setattr(
        cli_mod,
        "detach_organization_team_policy",
        lambda team_uid, binding_uid, *, timeout=None: None,
    )

    result = runner.invoke(
        cli_mod.app, ["organization", "teams", "access", "revoke", TEAM_UID, BINDING_UID, "--json"]
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == {
        "team_uid": TEAM_UID,
        "binding_uid": BINDING_UID,
        "detached": True,
    }


def test_access_explain_prints_the_answer_and_what_gives_it(
    cli_mod, runner, monkeypatch, logged_in, rendered
):
    calls = []

    def _explain(team_uid, *, action, principal_uid=None, timeout=None):
        calls.append((team_uid, action, principal_uid, timeout))
        return _explanation()

    monkeypatch.setattr(cli_mod, "explain_organization_team_access", _explain)

    result = runner.invoke(
        cli_mod.app,
        [
            "organization",
            "teams",
            "access",
            "explain",
            TEAM_UID,
            "organization-teams.update",
            "--user",
            USER_UID,
        ],
    )

    assert result.exit_code == 0, result.output
    assert calls == [(TEAM_UID, "organization-teams.update", USER_UID, None)]
    assert rendered["panels"] == [
        (
            "Access Explanation",
            {
                "Action": "organization-teams.update",
                "Allowed": "yes",
                "In scope": "yes",
                "Blocked by a platform rule": "no",
                "Policies that give it": "organization-teams.editor, organization-teams.owner",
            },
        )
    ]
    assert rendered["tables"] == [
        (
            "What Gives It",
            ["Source", "Policy", "Through"],
            [
                [
                    "team",
                    "organization-teams.editor",
                    f"team {OTHER_TEAM_UID}, binding {BINDING_UID}",
                ]
            ],
        )
    ]


def test_access_explain_says_when_nothing_gives_the_action(
    cli_mod, runner, monkeypatch, logged_in, rendered
):
    monkeypatch.setattr(
        cli_mod,
        "explain_organization_team_access",
        lambda team_uid, *, action, principal_uid=None, timeout=None: _explanation(
            action=action, allowed=False, paths=[], granting_policies=["organization-teams.owner"]
        ),
    )

    result = runner.invoke(
        cli_mod.app,
        ["organization", "teams", "access", "explain", TEAM_UID, "organization-teams.delete"],
    )

    assert result.exit_code == 0, result.output
    assert rendered["panels"][0][1]["Allowed"] == "no"
    assert rendered["tables"] == []
    assert "Nothing gives this action." in result.output


@pytest.mark.parametrize(
    "retired",
    ["can_view", "can_edit", "add_to_view", "add_to_edit", "remove_from_view", "remove_from_edit"],
)
def test_the_view_and_edit_sharing_commands_are_gone(cli_mod, runner, retired):
    arguments = ["organization", "teams", retired, TEAM_UID]
    if retired not in {"can_view", "can_edit"}:
        arguments.append(USER_UID)

    result = runner.invoke(cli_mod.app, arguments)

    assert result.exit_code == 2
    assert "No such command" in result.output


# --- API helpers -----------------------------------------------------------------


class _FakeTeam:
    def __init__(self, calls, action_result=None, action_error=None):
        self._calls = calls
        self._action_result = action_result
        self._action_error = action_error

    def __getattr__(self, name):
        def _action(*args, **kwargs):
            self._calls.append((name, args, kwargs))
            if self._action_error is not None:
                raise self._action_error
            return self._action_result

        return _action


def _install_fake_team(monkeypatch, *, calls, team=None, read_error=None):
    api_mod = importlib.import_module("mainsequence.cli.api")

    class _ClientTeam:
        @classmethod
        def get_by_uid(cls, uid, timeout=None):
            calls.append(("get_by_uid", (uid,), {"timeout": timeout}))
            if read_error is not None:
                raise read_error
            return team

    def _run_sdk_model_operation(*, module_name, class_name, operation):
        calls.append(("operation", (module_name, class_name), {}))
        return operation(_ClientTeam)

    monkeypatch.setattr(api_mod, "_run_sdk_model_operation", _run_sdk_model_operation)
    return api_mod


def _sdk_model(payload):
    return types.SimpleNamespace(model_dump=lambda mode="python": {**payload, "orm_class": "X"})


def test_list_organization_team_members_reads_the_team_then_its_members(cli_mod, monkeypatch):
    calls = []
    team = _FakeTeam(calls, action_result=[_sdk_model(_member())])
    api_mod = _install_fake_team(monkeypatch, calls=calls, team=team)

    out = api_mod.list_organization_team_members(TEAM_UID, timeout=4)

    assert out == [_member()]
    assert calls == [
        ("operation", ("mainsequence.client.models_user", "Team"), {}),
        ("get_by_uid", (TEAM_UID,), {"timeout": 4}),
        ("list_members", (), {"timeout": 4}),
    ]


def test_team_helpers_call_the_matching_team_methods(cli_mod, monkeypatch):
    calls = []
    team = _FakeTeam(calls, action_result=_sdk_model(_binding()))
    api_mod = _install_fake_team(monkeypatch, calls=calls, team=team)

    api_mod.add_organization_team_member(TEAM_UID, USER_UID, timeout=1)
    api_mod.remove_organization_team_member(TEAM_UID, USER_UID, timeout=2)
    api_mod.leave_organization_team(TEAM_UID, timeout=3)
    api_mod.get_organization_team_access(TEAM_UID, timeout=4)
    api_mod.attach_organization_team_policy(
        TEAM_UID, policy="viewer", principal_type="team", principal_uid=OTHER_TEAM_UID, timeout=5
    )
    api_mod.detach_organization_team_policy(TEAM_UID, BINDING_UID, timeout=6)
    explained = api_mod.explain_organization_team_access(
        TEAM_UID, action="organization-teams.read", principal_uid=USER_UID, timeout=7
    )

    team_calls = [call for call in calls if call[0] not in {"operation", "get_by_uid"}]
    assert team_calls == [
        ("add_member", (USER_UID,), {"timeout": 1}),
        ("remove_member", (USER_UID,), {"timeout": 2}),
        ("leave", (), {"timeout": 3}),
        ("get_access", (), {"timeout": 4}),
        (
            "attach_policy",
            ("viewer",),
            {"principal_type": "team", "principal_uid": OTHER_TEAM_UID, "timeout": 5},
        ),
        ("detach_policy", (BINDING_UID,), {"timeout": 6}),
        (
            "explain_access",
            ("organization-teams.read",),
            {"principal_uid": USER_UID, "timeout": 7},
        ),
    ]
    assert explained == _binding()


def test_a_team_that_cannot_be_read_is_reported_as_not_found(cli_mod, monkeypatch):
    class NotFoundError(Exception):
        pass

    api_mod = _install_fake_team(monkeypatch, calls=[], read_error=NotFoundError("404"))

    with pytest.raises(api_mod.ApiError, match=f"Team not found: {TEAM_UID}"):
        api_mod.leave_organization_team(TEAM_UID)


def test_a_refused_action_names_the_policies_that_give_it(cli_mod, monkeypatch):
    from mainsequence.client.exceptions import PermissionDeniedError

    api_mod = importlib.import_module("mainsequence.cli.api")
    refusal = PermissionDeniedError(
        "This needs the organization-teams.manage_access action.",
        payload={
            "detail": "This needs the organization-teams.manage_access action.",
            "code": "missing_action",
            "required_action": "organization-teams.manage_access",
            "granting_policies": ["organization-teams.owner"],
        },
    )

    def _run_sdk_model_operation(*, module_name, class_name, operation):
        # The CLI's model runner reports a 403 as NotLoggedIn, caused by the SDK error.
        raise api_mod.NotLoggedIn(str(refusal)) from refusal

    monkeypatch.setattr(api_mod, "_run_sdk_model_operation", _run_sdk_model_operation)

    with pytest.raises(api_mod.ApiError) as raised:
        api_mod.attach_organization_team_policy(
            TEAM_UID, policy="owner", principal_type="user", principal_uid=USER_UID
        )

    assert not isinstance(raised.value, api_mod.NotLoggedIn)
    assert str(raised.value) == (
        "This needs the organization-teams.manage_access action. "
        "Policies that give it: organization-teams.owner."
    )


def test_another_failure_keeps_its_message(cli_mod, monkeypatch):
    calls = []
    team = _FakeTeam(calls, action_error=ValueError("user_uid must be a UUID."))
    api_mod = _install_fake_team(monkeypatch, calls=calls, team=team)

    with pytest.raises(api_mod.ApiError, match=r"^user_uid must be a UUID\.$"):
        api_mod.add_organization_team_member(TEAM_UID, "ana")


# --- Through the real SDK client --------------------------------------------------


class _Response:
    def __init__(self, status_code, body=None):
        self.status_code = status_code
        self._body = body
        self.content = b"" if body is None else json.dumps(body).encode()
        self.text = self.content.decode()
        self.headers = {}

    def json(self):
        return self._body


def _point_the_cli_at_a_mocked_backend(cli_mod, monkeypatch, *responses):
    import mainsequence.client.base as base_mod
    import mainsequence.client.models_user as models_user
    from mainsequence.client import utils as client_utils

    api_mod = importlib.import_module("mainsequence.cli.api")
    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"}
    )
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    # The operation points the SDK at the CLI's backend and leaves it there; undo it afterwards.
    for name in ("MAINSEQUENCE_ENDPOINT", "API_ENDPOINT", "AUTH_ENDPOINT"):
        monkeypatch.setattr(client_utils, name, getattr(client_utils, name))
    monkeypatch.setattr(base_mod.BaseObjectOrm, "ROOT_URL", base_mod.BaseObjectOrm.ROOT_URL)
    monkeypatch.setattr(models_user.Team, "ROOT_URL", models_user.Team.ROOT_URL)

    queue = list(responses)
    sent = []

    def fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        sent.append((r_type, url, payload))
        return queue.pop(0)

    monkeypatch.setattr(base_mod, "make_request", fake_make_request)
    return sent


def test_access_grant_runs_through_the_sdk_client_at_the_cli_backend(cli_mod, runner, monkeypatch):
    sent = _point_the_cli_at_a_mocked_backend(
        cli_mod,
        monkeypatch,
        _Response(200, {"uid": TEAM_UID, "name": "Research", "members": [], "member_count": 0}),
        _Response(201, _binding()),
    )

    result = runner.invoke(
        cli_mod.app,
        [
            "organization",
            "teams",
            "access",
            "grant",
            TEAM_UID,
            "viewer",
            "--user",
            USER_UID,
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert sent == [
        ("GET", f"https://backend.test/api/v1/teams/{TEAM_UID}/", {"params": {}}),
        (
            "POST",
            f"https://backend.test/api/v1/teams/{TEAM_UID}/access/bindings/",
            {"json": {"policy": "viewer", "principal_type": "user", "principal_uid": USER_UID}},
        ),
    ]
    assert json.loads(result.output) == _binding()


def test_a_refusal_from_the_platform_reaches_the_command_with_its_policies(
    cli_mod, runner, monkeypatch
):
    sent = _point_the_cli_at_a_mocked_backend(
        cli_mod,
        monkeypatch,
        _Response(200, {"uid": TEAM_UID, "name": "Research", "members": [], "member_count": 0}),
        _Response(
            403,
            {
                "detail": "This needs the organization-teams.remove_member action.",
                "code": "missing_action",
                "required_action": "organization-teams.remove_member",
                "granting_policies": [
                    "organization-teams.membership_manager",
                    "organization-teams.owner",
                ],
                "resource_type": "organization-teams",
                "resource_uid": TEAM_UID,
            },
        ),
    )

    result = runner.invoke(
        cli_mod.app, ["organization", "teams", "members", "remove", TEAM_UID, USER_UID]
    )

    assert result.exit_code == 1
    assert sent[1][:2] == (
        "DELETE",
        f"https://backend.test/api/v1/teams/{TEAM_UID}/members/{USER_UID}/",
    )
    output = " ".join(result.output.split())
    assert "Team member removal failed: 403" in output
    assert "This needs the organization-teams.remove_member action." in output
    assert (
        "Policies that give it: organization-teams.membership_manager, organization-teams.owner."
        in output
    )
    assert "Not logged in" not in output
