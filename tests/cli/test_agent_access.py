"""`mainsequence agent access` commands.

The commands are tested with the CLI's API helpers replaced; the helpers are
tested with the SDK client replaced; and some commands run the real SDK client
against a mocked `make_request` at the CLI's backend URL. An Agent's access
routes address it by its UID alone: no command reads the Agent first, and none
sends an Environment, even from a branch that has one. A refusal keeps the
platform's message and names the policies that would allow the action.
"""

from __future__ import annotations

import importlib
import json
import types

import pytest

from tests.cli.support import TEAM_UID, USER_UID

AGENT_UID = "e0e75693-4110-464c-93e0-82c7fd9c9a23"
ENVIRONMENT_UID = "58218213-5e4e-43de-a5bd-6757f4e1c8f6"
BINDING_UID = "7d3c6a51-0b8e-4c2f-9a14-5e6f7a8b9c0d"
OTHER_BINDING_UID = "1b2c3d4e-5f60-4718-8293-a4b5c6d7e8f9"


def _binding(**overrides):
    binding = {
        "uid": BINDING_UID,
        "policy": {"id": "agents.user", "label": "Agent user"},
        "principal": {
            "type": "user",
            "uid": USER_UID,
            "name": "Ana Smith",
            "email": "ana@example.com",
            "identity_type": "human",
        },
        "source": "explicit",
        "granted_by": {"uid": TEAM_UID, "email": "owner@example.com"},
        "workflow_declaration": None,
        "created_at": "2026-10-18T09:00:00Z",
    }
    binding.update(overrides)
    return binding


def _listing():
    return {
        "object": {"resource_type": "agents", "uid": AGENT_UID},
        "bindings": [
            _binding(),
            _binding(
                uid=OTHER_BINDING_UID,
                policy={"id": "agents.operator", "label": "Agent operator"},
                principal={
                    "type": "team",
                    "uid": TEAM_UID,
                    "name": "Research",
                    "email": None,
                    "identity_type": None,
                },
                source="migrated",
            ),
        ],
        "computed": [
            {
                "source": "role",
                "detail": "Organization administrators hold every action.",
                "policy": None,
            }
        ],
    }


def _explanation(**overrides):
    explanation = {
        "action": "agents.use",
        "allowed": True,
        "in_scope": True,
        "paths": [
            {
                "source": "team",
                "binding_uid": OTHER_BINDING_UID,
                "policy": "agents.operator",
                "team_uid": TEAM_UID,
            }
        ],
        "blocked_by_rule": False,
        "granting_policies": [
            "agents.editor",
            "agents.operator",
            "agents.owner",
            "agents.user",
            "agents.viewer",
        ],
    }
    explanation.update(overrides)
    return explanation


def _refusal(action, granting_policies):
    return {
        "detail": f"This needs the {action} action.",
        "code": "missing_action",
        "required_action": action,
        "granting_policies": granting_policies,
    }


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


# --- commands --------------------------------------------------------------------


def test_access_list_prints_the_bindings_and_the_computed_access(
    cli_mod, runner, monkeypatch, logged_in, rendered
):
    calls = []

    def _get(agent_uid, *, timeout=None):
        calls.append((agent_uid, timeout))
        return _listing()

    monkeypatch.setattr(cli_mod, "get_agent_access", _get)

    result = runner.invoke(cli_mod.app, ["agent", "access", "list", AGENT_UID, "--timeout", "6"])

    assert result.exit_code == 0, result.output
    assert calls == [(AGENT_UID, 6)]
    assert rendered["tables"] == [
        (
            "Agent Policies",
            ["Binding UID", "Policy", "Principal", "Source"],
            [
                [BINDING_UID, "agents.user", "ana@example.com", "explicit"],
                [OTHER_BINDING_UID, "agents.operator", "Team Research", "migrated"],
            ],
        ),
        (
            "Computed Access",
            ["Source", "Policy", "Who"],
            [["role", "every action", "Organization administrators hold every action."]],
        ),
    ]
    assert "Total policy bindings: 2" in result.output


def test_access_list_says_when_no_policy_is_attached(
    cli_mod, runner, monkeypatch, logged_in, rendered
):
    monkeypatch.setattr(
        cli_mod,
        "get_agent_access",
        lambda agent_uid, *, timeout=None: {**_listing(), "bindings": [], "computed": []},
    )

    result = runner.invoke(cli_mod.app, ["agent", "access", "list", AGENT_UID])

    assert result.exit_code == 0, result.output
    assert rendered["tables"] == []
    assert "No policies are attached to this agent." in result.output


def test_access_list_json_prints_the_listing(cli_mod, runner, monkeypatch, logged_in):
    monkeypatch.setattr(cli_mod, "get_agent_access", lambda agent_uid, *, timeout=None: _listing())

    result = runner.invoke(cli_mod.app, ["agent", "access", "list", AGENT_UID, "--json"])

    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == _listing()


def test_access_list_reports_an_agent_that_cannot_be_reached(
    cli_mod, runner, monkeypatch, logged_in
):
    def _get(agent_uid, *, timeout=None):
        raise cli_mod.ApiError(f"Agent not found: {agent_uid}")

    monkeypatch.setattr(cli_mod, "get_agent_access", _get)

    result = runner.invoke(cli_mod.app, ["agent", "access", "list", AGENT_UID])

    assert result.exit_code == 1
    output = " ".join(result.output.split())
    assert f"Agent access fetch failed: Agent not found: {AGENT_UID}" in output


@pytest.mark.parametrize(
    ("option", "principal_type", "principal_uid"),
    [("--user", "user", USER_UID), ("--team", "team", TEAM_UID)],
)
def test_access_grant_attaches_the_policy_to_a_user_or_a_team(
    cli_mod, runner, monkeypatch, logged_in, rendered, option, principal_type, principal_uid
):
    calls = []

    def _attach(agent_uid, *, policy, principal_type, principal_uid, timeout=None):
        calls.append((agent_uid, policy, principal_type, principal_uid, timeout))
        return _binding()

    monkeypatch.setattr(cli_mod, "attach_agent_policy", _attach)

    result = runner.invoke(
        cli_mod.app,
        ["agent", "access", "grant", AGENT_UID, "user", option, principal_uid.upper()],
    )

    assert result.exit_code == 0, result.output
    assert calls == [(AGENT_UID, "user", principal_type, principal_uid, None)]
    output = " ".join(result.output.split())
    assert f"Attached agents.user to ana@example.com on agent {AGENT_UID}." in output
    assert rendered["panels"][0][1]["Binding UID"] == BINDING_UID
    assert rendered["panels"][0][1]["Granted by"] == "owner@example.com"


@pytest.mark.parametrize(
    "principal_options",
    [[], ["--user", USER_UID, "--team", TEAM_UID]],
    ids=["neither", "both"],
)
def test_access_grant_needs_exactly_one_principal(cli_mod, runner, monkeypatch, principal_options):
    monkeypatch.setattr(
        cli_mod, "_require_login", lambda: pytest.fail("no session check for a malformed command")
    )
    monkeypatch.setattr(
        cli_mod,
        "attach_agent_policy",
        lambda *args, **kwargs: pytest.fail("no request for a malformed command"),
    )

    result = runner.invoke(
        cli_mod.app, ["agent", "access", "grant", AGENT_UID, "user", *principal_options]
    )

    assert result.exit_code == 1
    assert "Pass exactly one of --user or --team." in result.output


def test_access_grant_json_prints_the_binding(cli_mod, runner, monkeypatch, logged_in):
    monkeypatch.setattr(
        cli_mod,
        "attach_agent_policy",
        lambda agent_uid, *, policy, principal_type, principal_uid, timeout=None: _binding(),
    )

    result = runner.invoke(
        cli_mod.app,
        ["agent", "access", "grant", AGENT_UID, "user", "--user", USER_UID, "--json"],
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == _binding()


def test_access_grant_reports_a_refusal_with_the_policies_that_give_it(
    cli_mod, runner, monkeypatch, logged_in
):
    def _attach(agent_uid, *, policy, principal_type, principal_uid, timeout=None):
        raise cli_mod.ApiError(
            "403 POST url: This needs the agents.manage_access action. "
            "Policies that give it: agents.owner."
        )

    monkeypatch.setattr(cli_mod, "attach_agent_policy", _attach)

    result = runner.invoke(
        cli_mod.app, ["agent", "access", "grant", AGENT_UID, "editor", "--user", USER_UID]
    )

    assert result.exit_code == 1
    output = " ".join(result.output.split())
    assert "Agent access grant failed: 403 POST url" in output
    assert "Policies that give it: agents.owner." in output


def test_access_revoke_detaches_the_binding(cli_mod, runner, monkeypatch, logged_in):
    calls = []
    monkeypatch.setattr(
        cli_mod,
        "detach_agent_policy",
        lambda agent_uid, binding_uid, *, timeout=None: calls.append(
            (agent_uid, binding_uid, timeout)
        ),
    )

    result = runner.invoke(cli_mod.app, ["agent", "access", "revoke", AGENT_UID, BINDING_UID])

    assert result.exit_code == 0, result.output
    assert calls == [(AGENT_UID, BINDING_UID, None)]
    output = " ".join(result.output.split())
    assert f"Detached binding {BINDING_UID} from agent {AGENT_UID}." in output


def test_access_revoke_json_says_what_was_detached(cli_mod, runner, monkeypatch, logged_in):
    monkeypatch.setattr(
        cli_mod, "detach_agent_policy", lambda agent_uid, binding_uid, *, timeout=None: None
    )

    result = runner.invoke(
        cli_mod.app, ["agent", "access", "revoke", AGENT_UID, BINDING_UID, "--json"]
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == {
        "agent_uid": AGENT_UID,
        "binding_uid": BINDING_UID,
        "detached": True,
    }


def test_access_revoke_refuses_a_binding_uid_that_is_not_a_uuid(
    cli_mod, runner, monkeypatch, logged_in
):
    monkeypatch.setattr(
        cli_mod,
        "detach_agent_policy",
        lambda *args, **kwargs: pytest.fail("no request for a malformed UID"),
    )

    result = runner.invoke(cli_mod.app, ["agent", "access", "revoke", AGENT_UID, "owner"])

    assert result.exit_code == 2
    assert "invalid value" in result.output.lower()


def test_access_explain_prints_the_answer_and_what_gives_it(
    cli_mod, runner, monkeypatch, logged_in, rendered
):
    calls = []

    def _explain(agent_uid, *, action, principal_uid=None, timeout=None):
        calls.append((agent_uid, action, principal_uid, timeout))
        return _explanation()

    monkeypatch.setattr(cli_mod, "explain_agent_access", _explain)

    result = runner.invoke(
        cli_mod.app,
        ["agent", "access", "explain", AGENT_UID, "agents.use", "--user", USER_UID],
    )

    assert result.exit_code == 0, result.output
    assert calls == [(AGENT_UID, "agents.use", USER_UID, None)]
    assert rendered["panels"] == [
        (
            "Access Explanation",
            {
                "Action": "agents.use",
                "Allowed": "yes",
                "In scope": "yes",
                "Blocked by a platform rule": "no",
                "Policies that give it": (
                    "agents.editor, agents.operator, agents.owner, agents.user, agents.viewer"
                ),
            },
        )
    ]
    assert rendered["tables"] == [
        (
            "What Gives It",
            ["Source", "Policy", "Through"],
            [["team", "agents.operator", f"team {TEAM_UID}, binding {OTHER_BINDING_UID}"]],
        )
    ]


def test_access_explain_says_that_no_policy_turns_on_acting_for_the_requester(
    cli_mod, runner, monkeypatch, logged_in, rendered
):
    # Acting for the requester is in no policy: only Organization admins turn it on.
    monkeypatch.setattr(
        cli_mod,
        "explain_agent_access",
        lambda agent_uid, *, action, principal_uid=None, timeout=None: _explanation(
            action=action, allowed=False, paths=[], granting_policies=[]
        ),
    )

    result = runner.invoke(
        cli_mod.app,
        ["agent", "access", "explain", AGENT_UID, "agents.set_acts_for_requester"],
    )

    assert result.exit_code == 0, result.output
    assert rendered["panels"][0][1]["Allowed"] == "no"
    assert rendered["panels"][0][1]["Policies that give it"] == "-"
    assert rendered["tables"] == []
    assert "Nothing gives this action." in result.output


@pytest.mark.parametrize(
    "retired",
    [
        "can_view",
        "can_edit",
        "add_to_view",
        "add_to_edit",
        "remove_from_view",
        "remove_from_edit",
        "add_team_to_view",
        "add_team_to_edit",
        "remove_team_from_view",
        "remove_team_from_edit",
    ],
)
def test_the_view_and_edit_sharing_commands_are_gone(cli_mod, runner, retired):
    arguments = ["agent", retired, AGENT_UID]
    if retired not in {"can_view", "can_edit"}:
        arguments.append(USER_UID)

    result = runner.invoke(cli_mod.app, arguments)

    assert result.exit_code == 2
    assert "No such command" in result.output


def test_the_generic_sharing_helpers_are_gone(cli_mod):
    for retired in (
        "_shareable_user_list_impl",
        "_shareable_user_access_update_impl",
        "_shareable_team_access_update_impl",
        "_format_shareable_permission_change",
        "_render_shareable_team_name",
    ):
        assert not hasattr(cli_mod, retired), retired


# --- API helpers -----------------------------------------------------------------


class _FakeAgent:
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


def _install_fake_agent(monkeypatch, *, calls, agent=None):
    api_mod = importlib.import_module("mainsequence.cli.api")

    class _ClientAgent:
        @classmethod
        def model_construct(cls, **values):
            calls.append(("model_construct", (), values))
            return agent

        @classmethod
        def get(cls, *args, **kwargs):
            pytest.fail("the access routes address the Agent by its UID alone")

        @classmethod
        def get_by_uid(cls, *args, **kwargs):
            pytest.fail("the access routes address the Agent by its UID alone")

    def _run_sdk_model_operation(*, module_name, class_name, operation):
        calls.append(("operation", (module_name, class_name), {}))
        return operation(_ClientAgent)

    monkeypatch.setattr(api_mod, "_run_sdk_model_operation", _run_sdk_model_operation)
    return api_mod


def _sdk_model(payload):
    return types.SimpleNamespace(model_dump=lambda mode="python": {**payload, "orm_class": "X"})


def test_agent_helpers_call_the_access_methods_on_the_uid_alone(cli_mod, monkeypatch):
    calls = []
    agent = _FakeAgent(calls, action_result=_sdk_model(_binding()))
    api_mod = _install_fake_agent(monkeypatch, calls=calls, agent=agent)

    listing = api_mod.get_agent_access(AGENT_UID, timeout=1)
    api_mod.attach_agent_policy(
        AGENT_UID, policy="viewer", principal_type="team", principal_uid=TEAM_UID, timeout=2
    )
    api_mod.detach_agent_policy(AGENT_UID, BINDING_UID, timeout=3)
    api_mod.explain_agent_access(AGENT_UID, action="agents.use", principal_uid=USER_UID, timeout=4)

    assert listing == _binding()
    assert [call for call in calls if call[0] == "operation"] == [
        ("operation", ("mainsequence.client.agent_runtime_models", "Agent"), {})
    ] * 4
    assert [call for call in calls if call[0] == "model_construct"] == [
        ("model_construct", (), {"uid": AGENT_UID})
    ] * 4
    agent_calls = [call for call in calls if call[0] not in {"operation", "model_construct"}]
    assert agent_calls == [
        ("get_access", (), {"timeout": 1}),
        (
            "attach_policy",
            ("viewer",),
            {"principal_type": "team", "principal_uid": TEAM_UID, "timeout": 2},
        ),
        ("detach_policy", (BINDING_UID,), {"timeout": 3}),
        ("explain_access", ("agents.use",), {"principal_uid": USER_UID, "timeout": 4}),
    ]


def test_an_agent_that_cannot_be_reached_is_reported_as_not_found(cli_mod, monkeypatch):
    class NotFoundError(Exception):
        pass

    calls = []
    agent = _FakeAgent(calls, action_error=NotFoundError("404"))
    api_mod = _install_fake_agent(monkeypatch, calls=calls, agent=agent)

    with pytest.raises(api_mod.ApiError, match=f"^Agent not found: {AGENT_UID}$"):
        api_mod.get_agent_access(AGENT_UID)


def _runner_reporting_a_refusal(api_mod, refusal):
    def _run_sdk_model_operation(*, module_name, class_name, operation):
        # The CLI's model runner reports a 403 as NotLoggedIn, caused by the SDK error.
        raise api_mod.NotLoggedIn(str(refusal)) from refusal

    return _run_sdk_model_operation


def test_a_refused_agent_action_names_the_policies_that_give_it(cli_mod, monkeypatch):
    from mainsequence.client.exceptions import PermissionDeniedError

    api_mod = importlib.import_module("mainsequence.cli.api")
    refusal = PermissionDeniedError(
        "This needs the agents.manage_access action.",
        payload=_refusal("agents.manage_access", ["agents.owner"]),
    )
    monkeypatch.setattr(
        api_mod, "_run_sdk_model_operation", _runner_reporting_a_refusal(api_mod, refusal)
    )

    with pytest.raises(api_mod.ApiError) as raised:
        api_mod.detach_agent_policy(AGENT_UID, BINDING_UID)

    assert not isinstance(raised.value, api_mod.NotLoggedIn)
    assert str(raised.value) == (
        "This needs the agents.manage_access action. Policies that give it: agents.owner."
    )


def test_another_agent_failure_keeps_its_message(cli_mod, monkeypatch):
    calls = []
    agent = _FakeAgent(calls, action_error=ValueError("principal_uid must be a UUID."))
    api_mod = _install_fake_agent(monkeypatch, calls=calls, agent=agent)

    with pytest.raises(api_mod.ApiError, match=r"^principal_uid must be a UUID\.$"):
        api_mod.attach_agent_policy(
            AGENT_UID, policy="user", principal_type="user", principal_uid="ana"
        )


# --- Through the real SDK client -------------------------------------------------


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
    """Answer the SDK client's requests in order, from a branch that has an Environment."""

    import mainsequence.client.agent_runtime_models as agent_runtime_models
    import mainsequence.client.base as base_mod
    import mainsequence.code_repository_context as code_repository_context
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
    monkeypatch.setattr(agent_runtime_models.Agent, "ROOT_URL", agent_runtime_models.Agent.ROOT_URL)
    # An Environment the SDK could send; the access routes must not send it.
    monkeypatch.setattr(
        code_repository_context,
        "resolve_organization_environment_uid",
        lambda operation: ENVIRONMENT_UID,
    )

    queue = list(responses)
    sent = []

    def fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        sent.append((r_type, url, payload))
        return queue.pop(0)

    monkeypatch.setattr(base_mod, "make_request", fake_make_request)
    return sent


AGENT_URL = f"https://backend.test/api/v1/agents/{AGENT_UID}/"


def test_access_grant_runs_through_the_sdk_client_without_an_environment(
    cli_mod, runner, monkeypatch
):
    sent = _point_the_cli_at_a_mocked_backend(cli_mod, monkeypatch, _Response(201, _binding()))

    result = runner.invoke(
        cli_mod.app,
        ["agent", "access", "grant", AGENT_UID, "user", "--user", USER_UID, "--json"],
    )

    assert result.exit_code == 0, result.output
    # One request, with no organization_environment_uid anywhere in it.
    assert sent == [
        (
            "POST",
            f"{AGENT_URL}access/bindings/",
            {"json": {"policy": "user", "principal_type": "user", "principal_uid": USER_UID}},
        )
    ]
    assert json.loads(result.output) == _binding()


def test_access_list_and_explain_send_no_environment(cli_mod, runner, monkeypatch):
    sent = _point_the_cli_at_a_mocked_backend(
        cli_mod,
        monkeypatch,
        _Response(200, _listing()),
        _Response(200, _explanation()),
    )

    listed = runner.invoke(cli_mod.app, ["agent", "access", "list", AGENT_UID, "--json"])
    explained = runner.invoke(
        cli_mod.app, ["agent", "access", "explain", AGENT_UID, "agents.use", "--json"]
    )

    assert listed.exit_code == 0, listed.output
    assert explained.exit_code == 0, explained.output
    assert sent == [
        ("GET", f"{AGENT_URL}access/", {}),
        ("GET", f"{AGENT_URL}access/explain/", {"params": {"action": "agents.use"}}),
    ]
    assert json.loads(listed.output)["object"]["resource_type"] == "agents"
    assert json.loads(explained.output)["paths"][0]["policy"] == "agents.operator"


def test_an_agent_the_platform_does_not_show_is_reported_as_not_found(cli_mod, runner, monkeypatch):
    sent = _point_the_cli_at_a_mocked_backend(
        cli_mod, monkeypatch, _Response(404, {"detail": "No Agent matches the given query."})
    )

    result = runner.invoke(cli_mod.app, ["agent", "access", "list", AGENT_UID])

    assert result.exit_code == 1
    assert sent == [("GET", f"{AGENT_URL}access/", {})]
    output = " ".join(result.output.split())
    assert f"Agent access fetch failed: Agent not found: {AGENT_UID}" in output


def test_a_refusal_from_the_platform_reaches_the_command_with_its_policies(
    cli_mod, runner, monkeypatch
):
    sent = _point_the_cli_at_a_mocked_backend(
        cli_mod,
        monkeypatch,
        _Response(
            403,
            {
                **_refusal("agents.manage_access", ["agents.owner"]),
                "resource_type": "agents",
                "resource_uid": AGENT_UID,
            },
        ),
    )

    result = runner.invoke(cli_mod.app, ["agent", "access", "revoke", AGENT_UID, BINDING_UID])

    assert result.exit_code == 1
    assert sent == [("DELETE", f"{AGENT_URL}access/bindings/{BINDING_UID}/", {})]
    output = " ".join(result.output.split())
    assert "Agent access revoke failed: 403" in output
    assert "This needs the agents.manage_access action." in output
    assert "Policies that give it: agents.owner." in output
    assert "Not logged in" not in output
