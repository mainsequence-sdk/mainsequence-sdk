"""`mainsequence constants access` commands.

The commands are tested with the CLI's API helpers replaced; the helpers are
tested with the SDK client replaced; and three commands run the real SDK client
against a mocked `make_request` at the CLI's backend URL, on a branch without an
Environment: a Constant's access routes address it by its UID alone.
"""

from __future__ import annotations

import importlib
import json
import types

import pytest

from tests.cli.support import TEAM_UID, USER_UID

CONSTANT_UID = "498d499f-b74c-43f7-acf1-2e2955ad0e6b"
BINDING_UID = "7d3c6a51-0b8e-4c2f-9a14-5e6f7a8b9c0d"
OTHER_BINDING_UID = "1b2c3d4e-5f60-4718-8293-a4b5c6d7e8f9"


def _binding(**overrides):
    binding = {
        "uid": BINDING_UID,
        "policy": {"id": "constants.reader", "label": "Constant reader"},
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
        "created_at": "2026-10-17T09:00:00Z",
    }
    binding.update(overrides)
    return binding


def _listing():
    return {
        "object": {"resource_type": "constants", "uid": CONSTANT_UID},
        "bindings": [
            _binding(),
            _binding(
                uid=OTHER_BINDING_UID,
                policy={"id": "constants.editor", "label": "Constant editor"},
                principal={
                    "type": "team",
                    "uid": TEAM_UID,
                    "name": "Research",
                    "email": None,
                    "identity_type": None,
                },
                source="workflow",
                workflow_declaration={"path": ".mainsequence/workflows/app.yaml", "key": "app"},
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
        "action": "constants.update",
        "allowed": True,
        "in_scope": True,
        "paths": [
            {
                "source": "team",
                "binding_uid": OTHER_BINDING_UID,
                "policy": "constants.editor",
                "team_uid": TEAM_UID,
            }
        ],
        "blocked_by_rule": False,
        "granting_policies": ["constants.editor", "constants.owner"],
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


# --- commands --------------------------------------------------------------------


def test_access_list_prints_the_bindings_and_the_computed_access(
    cli_mod, runner, monkeypatch, logged_in, rendered
):
    calls = []

    def _get(constant_uid, *, timeout=None):
        calls.append((constant_uid, timeout))
        return _listing()

    monkeypatch.setattr(cli_mod, "get_constant_access", _get)

    result = runner.invoke(
        cli_mod.app, ["constants", "access", "list", CONSTANT_UID, "--timeout", "6"]
    )

    assert result.exit_code == 0, result.output
    assert calls == [(CONSTANT_UID, 6)]
    assert rendered["tables"] == [
        (
            "Constant Policies",
            ["Binding UID", "Policy", "Principal", "Source"],
            [
                [BINDING_UID, "constants.reader", "ana@example.com", "explicit"],
                [OTHER_BINDING_UID, "constants.editor", "Team Research", "workflow"],
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
        "get_constant_access",
        lambda constant_uid, *, timeout=None: {**_listing(), "bindings": [], "computed": []},
    )

    result = runner.invoke(cli_mod.app, ["constants", "access", "list", CONSTANT_UID])

    assert result.exit_code == 0, result.output
    assert rendered["tables"] == []
    assert "No policies are attached to this constant." in result.output


def test_access_list_gives_every_action_only_to_the_organization_admin_row(
    cli_mod, runner, monkeypatch, logged_in, rendered
):
    managers = "The managers of the workload that created this object hold its creator rights."
    monkeypatch.setattr(
        cli_mod,
        "get_constant_access",
        lambda constant_uid, *, timeout=None: {
            **_listing(),
            "computed": [
                *_listing()["computed"],
                {"source": "manager", "detail": managers, "policy": None},
            ],
        },
    )

    result = runner.invoke(cli_mod.app, ["constants", "access", "list", CONSTANT_UID])

    assert result.exit_code == 0, result.output
    assert rendered["tables"][1] == (
        "Computed Access",
        ["Source", "Policy", "Who"],
        [
            ["role", "every action", "Organization administrators hold every action."],
            ["manager", "-", managers],
        ],
    )


def test_access_list_json_prints_the_listing(cli_mod, runner, monkeypatch, logged_in):
    monkeypatch.setattr(
        cli_mod, "get_constant_access", lambda constant_uid, *, timeout=None: _listing()
    )

    result = runner.invoke(cli_mod.app, ["constants", "access", "list", CONSTANT_UID, "--json"])

    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == _listing()


def test_access_list_reports_a_constant_that_cannot_be_reached(
    cli_mod, runner, monkeypatch, logged_in
):
    def _get(constant_uid, *, timeout=None):
        raise cli_mod.ApiError(f"Constant not found: {constant_uid}")

    monkeypatch.setattr(cli_mod, "get_constant_access", _get)

    result = runner.invoke(cli_mod.app, ["constants", "access", "list", CONSTANT_UID])

    assert result.exit_code == 1
    assert f"Constant access fetch failed: Constant not found: {CONSTANT_UID}" in result.output


@pytest.mark.parametrize(
    ("option", "principal_type", "principal_uid"),
    [("--user", "user", USER_UID), ("--team", "team", TEAM_UID)],
)
def test_access_grant_attaches_the_policy_to_a_user_or_a_team(
    cli_mod, runner, monkeypatch, logged_in, rendered, option, principal_type, principal_uid
):
    calls = []

    def _attach(constant_uid, *, policy, principal_type, principal_uid, timeout=None):
        calls.append((constant_uid, policy, principal_type, principal_uid, timeout))
        return _binding()

    monkeypatch.setattr(cli_mod, "attach_constant_policy", _attach)

    result = runner.invoke(
        cli_mod.app,
        ["constants", "access", "grant", CONSTANT_UID, "reader", option, principal_uid.upper()],
    )

    assert result.exit_code == 0, result.output
    assert calls == [(CONSTANT_UID, "reader", principal_type, principal_uid, None)]
    output = " ".join(result.output.split())
    assert f"Attached constants.reader to ana@example.com on constant {CONSTANT_UID}." in output
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
        "attach_constant_policy",
        lambda *args, **kwargs: pytest.fail("no request for a malformed command"),
    )

    result = runner.invoke(
        cli_mod.app,
        ["constants", "access", "grant", CONSTANT_UID, "reader", *principal_options],
    )

    assert result.exit_code == 1
    assert "Pass exactly one of --user or --team." in result.output


def test_access_grant_json_prints_the_binding(cli_mod, runner, monkeypatch, logged_in):
    monkeypatch.setattr(
        cli_mod,
        "attach_constant_policy",
        lambda constant_uid, *, policy, principal_type, principal_uid, timeout=None: _binding(),
    )

    result = runner.invoke(
        cli_mod.app,
        ["constants", "access", "grant", CONSTANT_UID, "reader", "--user", USER_UID, "--json"],
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == _binding()


def test_access_grant_reports_a_refusal_with_the_policies_that_give_it(
    cli_mod, runner, monkeypatch, logged_in
):
    def _attach(constant_uid, *, policy, principal_type, principal_uid, timeout=None):
        raise cli_mod.ApiError(
            "403 POST url: This needs the constants.manage_access action. "
            "Policies that give it: constants.owner."
        )

    monkeypatch.setattr(cli_mod, "attach_constant_policy", _attach)

    result = runner.invoke(
        cli_mod.app,
        ["constants", "access", "grant", CONSTANT_UID, "editor", "--user", USER_UID],
    )

    assert result.exit_code == 1
    output = " ".join(result.output.split())
    assert "Constant access grant failed: 403 POST url" in output
    assert "Policies that give it: constants.owner." in output


def test_access_revoke_detaches_the_binding(cli_mod, runner, monkeypatch, logged_in):
    calls = []
    monkeypatch.setattr(
        cli_mod,
        "detach_constant_policy",
        lambda constant_uid, binding_uid, *, timeout=None: calls.append(
            (constant_uid, binding_uid, timeout)
        ),
    )

    result = runner.invoke(
        cli_mod.app, ["constants", "access", "revoke", CONSTANT_UID, BINDING_UID]
    )

    assert result.exit_code == 0, result.output
    assert calls == [(CONSTANT_UID, BINDING_UID, None)]
    output = " ".join(result.output.split())
    assert f"Detached binding {BINDING_UID} from constant {CONSTANT_UID}." in output


def test_access_revoke_json_says_what_was_detached(cli_mod, runner, monkeypatch, logged_in):
    monkeypatch.setattr(
        cli_mod,
        "detach_constant_policy",
        lambda constant_uid, binding_uid, *, timeout=None: None,
    )

    result = runner.invoke(
        cli_mod.app, ["constants", "access", "revoke", CONSTANT_UID, BINDING_UID, "--json"]
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == {
        "constant_uid": CONSTANT_UID,
        "binding_uid": BINDING_UID,
        "detached": True,
    }


def test_access_revoke_refuses_a_binding_uid_that_is_not_a_uuid(
    cli_mod, runner, monkeypatch, logged_in
):
    monkeypatch.setattr(
        cli_mod,
        "detach_constant_policy",
        lambda *args, **kwargs: pytest.fail("no request for a malformed UID"),
    )

    result = runner.invoke(cli_mod.app, ["constants", "access", "revoke", CONSTANT_UID, "owner"])

    assert result.exit_code == 2


def test_access_explain_prints_the_answer_and_what_gives_it(
    cli_mod, runner, monkeypatch, logged_in, rendered
):
    calls = []

    def _explain(constant_uid, *, action, principal_uid=None, timeout=None):
        calls.append((constant_uid, action, principal_uid, timeout))
        return _explanation()

    monkeypatch.setattr(cli_mod, "explain_constant_access", _explain)

    result = runner.invoke(
        cli_mod.app,
        ["constants", "access", "explain", CONSTANT_UID, "constants.update", "--user", USER_UID],
    )

    assert result.exit_code == 0, result.output
    assert calls == [(CONSTANT_UID, "constants.update", USER_UID, None)]
    assert rendered["panels"] == [
        (
            "Access Explanation",
            {
                "Action": "constants.update",
                "Allowed": "yes",
                "In scope": "yes",
                "Blocked by a platform rule": "no",
                "Policies that give it": "constants.editor, constants.owner",
            },
        )
    ]
    assert rendered["tables"] == [
        (
            "What Gives It",
            ["Source", "Policy", "Through"],
            [["team", "constants.editor", f"team {TEAM_UID}, binding {OTHER_BINDING_UID}"]],
        )
    ]


def test_access_explain_says_when_nothing_gives_the_action(
    cli_mod, runner, monkeypatch, logged_in, rendered
):
    monkeypatch.setattr(
        cli_mod,
        "explain_constant_access",
        lambda constant_uid, *, action, principal_uid=None, timeout=None: _explanation(
            action=action, allowed=False, paths=[], granting_policies=["constants.owner"]
        ),
    )

    result = runner.invoke(
        cli_mod.app, ["constants", "access", "explain", CONSTANT_UID, "constants.manage_access"]
    )

    assert result.exit_code == 0, result.output
    assert rendered["panels"][0][1]["Allowed"] == "no"
    assert rendered["panels"][0][1]["Policies that give it"] == "constants.owner"
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
    arguments = ["constants", retired, CONSTANT_UID]
    if retired not in {"can_view", "can_edit"}:
        arguments.append(USER_UID)

    result = runner.invoke(cli_mod.app, arguments)

    assert result.exit_code == 2
    assert "No such command" in result.output


# --- API helpers -----------------------------------------------------------------


class _FakeConstant:
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


def _install_fake_constant(monkeypatch, *, calls, constant=None):
    api_mod = importlib.import_module("mainsequence.cli.api")

    class _ClientConstant:
        @classmethod
        def model_construct(cls, **values):
            calls.append(("model_construct", (), values))
            return constant

        @classmethod
        def get_by_uid(cls, *args, **kwargs):
            pytest.fail("reading a Constant needs an Environment; the access routes do not")

    def _run_sdk_model_operation(*, module_name, class_name, operation):
        calls.append(("operation", (module_name, class_name), {}))
        return operation(_ClientConstant)

    monkeypatch.setattr(api_mod, "_run_sdk_model_operation", _run_sdk_model_operation)
    return api_mod


def _sdk_model(payload):
    return types.SimpleNamespace(model_dump=lambda mode="python": {**payload, "orm_class": "X"})


def test_constant_helpers_call_the_access_methods_on_the_constant_uid_alone(cli_mod, monkeypatch):
    calls = []
    constant = _FakeConstant(calls, action_result=_sdk_model(_binding()))
    api_mod = _install_fake_constant(monkeypatch, calls=calls, constant=constant)

    listing = api_mod.get_constant_access(CONSTANT_UID, timeout=1)
    api_mod.attach_constant_policy(
        CONSTANT_UID, policy="reader", principal_type="team", principal_uid=TEAM_UID, timeout=2
    )
    api_mod.detach_constant_policy(CONSTANT_UID, BINDING_UID, timeout=3)
    api_mod.explain_constant_access(
        CONSTANT_UID, action="constants.read", principal_uid=USER_UID, timeout=4
    )

    assert listing == _binding()
    assert [call for call in calls if call[0] == "operation"] == [
        ("operation", ("mainsequence.client.models_foundry", "Constant"), {})
    ] * 4
    assert [call for call in calls if call[0] == "model_construct"] == [
        ("model_construct", (), {"uid": CONSTANT_UID})
    ] * 4
    constant_calls = [call for call in calls if call[0] not in {"operation", "model_construct"}]
    assert constant_calls == [
        ("get_access", (), {"timeout": 1}),
        (
            "attach_policy",
            ("reader",),
            {"principal_type": "team", "principal_uid": TEAM_UID, "timeout": 2},
        ),
        ("detach_policy", (BINDING_UID,), {"timeout": 3}),
        ("explain_access", ("constants.read",), {"principal_uid": USER_UID, "timeout": 4}),
    ]


def test_a_constant_that_cannot_be_reached_is_reported_as_not_found(cli_mod, monkeypatch):
    class NotFoundError(Exception):
        pass

    calls = []
    constant = _FakeConstant(calls, action_error=NotFoundError("404"))
    api_mod = _install_fake_constant(monkeypatch, calls=calls, constant=constant)

    with pytest.raises(api_mod.ApiError, match=f"^Constant not found: {CONSTANT_UID}$"):
        api_mod.get_constant_access(CONSTANT_UID)


def test_a_refused_constant_action_names_the_policies_that_give_it(cli_mod, monkeypatch):
    from mainsequence.client.exceptions import PermissionDeniedError

    api_mod = importlib.import_module("mainsequence.cli.api")
    refusal = PermissionDeniedError(
        "This needs the constants.manage_access action.",
        payload={
            "detail": "This needs the constants.manage_access action.",
            "code": "missing_action",
            "required_action": "constants.manage_access",
            "granting_policies": ["constants.owner"],
        },
    )

    def _run_sdk_model_operation(*, module_name, class_name, operation):
        # The CLI's model runner reports a 403 as NotLoggedIn, caused by the SDK error.
        raise api_mod.NotLoggedIn(str(refusal)) from refusal

    monkeypatch.setattr(api_mod, "_run_sdk_model_operation", _run_sdk_model_operation)

    with pytest.raises(api_mod.ApiError) as raised:
        api_mod.detach_constant_policy(CONSTANT_UID, BINDING_UID)

    assert not isinstance(raised.value, api_mod.NotLoggedIn)
    assert str(raised.value) == (
        "This needs the constants.manage_access action. Policies that give it: constants.owner."
    )


def test_another_constant_failure_keeps_its_message(cli_mod, monkeypatch):
    calls = []
    constant = _FakeConstant(calls, action_error=ValueError("principal_uid must be a UUID."))
    api_mod = _install_fake_constant(monkeypatch, calls=calls, constant=constant)

    with pytest.raises(api_mod.ApiError, match=r"^principal_uid must be a UUID\.$"):
        api_mod.attach_constant_policy(
            CONSTANT_UID, policy="reader", principal_type="user", principal_uid="ana"
        )


# --- Through the real SDK client, on a branch without an Environment ---------------


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
    import mainsequence.client.models_foundry as models_foundry
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
    monkeypatch.setattr(models_foundry.Constant, "ROOT_URL", models_foundry.Constant.ROOT_URL)

    def _no_environment(operation):
        raise code_repository_context.CodeRepositoryEnvironmentContextRequiredError(
            f"{operation} requires an Organization Environment."
        )

    monkeypatch.setattr(
        code_repository_context, "resolve_organization_environment_uid", _no_environment
    )

    queue = list(responses)
    sent = []

    def fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        sent.append((r_type, url, payload))
        return queue.pop(0)

    monkeypatch.setattr(base_mod, "make_request", fake_make_request)
    return sent


def test_access_grant_runs_through_the_sdk_client_without_an_environment(
    cli_mod, runner, monkeypatch
):
    sent = _point_the_cli_at_a_mocked_backend(cli_mod, monkeypatch, _Response(201, _binding()))

    result = runner.invoke(
        cli_mod.app,
        ["constants", "access", "grant", CONSTANT_UID, "reader", "--user", USER_UID, "--json"],
    )

    assert result.exit_code == 0, result.output
    assert sent == [
        (
            "POST",
            f"https://backend.test/api/v1/constants/{CONSTANT_UID}/access/bindings/",
            {"json": {"policy": "reader", "principal_type": "user", "principal_uid": USER_UID}},
        )
    ]
    assert json.loads(result.output) == _binding()


def test_a_constant_the_platform_does_not_show_is_reported_as_not_found(
    cli_mod, runner, monkeypatch
):
    sent = _point_the_cli_at_a_mocked_backend(
        cli_mod,
        monkeypatch,
        _Response(404, {"detail": "No Constant matches the given query."}),
    )

    result = runner.invoke(cli_mod.app, ["constants", "access", "list", CONSTANT_UID])

    assert result.exit_code == 1
    assert sent == [
        ("GET", f"https://backend.test/api/v1/constants/{CONSTANT_UID}/access/", {}),
    ]
    output = " ".join(result.output.split())
    assert f"Constant access fetch failed: Constant not found: {CONSTANT_UID}" in output


def test_a_refusal_from_the_platform_reaches_the_constant_command_with_its_policies(
    cli_mod, runner, monkeypatch
):
    sent = _point_the_cli_at_a_mocked_backend(
        cli_mod,
        monkeypatch,
        _Response(
            403,
            {
                "detail": "This needs the constants.manage_access action.",
                "code": "missing_action",
                "required_action": "constants.manage_access",
                "granting_policies": ["constants.owner"],
                "resource_type": "constants",
                "resource_uid": CONSTANT_UID,
            },
        ),
    )

    result = runner.invoke(
        cli_mod.app, ["constants", "access", "revoke", CONSTANT_UID, BINDING_UID]
    )

    assert result.exit_code == 1
    assert sent == [
        (
            "DELETE",
            f"https://backend.test/api/v1/constants/{CONSTANT_UID}/access/bindings/{BINDING_UID}/",
            {},
        )
    ]
    output = " ".join(result.output.split())
    assert "Constant access revoke failed: 403" in output
    assert "This needs the constants.manage_access action." in output
    assert "Policies that give it: constants.owner." in output
    assert "Not logged in" not in output
