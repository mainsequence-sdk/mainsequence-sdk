"""`mainsequence secrets access` commands, and the refusals of a secret's value reads.

The commands are tested with the CLI's API helpers replaced; the helpers are
tested with the SDK client replaced; and some commands run the real SDK client
against a mocked `make_request` at the CLI's backend URL. The access commands
run on a branch without an Environment: a Secret's access routes address it by
its UID alone. `secrets delete` reads the secret's value first, and a refusal
there keeps the platform's message and names the policies that would allow it.
"""

from __future__ import annotations

import importlib
import json
import types

import pytest

from tests.cli.support import TEAM_UID, USER_UID

SECRET_UID = "6c2b9e1d-3f4a-4b5c-8d7e-9f0a1b2c3d4e"
ENVIRONMENT_UID = "58218213-5e4e-43de-a5bd-6757f4e1c8f6"
BINDING_UID = "7d3c6a51-0b8e-4c2f-9a14-5e6f7a8b9c0d"
OTHER_BINDING_UID = "1b2c3d4e-5f60-4718-8293-a4b5c6d7e8f9"


def _binding(**overrides):
    binding = {
        "uid": BINDING_UID,
        "policy": {"id": "secrets.value_reader", "label": "Secret value reader"},
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
        "object": {"resource_type": "secrets", "uid": SECRET_UID},
        "bindings": [
            _binding(),
            _binding(
                uid=OTHER_BINDING_UID,
                policy={"id": "secrets.editor", "label": "Secret editor"},
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
        "action": "secrets.read_value",
        "allowed": True,
        "in_scope": True,
        "paths": [
            {
                "source": "team",
                "binding_uid": OTHER_BINDING_UID,
                "policy": "secrets.editor",
                "team_uid": TEAM_UID,
            }
        ],
        "blocked_by_rule": False,
        "granting_policies": ["secrets.editor", "secrets.owner", "secrets.value_reader"],
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

    def _get(secret_uid, *, timeout=None):
        calls.append((secret_uid, timeout))
        return _listing()

    monkeypatch.setattr(cli_mod, "get_secret_access", _get)

    result = runner.invoke(cli_mod.app, ["secrets", "access", "list", SECRET_UID, "--timeout", "6"])

    assert result.exit_code == 0, result.output
    assert calls == [(SECRET_UID, 6)]
    assert rendered["tables"] == [
        (
            "Secret Policies",
            ["Binding UID", "Policy", "Principal", "Source"],
            [
                [BINDING_UID, "secrets.value_reader", "ana@example.com", "explicit"],
                [OTHER_BINDING_UID, "secrets.editor", "Team Research", "workflow"],
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
        "get_secret_access",
        lambda secret_uid, *, timeout=None: {**_listing(), "bindings": [], "computed": []},
    )

    result = runner.invoke(cli_mod.app, ["secrets", "access", "list", SECRET_UID])

    assert result.exit_code == 0, result.output
    assert rendered["tables"] == []
    assert "No policies are attached to this secret." in result.output


def test_access_list_json_prints_the_listing(cli_mod, runner, monkeypatch, logged_in):
    monkeypatch.setattr(
        cli_mod, "get_secret_access", lambda secret_uid, *, timeout=None: _listing()
    )

    result = runner.invoke(cli_mod.app, ["secrets", "access", "list", SECRET_UID, "--json"])

    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == _listing()


def test_access_list_reports_a_secret_that_cannot_be_reached(
    cli_mod, runner, monkeypatch, logged_in
):
    def _get(secret_uid, *, timeout=None):
        raise cli_mod.ApiError(f"Secret not found: {secret_uid}")

    monkeypatch.setattr(cli_mod, "get_secret_access", _get)

    result = runner.invoke(cli_mod.app, ["secrets", "access", "list", SECRET_UID])

    assert result.exit_code == 1
    assert f"Secret access fetch failed: Secret not found: {SECRET_UID}" in result.output


@pytest.mark.parametrize(
    ("option", "principal_type", "principal_uid"),
    [("--user", "user", USER_UID), ("--team", "team", TEAM_UID)],
)
def test_access_grant_attaches_the_policy_to_a_user_or_a_team(
    cli_mod, runner, monkeypatch, logged_in, rendered, option, principal_type, principal_uid
):
    calls = []

    def _attach(secret_uid, *, policy, principal_type, principal_uid, timeout=None):
        calls.append((secret_uid, policy, principal_type, principal_uid, timeout))
        return _binding()

    monkeypatch.setattr(cli_mod, "attach_secret_policy", _attach)

    result = runner.invoke(
        cli_mod.app,
        ["secrets", "access", "grant", SECRET_UID, "value_reader", option, principal_uid.upper()],
    )

    assert result.exit_code == 0, result.output
    assert calls == [(SECRET_UID, "value_reader", principal_type, principal_uid, None)]
    output = " ".join(result.output.split())
    assert f"Attached secrets.value_reader to ana@example.com on secret {SECRET_UID}." in output
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
        "attach_secret_policy",
        lambda *args, **kwargs: pytest.fail("no request for a malformed command"),
    )

    result = runner.invoke(
        cli_mod.app,
        ["secrets", "access", "grant", SECRET_UID, "reader", *principal_options],
    )

    assert result.exit_code == 1
    assert "Pass exactly one of --user or --team." in result.output


def test_access_grant_json_prints_the_binding(cli_mod, runner, monkeypatch, logged_in):
    monkeypatch.setattr(
        cli_mod,
        "attach_secret_policy",
        lambda secret_uid, *, policy, principal_type, principal_uid, timeout=None: _binding(),
    )

    result = runner.invoke(
        cli_mod.app,
        ["secrets", "access", "grant", SECRET_UID, "value_reader", "--user", USER_UID, "--json"],
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == _binding()


def test_access_grant_reports_a_refusal_with_the_policies_that_give_it(
    cli_mod, runner, monkeypatch, logged_in
):
    def _attach(secret_uid, *, policy, principal_type, principal_uid, timeout=None):
        raise cli_mod.ApiError(
            "403 POST url: This needs the secrets.manage_access action. "
            "Policies that give it: secrets.owner."
        )

    monkeypatch.setattr(cli_mod, "attach_secret_policy", _attach)

    result = runner.invoke(
        cli_mod.app,
        ["secrets", "access", "grant", SECRET_UID, "editor", "--user", USER_UID],
    )

    assert result.exit_code == 1
    output = " ".join(result.output.split())
    assert "Secret access grant failed: 403 POST url" in output
    assert "Policies that give it: secrets.owner." in output


def test_access_revoke_detaches_the_binding(cli_mod, runner, monkeypatch, logged_in):
    calls = []
    monkeypatch.setattr(
        cli_mod,
        "detach_secret_policy",
        lambda secret_uid, binding_uid, *, timeout=None: calls.append(
            (secret_uid, binding_uid, timeout)
        ),
    )

    result = runner.invoke(cli_mod.app, ["secrets", "access", "revoke", SECRET_UID, BINDING_UID])

    assert result.exit_code == 0, result.output
    assert calls == [(SECRET_UID, BINDING_UID, None)]
    output = " ".join(result.output.split())
    assert f"Detached binding {BINDING_UID} from secret {SECRET_UID}." in output


def test_access_revoke_json_says_what_was_detached(cli_mod, runner, monkeypatch, logged_in):
    monkeypatch.setattr(
        cli_mod,
        "detach_secret_policy",
        lambda secret_uid, binding_uid, *, timeout=None: None,
    )

    result = runner.invoke(
        cli_mod.app, ["secrets", "access", "revoke", SECRET_UID, BINDING_UID, "--json"]
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == {
        "secret_uid": SECRET_UID,
        "binding_uid": BINDING_UID,
        "detached": True,
    }


def test_access_revoke_refuses_a_binding_uid_that_is_not_a_uuid(
    cli_mod, runner, monkeypatch, logged_in
):
    monkeypatch.setattr(
        cli_mod,
        "detach_secret_policy",
        lambda *args, **kwargs: pytest.fail("no request for a malformed UID"),
    )

    result = runner.invoke(cli_mod.app, ["secrets", "access", "revoke", SECRET_UID, "owner"])

    assert result.exit_code == 2


def test_access_explain_prints_the_answer_and_what_gives_it(
    cli_mod, runner, monkeypatch, logged_in, rendered
):
    calls = []

    def _explain(secret_uid, *, action, principal_uid=None, timeout=None):
        calls.append((secret_uid, action, principal_uid, timeout))
        return _explanation()

    monkeypatch.setattr(cli_mod, "explain_secret_access", _explain)

    result = runner.invoke(
        cli_mod.app,
        ["secrets", "access", "explain", SECRET_UID, "secrets.read_value", "--user", USER_UID],
    )

    assert result.exit_code == 0, result.output
    assert calls == [(SECRET_UID, "secrets.read_value", USER_UID, None)]
    assert rendered["panels"] == [
        (
            "Access Explanation",
            {
                "Action": "secrets.read_value",
                "Allowed": "yes",
                "In scope": "yes",
                "Blocked by a platform rule": "no",
                "Policies that give it": "secrets.editor, secrets.owner, secrets.value_reader",
            },
        )
    ]
    assert rendered["tables"] == [
        (
            "What Gives It",
            ["Source", "Policy", "Through"],
            [["team", "secrets.editor", f"team {TEAM_UID}, binding {OTHER_BINDING_UID}"]],
        )
    ]


def test_access_explain_says_when_a_platform_rule_blocks_the_action(
    cli_mod, runner, monkeypatch, logged_in, rendered
):
    # A Secret the platform manages refuses a new value to everyone.
    monkeypatch.setattr(
        cli_mod,
        "explain_secret_access",
        lambda secret_uid, *, action, principal_uid=None, timeout=None: _explanation(
            action=action,
            allowed=False,
            paths=[],
            blocked_by_rule=True,
            granting_policies=["secrets.editor", "secrets.owner"],
        ),
    )

    result = runner.invoke(
        cli_mod.app, ["secrets", "access", "explain", SECRET_UID, "secrets.update_value"]
    )

    assert result.exit_code == 0, result.output
    assert rendered["panels"][0][1]["Allowed"] == "no"
    assert rendered["panels"][0][1]["Blocked by a platform rule"] == "yes"
    assert rendered["panels"][0][1]["Policies that give it"] == "secrets.editor, secrets.owner"
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
    arguments = ["secrets", retired, SECRET_UID]
    if retired not in {"can_view", "can_edit"}:
        arguments.append(USER_UID)

    result = runner.invoke(cli_mod.app, arguments)

    assert result.exit_code == 2
    assert "No such command" in result.output


# --- API helpers -----------------------------------------------------------------


class _FakeSecret:
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


def _install_fake_secret(monkeypatch, *, calls, secret=None):
    api_mod = importlib.import_module("mainsequence.cli.api")

    class _ClientSecret:
        @classmethod
        def model_construct(cls, **values):
            calls.append(("model_construct", (), values))
            return secret

        @classmethod
        def get(cls, *args, **kwargs):
            pytest.fail("reading a Secret reads its value; the access routes do not")

        @classmethod
        def get_by_uid(cls, *args, **kwargs):
            pytest.fail("reading a Secret reads its value; the access routes do not")

    def _run_sdk_model_operation(*, module_name, class_name, operation):
        calls.append(("operation", (module_name, class_name), {}))
        return operation(_ClientSecret)

    monkeypatch.setattr(api_mod, "_run_sdk_model_operation", _run_sdk_model_operation)
    return api_mod


def _sdk_model(payload):
    return types.SimpleNamespace(model_dump=lambda mode="python": {**payload, "orm_class": "X"})


def test_secret_helpers_call_the_access_methods_on_the_secret_uid_alone(cli_mod, monkeypatch):
    calls = []
    secret = _FakeSecret(calls, action_result=_sdk_model(_binding()))
    api_mod = _install_fake_secret(monkeypatch, calls=calls, secret=secret)

    listing = api_mod.get_secret_access(SECRET_UID, timeout=1)
    api_mod.attach_secret_policy(
        SECRET_UID, policy="value_reader", principal_type="team", principal_uid=TEAM_UID, timeout=2
    )
    api_mod.detach_secret_policy(SECRET_UID, BINDING_UID, timeout=3)
    api_mod.explain_secret_access(
        SECRET_UID, action="secrets.read_value", principal_uid=USER_UID, timeout=4
    )

    assert listing == _binding()
    assert [call for call in calls if call[0] == "operation"] == [
        ("operation", ("mainsequence.client.models_foundry", "Secret"), {})
    ] * 4
    assert [call for call in calls if call[0] == "model_construct"] == [
        ("model_construct", (), {"uid": SECRET_UID})
    ] * 4
    secret_calls = [call for call in calls if call[0] not in {"operation", "model_construct"}]
    assert secret_calls == [
        ("get_access", (), {"timeout": 1}),
        (
            "attach_policy",
            ("value_reader",),
            {"principal_type": "team", "principal_uid": TEAM_UID, "timeout": 2},
        ),
        ("detach_policy", (BINDING_UID,), {"timeout": 3}),
        ("explain_access", ("secrets.read_value",), {"principal_uid": USER_UID, "timeout": 4}),
    ]


def test_a_secret_that_cannot_be_reached_is_reported_as_not_found(cli_mod, monkeypatch):
    class NotFoundError(Exception):
        pass

    calls = []
    secret = _FakeSecret(calls, action_error=NotFoundError("404"))
    api_mod = _install_fake_secret(monkeypatch, calls=calls, secret=secret)

    with pytest.raises(api_mod.ApiError, match=f"^Secret not found: {SECRET_UID}$"):
        api_mod.get_secret_access(SECRET_UID)


def _runner_reporting_a_refusal(api_mod, refusal):
    def _run_sdk_model_operation(*, module_name, class_name, operation):
        # The CLI's model runner reports a 403 as NotLoggedIn, caused by the SDK error.
        raise api_mod.NotLoggedIn(str(refusal)) from refusal

    return _run_sdk_model_operation


def test_a_refused_secret_action_names_the_policies_that_give_it(cli_mod, monkeypatch):
    from mainsequence.client.exceptions import PermissionDeniedError

    api_mod = importlib.import_module("mainsequence.cli.api")
    refusal = PermissionDeniedError(
        "This needs the secrets.manage_access action.",
        payload=_refusal("secrets.manage_access", ["secrets.owner"]),
    )
    monkeypatch.setattr(
        api_mod, "_run_sdk_model_operation", _runner_reporting_a_refusal(api_mod, refusal)
    )

    with pytest.raises(api_mod.ApiError) as raised:
        api_mod.detach_secret_policy(SECRET_UID, BINDING_UID)

    assert not isinstance(raised.value, api_mod.NotLoggedIn)
    assert str(raised.value) == (
        "This needs the secrets.manage_access action. Policies that give it: secrets.owner."
    )


@pytest.mark.parametrize("helper_name", ["get_secret", "delete_secret"])
def test_a_refused_value_read_names_the_policies_that_give_it(cli_mod, monkeypatch, helper_name):
    from mainsequence.client.exceptions import PermissionDeniedError

    api_mod = importlib.import_module("mainsequence.cli.api")
    refusal = PermissionDeniedError(
        "This needs the secrets.read_value action.",
        payload=_refusal(
            "secrets.read_value", ["secrets.editor", "secrets.owner", "secrets.value_reader"]
        ),
    )
    monkeypatch.setattr(
        api_mod, "_run_sdk_model_operation", _runner_reporting_a_refusal(api_mod, refusal)
    )

    with pytest.raises(api_mod.ApiError) as raised:
        getattr(api_mod, helper_name)(SECRET_UID)

    assert not isinstance(raised.value, api_mod.NotLoggedIn)
    assert str(raised.value) == (
        "This needs the secrets.read_value action. Policies that give it: "
        "secrets.editor, secrets.owner, secrets.value_reader."
    )


def test_another_secret_failure_keeps_its_message(cli_mod, monkeypatch):
    calls = []
    secret = _FakeSecret(calls, action_error=ValueError("principal_uid must be a UUID."))
    api_mod = _install_fake_secret(monkeypatch, calls=calls, secret=secret)

    with pytest.raises(api_mod.ApiError, match=r"^principal_uid must be a UUID\.$"):
        api_mod.attach_secret_policy(
            SECRET_UID, policy="reader", principal_type="user", principal_uid="ana"
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


def _point_the_cli_at_a_mocked_backend(cli_mod, monkeypatch, *responses, environment_uid=None):
    """Answer the SDK client's requests in order; without `environment_uid` the branch has none."""

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
    monkeypatch.setattr(models_foundry.Secret, "ROOT_URL", models_foundry.Secret.ROOT_URL)

    def _resolve(operation):
        if environment_uid is None:
            raise code_repository_context.CodeRepositoryEnvironmentContextRequiredError(
                f"{operation} requires an Organization Environment."
            )
        return environment_uid

    monkeypatch.setattr(code_repository_context, "resolve_organization_environment_uid", _resolve)

    queue = list(responses)
    sent = []

    def fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        sent.append((r_type, url, payload))
        return queue.pop(0)

    monkeypatch.setattr(base_mod, "make_request", fake_make_request)
    return sent


SECRET_URL = f"https://backend.test/api/v1/secrets/{SECRET_UID}/"


def _secret_detail():
    return {
        "uid": SECRET_UID,
        "name": "API_KEY",
        "value": "provider-secret",
        "organization_environment_uid": ENVIRONMENT_UID,
        "organization_environment_name": "Development",
    }


def test_access_grant_runs_through_the_sdk_client_without_an_environment(
    cli_mod, runner, monkeypatch
):
    sent = _point_the_cli_at_a_mocked_backend(cli_mod, monkeypatch, _Response(201, _binding()))

    result = runner.invoke(
        cli_mod.app,
        ["secrets", "access", "grant", SECRET_UID, "value_reader", "--user", USER_UID, "--json"],
    )

    assert result.exit_code == 0, result.output
    assert sent == [
        (
            "POST",
            f"{SECRET_URL}access/bindings/",
            {
                "json": {
                    "policy": "value_reader",
                    "principal_type": "user",
                    "principal_uid": USER_UID,
                }
            },
        )
    ]
    assert json.loads(result.output) == _binding()


def test_a_secret_the_platform_does_not_show_is_reported_as_not_found(cli_mod, runner, monkeypatch):
    sent = _point_the_cli_at_a_mocked_backend(
        cli_mod,
        monkeypatch,
        _Response(404, {"detail": "No Secret matches the given query."}),
    )

    result = runner.invoke(cli_mod.app, ["secrets", "access", "list", SECRET_UID])

    assert result.exit_code == 1
    assert sent == [("GET", f"{SECRET_URL}access/", {})]
    output = " ".join(result.output.split())
    assert f"Secret access fetch failed: Secret not found: {SECRET_UID}" in output


def test_a_refusal_from_the_platform_reaches_the_secret_command_with_its_policies(
    cli_mod, runner, monkeypatch
):
    sent = _point_the_cli_at_a_mocked_backend(
        cli_mod,
        monkeypatch,
        _Response(
            403,
            {
                **_refusal("secrets.manage_access", ["secrets.owner"]),
                "resource_type": "secrets",
                "resource_uid": SECRET_UID,
            },
        ),
    )

    result = runner.invoke(cli_mod.app, ["secrets", "access", "revoke", SECRET_UID, BINDING_UID])

    assert result.exit_code == 1
    assert sent == [("DELETE", f"{SECRET_URL}access/bindings/{BINDING_UID}/", {})]
    output = " ".join(result.output.split())
    assert "Secret access revoke failed: 403" in output
    assert "This needs the secrets.manage_access action." in output
    assert "Policies that give it: secrets.owner." in output
    assert "Not logged in" not in output


def test_secrets_delete_reports_a_refused_value_read_and_deletes_nothing(
    cli_mod, runner, monkeypatch
):
    # A reader sees the secret but not its value; the preview reads the value.
    sent = _point_the_cli_at_a_mocked_backend(
        cli_mod,
        monkeypatch,
        _Response(
            403,
            _refusal(
                "secrets.read_value", ["secrets.editor", "secrets.owner", "secrets.value_reader"]
            ),
        ),
        environment_uid=ENVIRONMENT_UID,
    )

    result = runner.invoke(cli_mod.app, ["secrets", "delete", SECRET_UID])

    assert result.exit_code == 1
    assert sent == [
        ("GET", SECRET_URL, {"params": {"organization_environment_uid": ENVIRONMENT_UID}})
    ]
    output = " ".join(result.output.split())
    assert "Secret fetch failed: 403 This needs the secrets.read_value action." in output
    assert "Policies that give it: secrets.editor, secrets.owner, secrets.value_reader." in output
    assert "Not logged in" not in output


def test_secrets_delete_reports_a_refused_delete_with_the_policies_that_give_it(
    cli_mod, runner, monkeypatch
):
    # A value_reader reads the value but may not delete the secret.
    sent = _point_the_cli_at_a_mocked_backend(
        cli_mod,
        monkeypatch,
        _Response(200, _secret_detail()),
        _Response(200, _secret_detail()),
        _Response(403, _refusal("secrets.delete", ["secrets.editor", "secrets.owner"])),
        environment_uid=ENVIRONMENT_UID,
    )

    result = runner.invoke(cli_mod.app, ["secrets", "delete", SECRET_UID], input="API_KEY\n")

    assert result.exit_code == 1
    assert [(r_type, url) for r_type, url, _ in sent] == [
        ("GET", SECRET_URL),
        ("GET", SECRET_URL),
        ("DELETE", SECRET_URL),
    ]
    output = " ".join(result.output.split())
    assert "Secret deletion failed: 403 This needs the secrets.delete action." in output
    assert "Policies that give it: secrets.editor, secrets.owner." in output
    assert "provider-secret" not in output


def test_secrets_delete_reports_that_a_managed_secret_cannot_be_deleted(
    cli_mod, runner, monkeypatch
):
    managed = (
        "This secret is managed by hosted_database and cannot be deleted through the "
        "generic secret endpoint."
    )
    _point_the_cli_at_a_mocked_backend(
        cli_mod,
        monkeypatch,
        _Response(200, _secret_detail()),
        _Response(200, _secret_detail()),
        _Response(409, {"detail": managed, "managed_by": "hosted_database"}),
        environment_uid=ENVIRONMENT_UID,
    )

    result = runner.invoke(cli_mod.app, ["secrets", "delete", SECRET_UID], input="API_KEY\n")

    assert result.exit_code == 1
    output = " ".join(result.output.split())
    assert f"Secret deletion failed: 409 {managed}" in output
