"""`mainsequence code-repository jobs access` commands.

The commands are tested with the CLI's API helpers replaced; the helpers are
tested with the SDK client replaced; and some commands run the real SDK client
against a mocked `make_request` at the CLI's backend URL. A Job's access routes
address it by its UID alone: no command reads the Job first, and none sends an
Environment, even from a branch that has one. A refusal keeps the platform's
message and names the policies that would allow the action.
"""

from __future__ import annotations

import importlib
import json
import types

import pytest

from tests.cli.support import TEAM_UID, USER_UID

JOB_UID = "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da"
BRANCH_UID = "5a28020a-0f1b-47ee-aab8-334286234bea"
ENVIRONMENT_UID = "58218213-5e4e-43de-a5bd-6757f4e1c8f6"
BINDING_UID = "7d3c6a51-0b8e-4c2f-9a14-5e6f7a8b9c0d"
OTHER_BINDING_UID = "1b2c3d4e-5f60-4718-8293-a4b5c6d7e8f9"
BRANCH_READER_UID = "2d4f6a8c-0b1d-4e3f-a5b7-c9d1e3f5a7b9"


def _person(**overrides):
    principal = {
        "type": "user",
        "uid": USER_UID,
        "name": "Ana Smith",
        "email": "ana@example.com",
        "identity_type": "human",
    }
    principal.update(overrides)
    return principal


def _binding(**overrides):
    binding = {
        "uid": BINDING_UID,
        "policy": {"id": "jobs.runner", "label": "Job runner"},
        "principal": _person(),
        "source": "explicit",
        "granted_by": {"uid": TEAM_UID, "email": "owner@example.com"},
        "workflow_declaration": None,
        "created_at": "2026-10-18T09:00:00Z",
    }
    binding.update(overrides)
    return binding


def _inherited_from_the_branch():
    return {
        "source": "inherited",
        "detail": "Code repository branch: code-repository-branches.reader gives jobs.viewer here.",
        "policy": "jobs.viewer",
        "principal": _person(uid=BRANCH_READER_UID, email="bo@example.com", name="Bo Lee"),
        "relation": "branch-job",
        "parent": {"resource_type": "code-repository-branches", "uid": BRANCH_UID},
        "parent_policy": "code-repository-branches.reader",
    }


def _listing():
    return {
        "object": {"resource_type": "jobs", "uid": JOB_UID},
        "bindings": [
            _binding(),
            _binding(
                uid=OTHER_BINDING_UID,
                policy={"id": "jobs.editor", "label": "Job editor"},
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
            },
            _inherited_from_the_branch(),
        ],
    }


def _explanation(**overrides):
    explanation = {
        "action": "jobs.run",
        "allowed": True,
        "in_scope": True,
        "paths": [
            {
                "source": "direct",
                "binding_uid": BINDING_UID,
                "policy": "jobs.runner",
                "team_uid": None,
            }
        ],
        "blocked_by_rule": False,
        "granting_policies": ["jobs.editor", "jobs.owner", "jobs.runner"],
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


def _jobs_access(*arguments):
    return ["code-repository", "jobs", "access", *arguments]


# --- commands --------------------------------------------------------------------


def test_access_list_prints_the_bindings_and_the_access_from_the_branch(
    cli_mod, runner, monkeypatch, logged_in, rendered
):
    calls = []

    def _get(job_uid, *, timeout=None):
        calls.append((job_uid, timeout))
        return _listing()

    monkeypatch.setattr(cli_mod, "get_code_repository_job_access", _get)

    result = runner.invoke(cli_mod.app, _jobs_access("list", JOB_UID, "--timeout", "6"))

    assert result.exit_code == 0, result.output
    assert calls == [(JOB_UID, 6)]
    assert rendered["tables"] == [
        (
            "Job Policies",
            ["Binding UID", "Policy", "Principal", "Source"],
            [
                [BINDING_UID, "jobs.runner", "ana@example.com", "explicit"],
                [OTHER_BINDING_UID, "jobs.editor", "Team Research", "migrated"],
            ],
        ),
        (
            "Computed Access",
            ["Source", "Policy", "Who"],
            [
                ["role", "every action", "Organization administrators hold every action."],
                [
                    "inherited",
                    "jobs.viewer",
                    "bo@example.com (Code repository branch: code-repository-branches.reader "
                    "gives jobs.viewer here.)",
                ],
            ],
        ),
    ]
    assert "Total policy bindings: 2" in result.output


def test_access_list_says_when_no_policy_is_attached(
    cli_mod, runner, monkeypatch, logged_in, rendered
):
    monkeypatch.setattr(
        cli_mod,
        "get_code_repository_job_access",
        lambda job_uid, *, timeout=None: {**_listing(), "bindings": [], "computed": []},
    )

    result = runner.invoke(cli_mod.app, _jobs_access("list", JOB_UID))

    assert result.exit_code == 0, result.output
    assert rendered["tables"] == []
    assert "No policies are attached to this job." in result.output


def test_access_list_json_prints_the_listing(cli_mod, runner, monkeypatch, logged_in):
    monkeypatch.setattr(
        cli_mod, "get_code_repository_job_access", lambda job_uid, *, timeout=None: _listing()
    )

    result = runner.invoke(cli_mod.app, _jobs_access("list", JOB_UID, "--json"))

    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == _listing()


def test_access_list_reports_a_job_that_cannot_be_reached(cli_mod, runner, monkeypatch, logged_in):
    def _get(job_uid, *, timeout=None):
        raise cli_mod.ApiError(f"Job not found: {job_uid}")

    monkeypatch.setattr(cli_mod, "get_code_repository_job_access", _get)

    result = runner.invoke(cli_mod.app, _jobs_access("list", JOB_UID))

    assert result.exit_code == 1
    output = " ".join(result.output.split())
    assert f"Job access fetch failed: Job not found: {JOB_UID}" in output


@pytest.mark.parametrize(
    ("option", "principal_type", "principal_uid"),
    [("--user", "user", USER_UID), ("--team", "team", TEAM_UID)],
)
def test_access_grant_attaches_the_policy_to_a_user_or_a_team(
    cli_mod, runner, monkeypatch, logged_in, rendered, option, principal_type, principal_uid
):
    calls = []

    def _attach(job_uid, *, policy, principal_type, principal_uid, timeout=None):
        calls.append((job_uid, policy, principal_type, principal_uid, timeout))
        return _binding()

    monkeypatch.setattr(cli_mod, "attach_code_repository_job_policy", _attach)

    result = runner.invoke(
        cli_mod.app,
        _jobs_access("grant", JOB_UID, "runner", option, principal_uid.upper()),
    )

    assert result.exit_code == 0, result.output
    assert calls == [(JOB_UID, "runner", principal_type, principal_uid, None)]
    output = " ".join(result.output.split())
    assert f"Attached jobs.runner to ana@example.com on job {JOB_UID}." in output
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
        "attach_code_repository_job_policy",
        lambda *args, **kwargs: pytest.fail("no request for a malformed command"),
    )

    result = runner.invoke(
        cli_mod.app, _jobs_access("grant", JOB_UID, "runner", *principal_options)
    )

    assert result.exit_code == 1
    assert "Pass exactly one of --user or --team." in result.output


def test_access_grant_json_prints_the_binding(cli_mod, runner, monkeypatch, logged_in):
    monkeypatch.setattr(
        cli_mod,
        "attach_code_repository_job_policy",
        lambda job_uid, *, policy, principal_type, principal_uid, timeout=None: _binding(),
    )

    result = runner.invoke(
        cli_mod.app, _jobs_access("grant", JOB_UID, "runner", "--user", USER_UID, "--json")
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == _binding()


def test_access_grant_reports_a_refusal_with_the_policies_that_give_it(
    cli_mod, runner, monkeypatch, logged_in
):
    def _attach(job_uid, *, policy, principal_type, principal_uid, timeout=None):
        raise cli_mod.ApiError(
            "403 POST url: This needs the jobs.manage_access action. "
            "Policies that give it: jobs.owner."
        )

    monkeypatch.setattr(cli_mod, "attach_code_repository_job_policy", _attach)

    result = runner.invoke(
        cli_mod.app, _jobs_access("grant", JOB_UID, "editor", "--user", USER_UID)
    )

    assert result.exit_code == 1
    output = " ".join(result.output.split())
    assert "Job access grant failed: 403 POST url" in output
    assert "Policies that give it: jobs.owner." in output


def test_access_revoke_detaches_the_binding(cli_mod, runner, monkeypatch, logged_in):
    calls = []
    monkeypatch.setattr(
        cli_mod,
        "detach_code_repository_job_policy",
        lambda job_uid, binding_uid, *, timeout=None: calls.append((job_uid, binding_uid, timeout)),
    )

    result = runner.invoke(cli_mod.app, _jobs_access("revoke", JOB_UID, BINDING_UID))

    assert result.exit_code == 0, result.output
    assert calls == [(JOB_UID, BINDING_UID, None)]
    output = " ".join(result.output.split())
    assert f"Detached binding {BINDING_UID} from job {JOB_UID}." in output


def test_access_revoke_json_says_what_was_detached(cli_mod, runner, monkeypatch, logged_in):
    monkeypatch.setattr(
        cli_mod,
        "detach_code_repository_job_policy",
        lambda job_uid, binding_uid, *, timeout=None: None,
    )

    result = runner.invoke(cli_mod.app, _jobs_access("revoke", JOB_UID, BINDING_UID, "--json"))

    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == {
        "job_uid": JOB_UID,
        "binding_uid": BINDING_UID,
        "detached": True,
    }


def test_access_revoke_refuses_a_binding_uid_that_is_not_a_uuid(
    cli_mod, runner, monkeypatch, logged_in
):
    monkeypatch.setattr(
        cli_mod,
        "detach_code_repository_job_policy",
        lambda *args, **kwargs: pytest.fail("no request for a malformed UID"),
    )

    result = runner.invoke(cli_mod.app, _jobs_access("revoke", JOB_UID, "owner"))

    assert result.exit_code == 2
    assert "invalid value" in result.output.lower()


def test_access_explain_prints_the_answer_and_what_gives_it(
    cli_mod, runner, monkeypatch, logged_in, rendered
):
    calls = []

    def _explain(job_uid, *, action, principal_uid=None, timeout=None):
        calls.append((job_uid, action, principal_uid, timeout))
        return _explanation()

    monkeypatch.setattr(cli_mod, "explain_code_repository_job_access", _explain)

    result = runner.invoke(
        cli_mod.app, _jobs_access("explain", JOB_UID, "jobs.run", "--user", USER_UID)
    )

    assert result.exit_code == 0, result.output
    assert calls == [(JOB_UID, "jobs.run", USER_UID, None)]
    assert rendered["panels"] == [
        (
            "Access Explanation",
            {
                "Action": "jobs.run",
                "Allowed": "yes",
                "In scope": "yes",
                "Blocked by a platform rule": "no",
                "Policies that give it": "jobs.editor, jobs.owner, jobs.runner",
            },
        )
    ]
    assert rendered["tables"] == [
        (
            "What Gives It",
            ["Source", "Policy", "Through"],
            [["direct", "jobs.runner", f"binding {BINDING_UID}"]],
        )
    ]


def test_access_explain_shows_that_a_backing_job_runs_only_with_its_release(
    cli_mod, runner, monkeypatch, logged_in, rendered
):
    # A release's backing Job is run with its release: a platform rule blocks
    # jobs.run on it, even for a holder of the release's editor policy.
    monkeypatch.setattr(
        cli_mod,
        "explain_code_repository_job_access",
        lambda job_uid, *, action, principal_uid=None, timeout=None: _explanation(
            allowed=False,
            blocked_by_rule=True,
            paths=[
                {
                    "source": "inherited",
                    "relation": "release-backing-job",
                    "parent_policy": "resource-releases.editor",
                    "policy": "jobs.editor",
                }
            ],
        ),
    )

    result = runner.invoke(cli_mod.app, _jobs_access("explain", JOB_UID, "jobs.run"))

    assert result.exit_code == 0, result.output
    assert rendered["panels"][0][1]["Allowed"] == "no"
    assert rendered["panels"][0][1]["Blocked by a platform rule"] == "yes"
    assert rendered["tables"] == [
        (
            "What Gives It",
            ["Source", "Policy", "Through"],
            [
                [
                    "inherited",
                    "jobs.editor",
                    "resource-releases.editor on its release-backing-job",
                ]
            ],
        )
    ]


# --- API helpers -----------------------------------------------------------------


class _FakeJob:
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


def _install_fake_job(monkeypatch, *, calls, job=None):
    api_mod = importlib.import_module("mainsequence.cli.api")

    class _ClientJob:
        @classmethod
        def model_construct(cls, **values):
            calls.append(("model_construct", (), values))
            return job

        @classmethod
        def get(cls, *args, **kwargs):
            pytest.fail("the access routes address the Job by its UID alone")

        @classmethod
        def get_by_uid(cls, *args, **kwargs):
            pytest.fail("the access routes address the Job by its UID alone")

    def _run_sdk_model_operation(*, module_name, class_name, operation):
        calls.append(("operation", (module_name, class_name), {}))
        return operation(_ClientJob)

    monkeypatch.setattr(api_mod, "_run_sdk_model_operation", _run_sdk_model_operation)
    return api_mod


def _sdk_model(payload):
    return types.SimpleNamespace(model_dump=lambda mode="python": {**payload, "orm_class": "X"})


def test_job_helpers_call_the_access_methods_on_the_uid_alone(cli_mod, monkeypatch):
    calls = []
    job = _FakeJob(calls, action_result=_sdk_model(_binding()))
    api_mod = _install_fake_job(monkeypatch, calls=calls, job=job)

    listing = api_mod.get_code_repository_job_access(JOB_UID, timeout=1)
    api_mod.attach_code_repository_job_policy(
        JOB_UID, policy="viewer", principal_type="team", principal_uid=TEAM_UID, timeout=2
    )
    api_mod.detach_code_repository_job_policy(JOB_UID, BINDING_UID, timeout=3)
    api_mod.explain_code_repository_job_access(
        JOB_UID, action="jobs.run", principal_uid=USER_UID, timeout=4
    )

    assert listing == _binding()
    assert [call for call in calls if call[0] == "operation"] == [
        ("operation", ("mainsequence.client.models_helpers", "Job"), {})
    ] * 4
    assert [call for call in calls if call[0] == "model_construct"] == [
        ("model_construct", (), {"uid": JOB_UID})
    ] * 4
    job_calls = [call for call in calls if call[0] not in {"operation", "model_construct"}]
    assert job_calls == [
        ("get_access", (), {"timeout": 1}),
        (
            "attach_policy",
            ("viewer",),
            {"principal_type": "team", "principal_uid": TEAM_UID, "timeout": 2},
        ),
        ("detach_policy", (BINDING_UID,), {"timeout": 3}),
        ("explain_access", ("jobs.run",), {"principal_uid": USER_UID, "timeout": 4}),
    ]


def test_a_job_that_cannot_be_reached_is_reported_as_not_found(cli_mod, monkeypatch):
    class NotFoundError(Exception):
        pass

    calls = []
    job = _FakeJob(calls, action_error=NotFoundError("404"))
    api_mod = _install_fake_job(monkeypatch, calls=calls, job=job)

    with pytest.raises(api_mod.ApiError, match=f"^Job not found: {JOB_UID}$"):
        api_mod.get_code_repository_job_access(JOB_UID)


def test_a_refused_job_action_names_the_policies_that_give_it(cli_mod, monkeypatch):
    from mainsequence.client.exceptions import PermissionDeniedError

    api_mod = importlib.import_module("mainsequence.cli.api")
    refusal = PermissionDeniedError(
        "This needs the jobs.manage_access action.",
        payload=_refusal("jobs.manage_access", ["jobs.owner"]),
    )

    def _run_sdk_model_operation(*, module_name, class_name, operation):
        # The CLI's model runner reports a 403 as NotLoggedIn, caused by the SDK error.
        raise api_mod.NotLoggedIn(str(refusal)) from refusal

    monkeypatch.setattr(api_mod, "_run_sdk_model_operation", _run_sdk_model_operation)

    with pytest.raises(api_mod.ApiError) as raised:
        api_mod.detach_code_repository_job_policy(JOB_UID, BINDING_UID)

    assert not isinstance(raised.value, api_mod.NotLoggedIn)
    assert str(raised.value) == (
        "This needs the jobs.manage_access action. Policies that give it: jobs.owner."
    )


def test_another_job_failure_keeps_its_message(cli_mod, monkeypatch):
    calls = []
    job = _FakeJob(calls, action_error=ValueError("principal_uid must be a UUID."))
    api_mod = _install_fake_job(monkeypatch, calls=calls, job=job)

    with pytest.raises(api_mod.ApiError, match=r"^principal_uid must be a UUID\.$"):
        api_mod.attach_code_repository_job_policy(
            JOB_UID, policy="runner", principal_type="user", principal_uid="ana"
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

    import mainsequence.client.base as base_mod
    import mainsequence.client.models_helpers as models_helpers
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
    monkeypatch.setattr(models_helpers.Job, "ROOT_URL", models_helpers.Job.ROOT_URL)
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


JOB_URL = f"https://backend.test/api/v1/jobs/{JOB_UID}/"


def test_access_grant_runs_through_the_sdk_client_without_an_environment(
    cli_mod, runner, monkeypatch
):
    sent = _point_the_cli_at_a_mocked_backend(cli_mod, monkeypatch, _Response(201, _binding()))

    result = runner.invoke(
        cli_mod.app, _jobs_access("grant", JOB_UID, "runner", "--user", USER_UID, "--json")
    )

    assert result.exit_code == 0, result.output
    # One request, with no organization_environment_uid anywhere in it.
    assert sent == [
        (
            "POST",
            f"{JOB_URL}access/bindings/",
            {"json": {"policy": "runner", "principal_type": "user", "principal_uid": USER_UID}},
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

    listed = runner.invoke(cli_mod.app, _jobs_access("list", JOB_UID, "--json"))
    explained = runner.invoke(cli_mod.app, _jobs_access("explain", JOB_UID, "jobs.run", "--json"))

    assert listed.exit_code == 0, listed.output
    assert explained.exit_code == 0, explained.output
    assert sent == [
        ("GET", f"{JOB_URL}access/", {}),
        ("GET", f"{JOB_URL}access/explain/", {"params": {"action": "jobs.run"}}),
    ]
    computed = json.loads(listed.output)["computed"]
    assert computed[1]["parent"] == {
        "resource_type": "code-repository-branches",
        "uid": BRANCH_UID,
    }
    assert json.loads(explained.output)["granting_policies"] == [
        "jobs.editor",
        "jobs.owner",
        "jobs.runner",
    ]


def test_a_job_the_platform_does_not_show_is_reported_as_not_found(cli_mod, runner, monkeypatch):
    sent = _point_the_cli_at_a_mocked_backend(
        cli_mod, monkeypatch, _Response(404, {"detail": "No Job matches the given query."})
    )

    result = runner.invoke(cli_mod.app, _jobs_access("list", JOB_UID))

    assert result.exit_code == 1
    assert sent == [("GET", f"{JOB_URL}access/", {})]
    output = " ".join(result.output.split())
    assert f"Job access fetch failed: Job not found: {JOB_UID}" in output


def test_a_refusal_from_the_platform_reaches_the_command_with_its_policies(
    cli_mod, runner, monkeypatch
):
    sent = _point_the_cli_at_a_mocked_backend(
        cli_mod,
        monkeypatch,
        _Response(
            403,
            {
                **_refusal("jobs.manage_access", ["jobs.owner"]),
                "resource_type": "jobs",
                "resource_uid": JOB_UID,
            },
        ),
    )

    result = runner.invoke(cli_mod.app, _jobs_access("revoke", JOB_UID, BINDING_UID))

    assert result.exit_code == 1
    assert sent == [("DELETE", f"{JOB_URL}access/bindings/{BINDING_UID}/", {})]
    output = " ".join(result.output.split())
    assert "Job access revoke failed: 403" in output
    assert "This needs the jobs.manage_access action." in output
    assert "Policies that give it: jobs.owner." in output
    assert "Not logged in" not in output
