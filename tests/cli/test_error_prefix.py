"""A command describes a failed SDK client call once (#230).

The CLI helpers turn the SDK client's errors into the CLI's `ApiError` without a
prefix, and the command that called them names the operation that failed.
"""

from __future__ import annotations

import importlib
from types import SimpleNamespace

import pytest

from mainsequence.client.exceptions import BadRequestError, ServerError

FAILED_URL = "https://backend.test/api/v1/failing-route/"
AGENT_UID = "e0e75693-4110-464c-93e0-82c7fd9c9a23"
CONSTANT_UID = "498d499f-b74c-43f7-acf1-2e2955ad0e6b"
SECRET_UID = "6c2b9e1d-3f4a-4b5c-8d7e-9f0a1b2c3d4e"
CODE_REPOSITORY_UID = "1d0530c0-65d1-4db0-856b-dc29d8260a09"
JOB_UID = "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da"
RELEASE_UID = "0ce33c15-e3b1-4677-a66e-70460b89198f"


def _server_error() -> ServerError:
    response = SimpleNamespace(
        status_code=500,
        url=FAILED_URL,
        request=SimpleNamespace(method="GET"),
        headers={},
    )
    return ServerError("Server Error", response=response)


@pytest.fixture()
def failing_sdk_client(cli_mod, monkeypatch):
    """Every SDK client call the CLI makes raises the SDK's ServerError for a 500."""

    api_mod = importlib.import_module("mainsequence.cli.api")
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _run_sdk_model_operation(**kwargs):
        raise _server_error()

    monkeypatch.setattr(api_mod, "_run_sdk_model_operation", _run_sdk_model_operation)


@pytest.mark.parametrize(
    ("arguments", "prefix"),
    [
        (["organization", "teams", "list"], "Organization teams fetch failed"),
        (["access", "policies"], "Access policies fetch failed"),
        (["agent", "list"], "Agents fetch failed"),
        (["agent", "logs", AGENT_UID], "Agent logs fetch failed"),
        (["constants", "list"], "Constants fetch failed"),
        (["secrets", "list"], "Secrets fetch failed"),
        (["agent", "access", "list", AGENT_UID], "Agent access fetch failed"),
        (["constants", "access", "list", CONSTANT_UID], "Constant access fetch failed"),
        (["secrets", "access", "list", SECRET_UID], "Secret access fetch failed"),
        (
            ["code-repository", "add-label", CODE_REPOSITORY_UID, "--label", "rates"],
            "CodeRepository add-label failed",
        ),
        (["code-repository", "search", "tutorial"], "CodeRepository search failed"),
        (
            ["code-repository", "access", "list", CODE_REPOSITORY_UID],
            "CodeRepository access fetch failed",
        ),
        (["code-repository", "jobs", "access", "list", JOB_UID], "Job access fetch failed"),
        (
            ["code-repository", "resources", "access", "list", RELEASE_UID],
            "ResourceRelease access fetch failed",
        ),
    ],
    ids=[
        "teams",
        "access-policies",
        "agents",
        "agent-observability",
        "constants",
        "secrets",
        "agent-access",
        "constant-access",
        "secret-access",
        "labels",
        "code-repositories",
        "code-repository-access",
        "job-access",
        "resource-release-access",
    ],
)
def test_a_failed_sdk_call_is_described_once(
    cli_mod, runner, failing_sdk_client, arguments, prefix
):
    result = runner.invoke(cli_mod.app, arguments)

    output = " ".join(result.output.split())
    assert result.exit_code == 1, result.output
    assert f"{prefix}: 500 GET {FAILED_URL}: Server Error" in output
    assert output.count(" failed: ") == 1, output


def test_the_helper_keeps_the_sdk_errors_message(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")

    def _run_sdk_model_operation(**kwargs):
        raise BadRequestError("name: This field is required.")

    monkeypatch.setattr(api_mod, "_run_sdk_model_operation", _run_sdk_model_operation)

    with pytest.raises(api_mod.ApiError) as raised:
        api_mod.create_organization_team(name="")

    assert str(raised.value) == "name: This field is required."
    assert isinstance(raised.value.__cause__, BadRequestError)


def test_an_error_without_a_message_is_named_by_its_type(cli_mod):
    api_mod = importlib.import_module("mainsequence.cli.api")

    assert str(api_mod._as_cli_error(RuntimeError())) == "RuntimeError"
