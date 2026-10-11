"""Every CLI request ends within the SDK's default request budget.

The tests cross the real HTTP boundary with a local server. A short stand-in for
the default budget (5 s connect, 120 s read, 125 s total) keeps them fast: the
CLI reads the default when each request starts.
"""

from __future__ import annotations

import json
import time
from contextlib import ExitStack

import pytest
import requests
from typer.testing import CliRunner

from mainsequence.cli import api as cli_api
from mainsequence.cli import cli as cli_mod
from mainsequence.client import _transport as transport
from tests._http_server import answer_json, answer_sequence, never_answer, serve

runner = CliRunner()

TOKEN_PATH = "/auth/cli/token/"
HANDOFF_START_PATH = "/auth/mcp/cli-handoff/start/"
HANDOFF_CALLBACK_PATH = "/auth/mcp/cli-handoff/callback/"
LOGOUT_PATH = "/auth/jwt-token/logout/"
REVOKE_PATH = "/auth/cli/revoke/"
REFRESH_PATH = "/auth/jwt-token/token/refresh/"
ME_PATH = "/api/v1/users/me/"


@pytest.fixture
def cli_backend(monkeypatch):
    """Serve routes as the CLI's backend, with a saved session; return (backend, calls)."""
    monkeypatch.delenv("MAINSEQUENCE_AUTH_MODE", raising=False)
    monkeypatch.setattr(cli_api, "_access_token", lambda: "saved-jwt")
    monkeypatch.setattr(cli_api, "_refresh_token", lambda: "saved-refresh")
    with ExitStack() as servers:

        def start(routes):
            url, calls = servers.enter_context(serve(routes))
            backend = url.rstrip("/")
            monkeypatch.setattr(cli_api, "backend_url", lambda: backend)
            monkeypatch.setattr(cli_mod.cfg, "backend_url", lambda: backend)
            return backend, calls

        yield start


@pytest.fixture
def short_default_budget(monkeypatch):
    monkeypatch.setattr(transport, "DEFAULT_TIMEOUT", (0.2, 0.3))


def _sent(calls):
    return [(method, path) for method, path, _body in calls]


@pytest.mark.parametrize(
    ("method", "path", "call"),
    [
        (
            "POST",
            TOKEN_PATH,
            lambda backend: cli_api.exchange_cli_authorization_code(
                code="code", code_verifier="verifier", redirect_uri="http://127.0.0.1/callback"
            ),
        ),
        (
            "POST",
            HANDOFF_START_PATH,
            lambda backend: cli_api.start_mcp_cli_handoff(state="state", code_challenge="c"),
        ),
        (
            "POST",
            HANDOFF_CALLBACK_PATH,
            lambda backend: cli_api.poll_mcp_cli_handoff(
                redirect_uri=f"{backend}{HANDOFF_CALLBACK_PATH}",
                handoff_uid="handoff",
                state="state",
                code_verifier="verifier",
            ),
        ),
        ("POST", REFRESH_PATH, lambda backend: cli_api.refresh_access()),
        ("GET", ME_PATH, lambda backend: cli_api.authed("GET", ME_PATH)),
    ],
    ids=["login-code-exchange", "mcp-handoff-start", "mcp-handoff-poll", "refresh", "authed"],
)
def test_request_to_a_backend_that_never_answers_fails_within_the_default_budget(
    cli_backend, short_default_budget, method, path, call
):
    backend, calls = cli_backend({(method, path): never_answer()})
    started = time.monotonic()
    with pytest.raises(requests.Timeout):
        call(backend)
    elapsed = time.monotonic() - started

    assert elapsed < 0.8
    assert _sent(calls) == [(method, path)]


def test_logout_ends_within_the_default_budget_when_the_backend_never_answers(
    cli_backend, short_default_budget
):
    backend, calls = cli_backend(
        {("POST", REVOKE_PATH): never_answer(), ("POST", LOGOUT_PATH): never_answer()}
    )
    started = time.monotonic()
    revoke = cli_api.logout_cli_session()
    logout = cli_api.logout_jwt_session()
    elapsed = time.monotonic() - started

    assert elapsed < 1.6
    assert revoke["attempted"] is True
    assert revoke["revoked"] is False
    assert revoke["method"] == "error"
    assert logout is False
    assert _sent(calls) == [("POST", REVOKE_PATH), ("POST", LOGOUT_PATH)]


def test_token_renewal_inside_an_mcp_request_is_bounded_too(cli_backend, short_default_budget):
    backend, calls = cli_backend(
        {
            ("POST", "/mcp"): answer_json(401, {"detail": "expired"}),
            ("POST", REFRESH_PATH): never_answer(),
        }
    )
    started = time.monotonic()
    with pytest.raises(cli_api.ApiError, match="MCP resources/list request timed out"):
        cli_api._mcp_json_rpc("resources/list", {}, request_id=7)
    elapsed = time.monotonic() - started

    assert elapsed < 0.8
    assert _sent(calls) == [("POST", "/mcp"), ("POST", REFRESH_PATH)]


def test_authenticated_command_reports_a_backend_that_never_answers(
    cli_backend, short_default_budget
):
    backend, calls = cli_backend({("GET", ME_PATH): never_answer()})

    result = runner.invoke(cli_mod.app, ["code-repository", "list"])

    assert result.exit_code == 1
    assert isinstance(result.exception, SystemExit)
    assert result.output == f"The backend at {backend} could not be reached (ReadTimeout).\n"
    assert _sent(calls) == [("GET", ME_PATH)]


@pytest.mark.parametrize(
    "failure", [requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout]
)
def test_login_says_when_the_backend_could_not_be_reached(monkeypatch, failure):
    monkeypatch.delenv("MAINSEQUENCE_AUTH_MODE", raising=False)
    monkeypatch.setattr(cli_mod.cfg, "backend_url", lambda: "https://backend.example")

    def unreachable(**_kwargs):
        raise failure("HTTPSConnectionPool(host='backend.example', port=443): Read timed out.")

    monkeypatch.setattr(cli_mod, "login_via_browser", unreachable)

    result = runner.invoke(cli_mod.app, ["login", "--no-status"])

    assert result.exit_code == 1
    # The command ended itself; an exception that escapes it is what prints a traceback.
    assert isinstance(result.exception, SystemExit)
    assert (
        f"The backend at https://backend.example could not be reached ({failure.__name__})."
        in result.output
    )
    assert "HTTPSConnectionPool" not in result.output


def test_refresh_and_the_authed_retry_work_against_a_responsive_backend(cli_backend, monkeypatch):
    saved = []
    monkeypatch.setattr(cli_api, "get_tokens", lambda: {"username": "user@example.com"})
    monkeypatch.setattr(cli_api, "save_tokens", lambda *args: saved.append(args) or True)
    backend, calls = cli_backend(
        {
            ("GET", ME_PATH): answer_sequence(
                answer_json(401, {"detail": "expired"}),
                answer_json(200, {"user": {"username": "user@example.com"}}),
            ),
            ("POST", REFRESH_PATH): answer_json(
                200, {"access": "renewed-jwt", "refresh": "renewed-refresh"}
            ),
        }
    )

    response = cli_api.authed("GET", ME_PATH)

    assert response.status_code == 200
    assert response.json() == {"user": {"username": "user@example.com"}}
    assert _sent(calls) == [("GET", ME_PATH), ("POST", REFRESH_PATH), ("GET", ME_PATH)]
    assert json.loads(calls[1][2]) == {"refresh": "saved-refresh"}
    assert saved == [("user@example.com", "renewed-jwt", "renewed-refresh")]


def test_login_code_exchange_works_against_a_responsive_backend(cli_backend):
    backend, calls = cli_backend(
        {("POST", TOKEN_PATH): answer_json(200, {"access": "jwt", "refresh": "refresh"})}
    )

    tokens = cli_api.exchange_cli_authorization_code(
        code="code", code_verifier="verifier", redirect_uri="http://127.0.0.1/callback"
    )

    assert tokens == {"backend": backend, "access": "jwt", "refresh": "refresh"}
    assert json.loads(calls[0][2]) == {
        "client_id": cli_api.CLI_BROWSER_CLIENT_ID,
        "code": "code",
        "code_verifier": "verifier",
        "redirect_uri": "http://127.0.0.1/callback",
    }


def test_logout_revoke_works_against_a_responsive_backend(cli_backend):
    backend, calls = cli_backend(
        {("POST", REVOKE_PATH): answer_json(200, {"detail": "CLI refresh token revoked."})}
    )

    result = cli_api.logout_cli_session()

    assert result == {
        "attempted": True,
        "revoked": True,
        "method": "cli_revoke",
        "detail": "CLI refresh token revoked.",
    }
    assert json.loads(calls[0][2]) == {"refresh": "saved-refresh"}
