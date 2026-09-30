"""CLI authentication behavior."""

from __future__ import annotations

import json

import pytest
from typer.testing import CliRunner

from mainsequence.cli import cli as cli_mod

runner = CliRunner()


@pytest.fixture(autouse=True)
def isolated_login_configuration(monkeypatch):
    monkeypatch.delenv("MAINSEQUENCE_AUTH_MODE", raising=False)
    monkeypatch.setattr(cli_mod.cfg, "set_session_overrides", lambda **kwargs: None)
    monkeypatch.setattr(
        cli_mod.cfg, "get_config", lambda: {"mainsequence_path": "/tmp/sdk-test-repositories"}
    )
    monkeypatch.setattr(cli_mod.cfg, "auth_persistence_label", lambda: "test store")
    monkeypatch.setattr(cli_mod, "get_current_user_profile", lambda: {})


def test_cli_exposes_core_and_code_repository_commands():
    result = runner.invoke(cli_mod.app, ["--help"])
    assert result.exit_code == 0
    for command in ("login", "logout", "settings", "version", "doctor"):
        assert command in result.output
    assert "code-repository" in result.output


def test_manual_jwt_login_persists_tokens(monkeypatch):
    saved = {}
    monkeypatch.setattr(cli_mod.cfg, "backend_url", lambda: "https://backend.example")
    monkeypatch.setattr(
        cli_mod.cfg,
        "save_tokens",
        lambda username, access, refresh: (
            saved.update(username=username, access=access, refresh=refresh) or True
        ),
    )

    result = runner.invoke(
        cli_mod.app, ["login", "--access-token", "access", "--refresh-token", "refresh"]
    )

    assert result.exit_code == 0
    assert saved == {"username": "", "access": "access", "refresh": "refresh"}
    assert "Signed in" in result.output


def test_browser_login_uses_existing_auth_helper(monkeypatch):
    monkeypatch.setattr(cli_mod.cfg, "backend_url", lambda: "https://backend.example")
    monkeypatch.setattr(cli_mod.cfg, "save_tokens", lambda *_: True)
    monkeypatch.setattr(
        cli_mod,
        "login_via_browser",
        lambda **kwargs: {"access": "browser-access", "refresh": "browser-refresh"},
    )

    result = runner.invoke(cli_mod.app, ["login", "--no-open"])

    assert result.exit_code == 0
    assert "Signed in" in result.output


def test_mcp_login_uses_existing_handoff_helper(monkeypatch):
    monkeypatch.setattr(cli_mod.cfg, "backend_url", lambda: "https://backend.example")
    monkeypatch.setattr(cli_mod.cfg, "save_tokens", lambda *_: True)

    def handoff(**kwargs):
        kwargs["on_handoff"](
            {"mcp_tool": "auth.cli_authorize", "mcp_arguments": {"handoff_uid": "h"}}
        )
        return {"access": "mcp-access", "refresh": "mcp-refresh"}

    monkeypatch.setattr(cli_mod, "login_via_mcp_handoff", handoff)

    result = runner.invoke(cli_mod.app, ["login", "--mcp"])

    assert result.exit_code == 0
    assert "auth.cli_authorize" in result.output


def test_logout_revokes_before_clearing(monkeypatch):
    calls = []
    monkeypatch.setattr(
        cli_mod,
        "logout_cli_session",
        lambda: calls.append("revoke") or {"attempted": True, "revoked": True},
    )
    monkeypatch.setattr(cli_mod.cfg, "clear_tokens", lambda: calls.append("clear") or True)
    monkeypatch.setattr(cli_mod.cfg, "clear_session_overrides", lambda: calls.append("overrides"))

    result = runner.invoke(cli_mod.app, ["logout"])

    assert result.exit_code == 0
    assert calls == ["revoke", "clear", "overrides"]


def test_doctor_uses_full_local_diagnostics(monkeypatch):
    calls = []
    monkeypatch.setattr(cli_mod, "run_doctor", lambda: calls.append("doctor"))

    result = runner.invoke(cli_mod.app, ["doctor"])

    assert result.exit_code == 0
    assert calls == ["doctor"]


@pytest.mark.parametrize("export_flag", ["--export", "--export-env"])
def test_login_preserves_positional_backend_base_and_auth_mode_export(monkeypatch, export_flag):
    overrides = {}
    monkeypatch.setattr(cli_mod.cfg, "backend_url", lambda: "https://backend.example")
    monkeypatch.setattr(cli_mod.cfg, "save_tokens", lambda *args: True)
    monkeypatch.setattr(
        cli_mod.cfg, "set_session_overrides", lambda **kwargs: overrides.update(kwargs)
    )
    result = runner.invoke(
        cli_mod.app,
        [
            "login",
            "https://another.example",
            "/tmp/sdk-repositories",
            "--access-token",
            "access",
            "--refresh-token",
            "refresh",
            export_flag,
        ],
    )
    assert result.exit_code == 0, result.output
    assert 'export MAINSEQUENCE_AUTH_MODE="jwt"' in result.output
    assert 'export MAINSEQUENCE_ACCESS_TOKEN="access"' in result.output
    assert 'export MAINSEQUENCE_REFRESH_TOKEN="refresh"' in result.output
    assert overrides["backend_url"].rstrip("/") == "https://another.example"
    assert overrides["mainsequence_path"] == "/tmp/sdk-repositories"


def test_runtime_login_exports_existing_runtime_mode(monkeypatch):
    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "runtime_credential")
    monkeypatch.setattr(cli_mod.cfg, "backend_url", lambda: "https://backend.example")
    monkeypatch.setattr(cli_mod.cfg, "save_tokens", lambda *args: True)
    monkeypatch.setattr(
        cli_mod,
        "_exchange_runtime_credential_for_cli_login",
        lambda backend: "runtime-access",
    )
    result = runner.invoke(cli_mod.app, ["login", "--export-env"])
    assert result.exit_code == 0, result.output
    assert 'export MAINSEQUENCE_AUTH_MODE="runtime_credential"' in result.output
    assert 'export MAINSEQUENCE_ACCESS_TOKEN="runtime-access"' in result.output
    assert "MAINSEQUENCE_REFRESH_TOKEN" not in result.output


def test_logout_preserves_export_env_alias(monkeypatch):
    monkeypatch.setattr(cli_mod, "logout_cli_session", lambda: {"attempted": False})
    monkeypatch.setattr(cli_mod.cfg, "clear_tokens", lambda: True)
    monkeypatch.setattr(cli_mod.cfg, "clear_session_overrides", lambda: None)
    result = runner.invoke(cli_mod.app, ["logout", "--export-env"])
    assert result.exit_code == 0, result.output
    assert "unset MAINSEQUENCE_ACCESS_TOKEN" in result.output


def test_auth_token_json_is_the_contract_other_tools_read(monkeypatch):
    monkeypatch.setattr(cli_mod.cfg, "backend_url", lambda: "https://backend.example")
    monkeypatch.setattr(cli_mod, "current_access_token", lambda: ("access-value", 1_900_000_000))

    result = runner.invoke(cli_mod.app, ["auth", "token", "--json"])

    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == {
        "endpoint": "https://backend.example",
        "access_token": "access-value",
        "token_type": "Bearer",
        "expires_at": 1_900_000_000,
    }


def test_auth_token_without_json_prints_the_access_token_alone(monkeypatch):
    monkeypatch.setattr(cli_mod.cfg, "backend_url", lambda: "https://backend.example")
    monkeypatch.setattr(cli_mod, "current_access_token", lambda: ("access-value", None))

    result = runner.invoke(cli_mod.app, ["auth", "token"])

    assert result.exit_code == 0, result.output
    assert result.output == "access-value\n"


def test_auth_token_never_prints_the_refresh_token(monkeypatch):
    from mainsequence.cli import api as api_mod

    monkeypatch.setattr(cli_mod.cfg, "backend_url", lambda: "https://backend.example")
    monkeypatch.setattr(
        api_mod,
        "get_tokens",
        lambda: {"username": "u", "access": _jwt(_now() + 3600), "refresh": "refresh-value"},
    )

    result = runner.invoke(cli_mod.app, ["auth", "token", "--json"])

    assert result.exit_code == 0, result.output
    assert "refresh-value" not in result.output
    assert set(json.loads(result.output)) == {
        "endpoint",
        "access_token",
        "token_type",
        "expires_at",
    }


def test_auth_token_without_a_session_fails_and_asks_for_a_login(monkeypatch):
    def no_session():
        raise cli_mod.NotLoggedIn("Not logged in.")

    monkeypatch.setattr(cli_mod, "current_access_token", no_session)
    monkeypatch.setattr(cli_mod.cfg, "secure_store_available", lambda: True)
    monkeypatch.setattr(cli_mod.cfg, "store_read_error", lambda: None)

    result = runner.invoke(cli_mod.app, ["auth", "token", "--json"])

    assert result.exit_code == cli_mod.AUTH_EXIT_NOT_LOGGED_IN == 1
    assert "Not logged in. Run: mainsequence login" in result.output
    assert "access_token" not in result.output


def test_auth_token_says_when_the_saved_session_could_not_be_read(monkeypatch):
    def no_session():
        raise cli_mod.NotLoggedIn("Not logged in.")

    monkeypatch.setattr(cli_mod, "current_access_token", no_session)
    monkeypatch.setattr(cli_mod.cfg, "secure_store_available", lambda: True)
    monkeypatch.setattr(
        cli_mod.cfg, "store_read_error", lambda: "The Keychain did not answer within 10 seconds."
    )

    result = runner.invoke(cli_mod.app, ["auth", "token"])

    assert result.exit_code == cli_mod.AUTH_EXIT_NOT_LOGGED_IN
    assert "The saved session could not be read." in result.output
    assert "The Keychain did not answer within 10 seconds." in result.output
    assert "mainsequence login" in result.output


def test_auth_token_on_a_machine_without_a_credential_store_says_so(monkeypatch):
    from mainsequence import bootstrap

    def no_session():
        raise cli_mod.NotLoggedIn("Not logged in.")

    monkeypatch.setattr(cli_mod, "current_access_token", no_session)
    monkeypatch.setattr(cli_mod.cfg, "secure_store_available", lambda: False)
    monkeypatch.setattr(bootstrap, "_credential_source", None)

    result = runner.invoke(cli_mod.app, ["auth", "token"])

    assert result.exit_code == cli_mod.AUTH_EXIT_NO_CREDENTIAL_STORE == 3
    assert "No credential store is available" in result.output


def test_auth_token_does_not_echo_a_transport_failure(monkeypatch):
    def unreachable():
        raise ConnectionError("https://backend.example/auth/?refresh=refresh-value")

    monkeypatch.setattr(cli_mod, "current_access_token", unreachable)

    result = runner.invoke(cli_mod.app, ["auth", "token"])

    assert result.exit_code == 1
    assert "refresh-value" not in result.output
    assert "ConnectionError" in result.output


def _session(**overrides):
    report = {
        "endpoint": "https://backend.example",
        "authenticated": True,
        "checked_with_backend": False,
        "auth_mode": "jwt",
        "username": "user@example.com",
        "source": "store",
        "storage": "test store",
        "store_error": None,
        "session_expires_at": 1_900_000_000,
        "access_expires_at": 1_899_000_000,
    }
    report.update(overrides)
    return report


def test_auth_status_reports_the_session_without_a_token(monkeypatch):
    monkeypatch.setattr(cli_mod.cfg, "session_report", _session)

    result = runner.invoke(cli_mod.app, ["auth", "status", "--json"])

    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == _session()


def test_auth_status_exit_code_says_whether_a_session_exists(monkeypatch):
    monkeypatch.setattr(cli_mod.cfg, "session_report", lambda: _session(authenticated=False))

    result = runner.invoke(cli_mod.app, ["auth", "status"])

    assert result.exit_code == 1
    assert "Authenticated" in result.output
    assert "Credential store error" not in result.output


def test_auth_status_and_doctor_tell_an_unreadable_store_from_no_session(monkeypatch):
    unreadable = _session(
        authenticated=False,
        username=None,
        source=None,
        session_expires_at=None,
        access_expires_at=None,
        store_error="The Keychain entry could not be read (security exit 128).",
    )
    monkeypatch.setattr(cli_mod.cfg, "session_report", lambda: unreadable)
    monkeypatch.setattr(
        cli_mod.cfg, "get_tokens", lambda: {"username": "", "access": "", "refresh": ""}
    )

    def text(output: str) -> str:
        # The report is drawn in a panel that wraps long lines.
        return " ".join(output.replace("│", " ").split())

    status = runner.invoke(cli_mod.app, ["auth", "status"])

    assert status.exit_code == 1
    assert "Credential store error" in text(status.output)
    assert "security exit 128" in text(status.output)

    doctor = runner.invoke(cli_mod.app, ["doctor"])

    assert doctor.exit_code == 0, doctor.output
    assert "Could not be read" in text(doctor.output)
    assert "security exit 128" in text(doctor.output)
    assert "mainsequence login" in text(doctor.output)


def test_auth_status_check_asks_the_backend(monkeypatch):
    monkeypatch.setattr(cli_mod.cfg, "session_report", lambda: _session(username=None))
    monkeypatch.setattr(
        cli_mod, "get_current_user_profile", lambda: {"username": "checked@example.com"}
    )

    accepted = runner.invoke(cli_mod.app, ["auth", "status", "--check", "--json"])

    assert accepted.exit_code == 0, accepted.output
    assert json.loads(accepted.output)["checked_with_backend"] is True
    assert json.loads(accepted.output)["username"] == "checked@example.com"

    def rejected():
        raise cli_mod.NotLoggedIn("Not logged in.")

    monkeypatch.setattr(cli_mod, "get_current_user_profile", rejected)

    refused = runner.invoke(cli_mod.app, ["auth", "status", "--check", "--json"])

    assert refused.exit_code == 1
    assert json.loads(refused.output)["authenticated"] is False


def _now() -> int:
    import time

    return int(time.time())


def _jwt(expiry: int | None) -> str:
    import base64

    claims = {} if expiry is None else {"exp": expiry}
    body = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=")
    return f"e30.{body}.signature"


def test_current_access_token_reuses_a_token_that_is_still_valid(monkeypatch):
    from mainsequence.cli import api as api_mod

    expiry = _now() + 3600
    access = _jwt(expiry)
    monkeypatch.setattr(api_mod, "get_tokens", lambda: {"access": access, "refresh": "r"})
    monkeypatch.setattr(
        api_mod, "refresh_access", lambda: pytest.fail("a valid token must not be renewed")
    )

    assert api_mod.current_access_token() == (access, expiry)


def test_current_access_token_renews_a_token_about_to_expire(monkeypatch):
    from mainsequence.cli import api as api_mod

    renewed = _jwt(_now() + 300)
    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"access": _jwt(_now() + 30), "refresh": "r"}
    )
    monkeypatch.setattr(api_mod, "refresh_access", lambda: renewed)

    assert api_mod.current_access_token()[0] == renewed


def test_current_access_token_renews_when_the_session_has_no_access_token(monkeypatch):
    from mainsequence.cli import api as api_mod

    renewed = _jwt(_now() + 300)
    monkeypatch.setattr(api_mod, "get_tokens", lambda: {"access": "", "refresh": "r"})
    monkeypatch.setattr(api_mod, "refresh_access", lambda: renewed)

    assert api_mod.current_access_token()[0] == renewed


def test_current_access_token_returns_a_token_that_carries_no_expiry(monkeypatch):
    from mainsequence.cli import api as api_mod

    monkeypatch.setattr(api_mod, "get_tokens", lambda: {"access": "opaque-token", "refresh": ""})
    monkeypatch.setattr(
        api_mod, "refresh_access", lambda: pytest.fail("an opaque token cannot be judged expired")
    )

    assert api_mod.current_access_token() == ("opaque-token", None)
