from __future__ import annotations

import importlib
import json
import os
import sys
import types
from uuid import UUID

import pytest

from tests.cli.support import (
    _UNSUPPORTED_REPOSITORY_NUMERIC_ID_ENV,
    _UNSUPPORTED_REPOSITORY_UID_ENV,
    TEAM_UID,
    USER_UID,
    _session_report,
)


def test_login_mocked(cli_mod, runner, monkeypatch):
    configured_backend = "https://configured.example.test"
    session_override = {}
    monkeypatch.setattr(
        cli_mod,
        "login_via_browser",
        lambda no_open=False, on_authorize_url=None: {
            "backend": "https://example.test",
            "access": "acc-123",
            "refresh": "ref-456",
        },
    )
    monkeypatch.setattr(
        cli_mod, "get_current_user_profile", lambda: {"username": "user@example.com"}
    )
    monkeypatch.setattr(cli_mod.cfg, "save_tokens", lambda username, access, refresh: True)
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": "/tmp/mainsequence"},
    )
    monkeypatch.setattr(cli_mod.cfg, "auth_persistence_label", lambda: "secure OS storage")
    monkeypatch.setattr(
        cli_mod.cfg,
        "set_session_overrides",
        lambda **kwargs: session_override.update(kwargs) or kwargs,
    )
    monkeypatch.setattr(
        cli_mod,
        "get_code_repositories",
        lambda: (_ for _ in ()).throw(AssertionError("login should not fetch CodeRepositories")),
    )
    monkeypatch.setenv("MAINSEQUENCE_ENDPOINT", configured_backend)

    result = runner.invoke(
        cli_mod.app,
        ["login", "--no-status"],
    )
    assert result.exit_code == 0
    assert "MAIN SEQUENCE" in result.output
    assert "__  __" in result.output
    assert "Signed in as user@example.com" in result.output
    assert "Auth tokens are persisted in secure OS storage" in result.output
    assert configured_backend in result.output
    assert session_override == {
        "backend_url": configured_backend,
        "mainsequence_path": None,
    }


def test_login_via_mcp_handoff_persists_without_browser(
    cli_mod,
    runner,
    monkeypatch,
):
    configured_backend = "https://dev.example.test"
    handoff_uid = "00000000-0000-4000-8000-000000000001"
    captured: dict[str, object] = {}
    saved: list[tuple[str, str, str]] = []
    saved_backends: list[str] = []

    def _mcp_login(*, timeout_seconds, on_handoff):
        captured["timeout_seconds"] = timeout_seconds
        captured["backend_during_handoff"] = cli_mod.cfg.backend_url()
        on_handoff(
            {
                "mcp_tool": "auth.cli_authorize",
                "mcp_arguments": {"handoff_uid": handoff_uid},
            }
        )
        return {
            "backend": captured["backend_during_handoff"],
            "access": "mcp-access",
            "refresh": "mcp-refresh",
            "user": {"username": "coding-agent@example.com"},
        }

    def _save_tokens(username, access, refresh):
        saved.append((username, access, refresh))
        saved_backends.append(cli_mod.cfg.backend_url())
        return True

    monkeypatch.setattr(cli_mod, "login_via_mcp_handoff", _mcp_login)
    monkeypatch.setattr(
        cli_mod,
        "login_via_browser",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("browser login must not run")),
    )
    monkeypatch.setattr(
        cli_mod.cfg,
        "save_tokens",
        _save_tokens,
    )
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": "/tmp/mainsequence"},
    )
    monkeypatch.setattr(
        cli_mod.cfg,
        "set_session_overrides",
        lambda **kwargs: captured.update({"session": kwargs}),
    )
    monkeypatch.setattr(
        cli_mod.cfg,
        "auth_persistence_label",
        lambda: "local CLI auth storage",
    )
    monkeypatch.setenv("MAINSEQUENCE_ENDPOINT", configured_backend)

    result = runner.invoke(
        cli_mod.app,
        ["login", "--mcp", "--mcp-timeout-seconds", "42"],
    )

    assert result.exit_code == 0, result.output
    assert captured["timeout_seconds"] == 42
    assert captured["backend_during_handoff"] == configured_backend
    assert '"tool":"auth.cli_authorize"' in result.output
    assert handoff_uid in result.output
    assert "mcp-access" not in result.output
    assert "mcp-refresh" not in result.output
    assert saved == [
        ("", "mcp-access", "mcp-refresh"),
        (
            "coding-agent@example.com",
            "mcp-access",
            "mcp-refresh",
        ),
    ]
    assert saved_backends == [configured_backend, configured_backend]
    assert captured["session"] == {
        "backend_url": configured_backend,
        "mainsequence_path": None,
    }
    assert os.environ["MAINSEQUENCE_ENDPOINT"] == configured_backend


def test_login_via_mcp_handoff_explicit_backend_overrides_configured_backend(
    cli_mod,
    runner,
    monkeypatch,
    tmp_path,
):
    configured_backend = "https://configured.example.test"
    explicit_backend = "https://explicit.example.test"
    code_repositories_base = str(tmp_path / "code-repositories")
    captured: dict[str, object] = {}

    def _mcp_login(*, timeout_seconds, on_handoff):
        captured["backend_during_handoff"] = cli_mod.cfg.backend_url()
        return {
            "backend": captured["backend_during_handoff"],
            "access": "mcp-access",
            "refresh": "mcp-refresh",
            "user": {"username": "coding-agent@example.com"},
        }

    monkeypatch.setattr(cli_mod, "login_via_mcp_handoff", _mcp_login)
    monkeypatch.setattr(
        cli_mod,
        "login_via_browser",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("browser login must not run")),
    )
    monkeypatch.setattr(cli_mod.cfg, "save_tokens", lambda username, access, refresh: True)
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {
            "mainsequence_path": code_repositories_base,
            "backend_url": configured_backend,
        },
    )
    monkeypatch.setattr(
        cli_mod.cfg,
        "set_session_overrides",
        lambda **kwargs: captured.update({"session": kwargs}),
    )
    monkeypatch.setattr(
        cli_mod.cfg,
        "auth_persistence_label",
        lambda: "local CLI auth storage",
    )
    monkeypatch.setenv("MAINSEQUENCE_ENDPOINT", configured_backend)

    result = runner.invoke(
        cli_mod.app,
        [
            "login",
            "--mcp",
            "--backend",
            explicit_backend,
            "--code-repositories-base",
            code_repositories_base,
        ],
    )

    assert result.exit_code == 0, result.output
    assert captured["backend_during_handoff"] == explicit_backend
    assert captured["session"] == {
        "backend_url": explicit_backend,
        "mainsequence_path": code_repositories_base,
    }
    assert os.environ["MAINSEQUENCE_ENDPOINT"] == configured_backend


def test_login_via_mcp_handoff_rejects_export(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(
        cli_mod,
        "login_via_mcp_handoff",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("handoff must not start")),
    )

    result = runner.invoke(cli_mod.app, ["login", "--mcp", "--export"])

    assert result.exit_code == 1
    assert "cannot be combined with --export" in result.output


def test_login_via_mcp_handoff_rejects_runtime_credential_mode(cli_mod, runner, monkeypatch):
    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "runtime_credential")
    monkeypatch.setattr(
        cli_mod,
        "login_via_mcp_handoff",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("handoff must not start")),
    )

    result = runner.invoke(cli_mod.app, ["login", "--mcp"])

    assert result.exit_code == 1
    assert "MAINSEQUENCE_AUTH_MODE=runtime_credential" in result.output
    assert "Run `mainsequence login` instead" in result.output


def test_login_via_mcp_handoff_reports_mcp_failure(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(
        cli_mod,
        "login_via_mcp_handoff",
        lambda **_kwargs: (_ for _ in ()).throw(cli_mod.BrowserAuthError("handoff expired")),
    )

    result = runner.invoke(cli_mod.app, ["login", "--mcp"])

    assert result.exit_code == 1
    assert "MCP handoff login failed: handoff expired" in result.output
    assert "Browser login failed" not in result.output


def test_login_with_backend_override(cli_mod, runner, monkeypatch):
    seen = {}
    session_override = {}
    cleared = {"called": False}

    def _browser_login(no_open=False, on_authorize_url=None):
        seen["backend"] = cli_mod.cfg.backend_url()
        return {
            "backend": seen["backend"],
            "access": "acc-123",
            "refresh": "ref-456",
        }

    monkeypatch.setattr(cli_mod, "login_via_browser", _browser_login)
    monkeypatch.setattr(
        cli_mod, "get_current_user_profile", lambda: {"username": "user@example.com"}
    )
    monkeypatch.setattr(cli_mod.cfg, "save_tokens", lambda username, access, refresh: True)
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {
            "mainsequence_path": "/tmp/mainsequence",
            "backend_url": "https://main-sequence.app",
        },
    )
    monkeypatch.setattr(
        cli_mod.cfg,
        "set_session_overrides",
        lambda **kwargs: session_override.update(kwargs) or kwargs,
    )
    monkeypatch.setattr(cli_mod.cfg, "clear_session_overrides", lambda: cleared.update(called=True))
    monkeypatch.setattr(cli_mod.cfg, "auth_persistence_label", lambda: "secure OS storage")
    monkeypatch.setattr(
        cli_mod,
        "get_code_repositories",
        lambda: (_ for _ in ()).throw(AssertionError("login should not fetch CodeRepositories")),
    )
    monkeypatch.delenv("MAINSEQUENCE_ENDPOINT", raising=False)

    result = runner.invoke(
        cli_mod.app,
        ["login", "127.0.0.1:800", "mainsequence-dev", "--no-status"],
    )
    assert result.exit_code == 0
    assert seen["backend"] == "http://127.0.0.1:800"
    assert session_override["backend_url"] == "http://127.0.0.1:800"
    assert session_override["mainsequence_path"] == "mainsequence-dev"
    assert cleared["called"] is False
    assert "http://127.0.0.1:800" in result.output
    assert "MAINSEQUENCE_ENDPOINT" not in os.environ


def test_login_with_different_backend_requires_code_repositories_base(cli_mod, runner, monkeypatch):
    called = {"browser": False}

    def _browser_login(no_open=False, on_authorize_url=None):
        called["browser"] = True
        return {"backend": "http://127.0.0.1:8000", "access": "acc-123", "refresh": "ref-456"}

    monkeypatch.setattr(cli_mod, "login_via_browser", _browser_login)
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {
            "mainsequence_path": "/tmp/mainsequence",
            "backend_url": "https://main-sequence.app",
        },
    )
    monkeypatch.delenv("MAINSEQUENCE_ENDPOINT", raising=False)

    result = runner.invoke(
        cli_mod.app,
        ["login", "127.0.0.1:8000", "--no-status"],
    )
    assert result.exit_code == 1
    assert "must also specify a CodeRepositories base folder" in result.output
    assert called["browser"] is False


def test_login_with_different_backend_allows_current_code_repositories_base(
    cli_mod, runner, monkeypatch
):
    called = {"browser": False}

    def _browser_login(no_open=False, on_authorize_url=None):
        called["browser"] = True
        return {"backend": "http://127.0.0.1:8000", "access": "acc-123", "refresh": "ref-456"}

    monkeypatch.setattr(cli_mod, "login_via_browser", _browser_login)
    monkeypatch.setattr(
        cli_mod, "get_current_user_profile", lambda: {"username": "user@example.com"}
    )
    monkeypatch.setattr(cli_mod.cfg, "save_tokens", lambda username, access, refresh: True)
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {
            "mainsequence_path": "/tmp/mainsequence",
            "backend_url": "https://main-sequence.app",
        },
    )
    monkeypatch.setattr(cli_mod.cfg, "auth_persistence_label", lambda: "local CLI auth storage")
    monkeypatch.setattr(cli_mod.cfg, "set_session_overrides", lambda **kwargs: kwargs)
    monkeypatch.setattr(cli_mod.cfg, "clear_session_overrides", lambda: None)
    monkeypatch.delenv("MAINSEQUENCE_ENDPOINT", raising=False)

    result = runner.invoke(
        cli_mod.app,
        ["login", "127.0.0.1:8000", "/tmp/mainsequence", "--no-status"],
    )
    assert result.exit_code == 0
    assert called["browser"] is True


def test_login_export_env(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(
        cli_mod,
        "login_via_browser",
        lambda no_open=False, on_authorize_url=None: {
            "backend": "https://example.test",
            "access": "acc-123",
            "refresh": "ref-456",
        },
    )
    monkeypatch.setattr(
        cli_mod, "get_current_user_profile", lambda: {"username": "user@example.com"}
    )
    monkeypatch.setattr(cli_mod.cfg, "save_tokens", lambda username, access, refresh: True)
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": "/tmp/mainsequence"},
    )
    monkeypatch.setattr(cli_mod.cfg, "set_session_overrides", lambda **kwargs: kwargs)
    monkeypatch.setattr(
        cli_mod,
        "get_code_repositories",
        lambda: (_ for _ in ()).throw(AssertionError("login should not fetch CodeRepositories")),
    )

    result = runner.invoke(
        cli_mod.app,
        ["login", "--no-status", "--export"],
    )
    assert result.exit_code == 0
    assert 'export MAINSEQUENCE_ACCESS_TOKEN="acc-123"' in result.output
    assert 'export MAINSEQUENCE_REFRESH_TOKEN="ref-456"' in result.output
    assert 'export MAINSEQUENCE_USERNAME="user@example.com"' in result.output


def test_login_with_jwt_tokens(cli_mod, runner, monkeypatch):
    configured_backend = "https://configured.example.test"
    saved = {}
    session_override = {}

    monkeypatch.setattr(
        cli_mod.cfg,
        "save_tokens",
        lambda username, access, refresh: saved.update(
            {"username": username, "access": access, "refresh": refresh}
        )
        or True,
    )
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": "/tmp/mainsequence"},
    )
    monkeypatch.setattr(cli_mod.cfg, "auth_persistence_label", lambda: "local CLI auth storage")
    monkeypatch.setattr(
        cli_mod.cfg,
        "set_session_overrides",
        lambda **kwargs: session_override.update(kwargs) or kwargs,
    )
    monkeypatch.setattr(
        cli_mod,
        "get_code_repositories",
        lambda: (_ for _ in ()).throw(AssertionError("login should not fetch CodeRepositories")),
    )
    monkeypatch.setenv("MAINSEQUENCE_ENDPOINT", configured_backend)

    result = runner.invoke(
        cli_mod.app,
        [
            "login",
            "--access-token",
            "acc-123",
            "--refresh-token",
            "ref-456",
            "--no-status",
        ],
    )
    assert result.exit_code == 0
    assert saved == {"username": "", "access": "acc-123", "refresh": "ref-456"}
    assert "Signed in with JWT tokens" in result.output
    assert "Auth tokens are persisted in local CLI auth storage" in result.output
    assert session_override == {
        "backend_url": configured_backend,
        "mainsequence_path": None,
    }


def test_login_runtime_credential_exchanges_token(cli_mod, runner, monkeypatch):
    configured_backend = "https://configured.example.test"
    session_override = {}
    exchange = {"called": False}
    saved = {}

    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "runtime_credential")
    monkeypatch.setenv("MAINSEQUENCE_ENDPOINT", configured_backend)

    def _exchange_runtime_credential_for_cli_login(backend_url):
        exchange["called"] = True
        exchange["backend_url"] = backend_url
        os.environ["MAINSEQUENCE_ACCESS_TOKEN"] = "runtime-access"
        return "runtime-access"

    monkeypatch.setattr(
        cli_mod,
        "_exchange_runtime_credential_for_cli_login",
        _exchange_runtime_credential_for_cli_login,
    )
    monkeypatch.setattr(
        cli_mod, "login_via_browser", lambda **kwargs: (_ for _ in ()).throw(AssertionError)
    )
    monkeypatch.setattr(
        cli_mod.cfg,
        "save_tokens",
        lambda username, access, refresh: saved.update(
            username=username, access=access, refresh=refresh
        )
        or True,
    )
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": "/tmp/mainsequence"},
    )
    monkeypatch.setattr(cli_mod.cfg, "auth_persistence_label", lambda: "local CLI auth storage")
    monkeypatch.setattr(
        cli_mod.cfg,
        "set_session_overrides",
        lambda **kwargs: session_override.update(kwargs) or kwargs,
    )

    result = runner.invoke(cli_mod.app, ["login"])

    assert result.exit_code == 0
    assert exchange["called"] is True
    assert exchange["backend_url"] == configured_backend
    assert saved == {"username": "", "access": "runtime-access", "refresh": ""}
    assert os.environ["MAINSEQUENCE_ACCESS_TOKEN"] == "runtime-access"
    assert "Signed in with runtime credential" in result.output
    assert "no CLI JWT refresh token exists" in result.output
    assert "re-exchange the runtime credential automatically" in result.output
    assert session_override == {
        "backend_url": configured_backend,
        "mainsequence_path": None,
    }


def test_login_runtime_credential_export(monkeypatch, cli_mod, runner):
    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "runtime_credential")
    monkeypatch.setattr(
        cli_mod,
        "_exchange_runtime_credential_for_cli_login",
        lambda backend_url: "runtime-access",
    )
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": "/tmp/mainsequence"},
    )
    monkeypatch.setattr(cli_mod.cfg, "set_session_overrides", lambda **kwargs: kwargs)

    result = runner.invoke(cli_mod.app, ["login", "--export"])

    assert result.exit_code == 0
    assert 'export MAINSEQUENCE_AUTH_MODE="runtime_credential"' in result.output
    assert 'export MAINSEQUENCE_ACCESS_TOKEN="runtime-access"' in result.output
    assert "MAINSEQUENCE_REFRESH_TOKEN" not in result.output


def test_login_runtime_credential_uses_backend_override(cli_mod, runner, monkeypatch):
    seen = {}
    session_override = {}

    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "runtime_credential")
    monkeypatch.setattr(
        cli_mod,
        "_exchange_runtime_credential_for_cli_login",
        lambda backend_url: seen.update(backend_url=backend_url) or "runtime-access",
    )
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {
            "mainsequence_path": "/tmp/mainsequence",
            "backend_url": "https://main-sequence.app",
        },
    )
    monkeypatch.setattr(
        cli_mod.cfg,
        "set_session_overrides",
        lambda **kwargs: session_override.update(kwargs) or kwargs,
    )
    monkeypatch.setattr(cli_mod.cfg, "auth_persistence_label", lambda: "local CLI auth storage")
    monkeypatch.delenv("MAINSEQUENCE_ENDPOINT", raising=False)

    result = runner.invoke(
        cli_mod.app,
        [
            "login",
            "--backend",
            "http://127.0.0.1:8000",
            "--code-repositories-base",
            "mainsequence-dev",
        ],
    )

    assert result.exit_code == 0
    assert seen["backend_url"] == "http://127.0.0.1:8000"
    assert session_override == {
        "backend_url": "http://127.0.0.1:8000",
        "mainsequence_path": "mainsequence-dev",
    }
    assert "MAINSEQUENCE_ENDPOINT" not in os.environ


def test_login_runtime_credential_rejects_manual_jwt(cli_mod, runner, monkeypatch):
    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "runtime_credential")

    result = runner.invoke(
        cli_mod.app,
        ["login", "--access-token", "acc-123", "--refresh-token", "ref-456"],
    )

    assert result.exit_code == 1
    assert "Runtime credential login cannot be combined" in result.output


def test_api_refresh_access_runtime_credential_reexchange(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "runtime_credential")
    monkeypatch.setattr(api_mod, "backend_url", lambda: "http://127.0.0.1:8000")
    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"username": "", "access": "", "refresh": ""}
    )
    monkeypatch.delenv("MAINSEQUENCE_ACCESS_TOKEN", raising=False)

    saved = {}

    def _save_tokens(username, access, refresh):
        saved["username"] = username
        saved["access"] = access
        saved["refresh"] = refresh
        return True

    monkeypatch.setattr(api_mod, "save_tokens", _save_tokens)

    fake_utils = types.ModuleType("mainsequence.client.utils")

    class _Provider:
        def __init__(self, token_url):
            saved["token_url"] = token_url

        def refresh(self, force=False):
            saved["force"] = force
            os.environ["MAINSEQUENCE_ACCESS_TOKEN"] = "runtime-new-access"

    fake_utils.RuntimeCredentialAuthProvider = _Provider
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)

    out = api_mod.refresh_access()
    assert out == "runtime-new-access"
    assert saved["token_url"] == "http://127.0.0.1:8000/api/v1/runtime-credentials/token/"
    assert saved["force"] is True
    assert saved["access"] == "runtime-new-access"
    assert saved["refresh"] == ""


def test_api_logout_cli_session_revokes_tracked_cli_refresh(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")

    class _Response:
        status_code = 200
        text = ""

        @staticmethod
        def json():
            return {"detail": "CLI refresh token revoked."}

    class _Session:
        @staticmethod
        def post(url, data=None, headers=None):
            assert url == "http://127.0.0.1:8000/auth/cli/revoke/"
            assert json.loads(data) == {"refresh": "ref-123"}
            assert headers is None
            return _Response()

    monkeypatch.setattr(api_mod, "backend_url", lambda: "http://127.0.0.1:8000")
    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"username": "", "access": "acc-123", "refresh": "ref-123"}
    )
    monkeypatch.setattr(api_mod, "S", _Session())

    out = api_mod.logout_cli_session()
    assert out == {
        "attempted": True,
        "revoked": True,
        "method": "cli_revoke",
        "detail": "CLI refresh token revoked.",
    }


def test_api_logout_cli_session_falls_back_to_jwt_logout_on_404(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")

    class _Response:
        status_code = 404
        text = ""

        @staticmethod
        def json():
            return {"detail": "Not found."}

    class _Session:
        @staticmethod
        def post(url, data=None, headers=None):
            assert url == "http://127.0.0.1:8000/auth/cli/revoke/"
            assert json.loads(data) == {"refresh": "ref-123"}
            return _Response()

    monkeypatch.setattr(api_mod, "backend_url", lambda: "http://127.0.0.1:8000")
    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"username": "", "access": "acc-123", "refresh": "ref-123"}
    )
    monkeypatch.setattr(api_mod, "logout_jwt_session", lambda: True)
    monkeypatch.setattr(api_mod, "S", _Session())

    out = api_mod.logout_cli_session()
    assert out == {
        "attempted": True,
        "revoked": True,
        "method": "jwt_logout_fallback",
        "detail": "CLI revoke endpoint unavailable; used JWT logout fallback.",
    }


def test_api_logout_cli_session_skips_backend_revoke_without_refresh(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"username": "", "access": "acc-123", "refresh": ""}
    )

    out = api_mod.logout_cli_session()
    assert out == {
        "attempted": False,
        "revoked": False,
        "method": "local_only",
        "detail": "No CLI browser-login refresh token available.",
    }


def test_login_with_jwt_tokens_and_backend_override(cli_mod, runner, monkeypatch):
    session_override = {}
    monkeypatch.setattr(cli_mod.cfg, "save_tokens", lambda username, access, refresh: True)
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {
            "mainsequence_path": "/tmp/mainsequence",
            "backend_url": "https://main-sequence.app",
        },
    )
    monkeypatch.setattr(
        cli_mod.cfg,
        "set_session_overrides",
        lambda **kwargs: session_override.update(kwargs) or kwargs,
    )
    monkeypatch.setattr(cli_mod.cfg, "auth_persistence_label", lambda: "local CLI auth storage")
    monkeypatch.setattr(
        cli_mod,
        "get_code_repositories",
        lambda: (_ for _ in ()).throw(AssertionError("login should not fetch CodeRepositories")),
    )
    monkeypatch.delenv("MAINSEQUENCE_ENDPOINT", raising=False)

    result = runner.invoke(
        cli_mod.app,
        [
            "login",
            "--access-token",
            "acc-123",
            "--refresh-token",
            "ref-456",
            "--backend",
            "http://127.0.0.1:80",
            "--code-repositories-base",
            "mainsequence-dev",
            "--no-status",
        ],
    )
    assert result.exit_code == 0
    assert session_override == {
        "backend_url": "http://127.0.0.1:80",
        "mainsequence_path": "mainsequence-dev",
    }
    assert "http://127.0.0.1:80" in result.output
    assert "MAINSEQUENCE_ENDPOINT" not in os.environ


def test_login_with_jwt_tokens_and_different_backend_requires_code_repositories_base(
    cli_mod, runner, monkeypatch
):
    called = {"save_tokens": False}

    def _save_tokens(username, access, refresh):
        called["save_tokens"] = True
        return True

    monkeypatch.setattr(cli_mod.cfg, "save_tokens", _save_tokens)
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {
            "mainsequence_path": "/tmp/mainsequence",
            "backend_url": "https://main-sequence.app",
        },
    )
    monkeypatch.delenv("MAINSEQUENCE_ENDPOINT", raising=False)

    result = runner.invoke(
        cli_mod.app,
        [
            "login",
            "--access-token",
            "acc-123",
            "--refresh-token",
            "ref-456",
            "--backend",
            "127.0.0.1:8000",
            "--no-status",
        ],
    )
    assert result.exit_code == 1
    assert "must also specify a CodeRepositories base folder" in result.output
    assert called["save_tokens"] is False


def test_login_export_env_with_jwt_tokens_omits_username(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod.cfg, "save_tokens", lambda username, access, refresh: True)
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": "/tmp/mainsequence"},
    )
    monkeypatch.setattr(cli_mod.cfg, "set_session_overrides", lambda **kwargs: kwargs)

    result = runner.invoke(
        cli_mod.app,
        [
            "login",
            "--access-token",
            "acc-123",
            "--refresh-token",
            "ref-456",
            "--export",
            "--no-status",
        ],
    )
    assert result.exit_code == 0
    assert 'export MAINSEQUENCE_ACCESS_TOKEN="acc-123"' in result.output
    assert 'export MAINSEQUENCE_REFRESH_TOKEN="ref-456"' in result.output
    assert "MAINSEQUENCE_USERNAME" not in result.output


def test_login_warns_when_secure_persist_fails(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(
        cli_mod,
        "login_via_browser",
        lambda no_open=False, on_authorize_url=None: {
            "backend": "https://example.test",
            "access": "acc-123",
            "refresh": "ref-456",
        },
    )
    monkeypatch.setattr(cli_mod, "get_current_user_profile", lambda: {})
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": "/tmp/mainsequence"},
    )
    monkeypatch.setattr(cli_mod.cfg, "save_tokens", lambda username, access, refresh: False)
    monkeypatch.setattr(cli_mod.cfg, "auth_persistence_label", lambda: "secure OS storage")
    monkeypatch.setattr(cli_mod.cfg, "set_session_overrides", lambda **kwargs: kwargs)
    monkeypatch.setattr(
        cli_mod,
        "get_code_repositories",
        lambda: (_ for _ in ()).throw(AssertionError("login should not fetch CodeRepositories")),
    )

    result = runner.invoke(
        cli_mod.app,
        ["login", "--no-status"],
    )
    assert result.exit_code == 0
    assert "Could not persist auth tokens in secure OS storage" in result.output


def test_login_does_not_fetch_code_repositories_after_success(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(
        cli_mod,
        "login_via_browser",
        lambda no_open=False, on_authorize_url=None: {
            "backend": "https://example.test",
            "access": "acc-123",
            "refresh": "ref-456",
        },
    )
    monkeypatch.setattr(
        cli_mod, "get_current_user_profile", lambda: {"username": "user@example.com"}
    )
    monkeypatch.setattr(cli_mod.cfg, "save_tokens", lambda username, access, refresh: True)
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": "/tmp/mainsequence"},
    )
    monkeypatch.setattr(cli_mod.cfg, "auth_persistence_label", lambda: "local CLI auth storage")
    monkeypatch.setattr(cli_mod.cfg, "set_session_overrides", lambda **kwargs: kwargs)
    monkeypatch.setattr(
        cli_mod,
        "get_code_repositories",
        lambda: (_ for _ in ()).throw(AssertionError("login should not fetch CodeRepositories")),
    )

    result = runner.invoke(
        cli_mod.app,
        ["login"],
    )
    assert result.exit_code == 0
    assert "Signed in as user@example.com" in result.output
    assert "CodeRepositories:" not in result.output


def test_jwt_login_does_not_fetch_code_repositories_after_success(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod.cfg, "save_tokens", lambda username, access, refresh: True)
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": "/tmp/mainsequence"},
    )
    monkeypatch.setattr(cli_mod.cfg, "auth_persistence_label", lambda: "local CLI auth storage")
    monkeypatch.setattr(cli_mod.cfg, "set_session_overrides", lambda **kwargs: kwargs)
    monkeypatch.setattr(
        cli_mod,
        "get_code_repositories",
        lambda: (_ for _ in ()).throw(AssertionError("login should not fetch CodeRepositories")),
    )

    result = runner.invoke(
        cli_mod.app,
        ["login", "--access-token", "acc-123", "--refresh-token", "ref-456"],
    )
    assert result.exit_code == 0
    assert "Signed in with JWT tokens" in result.output
    assert "CodeRepositories:" not in result.output


def test_logout_export_env(cli_mod, runner, monkeypatch):
    cleared = {"called": False}
    monkeypatch.setattr(
        cli_mod,
        "logout_cli_session",
        lambda: {
            "attempted": True,
            "revoked": True,
            "method": "cli_revoke",
            "detail": "CLI refresh token revoked.",
        },
    )
    monkeypatch.setattr(cli_mod.cfg, "clear_tokens", lambda: True)
    monkeypatch.setattr(cli_mod.cfg, "clear_session_overrides", lambda: cleared.update(called=True))
    result = runner.invoke(cli_mod.app, ["logout", "--export"])
    assert result.exit_code == 0
    assert "unset MAINSEQUENCE_ACCESS_TOKEN" in result.output
    assert "unset MAINSEQUENCE_REFRESH_TOKEN" in result.output
    assert "unset MAINSEQUENCE_USERNAME" in result.output
    assert cleared["called"] is True


def test_logout_warns_when_backend_revoke_cannot_be_confirmed(cli_mod, runner, monkeypatch):
    cleared = {"called": False}
    monkeypatch.setattr(
        cli_mod,
        "logout_cli_session",
        lambda: {
            "attempted": True,
            "revoked": False,
            "method": "error",
            "detail": "CLI revoke failed with status 400.",
        },
    )
    monkeypatch.setattr(cli_mod.cfg, "clear_tokens", lambda: True)
    monkeypatch.setattr(cli_mod.cfg, "clear_session_overrides", lambda: cleared.update(called=True))
    result = runner.invoke(cli_mod.app, ["logout"])
    assert result.exit_code == 0
    assert "Signed out locally, but backend session revoke could not be confirmed." in result.output
    assert "CLI revoke failed with status 400." in result.output
    assert cleared["called"] is True


def test_login_rejects_legacy_email_argument(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": "/tmp/mainsequence"},
    )
    result = runner.invoke(cli_mod.app, ["login", "user@example.com"])
    assert result.exit_code == 1
    assert "Email/password CLI login was removed" in result.output


def test_login_no_open_prints_authorize_url(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": "/tmp/mainsequence"},
    )
    monkeypatch.setattr(cli_mod.cfg, "auth_persistence_label", lambda: "local CLI auth storage")
    monkeypatch.setattr(cli_mod.cfg, "set_session_overrides", lambda **kwargs: kwargs)
    monkeypatch.setattr(cli_mod.cfg, "save_tokens", lambda username, access, refresh: True)
    monkeypatch.setattr(
        cli_mod, "get_current_user_profile", lambda: {"username": "user@example.com"}
    )

    def _browser_login(no_open=False, on_authorize_url=None):
        assert no_open is True
        assert on_authorize_url is not None
        on_authorize_url("https://example.test/auth")
        return {"backend": "https://example.test", "access": "acc-123", "refresh": "ref-456"}

    monkeypatch.setattr(cli_mod, "login_via_browser", _browser_login)

    result = runner.invoke(cli_mod.app, ["login", "--no-open", "--no-status"])
    assert result.exit_code == 0
    assert "Open this URL to authenticate: https://example.test/auth" in result.output


def test_config_get_tokens_fallback_secure_store(cli_mod, monkeypatch):
    monkeypatch.delenv(cli_mod.cfg.ENV_ACCESS, raising=False)
    monkeypatch.delenv(cli_mod.cfg.ENV_REFRESH, raising=False)
    monkeypatch.delenv(cli_mod.cfg.ENV_USERNAME, raising=False)
    monkeypatch.setattr(cli_mod.cfg, "_read_local_tokens", lambda: {})
    monkeypatch.setattr(
        cli_mod.cfg,
        "_read_secure_tokens",
        lambda: {"username": "u@example.com", "access": "acc", "refresh": "ref"},
    )
    out = cli_mod.cfg.get_tokens()
    assert out["username"] == "u@example.com"
    assert out["access"] == "acc"
    assert out["refresh"] == "ref"


def test_config_get_tokens_fallback_legacy_env(cli_mod, monkeypatch):
    monkeypatch.delenv(cli_mod.cfg.ENV_ACCESS, raising=False)
    monkeypatch.delenv(cli_mod.cfg.ENV_REFRESH, raising=False)
    monkeypatch.delenv(cli_mod.cfg.ENV_USERNAME, raising=False)
    monkeypatch.setenv(cli_mod.cfg.LEGACY_ENV_ACCESS, "legacy-acc")
    monkeypatch.setenv(cli_mod.cfg.LEGACY_ENV_REFRESH, "legacy-ref")
    monkeypatch.setenv(cli_mod.cfg.LEGACY_ENV_USERNAME, "legacy@example.com")
    out = cli_mod.cfg.get_tokens()
    assert out["username"] == "legacy@example.com"
    assert out["access"] == "legacy-acc"
    assert out["refresh"] == "legacy-ref"


def test_config_get_tokens_prefers_env_over_local_store(cli_mod, monkeypatch, tmp_path):
    auth_json = tmp_path / "auth.json"
    cli_mod.cfg.write_json(
        auth_json,
        {"username": "file@example.com", "access": "file-acc", "refresh": "file-ref"},
    )
    monkeypatch.setattr(cli_mod.cfg, "AUTH_JSON", auth_json)
    monkeypatch.setattr(cli_mod.cfg, "_read_secure_tokens", lambda: {})
    monkeypatch.setenv(cli_mod.cfg.ENV_USERNAME, "env@example.com")
    monkeypatch.setenv(cli_mod.cfg.ENV_ACCESS, "env-acc")
    monkeypatch.setenv(cli_mod.cfg.ENV_REFRESH, "env-ref")

    out = cli_mod.cfg.get_tokens()
    assert out["username"] == "env@example.com"
    assert out["access"] == "env-acc"
    assert out["refresh"] == "env-ref"


def test_prime_runtime_env_reads_endpoint_but_ignores_unsupported_repository_identity(
    cli_mod, monkeypatch, tmp_path
):
    bootstrap = importlib.import_module("mainsequence.bootstrap")
    code_repository_dir = tmp_path / "code-repository"
    code_repository_dir.mkdir(parents=True, exist_ok=True)
    (code_repository_dir / ".env").write_text(
        "MAINSEQUENCE_ENDPOINT=https://sdk-backend.test\n"
        f"{_UNSUPPORTED_REPOSITORY_UID_ENV}=code-repository-uid-123\n",
        encoding="utf-8",
    )

    monkeypatch.chdir(code_repository_dir)
    monkeypatch.setattr(cli_mod.cfg, "backend_url", lambda: "https://session-backend.test")
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_tokens",
        lambda: {"username": "user@example.com", "access": "acc-123", "refresh": "ref-456"},
    )
    for key in (
        "MAINSEQUENCE_ENDPOINT",
        _UNSUPPORTED_REPOSITORY_UID_ENV,
        _UNSUPPORTED_REPOSITORY_NUMERIC_ID_ENV,
        "MAINSEQUENCE_ACCESS_TOKEN",
        "MAINSEQUENCE_REFRESH_TOKEN",
    ):
        monkeypatch.delenv(key, raising=False)

    bootstrap.prime_runtime_env()

    assert os.environ["MAINSEQUENCE_ENDPOINT"] == "https://sdk-backend.test"
    assert _UNSUPPORTED_REPOSITORY_UID_ENV not in os.environ
    assert _UNSUPPORTED_REPOSITORY_NUMERIC_ID_ENV not in os.environ
    assert os.environ["MAINSEQUENCE_ACCESS_TOKEN"] == "acc-123"
    assert os.environ["MAINSEQUENCE_REFRESH_TOKEN"] == "ref-456"


def test_prime_runtime_env_falls_back_to_cli_login_context(cli_mod, monkeypatch, tmp_path):
    bootstrap = importlib.import_module("mainsequence.bootstrap")
    code_repository_dir = tmp_path / "code-repository"
    code_repository_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.chdir(code_repository_dir)
    monkeypatch.setattr(cli_mod.cfg, "backend_url", lambda: "http://127.0.0.1:8000")
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_tokens",
        lambda: {"username": "user@example.com", "access": "acc-123", "refresh": "ref-456"},
    )
    for key in (
        "MAINSEQUENCE_ENDPOINT",
        _UNSUPPORTED_REPOSITORY_UID_ENV,
        _UNSUPPORTED_REPOSITORY_NUMERIC_ID_ENV,
        "MAINSEQUENCE_ACCESS_TOKEN",
        "MAINSEQUENCE_REFRESH_TOKEN",
    ):
        monkeypatch.delenv(key, raising=False)

    bootstrap.prime_runtime_env()

    assert os.environ["MAINSEQUENCE_ENDPOINT"] == "http://127.0.0.1:8000"
    assert _UNSUPPORTED_REPOSITORY_UID_ENV not in os.environ
    assert _UNSUPPORTED_REPOSITORY_NUMERIC_ID_ENV not in os.environ
    assert os.environ["MAINSEQUENCE_ACCESS_TOKEN"] == "acc-123"
    assert os.environ["MAINSEQUENCE_REFRESH_TOKEN"] == "ref-456"


def test_config_clear_tokens_removes_local_store(cli_mod, monkeypatch, tmp_path):
    auth_json = tmp_path / "auth.json"
    cli_mod.cfg.write_json(
        auth_json,
        {"username": "u@example.com", "access": "acc", "refresh": "ref"},
    )
    monkeypatch.setattr(cli_mod.cfg, "AUTH_JSON", auth_json)
    monkeypatch.setattr(cli_mod.cfg, "_clear_secure_tokens", lambda: True)
    monkeypatch.setenv(cli_mod.cfg.ENV_USERNAME, "u@example.com")
    monkeypatch.setenv(cli_mod.cfg.ENV_ACCESS, "acc")
    monkeypatch.setenv(cli_mod.cfg.ENV_REFRESH, "ref")

    ok = cli_mod.cfg.clear_tokens()

    assert ok is True
    assert not auth_json.exists()
    assert os.environ.get(cli_mod.cfg.ENV_USERNAME) is None
    assert os.environ.get(cli_mod.cfg.ENV_ACCESS) is None
    assert os.environ.get(cli_mod.cfg.ENV_REFRESH) is None


def test_get_current_user_profile_uses_user_details_endpoint(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    seen = {}

    class _Response:
        def __init__(self, payload):
            self.ok = True
            self._payload = payload

        def json(self):
            return self._payload

    def _authed(method, path, body=None):
        seen["method"] = method
        seen["path"] = path
        return _Response(
            {
                "user": {
                    "uid": "user-uid-123",
                    "username": "jose@main-sequence.io",
                    "organization": {"uid": "org-uid-456", "name": "Main Sequence"},
                }
            }
        )

    monkeypatch.setattr(api_mod, "authed", _authed)

    out = api_mod.get_current_user_profile()
    assert seen == {"method": "GET", "path": "/api/v1/users/me/"}
    assert out == {"username": "jose@main-sequence.io", "organization": "Main Sequence"}


def test_get_current_user_profile_accepts_top_level_organization_object(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")

    class _Response:
        ok = True

        @staticmethod
        def json():
            return {
                "username": "jose@main-sequence.io",
                "organization": {"uid": "org-uid-456", "name": "Main Sequence Dev"},
            }

    monkeypatch.setattr(api_mod, "authed", lambda method, path, body=None: _Response())

    out = api_mod.get_current_user_profile()
    assert out == {"username": "jose@main-sequence.io", "organization": "Main Sequence Dev"}


def test_get_logged_user_details_uses_canonical_authenticated_user_method(
    cli_mod, runner, monkeypatch
):
    from mainsequence.client.models_user import User

    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}
    user = User.model_validate(
        {
            "id": 7,
            "uid": USER_UID,
            "username": "jose",
            "email": "jose@main-sequence.io",
            "date_joined": "2026-01-01T00:00:00Z",
            "is_active": True,
            "api_request_limit": 10000,
            "mfa_enabled": False,
            "active_team_uids": [TEAM_UID],
            "organization": {
                "id": 2,
                "uid": "00000000-0000-4000-8000-000000000003",
                "name": "Main Sequence",
                "organization_domain": "main-sequence.io",
                "production_environment_uid": "00000000-0000-4000-8000-000000000002",
            },
        }
    )
    assert isinstance(user.model_dump()["active_team_uids"][0], UUID)

    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"}
    )
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

    def _reject_legacy_request(*args, **kwargs):
        raise AssertionError("get_logged_user_details must not call a legacy auth endpoint")

    monkeypatch.setattr(api_mod, "authed", _reject_legacy_request)

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
    fake_models_user = types.ModuleType("mainsequence.client.models_user")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.MAINSEQUENCE_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/api/v1"
    fake_utils.AUTH_ENDPOINT = "https://old.test"

    def _set_mainsequence_endpoint(endpoint):
        normalized = endpoint.rstrip("/")
        fake_utils.MAINSEQUENCE_ENDPOINT = normalized
        fake_utils.API_ENDPOINT = f"{normalized}/api/v1"
        fake_utils.AUTH_ENDPOINT = normalized
        captured["endpoint"] = normalized

    fake_utils.set_mainsequence_endpoint = _set_mainsequence_endpoint

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/api/v1"

    class FakeUser:
        ROOT_URL = "https://old.test/api/v1/users"

        @classmethod
        def get_authenticated_user_details(cls):
            captured["current_user_url"] = f"{cls.ROOT_URL}/users/me/"
            return user

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models_user.User = FakeUser
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_user", fake_models_user)

    out = api_mod.get_logged_user_details()
    assert captured["endpoint"] == "https://backend.test"
    assert fake_utils.MAINSEQUENCE_ENDPOINT == "https://backend.test"
    assert fake_utils.API_ENDPOINT == "https://backend.test/api/v1"
    assert fake_utils.AUTH_ENDPOINT == "https://backend.test"
    assert captured["jwt"] == ("acc", "ref")
    assert captured["current_user_url"] == "https://backend.test/api/v1/users/me/"
    assert "id" not in out
    assert "id" not in out["organization"]
    assert out["uid"] == USER_UID
    assert out["username"] == "jose"
    assert out["active_team_uids"] == [TEAM_UID]
    assert json.loads(json.dumps(out))["uid"] == USER_UID
    assert (
        out["organization"]["production_environment_uid"] == "00000000-0000-4000-8000-000000000002"
    )

    normal = runner.invoke(cli_mod.app, ["user"])
    structured = runner.invoke(cli_mod.app, ["user", "--json"])

    assert normal.exit_code == structured.exit_code == 0
    assert USER_UID in normal.output
    assert "Main Sequence" in normal.output
    payload = json.loads(structured.output)
    assert payload["uid"] == out["uid"]
    assert payload["organization"]["uid"] == out["organization"]["uid"]
    assert payload["active_team_uids"] == [TEAM_UID]
    assert "id" not in payload
    assert "id" not in payload["organization"]
    assert "user_permissions" not in payload
    assert "orm_class" not in payload
    assert '"acc"' not in structured.output
    assert '"ref"' not in structured.output


def test_refresh_token_renews_the_saved_session(cli_mod, runner, monkeypatch, tmp_path):
    renewals = []
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cli_mod, "refresh_access", lambda: renewals.append(1) or "new-access")
    monkeypatch.setattr(cli_mod.cfg, "session_report", _session_report)

    result = runner.invoke(cli_mod.app, ["refresh-token"])

    assert result.exit_code == 0, result.output
    assert renewals == [1]
    assert "Session renewed for u@example.com on https://backend.test" in result.output
    assert "valid until 2030-03-17 17:46 UTC" in result.output
    assert "new-access" not in result.output
    # It works anywhere: no checkout, no `.env`, and none is created.
    assert list(tmp_path.iterdir()) == []


def test_refresh_token_is_also_reachable_with_an_underscore(cli_mod, runner, monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cli_mod, "refresh_access", lambda: "new-access")
    monkeypatch.setattr(cli_mod.cfg, "session_report", _session_report)

    result = runner.invoke(cli_mod.app, ["refresh_token"])

    assert result.exit_code == 0, result.output
    assert "Session renewed" in result.output
    assert "refresh_token" not in runner.invoke(cli_mod.app, ["--help"]).output


def test_refresh_token_removes_credentials_left_in_the_env_file(
    cli_mod, runner, monkeypatch, tmp_path
):
    env_path = tmp_path / ".env"
    env_path.write_text(
        "FOO=bar\n"
        f"{_UNSUPPORTED_REPOSITORY_UID_ENV}=code-repository-uid-123\n"
        "MAINSEQUENCE_ACCESS_TOKEN=old-access\n"
        "export MAINSEQUENCE_REFRESH_TOKEN=old-refresh\n"
        "MAINSEQUENCE_ENDPOINT=https://other-backend.test\n"
        "MAINSEQUENCE_TOKEN=legacy-token\n"
        "TAU_LOCAL_MODE=true\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cli_mod, "refresh_access", lambda: "new-access")
    monkeypatch.setattr(cli_mod.cfg, "session_report", _session_report)

    result = runner.invoke(cli_mod.app, ["refresh-token"])

    assert result.exit_code == 0, result.output
    # Only the credential entries go. Every other line stays as it was, the
    # endpoint included: the command does not manage the checkout.
    assert env_path.read_text(encoding="utf-8") == (
        "FOO=bar\n"
        f"{_UNSUPPORTED_REPOSITORY_UID_ENV}=code-repository-uid-123\n"
        "MAINSEQUENCE_ENDPOINT=https://other-backend.test\n"
        "TAU_LOCAL_MODE=true\n"
    )
    assert "MAINSEQUENCE_ACCESS_TOKEN, MAINSEQUENCE_REFRESH_TOKEN, MAINSEQUENCE_TOKEN" in (
        result.output
    )
    for secret in ("old-access", "old-refresh", "legacy-token", "new-access"):
        assert secret not in result.output


def test_refresh_token_removes_the_runtime_credential_block(cli_mod, runner, monkeypatch, tmp_path):
    env_path = tmp_path / ".env"
    env_path.write_text(
        "FOO=bar\n"
        "MAINSEQUENCE_AUTH_MODE=runtime_credential\n"
        "MAINSEQUENCE_ACCESS_TOKEN=old-access\n"
        "MAINSEQUENCE_RUNTIME_CREDENTIAL_ID=old-cred-id\n"
        "MAINSEQUENCE_RUNTIME_CREDENTIAL_SECRET=old-cred-secret\n"
        "MAINSEQUENCE_ENDPOINT=https://backend.test\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "runtime_credential")
    monkeypatch.setattr(cli_mod, "refresh_access", lambda: "exchanged-access")
    monkeypatch.setattr(
        cli_mod.cfg,
        "session_report",
        lambda: _session_report(auth_mode="runtime_credential", username=None, source=None),
    )

    result = runner.invoke(cli_mod.app, ["refresh-token"])

    assert result.exit_code == 0, result.output
    assert env_path.read_text(encoding="utf-8") == (
        "FOO=bar\nMAINSEQUENCE_ENDPOINT=https://backend.test\n"
    )
    assert "Session renewed on https://backend.test" in result.output
    for secret in ("old-access", "old-cred-secret", "exchanged-access"):
        assert secret not in result.output


def test_refresh_token_without_a_session_asks_for_a_login(cli_mod, runner, monkeypatch, tmp_path):
    env_path = tmp_path / ".env"
    env_path.write_text("FOO=bar\nMAINSEQUENCE_REFRESH_TOKEN=old-refresh\n", encoding="utf-8")

    def no_session():
        raise cli_mod.NotLoggedIn("Not logged in. Run `mainsequence login`.")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cli_mod, "refresh_access", no_session)
    monkeypatch.setattr(cli_mod.cfg, "secure_store_available", lambda: True)
    monkeypatch.setattr(cli_mod.cfg, "store_read_error", lambda: None)

    result = runner.invoke(cli_mod.app, ["refresh-token"])

    assert result.exit_code == cli_mod.AUTH_EXIT_NOT_LOGGED_IN == 1
    assert "Not logged in. Run: mainsequence login" in result.output
    # The stale entry is removed either way: it would hide the session a login saves.
    assert env_path.read_text(encoding="utf-8") == "FOO=bar\n"
    assert "old-refresh" not in result.output


def test_refresh_token_does_not_echo_a_transport_failure(cli_mod, runner, monkeypatch, tmp_path):
    def unreachable():
        raise ConnectionError("https://backend.test/auth/?refresh=refresh-value")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cli_mod, "refresh_access", unreachable)

    result = runner.invoke(cli_mod.app, ["refresh-token"])

    assert result.exit_code == 1
    assert "The session could not be renewed (ConnectionError)." in result.output
    assert "refresh-value" not in result.output
    assert "Traceback" not in result.output


def test_refresh_token_json_is_the_session_report(cli_mod, runner, monkeypatch, tmp_path):
    (tmp_path / ".env").write_text("MAINSEQUENCE_ACCESS_TOKEN=old-access\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cli_mod, "refresh_access", lambda: "new-access")
    monkeypatch.setattr(cli_mod.cfg, "session_report", _session_report)

    result = runner.invoke(cli_mod.app, ["refresh-token", "--json"])

    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == {
        **_session_report(),
        "removed_env_entries": ["MAINSEQUENCE_ACCESS_TOKEN"],
    }


def test_the_per_checkout_refresh_token_command_is_gone(cli_mod, runner):
    result = runner.invoke(cli_mod.app, ["code-repository", "refresh-token", "--path", "."])

    assert result.exit_code == 2
    assert "No such command" in result.output


@pytest.mark.live
def test_login_live_with_env_tokens(cli_mod, runner, monkeypatch):
    """
    Optional live JWT import check.

    Set:
      - MAINSEQUENCE_TEST_ACCESS_TOKEN
      - MAINSEQUENCE_TEST_REFRESH_TOKEN
    """
    access_token = os.getenv("MAINSEQUENCE_TEST_ACCESS_TOKEN")
    refresh_token = os.getenv("MAINSEQUENCE_TEST_REFRESH_TOKEN")
    if not access_token or not refresh_token:
        pytest.skip("Missing MAINSEQUENCE_TEST_ACCESS_TOKEN / MAINSEQUENCE_TEST_REFRESH_TOKEN")

    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": "/tmp/mainsequence"},
    )
    result = runner.invoke(
        cli_mod.app,
        [
            "login",
            "--access-token",
            access_token,
            "--refresh-token",
            refresh_token,
            "--no-status",
        ],
    )
    assert result.exit_code == 0
