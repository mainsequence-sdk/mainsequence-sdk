"""Thin SDK CLI behavior."""

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


def test_cli_exposes_only_adapter_commands():
    result = runner.invoke(cli_mod.app, ["--help"])
    assert result.exit_code == 0
    for command in ("login", "logout", "settings", "version", "doctor"):
        assert command in result.output


def test_manual_jwt_login_persists_tokens(monkeypatch):
    saved = {}
    monkeypatch.setattr(cli_mod.cfg, "backend_url", lambda: "https://backend.example")
    monkeypatch.setattr(
        cli_mod.cfg,
        "save_tokens",
        lambda username, access, refresh: saved.update(
            username=username, access=access, refresh=refresh
        )
        or True,
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


def test_doctor_does_not_probe_network_unless_requested(monkeypatch):
    monkeypatch.setattr(cli_mod.cfg, "backend_url", lambda: "https://backend.example")
    monkeypatch.setattr(cli_mod.cfg, "get_tokens", lambda: {"access": "test", "refresh": ""})
    monkeypatch.setattr(cli_mod.cfg, "auth_persistence_label", lambda: "test store")
    monkeypatch.setattr(
        cli_mod.requests,
        "get",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("unexpected network request")
        ),
    )

    result = runner.invoke(cli_mod.app, ["doctor"])

    assert result.exit_code == 0
    assert json.loads(result.output)["authenticated"] is True


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
    monkeypatch.setattr(cli_mod, "_exchange_runtime_credential", lambda backend: "runtime-access")
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
