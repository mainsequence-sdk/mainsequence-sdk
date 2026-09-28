"""Small command-line surface for SDK authentication and endpoint configuration."""

from __future__ import annotations

import importlib.metadata
import json
import os

import requests
import typer

from . import config as cfg
from .api import ApiError, logout_cli_session
from .browser_auth import BrowserAuthError, login_via_browser, login_via_mcp_handoff
from .ui import error, success, warn

app = typer.Typer(help="Main Sequence platform SDK", no_args_is_help=True)
settings = typer.Typer(help="Backend endpoint settings", no_args_is_help=True)
app.add_typer(settings, name="settings")


def _runtime_credential_mode_enabled() -> bool:
    return (os.environ.get("MAINSEQUENCE_AUTH_MODE") or "").strip().lower() == "runtime_credential"


def _exchange_runtime_credential(backend: str) -> str:
    from mainsequence.client.utils import RuntimeCredentialAuthProvider

    provider = RuntimeCredentialAuthProvider(
        token_url=f"{backend.rstrip('/')}/api/v1/runtime-credentials/token/"
    )
    provider.refresh(force=True)
    access = (os.environ.get("MAINSEQUENCE_ACCESS_TOKEN") or "").strip()
    if not access:
        raise ApiError("Runtime credential exchange returned no access token.")
    return access


@app.command()
def login(
    backend: str | None = typer.Option(None, "--backend", help="Backend URL or host[:port]."),
    access_token: str | None = typer.Option(None, "--access-token", help="JWT access token."),
    refresh_token: str | None = typer.Option(None, "--refresh-token", help="JWT refresh token."),
    no_open: bool = typer.Option(False, "--no-open", help="Print the browser authorization URL."),
    mcp: bool = typer.Option(False, "--mcp", help="Authorize through an existing MCP session."),
    mcp_timeout_seconds: int = typer.Option(300, "--mcp-timeout-seconds", min=1),
    export: bool = typer.Option(False, "--export", help="Print shell auth exports."),
) -> None:
    """Authenticate and store a platform session."""
    manual = access_token is not None or refresh_token is not None
    runtime = _runtime_credential_mode_enabled()
    if sum((manual, mcp, runtime)) > 1:
        raise typer.BadParameter("Choose only one of JWT tokens, MCP handoff, or runtime credentials.")
    if manual and (not (access_token or "").strip() or not (refresh_token or "").strip()):
        raise typer.BadParameter("JWT login requires both --access-token and --refresh-token.")
    if mcp and export:
        raise typer.BadParameter("--mcp and --export cannot be combined.")

    selected_backend = cfg.normalize_backend_url(backend or cfg.backend_url())
    previous_backend = os.environ.get("MAINSEQUENCE_ENDPOINT")
    os.environ["MAINSEQUENCE_ENDPOINT"] = selected_backend
    try:
        if runtime:
            access, refresh = _exchange_runtime_credential(selected_backend), ""
        elif manual:
            access, refresh = (access_token or "").strip(), (refresh_token or "").strip()
        elif mcp:
            flow = login_via_mcp_handoff(
                timeout_seconds=mcp_timeout_seconds,
                on_handoff=lambda handoff: typer.echo(
                    json.dumps({"tool": handoff["mcp_tool"], "arguments": handoff["mcp_arguments"]})
                ),
            )
            access, refresh = flow["access"], flow["refresh"]
        else:
            flow = login_via_browser(
                no_open=no_open,
                on_authorize_url=lambda url: typer.echo(f"Open this URL to authenticate: {url}"),
            )
            access, refresh = flow["access"], flow["refresh"]
        persisted = cfg.save_tokens("", access, refresh)
    except (ApiError, BrowserAuthError, requests.RequestException, ValueError) as exc:
        error(f"Login failed: {exc}")
        raise typer.Exit(1) from exc
    finally:
        if previous_backend is None:
            os.environ.pop("MAINSEQUENCE_ENDPOINT", None)
        else:
            os.environ["MAINSEQUENCE_ENDPOINT"] = previous_backend

    if backend is not None:
        cfg.set_session_overrides(backend_url=selected_backend)
    if export:
        typer.echo(f"export MAINSEQUENCE_ACCESS_TOKEN={json.dumps(access)}")
        if refresh:
            typer.echo(f"export MAINSEQUENCE_REFRESH_TOKEN={json.dumps(refresh)}")
        return
    success(f"Signed in to {selected_backend}.")
    if not persisted:
        warn("Tokens could not be persisted; use --export for shell-managed authentication.")


@app.command()
def logout(
    export: bool = typer.Option(False, "--export", help="Print shell auth unsets."),
) -> None:
    """Revoke the CLI session when possible and clear local tokens."""
    try:
        revoke = logout_cli_session()
    except Exception as exc:
        revoke = {"attempted": True, "revoked": False, "detail": str(exc)}
    cleared = cfg.clear_tokens()
    cfg.clear_session_overrides()
    if export:
        for name in (
            "MAINSEQUENCE_ACCESS_TOKEN", "MAINSEQUENCE_REFRESH_TOKEN", "MAINSEQUENCE_USERNAME",
            "MAIN_SEQUENCE_USER_TOKEN", "MAIN_SEQUENCE_REFRESH_TOKEN", "MAIN_SEQUENCE_USERNAME",
        ):
            typer.echo(f"unset {name}")
        return
    if not cleared or (revoke.get("attempted") and not revoke.get("revoked")):
        warn("Local session cleared; backend revocation or persistent-store cleanup was not confirmed.")
    else:
        success("Signed out.")


@app.command()
def version() -> None:
    """Show the installed SDK version."""
    try:
        typer.echo(importlib.metadata.version("mainsequence"))
    except importlib.metadata.PackageNotFoundError:
        typer.echo("unknown")


@app.command()
def doctor(
    check_connection: bool = typer.Option(False, "--check-connection", help="Probe the backend URL."),
) -> None:
    """Show endpoint, authentication, and optional connection diagnostics."""
    tokens = cfg.get_tokens()
    report: dict[str, object] = {
        "backend_url": cfg.backend_url(),
        "config_file": str(cfg.CONFIG_JSON),
        "auth_storage": cfg.auth_persistence_label(),
        "authenticated": bool(tokens.get("access")),
        "refresh_available": bool(tokens.get("refresh")),
    }
    if check_connection:
        try:
            response = requests.get(cfg.backend_url(), timeout=5)
            report["connection_status"] = response.status_code
        except requests.RequestException as exc:
            report["connection_error"] = str(exc)
    typer.echo(json.dumps(report, indent=2))


@settings.command("show")
def settings_show() -> None:
    """Show the current backend endpoint."""
    typer.echo(json.dumps({"backend_url": cfg.backend_url()}, indent=2))


@settings.command("set-backend")
def settings_set_backend(url: str) -> None:
    """Persist the backend endpoint."""
    value = cfg.set_backend_url(url)
    success(f"Backend URL set to: {value['backend_url']}")


@settings.command("reset")
def settings_reset() -> None:
    """Restore the standard backend endpoint."""
    cfg.clear_session_overrides()
    value = cfg.set_backend_url(cfg.STANDARD_BACKEND_URL)
    success(f"Backend URL set to: {value['backend_url']}")
