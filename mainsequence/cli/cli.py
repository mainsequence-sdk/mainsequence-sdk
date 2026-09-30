"""Small command-line surface for SDK authentication and endpoint configuration."""

from __future__ import annotations

import importlib.metadata
import json
import os

import requests
import typer

from . import config as cfg
from .api import ApiError, get_current_user_profile, logout_cli_session
from .browser_auth import BrowserAuthError, login_via_browser, login_via_mcp_handoff
from .code_repository import code_repository
from .ui import error, success, warn

app = typer.Typer(help="Main Sequence platform SDK", no_args_is_help=True)
settings = typer.Typer(help="Backend endpoint settings", no_args_is_help=True)
app.add_typer(settings, name="settings")
app.add_typer(code_repository, name="code-repository")


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
    backend: str | None = typer.Argument(
        None,
        help="Optional backend URL or host[:port], for example 127.0.0.1:8000.",
    ),
    code_repositories_base: str | None = typer.Argument(
        None,
        help="Optional local CodeRepositories base folder, for example mainsequence-dev.",
    ),
    access_token: str | None = typer.Option(None, "--access-token", help="JWT access token."),
    refresh_token: str | None = typer.Option(None, "--refresh-token", help="JWT refresh token."),
    no_open: bool = typer.Option(
        False,
        "--no-open",
        help="Do not auto-open a browser. Print the authorization URL and wait for callback.",
    ),
    mcp: bool = typer.Option(
        False,
        "--mcp",
        help=(
            "Authorize this CLI through the already-authenticated Main Sequence "
            "MCP principal instead of opening a browser."
        ),
    ),
    mcp_timeout_seconds: int = typer.Option(
        300,
        "--mcp-timeout-seconds",
        min=1,
        help="Seconds to wait for the MCP host to authorize the CLI handoff.",
    ),
    backend_option: str | None = typer.Option(
        None,
        "--backend",
        help="Backend URL or host[:port], for example http://127.0.0.1:8000.",
    ),
    code_repositories_base_option: str | None = typer.Option(
        None,
        "--code-repositories-base",
        "--base-folder",
        help="Local CodeRepositories base folder for this terminal session, for example mainsequence-dev.",
    ),
    no_status: bool = typer.Option(
        False,
        "--no-status",
        hidden=True,
        help="Deprecated no-op kept only for backward compatibility.",
    ),
    export: bool = typer.Option(
        False,
        "--export",
        "--export-env",
        help="Print shell export commands for session auth variables.",
    ),
):
    """
    Authenticate to the MainSequence platform.

    Persists auth tokens in the active CLI auth store so subsequent
    CLI invocations can run without re-authentication. Backend/base-folder
    overrides passed to `login` are scoped to the current terminal session.
    When no backend is provided, login uses the currently configured backend.

    Interactive login uses browser-based authentication and finishes with
    standard JWT access/refresh tokens persisted by the CLI.

    When a backend-launched process already has
    `MAINSEQUENCE_AUTH_MODE=runtime_credential`, login exchanges the injected
    runtime credential for a short-lived access token instead of opening the
    browser or persisting CLI JWT tokens. This is not a user branch/runtime
    selection mechanism.

    Parameters
    ----------
    backend:
        Optional positional backend override for backward compatibility.
    code_repositories_base:
        Optional positional CodeRepositories base folder.
    access_token:
        JWT access token for manual token import.
    refresh_token:
        JWT refresh token for manual token import.
    no_open:
        If True, do not auto-open a browser. The CLI prints the auth URL.
    mcp:
        If True, use a backend-issued handoff approved by auth.cli_authorize.
    mcp_timeout_seconds:
        Maximum time to wait for the MCP authorization handoff.
    backend_option:
        Backend override for this terminal session.
    code_repositories_base_option:
        CodeRepositories base-folder override for this terminal session.
    export:
        If True, print shell export lines for auth variables.

    Examples
    --------
    ```bash
    mainsequence login
    mainsequence login 127.0.0.1:8000 mainsequence-dev
    mainsequence login --no-open
    mainsequence login --mcp
    mainsequence login --access-token "$TOKEN" --refresh-token "$REFRESH"
    mainsequence login --access-token "$TOKEN" --refresh-token "$REFRESH" --backend http://127.0.0.1:8000 --code-repositories-base mainsequence-dev
    mainsequence login --export
    ```
    """
    using_jwt = bool((access_token or "").strip() or (refresh_token or "").strip())
    using_runtime_credential = _runtime_credential_mode_enabled()

    if using_runtime_credential and using_jwt:
        error("Runtime credential login cannot be combined with --access-token/--refresh-token.")
        raise typer.Exit(1)
    if using_runtime_credential and mcp:
        error(
            "MCP handoff login cannot be used when "
            "MAINSEQUENCE_AUTH_MODE=runtime_credential. Run `mainsequence login` instead."
        )
        raise typer.Exit(1)

    if mcp and using_jwt:
        error("MCP handoff login cannot be combined with --access-token/--refresh-token.")
        raise typer.Exit(1)
    if mcp and export:
        error("MCP handoff login cannot be combined with --export.")
        raise typer.Exit(1)

    if using_runtime_credential and no_open:
        warn("--no-open is ignored when MAINSEQUENCE_AUTH_MODE=runtime_credential.")
    if mcp and no_open:
        warn("--no-open is ignored during MCP handoff login.")

    if not using_jwt and backend and "@" in backend:
        error(
            "Email/password CLI login was removed. Use `mainsequence login` for browser login "
            "or use --access-token/--refresh-token for manual JWT import."
        )
        raise typer.Exit(1)

    if backend and backend_option:
        if cfg.normalize_backend_url(backend) != cfg.normalize_backend_url(backend_option):
            error("Pass backend either positionally or with --backend, not both.")
            raise typer.Exit(1)
    explicit_backend_input = backend_option if backend_option is not None else backend
    current_backend = cfg.backend_url()
    effective_backend_input = (
        explicit_backend_input if explicit_backend_input is not None else current_backend
    )

    if code_repositories_base and code_repositories_base_option:
        if cfg.normalize_mainsequence_path(
            code_repositories_base
        ) != cfg.normalize_mainsequence_path(code_repositories_base_option):
            error(
                "Pass the CodeRepositories base either positionally or with "
                "--code-repositories-base/--base-folder, not both."
            )
            raise typer.Exit(1)
    effective_code_repositories_base_input = (
        code_repositories_base_option
        if code_repositories_base_option is not None
        else code_repositories_base
    )

    if using_jwt:
        if not (access_token or "").strip() or not (refresh_token or "").strip():
            error("JWT login requires both --access-token and --refresh-token.")
            raise typer.Exit(1)
    elif access_token is not None or refresh_token is not None:
        error("JWT login requires both --access-token and --refresh-token.")
        raise typer.Exit(1)

    normalized_backend = cfg.normalize_backend_url(effective_backend_input)

    if explicit_backend_input is not None and normalized_backend != current_backend:
        if not effective_code_repositories_base_input:
            error(
                "When using a different backend, you must also specify a "
                "CodeRepositories base folder."
            )
            raise typer.Exit(1)

    previous_backend_override = os.environ.get("MAINSEQUENCE_ENDPOINT")
    os.environ["MAINSEQUENCE_ENDPOINT"] = normalized_backend

    try:
        if using_runtime_credential:
            access = _exchange_runtime_credential(normalized_backend)
            persisted = cfg.save_tokens("", access, "")
            res = {
                "username": "",
                "backend": normalized_backend,
                "access": access,
                "refresh": "",
                "persisted": bool(persisted),
                "auth_mode": "runtime_credential",
            }
        elif using_jwt:
            os.environ.pop(cfg.ENV_USERNAME, None)
            os.environ.pop(cfg.LEGACY_ENV_USERNAME, None)
            persisted = cfg.save_tokens(
                "", (access_token or "").strip(), (refresh_token or "").strip()
            )
            res = {
                "username": "",
                "backend": normalized_backend,
                "access": (access_token or "").strip(),
                "refresh": (refresh_token or "").strip(),
                "persisted": bool(persisted),
                "auth_mode": "jwt",
            }
        elif mcp:

            def _emit_mcp_handoff(handoff: dict) -> None:
                tool_call = {
                    "tool": handoff["mcp_tool"],
                    "arguments": handoff["mcp_arguments"],
                }
                typer.echo("Authorize this CLI from the connected Main Sequence MCP session:")
                typer.echo(json.dumps(tool_call, separators=(",", ":")))

            flow = login_via_mcp_handoff(
                timeout_seconds=mcp_timeout_seconds,
                on_handoff=_emit_mcp_handoff,
            )
            access = (flow.get("access") or "").strip()
            refresh = (flow.get("refresh") or "").strip()
            if not access or not refresh:
                raise ApiError("MCP handoff login did not return access and refresh tokens.")

            persisted = cfg.save_tokens("", access, refresh)
            user_payload = flow.get("user")
            username = ""
            if isinstance(user_payload, dict):
                username = str(user_payload.get("username") or "").strip()
            if not username:
                profile = get_current_user_profile()
                if isinstance(profile, dict):
                    username = (profile.get("username") or "").strip()
            if username:
                persisted = bool(cfg.save_tokens(username, access, refresh) and persisted)

            res = {
                "username": username,
                "backend": normalized_backend,
                "access": access,
                "refresh": refresh,
                "persisted": bool(persisted),
                "auth_mode": "jwt",
            }
        else:

            def _emit_auth_url(url: str) -> None:
                typer.echo(f"Open this URL to authenticate: {url}")

            flow = login_via_browser(
                no_open=no_open,
                on_authorize_url=_emit_auth_url if no_open else None,
            )
            access = (flow.get("access") or "").strip()
            refresh = (flow.get("refresh") or "").strip()
            if not access or not refresh:
                raise ApiError("Browser login did not return access and refresh tokens.")

            persisted = cfg.save_tokens("", access, refresh)
            username = ""
            profile = get_current_user_profile()
            if isinstance(profile, dict):
                username = (profile.get("username") or "").strip()
            if username:
                persisted = bool(cfg.save_tokens(username, access, refresh) and persisted)

            res = {
                "username": username,
                "backend": normalized_backend,
                "access": access,
                "refresh": refresh,
                "persisted": bool(persisted),
                "auth_mode": "jwt",
            }
    except BrowserAuthError as e:
        login_kind = "MCP handoff" if mcp else "Browser"
        error(f"{login_kind} login failed: {e}")
        raise typer.Exit(1) from e
    except ApiError as e:
        error(f"Login failed: {e}")
        raise typer.Exit(1) from e
    finally:
        if previous_backend_override is None:
            os.environ.pop("MAINSEQUENCE_ENDPOINT", None)
        else:
            os.environ["MAINSEQUENCE_ENDPOINT"] = previous_backend_override

    cfg.set_session_overrides(
        backend_url=normalized_backend,
        mainsequence_path=effective_code_repositories_base_input,
    )

    if export:
        access = (res.get("access") or "").replace('"', '\\"')
        refresh = (res.get("refresh") or "").replace('"', '\\"')
        username = (res.get("username") or "").replace('"', '\\"')
        auth_mode = (res.get("auth_mode") or "").replace('"', '\\"')
        if auth_mode:
            typer.echo(f'export MAINSEQUENCE_AUTH_MODE="{auth_mode}"')
        typer.echo(f'export MAINSEQUENCE_ACCESS_TOKEN="{access}"')
        if refresh:
            typer.echo(f'export MAINSEQUENCE_REFRESH_TOKEN="{refresh}"')
        if username:
            typer.echo(f'export MAINSEQUENCE_USERNAME="{username}"')
        return

    cfg_obj = cfg.get_config()
    base = cfg_obj["mainsequence_path"]
    typer.echo("MAIN SEQUENCE")
    if res.get("username"):
        success(f"Signed in as {res['username']} (Backend: {res['backend']})")
    elif res.get("auth_mode") == "runtime_credential":
        success(f"Signed in with runtime credential (Backend: {res['backend']})")
    else:
        success(f"Signed in with JWT tokens (Backend: {res['backend']})")
    typer.echo(f"CodeRepositories base folder: {base}")
    auth_store_label = cfg.auth_persistence_label()
    if res.get("auth_mode") == "runtime_credential":
        typer.echo(
            f"Runtime credential access token is persisted in {auth_store_label}; no CLI JWT refresh token exists."
        )
        typer.echo(
            "When the access token expires, CLI will re-exchange the runtime credential automatically."
        )
    elif res.get("persisted", True):
        typer.echo(f"Auth tokens are persisted in {auth_store_label} for subsequent CLI commands.")
    else:
        warn(
            f"Could not persist auth tokens in {auth_store_label}. Use --export for shell-based auth."
        )


@app.command()
def logout(
    export: bool = typer.Option(False, "--export", "--export-env", help="Print shell auth unsets."),
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
            "MAINSEQUENCE_ACCESS_TOKEN",
            "MAINSEQUENCE_REFRESH_TOKEN",
            "MAINSEQUENCE_USERNAME",
            "MAIN_SEQUENCE_USER_TOKEN",
            "MAIN_SEQUENCE_REFRESH_TOKEN",
            "MAIN_SEQUENCE_USERNAME",
        ):
            typer.echo(f"unset {name}")
        return
    if not cleared or (revoke.get("attempted") and not revoke.get("revoked")):
        warn(
            "Local session cleared; backend revocation or persistent-store cleanup was not confirmed."
        )
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
    check_connection: bool = typer.Option(
        False, "--check-connection", help="Probe the backend URL."
    ),
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
