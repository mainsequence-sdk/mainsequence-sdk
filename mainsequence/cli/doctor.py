"""
mainsequence.cli.doctor
=======================

Diagnostics command similar in spirit to the VS Code extension runtime panel.

Checks:
- Config paths, backend URL, base folder
- The session: where it is stored, where its credentials came from, when it expires
- Credential entries left in the checkout `.env`
- External dependencies: git, ssh tools, docker, etc.
"""

from __future__ import annotations

import datetime
import os
import pathlib
import platform
import shutil
import sys

from . import config as cfg
from .ui import print_kv, print_table


def _which(cmd: str) -> str | None:
    return shutil.which(cmd)


def _session_summary(report: dict) -> str:
    if not report["authenticated"]:
        return "none" if report["session_expires_at"] is None else "expired"
    if report["session_expires_at"] is None:
        return "valid"
    expiry = datetime.datetime.fromtimestamp(report["session_expires_at"], datetime.UTC)
    return f"valid until {expiry.strftime('%Y-%m-%d %H:%M UTC')}"


def _checkout_env_credential_keys() -> list[str]:
    """Name the credential entries in the current directory's `.env`. Values are not read out."""
    try:
        return cfg.project_env_credential_keys(
            (pathlib.Path.cwd() / ".env").read_text(encoding="utf-8", errors="replace")
        )
    except OSError:
        return []


def run_doctor() -> None:
    """
    Print a diagnostics report.
    """
    from mainsequence import bootstrap

    c = cfg.get_config()
    tokens = cfg.get_tokens()
    session = cfg.session_report()
    from_environment = session["source"] == bootstrap.CREDENTIALS_FROM_ENVIRONMENT

    print_kv(
        "MainSequence CLI - Doctor",
        [
            ("Python", sys.version.split()[0]),
            ("OS", f"{platform.system()} {platform.release()}"),
            ("Arch", platform.machine()),
            ("Backend", cfg.backend_url()),
            ("Config dir", str(cfg.CFG_DIR)),
            ("Config file", str(cfg.CONFIG_JSON)),
            ("Auth storage", session["storage"]),
            ("CodeRepositories base", str(c.get("mainsequence_path"))),
            ("Logged in user", tokens.get("username") or "-"),
            ("Session", _session_summary(session)),
            ("Credentials from", session["source"] or "-"),
        ],
    )

    # A store that cannot be read looks like a machine that is not logged in.
    if session["store_error"]:
        print_kv(
            "Credential store",
            [
                ("Could not be read", session["store_error"]),
                ("Replace the saved session with", "mainsequence login"),
            ],
        )

    tools = [
        ("git", _which("git")),
        ("ssh", _which("ssh")),
        ("ssh-keygen", _which("ssh-keygen")),
        ("ssh-agent", _which("ssh-agent")),
        ("ssh-add", _which("ssh-add")),
        ("docker", _which("docker")),
        ("code (VS Code)", _which("code")),
        ("pbcopy (macOS)", _which("pbcopy") if sys.platform == "darwin" else None),
        ("wl-copy (Wayland)", _which("wl-copy")),
        ("xclip (X11)", _which("xclip")),
        ("xsel (X11)", _which("xsel")),
    ]

    rows = []
    for name, path in tools:
        rows.append([name, "OK" if path else "-", path or "not found"])

    print_table("External dependencies", ["Tool", "Status", "Path"], rows)

    # Environment hints. The SDK copies the saved session into this process's
    # environment when it starts, so a token variable is an override only when it
    # was set before that.
    hints = []
    if os.environ.get("MAINSEQUENCE_ENDPOINT") is not None:
        hints.append(("MAINSEQUENCE_ENDPOINT", os.environ.get("MAINSEQUENCE_ENDPOINT") or ""))
    if from_environment and os.environ.get("MAINSEQUENCE_ACCESS_TOKEN"):
        hints.append(("MAINSEQUENCE_ACCESS_TOKEN", "(set)"))
    if from_environment and os.environ.get("MAINSEQUENCE_REFRESH_TOKEN"):
        hints.append(("MAINSEQUENCE_REFRESH_TOKEN", "(set)"))
    if os.environ.get("MAIN_SEQUENCE_USER_TOKEN"):
        hints.append(("MAIN_SEQUENCE_USER_TOKEN", "(legacy set)"))
    if hints:
        print_kv("Environment overrides", hints)

    leftover = _checkout_env_credential_keys()
    if leftover:
        print_kv(
            "Credentials in this checkout's .env",
            [
                ("Entries", ", ".join(leftover)),
                ("Needed", "no: the session is read from the credential store"),
                ("Remove with", "mainsequence refresh-token, run in this directory"),
            ],
        )
