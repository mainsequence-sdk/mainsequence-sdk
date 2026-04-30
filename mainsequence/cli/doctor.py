from __future__ import annotations

"""
mainsequence.cli.doctor
=======================

Diagnostics command similar in spirit to the VS Code extension runtime panel.

Checks:
- Config paths, backend URL, base folder
- Token presence
- External dependencies: git, ssh tools, docker, etc.
"""

import os
import platform
import shutil
import sys

from . import config as cfg
from .ui import print_kv, print_table


def _which(cmd: str) -> str | None:
    return shutil.which(cmd)


def run_doctor() -> None:
    """
    Print a diagnostics report.
    """
    c = cfg.get_config()
    tokens = cfg.get_tokens()

    print_kv(
        "MainSequence CLI - Doctor",
        [
            ("Python", sys.version.split()[0]),
            ("OS", f"{platform.system()} {platform.release()}"),
            ("Arch", platform.machine()),
            ("Backend", cfg.backend_url()),
            ("Config dir", str(cfg.CFG_DIR)),
            ("Config file", str(cfg.CONFIG_JSON)),
            ("Auth storage", cfg.auth_persistence_label()),
            ("Projects base", str(c.get("mainsequence_path"))),
            ("Logged in user", tokens.get("username") or "-"),
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

    # Environment hints
    hints = []
    if os.environ.get("MAINSEQUENCE_ENDPOINT") is not None:
        hints.append(("MAINSEQUENCE_ENDPOINT", os.environ.get("MAINSEQUENCE_ENDPOINT") or ""))
    if os.environ.get("MAINSEQUENCE_ACCESS_TOKEN"):
        hints.append(("MAINSEQUENCE_ACCESS_TOKEN", "(set)"))
    if os.environ.get("MAINSEQUENCE_REFRESH_TOKEN"):
        hints.append(("MAINSEQUENCE_REFRESH_TOKEN", "(set)"))
    if os.environ.get("MAIN_SEQUENCE_USER_TOKEN"):
        hints.append(("MAIN_SEQUENCE_USER_TOKEN", "(legacy set)"))
    if hints:
        print_kv("Environment overrides", hints)
