from __future__ import annotations

"""
mainsequence.cli.config
=======================

Configuration and token persistence for the MainSequence CLI.

This module mirrors the VS Code extension behavior:
- Stores config.json and token.json in an OS-specific config directory
- Supports MAIN_SEQUENCE_BACKEND_URL env override (same semantics as extension)
- Provides helpers to clear tokens (logout) and update settings
"""

import json
import os
import pathlib
import sys
import time

APP_NAME = "MainSequenceCLI"


def _config_dir() -> pathlib.Path:
    """
    Return the platform-specific config directory used by the CLI.

    Matches the VS Code extension behavior:
      - Windows:  %APPDATA%\\MainSequenceCLI
      - macOS:    ~/Library/Application Support/MainSequenceCLI
      - Linux:    ~/.config/mainsequence
    """
    home = pathlib.Path.home()
    if sys.platform == "win32":
        base = pathlib.Path(os.environ.get("APPDATA", str(home)))
        return base / APP_NAME
    if sys.platform == "darwin":
        return home / "Library" / "Application Support" / APP_NAME
    return home / ".config" / "mainsequence"


CFG_DIR = _config_dir()
CFG_DIR.mkdir(parents=True, exist_ok=True)

CONFIG_JSON = CFG_DIR / "config.json"
TOKENS_JSON = CFG_DIR / "token.json"  # {username, access, refresh, ts}

DEFAULTS = {
    "backend_url": os.environ.get("MAIN_SEQUENCE_BACKEND_URL", "https://main-sequence.app/"),
    "mainsequence_path": str(pathlib.Path.home() / "mainsequence"),
    "version": 1,
}


def read_json(path: pathlib.Path, default):
    """
    Read and parse JSON from 'path'. If missing/invalid, return 'default'.
    """
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def write_json(path: pathlib.Path, obj) -> None:
    """
    Write JSON to 'path' atomically.

    We write to a temporary file and then os.replace() to ensure atomic updates
    (works on POSIX and Windows).
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def get_config() -> dict:
    """
    Load config.json merged with DEFAULTS and ensure the base projects path exists.

    Returns:
        dict: merged config with at least {backend_url, mainsequence_path, version}.
    """
    cfg = DEFAULTS | read_json(CONFIG_JSON, {})
    pathlib.Path(cfg["mainsequence_path"]).mkdir(parents=True, exist_ok=True)
    return cfg


def set_config(updates: dict) -> dict:
    """
    Update config.json with 'updates' (merged) and add 'updated_at' timestamp.

    Args:
        updates: dict of keys to set, e.g. {"backend_url": "..."}.

    Returns:
        dict: the updated full config object.
    """
    cfg = get_config() | (updates or {})
    cfg["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    write_json(CONFIG_JSON, cfg)
    return cfg


def set_backend_url(url: str) -> dict:
    """
    Convenience helper to set backend_url in config.json.

    Args:
        url: Backend base URL (e.g. https://main-sequence.app)

    Returns:
        dict: updated config
    """
    url = (url or "").strip()
    return set_config({"backend_url": url})


def get_tokens() -> dict:
    """
    Return token.json contents (or {}).
    """
    return read_json(TOKENS_JSON, {})


def save_tokens(username: str, access: str, refresh: str) -> None:
    """
    Persist {username, access, refresh, ts} into token.json.

    Args:
        username: email/username used to login
        access: access token string
        refresh: refresh token string
    """
    write_json(
        TOKENS_JSON,
        {"username": username, "access": access, "refresh": refresh, "ts": int(time.time())},
    )


def clear_tokens() -> bool:
    """
    Delete token.json and remove MAIN_SEQUENCE_USER_TOKEN from this process env.

    Returns:
        bool: True if token.json was removed (or didn't exist), False if removal failed.
    """
    ok = True
    try:
        if TOKENS_JSON.exists():
            TOKENS_JSON.unlink()
    except Exception:
        ok = False
    os.environ.pop("MAIN_SEQUENCE_USER_TOKEN", None)
    return ok


def set_env_access(access: str) -> None:
    """
    Set MAIN_SEQUENCE_USER_TOKEN for current process (and children).

    Note: cannot update the parent shell environment.
    """
    os.environ["MAIN_SEQUENCE_USER_TOKEN"] = access


def backend_url() -> str:
    """
    Return backend base URL with trailing slash removed.

    Semantics match VS Code extension:
      - Default comes from config.json or DEFAULTS
      - If MAIN_SEQUENCE_BACKEND_URL env var is set (even to empty string),
        it overrides config.json.
    """
    cfg = get_config()
    url = (cfg.get("backend_url") or DEFAULTS["backend_url"]).rstrip("/")

    # If env var exists (even empty string), override (matching extension semantics)
    if os.environ.get("MAIN_SEQUENCE_BACKEND_URL") is not None:
        url = (os.environ.get("MAIN_SEQUENCE_BACKEND_URL") or "").rstrip("/")

    return url
