from __future__ import annotations

"""
mainsequence.cli.config
=======================

Configuration and auth handling for the MainSequence CLI.

This module stores non-secret config on disk and keeps auth tokens in env,
with secure persistence via OS keychain on supported platforms.
"""

import json
import os
import pathlib
import shutil
import subprocess
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
# Deprecated compatibility constant (token file persistence is disabled).
TOKENS_JSON = CFG_DIR / "token.json"

# Session-scoped auth environment variables (no token file persistence).
ENV_USERNAME = "MAIN_SEQUENCE_USERNAME"
ENV_ACCESS = "MAIN_SEQUENCE_USER_TOKEN"
ENV_REFRESH = "MAIN_SEQUENCE_REFRESH_TOKEN"
KEYCHAIN_SERVICE = "MainSequenceCLI.auth"
KEYCHAIN_ACCOUNT = "default"

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
    Return auth tokens from environment variables, with secure-store fallback.
    """
    tokens = {
        "username": os.environ.get(ENV_USERNAME, ""),
        "access": os.environ.get(ENV_ACCESS, ""),
        "refresh": os.environ.get(ENV_REFRESH, ""),
    }
    if tokens["access"] and tokens["refresh"]:
        return tokens

    # Fallback to OS keychain if available.
    secret = _read_secure_tokens()
    if secret:
        tokens = {
            "username": tokens["username"] or secret.get("username", ""),
            "access": tokens["access"] or secret.get("access", ""),
            "refresh": tokens["refresh"] or secret.get("refresh", ""),
        }
    return tokens


def save_tokens(username: str, access: str, refresh: str) -> bool:
    """
    Save auth tokens in process environment and secure store (when supported).

    Args:
        username: email/username used to login
        access: access token string
        refresh: refresh token string
    Returns:
        bool: True if secure persistence succeeded (or is not applicable), False otherwise.
    """
    if username:
        os.environ[ENV_USERNAME] = username
    os.environ[ENV_ACCESS] = access
    os.environ[ENV_REFRESH] = refresh
    return _write_secure_tokens(username=username, access=access, refresh=refresh)


def clear_tokens() -> bool:
    """
    Clear session auth env vars for current process.
    Also remove legacy token.json if present.

    Returns:
        bool: True on success; False if legacy token file removal fails.
    """
    ok = True
    try:
        if TOKENS_JSON.exists():
            TOKENS_JSON.unlink()
    except Exception:
        ok = False

    os.environ.pop(ENV_ACCESS, None)
    os.environ.pop(ENV_REFRESH, None)
    os.environ.pop(ENV_USERNAME, None)
    if not _clear_secure_tokens():
        ok = False
    return ok


def set_env_access(access: str) -> None:
    """
    Set MAIN_SEQUENCE_USER_TOKEN for current process (and children).

    Note: cannot update the parent shell environment.
    """
    os.environ[ENV_ACCESS] = access


def _macos_security_exists() -> bool:
    return sys.platform == "darwin" and bool(shutil.which("security"))


def secure_store_available() -> bool:
    """
    Return whether a secure token store is available on this platform.
    """
    return _macos_security_exists()


def _read_secure_tokens() -> dict:
    """
    Read persisted tokens from OS keychain (macOS) when available.
    """
    if not _macos_security_exists():
        return {}
    try:
        proc = subprocess.run(
            [
                "security",
                "find-generic-password",
                "-s",
                KEYCHAIN_SERVICE,
                "-a",
                KEYCHAIN_ACCOUNT,
                "-w",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if proc.returncode != 0:
            return {}
        raw = (proc.stdout or "").strip()
        if not raw:
            return {}
        data = json.loads(raw)
        if not isinstance(data, dict):
            return {}
        return {
            "username": str(data.get("username") or ""),
            "access": str(data.get("access") or ""),
            "refresh": str(data.get("refresh") or ""),
        }
    except Exception:
        return {}


def _write_secure_tokens(*, username: str, access: str, refresh: str) -> bool:
    """
    Persist tokens in OS keychain (macOS) without writing plain token files.
    """
    if not _macos_security_exists():
        return True
    payload = json.dumps({"username": username or "", "access": access or "", "refresh": refresh or ""})
    try:
        proc = subprocess.run(
            [
                "security",
                "add-generic-password",
                "-s",
                KEYCHAIN_SERVICE,
                "-a",
                KEYCHAIN_ACCOUNT,
                "-w",
                payload,
                "-U",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        return proc.returncode == 0
    except Exception:
        return False


def _clear_secure_tokens() -> bool:
    """
    Delete persisted tokens from OS keychain (macOS). Missing entry is treated as success.
    """
    if not _macos_security_exists():
        return True
    try:
        proc = subprocess.run(
            [
                "security",
                "delete-generic-password",
                "-s",
                KEYCHAIN_SERVICE,
                "-a",
                KEYCHAIN_ACCOUNT,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        # Return code 0: deleted. Non-zero with "could not be found" also acceptable.
        err = (proc.stderr or "").lower()
        if proc.returncode == 0 or "could not be found" in err:
            return True
        return False
    except Exception:
        return False


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
