from __future__ import annotations

"""
mainsequence.cli.config
=======================

Configuration and auth handling for the MainSequence CLI.

This module stores non-secret config on disk and keeps auth tokens in env,
with persistent storage via OS keychain on supported platforms and a
CLI-managed local auth store elsewhere.
"""

import hashlib
import ipaddress
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
SESSION_OVERRIDES_DIR = CFG_DIR / "session_overrides"
# Deprecated compatibility constant kept for cleanup of legacy installs.
TOKENS_JSON = CFG_DIR / "token.json"
AUTH_JSON = CFG_DIR / "auth.json"

# Session-scoped auth environment variables (no token file persistence).
ENV_USERNAME = "MAINSEQUENCE_USERNAME"
ENV_ACCESS = "MAINSEQUENCE_ACCESS_TOKEN"
ENV_REFRESH = "MAINSEQUENCE_REFRESH_TOKEN"
LEGACY_ENV_USERNAME = "MAIN_SEQUENCE_USERNAME"
LEGACY_ENV_ACCESS = "MAIN_SEQUENCE_USER_TOKEN"
LEGACY_ENV_REFRESH = "MAIN_SEQUENCE_REFRESH_TOKEN"
KEYCHAIN_SERVICE = "MainSequenceCLI.auth"
KEYCHAIN_ACCOUNT = "default"

DEFAULTS = {
    "backend_url": os.environ.get("MAIN_SEQUENCE_BACKEND_URL", "https://api.main-sequence.app/"),
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


def get_persistent_config() -> dict:
    """
    Return persisted config only, without terminal-session overrides.
    """
    return DEFAULTS | read_json(CONFIG_JSON, {})


def _session_scope_key() -> str | None:
    """
    Return a stable identifier for the current terminal session.

    Uses the parent shell pid plus controlling tty when available, so overrides
    stay visible to subsequent CLI invocations from the same terminal only.
    """
    explicit = (os.environ.get("MAINSEQUENCE_CLI_SESSION_ID") or "").strip()
    if explicit:
        return explicit

    tty = ""
    for fd in (0, 1, 2):
        try:
            tty = os.ttyname(fd)
            if tty:
                break
        except Exception:
            continue

    parent_pid = os.getppid()
    if not tty and not parent_pid:
        return None
    return f"{parent_pid}:{tty}"


def _session_override_path() -> pathlib.Path | None:
    key = _session_scope_key()
    if not key:
        return None
    SESSION_OVERRIDES_DIR.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
    return SESSION_OVERRIDES_DIR / f"{digest}.json"


def get_session_overrides() -> dict:
    """
    Return config overrides scoped to the current terminal session.
    """
    path = _session_override_path()
    if path is None:
        return {}
    data = read_json(path, {})
    return data if isinstance(data, dict) else {}


def set_session_overrides(*, backend_url: str | None = None, mainsequence_path: str | None = None) -> dict:
    """
    Persist backend/path overrides for the current terminal session only.
    """
    path = _session_override_path()
    if path is None:
        return {}

    overrides: dict[str, str] = {}
    if backend_url is not None:
        overrides["backend_url"] = normalize_backend_url(backend_url)
    if mainsequence_path is not None:
        normalized_path = normalize_mainsequence_path(mainsequence_path)
        pathlib.Path(normalized_path).mkdir(parents=True, exist_ok=True)
        overrides["mainsequence_path"] = normalized_path

    if overrides:
        write_json(path, overrides)
    elif path.exists():
        path.unlink()
    return overrides


def clear_session_overrides() -> None:
    """
    Clear backend/path overrides for the current terminal session.
    """
    path = _session_override_path()
    if path and path.exists():
        try:
            path.unlink()
        except Exception:
            pass


def get_config() -> dict:
    """
    Load config.json merged with DEFAULTS and ensure the base projects path exists.

    Returns:
        dict: merged config with at least {backend_url, mainsequence_path, version}.
    """
    cfg = get_persistent_config() | get_session_overrides()
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
    cfg = get_persistent_config() | (updates or {})
    cfg["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    write_json(CONFIG_JSON, cfg)
    return cfg


def set_backend_url(url: str) -> dict:
    """
    Convenience helper to set backend_url in config.json.

    Args:
        url: Backend base URL (e.g. https://api.main-sequence.app)

    Returns:
        dict: updated config
    """
    url = normalize_backend_url(url)
    return set_config({"backend_url": url})


def set_mainsequence_path(path: str) -> dict:
    """
    Convenience helper to set the projects base folder in config.json.

    A bare folder name like `mainsequence-dev` is interpreted as `~/mainsequence-dev`.
    """
    normalized = normalize_mainsequence_path(path)
    pathlib.Path(normalized).mkdir(parents=True, exist_ok=True)
    return set_config({"mainsequence_path": normalized})


def normalize_backend_url(url: str | None) -> str:
    """
    Normalize backend input into an absolute base URL without trailing slash.

    Rules:
      - keep explicit `http://` / `https://` as-is
      - default to `http://` for localhost/private IP style targets
      - default to `https://` for everything else
    """
    raw = (url or "").strip()
    if not raw:
        return raw

    if "://" in raw:
        return raw.rstrip("/")

    host = raw.split("/", 1)[0].split(":", 1)[0].strip("[]").lower()
    scheme = "https"

    if host in {"localhost", "0.0.0.0"}:
        scheme = "http"
    else:
        try:
            ip = ipaddress.ip_address(host)
            if ip.is_loopback or ip.is_private or ip.is_unspecified:
                scheme = "http"
        except ValueError:
            pass

    return f"{scheme}://{raw}".rstrip("/")


def normalize_mainsequence_path(path: str | None) -> str:
    """
    Normalize projects base folder input into an absolute path.

    Rules:
      - `~/foo`, `/tmp/foo`, `./foo`, `../foo` behave like normal filesystem paths
      - a bare folder name like `mainsequence-dev` maps to `~/mainsequence-dev`
    """
    raw = (path or "").strip()
    if not raw:
        return str(pathlib.Path.home() / "mainsequence")

    if raw.startswith(("~", ".", "/")) or "\\" in raw or "/" in raw:
        return str(pathlib.Path(raw).expanduser().resolve())

    return str((pathlib.Path.home() / raw).resolve())


def _normalize_token_payload(data: dict | None) -> dict:
    if not isinstance(data, dict):
        return {}
    return {
        "username": str(data.get("username") or ""),
        "access": str(data.get("access") or ""),
        "refresh": str(data.get("refresh") or ""),
    }


def _read_local_tokens() -> dict:
    """
    Read persisted tokens from the CLI-managed local auth store.
    """
    try:
        return _normalize_token_payload(read_json(AUTH_JSON, {}))
    except Exception:
        return {}


def _write_local_tokens(*, username: str, access: str, refresh: str) -> bool:
    """
    Persist tokens in the CLI-managed local auth store.
    """
    try:
        write_json(
            AUTH_JSON,
            {
                "username": username or "",
                "access": access or "",
                "refresh": refresh or "",
            },
        )
        if os.name == "posix":
            try:
                os.chmod(AUTH_JSON, 0o600)
            except Exception:
                pass
        return True
    except Exception:
        return False


def _clear_local_tokens() -> bool:
    """
    Delete the CLI-managed local auth store. Missing file is treated as success.
    """
    try:
        if AUTH_JSON.exists():
            AUTH_JSON.unlink()
        return True
    except Exception:
        return False


def auth_persistence_label() -> str:
    """
    Return the human-readable auth persistence backend label.
    """
    if _macos_security_exists():
        return "secure OS storage"
    return "local CLI auth storage"


def get_tokens() -> dict:
    """
    Return auth tokens from environment variables, with persistent-store fallback.
    """
    runtime_mode = (os.environ.get("MAINSEQUENCE_AUTH_MODE") or "").strip().lower() == "runtime_credential"
    tokens = {
        "username": os.environ.get(ENV_USERNAME) or os.environ.get(LEGACY_ENV_USERNAME, ""),
        "access": os.environ.get(ENV_ACCESS) or os.environ.get(LEGACY_ENV_ACCESS, ""),
        "refresh": os.environ.get(ENV_REFRESH) or os.environ.get(LEGACY_ENV_REFRESH, ""),
    }
    if tokens["access"] and (tokens["refresh"] or runtime_mode):
        return tokens

    for secret in (_read_secure_tokens(), _read_local_tokens()):
        if not secret:
            continue
        tokens = {
            "username": tokens["username"] or secret.get("username", ""),
            "access": tokens["access"] or secret.get("access", ""),
            "refresh": tokens["refresh"] or secret.get("refresh", ""),
        }
        if tokens["access"] and (tokens["refresh"] or runtime_mode):
            break
    return tokens


def save_tokens(username: str, access: str, refresh: str) -> bool:
    """
    Save auth tokens in process environment and the active persistent store.

    Args:
        username: email/username used to login
        access: access token string
        refresh: refresh token string
    Returns:
        bool: True if persistent storage succeeded, False otherwise.
    """
    if username:
        os.environ[ENV_USERNAME] = username
    os.environ[ENV_ACCESS] = access
    os.environ[ENV_REFRESH] = refresh
    os.environ.pop(LEGACY_ENV_USERNAME, None)
    os.environ.pop(LEGACY_ENV_ACCESS, None)
    os.environ.pop(LEGACY_ENV_REFRESH, None)
    if _macos_security_exists():
        return _write_secure_tokens(username=username, access=access, refresh=refresh)
    return _write_local_tokens(username=username, access=access, refresh=refresh)


def clear_tokens() -> bool:
    """
    Clear session auth env vars for current process.
    Also remove persisted auth state from the active local/secure store.

    Returns:
        bool: True on success; False if any persisted auth state could not be removed.
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
    os.environ.pop(LEGACY_ENV_ACCESS, None)
    os.environ.pop(LEGACY_ENV_REFRESH, None)
    os.environ.pop(LEGACY_ENV_USERNAME, None)
    if not _clear_secure_tokens():
        ok = False
    if not _clear_local_tokens():
        ok = False
    return ok


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
        return _normalize_token_payload(json.loads(raw))
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

    return normalize_backend_url(url)
