"""
mainsequence.cli.config
=======================

Configuration and auth handling for the MainSequence CLI.

This module stores non-secret config on disk and keeps auth tokens in env,
with persistent storage in the operating system credential store. A project
directory never holds a credential: the session is one record per backend in
that store, and `.env` keeps only non-secret settings such as the endpoint.
"""

from __future__ import annotations

import base64
import contextlib
import hashlib
import ipaddress
import json
import os
import pathlib
import re
import subprocess
import sys
import time

import keyring
from keyring import core as keyring_core
from keyring import errors as keyring_errors

from mainsequence.defaults import CANONICAL_BACKEND_ENV, STANDARD_BACKEND_URL

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
RUNTIME_ACCESS_CACHE_JSON = CFG_DIR / "runtime_access_cache.json"
A2A_HANDLE_CACHE_JSON = CFG_DIR / "a2a_handle_cache.json"

# Session-scoped auth environment variables (no token file persistence).
ENV_USERNAME = "MAINSEQUENCE_USERNAME"
ENV_ACCESS = "MAINSEQUENCE_ACCESS_TOKEN"
ENV_REFRESH = "MAINSEQUENCE_REFRESH_TOKEN"
LEGACY_ENV_USERNAME = "MAIN_SEQUENCE_USERNAME"
LEGACY_ENV_ACCESS = "MAIN_SEQUENCE_USER_TOKEN"
LEGACY_ENV_REFRESH = "MAIN_SEQUENCE_REFRESH_TOKEN"
KEYCHAIN_SERVICE = "MainSequenceCLI.auth"
KEYCHAIN_ACCOUNT = "default"
AUTH_RECORD_VERSION = 1
# Credential entries a project `.env` must not hold. The CLI removes them and
# never writes them; the session lives in the operating system credential store.
PROJECT_ENV_CREDENTIAL_KEYS = (
    "MAINSEQUENCE_ACCESS_TOKEN",
    "MAINSEQUENCE_REFRESH_TOKEN",
    "MAINSEQUENCE_RUNTIME_CREDENTIAL_ID",
    "MAINSEQUENCE_RUNTIME_CREDENTIAL_SECRET",
    "MAINSEQUENCE_TOKEN",
)
DEFAULT_RUNTIME_ACCESS_CACHE_TTL_SECONDS = 60
RUNTIME_ACCESS_CACHE_EXPIRY_SKEW_SECONDS = 30

DEFAULTS = {
    "backend_url": os.environ.get(CANONICAL_BACKEND_ENV, f"{STANDARD_BACKEND_URL}/"),
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


def set_session_overrides(
    *, backend_url: str | None = None, mainsequence_path: str | None = None
) -> dict:
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
    Load config.json merged with DEFAULTS and ensure the CodeRepositories base path exists.

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
        url: Backend base URL.

    Returns:
        dict: updated config
    """
    url = normalize_backend_url(url)
    return set_config({"backend_url": url})


def set_mainsequence_path(path: str) -> dict:
    """
    Convenience helper to set the CodeRepositories base folder in config.json.

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
    Normalize CodeRepositories base folder input into an absolute path.

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


def _auth_backend_key(backend: str | None = None) -> str:
    """
    Return the normalized backend key used for auth persistence scoping.
    """
    return normalize_backend_url(backend or backend_url())


def token_expiry(token: str | None) -> int | None:
    """
    Return the `exp` claim of a JWT as epoch seconds, without verifying the token.

    Used only to decide whether a token is still worth sending. Returns None when
    the value is not a JWT or carries no expiry.
    """
    if not token:
        return None
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return None
        payload = parts[1] + "=" * (-len(parts[1]) % 4)
        claims = json.loads(base64.urlsafe_b64decode(payload.encode("ascii")).decode("utf-8"))
        expiry = claims.get("exp")
        return int(expiry) if expiry is not None else None
    except Exception:
        return None


_ENV_ASSIGNMENT = re.compile(r"\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=")


def env_line_key(line: str) -> str | None:
    """
    Return the variable a `.env` line assigns, or None for any other line.

    The forms are the ones the tools that load a `.env` accept: leading
    whitespace, an `export` prefix, and whitespace before the equals sign. A
    commented line assigns nothing.
    """
    match = _ENV_ASSIGNMENT.match(line)
    return match.group(1) if match else None


def project_env_credential_keys(env_text: str) -> list[str]:
    """
    Return the credential entries present in a project `.env`, by name only.
    """
    assigned = {env_line_key(line) for line in (env_text or "").replace("\r", "").splitlines()}
    return [key for key in PROJECT_ENV_CREDENTIAL_KEYS if key in assigned]


def strip_env_credentials(env_text: str) -> tuple[str, list[str]]:
    """
    Return `.env` text without its credential entries, and the names removed.

    Every other line is kept exactly as it is, the endpoint included. A
    `MAINSEQUENCE_AUTH_MODE=runtime_credential` line goes with the runtime
    credential it announced; any other mode is the developer's own setting.
    """
    kept: list[str] = []
    dropped_mode = False
    for line in (env_text or "").splitlines(keepends=True):
        key = env_line_key(line)
        if key in PROJECT_ENV_CREDENTIAL_KEYS:
            continue
        if key == "MAINSEQUENCE_AUTH_MODE":
            value = line.split("=", 1)[1].split("#", 1)[0]
            if value.strip().strip("\"'").lower() == "runtime_credential":
                dropped_mode = True
                continue
        kept.append(line)
    removed = project_env_credential_keys(env_text)
    if dropped_mode:
        removed.append("MAINSEQUENCE_AUTH_MODE")
    return ("".join(kept) if removed else env_text or ""), removed


def _runtime_access_user_key() -> str:
    tokens = get_tokens()
    username = str(tokens.get("username") or "").strip()
    if username:
        return f"username:{username}"
    access = str(tokens.get("access") or "").strip()
    if access:
        return f"access:{hashlib.sha256(access.encode('utf-8')).hexdigest()}"
    return "anonymous"


def _scoped_cache_key(value: str, backend: str | None = None) -> str:
    raw = f"{_auth_backend_key(backend)}|{_runtime_access_user_key()}|{value.strip()}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _read_cache(path: pathlib.Path) -> dict:
    data = read_json(path, {})
    entries = data.get("entries") if isinstance(data, dict) else None
    return {"version": 1, "entries": entries if isinstance(entries, dict) else {}}


def _write_cache(path: pathlib.Path, data: dict) -> None:
    write_json(path, data)
    if os.name == "posix":
        try:
            os.chmod(path, 0o600)
        except OSError:
            pass


def _parse_iso_utc_to_epoch(value: object) -> float | None:
    if not isinstance(value, str) or not value.strip():
        return None
    from datetime import UTC, datetime

    raw = value.strip()
    if raw.endswith("Z"):
        raw = f"{raw[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.timestamp()


def _iso_utc_from_epoch(value: float) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(value))


def get_runtime_access_cache_entry(
    agent_session_uid: str,
    *,
    backend: str | None = None,
) -> dict | None:
    """Return a non-expired runtime-access cache entry for one AgentSession."""

    key = _scoped_cache_key(str(agent_session_uid), backend)
    cache = _read_cache(RUNTIME_ACCESS_CACHE_JSON)
    entry = cache["entries"].get(key)
    if not isinstance(entry, dict):
        return None
    expires_at = entry.get("expires_at_epoch")
    access = entry.get("access")
    if (
        isinstance(expires_at, int | float)
        and expires_at <= time.time()
        or not isinstance(access, dict)
    ):
        cache["entries"].pop(key, None)
        if cache["entries"]:
            _write_cache(RUNTIME_ACCESS_CACHE_JSON, cache)
        else:
            RUNTIME_ACCESS_CACHE_JSON.unlink(missing_ok=True)
        return None
    return dict(entry)


def get_runtime_access_cache(
    agent_session_uid: str,
    *,
    backend: str | None = None,
) -> dict | None:
    """Return cached runtime access for one AgentSession."""

    entry = get_runtime_access_cache_entry(agent_session_uid, backend=backend)
    if entry is None:
        return None
    access = entry.get("access")
    return dict(access) if isinstance(access, dict) else None


def save_runtime_access_cache(
    agent_session_uid: str,
    access_payload: dict,
    *,
    backend: str | None = None,
    ttl_seconds: int | float | None = DEFAULT_RUNTIME_ACCESS_CACHE_TTL_SECONDS,
) -> dict:
    """Persist short-lived runtime access separately from login credentials."""

    if not isinstance(access_payload, dict):
        raise TypeError("access_payload must be a dict")
    now = time.time()
    payload_expiry = _parse_iso_utc_to_epoch(access_payload.get("expires_at"))
    expires_at = (
        max(now, payload_expiry - RUNTIME_ACCESS_CACHE_EXPIRY_SKEW_SECONDS)
        if payload_expiry is not None
        else None if ttl_seconds is None else now + float(ttl_seconds)
    )
    entry = {
        "backend_url": _auth_backend_key(backend),
        "agent_session_uid": str(agent_session_uid),
        "cached_at_epoch": now,
        "cached_at": _iso_utc_from_epoch(now),
        "expires_at_epoch": expires_at,
        "expires_at": _iso_utc_from_epoch(expires_at) if expires_at else None,
        "access": dict(access_payload),
    }
    cache = _read_cache(RUNTIME_ACCESS_CACHE_JSON)
    cache["entries"][_scoped_cache_key(str(agent_session_uid), backend)] = entry
    _write_cache(RUNTIME_ACCESS_CACHE_JSON, cache)
    return dict(entry)


def clear_runtime_access_cache(
    agent_session_uid: str | None = None,
    *,
    backend: str | None = None,
) -> bool:
    """Clear cached runtime access for one AgentSession or for all sessions."""

    try:
        if not RUNTIME_ACCESS_CACHE_JSON.exists():
            return True
        if agent_session_uid is None:
            RUNTIME_ACCESS_CACHE_JSON.unlink()
            return True
        cache = _read_cache(RUNTIME_ACCESS_CACHE_JSON)
        cache["entries"].pop(_scoped_cache_key(str(agent_session_uid), backend), None)
        if cache["entries"]:
            _write_cache(RUNTIME_ACCESS_CACHE_JSON, cache)
        else:
            RUNTIME_ACCESS_CACHE_JSON.unlink()
        return True
    except OSError:
        return False


def get_a2a_handle_cache(
    handle_unique_id: str,
    *,
    backend: str | None = None,
) -> dict | None:
    """Return one backend- and user-scoped A2A handle mapping."""

    handle = str(handle_unique_id or "").strip()
    if not handle:
        return None
    entry = _read_cache(A2A_HANDLE_CACHE_JSON)["entries"].get(
        _scoped_cache_key(handle, backend)
    )
    return dict(entry) if isinstance(entry, dict) else None


def save_a2a_handle_cache(
    handle_unique_id: str,
    *,
    agent_uid: str,
    agent_session_uid: str,
    name: str | None = None,
    backend: str | None = None,
) -> dict:
    """Persist an A2A handle mapping without storing runtime credentials."""

    handle = str(handle_unique_id or "").strip()
    if not handle:
        raise ValueError("handle_unique_id is required")
    now = time.time()
    entry = {
        "backend_url": _auth_backend_key(backend),
        "handle_unique_id": handle,
        "agent_uid": str(agent_uid),
        "agent_session_uid": str(agent_session_uid),
        "name": str(name) if name is not None else None,
        "cached_at_epoch": now,
        "cached_at": _iso_utc_from_epoch(now),
    }
    cache = _read_cache(A2A_HANDLE_CACHE_JSON)
    cache["entries"][_scoped_cache_key(handle, backend)] = entry
    _write_cache(A2A_HANDLE_CACHE_JSON, cache)
    return dict(entry)


def _keychain_account_for_backend(backend: str | None = None) -> str:
    """
    Return the backend-scoped keychain account name.
    """
    digest = hashlib.sha256(_auth_backend_key(backend).encode("utf-8")).hexdigest()[:16]
    return f"{KEYCHAIN_ACCOUNT}.{digest}"


def _read_local_tokens(backend: str | None = None) -> dict:
    """
    Read persisted tokens from the CLI-managed local auth store.
    """
    try:
        data = read_json(AUTH_JSON, {})
        if isinstance(data, dict) and isinstance(data.get("by_backend"), dict):
            return _normalize_token_payload(data["by_backend"].get(_auth_backend_key(backend)))
        return _normalize_token_payload(data)
    except Exception:
        return {}


def _clear_local_tokens(backend: str | None = None) -> bool:
    """
    Delete legacy file-based auth for one backend. Missing state is success.
    """
    try:
        if not AUTH_JSON.exists():
            return True
        data = read_json(AUTH_JSON, {})
        if isinstance(data, dict) and isinstance(data.get("by_backend"), dict):
            by_backend = dict(data["by_backend"])
            by_backend.pop(_auth_backend_key(backend), None)
            if by_backend:
                write_json(AUTH_JSON, {"version": 2, "by_backend": by_backend})
                if os.name == "posix":
                    try:
                        os.chmod(AUTH_JSON, 0o600)
                    except Exception:
                        pass
            else:
                AUTH_JSON.unlink()
        else:
            AUTH_JSON.unlink()
        return True
    except Exception:
        return False


def auth_persistence_label() -> str:
    """
    Return the human-readable auth persistence backend label.
    """
    secure_backend = _secure_keyring_backend()
    if secure_backend is None:
        return "process environment only (secure OS credential storage unavailable)"
    return f"secure OS credential storage ({secure_backend.name})"


def get_tokens() -> dict:
    """
    Return auth tokens from environment variables, with persistent-store fallback.
    """
    runtime_mode = (
        os.environ.get("MAINSEQUENCE_AUTH_MODE") or ""
    ).strip().lower() == "runtime_credential"
    tokens = {
        "username": os.environ.get(ENV_USERNAME) or os.environ.get(LEGACY_ENV_USERNAME, ""),
        "access": os.environ.get(ENV_ACCESS) or os.environ.get(LEGACY_ENV_ACCESS, ""),
        "refresh": os.environ.get(ENV_REFRESH) or os.environ.get(LEGACY_ENV_REFRESH, ""),
    }
    if tokens["access"] and (tokens["refresh"] or runtime_mode):
        return tokens

    secret = _read_secure_tokens()
    # A store that could not be read is not an empty store: moving the old plain
    # file's session into it would replace the entry that could not be read.
    if not secret and store_read_error() is None:
        secret = _migrate_local_tokens_to_secure_store()
    # A stored session is usable with a refresh token alone: the access token is
    # short-lived and is renewed from it. An access token alone is a session only
    # for a runtime credential, which has no refresh token.
    if secret and (secret.get("refresh") or (runtime_mode and secret.get("access"))):
        tokens = {
            "username": tokens["username"] or secret.get("username", ""),
            "access": tokens["access"] or secret.get("access", ""),
            "refresh": tokens["refresh"] or secret.get("refresh", ""),
        }
    return tokens


def saved_username_for(refresh: str) -> str:
    """
    Return the user name saved with the session a refresh token belongs to.

    A process that was handed only tokens, without the user they belong to, renews
    the session without knowing its user. The name the session was saved with is
    kept when the saved record holds the same refresh token, and is "" otherwise.
    """
    if not refresh:
        return ""
    saved = _read_secure_tokens()
    return str(saved.get("username") or "") if saved.get("refresh") == refresh else ""


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
    if not _write_secure_tokens(username=username, access=access, refresh=refresh):
        return False
    readback = _read_secure_tokens()
    persisted = readback.get("access") == access and readback.get("refresh") == refresh
    if persisted:
        _clear_local_tokens()
    return persisted


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


def secure_store_available() -> bool:
    """
    Return whether a secure token store is available on this platform.
    """
    return _secure_keyring_backend() is not None


def stored_session_available(backend: str | None = None) -> bool:
    """
    Return whether the credential store holds a renewable session for a backend.

    This reads the store only; it says nothing about the process environment.
    """
    return bool(_read_secure_tokens(backend).get("refresh"))


def session_report() -> dict:
    """
    Describe the session this process would use, without any token value.

    `authenticated` is judged from the tokens' own expiry and asks nothing of the
    backend. A runtime credential renews by a new exchange, so it has no session
    expiry. `source` says whether the credentials were already in the process
    environment or came from the saved CLI session. `store_error` says why the
    credential store could not be read, when it could not.
    """
    from mainsequence import bootstrap

    tokens = get_tokens()
    access = (tokens.get("access") or "").strip()
    refresh = (tokens.get("refresh") or "").strip()
    auth_mode = (os.environ.get("MAINSEQUENCE_AUTH_MODE") or "jwt").strip().lower()
    now = int(time.time())
    access_expires_at = token_expiry(access)
    refresh_expires_at = token_expiry(refresh)

    if auth_mode == "runtime_credential":
        authenticated = bool(
            access
            or (
                (os.environ.get("MAINSEQUENCE_RUNTIME_CREDENTIAL_ID") or "").strip()
                and (os.environ.get("MAINSEQUENCE_RUNTIME_CREDENTIAL_SECRET") or "").strip()
            )
        )
        session_expires_at = None
    elif refresh:
        authenticated = refresh_expires_at is None or refresh_expires_at > now
        session_expires_at = refresh_expires_at
    else:
        authenticated = bool(access) and (access_expires_at is None or access_expires_at > now)
        session_expires_at = access_expires_at

    return {
        "endpoint": backend_url(),
        "authenticated": authenticated,
        "checked_with_backend": False,
        "auth_mode": auth_mode,
        "username": (tokens.get("username") or "").strip() or None,
        "source": bootstrap.credential_source(),
        "storage": auth_persistence_label(),
        "store_error": store_read_error(),
        "session_expires_at": session_expires_at,
        "access_expires_at": access_expires_at,
    }


_SECURITY_PROGRAM = "/usr/bin/security"
_SECURITY_ITEM_NOT_FOUND = 44
_SECURITY_TIMEOUT_SECONDS = 10
_SECURITY_RETRY_AFTER_SECONDS = 60
# `security -i` reads one command per line into a 4,096-character buffer. It cuts
# a longer line there: the first part runs with a truncated secret and the rest
# runs as another command. Stay below the buffer instead of storing half a record.
_SECURITY_STDIN_LINE_LIMIT = 4000
_SECURITY_SAFE_NAME = re.compile(r"[A-Za-z0-9._@-]+")
# The comment attribute of every entry this code writes. Attributes are readable
# without consent, the secret is not: the mark tells, before the secret is asked
# for, that `security` wrote the entry and may read it without a dialog.
_SECURITY_ENTRY_MARK = "MainSequenceCLI.session.v1"
_SECURITY_COMMENT_ATTRIBUTE = re.compile(r'^\s*"icmt"<blob>="([^"]*)"\s*$', re.MULTILINE)
_MAX_DUPLICATE_ENTRIES = 16


class _MacOSKeychain:
    """
    The login Keychain, through Apple's `security` program.

    The Keychain grants access per program. An entry written through the Security
    framework belongs to the interpreter that wrote it; every other interpreter
    build, and the same one after an upgrade, blocks on a consent dialog when it
    reads that entry. `security` is one program for every interpreter, so a login
    made from one CodeRepository is readable from another without a dialog. The
    cost is that any program of the same user can read the entry the same way.

    Asking `security` for the secret of an entry that another program wrote shows
    that dialog too, in every process that starts. The secret is therefore asked
    for only when the entry carries this code's mark in its comment attribute,
    which is read without consent. Any other entry is left alone until a login
    replaces it.

    The secret reaches `security` on standard input, never on a command line.
    """

    name = "macOS Keychain"

    # What this process learned about entries, by (service, account). An entry
    # `security` read is one it may update in place. An entry that could not be
    # read is not asked for again for a while: one command would otherwise repeat
    # the same calls, or the same wait, once per read.
    _readable: set[tuple[str, str]] = set()
    _unreadable: dict[tuple[str, str], tuple[str, float]] = {}

    @staticmethod
    def available() -> bool:
        return os.access(_SECURITY_PROGRAM, os.X_OK)

    @staticmethod
    def _run(arguments: list[str], *, stdin: str | None = None) -> subprocess.CompletedProcess:
        try:
            return subprocess.run(
                [_SECURITY_PROGRAM, *arguments],
                input=stdin,
                stdin=subprocess.DEVNULL if stdin is None else None,
                capture_output=True,
                text=True,
                check=False,
                timeout=_SECURITY_TIMEOUT_SECONDS,
            )
        except subprocess.TimeoutExpired as exc:
            raise keyring_errors.KeyringError(
                f"The Keychain did not answer within {_SECURITY_TIMEOUT_SECONDS} seconds. "
                "It was waiting for the user: the entry belongs to another program, "
                "or the Keychain is locked."
            ) from exc
        except OSError as exc:
            raise keyring_errors.KeyringError(
                f"The `security` program could not be run ({type(exc).__name__})."
            ) from exc

    def get_password(self, service: str, account: str) -> str | None:
        key = (service, account)
        remembered = self._unreadable.get(key)
        if remembered is not None:
            if time.monotonic() < remembered[1]:
                raise keyring_errors.KeyringError(remembered[0])
            del self._unreadable[key]
        try:
            mark = self._entry_mark(service, account)
            if mark is None:
                self._readable.discard(key)
                return None
            if mark != _SECURITY_ENTRY_MARK:
                raise keyring_errors.KeyringError(
                    "The Keychain entry was written by another version of the CLI. "
                    "Asking for it would show a consent dialog in every process, "
                    "so it is not read."
                )
            done = self._run(["find-generic-password", "-s", service, "-a", account, "-w"])
            if done.returncode not in (0, _SECURITY_ITEM_NOT_FOUND):
                raise keyring_errors.KeyringError(
                    f"The Keychain entry could not be read (security exit {done.returncode})."
                )
        except keyring_errors.KeyringError as exc:
            self._readable.discard(key)
            self._unreadable[key] = (str(exc), time.monotonic() + _SECURITY_RETRY_AFTER_SECONDS)
            raise
        if done.returncode == _SECURITY_ITEM_NOT_FOUND:
            self._readable.discard(key)
            return None
        self._readable.add(key)
        return _decode_security_secret(done.stdout)

    def _entry_mark(self, service: str, account: str) -> str | None:
        """
        Return the comment attribute of an entry, or None when there is no entry.

        Only attributes are asked for, never the secret, so this shows no dialog
        whoever wrote the entry.
        """
        done = self._run(["find-generic-password", "-s", service, "-a", account])
        if done.returncode == _SECURITY_ITEM_NOT_FOUND:
            return None
        if done.returncode != 0:
            raise keyring_errors.KeyringError(
                f"The Keychain entry could not be inspected (security exit {done.returncode})."
            )
        comment = _SECURITY_COMMENT_ATTRIBUTE.search(done.stdout)
        return comment.group(1) if comment else ""

    def set_password(self, service: str, account: str, password: str) -> None:
        if not (_SECURITY_SAFE_NAME.fullmatch(service) and _SECURITY_SAFE_NAME.fullmatch(account)):
            raise keyring_errors.PasswordSetError("Unsupported Keychain entry name.")
        line = (
            f"add-generic-password -U -s {service} -a {account} "
            f"-j {_SECURITY_ENTRY_MARK} -X {password.encode('utf-8').hex()}\n"
        )
        if len(line) > _SECURITY_STDIN_LINE_LIMIT:
            raise keyring_errors.PasswordSetError(
                "The session record is too long to store through the Keychain helper."
            )
        key = (service, account)
        # An entry `security` read in this process is updated in place, so another
        # process never finds it missing.
        if key in self._readable:
            try:
                if self._run(["-i"], stdin=line).returncode == 0:
                    return
            except keyring_errors.KeyringError:
                pass
            self._readable.discard(key)
        # Any other entry is removed first. Updating one that another program
        # created would make `security` wait for consent; removing it does not,
        # and the new entry then belongs to `security` and carries the mark.
        self._delete(service, account)
        done = self._run(["-i"], stdin=line)
        # Never report this call's output: on a parse error it echoes the line.
        if done.returncode != 0:
            raise keyring_errors.PasswordSetError(
                f"The Keychain entry could not be written (security exit {done.returncode})."
            )
        self._readable.add(key)

    def delete_password(self, service: str, account: str) -> None:
        if not self._delete(service, account):
            raise keyring_errors.PasswordDeleteError("No Keychain entry to delete.")

    def _delete(self, service: str, account: str) -> bool:
        done = self._run(["delete-generic-password", "-s", service, "-a", account])
        if done.returncode == _SECURITY_ITEM_NOT_FOUND:
            self._forget(service, account)
            return False
        if done.returncode != 0:
            raise keyring_errors.KeyringError(
                f"The Keychain entry could not be deleted (security exit {done.returncode})."
            )
        self._forget(service, account)
        return True

    def _forget(self, service: str, account: str) -> None:
        self._readable.discard((service, account))
        self._unreadable.pop((service, account), None)


def _decode_security_secret(output: str) -> str:
    """
    Return the secret `security find-generic-password -w` printed.

    It prints printable ASCII as it is and anything else as hexadecimal. The
    session record is JSON, so a value made only of hexadecimal digits is the
    second form.
    """
    value = output[:-1] if output.endswith("\n") else output
    if value and len(value) % 2 == 0 and re.fullmatch(r"[0-9a-fA-F]+", value):
        try:
            return bytes.fromhex(value).decode("utf-8")
        except (ValueError, UnicodeDecodeError):
            return value
    return value


class _SecretServiceStore:
    """
    Secret Service, through the keyring library's backend for it.

    The backend is named explicitly instead of taking the library's
    highest-priority one, so the record is in the same store on every Linux
    desktop. The library replaces its own item in place, so another process never
    finds the record missing during a write. It knows its own item by an
    `application` attribute: an item for the same service and account that
    another program stored would stay next to the new one and make the record
    ambiguous. Writing therefore removes such items first, and deleting removes
    every match.
    """

    name = "Secret Service"

    def __init__(self, backend) -> None:
        self._backend = backend

    def get_password(self, service: str, account: str) -> str | None:
        return self._backend.get_password(service, account)

    def set_password(self, service: str, account: str, password: str) -> None:
        self._remove_items_of_other_programs(service, account)
        self._backend.set_password(service, account, password)

    def delete_password(self, service: str, account: str) -> None:
        if not self._remove_every_match(service, account):
            raise keyring_errors.PasswordDeleteError("No Secret Service entry to delete.")

    def _remove_every_match(self, service: str, account: str) -> int:
        removed = 0
        for _ in range(_MAX_DUPLICATE_ENTRIES):
            try:
                self._backend.delete_password(service, account)
            except keyring_errors.PasswordDeleteError:
                break
            removed += 1
        return removed

    def _remove_items_of_other_programs(self, service: str, account: str) -> None:
        try:
            collection = self._backend.get_preferred_collection()
            with contextlib.closing(collection.connection):
                for item in collection.search_items({"username": account, "service": service}):
                    if item.get_attributes().get("application") == self._backend.appid:
                        continue
                    self._backend.unlock(item)
                    item.delete()
        except keyring_errors.KeyringError:
            raise
        except Exception as exc:
            # The Secret Service client raises its own errors for a lost bus or a
            # dismissed prompt.
            raise keyring_errors.KeyringError(
                f"Secret Service items could not be cleaned up ({type(exc).__name__})."
            ) from exc


def _secret_service_store():
    try:
        from keyring.backends import SecretService

        # The priority property raises when no Secret Service is reachable.
        if float(SecretService.Keyring.priority) < 1:
            return None
        return _SecretServiceStore(SecretService.Keyring())
    except Exception:
        return None


def _recommended_keyring_backend():
    """
    Return the keyring library's recommended backend, never a plaintext fallback.
    """
    try:
        keyring_core.init_backend(limit=keyring_core.recommended)
        backend = keyring.get_keyring()
        if float(backend.priority) < 1:
            return None
        return backend
    except Exception:
        return None


def _secure_keyring_backend():
    """
    Return this system's credential store, never a plaintext fallback.

    macOS and Linux each use one named store, for the reasons given on the two
    classes above. Every other system, Windows included, keeps the keyring
    library's recommended backend.
    """
    if sys.platform == "darwin":
        return _MacOSKeychain() if _MacOSKeychain.available() else None
    if sys.platform.startswith("linux"):
        return _secret_service_store()
    return _recommended_keyring_backend()


def _token_record(*, username: str, access: str, refresh: str, backend: str | None = None) -> str:
    """
    Serialize one session record.

    The record names its backend, so a reader can refuse an entry that is not the
    one it asked for. `json.dumps` escapes non-ASCII, so every store and every
    reader sees the same bytes.
    """
    return json.dumps(
        {
            "v": AUTH_RECORD_VERSION,
            "backend": _auth_backend_key(backend),
            "username": username or "",
            "access": access or "",
            "refresh": refresh or "",
        }
    )


_store_read_error: str | None = None


def store_read_error() -> str | None:
    """
    Say why the last read of the credential store failed, or None when it did not.

    A store that cannot be read looks like a machine with no saved session. This
    keeps the two apart for `mainsequence doctor` and `mainsequence auth status`.
    The text never contains a credential.
    """
    return _store_read_error


def _read_secure_tokens(backend: str | None = None) -> dict:
    """
    Read the session record for one backend from the OS credential store.
    """
    global _store_read_error

    _store_read_error = None
    secure_backend = _secure_keyring_backend()
    if secure_backend is None:
        return {}
    try:
        backend_key = _auth_backend_key(backend)
        accounts = [_keychain_account_for_backend(backend)]
        if KEYCHAIN_ACCOUNT not in accounts:
            accounts.append(KEYCHAIN_ACCOUNT)
        for account in accounts:
            raw = secure_backend.get_password(KEYCHAIN_SERVICE, account)
            if not raw:
                continue
            record = json.loads(raw)
            if not isinstance(record, dict):
                continue
            # Records written before the backend was part of the record carry none.
            recorded_backend = str(record.get("backend") or "").strip()
            if recorded_backend and normalize_backend_url(recorded_backend) != backend_key:
                continue
            payload = _normalize_token_payload(record)
            if payload.get("access") or payload.get("refresh"):
                return payload
        return {}
    except keyring_errors.KeyringError as exc:
        _store_read_error = str(exc) or type(exc).__name__
        return {}
    except (json.JSONDecodeError, TypeError, ValueError):
        _store_read_error = "The saved session record is not valid."
        return {}


def _write_secure_tokens(
    *, username: str, access: str, refresh: str, backend: str | None = None
) -> bool:
    """
    Persist the session record for one backend in the OS credential store.
    """
    secure_backend = _secure_keyring_backend()
    if secure_backend is None:
        return False
    payload = _token_record(username=username, access=access, refresh=refresh, backend=backend)
    try:
        secure_backend.set_password(
            KEYCHAIN_SERVICE,
            _keychain_account_for_backend(backend),
            payload,
        )
        return True
    except keyring_errors.KeyringError:
        return False


def _clear_secure_tokens(backend: str | None = None) -> bool:
    """
    Delete persisted tokens from the OS credential store. Missing state is success.
    """
    secure_backend = _secure_keyring_backend()
    if secure_backend is None:
        return True
    try:
        accounts = [_keychain_account_for_backend(backend)]
        if KEYCHAIN_ACCOUNT not in accounts:
            accounts.append(KEYCHAIN_ACCOUNT)

        ok = True
        for account in accounts:
            try:
                secure_backend.delete_password(KEYCHAIN_SERVICE, account)
            except keyring_errors.PasswordDeleteError:
                pass
            except keyring_errors.KeyringError:
                ok = False
        return ok
    except keyring_errors.KeyringError:
        return False


def _migrate_local_tokens_to_secure_store(backend: str | None = None) -> dict:
    """
    Move legacy auth.json credentials into secure storage when available.

    The legacy file is never used as an ongoing authentication source. If the
    platform has no secure credential backend, it remains untouched and the CLI
    behaves as unauthenticated outside the current process.
    """
    legacy = _read_local_tokens(backend)
    if not legacy.get("access") or _secure_keyring_backend() is None:
        return {}
    if not _write_secure_tokens(
        username=legacy.get("username", ""),
        access=legacy["access"],
        refresh=legacy.get("refresh", ""),
        backend=backend,
    ):
        return {}
    migrated = _read_secure_tokens(backend)
    if migrated.get("access") != legacy["access"]:
        return {}
    if migrated.get("refresh", "") != legacy.get("refresh", ""):
        return {}
    _clear_local_tokens(backend)
    return migrated


def backend_url() -> str:
    """
    Return backend base URL with trailing slash removed.

    Semantics match VS Code extension:
      - Default comes from config.json or DEFAULTS
      - If MAINSEQUENCE_ENDPOINT env var is set (even to empty string),
        it overrides config.json.
    """
    cfg = get_config()
    url = (cfg.get("backend_url") or DEFAULTS["backend_url"]).rstrip("/")

    # If env var exists (even empty string), override (matching extension semantics)
    if os.environ.get(CANONICAL_BACKEND_ENV) is not None:
        url = (os.environ.get(CANONICAL_BACKEND_ENV) or "").rstrip("/")

    return normalize_backend_url(url)
