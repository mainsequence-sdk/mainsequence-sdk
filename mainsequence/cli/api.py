from __future__ import annotations

"""
mainsequence.cli.api
====================

HTTP API wrapper for MainSequence CLI.

This module is intentionally aligned with the VS Code extension implementation:
- Token login + refresh
- authed() retries after refresh on 401
- Project helpers: list projects, get env text, deploy key, project token

Any behavioral differences vs the VS Code extension should be considered bugs.
"""

import json
import os
import re
from typing import Any
from urllib.parse import urlencode

import requests

from .config import backend_url, get_tokens, save_tokens, set_env_access

AUTH_PATHS = {
    "obtain": "/auth/jwt-token/token/",
    "refresh": "/auth/jwt-token/token/refresh/",
    "ping": "/auth/rest-auth/user/",
}

S = requests.Session()
S.headers.update({"Content-Type": "application/json"})


class ApiError(RuntimeError):
    """Base error for API failures."""


class NotLoggedIn(ApiError):
    """Raised when auth is missing/expired and refresh fails."""


def _full(path: str) -> str:
    """Return fully-qualified URL for a backend-relative path."""
    p = "/" + path.lstrip("/")
    return backend_url() + p


def _normalize_api_path(p: str) -> str:
    """
    Only allow calls to known API namespaces to avoid accidental SSRF/path misuse.

    Allowed prefixes:
        /api, /auth, /pods, /orm, /user
    """
    p = "/" + (p or "").lstrip("/")
    if not re.match(r"^/(api|auth|pods|orm|user)(/|$)", p):
        raise ApiError("Only /api/*, /auth/*, /pods/*, /orm/*, /user/* allowed")
    return p


def _access_token() -> str | None:
    """Return access token from session environment."""
    t = os.environ.get("MAIN_SEQUENCE_USER_TOKEN")
    if t:
        return t
    tok = get_tokens()
    return tok.get("access")


def _refresh_token() -> str | None:
    """Return refresh token from session environment."""
    tok = get_tokens()
    return tok.get("refresh")


def login(email: str, password: str) -> dict:
    """
    Authenticate and store session tokens in process environment.

    Args:
        email: login email (server expects 'email' field)
        password: password

    Returns:
        dict: {"username": email, "backend": backend_url(), "access": "...", "refresh": "...", "persisted": bool}
    """
    email = (email or "").strip()
    password = (password or "").rstrip("\r\n")

    url = _full(AUTH_PATHS["obtain"])
    payload = {"email": email, "password": password}
    r = S.post(url, data=json.dumps(payload))
    try:
        data = r.json()
    except Exception:
        data = {}

    if not r.ok:
        msg = data.get("detail") or data.get("message") or r.text
        raise ApiError(f"{msg}")

    access = data.get("access") or data.get("token") or data.get("jwt") or data.get("access_token")
    refresh = data.get("refresh") or data.get("refresh_token")
    if not access or not refresh:
        raise ApiError("Server did not return expected tokens.")

    persisted = save_tokens(email, access, refresh)
    set_env_access(access)
    return {"username": email, "backend": backend_url(), "access": access, "refresh": refresh, "persisted": bool(persisted)}


def refresh_access() -> str:
    """
    Use refresh token to obtain a new access token and update session env.

    Raises:
        NotLoggedIn: if refresh is missing or refresh fails
    """
    refresh = _refresh_token()
    if not refresh:
        raise NotLoggedIn("Not logged in. Run `mainsequence login <email>`.")

    r = S.post(_full(AUTH_PATHS["refresh"]), data=json.dumps({"refresh": refresh}))
    data = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
    if not r.ok:
        raise NotLoggedIn(data.get("detail") or "Token refresh failed.")

    access = data.get("access")
    if not access:
        raise NotLoggedIn("Refresh succeeded but no access token returned.")

    tokens = get_tokens()
    save_tokens(tokens.get("username") or "", access, refresh)
    set_env_access(access)
    return access


def authed(method: str, api_path: str, body: dict | None = None) -> requests.Response:
    """
    Perform an authenticated request with automatic refresh on 401.

    Args:
        method: HTTP method string
        api_path: backend path (must be in allowed namespaces)
        body: JSON body (for non-GET/HEAD)

    Returns:
        requests.Response

    Raises:
        NotLoggedIn: if auth fails even after refresh
    """
    api_path = _normalize_api_path(api_path)
    access = _access_token()
    if not access:
        access = refresh_access()

    headers = {"Authorization": f"Bearer {access}"}
    r = S.request(
        method.upper(),
        _full(api_path),
        headers=headers,
        data=None if method.upper() in {"GET", "HEAD"} else json.dumps(body or {}),
    )
    if r.status_code == 401:
        access = refresh_access()
        headers = {"Authorization": f"Bearer {access}"}
        r = S.request(
            method.upper(),
            _full(api_path),
            headers=headers,
            data=None if method.upper() in {"GET", "HEAD"} else json.dumps(body or {}),
        )
    if r.status_code == 401:
        raise NotLoggedIn("Not logged in.")
    return r


# ---------- Helper APIs (parity with VS Code extension) ----------


def safe_slug(s: str) -> str:
    """Return a filesystem-safe slug (max 64 chars)."""
    x = re.sub(r"[^a-z0-9-_]+", "-", (s or "project").lower()).strip("-")
    return x[:64] or "project"


def repo_name_from_git_url(url: str | None) -> str | None:
    """Extract a repo name from a git URL and normalize it for filesystem use."""
    if not url:
        return None
    s = re.sub(r"[?#].*$", "", url.strip())
    last = s.split("/")[-1] if "/" in s else s
    if last.lower().endswith(".git"):
        last = last[:-4]
    return re.sub(r"[^A-Za-z0-9._-]+", "-", last)


def deep_find_repo_url(extra) -> str | None:
    """Recursively scan a dict for plausible git SSH URLs."""
    if not isinstance(extra, dict):
        return None
    cand = ["ssh_url", "git_ssh_url", "repo_ssh_url", "git_url", "repo_url", "repository", "url"]
    for k in cand:
        v = extra.get(k)
        if isinstance(v, str) and (v.startswith("git@") or re.search(r"\.git($|\?)", v)):
            return v
        if isinstance(v, dict):
            for vv in v.values():
                if isinstance(vv, str) and (vv.startswith("git@") or re.search(r"\.git($|\?)", vv)):
                    return vv
    for v in extra.values():
        if isinstance(v, dict):
            found = deep_find_repo_url(v)
            if found:
                return found
    return None


def _format_env_value(value: Any) -> str:
    """
    Format arbitrary env values into strings, matching the VS Code extension behavior.
    """
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float, bool)):
        return str(value)
    try:
        return json.dumps(value)
    except Exception:
        return str(value)


def get_current_user_profile() -> dict:
    """
    Return current user profile (username + organization name) via backend endpoints.

    Returns:
        dict: {"username": "...", "organization": "..."} or {}
    """
    who = authed("GET", AUTH_PATHS["ping"])
    d = who.json() if who.ok else {}
    uid = d.get("id") or d.get("pk") or (d.get("user") or {}).get("id") or d.get("user_id")
    if not uid:
        return {}
    full = authed("GET", f"/user/api/user/{uid}/")
    u = full.json() if full.ok else {}
    org_name = (u.get("organization") or {}).get("name") or u.get("organization_name") or ""
    return {"username": u.get("username") or "", "organization": org_name}


def get_projects() -> list[dict]:
    """
    List projects visible to the current user.

    Returns:
        list[dict]: project objects (API-dependent shape)

    Raises:
        ApiError: on non-200 response
    """
    r = authed("GET", "/orm/api/pods/projects/")
    if not r.ok:
        raise ApiError(f"Projects fetch failed ({r.status_code}).")
    data = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
    if isinstance(data, list):
        return data
    return data.get("results") or []


def get_project(project_id: int | str) -> dict:
    """
    Fetch a single project by id.
    """
    r = authed("GET", f"/orm/api/pods/projects/{project_id}/")
    if not r.ok:
        msg = r.text or ""
        try:
            if "application/json" in (r.headers.get("content-type") or "").lower():
                data = r.json()
                msg = data.get("detail") or data.get("message") or msg
        except Exception:
            pass
        raise ApiError(f"Project fetch failed ({r.status_code}). {msg}".strip())

    if not r.headers.get("content-type", "").startswith("application/json"):
        raise ApiError(
            f"Project fetch response was not JSON (content-type: {r.headers.get('content-type')})."
        )
    data = r.json()
    if not isinstance(data, dict):
        raise ApiError("Project fetch response had unexpected payload shape.")
    return data


def _json_results(r: requests.Response) -> list[dict]:
    """
    Return list-like API payloads for DRF list endpoints.
    """
    data = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        results = data.get("results")
        if isinstance(results, list):
            return results
    return []


def list_dynamic_table_data_sources(status: str | None = "AVAILABLE") -> list[dict]:
    """
    List DynamicTableDataSource rows (optionally filtered by related resource status).
    """
    query = ""
    if status:
        query = "?" + urlencode({"related_resource__status": status})
    r = authed("GET", f"/orm/api/ts_manager/dynamic_table_data_source/{query}")
    if not r.ok:
        raise ApiError(f"Data sources fetch failed ({r.status_code}).")
    return _json_results(r)


def list_project_base_images() -> list[dict]:
    """
    List available ProjectBaseImage rows.
    """
    r = authed("GET", "/orm/api/pods/project-base-image/")
    if not r.ok:
        raise ApiError(f"Project base images fetch failed ({r.status_code}).")
    return _json_results(r)


def list_github_organizations() -> list[dict]:
    """
    List available GitHub organizations for the current user.
    """
    r = authed("GET", "/orm/api/pods/github-organization/")
    if not r.ok:
        raise ApiError(f"GitHub organizations fetch failed ({r.status_code}).")
    return _json_results(r)


def create_project(
    *,
    project_name: str,
    data_source_id: int | None = None,
    default_base_image_id: int | None = None,
    github_org_id: int | None = None,
    repository_branch: str | None = None,
    env_vars: dict[str, str] | None = None,
) -> dict:
    """
    Create a new project.
    """
    payload: dict[str, Any] = {"project_name": project_name}

    if repository_branch:
        payload["repository_branch"] = repository_branch
    if data_source_id is not None:
        payload["data_source_id"] = int(data_source_id)
    if default_base_image_id is not None:
        payload["default_base_image_id"] = int(default_base_image_id)
    if github_org_id is not None:
        payload["github_org_id"] = int(github_org_id)
    if env_vars:
        payload["env_vars"] = [{"name": k, "value": str(v)} for k, v in env_vars.items()]

    r = authed("POST", "/orm/api/pods/projects/", payload)
    if not r.ok:
        msg = r.text or ""
        try:
            if "application/json" in (r.headers.get("content-type") or "").lower():
                data = r.json()
                msg = data.get("detail") or data.get("message") or msg
        except Exception:
            pass
        raise ApiError(f"Project create failed ({r.status_code}). {msg}".strip())

    if not r.headers.get("content-type", "").startswith("application/json"):
        raise ApiError(
            f"Project create response was not JSON (content-type: {r.headers.get('content-type')})."
        )
    data = r.json()
    if not isinstance(data, dict):
        raise ApiError("Project create response had unexpected payload shape.")
    return data


def delete_project(project_id: int | str, *, delete_repositories: bool = False) -> dict[str, Any] | None:
    """
    Delete a project by id.

    Mirrors backend behavior:
      - DELETE /orm/api/pods/projects/{id}/
      - optional query param delete_repositories=true
    """
    path = f"/orm/api/pods/projects/{project_id}/"
    if delete_repositories:
        path = f"{path}?delete_repositories=true"

    r = authed("DELETE", path)
    if not r.ok:
        msg = r.text or ""
        try:
            if "application/json" in (r.headers.get("content-type") or "").lower():
                data = r.json()
                msg = data.get("detail") or data.get("message") or msg
        except Exception:
            pass
        raise ApiError(f"Project delete failed ({r.status_code}). {msg}".strip())

    if r.content:
        try:
            return r.json()
        except Exception:
            return {"detail": (r.text or "").strip()} if (r.text or "").strip() else None
    return None


def fetch_project_env_text(project_id: int | str) -> str:
    """
    Fetch the project's environment (.env content) and return it as text.

    Parity: VS Code extension supports:
      - JSON containing 'environment'/'env'/'content'/'text'
      - JSON dict of key-values (flattened into KEY=value lines)
      - Raw string response

    Returns:
        str: environment text (possibly empty)
    """
    r = authed("GET", f"/orm/api/pods/projects/{project_id}/get_environment/")
    raw: Any
    if r.headers.get("content-type", "").startswith("application/json"):
        try:
            raw = r.json()
        except Exception:
            raw = {}
    else:
        raw = r.text

    if isinstance(raw, dict):
        embedded = raw.get("environment") or raw.get("env") or raw.get("content") or raw.get("text")
        if isinstance(embedded, str):
            return embedded
        if isinstance(embedded, dict):
            raw = embedded

    if isinstance(raw, dict):
        return "\n".join(f"{k}={_format_env_value(v)}" for k, v in raw.items())

    if isinstance(raw, str):
        return raw

    return ""


def add_deploy_key(project_id: int | str, key_title: str, public_key: str) -> None:
    """
    Add a deploy key for the project.

    Note: The CLI command should treat this as best-effort (like the VS Code extension),
    because failures can happen if the key already exists or permissions differ.
    """
    r = authed(
        "POST",
        f"/orm/api/pods/projects/{project_id}/add_deploy_key/",
        {"key_title": key_title, "public_key": public_key},
    )
    r.raise_for_status()


def get_project_token(project_id: int | str) -> str:
    """
    Fetch the project's token using the current access token.

    Parity: VS Code extension accepts:
      - {"token": "<string>"}
      - {"development": ["<token>", ...]} (legacy/alternate shape)

    Raises:
        NotLoggedIn: if auth fails
        ApiError: for non-OK responses or missing token
    """
    r = authed("GET", f"/orm/api/pods/projects/{project_id}/get_project_token/")
    if not r.ok:
        msg = r.text or ""
        try:
            if "application/json" in (r.headers.get("content-type") or "").lower():
                data = r.json()
                msg = data.get("detail") or data.get("message") or msg
        except Exception:
            pass
        raise ApiError(f"Project token fetch failed ({r.status_code}). {msg}".strip())

    try:
        content_type = (r.headers.get("content-type") or "").lower()
        if "application/json" not in content_type:
            raise ApiError(
                f"Project token response was not JSON (content-type: {r.headers.get('content-type')})."
            )
        data = r.json()
    except ValueError as e:
        raise ApiError(f"Project token response contained invalid JSON: {e}") from e

    token = data.get("token")
    if isinstance(token, str) and token:
        return token

    dev = data.get("development")
    if isinstance(dev, list) and dev and isinstance(dev[0], str) and dev[0]:
        return dev[0]

    raise ApiError("Project token response did not include a valid token.")
