from __future__ import annotations

"""
mainsequence.cli.api
====================

HTTP API wrapper for MainSequence CLI.

This module is intentionally aligned with the VS Code extension implementation:
- Token login + refresh
- authed() retries after refresh on 401
- Project helpers: list projects, get env text, deploy key

Any behavioral differences vs the VS Code extension should be considered bugs.
"""

import importlib
import json
import os
import re
from typing import Any
from urllib.parse import urlencode

import requests

from .config import backend_url, get_tokens, save_tokens

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

    new_refresh = data.get("refresh") or refresh
    tokens = get_tokens()
    save_tokens(tokens.get("username") or "", access, new_refresh)
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


def _sdk_object_to_dict(obj: Any) -> dict[str, Any]:
    if isinstance(obj, dict):
        return dict(obj)
    if hasattr(obj, "model_dump"):
        return obj.model_dump(mode="json")
    return {"id": getattr(obj, "id", None)}


def _run_sdk_model_operation(
    *,
    module_name: str,
    class_name: str,
    operation,
    project_id_env: int | str | None = None,
):
    tokens = get_tokens()
    access = (tokens.get("access") or "").strip()
    refresh = (tokens.get("refresh") or "").strip()
    if not access:
        raise NotLoggedIn("Not logged in.")

    endpoint = backend_url().rstrip("/")
    root_url = f"{endpoint}/orm/api"

    old_env = {
        "MAINSEQUENCE_AUTH_MODE": os.environ.get("MAINSEQUENCE_AUTH_MODE"),
        "MAINSEQUENCE_ACCESS_TOKEN": os.environ.get("MAINSEQUENCE_ACCESS_TOKEN"),
        "MAINSEQUENCE_REFRESH_TOKEN": os.environ.get("MAINSEQUENCE_REFRESH_TOKEN"),
        "TDAG_ENDPOINT": os.environ.get("TDAG_ENDPOINT"),
        "MAINSEQUENCE_ENDPOINT": os.environ.get("MAINSEQUENCE_ENDPOINT"),
        "MAIN_SEQUENCE_PROJECT_ID": os.environ.get("MAIN_SEQUENCE_PROJECT_ID"),
    }

    client_utils = None
    old_provider = None
    old_base_root_url = None
    client_model = None
    old_model_root_url = None

    try:
        os.environ["MAINSEQUENCE_AUTH_MODE"] = "jwt"
        os.environ["MAINSEQUENCE_ACCESS_TOKEN"] = access
        if refresh:
            os.environ["MAINSEQUENCE_REFRESH_TOKEN"] = refresh
        else:
            os.environ.pop("MAINSEQUENCE_REFRESH_TOKEN", None)
        os.environ["TDAG_ENDPOINT"] = endpoint
        os.environ["MAINSEQUENCE_ENDPOINT"] = endpoint
        if project_id_env is not None:
            os.environ["MAIN_SEQUENCE_PROJECT_ID"] = str(project_id_env)
        else:
            os.environ.pop("MAIN_SEQUENCE_PROJECT_ID", None)

        from mainsequence.client import utils as _client_utils
        from mainsequence.client.base import BaseObjectOrm

        module = importlib.import_module(module_name)
        client_model = getattr(module, class_name)

        client_utils = _client_utils
        old_provider = getattr(client_utils.loaders, "provider", None)
        old_base_root_url = BaseObjectOrm.ROOT_URL
        old_model_root_url = getattr(client_model, "ROOT_URL", None)

        client_utils.TDAG_ENDPOINT = endpoint
        client_utils.API_ENDPOINT = root_url
        client_utils.loaders.use_jwt(access=access, refresh=refresh or None)

        BaseObjectOrm.ROOT_URL = root_url
        client_model.ROOT_URL = root_url

        return operation(client_model)

    except Exception as e:
        err_name = type(e).__name__
        if err_name in {"AuthenticationError", "PermissionDeniedError"}:
            raise NotLoggedIn(str(e) or "Not logged in.")
        raise
    finally:
        if client_utils is not None:
            try:
                client_utils.loaders.provider = old_provider
            except Exception:
                pass
        if old_base_root_url is not None:
            try:
                from mainsequence.client.base import BaseObjectOrm

                BaseObjectOrm.ROOT_URL = old_base_root_url
            except Exception:
                pass
        if client_model is not None and old_model_root_url is not None:
            try:
                client_model.ROOT_URL = old_model_root_url
            except Exception:
                pass

        for k, v in old_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
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


def get_logged_user_details() -> dict[str, Any]:
    """
    Return the authenticated user via SDK client `User.get_logged_user()`.

    The CLI does not naturally run inside a request context, so this bridge resolves
    the current user id from the authenticated API session and temporarily binds
    `X-User-ID` into `mainsequence.client.models_user._CURRENT_AUTH_HEADERS`
    before calling the SDK method.
    """
    tokens = get_tokens()
    access = (tokens.get("access") or "").strip()
    refresh = (tokens.get("refresh") or "").strip()
    if not access:
        raise NotLoggedIn("Not logged in.")

    endpoint = backend_url().rstrip("/")
    root_url = f"{endpoint}/orm/api"

    old_env = {
        "MAINSEQUENCE_AUTH_MODE": os.environ.get("MAINSEQUENCE_AUTH_MODE"),
        "MAINSEQUENCE_ACCESS_TOKEN": os.environ.get("MAINSEQUENCE_ACCESS_TOKEN"),
        "MAINSEQUENCE_REFRESH_TOKEN": os.environ.get("MAINSEQUENCE_REFRESH_TOKEN"),
        "TDAG_ENDPOINT": os.environ.get("TDAG_ENDPOINT"),
        "MAINSEQUENCE_ENDPOINT": os.environ.get("MAINSEQUENCE_ENDPOINT"),
    }

    client_utils = None
    old_provider = None
    old_base_root_url = None
    old_user_root_url = None
    headers_token = None
    current_auth_headers = None

    try:
        who = authed("GET", AUTH_PATHS["ping"])
        data = who.json() if who.ok else {}
        user_id = data.get("id") or data.get("pk") or (data.get("user") or {}).get("id") or data.get("user_id")
        if user_id in (None, ""):
            raise ApiError("Could not determine the authenticated user id.")

        os.environ["MAINSEQUENCE_AUTH_MODE"] = "jwt"
        os.environ["MAINSEQUENCE_ACCESS_TOKEN"] = access
        if refresh:
            os.environ["MAINSEQUENCE_REFRESH_TOKEN"] = refresh
        else:
            os.environ.pop("MAINSEQUENCE_REFRESH_TOKEN", None)
        os.environ["TDAG_ENDPOINT"] = endpoint
        os.environ["MAINSEQUENCE_ENDPOINT"] = endpoint

        from mainsequence.client import utils as _client_utils
        from mainsequence.client.base import BaseObjectOrm
        from mainsequence.client.models_user import (
            _CURRENT_AUTH_HEADERS,
        )
        from mainsequence.client.models_user import (
            User as ClientUser,
        )

        client_utils = _client_utils
        current_auth_headers = _CURRENT_AUTH_HEADERS
        old_provider = getattr(client_utils.loaders, "provider", None)
        old_base_root_url = BaseObjectOrm.ROOT_URL
        old_user_root_url = getattr(ClientUser, "ROOT_URL", None)

        client_utils.TDAG_ENDPOINT = endpoint
        client_utils.API_ENDPOINT = root_url
        client_utils.loaders.use_jwt(access=access, refresh=refresh or None)

        BaseObjectOrm.ROOT_URL = root_url
        ClientUser.ROOT_URL = root_url
        headers_token = current_auth_headers.set({"X-User-ID": str(user_id)})

        user = ClientUser.get_logged_user()
        if isinstance(user, dict):
            return user
        if hasattr(user, "model_dump"):
            return user.model_dump()
        return {"id": getattr(user, "id", None)}

    except Exception as e:
        err_name = type(e).__name__
        if err_name in {"AuthenticationError", "PermissionDeniedError"}:
            raise NotLoggedIn(str(e) or "Not logged in.")
        raise ApiError(f"Current user fetch failed: {e}")
    finally:
        if current_auth_headers is not None and headers_token is not None:
            try:
                current_auth_headers.reset(headers_token)
            except Exception:
                pass
        if client_utils is not None:
            try:
                client_utils.loaders.provider = old_provider
            except Exception:
                pass
        if old_base_root_url is not None:
            try:
                from mainsequence.client.base import BaseObjectOrm

                BaseObjectOrm.ROOT_URL = old_base_root_url
            except Exception:
                pass
        if old_user_root_url is not None:
            try:
                from mainsequence.client.models_user import User as ClientUser

                ClientUser.ROOT_URL = old_user_root_url
            except Exception:
                pass

        for k, v in old_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


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


def list_project_users_can_view(
    project_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Fetch the view-access state for a project via `ShareableObjectMixin.can_view()`.
    """
    return _get_shareable_object_access_state(
        module_name="mainsequence.client.models_tdag",
        class_name="Project",
        object_id=project_id,
        accessor_name="can_view",
        timeout=timeout,
    )


def list_project_users_can_edit(
    project_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Fetch the edit-access state for a project via `ShareableObjectMixin.can_edit()`.
    """
    return _get_shareable_object_access_state(
        module_name="mainsequence.client.models_tdag",
        class_name="Project",
        object_id=project_id,
        accessor_name="can_edit",
        timeout=timeout,
    )


def add_project_user_to_view(
    project_id: int | str,
    user_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Grant explicit view access to one user for a project.
    """
    return _mutate_shareable_object_access(
        module_name="mainsequence.client.models_tdag",
        class_name="Project",
        object_id=project_id,
        action_name="add_to_view",
        user_id=user_id,
        timeout=timeout,
    )


def add_project_user_to_edit(
    project_id: int | str,
    user_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Grant explicit edit access to one user for a project.
    """
    return _mutate_shareable_object_access(
        module_name="mainsequence.client.models_tdag",
        class_name="Project",
        object_id=project_id,
        action_name="add_to_edit",
        user_id=user_id,
        timeout=timeout,
    )


def remove_project_user_from_view(
    project_id: int | str,
    user_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Remove explicit view access from one user for a project.
    """
    return _mutate_shareable_object_access(
        module_name="mainsequence.client.models_tdag",
        class_name="Project",
        object_id=project_id,
        action_name="remove_from_view",
        user_id=user_id,
        timeout=timeout,
    )


def remove_project_user_from_edit(
    project_id: int | str,
    user_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Remove explicit edit access from one user for a project.
    """
    return _mutate_shareable_object_access(
        module_name="mainsequence.client.models_tdag",
        class_name="Project",
        object_id=project_id,
        action_name="remove_from_edit",
        user_id=user_id,
        timeout=timeout,
    )


def get_project_data_node_updates(project_id: int | str, *, timeout: int | None = None) -> list[dict[str, Any]]:
    """
    Fetch project data node updates via SDK client model.

    Single source of truth:
      - delegates response parsing to `Project.get_data_nodes_updates()`
      - avoids duplicating payload-shape logic in the CLI API wrapper
    """
    tokens = get_tokens()
    access = (tokens.get("access") or "").strip()
    refresh = (tokens.get("refresh") or "").strip()
    if not access:
        raise NotLoggedIn("Not logged in.")

    endpoint = backend_url().rstrip("/")
    root_url = f"{endpoint}/orm/api"

    old_env = {
        "MAINSEQUENCE_AUTH_MODE": os.environ.get("MAINSEQUENCE_AUTH_MODE"),
        "MAINSEQUENCE_ACCESS_TOKEN": os.environ.get("MAINSEQUENCE_ACCESS_TOKEN"),
        "MAINSEQUENCE_REFRESH_TOKEN": os.environ.get("MAINSEQUENCE_REFRESH_TOKEN"),
        "TDAG_ENDPOINT": os.environ.get("TDAG_ENDPOINT"),
        "MAINSEQUENCE_ENDPOINT": os.environ.get("MAINSEQUENCE_ENDPOINT"),
        "MAIN_SEQUENCE_PROJECT_ID": os.environ.get("MAIN_SEQUENCE_PROJECT_ID"),
    }

    client_utils = None
    old_provider = None
    old_base_root_url = None
    old_project_root_url = None

    try:
        # Configure client auth/runtime to use JWT credentials from CLI login.
        os.environ["MAINSEQUENCE_AUTH_MODE"] = "jwt"
        os.environ["MAINSEQUENCE_ACCESS_TOKEN"] = access
        if refresh:
            os.environ["MAINSEQUENCE_REFRESH_TOKEN"] = refresh
        else:
            os.environ.pop("MAINSEQUENCE_REFRESH_TOKEN", None)
        os.environ["TDAG_ENDPOINT"] = endpoint
        os.environ["MAINSEQUENCE_ENDPOINT"] = endpoint
        os.environ["MAIN_SEQUENCE_PROJECT_ID"] = str(project_id)

        from mainsequence.client import utils as _client_utils
        from mainsequence.client.base import BaseObjectOrm
        from mainsequence.client.models_tdag import Project as ClientProject

        client_utils = _client_utils
        old_provider = getattr(client_utils.loaders, "provider", None)
        old_base_root_url = BaseObjectOrm.ROOT_URL
        old_project_root_url = getattr(ClientProject, "ROOT_URL", None)

        client_utils.TDAG_ENDPOINT = endpoint
        client_utils.API_ENDPOINT = root_url
        client_utils.loaders.use_jwt(access=access, refresh=refresh or None)

        BaseObjectOrm.ROOT_URL = root_url
        ClientProject.ROOT_URL = root_url

        project = ClientProject.get(pk=project_id, timeout=timeout)
        updates = project.get_data_nodes_updates(timeout=timeout)

        out: list[dict[str, Any]] = []
        for u in updates:
            if isinstance(u, dict):
                out.append(u)
            elif hasattr(u, "model_dump"):
                out.append(u.model_dump())
            else:
                out.append({"id": getattr(u, "id", None)})
        return out

    except Exception as e:
        # Delay class references to after import path above.
        err_name = type(e).__name__
        if err_name in {"AuthenticationError", "PermissionDeniedError"}:
            raise NotLoggedIn(str(e) or "Not logged in.")
        if err_name == "NotFoundError":
            raise ApiError(f"Project not found: {project_id}")
        raise ApiError(f"Data node updates fetch failed: {e}")
    finally:
        if client_utils is not None:
            try:
                client_utils.loaders.provider = old_provider
            except Exception:
                pass
        if old_base_root_url is not None:
            try:
                from mainsequence.client.base import BaseObjectOrm

                BaseObjectOrm.ROOT_URL = old_base_root_url
            except Exception:
                pass
        if old_project_root_url is not None:
            try:
                from mainsequence.client.models_tdag import Project as ClientProject

                ClientProject.ROOT_URL = old_project_root_url
            except Exception:
                pass

        for k, v in old_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def sync_project_after_commit(project_id: int | str, *, timeout: int | None = None) -> dict[str, Any] | None:
    """
    Notify the backend that a project commit has been pushed.

    Single source of truth:
      - delegates request behavior and payload parsing to `Project.sync_project_after_commit()`
    """
    tokens = get_tokens()
    access = (tokens.get("access") or "").strip()
    refresh = (tokens.get("refresh") or "").strip()
    if not access:
        raise NotLoggedIn("Not logged in.")

    endpoint = backend_url().rstrip("/")
    root_url = f"{endpoint}/orm/api"

    old_env = {
        "MAINSEQUENCE_AUTH_MODE": os.environ.get("MAINSEQUENCE_AUTH_MODE"),
        "MAINSEQUENCE_ACCESS_TOKEN": os.environ.get("MAINSEQUENCE_ACCESS_TOKEN"),
        "MAINSEQUENCE_REFRESH_TOKEN": os.environ.get("MAINSEQUENCE_REFRESH_TOKEN"),
        "TDAG_ENDPOINT": os.environ.get("TDAG_ENDPOINT"),
        "MAINSEQUENCE_ENDPOINT": os.environ.get("MAINSEQUENCE_ENDPOINT"),
        "MAIN_SEQUENCE_PROJECT_ID": os.environ.get("MAIN_SEQUENCE_PROJECT_ID"),
    }

    client_utils = None
    old_provider = None
    old_base_root_url = None
    old_project_root_url = None

    try:
        os.environ["MAINSEQUENCE_AUTH_MODE"] = "jwt"
        os.environ["MAINSEQUENCE_ACCESS_TOKEN"] = access
        if refresh:
            os.environ["MAINSEQUENCE_REFRESH_TOKEN"] = refresh
        else:
            os.environ.pop("MAINSEQUENCE_REFRESH_TOKEN", None)
        os.environ["TDAG_ENDPOINT"] = endpoint
        os.environ["MAINSEQUENCE_ENDPOINT"] = endpoint
        os.environ["MAIN_SEQUENCE_PROJECT_ID"] = str(project_id)

        from mainsequence.client import utils as _client_utils
        from mainsequence.client.base import BaseObjectOrm
        from mainsequence.client.models_tdag import Project as ClientProject

        client_utils = _client_utils
        old_provider = getattr(client_utils.loaders, "provider", None)
        old_base_root_url = BaseObjectOrm.ROOT_URL
        old_project_root_url = getattr(ClientProject, "ROOT_URL", None)

        client_utils.TDAG_ENDPOINT = endpoint
        client_utils.API_ENDPOINT = root_url
        client_utils.loaders.use_jwt(access=access, refresh=refresh or None)

        BaseObjectOrm.ROOT_URL = root_url
        ClientProject.ROOT_URL = root_url

        payload = ClientProject.sync_project_after_commit(int(project_id), timeout=timeout)
        if payload is None:
            return None
        if isinstance(payload, dict):
            return payload
        if hasattr(payload, "model_dump"):
            return payload.model_dump()
        return {"id": getattr(payload, "id", None)}

    except Exception as e:
        err_name = type(e).__name__
        if err_name in {"AuthenticationError", "PermissionDeniedError"}:
            raise NotLoggedIn(str(e) or "Not logged in.")
        if err_name == "NotFoundError":
            raise ApiError(f"Project not found: {project_id}")
        raise ApiError(f"Project post-commit sync failed: {e}")
    finally:
        if client_utils is not None:
            try:
                client_utils.loaders.provider = old_provider
            except Exception:
                pass
        if old_base_root_url is not None:
            try:
                from mainsequence.client.base import BaseObjectOrm

                BaseObjectOrm.ROOT_URL = old_base_root_url
            except Exception:
                pass
        if old_project_root_url is not None:
            try:
                from mainsequence.client.models_tdag import Project as ClientProject

                ClientProject.ROOT_URL = old_project_root_url
            except Exception:
                pass

        for k, v in old_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def create_project_image(
    *,
    project_repo_hash: str,
    related_project_id: int | str,
    base_image_id: int | None = None,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Create a project image via SDK client model.

    Single source of truth:
      - delegates payload construction and request behavior to `ProjectImage.create()`
      - avoids duplicating the endpoint contract in the CLI API wrapper
    """
    tokens = get_tokens()
    access = (tokens.get("access") or "").strip()
    refresh = (tokens.get("refresh") or "").strip()
    if not access:
        raise NotLoggedIn("Not logged in.")

    endpoint = backend_url().rstrip("/")
    root_url = f"{endpoint}/orm/api"

    old_env = {
        "MAINSEQUENCE_AUTH_MODE": os.environ.get("MAINSEQUENCE_AUTH_MODE"),
        "MAINSEQUENCE_ACCESS_TOKEN": os.environ.get("MAINSEQUENCE_ACCESS_TOKEN"),
        "MAINSEQUENCE_REFRESH_TOKEN": os.environ.get("MAINSEQUENCE_REFRESH_TOKEN"),
        "TDAG_ENDPOINT": os.environ.get("TDAG_ENDPOINT"),
        "MAINSEQUENCE_ENDPOINT": os.environ.get("MAINSEQUENCE_ENDPOINT"),
        "MAIN_SEQUENCE_PROJECT_ID": os.environ.get("MAIN_SEQUENCE_PROJECT_ID"),
    }

    client_utils = None
    old_provider = None
    old_base_root_url = None
    old_image_root_url = None

    try:
        os.environ["MAINSEQUENCE_AUTH_MODE"] = "jwt"
        os.environ["MAINSEQUENCE_ACCESS_TOKEN"] = access
        if refresh:
            os.environ["MAINSEQUENCE_REFRESH_TOKEN"] = refresh
        else:
            os.environ.pop("MAINSEQUENCE_REFRESH_TOKEN", None)
        os.environ["TDAG_ENDPOINT"] = endpoint
        os.environ["MAINSEQUENCE_ENDPOINT"] = endpoint
        os.environ["MAIN_SEQUENCE_PROJECT_ID"] = str(related_project_id)

        from mainsequence.client import utils as _client_utils
        from mainsequence.client.base import BaseObjectOrm
        from mainsequence.client.models_tdag import ProjectImage as ClientProjectImage

        client_utils = _client_utils
        old_provider = getattr(client_utils.loaders, "provider", None)
        old_base_root_url = BaseObjectOrm.ROOT_URL
        old_image_root_url = getattr(ClientProjectImage, "ROOT_URL", None)

        client_utils.TDAG_ENDPOINT = endpoint
        client_utils.API_ENDPOINT = root_url
        client_utils.loaders.use_jwt(access=access, refresh=refresh or None)

        BaseObjectOrm.ROOT_URL = root_url
        ClientProjectImage.ROOT_URL = root_url

        created = ClientProjectImage.create(
            project_repo_hash=project_repo_hash,
            related_project_id=int(related_project_id),
            base_image_id=base_image_id,
            timeout=timeout,
        )
        if isinstance(created, dict):
            return created
        if hasattr(created, "model_dump"):
            return created.model_dump()
        return {"id": getattr(created, "id", None)}

    except Exception as e:
        err_name = type(e).__name__
        if err_name in {"AuthenticationError", "PermissionDeniedError"}:
            raise NotLoggedIn(str(e) or "Not logged in.")
        if err_name == "NotFoundError":
            raise ApiError(f"Project not found: {related_project_id}")
        raise ApiError(f"Project image create failed: {e}")
    finally:
        if client_utils is not None:
            try:
                client_utils.loaders.provider = old_provider
            except Exception:
                pass
        if old_base_root_url is not None:
            try:
                from mainsequence.client.base import BaseObjectOrm

                BaseObjectOrm.ROOT_URL = old_base_root_url
            except Exception:
                pass
        if old_image_root_url is not None:
            try:
                from mainsequence.client.models_tdag import ProjectImage as ClientProjectImage

                ClientProjectImage.ROOT_URL = old_image_root_url
            except Exception:
                pass

        for k, v in old_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def list_project_images(
    *,
    related_project_id: int | str,
    filters: dict[str, Any] | None = None,
    timeout: int | None = None,
) -> list[dict[str, Any]]:
    """
    List project images for a project via SDK client model.

    Single source of truth:
      - delegates filtering and payload parsing to `ProjectImage.filter()`
      - avoids duplicating filter endpoint behavior in the CLI API wrapper
    """
    tokens = get_tokens()
    access = (tokens.get("access") or "").strip()
    refresh = (tokens.get("refresh") or "").strip()
    if not access:
        raise NotLoggedIn("Not logged in.")

    endpoint = backend_url().rstrip("/")
    root_url = f"{endpoint}/orm/api"

    old_env = {
        "MAINSEQUENCE_AUTH_MODE": os.environ.get("MAINSEQUENCE_AUTH_MODE"),
        "MAINSEQUENCE_ACCESS_TOKEN": os.environ.get("MAINSEQUENCE_ACCESS_TOKEN"),
        "MAINSEQUENCE_REFRESH_TOKEN": os.environ.get("MAINSEQUENCE_REFRESH_TOKEN"),
        "TDAG_ENDPOINT": os.environ.get("TDAG_ENDPOINT"),
        "MAINSEQUENCE_ENDPOINT": os.environ.get("MAINSEQUENCE_ENDPOINT"),
        "MAIN_SEQUENCE_PROJECT_ID": os.environ.get("MAIN_SEQUENCE_PROJECT_ID"),
    }

    client_utils = None
    old_provider = None
    old_base_root_url = None
    old_image_root_url = None

    try:
        os.environ["MAINSEQUENCE_AUTH_MODE"] = "jwt"
        os.environ["MAINSEQUENCE_ACCESS_TOKEN"] = access
        if refresh:
            os.environ["MAINSEQUENCE_REFRESH_TOKEN"] = refresh
        else:
            os.environ.pop("MAINSEQUENCE_REFRESH_TOKEN", None)
        os.environ["TDAG_ENDPOINT"] = endpoint
        os.environ["MAINSEQUENCE_ENDPOINT"] = endpoint
        os.environ["MAIN_SEQUENCE_PROJECT_ID"] = str(related_project_id)

        from mainsequence.client import utils as _client_utils
        from mainsequence.client.base import BaseObjectOrm
        from mainsequence.client.models_tdag import ProjectImage as ClientProjectImage

        client_utils = _client_utils
        old_provider = getattr(client_utils.loaders, "provider", None)
        old_base_root_url = BaseObjectOrm.ROOT_URL
        old_image_root_url = getattr(ClientProjectImage, "ROOT_URL", None)

        client_utils.TDAG_ENDPOINT = endpoint
        client_utils.API_ENDPOINT = root_url
        client_utils.loaders.use_jwt(access=access, refresh=refresh or None)

        BaseObjectOrm.ROOT_URL = root_url
        ClientProjectImage.ROOT_URL = root_url

        merged_filters = dict(filters or {})
        merged_filters["related_project__id__in"] = [int(related_project_id)]
        images = ClientProjectImage.filter(timeout=timeout, **merged_filters)

        out: list[dict[str, Any]] = []
        for image in images:
            if isinstance(image, dict):
                out.append(image)
            elif hasattr(image, "model_dump"):
                out.append(image.model_dump())
            else:
                out.append({"id": getattr(image, "id", None)})
        return out

    except Exception as e:
        err_name = type(e).__name__
        if err_name in {"AuthenticationError", "PermissionDeniedError"}:
            raise NotLoggedIn(str(e) or "Not logged in.")
        if err_name == "NotFoundError":
            raise ApiError(f"Project not found: {related_project_id}")
        raise ApiError(f"Project images fetch failed: {e}")
    finally:
        if client_utils is not None:
            try:
                client_utils.loaders.provider = old_provider
            except Exception:
                pass
        if old_base_root_url is not None:
            try:
                from mainsequence.client.base import BaseObjectOrm

                BaseObjectOrm.ROOT_URL = old_base_root_url
            except Exception:
                pass
        if old_image_root_url is not None:
            try:
                from mainsequence.client.models_tdag import ProjectImage as ClientProjectImage

                ClientProjectImage.ROOT_URL = old_image_root_url
            except Exception:
                pass

        for k, v in old_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def get_project_image(
    *,
    image_id: int | str,
    timeout: int | None = None,
) -> dict[str, Any]:
    try:
        image = _run_sdk_model_operation(
            module_name="mainsequence.client.models_tdag",
            class_name="ProjectImage",
            operation=lambda ClientProjectImage: ClientProjectImage.get(
                pk=int(image_id),
                timeout=timeout,
            ),
        )
        return _sdk_object_to_dict(image)
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"Project image not found: {image_id}")
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Project image fetch failed: {e}")


def delete_project_image(
    *,
    image_id: int | str,
    timeout: int | None = None,
) -> dict[str, Any]:
    try:
        def _delete(ClientProjectImage):
            image = ClientProjectImage.get(pk=int(image_id), timeout=timeout)
            payload = _sdk_object_to_dict(image)
            image.delete()
            return payload

        return _run_sdk_model_operation(
            module_name="mainsequence.client.models_tdag",
            class_name="ProjectImage",
            operation=_delete,
        )
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"Project image not found: {image_id}")
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Project image deletion failed: {e}")


def _normalize_release_kind_value(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "value"):
        return str(value.value)
    return str(value).strip() or None


def get_resource_release(
    *,
    release_id: int | str,
    expected_release_kind: str | None = None,
    timeout: int | None = None,
) -> dict[str, Any]:
    try:
        release = _run_sdk_model_operation(
            module_name="mainsequence.client.models_helpers",
            class_name="ResourceRelease",
            operation=lambda ClientResourceRelease: ClientResourceRelease.get(
                pk=int(release_id),
                timeout=timeout,
            ),
        )
        payload = _sdk_object_to_dict(release)
        actual_kind = _normalize_release_kind_value(payload.get("release_kind"))
        if expected_release_kind and actual_kind != expected_release_kind:
            raise ApiError(f"Resource release {release_id} is not {expected_release_kind}.")
        return payload
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"Resource release not found: {release_id}")
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Resource release fetch failed: {e}")


def delete_resource_release(
    *,
    release_id: int | str,
    expected_release_kind: str | None = None,
    timeout: int | None = None,
) -> dict[str, Any]:
    try:
        def _delete(ClientResourceRelease):
            release = ClientResourceRelease.get(pk=int(release_id), timeout=timeout)
            payload = _sdk_object_to_dict(release)
            actual_kind = _normalize_release_kind_value(payload.get("release_kind"))
            if expected_release_kind and actual_kind != expected_release_kind:
                raise ApiError(f"Resource release {release_id} is not {expected_release_kind}.")
            release.delete()
            return payload

        return _run_sdk_model_operation(
            module_name="mainsequence.client.models_helpers",
            class_name="ResourceRelease",
            operation=_delete,
        )
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"Resource release not found: {release_id}")
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Resource release deletion failed: {e}")


def list_project_jobs(
    *,
    project_id: int | str,
    filters: dict[str, Any] | None = None,
    timeout: int | None = None,
) -> list[dict[str, Any]]:
    """
    List jobs for a project via SDK client model.

    Single source of truth:
      - delegates filtering and payload parsing to `Job.filter()`
    """
    tokens = get_tokens()
    access = (tokens.get("access") or "").strip()
    refresh = (tokens.get("refresh") or "").strip()
    if not access:
        raise NotLoggedIn("Not logged in.")

    endpoint = backend_url().rstrip("/")
    root_url = f"{endpoint}/orm/api"

    old_env = {
        "MAINSEQUENCE_AUTH_MODE": os.environ.get("MAINSEQUENCE_AUTH_MODE"),
        "MAINSEQUENCE_ACCESS_TOKEN": os.environ.get("MAINSEQUENCE_ACCESS_TOKEN"),
        "MAINSEQUENCE_REFRESH_TOKEN": os.environ.get("MAINSEQUENCE_REFRESH_TOKEN"),
        "TDAG_ENDPOINT": os.environ.get("TDAG_ENDPOINT"),
        "MAINSEQUENCE_ENDPOINT": os.environ.get("MAINSEQUENCE_ENDPOINT"),
        "MAIN_SEQUENCE_PROJECT_ID": os.environ.get("MAIN_SEQUENCE_PROJECT_ID"),
    }

    client_utils = None
    old_provider = None
    old_base_root_url = None
    old_job_root_url = None

    try:
        os.environ["MAINSEQUENCE_AUTH_MODE"] = "jwt"
        os.environ["MAINSEQUENCE_ACCESS_TOKEN"] = access
        if refresh:
            os.environ["MAINSEQUENCE_REFRESH_TOKEN"] = refresh
        else:
            os.environ.pop("MAINSEQUENCE_REFRESH_TOKEN", None)
        os.environ["TDAG_ENDPOINT"] = endpoint
        os.environ["MAINSEQUENCE_ENDPOINT"] = endpoint
        os.environ["MAIN_SEQUENCE_PROJECT_ID"] = str(project_id)

        from mainsequence.client import utils as _client_utils
        from mainsequence.client.base import BaseObjectOrm
        from mainsequence.client.models_helpers import Job as ClientJob

        client_utils = _client_utils
        old_provider = getattr(client_utils.loaders, "provider", None)
        old_base_root_url = BaseObjectOrm.ROOT_URL
        old_job_root_url = getattr(ClientJob, "ROOT_URL", None)

        client_utils.TDAG_ENDPOINT = endpoint
        client_utils.API_ENDPOINT = root_url
        client_utils.loaders.use_jwt(access=access, refresh=refresh or None)

        BaseObjectOrm.ROOT_URL = root_url
        ClientJob.ROOT_URL = root_url

        jobs = []
        extra_filters = dict(filters or {})
        for candidate_filters in ({"project": int(project_id)}, {"project__id__in": [int(project_id)]}):
            try:
                jobs = ClientJob.filter(timeout=timeout, **{**extra_filters, **candidate_filters})
                if jobs or "project__id__in" in candidate_filters:
                    break
            except Exception:
                continue

        out: list[dict[str, Any]] = []
        for job in jobs:
            if isinstance(job, dict):
                out.append(job)
            elif hasattr(job, "model_dump"):
                out.append(job.model_dump())
            else:
                out.append({"id": getattr(job, "id", None)})
        return out

    except Exception as e:
        err_name = type(e).__name__
        if err_name in {"AuthenticationError", "PermissionDeniedError"}:
            raise NotLoggedIn(str(e) or "Not logged in.")
        if err_name == "NotFoundError":
            raise ApiError(f"Project not found: {project_id}")
        raise ApiError(f"Project jobs fetch failed: {e}")
    finally:
        if client_utils is not None:
            try:
                client_utils.loaders.provider = old_provider
            except Exception:
                pass
        if old_base_root_url is not None:
            try:
                from mainsequence.client.base import BaseObjectOrm

                BaseObjectOrm.ROOT_URL = old_base_root_url
            except Exception:
                pass
        if old_job_root_url is not None:
            try:
                from mainsequence.client.models_helpers import Job as ClientJob

                ClientJob.ROOT_URL = old_job_root_url
            except Exception:
                pass

        for k, v in old_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def list_project_resources(
    *,
    project_id: int | str,
    repo_commit_sha: str,
    resource_type: str | None = None,
    filters: dict[str, Any] | None = None,
    timeout: int | None = None,
) -> list[dict[str, Any]]:
    """
    List project resources for a project and repository commit via SDK client model.

    Single source of truth:
      - delegates filtering and payload parsing to `ProjectResource.filter()`
    """
    tokens = get_tokens()
    access = (tokens.get("access") or "").strip()
    refresh = (tokens.get("refresh") or "").strip()
    if not access:
        raise NotLoggedIn("Not logged in.")

    endpoint = backend_url().rstrip("/")
    root_url = f"{endpoint}/orm/api"

    old_env = {
        "MAINSEQUENCE_AUTH_MODE": os.environ.get("MAINSEQUENCE_AUTH_MODE"),
        "MAINSEQUENCE_ACCESS_TOKEN": os.environ.get("MAINSEQUENCE_ACCESS_TOKEN"),
        "MAINSEQUENCE_REFRESH_TOKEN": os.environ.get("MAINSEQUENCE_REFRESH_TOKEN"),
        "TDAG_ENDPOINT": os.environ.get("TDAG_ENDPOINT"),
        "MAINSEQUENCE_ENDPOINT": os.environ.get("MAINSEQUENCE_ENDPOINT"),
        "MAIN_SEQUENCE_PROJECT_ID": os.environ.get("MAIN_SEQUENCE_PROJECT_ID"),
    }

    client_utils = None
    old_provider = None
    old_base_root_url = None
    old_resource_root_url = None

    try:
        os.environ["MAINSEQUENCE_AUTH_MODE"] = "jwt"
        os.environ["MAINSEQUENCE_ACCESS_TOKEN"] = access
        if refresh:
            os.environ["MAINSEQUENCE_REFRESH_TOKEN"] = refresh
        else:
            os.environ.pop("MAINSEQUENCE_REFRESH_TOKEN", None)
        os.environ["TDAG_ENDPOINT"] = endpoint
        os.environ["MAINSEQUENCE_ENDPOINT"] = endpoint
        os.environ["MAIN_SEQUENCE_PROJECT_ID"] = str(project_id)

        from mainsequence.client import utils as _client_utils
        from mainsequence.client.base import BaseObjectOrm
        from mainsequence.client.models_helpers import ProjectResource as ClientProjectResource

        client_utils = _client_utils
        old_provider = getattr(client_utils.loaders, "provider", None)
        old_base_root_url = BaseObjectOrm.ROOT_URL
        old_resource_root_url = getattr(ClientProjectResource, "ROOT_URL", None)

        client_utils.TDAG_ENDPOINT = endpoint
        client_utils.API_ENDPOINT = root_url
        client_utils.loaders.use_jwt(access=access, refresh=refresh or None)

        BaseObjectOrm.ROOT_URL = root_url
        ClientProjectResource.ROOT_URL = root_url

        merged_filters: dict[str, Any] = dict(filters or {})
        merged_filters.update(
            {
            "project__id": int(project_id),
            "repo_commit_sha": str(repo_commit_sha).strip(),
            }
        )
        normalized_resource_type = str(resource_type).strip() if resource_type is not None else ""
        if normalized_resource_type:
            merged_filters["resource_type"] = normalized_resource_type

        resources = ClientProjectResource.filter(timeout=timeout, **merged_filters)

        out: list[dict[str, Any]] = []
        for resource in resources:
            if isinstance(resource, dict):
                out.append(resource)
            elif hasattr(resource, "model_dump"):
                out.append(resource.model_dump())
            else:
                out.append({"id": getattr(resource, "id", None)})
        return out

    except Exception as e:
        err_name = type(e).__name__
        if err_name in {"AuthenticationError", "PermissionDeniedError"}:
            raise NotLoggedIn(str(e) or "Not logged in.")
        if err_name == "NotFoundError":
            raise ApiError(f"Project not found: {project_id}")
        raise ApiError(f"Project resources fetch failed: {e}")
    finally:
        if client_utils is not None:
            try:
                client_utils.loaders.provider = old_provider
            except Exception:
                pass
        if old_base_root_url is not None:
            try:
                from mainsequence.client.base import BaseObjectOrm

                BaseObjectOrm.ROOT_URL = old_base_root_url
            except Exception:
                pass
        if old_resource_root_url is not None:
            try:
                from mainsequence.client.models_helpers import (
                    ProjectResource as ClientProjectResource,
                )

                ClientProjectResource.ROOT_URL = old_resource_root_url
            except Exception:
                pass

        for k, v in old_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def create_project_resource_release(
    *,
    release_kind: str,
    resource_id: int | str,
    related_image_id: int | None = None,
    readme_resource_id: int | None = None,
    cpu_request: str | int | float | None = None,
    memory_request: str | int | float | None = None,
    gpu_request: str | int | None = None,
    gpu_type: str | None = None,
    spot: bool | None = None,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Create a resource release via SDK client model.

    Single source of truth:
      - delegates to `ProjectResource.create_dashboard()` / `ProjectResource.create_agent()`
      - which in turn use `ResourceRelease.create()`
    """
    tokens = get_tokens()
    access = (tokens.get("access") or "").strip()
    refresh = (tokens.get("refresh") or "").strip()
    if not access:
        raise NotLoggedIn("Not logged in.")

    endpoint = backend_url().rstrip("/")
    root_url = f"{endpoint}/orm/api"

    old_env = {
        "MAINSEQUENCE_AUTH_MODE": os.environ.get("MAINSEQUENCE_AUTH_MODE"),
        "MAINSEQUENCE_ACCESS_TOKEN": os.environ.get("MAINSEQUENCE_ACCESS_TOKEN"),
        "MAINSEQUENCE_REFRESH_TOKEN": os.environ.get("MAINSEQUENCE_REFRESH_TOKEN"),
        "TDAG_ENDPOINT": os.environ.get("TDAG_ENDPOINT"),
        "MAINSEQUENCE_ENDPOINT": os.environ.get("MAINSEQUENCE_ENDPOINT"),
    }

    client_utils = None
    old_provider = None
    old_base_root_url = None
    old_resource_root_url = None

    try:
        os.environ["MAINSEQUENCE_AUTH_MODE"] = "jwt"
        os.environ["MAINSEQUENCE_ACCESS_TOKEN"] = access
        if refresh:
            os.environ["MAINSEQUENCE_REFRESH_TOKEN"] = refresh
        else:
            os.environ.pop("MAINSEQUENCE_REFRESH_TOKEN", None)
        os.environ["TDAG_ENDPOINT"] = endpoint
        os.environ["MAINSEQUENCE_ENDPOINT"] = endpoint

        from mainsequence.client import utils as _client_utils
        from mainsequence.client.base import BaseObjectOrm
        from mainsequence.client.models_helpers import ProjectResource as ClientProjectResource

        client_utils = _client_utils
        old_provider = getattr(client_utils.loaders, "provider", None)
        old_base_root_url = BaseObjectOrm.ROOT_URL
        old_resource_root_url = getattr(ClientProjectResource, "ROOT_URL", None)

        client_utils.TDAG_ENDPOINT = endpoint
        client_utils.API_ENDPOINT = root_url
        client_utils.loaders.use_jwt(access=access, refresh=refresh or None)

        BaseObjectOrm.ROOT_URL = root_url
        ClientProjectResource.ROOT_URL = root_url

        resource = ClientProjectResource.get(pk=int(resource_id), timeout=timeout)
        create_kwargs: dict[str, Any] = {
            "related_image_id": related_image_id,
            "readme_resource_id": readme_resource_id,
            "cpu_request": cpu_request,
            "memory_request": memory_request,
            "gpu_request": gpu_request,
            "gpu_type": gpu_type,
            "spot": spot,
            "timeout": timeout,
        }

        if release_kind == "streamlit_dashboard":
            created = resource.create_dashboard(**create_kwargs)
        elif release_kind == "agent":
            created = resource.create_agent(**create_kwargs)
        else:
            raise ApiError("release_kind must be either 'streamlit_dashboard' or 'agent'.")

        if isinstance(created, dict):
            return created
        if hasattr(created, "model_dump"):
            return created.model_dump()
        return {"id": getattr(created, "id", None)}

    except Exception as e:
        err_name = type(e).__name__
        if err_name in {"AuthenticationError", "PermissionDeniedError"}:
            raise NotLoggedIn(str(e) or "Not logged in.")
        if err_name == "NotFoundError":
            raise ApiError(f"Project resource not found: {resource_id}")
        raise ApiError(f"Project resource release create failed: {e}")
    finally:
        if client_utils is not None:
            try:
                client_utils.loaders.provider = old_provider
            except Exception:
                pass
        if old_base_root_url is not None:
            try:
                from mainsequence.client.base import BaseObjectOrm

                BaseObjectOrm.ROOT_URL = old_base_root_url
            except Exception:
                pass
        if old_resource_root_url is not None:
            try:
                from mainsequence.client.models_helpers import (
                    ProjectResource as ClientProjectResource,
                )

                ClientProjectResource.ROOT_URL = old_resource_root_url
            except Exception:
                pass

        for k, v in old_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def list_market_portfolios(
    *,
    filters: dict[str, Any] | None = None,
    timeout: int | None = None,
) -> list[dict[str, Any]]:
    """
    List markets portfolios via SDK client model.

    Single source of truth:
      - delegates filtering and payload parsing to `Portfolio.filter()`
    """
    tokens = get_tokens()
    access = (tokens.get("access") or "").strip()
    refresh = (tokens.get("refresh") or "").strip()
    if not access:
        raise NotLoggedIn("Not logged in.")

    endpoint = backend_url().rstrip("/")
    root_url = f"{endpoint}/orm/api"

    old_env = {
        "MAINSEQUENCE_AUTH_MODE": os.environ.get("MAINSEQUENCE_AUTH_MODE"),
        "MAINSEQUENCE_ACCESS_TOKEN": os.environ.get("MAINSEQUENCE_ACCESS_TOKEN"),
        "MAINSEQUENCE_REFRESH_TOKEN": os.environ.get("MAINSEQUENCE_REFRESH_TOKEN"),
        "TDAG_ENDPOINT": os.environ.get("TDAG_ENDPOINT"),
        "MAINSEQUENCE_ENDPOINT": os.environ.get("MAINSEQUENCE_ENDPOINT"),
    }

    client_utils = None
    old_provider = None
    old_base_root_url = None
    old_portfolio_root_url = None

    try:
        os.environ["MAINSEQUENCE_AUTH_MODE"] = "jwt"
        os.environ["MAINSEQUENCE_ACCESS_TOKEN"] = access
        if refresh:
            os.environ["MAINSEQUENCE_REFRESH_TOKEN"] = refresh
        else:
            os.environ.pop("MAINSEQUENCE_REFRESH_TOKEN", None)
        os.environ["TDAG_ENDPOINT"] = endpoint
        os.environ["MAINSEQUENCE_ENDPOINT"] = endpoint

        from mainsequence.client import utils as _client_utils
        from mainsequence.client.base import BaseObjectOrm
        from mainsequence.client.models_vam import Portfolio as ClientPortfolio

        client_utils = _client_utils
        old_provider = getattr(client_utils.loaders, "provider", None)
        old_base_root_url = BaseObjectOrm.ROOT_URL
        old_portfolio_root_url = getattr(ClientPortfolio, "ROOT_URL", None)

        client_utils.TDAG_ENDPOINT = endpoint
        client_utils.API_ENDPOINT = root_url
        client_utils.loaders.use_jwt(access=access, refresh=refresh or None)

        BaseObjectOrm.ROOT_URL = root_url
        ClientPortfolio.ROOT_URL = root_url

        portfolios = ClientPortfolio.filter(timeout=timeout, **dict(filters or {}))

        out: list[dict[str, Any]] = []
        for portfolio in portfolios:
            if isinstance(portfolio, dict):
                out.append(portfolio)
            elif hasattr(portfolio, "model_dump"):
                out.append(portfolio.model_dump())
            else:
                out.append({"id": getattr(portfolio, "id", None)})
        return out

    except Exception as e:
        err_name = type(e).__name__
        if err_name in {"AuthenticationError", "PermissionDeniedError"}:
            raise NotLoggedIn(str(e) or "Not logged in.")
        raise ApiError(f"Markets portfolios fetch failed: {e}")
    finally:
        if client_utils is not None:
            try:
                client_utils.loaders.provider = old_provider
            except Exception:
                pass
        if old_base_root_url is not None:
            try:
                from mainsequence.client.base import BaseObjectOrm

                BaseObjectOrm.ROOT_URL = old_base_root_url
            except Exception:
                pass
        if old_portfolio_root_url is not None:
            try:
                from mainsequence.client.models_vam import Portfolio as ClientPortfolio

                ClientPortfolio.ROOT_URL = old_portfolio_root_url
            except Exception:
                pass

        for k, v in old_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def list_data_node_storages(
    *,
    filters: dict[str, Any] | None = None,
    timeout: int | None = None,
) -> list[dict[str, Any]]:
    """
    List data node storages via SDK client model.

    Single source of truth:
      - delegates filtering and payload parsing to `DataNodeStorage.filter()`
    """
    try:
        storages = _run_sdk_model_operation(
            module_name="mainsequence.client.models_tdag",
            class_name="DataNodeStorage",
            operation=lambda ClientDataNodeStorage: ClientDataNodeStorage.filter(
                timeout=timeout,
                **dict(filters or {}),
            ),
        )
        out: list[dict[str, Any]] = []
        for storage in storages:
            out.append(_sdk_object_to_dict(storage))
        return out
    except Exception as e:
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Data node storages fetch failed: {e}")


def list_constants(
    *,
    filters: dict[str, Any] | None = None,
    timeout: int | None = None,
) -> list[dict[str, Any]]:
    """
    List constants via SDK client model.

    Single source of truth:
      - delegates filtering and payload parsing to `Constant.filter()`
    """
    try:
        constants = _run_sdk_model_operation(
            module_name="mainsequence.client.models_tdag",
            class_name="Constant",
            operation=lambda ClientConstant: ClientConstant.filter(
                timeout=timeout,
                **dict(filters or {}),
            ),
        )
        return [_sdk_object_to_dict(constant) for constant in constants]
    except Exception as e:
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Constants fetch failed: {e}")


def list_secrets(
    *,
    filters: dict[str, Any] | None = None,
    timeout: int | None = None,
) -> list[dict[str, Any]]:
    """
    List secrets via SDK client model.

    Single source of truth:
      - delegates filtering and payload parsing to `Secret.filter()`
    """
    try:
        secrets = _run_sdk_model_operation(
            module_name="mainsequence.client.models_tdag",
            class_name="Secret",
            operation=lambda ClientSecret: ClientSecret.filter(
                timeout=timeout,
                **dict(filters or {}),
            ),
        )
        return [_sdk_object_to_dict(secret) for secret in secrets]
    except Exception as e:
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Secrets fetch failed: {e}")


def get_secret(
    secret_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Retrieve one secret via SDK client model.

    Single source of truth:
      - delegates detail fetching and payload parsing to `Secret.get()`
    """
    try:
        secret = _run_sdk_model_operation(
            module_name="mainsequence.client.models_tdag",
            class_name="Secret",
            operation=lambda ClientSecret: ClientSecret.get(
                pk=int(secret_id),
                timeout=timeout,
            ),
        )
        return _sdk_object_to_dict(secret)
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"Secret not found: {secret_id}")
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Secret fetch failed: {e}")


def create_secret(
    *,
    name: str,
    value: str,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Create one secret via SDK client model.

    Single source of truth:
      - delegates create payload and parsing to `Secret.create()`
    """
    try:
        secret = _run_sdk_model_operation(
            module_name="mainsequence.client.models_tdag",
            class_name="Secret",
            operation=lambda ClientSecret: ClientSecret.create(
                name=name,
                value=value,
                timeout=timeout,
            ),
        )
        return _sdk_object_to_dict(secret)
    except Exception as e:
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Secret creation failed: {e}")


def delete_secret(
    secret_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Delete one secret via SDK client model.

    Single source of truth:
      - delegates deletion to `Secret.delete()`
    """
    try:
        def _delete(ClientSecret):
            secret = ClientSecret.get(pk=int(secret_id), timeout=timeout)
            payload = _sdk_object_to_dict(secret)
            secret.delete(timeout=timeout)
            return payload

        return _run_sdk_model_operation(
            module_name="mainsequence.client.models_tdag",
            class_name="Secret",
            operation=_delete,
        )
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"Secret not found: {secret_id}")
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Secret deletion failed: {e}")


def list_secret_users_can_view(
    secret_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Fetch the view-access state for a secret via `ShareableObjectMixin.can_view()`.
    """
    return _get_shareable_object_access_state(
        module_name="mainsequence.client.models_tdag",
        class_name="Secret",
        object_id=secret_id,
        accessor_name="can_view",
        timeout=timeout,
    )


def list_secret_users_can_edit(
    secret_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Fetch the edit-access state for a secret via `ShareableObjectMixin.can_edit()`.
    """
    return _get_shareable_object_access_state(
        module_name="mainsequence.client.models_tdag",
        class_name="Secret",
        object_id=secret_id,
        accessor_name="can_edit",
        timeout=timeout,
    )


def add_secret_user_to_view(
    secret_id: int | str,
    user_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Grant explicit view access to one user for a secret.
    """
    return _mutate_shareable_object_access(
        module_name="mainsequence.client.models_tdag",
        class_name="Secret",
        object_id=secret_id,
        action_name="add_to_view",
        user_id=user_id,
        timeout=timeout,
    )


def add_secret_user_to_edit(
    secret_id: int | str,
    user_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Grant explicit edit access to one user for a secret.
    """
    return _mutate_shareable_object_access(
        module_name="mainsequence.client.models_tdag",
        class_name="Secret",
        object_id=secret_id,
        action_name="add_to_edit",
        user_id=user_id,
        timeout=timeout,
    )


def remove_secret_user_from_view(
    secret_id: int | str,
    user_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Remove explicit view access from one user for a secret.
    """
    return _mutate_shareable_object_access(
        module_name="mainsequence.client.models_tdag",
        class_name="Secret",
        object_id=secret_id,
        action_name="remove_from_view",
        user_id=user_id,
        timeout=timeout,
    )


def remove_secret_user_from_edit(
    secret_id: int | str,
    user_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Remove explicit edit access from one user for a secret.
    """
    return _mutate_shareable_object_access(
        module_name="mainsequence.client.models_tdag",
        class_name="Secret",
        object_id=secret_id,
        action_name="remove_from_edit",
        user_id=user_id,
        timeout=timeout,
    )


def get_constant(
    constant_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Retrieve one constant via SDK client model.

    Single source of truth:
      - delegates detail fetching and payload parsing to `Constant.get()`
    """
    try:
        constant = _run_sdk_model_operation(
            module_name="mainsequence.client.models_tdag",
            class_name="Constant",
            operation=lambda ClientConstant: ClientConstant.get(
                pk=int(constant_id),
                timeout=timeout,
            ),
        )
        return _sdk_object_to_dict(constant)
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"Constant not found: {constant_id}")
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Constant fetch failed: {e}")


def create_constant(
    *,
    name: str,
    value: Any,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Create one constant via SDK client model.

    Single source of truth:
      - delegates create payload and parsing to `Constant.create()`
    """
    try:
        constant = _run_sdk_model_operation(
            module_name="mainsequence.client.models_tdag",
            class_name="Constant",
            operation=lambda ClientConstant: ClientConstant.create(
                name=name,
                value=value,
                timeout=timeout,
            ),
        )
        return _sdk_object_to_dict(constant)
    except Exception as e:
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Constant creation failed: {e}")


def delete_constant(
    constant_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Delete one constant via SDK client model.

    Single source of truth:
      - delegates deletion to `Constant.delete()`
    """
    try:
        def _delete(ClientConstant):
            constant = ClientConstant.get(pk=int(constant_id), timeout=timeout)
            payload = _sdk_object_to_dict(constant)
            constant.delete(timeout=timeout)
            return payload

        return _run_sdk_model_operation(
            module_name="mainsequence.client.models_tdag",
            class_name="Constant",
            operation=_delete,
        )
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"Constant not found: {constant_id}")
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Constant deletion failed: {e}")


def _get_shareable_object_access_state(
    *,
    module_name: str,
    class_name: str,
    object_id: int | str,
    accessor_name: str,
    timeout: int | None = None,
) -> dict[str, Any]:
    try:
        payload = _run_sdk_model_operation(
            module_name=module_name,
            class_name=class_name,
            operation=lambda ClientObject: getattr(
                ClientObject.get(pk=int(object_id), timeout=timeout),
                accessor_name,
            )(timeout=timeout),
        )
        return _sdk_object_to_dict(payload)
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"{class_name} not found: {object_id}")
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"{class_name} share access fetch failed: {e}")


def _mutate_shareable_object_access(
    *,
    module_name: str,
    class_name: str,
    object_id: int | str,
    action_name: str,
    user_id: int | str,
    timeout: int | None = None,
) -> dict[str, Any]:
    try:
        payload = _run_sdk_model_operation(
            module_name=module_name,
            class_name=class_name,
            operation=lambda ClientObject: getattr(
                ClientObject.get(pk=int(object_id), timeout=timeout),
                action_name,
            )(int(user_id), timeout=timeout),
        )
        return _sdk_object_to_dict(payload)
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"{class_name} not found: {object_id}")
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"{class_name} share access update failed: {e}")


def list_constant_users_can_view(
    constant_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Fetch the view-access state for a constant via `ShareableObjectMixin.can_view()`.
    """
    return _get_shareable_object_access_state(
        module_name="mainsequence.client.models_tdag",
        class_name="Constant",
        object_id=constant_id,
        accessor_name="can_view",
        timeout=timeout,
    )


def list_constant_users_can_edit(
    constant_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Fetch the edit-access state for a constant via `ShareableObjectMixin.can_edit()`.
    """
    return _get_shareable_object_access_state(
        module_name="mainsequence.client.models_tdag",
        class_name="Constant",
        object_id=constant_id,
        accessor_name="can_edit",
        timeout=timeout,
    )


def add_constant_user_to_view(
    constant_id: int | str,
    user_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Grant explicit view access to one user for a constant.
    """
    return _mutate_shareable_object_access(
        module_name="mainsequence.client.models_tdag",
        class_name="Constant",
        object_id=constant_id,
        action_name="add_to_view",
        user_id=user_id,
        timeout=timeout,
    )


def add_constant_user_to_edit(
    constant_id: int | str,
    user_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Grant explicit edit access to one user for a constant.
    """
    return _mutate_shareable_object_access(
        module_name="mainsequence.client.models_tdag",
        class_name="Constant",
        object_id=constant_id,
        action_name="add_to_edit",
        user_id=user_id,
        timeout=timeout,
    )


def remove_constant_user_from_view(
    constant_id: int | str,
    user_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Remove explicit view access from one user for a constant.
    """
    return _mutate_shareable_object_access(
        module_name="mainsequence.client.models_tdag",
        class_name="Constant",
        object_id=constant_id,
        action_name="remove_from_view",
        user_id=user_id,
        timeout=timeout,
    )


def remove_constant_user_from_edit(
    constant_id: int | str,
    user_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Remove explicit edit access from one user for a constant.
    """
    return _mutate_shareable_object_access(
        module_name="mainsequence.client.models_tdag",
        class_name="Constant",
        object_id=constant_id,
        action_name="remove_from_edit",
        user_id=user_id,
        timeout=timeout,
    )


def get_data_node_storage(
    storage_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Retrieve one data node storage via SDK client model.

    Single source of truth:
      - delegates detail fetching and payload parsing to `DataNodeStorage.get()`
    """
    try:
        storage = _run_sdk_model_operation(
            module_name="mainsequence.client.models_tdag",
            class_name="DataNodeStorage",
            operation=lambda ClientDataNodeStorage: ClientDataNodeStorage.get(
                pk=int(storage_id),
                timeout=timeout,
            ),
        )
        return _sdk_object_to_dict(storage)
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"Data node storage not found: {storage_id}")
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Data node storage fetch failed: {e}")


def delete_data_node_storage(
    storage_id: int | str,
    *,
    full_delete_selected: bool = False,
    full_delete_downstream_tables: bool = False,
    delete_with_no_table: bool = False,
    override_protection: bool = False,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Delete one data node storage via SDK client model.

    Single source of truth:
      - delegates deletion and destroy query params to `DataNodeStorage.delete()`
    """
    try:
        def _delete(ClientDataNodeStorage):
            storage = ClientDataNodeStorage.get(pk=int(storage_id), timeout=timeout)
            payload = _sdk_object_to_dict(storage)
            storage.delete(
                full_delete_selected=full_delete_selected,
                full_delete_downstream_tables=full_delete_downstream_tables,
                delete_with_no_table=delete_with_no_table,
                override_protection=override_protection,
                timeout=timeout,
            )
            return payload

        return _run_sdk_model_operation(
            module_name="mainsequence.client.models_tdag",
            class_name="DataNodeStorage",
            operation=_delete,
        )
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"Data node storage not found: {storage_id}")
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Data node storage deletion failed: {e}")


def list_market_asset_translation_tables(
    *,
    filters: dict[str, Any] | None = None,
    timeout: int | None = None,
) -> list[dict[str, Any]]:
    """
    List markets asset translation tables via SDK client model.

    Single source of truth:
      - delegates filtering and payload parsing to `AssetTranslationTable.filter()`
    """
    tokens = get_tokens()
    access = (tokens.get("access") or "").strip()
    refresh = (tokens.get("refresh") or "").strip()
    if not access:
        raise NotLoggedIn("Not logged in.")

    endpoint = backend_url().rstrip("/")
    root_url = f"{endpoint}/orm/api"

    old_env = {
        "MAINSEQUENCE_AUTH_MODE": os.environ.get("MAINSEQUENCE_AUTH_MODE"),
        "MAINSEQUENCE_ACCESS_TOKEN": os.environ.get("MAINSEQUENCE_ACCESS_TOKEN"),
        "MAINSEQUENCE_REFRESH_TOKEN": os.environ.get("MAINSEQUENCE_REFRESH_TOKEN"),
        "TDAG_ENDPOINT": os.environ.get("TDAG_ENDPOINT"),
        "MAINSEQUENCE_ENDPOINT": os.environ.get("MAINSEQUENCE_ENDPOINT"),
    }

    client_utils = None
    old_provider = None
    old_base_root_url = None
    old_table_root_url = None

    try:
        os.environ["MAINSEQUENCE_AUTH_MODE"] = "jwt"
        os.environ["MAINSEQUENCE_ACCESS_TOKEN"] = access
        if refresh:
            os.environ["MAINSEQUENCE_REFRESH_TOKEN"] = refresh
        else:
            os.environ.pop("MAINSEQUENCE_REFRESH_TOKEN", None)
        os.environ["TDAG_ENDPOINT"] = endpoint
        os.environ["MAINSEQUENCE_ENDPOINT"] = endpoint

        from mainsequence.client import utils as _client_utils
        from mainsequence.client.base import BaseObjectOrm
        from mainsequence.client.models_vam import (
            AssetTranslationTable as ClientAssetTranslationTable,
        )

        client_utils = _client_utils
        old_provider = getattr(client_utils.loaders, "provider", None)
        old_base_root_url = BaseObjectOrm.ROOT_URL
        old_table_root_url = getattr(ClientAssetTranslationTable, "ROOT_URL", None)

        client_utils.TDAG_ENDPOINT = endpoint
        client_utils.API_ENDPOINT = root_url
        client_utils.loaders.use_jwt(access=access, refresh=refresh or None)

        BaseObjectOrm.ROOT_URL = root_url
        ClientAssetTranslationTable.ROOT_URL = root_url

        tables = ClientAssetTranslationTable.filter(timeout=timeout, **dict(filters or {}))

        out: list[dict[str, Any]] = []
        for table in tables:
            if isinstance(table, dict):
                out.append(table)
            elif hasattr(table, "model_dump"):
                out.append(table.model_dump())
            else:
                out.append({"id": getattr(table, "id", None)})
        return out

    except Exception as e:
        err_name = type(e).__name__
        if err_name in {"AuthenticationError", "PermissionDeniedError"}:
            raise NotLoggedIn(str(e) or "Not logged in.")
        raise ApiError(f"Markets asset translation tables fetch failed: {e}")
    finally:
        if client_utils is not None:
            try:
                client_utils.loaders.provider = old_provider
            except Exception:
                pass
        if old_base_root_url is not None:
            try:
                from mainsequence.client.base import BaseObjectOrm

                BaseObjectOrm.ROOT_URL = old_base_root_url
            except Exception:
                pass
        if old_table_root_url is not None:
            try:
                from mainsequence.client.models_vam import (
                    AssetTranslationTable as ClientAssetTranslationTable,
                )

                ClientAssetTranslationTable.ROOT_URL = old_table_root_url
            except Exception:
                pass

        for k, v in old_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def get_market_asset_translation_table(
    table_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Retrieve one markets asset translation table via SDK client model.

    Single source of truth:
      - delegates detail fetching and payload parsing to `AssetTranslationTable.get()`
    """
    tokens = get_tokens()
    access = (tokens.get("access") or "").strip()
    refresh = (tokens.get("refresh") or "").strip()
    if not access:
        raise NotLoggedIn("Not logged in.")

    endpoint = backend_url().rstrip("/")
    root_url = f"{endpoint}/orm/api"

    old_env = {
        "MAINSEQUENCE_AUTH_MODE": os.environ.get("MAINSEQUENCE_AUTH_MODE"),
        "MAINSEQUENCE_ACCESS_TOKEN": os.environ.get("MAINSEQUENCE_ACCESS_TOKEN"),
        "MAINSEQUENCE_REFRESH_TOKEN": os.environ.get("MAINSEQUENCE_REFRESH_TOKEN"),
        "TDAG_ENDPOINT": os.environ.get("TDAG_ENDPOINT"),
        "MAINSEQUENCE_ENDPOINT": os.environ.get("MAINSEQUENCE_ENDPOINT"),
    }

    client_utils = None
    old_provider = None
    old_base_root_url = None
    old_table_root_url = None

    try:
        os.environ["MAINSEQUENCE_AUTH_MODE"] = "jwt"
        os.environ["MAINSEQUENCE_ACCESS_TOKEN"] = access
        if refresh:
            os.environ["MAINSEQUENCE_REFRESH_TOKEN"] = refresh
        else:
            os.environ.pop("MAINSEQUENCE_REFRESH_TOKEN", None)
        os.environ["TDAG_ENDPOINT"] = endpoint
        os.environ["MAINSEQUENCE_ENDPOINT"] = endpoint

        from mainsequence.client import utils as _client_utils
        from mainsequence.client.base import BaseObjectOrm
        from mainsequence.client.models_vam import (
            AssetTranslationTable as ClientAssetTranslationTable,
        )

        client_utils = _client_utils
        old_provider = getattr(client_utils.loaders, "provider", None)
        old_base_root_url = BaseObjectOrm.ROOT_URL
        old_table_root_url = getattr(ClientAssetTranslationTable, "ROOT_URL", None)

        client_utils.TDAG_ENDPOINT = endpoint
        client_utils.API_ENDPOINT = root_url
        client_utils.loaders.use_jwt(access=access, refresh=refresh or None)

        BaseObjectOrm.ROOT_URL = root_url
        ClientAssetTranslationTable.ROOT_URL = root_url

        table = ClientAssetTranslationTable.get(pk=int(table_id), timeout=timeout)
        if isinstance(table, dict):
            return table
        if hasattr(table, "model_dump"):
            return table.model_dump()
        return {"id": getattr(table, "id", None)}

    except Exception as e:
        err_name = type(e).__name__
        if err_name in {"AuthenticationError", "PermissionDeniedError"}:
            raise NotLoggedIn(str(e) or "Not logged in.")
        if err_name == "NotFoundError":
            raise ApiError(f"Markets asset translation table not found: {table_id}")
        raise ApiError(f"Markets asset translation table fetch failed: {e}")
    finally:
        if client_utils is not None:
            try:
                client_utils.loaders.provider = old_provider
            except Exception:
                pass
        if old_base_root_url is not None:
            try:
                from mainsequence.client.base import BaseObjectOrm

                BaseObjectOrm.ROOT_URL = old_base_root_url
            except Exception:
                pass
        if old_table_root_url is not None:
            try:
                from mainsequence.client.models_vam import (
                    AssetTranslationTable as ClientAssetTranslationTable,
                )

                ClientAssetTranslationTable.ROOT_URL = old_table_root_url
            except Exception:
                pass

        for k, v in old_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def create_project_job(
    *,
    name: str,
    project_id: int | str,
    execution_path: str | None = None,
    app_name: str | None = None,
    task_schedule: dict[str, Any] | str | None = None,
    cpu_request: str | int | float | None = None,
    memory_request: str | int | float | None = None,
    gpu_request: str | int | float | None = None,
    gpu_type: str | None = None,
    spot: bool | None = None,
    max_runtime_seconds: int | None = None,
    related_image_id: int | None = None,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Create a project job via SDK client model.

    Single source of truth:
      - delegates payload construction and request behavior to `Job.create()`
    """
    tokens = get_tokens()
    access = (tokens.get("access") or "").strip()
    refresh = (tokens.get("refresh") or "").strip()
    if not access:
        raise NotLoggedIn("Not logged in.")

    endpoint = backend_url().rstrip("/")
    root_url = f"{endpoint}/orm/api"

    old_env = {
        "MAINSEQUENCE_AUTH_MODE": os.environ.get("MAINSEQUENCE_AUTH_MODE"),
        "MAINSEQUENCE_ACCESS_TOKEN": os.environ.get("MAINSEQUENCE_ACCESS_TOKEN"),
        "MAINSEQUENCE_REFRESH_TOKEN": os.environ.get("MAINSEQUENCE_REFRESH_TOKEN"),
        "TDAG_ENDPOINT": os.environ.get("TDAG_ENDPOINT"),
        "MAINSEQUENCE_ENDPOINT": os.environ.get("MAINSEQUENCE_ENDPOINT"),
        "MAIN_SEQUENCE_PROJECT_ID": os.environ.get("MAIN_SEQUENCE_PROJECT_ID"),
    }

    client_utils = None
    old_provider = None
    old_base_root_url = None
    old_job_root_url = None

    try:
        os.environ["MAINSEQUENCE_AUTH_MODE"] = "jwt"
        os.environ["MAINSEQUENCE_ACCESS_TOKEN"] = access
        if refresh:
            os.environ["MAINSEQUENCE_REFRESH_TOKEN"] = refresh
        else:
            os.environ.pop("MAINSEQUENCE_REFRESH_TOKEN", None)
        os.environ["TDAG_ENDPOINT"] = endpoint
        os.environ["MAINSEQUENCE_ENDPOINT"] = endpoint
        os.environ["MAIN_SEQUENCE_PROJECT_ID"] = str(project_id)

        from mainsequence.client import utils as _client_utils
        from mainsequence.client.base import BaseObjectOrm
        from mainsequence.client.models_helpers import Job as ClientJob

        client_utils = _client_utils
        old_provider = getattr(client_utils.loaders, "provider", None)
        old_base_root_url = BaseObjectOrm.ROOT_URL
        old_job_root_url = getattr(ClientJob, "ROOT_URL", None)

        client_utils.TDAG_ENDPOINT = endpoint
        client_utils.API_ENDPOINT = root_url
        client_utils.loaders.use_jwt(access=access, refresh=refresh or None)

        BaseObjectOrm.ROOT_URL = root_url
        ClientJob.ROOT_URL = root_url

        created = ClientJob.create(
            name=name,
            project_id=int(project_id),
            execution_path=execution_path,
            app_name=app_name,
            task_schedule=task_schedule,
            cpu_request=cpu_request,
            memory_request=memory_request,
            gpu_request=gpu_request,
            gpu_type=gpu_type,
            spot=spot,
            max_runtime_seconds=max_runtime_seconds,
            related_image_id=related_image_id,
            timeout=timeout,
        )
        if isinstance(created, dict):
            return created
        if hasattr(created, "model_dump"):
            return created.model_dump()
        return {"id": getattr(created, "id", None)}

    except Exception as e:
        err_name = type(e).__name__
        if err_name in {"AuthenticationError", "PermissionDeniedError"}:
            raise NotLoggedIn(str(e) or "Not logged in.")
        if err_name == "NotFoundError":
            raise ApiError(f"Project not found: {project_id}")
        raise ApiError(f"Project job create failed: {e}")
    finally:
        if client_utils is not None:
            try:
                client_utils.loaders.provider = old_provider
            except Exception:
                pass
        if old_base_root_url is not None:
            try:
                from mainsequence.client.base import BaseObjectOrm

                BaseObjectOrm.ROOT_URL = old_base_root_url
            except Exception:
                pass
        if old_job_root_url is not None:
            try:
                from mainsequence.client.models_helpers import Job as ClientJob

                ClientJob.ROOT_URL = old_job_root_url
            except Exception:
                pass

        for k, v in old_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def schedule_batch_project_jobs(
    *,
    file_path: str,
    project_id: int | str,
    strict: bool = False,
    timeout: int | None = None,
) -> list[dict[str, Any]] | dict[str, Any]:
    """
    Create or update a batch of project jobs from a YAML file via SDK client model.

    Single source of truth:
      - delegates file validation, payload normalization, and request behavior to
        `Job.bulk_get_or_create()`

    Response can be either:
      - a list of jobs, or
      - a summary dict with counts/results/deleted/not_deleted details from `sync_jobs`
    """
    tokens = get_tokens()
    access = (tokens.get("access") or "").strip()
    refresh = (tokens.get("refresh") or "").strip()
    if not access:
        raise NotLoggedIn("Not logged in.")

    endpoint = backend_url().rstrip("/")
    root_url = f"{endpoint}/orm/api"

    old_env = {
        "MAINSEQUENCE_AUTH_MODE": os.environ.get("MAINSEQUENCE_AUTH_MODE"),
        "MAINSEQUENCE_ACCESS_TOKEN": os.environ.get("MAINSEQUENCE_ACCESS_TOKEN"),
        "MAINSEQUENCE_REFRESH_TOKEN": os.environ.get("MAINSEQUENCE_REFRESH_TOKEN"),
        "TDAG_ENDPOINT": os.environ.get("TDAG_ENDPOINT"),
        "MAINSEQUENCE_ENDPOINT": os.environ.get("MAINSEQUENCE_ENDPOINT"),
        "MAIN_SEQUENCE_PROJECT_ID": os.environ.get("MAIN_SEQUENCE_PROJECT_ID"),
    }

    client_utils = None
    old_provider = None
    old_base_root_url = None
    old_job_root_url = None

    try:
        os.environ["MAINSEQUENCE_AUTH_MODE"] = "jwt"
        os.environ["MAINSEQUENCE_ACCESS_TOKEN"] = access
        if refresh:
            os.environ["MAINSEQUENCE_REFRESH_TOKEN"] = refresh
        else:
            os.environ.pop("MAINSEQUENCE_REFRESH_TOKEN", None)
        os.environ["TDAG_ENDPOINT"] = endpoint
        os.environ["MAINSEQUENCE_ENDPOINT"] = endpoint
        os.environ["MAIN_SEQUENCE_PROJECT_ID"] = str(project_id)

        from mainsequence.client import utils as _client_utils
        from mainsequence.client.base import BaseObjectOrm
        from mainsequence.client.models_helpers import Job as ClientJob

        client_utils = _client_utils
        old_provider = getattr(client_utils.loaders, "provider", None)
        old_base_root_url = BaseObjectOrm.ROOT_URL
        old_job_root_url = getattr(ClientJob, "ROOT_URL", None)

        client_utils.TDAG_ENDPOINT = endpoint
        client_utils.API_ENDPOINT = root_url
        client_utils.loaders.use_jwt(access=access, refresh=refresh or None)

        BaseObjectOrm.ROOT_URL = root_url
        ClientJob.ROOT_URL = root_url

        created = ClientJob.bulk_get_or_create(
            yaml_file=file_path,
            project_id=int(project_id),
            strict=bool(strict),
            timeout=timeout,
        )
        if isinstance(created, list):
            out: list[dict[str, Any]] = []
            for item in created:
                if hasattr(item, "model_dump"):
                    out.append(item.model_dump())
                elif isinstance(item, dict):
                    out.append(item)
                else:
                    out.append({"id": getattr(item, "id", None)})
            return out
        if isinstance(created, dict):
            return created
        return {"result": created}

    except Exception as e:
        err_name = type(e).__name__
        if err_name in {"AuthenticationError", "PermissionDeniedError"}:
            raise NotLoggedIn(str(e) or "Not logged in.")
        if err_name == "NotFoundError":
            raise ApiError(f"Project not found: {project_id}")
        raise ApiError(f"Project batch job scheduling failed: {e}")
    finally:
        if client_utils is not None:
            try:
                client_utils.loaders.provider = old_provider
            except Exception:
                pass
        if old_base_root_url is not None:
            try:
                from mainsequence.client.base import BaseObjectOrm

                BaseObjectOrm.ROOT_URL = old_base_root_url
            except Exception:
                pass
        if old_job_root_url is not None:
            try:
                from mainsequence.client.models_helpers import Job as ClientJob

                ClientJob.ROOT_URL = old_job_root_url
            except Exception:
                pass

        for k, v in old_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def run_project_job(
    job_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Run a project job via SDK client model.

    Single source of truth:
      - delegates request behavior to `Job.run_job()`
    """
    tokens = get_tokens()
    access = (tokens.get("access") or "").strip()
    refresh = (tokens.get("refresh") or "").strip()
    if not access:
        raise NotLoggedIn("Not logged in.")

    endpoint = backend_url().rstrip("/")
    root_url = f"{endpoint}/orm/api"

    old_env = {
        "MAINSEQUENCE_AUTH_MODE": os.environ.get("MAINSEQUENCE_AUTH_MODE"),
        "MAINSEQUENCE_ACCESS_TOKEN": os.environ.get("MAINSEQUENCE_ACCESS_TOKEN"),
        "MAINSEQUENCE_REFRESH_TOKEN": os.environ.get("MAINSEQUENCE_REFRESH_TOKEN"),
        "TDAG_ENDPOINT": os.environ.get("TDAG_ENDPOINT"),
        "MAINSEQUENCE_ENDPOINT": os.environ.get("MAINSEQUENCE_ENDPOINT"),
    }

    client_utils = None
    old_provider = None
    old_base_root_url = None
    old_job_root_url = None

    try:
        os.environ["MAINSEQUENCE_AUTH_MODE"] = "jwt"
        os.environ["MAINSEQUENCE_ACCESS_TOKEN"] = access
        if refresh:
            os.environ["MAINSEQUENCE_REFRESH_TOKEN"] = refresh
        else:
            os.environ.pop("MAINSEQUENCE_REFRESH_TOKEN", None)
        os.environ["TDAG_ENDPOINT"] = endpoint
        os.environ["MAINSEQUENCE_ENDPOINT"] = endpoint

        from mainsequence.client import utils as _client_utils
        from mainsequence.client.base import BaseObjectOrm
        from mainsequence.client.models_helpers import Job as ClientJob

        client_utils = _client_utils
        old_provider = getattr(client_utils.loaders, "provider", None)
        old_base_root_url = BaseObjectOrm.ROOT_URL
        old_job_root_url = getattr(ClientJob, "ROOT_URL", None)

        client_utils.TDAG_ENDPOINT = endpoint
        client_utils.API_ENDPOINT = root_url
        client_utils.loaders.use_jwt(access=access, refresh=refresh or None)

        BaseObjectOrm.ROOT_URL = root_url
        ClientJob.ROOT_URL = root_url

        job = ClientJob.get(pk=int(job_id), timeout=timeout)
        payload = job.run_job(timeout=timeout)
        if isinstance(payload, dict):
            return payload
        if hasattr(payload, "model_dump"):
            return payload.model_dump()
        return {"job_id": int(job_id)}

    except Exception as e:
        err_name = type(e).__name__
        if err_name in {"AuthenticationError", "PermissionDeniedError"}:
            raise NotLoggedIn(str(e) or "Not logged in.")
        if err_name == "NotFoundError":
            raise ApiError(f"Job not found: {job_id}")
        raise ApiError(f"Project job run failed: {e}")
    finally:
        if client_utils is not None:
            try:
                client_utils.loaders.provider = old_provider
            except Exception:
                pass
        if old_base_root_url is not None:
            try:
                from mainsequence.client.base import BaseObjectOrm

                BaseObjectOrm.ROOT_URL = old_base_root_url
            except Exception:
                pass
        if old_job_root_url is not None:
            try:
                from mainsequence.client.models_helpers import Job as ClientJob

                ClientJob.ROOT_URL = old_job_root_url
            except Exception:
                pass

        for k, v in old_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def list_project_job_runs(
    *,
    job_id: int | str,
    filters: dict[str, Any] | None = None,
    timeout: int | None = None,
) -> list[dict[str, Any]]:
    """
    List job runs via SDK client model.

    Single source of truth:
      - delegates response parsing to `JobRun.filter()`
    """
    tokens = get_tokens()
    access = (tokens.get("access") or "").strip()
    refresh = (tokens.get("refresh") or "").strip()
    if not access:
        raise NotLoggedIn("Not logged in.")

    endpoint = backend_url().rstrip("/")
    root_url = f"{endpoint}/orm/api"

    old_env = {
        "MAINSEQUENCE_AUTH_MODE": os.environ.get("MAINSEQUENCE_AUTH_MODE"),
        "MAINSEQUENCE_ACCESS_TOKEN": os.environ.get("MAINSEQUENCE_ACCESS_TOKEN"),
        "MAINSEQUENCE_REFRESH_TOKEN": os.environ.get("MAINSEQUENCE_REFRESH_TOKEN"),
        "TDAG_ENDPOINT": os.environ.get("TDAG_ENDPOINT"),
        "MAINSEQUENCE_ENDPOINT": os.environ.get("MAINSEQUENCE_ENDPOINT"),
    }

    client_utils = None
    old_provider = None
    old_base_root_url = None
    old_job_run_root_url = None

    try:
        os.environ["MAINSEQUENCE_AUTH_MODE"] = "jwt"
        os.environ["MAINSEQUENCE_ACCESS_TOKEN"] = access
        if refresh:
            os.environ["MAINSEQUENCE_REFRESH_TOKEN"] = refresh
        else:
            os.environ.pop("MAINSEQUENCE_REFRESH_TOKEN", None)
        os.environ["TDAG_ENDPOINT"] = endpoint
        os.environ["MAINSEQUENCE_ENDPOINT"] = endpoint

        from mainsequence.client import utils as _client_utils
        from mainsequence.client.base import BaseObjectOrm
        from mainsequence.client.models_helpers import JobRun as ClientJobRun

        client_utils = _client_utils
        old_provider = getattr(client_utils.loaders, "provider", None)
        old_base_root_url = BaseObjectOrm.ROOT_URL
        old_job_run_root_url = getattr(ClientJobRun, "ROOT_URL", None)

        client_utils.TDAG_ENDPOINT = endpoint
        client_utils.API_ENDPOINT = root_url
        client_utils.loaders.use_jwt(access=access, refresh=refresh or None)

        BaseObjectOrm.ROOT_URL = root_url
        ClientJobRun.ROOT_URL = root_url

        merged_filters = dict(filters or {})
        merged_filters["job__id"] = [int(job_id)]
        runs = ClientJobRun.filter(timeout=timeout, **merged_filters)
        out: list[dict[str, Any]] = []
        for run in runs:
            if isinstance(run, dict):
                out.append(run)
            elif hasattr(run, "model_dump"):
                out.append(run.model_dump())
            else:
                out.append({"id": getattr(run, "id", None)})
        return out

    except Exception as e:
        err_name = type(e).__name__
        if err_name in {"AuthenticationError", "PermissionDeniedError"}:
            raise NotLoggedIn(str(e) or "Not logged in.")
        if err_name == "NotFoundError":
            raise ApiError(f"Job not found: {job_id}")
        raise ApiError(f"Project job runs fetch failed: {e}")
    finally:
        if client_utils is not None:
            try:
                client_utils.loaders.provider = old_provider
            except Exception:
                pass
        if old_base_root_url is not None:
            try:
                from mainsequence.client.base import BaseObjectOrm

                BaseObjectOrm.ROOT_URL = old_base_root_url
            except Exception:
                pass
        if old_job_run_root_url is not None:
            try:
                from mainsequence.client.models_helpers import JobRun as ClientJobRun

                ClientJobRun.ROOT_URL = old_job_run_root_url
            except Exception:
                pass

        for k, v in old_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def get_project_job_run_logs(
    job_run_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Fetch job run logs via SDK client model.

    Single source of truth:
      - delegates request behavior to `JobRun.get_logs()`
    """
    tokens = get_tokens()
    access = (tokens.get("access") or "").strip()
    refresh = (tokens.get("refresh") or "").strip()
    if not access:
        raise NotLoggedIn("Not logged in.")

    endpoint = backend_url().rstrip("/")
    root_url = f"{endpoint}/orm/api"

    old_env = {
        "MAINSEQUENCE_AUTH_MODE": os.environ.get("MAINSEQUENCE_AUTH_MODE"),
        "MAINSEQUENCE_ACCESS_TOKEN": os.environ.get("MAINSEQUENCE_ACCESS_TOKEN"),
        "MAINSEQUENCE_REFRESH_TOKEN": os.environ.get("MAINSEQUENCE_REFRESH_TOKEN"),
        "TDAG_ENDPOINT": os.environ.get("TDAG_ENDPOINT"),
        "MAINSEQUENCE_ENDPOINT": os.environ.get("MAINSEQUENCE_ENDPOINT"),
    }

    client_utils = None
    old_provider = None
    old_base_root_url = None
    old_job_run_root_url = None

    try:
        os.environ["MAINSEQUENCE_AUTH_MODE"] = "jwt"
        os.environ["MAINSEQUENCE_ACCESS_TOKEN"] = access
        if refresh:
            os.environ["MAINSEQUENCE_REFRESH_TOKEN"] = refresh
        else:
            os.environ.pop("MAINSEQUENCE_REFRESH_TOKEN", None)
        os.environ["TDAG_ENDPOINT"] = endpoint
        os.environ["MAINSEQUENCE_ENDPOINT"] = endpoint

        from mainsequence.client import utils as _client_utils
        from mainsequence.client.base import BaseObjectOrm
        from mainsequence.client.models_helpers import JobRun as ClientJobRun

        client_utils = _client_utils
        old_provider = getattr(client_utils.loaders, "provider", None)
        old_base_root_url = BaseObjectOrm.ROOT_URL
        old_job_run_root_url = getattr(ClientJobRun, "ROOT_URL", None)

        client_utils.TDAG_ENDPOINT = endpoint
        client_utils.API_ENDPOINT = root_url
        client_utils.loaders.use_jwt(access=access, refresh=refresh or None)

        BaseObjectOrm.ROOT_URL = root_url
        ClientJobRun.ROOT_URL = root_url

        job_run = ClientJobRun.get(pk=int(job_run_id), timeout=timeout)
        payload = job_run.get_logs(timeout=timeout)
        if isinstance(payload, dict):
            return payload
        if hasattr(payload, "model_dump"):
            return payload.model_dump()
        return {"job_run_id": int(job_run_id), "rows": []}

    except Exception as e:
        err_name = type(e).__name__
        if err_name in {"AuthenticationError", "PermissionDeniedError"}:
            raise NotLoggedIn(str(e) or "Not logged in.")
        if err_name == "NotFoundError":
            raise ApiError(f"Job run not found: {job_run_id}")
        raise ApiError(f"Project job run logs fetch failed: {e}")
    finally:
        if client_utils is not None:
            try:
                client_utils.loaders.provider = old_provider
            except Exception:
                pass
        if old_base_root_url is not None:
            try:
                from mainsequence.client.base import BaseObjectOrm

                BaseObjectOrm.ROOT_URL = old_base_root_url
            except Exception:
                pass
        if old_job_run_root_url is not None:
            try:
                from mainsequence.client.models_helpers import JobRun as ClientJobRun

                ClientJobRun.ROOT_URL = old_job_run_root_url
            except Exception:
                pass

        for k, v in old_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


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
