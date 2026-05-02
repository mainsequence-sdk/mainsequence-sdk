from __future__ import annotations

"""
mainsequence.cli.api
====================

HTTP API wrapper for MainSequence CLI.

This module is intentionally aligned with the VS Code extension implementation:
- Browser-based code exchange + refresh
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
    "authorize": "/auth/cli/authorize/",
    "cli_token": "/auth/cli/token/",
    "refresh": "/auth/jwt-token/token/refresh/",
    "logout": "/auth/jwt-token/logout/",
    "ping": "/auth/rest-auth/user/",
}
CLI_BROWSER_CLIENT_ID = "mainsequence-cli"

S = requests.Session()
S.headers.update({"Content-Type": "application/json"})
_UNSET = object()


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


def _set_client_utils_endpoint(client_utils, endpoint: str) -> None:
    """
    Update client utils endpoint globals for in-process SDK operations.

    Keep a defensive fallback for lightweight test doubles that do not implement
    the full helper surface.
    """
    if hasattr(client_utils, "set_mainsequence_endpoint"):
        client_utils.set_mainsequence_endpoint(endpoint)
        return

    normalized = endpoint.rstrip("/")
    client_utils.MAINSEQUENCE_ENDPOINT = normalized
    client_utils.API_ENDPOINT = f"{normalized}/orm/api"
    client_utils.AUTH_ENDPOINT = normalized


def build_cli_authorize_url(
    *,
    redirect_uri: str,
    state: str,
    code_challenge: str,
    client_id: str = CLI_BROWSER_CLIENT_ID,
) -> str:
    """
    Build the browser authorization URL for CLI OAuth-style login.
    """
    params = {
        "client_id": client_id,
        "response_type": "code",
        "state": state,
        "redirect_uri": redirect_uri,
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
    }
    return f"{_full(AUTH_PATHS['authorize'])}?{urlencode(params)}"


def exchange_cli_authorization_code(
    *,
    code: str,
    code_verifier: str,
    redirect_uri: str,
    client_id: str = CLI_BROWSER_CLIENT_ID,
) -> dict:
    """
    Exchange a browser login authorization code for access/refresh JWT tokens.
    """
    payload = {
        "client_id": client_id,
        "code": code,
        "code_verifier": code_verifier,
        "redirect_uri": redirect_uri,
    }
    r = S.post(_full(AUTH_PATHS["cli_token"]), data=json.dumps(payload))
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

    return {
        "backend": backend_url(),
        "access": str(access),
        "refresh": str(refresh),
    }


def logout_jwt_session() -> bool:
    """
    Attempt backend-side JWT logout for the current authenticated CLI session.

    Returns:
        bool: True when backend logout returns success, False otherwise.
    """
    access = _access_token()
    refresh = _refresh_token()
    if not access:
        return False

    payload = {"refresh": refresh} if refresh else {}
    headers = {"Authorization": f"Bearer {access}"}

    for _ in range(2):
        try:
            r = S.post(_full(AUTH_PATHS["logout"]), headers=headers, data=json.dumps(payload))
        except Exception:
            return False

        if r.status_code != 401:
            return bool(r.ok)

        try:
            access = refresh_access()
        except NotLoggedIn:
            return False
        headers = {"Authorization": f"Bearer {access}"}

    return False


def refresh_access() -> str:
    """
    Use refresh token to obtain a new access token and update session env.

    Raises:
        NotLoggedIn: if refresh is missing or refresh fails
    """
    refresh = _refresh_token()
    runtime_mode = (os.environ.get("MAINSEQUENCE_AUTH_MODE") or "").strip().lower() == "runtime_credential"

    if not refresh and runtime_mode:
        try:
            from mainsequence.client.utils import RuntimeCredentialAuthProvider
        except Exception as exc:
            raise NotLoggedIn(f"Runtime credential auth is unavailable: {exc}") from exc

        token_url = f"{backend_url().rstrip('/')}/orm/api/pods/runtime-credentials/token/"
        try:
            RuntimeCredentialAuthProvider(token_url=token_url).refresh(force=True)
        except Exception as exc:
            raise NotLoggedIn(f"Runtime credential exchange failed: {exc}") from exc

        access = (os.environ.get("MAINSEQUENCE_ACCESS_TOKEN") or "").strip()
        if not access:
            raise NotLoggedIn("Runtime credential exchange did not produce MAINSEQUENCE_ACCESS_TOKEN.")

        tokens = get_tokens()
        save_tokens(tokens.get("username") or "", access, "")
        return access

    if not refresh:
        raise NotLoggedIn("Not logged in. Run `mainsequence login`.")

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
    try:
        return json.dumps(value)
    except Exception:
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

        _set_client_utils_endpoint(client_utils, endpoint)
        client_utils.loaders.use_jwt(access=access, refresh=refresh or None)

        BaseObjectOrm.ROOT_URL = root_url
        client_model.ROOT_URL = root_url

        return operation(client_model)

    except Exception as e:
        err_name = type(e).__name__
        if err_name in {"AuthenticationError", "PermissionDeniedError"}:
            raise NotLoggedIn(str(e) or "Not logged in.") from e
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
    `X-User-ID` plus `Authorization` into
    `mainsequence.client.models_user._CURRENT_AUTH_HEADERS`
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

        _set_client_utils_endpoint(client_utils, endpoint)
        client_utils.loaders.use_jwt(access=access, refresh=refresh or None)

        BaseObjectOrm.ROOT_URL = root_url
        ClientUser.ROOT_URL = root_url
        headers_token = current_auth_headers.set(
            {
                "X-User-ID": str(user_id),
                "Authorization": f"Bearer {access}",
            }
        )

        user = ClientUser.get_logged_user()
        if isinstance(user, dict):
            return user
        if hasattr(user, "model_dump"):
            return user.model_dump()
        return {"id": getattr(user, "id", None)}

    except Exception as e:
        err_name = type(e).__name__
        if err_name in {"AuthenticationError", "PermissionDeniedError"}:
            raise NotLoggedIn(str(e) or "Not logged in.") from e
        raise ApiError(f"Current user fetch failed: {e}") from e
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


def list_org_project_names(
    *,
    timeout: int | None = None,
) -> list[str]:
    """
    List organization-visible project names via SDK client model.

    Single source of truth:
      - delegates payload parsing to `Project.get_org_project_names()`
    """
    try:
        payload = _run_sdk_model_operation(
            module_name="mainsequence.client.models_tdag",
            class_name="Project",
            operation=lambda ClientProject: ClientProject.get_org_project_names(timeout=timeout),
        )
        return [str(item) for item in list(payload or [])]
    except Exception as e:
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Organization project names fetch failed: {e}") from e


def validate_project_name(
    *,
    project_name: str,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Validate whether a project name is available for creation.

    Single source of truth:
      - delegates payload parsing to `Project.validate_name()`
    """
    try:
        payload = _run_sdk_model_operation(
            module_name="mainsequence.client.models_tdag",
            class_name="Project",
            operation=lambda ClientProject: ClientProject.validate_name(
                project_name=project_name,
                timeout=timeout,
            ),
        )
        return _sdk_object_to_dict(payload)
    except Exception as e:
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Project name validation failed: {e}") from e


def list_organization_teams(
    *,
    timeout: int | None = None,
    filters: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """
    List organization teams via SDK client model.

    Single source of truth:
      - delegates filtering and payload parsing to `Team.filter()`
    """
    try:
        payload = _run_sdk_model_operation(
            module_name="mainsequence.client.models_user",
            class_name="Team",
            operation=lambda ClientTeam: ClientTeam.filter(timeout=timeout, **(filters or {})),
        )
        return [_sdk_object_to_dict(item) for item in list(payload or [])]
    except Exception as e:
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Organization teams fetch failed: {e}") from e


def get_organization_team(
    team_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Retrieve one organization team via SDK client model.
    """
    try:
        team = _run_sdk_model_operation(
            module_name="mainsequence.client.models_user",
            class_name="Team",
            operation=lambda ClientTeam: ClientTeam.get(pk=int(team_id), timeout=timeout),
        )
        return _sdk_object_to_dict(team)
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"Team not found: {team_id}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Organization team fetch failed: {e}") from e


def create_organization_team(
    *,
    name: str,
    description: str = "",
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Create one organization team via SDK client model.
    """
    try:
        team = _run_sdk_model_operation(
            module_name="mainsequence.client.models_user",
            class_name="Team",
            operation=lambda ClientTeam: ClientTeam.create(
                name=name,
                description=description,
                timeout=timeout,
            ),
        )
        return _sdk_object_to_dict(team)
    except Exception as e:
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Organization team creation failed: {e}") from e


def update_organization_team(
    team_id: int | str,
    *,
    name: str | None = None,
    description: str | None = None,
    is_active: bool | None = None,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Update one organization team via SDK client model.
    """
    updates = {
        key: value
        for key, value in {
            "name": name,
            "description": description,
            "is_active": is_active,
        }.items()
        if value is not None
    }
    if not updates:
        raise ApiError("No team fields were provided to update.")

    try:
        team = _run_sdk_model_operation(
            module_name="mainsequence.client.models_user",
            class_name="Team",
            operation=lambda ClientTeam: ClientTeam.get(pk=int(team_id), timeout=timeout).patch(**updates),
        )
        return _sdk_object_to_dict(team)
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"Team not found: {team_id}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Organization team update failed: {e}") from e


def delete_organization_team(
    team_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Delete one organization team via SDK client model.
    """
    try:
        def _delete(ClientTeam):
            team = ClientTeam.get(pk=int(team_id), timeout=timeout)
            payload = _sdk_object_to_dict(team)
            team.delete(timeout=timeout)
            return payload

        return _run_sdk_model_operation(
            module_name="mainsequence.client.models_user",
            class_name="Team",
            operation=_delete,
        )
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"Team not found: {team_id}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Organization team deletion failed: {e}") from e


def list_agents(
    *,
    timeout: int | None = None,
    filters: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """
    List agents via SDK client model.
    """
    try:
        payload = _run_sdk_model_operation(
            module_name="mainsequence.client.agent_runtime_models",
            class_name="Agent",
            operation=lambda ClientAgent: ClientAgent.filter(timeout=timeout, **(filters or {})),
        )
        return [_sdk_object_to_dict(item) for item in list(payload or [])]
    except Exception as e:
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Agents fetch failed: {e}") from e


def get_agent(
    agent_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Retrieve one agent via SDK client model.
    """
    try:
        agent = _run_sdk_model_operation(
            module_name="mainsequence.client.agent_runtime_models",
            class_name="Agent",
            operation=lambda ClientAgent: ClientAgent.get(pk=int(agent_id), timeout=timeout),
        )
        return _sdk_object_to_dict(agent)
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"Agent not found: {agent_id}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Agent fetch failed: {e}") from e


def create_agent(
    *,
    name: str,
    agent_unique_id: str,
    description: str | None = None,
    status: str | None = None,
    labels: list[str] | None = None,
    llm_provider: str | None = None,
    llm_model: str | None = None,
    engine_name: str | None = None,
    runtime_config: dict[str, Any] | None = None,
    configuration: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Create one agent via SDK client model.
    """
    payload = {
        key: value
        for key, value in {
            "name": name,
            "agent_unique_id": agent_unique_id,
            "description": description,
            "status": status,
            "labels": labels,
            "llm_provider": llm_provider,
            "llm_model": llm_model,
            "engine_name": engine_name,
            "runtime_config": runtime_config,
            "configuration": configuration,
            "metadata": metadata,
        }.items()
        if value is not None
    }

    try:
        agent = _run_sdk_model_operation(
            module_name="mainsequence.client.agent_runtime_models",
            class_name="Agent",
            operation=lambda ClientAgent: ClientAgent.create(timeout=timeout, **payload),
        )
        return _sdk_object_to_dict(agent)
    except Exception as e:
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Agent creation failed: {e}") from e


def get_or_create_agent(
    *,
    name: str,
    agent_unique_id: str,
    description: str | None = None,
    status: str | None = None,
    labels: list[str] | None = None,
    llm_provider: str | None = None,
    llm_model: str | None = None,
    engine_name: str | None = None,
    runtime_config: dict[str, Any] | None = None,
    configuration: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Get or create one agent via SDK client model.
    """
    payload = {
        key: value
        for key, value in {
            "name": name,
            "agent_unique_id": agent_unique_id,
            "description": description,
            "status": status,
            "labels": labels,
            "llm_provider": llm_provider,
            "llm_model": llm_model,
            "engine_name": engine_name,
            "runtime_config": runtime_config,
            "configuration": configuration,
            "metadata": metadata,
        }.items()
        if value is not None
    }

    try:
        agent = _run_sdk_model_operation(
            module_name="mainsequence.client.agent_runtime_models",
            class_name="Agent",
            operation=lambda ClientAgent: ClientAgent.get_or_create(timeout=timeout, **payload),
        )
        return _sdk_object_to_dict(agent)
    except Exception as e:
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Agent get_or_create failed: {e}") from e


def delete_agent(
    agent_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Delete one agent via SDK client model.
    """
    try:
        def _delete(ClientAgent):
            agent = ClientAgent.get(pk=int(agent_id), timeout=timeout)
            payload = _sdk_object_to_dict(agent)
            agent.delete(timeout=timeout)
            return payload

        return _run_sdk_model_operation(
            module_name="mainsequence.client.agent_runtime_models",
            class_name="Agent",
            operation=_delete,
        )
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"Agent not found: {agent_id}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Agent deletion failed: {e}") from e


def start_agent_new_session(
    agent_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Start a new session for one agent via SDK client model.
    """
    try:
        def _start(ClientAgent):
            agent = ClientAgent.get(pk=int(agent_id), timeout=timeout)
            return agent.start_new_session(timeout=timeout)

        session = _run_sdk_model_operation(
            module_name="mainsequence.client.agent_runtime_models",
            class_name="Agent",
            operation=_start,
        )
        return _sdk_object_to_dict(session)
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"Agent not found: {agent_id}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Agent session start failed: {e}") from e


def get_agent_latest_session(
    agent_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Retrieve the latest session for one agent via SDK client model.
    """
    try:
        def _get_latest(ClientAgent):
            agent = ClientAgent.get(pk=int(agent_id), timeout=timeout)
            return agent.get_latest_session(timeout=timeout)

        session = _run_sdk_model_operation(
            module_name="mainsequence.client.agent_runtime_models",
            class_name="Agent",
            operation=_get_latest,
        )
        return _sdk_object_to_dict(session)
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"Agent not found: {agent_id}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Agent latest session fetch failed: {e}") from e


def get_agent_session(
    agent_session_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Retrieve one agent session via SDK client model.
    """
    try:
        agent_session = _run_sdk_model_operation(
            module_name="mainsequence.client.agent_runtime_models",
            class_name="AgentSession",
            operation=lambda ClientAgentSession: ClientAgentSession.get(
                pk=int(agent_session_id), timeout=timeout
            ),
        )
        return _sdk_object_to_dict(agent_session)
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"Agent session not found: {agent_session_id}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Agent session fetch failed: {e}") from e


def list_agent_users_can_view(
    agent_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Fetch the view-access state for an agent via `ShareableObjectMixin.can_view()`.
    """
    return _get_shareable_object_access_state(
        module_name="mainsequence.client.agent_runtime_models",
        class_name="Agent",
        object_id=agent_id,
        accessor_name="can_view",
        timeout=timeout,
    )


def list_agent_users_can_edit(
    agent_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Fetch the edit-access state for an agent via `ShareableObjectMixin.can_edit()`.
    """
    return _get_shareable_object_access_state(
        module_name="mainsequence.client.agent_runtime_models",
        class_name="Agent",
        object_id=agent_id,
        accessor_name="can_edit",
        timeout=timeout,
    )


def add_agent_user_to_view(
    agent_id: int | str,
    user_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    return _mutate_shareable_object_access(
        module_name="mainsequence.client.agent_runtime_models",
        class_name="Agent",
        object_id=agent_id,
        action_name="add_to_view",
        user_id=user_id,
        timeout=timeout,
    )


def add_agent_user_to_edit(
    agent_id: int | str,
    user_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    return _mutate_shareable_object_access(
        module_name="mainsequence.client.agent_runtime_models",
        class_name="Agent",
        object_id=agent_id,
        action_name="add_to_edit",
        user_id=user_id,
        timeout=timeout,
    )


def remove_agent_user_from_view(
    agent_id: int | str,
    user_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    return _mutate_shareable_object_access(
        module_name="mainsequence.client.agent_runtime_models",
        class_name="Agent",
        object_id=agent_id,
        action_name="remove_from_view",
        user_id=user_id,
        timeout=timeout,
    )


def remove_agent_user_from_edit(
    agent_id: int | str,
    user_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    return _mutate_shareable_object_access(
        module_name="mainsequence.client.agent_runtime_models",
        class_name="Agent",
        object_id=agent_id,
        action_name="remove_from_edit",
        user_id=user_id,
        timeout=timeout,
    )


def add_agent_team_to_view(
    agent_id: int | str,
    team_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    return _mutate_shareable_object_team_access(
        module_name="mainsequence.client.agent_runtime_models",
        class_name="Agent",
        object_id=agent_id,
        action_name="add_team_to_view",
        team_id=team_id,
        timeout=timeout,
    )


def add_agent_team_to_edit(
    agent_id: int | str,
    team_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    return _mutate_shareable_object_team_access(
        module_name="mainsequence.client.agent_runtime_models",
        class_name="Agent",
        object_id=agent_id,
        action_name="add_team_to_edit",
        team_id=team_id,
        timeout=timeout,
    )


def remove_agent_team_from_view(
    agent_id: int | str,
    team_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    return _mutate_shareable_object_team_access(
        module_name="mainsequence.client.agent_runtime_models",
        class_name="Agent",
        object_id=agent_id,
        action_name="remove_team_from_view",
        team_id=team_id,
        timeout=timeout,
    )


def remove_agent_team_from_edit(
    agent_id: int | str,
    team_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    return _mutate_shareable_object_team_access(
        module_name="mainsequence.client.agent_runtime_models",
        class_name="Agent",
        object_id=agent_id,
        action_name="remove_team_from_edit",
        team_id=team_id,
        timeout=timeout,
    )


def list_agent_runs(
    *,
    timeout: int | None = None,
    filters: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """
    List agent runs via SDK client model.
    """
    try:
        payload = _run_sdk_model_operation(
            module_name="mainsequence.client.agent_runtime_models",
            class_name="AgentRun",
            operation=lambda ClientAgentRun: ClientAgentRun.filter(timeout=timeout, **(filters or {})),
        )
        return [_sdk_object_to_dict(item) for item in list(payload or [])]
    except Exception as e:
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Agent runs fetch failed: {e}") from e


def get_agent_run(
    agent_run_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Retrieve one agent run via SDK client model.
    """
    try:
        agent_run = _run_sdk_model_operation(
            module_name="mainsequence.client.agent_runtime_models",
            class_name="AgentRun",
            operation=lambda ClientAgentRun: ClientAgentRun.get(pk=int(agent_run_id), timeout=timeout),
        )
        return _sdk_object_to_dict(agent_run)
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"Agent run not found: {agent_run_id}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Agent run fetch failed: {e}") from e


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


def list_team_users_can_view(
    team_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Fetch the view-access state for a team via `Team.can_view()`.
    """
    return _get_shareable_object_access_state(
        module_name="mainsequence.client.models_user",
        class_name="Team",
        object_id=team_id,
        accessor_name="can_view",
        timeout=timeout,
    )


def list_team_users_can_edit(
    team_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Fetch the edit-access state for a team via `Team.can_edit()`.
    """
    return _get_shareable_object_access_state(
        module_name="mainsequence.client.models_user",
        class_name="Team",
        object_id=team_id,
        accessor_name="can_edit",
        timeout=timeout,
    )


def add_team_user_to_view(
    team_id: int | str,
    user_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    return _mutate_shareable_object_access(
        module_name="mainsequence.client.models_user",
        class_name="Team",
        object_id=team_id,
        action_name="add_to_view",
        user_id=user_id,
        timeout=timeout,
    )


def add_team_user_to_edit(
    team_id: int | str,
    user_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    return _mutate_shareable_object_access(
        module_name="mainsequence.client.models_user",
        class_name="Team",
        object_id=team_id,
        action_name="add_to_edit",
        user_id=user_id,
        timeout=timeout,
    )


def remove_team_user_from_view(
    team_id: int | str,
    user_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    return _mutate_shareable_object_access(
        module_name="mainsequence.client.models_user",
        class_name="Team",
        object_id=team_id,
        action_name="remove_from_view",
        user_id=user_id,
        timeout=timeout,
    )


def remove_team_user_from_edit(
    team_id: int | str,
    user_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    return _mutate_shareable_object_access(
        module_name="mainsequence.client.models_user",
        class_name="Team",
        object_id=team_id,
        action_name="remove_from_edit",
        user_id=user_id,
        timeout=timeout,
    )


def add_project_team_to_view(
    project_id: int | str,
    team_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    return _mutate_shareable_object_team_access(
        module_name="mainsequence.client.models_tdag",
        class_name="Project",
        object_id=project_id,
        action_name="add_team_to_view",
        team_id=team_id,
        timeout=timeout,
    )


def add_project_team_to_edit(
    project_id: int | str,
    team_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    return _mutate_shareable_object_team_access(
        module_name="mainsequence.client.models_tdag",
        class_name="Project",
        object_id=project_id,
        action_name="add_team_to_edit",
        team_id=team_id,
        timeout=timeout,
    )


def remove_project_team_from_view(
    project_id: int | str,
    team_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    return _mutate_shareable_object_team_access(
        module_name="mainsequence.client.models_tdag",
        class_name="Project",
        object_id=project_id,
        action_name="remove_team_from_view",
        team_id=team_id,
        timeout=timeout,
    )


def remove_project_team_from_edit(
    project_id: int | str,
    team_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    return _mutate_shareable_object_team_access(
        module_name="mainsequence.client.models_tdag",
        class_name="Project",
        object_id=project_id,
        action_name="remove_team_from_edit",
        team_id=team_id,
        timeout=timeout,
    )


def add_project_labels(
    project_id: int | str,
    labels: list[str],
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """Attach one or more organizational labels to a project."""

    return _mutate_labelable_object_labels(
        module_name="mainsequence.client.models_tdag",
        class_name="Project",
        object_id=project_id,
        action_name="add_label",
        labels=labels,
        timeout=timeout,
    )


def remove_project_labels(
    project_id: int | str,
    labels: list[str],
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """Remove one or more organizational labels from a project."""

    return _mutate_labelable_object_labels(
        module_name="mainsequence.client.models_tdag",
        class_name="Project",
        object_id=project_id,
        action_name="remove_label",
        labels=labels,
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
        os.environ["MAINSEQUENCE_ENDPOINT"] = endpoint
        os.environ["MAIN_SEQUENCE_PROJECT_ID"] = str(project_id)

        from mainsequence.client import utils as _client_utils
        from mainsequence.client.base import BaseObjectOrm
        from mainsequence.client.models_tdag import Project as ClientProject

        client_utils = _client_utils
        old_provider = getattr(client_utils.loaders, "provider", None)
        old_base_root_url = BaseObjectOrm.ROOT_URL
        old_project_root_url = getattr(ClientProject, "ROOT_URL", None)

        _set_client_utils_endpoint(client_utils, endpoint)
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
            raise NotLoggedIn(str(e) or "Not logged in.") from e
        if err_name == "NotFoundError":
            raise ApiError(f"Project not found: {project_id}") from e
        raise ApiError(f"Data node updates fetch failed: {e}") from e
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
    try:
        payload = _run_sdk_model_operation(
            module_name="mainsequence.client.models_tdag",
            class_name="Project",
            operation=lambda ClientProject: ClientProject.sync_project_after_commit(
                int(project_id),
                timeout=timeout,
            ),
            project_id_env=project_id,
        )
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
            raise NotLoggedIn(str(e) or "Not logged in.") from e
        if err_name == "NotFoundError":
            raise ApiError(f"Project not found: {project_id}") from e
        raise ApiError(f"Project post-commit sync failed: {e}") from e


def prime_sync_project_after_commit_sdk() -> None:
    """
    Load the SDK modules needed by `sync_project_after_commit()` before the active
    virtual environment is mutated by `uv sync`.

    `project sync` runs inside the same `.venv` that it later updates. If the
    environment changes during the command, a fresh `mainsequence.client` import
    can fail late in the flow. Priming the relevant SDK modules first keeps the
    post-commit sync path available in the current process.
    """
    try:
        importlib.import_module("mainsequence.client")
        importlib.import_module("mainsequence.client.utils")
        importlib.import_module("mainsequence.client.base")
        importlib.import_module("mainsequence.client.models_tdag")
    except Exception as e:
        raise ApiError(f"Project post-commit SDK import failed: {e}") from e


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
        os.environ["MAINSEQUENCE_ENDPOINT"] = endpoint
        os.environ["MAIN_SEQUENCE_PROJECT_ID"] = str(related_project_id)

        from mainsequence.client import utils as _client_utils
        from mainsequence.client.base import BaseObjectOrm
        from mainsequence.client.models_tdag import ProjectImage as ClientProjectImage

        client_utils = _client_utils
        old_provider = getattr(client_utils.loaders, "provider", None)
        old_base_root_url = BaseObjectOrm.ROOT_URL
        old_image_root_url = getattr(ClientProjectImage, "ROOT_URL", None)

        _set_client_utils_endpoint(client_utils, endpoint)
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
            raise NotLoggedIn(str(e) or "Not logged in.") from e
        if err_name == "NotFoundError":
            raise ApiError(f"Project not found: {related_project_id}") from e
        raise ApiError(f"Project image create failed: {e}") from e
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
        os.environ["MAINSEQUENCE_ENDPOINT"] = endpoint
        os.environ["MAIN_SEQUENCE_PROJECT_ID"] = str(related_project_id)

        from mainsequence.client import utils as _client_utils
        from mainsequence.client.base import BaseObjectOrm
        from mainsequence.client.models_tdag import ProjectImage as ClientProjectImage

        client_utils = _client_utils
        old_provider = getattr(client_utils.loaders, "provider", None)
        old_base_root_url = BaseObjectOrm.ROOT_URL
        old_image_root_url = getattr(ClientProjectImage, "ROOT_URL", None)

        _set_client_utils_endpoint(client_utils, endpoint)
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
            raise NotLoggedIn(str(e) or "Not logged in.") from e
        if err_name == "NotFoundError":
            raise ApiError(f"Project not found: {related_project_id}") from e
        raise ApiError(f"Project images fetch failed: {e}") from e
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
            raise ApiError(f"Project image not found: {image_id}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Project image fetch failed: {e}") from e


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
            raise ApiError(f"Project image not found: {image_id}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Project image deletion failed: {e}") from e


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
            raise ApiError(f"Resource release not found: {release_id}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Resource release fetch failed: {e}") from e


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
            raise ApiError(f"Resource release not found: {release_id}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Resource release deletion failed: {e}") from e


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
        os.environ["MAINSEQUENCE_ENDPOINT"] = endpoint
        os.environ["MAIN_SEQUENCE_PROJECT_ID"] = str(project_id)

        from mainsequence.client import utils as _client_utils
        from mainsequence.client.base import BaseObjectOrm
        from mainsequence.client.models_helpers import Job as ClientJob

        client_utils = _client_utils
        old_provider = getattr(client_utils.loaders, "provider", None)
        old_base_root_url = BaseObjectOrm.ROOT_URL
        old_job_root_url = getattr(ClientJob, "ROOT_URL", None)

        _set_client_utils_endpoint(client_utils, endpoint)
        client_utils.loaders.use_jwt(access=access, refresh=refresh or None)

        BaseObjectOrm.ROOT_URL = root_url
        ClientJob.ROOT_URL = root_url

        extra_filters = dict(filters or {})
        jobs = ClientJob.filter(
            timeout=timeout,
            **{**extra_filters, "project__id": int(project_id)},
        )

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
            raise NotLoggedIn(str(e) or "Not logged in.") from e
        if err_name == "NotFoundError":
            raise ApiError(f"Project not found: {project_id}") from e
        raise ApiError(f"Project jobs fetch failed: {e}") from e
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
        os.environ["MAINSEQUENCE_ENDPOINT"] = endpoint
        os.environ["MAIN_SEQUENCE_PROJECT_ID"] = str(project_id)

        from mainsequence.client import utils as _client_utils
        from mainsequence.client.base import BaseObjectOrm
        from mainsequence.client.models_helpers import ProjectResource as ClientProjectResource

        client_utils = _client_utils
        old_provider = getattr(client_utils.loaders, "provider", None)
        old_base_root_url = BaseObjectOrm.ROOT_URL
        old_resource_root_url = getattr(ClientProjectResource, "ROOT_URL", None)

        _set_client_utils_endpoint(client_utils, endpoint)
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
            raise NotLoggedIn(str(e) or "Not logged in.") from e
        if err_name == "NotFoundError":
            raise ApiError(f"Project not found: {project_id}") from e
        raise ApiError(f"Project resources fetch failed: {e}") from e
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
      - delegates to `ProjectResource.create_dashboard()`, `ProjectResource.create_agent()`,
        or `ProjectResource.create_fastapi()`
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
        os.environ["MAINSEQUENCE_ENDPOINT"] = endpoint

        from mainsequence.client import utils as _client_utils
        from mainsequence.client.base import BaseObjectOrm
        from mainsequence.client.models_helpers import ProjectResource as ClientProjectResource

        client_utils = _client_utils
        old_provider = getattr(client_utils.loaders, "provider", None)
        old_base_root_url = BaseObjectOrm.ROOT_URL
        old_resource_root_url = getattr(ClientProjectResource, "ROOT_URL", None)

        _set_client_utils_endpoint(client_utils, endpoint)
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

        create_method_name = {
            "streamlit_dashboard": "create_dashboard",
            "agent": "create_agent",
            "fastapi": "create_fastapi",
        }.get(str(release_kind).strip())
        if not create_method_name:
            raise ApiError(
                "release_kind must be one of: 'streamlit_dashboard', 'agent', 'fastapi'."
            )

        create_method = getattr(resource, create_method_name, None)
        if not callable(create_method):
            raise ApiError(
                f"ProjectResource does not implement {create_method_name}()."
            )
        created = create_method(**create_kwargs)

        if isinstance(created, dict):
            return created
        if hasattr(created, "model_dump"):
            return created.model_dump()
        return {"id": getattr(created, "id", None)}

    except Exception as e:
        err_name = type(e).__name__
        if err_name in {"AuthenticationError", "PermissionDeniedError"}:
            raise NotLoggedIn(str(e) or "Not logged in.") from e
        if err_name == "NotFoundError":
            raise ApiError(f"Project resource not found: {resource_id}") from e
        raise ApiError(f"Project resource release create failed: {e}") from e
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
        os.environ["MAINSEQUENCE_ENDPOINT"] = endpoint

        from mainsequence.client import utils as _client_utils
        from mainsequence.client.base import BaseObjectOrm
        from mainsequence.client.models_vam import Portfolio as ClientPortfolio

        client_utils = _client_utils
        old_provider = getattr(client_utils.loaders, "provider", None)
        old_base_root_url = BaseObjectOrm.ROOT_URL
        old_portfolio_root_url = getattr(ClientPortfolio, "ROOT_URL", None)

        _set_client_utils_endpoint(client_utils, endpoint)
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
            raise NotLoggedIn(str(e) or "Not logged in.") from e
        raise ApiError(f"Markets portfolios fetch failed: {e}") from e
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
        raise ApiError(f"Data node storages fetch failed: {e}") from e


def list_simple_table_storages(
    *,
    filters: dict[str, Any] | None = None,
    timeout: int | None = None,
) -> list[dict[str, Any]]:
    """
    List simple table storages via SDK client model.

    Single source of truth:
      - delegates filtering and payload parsing to `SimpleTableStorage.filter()`
    """
    try:
        storages = _run_sdk_model_operation(
            module_name="mainsequence.client.models_simple_tables",
            class_name="SimpleTableStorage",
            operation=lambda ClientSimpleTableStorage: ClientSimpleTableStorage.filter(
                timeout=timeout,
                **dict(filters or {}),
            ),
        )
        return [_sdk_object_to_dict(storage) for storage in storages]
    except Exception as e:
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Simple table storages fetch failed: {e}") from e


def list_workspaces(
    *,
    filters: dict[str, Any] | None = None,
    timeout: int | None = None,
) -> list[dict[str, Any]]:
    """
    List command-center workspaces via SDK client model.
    """
    try:
        workspaces = _run_sdk_model_operation(
            module_name="mainsequence.client.command_center",
            class_name="Workspace",
            operation=lambda ClientWorkspace: ClientWorkspace.filter(
                timeout=timeout,
                **dict(filters or {}),
            ),
        )
        return [_sdk_object_to_dict(workspace) for workspace in workspaces]
    except Exception as e:
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Workspaces fetch failed: {e}") from e


def get_workspace(
    workspace_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Retrieve one workspace via SDK client model.
    """
    try:
        workspace = _run_sdk_model_operation(
            module_name="mainsequence.client.command_center",
            class_name="Workspace",
            operation=lambda ClientWorkspace: ClientWorkspace.get(
                pk=int(workspace_id),
                timeout=timeout,
            ),
        )
        return _sdk_object_to_dict(workspace)
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"Workspace not found: {workspace_id}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Workspace fetch failed: {e}") from e


def create_workspace(
    *,
    title: str,
    description: str = "",
    labels: list[str] | None = None,
    category: str = "Custom",
    source: str = "user",
    schema_version: int = 1,
    required_permissions: list[str] | None = None,
    grid: dict[str, Any] | None = None,
    layout_kind: str = "custom",
    auto_grid: dict[str, Any] | None = None,
    companions: list[dict[str, Any]] | None = None,
    controls: dict[str, Any] | None = None,
    widgets: list[dict[str, Any]] | None = None,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Create one command-center workspace via SDK client model.
    """
    try:
        workspace = _run_sdk_model_operation(
            module_name="mainsequence.client.command_center",
            class_name="Workspace",
            operation=lambda ClientWorkspace: ClientWorkspace.create(
                title=title,
                description=description,
                labels=list(labels or []),
                category=category,
                source=source,
                schemaVersion=schema_version,
                requiredPermissions=required_permissions,
                grid=dict(grid or {}),
                layoutKind=layout_kind,
                autoGrid=dict(auto_grid or {}),
                companions=list(companions or []),
                controls=dict(controls or {}),
                widgets=list(widgets or []),
                timeout=timeout,
            ),
        )
        return _sdk_object_to_dict(workspace)
    except Exception as e:
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Workspace creation failed: {e}") from e


def update_workspace(
    workspace_id: int | str,
    *,
    title: Any = _UNSET,
    description: Any = _UNSET,
    labels: Any = _UNSET,
    category: Any = _UNSET,
    source: Any = _UNSET,
    schema_version: Any = _UNSET,
    required_permissions: Any = _UNSET,
    grid: Any = _UNSET,
    layout_kind: Any = _UNSET,
    auto_grid: Any = _UNSET,
    companions: Any = _UNSET,
    controls: Any = _UNSET,
    widgets: Any = _UNSET,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Update one command-center workspace via SDK client model.
    """
    patch_kwargs: dict[str, Any] = {}
    if title is not _UNSET:
        patch_kwargs["title"] = title
    if description is not _UNSET:
        patch_kwargs["description"] = description
    if labels is not _UNSET:
        patch_kwargs["labels"] = None if labels is None else list(labels)
    if category is not _UNSET:
        patch_kwargs["category"] = category
    if source is not _UNSET:
        patch_kwargs["source"] = source
    if schema_version is not _UNSET:
        patch_kwargs["schemaVersion"] = schema_version
    if required_permissions is not _UNSET:
        patch_kwargs["requiredPermissions"] = required_permissions
    if grid is not _UNSET:
        patch_kwargs["grid"] = None if grid is None else dict(grid)
    if layout_kind is not _UNSET:
        patch_kwargs["layoutKind"] = layout_kind
    if auto_grid is not _UNSET:
        patch_kwargs["autoGrid"] = None if auto_grid is None else dict(auto_grid)
    if companions is not _UNSET:
        patch_kwargs["companions"] = None if companions is None else list(companions)
    if controls is not _UNSET:
        patch_kwargs["controls"] = None if controls is None else dict(controls)
    if widgets is not _UNSET:
        patch_kwargs["widgets"] = None if widgets is None else list(widgets)

    if not patch_kwargs:
        raise ApiError("Workspace update payload does not include any writable fields.")

    try:
        workspace = _run_sdk_model_operation(
            module_name="mainsequence.client.command_center",
            class_name="Workspace",
            operation=lambda ClientWorkspace: ClientWorkspace.get(
                pk=int(workspace_id),
                timeout=timeout,
            ).patch(**patch_kwargs),
        )
        return _sdk_object_to_dict(workspace)
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"Workspace not found: {workspace_id}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Workspace update failed: {e}") from e


def delete_workspace(
    workspace_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Delete one workspace via SDK client model.
    """
    try:
        def _delete(ClientWorkspace):
            workspace = ClientWorkspace.get(pk=int(workspace_id), timeout=timeout)
            payload = _sdk_object_to_dict(workspace)
            workspace.delete(timeout=timeout)
            return payload

        return _run_sdk_model_operation(
            module_name="mainsequence.client.command_center",
            class_name="Workspace",
            operation=_delete,
        )
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"Workspace not found: {workspace_id}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Workspace deletion failed: {e}") from e


def add_workspace_labels(
    workspace_id: int | str,
    labels: list[str],
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """Attach one or more organizational labels to a workspace."""

    return _mutate_labelable_object_labels(
        module_name="mainsequence.client.command_center",
        class_name="Workspace",
        object_id=workspace_id,
        action_name="add_label",
        labels=labels,
        timeout=timeout,
    )


def remove_workspace_labels(
    workspace_id: int | str,
    labels: list[str],
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """Remove one or more organizational labels from a workspace."""

    return _mutate_labelable_object_labels(
        module_name="mainsequence.client.command_center",
        class_name="Workspace",
        object_id=workspace_id,
        action_name="remove_label",
        labels=labels,
        timeout=timeout,
    )


def list_registered_widget_types(
    *,
    filters: dict[str, Any] | None = None,
    timeout: int | None = None,
) -> list[dict[str, Any]]:
    """
    List registered widget types via SDK client model.
    """
    try:
        widgets = _run_sdk_model_operation(
            module_name="mainsequence.client.command_center",
            class_name="RegisteredWidgetType",
            operation=lambda ClientWidgetType: ClientWidgetType.filter(
                timeout=timeout,
                **dict(filters or {}),
            ),
        )
        return [_sdk_object_to_dict(widget) for widget in widgets]
    except Exception as e:
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Registered widget types fetch failed: {e}") from e


def get_registered_widget_type(
    widget_id: str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Retrieve one registered widget type via SDK client model.
    """
    try:
        widget = _run_sdk_model_operation(
            module_name="mainsequence.client.command_center",
            class_name="RegisteredWidgetType",
            operation=lambda ClientWidgetType: ClientWidgetType.get(
                widget_id=str(widget_id),
                timeout=timeout,
            ),
        )
        return _sdk_object_to_dict(widget)
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"Registered widget type not found: {widget_id}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Registered widget type fetch failed: {e}") from e


def list_connection_types(
    *,
    filters: dict[str, Any] | None = None,
    timeout: int | None = None,
) -> list[dict[str, Any]]:
    """
    List Command Center connection types via SDK client model.
    """
    try:
        connection_types = _run_sdk_model_operation(
            module_name="mainsequence.client.command_center.connections",
            class_name="ConnectionType",
            operation=lambda ClientConnectionType: ClientConnectionType.filter(
                timeout=timeout,
                **dict(filters or {}),
            ),
        )
        return [_sdk_object_to_dict(connection_type) for connection_type in connection_types]
    except Exception as e:
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Connection types fetch failed: {e}") from e


def get_connection_type(
    type_id: str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Retrieve one Command Center connection type via SDK client model.
    """
    try:
        connection_type = _run_sdk_model_operation(
            module_name="mainsequence.client.command_center.connections",
            class_name="ConnectionType",
            operation=lambda ClientConnectionType: ClientConnectionType.get(
                type_id=str(type_id),
                timeout=timeout,
            ),
        )
        return _sdk_object_to_dict(connection_type)
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"Connection type not found: {type_id}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Connection type fetch failed: {e}") from e


def list_connection_instances(
    *,
    filters: dict[str, Any] | None = None,
    timeout: int | None = None,
) -> list[dict[str, Any]]:
    """
    List Command Center connection instances via SDK client model.
    """
    try:
        connections = _run_sdk_model_operation(
            module_name="mainsequence.client.command_center.connections",
            class_name="ConnectionInstance",
            operation=lambda ClientConnectionInstance: ClientConnectionInstance.filter(
                timeout=timeout,
                **dict(filters or {}),
            ),
        )
        return [_sdk_object_to_dict(connection) for connection in connections]
    except Exception as e:
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Connections fetch failed: {e}") from e


def get_connection_instance(
    connection_uid: str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Retrieve one Command Center connection instance via SDK client model.
    """
    try:
        connection = _run_sdk_model_operation(
            module_name="mainsequence.client.command_center.connections",
            class_name="ConnectionInstance",
            operation=lambda ClientConnectionInstance: ClientConnectionInstance.get(
                uid=str(connection_uid),
                timeout=timeout,
            ),
        )
        return _sdk_object_to_dict(connection)
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"Connection not found: {connection_uid}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Connection fetch failed: {e}") from e


def get_simple_table_storage(
    storage_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Retrieve one simple table storage via SDK client model.
    """
    try:
        storage = _run_sdk_model_operation(
            module_name="mainsequence.client.models_simple_tables",
            class_name="SimpleTableStorage",
            operation=lambda ClientSimpleTableStorage: ClientSimpleTableStorage.get(
                pk=int(storage_id),
                timeout=timeout,
            ),
        )
        return _sdk_object_to_dict(storage)
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"Simple table storage not found: {storage_id}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Simple table storage fetch failed: {e}") from e


def delete_simple_table_storage(
    storage_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Delete one simple table storage via SDK client model.
    """
    try:
        def _delete(ClientSimpleTableStorage):
            storage = ClientSimpleTableStorage.get(pk=int(storage_id), timeout=timeout)
            payload = _sdk_object_to_dict(storage)
            storage.delete(timeout=timeout)
            return payload

        return _run_sdk_model_operation(
            module_name="mainsequence.client.models_simple_tables",
            class_name="SimpleTableStorage",
            operation=_delete,
        )
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"Simple table storage not found: {storage_id}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Simple table storage deletion failed: {e}") from e


def add_simple_table_storage_labels(
    storage_id: int | str,
    labels: list[str],
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """Attach one or more organizational labels to a simple table storage."""

    return _mutate_labelable_object_labels(
        module_name="mainsequence.client.models_simple_tables",
        class_name="SimpleTableStorage",
        object_id=storage_id,
        action_name="add_label",
        labels=labels,
        timeout=timeout,
    )


def remove_simple_table_storage_labels(
    storage_id: int | str,
    labels: list[str],
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """Remove one or more organizational labels from a simple table storage."""

    return _mutate_labelable_object_labels(
        module_name="mainsequence.client.models_simple_tables",
        class_name="SimpleTableStorage",
        object_id=storage_id,
        action_name="remove_label",
        labels=labels,
        timeout=timeout,
    )


def _serialize_sdk_search_response(payload: Any) -> dict[str, Any] | list[dict[str, Any]]:
    if isinstance(payload, list):
        return [_sdk_object_to_dict(item) for item in payload]

    if isinstance(payload, dict):
        if isinstance(payload.get("results"), list):
            hydrated = dict(payload)
            hydrated["results"] = [_sdk_object_to_dict(item) for item in payload["results"]]
            return hydrated
        return _sdk_object_to_dict(payload)

    return _sdk_object_to_dict(payload)


def data_node_storage_description_search(
    q: str,
    *,
    q_embedding: list[float] | None = None,
    trigram_k: int = 200,
    embed_k: int = 200,
    w_trgm: float = 0.65,
    w_emb: float = 0.35,
    embedding_model: str = "default",
    filters: dict[str, Any] | None = None,
) -> dict[str, Any] | list[dict[str, Any]]:
    """
    Search data node storages by description via SDK client model.

    Single source of truth:
      - delegates search behavior and payload parsing to
        `DataNodeStorage.description_search()`
    """
    try:
        payload = _run_sdk_model_operation(
            module_name="mainsequence.client.models_tdag",
            class_name="DataNodeStorage",
            operation=lambda ClientDataNodeStorage: ClientDataNodeStorage.description_search(
                q,
                q_embedding=q_embedding,
                trigram_k=trigram_k,
                embed_k=embed_k,
                w_trgm=w_trgm,
                w_emb=w_emb,
                embedding_model=embedding_model,
                **dict(filters or {}),
            ),
        )
        return _serialize_sdk_search_response(payload)
    except Exception as e:
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Data node storage description search failed: {e}") from e


def data_node_storage_column_search(
    q: str,
    *,
    filters: dict[str, Any] | None = None,
) -> dict[str, Any] | list[dict[str, Any]]:
    """
    Search data node storages by column metadata via SDK client model.

    Single source of truth:
      - delegates search behavior and payload parsing to
        `DataNodeStorage.column_search()`
    """
    try:
        payload = _run_sdk_model_operation(
            module_name="mainsequence.client.models_tdag",
            class_name="DataNodeStorage",
            operation=lambda ClientDataNodeStorage: ClientDataNodeStorage.column_search(
                q,
                **dict(filters or {}),
            ),
        )
        return _serialize_sdk_search_response(payload)
    except Exception as e:
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Data node storage column search failed: {e}") from e


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
        raise ApiError(f"Constants fetch failed: {e}") from e


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
        raise ApiError(f"Secrets fetch failed: {e}") from e


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
            raise ApiError(f"Secret not found: {secret_id}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Secret fetch failed: {e}") from e


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
        raise ApiError(f"Secret creation failed: {e}") from e


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
            raise ApiError(f"Secret not found: {secret_id}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Secret deletion failed: {e}") from e


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


def add_secret_team_to_view(
    secret_id: int | str,
    team_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    return _mutate_shareable_object_team_access(
        module_name="mainsequence.client.models_tdag",
        class_name="Secret",
        object_id=secret_id,
        action_name="add_team_to_view",
        team_id=team_id,
        timeout=timeout,
    )


def add_secret_team_to_edit(
    secret_id: int | str,
    team_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    return _mutate_shareable_object_team_access(
        module_name="mainsequence.client.models_tdag",
        class_name="Secret",
        object_id=secret_id,
        action_name="add_team_to_edit",
        team_id=team_id,
        timeout=timeout,
    )


def remove_secret_team_from_view(
    secret_id: int | str,
    team_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    return _mutate_shareable_object_team_access(
        module_name="mainsequence.client.models_tdag",
        class_name="Secret",
        object_id=secret_id,
        action_name="remove_team_from_view",
        team_id=team_id,
        timeout=timeout,
    )


def remove_secret_team_from_edit(
    secret_id: int | str,
    team_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    return _mutate_shareable_object_team_access(
        module_name="mainsequence.client.models_tdag",
        class_name="Secret",
        object_id=secret_id,
        action_name="remove_team_from_edit",
        team_id=team_id,
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
            raise ApiError(f"Constant not found: {constant_id}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Constant fetch failed: {e}") from e


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
        raise ApiError(f"Constant creation failed: {e}") from e


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
            raise ApiError(f"Constant not found: {constant_id}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Constant deletion failed: {e}") from e


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
            raise ApiError(f"{class_name} not found: {object_id}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"{class_name} share access fetch failed: {e}") from e


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
            raise ApiError(f"{class_name} not found: {object_id}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"{class_name} share access update failed: {e}") from e


def _mutate_shareable_object_team_access(
    *,
    module_name: str,
    class_name: str,
    object_id: int | str,
    action_name: str,
    team_id: int | str,
    timeout: int | None = None,
) -> dict[str, Any]:
    try:
        payload = _run_sdk_model_operation(
            module_name=module_name,
            class_name=class_name,
            operation=lambda ClientObject: getattr(
                ClientObject.get(pk=int(object_id), timeout=timeout),
                action_name,
            )(int(team_id), timeout=timeout),
        )
        return _sdk_object_to_dict(payload)
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"{class_name} not found: {object_id}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"{class_name} team share access update failed: {e}") from e


def _mutate_labelable_object_labels(
    *,
    module_name: str,
    class_name: str,
    object_id: int | str,
    action_name: str,
    labels: list[str],
    timeout: int | None = None,
) -> dict[str, Any]:
    try:
        payload = _run_sdk_model_operation(
            module_name=module_name,
            class_name=class_name,
            operation=lambda ClientObject: getattr(
                ClientObject.get(pk=int(object_id), timeout=timeout),
                action_name,
            )(labels, timeout=timeout),
        )
        return _sdk_object_to_dict(payload)
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"{class_name} not found: {object_id}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"{class_name} label update failed: {e}") from e


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


def add_constant_team_to_view(
    constant_id: int | str,
    team_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    return _mutate_shareable_object_team_access(
        module_name="mainsequence.client.models_tdag",
        class_name="Constant",
        object_id=constant_id,
        action_name="add_team_to_view",
        team_id=team_id,
        timeout=timeout,
    )


def add_constant_team_to_edit(
    constant_id: int | str,
    team_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    return _mutate_shareable_object_team_access(
        module_name="mainsequence.client.models_tdag",
        class_name="Constant",
        object_id=constant_id,
        action_name="add_team_to_edit",
        team_id=team_id,
        timeout=timeout,
    )


def remove_constant_team_from_view(
    constant_id: int | str,
    team_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    return _mutate_shareable_object_team_access(
        module_name="mainsequence.client.models_tdag",
        class_name="Constant",
        object_id=constant_id,
        action_name="remove_team_from_view",
        team_id=team_id,
        timeout=timeout,
    )


def remove_constant_team_from_edit(
    constant_id: int | str,
    team_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    return _mutate_shareable_object_team_access(
        module_name="mainsequence.client.models_tdag",
        class_name="Constant",
        object_id=constant_id,
        action_name="remove_team_from_edit",
        team_id=team_id,
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
            raise ApiError(f"Data node storage not found: {storage_id}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Data node storage fetch failed: {e}") from e


def refresh_data_node_storage_search_index(
    storage_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Refresh one data node storage search index via SDK client model.

    Single source of truth:
      - delegates the refresh call to `DataNodeStorage.refresh_table_search_index()`
    """
    try:
        def _refresh(ClientDataNodeStorage):
            storage = ClientDataNodeStorage.get(pk=int(storage_id), timeout=timeout)
            payload = storage.refresh_table_search_index(timeout=timeout)
            if isinstance(payload, dict):
                out = dict(payload)
                out.setdefault("id", int(storage_id))
                return out
            return {"id": int(storage_id)}

        return _run_sdk_model_operation(
            module_name="mainsequence.client.models_tdag",
            class_name="DataNodeStorage",
            operation=_refresh,
        )
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"Data node storage not found: {storage_id}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Data node storage search index refresh failed: {e}") from e


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
            raise ApiError(f"Data node storage not found: {storage_id}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Data node storage deletion failed: {e}") from e


def list_data_node_storage_users_can_view(
    storage_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Fetch the view-access state for a data node storage via `ShareableObjectMixin.can_view()`.
    """
    return _get_shareable_object_access_state(
        module_name="mainsequence.client.models_tdag",
        class_name="DataNodeStorage",
        object_id=storage_id,
        accessor_name="can_view",
        timeout=timeout,
    )


def list_data_node_storage_users_can_edit(
    storage_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Fetch the edit-access state for a data node storage via `ShareableObjectMixin.can_edit()`.
    """
    return _get_shareable_object_access_state(
        module_name="mainsequence.client.models_tdag",
        class_name="DataNodeStorage",
        object_id=storage_id,
        accessor_name="can_edit",
        timeout=timeout,
    )


def add_data_node_storage_user_to_view(
    storage_id: int | str,
    user_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Grant explicit view access to one user for a data node storage.
    """
    return _mutate_shareable_object_access(
        module_name="mainsequence.client.models_tdag",
        class_name="DataNodeStorage",
        object_id=storage_id,
        action_name="add_to_view",
        user_id=user_id,
        timeout=timeout,
    )


def add_data_node_storage_user_to_edit(
    storage_id: int | str,
    user_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Grant explicit edit access to one user for a data node storage.
    """
    return _mutate_shareable_object_access(
        module_name="mainsequence.client.models_tdag",
        class_name="DataNodeStorage",
        object_id=storage_id,
        action_name="add_to_edit",
        user_id=user_id,
        timeout=timeout,
    )


def remove_data_node_storage_user_from_view(
    storage_id: int | str,
    user_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Remove explicit view access from one user for a data node storage.
    """
    return _mutate_shareable_object_access(
        module_name="mainsequence.client.models_tdag",
        class_name="DataNodeStorage",
        object_id=storage_id,
        action_name="remove_from_view",
        user_id=user_id,
        timeout=timeout,
    )


def remove_data_node_storage_user_from_edit(
    storage_id: int | str,
    user_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Remove explicit edit access from one user for a data node storage.
    """
    return _mutate_shareable_object_access(
        module_name="mainsequence.client.models_tdag",
        class_name="DataNodeStorage",
        object_id=storage_id,
        action_name="remove_from_edit",
        user_id=user_id,
        timeout=timeout,
    )


def add_data_node_storage_team_to_view(
    storage_id: int | str,
    team_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    return _mutate_shareable_object_team_access(
        module_name="mainsequence.client.models_tdag",
        class_name="DataNodeStorage",
        object_id=storage_id,
        action_name="add_team_to_view",
        team_id=team_id,
        timeout=timeout,
    )


def add_data_node_storage_team_to_edit(
    storage_id: int | str,
    team_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    return _mutate_shareable_object_team_access(
        module_name="mainsequence.client.models_tdag",
        class_name="DataNodeStorage",
        object_id=storage_id,
        action_name="add_team_to_edit",
        team_id=team_id,
        timeout=timeout,
    )


def remove_data_node_storage_team_from_view(
    storage_id: int | str,
    team_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    return _mutate_shareable_object_team_access(
        module_name="mainsequence.client.models_tdag",
        class_name="DataNodeStorage",
        object_id=storage_id,
        action_name="remove_team_from_view",
        team_id=team_id,
        timeout=timeout,
    )


def remove_data_node_storage_team_from_edit(
    storage_id: int | str,
    team_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    return _mutate_shareable_object_team_access(
        module_name="mainsequence.client.models_tdag",
        class_name="DataNodeStorage",
        object_id=storage_id,
        action_name="remove_team_from_edit",
        team_id=team_id,
        timeout=timeout,
    )


def add_data_node_storage_labels(
    storage_id: int | str,
    labels: list[str],
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """Attach one or more organizational labels to a data node storage."""

    return _mutate_labelable_object_labels(
        module_name="mainsequence.client.models_tdag",
        class_name="DataNodeStorage",
        object_id=storage_id,
        action_name="add_label",
        labels=labels,
        timeout=timeout,
    )


def remove_data_node_storage_labels(
    storage_id: int | str,
    labels: list[str],
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """Remove one or more organizational labels from a data node storage."""

    return _mutate_labelable_object_labels(
        module_name="mainsequence.client.models_tdag",
        class_name="DataNodeStorage",
        object_id=storage_id,
        action_name="remove_label",
        labels=labels,
        timeout=timeout,
    )


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

        _set_client_utils_endpoint(client_utils, endpoint)
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
            raise NotLoggedIn(str(e) or "Not logged in.") from e
        raise ApiError(f"Markets asset translation tables fetch failed: {e}") from e
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

        _set_client_utils_endpoint(client_utils, endpoint)
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
            raise NotLoggedIn(str(e) or "Not logged in.") from e
        if err_name == "NotFoundError":
            raise ApiError(f"Markets asset translation table not found: {table_id}") from e
        raise ApiError(f"Markets asset translation table fetch failed: {e}") from e
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
        os.environ["MAINSEQUENCE_ENDPOINT"] = endpoint
        os.environ["MAIN_SEQUENCE_PROJECT_ID"] = str(project_id)

        from mainsequence.client import utils as _client_utils
        from mainsequence.client.base import BaseObjectOrm
        from mainsequence.client.models_helpers import Job as ClientJob

        client_utils = _client_utils
        old_provider = getattr(client_utils.loaders, "provider", None)
        old_base_root_url = BaseObjectOrm.ROOT_URL
        old_job_root_url = getattr(ClientJob, "ROOT_URL", None)

        _set_client_utils_endpoint(client_utils, endpoint)
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
            raise NotLoggedIn(str(e) or "Not logged in.") from e
        if err_name == "NotFoundError":
            raise ApiError(f"Project not found: {project_id}") from e
        raise ApiError(f"Project job create failed: {e}") from e
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
        os.environ["MAINSEQUENCE_ENDPOINT"] = endpoint
        os.environ["MAIN_SEQUENCE_PROJECT_ID"] = str(project_id)

        from mainsequence.client import utils as _client_utils
        from mainsequence.client.base import BaseObjectOrm
        from mainsequence.client.models_helpers import Job as ClientJob

        client_utils = _client_utils
        old_provider = getattr(client_utils.loaders, "provider", None)
        old_base_root_url = BaseObjectOrm.ROOT_URL
        old_job_root_url = getattr(ClientJob, "ROOT_URL", None)

        _set_client_utils_endpoint(client_utils, endpoint)
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
            raise NotLoggedIn(str(e) or "Not logged in.") from e
        if err_name == "NotFoundError":
            raise ApiError(f"Project not found: {project_id}") from e
        raise ApiError(f"Project batch job scheduling failed: {e}") from e
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
    command_args: list[str] | None = None,
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
        os.environ["MAINSEQUENCE_ENDPOINT"] = endpoint

        from mainsequence.client import utils as _client_utils
        from mainsequence.client.base import BaseObjectOrm
        from mainsequence.client.models_helpers import Job as ClientJob

        client_utils = _client_utils
        old_provider = getattr(client_utils.loaders, "provider", None)
        old_base_root_url = BaseObjectOrm.ROOT_URL
        old_job_root_url = getattr(ClientJob, "ROOT_URL", None)

        _set_client_utils_endpoint(client_utils, endpoint)
        client_utils.loaders.use_jwt(access=access, refresh=refresh or None)

        BaseObjectOrm.ROOT_URL = root_url
        ClientJob.ROOT_URL = root_url

        job = ClientJob.get(pk=int(job_id), timeout=timeout)
        payload = job.run_job(timeout=timeout, command_args=command_args)
        if isinstance(payload, dict):
            return payload
        if hasattr(payload, "model_dump"):
            return payload.model_dump()
        return {"job_id": int(job_id)}

    except Exception as e:
        err_name = type(e).__name__
        if err_name in {"AuthenticationError", "PermissionDeniedError"}:
            raise NotLoggedIn(str(e) or "Not logged in.") from e
        if err_name == "NotFoundError":
            raise ApiError(f"Job not found: {job_id}") from e
        raise ApiError(f"Project job run failed: {e}") from e
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
        os.environ["MAINSEQUENCE_ENDPOINT"] = endpoint

        from mainsequence.client import utils as _client_utils
        from mainsequence.client.base import BaseObjectOrm
        from mainsequence.client.models_helpers import JobRun as ClientJobRun

        client_utils = _client_utils
        old_provider = getattr(client_utils.loaders, "provider", None)
        old_base_root_url = BaseObjectOrm.ROOT_URL
        old_job_run_root_url = getattr(ClientJobRun, "ROOT_URL", None)

        _set_client_utils_endpoint(client_utils, endpoint)
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
            raise NotLoggedIn(str(e) or "Not logged in.") from e
        if err_name == "NotFoundError":
            raise ApiError(f"Job not found: {job_id}") from e
        raise ApiError(f"Project job runs fetch failed: {e}") from e
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
        os.environ["MAINSEQUENCE_ENDPOINT"] = endpoint

        from mainsequence.client import utils as _client_utils
        from mainsequence.client.base import BaseObjectOrm
        from mainsequence.client.models_helpers import JobRun as ClientJobRun

        client_utils = _client_utils
        old_provider = getattr(client_utils.loaders, "provider", None)
        old_base_root_url = BaseObjectOrm.ROOT_URL
        old_job_run_root_url = getattr(ClientJobRun, "ROOT_URL", None)

        _set_client_utils_endpoint(client_utils, endpoint)
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
            raise NotLoggedIn(str(e) or "Not logged in.") from e
        if err_name == "NotFoundError":
            raise ApiError(f"Job run not found: {job_run_id}") from e
        raise ApiError(f"Project job run logs fetch failed: {e}") from e
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
