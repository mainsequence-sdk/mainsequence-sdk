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

from __future__ import annotations

import importlib
import json
import os
import re
import shlex
from typing import Any
from urllib.parse import urlencode

import requests

from .config import backend_url, get_tokens, save_tokens

AUTH_PATHS = {
    "authorize": "/auth/cli/authorize/",
    "cli_token": "/auth/cli/token/",
    "cli_revoke": "/auth/cli/revoke/",
    "refresh": "/auth/jwt-token/token/refresh/",
    "logout": "/auth/jwt-token/logout/",
    "ping": "/auth/rest-auth/user/",
}
CLI_BROWSER_CLIENT_ID = "mainsequence-cli"

S = requests.Session()
S.headers.update({"Content-Type": "application/json"})

DEFAULT_AGENT_RUNTIME_READY_TIMEOUT_SECONDS = 900.0
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


def logout_cli_session() -> dict[str, Any]:
    """
    Revoke the current tracked CLI login session when possible.

    Behavior:
    - If a CLI browser-login refresh token exists, call `/auth/cli/revoke/`.
    - If the backend does not support that endpoint (`404`), fall back to
      `/auth/jwt-token/logout/` when an access token is still available.
    - If no refresh token exists, do not attempt backend revoke. This covers
      runtime credential mode and any local-only access-token state.

    Returns a status dict with:
    - `attempted`: whether backend revoke/logout was attempted
    - `revoked`: whether backend-side logout completed
    - `method`: `cli_revoke`, `jwt_logout_fallback`, `local_only`, or `error`
    - `detail`: best-effort human-readable detail
    """
    access = (_access_token() or "").strip()
    refresh = (_refresh_token() or "").strip()

    if not refresh:
        return {
            "attempted": False,
            "revoked": False,
            "method": "local_only",
            "detail": "No CLI browser-login refresh token available.",
        }

    payload = {"refresh": refresh}
    try:
        response = S.post(_full(AUTH_PATHS["cli_revoke"]), data=json.dumps(payload))
    except Exception as exc:
        return {
            "attempted": True,
            "revoked": False,
            "method": "error",
            "detail": str(exc),
        }

    try:
        data = response.json()
    except Exception:
        data = {}

    detail = ""
    if isinstance(data, dict):
        detail = str(data.get("detail") or data.get("message") or "").strip()
    if not detail:
        detail = response.text.strip()

    if response.status_code == 200:
        return {
            "attempted": True,
            "revoked": True,
            "method": "cli_revoke",
            "detail": detail or "CLI refresh token revoked.",
        }

    if response.status_code == 404:
        if access and logout_jwt_session():
            return {
                "attempted": True,
                "revoked": True,
                "method": "jwt_logout_fallback",
                "detail": "CLI revoke endpoint unavailable; used JWT logout fallback.",
            }
        return {
            "attempted": True,
            "revoked": False,
            "method": "error",
            "detail": detail or "CLI revoke endpoint unavailable.",
        }

    return {
        "attempted": True,
        "revoked": False,
        "method": "error",
        "detail": detail or f"CLI revoke failed with status {response.status_code}.",
    }


def refresh_access() -> str:
    """
    Use refresh token to obtain a new access token and update session env.

    Raises:
        NotLoggedIn: if refresh is missing or refresh fails
    """
    refresh = _refresh_token()
    runtime_mode = (
        os.environ.get("MAINSEQUENCE_AUTH_MODE") or ""
    ).strip().lower() == "runtime_credential"

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
            raise NotLoggedIn(
                "Runtime credential exchange did not produce MAINSEQUENCE_ACCESS_TOKEN."
            )

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
    def _strip_client_metadata(value: Any) -> Any:
        if isinstance(value, dict):
            return {
                key: _strip_client_metadata(item)
                for key, item in value.items()
                if key != "orm_class"
            }
        if isinstance(value, list):
            return [_strip_client_metadata(item) for item in value]
        if isinstance(value, tuple):
            return [_strip_client_metadata(item) for item in value]
        return value

    if isinstance(obj, dict):
        return _strip_client_metadata(dict(obj))
    if hasattr(obj, "model_dump"):
        return _strip_client_metadata(obj.model_dump(mode="json"))
    if hasattr(obj, "uid"):
        return {"uid": getattr(obj, "uid", None)}
    return {"id": getattr(obj, "id", None)}


def _run_sdk_model_operation(
    *,
    module_name: str,
    class_name: str,
    operation,
    project_id_env: int | str | None = None,
    project_uid_env: str | None = None,
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
        "MAIN_SEQUENCE_PROJECT_UID": os.environ.get("MAIN_SEQUENCE_PROJECT_UID"),
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
        if project_uid_env is not None:
            os.environ["MAIN_SEQUENCE_PROJECT_UID"] = str(project_uid_env)
        elif project_id_env is not None and not str(project_id_env).strip().isdigit():
            os.environ["MAIN_SEQUENCE_PROJECT_UID"] = str(project_id_env)
        else:
            os.environ.pop("MAIN_SEQUENCE_PROJECT_UID", None)
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
    Return current user profile (username + organization name) via the canonical user-details endpoint.

    Returns:
        dict: {"username": "...", "organization": "..."} or {}
    """
    details = authed("GET", "/user/api/user/get_user_details/")
    payload = details.json() if details.ok else {}
    user = payload.get("user") if isinstance(payload, dict) else {}
    if not isinstance(user, dict):
        user = {}
    organization = user.get("organization") if isinstance(user, dict) else {}
    if not isinstance(organization, dict):
        organization = {}
    payload_organization = payload.get("organization") if isinstance(payload, dict) else {}
    if not isinstance(payload_organization, dict):
        payload_organization = {}
    org_name = (
        organization.get("name")
        or payload_organization.get("name")
        or payload.get("organization_name")
        or payload.get("organization")
        or ""
    )
    return {
        "username": user.get("username") or payload.get("username") or "",
        "organization": org_name,
    }


def get_logged_user_details() -> dict[str, Any]:
    """
    Return the authenticated user via SDK client `User.get_logged_user()`.

    The CLI does not naturally run inside a request context, so this bridge resolves
    the current user UID from the authenticated API session and temporarily binds
    `X-User-UID` plus `Authorization` into
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
        user_uid = data.get("uid") or (data.get("user") or {}).get("uid") or data.get("user_uid")
        if user_uid in (None, ""):
            raise ApiError("Could not determine the authenticated user uid.")

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
                "X-User-UID": str(user_uid),
                "Authorization": f"Bearer {access}",
            }
        )

        user = ClientUser.get_logged_user()
        if isinstance(user, dict):
            user.pop("id", None)
            organization = user.get("organization")
            if isinstance(organization, dict):
                organization.pop("id", None)
            return user
        if hasattr(user, "model_dump"):
            payload = user.model_dump()
            payload.pop("id", None)
            organization = payload.get("organization")
            if isinstance(organization, dict):
                organization.pop("id", None)
            return payload
        return {"uid": getattr(user, "uid", None)}

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


def _normalize_project_reference(project_ref: int | str) -> str:
    normalized = str(project_ref).strip()
    if not normalized:
        raise ApiError("Project UID is required.")
    return normalized


def _project_matches_reference(project_payload: dict[str, Any], project_ref: str) -> bool:
    return (
        str(project_payload.get("uid") or "").strip() == project_ref
        or str(project_payload.get("id") or "").strip() == project_ref
    )


def resolve_project(project_ref: int | str) -> dict[str, Any]:
    normalized_ref = _normalize_project_reference(project_ref)

    try:
        payload = get_project(normalized_ref)
        if isinstance(payload, dict) and payload:
            return payload
    except ApiError:
        pass

    for project_payload in get_projects():
        if isinstance(project_payload, dict) and _project_matches_reference(
            project_payload, normalized_ref
        ):
            return project_payload

    raise ApiError(f"Project not found: {normalized_ref}")


def resolve_project_uid(project_ref: int | str) -> str:
    normalized_ref = _normalize_project_reference(project_ref)
    if normalized_ref.isdigit():
        return normalized_ref

    payload = resolve_project(project_ref)
    normalized_uid = str(payload.get("uid") or "").strip()
    if normalized_uid:
        return normalized_uid
    if not normalized_ref.isdigit():
        return normalized_ref
    raise ApiError(f"Project UID is not available for project reference: {normalized_ref}")


def resolve_project_row_id(project_ref: int | str) -> int:
    normalized_ref = _normalize_project_reference(project_ref)
    if normalized_ref.isdigit():
        return int(normalized_ref)

    payload = resolve_project(project_ref)
    row_id = payload.get("id")
    if row_id is None:
        if normalized_ref.isdigit():
            return int(normalized_ref)
        raise ApiError(f"Backend row id is not available for project reference: {normalized_ref}")
    return int(row_id)


def search_projects(
    q: str,
    *,
    limit: int = 20,
    timeout: int | None = None,
) -> list[dict[str, Any]]:
    """
    Search projects visible to the authenticated user via SDK client model.

    Single source of truth:
      - delegates payload parsing to `Project.quick_search()`
    """
    try:
        payload = _run_sdk_model_operation(
            module_name="mainsequence.client.models_foundry",
            class_name="Project",
            operation=lambda ClientProject: ClientProject.quick_search(
                q=q,
                limit=limit,
                timeout=timeout,
            ),
        )
        return [_sdk_object_to_dict(item) for item in list(payload or [])]
    except Exception as e:
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Project search failed: {e}") from e


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
            module_name="mainsequence.client.models_foundry",
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
            operation=lambda ClientTeam: ClientTeam.get(pk=int(team_id), timeout=timeout).patch(
                **updates
            ),
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
    agent_uid: str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Retrieve one agent via SDK client model using its public UID.
    """
    try:
        agent = _run_sdk_model_operation(
            module_name="mainsequence.client.agent_runtime_models",
            class_name="Agent",
            operation=lambda ClientAgent: ClientAgent.get_by_uid(str(agent_uid), timeout=timeout),
        )
        return _sdk_object_to_dict(agent)
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"Agent not found: {agent_uid}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Agent fetch failed: {e}") from e


def semantic_search_agents(
    q: str,
    *,
    limit: int = 20,
    timeout: int | None = None,
) -> list[dict[str, Any]]:
    """
    Search agents via SDK client `Agent.semantic_search()`.
    """
    try:
        payload = _run_sdk_model_operation(
            module_name="mainsequence.client.agent_runtime_models",
            class_name="Agent",
            operation=lambda ClientAgent: ClientAgent.semantic_search(
                q,
                limit=limit,
                timeout=timeout,
            ),
        )
        return [_sdk_object_to_dict(item) for item in list(payload or [])]
    except Exception as e:
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Agent search failed: {e}") from e


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
    agent_uid: str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Delete one agent via SDK client model using its public UID.
    """
    try:

        def _delete(ClientAgent):
            agent = ClientAgent.get_by_uid(str(agent_uid), timeout=timeout)
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
            raise ApiError(f"Agent not found: {agent_uid}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Agent deletion failed: {e}") from e


def allocate_agent_a2a_target_session(
    agent_uid: str,
    *,
    caller_agent_session_uid: str,
    handle_unique_id: str | None = None,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Allocate or reuse the delegated A2A target session for one agent via SDK client model.
    """
    try:

        def _allocate(ClientAgent):
            agent = ClientAgent.get_by_uid(str(agent_uid), timeout=timeout)
            return agent.allocate_a2a_target_session(
                caller_agent_session_uid=str(caller_agent_session_uid),
                handle_unique_id=handle_unique_id,
                timeout=timeout,
            )

        payload = _run_sdk_model_operation(
            module_name="mainsequence.client.agent_runtime_models",
            class_name="Agent",
            operation=_allocate,
        )
        return _sdk_object_to_dict(payload)
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"Agent not found: {agent_uid}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Agent A2A target session allocation failed: {e}") from e


def get_agent_latest_session(
    agent_uid: str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Retrieve the latest session for one agent via SDK client model.
    """
    try:

        def _get_latest(ClientAgent):
            agent = ClientAgent.get_by_uid(str(agent_uid), timeout=timeout)
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
            raise ApiError(f"Agent not found: {agent_uid}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Agent latest session fetch failed: {e}") from e


def list_agent_sessions(
    *,
    timeout: int | None = None,
    filters: dict[str, Any] | None = None,
    agent_uid: str | None = None,
    agent_unique_id: str | None = None,
) -> list[dict[str, Any]]:
    """
    List agent sessions via SDK client model, optionally scoped to one agent.
    """
    if agent_uid and agent_unique_id:
        raise ApiError("Pass either agent_uid or agent_unique_id, not both.")

    session_filters = dict(filters or {})
    if agent_uid:
        session_filters["agent_uid"] = str(agent_uid)
    elif agent_unique_id:
        try:
            agent = _run_sdk_model_operation(
                module_name="mainsequence.client.agent_runtime_models",
                class_name="Agent",
                operation=lambda ClientAgent: ClientAgent.get_by_agent_unique_id(
                    str(agent_unique_id), timeout=timeout
                ),
            )
        except Exception as e:
            err_name = type(e).__name__
            if err_name in {"DoesNotExist", "NotFoundError"}:
                raise ApiError(f"Agent not found for agent_unique_id={agent_unique_id!r}") from e
            if isinstance(e, (ApiError, NotLoggedIn)):
                raise
            raise ApiError(
                f"Agent lookup failed for agent_unique_id={agent_unique_id!r}: {e}"
            ) from e

        resolved_agent = _sdk_object_to_dict(agent)
        resolved_agent_uid = str(resolved_agent.get("uid") or "").strip()
        if not resolved_agent_uid:
            raise ApiError(f"Agent lookup returned no uid for agent_unique_id={agent_unique_id!r}")
        session_filters["agent_uid"] = resolved_agent_uid

    try:
        payload = _run_sdk_model_operation(
            module_name="mainsequence.client.agent_runtime_models",
            class_name="AgentSession",
            operation=lambda ClientAgentSession: ClientAgentSession.filter(
                timeout=timeout, **session_filters
            ),
        )
        return [_sdk_object_to_dict(item) for item in list(payload or [])]
    except Exception as e:
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Agent sessions fetch failed: {e}") from e


def get_agent_session(
    agent_session_uid: str,
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
                pk=str(agent_session_uid), timeout=timeout
            ),
        )
        return _sdk_object_to_dict(agent_session)
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"Agent session not found: {agent_session_uid}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Agent session fetch failed: {e}") from e


def resolve_agent_session_runtime_access(
    agent_session_uid: str,
    *,
    wait_for_runtime: bool = False,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Resolve runtime access for one agent session via SDK client model.
    """
    try:
        runtime_access = _run_sdk_model_operation(
            module_name="mainsequence.client.agent_runtime_models",
            class_name="AgentSession",
            operation=lambda ClientAgentSession: ClientAgentSession.resolve_runtime_access(
                str(agent_session_uid),
                wait_for_runtime=wait_for_runtime,
                timeout=timeout,
            ),
        )
        return _sdk_object_to_dict(runtime_access)
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"Agent session not found: {agent_session_uid}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Agent session runtime access resolve failed: {e}") from e


def wait_agent_session_runtime_ready(
    agent_session_uid: str,
    *,
    timeout_seconds: float = DEFAULT_AGENT_RUNTIME_READY_TIMEOUT_SECONDS,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Wait for one agent session runtime through SDK client model.
    """
    try:
        ready = _run_sdk_model_operation(
            module_name="mainsequence.client.agent_runtime_models",
            class_name="AgentSession",
            operation=lambda ClientAgentSession: ClientAgentSession.wait_until_runtime_ready(
                str(agent_session_uid),
                timeout_seconds=timeout_seconds,
                timeout=timeout,
            ),
        )
        return _sdk_object_to_dict(ready)
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"Agent session not found: {agent_session_uid}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Agent session runtime readiness failed: {e}") from e


def send_agent_session_a2a_chat(
    agent_session_uid: str,
    *,
    message: str | None = None,
    a2a_payload: dict[str, Any] | None = None,
    wait_for_runtime: bool = True,
    runtime_ready_timeout_seconds: float = DEFAULT_AGENT_RUNTIME_READY_TIMEOUT_SECONDS,
    poll_task_until_stable: bool | None = None,
    runtime_turn_timeout_seconds: float | None = None,
    omit_reasoning: bool | None = None,
    response_format: dict[str, Any] | None = None,
    json_repair: dict[str, Any] | None = None,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Send an A2A request through the legacy backend compatibility endpoint.
    """
    try:
        chat = _run_sdk_model_operation(
            module_name="mainsequence.client.agent_runtime_models",
            class_name="AgentSession",
            operation=lambda ClientAgentSession: ClientAgentSession.send_a2a_chat(
                str(agent_session_uid),
                message=message,
                a2a_payload=a2a_payload,
                wait_for_runtime=wait_for_runtime,
                runtime_ready_timeout_seconds=runtime_ready_timeout_seconds,
                poll_task_until_stable=poll_task_until_stable,
                runtime_turn_timeout_seconds=runtime_turn_timeout_seconds,
                omit_reasoning=omit_reasoning,
                response_format=response_format,
                json_repair=json_repair,
                timeout=timeout,
            ),
        )
        return _sdk_object_to_dict(chat)
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"Agent session not found: {agent_session_uid}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Agent session A2A chat failed: {e}") from e


def send_agent_session_a2a_message(
    agent_session_uid: str,
    *,
    message: str,
    strict_dictionary: bool = False,
    json_repair_attempts: int = 3,
    history_length: int = 0,
    return_immediately: bool = False,
    wait_for_runtime: bool = True,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Send one standard A2A message through the target session runtime.
    """
    try:
        payload = _run_sdk_model_operation(
            module_name="mainsequence.client.agent_runtime_models",
            class_name="AgentSession",
            operation=lambda ClientAgentSession: ClientAgentSession.send_a2a_message(
                str(agent_session_uid),
                message=message,
                strict_dictionary=strict_dictionary,
                json_repair_attempts=json_repair_attempts,
                history_length=history_length,
                return_immediately=return_immediately,
                wait_for_runtime=wait_for_runtime,
                timeout=timeout,
            ),
        )
        return _sdk_object_to_dict(payload)
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"Agent session not found: {agent_session_uid}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Agent session A2A message send failed: {e}") from e


def chat_agent_session_runtime(
    agent_session_uid: str,
    *,
    message: str,
    runtime_access: dict[str, Any] | None = None,
    wait_for_runtime: bool = True,
    runtime_turn_timeout_seconds: float | None = None,
    omit_reasoning: bool | None = True,
    response_format: dict[str, Any] | None = None,
    json_repair: dict[str, Any] | None = None,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Send one message through the resolved A2A runtime chat stream.
    """
    operation_kwargs: dict[str, Any] = {
        "message": message,
        "wait_for_runtime": wait_for_runtime,
        "runtime_turn_timeout_seconds": runtime_turn_timeout_seconds,
        "omit_reasoning": omit_reasoning,
        "response_format": response_format,
        "json_repair": json_repair,
        "timeout": timeout,
    }
    if runtime_access is not None:
        operation_kwargs["runtime_access"] = runtime_access
    try:
        payload = _run_sdk_model_operation(
            module_name="mainsequence.client.agent_runtime_models",
            class_name="AgentSession",
            operation=lambda ClientAgentSession: ClientAgentSession.a2a_chat(
                str(agent_session_uid),
                **operation_kwargs,
            ),
        )
        return _sdk_object_to_dict(payload)
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"Agent session not found: {agent_session_uid}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Agent session runtime chat failed: {e}") from e


def cancel_agent_session_runtime(
    agent_session_uid: str,
    *,
    reason: str = "client_requested",
    message: str | None = None,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Cancel the active turn on the resolved A2A runtime.
    """
    try:
        payload = _run_sdk_model_operation(
            module_name="mainsequence.client.agent_runtime_models",
            class_name="AgentSession",
            operation=lambda ClientAgentSession: ClientAgentSession.cancel_runtime(
                str(agent_session_uid),
                reason=reason,
                message=message,
                timeout=timeout,
            ),
        )
        return _sdk_object_to_dict(payload)
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"Agent session not found: {agent_session_uid}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Agent session runtime cancel failed: {e}") from e


def detach_agent_session_runtime(
    agent_session_uid: str,
    *,
    reason: str = "client_done",
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Detach the resolved A2A runtime for one agent session.
    """
    try:
        payload = _run_sdk_model_operation(
            module_name="mainsequence.client.agent_runtime_models",
            class_name="AgentSession",
            operation=lambda ClientAgentSession: ClientAgentSession.detach_runtime(
                str(agent_session_uid),
                reason=reason,
                timeout=timeout,
            ),
        )
        return _sdk_object_to_dict(payload)
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"Agent session not found: {agent_session_uid}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Agent session runtime detach failed: {e}") from e


def list_agent_users_can_view(
    agent_uid: str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Fetch the view-access state for an agent via `ShareableObjectMixin.can_view()`.
    """
    return _get_shareable_object_access_state(
        module_name="mainsequence.client.agent_runtime_models",
        class_name="Agent",
        object_id=agent_uid,
        accessor_name="can_view",
        object_lookup_field="uid",
        timeout=timeout,
    )


def list_agent_users_can_edit(
    agent_uid: str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Fetch the edit-access state for an agent via `ShareableObjectMixin.can_edit()`.
    """
    return _get_shareable_object_access_state(
        module_name="mainsequence.client.agent_runtime_models",
        class_name="Agent",
        object_id=agent_uid,
        accessor_name="can_edit",
        object_lookup_field="uid",
        timeout=timeout,
    )


def add_agent_user_to_view(
    agent_uid: str,
    user_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    return _mutate_shareable_object_access(
        module_name="mainsequence.client.agent_runtime_models",
        class_name="Agent",
        object_id=agent_uid,
        action_name="add_to_view",
        object_lookup_field="uid",
        user_id=user_id,
        timeout=timeout,
    )


def add_agent_user_to_edit(
    agent_uid: str,
    user_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    return _mutate_shareable_object_access(
        module_name="mainsequence.client.agent_runtime_models",
        class_name="Agent",
        object_id=agent_uid,
        action_name="add_to_edit",
        object_lookup_field="uid",
        user_id=user_id,
        timeout=timeout,
    )


def remove_agent_user_from_view(
    agent_uid: str,
    user_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    return _mutate_shareable_object_access(
        module_name="mainsequence.client.agent_runtime_models",
        class_name="Agent",
        object_id=agent_uid,
        action_name="remove_from_view",
        object_lookup_field="uid",
        user_id=user_id,
        timeout=timeout,
    )


def remove_agent_user_from_edit(
    agent_uid: str,
    user_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    return _mutate_shareable_object_access(
        module_name="mainsequence.client.agent_runtime_models",
        class_name="Agent",
        object_id=agent_uid,
        action_name="remove_from_edit",
        object_lookup_field="uid",
        user_id=user_id,
        timeout=timeout,
    )


def add_agent_team_to_view(
    agent_uid: str,
    team_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    return _mutate_shareable_object_team_access(
        module_name="mainsequence.client.agent_runtime_models",
        class_name="Agent",
        object_id=agent_uid,
        action_name="add_team_to_view",
        object_lookup_field="uid",
        team_id=team_id,
        timeout=timeout,
    )


def add_agent_team_to_edit(
    agent_uid: str,
    team_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    return _mutate_shareable_object_team_access(
        module_name="mainsequence.client.agent_runtime_models",
        class_name="Agent",
        object_id=agent_uid,
        action_name="add_team_to_edit",
        object_lookup_field="uid",
        team_id=team_id,
        timeout=timeout,
    )


def remove_agent_team_from_view(
    agent_uid: str,
    team_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    return _mutate_shareable_object_team_access(
        module_name="mainsequence.client.agent_runtime_models",
        class_name="Agent",
        object_id=agent_uid,
        action_name="remove_team_from_view",
        object_lookup_field="uid",
        team_id=team_id,
        timeout=timeout,
    )


def remove_agent_team_from_edit(
    agent_uid: str,
    team_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    return _mutate_shareable_object_team_access(
        module_name="mainsequence.client.agent_runtime_models",
        class_name="Agent",
        object_id=agent_uid,
        action_name="remove_team_from_edit",
        object_lookup_field="uid",
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
            operation=lambda ClientAgentRun: ClientAgentRun.filter(
                timeout=timeout, **(filters or {})
            ),
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
            operation=lambda ClientAgentRun: ClientAgentRun.get(
                pk=int(agent_run_id), timeout=timeout
            ),
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
    Fetch a single project by public reference.
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
        module_name="mainsequence.client.models_foundry",
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
        module_name="mainsequence.client.models_foundry",
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
        module_name="mainsequence.client.models_foundry",
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
        module_name="mainsequence.client.models_foundry",
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
        module_name="mainsequence.client.models_foundry",
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
        module_name="mainsequence.client.models_foundry",
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
        module_name="mainsequence.client.models_foundry",
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
        module_name="mainsequence.client.models_foundry",
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
        module_name="mainsequence.client.models_foundry",
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
        module_name="mainsequence.client.models_foundry",
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
        module_name="mainsequence.client.models_foundry",
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
        module_name="mainsequence.client.models_foundry",
        class_name="Project",
        object_id=project_id,
        action_name="remove_label",
        labels=labels,
        timeout=timeout,
    )


def get_project_data_node_updates(
    project_id: int | str, *, timeout: int | None = None
) -> list[dict[str, Any]]:
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
        "MAIN_SEQUENCE_PROJECT_UID": os.environ.get("MAIN_SEQUENCE_PROJECT_UID"),
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
        project_uid = resolve_project_uid(project_id)
        project_row_id = resolve_project_row_id(project_id)
        os.environ["MAIN_SEQUENCE_PROJECT_UID"] = project_uid
        os.environ["MAIN_SEQUENCE_PROJECT_ID"] = str(project_row_id)

        from mainsequence.client import utils as _client_utils
        from mainsequence.client.base import BaseObjectOrm
        from mainsequence.client.models_foundry import Project as ClientProject

        client_utils = _client_utils
        old_provider = getattr(client_utils.loaders, "provider", None)
        old_base_root_url = BaseObjectOrm.ROOT_URL
        old_project_root_url = getattr(ClientProject, "ROOT_URL", None)

        _set_client_utils_endpoint(client_utils, endpoint)
        client_utils.loaders.use_jwt(access=access, refresh=refresh or None)

        BaseObjectOrm.ROOT_URL = root_url
        ClientProject.ROOT_URL = root_url

        project = ClientProject.get(pk=project_uid, timeout=timeout)
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
                from mainsequence.client.models_foundry import Project as ClientProject

                ClientProject.ROOT_URL = old_project_root_url
            except Exception:
                pass

        for k, v in old_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def sync_project_after_commit(
    project_id: int | str, *, timeout: int | None = None
) -> dict[str, Any] | None:
    """
    Notify the backend that a project commit has been pushed.

    Single source of truth:
      - delegates request behavior and payload parsing to `Project.sync_project_after_commit()`
    """
    try:
        project_uid = resolve_project_uid(project_id)
        payload = _run_sdk_model_operation(
            module_name="mainsequence.client.models_foundry",
            class_name="Project",
            operation=lambda ClientProject: ClientProject.sync_project_after_commit(
                project_uid,
                timeout=timeout,
            ),
            project_id_env=resolve_project_row_id(project_id),
            project_uid_env=project_uid,
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
        importlib.import_module("mainsequence.client.models_foundry")
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
        "MAIN_SEQUENCE_PROJECT_UID": os.environ.get("MAIN_SEQUENCE_PROJECT_UID"),
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
        project_uid = resolve_project_uid(related_project_id)
        project_row_id = resolve_project_row_id(related_project_id)
        os.environ["MAIN_SEQUENCE_PROJECT_UID"] = project_uid
        os.environ["MAIN_SEQUENCE_PROJECT_ID"] = str(project_row_id)

        from mainsequence.client import utils as _client_utils
        from mainsequence.client.base import BaseObjectOrm
        from mainsequence.client.models_foundry import ProjectImage as ClientProjectImage

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
            related_project_id=project_row_id,
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
                from mainsequence.client.models_foundry import ProjectImage as ClientProjectImage

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
        "MAIN_SEQUENCE_PROJECT_UID": os.environ.get("MAIN_SEQUENCE_PROJECT_UID"),
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
        project_uid = resolve_project_uid(related_project_id)
        project_row_id = resolve_project_row_id(related_project_id)
        os.environ["MAIN_SEQUENCE_PROJECT_UID"] = project_uid
        os.environ["MAIN_SEQUENCE_PROJECT_ID"] = str(project_row_id)

        from mainsequence.client import utils as _client_utils
        from mainsequence.client.base import BaseObjectOrm
        from mainsequence.client.models_foundry import ProjectImage as ClientProjectImage

        client_utils = _client_utils
        old_provider = getattr(client_utils.loaders, "provider", None)
        old_base_root_url = BaseObjectOrm.ROOT_URL
        old_image_root_url = getattr(ClientProjectImage, "ROOT_URL", None)

        _set_client_utils_endpoint(client_utils, endpoint)
        client_utils.loaders.use_jwt(access=access, refresh=refresh or None)

        BaseObjectOrm.ROOT_URL = root_url
        ClientProjectImage.ROOT_URL = root_url

        merged_filters = dict(filters or {})
        merged_filters["related_project__id__in"] = [project_row_id]
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
                from mainsequence.client.models_foundry import ProjectImage as ClientProjectImage

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
            module_name="mainsequence.client.models_foundry",
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
            module_name="mainsequence.client.models_foundry",
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
        "MAIN_SEQUENCE_PROJECT_UID": os.environ.get("MAIN_SEQUENCE_PROJECT_UID"),
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
        project_uid = resolve_project_uid(project_id)
        project_row_id = resolve_project_row_id(project_id)
        os.environ["MAIN_SEQUENCE_PROJECT_UID"] = project_uid
        os.environ["MAIN_SEQUENCE_PROJECT_ID"] = str(project_row_id)

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
            **{**extra_filters, "project__id": project_row_id},
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
        "MAIN_SEQUENCE_PROJECT_UID": os.environ.get("MAIN_SEQUENCE_PROJECT_UID"),
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
        project_uid = resolve_project_uid(project_id)
        project_row_id = resolve_project_row_id(project_id)
        os.environ["MAIN_SEQUENCE_PROJECT_UID"] = project_uid
        os.environ["MAIN_SEQUENCE_PROJECT_ID"] = str(project_row_id)

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
                "project__id": project_row_id,
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
      - delegates to `ProjectResource.create_dashboard()` or
        `ProjectResource.create_fastapi()`
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
            "fastapi": "create_fastapi",
        }.get(str(release_kind).strip())
        if not create_method_name:
            raise ApiError("release_kind must be one of: 'streamlit_dashboard', 'fastapi'.")

        create_method = getattr(resource, create_method_name, None)
        if not callable(create_method):
            raise ApiError(f"ProjectResource does not implement {create_method_name}().")
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


def list_data_node_storages(
    *,
    filters: dict[str, Any] | None = None,
    timeout: int | None = None,
) -> list[dict[str, Any]]:
    """
    List data node storages via SDK client model.

    Single source of truth:
      - delegates filtering and payload parsing to `TimeIndexMetaTable.filter()`
    """
    try:
        storages = _run_sdk_model_operation(
            module_name="mainsequence.client.metatables",
            class_name="TimeIndexMetaTable",
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


def list_meta_tables(
    *,
    filters: dict[str, Any] | None = None,
    timeout: int | None = None,
) -> list[dict[str, Any]]:
    """
    List MetaTables via SDK client model.

    Single source of truth:
      - delegates filtering and payload parsing to `MetaTable.filter()`
    """
    try:
        meta_tables = _run_sdk_model_operation(
            module_name="mainsequence.client.metatables",
            class_name="MetaTable",
            operation=lambda ClientMetaTable: ClientMetaTable.filter(
                timeout=timeout,
                **dict(filters or {}),
            ),
        )
        return [_sdk_object_to_dict(meta_table) for meta_table in meta_tables]
    except Exception as e:
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"MetaTables fetch failed: {e}") from e


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
    workspace_uid: str,
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
            operation=lambda ClientWorkspace: ClientWorkspace.get_by_uid(
                str(workspace_uid),
                timeout=timeout,
            ),
        )
        return _sdk_object_to_dict(workspace)
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"Workspace not found: {workspace_uid}") from e
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
    workspace_uid: str,
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
            operation=lambda ClientWorkspace: ClientWorkspace.get_by_uid(
                str(workspace_uid),
                timeout=timeout,
            ).patch(**patch_kwargs),
        )
        return _sdk_object_to_dict(workspace)
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"Workspace not found: {workspace_uid}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Workspace update failed: {e}") from e


def delete_workspace(
    workspace_uid: str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Delete one workspace via SDK client model.
    """
    try:

        def _delete(ClientWorkspace):
            workspace = ClientWorkspace.get_by_uid(str(workspace_uid), timeout=timeout)
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
            raise ApiError(f"Workspace not found: {workspace_uid}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Workspace deletion failed: {e}") from e


def add_workspace_labels(
    workspace_uid: str,
    labels: list[str],
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """Attach one or more organizational labels to a workspace."""

    return _mutate_labelable_object_labels(
        module_name="mainsequence.client.command_center",
        class_name="Workspace",
        object_id=workspace_uid,
        action_name="add_label",
        object_lookup_field="uid",
        labels=labels,
        timeout=timeout,
    )


def remove_workspace_labels(
    workspace_uid: str,
    labels: list[str],
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """Remove one or more organizational labels from a workspace."""

    return _mutate_labelable_object_labels(
        module_name="mainsequence.client.command_center",
        class_name="Workspace",
        object_id=workspace_uid,
        action_name="remove_label",
        object_lookup_field="uid",
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
            operation=lambda ClientConnectionInstance: ClientConnectionInstance.get_by_uid(
                str(connection_uid),
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


def _adapter_from_api_public_config_model():
    module = importlib.import_module("mainsequence.client.command_center.connections")
    return module.AdapterFromApiConnectionPublicConfig


def _adapter_from_api_public_config_requested(
    *,
    public_config: dict[str, Any] | None,
    api_base_url: str | None,
    debug_api_base_url: str | None,
    config_values: dict[str, Any] | None,
    compiled_contract: dict[str, Any] | None,
    contract_version: str | None,
    request_timeout_ms: int | None,
    query_cache_policy: str | None,
    query_cache_ttl_ms: int | None,
    dedupe_in_flight: bool | None,
) -> bool:
    return any(
        value is not None
        for value in (
            public_config,
            api_base_url,
            debug_api_base_url,
            config_values,
            compiled_contract,
            contract_version,
            request_timeout_ms,
            query_cache_policy,
            query_cache_ttl_ms,
            dedupe_in_flight,
        )
    )


def _adapter_from_api_public_config_payload(
    *,
    public_config: dict[str, Any] | None = None,
    api_base_url: str | None = None,
    debug_api_base_url: str | None = None,
    existing_public_config: dict[str, Any] | None = None,
    config_values: dict[str, Any] | None = None,
    compiled_contract: dict[str, Any] | None = None,
    contract_version: str | None = None,
    request_timeout_ms: int | None = None,
    query_cache_policy: str | None = None,
    query_cache_ttl_ms: int | None = None,
    dedupe_in_flight: bool | None = None,
) -> dict[str, Any]:
    public_config_model = _adapter_from_api_public_config_model()
    normalized_api_base_url = (
        api_base_url.strip() if isinstance(api_base_url, str) else api_base_url
    )
    normalized_debug_api_base_url = (
        debug_api_base_url.strip() if isinstance(debug_api_base_url, str) else debug_api_base_url
    )
    normalized_api_base_url = normalized_api_base_url or None
    normalized_debug_api_base_url = normalized_debug_api_base_url or None

    source_count = sum(
        value is not None
        for value in (public_config, normalized_api_base_url, normalized_debug_api_base_url)
    )
    if source_count > 1:
        raise ApiError(
            "Provide only one public config source: public_config, api_base_url, "
            "or debug_api_base_url."
        )

    optional_kwargs: dict[str, Any] = {}
    if config_values is not None:
        optional_kwargs["config_values"] = dict(config_values)
    if compiled_contract is not None:
        optional_kwargs["compiled_contract"] = dict(compiled_contract)
    if contract_version is not None:
        optional_kwargs["contract_version"] = contract_version
    if request_timeout_ms is not None:
        optional_kwargs["request_timeout_ms"] = request_timeout_ms
    if query_cache_policy is not None:
        optional_kwargs["query_cache_policy"] = query_cache_policy
    if query_cache_ttl_ms is not None:
        optional_kwargs["query_cache_ttl_ms"] = query_cache_ttl_ms
    if dedupe_in_flight is not None:
        optional_kwargs["dedupe_in_flight"] = dedupe_in_flight

    alias_updates = {
        "configValues": config_values,
        "compiledContract": compiled_contract,
        "contractVersion": contract_version,
        "requestTimeoutMs": request_timeout_ms,
        "queryCachePolicy": query_cache_policy,
        "queryCacheTtlMs": query_cache_ttl_ms,
        "dedupeInFlight": dedupe_in_flight,
    }

    try:
        if public_config is not None:
            payload = dict(public_config)
            payload.update(
                {key: value for key, value in alias_updates.items() if value is not None}
            )
            config = public_config_model.model_validate(payload)
        elif normalized_debug_api_base_url is not None:
            config = public_config_model.direct(
                debug_api_base_url=normalized_debug_api_base_url,
                **optional_kwargs,
            )
        elif normalized_api_base_url is not None:
            config = public_config_model.backend(
                api_base_url=normalized_api_base_url,
                **optional_kwargs,
            )
        else:
            payload = dict(existing_public_config or {})
            if not payload:
                raise ApiError(
                    "Adapter from API public config is required. Provide api_base_url, "
                    "debug_api_base_url, or public_config."
                )
            payload.update(
                {key: value for key, value in alias_updates.items() if value is not None}
            )
            config = public_config_model.model_validate(payload)
    except ApiError:
        raise
    except Exception as e:
        raise ApiError(f"Adapter from API public config is invalid: {e}") from e

    return config.to_public_config()


def create_adapter_from_api_connection(
    *,
    name: str,
    description: str = "",
    public_config: dict[str, Any] | None = None,
    api_base_url: str | None = None,
    debug_api_base_url: str | None = None,
    secure_config: dict[str, Any] | None = None,
    workspace_uid: str | None = None,
    is_default: bool | None = None,
    tags: list[str] | None = None,
    config_values: dict[str, Any] | None = None,
    compiled_contract: dict[str, Any] | None = None,
    contract_version: str | None = None,
    request_timeout_ms: int | None = None,
    query_cache_policy: str | None = None,
    query_cache_ttl_ms: int | None = None,
    dedupe_in_flight: bool | None = None,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Create one command_center.adapter_from_api connection through the SDK client model.
    """
    public_config_payload = _adapter_from_api_public_config_payload(
        public_config=public_config,
        api_base_url=api_base_url,
        debug_api_base_url=debug_api_base_url,
        config_values=config_values,
        compiled_contract=compiled_contract,
        contract_version=contract_version,
        request_timeout_ms=request_timeout_ms,
        query_cache_policy=query_cache_policy,
        query_cache_ttl_ms=query_cache_ttl_ms,
        dedupe_in_flight=dedupe_in_flight,
    )

    try:
        connection = _run_sdk_model_operation(
            module_name="mainsequence.client.command_center.connections",
            class_name="ConnectionInstance",
            operation=lambda ClientConnectionInstance: ClientConnectionInstance.create_adapter_from_api(
                name=name,
                description=description,
                public_config=public_config_payload,
                secure_config=secure_config,
                workspace_uid=workspace_uid,
                is_default=is_default,
                tags=tags,
                timeout=timeout,
            ),
        )
        return _sdk_object_to_dict(connection)
    except Exception as e:
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Adapter from API connection creation failed: {e}") from e


def patch_adapter_from_api_connection(
    connection_uid: str,
    *,
    name: str | None = None,
    description: str | None = None,
    public_config: dict[str, Any] | None = None,
    api_base_url: str | None = None,
    debug_api_base_url: str | None = None,
    secure_config: dict[str, Any] | None = None,
    workspace_uid: str | None = None,
    is_default: bool | None = None,
    tags: list[str] | None = None,
    config_values: dict[str, Any] | None = None,
    compiled_contract: dict[str, Any] | None = None,
    contract_version: str | None = None,
    request_timeout_ms: int | None = None,
    query_cache_policy: str | None = None,
    query_cache_ttl_ms: int | None = None,
    dedupe_in_flight: bool | None = None,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Patch one command_center.adapter_from_api connection through the SDK client model.
    """
    patch_kwargs: dict[str, Any] = {}
    if name is not None:
        patch_kwargs["name"] = name
    if description is not None:
        patch_kwargs["description"] = description
    if secure_config is not None:
        patch_kwargs["secureConfig"] = secure_config
    if workspace_uid is not None:
        patch_kwargs["workspaceUid"] = workspace_uid
    if is_default is not None:
        patch_kwargs["isDefault"] = is_default
    if tags is not None:
        patch_kwargs["tags"] = list(tags)

    public_config_requested = _adapter_from_api_public_config_requested(
        public_config=public_config,
        api_base_url=api_base_url,
        debug_api_base_url=debug_api_base_url,
        config_values=config_values,
        compiled_contract=compiled_contract,
        contract_version=contract_version,
        request_timeout_ms=request_timeout_ms,
        query_cache_policy=query_cache_policy,
        query_cache_ttl_ms=query_cache_ttl_ms,
        dedupe_in_flight=dedupe_in_flight,
    )
    if not patch_kwargs and not public_config_requested:
        raise ApiError("Adapter from API connection patch payload is empty.")

    try:

        def _patch(ClientConnectionInstance):
            connection = ClientConnectionInstance.get_adapter_from_api(
                uid=str(connection_uid),
                timeout=timeout,
            )
            resolved_patch_kwargs = dict(patch_kwargs)
            if public_config_requested:
                resolved_patch_kwargs["publicConfig"] = _adapter_from_api_public_config_payload(
                    public_config=public_config,
                    api_base_url=api_base_url,
                    debug_api_base_url=debug_api_base_url,
                    existing_public_config=getattr(connection, "public_config", None),
                    config_values=config_values,
                    compiled_contract=compiled_contract,
                    contract_version=contract_version,
                    request_timeout_ms=request_timeout_ms,
                    query_cache_policy=query_cache_policy,
                    query_cache_ttl_ms=query_cache_ttl_ms,
                    dedupe_in_flight=dedupe_in_flight,
                )
            return connection.patch(**resolved_patch_kwargs)

        connection = _run_sdk_model_operation(
            module_name="mainsequence.client.command_center.connections",
            class_name="ConnectionInstance",
            operation=_patch,
        )
        return _sdk_object_to_dict(connection)
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"Connection not found: {connection_uid}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Adapter from API connection patch failed: {e}") from e


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
        `TimeIndexMetaTable.description_search()`
    """
    try:
        payload = _run_sdk_model_operation(
            module_name="mainsequence.client.metatables",
            class_name="TimeIndexMetaTable",
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
        `TimeIndexMetaTable.column_search()`
    """
    try:
        payload = _run_sdk_model_operation(
            module_name="mainsequence.client.metatables",
            class_name="TimeIndexMetaTable",
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
            module_name="mainsequence.client.models_foundry",
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
            module_name="mainsequence.client.models_foundry",
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
    secret_uid: str,
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
            module_name="mainsequence.client.models_foundry",
            class_name="Secret",
            operation=lambda ClientSecret: ClientSecret.get(
                pk=secret_uid,
                timeout=timeout,
            ),
        )
        return _sdk_object_to_dict(secret)
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"Secret not found: {secret_uid}") from e
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
            module_name="mainsequence.client.models_foundry",
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
    secret_uid: str,
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
            secret = ClientSecret.get(pk=secret_uid, timeout=timeout)
            payload = _sdk_object_to_dict(secret)
            secret.delete(timeout=timeout)
            return payload

        return _run_sdk_model_operation(
            module_name="mainsequence.client.models_foundry",
            class_name="Secret",
            operation=_delete,
        )
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"Secret not found: {secret_uid}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Secret deletion failed: {e}") from e


def list_secret_users_can_view(
    secret_uid: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Fetch the view-access state for a secret via `ShareableObjectMixin.can_view()`.
    """
    return _get_shareable_object_access_state(
        module_name="mainsequence.client.models_foundry",
        class_name="Secret",
        object_id=secret_uid,
        accessor_name="can_view",
        timeout=timeout,
    )


def list_secret_users_can_edit(
    secret_uid: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Fetch the edit-access state for a secret via `ShareableObjectMixin.can_edit()`.
    """
    return _get_shareable_object_access_state(
        module_name="mainsequence.client.models_foundry",
        class_name="Secret",
        object_id=secret_uid,
        accessor_name="can_edit",
        timeout=timeout,
    )


def add_secret_user_to_view(
    secret_uid: int | str,
    user_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Grant explicit view access to one user for a secret.
    """
    return _mutate_shareable_object_access(
        module_name="mainsequence.client.models_foundry",
        class_name="Secret",
        object_id=secret_uid,
        action_name="add_to_view",
        user_id=user_id,
        timeout=timeout,
    )


def add_secret_user_to_edit(
    secret_uid: int | str,
    user_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Grant explicit edit access to one user for a secret.
    """
    return _mutate_shareable_object_access(
        module_name="mainsequence.client.models_foundry",
        class_name="Secret",
        object_id=secret_uid,
        action_name="add_to_edit",
        user_id=user_id,
        timeout=timeout,
    )


def remove_secret_user_from_view(
    secret_uid: int | str,
    user_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Remove explicit view access from one user for a secret.
    """
    return _mutate_shareable_object_access(
        module_name="mainsequence.client.models_foundry",
        class_name="Secret",
        object_id=secret_uid,
        action_name="remove_from_view",
        user_id=user_id,
        timeout=timeout,
    )


def remove_secret_user_from_edit(
    secret_uid: int | str,
    user_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Remove explicit edit access from one user for a secret.
    """
    return _mutate_shareable_object_access(
        module_name="mainsequence.client.models_foundry",
        class_name="Secret",
        object_id=secret_uid,
        action_name="remove_from_edit",
        user_id=user_id,
        timeout=timeout,
    )


def add_secret_team_to_view(
    secret_uid: int | str,
    team_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    return _mutate_shareable_object_team_access(
        module_name="mainsequence.client.models_foundry",
        class_name="Secret",
        object_id=secret_uid,
        action_name="add_team_to_view",
        team_id=team_id,
        timeout=timeout,
    )


def add_secret_team_to_edit(
    secret_uid: int | str,
    team_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    return _mutate_shareable_object_team_access(
        module_name="mainsequence.client.models_foundry",
        class_name="Secret",
        object_id=secret_uid,
        action_name="add_team_to_edit",
        team_id=team_id,
        timeout=timeout,
    )


def remove_secret_team_from_view(
    secret_uid: int | str,
    team_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    return _mutate_shareable_object_team_access(
        module_name="mainsequence.client.models_foundry",
        class_name="Secret",
        object_id=secret_uid,
        action_name="remove_team_from_view",
        team_id=team_id,
        timeout=timeout,
    )


def remove_secret_team_from_edit(
    secret_uid: int | str,
    team_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    return _mutate_shareable_object_team_access(
        module_name="mainsequence.client.models_foundry",
        class_name="Secret",
        object_id=secret_uid,
        action_name="remove_team_from_edit",
        team_id=team_id,
        timeout=timeout,
    )


def get_constant(
    constant_uid: str,
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
            module_name="mainsequence.client.models_foundry",
            class_name="Constant",
            operation=lambda ClientConstant: ClientConstant.get_by_uid(
                str(constant_uid),
                timeout=timeout,
            ),
        )
        return _sdk_object_to_dict(constant)
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"Constant not found: {constant_uid}") from e
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
            module_name="mainsequence.client.models_foundry",
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
    constant_uid: str,
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
            constant = ClientConstant.get_by_uid(str(constant_uid), timeout=timeout)
            payload = _sdk_object_to_dict(constant)
            constant.delete(timeout=timeout)
            return payload

        return _run_sdk_model_operation(
            module_name="mainsequence.client.models_foundry",
            class_name="Constant",
            operation=_delete,
        )
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"Constant not found: {constant_uid}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Constant deletion failed: {e}") from e


def _get_client_object_by_lookup(
    ClientObject,
    *,
    object_id: int | str,
    lookup_field: str,
    timeout: int | None = None,
):
    if lookup_field == "uid":
        return ClientObject.get_by_uid(str(object_id), timeout=timeout)
    return ClientObject.get(pk=int(object_id), timeout=timeout)


def _get_shareable_object_access_state(
    *,
    module_name: str,
    class_name: str,
    object_id: int | str,
    accessor_name: str,
    object_lookup_field: str = "uid",
    timeout: int | None = None,
) -> dict[str, Any]:
    try:
        payload = _run_sdk_model_operation(
            module_name=module_name,
            class_name=class_name,
            operation=lambda ClientObject: getattr(
                _get_client_object_by_lookup(
                    ClientObject,
                    object_id=object_id,
                    lookup_field=object_lookup_field,
                    timeout=timeout,
                ),
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
    object_lookup_field: str = "uid",
    timeout: int | None = None,
) -> dict[str, Any]:
    try:
        payload = _run_sdk_model_operation(
            module_name=module_name,
            class_name=class_name,
            operation=lambda ClientObject: getattr(
                _get_client_object_by_lookup(
                    ClientObject,
                    object_id=object_id,
                    lookup_field=object_lookup_field,
                    timeout=timeout,
                ),
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
    object_lookup_field: str = "uid",
    timeout: int | None = None,
) -> dict[str, Any]:
    try:
        payload = _run_sdk_model_operation(
            module_name=module_name,
            class_name=class_name,
            operation=lambda ClientObject: getattr(
                _get_client_object_by_lookup(
                    ClientObject,
                    object_id=object_id,
                    lookup_field=object_lookup_field,
                    timeout=timeout,
                ),
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
    object_lookup_field: str = "uid",
    timeout: int | None = None,
) -> dict[str, Any]:
    try:
        payload = _run_sdk_model_operation(
            module_name=module_name,
            class_name=class_name,
            operation=lambda ClientObject: getattr(
                _get_client_object_by_lookup(
                    ClientObject,
                    object_id=object_id,
                    lookup_field=object_lookup_field,
                    timeout=timeout,
                ),
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
    constant_uid: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Fetch the view-access state for a constant via `ShareableObjectMixin.can_view()`.
    """
    return _get_shareable_object_access_state(
        module_name="mainsequence.client.models_foundry",
        class_name="Constant",
        object_id=constant_uid,
        accessor_name="can_view",
        timeout=timeout,
    )


def list_constant_users_can_edit(
    constant_uid: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Fetch the edit-access state for a constant via `ShareableObjectMixin.can_edit()`.
    """
    return _get_shareable_object_access_state(
        module_name="mainsequence.client.models_foundry",
        class_name="Constant",
        object_id=constant_uid,
        accessor_name="can_edit",
        timeout=timeout,
    )


def add_constant_user_to_view(
    constant_uid: int | str,
    user_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Grant explicit view access to one user for a constant.
    """
    return _mutate_shareable_object_access(
        module_name="mainsequence.client.models_foundry",
        class_name="Constant",
        object_id=constant_uid,
        action_name="add_to_view",
        user_id=user_id,
        timeout=timeout,
    )


def add_constant_user_to_edit(
    constant_uid: int | str,
    user_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Grant explicit edit access to one user for a constant.
    """
    return _mutate_shareable_object_access(
        module_name="mainsequence.client.models_foundry",
        class_name="Constant",
        object_id=constant_uid,
        action_name="add_to_edit",
        user_id=user_id,
        timeout=timeout,
    )


def remove_constant_user_from_view(
    constant_uid: int | str,
    user_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Remove explicit view access from one user for a constant.
    """
    return _mutate_shareable_object_access(
        module_name="mainsequence.client.models_foundry",
        class_name="Constant",
        object_id=constant_uid,
        action_name="remove_from_view",
        user_id=user_id,
        timeout=timeout,
    )


def remove_constant_user_from_edit(
    constant_uid: int | str,
    user_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Remove explicit edit access from one user for a constant.
    """
    return _mutate_shareable_object_access(
        module_name="mainsequence.client.models_foundry",
        class_name="Constant",
        object_id=constant_uid,
        action_name="remove_from_edit",
        user_id=user_id,
        timeout=timeout,
    )


def add_constant_team_to_view(
    constant_uid: int | str,
    team_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    return _mutate_shareable_object_team_access(
        module_name="mainsequence.client.models_foundry",
        class_name="Constant",
        object_id=constant_uid,
        action_name="add_team_to_view",
        team_id=team_id,
        timeout=timeout,
    )


def add_constant_team_to_edit(
    constant_uid: int | str,
    team_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    return _mutate_shareable_object_team_access(
        module_name="mainsequence.client.models_foundry",
        class_name="Constant",
        object_id=constant_uid,
        action_name="add_team_to_edit",
        team_id=team_id,
        timeout=timeout,
    )


def remove_constant_team_from_view(
    constant_uid: int | str,
    team_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    return _mutate_shareable_object_team_access(
        module_name="mainsequence.client.models_foundry",
        class_name="Constant",
        object_id=constant_uid,
        action_name="remove_team_from_view",
        team_id=team_id,
        timeout=timeout,
    )


def remove_constant_team_from_edit(
    constant_uid: int | str,
    team_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    return _mutate_shareable_object_team_access(
        module_name="mainsequence.client.models_foundry",
        class_name="Constant",
        object_id=constant_uid,
        action_name="remove_team_from_edit",
        team_id=team_id,
        timeout=timeout,
    )


def get_data_node_storage(
    storage_uid: str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Retrieve one data node storage via SDK client model.

    Single source of truth:
      - delegates detail fetching and payload parsing to `TimeIndexMetaTable.get()`
    """
    try:
        storage = _run_sdk_model_operation(
            module_name="mainsequence.client.metatables",
            class_name="TimeIndexMetaTable",
            operation=lambda ClientDataNodeStorage: ClientDataNodeStorage.get(
                uid=str(storage_uid),
                timeout=timeout,
            ),
        )
        return _sdk_object_to_dict(storage)
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"Data node storage not found: {storage_uid}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Data node storage fetch failed: {e}") from e


def get_meta_table(
    meta_table_uid: str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Retrieve one MetaTable via SDK client model.

    Single source of truth:
      - delegates detail fetching and payload parsing to `MetaTable.get()`
    """
    try:
        meta_table = _run_sdk_model_operation(
            module_name="mainsequence.client.metatables",
            class_name="MetaTable",
            operation=lambda ClientMetaTable: ClientMetaTable.get(
                uid=str(meta_table_uid),
                timeout=timeout,
            ),
        )
        return _sdk_object_to_dict(meta_table)
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"MetaTable not found: {meta_table_uid}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"MetaTable fetch failed: {e}") from e


def refresh_data_node_storage_search_index(
    storage_uid: str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Refresh one data node storage search index via SDK client model.

    Single source of truth:
      - delegates the refresh call to `TimeIndexMetaTable.refresh_table_search_index()`
    """
    try:

        def _refresh(ClientDataNodeStorage):
            storage = ClientDataNodeStorage.get(uid=str(storage_uid), timeout=timeout)
            payload = storage.refresh_table_search_index(timeout=timeout)
            if isinstance(payload, dict):
                out = dict(payload)
                out.setdefault("uid", str(storage_uid))
                return out
            return {"uid": str(storage_uid)}

        return _run_sdk_model_operation(
            module_name="mainsequence.client.metatables",
            class_name="TimeIndexMetaTable",
            operation=_refresh,
        )
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"Data node storage not found: {storage_uid}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Data node storage search index refresh failed: {e}") from e


def run_data_node_storage_query(
    storage_uid: str,
    sql: str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Run a raw SQL query against one data node storage via SDK client model.
    """
    try:

        def _run_query(ClientDataNodeStorage):
            storage = ClientDataNodeStorage.get(uid=str(storage_uid), timeout=timeout)
            payload = storage.run_query(sql, timeout=timeout)
            return dict(payload) if isinstance(payload, dict) else {"ok": True, "results": payload}

        return _run_sdk_model_operation(
            module_name="mainsequence.client.metatables",
            class_name="TimeIndexMetaTable",
            operation=_run_query,
        )
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"Data node storage not found: {storage_uid}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Data node query failed: {e}") from e


def delete_data_node_storage(
    storage_uid: str,
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
      - delegates deletion and destroy query params to `TimeIndexMetaTable.delete()`
    """
    try:

        def _delete(ClientDataNodeStorage):
            storage = ClientDataNodeStorage.get(uid=str(storage_uid), timeout=timeout)
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
            module_name="mainsequence.client.metatables",
            class_name="TimeIndexMetaTable",
            operation=_delete,
        )
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"Data node storage not found: {storage_uid}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Data node storage deletion failed: {e}") from e


def delete_meta_table(
    meta_table_uid: str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Delete one MetaTable via SDK client model.

    Single source of truth:
      - delegates deletion to `MetaTable.delete()`
    """
    try:

        def _delete(ClientMetaTable):
            meta_table = ClientMetaTable.get(uid=str(meta_table_uid), timeout=timeout)
            payload = _sdk_object_to_dict(meta_table)
            meta_table.delete(timeout=timeout)
            return payload

        return _run_sdk_model_operation(
            module_name="mainsequence.client.metatables",
            class_name="MetaTable",
            operation=_delete,
        )
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"MetaTable not found: {meta_table_uid}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"MetaTable deletion failed: {e}") from e


def list_data_node_storage_users_can_view(
    storage_uid: str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Fetch the view-access state for a data node storage via `ShareableObjectMixin.can_view()`.
    """
    return _get_shareable_object_access_state(
        module_name="mainsequence.client.metatables",
        class_name="TimeIndexMetaTable",
        object_id=storage_uid,
        object_lookup_field="uid",
        accessor_name="can_view",
        timeout=timeout,
    )


def list_meta_table_users_can_view(
    meta_table_uid: str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Fetch the view-access state for a MetaTable via `ShareableObjectMixin.can_view()`.
    """
    return _get_shareable_object_access_state(
        module_name="mainsequence.client.metatables",
        class_name="MetaTable",
        object_id=meta_table_uid,
        object_lookup_field="uid",
        accessor_name="can_view",
        timeout=timeout,
    )


def list_data_node_storage_users_can_edit(
    storage_uid: str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Fetch the edit-access state for a data node storage via `ShareableObjectMixin.can_edit()`.
    """
    return _get_shareable_object_access_state(
        module_name="mainsequence.client.metatables",
        class_name="TimeIndexMetaTable",
        object_id=storage_uid,
        object_lookup_field="uid",
        accessor_name="can_edit",
        timeout=timeout,
    )


def list_meta_table_users_can_edit(
    meta_table_uid: str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Fetch the edit-access state for a MetaTable via `ShareableObjectMixin.can_edit()`.
    """
    return _get_shareable_object_access_state(
        module_name="mainsequence.client.metatables",
        class_name="MetaTable",
        object_id=meta_table_uid,
        object_lookup_field="uid",
        accessor_name="can_edit",
        timeout=timeout,
    )


def add_data_node_storage_user_to_view(
    storage_uid: str,
    user_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Grant explicit view access to one user for a data node storage.
    """
    return _mutate_shareable_object_access(
        module_name="mainsequence.client.metatables",
        class_name="TimeIndexMetaTable",
        object_id=storage_uid,
        object_lookup_field="uid",
        action_name="add_to_view",
        user_id=user_id,
        timeout=timeout,
    )


def add_meta_table_user_to_view(
    meta_table_uid: str,
    user_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Grant explicit view access to one user for a MetaTable.
    """
    return _mutate_shareable_object_access(
        module_name="mainsequence.client.metatables",
        class_name="MetaTable",
        object_id=meta_table_uid,
        object_lookup_field="uid",
        action_name="add_to_view",
        user_id=user_id,
        timeout=timeout,
    )


def add_data_node_storage_user_to_edit(
    storage_uid: str,
    user_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Grant explicit edit access to one user for a data node storage.
    """
    return _mutate_shareable_object_access(
        module_name="mainsequence.client.metatables",
        class_name="TimeIndexMetaTable",
        object_id=storage_uid,
        object_lookup_field="uid",
        action_name="add_to_edit",
        user_id=user_id,
        timeout=timeout,
    )


def add_meta_table_user_to_edit(
    meta_table_uid: str,
    user_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Grant explicit edit access to one user for a MetaTable.
    """
    return _mutate_shareable_object_access(
        module_name="mainsequence.client.metatables",
        class_name="MetaTable",
        object_id=meta_table_uid,
        object_lookup_field="uid",
        action_name="add_to_edit",
        user_id=user_id,
        timeout=timeout,
    )


def remove_data_node_storage_user_from_view(
    storage_uid: str,
    user_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Remove explicit view access from one user for a data node storage.
    """
    return _mutate_shareable_object_access(
        module_name="mainsequence.client.metatables",
        class_name="TimeIndexMetaTable",
        object_id=storage_uid,
        object_lookup_field="uid",
        action_name="remove_from_view",
        user_id=user_id,
        timeout=timeout,
    )


def remove_meta_table_user_from_view(
    meta_table_uid: str,
    user_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Remove explicit view access from one user for a MetaTable.
    """
    return _mutate_shareable_object_access(
        module_name="mainsequence.client.metatables",
        class_name="MetaTable",
        object_id=meta_table_uid,
        object_lookup_field="uid",
        action_name="remove_from_view",
        user_id=user_id,
        timeout=timeout,
    )


def remove_data_node_storage_user_from_edit(
    storage_uid: str,
    user_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Remove explicit edit access from one user for a data node storage.
    """
    return _mutate_shareable_object_access(
        module_name="mainsequence.client.metatables",
        class_name="TimeIndexMetaTable",
        object_id=storage_uid,
        object_lookup_field="uid",
        action_name="remove_from_edit",
        user_id=user_id,
        timeout=timeout,
    )


def remove_meta_table_user_from_edit(
    meta_table_uid: str,
    user_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Remove explicit edit access from one user for a MetaTable.
    """
    return _mutate_shareable_object_access(
        module_name="mainsequence.client.metatables",
        class_name="MetaTable",
        object_id=meta_table_uid,
        object_lookup_field="uid",
        action_name="remove_from_edit",
        user_id=user_id,
        timeout=timeout,
    )


def add_data_node_storage_team_to_view(
    storage_uid: str,
    team_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    return _mutate_shareable_object_team_access(
        module_name="mainsequence.client.metatables",
        class_name="TimeIndexMetaTable",
        object_id=storage_uid,
        object_lookup_field="uid",
        action_name="add_team_to_view",
        team_id=team_id,
        timeout=timeout,
    )


def add_meta_table_team_to_view(
    meta_table_uid: str,
    team_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    return _mutate_shareable_object_team_access(
        module_name="mainsequence.client.metatables",
        class_name="MetaTable",
        object_id=meta_table_uid,
        object_lookup_field="uid",
        action_name="add_team_to_view",
        team_id=team_id,
        timeout=timeout,
    )


def add_data_node_storage_team_to_edit(
    storage_uid: str,
    team_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    return _mutate_shareable_object_team_access(
        module_name="mainsequence.client.metatables",
        class_name="TimeIndexMetaTable",
        object_id=storage_uid,
        object_lookup_field="uid",
        action_name="add_team_to_edit",
        team_id=team_id,
        timeout=timeout,
    )


def add_meta_table_team_to_edit(
    meta_table_uid: str,
    team_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    return _mutate_shareable_object_team_access(
        module_name="mainsequence.client.metatables",
        class_name="MetaTable",
        object_id=meta_table_uid,
        object_lookup_field="uid",
        action_name="add_team_to_edit",
        team_id=team_id,
        timeout=timeout,
    )


def remove_data_node_storage_team_from_view(
    storage_uid: str,
    team_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    return _mutate_shareable_object_team_access(
        module_name="mainsequence.client.metatables",
        class_name="TimeIndexMetaTable",
        object_id=storage_uid,
        object_lookup_field="uid",
        action_name="remove_team_from_view",
        team_id=team_id,
        timeout=timeout,
    )


def remove_meta_table_team_from_view(
    meta_table_uid: str,
    team_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    return _mutate_shareable_object_team_access(
        module_name="mainsequence.client.metatables",
        class_name="MetaTable",
        object_id=meta_table_uid,
        object_lookup_field="uid",
        action_name="remove_team_from_view",
        team_id=team_id,
        timeout=timeout,
    )


def remove_data_node_storage_team_from_edit(
    storage_uid: str,
    team_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    return _mutate_shareable_object_team_access(
        module_name="mainsequence.client.metatables",
        class_name="TimeIndexMetaTable",
        object_id=storage_uid,
        object_lookup_field="uid",
        action_name="remove_team_from_edit",
        team_id=team_id,
        timeout=timeout,
    )


def remove_meta_table_team_from_edit(
    meta_table_uid: str,
    team_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    return _mutate_shareable_object_team_access(
        module_name="mainsequence.client.metatables",
        class_name="MetaTable",
        object_id=meta_table_uid,
        object_lookup_field="uid",
        action_name="remove_team_from_edit",
        team_id=team_id,
        timeout=timeout,
    )


def add_data_node_storage_labels(
    storage_uid: str,
    labels: list[str],
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """Attach one or more organizational labels to a data node storage."""

    return _mutate_labelable_object_labels(
        module_name="mainsequence.client.metatables",
        class_name="TimeIndexMetaTable",
        object_id=storage_uid,
        object_lookup_field="uid",
        action_name="add_label",
        labels=labels,
        timeout=timeout,
    )


def add_meta_table_labels(
    meta_table_uid: str,
    labels: list[str],
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """Attach one or more organizational labels to a MetaTable."""

    return _mutate_labelable_object_labels(
        module_name="mainsequence.client.metatables",
        class_name="MetaTable",
        object_id=meta_table_uid,
        object_lookup_field="uid",
        action_name="add_label",
        labels=labels,
        timeout=timeout,
    )


def remove_data_node_storage_labels(
    storage_uid: str,
    labels: list[str],
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """Remove one or more organizational labels from a data node storage."""

    return _mutate_labelable_object_labels(
        module_name="mainsequence.client.metatables",
        class_name="TimeIndexMetaTable",
        object_id=storage_uid,
        object_lookup_field="uid",
        action_name="remove_label",
        labels=labels,
        timeout=timeout,
    )


def remove_meta_table_labels(
    meta_table_uid: str,
    labels: list[str],
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """Remove one or more organizational labels from a MetaTable."""

    return _mutate_labelable_object_labels(
        module_name="mainsequence.client.metatables",
        class_name="MetaTable",
        object_id=meta_table_uid,
        object_lookup_field="uid",
        action_name="remove_label",
        labels=labels,
        timeout=timeout,
    )


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
        "MAIN_SEQUENCE_PROJECT_UID": os.environ.get("MAIN_SEQUENCE_PROJECT_UID"),
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
        project_uid = resolve_project_uid(project_id)
        project_row_id = resolve_project_row_id(project_id)
        os.environ["MAIN_SEQUENCE_PROJECT_UID"] = project_uid
        os.environ["MAIN_SEQUENCE_PROJECT_ID"] = str(project_row_id)

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
            project_id=project_row_id,
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
        "MAIN_SEQUENCE_PROJECT_UID": os.environ.get("MAIN_SEQUENCE_PROJECT_UID"),
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
        project_uid = resolve_project_uid(project_id)
        project_row_id = resolve_project_row_id(project_id)
        os.environ["MAIN_SEQUENCE_PROJECT_UID"] = project_uid
        os.environ["MAIN_SEQUENCE_PROJECT_ID"] = str(project_row_id)

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
            project_id=project_row_id,
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


def get_project_job(
    job_id: int | str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    """
    Retrieve one project job via SDK client model.
    """
    try:
        job = _run_sdk_model_operation(
            module_name="mainsequence.client.models_helpers",
            class_name="Job",
            operation=lambda ClientJob: ClientJob.get(pk=int(job_id), timeout=timeout),
        )
        return _sdk_object_to_dict(job)
    except Exception as e:
        err_name = type(e).__name__
        if err_name == "NotFoundError":
            raise ApiError(f"Job not found: {job_id}") from e
        if isinstance(e, (ApiError, NotLoggedIn)):
            raise
        raise ApiError(f"Project job fetch failed: {e}") from e


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
        effective_tokens: list[str] = []
        execution_path = str(getattr(job, "execution_path", "") or "").strip()
        app_name = str(getattr(job, "app_name", "") or "").strip()
        if execution_path:
            effective_tokens.append(execution_path)
        elif app_name:
            effective_tokens.append(f"app:{app_name}")
        if command_args:
            effective_tokens.extend(str(arg) for arg in command_args)
        effective_run = shlex.join(effective_tokens) if effective_tokens else None
        if isinstance(payload, dict):
            if effective_run:
                payload.setdefault("effective_run", effective_run)
            if command_args is not None:
                payload.setdefault("command_args", list(command_args))
            return payload
        if hasattr(payload, "model_dump"):
            payload = payload.model_dump()
            if effective_run:
                payload.setdefault("effective_run", effective_run)
            if command_args is not None:
                payload.setdefault("command_args", list(command_args))
            return payload
        out = {"job_id": int(job_id)}
        if effective_run:
            out["effective_run"] = effective_run
        if command_args is not None:
            out["command_args"] = list(command_args)
        return out

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
    job_run_uid: str,
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

        job_run = ClientJobRun.get(pk=job_run_uid, timeout=timeout)
        payload = job_run.get_logs(timeout=timeout)
        if isinstance(payload, dict):
            return payload
        if hasattr(payload, "model_dump"):
            return payload.model_dump()
        return {"job_run_uid": job_run_uid, "rows": []}

    except Exception as e:
        err_name = type(e).__name__
        if err_name in {"AuthenticationError", "PermissionDeniedError"}:
            raise NotLoggedIn(str(e) or "Not logged in.") from e
        if err_name == "NotFoundError":
            raise ApiError(f"Job run not found: {job_run_uid}") from e
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


def delete_project(
    project_id: int | str, *, delete_repositories: bool = False
) -> dict[str, Any] | None:
    """
    Delete a project by public reference.

    Mirrors backend behavior:
      - DELETE /orm/api/pods/projects/{uid}/
      - optional query param delete_repositories=true
    """
    project_uid = resolve_project_uid(project_id)
    path = f"/orm/api/pods/projects/{project_uid}/"
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
        f"/orm/api/pods/projects/{resolve_project_uid(project_id)}/add_deploy_key/",
        {"key_title": key_title, "public_key": public_key},
    )
    r.raise_for_status()
