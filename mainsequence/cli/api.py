"""
mainsequence.cli.api
====================

HTTP API wrapper for MainSequence CLI.

This module is intentionally aligned with the VS Code extension implementation:
- Browser-based code exchange + refresh
- authed() retries after refresh on 401
- CodeRepository helpers: list repositories, get env text, deploy key

Any behavioral differences vs the VS Code extension should be considered bugs.
"""

from __future__ import annotations

import importlib.metadata
import json
import os
import re
from typing import Any
from urllib.parse import urlencode

import requests

from ..code_repository_skills import (
    PLATFORM_ONTOLOGY_URI,
    PLATFORM_SKILL_URI_PREFIX,
    CodeRepositorySkillAssemblyError,
    PlatformCodeRepositorySkillCatalog,
    parse_platform_code_repository_skill_catalog,
    parse_platform_code_repository_skill_declarations,
    validate_platform_code_repository_skill_membership,
)
from .config import (
    backend_url,
    get_tokens,
    save_tokens,
)

AUTH_PATHS = {
    "authorize": "/auth/cli/authorize/",
    "cli_token": "/auth/cli/token/",
    "cli_revoke": "/auth/cli/revoke/",
    "refresh": "/auth/jwt-token/token/refresh/",
    "logout": "/auth/jwt-token/logout/",
    "mcp_cli_handoff_start": "/auth/mcp/cli-handoff/start/",
}
CLI_BROWSER_CLIENT_ID = "mainsequence-cli"
MCP_RESOURCE_PATH = "/mcp"
MCP_PROTOCOL_VERSION = "2025-11-25"
MCP_CLIENT_NAME = "mainsequence-cli"
MCP_MAX_RESOURCE_PAGES = 100

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
        /api/v1, /auth
    """
    p = "/" + (p or "").lstrip("/")
    if not re.match(r"^/(?:api/v1|auth)(?:/|\Z)", p):
        raise ApiError("Only /api/v1/* and /auth/* are allowed")
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
    client_utils.API_ENDPOINT = f"{normalized}/api/v1"
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


def start_mcp_cli_handoff(
    *,
    state: str,
    code_challenge: str,
) -> dict:
    """Create a backend-owned CLI login handoff for an MCP agent."""
    payload = {
        "state": state,
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
    }
    response = S.post(
        _full(AUTH_PATHS["mcp_cli_handoff_start"]),
        data=json.dumps(payload),
    )
    try:
        data = response.json()
    except Exception:
        data = {}
    if not response.ok:
        message = data.get("detail") or data.get("message") or response.text
        raise ApiError(str(message or "Could not create MCP CLI login handoff."))

    required = ("handoff_uid", "redirect_uri", "expires_at", "mcp_tool", "mcp_arguments")
    if not all(data.get(field_name) for field_name in required):
        raise ApiError("Server returned an incomplete MCP CLI login handoff.")
    return data


def poll_mcp_cli_handoff(
    *,
    redirect_uri: str,
    handoff_uid: str,
    state: str,
    code_verifier: str,
) -> dict | None:
    """Poll the exact backend-issued callback and exchange an approved grant."""
    response = S.post(
        redirect_uri,
        data=json.dumps(
            {
                "handoff_uid": handoff_uid,
                "state": state,
                "code_verifier": code_verifier,
            }
        ),
    )
    try:
        data = response.json()
    except Exception:
        data = {}
    if response.status_code == 202:
        return None
    if not response.ok:
        message = data.get("detail") or data.get("message") or response.text
        raise ApiError(str(message or "MCP CLI login handoff failed."))

    access = data.get("access") or data.get("access_token")
    refresh = data.get("refresh") or data.get("refresh_token")
    if not access or not refresh:
        raise ApiError("Server did not return expected CLI login tokens.")
    return {
        "backend": backend_url(),
        "access": str(access),
        "refresh": str(refresh),
        "user": data.get("user"),
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

        token_url = f"{backend_url().rstrip('/')}/api/v1/runtime-credentials/token/"
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


def get_current_user_profile() -> dict:
    """
    Return current user profile (username + organization name) via the canonical user-details endpoint.

    Returns:
        dict: {"username": "...", "organization": "..."} or {}
    """
    details = authed("GET", "/api/v1/users/me/")
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


def _mcp_json_rpc(
    method: str,
    params: dict[str, Any],
    *,
    request_id: int,
    protocol_version: str | None = None,
) -> dict[str, Any]:
    """Send one authenticated stateless MCP JSON-RPC request."""

    access = _access_token()
    if not access:
        access = refresh_access()

    payload = {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": method,
        "params": params,
    }

    def _send(token: str) -> requests.Response:
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if protocol_version is not None:
            headers["MCP-Protocol-Version"] = protocol_version
        return S.request(
            "POST",
            _full(MCP_RESOURCE_PATH),
            headers=headers,
            data=json.dumps(payload),
        )

    response = _send(access)
    if response.status_code == 401:
        response = _send(refresh_access())
    if response.status_code == 401:
        raise NotLoggedIn("Not logged in.")
    if not response.ok:
        raise ApiError(f"MCP {method} request failed ({response.status_code}).")
    if not response.headers.get("content-type", "").startswith("application/json"):
        raise ApiError(f"MCP {method} response was not JSON.")
    try:
        response_payload = response.json()
    except Exception as exc:
        raise ApiError(f"MCP {method} response was not valid JSON.") from exc
    if not isinstance(response_payload, dict):
        raise ApiError(f"MCP {method} response must be an object.")
    if response_payload.get("jsonrpc") != "2.0":
        raise ApiError(f"MCP {method} response has an invalid JSON-RPC version.")
    if response_payload.get("id") != request_id:
        raise ApiError(f"MCP {method} response id does not match the request.")

    error_payload = response_payload.get("error")
    if error_payload is not None:
        if isinstance(error_payload, dict):
            code = error_payload.get("code")
            message = error_payload.get("message") or "Unknown MCP error."
            raise ApiError(f"MCP {method} failed with error {code}: {message}")
        raise ApiError(f"MCP {method} returned an invalid error object.")

    result = response_payload.get("result")
    if not isinstance(result, dict):
        raise ApiError(f"MCP {method} response result must be an object.")
    return result


def _mcp_client_version() -> str:
    try:
        return importlib.metadata.version("mainsequence")
    except importlib.metadata.PackageNotFoundError:
        return "unknown"


def fetch_platform_code_repository_skill_catalog() -> PlatformCodeRepositorySkillCatalog:
    """Fetch and validate the server-owned platform skills through authenticated MCP."""

    initialize_result = _mcp_json_rpc(
        "initialize",
        {
            "protocolVersion": MCP_PROTOCOL_VERSION,
            "capabilities": {},
            "clientInfo": {
                "name": MCP_CLIENT_NAME,
                "version": _mcp_client_version(),
            },
        },
        request_id=1,
    )
    negotiated_protocol_version = initialize_result.get("protocolVersion")
    if negotiated_protocol_version != MCP_PROTOCOL_VERSION:
        raise ApiError(
            "MCP initialize negotiated an unsupported protocol version: "
            f"{negotiated_protocol_version!r}."
        )
    server_capabilities = initialize_result.get("capabilities")
    if not isinstance(server_capabilities, dict) or not isinstance(
        server_capabilities.get("resources"), dict
    ):
        raise ApiError("MCP server did not advertise resource support.")

    resources: list[Any] = []
    request_id = 2
    cursor: str | None = None
    seen_cursors: set[str] = set()
    for _page_number in range(MCP_MAX_RESOURCE_PAGES):
        params = {} if cursor is None else {"cursor": cursor}
        list_result = _mcp_json_rpc(
            "resources/list",
            params,
            request_id=request_id,
            protocol_version=MCP_PROTOCOL_VERSION,
        )
        request_id += 1
        page_resources = list_result.get("resources")
        if not isinstance(page_resources, list):
            raise ApiError("MCP resources/list did not return a resource array.")
        resources.extend(page_resources)

        next_cursor = list_result.get("nextCursor")
        if next_cursor is None:
            break
        if (
            not isinstance(next_cursor, str)
            or not next_cursor.strip()
            or next_cursor != next_cursor.strip()
        ):
            raise ApiError("MCP resources/list returned an invalid nextCursor.")
        if next_cursor in seen_cursors:
            raise ApiError("MCP resources/list repeated a pagination cursor.")
        seen_cursors.add(next_cursor)
        cursor = next_cursor
    else:
        raise ApiError(
            f"MCP resources/list exceeded the {MCP_MAX_RESOURCE_PAGES}-page safety limit."
        )

    resources_by_uri: dict[str, dict[str, Any]] = {}
    for raw_resource in resources:
        if not isinstance(raw_resource, dict):
            raise ApiError("MCP resources/list returned a non-object resource.")
        uri = raw_resource.get("uri")
        if not isinstance(uri, str) or not uri.strip() or uri != uri.strip():
            raise ApiError("MCP resources/list returned a resource without a URI.")
        if uri in resources_by_uri:
            raise ApiError(f"MCP resources/list returned duplicate URI {uri!r}.")
        resources_by_uri[uri] = raw_resource

    ontology_list_row = resources_by_uri.get(PLATFORM_ONTOLOGY_URI)
    if ontology_list_row is None:
        raise ApiError(f"MCP resources/list did not return {PLATFORM_ONTOLOGY_URI!r}.")

    def _read_resource(uri: str, raw_resource: dict[str, Any]) -> dict[str, Any]:
        nonlocal request_id

        read_result = _mcp_json_rpc(
            "resources/read",
            {"uri": uri},
            request_id=request_id,
            protocol_version=MCP_PROTOCOL_VERSION,
        )
        request_id += 1
        contents = read_result.get("contents")
        if not isinstance(contents, list) or len(contents) != 1:
            raise ApiError(f"MCP resources/read for {uri!r} must return exactly one content item.")
        content = contents[0]
        if not isinstance(content, dict):
            raise ApiError(f"MCP resources/read for {uri!r} returned a non-object content item.")
        return {**raw_resource, "_content": content}

    ontology_row = _read_resource(PLATFORM_ONTOLOGY_URI, ontology_list_row)
    try:
        declarations = parse_platform_code_repository_skill_declarations(
            ontology_row["_content"].get("text")
        )
        listed_skill_uris = {
            uri for uri in resources_by_uri if uri.startswith(PLATFORM_SKILL_URI_PREFIX)
        }
        validate_platform_code_repository_skill_membership(
            declarations,
            listed_skill_uris=listed_skill_uris,
        )

        platform_rows = [ontology_row]
        platform_rows.extend(
            _read_resource(declaration.uri, resources_by_uri[declaration.uri])
            for declaration in declarations
        )
        return parse_platform_code_repository_skill_catalog(
            platform_rows,
            source_url=_full(MCP_RESOURCE_PATH),
        )
    except CodeRepositorySkillAssemblyError as exc:
        raise ApiError(f"Platform MCP resource catalog is invalid: {exc}") from exc
