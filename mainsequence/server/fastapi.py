"""Application-installed request identity; independent of the platform launcher."""

from __future__ import annotations

import asyncio
import json
import os
import re
from contextlib import asynccontextmanager, contextmanager
from urllib.parse import urlsplit
from uuid import UUID

import requests

from mainsequence._request_identity import (
    RequestIdentityError,
    _get_request_identity,
    _reads_as_caller,
    _request_scope,
)
from mainsequence.client.models_user import RequestUserIdentity

from .caller_assertions import (
    ASSERTION_HEADER,
    CallerAssertionUnavailable,
    CallerAssertionVerifier,
    InvalidCallerAssertion,
)

_INSTALLATION = "mainsequence_request_identity"
_IDENTITY_HEADERS = {
    b"authorization",
    b"x-user-id",
    b"x-user-uid",
    b"x-username",
    b"x-organization-environment-uid",
    b"x-organization-project-environment-uid",
    b"x-resource-release-uid",
    b"x-mainsequence-caller-assertion",
    b"x-websocket-ticket",
    b"x-public-ingress",
}


class _AdmissionError(Exception):
    def __init__(self, status, detail):
        self.status = status
        self.detail = detail


def _mode():
    hosted = any(
        os.environ.get(name)
        for name in (
            "APP_NAME",
            "FASTAPI_PUBLIC_BASE_URL",
            "MAINSEQUENCE_CALLER_ASSERTION_ISSUER",
            "MAINSEQUENCE_CALLER_ASSERTION_JWKS_URL",
        )
    )
    mode = os.environ.get("MAINSEQUENCE_CALLER_AUTH_MODE") or ("assertion" if hosted else "local")
    if mode not in {"local", "assertion"} or (mode == "local" and hosted):
        raise RuntimeError("Invalid caller authentication mode for this deployment.")
    return mode


def _public_routes():
    try:
        entries = json.loads(os.environ.get("FASTAPI_PUBLIC_INGRESS", "[]"))
        if not isinstance(entries, list) or len(entries) > 16:
            raise ValueError
        routes = []
        for entry in entries:
            if not isinstance(entry, dict) or set(entry) != {"method", "path"}:
                raise ValueError
            method, path = entry["method"], entry["path"]
            if (
                method not in {"GET", "POST"}
                or not isinstance(path, str)
                or len(path) > 512
                or re.fullmatch(r"/[A-Za-z0-9_~./-]+", path, flags=re.ASCII) is None
                or "//" in path
                or any(p in {".", ".."} for p in path.split("/"))
                or path
                in {"/", "/ms-health-deployment", "/ready", "/openapi.json", "/docs", "/redoc"}
                or path.startswith("/_")
            ):
                raise ValueError
            routes.append((method, path))
        if len(set(routes)) != len(routes):
            raise ValueError
        return tuple(routes)
    except (ValueError, TypeError, KeyError) as exc:
        raise RuntimeError("Invalid FASTAPI_PUBLIC_INGRESS configuration.") from exc


def _header(scope, name):
    values = [
        value.decode("latin-1") for key, value in scope.get("headers", ()) if key.lower() == name
    ]
    if len(values) > 1:
        raise _AdmissionError(
            403 if scope["type"] == "websocket" else 401, "Conflicting authentication headers."
        )
    return values[0] if values else None


def _local_user(scope):
    authorization = _header(scope, b"authorization") or ""
    scheme, separator, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not separator or not token.strip():
        raise _AdmissionError(401, "Authenticated request user could not be resolved.")
    endpoint = os.environ.get("MAINSEQUENCE_ENDPOINT", "").strip().rstrip("/")
    if not endpoint:
        raise _AdmissionError(503, "Local caller authentication is unavailable.")
    try:
        response = requests.get(
            endpoint + "/api/v1/users/me/",
            headers={"Authorization": "Bearer " + token.strip()},
            timeout=10,
            allow_redirects=False,
        )
        if response.status_code >= 500:
            raise _AdmissionError(503, "Caller authentication is unavailable.")
        if response.status_code != 200:
            raise _AdmissionError(401, "Authenticated request user could not be resolved.")
        payload = response.json()
        user = RequestUserIdentity(uid=payload["uid"], username=payload.get("username"))
    except requests.RequestException as exc:
        raise _AdmissionError(503, "Caller authentication is unavailable.") from exc
    except (ValueError, TypeError, KeyError) as exc:
        raise _AdmissionError(401, "Authenticated request user could not be resolved.") from exc
    uid_header = _header(scope, b"x-user-uid")
    if uid_header:
        try:
            if str(UUID(uid_header)) != user.uid:
                raise ValueError
        except ValueError as exc:
            raise _AdmissionError(401, "Conflicting authenticated identity.") from exc
    return user


def _websocket_user(scope):
    uid = _header(scope, b"x-user-uid")
    try:
        user = RequestUserIdentity(uid=uid, username=_header(scope, b"x-username"))
    except ValueError as exc:
        raise _AdmissionError(401, "Authenticated WebSocket user could not be resolved.") from exc
    # Preserve the existing gateway/ticket contract without requiring HTTP proof.
    for name in (b"x-resource-release-uid", b"x-organization-environment-uid"):
        _header(scope, name)
    return user


async def _deny(scope, receive, send, error):
    body = json.dumps({"detail": error.detail}).encode()
    if scope["type"] == "websocket":
        message = await receive()
        if message["type"] != "websocket.connect":
            return
        if "websocket.http.response" not in scope.get("extensions", {}):
            await send({"type": "websocket.close", "code": 1013 if error.status == 503 else 1008})
            return
        prefix = "websocket.http.response"
    else:
        prefix = "http.response"
    await send(
        {
            "type": prefix + ".start",
            "status": error.status,
            "headers": [(b"content-type", b"application/json"), (b"cache-control", b"no-store")],
        }
    )
    await send({"type": prefix + ".body", "body": body})


class _RequestIdentityMiddleware:
    def __init__(self, app, *, mode, verifier, public_ingress, routes):
        self.app = app
        self.mode = mode
        self.verifier = verifier
        self.public_ingress = frozenset(public_ingress)
        for method, path in public_ingress:
            if not any(
                getattr(route, "path", None) == path
                and method in (getattr(route, "methods", ()) or ())
                for route in routes
            ):
                raise RuntimeError(f"Public ingress route {method} {path} is not mounted.")

    async def __call__(self, scope, receive, send):
        if scope["type"] not in {"http", "websocket"}:
            await self.app(scope, receive, send)
            return
        state = scope.setdefault("state", {})
        state.update(user=None, user_uid=None, auth_outcome="anonymous")
        with _request_scope() as context:
            try:
                public = (
                    scope["type"] == "http"
                    and (scope.get("method"), scope.get("path")) in self.public_ingress
                    and _header(scope, b"x-public-ingress") == "1"
                )
                preflight = scope["type"] == "http" and scope.get("method") == "OPTIONS"
                if public or preflight:
                    state["auth_outcome"] = "public_ingress" if public else "not_applicable"
                    scope["headers"] = [
                        (k, v)
                        for k, v in scope.get("headers", ())
                        if k.lower() not in _IDENTITY_HEADERS
                    ]
                else:
                    if _header(scope, b"x-user-id") is not None:
                        raise _AdmissionError(
                            403 if scope["type"] == "websocket" else 401,
                            "Numeric request identity is not supported.",
                        )
                    if scope["type"] == "websocket":
                        user = _websocket_user(scope)
                    elif self.mode == "assertion":
                        assertion = _header(scope, ASSERTION_HEADER.lower().encode()) or ""
                        caller = await asyncio.to_thread(self.verifier.verify, assertion)
                        user = RequestUserIdentity(
                            uid=caller.user_uid,
                            team_uids=caller.team_uids,
                            is_organization_admin=caller.is_organization_admin,
                        )
                        state["resource_release_uid"] = caller.release_uid
                        state["organization_environment_uid"] = caller.environment_uid
                        context.caller_assertion = assertion
                        if caller._requester is not None:
                            # Never an admin: the requester's access is member level.
                            context.requester = RequestUserIdentity(
                                uid=caller._requester.user_uid,
                                team_uids=caller._requester.team_uids,
                            )
                    else:
                        user = await asyncio.to_thread(_local_user, scope)
                    context.user = user
                    state.update(user=user, user_uid=user.uid, auth_outcome="authenticated")
                    scope["headers"] = [
                        (k, v)
                        for k, v in scope.get("headers", ())
                        if k.lower() not in {b"authorization", b"x-user-id", b"x-websocket-ticket"}
                    ]
            except (InvalidCallerAssertion, CallerAssertionUnavailable, _AdmissionError) as exc:
                if isinstance(exc, _AdmissionError):
                    error = exc
                elif isinstance(exc, CallerAssertionUnavailable):
                    error = _AdmissionError(503, "Caller authentication is unavailable.")
                else:
                    error = _AdmissionError(401, "Invalid or missing caller assertion.")
                state["auth_outcome"] = "unavailable" if error.status == 503 else "rejected"
                await _deny(scope, receive, send, error)
                return
            await self.app(scope, receive, send)


def install_request_identity(app):
    """Install once at application creation.

    Handlers use User.get_logged_user() for the caller and, on a
    requester-bound call, User.get_requester() for the person it works for.
    """
    if getattr(app.state, _INSTALLATION, None) is not None:
        raise RuntimeError("Request identity is already installed.")
    mode = _mode()
    public_ingress = _public_routes()
    verifier = CallerAssertionVerifier.from_environment() if mode == "assertion" else None
    app.add_middleware(
        _RequestIdentityMiddleware,
        mode=mode,
        verifier=verifier,
        public_ingress=public_ingress,
        routes=app.routes,
    )
    setattr(
        app.state,
        _INSTALLATION,
        {
            "installed": True,
            "mode": mode,
            "public_ingress": public_ingress,
        },
    )


@contextmanager
def reads_as_caller():
    """Read the directory as the request's caller instead of as the application.

    Inside the block, ``User.filter``, ``User.get_by_uid``, ``Team.filter`` and
    ``Team.get_by_uid`` present the caller assertion the request arrived with,
    and the platform answers with what the caller may see. No other call sends
    it. Raises ``RequestIdentityError`` outside an authenticated request or when
    the request carries no assertion (local mode, WebSockets); it never reads as
    the application instead. It also raises in a requester-bound request
    (``User.get_requester()`` is not ``None``): this helper cannot forward a
    requester-bearing assertion to the directory endpoints. Platform-owned
    Agent delegation is separate. See ADR-0036.
    """
    with _reads_as_caller():
        yield


_MCP_INSTALLATION = "mainsequence_mcp"
_MCP_PATH = "/mcp"
_WILDCARD_ORIGIN = re.compile(r"(https?)://\*\.([a-z0-9.-]+)(?::([0-9]{1,5}))?", flags=re.ASCII)


def _mcp_allowed_origins():
    """The release's public origin and its CORS origins, read once at installation."""
    entries = [os.environ.get("FASTAPI_PUBLIC_BASE_URL", "")]
    entries += os.environ.get("FASTAPI_CORS_ALLOW_ORIGINS", "").split(",")
    exact, patterns = set(), []
    for entry in entries:
        entry = entry.strip().rstrip("/").lower()
        wildcard = _WILDCARD_ORIGIN.fullmatch(entry)
        if wildcard:
            scheme, host, port = wildcard.groups()
            pattern = rf"{scheme}://[a-z0-9-]+\.{re.escape(host)}"
            patterns.append(re.compile(pattern + (rf":{port}" if port else ""), flags=re.ASCII))
            continue
        parts = urlsplit(entry)
        if parts.scheme in {"http", "https"} and parts.netloc and "*" not in parts.netloc:
            exact.add(f"{parts.scheme}://{parts.netloc}")
    return frozenset(exact), tuple(patterns)


def _mcp_conflicts(routes, *, installed=None):
    for route in routes:
        path = getattr(route, "path", None)
        if route is not installed and isinstance(path, str):
            if path == _MCP_PATH or path.startswith(_MCP_PATH + "/"):
                raise RuntimeError(f"Route {path} conflicts with the MCP endpoint at {_MCP_PATH}.")


async def _reply(send, status, detail, headers=()):
    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": [
                (b"content-type", b"application/json"),
                (b"cache-control", b"no-store"),
                *headers,
            ],
        }
    )
    await send({"type": "http.response.body", "body": json.dumps({"detail": detail}).encode()})


class _McpEndpoint:
    """Admit authenticated POSTs from an allowed Origin to the application's MCP app."""

    def __init__(self, app, *, allowed_origins):
        self.app = app
        self.exact_origins, self.origin_patterns = allowed_origins

    def _origin_allowed(self, origin):
        origin = origin.strip().rstrip("/").lower()
        return origin in self.exact_origins or any(
            pattern.fullmatch(origin) for pattern in self.origin_patterns
        )

    async def __call__(self, scope, receive, send):
        if scope.get("method") != "POST":
            # Stateless profile: no standalone GET stream and no session to delete.
            await _reply(send, 405, "Method Not Allowed.", headers=[(b"allow", b"POST")])
            return
        origins = [value for key, value in scope.get("headers", ()) if key.lower() == b"origin"]
        if len(origins) > 1 or (origins and not self._origin_allowed(origins[0].decode("latin-1"))):
            await _reply(send, 403, "Origin is not allowed.")
            return
        try:
            _get_request_identity()
        except RequestIdentityError:
            await _reply(send, 401, "Authenticated request user could not be resolved.")
            return
        # Stateless profile: a session identifier never selects server-side state.
        scope = dict(scope)
        scope["headers"] = [
            (key, value)
            for key, value in scope.get("headers", ())
            if key.lower() != b"mcp-session-id"
        ]
        await self.app(scope, receive, send)


def install_mcp(app, mcp_app, *, lifespan):
    """Serve an application-owned MCP server at ``/mcp`` inside request identity.

    ``mcp_app`` is the server's ASGI application, answering Streamable HTTP at
    ``/mcp`` statelessly with JSON responses. ``lifespan`` is a zero-argument
    callable returning its async context manager. With the official MCP Python
    SDK, pass ``mcp.streamable_http_app()`` and ``mcp.session_manager.run``.

    Install request identity first. Tools read the caller with
    ``User.get_logged_user()`` and, on a requester-bound call, the person it
    works for with ``User.get_requester()``. Only authenticated POSTs reach the
    server; other methods answer 405 and an ``Origin`` other than the release's
    own or its CORS origins answers 403. See ADR 0038.
    """
    from starlette.routing import Route

    identity = getattr(app.state, _INSTALLATION, None)
    if not isinstance(identity, dict) or identity.get("installed") is not True:
        raise RuntimeError(
            "Install request identity before MCP: call install_request_identity(app)."
        )
    if getattr(app.state, _MCP_INSTALLATION, None) is not None:
        raise RuntimeError("MCP is already installed.")
    if any(
        path == _MCP_PATH or path.startswith(_MCP_PATH + "/")
        for _, path in identity.get("public_ingress", ())
    ):
        raise RuntimeError(f"{_MCP_PATH} cannot be public ingress; MCP requests are authenticated.")
    if not callable(mcp_app) or not callable(lifespan):
        raise TypeError("install_mcp needs the MCP ASGI app and its lifespan factory.")
    _mcp_conflicts(app.router.routes)

    route = Route(
        _MCP_PATH,
        endpoint=_McpEndpoint(mcp_app, allowed_origins=_mcp_allowed_origins()),
        include_in_schema=False,
    )
    app.router.routes.insert(0, route)
    application_lifespan = app.router.lifespan_context
    started = False

    @asynccontextmanager
    async def composed_lifespan(application):
        nonlocal started
        if started:
            raise RuntimeError("The MCP lifespan runs once per application.")
        started = True
        _mcp_conflicts(app.router.routes, installed=route)
        async with application_lifespan(application) as state:
            async with lifespan():
                yield state

    app.router.lifespan_context = composed_lifespan
    setattr(
        app.state,
        _MCP_INSTALLATION,
        {
            "installed": True,
            "path": _MCP_PATH,
            "transport": "streamable-http",
            "stateless": True,
            "request_identity": True,
        },
    )


__all__ = ["install_mcp", "install_request_identity", "reads_as_caller"]
