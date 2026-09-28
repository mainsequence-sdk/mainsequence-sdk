"""Application-installed request identity; independent of the platform launcher."""

from __future__ import annotations

import asyncio
import json
import os
import re
from uuid import UUID

import requests

from mainsequence._request_identity import _request_scope
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
                        user = RequestUserIdentity(uid=caller.user_uid)
                        state["resource_release_uid"] = caller.release_uid
                        state["organization_environment_uid"] = caller.environment_uid
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
    """Install once at application creation. Handlers use User.get_logged_user()."""
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


__all__ = ["install_request_identity"]
