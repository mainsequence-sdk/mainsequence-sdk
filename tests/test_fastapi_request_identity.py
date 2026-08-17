from __future__ import annotations

import asyncio
import json

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from mainsequence.client.fastapi import LoggedUserContextMiddleware
from mainsequence.client.models_user import _CURRENT_AUTH_HEADERS, _CURRENT_USER

USER_UID = "8f5d6b54-2f5e-4a8b-bb10-0b17f3f4c123"


def _request(app: Starlette, *, method: str = "GET", headers=None):
    messages: list[dict] = []
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "path": "/identity",
        "raw_path": b"/identity",
        "query_string": b"",
        "headers": [
            (str(key).lower().encode(), str(value).encode())
            for key, value in (headers or {}).items()
        ],
        "client": ("testclient", 50000),
        "server": ("testserver", 80),
    }

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        messages.append(message)

    asyncio.run(app(scope, receive, send))
    status = next(message["status"] for message in messages if message["type"] == "http.response.start")
    body = b"".join(
        message.get("body", b"")
        for message in messages
        if message["type"] == "http.response.body"
    )
    return status, json.loads(body) if body else None


def _build_app(route_calls: list[str]) -> Starlette:
    async def identity(request: Request) -> JSONResponse:
        route_calls.append("identity")
        return JSONResponse(
            {
                "uid": request.state.user.uid,
                "state_uid": request.state.user_uid,
                "has_numeric_id": hasattr(request.state, "user_id"),
            }
        )

    async def preflight(request: Request) -> Response:
        route_calls.append("preflight")
        return Response(status_code=204)

    app = Starlette(
        routes=[
            Route("/identity", identity),
            Route("/identity", preflight, methods=["OPTIONS"]),
        ]
    )
    app.add_middleware(LoggedUserContextMiddleware)
    return app


def test_middleware_exposes_uid_only_request_state_and_resets_context():
    route_calls: list[str] = []
    status, body = _request(
        _build_app(route_calls),
        headers={"X-User-UID": USER_UID, "X-Username": "jose"},
    )

    assert status == 200
    assert body == {
        "uid": USER_UID,
        "state_uid": USER_UID,
        "has_numeric_id": False,
    }
    assert route_calls == ["identity"]
    assert _CURRENT_AUTH_HEADERS.get() is None
    assert _CURRENT_USER.get() is None


def test_middleware_returns_401_without_running_route():
    route_calls: list[str] = []
    status, _ = _request(_build_app(route_calls))

    assert status == 401
    assert route_calls == []


def test_middleware_allows_unauthenticated_options_preflight():
    route_calls: list[str] = []
    status, _ = _request(_build_app(route_calls), method="OPTIONS")

    assert status == 204
    assert route_calls == ["preflight"]
