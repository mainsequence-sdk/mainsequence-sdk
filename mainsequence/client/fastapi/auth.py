from __future__ import annotations

from starlette.concurrency import run_in_threadpool
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send

from mainsequence.client.exceptions import AuthenticationError, PermissionDeniedError


def _load_auth_bindings():
    from mainsequence.client.models_user import (
        _CURRENT_AUTH_HEADERS,
        _CURRENT_USER,
        RequestIdentityError,
        User,
    )

    return User, RequestIdentityError, _CURRENT_AUTH_HEADERS, _CURRENT_USER


class LoggedUserContextMiddleware:
    """Resolve and bind the human making each authenticated FastAPI request."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request = Request(scope)
        if request.method == "OPTIONS":
            await self.app(scope, receive, send)
            return

        User, RequestIdentityError, current_auth_headers, current_user = (
            _load_auth_bindings()
        )
        headers_token = current_auth_headers.set(request.headers)
        user_token = current_user.set(None)
        resolved_user_token = None
        try:
            try:
                user = await run_in_threadpool(User.get_logged_user)
            except (RequestIdentityError, AuthenticationError, PermissionDeniedError):
                response = JSONResponse(
                    {"detail": "Authenticated request user could not be resolved."},
                    status_code=401,
                )
                await response(scope, receive, send)
                return

            resolved_user_token = current_user.set(user)
            request.state.user = user
            request.state.user_uid = user.uid
            await self.app(scope, receive, send)
        finally:
            if resolved_user_token is not None:
                current_user.reset(resolved_user_token)
            current_user.reset(user_token)
            current_auth_headers.reset(headers_token)


__all__ = ["LoggedUserContextMiddleware"]
