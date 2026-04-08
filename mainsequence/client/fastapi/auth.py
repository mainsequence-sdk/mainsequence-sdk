from __future__ import annotations

from starlette.concurrency import run_in_threadpool
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send


def _load_auth_bindings():
    from mainsequence.client.models_user import _CURRENT_AUTH_HEADERS, User

    return User, _CURRENT_AUTH_HEADERS


class AuthenticatedUserMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request = Request(scope)
        User, current_auth_headers = _load_auth_bindings()
        headers_token = current_auth_headers.set(request.headers)

        try:
            user = await run_in_threadpool(User.get_logged_user)
            if user is None:
                raise RuntimeError("User is not authenticated.")

            request.state.user = user
            request.state.user_id = user.id
        except Exception:
            current_auth_headers.reset(headers_token)
            response = JSONResponse(
                {"detail": "User is not authenticated."},
                status_code=401,
            )
            await response(scope, receive, send)
            return

        try:
            await self.app(scope, receive, send)
        finally:
            current_auth_headers.reset(headers_token)


__all__ = ["AuthenticatedUserMiddleware"]
