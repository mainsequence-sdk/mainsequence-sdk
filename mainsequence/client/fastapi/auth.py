from __future__ import annotations

from starlette.concurrency import run_in_threadpool
from starlette.requests import Request
from starlette.types import ASGIApp, Receive, Scope, Send

from mainsequence.logconf import logger


def _load_auth_bindings():
    from mainsequence.client.models_user import _CURRENT_AUTH_HEADERS, User

    return User, _CURRENT_AUTH_HEADERS


def _header_keys(headers) -> list[str]:
    if headers is None:
        return []
    try:
        return sorted(str(key) for key in headers.keys())
    except Exception:
        return []


def _header_get(headers, key: str):
    if headers is None:
        return None
    getter = getattr(headers, "get", None)
    if callable(getter):
        value = getter(key)
        if value is not None:
            return value
        if key.lower() != key:
            return getter(key.lower())
        return value
    return None


def _authorization_scheme(headers) -> str | None:
    authorization = _header_get(headers, "authorization")
    if not authorization:
        return None
    return str(authorization).split(" ", 1)[0]


class LoggedUserContextMiddleware:
    """Bind request headers into the client auth context and populate request.state."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request = Request(scope)
        User, current_auth_headers = _load_auth_bindings()
        headers_token = current_auth_headers.set(request.headers)
        authorization_scheme = _authorization_scheme(request.headers)
        logger.info(
            "LoggedUserContextMiddleware request context method=%s path=%s "
            "x-user-id=%r x-username=%r x-resource-release-id=%r x-fastapi-id=%r "
            "authorization_present=%s authorization_scheme=%r",
            request.method,
            request.url.path,
            request.headers.get("x-user-id"),
            request.headers.get("x-username"),
            request.headers.get("x-resource-release-id"),
            request.headers.get("x-fastapi-id"),
            request.headers.get("authorization") is not None,
            authorization_scheme,
        )
        bound_headers = current_auth_headers.get()
        logger.info(
            "LoggedUserContextMiddleware bound context current_auth_headers_is_none=%s "
            "header_keys=%s x-user-id=%r",
            bound_headers is None,
            _header_keys(bound_headers),
            _header_get(bound_headers, "x-user-id"),
        )

        try:
            try:
                user = await run_in_threadpool(User.get_logged_user)
            except Exception as exc:
                logger.exception(
                    "LoggedUserContextMiddleware User.get_logged_user failed for %s %s: %s",
                    request.method,
                    request.url.path,
                    exc,
                )
                raise

            request.state.user = user
            request.state.user_id = user.id
            logger.info(
                "LoggedUserContextMiddleware User.get_logged_user resolved user_id=%s for %s %s",
                request.state.user_id,
                request.method,
                request.url.path,
            )

            await self.app(scope, receive, send)
        finally:
            current_auth_headers.reset(headers_token)

__all__ = ["LoggedUserContextMiddleware"]
