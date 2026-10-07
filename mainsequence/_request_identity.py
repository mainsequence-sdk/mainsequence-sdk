"""Private lifecycle shared by the request integration and the public user getter."""

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit

CALLER_ASSERTION_HEADER = "X-MainSequence-Caller-Assertion"


class RequestIdentityError(RuntimeError):
    """No authenticated caller is bound to the current request."""


@dataclass
class _RequestScope:
    user: Any = None
    active: bool = True
    # The verified assertion of a hosted HTTP request, kept only so that
    # reads_as_caller() can present it (ADR-0036, amended for #198).
    caller_assertion: str | None = field(default=None, repr=False)
    # The person a requester-bound request works for, verified from its
    # assertion; None on any other request. The user stays the caller, the
    # acting application (ADR-0036, requester-bound calls).
    requester: Any = None


_current_scope: ContextVar[_RequestScope | None] = ContextVar(
    "mainsequence_request_identity", default=None
)
# The request scope whose caller the directory reads answer as, inside reads_as_caller().
_caller_reads: ContextVar[_RequestScope | None] = ContextVar(
    "mainsequence_caller_reads", default=None
)


@contextmanager
def _request_scope():
    scope = _RequestScope()
    token = _current_scope.set(scope)
    try:
        yield scope
    finally:
        # Copied child-task contexts hold this same frame and must also expire.
        scope.active = False
        scope.user = None
        scope.caller_assertion = None
        scope.requester = None
        _current_scope.reset(token)


def _authenticated_scope(getter: str) -> _RequestScope:
    scope = _current_scope.get()
    if scope is None or not scope.active or scope.user is None:
        raise RequestIdentityError(
            "No authenticated request user is available. Install request identity "
            f"once in the application and call {getter} inside a request."
        )
    return scope


def _get_request_identity():
    return _authenticated_scope("User.get_logged_user()").user


def _get_requester():
    """The requester of the current request; None when it is not requester-bound."""
    return _authenticated_scope("User.get_requester()").requester


@contextmanager
def _reads_as_caller():
    scope = _current_scope.get()
    if scope is None or not scope.active or scope.user is None:
        raise RequestIdentityError(
            "Reading as the caller needs an authenticated request. Install request "
            "identity once in the application and call reads_as_caller() inside a request."
        )
    if scope.requester is not None:
        raise RequestIdentityError(
            "This request is requester-bound. reads_as_caller() cannot forward a "
            "requester-bearing assertion to the directory endpoints."
        )
    if scope.caller_assertion is None:
        raise RequestIdentityError(
            "This request carries no caller assertion. Reading as the caller needs a "
            "hosted HTTP request in assertion mode, not local mode or a WebSocket."
        )
    token = _caller_reads.set(scope)
    try:
        yield
    finally:
        _caller_reads.reset(token)


def _origin(url: str) -> tuple[str, str | None, int | None]:
    parts = urlsplit(url)
    scheme = parts.scheme.lower()
    return scheme, parts.hostname, parts.port or {"http": 80, "https": 443}.get(scheme)


def _caller_read_headers(url: str, endpoint_url: str) -> dict[str, str]:
    """The header a directory read sends to ``url``: none outside reads_as_caller()."""
    scope = _caller_reads.get()
    if scope is None:
        return {}
    if not scope.active or scope.caller_assertion is None:
        raise RequestIdentityError(
            "The request that reads_as_caller() belongs to has ended; its caller "
            "assertion is no longer available."
        )
    if _origin(url) != _origin(endpoint_url):
        raise RequestIdentityError(
            "The caller assertion is sent only to the configured platform endpoint."
        )
    return {CALLER_ASSERTION_HEADER: scope.caller_assertion}
