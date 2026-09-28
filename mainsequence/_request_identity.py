"""Private lifecycle shared by the request integration and the public user getter."""

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any


class RequestIdentityError(RuntimeError):
    """No authenticated caller is bound to the current request."""


@dataclass
class _RequestScope:
    user: Any = None
    active: bool = True


_current_scope: ContextVar[_RequestScope | None] = ContextVar(
    "mainsequence_request_identity", default=None
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
        _current_scope.reset(token)


def _get_request_identity():
    scope = _current_scope.get()
    if scope is None or not scope.active or scope.user is None:
        raise RequestIdentityError(
            "No authenticated request user is available. Install request identity "
            "once in the application and call User.get_logged_user() inside a request."
        )
    return scope.user
