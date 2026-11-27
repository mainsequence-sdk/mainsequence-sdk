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
    # The verified assertion of a hosted HTTP request, kept only so that a
    # directory read with as_caller=True, or a release call with
    # act_for_requester=True, can present it (ADR-0036, amended for #198 and #272).
    caller_assertion: str | None = field(default=None, repr=False)
    # The person a requester-bound request works for, verified from its
    # assertion; None on any other request. The user stays the caller, the
    # acting application (ADR-0036, requester-bound calls).
    requester: Any = None
    # The release and Environment the verifier trusts, from deployment
    # configuration, never from the request. They key membership answers (#225).
    release_uid: str | None = None
    environment_uid: str | None = None
    # in_teams() answers within this request, and the application's cache of
    # answers across requests with its lifetime (ADR-0036, #225).
    team_memberships: dict = field(default_factory=dict, repr=False)
    team_membership_cache: Any = field(default=None, repr=False)
    team_membership_cache_seconds: float = 0
    # Release access the platform gave for this request's caller, per release
    # UID: never kept beyond the request, never in the process cache (#272).
    release_grants: dict = field(default_factory=dict, repr=False)


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
        scope.caller_assertion = None
        scope.requester = None
        scope.team_memberships = {}
        scope.team_membership_cache = None
        scope.release_grants = {}
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


def _team_membership_scope(person_uid: str) -> _RequestScope:
    """The request whose caller or requester ``person_uid`` is, for in_teams()."""
    scope = _authenticated_scope("in_teams()")
    if scope.caller_assertion is None:
        raise RequestIdentityError(
            "This request carries no caller assertion. Team membership checks need a "
            "hosted HTTP request in assertion mode, not local mode or a WebSocket."
        )
    people = {scope.user.uid}
    if scope.requester is not None:
        people.add(scope.requester.uid)
    if person_uid not in people:
        raise RequestIdentityError(
            "in_teams() answers only for the request's caller or its requester."
        )
    return scope


def _get_requester():
    """The requester of the current request; None when it is not requester-bound."""
    return _authenticated_scope("User.get_requester()").requester


def _origin(url: str) -> tuple[str, str | None, int | None]:
    parts = urlsplit(url)
    scheme = parts.scheme.lower()
    return scheme, parts.hostname, parts.port or {"http": 80, "https": 443}.get(scheme)


def _assertion_scope(option: str) -> _RequestScope:
    """The request scope whose caller assertion ``option`` presents.

    Raises ``RequestIdentityError`` before anything is sent when there is none
    to present; it never lets the call go out as the application instead.
    """
    scope = _current_scope.get()
    if scope is not None and not scope.active:
        raise RequestIdentityError(
            "The request this call belongs to has ended; its caller assertion is no "
            "longer available."
        )
    if scope is None or scope.user is None:
        raise RequestIdentityError(
            f"{option} needs an authenticated request. Install request identity once "
            f"in the application and pass {option} inside a request."
        )
    if scope.requester is not None:
        raise RequestIdentityError(
            f"This request is requester-bound. {option} cannot forward a "
            "requester-bearing assertion to the platform."
        )
    if not scope.caller_assertion:
        raise RequestIdentityError(
            f"This request carries no caller assertion. {option} needs a hosted HTTP "
            "request in assertion mode, not local mode or a WebSocket."
        )
    return scope


def _assertion_header(
    url: str, endpoint_url: str, *, option: str, scope: _RequestScope | None = None
) -> dict[str, str]:
    """The caller assertion header for one platform request to ``url``.

    ``scope`` is the request the call was bound to when it started, for a read
    that sends several requests; by default it is the current request.
    """
    if scope is None:
        scope = _assertion_scope(option)
    elif not scope.active:
        raise RequestIdentityError(
            "The request this call belongs to has ended; its caller assertion is no "
            "longer available."
        )
    if _origin(url) != _origin(endpoint_url):
        raise RequestIdentityError(
            "The caller assertion is sent only to the configured platform endpoint."
        )
    return {CALLER_ASSERTION_HEADER: scope.caller_assertion}
