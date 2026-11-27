"""Calls from one release to another that tell a cold start from a hang (ADR 0041).

Readiness is asked of the platform, never guessed. A call waits for the target
under a readiness limit and only then starts the request's own timeout. A
process cache skips the platform call while the target is known warm.
"""

from __future__ import annotations

import asyncio
import contextvars
import datetime
import math
import os
import threading
import time
from collections.abc import Iterable, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Literal

import requests

from mainsequence._request_deadline import (
    DEADLINE_HEADER,
    format_timeout_ms,
    remaining_request_seconds,
)
from mainsequence._request_identity import _assertion_scope
from mainsequence._transport import request_budget, request_within_budget
from mainsequence.instrumentation.utils import DependencyCall
from mainsequence.logconf import logger

from .exceptions import ApiError, TransportError
from .utils import _decode_jwt_exp

READY_TIMEOUT_ENV = "MAINSEQUENCE_RELEASE_READY_TIMEOUT_SECONDS"
IDLE_TIMEOUT_ENV = "MAINSEQUENCE_RELEASE_IDLE_TIMEOUT_SECONDS"
DEFAULT_READY_TIMEOUT_SECONDS = 300.0
MIN_READY_TIMEOUT_SECONDS = 30.0
# Used until the platform states a release's own idle window.
DEFAULT_IDLE_TIMEOUT_SECONDS = 60.0
MIN_IDLE_MARGIN_SECONDS = 10.0
IDLE_MARGIN_FRACTION = 0.2
DEFAULT_RETRY_SECONDS = 2.0
MIN_RETRY_SECONDS = 0.1
DEFAULT_ACCESS_TTL_SECONDS = 60.0
ACCESS_EXPIRY_SKEW_SECONDS = 30.0
PLATFORM_TIMEOUT_SECONDS = 10.0
MAX_FAILED_PLATFORM_ASKS = 3

PROCESS_STARTED_AT_HEADER = "Mainsequence-Process-Started-At"
COLD_START_HEADER = "Mainsequence-Cold-Start"

_NOT_SERVING_PHASES = frozenset({"idle", "provisioning", "pulling_image", "starting"})
_RESERVED_HEADERS = frozenset({"authorization", DEADLINE_HEADER.lower()})


class ReleaseCallError(ApiError):
    """A call to a release failed before or without an answer from the application.

    ``phase`` is ``"readiness"`` when the release never became ready and
    ``"request"`` when the request was sent.
    """

    phase: str = "request"

    def __init__(
        self,
        message: str,
        *,
        resource_release_uid: str,
        timeout_seconds: float | None = None,
        runtime_state: str | None = None,
        runtime_phase: str | None = None,
        wake_requested_at: datetime.datetime | None = None,
        expected_cold_start_seconds: float | None = None,
        remaining_seconds: float | None = None,
    ):
        super().__init__(message)
        self.resource_release_uid = resource_release_uid
        self.timeout_seconds = timeout_seconds
        self.runtime_state = runtime_state
        self.runtime_phase = runtime_phase
        self.wake_requested_at = wake_requested_at
        self.expected_cold_start_seconds = expected_cold_start_seconds
        self.remaining_seconds = remaining_seconds


class ReleaseReadinessTimeoutError(ReleaseCallError, TimeoutError):
    """The readiness limit ended while the platform still asked to try again later."""

    phase = "readiness"


class ReleaseUnavailableError(ReleaseCallError, RuntimeError):
    """The platform answered that the release cannot take requests."""

    phase = "readiness"


class ReleaseNotDeployedError(ReleaseUnavailableError):
    """The release has no active revision: it was never deployed, or awaits its first rollout."""


class ReleaseRequesterError(ReleaseCallError, PermissionError):
    """A call for the request's person could not be made; nothing was sent.

    ``expired`` is ``True`` when the person's assertion ran out before the
    release could be called, and ``False`` when the platform refused to give
    access for the person (``requester_binding_invalid``). The call is never
    made as the application instead.
    """

    phase = "readiness"

    def __init__(self, message: str, *, expired: bool = False, **fields: Any):
        super().__init__(message, **fields)
        self.expired = expired


class ReleaseDeadlineTooShortError(ReleaseCallError, TimeoutError):
    """The caller's remaining time cannot cover the release's start; nothing was sent."""

    phase = "readiness"


class ReleaseColdStartTimeoutError(ReleaseCallError, TimeoutError):
    """The request timed out while the release was starting, not while it worked."""


class ReleaseRequestTimeoutError(ReleaseCallError, TimeoutError):
    """The request timed out on a serving release: the application was slow or hung."""


@dataclass(frozen=True)
class ReleaseRuntimeStatus:
    """What this process knows about a release, without asking the platform.

    ``state`` is ``"warm"`` when a request sent now is expected to be served at
    once, ``"cold"`` when the platform last answered that the release was not
    serving, and ``"unknown"`` otherwise. The scaling fields are ``None`` until
    the platform states them.
    """

    resource_release_uid: str
    state: Literal["warm", "cold", "unknown"]
    min_instances: int | None = None
    idle_timeout_seconds: float | None = None
    expected_cold_start_seconds: float | None = None
    runtime_phase: str | None = None
    observed_at: datetime.datetime | None = None
    last_response_at: datetime.datetime | None = None
    last_cold_start_at: datetime.datetime | None = None
    last_cold_start_seconds: float | None = None
    process_started_at: datetime.datetime | None = None


@dataclass
class _Grant:
    rpc_url: str
    token: str = field(repr=False)
    expires_at: float


@dataclass
class _Entry:
    grant: _Grant | None = None
    scaling: Any = None
    can_request: bool | None = None
    runtime_phase: str | None = None
    observed: float | None = None
    observed_at: datetime.datetime | None = None
    # Monotonic time of the last sign that the release was serving.
    served: float | None = None
    last_response_at: datetime.datetime | None = None
    last_cold_start_at: datetime.datetime | None = None
    last_cold_start_seconds: float | None = None
    process_started_at: datetime.datetime | None = None


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.UTC)


def _positive_seconds(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    seconds = float(value)
    return seconds if math.isfinite(seconds) and seconds > 0 else None


def _environment_seconds(name: str, default: float) -> float:
    try:
        return _positive_seconds(float(os.environ[name])) or default
    except (KeyError, ValueError):
        return default


def _parse_datetime(value: Any) -> datetime.datetime | None:
    if isinstance(value, datetime.datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        try:
            parsed = datetime.datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=datetime.UTC)


def _grant_from_access(access: Any) -> _Grant | None:
    """The token access of a platform answer, or ``None`` when it offers none."""
    grant = getattr(access, "access", None)
    if not isinstance(grant, Mapping) or grant.get("mode") != "token":
        return None
    token, rpc_url = grant.get("token"), grant.get("rpc_url")
    if not isinstance(token, str) or not token.strip() or not isinstance(rpc_url, str):
        return None
    if not rpc_url.startswith(("https://", "http://")):
        return None
    expires = _parse_datetime(grant.get("expires_at"))
    expires_at = expires.timestamp() if expires is not None else _decode_jwt_exp(token)
    if expires_at is None:
        expires_at = time.time() + DEFAULT_ACCESS_TTL_SECONDS + ACCESS_EXPIRY_SKEW_SECONDS
    return _Grant(rpc_url=rpc_url, token=token.strip(), expires_at=float(expires_at))


def _scaling_seconds(scaling: Any, name: str) -> float | None:
    return _positive_seconds(getattr(scaling, name, None))


class ReleaseRuntimeCache:
    """One process's view of the releases it calls. In memory, never persisted.

    It is an optimisation, not the truth: another caller may keep a release
    warm, and a deployment may restart it. A wrong "warm" costs one held
    request, which the call routine then classifies.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._entries: dict[str, _Entry] = {}
        self._starting: dict[str, threading.Lock] = {}

    def _entry(self, uid: str) -> _Entry:
        return self._entries.setdefault(uid, _Entry())

    @contextmanager
    def starting(self, uid: str, seconds: float):
        """Let one caller wait for a release while the others wait for that caller.

        Holds the release's turn for at most ``seconds``; a caller that does
        not get it goes on by itself, so no call is held longer than its own
        limits.
        """
        with self._lock:
            turn = self._starting.setdefault(uid, threading.Lock())
        held = turn.acquire(timeout=max(0.0, seconds))
        try:
            yield
        finally:
            if held:
                turn.release()

    def observe(self, uid: str, access: Any, *, keep_grant: bool = True) -> _Grant | None:
        """Record a platform answer; returns its token access when it can be called.

        With ``keep_grant=False`` the answer's token, given for a request's
        person, is returned but never kept as the release's access; the facts
        about the release (presence, scaling, whether it serves) still are.
        """
        admission = getattr(access, "runtime_access", None)
        presence = getattr(access, "runtime_presence", None)
        can_request = bool(getattr(admission, "can_request", False))
        grant = _grant_from_access(access) if can_request else None
        scaling = getattr(access, "scaling", None)
        with self._lock:
            entry = self._entry(uid)
            entry.can_request = can_request
            entry.runtime_phase = getattr(presence, "phase", None)
            entry.observed = time.monotonic()
            entry.observed_at = _utcnow()
            if scaling is not None:
                entry.scaling = scaling
            if keep_grant or not can_request:
                entry.grant = grant
            # The platform's answer is the newest sign either way.
            entry.served = entry.observed if grant is not None else None
        return grant

    def record_response(self, uid: str, headers: Mapping[str, str] | None = None) -> None:
        """Record that the release's application answered a request."""
        headers = headers or {}
        started_at = _parse_datetime(headers.get(PROCESS_STARTED_AT_HEADER))
        with self._lock:
            entry = self._entry(uid)
            entry.served = time.monotonic()
            entry.last_response_at = _utcnow()
            if started_at is not None:
                entry.process_started_at = started_at

    def record_cold_start(self, uid: str, started_at: datetime.datetime, seconds: float) -> None:
        with self._lock:
            entry = self._entry(uid)
            entry.last_cold_start_at = started_at
            entry.last_cold_start_seconds = seconds

    def invalidate(self, uid: str) -> None:
        """Forget that the release is warm; its facts and history are kept."""
        with self._lock:
            entry = self._entries.get(uid)
            if entry is not None:
                entry.grant = None
                entry.served = None

    def forget(self, uid: str) -> None:
        with self._lock:
            self._entries.pop(uid, None)

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()

    @staticmethod
    def _recently_served(entry: _Entry) -> bool:
        min_instances = getattr(entry.scaling, "min_instances", None)
        if isinstance(min_instances, int) and min_instances >= 1 and entry.served is not None:
            return True
        if entry.served is None:
            return False
        idle = _scaling_seconds(entry.scaling, "idle_timeout_seconds") or _environment_seconds(
            IDLE_TIMEOUT_ENV, DEFAULT_IDLE_TIMEOUT_SECONDS
        )
        margin = max(MIN_IDLE_MARGIN_SECONDS, IDLE_MARGIN_FRACTION * idle)
        return time.monotonic() - entry.served < idle - margin

    def recently_served(self, uid: str) -> bool:
        """Whether a request sent now is expected to be served at once."""
        with self._lock:
            entry = self._entries.get(uid)
            return entry is not None and self._recently_served(entry)

    def warm_access(self, uid: str) -> _Grant | None:
        """Token access to use without asking the platform, or ``None``."""
        with self._lock:
            entry = self._entries.get(uid)
            if entry is None or entry.grant is None:
                return None
            if time.time() >= entry.grant.expires_at - ACCESS_EXPIRY_SKEW_SECONDS:
                return None
            return entry.grant if self._recently_served(entry) else None

    def status(self, uid: str) -> ReleaseRuntimeStatus:
        with self._lock:
            entry = self._entries.get(uid)
            if entry is None:
                return ReleaseRuntimeStatus(resource_release_uid=uid, state="unknown")
            if self._recently_served(entry):
                state = "warm"
            elif entry.can_request is False and entry.served is None:
                state = "cold"
            else:
                state = "unknown"
            min_instances = getattr(entry.scaling, "min_instances", None)
            return ReleaseRuntimeStatus(
                resource_release_uid=uid,
                state=state,
                min_instances=min_instances if isinstance(min_instances, int) else None,
                idle_timeout_seconds=_scaling_seconds(entry.scaling, "idle_timeout_seconds"),
                expected_cold_start_seconds=_scaling_seconds(
                    entry.scaling, "expected_cold_start_seconds"
                ),
                runtime_phase=entry.runtime_phase,
                observed_at=entry.observed_at,
                last_response_at=entry.last_response_at,
                last_cold_start_at=entry.last_cold_start_at,
                last_cold_start_seconds=entry.last_cold_start_seconds,
                process_started_at=entry.process_started_at,
            )


release_runtime_cache = ReleaseRuntimeCache()


@dataclass
class _Delegation:
    """A release call made for the person whose request the application serves (#272).

    The person's access lives in the request's own scope, per release, and
    ends with the request or the token, whichever is first. It is never the
    release's kept access in ``release_runtime_cache``.
    """

    scope: Any
    expires_at: float | None

    @classmethod
    def current(cls) -> _Delegation:
        scope = _assertion_scope("act_for_requester=True")
        return cls(scope=scope, expires_at=_decode_jwt_exp(scope.caller_assertion))

    def kept(self, uid: str, cache: ReleaseRuntimeCache) -> _Grant | None:
        grant = self.scope.release_grants.get(uid) if self.scope.active else None
        if grant is None or time.time() >= grant.expires_at - ACCESS_EXPIRY_SKEW_SECONDS:
            return None
        return grant if cache.recently_served(uid) else None

    def keep(self, uid: str, grant: _Grant) -> None:
        if self.scope.active:
            self.scope.release_grants[uid] = grant

    def drop(self, uid: str) -> None:
        self.scope.release_grants.pop(uid, None)

    def resolve(self, release: Any) -> Any:
        return lambda seconds: release.resolve_runtime_access(
            timeout=seconds, act_for_requester=True
        )


def _response_code(response: Any) -> str | None:
    """The ``code`` of a JSON error body, or ``None``."""
    try:
        body = response.json()
    except Exception:
        return None
    code = body.get("code") if isinstance(body, Mapping) else None
    return code if isinstance(code, str) else None


def _requester_expired(uid: str, access: Any = None) -> ReleaseRequesterError:
    presence = getattr(access, "runtime_presence", None)
    admission = getattr(access, "runtime_access", None)
    return ReleaseRequesterError(
        f"The assertion of the person this request serves expires before release {uid} "
        "can be called; nothing was sent, and the call cannot continue for that person.",
        expired=True,
        resource_release_uid=uid,
        runtime_state=str(admission.state) if admission is not None else None,
        runtime_phase=getattr(presence, "phase", None),
    )


def _release_uid(release: Any) -> str:
    uid = getattr(release, "uid", None)
    if uid in (None, ""):
        raise ValueError("ResourceRelease.uid is required to call the release")
    return str(uid)


def _ready_limit(ready_timeout: float | None, expected_cold_start: float | None) -> float:
    if ready_timeout is not None:
        return ready_timeout
    if expected_cold_start is not None:
        return max(MIN_READY_TIMEOUT_SECONDS, 2 * expected_cold_start)
    return _environment_seconds(READY_TIMEOUT_ENV, DEFAULT_READY_TIMEOUT_SECONDS)


def _retry_hint_seconds(admission: Any) -> float | None:
    """The platform's ``retry_after_ms`` in seconds, or ``None`` when it gives none."""
    retry_after_ms = getattr(admission, "retry_after_ms", None)
    if isinstance(retry_after_ms, bool) or not isinstance(retry_after_ms, (int, float)):
        return None
    return max(MIN_RETRY_SECONDS, retry_after_ms / 1000.0)


def _unavailable_error(uid: str, access: Any) -> ReleaseUnavailableError:
    """The error for an answer that cannot take requests and asks for no retry."""
    admission = access.runtime_access
    presence = access.runtime_presence
    fields = {
        "resource_release_uid": uid,
        "runtime_state": str(admission.state),
        "runtime_phase": getattr(presence, "phase", None),
    }
    routing = getattr(access, "routing", None)
    if routing is not None and getattr(routing, "active_revision_uid", None) is None:
        return ReleaseNotDeployedError(
            f"Release {uid} has no active revision: it has not been deployed yet.", **fields
        )
    notice = getattr(admission, "notice", None)
    detail = getattr(notice, "message", None) or getattr(presence, "detail", None)
    return ReleaseUnavailableError(detail or f"Release {uid} cannot take requests.", **fields)


def _deadline_too_short(
    uid: str,
    remaining: float,
    *,
    expected_cold_start: float | None = None,
    phase: str | None = None,
) -> ReleaseDeadlineTooShortError:
    remaining = max(0.0, remaining)
    message = f"The caller will wait {remaining:.1f}s more, too short to call release {uid}"
    if expected_cold_start is not None:
        message += f", which is starting and is expected to take {expected_cold_start:.0f}s"
    return ReleaseDeadlineTooShortError(
        message + ".",
        resource_release_uid=uid,
        remaining_seconds=remaining,
        expected_cold_start_seconds=expected_cold_start,
        runtime_phase=phase,
    )


def _wait_until_ready(
    release: Any,
    *,
    ready_timeout: float | None,
    cache: ReleaseRuntimeCache,
    delegation: _Delegation | None = None,
) -> _Grant:
    """Ask the platform, which wakes the release, until it can be called with a token."""
    if delegation is None:
        access, grant = wait_for_release(release, ready_timeout=ready_timeout, cache=cache)
    else:
        access, grant = wait_for_release(
            release,
            ready_timeout=ready_timeout,
            cache=cache,
            resolve=delegation.resolve(release),
            keep_grant=False,
            expires_at=delegation.expires_at,
        )
    if grant is None:
        raise ReleaseUnavailableError(
            f"Release {_release_uid(release)} offers no token access to call it.",
            resource_release_uid=_release_uid(release),
            runtime_state=str(access.runtime_access.state),
            runtime_phase=getattr(access.runtime_presence, "phase", None),
        )
    if delegation is not None:
        delegation.keep(_release_uid(release), grant)
    return grant


def wait_for_release(
    release: Any,
    *,
    ready_timeout: float | None,
    cache: ReleaseRuntimeCache | None,
    resolve: Any = None,
    keep_grant: bool = True,
    expires_at: float | None = None,
) -> tuple[Any, _Grant | None]:
    """Ask the platform, which wakes the release, until it admits requests.

    The platform's ``retry_after_ms`` alone decides whether to wait: an answer
    that cannot take requests and carries it is asked again after that long,
    whatever its state, phase or notice, and one without it ends the wait. An
    ask that fails with a ``TransportError`` is
    made again, up to ``MAX_FAILED_PLATFORM_ASKS`` in a row. Every wait ends
    at the readiness limit and at the caller's deadline.

    Returns the platform's answer and its token access, when it offers one.
    ``resolve`` asks the platform, given a timeout in seconds; by default it is
    the release's ``resolve_runtime_access``. ``cache`` records what is learned;
    ``None`` records nothing. ``keep_grant=False`` records it without keeping
    the token. ``expires_at`` (epoch seconds) ends the wait with
    ``ReleaseRequesterError`` when the next ask would come after it: the
    expiry of the person's assertion a delegated call presents.
    """
    uid = _release_uid(release)
    if resolve is None:
        resolve = lambda seconds: release.resolve_runtime_access(timeout=seconds)  # noqa: E731
    started = time.monotonic()
    limit = _ready_limit(ready_timeout, None)
    waking_since: float | None = None
    waking_at: datetime.datetime | None = None
    expected: float | None = None
    waited_on: tuple[str, str | None] | None = None
    failed_asks = 0
    while True:
        if expires_at is not None and time.time() >= expires_at:
            raise _requester_expired(uid)
        caller_remaining = remaining_request_seconds()
        if caller_remaining is not None and caller_remaining <= 0:
            raise _deadline_too_short(uid, caller_remaining)
        platform_timeout = PLATFORM_TIMEOUT_SECONDS
        if caller_remaining is not None:
            platform_timeout = min(platform_timeout, caller_remaining)
        try:
            access = resolve(platform_timeout)
        except TransportError as error:
            failed_asks += 1
            remaining = _wait_remaining(started, limit)
            if failed_asks >= MAX_FAILED_PLATFORM_ASKS or remaining <= 0:
                raise
            logger.warning(
                "release.call.platform_ask_failed",
                resource_release_uid=uid,
                failed_asks=failed_asks,
                error=str(error),
            )
            time.sleep(min(remaining, DEFAULT_RETRY_SECONDS))
            continue
        failed_asks = 0
        if cache is not None:
            grant = cache.observe(uid, access, keep_grant=keep_grant)
        elif access.runtime_access.can_request:
            grant = _grant_from_access(access)
        else:
            grant = None
        admission = access.runtime_access
        presence = access.runtime_presence
        state = str(admission.state)
        phase = getattr(presence, "phase", None)
        if admission.can_request:
            if waking_since is not None:
                seconds = time.monotonic() - waking_since
                if cache is not None:
                    cache.record_cold_start(uid, waking_at, seconds)
                logger.info(
                    "release.call.cold_start",
                    resource_release_uid=uid,
                    cold_start_seconds=round(seconds, 3),
                    wake_requested_at=waking_at.isoformat(),
                )
            return access, grant
        retry = _retry_hint_seconds(admission)
        if retry is None:
            raise _unavailable_error(uid, access)

        if state == "waking" and waking_since is None:
            waking_since = time.monotonic()
            wake = getattr(presence, "wake", None)
            waking_at = _parse_datetime(getattr(wake, "requested_at", None)) or _utcnow()
            expected = _scaling_seconds(
                getattr(access, "scaling", None), "expected_cold_start_seconds"
            )
            limit = _ready_limit(ready_timeout, expected)
            caller_remaining = remaining_request_seconds()
            if (
                caller_remaining is not None
                and expected is not None
                and caller_remaining < expected
            ):
                raise _deadline_too_short(
                    uid, caller_remaining, expected_cold_start=expected, phase=phase
                )
        if waited_on != (state, phase):
            waited_on = (state, phase)
            notice = getattr(admission, "notice", None)
            logger.info(
                "release.call.waiting",
                resource_release_uid=uid,
                runtime_state=state,
                runtime_phase=phase,
                notice_code=getattr(notice, "code", None),
                ready_timeout_seconds=limit,
            )
        remaining = started + limit - time.monotonic()
        caller_remaining = remaining_request_seconds()
        if caller_remaining is not None and caller_remaining < remaining:
            if caller_remaining <= 0:
                raise _deadline_too_short(
                    uid, caller_remaining, expected_cold_start=expected, phase=phase
                )
            remaining = caller_remaining
        if remaining <= 0:
            raise _not_ready_in_time(uid, access, limit, waking_at, expected)
        pause = min(remaining, retry)
        if expires_at is not None and time.time() + pause >= expires_at:
            raise _requester_expired(uid, access)
        time.sleep(pause)


def _wait_remaining(started: float, limit: float) -> float:
    """Seconds left of the readiness limit, or of the caller's time when shorter."""
    remaining = started + limit - time.monotonic()
    caller_remaining = remaining_request_seconds()
    if caller_remaining is not None:
        remaining = min(remaining, caller_remaining)
    return remaining


def _not_ready_in_time(
    uid: str,
    access: Any,
    limit: float,
    waking_at: datetime.datetime | None,
    expected: float | None,
) -> ReleaseReadinessTimeoutError:
    admission = access.runtime_access
    state = str(admission.state)
    phase = getattr(access.runtime_presence, "phase", None)
    if state == "waking":
        message = f"Release {uid} was still starting after {limit:.0f}s" + (
            f" (phase: {phase})." if phase else "."
        )
    else:
        notice = getattr(admission, "notice", None)
        detail = getattr(notice, "message", None)
        message = f"Release {uid} still could not take requests after {limit:.0f}s" + (
            f": {detail}" if detail else "."
        )
    return ReleaseReadinessTimeoutError(
        message,
        resource_release_uid=uid,
        timeout_seconds=limit,
        runtime_state=state,
        runtime_phase=phase,
        wake_requested_at=waking_at,
        expected_cold_start_seconds=expected,
    )


def _total_seconds(timeout: Any) -> float:
    if isinstance(timeout, tuple):
        connect, read = timeout
        return float(connect) + float(read)
    return float(timeout)


def _held_by_a_start(access: Any, sent_at: datetime.datetime) -> bool:
    """Whether a platform answer, read after a timeout, shows the release was starting."""
    admission = access.runtime_access
    presence = access.runtime_presence
    if admission.state == "waking" or getattr(presence, "phase", None) in _NOT_SERVING_PHASES:
        return True
    if getattr(getattr(presence, "replicas", None), "actual", None) == 0:
        return True
    # A replica that began serving after the request was sent: the start held it.
    serving_since = _parse_datetime(
        getattr(getattr(access, "scaling", None), "serving_since", None)
    )
    return serving_since is not None and serving_since > sent_at


def _access_for_call(
    release: Any,
    uid: str,
    *,
    ready_timeout: float | None,
    cache: ReleaseRuntimeCache,
    delegation: _Delegation | None = None,
) -> tuple[_Grant, bool]:
    """The access to send with, and whether it is the one this process kept.

    The kept access is used while the release is warm and its token fresh;
    otherwise the platform is asked, which starts the release, and the call
    waits while it starts. Concurrent first calls to a cold release share one
    wait. A delegated call uses only the access its own request was given, and
    never shares a wait, so no other request receives the person's token.
    """
    if delegation is not None:
        grant = delegation.kept(uid, cache)
        if grant is not None:
            return grant, True
        grant = _wait_until_ready(
            release, ready_timeout=ready_timeout, cache=cache, delegation=delegation
        )
        return grant, False
    grant = cache.warm_access(uid)
    if grant is not None:
        return grant, True
    with cache.starting(uid, _ready_limit(ready_timeout, None)):
        grant = cache.warm_access(uid)
        if grant is None:
            grant = _wait_until_ready(release, ready_timeout=ready_timeout, cache=cache)
    return grant, False


def _check_path(path: Any) -> None:
    if not isinstance(path, str) or not path.startswith("/") or path.startswith("//"):
        raise ValueError("path must be a path on the release, starting with one '/'.")


class ReleaseCall:
    """One request your own HTTP client sends to a release, prepared as ``request`` would.

    Made with ``ResourceRelease.call``, for a client library that keeps its
    own ``requests`` or ``httpx`` client. Entering the block, with ``with`` or
    ``async with``, does what ``request`` does before it sends: it takes the
    access this process keeps while the release is warm and its token fresh,
    or asks the platform, which starts the release, and waits while it starts
    (ADR 0041). Then:

    - ``url`` is the address the platform gave, followed by ``path``;
    - ``timeout`` is the call's ``timeout`` in seconds, cut to what the
      caller of the current request will still wait;
    - ``headers`` are ``Authorization``, ``Mainsequence-Request-Timeout-Ms``
      with that time, and ``traceparent`` and ``baggage`` naming the call's
      own span (ADR 0042);
    - ``reused_access`` is ``True`` when the kept access was used.

    Send one request to ``url`` with ``headers`` and ``timeout``, and pass its
    response to :meth:`record`. Leaving the block does what ``request`` does
    after it sends: an answer below status 500 is recorded as a sign that the
    release serves, which ``runtime_status`` and later calls use; a ``401``
    to the kept access, an exception in the block or a block that recorded
    no response makes the next call ask the platform again. The call's
    ``dependency.call.*`` line is written in every case.
    """

    def __init__(
        self,
        release: Any,
        method: str,
        path: str,
        *,
        timeout: float,
        ready_timeout: float | None = None,
        act_for_requester: bool = False,
    ) -> None:
        self._uid = _release_uid(release)
        _check_path(path)
        if not isinstance(method, str) or not method.strip():
            raise ValueError("method must be an HTTP method, such as GET.")
        if _positive_seconds(timeout) is None:
            raise ValueError("timeout must be a positive number of seconds.")
        if ready_timeout is not None and _positive_seconds(ready_timeout) is None:
            raise ValueError("ready_timeout must be greater than 0")
        self._release = release
        self._method = method.strip().upper()
        self._path = path
        self._total = float(timeout)
        self._ready_timeout = ready_timeout
        self._delegation = _Delegation.current() if act_for_requester else None
        self._dependency: DependencyCall | None = None
        self.url: str | None = None
        self.headers: dict[str, str] = {}
        self.timeout: float | None = None
        self.reused_access = False
        self.status_code: int | None = None
        self.response_headers: Mapping[str, str] | None = None

    def record(self, response: Any) -> None:
        """Record the answer: any response with ``status_code`` and ``headers``."""
        self.status_code = int(response.status_code)
        self.response_headers = getattr(response, "headers", None)

    def __enter__(self) -> ReleaseCall:
        if self._dependency is not None:
            raise RuntimeError("A ReleaseCall prepares one send; make a new one for each.")
        grant, self.reused_access = _access_for_call(
            self._release,
            self._uid,
            ready_timeout=self._ready_timeout,
            cache=release_runtime_cache,
            delegation=self._delegation,
        )
        caller_remaining = remaining_request_seconds()
        if caller_remaining is not None and caller_remaining <= 0:
            raise _deadline_too_short(self._uid, caller_remaining)
        self.timeout = (
            self._total if caller_remaining is None else min(self._total, caller_remaining)
        )
        self.url = grant.rpc_url.rstrip("/") + self._path
        # The release's address is the one the platform gave (ADR 0042).
        self._dependency = DependencyCall("release", self._method, self._path, name=self._uid)
        self.headers = {
            **self._dependency.headers(),
            "Authorization": f"Bearer {grant.token}",
            DEADLINE_HEADER: format_timeout_ms(self.timeout),
        }
        self._dependency.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        cache = release_runtime_cache
        status = self.status_code
        if exc is not None or status is None or (status == 401 and self.reused_access):
            cache.invalidate(self._uid)
            if self._delegation is not None:
                self._delegation.drop(self._uid)
        elif status < 500:
            cache.record_response(self._uid, self.response_headers)
        self._dependency.status_code = status
        return self._dependency.__exit__(exc_type, exc, tb)

    async def __aenter__(self) -> ReleaseCall:
        # Waiting for a start blocks; the copied context keeps the deadline and span.
        return await asyncio.to_thread(self.__enter__)

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return self.__exit__(exc_type, exc, tb)


def request_release(
    release: Any,
    method: str,
    path: str,
    *,
    timeout: float | tuple[float, float],
    ready_timeout: float | None = None,
    idempotent: bool = False,
    headers: Mapping[str, str] | None = None,
    act_for_requester: bool = False,
    **kwargs: Any,
) -> requests.Response:
    """Send one HTTP request to a release's application. See ``ResourceRelease.request``."""
    uid = _release_uid(release)
    _check_path(path)
    if ready_timeout is not None and _positive_seconds(ready_timeout) is None:
        raise ValueError("ready_timeout must be greater than 0")
    if "stream" in kwargs:
        raise TypeError("A release call reads its whole response; stream is not supported.")
    caller_headers = dict(headers or {})
    for name in caller_headers:
        if name.lower() in _RESERVED_HEADERS:
            raise ValueError(f"The {name} header is set by the SDK.")
    kwargs.setdefault("allow_redirects", False)
    total = _total_seconds(timeout)
    cache = release_runtime_cache

    delegation = _Delegation.current() if act_for_requester else None

    grant, from_cache = _access_for_call(
        release, uid, ready_timeout=ready_timeout, cache=cache, delegation=delegation
    )
    replays = 1 if idempotent else 0
    while True:
        caller_remaining = remaining_request_seconds()
        if caller_remaining is not None and caller_remaining <= 0:
            raise _deadline_too_short(uid, caller_remaining)
        limit = total if caller_remaining is None else min(total, caller_remaining)
        sent_at = _utcnow()
        try:
            with request_budget(limit):
                response = request_within_budget(
                    method,
                    grant.rpc_url.rstrip("/") + path,
                    headers={
                        **caller_headers,
                        "Authorization": f"Bearer {grant.token}",
                        DEADLINE_HEADER: format_timeout_ms(limit),
                    },
                    timeout=timeout,
                    # The release's address is the one the platform gave (ADR 0042).
                    dependency=DependencyCall("release", method, path, name=uid),
                    **kwargs,
                )
        except requests.Timeout as error:
            cache.invalidate(uid)
            if delegation is not None:
                delegation.drop(uid)
            try:
                if delegation is not None:
                    after = delegation.resolve(release)(PLATFORM_TIMEOUT_SECONDS)
                else:
                    after = release.resolve_runtime_access(timeout=PLATFORM_TIMEOUT_SECONDS)
            except Exception:
                after = None
            if after is not None:
                cache.observe(uid, after, keep_grant=delegation is None)
            held = after is not None and _held_by_a_start(after, sent_at)
            if held and replays:
                replays -= 1
                grant = _wait_until_ready(
                    release, ready_timeout=ready_timeout, cache=cache, delegation=delegation
                )
                from_cache = False
                continue
            failure = ReleaseColdStartTimeoutError if held else ReleaseRequestTimeoutError
            presence = getattr(after, "runtime_presence", None)
            raise failure(
                f"The request to release {uid} did not finish within {limit:.0f}s"
                + (" while the release was starting." if held else "."),
                resource_release_uid=uid,
                timeout_seconds=limit,
                runtime_state=str(after.runtime_access.state) if after is not None else None,
                runtime_phase=getattr(presence, "phase", None),
            ) from error
        if response.status_code == 401 and from_cache:
            # The kept token was refused before the application ran: ask for a new one, once.
            cache.invalidate(uid)
            if delegation is not None:
                delegation.drop(uid)
            grant = _wait_until_ready(
                release, ready_timeout=ready_timeout, cache=cache, delegation=delegation
            )
            from_cache = False
            continue
        if response.status_code < 500:
            cache.record_response(uid, response.headers)
        return response


def _as_release(release: Any) -> Any:
    if hasattr(release, "resolve_runtime_access"):
        return release
    from .models_helpers import ResourceRelease

    return ResourceRelease.by_uid(release)


def prewarm_releases(releases: Iterable[Any], *, wait: bool = False) -> None:
    """Ask the platform to start each release, in parallel, without waiting for readiness.

    ``releases`` holds ``ResourceRelease`` objects or their UIDs. A release that
    cannot be asked about is logged and skipped; nothing is raised. ``wait``
    blocks until the platform has answered each ask, not until the releases
    serve.
    """

    def ask(release: Any) -> None:
        try:
            uid = _release_uid(release)
            access = release.resolve_runtime_access(timeout=PLATFORM_TIMEOUT_SECONDS)
            release_runtime_cache.observe(uid, access)
        except Exception as error:
            logger.warning(
                "release.prewarm.failed",
                resource_release_uid=str(getattr(release, "uid", None)),
                error=type(error).__name__,
            )

    threads = [
        threading.Thread(
            target=contextvars.copy_context().run,
            args=(ask, _as_release(release)),
            name="mainsequence-release-prewarm",
            daemon=True,
        )
        for release in releases
    ]
    for thread in threads:
        thread.start()
    if wait:
        for thread in threads:
            thread.join()


__all__ = [
    "COLD_START_HEADER",
    "PROCESS_STARTED_AT_HEADER",
    "ReleaseCall",
    "ReleaseCallError",
    "ReleaseColdStartTimeoutError",
    "ReleaseDeadlineTooShortError",
    "ReleaseNotDeployedError",
    "ReleaseReadinessTimeoutError",
    "ReleaseRequesterError",
    "ReleaseRequestTimeoutError",
    "ReleaseRuntimeCache",
    "ReleaseRuntimeStatus",
    "ReleaseUnavailableError",
    "prewarm_releases",
    "release_runtime_cache",
    "request_release",
    "wait_for_release",
]
