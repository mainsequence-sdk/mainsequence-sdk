"""Local-mode callers an application may reuse across requests (#251)."""

from __future__ import annotations

import hashlib
import hmac
import math
import secrets
import threading
import time
from collections import OrderedDict

import jwt

# At most this many callers per process, as for team membership answers.
MAX_CACHED_CALLERS = 10_000


def _now() -> float:
    return time.monotonic()


def _seconds_until_expiry(token: str) -> float | None:
    """Seconds until the token's ``exp``, read unverified only to shorten a lifetime.

    The platform has already verified the token; None when it is not a JWT or
    carries no usable ``exp``.
    """
    try:
        expiry = jwt.decode(token, options={"verify_signature": False}).get("exp")
    except (jwt.PyJWTError, AttributeError):
        return None
    if isinstance(expiry, bool) or not isinstance(expiry, (int, float)):
        return None
    if not math.isfinite(expiry):
        return None
    return expiry - time.time()


class _Check:
    """One platform check that requests for the same token wait for."""

    def __init__(self):
        self.done = threading.Event()
        self.outcome = None
        self.waiters = 0


class LocalCallerCache:
    """Who holds a Bearer token, as the platform answered, oldest dropped first.

    An entry is keyed by the endpoint and an HMAC-SHA256 of the token under a
    key generated for this cache, so the token is never stored and the key
    means nothing outside the process. Only successful answers are kept, for
    at most ``lifetime`` seconds and never past the token's ``exp``. Requests
    for the same token while its check is in flight wait for that check and
    share its result, success or failure.
    """

    def __init__(self, max_callers: int = MAX_CACHED_CALLERS):
        self._hmac_key = secrets.token_bytes(32)
        self._callers: OrderedDict[tuple[str, bytes], tuple[object, float]] = OrderedDict()
        self._checks: dict[tuple[str, bytes], _Check] = {}
        self._max_callers = max_callers
        self._lock = threading.Lock()

    def caller(self, endpoint: str, token: str, *, lifetime: float, check):
        """The caller ``check()`` answers for ``token``, reused while it is fresh.

        ``check`` asks the platform and raises when it refuses or cannot answer.
        """
        key = (endpoint, hmac.new(self._hmac_key, token.encode(), hashlib.sha256).digest())
        with self._lock:
            stored = self._callers.get(key)
            if stored is not None:
                caller, expires_at = stored
                if _now() < expires_at:
                    return caller
                del self._callers[key]
            pending = self._checks.get(key)
            leader = pending is None
            if leader:
                pending = self._checks[key] = _Check()
            else:
                pending.waiters += 1
        if not leader:
            pending.done.wait()
            if isinstance(pending.outcome, BaseException):
                raise pending.outcome
            return pending.outcome
        try:
            pending.outcome = check()
        except BaseException as exc:
            pending.outcome = exc
            raise
        finally:
            refused = isinstance(pending.outcome, BaseException)
            if not refused:
                remaining = _seconds_until_expiry(token)
                lifetime = lifetime if remaining is None else min(lifetime, remaining)
            with self._lock:
                if not refused:
                    self._store(key, pending.outcome, lifetime)
                del self._checks[key]
            pending.done.set()
        return pending.outcome

    def _store(self, key, caller, lifetime: float) -> None:
        if lifetime <= 0:
            return
        self._callers[key] = (caller, _now() + lifetime)
        self._callers.move_to_end(key)
        while len(self._callers) > self._max_callers:
            self._callers.popitem(last=False)

    def __len__(self) -> int:
        with self._lock:
            return len(self._callers)

    def __repr__(self) -> str:
        return f"LocalCallerCache({len(self)} callers)"
