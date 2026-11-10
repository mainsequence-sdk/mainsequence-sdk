"""The time a request's caller will still wait, carried from hop to hop (ADR 0041)."""

from __future__ import annotations

import math
import time
from contextlib import contextmanager
from contextvars import ContextVar

DEADLINE_HEADER = "Mainsequence-Request-Timeout-Ms"

# A monotonic instant, so the wall clock of neither side matters.
_DEADLINE: ContextVar[float | None] = ContextVar("mainsequence_request_deadline", default=None)


def parse_timeout_ms(value: str | None) -> float | None:
    """Seconds from a header value in milliseconds; ``None`` when absent or unusable."""
    if value is None:
        return None
    try:
        milliseconds = int(value.strip())
    except (ValueError, AttributeError):
        return None
    if milliseconds <= 0:
        return None
    return milliseconds / 1000.0


def format_timeout_ms(seconds: float) -> str:
    return str(max(1, math.floor(seconds * 1000)))


@contextmanager
def request_deadline(seconds: float | None):
    """Bound the calls made inside the block; ``None`` leaves them unbounded.

    An enclosing deadline can only be shortened.
    """
    if seconds is None:
        yield
        return
    deadline = time.monotonic() + seconds
    parent = _DEADLINE.get()
    if parent is not None:
        deadline = min(deadline, parent)
    token = _DEADLINE.set(deadline)
    try:
        yield
    finally:
        _DEADLINE.reset(token)


def remaining_request_seconds() -> float | None:
    """Seconds the current request's caller will still wait; ``None`` when it set no limit."""
    deadline = _DEADLINE.get()
    if deadline is None:
        return None
    return deadline - time.monotonic()
