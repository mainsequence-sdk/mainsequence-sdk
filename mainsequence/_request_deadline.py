"""The time a request's caller will still wait, carried from hop to hop (ADR 0041).

``remaining_request_seconds`` and ``request_deadline`` are public, exported
from ``mainsequence.client``; the rest of this module is the SDK's own.
"""

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
    """Bound the work inside the block by ``seconds``, or by the caller's time if shorter.

    Inside the block, :func:`remaining_request_seconds` and the SDK's own calls
    (``ResourceRelease.request``, ``ResourceRelease.call``, A2A sends) see the
    shorter of ``seconds`` and what an enclosing deadline leaves, such as the
    one ``install_request_identity`` sets from a verified caller's
    ``Mainsequence-Request-Timeout-Ms``. A deadline can only be shortened,
    never extended. ``None`` changes nothing.
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
    """Seconds the current request's caller will still wait; ``None`` when it set no limit.

    Inside a request that ``install_request_identity`` handles for a verified
    caller who sent ``Mainsequence-Request-Timeout-Ms``, it is what remains of
    that time, cut by any :func:`request_deadline` around the code. It is zero
    or less once that time has passed, and ``None`` without a limit: a request
    without the header, a public request, a Job run or a script.

    The value is held in a context variable. ``asyncio`` tasks,
    ``asyncio.to_thread`` and FastAPI's thread pool for sync handlers carry it;
    a plain ``threading.Thread`` or a ``concurrent.futures`` executor does not,
    so work started there reads ``None``.
    """
    deadline = _DEADLINE.get()
    if deadline is None:
        return None
    return deadline - time.monotonic()
