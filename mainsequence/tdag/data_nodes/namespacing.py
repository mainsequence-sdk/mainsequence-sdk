# mainsequence/tdag/data_nodes/namespacing.py
from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar

# IMPORTANT:
# - no env var here (tests-only, explicit usage)
# - default is empty => production behavior unchanged
_TDAG_HASH_NAMESPACE: ContextVar[str] = ContextVar("TDAG_HASH_NAMESPACE", default="")


def current_hash_namespace() -> str:
    """Returns the current namespace for hashing (empty string means 'no namespace')."""
    return _TDAG_HASH_NAMESPACE.get() or ""


@contextmanager
def hash_namespace(namespace: str):
    """
    Use in tests to isolate ALL DataNodes created inside this context.

    Example:
        with hash_namespace("pytest"):
            ts = MyNode(...)
            ts.run(...)
    """
    ns = (namespace or "").strip()
    token = _TDAG_HASH_NAMESPACE.set(ns)
    try:
        yield
    finally:
        _TDAG_HASH_NAMESPACE.reset(token)


@contextmanager
def disable_hash_namespace():
    """
    Internal safety guard: temporarily disables the active namespace.

    We use this around "rebuild from stored config" so that ambient pytest namespace
    does NOT accidentally change hashes for legacy/prod nodes that were stored without
    'hash_namespace' in their config.
    """
    token = _TDAG_HASH_NAMESPACE.set("")
    try:
        yield
    finally:
        _TDAG_HASH_NAMESPACE.reset(token)
