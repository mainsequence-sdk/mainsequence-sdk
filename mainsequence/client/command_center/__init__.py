"""Canonical client namespace for Command Center APIs and contracts."""

from __future__ import annotations

from importlib import import_module
from types import ModuleType

__all__ = ["connections", "sdk", "workspaces"]


def __getattr__(name: str) -> ModuleType:
    """Load public subpackages on demand without importing client-backed models eagerly."""

    if name not in __all__:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module = import_module(f"{__name__}.{name}")
    globals()[name] = module
    return module
