"""Deprecated compatibility namespace for :mod:`mainsequence.command_center`."""

from __future__ import annotations

import warnings
from importlib import import_module
from types import ModuleType

warnings.warn(
    "mainsequence.client.command_center has moved to mainsequence.command_center; "
    "update imports to the top-level package.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = ["connections", "sdk", "workspaces"]


def __getattr__(name: str) -> ModuleType:
    if name not in __all__:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module = import_module(f"mainsequence.command_center.{name}")
    globals()[name] = module
    return module
