from __future__ import annotations

import warnings
from importlib import import_module

_TARGET = "mainsequence.markets.instruments"

warnings.warn(
    "mainsequence.instruments is deprecated; use mainsequence.markets.instruments.",
    DeprecationWarning,
    stacklevel=2,
)

_module = import_module(_TARGET)

__path__ = _module.__path__
__all__ = getattr(_module, "__all__", [])


def __getattr__(name: str):
    return getattr(_module, name)
