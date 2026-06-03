from __future__ import annotations

import sys
import types

from . import core as core
from .core import *  # noqa: F403

__all__ = [
    *core.__all__,
]


def __getattr__(name: str):
    if hasattr(core, name):
        return getattr(core, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


class _MetaTablesModule(types.ModuleType):
    def __setattr__(self, name: str, value):
        if hasattr(core, name):
            setattr(core, name, value)
        super().__setattr__(name, value)


sys.modules[__name__].__class__ = _MetaTablesModule
