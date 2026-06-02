from __future__ import annotations

import sys
import types

from . import core as core
from . import migrations as migrations
from .core import *  # noqa: F403
from .migrations import *  # noqa: F403

migrations._bind_meta_table_migration_methods(core.MetaTable)

__all__ = [
    *migrations.__all__,
    *core.__all__,
]


def __getattr__(name: str):
    if hasattr(core, name):
        return getattr(core, name)
    if hasattr(migrations, name):
        return getattr(migrations, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


class _MetaTablesModule(types.ModuleType):
    def __setattr__(self, name: str, value):
        if hasattr(core, name):
            setattr(core, name, value)
        if hasattr(migrations, name):
            setattr(migrations, name, value)
        super().__setattr__(name, value)


sys.modules[__name__].__class__ = _MetaTablesModule
