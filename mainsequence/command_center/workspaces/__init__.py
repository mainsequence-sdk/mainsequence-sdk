"""Command Center workspace clients, contracts, and payload builders.

Use the explicit owning modules so importing one workspace concern does not
eagerly load browser snapshot support and every widget contract.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

_PUBLIC_SUBMODULES = (
    "app_component",
    "documents",
    "models",
    "mounted_widgets",
    "snapshot",
    "widgets",
)
__all__ = list(_PUBLIC_SUBMODULES)


def __getattr__(name: str) -> Any:
    """Resolve package-level exports while keeping unrelated workspace modules lazy."""

    if name in _PUBLIC_SUBMODULES:
        value = import_module(f"{__name__}.{name}")
        globals()[name] = value
        return value

    for module_name in _PUBLIC_SUBMODULES:
        module = import_module(f"{__name__}.{module_name}")
        if hasattr(module, name):
            value = getattr(module, name)
            globals()[name] = value
            return value

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
