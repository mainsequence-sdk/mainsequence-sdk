"""Python projections of the standalone Command Center SDK contracts.

Import concrete contracts from their owning submodule. Keeping this package
initializer lightweight prevents a resource-contract import from loading
workspace clients or optional framework integrations.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

_PUBLIC_SUBMODULES = ("data_models", "resource", "contracts", "providers")
__all__ = list(_PUBLIC_SUBMODULES)


def __getattr__(name: str) -> Any:
    """Resolve legacy package-level exports without eagerly importing every contract."""

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
