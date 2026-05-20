from __future__ import annotations

from importlib import import_module

__all__ = [
    "AmortizingFixedRateBond",
    "AmortizingFloatingRateBond",
    "CallableFixedRateBond",
    "FixedRateBond",
    "FloatingRateBond",
    "Instrument",
    "Position",
    "PositionLine",
    "ZeroCouponBond",
]

_ATTR_TO_MODULE = {
    name: ".instruments"
    for name in __all__
}


def __getattr__(name: str):
    module_name = _ATTR_TO_MODULE.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module = import_module(module_name, __name__)
    value = getattr(module, name)
    globals()[name] = value
    return value
