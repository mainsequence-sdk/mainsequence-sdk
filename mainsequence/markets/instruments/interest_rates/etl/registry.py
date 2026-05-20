from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Generic, Protocol, TypeVar

import pandas as pd

from mainsequence.client import Constant as _C
from mainsequence.tdag import APIDataNode

# ---------- builder protocols (typed contracts) ----------

class DiscountCurveBuilder(Protocol):
    def __call__(
        self,
        *,
        update_statistics,
        curve_unique_identifier: str,     # resolved UID VALUE (ticker)
        base_node_curve_points: APIDataNode | None,
    ) -> pd.DataFrame: ...

class FixingRateBuilder(Protocol):
    def __call__(
        self,
        *,
        update_statistics,
        unique_identifier: str,           # resolved UID VALUE (ticker)
    ) -> pd.DataFrame: ...

T = TypeVar("T", bound=Callable)

# ---------- lazy constant registry ----------

@dataclass
class _Entry(Generic[T]):
    const_name: str
    builder: T

class ConstantRegistry(Generic[T]):
    """
    Registry keyed by Constant *name* (e.g. 'ZERO_CURVE__VALMER_TIIE_28').

    At runtime you can:
      - resolve(const_name) -> uid_value
      - get_builder_by_uid(uid_value)
    """
    def __init__(self) -> None:
        self._by_const: dict[str, T] = {}
        self._uid_cache: dict[str, str] = {}    # const_name -> uid_value
        self._by_uid: dict[str, T] = {}         # uid_value -> builder

    def register(self, const_name: str, builder: T) -> None:
        if const_name in self._by_const and self._by_const[const_name] is not builder:
            raise ValueError(f"Duplicate registry key with different builder: {const_name}")
        self._by_const[const_name] = builder

    def uid(self, const_name: str) -> str:
        if const_name not in self._uid_cache:
            self._uid_cache[const_name] = _C.get_value(name=const_name)
        return self._uid_cache[const_name]

    def builder_for_const(self, const_name: str) -> T:
        return self._by_const[const_name]

    def builder_for_uid(self, uid_value: str) -> T:
        # build reverse map lazily
        if uid_value in self._by_uid:
            return self._by_uid[uid_value]
        for const_name, builder in self._by_const.items():
            if self.uid(const_name) == uid_value:
                self._by_uid[uid_value] = builder
                return builder
        raise KeyError(f"No builder registered for uid={uid_value!r}")

    def all_const_names(self) -> list[str]:
        return sorted(self._by_const.keys())

# ---------- concrete registries ----------

DISCOUNT_CURVE_BUILDERS = ConstantRegistry[DiscountCurveBuilder]()
FIXING_RATE_BUILDERS    = ConstantRegistry[FixingRateBuilder]()

