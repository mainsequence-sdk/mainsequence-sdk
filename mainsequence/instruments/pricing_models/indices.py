# pricing_models/indices.py
"""
Index factory for QuantLib (identifier-driven).

Core rule
---------
This module is PURE/BASE: it contains NO Constant names and NO back-compat fallbacks.

You MUST register how to build each index via `register_index_spec(...)`
(from your extended package, e.g. banxico.src) before calling `get_index(...)`.

Usage
-----
>>> from datetime import date
>>> import QuantLib as ql
>>> from mainsequence.instruments.pricing_models.indices import (
...     register_index_spec, get_index
... )
>>> from mainsequence.instruments.pricing_models.indices_builders import (
...     tiie_spec, mx_calendar, mx_currency
... )
>>>
>>> register_index_spec(
...     "TIIE_28",
...     lambda: tiie_spec(
...         curve_uid="ZERO_CURVE_UID_FOR_TIIE_28",
...         period_days=28,
...         calendar=mx_calendar(),
...         day_counter=ql.Actual360(),
...         currency=mx_currency(),
...         settlement_days=1,
...         bdc=ql.ModifiedFollowing,
...         end_of_month=False,
...     ),
... )
>>>
>>> idx = get_index("TIIE_28", target_date=date(2024, 6, 14))

Notes
-----
- `get_index` is driven ONLY by `index_identifier` and `target_date` (plus optional overrides).
- QuantLib index `name()` is set to your UID (the normalized identifier).
- Curve construction comes from `build_zero_curve(target_date, index_identifier)`,
  which depends on the registered IndexSpec (curve_uid, calendar, day_counter, etc).
"""

from __future__ import annotations

import datetime
import re
from collections.abc import Callable
from dataclasses import is_dataclass, replace
from functools import lru_cache
from threading import RLock

import QuantLib as ql

from mainsequence.instruments.data_interface import data_interface

# IndexSpec (and builders like tiie_spec/cete_spec) live here.
from mainsequence.instruments.pricing_models.indices_builders import IndexSpec
from mainsequence.instruments.utils import to_py_date, to_ql_date

# ----------------------------- Identifier normalization ----------------------------------- #

# ex: "TIIE_28D" -> "TIIE_28"
_DAY_SUFFIX_RE = re.compile(r"\d+D$", re.IGNORECASE)


def _normalize_index_identifier(x: str) -> str:
    """
    Normalize index identifiers for compatibility.

    - If you pass "TIIE_28D" we normalize to "TIIE_28" (only when suffix matches <digits>D).
      This avoids the old buggy logic that rejected ANY 'D' (e.g. 'USD_SOFR').
    """
    s = str(x).strip()
    if not s:
        raise ValueError("index_identifier must be a non-empty string")
    if _DAY_SUFFIX_RE.search(s):
        return s[:-1]
    return s


def _ensure_py_date(d: datetime.date | datetime.datetime | ql.Date) -> datetime.date:
    """Return a Python date; target_date is REQUIRED and must not be None."""
    if d is None:
        raise ValueError("target_date is required and cannot be None.")
    if isinstance(d, datetime.datetime):
        return d.date()
    if isinstance(d, datetime.date):
        return d
    # ql.Date
    return to_py_date(d).date()


def _with_default_fixings_uid(spec: IndexSpec, index_uid: str) -> IndexSpec:
    """
    If spec.fixings_uid is missing, default it to index_uid.
    Works with frozen dataclasses (via dataclasses.replace).
    """
    if getattr(spec, "fixings_uid", None):
        return spec
    if is_dataclass(spec):
        return replace(spec, fixings_uid=index_uid)
    # Fallback (unlikely): try attribute set
    try:
        spec.fixings_uid = index_uid
        return spec
    except Exception as e:
        raise TypeError("IndexSpec does not support setting fixings_uid") from e


# ----------------------------- IndexSpec registry (plugin wiring) ------------------------- #

_SPEC_LOCK = RLock()
_SPEC_FACTORIES: dict[str, Callable[[], IndexSpec]] = {}
_SPEC_CACHE: dict[str, IndexSpec] = {}


def register_index_spec(
    index_uid: str,
    spec: IndexSpec | Callable[[], IndexSpec],
    *,
    override: bool = False,
) -> None:
    """
    Register how to build an index and which curve UID backs it.

    index_uid is what you pass into get_index(...), and becomes QuantLib index name().

    This is the ONLY wiring mechanism in the base library.
    """
    uid = _normalize_index_identifier(index_uid)

    def factory() -> IndexSpec:
        return spec() if callable(spec) else spec

    with _SPEC_LOCK:
        if (not override) and uid in _SPEC_FACTORIES:
            raise ValueError(f"IndexSpec already registered for {uid!r}")
        _SPEC_FACTORIES[uid] = factory
        _SPEC_CACHE.pop(uid, None)

    # Wiring changed => cached curves/indices may be stale.
    clear_index_cache()
    _default_curve_cached.cache_clear()


def clear_index_spec_cache() -> None:
    """Clear only the resolved IndexSpec cache (does not clear registered factories)."""
    with _SPEC_LOCK:
        _SPEC_CACHE.clear()


def get_index_spec(index_uid: str) -> IndexSpec:
    """
    Resolve an IndexSpec for `index_uid`.

    No built-ins. No fallbacks. If it's not registered, that's a hard error.
    """
    uid = _normalize_index_identifier(index_uid)

    with _SPEC_LOCK:
        cached = _SPEC_CACHE.get(uid)
        if cached is not None:
            return cached

        f = _SPEC_FACTORIES.get(uid)
        if f is None:
            raise KeyError(
                f"No IndexSpec registered for {uid!r}. "
                f"Register one via register_index_spec({uid!r}, ...)."
            )

        spec = _with_default_fixings_uid(f(), uid)
        _SPEC_CACHE[uid] = spec
        return spec


# ----------------------------- Cache (identifier + date + curve-handle identity) ---------- #

# key: (index_identifier, target_date_py, forwarding_curve_id_or_none, settlement_days_override_or_none)
_IndexCacheKey = tuple[str, datetime.date, int | None, int | None]
_INDEX_CACHE: dict[_IndexCacheKey, ql.Index] = {}


def clear_index_cache() -> None:
    _INDEX_CACHE.clear()


# ----------------------------- Zero-curve builder ----------------------------------------- #

def build_zero_curve_with_effective_date(
    target_date: datetime.date | datetime.datetime,
    index_identifier: str,
) -> tuple[ql.YieldTermStructureHandle, datetime.date | datetime.datetime]:
    """
    Build a discount curve for the given index_identifier as of target_date
    and also return the effective curve date used by the data backend.
    """
    uid = _normalize_index_identifier(index_identifier)
    spec = get_index_spec(uid)

    dc: ql.DayCounter = spec.day_counter
    calendar: ql.Calendar = spec.calendar
    curve_uid: str = spec.curve_uid

    nodes, effective_curve_date = data_interface.get_historical_discount_curve(curve_uid, target_date)

    # Anchor at the effective curve date (not necessarily the requested date)
    base_dt = (
        effective_curve_date
        if isinstance(effective_curve_date, datetime.datetime)
        else datetime.datetime.combine(effective_curve_date, datetime.time())
    )
    base = to_ql_date(base_dt)

    dates = [base]
    discounts = [1.0]
    seen = {base.serialNumber()}

    for n in sorted(nodes, key=lambda n: int(n["days_to_maturity"])):
        days = int(n["days_to_maturity"])
        if days <= 0:
            continue

        d = to_ql_date(base_dt + datetime.timedelta(days=days))

        sn = d.serialNumber()
        if sn in seen:
            continue
        seen.add(sn)

        z = n.get("zero", n.get("zero_rate", n.get("rate")))
        z = float(z)
        if z > 1.0:
            z *= 0.01  # percent -> decimal

        T = dc.yearFraction(base, d)
        # Preserving your existing assumption: "Valmer zero is simple ACT/360"
        df = 1.0 / (1.0 + z * T)

        dates.append(d)
        discounts.append(df)

    ts = ql.DiscountCurve(dates, discounts, dc, calendar)
    ts.enableExtrapolation()
    return ql.YieldTermStructureHandle(ts), effective_curve_date


def build_zero_curve(
    target_date: datetime.date | datetime.datetime,
    index_identifier: str,
) -> ql.YieldTermStructureHandle:
    """Return only the curve handle. Use build_zero_curve_with_effective_date for the effective date."""
    curve, _effective_date = build_zero_curve_with_effective_date(target_date, index_identifier)
    return curve


@lru_cache(maxsize=256)
def _default_curve_cached(index_identifier: str, date_key: datetime.date) -> ql.YieldTermStructureHandle:
    target_dt = datetime.datetime.combine(date_key, datetime.time())
    return build_zero_curve(target_dt, index_identifier)


def _default_curve(index_identifier: str, target_date: datetime.date | datetime.datetime | ql.Date) -> ql.YieldTermStructureHandle:
    dk = _ensure_py_date(target_date)
    uid = _normalize_index_identifier(index_identifier)
    return _default_curve_cached(uid, dk)


# ----------------------------- Historical fixings hydration -------------------------------- #

_FIXINGS_LOCK = RLock()
_FIXINGS_HYDRATED_UNTIL: dict[str, datetime.date] = {}


def add_historical_fixings(
    target_date: ql.Date,
    ibor_index: ql.IborIndex,
    *,
    reference_rate_uid: str | None = None,
) -> None:
    """
    Backfill historical fixings for an index up to (but not including) target_date,
    restricted to valid fixing dates for that index's calendar.
    """
    end_date = to_py_date(target_date)  # timezone-aware datetime from utils.py
    start_date = end_date - datetime.timedelta(days=365)

    uid = reference_rate_uid
    if not uid:
        # We set QuantLib index name() to the UID.
        try:
            uid = str(ibor_index.name())
        except Exception:
            uid = None
    if not uid:
        try:
            uid = str(ibor_index.familyName())
        except Exception:
            uid = "UNKNOWN_UID"

    historical_fixings = data_interface.get_historical_fixings(
        reference_rate_uid=uid, start_date=start_date, end_date=end_date
    )
    if not historical_fixings:
        return

    valid_qld: list[ql.Date] = []
    valid_rates: list[float] = []

    for dt_py, rate in sorted(historical_fixings.items()):
        qld = to_ql_date(dt_py)
        if qld < target_date and ibor_index.isValidFixingDate(qld):
            valid_qld.append(qld)
            valid_rates.append(float(rate))

    if not valid_qld:
        return

    ibor_index.addFixings(valid_qld, valid_rates, True)


def _ensure_fixings_hydrated(
    ibor_index: ql.IborIndex,
    *,
    fixings_uid: str,
    target_date_py: datetime.date,
) -> None:
    """Ensure fixings are hydrated up to target_date_py (exclusive) at least once per process."""
    with _FIXINGS_LOCK:
        last = _FIXINGS_HYDRATED_UNTIL.get(fixings_uid)
        if last is not None and last >= target_date_py:
            return
        _FIXINGS_HYDRATED_UNTIL[fixings_uid] = target_date_py

    try:
        add_historical_fixings(to_ql_date(target_date_py), ibor_index, reference_rate_uid=fixings_uid)
    except Exception:
        with _FIXINGS_LOCK:
            cur = _FIXINGS_HYDRATED_UNTIL.get(fixings_uid)
            if cur == target_date_py:
                _FIXINGS_HYDRATED_UNTIL.pop(fixings_uid, None)
        raise


# ----------------------------- Index construction ------------------------------------------ #

def _make_index_from_spec(
    index_identifier: str,
    spec: IndexSpec,
    curve: ql.YieldTermStructureHandle,
    *,
    override_settlement_days: int | None = None,
) -> ql.IborIndex:
    """Build a ql.IborIndex using the spec stored in IndexSpec."""
    settle: int = (
        int(override_settlement_days)
        if override_settlement_days is not None
        else int(spec.settlement_days)
    )

    # IMPORTANT: QuantLib index name() is the UID
    return ql.IborIndex(
        index_identifier,
        spec.period,
        settle,
        spec.currency,
        spec.calendar,
        spec.bdc,
        spec.end_of_month,
        spec.day_counter,
        curve,
    )


# ----------------------------- Public API -------------------------------------------------- #

def get_index(
    index_identifier: str,
    target_date: datetime.date | datetime.datetime | ql.Date,
    *,
    forwarding_curve: ql.YieldTermStructureHandle | ql.YieldTermStructure | None = None,
    hydrate_fixings: bool = True,
    settlement_days: int | None = None,
) -> ql.Index:
    """
    Return a QuantLib index instance based ONLY on a stable index_identifier and a target_date.

    This function does NOT resolve Constant names and does NOT contain any built-in defaults.
    """
    uid = _normalize_index_identifier(index_identifier)
    target_date_py = _ensure_py_date(target_date)

    # Normalize forwarding_curve to a handle if needed
    use_curve_handle: ql.YieldTermStructureHandle
    curve_key: int | None

    if forwarding_curve is not None:
        if isinstance(forwarding_curve, ql.YieldTermStructureHandle):
            use_curve_handle = forwarding_curve
            curve_key = id(use_curve_handle)
        else:
            use_curve_handle = ql.YieldTermStructureHandle(forwarding_curve)
            curve_key = id(forwarding_curve)
    else:
        use_curve_handle = _default_curve(uid, target_date_py)
        curve_key = None

    cache_key: _IndexCacheKey = (uid, target_date_py, curve_key, settlement_days)
    cached = _INDEX_CACHE.get(cache_key)
    if cached is not None:
        idx = cached
    else:
        spec = get_index_spec(uid)
        idx = _make_index_from_spec(
            index_identifier=uid,
            spec=spec,
            curve=use_curve_handle,
            override_settlement_days=settlement_days,
        )
        _INDEX_CACHE[cache_key] = idx

    if hydrate_fixings and isinstance(idx, ql.IborIndex):
        spec = get_index_spec(uid)
        fix_uid = spec.fixings_uid or uid
        _ensure_fixings_hydrated(idx, fixings_uid=fix_uid, target_date_py=target_date_py)

    return idx


index_by_name = get_index

__all__ = [
    "IndexSpec",
    "register_index_spec",
    "get_index_spec",
    "clear_index_spec_cache",
    "clear_index_cache",
    "build_zero_curve",
    "build_zero_curve_with_effective_date",
    "add_historical_fixings",
    "get_index",
    "index_by_name",
]

