# pricing_models/indices.py
# -*- coding: utf-8 -*-
"""
Index factory for QuantLib (identifier-driven).

Usage
-----
>>> from datetime import date
>>> from mainsequence.instruments.pricing_models.indices import get_index
>>> idx = get_index("TIIE_28", target_date=date(2024, 6, 14))

You can also supply a forwarding curve handle:
>>> h = ql.RelinkableYieldTermStructureHandle()
>>> idx = get_index("TIIE_28", target_date=date(2025, 9, 16), forwarding_curve=h)

Notes
-----
- `get_index` is driven ONLY by `index_identifier` and `target_date`.
- We set the QuantLib index **name** to `index_identifier`, so `index.name()` **is** your UID.
- Curve construction comes from `build_zero_curve(target_date, index_identifier)`.
"""

from __future__ import annotations

import datetime
from typing import Dict, Tuple, Union, Optional

import QuantLib as ql
from functools import lru_cache

from mainsequence.instruments.data_interface import data_interface
from mainsequence.instruments.utils import to_py_date, to_ql_date
from mainsequence.client import Constant as _C

# ----------------------------- Cache (ONLY by identifier + date) ----------------------------- #

# key: (index_identifier, target_date_py)
_IndexCacheKey = Tuple[str, datetime.date]
_INDEX_CACHE: Dict[_IndexCacheKey, ql.Index] = {}


def clear_index_cache() -> None:
    _INDEX_CACHE.clear()


# ----------------------------- Config ----------------------------- #
# Put every supported identifier here with its curve + index construction config.
# No tenor tokens; we store the QuantLib Period directly.

INDEX_CONFIGS: Dict[str, Dict] = {
    _C.get_value(name="REFERENCE_RATE__TIIE_28"): dict(
        curve_uid=_C.get_value(name="ZERO_CURVE__VALMER_TIIE_28"),
        calendar=(ql.Mexico() if hasattr(ql, "Mexico") else ql.TARGET()),
        day_counter=ql.Actual360(),
        currency=(ql.MXNCurrency() if hasattr(ql, "MXNCurrency") else ql.USDCurrency()),
        period=ql.Period(28, ql.Days),
        settlement_days=1,
        bdc=ql.ModifiedFollowing,
        end_of_month=False,
    ),
    _C.get_value(name="REFERENCE_RATE__TIIE_91"): dict(
        curve_uid=_C.get_value(name="ZERO_CURVE__VALMER_TIIE_28"),
        calendar=(ql.Mexico() if hasattr(ql, "Mexico") else ql.TARGET()),
        day_counter=ql.Actual360(),
        currency=ql.MXNCurrency(),
        period=ql.Period(91, ql.Days),
        settlement_days=1,
        bdc=ql.ModifiedFollowing,
        end_of_month=False,
    ),
    _C.get_value(name="REFERENCE_RATE__TIIE_182"): dict(
        curve_uid=_C.get_value(name="ZERO_CURVE__VALMER_TIIE_28"),
        calendar=(ql.Mexico() if hasattr(ql, "Mexico") else ql.TARGET()),
        day_counter=ql.Actual360(),
        currency=ql.MXNCurrency(),
        period=ql.Period(182, ql.Days),
        settlement_days=1,
        bdc=ql.ModifiedFollowing,
        end_of_month=False,
    ),
    # Add more identifiers here as needed.
    _C.get_value(name="REFERENCE_RATE__TIIE_OVERNIGHT"): dict(
        curve_uid=_C.get_value(name="ZERO_CURVE__VALMER_TIIE_28"),
        calendar=(ql.Mexico() if hasattr(ql, "Mexico") else ql.TARGET()),
        day_counter=ql.Actual360(),
        currency=ql.MXNCurrency(),
        period=ql.Period(1, ql.Days),
        settlement_days=1,
        bdc=ql.ModifiedFollowing,
        end_of_month=False,
    ),
    _C.get_value(name="REFERENCE_RATE__CETE_28"): dict(
        curve_uid=_C.get_value(name="ZERO_CURVE__BANXICO_M_BONOS_OTR"),
        calendar=(ql.Mexico() if hasattr(ql, "Mexico") else ql.TARGET()),
        day_counter=ql.Actual360(),  # BONOS accrue on Act/360
        currency=ql.MXNCurrency(),
        period=ql.Period(28, ql.Days),  # Coupons every 28 days
        settlement_days=1,  # T+1 in Mexico since May 27–28, 2024
        bdc=ql.Following,  # “next banking business day” => Following
        end_of_month=False,  # Irrelevant when scheduling by days
    ),
    _C.get_value(name="REFERENCE_RATE__CETE_91"): dict(
        curve_uid=_C.get_value(name="ZERO_CURVE__BANXICO_M_BONOS_OTR"),
        calendar=(ql.Mexico() if hasattr(ql, "Mexico") else ql.TARGET()),
        day_counter=ql.Actual360(),  # BONOS accrue on Act/360
        currency=ql.MXNCurrency(),
        period=ql.Period(91, ql.Days),  # Coupons every 28 days
        settlement_days=1,  # T+1 in Mexico since May 27–28, 2024
        bdc=ql.Following,  # “next banking business day” => Following
        end_of_month=False,  # Irrelevant when scheduling by days
    ),

    _C.get_value(name="REFERENCE_RATE__CETE_182"): dict(
        curve_uid=_C.get_value(name="ZERO_CURVE__BANXICO_M_BONOS_OTR"),
        calendar=(ql.Mexico() if hasattr(ql, "Mexico") else ql.TARGET()),
        day_counter=ql.Actual360(),  # BONOS accrue on Act/360
        currency=ql.MXNCurrency(),
        period=ql.Period(182, ql.Days),  # Coupons every 182 days
        settlement_days=1,  # T+1 in Mexico since May 27–28, 2024
        bdc=ql.Following,  # “next banking business day” => Following
        end_of_month=False,  # Irrelevant when scheduling by days
    ),


    _C.get_value(name="REFERENCE_RATE__USD_SOFR"): dict(
        curve_uid=_C.get_value(name="ZERO_CURVE__UST_CMT_ZERO_CURVE_UID"),
        calendar=ql.UnitedStates(ql.UnitedStates.FederalReserve),
        day_counter=ql.Actual360(),
        currency=ql.USDCurrency(),
        period=ql.Period(6, ql.Months),  # Semiannual coupons
        settlement_days=1,  # T+1
        bdc=ql.ModifiedFollowing,
        end_of_month=False,  # Irrelevant when scheduling by days
    ),
}


# ----------------------------- Utilities ----------------------------- #

def _ensure_py_date(
    d: Union[datetime.date, datetime.datetime, ql.Date]
) -> datetime.date:
    """Return a Python date; target_date is REQUIRED and must not be None."""
    if d is None:
        raise ValueError("target_date is required and cannot be None.")
    if isinstance(d, datetime.datetime):
        return d.date()
    if isinstance(d, datetime.date):
        return d
    # ql.Date
    return to_py_date(d)


# ----------------------------- Zero-curve builder (kept) ----------------------------- #

def build_zero_curve(
    target_date: Union[datetime.date, datetime.datetime],
    index_identifier: str,
) -> ql.YieldTermStructureHandle:
    """
    Build a discount curve for the given index_identifier as of target_date.
    Configuration comes from INDEX_CONFIGS[index_identifier].
    """
    cfg = INDEX_CONFIGS.get(index_identifier)
    if cfg is None:
        raise KeyError(
            f"No curve/index config for index_identifier {index_identifier!r}. "
            f"Add it to INDEX_CONFIGS."
        )

    dc: ql.DayCounter = cfg["day_counter"]
    calendar: ql.Calendar = cfg["calendar"]
    curve_uid: str = cfg["curve_uid"]

    nodes = data_interface.get_historical_discount_curve(curve_uid, target_date)

    base = to_ql_date(target_date)
    base_py = target_date if isinstance(target_date, datetime.datetime) else datetime.datetime.combine(
        target_date, datetime.time()
    )

    dates = [base]
    discounts = [1.0]
    seen = {base.serialNumber()}

    for n in sorted(nodes, key=lambda n: int(n["days_to_maturity"])):
        days = int(n["days_to_maturity"])
        if days < 0:
            continue

        d = to_ql_date(base_py + datetime.timedelta(days=days))

        sn = d.serialNumber()
        if sn in seen:
            continue
        seen.add(sn)

        z = n.get("zero", n.get("zero_rate", n.get("rate")))
        z = float(z)
        if z > 1.0:
            z *= 0.01  # percent -> decimal

        T = dc.yearFraction(base, d)
        df = 1.0 / (1.0 + z * T)  # Valmer zero is simple ACT/360

        dates.append(d)
        discounts.append(df)

    ts = ql.DiscountCurve(dates, discounts, dc, calendar)
    ts.enableExtrapolation()
    return ql.YieldTermStructureHandle(ts)


@lru_cache(maxsize=256)
def _default_curve_cached(index_identifier: str, date_key: datetime.date) -> ql.YieldTermStructureHandle:
    """Small cache for default curves, keyed only by (identifier, date)."""
    target_dt = datetime.datetime.combine(date_key, datetime.time())
    return build_zero_curve(target_dt, index_identifier)


def _default_curve(index_identifier: str, target_date: Union[datetime.date, datetime.datetime, ql.Date]) -> ql.YieldTermStructureHandle:
    dk = _ensure_py_date(target_date)
    return _default_curve_cached(index_identifier, dk)


# ----------------------------- Historical fixings hydration ----------------------------- #

def add_historical_fixings(target_date: ql.Date, ibor_index: ql.IborIndex):
    """
    Backfill historical fixings for an index up to (but not including) target_date,
    restricted to valid fixing dates for that index's calendar.

    We use the index's **name()** as the UID (because we set it to the identifier).
    """
    print("Fetching and adding historical fixings...")

    end_date = to_py_date(target_date)  # inclusive endpoint; filter qld < target_date below
    start_date = end_date - datetime.timedelta(days=365)

    uid = ibor_index.familyName()

    historical_fixings = data_interface.get_historical_fixings(
        reference_rate_uid=uid,
        start_date=start_date,
        end_date=end_date
    )

    if not historical_fixings:
        print("No historical fixings found in the given date range.")
        return

    valid_qld: list[ql.Date] = []
    valid_rates: list[float] = []

    for dt_py, rate in sorted(historical_fixings.items()):
        qld = to_ql_date(dt_py)
        if qld < target_date and ibor_index.isValidFixingDate(qld):
            valid_qld.append(qld)
            valid_rates.append(float(rate))

    if not valid_qld:
        print("No valid fixing dates for the index calendar; skipping addFixings.")
        return

    ibor_index.addFixings(valid_qld, valid_rates, True)
    print(f"Successfully added {len(valid_qld)} fixings for {uid}.")


# ----------------------------- Index construction ----------------------------- #

def _make_index_from_config(index_identifier: str,
                            curve: ql.YieldTermStructureHandle,
                            *,
                            override_settlement_days: Optional[int] = None) -> ql.IborIndex:
    """
    Build a ql.IborIndex using the exact spec stored in INDEX_CONFIGS[index_identifier].
    No tenor tokens. Period comes from config.
    """
    cfg = INDEX_CONFIGS.get(index_identifier)
    if cfg is None:
        raise KeyError(
            f"No index config for {index_identifier!r}. Add an entry to INDEX_CONFIGS."
        )

    cal: ql.Calendar = cfg["calendar"]
    ccy: ql.Currency = cfg["currency"]
    dc: ql.DayCounter = cfg["day_counter"]
    period: ql.Period = cfg["period"]
    bdc: ql.BusinessDayConvention = cfg["bdc"]
    eom: bool = cfg["end_of_month"]
    settle: int = override_settlement_days if override_settlement_days is not None else cfg["settlement_days"]

    # IMPORTANT: we set the QuantLib index **name** to the UID
    return ql.IborIndex(
        index_identifier,  # name == UID
        period,
        settle,
        ccy,
        cal,
        bdc,
        eom,
        dc,
        curve
    )


# ----------------------------- Public API ----------------------------- #

def get_index(
        index_identifier: str,
        target_date: Union[datetime.date, datetime.datetime, ql.Date],
        *,
        forwarding_curve: Optional[ql.YieldTermStructureHandle] = None,
        hydrate_fixings: bool = True,
        settlement_days: Optional[int] = None
) -> ql.Index:
    """
    Return a QuantLib index instance based ONLY on a stable index_identifier and a target_date.

    Parameters
    ----------
    index_identifier : str
        A stable UID from your settings/data model (e.g., 'TIIE_28_UID').
        This becomes the QuantLib index name (so uid == index.name()).
    target_date : date|datetime|ql.Date
        As-of date used to build the default curve when no forwarding_curve is supplied.
    forwarding_curve : Optional[ql.YieldTermStructureHandle]
        If provided and non-empty, use it; otherwise a default curve is built by identifier.
    hydrate_fixings : bool
        If True and the index is Ibor-like, backfill fixings strictly before `target_date`.
    settlement_days : Optional[int]
        Optional override for settlement days.

    Returns
    -------
    ql.Index
    """
    target_date_py = _ensure_py_date(target_date)
    if "D" in index_identifier:
        raise Exception(f"Index identifier {index_identifier!r} cannot have D.")
    # Cache ONLY by (identifier, date)
    cache_key: _IndexCacheKey = (index_identifier, target_date_py)
    cached = _INDEX_CACHE.get(cache_key)
    if cached is not None:
        return cached

    # Resolve forwarding curve or build a default one for the identifier
    if forwarding_curve is not None:
        use_curve = forwarding_curve
    else:
        use_curve = _default_curve(index_identifier, target_date_py)

    # Build the index exactly as configured
    idx = _make_index_from_config(
        index_identifier=index_identifier,
        curve=use_curve,
        override_settlement_days=settlement_days
    )

    # Optional: hydrate fixings up to (but not including) target_date
    if hydrate_fixings and isinstance(idx, ql.IborIndex):
        add_historical_fixings(to_ql_date(target_date_py), idx)

    _INDEX_CACHE[cache_key] = idx
    return idx


# ----------------------------- Convenience alias ----------------------------- #

index_by_name = get_index
