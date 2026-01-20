# pricing_models/indices_builders.py
"""
Small, dependency-light helpers to construct IndexSpec objects.

Why this module exists
----------------------
We want to keep the *core analytics/pricing code* independent from any
"who provides my curves/fixings" wiring (Valmer/Banxico/Polygon/etc).

Extended packages can import these helpers and then register IndexSpecs via:

    from mainsequence.instruments.pricing_models.indices import register_index_spec
    from mainsequence.instruments.pricing_models.indices_builders import tiie_spec

    register_index_spec("TIIE_28", tiie_spec(curve_uid="F_TIIE_28_VALMER", period_days=28))

Importantly:
- This module does NOT resolve Constants (_C.get_value) and does NOT do API calls.
- It only builds QuantLib-ready IndexSpec objects.
"""

from __future__ import annotations

from dataclasses import dataclass

import QuantLib as ql


@dataclass(frozen=True)
class IndexSpec:
    """
    Full specification needed to:
      1) build the curve (via curve_uid)
      2) build the QuantLib index (calendar/daycount/currency/period/etc)
      3) hydrate fixings (fixings_uid)

    Notes
    -----
    - curve_uid: UID that exists in the discount-curves storage (the "asset" identifier).
    - fixings_uid: UID that exists in the fixings storage. If None, caller should default it
      to the index UID used in get_index(...).
    """
    curve_uid: str                       # UID in discount curves table
    calendar: ql.Calendar
    day_counter: ql.DayCounter
    currency: ql.Currency
    period: ql.Period
    settlement_days: int = 1
    bdc: int = ql.ModifiedFollowing
    end_of_month: bool = False
    fixings_uid: str | None = None    # UID in fixings table (defaults to index uid)


# ----------------------------- Tiny helpers (keep it DRY) --------------------------------- #

def mx_calendar() -> ql.Calendar:
    return ql.Mexico() if hasattr(ql, "Mexico") else ql.TARGET()


def mx_currency() -> ql.Currency:
    return ql.MXNCurrency() if hasattr(ql, "MXNCurrency") else ql.USDCurrency()


# ----------------------------- Spec builders ------------------------------------------------ #

def ibor_spec(
    *,
    curve_uid: str,
    calendar: ql.Calendar,
    day_counter: ql.DayCounter,
    currency: ql.Currency,
    period: ql.Period,
    settlement_days: int = 1,
    bdc: int = ql.ModifiedFollowing,
    end_of_month: bool = False,
    fixings_uid: str | None = None,
) -> IndexSpec:
    """
    Generic builder for Ibor-like indices. Use this for any custom index you want
    to plug into the pricers without introducing constants.
    """
    return IndexSpec(
        curve_uid=curve_uid,
        calendar=calendar,
        day_counter=day_counter,
        currency=currency,
        period=period,
        settlement_days=int(settlement_days),
        bdc=int(bdc),
        end_of_month=bool(end_of_month),
        fixings_uid=fixings_uid,
    )


def tiie_spec(
    *,
    curve_uid: str,
    period_days: int,
    calendar: ql.Calendar | None = None,
    day_counter: ql.DayCounter | None = None,
    currency: ql.Currency | None = None,
    settlement_days: int = 1,
    bdc: int = ql.ModifiedFollowing,
    end_of_month: bool = False,
    fixings_uid: str | None = None,
) -> IndexSpec:
    """
    MXN TIIE-style IborIndex spec.
    """
    return ibor_spec(
        curve_uid=curve_uid,
        calendar=calendar or mx_calendar(),
        day_counter=day_counter or ql.Actual360(),
        currency=currency or mx_currency(),
        period=ql.Period(int(period_days), ql.Days),
        settlement_days=settlement_days,
        bdc=bdc,
        end_of_month=end_of_month,
        fixings_uid=fixings_uid,
    )


def cete_spec(
    *,
    curve_uid: str,
    period_days: int,
    calendar: ql.Calendar | None = None,
    day_counter: ql.DayCounter | None = None,
    currency: ql.Currency | None = None,
    settlement_days: int = 1,
    bdc: int = ql.Following,
    end_of_month: bool = False,
    fixings_uid: str | None = None,
) -> IndexSpec:
    """
    CETE-style index spec.

    Notes
    -----
    - discount CETE off the gov curve (M Bonos OTR).
    - DayCounter: kept as Actual360 to match your existing assumptions.
    """
    return ibor_spec(
        curve_uid=curve_uid,
        calendar=calendar or mx_calendar(),
        day_counter=day_counter or ql.Actual360(),
        currency=currency or mx_currency(),
        period=ql.Period(int(period_days), ql.Days),
        settlement_days=settlement_days,
        bdc=bdc,
        end_of_month=end_of_month,
        fixings_uid=fixings_uid,
    )


def mx_gov_overnight_spec(
    *,
    curve_uid: str,
    period_days: int = 1,
    calendar: ql.Calendar | None = None,
    day_counter: ql.DayCounter | None = None,
    currency: ql.Currency | None = None,
    settlement_days: int = 1,
    bdc: int = ql.ModifiedFollowing,
    end_of_month: bool = False,
    fixings_uid: str | None = None,
) -> IndexSpec:
    """
    "MX gov overnight" style spec (your helper existed already).
    Common use: BONDES / overnight MXN government curve discounting.
    """
    return ibor_spec(
        curve_uid=curve_uid,
        calendar=calendar or mx_calendar(),
        day_counter=day_counter or ql.Actual360(),
        currency=currency or mx_currency(),
        period=ql.Period(int(period_days), ql.Days),
        settlement_days=settlement_days,
        bdc=bdc,
        end_of_month=end_of_month,
        fixings_uid=fixings_uid,
    )


def usd_sofr_spec(
    *,
    curve_uid: str,
    calendar: ql.Calendar | None = None,
    day_counter: ql.DayCounter | None = None,
    currency: ql.Currency | None = None,
    period: ql.Period | None = None,
    settlement_days: int = 1,
    bdc: int = ql.ModifiedFollowing,
    end_of_month: bool = False,
    fixings_uid: str | None = None,
) -> IndexSpec:
    """
    USD SOFR spec.

    IMPORTANT
    ---------
    Your previous code modeled this through ql.IborIndex with a 6M period.
    This helper preserves that behavior by default.
    """
    return ibor_spec(
        curve_uid=curve_uid,
        calendar=calendar or ql.UnitedStates(ql.UnitedStates.FederalReserve),
        day_counter=day_counter or ql.Actual360(),
        currency=currency or ql.USDCurrency(),
        period=period or ql.Period(6, ql.Months),
        settlement_days=settlement_days,
        bdc=bdc,
        end_of_month=end_of_month,
        fixings_uid=fixings_uid,
    )


__all__ = [
    "IndexSpec",
    "mx_calendar",
    "mx_currency",
    "ibor_spec",
    "tiie_spec",
    "cete_spec",
    "mx_gov_overnight_spec",
    "usd_sofr_spec",
]
