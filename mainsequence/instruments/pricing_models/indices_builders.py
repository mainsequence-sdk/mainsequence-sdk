"""mainsequence.instruments.pricing_models.indices_builders

Dependency-light builders for :class:`~IndexSpec`.

Design goal
-----------
This module is intentionally **generic**. It provides the core *shape* of an
index specification and a small helper to build one.

Country/provider-specific conventions (e.g. "MX calendar", TIIE/CETE defaults,
curve-UID selection, etc.) should live next to the code that **registers** those
indices (e.g. ``register_pricing_indices`` in the Banxico package).

Importantly:
- This module does NOT resolve Constants (``_C.get_value``).
- This module does NOT do API calls.
- It only builds QuantLib-ready :class:`~IndexSpec` objects.
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
    curve_uid: str
    calendar: ql.Calendar
    day_counter: ql.DayCounter
    currency: ql.Currency
    period: ql.Period
    settlement_days: int = 1
    bdc: int = ql.ModifiedFollowing
    end_of_month: bool = False
    fixings_uid: str | None = None


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


__all__ = [
    "IndexSpec",
    "ibor_spec",
]
