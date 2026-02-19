
"""
Test: rebuild all bond instruments from pure JSON payloads and compute a price.

This file is intentionally "integration-style":
- Instruments are rebuilt using InstrumentModel.rebuild(...)
- Inputs are plain JSON-compatible dicts (no QuantLib objects)
- Each instrument is priced (using with_yield to avoid needing external curves)
- Floaters are given a deterministic injected index+curve to avoid dependency on get_index()

Run:
    pytest -q

(or as a script)
    python test_bonds_from_json.py
"""

from __future__ import annotations

import datetime
import json
from typing import Any

import pytest
import QuantLib as ql

# Ensure subclasses are registered for InstrumentModel.rebuild()
# (Importing the module triggers InstrumentModel.__init_subclass__ registration.)
import mainsequence.instruments.instruments.bond as _bond_module  # noqa: F401
from mainsequence.instruments.instruments.base_instrument import InstrumentModel
from mainsequence.instruments.utils import to_ql_date


def wire(instrument_type: str, instrument: dict[str, Any]) -> str:
    """Build the exact wire format InstrumentModel.rebuild expects."""
    return json.dumps({"instrument_type": instrument_type, "instrument": instrument})


def make_flat_curve_handle(dt, y, day_count, *, comp=ql.Compounded, freq=ql.Annual):
    return ql.YieldTermStructureHandle(ql.FlatForward(to_ql_date(dt), y, day_count, comp, freq))



def inject_test_index(inst: Any, curve: ql.YieldTermStructureHandle) -> None:
    """
    Avoid external index/market-data dependencies by injecting a deterministic IborIndex.

    This keeps the test "pure JSON in -> object -> price out" without requiring get_index() to work.
    """
    if not hasattr(inst, "_index"):
        return
    if getattr(inst, "_index", None) is not None:
        return

    tenor = getattr(inst, "coupon_frequency", None)
    if tenor is None:
        tenor = ql.Period(3, ql.Months)

    # Prefer USDLibor; fallback to Euribor if needed.
    try:
        inst._index = ql.USDLibor(tenor, curve)
    except Exception:
        inst._index = ql.Euribor(tenor, curve)

    # Optional: mirror production behavior
    if hasattr(inst, "_register_index_observer"):
        try:
            inst._register_index_observer()
        except Exception:
            pass

def rebuild_and_price(
    instrument_type: str,
    instrument_payload: dict[str, Any],
    *,
    valuation_dt: datetime.datetime,
    with_yield: float = 0.05,
) -> float:
    obj = InstrumentModel.rebuild(wire(instrument_type, instrument_payload))
    obj.set_valuation_date(valuation_dt)

    # Inject a deterministic curve+index for floaters (if present)
    try:
        dc = getattr(obj, "day_count", ql.Actual360())
        curve = make_flat_curve_handle(valuation_dt, with_yield, dc)
        inject_test_index(obj, curve)
    except Exception:
        pass

    px = float(obj.price(with_yield=with_yield))
    assert px == px  # not NaN
    return px


def test_rebuild_and_price_all_bond_types() -> None:
    # Use a valuation date BEFORE issue dates to avoid requiring historical fixings for floaters.
    valuation_dt = datetime.datetime(2024, 1, 5)

    # -------------------------
    # FixedRateBond (auto schedule)
    # -------------------------
    fixed_auto = {
        "face_value": 1000.0,
        "issue_date": "2024-01-15",
        "maturity_date": "2026-01-15",
        "day_count": "Actual/360",
        "calendar": {"name": "TARGET"},
        "business_day_convention": "Following",
        "settlement_days": 2,
        "coupon_frequency": "6M",
        "coupon_rate": 0.05,
    }
    px_fixed_auto = rebuild_and_price("FixedRateBond", fixed_auto, valuation_dt=valuation_dt)

    # -------------------------
    # FixedRateBond (custom schedule)
    # -------------------------
    fixed_sched = {
        **fixed_auto,
        "schedule": {
            "dates": [
                "2024-01-15",
                "2024-07-15",
                "2025-01-15",
                "2025-07-15",
                "2026-01-15",
            ],
            "calendar": {"name": "TARGET"},
            "business_day_convention": "Following",
            "termination_business_day_convention": "Following",
            "end_of_month": False,
            "tenor": "6M",
            "rule": "Forward",
        },
    }
    px_fixed_sched = rebuild_and_price("FixedRateBond", fixed_sched, valuation_dt=valuation_dt)

    # -------------------------
    # ZeroCouponBond
    # -------------------------
    zcb = {
        "face_value": 1000.0,
        "issue_date": "2024-01-15",
        "maturity_date": "2026-01-15",
        "day_count": "Actual/360",
        "calendar": {"name": "TARGET"},
        "business_day_convention": "Following",
        "settlement_days": 2,
        "redemption_pct": 100.0,
    }
    px_zcb = rebuild_and_price("ZeroCouponBond", zcb, valuation_dt=valuation_dt)

    # -------------------------
    # FloatingRateBond
    # -------------------------
    floater = {
        "face_value": 1000.0,
        "issue_date": "2024-01-15",
        "maturity_date": "2025-01-15",
        "day_count": "Actual/360",
        "calendar": {"name": "TARGET"},
        "business_day_convention": "Following",
        "settlement_days": 2,
        "coupon_frequency": "3M",
        "floating_rate_index_name": "USD-LIBOR-3M",
        "spread": 0.0025,
        # Optional QuantLib args (all JSON-friendly)
        "caps": None,
        "floors": None,
        "gearings": None,
        "spreads": None,
        "fixing_days": None,
        "in_arrears": False,
        "redemption": 100.0,
    }
    px_floater = rebuild_and_price("FloatingRateBond", floater, valuation_dt=valuation_dt)

    # -------------------------
    # CallableFixedRateBond (Tree engine via default discount_parameters)
    # -------------------------
    callable_bond = {
        "face_value": 1000.0,
        "issue_date": "2024-01-15",
        "maturity_date": "2030-01-15",
        "day_count": "Actual/360",
        "calendar": {"name": "TARGET"},
        "business_day_convention": "Following",
        "settlement_days": 2,
        "coupon_frequency": "1Y",
        "coupon_rate": 0.05,
        "callability": [
            {"date": "2027-01-15", "type": "Call", "price": 100.0, "price_type": "Clean"},
            {"date": "2028-01-15", "type": "Call", "price": 100.0, "price_type": "Clean"},
        ],
        # discount_parameters omitted -> uses model default_factory
    }
    px_callable = rebuild_and_price("CallableFixedRateBond", callable_bond, valuation_dt=valuation_dt)

    # -------------------------
    # AmortizingFixedRateBond
    # (6M schedule from 2024-01-15 to 2026-01-15 -> 4 periods -> 4 notionals)
    # -------------------------
    amort_fixed = {
        "face_value": 1000.0,
        "issue_date": "2024-01-15",
        "maturity_date": "2026-01-15",
        "day_count": "Actual/360",
        "calendar": {"name": "TARGET"},
        "business_day_convention": "Following",
        "settlement_days": 2,
        "coupon_frequency": "6M",
        "coupon_rate": 0.04,
        "amortization": {
            "notionals": [1000.0, 900.0, 800.0, 700.0],
            "payment_lag": 0,
        },
    }
    px_amort_fixed = rebuild_and_price("AmortizingFixedRateBond", amort_fixed, valuation_dt=valuation_dt)

    # -------------------------
    # AmortizingFloatingRateBond
    # (3M schedule from 2024-01-15 to 2025-01-15 -> 4 periods -> 4 notionals)
    # -------------------------
    amort_float = {
        "face_value": 1000.0,
        "issue_date": "2024-01-15",
        "maturity_date": "2025-01-15",
        "day_count": "Actual/360",
        "calendar": {"name": "TARGET"},
        "business_day_convention": "Following",
        "settlement_days": 2,
        "coupon_frequency": "3M",
        "floating_rate_index_name": "USD-LIBOR-3M",
        "spread": 0.0010,
        "amortization": {
            "notionals": [1000.0, 900.0, 800.0, 700.0],
            "payment_lag": 0,
        },
    }
    px_amort_float = rebuild_and_price("AmortizingFloatingRateBond", amort_float, valuation_dt=valuation_dt)

    # Basic sanity: all prices are finite floats
    for name, px in [
        ("fixed_auto", px_fixed_auto),
        ("fixed_sched", px_fixed_sched),
        ("zcb", px_zcb),
        ("floater", px_floater),
        ("callable", px_callable),
        ("amort_fixed", px_amort_fixed),
        ("amort_float", px_amort_float),
    ]:
        assert isinstance(px, float), name
        assert abs(px) < 1e12, name


def test_amortizing_zspread_currency_to_per100_scale_is_notional_aware() -> None:
    """
    Regression test for the scaling fix:
    when converting currency dirty price to per-100 clean price, use bond.notional(settlementDate)
    (amortizing bonds), not the original face_value.
    """
    # Choose a valuation date AFTER amortization starts so notional(settlementDate) != face_value.
    valuation_dt = datetime.datetime(2025, 2, 1)

    payload = {
        "face_value": 1000.0,
        "issue_date": "2024-01-15",
        "maturity_date": "2026-01-15",
        "day_count": "Actual/360",
        "calendar": {"name": "TARGET"},
        "business_day_convention": "Following",
        "settlement_days": 2,
        "coupon_frequency": "6M",
        "coupon_rate": 0.04,
        "amortization": {
            # 6M schedule has 4 periods
            "notionals": [1000.0, 900.0, 800.0, 700.0],
            "redemptions": [100.0],
            "payment_lag": 0,
        },
    }

    inst = InstrumentModel.rebuild(wire("AmortizingFixedRateBond", payload))
    inst.set_valuation_date(valuation_dt)

    # Price off a flat curve (no benchmark/index required)
    y = 0.05
    an = inst.analytics(with_yield=y, flat_compounding=ql.Compounded, flat_frequency=ql.Annual)
    qb = inst.get_ql_bond(build_if_needed=True, with_yield=y)

    settle = qb.settlementDate()

    # Defensive notional() access across wrappers
    try:
        notional = float(qb.notional(settle))
    except TypeError:
        notional = float(qb.notional())

    # Convert per-100 dirty quote to currency dirty using the ACTUAL notional at settlement
    target_dirty_ccy = float(an["dirty_price"]) * notional / 100.0

    # Explicitly pass the same flat curve handle to z_spread (fixed bonds don't have a default curve)
    h = make_flat_curve_handle(valuation_dt, y, inst.day_count, comp=ql.Compounded, freq=ql.Annual)
    z = float(inst.z_spread(target_dirty_ccy, discount_curve=h, use_quantlib=True))

    assert z == pytest.approx(0.0, abs=1e-8)



def test_flat_curve_convention_change_rebuilds() -> None:
    valuation_dt = datetime.datetime(2024, 1, 5)
    payload = {
        "face_value": 1000.0,
        "issue_date": "2024-01-15",
        "maturity_date": "2026-01-15",
        "day_count": "Actual/360",
        "calendar": {"name": "TARGET"},
        "business_day_convention": "Following",
        "settlement_days": 2,
        "coupon_frequency": "6M",
        "coupon_rate": 0.05,
    }
    inst = InstrumentModel.rebuild(wire("FixedRateBond", payload))
    inst.set_valuation_date(valuation_dt)

    px1 = inst.price(with_yield=0.05, flat_compounding=ql.Compounded, flat_frequency=ql.Annual)
    px2 = inst.price(with_yield=0.05, flat_compounding=ql.Continuous, flat_frequency=ql.NoFrequency)

    assert px1 != px2

if __name__ == "__main__":
    # Allow running as a script for quick local sanity checks.

    # Run the tests in-process
    raise SystemExit(pytest.main([__file__, "-q"]))
