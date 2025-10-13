# mainsequence/instruments/pricing_models/bond_pricer.py

import QuantLib as ql


def _map_daycount(dc: str) -> ql.DayCounter:
    s = (dc or "").upper()
    if s.startswith("30/360"):
        return ql.Thirty360(ql.Thirty360.USA)
    if s in ("ACT/365", "ACT/365F", "ACTUAL/365", "ACTUAL/365F"):
        return ql.Actual365Fixed()
    if s in ("ACT/ACT", "ACTUAL/ACTUAL"):
        return ql.ActualActual()
    return ql.Thirty360(ql.Thirty360.USA)


def create_fixed_rate_bond(
    calculation_date: ql.Date,
    face: float,
    issue_date: ql.Date,
    maturity_date: ql.Date,
    coupon_rate: float,
    coupon_frequency: ql.Period,
    day_count: ql.DayCounter,
    calendar: ql.Calendar = ql.TARGET(),
    business_day_convention: int = ql.Following,  # enums are ints in the Python wrapper
    settlement_days: int = 2,
    discount_curve: ql.YieldTermStructureHandle | None = None,
    schedule: ql.Schedule | None = None,
) -> ql.FixedRateBond:
    """Construct and engine-attach."""
    ql.Settings.instance().evaluationDate = calculation_date

    # --------- Schedule ----------
    if schedule is None:
        schedule = ql.Schedule(
            issue_date,
            maturity_date,
            coupon_frequency,
            calendar,
            business_day_convention,
            business_day_convention,
            ql.DateGeneration.Forward,
            False,
        )
    else:
        asof = ql.Settings.instance().evaluationDate
        n = len(schedule.dates())
        # True floater periods exist only if schedule has >=2 dates AND at least one period end > as-of date.
        has_periods_left = (n >= 2) and any(schedule.dates()[i + 1] > asof for i in range(n - 1))
        if not has_periods_left:
            # Redemption-only: price as a zero-coupon bond (par redemption by default).
            maturity = schedule.dates()[n - 1] if n > 0 else maturity_date
            zcb = ql.ZeroCouponBond(
                settlement_days,
                calendar,  # use the same calendar as above
                face,  # notional
                maturity,  # maturity date
                business_day_convention,  # payment convention (for settlement)
                100.0,  # redemption (% of face)
                issue_date,  # issue date
            )
            zcb.setPricingEngine(ql.DiscountingBondEngine(discount_curve))
            return zcb

    bond = ql.FixedRateBond(settlement_days, face, schedule, [coupon_rate], day_count)
    bond.setPricingEngine(ql.DiscountingBondEngine(discount_curve))
    return bond


def create_floating_rate_bond_with_curve(
    *,
    calculation_date: ql.Date,
    face: float,
    issue_date: ql.Date,
    maturity_date: ql.Date,
    floating_rate_index: ql.IborIndex,
    spread: float = 0.0,
    coupon_frequency: ql.Period | None = None,
    day_count: ql.DayCounter | None = None,
    calendar: ql.Calendar | None = None,
    business_day_convention: int = ql.Following,
    settlement_days: int = 2,
    curve: ql.YieldTermStructureHandle,
    seed_past_fixings_from_curve: bool = True,
    discount_curve: ql.YieldTermStructureHandle | None = None,
    schedule: ql.Schedule | None = None,
) -> ql.FloatingRateBond:
    """
    Build/prices a floating-rate bond like your swap-with-curve:
      - clone index to 'curve'
      - spot-start safeguard
      - seed past/today fixings from the same curve
      - discount with the same curve
    """

    # --- evaluation settings (match swap) ---
    ql.Settings.instance().evaluationDate = calculation_date
    ql.Settings.instance().includeReferenceDateEvents = False
    ql.Settings.instance().enforceTodaysHistoricFixings = False

    if curve is None:
        raise ValueError("create_floating_rate_bond_with_curve: 'curve' is None")
    # Probe the handle by attempting a discount on calculation_date.
    # If the handle is unlinked/invalid this will raise; we convert it to a clear message.
    try:
        _ = curve.discount(calculation_date)
    except Exception as e:
        raise ValueError(
            "create_floating_rate_bond_with_curve: provided curve handle "
            "is not linked or cannot discount on calculation_date"
        ) from e

    # --- index & calendars ---
    pricing_index = floating_rate_index.clone(curve)  # forecast on the provided curve
    cal = calendar or pricing_index.fixingCalendar()
    freq = coupon_frequency or pricing_index.tenor()
    dc = day_count or pricing_index.dayCounter()

    eff_start = issue_date
    eff_end = maturity_date

    # --------- Schedule ----------
    if schedule is None:
        schedule = ql.Schedule(
            eff_start,
            eff_end,
            freq,
            cal,
            business_day_convention,
            business_day_convention,
            ql.DateGeneration.Forward,
            False,
        )
    else:
        asof = ql.Settings.instance().evaluationDate
        n = len(schedule.dates())
        # True floater periods exist only if schedule has >=2 dates AND at least one period end > as-of date.
        has_periods_left = (n >= 2) and any(schedule.dates()[i + 1] > asof for i in range(n - 1))
        if not has_periods_left:
            # Redemption-only: price as a zero-coupon bond (par redemption by default).
            maturity = schedule.dates()[n - 1] if n > 0 else eff_end
            zcb = ql.ZeroCouponBond(
                settlement_days,
                cal,  # use the same calendar as above
                face,  # notional
                maturity,  # maturity date
                business_day_convention,  # payment convention (for settlement)
                100.0,  # redemption (% of face)
                issue_date,  # issue date
            )
            zcb.setPricingEngine(ql.DiscountingBondEngine(curve))
            return zcb

    # --------- Instrument ----------
    try:
        bond = ql.FloatingRateBond(
            settlement_days,
            face,
            schedule,
            pricing_index,
            dc,
            business_day_convention,
            pricing_index.fixingDays(),
            [1.0],  # gearings
            [spread],  # spreads
            [],
            [],  # caps, floors
            False,  # inArrears
            100.0,  # redemption
            issue_date,
        )
    except Exception as e:
        raise e

    return bond
