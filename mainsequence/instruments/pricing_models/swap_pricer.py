import datetime
from typing import Any

import matplotlib.pyplot as plt
import QuantLib as ql

from mainsequence.instruments.data_interface import data_interface
from mainsequence.instruments.utils import to_py_date, to_ql_date


def _coerce_to_ql_date(x, fallback: ql.Date) -> ql.Date:
    """Coerce various date-like inputs to ql.Date, or return fallback."""
    if x is None:
        return fallback
    if isinstance(x, ql.Date):
        return x
    if isinstance(x, datetime.date):
        return to_ql_date(x)
    if isinstance(x, str):
        try:
            return to_ql_date(datetime.date.fromisoformat(x))
        except Exception:
            return fallback
    # pandas.Timestamp / numpy datetime64
    try:
        if hasattr(x, "to_pydatetime"):
            return to_ql_date(x.to_pydatetime().date())
    except Exception:
        pass
    return fallback


def make_ftiie_index(
    curve: ql.YieldTermStructureHandle, settlement_days: int = 1
) -> ql.OvernightIndex:
    cal = ql.Mexico() if hasattr(ql, "Mexico") else ql.TARGET()
    try:
        ccy = ql.MXNCurrency()
    except Exception:
        ccy = ql.USDCurrency()  # label only
    return ql.OvernightIndex("F-TIIE", settlement_days, ccy, cal, ql.Actual360(), curve)


def build_yield_curve(calculation_date: ql.Date) -> ql.YieldTermStructureHandle:
    """
    Builds a piecewise yield curve by bootstrapping over a set of market rates.
    """
    print("Building bootstrapped yield curve from market nodes...")

    rate_data = data_interface.get_historical_data("interest_rate_swaps", {"USD_rates": {}})
    curve_nodes = rate_data["curve_nodes"]

    calendar = ql.TARGET()
    day_counter = ql.Actual365Fixed()

    rate_helpers = []

    swap_fixed_leg_frequency = ql.Annual
    swap_fixed_leg_convention = ql.Unadjusted
    swap_fixed_leg_daycounter = ql.Thirty360(ql.Thirty360.USA)
    yield_curve_handle = ql.YieldTermStructureHandle(
        ql.FlatForward(calculation_date, 0.05, day_counter)
    )
    ibor_index = ql.USDLibor(ql.Period("3M"), yield_curve_handle)

    for node in curve_nodes:
        rate = node["rate"]
        tenor = ql.Period(node["tenor"])
        quote_handle = ql.QuoteHandle(ql.SimpleQuote(rate))

        if node["type"] == "deposit":
            helper = ql.DepositRateHelper(
                quote_handle, tenor, 2, calendar, ql.ModifiedFollowing, False, day_counter
            )
            rate_helpers.append(helper)
        elif node["type"] == "swap":
            helper = ql.SwapRateHelper(
                quote_handle,
                tenor,
                calendar,
                swap_fixed_leg_frequency,
                swap_fixed_leg_convention,
                swap_fixed_leg_daycounter,
                ibor_index,
            )
            rate_helpers.append(helper)

    yield_curve = ql.PiecewiseLogCubicDiscount(calculation_date, rate_helpers, day_counter)
    yield_curve.enableExtrapolation()

    print("Yield curve built successfully.")
    return ql.YieldTermStructureHandle(yield_curve)


# src/pricing_models/swap_pricer.py
def price_vanilla_swap_with_curve(
    calculation_date: ql.Date,
    notional: float,
    start_date: ql.Date,
    maturity_date: ql.Date,
    fixed_rate: float,
    fixed_leg_tenor: ql.Period,
    fixed_leg_convention: int,
    fixed_leg_daycount: ql.DayCounter,
    float_leg_tenor: ql.Period,
    float_leg_spread: float,
    ibor_index: ql.IborIndex,
    curve: ql.YieldTermStructureHandle,
) -> ql.VanillaSwap:
    # --- evaluation settings ---
    ql.Settings.instance().evaluationDate = calculation_date
    ql.Settings.instance().includeReferenceDateEvents = False
    ql.Settings.instance().enforceTodaysHistoricFixings = False

    # index linked to the provided curve
    pricing_ibor_index = ibor_index.clone(curve)
    calendar = pricing_ibor_index.fixingCalendar()

    # --------- EFFECTIVE DATES (spot start safeguard) ----------
    fixingDate = calendar.adjust(calculation_date, ql.Following)
    while not pricing_ibor_index.isValidFixingDate(fixingDate):
        fixingDate = calendar.advance(fixingDate, 1, ql.Days)
    spot_start = pricing_ibor_index.valueDate(fixingDate)

    eff_start = start_date if start_date > calculation_date else spot_start
    eff_end = maturity_date
    if eff_end <= eff_start:
        eff_end = calendar.advance(eff_start, float_leg_tenor)

    # --------- Schedules ----------
    fixed_schedule = ql.Schedule(
        eff_start,
        eff_end,
        fixed_leg_tenor,
        calendar,
        fixed_leg_convention,
        fixed_leg_convention,
        ql.DateGeneration.Forward,
        False,
    )
    float_schedule = ql.Schedule(
        eff_start,
        eff_end,
        float_leg_tenor,
        calendar,
        pricing_ibor_index.businessDayConvention(),
        pricing_ibor_index.businessDayConvention(),
        ql.DateGeneration.Forward,
        False,
    )

    # --------- Instrument ----------
    swap = ql.VanillaSwap(
        ql.VanillaSwap.Payer,
        notional,
        fixed_schedule,
        fixed_rate,
        fixed_leg_daycount,
        float_schedule,
        pricing_ibor_index,
        float_leg_spread,
        pricing_ibor_index.dayCounter(),
    )

    # --------- Seed past fixings from the curve (coupon by coupon) ----------
    # For any coupon whose fixing date <= evaluation date, insert the forward
    # implied by *this same curve* for that coupon’s accrual period (ACT/360 simple).
    dc = pricing_ibor_index.dayCounter()
    for cf in swap.leg(1):
        cup = ql.as_floating_rate_coupon(cf)
        fix = cup.fixingDate()
        if fix <= calculation_date:
            # If a real fixing already exists, leave it
            try:
                _ = pricing_ibor_index.fixing(fix)
            except RuntimeError:
                start = cup.accrualStartDate()
                end = cup.accrualEndDate()
                tau = dc.yearFraction(start, end)
                df0 = curve.discount(start)
                df1 = curve.discount(end)
                fwd = (df0 / df1 - 1.0) / tau  # simple ACT/360
                pricing_ibor_index.addFixing(fix, fwd)

    swap.setPricingEngine(ql.DiscountingSwapEngine(curve))
    return swap


def get_swap_cashflows(swap) -> dict[str, list[dict[str, Any]]]:
    """
    Analyzes the cashflows of a swap's fixed and floating legs.
    """
    cashflows = {"fixed": [], "floating": []}

    for cf in swap.leg(0):
        if not cf.hasOccurred():
            cashflows["fixed"].append(
                {"payment_date": to_py_date(cf.date()), "amount": cf.amount()}
            )

    for cf in swap.leg(1):
        if not cf.hasOccurred():
            coupon = ql.as_floating_rate_coupon(cf)
            cashflows["floating"].append(
                {
                    "payment_date": to_py_date(coupon.date()),
                    "fixing_date": to_py_date(coupon.fixingDate()),
                    "rate": coupon.rate(),
                    "spread": coupon.spread(),
                    "amount": coupon.amount(),
                }
            )

    return cashflows


def plot_swap_zero_curve(
    calculation_date: ql.Date | datetime.date,
    max_years: int = 30,
    step_months: int = 3,
    compounding=ql.Continuous,  # QuantLib enums are ints; don't type-hint them
    frequency=ql.Annual,
    show: bool = False,
    ax: plt.Axes | None = None,
) -> tuple[list[float], list[float]]:
    """
    Plot the zero-coupon (spot) curve implied by the swap-bootstrapped curve.

    Returns:
        (tenors_in_years, zero_rates) with zero_rates in decimals (e.g., 0.045).
    """
    # normalize date
    ql_calc = (
        to_ql_date(calculation_date)
        if isinstance(calculation_date, datetime.date)
        else calculation_date
    )
    ql.Settings.instance().evaluationDate = ql_calc

    # build curve from the mocked swap/deposit nodes
    ts_handle = build_yield_curve(ql_calc)

    calendar = ql.TARGET()
    day_count = ql.Actual365Fixed()

    years: list[float] = []
    zeros: list[float] = []

    months = 1
    while months <= max_years * 12:
        d = calendar.advance(ql_calc, ql.Period(months, ql.Months))
        T = day_count.yearFraction(ql_calc, d)
        z = ts_handle.zeroRate(d, day_count, compounding, frequency).rate()
        years.append(T)
        zeros.append(z)
        months += step_months

    # plot
    if ax is None:
        fig, ax = plt.subplots(figsize=(7, 4))
    ax.plot(years, [z * 100 for z in zeros])
    ax.set_xlabel("Maturity (years)")
    ax.set_ylabel("Zero rate (%)")
    ax.set_title("Zero-Coupon Yield Curve (Swaps)")
    ax.grid(True, linestyle="--", alpha=0.4)

    if show:
        plt.show()

    return years, zeros


# src/pricing_models/swap_pricer.py
import QuantLib as ql


def price_ftiie_ois_with_curve(
    calculation_date: ql.Date,
    notional: float,
    start_date: ql.Date,
    maturity_date: ql.Date,
    fixed_rate: float,
    fixed_leg_tenor: ql.Period,  # e.g., 28D
    fixed_leg_convention: int,  # ql.ModifiedFollowing
    fixed_leg_daycount: ql.DayCounter,  # ql.Actual360()
    on_index: ql.OvernightIndex,  # FTIIE overnight index
    curve: ql.YieldTermStructureHandle,
) -> ql.OvernightIndexedSwap:
    # Consistent evaluation settings (no ‘today’ leakage)
    ql.Settings.instance().evaluationDate = calculation_date
    ql.Settings.instance().includeReferenceDateEvents = False
    ql.Settings.instance().enforceTodaysHistoricFixings = False

    cal = on_index.fixingCalendar()

    # -------- Spot start (T+1 Mexico) with tenor preservation --------
    fixing = cal.adjust(calculation_date, ql.Following)
    while not on_index.isValidFixingDate(fixing):
        fixing = cal.advance(fixing, 1, ql.Days)
    spot_start = on_index.valueDate(fixing)

    start_shifted = start_date <= calculation_date
    eff_start = start_date if not start_shifted else spot_start

    if start_shifted:
        # shift end by the same calendar-day offset
        try:
            day_offset = int(eff_start - start_date)
        except Exception:
            day_offset = eff_start.serialNumber() - start_date.serialNumber()
        eff_end = cal.advance(maturity_date, int(day_offset), ql.Days)
    else:
        eff_end = maturity_date

    if eff_end <= eff_start:
        eff_end = cal.advance(eff_start, fixed_leg_tenor)

    fixed_sched = ql.Schedule(
        eff_start,
        eff_end,
        fixed_leg_tenor,
        cal,
        fixed_leg_convention,
        fixed_leg_convention,
        ql.DateGeneration.Forward,
        False,
    )

    ois = ql.OvernightIndexedSwap(
        ql.OvernightIndexedSwap.Payer,
        notional,
        fixed_sched,
        fixed_rate,
        fixed_leg_daycount,
        on_index,
    )
    ois.setPricingEngine(ql.DiscountingSwapEngine(curve))
    return ois


import math


# --- add in: src/pricing_models/swap_pricer.py ---
def debug_swap_coupons(
    swap: ql.VanillaSwap,
    curve: ql.YieldTermStructureHandle,
    ibor_index: ql.IborIndex,
    header: str = "[SWAP COUPON DEBUG]",
    show_past: bool = True,
    max_rows: int | None = None,
) -> None:
    """
    Print coupon-by-coupon diagnostics for fixed and floating legs:
    accrual dates, fixing date, year fractions, forward/used rate, amount, DF and PV.
    """
    print("\n" + header)
    asof = ql.Settings.instance().evaluationDate
    print(f"evalDate: {_fmt(asof)}  notional: {swap.nominal()}")
    print(
        f"index: {ibor_index.name()}  tenor: {ibor_index.tenor().length()} {ibor_index.tenor().units()}  "
        f"DC(float)={type(ibor_index.dayCounter()).__name__}"
    )

    # ---- fixed leg ----
    print("\n[FIXED LEG]")
    fixed_dc = swap.fixedDayCount()
    fixed_rate = swap.fixedRate()
    print(f"fixed rate input: {fixed_rate:.8f}   DC(fixed)={type(fixed_dc).__name__}")

    print(
        f"{'#':>3} {'accrualStart':>12} {'accrualEnd':>12} {'pay':>12} {'tau':>8} "
        f"{'df':>12} {'amount':>14} {'pv':>14}"
    )
    print("-" * 96)
    pv_fixed = 0.0
    rows = 0
    for i, cf in enumerate(swap.leg(0)):
        c: ql.FixedRateCoupon = ql.as_fixed_rate_coupon(cf)
        if (not show_past) and cf.hasOccurred(asof):
            continue
        tau = fixed_dc.yearFraction(c.accrualStartDate(), c.accrualEndDate())
        df = curve.discount(c.date())
        amt = c.amount()  # already notional * rate * tau
        pv = amt * df
        pv_fixed += pv
        print(
            f"{i:3d} {_fmt(c.accrualStartDate()):>12} {_fmt(c.accrualEndDate()):>12} {_fmt(c.date()):>12} "
            f"{tau:8.5f} {df:12.8f} {amt:14.2f} {pv:14.2f}"
        )
        rows += 1
        if max_rows and rows >= max_rows:
            break

    # ---- float leg ----
    print("\n[FLOAT LEG]")
    f_dc = ibor_index.dayCounter()
    print(f"spread: {swap.spread():.8f}   DC(float)={type(f_dc).__name__}")
    print(
        f"{'#':>3} {'fix':>12} {'accrualStart':>12} {'accrualEnd':>12} {'pay':>12} {'tau':>8} "
        f"{'hasFix':>6} {'idxUsed':>10} {'fwdCurve':>10} {'rateUsed':>10} {'df':>12} {'amount':>14} {'pv':>14}"
    )
    print("-" * 144)
    pv_float = 0.0
    rows = 0
    for i, cf in enumerate(swap.leg(1)):
        c: ql.FloatingRateCoupon = ql.as_floating_rate_coupon(cf)
        if (not show_past) and cf.hasOccurred(asof):
            continue
        tau = f_dc.yearFraction(c.accrualStartDate(), c.accrualEndDate())
        df = curve.discount(c.date())

        # curve-implied forward over this exact accrual
        df0 = curve.discount(c.accrualStartDate())
        df1 = curve.discount(c.accrualEndDate())
        fwd = (df0 / df1 - 1.0) / max(tau, 1e-12)

        # fixing available?
        has_fix = False
        idx_used = math.nan
        try:
            idx_used = ibor_index.fixing(c.fixingDate())
            has_fix = True
        except RuntimeError:
            pass

        rate_used = c.rate()  # will be fixing (if present) else forward + spread
        amt = c.amount()
        pv = amt * df
        pv_float += pv

        print(
            f"{i:3d} {_fmt(c.fixingDate()):>12} {_fmt(c.accrualStartDate()):>12} {_fmt(c.accrualEndDate()):>12} "
            f"{_fmt(c.date()):>12} {tau:8.5f} {str(has_fix):>6} "
            f"{(idx_used if has_fix else float('nan')):10.6f} {fwd:10.6f} {rate_used:10.6f} "
            f"{df:12.8f} {amt:14.2f} {pv:14.2f}"
        )

        rows += 1
        if max_rows and rows >= max_rows:
            break

    # ---- summary / par checks ----
    print("\n[PV DECOMP]")
    print(f"PV_fixed : {pv_fixed:,.2f}")
    print(f"PV_float : {pv_float:,.2f}")
    print(f"NPV (QL) : {swap.NPV():,.2f}")

    # annuity of the fixed leg (Σ tau_i * df_i on fixed pay dates)
    annuity = 0.0
    for cf in swap.leg(0):
        c = ql.as_fixed_rate_coupon(cf)
        tau = fixed_dc.yearFraction(c.accrualStartDate(), c.accrualEndDate())
        df = curve.discount(c.date())
        annuity += tau * df

    # coupon-by-coupon float PV using curve forwards (no spread)
    float_pv_curve = 0.0
    for cf in swap.leg(1):
        c = ql.as_floating_rate_coupon(cf)
        tau = f_dc.yearFraction(c.accrualStartDate(), c.accrualEndDate())
        df = curve.discount(c.date())
        df0 = curve.discount(c.accrualStartDate())
        df1 = curve.discount(c.accrualEndDate())
        fwd = (df0 / df1 - 1.0) / max(tau, 1e-12)
        float_pv_curve += swap.nominal() * (fwd + c.spread()) * tau * df

    # par fixed rate from curve coupons
    par_from_coupons = float_pv_curve / max(annuity * swap.nominal(), 1e-12)

    # also the classic (1 - DF(T)) / annuity formula (works when float = same curve, no stubs)
    # approximate float PV = notional*(DF(start) - DF(end))
    try:
        start = ql.as_floating_rate_coupon(swap.leg(1)[0]).accrualStartDate()
        end = ql.as_floating_rate_coupon(swap.leg(1)[-1]).accrualEndDate()
        par_alt = (curve.discount(start) - curve.discount(end)) / max(annuity, 1e-12)
    except Exception:
        par_alt = float("nan")

    print(f"Annuity (fixed)           : {annuity:,.8f}")
    print(f"Par (from curve coupons)  : {par_from_coupons:,.8f}")
    print(f"Par (alt 1-DF/annuity)    : {par_alt:,.8f}")
    print(f"Par (QL fairRate)         : {swap.fairRate():,.8f}")
    print()


def _fmt(qld: ql.Date) -> str:
    return f"{qld.year():04d}-{qld.month():02d}-{qld.dayOfMonth():02d}"


def debug_tiie_zero_curve(
    calculation_date: ql.Date,
    curve: ql.YieldTermStructureHandle,
    cal: ql.Calendar | None = None,
    day_count: ql.DayCounter | None = None,
    sample_months: list[int] | None = None,
    header: str = "[TIIE ZERO CURVE DEBUG]",
) -> None:
    """
    Print a readable snapshot of the zero curve: sample maturities, DFs and zero rates.
    """
    print("\n" + header)
    ql.Settings.instance().evaluationDate = calculation_date
    cal = cal or (ql.Mexico() if hasattr(ql, "Mexico") else ql.TARGET())
    dc = day_count or ql.Actual360()

    asof = calculation_date
    print(f"asof (evalDate): {_fmt(asof)}")
    try:
        link = curve.currentLink()
        print(
            f"curve link: {type(link).__name__} (extrap={'Yes' if link.allowsExtrapolation() else 'No'})"
        )
    except Exception:
        pass

    # sampling grid
    if sample_months is None:
        sample_months = [1, 2, 3, 6, 9, 12, 18, 24, 36, 48, 60, 84]  # up to 7Y

    print(f"{'m (M)':>6} {'date':>12} {'T(yr)':>8} {'DF':>12} {'Zero(%)':>10}")
    print("-" * 52)
    for m in sample_months:
        d = cal.advance(asof, ql.Period(m, ql.Months))
        T = dc.yearFraction(asof, d)
        df = curve.discount(d)
        z = curve.zeroRate(d, dc, ql.Continuous, ql.Annual).rate() * 100.0
        print(f"{m:6d} {_fmt(d):>12} {T:8.5f} {df:12.8f} {z:10.4f}")
    print()


def debug_tiie_trade(
    valuation_date: ql.Date,
    swap: ql.VanillaSwap,
    curve: ql.YieldTermStructureHandle,
    ibor_index: ql.IborIndex,
) -> None:
    """
    Convenience wrapper: dump curve snapshot and full coupon drilldown for a TIIE swap.
    """
    debug_tiie_zero_curve(valuation_date, curve)
    debug_swap_coupons(swap, curve, ibor_index)
