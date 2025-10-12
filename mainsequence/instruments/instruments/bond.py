# mainsequence/instruments/instruments/bond.py
import datetime
from typing import Optional, Dict, Any, List

import QuantLib as ql
from pydantic import Field, PrivateAttr

from .base_instrument import InstrumentModel
from .ql_fields import (
    QuantLibPeriod as QPeriod,
    QuantLibDayCounter as QDayCounter,
    QuantLibCalendar as QCalendar,
    QuantLibBDC as QBDC,
    QuantLibSchedule as QSchedule,
)
from mainsequence.instruments.utils import to_ql_date, to_py_date
from mainsequence.instruments.pricing_models.bond_pricer import (
    create_floating_rate_bond_with_curve,
)
from mainsequence.instruments.pricing_models.indices import get_index


class Bond(InstrumentModel):
    """
    Shared pricing lifecycle for vanilla bonds.

    Subclasses must implement:
      - _get_default_discount_curve(): Optional[ql.YieldTermStructureHandle]
      - _create_bond(discount_curve: ql.YieldTermStructureHandle) -> ql.Bond
        (return a ql.FixedRateBond or ql.FloatingRateBond, etc. *without* assuming any global state)
    """

    face_value: float = Field(...)
    issue_date: datetime.date = Field(...)
    maturity_date: datetime.date = Field(...)
    coupon_frequency: QPeriod = Field(...)
    day_count: QDayCounter = Field(...)
    calendar: QCalendar = Field(default_factory=ql.TARGET)
    business_day_convention: QBDC = Field(default=ql.Following)
    settlement_days: int = Field(default=2)
    schedule: Optional[QSchedule] = Field(None)

    benchmark_rate_index_name: Optional[str] = Field(...,description="A default index benchmark rate, helpful when doing"
                                                                     "analysis and we want to  map the bond to a bencharmk for example to"
                                                                     "the SOFR Curve or to de US Treasury curve etc")

    model_config = {"arbitrary_types_allowed": True}

    _bond: Optional[ql.Bond] = PrivateAttr(default=None)
    _with_yield: Optional[float] = PrivateAttr(default=None)

    # ---- valuation lifecycle ----
    def _on_valuation_date_set(self) -> None:
        self._bond = None
        self._with_yield = None

    # ---- hooks for subclasses ----
    def _get_default_discount_curve(self) -> Optional[ql.YieldTermStructureHandle]:
        """
        Subclasses return a curve if they have one (e.g., floating uses its index curve),
        or None if they require with_yield or an explicitly supplied handle.
        """
        return None

    def _create_bond(self, discount_curve: Optional[ql.YieldTermStructureHandle]) -> ql.Bond:

        """Subclasses must create and return a QuantLib bond (Fixed or Floating).
        discount_curve may be None: subclasses must not assume it is present for cashflow-only usage."""
        raise NotImplementedError

    def _ensure_instrument(self) -> None:
        if self.valuation_date is None:
            raise ValueError("Set valuation_date before building instrument: set_valuation_date(dt).")

        ql_calc_date = to_ql_date(self.valuation_date)
        ql.Settings.instance().evaluationDate = ql_calc_date
        ql.Settings.instance().includeReferenceDateEvents = False
        ql.Settings.instance().enforceTodaysHistoricFixings = False

        # Build only if not already built
        if self._bond is None:
            self._bond = self._create_bond(None)  # << NO discount curve required here

    # ---- internal helpers ----
    def _resolve_discount_curve(
        self, with_yield: Optional[float]
    ) -> ql.YieldTermStructureHandle:
        """
        Priority:
          1) If with_yield provided -> build a flat curve off that yield.
          2) Otherwise, use subclass-provided default curve.
        """
        ql_calc_date = to_ql_date(self.valuation_date)

        if with_yield is not None:
            # Compounded Annual for YTM-style flat curves; day_count from instrument
            flat = ql.FlatForward(
                ql_calc_date, with_yield, self.day_count, ql.Compounded, ql.Annual
            )
            return ql.YieldTermStructureHandle(flat)

        default = self._get_default_discount_curve()
        if default is None:
            raise ValueError(
                "No discount curve available. Either pass with_yield=... to price(), "
                "or the instrument must supply a default discount curve."
            )
        return default

    def _setup_pricer(self, with_yield: Optional[float] = None) -> None:
        if self.valuation_date is None:
            raise ValueError("Set valuation_date before pricing: set_valuation_date(dt).")

        ql_calc_date = to_ql_date(self.valuation_date)
        ql.Settings.instance().evaluationDate = ql_calc_date
        ql.Settings.instance().includeReferenceDateEvents = False
        ql.Settings.instance().enforceTodaysHistoricFixings = False

        # Build or rebuild only when needed
        if self._bond is None or self._with_yield != with_yield:
            discount_curve = self._resolve_discount_curve(with_yield)
            bond = self._create_bond(discount_curve)
            # Ensure engine is attached (safe even if subclass already set one)
            bond.setPricingEngine(ql.DiscountingBondEngine(discount_curve))
            self._bond = bond
            self._with_yield = with_yield

    # ---- public API shared by all vanilla bonds ----
    def price(self, with_yield: Optional[float] = None) -> float:
        self._setup_pricer(with_yield=with_yield)
        return float(self._bond.NPV())

    def analytics(self, with_yield: Optional[float] = None) -> dict:
        self._setup_pricer(with_yield=with_yield)
        _ = self._bond.NPV()
        return {
            "clean_price": self._bond.cleanPrice(),
            "dirty_price": self._bond.dirtyPrice(),
            "accrued_amount": self._bond.accruedAmount(),
        }

    def get_cashflows(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Generic cashflow extractor.
        For fixed bonds, you'll see "fixed" + "redemption".
        For floaters, you'll see "floating" + "redemption".
        """
        self._setup_pricer()
        ql.Settings.instance().evaluationDate = to_ql_date(self.valuation_date)

        out: Dict[str, List[Dict[str, Any]]] = {"fixed": [], "floating": [], "redemption": []}

        for cf in self._bond.cashflows():
            if cf.hasOccurred():
                continue

            f_cpn = ql.as_floating_rate_coupon(cf)
            if f_cpn is not None:
                out["floating"].append({
                    "payment_date": to_py_date(f_cpn.date()),
                    "fixing_date": to_py_date(f_cpn.fixingDate()),
                    "rate": float(f_cpn.rate()),
                    "spread": float(f_cpn.spread()),
                    "amount": float(f_cpn.amount()),
                })
                continue

            x_cpn = ql.as_fixed_rate_coupon(cf)
            if x_cpn is not None:
                out["fixed"].append({
                    "payment_date": to_py_date(x_cpn.date()),
                    "rate": float(x_cpn.rate()),
                    "amount": float(x_cpn.amount()),
                })
                continue

            # Redemption/principal
            out["redemption"].append({
                "payment_date": to_py_date(cf.date()),
                "amount": float(cf.amount()),
            })

        # Trim empty legs to stay tidy
        return {k: v for k, v in out.items() if len(v) > 0}

    def get_cashflows_df(self):
        """Convenience dataframe with coupon + redemption aligned."""
        self._ensure_instrument()  # << build-only; no curve/yield needed

        import pandas as pd
        cfs = self.get_cashflows()
        legs = [k for k in ("fixed", "floating") if k in cfs]
        if not legs and "redemption" not in cfs:
            return pd.DataFrame()

        # build coupon df
        df_cpn = None
        for leg in legs:
            df_leg = pd.DataFrame(cfs[leg]) if len(cfs[leg]) else pd.DataFrame(columns=["payment_date", "amount"])
            if not df_leg.empty:
                df_leg = df_leg[["payment_date", "amount"]].set_index("payment_date")
            if df_cpn is None:
                df_cpn = df_leg
            else:
                # if both fixed and floating exist (exotics), sum them
                df_cpn = df_cpn.add(df_leg, fill_value=0.0)

        df_red = pd.DataFrame(cfs.get("redemption", []))
        if not df_red.empty:
            df_red = df_red.set_index("payment_date")[["amount"]]

        if df_cpn is None and df_red is None:
            return pd.DataFrame()

        if df_cpn is None:
            df_out = df_red.rename(columns={"amount": "net_cashflow"})
        elif df_red is None or df_red.empty:
            df_out = df_cpn.rename(columns={"amount": "net_cashflow"})
        else:
            idx = df_cpn.index.union(df_red.index)
            df_cpn = df_cpn.reindex(idx).fillna(0.0)
            df_red = df_red.reindex(idx).fillna(0.0)
            df_out = (df_cpn["amount"] + df_red["amount"]).to_frame("net_cashflow")

        return df_out

    def get_net_cashflows(self):
        """Shorthand Series of combined coupon + redemption."""
        df = self.get_cashflows_df()
        return df["net_cashflow"] if "net_cashflow" in df.columns else df.squeeze()

    def get_yield(self, override_clean_price: Optional[float] = None) -> float:
        """
        Yield-to-maturity based on current clean price (or override), compounded annually.
        """
        self._setup_pricer()
        ql.Settings.instance().evaluationDate = to_ql_date(self.valuation_date)

        clean_price = override_clean_price if override_clean_price is not None else self._bond.cleanPrice()
        freq: ql.Frequency = self.coupon_frequency.frequency()
        settlement: ql.Date = self._bond.settlementDate()

        ytm = self._bond.bondYield(
            clean_price,
            self.day_count,
            ql.Compounded,
            freq,
            settlement
        )
        return float(ytm)

    def get_ql_bond(
            self,
            *,
            build_if_needed: bool = True,
            with_yield: Optional[float] = None
    ) -> ql.Bond:
        """
        Safely access the underlying QuantLib bond.
        If you don't pass a yield and there is no default curve, we build without an engine.
        """
        if self.valuation_date is None:
            raise ValueError("Set valuation_date before accessing the QuantLib bond (set_valuation_date(dt)).")

        if build_if_needed:
            # If caller gave a yield OR we have a default curve, do full pricing setup.
            if with_yield is not None or self._get_default_discount_curve() is not None:
                self._setup_pricer(with_yield=with_yield if with_yield is not None else self._with_yield)
            else:
                # No curve, no yield -> build instrument only (good for fixed cashflows)
                self._ensure_instrument()

        if self._bond is None:
            raise RuntimeError(
                "Underlying QuantLib bond is not available. "
                "Call price()/analytics() first or use get_ql_bond(build_if_needed=True, "
                "with_yield=...) to build it."
            )
        return self._bond


class FixedRateBond(Bond):
    """Plain-vanilla fixed-rate bond following the shared Bond lifecycle."""

    coupon_rate: float = Field(...)

    model_config = {"arbitrary_types_allowed": True}

    # Optional market curve if you want to discount off a curve instead of a flat yield
    _discount_curve: Optional[ql.YieldTermStructureHandle] = PrivateAttr(default=None)

    def reset_curve(self, curve: ql.YieldTermStructureHandle) -> None:
        self._discount_curve = curve
    
    def _get_default_discount_curve(self) -> Optional[ql.YieldTermStructureHandle]:
        return self._discount_curve

    def _build_schedule(self) -> ql.Schedule:
        if self.schedule is not None:
            return self.schedule
        return ql.Schedule(
            to_ql_date(self.issue_date),
            to_ql_date(self.maturity_date),
            self.coupon_frequency,
            self.calendar,
            self.business_day_convention,
            self.business_day_convention,
            ql.DateGeneration.Forward,
            False
        )

    def _create_bond(self, discount_curve: Optional[ql.YieldTermStructureHandle]) -> ql.Bond:
        ql.Settings.instance().evaluationDate = to_ql_date(self.valuation_date)
        sched = self._build_schedule()

        dates = list(sched.dates())
        asof = ql.Settings.instance().evaluationDate
        has_periods_left = len(dates) >= 2 and any(dates[i + 1] > asof for i in range(len(dates) - 1))
        if not has_periods_left:
            maturity = dates[-1] if dates else to_ql_date(self.maturity_date)
            return ql.ZeroCouponBond(
                self.settlement_days,
                self.calendar,
                self.face_value,
                maturity,
                self.business_day_convention,
                100.0,
                to_ql_date(self.issue_date),
            )

        return ql.FixedRateBond(
            self.settlement_days,
            self.face_value,
            sched,
            [self.coupon_rate],
            self.day_count
        )

class FloatingRateBond(Bond):
    """Floating-rate bond with specified floating rate index (backward compatible)."""

    face_value: float = Field(...)
    floating_rate_index_name: str = Field(...)
    spread: float = Field(default=0.0)
    # All other fields (issue_date, maturity_date, coupon_frequency, day_count, calendar, etc.)
    # are inherited from Bond

    model_config = {"arbitrary_types_allowed": True}

    _bond: Optional[ql.FloatingRateBond] = PrivateAttr(default=None)
    _index: Optional[ql.IborIndex] = PrivateAttr(default=None)
    _with_yield: Optional[float] = PrivateAttr(default=None)

    # ---------- lifecycle ----------
    def _ensure_index(self) -> None:
        if self._index is not None:
            return
        if self.valuation_date is None:
            raise ValueError("Set valuation_date before pricing: set_valuation_date(dt).")
        self._index = get_index(
            self.floating_rate_index_name,
            target_date=self.valuation_date,
            hydrate_fixings=True,
        )

    def _on_valuation_date_set(self) -> None:
        super()._on_valuation_date_set()
        self._index = None

    def reset_curve(self, curve: ql.YieldTermStructureHandle) -> None:
        """Optional: re-link a custom curve to this index and rebuild."""
        if self.valuation_date is None:
            raise ValueError("Set valuation_date before reset_curve().")

        self._index = get_index(
            self.floating_rate_index_name,
            target_date=self.valuation_date,
            forwarding_curve=curve,
            hydrate_fixings=True,
        )

        private = ql.RelinkableYieldTermStructureHandle()
        link = curve.currentLink() if hasattr(curve, "currentLink") else curve
        private.linkTo(link)
        self._index = self._index.clone(private)

        # Force rebuild on next price()
        self._bond = None
        self._with_yield = None

    # ---- Bond hooks ----
    def _get_default_discount_curve(self) -> Optional[ql.YieldTermStructureHandle]:
        self._ensure_index()
        # Forecasting and (by default) discounting off the index curve for compatibility
        return self._index.forwardingTermStructure()

    def _create_bond(self, discount_curve: Optional[ql.YieldTermStructureHandle]) -> ql.Bond:
        self._ensure_index()
        ql_calc_date = to_ql_date(self.valuation_date)
        forecasting = self._index.forwardingTermStructure()

        return create_floating_rate_bond_with_curve(
            calculation_date=ql_calc_date,
            face=self.face_value,
            issue_date=to_ql_date(self.issue_date),
            maturity_date=to_ql_date(self.maturity_date),
            floating_rate_index=self._index,
            spread=self.spread,
            coupon_frequency=self.coupon_frequency,
            day_count=self.day_count,
            calendar=self.calendar,
            business_day_convention=self.business_day_convention,
            settlement_days=self.settlement_days,
            curve=forecasting,
            discount_curve=discount_curve,  # may be None (OK)
            seed_past_fixings_from_curve=True,
            schedule=self.schedule
        )

    # ---------- public API (kept for backward compatibility) ----------
    def get_index_curve(self):
        self._ensure_index()
        return self._index.forwardingTermStructure()

    # price(with_yield) and analytics(with_yield) are inherited from Bond and remain compatible

    def get_cashflows(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Keep the original floater-specific structure (floating + redemption).
        """
        self._setup_pricer()
        ql.Settings.instance().evaluationDate = to_ql_date(self.valuation_date)

        out: Dict[str, List[Dict[str, Any]]] = {"floating": [], "redemption": []}

        for cf in self._bond.cashflows():
            if cf.hasOccurred():
                continue

            cpn = ql.as_floating_rate_coupon(cf)
            if cpn is not None:
                out["floating"].append({
                    "payment_date": to_py_date(cpn.date()),
                    "fixing_date": to_py_date(cpn.fixingDate()),
                    "rate": float(cpn.rate()),
                    "spread": float(cpn.spread()),
                    "amount": float(cpn.amount()),
                })
            else:
                out["redemption"].append({
                    "payment_date": to_py_date(cf.date()),
                    "amount": float(cf.amount()),
                })

        return out