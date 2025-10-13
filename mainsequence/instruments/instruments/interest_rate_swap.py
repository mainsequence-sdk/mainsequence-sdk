from __future__ import annotations

import datetime
from typing import Any

import pandas as pd
import QuantLib as ql
from pydantic import Field, PrivateAttr

from mainsequence.instruments.pricing_models.indices import build_zero_curve, get_index
from mainsequence.instruments.pricing_models.swap_pricer import (
    get_swap_cashflows,
    price_vanilla_swap_with_curve,
)
from mainsequence.instruments.utils import to_py_date, to_ql_date

from .base_instrument import InstrumentModel
from .ql_fields import (
    QuantLibBDC as QBDC,
)
from .ql_fields import (
    QuantLibDayCounter as QDayCounter,
)
from .ql_fields import (
    QuantLibPeriod as QPeriod,
)


class InterestRateSwap(InstrumentModel):
    """Plain-vanilla fixed-for-floating interest rate swap.

    Indices are referenced by NAME (string) to keep the model stateless/JSON-friendly.
    The actual QuantLib index is materialized lazily after set_valuation_date().
    """

    # ---- core economics ----
    notional: float = Field(...)
    start_date: datetime.date = Field(...)
    maturity_date: datetime.date = Field(...)
    fixed_rate: float = Field(...)

    # ---- fixed leg ----
    fixed_leg_tenor: QPeriod = Field(...)
    fixed_leg_convention: QBDC = Field(...)
    fixed_leg_daycount: QDayCounter = Field(...)

    # ---- floating leg ----
    float_leg_tenor: QPeriod = Field(...)
    float_leg_spread: float = Field(...)
    float_leg_index_name: str = Field()

    tenor: ql.Period | None = Field(
        default=None,
        description="If set (e.g. ql.Period('156W')), maturity is start + tenor using spot start (T+1).",
    )

    model_config = {"arbitrary_types_allowed": True}

    # runtime-only
    _swap: ql.VanillaSwap | None = PrivateAttr(default=None)
    _float_leg_index: ql.IborIndex | None = PrivateAttr(default=None)

    # ---------- convenience access to runtime index (NOT serialized) ----------
    @property
    def float_leg_ibor_index(self) -> ql.IborIndex | None:
        return self._float_leg_index

    # ---------- lifecycle ----------
    def _ensure_index(self) -> None:
        if self._float_leg_index is not None:
            return
        if self.valuation_date is None:
            raise ValueError("Set valuation_date before pricing: set_valuation_date(dt).")
        # Date-aware registry (TIIE picks Valmer curve by default)
        self._float_leg_index = get_index(
            self.float_leg_index_name,
            target_date=self.valuation_date,
            hydrate_fixings=True,
        )

    def _reset_runtime(self) -> None:
        self._swap = None

    def _on_valuation_date_set(self) -> None:
        self._float_leg_index = None
        self._reset_runtime()

    # Optional: allow injecting a custom curve for the float leg
    def reset_curve(self, curve: ql.YieldTermStructure) -> None:
        if self.valuation_date is None:
            raise ValueError("Set valuation_date before reset_curve().")
        self._float_leg_index = get_index(
            self.float_leg_index_name,
            target_date=self.valuation_date,
            forwarding_curve=ql.YieldTermStructureHandle(curve),
            hydrate_fixings=True,
        )
        self._swap = None

    # ---------- pricing ----------
    def _setup_pricer(self) -> None:
        """Sets up the initial pricer using the default Valmer curve."""
        if self._swap is not None:
            return
        assert self.valuation_date is not None
        # Build the default  curve.
        self._ensure_index()
        default_curve = build_zero_curve(
            target_date=self.valuation_date,
            index_identifier=self.float_leg_ibor_index.familyName(),
        )

        # Call the common swap construction logic.
        self._build_swap(default_curve)

    def price(self) -> float:
        self._setup_pricer()
        return float(self._swap.NPV())

    def get_cashflows(self) -> dict[str, list[dict[str, Any]]]:
        self._setup_pricer()
        return get_swap_cashflows(self._swap)

    def get_net_cashflows(self) -> pd.Series:
        cashflows = self.get_cashflows()
        fixed_df = pd.DataFrame(cashflows["fixed"]).set_index("payment_date")
        float_df = pd.DataFrame(cashflows["floating"]).set_index("payment_date")
        joint = fixed_df.index.union(float_df.index)
        fixed_df = fixed_df.reindex(joint).fillna(0.0)
        float_df = float_df.reindex(joint).fillna(0.0)
        net = fixed_df["amount"] - float_df["amount"]
        net.name = "net_cashflow"
        return net

    def _build_swap(self, curve: ql.YieldTermStructure) -> None:
        """
        Private helper method to construct the QuantLib swap object.
        This contains the common logic previously duplicated in _setup_pricer and reset_curve.
        """
        ql_val = to_ql_date(self.valuation_date)
        ql.Settings.instance().evaluationDate = ql_val
        ql.Settings.instance().includeReferenceDateEvents = False
        ql.Settings.instance().enforceTodaysHistoricFixings = True

        cal = self.float_leg_ibor_index.fixingCalendar()

        # 3) Effective end
        if self.tenor is not None:
            eff_end = cal.advance(self.start_date, self.tenor)
        else:
            eff_end = to_ql_date(self.maturity_date)

        # 4) Price vanilla IRS using the schedule and the provided curve
        self._swap = price_vanilla_swap_with_curve(
            calculation_date=ql_val,
            notional=self.notional,
            start_date=to_ql_date(self.start_date),
            maturity_date=eff_end,
            fixed_rate=self.fixed_rate,
            fixed_leg_tenor=self.fixed_leg_tenor,
            fixed_leg_convention=self.fixed_leg_convention,
            fixed_leg_daycount=self.fixed_leg_daycount,
            float_leg_tenor=self.float_leg_tenor,
            float_leg_spread=self.float_leg_spread,
            ibor_index=self.float_leg_ibor_index,
            curve=curve,
        )

    # ---------- FACTORY: TIIE(28D) swap --------------------------------------
    @classmethod
    def from_tiie(
        cls,
        *,
        notional: float,
        start_date: datetime.date,
        fixed_rate: float,
        float_leg_spread: float = 0.0,
        # choose exactly one of (tenor, maturity_date)
        tenor: ql.Period | None = None,
        maturity_date: datetime.date | None = None,
    ) -> InterestRateSwap:
        """
        Build a MXN TIIE(28D) IRS with standard conventions:

        - Fixed leg: tenor=28D, daycount=ACT/360, BDC=ModifiedFollowing
        - Float leg: tenor=28D, index name='TIIE_28D'
        - Effective start: T+1 adjusted on Mexico calendar from trade_date
        - Maturity: eff_start + tenor (if tenor provided) OR explicit maturity_date
        """
        if (tenor is None) == (maturity_date is None):
            raise ValueError("Provide exactly one of 'tenor' or 'maturity_date'.")

        cal = ql.Mexico() if hasattr(ql, "Mexico") else ql.TARGET()
        eff_start_ql = cal.adjust(to_ql_date(start_date), ql.Following)
        eff_start = to_py_date(eff_start_ql)

        if tenor is not None:
            eff_end_ql = cal.advance(eff_start_ql, tenor)
            maturity = to_py_date(eff_end_ql)
        else:
            maturity = maturity_date  # type: ignore[assignment]

        return cls(
            notional=notional,
            start_date=eff_start,
            maturity_date=maturity,
            fixed_rate=fixed_rate,
            fixed_leg_tenor=ql.Period("28D"),
            fixed_leg_convention=ql.ModifiedFollowing,
            fixed_leg_daycount=ql.Actual360(),
            float_leg_tenor=ql.Period("28D"),
            float_leg_spread=float_leg_spread,
            float_leg_index_name="TIIE_28D",
        )
