import datetime
from typing import Literal

import QuantLib as ql
from pydantic import Field, PrivateAttr

from mainsequence.instruments.pricing_models.fx_option_pricer import get_fx_market_data
from mainsequence.instruments.pricing_models.knockout_fx_pricer import create_knockout_fx_option
from mainsequence.instruments.utils import to_ql_date

from .base_instrument import InstrumentModel


class KnockOutFXOption(InstrumentModel):
    """
    Knock-out FX option - a path-dependent option that becomes worthless
    if the underlying FX rate hits the barrier level during the option's life.
    """

    currency_pair: str = Field(
        ..., description="Currency pair in format 'EURUSD', 'GBPUSD', etc. (6 characters)."
    )
    strike: float = Field(
        ..., description="Option strike price (domestic currency per unit of foreign currency)."
    )
    barrier: float = Field(
        ..., description="Barrier level - option is knocked out if FX rate hits this level."
    )
    maturity: datetime.date = Field(..., description="Option expiration date.")
    option_type: Literal["call", "put"] = Field(..., description="Option type: 'call' or 'put'.")
    barrier_type: Literal["up_and_out", "down_and_out"] = Field(
        ...,
        description="Barrier type: 'up_and_out' (knocked out if rate goes above barrier) or 'down_and_out' (knocked out if rate goes below barrier).",
    )
    notional: float = Field(..., description="Notional amount in foreign currency units.")
    rebate: float = Field(
        default=0.0, description="Rebate paid if option is knocked out (default: 0.0)."
    )

    # Allow QuantLib types & keep runtime attrs out of the schema
    model_config = {"arbitrary_types_allowed": True}

    # Runtime-only QuantLib objects
    _option: ql.BarrierOption | None = PrivateAttr(default=None)
    _engine: ql.PricingEngine | None = PrivateAttr(default=None)

    def _setup_pricing_components(self) -> None:
        """Set up the QuantLib pricing components for the knock-out FX option."""
        # 1) Validate currency pair format
        if len(self.currency_pair) != 6:
            raise ValueError("Currency pair must be 6 characters (e.g., 'EURUSD')")

        # 2) Validate barrier logic
        market_data = get_fx_market_data(self.currency_pair, self.valuation_date)
        spot_fx = market_data["spot_fx_rate"]

        if self.barrier_type == "up_and_out" and self.barrier <= spot_fx:
            raise ValueError(
                "For up-and-out barrier, barrier level must be above current spot rate"
            )
        elif self.barrier_type == "down_and_out" and self.barrier >= spot_fx:
            raise ValueError(
                "For down-and-out barrier, barrier level must be below current spot rate"
            )

        # 3) Convert dates to QuantLib format
        ql_calc = to_ql_date(self.valuation_date)
        ql_mty = to_ql_date(self.maturity)
        ql.Settings.instance().evaluationDate = ql_calc

        # 4) Create the barrier option using the specialized pricer
        self._option, self._engine = create_knockout_fx_option(
            currency_pair=self.currency_pair,
            calculation_date=ql_calc,
            maturity_date=ql_mty,
            strike=self.strike,
            barrier=self.barrier,
            option_type=self.option_type,
            barrier_type=self.barrier_type,
            rebate=self.rebate,
        )

    def price(self) -> float:
        """Calculate the knock-out option price (NPV)."""
        if not self._option:
            self._setup_pricing_components()
        # Return price multiplied by notional
        return float(self._option.NPV() * self.notional)

    def get_greeks(self) -> dict:
        """Calculate the option Greeks."""
        if not self._option:
            self._setup_pricing_components()

        # Ensure calculations are performed
        npv = self._option.NPV()

        return {
            "delta": self._option.delta() * self.notional,
            "gamma": self._option.gamma() * self.notional,
            "vega": self._option.vega() * self.notional / 100.0,  # Convert to 1% vol change
            "theta": self._option.theta() * self.notional / 365.0,  # Convert to per day
            "rho_domestic": self._option.rho() * self.notional / 100.0,  # Convert to 1% rate change
        }

    def get_market_info(self) -> dict:
        """Get the market data used for pricing."""
        market_data = get_fx_market_data(self.currency_pair, self.valuation_date)
        foreign_ccy = self.currency_pair[:3]
        domestic_ccy = self.currency_pair[3:]

        return {
            "currency_pair": self.currency_pair,
            "foreign_currency": foreign_ccy,
            "domestic_currency": domestic_ccy,
            "spot_fx_rate": market_data["spot_fx_rate"],
            "volatility": market_data["volatility"],
            "domestic_rate": market_data["domestic_rate"],
            "foreign_rate": market_data["foreign_rate"],
            "barrier": self.barrier,
            "barrier_type": self.barrier_type,
            "rebate": self.rebate,
        }

    def get_barrier_info(self) -> dict:
        """Get information about the barrier and current market position."""
        market_data = get_fx_market_data(self.currency_pair, self.valuation_date)
        spot_fx = market_data["spot_fx_rate"]

        if self.barrier_type == "up_and_out":
            distance_to_barrier = (self.barrier - spot_fx) / spot_fx
            barrier_status = "Active" if spot_fx < self.barrier else "Knocked Out"
        else:  # down_and_out
            distance_to_barrier = (spot_fx - self.barrier) / spot_fx
            barrier_status = "Active" if spot_fx > self.barrier else "Knocked Out"

        return {
            "barrier_level": self.barrier,
            "barrier_type": self.barrier_type,
            "current_spot": spot_fx,
            "distance_to_barrier_pct": distance_to_barrier * 100,
            "barrier_status": barrier_status,
            "rebate": self.rebate,
        }
