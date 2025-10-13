import datetime
from typing import Literal

import QuantLib as ql
from pydantic import Field, PrivateAttr

from mainsequence.instruments.pricing_models.fx_option_pricer import (
    create_fx_garman_kohlhagen_model,
    get_fx_market_data,
)
from mainsequence.instruments.utils import to_ql_date

from .base_instrument import InstrumentModel


class VanillaFXOption(InstrumentModel):
    """Vanilla FX option priced with Garman-Kohlhagen model."""

    currency_pair: str = Field(
        ..., description="Currency pair in format 'EURUSD', 'GBPUSD', etc. (6 characters)."
    )
    strike: float = Field(
        ..., description="Option strike price (domestic currency per unit of foreign currency)."
    )
    maturity: datetime.date = Field(..., description="Option expiration date.")
    option_type: Literal["call", "put"] = Field(..., description="Option type: 'call' or 'put'.")
    notional: float = Field(..., description="Notional amount in foreign currency units.")

    # Allow QuantLib types & keep runtime attrs out of the schema
    model_config = {"arbitrary_types_allowed": True}

    # Runtime-only QuantLib objects
    _option: ql.VanillaOption | None = PrivateAttr(default=None)
    _engine: ql.PricingEngine | None = PrivateAttr(default=None)

    def _setup_pricing_components(self) -> None:
        """Set up the QuantLib pricing components for the FX option."""
        # 1) Validate currency pair format
        if len(self.currency_pair) != 6:
            raise ValueError("Currency pair must be 6 characters (e.g., 'EURUSD')")

        # 2) Get FX market data
        market_data = get_fx_market_data(self.currency_pair, self.valuation_date)
        spot_fx = market_data["spot_fx_rate"]
        vol = market_data["volatility"]
        domestic_rate = market_data["domestic_rate"]
        foreign_rate = market_data["foreign_rate"]

        # 3) Convert dates to QuantLib format
        ql_calc = to_ql_date(self.valuation_date)
        ql_mty = to_ql_date(self.maturity)
        ql.Settings.instance().evaluationDate = ql_calc

        # 4) Create Garman-Kohlhagen process
        process = create_fx_garman_kohlhagen_model(
            ql_calc, spot_fx, vol, domestic_rate, foreign_rate
        )

        # 5) Create instrument and engine
        payoff = ql.PlainVanillaPayoff(
            ql.Option.Call if self.option_type == "call" else ql.Option.Put, self.strike
        )
        exercise = ql.EuropeanExercise(ql_mty)
        self._option = ql.VanillaOption(payoff, exercise)
        self._engine = ql.AnalyticEuropeanEngine(process)
        self._option.setPricingEngine(self._engine)

    def price(self) -> float:
        """Calculate the option price (NPV)."""
        if not self._option:
            self._setup_pricing_components()
        # Return price multiplied by notional
        return float(self._option.NPV() * self.notional)

    def get_greeks(self) -> dict:
        """Calculate the option Greeks."""
        if not self._option:
            self._setup_pricing_components()
            self._option.NPV()  # Ensure calculations are performed

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
        }
