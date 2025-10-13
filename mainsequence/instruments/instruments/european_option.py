import datetime
from typing import Literal

import QuantLib as ql
from pydantic import Field, PrivateAttr

from mainsequence.instruments.data_interface import DateInfo, data_interface
from mainsequence.instruments.pricing_models.black_scholes import create_bsm_model
from mainsequence.instruments.utils import to_ql_date

from .base_instrument import InstrumentModel


class EuropeanOption(InstrumentModel):
    """European option priced with Black–Scholes–Merton."""

    underlying: str = Field(
        ..., description="Ticker/identifier of the underlying asset (e.g., 'SPY')."
    )
    strike: float = Field(..., description="Option strike price (in underlying currency units).")
    maturity: datetime.date = Field(..., description="Option expiration date.")
    option_type: Literal["call", "put"] = Field(..., description="Option type: 'call' or 'put'.")

    # Allow QuantLib types & keep runtime attrs out of the schema
    model_config = {"arbitrary_types_allowed": True}

    # Runtime-only QuantLib objects
    _option: ql.VanillaOption | None = PrivateAttr(default=None)
    _engine: ql.PricingEngine | None = PrivateAttr(default=None)

    def _setup_pricing_components(self) -> None:
        # 1) market data
        asset_range_map = {self.underlying: DateInfo(start_date=self.valuation_date)}
        md = data_interface.get_historical_data("equities_daily", asset_range_map)
        spot, vol, r, q = (
            md["spot_price"],
            md["volatility"],
            md["risk_free_rate"],
            md["dividend_yield"],
        )

        # 2) dates
        ql_calc = to_ql_date(self.valuation_date)
        ql_mty = to_ql_date(self.maturity)
        ql.Settings.instance().evaluationDate = ql_calc

        # 3) model
        process = create_bsm_model(ql_calc, spot, vol, r, q)

        # 4) instrument + engine
        payoff = ql.PlainVanillaPayoff(
            ql.Option.Call if self.option_type == "call" else ql.Option.Put, self.strike
        )
        exercise = ql.EuropeanExercise(ql_mty)
        self._option = ql.VanillaOption(payoff, exercise)
        self._engine = ql.AnalyticEuropeanEngine(process)
        self._option.setPricingEngine(self._engine)

    def price(self) -> float:
        if not self._option:
            self._setup_pricing_components()
        return float(self._option.NPV())

    def get_greeks(self) -> dict:
        if not self._option:
            self._setup_pricing_components()
            self._option.NPV()
        return {
            "delta": self._option.delta(),
            "gamma": self._option.gamma(),
            "vega": self._option.vega() / 100.0,
            "theta": self._option.theta() / 365.0,
            "rho": self._option.rho() / 100.0,
        }
