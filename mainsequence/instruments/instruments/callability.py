from __future__ import annotations

import datetime
from typing import Annotated, Literal

import QuantLib as ql
from pydantic import BaseModel, Field


class CallabilityItem(BaseModel):
    date: datetime.date = Field(
        ...,
        description="Exercise date (call/put date).",
        examples=["2029-06-15", "2030-06-15"],
    )
    type: Literal["Call", "Put"] = Field(
        ...,
        description="Exercise type.",
        examples=["Call"],
    )
    price: float = Field(
        ...,
        gt=0,
        description="Exercise price per 100 of face (100=par).",
        examples=[100.0, 102.0],
        json_schema_extra={"unit": "per_100"},
    )
    price_type: Literal["Clean", "Dirty"] = Field(
        default="Clean",
        description="Whether exercise price is clean or dirty.",
        examples=["Clean"],
    )


class HullWhiteParams(BaseModel):
    a: float = Field(
        0.03,
        ge=0,
        description="Hull-White mean reversion (a).",
        examples=[0.03, 0.05],
    )
    sigma: float = Field(
        0.01,
        ge=0,
        description="Hull-White volatility (sigma).",
        examples=[0.01, 0.005],
    )

class TreeCallableEngineParams(BaseModel):
    engine: Literal["TreeCallableFixedRateBondEngine"] = Field(
        "TreeCallableFixedRateBondEngine",
        description="QuantLib lattice engine for callable fixed-rate bonds.",
        examples=["TreeCallableFixedRateBondEngine"],
    )
    time_steps: int = Field(
        40,
        ge=1,
        description="Tree time steps.",
        examples=[40, 80],
    )
    model: Literal["HullWhite"] = Field(
        "HullWhite",
        description="Short-rate model used by the tree engine.",
        examples=["HullWhite"],
    )
    hull_white: HullWhiteParams = Field(
        default_factory=HullWhiteParams,
        description="Hull-White model parameters.",
    )
    pass_term_structure: bool = Field(
        default=False,
        description="If True, pass discount curve handle also as engine termStructure arg.",
        examples=[False, True],
    )

class BlackCallableEngineParams(BaseModel):
    engine: Literal["BlackCallableFixedRateBondEngine"] = Field(
        "BlackCallableFixedRateBondEngine",
        description="QuantLib Black engine for callable fixed-rate bonds.",
        examples=["BlackCallableFixedRateBondEngine"],
    )
    fwd_yield_vol: float = Field(
        ...,
        ge=0,
        description="Forward yield volatility (decimal, e.g. 0.20 = 20%).",
        examples=[0.20, 0.10],
        json_schema_extra={"unit": "vol_decimal"},
    )

CallableEngineParams = Annotated[
    TreeCallableEngineParams | BlackCallableEngineParams,
    Field(discriminator="engine"),
]

class DiscountParameters(BaseModel):
    """
    Engine choice + engine params for callable bond pricing.
    """
    engine: CallableEngineParams = Field(
        default_factory=TreeCallableEngineParams,
        description="Callable bond pricing engine configuration.",
        examples=[
            {"engine": "TreeCallableFixedRateBondEngine", "time_steps": 40, "model": "HullWhite", "hull_white": {"a": 0.03, "sigma": 0.01}},
            {"engine": "BlackCallableFixedRateBondEngine", "fwd_yield_vol": 0.20},
        ],
    )

    def build_engine(self, discount_curve: ql.YieldTermStructureHandle) -> ql.PricingEngine:
        e = self.engine

        if isinstance(e, TreeCallableEngineParams):
            # Hull-White model constructed on the discount curve handle
            model = ql.HullWhite(discount_curve, float(e.hull_white.a), float(e.hull_white.sigma))
            if e.pass_term_structure:
                return ql.TreeCallableFixedRateBondEngine(model, int(e.time_steps), discount_curve)
            return ql.TreeCallableFixedRateBondEngine(model, int(e.time_steps))

        if isinstance(e, BlackCallableEngineParams):
            vol_handle = ql.QuoteHandle(ql.SimpleQuote(float(e.fwd_yield_vol)))
            return ql.BlackCallableFixedRateBondEngine(vol_handle, discount_curve)

        raise TypeError(f"Unsupported engine params type: {type(e)}")


class AmortizationParameters(BaseModel):
    notionals: list[float] = Field(
        ...,
        min_length=1,
        description=(
            "Outstanding notionals per coupon period (QuantLib amortizing bonds). "
            "Length must match the number of coupon periods: typically len(schedule.dates()) - 1."
        ),
        examples=[[10000, 10000, 8000, 8000, 6000, 6000]],
    )

    redemptions: list[float] | None = Field(
        default=None,
        description=(
            "Optional QuantLib 'redemptions' argument (vector, default {100.0} in QuantLib-SWIG). "
            "Used for pool/loan loss modelling; omit for standard amortization."
        ),
        examples=[None, [100.0], [98.0]],
        json_schema_extra={"unit": "per_100"},
    )

    payment_lag: int = Field(
        default=0,
        ge=0,
        description="Optional QuantLib 'paymentLag' argument for amortizing bonds (days).",
        examples=[0, 2],
    )