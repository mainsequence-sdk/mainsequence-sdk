from typing import Literal

import QuantLib as ql

from mainsequence.instruments.pricing_models.fx_option_pricer import (
    create_fx_garman_kohlhagen_model,
    get_fx_market_data,
)


def create_knockout_fx_option(
    currency_pair: str,
    calculation_date: ql.Date,
    maturity_date: ql.Date,
    strike: float,
    barrier: float,
    option_type: Literal["call", "put"],
    barrier_type: Literal["up_and_out", "down_and_out"],
    rebate: float = 0.0,
) -> tuple[ql.BarrierOption, ql.PricingEngine]:
    """
    Creates a knock-out FX barrier option using QuantLib.

    Args:
        currency_pair: Currency pair (e.g., "EURUSD")
        calculation_date: Valuation date
        maturity_date: Option expiration date
        strike: Strike price
        barrier: Barrier level
        option_type: "call" or "put"
        barrier_type: "up_and_out" or "down_and_out"
        rebate: Rebate paid if knocked out (default: 0.0)

    Returns:
        Tuple of (BarrierOption, PricingEngine)
    """
    # 1) Get market data
    market_data = get_fx_market_data(currency_pair, calculation_date)
    spot_fx = market_data["spot_fx_rate"]
    vol = market_data["volatility"]
    domestic_rate = market_data["domestic_rate"]
    foreign_rate = market_data["foreign_rate"]

    # 2) Create the Garman-Kohlhagen process
    gk_process = create_fx_garman_kohlhagen_model(
        calculation_date, spot_fx, vol, domestic_rate, foreign_rate
    )

    # 3) Define the payoff
    ql_option_type = ql.Option.Call if option_type == "call" else ql.Option.Put
    payoff = ql.PlainVanillaPayoff(ql_option_type, strike)

    # 4) Define the exercise (European for barrier options)
    exercise = ql.EuropeanExercise(maturity_date)

    # 5) Define the barrier type
    if barrier_type == "up_and_out":
        ql_barrier_type = ql.Barrier.UpOut
    elif barrier_type == "down_and_out":
        ql_barrier_type = ql.Barrier.DownOut
    else:
        raise ValueError(f"Unsupported barrier type: {barrier_type}")

    # 6) Create the barrier option
    barrier_option = ql.BarrierOption(ql_barrier_type, barrier, rebate, payoff, exercise)

    # 7) Create the pricing engine
    # For barrier options, we can use analytical engines for simple cases
    # or Monte Carlo for more complex scenarios
    try:
        # Try analytical engine first (works for European barrier options)
        engine = ql.AnalyticBarrierEngine(gk_process)
    except Exception:
        # Fall back to Monte Carlo if analytical doesn't work
        engine = create_monte_carlo_barrier_engine(gk_process)

    # 8) Set the pricing engine
    barrier_option.setPricingEngine(engine)

    return barrier_option, engine


def create_monte_carlo_barrier_engine(
    process: ql.BlackScholesMertonProcess,
    time_steps: int = 252,
    mc_samples: int = 100000,
    seed: int = 42,
) -> ql.PricingEngine:
    """
    Creates a Monte Carlo pricing engine for barrier options.

    Args:
        process: The underlying stochastic process
        time_steps: Number of time steps for simulation (default: 252 for daily)
        mc_samples: Number of Monte Carlo samples (default: 100,000)
        seed: Random seed for reproducibility

    Returns:
        Monte Carlo pricing engine
    """
    # Set up the random number generator
    rng = ql.UniformRandomSequenceGenerator(time_steps, ql.UniformRandomGenerator(seed))
    seq = ql.GaussianRandomSequenceGenerator(rng)

    # Create the Monte Carlo engine
    engine = ql.MCBarrierEngine(
        process,
        "pseudorandom",  # or "lowdiscrepancy"
        time_steps,
        requiredSamples=mc_samples,
        seed=seed,
    )

    return engine


def get_barrier_option_analytics(
    barrier_option: ql.BarrierOption, spot_fx: float, barrier: float, barrier_type: str
) -> dict:
    """
    Calculate additional analytics specific to barrier options.

    Args:
        barrier_option: The QuantLib barrier option object
        spot_fx: Current spot FX rate
        barrier: Barrier level
        barrier_type: Type of barrier ("up_and_out" or "down_and_out")

    Returns:
        Dictionary with barrier-specific analytics
    """
    # Calculate probability of knock-out (approximation)
    if barrier_type == "up_and_out":
        distance_to_barrier = (barrier - spot_fx) / spot_fx
        barrier_status = "Active" if spot_fx < barrier else "Knocked Out"
    else:  # down_and_out
        distance_to_barrier = (spot_fx - barrier) / spot_fx
        barrier_status = "Active" if spot_fx > barrier else "Knocked Out"

    # Get standard option analytics
    try:
        npv = barrier_option.NPV()
        delta = barrier_option.delta()
        gamma = barrier_option.gamma()
        vega = barrier_option.vega()
        theta = barrier_option.theta()
        rho = barrier_option.rho()
    except Exception as e:
        # If analytics fail, return basic info
        return {
            "barrier_status": barrier_status,
            "distance_to_barrier_pct": distance_to_barrier * 100,
            "error": f"Analytics calculation failed: {str(e)}",
        }

    return {
        "npv": npv,
        "delta": delta,
        "gamma": gamma,
        "vega": vega,
        "theta": theta,
        "rho": rho,
        "barrier_status": barrier_status,
        "distance_to_barrier_pct": distance_to_barrier * 100,
    }


def validate_barrier_parameters(
    spot_fx: float, strike: float, barrier: float, barrier_type: str, option_type: str
) -> None:
    """
    Validate barrier option parameters for logical consistency.

    Args:
        spot_fx: Current spot FX rate
        strike: Strike price
        barrier: Barrier level
        barrier_type: "up_and_out" or "down_and_out"
        option_type: "call" or "put"

    Raises:
        ValueError: If parameters are inconsistent
    """
    if barrier_type == "up_and_out":
        if barrier <= spot_fx:
            raise ValueError("Up-and-out barrier must be above current spot rate")
        if option_type == "call" and barrier <= strike:
            raise ValueError("Up-and-out call barrier should typically be above strike")

    elif barrier_type == "down_and_out":
        if barrier >= spot_fx:
            raise ValueError("Down-and-out barrier must be below current spot rate")
        if option_type == "put" and barrier >= strike:
            raise ValueError("Down-and-out put barrier should typically be below strike")

    else:
        raise ValueError(f"Unsupported barrier type: {barrier_type}")

    if strike <= 0 or barrier <= 0 or spot_fx <= 0:
        raise ValueError("Strike, barrier, and spot rates must be positive")
