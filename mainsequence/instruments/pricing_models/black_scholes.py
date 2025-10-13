import QuantLib as ql


def create_bsm_model(
    calculation_date: ql.Date,
    spot_price: float,
    volatility: float,
    risk_free_rate: float,
    dividend_yield: float,
) -> ql.BlackScholesMertonProcess:
    """
    Sets up the Black-Scholes-Merton process in QuantLib.

    Args:
        calculation_date: The date for which the pricing is being performed.
        spot_price: The current price of the underlying asset.
        volatility: The annualized volatility of the underlying asset.
        risk_free_rate: The annualized risk-free interest rate.
        dividend_yield: The annualized dividend yield of the underlying asset.

    Returns:
        A QuantLib BlackScholesMertonProcess object configured with the market data.
    """
    # Set the evaluation date in QuantLib
    ql.Settings.instance().evaluationDate = calculation_date

    # Define underlying asset price and curves
    underlying_handle = ql.QuoteHandle(ql.SimpleQuote(spot_price))

    day_count = ql.Actual365Fixed()

    flat_ts = ql.YieldTermStructureHandle(
        ql.FlatForward(calculation_date, risk_free_rate, day_count)
    )

    dividend_ts = ql.YieldTermStructureHandle(
        ql.FlatForward(calculation_date, dividend_yield, day_count)
    )

    flat_vol_ts = ql.BlackVolTermStructureHandle(
        ql.BlackConstantVol(calculation_date, ql.TARGET(), volatility, day_count)
    )

    # Create the Black-Scholes-Merton process
    bsm_process = ql.BlackScholesMertonProcess(underlying_handle, dividend_ts, flat_ts, flat_vol_ts)

    return bsm_process
