import QuantLib as ql

from mainsequence.instruments.data_interface import DateInfo, data_interface


def create_fx_garman_kohlhagen_model(
    calculation_date: ql.Date,
    spot_fx_rate: float,
    volatility: float,
    domestic_rate: float,
    foreign_rate: float,
) -> ql.BlackScholesMertonProcess:
    """
    Sets up the Garman-Kohlhagen process for FX options in QuantLib.

    The Garman-Kohlhagen model is essentially Black-Scholes where:
    - The underlying is the FX spot rate
    - The "dividend yield" is replaced by the foreign risk-free rate
    - The risk-free rate is the domestic risk-free rate

    Args:
        calculation_date: The date for which the pricing is being performed.
        spot_fx_rate: The current FX spot rate (domestic per foreign currency).
        volatility: The annualized volatility of the FX rate.
        domestic_rate: The annualized domestic risk-free interest rate.
        foreign_rate: The annualized foreign risk-free interest rate.

    Returns:
        A QuantLib BlackScholesMertonProcess object configured for FX options.
    """
    # Set the evaluation date in QuantLib
    ql.Settings.instance().evaluationDate = calculation_date

    # Define FX spot rate handle
    spot_handle = ql.QuoteHandle(ql.SimpleQuote(spot_fx_rate))

    day_count = ql.Actual365Fixed()

    # Domestic risk-free rate curve
    domestic_ts = ql.YieldTermStructureHandle(
        ql.FlatForward(calculation_date, domestic_rate, day_count)
    )

    # Foreign risk-free rate curve (treated as "dividend yield" in BS framework)
    foreign_ts = ql.YieldTermStructureHandle(
        ql.FlatForward(calculation_date, foreign_rate, day_count)
    )

    # FX volatility surface
    vol_ts = ql.BlackVolTermStructureHandle(
        ql.BlackConstantVol(calculation_date, ql.TARGET(), volatility, day_count)
    )

    # Create the Garman-Kohlhagen process (using BlackScholesMertonProcess)
    gk_process = ql.BlackScholesMertonProcess(spot_handle, foreign_ts, domestic_ts, vol_ts)

    return gk_process


def get_fx_market_data(currency_pair: str, calculation_date) -> dict:
    """
    Fetches FX market data for a given currency pair.

    Args:
        currency_pair: Currency pair in format "EURUSD", "GBPUSD", etc.
        calculation_date: The calculation date for market data

    Returns:
        Dictionary containing spot rate, volatility, domestic rate, and foreign rate
    """
    # Extract domestic and foreign currencies from pair
    if len(currency_pair) != 6:
        raise ValueError("Currency pair must be 6 characters (e.g., 'EURUSD')")

    foreign_ccy = currency_pair[:3]  # First 3 characters
    domestic_ccy = currency_pair[3:]  # Last 3 characters

    # Fetch market data using the data interface
    asset_range_map = {currency_pair: DateInfo(start_date=calculation_date)}
    market_data = data_interface.get_historical_data("fx_options", asset_range_map)

    return {
        "spot_fx_rate": market_data["spot_fx_rate"],
        "volatility": market_data["volatility"],
        "domestic_rate": market_data["domestic_rate"],
        "foreign_rate": market_data["foreign_rate"],
        "foreign_currency": foreign_ccy,
        "domestic_currency": domestic_ccy,
    }
