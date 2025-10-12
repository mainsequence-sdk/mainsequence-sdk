import datetime
from typing import Dict, Optional, TypedDict, Any
import random
from mainsequence.instruments.utils import to_ql_date
import mainsequence.client as msc
import QuantLib as ql
import os
import pandas as pd
from pathlib import Path



DISCOUNT_CURVES_TABLE=msc.Constant.get_value(name="DISCOUNT_CURVES_TABLE")
REFERENCE_RATES_FIXING_TABLE = msc.Constant.get_value(name="REFERENCE_RATES_FIXING_TABLE")

assert DISCOUNT_CURVES_TABLE is not None, "DISCOUNT_CURVES_TABLE not found in constants"
assert REFERENCE_RATES_FIXING_TABLE is not None, "REFERENCE_RATES_FIXING_TABLE not found in constants"



class DateInfo(TypedDict, total=False):
    """Defines the date range for a data query."""
    start_date: Optional[datetime.datetime]
    start_date_operand: Optional[str]
    end_date: Optional[datetime.datetime]
    end_date_operand: Optional[str]


UniqueIdentifierRangeMap = Dict[str, DateInfo]


class MockDataInterface:
    """
    A mock class to simulate fetching financial time series data from an API.

    In a real-world scenario, this class would contain logic to connect to a
    financial data provider (e.g., Bloomberg, Refinitiv, a database).
    """

    @staticmethod
    def get_historical_fixings(index_name: str, start_date: datetime.date, end_date: datetime.date) -> Dict[
        datetime.date, float]:
        """
        Simulates fetching historical index fixings from a database.

        CORRECTED: This now dynamically selects the appropriate calendar based on the index name.
        """

        # Dynamically select the calendar based on the index name
        calendar = ql.TARGET()  # Default calendar
        if 'USDLibor' in index_name:
            calendar = ql.UnitedKingdom()
            print("Using UnitedKingdom calendar for LIBOR.")
        elif 'Euribor' in index_name:
            calendar = ql.TARGET()  # TARGET is the standard for EUR rates
            print("Using TARGET calendar for Euribor.")
        elif 'SOFR' in index_name:
            calendar = ql.UnitedStates(ql.UnitedStates.SOFR)
            print("Using UnitedStates.SOFR calendar for SOFR.")
        elif index_name == "TIIE28":
            DEFAULT_TIIE_CSV = Path(__file__).resolve().parents[2] / "data" / "TIIE28_FIXINGS.csv"
            csv_path = os.getenv("TIIE28_FIXINGS_CSV") or str(DEFAULT_TIIE_CSV)

            fixings = pd.read_csv(csv_path)
            fixings.columns = ["date", "rate"]
            fixings["date"] = pd.to_datetime(fixings["date"], format="%m/%d/%Y")
            fixings["date"] = fixings["date"].dt.date
            if end_date > fixings["date"].max():
                raise Exception("Fixing not existent")
            fixings = fixings[fixings.date <= end_date]
            fixings["rate"] = fixings["rate"] / 100
            return fixings.set_index("date")["rate"].to_dict()


        elif 'TIIE' in index_name or 'F-TIIE' in index_name:
            raise Exception("Unrecognized index name")

        print("---------------------\n")

        fixings = {}
        current_date = start_date
        base_rate = 0.05

        while current_date <= end_date:
            ql_date = to_ql_date(current_date)
            # Only generate a fixing if the date is a business day for the selected calendar
            if calendar.isBusinessDay(ql_date):
                random_factor = (random.random() - 0.5) * 0.001
                fixings[current_date] = base_rate + random_factor

            current_date += datetime.timedelta(days=1)

        return fixings

    @staticmethod
    def get_historical_discount_curve(curve_name: str, target_date) -> list[dict]:
        """
        Simulates fetching historical data for a given asset or data type.

        Args:
            table_name: The name of the data table to query.
            asset_range_map: A dictionary mapping identifiers to date ranges.

        Returns:
            A dictionary containing mock market data.
        """
        print(f"--- MOCK DATA API ---")
        print(f"Fetching data from table '{table_name}' for assets: {list(asset_range_map.keys())}")
        print("---------------------\n")

        if table_name == "equities_daily":
            asset_ticker = list(asset_range_map.keys())[0]
            mock_data = {
                asset_ticker: {
                    "spot_price": 175.50,
                    "volatility": 0.20,
                    "dividend_yield": 0.015,
                    "risk_free_rate": 0.04
                }
            }
            if asset_ticker in mock_data:
                return mock_data[asset_ticker]
            else:
                raise ValueError(f"No mock data available for asset: {asset_ticker}")

        elif table_name == "interest_rate_swaps":
            # A more realistic set of market rates for curve bootstrapping.
            # This includes short-term deposit rates and longer-term swap rates.
            return {
                "curve_nodes": [
                    {'type': 'deposit', 'tenor': '3M', 'rate': 0.048},
                    {'type': 'deposit', 'tenor': '6M', 'rate': 0.050},
                    {'type': 'swap', 'tenor': '1Y', 'rate': 0.052},
                    {'type': 'swap', 'tenor': '2Y', 'rate': 0.054},
                    {'type': 'swap', 'tenor': '3Y', 'rate': 0.055},
                    {'type': 'swap', 'tenor': '5Y', 'rate': 0.056},
                    {'type': 'swap', 'tenor': '10Y', 'rate': 0.057},
                ]
            }
        elif table_name == "discount_bond_curve":
            # Zero rates for discounting bond cashflows (simple upward-sloping curve).
            # Tenors are parsed by QuantLib (e.g., "6M", "5Y").
            return {
                "curve_nodes": [
                    # --- Zero-coupon section (<= 1Y) ---
                    {"type": "zcb", "days_to_maturity": 30, "yield": 0.0370},
                    {"type": "zcb", "days_to_maturity": 90, "yield": 0.0385},
                    {"type": "zcb", "days_to_maturity": 180, "yield": 0.0395},
                    {"type": "zcb", "days_to_maturity": 270, "yield": 0.0405},
                    {"type": "zcb", "days_to_maturity": 360, "yield": 0.0410},

                    # --- Coupon bond section (>= 2Y) ---
                    {"type": "bond", "days_to_maturity": 730, "coupon": 0.0425, "clean_price": 99.20,
                     "dirty_price": 99.45, "frequency": "6M", "day_count": "30/360"},
                    {"type": "bond", "days_to_maturity": 1095, "coupon": 0.0440, "clean_price": 98.85,
                     "dirty_price": 99.10, "frequency": "6M", "day_count": "30/360"},
                    {"type": "bond", "days_to_maturity": 1825, "coupon": 0.0475, "clean_price": 98.10,
                     "dirty_price": 98.40, "frequency": "6M", "day_count": "30/360"},
                    {"type": "bond", "days_to_maturity": 2555, "coupon": 0.0490, "clean_price": 97.25,
                     "dirty_price": 97.60, "frequency": "6M", "day_count": "30/360"},
                    {"type": "bond", "days_to_maturity": 3650, "coupon": 0.0500, "clean_price": 96.80,
                     "dirty_price": 97.20, "frequency": "6M", "day_count": "30/360"},
                ]
            }
        elif table_name == "fx_options":
            # Mock FX options market data
            currency_pair = list(asset_range_map.keys())[0]

            # Mock data for common currency pairs
            fx_mock_data = {
                "EURUSD": {
                    "spot_fx_rate": 1.0850,
                    "volatility": 0.12,
                    "domestic_rate": 0.045,  # USD rate
                    "foreign_rate": 0.035  # EUR rate
                },
                "GBPUSD": {
                    "spot_fx_rate": 1.2650,
                    "volatility": 0.15,
                    "domestic_rate": 0.045,  # USD rate
                    "foreign_rate": 0.040  # GBP rate
                },
                "USDJPY": {
                    "spot_fx_rate": 148.50,
                    "volatility": 0.11,
                    "domestic_rate": 0.005,  # JPY rate
                    "foreign_rate": 0.045  # USD rate
                },
                "USDCHF": {
                    "spot_fx_rate": 0.8950,
                    "volatility": 0.13,
                    "domestic_rate": 0.015,  # CHF rate
                    "foreign_rate": 0.045  # USD rate
                }
            }

            if currency_pair in fx_mock_data:
                return fx_mock_data[currency_pair]
            else:
                # Default mock data for unknown pairs
                return {
                    "spot_fx_rate": 1.0000,
                    "volatility": 0.15,
                    "domestic_rate": 0.040,
                    "foreign_rate": 0.040
                }

        elif table_name == "tiie_zero_valmer":
            """
            Return a pre-built MXN TIIE zero curve parsed from a CSV.

            Expected CSV columns (case-insensitive; flexible):
              - Either 'maturity_date' (YYYY-MM-DD) OR 'days_to_maturity' OR a 'tenor' like '28D','3M','2Y'
              - One rate column among: ['zero','rate','yield','tiie'] as a decimal (e.g., 0.095 for 9.5%)
                (if the file holds percents like 9.50, we'll auto-convert to 0.095)
            """

            # You can override this path in your env; default points to the uploaded file
            DEFAULT_TIIE_CSV = Path(__file__).resolve().parents[2] / "data" / "MEXDERSWAP_IRSTIIEPR.csv"
            csv_path = os.getenv("TIIE_ZERO_CSV") or str(DEFAULT_TIIE_CSV)
            if not os.path.exists(csv_path):
                raise FileNotFoundError(f"TIIE zero curve CSV not found at: {csv_path}")

            names = ["id", "curve_name", "asof_yyMMdd", "idx", "zero_rate"]
            # STRICT: comma-separated, headerless, exactly these six columns
            df = pd.read_csv(csv_path, header=None, names=names, sep=",", engine="c", dtype=str)
            # pick a rate column

            df["asof_yyMMdd"] = pd.to_datetime(df["asof_yyMMdd"], format="%y%m%d")

            df["idx"] = df["idx"].astype(int)
            df["days_to_maturity"] = (df["asof_yyMMdd"] - df["asof_yyMMdd"].iloc[0]).dt.days
            df["zero_rate"] = df["zero_rate"].astype(float) / 100
            base_dt = df["asof_yyMMdd"].iloc[0].date()
            nodes = [
                {"days_to_maturity": d, "zero": z}
                for d, z in zip(df["days_to_maturity"], df["zero_rate"])
                if d > 0
            ]
            return {"curve_nodes": nodes}

        else:
            raise ValueError(f"Table '{table_name}' not found in mock data API.")


import json
import base64
import gzip
from cachetools import cachedmethod, LRUCache
from operator import attrgetter
from threading import RLock

class MSInterface():

    # ---- bounded, shared caches (class-level) ----
    _curve_cache = LRUCache(maxsize=1024)
    _curve_cache_lock = RLock()

    _fixings_cache = LRUCache(maxsize=4096)
    _fixings_cache_lock = RLock()

    @staticmethod
    def decompress_string_to_curve(b64_string: str) -> Dict[Any, Any]:
        """
        Decodes, decompresses, and deserializes a string back into a curve dictionary.

        Pipeline: Base64 (text) -> Gzip (binary) -> JSON -> Dict

        Args:
            b64_string: The Base64-encoded string from the database or API.

        Returns:
            The reconstructed Python dictionary.
        """
        # 1. Encode the ASCII string back into Base64 bytes
        base64_bytes = b64_string.encode('ascii')

        # 2. Decode the Base64 to get the compressed Gzip bytes
        compressed_bytes = base64.b64decode(base64_bytes)

        # 3. Decompress the Gzip bytes to get the original JSON bytes
        json_bytes = gzip.decompress(compressed_bytes)

        # 4. Decode the JSON bytes to a string and parse back into a dictionary
        return json.loads(json_bytes.decode('utf-8'))

    # NOTE: caching is applied at the method boundary; body is unchanged.
    @cachedmethod(cache=attrgetter("_curve_cache"), lock=attrgetter("_curve_cache_lock"))
    def get_historical_discount_curve(self, curve_name, target_date):
        from mainsequence.tdag import APIDataNode
        from mainsequence.logconf import logger
        data_node = APIDataNode.build_from_identifier(identifier=DISCOUNT_CURVES_TABLE)



        # for test purposes only get lats observations
        use_last_observation=os.environ.get("USE_LAST_OBSERVATION_MS_INSTRUMENT","true").lower()=="true"
        if use_last_observation:
            update_statistics = data_node.get_update_statistics()
            target_date = update_statistics.asset_time_statistics[curve_name]
            logger.warning("Curve is using last observation")


    
        limit = target_date + datetime.timedelta(days=1)
       




        curve = data_node.get_ranged_data_per_asset(
            range_descriptor={curve_name: {"start_date": target_date, "start_date_operand": ">=",
                                           "end_date": limit, "end_date_operand": "<", }}
            )

        if curve.empty:
            raise Exception(f"{target_date} is empty.")
        zeros = self.decompress_string_to_curve(curve["curve"].iloc[0])
        zeros = pd.Series(zeros).reset_index()
        zeros["index"] = pd.to_numeric(zeros["index"])
        zeros = zeros.set_index("index")[0]

        nodes = [
            {"days_to_maturity": d, "zero": z}
            for d, z in zeros.to_dict().items()
            if d > 0
        ]

        return nodes

    @cachedmethod(cache=attrgetter("_fixings_cache"), lock=attrgetter("_fixings_cache_lock"))
    def get_historical_fixings(self, reference_rate_uid: str, start_date: datetime.datetime,
                               end_date: datetime.datetime):
        """

        :param reference_rate_uid:
        :param start_date:
        :param end_date:
        :return:
        """
        from mainsequence.tdag import APIDataNode
        from mainsequence.logconf import logger
        import pytz  # patch

        data_node = APIDataNode.build_from_identifier(identifier=REFERENCE_RATES_FIXING_TABLE)

        start_date = datetime.datetime(2024, 9, 10, tzinfo=pytz.utc)
        end_date=datetime.datetime(2025, 9, 17, tzinfo=pytz.utc)
        
        
        

        fixings_df = data_node.get_ranged_data_per_asset(
            range_descriptor={reference_rate_uid: {"start_date": start_date, "start_date_operand": ">=",
                                                   "end_date": end_date, "end_date_operand": "<=", }}
        )
        if fixings_df.empty:

            use_last_observation = os.environ.get("USE_LAST_OBSERVATION_MS_INSTRUMENT", "true").lower() == "true"
            if use_last_observation:
                logger.warning("Fixings are using last observation and filled forward")
                fixings_df = data_node.get_ranged_data_per_asset(
                    range_descriptor={reference_rate_uid: {"start_date": datetime.datetime(1900,1,1,tzinfo=pytz.utc),
                                                           "start_date_operand": ">=",
                                                           }}


                )
                
                a=5
            
            raise Exception(f"{reference_rate_uid} has not data between {start_date} and {end_date}.")
        fixings_df = fixings_df.reset_index().rename(columns={"time_index": "date"})
        fixings_df["date"] = fixings_df["date"].dt.date
        return fixings_df.set_index("date")["rate"].to_dict()

    # optional helpers
    @classmethod
    def clear_caches(cls) -> None:
        cls._curve_cache.clear()
        cls._fixings_cache.clear()

    @classmethod
    def cache_info(cls) -> dict:
        return {
            "discount_curve_cache": {"size": cls._curve_cache.currsize, "max": cls._curve_cache.maxsize},
            "fixings_cache": {"size": cls._fixings_cache.currsize, "max": cls._fixings_cache.maxsize},
        }

