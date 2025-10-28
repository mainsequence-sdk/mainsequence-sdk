import datetime
import os
import random
from pathlib import Path
from typing import Any, TypedDict

import pandas as pd
import QuantLib as ql

import mainsequence.client as msc
from mainsequence.instruments.utils import to_ql_date
import base64
import gzip
import json
from operator import attrgetter
from threading import RLock
import os 
from cachetools import LRUCache, cachedmethod



class DateInfo(TypedDict, total=False):
    """Defines the date range for a data query."""

    start_date: datetime.datetime | None
    start_date_operand: str | None
    end_date: datetime.datetime | None
    end_date_operand: str | None


UniqueIdentifierRangeMap = dict[str, DateInfo]



class MSInterface:

    # ---- bounded, shared caches (class-level) ----
    _curve_cache = LRUCache(maxsize=1024)
    _curve_cache_lock = RLock()

    _fixings_cache = LRUCache(maxsize=4096)
    _fixings_cache_lock = RLock()

    @staticmethod
    def decompress_string_to_curve(b64_string: str) -> dict[Any, Any]:
        """
        Decodes, decompresses, and deserializes a string back into a curve dictionary.

        Pipeline: Base64 (text) -> Gzip (binary) -> JSON -> Dict

        Args:
            b64_string: The Base64-encoded string from the database or API.

        Returns:
            The reconstructed Python dictionary.
        """
        # 1. Encode the ASCII string back into Base64 bytes
        base64_bytes = b64_string.encode("ascii")

        # 2. Decode the Base64 to get the compressed Gzip bytes
        compressed_bytes = base64.b64decode(base64_bytes)

        # 3. Decompress the Gzip bytes to get the original JSON bytes
        json_bytes = gzip.decompress(compressed_bytes)

        # 4. Decode the JSON bytes to a string and parse back into a dictionary
        return json.loads(json_bytes.decode("utf-8"))

    # NOTE: caching is applied at the method boundary; body is unchanged.
    @cachedmethod(cache=attrgetter("_curve_cache"), lock=attrgetter("_curve_cache_lock"))
    def get_historical_discount_curve(self, curve_name, target_date):
        from mainsequence.logconf import logger
        from mainsequence.tdag import APIDataNode
        instrument_configuration=msc.InstrumentsConfiguration.filter()[0]

        if instrument_configuration.discount_curves_storage_node is None:
            raise Exception("discount_curves_storage_node needs to be set in https://main-sequence.app/instruments/config/")

        data_node = APIDataNode.build_from_table_id(table_id=instrument_configuration.discount_curves_storage_node)

        # for test purposes only get lats observations
        use_last_observation = (
            os.environ.get("USE_LAST_OBSERVATION_MS_INSTRUMENT", "false").lower() == "true"
        )
        if use_last_observation:
            update_statistics = data_node.get_update_statistics()
            target_date = update_statistics.asset_time_statistics[curve_name]
            logger.warning("Curve is using last observation")

        limit = target_date + datetime.timedelta(days=1)

        curve = data_node.get_ranged_data_per_asset(
            range_descriptor={
                curve_name: {
                    "start_date": target_date,
                    "start_date_operand": ">=",
                    "end_date": limit,
                    "end_date_operand": "<",
                }
            }
        )

        if curve.empty:
            raise Exception(f"{target_date} is empty.")
        zeros = self.decompress_string_to_curve(curve["curve"].iloc[0])
        zeros = pd.Series(zeros).reset_index()
        zeros["index"] = pd.to_numeric(zeros["index"])
        zeros = zeros.set_index("index")[0]

        nodes = [{"days_to_maturity": d, "zero": z} for d, z in zeros.to_dict().items() if d > 0]

        return nodes, target_date

    @cachedmethod(cache=attrgetter("_fixings_cache"), lock=attrgetter("_fixings_cache_lock"))
    def get_historical_fixings(
        self, reference_rate_uid: str, start_date: datetime.datetime, end_date: datetime.datetime
    ):
        """

        :param reference_rate_uid:
        :param start_date:
        :param end_date:
        :return:
        """
        import pytz  # patch

        from mainsequence.logconf import logger
        from mainsequence.tdag import APIDataNode

        instrument_configuration = msc.InstrumentsConfiguration.filter()[0]
        if instrument_configuration.reference_rates_fixings_storage_node is None:
            raise Exception("reference_rates_fixings_storage_node needs to be set in https://main-sequence.app/instruments/config/")

        data_node = APIDataNode.build_from_table_id(table_id=instrument_configuration.reference_rates_fixings_storage_node)



        fixings_df = data_node.get_ranged_data_per_asset(
            range_descriptor={
                reference_rate_uid: {
                    "start_date": start_date,
                    "start_date_operand": ">=",
                    "end_date": end_date,
                    "end_date_operand": "<=",
                }
            }
        )
        if fixings_df.empty:

            use_last_observation = (
                os.environ.get("USE_LAST_OBSERVATION_MS_INSTRUMENT", "false").lower() == "true"
            )
            if use_last_observation:
                logger.warning("Fixings are using last observation and filled forward")
                fixings_df = data_node.get_ranged_data_per_asset(
                    range_descriptor={
                        reference_rate_uid: {
                            "start_date": datetime.datetime(1900, 1, 1, tzinfo=pytz.utc),
                            "start_date_operand": ">=",
                        }
                    }
                )

                a = 5

            raise Exception(
                f"{reference_rate_uid} has not data between {start_date} and {end_date}."
            )
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
            "discount_curve_cache": {
                "size": cls._curve_cache.currsize,
                "max": cls._curve_cache.maxsize,
            },
            "fixings_cache": {
                "size": cls._fixings_cache.currsize,
                "max": cls._fixings_cache.maxsize,
            },
        }
