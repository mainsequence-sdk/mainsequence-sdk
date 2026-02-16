import datetime
import logging
from typing import Any

import pandas as pd
import pandas_market_calendars as mcal
from pydantic import BaseModel, Field, PrivateAttr, field_validator

logger = logging.getLogger("virtualfundbuilder")


class RebalanceStrategyBase(BaseModel):

    calendar_key: str = Field(
        "24/7", description="Trading calendar should match pandas market calendar string"
    )

    # Optional cache for the heavy pmc calendar object; excluded from serialization/pickling
    _calendar_obj: Any = PrivateAttr(default=None)

    @field_validator("calendar_key")
    @classmethod
    def _validate_calendar_exists(cls, v: str) -> str:
        # Validate the key early; we don't keep the object here
        try:
            mcal.get_calendar(v)
        except Exception as e:
            raise ValueError(f"Unknown calendar '{v}': {e}")
        return v

    @property
    def calendar(self):
        """
        Recreate (and cache) the pandas_market_calendars calendar object on access.
        Not included in serialization; avoids pickling issues.
        """
        if (
            self._calendar_obj is None
            or getattr(self._calendar_obj, "name", None) != self.calendar_key
        ):
            self._calendar_obj = mcal.get_calendar(self.calendar_key)
        return self._calendar_obj

    def get_explanation(self):
        info = f"""
        <p>{self.__class__.__name__}: Rebalance strategy class.</p>
        """
        return info

    def calculate_rebalance_dates(
        self,
        start: datetime.datetime,
        end: datetime.datetime,
        calendar,
        rebalance_frequency_strategy: str,
    ) -> pd.DatetimeIndex:
        """
        Determines the dates on which portfolio rebalancing should be executed.
        Keeps the same signature for backward compatibility.
        """
        if end is None:
            raise NotImplementedError("end_date cannot be None")

        if rebalance_frequency_strategy == "daily":
            early = calendar.schedule(start_date=start.date(), end_date=end.date())
            rebalance_dates = early.set_index("market_open").index
        elif rebalance_frequency_strategy == "EOQ":
            # careful to use dates from the same calendar
            raise NotImplementedError
        else:
            raise NotImplementedError(f"Strategy {rebalance_frequency_strategy} not implemented")

        return pd.DatetimeIndex(rebalance_dates)







