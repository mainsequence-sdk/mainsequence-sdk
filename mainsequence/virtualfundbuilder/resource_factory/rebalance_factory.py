import datetime
import logging
import os
from typing import Any, ClassVar

import pandas as pd
import pandas_market_calendars as mcal
from pydantic import BaseModel, Field, PrivateAttr, field_validator

from mainsequence.virtualfundbuilder.enums import ResourceType
from mainsequence.virtualfundbuilder.resource_factory.base_factory import (
    BaseFactory,
    BaseResource,
    insert_in_registry,
)

logger = logging.getLogger("virtualfundbuilder")


class RebalanceStrategyBase(BaseResource, BaseModel):
    TYPE: ClassVar[ResourceType] = ResourceType.REBALANCE_STRATEGY

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


REBALANCE_CLASS_REGISTRY = (
    REBALANCE_CLASS_REGISTRY if "REBALANCE_CLASS_REGISTRY" in globals() else {}
)


def register_rebalance_class(name=None, register_in_agent=True):
    """
    Decorator to register a model class in the factory.
    If `name` is not provided, the class's name is used as the key.
    """

    def decorator(cls):
        if os.environ.get("IGNORE_MS_AGENT", "false").lower() == "true":
            logger.warning("Ignoring MS agent registration")
            return cls

        return insert_in_registry(REBALANCE_CLASS_REGISTRY, cls, register_in_agent, name)

    return decorator


class RebalanceFactory(BaseFactory):

    @staticmethod
    def get_rebalance_strategy(rebalance_strategy_name: str):
        if rebalance_strategy_name not in REBALANCE_CLASS_REGISTRY:
            RebalanceFactory.get_rebalance_strategies()
        try:
            return REBALANCE_CLASS_REGISTRY[rebalance_strategy_name]
        except KeyError:
            logger.exception(f"{rebalance_strategy_name} is not registered in this project")

    @staticmethod
    def get_rebalance_strategies():

        try:
            RebalanceFactory.import_module("rebalance_strategies")
        except FileNotFoundError:
            logger.info("rebalance_strategies folder no present no strategies to import")
        return REBALANCE_CLASS_REGISTRY
