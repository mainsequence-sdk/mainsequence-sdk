from enum import Enum


class RebalanceFrequencyStrategyName(Enum):
    DAILY = "daily"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"


class PriceTypeNames(Enum):
    VWAP = "vwap"
    OPEN = "open"
    CLOSE = "close"


class RunStrategy(Enum):
    BACKTEST = "backtest"
    LIVE = "live"
    ALL = "all"

