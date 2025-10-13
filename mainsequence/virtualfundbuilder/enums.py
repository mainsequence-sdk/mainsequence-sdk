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


class ResourceType(Enum):
    SIGNAL_WEIGHTS_STRATEGY = "signal_weights_strategy"
    REBALANCE_STRATEGY = "rebalance_strategy"
    APP = "app"
    HTML_APP = "html_app"
