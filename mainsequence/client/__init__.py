from mainsequence.logconf import logger

from .command_center import *
from .models_helpers import *
from .models_metatables import *
from .models_simple_tables import *
from .models_tdag import *
from .models_user import *
from .utils import MARKETS_CONSTANTS, TDAG_CONSTANTS, AuthLoaders, bios_uuid


def __getattr__(name: str):
    from mainsequence.markets.client import models as market_models

    if hasattr(market_models, name):
        import warnings

        warnings.warn(
            f"mainsequence.client.{name} is deprecated; import {name} from "
            "mainsequence.markets.client.models.",
            DeprecationWarning,
            stacklevel=2,
        )
        return getattr(market_models, name)
    raise AttributeError(f"module 'mainsequence.client' has no attribute {name!r}")
