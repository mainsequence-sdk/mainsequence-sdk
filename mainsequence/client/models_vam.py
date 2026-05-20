from mainsequence.logconf import logger

logger.warning(
    "mainsequence.client.models_vam is deprecated and will be removed in a future release. "
    "Use mainsequence.client.markets.models instead, for example: "
    "from mainsequence.client.markets.models import Asset, Portfolio."
)

from mainsequence.client.markets.models import *  # noqa: F401,F403
