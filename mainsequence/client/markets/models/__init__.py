"""Deprecated compatibility exports for market client models."""

from __future__ import annotations

import warnings

warnings.warn(
    "mainsequence.client.markets.models is deprecated; " "use mainsequence.markets.client.models.",
    DeprecationWarning,
    stacklevel=2,
)

from mainsequence.markets.client.models import *  # noqa: F401,F403,E402
