"""Deprecated compatibility module for market client asset models."""

from __future__ import annotations

import warnings

warnings.warn(
    "mainsequence.client.markets.models.assets is deprecated; "
    "use mainsequence.markets.client.models.assets.",
    DeprecationWarning,
    stacklevel=2,
)

from mainsequence.markets.client.models.assets import *  # noqa: F401,F403,E402
