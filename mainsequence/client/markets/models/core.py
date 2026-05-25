"""Deprecated compatibility module for market client core models."""

from __future__ import annotations

import warnings

warnings.warn(
    "mainsequence.client.markets.models.core is deprecated; "
    "use mainsequence.markets.client.models.core.",
    DeprecationWarning,
    stacklevel=2,
)

from mainsequence.markets.client.models.core import *  # noqa: F401,F403,E402
