"""Deprecated compatibility package for market-specific client APIs."""

from __future__ import annotations

import warnings

warnings.warn(
    "mainsequence.client.markets is deprecated; use mainsequence.markets.client.",
    DeprecationWarning,
    stacklevel=2,
)

from mainsequence.markets.client import *  # noqa: F401,F403,E402
