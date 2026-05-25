"""Deprecated compatibility module for market client account and portfolio models."""

from __future__ import annotations

import warnings

warnings.warn(
    "mainsequence.client.markets.models.accounts_and_portfolios is deprecated; "
    "use mainsequence.markets.client.models.accounts_and_portfolios.",
    DeprecationWarning,
    stacklevel=2,
)

from mainsequence.markets.client.models.accounts_and_portfolios import *  # noqa: F401,F403,E402
