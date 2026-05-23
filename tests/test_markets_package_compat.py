from __future__ import annotations

import importlib
import importlib.util


def test_markets_canonical_imports():
    instruments = importlib.import_module("mainsequence.markets.instruments")
    portfolios = importlib.import_module("mainsequence.markets.portfolios")

    assert instruments.__name__ == "mainsequence.markets.instruments"
    assert portfolios.__name__ == "mainsequence.markets.portfolios"


def test_old_top_level_market_shims_are_removed():
    assert importlib.util.find_spec("mainsequence.instruments") is None
    assert importlib.util.find_spec("mainsequence.virtualfundbuilder") is None
    assert importlib.util.find_spec("mainsequence.markets.virtualfundbuilder") is None
