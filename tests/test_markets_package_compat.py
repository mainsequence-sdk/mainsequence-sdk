from __future__ import annotations

import importlib
import importlib.util


def test_markets_canonical_imports():
    instruments = importlib.import_module("mainsequence.markets.instruments")
    virtualfundbuilder = importlib.import_module("mainsequence.markets.virtualfundbuilder")

    assert instruments.__name__ == "mainsequence.markets.instruments"
    assert virtualfundbuilder.__name__ == "mainsequence.markets.virtualfundbuilder"


def test_old_top_level_market_shims_are_removed():
    assert importlib.util.find_spec("mainsequence.instruments") is None
    assert importlib.util.find_spec("mainsequence.virtualfundbuilder") is None
