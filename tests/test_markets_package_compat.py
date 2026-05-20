from __future__ import annotations

import importlib
import sys

import pytest


def _drop_modules(*names: str) -> None:
    for name in names:
        sys.modules.pop(name, None)


def test_markets_canonical_imports():
    instruments = importlib.import_module("mainsequence.markets.instruments")
    virtualfundbuilder = importlib.import_module("mainsequence.markets.virtualfundbuilder")

    assert instruments.__name__ == "mainsequence.markets.instruments"
    assert virtualfundbuilder.__name__ == "mainsequence.markets.virtualfundbuilder"


def test_instruments_compat_import_warns():
    _drop_modules("mainsequence.instruments")

    with pytest.warns(
        DeprecationWarning,
        match=(
            r"mainsequence\.instruments is deprecated; "
            r"use mainsequence\.markets\.instruments\."
        ),
    ):
        module = importlib.import_module("mainsequence.instruments")

    assert module.__name__ == "mainsequence.instruments"
    assert module.FixedRateBond is importlib.import_module(
        "mainsequence.markets.instruments"
    ).FixedRateBond


def test_virtualfundbuilder_compat_import_warns():
    _drop_modules("mainsequence.virtualfundbuilder")

    with pytest.warns(
        DeprecationWarning,
        match=(
            r"mainsequence\.virtualfundbuilder is deprecated; "
            r"use mainsequence\.markets\.virtualfundbuilder\."
        ),
    ):
        module = importlib.import_module("mainsequence.virtualfundbuilder")

    assert module.__name__ == "mainsequence.virtualfundbuilder"
    assert module.TIMEDELTA is importlib.import_module(
        "mainsequence.markets.virtualfundbuilder"
    ).TIMEDELTA
