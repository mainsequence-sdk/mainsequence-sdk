import importlib.util
import sys

import pytest


def _drop_modules(*names: str) -> None:
    for name in names:
        sys.modules.pop(name, None)


def test_markets_client_models_canonical_import():
    from mainsequence.markets.client.models import Asset, Portfolio

    assert Asset.__name__ == "Asset"
    assert Portfolio.__name__ == "Portfolio"


def test_old_client_market_models_path_is_compatibility_shim():
    from mainsequence.markets.client.models import Asset as CanonicalAsset

    with pytest.warns(DeprecationWarning, match="mainsequence.markets.client.models"):
        from mainsequence.client.markets.models import Asset as CompatAsset

    assert CompatAsset is CanonicalAsset


def test_client_package_reexport_is_lazy_compatibility():
    _drop_modules("mainsequence.markets", "mainsequence.markets.client")


    assert "mainsequence.markets" not in sys.modules


def test_client_package_reexport_uses_canonical_market_models():
    _drop_modules("mainsequence.client.models_vam")

    from mainsequence.markets.client.models import Asset as NewAsset

    with pytest.warns(DeprecationWarning, match="mainsequence.markets.client.models"):
        from mainsequence.client import Asset as ClientAsset

    assert ClientAsset is NewAsset
    assert "mainsequence.client.models_vam" not in sys.modules


def test_client_models_vam_module_is_removed():
    _drop_modules("mainsequence.client.models_vam")

    assert importlib.util.find_spec("mainsequence.client.models_vam") is None
