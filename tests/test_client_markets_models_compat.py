import importlib.util
import sys


def _drop_modules(*names: str) -> None:
    for name in names:
        sys.modules.pop(name, None)


def test_client_market_models_canonical_import():
    from mainsequence.client.markets.models import Asset, Portfolio

    assert Asset.__name__ == "Asset"
    assert Portfolio.__name__ == "Portfolio"


def test_client_package_reexport_uses_canonical_market_models():
    _drop_modules("mainsequence.client.models_vam")

    from mainsequence.client import Asset as ClientAsset
    from mainsequence.client.markets.models import Asset as NewAsset

    assert ClientAsset is NewAsset
    assert "mainsequence.client.models_vam" not in sys.modules


def test_client_models_vam_module_is_removed():
    _drop_modules("mainsequence.client.models_vam")

    assert importlib.util.find_spec("mainsequence.client.models_vam") is None
