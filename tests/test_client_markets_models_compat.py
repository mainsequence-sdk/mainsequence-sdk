from __future__ import annotations

import importlib
import sys


def _drop_modules(*names: str) -> None:
    for name in names:
        sys.modules.pop(name, None)


def test_client_market_models_canonical_import():
    from mainsequence.client.markets.models import Asset, Portfolio

    assert Asset.__name__ == "Asset"
    assert Portfolio.__name__ == "Portfolio"


def test_client_models_vam_shim_logs_deprecation(monkeypatch):
    import mainsequence.logconf
    from mainsequence.client.markets.models import Asset as NewAsset

    messages: list[str] = []

    class FakeLogger:
        def warning(self, message: str) -> None:
            messages.append(message)

    monkeypatch.setattr(mainsequence.logconf, "logger", FakeLogger())
    _drop_modules("mainsequence.client.models_vam")

    deprecated_module = importlib.import_module("mainsequence.client.models_vam")

    assert deprecated_module.Asset is NewAsset
    assert messages == [
        "mainsequence.client.models_vam is deprecated and will be removed in a future release. "
        "Use mainsequence.client.markets.models instead, for example: "
        "from mainsequence.client.markets.models import Asset, Portfolio."
    ]


def test_client_package_reexport_uses_canonical_market_models():
    _drop_modules("mainsequence.client.models_vam")

    from mainsequence.client import Asset as ClientAsset
    from mainsequence.client.markets.models import Asset as NewAsset

    assert ClientAsset is NewAsset
    assert "mainsequence.client.models_vam" not in sys.modules
