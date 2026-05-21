from __future__ import annotations

import pytest

import mainsequence.client.markets.models.assets as assets_mod
from mainsequence.client.markets.models.assets import Asset


class _FakeResponse:
    def __init__(self, status_code: int, payload, content: bytes = b"{}"):
        self.status_code = status_code
        self._payload = payload
        self.content = content
        self.text = str(payload)

    def json(self):
        return self._payload


def test_asset_quick_search_uses_frontend_list_endpoint(monkeypatch):
    captured: dict[str, object] = {}

    def fake_make_request(*, s, loaders, r_type, url, payload, time_out):
        captured["r_type"] = r_type
        captured["url"] = url
        captured["payload"] = payload
        captured["time_out"] = time_out
        return _FakeResponse(
            200,
            {
                "results": [
                    {
                        "id": 1,
                        "unique_identifier": "BTCUSDT-SPOT",
                        "name": "Bitcoin / Tether",
                    }
                ]
            },
        )

    monkeypatch.setattr(assets_mod, "make_request", fake_make_request)

    results = Asset.quick_search(" Bitcoin ", limit=1, timeout=30)

    assert results == [
        {
            "id": 1,
            "unique_identifier": "BTCUSDT-SPOT",
            "name": "Bitcoin / Tether",
        }
    ]
    assert captured["r_type"] == "GET"
    assert str(captured["url"]).endswith("/orm/api/assets/asset/")
    assert captured["payload"] == {
        "params": {
            "response_format": "frontend_list",
            "search": "Bitcoin",
            "limit": 1,
        }
    }
    assert captured["time_out"] == 30


def test_asset_quick_search_accepts_list_payload(monkeypatch):
    def fake_make_request(*, s, loaders, r_type, url, payload, time_out):
        return _FakeResponse(
            200,
            [{"unique_identifier": "ETHUSDT-SPOT", "name": "Ether / Tether"}],
        )

    monkeypatch.setattr(assets_mod, "make_request", fake_make_request)

    assert Asset.quick_search("eth", limit=1) == [
        {"unique_identifier": "ETHUSDT-SPOT", "name": "Ether / Tether"}
    ]


def test_asset_quick_search_rejects_empty_query():
    with pytest.raises(ValueError, match="cannot be empty"):
        Asset.quick_search("   ")
