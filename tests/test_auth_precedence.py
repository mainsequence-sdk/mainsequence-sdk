from __future__ import annotations

import importlib
import json
import pathlib
import sys
import types

import requests


def _load_mainsequence_submodule(module_name: str):
    repo_root = pathlib.Path(__file__).resolve().parents[1]
    pkg_root = repo_root / "mainsequence"
    client_pkg_root = pkg_root / "client"

    for name in tuple(sys.modules):
        if name == "mainsequence" or name.startswith("mainsequence."):
            sys.modules.pop(name, None)

    pkg = types.ModuleType("mainsequence")
    pkg.__path__ = [str(pkg_root)]
    sys.modules["mainsequence"] = pkg

    client_pkg = types.ModuleType("mainsequence.client")
    client_pkg.__path__ = [str(client_pkg_root)]
    sys.modules["mainsequence.client"] = client_pkg

    return importlib.import_module(module_name)


class _FakeResponse:
    def __init__(self, status_code: int, payload: dict | None = None):
        self.status_code = status_code
        self._payload = payload or {}
        self.text = json.dumps(self._payload)

    def json(self):
        return self._payload


def test_build_default_auth_provider_prefers_mainsequence_token(monkeypatch):
    monkeypatch.delenv("MAINSEQUENCE_TOKEN", raising=False)
    monkeypatch.delenv("MAINSEQUENCE_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("MAINSEQUENCE_REFRESH_TOKEN", raising=False)
    monkeypatch.delenv("MAINSEQUENCE_AUTH_MODE", raising=False)

    utils = _load_mainsequence_submodule("mainsequence.client.utils")

    monkeypatch.setenv("MAINSEQUENCE_TOKEN", "legacy-token")
    monkeypatch.setenv("MAINSEQUENCE_ACCESS_TOKEN", "jwt-access")
    monkeypatch.setenv("MAINSEQUENCE_REFRESH_TOKEN", "jwt-refresh")
    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "jwt")

    provider = utils.build_default_auth_provider()

    assert isinstance(provider, utils.DRFTokenAuthProvider)
    assert provider.get_headers()["Authorization"] == "Token legacy-token"


def test_auth_loaders_switch_cached_jwt_provider_to_mainsequence_token(monkeypatch):
    monkeypatch.delenv("MAINSEQUENCE_TOKEN", raising=False)
    monkeypatch.delenv("MAINSEQUENCE_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("MAINSEQUENCE_REFRESH_TOKEN", raising=False)
    monkeypatch.delenv("MAINSEQUENCE_AUTH_MODE", raising=False)

    utils = _load_mainsequence_submodule("mainsequence.client.utils")

    loaders = utils.AuthLoaders()
    loaders.use_jwt(access="jwt-access", refresh="jwt-refresh")

    monkeypatch.setenv("MAINSEQUENCE_TOKEN", "legacy-token")
    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "jwt")

    headers = loaders.refresh_headers()

    assert headers["Authorization"] == "Token legacy-token"
    assert isinstance(loaders.provider, utils.DRFTokenAuthProvider)


def test_logconf_prefers_mainsequence_token_over_jwt_refresh(monkeypatch):
    monkeypatch.delenv("MAINSEQUENCE_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("MAINSEQUENCE_REFRESH_TOKEN", raising=False)
    monkeypatch.delenv("MAINSEQUENCE_AUTH_MODE", raising=False)
    monkeypatch.setenv("MAINSEQUENCE_TOKEN", "legacy-token")
    monkeypatch.setenv("MAINSEQUENCE_REFRESH_TOKEN", "jwt-refresh")

    get_calls: list[dict] = []
    post_calls: list[dict] = []

    def _fake_get(url, **kwargs):
        get_calls.append({"url": url, **kwargs})
        return _FakeResponse(200, {})

    def _fake_post(url, **kwargs):
        post_calls.append({"url": url, **kwargs})
        return _FakeResponse(200, {"access": "jwt-access"})

    monkeypatch.setattr(requests, "get", _fake_get)
    monkeypatch.setattr(requests, "post", _fake_post)

    _load_mainsequence_submodule("mainsequence.logconf")

    assert post_calls == []
    assert get_calls
    assert get_calls[0]["headers"]["Authorization"] == "Token legacy-token"
