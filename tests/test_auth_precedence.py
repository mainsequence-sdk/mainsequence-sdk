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


def test_build_default_auth_provider_uses_jwt(monkeypatch):
    monkeypatch.delenv("MAINSEQUENCE_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("MAINSEQUENCE_REFRESH_TOKEN", raising=False)
    monkeypatch.delenv("MAINSEQUENCE_AUTH_MODE", raising=False)

    utils = _load_mainsequence_submodule("mainsequence.client.utils")

    monkeypatch.setenv("MAINSEQUENCE_ACCESS_TOKEN", "jwt-access")
    monkeypatch.setenv("MAINSEQUENCE_REFRESH_TOKEN", "jwt-refresh")
    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "jwt")

    provider = utils.build_default_auth_provider()

    assert isinstance(provider, utils.JWTAuthProvider)
    assert provider.get_headers()["Authorization"] == "Bearer jwt-access"


def test_auth_loaders_build_headers_from_env_jwt(monkeypatch):
    monkeypatch.delenv("MAINSEQUENCE_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("MAINSEQUENCE_REFRESH_TOKEN", raising=False)
    monkeypatch.delenv("MAINSEQUENCE_AUTH_MODE", raising=False)

    utils = _load_mainsequence_submodule("mainsequence.client.utils")

    loaders = utils.AuthLoaders()
    monkeypatch.setenv("MAINSEQUENCE_ACCESS_TOKEN", "env-access")
    monkeypatch.setenv("MAINSEQUENCE_REFRESH_TOKEN", "env-refresh")
    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "jwt")

    headers = loaders.refresh_headers()

    assert headers["Authorization"] == "Bearer env-access"
    assert isinstance(loaders.provider, utils.JWTAuthProvider)
    assert loaders.provider.access_token == "env-access"
    assert loaders.provider.refresh_token == "env-refresh"


def test_logconf_refreshes_jwt_before_startup_state_request(monkeypatch):
    monkeypatch.delenv("MAINSEQUENCE_ACCESS_TOKEN", raising=False)
    monkeypatch.setenv("MAINSEQUENCE_REFRESH_TOKEN", "jwt-refresh")
    monkeypatch.setenv("COMMAND_ID", "77")
    monkeypatch.setenv("JOB_RUN_ID", "34")
    monkeypatch.setenv("TDAG_ENDPOINT", "https://backend.example")

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

    assert post_calls
    assert get_calls
    assert get_calls[0]["headers"]["Authorization"] == "Bearer jwt-access"



def test_build_default_auth_provider_defaults_to_jwt_when_mode_unset(monkeypatch):
    monkeypatch.delenv("MAINSEQUENCE_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("MAINSEQUENCE_REFRESH_TOKEN", raising=False)
    monkeypatch.delenv("MAINSEQUENCE_AUTH_MODE", raising=False)

    utils = _load_mainsequence_submodule("mainsequence.client.utils")

    monkeypatch.setenv("MAINSEQUENCE_ACCESS_TOKEN", "jwt-access")
    monkeypatch.setenv("MAINSEQUENCE_REFRESH_TOKEN", "jwt-refresh")

    provider = utils.build_default_auth_provider()

    assert isinstance(provider, utils.JWTAuthProvider)
    assert provider.refresh_token == "jwt-refresh"


def test_build_default_auth_provider_uses_session_jwt(monkeypatch):
    monkeypatch.delenv("MAINSEQUENCE_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("MAINSEQUENCE_REFRESH_TOKEN", raising=False)
    monkeypatch.delenv("MAINSEQUENCE_AUTH_MODE", raising=False)

    utils = _load_mainsequence_submodule("mainsequence.client.utils")

    monkeypatch.setenv("MAINSEQUENCE_ACCESS_TOKEN", "runtime-access")
    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "session_jwt")

    provider = utils.build_default_auth_provider()

    assert isinstance(provider, utils.SessionJWTAuthProvider)
    assert provider.get_headers()["Authorization"] == "Bearer runtime-access"


def test_session_jwt_rejects_refresh_token(monkeypatch):
    monkeypatch.delenv("MAINSEQUENCE_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("MAINSEQUENCE_REFRESH_TOKEN", raising=False)
    monkeypatch.delenv("MAINSEQUENCE_AUTH_MODE", raising=False)

    utils = _load_mainsequence_submodule("mainsequence.client.utils")

    monkeypatch.setenv("MAINSEQUENCE_ACCESS_TOKEN", "runtime-access")
    monkeypatch.setenv("MAINSEQUENCE_REFRESH_TOKEN", "should-not-be-here")
    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "session_jwt")

    try:
        utils.build_default_auth_provider()
        raise AssertionError("Expected AuthError")
    except utils.AuthError as exc:
        assert "MAINSEQUENCE_REFRESH_TOKEN is not allowed" in str(exc)


def test_session_jwt_refresh_headers_do_not_refresh(monkeypatch):
    monkeypatch.delenv("MAINSEQUENCE_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("MAINSEQUENCE_REFRESH_TOKEN", raising=False)
    monkeypatch.delenv("MAINSEQUENCE_AUTH_MODE", raising=False)

    utils = _load_mainsequence_submodule("mainsequence.client.utils")

    monkeypatch.setenv("MAINSEQUENCE_ACCESS_TOKEN", "runtime-access")
    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "session_jwt")

    loaders = utils.AuthLoaders()
    headers = loaders.refresh_headers()

    assert headers["Authorization"] == "Bearer runtime-access"
    assert isinstance(loaders.provider, utils.SessionJWTAuthProvider)


def test_logconf_session_jwt_does_not_refresh(monkeypatch):
    monkeypatch.setenv("MAINSEQUENCE_ACCESS_TOKEN", "runtime-access")
    monkeypatch.delenv("MAINSEQUENCE_REFRESH_TOKEN", raising=False)
    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "session_jwt")
    monkeypatch.setenv("JOB_RUN_ID", "34")
    monkeypatch.setenv("COMMAND_ID", "12")
    monkeypatch.setenv("TDAG_ENDPOINT", "https://backend.example")

    get_calls: list[dict] = []
    post_calls: list[dict] = []

    def _fake_get(url, **kwargs):
        get_calls.append({"url": url, **kwargs})
        return _FakeResponse(200, {})

    def _fake_post(url, **kwargs):
        post_calls.append({"url": url, **kwargs})
        return _FakeResponse(200, {"access": "unexpected"})

    monkeypatch.setattr(requests, "get", _fake_get)
    monkeypatch.setattr(requests, "post", _fake_post)

    _load_mainsequence_submodule("mainsequence.logconf")

    assert get_calls
    assert post_calls == []
    assert get_calls[0]["headers"]["Authorization"] == "Bearer runtime-access"


def test_logconf_session_jwt_rejects_refresh_token(monkeypatch):
    monkeypatch.delenv("MAINSEQUENCE_ACCESS_TOKEN", raising=False)
    monkeypatch.setenv("MAINSEQUENCE_REFRESH_TOKEN", "bad-refresh")
    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "session_jwt")
    monkeypatch.setenv("JOB_RUN_ID", "34")

    try:
        _load_mainsequence_submodule("mainsequence.logconf")
        raise AssertionError("Expected RuntimeError")
    except RuntimeError as exc:
        assert "MAINSEQUENCE_REFRESH_TOKEN is not allowed" in str(exc)
