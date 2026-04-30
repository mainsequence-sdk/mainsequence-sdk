from __future__ import annotations

import base64
import importlib
import json
import os
import pathlib
import sys
import time
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


def _jwt_with_exp(exp: int) -> str:
    header = base64.urlsafe_b64encode(json.dumps({"alg": "none"}).encode()).rstrip(b"=").decode()
    payload = base64.urlsafe_b64encode(json.dumps({"exp": exp}).encode()).rstrip(b"=").decode()
    return f"{header}.{payload}.signature"


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
    monkeypatch.setenv("MAINSEQUENCE_ENDPOINT", "https://backend.example")

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


def test_logconf_runtime_credential_exchanges_before_startup_state_request(monkeypatch):
    monkeypatch.delenv("MAINSEQUENCE_ACCESS_TOKEN", raising=False)
    monkeypatch.setenv("MAINSEQUENCE_REFRESH_TOKEN", "must-not-be-used")
    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "runtime_credential")
    monkeypatch.setenv("MAINSEQUENCE_RUNTIME_CREDENTIAL_ID", "cred-id")
    monkeypatch.setenv("MAINSEQUENCE_RUNTIME_CREDENTIAL_SECRET", "cred-secret")
    monkeypatch.setenv("COMMAND_ID", "77")
    monkeypatch.setenv("JOB_RUN_ID", "34")
    monkeypatch.setenv("MAINSEQUENCE_ENDPOINT", "https://backend.example")

    get_calls: list[dict] = []
    post_calls: list[dict] = []

    def _fake_get(url, **kwargs):
        get_calls.append({"url": url, **kwargs})
        return _FakeResponse(200, {})

    def _fake_post(url, **kwargs):
        post_calls.append({"url": url, **kwargs})
        return _FakeResponse(200, {"access": "runtime-access", "expires_in": 300})

    monkeypatch.setattr(requests, "get", _fake_get)
    monkeypatch.setattr(requests, "post", _fake_post)

    _load_mainsequence_submodule("mainsequence.logconf")

    assert post_calls == [
        {
            "url": "https://backend.example/orm/api/pods/runtime-credentials/token/",
            "headers": {"Content-Type": "application/json"},
            "json": {
                "credential_id": "cred-id",
                "credential_secret": "cred-secret",
            },
            "timeout": 10.0,
        }
    ]
    assert os.environ["MAINSEQUENCE_ACCESS_TOKEN"] == "runtime-access"
    assert get_calls
    assert get_calls[0]["headers"]["Authorization"] == "Bearer runtime-access"


def test_logconf_runtime_credential_retries_after_auth_failure(monkeypatch):
    monkeypatch.setenv("MAINSEQUENCE_ACCESS_TOKEN", "stale-runtime-access")
    monkeypatch.setenv("MAINSEQUENCE_REFRESH_TOKEN", "must-not-be-used")
    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "runtime_credential")
    monkeypatch.setenv("MAINSEQUENCE_RUNTIME_CREDENTIAL_ID", "cred-id")
    monkeypatch.setenv("MAINSEQUENCE_RUNTIME_CREDENTIAL_SECRET", "cred-secret")
    monkeypatch.setenv("COMMAND_ID", "77")
    monkeypatch.setenv("JOB_RUN_ID", "34")
    monkeypatch.setenv("MAINSEQUENCE_ENDPOINT", "https://backend.example")

    get_calls: list[dict] = []
    post_calls: list[dict] = []

    def _fake_get(url, **kwargs):
        get_calls.append({"url": url, **kwargs})
        status = 401 if len(get_calls) == 1 else 200
        return _FakeResponse(status, {})

    def _fake_post(url, **kwargs):
        post_calls.append({"url": url, **kwargs})
        return _FakeResponse(200, {"access": "fresh-runtime-access", "expires_in": 300})

    monkeypatch.setattr(requests, "get", _fake_get)
    monkeypatch.setattr(requests, "post", _fake_post)

    _load_mainsequence_submodule("mainsequence.logconf")

    assert len(get_calls) == 2
    assert len(post_calls) == 1
    assert get_calls[0]["headers"]["Authorization"] == "Bearer stale-runtime-access"
    assert get_calls[1]["headers"]["Authorization"] == "Bearer fresh-runtime-access"
    assert os.environ["MAINSEQUENCE_ACCESS_TOKEN"] == "fresh-runtime-access"


def test_runtime_credential_provider_exchanges_and_writes_access_token(monkeypatch):
    monkeypatch.delenv("MAINSEQUENCE_ACCESS_TOKEN", raising=False)
    monkeypatch.setenv("MAINSEQUENCE_REFRESH_TOKEN", "must-not-be-used")
    monkeypatch.setenv("MAINSEQUENCE_RUNTIME_CREDENTIAL_ID", "cred-id")
    monkeypatch.setenv("MAINSEQUENCE_RUNTIME_CREDENTIAL_SECRET", "cred-secret")

    utils = _load_mainsequence_submodule("mainsequence.client.utils")
    calls: list[dict] = []

    def _fake_post(url, **kwargs):
        calls.append({"url": url, **kwargs})
        return _FakeResponse(
            200,
            {
                "access": "runtime-access",
                "token_type": "Bearer",
                "expires_in": 300,
            },
        )

    monkeypatch.setattr(requests, "post", _fake_post)

    provider = utils.RuntimeCredentialAuthProvider()
    headers = provider.get_headers()

    assert headers["Authorization"] == "Bearer runtime-access"
    assert calls[0]["json"] == {
        "credential_id": "cred-id",
        "credential_secret": "cred-secret",
    }
    assert calls[0]["url"].endswith("/orm/api/pods/runtime-credentials/token/")
    assert calls[0]["headers"]["Content-Type"] == "application/json"
    assert os.environ["MAINSEQUENCE_ACCESS_TOKEN"] == "runtime-access"
    assert os.environ["MAINSEQUENCE_REFRESH_TOKEN"] == "must-not-be-used"


def test_runtime_credential_provider_reuses_valid_exchanged_access_token(monkeypatch):
    monkeypatch.delenv("MAINSEQUENCE_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("MAINSEQUENCE_REFRESH_TOKEN", raising=False)
    monkeypatch.setenv("MAINSEQUENCE_RUNTIME_CREDENTIAL_ID", "cred-id")
    monkeypatch.setenv("MAINSEQUENCE_RUNTIME_CREDENTIAL_SECRET", "cred-secret")

    utils = _load_mainsequence_submodule("mainsequence.client.utils")
    calls = {"count": 0}

    def _fake_post(url, **kwargs):
        calls["count"] += 1
        return _FakeResponse(
            200,
            {
                "access": f"runtime-access-{calls['count']}",
                "token_type": "Bearer",
                "expires_in": 300,
            },
        )

    monkeypatch.setattr(requests, "post", _fake_post)

    provider = utils.RuntimeCredentialAuthProvider()
    first_headers = provider.get_headers()
    second_headers = provider.get_headers()

    assert first_headers["Authorization"] == "Bearer runtime-access-1"
    assert second_headers["Authorization"] == "Bearer runtime-access-1"
    assert calls["count"] == 1


def test_runtime_credential_provider_force_refresh_exchanges_again(monkeypatch):
    monkeypatch.delenv("MAINSEQUENCE_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("MAINSEQUENCE_REFRESH_TOKEN", raising=False)
    monkeypatch.setenv("MAINSEQUENCE_RUNTIME_CREDENTIAL_ID", "cred-id")
    monkeypatch.setenv("MAINSEQUENCE_RUNTIME_CREDENTIAL_SECRET", "cred-secret")

    utils = _load_mainsequence_submodule("mainsequence.client.utils")
    calls = {"count": 0}

    def _fake_post(url, **kwargs):
        calls["count"] += 1
        return _FakeResponse(
            200,
            {
                "access": f"runtime-access-{calls['count']}",
                "token_type": "Bearer",
                "expires_in": 300,
            },
        )

    monkeypatch.setattr(requests, "post", _fake_post)

    provider = utils.RuntimeCredentialAuthProvider()
    provider.refresh()
    provider.refresh(force=True)

    assert os.environ["MAINSEQUENCE_ACCESS_TOKEN"] == "runtime-access-2"
    assert calls["count"] == 2


def test_runtime_credential_provider_exchanges_near_expiry_access_token(monkeypatch):
    monkeypatch.setenv("MAINSEQUENCE_ACCESS_TOKEN", _jwt_with_exp(int(time.time()) + 10))
    monkeypatch.setenv("MAINSEQUENCE_REFRESH_TOKEN", "must-not-be-used")
    monkeypatch.setenv("MAINSEQUENCE_RUNTIME_CREDENTIAL_ID", "cred-id")
    monkeypatch.setenv("MAINSEQUENCE_RUNTIME_CREDENTIAL_SECRET", "cred-secret")

    utils = _load_mainsequence_submodule("mainsequence.client.utils")
    calls: list[dict] = []

    def _fake_post(url, **kwargs):
        calls.append({"url": url, **kwargs})
        return _FakeResponse(
            200,
            {
                "access": "runtime-access",
                "token_type": "Bearer",
                "expires_in": 300,
            },
        )

    monkeypatch.setattr(requests, "post", _fake_post)

    provider = utils.RuntimeCredentialAuthProvider()
    headers = provider.get_headers()

    assert headers["Authorization"] == "Bearer runtime-access"
    assert os.environ["MAINSEQUENCE_ACCESS_TOKEN"] == "runtime-access"
    assert os.environ["MAINSEQUENCE_REFRESH_TOKEN"] == "must-not-be-used"
    assert len(calls) == 1


def test_runtime_credential_make_request_401_forces_exchange_and_retry(monkeypatch):
    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "runtime_credential")
    monkeypatch.setenv("MAINSEQUENCE_ACCESS_TOKEN", "stale-runtime-access")
    monkeypatch.setenv("MAINSEQUENCE_REFRESH_TOKEN", "must-not-be-used")
    monkeypatch.setenv("MAINSEQUENCE_RUNTIME_CREDENTIAL_ID", "cred-id")
    monkeypatch.setenv("MAINSEQUENCE_RUNTIME_CREDENTIAL_SECRET", "cred-secret")

    utils = _load_mainsequence_submodule("mainsequence.client.utils")
    post_calls: list[dict] = []

    def _fake_post(url, **kwargs):
        post_calls.append({"url": url, **kwargs})
        return _FakeResponse(
            200,
            {
                "access": "fresh-runtime-access",
                "token_type": "Bearer",
                "expires_in": 300,
            },
        )

    class _FakeSession:
        def __init__(self):
            self.headers = requests.structures.CaseInsensitiveDict()
            self.get_calls: list[dict] = []

        def get(self, url, **kwargs):
            self.get_calls.append(
                {
                    "url": url,
                    "authorization": self.headers.get("Authorization"),
                    **kwargs,
                }
            )
            status = 401 if len(self.get_calls) == 1 else 200
            return _FakeResponse(status, {"ok": status == 200})

    monkeypatch.setattr(requests, "post", _fake_post)

    session = _FakeSession()
    response = utils.make_request(
        session,
        "GET",
        "https://backend.example/orm/api/example/",
        utils.AuthLoaders(),
    )

    assert response.status_code == 200
    assert [call["authorization"] for call in session.get_calls] == [
        "Bearer stale-runtime-access",
        "Bearer fresh-runtime-access",
    ]
    assert len(post_calls) == 1
    assert post_calls[0]["json"] == {
        "credential_id": "cred-id",
        "credential_secret": "cred-secret",
    }
    assert os.environ["MAINSEQUENCE_ACCESS_TOKEN"] == "fresh-runtime-access"
    assert os.environ["MAINSEQUENCE_REFRESH_TOKEN"] == "must-not-be-used"


def test_runtime_credential_provider_requires_credential_env(monkeypatch):
    monkeypatch.delenv("MAINSEQUENCE_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("MAINSEQUENCE_REFRESH_TOKEN", raising=False)
    monkeypatch.delenv("MAINSEQUENCE_RUNTIME_CREDENTIAL_ID", raising=False)
    monkeypatch.delenv("MAINSEQUENCE_RUNTIME_CREDENTIAL_SECRET", raising=False)

    utils = _load_mainsequence_submodule("mainsequence.client.utils")

    provider = utils.RuntimeCredentialAuthProvider()
    try:
        provider.get_headers()
        raise AssertionError("Expected AuthError")
    except utils.AuthError as exc:
        assert "MAINSEQUENCE_RUNTIME_CREDENTIAL_ID is required" in str(exc)


def test_build_default_auth_provider_uses_runtime_credential_mode(monkeypatch):
    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "runtime_credential")
    monkeypatch.setenv("MAINSEQUENCE_ACCESS_TOKEN", "jwt-access")
    monkeypatch.setenv("MAINSEQUENCE_REFRESH_TOKEN", "jwt-refresh")
    monkeypatch.setenv("MAINSEQUENCE_RUNTIME_CREDENTIAL_ID", "cred-id")
    monkeypatch.setenv("MAINSEQUENCE_RUNTIME_CREDENTIAL_SECRET", "cred-secret")

    utils = _load_mainsequence_submodule("mainsequence.client.utils")

    provider = utils.build_default_auth_provider()

    assert isinstance(provider, utils.RuntimeCredentialAuthProvider)
    assert provider.credential_id == "cred-id"
    assert provider.credential_secret == "cred-secret"


def test_auth_loaders_switch_to_runtime_credential_mode(monkeypatch):
    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "runtime_credential")
    monkeypatch.setenv("MAINSEQUENCE_ACCESS_TOKEN", "jwt-access")
    monkeypatch.setenv("MAINSEQUENCE_REFRESH_TOKEN", "jwt-refresh")
    monkeypatch.setenv("MAINSEQUENCE_RUNTIME_CREDENTIAL_ID", "cred-id")
    monkeypatch.setenv("MAINSEQUENCE_RUNTIME_CREDENTIAL_SECRET", "cred-secret")

    utils = _load_mainsequence_submodule("mainsequence.client.utils")
    loaders = utils.AuthLoaders()
    loaders.use_jwt(access="old-access", refresh="old-refresh")

    provider = loaders._provider()

    assert isinstance(provider, utils.RuntimeCredentialAuthProvider)
    assert provider.credential_id == "cred-id"
    assert provider.credential_secret == "cred-secret"


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
    monkeypatch.setenv("MAINSEQUENCE_ENDPOINT", "https://backend.example")

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
