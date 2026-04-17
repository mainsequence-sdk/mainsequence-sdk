from __future__ import annotations

import json
from urllib.parse import parse_qs, urlparse

import pytest

from mainsequence.cli import api as cli_api
from mainsequence.cli import browser_auth as auth


def test_pkce_verifier_and_challenge_shape():
    verifier = auth._pkce_code_verifier()
    challenge = auth._pkce_code_challenge(verifier)

    assert 43 <= len(verifier) <= 128
    assert challenge
    assert "=" not in challenge


def test_build_cli_authorize_url_includes_client_id(monkeypatch):
    monkeypatch.setattr(cli_api, "backend_url", lambda: "http://127.0.0.1:8000")
    url = cli_api.build_cli_authorize_url(
        redirect_uri="http://127.0.0.1:61533/callback",
        state="abc123",
        code_challenge="challenge-xyz",
    )
    parsed = urlparse(url)
    query = parse_qs(parsed.query)

    assert parsed.path == "/auth/cli/authorize/"
    assert query["client_id"] == ["mainsequence-cli"]
    assert query["redirect_uri"] == ["http://127.0.0.1:61533/callback"]
    assert query["state"] == ["abc123"]
    assert query["code_challenge"] == ["challenge-xyz"]
    assert query["code_challenge_method"] == ["S256"]


def test_login_via_browser_no_open_success(monkeypatch):
    seen = {}
    emitted_urls: list[str] = []

    class _FakeServer:
        redirect_uri = "http://127.0.0.1:43123/callback"

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def wait_for_callback(self, *, timeout_seconds):
            seen["timeout"] = timeout_seconds
            return auth.BrowserAuthCallback(code="auth-code", state="expected-state")

    monkeypatch.setattr(auth, "_state_token", lambda: "expected-state")
    monkeypatch.setattr(auth, "_pkce_code_verifier", lambda: "verifier-value")
    monkeypatch.setattr(auth, "_LoopbackAuthServer", _FakeServer)
    monkeypatch.setattr(
        auth,
        "build_cli_authorize_url",
        lambda **kwargs: seen.update({"authorize_kwargs": kwargs}) or "https://example.test/authorize",
    )
    monkeypatch.setattr(
        auth,
        "exchange_cli_authorization_code",
        lambda **kwargs: seen.update({"exchange_kwargs": kwargs}) or {"access": "acc", "refresh": "ref"},
    )

    out = auth.login_via_browser(no_open=True, on_authorize_url=emitted_urls.append)
    assert out["access"] == "acc"
    assert out["refresh"] == "ref"
    assert emitted_urls == ["https://example.test/authorize"]
    assert seen["authorize_kwargs"]["state"] == "expected-state"
    assert seen["authorize_kwargs"]["client_id"] == "mainsequence-cli"
    assert seen["exchange_kwargs"] == {
        "client_id": "mainsequence-cli",
        "code": "auth-code",
        "code_verifier": "verifier-value",
        "redirect_uri": "http://127.0.0.1:43123/callback",
    }


def test_exchange_cli_authorization_code_includes_client_id(monkeypatch):
    monkeypatch.setattr(cli_api, "backend_url", lambda: "http://127.0.0.1:8000")
    captured: dict[str, object] = {}

    class _Resp:
        ok = True
        status_code = 200
        headers = {"content-type": "application/json"}
        text = ""

        @staticmethod
        def json():
            return {"access": "acc", "refresh": "ref"}

    def _post(url, data):
        captured["url"] = url
        captured["payload"] = json.loads(data)
        return _Resp()

    monkeypatch.setattr(cli_api.S, "post", _post)
    out = cli_api.exchange_cli_authorization_code(
        code="abc",
        code_verifier="ver",
        redirect_uri="http://127.0.0.1:61533/callback",
    )

    assert out["access"] == "acc"
    assert out["refresh"] == "ref"
    assert captured["url"] == "http://127.0.0.1:8000/auth/cli/token/"
    assert captured["payload"] == {
        "client_id": "mainsequence-cli",
        "code": "abc",
        "code_verifier": "ver",
        "redirect_uri": "http://127.0.0.1:61533/callback",
    }


def test_login_via_browser_state_mismatch(monkeypatch):
    class _FakeServer:
        redirect_uri = "http://127.0.0.1:43123/callback"

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def wait_for_callback(self, *, timeout_seconds):
            return auth.BrowserAuthCallback(code="auth-code", state="unexpected-state")

    monkeypatch.setattr(auth, "_state_token", lambda: "expected-state")
    monkeypatch.setattr(auth, "_pkce_code_verifier", lambda: "verifier-value")
    monkeypatch.setattr(auth, "_LoopbackAuthServer", _FakeServer)
    monkeypatch.setattr(auth, "build_cli_authorize_url", lambda **kwargs: "https://example.test/authorize")

    with pytest.raises(auth.BrowserAuthError, match="state mismatch"):
        auth.login_via_browser(no_open=True)


def test_login_via_browser_timeout(monkeypatch):
    class _FakeServer:
        redirect_uri = "http://127.0.0.1:43123/callback"

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def wait_for_callback(self, *, timeout_seconds):
            return None

    monkeypatch.setattr(auth, "_state_token", lambda: "expected-state")
    monkeypatch.setattr(auth, "_pkce_code_verifier", lambda: "verifier-value")
    monkeypatch.setattr(auth, "_LoopbackAuthServer", _FakeServer)
    monkeypatch.setattr(auth, "build_cli_authorize_url", lambda **kwargs: "https://example.test/authorize")

    with pytest.raises(auth.BrowserAuthError, match="Timed out waiting for browser login callback"):
        auth.login_via_browser(no_open=True)


def test_login_via_browser_open_failure(monkeypatch):
    class _FakeServer:
        redirect_uri = "http://127.0.0.1:43123/callback"

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def wait_for_callback(self, *, timeout_seconds):
            raise AssertionError("wait_for_callback should not run when browser open fails")

    monkeypatch.setattr(auth, "_state_token", lambda: "expected-state")
    monkeypatch.setattr(auth, "_pkce_code_verifier", lambda: "verifier-value")
    monkeypatch.setattr(auth, "_LoopbackAuthServer", _FakeServer)
    monkeypatch.setattr(auth, "build_cli_authorize_url", lambda **kwargs: "https://example.test/authorize")
    monkeypatch.setattr(auth.webbrowser, "open", lambda *args, **kwargs: False)

    with pytest.raises(auth.BrowserAuthError, match="--no-open"):
        auth.login_via_browser(no_open=False)
