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


def test_start_mcp_cli_handoff_uses_backend_without_redirect_input(monkeypatch):
    monkeypatch.setattr(cli_api, "backend_url", lambda: "https://api.example.test")
    captured: dict[str, object] = {}

    class _Resp:
        ok = True
        status_code = 201
        text = ""

        @staticmethod
        def json():
            return {
                "handoff_uid": "00000000-0000-4000-8000-000000000001",
                "status": "pending",
                "redirect_uri": "https://api.example.test/auth/mcp/cli-handoff/callback/",
                "expires_at": "2026-08-01T10:00:00Z",
                "mcp_tool": "auth.cli_authorize",
                "mcp_arguments": {
                    "handoff_uid": "00000000-0000-4000-8000-000000000001"
                },
            }

    def _post(url, data):
        captured["url"] = url
        captured["payload"] = json.loads(data)
        return _Resp()

    monkeypatch.setattr(cli_api.S, "post", _post)
    result = cli_api.start_mcp_cli_handoff(
        state="opaque-state",
        code_challenge="challenge",
    )

    assert captured["url"] == "https://api.example.test/auth/mcp/cli-handoff/start/"
    assert captured["payload"] == {
        "state": "opaque-state",
        "code_challenge": "challenge",
        "code_challenge_method": "S256",
    }
    assert "redirect_uri" not in captured["payload"]
    assert result["mcp_tool"] == "auth.cli_authorize"


def test_poll_mcp_cli_handoff_uses_exact_backend_callback(monkeypatch):
    monkeypatch.setattr(cli_api, "backend_url", lambda: "https://api.example.test")
    responses = iter(
        [
            (202, {"status": "pending"}),
            (200, {"access": "access", "refresh": "refresh", "user": {"username": "agent"}}),
        ]
    )
    captured: list[tuple[str, dict]] = []

    class _Resp:
        text = ""

        def __init__(self, status_code, payload):
            self.status_code = status_code
            self.ok = 200 <= status_code < 300
            self._payload = payload

        def json(self):
            return self._payload

    def _post(url, data):
        captured.append((url, json.loads(data)))
        return _Resp(*next(responses))

    monkeypatch.setattr(cli_api.S, "post", _post)
    kwargs = {
        "redirect_uri": "https://api.example.test/auth/mcp/cli-handoff/callback/",
        "handoff_uid": "00000000-0000-4000-8000-000000000001",
        "state": "opaque-state",
        "code_verifier": "v" * 64,
    }

    assert cli_api.poll_mcp_cli_handoff(**kwargs) is None
    tokens = cli_api.poll_mcp_cli_handoff(**kwargs)

    assert tokens["access"] == "access"
    assert tokens["refresh"] == "refresh"
    assert all(item[0] == kwargs["redirect_uri"] for item in captured)


def test_login_via_mcp_handoff_waits_for_tool_authorization(monkeypatch):
    handoff_uid = "00000000-0000-4000-8000-000000000001"
    handoff = {
        "handoff_uid": handoff_uid,
        "redirect_uri": "https://api.example.test/auth/mcp/cli-handoff/callback/",
        "expires_at": "2026-08-01T10:00:00Z",
        "mcp_tool": "auth.cli_authorize",
        "mcp_arguments": {"handoff_uid": handoff_uid},
    }
    observed: dict[str, object] = {}
    poll_results = iter([None, {"access": "access", "refresh": "refresh"}])
    clock = iter([0.0, 0.0, 0.1])

    monkeypatch.setattr(auth, "_state_token", lambda: "opaque-state")
    monkeypatch.setattr(auth, "_pkce_code_verifier", lambda: "v" * 64)
    monkeypatch.setattr(auth, "_pkce_code_challenge", lambda _value: "challenge")
    monkeypatch.setattr(auth, "backend_url", lambda: "https://api.example.test")
    monkeypatch.setattr(
        auth,
        "start_mcp_cli_handoff",
        lambda **kwargs: observed.update({"start": kwargs}) or handoff,
    )
    monkeypatch.setattr(
        auth,
        "poll_mcp_cli_handoff",
        lambda **kwargs: observed.update({"poll": kwargs}) or next(poll_results),
    )
    monkeypatch.setattr(auth.time, "monotonic", lambda: next(clock))
    monkeypatch.setattr(auth.time, "sleep", lambda _seconds: None)
    emitted: list[dict] = []

    result = auth.login_via_mcp_handoff(
        timeout_seconds=300,
        on_handoff=emitted.append,
    )

    assert observed["start"] == {
        "state": "opaque-state",
        "code_challenge": "challenge",
    }
    assert observed["poll"] == {
        "redirect_uri": handoff["redirect_uri"],
        "handoff_uid": handoff_uid,
        "state": "opaque-state",
        "code_verifier": "v" * 64,
    }
    assert emitted == [handoff]
    assert result["access"] == "access"
    assert result["refresh"] == "refresh"
    assert result["handoff_uid"] == handoff_uid


def test_login_via_mcp_handoff_rejects_cross_origin_callback(monkeypatch):
    handoff_uid = "00000000-0000-4000-8000-000000000001"
    monkeypatch.setattr(auth, "backend_url", lambda: "https://api.example.test")
    monkeypatch.setattr(
        auth,
        "start_mcp_cli_handoff",
        lambda **_kwargs: {
            "handoff_uid": handoff_uid,
            "redirect_uri": "https://other.example.test/callback/",
            "expires_at": "2026-08-01T10:00:00Z",
            "mcp_tool": "auth.cli_authorize",
            "mcp_arguments": {"handoff_uid": handoff_uid},
        },
    )

    with pytest.raises(auth.BrowserAuthError, match="different origin"):
        auth.login_via_mcp_handoff()
