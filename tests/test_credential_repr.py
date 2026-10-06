"""Credentials stay out of repr(), str(), log lines and validation errors.

Each object below holds a credential and still uses it: only the text the object
becomes when it is printed, formatted or logged leaves the credential out.
"""

from __future__ import annotations

import logging

import pydantic
import pytest

from mainsequence.cli.browser_auth import BrowserAuthCallback
from mainsequence.client.agent_runtime_models import AgentSessionRuntimeAccess
from mainsequence.client.models_foundry import Secret
from mainsequence.client.models_helpers import ResourceReleaseRuntimeAccess
from mainsequence.client.utils import JWTAuthProvider, SessionJWTAuthProvider
from tests.client.support import _ready_runtime_contract, _runtime_access_payload

ACCESS_TOKEN = "access-token-5e2a"
REFRESH_TOKEN = "refresh-token-9b4f"
GATEWAY_TOKEN = "gateway-token-3a8e"
RELEASE_TOKEN = "release-token-6d0c"
AUTHORIZATION_CODE = "authorization-code-2f7b"
SECRET_VALUE = "secret-value-8e5a"


def _shown(obj, caplog) -> str:
    """Return repr(), str() and a log line that formats the object both ways."""
    caplog.clear()
    caplog.set_level(logging.INFO, logger=__name__)
    logging.getLogger(__name__).info("%s %r", obj, obj)
    return "\n".join((repr(obj), str(obj), caplog.text))


def _error_text(raised: pytest.ExceptionInfo) -> str:
    return f"{raised.value}\n{raised.value!r}"


def test_jwt_provider_leaves_its_tokens_out(monkeypatch, caplog):
    monkeypatch.setenv("MAINSEQUENCE_ACCESS_TOKEN", ACCESS_TOKEN)
    monkeypatch.setenv("MAINSEQUENCE_REFRESH_TOKEN", REFRESH_TOKEN)

    for provider in (
        JWTAuthProvider(),
        JWTAuthProvider(access_token=ACCESS_TOKEN, refresh_token=REFRESH_TOKEN),
    ):
        shown = _shown(provider, caplog)

        assert provider.get_headers()["Authorization"] == f"Bearer {ACCESS_TOKEN}"
        assert provider.refresh_token == REFRESH_TOKEN
        assert ACCESS_TOKEN not in shown
        assert REFRESH_TOKEN not in shown
        assert "header_keyword='Bearer'" in shown


def test_session_jwt_provider_leaves_its_token_out(monkeypatch, caplog):
    monkeypatch.setenv("MAINSEQUENCE_ACCESS_TOKEN", ACCESS_TOKEN)
    monkeypatch.delenv("MAINSEQUENCE_REFRESH_TOKEN", raising=False)
    provider = SessionJWTAuthProvider()

    shown = _shown(provider, caplog)

    assert provider.get_headers()["Authorization"] == f"Bearer {ACCESS_TOKEN}"
    assert ACCESS_TOKEN not in shown
    assert "header_keyword='Bearer'" in shown


def test_agent_session_runtime_access_leaves_its_token_out(caplog):
    access = AgentSessionRuntimeAccess.model_validate(
        {
            "mode": "token",
            "rpc_url": "https://runtime.example/",
            "token": GATEWAY_TOKEN,
            **_ready_runtime_contract(),
        }
    )

    shown = _shown(access, caplog)

    assert access.token == GATEWAY_TOKEN
    assert access.model_dump()["token"] == GATEWAY_TOKEN
    assert GATEWAY_TOKEN not in shown
    assert "rpc_url='https://runtime.example/'" in shown


def test_agent_session_runtime_access_validation_error_leaves_the_token_out():
    with pytest.raises(pydantic.ValidationError) as raised:
        AgentSessionRuntimeAccess.model_validate({"mode": "token", "token": GATEWAY_TOKEN})

    assert "runtime_interaction" in str(raised.value)
    assert GATEWAY_TOKEN not in _error_text(raised)


def _release_runtime_access_payload() -> dict:
    payload = _runtime_access_payload(state="ready", can_request=True)
    payload["access"]["token"] = RELEASE_TOKEN
    return payload


def test_resource_release_runtime_access_leaves_its_token_out(caplog):
    access = ResourceReleaseRuntimeAccess.model_validate(_release_runtime_access_payload())

    shown = _shown(access, caplog)

    assert access.access["token"] == RELEASE_TOKEN
    assert access.model_dump()["access"]["token"] == RELEASE_TOKEN
    assert RELEASE_TOKEN not in shown
    assert "resource_release_uid='2f4c4c3d-5669-4da5-9d86-b84633c1e6ed'" in shown


def test_resource_release_runtime_access_validation_error_leaves_the_token_out():
    payload = _release_runtime_access_payload()
    del payload["routing"]
    # A shown input is cut in its middle; this order puts the token at its end.
    payload["access"] = {"mode": "token", "token": RELEASE_TOKEN}

    with pytest.raises(pydantic.ValidationError) as raised:
        ResourceReleaseRuntimeAccess.model_validate(payload)

    assert "routing" in str(raised.value)
    assert RELEASE_TOKEN not in _error_text(raised)


def test_secret_leaves_its_value_out(caplog):
    secret = Secret(name="POLYGON_API_KEY", value=SECRET_VALUE)

    shown = _shown(secret, caplog)

    assert secret.value.get_secret_value() == SECRET_VALUE
    assert SECRET_VALUE not in shown
    with pytest.raises(pydantic.ValidationError) as raised:
        Secret.model_validate({"value": SECRET_VALUE})
    assert "name" in str(raised.value)
    assert SECRET_VALUE not in _error_text(raised)


def test_browser_auth_callback_leaves_the_authorization_code_out(caplog):
    callback = BrowserAuthCallback(code=AUTHORIZATION_CODE, state="state-1")

    shown = _shown(callback, caplog)

    assert callback.code == AUTHORIZATION_CODE
    assert AUTHORIZATION_CODE not in shown
    assert "state='state-1'" in shown
