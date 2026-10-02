from contextlib import nullcontext
from unittest.mock import Mock

import pytest
import requests

from mainsequence.client import inference

ENVIRONMENT = "00000000-0000-4000-8000-000000000001"
CONVERSATION = "00000000-0000-4000-8000-000000000002"
REQUEST = "00000000-0000-4000-8000-000000000003"


def result(**extra):
    return {
        "conversation_uid": CONVERSATION,
        "request_uid": REQUEST,
        "sequence": 1,
        "provider": "openai",
        "model": "configured-model",
        "status": "completed",
        "error_code": "",
        "content_available": True,
        "usage": {"complete": False},
        **extra,
    }


@pytest.fixture
def transport(monkeypatch):
    session = Mock()
    response = Mock(status_code=200)
    response.json.return_value = result()
    session.request.return_value = response
    factory = Mock(return_value=nullcontext(session))
    monkeypatch.setattr(inference.utils, "build_session", factory)
    monkeypatch.setattr(
        inference.utils.loaders,
        "refresh_headers",
        Mock(return_value={"Authorization": "test-auth"}),
    )
    monkeypatch.setattr(inference.utils, "API_ENDPOINT", "https://mainsequence.test/api/v1")
    return session, response, factory


@pytest.mark.parametrize(
    "provider,options",
    [
        ("openai", {"store": False, "text": {"verbosity": "low"}}),
        ("anthropic", {"thinking": {"type": "adaptive"}, "output_config": {"effort": "high"}}),
        ("ollama-local", {"reasoning_effort": "high"}),
        ("openrouter", {"provider": {"zdr": True}, "reasoning": {"effort": "high"}}),
    ],
)
def test_native_options_survive_for_each_provider(transport, provider, options):
    options["future_field"] = [None, False, {"new": "value"}]
    session, response, factory = transport
    client = inference.InferenceClient(organization_environment_uid=ENVIRONMENT)
    answer = client.complete(
        provider=provider,
        model="configured-model",
        messages=[{"role": "user", "content": "test"}],
        idempotency_key="one",
        provider_options=options,
    )
    assert answer.request_uid == REQUEST
    kwargs = session.request.call_args.kwargs
    assert kwargs["json"]["provider_options"] == options
    assert "thinking_level" not in kwargs["json"]
    assert kwargs["headers"]["Idempotency-Key"] == "one"
    assert kwargs["params"]["organization_environment_uid"] == ENVIRONMENT
    assert factory.call_args.kwargs["retries"] == 0


def test_common_controls_and_retry_identity(transport):
    session, _, _ = transport
    client = inference.InferenceClient()
    kwargs = dict(
        provider="openai",
        model="configured-model",
        messages=[],
        idempotency_key="stable",
        thinking_level="high",
        provider_storage={"store_response": False},
        provider_options={"future": None},
    )
    client.complete(**kwargs)
    first = session.request.call_args
    client.complete(**kwargs)
    assert session.request.call_args == first
    assert first.kwargs["json"]["thinking_level"] == "high"
    assert first.kwargs["json"]["provider_storage"] == {"store_response": False}


def test_timeout_does_not_retry_or_create_a_new_key(transport):
    session, _, _ = transport
    session.request.side_effect = requests.Timeout()
    with pytest.raises(inference.InferenceTransportError) as exc:
        inference.InferenceClient().complete(
            provider="openai", model="configured-model", messages=[], idempotency_key="stable"
        )
    assert exc.value.idempotency_key == "stable"
    assert session.request.call_count == 1


def test_recorded_failure_preserves_identifiers(transport):
    session, response, _ = transport
    response.status_code = 504
    response.json.return_value = result(status="unknown", error_code="provider_outcome_unknown")
    with pytest.raises(inference.InferenceExecutionError) as exc:
        inference.InferenceClient().complete(
            provider="openai", model="configured-model", messages=[], idempotency_key="stable"
        )
    assert exc.value.result.request_uid == REQUEST
    assert session.request.call_count == 1


@pytest.mark.parametrize(
    "method,path",
    [
        ("conversation", ""),
        ("history", "history/"),
        ("insights", "insights/"),
        ("delete_content", "content/"),
    ],
)
def test_history_operations_use_mainsequence_and_exact_uid(transport, method, path):
    session, _, _ = transport
    getattr(inference.InferenceClient(organization_environment_uid=ENVIRONMENT), method)(
        CONVERSATION
    )
    assert (
        session.request.call_args.args[1]
        == f"https://mainsequence.test/api/v1/inference/conversations/{CONVERSATION}/{path}"
    )
    assert session.request.call_args.args[0] == ("DELETE" if method == "delete_content" else "GET")


def test_authentication_refresh_keeps_body_and_key(transport):
    session, response, _ = transport
    refused = Mock(status_code=401)
    session.request.side_effect = [refused, response]
    inference.InferenceClient().complete(
        provider="openai", model="configured-model", messages=[], idempotency_key="stable"
    )
    assert session.request.call_count == 2
    assert (
        session.request.call_args_list[0].kwargs["json"]
        == session.request.call_args_list[1].kwargs["json"]
    )
    assert session.request.call_args_list[1].kwargs["headers"]["Idempotency-Key"] == "stable"


@pytest.mark.parametrize("invalid", ["html", {}, {"request_uid": REQUEST}])
def test_invalid_success_is_uncertain_and_never_retried(transport, invalid):
    session, response, _ = transport
    if invalid == "html":
        response.json.side_effect = ValueError("Non-JSON reply")
    else:
        response.json.return_value = invalid
    with pytest.raises(inference.InferenceTransportError) as exc:
        inference.InferenceClient().complete(
            provider="openai", model="configured-model", messages=[], idempotency_key="stable"
        )
    assert exc.value.idempotency_key == "stable"
    assert session.request.call_count == 1
    assert session.request.call_args.kwargs["allow_redirects"] is False
