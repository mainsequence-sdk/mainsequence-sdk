"""A direct Message that reaches the platform's Message limit fails with a typed error."""

import copy
import json
from contextlib import ExitStack

import pytest

import mainsequence.client.agent_runtime_models as agent_models_mod
from mainsequence.client.exceptions import ServerError
from tests._http_server import answer_json, serve
from tests.client.support import _ready_runtime_contract

SESSION_UID = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
MESSAGE_PATH = "/api/a2a/v1/message:send"
# The target Agent's answer when a direct Message reaches the platform's limit.
LIMIT_ERROR = {
    "error": {
        "code": 504,
        "status": "DEADLINE_EXCEEDED",
        "message": (
            "The Agent did not answer the Message within 90 seconds. "
            "Use Task mode for longer work."
        ),
        "details": [
            {
                "@type": "type.googleapis.com/google.rpc.ErrorInfo",
                "reason": "MESSAGE_RESPONSE_LIMIT_EXCEEDED",
                "domain": "mainsequence.ai",
                "metadata": {"limitSeconds": "90"},
            }
        ],
    }
}


def _limit_error(**detail_changes):
    payload = copy.deepcopy(LIMIT_ERROR)
    payload["error"]["details"][0].update(detail_changes)
    return payload


@pytest.fixture
def runtime_answering():
    """Serve one answer at the runtime's message:send route; return the requests it got."""
    with ExitStack() as servers:

        def start(status, payload):
            route = answer_json(status, payload, content_type="application/a2a+json")
            url, calls = servers.enter_context(serve({("POST", MESSAGE_PATH): route}))
            agent_models_mod.AgentSession.cache_runtime_access(
                SESSION_UID,
                {
                    "mode": "token",
                    "rpc_url": url,
                    "token": "tok-secret",
                    **_ready_runtime_contract(),
                },
            )
            return calls

        agent_models_mod.AgentSession.clear_cached_runtime_access(SESSION_UID)
        yield start
        agent_models_mod.AgentSession.clear_cached_runtime_access(SESSION_UID)


def test_message_limit_answer_raises_the_typed_error_once(runtime_answering):
    calls = runtime_answering(504, LIMIT_ERROR)

    with pytest.raises(agent_models_mod.A2AMessageLimitExceededError) as error:
        agent_models_mod.AgentSession.send_a2a_message(
            SESSION_UID, message="Run the full analysis.", timeout=5
        )

    assert error.value.limit_seconds == 90
    assert error.value.status_code == 504
    assert isinstance(error.value, ServerError)
    message = str(error.value)
    assert "did not answer within the platform's 90-second limit for a direct Message" in message
    assert 'response_kind="task"' in message
    assert "--response-kind task" in message
    # Sent once, as a direct Message: never retried and never turned into a Task.
    assert len(calls) == 1
    assert json.loads(calls[0][2])["configuration"]["responseKind"] == "message"


def test_message_limit_answer_without_a_limit_still_raises_the_typed_error(runtime_answering):
    runtime_answering(504, _limit_error(metadata={}))

    with pytest.raises(agent_models_mod.A2AMessageLimitExceededError) as error:
        agent_models_mod.AgentSession.send_a2a_message(SESSION_UID, message="Run it.", timeout=5)

    assert error.value.limit_seconds is None
    assert "within the platform's limit for a direct Message" in str(error.value)


@pytest.mark.parametrize(
    ("status", "payload"),
    [
        (504, {"detail": "Gateway timeout"}),
        (502, LIMIT_ERROR),
        (504, _limit_error(domain="example.com")),
        (504, _limit_error(reason="DEADLINE_EXCEEDED")),
        (504, _limit_error(**{"@type": "type.googleapis.com/google.rpc.RetryInfo"})),
    ],
    ids=["gateway-504", "502", "other-domain", "other-reason", "not-error-info"],
)
def test_other_gateway_answers_keep_the_ordinary_server_error(runtime_answering, status, payload):
    calls = runtime_answering(status, payload)

    with pytest.raises(ServerError) as error:
        agent_models_mod.AgentSession.send_a2a_message(SESSION_UID, message="Run it.", timeout=5)

    assert not isinstance(error.value, agent_models_mod.A2AMessageLimitExceededError)
    assert error.value.status_code == status
    assert len(calls) == 1


def test_task_mode_keeps_the_ordinary_server_error(runtime_answering):
    calls = runtime_answering(504, LIMIT_ERROR)

    with pytest.raises(ServerError) as error:
        agent_models_mod.AgentSession.send_a2a_message(
            SESSION_UID,
            message="Run it.",
            response_kind=agent_models_mod.A2AResponseKind.TASK,
            a2a_profile={"supported_response_kinds": ["message", "task"]},
            timeout=5,
        )

    assert not isinstance(error.value, agent_models_mod.A2AMessageLimitExceededError)
    assert len(calls) == 1
