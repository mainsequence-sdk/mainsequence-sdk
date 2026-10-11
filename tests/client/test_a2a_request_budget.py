"""A2A calls end within one total budget, measured at the real HTTP boundary."""

import json
import time

import pytest
import requests

import mainsequence.client.agent_runtime_models as agent_models_mod
from tests._http_server import answer_json, serve, trickle_body, trickle_headers
from tests.client.support import _ready_runtime_contract

SESSION_UID = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
MESSAGE_PATH = "/api/a2a/v1/message:send"
TASK_PATH = "/api/a2a/v1/tasks/task-1"
CANCEL_PATH = "/api/a2a/v1/tasks/task-1:cancel"


@pytest.fixture
def ready_runtime():
    """Cache ready runtime access that points at the given test server."""

    def cache(rpc_url):
        agent_models_mod.AgentSession.cache_runtime_access(
            SESSION_UID,
            {
                "mode": "token",
                "rpc_url": rpc_url,
                "token": "tok-secret",
                **_ready_runtime_contract(),
            },
        )

    agent_models_mod.AgentSession.clear_cached_runtime_access(SESSION_UID)
    yield cache
    agent_models_mod.AgentSession.clear_cached_runtime_access(SESSION_UID)


def _send(timeout):
    return agent_models_mod.AgentSession.send_a2a_message(
        SESSION_UID, message="Summarize the portfolio.", timeout=timeout
    )


def _get(timeout):
    return agent_models_mod.AgentSession.get_a2a_task(
        SESSION_UID, task_id="task-1", timeout=timeout
    )


def _cancel(timeout):
    return agent_models_mod.AgentSession.cancel_a2a_task(
        SESSION_UID, task_id="task-1", timeout=timeout
    )


@pytest.mark.parametrize(
    ("method", "path", "call"),
    [("POST", MESSAGE_PATH, _send), ("GET", TASK_PATH, _get), ("POST", CANCEL_PATH, _cancel)],
    ids=["send", "task-get", "task-cancel"],
)
@pytest.mark.parametrize(
    "route",
    [trickle_body(interval=0.05, size=400), trickle_headers(interval=0.05)],
    ids=["trickled-body", "trickled-headers"],
)
def test_numeric_timeout_is_one_total_budget_while_the_runtime_trickles(
    ready_runtime, method, path, call, route
):
    # A byte every 50 ms never trips a per-read limit, so only a total deadline ends the call.
    with serve({(method, path): route}) as (url, calls):
        ready_runtime(url)
        started = time.monotonic()
        with pytest.raises(requests.Timeout):
            call(0.4)
        elapsed = time.monotonic() - started

    assert 0.35 <= elapsed < 0.8
    assert [(sent_method, sent_path) for sent_method, sent_path, _body in calls] == [(method, path)]


def test_tuple_timeout_total_is_the_sum_of_its_phases(ready_runtime):
    with serve({("POST", MESSAGE_PATH): trickle_body(interval=0.05, size=400)}) as (url, calls):
        ready_runtime(url)
        started = time.monotonic()
        with pytest.raises(requests.Timeout):
            _send((0.1, 0.3))
        elapsed = time.monotonic() - started

    assert 0.35 <= elapsed < 0.8
    assert len(calls) == 1


def test_send_reads_a_complete_answer_within_its_budget(ready_runtime):
    message = {
        "messageId": "msg-runtime-output",
        "role": "ROLE_AGENT",
        "contextId": SESSION_UID,
        "parts": [{"text": "Done."}],
    }
    route = answer_json(200, {"message": message}, content_type="application/a2a+json")
    with serve({("POST", MESSAGE_PATH): route}) as (url, calls):
        ready_runtime(url)
        result = _send(5)

    assert result.message == message
    assert len(calls) == 1
    assert json.loads(calls[0][2])["configuration"]["responseKind"] == "message"
