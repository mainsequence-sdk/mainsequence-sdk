"""A2A calls end within one total budget, measured at the real HTTP boundary."""

import json
import time

import pytest
import requests

import mainsequence.client.agent_runtime_models as agent_models_mod
from mainsequence.client import _transport as transport
from mainsequence.client import utils
from tests._http_server import (
    answer_json,
    answer_sequence,
    serve,
    trickle_body,
    trickle_headers,
)
from tests.client.support import _ready_runtime_contract

SESSION_UID = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
MESSAGE_PATH = "/api/a2a/v1/message:send"
TASK_PATH = "/api/a2a/v1/tasks/task-1"
CANCEL_PATH = "/api/a2a/v1/tasks/task-1:cancel"
RESOLVE_PATH = f"/agent-sessions/{SESSION_UID}/resolve-runtime-access/"


def _waking_access(retry_after_ms):
    return {
        "mode": "unavailable",
        "rpc_url": None,
        "token": None,
        "runtime_interaction": {
            "state": "waking",
            "can_submit": False,
            "notice": None,
            "operation": None,
            "retry_after_ms": retry_after_ms,
        },
        "runtime_presence": {
            "phase": "starting",
            "replicas": {"desired": 1, "actual": 0},
            "detail": "The runtime is starting.",
            "observed_at": "2026-10-11T10:00:00Z",
            "wake": None,
        },
    }


def _ready_access(rpc_url):
    return {"mode": "token", "rpc_url": rpc_url, "token": "tok-secret", **_ready_runtime_contract()}


@pytest.fixture
def ready_runtime():
    """Cache ready runtime access that points at the given test server."""

    def cache(rpc_url):
        agent_models_mod.AgentSession.cache_runtime_access(SESSION_UID, _ready_access(rpc_url))

    agent_models_mod.AgentSession.clear_cached_runtime_access(SESSION_UID)
    yield cache
    agent_models_mod.AgentSession.clear_cached_runtime_access(SESSION_UID)


@pytest.fixture
def platform_at(monkeypatch):
    """Resolve runtime access through the platform route of the given test server."""
    sessions = []

    def point(url):
        session = utils.build_session()
        sessions.append(session)
        monkeypatch.setattr(
            agent_models_mod.AgentSession,
            "get_object_url",
            classmethod(lambda cls, custom_endpoint_name=None: f"{url}agent-sessions"),
        )
        monkeypatch.setattr(
            agent_models_mod.AgentSession, "build_session", classmethod(lambda cls: session)
        )
        monkeypatch.setattr(agent_models_mod.AgentSession, "LOADERS", None)

    agent_models_mod.AgentSession.clear_cached_runtime_access(SESSION_UID)
    yield point
    agent_models_mod.AgentSession.clear_cached_runtime_access(SESSION_UID)
    for session in sessions:
        session.close()


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


@pytest.mark.parametrize(
    ("path", "call"),
    [(MESSAGE_PATH, _send), (TASK_PATH, _get), (CANCEL_PATH, _cancel)],
    ids=["send", "task-get", "task-cancel"],
)
def test_runtime_that_stays_waking_ends_the_call_within_its_budget_and_nothing_is_sent(
    platform_at, path, call
):
    # The platform asks for a retry every 0.5 s; checks run at about 0, 0.5 and 1.0 s, and
    # the third retry no longer fits in 1.25 s.
    routes = {("POST", RESOLVE_PATH): answer_json(200, _waking_access(500))}
    with serve(routes) as (url, calls):
        platform_at(url)
        started = time.monotonic()
        with pytest.raises(agent_models_mod.AgentRuntimeNotReadyError) as error:
            call(1.25)
        elapsed = time.monotonic() - started

    assert elapsed < 1.25
    assert error.value.state == "waking"
    assert "did not become ready" in str(error.value)
    assert "Nothing was sent" in str(error.value)
    assert [sent_path for _method, sent_path, _body in calls] == [RESOLVE_PATH] * 3
    assert path not in [sent_path for _method, sent_path, _body in calls]


def test_runtime_that_becomes_ready_within_the_budget_receives_the_message_once(platform_at):
    message = {
        "messageId": "msg-runtime-output",
        "role": "ROLE_AGENT",
        "contextId": SESSION_UID,
        "parts": [{"text": "Ready."}],
    }
    routes = {}
    with serve(routes) as (url, calls):
        routes[("POST", RESOLVE_PATH)] = answer_sequence(
            answer_json(200, _waking_access(500)), answer_json(200, _ready_access(url))
        )
        routes[("POST", MESSAGE_PATH)] = answer_json(
            200, {"message": message}, content_type="application/a2a+json"
        )
        platform_at(url)
        result = _send(5)

    assert result.message == message
    assert [sent_path for _method, sent_path, _body in calls] == [
        RESOLVE_PATH,
        RESOLVE_PATH,
        MESSAGE_PATH,
    ]


def test_wait_without_a_timeout_ends_at_the_default_a2a_budget(monkeypatch):
    # The default A2A budget is (5, 900): 905 s in total. The platform asks for the longest
    # retry the SDK honours, 30 s, so the wait ends after 30 sleeps, at 900 s.
    clock = {"now": 0.0}
    sleeps = []

    def sleep(seconds):
        sleeps.append(seconds)
        clock["now"] += seconds

    resolves = []

    def resolve(cls, agent_session, *, cache=True, timeout=None):
        resolves.append(timeout)
        return agent_models_mod.AgentSessionRuntimeAccess(**_waking_access(30_000))

    monkeypatch.setattr(transport.time, "monotonic", lambda: clock["now"])
    monkeypatch.setattr(agent_models_mod.time, "sleep", sleep)
    monkeypatch.setattr(
        agent_models_mod.AgentSession, "resolve_runtime_access", classmethod(resolve)
    )
    agent_models_mod.AgentSession.clear_cached_runtime_access(SESSION_UID)

    with pytest.raises(agent_models_mod.AgentRuntimeNotReadyError):
        agent_models_mod.AgentSession._resolve_runtime_access_for_message_send(SESSION_UID)

    assert sleeps == [30.0] * 30
    assert clock["now"] == 900.0
    assert len(resolves) == 31
    agent_models_mod.AgentSession.clear_cached_runtime_access(SESSION_UID)
