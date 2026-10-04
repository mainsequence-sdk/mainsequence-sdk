import json
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

import mainsequence.client.agent_runtime_models as agent_models_mod
from tests.client.support import ENVIRONMENT_UID, _ready_runtime_contract


def test_agent_session_runtime_access_uses_session_uid_route(monkeypatch):
    captured = {}
    session_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"

    class FakeResponse:
        status_code = 200
        content = b'{"ok": true}'

        @staticmethod
        def json():
            return {
                "coding_agent_service_uid": "7bd86be3-d11a-4ad1-8fe2-9260ccdbca7f",
                "mode": "token",
                "rpc_url": "https://7bd86be3-d11a-4ad1-8fe2-9260ccdbca7f.coding-agent.main-sequence.app/",
                "token": "tok-secret",
                "is_ready": True,
                **_ready_runtime_contract(),
                "service_runtime_uid": "70c6efb9-8e80-4051-ad3a-f432b2c37f5a",
                "image_drift": {
                    "agent_kind": "astro_orchestrator",
                    "available": True,
                    "has_drift": False,
                    "requires_user_action": False,
                    "autoheal_available": False,
                    "autoheal_message": "No automatic drift repair is needed.",
                    "checks": [
                        {
                            "key": "orchestrator_image",
                            "label": "Orchestrator image",
                            "status": "match",
                            "has_drift": False,
                            "matches": True,
                            "reason": "match",
                            "message": "The runtime image matches the catalog image.",
                            "autoheal_supported": True,
                            "autoheal_mode": "request_driven_runtime_sync",
                            "autoheal_message": "No runtime repair is needed.",
                            "expected_image_uri": "registry.example/astro@sha256:expected",
                            "actual_image_uri": "registry.example/astro@sha256:expected",
                            "expected_commit_hash": "",
                            "actual_commit_hash": "",
                        }
                    ],
                    "detail": None,
                    "catalog_state": {
                        "image_prefix": "astro",
                        "tag": "latest",
                        "ttl_seconds": 3600,
                        "last_synced_at": "2026-07-17T10:00:00+00:00",
                        "status": "fresh",
                        "fresh": True,
                        "refresh_required": False,
                        "detail": "",
                        "catalog_image_registry_id": 11,
                        "catalog_image_registry_uid": "registry-uid",
                        "catalog_image_id": 42,
                        "latest_pinned_uri": "registry.example/astro@sha256:expected",
                        "age_seconds": 12.5,
                    },
                },
            }

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured["r_type"] = r_type
        captured["url"] = url
        captured["payload"] = payload
        captured["timeout"] = time_out
        return FakeResponse()

    monkeypatch.setattr(agent_models_mod, "make_request", _fake_make_request)

    access = agent_models_mod.AgentSession.resolve_runtime_access(session_uid, timeout=11)

    assert access.coding_agent_service_uid == "7bd86be3-d11a-4ad1-8fe2-9260ccdbca7f"
    assert "coding_agent_service_id" not in agent_models_mod.AgentSessionRuntimeAccess.model_fields
    assert "coding_agent_id" not in agent_models_mod.AgentSessionRuntimeAccess.model_fields
    assert access.is_ready is True
    assert access.runtime_interaction.can_submit is True
    assert "action" not in access.runtime_interaction.model_dump()
    assert access.service_runtime_uid == "70c6efb9-8e80-4051-ad3a-f432b2c37f5a"
    assert access.knative_service_runtime_uid == "70c6efb9-8e80-4051-ad3a-f432b2c37f5a"
    assert access.image_drift is not None
    assert access.image_drift.agent_kind == "astro_orchestrator"
    assert access.image_drift.requires_user_action is False
    assert access.image_drift.checks[0].key == "orchestrator_image"
    assert access.image_drift.catalog_state is not None
    assert access.image_drift.catalog_state["refresh_required"] is False
    assert captured == {
        "r_type": "POST",
        "url": f"{agent_models_mod.AgentSession.get_object_url()}/{session_uid}/resolve-runtime-access/",
        "payload": {"json": {}},
        "timeout": 11,
    }


def test_agent_session_runtime_access_accepts_minimal_image_drift(monkeypatch):
    session_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"

    class FakeResponse:
        status_code = 200
        content = b'{"ok": true}'

        @staticmethod
        def json():
            return {
                "coding_agent_service_uid": "7bd86be3-d11a-4ad1-8fe2-9260ccdbca7f",
                "mode": "token",
                "rpc_url": "https://7bd86be3-d11a-4ad1-8fe2-9260ccdbca7f.coding-agent.main-sequence.app/",
                "token": "tok-secret",
                "is_ready": True,
                **_ready_runtime_contract(),
                "knative_service_runtime_uid": "70c6efb9-8e80-4051-ad3a-f432b2c37f5a",
                "image_drift": {
                    "has_drift": False,
                    "detail": None,
                },
                "reconciliation": {
                    "queued": False,
                    "reason": "not_required",
                },
            }

    monkeypatch.setattr(agent_models_mod, "make_request", lambda **kwargs: FakeResponse())

    access = agent_models_mod.AgentSession.resolve_runtime_access(session_uid, timeout=11)

    assert access.image_drift is not None
    assert access.image_drift.has_drift is False
    assert access.image_drift.requires_user_action is False
    assert access.image_drift.detail is None
    assert access.model_dump()["reconciliation"] == {
        "queued": False,
        "reason": "not_required",
    }


@pytest.mark.parametrize("requires_user_action", [False, True])
def test_agent_runtime_image_drift_parses_user_action_signal(requires_user_action):
    image_drift = agent_models_mod.AgentRuntimeImageDrift.model_validate(
        {"requires_user_action": requires_user_action}
    )

    assert image_drift.requires_user_action is requires_user_action

    with pytest.raises(ValidationError, match="requires_user_action"):
        agent_models_mod.AgentRuntimeImageDrift.model_validate({"requires_user_action": None})


def test_agent_session_send_a2a_message_posts_standard_contract(monkeypatch):
    captured = {"resolve_count": 0, "runtime": {}}
    session_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
    session = agent_models_mod.AgentSession.model_validate(
        {
            "uid": session_uid,
            "organization_environment_uid": ENVIRONMENT_UID,
            "organization_environment_name": "production",
            "catalog_digest": "sha256:" + ("e" * 64),
            "llm_provider": "openai",
            "llm_model": "gpt-5.4",
        }
    )

    agent_models_mod.AgentSession.clear_cached_runtime_access(session_uid)

    class FakeResolveResponse:
        status_code = 200
        content = b'{"ok": true}'

        @staticmethod
        def json():
            return {
                "coding_agent_service_uid": "7bd86be3-d11a-4ad1-8fe2-9260ccdbca7f",
                "mode": "token",
                "rpc_url": "https://7bd86be3-d11a-4ad1-8fe2-9260ccdbca7f.coding-agent.main-sequence.app/",
                "token": "tok-secret",
                "expires_at": "2999-01-01T00:00:00Z",
                **_ready_runtime_contract(),
                "image_drift": {
                    "agent_kind": "astro_orchestrator",
                    "available": True,
                    "has_drift": False,
                    "requires_user_action": False,
                    "checks": [],
                },
            }

    class FakeRuntimeResponse:
        status_code = 200
        headers = {"Content-Type": "application/a2a+json"}
        text = ""

        @staticmethod
        def json():
            return {
                "message": {
                    "messageId": "msg-runtime-output",
                    "role": "ROLE_RESPONDER",
                    "contextId": session_uid,
                    "parts": [{"text": "I can analyze workspaces."}],
                }
            }

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured["resolve_count"] += 1
        captured["resolve"] = {
            "r_type": r_type,
            "url": url,
            "payload": payload,
            "timeout": time_out,
        }
        return FakeResolveResponse()

    def _fake_post(url, *, headers, data, timeout):
        captured["runtime"] = {
            "url": url,
            "headers": headers,
            "data": data,
            "timeout": timeout,
        }
        return FakeRuntimeResponse()

    monkeypatch.setattr(agent_models_mod, "make_request", _fake_make_request)
    monkeypatch.setattr(agent_models_mod.requests, "post", _fake_post)
    monkeypatch.setattr(
        agent_models_mod.uuid,
        "uuid4",
        lambda: "00000000-0000-4000-8000-000000000001",
    )

    payload = agent_models_mod.AgentSession.send_a2a_message(
        session,
        message="What can this agent do?",
        timeout=15,
    )

    assert captured["resolve_count"] == 1
    cached_access = agent_models_mod.AgentSession.get_cached_runtime_access(session)
    assert cached_access is not None
    assert cached_access.image_drift is not None
    assert cached_access.image_drift.requires_user_action is False
    assert captured["resolve"] == {
        "r_type": "POST",
        "url": f"{agent_models_mod.AgentSession.get_object_url()}/{session_uid}/resolve-runtime-access/",
        "payload": {"json": {}},
        "timeout": 15,
    }
    assert payload.message is not None
    assert payload.message["parts"] == [{"text": "I can analyze workspaces."}]
    assert captured["runtime"]["url"] == (
        "https://7bd86be3-d11a-4ad1-8fe2-9260ccdbca7f.coding-agent.main-sequence.app/"
        "api/a2a/v1/message:send"
    )
    assert captured["runtime"]["headers"]["Content-Type"] == "application/a2a+json"
    assert captured["runtime"]["headers"]["Accept"] == "application/a2a+json"
    assert captured["runtime"]["headers"]["Authorization"] == "Bearer tok-secret"
    assert captured["runtime"]["headers"]["A2A-Extensions"] == (
        agent_models_mod.STANDARD_A2A_RESPONSE_KIND_EXTENSION_URI
    )
    request_body = json.loads(captured["runtime"]["data"])
    assert request_body == {
        "message": {
            "messageId": "msg-00000000-0000-4000-8000-000000000001",
            "role": "ROLE_REQUESTER",
            "contextId": session_uid,
            "parts": [{"text": "What can this agent do?"}],
        },
        "configuration": {
            "acceptedOutputModes": ["text/plain"],
            "responseKind": "message",
        },
    }
    assert "omit_reasoning" not in captured["runtime"]["data"]


@pytest.mark.parametrize("invalid_role", [None, "ROLE_REQUESTER", "requester"])
def test_a2a_message_result_requires_responder_role(invalid_role):
    with pytest.raises(ValidationError, match="ROLE_RESPONDER"):
        agent_models_mod.A2AMessageSendResult.model_validate(
            {
                "message": {
                    "messageId": "msg-output",
                    "role": invalid_role,
                    "contextId": "session-uid",
                    "parts": [{"text": "Result."}],
                }
            }
        )


def test_agent_session_send_rejects_mismatched_response_context(monkeypatch):
    session_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return {
                "message": {
                    "messageId": "msg-output",
                    "role": "ROLE_RESPONDER",
                    "contextId": "different-session",
                    "parts": [{"text": "Result."}],
                }
            }

    monkeypatch.setattr(
        agent_models_mod.AgentSession,
        "_resolve_runtime_access_for_message_send",
        classmethod(lambda cls, agent_session, timeout=None: object()),
    )
    monkeypatch.setattr(
        agent_models_mod.AgentSession,
        "_post_standard_a2a_message",
        classmethod(lambda cls, access, *, body, timeout=None: FakeResponse()),
    )

    with pytest.raises(agent_models_mod.ApiError, match="contextId"):
        agent_models_mod.AgentSession.send_a2a_message(
            session_uid,
            message="Return a result.",
        )


def test_agent_session_send_waits_only_while_runtime_interaction_is_transient(
    monkeypatch,
):
    session_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
    agent_models_mod.AgentSession.clear_cached_runtime_access(session_uid)
    responses = iter(
        [
            {
                "mode": "unavailable",
                "rpc_url": None,
                "token": None,
                "runtime_interaction": {
                    "state": "waking",
                    "can_submit": False,
                    "notice": None,
                    "operation": None,
                    "retry_after_ms": 1500,
                },
                "runtime_presence": {
                    "phase": "pulling_image",
                    "replicas": {"desired": 1, "actual": 0},
                    "detail": "Fetching the runtime image.",
                    "observed_at": "2026-09-03T10:00:00Z",
                    "wake": None,
                },
            },
            {
                "mode": "token",
                "rpc_url": "https://runtime.example.test/",
                "token": "tok-secret",
                **_ready_runtime_contract(),
            },
        ]
    )
    resolve_calls = []
    sleeps = []

    class FakeResolveResponse:
        status_code = 200

        @staticmethod
        def json():
            resolve_calls.append("resolve")
            return next(responses)

    class FakeRuntimeResponse:
        status_code = 200

        @staticmethod
        def json():
            return {
                "message": {
                    "messageId": "msg-output",
                    "role": "ROLE_RESPONDER",
                    "contextId": session_uid,
                    "parts": [{"text": "Ready."}],
                }
            }

    monkeypatch.setattr(
        agent_models_mod,
        "make_request",
        lambda **_kwargs: FakeResolveResponse(),
    )
    monkeypatch.setattr(agent_models_mod.time, "sleep", sleeps.append)
    monkeypatch.setattr(
        agent_models_mod.requests,
        "post",
        lambda *_args, **_kwargs: FakeRuntimeResponse(),
    )

    result = agent_models_mod.AgentSession.send_a2a_message(
        session_uid,
        message="Wait for the active wake.",
    )

    assert result.message is not None
    assert resolve_calls == ["resolve", "resolve"]
    assert sleeps == [1.5]


def test_agent_session_send_a2a_task_returns_typed_task(monkeypatch):
    captured = {}
    session_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
    agent_models_mod.AgentSession.clear_cached_runtime_access(session_uid)
    agent_models_mod.AgentSession.cache_runtime_access(
        session_uid,
        {
            "mode": "token",
            "rpc_url": "https://runtime.example.test/",
            "token": "tok-secret",
            **_ready_runtime_contract(),
        },
    )

    class FakeRuntimeResponse:
        status_code = 200

        @staticmethod
        def json():
            return {
                "task": {
                    "kind": "task",
                    "id": "task-1",
                    "contextId": session_uid,
                    "status": {"state": "TASK_STATE_SUBMITTED"},
                    "artifacts": [],
                }
            }

    def _fake_post(url, *, headers, data, timeout):
        captured.update(url=url, headers=headers, data=data, timeout=timeout)
        return FakeRuntimeResponse()

    monkeypatch.setattr(agent_models_mod.requests, "post", _fake_post)

    result = agent_models_mod.AgentSession.send_a2a_message(
        session_uid,
        message="Run this asynchronously.",
        response_kind=agent_models_mod.A2AResponseKind.TASK,
        a2a_profile={
            "supported_response_kinds": ["message", "task"],
            "default_response_kind": "message",
        },
    )

    assert result.response_kind is agent_models_mod.A2AResponseKind.TASK
    assert result.task is not None
    assert result.task.id == "task-1"
    assert result.task.context_id == session_uid
    assert json.loads(captured["data"])["configuration"]["responseKind"] == "task"


def test_agent_a2a_profile_requires_message_default_and_support():
    with pytest.raises(ValueError, match="must include 'message'"):
        agent_models_mod.AgentA2AProfile(
            supported_response_kinds=[agent_models_mod.A2AResponseKind.TASK],
            default_response_kind=agent_models_mod.A2AResponseKind.MESSAGE,
        )

    with pytest.raises(ValueError, match="must be 'message'"):
        agent_models_mod.AgentA2AProfile(
            supported_response_kinds=[
                agent_models_mod.A2AResponseKind.MESSAGE,
                agent_models_mod.A2AResponseKind.TASK,
            ],
            default_response_kind=agent_models_mod.A2AResponseKind.TASK,
        )

    with pytest.raises(ValueError, match="extension_uri is not supported"):
        agent_models_mod.AgentA2AProfile(
            response_kind_extension_uri="https://example.test/a2a/response-kind",
        )

    with pytest.raises(ValidationError, match="Input should be 'task'"):
        agent_models_mod.A2ATask.model_validate(
            {
                "kind": "message",
                "id": "task-1",
                "contextId": "session-1",
                "status": {"state": "TASK_STATE_SUBMITTED"},
            }
        )


def test_agent_session_task_send_discovers_message_only_profile_before_runtime(monkeypatch):
    session_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
    agent_uid = "e0e75693-4110-464c-a253-e302754872c1"
    discovered = {}

    def _get_session(cls, pk=None, timeout=None, **_filters):
        discovered["session"] = (pk, timeout)
        return SimpleNamespace(agent_uid=agent_uid)

    def _get_agent(cls, pk=None, timeout=None, **_filters):
        discovered["agent"] = (pk, timeout)
        return SimpleNamespace(a2a_profile=agent_models_mod.AgentA2AProfile())

    monkeypatch.setattr(agent_models_mod.AgentSession, "get", classmethod(_get_session))
    monkeypatch.setattr(agent_models_mod.Agent, "get", classmethod(_get_agent))

    with pytest.raises(ValueError, match="is not advertised"):
        agent_models_mod.AgentSession.send_a2a_message(
            session_uid,
            message="Run this asynchronously.",
            response_kind=agent_models_mod.A2AResponseKind.TASK,
            timeout=17,
        )

    assert discovered == {
        "session": (session_uid, 17),
        "agent": (agent_uid, 17),
    }


def test_agent_session_a2a_task_helpers_get_wait_and_cancel(monkeypatch):
    session_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
    agent_models_mod.AgentSession.clear_cached_runtime_access(session_uid)
    agent_models_mod.AgentSession.cache_runtime_access(
        session_uid,
        {
            "mode": "token",
            "rpc_url": "https://runtime.example.test/",
            "token": "tok-secret",
            **_ready_runtime_contract(),
        },
    )
    states = iter(["TASK_STATE_WORKING", "TASK_STATE_COMPLETED", "TASK_STATE_CANCELED"])
    calls = []

    class FakeTaskResponse:
        status_code = 200

        def __init__(self, state):
            self.state = state

        def json(self):
            return {
                "task": {
                    "kind": "task",
                    "id": "task-1",
                    "contextId": session_uid,
                    "status": {"state": self.state},
                    "artifacts": [],
                }
            }

    def _fake_request(method, url, *, headers, timeout):
        calls.append((method, url, headers, timeout))
        return FakeTaskResponse(next(states))

    monkeypatch.setattr(agent_models_mod.requests, "request", _fake_request)
    monkeypatch.setattr(agent_models_mod.time, "sleep", lambda _seconds: None)

    completed = agent_models_mod.AgentSession.wait_for_a2a_task(
        session_uid,
        task_id="task-1",
        poll_interval_seconds=0.01,
    )
    canceled = agent_models_mod.AgentSession.cancel_a2a_task(
        session_uid,
        task_id="task-1",
    )

    assert completed.status.state == "TASK_STATE_COMPLETED"
    assert canceled.status.state == "TASK_STATE_CANCELED"
    assert [method for method, *_rest in calls] == ["GET", "GET", "POST"]
    assert calls[-1][1].endswith("/api/a2a/v1/tasks/task-1:cancel")


def test_agent_session_send_a2a_message_reports_unavailable_runtime_without_post(
    monkeypatch,
):
    session_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
    agent_models_mod.AgentSession.clear_cached_runtime_access(session_uid)

    class FakeResolveResponse:
        status_code = 200
        content = b'{"ok": true}'

        @staticmethod
        def json():
            return {
                "coding_agent_service_uid": None,
                "mode": "unavailable",
                "rpc_url": None,
                "token": None,
                "is_ready": False,
                "detail": "Runtime reconciliation is queued.",
                "runtime_interaction": {
                    "state": "unavailable",
                    "can_submit": False,
                    "notice": None,
                    "operation": None,
                    "retry_after_ms": None,
                },
                "runtime_presence": {
                    "phase": "not_deployed",
                    "replicas": {"desired": None, "actual": None},
                    "detail": "The runtime has not been deployed.",
                    "observed_at": None,
                    "wake": None,
                },
            }

    monkeypatch.setattr(
        agent_models_mod,
        "make_request",
        lambda **kwargs: FakeResolveResponse(),
    )

    def _unexpected_post(*args, **kwargs):
        pytest.fail("A2A HTTP request must not run for unavailable runtime access")

    monkeypatch.setattr(agent_models_mod.requests, "post", _unexpected_post)

    with pytest.raises(
        agent_models_mod.ApiError,
        match=("Coding-agent runtime access is unavailable. Runtime reconciliation is queued."),
    ):
        agent_models_mod.AgentSession.send_a2a_message(
            session_uid,
            message="Do not send this yet.",
        )


def test_agent_session_send_a2a_message_posts_strict_dictionary_contract(monkeypatch):
    captured = {"resolve_count": 0}
    session_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"

    agent_models_mod.AgentSession.clear_cached_runtime_access(session_uid)

    class FakeResolveResponse:
        status_code = 200
        content = b'{"ok": true}'

        @staticmethod
        def json():
            return {
                "coding_agent_service_uid": "7bd86be3-d11a-4ad1-8fe2-9260ccdbca7f",
                "mode": "token",
                "rpc_url": "https://7bd86be3-d11a-4ad1-8fe2-9260ccdbca7f.coding-agent.main-sequence.app/",
                "token": "tok-secret",
                **_ready_runtime_contract(),
            }

    class FakeRuntimeResponse:
        status_code = 200
        headers = {"Content-Type": "application/a2a+json"}

        @staticmethod
        def json():
            return {
                "message": {
                    "messageId": "msg-runtime-output",
                    "role": "ROLE_RESPONDER",
                    "contextId": session_uid,
                    "parts": [{"text": '{"ok": true}'}],
                }
            }

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured["resolve_count"] += 1
        return FakeResolveResponse()

    def _fake_post(url, *, headers, data, timeout):
        captured["data"] = data
        return FakeRuntimeResponse()

    monkeypatch.setattr(agent_models_mod, "make_request", _fake_make_request)
    monkeypatch.setattr(agent_models_mod.requests, "post", _fake_post)
    monkeypatch.setattr(
        agent_models_mod.uuid,
        "uuid4",
        lambda: "00000000-0000-4000-8000-000000000002",
    )

    agent_models_mod.AgentSession.send_a2a_message(
        session_uid,
        message="Return a JSON dictionary with keys ok, answer, and example.",
        strict_dictionary=True,
        json_repair_attempts=3,
    )

    assert captured["resolve_count"] == 1
    request_body = json.loads(captured["data"])
    assert request_body["configuration"]["acceptedOutputModes"] == ["application/json"]
    assert request_body["metadata"] == {
        "https://mainsequence.ai/a2a/extensions/output-contract/v1": {
            "response_format": {
                "type": "dictionary",
                "strict": True,
            },
            "jsonRepairAttempts": 3,
        }
    }
    assert "omit_reasoning" not in captured["data"]


def test_agent_session_send_a2a_message_refreshes_access_and_reuses_body(monkeypatch):
    captured = {"resolve_count": 0, "runtime_bodies": []}
    session_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
    agent_models_mod.AgentSession.clear_cached_runtime_access(session_uid)

    class FakeResolveResponse:
        status_code = 200
        content = b'{"ok": true}'

        @staticmethod
        def json():
            captured["resolve_count"] += 1
            return {
                "coding_agent_service_uid": "7bd86be3-d11a-4ad1-8fe2-9260ccdbca7f",
                "mode": "token",
                "rpc_url": "https://7bd86be3-d11a-4ad1-8fe2-9260ccdbca7f.coding-agent.main-sequence.app/",
                "token": f"tok-secret-{captured['resolve_count']}",
                **_ready_runtime_contract(),
            }

    class FakeUnauthorizedResponse:
        status_code = 401
        headers = {"Content-Type": "application/a2a+json"}
        text = '{"error":"unauthorized"}'

        @staticmethod
        def json():
            return {"error": "unauthorized"}

    class FakeRuntimeResponse:
        status_code = 200
        headers = {"Content-Type": "application/a2a+json"}
        text = ""

        @staticmethod
        def json():
            return {
                "message": {
                    "messageId": "msg-runtime-output",
                    "role": "ROLE_RESPONDER",
                    "contextId": session_uid,
                    "parts": [{"text": "Done."}],
                }
            }

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        return FakeResolveResponse()

    def _fake_post(url, *, headers, data, timeout):
        captured["runtime_bodies"].append(data)
        captured.setdefault("tokens", []).append(headers["Authorization"])
        if len(captured["runtime_bodies"]) == 1:
            return FakeUnauthorizedResponse()
        return FakeRuntimeResponse()

    monkeypatch.setattr(agent_models_mod, "make_request", _fake_make_request)
    monkeypatch.setattr(agent_models_mod.requests, "post", _fake_post)

    payload = agent_models_mod.AgentSession.send_a2a_message(
        session_uid,
        message="Retry safely.",
        message_id="msg-client-retry-1",
    )

    assert payload.message is not None
    assert payload.message["parts"] == [{"text": "Done."}]
    assert captured["resolve_count"] == 2
    assert captured["runtime_bodies"][0] == captured["runtime_bodies"][1]
    request_body = json.loads(captured["runtime_bodies"][0])
    assert request_body["message"]["messageId"] == "msg-client-retry-1"
    assert captured["tokens"] == ["Bearer tok-secret-1", "Bearer tok-secret-2"]
