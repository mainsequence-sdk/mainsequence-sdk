import pytest
from pydantic import ValidationError

import mainsequence.client.agent_runtime_models as agent_models_mod
import mainsequence.client.base as base_mod
from tests.client.support import ENVIRONMENT_UID, _agent_runtime_update_contract


def test_agent_session_filter_supports_archive_history_query(monkeypatch):
    captured = {}
    user_uid = "3a715c8d-da66-452a-b6cb-ffbb696ef121"

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return {"count": 0, "next": None, "previous": None, "results": []}

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured.update(
            {
                "r_type": r_type,
                "url": url,
                "payload": payload,
                "timeout": time_out,
            }
        )
        return FakeResponse()

    monkeypatch.setattr(base_mod, "make_request", _fake_make_request)

    sessions = agent_models_mod.AgentSession.filter(
        created_by_user_uid=user_uid,
        is_archived=False,
        ordering="-started_at",
        limit=20,
        offset=0,
        timeout=7,
    )

    assert sessions == []
    assert captured == {
        "r_type": "GET",
        "url": f"{agent_models_mod.AgentSession.get_object_url()}/",
        "payload": {
            "params": {
                "created_by_user_uid": user_uid,
                "is_archived": False,
                "ordering": "-started_at",
                "limit": "20",
                "offset": "0",
            }
        },
        "timeout": 7,
    }


def test_agent_session_list_and_detail_parse_runtime_capabilities(monkeypatch):
    session_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
    agent_uid = "e0e75693-4110-464c-93e0-82c7fd9c9a23"
    session_payload = {
        "uid": session_uid,
        "agent_uid": agent_uid,
        "agent_name": "Research Copilot",
        "organization_environment_uid": ENVIRONMENT_UID,
        "organization_environment_name": "production",
        "harness": "tau",
        "harness_protocol": "tau-session-v1",
        "harness_version": "0.3.1",
        "name": "Reusable session",
        "status": "running",
        "llm_provider": "openai",
        "llm_model": "gpt-5.4",
        "llm_thinking": "medium",
        "observability": {
            "application_logs_url": f"/api/v1/agent-sessions/{session_uid}/logs/",
            "resource_usage_url": None,
            "deployment_runs_url": None,
            "sessions_url": None,
        },
        "runtime_capabilities": {
            "tau_runtime_bootstrap": "v1",
            "tau_resume_snapshot": "v1",
            "tau_activity_sequence": "v1",
            "tau_turn_commit": "v1",
        },
        "catalog_digest": "sha256:" + ("b" * 64),
    }
    responses = iter(
        [
            {"count": 1, "next": None, "previous": None, "results": [dict(session_payload)]},
            dict(session_payload),
        ]
    )

    class FakeResponse:
        status_code = 200
        content = b'{"ok": true}'

        def __init__(self, payload):
            self._payload = payload

        def json(self):
            return self._payload

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        return FakeResponse(next(responses))

    monkeypatch.setattr(base_mod, "make_request", _fake_make_request)

    listed = agent_models_mod.AgentSession.filter(agent_uid=agent_uid)
    detailed = agent_models_mod.AgentSession.get(pk=session_uid)

    assert listed[0].runtime_capabilities["tau_turn_commit"] == "v1"
    assert detailed.runtime_capabilities == listed[0].runtime_capabilities
    assert detailed.observability.application_logs_url.endswith(f"/{session_uid}/logs/")
    assert listed[0].organization_environment_uid == ENVIRONMENT_UID
    assert detailed.organization_environment_name == "production"
    assert detailed.catalog_digest == "sha256:" + ("b" * 64)

    # Tolerant reading (ADR 0032): an undeclared projection is dropped.
    assert not hasattr(
        agent_models_mod.AgentSession.model_validate(
            {**session_payload, "unexpected_projection": "ignored"}
        ),
        "unexpected_projection",
    )


def test_agent_session_environment_and_catalog_projection_contract_is_strict():
    payload = {
        "organization_environment_uid": ENVIRONMENT_UID,
        "organization_environment_name": "production",
        "catalog_digest": None,
        "llm_provider": "openai",
        "llm_model": "gpt-5.4",
    }

    session = agent_models_mod.AgentSession.model_validate(payload)
    assert session.catalog_digest is None

    for field_name in (
        "organization_environment_uid",
        "organization_environment_name",
        "catalog_digest",
    ):
        incomplete = dict(payload)
        incomplete.pop(field_name)
        with pytest.raises(ValidationError, match=field_name):
            agent_models_mod.AgentSession.model_validate(incomplete)

    for field_name in (
        "organization_environment_uid",
        "organization_environment_name",
    ):
        with pytest.raises(ValidationError, match=field_name):
            agent_models_mod.AgentSession.model_validate(payload | {field_name: None})

    with pytest.raises(ValidationError, match="catalog_digest"):
        agent_models_mod.AgentSession.model_validate(
            payload | {"catalog_digest": "not-a-catalog-digest"}
        )


def test_agent_session_get_insights_parses_pi_response(monkeypatch):
    session_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
    captured = {}

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return {
                "has_insights": False,
                "agent_session_uid": session_uid,
                "harness": "pi",
                "harness_protocol": "pi-checkpoint-v1",
                "harness_version": "",
                "checkpoint_version": None,
                "bundle_hash": "",
                "computed_at": None,
                "flushed_at": None,
                "reason": None,
                "insights": {},
                "updated_at": None,
            }

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured.update(
            {
                "r_type": r_type,
                "url": url,
                "payload": payload,
                "timeout": time_out,
            }
        )
        return FakeResponse()

    monkeypatch.setattr(agent_models_mod, "make_request", _fake_make_request)

    insights = agent_models_mod.AgentSession.get_insights(session_uid, timeout=9)

    assert isinstance(insights, agent_models_mod.PiAgentSessionInsights)
    assert insights.has_insights is False
    assert insights.checkpoint_version is None
    assert captured == {
        "r_type": "GET",
        "url": f"{agent_models_mod.AgentSession.get_object_url()}/{session_uid}/insights/",
        "payload": {},
        "timeout": 9,
    }


def test_agent_session_get_insights_parses_tau_response(monkeypatch):
    session_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return {
                "has_insights": True,
                "agent_session_uid": session_uid,
                "harness": "tau",
                "harness_protocol": "tau-session-v1",
                "harness_version": "0.3.1",
                "entry_count": 4,
                "last_sequence": 4,
                "active_branch_entry_count": 3,
                "entry_type_counts": {"message": 3, "session_info": 1},
                "title": "Competition analysis",
                "computed_at": "2026-07-26T19:25:00Z",
                "reason": "tau_entries_projection",
                "updated_at": "2026-07-26T19:24:00Z",
                "insights": {
                    "version": 1,
                    "model": {
                        "provider": "openai",
                        "model": "gpt-5.4",
                        "reasoningEffort": "high",
                    },
                    "session": {
                        "agentSessionId": session_uid,
                        "sessionId": "thread-123",
                        "threadId": "thread-123",
                        "status": "running",
                        "startedAt": "2026-07-26T19:00:00Z",
                        "updatedAt": "2026-07-26T19:24:00Z",
                        "lastError": None,
                    },
                    "usage": {
                        "totalMessages": 3,
                        "userMessages": 1,
                        "assistantMessages": 2,
                        "assistantTurns": 2,
                        "toolCalls": 1,
                        "toolResults": 0,
                        "estimatedCostUsd": 0.42,
                        "tokens": {
                            "input": 100,
                            "output": 50,
                            "cacheRead": 10,
                            "cacheWrite": 5,
                            "total": 165,
                        },
                    },
                    "context": {
                        "source": "tau_entries",
                        "status": "reported_by_last_assistant",
                        "tokens": 100,
                        "latestCompaction": None,
                    },
                    "lastTurn": {
                        "completedAt": "2026-07-26T19:24:00Z",
                        "finishReason": "stop",
                        "errorMessage": None,
                        "model": {
                            "provider": "openai",
                            "model": "gpt-5.4",
                        },
                        "tokens": {
                            "input": 60,
                            "output": 20,
                            "cacheRead": 10,
                            "cacheWrite": 5,
                            "total": 95,
                        },
                    },
                },
            }

    monkeypatch.setattr(agent_models_mod, "make_request", lambda **kwargs: FakeResponse())

    insights = agent_models_mod.AgentSession.get_insights(session_uid)

    assert isinstance(insights, agent_models_mod.TauAgentSessionInsights)
    assert insights.entry_count == 4
    assert insights.entry_type_counts == {"message": 3, "session_info": 1}
    assert insights.insights.model.reasoning_effort == "high"
    assert insights.insights.session.agent_session_id == session_uid
    assert insights.insights.usage.tokens.cache_read == 10
    assert insights.insights.last_turn is not None
    assert insights.insights.last_turn.finish_reason == "stop"


def test_agent_session_archive_actions_return_current_session_contract(monkeypatch):
    session_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
    requests = []

    class FakeResponse:
        status_code = 200

        def __init__(self, archived):
            self.archived = archived

        def json(self):
            return {
                "uid": session_uid,
                "agent_uid": "9d81d63f-b8c9-404d-9f1a-5f2ad29dbf16",
                "agent_name": "Research Copilot",
                "organization_environment_uid": ENVIRONMENT_UID,
                "organization_environment_name": "production",
                "harness": "tau",
                "harness_protocol": "tau-session-v1",
                "harness_version": "0.3.1",
                "created_by_user_uid": "3a715c8d-da66-452a-b6cb-ffbb696ef121",
                "parent_session_uid": None,
                "name": "Research follow-up",
                "status": "running",
                "started_at": "2026-07-26T19:00:00Z",
                "ended_at": None,
                "is_archived": self.archived,
                "archived_at": "2026-07-26T20:00:00Z" if self.archived else None,
                "llm_provider": "openai",
                "llm_model": "gpt-5.4",
                "llm_thinking": "high",
                "active_provider": "openai",
                "active_model": "gpt-5.4",
                "active_thinking": "high",
                "engine_name": "tau",
                "runtime_config_snapshot": {},
                "error_detail": "",
                "thread_id": "thread-123",
                "session_metadata": {},
                "bound_handle": None,
                "catalog_digest": None,
            }

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        requests.append((r_type, url, payload, time_out))
        return FakeResponse(url.endswith("/archive/"))

    monkeypatch.setattr(agent_models_mod, "make_request", _fake_make_request)

    archived = agent_models_mod.AgentSession.archive_by_uid(session_uid, timeout=5)
    unarchived = archived.unarchive(timeout=6)

    assert archived.is_archived is True
    assert archived.archived_at is not None
    assert unarchived.is_archived is False
    assert unarchived.archived_at is None
    assert requests == [
        (
            "POST",
            f"{agent_models_mod.AgentSession.get_object_url()}/{session_uid}/archive/",
            {},
            5,
        ),
        (
            "POST",
            f"{agent_models_mod.AgentSession.get_object_url()}/{session_uid}/unarchive/",
            {},
            6,
        ),
    ]


@pytest.mark.parametrize("subscription_uid", [None, "11111111-1111-4111-8111-111111111111"])
def test_agent_get_or_create_session_posts_new_contract(monkeypatch, subscription_uid):
    captured = {}
    agent_uid = "e0e75693-4110-464c-93e0-82c7fd9c9a23"
    session_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
    parent_session_uid = "33333333-3333-4333-8333-333333333333"
    user_uid = "fdf409f7-d16f-4f71-986b-9057db6c7eca"
    agent = agent_models_mod.Agent(
        uid=agent_uid,
        name="Research Copilot",
        description="Research assistant.",
        agent_card={"name": "Research Copilot", "description": "Research assistant."},
        llm_provider="openai",
        llm_model="gpt-5.4",
        llm_thinking="medium",
        repository_branch=None,
        organization_environment_uid=None,
        organization_environment_name=None,
        runtime_update=_agent_runtime_update_contract(),
    )

    class FakeResponse:
        status_code = 201
        content = b'{"ok": true}'

        @staticmethod
        def json():
            return {
                "uid": session_uid,
                "custom_id": subscription_uid,
                "agent_uid": agent_uid,
                "agent_name": "Research Copilot",
                "organization_environment_uid": ENVIRONMENT_UID,
                "organization_environment_name": "production",
                "created_by_user_uid": user_uid,
                "parent_session_uid": parent_session_uid,
                "name": "Quarterly portfolio review",
                "status": "running",
                "runtime_state": "running",
                "working": True,
                "started_at": "2026-01-01T00:00:00Z",
                "ended_at": None,
                "llm_provider": "openai",
                "llm_model": "gpt-5.4",
                "llm_thinking": "",
                "engine_name": "codex",
                "runtime_config_snapshot": {},
                "error_detail": "",
                "thread_id": "",
                "session_metadata": {},
                "bound_handle": {
                    "uid": "44444444-4444-4444-8444-444444444444",
                    "handle_unique_id": "portfolio-review-q2-2026",
                    "owner_user_uid": user_uid,
                    "is_locked": False,
                },
                "catalog_digest": "sha256:" + ("c" * 64),
            }

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured["r_type"] = r_type
        captured["url"] = url
        captured["payload"] = payload
        captured["timeout"] = time_out
        return FakeResponse()

    monkeypatch.setattr(agent_models_mod, "make_request", _fake_make_request)

    session = agent.get_or_create_session(
        handle_unique_id="portfolio-review-q2-2026",
        name="Quarterly portfolio review",
        parent_session_uid=parent_session_uid,
        llm_provider="openai",
        llm_model="gpt-5.4",
        llm_thinking="",
        timeout=13,
        custom_id=subscription_uid,
    )

    assert session.custom_id == subscription_uid
    assert session.uid == session_uid
    assert session.name == "Quarterly portfolio review"
    assert session.parent_session_uid == parent_session_uid
    assert session.organization_environment_uid == ENVIRONMENT_UID
    assert session.catalog_digest == "sha256:" + ("c" * 64)
    expected = {
        "r_type": "POST",
        "url": (
            f"{agent_models_mod.Agent.get_object_url()}/{agent_uid}/sessions/get-or-create-session/"
        ),
        "payload": {
            "json": {
                "handle_unique_id": "portfolio-review-q2-2026",
                "name": "Quarterly portfolio review",
                "parent_session_uid": parent_session_uid,
                "llm_provider": "openai",
                "llm_model": "gpt-5.4",
                "llm_thinking": "",
            }
        },
        "timeout": 13,
    }
    if subscription_uid is not None:
        expected["payload"]["json"]["custom_id"] = subscription_uid
    assert captured == expected



def test_agent_get_or_create_session_parses_reused_handle_capabilities(monkeypatch):
    agent_uid = "e0e75693-4110-464c-93e0-82c7fd9c9a23"
    session_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
    handle_unique_id = "tutorial-evaluation-development"
    agent = agent_models_mod.Agent(
        uid=agent_uid,
        name="Research Copilot",
        description="Research assistant.",
        agent_card={"name": "Research Copilot", "description": "Research assistant."},
        llm_provider="openai",
        llm_model="gpt-5.4",
        llm_thinking="medium",
        repository_branch=None,
        organization_environment_uid=None,
        organization_environment_name=None,
        runtime_update=_agent_runtime_update_contract(),
    )
    captured = {}

    class FakeResponse:
        status_code = 200
        content = b'{"ok": true}'

        @staticmethod
        def json():
            return {
                "uid": session_uid,
                "agent_uid": agent_uid,
                "agent_name": "Research Copilot",
                "organization_environment_uid": ENVIRONMENT_UID,
                "organization_environment_name": "production",
                "harness": "tau",
                "harness_protocol": "tau-session-v1",
                "harness_version": "0.3.1",
                "name": "Tutorial evaluation",
                "status": "running",
                "llm_provider": "openai",
                "llm_model": "gpt-5.4",
                "llm_thinking": "medium",
                "bound_handle": {
                    "uid": "44444444-4444-4444-8444-444444444444",
                    "handle_unique_id": handle_unique_id,
                    "owner_user_uid": "fdf409f7-d16f-4f71-986b-9057db6c7eca",
                    "is_locked": False,
                },
                "observability": {
                    "application_logs_url": f"/api/v1/agent-sessions/{session_uid}/logs/",
                    "resource_usage_url": None,
                    "deployment_runs_url": None,
                    "sessions_url": None,
                },
                "runtime_capabilities": {
                    "tau_runtime_bootstrap": "v1",
                    "tau_resume_snapshot": "v1",
                    "tau_activity_sequence": "v1",
                    "tau_turn_commit": "v1",
                },
                "catalog_digest": "sha256:" + ("d" * 64),
            }

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured.update({"r_type": r_type, "url": url, "payload": payload, "timeout": time_out})
        return FakeResponse()

    monkeypatch.setattr(agent_models_mod, "make_request", _fake_make_request)

    session = agent.get_or_create_session(handle_unique_id=handle_unique_id, timeout=13)

    assert session.uid == session_uid
    assert session.bound_handle["handle_unique_id"] == handle_unique_id
    assert session.runtime_capabilities["tau_runtime_bootstrap"] == "v1"
    assert session.observability.application_logs_url.endswith(f"/{session_uid}/logs/")
    assert session.organization_environment_name == "production"
    assert session.catalog_digest == "sha256:" + ("d" * 64)
    assert captured["payload"] == {"json": {"handle_unique_id": handle_unique_id}}
    assert captured["timeout"] == 13


def test_agent_get_or_create_session_by_uid_sends_only_session_uid(monkeypatch):
    captured = {}
    agent_uid = "e0e75693-4110-464c-93e0-82c7fd9c9a23"
    session_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
    agent = agent_models_mod.Agent(
        uid=agent_uid,
        name="Research Copilot",
        description="Research assistant.",
        agent_card={"name": "Research Copilot", "description": "Research assistant."},
        llm_provider="openai",
        llm_model="gpt-5.4",
        llm_thinking="medium",
        repository_branch=None,
        organization_environment_uid=None,
        organization_environment_name=None,
        runtime_update=_agent_runtime_update_contract(),
    )

    class FakeResponse:
        status_code = 200
        content = b'{"ok": true}'

        @staticmethod
        def json():
            return {
                "uid": session_uid,
                "agent_uid": agent_uid,
                "agent_name": "Research Copilot",
                "organization_environment_uid": ENVIRONMENT_UID,
                "organization_environment_name": "production",
                "name": "Existing session",
                "status": "running",
                "llm_provider": "openai",
                "llm_model": "gpt-5.4",
                "llm_thinking": "",
                "bound_handle": None,
                "catalog_digest": None,
            }

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured["r_type"] = r_type
        captured["url"] = url
        captured["payload"] = payload
        captured["timeout"] = time_out
        return FakeResponse()

    monkeypatch.setattr(agent_models_mod, "make_request", _fake_make_request)

    session = agent.get_or_create_session(session_uid=session_uid, timeout=9)

    assert session.uid == session_uid
    assert captured == {
        "r_type": "POST",
        "url": (
            f"{agent_models_mod.Agent.get_object_url()}/{agent_uid}/sessions/get-or-create-session/"
        ),
        "payload": {"json": {"session_uid": session_uid}},
        "timeout": 9,
    }
