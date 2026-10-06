import uuid

import pytest
from pydantic import ValidationError

import mainsequence.client.agent_runtime_models as agent_models_mod
import mainsequence.client.base as base_mod
import mainsequence.code_repository_context as code_repository_context
from tests.client.support import ENVIRONMENT_UID, _agent_runtime_update_contract


@pytest.mark.parametrize(
    ("state", "needs_redeploy", "remediation_tool"),
    [
        ("current", False, None),
        ("update_required", True, "agent.update_runtime"),
        ("updating", True, None),
        ("update_failed", True, "agent.update_runtime"),
        ("not_deployed", False, None),
        ("unknown", None, None),
    ],
)
def test_agent_runtime_update_parses_every_backend_state(
    state,
    needs_redeploy,
    remediation_tool,
):
    runtime_update = agent_models_mod.AgentRuntimeUpdate.model_validate(
        _agent_runtime_update_contract(state)
    )

    assert runtime_update.state == state
    assert runtime_update.needs_redeploy is needs_redeploy
    assert (
        runtime_update.remediation.tool if runtime_update.remediation is not None else None
    ) == remediation_tool


def test_agent_runtime_update_contract_is_complete_and_read_tolerantly():
    payload = _agent_runtime_update_contract("unknown")
    for field_name in ("state", "needs_redeploy", "remediation"):
        incomplete = dict(payload)
        incomplete.pop(field_name)
        with pytest.raises(ValidationError, match=field_name):
            agent_models_mod.AgentRuntimeUpdate.model_validate(incomplete)

    # Tolerant reading (ADR 0032): a field this release does not declare is dropped.
    tolerated = agent_models_mod.AgentRuntimeUpdate.model_validate(
        payload | {"future_state": "value"}
    )
    assert tolerated.state == payload["state"]
    assert not hasattr(tolerated, "future_state")

    invalid_remediation = _agent_runtime_update_contract("update_required")
    invalid_remediation["remediation"] = {"tool": "agent.update_session"}
    with pytest.raises(ValidationError, match="agent.update_runtime"):
        agent_models_mod.AgentRuntimeUpdate.model_validate(invalid_remediation)


def test_agent_runtime_models_deserialize_backend_uid_payloads():
    agent_uid = "e0e75693-4110-464c-93e0-82c7fd9c9a23"
    session_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
    user_uid = "fdf409f7-d16f-4f71-986b-9057db6c7eca"
    code_repository_branch_uid = "9d81d63f-b8c9-404d-9f1a-5f2ad29dbf16"
    environment_uid = "22222222-2222-4222-8222-222222222222"

    agent = agent_models_mod.Agent.model_validate(
        {
            "uid": agent_uid,
            "name": "Research Copilot",
            "description": "Research assistant.",
            "agent_card": {"name": "Research Copilot", "description": "Research assistant."},
            "llm_provider": "openai",
            "llm_model": "gpt-5.4",
            "llm_thinking": "medium",
            "runtime_config": {"temperature": 0},
            "configuration": {"mode": "analysis"},
            "last_session_at": "2026-01-01T00:00:00Z",
            "code_repository_branch_uid": code_repository_branch_uid,
            "repository_branch": "main",
            "organization_environment_uid": environment_uid,
            "organization_environment_name": "production",
            "runtime_update": _agent_runtime_update_contract(),
        }
    )
    assert agent.uid == agent_uid
    assert agent.code_repository_branch_uid == code_repository_branch_uid
    assert agent.repository_branch == "main"
    assert agent.organization_environment_uid == environment_uid
    assert agent.organization_environment_name == "production"
    assert agent.runtime_update.state == "current"
    assert agent.runtime_update.needs_redeploy is False
    assert agent.a2a_profile.supported_response_kinds == [agent_models_mod.A2AResponseKind.MESSAGE]

    search_result = agent_models_mod.AgentSemanticSearchResult.model_validate(
        {
            "uid": agent_uid,
            "name": "Research Copilot",
            "description": "Research assistant.",
            "code_repository_branch_uid": code_repository_branch_uid,
            "repository_branch": "main",
            "organization_environment_uid": environment_uid,
            "organization_environment_name": "production",
            "runtime_update": _agent_runtime_update_contract("update_required"),
            "semantic_score": 0.91,
            "text_score": 0.74,
            "combined_score": 0.85,
        }
    )
    assert search_result.uid == agent_uid
    assert search_result.code_repository_branch_uid == code_repository_branch_uid
    assert search_result.repository_branch == "main"
    assert search_result.organization_environment_uid == environment_uid
    assert search_result.organization_environment_name == "production"
    assert search_result.runtime_update.state == "update_required"
    assert search_result.runtime_update.remediation.tool == "agent.update_runtime"
    assert search_result.a2a_profile.default_response_kind == (
        agent_models_mod.A2AResponseKind.MESSAGE
    )

    session = agent_models_mod.AgentSession.model_validate(
        {
            "uid": session_uid,
            "agent_uid": agent_uid,
            "agent_name": "Research Copilot",
            "organization_environment_uid": environment_uid,
            "organization_environment_name": "production",
            "harness": "tau",
            "harness_protocol": "tau-session-v1",
            "harness_version": "0.3.1",
            "created_by_user_uid": user_uid,
            "parent_session_uid": None,
            "name": "Research follow-up",
            "status": "running",
            "runtime_state": "running",
            "working": True,
            "started_at": "2026-01-01T00:00:00Z",
            "ended_at": None,
            "is_archived": False,
            "archived_at": None,
            "llm_provider": "openai",
            "llm_model": "gpt-5.4",
            "llm_thinking": "medium",
            "active_provider": "openai",
            "active_model": "gpt-5.4",
            "active_thinking": "medium",
            "engine_name": "codex",
            "runtime_config_snapshot": {"temperature": 0},
            "error_detail": "",
            "thread_id": "thread-123",
            "session_metadata": {"origin": "test"},
            "bound_handle": {
                "uid": "44444444-4444-4444-8444-444444444444",
                "handle_unique_id": "delegated-handle-1",
                "owner_user_uid": user_uid,
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
            "catalog_digest": "sha256:" + ("a" * 64),
        }
    )
    assert session.uid == session_uid
    assert session.agent_uid == agent_uid
    assert session.organization_environment_uid == environment_uid
    assert session.organization_environment_name == "production"
    assert session.catalog_digest == "sha256:" + ("a" * 64)
    assert session.name == "Research follow-up"
    assert session.harness is agent_models_mod.AgentHarnessKind.TAU
    assert session.harness_protocol is agent_models_mod.AgentHarnessProtocol.TAU_SESSION_V1
    assert session.harness_version == "0.3.1"
    assert session.is_archived is False
    assert session.active_model == "gpt-5.4"
    assert session.observability.application_logs_url.endswith(f"/{session_uid}/logs/")
    assert session.runtime_capabilities == {
        "tau_runtime_bootstrap": "v1",
        "tau_resume_snapshot": "v1",
        "tau_activity_sequence": "v1",
        "tau_turn_commit": "v1",
    }


def test_agent_client_contract_matches_backend_agent_serializer():
    agent_fields = set(agent_models_mod.Agent.model_fields) - {"orm_class"}
    assert agent_fields == {
        "uid",
        "name",
        "description",
        "agent_card",
        "a2a_profile",
        "llm_provider",
        "llm_model",
        "llm_thinking",
        "runtime_config",
        "configuration",
        "last_session_at",
        "runtime_release_uid",
        "workload_user_uid",
        "code_repository_branch_uid",
        "repository_branch",
        "organization_environment_uid",
        "organization_environment_name",
        "runtime_update",
        "observability",
    }
    assert agent_models_mod.Agent.FILTERSET_FIELDS == {
        "uid": ["exact", "in"],
        "name": ["exact"],
        "search": ["exact"],
    }
    assert "agent_type" not in agent_models_mod.AgentSemanticSearchResult.model_fields
    assert "agent_type" not in agent_models_mod.AgentSession.model_fields
    assert not hasattr(agent_models_mod, "CodingAgentService")

    payload = {
        "name": "Research Copilot",
        "description": "Research assistant.",
        "agent_card": {"name": "Research Copilot", "description": "Research assistant."},
        "llm_thinking": "medium",
        "repository_branch": "main",
        "organization_environment_uid": ENVIRONMENT_UID,
        "organization_environment_name": "Development",
        "runtime_update": _agent_runtime_update_contract(),
    }
    assert agent_models_mod.Agent.model_validate(payload).agent_card == payload["agent_card"]
    for invalid_payload in (
        {key: value for key, value in payload.items() if key != "agent_card"},
        {**payload, "agent_card": None},
        {**payload, "agent_card": {}},
        {**payload, "name": "Unrelated label"},
    ):
        with pytest.raises(ValidationError):
            agent_models_mod.Agent.model_validate(invalid_payload)

    # Tolerant reading (ADR 0032): retired keys are dropped, never declared.
    for retired_field, retired_value in (
        ("agent_type", "custom"),
        ("metadata", {}),
        ("has_agent_service", False),
    ):
        assert retired_field not in agent_models_mod.Agent.model_fields
        assert not hasattr(
            agent_models_mod.Agent.model_validate({**payload, retired_field: retired_value}),
            retired_field,
        )

    with pytest.raises(agent_models_mod.ApiError, match="harness_agent workflow"):
        agent_models_mod.Agent.create(name="Research Copilot")
    with pytest.raises(ValueError, match="Unsupported Agent filter"):
        agent_models_mod.Agent._normalize_filter_kwargs({"agent_type": "custom"})


def test_agent_scope_code_repositoryion_is_required_but_nullable():
    payload = {
        "name": "Astro Orchestrator",
        "description": "Helps with workflows.",
        "agent_card": {"name": "Astro Orchestrator", "description": "Helps with workflows."},
        "llm_thinking": "medium",
        "repository_branch": None,
        "organization_environment_uid": None,
        "organization_environment_name": None,
        "runtime_update": _agent_runtime_update_contract("not_deployed"),
    }

    agent = agent_models_mod.Agent.model_validate(payload)

    assert agent.repository_branch is None
    assert agent.organization_environment_uid is None
    assert agent.organization_environment_name is None

    missing_code_repositoryion = dict(payload)
    missing_code_repositoryion.pop("repository_branch")
    with pytest.raises(ValidationError, match="repository_branch"):
        agent_models_mod.Agent.model_validate(missing_code_repositoryion)


def test_agent_filter_sends_environment_read_scope_and_parses_code_repositoryion(monkeypatch):
    captured = {}
    environment_uid = uuid.UUID("22222222-2222-4222-8222-222222222222")
    monkeypatch.setattr(
        code_repository_context,
        "resolve_organization_environment_uid",
        lambda operation: str(environment_uid),
    )
    monkeypatch.setattr(
        code_repository_context,
        "is_authenticated_runtime_code_repository_context",
        lambda: False,
    )

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return {
                "results": [
                    {
                        "uid": "e0e75693-4110-464c-93e0-82c7fd9c9a23",
                        "name": "CodeRepository Executor",
                        "description": "Runs the repository workflow.",
                        "agent_card": {
                            "name": "CodeRepository Executor",
                            "description": "Runs the repository workflow.",
                        },
                        "llm_thinking": "medium",
                        "code_repository_branch_uid": "9d81d63f-b8c9-404d-9f1a-5f2ad29dbf16",
                        "repository_branch": "main",
                        "organization_environment_uid": str(environment_uid),
                        "organization_environment_name": "production",
                        "runtime_update": _agent_runtime_update_contract("current"),
                    }
                ],
                "next": None,
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

    monkeypatch.setattr(base_mod, "make_request", _fake_make_request)
    monkeypatch.setattr(
        agent_models_mod.Agent,
        "build_session",
        classmethod(lambda cls: object()),
    )

    agents = agent_models_mod.Agent.filter(name="CodeRepository Executor", timeout=11)

    assert captured == {
        "r_type": "GET",
        "url": f"{agent_models_mod.Agent.get_object_url()}/",
        "payload": {
            "params": {
                "name": "CodeRepository Executor",
                "organization_environment_uid": str(environment_uid),
            }
        },
        "timeout": 11,
    }
    assert len(agents) == 1
    assert agents[0].repository_branch == "main"
    assert agents[0].organization_environment_uid == str(environment_uid)
    assert agents[0].runtime_update.state == "current"

    monkeypatch.setattr(
        code_repository_context,
        "is_authenticated_runtime_code_repository_context",
        lambda: True,
    )
    agent_models_mod.Agent.filter(name="CodeRepository Executor")
    assert captured["payload"]["params"] == {"name": "CodeRepository Executor"}


def test_agent_discovery_rejects_caller_environment_and_unregistered_branch(monkeypatch):
    with pytest.raises(ValueError, match="cannot override SDK-resolved context"):
        agent_models_mod.Agent.filter(organization_environment_uid=ENVIRONMENT_UID)

    def missing_branch(operation):
        raise code_repository_context.CodeRepositoryBranchContextRequiredError(
            f"{operation} requires a registered active CodeRepositoryBranch."
        )

    monkeypatch.setattr(
        code_repository_context,
        "resolve_organization_environment_uid",
        missing_branch,
    )
    with pytest.raises(
        code_repository_context.CodeRepositoryBranchContextRequiredError,
        match="registered active CodeRepositoryBranch",
    ):
        agent_models_mod.Agent.filter(name="SentinelExecutor")


def test_agent_runtime_discovery_uses_authenticated_backend_environment(monkeypatch):
    monkeypatch.setattr(
        code_repository_context,
        "resolve_organization_environment_uid",
        lambda operation: ENVIRONMENT_UID,
    )
    monkeypatch.setattr(
        code_repository_context,
        "is_authenticated_runtime_code_repository_context",
        lambda: True,
    )
    assert agent_models_mod.Agent._sdk_owned_query_context("Agent.filter") == {}
    assert agent_models_mod.Agent._sdk_owned_query_context("Agent.semantic_search") == {}
    assert agent_models_mod.Agent._sdk_owned_query_context("Agent.get") == {}


def test_agent_get_parses_runtime_update_projection(monkeypatch):
    captured = {}
    agent_uid = "e0e75693-4110-464c-93e0-82c7fd9c9a23"
    runtime_release_uid = "ee79fff7-813b-481b-8726-8a7e7a778862"
    environment_uid = uuid.UUID("22222222-2222-4222-8222-222222222222")

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return {
                "uid": agent_uid,
                "name": "CodeRepository Executor",
                "description": "Runs the repository workflow.",
                "agent_card": {
                    "name": "CodeRepository Executor",
                    "description": "Runs the repository workflow.",
                },
                "llm_thinking": "medium",
                "runtime_release_uid": runtime_release_uid,
                "code_repository_branch_uid": "9d81d63f-b8c9-404d-9f1a-5f2ad29dbf16",
                "repository_branch": "main",
                "organization_environment_uid": str(environment_uid),
                "organization_environment_name": "production",
                "runtime_update": _agent_runtime_update_contract("update_failed"),
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

    monkeypatch.setattr(base_mod, "make_request", _fake_make_request)
    monkeypatch.setattr(
        agent_models_mod.Agent,
        "build_session",
        classmethod(lambda cls: object()),
    )

    agent = agent_models_mod.Agent.get(
        pk=agent_uid,
        timeout=12,
    )

    assert captured == {
        "r_type": "GET",
        "url": f"{agent_models_mod.Agent.get_object_url()}/{agent_uid}/",
        "payload": {"params": {}},
        "timeout": 12,
    }
    assert agent.runtime_release_uid == runtime_release_uid
    assert agent.runtime_update.state == "update_failed"
    assert agent.runtime_update.needs_redeploy is True
    assert agent.runtime_update.remediation.tool == "agent.update_runtime"
    with pytest.raises(ValueError, match="cannot override SDK-resolved context"):
        agent_models_mod.Agent.get(
            pk=agent_uid,
            organization_environment_uid=environment_uid,
        )
    assert (
        agent_models_mod.Agent.model_validate(
            {**FakeResponse.json(), "runtime_release_uid": None}
        ).runtime_release_uid
        is None
    )

    # Tolerant reading (ADR 0032): an undeclared projection is dropped.
    assert not hasattr(
        agent_models_mod.Agent.model_validate(
            {**FakeResponse.json(), "unexpected_projection": "ignored"}
        ),
        "unexpected_projection",
    )


def test_agent_semantic_search_sends_environment_scope_and_parses_code_repositoryion(monkeypatch):
    captured = {}
    environment_uid = uuid.UUID("22222222-2222-4222-8222-222222222222")
    monkeypatch.setattr(
        code_repository_context,
        "resolve_organization_environment_uid",
        lambda operation: str(environment_uid),
    )
    monkeypatch.setattr(
        code_repository_context,
        "is_authenticated_runtime_code_repository_context",
        lambda: False,
    )

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return [
                {
                    "uid": "e0e75693-4110-464c-93e0-82c7fd9c9a23",
                    "name": "CodeRepository Executor",
                    "description": "CodeRepository coding agent.",
                    "code_repository_branch_uid": "9d81d63f-b8c9-404d-9f1a-5f2ad29dbf16",
                    "repository_branch": "main",
                    "organization_environment_uid": str(environment_uid),
                    "organization_environment_name": "production",
                    "runtime_update": _agent_runtime_update_contract("unknown"),
                    "semantic_score": 0.91,
                    "text_score": 0.74,
                    "combined_score": 0.85,
                }
            ]

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
    monkeypatch.setattr(
        agent_models_mod.Agent,
        "build_session",
        classmethod(lambda cls: object()),
    )

    results = agent_models_mod.Agent.semantic_search(
        "repository coding",
        limit=7,
        timeout=13,
    )

    assert captured == {
        "r_type": "POST",
        "url": f"{agent_models_mod.Agent.get_object_url()}/semantic-search/",
        "payload": {
            "json": {
                "organization_environment_uid": str(environment_uid),
                "q": "repository coding",
                "limit": 7,
            }
        },
        "timeout": 13,
    }
    assert len(results) == 1
    assert results[0].code_repository_branch_uid == "9d81d63f-b8c9-404d-9f1a-5f2ad29dbf16"
    assert results[0].organization_environment_name == "production"
    assert results[0].runtime_update.state == "unknown"
    assert results[0].runtime_update.needs_redeploy is None

    monkeypatch.setattr(
        code_repository_context,
        "is_authenticated_runtime_code_repository_context",
        lambda: True,
    )
    agent_models_mod.Agent.semantic_search("repository coding", limit=7)
    assert "organization_environment_uid" not in captured["payload"]["json"]


def test_agent_has_no_one_shot_model_response_methods():
    assert not hasattr(agent_models_mod.Agent, "respond")
    assert not hasattr(agent_models_mod.Agent, "stream_response")
    assert not hasattr(agent_models_mod.Agent, "resolve_runtime_access")


WORKLOAD_USER_UID = "88888888-8888-4888-8888-888888888888"


@pytest.mark.parametrize(
    ("extra", "expected"),
    [
        ({"workload_user_uid": WORKLOAD_USER_UID}, WORKLOAD_USER_UID),
        ({"workload_user_uid": None}, None),
        ({}, None),
    ],
)
def test_agent_reads_workload_user_uid(extra, expected):
    payload = {
        "name": "Research Copilot",
        "description": "Research assistant.",
        "agent_card": {"name": "Research Copilot", "description": "Research assistant."},
        "llm_thinking": "medium",
        "repository_branch": "main",
        "organization_environment_uid": ENVIRONMENT_UID,
        "organization_environment_name": "Development",
        "runtime_update": _agent_runtime_update_contract(),
        **extra,
    }

    assert agent_models_mod.Agent.model_validate(payload).workload_user_uid == expected
