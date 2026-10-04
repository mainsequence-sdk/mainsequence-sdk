from __future__ import annotations

import importlib
import json
import types

from tests.cli.support import TEAM_UID


def test_list_agents_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    class FakeAgent:
        def __init__(self, uid, name):
            self.uid = uid
            self.name = name

        def model_dump(self, mode="json"):
            return {"uid": self.uid, "name": self.name}

    def _run_sdk_model_operation(
        *, module_name, class_name, operation, code_repository_id_env=None
    ):
        captured["module_name"] = module_name
        captured["class_name"] = class_name

        class _ClientAgent:
            @classmethod
            def filter(cls, timeout=None, **kwargs):
                captured["timeout"] = timeout
                captured["filters"] = kwargs
                return [FakeAgent("e0e75693-4110-464c-93e0-82c7fd9c9a23", "Research Copilot")]

        return operation(_ClientAgent)

    monkeypatch.setattr(api_mod, "_run_sdk_model_operation", _run_sdk_model_operation)

    out = api_mod.list_agents(
        timeout=9,
        filters={"search": "Research"},
    )
    assert captured == {
        "module_name": "mainsequence.client.agent_runtime_models",
        "class_name": "Agent",
        "timeout": 9,
        "filters": {"search": "Research"},
    }
    assert out == [
        {
            "uid": "e0e75693-4110-464c-93e0-82c7fd9c9a23",
            "name": "Research Copilot",
        }
    ]


def test_agent_create_command_is_not_advertised(cli_mod):
    api_mod = importlib.import_module("mainsequence.cli.api")
    assert not hasattr(api_mod, "create_agent")
    assert "create" not in {command.name for command in cli_mod.agent.registered_commands}


def test_semantic_search_agents_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    class FakeSearchResult:
        def __init__(self):
            self.uid = "e0e75693-4110-464c-93e0-82c7fd9c9a23"
            self.name = "Research Copilot"
            self.description = "Searchable data research agent."
            self.semantic_score = 0.91
            self.text_score = 0.74
            self.combined_score = 0.85

        def model_dump(self, mode="json"):
            return {
                "uid": self.uid,
                "name": self.name,
                "description": self.description,
                "semantic_score": self.semantic_score,
                "text_score": self.text_score,
                "combined_score": self.combined_score,
            }

    def _run_sdk_model_operation(
        *, module_name, class_name, operation, code_repository_id_env=None
    ):
        captured["module_name"] = module_name
        captured["class_name"] = class_name

        class _ClientAgent:
            @classmethod
            def semantic_search(
                cls,
                q,
                *,
                limit=20,
                timeout=None,
            ):
                captured["q"] = q
                captured["limit"] = limit
                captured["timeout"] = timeout
                return [FakeSearchResult()]

        return operation(_ClientAgent)

    monkeypatch.setattr(api_mod, "_run_sdk_model_operation", _run_sdk_model_operation)

    out = api_mod.semantic_search_agents(
        "data research",
        limit=10,
        timeout=17,
    )
    assert captured == {
        "module_name": "mainsequence.client.agent_runtime_models",
        "class_name": "Agent",
        "q": "data research",
        "limit": 10,
        "timeout": 17,
    }
    assert out == [
        {
            "uid": "e0e75693-4110-464c-93e0-82c7fd9c9a23",
            "name": "Research Copilot",
            "description": "Searchable data research agent.",
            "semantic_score": 0.91,
            "text_score": 0.74,
            "combined_score": 0.85,
        }
    ]


def test_get_agent_uses_agent_uid_detail_route(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}
    agent_uid = "e0e75693-4110-464c-93e0-82c7fd9c9a23"

    class FakeAgent:
        @staticmethod
        def model_dump(mode="json"):
            return {
                "uid": agent_uid,
                "name": "Research Copilot",
            }

    def _run_sdk_model_operation(
        *, module_name, class_name, operation, code_repository_id_env=None
    ):
        captured["module_name"] = module_name
        captured["class_name"] = class_name

        class _ClientAgent:
            @classmethod
            def get_by_uid(cls, uid, timeout=None):
                captured["uid"] = uid
                captured["timeout"] = timeout
                return FakeAgent()

        return operation(_ClientAgent)

    monkeypatch.setattr(api_mod, "_run_sdk_model_operation", _run_sdk_model_operation)

    out = api_mod.get_agent(agent_uid, timeout=12)
    assert captured == {
        "module_name": "mainsequence.client.agent_runtime_models",
        "class_name": "Agent",
        "uid": agent_uid,
        "timeout": 12,
    }
    assert out["uid"] == agent_uid


def test_list_agent_sessions_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}
    agent_uid = "e0e75693-4110-464c-93e0-82c7fd9c9a23"
    session_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"

    class FakeAgentSession:
        @staticmethod
        def model_dump(mode="json"):
            return {
                "uid": session_uid,
                "agent_uid": agent_uid,
                "status": "completed",
                "llm_provider": "openai",
                "llm_model": "gpt-5.4",
                "engine_name": "codex",
            }

    def _run_sdk_model_operation(
        *, module_name, class_name, operation, code_repository_id_env=None
    ):
        captured["module_name"] = module_name
        captured["class_name"] = class_name

        class _ClientAgentSession:
            @classmethod
            def filter(cls, timeout=None, **filters):
                captured["timeout"] = timeout
                captured["filters"] = filters
                return [FakeAgentSession()]

        return operation(_ClientAgentSession)

    monkeypatch.setattr(api_mod, "_run_sdk_model_operation", _run_sdk_model_operation)

    out = api_mod.list_agent_sessions(
        timeout=18,
        filters={"status": "completed"},
        agent_uid=agent_uid,
    )
    assert captured == {
        "module_name": "mainsequence.client.agent_runtime_models",
        "class_name": "AgentSession",
        "timeout": 18,
        "filters": {"status": "completed", "agent_uid": agent_uid},
    }
    assert out == [FakeAgentSession().model_dump()]


def test_get_or_create_agent_session_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}
    agent_uid = "e0e75693-4110-464c-93e0-82c7fd9c9a23"
    session_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
    parent_session_uid = "33333333-3333-4333-8333-333333333333"

    class FakeAgentSession:
        @staticmethod
        def model_dump(mode="json"):
            return {
                "uid": session_uid,
                "agent_uid": agent_uid,
                "agent_name": "Research Copilot",
                "parent_session_uid": parent_session_uid,
                "name": "Quarterly portfolio review",
                "status": "running",
                "llm_provider": "openai",
                "llm_model": "gpt-5.4",
                "engine_name": "codex",
                "bound_handle": {"handle_unique_id": "portfolio-review-q2-2026"},
            }

    def _run_sdk_model_operation(
        *, module_name, class_name, operation, code_repository_id_env=None
    ):
        captured["module_name"] = module_name
        captured["class_name"] = class_name

        class _ClientAgent:
            @classmethod
            def get_by_uid(cls, uid, timeout=None):
                captured["agent_uid"] = uid
                captured["get_timeout"] = timeout

                class _Agent:
                    @staticmethod
                    def get_or_create_session(**kwargs):
                        captured["kwargs"] = kwargs
                        return FakeAgentSession()

                return _Agent()

        return operation(_ClientAgent)

    monkeypatch.setattr(api_mod, "_run_sdk_model_operation", _run_sdk_model_operation)

    out = api_mod.get_or_create_agent_session(
        agent_uid,
        handle_unique_id="portfolio-review-q2-2026",
        name="Quarterly portfolio review",
        parent_session_uid=parent_session_uid,
        llm_provider="openai",
        llm_model="gpt-5.4",
        llm_thinking="",
        timeout=18,
    )
    assert captured == {
        "module_name": "mainsequence.client.agent_runtime_models",
        "class_name": "Agent",
        "agent_uid": agent_uid,
        "get_timeout": 18,
        "kwargs": {
            "session_uid": None,
            "handle_unique_id": "portfolio-review-q2-2026",
            "name": "Quarterly portfolio review",
            "parent_session_uid": parent_session_uid,
            "llm_provider": "openai",
            "llm_model": "gpt-5.4",
            "llm_thinking": "",
            "timeout": 18,
        },
    }
    assert out["uid"] == session_uid
    assert out["bound_handle"]["handle_unique_id"] == "portfolio-review-q2-2026"


def test_get_agent_session_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}
    session_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"

    class FakeAgentSession:
        @staticmethod
        def model_dump(mode="json"):
            return {
                "uid": session_uid,
                "agent_uid": "e0e75693-4110-464c-93e0-82c7fd9c9a23",
                "status": "completed",
                "llm_provider": "openai",
                "llm_model": "gpt-5.4",
                "engine_name": "codex",
            }

    def _run_sdk_model_operation(
        *, module_name, class_name, operation, code_repository_id_env=None
    ):
        captured["module_name"] = module_name
        captured["class_name"] = class_name

        class _ClientAgentSession:
            @classmethod
            def get(cls, pk=None, timeout=None):
                captured["pk"] = pk
                captured["timeout"] = timeout
                return FakeAgentSession()

        return operation(_ClientAgentSession)

    monkeypatch.setattr(api_mod, "_run_sdk_model_operation", _run_sdk_model_operation)

    out = api_mod.get_agent_session(session_uid, timeout=18)
    assert captured == {
        "module_name": "mainsequence.client.agent_runtime_models",
        "class_name": "AgentSession",
        "pk": session_uid,
        "timeout": 18,
    }
    assert out["uid"] == session_uid


def test_send_agent_session_a2a_message_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}
    session_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"

    monkeypatch.setattr(
        api_mod,
        "get_runtime_access_cache",
        lambda agent_session_uid: {
            "coding_agent_service_uid": "7bd86be3-d11a-4ad1-8fe2-9260ccdbca7f",
            "mode": "token",
            "rpc_url": "https://7bd86be3-d11a-4ad1-8fe2-9260ccdbca7f.coding-agent.main-sequence.app/",
            "token": "tok-secret",
        },
    )

    def _save_cache(agent_session_uid, access_payload):
        captured["saved_cache"] = {
            "agent_session_uid": agent_session_uid,
            "access_payload": access_payload,
        }
        return {}

    monkeypatch.setattr(api_mod, "save_runtime_access_cache", _save_cache)

    def _run_sdk_model_operation(
        *, module_name, class_name, operation, code_repository_id_env=None
    ):
        captured["module_name"] = module_name
        captured["class_name"] = class_name

        class _ClientAgentSession:
            _cached_runtime_access = None

            @classmethod
            def cache_runtime_access(cls, agent_session, runtime_access):
                captured["cached_runtime_access"] = {
                    "agent_session": agent_session,
                    "runtime_access": runtime_access,
                }
                cls._cached_runtime_access = runtime_access
                return runtime_access

            @classmethod
            def get_cached_runtime_access(cls, agent_session):
                captured["get_cached_runtime_access"] = agent_session
                return cls._cached_runtime_access

            @classmethod
            def send_a2a_message(cls, agent_session, **kwargs):
                captured["agent_session"] = agent_session
                captured["kwargs"] = kwargs
                return {
                    "message": {
                        "messageId": "msg-runtime-output",
                        "role": "ROLE_RESPONDER",
                        "contextId": agent_session,
                        "parts": [{"text": "Done."}],
                    }
                }

        return operation(_ClientAgentSession)

    monkeypatch.setattr(api_mod, "_run_sdk_model_operation", _run_sdk_model_operation)

    out = api_mod.send_agent_session_a2a_message(
        session_uid,
        message="Return JSON.",
        message_id="msg-client-1",
        strict_dictionary=True,
        json_repair_attempts=3,
        timeout=21,
    )

    assert captured["module_name"] == "mainsequence.client.agent_runtime_models"
    assert captured["class_name"] == "AgentSession"
    assert captured["agent_session"] == session_uid
    assert captured["cached_runtime_access"] == {
        "agent_session": session_uid,
        "runtime_access": {
            "coding_agent_service_uid": "7bd86be3-d11a-4ad1-8fe2-9260ccdbca7f",
            "mode": "token",
            "rpc_url": "https://7bd86be3-d11a-4ad1-8fe2-9260ccdbca7f.coding-agent.main-sequence.app/",
            "token": "tok-secret",
        },
    }
    assert captured["kwargs"]["message"] == "Return JSON."
    assert captured["kwargs"]["message_id"] == "msg-client-1"
    assert captured["kwargs"]["strict_dictionary"] is True
    assert captured["kwargs"]["json_repair_attempts"] == 3
    assert "omit_reasoning" not in captured["kwargs"]
    assert captured["saved_cache"]["agent_session_uid"] == session_uid
    assert out["message"]["parts"] == [{"text": "Done."}]


def test_list_agent_users_can_view_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    def _run_sdk_model_operation(
        *, module_name, class_name, operation, code_repository_id_env=None
    ):
        captured["module_name"] = module_name
        captured["class_name"] = class_name

        class _ClientAgent:
            @classmethod
            def get_by_uid(cls, uid, timeout=None):
                captured["uid"] = uid
                captured["timeout"] = timeout

                class _Agent:
                    def can_view(self, timeout=None):
                        captured["can_view_timeout"] = timeout
                        return types.SimpleNamespace(
                            model_dump=lambda mode="json": {
                                "access_level": "view",
                                "users": [{"id": 7, "username": "viewer"}],
                                "teams": [],
                            }
                        )

                return _Agent()

        return operation(_ClientAgent)

    monkeypatch.setattr(api_mod, "_run_sdk_model_operation", _run_sdk_model_operation)

    out = api_mod.list_agent_users_can_view("e0e75693-4110-464c-93e0-82c7fd9c9a23", timeout=16)
    assert captured == {
        "module_name": "mainsequence.client.agent_runtime_models",
        "class_name": "Agent",
        "uid": "e0e75693-4110-464c-93e0-82c7fd9c9a23",
        "timeout": 16,
        "can_view_timeout": 16,
    }
    assert out["users"][0]["username"] == "viewer"


def test_agent_list(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_agents",
        lambda timeout=None, filters=None: [
            {
                "uid": "e0e75693-4110-464c-93e0-82c7fd9c9a23",
                "name": "Research Copilot",
                "status": "active",
                "labels": ["research", "desk"],
                "llm_provider": "openai",
                "llm_model": "gpt-5.4",
                "engine_name": "codex",
                "last_run_at": "2026-04-10T09:15:00Z",
            }
        ],
    )

    result = runner.invoke(
        cli_mod.app,
        ["agent", "list", "--filter", "name=SentinelExecutor"],
    )
    assert result.exit_code == 0
    assert "Agents" in result.output
    assert "UID" in result.output
    assert "e0e756" in result.output
    assert "Resear" in result.output
    assert "Copilo" in result.output
    assert "Total agents: 1" in result.output


def test_agent_list_json(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_agents",
        lambda timeout=None, filters=None: [
            {
                "uid": "e0e75693-4110-464c-93e0-82c7fd9c9a23",
                "name": "Research Copilot",
                "status": "active",
                "labels": ["research", "desk"],
                "llm_provider": "openai",
                "llm_model": "gpt-5.4",
                "engine_name": "codex",
                "last_run_at": "2026-04-10T09:15:00Z",
            }
        ],
    )

    result = runner.invoke(
        cli_mod.app,
        ["agent", "list", "--json"],
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload[0]["uid"] == "e0e75693-4110-464c-93e0-82c7fd9c9a23"
    assert payload[0]["llm_model"] == "gpt-5.4"


def test_agent_search(cli_mod, runner, monkeypatch):
    captured = {}
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _search(q, *, limit=20, timeout=None):
        captured.update(
            {
                "q": q,
                "limit": limit,
                "timeout": timeout,
            }
        )
        return [
            {
                "uid": "e0e75693-4110-464c-93e0-82c7fd9c9a23",
                "name": "Research Copilot",
                "description": "Searchable data research agent.",
                "semantic_score": 0.91,
                "text_score": 0.74,
                "combined_score": 0.85,
            }
        ]

    monkeypatch.setattr(cli_mod, "semantic_search_agents", _search)

    result = runner.invoke(
        cli_mod.app,
        [
            "agent",
            "search",
            "data research",
            "--limit",
            "10",
            "--timeout",
            "17",
        ],
    )
    assert result.exit_code == 0
    assert captured == {
        "q": "data research",
        "limit": 10,
        "timeout": 17,
    }
    assert "Agent Search Results" in result.output
    assert "UID" in result.output
    assert "Research" in result.output
    assert "Copilot" in result.output
    assert "e0e756" in result.output
    assert "0.85" in result.output
    assert 'Agent search matches for "data research": 1' in result.output


def test_agent_search_json(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "semantic_search_agents",
        lambda q, **kwargs: [
            {
                "uid": "e0e75693-4110-464c-93e0-82c7fd9c9a23",
                "name": "Research Copilot",
                "description": "Searchable data research agent.",
                "semantic_score": 0.91,
                "text_score": 0.74,
                "combined_score": 0.85,
            }
        ],
    )

    result = runner.invoke(
        cli_mod.app,
        [
            "agent",
            "search",
            "data research",
            "--json",
        ],
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload[0]["uid"] == "e0e75693-4110-464c-93e0-82c7fd9c9a23"
    assert payload[0]["combined_score"] == 0.85


def test_agent_detail_uses_agent_uid(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    agent_uid = "e0e75693-4110-464c-93e0-82c7fd9c9a23"
    captured = {}

    def _get(agent_uid_arg, timeout=None):
        captured["agent_uid"] = agent_uid_arg
        captured["timeout"] = timeout
        return {
            "uid": agent_uid,
            "name": "Research Copilot",
            "status": "active",
            "labels": ["research"],
            "runtime_config": {"temperature": 0},
            "configuration": {"mode": "analysis"},
            "metadata": {"owner": "quant"},
        }

    monkeypatch.setattr(cli_mod, "get_agent", _get)

    result = runner.invoke(cli_mod.app, ["agent", "detail", agent_uid, "--timeout", "11"])
    assert result.exit_code == 0
    assert captured == {"agent_uid": agent_uid, "timeout": 11}
    assert "Agent" in result.output
    assert agent_uid[:8] in result.output


def test_agent_logs_forwards_owner_filters(cli_mod, runner, monkeypatch):
    captured = {}
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def fake_get_agent_logs(agent_uid, **kwargs):
        captured.update(agent_uid=agent_uid, **kwargs)
        return {
            "organization_environment_uid": "environment-uid",
            "start": 100,
            "end": 200,
            "next_cursor": None,
            "truncated": False,
            "rows": [{"severity": "ERROR", "message": "failed"}],
        }

    monkeypatch.setattr(cli_mod, "get_agent_logs", fake_get_agent_logs)

    result = runner.invoke(
        cli_mod.app,
        [
            "agent",
            "logs",
            "agent-uid-1",
            "--severity",
            "ERROR",
            "--agent-session-uid",
            "session-uid-1",
        ],
    )

    assert result.exit_code == 0
    assert captured["agent_uid"] == "agent-uid-1"
    assert captured["severity"] == "ERROR"
    assert captured["agent_session_uid"] == "session-uid-1"
    assert "failed" in result.output


def test_agent_session_logs_has_fixed_owner_path(cli_mod, runner, monkeypatch):
    captured = {}
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def fake_get_agent_session_logs(agent_session_uid, **kwargs):
        captured.update(agent_session_uid=agent_session_uid, **kwargs)
        return {
            "organization_environment_uid": "environment-uid",
            "start": 100,
            "end": 200,
            "next_cursor": None,
            "truncated": False,
            "rows": [],
        }

    monkeypatch.setattr(cli_mod, "get_agent_session_logs", fake_get_agent_session_logs)

    result = runner.invoke(
        cli_mod.app,
        ["agent", "session", "logs", "session-uid-1", "--event", "tool.completed"],
    )

    assert result.exit_code == 0
    assert captured["agent_session_uid"] == "session-uid-1"
    assert captured["event"] == "tool.completed"


def test_agent_session_list_scoped_by_agent_uid(cli_mod, runner, monkeypatch):
    captured = {}
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    agent_uid = "e0e75693-4110-464c-93e0-82c7fd9c9a23"
    session_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"

    def _list_agent_sessions(*, timeout=None, filters=None, agent_uid=None):
        captured["timeout"] = timeout
        captured["filters"] = filters
        captured["agent_uid"] = agent_uid
        return [
            {
                "uid": session_uid,
                "agent_uid": agent_uid,
                "agent_name": "Research Copilot",
                "status": "running",
                "runtime_state": "connected",
                "started_at": "2026-04-11T09:15:00Z",
                "name": "Rates check",
            }
        ]

    monkeypatch.setattr(cli_mod, "list_agent_sessions", _list_agent_sessions)

    result = runner.invoke(
        cli_mod.app,
        [
            "agent",
            "session",
            "list",
            "--agent-uid",
            agent_uid,
            "--filter",
            "status=running",
            "--timeout",
            "12",
        ],
    )
    assert result.exit_code == 0
    assert captured == {
        "timeout": 12,
        "filters": {"status": "running"},
        "agent_uid": agent_uid,
    }
    assert "Agent Sessions" in result.output
    assert "3f1cc45" in result.output
    assert "Copilot" in result.output
    assert "connect" in result.output
    assert "Total agent sessions: 1" in result.output


def test_agent_session_get_or_create_by_handle(cli_mod, runner, monkeypatch):
    captured = {}
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    agent_uid = "e0e75693-4110-464c-93e0-82c7fd9c9a23"
    session_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
    parent_session_uid = "33333333-3333-4333-8333-333333333333"

    def _get_or_create_agent_session(agent_uid_arg, **kwargs):
        captured["agent_uid"] = agent_uid_arg
        captured["kwargs"] = kwargs
        return {
            "uid": session_uid,
            "agent_uid": agent_uid,
            "agent_name": "Research Copilot",
            "parent_session_uid": parent_session_uid,
            "name": "Quarterly portfolio review",
            "status": "running",
            "llm_provider": "openai",
            "llm_model": "gpt-5.4",
            "engine_name": "codex",
            "bound_handle": {"handle_unique_id": "portfolio-review-q2-2026"},
        }

    monkeypatch.setattr(cli_mod, "get_or_create_agent_session", _get_or_create_agent_session)

    result = runner.invoke(
        cli_mod.app,
        [
            "agent",
            "session",
            "get_or_create",
            agent_uid,
            "--handle-unique-id",
            "portfolio-review-q2-2026",
            "--name",
            "Quarterly portfolio review",
            "--parent-session-uid",
            parent_session_uid,
            "--llm-provider",
            "openai",
            "--llm-model",
            "gpt-5.4",
            "--llm-thinking",
            "",
            "--timeout",
            "12",
            "--json",
        ],
    )
    assert result.exit_code == 0
    assert captured == {
        "agent_uid": agent_uid,
        "kwargs": {
            "session_uid": None,
            "handle_unique_id": "portfolio-review-q2-2026",
            "name": "Quarterly portfolio review",
            "parent_session_uid": parent_session_uid,
            "llm_provider": "openai",
            "llm_model": "gpt-5.4",
            "llm_thinking": "",
            "timeout": 12,
        },
    }
    payload = json.loads(result.output)
    assert payload["uid"] == session_uid
    assert payload["bound_handle"]["handle_unique_id"] == "portfolio-review-q2-2026"


def test_agent_session_get_or_create_requires_one_lookup_key(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    result = runner.invoke(
        cli_mod.app,
        [
            "agent",
            "session",
            "get_or_create",
            "e0e75693-4110-464c-93e0-82c7fd9c9a23",
            "--session-uid",
            "3f1cc452-43ec-49cb-b2ba-87dbac164d29",
            "--handle-unique-id",
            "portfolio-review-q2-2026",
        ],
    )

    assert result.exit_code == 1
    assert "Provide exactly one of --session-uid or --handle-unique-id." in result.output


def test_agent_session_detail(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "get_agent_session",
        lambda agent_session_uid, timeout=None: {
            "uid": agent_session_uid,
            "agent_uid": "e0e75693-4110-464c-93e0-82c7fd9c9a23",
            "status": "completed",
            "started_at": "2026-04-11T09:15:00Z",
            "ended_at": "2026-04-11T09:16:00Z",
            "llm_provider": "openai",
            "llm_model": "gpt-5.4",
            "engine_name": "codex",
            "created_by_user_uid": "fdf409f7-d16f-4f71-986b-9057db6c7eca",
            "input_text": "Summarize rates moves",
            "output_text": "Bunds rallied 4bp.",
            "usage_summary": {"prompt_tokens": 100},
            "session_metadata": {"origin": "cli"},
        },
    )

    result = runner.invoke(
        cli_mod.app, ["agent", "session", "detail", "3f1cc452-43ec-49cb-b2ba-87dbac164d29"]
    )
    assert result.exit_code == 0
    assert "Agent Session Details" in result.output
    assert "Summarize rates moves" in result.output
    assert "Bunds rallied 4bp." in result.output
    assert "prompt_tokens" in result.output


def test_removed_agent_session_runtime_commands_are_not_available(cli_mod, runner):
    removed_commands = [
        ["agent", "session", "runtime", "resolve", "3f1cc452-43ec-49cb-b2ba-87dbac164d29"],
        ["agent", "session", "runtime", "chat", "3f1cc452-43ec-49cb-b2ba-87dbac164d29"],
        ["agent", "session", "runtime", "cancel", "3f1cc452-43ec-49cb-b2ba-87dbac164d29"],
        ["agent", "session", "runtime", "detach", "3f1cc452-43ec-49cb-b2ba-87dbac164d29"],
        ["agent", "session", "resolve_runtime_access", "3f1cc452-43ec-49cb-b2ba-87dbac164d29"],
    ]

    for command in removed_commands:
        result = runner.invoke(cli_mod.app, command)
        assert result.exit_code != 0


def test_agent_session_a2a_send_always_returns_json(cli_mod, runner, monkeypatch):
    captured = {}
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _send(agent_session_uid, **kwargs):
        captured["agent_session_uid"] = agent_session_uid
        captured["kwargs"] = kwargs
        return {
            "message": {
                "messageId": "msg-runtime-output",
                "role": "ROLE_RESPONDER",
                "contextId": agent_session_uid,
                "parts": [{"text": '{"ok": true}'}],
            }
        }

    monkeypatch.setattr(cli_mod, "send_agent_session_a2a_message", _send)

    result = runner.invoke(
        cli_mod.app,
        [
            "agent",
            "session",
            "a2a",
            "send",
            "3f1cc452-43ec-49cb-b2ba-87dbac164d29",
            "--message",
            "Return JSON.",
            "--message-id",
            "msg-client-1",
            "--strict-dictionary",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["message"]["parts"] == [{"text": '{"ok": true}'}]
    assert captured["agent_session_uid"] == "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
    assert captured["kwargs"]["message"] == "Return JSON."
    assert captured["kwargs"]["message_id"] == "msg-client-1"
    assert captured["kwargs"]["strict_dictionary"] is True
    assert captured["kwargs"]["json_repair_attempts"] == 3
    assert captured["kwargs"]["response_kind"] == "message"
    assert "omit_reasoning" not in captured["kwargs"]


def test_agent_session_a2a_send_does_not_duplicate_error_prefix(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _send(*args, **kwargs):
        raise cli_mod.ApiError("Agent session A2A message send failed: 502 POST runtime")

    monkeypatch.setattr(cli_mod, "send_agent_session_a2a_message", _send)

    result = runner.invoke(
        cli_mod.app,
        [
            "agent",
            "session",
            "a2a",
            "send",
            "3f1cc452-43ec-49cb-b2ba-87dbac164d29",
            "--message",
            "Return JSON.",
        ],
    )

    assert result.exit_code == 1
    combined_output = result.output + (result.stderr or "")
    assert "Agent session A2A message send failed: 502 POST runtime" in combined_output
    assert (
        "Agent session A2A message send failed: Agent session A2A message send failed"
        not in combined_output
    )


def test_agent_session_a2a_send_does_not_require_runtime_resolve(cli_mod, runner, monkeypatch):
    called = {}
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _send(*args, **kwargs):
        called["send"] = True
        return {
            "message": {
                "messageId": "msg-runtime-output",
                "role": "ROLE_RESPONDER",
                "contextId": args[0],
                "parts": [{"text": "Done."}],
            }
        }

    monkeypatch.setattr(cli_mod, "send_agent_session_a2a_message", _send)

    result = runner.invoke(
        cli_mod.app,
        [
            "agent",
            "session",
            "a2a",
            "send",
            "3f1cc452-43ec-49cb-b2ba-87dbac164d29",
            "--message",
            "Return JSON.",
        ],
    )

    assert result.exit_code == 0
    assert called == {"send": True}
    assert "Runtime access is not resolved for this agent session" not in result.output
    assert "runtime resolve" not in result.output


def test_agent_session_a2a_send_reports_runtime_auth_error_without_resolve_instruction(
    cli_mod, runner, monkeypatch
):
    session_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _send(*args, **kwargs):
        raise cli_mod.ApiError("401 POST runtime: unauthorized")

    monkeypatch.setattr(cli_mod, "send_agent_session_a2a_message", _send)

    result = runner.invoke(
        cli_mod.app,
        [
            "agent",
            "session",
            "a2a",
            "send",
            session_uid,
            "--message",
            "Return JSON.",
        ],
    )

    assert result.exit_code == 1
    combined_output = result.output + (result.stderr or "")
    assert "401 POST runtime: unauthorized" in combined_output
    assert "runtime resolve" not in combined_output


def test_agent_delete_requires_typed_verification(cli_mod, runner, monkeypatch):
    captured = {}
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    agent_uid = "e0e75693-4110-464c-93e0-82c7fd9c9a23"
    monkeypatch.setattr(
        cli_mod,
        "get_agent",
        lambda agent_uid, timeout=None: {
            "uid": agent_uid,
            "name": "Research Copilot",
            "status": "active",
            "labels": ["research"],
        },
    )

    def _delete(agent_uid_arg, timeout=None):
        captured["agent_uid"] = agent_uid_arg
        captured["timeout"] = timeout
        return {
            "uid": agent_uid,
            "name": "Research Copilot",
            "status": "active",
            "labels": ["research"],
        }

    monkeypatch.setattr(cli_mod, "delete_agent", _delete)

    result = runner.invoke(
        cli_mod.app,
        ["agent", "delete", agent_uid],
        input="Research Copilot\n",
    )
    assert result.exit_code == 0
    assert "Agent Delete Preview" in result.output
    assert "Type agent name 'Research Copilot' to confirm deletion" in result.output
    assert captured["agent_uid"] == agent_uid
    assert f"Agent deleted: agent_uid={agent_uid}" in result.output


def test_agent_can_edit(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    agent_uid = "e0e75693-4110-464c-93e0-82c7fd9c9a23"
    monkeypatch.setattr(
        cli_mod,
        "list_agent_users_can_edit",
        lambda agent_uid, timeout=None: {
            "access_level": "edit",
            "users": [
                {
                    "id": 9,
                    "username": "editor",
                    "email": "editor@example.com",
                    "first_name": "Edit",
                    "last_name": "User",
                }
            ],
            "teams": [{"id": 3, "name": "Research", "description": "Core team", "member_count": 6}],
        },
    )

    result = runner.invoke(cli_mod.app, ["agent", "can_edit", agent_uid])
    assert result.exit_code == 0
    assert "Agent Users Who Can Edit" in result.output
    assert "Agent Teams Who Can Edit" in result.output
    assert "editor@example.com" in result.output
    assert "Total users who can edit: 1" in result.output
    assert "Total teams who can edit: 1" in result.output


def test_agent_add_team_to_edit(cli_mod, runner, monkeypatch):
    captured = {}
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    agent_uid = "e0e75693-4110-464c-93e0-82c7fd9c9a23"

    def _add(agent_uid_arg, team_uid, timeout=None):
        captured["agent_uid"] = agent_uid_arg
        captured["team_uid"] = team_uid
        captured["timeout"] = timeout
        return {
            "ok": True,
            "action": "add_team_to_edit",
            "detail": "Team now has explicit edit access.",
            "object_uid": agent_uid_arg,
            "object_type": "agent.agent",
            "team": {
                "uid": team_uid,
                "name": "Research",
                "description": "Core team",
            },
            "explicit_can_view": True,
            "explicit_can_edit": True,
            "explicit_can_view_team_uids": [team_uid],
            "explicit_can_edit_team_uids": [team_uid],
        }

    monkeypatch.setattr(cli_mod, "add_agent_team_to_edit", _add)

    result = runner.invoke(
        cli_mod.app,
        ["agent", "add_team_to_edit", agent_uid, TEAM_UID],
    )
    assert result.exit_code == 0
    assert captured == {"agent_uid": agent_uid, "team_uid": TEAM_UID, "timeout": None}
    assert "Agent add_team_to_edit completed." in result.output
