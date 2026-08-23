from __future__ import annotations

import hashlib
import importlib
import json
import os
import pathlib
import subprocess
import sys
import types

import pytest
from typer.testing import CliRunner

USER_UID = "8f5d6b54-2f5e-4a8b-bb10-0b17f3f4c123"
TEAM_UID = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
_REAL_SUBPROCESS_RUN = subprocess.run


def _load_cli_module():
    """
    Import mainsequence.cli.cli without requiring mainsequence/__init__.py side effects.

    In this repo, importing `mainsequence` can trigger runtime logging bootstrap that expects
    TDAG-specific env vars and network access. For CLI tests we only need the CLI package.
    """
    try:
        return importlib.import_module("mainsequence.cli.cli")
    except Exception:
        repo_root = pathlib.Path(__file__).resolve().parents[1]
        pkg_root = repo_root / "mainsequence"
        cli_root = pkg_root / "cli"

        for name in ("mainsequence.cli.cli", "mainsequence.cli", "mainsequence"):
            sys.modules.pop(name, None)

        pkg = types.ModuleType("mainsequence")
        pkg.__path__ = [str(pkg_root)]
        sys.modules["mainsequence"] = pkg

        subpkg = types.ModuleType("mainsequence.cli")
        subpkg.__path__ = [str(cli_root)]
        sys.modules["mainsequence.cli"] = subpkg
        return importlib.import_module("mainsequence.cli.cli")


@pytest.fixture()
def cli_mod(monkeypatch):
    module = _load_cli_module()
    monkeypatch.setattr(
        module,
        "get_project_runtime_context",
        lambda *args, **kwargs: types.SimpleNamespace(
            project_uid="project-uid-123",
            repository_branch="main",
            canonical_repository_identity=("github.com/mainsequence-sdk/cli-test-project"),
            commit_sha="a" * 40,
            project_branch_uid="project-branch-uid-123",
            organization_project_environment_uid="environment-uid-123",
            status="resolved",
            detail="",
        ),
    )
    return module


@pytest.fixture()
def runner():
    return CliRunner()


def test_root_version(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_package_version", lambda: "3.18.9")

    result = runner.invoke(cli_mod.app, ["--version"])
    assert result.exit_code == 0
    assert result.output.strip() == "mainsequence 3.18.9"


@pytest.fixture(autouse=True)
def _print_cli_terminal(monkeypatch):
    """
    Print the simulated terminal command and CLI output for each CliRunner invocation.
    """
    original_invoke = CliRunner.invoke

    def _ensure_test_git_checkout(args) -> None:
        values = [str(value) for value in (args or [])]
        if not values or values[0] != "project":
            return
        if len(values) > 1 and values[1] in {"list", "create", "set-up-locally"}:
            return
        candidate = pathlib.Path.cwd()
        if "--path" in values:
            index = values.index("--path")
            if index + 1 < len(values):
                candidate = pathlib.Path(values[index + 1])
        if not candidate.is_dir() or (candidate / ".git").exists():
            return
        _REAL_SUBPROCESS_RUN(
            ["git", "init", "-q", "-b", "main"],
            cwd=candidate,
            check=True,
        )
        _REAL_SUBPROCESS_RUN(
            ["git", "config", "user.email", "cli-tests@example.test"],
            cwd=candidate,
            check=True,
        )
        _REAL_SUBPROCESS_RUN(
            ["git", "config", "user.name", "CLI Tests"],
            cwd=candidate,
            check=True,
        )
        _REAL_SUBPROCESS_RUN(
            ["git", "commit", "-q", "--allow-empty", "-m", "Test checkout"],
            cwd=candidate,
            check=True,
        )
        _REAL_SUBPROCESS_RUN(
            [
                "git",
                "remote",
                "add",
                "origin",
                "git@github.com:mainsequence-sdk/cli-test-project.git",
            ],
            cwd=candidate,
            check=True,
        )

    def _invoke(self, app, args=None, **kwargs):
        _ensure_test_git_checkout(args)
        cmd = " ".join(str(x) for x in (args or []))
        print(f"\n$ mainsequence {cmd}".rstrip())
        result = original_invoke(self, app, args=args, **kwargs)
        out = getattr(result, "output", "")
        if out:
            print(out, end="" if out.endswith("\n") else "\n")
        return result

    monkeypatch.setattr(CliRunner, "invoke", _invoke)


def test_settings_defaults_to_show(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_persistent_config",
        lambda: {
            "backend_url": "https://main-sequence.app",
            "mainsequence_path": "/tmp/mainsequence",
        },
    )
    result = runner.invoke(cli_mod.app, ["settings"])
    assert result.exit_code == 0
    assert "backend_url" in result.output
    assert "mainsequence_path" in result.output


def test_user_show(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(
        cli_mod,
        "get_logged_user_details",
        lambda: {
            "uid": "user-uid-7",
            "username": "jose",
            "email": "jose@main-sequence.io",
            "organization": {"uid": "org-uid-2", "name": "Main Sequence"},
            "is_active": True,
            "is_verified": True,
            "mfa_enabled": False,
            "date_joined": "2026-01-01T10:00:00Z",
            "last_login": "2026-03-15T09:30:00Z",
        },
    )

    result = runner.invoke(cli_mod.app, ["user"])
    assert result.exit_code == 0
    assert "MainSequence User" in result.output
    assert "user-uid-7" in result.output
    assert "jose" in result.output
    assert "jose@main-sequence.io" in result.output
    assert "Main Sequence" in result.output


def test_user_show_json(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(
        cli_mod,
        "get_logged_user_details",
        lambda: {
            "uid": "user-uid-7",
            "username": "jose",
            "email": "jose@main-sequence.io",
            "organization": {"uid": "org-uid-2", "name": "Main Sequence"},
        },
    )

    result = runner.invoke(cli_mod.app, ["user", "--json"])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert "id" not in payload
    assert payload["uid"] == "user-uid-7"
    assert payload["username"] == "jose"
    assert payload["organization"]["name"] == "Main Sequence"


def test_skills_list(cli_mod, runner, monkeypatch, tmp_path):
    bundle_dir = tmp_path / "agent_scaffold"
    (bundle_dir / "skills" / "project_design").mkdir(parents=True)
    (bundle_dir / "skills" / "project_design" / "SKILL.md").write_text(
        "project builder", encoding="utf-8"
    )
    (bundle_dir / "skills" / "data_publishing" / "meta_tables").mkdir(parents=True)
    (bundle_dir / "skills" / "data_publishing" / "meta_tables" / "SKILL.md").write_text(
        "MetaTable publishing", encoding="utf-8"
    )

    monkeypatch.setattr(cli_mod, "_installed_agent_scaffold_bundle_dir", lambda: bundle_dir)

    result = runner.invoke(cli_mod.app, ["skills", "list"])
    assert result.exit_code == 0
    assert "project_design" in result.output
    assert "data_publishing/meta_tables" in result.output


def test_skills_path(cli_mod, runner, monkeypatch, tmp_path):
    bundle_dir = tmp_path / "agent_scaffold"
    (bundle_dir / "skills" / "data_publishing" / "meta_tables").mkdir(parents=True)
    expected = bundle_dir / "skills" / "data_publishing" / "meta_tables" / "SKILL.md"
    expected.write_text("MetaTable publishing", encoding="utf-8")

    monkeypatch.setattr(cli_mod, "_installed_agent_scaffold_bundle_dir", lambda: bundle_dir)

    result = runner.invoke(cli_mod.app, ["skills", "path", "data_publishing/meta_tables"])
    assert result.exit_code == 0
    assert result.output.strip() == str(expected)


def test_skills_path_bundle_root(cli_mod, runner, monkeypatch, tmp_path):
    bundle_dir = tmp_path / "agent_scaffold"
    (bundle_dir / "skills").mkdir(parents=True)

    monkeypatch.setattr(cli_mod, "_installed_agent_scaffold_bundle_dir", lambda: bundle_dir)

    result = runner.invoke(cli_mod.app, ["skills", "path"])
    assert result.exit_code == 0
    assert result.output.strip() == str(bundle_dir / "skills")


def test_skills_path_unique_leaf_name(cli_mod, runner, monkeypatch, tmp_path):
    bundle_dir = tmp_path / "agent_scaffold"
    (bundle_dir / "skills" / "data_publishing" / "meta_tables").mkdir(parents=True)
    expected = bundle_dir / "skills" / "data_publishing" / "meta_tables" / "SKILL.md"
    expected.write_text("MetaTable publishing", encoding="utf-8")

    monkeypatch.setattr(cli_mod, "_installed_agent_scaffold_bundle_dir", lambda: bundle_dir)

    result = runner.invoke(cli_mod.app, ["skills", "path", "meta_tables"])
    assert result.exit_code == 0
    assert result.output.strip() == str(expected)


def test_project_search(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "search_projects",
        lambda q, limit=20, timeout=None: [
            {
                "uid": "project-uid-11",
                "project_name": "alpha-research",
                "repository_branch": "main",
                "cluster_id": 7,
            },
            {
                "uid": "project-uid-12",
                "project_name": "data-live",
                "repository_branch": "release",
                "cluster_id": 9,
            },
        ],
    )

    result = runner.invoke(cli_mod.app, ["project", "search", "alpha", "--limit", "10"])
    assert result.exit_code == 0
    assert "Project Search Results" in result.output
    assert "UID" in result.output
    assert "project-uid-11" in result.output
    assert "Project Name" in result.output
    assert "alpha-research" in result.output
    assert "data-live" in result.output
    assert 'Project search matches for "alpha": 2' in result.output


def test_project_search_rejects_short_query(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "search_projects",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("search_projects should not be called")
        ),
    )

    result = runner.invoke(cli_mod.app, ["project", "search", ".."])
    assert result.exit_code == 1
    assert "Project search failed: Query must contain at least 3 characters." in result.output


def test_project_search_json(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "search_projects",
        lambda q, limit=20, timeout=None: [
            {
                "uid": "project-uid-11",
                "project_name": "alpha-research",
                "repository_branch": "main",
                "cluster_id": 7,
            },
        ],
    )

    result = runner.invoke(cli_mod.app, ["project", "search", "alpha", "--json"])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload == [
        {
            "uid": "project-uid-11",
            "project_name": "alpha-research",
            "repository_branch": "main",
            "cluster_id": 7,
        }
    ]


def test_organization_github_organizations(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_github_organizations",
        lambda: [
            {
                "uid": "github-org-uid-33",
                "display_name": "Main Sequence Projects",
                "login": "mainsequence-projects",
            },
            {
                "uid": "github-org-uid-34",
                "display_name": "Research Labs",
                "login": "research-labs",
            },
        ],
    )

    result = runner.invoke(cli_mod.app, ["organization", "github-organizations"])
    assert result.exit_code == 0
    assert "GitHub Organizations" in result.output
    assert "github-org-uid-33" in result.output
    assert "Main Sequence Projects" in result.output


def test_organization_github_organizations_json(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_github_organizations",
        lambda: [
            {
                "uid": "github-org-uid-33",
                "display_name": "Main Sequence Projects",
                "login": "mainsequence-projects",
            }
        ],
    )

    result = runner.invoke(cli_mod.app, ["organization", "github-organizations", "--json"])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload == [
        {
            "uid": "github-org-uid-33",
            "display_name": "Main Sequence Projects",
            "login": "mainsequence-projects",
        }
    ]


def test_organization_teams_list(cli_mod, runner, monkeypatch):
    team_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_organization_teams",
        lambda timeout=None, filters=None: [
            {
                "uid": team_uid,
                "name": "Research",
                "description": "Model validation",
                "member_count": 4,
                "is_active": True,
            }
        ],
    )

    result = runner.invoke(cli_mod.app, ["organization", "teams", "list"])
    assert result.exit_code == 0
    assert "Organization Teams" in result.output
    assert "Research" in result.output
    assert "Model validation" in result.output
    assert "Total organization teams: 1" in result.output


def test_organization_teams_create(cli_mod, runner, monkeypatch):
    captured = {}
    team_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _create(*, name, description="", timeout=None):
        captured["name"] = name
        captured["description"] = description
        captured["timeout"] = timeout
        return {
            "uid": team_uid,
            "name": name,
            "description": description,
            "member_count": 0,
            "is_active": True,
        }

    monkeypatch.setattr(cli_mod, "create_organization_team", _create)

    result = runner.invoke(
        cli_mod.app,
        ["organization", "teams", "create", "Research", "--description", "Model validation"],
    )
    assert result.exit_code == 0
    assert captured == {"name": "Research", "description": "Model validation", "timeout": None}
    assert "Organization team created: Research" in result.output


def test_organization_teams_edit(cli_mod, runner, monkeypatch):
    captured = {}
    team_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "get_organization_team",
        lambda team_uid_arg, timeout=None: {
            "uid": team_uid_arg,
            "name": "Research",
            "description": "Old description",
            "member_count": 4,
            "is_active": True,
        },
    )

    def _update(team_uid_arg, *, name=None, description=None, is_active=None, timeout=None):
        captured["team_uid"] = team_uid_arg
        captured["name"] = name
        captured["description"] = description
        captured["is_active"] = is_active
        captured["timeout"] = timeout
        return {
            "uid": team_uid_arg,
            "name": name or "Research",
            "description": description or "Old description",
            "member_count": 4,
            "is_active": is_active,
        }

    monkeypatch.setattr(cli_mod, "update_organization_team", _update)

    result = runner.invoke(
        cli_mod.app,
        [
            "organization",
            "teams",
            "edit",
            team_uid,
            "--name",
            "Research Core",
            "--inactive",
        ],
    )
    assert result.exit_code == 0
    assert captured == {
        "team_uid": team_uid,
        "name": "Research Core",
        "description": None,
        "is_active": False,
        "timeout": None,
    }
    assert f"Organization team updated: uid={team_uid}" in result.output


def test_organization_teams_delete(cli_mod, runner, monkeypatch):
    captured = {}
    team_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "get_organization_team",
        lambda team_uid_arg, timeout=None: {
            "uid": team_uid_arg,
            "name": "Research",
            "description": "Model validation",
            "member_count": 4,
            "is_active": True,
        },
    )
    monkeypatch.setattr(cli_mod, "_require_delete_verification", lambda **kwargs: None)

    def _delete(team_uid_arg, *, timeout=None):
        captured["team_uid"] = team_uid_arg
        captured["timeout"] = timeout
        return {
            "uid": team_uid_arg,
            "name": "Research",
            "description": "Model validation",
            "member_count": 4,
            "is_active": True,
        }

    monkeypatch.setattr(cli_mod, "delete_organization_team", _delete)

    result = runner.invoke(cli_mod.app, ["organization", "teams", "delete", team_uid])
    assert result.exit_code == 0
    assert captured == {"team_uid": team_uid, "timeout": None}
    assert f"Organization team deleted: uid={team_uid}" in result.output


def test_list_organization_teams_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    class FakeTeam:
        def __init__(self, team_uid, name):
            self.uid = team_uid
            self.name = name

        def model_dump(self, mode="json"):
            return {"uid": self.uid, "name": self.name}

    def _run_sdk_model_operation(*, module_name, class_name, operation, project_id_env=None):
        captured["module_name"] = module_name
        captured["class_name"] = class_name

        class _ClientTeam:
            @classmethod
            def filter(cls, timeout=None, **kwargs):
                captured["timeout"] = timeout
                captured["filters"] = kwargs
                return [FakeTeam("3f1cc452-43ec-49cb-b2ba-87dbac164d29", "Research")]

        return operation(_ClientTeam)

    monkeypatch.setattr(api_mod, "_run_sdk_model_operation", _run_sdk_model_operation)

    out = api_mod.list_organization_teams(timeout=9, filters={"name__contains": "Res"})
    assert captured == {
        "module_name": "mainsequence.client.models_user",
        "class_name": "Team",
        "timeout": 9,
        "filters": {"name__contains": "Res"},
    }
    assert out == [{"uid": "3f1cc452-43ec-49cb-b2ba-87dbac164d29", "name": "Research"}]


def test_list_agents_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}
    environment_uid = "22222222-2222-4222-8222-222222222222"

    class FakeAgent:
        def __init__(self, uid, name):
            self.uid = uid
            self.name = name

        def model_dump(self, mode="json"):
            return {"uid": self.uid, "name": self.name}

    def _run_sdk_model_operation(*, module_name, class_name, operation, project_id_env=None):
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
        organization_project_environment_uid=environment_uid,
        timeout=9,
        filters={"agent_type": "custom"},
    )
    assert captured == {
        "module_name": "mainsequence.client.agent_runtime_models",
        "class_name": "Agent",
        "timeout": 9,
        "filters": {
            "agent_type": "custom",
            "organization_project_environment_uid": environment_uid,
        },
    }
    assert out == [
        {
            "uid": "e0e75693-4110-464c-93e0-82c7fd9c9a23",
            "name": "Research Copilot",
        }
    ]


def test_get_project_repository_uses_public_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}
    repository_uid = "2bcf47e3-3a79-4f1e-a428-176c0218a8d1"

    class FakeRepository:
        def model_dump(self, mode="json"):
            return {
                "uid": repository_uid,
                "git_ssh_url": "git@github.com:mainsequence-projects/tutorial.git",
                "git_repo_url": "https://github.com/mainsequence-projects/tutorial.git",
            }

    def _run_sdk_model_operation(*, module_name, class_name, operation, **kwargs):
        captured["module_name"] = module_name
        captured["class_name"] = class_name

        class ClientGitRepository:
            @classmethod
            def get_by_uid(cls, uid, timeout=None):
                captured["uid"] = uid
                captured["timeout"] = timeout
                return FakeRepository()

        return operation(ClientGitRepository)

    monkeypatch.setattr(api_mod, "_run_sdk_model_operation", _run_sdk_model_operation)

    result = api_mod.get_project_repository(repository_uid, timeout=12)

    assert captured == {
        "module_name": "mainsequence.client.models_foundry",
        "class_name": "GitRepository",
        "uid": repository_uid,
        "timeout": 12,
    }
    assert result["uid"] == repository_uid
    assert result["git_ssh_url"].startswith("git@github.com:")


def test_semantic_search_agents_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}
    environment_uid = "22222222-2222-4222-8222-222222222222"

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

    def _run_sdk_model_operation(*, module_name, class_name, operation, project_id_env=None):
        captured["module_name"] = module_name
        captured["class_name"] = class_name

        class _ClientAgent:
            @classmethod
            def semantic_search(
                cls,
                q,
                *,
                organization_project_environment_uid,
                limit=20,
                timeout=None,
            ):
                captured["q"] = q
                captured["organization_project_environment_uid"] = (
                    organization_project_environment_uid
                )
                captured["limit"] = limit
                captured["timeout"] = timeout
                return [FakeSearchResult()]

        return operation(_ClientAgent)

    monkeypatch.setattr(api_mod, "_run_sdk_model_operation", _run_sdk_model_operation)

    out = api_mod.semantic_search_agents(
        "data research",
        organization_project_environment_uid=environment_uid,
        limit=10,
        timeout=17,
    )
    assert captured == {
        "module_name": "mainsequence.client.agent_runtime_models",
        "class_name": "Agent",
        "q": "data research",
        "organization_project_environment_uid": environment_uid,
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


def test_create_agent_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    class FakeAgent:
        @staticmethod
        def model_dump(mode="json"):
            return {
                "uid": "e0e75693-4110-464c-93e0-82c7fd9c9a23",
                "name": "Research Copilot",
            }

    def _run_sdk_model_operation(*, module_name, class_name, operation, project_id_env=None):
        captured["module_name"] = module_name
        captured["class_name"] = class_name

        class _ClientAgent:
            @classmethod
            def create(cls, timeout=None, **kwargs):
                captured["timeout"] = timeout
                captured["create_kwargs"] = kwargs
                return FakeAgent()

        return operation(_ClientAgent)

    monkeypatch.setattr(api_mod, "_run_sdk_model_operation", _run_sdk_model_operation)

    out = api_mod.create_agent(
        name="Research Copilot",
        description="Desk agent",
        status="active",
        labels=["research", "desk"],
        llm_provider="openai",
        llm_model="gpt-5.4",
        engine_name="codex",
        runtime_config={"temperature": 0},
        configuration={"mode": "analysis"},
        metadata={"owner": "quant"},
        timeout=14,
    )
    assert captured == {
        "module_name": "mainsequence.client.agent_runtime_models",
        "class_name": "Agent",
        "timeout": 14,
        "create_kwargs": {
            "name": "Research Copilot",
            "description": "Desk agent",
            "status": "active",
            "labels": ["research", "desk"],
            "llm_provider": "openai",
            "llm_model": "gpt-5.4",
            "engine_name": "codex",
            "runtime_config": {"temperature": 0},
            "configuration": {"mode": "analysis"},
            "metadata": {"owner": "quant"},
        },
    }
    assert out["uid"] == "e0e75693-4110-464c-93e0-82c7fd9c9a23"


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

    def _run_sdk_model_operation(*, module_name, class_name, operation, project_id_env=None):
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

    def _run_sdk_model_operation(*, module_name, class_name, operation, project_id_env=None):
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

    def _run_sdk_model_operation(*, module_name, class_name, operation, project_id_env=None):
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

    def _run_sdk_model_operation(*, module_name, class_name, operation, project_id_env=None):
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

    def _run_sdk_model_operation(*, module_name, class_name, operation, project_id_env=None):
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
                        "role": "ROLE_AGENT",
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

    def _run_sdk_model_operation(*, module_name, class_name, operation, project_id_env=None):
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


def test_pydantic_cli_metadata_from_source():
    metadata_mod = importlib.import_module("mainsequence.cli.pydantic_cli")
    meta = metadata_mod.get_cli_field_metadata(
        "mainsequence.client.models_helpers.Job",
        "execution_path",
    )
    assert meta.label == "Execution path"
    assert "content root" in meta.description
    assert "scripts/test.py" in meta.examples


def test_model_filter_parser_uses_filterset_metadata():
    filters_mod = importlib.import_module("mainsequence.cli.model_filters")

    class FakeModel:
        FILTERSET_FIELDS = {
            "id": ["exact", "in"],
            "is_active": ["exact", "isnull"],
            "name": ["contains"],
        }
        FILTER_VALUE_NORMALIZERS = {
            "id": "id",
            "is_active__isnull": "bool",
            "name": "str",
        }

    rows = filters_mod.build_cli_model_filter_rows(FakeModel)
    assert ["id", "exact", "integer ID", "id"] in rows
    assert ["id__in", "in", "comma-separated integer IDs", "id"] in rows
    assert ["is_active__isnull", "isnull", "true/false", "bool"] in rows
    assert ["name__contains", "contains", "text", "str"] in rows

    parsed = filters_mod.parse_cli_model_filters(
        FakeModel,
        ["id__in=1,2,3", "name__contains=daily", "is_active__isnull=true"],
    )
    assert parsed == {
        "id__in": ["1", "2", "3"],
        "name__contains": "daily",
        "is_active__isnull": "true",
    }


def test_shared_compute_validation_supports_k8s_quantities(cli_mod):
    compute_mod = importlib.import_module("mainsequence.client.compute_validation")

    decimal_out = compute_mod.validate_and_normalize_compute_fields(
        cpu_request="500m",
        memory_request="1Gi",
        gpu_request="",
        gpu_type="",
        output_format="decimal",
    )
    assert decimal_out == {
        "cpu_request": "0.5",
        "memory_request": "1",
        "gpu_request": None,
        "gpu_type": None,
    }

    k8s_out = compute_mod.validate_and_normalize_compute_fields(
        cpu_request="500m",
        memory_request="1Gi",
        gpu_request="",
        gpu_type="",
        output_format="k8s",
    )
    assert k8s_out == {
        "cpu_request": "500m",
        "memory_request": "1Gi",
        "gpu_request": None,
        "gpu_type": None,
    }


def test_resolve_compute_defaults_accepts_k8s_quantities(cli_mod):
    cpu_request, memory_request, spot, used_defaults = cli_mod._resolve_compute_defaults(
        cpu_request="500m",
        memory_request="1Gi",
        spot=True,
    )

    assert cpu_request == "0.5"
    assert memory_request == "1"
    assert spot is True
    assert used_defaults == []


def test_project_jobs_create_help_uses_pydantic_metadata(cli_mod, runner):
    result = runner.invoke(cli_mod.app, ["project", "jobs", "create", "--help"])
    assert result.exit_code == 0
    assert "Human-readable job" in result.output
    assert "feature build" in result.output
    assert "scripts/test.py" in result.output
    assert "Five-field crontab" in result.output
    assert "day_of_week" in result.output


def test_settings_show_ignores_session_overrides(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_persistent_config",
        lambda: {
            "backend_url": "https://main-sequence.app",
            "mainsequence_path": "/tmp/mainsequence",
        },
    )
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_session_overrides",
        lambda: {
            "backend_url": "http://127.0.0.1:8000",
            "mainsequence_path": "/tmp/mainsequence-dev",
        },
    )
    result = runner.invoke(cli_mod.app, ["settings"])
    assert result.exit_code == 0
    assert "https://main-sequence.app" in result.output
    assert "/tmp/mainsequence" in result.output
    assert "127.0.0.1:8000" not in result.output


def test_settings_set_base(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(
        cli_mod.cfg,
        "set_mainsequence_path",
        lambda path: {"mainsequence_path": path},
    )
    result = runner.invoke(cli_mod.app, ["settings", "set-base", "/tmp/ms-base"])
    assert result.exit_code == 0
    assert "Projects base folder set to" in result.output


def test_settings_set_base_json(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(
        cli_mod.cfg,
        "set_mainsequence_path",
        lambda path: {"mainsequence_path": path},
    )
    result = runner.invoke(cli_mod.app, ["settings", "set-base", "/tmp/ms-base", "--json"])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["mainsequence_path"] == "/tmp/ms-base"


def test_settings_set_backend(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(
        cli_mod.cfg,
        "set_backend_url",
        lambda url: {"backend_url": url},
    )
    result = runner.invoke(cli_mod.app, ["settings", "set-backend", "https://example.test"])
    assert result.exit_code == 0
    assert "Backend URL set to" in result.output


def test_settings_set_backend_json(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(
        cli_mod.cfg,
        "set_backend_url",
        lambda url: {"backend_url": url},
    )
    result = runner.invoke(
        cli_mod.app, ["settings", "set-backend", "https://example.test", "--json"]
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["backend_url"] == "https://example.test"


def test_settings_reset(cli_mod, runner, monkeypatch):
    captured = {}
    monkeypatch.setattr(
        cli_mod.cfg,
        "DEFAULTS",
        {
            "backend_url": f"{cli_mod.cfg.STANDARD_BACKEND_URL}/",
            "mainsequence_path": "/tmp/mainsequence",
            "version": 1,
        },
    )
    monkeypatch.setattr(
        cli_mod.cfg,
        "set_config",
        lambda updates: captured.update(updates)
        or updates | {"updated_at": "2026-04-20T00:00:00Z"},
    )
    monkeypatch.setattr(
        cli_mod.cfg, "clear_session_overrides", lambda: captured.update(cleared=True)
    )

    result = runner.invoke(cli_mod.app, ["settings", "reset"])
    assert result.exit_code == 0
    assert captured["backend_url"] == cli_mod.cfg.STANDARD_BACKEND_URL
    assert captured["mainsequence_path"].endswith("/tmp/mainsequence")
    assert captured["cleared"] is True
    assert "Settings reset to standard defaults" in result.output


def test_settings_refresh_alias(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(
        cli_mod.cfg,
        "DEFAULTS",
        {
            "backend_url": f"{cli_mod.cfg.STANDARD_BACKEND_URL}/",
            "mainsequence_path": "/tmp/mainsequence",
            "version": 1,
        },
    )
    monkeypatch.setattr(
        cli_mod.cfg,
        "set_config",
        lambda updates: updates | {"updated_at": "2026-04-20T00:00:00Z"},
    )
    monkeypatch.setattr(cli_mod.cfg, "clear_session_overrides", lambda: None)

    result = runner.invoke(cli_mod.app, ["settings", "refresh"])
    assert result.exit_code == 0
    assert "Settings reset to standard defaults" in result.output


def test_config_normalize_backend_url(cli_mod):
    assert cli_mod.cfg.normalize_backend_url("127.0.0.1:800") == "http://127.0.0.1:800"
    assert cli_mod.cfg.normalize_backend_url("localhost:8000") == "http://localhost:8000"
    assert cli_mod.cfg.normalize_backend_url("main-sequence.app") == "https://main-sequence.app"
    assert cli_mod.cfg.normalize_backend_url("https://example.test/") == "https://example.test"


def test_config_normalize_mainsequence_path(cli_mod):
    assert cli_mod.cfg.normalize_mainsequence_path("mainsequence-dev").endswith("/mainsequence-dev")
    assert cli_mod.cfg.normalize_mainsequence_path("~/mainsequence-dev").endswith(
        "/mainsequence-dev"
    )


def test_config_session_overrides_do_not_persist(cli_mod, monkeypatch, tmp_path):
    config_json = tmp_path / "config.json"
    session_json = tmp_path / "session.json"
    cli_mod.cfg.write_json(
        config_json,
        {
            "backend_url": "https://prod.test",
            "mainsequence_path": str(tmp_path / "mainsequence"),
            "version": 1,
        },
    )

    monkeypatch.setattr(cli_mod.cfg, "CONFIG_JSON", config_json)
    monkeypatch.setattr(cli_mod.cfg, "_session_override_path", lambda: session_json)

    cli_mod.cfg.set_session_overrides(
        backend_url="127.0.0.1:8000",
        mainsequence_path="mainsequence-dev",
    )

    effective = cli_mod.cfg.get_config()
    persisted = cli_mod.cfg.read_json(config_json, {})

    assert effective["backend_url"] == "http://127.0.0.1:8000"
    assert effective["mainsequence_path"].endswith("/mainsequence-dev")
    assert persisted["backend_url"] == "https://prod.test"
    assert persisted["mainsequence_path"] == str(tmp_path / "mainsequence")


def test_login_mocked(cli_mod, runner, monkeypatch):
    session_override = {}
    monkeypatch.setattr(
        cli_mod,
        "login_via_browser",
        lambda no_open=False, on_authorize_url=None: {
            "backend": "https://example.test",
            "access": "acc-123",
            "refresh": "ref-456",
        },
    )
    monkeypatch.setattr(
        cli_mod, "get_current_user_profile", lambda: {"username": "user@example.com"}
    )
    monkeypatch.setattr(cli_mod.cfg, "save_tokens", lambda username, access, refresh: True)
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": "/tmp/mainsequence"},
    )
    monkeypatch.setattr(cli_mod.cfg, "auth_persistence_label", lambda: "secure OS storage")
    monkeypatch.setattr(
        cli_mod.cfg,
        "set_session_overrides",
        lambda **kwargs: session_override.update(kwargs) or kwargs,
    )
    monkeypatch.setattr(
        cli_mod,
        "get_projects",
        lambda: (_ for _ in ()).throw(AssertionError("login should not fetch projects")),
    )

    result = runner.invoke(
        cli_mod.app,
        ["login", "--no-status"],
    )
    assert result.exit_code == 0
    assert "MAIN SEQUENCE" in result.output
    assert "__  __" in result.output
    assert "Signed in as user@example.com" in result.output
    assert "Auth tokens are persisted in secure OS storage" in result.output
    assert cli_mod.cfg.STANDARD_BACKEND_URL in result.output
    assert session_override == {
        "backend_url": cli_mod.cfg.STANDARD_BACKEND_URL,
        "mainsequence_path": None,
    }


def test_login_via_mcp_handoff_persists_without_browser(
    cli_mod,
    runner,
    monkeypatch,
):
    handoff_uid = "00000000-0000-4000-8000-000000000001"
    captured: dict[str, object] = {}
    saved: list[tuple[str, str, str]] = []

    def _mcp_login(*, timeout_seconds, on_handoff):
        captured["timeout_seconds"] = timeout_seconds
        on_handoff(
            {
                "mcp_tool": "auth.cli_authorize",
                "mcp_arguments": {"handoff_uid": handoff_uid},
            }
        )
        return {
            "backend": cli_mod.cfg.STANDARD_BACKEND_URL,
            "access": "mcp-access",
            "refresh": "mcp-refresh",
            "user": {"username": "coding-agent@example.com"},
        }

    monkeypatch.setattr(cli_mod, "login_via_mcp_handoff", _mcp_login)
    monkeypatch.setattr(
        cli_mod,
        "login_via_browser",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("browser login must not run")),
    )
    monkeypatch.setattr(
        cli_mod.cfg,
        "save_tokens",
        lambda username, access, refresh: saved.append((username, access, refresh)) or True,
    )
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": "/tmp/mainsequence"},
    )
    monkeypatch.setattr(
        cli_mod.cfg,
        "set_session_overrides",
        lambda **kwargs: captured.update({"session": kwargs}),
    )
    monkeypatch.setattr(
        cli_mod.cfg,
        "auth_persistence_label",
        lambda: "local CLI auth storage",
    )

    result = runner.invoke(
        cli_mod.app,
        ["login", "--mcp", "--mcp-timeout-seconds", "42"],
    )

    assert result.exit_code == 0, result.output
    assert captured["timeout_seconds"] == 42
    assert '"tool":"auth.cli_authorize"' in result.output
    assert handoff_uid in result.output
    assert "mcp-access" not in result.output
    assert "mcp-refresh" not in result.output
    assert saved == [
        ("", "mcp-access", "mcp-refresh"),
        (
            "coding-agent@example.com",
            "mcp-access",
            "mcp-refresh",
        ),
    ]


def test_login_via_mcp_handoff_rejects_export(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(
        cli_mod,
        "login_via_mcp_handoff",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("handoff must not start")),
    )

    result = runner.invoke(cli_mod.app, ["login", "--mcp", "--export"])

    assert result.exit_code == 1
    assert "cannot be combined with --export" in result.output


def test_login_via_mcp_handoff_rejects_runtime_credential_mode(cli_mod, runner, monkeypatch):
    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "runtime_credential")
    monkeypatch.setattr(
        cli_mod,
        "login_via_mcp_handoff",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("handoff must not start")),
    )

    result = runner.invoke(cli_mod.app, ["login", "--mcp"])

    assert result.exit_code == 1
    assert "MAINSEQUENCE_AUTH_MODE=runtime_credential" in result.output
    assert "Run `mainsequence login` instead" in result.output


def test_login_via_mcp_handoff_reports_mcp_failure(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(
        cli_mod,
        "login_via_mcp_handoff",
        lambda **_kwargs: (_ for _ in ()).throw(cli_mod.BrowserAuthError("handoff expired")),
    )

    result = runner.invoke(cli_mod.app, ["login", "--mcp"])

    assert result.exit_code == 1
    assert "MCP handoff login failed: handoff expired" in result.output
    assert "Browser login failed" not in result.output


def test_login_with_backend_override(cli_mod, runner, monkeypatch):
    seen = {}
    session_override = {}
    cleared = {"called": False}

    def _browser_login(no_open=False, on_authorize_url=None):
        seen["backend"] = cli_mod.cfg.backend_url()
        return {
            "backend": seen["backend"],
            "access": "acc-123",
            "refresh": "ref-456",
        }

    monkeypatch.setattr(cli_mod, "login_via_browser", _browser_login)
    monkeypatch.setattr(
        cli_mod, "get_current_user_profile", lambda: {"username": "user@example.com"}
    )
    monkeypatch.setattr(cli_mod.cfg, "save_tokens", lambda username, access, refresh: True)
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {
            "mainsequence_path": "/tmp/mainsequence",
            "backend_url": "https://main-sequence.app",
        },
    )
    monkeypatch.setattr(
        cli_mod.cfg,
        "set_session_overrides",
        lambda **kwargs: session_override.update(kwargs) or kwargs,
    )
    monkeypatch.setattr(cli_mod.cfg, "clear_session_overrides", lambda: cleared.update(called=True))
    monkeypatch.setattr(cli_mod.cfg, "auth_persistence_label", lambda: "secure OS storage")
    monkeypatch.setattr(
        cli_mod,
        "get_projects",
        lambda: (_ for _ in ()).throw(AssertionError("login should not fetch projects")),
    )
    monkeypatch.delenv("MAINSEQUENCE_ENDPOINT", raising=False)

    result = runner.invoke(
        cli_mod.app,
        ["login", "127.0.0.1:800", "mainsequence-dev", "--no-status"],
    )
    assert result.exit_code == 0
    assert seen["backend"] == "http://127.0.0.1:800"
    assert session_override["backend_url"] == "http://127.0.0.1:800"
    assert session_override["mainsequence_path"] == "mainsequence-dev"
    assert cleared["called"] is False
    assert "http://127.0.0.1:800" in result.output
    assert "MAINSEQUENCE_ENDPOINT" not in os.environ


def test_login_with_different_backend_requires_projects_base(cli_mod, runner, monkeypatch):
    called = {"browser": False}

    def _browser_login(no_open=False, on_authorize_url=None):
        called["browser"] = True
        return {"backend": "http://127.0.0.1:8000", "access": "acc-123", "refresh": "ref-456"}

    monkeypatch.setattr(cli_mod, "login_via_browser", _browser_login)
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {
            "mainsequence_path": "/tmp/mainsequence",
            "backend_url": "https://main-sequence.app",
        },
    )
    monkeypatch.delenv("MAINSEQUENCE_ENDPOINT", raising=False)

    result = runner.invoke(
        cli_mod.app,
        ["login", "127.0.0.1:8000", "--no-status"],
    )
    assert result.exit_code == 1
    assert "must also specify a projects base folder" in result.output
    assert called["browser"] is False


def test_login_with_different_backend_allows_current_projects_base(cli_mod, runner, monkeypatch):
    called = {"browser": False}

    def _browser_login(no_open=False, on_authorize_url=None):
        called["browser"] = True
        return {"backend": "http://127.0.0.1:8000", "access": "acc-123", "refresh": "ref-456"}

    monkeypatch.setattr(cli_mod, "login_via_browser", _browser_login)
    monkeypatch.setattr(
        cli_mod, "get_current_user_profile", lambda: {"username": "user@example.com"}
    )
    monkeypatch.setattr(cli_mod.cfg, "save_tokens", lambda username, access, refresh: True)
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {
            "mainsequence_path": "/tmp/mainsequence",
            "backend_url": "https://main-sequence.app",
        },
    )
    monkeypatch.setattr(cli_mod.cfg, "auth_persistence_label", lambda: "local CLI auth storage")
    monkeypatch.setattr(cli_mod.cfg, "set_session_overrides", lambda **kwargs: kwargs)
    monkeypatch.setattr(cli_mod.cfg, "clear_session_overrides", lambda: None)
    monkeypatch.delenv("MAINSEQUENCE_ENDPOINT", raising=False)

    result = runner.invoke(
        cli_mod.app,
        ["login", "127.0.0.1:8000", "/tmp/mainsequence", "--no-status"],
    )
    assert result.exit_code == 0
    assert called["browser"] is True


def test_login_export_env(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(
        cli_mod,
        "login_via_browser",
        lambda no_open=False, on_authorize_url=None: {
            "backend": "https://example.test",
            "access": "acc-123",
            "refresh": "ref-456",
        },
    )
    monkeypatch.setattr(
        cli_mod, "get_current_user_profile", lambda: {"username": "user@example.com"}
    )
    monkeypatch.setattr(cli_mod.cfg, "save_tokens", lambda username, access, refresh: True)
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": "/tmp/mainsequence"},
    )
    monkeypatch.setattr(cli_mod.cfg, "set_session_overrides", lambda **kwargs: kwargs)
    monkeypatch.setattr(
        cli_mod,
        "get_projects",
        lambda: (_ for _ in ()).throw(AssertionError("login should not fetch projects")),
    )

    result = runner.invoke(
        cli_mod.app,
        ["login", "--no-status", "--export"],
    )
    assert result.exit_code == 0
    assert 'export MAINSEQUENCE_ACCESS_TOKEN="acc-123"' in result.output
    assert 'export MAINSEQUENCE_REFRESH_TOKEN="ref-456"' in result.output
    assert 'export MAINSEQUENCE_USERNAME="user@example.com"' in result.output


def test_login_with_jwt_tokens(cli_mod, runner, monkeypatch):
    saved = {}
    session_override = {}

    monkeypatch.setattr(
        cli_mod.cfg,
        "save_tokens",
        lambda username, access, refresh: saved.update(
            {"username": username, "access": access, "refresh": refresh}
        )
        or True,
    )
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": "/tmp/mainsequence"},
    )
    monkeypatch.setattr(cli_mod.cfg, "auth_persistence_label", lambda: "local CLI auth storage")
    monkeypatch.setattr(
        cli_mod.cfg,
        "set_session_overrides",
        lambda **kwargs: session_override.update(kwargs) or kwargs,
    )
    monkeypatch.setattr(
        cli_mod,
        "get_projects",
        lambda: (_ for _ in ()).throw(AssertionError("login should not fetch projects")),
    )

    result = runner.invoke(
        cli_mod.app,
        [
            "login",
            "--access-token",
            "acc-123",
            "--refresh-token",
            "ref-456",
            "--no-status",
        ],
    )
    assert result.exit_code == 0
    assert saved == {"username": "", "access": "acc-123", "refresh": "ref-456"}
    assert "Signed in with JWT tokens" in result.output
    assert "Auth tokens are persisted in local CLI auth storage" in result.output
    assert session_override == {
        "backend_url": cli_mod.cfg.STANDARD_BACKEND_URL,
        "mainsequence_path": None,
    }


def test_login_runtime_credential_exchanges_token(cli_mod, runner, monkeypatch):
    session_override = {}
    exchange = {"called": False}
    saved = {}

    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "runtime_credential")

    def _exchange_runtime_credential_for_cli_login(backend_url):
        exchange["called"] = True
        exchange["backend_url"] = backend_url
        os.environ["MAINSEQUENCE_ACCESS_TOKEN"] = "runtime-access"
        return "runtime-access"

    monkeypatch.setattr(
        cli_mod,
        "_exchange_runtime_credential_for_cli_login",
        _exchange_runtime_credential_for_cli_login,
    )
    monkeypatch.setattr(
        cli_mod, "login_via_browser", lambda **kwargs: (_ for _ in ()).throw(AssertionError)
    )
    monkeypatch.setattr(
        cli_mod.cfg,
        "save_tokens",
        lambda username, access, refresh: saved.update(
            username=username, access=access, refresh=refresh
        )
        or True,
    )
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": "/tmp/mainsequence"},
    )
    monkeypatch.setattr(cli_mod.cfg, "auth_persistence_label", lambda: "local CLI auth storage")
    monkeypatch.setattr(
        cli_mod.cfg,
        "set_session_overrides",
        lambda **kwargs: session_override.update(kwargs) or kwargs,
    )

    result = runner.invoke(cli_mod.app, ["login"])

    assert result.exit_code == 0
    assert exchange["called"] is True
    assert exchange["backend_url"]
    assert saved == {"username": "", "access": "runtime-access", "refresh": ""}
    assert os.environ["MAINSEQUENCE_ACCESS_TOKEN"] == "runtime-access"
    assert "Signed in with runtime credential" in result.output
    assert "no CLI JWT refresh token exists" in result.output
    assert "re-exchange the runtime credential automatically" in result.output
    assert session_override == {
        "backend_url": cli_mod.cfg.STANDARD_BACKEND_URL,
        "mainsequence_path": None,
    }


def test_login_runtime_credential_export(monkeypatch, cli_mod, runner):
    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "runtime_credential")
    monkeypatch.setattr(
        cli_mod,
        "_exchange_runtime_credential_for_cli_login",
        lambda backend_url: "runtime-access",
    )
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": "/tmp/mainsequence"},
    )
    monkeypatch.setattr(cli_mod.cfg, "set_session_overrides", lambda **kwargs: kwargs)

    result = runner.invoke(cli_mod.app, ["login", "--export"])

    assert result.exit_code == 0
    assert 'export MAINSEQUENCE_AUTH_MODE="runtime_credential"' in result.output
    assert 'export MAINSEQUENCE_ACCESS_TOKEN="runtime-access"' in result.output
    assert "MAINSEQUENCE_REFRESH_TOKEN" not in result.output


def test_login_runtime_credential_uses_backend_override(cli_mod, runner, monkeypatch):
    seen = {}
    session_override = {}

    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "runtime_credential")
    monkeypatch.setattr(
        cli_mod,
        "_exchange_runtime_credential_for_cli_login",
        lambda backend_url: seen.update(backend_url=backend_url) or "runtime-access",
    )
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {
            "mainsequence_path": "/tmp/mainsequence",
            "backend_url": "https://main-sequence.app",
        },
    )
    monkeypatch.setattr(
        cli_mod.cfg,
        "set_session_overrides",
        lambda **kwargs: session_override.update(kwargs) or kwargs,
    )
    monkeypatch.setattr(cli_mod.cfg, "auth_persistence_label", lambda: "local CLI auth storage")
    monkeypatch.delenv("MAINSEQUENCE_ENDPOINT", raising=False)

    result = runner.invoke(
        cli_mod.app,
        [
            "login",
            "--backend",
            "http://127.0.0.1:8000",
            "--projects-base",
            "mainsequence-dev",
        ],
    )

    assert result.exit_code == 0
    assert seen["backend_url"] == "http://127.0.0.1:8000"
    assert session_override == {
        "backend_url": "http://127.0.0.1:8000",
        "mainsequence_path": "mainsequence-dev",
    }
    assert "MAINSEQUENCE_ENDPOINT" not in os.environ


def test_login_runtime_credential_rejects_manual_jwt(cli_mod, runner, monkeypatch):
    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "runtime_credential")

    result = runner.invoke(
        cli_mod.app,
        ["login", "--access-token", "acc-123", "--refresh-token", "ref-456"],
    )

    assert result.exit_code == 1
    assert "Runtime credential login cannot be combined" in result.output


def test_api_refresh_access_runtime_credential_reexchange(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "runtime_credential")
    monkeypatch.setattr(api_mod, "backend_url", lambda: "http://127.0.0.1:8000")
    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"username": "", "access": "", "refresh": ""}
    )
    monkeypatch.delenv("MAINSEQUENCE_ACCESS_TOKEN", raising=False)

    saved = {}

    def _save_tokens(username, access, refresh):
        saved["username"] = username
        saved["access"] = access
        saved["refresh"] = refresh
        return True

    monkeypatch.setattr(api_mod, "save_tokens", _save_tokens)

    fake_utils = types.ModuleType("mainsequence.client.utils")

    class _Provider:
        def __init__(self, token_url):
            saved["token_url"] = token_url

        def refresh(self, force=False):
            saved["force"] = force
            os.environ["MAINSEQUENCE_ACCESS_TOKEN"] = "runtime-new-access"

    fake_utils.RuntimeCredentialAuthProvider = _Provider
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)

    out = api_mod.refresh_access()
    assert out == "runtime-new-access"
    assert saved["token_url"] == "http://127.0.0.1:8000/api/v1/runtime-credentials/token/"
    assert saved["force"] is True
    assert saved["access"] == "runtime-new-access"
    assert saved["refresh"] == ""


def test_api_logout_cli_session_revokes_tracked_cli_refresh(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")

    class _Response:
        status_code = 200
        text = ""

        @staticmethod
        def json():
            return {"detail": "CLI refresh token revoked."}

    class _Session:
        @staticmethod
        def post(url, data=None, headers=None):
            assert url == "http://127.0.0.1:8000/auth/cli/revoke/"
            assert json.loads(data) == {"refresh": "ref-123"}
            assert headers is None
            return _Response()

    monkeypatch.setattr(api_mod, "backend_url", lambda: "http://127.0.0.1:8000")
    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"username": "", "access": "acc-123", "refresh": "ref-123"}
    )
    monkeypatch.setattr(api_mod, "S", _Session())

    out = api_mod.logout_cli_session()
    assert out == {
        "attempted": True,
        "revoked": True,
        "method": "cli_revoke",
        "detail": "CLI refresh token revoked.",
    }


def test_api_logout_cli_session_falls_back_to_jwt_logout_on_404(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")

    class _Response:
        status_code = 404
        text = ""

        @staticmethod
        def json():
            return {"detail": "Not found."}

    class _Session:
        @staticmethod
        def post(url, data=None, headers=None):
            assert url == "http://127.0.0.1:8000/auth/cli/revoke/"
            assert json.loads(data) == {"refresh": "ref-123"}
            return _Response()

    monkeypatch.setattr(api_mod, "backend_url", lambda: "http://127.0.0.1:8000")
    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"username": "", "access": "acc-123", "refresh": "ref-123"}
    )
    monkeypatch.setattr(api_mod, "logout_jwt_session", lambda: True)
    monkeypatch.setattr(api_mod, "S", _Session())

    out = api_mod.logout_cli_session()
    assert out == {
        "attempted": True,
        "revoked": True,
        "method": "jwt_logout_fallback",
        "detail": "CLI revoke endpoint unavailable; used JWT logout fallback.",
    }


def test_api_logout_cli_session_skips_backend_revoke_without_refresh(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"username": "", "access": "acc-123", "refresh": ""}
    )

    out = api_mod.logout_cli_session()
    assert out == {
        "attempted": False,
        "revoked": False,
        "method": "local_only",
        "detail": "No CLI browser-login refresh token available.",
    }


def test_login_with_jwt_tokens_and_backend_override(cli_mod, runner, monkeypatch):
    session_override = {}
    monkeypatch.setattr(cli_mod.cfg, "save_tokens", lambda username, access, refresh: True)
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {
            "mainsequence_path": "/tmp/mainsequence",
            "backend_url": "https://main-sequence.app",
        },
    )
    monkeypatch.setattr(
        cli_mod.cfg,
        "set_session_overrides",
        lambda **kwargs: session_override.update(kwargs) or kwargs,
    )
    monkeypatch.setattr(cli_mod.cfg, "auth_persistence_label", lambda: "local CLI auth storage")
    monkeypatch.setattr(
        cli_mod,
        "get_projects",
        lambda: (_ for _ in ()).throw(AssertionError("login should not fetch projects")),
    )
    monkeypatch.delenv("MAINSEQUENCE_ENDPOINT", raising=False)

    result = runner.invoke(
        cli_mod.app,
        [
            "login",
            "--access-token",
            "acc-123",
            "--refresh-token",
            "ref-456",
            "--backend",
            "http://127.0.0.1:80",
            "--projects-base",
            "mainsequence-dev",
            "--no-status",
        ],
    )
    assert result.exit_code == 0
    assert session_override == {
        "backend_url": "http://127.0.0.1:80",
        "mainsequence_path": "mainsequence-dev",
    }
    assert "http://127.0.0.1:80" in result.output
    assert "MAINSEQUENCE_ENDPOINT" not in os.environ


def test_login_with_jwt_tokens_and_different_backend_requires_projects_base(
    cli_mod, runner, monkeypatch
):
    called = {"save_tokens": False}

    def _save_tokens(username, access, refresh):
        called["save_tokens"] = True
        return True

    monkeypatch.setattr(cli_mod.cfg, "save_tokens", _save_tokens)
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {
            "mainsequence_path": "/tmp/mainsequence",
            "backend_url": "https://main-sequence.app",
        },
    )
    monkeypatch.delenv("MAINSEQUENCE_ENDPOINT", raising=False)

    result = runner.invoke(
        cli_mod.app,
        [
            "login",
            "--access-token",
            "acc-123",
            "--refresh-token",
            "ref-456",
            "--backend",
            "127.0.0.1:8000",
            "--no-status",
        ],
    )
    assert result.exit_code == 1
    assert "must also specify a projects base folder" in result.output
    assert called["save_tokens"] is False


def test_login_export_env_with_jwt_tokens_omits_username(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod.cfg, "save_tokens", lambda username, access, refresh: True)
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": "/tmp/mainsequence"},
    )
    monkeypatch.setattr(cli_mod.cfg, "set_session_overrides", lambda **kwargs: kwargs)

    result = runner.invoke(
        cli_mod.app,
        [
            "login",
            "--access-token",
            "acc-123",
            "--refresh-token",
            "ref-456",
            "--export",
            "--no-status",
        ],
    )
    assert result.exit_code == 0
    assert 'export MAINSEQUENCE_ACCESS_TOKEN="acc-123"' in result.output
    assert 'export MAINSEQUENCE_REFRESH_TOKEN="ref-456"' in result.output
    assert "MAINSEQUENCE_USERNAME" not in result.output


def test_login_warns_when_secure_persist_fails(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(
        cli_mod,
        "login_via_browser",
        lambda no_open=False, on_authorize_url=None: {
            "backend": "https://example.test",
            "access": "acc-123",
            "refresh": "ref-456",
        },
    )
    monkeypatch.setattr(cli_mod, "get_current_user_profile", lambda: {})
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": "/tmp/mainsequence"},
    )
    monkeypatch.setattr(cli_mod.cfg, "save_tokens", lambda username, access, refresh: False)
    monkeypatch.setattr(cli_mod.cfg, "auth_persistence_label", lambda: "secure OS storage")
    monkeypatch.setattr(cli_mod.cfg, "set_session_overrides", lambda **kwargs: kwargs)
    monkeypatch.setattr(
        cli_mod,
        "get_projects",
        lambda: (_ for _ in ()).throw(AssertionError("login should not fetch projects")),
    )

    result = runner.invoke(
        cli_mod.app,
        ["login", "--no-status"],
    )
    assert result.exit_code == 0
    assert "Could not persist auth tokens in secure OS storage" in result.output


def test_login_does_not_fetch_projects_after_success(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(
        cli_mod,
        "login_via_browser",
        lambda no_open=False, on_authorize_url=None: {
            "backend": "https://example.test",
            "access": "acc-123",
            "refresh": "ref-456",
        },
    )
    monkeypatch.setattr(
        cli_mod, "get_current_user_profile", lambda: {"username": "user@example.com"}
    )
    monkeypatch.setattr(cli_mod.cfg, "save_tokens", lambda username, access, refresh: True)
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": "/tmp/mainsequence"},
    )
    monkeypatch.setattr(cli_mod.cfg, "auth_persistence_label", lambda: "local CLI auth storage")
    monkeypatch.setattr(cli_mod.cfg, "set_session_overrides", lambda **kwargs: kwargs)
    monkeypatch.setattr(
        cli_mod,
        "get_projects",
        lambda: (_ for _ in ()).throw(AssertionError("login should not fetch projects")),
    )

    result = runner.invoke(
        cli_mod.app,
        ["login"],
    )
    assert result.exit_code == 0
    assert "Signed in as user@example.com" in result.output
    assert "Projects:" not in result.output


def test_jwt_login_does_not_fetch_projects_after_success(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod.cfg, "save_tokens", lambda username, access, refresh: True)
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": "/tmp/mainsequence"},
    )
    monkeypatch.setattr(cli_mod.cfg, "auth_persistence_label", lambda: "local CLI auth storage")
    monkeypatch.setattr(cli_mod.cfg, "set_session_overrides", lambda **kwargs: kwargs)
    monkeypatch.setattr(
        cli_mod,
        "get_projects",
        lambda: (_ for _ in ()).throw(AssertionError("login should not fetch projects")),
    )

    result = runner.invoke(
        cli_mod.app,
        ["login", "--access-token", "acc-123", "--refresh-token", "ref-456"],
    )
    assert result.exit_code == 0
    assert "Signed in with JWT tokens" in result.output
    assert "Projects:" not in result.output


def test_logout(cli_mod, runner, monkeypatch):
    cleared = {"called": False}
    monkeypatch.setattr(
        cli_mod,
        "logout_cli_session",
        lambda: {
            "attempted": True,
            "revoked": True,
            "method": "cli_revoke",
            "detail": "CLI refresh token revoked.",
        },
    )
    monkeypatch.setattr(cli_mod.cfg, "clear_tokens", lambda: True)
    monkeypatch.setattr(cli_mod.cfg, "clear_session_overrides", lambda: cleared.update(called=True))
    result = runner.invoke(cli_mod.app, ["logout"])
    assert result.exit_code == 0
    assert "backend session revoked" in result.output
    assert cleared["called"] is True


def test_logout_export_env(cli_mod, runner, monkeypatch):
    cleared = {"called": False}
    monkeypatch.setattr(
        cli_mod,
        "logout_cli_session",
        lambda: {
            "attempted": True,
            "revoked": True,
            "method": "cli_revoke",
            "detail": "CLI refresh token revoked.",
        },
    )
    monkeypatch.setattr(cli_mod.cfg, "clear_tokens", lambda: True)
    monkeypatch.setattr(cli_mod.cfg, "clear_session_overrides", lambda: cleared.update(called=True))
    result = runner.invoke(cli_mod.app, ["logout", "--export"])
    assert result.exit_code == 0
    assert "unset MAINSEQUENCE_ACCESS_TOKEN" in result.output
    assert "unset MAINSEQUENCE_REFRESH_TOKEN" in result.output
    assert "unset MAINSEQUENCE_USERNAME" in result.output
    assert cleared["called"] is True


def test_logout_warns_when_backend_revoke_cannot_be_confirmed(cli_mod, runner, monkeypatch):
    cleared = {"called": False}
    monkeypatch.setattr(
        cli_mod,
        "logout_cli_session",
        lambda: {
            "attempted": True,
            "revoked": False,
            "method": "error",
            "detail": "CLI revoke failed with status 400.",
        },
    )
    monkeypatch.setattr(cli_mod.cfg, "clear_tokens", lambda: True)
    monkeypatch.setattr(cli_mod.cfg, "clear_session_overrides", lambda: cleared.update(called=True))
    result = runner.invoke(cli_mod.app, ["logout"])
    assert result.exit_code == 0
    assert "Signed out locally, but backend session revoke could not be confirmed." in result.output
    assert "CLI revoke failed with status 400." in result.output
    assert cleared["called"] is True


def test_login_rejects_legacy_email_argument(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": "/tmp/mainsequence"},
    )
    result = runner.invoke(cli_mod.app, ["login", "user@example.com"])
    assert result.exit_code == 1
    assert "Email/password CLI login was removed" in result.output


def test_login_no_open_prints_authorize_url(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": "/tmp/mainsequence"},
    )
    monkeypatch.setattr(cli_mod.cfg, "auth_persistence_label", lambda: "local CLI auth storage")
    monkeypatch.setattr(cli_mod.cfg, "set_session_overrides", lambda **kwargs: kwargs)
    monkeypatch.setattr(cli_mod.cfg, "save_tokens", lambda username, access, refresh: True)
    monkeypatch.setattr(
        cli_mod, "get_current_user_profile", lambda: {"username": "user@example.com"}
    )

    def _browser_login(no_open=False, on_authorize_url=None):
        assert no_open is True
        assert on_authorize_url is not None
        on_authorize_url("https://example.test/auth")
        return {"backend": "https://example.test", "access": "acc-123", "refresh": "ref-456"}

    monkeypatch.setattr(cli_mod, "login_via_browser", _browser_login)

    result = runner.invoke(cli_mod.app, ["login", "--no-open", "--no-status"])
    assert result.exit_code == 0
    assert "Open this URL to authenticate: https://example.test/auth" in result.output


def test_config_get_tokens_fallback_secure_store(cli_mod, monkeypatch):
    monkeypatch.delenv(cli_mod.cfg.ENV_ACCESS, raising=False)
    monkeypatch.delenv(cli_mod.cfg.ENV_REFRESH, raising=False)
    monkeypatch.delenv(cli_mod.cfg.ENV_USERNAME, raising=False)
    monkeypatch.setattr(cli_mod.cfg, "_read_local_tokens", lambda: {})
    monkeypatch.setattr(
        cli_mod.cfg,
        "_read_secure_tokens",
        lambda: {"username": "u@example.com", "access": "acc", "refresh": "ref"},
    )
    out = cli_mod.cfg.get_tokens()
    assert out["username"] == "u@example.com"
    assert out["access"] == "acc"
    assert out["refresh"] == "ref"


def test_config_get_tokens_fallback_legacy_env(cli_mod, monkeypatch):
    monkeypatch.delenv(cli_mod.cfg.ENV_ACCESS, raising=False)
    monkeypatch.delenv(cli_mod.cfg.ENV_REFRESH, raising=False)
    monkeypatch.delenv(cli_mod.cfg.ENV_USERNAME, raising=False)
    monkeypatch.setenv(cli_mod.cfg.LEGACY_ENV_ACCESS, "legacy-acc")
    monkeypatch.setenv(cli_mod.cfg.LEGACY_ENV_REFRESH, "legacy-ref")
    monkeypatch.setenv(cli_mod.cfg.LEGACY_ENV_USERNAME, "legacy@example.com")
    out = cli_mod.cfg.get_tokens()
    assert out["username"] == "legacy@example.com"
    assert out["access"] == "legacy-acc"
    assert out["refresh"] == "legacy-ref"


def test_config_get_tokens_fallback_local_store(cli_mod, monkeypatch, tmp_path):
    auth_json = tmp_path / "auth.json"
    cli_mod.cfg.write_json(
        auth_json,
        {"username": "u@example.com", "access": "acc", "refresh": "ref"},
    )
    monkeypatch.setattr(cli_mod.cfg, "AUTH_JSON", auth_json)
    monkeypatch.delenv(cli_mod.cfg.ENV_ACCESS, raising=False)
    monkeypatch.delenv(cli_mod.cfg.ENV_REFRESH, raising=False)
    monkeypatch.delenv(cli_mod.cfg.ENV_USERNAME, raising=False)
    monkeypatch.setattr(cli_mod.cfg, "_read_secure_tokens", lambda: {})

    out = cli_mod.cfg.get_tokens()
    assert out["username"] == "u@example.com"
    assert out["access"] == "acc"
    assert out["refresh"] == "ref"


def test_config_get_tokens_fallback_backend_scoped_local_store(cli_mod, monkeypatch, tmp_path):
    auth_json = tmp_path / "auth.json"
    cli_mod.cfg.write_json(
        auth_json,
        {
            "version": 2,
            "by_backend": {
                "https://api.main-sequence.app": {
                    "username": "prod@example.com",
                    "access": "prod-acc",
                    "refresh": "prod-ref",
                },
                "http://127.0.0.1:8000": {
                    "username": "dev@example.com",
                    "access": "dev-acc",
                    "refresh": "dev-ref",
                },
            },
        },
    )
    monkeypatch.setattr(cli_mod.cfg, "AUTH_JSON", auth_json)
    monkeypatch.setattr(cli_mod.cfg, "backend_url", lambda: "http://127.0.0.1:8000")
    monkeypatch.delenv(cli_mod.cfg.ENV_ACCESS, raising=False)
    monkeypatch.delenv(cli_mod.cfg.ENV_REFRESH, raising=False)
    monkeypatch.delenv(cli_mod.cfg.ENV_USERNAME, raising=False)
    monkeypatch.setattr(cli_mod.cfg, "_read_secure_tokens", lambda: {})

    out = cli_mod.cfg.get_tokens()
    assert out["username"] == "dev@example.com"
    assert out["access"] == "dev-acc"
    assert out["refresh"] == "dev-ref"


def test_config_get_tokens_runtime_mode_allows_access_without_refresh(
    cli_mod, monkeypatch, tmp_path
):
    auth_json = tmp_path / "auth.json"
    cli_mod.cfg.write_json(
        auth_json,
        {"username": "u@example.com", "access": "acc", "refresh": ""},
    )
    monkeypatch.setattr(cli_mod.cfg, "AUTH_JSON", auth_json)
    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "runtime_credential")
    monkeypatch.delenv(cli_mod.cfg.ENV_ACCESS, raising=False)
    monkeypatch.delenv(cli_mod.cfg.ENV_REFRESH, raising=False)
    monkeypatch.delenv(cli_mod.cfg.ENV_USERNAME, raising=False)
    monkeypatch.setattr(cli_mod.cfg, "_read_secure_tokens", lambda: {})

    out = cli_mod.cfg.get_tokens()
    assert out["username"] == "u@example.com"
    assert out["access"] == "acc"
    assert out["refresh"] == ""


def test_config_get_tokens_prefers_env_over_local_store(cli_mod, monkeypatch, tmp_path):
    auth_json = tmp_path / "auth.json"
    cli_mod.cfg.write_json(
        auth_json,
        {"username": "file@example.com", "access": "file-acc", "refresh": "file-ref"},
    )
    monkeypatch.setattr(cli_mod.cfg, "AUTH_JSON", auth_json)
    monkeypatch.setattr(cli_mod.cfg, "_read_secure_tokens", lambda: {})
    monkeypatch.setenv(cli_mod.cfg.ENV_USERNAME, "env@example.com")
    monkeypatch.setenv(cli_mod.cfg.ENV_ACCESS, "env-acc")
    monkeypatch.setenv(cli_mod.cfg.ENV_REFRESH, "env-ref")

    out = cli_mod.cfg.get_tokens()
    assert out["username"] == "env@example.com"
    assert out["access"] == "env-acc"
    assert out["refresh"] == "env-ref"


def test_prime_runtime_env_reads_endpoint_but_ignores_retired_project_identity(
    cli_mod, monkeypatch, tmp_path
):
    bootstrap = importlib.import_module("mainsequence.bootstrap")
    project_dir = tmp_path / "project"
    project_dir.mkdir(parents=True, exist_ok=True)
    (project_dir / ".env").write_text(
        "MAINSEQUENCE_ENDPOINT=https://project-backend.test\n"
        "MAIN_SEQUENCE_PROJECT_UID=project-uid-123\n",
        encoding="utf-8",
    )

    monkeypatch.chdir(project_dir)
    monkeypatch.setattr(cli_mod.cfg, "backend_url", lambda: "https://session-backend.test")
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_tokens",
        lambda: {"username": "user@example.com", "access": "acc-123", "refresh": "ref-456"},
    )
    for key in (
        "MAINSEQUENCE_ENDPOINT",
        "MAIN_SEQUENCE_PROJECT_UID",
        "MAIN_SEQUENCE_PROJECT_ID",
        "MAINSEQUENCE_ACCESS_TOKEN",
        "MAINSEQUENCE_REFRESH_TOKEN",
    ):
        monkeypatch.delenv(key, raising=False)

    bootstrap.prime_runtime_env()

    assert os.environ["MAINSEQUENCE_ENDPOINT"] == "https://project-backend.test"
    assert "MAIN_SEQUENCE_PROJECT_UID" not in os.environ
    assert "MAIN_SEQUENCE_PROJECT_ID" not in os.environ
    assert os.environ["MAINSEQUENCE_ACCESS_TOKEN"] == "acc-123"
    assert os.environ["MAINSEQUENCE_REFRESH_TOKEN"] == "ref-456"


def test_prime_runtime_env_falls_back_to_cli_login_context(cli_mod, monkeypatch, tmp_path):
    bootstrap = importlib.import_module("mainsequence.bootstrap")
    project_dir = tmp_path / "project"
    project_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.chdir(project_dir)
    monkeypatch.setattr(cli_mod.cfg, "backend_url", lambda: "http://127.0.0.1:8000")
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_tokens",
        lambda: {"username": "user@example.com", "access": "acc-123", "refresh": "ref-456"},
    )
    for key in (
        "MAINSEQUENCE_ENDPOINT",
        "MAIN_SEQUENCE_PROJECT_UID",
        "MAIN_SEQUENCE_PROJECT_ID",
        "MAINSEQUENCE_ACCESS_TOKEN",
        "MAINSEQUENCE_REFRESH_TOKEN",
    ):
        monkeypatch.delenv(key, raising=False)

    bootstrap.prime_runtime_env()

    assert os.environ["MAINSEQUENCE_ENDPOINT"] == "http://127.0.0.1:8000"
    assert "MAIN_SEQUENCE_PROJECT_UID" not in os.environ
    assert "MAIN_SEQUENCE_PROJECT_ID" not in os.environ
    assert os.environ["MAINSEQUENCE_ACCESS_TOKEN"] == "acc-123"
    assert os.environ["MAINSEQUENCE_REFRESH_TOKEN"] == "ref-456"


def test_config_save_tokens_writes_secure_store(cli_mod, monkeypatch):
    captured = {}

    def _write_secure_tokens(*, username, access, refresh):
        captured["username"] = username
        captured["access"] = access
        captured["refresh"] = refresh
        return True

    monkeypatch.setattr(cli_mod.cfg, "_write_secure_tokens", _write_secure_tokens)
    monkeypatch.setattr(cli_mod.cfg, "_macos_security_exists", lambda: True)
    cli_mod.cfg.save_tokens("u@example.com", "acc", "ref")
    assert captured == {"username": "u@example.com", "access": "acc", "refresh": "ref"}


def test_config_save_tokens_writes_local_store_when_secure_store_unavailable(
    cli_mod, monkeypatch, tmp_path
):
    auth_json = tmp_path / "auth.json"
    monkeypatch.setattr(cli_mod.cfg, "AUTH_JSON", auth_json)
    monkeypatch.setattr(cli_mod.cfg, "_macos_security_exists", lambda: False)
    monkeypatch.setattr(cli_mod.cfg, "backend_url", lambda: "https://api.main-sequence.app")

    ok = cli_mod.cfg.save_tokens("u@example.com", "acc", "ref")

    assert ok is True
    assert cli_mod.cfg.read_json(auth_json, {}) == {
        "version": 2,
        "by_backend": {
            "https://api.main-sequence.app": {
                "username": "u@example.com",
                "access": "acc",
                "refresh": "ref",
            }
        },
    }


def test_config_save_tokens_falls_back_to_local_store_when_secure_readback_fails(
    cli_mod, monkeypatch, tmp_path
):
    auth_json = tmp_path / "auth.json"
    monkeypatch.setattr(cli_mod.cfg, "AUTH_JSON", auth_json)
    monkeypatch.setattr(cli_mod.cfg, "_macos_security_exists", lambda: True)
    monkeypatch.setattr(cli_mod.cfg, "backend_url", lambda: "http://127.0.0.1:8000")
    monkeypatch.setattr(cli_mod.cfg, "_write_secure_tokens", lambda **kwargs: True)
    monkeypatch.setattr(cli_mod.cfg, "_read_secure_tokens", lambda: {})

    ok = cli_mod.cfg.save_tokens("dev@example.com", "dev-acc", "dev-ref")

    assert ok is True
    assert cli_mod.cfg.read_json(auth_json, {}) == {
        "version": 2,
        "by_backend": {
            "http://127.0.0.1:8000": {
                "username": "dev@example.com",
                "access": "dev-acc",
                "refresh": "dev-ref",
            }
        },
    }


def test_config_clear_tokens_removes_local_store(cli_mod, monkeypatch, tmp_path):
    auth_json = tmp_path / "auth.json"
    cli_mod.cfg.write_json(
        auth_json,
        {"username": "u@example.com", "access": "acc", "refresh": "ref"},
    )
    monkeypatch.setattr(cli_mod.cfg, "AUTH_JSON", auth_json)
    monkeypatch.setattr(cli_mod.cfg, "_clear_secure_tokens", lambda: True)
    monkeypatch.setenv(cli_mod.cfg.ENV_USERNAME, "u@example.com")
    monkeypatch.setenv(cli_mod.cfg.ENV_ACCESS, "acc")
    monkeypatch.setenv(cli_mod.cfg.ENV_REFRESH, "ref")

    ok = cli_mod.cfg.clear_tokens()

    assert ok is True
    assert not auth_json.exists()
    assert os.environ.get(cli_mod.cfg.ENV_USERNAME) is None
    assert os.environ.get(cli_mod.cfg.ENV_ACCESS) is None
    assert os.environ.get(cli_mod.cfg.ENV_REFRESH) is None


def test_get_current_user_profile_uses_user_details_endpoint(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    seen = {}

    class _Response:
        def __init__(self, payload):
            self.ok = True
            self._payload = payload

        def json(self):
            return self._payload

    def _authed(method, path, body=None):
        seen["method"] = method
        seen["path"] = path
        return _Response(
            {
                "user": {
                    "uid": "user-uid-123",
                    "username": "jose@main-sequence.io",
                    "organization": {"uid": "org-uid-456", "name": "Main Sequence"},
                }
            }
        )

    monkeypatch.setattr(api_mod, "authed", _authed)

    out = api_mod.get_current_user_profile()
    assert seen == {"method": "GET", "path": "/api/v1/users/me/"}
    assert out == {"username": "jose@main-sequence.io", "organization": "Main Sequence"}


def test_get_current_user_profile_accepts_top_level_organization_object(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")

    class _Response:
        ok = True

        @staticmethod
        def json():
            return {
                "username": "jose@main-sequence.io",
                "organization": {"uid": "org-uid-456", "name": "Main Sequence Dev"},
            }

    monkeypatch.setattr(api_mod, "authed", lambda method, path, body=None: _Response())

    out = api_mod.get_current_user_profile()
    assert out == {"username": "jose@main-sequence.io", "organization": "Main Sequence Dev"}


def test_add_deploy_key_uses_project_route(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    class _Response:
        @staticmethod
        def raise_for_status():
            return None

    def _authed(method, path, body=None):
        captured.update(method=method, path=path, body=body)
        return _Response()

    monkeypatch.setattr(api_mod, "resolve_project_uid", lambda project_ref: "project-uid-123")
    monkeypatch.setattr(api_mod, "authed", _authed)

    api_mod.add_deploy_key("project-uid-123", "workstation", "ssh-ed25519 AAA test")

    assert captured == {
        "method": "POST",
        "path": "/api/v1/projects/project-uid-123/add-deploy-key/",
        "body": {"key_title": "workstation", "public_key": "ssh-ed25519 AAA test"},
    }


def test_render_project_branch_default_redeployment_tag_uses_backend_contract(
    cli_mod,
    monkeypatch,
):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    class _Response:
        ok = True
        status_code = 200

        @staticmethod
        def json():
            return {"version": "1.2.3", "tag_name": "v1.2.3-dev.1"}

    def _authed(method, path, body=None):
        captured.update(method=method, path=path, body=body)
        return _Response()

    monkeypatch.setattr(api_mod, "authed", _authed)

    tag_name = api_mod.render_project_branch_default_redeployment_tag(
        "project-branch-uid-123",
        version="1.2.3",
    )

    assert tag_name == "v1.2.3-dev.1"
    assert captured == {
        "method": "POST",
        "path": ("/api/v1/project-branches/project-branch-uid-123/default-redeployment-tag/"),
        "body": {"version": "1.2.3"},
    }


def test_org_slug_from_profile_handles_organization_object(cli_mod, monkeypatch):
    monkeypatch.setattr(
        cli_mod,
        "get_current_user_profile",
        lambda: {"organization": {"uid": "org-uid-456", "name": "Main Sequence Dev"}},
    )

    out = cli_mod._org_slug_from_profile()
    assert out == "main-sequence-dev"


def test_doctor_command(cli_mod, runner, monkeypatch):
    called = {"value": False}

    def _run():
        called["value"] = True

    monkeypatch.setattr(cli_mod, "run_doctor", _run)
    result = runner.invoke(cli_mod.app, ["doctor"])
    assert result.exit_code == 0
    assert called["value"] is True


def test_copy_llm_instructions_print(cli_mod, runner, tmp_path):
    docs_dir = tmp_path / "examples" / "ai" / "instructions"
    docs_dir.mkdir(parents=True, exist_ok=True)
    (docs_dir / "01.md").write_text("# test\nhello\n", encoding="utf-8")
    result = runner.invoke(
        cli_mod.app,
        ["copy-llm-instructions", "--dir", str(docs_dir), "--print"],
    )
    assert result.exit_code == 0
    assert "AI Instructions" in result.output
    assert "hello" in result.output


def test_sdk_latest(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "fetch_latest_sdk_version", lambda: "v1.2.3")
    result = runner.invoke(cli_mod.app, ["sdk", "latest"])
    assert result.exit_code == 0
    assert "Latest SDK (GitHub): v1.2.3" in result.output


def test_sdk_latest_json(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "fetch_latest_sdk_version", lambda: "v1.2.3")
    result = runner.invoke(cli_mod.app, ["sdk", "latest", "--json"])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["latest"] == "v1.2.3"


def test_project_list(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": "/tmp/mainsequence"},
    )
    monkeypatch.setattr(cli_mod, "_org_slug_from_profile", lambda: "org")
    monkeypatch.setattr(
        cli_mod,
        "get_projects",
        lambda: [
            {
                "uid": "project-uid-1",
                "project_name": "Demo",
                "is_initialized": True,
            }
        ],
    )
    result = runner.invoke(cli_mod.app, ["project", "list"])
    assert result.exit_code == 0
    assert "UID" in result.output
    assert "project-uid-1" in result.output
    assert "Branches" in result.output
    assert "Local" not in result.output
    assert "Demo" in result.output
    assert "Data Source" not in result.output
    assert "Class" not in result.output


def test_project_get_data_node_updates(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "get_project_data_node_updates",
        lambda project_branch_uid, timeout=None: [
            {
                "uid": "data-node-update-uid-10",
                "update_hash": "abc123",
                "data_node_storage": {
                    "uid": "meta-table-uid-42",
                    "physical_table_name": "storage-xyz",
                },
                "update_details": {"related_table_uid": "data-node-update-uid-10"},
            }
        ],
    )
    monkeypatch.setattr(
        cli_mod,
        "_resolve_project_branch_uid_for_command",
        lambda *args, **kwargs: "project-branch-uid-123",
    )

    result = runner.invoke(cli_mod.app, ["project", "data-node-updates", "list", "123"])
    assert result.exit_code == 0
    assert "Project Data Node Updates" in result.output
    assert "data-node-update-ui" in result.output
    assert "abc123" in result.output
    assert "storage-xyz" in result.output
    assert "Total updates: 1" in result.output


def test_project_can_view(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_project_users_can_view",
        lambda project_id, timeout=None: [
            {
                "id": 12,
                "username": "viewer",
                "email": "viewer@example.com",
                "first_name": "View",
                "last_name": "User",
            }
        ],
    )

    result = runner.invoke(cli_mod.app, ["project", "can_view", "4"])
    assert result.exit_code == 0
    assert "Project Users Who Can View" in result.output
    assert "viewer@example.com" in result.output
    assert "Total users who can view: 1" in result.output


def test_project_add_label(cli_mod, runner, monkeypatch):
    captured = {}
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _add(project_id, labels, timeout=None):
        captured["project_id"] = project_id
        captured["labels"] = labels
        captured["timeout"] = timeout
        return {"labels": [{"name": "rates"}, {"name": "research"}]}

    monkeypatch.setattr(cli_mod, "add_project_labels", _add)

    result = runner.invoke(
        cli_mod.app,
        ["project", "add-label", "project-uid-4", "--label", "rates,research"],
    )
    assert result.exit_code == 0
    assert captured == {
        "project_id": "project-uid-4",
        "labels": ["rates", "research"],
        "timeout": None,
    }
    assert "Project add-label completed." in result.output
    assert "rates, research" in result.output


def test_project_add_to_edit(cli_mod, runner, monkeypatch):
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _add(project_id, user_uid, timeout=None):
        captured["project_id"] = project_id
        captured["user_uid"] = user_uid
        captured["timeout"] = timeout
        return {
            "ok": True,
            "action": "add_to_edit",
            "detail": "User now has explicit edit access.",
            "object_uid": project_id,
            "object_type": "tdag.project",
            "user": {
                "uid": user_uid,
                "username": "editor",
                "email": "editor@example.com",
                "first_name": "Edit",
                "last_name": "User",
            },
            "explicit_can_view": True,
            "explicit_can_edit": True,
            "explicit_can_view_user_uids": [user_uid],
            "explicit_can_edit_user_uids": [user_uid],
        }

    monkeypatch.setattr(cli_mod, "add_project_user_to_edit", _add)

    result = runner.invoke(
        cli_mod.app,
        ["project", "add_to_edit", "project-uid-4", USER_UID],
    )
    assert result.exit_code == 0
    assert captured == {
        "project_id": "project-uid-4",
        "user_uid": USER_UID,
        "timeout": None,
    }
    assert "Project add_to_edit completed." in result.output
    assert "Project Sharing Update" in result.output
    assert "editor@example.com" in result.output


def test_project_add_to_edit_rejects_numeric_user_identifier(cli_mod, runner):
    result = runner.invoke(
        cli_mod.app,
        ["project", "add_to_edit", "project-uid-4", "12"],
    )

    assert result.exit_code == 2
    assert "invalid value" in result.output.lower()


def test_project_add_team_to_view(cli_mod, runner, monkeypatch):
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _add(project_id, team_uid, timeout=None):
        captured["project_id"] = project_id
        captured["team_uid"] = team_uid
        captured["timeout"] = timeout
        return {
            "action": "add_team_to_view",
            "detail": "Team now has explicit view access.",
            "object_uid": project_id,
            "object_type": "tdag.project",
            "team": {
                "uid": team_uid,
                "name": "Research",
                "description": "Core team",
            },
            "explicit_can_view": True,
            "explicit_can_edit": False,
            "explicit_can_view_team_uids": [team_uid],
            "explicit_can_edit_team_uids": [],
        }

    monkeypatch.setattr(cli_mod, "add_project_team_to_view", _add)

    result = runner.invoke(
        cli_mod.app,
        ["project", "add_team_to_view", "project-uid-4", TEAM_UID],
    )
    assert result.exit_code == 0
    assert captured == {
        "project_id": "project-uid-4",
        "team_uid": TEAM_UID,
        "timeout": None,
    }
    assert "Project add_team_to_view completed." in result.output
    assert "Research" in result.output


def test_get_project_data_node_updates_sets_project_env(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"}
    )
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")
    monkeypatch.delenv("MAIN_SEQUENCE_PROJECT_UID", raising=False)
    monkeypatch.setattr(
        api_mod,
        "resolve_project_branch_uid",
        lambda value: str(value),
    )

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
    fake_models = types.ModuleType("mainsequence.client.models_foundry")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.MAINSEQUENCE_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/api/v1"
    fake_utils.AUTH_ENDPOINT = "https://old.test"

    def _set_mainsequence_endpoint(endpoint):
        normalized = endpoint.rstrip("/")
        fake_utils.MAINSEQUENCE_ENDPOINT = normalized
        fake_utils.API_ENDPOINT = f"{normalized}/api/v1"
        fake_utils.AUTH_ENDPOINT = normalized
        captured["endpoint"] = normalized

    fake_utils.set_mainsequence_endpoint = _set_mainsequence_endpoint
    fake_utils.AUTH_ENDPOINT = "https://old.test"

    def _set_mainsequence_endpoint(endpoint):
        normalized = endpoint.rstrip("/")
        fake_utils.MAINSEQUENCE_ENDPOINT = normalized
        fake_utils.API_ENDPOINT = f"{normalized}/api/v1"
        fake_utils.AUTH_ENDPOINT = normalized
        captured["endpoint"] = normalized

    fake_utils.set_mainsequence_endpoint = _set_mainsequence_endpoint
    fake_utils.AUTH_ENDPOINT = "https://old.test"

    def _set_mainsequence_endpoint(endpoint):
        fake_utils.MAINSEQUENCE_ENDPOINT = endpoint
        fake_utils.API_ENDPOINT = f"{endpoint.rstrip('/')}/api/v1"
        fake_utils.AUTH_ENDPOINT = endpoint.rstrip("/")
        captured["endpoint"] = endpoint

    fake_utils.set_mainsequence_endpoint = _set_mainsequence_endpoint

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/api/v1"

    class FakeUpdate:
        def model_dump(self):
            return {"uid": "data-node-update-uid-10", "update_hash": "abc123"}

    class FakeProjectBranch:
        ROOT_URL = "https://old.test/api/v1/project-branches"

        @classmethod
        def get(cls, pk, timeout=None):
            captured["project_branch_uid_arg"] = pk
            captured["env_project_uid"] = os.environ.get("MAIN_SEQUENCE_PROJECT_UID")
            return types.SimpleNamespace(get_data_nodes_updates=lambda timeout=None: [FakeUpdate()])

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.ProjectBranch = FakeProjectBranch
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_foundry", fake_models)

    out = api_mod.get_project_data_node_updates("5a28020a-0f1b-47ee-aab8-334286234bea")
    assert captured["project_branch_uid_arg"] == "5a28020a-0f1b-47ee-aab8-334286234bea"
    assert captured["env_project_uid"] is None
    assert captured["jwt"] == ("acc", "ref")
    assert out == [{"uid": "data-node-update-uid-10", "update_hash": "abc123"}]
    assert os.environ.get("MAIN_SEQUENCE_PROJECT_UID") is None


def test_list_project_users_can_view_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"}
    )
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
    fake_models = types.ModuleType("mainsequence.client.models_foundry")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.MAINSEQUENCE_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/api/v1"
    fake_utils.AUTH_ENDPOINT = "https://old.test"

    def _set_mainsequence_endpoint(endpoint):
        normalized = endpoint.rstrip("/")
        fake_utils.MAINSEQUENCE_ENDPOINT = normalized
        fake_utils.API_ENDPOINT = f"{normalized}/api/v1"
        fake_utils.AUTH_ENDPOINT = normalized
        captured["endpoint"] = normalized

    fake_utils.set_mainsequence_endpoint = _set_mainsequence_endpoint

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/api/v1"

    class FakeProject:
        ROOT_URL = "https://old.test/api/v1/projects"

        @classmethod
        def get_by_uid(cls, uid, timeout=None):
            captured["get_by_uid"] = {"uid": uid, "timeout": timeout}

            class _Project:
                def can_view(self, timeout=None):
                    captured["can_view_timeout"] = timeout
                    return types.SimpleNamespace(
                        model_dump=lambda mode="python": {
                            "object_uid": uid,
                            "object_type": "tdag.project",
                            "access_level": "view",
                            "users": [
                                {
                                    "id": 12,
                                    "username": "viewer",
                                    "email": "viewer@example.com",
                                    "first_name": "View",
                                    "last_name": "User",
                                }
                            ],
                            "teams": [],
                        }
                    )

            return _Project()

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.Project = FakeProject
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_foundry", fake_models)

    out = api_mod.list_project_users_can_view("project-uid-4", timeout=9)
    assert captured["get_by_uid"] == {"uid": "project-uid-4", "timeout": 9}
    assert captured["can_view_timeout"] == 9
    assert captured["jwt"] == ("acc", "ref")
    assert out["users"][0]["username"] == "viewer"


def test_add_project_user_to_edit_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"}
    )
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
    fake_models = types.ModuleType("mainsequence.client.models_foundry")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.MAINSEQUENCE_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/api/v1"
    fake_utils.AUTH_ENDPOINT = "https://old.test"

    def _set_mainsequence_endpoint(endpoint):
        normalized = endpoint.rstrip("/")
        fake_utils.MAINSEQUENCE_ENDPOINT = normalized
        fake_utils.API_ENDPOINT = f"{normalized}/api/v1"
        fake_utils.AUTH_ENDPOINT = normalized
        captured["endpoint"] = normalized

    fake_utils.set_mainsequence_endpoint = _set_mainsequence_endpoint

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/api/v1"

    class FakeProject:
        ROOT_URL = "https://old.test/api/v1/projects"

        @classmethod
        def get_by_uid(cls, uid, timeout=None):
            captured["get_by_uid"] = {"uid": uid, "timeout": timeout}

            class _Project:
                def add_to_edit(self, user_uid, timeout=None):
                    captured["add_to_edit"] = {"user_uid": user_uid, "timeout": timeout}
                    return {
                        "ok": True,
                        "action": "add_to_edit",
                        "detail": "User now has explicit edit access.",
                        "object_uid": uid,
                        "object_type": "tdag.project",
                        "user": {
                            "uid": user_uid,
                            "username": "editor",
                            "email": "editor@example.com",
                        },
                        "explicit_can_view": True,
                        "explicit_can_edit": True,
                        "explicit_can_view_user_uids": [user_uid],
                        "explicit_can_edit_user_uids": [user_uid],
                    }

            return _Project()

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.Project = FakeProject
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_foundry", fake_models)

    out = api_mod.add_project_user_to_edit("project-uid-4", USER_UID, timeout=10)
    assert captured["get_by_uid"] == {"uid": "project-uid-4", "timeout": 10}
    assert captured["add_to_edit"] == {"user_uid": USER_UID, "timeout": 10}
    assert captured["jwt"] == ("acc", "ref")
    assert out["action"] == "add_to_edit"


def test_list_constants_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {"filters": []}

    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"}
    )
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
    fake_models = types.ModuleType("mainsequence.client.models_foundry")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.MAINSEQUENCE_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/api/v1"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/api/v1"

    class FakeConstant:
        ROOT_URL = "https://old.test/api/v1/constants"

        @classmethod
        def filter(cls, timeout=None, **kwargs):
            captured["filters"].append(kwargs)
            return [
                types.SimpleNamespace(
                    model_dump=lambda mode="python": {
                        "id": 7,
                        "name": "ASSETS__MASTER",
                        "value": {"source": "bbg"},
                    }
                )
            ]

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.Constant = FakeConstant
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_foundry", fake_models)

    out = api_mod.list_constants(filters={"name__in": ["ASSETS__MASTER"]})
    assert captured["filters"][0] == {"name__in": ["ASSETS__MASTER"]}
    assert captured["jwt"] == ("acc", "ref")
    assert out == [{"id": 7, "name": "ASSETS__MASTER", "value": {"source": "bbg"}}]


def test_create_constant_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"}
    )
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
    fake_models = types.ModuleType("mainsequence.client.models_foundry")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.MAINSEQUENCE_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/api/v1"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/api/v1"

    class FakeConstant:
        ROOT_URL = "https://old.test/api/v1/constants"

        @classmethod
        def create(cls, *, name, value, timeout=None):
            captured["name"] = name
            captured["value"] = value
            captured["timeout"] = timeout
            return types.SimpleNamespace(
                model_dump=lambda mode="python": {
                    "id": 7,
                    "name": name,
                    "value": value,
                }
            )

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.Constant = FakeConstant
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_foundry", fake_models)

    out = api_mod.create_constant(name="ASSETS__MASTER", value={"source": "bbg"}, timeout=15)
    assert captured["name"] == "ASSETS__MASTER"
    assert captured["value"] == {"source": "bbg"}
    assert captured["timeout"] == 15
    assert captured["jwt"] == ("acc", "ref")
    assert out["id"] == 7


def test_delete_constant_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"}
    )
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
    fake_models = types.ModuleType("mainsequence.client.models_foundry")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.MAINSEQUENCE_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/api/v1"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/api/v1"

    class FakeConstant:
        ROOT_URL = "https://old.test/api/v1/constants"

        @classmethod
        def get_by_uid(cls, uid, timeout=None):
            captured["get_by_uid"] = {"uid": uid, "timeout": timeout}

            class _Constant:
                def model_dump(self, mode="python"):
                    return {
                        "uid": uid,
                        "name": "ASSETS__MASTER",
                        "value": {"source": "bbg"},
                    }

                def delete(self, timeout=None):
                    captured["delete_timeout"] = timeout

            return _Constant()

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.Constant = FakeConstant
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_foundry", fake_models)

    out = api_mod.delete_constant("constant-uid-7", timeout=20)
    assert captured["get_by_uid"] == {"uid": "constant-uid-7", "timeout": 20}
    assert captured["delete_timeout"] == 20
    assert captured["jwt"] == ("acc", "ref")
    assert out["name"] == "ASSETS__MASTER"


def test_list_constant_users_can_edit_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"}
    )
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
    fake_models = types.ModuleType("mainsequence.client.models_foundry")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.MAINSEQUENCE_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/api/v1"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/api/v1"

    class FakeConstant:
        ROOT_URL = "https://old.test/api/v1/constants"

        @classmethod
        def get_by_uid(cls, uid, timeout=None):
            captured["get_by_uid"] = {"uid": uid, "timeout": timeout}

            class _Constant:
                def can_edit(self, timeout=None):
                    captured["can_edit_timeout"] = timeout
                    return types.SimpleNamespace(
                        model_dump=lambda mode="python": {
                            "object_uid": uid,
                            "object_type": "tdag.constant",
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
                            "teams": [],
                        }
                    )

            return _Constant()

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.Constant = FakeConstant
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_foundry", fake_models)

    out = api_mod.list_constant_users_can_edit("constant-uid-7", timeout=12)
    assert captured["get_by_uid"] == {"uid": "constant-uid-7", "timeout": 12}
    assert captured["can_edit_timeout"] == 12
    assert captured["jwt"] == ("acc", "ref")
    assert out["users"][0]["username"] == "editor"


def test_add_constant_user_to_edit_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"}
    )
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
    fake_models = types.ModuleType("mainsequence.client.models_foundry")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.MAINSEQUENCE_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/api/v1"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/api/v1"

    class FakeConstant:
        ROOT_URL = "https://old.test/api/v1/constants"

        @classmethod
        def get_by_uid(cls, uid, timeout=None):
            captured["get_by_uid"] = {"uid": uid, "timeout": timeout}

            class _Constant:
                def add_to_edit(self, user_uid, timeout=None):
                    captured["add_to_edit"] = {"user_uid": user_uid, "timeout": timeout}
                    return {
                        "ok": True,
                        "action": "add_to_edit",
                        "detail": "User now has explicit edit access.",
                        "object_uid": uid,
                        "object_type": "tdag.constant",
                        "user": {
                            "uid": user_uid,
                            "username": "editor",
                            "email": "editor@example.com",
                        },
                        "explicit_can_view": True,
                        "explicit_can_edit": True,
                        "explicit_can_view_user_uids": [user_uid],
                        "explicit_can_edit_user_uids": [user_uid],
                    }

            return _Constant()

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.Constant = FakeConstant
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_foundry", fake_models)

    out = api_mod.add_constant_user_to_edit("constant-uid-7", USER_UID, timeout=14)
    assert captured["get_by_uid"] == {"uid": "constant-uid-7", "timeout": 14}
    assert captured["add_to_edit"] == {"user_uid": USER_UID, "timeout": 14}
    assert captured["jwt"] == ("acc", "ref")
    assert out["action"] == "add_to_edit"


def test_list_secrets_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {"filters": []}

    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"}
    )
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
    fake_models = types.ModuleType("mainsequence.client.models_foundry")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.MAINSEQUENCE_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/api/v1"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/api/v1"

    class FakeSecret:
        ROOT_URL = "https://old.test/api/v1/secrets"

        @classmethod
        def filter(cls, timeout=None, **kwargs):
            captured["filters"].append(kwargs)
            return [
                types.SimpleNamespace(
                    model_dump=lambda mode="python": {
                        "id": 8,
                        "name": "API_KEY",
                    }
                )
            ]

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.Secret = FakeSecret
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_foundry", fake_models)

    out = api_mod.list_secrets(filters={"name__in": ["API_KEY"]})
    assert captured["filters"][0] == {"name__in": ["API_KEY"]}
    assert captured["jwt"] == ("acc", "ref")
    assert out == [{"id": 8, "name": "API_KEY"}]


def test_list_secret_users_can_view_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"}
    )
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
    fake_models = types.ModuleType("mainsequence.client.models_foundry")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.MAINSEQUENCE_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/api/v1"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/api/v1"

    class FakeSecret:
        ROOT_URL = "https://old.test/api/v1/secrets"

        @classmethod
        def get_by_uid(cls, uid, timeout=None):
            captured["get_by_uid"] = {"uid": uid, "timeout": timeout}

            class _Secret:
                def can_view(self, timeout=None):
                    captured["can_view_timeout"] = timeout
                    return types.SimpleNamespace(
                        model_dump=lambda mode="python": {
                            "object_uid": uid,
                            "object_type": "tdag.secret",
                            "access_level": "view",
                            "users": [
                                {
                                    "id": 11,
                                    "username": "viewer",
                                    "email": "viewer@example.com",
                                    "first_name": "View",
                                    "last_name": "User",
                                }
                            ],
                            "teams": [
                                {"id": 4, "name": "Ops", "description": "", "member_count": 2}
                            ],
                        }
                    )

            return _Secret()

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.Secret = FakeSecret
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_foundry", fake_models)

    out = api_mod.list_secret_users_can_view("secret-uid-8", timeout=13)
    assert captured["get_by_uid"] == {"uid": "secret-uid-8", "timeout": 13}
    assert captured["can_view_timeout"] == 13
    assert captured["jwt"] == ("acc", "ref")
    assert out["users"][0]["username"] == "viewer"
    assert out["teams"][0]["name"] == "Ops"
    assert out["teams"][0]["member_count"] == 2


def test_add_secret_user_to_edit_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"}
    )
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
    fake_models = types.ModuleType("mainsequence.client.models_foundry")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.MAINSEQUENCE_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/api/v1"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/api/v1"

    class FakeSecret:
        ROOT_URL = "https://old.test/api/v1/secrets"

        @classmethod
        def get_by_uid(cls, uid, timeout=None):
            captured["get_by_uid"] = {"uid": uid, "timeout": timeout}

            class _Secret:
                def add_to_edit(self, user_uid, timeout=None):
                    captured["add_to_edit"] = {"user_uid": user_uid, "timeout": timeout}
                    return {
                        "ok": True,
                        "action": "add_to_edit",
                        "detail": "User now has explicit edit access.",
                        "object_uid": uid,
                        "object_type": "tdag.secret",
                        "user": {
                            "uid": user_uid,
                            "username": "editor",
                            "email": "editor@example.com",
                        },
                        "explicit_can_view": True,
                        "explicit_can_edit": True,
                        "explicit_can_view_user_uids": [user_uid],
                        "explicit_can_edit_user_uids": [user_uid],
                    }

            return _Secret()

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.Secret = FakeSecret
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_foundry", fake_models)

    out = api_mod.add_secret_user_to_edit("secret-uid-8", USER_UID, timeout=14)
    assert captured["get_by_uid"] == {"uid": "secret-uid-8", "timeout": 14}
    assert captured["add_to_edit"] == {"user_uid": USER_UID, "timeout": 14}
    assert captured["jwt"] == ("acc", "ref")
    assert out["action"] == "add_to_edit"


def test_create_secret_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"}
    )
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
    fake_models = types.ModuleType("mainsequence.client.models_foundry")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.MAINSEQUENCE_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/api/v1"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/api/v1"

    class FakeSecret:
        ROOT_URL = "https://old.test/api/v1/secrets"

        @classmethod
        def create(cls, *, name, value, timeout=None):
            captured["name"] = name
            captured["value"] = value
            captured["timeout"] = timeout
            return types.SimpleNamespace(
                model_dump=lambda mode="python": {
                    "id": 8,
                    "name": name,
                }
            )

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.Secret = FakeSecret
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_foundry", fake_models)

    out = api_mod.create_secret(name="API_KEY", value="super-secret", timeout=10)
    assert captured["name"] == "API_KEY"
    assert captured["value"] == "super-secret"
    assert captured["timeout"] == 10
    assert captured["jwt"] == ("acc", "ref")
    assert out["id"] == 8


def test_delete_secret_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"}
    )
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
    fake_models = types.ModuleType("mainsequence.client.models_foundry")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.MAINSEQUENCE_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/api/v1"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/api/v1"

    class FakeSecret:
        ROOT_URL = "https://old.test/api/v1/secrets"

        @classmethod
        def get(cls, pk=None, timeout=None, **filters):
            captured["get"] = {"pk": pk, "timeout": timeout, "filters": filters}

            class _Secret:
                id = pk

                def model_dump(self, mode="python"):
                    return {
                        "id": pk,
                        "name": "API_KEY",
                    }

                def delete(self, timeout=None):
                    captured["delete_timeout"] = timeout

            return _Secret()

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.Secret = FakeSecret
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_foundry", fake_models)

    out = api_mod.delete_secret(8, timeout=20)
    assert captured["get"] == {"pk": 8, "timeout": 20, "filters": {}}
    assert captured["delete_timeout"] == 20
    assert captured["jwt"] == ("acc", "ref")
    assert out["name"] == "API_KEY"


def test_create_project_image_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"}
    )
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")
    monkeypatch.delenv("MAIN_SEQUENCE_PROJECT_UID", raising=False)
    monkeypatch.setattr(api_mod, "resolve_project_branch_uid", lambda value: str(value))

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
    fake_models = types.ModuleType("mainsequence.client.models_foundry")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.MAINSEQUENCE_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/api/v1"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/api/v1"

    class FakeProjectImage:
        ROOT_URL = "https://old.test/api/v1/project-images"

        @classmethod
        def create(
            cls,
            *,
            project_repo_hash,
            related_project_branch_uid=None,
            base_image_uid=None,
            timeout=None,
        ):
            captured["project_repo_hash"] = project_repo_hash
            captured["related_project_branch_uid"] = related_project_branch_uid
            captured["base_image_uid"] = base_image_uid
            captured["env_project_uid"] = os.environ.get("MAIN_SEQUENCE_PROJECT_UID")
            return types.SimpleNamespace(
                model_dump=lambda: {
                    "uid": "8b62d1dd-e146-44af-957c-38c5f5b1d8d5",
                    "project_repo_hash": project_repo_hash,
                    "related_project": related_project_branch_uid,
                    "base_image": base_image_uid,
                    "build_error": False,
                    "is_ready": False,
                    "creation_date": "2026-04-07T09:00:00Z",
                }
            )

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.ProjectImage = FakeProjectImage
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_foundry", fake_models)

    out = api_mod.create_project_image(
        project_repo_hash="abc123",
        related_project_branch_uid="5a28020a-0f1b-47ee-aab8-334286234bea",
        base_image_uid="22222222-2222-4222-8222-222222222222",
    )
    assert captured["project_repo_hash"] == "abc123"
    assert captured["related_project_branch_uid"] == "5a28020a-0f1b-47ee-aab8-334286234bea"
    assert captured["base_image_uid"] == "22222222-2222-4222-8222-222222222222"
    assert captured["env_project_uid"] is None
    assert captured["jwt"] == ("acc", "ref")
    assert out["uid"] == "8b62d1dd-e146-44af-957c-38c5f5b1d8d5"
    assert out["build_error"] is False
    assert os.environ.get("MAIN_SEQUENCE_PROJECT_UID") is None


def test_list_project_images_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {"filters": []}

    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"}
    )
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")
    monkeypatch.delenv("MAIN_SEQUENCE_PROJECT_UID", raising=False)
    monkeypatch.setattr(api_mod, "resolve_project_branch_uid", lambda value: str(value))

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
    fake_models = types.ModuleType("mainsequence.client.models_foundry")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.MAINSEQUENCE_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/api/v1"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/api/v1"

    class FakeProjectImage:
        ROOT_URL = "https://old.test/api/v1/project-images"

        @classmethod
        def filter(cls, timeout=None, **kwargs):
            captured["filters"].append(kwargs)
            captured["env_project_uid"] = os.environ.get("MAIN_SEQUENCE_PROJECT_UID")
            if "related_project_branch_uid" in kwargs:
                return [
                    types.SimpleNamespace(
                        model_dump=lambda: {
                            "uid": "8b62d1dd-e146-44af-957c-38c5f5b1d8d5",
                            "project_repo_hash": "abc123",
                            "related_project": 123,
                            "base_image": {"id": 22, "title": "Python 3.12"},
                            "build_error": False,
                            "is_ready": False,
                            "creation_date": "2026-04-07T09:00:00Z",
                        }
                    )
                ]
            return []

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.ProjectImage = FakeProjectImage
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_foundry", fake_models)

    out = api_mod.list_project_images(
        related_project_branch_uid="5a28020a-0f1b-47ee-aab8-334286234bea",
        filters={"project_repo_hash__in": ["abc123", "def456"]},
    )
    assert captured["filters"][0] == {
        "project_repo_hash__in": ["abc123", "def456"],
        "related_project_branch_uid": "5a28020a-0f1b-47ee-aab8-334286234bea",
    }
    assert captured["env_project_uid"] is None
    assert captured["jwt"] == ("acc", "ref")
    assert out == [
        {
            "uid": "8b62d1dd-e146-44af-957c-38c5f5b1d8d5",
            "project_repo_hash": "abc123",
            "related_project": 123,
            "base_image": {"id": 22, "title": "Python 3.12"},
            "build_error": False,
            "is_ready": False,
            "creation_date": "2026-04-07T09:00:00Z",
        }
    ]
    assert os.environ.get("MAIN_SEQUENCE_PROJECT_UID") is None


def test_delete_project_image_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"}
    )
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
    fake_models = types.ModuleType("mainsequence.client.models_foundry")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.MAINSEQUENCE_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/api/v1"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/api/v1"

    class FakeProjectImage:
        ROOT_URL = "https://old.test/api/v1/project-images"

        @classmethod
        def get(cls, pk=None, timeout=None, **filters):
            captured["get"] = {"pk": pk, "timeout": timeout, "filters": filters}

            class _Image:
                uid = pk

                def model_dump(self, mode="python"):
                    return {
                        "uid": pk,
                        "project_repo_hash": "abc123",
                        "base_image": {"id": 22, "title": "Python 3.12"},
                        "is_ready": True,
                    }

                def delete(self):
                    captured["deleted"] = pk

            return _Image()

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.ProjectImage = FakeProjectImage
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_foundry", fake_models)

    image_uid = "8b62d1dd-e146-44af-957c-38c5f5b1d8d5"
    out = api_mod.delete_project_image(image_uid=image_uid)
    assert captured["get"] == {"pk": image_uid, "timeout": None, "filters": {}}
    assert captured["deleted"] == image_uid
    assert captured["jwt"] == ("acc", "ref")
    assert out["uid"] == image_uid
    assert out["project_repo_hash"] == "abc123"


def test_list_project_jobs_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {"filters": []}

    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"}
    )
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")
    monkeypatch.delenv("MAIN_SEQUENCE_PROJECT_UID", raising=False)
    monkeypatch.setattr(api_mod, "resolve_project_branch_uid", lambda value: str(value))

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.MAINSEQUENCE_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/api/v1"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/api/v1"

    class FakeJob:
        ROOT_URL = "https://old.test/api/v1/jobs"

        @classmethod
        def filter(cls, timeout=None, **kwargs):
            captured["filters"].append(kwargs)
            captured["env_project_uid"] = os.environ.get("MAIN_SEQUENCE_PROJECT_UID")
            if "project_branch_uid" in kwargs:
                return [
                    types.SimpleNamespace(
                        model_dump=lambda: {
                            "uid": "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da",
                            "name": "daily-run",
                            "organization_project_environment_uid": (
                                "58218213-5e4e-43de-a5bd-6757f4e1c8f6"
                            ),
                            "project_repo_hash": "abc123",
                            "execution_path": "src.jobs.daily:main",
                            "app_name": None,
                            "task_schedule": {
                                "name": "Every hour",
                                "task": "daily-run",
                                "schedule": {"type": "interval", "every": 1, "period": "hours"},
                            },
                            "related_image": 77,
                        }
                    )
                ]
            return []

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_helpers = types.ModuleType("mainsequence.client.models_helpers")
    fake_helpers.Job = FakeJob
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_helpers", fake_helpers)

    project_branch_uid = "5a28020a-0f1b-47ee-aab8-334286234bea"
    out = api_mod.list_project_jobs(
        project_branch_uid=project_branch_uid,
        filters={"name__contains": "daily"},
    )
    assert captured["filters"][0] == {
        "name__contains": "daily",
        "project_branch_uid": project_branch_uid,
    }
    assert captured["env_project_uid"] is None
    assert captured["jwt"] == ("acc", "ref")
    assert out == [
        {
            "uid": "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da",
            "name": "daily-run",
            "organization_project_environment_uid": (
                "58218213-5e4e-43de-a5bd-6757f4e1c8f6"
            ),
            "project_repo_hash": "abc123",
            "execution_path": "src.jobs.daily:main",
            "app_name": None,
            "task_schedule": {
                "name": "Every hour",
                "task": "daily-run",
                "schedule": {"type": "interval", "every": 1, "period": "hours"},
            },
            "related_image": 77,
        }
    ]
    assert os.environ.get("MAIN_SEQUENCE_PROJECT_UID") is None


def test_list_project_resources_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {"filters": []}

    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"}
    )
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")
    monkeypatch.delenv("MAIN_SEQUENCE_PROJECT_UID", raising=False)
    monkeypatch.setattr(api_mod, "resolve_project_branch_uid", lambda value: str(value))

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
    fake_helpers = types.ModuleType("mainsequence.client.models_helpers")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.MAINSEQUENCE_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/api/v1"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/api/v1"

    class FakeProjectResource:
        ROOT_URL = "https://old.test/api/v1/project-resources"

        @classmethod
        def filter(cls, timeout=None, **kwargs):
            captured["filters"].append(kwargs)
            captured["env_project_uid"] = os.environ.get("MAIN_SEQUENCE_PROJECT_UID")
            return [
                types.SimpleNamespace(
                    model_dump=lambda: {
                        "uid": "857bec7b-dd77-4272-aecd-13fc2138eacc",
                        "name": "analytics_dashboard.py",
                        "resource_type": "script",
                        "path": "src/dashboards/analytics_dashboard.py",
                        "filesize": 2048,
                        "last_modified": "2026-03-15T10:30:00Z",
                        "repo_commit_sha": "abc123",
                    }
                )
            ]

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_helpers.ProjectResource = FakeProjectResource
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_helpers", fake_helpers)

    out = api_mod.list_project_resources(
        project_branch_uid="5a28020a-0f1b-47ee-aab8-334286234bea",
        repo_commit_sha="abc123",
        resource_type="dashboard",
        filters={"uid__in": ["857bec7b-dd77-4272-aecd-13fc2138eacc"]},
    )
    assert captured["filters"][0] == {
        "uid__in": ["857bec7b-dd77-4272-aecd-13fc2138eacc"],
        "project__uid": "5a28020a-0f1b-47ee-aab8-334286234bea",
        "repo_commit_sha": "abc123",
        "resource_type": "dashboard",
    }
    assert captured["env_project_uid"] is None
    assert captured["jwt"] == ("acc", "ref")
    assert out[0]["name"] == "analytics_dashboard.py"
    assert os.environ.get("MAIN_SEQUENCE_PROJECT_UID") is None


def test_create_project_resource_release_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"}
    )
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
    fake_helpers = types.ModuleType("mainsequence.client.models_helpers")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.MAINSEQUENCE_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/api/v1"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/api/v1"

    class FakeProjectResource:
        ROOT_URL = "https://old.test/api/v1/project-resources"

        @classmethod
        def get(cls, pk=None, timeout=None, **filters):
            captured["get"] = {"pk": pk, "timeout": timeout, "filters": filters}

            class _Resource:
                def create_dashboard(self, **kwargs):
                    captured["create_dashboard"] = kwargs
                    return types.SimpleNamespace(
                        model_dump=lambda: {
                            "uid": "0ce33c15-e3b1-4677-a66e-70460b89198f",
                            "resource": pk,
                            "related_image": kwargs["related_image_uid"],
                        }
                    )

                def create_agent(self, **kwargs):
                    captured["create_agent"] = kwargs
                    return types.SimpleNamespace(
                        model_dump=lambda: {
                            "uid": "1ce33c15-e3b1-4677-a66e-70460b89198f",
                            "resource": pk,
                            "related_image": kwargs["related_image_uid"],
                        }
                    )

                def create_fastapi(self, **kwargs):
                    captured["create_fastapi"] = kwargs
                    return types.SimpleNamespace(
                        model_dump=lambda: {
                            "uid": "2ce33c15-e3b1-4677-a66e-70460b89198f",
                            "resource": pk,
                            "related_image": kwargs["related_image_uid"],
                        }
                    )

            return _Resource()

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_helpers.ProjectResource = FakeProjectResource
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_helpers", fake_helpers)

    out = api_mod.create_project_resource_release(
        release_kind="streamlit_dashboard",
        resource_uid="857bec7b-dd77-4272-aecd-13fc2138eacc",
        related_image_uid="8b62d1dd-e146-44af-957c-38c5f5b1d8d5",
        spot=True,
        cpu_request="0.5",
        memory_request="1",
        gpu_request="",
        gpu_type="",
        automatic_deployment=True,
    )
    assert captured["get"] == {
        "pk": "857bec7b-dd77-4272-aecd-13fc2138eacc",
        "timeout": None,
        "filters": {},
    }
    assert (
        captured["create_dashboard"]["related_image_uid"] == "8b62d1dd-e146-44af-957c-38c5f5b1d8d5"
    )
    assert captured["create_dashboard"]["spot"] is True
    assert captured["create_dashboard"]["cpu_request"] == "0.5"
    assert captured["create_dashboard"]["memory_request"] == "1"
    assert captured["create_dashboard"]["automatic_deployment"] is True
    assert captured["jwt"] == ("acc", "ref")
    assert out == {
        "uid": "0ce33c15-e3b1-4677-a66e-70460b89198f",
        "resource": "857bec7b-dd77-4272-aecd-13fc2138eacc",
        "related_image": "8b62d1dd-e146-44af-957c-38c5f5b1d8d5",
    }


def test_create_project_resource_release_uses_client_model_for_fastapi(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"}
    )
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
    fake_helpers = types.ModuleType("mainsequence.client.models_helpers")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.MAINSEQUENCE_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/api/v1"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/api/v1"

    class FakeProjectResource:
        ROOT_URL = "https://old.test/api/v1/project-resources"

        @classmethod
        def get(cls, pk=None, timeout=None, **filters):
            captured["get"] = {"pk": pk, "timeout": timeout, "filters": filters}

            class _Resource:
                def create_fastapi(self, **kwargs):
                    captured["create_fastapi"] = kwargs
                    return types.SimpleNamespace(
                        model_dump=lambda: {
                            "uid": "2ce33c15-e3b1-4677-a66e-70460b89198f",
                            "resource": pk,
                            "related_image": kwargs["related_image_uid"],
                        }
                    )

            return _Resource()

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_helpers.ProjectResource = FakeProjectResource
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_helpers", fake_helpers)

    out = api_mod.create_project_resource_release(
        release_kind="fastapi",
        resource_uid="857bec7b-dd77-4272-aecd-13fc2138eacc",
        related_image_uid="8b62d1dd-e146-44af-957c-38c5f5b1d8d5",
        spot=False,
        cpu_request="0.5",
        memory_request="1",
        gpu_request="",
        gpu_type="",
        automatic_deployment=False,
    )
    assert captured["get"] == {
        "pk": "857bec7b-dd77-4272-aecd-13fc2138eacc",
        "timeout": None,
        "filters": {},
    }
    assert captured["create_fastapi"]["related_image_uid"] == "8b62d1dd-e146-44af-957c-38c5f5b1d8d5"
    assert captured["create_fastapi"]["spot"] is False
    assert captured["create_fastapi"]["cpu_request"] == "0.5"
    assert captured["create_fastapi"]["memory_request"] == "1"
    assert captured["create_fastapi"]["automatic_deployment"] is False
    assert captured["jwt"] == ("acc", "ref")
    assert out == {
        "uid": "2ce33c15-e3b1-4677-a66e-70460b89198f",
        "resource": "857bec7b-dd77-4272-aecd-13fc2138eacc",
        "related_image": "8b62d1dd-e146-44af-957c-38c5f5b1d8d5",
    }


def test_delete_resource_release_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"}
    )
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
    fake_helpers = types.ModuleType("mainsequence.client.models_helpers")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.MAINSEQUENCE_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/api/v1"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/api/v1"

    class FakeResourceRelease:
        ROOT_URL = "https://old.test/api/v1/resource-releases"

        @classmethod
        def get(cls, pk=None, timeout=None, **filters):
            captured["get"] = {"pk": pk, "timeout": timeout, "filters": filters}

            class _Release:
                uid = pk

                def model_dump(self, mode="python"):
                    return {
                        "uid": pk,
                        "release_kind": "streamlit_dashboard",
                        "resource": 381,
                        "related_image": 94,
                    }

                def delete(self):
                    captured["deleted"] = pk

            return _Release()

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_helpers.ResourceRelease = FakeResourceRelease
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_helpers", fake_helpers)

    out = api_mod.delete_resource_release(
        release_uid="0ce33c15-e3b1-4677-a66e-70460b89198f",
        expected_release_kind="streamlit_dashboard",
    )
    assert captured["get"] == {
        "pk": "0ce33c15-e3b1-4677-a66e-70460b89198f",
        "timeout": None,
        "filters": {},
    }
    assert captured["deleted"] == "0ce33c15-e3b1-4677-a66e-70460b89198f"
    assert captured["jwt"] == ("acc", "ref")
    assert out["uid"] == "0ce33c15-e3b1-4677-a66e-70460b89198f"
    assert out["release_kind"] == "streamlit_dashboard"


def test_list_data_node_storages_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {"filters": []}

    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"}
    )
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
    fake_models = types.ModuleType("mainsequence.client.metatables")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.MAINSEQUENCE_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/api/v1"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/api/v1"

    class FakeDataNodeStorage:
        ROOT_URL = "https://old.test/api/v1/time-index-meta-tables"

        @classmethod
        def filter(cls, timeout=None, **kwargs):
            captured["filters"].append(kwargs)
            return [
                types.SimpleNamespace(
                    model_dump=lambda *args, **kwargs: {
                        "uid": "data-node-storage-42",
                        "physical_table_name": "weights_daily_physical",
                        "source_class_name": "NodeWeights",
                        "identifier": "weights_daily",
                        "data_source": {"display_name": "Default DB", "class_type": "timescale_db"},
                    }
                )
            ]

        @classmethod
        def get(cls, uid=None, timeout=None, **filters):
            captured["get"] = {"uid": uid, "filters": filters, "timeout": timeout}
            return types.SimpleNamespace(
                model_dump=lambda *args, **kwargs: {
                    "uid": uid,
                    "physical_table_name": "weights_daily_physical",
                    "source_class_name": "NodeWeights",
                    "identifier": "weights_daily",
                    "data_source": {"display_name": "Default DB", "class_type": "timescale_db"},
                    "protect_from_deletion": True,
                }
            )

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.TimeIndexMetaTable = FakeDataNodeStorage
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.metatables", fake_models)

    out = api_mod.list_data_node_storages(filters={"physical_table_name__contains": "weights"})
    detail = api_mod.get_data_node_storage("data-node-storage-42")
    assert captured["filters"][0] == {"physical_table_name__contains": "weights"}
    assert captured["get"] == {"uid": "data-node-storage-42", "filters": {}, "timeout": None}
    assert captured["jwt"] == ("acc", "ref")
    assert out == [
        {
            "uid": "data-node-storage-42",
            "physical_table_name": "weights_daily_physical",
            "source_class_name": "NodeWeights",
            "identifier": "weights_daily",
            "data_source": {"display_name": "Default DB", "class_type": "timescale_db"},
        }
    ]
    assert detail["uid"] == "data-node-storage-42"
    assert detail["physical_table_name"] == "weights_daily_physical"


def test_meta_table_api_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {"filters": []}

    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"}
    )
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
    fake_models = types.ModuleType("mainsequence.client.metatables")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.MAINSEQUENCE_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/api/v1"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/api/v1"

    class FakeMetaTable:
        ROOT_URL = "https://old.test/api/v1/meta-tables"

        @classmethod
        def filter(cls, timeout=None, **kwargs):
            captured["filters"].append({"filters": kwargs, "timeout": timeout})
            return [
                types.SimpleNamespace(
                    model_dump=lambda *args, **kwargs: {
                        "uid": "meta-table-42",
                        "physical_table_name": "weights_daily",
                        "identifier": "weights",
                        "namespace": "pytest",
                        "management_mode": "platform_managed",
                    }
                )
            ]

        @classmethod
        def get(cls, uid=None, timeout=None, **filters):
            captured["get"] = {"uid": uid, "filters": filters, "timeout": timeout}

            class _MetaTable:
                def model_dump(self, mode="json"):
                    return {
                        "uid": uid,
                        "physical_table_name": "weights_daily",
                        "identifier": "weights",
                    }

                def delete(self, *, timeout=None):
                    captured["delete"] = {"timeout": timeout}

            return _MetaTable()

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.MetaTable = FakeMetaTable
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.metatables", fake_models)

    out = api_mod.list_meta_tables(filters={"namespace": "pytest"}, timeout=11)
    detail = api_mod.get_meta_table("meta-table-42", timeout=12)
    deleted = api_mod.delete_meta_table("meta-table-42", timeout=13)

    assert captured["filters"] == [{"filters": {"namespace": "pytest"}, "timeout": 11}]
    assert captured["get"] == {"uid": "meta-table-42", "filters": {}, "timeout": 13}
    assert captured["delete"] == {"timeout": 13}
    assert captured["jwt"] == ("acc", "ref")
    assert out[0]["uid"] == "meta-table-42"
    assert detail["physical_table_name"] == "weights_daily"
    assert deleted["uid"] == "meta-table-42"


def test_validate_project_name_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"}
    )
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
    fake_models = types.ModuleType("mainsequence.client.models_foundry")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.MAINSEQUENCE_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/api/v1"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/api/v1"

    class FakeProject:
        ROOT_URL = "https://old.test/api/v1/projects"

        @classmethod
        def validate_name(cls, *, project_name, timeout=None):
            captured["project_name"] = project_name
            captured["timeout"] = timeout
            return types.SimpleNamespace(
                model_dump=lambda mode="json": {
                    "project_name": project_name,
                    "available": False,
                    "reason": "A project with this name already exists in your organization.",
                    "normalized": {
                        "slugified_project_name": "rates-platform",
                        "project_library_name": "rates_platform",
                    },
                    "suggestions": ["Rates Platform 2", "Rates Platform 3"],
                }
            )

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.Project = FakeProject
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_foundry", fake_models)

    out = api_mod.validate_project_name(project_name="Rates Platform", timeout=25)

    assert captured["jwt"] == ("acc", "ref")
    assert captured["project_name"] == "Rates Platform"
    assert captured["timeout"] == 25
    assert out["available"] is False
    assert out["normalized"]["project_library_name"] == "rates_platform"


def test_data_node_storage_description_search_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"}
    )
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
    fake_models = types.ModuleType("mainsequence.client.metatables")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.MAINSEQUENCE_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/api/v1"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/api/v1"

    class FakeDataNodeStorage:
        ROOT_URL = "https://old.test/api/v1/time-index-meta-tables"

        @classmethod
        def description_search(
            cls,
            q,
            *,
            q_embedding=None,
            trigram_k=200,
            embed_k=200,
            w_trgm=0.65,
            w_emb=0.35,
            embedding_model="default",
            **filters,
        ):
            captured["search"] = {
                "q": q,
                "q_embedding": q_embedding,
                "trigram_k": trigram_k,
                "embed_k": embed_k,
                "w_trgm": w_trgm,
                "w_emb": w_emb,
                "embedding_model": embedding_model,
                "filters": filters,
            }
            return {
                "count": 1,
                "next": None,
                "previous": None,
                "results": [
                    types.SimpleNamespace(
                        model_dump=lambda *args, **kwargs: {
                            "uid": "data-node-storage-42",
                            "physical_table_name": "weights_daily",
                            "identifier": "weights_daily",
                        }
                    )
                ],
            }

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.TimeIndexMetaTable = FakeDataNodeStorage
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.metatables", fake_models)

    out = api_mod.data_node_storage_description_search(
        "node weights",
        q_embedding=[0.1, 0.2],
        trigram_k=150,
        embed_k=120,
        w_trgm=0.7,
        w_emb=0.3,
        embedding_model="text-embedding-3-large",
        filters={"data_source__uid": "data-source-uid-2"},
    )

    assert captured["jwt"] == ("acc", "ref")
    assert captured["search"] == {
        "q": "node weights",
        "q_embedding": [0.1, 0.2],
        "trigram_k": 150,
        "embed_k": 120,
        "w_trgm": 0.7,
        "w_emb": 0.3,
        "embedding_model": "text-embedding-3-large",
        "filters": {"data_source__uid": "data-source-uid-2"},
    }
    assert out == {
        "count": 1,
        "next": None,
        "previous": None,
        "results": [
            {
                "uid": "data-node-storage-42",
                "physical_table_name": "weights_daily",
                "identifier": "weights_daily",
            }
        ],
    }


def test_data_node_storage_column_search_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"}
    )
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
    fake_models = types.ModuleType("mainsequence.client.metatables")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.MAINSEQUENCE_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/api/v1"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/api/v1"

    class FakeDataNodeStorage:
        ROOT_URL = "https://old.test/api/v1/time-index-meta-tables"

        @classmethod
        def column_search(cls, q, **filters):
            captured["search"] = {"q": q, "filters": filters}
            return [
                types.SimpleNamespace(
                    model_dump=lambda *args, **kwargs: {
                        "uid": "data-node-storage-43",
                        "physical_table_name": "prices_daily",
                        "identifier": "prices_daily",
                    }
                )
            ]

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.TimeIndexMetaTable = FakeDataNodeStorage
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.metatables", fake_models)

    out = api_mod.data_node_storage_column_search(
        "close", filters={"physical_table_name__contains": "prices"}
    )

    assert captured["jwt"] == ("acc", "ref")
    assert captured["search"] == {
        "q": "close",
        "filters": {"physical_table_name__contains": "prices"},
    }
    assert out == [
        {
            "uid": "data-node-storage-43",
            "physical_table_name": "prices_daily",
            "identifier": "prices_daily",
        }
    ]


def test_refresh_data_node_storage_search_index_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"}
    )
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
    fake_models = types.ModuleType("mainsequence.client.metatables")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.MAINSEQUENCE_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/api/v1"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/api/v1"

    class FakeDataNodeStorage:
        ROOT_URL = "https://old.test/api/v1/time-index-meta-tables"

        @classmethod
        def get(cls, uid=None, timeout=None, **filters):
            captured["get"] = {"uid": uid, "filters": filters, "timeout": timeout}

            class _Storage:
                def refresh_table_search_index(self, *, timeout=None):
                    captured["refresh"] = {"timeout": timeout}
                    return {"status": "queued", "message": "refresh started"}

            return _Storage()

        @classmethod
        def get_by_uid(cls, uid, timeout=None):
            return cls.get(uid=uid, timeout=timeout)

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.TimeIndexMetaTable = FakeDataNodeStorage
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.metatables", fake_models)

    out = api_mod.refresh_data_node_storage_search_index("data-node-storage-42", timeout=30)

    assert captured["jwt"] == ("acc", "ref")
    assert captured["get"] == {"uid": "data-node-storage-42", "filters": {}, "timeout": 30}
    assert captured["refresh"] == {"timeout": 30}
    assert out == {"status": "queued", "message": "refresh started", "uid": "data-node-storage-42"}


def test_delete_data_node_storage_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"}
    )
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
    fake_models = types.ModuleType("mainsequence.client.metatables")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.MAINSEQUENCE_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/api/v1"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/api/v1"

    class FakeDataNodeStorage:
        ROOT_URL = "https://old.test/api/v1/time-index-meta-tables"

        @classmethod
        def get(cls, uid=None, timeout=None, **filters):
            captured["get"] = {"uid": uid, "filters": filters, "timeout": timeout}

            class _Storage:
                def model_dump(self, mode="python"):
                    return {
                        "uid": uid,
                        "physical_table_name": "weights_daily",
                        "identifier": "weights_daily",
                    }

                def delete(
                    self,
                    *,
                    full_delete_selected=False,
                    full_delete_downstream_tables=False,
                    delete_with_no_table=False,
                    override_protection=False,
                    timeout=None,
                ):
                    captured["delete"] = {
                        "full_delete_selected": full_delete_selected,
                        "full_delete_downstream_tables": full_delete_downstream_tables,
                        "delete_with_no_table": delete_with_no_table,
                        "override_protection": override_protection,
                        "timeout": timeout,
                    }

            return _Storage()

        @classmethod
        def get_by_uid(cls, uid, timeout=None):
            return cls.get(uid=uid, timeout=timeout)

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.TimeIndexMetaTable = FakeDataNodeStorage
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.metatables", fake_models)

    out = api_mod.delete_data_node_storage(
        "data-node-storage-42",
        full_delete_selected=True,
        full_delete_downstream_tables=True,
        delete_with_no_table=False,
        override_protection=True,
        timeout=30,
    )
    assert captured["get"] == {"uid": "data-node-storage-42", "filters": {}, "timeout": 30}
    assert captured["delete"] == {
        "full_delete_selected": True,
        "full_delete_downstream_tables": True,
        "delete_with_no_table": False,
        "override_protection": True,
        "timeout": 30,
    }
    assert captured["jwt"] == ("acc", "ref")
    assert out == {
        "uid": "data-node-storage-42",
        "physical_table_name": "weights_daily",
        "identifier": "weights_daily",
    }


def test_list_data_node_storage_users_can_view_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"}
    )
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
    fake_models = types.ModuleType("mainsequence.client.metatables")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.MAINSEQUENCE_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/api/v1"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/api/v1"

    class FakeDataNodeStorage:
        ROOT_URL = "https://old.test/api/v1/time-index-meta-tables"

        @classmethod
        def get(cls, uid=None, timeout=None, **filters):
            captured["get"] = {"uid": uid, "filters": filters, "timeout": timeout}

            class _Storage:
                def can_view(self, timeout=None):
                    captured["can_view_timeout"] = timeout
                    return types.SimpleNamespace(
                        model_dump=lambda mode="python": {
                            "object_uid": uid,
                            "object_type": "tdag.datanodestorage",
                            "access_level": "view",
                            "users": [
                                {
                                    "id": 8,
                                    "username": "viewer",
                                    "email": "viewer@example.com",
                                    "first_name": "View",
                                    "last_name": "User",
                                }
                            ],
                            "teams": [],
                        }
                    )

            return _Storage()

        @classmethod
        def get_by_uid(cls, uid, timeout=None):
            return cls.get(uid=uid, timeout=timeout)

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.TimeIndexMetaTable = FakeDataNodeStorage
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.metatables", fake_models)

    out = api_mod.list_data_node_storage_users_can_view("data-node-storage-42", timeout=15)
    assert captured["get"] == {"uid": "data-node-storage-42", "filters": {}, "timeout": 15}
    assert captured["can_view_timeout"] == 15
    assert captured["jwt"] == ("acc", "ref")
    assert out["users"][0]["username"] == "viewer"


def test_add_data_node_storage_user_to_edit_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"}
    )
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
    fake_models = types.ModuleType("mainsequence.client.metatables")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.MAINSEQUENCE_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/api/v1"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/api/v1"

    class FakeDataNodeStorage:
        ROOT_URL = "https://old.test/api/v1/time-index-meta-tables"

        @classmethod
        def get(cls, uid=None, timeout=None, **filters):
            captured["get"] = {"uid": uid, "filters": filters, "timeout": timeout}

            class _Storage:
                def add_to_edit(self, user_uid, timeout=None):
                    captured["add_to_edit"] = {"user_uid": user_uid, "timeout": timeout}
                    return {
                        "ok": True,
                        "action": "add_to_edit",
                        "detail": "User now has explicit edit access.",
                        "object_uid": uid,
                        "object_type": "tdag.datanodestorage",
                        "user": {
                            "uid": user_uid,
                            "username": "editor",
                            "email": "editor@example.com",
                        },
                        "explicit_can_view": True,
                        "explicit_can_edit": True,
                        "explicit_can_view_user_uids": [user_uid],
                        "explicit_can_edit_user_uids": [user_uid],
                    }

            return _Storage()

        @classmethod
        def get_by_uid(cls, uid, timeout=None):
            return cls.get(uid=uid, timeout=timeout)

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.TimeIndexMetaTable = FakeDataNodeStorage
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.metatables", fake_models)

    out = api_mod.add_data_node_storage_user_to_edit(
        "data-node-storage-42",
        USER_UID,
        timeout=16,
    )
    assert captured["get"] == {"uid": "data-node-storage-42", "filters": {}, "timeout": 16}
    assert captured["add_to_edit"] == {"user_uid": USER_UID, "timeout": 16}
    assert captured["jwt"] == ("acc", "ref")
    assert out["action"] == "add_to_edit"


def test_get_logged_user_details_uses_canonical_authenticated_user_method(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"}
    )
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

    def _reject_legacy_request(*args, **kwargs):
        raise AssertionError("get_logged_user_details must not call a legacy auth endpoint")

    monkeypatch.setattr(api_mod, "authed", _reject_legacy_request)

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
    fake_models_user = types.ModuleType("mainsequence.client.models_user")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.MAINSEQUENCE_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/api/v1"
    fake_utils.AUTH_ENDPOINT = "https://old.test"

    def _set_mainsequence_endpoint(endpoint):
        normalized = endpoint.rstrip("/")
        fake_utils.MAINSEQUENCE_ENDPOINT = normalized
        fake_utils.API_ENDPOINT = f"{normalized}/api/v1"
        fake_utils.AUTH_ENDPOINT = normalized
        captured["endpoint"] = normalized

    fake_utils.set_mainsequence_endpoint = _set_mainsequence_endpoint

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/api/v1"

    class FakeUser:
        ROOT_URL = "https://old.test/api/v1/users"

        @classmethod
        def get_authenticated_user_details(cls):
            captured["current_user_url"] = f"{cls.ROOT_URL}/users/me/"
            return types.SimpleNamespace(
                model_dump=lambda: {
                    "id": 7,
                    "uid": "user-uid-7",
                    "username": "jose",
                    "email": "jose@main-sequence.io",
                    "organization": {
                        "id": 2,
                        "uid": "org-uid-2",
                        "name": "Main Sequence",
                        "production_environment_uid": ("00000000-0000-4000-8000-000000000002"),
                    },
                }
            )

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models_user.User = FakeUser
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_user", fake_models_user)

    out = api_mod.get_logged_user_details()
    assert captured["endpoint"] == "https://backend.test"
    assert fake_utils.MAINSEQUENCE_ENDPOINT == "https://backend.test"
    assert fake_utils.API_ENDPOINT == "https://backend.test/api/v1"
    assert fake_utils.AUTH_ENDPOINT == "https://backend.test"
    assert captured["jwt"] == ("acc", "ref")
    assert captured["current_user_url"] == "https://backend.test/api/v1/users/me/"
    assert "id" not in out
    assert "id" not in out["organization"]
    assert out["uid"] == "user-uid-7"
    assert out["username"] == "jose"
    assert (
        out["organization"]["production_environment_uid"] == "00000000-0000-4000-8000-000000000002"
    )


def test_search_projects_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"}
    )
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
    fake_models = types.ModuleType("mainsequence.client.models_foundry")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.MAINSEQUENCE_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/api/v1"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/api/v1"

    class FakeProject:
        ROOT_URL = "https://old.test/api/v1/projects"

        @classmethod
        def quick_search(cls, q, *, limit=20, timeout=None):
            captured["q"] = q
            captured["limit"] = limit
            captured["timeout"] = timeout
            return [
                types.SimpleNamespace(
                    model_dump=lambda mode="json": {
                        "uid": "project-uid-11",
                        "project_name": "alpha-research",
                        "project_type": "python",
                        "cluster_id": 7,
                    }
                ),
                types.SimpleNamespace(
                    model_dump=lambda mode="json": {
                        "uid": "project-uid-12",
                        "project_name": "data-live",
                        "project_type": "vite_react",
                        "cluster_id": 9,
                    }
                ),
            ]

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.Project = FakeProject
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_foundry", fake_models)

    out = api_mod.search_projects("alpha", limit=10, timeout=12)

    assert captured["jwt"] == ("acc", "ref")
    assert captured["q"] == "alpha"
    assert captured["limit"] == 10
    assert captured["timeout"] == 12
    assert out == [
        {
            "uid": "project-uid-11",
            "project_name": "alpha-research",
            "project_type": "python",
            "cluster_id": 7,
        },
        {
            "uid": "project-uid-12",
            "project_name": "data-live",
            "project_type": "vite_react",
            "cluster_id": 9,
        },
    ]


def test_create_project_job_uses_client_model_task_schedule(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"}
    )
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")
    monkeypatch.delenv("MAIN_SEQUENCE_PROJECT_UID", raising=False)
    monkeypatch.setattr(api_mod, "resolve_project_branch_uid", lambda value: str(value))

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
    fake_helpers = types.ModuleType("mainsequence.client.models_helpers")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.MAINSEQUENCE_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/api/v1"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/api/v1"

    class FakeJob:
        ROOT_URL = "https://old.test/api/v1/jobs"

        @classmethod
        def create(
            cls,
            *,
            name,
            project_branch_uid,
            execution_path=None,
            app_name=None,
            task_schedule=None,
            cpu_request=None,
            memory_request=None,
            gpu_request=None,
            gpu_type=None,
            spot=None,
            max_runtime_seconds=None,
            related_image_uid=None,
            automatic_deployment=False,
            automatic_redeployment_policy=None,
            timeout=None,
        ):
            captured["payload"] = {
                "name": name,
                "project_branch_uid": project_branch_uid,
                "execution_path": execution_path,
                "app_name": app_name,
                "task_schedule": task_schedule,
                "cpu_request": cpu_request,
                "memory_request": memory_request,
                "gpu_request": gpu_request,
                "gpu_type": gpu_type,
                "spot": spot,
                "max_runtime_seconds": max_runtime_seconds,
                "related_image_uid": related_image_uid,
                "automatic_deployment": automatic_deployment,
                "automatic_redeployment_policy": automatic_redeployment_policy,
                "timeout": timeout,
            }
            captured["env_project_uid"] = os.environ.get("MAIN_SEQUENCE_PROJECT_UID")
            return types.SimpleNamespace(
                model_dump=lambda: {
                    "uid": "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da",
                    "task_schedule": task_schedule,
                }
            )

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_helpers.Job = FakeJob
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_helpers", fake_helpers)

    schedule = {"schedule": {"type": "interval", "every": 1, "period": "hours"}, "one_off": False}
    out = api_mod.create_project_job(
        name="demo-job",
        project_branch_uid="5a28020a-0f1b-47ee-aab8-334286234bea",
        execution_path="scripts/test.py",
        task_schedule=schedule,
        cpu_request="0.25",
        memory_request="0.5",
        spot=False,
        max_runtime_seconds=86400,
        related_image_uid="6a28020a-0f1b-47ee-aab8-334286234bea",
    )
    assert captured["payload"]["task_schedule"] == schedule
    assert captured["payload"]["related_image_uid"] == "6a28020a-0f1b-47ee-aab8-334286234bea"
    assert captured["payload"]["automatic_deployment"] is False
    assert captured["payload"]["automatic_redeployment_policy"] == {"tag_regex": None}
    assert captured["env_project_uid"] is None
    assert captured["jwt"] == ("acc", "ref")
    assert out == {
        "uid": "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da",
        "task_schedule": schedule,
    }
    assert os.environ.get("MAIN_SEQUENCE_PROJECT_UID") is None


def test_create_project_does_not_send_project_visible(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    class FakeResponse:
        ok = True
        status_code = 201
        headers = {"content-type": "application/json"}

        def json(self):
            return {"id": 321, "project_name": "demo-project"}

    def _fake_authed(method, api_path, body=None):
        captured["method"] = method
        captured["api_path"] = api_path
        captured["body"] = body
        return FakeResponse()

    monkeypatch.setattr(api_mod, "authed", _fake_authed)

    out = api_mod.create_project(
        project_name="demo-project",
        default_base_image_uid="22222222-2222-4222-8222-222222222222",
        github_org_uid="33333333-3333-4333-8333-333333333333",
        env_vars={"FOO": "bar"},
    )

    assert captured["method"] == "POST"
    assert captured["api_path"] == "/api/v1/projects/"
    assert captured["body"] == {
        "project_name": "demo-project",
        "project_type": "python",
        "default_base_image_uid": "22222222-2222-4222-8222-222222222222",
        "github_org_uid": "33333333-3333-4333-8333-333333333333",
        "env_vars": [{"name": "FOO", "value": "bar"}],
    }
    assert "project_visible" not in captured["body"]
    assert out == {"id": 321, "project_name": "demo-project"}


def test_run_project_job_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"}
    )
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
    fake_helpers = types.ModuleType("mainsequence.client.models_helpers")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.MAINSEQUENCE_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/api/v1"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/api/v1"

    class FakeJob:
        ROOT_URL = "https://old.test/api/v1/jobs"

        @classmethod
        def get_by_uid(cls, uid, timeout=None):
            captured["job_uid_arg"] = uid
            return types.SimpleNamespace(
                execution_path="scripts/test.py",
                app_name=None,
                run_job=lambda timeout=None, command_args=None: (
                    captured.update(command_args=command_args)
                    or {
                        "uid": "4c1d77c8-8a42-42b8-a9c1-06be9a336e5d",
                        "job_uid": uid,
                        "status": "QUEUED",
                        "unique_identifier": "jobrun_abc123",
                    }
                ),
            )

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_helpers.Job = FakeJob
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_helpers", fake_helpers)

    job_uid = "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da"
    out = api_mod.run_project_job(job_uid, command_args=["python", "-m", "jobs.daily"])
    assert captured["job_uid_arg"] == job_uid
    assert captured["command_args"] == ["python", "-m", "jobs.daily"]
    assert captured["jwt"] == ("acc", "ref")
    assert out == {
        "uid": "4c1d77c8-8a42-42b8-a9c1-06be9a336e5d",
        "job_uid": job_uid,
        "status": "QUEUED",
        "unique_identifier": "jobrun_abc123",
        "effective_run": "scripts/test.py python -m jobs.daily",
        "command_args": ["python", "-m", "jobs.daily"],
    }


def test_list_project_job_runs_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {"filters": []}

    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"}
    )
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
    fake_helpers = types.ModuleType("mainsequence.client.models_helpers")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.MAINSEQUENCE_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/api/v1"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/api/v1"

    class FakeJobRun:
        ROOT_URL = "https://old.test/api/v1/job-runs"

        @classmethod
        def filter(cls, timeout=None, **kwargs):
            captured["filters"].append(kwargs)
            return [
                types.SimpleNamespace(
                    model_dump=lambda: {
                        "uid": "4c1d77c8-8a42-42b8-a9c1-06be9a336e5d",
                        "name": "daily-run-1",
                        "status": "COMPLETED",
                        "unique_identifier": "jobrun_abc123",
                    }
                )
            ]

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_helpers.JobRun = FakeJobRun
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_helpers", fake_helpers)

    job_uid = "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da"
    out = api_mod.list_project_job_runs(job_uid=job_uid, filters={"status": "COMPLETED"})
    assert captured["filters"][0] == {"status": "COMPLETED", "job__uid": job_uid}
    assert captured["jwt"] == ("acc", "ref")
    assert out == [
        {
            "uid": "4c1d77c8-8a42-42b8-a9c1-06be9a336e5d",
            "name": "daily-run-1",
            "status": "COMPLETED",
            "unique_identifier": "jobrun_abc123",
        }
    ]


def test_get_project_job_run_logs_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    real_job_run = importlib.import_module("mainsequence.client.models_helpers").JobRun
    captured = {}

    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"}
    )
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
    fake_helpers = types.ModuleType("mainsequence.client.models_helpers")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.MAINSEQUENCE_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/api/v1"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/api/v1"

    class CanonicalJobRun(real_job_run):
        @classmethod
        def get(cls, pk, timeout=None):
            captured["job_run_uid_arg"] = pk
            return cls.model_validate(
                {
                    "uid": pk,
                    "name": "daily-run-1",
                    "unique_identifier": "jobrun_abc123",
                    "job_uid": "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da",
                    "job_name": "daily-prices",
                    "project_uid": "1d0530c0-65d1-4db0-856b-dc29d8260a09",
                    "project_name": "market-data-service",
                    "project_branch_uid": "5a28020a-0f1b-47ee-aab8-334286234bea",
                    "project_branch_name": "main",
                    "status": "RUNNING",
                    "runtime_image_uid": "6cfdb152-923e-45b9-a150-c4541c68b0d1",
                    "runtime_image_digest": "sha256:" + "b" * 64,
                }
            )

        def get_logs(self, *, timeout=None):
            captured["get_logs_timeout"] = timeout
            return {
                "job_run_uid": self.uid,
                "status": self.status,
                "rows": ["first line"],
            }

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_helpers.JobRun = CanonicalJobRun
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_helpers", fake_helpers)

    out = api_mod.get_project_job_run_logs("4c1d77c8-8a42-42b8-a9c1-06be9a336e5d")
    assert captured["job_run_uid_arg"] == "4c1d77c8-8a42-42b8-a9c1-06be9a336e5d"
    assert captured["get_logs_timeout"] is None
    assert captured["jwt"] == ("acc", "ref")
    assert out == {
        "job_run_uid": "4c1d77c8-8a42-42b8-a9c1-06be9a336e5d",
        "status": "RUNNING",
        "rows": ["first line"],
    }


def test_project_get_data_node_updates_defaults_to_env_project_uid(
    cli_mod, runner, monkeypatch, tmp_path
):
    target = tmp_path / "demo-123"
    target.mkdir(parents=True, exist_ok=True)
    (target / ".env").write_text("", encoding="utf-8")

    captured = {}

    monkeypatch.chdir(target)
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "_resolve_project_branch_uid_for_command",
        lambda *args, **kwargs: "project-branch-uid-123",
    )

    def _get_updates(project_branch_uid, timeout=None):
        captured["project_branch_uid"] = project_branch_uid
        return [
            {
                "uid": "data-node-update-uid-10",
                "update_hash": "abc123",
                "data_node_storage": {
                    "uid": "meta-table-uid-42",
                    "physical_table_name": "storage-xyz",
                },
                "update_details": {"related_table_uid": "data-node-update-uid-10"},
            }
        ]

    monkeypatch.setattr(cli_mod, "get_project_data_node_updates", _get_updates)

    result = runner.invoke(cli_mod.app, ["project", "data-node-updates", "list"])
    assert result.exit_code == 0
    assert captured["project_branch_uid"] == "project-branch-uid-123"
    assert "abc123" in result.output
    assert "storage-xyz" in result.output


def test_project_images_defaults_to_env_project_id(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "demo-123"
    target.mkdir(parents=True, exist_ok=True)
    (target / ".env").write_text("", encoding="utf-8")

    monkeypatch.chdir(target)
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "_resolve_project_branch_uid_for_command",
        lambda *args, **kwargs: "project-branch-uid-123",
    )
    monkeypatch.setattr(
        cli_mod,
        "list_project_images",
        lambda related_project_branch_uid, filters=None, timeout=None: [
            {
                "uid": "8b62d1dd-e146-44af-957c-38c5f5b1d8d5",
                "project_repo_hash": "abc123",
                "base_image": {"id": 22, "title": "Python 3.12"},
            }
        ],
    )

    result = runner.invoke(cli_mod.app, ["project", "images", "list"])
    assert result.exit_code == 0
    assert "Project Images" in result.output
    assert "abc123" in result.output
    assert "Python 3.12" in result.output
    assert "Total images: 1" in result.output


def test_project_images_list_json(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "demo-123"
    target.mkdir(parents=True, exist_ok=True)
    (target / ".env").write_text("", encoding="utf-8")

    monkeypatch.chdir(target)
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "_resolve_project_branch_uid_for_command",
        lambda *args, **kwargs: "project-branch-uid-123",
    )
    monkeypatch.setattr(
        cli_mod,
        "list_project_images",
        lambda related_project_branch_uid, filters=None, timeout=None: [
            {
                "uid": "8b62d1dd-e146-44af-957c-38c5f5b1d8d5",
                "project_repo_hash": "abc123",
                "base_image": {"id": 22, "title": "Python 3.12"},
                "creation_date": "2026-04-10T12:00:00Z",
            }
        ],
    )

    result = runner.invoke(cli_mod.app, ["project", "images", "list", "--json"])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload[0]["uid"] == "8b62d1dd-e146-44af-957c-38c5f5b1d8d5"
    assert payload[0]["project_repo_hash"] == "abc123"
    assert payload[0]["creation_date"] == "2026-04-10T12:00:00Z"


def test_project_images_list_rejects_reserved_filter(cli_mod, runner, monkeypatch):
    def _parse(model_ref, entries):
        return {"related_project_branch_uid": ["other-branch-uid"]}

    monkeypatch.setattr(cli_mod, "parse_cli_model_filters", _parse)

    result = runner.invoke(
        cli_mod.app,
        [
            "project",
            "images",
            "list",
            "project-uid-123",
            "--filter",
            "related_project_branch_uid=other-branch-uid",
        ],
    )
    assert result.exit_code == 1
    assert "cannot be overridden" in result.output


def test_project_images_delete_requires_confirmation(cli_mod, runner, monkeypatch):
    captured = {}
    image_uid = "8b62d1dd-e146-44af-957c-38c5f5b1d8d5"

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "get_project_image",
        lambda image_uid, timeout=None: {
            "uid": image_uid,
            "project_repo_hash": "abc123",
            "base_image": {"id": 22, "title": "Python 3.12"},
            "is_ready": True,
        },
    )

    def _delete_project_image(image_uid, timeout=None):
        captured["image_uid"] = image_uid
        return {
            "uid": image_uid,
            "project_repo_hash": "abc123",
            "base_image": {"id": 22, "title": "Python 3.12"},
            "is_ready": True,
        }

    monkeypatch.setattr(cli_mod, "delete_project_image", _delete_project_image)

    result = runner.invoke(cli_mod.app, ["project", "images", "delete", image_uid], input="y\n")
    assert result.exit_code == 0
    assert captured["image_uid"] == image_uid
    assert "Project Image Delete Preview" in result.output
    assert f"Delete project image {image_uid}?" in result.output
    assert f"Project image deleted: uid={image_uid}" in result.output


def test_project_jobs_list_defaults_to_env_project_id(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "demo-123"
    target.mkdir(parents=True, exist_ok=True)
    (target / ".env").write_text("", encoding="utf-8")

    monkeypatch.chdir(target)
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "_resolve_project_branch_uid_for_command",
        lambda *args, **kwargs: "project-branch-uid-123",
    )
    monkeypatch.setattr(
        cli_mod,
        "list_project_jobs",
        lambda project_branch_uid, filters=None, timeout=None: [
            {
                "uid": "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da",
                "name": "daily-run",
                "project_repo_hash": "abc123",
                "execution_path": "src.jobs.daily:main",
                "app_name": None,
                "task_schedule": {
                    "name": "Every hour",
                    "task": "daily-run",
                    "schedule": {"type": "interval", "every": 1, "period": "hours"},
                },
                "related_image": 77,
            }
        ],
    )

    result = runner.invoke(cli_mod.app, ["project", "jobs", "list"])
    assert result.exit_code == 0
    assert "Project Jobs" in result.output
    assert "daily-ru" in result.output
    assert "abc123" in result.output
    assert "Every" in result.output
    assert "hour:" in result.output
    assert "every 1" in result.output
    assert "hours" in result.output
    assert "Total jobs: 1" in result.output


def test_project_jobs_list_show_filters_mentions_project_scope(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "build_cli_model_filter_rows", lambda model_ref: [])

    result = runner.invoke(cli_mod.app, ["project", "jobs", "list", "--show-filters"])
    assert result.exit_code == 0
    assert "No additional model filters exposed by Project Jobs." in result.output
    assert "Always Applied Filters" in result.output
    assert "project" in result.output


def test_project_project_resource_list_defaults_to_remote_branch_head(
    cli_mod, runner, monkeypatch, tmp_path
):
    target = tmp_path / "demo-123"
    target.mkdir(parents=True, exist_ok=True)
    (target / ".env").write_text("", encoding="utf-8")
    captured = {}

    monkeypatch.chdir(target)
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "_resolve_project_branch_uid_for_command",
        lambda *args, **kwargs: "project-branch-uid-123",
    )
    monkeypatch.setattr(
        cli_mod,
        "_get_remote_branch_head_commit",
        lambda project_dir: ("origin/main", "abc123"),
    )

    def _list_project_resources(
        project_branch_uid, repo_commit_sha, resource_type=None, filters=None, timeout=None
    ):
        captured["project_branch_uid"] = project_branch_uid
        captured["repo_commit_sha"] = repo_commit_sha
        captured["resource_type"] = resource_type
        return [
            {
                "uid": "857bec7b-dd77-4272-aecd-13fc2138eacc",
                "name": "analytics_dashboard.py",
                "resource_type": "script",
                "path": "src/dashboards/analytics_dashboard.py",
                "filesize": 2048,
                "last_modified": "2026-03-15T10:30:00Z",
            }
        ]

    monkeypatch.setattr(cli_mod, "list_project_resources", _list_project_resources)

    result = runner.invoke(cli_mod.app, ["project", "project_resource", "list"])
    assert result.exit_code == 0
    assert captured["project_branch_uid"] == "project-branch-uid-123"
    assert captured["repo_commit_sha"] == "abc123"
    assert "Using repo_commit_sha=abc123 from origin/main." in result.output
    assert "Project Resources" in result.output
    assert "analytics_d" in result.output
    assert "board.py" in result.output
    assert "Total project resources: 1" in result.output


def test_project_project_resource_list_passes_extra_filters(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "demo-123"
    target.mkdir(parents=True, exist_ok=True)
    (target / ".env").write_text("", encoding="utf-8")
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "_resolve_project_branch_uid_for_command",
        lambda *args, **kwargs: "project-branch-uid-123",
    )
    monkeypatch.setattr(
        cli_mod, "_get_remote_branch_head_commit", lambda project_dir: ("origin/main", "abc123")
    )

    def _parse(model_ref, entries):
        captured["entries"] = list(entries or [])
        return {"uid__in": ["857bec7b-dd77-4272-aecd-13fc2138eacc"]}

    def _list_project_resources(
        project_branch_uid, repo_commit_sha, resource_type=None, filters=None, timeout=None
    ):
        captured["filters"] = filters
        return []

    monkeypatch.setattr(cli_mod, "parse_cli_model_filters", _parse)
    monkeypatch.setattr(cli_mod, "list_project_resources", _list_project_resources)

    result = runner.invoke(
        cli_mod.app,
        [
            "project",
            "project_resource",
            "list",
            "--path",
            str(target),
            "--filter",
            "uid__in=857bec7b-dd77-4272-aecd-13fc2138eacc",
        ],
    )
    assert result.exit_code == 0
    assert captured["entries"] == ["uid__in=857bec7b-dd77-4272-aecd-13fc2138eacc"]
    assert captured["filters"] == {"uid__in": ["857bec7b-dd77-4272-aecd-13fc2138eacc"]}


def test_project_project_resource_create_dashboard_filters_resources_by_selected_image(
    cli_mod, runner, monkeypatch, tmp_path
):
    target = tmp_path / "demo-123"
    target.mkdir(parents=True, exist_ok=True)
    (target / ".env").write_text("", encoding="utf-8")
    captured = {}

    monkeypatch.chdir(target)
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "_resolve_project_branch_uid_for_command",
        lambda *args, **kwargs: "project-branch-uid-123",
    )
    monkeypatch.setattr(
        cli_mod,
        "list_project_images",
        lambda related_project_branch_uid, timeout=None: [
            {
                "uid": "8b62d1dd-e146-44af-957c-38c5f5b1d8d5",
                "project_repo_hash": "sha-94",
                "base_image": {"uid": "base-image-uid-1", "title": "py311"},
            },
        ],
    )

    def _list_project_resources(
        project_branch_uid, repo_commit_sha, resource_type=None, timeout=None
    ):
        captured["project_branch_uid"] = project_branch_uid
        captured["repo_commit_sha"] = repo_commit_sha
        captured["resource_type"] = resource_type
        return [
            {
                "uid": "857bec7b-dd77-4272-aecd-13fc2138eacc",
                "name": "analytics_dashboard.py",
                "resource_type": "dashboard",
                "path": "dashboards/analytics_dashboard.py",
                "filesize": 2048,
                "last_modified": "2026-03-15T10:30:00Z",
            }
        ]

    monkeypatch.setattr(cli_mod, "list_project_resources", _list_project_resources)

    def _create_release(**kwargs):
        captured["create_release"] = kwargs
        return {
            "uid": "0ce33c15-e3b1-4677-a66e-70460b89198f",
            "resource": kwargs["resource_uid"],
            "related_image": kwargs["related_image_uid"],
            "cpu_request": kwargs["cpu_request"],
            "memory_request": kwargs["memory_request"],
            "spot": kwargs["spot"],
        }

    monkeypatch.setattr(cli_mod, "create_project_resource_release", _create_release)

    result = runner.invoke(
        cli_mod.app,
        ["project", "project_resource", "create_dashboard", "--automatic-deployment"],
        input=("8b62d1dd-e146-44af-957c-38c5f5b1d8d5\n857bec7b-dd77-4272-aecd-13fc2138eacc\n"),
    )
    assert result.exit_code == 0
    assert captured["project_branch_uid"] == "project-branch-uid-123"
    assert captured["repo_commit_sha"] == "sha-94"
    assert captured["resource_type"] == "dashboard"
    assert captured["create_release"]["release_kind"] == "streamlit_dashboard"
    assert captured["create_release"]["related_image_uid"] == "8b62d1dd-e146-44af-957c-38c5f5b1d8d5"
    assert captured["create_release"]["resource_uid"] == "857bec7b-dd77-4272-aecd-13fc2138eacc"
    assert captured["create_release"]["cpu_request"] == "0.25"
    assert captured["create_release"]["memory_request"] == "0.5"
    assert captured["create_release"]["spot"] is False
    assert captured["create_release"]["automatic_deployment"] is True
    assert "Using defaults: cpu_request=0.25, memory_request=0.5, spot=false." in result.output
    assert "0ce33c15-e3b1-4677-a66e-70460b89198f" in result.output


def test_project_project_resource_create_fastapi_filters_resources_by_selected_image(
    cli_mod, runner, monkeypatch, tmp_path
):
    target = tmp_path / "demo-123"
    target.mkdir(parents=True, exist_ok=True)
    (target / ".env").write_text("", encoding="utf-8")
    captured = {}

    monkeypatch.chdir(target)
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "_resolve_project_branch_uid_for_command",
        lambda *args, **kwargs: "project-branch-uid-123",
    )
    monkeypatch.setattr(
        cli_mod,
        "list_project_images",
        lambda related_project_branch_uid, timeout=None: [
            {
                "uid": "8b62d1dd-e146-44af-957c-38c5f5b1d8d5",
                "project_repo_hash": "sha-94",
                "base_image": {"uid": "base-image-uid-1", "title": "py311"},
            },
        ],
    )

    def _list_project_resources(
        project_branch_uid, repo_commit_sha, resource_type=None, timeout=None
    ):
        captured["project_branch_uid"] = project_branch_uid
        captured["repo_commit_sha"] = repo_commit_sha
        captured["resource_type"] = resource_type
        return [
            {
                "uid": "957bec7b-dd77-4272-aecd-13fc2138eacc",
                "name": "tutorial_api",
                "resource_type": "fastapi",
                "path": "src/apis/tutorial_api/main.py",
                "filesize": 4096,
                "last_modified": "2026-04-05T10:30:00Z",
            }
        ]

    monkeypatch.setattr(cli_mod, "list_project_resources", _list_project_resources)

    def _create_release(**kwargs):
        captured["create_release"] = kwargs
        return {
            "uid": "2ce33c15-e3b1-4677-a66e-70460b89198f",
            "resource": kwargs["resource_uid"],
            "related_image": kwargs["related_image_uid"],
            "cpu_request": kwargs["cpu_request"],
            "memory_request": kwargs["memory_request"],
            "spot": kwargs["spot"],
        }

    monkeypatch.setattr(cli_mod, "create_project_resource_release", _create_release)

    result = runner.invoke(
        cli_mod.app,
        ["project", "project_resource", "create_fastapi"],
        input=("8b62d1dd-e146-44af-957c-38c5f5b1d8d5\n957bec7b-dd77-4272-aecd-13fc2138eacc\n"),
    )
    assert result.exit_code == 0
    assert captured["project_branch_uid"] == "project-branch-uid-123"
    assert captured["repo_commit_sha"] == "sha-94"
    assert captured["resource_type"] == "fastapi"
    assert captured["create_release"]["release_kind"] == "fastapi"
    assert captured["create_release"]["related_image_uid"] == "8b62d1dd-e146-44af-957c-38c5f5b1d8d5"
    assert captured["create_release"]["resource_uid"] == "957bec7b-dd77-4272-aecd-13fc2138eacc"
    assert captured["create_release"]["cpu_request"] == "0.25"
    assert captured["create_release"]["memory_request"] == "0.5"
    assert captured["create_release"]["spot"] is False
    assert captured["create_release"]["automatic_deployment"] is None
    assert "Using defaults: cpu_request=0.25, memory_request=0.5, spot=false." in result.output
    assert "2ce33c15-e3b1-4677-a66e-70460b89198f" in result.output


def test_project_project_resource_delete_dashboard_requires_confirmation(
    cli_mod, runner, monkeypatch
):
    captured = {}
    release_uid = "0ce33c15-e3b1-4677-a66e-70460b89198f"

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "get_resource_release",
        lambda release_uid, expected_release_kind=None, timeout=None: {
            "uid": release_uid,
            "release_kind": expected_release_kind,
            "resource": 381,
            "related_image": 94,
        },
    )

    def _delete_resource_release(release_uid, expected_release_kind=None, timeout=None):
        captured["release_uid"] = release_uid
        captured["expected_release_kind"] = expected_release_kind
        return {
            "uid": release_uid,
            "release_kind": expected_release_kind,
            "resource": 381,
            "related_image": 94,
        }

    monkeypatch.setattr(cli_mod, "delete_resource_release", _delete_resource_release)

    result = runner.invoke(
        cli_mod.app,
        ["project", "project_resource", "delete_dashboard", release_uid],
        input="y\n",
    )
    assert result.exit_code == 0
    assert captured["release_uid"] == release_uid
    assert captured["expected_release_kind"] == "streamlit_dashboard"
    assert "Project Resource Release Delete Preview" in result.output
    assert "Subdomain" not in result.output
    assert f"Delete dashboard release {release_uid}?" in result.output
    assert f"Project resource release deleted: uid={release_uid}" in result.output


def test_project_project_resource_delete_fastapi_requires_confirmation(
    cli_mod, runner, monkeypatch
):
    captured = {}
    release_uid = "2ec33c15-e3b1-4677-a66e-70460b89198f"

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "get_resource_release",
        lambda release_uid, expected_release_kind=None, timeout=None: {
            "uid": release_uid,
            "release_kind": expected_release_kind,
            "resource": 382,
            "related_image": 94,
        },
    )

    def _delete_resource_release(release_uid, expected_release_kind=None, timeout=None):
        captured["release_uid"] = release_uid
        captured["expected_release_kind"] = expected_release_kind
        return {
            "uid": release_uid,
            "release_kind": expected_release_kind,
            "resource": 382,
            "related_image": 94,
        }

    monkeypatch.setattr(cli_mod, "delete_resource_release", _delete_resource_release)

    result = runner.invoke(
        cli_mod.app,
        ["project", "project_resource", "delete_fastapi", release_uid],
        input="y\n",
    )
    assert result.exit_code == 0
    assert captured["release_uid"] == release_uid
    assert captured["expected_release_kind"] == "fastapi"
    assert "Subdomain" not in result.output
    assert f"Delete FastAPI release {release_uid}?" in result.output
    assert f"Project resource release deleted: uid={release_uid}" in result.output


def test_agent_list(cli_mod, runner, monkeypatch):
    environment_uid = "22222222-2222-4222-8222-222222222222"
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_agents",
        lambda organization_project_environment_uid, timeout=None, filters=None: [
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
        ["agent", "list", "--environment-uid", environment_uid],
    )
    assert result.exit_code == 0
    assert "Agents" in result.output
    assert "UID" in result.output
    assert "e0e756" in result.output
    assert "Resear" in result.output
    assert "Copilo" in result.output
    assert "Total agents: 1" in result.output


def test_agent_list_json(cli_mod, runner, monkeypatch):
    environment_uid = "22222222-2222-4222-8222-222222222222"
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_agents",
        lambda organization_project_environment_uid, timeout=None, filters=None: [
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
        ["agent", "list", "--environment-uid", environment_uid, "--json"],
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload[0]["uid"] == "e0e75693-4110-464c-93e0-82c7fd9c9a23"
    assert payload[0]["llm_model"] == "gpt-5.4"


def test_agent_search(cli_mod, runner, monkeypatch):
    captured = {}
    environment_uid = "22222222-2222-4222-8222-222222222222"
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _search(q, *, organization_project_environment_uid, limit=20, timeout=None):
        captured.update(
            {
                "q": q,
                "organization_project_environment_uid": organization_project_environment_uid,
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
            "--environment-uid",
            environment_uid,
            "--limit",
            "10",
            "--timeout",
            "17",
        ],
    )
    assert result.exit_code == 0
    assert captured == {
        "q": "data research",
        "organization_project_environment_uid": environment_uid,
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
    environment_uid = "22222222-2222-4222-8222-222222222222"
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
            "--environment-uid",
            environment_uid,
            "--json",
        ],
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload[0]["uid"] == "e0e75693-4110-464c-93e0-82c7fd9c9a23"
    assert payload[0]["combined_score"] == 0.85


def test_agent_create_parses_json_fields(cli_mod, runner, monkeypatch):
    captured = {}
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _create(**kwargs):
        captured.update(kwargs)
        return {
            "id": 12,
            "name": kwargs["name"],
            "status": kwargs.get("status") or "draft",
            "labels": kwargs.get("labels") or [],
            "llm_provider": kwargs.get("llm_provider") or "",
            "llm_model": kwargs.get("llm_model") or "",
            "engine_name": kwargs.get("engine_name") or "",
        }

    monkeypatch.setattr(cli_mod, "create_agent", _create)

    result = runner.invoke(
        cli_mod.app,
        [
            "agent",
            "create",
            "Research Copilot",
            "--description",
            "Desk agent",
            "--status",
            "active",
            "--label",
            "research,desk",
            "--llm-provider",
            "openai",
            "--llm-model",
            "gpt-5.4",
            "--engine-name",
            "codex",
            "--runtime-config",
            '{"temperature":0}',
            "--configuration",
            '{"mode":"analysis"}',
            "--metadata",
            '{"owner":"quant"}',
        ],
    )
    assert result.exit_code == 0
    assert captured["name"] == "Research Copilot"
    assert captured["status"] == "active"
    assert captured["labels"] == ["research", "desk"]
    assert captured["runtime_config"] == {"temperature": 0}
    assert captured["configuration"] == {"mode": "analysis"}
    assert captured["metadata"] == {"owner": "quant"}
    assert "Agent created: Research Copilot" in result.output


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
                "role": "ROLE_AGENT",
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
    assert captured["kwargs"]["return_immediately"] is False
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
                "role": "ROLE_AGENT",
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


def test_constants_list(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_constants",
        lambda filters=None, timeout=None: [
            {
                "uid": "11111111-1111-4111-8111-111111111111",
                "name": "ASSETS__MASTER",
                "value": {"source": "bbg"},
            }
        ],
    )

    result = runner.invoke(cli_mod.app, ["constants", "list"])
    assert result.exit_code == 0
    assert "Constants" in result.output
    assert "ASSETS__MASTER" in result.output
    assert "ASSETS" in result.output
    assert "Total constants: 1" in result.output


def test_constants_list_passes_cli_filters(cli_mod, runner, monkeypatch):
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _parse(model_ref, entries):
        captured["entries"] = list(entries or [])
        return {"name__in": ["ASSETS__MASTER", "APP__MODE"]}

    def _list(timeout=None, filters=None):
        captured["filters"] = filters
        return []

    monkeypatch.setattr(cli_mod, "parse_cli_model_filters", _parse)
    monkeypatch.setattr(cli_mod, "list_constants", _list)

    result = runner.invoke(
        cli_mod.app,
        ["constants", "list", "--filter", "name__in=ASSETS__MASTER,APP__MODE"],
    )
    assert result.exit_code == 0
    assert captured["entries"] == ["name__in=ASSETS__MASTER,APP__MODE"]
    assert captured["filters"] == {"name__in": ["ASSETS__MASTER", "APP__MODE"]}


def test_constants_create_parses_json_value(cli_mod, runner, monkeypatch):
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _create(*, name, value, timeout=None):
        captured["name"] = name
        captured["value"] = value
        captured["timeout"] = timeout
        return {"uid": "11111111-1111-4111-8111-111111111111", "name": name, "value": value}

    monkeypatch.setattr(cli_mod, "create_constant", _create)

    result = runner.invoke(
        cli_mod.app,
        ["constants", "create", "ASSETS__MASTER", '{"source":"bbg"}'],
    )
    assert result.exit_code == 0
    assert captured["name"] == "ASSETS__MASTER"
    assert captured["value"] == {"source": "bbg"}
    assert "Constant created: ASSETS__MASTER" in result.output
    assert "Created Constant" in result.output
    assert "ASSETS" in result.output


def test_constants_delete_requires_typed_verification(cli_mod, runner, monkeypatch):
    captured = {}
    constant_uid = "11111111-1111-4111-8111-111111111111"

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "get_constant",
        lambda constant_uid, timeout=None: {
            "uid": constant_uid,
            "name": "ASSETS__MASTER",
            "value": {"source": "bbg"},
        },
    )

    def _delete(constant_uid, timeout=None):
        captured["constant_uid"] = constant_uid
        captured["timeout"] = timeout
        return {
            "uid": constant_uid,
            "name": "ASSETS__MASTER",
            "value": {"source": "bbg"},
        }

    monkeypatch.setattr(cli_mod, "delete_constant", _delete)

    result = runner.invoke(
        cli_mod.app,
        ["constants", "delete", constant_uid],
        input="ASSETS__MASTER\n",
    )
    assert result.exit_code == 0
    assert "Constant Delete Preview" in result.output
    assert "Type constant name 'ASSETS__MASTER' to confirm deletion" in result.output
    assert captured["constant_uid"] == constant_uid
    assert f"Constant deleted: uid={constant_uid}" in result.output


def test_constants_can_edit(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_constant_users_can_edit",
        lambda constant_uid, timeout=None: {
            "object_uid": constant_uid,
            "object_type": "tdag.constant",
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

    result = runner.invoke(
        cli_mod.app,
        ["constants", "can_edit", "11111111-1111-4111-8111-111111111111"],
    )
    assert result.exit_code == 0
    assert "Constant Users Who Can Edit" in result.output
    assert "Constant Teams Who Can Edit" in result.output
    assert "editor" in result.output
    assert "editor@example.com" in result.output
    assert "Total users who can edit: 1" in result.output
    assert "Total teams who can edit: 1" in result.output


def test_constants_add_to_edit(cli_mod, runner, monkeypatch):
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _add(constant_uid, user_uid, timeout=None):
        captured["constant_uid"] = constant_uid
        captured["user_uid"] = user_uid
        captured["timeout"] = timeout
        return {
            "ok": True,
            "action": "add_to_edit",
            "detail": "User now has explicit edit access.",
            "object_uid": constant_uid,
            "object_type": "tdag.constant",
            "user": {
                "uid": user_uid,
                "username": "editor",
                "email": "editor@example.com",
                "first_name": "Edit",
                "last_name": "User",
            },
            "explicit_can_view": True,
            "explicit_can_edit": True,
            "explicit_can_view_user_uids": [user_uid],
            "explicit_can_edit_user_uids": [user_uid],
        }

    monkeypatch.setattr(cli_mod, "add_constant_user_to_edit", _add)

    constant_uid = "11111111-1111-4111-8111-111111111111"
    result = runner.invoke(
        cli_mod.app,
        ["constants", "add_to_edit", constant_uid, USER_UID],
    )
    assert result.exit_code == 0
    assert captured == {
        "constant_uid": constant_uid,
        "user_uid": USER_UID,
        "timeout": None,
    }
    assert "Constant add_to_edit completed." in result.output
    assert "Constant Sharing Update" in result.output
    assert "editor@example.com" in result.output


def test_secrets_list(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_secrets",
        lambda filters=None, timeout=None: [
            {
                "uid": "498d499f-b74c-43f7-acf1-2e2955ad0e6b",
                "name": "API_KEY",
            }
        ],
    )

    result = runner.invoke(cli_mod.app, ["secrets", "list"])
    assert result.exit_code == 0
    assert "Secrets" in result.output
    assert "UID" in result.output
    assert "API_KEY" in result.output
    assert "Total secrets: 1" in result.output


def test_secrets_list_passes_cli_filters(cli_mod, runner, monkeypatch):
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _parse(model_ref, entries):
        captured["entries"] = list(entries or [])
        return {"name__in": ["API_KEY", "DB_PASSWORD"]}

    def _list(timeout=None, filters=None):
        captured["filters"] = filters
        return []

    monkeypatch.setattr(cli_mod, "parse_cli_model_filters", _parse)
    monkeypatch.setattr(cli_mod, "list_secrets", _list)

    result = runner.invoke(
        cli_mod.app,
        ["secrets", "list", "--filter", "name__in=API_KEY,DB_PASSWORD"],
    )
    assert result.exit_code == 0
    assert captured["entries"] == ["name__in=API_KEY,DB_PASSWORD"]
    assert captured["filters"] == {"name__in": ["API_KEY", "DB_PASSWORD"]}


def test_secrets_create_hides_value_in_output(cli_mod, runner, monkeypatch):
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _create(*, name, value, timeout=None):
        captured["name"] = name
        captured["value"] = value
        captured["timeout"] = timeout
        return {"uid": "498d499f-b74c-43f7-acf1-2e2955ad0e6b", "name": name}

    monkeypatch.setattr(cli_mod, "create_secret", _create)

    result = runner.invoke(
        cli_mod.app,
        ["secrets", "create", "API_KEY", "super-secret"],
    )
    assert result.exit_code == 0
    assert captured["name"] == "API_KEY"
    assert captured["value"] == "super-secret"
    assert "Secret created: API_KEY" in result.output
    assert "Created Secret" in result.output
    assert "super-secret" not in result.output


def test_secrets_delete_requires_typed_verification(cli_mod, runner, monkeypatch):
    captured = {}
    secret_uid = "498d499f-b74c-43f7-acf1-2e2955ad0e6b"

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "get_secret",
        lambda secret_uid, timeout=None: {
            "uid": secret_uid,
            "name": "API_KEY",
        },
    )

    def _delete(secret_uid, timeout=None):
        captured["secret_uid"] = secret_uid
        captured["timeout"] = timeout
        return {
            "uid": secret_uid,
            "name": "API_KEY",
        }

    monkeypatch.setattr(cli_mod, "delete_secret", _delete)

    result = runner.invoke(
        cli_mod.app,
        ["secrets", "delete", secret_uid],
        input="API_KEY\n",
    )
    assert result.exit_code == 0
    assert "Secret Delete Preview" in result.output
    assert "Type secret name 'API_KEY' to confirm deletion" in result.output
    assert captured["secret_uid"] == secret_uid
    assert f"Secret deleted: uid={secret_uid}" in result.output


def test_secrets_can_view(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_secret_users_can_view",
        lambda secret_uid, timeout=None: {
            "object_uid": secret_uid,
            "object_type": "tdag.secret",
            "access_level": "view",
            "users": [
                {
                    "id": 11,
                    "username": "viewer",
                    "email": "viewer@example.com",
                    "first_name": "View",
                    "last_name": "User",
                }
            ],
            "teams": [],
        },
    )

    result = runner.invoke(
        cli_mod.app,
        ["secrets", "can_view", "498d499f-b74c-43f7-acf1-2e2955ad0e6b"],
    )
    assert result.exit_code == 0
    assert "Secret Users Who Can View" in result.output
    assert "viewer@example.com" in result.output
    assert "Total users who can view: 1" in result.output
    assert "Total teams who can view: 0" in result.output


def test_secrets_add_to_edit(cli_mod, runner, monkeypatch):
    captured = {}
    secret_uid = "498d499f-b74c-43f7-acf1-2e2955ad0e6b"

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _add(secret_uid, user_uid, timeout=None):
        captured["secret_uid"] = secret_uid
        captured["user_uid"] = user_uid
        captured["timeout"] = timeout
        return {
            "ok": True,
            "action": "add_to_edit",
            "detail": "User now has explicit edit access.",
            "object_uid": secret_uid,
            "object_type": "tdag.secret",
            "user": {
                "uid": user_uid,
                "username": "editor",
                "email": "editor@example.com",
                "first_name": "Edit",
                "last_name": "User",
            },
            "explicit_can_view": True,
            "explicit_can_edit": True,
            "explicit_can_view_user_uids": [user_uid],
            "explicit_can_edit_user_uids": [user_uid],
        }

    monkeypatch.setattr(cli_mod, "add_secret_user_to_edit", _add)

    result = runner.invoke(
        cli_mod.app,
        ["secrets", "add_to_edit", secret_uid, USER_UID],
    )
    assert result.exit_code == 0
    assert captured == {
        "secret_uid": secret_uid,
        "user_uid": USER_UID,
        "timeout": None,
    }
    assert "Secret add_to_edit completed." in result.output
    assert "Secret Sharing Update" in result.output
    assert "editor@example.com" in result.output


def test_data_node_storage_list(cli_mod, runner, monkeypatch):
    captured = {}
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _list(filters=None, timeout=None):
        captured["filters"] = filters
        captured["timeout"] = timeout
        return [
            {
                "uid": "data-node-storage-42",
                "physical_table_name": "weights_daily_physical",
                "source_class_name": "NodeWeights",
                "identifier": "weights_daily",
                "namespace": "pytest_weights",
                "data_source": {"display_name": "Default DB", "class_type": "timescale_db"},
            }
        ]

    monkeypatch.setattr(cli_mod, "list_data_node_storages", _list)

    result = runner.invoke(cli_mod.app, ["data-node", "list"])
    assert result.exit_code == 0
    assert captured == {
        "filters": {},
        "timeout": None,
    }
    assert "Data Node Storages" in result.output
    assert "weights_" in result.output
    assert "Node" in result.output
    assert "NodeWeig" in result.output
    assert "Namespac" in result.output
    assert "pytest_w" in result.output
    assert "Default" in result.output
    assert "DB" in result.output
    assert "Total data node storages: 1" in result.output


def test_meta_table_list_uses_canonical_command(cli_mod, runner, monkeypatch):
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _parse(model_ref, entries):
        captured["model_ref"] = model_ref
        captured["entries"] = list(entries or [])
        return {"namespace": "pytest_weights"}

    def _list(timeout=None, filters=None):
        captured["timeout"] = timeout
        captured["filters"] = filters
        return [
            {
                "uid": "meta-table-42",
                "physical_table_name": "weights_daily_physical",
                "identifier": "weights_daily",
                "namespace": "pytest_weights",
                "management_mode": "platform_managed",
                "data_source": {"display_name": "Default DB", "class_type": "timescale_db"},
            }
        ]

    monkeypatch.setattr(cli_mod, "parse_cli_model_filters", _parse)
    monkeypatch.setattr(cli_mod, "list_meta_tables", _list)

    result = runner.invoke(
        cli_mod.app,
        [
            "meta-table",
            "list",
            "--filter",
            "namespace=pytest_weights",
            "--data-source-uid",
            "data-source-1",
            "--timeout",
            "15",
        ],
    )

    assert result.exit_code == 0
    assert captured == {
        "model_ref": "mainsequence.client.metatables.MetaTable",
        "entries": ["namespace=pytest_weights"],
        "timeout": 15,
        "filters": {
            "namespace": "pytest_weights",
            "data_source__uid": "data-source-1",
        },
    }
    assert "MetaTables" in result.output
    assert "weights_" in result.output
    assert "platform" in result.output
    assert "Total MetaTables: 1" in result.output


def test_meta_table_list_rejects_environment_selector(cli_mod, runner):
    result = runner.invoke(
        cli_mod.app,
        [
            "meta-table",
            "list",
            "--organization-project-environment-uid",
            "22222222-2222-4222-8222-222222222222",
        ],
    )

    assert result.exit_code == 2
    assert "No such option" in result.output


def test_meta_table_detail(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "get_meta_table",
        lambda meta_table_uid, timeout=None: {
            "uid": meta_table_uid,
            "physical_table_name": "weights_daily_physical",
            "identifier": "weights_daily",
            "namespace": "pytest_weights",
            "management_mode": "platform_managed",
            "data_source": {"display_name": "Default DB", "class_type": "timescale_db"},
            "protect_from_deletion": True,
            "contract_version": "relational-table.v1",
            "table_contract": {"columns": [{"name": "time_index"}]},
            "columns": [{"name": "time_index", "data_type": "timestamp with time zone"}],
            "introspection_snapshot": {"row_count": 7},
        },
    )

    result = runner.invoke(cli_mod.app, ["meta-table", "detail", "meta-table-42"])

    assert result.exit_code == 0
    assert "MetaTable" in result.output
    assert "weights_daily" in result.output
    assert "MetaTable Contract" in result.output
    assert "relational-table.v1" in result.output
    assert "time_index" in result.output


def test_meta_table_command_exposes_storage_help(cli_mod, runner):
    meta_result = runner.invoke(cli_mod.app, ["meta-table", "--help"])

    assert meta_result.exit_code == 0
    assert "list" in meta_result.output
    assert "detail" in meta_result.output
    assert "delete" in meta_result.output
    assert "can_view" in meta_result.output
    assert "add-label" in meta_result.output


def test_data_node_storage_list_forwards_namespace_filter(cli_mod, runner, monkeypatch):
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _fake_list_data_node_storages(filters=None, timeout=None):
        captured["timeout"] = timeout
        captured["filters"] = filters
        return []

    monkeypatch.setattr(cli_mod, "list_data_node_storages", _fake_list_data_node_storages)

    result = runner.invoke(
        cli_mod.app,
        ["data-node", "list", "--filter", "namespace=pytest_weights"],
    )

    assert result.exit_code == 0
    assert captured == {
        "timeout": None,
        "filters": {
            "namespace": "pytest_weights",
        },
    }


def test_project_validate_name_cmd(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "validate_project_name",
        lambda project_name, timeout=None: {
            "project_name": project_name,
            "available": False,
            "reason": "A project with this name already exists in your organization.",
            "normalized": {
                "slugified_project_name": "rates-platform",
                "project_library_name": "rates_platform",
            },
            "suggestions": ["Rates Platform 2", "Rates Platform 3"],
        },
    )

    result = runner.invoke(cli_mod.app, ["project", "validate-name", "Rates Platform"])
    assert result.exit_code == 1
    assert "Project Name Validation" in result.output
    assert "rates-platform" in result.output
    assert "rates_platform" in result.output
    assert "Rates Platform 2" in result.output
    assert "Rates Platform 3" in result.output


def test_data_node_storage_list_passes_cli_filters(cli_mod, runner, monkeypatch):
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _parse(model_ref, entries):
        captured["entries"] = list(entries or [])
        return {"uid__in": ["data-node-storage-42", "data-node-storage-43"]}

    def _list(timeout=None, filters=None):
        captured["filters"] = filters
        return []

    monkeypatch.setattr(cli_mod, "parse_cli_model_filters", _parse)
    monkeypatch.setattr(cli_mod, "list_data_node_storages", _list)

    result = runner.invoke(
        cli_mod.app,
        ["data-node", "list", "--filter", "uid__in=data-node-storage-42,data-node-storage-43"],
    )
    assert result.exit_code == 0
    assert captured["entries"] == ["uid__in=data-node-storage-42,data-node-storage-43"]
    assert captured["filters"] == {
        "uid__in": ["data-node-storage-42", "data-node-storage-43"],
    }


def test_data_node_storage_search_supports_data_source_uid_option(cli_mod, runner, monkeypatch):
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(cli_mod, "parse_cli_model_filters", lambda model_ref, entries: {})

    def _description(
        q,
        *,
        q_embedding=None,
        trigram_k=200,
        embed_k=200,
        w_trgm=0.65,
        w_emb=0.35,
        embedding_model="default",
        filters=None,
    ):
        captured["filters"] = filters
        return {"count": 0, "next": None, "previous": None, "results": []}

    monkeypatch.setattr(cli_mod, "data_node_storage_description_search", _description)
    monkeypatch.setattr(cli_mod, "data_node_storage_column_search", lambda q, *, filters=None: [])

    result = runner.invoke(
        cli_mod.app,
        [
            "data_node",
            "search",
            "close price",
            "--mode",
            "description",
            "--data-source-uid",
            "data-source-uid-2",
        ],
    )
    assert result.exit_code == 0
    assert captured["filters"] == {"data_source__uid": "data-source-uid-2"}


def test_data_node_storage_search_rejects_conflicting_data_source_filters(
    cli_mod, runner, monkeypatch
):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _parse(model_ref, entries):
        return {"data_source__uid": "data-source-uid-9"}

    monkeypatch.setattr(cli_mod, "parse_cli_model_filters", _parse)

    result = runner.invoke(
        cli_mod.app,
        [
            "data-node",
            "search",
            "close price",
            "--data-source-uid",
            "data-source-uid-2",
            "--filter",
            "data_source__uid=data-source-uid-9",
        ],
    )
    assert result.exit_code == 1
    assert (
        "Do not pass both `--data-source-uid` and `--filter data_source__uid=...`." in result.output
    )


def test_data_node_storage_description_search(cli_mod, runner, monkeypatch):
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _parse(model_ref, entries):
        captured["entries"] = list(entries or [])
        return {}

    def _search(
        q,
        *,
        q_embedding=None,
        trigram_k=200,
        embed_k=200,
        w_trgm=0.65,
        w_emb=0.35,
        embedding_model="default",
        filters=None,
    ):
        captured["search"] = {
            "q": q,
            "q_embedding": q_embedding,
            "trigram_k": trigram_k,
            "embed_k": embed_k,
            "w_trgm": w_trgm,
            "w_emb": w_emb,
            "embedding_model": embedding_model,
            "filters": filters,
        }
        return {
            "count": 3,
            "next": "https://backend.test/page/2",
            "previous": None,
            "results": [
                {
                    "uid": "data-node-storage-42",
                    "physical_table_name": "weights_daily_physical",
                    "source_class_name": "NodeWeights",
                    "identifier": "weights_daily",
                    "data_source": {"display_name": "Default DB", "class_type": "timescale_db"},
                }
            ],
        }

    monkeypatch.setattr(cli_mod, "parse_cli_model_filters", _parse)
    monkeypatch.setattr(cli_mod, "data_node_storage_description_search", _search)

    result = runner.invoke(
        cli_mod.app,
        [
            "data-node",
            "description-search",
            "node weights",
            "--data-source-uid",
            "data-source-uid-2",
            "--q-embedding",
            "0.1,0.2",
            "--trigram-k",
            "150",
            "--embed-k",
            "180",
            "--w-trgm",
            "0.7",
            "--w-emb",
            "0.3",
            "--embedding-model",
            "text-embedding-3-large",
        ],
    )
    assert result.exit_code == 0
    assert captured["entries"] == []
    assert captured["search"] == {
        "q": "node weights",
        "q_embedding": [0.1, 0.2],
        "trigram_k": 150,
        "embed_k": 180,
        "w_trgm": 0.7,
        "w_emb": 0.3,
        "embedding_model": "text-embedding-3-large",
        "filters": {"data_source__uid": "data-source-uid-2"},
    }
    assert "Description Matches" in result.output
    assert "weights_" in result.output
    assert "Pagination" in result.output
    assert "Count" in result.output


def test_data_node_storage_column_search(cli_mod, runner, monkeypatch):
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _parse(model_ref, entries):
        captured["entries"] = list(entries or [])
        return {"physical_table_name__contains": "weights"}

    def _search(q, *, filters=None):
        captured["search"] = {"q": q, "filters": filters}
        return [
            {
                "uid": "data-node-storage-43",
                "physical_table_name": "prices_daily_physical",
                "source_class_name": "PriceBars",
                "identifier": "prices_daily",
                "data_source": {"display_name": "Default DB", "class_type": "timescale_db"},
            }
        ]

    monkeypatch.setattr(cli_mod, "parse_cli_model_filters", _parse)
    monkeypatch.setattr(cli_mod, "data_node_storage_column_search", _search)

    result = runner.invoke(
        cli_mod.app,
        [
            "data-node",
            "column-search",
            "close",
            "--filter",
            "physical_table_name__contains=weights",
        ],
    )
    assert result.exit_code == 0
    assert captured["entries"] == ["physical_table_name__contains=weights"]
    assert captured["search"] == {
        "q": "close",
        "filters": {"physical_table_name__contains": "weights"},
    }
    assert "Column Matches" in result.output
    assert "prices_d" in result.output
    assert 'Column Matches: 1 match(es) for "close"' in result.output


def test_data_node_storage_search_defaults_to_description(cli_mod, runner, monkeypatch):
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _parse(model_ref, entries):
        captured["entries"] = list(entries or [])
        return {}

    def _description(
        q,
        *,
        q_embedding=None,
        trigram_k=200,
        embed_k=200,
        w_trgm=0.65,
        w_emb=0.35,
        embedding_model="default",
        filters=None,
    ):
        captured["description"] = {
            "q": q,
            "q_embedding": q_embedding,
            "trigram_k": trigram_k,
            "embed_k": embed_k,
            "w_trgm": w_trgm,
            "w_emb": w_emb,
            "embedding_model": embedding_model,
            "filters": filters,
        }
        return {
            "count": 2,
            "next": None,
            "previous": None,
            "results": [
                {
                    "uid": "data-node-storage-42",
                    "physical_table_name": "weights_daily_physical",
                    "source_class_name": "NodeWeights",
                    "identifier": "weights_daily",
                    "data_source": {"display_name": "Default DB", "class_type": "timescale_db"},
                }
            ],
        }

    monkeypatch.setattr(cli_mod, "parse_cli_model_filters", _parse)
    monkeypatch.setattr(cli_mod, "data_node_storage_description_search", _description)
    monkeypatch.setattr(
        cli_mod,
        "data_node_storage_column_search",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("column search should not run by default")
        ),
    )

    result = runner.invoke(
        cli_mod.app,
        [
            "data_node",
            "search",
            "close price",
            "--data-source-uid",
            "data-source-uid-2",
            "--q-embedding",
            "0.1,0.2",
        ],
    )
    assert result.exit_code == 0
    assert captured["entries"] == []
    assert captured["description"]["q"] == "close price"
    assert captured["description"]["q_embedding"] == [0.1, 0.2]
    assert captured["description"]["filters"] == {"data_source__uid": "data-source-uid-2"}
    assert "Description Matches" in result.output
    assert "Column Matches" not in result.output
    assert 'Total search matches for "close price": 1' in result.output


def test_data_node_storage_search_both_mode_combines_description_and_column(
    cli_mod, runner, monkeypatch
):
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _parse(model_ref, entries):
        captured["entries"] = list(entries or [])
        return {}

    def _description(
        q,
        *,
        q_embedding=None,
        trigram_k=200,
        embed_k=200,
        w_trgm=0.65,
        w_emb=0.35,
        embedding_model="default",
        filters=None,
    ):
        captured["description"] = {
            "q": q,
            "q_embedding": q_embedding,
            "filters": filters,
        }
        return {
            "count": 2,
            "next": None,
            "previous": None,
            "results": [
                {
                    "uid": "data-node-storage-42",
                    "physical_table_name": "weights_daily_physical",
                    "identifier": "weights_daily",
                }
            ],
        }

    def _column(q, *, filters=None):
        captured["column"] = {"q": q, "filters": filters}
        return [
            {
                "uid": "data-node-storage-43",
                "physical_table_name": "prices_daily_physical",
                "identifier": "prices_daily",
            }
        ]

    monkeypatch.setattr(cli_mod, "parse_cli_model_filters", _parse)
    monkeypatch.setattr(cli_mod, "data_node_storage_description_search", _description)
    monkeypatch.setattr(cli_mod, "data_node_storage_column_search", _column)

    result = runner.invoke(
        cli_mod.app,
        [
            "data_node",
            "search",
            "close price",
            "--mode",
            "both",
            "--data-source-uid",
            "data-source-uid-2",
            "--q-embedding",
            "0.1,0.2",
        ],
    )
    assert result.exit_code == 0
    assert captured["entries"] == []
    assert captured["description"] == {
        "q": "close price",
        "q_embedding": [0.1, 0.2],
        "filters": {"data_source__uid": "data-source-uid-2"},
    }
    assert captured["column"] == {
        "q": "close price",
        "filters": {"data_source__uid": "data-source-uid-2"},
    }
    assert "Description Matches" in result.output
    assert "Column Matches" in result.output
    assert 'Total search matches for "close price": 2' in result.output


def test_data_node_storage_search_column_mode_only(cli_mod, runner, monkeypatch):
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(cli_mod, "parse_cli_model_filters", lambda model_ref, entries: {})
    monkeypatch.setattr(
        cli_mod,
        "data_node_storage_description_search",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("description search should not run")
        ),
    )

    def _column(q, *, filters=None):
        captured["column"] = {"q": q, "filters": filters}
        return []

    monkeypatch.setattr(cli_mod, "data_node_storage_column_search", _column)

    result = runner.invoke(cli_mod.app, ["data-node", "search", "close", "--mode", "column"])
    assert result.exit_code == 0
    assert captured["column"] == {"q": "close", "filters": {}}
    assert 'Column Matches: 0 match(es) for "close"' in result.output


def test_data_node_storage_detail(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "get_data_node_storage",
        lambda storage_uid, timeout=None: {
            "uid": storage_uid,
            "physical_table_name": "weights_daily_physical",
            "identifier": "weights_daily",
            "source_class_name": "NodeWeights",
            "data_source": {"display_name": "Default DB", "class_type": "timescale_db"},
            "protect_from_deletion": True,
            "creation_date": "2026-03-16T10:00:00Z",
            "created_by_user": 7,
            "organization_owner": 2,
            "description": "Daily node weights",
            "time_indexed_profile": {
                "time_index_name": "time_index",
                "storage_layout": {
                    "time_index": "time_index",
                    "identity_dimensions": ["account_uid", "unique_identifier"],
                },
                "physical_index_plan": {
                    "uniqueness": {"columns": ["time_index", "account_uid", "unique_identifier"]}
                },
            },
            "table_index_names": {"0": "time_index"},
            "compression_policy_config": {"after": "7 days"},
            "retention_policy_config": {"after": "90 days"},
        },
    )

    result = runner.invoke(cli_mod.app, ["data-node", "detail", "data-node-storage-42"])
    assert result.exit_code == 0
    assert "Data Node Storage" in result.output
    assert "weights_daily" in result.output
    assert "weights_daily_physical" in result.output
    assert "Daily node weights" in result.output
    assert "Build Configuration" not in result.output
    assert "time_index_name" in result.output
    assert "Storage Layout" in result.output
    assert "identity_dimensions" in result.output
    assert "Physical Index Plan" in result.output
    assert "uniqueness" in result.output
    assert "90 days" in result.output


def test_run_data_node_storage_query_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    def _run_sdk_model_operation(*, module_name, class_name, operation, project_id_env=None):
        captured["module_name"] = module_name
        captured["class_name"] = class_name

        class _ClientDataNodeStorage:
            @classmethod
            def get(cls, uid, timeout=None):
                captured["uid"] = uid
                captured["timeout"] = timeout

                class _Storage:
                    def run_query(self, sql, *, timeout=None):
                        captured["sql"] = sql
                        captured["query_timeout"] = timeout
                        return {
                            "ok": True,
                            "query_id": "query-456",
                            "time_index_meta_table_uid": uid,
                            "results": [{"value": 1}],
                            "truncated": False,
                            "max_rows": 0,
                            "row_count": 1,
                            "error": None,
                        }

                return _Storage()

        return operation(_ClientDataNodeStorage)

    monkeypatch.setattr(api_mod, "_run_sdk_model_operation", _run_sdk_model_operation)

    out = api_mod.run_data_node_storage_query(
        "data-node-storage-42", "SELECT 1 AS value", timeout=14
    )
    assert captured == {
        "module_name": "mainsequence.client.metatables",
        "class_name": "TimeIndexMetaTable",
        "uid": "data-node-storage-42",
        "timeout": 14,
        "sql": "SELECT 1 AS value",
        "query_timeout": 14,
    }
    assert out["ok"] is True
    assert out["time_index_meta_table_uid"] == "data-node-storage-42"
    assert out["results"] == [{"value": 1}]


def test_run_meta_table_query_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    def _run_sdk_model_operation(*, module_name, class_name, operation, project_id_env=None):
        captured["module_name"] = module_name
        captured["class_name"] = class_name

        class _ClientMetaTable:
            @classmethod
            def get(cls, uid, timeout=None):
                captured["uid"] = uid
                captured["timeout"] = timeout

                class _MetaTable:
                    def run_query(self, sql, *, timeout=None):
                        captured["sql"] = sql
                        captured["query_timeout"] = timeout
                        return {
                            "ok": True,
                            "query_id": "query-789",
                            "meta_table_uid": uid,
                            "results": [{"value": 2}],
                            "truncated": False,
                            "max_rows": 1000,
                            "row_count": 1,
                            "error": None,
                        }

                return _MetaTable()

        return operation(_ClientMetaTable)

    monkeypatch.setattr(api_mod, "_run_sdk_model_operation", _run_sdk_model_operation)

    out = api_mod.run_meta_table_query("meta-table-42", "SELECT 2 AS value", timeout=16)
    assert captured == {
        "module_name": "mainsequence.client.metatables",
        "class_name": "MetaTable",
        "uid": "meta-table-42",
        "timeout": 16,
        "sql": "SELECT 2 AS value",
        "query_timeout": 16,
    }
    assert out["ok"] is True
    assert out["meta_table_uid"] == "meta-table-42"
    assert out["results"] == [{"value": 2}]


def test_data_node_storage_run_query(cli_mod, runner, monkeypatch):
    captured = {}
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _run_query(storage_uid, sql, *, timeout=None):
        captured["storage_uid"] = storage_uid
        captured["sql"] = sql
        captured["timeout"] = timeout
        return {
            "ok": True,
            "query_id": "query-456",
            "time_index_meta_table_uid": storage_uid,
            "results": [{"value": 1}],
            "truncated": False,
            "max_rows": 0,
            "row_count": 1,
            "error": None,
        }

    monkeypatch.setattr(cli_mod, "run_data_node_storage_query", _run_query)

    result = runner.invoke(
        cli_mod.app,
        ["data-node", "run_query", "data-node-storage-42", "SELECT 1 AS value", "--timeout", "15"],
    )
    assert result.exit_code == 0
    assert captured == {
        "storage_uid": "data-node-storage-42",
        "sql": "SELECT 1 AS value",
        "timeout": 15,
    }
    assert "Data node query completed: uid=data-node-storage-42" in result.output
    assert "Data Node Query" in result.output
    assert "query-456" in result.output
    assert "Time Index MetaTable UID" in result.output
    assert "data-node-storage-42" in result.output
    assert '"value": 1' in result.output


def test_meta_table_run_query(cli_mod, runner, monkeypatch):
    captured = {}
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _run_query(meta_table_uid, sql, *, timeout=None):
        captured["meta_table_uid"] = meta_table_uid
        captured["sql"] = sql
        captured["timeout"] = timeout
        return {
            "ok": True,
            "query_id": "query-789",
            "meta_table_uid": meta_table_uid,
            "results": [{"value": 2}],
            "truncated": False,
            "max_rows": 1000,
            "row_count": 1,
            "error": None,
        }

    monkeypatch.setattr(cli_mod, "run_meta_table_query", _run_query)

    result = runner.invoke(
        cli_mod.app,
        ["meta-table", "run_query", "meta-table-42", "SELECT 2 AS value", "--timeout", "15"],
    )
    assert result.exit_code == 0
    assert captured == {
        "meta_table_uid": "meta-table-42",
        "sql": "SELECT 2 AS value",
        "timeout": 15,
    }
    assert "MetaTable query completed: uid=meta-table-42" in result.output
    assert "MetaTable Query" in result.output
    assert "query-789" in result.output
    assert '"value": 2' in result.output


def test_data_node_storage_refresh_search_index(cli_mod, runner, monkeypatch):
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _refresh(storage_uid, timeout=None):
        captured["storage_uid"] = storage_uid
        captured["timeout"] = timeout
        return {
            "uid": storage_uid,
            "status": "queued",
            "message": "refresh started",
        }

    monkeypatch.setattr(cli_mod, "refresh_data_node_storage_search_index", _refresh)

    result = runner.invoke(
        cli_mod.app,
        ["data-node", "refresh-search-index", "data-node-storage-42", "--timeout", "15"],
    )
    assert result.exit_code == 0
    assert captured == {"storage_uid": "data-node-storage-42", "timeout": 15}
    assert "Data node search index refresh requested: uid=data-node-storage-42" in result.output
    assert "Data Node Search Index Refresh" in result.output
    assert "queued" in result.output
    assert "refresh started" in result.output


def test_data_node_storage_can_view(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_data_node_storage_users_can_view",
        lambda storage_uid, timeout=None: {
            "access_level": "view",
            "users": [
                {
                    "id": 8,
                    "username": "viewer",
                    "email": "viewer@example.com",
                    "first_name": "View",
                    "last_name": "User",
                }
            ],
            "teams": [],
        },
    )

    result = runner.invoke(cli_mod.app, ["data-node", "can_view", "data-node-storage-42"])
    assert result.exit_code == 0
    assert "Data Node Users Who Can View" in result.output
    assert "viewer@example.com" in result.output
    assert "Total users who can view: 1" in result.output


def test_data_node_storage_add_label(cli_mod, runner, monkeypatch):
    captured = {}
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _add(storage_uid, labels, timeout=None):
        captured["storage_uid"] = storage_uid
        captured["labels"] = labels
        captured["timeout"] = timeout
        return {"labels": [{"name": "curated"}]}

    monkeypatch.setattr(cli_mod, "add_data_node_storage_labels", _add)

    result = runner.invoke(
        cli_mod.app,
        ["data-node", "add-label", "data-node-storage-42", "--label", "curated"],
    )
    assert result.exit_code == 0
    assert captured == {
        "storage_uid": "data-node-storage-42",
        "labels": ["curated"],
        "timeout": None,
    }
    assert "Data Node add-label completed." in result.output
    assert "curated" in result.output


def test_data_node_storage_add_to_edit(cli_mod, runner, monkeypatch):
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _add(storage_uid, user_uid, timeout=None):
        captured["storage_uid"] = storage_uid
        captured["user_uid"] = user_uid
        captured["timeout"] = timeout
        return {
            "ok": True,
            "action": "add_to_edit",
            "detail": "User now has explicit edit access.",
            "object_uid": storage_uid,
            "object_type": "tdag.datanodestorage",
            "user": {
                "uid": user_uid,
                "username": "editor",
                "email": "editor@example.com",
                "first_name": "Edit",
                "last_name": "User",
            },
            "explicit_can_view": True,
            "explicit_can_edit": True,
            "explicit_can_view_user_uids": [user_uid],
            "explicit_can_edit_user_uids": [user_uid],
        }

    monkeypatch.setattr(cli_mod, "add_data_node_storage_user_to_edit", _add)

    result = runner.invoke(
        cli_mod.app,
        ["data-node", "add_to_edit", "data-node-storage-42", USER_UID],
    )
    assert result.exit_code == 0
    assert captured == {
        "storage_uid": "data-node-storage-42",
        "user_uid": USER_UID,
        "timeout": None,
    }
    assert "Data Node add_to_edit completed." in result.output
    assert "Data Node Sharing Update" in result.output
    assert "editor@example.com" in result.output


def test_data_node_storage_delete_requires_typed_verification(cli_mod, runner, monkeypatch):
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "get_data_node_storage",
        lambda storage_uid, timeout=None: {
            "uid": storage_uid,
            "physical_table_name": "weights_daily_physical",
            "identifier": "weights_daily",
            "source_class_name": "NodeWeights",
            "data_source": {"display_name": "Default DB", "class_type": "timescale_db"},
            "protect_from_deletion": True,
        },
    )

    def _delete(storage_uid, **kwargs):
        captured["storage_uid"] = storage_uid
        captured["kwargs"] = kwargs
        return {
            "uid": storage_uid,
            "physical_table_name": "weights_daily_physical",
            "identifier": "weights_daily",
            "source_class_name": "NodeWeights",
            "data_source": {"display_name": "Default DB", "class_type": "timescale_db"},
            "protect_from_deletion": True,
        }

    monkeypatch.setattr(cli_mod, "delete_data_node_storage", _delete)

    result = runner.invoke(
        cli_mod.app,
        ["data-node", "delete", "data-node-storage-42", "--full-delete-selected"],
        input="weights_daily_physical\n",
    )
    assert result.exit_code == 0
    assert "Data Node Storage Delete Preview" in result.output
    assert "Type physical table name 'weights_daily_physical' to confirm deletion" in result.output
    assert captured["storage_uid"] == "data-node-storage-42"
    assert captured["kwargs"]["full_delete_selected"] is True
    assert "Data node storage deleted: uid=data-node-storage-42" in result.output


def test_data_node_storage_delete_wrong_verification_cancels(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "get_data_node_storage",
        lambda storage_uid, timeout=None: {
            "uid": storage_uid,
            "physical_table_name": "weights_daily_physical",
            "identifier": "weights_daily",
            "source_class_name": "NodeWeights",
            "data_source": {"display_name": "Default DB", "class_type": "timescale_db"},
            "protect_from_deletion": False,
        },
    )

    called = {"value": False}

    def _delete(storage_uid, **kwargs):
        called["value"] = True
        return {}

    monkeypatch.setattr(cli_mod, "delete_data_node_storage", _delete)

    result = runner.invoke(
        cli_mod.app,
        ["data-node", "delete", "data-node-storage-42"],
        input="wrong-value\n",
    )
    assert result.exit_code == 0
    assert called["value"] is False
    assert "Cancelled." in result.output


def test_project_jobs_run(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    captured = {}
    job_uid = "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da"
    monkeypatch.setattr(
        cli_mod,
        "get_project_job",
        lambda job_uid_arg, timeout=None: {
            "uid": job_uid_arg,
            "name": "daily-run",
            "execution_path": "scripts/test.py",
            "app_name": None,
        },
    )
    monkeypatch.setattr(
        cli_mod,
        "run_project_job",
        lambda job_uid, command_args=None, timeout=None: captured.update(
            job_uid=job_uid,
            command_args=command_args,
            timeout=timeout,
        )
        or {
            "uid": "4c1d77c8-8a42-42b8-a9c1-06be9a336e5d",
            "job_uid": job_uid,
            "status": "QUEUED",
            "unique_identifier": "jobrun_abc123",
            "effective_run": "scripts/test.py --name demo-from-cli",
        },
    )

    result = runner.invoke(
        cli_mod.app,
        ["project", "jobs", "run", job_uid, "--", "--name", "demo-from-cli"],
    )
    assert result.exit_code == 0
    assert captured == {
        "job_uid": job_uid,
        "command_args": ["--name", "demo-from-cli"],
        "timeout": None,
    }
    assert "Effective run: scripts/test.py --name demo-from-cli" in result.output
    assert f"Project job run requested: job_uid={job_uid}" in result.output
    assert "jobrun_abc123" in result.output
    assert "QUEUED" in result.output


def test_project_jobs_run_with_arg_option(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    captured = {}
    job_uid = "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da"
    monkeypatch.setattr(
        cli_mod,
        "get_project_job",
        lambda job_uid_arg, timeout=None: {
            "uid": job_uid_arg,
            "name": "daily-run",
            "execution_path": "scripts/test.py",
            "app_name": None,
        },
    )
    monkeypatch.setattr(
        cli_mod,
        "run_project_job",
        lambda job_uid, command_args=None, timeout=None: captured.update(
            job_uid=job_uid,
            command_args=command_args,
            timeout=timeout,
        )
        or {
            "uid": "4c1d77c8-8a42-42b8-a9c1-06be9a336e5d",
            "job_uid": job_uid,
            "status": "QUEUED",
            "unique_identifier": "jobrun_abc123",
            "effective_run": "scripts/test.py demo-from-cli",
        },
    )

    result = runner.invoke(
        cli_mod.app,
        ["project", "jobs", "run", job_uid, "--arg", "demo-from-cli"],
    )
    assert result.exit_code == 0
    assert captured == {
        "job_uid": job_uid,
        "command_args": ["demo-from-cli"],
        "timeout": None,
    }
    assert "Effective run: scripts/test.py demo-from-cli" in result.output


def test_project_job_runs_list(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_project_job_runs",
        lambda job_uid, filters=None, timeout=None: [
            {
                "uid": "4c1d77c8-8a42-42b8-a9c1-06be9a336e5d",
                "name": "daily-run-1",
                "status": "COMPLETED",
                "execution_start": "2026-03-14T09:00:00Z",
                "execution_end": "2026-03-14T09:10:00Z",
                "unique_identifier": "jobrun_abc123",
                "commit_hash": "abc123",
            }
        ],
    )

    result = runner.invoke(
        cli_mod.app,
        [
            "project",
            "jobs",
            "runs",
            "list",
            "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da",
        ],
    )
    assert result.exit_code == 0
    assert "Project Job Runs" in result.output
    assert "daily-ru" in result.output
    assert "jobrun_ab" in result.output
    assert "Total job runs: 1" in result.output


def test_project_job_runs_list_passes_cli_filters(cli_mod, runner, monkeypatch):
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _parse(model_ref, entries):
        captured["entries"] = list(entries or [])
        return {"status": "COMPLETED"}

    def _list_project_job_runs(job_uid, filters=None, timeout=None):
        captured["job_uid"] = job_uid
        captured["filters"] = filters
        return []

    monkeypatch.setattr(cli_mod, "parse_cli_model_filters", _parse)
    monkeypatch.setattr(cli_mod, "list_project_job_runs", _list_project_job_runs)

    result = runner.invoke(
        cli_mod.app,
        [
            "project",
            "jobs",
            "runs",
            "list",
            "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da",
            "--filter",
            "status=COMPLETED",
        ],
    )
    assert result.exit_code == 0
    assert captured["job_uid"] == "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da"
    assert captured["entries"] == ["status=COMPLETED"]
    assert captured["filters"] == {"status": "COMPLETED"}


def test_project_job_runs_logs(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "get_project_job_run_logs",
        lambda job_run_uid, timeout=None: {
            "job_run_uid": job_run_uid,
            "status": "COMPLETED",
            "rows": [
                {"timestamp": "2026-03-14T09:00:00Z", "level": "info", "event": "job started"},
                {"timestamp": "2026-03-14T09:10:00Z", "level": "info", "event": "job finished"},
            ],
        },
    )

    result = runner.invoke(
        cli_mod.app,
        ["project", "jobs", "runs", "logs", "4c1d77c8-8a42-42b8-a9c1-06be9a336e5d"],
    )
    assert result.exit_code == 0
    assert "Job Run Logs" in result.output
    assert "job started" in result.output
    assert "job finished" in result.output
    assert "COMPLETED" in result.output


def test_project_job_runs_logs_polls_and_prints_incrementally(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    responses = iter(
        [
            {
                "job_run_uid": "4c1d77c8-8a42-42b8-a9c1-06be9a336e5d",
                "status": "PENDING",
                "rows": ["first line"],
            },
            {
                "job_run_uid": "4c1d77c8-8a42-42b8-a9c1-06be9a336e5d",
                "status": "RUNNING",
                "rows": ["first line", "second line"],
            },
            {
                "job_run_uid": "4c1d77c8-8a42-42b8-a9c1-06be9a336e5d",
                "status": "COMPLETED",
                "rows": ["first line", "second line", "third line"],
            },
        ]
    )
    sleeps = []

    monkeypatch.setattr(
        cli_mod, "get_project_job_run_logs", lambda job_run_uid, timeout=None: next(responses)
    )
    monkeypatch.setattr(cli_mod.time, "sleep", lambda seconds: sleeps.append(seconds))

    result = runner.invoke(
        cli_mod.app,
        [
            "project",
            "jobs",
            "runs",
            "logs",
            "4c1d77c8-8a42-42b8-a9c1-06be9a336e5d",
            "--poll-interval",
            "3",
        ],
    )
    assert result.exit_code == 0
    assert result.output.count("first line") == 1
    assert result.output.count("second line") == 1
    assert result.output.count("third line") == 1
    assert "Polling again in 3s" in result.output
    assert sleeps == [3, 3]


def test_project_job_runs_logs_stops_after_max_wait(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    responses = iter(
        [
            {
                "job_run_uid": "6d2f9e1a-7d5a-46d0-a01e-61c80f702c8a",
                "status": "PENDING",
                "rows": ["first line"],
            },
            {
                "job_run_uid": "6d2f9e1a-7d5a-46d0-a01e-61c80f702c8a",
                "status": "RUNNING",
                "rows": ["first line", "second line"],
            },
        ]
    )
    sleeps = []
    monotonic_values = iter([100.0, 100.0, 106.0])

    monkeypatch.setattr(
        cli_mod, "get_project_job_run_logs", lambda job_run_uid, timeout=None: next(responses)
    )
    monkeypatch.setattr(cli_mod.time, "sleep", lambda seconds: sleeps.append(seconds))
    monkeypatch.setattr(cli_mod.time, "monotonic", lambda: next(monotonic_values))

    result = runner.invoke(
        cli_mod.app,
        [
            "project",
            "jobs",
            "runs",
            "logs",
            "6d2f9e1a-7d5a-46d0-a01e-61c80f702c8a",
            "--poll-interval",
            "3",
            "--max-wait-seconds",
            "5",
        ],
    )
    assert result.exit_code == 0
    assert result.output.count("first line") == 1
    assert result.output.count("second line") == 1
    assert "Stopping log polling after 5s while job run is still RUNNING." in result.output
    assert sleeps == [3]


def test_project_jobs_create_interactive_defaults(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "demo-123"
    target.mkdir(parents=True, exist_ok=True)
    (target / ".env").write_text("", encoding="utf-8")

    monkeypatch.chdir(target)
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "_resolve_project_branch_uid_for_command",
        lambda *args, **kwargs: "project-branch-uid-123",
    )
    monkeypatch.setattr(
        cli_mod,
        "list_project_images",
        lambda related_project_branch_uid, timeout=None: [
            {
                "uid": "project-image-uid-77",
                "project_repo_hash": "abc123",
                "base_image": {"uid": "base-image-uid-22", "title": "Python 3.12"},
            }
        ],
    )
    monkeypatch.setattr(
        cli_mod,
        "_prompt_select_uid",
        lambda **kwargs: "project-image-uid-77",
    )

    captured = {}

    def _create_project_job(**kwargs):
        captured.update(kwargs)
        return {
            "uid": "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da",
            "name": kwargs["name"],
            "execution_path": kwargs["execution_path"],
            "app_name": kwargs["app_name"],
            "related_image": kwargs["related_image_uid"],
        }

    monkeypatch.setattr(cli_mod, "create_project_job", _create_project_job)

    result = runner.invoke(
        cli_mod.app, ["project", "jobs", "create"], input="demo-job\nscripts/test.py\n\n\n"
    )
    assert result.exit_code == 0
    assert captured["project_branch_uid"] == "project-branch-uid-123"
    assert captured["name"] == "demo-job"
    assert captured["related_image_uid"] == "project-image-uid-77"
    assert captured["execution_path"] == "scripts/test.py"
    assert captured["task_schedule"] is None
    assert captured["cpu_request"] == "0.25"
    assert captured["memory_request"] == "0.5"
    assert captured["spot"] is False
    assert captured["max_runtime_seconds"] == 86400
    assert captured["automatic_deployment"] is False
    assert captured["automatic_redeployment_tag_regex"] is None
    assert (
        "Using defaults: cpu_request=0.25, memory_request=0.5, spot=false, max_runtime_seconds=86400."
        in result.output
    )
    assert "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da" in result.output


def test_project_jobs_create_delegates_initial_image_with_automatic_deployment(
    cli_mod, runner, monkeypatch, tmp_path
):
    target = tmp_path / "demo-123"
    target.mkdir(parents=True, exist_ok=True)
    (target / ".env").write_text("", encoding="utf-8")

    monkeypatch.chdir(target)
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "_resolve_project_branch_uid_for_command",
        lambda *args, **kwargs: "project-branch-uid-123",
    )
    monkeypatch.setattr(
        cli_mod,
        "list_project_images",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("Automatic Job creation must not list caller-selectable images")
        ),
    )
    captured = {}

    def _create_project_job(**kwargs):
        captured.update(kwargs)
        return {
            "uid": "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da",
            "name": kwargs["name"],
            "execution_path": kwargs["execution_path"],
            "related_image_uid": "backend-derived-image-uid",
            "project_repo_hash": "a" * 40,
            "image_status": "ready",
            "automatic_deployment": kwargs["automatic_deployment"],
            "automatic_redeployment_policy": {
                "tag_regex": kwargs["automatic_redeployment_tag_regex"],
                "policy_revision": 1,
            },
        }

    monkeypatch.setattr(cli_mod, "create_project_job", _create_project_job)

    result = runner.invoke(
        cli_mod.app,
        [
            "project",
            "jobs",
            "create",
            "--name",
            "demo-job",
            "--execution-path",
            "scripts/test.py",
            "--automatic-deployment",
            "--automatic-redeployment-tag-regex",
            "^v[0-9]+$",
        ],
        input="\n",
    )

    assert result.exit_code == 0
    assert captured["related_image_uid"] is None
    assert captured["automatic_deployment"] is True
    assert captured["automatic_redeployment_tag_regex"] == "^v[0-9]+$"
    assert "ready" in result.output
    assert "aaaaaaaa" in result.output


def test_project_jobs_create_rejects_image_with_automatic_deployment(
    cli_mod, runner, monkeypatch, tmp_path
):
    target = tmp_path / "demo-123"
    target.mkdir(parents=True, exist_ok=True)
    (target / ".env").write_text("", encoding="utf-8")
    monkeypatch.chdir(target)
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "_resolve_project_branch_uid_for_command",
        lambda *args, **kwargs: "project-branch-uid-123",
    )

    result = runner.invoke(
        cli_mod.app,
        [
            "project",
            "jobs",
            "create",
            "--name",
            "invalid-auto-job",
            "--execution-path",
            "scripts/test.py",
            "--automatic-deployment",
            "--related-image-uid",
            "project-image-uid-77",
        ],
    )

    assert result.exit_code == 1
    assert "Do not provide --related-image-uid" in result.output


def test_project_jobs_create_derives_memory_from_cpu(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "demo-123"
    target.mkdir(parents=True, exist_ok=True)
    (target / ".env").write_text("", encoding="utf-8")

    monkeypatch.chdir(target)
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "_resolve_project_branch_uid_for_command",
        lambda *args, **kwargs: "project-branch-uid-123",
    )
    monkeypatch.setattr(
        cli_mod,
        "list_project_images",
        lambda related_project_branch_uid, timeout=None: [
            {
                "uid": "project-image-uid-77",
                "project_repo_hash": "abc123",
                "base_image": {"uid": "base-image-uid-22", "title": "Python 3.12"},
            }
        ],
    )

    captured = {}

    def _create_project_job(**kwargs):
        captured.update(kwargs)
        return {
            "uid": "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da",
            "name": kwargs["name"],
            "execution_path": kwargs["execution_path"],
            "related_image": kwargs["related_image_uid"],
        }

    monkeypatch.setattr(cli_mod, "create_project_job", _create_project_job)

    result = runner.invoke(
        cli_mod.app,
        [
            "project",
            "jobs",
            "create",
            "--name",
            "demo-job",
            "--cpu-request",
            "1",
            "--execution-path",
            "scripts/test.py",
            "--related-image-uid",
            "project-image-uid-77",
        ],
        input="\n",
    )
    assert result.exit_code == 0
    assert captured["cpu_request"] == "1"
    assert captured["memory_request"] == "1"
    assert captured["spot"] is False
    assert captured["max_runtime_seconds"] == 86400
    assert captured["task_schedule"] is None
    assert (
        "Using defaults: memory_request=1, spot=false, max_runtime_seconds=86400." in result.output
    )


def test_project_jobs_create_interactive_interval_schedule(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "demo-123"
    target.mkdir(parents=True, exist_ok=True)
    (target / ".env").write_text("", encoding="utf-8")

    monkeypatch.chdir(target)
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "_resolve_project_branch_uid_for_command",
        lambda *args, **kwargs: "project-branch-uid-123",
    )
    monkeypatch.setattr(
        cli_mod,
        "list_project_images",
        lambda related_project_branch_uid, timeout=None: [
            {
                "uid": "project-image-uid-77",
                "project_repo_hash": "abc123",
                "base_image": {"uid": "base-image-uid-22", "title": "Python 3.12"},
            }
        ],
    )

    captured = {}

    def _create_project_job(**kwargs):
        captured.update(kwargs)
        return {
            "uid": "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da",
            "name": kwargs["name"],
            "execution_path": kwargs["execution_path"],
            "task_schedule": kwargs["task_schedule"],
            "related_image": kwargs["related_image_uid"],
        }

    monkeypatch.setattr(cli_mod, "create_project_job", _create_project_job)

    result = runner.invoke(
        cli_mod.app,
        ["project", "jobs", "create"],
        input="demo-job\n\nscripts/test.py\ny\n\n\n\n\nn\n",
    )
    assert result.exit_code == 0
    assert captured["task_schedule"] == {
        "schedule": {"type": "interval", "every": 1, "period": "hours"},
        "one_off": False,
    }
    assert "every 1 hours" in result.output


def test_project_create_image_interactive_defaults(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "demo-123"
    target.mkdir(parents=True, exist_ok=True)
    (target / ".env").write_text("", encoding="utf-8")

    monkeypatch.chdir(target)
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "_resolve_project_branch_uid_for_command",
        lambda *args, **kwargs: "project-branch-uid-123",
    )
    monkeypatch.setattr(
        cli_mod,
        "list_project_base_images",
        lambda: [
            {
                "uid": "22222222-2222-4222-8222-222222222222",
                "title": "Python 3.12",
                "description": "Default image",
            }
        ],
    )
    monkeypatch.setattr(
        cli_mod,
        "list_project_images",
        lambda related_project_branch_uid, timeout=None: [
            {
                "uid": "66666666-6666-4666-8666-666666666666",
                "project_repo_hash": "1111111111111111111111111111111111111111",
            }
        ],
    )

    def _git_run(cmd, capture_output=None, text=None, **kwargs):
        args = cmd[3:]
        if args == ["rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{upstream}"]:
            return types.SimpleNamespace(returncode=0, stdout="origin/main\n", stderr="")
        if args[:3] == [
            "rev-parse",
            "--verify",
            "1111111111111111111111111111111111111111^{commit}",
        ]:
            return types.SimpleNamespace(
                returncode=0,
                stdout="1111111111111111111111111111111111111111\n",
                stderr="",
            )
        if args[:1] == ["log"] and "--not" in args and "--remotes" in args:
            return types.SimpleNamespace(
                returncode=0,
                stdout="2222222222222222222222222222222222222222\t2222222\t2026-03-14 11:45:00\tLocal pending commit\n",
                stderr="",
            )
        if args[:1] == ["log"]:
            return types.SimpleNamespace(
                returncode=0,
                stdout="1111111111111111111111111111111111111111\t1111111\t2026-03-14 10:11:12\tPushed commit\n",
                stderr="",
            )
        if args[:3] == ["branch", "-r", "--contains"]:
            return types.SimpleNamespace(returncode=0, stdout="  origin/main\n", stderr="")
        raise AssertionError(f"Unexpected git command: {cmd}")

    monkeypatch.setattr(cli_mod.subprocess, "run", _git_run)

    captured = {}

    def _create_project_image(**kwargs):
        captured.update(kwargs)
        return {
            "uid": "8b62d1dd-e146-44af-957c-38c5f5b1d8d5",
            "project_repo_hash": kwargs["project_repo_hash"],
            "base_image": kwargs["base_image_uid"],
        }

    monkeypatch.setattr(cli_mod, "create_project_image", _create_project_image)

    result = runner.invoke(cli_mod.app, ["project", "images", "create"], input="\n\n")
    assert result.exit_code == 0
    assert captured["related_project_branch_uid"] == "project-branch-uid-123"
    assert captured["project_repo_hash"] == "1111111111111111111111111111111111111111"
    assert captured["base_image_uid"] == "22222222-2222-4222-8222-222222222222"
    assert "8b62d1dd-e146-44af-957c-38c5f5b1d8d5" in result.output
    assert "local commit(s) have not been pushed yet" in result.output
    assert "2026-03-14" in result.output
    assert "10:11:12" in result.output
    assert "66666666" in result.output
    assert "already has project image(s)" in result.output


def test_project_create_image_rejects_unpushed_hash(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "demo-123"
    target.mkdir(parents=True, exist_ok=True)
    (target / ".env").write_text("", encoding="utf-8")

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "_resolve_project_branch_uid_for_command",
        lambda *args, **kwargs: "project-branch-uid-123",
    )
    monkeypatch.setattr(
        cli_mod, "list_project_images", lambda related_project_branch_uid, timeout=None: []
    )

    def _git_run(cmd, capture_output=None, text=None, **kwargs):
        args = cmd[3:]
        if args[:3] == ["rev-parse", "--verify", "deadbeef^{commit}"]:
            return types.SimpleNamespace(
                returncode=0,
                stdout="deadbeefdeadbeefdeadbeefdeadbeefdeadbeef\n",
                stderr="",
            )
        if args[:1] == ["log"] and "--not" in args and "--remotes" in args:
            return types.SimpleNamespace(returncode=0, stdout="", stderr="")
        if args[:3] == ["branch", "-r", "--contains"]:
            return types.SimpleNamespace(returncode=0, stdout="", stderr="")
        raise AssertionError(f"Unexpected git command: {cmd}")

    monkeypatch.setattr(cli_mod.subprocess, "run", _git_run)

    result = runner.invoke(
        cli_mod.app,
        [
            "project",
            "images",
            "create",
            "project-uid-123",
            "deadbeef",
            "--path",
            str(target),
        ],
    )
    assert result.exit_code == 1
    assert "must reference a commit that has already been pushed" in result.output


def test_project_create_image_polls_until_ready(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "demo-123"
    target.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "_resolve_project_branch_uid_for_command",
        lambda *args, **kwargs: "project-branch-uid-123",
    )
    monkeypatch.setattr(cli_mod, "_list_unpushed_commits", lambda *_: [])
    monkeypatch.setattr(
        cli_mod,
        "_resolve_full_commit_hash",
        lambda *_: "abc123abc123abc123abc123abc123abc123abcd",
    )
    monkeypatch.setattr(cli_mod, "_is_pushed_commit", lambda *_: True)

    list_calls = {"count": 0}

    def _list_project_images(related_project_branch_uid, timeout=None):
        list_calls["count"] += 1
        if list_calls["count"] == 1:
            return []
        if list_calls["count"] == 2:
            return [
                {
                    "uid": "8b62d1dd-e146-44af-957c-38c5f5b1d8d5",
                    "project_repo_hash": "abc123abc123abc123abc123abc123abc123abcd",
                    "base_image": 22,
                    "build_error": False,
                    "is_ready": False,
                }
            ]
        return [
            {
                "uid": "8b62d1dd-e146-44af-957c-38c5f5b1d8d5",
                "project_repo_hash": "abc123abc123abc123abc123abc123abc123abcd",
                "base_image": 22,
                "build_error": False,
                "is_ready": True,
            }
        ]

    monkeypatch.setattr(cli_mod, "list_project_images", _list_project_images)
    monkeypatch.setattr(
        cli_mod,
        "create_project_image",
        lambda **kwargs: {
            "uid": "8b62d1dd-e146-44af-957c-38c5f5b1d8d5",
            "project_repo_hash": kwargs["project_repo_hash"],
            "base_image": 22,
            "build_error": False,
            "is_ready": False,
        },
    )

    sleep_calls = []
    monkeypatch.setattr(cli_mod.time, "sleep", lambda secs: sleep_calls.append(secs))

    result = runner.invoke(
        cli_mod.app,
        [
            "project",
            "images",
            "create",
            "project-uid-123",
            "abc123",
            "--path",
            str(target),
            "--base-image-uid",
            "22222222-2222-4222-8222-222222222222",
        ],
    )
    assert result.exit_code == 0
    assert sleep_calls == [30, 30]
    assert "Project image is still building." in result.output
    assert "Project image is ready." in result.output
    assert "Is Ready" in result.output
    assert "True" in result.output


def test_project_create_image_normalizes_short_hash_to_full_sha(
    cli_mod, runner, monkeypatch, tmp_path
):
    target = tmp_path / "demo-123"
    target.mkdir(parents=True, exist_ok=True)
    full_hash = "adb3fbb" + ("0" * 33)

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "_resolve_project_branch_uid_for_command",
        lambda *args, **kwargs: "project-branch-uid-123",
    )
    monkeypatch.setattr(cli_mod, "_list_unpushed_commits", lambda *_: [])
    monkeypatch.setattr(
        cli_mod, "list_project_images", lambda related_project_branch_uid, timeout=None: []
    )

    def _git_run(cmd, capture_output=None, text=None, **kwargs):
        args = cmd[3:]
        if args[:3] == ["rev-parse", "--verify", "adb3fbb^{commit}"]:
            return types.SimpleNamespace(
                returncode=0,
                stdout=f"{full_hash}\n",
                stderr="",
            )
        if args[:3] == ["branch", "-r", "--contains"]:
            assert args[-1] == full_hash
            return types.SimpleNamespace(returncode=0, stdout="  origin/main\n", stderr="")
        raise AssertionError(f"Unexpected git command: {cmd}")

    monkeypatch.setattr(cli_mod.subprocess, "run", _git_run)

    captured = {}

    def _create_project_image(**kwargs):
        captured.update(kwargs)
        return {
            "uid": "8b62d1dd-e146-44af-957c-38c5f5b1d8d5",
            "project_repo_hash": kwargs["project_repo_hash"],
            "base_image": kwargs["base_image_uid"],
            "is_ready": True,
        }

    monkeypatch.setattr(cli_mod, "create_project_image", _create_project_image)

    result = runner.invoke(
        cli_mod.app,
        [
            "project",
            "images",
            "create",
            "project-uid-123",
            "adb3fbb",
            "--path",
            str(target),
            "--base-image-uid",
            "22222222-2222-4222-8222-222222222222",
        ],
    )

    assert result.exit_code == 0
    assert captured["project_repo_hash"] == full_hash
    assert full_hash in result.output


def test_project_create_image_rejects_unresolvable_short_hash(
    cli_mod, runner, monkeypatch, tmp_path
):
    target = tmp_path / "demo-123"
    target.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "_resolve_project_branch_uid_for_command",
        lambda *args, **kwargs: "project-branch-uid-123",
    )
    monkeypatch.setattr(cli_mod, "_list_unpushed_commits", lambda *_: [])
    monkeypatch.setattr(
        cli_mod, "list_project_images", lambda related_project_branch_uid, timeout=None: []
    )

    def _git_run(cmd, capture_output=None, text=None, **kwargs):
        args = cmd[3:]
        if args[:3] == ["rev-parse", "--verify", "adb3fbb^{commit}"]:
            return types.SimpleNamespace(
                returncode=128,
                stdout="",
                stderr="fatal: ambiguous argument 'adb3fbb^{commit}': unknown revision or path not in the working tree.",
            )
        raise AssertionError(f"Unexpected git command: {cmd}")

    monkeypatch.setattr(cli_mod.subprocess, "run", _git_run)

    result = runner.invoke(
        cli_mod.app,
        [
            "project",
            "images",
            "create",
            "project-uid-123",
            "adb3fbb",
            "--path",
            str(target),
            "--base-image-uid",
            "22222222-2222-4222-8222-222222222222",
        ],
    )

    assert result.exit_code == 1
    assert "Could not resolve project_repo_hash to a full commit SHA" in result.output


def test_project_list_requires_shell_auth_hint(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "get_current_user_profile", lambda: {})
    result = runner.invoke(cli_mod.app, ["project", "list"])
    assert result.exit_code == 1
    assert "Not logged in. Run: mainsequence login" in result.output


def test_project_create_interactive_defaults(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "validate_project_name",
        lambda project_name, timeout=None: {
            "project_name": project_name,
            "available": True,
            "reason": None,
            "normalized": {
                "slugified_project_name": "demo-project",
                "project_library_name": "demo_project",
            },
            "suggestions": [],
        },
    )
    monkeypatch.setattr(
        cli_mod,
        "list_project_base_images",
        lambda: [
            {
                "uid": "22222222-2222-4222-8222-222222222222",
                "title": "Python 3.12",
                "description": "Default image",
            }
        ],
    )
    monkeypatch.setattr(
        cli_mod,
        "list_github_organizations",
        lambda: [
            {
                "uid": "33333333-3333-4333-8333-333333333333",
                "login": "main-sequence",
                "display_name": "Main Sequence",
            }
        ],
    )

    captured = {}

    def _create_project(**kwargs):
        captured.update(kwargs)
        return {
            "uid": "project-uid-321",
            "project_name": kwargs["project_name"],
        }

    monkeypatch.setattr(cli_mod, "create_project", _create_project)

    # Prompts:
    # 1) Project name
    # 2) Default base image uid
    # 3) GitHub organization uid
    # 4) Environment variables line
    user_input = "demo-project\n\n\nFOO=bar, BAZ=qux\n"
    result = runner.invoke(cli_mod.app, ["project", "create"], input=user_input)

    assert result.exit_code == 0
    assert captured["project_name"] == "demo-project"
    assert captured["project_type"] == "python"
    assert captured["default_base_image_uid"] == "22222222-2222-4222-8222-222222222222"
    assert captured["github_org_uid"] == "33333333-3333-4333-8333-333333333333"
    assert captured["env_vars"] == {"FOO": "bar", "BAZ": "qux"}
    assert "Project created: demo-project (uid=project-uid-321)" in result.output


def test_project_create_with_explicit_options_returns_logical_project(
    cli_mod,
    runner,
    monkeypatch,
):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "validate_project_name",
        lambda project_name, timeout=None: {
            "project_name": project_name,
            "available": True,
            "reason": None,
            "normalized": {
                "slugified_project_name": "demo-project",
                "project_library_name": "demo_project",
            },
            "suggestions": [],
        },
    )
    captured = {}

    def _create_project(**kwargs):
        captured.update(kwargs)
        return {
            "uid": "77777777-7777-4777-8777-777777777777",
            "project_name": kwargs["project_name"],
        }

    monkeypatch.setattr(cli_mod, "create_project", _create_project)

    result = runner.invoke(
        cli_mod.app,
        [
            "project",
            "create",
            "demo-project",
            "--default-base-image-uid",
            "22222222-2222-4222-8222-222222222222",
            "--github-org-uid",
            "33333333-3333-4333-8333-333333333333",
            "--env",
            "FOO=bar",
        ],
    )

    assert result.exit_code == 0
    assert "default_metatables_data_source_uid" not in captured
    assert "Project created: demo-project" in result.output


def test_project_create_rejects_unavailable_name(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "validate_project_name",
        lambda project_name, timeout=None: {
            "project_name": project_name,
            "available": False,
            "reason": "A project with this name already exists in your organization.",
            "normalized": {
                "slugified_project_name": "demo-project",
                "project_library_name": "demo_project",
            },
            "suggestions": ["Demo Project 2", "Demo Project 3"],
        },
    )

    result = runner.invoke(cli_mod.app, ["project", "create", "Demo Project"])

    assert result.exit_code == 1
    assert "A project with this name already exists in your organization." in result.output
    assert "Project Name Validation" in result.output
    assert "Demo Project 2" in result.output
    assert "Demo Project 3" in result.output


def test_project_delete_remote_yes(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "resolve_project",
        lambda project_id: {"id": 321, "uid": "project-uid-321", "project_name": "Demo Project"},
    )

    captured = {}

    def _bulk_delete_projects(*, uids, delete_repositories=False):
        captured["uids"] = uids
        captured["delete_repositories"] = delete_repositories
        return {"detail": "deleted", "deleted_count": 1}

    monkeypatch.setattr(cli_mod, "bulk_delete_projects", _bulk_delete_projects)

    result = runner.invoke(
        cli_mod.app,
        ["project", "delete", "project-uid-321", "--yes", "--delete-repositories"],
    )
    assert result.exit_code == 0
    assert captured["uids"] == ["project-uid-321"]
    assert captured["delete_repositories"] is True
    assert "Project deleted: Demo Project (uid=project-uid-321; deleted=1)" in result.output


def test_resolve_project_repository_ssh_url_uses_canonical_repository(
    cli_mod,
    monkeypatch,
):
    repository_uid = "2bcf47e3-3a79-4f1e-a428-176c0218a8d1"
    captured = {}

    def _get_project_repository(uid):
        captured["uid"] = uid
        return {
            "uid": uid,
            "git_ssh_url": "git@github.com:mainsequence-projects/tutorial.git",
            "git_repo_url": "https://github.com/mainsequence-projects/tutorial.git",
        }

    monkeypatch.setattr(cli_mod, "get_project_repository", _get_project_repository)

    result = cli_mod._resolve_project_repository_ssh_url({"git_repository_uid": repository_uid})

    assert captured["uid"] == repository_uid
    assert result == "git@github.com:mainsequence-projects/tutorial.git"


def test_resolve_project_repository_ssh_url_requires_linked_repository(cli_mod, monkeypatch):
    monkeypatch.setattr(
        cli_mod,
        "get_project_repository",
        lambda uid: (_ for _ in ()).throw(AssertionError("must not fetch")),
    )

    with pytest.raises(cli_mod.ApiError, match="no linked GitRepository"):
        cli_mod._resolve_project_repository_ssh_url({})


def test_resolve_project_repository_ssh_url_requires_ssh_url(cli_mod, monkeypatch):
    repository_uid = "2bcf47e3-3a79-4f1e-a428-176c0218a8d1"
    monkeypatch.setattr(
        cli_mod,
        "get_project_repository",
        lambda uid: {
            "uid": uid,
            "git_repo_url": "https://github.com/mainsequence-projects/tutorial.git",
        },
    )

    with pytest.raises(cli_mod.ApiError, match="has no SSH clone URL"):
        cli_mod._resolve_project_repository_ssh_url({"git_repository_uid": repository_uid})


def test_ensure_project_repository_ssh_access_registers_new_key_before_verification(
    cli_mod, monkeypatch, tmp_path
):
    origin = "git@github.com:org/repo.git"
    key = tmp_path / "mainsequence-repo-example"
    public_key_path = pathlib.Path(f"{key}.pub")
    events = []

    monkeypatch.setattr(cli_mod, "repository_ssh_key_paths", lambda repo: (key, public_key_path))
    monkeypatch.setattr(
        cli_mod,
        "ensure_key_for_repo",
        lambda repo: (key, public_key_path, "ssh-ed25519 AAA test"),
    )
    monkeypatch.setattr(cli_mod.platform, "node", lambda: "developer-laptop")
    monkeypatch.setattr(
        cli_mod,
        "add_deploy_key",
        lambda project_ref, title, public_key: events.append(
            ("register", project_ref, title, public_key)
        ),
    )

    key_path, public_key, env = cli_mod._ensure_project_repository_ssh_access(
        origin=origin,
        project_ref="project-uid-123",
        verify_access=lambda ssh_env: events.append(("verify", ssh_env["GIT_SSH_COMMAND"])),
    )

    assert key_path == key
    assert public_key == "ssh-ed25519 AAA test"
    assert env["GIT_SSH_COMMAND"] == f'ssh -i "{key}" -o IdentitiesOnly=yes'
    assert events == [
        ("register", "project-uid-123", "developer-laptop", "ssh-ed25519 AAA test"),
        ("verify", env["GIT_SSH_COMMAND"]),
    ]


def test_ensure_project_repository_ssh_access_registers_inaccessible_existing_key(
    cli_mod, monkeypatch, tmp_path
):
    origin = "git@github.com:org/repo.git"
    key = tmp_path / "mainsequence-repo-example"
    public_key_path = pathlib.Path(f"{key}.pub")
    key.write_text("private", encoding="utf-8")
    public_key_path.write_text("ssh-ed25519 AAA test\n", encoding="utf-8")
    events = []

    monkeypatch.setattr(cli_mod, "repository_ssh_key_paths", lambda repo: (key, public_key_path))
    monkeypatch.setattr(
        cli_mod,
        "ensure_key_for_repo",
        lambda repo: (key, public_key_path, "ssh-ed25519 AAA test"),
    )
    monkeypatch.setattr(cli_mod.platform, "node", lambda: "developer-laptop")
    monkeypatch.setattr(
        cli_mod,
        "add_deploy_key",
        lambda project_ref, title, public_key: events.append("register"),
    )

    def verify_access(env):
        events.append("verify")
        if events == ["verify"]:
            raise RuntimeError("not registered")

    cli_mod._ensure_project_repository_ssh_access(
        origin=origin,
        project_ref="project-uid-123",
        verify_access=verify_access,
    )

    assert events == ["verify", "register", "verify"]


def test_project_set_up_locally(cli_mod, runner, monkeypatch, tmp_path):
    base = tmp_path / "base"
    base.mkdir(parents=True, exist_ok=True)
    key = tmp_path / "id_ed25519"
    pub = tmp_path / "id_ed25519.pub"

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": str(base)},
    )
    monkeypatch.setattr(cli_mod.cfg, "backend_url", lambda: "https://backend.test")
    monkeypatch.setattr(cli_mod, "_org_slug_from_profile", lambda: "org")
    monkeypatch.setattr(
        cli_mod,
        "resolve_project",
        lambda project_ref: {
            "uid": "project-uid-123",
            "project_name": "Demo",
            "git_repository_uid": "repository-uid-123",
            "archived": False,
            "created_by": "u",
            "labels": [],
            "branches": [{"uid": "project-branch-uid-123", "repository_branch": "main"}],
        },
    )
    monkeypatch.setattr(
        cli_mod,
        "get_project_branch",
        lambda branch_uid: {
            "uid": branch_uid,
            "project_uid": "project-uid-123",
            "project_name": "Demo",
            "repository_branch": "main",
            "is_initialized": True,
        },
    )
    repository_requests = []
    monkeypatch.setattr(
        cli_mod,
        "get_project_repository",
        lambda repository_uid: repository_requests.append(repository_uid)
        or {
            "uid": repository_uid,
            "git_ssh_url": "git@github.com:org/repo.git",
            "git_repo_url": "https://github.com/org/repo.git",
        },
    )
    monkeypatch.setattr(
        cli_mod, "ensure_key_for_repo", lambda repo: (key, pub, "ssh-ed25519 AAA test")
    )
    monkeypatch.setattr(cli_mod, "repository_ssh_key_paths", lambda repo: (key, pub))
    monkeypatch.setattr(cli_mod, "verify_git_remote_access", lambda repo, env: None)
    monkeypatch.setattr(cli_mod, "_copy_clipboard", lambda txt: True)
    deploy_key_requests = []
    monkeypatch.setattr(
        cli_mod,
        "add_deploy_key",
        lambda *args, **kwargs: deploy_key_requests.append((args, kwargs)),
    )
    monkeypatch.setattr(cli_mod, "start_agent_and_add_key", lambda *_: {})

    def _clone(cmd, env=None, cwd=None):
        assert cmd[0:2] == ["git", "clone"]
        pathlib.Path(cmd[-1]).mkdir(parents=True, exist_ok=True)
        return 0

    monkeypatch.setattr(cli_mod.subprocess, "call", _clone)
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_tokens",
        lambda: {"username": "u", "access": "access-123", "refresh": "refresh-456"},
    )

    result = runner.invoke(
        cli_mod.app,
        ["project", "set-up-locally", "project-uid-123", "--branch", "main"],
    )
    assert result.exit_code == 0
    assert repository_requests == ["repository-uid-123"]
    assert len(deploy_key_requests) == 1
    assert deploy_key_requests[0][0][0] == "project-uid-123"
    assert deploy_key_requests[0][0][2] == "ssh-ed25519 AAA test"
    assert deploy_key_requests[0][1] == {}

    env_file = base / "org" / "projects" / "demo-project-uid-123" / ".env"
    assert env_file.exists()
    env_text = env_file.read_text(encoding="utf-8")
    assert "MAINSEQUENCE_ACCESS_TOKEN=access-123" in env_text
    assert "MAINSEQUENCE_REFRESH_TOKEN=refresh-456" in env_text
    assert "MAINSEQUENCE_ENDPOINT=https://backend.test" in env_text
    assert "MAIN_SEQUENCE_PROJECT_UID" not in env_text
    assert "MAIN_SEQUENCE_PROJECT_ID" not in env_text
    assert "DEFAULT_BASE_IMAGE" not in env_text
    assert "FOO=bar" not in env_text
    assert "MAINSEQUENCE_TOKEN=legacy-token" not in env_text


def test_project_set_up_locally_runtime_credential(cli_mod, runner, monkeypatch, tmp_path):
    base = tmp_path / "base"
    base.mkdir(parents=True, exist_ok=True)
    key = tmp_path / "id_ed25519"
    pub = tmp_path / "id_ed25519.pub"

    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "runtime_credential")
    monkeypatch.setenv("MAINSEQUENCE_RUNTIME_CREDENTIAL_ID", "cred-id")
    monkeypatch.setenv("MAINSEQUENCE_RUNTIME_CREDENTIAL_SECRET", "cred-secret")
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": str(base)},
    )
    monkeypatch.setattr(cli_mod.cfg, "backend_url", lambda: "https://backend.test")
    monkeypatch.setattr(cli_mod, "_org_slug_from_profile", lambda: "org")
    monkeypatch.setattr(
        cli_mod,
        "resolve_project",
        lambda project_ref: {
            "uid": "project-uid-123",
            "project_name": "Demo",
            "git_repository_uid": "repository-uid-123",
            "archived": False,
            "created_by": "u",
            "labels": [],
            "branches": [{"uid": "project-branch-uid-123", "repository_branch": "main"}],
        },
    )
    monkeypatch.setattr(
        cli_mod,
        "get_project_branch",
        lambda branch_uid: {
            "uid": branch_uid,
            "project_uid": "project-uid-123",
            "project_name": "Demo",
            "repository_branch": "main",
            "is_initialized": True,
        },
    )
    repository_requests = []
    monkeypatch.setattr(
        cli_mod,
        "get_project_repository",
        lambda repository_uid: repository_requests.append(repository_uid)
        or {
            "uid": repository_uid,
            "git_ssh_url": "git@github.com:org/repo.git",
            "git_repo_url": "https://github.com/org/repo.git",
        },
    )
    monkeypatch.setattr(
        cli_mod, "ensure_key_for_repo", lambda repo: (key, pub, "ssh-ed25519 AAA test")
    )
    monkeypatch.setattr(cli_mod, "repository_ssh_key_paths", lambda repo: (key, pub))
    monkeypatch.setattr(cli_mod, "verify_git_remote_access", lambda repo, env: None)
    monkeypatch.setattr(cli_mod, "_copy_clipboard", lambda txt: True)
    deploy_key_requests = []
    monkeypatch.setattr(
        cli_mod,
        "add_deploy_key",
        lambda *args, **kwargs: deploy_key_requests.append((args, kwargs)),
    )
    monkeypatch.setattr(cli_mod, "start_agent_and_add_key", lambda *_: {})

    def _clone(cmd, env=None, cwd=None):
        assert cmd[0:2] == ["git", "clone"]
        pathlib.Path(cmd[-1]).mkdir(parents=True, exist_ok=True)
        return 0

    monkeypatch.setattr(cli_mod.subprocess, "call", _clone)
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_tokens",
        lambda: (_ for _ in ()).throw(AssertionError("JWT tokens should not be used")),
    )
    monkeypatch.setattr(
        cli_mod,
        "_exchange_runtime_credential_for_cli_login",
        lambda backend_url: "runtime-access",
    )

    result = runner.invoke(
        cli_mod.app,
        ["project", "set-up-locally", "project-uid-123", "--branch", "main"],
    )
    assert result.exit_code == 0
    assert repository_requests == ["repository-uid-123"]
    assert len(deploy_key_requests) == 1
    assert deploy_key_requests[0][0][0] == "project-uid-123"
    assert deploy_key_requests[0][0][2] == "ssh-ed25519 AAA test"
    assert deploy_key_requests[0][1] == {}

    env_file = base / "org" / "projects" / "demo-project-uid-123" / ".env"
    env_text = env_file.read_text(encoding="utf-8")
    assert "MAINSEQUENCE_AUTH_MODE=runtime_credential" in env_text
    assert "MAINSEQUENCE_ACCESS_TOKEN=runtime-access" in env_text
    assert "MAINSEQUENCE_RUNTIME_CREDENTIAL_ID=cred-id" in env_text
    assert "MAINSEQUENCE_RUNTIME_CREDENTIAL_SECRET=cred-secret" in env_text
    assert "MAINSEQUENCE_ENDPOINT=https://backend.test" in env_text
    assert "MAIN_SEQUENCE_PROJECT_UID" not in env_text
    assert "MAIN_SEQUENCE_PROJECT_ID" not in env_text
    assert "MAINSEQUENCE_REFRESH_TOKEN" not in env_text
    assert "DEFAULT_BASE_IMAGE" not in env_text
    assert "FOO=bar" not in env_text
    assert "old-access" not in env_text
    assert "old-refresh" not in env_text


def test_project_set_up_locally_rejects_uninitialized_project(
    cli_mod, runner, monkeypatch, tmp_path
):
    base = tmp_path / "base"
    base.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": str(base)},
    )
    monkeypatch.setattr(cli_mod, "_org_slug_from_profile", lambda: "org")
    monkeypatch.setattr(
        cli_mod,
        "resolve_project",
        lambda project_ref: {
            "uid": "project-uid-123",
            "project_name": "Demo",
            "git_repository_uid": "repository-uid-123",
            "archived": False,
            "created_by": "u",
            "labels": [],
            "branches": [{"uid": "project-branch-uid-123", "repository_branch": "main"}],
        },
    )
    monkeypatch.setattr(
        cli_mod,
        "get_project_branch",
        lambda branch_uid: {
            "uid": branch_uid,
            "project_uid": "project-uid-123",
            "project_name": "Demo",
            "repository_branch": "main",
            "is_initialized": False,
        },
    )

    clone_calls = {"count": 0}

    def _clone(*args, **kwargs):
        clone_calls["count"] += 1
        return 0

    monkeypatch.setattr(cli_mod.subprocess, "call", _clone)

    result = runner.invoke(
        cli_mod.app,
        ["project", "set-up-locally", "project-uid-123", "--branch", "main"],
    )

    assert result.exit_code == 1
    assert "Project has not finished initializing yet." in result.output
    assert clone_calls["count"] == 0


def test_project_open(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "project"
    target.mkdir(parents=True, exist_ok=True)
    opened = {"path": None}

    monkeypatch.setattr(cli_mod, "open_folder", lambda p: opened.update(path=p))
    result = runner.invoke(cli_mod.app, ["project", "open", "--path", str(target)])
    assert result.exit_code == 0
    assert opened["path"] == str(target.resolve())


def test_project_refresh_token(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "demo-project-uid-123"
    target.mkdir(parents=True, exist_ok=True)
    env_path = target / ".env"
    env_path.write_text(
        "FOO=bar\n"
        "MAIN_SEQUENCE_PROJECT_UID=project-uid-123\n"
        "MAIN_SEQUENCE_PROJECT_ID=123\n"
        "MAINSEQUENCE_ACCESS_TOKEN=old-access\n"
        "MAINSEQUENCE_REFRESH_TOKEN=old-refresh\n"
        "MAINSEQUENCE_ENDPOINT=https://old-backend.test\n"
        "MAINSEQUENCE_TOKEN=legacy-token\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_tokens",
        lambda: {"username": "u", "access": "new-access", "refresh": "new-refresh"},
    )
    monkeypatch.setattr(cli_mod.cfg, "backend_url", lambda: "https://backend.test")

    result = runner.invoke(cli_mod.app, ["project", "refresh_token", "--path", str(target)])
    assert result.exit_code == 0

    env_text = env_path.read_text(encoding="utf-8")
    assert "FOO=bar" in env_text
    assert "MAINSEQUENCE_ACCESS_TOKEN=new-access" in env_text
    assert "MAINSEQUENCE_REFRESH_TOKEN=new-refresh" in env_text
    assert "MAINSEQUENCE_ENDPOINT=https://backend.test" in env_text
    assert "MAIN_SEQUENCE_PROJECT_UID" not in env_text
    assert "MAIN_SEQUENCE_PROJECT_ID" not in env_text
    assert "MAINSEQUENCE_TOKEN" not in env_text
    assert "old-access" not in env_text
    assert "old-refresh" not in env_text


def test_project_refresh_token_runtime_credential(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "demo-project-uid-123"
    target.mkdir(parents=True, exist_ok=True)
    env_path = target / ".env"
    env_path.write_text(
        "FOO=bar\n"
        "MAIN_SEQUENCE_PROJECT_UID=project-uid-123\n"
        "MAIN_SEQUENCE_PROJECT_ID=123\n"
        "MAINSEQUENCE_AUTH_MODE=jwt\n"
        "MAINSEQUENCE_ACCESS_TOKEN=old-access\n"
        "MAINSEQUENCE_REFRESH_TOKEN=old-refresh\n"
        "MAINSEQUENCE_TOKEN=legacy-token\n",
        encoding="utf-8",
    )

    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "runtime_credential")
    monkeypatch.setenv("MAINSEQUENCE_RUNTIME_CREDENTIAL_ID", "cred-id")
    monkeypatch.setenv("MAINSEQUENCE_RUNTIME_CREDENTIAL_SECRET", "cred-secret")
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_tokens",
        lambda: (_ for _ in ()).throw(AssertionError("JWT tokens should not be used")),
    )
    monkeypatch.setattr(cli_mod.cfg, "backend_url", lambda: "https://backend.test")
    monkeypatch.setattr(
        cli_mod,
        "_exchange_runtime_credential_for_cli_login",
        lambda backend_url: "runtime-new-access",
    )

    result = runner.invoke(cli_mod.app, ["project", "refresh_token", "--path", str(target)])
    assert result.exit_code == 0

    env_text = env_path.read_text(encoding="utf-8")
    assert "FOO=bar" in env_text
    assert "MAINSEQUENCE_AUTH_MODE=runtime_credential" in env_text
    assert "MAINSEQUENCE_ACCESS_TOKEN=runtime-new-access" in env_text
    assert "MAINSEQUENCE_RUNTIME_CREDENTIAL_ID=cred-id" in env_text
    assert "MAINSEQUENCE_RUNTIME_CREDENTIAL_SECRET=cred-secret" in env_text
    assert "MAINSEQUENCE_ENDPOINT=https://backend.test" in env_text
    assert "MAIN_SEQUENCE_PROJECT_UID" not in env_text
    assert "MAIN_SEQUENCE_PROJECT_ID" not in env_text
    assert "MAINSEQUENCE_TOKEN" not in env_text
    assert "MAINSEQUENCE_REFRESH_TOKEN" not in env_text
    assert "old-access" not in env_text
    assert "old-refresh" not in env_text


def test_project_refresh_token_defaults_to_cwd(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "demo-project-uid-123"
    target.mkdir(parents=True, exist_ok=True)
    env_path = target / ".env"
    env_path.write_text(
        "FOO=bar\n"
        "MAIN_SEQUENCE_PROJECT_UID=project-uid-123\n"
        "MAINSEQUENCE_ACCESS_TOKEN=old-access\n"
        "MAINSEQUENCE_REFRESH_TOKEN=old-refresh\n",
        encoding="utf-8",
    )

    monkeypatch.chdir(target)
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_tokens",
        lambda: {"username": "u", "access": "new-access", "refresh": "new-refresh"},
    )
    monkeypatch.setattr(cli_mod.cfg, "backend_url", lambda: "https://backend.test")

    result = runner.invoke(cli_mod.app, ["project", "refresh_token"])
    assert result.exit_code == 0

    env_text = env_path.read_text(encoding="utf-8")
    assert "MAINSEQUENCE_ACCESS_TOKEN=new-access" in env_text
    assert "MAINSEQUENCE_REFRESH_TOKEN=new-refresh" in env_text
    assert "MAINSEQUENCE_ENDPOINT=https://backend.test" in env_text
    assert "MAIN_SEQUENCE_PROJECT_UID" not in env_text


def test_project_delete_local(cli_mod, runner, monkeypatch, tmp_path):
    base = tmp_path / "base"
    project_path = base / "org" / "projects" / "demo-123"
    project_path.mkdir(parents=True, exist_ok=True)
    (project_path / "x.txt").write_text("x", encoding="utf-8")

    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": str(base)},
    )
    monkeypatch.setattr(cli_mod, "get_current_user_profile", lambda: {"organization": "Org"})
    monkeypatch.setattr(cli_mod, "_org_slug_from_profile", lambda: "org")

    result = runner.invoke(
        cli_mod.app,
        ["project", "delete-local", "--path", str(project_path), "--yes"],
    )
    assert result.exit_code == 0
    assert not project_path.exists()


def test_project_open_signed_terminal(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "project"
    target.mkdir(parents=True, exist_ok=True)
    key = tmp_path / "id_ed25519"
    called = {"args": None}

    monkeypatch.setattr(cli_mod, "git_origin", lambda _: "git@github.com:org/repo.git")
    monkeypatch.setattr(
        cli_mod,
        "_ensure_project_repository_ssh_access",
        lambda **kwargs: (key, "pub", {"GIT_SSH_COMMAND": "forced"}),
    )
    monkeypatch.setattr(
        cli_mod,
        "open_signed_terminal",
        lambda project_dir, key_path, repo_name: called.update(
            args=(project_dir, str(key_path), repo_name)
        ),
    )

    result = runner.invoke(
        cli_mod.app,
        ["project", "open-signed-terminal", "--path", str(target)],
    )
    assert result.exit_code == 0
    assert called["args"] is not None


def test_project_build_local_venv(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "project"
    target.mkdir(parents=True, exist_ok=True)
    (target / "pyproject.toml").write_text(
        '[project]\nname = "demo"\nrequires-python = ">=3.13,<3.14"\n',
        encoding="utf-8",
    )

    monkeypatch.setattr(cli_mod, "_resolve_uv_runner", lambda: (["uv"], "uv"))
    calls = []

    def _run(cmd, cwd=None, env=None, capture_output=None, text=None):
        calls.append((cmd, cwd, env))
        return types.SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(cli_mod.subprocess, "run", _run)

    result = runner.invoke(
        cli_mod.app,
        ["project", "build_local_venv", "--path", str(target)],
    )
    assert result.exit_code == 0
    assert calls[0][0] == ["uv", "venv", ".venv", "--python", ">=3.13,<3.14"]
    assert calls[0][1] == str(target.resolve())
    assert calls[1][0] == ["uv", "sync"]
    assert calls[1][2]["UV_PROJECT_ENVIRONMENT"] == ".venv"
    assert "Local .venv built for Python requirement >=3.13,<3.14." in result.output


def test_project_build_local_venv_defaults_to_cwd_with_env_project_id(
    cli_mod, runner, monkeypatch, tmp_path
):
    target = tmp_path / "demo-project-uid-123"
    target.mkdir(parents=True, exist_ok=True)
    (target / ".env").write_text("", encoding="utf-8")
    (target / "pyproject.toml").write_text(
        '[project]\nname = "demo"\nrequires-python = ">=3.13,<3.14"\n',
        encoding="utf-8",
    )

    monkeypatch.chdir(target)
    monkeypatch.setattr(cli_mod, "_resolve_uv_runner", lambda: (["uv"], "uv"))
    calls = []

    def _run(cmd, cwd=None, env=None, capture_output=None, text=None):
        calls.append((cmd, cwd, env))
        return types.SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(cli_mod.subprocess, "run", _run)

    result = runner.invoke(cli_mod.app, ["project", "build_local_venv"])
    assert result.exit_code == 0
    assert calls[0][0] == ["uv", "venv", ".venv", "--python", ">=3.13,<3.14"]
    assert calls[0][1] == str(target.resolve())
    assert calls[1][0] == ["uv", "sync"]
    assert calls[1][2]["UV_PROJECT_ENVIRONMENT"] == ".venv"


@pytest.mark.parametrize(
    ("spec", "expected"),
    [
        (">=3.13,<3.15,!=3.14.1", ">=3.13,<3.15,!=3.14.1"),
        ("^3.13", ">=3.13,<4.0"),
        ("~3.13", ">=3.13,<3.14"),
        ("3.13", "==3.13.*"),
        ("3.13.2", "==3.13.2"),
    ],
)
def test_normalize_python_version_request(cli_mod, spec, expected):
    assert cli_mod._normalize_python_version_request(spec) == expected


def test_normalize_python_version_request_rejects_invalid_constraint(cli_mod):
    assert cli_mod._normalize_python_version_request("not-a-version") is None


def test_project_build_local_venv_skips_compatible_existing_environment(
    cli_mod, runner, monkeypatch, tmp_path
):
    target = tmp_path / "project"
    target.mkdir(parents=True, exist_ok=True)
    (target / ".venv").mkdir(parents=True, exist_ok=True)
    (target / "pyproject.toml").write_text(
        '[project]\nname = "demo"\nrequires-python = ">=3.13,<3.14"\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(
        cli_mod,
        "_read_venv_python_version",
        lambda _: cli_mod.Version("3.13.7"),
    )

    result = runner.invoke(
        cli_mod.app,
        ["project", "build_local_venv", "--path", str(target)],
    )
    assert result.exit_code == 0
    assert "already uses compatible Python 3.13.7" in result.output


def test_project_build_local_venv_rejects_incompatible_existing_environment(
    cli_mod, runner, monkeypatch, tmp_path
):
    target = tmp_path / "project"
    target.mkdir(parents=True, exist_ok=True)
    (target / ".venv").mkdir(parents=True, exist_ok=True)
    (target / "pyproject.toml").write_text(
        '[project]\nname = "demo"\nrequires-python = ">=3.13"\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(
        cli_mod,
        "_read_venv_python_version",
        lambda _: cli_mod.Version("3.12.8"),
    )

    result = runner.invoke(
        cli_mod.app,
        ["project", "build_local_venv", "--path", str(target)],
    )

    assert result.exit_code == 1
    assert "Python 3.12.8, which does not satisfy >=3.13" in result.output
    assert "Re-run with --recreate" in result.output


def test_project_build_local_venv_recreates_incompatible_environment(
    cli_mod, runner, monkeypatch, tmp_path
):
    target = tmp_path / "project"
    target.mkdir(parents=True, exist_ok=True)
    venv_path = target / ".venv"
    venv_path.mkdir(parents=True, exist_ok=True)
    (venv_path / "old-environment").write_text("old", encoding="utf-8")
    (target / "pyproject.toml").write_text(
        '[project]\nname = "demo"\nrequires-python = ">=3.13"\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(cli_mod, "_resolve_uv_runner", lambda: (["uv"], "uv"))
    calls = []

    def _run(cmd, cwd=None, env=None, capture_output=None, text=None):
        calls.append((cmd, cwd, env))
        return types.SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(cli_mod.subprocess, "run", _run)

    result = runner.invoke(
        cli_mod.app,
        ["project", "build_local_venv", "--path", str(target), "--recreate"],
    )

    assert result.exit_code == 0
    assert not (venv_path / "old-environment").exists()
    assert calls[0][0] == ["uv", "venv", ".venv", "--python", ">=3.13"]
    assert "Replacing existing" in result.output


def test_project_build_local_venv_preserves_existing_environment_when_uv_is_unavailable(
    cli_mod, runner, monkeypatch, tmp_path
):
    target = tmp_path / "project"
    target.mkdir(parents=True, exist_ok=True)
    marker = target / ".venv" / "existing-environment"
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text("existing", encoding="utf-8")
    (target / "pyproject.toml").write_text(
        '[project]\nname = "demo"\nrequires-python = ">=3.13"\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(cli_mod, "_resolve_uv_runner", lambda: None)
    monkeypatch.setattr(cli_mod, "_install_uv", lambda: (False, "offline"))

    result = runner.invoke(
        cli_mod.app,
        ["project", "build_local_venv", "--path", str(target), "--recreate"],
    )

    assert result.exit_code == 1
    assert marker.read_text(encoding="utf-8") == "existing"
    assert "automatic install failed: offline" in result.output


def test_project_build_local_venv_requires_pyproject(cli_mod, runner, tmp_path):
    target = tmp_path / "project"
    target.mkdir(parents=True, exist_ok=True)

    result = runner.invoke(
        cli_mod.app,
        ["project", "build_local_venv", "--path", str(target)],
    )
    assert result.exit_code == 1
    assert "pyproject.toml not found in the project root." in result.output


def test_project_freeze_env(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "project"
    target.mkdir(parents=True, exist_ok=True)
    uv_path = target / ".venv" / "bin" / "uv"
    calls = []

    monkeypatch.setattr(cli_mod, "ensure_venv", lambda *_: None)
    monkeypatch.setattr(cli_mod, "ensure_uv_installed", lambda *_: uv_path)
    monkeypatch.setattr(
        cli_mod,
        "uv_export_requirements",
        lambda uv, cwd, **kwargs: calls.append((uv, cwd, kwargs)),
    )

    result = runner.invoke(cli_mod.app, ["project", "freeze-env", "--path", str(target)])
    assert result.exit_code == 0
    assert len(calls) == 1
    assert calls[0][2]["output_file"] == "requirements.txt"


@pytest.mark.parametrize(
    ("git_branch", "rendered_tag"),
    [
        ("main", "v1.2.4"),
        ("dev", "v1.2.4-dev.1"),
        ("feature/foo", "v1.2.4-feature-foo-12345678.1"),
    ],
)
def test_project_sync(
    cli_mod,
    runner,
    monkeypatch,
    tmp_path,
    git_branch,
    rendered_tag,
):
    target = tmp_path / "project"
    target.mkdir(parents=True, exist_ok=True)
    (target / ".env").write_text("", encoding="utf-8")
    key = tmp_path / "id_ed25519"
    uv_path = target / ".venv" / "bin" / "uv"
    uv_calls = []
    git_calls = []
    tag_requests = []

    monkeypatch.setattr(cli_mod, "ensure_venv", lambda *_: None)
    monkeypatch.setattr(
        cli_mod,
        "_resolve_git_project_branch_context",
        lambda *args, **kwargs: (git_branch, "project-branch-uid-123"),
    )
    monkeypatch.setattr(cli_mod, "git_origin", lambda *_: "git@github.com:org/repo.git")
    monkeypatch.setattr(
        cli_mod,
        "_ensure_project_repository_ssh_access",
        lambda **kwargs: (key, "pub", {"GIT_SSH_COMMAND": "forced"}),
    )
    monkeypatch.setattr(cli_mod, "ensure_uv_installed", lambda *_: uv_path)
    monkeypatch.setattr(cli_mod, "uv_project_version", lambda *_, **__: "1.2.4")
    monkeypatch.setattr(cli_mod, "uv_preview_patch_version", lambda *_, **__: "1.2.4")
    monkeypatch.setattr(cli_mod, "verify_git_tag_absent", lambda *_, **__: None)
    monkeypatch.setattr(cli_mod, "verify_git_remote_tag_absent", lambda *_, **__: None)
    monkeypatch.setattr(
        cli_mod,
        "render_project_branch_default_redeployment_tag",
        lambda uid, *, version: tag_requests.append((uid, version)) or rendered_tag,
    )
    monkeypatch.setattr(
        cli_mod,
        "run_uv",
        lambda uv, args, cwd, env=None: uv_calls.append(args),
    )
    monkeypatch.setattr(
        cli_mod,
        "uv_export_requirements",
        lambda uv, cwd, **kwargs: uv_calls.append(["export"]),
    )
    monkeypatch.setattr(
        cli_mod,
        "run_cmd",
        lambda cmd, cwd, env=None: git_calls.append(cmd),
    )
    result = runner.invoke(
        cli_mod.app,
        ["project", "sync", "--message", "Update deps", "--path", str(target)],
    )
    assert result.exit_code == 0
    assert ["version", "--bump", "patch"] in uv_calls
    assert ["lock"] in uv_calls
    assert ["sync"] in uv_calls
    assert ["git", "add", "-A"] in git_calls
    assert ["git", "commit", "-m", "Update deps"] in git_calls
    assert ["git", "tag", "-a", rendered_tag, "-m", rendered_tag] in git_calls
    assert [
        "git",
        "push",
        "--atomic",
        "--follow-tags",
        "origin",
        f"HEAD:refs/heads/{git_branch}",
        f"refs/tags/{rendered_tag}:refs/tags/{rendered_tag}",
    ] in git_calls
    assert tag_requests == [("project-branch-uid-123", "1.2.4")]


def test_project_sync_defaults_to_cwd_with_positional_message(
    cli_mod, runner, monkeypatch, tmp_path
):
    target = tmp_path / "project"
    target.mkdir(parents=True, exist_ok=True)
    (target / ".env").write_text("", encoding="utf-8")
    key = tmp_path / "id_ed25519"
    uv_path = target / ".venv" / "bin" / "uv"
    git_calls = []

    monkeypatch.chdir(target)
    monkeypatch.setattr(cli_mod, "ensure_venv", lambda *_: None)
    monkeypatch.setattr(
        cli_mod,
        "_resolve_git_project_branch_context",
        lambda *args, **kwargs: ("main", "project-branch-uid-123"),
    )
    monkeypatch.setattr(cli_mod, "git_origin", lambda *_: "git@github.com:org/repo.git")
    monkeypatch.setattr(
        cli_mod,
        "_ensure_project_repository_ssh_access",
        lambda **kwargs: (key, "pub", {"GIT_SSH_COMMAND": "forced"}),
    )
    monkeypatch.setattr(cli_mod, "ensure_uv_installed", lambda *_: uv_path)
    monkeypatch.setattr(cli_mod, "uv_project_version", lambda *_, **__: "1.2.4")
    monkeypatch.setattr(cli_mod, "uv_preview_patch_version", lambda *_, **__: "1.2.4")
    monkeypatch.setattr(cli_mod, "verify_git_tag_absent", lambda *_, **__: None)
    monkeypatch.setattr(cli_mod, "verify_git_remote_tag_absent", lambda *_, **__: None)
    monkeypatch.setattr(
        cli_mod,
        "render_project_branch_default_redeployment_tag",
        lambda uid, *, version: "v1.2.4",
    )
    monkeypatch.setattr(cli_mod, "run_uv", lambda uv, args, cwd, env=None: None)
    monkeypatch.setattr(cli_mod, "uv_export_requirements", lambda uv, cwd, **kwargs: None)
    monkeypatch.setattr(
        cli_mod,
        "run_cmd",
        lambda cmd, cwd, env=None: git_calls.append((cmd, pathlib.Path(cwd))),
    )

    result = runner.invoke(
        cli_mod.app,
        ["project", "sync", "Update deps"],
    )
    assert result.exit_code == 0
    assert (["git", "commit", "-m", "Update deps"], target) in git_calls
    assert all(cwd == target for _, cwd in git_calls)


@pytest.mark.parametrize(
    "preflight_error",
    [
        "Current Git checkout is detached or has no named branch.",
        "Git branch 'feature/missing' is not registered as a ProjectBranch for this Project.",
    ],
)
def test_project_sync_rejects_invalid_branch_before_local_mutation(
    cli_mod,
    runner,
    monkeypatch,
    tmp_path,
    preflight_error,
):
    target = tmp_path / "project"
    target.mkdir(parents=True, exist_ok=True)
    (target / ".env").write_text("", encoding="utf-8")

    def fail_preflight(*args, **kwargs):
        raise cli_mod.ApiError(preflight_error)

    monkeypatch.setattr(cli_mod, "_resolve_git_project_branch_context", fail_preflight)
    monkeypatch.setattr(
        cli_mod,
        "ensure_venv",
        lambda *_: pytest.fail("ensure_venv must not run before branch preflight"),
    )

    result = runner.invoke(
        cli_mod.app,
        [
            "project",
            "sync",
            "--message",
            "Update deps",
            "--path",
            str(target),
            "--dry-run",
        ],
    )

    assert result.exit_code == 1
    assert f"Project sync preflight failed: {preflight_error}" in result.output


def test_project_sync_dry_run_does_not_mutate(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "project"
    target.mkdir(parents=True, exist_ok=True)
    (target / ".env").write_text(
        "",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        cli_mod,
        "_resolve_git_project_branch_context",
        lambda *args, **kwargs: ("main", "project-branch-uid-123"),
    )
    monkeypatch.setattr(cli_mod, "ensure_venv", lambda *_: None)
    monkeypatch.setattr(cli_mod, "git_origin", lambda *_: "git@github.com:org/repo.git")
    monkeypatch.setattr(cli_mod, "ensure_uv_installed", lambda *_: target / ".venv/bin/uv")
    monkeypatch.setattr(cli_mod, "uv_project_version", lambda *_, **__: "1.2.3")
    monkeypatch.setattr(cli_mod, "uv_preview_patch_version", lambda *_, **__: "1.2.4")
    monkeypatch.setattr(
        cli_mod,
        "render_project_branch_default_redeployment_tag",
        lambda uid, *, version: "v1.2.4",
    )
    monkeypatch.setattr(cli_mod, "verify_git_tag_absent", lambda *_, **__: None)

    def fail_mutation(*args, **kwargs):
        pytest.fail("dry-run must not execute sync mutations")

    for name in (
        "_ensure_project_repository_ssh_access",
        "verify_git_remote_tag_absent",
        "run_uv",
        "uv_export_requirements",
        "run_cmd",
    ):
        monkeypatch.setattr(cli_mod, name, fail_mutation)

    result = runner.invoke(
        cli_mod.app,
        [
            "project",
            "sync",
            "--message",
            "Preview release",
            "--path",
            str(target),
            "--dry-run",
        ],
    )

    assert result.exit_code == 0
    assert "ensure repository SSH key" in result.output
    assert "Next version" in result.output
    assert "1.2.4" in result.output
    assert "v1.2.4" in result.output
    assert "read-only preflight complete; no changes made" in result.output


def test_project_sync_remote_tag_collision_stops_before_mutation(
    cli_mod, runner, monkeypatch, tmp_path
):
    target = tmp_path / "project"
    target.mkdir(parents=True, exist_ok=True)
    (target / ".env").write_text(
        "",
        encoding="utf-8",
    )
    uv_path = target / ".venv" / "bin" / "uv"

    monkeypatch.setattr(cli_mod, "ensure_venv", lambda *_: None)
    monkeypatch.setattr(
        cli_mod,
        "_resolve_git_project_branch_context",
        lambda *args, **kwargs: ("main", "project-branch-uid-123"),
    )
    monkeypatch.setattr(cli_mod, "git_origin", lambda *_: "git@github.com:org/repo.git")
    monkeypatch.setattr(cli_mod, "ensure_uv_installed", lambda *_: uv_path)
    monkeypatch.setattr(cli_mod, "uv_project_version", lambda *_, **__: "1.2.3")
    monkeypatch.setattr(cli_mod, "uv_preview_patch_version", lambda *_, **__: "1.2.4")
    monkeypatch.setattr(
        cli_mod,
        "render_project_branch_default_redeployment_tag",
        lambda uid, *, version: "v1.2.4",
    )
    monkeypatch.setattr(cli_mod, "verify_git_tag_absent", lambda *_, **__: None)
    monkeypatch.setattr(
        cli_mod,
        "_ensure_project_repository_ssh_access",
        lambda **kwargs: (tmp_path / "key", "pub", {"GIT_SSH_COMMAND": "forced"}),
    )
    monkeypatch.setattr(
        cli_mod,
        "verify_git_remote_tag_absent",
        lambda *_, **__: (_ for _ in ()).throw(
            RuntimeError("Git tag already exists remotely: v1.2.4")
        ),
    )
    monkeypatch.setattr(
        cli_mod,
        "run_uv",
        lambda *_, **__: pytest.fail("version mutation must not run after a collision"),
    )
    monkeypatch.setattr(
        cli_mod,
        "run_cmd",
        lambda *_, **__: pytest.fail("Git mutation must not run after a collision"),
    )

    result = runner.invoke(
        cli_mod.app,
        ["project", "sync", "--message", "Update deps", "--path", str(target)],
    )

    assert result.exit_code == 1
    assert "Project sync remote tag preflight failed" in result.output
    assert "already exists remotely: v1.2.4" in result.output


def test_project_sync_version_mismatch_stops_before_lock_or_git_mutation(
    cli_mod, runner, monkeypatch, tmp_path
):
    target = tmp_path / "project"
    target.mkdir(parents=True, exist_ok=True)
    (target / ".env").write_text(
        "",
        encoding="utf-8",
    )
    uv_path = target / ".venv" / "bin" / "uv"
    versions = iter(["1.2.3", "1.2.5"])
    uv_calls = []

    monkeypatch.setattr(cli_mod, "ensure_venv", lambda *_: None)
    monkeypatch.setattr(
        cli_mod,
        "_resolve_git_project_branch_context",
        lambda *args, **kwargs: ("main", "project-branch-uid-123"),
    )
    monkeypatch.setattr(cli_mod, "git_origin", lambda *_: "git@github.com:org/repo.git")
    monkeypatch.setattr(cli_mod, "ensure_uv_installed", lambda *_: uv_path)
    monkeypatch.setattr(cli_mod, "uv_project_version", lambda *_, **__: next(versions))
    monkeypatch.setattr(cli_mod, "uv_preview_patch_version", lambda *_, **__: "1.2.4")
    monkeypatch.setattr(
        cli_mod,
        "render_project_branch_default_redeployment_tag",
        lambda uid, *, version: "v1.2.4",
    )
    monkeypatch.setattr(cli_mod, "verify_git_tag_absent", lambda *_, **__: None)
    monkeypatch.setattr(cli_mod, "verify_git_remote_tag_absent", lambda *_, **__: None)
    monkeypatch.setattr(
        cli_mod,
        "_ensure_project_repository_ssh_access",
        lambda **kwargs: (tmp_path / "key", "pub", {"GIT_SSH_COMMAND": "forced"}),
    )
    monkeypatch.setattr(
        cli_mod,
        "run_uv",
        lambda uv, args, cwd, env=None: uv_calls.append(args),
    )
    monkeypatch.setattr(
        cli_mod,
        "run_cmd",
        lambda *_, **__: pytest.fail("Git mutation must not run after a version mismatch"),
    )

    result = runner.invoke(
        cli_mod.app,
        ["project", "sync", "--message", "Update deps", "--path", str(target)],
    )

    assert result.exit_code == 1
    assert "uv produced 1.2.5; preflight expected 1.2.4" in result.output
    assert uv_calls == [["version", "--bump", "patch"]]


def test_project_schedule_batch_jobs_is_removed(cli_mod, runner):
    result = runner.invoke(cli_mod.app, ["project", "schedule_batch_jobs"])

    assert result.exit_code == 2
    assert "No such command" in result.output


def test_project_sync_project(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "project"
    target.mkdir(parents=True, exist_ok=True)
    (target / ".env").write_text("", encoding="utf-8")
    key = tmp_path / "id_ed25519"
    uv_path = target / ".venv" / "bin" / "uv"
    uv_calls = []
    export_calls = []
    git_calls = []
    monkeypatch.setattr(cli_mod, "ensure_venv", lambda *_: None)
    monkeypatch.setattr(
        cli_mod,
        "_resolve_git_project_branch_context",
        lambda *args, **kwargs: ("main", "project-branch-uid-123"),
    )
    monkeypatch.setattr(cli_mod, "git_origin", lambda *_: "git@github.com:org/repo.git")
    monkeypatch.setattr(
        cli_mod,
        "_ensure_project_repository_ssh_access",
        lambda **kwargs: (key, "pub", {"GIT_SSH_COMMAND": "forced"}),
    )
    monkeypatch.setattr(cli_mod, "ensure_uv_installed", lambda *_: uv_path)
    monkeypatch.setattr(cli_mod, "uv_project_version", lambda *_, **__: "1.2.4")
    monkeypatch.setattr(cli_mod, "uv_preview_patch_version", lambda *_, **__: "1.2.4")
    monkeypatch.setattr(cli_mod, "verify_git_tag_absent", lambda *_, **__: None)
    monkeypatch.setattr(cli_mod, "verify_git_remote_tag_absent", lambda *_, **__: None)
    monkeypatch.setattr(
        cli_mod,
        "render_project_branch_default_redeployment_tag",
        lambda uid, *, version: "v1.2.4",
    )
    monkeypatch.setattr(
        cli_mod,
        "run_uv",
        lambda uv, args, cwd, env=None: uv_calls.append(args),
    )
    monkeypatch.setattr(
        cli_mod,
        "uv_export_requirements",
        lambda uv, cwd, **kwargs: export_calls.append(kwargs),
    )
    monkeypatch.setattr(
        cli_mod,
        "run_cmd",
        lambda cmd, cwd, env=None: git_calls.append(cmd),
    )
    result = runner.invoke(
        cli_mod.app,
        ["project", "sync_project", "Update deps", "--path", str(target)],
    )
    assert result.exit_code == 0
    assert uv_calls == [["version", "--bump", "patch"], ["lock"], ["sync"]]
    assert len(export_calls) == 1
    assert export_calls[0]["locked"] is True
    assert export_calls[0]["no_dev"] is True
    assert export_calls[0]["no_hashes"] is True
    assert export_calls[0]["output_file"] == "requirements.txt"
    assert git_calls == [
        ["git", "add", "-A"],
        ["git", "commit", "-m", "Update deps"],
        ["git", "tag", "-a", "v1.2.4", "-m", "v1.2.4"],
        [
            "git",
            "push",
            "--atomic",
            "--follow-tags",
            "origin",
            "HEAD:refs/heads/main",
            "refs/tags/v1.2.4:refs/tags/v1.2.4",
        ],
    ]


def test_project_sync_project_defaults_to_current_project_dir(
    cli_mod, runner, monkeypatch, tmp_path
):
    target = tmp_path / "project"
    target.mkdir(parents=True, exist_ok=True)
    (target / ".env").write_text("", encoding="utf-8")
    key = tmp_path / "id_ed25519"
    uv_path = target / ".venv" / "bin" / "uv"
    seen = {"cwd": []}

    monkeypatch.chdir(target)
    monkeypatch.setattr(cli_mod, "ensure_venv", lambda *_: None)
    monkeypatch.setattr(
        cli_mod,
        "_resolve_git_project_branch_context",
        lambda *args, **kwargs: ("main", "project-branch-uid-123"),
    )
    monkeypatch.setattr(cli_mod, "git_origin", lambda *_: "git@github.com:org/repo.git")
    monkeypatch.setattr(
        cli_mod,
        "_ensure_project_repository_ssh_access",
        lambda **kwargs: (key, "pub", {"GIT_SSH_COMMAND": "forced"}),
    )
    monkeypatch.setattr(cli_mod, "ensure_uv_installed", lambda *_: uv_path)
    monkeypatch.setattr(cli_mod, "uv_project_version", lambda *_, **__: "1.2.4")
    monkeypatch.setattr(cli_mod, "uv_preview_patch_version", lambda *_, **__: "1.2.4")
    monkeypatch.setattr(cli_mod, "verify_git_tag_absent", lambda *_, **__: None)
    monkeypatch.setattr(cli_mod, "verify_git_remote_tag_absent", lambda *_, **__: None)
    monkeypatch.setattr(
        cli_mod,
        "render_project_branch_default_redeployment_tag",
        lambda uid, *, version: "v1.2.4",
    )
    monkeypatch.setattr(
        cli_mod,
        "run_uv",
        lambda uv, args, cwd, env=None: seen["cwd"].append(cwd),
    )
    monkeypatch.setattr(
        cli_mod,
        "uv_export_requirements",
        lambda uv, cwd, **kwargs: seen["cwd"].append(cwd),
    )
    monkeypatch.setattr(
        cli_mod,
        "run_cmd",
        lambda cmd, cwd, env=None: seen["cwd"].append(cwd),
    )
    result = runner.invoke(
        cli_mod.app,
        ["project", "sync_project", "Update deps"],
    )
    assert result.exit_code == 0
    assert seen["cwd"]
    assert all(pathlib.Path(cwd) == target for cwd in seen["cwd"])


def test_project_build_docker_env(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "project"
    target.mkdir(parents=True, exist_ok=True)
    (target / "Dockerfile").write_text("FROM python:3.11\n", encoding="utf-8")

    monkeypatch.setattr(cli_mod, "compute_docker_image_ref", lambda _: "demo-img:tag")
    monkeypatch.setattr(
        cli_mod,
        "write_devcontainer_config",
        lambda project_dir, image_ref: project_dir / ".devcontainer" / "devcontainer.json",
    )
    monkeypatch.setattr(cli_mod, "build_docker_environment", lambda project_dir, image_ref: 0)

    result = runner.invoke(
        cli_mod.app,
        ["project", "build-docker-env", "--path", str(target)],
    )
    assert result.exit_code == 0
    assert "Docker image built: demo-img:tag" in result.output


def test_project_current(cli_mod, runner, monkeypatch, tmp_path):
    project_path = tmp_path / "org" / "projects" / "demo-project-uid-123"
    project_path.mkdir(parents=True, exist_ok=True)

    project_info = types.SimpleNamespace(
        path=str(project_path),
        folder="demo-project-uid-123",
        project_uid="project-uid-123",
        project_id="123",
        venv_path=None,
        python_version=None,
    )
    debug = types.SimpleNamespace(reason="detected", checks=[])

    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": str(tmp_path)},
    )
    monkeypatch.setattr(
        cli_mod, "detect_current_project", lambda workspaces, base: (project_info, debug)
    )
    monkeypatch.setattr(cli_mod, "read_local_sdk_version", lambda req: "1.2.3")
    monkeypatch.setattr(cli_mod, "fetch_latest_sdk_version", lambda: "1.2.3")
    monkeypatch.setattr(
        cli_mod,
        "get_project_runtime_context",
        lambda *args, **kwargs: types.SimpleNamespace(
            project_uid="project-uid-123",
            repository_branch="main",
            canonical_repository_identity="github.com/org/demo",
            commit_sha="a" * 40,
            project_branch_uid="project-branch-uid-123",
            status="resolved",
            detail="",
        ),
    )

    result = runner.invoke(cli_mod.app, ["project", "current"])
    assert result.exit_code == 0
    assert "Current Project" in result.output
    assert "main" in result.output
    assert "project-branch-uid-123" in result.output


def test_project_current_json(cli_mod, runner, monkeypatch, tmp_path):
    project_path = tmp_path / "org" / "projects" / "demo-project-uid-123"
    project_path.mkdir(parents=True, exist_ok=True)

    project_info = types.SimpleNamespace(
        path=str(project_path),
        folder="demo-project-uid-123",
        project_uid="project-uid-123",
        project_id="123",
        venv_path=None,
        python_version=None,
    )
    debug = types.SimpleNamespace(reason="detected", checks=[])

    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": str(tmp_path)},
    )
    monkeypatch.setattr(
        cli_mod, "detect_current_project", lambda workspaces, base: (project_info, debug)
    )
    monkeypatch.setattr(cli_mod, "read_local_sdk_version", lambda req: "1.2.3")
    monkeypatch.setattr(cli_mod, "fetch_latest_sdk_version", lambda: "1.2.3")
    monkeypatch.setattr(
        cli_mod,
        "get_project_runtime_context",
        lambda *args, **kwargs: types.SimpleNamespace(
            project_uid="project-uid-123",
            repository_branch="main",
            canonical_repository_identity="github.com/org/demo",
            commit_sha="a" * 40,
            project_branch_uid="project-branch-uid-123",
            status="resolved",
            detail="",
        ),
    )

    result = runner.invoke(cli_mod.app, ["project", "current", "--json"])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["project"]["project_uid"] == "project-uid-123"
    assert payload["project"]["git_branch"] == "main"
    assert payload["project"]["project_branch_uid"] == "project-branch-uid-123"
    assert payload["project"]["project_branch_status"] == "resolved"
    assert payload["project"]["project_branch_error"] is None
    assert payload["sdk_status"]["status"] == "match"


def test_project_sdk_status(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "project"
    target.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(cli_mod, "read_local_sdk_version", lambda req: "1.2.3")
    monkeypatch.setattr(cli_mod, "fetch_latest_sdk_version", lambda: "v1.2.3")

    result = runner.invoke(
        cli_mod.app,
        ["project", "sdk-status", "--path", str(target)],
    )
    assert result.exit_code == 0
    assert "SDK Status" in result.output


def test_project_sdk_status_json(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "project"
    target.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(cli_mod, "read_local_sdk_version", lambda req: "1.2.3")
    monkeypatch.setattr(cli_mod, "fetch_latest_sdk_version", lambda: "v1.2.3")

    result = runner.invoke(
        cli_mod.app,
        ["project", "sdk-status", "--path", str(target), "--json"],
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["project"] == str(target)
    assert payload["latest_github"] == "v1.2.3"
    assert payload["local_requirements_txt"] == "1.2.3"


def test_project_update_sdk(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "project"
    target.mkdir(parents=True, exist_ok=True)
    uv_path = target / ".venv" / "bin" / "uv"
    calls = []

    monkeypatch.setattr(cli_mod, "ensure_venv", lambda *_: None)
    monkeypatch.setattr(cli_mod, "ensure_uv_installed", lambda *_: uv_path)
    monkeypatch.setattr(cli_mod, "run_uv", lambda uv, args, cwd, env=None: calls.append(args))

    result = runner.invoke(
        cli_mod.app,
        ["project", "update-sdk", "--path", str(target)],
    )
    assert result.exit_code == 0
    assert ["lock", "--upgrade-package", "mainsequence"] in calls
    assert ["sync"] in calls


def _write_installed_agent_scaffold_bundle(bundle_dir: pathlib.Path) -> pathlib.Path:
    bundle_dir.mkdir(parents=True, exist_ok=True)
    agents_md = bundle_dir / "AGENTS.md"
    agents_md.write_text(
        "\n".join(
            [
                "# AGENTS.md",
                "",
                "Installed scaffold preface.",
                "",
                "Do not remove the `<!-- mainsequence-agent-scaffold:start schema=1 "
                "source=agent_scaffold -->`",
                "or `<!-- mainsequence-agent-scaffold:end -->` markers.",
                "",
                "<!-- mainsequence-agent-scaffold:start schema=1 source=agent_scaffold -->",
                "## Main Sequence Instructions",
                "",
                "Installed managed block.",
                "",
                "<!-- mainsequence-agent-scaffold:end -->",
                "",
                "Installed scaffold footer.",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return agents_md


def test_project_update_agents_md_replaces_custom_unmarked_file(
    cli_mod,
    runner,
    monkeypatch,
    tmp_path,
):
    agents_md = _write_installed_agent_scaffold_bundle(tmp_path / "bundle")
    target = tmp_path / "project"
    target.mkdir()
    original = "# Project Agents\n\nKeep this project-specific instruction.\n"
    (target / "AGENTS.md").write_text(original, encoding="utf-8")
    monkeypatch.setattr(cli_mod, "_installed_agent_scaffold_bundle_dir", lambda: agents_md.parent)

    result = runner.invoke(
        cli_mod.app,
        ["project", "update", "AGENTS.md", "--path", str(target)],
    )
    assert result.exit_code == 0
    content = (target / "AGENTS.md").read_text(encoding="utf-8")
    assert content == agents_md.read_text(encoding="utf-8")
    assert "Installed managed block." in cli_mod._extract_agents_md_managed_block(content)
    assert "Installed managed block." in content
    assert "Keep this project-specific instruction." not in content
    assert "Action" in result.output
    assert "replaced" in result.output


def test_project_update_agents_md_creates_file_when_missing(
    cli_mod,
    runner,
    monkeypatch,
    tmp_path,
):
    agents_md = _write_installed_agent_scaffold_bundle(tmp_path / "bundle")
    target = tmp_path / "project"
    target.mkdir()
    monkeypatch.setattr(cli_mod, "_installed_agent_scaffold_bundle_dir", lambda: agents_md.parent)

    result = runner.invoke(
        cli_mod.app,
        ["project", "update", "AGENTS.md", "--path", str(target)],
    )
    assert result.exit_code == 0
    content = (target / "AGENTS.md").read_text(encoding="utf-8")
    assert content == agents_md.read_text(encoding="utf-8")
    assert cli_mod.AGENTS_MD_MANAGED_BLOCK_START_PREFIX in content
    assert "Action" in result.output
    assert "created" in result.output


def test_project_update_agents_md_replaces_existing_managed_block(
    cli_mod,
    runner,
    monkeypatch,
    tmp_path,
):
    agents_md = _write_installed_agent_scaffold_bundle(tmp_path / "bundle")
    target = tmp_path / "project"
    target.mkdir()
    old_block = "\n".join(
        [
            f"{cli_mod.AGENTS_MD_MANAGED_BLOCK_START_PREFIX} schema=old source=old -->",
            "old managed content",
            cli_mod.AGENTS_MD_MANAGED_BLOCK_END,
        ]
    )
    (target / "AGENTS.md").write_text(
        f"# Project Agents\n\n{old_block}\n\nCustom tail.\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(cli_mod, "_installed_agent_scaffold_bundle_dir", lambda: agents_md.parent)

    result = runner.invoke(
        cli_mod.app,
        ["project", "update", "AGENTS.md", "--path", str(target)],
    )
    assert result.exit_code == 0
    content = (target / "AGENTS.md").read_text(encoding="utf-8")
    assert "old managed content" not in content
    assert "Installed managed block." in content
    assert "Custom tail.\n" in content
    assert "Installed managed block." in cli_mod._extract_agents_md_managed_block(content)
    assert "updated" in result.output


def test_project_update_agents_md_json_reports_action(
    cli_mod,
    runner,
    monkeypatch,
    tmp_path,
):
    agents_md = _write_installed_agent_scaffold_bundle(tmp_path / "bundle")
    target = tmp_path / "project"
    target.mkdir()
    monkeypatch.setattr(cli_mod, "_installed_agent_scaffold_bundle_dir", lambda: agents_md.parent)

    result = runner.invoke(
        cli_mod.app,
        ["project", "update", "AGENTS.md", "--path", str(target), "--json"],
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["target"] == "AGENTS.md"
    assert payload["project"] == str(target)
    assert payload["source"] == str(agents_md)
    assert payload["destination"] == str(target / "AGENTS.md")
    assert payload["action"] == "created"
    assert payload["changed"] is True
    assert payload["overwritten"] is False


def test_project_update_agents_md_keeps_existing_scaffold_shaped_file_current(
    cli_mod,
    runner,
    monkeypatch,
    tmp_path,
):
    agents_md = _write_installed_agent_scaffold_bundle(tmp_path / "bundle")
    target = tmp_path / "project"
    target.mkdir()
    (target / "AGENTS.md").write_text(agents_md.read_text(encoding="utf-8"), encoding="utf-8")
    monkeypatch.setattr(cli_mod, "_installed_agent_scaffold_bundle_dir", lambda: agents_md.parent)

    result = runner.invoke(
        cli_mod.app,
        ["project", "update", "AGENTS.md", "--path", str(target)],
    )
    assert result.exit_code == 0
    assert (target / "AGENTS.md").read_text(encoding="utf-8") == agents_md.read_text(
        encoding="utf-8"
    )
    assert "already current" in result.output


def test_project_update_agents_md_rejects_malformed_managed_block(
    cli_mod,
    runner,
    monkeypatch,
    tmp_path,
):
    agents_md = _write_installed_agent_scaffold_bundle(tmp_path / "bundle")
    target = tmp_path / "project"
    target.mkdir()
    start_marker = (
        f"{cli_mod.AGENTS_MD_MANAGED_BLOCK_START_PREFIX} schema=1 source=agent_scaffold -->"
    )
    (target / "AGENTS.md").write_text(
        f"# Project Agents\n\n{start_marker}\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(cli_mod, "_installed_agent_scaffold_bundle_dir", lambda: agents_md.parent)

    result = runner.invoke(
        cli_mod.app,
        ["project", "update", "AGENTS.md", "--path", str(target)],
    )
    assert result.exit_code == 1
    assert "malformed" in result.output


def test_project_update_agents_md_rejects_duplicate_managed_blocks(
    cli_mod,
    runner,
    monkeypatch,
    tmp_path,
):
    agents_md = _write_installed_agent_scaffold_bundle(tmp_path / "bundle")
    target = tmp_path / "project"
    target.mkdir()
    block = "\n".join(
        [
            f"{cli_mod.AGENTS_MD_MANAGED_BLOCK_START_PREFIX} schema=1 source=agent_scaffold -->",
            "managed content",
            cli_mod.AGENTS_MD_MANAGED_BLOCK_END,
        ]
    )
    (target / "AGENTS.md").write_text(f"{block}\n\n{block}\n", encoding="utf-8")
    monkeypatch.setattr(cli_mod, "_installed_agent_scaffold_bundle_dir", lambda: agents_md.parent)

    result = runner.invoke(
        cli_mod.app,
        ["project", "update", "AGENTS.md", "--path", str(target)],
    )
    assert result.exit_code == 1
    assert "multiple" in result.output


def test_project_update_agents_md_fails_when_installed_bundle_missing_agents_md(
    cli_mod,
    runner,
    monkeypatch,
    tmp_path,
):
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir()
    target = tmp_path / "project"
    target.mkdir()
    monkeypatch.setattr(cli_mod, "_installed_agent_scaffold_bundle_dir", lambda: bundle_dir)

    result = runner.invoke(
        cli_mod.app,
        ["project", "update", "AGENTS.md", "--path", str(target)],
    )
    assert result.exit_code == 1
    assert "Installed agent_scaffold bundle is missing AGENTS.md" in result.output


def test_installed_agent_scaffold_bundle_dir_falls_back_to_sibling_package(cli_mod, monkeypatch):
    original_import_module = cli_mod.importlib.import_module

    def _import_module(name, package=None):
        if name == "agent_scaffold":
            raise ModuleNotFoundError("No module named 'agent_scaffold'")
        return original_import_module(name, package)

    monkeypatch.setattr(cli_mod.importlib, "import_module", _import_module)

    resolved = cli_mod._installed_agent_scaffold_bundle_dir()
    expected = pathlib.Path(cli_mod.__file__).resolve().parents[2] / "agent_scaffold"
    assert resolved == expected.resolve()


def _cli_platform_skill_catalog():
    from pathlib import PurePosixPath

    from mainsequence.project_skills import (
        PLATFORM_ONTOLOGY_URI,
        PLATFORM_SKILL_URI_PREFIX,
        PlatformProjectResource,
        PlatformProjectSkill,
        PlatformProjectSkillCatalog,
    )

    ontology_content = '{"title":"Main Sequence","description":"Ontology"}\n'
    ontology = PlatformProjectResource(
        name="ontology",
        uri=PLATFORM_ONTOLOGY_URI,
        resource_path=PurePosixPath("ontology/platform.json"),
        content=ontology_content,
        content_sha256=hashlib.sha256(ontology_content.encode("utf-8")).hexdigest(),
        content_mime_type="application/json",
        content_size=len(ontology_content.encode("utf-8")),
    )
    skills = []
    for name in (
        "a2a_communication",
        "project_design",
        "project_to_agent",
    ):
        front_matter_name = name.replace("_", "-")
        uri = f"{PLATFORM_SKILL_URI_PREFIX}{front_matter_name}"
        content = f"---\nname: {front_matter_name}\ndescription: Platform {name}\n---\n"
        skills.append(
            PlatformProjectSkill(
                name=name,
                uri=uri,
                resource_path=PurePosixPath(
                    "skills",
                    name,
                    "SKILL.md",
                ),
                relative_path=PurePosixPath(name, "SKILL.md"),
                content=content,
                content_sha256=hashlib.sha256(content.encode("utf-8")).hexdigest(),
                content_mime_type="text/markdown",
                content_size=len(content.encode("utf-8")),
            )
        )
    return PlatformProjectSkillCatalog(
        source_url="https://platform.example.test/mcp",
        manifest_version=2,
        manifest_sha256="a" * 64,
        ontology=ontology,
        skills=tuple(skills),
    )


def test_project_update_agent_skills_overwrites_matching_folders(
    cli_mod, runner, monkeypatch, tmp_path
):
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir()
    (bundle_dir / "AGENTS.md").write_text("bundle agents", encoding="utf-8")
    (bundle_dir / "skills" / "data_publishing").mkdir(parents=True)
    (bundle_dir / "skills" / "data_publishing" / "SKILL.md").write_text(
        "new data skill", encoding="utf-8"
    )
    (bundle_dir / "skills" / "maintenance").mkdir(parents=True)
    (bundle_dir / "skills" / "maintenance" / "SKILL.md").write_text(
        "new maintenance skill", encoding="utf-8"
    )
    (bundle_dir / "skills" / "__pycache__").mkdir()
    (bundle_dir / "skills" / "__pycache__" / "ignored.txt").write_text(
        "ignore me", encoding="utf-8"
    )

    target = tmp_path / "project"
    stale_top_level = target / ".agents" / "skills" / "data_publishing"
    stale_top_level.mkdir(parents=True)
    (stale_top_level / "old.txt").write_text("stale but preserved", encoding="utf-8")
    existing_mainsequence = target / ".agents" / "skills" / "mainsequence" / "data_publishing"
    existing_mainsequence.mkdir(parents=True)
    (existing_mainsequence / "old.txt").write_text("stale mainsequence skill", encoding="utf-8")

    monkeypatch.setattr(
        cli_mod, "_project_agent_scaffold_bundle_dir", lambda project_dir: bundle_dir
    )
    monkeypatch.setattr(
        cli_mod,
        "_project_installed_package_version",
        lambda project_dir, package_name: "4.4.3",
    )
    monkeypatch.setattr(
        cli_mod,
        "fetch_platform_project_skill_catalog",
        _cli_platform_skill_catalog,
    )

    result = runner.invoke(cli_mod.app, ["project", "update_agent_skills", "--path", str(target)])
    assert result.exit_code == 0
    assert (
        target / ".agents" / "skills" / "mainsequence" / "data_publishing" / "SKILL.md"
    ).read_text(encoding="utf-8") == "new data skill"
    assert not (
        target / ".agents" / "skills" / "mainsequence" / "data_publishing" / "old.txt"
    ).exists()
    assert (target / ".agents" / "skills" / "data_publishing" / "old.txt").exists()
    assert (target / ".agents" / "skills" / "mainsequence" / "maintenance" / "SKILL.md").read_text(
        encoding="utf-8"
    ) == "new maintenance skill"
    assert not (target / ".agents" / "skills" / "mainsequence" / "__pycache__").exists()
    sentinel = target / ".agents" / "skills" / "mainsequence" / "PINNED_FROM.txt"
    sentinel_content = sentinel.read_text(encoding="utf-8")
    assert "library_name=mainsequence" in sentinel_content
    assert "namespace=mainsequence" in sentinel_content
    assert "pinned_version=4.4.3" in sentinel_content
    assert f"skills_path={(bundle_dir / 'skills').resolve()}" in sentinel_content
    assert "sdk_version=4.4.3" in sentinel_content
    assert f"platform_manifest_sha256={'a' * 64}" in sentinel_content
    assert "Updated Project Skills" in result.output
    assert "SDK Version" in result.output
    assert (
        target / ".agents" / "skills" / "mainsequence" / "project_design" / "SKILL.md"
    ).is_file()
    assert (
        target / ".agents" / "skills" / "mainsequence" / "a2a_communication" / "SKILL.md"
    ).is_file()
    assert (
        target / ".agents" / "skills" / "mainsequence" / "project_to_agent" / "SKILL.md"
    ).is_file()


def test_project_update_agent_skills_json_reports_pin_sentinel(
    cli_mod, runner, monkeypatch, tmp_path
):
    bundle_dir = tmp_path / "bundle"
    (bundle_dir / "skills" / "data_publishing").mkdir(parents=True)
    (bundle_dir / "skills" / "data_publishing" / "SKILL.md").write_text(
        "new data skill", encoding="utf-8"
    )
    target = tmp_path / "project"
    target.mkdir()

    monkeypatch.setattr(
        cli_mod, "_project_agent_scaffold_bundle_dir", lambda project_dir: bundle_dir
    )
    monkeypatch.setattr(
        cli_mod,
        "_project_installed_package_version",
        lambda project_dir, package_name: "4.4.3",
    )
    monkeypatch.setattr(
        cli_mod,
        "fetch_platform_project_skill_catalog",
        _cli_platform_skill_catalog,
    )

    result = runner.invoke(
        cli_mod.app,
        ["project", "update_agent_skills", "--path", str(target), "--json"],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    sentinel = target / ".agents" / "skills" / "mainsequence" / "PINNED_FROM.txt"
    assert payload["library_name"] == "mainsequence"
    assert payload["namespace"] == "mainsequence"
    assert payload["pinned_version"] == "4.4.3"
    assert payload["sentinel_path"] == str(sentinel.resolve())
    assert payload["destination_root"] == str(
        (target / ".agents" / "skills" / "mainsequence").resolve()
    )
    assert payload["updated_count"] == 4
    assert [item["name"] for item in payload["updated"]] == [
        "data_publishing",
        "a2a_communication",
        "project_design",
        "project_to_agent",
    ]
    assert payload["sdk"]["version"] == "4.4.3"
    assert payload["platform"]["manifest_sha256"] == "a" * 64
    assert [item["name"] for item in payload["platform"]["skills"]] == [
        "a2a_communication",
        "project_design",
        "project_to_agent",
    ]
    assert "pinned_version=4.4.3" in sentinel.read_text(encoding="utf-8")


def test_login_live_with_env_tokens(cli_mod, runner, monkeypatch):
    """
    Optional live JWT import check.

    Set:
      - MAINSEQUENCE_TEST_ACCESS_TOKEN
      - MAINSEQUENCE_TEST_REFRESH_TOKEN
    """
    access_token = os.getenv("MAINSEQUENCE_TEST_ACCESS_TOKEN")
    refresh_token = os.getenv("MAINSEQUENCE_TEST_REFRESH_TOKEN")
    if not access_token or not refresh_token:
        pytest.skip("Missing MAINSEQUENCE_TEST_ACCESS_TOKEN / MAINSEQUENCE_TEST_REFRESH_TOKEN")

    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": "/tmp/mainsequence"},
    )
    result = runner.invoke(
        cli_mod.app,
        [
            "login",
            "--access-token",
            access_token,
            "--refresh-token",
            refresh_token,
            "--no-status",
        ],
    )
    assert result.exit_code == 0
