from __future__ import annotations

import importlib
import json
import os
import sys
import types

import pytest


def test_code_repository_search(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "search_code_repositories",
        lambda q, limit=20, timeout=None: [
            {
                "uid": "code-repository-uid-11",
                "code_repository_name": "alpha-research",
                "repository_branch": "main",
                "cluster_id": 7,
            },
            {
                "uid": "code-repository-uid-12",
                "code_repository_name": "data-live",
                "repository_branch": "release",
                "cluster_id": 9,
            },
        ],
    )

    result = runner.invoke(cli_mod.app, ["code-repository", "search", "alpha", "--limit", "10"])
    assert result.exit_code == 0
    assert "CodeRepository Search Results" in result.output
    assert "UID" in result.output
    assert "code-repository-uid-11" in result.output
    assert "CodeRepository Name" in result.output
    assert "alpha-research" in result.output
    assert "data-live" in result.output
    assert 'CodeRepository search matches for "alpha": 2' in result.output


def test_code_repository_search_rejects_short_query(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "search_code_repositories",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("search_code_repositories should not be called")
        ),
    )

    result = runner.invoke(cli_mod.app, ["code-repository", "search", ".."])
    assert result.exit_code == 1
    assert (
        "CodeRepository search failed: Query must contain at least 3 characters." in result.output
    )


def test_code_repository_search_json(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "search_code_repositories",
        lambda q, limit=20, timeout=None: [
            {
                "uid": "code-repository-uid-11",
                "code_repository_name": "alpha-research",
                "repository_branch": "main",
                "cluster_id": 7,
            },
        ],
    )

    result = runner.invoke(cli_mod.app, ["code-repository", "search", "alpha", "--json"])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload == [
        {
            "uid": "code-repository-uid-11",
            "code_repository_name": "alpha-research",
            "repository_branch": "main",
            "cluster_id": 7,
        }
    ]


def test_get_code_repository_repository_uses_public_client_model(cli_mod, monkeypatch):
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

        class ClientGitHubRepositoryBinding:
            @classmethod
            def get_by_uid(cls, uid, timeout=None):
                captured["uid"] = uid
                captured["timeout"] = timeout
                return FakeRepository()

        return operation(ClientGitHubRepositoryBinding)

    monkeypatch.setattr(api_mod, "_run_sdk_model_operation", _run_sdk_model_operation)

    result = api_mod.get_code_repository_repository(repository_uid, timeout=12)

    assert captured == {
        "module_name": "mainsequence.client.models_foundry",
        "class_name": "GitHubRepositoryBinding",
        "uid": repository_uid,
        "timeout": 12,
    }
    assert result["uid"] == repository_uid
    assert result["git_ssh_url"].startswith("git@github.com:")


def test_code_repository_list(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": "/tmp/mainsequence"},
    )
    monkeypatch.setattr(cli_mod, "_org_slug_from_profile", lambda: "org")
    monkeypatch.setattr(
        cli_mod,
        "get_code_repositories",
        lambda: [
            {
                "uid": "code-repository-uid-1",
                "code_repository_name": "Demo",
                "is_initialized": True,
            }
        ],
    )
    result = runner.invoke(cli_mod.app, ["code-repository", "list"])
    assert result.exit_code == 0
    assert "UID" in result.output
    assert "code-repository-uid-1" in result.output
    assert "Branches" in result.output
    assert "Local" not in result.output
    assert "Demo" in result.output
    assert "Data Source" not in result.output
    assert "Class" not in result.output


def test_code_repository_add_label(cli_mod, runner, monkeypatch):
    captured = {}
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _add(code_repository_id, labels, timeout=None):
        captured["code_repository_id"] = code_repository_id
        captured["labels"] = labels
        captured["timeout"] = timeout
        return {"labels": [{"name": "rates"}, {"name": "research"}]}

    monkeypatch.setattr(cli_mod, "add_code_repository_labels", _add)

    result = runner.invoke(
        cli_mod.app,
        ["code-repository", "add-label", "code-repository-uid-4", "--label", "rates,research"],
    )
    assert result.exit_code == 0
    assert captured == {
        "code_repository_id": "code-repository-uid-4",
        "labels": ["rates", "research"],
        "timeout": None,
    }
    assert "CodeRepository add-label completed." in result.output
    assert "rates, research" in result.output


def test_validate_code_repository_name_uses_client_model(cli_mod, monkeypatch):
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

    class FakeCodeRepository:
        ROOT_URL = "https://old.test/api/v1/code-repositories"

        @classmethod
        def validate_name(cls, *, code_repository_name, timeout=None):
            captured["code_repository_name"] = code_repository_name
            captured["timeout"] = timeout
            return types.SimpleNamespace(
                model_dump=lambda mode="json": {
                    "code_repository_name": code_repository_name,
                    "available": False,
                    "reason": "A code repository with this name already exists in your organization.",
                    "normalized": {
                        "slugified_code_repository_name": "rates-platform",
                        "repository_library_name": "rates_platform",
                    },
                    "suggestions": ["Rates Platform 2", "Rates Platform 3"],
                }
            )

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.CodeRepository = FakeCodeRepository
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_foundry", fake_models)

    out = api_mod.validate_code_repository_name(code_repository_name="Rates Platform", timeout=25)

    assert captured["jwt"] == ("acc", "ref")
    assert captured["code_repository_name"] == "Rates Platform"
    assert captured["timeout"] == 25
    assert out["available"] is False
    assert out["normalized"]["repository_library_name"] == "rates_platform"


def test_search_code_repositories_uses_client_model(cli_mod, monkeypatch):
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

    class FakeCodeRepository:
        ROOT_URL = "https://old.test/api/v1/code-repositories"

        @classmethod
        def quick_search(cls, q, *, limit=20, timeout=None):
            captured["q"] = q
            captured["limit"] = limit
            captured["timeout"] = timeout
            return [
                types.SimpleNamespace(
                    model_dump=lambda mode="json": {
                        "uid": "code-repository-uid-11",
                        "code_repository_name": "alpha-research",
                        "code_repository_type": "python",
                        "cluster_id": 7,
                    }
                ),
                types.SimpleNamespace(
                    model_dump=lambda mode="json": {
                        "uid": "code-repository-uid-12",
                        "code_repository_name": "data-live",
                        "code_repository_type": "vite_react",
                        "cluster_id": 9,
                    }
                ),
            ]

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.CodeRepository = FakeCodeRepository
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_foundry", fake_models)

    out = api_mod.search_code_repositories("alpha", limit=10, timeout=12)

    assert captured["jwt"] == ("acc", "ref")
    assert captured["q"] == "alpha"
    assert captured["limit"] == 10
    assert captured["timeout"] == 12
    assert out == [
        {
            "uid": "code-repository-uid-11",
            "code_repository_name": "alpha-research",
            "code_repository_type": "python",
            "cluster_id": 7,
        },
        {
            "uid": "code-repository-uid-12",
            "code_repository_name": "data-live",
            "code_repository_type": "vite_react",
            "cluster_id": 9,
        },
    ]


def test_create_code_repository_does_not_send_code_repository_visible(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    class FakeResponse:
        ok = True
        status_code = 201
        headers = {"content-type": "application/json"}

        def json(self):
            return {"id": 321, "code_repository_name": "demo-repository"}

    def _fake_authed(method, api_path, body=None):
        captured["method"] = method
        captured["api_path"] = api_path
        captured["body"] = body
        return FakeResponse()

    monkeypatch.setattr(api_mod, "authed", _fake_authed)

    out = api_mod.create_code_repository(
        code_repository_name="demo-repository",
        default_base_image_uid="22222222-2222-4222-8222-222222222222",
        github_org_uid="33333333-3333-4333-8333-333333333333",
        bootstrap_organization_environment_uid="44444444-4444-4444-8444-444444444444",
    )

    assert captured["method"] == "POST"
    assert captured["api_path"] == "/api/v1/code-repositories/"
    assert captured["body"] == {
        "code_repository_name": "demo-repository",
        "code_repository_type": "python",
        "default_base_image_uid": "22222222-2222-4222-8222-222222222222",
        "github_org_uid": "33333333-3333-4333-8333-333333333333",
        "bootstrap_organization_environment_uid": "44444444-4444-4444-8444-444444444444",
    }
    assert "project_visible" not in captured["body"]
    assert out == {"id": 321, "code_repository_name": "demo-repository"}


def test_code_repository_validate_name_cmd(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "validate_code_repository_name",
        lambda code_repository_name, timeout=None: {
            "code_repository_name": code_repository_name,
            "available": False,
            "reason": "A code repository with this name already exists in your organization.",
            "normalized": {
                "slugified_code_repository_name": "rates-platform",
                "repository_library_name": "rates_platform",
            },
            "suggestions": ["Rates Platform 2", "Rates Platform 3"],
        },
    )

    result = runner.invoke(cli_mod.app, ["code-repository", "validate-name", "Rates Platform"])
    assert result.exit_code == 1
    assert "CodeRepository Name Validation" in result.output
    assert "rates-platform" in result.output
    assert "rates_platform" in result.output
    assert "Rates Platform 2" in result.output
    assert "Rates Platform 3" in result.output


def test_code_repository_list_requires_shell_auth_hint(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "get_current_user_profile", lambda: {})
    result = runner.invoke(cli_mod.app, ["code-repository", "list"])
    assert result.exit_code == 1
    assert "Not logged in. Run: mainsequence login" in result.output


def test_code_repository_create_interactive_defaults(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "validate_code_repository_name",
        lambda code_repository_name, timeout=None: {
            "code_repository_name": code_repository_name,
            "available": True,
            "reason": None,
            "normalized": {
                "slugified_code_repository_name": "demo-repository",
                "repository_library_name": "demo_repository",
            },
            "suggestions": [],
        },
    )
    monkeypatch.setattr(
        cli_mod,
        "list_code_repository_base_images",
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

    def _create_code_repository(**kwargs):
        captured.update(kwargs)
        return {
            "uid": "code-repository-uid-321",
            "code_repository_name": kwargs["code_repository_name"],
        }

    monkeypatch.setattr(cli_mod, "create_code_repository", _create_code_repository)

    # Prompts:
    # 1) CodeRepository name
    # 2) Default base image uid
    # 3) GitHub organization uid
    user_input = "demo-repository\n\n\n"
    result = runner.invoke(cli_mod.app, ["code-repository", "create"], input=user_input)

    assert result.exit_code == 0
    assert captured["code_repository_name"] == "demo-repository"
    assert captured["code_repository_type"] == "python"
    assert captured["default_base_image_uid"] == "22222222-2222-4222-8222-222222222222"
    assert captured["github_org_uid"] == "33333333-3333-4333-8333-333333333333"
    assert captured["bootstrap_organization_environment_uid"] is None
    assert "CodeRepository created: demo-repository (uid=code-repository-uid-321)" in result.output


def test_code_repository_create_with_explicit_options_returns_logical_code_repository(
    cli_mod,
    runner,
    monkeypatch,
):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "validate_code_repository_name",
        lambda code_repository_name, timeout=None: {
            "code_repository_name": code_repository_name,
            "available": True,
            "reason": None,
            "normalized": {
                "slugified_code_repository_name": "demo-repository",
                "repository_library_name": "demo_repository",
            },
            "suggestions": [],
        },
    )
    captured = {}

    def _create_code_repository(**kwargs):
        captured.update(kwargs)
        return {
            "uid": "77777777-7777-4777-8777-777777777777",
            "code_repository_name": kwargs["code_repository_name"],
        }

    monkeypatch.setattr(cli_mod, "create_code_repository", _create_code_repository)

    result = runner.invoke(
        cli_mod.app,
        [
            "code-repository",
            "create",
            "demo-repository",
            "--default-base-image-uid",
            "22222222-2222-4222-8222-222222222222",
            "--github-org-uid",
            "33333333-3333-4333-8333-333333333333",
            "--environment-uid",
            "44444444-4444-4444-8444-444444444444",
        ],
    )

    assert result.exit_code == 0
    assert "default_metatables_data_source_uid" not in captured
    assert (
        captured["bootstrap_organization_environment_uid"] == "44444444-4444-4444-8444-444444444444"
    )
    assert "CodeRepository created: demo-repository" in result.output


def test_code_repository_create_rejects_unavailable_name(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "validate_code_repository_name",
        lambda code_repository_name, timeout=None: {
            "code_repository_name": code_repository_name,
            "available": False,
            "reason": "A code repository with this name already exists in your organization.",
            "normalized": {
                "slugified_code_repository_name": "demo-repository",
                "repository_library_name": "demo_repository",
            },
            "suggestions": ["Demo CodeRepository 2", "Demo CodeRepository 3"],
        },
    )

    result = runner.invoke(cli_mod.app, ["code-repository", "create", "Demo CodeRepository"])

    assert result.exit_code == 1
    assert "A code repository with this name already exists in your organization." in result.output
    assert "CodeRepository Name Validation" in result.output
    assert "Demo CodeRepository 2" in result.output
    assert "Demo CodeRepository 3" in result.output


def test_code_repository_delete_remote_yes(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "resolve_code_repository",
        lambda code_repository_id: {
            "id": 321,
            "uid": "code-repository-uid-321",
            "code_repository_name": "Demo CodeRepository",
        },
    )

    captured = {}

    def _bulk_delete_code_repositories(*, uids):
        captured["uids"] = uids
        return {"detail": "deleted", "deleted_count": 1}

    monkeypatch.setattr(cli_mod, "bulk_delete_code_repositories", _bulk_delete_code_repositories)

    result = runner.invoke(
        cli_mod.app,
        ["code-repository", "delete", "code-repository-uid-321", "--yes"],
    )
    assert result.exit_code == 0
    assert captured["uids"] == ["code-repository-uid-321"]
    assert (
        "CodeRepository deleted: Demo CodeRepository (uid=code-repository-uid-321; deleted=1)"
        in result.output
    )


def test_code_repository_freeze_env_is_removed(cli_mod, runner):
    result = runner.invoke(cli_mod.app, ["code-repository", "freeze-env", "--path", "."])

    assert result.exit_code == 2
    assert "No such command" in result.output


def test_code_repository_schedule_batch_jobs_is_removed(cli_mod, runner):
    result = runner.invoke(cli_mod.app, ["code-repository", "schedule_batch_jobs"])

    assert result.exit_code == 2
    assert "No such command" in result.output


def test_code_repository_current(cli_mod, runner, monkeypatch, tmp_path):
    code_repository_path = tmp_path / "org" / "projects" / "demo-code-repository-uid-123"
    code_repository_path.mkdir(parents=True, exist_ok=True)

    code_repository_info = types.SimpleNamespace(
        path=str(code_repository_path),
        folder="demo-code-repository-uid-123",
        code_repository_uid="code-repository-uid-123",
        code_repository_id="123",
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
        cli_mod,
        "detect_current_code_repository",
        lambda workspaces, base: (code_repository_info, debug),
    )
    monkeypatch.setattr(cli_mod, "read_local_sdk_version", lambda req: "1.2.3")
    monkeypatch.setattr(cli_mod, "fetch_latest_sdk_version", lambda: "1.2.3")
    monkeypatch.setattr(
        cli_mod,
        "get_code_repository_context",
        lambda *args, **kwargs: types.SimpleNamespace(
            code_repository_uid="code-repository-uid-123",
            repository_branch="main",
            canonical_repository_identity="github.com/org/demo",
            commit_sha="a" * 40,
            code_repository_branch_uid="code-repository-branch-uid-123",
            status="resolved",
            detail="",
        ),
    )

    result = runner.invoke(cli_mod.app, ["code-repository", "current"])
    assert result.exit_code == 0
    assert "Current CodeRepository" in result.output
    assert "main" in result.output
    assert "code-repository-branch-uid-123" in result.output


def test_code_repository_current_json(cli_mod, runner, monkeypatch, tmp_path):
    code_repository_path = tmp_path / "org" / "projects" / "demo-code-repository-uid-123"
    code_repository_path.mkdir(parents=True, exist_ok=True)

    code_repository_info = types.SimpleNamespace(
        path=str(code_repository_path),
        folder="demo-code-repository-uid-123",
        code_repository_uid="code-repository-uid-123",
        code_repository_id="123",
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
        cli_mod,
        "detect_current_code_repository",
        lambda workspaces, base: (code_repository_info, debug),
    )
    monkeypatch.setattr(cli_mod, "read_local_sdk_version", lambda req: "1.2.3")
    monkeypatch.setattr(cli_mod, "fetch_latest_sdk_version", lambda: "1.2.3")
    monkeypatch.setattr(
        cli_mod,
        "get_code_repository_context",
        lambda *args, **kwargs: types.SimpleNamespace(
            code_repository_uid="code-repository-uid-123",
            repository_branch="main",
            canonical_repository_identity="github.com/org/demo",
            commit_sha="a" * 40,
            code_repository_branch_uid="code-repository-branch-uid-123",
            status="resolved",
            detail="",
        ),
    )

    result = runner.invoke(cli_mod.app, ["code-repository", "current", "--json"])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["code_repository"]["code_repository_uid"] == "code-repository-uid-123"
    assert payload["code_repository"]["git_branch"] == "main"
    assert (
        payload["code_repository"]["code_repository_branch_uid"] == "code-repository-branch-uid-123"
    )
    assert payload["code_repository"]["code_repository_branch_status"] == "resolved"
    assert payload["code_repository"]["code_repository_branch_error"] is None
    assert payload["sdk_status"]["status"] == "match"


def test_code_repository_current_debug_reports_authenticated_runtime_git_context(
    cli_mod,
    runner,
    monkeypatch,
    tmp_path,
):
    from mainsequence.code_repository_context import (
        CodeRepositoryContext,
        GitCodeRepositorySourceContext,
    )

    code_repository_path = tmp_path / "org" / "projects" / "demo-code-repository-uid-123"
    code_repository_path.mkdir(parents=True, exist_ok=True)
    code_repository_info = types.SimpleNamespace(
        path=str(code_repository_path),
        folder="demo-code-repository-uid-123",
        code_repository_uid="code-repository-uid-123",
        code_repository_id="123",
        venv_path=None,
        python_version="3.13.7",
    )
    source_context = GitCodeRepositorySourceContext(
        repository_root=code_repository_path,
        canonical_repository_identity="github.com/org/demo",
        repository_branch="development",
        repository_ref="refs/heads/development",
        commit_sha="b" * 40,
    )
    runtime_context = CodeRepositoryContext(
        source_context=source_context,
        code_repository_uid="code-repository-uid-123",
        code_repository_branch_uid="code-repository-branch-uid-123",
        organization_environment_uid="environment-uid-123",
        status="resolved",
        process_id=os.getpid(),
        code_repository_branch=types.SimpleNamespace(uid="code-repository-branch-uid-123"),
        context_source="authenticated_runtime",
    )

    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": str(tmp_path)},
    )
    monkeypatch.setattr(
        cli_mod,
        "detect_current_code_repository",
        lambda workspaces, base: (
            code_repository_info,
            types.SimpleNamespace(reason="detected", checks=[]),
        ),
    )
    monkeypatch.setattr(cli_mod, "get_code_repository_context", lambda **kwargs: runtime_context)
    monkeypatch.setattr(cli_mod, "read_local_sdk_version", lambda req: "7.0.2")
    monkeypatch.setattr(cli_mod, "fetch_latest_sdk_version", lambda: "7.0.2")

    result = runner.invoke(cli_mod.app, ["code-repository", "current", "--debug", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["code_repository"] == {
        "path": str(code_repository_path),
        "folder": "demo-code-repository-uid-123",
        "code_repository_uid": "code-repository-uid-123",
        "git_branch": "development",
        "github_repository_binding": "github.com/org/demo",
        "git_commit": "b" * 40,
        "code_repository_branch_uid": "code-repository-branch-uid-123",
        "code_repository_branch_status": "resolved",
        "code_repository_branch_error": None,
        "venv_path": None,
        "python_version": "3.13.7",
    }


def test_code_repository_sdk_status(cli_mod, runner, monkeypatch, git_checkout):
    target = git_checkout()
    monkeypatch.setattr(cli_mod, "read_local_sdk_version", lambda req: "1.2.3")
    monkeypatch.setattr(cli_mod, "fetch_latest_sdk_version", lambda: "v1.2.3")

    result = runner.invoke(
        cli_mod.app,
        ["code-repository", "sdk-status", "--path", str(target)],
    )
    assert result.exit_code == 0
    assert "SDK Status" in result.output


def test_code_repository_sdk_status_json(cli_mod, runner, monkeypatch, git_checkout):
    target = git_checkout()
    monkeypatch.setattr(cli_mod, "read_local_sdk_version", lambda req: "1.2.3")
    monkeypatch.setattr(cli_mod, "fetch_latest_sdk_version", lambda: "v1.2.3")

    result = runner.invoke(
        cli_mod.app,
        ["code-repository", "sdk-status", "--path", str(target), "--json"],
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["code_repository"] == str(target)
    assert payload["latest_github"] == "v1.2.3"
    assert payload["local_requirements_txt"] == "1.2.3"


def test_code_repository_update_sdk(cli_mod, runner, monkeypatch, git_checkout):
    target = git_checkout("code repository")
    uv_path = target / ".venv" / "bin" / "uv"
    calls = []

    monkeypatch.setattr(cli_mod, "ensure_venv", lambda *_: None)
    monkeypatch.setattr(cli_mod, "ensure_uv_installed", lambda *_: uv_path)
    monkeypatch.setattr(cli_mod, "run_uv", lambda uv, args, cwd: calls.append((uv, args, cwd)))

    result = runner.invoke(
        cli_mod.app,
        ["code-repository", "update-sdk", "--path", str(target)],
    )
    assert result.exit_code == 0, result.output
    assert calls == [
        (uv_path, ["lock", "--upgrade-package", "mainsequence"], target),
        (uv_path, ["sync"], target),
        (
            uv_path,
            [
                "run",
                "--no-sync",
                "mainsequence",
                "code-repository",
                "update-agent-skills",
                "--path",
                str(target),
            ],
            target,
        ),
    ]
    assert "SDK update and SDK skill refresh complete." in result.output


@pytest.mark.parametrize(
    ("failed_step", "failure"),
    [
        ("lock", RuntimeError("resolution failed")),
        ("sync", RuntimeError("installation failed")),
        ("run", RuntimeError("skill copy failed")),
        ("run", OSError("could not start child process")),
    ],
)
def test_code_repository_update_sdk_stops_on_failure(
    cli_mod, runner, monkeypatch, git_checkout, failed_step, failure
):
    target = git_checkout()
    calls = []

    def run_uv(uv, args, cwd):
        calls.append(args[0])
        if args[0] == failed_step:
            raise failure

    monkeypatch.setattr(cli_mod, "ensure_venv", lambda *_: None)
    monkeypatch.setattr(cli_mod, "ensure_uv_installed", lambda *_: target / ".venv" / "bin" / "uv")
    monkeypatch.setattr(cli_mod, "run_uv", run_uv)

    result = runner.invoke(
        cli_mod.app,
        ["code-repository", "update-sdk", "--path", str(target)],
    )

    assert result.exit_code != 0
    steps = ["lock", "sync", "run"]
    assert calls == steps[: steps.index(failed_step) + 1]
    assert "SDK update and SDK skill refresh complete." not in result.output
    if failed_step == "run":
        assert "SDK upgraded, but SDK skill refresh failed" in result.output
        assert str(failure) in result.output
        assert "not been rolled back" in result.output
        assert "Retry from" in result.output


def test_code_repository_update_sdk_dry_run(cli_mod, runner, monkeypatch, git_checkout):
    target = git_checkout()
    monkeypatch.setattr(cli_mod, "ensure_venv", lambda *_: None)
    monkeypatch.setattr(
        cli_mod, "ensure_uv_installed", lambda *_: pytest.fail("dry run must not resolve uv")
    )
    monkeypatch.setattr(
        cli_mod, "run_uv", lambda *args, **kwargs: pytest.fail("dry run must not run uv")
    )

    result = runner.invoke(
        cli_mod.app,
        ["code-repository", "update-sdk", "--path", str(target), "--dry-run"],
    )

    assert result.exit_code == 0, result.output
    assert "update-agent-skills" in result.output
    assert "Dry run: no commands executed." in result.output
    assert "SDK update and SDK skill refresh complete." not in result.output
