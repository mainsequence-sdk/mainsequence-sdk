from __future__ import annotations

import importlib
import os
import pathlib
import sys
import types

import pytest
from typer.testing import CliRunner


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
def cli_mod():
    return _load_cli_module()


@pytest.fixture()
def runner():
    return CliRunner()


@pytest.fixture(autouse=True)
def _print_cli_terminal(monkeypatch):
    """
    Print the simulated terminal command and CLI output for each CliRunner invocation.
    """
    original_invoke = CliRunner.invoke

    def _invoke(self, app, args=None, **kwargs):
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
        "get_config",
        lambda: {
            "backend_url": "https://main-sequence.app",
            "mainsequence_path": "/tmp/mainsequence",
        },
    )
    result = runner.invoke(cli_mod.app, ["settings"])
    assert result.exit_code == 0
    assert "backend_url" in result.output
    assert "mainsequence_path" in result.output


def test_settings_set_base(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(
        cli_mod.cfg,
        "set_config",
        lambda updates: {"mainsequence_path": updates["mainsequence_path"]},
    )
    result = runner.invoke(cli_mod.app, ["settings", "set-base", "/tmp/ms-base"])
    assert result.exit_code == 0
    assert "Projects base folder set to" in result.output


def test_settings_set_backend(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(
        cli_mod.cfg,
        "set_backend_url",
        lambda url: {"backend_url": url},
    )
    result = runner.invoke(cli_mod.app, ["settings", "set-backend", "https://example.test"])
    assert result.exit_code == 0
    assert "Backend URL set to" in result.output


def test_login_mocked(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(
        cli_mod,
        "api_login",
        lambda email, password: {"username": email, "backend": "https://example.test"},
    )
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": "/tmp/mainsequence"},
    )
    monkeypatch.setattr(cli_mod.cfg, "secure_store_available", lambda: True)
    monkeypatch.setattr(cli_mod, "get_projects", lambda: [])
    monkeypatch.setattr(cli_mod, "_org_slug_from_profile", lambda: "default")

    result = runner.invoke(
        cli_mod.app,
        ["login", "user@example.com", "--password", "secret", "--no-status"],
    )
    assert result.exit_code == 0
    assert "Signed in as user@example.com" in result.output
    assert "Auth tokens are persisted in secure OS storage" in result.output


def test_login_export_env(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(
        cli_mod,
        "api_login",
        lambda email, password: {
            "username": email,
            "backend": "https://example.test",
            "access": "acc-123",
            "refresh": "ref-456",
        },
    )
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": "/tmp/mainsequence"},
    )
    monkeypatch.setattr(cli_mod.cfg, "secure_store_available", lambda: True)
    monkeypatch.setattr(cli_mod, "get_projects", lambda: [])
    monkeypatch.setattr(cli_mod, "_org_slug_from_profile", lambda: "default")

    result = runner.invoke(
        cli_mod.app,
        ["login", "user@example.com", "--password", "secret", "--no-status", "--export"],
    )
    assert result.exit_code == 0
    assert 'export MAIN_SEQUENCE_USER_TOKEN="acc-123"' in result.output
    assert 'export MAIN_SEQUENCE_REFRESH_TOKEN="ref-456"' in result.output
    assert 'export MAIN_SEQUENCE_USERNAME="user@example.com"' in result.output


def test_login_warns_when_secure_persist_fails(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(
        cli_mod,
        "api_login",
        lambda email, password: {
            "username": email,
            "backend": "https://example.test",
            "access": "acc-123",
            "refresh": "ref-456",
            "persisted": False,
        },
    )
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": "/tmp/mainsequence"},
    )
    monkeypatch.setattr(cli_mod.cfg, "secure_store_available", lambda: True)
    monkeypatch.setattr(cli_mod, "get_projects", lambda: [])
    monkeypatch.setattr(cli_mod, "_org_slug_from_profile", lambda: "default")

    result = runner.invoke(
        cli_mod.app,
        ["login", "user@example.com", "--password", "secret", "--no-status"],
    )
    assert result.exit_code == 0
    assert "Could not persist auth tokens in secure OS storage" in result.output


def test_logout(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod.cfg, "clear_tokens", lambda: True)
    result = runner.invoke(cli_mod.app, ["logout"])
    assert result.exit_code == 0
    assert "Signed out" in result.output


def test_logout_export_env(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod.cfg, "clear_tokens", lambda: True)
    result = runner.invoke(cli_mod.app, ["logout", "--export"])
    assert result.exit_code == 0
    assert "unset MAIN_SEQUENCE_USER_TOKEN" in result.output
    assert "unset MAIN_SEQUENCE_REFRESH_TOKEN" in result.output
    assert "unset MAIN_SEQUENCE_USERNAME" in result.output


def test_config_get_tokens_fallback_secure_store(cli_mod, monkeypatch):
    monkeypatch.delenv(cli_mod.cfg.ENV_ACCESS, raising=False)
    monkeypatch.delenv(cli_mod.cfg.ENV_REFRESH, raising=False)
    monkeypatch.delenv(cli_mod.cfg.ENV_USERNAME, raising=False)
    monkeypatch.setattr(
        cli_mod.cfg,
        "_read_secure_tokens",
        lambda: {"username": "u@example.com", "access": "acc", "refresh": "ref"},
    )
    out = cli_mod.cfg.get_tokens()
    assert out["username"] == "u@example.com"
    assert out["access"] == "acc"
    assert out["refresh"] == "ref"


def test_config_save_tokens_writes_secure_store(cli_mod, monkeypatch):
    captured = {}

    def _write_secure_tokens(*, username, access, refresh):
        captured["username"] = username
        captured["access"] = access
        captured["refresh"] = refresh
        return True

    monkeypatch.setattr(cli_mod.cfg, "_write_secure_tokens", _write_secure_tokens)
    cli_mod.cfg.save_tokens("u@example.com", "acc", "ref")
    assert captured == {"username": "u@example.com", "access": "acc", "refresh": "ref"}


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
                "id": 1,
                "project_name": "Demo",
                "data_source": {"related_resource": {"display_name": "DS"}},
            }
        ],
    )
    result = runner.invoke(cli_mod.app, ["project", "list"])
    assert result.exit_code == 0
    assert "Demo" in result.output


def test_project_list_requires_shell_auth_hint(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "get_current_user_profile", lambda: {})
    result = runner.invoke(cli_mod.app, ["project", "list"])
    assert result.exit_code == 1
    assert "Not logged in. Run: mainsequence login <email>" in result.output


def test_project_create_interactive_defaults(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_dynamic_table_data_sources",
        lambda status="AVAILABLE": [
            {
                "id": 11,
                "related_resource": {
                    "display_name": "Default DS",
                    "class_type": "timescale_db",
                    "status": "AVAILABLE",
                },
                "related_resource_class_type": "timescale_db",
            }
        ],
    )
    monkeypatch.setattr(
        cli_mod,
        "list_project_base_images",
        lambda: [{"id": 22, "title": "Python 3.12", "description": "Default image"}],
    )
    monkeypatch.setattr(
        cli_mod,
        "list_github_organizations",
        lambda: [{"id": 33, "login": "main-sequence", "display_name": "Main Sequence"}],
    )

    captured = {}

    def _create_project(**kwargs):
        captured.update(kwargs)
        return {"id": 321, "project_name": kwargs["project_name"], "git_ssh_url": "git@github.com:org/repo.git"}

    monkeypatch.setattr(cli_mod, "create_project", _create_project)

    # Prompts:
    # 1) Project name
    # 2) Data source id (default=11)
    # 3) Default base image id (default=22)
    # 4) GitHub organization id (default=33)
    # 5) Repository branch (default=main)
    # 6) Environment variables line
    user_input = "demo-project\n\n\n\n\nFOO=bar, BAZ=qux\n"
    result = runner.invoke(cli_mod.app, ["project", "create"], input=user_input)

    assert result.exit_code == 0
    assert captured["project_name"] == "demo-project"
    assert captured["data_source_id"] == 11
    assert captured["default_base_image_id"] == 22
    assert captured["github_org_id"] == 33
    assert captured["repository_branch"] == "main"
    assert captured["env_vars"] == {"FOO": "bar", "BAZ": "qux"}
    assert "Project created: demo-project (id=321)" in result.output


def test_project_create_polls_until_initialized(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "create_project",
        lambda **kwargs: {
            "id": 777,
            "project_name": kwargs["project_name"],
            "git_ssh_url": "git@github.com:org/repo.git",
            "is_initialized": False,
        },
    )

    polled = [
        {
            "id": 777,
            "project_name": "demo-project",
            "git_ssh_url": "git@github.com:org/repo.git",
            "is_initialized": False,
        },
        {
            "id": 777,
            "project_name": "demo-project",
            "git_ssh_url": "git@github.com:org/repo.git",
            "is_initialized": True,
        },
    ]
    monkeypatch.setattr(cli_mod, "get_project", lambda project_id: polled.pop(0))

    sleep_calls = []
    monkeypatch.setattr(cli_mod.time, "sleep", lambda secs: sleep_calls.append(secs))

    result = runner.invoke(
        cli_mod.app,
        [
            "project",
            "create",
            "demo-project",
            "--data-source-id",
            "11",
            "--default-base-image-id",
            "22",
            "--github-org-id",
            "33",
            "--branch",
            "main",
            "--env",
            "FOO=bar",
        ],
    )

    assert result.exit_code == 0
    assert sleep_calls == [30, 30]
    assert "Project is still initializing." in result.output
    assert "Project is initialized and ready." in result.output


def test_project_delete_remote_yes(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "get_projects",
        lambda: [{"id": 321, "project_name": "Demo Project"}],
    )

    captured = {}

    def _delete_project(project_id, delete_repositories=False):
        captured["project_id"] = project_id
        captured["delete_repositories"] = delete_repositories
        return {"detail": "deleted"}

    monkeypatch.setattr(cli_mod, "delete_project", _delete_project)

    result = runner.invoke(
        cli_mod.app,
        ["project", "delete", "321", "--yes", "--delete-repositories"],
    )
    assert result.exit_code == 0
    assert captured["project_id"] == 321
    assert captured["delete_repositories"] is True
    assert "Project deleted: Demo Project (id=321)" in result.output


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
        "get_projects",
        lambda: [{"id": 123, "project_name": "Demo", "git_ssh_url": "git@github.com:org/repo.git"}],
    )
    monkeypatch.setattr(cli_mod, "ensure_key_for_repo", lambda repo: (key, pub, "ssh-ed25519 AAA test"))
    monkeypatch.setattr(cli_mod, "_copy_clipboard", lambda txt: True)
    monkeypatch.setattr(cli_mod, "add_deploy_key", lambda *a, **k: None)
    monkeypatch.setattr(cli_mod, "start_agent_and_add_key", lambda *_: {})

    def _clone(cmd, env=None, cwd=None):
        assert cmd[0:2] == ["git", "clone"]
        pathlib.Path(cmd[-1]).mkdir(parents=True, exist_ok=True)
        return 0

    monkeypatch.setattr(cli_mod.subprocess, "call", _clone)
    monkeypatch.setattr(
        cli_mod,
        "fetch_project_env_text",
        lambda project_id: "DEFAULT_BASE_IMAGE=none\nFOO=bar\n",
    )
    monkeypatch.setattr(cli_mod, "get_project_token", lambda project_id: "token-123")
    monkeypatch.setattr(cli_mod, "resolve_base_image", lambda _: ("ghcr.io/test/image:latest", []))
    monkeypatch.setattr(
        cli_mod,
        "ensure_docker_scaffold",
        lambda *_: (True, ["Created Dockerfile (base image: ghcr.io/test/image:latest)"]),
    )

    result = runner.invoke(cli_mod.app, ["project", "set-up-locally", "123"])
    assert result.exit_code == 0

    env_file = base / "org" / "projects" / "demo-123" / ".env"
    assert env_file.exists()
    env_text = env_file.read_text(encoding="utf-8")
    assert "MAINSEQUENCE_TOKEN=token-123" in env_text
    assert "TDAG_ENDPOINT=https://backend.test" in env_text
    assert "INGORE_MS_AGENT=true" in env_text


def test_project_open(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "project"
    target.mkdir(parents=True, exist_ok=True)
    opened = {"path": None}

    monkeypatch.setattr(cli_mod, "open_folder", lambda p: opened.update(path=p))
    result = runner.invoke(cli_mod.app, ["project", "open", "--path", str(target)])
    assert result.exit_code == 0
    assert opened["path"] == str(target.resolve())


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
    monkeypatch.setattr(cli_mod, "ensure_key_for_repo", lambda repo: (key, key.with_suffix(".pub"), "pub"))
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
        '[project]\nname = "demo"\nrequires-python = ">=3.11,<3.13"\n',
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
    assert calls[0][0] == ["uv", "venv", ".venv", "--python", "3.11"]
    assert calls[0][1] == str(target.resolve())
    assert calls[1][0] == ["uv", "sync"]
    assert calls[1][2]["UV_PROJECT_ENVIRONMENT"] == ".venv"
    assert "Local .venv built with Python 3.11." in result.output


def test_project_build_local_venv_skips_when_exists(cli_mod, runner, tmp_path):
    target = tmp_path / "project"
    target.mkdir(parents=True, exist_ok=True)
    (target / ".venv").mkdir(parents=True, exist_ok=True)

    result = runner.invoke(
        cli_mod.app,
        ["project", "build_local_venv", "--path", str(target)],
    )
    assert result.exit_code == 0
    assert "already exists" in result.output


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


def test_project_sync(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "project"
    target.mkdir(parents=True, exist_ok=True)
    key = tmp_path / "id_ed25519"
    uv_path = target / ".venv" / "bin" / "uv"
    uv_calls = []
    git_calls = []

    monkeypatch.setattr(cli_mod, "ensure_venv", lambda *_: None)
    monkeypatch.setattr(cli_mod, "git_origin", lambda *_: "git@github.com:org/repo.git")
    monkeypatch.setattr(cli_mod, "ensure_key_for_repo", lambda *_: (key, key.with_suffix(".pub"), "pub"))
    monkeypatch.setattr(cli_mod, "ensure_uv_installed", lambda *_: uv_path)
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
        ["project", "sync", "--message", "Update deps", "--path", str(target), "--no-push"],
    )
    assert result.exit_code == 0
    assert ["version", "--bump", "patch"] in uv_calls
    assert ["lock"] in uv_calls
    assert ["sync"] in uv_calls
    assert ["git", "add", "-A"] in git_calls
    assert ["git", "commit", "-m", "Update deps"] in git_calls
    assert ["git", "push"] not in git_calls


def test_project_sync_project(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "project"
    target.mkdir(parents=True, exist_ok=True)
    key = tmp_path / "id_ed25519"
    uv_path = target / ".venv" / "bin" / "uv"
    uv_calls = []
    export_calls = []
    git_calls = []

    monkeypatch.setattr(cli_mod, "ensure_venv", lambda *_: None)
    monkeypatch.setattr(cli_mod, "git_origin", lambda *_: "git@github.com:org/repo.git")
    monkeypatch.setattr(cli_mod, "ensure_key_for_repo", lambda *_: (key, key.with_suffix(".pub"), "pub"))
    monkeypatch.setattr(cli_mod, "ensure_uv_installed", lambda *_: uv_path)
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
        ["git", "push"],
    ]


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
    project_path = tmp_path / "org" / "projects" / "demo-123"
    project_path.mkdir(parents=True, exist_ok=True)

    project_info = types.SimpleNamespace(
        path=str(project_path),
        folder="demo-123",
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
    monkeypatch.setattr(cli_mod, "detect_current_project", lambda workspaces, base: (project_info, debug))
    monkeypatch.setattr(cli_mod, "read_local_sdk_version", lambda req: "1.2.3")
    monkeypatch.setattr(cli_mod, "fetch_latest_sdk_version", lambda: "1.2.3")

    result = runner.invoke(cli_mod.app, ["project", "current"])
    assert result.exit_code == 0
    assert "Current Project" in result.output


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


def test_login_live_with_env_credentials(cli_mod, runner, monkeypatch):
    """
    Optional live login check.

    Set:
      - MAINSEQUENCE_TEST_USERNAME
      - MAINSEQUENCE_TEST_PASSWORD
    """
    username = os.getenv("MAINSEQUENCE_TEST_USERNAME")
    password = os.getenv("MAINSEQUENCE_TEST_PASSWORD")
    if not username or not password:
        pytest.skip("Missing MAINSEQUENCE_TEST_USERNAME / MAINSEQUENCE_TEST_PASSWORD")

    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": "/tmp/mainsequence"},
    )
    result = runner.invoke(
        cli_mod.app,
        ["login", username, "--password", password, "--no-status"],
    )
    assert result.exit_code == 0
