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
    monkeypatch.setattr(cli_mod, "get_projects", lambda: [])
    monkeypatch.setattr(cli_mod, "_org_slug_from_profile", lambda: "default")

    result = runner.invoke(
        cli_mod.app,
        ["login", "user@example.com", "--password", "secret", "--no-status"],
    )
    assert result.exit_code == 0
    assert "Signed in as user@example.com" in result.output


def test_logout(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod.cfg, "clear_tokens", lambda: True)
    result = runner.invoke(cli_mod.app, ["logout"])
    assert result.exit_code == 0
    assert "Signed out" in result.output


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
