from __future__ import annotations

from types import SimpleNamespace

from typer.testing import CliRunner

from mainsequence.cli import code_repository as code_repository_mod
from mainsequence.cli.cli import app

runner = CliRunner()


def _git_checkout(tmp_path):
    root = tmp_path / "checkout"
    root.mkdir()
    (root / ".git").mkdir()
    return root


def test_code_repository_group_exposes_supported_development_commands():
    result = runner.invoke(app, ["code-repository", "--help"])

    assert result.exit_code == 0
    for command in (
        "set-up-locally",
        "refresh-token",
        "sync",
        "update-sdk",
        "update",
        "update-agent-skills",
        "freeze-env",
        "build-local-venv",
        "open-signed-terminal",
    ):
        assert command in result.output


def test_refresh_token_preserves_unmanaged_env_entries(monkeypatch, tmp_path):
    root = _git_checkout(tmp_path)
    env_file = root / ".env"
    env_file.write_text(
        "KEEP_ME=yes\n"
        "MAINSEQUENCE_ACCESS_TOKEN=old\n"
        "MAINSEQUENCE_REFRESH_TOKEN=old-refresh\n"
        "MAINSEQUENCE_REPOSITORY_BRANCH=caller-selected\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(code_repository_mod, "_require_login", lambda: {"username": "user"})
    monkeypatch.setattr(
        code_repository_mod.cfg,
        "get_tokens",
        lambda: {"access": "new", "refresh": "new-refresh"},
    )
    monkeypatch.setattr(code_repository_mod.cfg, "backend_url", lambda: "https://backend.example")

    result = runner.invoke(
        app,
        ["code-repository", "refresh-token", "--path", str(root)],
    )

    assert result.exit_code == 0
    rendered = env_file.read_text(encoding="utf-8")
    assert "KEEP_ME=yes" in rendered
    assert "MAINSEQUENCE_ACCESS_TOKEN=new" in rendered
    assert "MAINSEQUENCE_REFRESH_TOKEN=new-refresh" in rendered
    assert "MAINSEQUENCE_ENDPOINT=https://backend.example" in rendered
    assert "caller-selected" not in rendered


def test_update_agents_md_replaces_only_managed_block(tmp_path):
    root = _git_checkout(tmp_path)
    destination = root / "AGENTS.md"
    destination.write_text(
        "# Local rules\n\n"
        "<!-- mainsequence-agent-scaffold:start schema=1 source=agent_scaffold -->\n"
        "stale\n"
        "<!-- mainsequence-agent-scaffold:end -->\n\n"
        "Keep this local footer.\n",
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        ["code-repository", "update", "AGENTS.md", "--path", str(root)],
    )

    assert result.exit_code == 0
    rendered = destination.read_text(encoding="utf-8")
    assert rendered.startswith("# Local rules")
    assert rendered.endswith("Keep this local footer.\n")
    assert "stale" not in rendered
    assert "## Main Sequence Instructions" in rendered


def test_update_sdk_dry_run_does_not_execute_uv(monkeypatch, tmp_path):
    root = _git_checkout(tmp_path)
    monkeypatch.setattr(code_repository_mod, "ensure_venv", lambda _root: object())
    monkeypatch.setattr(
        code_repository_mod,
        "ensure_uv_installed",
        lambda _root: (_ for _ in ()).throw(AssertionError("dry run executed uv setup")),
    )

    result = runner.invoke(
        app,
        ["code-repository", "update-sdk", "--path", str(root), "--dry-run"],
    )

    assert result.exit_code == 0
    assert "Dry run: no commands executed." in result.output


def test_sync_dry_run_does_not_create_or_register_ssh_key(monkeypatch, tmp_path):
    root = _git_checkout(tmp_path)
    monkeypatch.setattr(
        code_repository_mod,
        "_resolve_git_code_repository_branch_context",
        lambda *_args, **_kwargs: ("main", "branch-uid"),
    )
    monkeypatch.setattr(
        code_repository_mod,
        "get_code_repository_context",
        lambda *_args, **_kwargs: SimpleNamespace(code_repository_uid="repository-uid"),
    )
    monkeypatch.setattr(code_repository_mod, "git_origin", lambda _root: "git@example/repo.git")
    monkeypatch.setattr(code_repository_mod, "require_ssh_git_origin", lambda _origin: None)
    monkeypatch.setattr(code_repository_mod, "ensure_venv", lambda _root: object())
    monkeypatch.setattr(code_repository_mod, "ensure_uv_installed", lambda _root: tmp_path / "uv")
    monkeypatch.setattr(
        code_repository_mod, "uv_project_version", lambda *_args, **_kwargs: "1.0.0"
    )
    monkeypatch.setattr(
        code_repository_mod, "uv_preview_patch_version", lambda *_args, **_kwargs: "1.0.1"
    )
    monkeypatch.setattr(
        code_repository_mod,
        "render_code_repository_branch_default_redeployment_tag",
        lambda *_args, **_kwargs: "v1.0.1-main",
    )
    monkeypatch.setattr(code_repository_mod, "verify_git_tag_absent", lambda *_args: None)
    monkeypatch.setattr(
        code_repository_mod,
        "_ensure_code_repository_repository_ssh_access",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("dry run created an SSH key")),
    )

    result = runner.invoke(
        app,
        [
            "code-repository",
            "sync",
            "Update dependencies",
            "--path",
            str(root),
            "--dry-run",
        ],
    )

    assert result.exit_code == 0
    assert "read-only preflight complete" in result.output


def test_freeze_env_exports_only_locked_runtime_dependencies(monkeypatch, tmp_path):
    root = _git_checkout(tmp_path)
    calls = []
    monkeypatch.setattr(code_repository_mod, "ensure_venv", lambda _root: object())
    monkeypatch.setattr(code_repository_mod, "ensure_uv_installed", lambda _root: tmp_path / "uv")
    monkeypatch.setattr(
        code_repository_mod,
        "uv_export_requirements",
        lambda uv, **kwargs: calls.append((uv, kwargs)),
    )

    result = runner.invoke(app, ["code-repository", "freeze-env", "--path", str(root)])

    assert result.exit_code == 0
    assert calls == [
        (
            tmp_path / "uv",
            {
                "cwd": root,
                "locked": True,
                "no_dev": True,
                "no_hashes": True,
                "output_file": "requirements.txt",
            },
        )
    ]
