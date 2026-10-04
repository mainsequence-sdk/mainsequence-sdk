from __future__ import annotations

import pathlib
import subprocess

import pytest

from tests.cli.support import _init_sync_checkout, _record_sync_side_effects, _sync_project


def test_code_repository_sync_runs_only_lock_sync_and_export(
    cli_mod, runner, monkeypatch, tmp_path
):
    project, uv_path = _sync_project(tmp_path)
    recorded = _record_sync_side_effects(monkeypatch, cli_mod)

    result = runner.invoke(cli_mod.app, ["code-repository", "sync", "--path", str(project)])

    assert result.exit_code == 0, result.output
    assert recorded["run"] == [
        ([str(uv_path), "lock"], project),
        ([str(uv_path), "sync"], project),
        (
            [
                str(uv_path),
                "export",
                "--locked",
                "--no-dev",
                "--no-hashes",
                "--format",
                "requirements-txt",
                "-o",
                "requirements.txt",
            ],
            project,
        ),
    ]
    assert not [cmd for cmd, _cwd in recorded["run"] if cmd[0] == "git"]
    assert recorded["popen"] == []
    assert recorded["network"] == []
    assert "Nothing was committed or pushed" in result.output
    assert "commit and push them yourself" in result.output


def test_code_repository_sync_defaults_to_the_current_directory(
    cli_mod, runner, monkeypatch, tmp_path
):
    project, uv_path = _sync_project(tmp_path)
    recorded = _record_sync_side_effects(monkeypatch, cli_mod)
    monkeypatch.chdir(project)

    result = runner.invoke(cli_mod.app, ["code-repository", "sync"])

    assert result.exit_code == 0, result.output
    assert [cmd[1] for cmd, _cwd in recorded["run"]] == ["lock", "sync", "export"]
    assert all(cwd == project for _cmd, cwd in recorded["run"])
    assert recorded["network"] == []


def test_code_repository_sync_creates_no_key_and_changes_no_version(
    cli_mod, runner, monkeypatch, tmp_path
):
    project, _uv_path = _sync_project(tmp_path)
    recorded = _record_sync_side_effects(monkeypatch, cli_mod)
    pyproject_before = (project / "pyproject.toml").read_text(encoding="utf-8")

    def _forbidden(*args, **kwargs):
        raise AssertionError("sync must not touch SSH keys, deploy keys or the backend")

    for name in (
        "ensure_key_for_repo",
        "add_deploy_key",
        "_ensure_code_repository_repository_ssh_access",
        "_resolve_git_code_repository_branch_context",
        "git_origin",
    ):
        monkeypatch.setattr(cli_mod, name, _forbidden)

    result = runner.invoke(cli_mod.app, ["code-repository", "sync", "--path", str(project)])

    assert result.exit_code == 0, result.output
    assert (project / "pyproject.toml").read_text(encoding="utf-8") == pyproject_before
    assert not [cmd for cmd, _cwd in recorded["run"] if "version" in cmd]


@pytest.mark.parametrize(
    "removed_arguments",
    [
        ["Update deps"],
        ["--message", "Update deps"],
        ["-m", "Update deps"],
        ["--dry-run"],
        ["Update deps", "code-repository-uid-123"],
    ],
)
def test_code_repository_sync_accepts_only_path(
    cli_mod, runner, monkeypatch, tmp_path, removed_arguments
):
    project, _uv_path = _sync_project(tmp_path)
    recorded = _record_sync_side_effects(monkeypatch, cli_mod)

    result = runner.invoke(
        cli_mod.app,
        ["code-repository", "sync", "--path", str(project), *removed_arguments],
    )

    assert result.exit_code == 2
    assert recorded["run"] == []


def test_code_repository_sync_requires_pyproject_in_the_project_root(
    cli_mod, runner, monkeypatch, tmp_path
):
    _init_sync_checkout(tmp_path)
    recorded = _record_sync_side_effects(monkeypatch, cli_mod)

    result = runner.invoke(cli_mod.app, ["code-repository", "sync", "--path", str(tmp_path)])

    assert result.exit_code == 1
    assert "pyproject.toml not found" in result.output
    assert recorded["run"] == []


def test_code_repository_sync_reports_a_missing_venv(cli_mod, runner, monkeypatch, tmp_path):
    project = tmp_path / "code-repository"
    _init_sync_checkout(project)
    (project / "pyproject.toml").write_text('[project]\nname = "demo"\n', encoding="utf-8")
    recorded = _record_sync_side_effects(monkeypatch, cli_mod)

    result = runner.invoke(cli_mod.app, ["code-repository", "sync", "--path", str(project)])

    assert result.exit_code == 1
    assert "CodeRepository sync failed" in result.output
    assert ".venv not found" in result.output
    assert recorded["run"] == []


def test_code_repository_sync_stops_at_the_failing_uv_step(cli_mod, runner, monkeypatch, tmp_path):
    project, uv_path = _sync_project(tmp_path)
    recorded = _record_sync_side_effects(monkeypatch, cli_mod)

    def _run(cmd, *args, **kwargs):
        recorded["run"].append((list(cmd), pathlib.Path(kwargs["cwd"])))
        return subprocess.CompletedProcess(cmd, 1 if cmd[1:] == ["sync"] else 0, "", "")

    monkeypatch.setattr(subprocess, "run", _run)

    result = runner.invoke(cli_mod.app, ["code-repository", "sync", "--path", str(project)])

    assert result.exit_code == 1
    assert "CodeRepository sync failed" in result.output
    assert [cmd for cmd, _cwd in recorded["run"]] == [
        [str(uv_path), "lock"],
        [str(uv_path), "sync"],
    ]
