from __future__ import annotations

import pathlib
import types

import pytest
from typer.testing import CliRunner

from tests.cli.support import _REAL_SUBPROCESS_RUN, _load_cli_module


@pytest.fixture()
def cli_mod(monkeypatch):
    module = _load_cli_module()
    monkeypatch.setattr(
        module,
        "get_code_repository_context",
        lambda *args, **kwargs: types.SimpleNamespace(
            code_repository_uid="code-repository-uid-123",
            repository_branch="main",
            canonical_repository_identity=("github.com/mainsequence-sdk/cli-test-repository"),
            commit_sha="a" * 40,
            code_repository_branch_uid="code-repository-branch-uid-123",
            organization_environment_uid="environment-uid-123",
            status="resolved",
            detail="",
        ),
    )
    return module


@pytest.fixture()
def runner():
    return CliRunner()


@pytest.fixture(autouse=True)
def _print_cli_terminal(monkeypatch):
    """
    Print the simulated terminal command and CLI output for each CliRunner invocation.
    """
    original_invoke = CliRunner.invoke

    def _ensure_test_git_checkout(args) -> None:
        values = [str(value) for value in (args or [])]
        if not values or values[0] != "code-repository":
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
                "git@github.com:mainsequence-sdk/cli-test-repository.git",
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
