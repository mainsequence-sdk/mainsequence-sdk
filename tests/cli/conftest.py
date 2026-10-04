from __future__ import annotations

import pathlib
import subprocess
import types

import pytest
from typer.testing import CliRunner

from tests.cli.support import _load_cli_module


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


@pytest.fixture
def git_checkout(tmp_path):
    """Factory for explicit checkouts confined to this test's temporary directory."""

    def create(relative_path: str = "code-repository") -> pathlib.Path:
        path = (tmp_path / relative_path).resolve()
        if not path.is_relative_to(tmp_path.resolve()):
            raise ValueError("Test checkouts must stay inside tmp_path")
        path.mkdir(parents=True)
        commands = (
            ["git", "init", "-q", "-b", "main"],
            ["git", "config", "user.email", "cli-tests@example.test"],
            ["git", "config", "user.name", "CLI Tests"],
            [
                "git",
                "-c",
                "commit.gpgsign=false",
                "commit",
                "-q",
                "--allow-empty",
                "-m",
                "Test checkout",
            ],
            [
                "git",
                "remote",
                "add",
                "origin",
                "git@github.com:mainsequence-sdk/cli-test-repository.git",
            ],
        )
        for command in commands:
            subprocess.run(command, cwd=path, check=True)
        return path

    return create
