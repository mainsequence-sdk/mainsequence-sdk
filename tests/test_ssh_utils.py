from __future__ import annotations

import pathlib

import pytest

from mainsequence.cli import ssh_utils


@pytest.mark.parametrize(
    ("origin", "identity", "uses_ssh", "key_name"),
    [
        (
            "git@github.com:org-a/app.git",
            "github.com/org-a/app",
            True,
            "mainsequence-app-30cab1d6d9237dda",
        ),
        (
            "ssh://git@github.com/org-a/app.git",
            "github.com/org-a/app",
            True,
            "mainsequence-app-30cab1d6d9237dda",
        ),
        (
            "https://github.com/org-a/app.git?token=ignored#fragment",
            "github.com/org-a/app",
            False,
            "mainsequence-app-30cab1d6d9237dda",
        ),
        (
            "git@github.com:org-b/app.git",
            "github.com/org-b/app",
            True,
            "mainsequence-app-8a36e97017a59942",
        ),
    ],
)
def test_repository_ssh_key_identity_v1(origin, identity, uses_ssh, key_name):
    assert ssh_utils.repository_ssh_key_identity(origin) == (identity, uses_ssh)
    assert ssh_utils.repository_ssh_key_name(origin) == key_name


def test_repository_ssh_key_identity_preserves_path_case_and_non_default_port():
    identity, uses_ssh = ssh_utils.repository_ssh_key_identity(
        "ssh://git@Example.COM:2222/Org/App.git"
    )

    assert identity == "example.com:2222/Org/App"
    assert uses_ssh is True


@pytest.mark.parametrize(
    "origin",
    ["", "file:///tmp/project.git", "github.com/org/project", "git@github.com:"],
)
def test_repository_ssh_key_identity_rejects_invalid_origins(origin):
    with pytest.raises(ValueError):
        ssh_utils.repository_ssh_key_identity(origin)


def test_require_ssh_git_origin_rejects_https():
    with pytest.raises(ValueError, match="must use SSH"):
        ssh_utils.require_ssh_git_origin("https://github.com/org/app.git")


def test_ensure_key_for_repo_uses_distinct_namespaced_paths(monkeypatch, tmp_path):
    monkeypatch.setenv("HOME", str(tmp_path))
    generated_paths: list[pathlib.Path] = []

    def fake_run(command, *args, env=None, cwd=None):
        assert command == "ssh-keygen"
        key_path = pathlib.Path(args[args.index("-f") + 1])
        generated_paths.append(key_path)
        key_path.write_text("private", encoding="utf-8")
        pathlib.Path(f"{key_path}.pub").write_text("ssh-ed25519 AAAATEST\n", encoding="utf-8")
        return 0, "", ""

    monkeypatch.setattr(ssh_utils, "run", fake_run)
    unrelated_key = tmp_path / ".ssh" / "app"
    unrelated_key.parent.mkdir(parents=True)
    unrelated_key.write_text("unrelated", encoding="utf-8")

    first_key, first_public_key, _ = ssh_utils.ensure_key_for_repo(
        "git@github.com:org-a/app.git"
    )
    second_key, second_public_key, _ = ssh_utils.ensure_key_for_repo(
        "git@github.com:org-b/app.git"
    )

    assert first_key.name == "mainsequence-app-30cab1d6d9237dda"
    assert second_key.name == "mainsequence-app-8a36e97017a59942"
    assert first_key != second_key
    assert first_public_key == pathlib.Path(f"{first_key}.pub")
    assert second_public_key == pathlib.Path(f"{second_key}.pub")
    assert generated_paths == [first_key, second_key]
    assert unrelated_key.read_text(encoding="utf-8") == "unrelated"


def test_ensure_key_for_repo_rejects_partial_keypair(monkeypatch, tmp_path):
    monkeypatch.setenv("HOME", str(tmp_path))
    key_path, public_key_path = ssh_utils.repository_ssh_key_paths(
        "git@github.com:org/app.git"
    )
    key_path.parent.mkdir(parents=True)
    key_path.write_text("private", encoding="utf-8")

    with pytest.raises(RuntimeError, match=str(public_key_path)):
        ssh_utils.ensure_key_for_repo("git@github.com:org/app.git")
