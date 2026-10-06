from __future__ import annotations

import importlib
import pathlib
import types

import pytest


def test_add_deploy_key_uses_code_repository_route(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    class _Response:
        @staticmethod
        def raise_for_status():
            return None

    def _authed(method, path, body=None):
        captured.update(method=method, path=path, body=body)
        return _Response()

    monkeypatch.setattr(
        api_mod,
        "resolve_code_repository_uid",
        lambda code_repository_ref: "code-repository-uid-123",
    )
    monkeypatch.setattr(api_mod, "authed", _authed)

    api_mod.add_deploy_key("code-repository-uid-123", "workstation", "ssh-ed25519 AAA test")

    assert captured == {
        "method": "POST",
        "path": "/api/v1/code-repositories/code-repository-uid-123/add-deploy-key/",
        "body": {"key_title": "workstation", "public_key": "ssh-ed25519 AAA test"},
    }


def test_cli_api_requests_no_default_redeployment_tag(cli_mod):
    """Release tags come from the repository's own CI, never from the platform."""
    api_mod = importlib.import_module("mainsequence.cli.api")
    source = pathlib.Path(api_mod.__file__).read_text(encoding="utf-8")

    assert not hasattr(api_mod, "render_code_repository_branch_default_redeployment_tag")
    assert "default-redeployment-tag" not in source


def test_org_slug_from_profile_handles_organization_object(cli_mod, monkeypatch):
    monkeypatch.setattr(
        cli_mod,
        "get_current_user_profile",
        lambda: {"organization": {"uid": "org-uid-456", "name": "Main Sequence Dev"}},
    )

    out = cli_mod._org_slug_from_profile()
    assert out == "main-sequence-dev"


def test_resolve_code_repository_repository_ssh_url_uses_canonical_repository(
    cli_mod,
    monkeypatch,
):
    repository_uid = "2bcf47e3-3a79-4f1e-a428-176c0218a8d1"
    captured = {}

    def _get_code_repository_repository(uid):
        captured["uid"] = uid
        return {
            "uid": uid,
            "git_ssh_url": "git@github.com:mainsequence-projects/tutorial.git",
            "git_repo_url": "https://github.com/mainsequence-projects/tutorial.git",
        }

    monkeypatch.setattr(cli_mod, "get_code_repository_repository", _get_code_repository_repository)

    result = cli_mod._resolve_code_repository_repository_ssh_url(
        {"github_repository_binding_uid": repository_uid}
    )

    assert captured["uid"] == repository_uid
    assert result == "git@github.com:mainsequence-projects/tutorial.git"


def test_resolve_code_repository_repository_ssh_url_requires_linked_repository(
    cli_mod, monkeypatch
):
    monkeypatch.setattr(
        cli_mod,
        "get_code_repository_repository",
        lambda uid: (_ for _ in ()).throw(AssertionError("must not fetch")),
    )

    with pytest.raises(cli_mod.ApiError, match="no linked GitHubRepositoryBinding"):
        cli_mod._resolve_code_repository_repository_ssh_url({})


def test_resolve_code_repository_repository_ssh_url_requires_ssh_url(cli_mod, monkeypatch):
    repository_uid = "2bcf47e3-3a79-4f1e-a428-176c0218a8d1"
    monkeypatch.setattr(
        cli_mod,
        "get_code_repository_repository",
        lambda uid: {
            "uid": uid,
            "git_repo_url": "https://github.com/mainsequence-projects/tutorial.git",
        },
    )

    with pytest.raises(cli_mod.ApiError, match="has no SSH clone URL"):
        cli_mod._resolve_code_repository_repository_ssh_url(
            {"github_repository_binding_uid": repository_uid}
        )


def test_ensure_code_repository_repository_ssh_access_registers_new_key_before_verification(
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
        lambda code_repository_ref, title, public_key: events.append(
            ("register", code_repository_ref, title, public_key)
        ),
    )

    key_path, public_key, env = cli_mod._ensure_code_repository_repository_ssh_access(
        origin=origin,
        code_repository_ref="code-repository-uid-123",
        verify_access=lambda ssh_env: events.append(("verify", ssh_env["GIT_SSH_COMMAND"])),
    )

    assert key_path == key
    assert public_key == "ssh-ed25519 AAA test"
    assert env["GIT_SSH_COMMAND"] == f'ssh -i "{key}" -o IdentitiesOnly=yes'
    assert events == [
        ("register", "code-repository-uid-123", "developer-laptop", "ssh-ed25519 AAA test"),
        ("verify", env["GIT_SSH_COMMAND"]),
    ]


def test_ensure_code_repository_repository_ssh_access_registers_inaccessible_existing_key(
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
        lambda code_repository_ref, title, public_key: events.append("register"),
    )

    def verify_access(env):
        events.append("verify")
        if events == ["verify"]:
            raise RuntimeError("not registered")

    cli_mod._ensure_code_repository_repository_ssh_access(
        origin=origin,
        code_repository_ref="code-repository-uid-123",
        verify_access=verify_access,
    )

    assert events == ["verify", "register", "verify"]


def test_code_repository_set_up_locally(cli_mod, runner, monkeypatch, tmp_path):
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
        "resolve_code_repository",
        lambda code_repository_ref: {
            "uid": "code-repository-uid-123",
            "code_repository_name": "Demo",
            "github_repository_binding_uid": "repository-uid-123",
            "archived": False,
            "created_by": "u",
            "labels": [],
            "branches": [{"uid": "code-repository-branch-uid-123", "repository_branch": "main"}],
        },
    )
    monkeypatch.setattr(
        cli_mod,
        "get_code_repository_branch",
        lambda branch_uid: {
            "uid": branch_uid,
            "code_repository_uid": "code-repository-uid-123",
            "code_repository_name": "Demo",
            "repository_branch": "main",
            "is_initialized": True,
        },
    )
    repository_requests = []
    monkeypatch.setattr(
        cli_mod,
        "get_code_repository_repository",
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
        ["code-repository", "set-up-locally", "code-repository-uid-123", "--branch", "main"],
    )
    assert result.exit_code == 0
    assert repository_requests == ["repository-uid-123"]
    assert len(deploy_key_requests) == 1
    assert deploy_key_requests[0][0][0] == "code-repository-uid-123"
    assert deploy_key_requests[0][0][2] == "ssh-ed25519 AAA test"
    assert deploy_key_requests[0][1] == {}

    env_file = base / "org" / "code-repositories" / "demo-code-repository-uid-123" / ".env"
    assert env_file.exists()
    # The checkout gets the endpoint and no credential, even though a session exists.
    assert env_file.read_text(encoding="utf-8") == "MAINSEQUENCE_ENDPOINT=https://backend.test\n"


def test_code_repository_set_up_locally_runtime_credential(cli_mod, runner, monkeypatch, tmp_path):
    base = tmp_path / "base"
    base.mkdir(parents=True, exist_ok=True)
    key = tmp_path / "id_ed25519"
    pub = tmp_path / "id_ed25519.pub"

    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "runtime_credential")
    monkeypatch.setenv("MAINSEQUENCE_RUNTIME_CREDENTIAL_ID", "cred-id")
    monkeypatch.setenv("MAINSEQUENCE_RUNTIME_IDENTITY_TOKEN_FILE", str(tmp_path / "token"))
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
        "resolve_code_repository",
        lambda code_repository_ref: {
            "uid": "code-repository-uid-123",
            "code_repository_name": "Demo",
            "github_repository_binding_uid": "repository-uid-123",
            "archived": False,
            "created_by": "u",
            "labels": [],
            "branches": [{"uid": "code-repository-branch-uid-123", "repository_branch": "main"}],
        },
    )
    monkeypatch.setattr(
        cli_mod,
        "get_code_repository_branch",
        lambda branch_uid: {
            "uid": branch_uid,
            "code_repository_uid": "code-repository-uid-123",
            "code_repository_name": "Demo",
            "repository_branch": "main",
            "is_initialized": True,
        },
    )
    repository_requests = []
    monkeypatch.setattr(
        cli_mod,
        "get_code_repository_repository",
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
        lambda backend_url: (_ for _ in ()).throw(
            AssertionError("The runtime credential should not be exchanged to write .env")
        ),
    )

    result = runner.invoke(
        cli_mod.app,
        ["code-repository", "set-up-locally", "code-repository-uid-123", "--branch", "main"],
    )
    assert result.exit_code == 0
    assert repository_requests == ["repository-uid-123"]
    assert len(deploy_key_requests) == 1
    assert deploy_key_requests[0][0][0] == "code-repository-uid-123"
    assert deploy_key_requests[0][0][2] == "ssh-ed25519 AAA test"
    assert deploy_key_requests[0][1] == {}

    # A runtime receives its credential in its own environment. None of it, and
    # no exchanged token, is copied into the checkout.
    env_file = base / "org" / "code-repositories" / "demo-code-repository-uid-123" / ".env"
    assert env_file.read_text(encoding="utf-8") == "MAINSEQUENCE_ENDPOINT=https://backend.test\n"


def test_code_repository_set_up_locally_rejects_uninitialized_code_repository(
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
        "resolve_code_repository",
        lambda code_repository_ref: {
            "uid": "code-repository-uid-123",
            "code_repository_name": "Demo",
            "github_repository_binding_uid": "repository-uid-123",
            "archived": False,
            "created_by": "u",
            "labels": [],
            "branches": [{"uid": "code-repository-branch-uid-123", "repository_branch": "main"}],
        },
    )
    monkeypatch.setattr(
        cli_mod,
        "get_code_repository_branch",
        lambda branch_uid: {
            "uid": branch_uid,
            "code_repository_uid": "code-repository-uid-123",
            "code_repository_name": "Demo",
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
        ["code-repository", "set-up-locally", "code-repository-uid-123", "--branch", "main"],
    )

    assert result.exit_code == 1
    assert "CodeRepository has not finished initializing yet." in result.output
    assert clone_calls["count"] == 0


def test_code_repository_open(cli_mod, runner, monkeypatch, git_checkout):
    target = git_checkout()
    opened = {"path": None}

    monkeypatch.setattr(cli_mod, "open_folder", lambda p: opened.update(path=p))
    result = runner.invoke(cli_mod.app, ["code-repository", "open", "--path", str(target)])
    assert result.exit_code == 0
    assert opened["path"] == str(target.resolve())


def test_code_repository_open_rejects_non_checkout_without_creating_git_metadata(
    cli_mod, runner, monkeypatch, tmp_path
):
    target = tmp_path / "not-a-checkout"
    target.mkdir()
    monkeypatch.setattr(
        cli_mod, "open_folder", lambda *_: pytest.fail("A non-checkout must not be opened")
    )

    result = runner.invoke(cli_mod.app, ["code-repository", "open", "--path", str(target)])

    assert result.exit_code == 1
    assert "Git CodeRepository checkout" in result.output
    assert not (target / ".git").exists()


def test_code_repository_delete_local(cli_mod, runner, monkeypatch, tmp_path, git_checkout):
    base = tmp_path / "base"
    code_repository_path = git_checkout("base/org/code-repositories/demo-123")
    (code_repository_path / "x.txt").write_text("x", encoding="utf-8")

    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": str(base)},
    )
    monkeypatch.setattr(cli_mod, "get_current_user_profile", lambda: {"organization": "Org"})
    monkeypatch.setattr(cli_mod, "_org_slug_from_profile", lambda: "org")

    result = runner.invoke(
        cli_mod.app,
        ["code-repository", "delete-local", "--path", str(code_repository_path), "--yes"],
    )
    assert result.exit_code == 0
    assert not code_repository_path.exists()


def test_code_repository_open_signed_terminal(cli_mod, runner, monkeypatch, tmp_path, git_checkout):
    target = git_checkout()
    key = tmp_path / "id_ed25519"
    called = {"args": None}

    monkeypatch.setattr(cli_mod, "git_origin", lambda _: "git@github.com:org/repo.git")
    monkeypatch.setattr(
        cli_mod,
        "_ensure_code_repository_repository_ssh_access",
        lambda **kwargs: (key, "pub", {"GIT_SSH_COMMAND": "forced"}),
    )
    monkeypatch.setattr(
        cli_mod,
        "open_signed_terminal",
        lambda code_repository_dir, key_path, repo_name: called.update(
            args=(code_repository_dir, str(key_path), repo_name)
        ),
    )

    result = runner.invoke(
        cli_mod.app,
        ["code-repository", "open-signed-terminal", "--path", str(target)],
    )
    assert result.exit_code == 0
    assert called["args"] is not None


def test_code_repository_build_local_venv(cli_mod, runner, monkeypatch, git_checkout):
    target = git_checkout()
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
        ["code-repository", "build-local-venv", "--path", str(target)],
    )
    assert result.exit_code == 0
    assert calls[0][0] == ["uv", "venv", ".venv", "--python", ">=3.13,<3.14"]
    assert calls[0][1] == str(target.resolve())
    assert calls[1][0] == ["uv", "sync"]
    assert calls[1][2]["UV_PROJECT_ENVIRONMENT"] == ".venv"
    assert "Local .venv built for Python requirement >=3.13,<3.14." in result.output


def test_code_repository_build_local_venv_defaults_to_cwd_with_env_code_repository_id(
    cli_mod, runner, monkeypatch, git_checkout
):
    target = git_checkout("demo-code-repository-uid-123")
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

    result = runner.invoke(cli_mod.app, ["code-repository", "build-local-venv"])
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


def test_code_repository_build_local_venv_skips_compatible_existing_environment(
    cli_mod, runner, monkeypatch, git_checkout
):
    target = git_checkout()
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
        ["code-repository", "build-local-venv", "--path", str(target)],
    )
    assert result.exit_code == 0
    assert "already uses compatible Python 3.13.7" in result.output


def test_code_repository_build_local_venv_rejects_incompatible_existing_environment(
    cli_mod, runner, monkeypatch, git_checkout
):
    target = git_checkout()
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
        ["code-repository", "build-local-venv", "--path", str(target)],
    )

    assert result.exit_code == 1
    assert "Python 3.12.8, which does not satisfy >=3.13" in result.output
    assert "Re-run with --recreate" in result.output


def test_code_repository_build_local_venv_recreates_incompatible_environment(
    cli_mod, runner, monkeypatch, git_checkout
):
    target = git_checkout()
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
        ["code-repository", "build-local-venv", "--path", str(target), "--recreate"],
    )

    assert result.exit_code == 0
    assert not (venv_path / "old-environment").exists()
    assert calls[0][0] == ["uv", "venv", ".venv", "--python", ">=3.13"]
    assert "Replacing existing" in result.output


def test_code_repository_build_local_venv_preserves_existing_environment_when_uv_is_unavailable(
    cli_mod, runner, monkeypatch, git_checkout
):
    target = git_checkout()
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
        ["code-repository", "build-local-venv", "--path", str(target), "--recreate"],
    )

    assert result.exit_code == 1
    assert marker.read_text(encoding="utf-8") == "existing"
    assert "automatic install failed: offline" in result.output


def test_code_repository_build_local_venv_requires_pyproject(cli_mod, runner, git_checkout):
    target = git_checkout()

    result = runner.invoke(
        cli_mod.app,
        ["code-repository", "build-local-venv", "--path", str(target)],
    )
    assert result.exit_code == 1
    assert "pyproject.toml not found in the CodeRepository root." in result.output


def test_code_repository_build_docker_env(cli_mod, runner, monkeypatch, git_checkout):
    target = git_checkout()
    (target / "Dockerfile").write_text("FROM python:3.11\n", encoding="utf-8")

    monkeypatch.setattr(cli_mod, "compute_docker_image_ref", lambda _: "demo-img:tag")
    monkeypatch.setattr(
        cli_mod,
        "write_devcontainer_config",
        lambda code_repository_dir, image_ref: code_repository_dir
        / ".devcontainer"
        / "devcontainer.json",
    )
    monkeypatch.setattr(
        cli_mod, "build_docker_environment", lambda code_repository_dir, image_ref: 0
    )

    result = runner.invoke(
        cli_mod.app,
        ["code-repository", "build-docker-env", "--path", str(target)],
    )
    assert result.exit_code == 0
    assert "Docker image built: demo-img:tag" in result.output
