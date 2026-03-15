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
            "id": 7,
            "username": "jose",
            "email": "jose@main-sequence.io",
            "organization": {"id": 2, "name": "Main Sequence"},
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
    assert "jose" in result.output
    assert "jose@main-sequence.io" in result.output
    assert "Main Sequence" in result.output


def test_pydantic_cli_metadata_from_source():
    metadata_mod = importlib.import_module("mainsequence.cli.pydantic_cli")
    meta = metadata_mod.get_cli_field_metadata(
        "mainsequence.client.models_helpers.Job",
        "execution_path",
    )
    assert meta.label == "Execution path"
    assert "content root" in meta.description
    assert "scripts/test.py" in meta.examples


def test_shared_compute_validation_supports_k8s_quantities(cli_mod):
    compute_mod = importlib.import_module("mainsequence.compute_validation")

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


def test_settings_set_backend(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(
        cli_mod.cfg,
        "set_backend_url",
        lambda url: {"backend_url": url},
    )
    result = runner.invoke(cli_mod.app, ["settings", "set-backend", "https://example.test"])
    assert result.exit_code == 0
    assert "Backend URL set to" in result.output


def test_config_normalize_backend_url(cli_mod):
    assert cli_mod.cfg.normalize_backend_url("127.0.0.1:800") == "http://127.0.0.1:800"
    assert cli_mod.cfg.normalize_backend_url("localhost:8000") == "http://localhost:8000"
    assert cli_mod.cfg.normalize_backend_url("main-sequence.app") == "https://main-sequence.app"
    assert cli_mod.cfg.normalize_backend_url("https://example.test/") == "https://example.test"


def test_config_normalize_mainsequence_path(cli_mod):
    assert cli_mod.cfg.normalize_mainsequence_path("mainsequence-dev").endswith("/mainsequence-dev")
    assert cli_mod.cfg.normalize_mainsequence_path("~/mainsequence-dev").endswith("/mainsequence-dev")


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
    cleared = {"called": False}
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
    monkeypatch.setattr(cli_mod.cfg, "clear_session_overrides", lambda: cleared.update(called=True))
    monkeypatch.setattr(cli_mod, "get_projects", lambda: [])
    monkeypatch.setattr(cli_mod, "_org_slug_from_profile", lambda: "default")

    result = runner.invoke(
        cli_mod.app,
        ["login", "user@example.com", "--password", "secret", "--no-status"],
    )
    assert result.exit_code == 0
    assert "MAIN SEQUENCE" in result.output
    assert "__  __" in result.output
    assert "Signed in as user@example.com" in result.output
    assert "Auth tokens are persisted in secure OS storage" in result.output
    assert cleared["called"] is True


def test_login_with_backend_override(cli_mod, runner, monkeypatch):
    seen = {}
    session_override = {}
    cleared = {"called": False}

    def _api_login(email, password):
        seen["backend"] = cli_mod.cfg.backend_url()
        return {"username": email, "backend": seen["backend"]}

    monkeypatch.setattr(cli_mod, "api_login", _api_login)
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": "/tmp/mainsequence", "backend_url": "https://main-sequence.app"},
    )
    monkeypatch.setattr(
        cli_mod.cfg,
        "set_session_overrides",
        lambda **kwargs: session_override.update(kwargs) or kwargs,
    )
    monkeypatch.setattr(cli_mod.cfg, "clear_session_overrides", lambda: cleared.update(called=True))
    monkeypatch.setattr(cli_mod.cfg, "secure_store_available", lambda: True)
    monkeypatch.setattr(cli_mod, "get_projects", lambda: [])
    monkeypatch.setattr(cli_mod, "_org_slug_from_profile", lambda: "default")
    monkeypatch.delenv("MAIN_SEQUENCE_BACKEND_URL", raising=False)

    result = runner.invoke(
        cli_mod.app,
        ["login", "user@example.com", "127.0.0.1:800", "mainsequence-dev", "--password", "secret", "--no-status"],
    )
    assert result.exit_code == 0
    assert seen["backend"] == "http://127.0.0.1:800"
    assert session_override["backend_url"] == "http://127.0.0.1:800"
    assert session_override["mainsequence_path"] == "mainsequence-dev"
    assert cleared["called"] is False
    assert "http://127.0.0.1:800" in result.output
    assert "MAIN_SEQUENCE_BACKEND_URL" not in os.environ


def test_login_with_different_backend_requires_projects_base(cli_mod, runner, monkeypatch):
    called = {"api_login": False}

    def _api_login(email, password):
        called["api_login"] = True
        return {"username": email, "backend": "http://127.0.0.1:8000"}

    monkeypatch.setattr(cli_mod, "api_login", _api_login)
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": "/tmp/mainsequence", "backend_url": "https://main-sequence.app"},
    )
    monkeypatch.delenv("MAIN_SEQUENCE_BACKEND_URL", raising=False)

    result = runner.invoke(
        cli_mod.app,
        ["login", "user@example.com", "127.0.0.1:8000", "--password", "secret", "--no-status"],
    )
    assert result.exit_code == 1
    assert "must also specify a different projects base folder" in result.output
    assert called["api_login"] is False


def test_login_with_different_backend_requires_different_projects_base(cli_mod, runner, monkeypatch):
    called = {"api_login": False}

    def _api_login(email, password):
        called["api_login"] = True
        return {"username": email, "backend": "http://127.0.0.1:8000"}

    monkeypatch.setattr(cli_mod, "api_login", _api_login)
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": "/tmp/mainsequence", "backend_url": "https://main-sequence.app"},
    )
    monkeypatch.delenv("MAIN_SEQUENCE_BACKEND_URL", raising=False)

    result = runner.invoke(
        cli_mod.app,
        ["login", "user@example.com", "127.0.0.1:8000", "/tmp/mainsequence", "--password", "secret", "--no-status"],
    )
    assert result.exit_code == 1
    assert "projects base folder must differ from the current one" in result.output
    assert called["api_login"] is False


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
    monkeypatch.setattr(cli_mod.cfg, "clear_session_overrides", lambda: None)
    monkeypatch.setattr(cli_mod, "get_projects", lambda: [])
    monkeypatch.setattr(cli_mod, "_org_slug_from_profile", lambda: "default")

    result = runner.invoke(
        cli_mod.app,
        ["login", "user@example.com", "--password", "secret", "--no-status", "--export"],
    )
    assert result.exit_code == 0
    assert 'export MAINSEQUENCE_ACCESS_TOKEN="acc-123"' in result.output
    assert 'export MAINSEQUENCE_REFRESH_TOKEN="ref-456"' in result.output
    assert 'export MAINSEQUENCE_USERNAME="user@example.com"' in result.output


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
    monkeypatch.setattr(cli_mod.cfg, "clear_session_overrides", lambda: None)
    monkeypatch.setattr(cli_mod, "get_projects", lambda: [])
    monkeypatch.setattr(cli_mod, "_org_slug_from_profile", lambda: "default")

    result = runner.invoke(
        cli_mod.app,
        ["login", "user@example.com", "--password", "secret", "--no-status"],
    )
    assert result.exit_code == 0
    assert "Could not persist auth tokens in secure OS storage" in result.output


def test_logout(cli_mod, runner, monkeypatch):
    cleared = {"called": False}
    monkeypatch.setattr(cli_mod.cfg, "clear_tokens", lambda: True)
    monkeypatch.setattr(cli_mod.cfg, "clear_session_overrides", lambda: cleared.update(called=True))
    result = runner.invoke(cli_mod.app, ["logout"])
    assert result.exit_code == 0
    assert "Signed out" in result.output
    assert cleared["called"] is True


def test_logout_export_env(cli_mod, runner, monkeypatch):
    cleared = {"called": False}
    monkeypatch.setattr(cli_mod.cfg, "clear_tokens", lambda: True)
    monkeypatch.setattr(cli_mod.cfg, "clear_session_overrides", lambda: cleared.update(called=True))
    result = runner.invoke(cli_mod.app, ["logout", "--export"])
    assert result.exit_code == 0
    assert "unset MAINSEQUENCE_ACCESS_TOKEN" in result.output
    assert "unset MAINSEQUENCE_REFRESH_TOKEN" in result.output
    assert "unset MAINSEQUENCE_USERNAME" in result.output
    assert cleared["called"] is True


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


def test_prime_runtime_env_prefers_local_project_env(cli_mod, monkeypatch, tmp_path):
    bootstrap = importlib.import_module("mainsequence.bootstrap")
    project_dir = tmp_path / "project"
    project_dir.mkdir(parents=True, exist_ok=True)
    (project_dir / ".env").write_text(
        "TDAG_ENDPOINT=https://project-backend.test\n"
        "MAIN_SEQUENCE_PROJECT_ID=123\n",
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
        "TDAG_ENDPOINT",
        "MAINSEQUENCE_ENDPOINT",
        "MAIN_SEQUENCE_PROJECT_ID",
        "MAINSEQUENCE_ACCESS_TOKEN",
        "MAINSEQUENCE_REFRESH_TOKEN",
    ):
        monkeypatch.delenv(key, raising=False)

    bootstrap.prime_runtime_env()

    assert os.environ["TDAG_ENDPOINT"] == "https://project-backend.test"
    assert os.environ["MAINSEQUENCE_ENDPOINT"] == "https://project-backend.test"
    assert os.environ["MAIN_SEQUENCE_PROJECT_ID"] == "123"
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
        "TDAG_ENDPOINT",
        "MAINSEQUENCE_ENDPOINT",
        "MAIN_SEQUENCE_PROJECT_ID",
        "MAINSEQUENCE_ACCESS_TOKEN",
        "MAINSEQUENCE_REFRESH_TOKEN",
    ):
        monkeypatch.delenv(key, raising=False)

    bootstrap.prime_runtime_env()

    assert os.environ["TDAG_ENDPOINT"] == "http://127.0.0.1:8000"
    assert os.environ["MAINSEQUENCE_ENDPOINT"] == "http://127.0.0.1:8000"
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


def test_project_get_data_node_updates(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "get_project_data_node_updates",
        lambda project_id, timeout=None: [
            {
                "id": 10,
                "update_hash": "abc123",
                "data_node_storage": {"id": 42, "storage_hash": "storage-xyz"},
                "update_details": {"id": 77},
            }
        ],
    )

    result = runner.invoke(cli_mod.app, ["project", "data-node-updates", "list", "123"])
    assert result.exit_code == 0
    assert "Project Data Node Updates" in result.output
    assert "abc123" in result.output
    assert "storage-xyz" in result.output
    assert "Total updates: 1" in result.output


def test_get_project_data_node_updates_sets_project_env(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"})
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")
    monkeypatch.delenv("MAIN_SEQUENCE_PROJECT_ID", raising=False)

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
    fake_models = types.ModuleType("mainsequence.client.models_tdag")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.TDAG_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/orm/api"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/orm/api"

    class FakeUpdate:
        def model_dump(self):
            return {"id": 10, "update_hash": "abc123"}

    class FakeProject:
        ROOT_URL = "https://old.test/orm/api/pods/project"

        @classmethod
        def get(cls, pk, timeout=None):
            captured["project_id_arg"] = pk
            captured["env_project_id"] = os.environ.get("MAIN_SEQUENCE_PROJECT_ID")
            return types.SimpleNamespace(get_data_nodes_updates=lambda timeout=None: [FakeUpdate()])

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.Project = FakeProject
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_tdag", fake_models)

    out = api_mod.get_project_data_node_updates(123)
    assert captured["project_id_arg"] == 123
    assert captured["env_project_id"] == "123"
    assert captured["jwt"] == ("acc", "ref")
    assert out == [{"id": 10, "update_hash": "abc123"}]
    assert os.environ.get("MAIN_SEQUENCE_PROJECT_ID") is None


def test_create_project_image_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"})
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")
    monkeypatch.delenv("MAIN_SEQUENCE_PROJECT_ID", raising=False)

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
    fake_models = types.ModuleType("mainsequence.client.models_tdag")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.TDAG_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/orm/api"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/orm/api"

    class FakeProjectImage:
        ROOT_URL = "https://old.test/orm/api/pods/project-image"

        @classmethod
        def create(cls, *, project_repo_hash, related_project_id=None, base_image_id=None, timeout=None):
            captured["project_repo_hash"] = project_repo_hash
            captured["related_project_id"] = related_project_id
            captured["base_image_id"] = base_image_id
            captured["env_project_id"] = os.environ.get("MAIN_SEQUENCE_PROJECT_ID")
            return types.SimpleNamespace(
                model_dump=lambda: {
                    "id": 77,
                    "project_repo_hash": project_repo_hash,
                    "related_project": related_project_id,
                    "base_image": base_image_id,
                }
            )

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.ProjectImage = FakeProjectImage
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_tdag", fake_models)

    out = api_mod.create_project_image(project_repo_hash="abc123", related_project_id=123, base_image_id=22)
    assert captured["project_repo_hash"] == "abc123"
    assert captured["related_project_id"] == 123
    assert captured["base_image_id"] == 22
    assert captured["env_project_id"] == "123"
    assert captured["jwt"] == ("acc", "ref")
    assert out["id"] == 77
    assert os.environ.get("MAIN_SEQUENCE_PROJECT_ID") is None


def test_list_project_images_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {"filters": []}

    monkeypatch.setattr(api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"})
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")
    monkeypatch.delenv("MAIN_SEQUENCE_PROJECT_ID", raising=False)

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
    fake_models = types.ModuleType("mainsequence.client.models_tdag")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.TDAG_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/orm/api"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/orm/api"

    class FakeProjectImage:
        ROOT_URL = "https://old.test/orm/api/pods/project-image"

        @classmethod
        def filter(cls, timeout=None, **kwargs):
            captured["filters"].append(kwargs)
            captured["env_project_id"] = os.environ.get("MAIN_SEQUENCE_PROJECT_ID")
            if "related_project__id__in" in kwargs:
                return [
                    types.SimpleNamespace(
                        model_dump=lambda: {
                            "id": 77,
                            "project_repo_hash": "abc123",
                            "related_project": 123,
                            "base_image": {"id": 22, "title": "Python 3.12"},
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
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_tdag", fake_models)

    out = api_mod.list_project_images(related_project_id=123)
    assert captured["filters"][0] == {"related_project__id__in": [123]}
    assert captured["env_project_id"] == "123"
    assert captured["jwt"] == ("acc", "ref")
    assert out == [
        {
            "id": 77,
            "project_repo_hash": "abc123",
            "related_project": 123,
            "base_image": {"id": 22, "title": "Python 3.12"},
        }
    ]
    assert os.environ.get("MAIN_SEQUENCE_PROJECT_ID") is None


def test_list_project_jobs_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {"filters": []}

    monkeypatch.setattr(api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"})
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")
    monkeypatch.delenv("MAIN_SEQUENCE_PROJECT_ID", raising=False)

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
    fake_helpers = types.ModuleType("mainsequence.client.models_helpers")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.TDAG_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/orm/api"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/orm/api"

    class FakeJob:
        ROOT_URL = "https://old.test/orm/api/pods/job"

        @classmethod
        def filter(cls, timeout=None, **kwargs):
            captured["filters"].append(kwargs)
            captured["env_project_id"] = os.environ.get("MAIN_SEQUENCE_PROJECT_ID")
            if "project" in kwargs:
                return [
                    types.SimpleNamespace(
                        model_dump=lambda: {
                            "id": 91,
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
                    )
                ]
            return []

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_helpers.Job = FakeJob
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_helpers", fake_helpers)

    out = api_mod.list_project_jobs(project_id=123)
    assert captured["filters"][0] == {"project": 123}
    assert captured["env_project_id"] == "123"
    assert captured["jwt"] == ("acc", "ref")
    assert out == [
        {
            "id": 91,
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
    ]
    assert os.environ.get("MAIN_SEQUENCE_PROJECT_ID") is None


def test_list_project_resources_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {"filters": []}

    monkeypatch.setattr(api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"})
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")
    monkeypatch.delenv("MAIN_SEQUENCE_PROJECT_ID", raising=False)

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
    fake_helpers = types.ModuleType("mainsequence.client.models_helpers")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.TDAG_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/orm/api"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/orm/api"

    class FakeProjectResource:
        ROOT_URL = "https://old.test/orm/api/pods/project-resource"

        @classmethod
        def filter(cls, timeout=None, **kwargs):
            captured["filters"].append(kwargs)
            captured["env_project_id"] = os.environ.get("MAIN_SEQUENCE_PROJECT_ID")
            return [
                types.SimpleNamespace(
                    model_dump=lambda: {
                        "id": 301,
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
        project_id=123,
        repo_commit_sha="abc123",
        resource_type="dashboard",
    )
    assert captured["filters"][0] == {
        "project__id": 123,
        "repo_commit_sha": "abc123",
        "resource_type": "dashboard",
    }
    assert captured["env_project_id"] == "123"
    assert captured["jwt"] == ("acc", "ref")
    assert out[0]["name"] == "analytics_dashboard.py"
    assert os.environ.get("MAIN_SEQUENCE_PROJECT_ID") is None


def test_create_project_resource_release_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"})
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
    fake_utils.TDAG_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/orm/api"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/orm/api"

    class FakeProjectResource:
        ROOT_URL = "https://old.test/orm/api/pods/project-resource"

        @classmethod
        def get(cls, pk=None, timeout=None, **filters):
            captured["get"] = {"pk": pk, "timeout": timeout, "filters": filters}

            class _Resource:
                def create_dashboard(self, **kwargs):
                    captured["create_dashboard"] = kwargs
                    return types.SimpleNamespace(model_dump=lambda: {"id": 501, "resource": pk, "related_image": kwargs["related_image_id"]})

                def create_agent(self, **kwargs):
                    captured["create_agent"] = kwargs
                    return types.SimpleNamespace(model_dump=lambda: {"id": 502, "resource": pk, "related_image": kwargs["related_image_id"]})

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
        resource_id=381,
        related_image_id=94,
        spot=True,
        cpu_request="0.5",
        memory_request="1",
        gpu_request="",
        gpu_type="",
    )
    assert captured["get"] == {"pk": 381, "timeout": None, "filters": {}}
    assert captured["create_dashboard"]["related_image_id"] == 94
    assert captured["create_dashboard"]["spot"] is True
    assert captured["create_dashboard"]["cpu_request"] == "0.5"
    assert captured["create_dashboard"]["memory_request"] == "1"
    assert captured["jwt"] == ("acc", "ref")
    assert out == {"id": 501, "resource": 381, "related_image": 94}


def test_list_market_portfolios_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {"filters": []}

    monkeypatch.setattr(api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"})
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
    fake_vam = types.ModuleType("mainsequence.client.models_vam")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.TDAG_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/orm/api"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/orm/api"

    class FakePortfolio:
        ROOT_URL = "https://old.test/orm/api/vam/portfolio"

        @classmethod
        def filter(cls, timeout=None, **kwargs):
            captured["filters"].append(kwargs)
            return [
                types.SimpleNamespace(
                    model_dump=lambda: {
                        "id": 42,
                        "index_asset": {"name": "Growth Model", "unique_identifier": "growth-model"},
                        "calendar": {"name": "NYSE"},
                        "data_node_update": {"id": 901, "update_hash": "weights_daily"},
                        "signal_data_node_update": {"id": 902, "update_hash": "signal_daily"},
                    }
                )
            ]

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_vam.Portfolio = FakePortfolio
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_vam", fake_vam)

    out = api_mod.list_market_portfolios()
    assert captured["filters"][0] == {}
    assert captured["jwt"] == ("acc", "ref")
    assert out == [
        {
            "id": 42,
            "index_asset": {"name": "Growth Model", "unique_identifier": "growth-model"},
            "calendar": {"name": "NYSE"},
            "data_node_update": {"id": 901, "update_hash": "weights_daily"},
            "signal_data_node_update": {"id": 902, "update_hash": "signal_daily"},
        }
    ]


def test_list_market_asset_translation_tables_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {"filters": []}

    monkeypatch.setattr(api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"})
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
    fake_vam = types.ModuleType("mainsequence.client.models_vam")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.TDAG_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/orm/api"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/orm/api"

    class FakeAssetTranslationTable:
        ROOT_URL = "https://old.test/orm/api/assets/asset-translation-tables"

        @classmethod
        def filter(cls, timeout=None, **kwargs):
            captured["filters"].append(kwargs)
            return [
                types.SimpleNamespace(
                    model_dump=lambda: {
                        "id": 12,
                        "unique_identifier": "prices_translation_table_1d",
                        "rules": [
                            {
                                "id": 100,
                                "asset_filter": {"security_market_sector": "Crypto"},
                                "markets_time_serie_unique_identifier": "binance_1d_bars",
                                "target_exchange_code": "krkn",
                                "default_column_name": "close",
                            }
                        ],
                    }
                )
            ]

        @classmethod
        def get(cls, pk=None, timeout=None, **filters):
            captured["get"] = {"pk": pk, "filters": filters, "timeout": timeout}
            return types.SimpleNamespace(
                model_dump=lambda: {
                    "id": pk,
                    "unique_identifier": "prices_translation_table_1d",
                    "rules": [
                        {
                            "id": 100,
                            "asset_filter": {"security_market_sector": "Crypto"},
                            "markets_time_serie_unique_identifier": "binance_1d_bars",
                            "target_exchange_code": "krkn",
                            "default_column_name": "close",
                        }
                    ],
                }
            )

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_vam.AssetTranslationTable = FakeAssetTranslationTable
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_vam", fake_vam)

    listed = api_mod.list_market_asset_translation_tables()
    assert captured["filters"][0] == {}
    assert captured["jwt"] == ("acc", "ref")
    assert listed == [
        {
            "id": 12,
            "unique_identifier": "prices_translation_table_1d",
            "rules": [
                {
                    "id": 100,
                    "asset_filter": {"security_market_sector": "Crypto"},
                    "markets_time_serie_unique_identifier": "binance_1d_bars",
                    "target_exchange_code": "krkn",
                    "default_column_name": "close",
                }
            ],
        }
    ]

    detail = api_mod.get_market_asset_translation_table(12)
    assert captured["get"] == {"pk": 12, "filters": {}, "timeout": None}
    assert detail["id"] == 12


def test_get_logged_user_details_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"})
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")
    monkeypatch.setattr(
        api_mod,
        "authed",
        lambda method, api_path, body=None: types.SimpleNamespace(
            ok=True,
            json=lambda: {"id": 7},
        ),
    )

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
    fake_models_user = types.ModuleType("mainsequence.client.models_user")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    class FakeHeadersContext:
        current = None

        def set(self, value):
            self.current = value
            captured["headers_set"] = value
            return "token"

        def reset(self, token):
            captured["headers_reset"] = token
            self.current = None

    fake_utils.loaders = FakeLoaders()
    fake_utils.TDAG_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/orm/api"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/orm/api"

    fake_headers = FakeHeadersContext()

    class FakeUser:
        ROOT_URL = "https://old.test/orm/api/user/user"

        @classmethod
        def get_logged_user(cls):
            captured["headers_seen"] = fake_headers.current
            return types.SimpleNamespace(
                model_dump=lambda: {
                    "id": 7,
                    "username": "jose",
                    "email": "jose@main-sequence.io",
                    "organization": {"id": 2, "name": "Main Sequence"},
                }
            )

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models_user.User = FakeUser
    fake_models_user._CURRENT_AUTH_HEADERS = fake_headers
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_user", fake_models_user)

    out = api_mod.get_logged_user_details()
    assert captured["jwt"] == ("acc", "ref")
    assert captured["headers_set"] == {"X-User-ID": "7"}
    assert captured["headers_seen"] == {"X-User-ID": "7"}
    assert captured["headers_reset"] == "token"
    assert out["username"] == "jose"


def test_sync_project_after_commit_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"})
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")
    monkeypatch.delenv("MAIN_SEQUENCE_PROJECT_ID", raising=False)

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
    fake_models = types.ModuleType("mainsequence.client.models_tdag")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.TDAG_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/orm/api"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/orm/api"

    class FakeProject:
        ROOT_URL = "https://old.test/orm/api/pods/projects"

        @classmethod
        def sync_project_after_commit(cls, project_id, timeout=None):
            captured["project_id"] = project_id
            captured["timeout"] = timeout
            captured["env_project_id"] = os.environ.get("MAIN_SEQUENCE_PROJECT_ID")
            return types.SimpleNamespace(model_dump=lambda: {"id": project_id, "project_name": "Demo"})

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.Project = FakeProject
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_tdag", fake_models)

    out = api_mod.sync_project_after_commit(123)
    assert captured["project_id"] == 123
    assert captured["timeout"] is None
    assert captured["env_project_id"] == "123"
    assert captured["jwt"] == ("acc", "ref")
    assert out == {"id": 123, "project_name": "Demo"}
    assert os.environ.get("MAIN_SEQUENCE_PROJECT_ID") is None


def test_create_project_job_uses_client_model_task_schedule(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"})
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")
    monkeypatch.delenv("MAIN_SEQUENCE_PROJECT_ID", raising=False)

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
    fake_helpers = types.ModuleType("mainsequence.client.models_helpers")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.TDAG_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/orm/api"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/orm/api"

    class FakeJob:
        ROOT_URL = "https://old.test/orm/api/pods/job"

        @classmethod
        def create(
            cls,
            *,
            name,
            project_id,
            execution_path=None,
            app_name=None,
            task_schedule=None,
            cpu_request=None,
            memory_request=None,
            gpu_request=None,
            gpu_type=None,
            spot=None,
            max_runtime_seconds=None,
            related_image_id=None,
            timeout=None,
        ):
            captured["payload"] = {
                "name": name,
                "project_id": project_id,
                "execution_path": execution_path,
                "app_name": app_name,
                "task_schedule": task_schedule,
                "cpu_request": cpu_request,
                "memory_request": memory_request,
                "gpu_request": gpu_request,
                "gpu_type": gpu_type,
                "spot": spot,
                "max_runtime_seconds": max_runtime_seconds,
                "related_image_id": related_image_id,
                "timeout": timeout,
            }
            captured["env_project_id"] = os.environ.get("MAIN_SEQUENCE_PROJECT_ID")
            return types.SimpleNamespace(model_dump=lambda: {"id": 91, "task_schedule": task_schedule})

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
        project_id=123,
        execution_path="scripts/test.py",
        task_schedule=schedule,
        cpu_request="0.25",
        memory_request="0.5",
        spot=False,
        max_runtime_seconds=86400,
        related_image_id=77,
    )
    assert captured["payload"]["task_schedule"] == schedule
    assert captured["env_project_id"] == "123"
    assert captured["jwt"] == ("acc", "ref")
    assert out == {"id": 91, "task_schedule": schedule}
    assert os.environ.get("MAIN_SEQUENCE_PROJECT_ID") is None


def test_run_project_job_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"})
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
    fake_utils.TDAG_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/orm/api"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/orm/api"

    class FakeJob:
        ROOT_URL = "https://old.test/orm/api/pods/job"

        @classmethod
        def get(cls, pk, timeout=None):
            captured["job_id_arg"] = pk
            return types.SimpleNamespace(
                run_job=lambda timeout=None: {
                    "id": 501,
                    "job": pk,
                    "status": "QUEUED",
                    "unique_identifier": "jobrun_abc123",
                }
            )

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_helpers.Job = FakeJob
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_helpers", fake_helpers)

    out = api_mod.run_project_job(91)
    assert captured["job_id_arg"] == 91
    assert captured["jwt"] == ("acc", "ref")
    assert out == {
        "id": 501,
        "job": 91,
        "status": "QUEUED",
        "unique_identifier": "jobrun_abc123",
    }


def test_list_project_job_runs_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {"filters": []}

    monkeypatch.setattr(api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"})
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
    fake_utils.TDAG_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/orm/api"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/orm/api"

    class FakeJobRun:
        ROOT_URL = "https://old.test/orm/api/pods/job-run"

        @classmethod
        def filter(cls, timeout=None, **kwargs):
            captured["filters"].append(kwargs)
            return [
                types.SimpleNamespace(
                    model_dump=lambda: {
                        "id": 501,
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

    out = api_mod.list_project_job_runs(job_id=91)
    assert captured["filters"][0] == {"job__id": [91]}
    assert captured["jwt"] == ("acc", "ref")
    assert out == [
        {
            "id": 501,
            "name": "daily-run-1",
            "status": "COMPLETED",
            "unique_identifier": "jobrun_abc123",
        }
    ]


def test_get_project_job_run_logs_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"})
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
    fake_utils.TDAG_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/orm/api"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/orm/api"

    class FakeJobRun:
        ROOT_URL = "https://old.test/orm/api/pods/job-run"

        @classmethod
        def get(cls, pk, timeout=None):
            captured["job_run_id_arg"] = pk
            return types.SimpleNamespace(
                get_logs=lambda timeout=None: {
                    "job_run_id": pk,
                    "status": "RUNNING",
                    "rows": ["first line"],
                }
            )

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_helpers.JobRun = FakeJobRun
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_helpers", fake_helpers)

    out = api_mod.get_project_job_run_logs(501)
    assert captured["job_run_id_arg"] == 501
    assert captured["jwt"] == ("acc", "ref")
    assert out == {
        "job_run_id": 501,
        "status": "RUNNING",
        "rows": ["first line"],
    }


def test_project_get_data_node_updates_defaults_to_env_project_id(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "demo-123"
    target.mkdir(parents=True, exist_ok=True)
    (target / ".env").write_text("MAIN_SEQUENCE_PROJECT_ID=123\n", encoding="utf-8")

    captured = {}

    monkeypatch.chdir(target)
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _get_updates(project_id, timeout=None):
        captured["project_id"] = project_id
        return [
            {
                "id": 10,
                "update_hash": "abc123",
                "data_node_storage": {"id": 42, "storage_hash": "storage-xyz"},
                "update_details": {"id": 77},
            }
        ]

    monkeypatch.setattr(cli_mod, "get_project_data_node_updates", _get_updates)

    result = runner.invoke(cli_mod.app, ["project", "data-node-updates", "list"])
    assert result.exit_code == 0
    assert captured["project_id"] == 123
    assert "abc123" in result.output
    assert "storage-xyz" in result.output


def test_project_images_defaults_to_env_project_id(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "demo-123"
    target.mkdir(parents=True, exist_ok=True)
    (target / ".env").write_text("MAIN_SEQUENCE_PROJECT_ID=123\n", encoding="utf-8")

    monkeypatch.chdir(target)
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_project_images",
        lambda related_project_id, timeout=None: [
            {
                "id": 77,
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


def test_project_jobs_list_defaults_to_env_project_id(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "demo-123"
    target.mkdir(parents=True, exist_ok=True)
    (target / ".env").write_text("MAIN_SEQUENCE_PROJECT_ID=123\n", encoding="utf-8")

    monkeypatch.chdir(target)
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_project_jobs",
        lambda project_id, timeout=None: [
            {
                "id": 91,
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
    assert "daily-run" in result.output
    assert "abc123" in result.output
    assert "Every" in result.output
    assert "hour:" in result.output
    assert "every 1" in result.output
    assert "hours" in result.output
    assert "Total jobs: 1" in result.output


def test_project_project_resource_list_defaults_to_remote_branch_head(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "demo-123"
    target.mkdir(parents=True, exist_ok=True)
    (target / ".env").write_text("MAIN_SEQUENCE_PROJECT_ID=123\n", encoding="utf-8")
    captured = {}

    monkeypatch.chdir(target)
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "_get_remote_branch_head_commit",
        lambda project_dir: ("origin/main", "abc123"),
    )

    def _list_project_resources(project_id, repo_commit_sha, resource_type=None, timeout=None):
        captured["project_id"] = project_id
        captured["repo_commit_sha"] = repo_commit_sha
        captured["resource_type"] = resource_type
        return [
            {
                "id": 301,
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
    assert captured["project_id"] == 123
    assert captured["repo_commit_sha"] == "abc123"
    assert "Using repo_commit_sha=abc123 from origin/main." in result.output
    assert "Project Resources" in result.output
    assert "analytics_dash" in result.output
    assert "board.py" in result.output
    assert "Total project resources: 1" in result.output


def test_project_project_resource_create_dashboard_filters_resources_by_selected_image(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "demo-123"
    target.mkdir(parents=True, exist_ok=True)
    (target / ".env").write_text("MAIN_SEQUENCE_PROJECT_ID=123\n", encoding="utf-8")
    captured = {}

    monkeypatch.chdir(target)
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_project_images",
        lambda related_project_id, timeout=None: [
            {"id": 94, "project_repo_hash": "sha-94", "base_image": {"id": 1, "title": "py311"}},
            {"id": 95, "project_repo_hash": "sha-95", "base_image": {"id": 2, "title": "py312"}},
        ],
    )

    def _list_project_resources(project_id, repo_commit_sha, resource_type=None, timeout=None):
        captured["project_id"] = project_id
        captured["repo_commit_sha"] = repo_commit_sha
        captured["resource_type"] = resource_type
        return [
            {
                "id": 381,
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
            "id": 501,
            "resource": kwargs["resource_id"],
            "related_image": kwargs["related_image_id"],
            "cpu_request": kwargs["cpu_request"],
            "memory_request": kwargs["memory_request"],
            "spot": kwargs["spot"],
        }

    monkeypatch.setattr(cli_mod, "create_project_resource_release", _create_release)

    result = runner.invoke(
        cli_mod.app,
        ["project", "project_resource", "create_dashboard"],
        input="94\n381\n",
    )
    assert result.exit_code == 0
    assert captured["project_id"] == 123
    assert captured["repo_commit_sha"] == "sha-94"
    assert captured["resource_type"] == "dashboard"
    assert captured["create_release"]["release_kind"] == "streamlit_dashboard"
    assert captured["create_release"]["related_image_id"] == 94
    assert captured["create_release"]["resource_id"] == 381
    assert captured["create_release"]["cpu_request"] == "0.25"
    assert captured["create_release"]["memory_request"] == "0.5"
    assert captured["create_release"]["spot"] is False
    assert "Using defaults: cpu_request=0.25, memory_request=0.5, spot=false." in result.output
    assert "Project resource release created: id=501" in result.output


def test_markets_portfolios_list(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_market_portfolios",
        lambda timeout=None: [
            {
                "id": 42,
                "index_asset": {"name": "Growth Model", "unique_identifier": "growth-model"},
                "calendar": {"name": "NYSE"},
                "data_node_update": {"id": 901, "update_hash": "weights_daily"},
                "signal_data_node_update": {"id": 902, "update_hash": "signal_daily"},
            }
        ],
    )

    result = runner.invoke(cli_mod.app, ["markets", "portfolios", "list"])
    assert result.exit_code == 0
    assert "Markets Portfolios" in result.output
    assert "Growth Model" in result.output
    assert "growth-model" in result.output
    assert "weights_daily" in result.output
    assert "signal_daily" in result.output
    assert "Total portfolios: 1" in result.output


def test_markets_asset_translation_table_list(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_market_asset_translation_tables",
        lambda timeout=None: [
            {
                "id": 12,
                "unique_identifier": "prices_translation_table_1d",
                "rules": [
                    {
                        "id": 100,
                        "asset_filter": {"security_market_sector": "Crypto"},
                        "markets_time_serie_unique_identifier": "binance_1d_bars",
                        "target_exchange_code": "krkn",
                        "default_column_name": "close",
                    }
                ],
            }
        ],
    )

    result = runner.invoke(cli_mod.app, ["markets", "asset-translation-table", "list"])
    assert result.exit_code == 0
    assert "Markets Asset Translation Tables" in result.output
    assert "prices_translation_table_1d" in result.output
    assert "Crypto" in result.output
    assert "binance_1d_bars" in result.output
    assert "Total asset translation tables: 1" in result.output


def test_markets_asset_translation_table_detail(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "get_market_asset_translation_table",
        lambda table_id, timeout=None: {
            "id": table_id,
            "unique_identifier": "prices_translation_table_1d",
            "rules": [
                {
                    "id": 100,
                    "asset_filter": {
                        "security_type": "ETP",
                        "security_market_sector": "Crypto",
                    },
                    "markets_time_serie_unique_identifier": "binance_1d_bars",
                    "target_exchange_code": "krkn",
                    "default_column_name": "close",
                }
            ],
        },
    )

    result = runner.invoke(cli_mod.app, ["markets", "asset-translation-table", "detail", "12"])
    assert result.exit_code == 0
    assert "Markets Asset Translation Table" in result.output
    assert "prices_translation_table_1d" in result.output
    assert "Rules" in result.output
    assert "security_type=ETP" in result.output
    assert "market_sector=Crypto" in result.output
    assert "binance_1d_bars @" in result.output
    assert "krkn (close)" in result.output


def test_project_jobs_run(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "run_project_job",
        lambda job_id, timeout=None: {
            "id": 501,
            "job": job_id,
            "status": "QUEUED",
            "unique_identifier": "jobrun_abc123",
        },
    )

    result = runner.invoke(cli_mod.app, ["project", "jobs", "run", "91"])
    assert result.exit_code == 0
    assert "Project job run requested: job_id=91" in result.output
    assert "jobrun_abc123" in result.output
    assert "QUEUED" in result.output


def test_project_job_runs_list(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_project_job_runs",
        lambda job_id, timeout=None: [
            {
                "id": 501,
                "name": "daily-run-1",
                "status": "COMPLETED",
                "execution_start": "2026-03-14T09:00:00Z",
                "execution_end": "2026-03-14T09:10:00Z",
                "unique_identifier": "jobrun_abc123",
                "commit_hash": "abc123",
            }
        ],
    )

    result = runner.invoke(cli_mod.app, ["project", "jobs", "runs", "list", "91"])
    assert result.exit_code == 0
    assert "Project Job Runs" in result.output
    assert "daily-run" in result.output
    assert "jobrun_ab" in result.output
    assert "Total job runs: 1" in result.output


def test_project_job_runs_logs(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "get_project_job_run_logs",
        lambda job_run_id, timeout=None: {
            "job_run_id": job_run_id,
            "status": "COMPLETED",
            "rows": [
                {"timestamp": "2026-03-14T09:00:00Z", "level": "info", "event": "job started"},
                {"timestamp": "2026-03-14T09:10:00Z", "level": "info", "event": "job finished"},
            ],
        },
    )

    result = runner.invoke(cli_mod.app, ["project", "jobs", "runs", "logs", "501"])
    assert result.exit_code == 0
    assert "Job Run Logs" in result.output
    assert "job started" in result.output
    assert "job finished" in result.output
    assert "COMPLETED" in result.output


def test_project_job_runs_logs_polls_and_prints_incrementally(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    responses = iter(
        [
            {"job_run_id": 501, "status": "PENDING", "rows": ["first line"]},
            {"job_run_id": 501, "status": "RUNNING", "rows": ["first line", "second line"]},
            {"job_run_id": 501, "status": "COMPLETED", "rows": ["first line", "second line", "third line"]},
        ]
    )
    sleeps = []

    monkeypatch.setattr(cli_mod, "get_project_job_run_logs", lambda job_run_id, timeout=None: next(responses))
    monkeypatch.setattr(cli_mod.time, "sleep", lambda seconds: sleeps.append(seconds))

    result = runner.invoke(cli_mod.app, ["project", "jobs", "runs", "logs", "501", "--poll-interval", "3"])
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
            {"job_run_id": 742, "status": "PENDING", "rows": ["first line"]},
            {"job_run_id": 742, "status": "RUNNING", "rows": ["first line", "second line"]},
        ]
    )
    sleeps = []
    monotonic_values = iter([100.0, 100.0, 106.0])

    monkeypatch.setattr(cli_mod, "get_project_job_run_logs", lambda job_run_id, timeout=None: next(responses))
    monkeypatch.setattr(cli_mod.time, "sleep", lambda seconds: sleeps.append(seconds))
    monkeypatch.setattr(cli_mod.time, "monotonic", lambda: next(monotonic_values))

    result = runner.invoke(
        cli_mod.app,
        ["project", "jobs", "runs", "logs", "742", "--poll-interval", "3", "--max-wait-seconds", "5"],
    )
    assert result.exit_code == 0
    assert result.output.count("first line") == 1
    assert result.output.count("second line") == 1
    assert "Stopping log polling after 5s while job run is still RUNNING." in result.output
    assert sleeps == [3]


def test_project_jobs_create_interactive_defaults(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "demo-123"
    target.mkdir(parents=True, exist_ok=True)
    (target / ".env").write_text("MAIN_SEQUENCE_PROJECT_ID=123\n", encoding="utf-8")

    monkeypatch.chdir(target)
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_project_images",
        lambda related_project_id, timeout=None: [
            {
                "id": 77,
                "project_repo_hash": "abc123",
                "base_image": {"id": 22, "title": "Python 3.12"},
            }
        ],
    )

    captured = {}

    def _create_project_job(**kwargs):
        captured.update(kwargs)
        return {
            "id": 91,
            "name": kwargs["name"],
            "execution_path": kwargs["execution_path"],
            "app_name": kwargs["app_name"],
            "related_image": kwargs["related_image_id"],
        }

    monkeypatch.setattr(cli_mod, "create_project_job", _create_project_job)

    result = runner.invoke(cli_mod.app, ["project", "jobs", "create"], input="demo-job\n\nscripts/test.py\n\n")
    assert result.exit_code == 0
    assert captured["project_id"] == 123
    assert captured["name"] == "demo-job"
    assert captured["related_image_id"] == 77
    assert captured["execution_path"] == "scripts/test.py"
    assert captured["task_schedule"] is None
    assert captured["cpu_request"] == "0.25"
    assert captured["memory_request"] == "0.5"
    assert captured["spot"] is False
    assert captured["max_runtime_seconds"] == 86400
    assert (
        "Using defaults: cpu_request=0.25, memory_request=0.5, spot=false, max_runtime_seconds=86400."
        in result.output
    )
    assert "Project job created: id=91" in result.output


def test_project_jobs_create_derives_memory_from_cpu(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "demo-123"
    target.mkdir(parents=True, exist_ok=True)
    (target / ".env").write_text("MAIN_SEQUENCE_PROJECT_ID=123\n", encoding="utf-8")

    monkeypatch.chdir(target)
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_project_images",
        lambda related_project_id, timeout=None: [
            {
                "id": 77,
                "project_repo_hash": "abc123",
                "base_image": {"id": 22, "title": "Python 3.12"},
            }
        ],
    )

    captured = {}

    def _create_project_job(**kwargs):
        captured.update(kwargs)
        return {
            "id": 91,
            "name": kwargs["name"],
            "execution_path": kwargs["execution_path"],
            "related_image": kwargs["related_image_id"],
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
            "--related-image-id",
            "77",
        ],
        input="\n",
    )
    assert result.exit_code == 0
    assert captured["cpu_request"] == "1"
    assert captured["memory_request"] == "1"
    assert captured["spot"] is False
    assert captured["max_runtime_seconds"] == 86400
    assert captured["task_schedule"] is None
    assert "Using defaults: memory_request=1, spot=false, max_runtime_seconds=86400." in result.output


def test_project_jobs_create_interactive_interval_schedule(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "demo-123"
    target.mkdir(parents=True, exist_ok=True)
    (target / ".env").write_text("MAIN_SEQUENCE_PROJECT_ID=123\n", encoding="utf-8")

    monkeypatch.chdir(target)
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_project_images",
        lambda related_project_id, timeout=None: [
            {
                "id": 77,
                "project_repo_hash": "abc123",
                "base_image": {"id": 22, "title": "Python 3.12"},
            }
        ],
    )

    captured = {}

    def _create_project_job(**kwargs):
        captured.update(kwargs)
        return {
            "id": 91,
            "name": kwargs["name"],
            "execution_path": kwargs["execution_path"],
            "task_schedule": kwargs["task_schedule"],
            "related_image": kwargs["related_image_id"],
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
    (target / ".env").write_text("MAIN_SEQUENCE_PROJECT_ID=123\n", encoding="utf-8")

    monkeypatch.chdir(target)
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_project_base_images",
        lambda: [{"id": 22, "title": "Python 3.12", "description": "Default image"}],
    )
    monkeypatch.setattr(
        cli_mod,
        "list_project_images",
        lambda related_project_id, timeout=None: [{"id": 66, "project_repo_hash": "1111111111111111111111111111111111111111"}],
    )

    def _git_run(cmd, capture_output=None, text=None):
        args = cmd[3:]
        if args == ["rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{upstream}"]:
            return types.SimpleNamespace(returncode=0, stdout="origin/main\n", stderr="")
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
        return {"id": 77, "project_repo_hash": kwargs["project_repo_hash"], "base_image": kwargs["base_image_id"]}

    monkeypatch.setattr(cli_mod, "create_project_image", _create_project_image)

    result = runner.invoke(cli_mod.app, ["project", "images", "create"], input="\n\n")
    assert result.exit_code == 0
    assert captured["related_project_id"] == 123
    assert captured["project_repo_hash"] == "1111111111111111111111111111111111111111"
    assert captured["base_image_id"] == 22
    assert "Project image created: id=77" in result.output
    assert "local commit(s) have not been pushed yet" in result.output
    assert "2026-03-14 10:11:12" in result.output
    assert "66" in result.output
    assert "already has project image(s): 66" in result.output


def test_project_create_image_rejects_unpushed_hash(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "demo-123"
    target.mkdir(parents=True, exist_ok=True)
    (target / ".env").write_text("MAIN_SEQUENCE_PROJECT_ID=123\n", encoding="utf-8")

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(cli_mod, "list_project_images", lambda related_project_id, timeout=None: [])

    def _git_run(cmd, capture_output=None, text=None):
        args = cmd[3:]
        if args[:1] == ["log"] and "--not" in args and "--remotes" in args:
            return types.SimpleNamespace(returncode=0, stdout="", stderr="")
        if args[:3] == ["branch", "-r", "--contains"]:
            return types.SimpleNamespace(returncode=0, stdout="", stderr="")
        raise AssertionError(f"Unexpected git command: {cmd}")

    monkeypatch.setattr(cli_mod.subprocess, "run", _git_run)

    result = runner.invoke(
        cli_mod.app,
        ["project", "images", "create", "123", "deadbeef", "--path", str(target)],
    )
    assert result.exit_code == 1
    assert "must reference a commit that has already been pushed" in result.output


def test_project_create_image_polls_until_ready(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "demo-123"
    target.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(cli_mod, "_list_unpushed_commits", lambda *_: [])
    monkeypatch.setattr(cli_mod, "_is_pushed_commit", lambda *_: True)

    list_calls = {"count": 0}

    def _list_project_images(related_project_id, timeout=None):
        list_calls["count"] += 1
        if list_calls["count"] == 1:
            return []
        if list_calls["count"] == 2:
            return [{"id": 77, "project_repo_hash": "abc123", "base_image": 22, "is_ready": False}]
        return [{"id": 77, "project_repo_hash": "abc123", "base_image": 22, "is_ready": True}]

    monkeypatch.setattr(cli_mod, "list_project_images", _list_project_images)
    monkeypatch.setattr(
        cli_mod,
        "create_project_image",
        lambda **kwargs: {"id": 77, "project_repo_hash": kwargs["project_repo_hash"], "base_image": 22, "is_ready": False},
    )

    sleep_calls = []
    monkeypatch.setattr(cli_mod.time, "sleep", lambda secs: sleep_calls.append(secs))

    result = runner.invoke(
        cli_mod.app,
        ["project", "images", "create", "123", "abc123", "--path", str(target), "--base-image-id", "22"],
    )
    assert result.exit_code == 0
    assert sleep_calls == [30, 30]
    assert "Project image is still building." in result.output
    assert "Project image is ready." in result.output
    assert "Is Ready" in result.output
    assert "True" in result.output


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
        lambda project_id: "DEFAULT_BASE_IMAGE=none\nFOO=bar\nMAINSEQUENCE_TOKEN=legacy-token\n",
    )
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_tokens",
        lambda: {"username": "u", "access": "access-123", "refresh": "refresh-456"},
    )
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
    assert "MAINSEQUENCE_ACCESS_TOKEN=access-123" in env_text
    assert "MAINSEQUENCE_REFRESH_TOKEN=refresh-456" in env_text
    assert "TDAG_ENDPOINT=https://backend.test" in env_text
    assert "MAIN_SEQUENCE_PROJECT_ID=123" in env_text
    assert "MAINSEQUENCE_TOKEN=" not in env_text


def test_project_open(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "project"
    target.mkdir(parents=True, exist_ok=True)
    opened = {"path": None}

    monkeypatch.setattr(cli_mod, "open_folder", lambda p: opened.update(path=p))
    result = runner.invoke(cli_mod.app, ["project", "open", "--path", str(target)])
    assert result.exit_code == 0
    assert opened["path"] == str(target.resolve())


def test_project_refresh_token(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "demo-123"
    target.mkdir(parents=True, exist_ok=True)
    env_path = target / ".env"
    env_path.write_text(
        "FOO=bar\n"
        "MAINSEQUENCE_ACCESS_TOKEN=old-access\n"
        "MAINSEQUENCE_REFRESH_TOKEN=old-refresh\n"
        "TDAG_ENDPOINT=https://old-backend.test\n"
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
    assert "TDAG_ENDPOINT=https://backend.test" in env_text
    assert "MAIN_SEQUENCE_PROJECT_ID=123" in env_text
    assert "MAINSEQUENCE_TOKEN=" not in env_text
    assert "old-access" not in env_text
    assert "old-refresh" not in env_text


def test_project_refresh_token_defaults_to_cwd(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "demo-123"
    target.mkdir(parents=True, exist_ok=True)
    env_path = target / ".env"
    env_path.write_text(
        "FOO=bar\n"
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
    assert "TDAG_ENDPOINT=https://backend.test" in env_text
    assert "MAIN_SEQUENCE_PROJECT_ID=123" in env_text


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


def test_project_build_local_venv_defaults_to_cwd_with_env_project_id(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "demo-123"
    target.mkdir(parents=True, exist_ok=True)
    (target / ".env").write_text("MAIN_SEQUENCE_PROJECT_ID=123\n", encoding="utf-8")
    (target / "pyproject.toml").write_text(
        '[project]\nname = "demo"\nrequires-python = ">=3.11,<3.13"\n',
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
    assert calls[0][0] == ["uv", "venv", ".venv", "--python", "3.11"]
    assert calls[0][1] == str(target.resolve())
    assert calls[1][0] == ["uv", "sync"]
    assert calls[1][2]["UV_PROJECT_ENVIRONMENT"] == ".venv"


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
    post_sync = {"called": False}

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
    monkeypatch.setattr(cli_mod, "sync_project_after_commit", lambda project_id: post_sync.update(called=True))

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
    assert post_sync["called"] is False


def test_project_sync_defaults_to_cwd_with_positional_message(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "project"
    target.mkdir(parents=True, exist_ok=True)
    (target / ".env").write_text("MAIN_SEQUENCE_PROJECT_ID=123\n", encoding="utf-8")
    key = tmp_path / "id_ed25519"
    uv_path = target / ".venv" / "bin" / "uv"
    git_calls = []

    monkeypatch.chdir(target)
    monkeypatch.setattr(cli_mod, "ensure_venv", lambda *_: None)
    monkeypatch.setattr(cli_mod, "git_origin", lambda *_: "git@github.com:org/repo.git")
    monkeypatch.setattr(cli_mod, "ensure_key_for_repo", lambda *_: (key, key.with_suffix(".pub"), "pub"))
    monkeypatch.setattr(cli_mod, "ensure_uv_installed", lambda *_: uv_path)
    monkeypatch.setattr(cli_mod, "run_uv", lambda uv, args, cwd, env=None: None)
    monkeypatch.setattr(cli_mod, "uv_export_requirements", lambda uv, cwd, **kwargs: None)
    monkeypatch.setattr(
        cli_mod,
        "run_cmd",
        lambda cmd, cwd, env=None: git_calls.append((cmd, pathlib.Path(cwd))),
    )

    result = runner.invoke(
        cli_mod.app,
        ["project", "sync", "Update deps", "--no-push"],
    )
    assert result.exit_code == 0
    assert (["git", "commit", "-m", "Update deps"], target) in git_calls
    assert all(cwd == target for _, cwd in git_calls)


def test_project_sync_triggers_backend_sync_after_push(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "project"
    target.mkdir(parents=True, exist_ok=True)
    (target / ".env").write_text("MAIN_SEQUENCE_PROJECT_ID=123\n", encoding="utf-8")
    key = tmp_path / "id_ed25519"
    uv_path = target / ".venv" / "bin" / "uv"
    post_sync = {}

    monkeypatch.chdir(target)
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(cli_mod, "ensure_venv", lambda *_: None)
    monkeypatch.setattr(cli_mod, "git_origin", lambda *_: "git@github.com:org/repo.git")
    monkeypatch.setattr(cli_mod, "ensure_key_for_repo", lambda *_: (key, key.with_suffix(".pub"), "pub"))
    monkeypatch.setattr(cli_mod, "ensure_uv_installed", lambda *_: uv_path)
    monkeypatch.setattr(cli_mod, "run_uv", lambda uv, args, cwd, env=None: None)
    monkeypatch.setattr(cli_mod, "uv_export_requirements", lambda uv, cwd, **kwargs: None)
    monkeypatch.setattr(cli_mod, "run_cmd", lambda cmd, cwd, env=None: None)
    monkeypatch.setattr(
        cli_mod,
        "sync_project_after_commit",
        lambda project_id: post_sync.update(project_id=project_id) or {"id": project_id},
    )

    result = runner.invoke(cli_mod.app, ["project", "sync", "Update deps"])
    assert result.exit_code == 0
    assert post_sync["project_id"] == 123
    assert "Triggered backend sync for project 123." in result.output


def test_project_sync_project(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "project"
    target.mkdir(parents=True, exist_ok=True)
    (target / ".env").write_text("MAIN_SEQUENCE_PROJECT_ID=123\n", encoding="utf-8")
    key = tmp_path / "id_ed25519"
    uv_path = target / ".venv" / "bin" / "uv"
    uv_calls = []
    export_calls = []
    git_calls = []
    post_sync = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
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
    monkeypatch.setattr(
        cli_mod,
        "sync_project_after_commit",
        lambda project_id: post_sync.update(project_id=project_id) or {"id": project_id},
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
    assert post_sync["project_id"] == 123


def test_project_sync_project_defaults_to_current_project_dir(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "project"
    target.mkdir(parents=True, exist_ok=True)
    (target / ".env").write_text("MAIN_SEQUENCE_PROJECT_ID=123\n", encoding="utf-8")
    key = tmp_path / "id_ed25519"
    uv_path = target / ".venv" / "bin" / "uv"
    seen = {"cwd": []}

    monkeypatch.chdir(target)
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(cli_mod, "ensure_venv", lambda *_: None)
    monkeypatch.setattr(cli_mod, "git_origin", lambda *_: "git@github.com:org/repo.git")
    monkeypatch.setattr(cli_mod, "ensure_key_for_repo", lambda *_: (key, key.with_suffix(".pub"), "pub"))
    monkeypatch.setattr(cli_mod, "ensure_uv_installed", lambda *_: uv_path)
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
    monkeypatch.setattr(cli_mod, "sync_project_after_commit", lambda project_id: {"id": project_id})

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
