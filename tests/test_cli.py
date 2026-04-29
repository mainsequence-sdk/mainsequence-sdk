from __future__ import annotations

import importlib
import json
import os
import pathlib
import sys
import types

import pytest
import yaml
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


def test_root_version(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_package_version", lambda: "3.18.9")

    result = runner.invoke(cli_mod.app, ["--version"])
    assert result.exit_code == 0
    assert result.output.strip() == "mainsequence 3.18.9"


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


def test_user_show_json(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(
        cli_mod,
        "get_logged_user_details",
        lambda: {
            "id": 7,
            "username": "jose",
            "email": "jose@main-sequence.io",
            "organization": {"id": 2, "name": "Main Sequence"},
        },
    )

    result = runner.invoke(cli_mod.app, ["user", "--json"])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["id"] == 7
    assert payload["username"] == "jose"
    assert payload["organization"]["name"] == "Main Sequence"


def test_skills_list(cli_mod, runner, monkeypatch, tmp_path):
    bundle_dir = tmp_path / "agent_scaffold"
    (bundle_dir / "skills" / "project_builder").mkdir(parents=True)
    (bundle_dir / "skills" / "project_builder" / "SKILL.md").write_text("project builder", encoding="utf-8")
    (bundle_dir / "skills" / "command_center" / "workspace_builder").mkdir(parents=True)
    (bundle_dir / "skills" / "command_center" / "workspace_builder" / "SKILL.md").write_text("workspace builder", encoding="utf-8")

    monkeypatch.setattr(cli_mod, "_installed_agent_scaffold_bundle_dir", lambda: bundle_dir)

    result = runner.invoke(cli_mod.app, ["skills", "list"])
    assert result.exit_code == 0
    assert "project_builder" in result.output
    assert "command_center/workspace_builder" in result.output


def test_skills_path(cli_mod, runner, monkeypatch, tmp_path):
    bundle_dir = tmp_path / "agent_scaffold"
    (bundle_dir / "skills" / "command_center" / "workspace_builder").mkdir(parents=True)
    expected = bundle_dir / "skills" / "command_center" / "workspace_builder" / "SKILL.md"
    expected.write_text("workspace builder", encoding="utf-8")

    monkeypatch.setattr(cli_mod, "_installed_agent_scaffold_bundle_dir", lambda: bundle_dir)

    result = runner.invoke(cli_mod.app, ["skills", "path", "command_center/workspace_builder"])
    assert result.exit_code == 0
    assert result.output.strip() == str(expected)


def test_skills_path_bundle_root(cli_mod, runner, monkeypatch, tmp_path):
    bundle_dir = tmp_path / "agent_scaffold"
    (bundle_dir / "skills").mkdir(parents=True)

    monkeypatch.setattr(cli_mod, "_installed_agent_scaffold_bundle_dir", lambda: bundle_dir)

    result = runner.invoke(cli_mod.app, ["skills", "path"])
    assert result.exit_code == 0
    assert result.output.strip() == str(bundle_dir / "skills")


def test_skills_path_unique_leaf_name(cli_mod, runner, monkeypatch, tmp_path):
    bundle_dir = tmp_path / "agent_scaffold"
    (bundle_dir / "skills" / "command_center" / "workspace_builder").mkdir(parents=True)
    expected = bundle_dir / "skills" / "command_center" / "workspace_builder" / "SKILL.md"
    expected.write_text("workspace builder", encoding="utf-8")

    monkeypatch.setattr(cli_mod, "_installed_agent_scaffold_bundle_dir", lambda: bundle_dir)

    result = runner.invoke(cli_mod.app, ["skills", "path", "workspace_builder"])
    assert result.exit_code == 0
    assert result.output.strip() == str(expected)


def test_organization_project_names(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_org_project_names",
        lambda timeout=None: ["alpha-research", "portfolio-live"],
    )

    result = runner.invoke(cli_mod.app, ["organization", "project-names"])
    assert result.exit_code == 0
    assert "Project Name" in result.output
    assert "alpha-research" in result.output
    assert "portfolio-live" in result.output
    assert "Total organization-visible project names: 2" in result.output


def test_organization_github_organizations(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_github_organizations",
        lambda: [
            {"id": 33, "name": "Main Sequence Projects", "login": "mainsequence-projects"},
            {"id": 34, "slug": "research-labs"},
        ],
    )

    result = runner.invoke(cli_mod.app, ["organization", "github-organizations"])
    assert result.exit_code == 0
    assert "GitHub Organizations" in result.output


def test_cc_workspace_snapshot_prints_resolved_output_path(cli_mod, runner, monkeypatch, tmp_path):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    snapshot_module = types.ModuleType("mainsequence.client.command_center.workspace_snapshot")
    extracted_dir = pathlib.Path("/tmp/workspace-15-20260429T120000Z-snapshot")

    snapshot_module._WorkspaceSnapshotError = RuntimeError
    snapshot_module._resolve_command_center_url = lambda: "http://localhost:5173"
    snapshot_module._build_snapshot_url = (
        lambda base_url, workspace_id: f"{base_url}/app/workspace-studio/workspaces?workspace={workspace_id}&snapshot=true&snapshotProfile=full-data"
    )
    snapshot_module._capture_workspace_snapshot = (
        lambda workspace_id, output_path=None: (b"zip-bytes", extracted_dir)
    )

    monkeypatch.setitem(sys.modules, "mainsequence.client.command_center.workspace_snapshot", snapshot_module)

    result = runner.invoke(cli_mod.app, ["cc", "workspace", "snapshot", "15"])
    assert result.exit_code == 0
    assert str(extracted_dir) in result.output
    assert "<timestamp>" not in result.output


def test_organization_github_organizations_json(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_github_organizations",
        lambda: [{"id": 33, "name": "Main Sequence Projects", "login": "mainsequence-projects"}],
    )

    result = runner.invoke(cli_mod.app, ["organization", "github-organizations", "--json"])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload == [{"id": 33, "name": "Main Sequence Projects", "login": "mainsequence-projects"}]


def test_organization_teams_list(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_organization_teams",
        lambda timeout=None, filters=None: [
            {
                "id": 9,
                "name": "Research",
                "description": "Model validation",
                "member_count": 4,
                "is_active": True,
            }
        ],
    )

    result = runner.invoke(cli_mod.app, ["organization", "teams", "list"])
    assert result.exit_code == 0
    assert "Organization Teams" in result.output
    assert "Research" in result.output
    assert "Model validation" in result.output
    assert "Total organization teams: 1" in result.output


def test_organization_teams_create(cli_mod, runner, monkeypatch):
    captured = {}
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _create(*, name, description="", timeout=None):
        captured["name"] = name
        captured["description"] = description
        captured["timeout"] = timeout
        return {"id": 9, "name": name, "description": description, "member_count": 0, "is_active": True}

    monkeypatch.setattr(cli_mod, "create_organization_team", _create)

    result = runner.invoke(
        cli_mod.app,
        ["organization", "teams", "create", "Research", "--description", "Model validation"],
    )
    assert result.exit_code == 0
    assert captured == {"name": "Research", "description": "Model validation", "timeout": None}
    assert "Organization team created: Research" in result.output


def test_organization_teams_edit(cli_mod, runner, monkeypatch):
    captured = {}
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "get_organization_team",
        lambda team_id, timeout=None: {
            "id": team_id,
            "name": "Research",
            "description": "Old description",
            "member_count": 4,
            "is_active": True,
        },
    )

    def _update(team_id, *, name=None, description=None, is_active=None, timeout=None):
        captured["team_id"] = team_id
        captured["name"] = name
        captured["description"] = description
        captured["is_active"] = is_active
        captured["timeout"] = timeout
        return {
            "id": team_id,
            "name": name or "Research",
            "description": description or "Old description",
            "member_count": 4,
            "is_active": is_active,
        }

    monkeypatch.setattr(cli_mod, "update_organization_team", _update)

    result = runner.invoke(
        cli_mod.app,
        ["organization", "teams", "edit", "9", "--name", "Research Core", "--inactive"],
    )
    assert result.exit_code == 0
    assert captured == {
        "team_id": 9,
        "name": "Research Core",
        "description": None,
        "is_active": False,
        "timeout": None,
    }
    assert "Organization team updated: id=9" in result.output


def test_organization_teams_delete(cli_mod, runner, monkeypatch):
    captured = {}
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "get_organization_team",
        lambda team_id, timeout=None: {
            "id": team_id,
            "name": "Research",
            "description": "Model validation",
            "member_count": 4,
            "is_active": True,
        },
    )
    monkeypatch.setattr(cli_mod, "_require_delete_verification", lambda **kwargs: None)

    def _delete(team_id, *, timeout=None):
        captured["team_id"] = team_id
        captured["timeout"] = timeout
        return {
            "id": team_id,
            "name": "Research",
            "description": "Model validation",
            "member_count": 4,
            "is_active": True,
        }

    monkeypatch.setattr(cli_mod, "delete_organization_team", _delete)

    result = runner.invoke(cli_mod.app, ["organization", "teams", "delete", "9"])
    assert result.exit_code == 0
    assert captured == {"team_id": 9, "timeout": None}
    assert "Organization team deleted: id=9" in result.output


def test_list_organization_teams_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    class FakeTeam:
        def __init__(self, team_id, name):
            self.id = team_id
            self.name = name

        def model_dump(self, mode="json"):
            return {"id": self.id, "name": self.name}

    def _run_sdk_model_operation(*, module_name, class_name, operation, project_id_env=None):
        captured["module_name"] = module_name
        captured["class_name"] = class_name

        class _ClientTeam:
            @classmethod
            def filter(cls, timeout=None, **kwargs):
                captured["timeout"] = timeout
                captured["filters"] = kwargs
                return [FakeTeam(9, "Research")]

        return operation(_ClientTeam)

    monkeypatch.setattr(api_mod, "_run_sdk_model_operation", _run_sdk_model_operation)

    out = api_mod.list_organization_teams(timeout=9, filters={"name__contains": "Res"})
    assert captured == {
        "module_name": "mainsequence.client.models_user",
        "class_name": "Team",
        "timeout": 9,
        "filters": {"name__contains": "Res"},
    }
    assert out == [{"id": 9, "name": "Research"}]


def test_list_simple_table_storages_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    class FakeStorage:
        def __init__(self, storage_id, source_class_name):
            self.id = storage_id
            self.source_class_name = source_class_name

        def model_dump(self, mode="json"):
            return {"id": self.id, "source_class_name": self.source_class_name}

    def _run_sdk_model_operation(*, module_name, class_name, operation, project_id_env=None):
        captured["module_name"] = module_name
        captured["class_name"] = class_name

        class _ClientSimpleTableStorage:
            @classmethod
            def filter(cls, timeout=None, **kwargs):
                captured["timeout"] = timeout
                captured["filters"] = kwargs
                return [FakeStorage(41, "OrdersTable")]

        return operation(_ClientSimpleTableStorage)

    monkeypatch.setattr(api_mod, "_run_sdk_model_operation", _run_sdk_model_operation)

    out = api_mod.list_simple_table_storages(timeout=5, filters={"id": 41})
    assert captured == {
        "module_name": "mainsequence.client.models_simple_tables",
        "class_name": "SimpleTableStorage",
        "timeout": 5,
        "filters": {"id": 41},
    }
    assert out == [{"id": 41, "source_class_name": "OrdersTable"}]


def test_simple_tables_list(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_simple_table_storages",
        lambda timeout=None, filters=None: [
            {
                "id": 41,
                "source_class_name": "OrdersTable",
                "namespace": "pytest_orders",
                "data_source": {"display_name": "Default DB", "class_type": "timescale_db"},
                "columns": [{"column_name": "id"}, {"column_name": "symbol"}],
                "open_for_everyone": False,
                "creation_date": "2026-03-24T09:00:00Z",
            }
        ],
    )

    result = runner.invoke(cli_mod.app, ["simple_table", "list"])
    assert result.exit_code == 0
    assert "Simple Tables" in result.output
    assert "OrdersTable" in result.output
    assert "Namespace" in result.output
    assert "pytest_orde" in result.output
    assert "Default DB" in result.output
    assert "timescale_" in result.output
    assert "Total simple tables: 1" in result.output


def test_simple_tables_list_forwards_namespace_filter(cli_mod, runner, monkeypatch):
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _fake_list_simple_table_storages(timeout=None, filters=None):
        captured["timeout"] = timeout
        captured["filters"] = filters
        return []

    monkeypatch.setattr(cli_mod, "list_simple_table_storages", _fake_list_simple_table_storages)

    result = runner.invoke(
        cli_mod.app,
        ["simple_table", "list", "--filter", "namespace=pytest_orders"],
    )

    assert result.exit_code == 0
    assert captured == {"timeout": None, "filters": {"namespace": "pytest_orders"}}


def test_simple_tables_detail(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "get_simple_table_storage",
        lambda storage_id, timeout=None: {
            "id": storage_id,
            "source_class_name": "OrdersTable",
            "data_source": {"display_name": "Default DB", "class_type": "timescale_db"},
            "columns": [{"column_name": "id"}, {"column_name": "symbol"}],
            "foreign_keys": [],
            "incoming_fks": [],
            "indexes_meta": [{"name": "orders_symbol_idx"}],
            "schema": {"name": "orders"},
            "build_configuration": {"mode": "append"},
            "open_for_everyone": False,
            "creation_date": "2026-03-24T09:00:00Z",
        },
    )

    result = runner.invoke(cli_mod.app, ["simple_table", "detail", "41"])
    assert result.exit_code == 0
    assert "Simple Table" in result.output
    assert "OrdersTable" in result.output
    assert "orders_symbol_idx" in result.output


def test_simple_tables_delete(cli_mod, runner, monkeypatch):
    captured = {}
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "get_simple_table_storage",
        lambda storage_id, timeout=None: {
            "id": storage_id,
            "source_class_name": "OrdersTable",
            "columns": [{"column_name": "id"}],
            "foreign_keys": [],
            "incoming_fks": [],
            "indexes_meta": [],
            "open_for_everyone": False,
            "creation_date": "2026-03-24T09:00:00Z",
        },
    )
    monkeypatch.setattr(cli_mod, "_require_delete_verification", lambda **kwargs: None)

    def _delete(storage_id, *, timeout=None):
        captured["storage_id"] = storage_id
        captured["timeout"] = timeout
        return {
            "id": storage_id,
            "source_class_name": "OrdersTable",
            "columns": [{"column_name": "id"}],
            "foreign_keys": [],
            "incoming_fks": [],
            "indexes_meta": [],
            "open_for_everyone": False,
            "creation_date": "2026-03-24T09:00:00Z",
        }

    monkeypatch.setattr(cli_mod, "delete_simple_table_storage", _delete)

    result = runner.invoke(cli_mod.app, ["simple_table", "delete", "41"])
    assert result.exit_code == 0
    assert captured == {"storage_id": 41, "timeout": None}
    assert "Simple table deleted: id=41" in result.output


def test_simple_tables_add_label(cli_mod, runner, monkeypatch):
    captured = {}
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _add(storage_id, labels, timeout=None):
        captured["storage_id"] = storage_id
        captured["labels"] = labels
        captured["timeout"] = timeout
        return {"labels": [{"name": "reference-data"}, {"name": "curated"}]}

    monkeypatch.setattr(cli_mod, "add_simple_table_storage_labels", _add)

    result = runner.invoke(
        cli_mod.app,
        ["simple_table", "add-label", "41", "--label", "reference-data", "--label", "curated"],
    )
    assert result.exit_code == 0
    assert captured == {"storage_id": 41, "labels": ["reference-data", "curated"], "timeout": None}
    assert "Simple Table add-label completed." in result.output
    assert "reference-data, curated" in result.output


def test_list_workspaces_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    class FakeWorkspace:
        def __init__(self, workspace_id, title):
            self.id = workspace_id
            self.title = title

        def model_dump(self, mode="json"):
            return {"id": self.id, "title": self.title}

    def _run_sdk_model_operation(*, module_name, class_name, operation, project_id_env=None):
        captured["module_name"] = module_name
        captured["class_name"] = class_name

        class _ClientWorkspace:
            @classmethod
            def filter(cls, timeout=None, **kwargs):
                captured["timeout"] = timeout
                captured["filters"] = kwargs
                return [FakeWorkspace(7, "Rates Desk")]

        return operation(_ClientWorkspace)

    monkeypatch.setattr(api_mod, "_run_sdk_model_operation", _run_sdk_model_operation)

    out = api_mod.list_workspaces(timeout=8, filters={"title__contains": "Rates"})
    assert captured == {
        "module_name": "mainsequence.client.command_center",
        "class_name": "Workspace",
        "timeout": 8,
        "filters": {"title__contains": "Rates"},
    }
    assert out == [{"id": 7, "title": "Rates Desk"}]


def test_update_workspace_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    class FakeWorkspace:
        def __init__(self, workspace_id):
            self.id = workspace_id

        def patch(self, **kwargs):
            captured["patch_kwargs"] = kwargs

            class _UpdatedWorkspace:
                id = 7

                @staticmethod
                def model_dump(mode="json"):
                    return {
                        "id": 7,
                        "title": kwargs.get("title", "Rates Desk"),
                        "layoutKind": kwargs.get("layoutKind", "custom"),
                    }

            return _UpdatedWorkspace()

    def _run_sdk_model_operation(*, module_name, class_name, operation, project_id_env=None):
        captured["module_name"] = module_name
        captured["class_name"] = class_name

        class _ClientWorkspace:
            @classmethod
            def get(cls, pk=None, timeout=None):
                captured["pk"] = pk
                captured["timeout"] = timeout
                return FakeWorkspace(pk)

        return operation(_ClientWorkspace)

    monkeypatch.setattr(api_mod, "_run_sdk_model_operation", _run_sdk_model_operation)

    out = api_mod.update_workspace(
        7,
        title="Updated Rates Desk",
        layout_kind="auto-grid",
        widgets=[{"id": "widget-1", "widgetId": "markdown-note"}],
        timeout=11,
    )
    assert captured == {
        "module_name": "mainsequence.client.command_center",
        "class_name": "Workspace",
        "pk": 7,
        "timeout": 11,
        "patch_kwargs": {
            "title": "Updated Rates Desk",
            "layoutKind": "auto-grid",
            "widgets": [{"id": "widget-1", "widgetId": "markdown-note"}],
        },
    }
    assert out == {"id": 7, "title": "Updated Rates Desk", "layoutKind": "auto-grid"}


def test_add_workspace_labels_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    class FakeWorkspace:
        def add_label(self, labels, timeout=None):
            captured["labels"] = labels
            captured["add_timeout"] = timeout

            class _Payload:
                @staticmethod
                def model_dump(mode="json"):
                    return {"labels": [{"name": "rates"}, {"name": "desk"}]}

            return _Payload()

    def _run_sdk_model_operation(*, module_name, class_name, operation, project_id_env=None):
        captured["module_name"] = module_name
        captured["class_name"] = class_name

        class _ClientWorkspace:
            @classmethod
            def get(cls, pk=None, timeout=None):
                captured["pk"] = pk
                captured["get_timeout"] = timeout
                return FakeWorkspace()

        return operation(_ClientWorkspace)

    monkeypatch.setattr(api_mod, "_run_sdk_model_operation", _run_sdk_model_operation)

    out = api_mod.add_workspace_labels(7, ["rates", "desk"], timeout=12)
    assert captured == {
        "module_name": "mainsequence.client.command_center",
        "class_name": "Workspace",
        "pk": 7,
        "get_timeout": 12,
        "labels": ["rates", "desk"],
        "add_timeout": 12,
    }
    assert out == {"labels": [{"name": "rates"}, {"name": "desk"}]}


def test_workspace_list(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_workspaces",
        lambda timeout=None, filters=None: [
            {
                "id": 7,
                "title": "Rates Desk",
                "category": "Trading",
                "source": "user",
                "layoutKind": "custom",
                "labels": ["rates", "desk"],
                "updatedAt": "2026-04-04T10:30:00Z",
            }
        ],
    )

    result = runner.invoke(cli_mod.app, ["cc", "workspace", "list"])
    assert result.exit_code == 0
    assert "Workspaces" in result.output
    assert "Rates Desk" in result.output
    assert "Total workspaces: 1" in result.output


def test_workspace_detail(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "get_workspace",
        lambda workspace_id, timeout=None: {
            "id": workspace_id,
            "title": "Rates Desk",
            "description": "Shared workspace",
            "category": "Trading",
            "source": "user",
            "schemaVersion": 1,
            "requiredPermissions": ["dashboard:view"],
            "grid": {"columns": 12},
            "layoutKind": "custom",
            "autoGrid": {},
            "companions": [],
            "controls": {"refresh": {"enabled": True}},
            "widgets": [{"id": "widget-1", "widgetId": "markdown-note"}],
            "updatedAt": "2026-04-04T10:30:00Z",
        },
    )

    result = runner.invoke(cli_mod.app, ["cc", "workspace", "detail", "7"])
    assert result.exit_code == 0
    assert "Workspace Details" in result.output
    assert "markdown-note" in result.output
    assert "dashboard:view" in result.output


def test_workspace_create(cli_mod, runner, monkeypatch):
    captured = {}
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _create(**kwargs):
        captured.update(kwargs)
        return {
            "id": 7,
            "title": kwargs["title"],
            "description": kwargs["description"],
            "category": kwargs["category"],
            "source": kwargs["source"],
            "layoutKind": kwargs["layout_kind"],
            "labels": kwargs["labels"],
            "updatedAt": "2026-04-04T10:30:00Z",
        }

    monkeypatch.setattr(cli_mod, "create_workspace", _create)

    result = runner.invoke(
        cli_mod.app,
        [
            "cc",
            "workspace",
            "create",
            "Rates Desk",
            "--description",
            "Shared workspace",
            "--label",
            "rates,desk",
            "--category",
            "Trading",
        ],
    )
    assert result.exit_code == 0
    assert captured["title"] == "Rates Desk"
    assert captured["description"] == "Shared workspace"
    assert captured["labels"] == ["rates", "desk"]
    assert captured["category"] == "Trading"
    assert "Workspace created: Rates Desk" in result.output


def test_workspace_create_from_file(cli_mod, runner, monkeypatch, tmp_path):
    captured = {}
    workspace_file = tmp_path / "workspace.yaml"
    workspace_file.write_text(
        yaml.safe_dump(
            {
                "title": "Rates Desk",
                "description": "Shared workspace",
                "labels": ["rates", "desk"],
                "category": "Trading",
                "source": "user",
                "schemaVersion": 1,
                "requiredPermissions": ["dashboard:view"],
                "grid": {"columns": 12},
                "layoutKind": "custom",
                "autoGrid": {},
                "companions": [],
                "controls": {"refresh": {"enabled": True}},
                "widgets": [{"id": "widget-1", "widgetId": "markdown-note"}],
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _create(**kwargs):
        captured.update(kwargs)
        return {
            "id": 7,
            "title": kwargs["title"],
            "description": kwargs["description"],
            "category": kwargs["category"],
            "source": kwargs["source"],
            "layoutKind": kwargs["layout_kind"],
            "labels": kwargs["labels"],
            "updatedAt": "2026-04-04T10:30:00Z",
        }

    monkeypatch.setattr(cli_mod, "create_workspace", _create)

    result = runner.invoke(
        cli_mod.app,
        ["cc", "workspace", "create", "--file", str(workspace_file)],
    )
    assert result.exit_code == 0
    assert captured["schema_version"] == 1
    assert captured["required_permissions"] == ["dashboard:view"]
    assert captured["controls"] == {"refresh": {"enabled": True}}
    assert captured["widgets"] == [{"id": "widget-1", "widgetId": "markdown-note"}]
    assert "Workspace created: Rates Desk" in result.output


def test_workspace_update_from_file(cli_mod, runner, monkeypatch, tmp_path):
    captured = {}
    workspace_file = tmp_path / "workspace-update.yaml"
    workspace_file.write_text(
        yaml.safe_dump(
            {
                "description": "Updated workspace",
                "layoutKind": "auto-grid",
                "controls": {"refresh": {"selectedIntervalMs": 60000}},
                "widgets": [{"id": "widget-1", "widgetId": "markdown-note"}],
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _update(workspace_id, **kwargs):
        captured["workspace_id"] = workspace_id
        captured.update(kwargs)
        return {
            "id": workspace_id,
            "title": "Rates Desk",
            "description": "Updated workspace",
            "category": "Trading",
            "source": "user",
            "schemaVersion": 1,
            "layoutKind": "auto-grid",
            "controls": {"refresh": {"selectedIntervalMs": 60000}},
            "widgets": [{"id": "widget-1", "widgetId": "markdown-note"}],
            "updatedAt": "2026-04-04T10:45:00Z",
        }

    monkeypatch.setattr(cli_mod, "update_workspace", _update)

    result = runner.invoke(
        cli_mod.app,
        ["cc", "workspace", "update", "7", "--file", str(workspace_file)],
    )
    assert result.exit_code == 0
    assert captured["workspace_id"] == 7
    assert captured["layout_kind"] == "auto-grid"
    assert captured["controls"] == {"refresh": {"selectedIntervalMs": 60000}}
    assert captured["widgets"] == [{"id": "widget-1", "widgetId": "markdown-note"}]
    assert "Workspace updated: id=7" in result.output


def test_workspace_delete(cli_mod, runner, monkeypatch):
    captured = {}
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "get_workspace",
        lambda workspace_id, timeout=None: {
            "id": workspace_id,
            "title": "Rates Desk",
            "description": "Shared workspace",
            "category": "Trading",
            "source": "user",
            "layoutKind": "custom",
            "labels": ["rates", "desk"],
            "updatedAt": "2026-04-04T10:30:00Z",
        },
    )
    monkeypatch.setattr(cli_mod, "_require_delete_verification", lambda **kwargs: None)

    def _delete(workspace_id, *, timeout=None):
        captured["workspace_id"] = workspace_id
        captured["timeout"] = timeout
        return {
            "id": workspace_id,
            "title": "Rates Desk",
            "description": "Shared workspace",
            "category": "Trading",
            "source": "user",
            "layoutKind": "custom",
            "labels": ["rates", "desk"],
            "updatedAt": "2026-04-04T10:30:00Z",
        }

    monkeypatch.setattr(cli_mod, "delete_workspace", _delete)

    result = runner.invoke(cli_mod.app, ["cc", "workspace", "delete", "7"])
    assert result.exit_code == 0
    assert captured == {"workspace_id": 7, "timeout": None}
    assert "Workspace deleted: id=7" in result.output


def test_workspace_add_label(cli_mod, runner, monkeypatch):
    captured = {}
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _add(workspace_id, labels, timeout=None):
        captured["workspace_id"] = workspace_id
        captured["labels"] = labels
        captured["timeout"] = timeout
        return {"labels": [{"name": "trading"}, {"name": "desk"}]}

    monkeypatch.setattr(cli_mod, "add_workspace_labels", _add)

    result = runner.invoke(
        cli_mod.app,
        ["cc", "workspace", "add-label", "7", "--label", "trading,desk"],
    )
    assert result.exit_code == 0
    assert captured == {"workspace_id": 7, "labels": ["trading", "desk"], "timeout": None}
    assert "Workspace add-label completed." in result.output
    assert "trading, desk" in result.output


def test_registered_widget_type_list(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_registered_widget_types",
        lambda timeout=None, filters=None: [
            {
                "id": 17,
                "widget_id": "main-sequence-data-node",
                "title": "Data Node",
                "category": "Main Sequence",
                "kind": "custom",
                "source": "main-sequence",
                "is_active": True,
                "registry_version": "2026.04.04",
            }
        ],
    )

    result = runner.invoke(cli_mod.app, ["cc", "registered_widget_type", "list"])
    assert result.exit_code == 0
    assert "Registered Widget Types" in result.output
    assert "17" in result.output
    assert "main-sequ" in result.output
    assert "uence-da" in result.output
    assert "-node" in result.output
    assert "Total registered widget types: 1" in result.output


def test_registered_widget_type_detail(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "get_registered_widget_type",
        lambda widget_id, timeout=None: {
            "id": 17,
            "widget_id": widget_id,
            "title": "Data Node",
            "description": "Renders a data node payload.",
            "category": "Main Sequence",
            "widget_version": "1.2.3",
            "kind": "custom",
            "source": "main-sequence",
            "is_active": True,
            "registry_version": "2026.04.04",
            "required_permissions": ["workspace:view"],
            "schema_payload": {"type": "object"},
            "io": {"outputs": ["selectedDataNode"]},
            "default_presentation": {"chrome": "card"},
            "default_size": {"w": 6, "h": 4},
            "responsive": {"sm": {"w": 12}},
            "usage_guidance": {"summary": "Use this to select a data node."},
            "capabilities": {"publishes": ["dataNodeId"]},
            "examples": [{"props": {"nodeId": 1}}],
            "descriptor": {"ui": "card"},
            "checksum": "abc123",
            "last_synced_at": "2026-04-04T10:00:00Z",
            "created_at": "2026-04-04T10:00:00Z",
            "updated_at": "2026-04-04T10:30:00Z",
        },
    )

    result = runner.invoke(
        cli_mod.app,
        ["cc", "registered_widget_type", "detail", "main-sequence-data-node"],
    )
    assert result.exit_code == 0
    assert "Registered Widget Type" in result.output
    assert "17" in result.output
    assert "main-sequence-data-node" in result.output
    assert "Renders a data node payload." in result.output
    assert "Schema" in result.output
    assert "Default Presentation" in result.output
    assert "Default Size" in result.output
    assert "Usage Guidance" in result.output
    assert "Capabilities" in result.output
    assert "Examples" in result.output
    assert "Extra Fields" in result.output
    assert "descriptor" in result.output
    assert "workspace:view" in result.output


def test_connection_type_list(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_connection_types",
        lambda timeout=None, filters=None: [
            {
                "type_id": "postgres",
                "type_version": 3,
                "title": "Postgres",
                "category": "database",
                "source": "main-sequence",
                "access_mode": "proxy",
            }
        ],
    )

    result = runner.invoke(cli_mod.app, ["cc", "connection_type", "list"])
    assert result.exit_code == 0
    assert "Connection Types" in result.output
    assert "postgres" in result.output
    assert "database" in result.output
    assert "Total connection types: 1" in result.output


def test_connection_type_detail(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "get_connection_type",
        lambda type_id, timeout=None: {
            "type_id": type_id,
            "type_version": 3,
            "title": "Postgres",
            "description": "Managed Postgres connection type.",
            "source": "main-sequence",
            "category": "database",
            "tags": ["sql", "warehouse"],
            "capabilities": ["query", "healthcheck"],
            "access_mode": "proxy",
            "public_config_schema": {"type": "object", "properties": {"host": {"type": "string"}}},
            "secure_config_schema": {"type": "object", "properties": {"password": {"type": "string"}}},
            "query_models": [{"id": "sql"}],
            "required_permissions": ["connections:view"],
            "usage_guidance": "Use for warehouse-backed SQL access.",
            "examples": [{"publicConfig": {"host": "db.internal"}}],
        },
    )

    result = runner.invoke(cli_mod.app, ["cc", "connection_type", "detail", "postgres"])
    assert result.exit_code == 0
    assert "Connection Type" in result.output
    assert "Managed Postgres connection type." in result.output
    assert "Public Config Schema" in result.output
    assert "Secure Config Schema" in result.output
    assert "Use for warehouse-backed SQL access." in result.output
    assert "connections:view" in result.output


def test_connection_list(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_connection_instances",
        lambda timeout=None, filters=None: [
            {
                "id": "conn-17",
                "uid": "warehouse-primary",
                "name": "Warehouse Primary",
                "type_id": "postgres",
                "status": "ok",
                "workspace_id": "7",
                "is_default": True,
                "is_system": False,
            }
        ],
    )

    result = runner.invoke(cli_mod.app, ["cc", "connection", "list"])
    assert result.exit_code == 0
    assert "Connections" in result.output
    assert "warehouse-" in result.output
    assert "primary" in result.output
    assert "Warehouse" in result.output
    assert "Primary" in result.output
    assert "Total connections: 1" in result.output


def test_connection_detail(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "get_connection_instance",
        lambda connection_uid, timeout=None: {
            "id": "conn-17",
            "uid": connection_uid,
            "type_id": "postgres",
            "type_version": 3,
            "name": "Warehouse Primary",
            "description": "Primary warehouse connection.",
            "organization_id": "2",
            "workspace_id": "7",
            "public_config": {"host": "db.internal", "database": "analytics"},
            "secure_fields": {"password": True},
            "status": "ok",
            "status_message": "Healthy.",
            "last_health_check_at": "2026-04-26T10:00:00Z",
            "is_default": True,
            "is_system": False,
            "tags": ["warehouse"],
            "created_by": "7",
            "created_at": "2026-04-26T09:00:00Z",
            "updated_at": "2026-04-26T10:05:00Z",
        },
    )

    result = runner.invoke(cli_mod.app, ["cc", "connection", "detail", "warehouse-primary"])
    assert result.exit_code == 0
    assert "Connection" in result.output
    assert "Primary warehouse connection." in result.output
    assert "Public Config" in result.output
    assert "Secure Fields" in result.output
    assert "Healthy." in result.output
    assert "warehouse-primary" in result.output


def test_list_agents_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    class FakeAgent:
        def __init__(self, agent_id, name):
            self.id = agent_id
            self.name = name

        def model_dump(self, mode="json"):
            return {"id": self.id, "name": self.name, "status": "draft"}

    def _run_sdk_model_operation(*, module_name, class_name, operation, project_id_env=None):
        captured["module_name"] = module_name
        captured["class_name"] = class_name

        class _ClientAgent:
            @classmethod
            def filter(cls, timeout=None, **kwargs):
                captured["timeout"] = timeout
                captured["filters"] = kwargs
                return [FakeAgent(12, "Research Copilot")]

        return operation(_ClientAgent)

    monkeypatch.setattr(api_mod, "_run_sdk_model_operation", _run_sdk_model_operation)

    out = api_mod.list_agents(timeout=9, filters={"status": "active"})
    assert captured == {
        "module_name": "mainsequence.client.agent_runtime_models",
        "class_name": "Agent",
        "timeout": 9,
        "filters": {"status": "active"},
    }
    assert out == [{"id": 12, "name": "Research Copilot", "status": "draft"}]


def test_create_agent_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    class FakeAgent:
        @staticmethod
        def model_dump(mode="json"):
            return {"id": 12, "name": "Research Copilot", "status": "active"}

    def _run_sdk_model_operation(*, module_name, class_name, operation, project_id_env=None):
        captured["module_name"] = module_name
        captured["class_name"] = class_name

        class _ClientAgent:
            @classmethod
            def create(cls, timeout=None, **kwargs):
                captured["timeout"] = timeout
                captured["create_kwargs"] = kwargs
                return FakeAgent()

        return operation(_ClientAgent)

    monkeypatch.setattr(api_mod, "_run_sdk_model_operation", _run_sdk_model_operation)

    out = api_mod.create_agent(
        name="Research Copilot",
        agent_unique_id="research-copilot",
        description="Desk agent",
        status="active",
        labels=["research", "desk"],
        llm_provider="openai",
        llm_model="gpt-5.4",
        engine_name="codex",
        runtime_config={"temperature": 0},
        configuration={"mode": "analysis"},
        metadata={"owner": "quant"},
        timeout=14,
    )
    assert captured == {
        "module_name": "mainsequence.client.agent_runtime_models",
        "class_name": "Agent",
        "timeout": 14,
        "create_kwargs": {
            "name": "Research Copilot",
            "agent_unique_id": "research-copilot",
            "description": "Desk agent",
            "status": "active",
            "labels": ["research", "desk"],
            "llm_provider": "openai",
            "llm_model": "gpt-5.4",
            "engine_name": "codex",
            "runtime_config": {"temperature": 0},
            "configuration": {"mode": "analysis"},
            "metadata": {"owner": "quant"},
        },
    }
    assert out["id"] == 12


def test_get_or_create_agent_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    class FakeAgent:
        @staticmethod
        def model_dump(mode="json"):
            return {
                "id": 12,
                "name": "Research Copilot",
                "agent_unique_id": "research-copilot",
                "status": "active",
            }

    def _run_sdk_model_operation(*, module_name, class_name, operation, project_id_env=None):
        captured["module_name"] = module_name
        captured["class_name"] = class_name

        class _ClientAgent:
            @classmethod
            def get_or_create(cls, timeout=None, **kwargs):
                captured["timeout"] = timeout
                captured["get_or_create_kwargs"] = kwargs
                return FakeAgent()

        return operation(_ClientAgent)

    monkeypatch.setattr(api_mod, "_run_sdk_model_operation", _run_sdk_model_operation)

    out = api_mod.get_or_create_agent(
        name="Research Copilot",
        agent_unique_id="research-copilot",
        description="Desk agent",
        status="active",
        labels=["research", "desk"],
        llm_provider="openai",
        llm_model="gpt-5.4",
        engine_name="codex",
        runtime_config={"temperature": 0},
        configuration={"mode": "analysis"},
        metadata={"owner": "quant"},
        timeout=14,
    )
    assert captured == {
        "module_name": "mainsequence.client.agent_runtime_models",
        "class_name": "Agent",
        "timeout": 14,
        "get_or_create_kwargs": {
            "name": "Research Copilot",
            "agent_unique_id": "research-copilot",
            "description": "Desk agent",
            "status": "active",
            "labels": ["research", "desk"],
            "llm_provider": "openai",
            "llm_model": "gpt-5.4",
            "engine_name": "codex",
            "runtime_config": {"temperature": 0},
            "configuration": {"mode": "analysis"},
            "metadata": {"owner": "quant"},
        },
    }
    assert out["id"] == 12


def test_start_agent_new_session_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    class FakeAgentSession:
        @staticmethod
        def model_dump(mode="json"):
            return {
                "id": 801,
                "agent": {"id": 12, "name": "Research Copilot"},
                "status": "pending",
                "llm_provider": "openai",
                "llm_model": "gpt-5.4",
                "engine_name": "codex",
            }

    def _run_sdk_model_operation(*, module_name, class_name, operation, project_id_env=None):
        captured["module_name"] = module_name
        captured["class_name"] = class_name

        class _ClientAgent:
            @classmethod
            def get(cls, pk=None, timeout=None):
                captured["pk"] = pk
                captured["timeout"] = timeout

                class _Agent:
                    def start_new_session(self, timeout=None):
                        captured["start_new_session_timeout"] = timeout
                        return FakeAgentSession()

                return _Agent()

        return operation(_ClientAgent)

    monkeypatch.setattr(api_mod, "_run_sdk_model_operation", _run_sdk_model_operation)

    out = api_mod.start_agent_new_session(12, timeout=16)
    assert captured == {
        "module_name": "mainsequence.client.agent_runtime_models",
        "class_name": "Agent",
        "pk": 12,
        "timeout": 16,
        "start_new_session_timeout": 16,
    }
    assert out["id"] == 801


def test_get_agent_latest_session_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    class FakeAgentSession:
        @staticmethod
        def model_dump(mode="json"):
            return {
                "id": 802,
                "agent": {"id": 12, "name": "Research Copilot"},
                "status": "completed",
                "llm_provider": "openai",
                "llm_model": "gpt-5.4",
                "engine_name": "codex",
            }

    def _run_sdk_model_operation(*, module_name, class_name, operation, project_id_env=None):
        captured["module_name"] = module_name
        captured["class_name"] = class_name

        class _ClientAgent:
            @classmethod
            def get(cls, pk=None, timeout=None):
                captured["pk"] = pk
                captured["timeout"] = timeout

                class _Agent:
                    def get_latest_session(self, timeout=None):
                        captured["get_latest_session_timeout"] = timeout
                        return FakeAgentSession()

                return _Agent()

        return operation(_ClientAgent)

    monkeypatch.setattr(api_mod, "_run_sdk_model_operation", _run_sdk_model_operation)

    out = api_mod.get_agent_latest_session(12, timeout=17)
    assert captured == {
        "module_name": "mainsequence.client.agent_runtime_models",
        "class_name": "Agent",
        "pk": 12,
        "timeout": 17,
        "get_latest_session_timeout": 17,
    }
    assert out["id"] == 802


def test_get_agent_session_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    class FakeAgentSession:
        @staticmethod
        def model_dump(mode="json"):
            return {
                "id": 803,
                "agent": {"id": 12, "name": "Research Copilot"},
                "status": "completed",
                "llm_provider": "openai",
                "llm_model": "gpt-5.4",
                "engine_name": "codex",
            }

    def _run_sdk_model_operation(*, module_name, class_name, operation, project_id_env=None):
        captured["module_name"] = module_name
        captured["class_name"] = class_name

        class _ClientAgentSession:
            @classmethod
            def get(cls, pk=None, timeout=None):
                captured["pk"] = pk
                captured["timeout"] = timeout
                return FakeAgentSession()

        return operation(_ClientAgentSession)

    monkeypatch.setattr(api_mod, "_run_sdk_model_operation", _run_sdk_model_operation)

    out = api_mod.get_agent_session(803, timeout=18)
    assert captured == {
        "module_name": "mainsequence.client.agent_runtime_models",
        "class_name": "AgentSession",
        "pk": 803,
        "timeout": 18,
    }
    assert out["id"] == 803


def test_list_agent_users_can_view_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    def _run_sdk_model_operation(*, module_name, class_name, operation, project_id_env=None):
        captured["module_name"] = module_name
        captured["class_name"] = class_name

        class _ClientAgent:
            @classmethod
            def get(cls, pk=None, timeout=None):
                captured["pk"] = pk
                captured["timeout"] = timeout

                class _Agent:
                    def can_view(self, timeout=None):
                        captured["can_view_timeout"] = timeout
                        return types.SimpleNamespace(
                            model_dump=lambda mode="json": {
                                "access_level": "view",
                                "users": [{"id": 7, "username": "viewer"}],
                                "teams": [],
                            }
                        )

                return _Agent()

        return operation(_ClientAgent)

    monkeypatch.setattr(api_mod, "_run_sdk_model_operation", _run_sdk_model_operation)

    out = api_mod.list_agent_users_can_view(12, timeout=16)
    assert captured == {
        "module_name": "mainsequence.client.agent_runtime_models",
        "class_name": "Agent",
        "pk": 12,
        "timeout": 16,
        "can_view_timeout": 16,
    }
    assert out["users"][0]["username"] == "viewer"


def test_list_agent_runs_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    class FakeAgentRun:
        def model_dump(self, mode="json"):
            return {"id": 501, "status": "running", "agent": {"id": 12, "name": "Research Copilot"}}

    def _run_sdk_model_operation(*, module_name, class_name, operation, project_id_env=None):
        captured["module_name"] = module_name
        captured["class_name"] = class_name

        class _ClientAgentRun:
            @classmethod
            def filter(cls, timeout=None, **kwargs):
                captured["timeout"] = timeout
                captured["filters"] = kwargs
                return [FakeAgentRun()]

        return operation(_ClientAgentRun)

    monkeypatch.setattr(api_mod, "_run_sdk_model_operation", _run_sdk_model_operation)

    out = api_mod.list_agent_runs(timeout=10, filters={"status": "running"})
    assert captured == {
        "module_name": "mainsequence.client.agent_runtime_models",
        "class_name": "AgentRun",
        "timeout": 10,
        "filters": {"status": "running"},
    }
    assert out == [{"id": 501, "status": "running", "agent": {"id": 12, "name": "Research Copilot"}}]


def test_pydantic_cli_metadata_from_source():
    metadata_mod = importlib.import_module("mainsequence.cli.pydantic_cli")
    meta = metadata_mod.get_cli_field_metadata(
        "mainsequence.client.models_helpers.Job",
        "execution_path",
    )
    assert meta.label == "Execution path"
    assert "content root" in meta.description
    assert "scripts/test.py" in meta.examples


def test_model_filter_parser_uses_filterset_metadata():
    filters_mod = importlib.import_module("mainsequence.cli.model_filters")

    class FakeModel:
        FILTERSET_FIELDS = {
            "id": ["exact", "in"],
            "is_active": ["exact", "isnull"],
            "name": ["contains"],
        }
        FILTER_VALUE_NORMALIZERS = {
            "id": "id",
            "is_active__isnull": "bool",
            "name": "str",
        }

    rows = filters_mod.build_cli_model_filter_rows(FakeModel)
    assert ["id", "exact", "integer ID", "id"] in rows
    assert ["id__in", "in", "comma-separated integer IDs", "id"] in rows
    assert ["is_active__isnull", "isnull", "true/false", "bool"] in rows
    assert ["name__contains", "contains", "text", "str"] in rows

    parsed = filters_mod.parse_cli_model_filters(
        FakeModel,
        ["id__in=1,2,3", "name__contains=daily", "is_active__isnull=true"],
    )
    assert parsed == {
        "id__in": ["1", "2", "3"],
        "name__contains": "daily",
        "is_active__isnull": "true",
    }


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


def test_settings_set_base_json(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(
        cli_mod.cfg,
        "set_mainsequence_path",
        lambda path: {"mainsequence_path": path},
    )
    result = runner.invoke(cli_mod.app, ["settings", "set-base", "/tmp/ms-base", "--json"])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["mainsequence_path"] == "/tmp/ms-base"


def test_settings_set_backend(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(
        cli_mod.cfg,
        "set_backend_url",
        lambda url: {"backend_url": url},
    )
    result = runner.invoke(cli_mod.app, ["settings", "set-backend", "https://example.test"])
    assert result.exit_code == 0
    assert "Backend URL set to" in result.output


def test_settings_set_backend_json(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(
        cli_mod.cfg,
        "set_backend_url",
        lambda url: {"backend_url": url},
    )
    result = runner.invoke(cli_mod.app, ["settings", "set-backend", "https://example.test", "--json"])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["backend_url"] == "https://example.test"


def test_settings_reset(cli_mod, runner, monkeypatch):
    captured = {}
    monkeypatch.setattr(
        cli_mod.cfg,
        "DEFAULTS",
        {"backend_url": f"{cli_mod.cfg.STANDARD_BACKEND_URL}/", "mainsequence_path": "/tmp/mainsequence", "version": 1},
    )
    monkeypatch.setattr(
        cli_mod.cfg,
        "set_config",
        lambda updates: captured.update(updates) or updates | {"updated_at": "2026-04-20T00:00:00Z"},
    )
    monkeypatch.setattr(cli_mod.cfg, "clear_session_overrides", lambda: captured.update(cleared=True))

    result = runner.invoke(cli_mod.app, ["settings", "reset"])
    assert result.exit_code == 0
    assert captured["backend_url"] == cli_mod.cfg.STANDARD_BACKEND_URL
    assert captured["mainsequence_path"].endswith("/tmp/mainsequence")
    assert captured["cleared"] is True
    assert "Settings reset to standard defaults" in result.output


def test_settings_refresh_alias(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(
        cli_mod.cfg,
        "DEFAULTS",
        {"backend_url": f"{cli_mod.cfg.STANDARD_BACKEND_URL}/", "mainsequence_path": "/tmp/mainsequence", "version": 1},
    )
    monkeypatch.setattr(
        cli_mod.cfg,
        "set_config",
        lambda updates: updates | {"updated_at": "2026-04-20T00:00:00Z"},
    )
    monkeypatch.setattr(cli_mod.cfg, "clear_session_overrides", lambda: None)

    result = runner.invoke(cli_mod.app, ["settings", "refresh"])
    assert result.exit_code == 0
    assert "Settings reset to standard defaults" in result.output


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
    session_override = {}
    monkeypatch.setattr(
        cli_mod,
        "login_via_browser",
        lambda no_open=False, on_authorize_url=None: {
            "backend": "https://example.test",
            "access": "acc-123",
            "refresh": "ref-456",
        },
    )
    monkeypatch.setattr(cli_mod, "get_current_user_profile", lambda: {"username": "user@example.com"})
    monkeypatch.setattr(cli_mod.cfg, "save_tokens", lambda username, access, refresh: True)
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": "/tmp/mainsequence"},
    )
    monkeypatch.setattr(cli_mod.cfg, "auth_persistence_label", lambda: "secure OS storage")
    monkeypatch.setattr(
        cli_mod.cfg,
        "set_session_overrides",
        lambda **kwargs: session_override.update(kwargs) or kwargs,
    )
    monkeypatch.setattr(
        cli_mod,
        "get_projects",
        lambda: (_ for _ in ()).throw(AssertionError("login should not fetch projects")),
    )

    result = runner.invoke(
        cli_mod.app,
        ["login", "--no-status"],
    )
    assert result.exit_code == 0
    assert "MAIN SEQUENCE" in result.output
    assert "__  __" in result.output
    assert "Signed in as user@example.com" in result.output
    assert "Auth tokens are persisted in secure OS storage" in result.output
    assert cli_mod.cfg.STANDARD_BACKEND_URL in result.output
    assert session_override == {
        "backend_url": cli_mod.cfg.STANDARD_BACKEND_URL,
        "mainsequence_path": None,
    }


def test_login_with_backend_override(cli_mod, runner, monkeypatch):
    seen = {}
    session_override = {}
    cleared = {"called": False}

    def _browser_login(no_open=False, on_authorize_url=None):
        seen["backend"] = cli_mod.cfg.backend_url()
        return {
            "backend": seen["backend"],
            "access": "acc-123",
            "refresh": "ref-456",
        }

    monkeypatch.setattr(cli_mod, "login_via_browser", _browser_login)
    monkeypatch.setattr(cli_mod, "get_current_user_profile", lambda: {"username": "user@example.com"})
    monkeypatch.setattr(cli_mod.cfg, "save_tokens", lambda username, access, refresh: True)
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
    monkeypatch.setattr(cli_mod.cfg, "auth_persistence_label", lambda: "secure OS storage")
    monkeypatch.setattr(
        cli_mod,
        "get_projects",
        lambda: (_ for _ in ()).throw(AssertionError("login should not fetch projects")),
    )
    monkeypatch.delenv("MAIN_SEQUENCE_BACKEND_URL", raising=False)

    result = runner.invoke(
        cli_mod.app,
        ["login", "127.0.0.1:800", "mainsequence-dev", "--no-status"],
    )
    assert result.exit_code == 0
    assert seen["backend"] == "http://127.0.0.1:800"
    assert session_override["backend_url"] == "http://127.0.0.1:800"
    assert session_override["mainsequence_path"] == "mainsequence-dev"
    assert cleared["called"] is False
    assert "http://127.0.0.1:800" in result.output
    assert "MAIN_SEQUENCE_BACKEND_URL" not in os.environ


def test_login_with_different_backend_requires_projects_base(cli_mod, runner, monkeypatch):
    called = {"browser": False}

    def _browser_login(no_open=False, on_authorize_url=None):
        called["browser"] = True
        return {"backend": "http://127.0.0.1:8000", "access": "acc-123", "refresh": "ref-456"}

    monkeypatch.setattr(cli_mod, "login_via_browser", _browser_login)
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": "/tmp/mainsequence", "backend_url": "https://main-sequence.app"},
    )
    monkeypatch.delenv("MAIN_SEQUENCE_BACKEND_URL", raising=False)

    result = runner.invoke(
        cli_mod.app,
        ["login", "127.0.0.1:8000", "--no-status"],
    )
    assert result.exit_code == 1
    assert "must also specify a projects base folder" in result.output
    assert called["browser"] is False


def test_login_with_different_backend_allows_current_projects_base(cli_mod, runner, monkeypatch):
    called = {"browser": False}

    def _browser_login(no_open=False, on_authorize_url=None):
        called["browser"] = True
        return {"backend": "http://127.0.0.1:8000", "access": "acc-123", "refresh": "ref-456"}

    monkeypatch.setattr(cli_mod, "login_via_browser", _browser_login)
    monkeypatch.setattr(cli_mod, "get_current_user_profile", lambda: {"username": "user@example.com"})
    monkeypatch.setattr(cli_mod.cfg, "save_tokens", lambda username, access, refresh: True)
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": "/tmp/mainsequence", "backend_url": "https://main-sequence.app"},
    )
    monkeypatch.setattr(cli_mod.cfg, "auth_persistence_label", lambda: "local CLI auth storage")
    monkeypatch.setattr(cli_mod.cfg, "set_session_overrides", lambda **kwargs: kwargs)
    monkeypatch.setattr(cli_mod.cfg, "clear_session_overrides", lambda: None)
    monkeypatch.delenv("MAIN_SEQUENCE_BACKEND_URL", raising=False)

    result = runner.invoke(
        cli_mod.app,
        ["login", "127.0.0.1:8000", "/tmp/mainsequence", "--no-status"],
    )
    assert result.exit_code == 0
    assert called["browser"] is True


def test_login_export_env(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(
        cli_mod,
        "login_via_browser",
        lambda no_open=False, on_authorize_url=None: {
            "backend": "https://example.test",
            "access": "acc-123",
            "refresh": "ref-456",
        },
    )
    monkeypatch.setattr(cli_mod, "get_current_user_profile", lambda: {"username": "user@example.com"})
    monkeypatch.setattr(cli_mod.cfg, "save_tokens", lambda username, access, refresh: True)
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": "/tmp/mainsequence"},
    )
    monkeypatch.setattr(cli_mod.cfg, "set_session_overrides", lambda **kwargs: kwargs)
    monkeypatch.setattr(
        cli_mod,
        "get_projects",
        lambda: (_ for _ in ()).throw(AssertionError("login should not fetch projects")),
    )

    result = runner.invoke(
        cli_mod.app,
        ["login", "--no-status", "--export"],
    )
    assert result.exit_code == 0
    assert 'export MAINSEQUENCE_ACCESS_TOKEN="acc-123"' in result.output
    assert 'export MAINSEQUENCE_REFRESH_TOKEN="ref-456"' in result.output
    assert 'export MAINSEQUENCE_USERNAME="user@example.com"' in result.output


def test_login_with_jwt_tokens(cli_mod, runner, monkeypatch):
    saved = {}
    session_override = {}

    monkeypatch.setattr(
        cli_mod.cfg,
        "save_tokens",
        lambda username, access, refresh: saved.update(
            {"username": username, "access": access, "refresh": refresh}
        ) or True,
    )
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": "/tmp/mainsequence"},
    )
    monkeypatch.setattr(cli_mod.cfg, "auth_persistence_label", lambda: "local CLI auth storage")
    monkeypatch.setattr(
        cli_mod.cfg,
        "set_session_overrides",
        lambda **kwargs: session_override.update(kwargs) or kwargs,
    )
    monkeypatch.setattr(
        cli_mod,
        "get_projects",
        lambda: (_ for _ in ()).throw(AssertionError("login should not fetch projects")),
    )

    result = runner.invoke(
        cli_mod.app,
        [
            "login",
            "--access-token",
            "acc-123",
            "--refresh-token",
            "ref-456",
            "--no-status",
        ],
    )
    assert result.exit_code == 0
    assert saved == {"username": "", "access": "acc-123", "refresh": "ref-456"}
    assert "Signed in with JWT tokens" in result.output
    assert "Auth tokens are persisted in local CLI auth storage" in result.output
    assert session_override == {
        "backend_url": cli_mod.cfg.STANDARD_BACKEND_URL,
        "mainsequence_path": None,
    }


def test_login_runtime_credential_exchanges_token(cli_mod, runner, monkeypatch):
    session_override = {}
    exchange = {"called": False}
    saved = {}

    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "runtime_credential")

    def _exchange_runtime_credential_for_cli_login(backend_url):
        exchange["called"] = True
        exchange["backend_url"] = backend_url
        os.environ["MAINSEQUENCE_ACCESS_TOKEN"] = "runtime-access"
        return "runtime-access"

    monkeypatch.setattr(
        cli_mod,
        "_exchange_runtime_credential_for_cli_login",
        _exchange_runtime_credential_for_cli_login,
    )
    monkeypatch.setattr(cli_mod, "login_via_browser", lambda **kwargs: (_ for _ in ()).throw(AssertionError))
    monkeypatch.setattr(
        cli_mod.cfg,
        "save_tokens",
        lambda username, access, refresh: saved.update(username=username, access=access, refresh=refresh) or True,
    )
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": "/tmp/mainsequence"},
    )
    monkeypatch.setattr(cli_mod.cfg, "auth_persistence_label", lambda: "local CLI auth storage")
    monkeypatch.setattr(
        cli_mod.cfg,
        "set_session_overrides",
        lambda **kwargs: session_override.update(kwargs) or kwargs,
    )

    result = runner.invoke(cli_mod.app, ["login"])

    assert result.exit_code == 0
    assert exchange["called"] is True
    assert exchange["backend_url"]
    assert saved == {"username": "", "access": "runtime-access", "refresh": ""}
    assert os.environ["MAINSEQUENCE_ACCESS_TOKEN"] == "runtime-access"
    assert "Signed in with runtime credential" in result.output
    assert "no CLI JWT refresh token exists" in result.output
    assert "re-exchange the runtime credential automatically" in result.output
    assert session_override == {
        "backend_url": cli_mod.cfg.STANDARD_BACKEND_URL,
        "mainsequence_path": None,
    }


def test_login_runtime_credential_export(monkeypatch, cli_mod, runner):
    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "runtime_credential")
    monkeypatch.setattr(
        cli_mod,
        "_exchange_runtime_credential_for_cli_login",
        lambda backend_url: "runtime-access",
    )
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": "/tmp/mainsequence"},
    )
    monkeypatch.setattr(cli_mod.cfg, "set_session_overrides", lambda **kwargs: kwargs)

    result = runner.invoke(cli_mod.app, ["login", "--export"])

    assert result.exit_code == 0
    assert 'export MAINSEQUENCE_AUTH_MODE="runtime_credential"' in result.output
    assert 'export MAINSEQUENCE_ACCESS_TOKEN="runtime-access"' in result.output
    assert "MAINSEQUENCE_REFRESH_TOKEN" not in result.output


def test_login_runtime_credential_uses_backend_override(cli_mod, runner, monkeypatch):
    seen = {}
    session_override = {}

    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "runtime_credential")
    monkeypatch.setattr(
        cli_mod,
        "_exchange_runtime_credential_for_cli_login",
        lambda backend_url: seen.update(backend_url=backend_url) or "runtime-access",
    )
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
    monkeypatch.setattr(cli_mod.cfg, "auth_persistence_label", lambda: "local CLI auth storage")
    monkeypatch.delenv("MAIN_SEQUENCE_BACKEND_URL", raising=False)

    result = runner.invoke(
        cli_mod.app,
        [
            "login",
            "--backend",
            "http://127.0.0.1:8000",
            "--projects-base",
            "mainsequence-dev",
        ],
    )

    assert result.exit_code == 0
    assert seen["backend_url"] == "http://127.0.0.1:8000"
    assert session_override == {
        "backend_url": "http://127.0.0.1:8000",
        "mainsequence_path": "mainsequence-dev",
    }
    assert "MAIN_SEQUENCE_BACKEND_URL" not in os.environ


def test_login_runtime_credential_rejects_manual_jwt(cli_mod, runner, monkeypatch):
    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "runtime_credential")

    result = runner.invoke(
        cli_mod.app,
        ["login", "--access-token", "acc-123", "--refresh-token", "ref-456"],
    )

    assert result.exit_code == 1
    assert "Runtime credential login cannot be combined" in result.output


def test_api_refresh_access_runtime_credential_reexchange(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "runtime_credential")
    monkeypatch.setattr(api_mod, "backend_url", lambda: "http://127.0.0.1:8000")
    monkeypatch.setattr(api_mod, "get_tokens", lambda: {"username": "", "access": "", "refresh": ""})
    monkeypatch.delenv("MAINSEQUENCE_ACCESS_TOKEN", raising=False)

    saved = {}

    def _save_tokens(username, access, refresh):
        saved["username"] = username
        saved["access"] = access
        saved["refresh"] = refresh
        return True

    monkeypatch.setattr(api_mod, "save_tokens", _save_tokens)

    fake_utils = types.ModuleType("mainsequence.client.utils")

    class _Provider:
        def __init__(self, token_url):
            saved["token_url"] = token_url

        def refresh(self, force=False):
            saved["force"] = force
            os.environ["MAINSEQUENCE_ACCESS_TOKEN"] = "runtime-new-access"

    fake_utils.RuntimeCredentialAuthProvider = _Provider
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)

    out = api_mod.refresh_access()
    assert out == "runtime-new-access"
    assert saved["token_url"] == "http://127.0.0.1:8000/orm/api/pods/runtime-credentials/token/"
    assert saved["force"] is True
    assert saved["access"] == "runtime-new-access"
    assert saved["refresh"] == ""


def test_login_with_jwt_tokens_and_backend_override(cli_mod, runner, monkeypatch):
    session_override = {}
    monkeypatch.setattr(cli_mod.cfg, "save_tokens", lambda username, access, refresh: True)
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
    monkeypatch.setattr(cli_mod.cfg, "auth_persistence_label", lambda: "local CLI auth storage")
    monkeypatch.setattr(
        cli_mod,
        "get_projects",
        lambda: (_ for _ in ()).throw(AssertionError("login should not fetch projects")),
    )
    monkeypatch.delenv("MAIN_SEQUENCE_BACKEND_URL", raising=False)

    result = runner.invoke(
        cli_mod.app,
        [
            "login",
            "--access-token",
            "acc-123",
            "--refresh-token",
            "ref-456",
            "--backend",
            "http://127.0.0.1:80",
            "--projects-base",
            "mainsequence-dev",
            "--no-status",
        ],
    )
    assert result.exit_code == 0
    assert session_override == {
        "backend_url": "http://127.0.0.1:80",
        "mainsequence_path": "mainsequence-dev",
    }
    assert "http://127.0.0.1:80" in result.output
    assert "MAIN_SEQUENCE_BACKEND_URL" not in os.environ


def test_login_with_jwt_tokens_and_different_backend_requires_projects_base(cli_mod, runner, monkeypatch):
    called = {"save_tokens": False}

    def _save_tokens(username, access, refresh):
        called["save_tokens"] = True
        return True

    monkeypatch.setattr(cli_mod.cfg, "save_tokens", _save_tokens)
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": "/tmp/mainsequence", "backend_url": "https://main-sequence.app"},
    )
    monkeypatch.delenv("MAIN_SEQUENCE_BACKEND_URL", raising=False)

    result = runner.invoke(
        cli_mod.app,
        [
            "login",
            "--access-token",
            "acc-123",
            "--refresh-token",
            "ref-456",
            "--backend",
            "127.0.0.1:8000",
            "--no-status",
        ],
    )
    assert result.exit_code == 1
    assert "must also specify a projects base folder" in result.output
    assert called["save_tokens"] is False


def test_login_export_env_with_jwt_tokens_omits_username(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod.cfg, "save_tokens", lambda username, access, refresh: True)
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": "/tmp/mainsequence"},
    )
    monkeypatch.setattr(cli_mod.cfg, "set_session_overrides", lambda **kwargs: kwargs)

    result = runner.invoke(
        cli_mod.app,
        [
            "login",
            "--access-token",
            "acc-123",
            "--refresh-token",
            "ref-456",
            "--export",
            "--no-status",
        ],
    )
    assert result.exit_code == 0
    assert 'export MAINSEQUENCE_ACCESS_TOKEN="acc-123"' in result.output
    assert 'export MAINSEQUENCE_REFRESH_TOKEN="ref-456"' in result.output
    assert "MAINSEQUENCE_USERNAME" not in result.output


def test_login_warns_when_secure_persist_fails(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(
        cli_mod,
        "login_via_browser",
        lambda no_open=False, on_authorize_url=None: {
            "backend": "https://example.test",
            "access": "acc-123",
            "refresh": "ref-456",
        },
    )
    monkeypatch.setattr(cli_mod, "get_current_user_profile", lambda: {})
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": "/tmp/mainsequence"},
    )
    monkeypatch.setattr(cli_mod.cfg, "save_tokens", lambda username, access, refresh: False)
    monkeypatch.setattr(cli_mod.cfg, "auth_persistence_label", lambda: "secure OS storage")
    monkeypatch.setattr(cli_mod.cfg, "set_session_overrides", lambda **kwargs: kwargs)
    monkeypatch.setattr(
        cli_mod,
        "get_projects",
        lambda: (_ for _ in ()).throw(AssertionError("login should not fetch projects")),
    )

    result = runner.invoke(
        cli_mod.app,
        ["login", "--no-status"],
    )
    assert result.exit_code == 0
    assert "Could not persist auth tokens in secure OS storage" in result.output


def test_login_does_not_fetch_projects_after_success(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(
        cli_mod,
        "login_via_browser",
        lambda no_open=False, on_authorize_url=None: {
            "backend": "https://example.test",
            "access": "acc-123",
            "refresh": "ref-456",
        },
    )
    monkeypatch.setattr(cli_mod, "get_current_user_profile", lambda: {"username": "user@example.com"})
    monkeypatch.setattr(cli_mod.cfg, "save_tokens", lambda username, access, refresh: True)
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": "/tmp/mainsequence"},
    )
    monkeypatch.setattr(cli_mod.cfg, "auth_persistence_label", lambda: "local CLI auth storage")
    monkeypatch.setattr(cli_mod.cfg, "set_session_overrides", lambda **kwargs: kwargs)
    monkeypatch.setattr(
        cli_mod,
        "get_projects",
        lambda: (_ for _ in ()).throw(AssertionError("login should not fetch projects")),
    )

    result = runner.invoke(
        cli_mod.app,
        ["login"],
    )
    assert result.exit_code == 0
    assert "Signed in as user@example.com" in result.output
    assert "Projects:" not in result.output


def test_jwt_login_does_not_fetch_projects_after_success(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod.cfg, "save_tokens", lambda username, access, refresh: True)
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": "/tmp/mainsequence"},
    )
    monkeypatch.setattr(cli_mod.cfg, "auth_persistence_label", lambda: "local CLI auth storage")
    monkeypatch.setattr(cli_mod.cfg, "set_session_overrides", lambda **kwargs: kwargs)
    monkeypatch.setattr(
        cli_mod,
        "get_projects",
        lambda: (_ for _ in ()).throw(AssertionError("login should not fetch projects")),
    )

    result = runner.invoke(
        cli_mod.app,
        ["login", "--access-token", "acc-123", "--refresh-token", "ref-456"],
    )
    assert result.exit_code == 0
    assert "Signed in with JWT tokens" in result.output
    assert "Projects:" not in result.output


def test_logout(cli_mod, runner, monkeypatch):
    cleared = {"called": False}
    monkeypatch.setattr(cli_mod, "logout_jwt_session", lambda: True)
    monkeypatch.setattr(cli_mod.cfg, "clear_tokens", lambda: True)
    monkeypatch.setattr(cli_mod.cfg, "clear_session_overrides", lambda: cleared.update(called=True))
    result = runner.invoke(cli_mod.app, ["logout"])
    assert result.exit_code == 0
    assert "backend session revoked" in result.output
    assert cleared["called"] is True


def test_logout_export_env(cli_mod, runner, monkeypatch):
    cleared = {"called": False}
    monkeypatch.setattr(cli_mod, "logout_jwt_session", lambda: True)
    monkeypatch.setattr(cli_mod.cfg, "clear_tokens", lambda: True)
    monkeypatch.setattr(cli_mod.cfg, "clear_session_overrides", lambda: cleared.update(called=True))
    result = runner.invoke(cli_mod.app, ["logout", "--export"])
    assert result.exit_code == 0
    assert "unset MAINSEQUENCE_ACCESS_TOKEN" in result.output
    assert "unset MAINSEQUENCE_REFRESH_TOKEN" in result.output
    assert "unset MAINSEQUENCE_USERNAME" in result.output
    assert cleared["called"] is True


def test_login_rejects_legacy_email_argument(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": "/tmp/mainsequence"},
    )
    result = runner.invoke(cli_mod.app, ["login", "user@example.com"])
    assert result.exit_code == 1
    assert "Email/password CLI login was removed" in result.output


def test_login_no_open_prints_authorize_url(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": "/tmp/mainsequence"},
    )
    monkeypatch.setattr(cli_mod.cfg, "auth_persistence_label", lambda: "local CLI auth storage")
    monkeypatch.setattr(cli_mod.cfg, "set_session_overrides", lambda **kwargs: kwargs)
    monkeypatch.setattr(cli_mod.cfg, "save_tokens", lambda username, access, refresh: True)
    monkeypatch.setattr(cli_mod, "get_current_user_profile", lambda: {"username": "user@example.com"})

    def _browser_login(no_open=False, on_authorize_url=None):
        assert no_open is True
        assert on_authorize_url is not None
        on_authorize_url("https://example.test/auth")
        return {"backend": "https://example.test", "access": "acc-123", "refresh": "ref-456"}

    monkeypatch.setattr(cli_mod, "login_via_browser", _browser_login)

    result = runner.invoke(cli_mod.app, ["login", "--no-open", "--no-status"])
    assert result.exit_code == 0
    assert "Open this URL to authenticate: https://example.test/auth" in result.output


def test_config_get_tokens_fallback_secure_store(cli_mod, monkeypatch):
    monkeypatch.delenv(cli_mod.cfg.ENV_ACCESS, raising=False)
    monkeypatch.delenv(cli_mod.cfg.ENV_REFRESH, raising=False)
    monkeypatch.delenv(cli_mod.cfg.ENV_USERNAME, raising=False)
    monkeypatch.setattr(cli_mod.cfg, "_read_local_tokens", lambda: {})
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


def test_config_get_tokens_fallback_local_store(cli_mod, monkeypatch, tmp_path):
    auth_json = tmp_path / "auth.json"
    cli_mod.cfg.write_json(
        auth_json,
        {"username": "u@example.com", "access": "acc", "refresh": "ref"},
    )
    monkeypatch.setattr(cli_mod.cfg, "AUTH_JSON", auth_json)
    monkeypatch.delenv(cli_mod.cfg.ENV_ACCESS, raising=False)
    monkeypatch.delenv(cli_mod.cfg.ENV_REFRESH, raising=False)
    monkeypatch.delenv(cli_mod.cfg.ENV_USERNAME, raising=False)
    monkeypatch.setattr(cli_mod.cfg, "_read_secure_tokens", lambda: {})

    out = cli_mod.cfg.get_tokens()
    assert out["username"] == "u@example.com"
    assert out["access"] == "acc"
    assert out["refresh"] == "ref"


def test_config_get_tokens_runtime_mode_allows_access_without_refresh(cli_mod, monkeypatch, tmp_path):
    auth_json = tmp_path / "auth.json"
    cli_mod.cfg.write_json(
        auth_json,
        {"username": "u@example.com", "access": "acc", "refresh": ""},
    )
    monkeypatch.setattr(cli_mod.cfg, "AUTH_JSON", auth_json)
    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "runtime_credential")
    monkeypatch.delenv(cli_mod.cfg.ENV_ACCESS, raising=False)
    monkeypatch.delenv(cli_mod.cfg.ENV_REFRESH, raising=False)
    monkeypatch.delenv(cli_mod.cfg.ENV_USERNAME, raising=False)
    monkeypatch.setattr(cli_mod.cfg, "_read_secure_tokens", lambda: {})

    out = cli_mod.cfg.get_tokens()
    assert out["username"] == "u@example.com"
    assert out["access"] == "acc"
    assert out["refresh"] == ""


def test_config_get_tokens_prefers_env_over_local_store(cli_mod, monkeypatch, tmp_path):
    auth_json = tmp_path / "auth.json"
    cli_mod.cfg.write_json(
        auth_json,
        {"username": "file@example.com", "access": "file-acc", "refresh": "file-ref"},
    )
    monkeypatch.setattr(cli_mod.cfg, "AUTH_JSON", auth_json)
    monkeypatch.setattr(cli_mod.cfg, "_read_secure_tokens", lambda: {})
    monkeypatch.setenv(cli_mod.cfg.ENV_USERNAME, "env@example.com")
    monkeypatch.setenv(cli_mod.cfg.ENV_ACCESS, "env-acc")
    monkeypatch.setenv(cli_mod.cfg.ENV_REFRESH, "env-ref")

    out = cli_mod.cfg.get_tokens()
    assert out["username"] == "env@example.com"
    assert out["access"] == "env-acc"
    assert out["refresh"] == "env-ref"


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
    monkeypatch.setattr(cli_mod.cfg, "_macos_security_exists", lambda: True)
    cli_mod.cfg.save_tokens("u@example.com", "acc", "ref")
    assert captured == {"username": "u@example.com", "access": "acc", "refresh": "ref"}


def test_config_save_tokens_writes_local_store_when_secure_store_unavailable(cli_mod, monkeypatch, tmp_path):
    auth_json = tmp_path / "auth.json"
    monkeypatch.setattr(cli_mod.cfg, "AUTH_JSON", auth_json)
    monkeypatch.setattr(cli_mod.cfg, "_macos_security_exists", lambda: False)

    ok = cli_mod.cfg.save_tokens("u@example.com", "acc", "ref")

    assert ok is True
    assert cli_mod.cfg.read_json(auth_json, {}) == {
        "username": "u@example.com",
        "access": "acc",
        "refresh": "ref",
    }


def test_config_clear_tokens_removes_local_store(cli_mod, monkeypatch, tmp_path):
    auth_json = tmp_path / "auth.json"
    cli_mod.cfg.write_json(
        auth_json,
        {"username": "u@example.com", "access": "acc", "refresh": "ref"},
    )
    monkeypatch.setattr(cli_mod.cfg, "AUTH_JSON", auth_json)
    monkeypatch.setattr(cli_mod.cfg, "_clear_secure_tokens", lambda: True)
    monkeypatch.setenv(cli_mod.cfg.ENV_USERNAME, "u@example.com")
    monkeypatch.setenv(cli_mod.cfg.ENV_ACCESS, "acc")
    monkeypatch.setenv(cli_mod.cfg.ENV_REFRESH, "ref")

    ok = cli_mod.cfg.clear_tokens()

    assert ok is True
    assert not auth_json.exists()
    assert os.environ.get(cli_mod.cfg.ENV_USERNAME) is None
    assert os.environ.get(cli_mod.cfg.ENV_ACCESS) is None
    assert os.environ.get(cli_mod.cfg.ENV_REFRESH) is None


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


def test_sdk_latest_json(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "fetch_latest_sdk_version", lambda: "v1.2.3")
    result = runner.invoke(cli_mod.app, ["sdk", "latest", "--json"])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["latest"] == "v1.2.3"


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


def test_project_can_view(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_project_users_can_view",
        lambda project_id, timeout=None: [
            {
                "id": 12,
                "username": "viewer",
                "email": "viewer@example.com",
                "first_name": "View",
                "last_name": "User",
            }
        ],
    )

    result = runner.invoke(cli_mod.app, ["project", "can_view", "4"])
    assert result.exit_code == 0
    assert "Project Users Who Can View" in result.output
    assert "viewer@example.com" in result.output
    assert "Total users who can view: 1" in result.output


def test_project_add_label(cli_mod, runner, monkeypatch):
    captured = {}
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _add(project_id, labels, timeout=None):
        captured["project_id"] = project_id
        captured["labels"] = labels
        captured["timeout"] = timeout
        return {"labels": [{"name": "rates"}, {"name": "research"}]}

    monkeypatch.setattr(cli_mod, "add_project_labels", _add)

    result = runner.invoke(
        cli_mod.app,
        ["project", "add-label", "4", "--label", "rates,research"],
    )
    assert result.exit_code == 0
    assert captured == {"project_id": 4, "labels": ["rates", "research"], "timeout": None}
    assert "Project add-label completed." in result.output
    assert "rates, research" in result.output


def test_project_add_to_edit(cli_mod, runner, monkeypatch):
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _add(project_id, user_id, timeout=None):
        captured["project_id"] = project_id
        captured["user_id"] = user_id
        captured["timeout"] = timeout
        return {
            "ok": True,
            "action": "add_to_edit",
            "detail": "User now has explicit edit access.",
            "object_id": project_id,
            "object_type": "tdag.project",
            "user": {
                "id": user_id,
                "username": "editor",
                "email": "editor@example.com",
                "first_name": "Edit",
                "last_name": "User",
            },
            "explicit_can_view": True,
            "explicit_can_edit": True,
            "explicit_can_view_user_ids": [user_id],
            "explicit_can_edit_user_ids": [user_id],
        }

    monkeypatch.setattr(cli_mod, "add_project_user_to_edit", _add)

    result = runner.invoke(cli_mod.app, ["project", "add_to_edit", "4", "12"])
    assert result.exit_code == 0
    assert captured == {"project_id": 4, "user_id": 12, "timeout": None}
    assert "Project add_to_edit completed." in result.output
    assert "Project Sharing Update" in result.output
    assert "editor@example.com" in result.output


def test_project_add_team_to_view(cli_mod, runner, monkeypatch):
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _add(project_id, team_id, timeout=None):
        captured["project_id"] = project_id
        captured["team_id"] = team_id
        captured["timeout"] = timeout
        return {
            "action": "add_team_to_view",
            "detail": "Team now has explicit view access.",
            "object_id": project_id,
            "object_type": "tdag.project",
            "team": {
                "id": team_id,
                "name": "Research",
                "description": "Core team",
            },
            "explicit_can_view": True,
            "explicit_can_edit": False,
            "explicit_can_view_team_ids": [team_id],
            "explicit_can_edit_team_ids": [],
        }

    monkeypatch.setattr(cli_mod, "add_project_team_to_view", _add)

    result = runner.invoke(cli_mod.app, ["project", "add_team_to_view", "4", "3"])
    assert result.exit_code == 0
    assert captured == {"project_id": 4, "team_id": 3, "timeout": None}
    assert "Project add_team_to_view completed." in result.output
    assert "Research" in result.output


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


def test_list_project_users_can_view_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"})
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

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
        def get(cls, pk=None, timeout=None, **filters):
            captured["get"] = {"pk": pk, "timeout": timeout, "filters": filters}

            class _Project:
                def can_view(self, timeout=None):
                    captured["can_view_timeout"] = timeout
                    return types.SimpleNamespace(
                        model_dump=lambda mode="python": {
                            "object_id": pk,
                            "object_type": "tdag.project",
                            "access_level": "view",
                            "users": [
                                {
                                    "id": 12,
                                    "username": "viewer",
                                    "email": "viewer@example.com",
                                    "first_name": "View",
                                    "last_name": "User",
                                }
                            ],
                            "teams": [],
                        }
                    )

            return _Project()

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.Project = FakeProject
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_tdag", fake_models)

    out = api_mod.list_project_users_can_view(4, timeout=9)
    assert captured["get"] == {"pk": 4, "timeout": 9, "filters": {}}
    assert captured["can_view_timeout"] == 9
    assert captured["jwt"] == ("acc", "ref")
    assert out["users"][0]["username"] == "viewer"


def test_add_project_user_to_edit_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"})
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

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
        def get(cls, pk=None, timeout=None, **filters):
            captured["get"] = {"pk": pk, "timeout": timeout, "filters": filters}

            class _Project:
                def add_to_edit(self, user_id, timeout=None):
                    captured["add_to_edit"] = {"user_id": user_id, "timeout": timeout}
                    return {
                        "ok": True,
                        "action": "add_to_edit",
                        "detail": "User now has explicit edit access.",
                        "object_id": pk,
                        "object_type": "tdag.project",
                        "user": {"id": user_id, "username": "editor", "email": "editor@example.com"},
                        "explicit_can_view": True,
                        "explicit_can_edit": True,
                        "explicit_can_view_user_ids": [user_id],
                        "explicit_can_edit_user_ids": [user_id],
                    }

            return _Project()

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.Project = FakeProject
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_tdag", fake_models)

    out = api_mod.add_project_user_to_edit(4, 12, timeout=10)
    assert captured["get"] == {"pk": 4, "timeout": 10, "filters": {}}
    assert captured["add_to_edit"] == {"user_id": 12, "timeout": 10}
    assert captured["jwt"] == ("acc", "ref")
    assert out["action"] == "add_to_edit"


def test_list_constants_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {"filters": []}

    monkeypatch.setattr(api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"})
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

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

    class FakeConstant:
        ROOT_URL = "https://old.test/orm/api/pods/constant"

        @classmethod
        def filter(cls, timeout=None, **kwargs):
            captured["filters"].append(kwargs)
            return [
                types.SimpleNamespace(
                    model_dump=lambda mode="python": {
                        "id": 7,
                        "name": "ASSETS__MASTER",
                        "value": {"source": "bbg"},
                    }
                )
            ]

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.Constant = FakeConstant
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_tdag", fake_models)

    out = api_mod.list_constants(filters={"name__in": ["ASSETS__MASTER"]})
    assert captured["filters"][0] == {"name__in": ["ASSETS__MASTER"]}
    assert captured["jwt"] == ("acc", "ref")
    assert out == [{"id": 7, "name": "ASSETS__MASTER", "value": {"source": "bbg"}}]


def test_create_constant_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"})
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

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

    class FakeConstant:
        ROOT_URL = "https://old.test/orm/api/pods/constant"

        @classmethod
        def create(cls, *, name, value, timeout=None):
            captured["name"] = name
            captured["value"] = value
            captured["timeout"] = timeout
            return types.SimpleNamespace(
                model_dump=lambda mode="python": {
                    "id": 7,
                    "name": name,
                    "value": value,
                }
            )

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.Constant = FakeConstant
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_tdag", fake_models)

    out = api_mod.create_constant(name="ASSETS__MASTER", value={"source": "bbg"}, timeout=15)
    assert captured["name"] == "ASSETS__MASTER"
    assert captured["value"] == {"source": "bbg"}
    assert captured["timeout"] == 15
    assert captured["jwt"] == ("acc", "ref")
    assert out["id"] == 7


def test_delete_constant_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"})
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

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

    class FakeConstant:
        ROOT_URL = "https://old.test/orm/api/pods/constant"

        @classmethod
        def get(cls, pk=None, timeout=None, **filters):
            captured["get"] = {"pk": pk, "timeout": timeout, "filters": filters}

            class _Constant:
                id = pk

                def model_dump(self, mode="python"):
                    return {
                        "id": pk,
                        "name": "ASSETS__MASTER",
                        "value": {"source": "bbg"},
                    }

                def delete(self, timeout=None):
                    captured["delete_timeout"] = timeout

            return _Constant()

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.Constant = FakeConstant
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_tdag", fake_models)

    out = api_mod.delete_constant(7, timeout=20)
    assert captured["get"] == {"pk": 7, "timeout": 20, "filters": {}}
    assert captured["delete_timeout"] == 20
    assert captured["jwt"] == ("acc", "ref")
    assert out["name"] == "ASSETS__MASTER"


def test_list_constant_users_can_edit_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"})
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

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

    class FakeConstant:
        ROOT_URL = "https://old.test/orm/api/pods/constant"

        @classmethod
        def get(cls, pk=None, timeout=None, **filters):
            captured["get"] = {"pk": pk, "timeout": timeout, "filters": filters}

            class _Constant:
                def can_edit(self, timeout=None):
                    captured["can_edit_timeout"] = timeout
                    return types.SimpleNamespace(
                        model_dump=lambda mode="python": {
                            "object_id": 7,
                            "object_type": "tdag.constant",
                            "access_level": "edit",
                            "users": [
                                {
                                    "id": 9,
                                    "username": "editor",
                                    "email": "editor@example.com",
                                    "first_name": "Edit",
                                    "last_name": "User",
                                }
                            ],
                            "teams": [],
                        }
                    )

            return _Constant()

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.Constant = FakeConstant
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_tdag", fake_models)

    out = api_mod.list_constant_users_can_edit(7, timeout=12)
    assert captured["get"] == {"pk": 7, "timeout": 12, "filters": {}}
    assert captured["can_edit_timeout"] == 12
    assert captured["jwt"] == ("acc", "ref")
    assert out["users"][0]["username"] == "editor"


def test_add_constant_user_to_edit_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"})
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

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

    class FakeConstant:
        ROOT_URL = "https://old.test/orm/api/pods/constant"

        @classmethod
        def get(cls, pk=None, timeout=None, **filters):
            captured["get"] = {"pk": pk, "timeout": timeout, "filters": filters}

            class _Constant:
                def add_to_edit(self, user_id, timeout=None):
                    captured["add_to_edit"] = {"user_id": user_id, "timeout": timeout}
                    return {
                        "ok": True,
                        "action": "add_to_edit",
                        "detail": "User now has explicit edit access.",
                        "object_id": pk,
                        "object_type": "tdag.constant",
                        "user": {"id": user_id, "username": "editor", "email": "editor@example.com"},
                        "explicit_can_view": True,
                        "explicit_can_edit": True,
                        "explicit_can_view_user_ids": [user_id],
                        "explicit_can_edit_user_ids": [user_id],
                    }

            return _Constant()

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.Constant = FakeConstant
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_tdag", fake_models)

    out = api_mod.add_constant_user_to_edit(7, 9, timeout=14)
    assert captured["get"] == {"pk": 7, "timeout": 14, "filters": {}}
    assert captured["add_to_edit"] == {"user_id": 9, "timeout": 14}
    assert captured["jwt"] == ("acc", "ref")
    assert out["action"] == "add_to_edit"


def test_list_secrets_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {"filters": []}

    monkeypatch.setattr(api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"})
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

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

    class FakeSecret:
        ROOT_URL = "https://old.test/orm/api/pods/secret"

        @classmethod
        def filter(cls, timeout=None, **kwargs):
            captured["filters"].append(kwargs)
            return [
                types.SimpleNamespace(
                    model_dump=lambda mode="python": {
                        "id": 8,
                        "name": "API_KEY",
                    }
                )
            ]

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.Secret = FakeSecret
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_tdag", fake_models)

    out = api_mod.list_secrets(filters={"name__in": ["API_KEY"]})
    assert captured["filters"][0] == {"name__in": ["API_KEY"]}
    assert captured["jwt"] == ("acc", "ref")
    assert out == [{"id": 8, "name": "API_KEY"}]


def test_list_secret_users_can_view_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"})
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

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

    class FakeSecret:
        ROOT_URL = "https://old.test/orm/api/pods/secret"

        @classmethod
        def get(cls, pk=None, timeout=None, **filters):
            captured["get"] = {"pk": pk, "timeout": timeout, "filters": filters}

            class _Secret:
                def can_view(self, timeout=None):
                    captured["can_view_timeout"] = timeout
                    return types.SimpleNamespace(
                        model_dump=lambda mode="python": {
                            "object_id": 8,
                            "object_type": "tdag.secret",
                            "access_level": "view",
                            "users": [
                                {
                                    "id": 11,
                                    "username": "viewer",
                                    "email": "viewer@example.com",
                                    "first_name": "View",
                                    "last_name": "User",
                                }
                            ],
                            "teams": [{"id": 4, "name": "Ops", "description": "", "member_count": 2}],
                        }
                    )

            return _Secret()

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.Secret = FakeSecret
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_tdag", fake_models)

    out = api_mod.list_secret_users_can_view(8, timeout=13)
    assert captured["get"] == {"pk": 8, "timeout": 13, "filters": {}}
    assert captured["can_view_timeout"] == 13
    assert captured["jwt"] == ("acc", "ref")
    assert out["users"][0]["username"] == "viewer"
    assert out["teams"][0]["name"] == "Ops"
    assert out["teams"][0]["member_count"] == 2


def test_add_secret_user_to_edit_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"})
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

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

    class FakeSecret:
        ROOT_URL = "https://old.test/orm/api/pods/secret"

        @classmethod
        def get(cls, pk=None, timeout=None, **filters):
            captured["get"] = {"pk": pk, "timeout": timeout, "filters": filters}

            class _Secret:
                def add_to_edit(self, user_id, timeout=None):
                    captured["add_to_edit"] = {"user_id": user_id, "timeout": timeout}
                    return {
                        "ok": True,
                        "action": "add_to_edit",
                        "detail": "User now has explicit edit access.",
                        "object_id": pk,
                        "object_type": "tdag.secret",
                        "user": {"id": user_id, "username": "editor", "email": "editor@example.com"},
                        "explicit_can_view": True,
                        "explicit_can_edit": True,
                        "explicit_can_view_user_ids": [user_id],
                        "explicit_can_edit_user_ids": [user_id],
                    }

            return _Secret()

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.Secret = FakeSecret
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_tdag", fake_models)

    out = api_mod.add_secret_user_to_edit(8, 11, timeout=14)
    assert captured["get"] == {"pk": 8, "timeout": 14, "filters": {}}
    assert captured["add_to_edit"] == {"user_id": 11, "timeout": 14}
    assert captured["jwt"] == ("acc", "ref")
    assert out["action"] == "add_to_edit"


def test_create_secret_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"})
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

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

    class FakeSecret:
        ROOT_URL = "https://old.test/orm/api/pods/secret"

        @classmethod
        def create(cls, *, name, value, timeout=None):
            captured["name"] = name
            captured["value"] = value
            captured["timeout"] = timeout
            return types.SimpleNamespace(
                model_dump=lambda mode="python": {
                    "id": 8,
                    "name": name,
                }
            )

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.Secret = FakeSecret
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_tdag", fake_models)

    out = api_mod.create_secret(name="API_KEY", value="super-secret", timeout=10)
    assert captured["name"] == "API_KEY"
    assert captured["value"] == "super-secret"
    assert captured["timeout"] == 10
    assert captured["jwt"] == ("acc", "ref")
    assert out["id"] == 8


def test_delete_secret_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"})
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

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

    class FakeSecret:
        ROOT_URL = "https://old.test/orm/api/pods/secret"

        @classmethod
        def get(cls, pk=None, timeout=None, **filters):
            captured["get"] = {"pk": pk, "timeout": timeout, "filters": filters}

            class _Secret:
                id = pk

                def model_dump(self, mode="python"):
                    return {
                        "id": pk,
                        "name": "API_KEY",
                    }

                def delete(self, timeout=None):
                    captured["delete_timeout"] = timeout

            return _Secret()

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.Secret = FakeSecret
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_tdag", fake_models)

    out = api_mod.delete_secret(8, timeout=20)
    assert captured["get"] == {"pk": 8, "timeout": 20, "filters": {}}
    assert captured["delete_timeout"] == 20
    assert captured["jwt"] == ("acc", "ref")
    assert out["name"] == "API_KEY"


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
                    "creation_date": "2026-04-07T09:00:00Z",
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
                            "creation_date": "2026-04-07T09:00:00Z",
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

    out = api_mod.list_project_images(
        related_project_id=123,
        filters={"project_repo_hash__in": ["abc123", "def456"]},
    )
    assert captured["filters"][0] == {
        "project_repo_hash__in": ["abc123", "def456"],
        "related_project__id__in": [123],
    }
    assert captured["env_project_id"] == "123"
    assert captured["jwt"] == ("acc", "ref")
    assert out == [
        {
            "id": 77,
            "project_repo_hash": "abc123",
            "related_project": 123,
            "base_image": {"id": 22, "title": "Python 3.12"},
            "creation_date": "2026-04-07T09:00:00Z",
        }
    ]
    assert os.environ.get("MAIN_SEQUENCE_PROJECT_ID") is None


def test_delete_project_image_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"})
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

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
        def get(cls, pk=None, timeout=None, **filters):
            captured["get"] = {"pk": pk, "timeout": timeout, "filters": filters}

            class _Image:
                id = pk

                def model_dump(self, mode="python"):
                    return {
                        "id": pk,
                        "project_repo_hash": "abc123",
                        "base_image": {"id": 22, "title": "Python 3.12"},
                        "is_ready": True,
                    }

                def delete(self):
                    captured["deleted"] = pk

            return _Image()

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.ProjectImage = FakeProjectImage
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_tdag", fake_models)

    out = api_mod.delete_project_image(image_id=94)
    assert captured["get"] == {"pk": 94, "timeout": None, "filters": {}}
    assert captured["deleted"] == 94
    assert captured["jwt"] == ("acc", "ref")
    assert out["id"] == 94
    assert out["project_repo_hash"] == "abc123"


def test_list_project_jobs_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {"filters": []}

    monkeypatch.setattr(api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"})
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")
    monkeypatch.delenv("MAIN_SEQUENCE_PROJECT_ID", raising=False)

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
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
    fake_helpers = types.ModuleType("mainsequence.client.models_helpers")
    fake_helpers.Job = FakeJob
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_helpers", fake_helpers)

    out = api_mod.list_project_jobs(project_id=123, filters={"name__contains": "daily"})
    assert captured["filters"][0] == {"name__contains": "daily", "project__id": 123}
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
        filters={"id__in": ["301", "302"]},
    )
    assert captured["filters"][0] == {
        "id__in": ["301", "302"],
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

                def create_fastapi(self, **kwargs):
                    captured["create_fastapi"] = kwargs
                    return types.SimpleNamespace(model_dump=lambda: {"id": 503, "resource": pk, "related_image": kwargs["related_image_id"]})

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


def test_create_project_resource_release_uses_client_model_for_fastapi(cli_mod, monkeypatch):
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
                def create_fastapi(self, **kwargs):
                    captured["create_fastapi"] = kwargs
                    return types.SimpleNamespace(model_dump=lambda: {"id": 503, "resource": pk, "related_image": kwargs["related_image_id"]})

            return _Resource()

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_helpers.ProjectResource = FakeProjectResource
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_helpers", fake_helpers)

    out = api_mod.create_project_resource_release(
        release_kind="fastapi",
        resource_id=381,
        related_image_id=94,
        spot=False,
        cpu_request="0.5",
        memory_request="1",
        gpu_request="",
        gpu_type="",
    )
    assert captured["get"] == {"pk": 381, "timeout": None, "filters": {}}
    assert captured["create_fastapi"]["related_image_id"] == 94
    assert captured["create_fastapi"]["spot"] is False
    assert captured["create_fastapi"]["cpu_request"] == "0.5"
    assert captured["create_fastapi"]["memory_request"] == "1"
    assert captured["jwt"] == ("acc", "ref")
    assert out == {"id": 503, "resource": 381, "related_image": 94}


def test_delete_resource_release_uses_client_model(cli_mod, monkeypatch):
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

    class FakeResourceRelease:
        ROOT_URL = "https://old.test/orm/api/pods/resource-release"

        @classmethod
        def get(cls, pk=None, timeout=None, **filters):
            captured["get"] = {"pk": pk, "timeout": timeout, "filters": filters}

            class _Release:
                id = pk

                def model_dump(self, mode="python"):
                    return {
                        "id": pk,
                        "release_kind": "streamlit_dashboard",
                        "subdomain": "analytics-123",
                        "resource": 381,
                        "related_image": 94,
                    }

                def delete(self):
                    captured["deleted"] = pk

            return _Release()

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_helpers.ResourceRelease = FakeResourceRelease
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_helpers", fake_helpers)

    out = api_mod.delete_resource_release(
        release_id=501,
        expected_release_kind="streamlit_dashboard",
    )
    assert captured["get"] == {"pk": 501, "timeout": None, "filters": {}}
    assert captured["deleted"] == 501
    assert captured["jwt"] == ("acc", "ref")
    assert out["id"] == 501
    assert out["release_kind"] == "streamlit_dashboard"


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

    out = api_mod.list_market_portfolios(filters={"id__in": ["42"]})
    assert captured["filters"][0] == {"id__in": ["42"]}
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


def test_list_data_node_storages_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {"filters": []}

    monkeypatch.setattr(api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"})
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

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

    class FakeDataNodeStorage:
        ROOT_URL = "https://old.test/orm/api/ts_manager/dynamic_table"

        @classmethod
        def filter(cls, timeout=None, **kwargs):
                captured["filters"].append(kwargs)
                return [
                    types.SimpleNamespace(
                        model_dump=lambda *args, **kwargs: {
                            "id": 42,
                            "storage_hash": "weights_daily",
                            "source_class_name": "PortfolioWeights",
                            "identifier": "weights_daily",
                            "data_source": {"display_name": "Default DB", "class_type": "timescale_db"},
                        "data_frequency_id": "1d",
                    }
                )
            ]

        @classmethod
        def get(cls, pk=None, timeout=None, **filters):
            captured["get"] = {"pk": pk, "filters": filters, "timeout": timeout}
            return types.SimpleNamespace(
                model_dump=lambda *args, **kwargs: {
                    "id": pk,
                    "storage_hash": "weights_daily",
                    "source_class_name": "PortfolioWeights",
                    "identifier": "weights_daily",
                    "data_source": {"display_name": "Default DB", "class_type": "timescale_db"},
                    "data_frequency_id": "1d",
                    "protect_from_deletion": True,
                }
            )

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.DataNodeStorage = FakeDataNodeStorage
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_tdag", fake_models)

    out = api_mod.list_data_node_storages(filters={"storage_hash__contains": "weights"})
    detail = api_mod.get_data_node_storage(42)
    assert captured["filters"][0] == {"storage_hash__contains": "weights"}
    assert captured["get"] == {"pk": 42, "filters": {}, "timeout": None}
    assert captured["jwt"] == ("acc", "ref")
    assert out == [
        {
            "id": 42,
            "storage_hash": "weights_daily",
            "source_class_name": "PortfolioWeights",
            "identifier": "weights_daily",
            "data_source": {"display_name": "Default DB", "class_type": "timescale_db"},
            "data_frequency_id": "1d",
        }
    ]
    assert detail["id"] == 42
    assert detail["storage_hash"] == "weights_daily"


def test_validate_project_name_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"})
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

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
        def validate_name(cls, *, project_name, timeout=None):
            captured["project_name"] = project_name
            captured["timeout"] = timeout
            return types.SimpleNamespace(
                model_dump=lambda mode="json": {
                    "project_name": project_name,
                    "available": False,
                    "reason": "A project with this name already exists in your organization.",
                    "normalized": {
                        "slugified_project_name": "rates-platform",
                        "project_library_name": "rates_platform",
                    },
                    "suggestions": ["Rates Platform 2", "Rates Platform 3"],
                }
            )

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.Project = FakeProject
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_tdag", fake_models)

    out = api_mod.validate_project_name(project_name="Rates Platform", timeout=25)

    assert captured["jwt"] == ("acc", "ref")
    assert captured["project_name"] == "Rates Platform"
    assert captured["timeout"] == 25
    assert out["available"] is False
    assert out["normalized"]["project_library_name"] == "rates_platform"


def test_data_node_storage_description_search_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"})
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

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

    class FakeDataNodeStorage:
        ROOT_URL = "https://old.test/orm/api/ts_manager/dynamic_table"

        @classmethod
        def description_search(
            cls,
            q,
            *,
            q_embedding=None,
            trigram_k=200,
            embed_k=200,
            w_trgm=0.65,
            w_emb=0.35,
            embedding_model="default",
            **filters,
        ):
            captured["search"] = {
                "q": q,
                "q_embedding": q_embedding,
                "trigram_k": trigram_k,
                "embed_k": embed_k,
                "w_trgm": w_trgm,
                "w_emb": w_emb,
                "embedding_model": embedding_model,
                "filters": filters,
            }
            return {
                "count": 1,
                "next": None,
                "previous": None,
                "results": [
                    types.SimpleNamespace(
                        model_dump=lambda *args, **kwargs: {
                            "id": 42,
                            "storage_hash": "weights_daily",
                            "identifier": "weights_daily",
                        }
                    )
                ],
            }

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.DataNodeStorage = FakeDataNodeStorage
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_tdag", fake_models)

    out = api_mod.data_node_storage_description_search(
        "portfolio weights",
        q_embedding=[0.1, 0.2],
        trigram_k=150,
        embed_k=120,
        w_trgm=0.7,
        w_emb=0.3,
        embedding_model="text-embedding-3-large",
        filters={"data_source__id": "2"},
    )

    assert captured["jwt"] == ("acc", "ref")
    assert captured["search"] == {
        "q": "portfolio weights",
        "q_embedding": [0.1, 0.2],
        "trigram_k": 150,
        "embed_k": 120,
        "w_trgm": 0.7,
        "w_emb": 0.3,
        "embedding_model": "text-embedding-3-large",
        "filters": {"data_source__id": "2"},
    }
    assert out == {
        "count": 1,
        "next": None,
        "previous": None,
        "results": [
            {
                "id": 42,
                "storage_hash": "weights_daily",
                "identifier": "weights_daily",
            }
        ],
    }


def test_data_node_storage_column_search_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"})
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

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

    class FakeDataNodeStorage:
        ROOT_URL = "https://old.test/orm/api/ts_manager/dynamic_table"

        @classmethod
        def column_search(cls, q, **filters):
            captured["search"] = {"q": q, "filters": filters}
            return [
                types.SimpleNamespace(
                    model_dump=lambda *args, **kwargs: {
                        "id": 43,
                        "storage_hash": "prices_daily",
                        "identifier": "prices_daily",
                    }
                )
            ]

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.DataNodeStorage = FakeDataNodeStorage
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_tdag", fake_models)

    out = api_mod.data_node_storage_column_search("close", filters={"storage_hash__contains": "prices"})

    assert captured["jwt"] == ("acc", "ref")
    assert captured["search"] == {"q": "close", "filters": {"storage_hash__contains": "prices"}}
    assert out == [
        {
            "id": 43,
            "storage_hash": "prices_daily",
            "identifier": "prices_daily",
        }
    ]


def test_refresh_data_node_storage_search_index_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"})
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

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

    class FakeDataNodeStorage:
        ROOT_URL = "https://old.test/orm/api/ts_manager/dynamic_table"

        @classmethod
        def get(cls, pk=None, timeout=None, **filters):
            captured["get"] = {"pk": pk, "filters": filters, "timeout": timeout}

            class _Storage:
                def refresh_table_search_index(self, *, timeout=None):
                    captured["refresh"] = {"timeout": timeout}
                    return {"status": "queued", "message": "refresh started"}

            return _Storage()

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.DataNodeStorage = FakeDataNodeStorage
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_tdag", fake_models)

    out = api_mod.refresh_data_node_storage_search_index(42, timeout=30)

    assert captured["jwt"] == ("acc", "ref")
    assert captured["get"] == {"pk": 42, "filters": {}, "timeout": 30}
    assert captured["refresh"] == {"timeout": 30}
    assert out == {"status": "queued", "message": "refresh started", "id": 42}


def test_delete_data_node_storage_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"})
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

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

    class FakeDataNodeStorage:
        ROOT_URL = "https://old.test/orm/api/ts_manager/dynamic_table"

        @classmethod
        def get(cls, pk=None, timeout=None, **filters):
            captured["get"] = {"pk": pk, "filters": filters, "timeout": timeout}

            class _Storage:
                id = pk

                def model_dump(self, mode="python"):
                    return {
                        "id": pk,
                        "storage_hash": "weights_daily",
                        "identifier": "weights_daily",
                    }

                def delete(
                    self,
                    *,
                    full_delete_selected=False,
                    full_delete_downstream_tables=False,
                    delete_with_no_table=False,
                    override_protection=False,
                    timeout=None,
                ):
                    captured["delete"] = {
                        "full_delete_selected": full_delete_selected,
                        "full_delete_downstream_tables": full_delete_downstream_tables,
                        "delete_with_no_table": delete_with_no_table,
                        "override_protection": override_protection,
                        "timeout": timeout,
                    }

            return _Storage()

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.DataNodeStorage = FakeDataNodeStorage
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_tdag", fake_models)

    out = api_mod.delete_data_node_storage(
        42,
        full_delete_selected=True,
        full_delete_downstream_tables=True,
        delete_with_no_table=False,
        override_protection=True,
        timeout=30,
    )
    assert captured["get"] == {"pk": 42, "filters": {}, "timeout": 30}
    assert captured["delete"] == {
        "full_delete_selected": True,
        "full_delete_downstream_tables": True,
        "delete_with_no_table": False,
        "override_protection": True,
        "timeout": 30,
    }
    assert captured["jwt"] == ("acc", "ref")
    assert out == {"id": 42, "storage_hash": "weights_daily", "identifier": "weights_daily"}


def test_list_data_node_storage_users_can_view_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"})
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

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

    class FakeDataNodeStorage:
        ROOT_URL = "https://old.test/orm/api/ts_manager/dynamic_table"

        @classmethod
        def get(cls, pk=None, timeout=None, **filters):
            captured["get"] = {"pk": pk, "filters": filters, "timeout": timeout}

            class _Storage:
                def can_view(self, timeout=None):
                    captured["can_view_timeout"] = timeout
                    return types.SimpleNamespace(
                        model_dump=lambda mode="python": {
                            "object_id": pk,
                            "object_type": "tdag.datanodestorage",
                            "access_level": "view",
                            "users": [
                                {
                                    "id": 8,
                                    "username": "viewer",
                                    "email": "viewer@example.com",
                                    "first_name": "View",
                                    "last_name": "User",
                                }
                            ],
                            "teams": [],
                        }
                    )

            return _Storage()

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.DataNodeStorage = FakeDataNodeStorage
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_tdag", fake_models)

    out = api_mod.list_data_node_storage_users_can_view(42, timeout=15)
    assert captured["get"] == {"pk": 42, "filters": {}, "timeout": 15}
    assert captured["can_view_timeout"] == 15
    assert captured["jwt"] == ("acc", "ref")
    assert out["users"][0]["username"] == "viewer"


def test_add_data_node_storage_user_to_edit_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"})
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

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

    class FakeDataNodeStorage:
        ROOT_URL = "https://old.test/orm/api/ts_manager/dynamic_table"

        @classmethod
        def get(cls, pk=None, timeout=None, **filters):
            captured["get"] = {"pk": pk, "filters": filters, "timeout": timeout}

            class _Storage:
                def add_to_edit(self, user_id, timeout=None):
                    captured["add_to_edit"] = {"user_id": user_id, "timeout": timeout}
                    return {
                        "ok": True,
                        "action": "add_to_edit",
                        "detail": "User now has explicit edit access.",
                        "object_id": pk,
                        "object_type": "tdag.datanodestorage",
                        "user": {"id": user_id, "username": "editor", "email": "editor@example.com"},
                        "explicit_can_view": True,
                        "explicit_can_edit": True,
                        "explicit_can_view_user_ids": [user_id],
                        "explicit_can_edit_user_ids": [user_id],
                    }

            return _Storage()

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.DataNodeStorage = FakeDataNodeStorage
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_tdag", fake_models)

    out = api_mod.add_data_node_storage_user_to_edit(42, 9, timeout=16)
    assert captured["get"] == {"pk": 42, "filters": {}, "timeout": 16}
    assert captured["add_to_edit"] == {"user_id": 9, "timeout": 16}
    assert captured["jwt"] == ("acc", "ref")
    assert out["action"] == "add_to_edit"


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

    listed = api_mod.list_market_asset_translation_tables(filters={"search": "prices"})
    assert captured["filters"][0] == {"search": "prices"}
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
    assert captured["headers_set"] == {"X-User-ID": "7", "Authorization": "Bearer acc"}
    assert captured["headers_seen"] == {"X-User-ID": "7", "Authorization": "Bearer acc"}
    assert captured["headers_reset"] == "token"
    assert out["username"] == "jose"


def test_list_org_project_names_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"})
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

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
        def get_org_project_names(cls, *, timeout=None):
            captured["timeout"] = timeout
            return ["alpha-research", "portfolio-live"]

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.Project = FakeProject
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_tdag", fake_models)

    out = api_mod.list_org_project_names(timeout=12)

    assert captured["jwt"] == ("acc", "ref")
    assert captured["timeout"] == 12
    assert out == ["alpha-research", "portfolio-live"]


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
    assert captured["payload"]["related_image_id"] == 77
    assert captured["env_project_id"] == "123"
    assert captured["jwt"] == ("acc", "ref")
    assert out == {"id": 91, "task_schedule": schedule}
    assert os.environ.get("MAIN_SEQUENCE_PROJECT_ID") is None


def test_schedule_batch_project_jobs_uses_client_model(cli_mod, monkeypatch, tmp_path):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    jobs_file = tmp_path / "scheduled_jobs.yaml"
    jobs_file.write_text("jobs: []\n", encoding="utf-8")

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
        def bulk_get_or_create(cls, *, yaml_file, project_id, strict=False, timeout=None):
            captured["payload"] = {
                "yaml_file": yaml_file,
                "project_id": project_id,
                "strict": strict,
                "timeout": timeout,
            }
            captured["env_project_id"] = os.environ.get("MAIN_SEQUENCE_PROJECT_ID")
            return [types.SimpleNamespace(model_dump=lambda: {"id": 91, "name": "Simulated Prices"})]

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_helpers.Job = FakeJob
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_helpers", fake_helpers)

    out = api_mod.schedule_batch_project_jobs(
        file_path=str(jobs_file),
        project_id=123,
        strict=True,
        timeout=45,
    )
    assert captured["payload"] == {
        "yaml_file": str(jobs_file),
        "project_id": 123,
        "strict": True,
        "timeout": 45,
    }
    assert captured["env_project_id"] == "123"
    assert captured["jwt"] == ("acc", "ref")
    assert out == [{"id": 91, "name": "Simulated Prices"}]
    assert os.environ.get("MAIN_SEQUENCE_PROJECT_ID") is None


def test_create_project_does_not_send_project_visible(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    class FakeResponse:
        ok = True
        status_code = 201
        headers = {"content-type": "application/json"}

        def json(self):
            return {"id": 321, "project_name": "demo-project"}

    def _fake_authed(method, api_path, body=None):
        captured["method"] = method
        captured["api_path"] = api_path
        captured["body"] = body
        return FakeResponse()

    monkeypatch.setattr(api_mod, "authed", _fake_authed)

    out = api_mod.create_project(
        project_name="demo-project",
        data_source_id=11,
        default_base_image_id=22,
        github_org_id=33,
        repository_branch="main",
        env_vars={"FOO": "bar"},
    )

    assert captured["method"] == "POST"
    assert captured["api_path"] == "/orm/api/pods/projects/"
    assert captured["body"] == {
        "project_name": "demo-project",
        "repository_branch": "main",
        "data_source_id": 11,
        "default_base_image_id": 22,
        "github_org_id": 33,
        "env_vars": [{"name": "FOO", "value": "bar"}],
    }
    assert "project_visible" not in captured["body"]
    assert out == {"id": 321, "project_name": "demo-project"}


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
                run_job=lambda timeout=None, command_args=None: (
                    captured.update(command_args=command_args)
                    or {
                    "id": 501,
                    "job": pk,
                    "status": "QUEUED",
                    "unique_identifier": "jobrun_abc123",
                    }
                )
            )

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_helpers.Job = FakeJob
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_helpers", fake_helpers)

    out = api_mod.run_project_job(91, command_args=["python", "-m", "jobs.daily"])
    assert captured["job_id_arg"] == 91
    assert captured["command_args"] == ["python", "-m", "jobs.daily"]
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

    out = api_mod.list_project_job_runs(job_id=91, filters={"status": "COMPLETED"})
    assert captured["filters"][0] == {"status": "COMPLETED", "job__id": [91]}
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
        lambda related_project_id, filters=None, timeout=None: [
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


def test_project_images_list_json(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "demo-123"
    target.mkdir(parents=True, exist_ok=True)
    (target / ".env").write_text("MAIN_SEQUENCE_PROJECT_ID=123\n", encoding="utf-8")

    monkeypatch.chdir(target)
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_project_images",
        lambda related_project_id, filters=None, timeout=None: [
            {
                "id": 77,
                "project_repo_hash": "abc123",
                "base_image": {"id": 22, "title": "Python 3.12"},
                "creation_date": "2026-04-10T12:00:00Z",
            }
        ],
    )

    result = runner.invoke(cli_mod.app, ["project", "images", "list", "--json"])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload[0]["id"] == 77
    assert payload[0]["project_repo_hash"] == "abc123"
    assert payload[0]["creation_date"] == "2026-04-10T12:00:00Z"


def test_project_images_list_rejects_reserved_filter(cli_mod, runner, monkeypatch):
    def _parse(model_ref, entries):
        return {"related_project__id__in": ["999"]}

    monkeypatch.setattr(cli_mod, "parse_cli_model_filters", _parse)

    result = runner.invoke(
        cli_mod.app,
        ["project", "images", "list", "123", "--filter", "related_project__id__in=999"],
    )
    assert result.exit_code == 1
    assert "cannot be overridden" in result.output


def test_project_images_delete_requires_confirmation(cli_mod, runner, monkeypatch):
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "get_project_image",
        lambda image_id, timeout=None: {
            "id": image_id,
            "project_repo_hash": "abc123",
            "base_image": {"id": 22, "title": "Python 3.12"},
            "is_ready": True,
        },
    )

    def _delete_project_image(image_id, timeout=None):
        captured["image_id"] = image_id
        return {
            "id": image_id,
            "project_repo_hash": "abc123",
            "base_image": {"id": 22, "title": "Python 3.12"},
            "is_ready": True,
        }

    monkeypatch.setattr(cli_mod, "delete_project_image", _delete_project_image)

    result = runner.invoke(cli_mod.app, ["project", "images", "delete", "94"], input="y\n")
    assert result.exit_code == 0
    assert captured["image_id"] == 94
    assert "Project Image Delete Preview" in result.output
    assert "Delete project image 94?" in result.output
    assert "Project image deleted: id=94" in result.output


def test_project_jobs_list_defaults_to_env_project_id(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "demo-123"
    target.mkdir(parents=True, exist_ok=True)
    (target / ".env").write_text("MAIN_SEQUENCE_PROJECT_ID=123\n", encoding="utf-8")

    monkeypatch.chdir(target)
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_project_jobs",
        lambda project_id, filters=None, timeout=None: [
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


def test_project_jobs_list_show_filters_mentions_project_scope(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "build_cli_model_filter_rows", lambda model_ref: [])

    result = runner.invoke(cli_mod.app, ["project", "jobs", "list", "--show-filters"])
    assert result.exit_code == 0
    assert "No additional model filters exposed by Project Jobs." in result.output
    assert "Always Applied Filters" in result.output
    assert "project" in result.output


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

    def _list_project_resources(project_id, repo_commit_sha, resource_type=None, filters=None, timeout=None):
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


def test_project_project_resource_list_passes_extra_filters(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "demo-123"
    target.mkdir(parents=True, exist_ok=True)
    (target / ".env").write_text("MAIN_SEQUENCE_PROJECT_ID=123\n", encoding="utf-8")
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(cli_mod, "_get_remote_branch_head_commit", lambda project_dir: ("origin/main", "abc123"))

    def _parse(model_ref, entries):
        captured["entries"] = list(entries or [])
        return {"id__in": ["301"]}

    def _list_project_resources(project_id, repo_commit_sha, resource_type=None, filters=None, timeout=None):
        captured["filters"] = filters
        return []

    monkeypatch.setattr(cli_mod, "parse_cli_model_filters", _parse)
    monkeypatch.setattr(cli_mod, "list_project_resources", _list_project_resources)

    result = runner.invoke(
        cli_mod.app,
        ["project", "project_resource", "list", "--path", str(target), "--filter", "id__in=301"],
    )
    assert result.exit_code == 0
    assert captured["entries"] == ["id__in=301"]
    assert captured["filters"] == {"id__in": ["301"]}


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


def test_project_project_resource_create_fastapi_filters_resources_by_selected_image(cli_mod, runner, monkeypatch, tmp_path):
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
                "id": 382,
                "name": "tutorial_api",
                "resource_type": "fastapi",
                "path": "src/apis/tutorial_api/main.py",
                "filesize": 4096,
                "last_modified": "2026-04-05T10:30:00Z",
            }
        ]

    monkeypatch.setattr(cli_mod, "list_project_resources", _list_project_resources)

    def _create_release(**kwargs):
        captured["create_release"] = kwargs
        return {
            "id": 503,
            "resource": kwargs["resource_id"],
            "related_image": kwargs["related_image_id"],
            "cpu_request": kwargs["cpu_request"],
            "memory_request": kwargs["memory_request"],
            "spot": kwargs["spot"],
        }

    monkeypatch.setattr(cli_mod, "create_project_resource_release", _create_release)

    result = runner.invoke(
        cli_mod.app,
        ["project", "project_resource", "create_fastapi"],
        input="94\n382\n",
    )
    assert result.exit_code == 0
    assert captured["project_id"] == 123
    assert captured["repo_commit_sha"] == "sha-94"
    assert captured["resource_type"] == "fastapi"
    assert captured["create_release"]["release_kind"] == "fastapi"
    assert captured["create_release"]["related_image_id"] == 94
    assert captured["create_release"]["resource_id"] == 382
    assert captured["create_release"]["cpu_request"] == "0.25"
    assert captured["create_release"]["memory_request"] == "0.5"
    assert captured["create_release"]["spot"] is False
    assert "Using defaults: cpu_request=0.25, memory_request=0.5, spot=false." in result.output
    assert "Project resource release created: id=503" in result.output


def test_project_project_resource_delete_dashboard_requires_confirmation(cli_mod, runner, monkeypatch):
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "get_resource_release",
        lambda release_id, expected_release_kind=None, timeout=None: {
            "id": release_id,
            "release_kind": expected_release_kind,
            "subdomain": "analytics-123",
            "resource": 381,
            "related_image": 94,
        },
    )

    def _delete_resource_release(release_id, expected_release_kind=None, timeout=None):
        captured["release_id"] = release_id
        captured["expected_release_kind"] = expected_release_kind
        return {
            "id": release_id,
            "release_kind": expected_release_kind,
            "subdomain": "analytics-123",
            "resource": 381,
            "related_image": 94,
        }

    monkeypatch.setattr(cli_mod, "delete_resource_release", _delete_resource_release)

    result = runner.invoke(
        cli_mod.app,
        ["project", "project_resource", "delete_dashboard", "501"],
        input="y\n",
    )
    assert result.exit_code == 0
    assert captured["release_id"] == 501
    assert captured["expected_release_kind"] == "streamlit_dashboard"
    assert "Project Resource Release Delete Preview" in result.output
    assert "Delete dashboard release 501?" in result.output
    assert "Project resource release deleted: id=501" in result.output


def test_project_project_resource_delete_agent_yes_skips_confirmation(cli_mod, runner, monkeypatch):
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "get_resource_release",
        lambda release_id, expected_release_kind=None, timeout=None: {
            "id": release_id,
            "release_kind": expected_release_kind,
            "subdomain": "agent-123",
            "resource": 390,
            "related_image": 95,
        },
    )

    def _delete_resource_release(release_id, expected_release_kind=None, timeout=None):
        captured["release_id"] = release_id
        captured["expected_release_kind"] = expected_release_kind
        return {
            "id": release_id,
            "release_kind": expected_release_kind,
            "subdomain": "agent-123",
            "resource": 390,
            "related_image": 95,
        }

    monkeypatch.setattr(cli_mod, "delete_resource_release", _delete_resource_release)

    result = runner.invoke(
        cli_mod.app,
        ["project", "project_resource", "delete_agent", "601", "--yes"],
    )
    assert result.exit_code == 0
    assert captured["release_id"] == 601
    assert captured["expected_release_kind"] == "agent"
    assert "Delete agent release 601?" not in result.output
    assert "Project resource release deleted: id=601" in result.output


def test_project_project_resource_delete_fastapi_requires_confirmation(cli_mod, runner, monkeypatch):
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "get_resource_release",
        lambda release_id, expected_release_kind=None, timeout=None: {
            "id": release_id,
            "release_kind": expected_release_kind,
            "subdomain": "api-123",
            "resource": 382,
            "related_image": 94,
        },
    )

    def _delete_resource_release(release_id, expected_release_kind=None, timeout=None):
        captured["release_id"] = release_id
        captured["expected_release_kind"] = expected_release_kind
        return {
            "id": release_id,
            "release_kind": expected_release_kind,
            "subdomain": "api-123",
            "resource": 382,
            "related_image": 94,
        }

    monkeypatch.setattr(cli_mod, "delete_resource_release", _delete_resource_release)

    result = runner.invoke(
        cli_mod.app,
        ["project", "project_resource", "delete_fastapi", "701"],
        input="y\n",
    )
    assert result.exit_code == 0
    assert captured["release_id"] == 701
    assert captured["expected_release_kind"] == "fastapi"
    assert "Delete FastAPI release 701?" in result.output
    assert "Project resource release deleted: id=701" in result.output


def test_markets_portfolios_list(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_market_portfolios",
        lambda filters=None, timeout=None: [
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


def test_markets_portfolios_list_show_filters(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(
        cli_mod,
        "build_cli_model_filter_rows",
        lambda model_ref: [["id__in", "in", "comma-separated integer IDs", "id"]],
    )

    result = runner.invoke(cli_mod.app, ["markets", "portfolios", "list", "--show-filters"])
    assert result.exit_code == 0
    assert "Markets Portfolios Filters" in result.output
    assert "id__in" in result.output
    assert "integer IDs" in result.output


def test_agent_list(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_agents",
        lambda timeout=None, filters=None: [
            {
                "id": 12,
                "name": "Research Copilot",
                "status": "active",
                "labels": ["research", "desk"],
                "llm_provider": "openai",
                "llm_model": "gpt-5.4",
                "engine_name": "codex",
                "last_run_at": "2026-04-10T09:15:00Z",
            }
        ],
    )

    result = runner.invoke(cli_mod.app, ["agent", "list"])
    assert result.exit_code == 0
    assert "Agents" in result.output
    assert "Research" in result.output
    assert "Copilot" in result.output
    assert "Total agents: 1" in result.output


def test_agent_list_json(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_agents",
        lambda timeout=None, filters=None: [
            {
                "id": 12,
                "agent_unique_id": "research-copilot",
                "name": "Research Copilot",
                "status": "active",
                "labels": ["research", "desk"],
                "llm_provider": "openai",
                "llm_model": "gpt-5.4",
                "engine_name": "codex",
                "last_run_at": "2026-04-10T09:15:00Z",
            }
        ],
    )

    result = runner.invoke(cli_mod.app, ["agent", "list", "--json"])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload[0]["id"] == 12
    assert payload[0]["agent_unique_id"] == "research-copilot"
    assert payload[0]["llm_model"] == "gpt-5.4"


def test_agent_create_parses_json_fields(cli_mod, runner, monkeypatch):
    captured = {}
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _create(**kwargs):
        captured.update(kwargs)
        return {
            "id": 12,
            "name": kwargs["name"],
            "status": kwargs.get("status") or "draft",
            "labels": kwargs.get("labels") or [],
            "llm_provider": kwargs.get("llm_provider") or "",
            "llm_model": kwargs.get("llm_model") or "",
            "engine_name": kwargs.get("engine_name") or "",
        }

    monkeypatch.setattr(cli_mod, "create_agent", _create)

    result = runner.invoke(
        cli_mod.app,
        [
            "agent",
            "create",
            "Research Copilot",
            "--agent-unique-id",
            "research-copilot",
            "--description",
            "Desk agent",
            "--status",
            "active",
            "--label",
            "research,desk",
            "--llm-provider",
            "openai",
            "--llm-model",
            "gpt-5.4",
            "--engine-name",
            "codex",
            "--runtime-config",
            '{"temperature":0}',
            "--configuration",
            '{"mode":"analysis"}',
            "--metadata",
            '{"owner":"quant"}',
        ],
    )
    assert result.exit_code == 0
    assert captured["name"] == "Research Copilot"
    assert captured["agent_unique_id"] == "research-copilot"
    assert captured["status"] == "active"
    assert captured["labels"] == ["research", "desk"]
    assert captured["runtime_config"] == {"temperature": 0}
    assert captured["configuration"] == {"mode": "analysis"}
    assert captured["metadata"] == {"owner": "quant"}
    assert "Agent created: Research Copilot" in result.output


def test_agent_get_or_create_parses_json_fields(cli_mod, runner, monkeypatch):
    captured = {}
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _get_or_create(**kwargs):
        captured.update(kwargs)
        return {
            "id": 12,
            "name": kwargs["name"],
            "agent_unique_id": kwargs["agent_unique_id"],
            "status": kwargs.get("status") or "draft",
            "labels": kwargs.get("labels") or [],
            "llm_provider": kwargs.get("llm_provider") or "",
            "llm_model": kwargs.get("llm_model") or "",
            "engine_name": kwargs.get("engine_name") or "",
        }

    monkeypatch.setattr(cli_mod, "get_or_create_agent", _get_or_create)

    result = runner.invoke(
        cli_mod.app,
        [
            "agent",
            "get_or_create",
            "Research Copilot",
            "--agent-unique-id",
            "research-copilot",
            "--description",
            "Desk agent",
            "--status",
            "active",
            "--label",
            "research,desk",
            "--llm-provider",
            "openai",
            "--llm-model",
            "gpt-5.4",
            "--engine-name",
            "codex",
            "--runtime-config",
            '{"temperature":0}',
            "--configuration",
            '{"mode":"analysis"}',
            "--metadata",
            '{"owner":"quant"}',
        ],
    )
    assert result.exit_code == 0
    assert captured["name"] == "Research Copilot"
    assert captured["agent_unique_id"] == "research-copilot"
    assert captured["status"] == "active"
    assert captured["labels"] == ["research", "desk"]
    assert captured["runtime_config"] == {"temperature": 0}
    assert captured["configuration"] == {"mode": "analysis"}
    assert captured["metadata"] == {"owner": "quant"}
    assert "Agent resolved via get_or_create: Research Copilot" in result.output


def test_agent_start_new_session(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "start_agent_new_session",
        lambda agent_id, timeout=None: {
            "id": 801,
            "agent": {"id": agent_id, "name": "Research Copilot"},
            "status": "pending",
            "started_at": "2026-04-11T09:15:00Z",
            "llm_provider": "openai",
            "llm_model": "gpt-5.4",
            "engine_name": "codex",
            "thread_id": "thread-123",
            "runtime_config_snapshot": {"temperature": 0},
            "session_metadata": {"origin": "cli"},
        },
    )

    result = runner.invoke(cli_mod.app, ["agent", "start_new_session", "12"])
    assert result.exit_code == 0
    assert "Agent session started: agent_id=12" in result.output
    assert "Agent Session Details" in result.output
    assert "thread-123" in result.output
    assert "temperature" in result.output


def test_agent_get_latest_session(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "get_agent_latest_session",
        lambda agent_id, timeout=None: {
            "id": 802,
            "agent": {"id": agent_id, "name": "Research Copilot"},
            "status": "completed",
            "started_at": "2026-04-11T09:15:00Z",
            "ended_at": "2026-04-11T09:16:00Z",
            "llm_provider": "openai",
            "llm_model": "gpt-5.4",
            "engine_name": "codex",
            "input_text": "Summarize rates moves",
            "output_text": "Bunds rallied 4bp.",
            "usage_summary": {"prompt_tokens": 100},
            "session_metadata": {"origin": "cli"},
        },
    )

    result = runner.invoke(cli_mod.app, ["agent", "get_latest_session", "12"])
    assert result.exit_code == 0
    assert "Agent Session Details" in result.output
    assert "Summarize rates moves" in result.output
    assert "Bunds rallied 4bp." in result.output
    assert "prompt_tokens" in result.output


def test_agent_session_detail(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "get_agent_session",
        lambda agent_session_id, timeout=None: {
            "id": agent_session_id,
            "agent": {"id": 12, "name": "Research Copilot"},
            "status": "completed",
            "started_at": "2026-04-11T09:15:00Z",
            "ended_at": "2026-04-11T09:16:00Z",
            "llm_provider": "openai",
            "llm_model": "gpt-5.4",
            "engine_name": "codex",
            "triggered_by_user": {"id": 7, "username": "jose"},
            "input_text": "Summarize rates moves",
            "output_text": "Bunds rallied 4bp.",
            "usage_summary": {"prompt_tokens": 100},
            "session_metadata": {"origin": "cli"},
        },
    )

    result = runner.invoke(cli_mod.app, ["agent", "session", "detail", "803"])
    assert result.exit_code == 0
    assert "Agent Session Details" in result.output
    assert "Summarize rates moves" in result.output
    assert "Bunds rallied 4bp." in result.output
    assert "prompt_tokens" in result.output


def test_agent_delete_requires_typed_verification(cli_mod, runner, monkeypatch):
    captured = {}
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "get_agent",
        lambda agent_id, timeout=None: {
            "id": agent_id,
            "name": "Research Copilot",
            "status": "active",
            "labels": ["research"],
        },
    )

    def _delete(agent_id, timeout=None):
        captured["agent_id"] = agent_id
        captured["timeout"] = timeout
        return {
            "id": agent_id,
            "name": "Research Copilot",
            "status": "active",
            "labels": ["research"],
        }

    monkeypatch.setattr(cli_mod, "delete_agent", _delete)

    result = runner.invoke(
        cli_mod.app,
        ["agent", "delete", "12"],
        input="Research Copilot\n",
    )
    assert result.exit_code == 0
    assert "Agent Delete Preview" in result.output
    assert "Type agent name 'Research Copilot' to confirm deletion" in result.output
    assert captured["agent_id"] == 12
    assert "Agent deleted: id=12" in result.output


def test_agent_can_edit(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_agent_users_can_edit",
        lambda agent_id, timeout=None: {
            "access_level": "edit",
            "users": [
                {
                    "id": 9,
                    "username": "editor",
                    "email": "editor@example.com",
                    "first_name": "Edit",
                    "last_name": "User",
                }
            ],
            "teams": [{"id": 3, "name": "Research", "description": "Core team", "member_count": 6}],
        },
    )

    result = runner.invoke(cli_mod.app, ["agent", "can_edit", "12"])
    assert result.exit_code == 0
    assert "Agent Users Who Can Edit" in result.output
    assert "Agent Teams Who Can Edit" in result.output
    assert "editor@example.com" in result.output
    assert "Total users who can edit: 1" in result.output
    assert "Total teams who can edit: 1" in result.output


def test_agent_add_team_to_edit(cli_mod, runner, monkeypatch):
    captured = {}
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _add(agent_id, team_id, timeout=None):
        captured["agent_id"] = agent_id
        captured["team_id"] = team_id
        captured["timeout"] = timeout
        return {
            "ok": True,
            "action": "add_team_to_edit",
            "detail": "Team now has explicit edit access.",
            "object_id": agent_id,
            "object_type": "agent.agent",
            "team": {
                "id": team_id,
                "name": "Research",
                "description": "Core team",
            },
            "explicit_can_view": True,
            "explicit_can_edit": True,
            "explicit_can_view_team_ids": [team_id],
            "explicit_can_edit_team_ids": [team_id],
        }

    monkeypatch.setattr(cli_mod, "add_agent_team_to_edit", _add)

    result = runner.invoke(cli_mod.app, ["agent", "add_team_to_edit", "12", "3"])
    assert result.exit_code == 0
    assert captured == {"agent_id": 12, "team_id": 3, "timeout": None}
    assert "Agent add_team_to_edit completed." in result.output


def test_agent_run_list(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_agent_runs",
        lambda timeout=None, filters=None: [
            {
                "id": 501,
                "agent": {"id": 12, "name": "Research Copilot"},
                "status": "running",
                "started_at": "2026-04-10T09:15:00Z",
                "ended_at": None,
                "llm_provider": "openai",
                "llm_model": "gpt-5.4",
                "engine_name": "codex",
            }
        ],
    )

    result = runner.invoke(cli_mod.app, ["agent", "run", "list"])
    assert result.exit_code == 0
    assert "Agent Runs" in result.output
    assert "Research" in result.output
    assert "Copilot" in result.output
    assert "Total agent runs: 1" in result.output


def test_agent_run_detail(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "get_agent_run",
        lambda agent_run_id, timeout=None: {
            "id": agent_run_id,
            "agent": {"id": 12, "name": "Research Copilot"},
            "status": "completed",
            "started_at": "2026-04-10T09:15:00Z",
            "ended_at": "2026-04-10T09:16:00Z",
            "llm_provider": "openai",
            "llm_model": "gpt-5.4",
            "engine_name": "codex",
            "triggered_by_user": {"id": 7, "username": "jose"},
            "input_text": "Summarize rates moves",
            "output_text": "Bunds rallied 4bp.",
            "runtime_config_snapshot": {"temperature": 0},
            "usage_summary": {"prompt_tokens": 100},
            "run_metadata": {"origin": "cli"},
        },
    )

    result = runner.invoke(cli_mod.app, ["agent", "run", "detail", "501"])
    assert result.exit_code == 0
    assert "Agent Run Details" in result.output
    assert "Summarize rates moves" in result.output
    assert "Bunds rallied 4bp." in result.output
    assert "prompt_tokens" in result.output


def test_constants_list(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_constants",
        lambda filters=None, timeout=None: [
            {
                "id": 7,
                "name": "ASSETS__MASTER",
                "value": {"source": "bbg"},
            }
        ],
    )

    result = runner.invoke(cli_mod.app, ["constants", "list"])
    assert result.exit_code == 0
    assert "Constants" in result.output
    assert "ASSETS__MASTER" in result.output
    assert "ASSETS" in result.output
    assert "Total constants: 1" in result.output


def test_constants_list_passes_cli_filters(cli_mod, runner, monkeypatch):
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _parse(model_ref, entries):
        captured["entries"] = list(entries or [])
        return {"name__in": ["ASSETS__MASTER", "APP__MODE"]}

    def _list(timeout=None, filters=None):
        captured["filters"] = filters
        return []

    monkeypatch.setattr(cli_mod, "parse_cli_model_filters", _parse)
    monkeypatch.setattr(cli_mod, "list_constants", _list)

    result = runner.invoke(
        cli_mod.app,
        ["constants", "list", "--filter", "name__in=ASSETS__MASTER,APP__MODE"],
    )
    assert result.exit_code == 0
    assert captured["entries"] == ["name__in=ASSETS__MASTER,APP__MODE"]
    assert captured["filters"] == {"name__in": ["ASSETS__MASTER", "APP__MODE"]}


def test_constants_create_parses_json_value(cli_mod, runner, monkeypatch):
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _create(*, name, value, timeout=None):
        captured["name"] = name
        captured["value"] = value
        captured["timeout"] = timeout
        return {"id": 7, "name": name, "value": value}

    monkeypatch.setattr(cli_mod, "create_constant", _create)

    result = runner.invoke(
        cli_mod.app,
        ["constants", "create", "ASSETS__MASTER", '{"source":"bbg"}'],
    )
    assert result.exit_code == 0
    assert captured["name"] == "ASSETS__MASTER"
    assert captured["value"] == {"source": "bbg"}
    assert "Constant created: ASSETS__MASTER" in result.output
    assert "Created Constant" in result.output
    assert "ASSETS" in result.output


def test_constants_delete_requires_typed_verification(cli_mod, runner, monkeypatch):
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "get_constant",
        lambda constant_id, timeout=None: {
            "id": constant_id,
            "name": "ASSETS__MASTER",
            "value": {"source": "bbg"},
        },
    )

    def _delete(constant_id, timeout=None):
        captured["constant_id"] = constant_id
        captured["timeout"] = timeout
        return {
            "id": constant_id,
            "name": "ASSETS__MASTER",
            "value": {"source": "bbg"},
        }

    monkeypatch.setattr(cli_mod, "delete_constant", _delete)

    result = runner.invoke(
        cli_mod.app,
        ["constants", "delete", "7"],
        input="ASSETS__MASTER\n",
    )
    assert result.exit_code == 0
    assert "Constant Delete Preview" in result.output
    assert "Type constant name 'ASSETS__MASTER' to confirm deletion" in result.output
    assert captured["constant_id"] == 7
    assert "Constant deleted: id=7" in result.output


def test_constants_can_edit(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_constant_users_can_edit",
        lambda constant_id, timeout=None: {
            "object_id": constant_id,
            "object_type": "tdag.constant",
            "access_level": "edit",
            "users": [
                {
                    "id": 9,
                    "username": "editor",
                    "email": "editor@example.com",
                    "first_name": "Edit",
                    "last_name": "User",
                }
            ],
            "teams": [{"id": 3, "name": "Research", "description": "Core team", "member_count": 6}],
        },
    )

    result = runner.invoke(cli_mod.app, ["constants", "can_edit", "7"])
    assert result.exit_code == 0
    assert "Constant Users Who Can Edit" in result.output
    assert "Constant Teams Who Can Edit" in result.output
    assert "editor" in result.output
    assert "editor@example.com" in result.output
    assert "Total users who can edit: 1" in result.output
    assert "Total teams who can edit: 1" in result.output


def test_constants_add_to_edit(cli_mod, runner, monkeypatch):
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _add(constant_id, user_id, timeout=None):
        captured["constant_id"] = constant_id
        captured["user_id"] = user_id
        captured["timeout"] = timeout
        return {
            "ok": True,
            "action": "add_to_edit",
            "detail": "User now has explicit edit access.",
            "object_id": constant_id,
            "object_type": "tdag.constant",
            "user": {
                "id": user_id,
                "username": "editor",
                "email": "editor@example.com",
                "first_name": "Edit",
                "last_name": "User",
            },
            "explicit_can_view": True,
            "explicit_can_edit": True,
            "explicit_can_view_user_ids": [user_id],
            "explicit_can_edit_user_ids": [user_id],
        }

    monkeypatch.setattr(cli_mod, "add_constant_user_to_edit", _add)

    result = runner.invoke(cli_mod.app, ["constants", "add_to_edit", "7", "9"])
    assert result.exit_code == 0
    assert captured == {"constant_id": 7, "user_id": 9, "timeout": None}
    assert "Constant add_to_edit completed." in result.output
    assert "Constant Sharing Update" in result.output
    assert "editor@example.com" in result.output


def test_secrets_list(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_secrets",
        lambda filters=None, timeout=None: [
            {
                "id": 8,
                "name": "API_KEY",
            }
        ],
    )

    result = runner.invoke(cli_mod.app, ["secrets", "list"])
    assert result.exit_code == 0
    assert "Secrets" in result.output
    assert "API_KEY" in result.output
    assert "Total secrets: 1" in result.output


def test_secrets_list_passes_cli_filters(cli_mod, runner, monkeypatch):
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _parse(model_ref, entries):
        captured["entries"] = list(entries or [])
        return {"name__in": ["API_KEY", "DB_PASSWORD"]}

    def _list(timeout=None, filters=None):
        captured["filters"] = filters
        return []

    monkeypatch.setattr(cli_mod, "parse_cli_model_filters", _parse)
    monkeypatch.setattr(cli_mod, "list_secrets", _list)

    result = runner.invoke(
        cli_mod.app,
        ["secrets", "list", "--filter", "name__in=API_KEY,DB_PASSWORD"],
    )
    assert result.exit_code == 0
    assert captured["entries"] == ["name__in=API_KEY,DB_PASSWORD"]
    assert captured["filters"] == {"name__in": ["API_KEY", "DB_PASSWORD"]}


def test_secrets_create_hides_value_in_output(cli_mod, runner, monkeypatch):
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _create(*, name, value, timeout=None):
        captured["name"] = name
        captured["value"] = value
        captured["timeout"] = timeout
        return {"id": 8, "name": name}

    monkeypatch.setattr(cli_mod, "create_secret", _create)

    result = runner.invoke(
        cli_mod.app,
        ["secrets", "create", "API_KEY", "super-secret"],
    )
    assert result.exit_code == 0
    assert captured["name"] == "API_KEY"
    assert captured["value"] == "super-secret"
    assert "Secret created: API_KEY" in result.output
    assert "Created Secret" in result.output
    assert "super-secret" not in result.output


def test_secrets_delete_requires_typed_verification(cli_mod, runner, monkeypatch):
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "get_secret",
        lambda secret_id, timeout=None: {
            "id": secret_id,
            "name": "API_KEY",
        },
    )

    def _delete(secret_id, timeout=None):
        captured["secret_id"] = secret_id
        captured["timeout"] = timeout
        return {
            "id": secret_id,
            "name": "API_KEY",
        }

    monkeypatch.setattr(cli_mod, "delete_secret", _delete)

    result = runner.invoke(
        cli_mod.app,
        ["secrets", "delete", "8"],
        input="API_KEY\n",
    )
    assert result.exit_code == 0
    assert "Secret Delete Preview" in result.output
    assert "Type secret name 'API_KEY' to confirm deletion" in result.output
    assert captured["secret_id"] == 8
    assert "Secret deleted: id=8" in result.output


def test_secrets_can_view(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_secret_users_can_view",
        lambda secret_id, timeout=None: {
            "object_id": secret_id,
            "object_type": "tdag.secret",
            "access_level": "view",
            "users": [
                {
                    "id": 11,
                    "username": "viewer",
                    "email": "viewer@example.com",
                    "first_name": "View",
                    "last_name": "User",
                }
            ],
            "teams": [],
        },
    )

    result = runner.invoke(cli_mod.app, ["secrets", "can_view", "8"])
    assert result.exit_code == 0
    assert "Secret Users Who Can View" in result.output
    assert "viewer@example.com" in result.output
    assert "Total users who can view: 1" in result.output
    assert "Total teams who can view: 0" in result.output


def test_secrets_add_to_edit(cli_mod, runner, monkeypatch):
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _add(secret_id, user_id, timeout=None):
        captured["secret_id"] = secret_id
        captured["user_id"] = user_id
        captured["timeout"] = timeout
        return {
            "ok": True,
            "action": "add_to_edit",
            "detail": "User now has explicit edit access.",
            "object_id": secret_id,
            "object_type": "tdag.secret",
            "user": {
                "id": user_id,
                "username": "editor",
                "email": "editor@example.com",
                "first_name": "Edit",
                "last_name": "User",
            },
            "explicit_can_view": True,
            "explicit_can_edit": True,
            "explicit_can_view_user_ids": [user_id],
            "explicit_can_edit_user_ids": [user_id],
        }

    monkeypatch.setattr(cli_mod, "add_secret_user_to_edit", _add)

    result = runner.invoke(cli_mod.app, ["secrets", "add_to_edit", "8", "11"])
    assert result.exit_code == 0
    assert captured == {"secret_id": 8, "user_id": 11, "timeout": None}
    assert "Secret add_to_edit completed." in result.output
    assert "Secret Sharing Update" in result.output
    assert "editor@example.com" in result.output


def test_data_node_storage_list(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_data_node_storages",
        lambda filters=None, timeout=None: [
            {
                "id": 42,
                "storage_hash": "weights_daily",
                "source_class_name": "PortfolioWeights",
                "identifier": "weights_daily",
                "namespace": "pytest_weights",
                "data_source": {"display_name": "Default DB", "class_type": "timescale_db"},
                "data_frequency_id": "1d",
            }
        ],
    )

    result = runner.invoke(cli_mod.app, ["data-node", "list"])
    assert result.exit_code == 0
    assert "Data Node Storages" in result.output
    assert "weights_d" in result.output
    assert "Portfolio" in result.output
    assert "Weights" in result.output
    assert "Namespace" in result.output
    assert "pytest_we" in result.output
    assert "Default DB" in result.output
    assert "Total data node storages: 1" in result.output


def test_data_node_storage_list_forwards_namespace_filter(cli_mod, runner, monkeypatch):
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _fake_list_data_node_storages(filters=None, timeout=None):
        captured["timeout"] = timeout
        captured["filters"] = filters
        return []

    monkeypatch.setattr(cli_mod, "list_data_node_storages", _fake_list_data_node_storages)

    result = runner.invoke(
        cli_mod.app,
        ["data-node", "list", "--filter", "namespace=pytest_weights"],
    )

    assert result.exit_code == 0
    assert captured == {"timeout": None, "filters": {"namespace": "pytest_weights"}}


def test_project_validate_name_cmd(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "validate_project_name",
        lambda project_name, timeout=None: {
            "project_name": project_name,
            "available": False,
            "reason": "A project with this name already exists in your organization.",
            "normalized": {
                "slugified_project_name": "rates-platform",
                "project_library_name": "rates_platform",
            },
            "suggestions": ["Rates Platform 2", "Rates Platform 3"],
        },
    )

    result = runner.invoke(cli_mod.app, ["project", "validate-name", "Rates Platform"])
    assert result.exit_code == 1
    assert "Project Name Validation" in result.output
    assert "rates-platform" in result.output
    assert "rates_platform" in result.output
    assert "Rates Platform 2" in result.output
    assert "Rates Platform 3" in result.output


def test_data_node_storage_list_passes_cli_filters(cli_mod, runner, monkeypatch):
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _parse(model_ref, entries):
        captured["entries"] = list(entries or [])
        return {"id__in": ["42", "43"]}

    def _list(timeout=None, filters=None):
        captured["filters"] = filters
        return []

    monkeypatch.setattr(cli_mod, "parse_cli_model_filters", _parse)
    monkeypatch.setattr(cli_mod, "list_data_node_storages", _list)

    result = runner.invoke(cli_mod.app, ["data-node", "list", "--filter", "id__in=42,43"])
    assert result.exit_code == 0
    assert captured["entries"] == ["id__in=42,43"]
    assert captured["filters"] == {"id__in": ["42", "43"]}


def test_data_node_storage_search_supports_data_source_id_option(cli_mod, runner, monkeypatch):
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(cli_mod, "parse_cli_model_filters", lambda model_ref, entries: {})

    def _description(
        q,
        *,
        q_embedding=None,
        trigram_k=200,
        embed_k=200,
        w_trgm=0.65,
        w_emb=0.35,
        embedding_model="default",
        filters=None,
    ):
        captured["filters"] = filters
        return {"count": 0, "next": None, "previous": None, "results": []}

    monkeypatch.setattr(cli_mod, "data_node_storage_description_search", _description)
    monkeypatch.setattr(cli_mod, "data_node_storage_column_search", lambda q, *, filters=None: [])

    result = runner.invoke(
        cli_mod.app,
        ["data_node", "search", "close price", "--mode", "description", "--data-source-id", "2"],
    )
    assert result.exit_code == 0
    assert captured["filters"] == {"data_source__id": "2"}


def test_data_node_storage_search_rejects_conflicting_data_source_filters(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _parse(model_ref, entries):
        return {"data_source__id": "9"}

    monkeypatch.setattr(cli_mod, "parse_cli_model_filters", _parse)

    result = runner.invoke(
        cli_mod.app,
        [
            "data-node",
            "search",
            "close price",
            "--data-source-id",
            "2",
            "--filter",
            "data_source__id=9",
        ],
    )
    assert result.exit_code == 1
    assert "Do not pass both `--data-source-id` and `--filter data_source__id=...`." in result.output


def test_data_node_storage_description_search(cli_mod, runner, monkeypatch):
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _parse(model_ref, entries):
        captured["entries"] = list(entries or [])
        return {}

    def _search(
        q,
        *,
        q_embedding=None,
        trigram_k=200,
        embed_k=200,
        w_trgm=0.65,
        w_emb=0.35,
        embedding_model="default",
        filters=None,
    ):
        captured["search"] = {
            "q": q,
            "q_embedding": q_embedding,
            "trigram_k": trigram_k,
            "embed_k": embed_k,
            "w_trgm": w_trgm,
            "w_emb": w_emb,
            "embedding_model": embedding_model,
            "filters": filters,
        }
        return {
            "count": 3,
            "next": "https://backend.test/page/2",
            "previous": None,
            "results": [
                {
                    "id": 42,
                    "storage_hash": "weights_daily",
                    "source_class_name": "PortfolioWeights",
                    "identifier": "weights_daily",
                    "data_source": {"display_name": "Default DB", "class_type": "timescale_db"},
                    "data_frequency_id": "1d",
                }
            ],
        }

    monkeypatch.setattr(cli_mod, "parse_cli_model_filters", _parse)
    monkeypatch.setattr(cli_mod, "data_node_storage_description_search", _search)

    result = runner.invoke(
        cli_mod.app,
        [
            "data-node",
            "description-search",
            "portfolio weights",
            "--data-source-id",
            "2",
            "--q-embedding",
            "0.1,0.2",
            "--trigram-k",
            "150",
            "--embed-k",
            "180",
            "--w-trgm",
            "0.7",
            "--w-emb",
            "0.3",
            "--embedding-model",
            "text-embedding-3-large",
        ],
    )
    assert result.exit_code == 0
    assert captured["entries"] == []
    assert captured["search"] == {
        "q": "portfolio weights",
        "q_embedding": [0.1, 0.2],
        "trigram_k": 150,
        "embed_k": 180,
        "w_trgm": 0.7,
        "w_emb": 0.3,
        "embedding_model": "text-embedding-3-large",
        "filters": {"data_source__id": "2"},
    }
    assert "Description Matches" in result.output
    assert "weights_daily" in result.output
    assert "Pagination" in result.output
    assert "Count" in result.output


def test_data_node_storage_column_search(cli_mod, runner, monkeypatch):
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _parse(model_ref, entries):
        captured["entries"] = list(entries or [])
        return {"storage_hash__contains": "portfolio"}

    def _search(q, *, filters=None):
        captured["search"] = {"q": q, "filters": filters}
        return [
            {
                "id": 43,
                "storage_hash": "prices_daily",
                "source_class_name": "PriceBars",
                "identifier": "prices_daily",
                "data_source": {"display_name": "Default DB", "class_type": "timescale_db"},
                "data_frequency_id": "1d",
            }
        ]

    monkeypatch.setattr(cli_mod, "parse_cli_model_filters", _parse)
    monkeypatch.setattr(cli_mod, "data_node_storage_column_search", _search)

    result = runner.invoke(
        cli_mod.app,
        ["data-node", "column-search", "close", "--filter", "storage_hash__contains=portfolio"],
    )
    assert result.exit_code == 0
    assert captured["entries"] == ["storage_hash__contains=portfolio"]
    assert captured["search"] == {"q": "close", "filters": {"storage_hash__contains": "portfolio"}}
    assert "Column Matches" in result.output
    assert "prices_daily" in result.output
    assert 'Column Matches: 1 match(es) for "close"' in result.output


def test_data_node_storage_search_combines_description_and_column(cli_mod, runner, monkeypatch):
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _parse(model_ref, entries):
        captured["entries"] = list(entries or [])
        return {}

    def _description(
        q,
        *,
        q_embedding=None,
        trigram_k=200,
        embed_k=200,
        w_trgm=0.65,
        w_emb=0.35,
        embedding_model="default",
        filters=None,
    ):
        captured["description"] = {
            "q": q,
            "q_embedding": q_embedding,
            "trigram_k": trigram_k,
            "embed_k": embed_k,
            "w_trgm": w_trgm,
            "w_emb": w_emb,
            "embedding_model": embedding_model,
            "filters": filters,
        }
        return {
            "count": 2,
            "next": None,
            "previous": None,
            "results": [
                {
                    "id": 42,
                    "storage_hash": "weights_daily",
                    "source_class_name": "PortfolioWeights",
                    "identifier": "weights_daily",
                    "data_source": {"display_name": "Default DB", "class_type": "timescale_db"},
                    "data_frequency_id": "1d",
                }
            ],
        }

    def _column(q, *, filters=None):
        captured["column"] = {"q": q, "filters": filters}
        return [
            {
                "id": 43,
                "storage_hash": "prices_daily",
                "source_class_name": "PriceBars",
                "identifier": "prices_daily",
                "data_source": {"display_name": "Default DB", "class_type": "timescale_db"},
                "data_frequency_id": "1d",
            }
        ]

    monkeypatch.setattr(cli_mod, "parse_cli_model_filters", _parse)
    monkeypatch.setattr(cli_mod, "data_node_storage_description_search", _description)
    monkeypatch.setattr(cli_mod, "data_node_storage_column_search", _column)

    result = runner.invoke(
        cli_mod.app,
        [
            "data_node",
            "search",
            "close price",
            "--data-source-id",
            "2",
            "--q-embedding",
            "0.1,0.2",
        ],
    )
    assert result.exit_code == 0
    assert captured["entries"] == []
    assert captured["description"]["q"] == "close price"
    assert captured["description"]["q_embedding"] == [0.1, 0.2]
    assert captured["description"]["filters"] == {"data_source__id": "2"}
    assert captured["column"] == {"q": "close price", "filters": {"data_source__id": "2"}}
    assert "Description Matches" in result.output
    assert "Column Matches" in result.output
    assert 'Total search matches for "close price": 2' in result.output


def test_data_node_storage_search_column_mode_only(cli_mod, runner, monkeypatch):
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(cli_mod, "parse_cli_model_filters", lambda model_ref, entries: {})
    monkeypatch.setattr(
        cli_mod,
        "data_node_storage_description_search",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("description search should not run")),
    )

    def _column(q, *, filters=None):
        captured["column"] = {"q": q, "filters": filters}
        return []

    monkeypatch.setattr(cli_mod, "data_node_storage_column_search", _column)

    result = runner.invoke(cli_mod.app, ["data-node", "search", "close", "--mode", "column"])
    assert result.exit_code == 0
    assert captured["column"] == {"q": "close", "filters": {}}
    assert 'Column Matches: 0 match(es) for "close"' in result.output


def test_data_node_storage_detail(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "get_data_node_storage",
        lambda storage_id, timeout=None: {
            "id": storage_id,
            "storage_hash": "weights_daily",
            "identifier": "weights_daily",
            "source_class_name": "PortfolioWeights",
            "data_source": {"display_name": "Default DB", "class_type": "timescale_db"},
            "data_frequency_id": "1d",
            "protect_from_deletion": True,
            "creation_date": "2026-03-16T10:00:00Z",
            "created_by_user": 7,
            "organization_owner": 2,
            "description": "Daily portfolio weights",
            "build_configuration": {"window": 30},
            "sourcetableconfiguration": {"time_index_name": "time_index"},
            "table_index_names": {"0": "time_index"},
            "compression_policy_config": {"after": "7 days"},
            "retention_policy_config": {"after": "90 days"},
        },
    )

    result = runner.invoke(cli_mod.app, ["data-node", "detail", "42"])
    assert result.exit_code == 0
    assert "Data Node Storage" in result.output
    assert "weights_daily" in result.output
    assert "Daily portfolio weights" in result.output
    assert "Build Configuration" in result.output
    assert "time_index_name" in result.output
    assert "90 days" in result.output


def test_data_node_storage_refresh_search_index(cli_mod, runner, monkeypatch):
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _refresh(storage_id, timeout=None):
        captured["storage_id"] = storage_id
        captured["timeout"] = timeout
        return {
            "id": storage_id,
            "status": "queued",
            "message": "refresh started",
        }

    monkeypatch.setattr(cli_mod, "refresh_data_node_storage_search_index", _refresh)

    result = runner.invoke(cli_mod.app, ["data-node", "refresh-search-index", "42", "--timeout", "15"])
    assert result.exit_code == 0
    assert captured == {"storage_id": 42, "timeout": 15}
    assert "Data node search index refresh requested: id=42" in result.output
    assert "Data Node Search Index Refresh" in result.output
    assert "queued" in result.output
    assert "refresh started" in result.output


def test_data_node_storage_can_view(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_data_node_storage_users_can_view",
        lambda storage_id, timeout=None: {
            "access_level": "view",
            "users": [
                {
                    "id": 8,
                    "username": "viewer",
                    "email": "viewer@example.com",
                    "first_name": "View",
                    "last_name": "User",
                }
            ],
            "teams": [],
        },
    )

    result = runner.invoke(cli_mod.app, ["data-node", "can_view", "42"])
    assert result.exit_code == 0
    assert "Data Node Users Who Can View" in result.output
    assert "viewer@example.com" in result.output
    assert "Total users who can view: 1" in result.output


def test_data_node_storage_add_label(cli_mod, runner, monkeypatch):
    captured = {}
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _add(storage_id, labels, timeout=None):
        captured["storage_id"] = storage_id
        captured["labels"] = labels
        captured["timeout"] = timeout
        return {"labels": [{"name": "curated"}]}

    monkeypatch.setattr(cli_mod, "add_data_node_storage_labels", _add)

    result = runner.invoke(
        cli_mod.app,
        ["data-node", "add-label", "42", "--label", "curated"],
    )
    assert result.exit_code == 0
    assert captured == {"storage_id": 42, "labels": ["curated"], "timeout": None}
    assert "Data Node add-label completed." in result.output
    assert "curated" in result.output


def test_data_node_storage_add_to_edit(cli_mod, runner, monkeypatch):
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _add(storage_id, user_id, timeout=None):
        captured["storage_id"] = storage_id
        captured["user_id"] = user_id
        captured["timeout"] = timeout
        return {
            "ok": True,
            "action": "add_to_edit",
            "detail": "User now has explicit edit access.",
            "object_id": storage_id,
            "object_type": "tdag.datanodestorage",
            "user": {
                "id": user_id,
                "username": "editor",
                "email": "editor@example.com",
                "first_name": "Edit",
                "last_name": "User",
            },
            "explicit_can_view": True,
            "explicit_can_edit": True,
            "explicit_can_view_user_ids": [user_id],
            "explicit_can_edit_user_ids": [user_id],
        }

    monkeypatch.setattr(cli_mod, "add_data_node_storage_user_to_edit", _add)

    result = runner.invoke(cli_mod.app, ["data-node", "add_to_edit", "42", "9"])
    assert result.exit_code == 0
    assert captured == {"storage_id": 42, "user_id": 9, "timeout": None}
    assert "Data Node add_to_edit completed." in result.output
    assert "Data Node Sharing Update" in result.output
    assert "editor@example.com" in result.output


def test_data_node_storage_delete_requires_typed_verification(cli_mod, runner, monkeypatch):
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "get_data_node_storage",
        lambda storage_id, timeout=None: {
            "id": storage_id,
            "storage_hash": "weights_daily",
            "identifier": "weights_daily",
            "source_class_name": "PortfolioWeights",
            "data_source": {"display_name": "Default DB", "class_type": "timescale_db"},
            "protect_from_deletion": True,
        },
    )

    def _delete(storage_id, **kwargs):
        captured["storage_id"] = storage_id
        captured["kwargs"] = kwargs
        return {
            "id": storage_id,
            "storage_hash": "weights_daily",
            "identifier": "weights_daily",
            "source_class_name": "PortfolioWeights",
            "data_source": {"display_name": "Default DB", "class_type": "timescale_db"},
            "protect_from_deletion": True,
        }

    monkeypatch.setattr(cli_mod, "delete_data_node_storage", _delete)

    result = runner.invoke(
        cli_mod.app,
        ["data-node", "delete", "42", "--full-delete-selected"],
        input="weights_daily\n",
    )
    assert result.exit_code == 0
    assert "Data Node Storage Delete Preview" in result.output
    assert "Type storage hash 'weights_daily' to confirm deletion" in result.output
    assert captured["storage_id"] == 42
    assert captured["kwargs"]["full_delete_selected"] is True
    assert "Data node storage deleted: id=42" in result.output


def test_data_node_storage_delete_wrong_verification_cancels(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "get_data_node_storage",
        lambda storage_id, timeout=None: {
            "id": storage_id,
            "storage_hash": "weights_daily",
            "identifier": "weights_daily",
            "source_class_name": "PortfolioWeights",
            "data_source": {"display_name": "Default DB", "class_type": "timescale_db"},
            "protect_from_deletion": False,
        },
    )

    called = {"value": False}

    def _delete(storage_id, **kwargs):
        called["value"] = True
        return {}

    monkeypatch.setattr(cli_mod, "delete_data_node_storage", _delete)

    result = runner.invoke(
        cli_mod.app,
        ["data-node", "delete", "42"],
        input="wrong-value\n",
    )
    assert result.exit_code == 0
    assert called["value"] is False
    assert "Cancelled." in result.output


def test_markets_asset_translation_table_list(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_market_asset_translation_tables",
        lambda filters=None, timeout=None: [
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
    captured = {}
    monkeypatch.setattr(
        cli_mod,
        "run_project_job",
        lambda job_id, command_args=None, timeout=None: captured.update(
            job_id=job_id,
            command_args=command_args,
            timeout=timeout,
        ) or {
            "id": 501,
            "job": job_id,
            "status": "QUEUED",
            "unique_identifier": "jobrun_abc123",
        },
    )

    result = runner.invoke(
        cli_mod.app,
        ["project", "jobs", "run", "91", "--command", "python", "--command", "-m", "--command", "jobs.daily"],
    )
    assert result.exit_code == 0
    assert captured == {
        "job_id": 91,
        "command_args": ["python", "-m", "jobs.daily"],
        "timeout": None,
    }
    assert "Project job run requested: job_id=91" in result.output
    assert "jobrun_abc123" in result.output
    assert "QUEUED" in result.output


def test_project_job_runs_list(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_project_job_runs",
        lambda job_id, filters=None, timeout=None: [
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


def test_project_job_runs_list_passes_cli_filters(cli_mod, runner, monkeypatch):
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _parse(model_ref, entries):
        captured["entries"] = list(entries or [])
        return {"status": "COMPLETED"}

    def _list_project_job_runs(job_id, filters=None, timeout=None):
        captured["job_id"] = job_id
        captured["filters"] = filters
        return []

    monkeypatch.setattr(cli_mod, "parse_cli_model_filters", _parse)
    monkeypatch.setattr(cli_mod, "list_project_job_runs", _list_project_job_runs)

    result = runner.invoke(
        cli_mod.app,
        ["project", "jobs", "runs", "list", "91", "--filter", "status=COMPLETED"],
    )
    assert result.exit_code == 0
    assert captured["job_id"] == 91
    assert captured["entries"] == ["status=COMPLETED"]
    assert captured["filters"] == {"status": "COMPLETED"}


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
        if args[:3] == ["rev-parse", "--verify", "1111111111111111111111111111111111111111^{commit}"]:
            return types.SimpleNamespace(
                returncode=0,
                stdout="1111111111111111111111111111111111111111\n",
                stderr="",
            )
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
        if args[:3] == ["rev-parse", "--verify", "deadbeef^{commit}"]:
            return types.SimpleNamespace(
                returncode=0,
                stdout="deadbeefdeadbeefdeadbeefdeadbeefdeadbeef\n",
                stderr="",
            )
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
    monkeypatch.setattr(
        cli_mod,
        "_resolve_full_commit_hash",
        lambda *_: "abc123abc123abc123abc123abc123abc123abcd",
    )
    monkeypatch.setattr(cli_mod, "_is_pushed_commit", lambda *_: True)

    list_calls = {"count": 0}

    def _list_project_images(related_project_id, timeout=None):
        list_calls["count"] += 1
        if list_calls["count"] == 1:
            return []
        if list_calls["count"] == 2:
            return [{"id": 77, "project_repo_hash": "abc123abc123abc123abc123abc123abc123abcd", "base_image": 22, "is_ready": False}]
        return [{"id": 77, "project_repo_hash": "abc123abc123abc123abc123abc123abc123abcd", "base_image": 22, "is_ready": True}]

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


def test_project_create_image_normalizes_short_hash_to_full_sha(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "demo-123"
    target.mkdir(parents=True, exist_ok=True)
    full_hash = "adb3fbb" + ("0" * 33)

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(cli_mod, "_list_unpushed_commits", lambda *_: [])
    monkeypatch.setattr(cli_mod, "list_project_images", lambda related_project_id, timeout=None: [])

    def _git_run(cmd, capture_output=None, text=None):
        args = cmd[3:]
        if args[:3] == ["rev-parse", "--verify", "adb3fbb^{commit}"]:
            return types.SimpleNamespace(
                returncode=0,
                stdout=f"{full_hash}\n",
                stderr="",
            )
        if args[:3] == ["branch", "-r", "--contains"]:
            assert args[-1] == full_hash
            return types.SimpleNamespace(returncode=0, stdout="  origin/main\n", stderr="")
        raise AssertionError(f"Unexpected git command: {cmd}")

    monkeypatch.setattr(cli_mod.subprocess, "run", _git_run)

    captured = {}

    def _create_project_image(**kwargs):
        captured.update(kwargs)
        return {
            "id": 77,
            "project_repo_hash": kwargs["project_repo_hash"],
            "base_image": kwargs["base_image_id"],
            "is_ready": True,
        }

    monkeypatch.setattr(cli_mod, "create_project_image", _create_project_image)

    result = runner.invoke(
        cli_mod.app,
        ["project", "images", "create", "123", "adb3fbb", "--path", str(target), "--base-image-id", "22"],
    )

    assert result.exit_code == 0
    assert captured["project_repo_hash"] == full_hash
    assert full_hash in result.output


def test_project_create_image_rejects_unresolvable_short_hash(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "demo-123"
    target.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(cli_mod, "_list_unpushed_commits", lambda *_: [])
    monkeypatch.setattr(cli_mod, "list_project_images", lambda related_project_id, timeout=None: [])

    def _git_run(cmd, capture_output=None, text=None):
        args = cmd[3:]
        if args[:3] == ["rev-parse", "--verify", "adb3fbb^{commit}"]:
            return types.SimpleNamespace(
                returncode=128,
                stdout="",
                stderr="fatal: ambiguous argument 'adb3fbb^{commit}': unknown revision or path not in the working tree.",
            )
        raise AssertionError(f"Unexpected git command: {cmd}")

    monkeypatch.setattr(cli_mod.subprocess, "run", _git_run)

    result = runner.invoke(
        cli_mod.app,
        ["project", "images", "create", "123", "adb3fbb", "--path", str(target), "--base-image-id", "22"],
    )

    assert result.exit_code == 1
    assert "Could not resolve project_repo_hash to a full commit SHA" in result.output


def test_project_list_requires_shell_auth_hint(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "get_current_user_profile", lambda: {})
    result = runner.invoke(cli_mod.app, ["project", "list"])
    assert result.exit_code == 1
    assert "Not logged in. Run: mainsequence login" in result.output


def test_project_create_interactive_defaults(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "validate_project_name",
        lambda project_name, timeout=None: {
            "project_name": project_name,
            "available": True,
            "reason": None,
            "normalized": {
                "slugified_project_name": "demo-project",
                "project_library_name": "demo_project",
            },
            "suggestions": [],
        },
    )
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
        "validate_project_name",
        lambda project_name, timeout=None: {
            "project_name": project_name,
            "available": True,
            "reason": None,
            "normalized": {
                "slugified_project_name": "demo-project",
                "project_library_name": "demo_project",
            },
            "suggestions": [],
        },
    )
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


def test_project_create_rejects_unavailable_name(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "validate_project_name",
        lambda project_name, timeout=None: {
            "project_name": project_name,
            "available": False,
            "reason": "A project with this name already exists in your organization.",
            "normalized": {
                "slugified_project_name": "demo-project",
                "project_library_name": "demo_project",
            },
            "suggestions": ["Demo Project 2", "Demo Project 3"],
        },
    )

    result = runner.invoke(cli_mod.app, ["project", "create", "Demo Project"])

    assert result.exit_code == 1
    assert "A project with this name already exists in your organization." in result.output
    assert "Project Name Validation" in result.output
    assert "Demo Project 2" in result.output
    assert "Demo Project 3" in result.output


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
        lambda: [
            {
                "id": 123,
                "project_name": "Demo",
                "git_ssh_url": "git@github.com:org/repo.git",
                "is_initialized": True,
            }
        ],
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
        cli_mod.cfg,
        "get_tokens",
        lambda: {"username": "u", "access": "access-123", "refresh": "refresh-456"},
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
    assert "DEFAULT_BASE_IMAGE" not in env_text
    assert "FOO=bar" not in env_text
    assert "MAINSEQUENCE_TOKEN=legacy-token" not in env_text


def test_project_set_up_locally_runtime_credential(cli_mod, runner, monkeypatch, tmp_path):
    base = tmp_path / "base"
    base.mkdir(parents=True, exist_ok=True)
    key = tmp_path / "id_ed25519"
    pub = tmp_path / "id_ed25519.pub"

    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "runtime_credential")
    monkeypatch.setenv("MAINSEQUENCE_RUNTIME_CREDENTIAL_ID", "cred-id")
    monkeypatch.setenv("MAINSEQUENCE_RUNTIME_CREDENTIAL_SECRET", "cred-secret")
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
        lambda: [
            {
                "id": 123,
                "project_name": "Demo",
                "git_ssh_url": "git@github.com:org/repo.git",
                "is_initialized": True,
            }
        ],
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
        cli_mod.cfg,
        "get_tokens",
        lambda: (_ for _ in ()).throw(AssertionError("JWT tokens should not be used")),
    )
    monkeypatch.setattr(
        cli_mod,
        "_exchange_runtime_credential_for_cli_login",
        lambda backend_url: "runtime-access",
    )

    result = runner.invoke(cli_mod.app, ["project", "set-up-locally", "123"])
    assert result.exit_code == 0

    env_file = base / "org" / "projects" / "demo-123" / ".env"
    env_text = env_file.read_text(encoding="utf-8")
    assert "MAINSEQUENCE_AUTH_MODE=runtime_credential" in env_text
    assert "MAINSEQUENCE_ACCESS_TOKEN=runtime-access" in env_text
    assert "MAINSEQUENCE_RUNTIME_CREDENTIAL_ID=cred-id" in env_text
    assert "MAINSEQUENCE_RUNTIME_CREDENTIAL_SECRET=cred-secret" in env_text
    assert "TDAG_ENDPOINT=https://backend.test" in env_text
    assert "MAIN_SEQUENCE_PROJECT_ID=123" in env_text
    assert "MAINSEQUENCE_REFRESH_TOKEN" not in env_text
    assert "DEFAULT_BASE_IMAGE" not in env_text
    assert "FOO=bar" not in env_text
    assert "old-access" not in env_text
    assert "old-refresh" not in env_text


def test_project_set_up_locally_rejects_uninitialized_project(cli_mod, runner, monkeypatch, tmp_path):
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
        "get_projects",
        lambda: [
            {
                "id": 123,
                "project_name": "Demo",
                "git_ssh_url": "git@github.com:org/repo.git",
                "is_initialized": False,
            }
        ],
    )

    clone_calls = {"count": 0}

    def _clone(*args, **kwargs):
        clone_calls["count"] += 1
        return 0

    monkeypatch.setattr(cli_mod.subprocess, "call", _clone)

    result = runner.invoke(cli_mod.app, ["project", "set-up-locally", "123"])

    assert result.exit_code == 1
    assert "Project has not finished initializing yet." in result.output
    assert clone_calls["count"] == 0


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
    assert "MAINSEQUENCE_TOKEN=legacy-token" in env_text
    assert "old-access" not in env_text
    assert "old-refresh" not in env_text


def test_project_refresh_token_runtime_credential(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "demo-123"
    target.mkdir(parents=True, exist_ok=True)
    env_path = target / ".env"
    env_path.write_text(
        "FOO=bar\n"
        "MAINSEQUENCE_AUTH_MODE=jwt\n"
        "MAINSEQUENCE_ACCESS_TOKEN=old-access\n"
        "MAINSEQUENCE_REFRESH_TOKEN=old-refresh\n",
        encoding="utf-8",
    )

    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "runtime_credential")
    monkeypatch.setenv("MAINSEQUENCE_RUNTIME_CREDENTIAL_ID", "cred-id")
    monkeypatch.setenv("MAINSEQUENCE_RUNTIME_CREDENTIAL_SECRET", "cred-secret")
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_tokens",
        lambda: (_ for _ in ()).throw(AssertionError("JWT tokens should not be used")),
    )
    monkeypatch.setattr(cli_mod.cfg, "backend_url", lambda: "https://backend.test")
    monkeypatch.setattr(
        cli_mod,
        "_exchange_runtime_credential_for_cli_login",
        lambda backend_url: "runtime-new-access",
    )

    result = runner.invoke(cli_mod.app, ["project", "refresh_token", "--path", str(target)])
    assert result.exit_code == 0

    env_text = env_path.read_text(encoding="utf-8")
    assert "FOO=bar" in env_text
    assert "MAINSEQUENCE_AUTH_MODE=runtime_credential" in env_text
    assert "MAINSEQUENCE_ACCESS_TOKEN=runtime-new-access" in env_text
    assert "MAINSEQUENCE_RUNTIME_CREDENTIAL_ID=cred-id" in env_text
    assert "MAINSEQUENCE_RUNTIME_CREDENTIAL_SECRET=cred-secret" in env_text
    assert "TDAG_ENDPOINT=https://backend.test" in env_text
    assert "MAIN_SEQUENCE_PROJECT_ID=123" in env_text
    assert "MAINSEQUENCE_REFRESH_TOKEN" not in env_text
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


def test_project_schedule_batch_jobs_defaults_to_cwd(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "project"
    target.mkdir(parents=True, exist_ok=True)
    (target / ".env").write_text("MAIN_SEQUENCE_PROJECT_ID=123\n", encoding="utf-8")
    (target / "scheduled_jobs.yaml").write_text("jobs: []\n", encoding="utf-8")
    captured = {}

    monkeypatch.chdir(target)
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(cli_mod.typer, "confirm", lambda message, default=False: True)
    monkeypatch.setattr(
        cli_mod,
        "schedule_batch_project_jobs",
        lambda **kwargs: captured.update(kwargs) or [
            {
                "id": 91,
                "name": "Simulated Prices",
                "execution_path": "scripts/simulated_prices_launcher.py",
                "app_name": None,
                "task_schedule": {"schedule": {"type": "crontab", "expression": "0 0 * * *"}},
            }
        ],
    )

    result = runner.invoke(
        cli_mod.app,
        ["project", "schedule_batch_jobs", "scheduled_jobs.yaml", "--strict"],
    )
    assert result.exit_code == 0
    assert captured["project_id"] == 123
    assert captured["file_path"] == str((target / "scheduled_jobs.yaml").resolve())
    assert captured["strict"] is True
    assert "Scheduled 1 jobs from scheduled_jobs.yaml." in result.output


def test_project_schedule_batch_jobs_prompts_for_project_image_and_applies_it_to_all_jobs(
    cli_mod, runner, monkeypatch, tmp_path
):
    target = tmp_path / "project"
    target.mkdir(parents=True, exist_ok=True)
    batch_file = target / "scheduled_jobs.yaml"
    (target / ".env").write_text("MAIN_SEQUENCE_PROJECT_ID=123\n", encoding="utf-8")
    batch_file.write_text(
        "\n".join(
            [
                "jobs:",
                '  - name: "Simulated Prices"',
                '    execution_path: "scripts/simulated_prices_launcher.py"',
                "    related_image_id: 78",
                "    task_schedule:",
                '      type: "crontab"',
                '      expression: "0 0 * * *"',
            ]
        ),
        encoding="utf-8",
    )
    captured = {}
    captured_picker = {}

    monkeypatch.chdir(target)
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    captured_confirm = {}
    monkeypatch.setattr(
        cli_mod,
        "list_project_images",
        lambda related_project_id, timeout=None: [
            {"id": 77, "project_repo_hash": "abc123", "base_image": {"title": "py311"}},
            {"id": 78, "project_repo_hash": "def456", "base_image": {"title": "py312"}},
        ],
    )
    monkeypatch.setattr(
        cli_mod,
        "_prompt_select_id",
        lambda **kwargs: captured_picker.update(kwargs) or 77,
    )
    monkeypatch.setattr(
        cli_mod.typer,
        "confirm",
        lambda message, default=False: captured_confirm.setdefault("message", message) or True,
    )

    def _fake_schedule_batch_project_jobs(**kwargs):
        captured.update(kwargs)
        captured["yaml"] = yaml.safe_load(pathlib.Path(kwargs["file_path"]).read_text(encoding="utf-8"))
        return [
            {
                "id": 91,
                "name": "Simulated Prices",
                "execution_path": "scripts/simulated_prices_launcher.py",
                "app_name": None,
                "task_schedule": {"schedule": {"type": "crontab", "expression": "0 0 * * *"}},
            }
        ]

    monkeypatch.setattr(cli_mod, "schedule_batch_project_jobs", _fake_schedule_batch_project_jobs)

    result = runner.invoke(
        cli_mod.app,
        ["project", "schedule_batch_jobs", "scheduled_jobs.yaml"],
    )
    assert result.exit_code == 0
    assert captured["project_id"] == 123
    assert captured["file_path"] != str(batch_file.resolve())
    assert captured_picker["title"] == "Available Project Images"
    assert captured_picker["items"][0]["id"] == 77
    assert captured["yaml"]["jobs"][0]["related_image_id"] == 77
    assert "same image (77)" in captured_confirm["message"]
    assert "Overriding related_image_id for 1 job(s)" in result.output
    assert "Using project image 77 for all 1 job(s) in this batch." in result.output


def test_project_schedule_batch_jobs_cancelled_on_confirmation(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "project"
    target.mkdir(parents=True, exist_ok=True)
    batch_file = target / "scheduled_jobs.yaml"
    (target / ".env").write_text("MAIN_SEQUENCE_PROJECT_ID=123\n", encoding="utf-8")
    batch_file.write_text(
        "\n".join(
            [
                "jobs:",
                '  - name: "Simulated Prices"',
                '    execution_path: "scripts/simulated_prices_launcher.py"',
                '    related_image_id: 77',
            ]
        ),
        encoding="utf-8",
    )

    monkeypatch.chdir(target)
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_project_images",
        lambda related_project_id, timeout=None: [
            {"id": 77, "project_repo_hash": "abc123", "base_image": {"title": "py311"}},
        ],
    )
    monkeypatch.setattr(cli_mod, "_prompt_select_id", lambda **kwargs: 77)
    monkeypatch.setattr(cli_mod.typer, "confirm", lambda message, default=False: False)
    monkeypatch.setattr(
        cli_mod,
        "schedule_batch_project_jobs",
        lambda **kwargs: pytest.fail("schedule_batch_project_jobs should not be called"),
    )

    result = runner.invoke(
        cli_mod.app,
        ["project", "schedule_batch_jobs", "scheduled_jobs.yaml"],
    )
    assert result.exit_code == 0
    assert "Cancelled." in result.output


def test_project_schedule_batch_jobs_renders_summary_response(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "project"
    target.mkdir(parents=True, exist_ok=True)
    (target / ".env").write_text("MAIN_SEQUENCE_PROJECT_ID=123\n", encoding="utf-8")
    (target / "scheduled_jobs.yaml").write_text("jobs: []\n", encoding="utf-8")

    monkeypatch.chdir(target)
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(cli_mod.typer, "confirm", lambda message, default=False: True)
    monkeypatch.setattr(
        cli_mod,
        "schedule_batch_project_jobs",
        lambda **kwargs: {
            "project_id": 123,
            "strict": True,
            "created_count": 1,
            "existing_count": 1,
            "deleted_count": 1,
            "deleted": [
                {
                    "id": 40,
                    "name": "old-job",
                    "execution_path": "scripts/old.py",
                    "app_name": None,
                }
            ],
            "not_deleted_count": 1,
            "not_deleted": [
                {
                    "job": {
                        "id": 50,
                        "name": "dashboard-job",
                        "execution_path": "scripts/dashboard.py",
                        "app_name": None,
                    },
                    "reason": "linked to dashboard",
                }
            ],
            "results": [
                {
                    "created": True,
                    "job": {
                        "id": 91,
                        "name": "new-job",
                        "execution_path": "scripts/new.py",
                        "app_name": None,
                        "task_schedule": {"schedule": {"type": "crontab", "expression": "0 0 * * *"}},
                    },
                },
                {
                    "created": False,
                    "job": {
                        "id": 92,
                        "name": "existing-job",
                        "execution_path": "scripts/existing.py",
                        "app_name": "svc",
                        "task_schedule": None,
                    },
                },
            ],
        },
    )

    result = runner.invoke(
        cli_mod.app,
        ["project", "schedule_batch_jobs", "scheduled_jobs.yaml", "--strict"],
    )

    assert result.exit_code == 0
    assert "Batch Scheduling Summary" in result.output
    assert "Created" in result.output
    assert "Existing" in result.output
    assert "Deleted Jobs" in result.output
    assert "Not Deleted Jobs" in result.output
    assert "new-job" in result.output
    assert "existing" in result.output
    assert "linked to dashboard" in result.output
    assert "Strict mode will not delete jobs that are still linked to dashboards or resource releases." in result.output


def test_project_sync_triggers_backend_sync_after_push(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "project"
    target.mkdir(parents=True, exist_ok=True)
    (target / ".env").write_text("MAIN_SEQUENCE_PROJECT_ID=123\n", encoding="utf-8")
    key = tmp_path / "id_ed25519"
    uv_path = target / ".venv" / "bin" / "uv"
    post_sync = {}
    call_order = []

    monkeypatch.chdir(target)
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(cli_mod, "ensure_venv", lambda *_: None)
    monkeypatch.setattr(cli_mod, "git_origin", lambda *_: "git@github.com:org/repo.git")
    monkeypatch.setattr(cli_mod, "ensure_key_for_repo", lambda *_: (key, key.with_suffix(".pub"), "pub"))
    monkeypatch.setattr(cli_mod, "ensure_uv_installed", lambda *_: uv_path)
    monkeypatch.setattr(
        cli_mod,
        "prime_sync_project_after_commit_sdk",
        lambda: call_order.append("prime"),
    )
    monkeypatch.setattr(cli_mod, "run_uv", lambda uv, args, cwd, env=None: call_order.append(f"uv:{args[0]}"))
    monkeypatch.setattr(cli_mod, "uv_export_requirements", lambda uv, cwd, **kwargs: call_order.append("uv:export"))
    monkeypatch.setattr(cli_mod, "run_cmd", lambda cmd, cwd, env=None: call_order.append(f"cmd:{cmd[0]}"))
    monkeypatch.setattr(
        cli_mod,
        "sync_project_after_commit",
        lambda project_id: call_order.append("post_sync") or post_sync.update(project_id=project_id) or {"id": project_id},
    )

    result = runner.invoke(cli_mod.app, ["project", "sync", "Update deps"])
    assert result.exit_code == 0
    assert post_sync["project_id"] == 123
    assert call_order[0] == "prime"
    assert "post_sync" == call_order[-1]
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


def test_project_current_json(cli_mod, runner, monkeypatch, tmp_path):
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

    result = runner.invoke(cli_mod.app, ["project", "current", "--json"])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["project"]["project_id"] == "123"
    assert payload["sdk_status"]["status"] == "match"


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


def test_project_sdk_status_json(cli_mod, runner, monkeypatch, tmp_path):
    target = tmp_path / "project"
    target.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(cli_mod, "read_local_sdk_version", lambda req: "1.2.3")
    monkeypatch.setattr(cli_mod, "fetch_latest_sdk_version", lambda: "v1.2.3")

    result = runner.invoke(
        cli_mod.app,
        ["project", "sdk-status", "--path", str(target), "--json"],
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["project"] == str(target)
    assert payload["latest_github"] == "v1.2.3"
    assert payload["local_requirements_txt"] == "1.2.3"


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


def _write_installed_agent_scaffold_bundle(bundle_dir: pathlib.Path) -> pathlib.Path:
    bundle_dir.mkdir(parents=True, exist_ok=True)
    agents_md = bundle_dir / "AGENTS.md"
    agents_md.write_text(
        "\n".join(
            [
                "# AGENTS.md",
                "",
                "Installed scaffold preface.",
                "",
                "<!-- mainsequence-agent-scaffold:start schema=1 source=agent_scaffold -->",
                "## Main Sequence Instructions",
                "",
                "Installed managed block.",
                "",
                "<!-- mainsequence-agent-scaffold:end -->",
                "",
                "Installed scaffold footer.",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return agents_md


def test_project_update_agents_md_replaces_custom_unmarked_file(
    cli_mod,
    runner,
    monkeypatch,
    tmp_path,
):
    agents_md = _write_installed_agent_scaffold_bundle(tmp_path / "bundle")
    target = tmp_path / "project"
    target.mkdir()
    original = "# Project Agents\n\nKeep this project-specific instruction.\n"
    (target / "AGENTS.md").write_text(original, encoding="utf-8")
    monkeypatch.setattr(cli_mod, "_installed_agent_scaffold_bundle_dir", lambda: agents_md.parent)

    result = runner.invoke(
        cli_mod.app,
        ["project", "update", "AGENTS.md", "--path", str(target)],
    )
    assert result.exit_code == 0
    content = (target / "AGENTS.md").read_text(encoding="utf-8")
    assert content == agents_md.read_text(encoding="utf-8")
    assert content.count(cli_mod.AGENTS_MD_MANAGED_BLOCK_START_PREFIX) == 1
    assert content.count(cli_mod.AGENTS_MD_MANAGED_BLOCK_END) == 1
    assert "Installed managed block." in content
    assert "Keep this project-specific instruction." not in content
    assert "Action" in result.output
    assert "replaced" in result.output


def test_project_update_agents_md_creates_file_when_missing(
    cli_mod,
    runner,
    monkeypatch,
    tmp_path,
):
    agents_md = _write_installed_agent_scaffold_bundle(tmp_path / "bundle")
    target = tmp_path / "project"
    target.mkdir()
    monkeypatch.setattr(cli_mod, "_installed_agent_scaffold_bundle_dir", lambda: agents_md.parent)

    result = runner.invoke(
        cli_mod.app,
        ["project", "update", "AGENTS.md", "--path", str(target)],
    )
    assert result.exit_code == 0
    content = (target / "AGENTS.md").read_text(encoding="utf-8")
    assert content == agents_md.read_text(encoding="utf-8")
    assert cli_mod.AGENTS_MD_MANAGED_BLOCK_START_PREFIX in content
    assert "Action" in result.output
    assert "created" in result.output


def test_project_update_agents_md_replaces_existing_managed_block(
    cli_mod,
    runner,
    monkeypatch,
    tmp_path,
):
    agents_md = _write_installed_agent_scaffold_bundle(tmp_path / "bundle")
    target = tmp_path / "project"
    target.mkdir()
    old_block = "\n".join(
        [
            f"{cli_mod.AGENTS_MD_MANAGED_BLOCK_START_PREFIX} "
            "schema=old source=old -->",
            "old managed content",
            cli_mod.AGENTS_MD_MANAGED_BLOCK_END,
        ]
    )
    (target / "AGENTS.md").write_text(
        f"# Project Agents\n\n{old_block}\n\nCustom tail.\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(cli_mod, "_installed_agent_scaffold_bundle_dir", lambda: agents_md.parent)

    result = runner.invoke(
        cli_mod.app,
        ["project", "update", "AGENTS.md", "--path", str(target)],
    )
    assert result.exit_code == 0
    content = (target / "AGENTS.md").read_text(encoding="utf-8")
    assert "old managed content" not in content
    assert "Installed managed block." in content
    assert "Custom tail.\n" in content
    assert content.count(cli_mod.AGENTS_MD_MANAGED_BLOCK_START_PREFIX) == 1
    assert "updated" in result.output


def test_project_update_agents_md_json_reports_action(
    cli_mod,
    runner,
    monkeypatch,
    tmp_path,
):
    agents_md = _write_installed_agent_scaffold_bundle(tmp_path / "bundle")
    target = tmp_path / "project"
    target.mkdir()
    monkeypatch.setattr(cli_mod, "_installed_agent_scaffold_bundle_dir", lambda: agents_md.parent)

    result = runner.invoke(
        cli_mod.app,
        ["project", "update", "AGENTS.md", "--path", str(target), "--json"],
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["target"] == "AGENTS.md"
    assert payload["project"] == str(target)
    assert payload["source"] == str(agents_md)
    assert payload["destination"] == str(target / "AGENTS.md")
    assert payload["action"] == "created"
    assert payload["changed"] is True
    assert payload["overwritten"] is False


def test_project_update_agents_md_rejects_malformed_managed_block(
    cli_mod,
    runner,
    monkeypatch,
    tmp_path,
):
    agents_md = _write_installed_agent_scaffold_bundle(tmp_path / "bundle")
    target = tmp_path / "project"
    target.mkdir()
    start_marker = (
        f"{cli_mod.AGENTS_MD_MANAGED_BLOCK_START_PREFIX} "
        "schema=1 source=agent_scaffold -->"
    )
    (target / "AGENTS.md").write_text(
        f"# Project Agents\n\n{start_marker}\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(cli_mod, "_installed_agent_scaffold_bundle_dir", lambda: agents_md.parent)

    result = runner.invoke(
        cli_mod.app,
        ["project", "update", "AGENTS.md", "--path", str(target)],
    )
    assert result.exit_code == 1
    assert "malformed" in result.output


def test_project_update_agents_md_rejects_duplicate_managed_blocks(
    cli_mod,
    runner,
    monkeypatch,
    tmp_path,
):
    agents_md = _write_installed_agent_scaffold_bundle(tmp_path / "bundle")
    target = tmp_path / "project"
    target.mkdir()
    block = "\n".join(
        [
            f"{cli_mod.AGENTS_MD_MANAGED_BLOCK_START_PREFIX} "
            "schema=1 source=agent_scaffold -->",
            "managed content",
            cli_mod.AGENTS_MD_MANAGED_BLOCK_END,
        ]
    )
    (target / "AGENTS.md").write_text(f"{block}\n\n{block}\n", encoding="utf-8")
    monkeypatch.setattr(cli_mod, "_installed_agent_scaffold_bundle_dir", lambda: agents_md.parent)

    result = runner.invoke(
        cli_mod.app,
        ["project", "update", "AGENTS.md", "--path", str(target)],
    )
    assert result.exit_code == 1
    assert "multiple" in result.output


def test_project_update_agents_md_fails_when_installed_bundle_missing_agents_md(
    cli_mod,
    runner,
    monkeypatch,
    tmp_path,
):
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir()
    target = tmp_path / "project"
    target.mkdir()
    monkeypatch.setattr(cli_mod, "_installed_agent_scaffold_bundle_dir", lambda: bundle_dir)

    result = runner.invoke(
        cli_mod.app,
        ["project", "update", "AGENTS.md", "--path", str(target)],
    )
    assert result.exit_code == 1
    assert "Installed agent_scaffold bundle is missing AGENTS.md" in result.output


def test_installed_agent_scaffold_bundle_dir_falls_back_to_sibling_package(cli_mod, monkeypatch):
    original_import_module = cli_mod.importlib.import_module

    def _import_module(name, package=None):
        if name == "agent_scaffold":
            raise ModuleNotFoundError("No module named 'agent_scaffold'")
        return original_import_module(name, package)

    monkeypatch.setattr(cli_mod.importlib, "import_module", _import_module)

    resolved = cli_mod._installed_agent_scaffold_bundle_dir()
    expected = pathlib.Path(cli_mod.__file__).resolve().parents[2] / "agent_scaffold"
    assert resolved == expected.resolve()


def test_project_update_agent_skills_overwrites_matching_folders(cli_mod, runner, monkeypatch, tmp_path):
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir()
    (bundle_dir / "AGENTS.md").write_text("bundle agents", encoding="utf-8")
    (bundle_dir / "skills" / "data_publishing").mkdir(parents=True)
    (bundle_dir / "skills" / "data_publishing" / "SKILL.md").write_text("new data skill", encoding="utf-8")
    (bundle_dir / "skills" / "maintenance").mkdir(parents=True)
    (bundle_dir / "skills" / "maintenance" / "SKILL.md").write_text("new maintenance skill", encoding="utf-8")
    (bundle_dir / "__pycache__").mkdir()
    (bundle_dir / "__pycache__" / "ignored.txt").write_text("ignore me", encoding="utf-8")

    target = tmp_path / "project"
    existing = target / ".agents" / "skills" / "data_publishing"
    existing.mkdir(parents=True)
    (existing / "old.txt").write_text("stale", encoding="utf-8")

    monkeypatch.setattr(cli_mod, "_project_agent_scaffold_bundle_dir", lambda project_dir: bundle_dir)

    result = runner.invoke(cli_mod.app, ["project", "update_agent_skills", "--path", str(target)])
    assert result.exit_code == 0
    assert (target / ".agents" / "skills" / "data_publishing" / "SKILL.md").read_text(encoding="utf-8") == "new data skill"
    assert not (target / ".agents" / "skills" / "data_publishing" / "old.txt").exists()
    assert (target / ".agents" / "skills" / "maintenance" / "SKILL.md").read_text(encoding="utf-8") == "new maintenance skill"
    assert not (target / ".agents" / "skills" / "__pycache__").exists()
    assert "Updated Agent Skills" in result.output


def test_login_live_with_env_tokens(cli_mod, runner, monkeypatch):
    """
    Optional live JWT import check.

    Set:
      - MAINSEQUENCE_TEST_ACCESS_TOKEN
      - MAINSEQUENCE_TEST_REFRESH_TOKEN
    """
    access_token = os.getenv("MAINSEQUENCE_TEST_ACCESS_TOKEN")
    refresh_token = os.getenv("MAINSEQUENCE_TEST_REFRESH_TOKEN")
    if not access_token or not refresh_token:
        pytest.skip("Missing MAINSEQUENCE_TEST_ACCESS_TOKEN / MAINSEQUENCE_TEST_REFRESH_TOKEN")

    monkeypatch.setattr(
        cli_mod.cfg,
        "get_config",
        lambda: {"mainsequence_path": "/tmp/mainsequence"},
    )
    result = runner.invoke(
        cli_mod.app,
        [
            "login",
            "--access-token",
            access_token,
            "--refresh-token",
            refresh_token,
            "--no-status",
        ],
    )
    assert result.exit_code == 0
