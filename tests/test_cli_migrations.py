from __future__ import annotations

import importlib
import json
import pathlib
import sys
import textwrap
import types

import pytest
from typer.testing import CliRunner

from mainsequence.client.metatables import (
    AlembicMigrationApplyResponse,
    AlembicMigrationStatusResponse,
)
from mainsequence.meta_tables.migrations import (
    AlembicMetaTableMigration,
    AlembicVersionMetaTable,
    PackagedAlembicMigrationArtifact,
)


def _load_cli_module():
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


def _write_provider(tmp_path: pathlib.Path, module_name: str = "sample_migration_provider") -> str:
    (tmp_path / f"{module_name}.py").write_text(
        textwrap.dedent(
            """
            import types
            from sqlalchemy import MetaData
            from mainsequence.meta_tables.migrations import (
                AlembicMetaTableMigration,
                AlembicVersionMetaTable,
            )


            class Registry(AlembicVersionMetaTable):
                __metatable_uid__ = "registry-meta-table-uid"
                __metatable_data_source_uid__ = "data-source-uid"
                __metatable__ = types.SimpleNamespace(
                    uid=__metatable_uid__,
                    data_source_uid=__metatable_data_source_uid__,
                )
                __metatable_namespace__ = "msm"
                __metatable_identifier__ = "msm.alembic_version"


            migration = AlembicMetaTableMigration(
                package="msm",
                migration_namespace="markets",
                script_location="msm:alembic",
                target_metadata=MetaData(),
                alembic_registry=Registry,
            )
            """
        ),
        encoding="utf-8",
    )
    return f"{module_name}:migration"


def _write_provider_with_register_hook(
    tmp_path: pathlib.Path,
    module_name: str = "sample_migration_provider_with_hook",
) -> str:
    (tmp_path / f"{module_name}.py").write_text(
        textwrap.dedent(
            """
            import types
            from sqlalchemy import MetaData
            from mainsequence.meta_tables.migrations import (
                AlembicMetaTableMigration,
                AlembicVersionMetaTable,
            )


            EVENTS = []


            class Registry(AlembicVersionMetaTable):
                __metatable_uid__ = "registry-meta-table-uid"
                __metatable_data_source_uid__ = "data-source-uid"
                __metatable__ = types.SimpleNamespace(
                    uid=__metatable_uid__,
                    data_source_uid=__metatable_data_source_uid__,
                )
                __metatable_namespace__ = "msm"
                __metatable_identifier__ = "msm.alembic_version"


            class AccountModel:
                __metatable_identifier__ = "global.account"

                @classmethod
                def register(cls, *, timeout=None):
                    EVENTS.append(("register", "account", timeout))
                    return {"uid": "account-meta-table-uid"}


            def after_register_metatables(registered):
                EVENTS.append(("hook", [item["uid"] for item in registered]))


            migration = AlembicMetaTableMigration(
                package="msm",
                migration_namespace="markets",
                script_location="msm:alembic",
                target_metadata=MetaData(),
                alembic_registry=Registry,
                metatable_models=[AccountModel],
                after_register_metatables=after_register_metatables,
            )
            """
        ),
        encoding="utf-8",
    )
    return f"{module_name}:migration"


def _result_stdout(result) -> str:
    return getattr(result, "stdout", result.output)


def test_migrations_current_uses_provider_registry_binding(cli_mod, runner, monkeypatch, tmp_path):
    provider_ref = _write_provider(tmp_path)
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()

    captured = {}
    migration_cli = importlib.import_module("mainsequence.cli.migrations")

    def fake_status(request, *, timeout=None):
        captured["request"] = request
        captured["timeout"] = timeout
        return AlembicMigrationStatusResponse(
            ok=True,
            alembic_version_meta_table_uid=request.alembic_version_meta_table_uid,
            alembic_version_table="public.alembic_version",
            data_source_uid="backend-data-source-uid",
            package=request.package,
            migration_namespace=request.migration_namespace,
            current_revision="0001_initial",
            current_revisions=["0001_initial"],
        )

    monkeypatch.setattr(migration_cli.MetaTable, "get_migration_status", staticmethod(fake_status))

    result = runner.invoke(
        cli_mod.app,
        ["migrations", "current", "--provider", provider_ref, "--json"],
    )

    assert result.exit_code == 0
    assert captured["request"].alembic_version_meta_table_uid == "registry-meta-table-uid"
    assert not hasattr(captured["request"], "data_source_uid")
    payload = json.loads(_result_stdout(result))
    assert payload["current_revision"] == "0001_initial"


def test_migrations_register_version_table_command_is_removed(cli_mod, runner):
    result = runner.invoke(cli_mod.app, ["migrations", "register-version-table"])

    assert result.exit_code != 0
    assert "No such command" in result.output


def test_migrations_revision_defaults_message(cli_mod, runner, monkeypatch, tmp_path):
    provider_ref = _write_provider(tmp_path, module_name="revision_migration_provider")
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()
    migration_cli = importlib.import_module("mainsequence.cli.migrations")
    from alembic import command

    captured = {}

    def fake_revision(config, *, message, autogenerate, rev_id, head):
        captured.update(
            {
                "message": message,
                "autogenerate": autogenerate,
                "rev_id": rev_id,
                "head": head,
                "script_location": config.get_main_option("script_location"),
            }
        )
        return types.SimpleNamespace(revision="0001", path="/tmp/0001_migration.py")

    monkeypatch.setattr(command, "revision", fake_revision)
    monkeypatch.setattr(
        migration_cli,
        "_next_sequential_revision_id",
        lambda migration: "0001",
    )

    result = runner.invoke(
        cli_mod.app,
        ["migrations", "revision", "--provider", provider_ref, "--json"],
    )

    assert result.exit_code == 0
    assert captured == {
        "message": "migration",
        "autogenerate": False,
        "rev_id": "0001",
        "head": "head",
        "script_location": "msm:alembic",
    }


def test_migrations_revision_autogenerate_requires_sqlalchemy_url(
    cli_mod,
    runner,
    tmp_path,
    monkeypatch,
):
    provider_ref = _write_provider(tmp_path, module_name="autogenerate_migration_provider")
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()

    result = runner.invoke(
        cli_mod.app,
        ["migrations", "revision", "--provider", provider_ref, "--autogenerate"],
    )

    assert result.exit_code != 0
    assert "Invalid value for --sqlalchemy-url" in result.output
    assert "--autogenerate because Alembic must connect" in result.output


def test_next_sequential_revision_id(cli_mod, tmp_path):
    migration_cli = importlib.import_module("mainsequence.cli.migrations")
    migrations_dir = tmp_path / "migrations"
    versions_dir = migrations_dir / "versions"
    versions_dir.mkdir(parents=True)
    (migrations_dir / "env.py").write_text("", encoding="utf-8")
    (migrations_dir / "script.py.mako").write_text("", encoding="utf-8")

    class Registry(AlembicVersionMetaTable):
        __metatable_uid__ = "registry-meta-table-uid"
        __metatable_data_source_uid__ = "data-source-uid"

    migration = AlembicMetaTableMigration(
        package="msm",
        migration_namespace="markets",
        script_location=str(migrations_dir),
        target_metadata=None,
        alembic_registry=Registry,
    )

    assert migration_cli._next_sequential_revision_id(migration) == "0001"

    (versions_dir / "0001_migration.py").write_text(
        textwrap.dedent(
            """
            revision = "0001"
            down_revision = None
            branch_labels = None
            depends_on = None
            """
        ),
        encoding="utf-8",
    )

    assert migration_cli._next_sequential_revision_id(migration) == "0002"


def test_migrations_render_prints_metatable_resolution(
    cli_mod,
    runner,
    monkeypatch,
    tmp_path,
):
    provider_ref = _write_provider(tmp_path, module_name="render_log_migration_provider")
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()
    migration_cli = importlib.import_module("mainsequence.cli.migrations")

    class LoggedModel:
        pass

    def fake_resolve(self, *, timeout=None, on_metatable_resolution=None):
        assert timeout is None
        on_metatable_resolution(
            LoggedModel,
            "global.logged_model",
            "registered",
            {
                "uid": "logged-model-uid",
                "physical_table_name": "mt_logged_model",
            },
        )
        on_metatable_resolution(
            LoggedModel,
            "global.existing_model",
            "exists",
            {"uid": "existing-model-uid"},
        )
        return []

    def fake_render(migration, **kwargs):
        return PackagedAlembicMigrationArtifact(
            manifest={
                "package": migration.package,
                "migration_namespace": migration.migration_namespace,
                "revision": "0001",
                "down_revision": None,
                "direction": "upgrade",
                "alembic_version_table": "public.alembic_version",
            },
            sql="CREATE TABLE logged_model (uid integer);",
            statement_boundaries=[],
        )

    monkeypatch.setattr(
        migration_cli.AlembicMetaTableMigration,
        "resolve_or_register_metatable_models",
        fake_resolve,
    )
    monkeypatch.setattr(
        migration_cli,
        "render_packaged_alembic_migration_for_provider",
        fake_render,
    )

    result = runner.invoke(
        cli_mod.app,
        ["migrations", "render", "--provider", provider_ref, "--json"],
    )

    assert result.exit_code == 0
    assert "migration MetaTable registered:" in result.stderr
    assert "identifier=global.logged_model" in result.stderr
    assert "uid=logged-model-uid" in result.stderr
    assert "physical_table_name=mt_logged_model" in result.stderr
    assert "migration MetaTable exists:" in result.stderr
    assert "identifier=global.existing_model" in result.stderr
    assert "uid=existing-model-uid" in result.stderr
    payload = json.loads(_result_stdout(result))
    assert payload["sql"] == "CREATE TABLE logged_model (uid integer);"


def test_migrations_upgrade_dry_runs_then_applies_same_artifact(
    cli_mod,
    runner,
    monkeypatch,
    tmp_path,
):
    provider_ref = _write_provider(tmp_path, module_name="upgrade_migration_provider")
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()
    migration_cli = importlib.import_module("mainsequence.cli.migrations")

    operations = []
    status_calls = []
    order = []

    class LoggedModel:
        pass

    def fake_status(request, *, timeout=None):
        status_calls.append(request)
        return AlembicMigrationStatusResponse(
            ok=True,
            alembic_version_meta_table_uid=request.alembic_version_meta_table_uid,
            alembic_version_table="public.alembic_version",
            data_source_uid="backend-data-source-uid",
            package=request.package,
            migration_namespace=request.migration_namespace,
            current_revision=None,
            current_revisions=[],
        )

    def fake_render(migration, **kwargs):
        order.append("render")
        assert order == ["resolve", "render"]
        assert kwargs["current_revision"] is None
        return PackagedAlembicMigrationArtifact(
            manifest={
                "package": migration.package,
                "migration_namespace": migration.migration_namespace,
                "revision": "0001_initial",
                "down_revision": None,
                "direction": "upgrade",
                "alembic_version_table": "public.alembic_version",
            },
            sql="CREATE TABLE asset (uid integer);",
            statement_boundaries=[],
        )

    def fake_apply(operation, *, timeout=None):
        operations.append(operation)
        return AlembicMigrationApplyResponse(
            ok=True,
            dry_run=operation.dry_run,
            alembic_version_meta_table_uid=operation.alembic_version_meta_table_uid,
            alembic_version_table="public.alembic_version",
            data_source_uid="backend-data-source-uid",
            package=operation.package,
            migration_namespace=operation.migration_namespace,
            revision=operation.revision,
            direction=operation.direction,
            status="validated" if operation.dry_run else "applied",
            current_revision="0001_initial",
            current_revisions=["0001_initial"],
        )

    monkeypatch.setattr(migration_cli.MetaTable, "get_migration_status", staticmethod(fake_status))
    monkeypatch.setattr(migration_cli.MetaTable, "apply_migration", staticmethod(fake_apply))

    def fake_resolve(self, *, timeout=None, on_metatable_resolution=None):
        order.append("resolve")
        assert timeout is None
        on_metatable_resolution(
            LoggedModel,
            "global.logged_model",
            "exists",
            {"uid": "logged-model-uid"},
        )
        return []

    monkeypatch.setattr(
        migration_cli.AlembicMetaTableMigration,
        "resolve_or_register_metatable_models",
        fake_resolve,
    )
    monkeypatch.setattr(
        migration_cli,
        "render_packaged_alembic_migration_for_provider",
        fake_render,
    )

    result = runner.invoke(
        cli_mod.app,
        ["migrations", "upgrade", "--provider", provider_ref, "--to", "head", "--json"],
    )

    assert result.exit_code == 0
    assert [operation.dry_run for operation in operations] == [True, False]
    assert len(status_calls) == 2
    assert {operation.sql for operation in operations} == {"CREATE TABLE asset (uid integer);"}
    assert all(not hasattr(operation, "data_source_uid") for operation in operations)
    assert {operation.alembic_version_meta_table_uid for operation in operations} == {
        "registry-meta-table-uid"
    }
    assert "migration MetaTable exists:" in result.stderr
    assert "identifier=global.logged_model" in result.stderr
    assert "uid=logged-model-uid" in result.stderr
    payload = json.loads(_result_stdout(result))
    assert payload["validation"]["status"] == "validated"
    assert payload["apply"]["status"] == "applied"
    assert payload["registered"] == []


def test_migrations_upgrade_dry_run_does_not_apply_or_sync_catalog(
    cli_mod,
    runner,
    monkeypatch,
    tmp_path,
):
    module_name = "dry_run_migration_provider_with_hook"
    provider_ref = _write_provider_with_register_hook(tmp_path, module_name=module_name)
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()
    migration_cli = importlib.import_module("mainsequence.cli.migrations")

    operations = []

    def fake_status(request, *, timeout=None):
        return AlembicMigrationStatusResponse(
            ok=True,
            alembic_version_meta_table_uid=request.alembic_version_meta_table_uid,
            alembic_version_table="public.alembic_version",
            data_source_uid="backend-data-source-uid",
            package=request.package,
            migration_namespace=request.migration_namespace,
            current_revision=None,
            current_revisions=[],
        )

    def fake_render(migration, **kwargs):
        return PackagedAlembicMigrationArtifact(
            manifest={
                "package": migration.package,
                "migration_namespace": migration.migration_namespace,
                "revision": "0001_initial",
                "down_revision": None,
                "direction": "upgrade",
                "alembic_version_table": "public.alembic_version",
            },
            sql="CREATE TABLE account (uid integer);",
            statement_boundaries=[],
        )

    def fake_apply(operation, *, timeout=None):
        operations.append(operation)
        return AlembicMigrationApplyResponse(
            ok=True,
            dry_run=operation.dry_run,
            alembic_version_meta_table_uid=operation.alembic_version_meta_table_uid,
            alembic_version_table="public.alembic_version",
            data_source_uid="backend-data-source-uid",
            package=operation.package,
            migration_namespace=operation.migration_namespace,
            revision=operation.revision,
            direction=operation.direction,
            status="validated",
            current_revision=None,
            current_revisions=[],
        )

    monkeypatch.setattr(migration_cli.MetaTable, "get_migration_status", staticmethod(fake_status))
    monkeypatch.setattr(migration_cli.MetaTable, "apply_migration", staticmethod(fake_apply))
    monkeypatch.setattr(migration_cli.MetaTable, "filter", staticmethod(lambda **kwargs: []))
    monkeypatch.setattr(
        migration_cli,
        "render_packaged_alembic_migration_for_provider",
        fake_render,
    )

    result = runner.invoke(
        cli_mod.app,
        ["migrations", "upgrade", "--provider", provider_ref, "--to", "head", "--dry-run"],
    )

    assert result.exit_code == 0
    assert [operation.dry_run for operation in operations] == [True]
    provider_module = importlib.import_module(module_name)
    assert provider_module.EVENTS == []


def test_migrations_upgrade_syncs_catalog_and_runs_provider_hook(
    cli_mod,
    runner,
    monkeypatch,
    tmp_path,
):
    module_name = "upgrade_migration_provider_with_hook"
    provider_ref = _write_provider_with_register_hook(tmp_path, module_name=module_name)
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()
    migration_cli = importlib.import_module("mainsequence.cli.migrations")

    def fake_status(request, *, timeout=None):
        return AlembicMigrationStatusResponse(
            ok=True,
            alembic_version_meta_table_uid=request.alembic_version_meta_table_uid,
            alembic_version_table="public.alembic_version",
            data_source_uid="backend-data-source-uid",
            package=request.package,
            migration_namespace=request.migration_namespace,
            current_revision=None,
            current_revisions=[],
        )

    def fake_render(migration, **kwargs):
        return PackagedAlembicMigrationArtifact(
            manifest={
                "package": migration.package,
                "migration_namespace": migration.migration_namespace,
                "revision": "0001_initial",
                "down_revision": None,
                "direction": "upgrade",
                "alembic_version_table": "public.alembic_version",
            },
            sql="CREATE TABLE account (uid integer);",
            statement_boundaries=[],
        )

    def fake_apply(operation, *, timeout=None):
        return AlembicMigrationApplyResponse(
            ok=True,
            dry_run=operation.dry_run,
            alembic_version_meta_table_uid=operation.alembic_version_meta_table_uid,
            alembic_version_table="public.alembic_version",
            data_source_uid="backend-data-source-uid",
            package=operation.package,
            migration_namespace=operation.migration_namespace,
            revision=operation.revision,
            direction=operation.direction,
            status="validated" if operation.dry_run else "applied",
            current_revision="0001_initial",
            current_revisions=["0001_initial"],
        )

    monkeypatch.setattr(migration_cli.MetaTable, "get_migration_status", staticmethod(fake_status))
    monkeypatch.setattr(migration_cli.MetaTable, "apply_migration", staticmethod(fake_apply))
    monkeypatch.setattr(migration_cli.MetaTable, "filter", staticmethod(lambda **kwargs: []))
    monkeypatch.setattr(
        migration_cli,
        "render_packaged_alembic_migration_for_provider",
        fake_render,
    )

    result = runner.invoke(
        cli_mod.app,
        [
            "migrations",
            "upgrade",
            "--provider",
            provider_ref,
            "--to",
            "head",
            "--json",
        ],
    )

    assert result.exit_code == 0
    provider_module = importlib.import_module(module_name)
    assert provider_module.EVENTS == [
        ("register", "account", None),
        ("hook", ["account-meta-table-uid"]),
    ]
    payload = json.loads(_result_stdout(result))
    assert payload["registered"] == [{"uid": "account-meta-table-uid"}]
