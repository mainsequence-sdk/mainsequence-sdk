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
from mainsequence.meta_tables.migrations import PackagedAlembicMigrationArtifact


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
            from sqlalchemy import MetaData
            from mainsequence.meta_tables.migrations import (
                AlembicMetaTableMigration,
                AlembicVersionMetaTable,
            )


            class Registry(AlembicVersionMetaTable):
                __metatable_uid__ = "registry-meta-table-uid"
                __metatable_data_source_uid__ = "data-source-uid"
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
            from sqlalchemy import MetaData
            from mainsequence.meta_tables.migrations import (
                AlembicMetaTableMigration,
                AlembicVersionMetaTable,
            )


            EVENTS = []


            class Registry(AlembicVersionMetaTable):
                __metatable_uid__ = "registry-meta-table-uid"
                __metatable_data_source_uid__ = "data-source-uid"
                __metatable_namespace__ = "msm"
                __metatable_identifier__ = "msm.alembic_version"


            class AccountModel:
                @classmethod
                def register(cls, *, data_source_uid=None, timeout=None):
                    EVENTS.append(("register", "account", data_source_uid, timeout))
                    return {"uid": "account-meta-table-uid", "data_source_uid": data_source_uid}


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
            data_source_uid=request.data_source_uid,
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
    assert captured["request"].data_source_uid == "data-source-uid"
    payload = json.loads(result.output)
    assert payload["current_revision"] == "0001_initial"


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

    def fake_status(request, *, timeout=None):
        return AlembicMigrationStatusResponse(
            ok=True,
            alembic_version_meta_table_uid=request.alembic_version_meta_table_uid,
            alembic_version_table="public.alembic_version",
            data_source_uid=request.data_source_uid,
            package=request.package,
            migration_namespace=request.migration_namespace,
            current_revision=None,
            current_revisions=[],
        )

    def fake_render(migration, **kwargs):
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
            data_source_uid=operation.data_source_uid,
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
    monkeypatch.setattr(
        migration_cli,
        "render_packaged_alembic_migration_for_provider",
        fake_render,
    )

    result = runner.invoke(
        cli_mod.app,
        ["migrations", "upgrade", "--provider", provider_ref, "--to", "head", "--apply", "--json"],
    )

    assert result.exit_code == 0
    assert [operation.dry_run for operation in operations] == [True, False]
    assert {operation.sql for operation in operations} == {"CREATE TABLE asset (uid integer);"}
    assert {operation.data_source_uid for operation in operations} == {"data-source-uid"}
    assert {operation.alembic_version_meta_table_uid for operation in operations} == {
        "registry-meta-table-uid"
    }
    payload = json.loads(result.output)
    assert payload["validation"]["status"] == "validated"
    assert payload["apply"]["status"] == "applied"


def test_migrations_upgrade_apply_register_metatables_runs_provider_hook(
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
            data_source_uid=request.data_source_uid,
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
            data_source_uid=operation.data_source_uid,
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
            "--apply",
            "--register-metatables",
            "--json",
        ],
    )

    assert result.exit_code == 0
    provider_module = importlib.import_module(module_name)
    assert provider_module.EVENTS == [
        ("register", "account", "data-source-uid", None),
        ("hook", ["account-meta-table-uid"]),
    ]
    payload = json.loads(result.output)
    assert payload["registered"] == [
        {"uid": "account-meta-table-uid", "data_source_uid": "data-source-uid"}
    ]
