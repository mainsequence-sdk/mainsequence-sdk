from __future__ import annotations

import importlib
import pathlib
import sys
import types

from sqlalchemy import MetaData
from typer.testing import CliRunner

from mainsequence.meta_tables.migrations import (
    AlembicMetaTableMigration,
    AlembicVersionMetaTable,
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


def _migration() -> AlembicMetaTableMigration:
    class Registry(AlembicVersionMetaTable):
        __metatable_uid__ = "registry-meta-table-uid"
        __metatable_data_source_uid__ = "data-source-uid"
        __metatable__ = types.SimpleNamespace(
            uid=__metatable_uid__,
            data_source_uid=__metatable_data_source_uid__,
        )
        __metatable_namespace__ = "msm"
        __metatable_identifier__ = "msm.alembic_version"

    return AlembicMetaTableMigration(
        package="msm",
        migration_namespace="markets",
        script_location="msm:alembic",
        target_metadata=MetaData(),
        alembic_registry=Registry,
    )


def _patch_preflight(monkeypatch, migration_cli, migration):
    monkeypatch.setattr(migration_cli, "_load_migration", lambda provider: migration)
    monkeypatch.setattr(
        AlembicMetaTableMigration,
        "ensure_alembic_registry",
        lambda self, timeout=None: None,
    )
    monkeypatch.setattr(
        AlembicMetaTableMigration,
        "prepare_for_alembic",
        lambda self, timeout=None: types.SimpleNamespace(
            data_source_uid="data-source-uid",
            meta_table_uids=["meta-table-uid"],
            owner_role_name="prepared-owner",
        ),
    )


def _patch_scoped_connection(monkeypatch, migration_cli, captured):
    class FakeDataSource:
        def issue_migration_connection(self, request, *, timeout=None):
            captured["connection_request"] = request
            captured["connection_timeout"] = timeout
            return types.SimpleNamespace(
                uri="postgresql://temporary-secret",
                owner_role_name="connection-owner",
            )

    def fake_get_by_uid(uid):
        captured["data_source_uid"] = uid
        return FakeDataSource()

    monkeypatch.setattr(
        migration_cli.DynamicTableDataSource,
        "get_by_uid",
        staticmethod(fake_get_by_uid),
    )


def test_migrations_current_uses_scoped_connection_without_printing_secret(monkeypatch):
    cli_mod = _load_cli_module()
    runner = CliRunner()
    migration_cli = importlib.import_module("mainsequence.cli.migrations")
    migration = _migration()
    captured = {}
    _patch_preflight(monkeypatch, migration_cli, migration)
    _patch_scoped_connection(monkeypatch, migration_cli, captured)

    from alembic import command

    def fake_current(config, verbose=False):
        captured["sqlalchemy_url"] = config.get_main_option("sqlalchemy.url")
        captured["owner_role"] = config.get_main_option("mainsequence.owner_role_name")
        captured["verbose"] = verbose

    monkeypatch.setattr(command, "current", fake_current)

    result = runner.invoke(
        cli_mod.app,
        ["migrations", "current", "--provider", "ignored:migration", "--timeout", "5"],
    )

    assert result.exit_code == 0
    assert captured["data_source_uid"] == "data-source-uid"
    assert captured["connection_request"].meta_table_uids == ["meta-table-uid"]
    assert captured["connection_timeout"] == 5.0
    assert captured["sqlalchemy_url"] == "postgresql://temporary-secret"
    assert captured["owner_role"] == "connection-owner"
    assert "temporary-secret" not in result.output


def test_migrations_upgrade_calls_alembic_and_refreshes_catalog(monkeypatch):
    cli_mod = _load_cli_module()
    runner = CliRunner()
    migration_cli = importlib.import_module("mainsequence.cli.migrations")
    migration = _migration()
    captured = {}
    _patch_preflight(monkeypatch, migration_cli, migration)
    _patch_scoped_connection(monkeypatch, migration_cli, captured)

    from alembic import command

    def fake_upgrade(config, revision):
        captured["upgrade_revision"] = revision
        captured["upgrade_url"] = config.get_main_option("sqlalchemy.url")

    monkeypatch.setattr(command, "upgrade", fake_upgrade)

    def fake_refresh(self, timeout=None):
        captured["refresh_timeout"] = timeout
        return []

    monkeypatch.setattr(AlembicMetaTableMigration, "refresh_metatable_catalog", fake_refresh)

    result = runner.invoke(
        cli_mod.app,
        ["migrations", "upgrade", "--provider", "ignored:migration", "--timeout", "7", "head"],
    )

    assert result.exit_code == 0
    assert captured["connection_request"].meta_table_uids == ["meta-table-uid"]
    assert captured["upgrade_revision"] == "head"
    assert captured["upgrade_url"] == "postgresql://temporary-secret"
    assert captured["refresh_timeout"] == 7.0
    assert "temporary-secret" not in result.output
