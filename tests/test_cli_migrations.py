from __future__ import annotations

import importlib
import logging
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


def _patch_preflight(monkeypatch, migration_cli, migration, *, emit_reservation=False):
    monkeypatch.setattr(migration_cli, "_load_migration", lambda provider: migration)
    monkeypatch.setattr(
        AlembicMetaTableMigration,
        "ensure_alembic_registry",
        lambda self, timeout=None, on_metatable_registered=None: None,
    )

    def fake_prepare_for_alembic(
        self,
        timeout=None,
        on_metatable_reservation_request=None,
        on_metatable_reservation_status=None,
        on_metatable_reserved=None,
    ):
        if emit_reservation and on_metatable_reserved is not None:
            class Account:
                __metatable_identifier__ = "Account"

            if on_metatable_reservation_request is not None:
                on_metatable_reservation_request(
                    [Account],
                    [types.SimpleNamespace(identifier="Account")],
                )
            if on_metatable_reservation_status is not None:
                on_metatable_reservation_status(
                    "Serializing POST http://example.test/reserve-managed/ payload..."
                )
                on_metatable_reservation_status(
                    "Sending HTTP POST http://example.test/reserve-managed/..."
                )
                on_metatable_reservation_status(
                    "Received HTTP 200 from POST http://example.test/reserve-managed/."
                )
            on_metatable_reserved(
                Account,
                types.SimpleNamespace(
                    identifier="Account",
                    meta_table_uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                    physical_table_name="mt_account_backend",
                    provisioning_status="reserved",
                    created=False,
                    matched_by="identifier",
                ),
            )
        return types.SimpleNamespace(
            data_source_uid="data-source-uid",
            meta_table_uids=["meta-table-uid"],
            owner_role_name="prepared-owner",
        )

    monkeypatch.setattr(
        AlembicMetaTableMigration,
        "prepare_for_alembic",
        fake_prepare_for_alembic,
    )


def _combined_output(result):
    output = result.output
    try:
        output += result.stderr
    except ValueError:
        pass
    return output


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
        captured["stdout"] = config.stdout
        captured["output_buffer"] = config.output_buffer
        logging.getLogger("alembic.runtime.migration").info(
            "fake alembic runtime log"
        )
        logging.getLogger("alembic.runtime.migration.deep").debug(
            "fake alembic debug log"
        )
        config.print_stdout("fake alembic current output")

    monkeypatch.setattr(command, "current", fake_current)

    result = runner.invoke(
        cli_mod.app,
        ["migrations", "current", "--provider", "ignored:migration", "--timeout", "5"],
    )

    assert result.exit_code == 0
    assert captured["data_source_uid"] == "data-source-uid"
    assert captured["connection_request"].meta_table_uids == [
        "registry-meta-table-uid",
        "meta-table-uid",
    ]
    assert captured["connection_timeout"] == 5.0
    assert captured["sqlalchemy_url"] == "postgresql://temporary-secret"
    assert captured["owner_role"] == "connection-owner"
    assert captured["stdout"] is not None
    assert captured["output_buffer"] is not None
    assert "temporary-secret" not in result.output
    output = _combined_output(result)
    assert "fake alembic current output" in output
    assert (
        "[alembic] INFO alembic.runtime.migration: fake alembic runtime log"
        in output
    )
    assert (
        "[alembic] DEBUG alembic.runtime.migration.deep: fake alembic debug log"
        in output
    )
    assert (
        "[mainsequence migrations] Importing Alembic command module for current..."
        in output
    )
    assert (
        "[mainsequence migrations] Imported Alembic command module for current."
        in output
    )
    assert "[mainsequence migrations] Ensuring Alembic registry MetaTable..." in output
    assert (
        "[mainsequence migrations] Preparing platform-managed MetaTable reservations..."
        in output
    )
    assert "[mainsequence migrations] Loading DynamicTableDataSource uid=data-source-uid..." in output
    assert "[mainsequence migrations] Requesting scoped migration connection" in output
    assert "[mainsequence migrations] Building Alembic config..." in output
    assert "[mainsequence migrations] Alembic config built." in output
    assert "[mainsequence migrations] Starting Alembic current now..." in output
    assert "[mainsequence migrations] Alembic current finished." in output


def test_migrations_current_prints_metatable_reservations(monkeypatch):
    cli_mod = _load_cli_module()
    runner = CliRunner()
    migration_cli = importlib.import_module("mainsequence.cli.migrations")
    migration = _migration()
    captured = {}
    _patch_preflight(monkeypatch, migration_cli, migration, emit_reservation=True)
    _patch_scoped_connection(monkeypatch, migration_cli, captured)

    from alembic import command

    monkeypatch.setattr(command, "current", lambda config, verbose=False: None)

    result = runner.invoke(
        cli_mod.app,
        ["migrations", "current", "--provider", "ignored:migration", "--timeout", "5"],
    )

    assert result.exit_code == 0
    output = _combined_output(result)
    assert (
        "Sending POST /orm/api/ts_manager/meta_table/reserve-managed/ request for "
        "1 MetaTables identifiers=Account"
    ) in output
    assert "POST /orm/api/ts_manager/meta_table/reserve-managed/" in output
    assert "reserved MetaTable identifier=Account" in output
    assert "uid=aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa" in output
    assert "physical_table=mt_account_backend" in output
    assert "provisioning_status=reserved" in output
    assert "created=False" in output
    assert "matched_by=identifier" in output
    assert "Serializing POST http://example.test/reserve-managed/ payload..." in output
    assert "Sending HTTP POST http://example.test/reserve-managed/..." in output
    assert "Received HTTP 200 from POST http://example.test/reserve-managed/." in output


def test_migrations_current_prints_alembic_registry_registration(monkeypatch):
    cli_mod = _load_cli_module()
    runner = CliRunner()
    migration_cli = importlib.import_module("mainsequence.cli.migrations")
    migration = _migration()
    captured = {}
    _patch_preflight(monkeypatch, migration_cli, migration)
    _patch_scoped_connection(monkeypatch, migration_cli, captured)

    def fake_ensure(self, timeout=None, on_metatable_registered=None):
        if on_metatable_registered is not None:
            on_metatable_registered(
                self.alembic_registry,
                types.SimpleNamespace(
                    identifier="msm.alembic_version",
                    uid="registry-meta-table-uid",
                    table_contract={
                        "physical": {"table_name": "alembic_version"},
                    },
                ),
            )

    monkeypatch.setattr(AlembicMetaTableMigration, "ensure_alembic_registry", fake_ensure)

    from alembic import command

    monkeypatch.setattr(command, "current", lambda config, verbose=False: None)

    result = runner.invoke(
        cli_mod.app,
        ["migrations", "current", "--provider", "ignored:migration", "--timeout", "5"],
    )

    assert result.exit_code == 0
    output = _combined_output(result)
    assert "POST /orm/api/ts_manager/meta_table/register/" in output
    assert "registered MetaTable identifier=msm.alembic_version" in output
    assert "uid=registry-meta-table-uid" in output
    assert "physical_table=alembic_version" in output


def test_migrations_revision_forwards_alembic_logs_and_scans_revision_id(monkeypatch):
    cli_mod = _load_cli_module()
    runner = CliRunner()
    migration_cli = importlib.import_module("mainsequence.cli.migrations")
    migration = _migration()
    captured = {}
    _patch_preflight(monkeypatch, migration_cli, migration)
    _patch_scoped_connection(monkeypatch, migration_cli, captured)

    from alembic import command
    from alembic.script import ScriptDirectory

    class FakeRevision:
        revision = "0001"

    class FakeScriptDirectory:
        def get_heads(self):
            return ["0001"]

        def walk_revisions(self):
            logging.getLogger("alembic.script").debug("fake revision scan log")
            return [FakeRevision()]

    monkeypatch.setattr(
        ScriptDirectory,
        "from_config",
        staticmethod(lambda config: FakeScriptDirectory()),
    )

    def fake_revision(config, message, autogenerate, rev_id, head):
        captured["message"] = message
        captured["autogenerate"] = autogenerate
        captured["rev_id"] = rev_id
        captured["head"] = head
        logging.getLogger("alembic.command").debug("fake revision command log")
        return types.SimpleNamespace(revision=rev_id, path="/tmp/0002_migration.py")

    monkeypatch.setattr(command, "revision", fake_revision)

    result = runner.invoke(
        cli_mod.app,
        [
            "migrations",
            "revision",
            "--provider",
            "ignored:migration",
            "--message",
            "schema change",
            "--timeout",
            "5",
        ],
    )

    assert result.exit_code == 0
    assert captured["message"] == "schema change"
    assert captured["autogenerate"] is True
    assert captured["rev_id"] == "0002"
    assert captured["head"] == "head"
    output = _combined_output(result)
    assert "[alembic] DEBUG alembic.script: fake revision scan log" in output
    assert "[alembic] DEBUG alembic.command: fake revision command log" in output


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

    def fake_refresh(self, timeout=None, on_metatable_registered=None):
        captured["refresh_timeout"] = timeout
        if on_metatable_registered is not None:
            class Asset:
                __metatable_identifier__ = "markets.Asset"

            on_metatable_registered(
                Asset,
                types.SimpleNamespace(
                    identifier="markets.Asset",
                    uid="asset-meta-table-uid",
                    physical_table_name="mt_asset",
                ),
            )
        return []

    monkeypatch.setattr(AlembicMetaTableMigration, "refresh_metatable_catalog", fake_refresh)

    result = runner.invoke(
        cli_mod.app,
        ["migrations", "upgrade", "--provider", "ignored:migration", "--timeout", "7", "head"],
    )

    assert result.exit_code == 0
    assert captured["connection_request"].meta_table_uids == [
        "registry-meta-table-uid",
        "meta-table-uid",
    ]
    assert captured["upgrade_revision"] == "head"
    assert captured["upgrade_url"] == "postgresql://temporary-secret"
    assert captured["refresh_timeout"] == 7.0
    assert "temporary-secret" not in result.output
    output = _combined_output(result)
    assert "POST /orm/api/ts_manager/meta_table/register/" in output
    assert "registered MetaTable identifier=markets.Asset" in output
    assert "physical_table=mt_asset" in output
