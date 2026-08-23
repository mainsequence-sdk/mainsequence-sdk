from __future__ import annotations

import importlib
import logging
import pathlib
import sys
import types
from collections.abc import Sequence

import pytest
from sqlalchemy import MetaData
from typer.testing import CliRunner

import mainsequence.project_context as project_context
from mainsequence.client.metatables import MetaTable
from mainsequence.client.metatables.core import MetaTableProjectContextRequest
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


def _migration(
    *,
    version_locations: str | Sequence[str] | None = None,
    version_path: str | None = None,
) -> AlembicMetaTableMigration:
    class Registry(AlembicVersionMetaTable):
        __metatable_uid__ = "registry-meta-table-uid"
        __metatable_data_source_uid__ = "data-source-uid"
        __metatable__ = MetaTable.model_construct(
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
        version_locations=version_locations,
        version_path=version_path,
    )


def _project_context() -> MetaTableProjectContextRequest:
    return MetaTableProjectContextRequest(
        project_branch_uid="11111111-1111-4111-8111-111111111111",
    )


@pytest.fixture(autouse=True)
def _resolved_project_branch_context(monkeypatch):
    project_uid = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    project_branch_uid = "11111111-1111-4111-8111-111111111111"
    environment_uid = "22222222-2222-4222-8222-222222222222"
    project_context._reset_project_runtime_context()
    source = project_context.GitProjectSourceContext(
        repository_root=pathlib.Path.cwd().resolve(),
        canonical_repository_identity="github.com/mainsequence-sdk/migrations",
        repository_branch="main",
        repository_ref="refs/heads/main",
        commit_sha="a" * 40,
    )
    monkeypatch.setattr(
        project_context,
        "_resolve_git_source_context",
        lambda project_dir: source,
    )
    project_context.get_project_runtime_context(
        _project_branch_context_loader=lambda resolved_source: types.SimpleNamespace(
            canonical_repository_identity=(resolved_source.canonical_repository_identity),
            repository_branch=resolved_source.repository_branch,
            repository_ref=resolved_source.repository_ref,
            commit_sha=resolved_source.commit_sha,
            project_branch=types.SimpleNamespace(
                uid=project_branch_uid,
                project_uid=project_uid,
                repository_branch=resolved_source.repository_branch,
                organization_project_environment_uid=environment_uid,
                metatables_data_source=None,
            ),
        ),
    )
    yield
    project_context._reset_project_runtime_context()


def _patch_preflight(monkeypatch, migration_cli, migration, *, emit_reservation=False):
    monkeypatch.setattr(migration_cli, "_load_migration", lambda provider: migration)
    monkeypatch.setattr(
        AlembicMetaTableMigration,
        "ensure_alembic_registry",
        lambda self, timeout=None, on_metatable_reserved=None: (
            self.alembic_registry.get_meta_table()
        ),
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
                    [
                        types.SimpleNamespace(
                            identifier="example_assets__account",
                            physical_table_name="example_assets__account",
                        )
                    ],
                )
            if on_metatable_reservation_status is not None:
                on_metatable_reservation_status(
                    "Serializing POST http://example.test/api/v1/meta-tables/ payload..."
                )
                on_metatable_reservation_status(
                    "Sending HTTP POST http://example.test/api/v1/meta-tables/..."
                )
                on_metatable_reservation_status(
                    "Received HTTP 200 from POST http://example.test/api/v1/meta-tables/."
                )
            on_metatable_reserved(
                Account,
                types.SimpleNamespace(
                    identifier="example_assets__account",
                    meta_table_uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                    physical_table_name="example_assets__account",
                    provisioning_status="reserved",
                    created=False,
                    matched_by="physical_table_name",
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
    import mainsequence.client.metatables.core as metatable_models

    monkeypatch.setattr(
        metatable_models,
        "_current_metatable_project_context",
        _project_context,
    )

    def fake_issue_migration_connection(self, request, *, timeout=None):
        captured["meta_table_uid"] = self.uid
        captured["connection_request"] = request
        captured["connection_timeout"] = timeout
        return types.SimpleNamespace(
            uri="postgresql://temporary-secret",
            owner_role_name="connection-owner",
        )

    monkeypatch.setattr(
        MetaTable,
        "issue_migration_connection",
        fake_issue_migration_connection,
    )


def test_migrations_scaffold_writes_sdk_owned_package(tmp_path):
    cli_mod = _load_cli_module()
    runner = CliRunner()

    result = runner.invoke(
        cli_mod.app,
        [
            "migrations",
            "scaffold",
            "--package",
            "msm",
            "--namespace",
            "mainsequence.examples",
            "--metadata",
            "msm.base:MarketsBase.metadata",
            "--base",
            "msm.base:MarketsBase",
            "--project-root",
            str(tmp_path),
        ],
    )

    assert result.exit_code == 0
    package_root = tmp_path / "src" / "migrations"
    assert (package_root / "__init__.py").exists()
    assert (package_root / "env.py").exists()
    assert (package_root / "script.py.mako").exists()
    assert (package_root / "versions" / "mainsequence_examples" / "__init__.py").exists()
    output = _combined_output(result)
    assert "[mainsequence migrations] Scaffold created" in output
    assert "mainsequence_examples" in output


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
        logging.getLogger("alembic.runtime.migration").info("fake alembic runtime log")
        logging.getLogger("alembic.runtime.migration.deep").debug("fake alembic debug log")
        config.print_stdout("fake alembic current output")

    monkeypatch.setattr(command, "current", fake_current)

    result = runner.invoke(
        cli_mod.app,
        ["migrations", "current", "--provider", "ignored:migration", "--timeout", "5"],
    )

    assert result.exit_code == 0
    assert captured["meta_table_uid"] == "registry-meta-table-uid"
    assert not hasattr(captured["connection_request"], "meta_table_uids")
    assert not hasattr(captured["connection_request"], "project_context")
    assert captured["connection_timeout"] == 5.0
    assert captured["sqlalchemy_url"] == "postgresql://temporary-secret"
    assert captured["owner_role"] == "connection-owner"
    assert captured["stdout"] is not None
    assert captured["output_buffer"] is not None
    assert "temporary-secret" not in result.output
    output = _combined_output(result)
    assert "fake alembic current output" in output
    assert "[alembic] INFO alembic.runtime.migration: fake alembic runtime log" in output
    assert "[alembic] DEBUG alembic.runtime.migration.deep: fake alembic debug log" in output
    assert "[mainsequence migrations] Importing Alembic command module for current..." in output
    assert "[mainsequence migrations] Imported Alembic command module for current." in output
    assert "[mainsequence migrations] Ensuring Alembic registry MetaTable..." in output
    assert (
        "[mainsequence migrations] Skipping provider MetaTable reservations "
        "for read-only Alembic command."
    ) in output
    assert "Requesting migration connection through Alembic registry MetaTable" in output
    assert "[mainsequence migrations] Building Alembic config..." in output
    assert "[mainsequence migrations] Alembic config built." in output
    assert "[mainsequence migrations] Starting Alembic current now..." in output
    assert "[mainsequence migrations] Alembic current finished." in output


def test_migrations_current_skips_provider_metatable_reservations(monkeypatch):
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
    assert "POST /api/v1/meta-tables/" not in output
    assert "reserved MetaTable identifier=example_assets__account" not in output
    assert not hasattr(captured["connection_request"], "meta_table_uids")
    assert (
        "[mainsequence migrations] Skipping provider MetaTable reservations "
        "for read-only Alembic command."
    ) in output


def test_migrations_current_prints_alembic_registry_reservation(monkeypatch):
    cli_mod = _load_cli_module()
    runner = CliRunner()
    migration_cli = importlib.import_module("mainsequence.cli.migrations")
    migration = _migration()
    captured = {}
    _patch_preflight(monkeypatch, migration_cli, migration)
    _patch_scoped_connection(monkeypatch, migration_cli, captured)

    def fake_ensure(
        self,
        timeout=None,
        on_metatable_reserved=None,
    ):
        if on_metatable_reserved is not None:
            on_metatable_reserved(
                self.alembic_registry,
                types.SimpleNamespace(
                    identifier="msm.alembic_version",
                    uid="registry-meta-table-uid",
                    table_contract={
                        "physical": {"table_name": "alembic_version"},
                    },
                ),
            )
        return self.alembic_registry.get_meta_table()

    monkeypatch.setattr(AlembicMetaTableMigration, "ensure_alembic_registry", fake_ensure)

    from alembic import command

    monkeypatch.setattr(command, "current", lambda config, verbose=False: None)

    result = runner.invoke(
        cli_mod.app,
        ["migrations", "current", "--provider", "ignored:migration", "--timeout", "5"],
    )

    assert result.exit_code == 0
    output = _combined_output(result)
    assert "POST /api/v1/meta-tables/" in output
    assert "reserved MetaTable identifier=msm.alembic_version" in output
    assert "uid=registry-meta-table-uid" in output
    assert "physical_table=alembic_version" in output


def test_migrations_current_reserves_scoped_platform_registry(monkeypatch):
    cli_mod = _load_cli_module()
    runner = CliRunner()
    migration_cli = importlib.import_module("mainsequence.cli.migrations")
    migration = _migration()
    captured = {}

    monkeypatch.setattr(migration_cli, "_load_migration", lambda provider: migration)

    import mainsequence.client.metatables.core as metatable_models

    class _Response:
        def __init__(self, payload, status_code):
            self._payload = payload
            self.status_code = status_code

        def json(self):
            return self._payload

    registry_payload = [
        {
            "uid": "registry-meta-table-uid",
            "data_source_uid": "data-source-uid",
            "identifier": "msm.alembic_version",
            "namespace": "msm",
            "management_mode": "platform_managed",
            "provisioning_status": "reserved",
            "schema_management_mode": "alembic_managed",
            "migration_package": "msm",
            "migration_namespace": "markets",
            "migration_provider_key": "msm:markets",
            "alembic_version_meta_table_uid": None,
            "physical_schema": "public",
            "physical_table_name": "alembic_version",
            "table_contract": {
                "version": "relational-table.v1",
                "physical": {
                    "schema": "public",
                    "table_name": "alembic_version",
                },
                "columns": [],
            },
        }
    ]

    def fake_make_request(**kwargs):
        if kwargs["url"].endswith("/filter/"):
            captured["registry_lookup"] = kwargs
            return _Response([], 200)
        captured["registry_request"] = kwargs
        return _Response(registry_payload, 201)

    monkeypatch.setattr(metatable_models, "make_request", fake_make_request)
    monkeypatch.setattr(
        MetaTable,
        "build_session",
        classmethod(lambda cls: types.SimpleNamespace(headers={})),
    )
    _patch_scoped_connection(monkeypatch, migration_cli, captured)

    from alembic import command

    monkeypatch.setattr(command, "current", lambda config, verbose=False: None)

    result = runner.invoke(
        cli_mod.app,
        ["migrations", "current", "--provider", "ignored:migration", "--timeout", "5"],
    )

    assert result.exit_code == 0
    registry_row = captured["registry_request"]["payload"]["json"][0]
    assert registry_row["management_mode"] == "platform_managed"
    assert registry_row["provisioning_status"] == "reserved"
    assert registry_row["is_alembic_managed"] is True
    assert registry_row["alembic_version_meta_table_uid"] is None
    assert registry_row["project_context"] == _project_context().model_dump(mode="json")
    registry = migration.alembic_registry.get_meta_table()
    assert registry is not None
    assert registry.management_mode == "platform_managed"
    assert registry.schema_management_mode == "alembic_managed"
    assert registry.provisioning_status == "reserved"
    assert "Alembic current finished." in _combined_output(result)


def test_migrations_current_reuses_registry_across_independent_cli_operations(
    monkeypatch,
):
    cli_mod = _load_cli_module()
    runner = CliRunner()
    migration_cli = importlib.import_module("mainsequence.cli.migrations")
    import mainsequence.client.metatables.core as metatable_models

    persisted_registry = None
    create_count = 0

    class _Response:
        def __init__(self, payload, status_code):
            self._payload = payload
            self.status_code = status_code
            self.content = b"{}"

        def json(self):
            return self._payload

    def registry_payload():
        return {
            "uid": "persistent-registry-meta-table-uid",
            "data_source_uid": "data-source-uid",
            "identifier": "msm.alembic_version",
            "namespace": "msm",
            "management_mode": "platform_managed",
            "provisioning_status": "reserved",
            "schema_management_mode": "alembic_managed",
            "migration_package": "msm",
            "migration_namespace": "markets",
            "migration_provider_key": "msm:markets",
            "alembic_version_meta_table_uid": None,
            "physical_schema": "public",
            "physical_table_name": "alembic_version",
            "table_contract": {
                "version": "relational-table.v1",
                "physical": {
                    "schema": "public",
                    "table_name": "alembic_version",
                },
                "columns": [],
            },
        }

    def fake_make_request(**kwargs):
        nonlocal persisted_registry, create_count
        if kwargs["url"].endswith("/filter/"):
            rows = [] if persisted_registry is None else [persisted_registry]
            return _Response(rows, 200)
        create_count += 1
        persisted_registry = registry_payload()
        return _Response([persisted_registry], 201)

    monkeypatch.setattr(migration_cli, "_load_migration", lambda provider: _migration())
    monkeypatch.setattr(metatable_models, "make_request", fake_make_request)
    monkeypatch.setattr(
        MetaTable,
        "build_session",
        classmethod(lambda cls: types.SimpleNamespace(headers={})),
    )
    monkeypatch.setattr(
        metatable_models,
        "_current_metatable_project_context",
        _project_context,
    )
    monkeypatch.setattr(
        MetaTable,
        "issue_migration_connection",
        lambda self, request, *, timeout=None: types.SimpleNamespace(
            uri="postgresql://temporary-secret",
            owner_role_name="connection-owner",
        ),
    )

    from alembic import command

    monkeypatch.setattr(command, "current", lambda config, verbose=False: None)

    first = runner.invoke(
        cli_mod.app,
        ["migrations", "current", "--provider", "ignored:migration"],
    )
    second = runner.invoke(
        cli_mod.app,
        ["migrations", "current", "--provider", "ignored:migration"],
    )

    assert first.exit_code == 0
    assert second.exit_code == 0
    assert create_count == 1
    assert "uid=persistent-registry-meta-table-uid" in _combined_output(second)


def test_migrations_revision_forwards_alembic_logs_and_scans_revision_id(monkeypatch):
    cli_mod = _load_cli_module()
    runner = CliRunner()
    migration_cli = importlib.import_module("mainsequence.cli.migrations")
    migration = _migration()
    captured = {}
    monkeypatch.setattr(migration_cli, "_load_migration", lambda provider: migration)

    def fail_backend_call(*args, **kwargs):
        raise AssertionError("revision must not provision MetaTables")

    monkeypatch.setattr(
        AlembicMetaTableMigration,
        "ensure_alembic_registry",
        fail_backend_call,
    )
    monkeypatch.setattr(
        AlembicMetaTableMigration,
        "prepare_for_alembic",
        fail_backend_call,
    )

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
        captured["sqlalchemy_url"] = config.get_main_option("sqlalchemy.url")
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
            "--autogenerate",
            "--sqlalchemy-url",
            "sqlite:///baseline.db",
        ],
    )

    assert result.exit_code == 0
    assert captured["message"] == "schema change"
    assert captured["autogenerate"] is True
    assert captured["rev_id"] == "0002"
    assert captured["head"] == "head"
    assert captured["sqlalchemy_url"] == "sqlite:///baseline.db"
    output = _combined_output(result)
    assert "[alembic] DEBUG alembic.script: fake revision scan log" in output
    assert "[alembic] DEBUG alembic.command: fake revision command log" in output
    assert "Ensuring Alembic registry MetaTable" not in output
    assert "Preparing platform-managed MetaTable reservations" not in output


def test_coerce_alembic_version_path_resolves_package_resource(tmp_path, monkeypatch):
    migration_cli = importlib.import_module("mainsequence.cli.migrations")
    package_root = tmp_path / "sample_provider"
    namespace_versions = package_root / "migrations" / "versions" / "mainsequence_examples"
    namespace_versions.mkdir(parents=True)
    (package_root / "__init__.py").write_text("", encoding="utf-8")
    (package_root / "migrations" / "__init__.py").write_text("", encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))

    assert migration_cli._coerce_alembic_version_path(
        "sample_provider:migrations/versions/mainsequence_examples"
    ) == str(namespace_versions)


def test_migrations_revision_passes_provider_version_path(monkeypatch):
    cli_mod = _load_cli_module()
    runner = CliRunner()
    migration_cli = importlib.import_module("mainsequence.cli.migrations")
    version_location = "msm:alembic/versions/mainsequence_examples"
    resolved_version_path = "/tmp/msm/alembic/versions/mainsequence_examples"
    migration = _migration(version_locations=[version_location], version_path=version_location)
    captured = {}
    monkeypatch.setattr(migration_cli, "_load_migration", lambda provider: migration)

    def fake_coerce_version_path(path):
        captured["unresolved_version_path"] = path
        return resolved_version_path

    monkeypatch.setattr(migration_cli, "_coerce_alembic_version_path", fake_coerce_version_path)

    def fail_backend_call(*args, **kwargs):
        raise AssertionError("revision must not provision MetaTables")

    monkeypatch.setattr(
        AlembicMetaTableMigration,
        "ensure_alembic_registry",
        fail_backend_call,
    )
    monkeypatch.setattr(
        AlembicMetaTableMigration,
        "prepare_for_alembic",
        fail_backend_call,
    )

    from alembic import command
    from alembic.script import ScriptDirectory

    class FakeScriptDirectory:
        def get_heads(self):
            return ["0004"]

    def fake_script_directory_from_config(config):
        captured["script_context_version_locations"] = config.get_main_option("version_locations")
        captured["script_context_path_separator"] = config.get_main_option("path_separator")
        return FakeScriptDirectory()

    monkeypatch.setattr(
        ScriptDirectory,
        "from_config",
        staticmethod(fake_script_directory_from_config),
    )

    def fake_revision(config, **kwargs):
        captured["revision_version_locations"] = config.get_main_option("version_locations")
        captured["revision_path_separator"] = config.get_main_option("path_separator")
        captured["version_path"] = kwargs["version_path"]
        return types.SimpleNamespace(revision=kwargs["rev_id"], path="/tmp/0005_migration.py")

    monkeypatch.setattr(command, "revision", fake_revision)

    result = runner.invoke(
        cli_mod.app,
        [
            "migrations",
            "revision",
            "--provider",
            "ignored:migration",
            "--rev-id",
            "0005",
            "--no-autogenerate",
        ],
    )

    assert result.exit_code == 0
    assert captured["script_context_version_locations"] == version_location
    assert captured["script_context_path_separator"] == "newline"
    assert captured["revision_version_locations"] == version_location
    assert captured["revision_path_separator"] == "newline"
    assert captured["unresolved_version_path"] == version_location
    assert captured["version_path"] == resolved_version_path


def test_migrations_revision_default_uses_registry_without_provider_reservations(monkeypatch):
    cli_mod = _load_cli_module()
    runner = CliRunner()
    migration_cli = importlib.import_module("mainsequence.cli.migrations")
    migration = _migration()
    captured = {}
    monkeypatch.setattr(migration_cli, "_load_migration", lambda provider: migration)
    _patch_scoped_connection(monkeypatch, migration_cli, captured)

    monkeypatch.setattr(
        AlembicMetaTableMigration,
        "ensure_alembic_registry",
        lambda self, timeout=None, on_metatable_reserved=None: (
            self.alembic_registry.get_meta_table()
        ),
    )

    def fail_backend_call(*args, **kwargs):
        raise AssertionError("revision must not reserve provider MetaTables")

    monkeypatch.setattr(
        AlembicMetaTableMigration,
        "prepare_for_alembic",
        fail_backend_call,
    )

    from alembic import command
    from alembic.script import ScriptDirectory

    class FakeScriptDirectory:
        def get_heads(self):
            return []

        def walk_revisions(self):
            return []

    monkeypatch.setattr(
        ScriptDirectory,
        "from_config",
        staticmethod(lambda config: FakeScriptDirectory()),
    )

    def fake_revision(config, message, autogenerate, rev_id, head):
        captured["autogenerate"] = autogenerate
        captured["sqlalchemy_url"] = config.get_main_option("sqlalchemy.url")
        return types.SimpleNamespace(revision=rev_id, path="/tmp/0001_migration.py")

    monkeypatch.setattr(command, "revision", fake_revision)

    result = runner.invoke(
        cli_mod.app,
        [
            "migrations",
            "revision",
            "--provider",
            "ignored:migration",
            "--timeout",
            "5",
            "--ttl-seconds",
            "60",
        ],
    )

    assert result.exit_code == 0
    assert captured["meta_table_uid"] == "registry-meta-table-uid"
    assert not hasattr(captured["connection_request"], "meta_table_uids")
    assert captured["connection_request"].ttl_seconds == 60
    assert captured["connection_request"].migration_provider_key == "msm:markets"
    assert captured["connection_timeout"] == 5.0
    assert captured["autogenerate"] is True
    assert captured["sqlalchemy_url"] == "postgresql://temporary-secret"
    output = _combined_output(result)
    assert "Ensuring Alembic registry MetaTable for revision" in output
    assert "Preparing platform-managed MetaTable reservations" not in output


def test_migrations_revision_no_autogenerate_does_not_touch_backend(monkeypatch):
    cli_mod = _load_cli_module()
    runner = CliRunner()
    migration_cli = importlib.import_module("mainsequence.cli.migrations")
    migration = _migration()
    captured = {}
    monkeypatch.setattr(migration_cli, "_load_migration", lambda provider: migration)

    def fail_backend_call(*args, **kwargs):
        raise AssertionError("revision must not provision MetaTables")

    monkeypatch.setattr(
        AlembicMetaTableMigration,
        "ensure_alembic_registry",
        fail_backend_call,
    )
    monkeypatch.setattr(
        AlembicMetaTableMigration,
        "prepare_for_alembic",
        fail_backend_call,
    )

    from alembic import command
    from alembic.script import ScriptDirectory

    class FakeScriptDirectory:
        def get_heads(self):
            return []

        def walk_revisions(self):
            return []

    monkeypatch.setattr(
        ScriptDirectory,
        "from_config",
        staticmethod(lambda config: FakeScriptDirectory()),
    )

    def fake_revision(config, message, autogenerate, rev_id, head):
        captured["autogenerate"] = autogenerate
        captured["sqlalchemy_url"] = config.get_main_option("sqlalchemy.url")
        return types.SimpleNamespace(revision=rev_id, path="/tmp/0001_migration.py")

    monkeypatch.setattr(command, "revision", fake_revision)

    result = runner.invoke(
        cli_mod.app,
        [
            "migrations",
            "revision",
            "--provider",
            "ignored:migration",
            "--no-autogenerate",
        ],
    )

    assert result.exit_code == 0
    assert captured["autogenerate"] is False
    assert captured["sqlalchemy_url"] == "postgresql://"
    output = _combined_output(result)
    assert "Ensuring Alembic registry MetaTable" not in output
    assert "Preparing platform-managed MetaTable reservations" not in output


def test_autogenerate_preflight_rejects_existing_head_with_no_visible_tables(
    monkeypatch,
):
    migration_cli = importlib.import_module("mainsequence.cli.migrations")
    monkeypatch.setattr(
        migration_cli,
        "_alembic_script_heads",
        lambda config: ["0001"],
    )
    prepared = types.SimpleNamespace(
        reserved_tables=[
            types.SimpleNamespace(
                physical_table_name="mt_asset",
                table_contract={"physical": {"schema": "main", "table_name": "mt_asset"}},
            )
        ]
    )
    config = types.SimpleNamespace(
        attributes={"mainsequence_migration_sqlalchemy_url": "sqlite:///:memory:"}
    )

    with pytest.raises(RuntimeError, match="duplicate initial create-all"):
        migration_cli._assert_autogenerate_baseline_visible(prepared, config)


def test_autogenerate_preflight_allows_existing_head_with_visible_baseline(
    monkeypatch,
    tmp_path,
):
    migration_cli = importlib.import_module("mainsequence.cli.migrations")
    monkeypatch.setattr(
        migration_cli,
        "_alembic_script_heads",
        lambda config: ["0001"],
    )
    db_path = tmp_path / "baseline.db"
    from sqlalchemy import create_engine, text

    engine = create_engine(f"sqlite:///{db_path}")
    try:
        with engine.begin() as connection:
            connection.execute(text("CREATE TABLE mt_asset (uid INTEGER PRIMARY KEY)"))
    finally:
        engine.dispose()

    prepared = types.SimpleNamespace(
        reserved_tables=[
            types.SimpleNamespace(
                physical_table_name="mt_asset",
                table_contract={"physical": {"schema": "main", "table_name": "mt_asset"}},
            )
        ]
    )
    config = types.SimpleNamespace(
        attributes={
            "mainsequence_migration_sqlalchemy_url": f"sqlite:///{db_path}",
        }
    )

    migration_cli._assert_autogenerate_baseline_visible(prepared, config)


def test_migrations_upgrade_calls_alembic_and_finalizes_catalog(monkeypatch):
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

    def fake_finalize(
        self,
        prepared=None,
        alembic_revision=None,
        timeout=None,
        on_metatable_finalized=None,
        on_metatable_finalize_status=None,
    ):
        captured["finalize_timeout"] = timeout
        captured["finalize_prepared"] = prepared
        captured["finalize_revision"] = alembic_revision
        if on_metatable_finalized is not None:

            class Asset:
                __metatable_identifier__ = "markets.Asset"

            on_metatable_finalized(
                Asset,
                types.SimpleNamespace(
                    identifier="markets.Asset",
                    meta_table_uid="asset-meta-table-uid",
                    physical_table_name="mt_asset",
                    provisioning_status="active",
                ),
            )
        return types.SimpleNamespace(
            ok=True,
            finalized_count=1,
            active_count=1,
            reserved_count=0,
            failed_count=0,
            tables=[],
        )

    monkeypatch.setattr(AlembicMetaTableMigration, "finalize_metatable_catalog", fake_finalize)

    result = runner.invoke(
        cli_mod.app,
        ["migrations", "upgrade", "--provider", "ignored:migration", "--timeout", "7", "head"],
    )

    assert result.exit_code == 0
    assert not hasattr(captured["connection_request"], "meta_table_uids")
    assert captured["upgrade_revision"] == "head"
    assert captured["upgrade_url"] == "postgresql://temporary-secret"
    assert captured["finalize_timeout"] == 7.0
    assert captured["finalize_revision"] == "head"
    assert captured["finalize_prepared"].meta_table_uids == [
        "meta-table-uid",
    ]
    assert "temporary-secret" not in result.output
    output = _combined_output(result)
    assert "POST /api/v1/meta-tables/finalize-managed/" in output
    assert "finalized MetaTable identifier=markets.Asset" in output
    assert "physical_table=mt_asset" in output


def test_migration_finalization_log_reports_existing_active_rows(monkeypatch):
    migration_cli = importlib.import_module("mainsequence.cli.migrations")
    messages = []
    monkeypatch.setattr(migration_cli, "_emit_progress", messages.append)

    class Asset:
        __metatable_identifier__ = "markets.Asset"

    migration_cli._emit_metatable_finalization(
        Asset,
        types.SimpleNamespace(
            identifier="markets.Asset",
            meta_table_uid="asset-meta-table-uid",
            physical_table_name="mt_asset",
            provisioning_status="active",
            finalized=False,
            physical_table_exists=True,
        ),
    )

    assert len(messages) == 1
    assert "active MetaTable identifier=markets.Asset" in messages[0]
    assert "finalize-failed" not in messages[0]
