from __future__ import annotations

import importlib
import textwrap
import types

import pytest
from sqlalchemy import Column, Integer, MetaData, Table

from mainsequence.client.metatables import MetaTable
from mainsequence.meta_tables.migrations import (
    DEFAULT_ALEMBIC_VERSION_COLUMN_NAME,
    DEFAULT_ALEMBIC_VERSION_IDENTIFIER,
    DEFAULT_ALEMBIC_VERSION_NAMESPACE,
    DEFAULT_ALEMBIC_VERSION_SCHEMA,
    DEFAULT_ALEMBIC_VERSION_TABLE_NAME,
    AlembicMetaTableMigration,
    AlembicVersionMetaTable,
    load_alembic_metatable_migration_provider,
    render_packaged_alembic_migration,
    render_packaged_alembic_migration_for_provider,
)


def test_alembic_version_metatable_builds_external_registration_request():
    request = AlembicVersionMetaTable.build_registration_request(
        data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd"
    )

    assert request.management_mode == "external_registered"
    assert request.identifier == DEFAULT_ALEMBIC_VERSION_IDENTIFIER
    assert request.namespace == DEFAULT_ALEMBIC_VERSION_NAMESPACE
    assert request.introspect is False
    assert request.table_contract.physical.table_name == DEFAULT_ALEMBIC_VERSION_TABLE_NAME
    assert len(request.table_contract.columns) == 1
    version_column = request.table_contract.columns[0]
    assert version_column.name == DEFAULT_ALEMBIC_VERSION_COLUMN_NAME
    assert version_column.data_type == "string"
    assert version_column.backend_type == "VARCHAR"
    assert version_column.nullable is False
    assert version_column.primary_key is True
    assert request.table_contract.constraints == []
    assert request.table_contract.indexes == []
    assert request.table_contract.foreign_keys == []
    assert request.table_contract.authoring == {
        "owner": "alembic",
        "schema": DEFAULT_ALEMBIC_VERSION_SCHEMA,
        "version_table": DEFAULT_ALEMBIC_VERSION_TABLE_NAME,
    }
    assert len(request.storage_hash) <= 63


def test_project_can_scope_alembic_version_metatable():
    class ProjectAlembicVersion(AlembicVersionMetaTable):
        __alembic_version_schema__ = "markets"
        __alembic_version_table_name__ = "alembic_version"
        __metatable_namespace__ = "msm"
        __metatable_identifier__ = "msm.alembic_version"
        __metatable_description__ = "Markets Alembic version table."

    request = ProjectAlembicVersion.build_registration_request(
        data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        labels=["migrations"],
    )

    assert request.identifier == "msm.alembic_version"
    assert request.namespace == "msm"
    assert request.description == "Markets Alembic version table."
    assert request.labels == ["migrations"]
    assert request.table_contract.columns[0].name == "version_num"
    assert request.table_contract.authoring["schema"] == "markets"


def test_alembic_version_metatable_register_posts_registration_request(monkeypatch):
    captured = {}

    def fake_register(request, *, timeout=None):
        captured["request"] = request
        captured["timeout"] = timeout
        return "registered"

    monkeypatch.setattr(MetaTable, "register", staticmethod(fake_register))

    result = AlembicVersionMetaTable.register(
        data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        timeout=10,
        schema="markets",
    )

    assert result == "registered"
    assert captured["timeout"] == 10
    assert captured["request"].data_source_uid == "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
    assert captured["request"].table_contract.columns[0].name == "version_num"
    assert captured["request"].table_contract.authoring["schema"] == "markets"


def test_alembic_version_metatable_binds_uid_and_data_source_uid(monkeypatch):
    class ProjectAlembicVersion(AlembicVersionMetaTable):
        __metatable_uid__ = None
        __metatable_data_source_uid__ = None

    def fake_register(request, *, timeout=None):
        return types.SimpleNamespace(
            uid="metatable-uid",
            data_source_uid=request.data_source_uid,
        )

    monkeypatch.setattr(MetaTable, "register", staticmethod(fake_register))

    ProjectAlembicVersion.register(data_source_uid="data-source-uid")

    assert ProjectAlembicVersion.get_meta_table_uid() == "metatable-uid"
    assert ProjectAlembicVersion.get_data_source_uid() == "data-source-uid"


def test_alembic_metatable_migration_uses_registry_for_data_source_uid():
    metadata = MetaData()
    Table("asset", metadata, Column("uid", Integer, primary_key=True), schema="public")

    class ProjectAlembicVersion(AlembicVersionMetaTable):
        __metatable_data_source_uid__ = "data-source-uid"

    migration = AlembicMetaTableMigration(
        package="msm",
        migration_namespace="markets",
        script_location="msm:alembic",
        target_metadata=metadata,
        alembic_registry=ProjectAlembicVersion,
        metatable_models=[],
    )

    assert migration.resolve_data_source_uid() == "data-source-uid"
    assert migration.alembic_version_table == "public.alembic_version"
    assert migration.include_name("asset", "table", {"schema_name": "public"}) is True
    assert migration.include_name("ignored", "table", {"schema_name": "public"}) is False


def test_alembic_metatable_migration_registers_registry_from_bound_data_source(monkeypatch):
    class ProjectAlembicVersion(AlembicVersionMetaTable):
        __metatable_uid__ = None
        __metatable_data_source_uid__ = "data-source-uid"

    migration = AlembicMetaTableMigration(
        package="msm",
        migration_namespace="markets",
        script_location="msm:alembic",
        target_metadata=MetaData(),
        alembic_registry=ProjectAlembicVersion,
    )
    captured = {}

    def fake_register(request, *, timeout=None):
        captured["request"] = request
        return types.SimpleNamespace(uid="registry-uid", data_source_uid=request.data_source_uid)

    monkeypatch.setattr(MetaTable, "register", staticmethod(fake_register))

    migration.register_alembic_registry()

    assert captured["request"].data_source_uid == "data-source-uid"
    assert ProjectAlembicVersion.get_meta_table_uid() == "registry-uid"


def test_alembic_metatable_migration_requires_callable_after_register_hook():
    class ProjectAlembicVersion(AlembicVersionMetaTable):
        __metatable_data_source_uid__ = "data-source-uid"

    with pytest.raises(TypeError, match="after_register_metatables must be callable"):
        AlembicMetaTableMigration(
            package="msm",
            migration_namespace="markets",
            script_location="msm:alembic",
            target_metadata=MetaData(),
            alembic_registry=ProjectAlembicVersion,
            after_register_metatables="not-callable",
        )


def test_alembic_metatable_migration_calls_after_register_hook_after_all_models():
    class ProjectAlembicVersion(AlembicVersionMetaTable):
        __metatable_data_source_uid__ = "data-source-uid"

    events = []

    class FirstModel:
        @classmethod
        def register(cls, *, data_source_uid=None, timeout=None):
            events.append(("first", data_source_uid, timeout))
            return types.SimpleNamespace(uid="first-uid")

    class SecondModel:
        @classmethod
        def register(cls, *, data_source_uid=None, timeout=None):
            events.append(("second", data_source_uid, timeout))
            return types.SimpleNamespace(uid="second-uid")

    def after_register(registered):
        events.append(("hook", [meta_table.uid for meta_table in registered]))

    migration = AlembicMetaTableMigration(
        package="msm",
        migration_namespace="markets",
        script_location="msm:alembic",
        target_metadata=MetaData(),
        alembic_registry=ProjectAlembicVersion,
        metatable_models=[FirstModel, SecondModel],
        after_register_metatables=after_register,
    )

    registered = migration.register_metatables(timeout=15)

    assert [meta_table.uid for meta_table in registered] == ["first-uid", "second-uid"]
    assert events == [
        ("first", "data-source-uid", 15),
        ("second", "data-source-uid", 15),
        ("hook", ["first-uid", "second-uid"]),
    ]


def test_alembic_metatable_migration_does_not_call_hook_when_model_registration_fails():
    class ProjectAlembicVersion(AlembicVersionMetaTable):
        __metatable_data_source_uid__ = "data-source-uid"

    events = []

    class FirstModel:
        @classmethod
        def register(cls, *, data_source_uid=None, timeout=None):
            events.append(("first", data_source_uid, timeout))
            return types.SimpleNamespace(uid="first-uid")

    class FailingModel:
        @classmethod
        def register(cls, *, data_source_uid=None, timeout=None):
            events.append(("failing", data_source_uid, timeout))
            raise RuntimeError("registration failed")

    def after_register(registered):
        events.append(("hook", registered))

    migration = AlembicMetaTableMigration(
        package="msm",
        migration_namespace="markets",
        script_location="msm:alembic",
        target_metadata=MetaData(),
        alembic_registry=ProjectAlembicVersion,
        metatable_models=[FirstModel, FailingModel],
        after_register_metatables=after_register,
    )

    with pytest.raises(RuntimeError, match="registration failed"):
        migration.register_metatables(timeout=15)

    assert events == [
        ("first", "data-source-uid", 15),
        ("failing", "data-source-uid", 15),
    ]


def test_load_alembic_metatable_migration_provider_by_reference(tmp_path, monkeypatch):
    provider_file = tmp_path / "mainsequence_migrations.py"
    provider_file.write_text(
        textwrap.dedent(
            """
            from sqlalchemy import MetaData
            from mainsequence.meta_tables.migrations import (
                AlembicMetaTableMigration,
                AlembicVersionMetaTable,
            )


            class Registry(AlembicVersionMetaTable):
                __metatable_data_source_uid__ = "data-source-uid"


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
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()

    migration = load_alembic_metatable_migration_provider(cwd=tmp_path)

    assert migration.package == "msm"
    assert migration.resolve_data_source_uid() == "data-source-uid"


def test_removed_registry_helpers_are_not_public():
    import mainsequence.meta_tables.migrations as migrations

    removed_names = {
        "Migration" + "MetaTable",
        "Packaged" + "MetaTable" + "Migration",
        "build_migration_registry_row",
        "build_registry_upsert_operation",
        "create_default_migration_registry_model",
        "create_default_alembic_version_model",
        "load_packaged_migration",
        "sync_packaged_migration",
    }

    for name in removed_names:
        assert not hasattr(migrations, name)


def test_apply_helpers_do_not_reintroduce_data_source_override():
    import mainsequence.meta_tables.migrations as migrations

    assert not hasattr(migrations, "get_migration_status")


def test_render_packaged_alembic_migration_builds_artifact(tmp_path, monkeypatch):
    package_name = "sample_alembic_package"
    _write_alembic_package(tmp_path, package_name)
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()

    artifact = render_packaged_alembic_migration(
        package=package_name,
        migration_namespace="markets",
        revision="0001_initial",
        down_revision=None,
        current_revision=None,
        alembic_version_table="public.alembic_version",
        statement_boundaries=[{"statement_index": 0, "start_line": 1, "end_line": 5}],
    )

    assert artifact.manifest == {
        "package": package_name,
        "migration_namespace": "markets",
        "revision": "0001_initial",
        "down_revision": None,
        "direction": "upgrade",
        "alembic_version_table": "public.alembic_version",
    }
    assert "CREATE TABLE public.asset" in artifact.sql
    assert "INSERT INTO public.alembic_version" in artifact.sql
    assert artifact.statement_boundaries == [{"statement_index": 0, "start_line": 1, "end_line": 5}]


def test_render_packaged_alembic_migration_requires_current_revision_for_downgrade():
    with pytest.raises(ValueError, match="current_revision is required"):
        render_packaged_alembic_migration(
            package="sample",
            migration_namespace="markets",
            revision="base",
            direction="downgrade",
        )


def test_render_packaged_alembic_migration_for_provider_resolves_head(tmp_path, monkeypatch):
    package_name = "provider_alembic_package"
    _write_alembic_package(tmp_path, package_name)
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()

    class ProjectAlembicVersion(AlembicVersionMetaTable):
        __metatable_data_source_uid__ = "data-source-uid"

    migration = AlembicMetaTableMigration(
        package=package_name,
        migration_namespace="markets",
        script_location=f"{package_name}:migrations",
        target_metadata=MetaData(),
        alembic_registry=ProjectAlembicVersion,
    )

    artifact = render_packaged_alembic_migration_for_provider(
        migration,
        revision="head",
        current_revision=None,
    )

    assert artifact.manifest["revision"] == "0001_initial"
    assert artifact.manifest["alembic_version_table"] == "public.alembic_version"
    assert "CREATE TABLE public.asset" in artifact.sql


def _write_alembic_package(tmp_path, package_name: str) -> None:
    package_dir = tmp_path / package_name
    migrations_dir = package_dir / "migrations"
    versions_dir = migrations_dir / "versions"
    versions_dir.mkdir(parents=True)
    (package_dir / "__init__.py").write_text("", encoding="utf-8")
    (migrations_dir / "__init__.py").write_text("", encoding="utf-8")
    (versions_dir / "__init__.py").write_text("", encoding="utf-8")
    (migrations_dir / "env.py").write_text(
        textwrap.dedent(
            """
            from alembic import context


            config = context.config


            def _version_table_parts(value):
                if "." in value:
                    schema, table = value.rsplit(".", 1)
                    return schema, table
                return None, value


            def run_migrations_offline():
                schema, table = _version_table_parts(
                    config.attributes.get("alembic_version_table", "alembic_version")
                )
                context.configure(
                    url=config.get_main_option("sqlalchemy.url"),
                    literal_binds=True,
                    dialect_opts={"paramstyle": "named"},
                    version_table=table,
                    version_table_schema=schema,
                )
                with context.begin_transaction():
                    context.run_migrations()


            run_migrations_offline()
            """
        ),
        encoding="utf-8",
    )
    (versions_dir / "0001_initial.py").write_text(
        textwrap.dedent(
            """
            from alembic import op
            import sqlalchemy as sa


            revision = "0001_initial"
            down_revision = None
            branch_labels = None
            depends_on = None


            def upgrade():
                op.create_table(
                    "asset",
                    sa.Column("uid", sa.Integer(), primary_key=True),
                    schema="public",
                )


            def downgrade():
                op.drop_table("asset", schema="public")
            """
        ),
        encoding="utf-8",
    )
