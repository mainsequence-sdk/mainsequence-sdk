from __future__ import annotations

import importlib
import textwrap
import types
import uuid

import pytest
from sqlalchemy import Column, Integer, MetaData, Table, Uuid
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from mainsequence.client.metatables import MetaTable
from mainsequence.meta_tables import PlatformManagedMetaTable
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
    class ProjectAlembicVersion(AlembicVersionMetaTable):
        __metatable_data_source_uid__ = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"

    captured = {}

    def fake_register(request, *, timeout=None):
        captured["request"] = request
        captured["timeout"] = timeout
        return types.SimpleNamespace(
            uid="registered-uid",
            data_source_uid=request.data_source_uid,
        )

    monkeypatch.setattr(MetaTable, "register", staticmethod(fake_register))

    result = ProjectAlembicVersion.register(
        timeout=10,
        schema="markets",
    )

    assert result.uid == "registered-uid"
    assert captured["timeout"] == 10
    assert captured["request"].data_source_uid == "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
    assert captured["request"].table_contract.columns[0].name == "version_num"
    assert captured["request"].table_contract.authoring["schema"] == "markets"


def test_alembic_version_metatable_binds_uid_and_data_source_uid(monkeypatch):
    class ProjectAlembicVersion(AlembicVersionMetaTable):
        __metatable_uid__ = None
        __metatable_data_source_uid__ = "data-source-uid"

    def fake_register(request, *, timeout=None):
        return types.SimpleNamespace(
            uid="metatable-uid",
            data_source_uid=request.data_source_uid,
        )

    monkeypatch.setattr(MetaTable, "register", staticmethod(fake_register))

    ProjectAlembicVersion.register()

    assert ProjectAlembicVersion.get_meta_table_uid() == "metatable-uid"
    assert ProjectAlembicVersion.get_data_source_uid() == "data-source-uid"


def test_alembic_metatable_migration_uses_registry_for_version_table_config():
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


def test_alembic_metatable_migration_syncs_catalog_and_calls_after_register_hook(monkeypatch):
    class ProjectAlembicVersion(AlembicVersionMetaTable):
        __metatable_data_source_uid__ = "data-source-uid"

    events = []

    class FirstModel:
        __metatable_identifier__ = "global.first"

        @classmethod
        def _bind_meta_table(cls, meta_table):
            events.append(("bind", "first", meta_table.uid))

        @classmethod
        def register(cls, *, timeout=None):
            events.append(("first", timeout))
            return types.SimpleNamespace(uid="first-uid")

    class SecondModel:
        __metatable_identifier__ = "global.second"

        @classmethod
        def _bind_meta_table(cls, meta_table):
            events.append(("bind", "second", meta_table.uid))

        @classmethod
        def register(cls, *, timeout=None):
            events.append(("second", timeout))
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

    filter_calls = []

    def fake_filter(**kwargs):
        filter_calls.append(kwargs)
        return [
            types.SimpleNamespace(
                identifier="global.first",
                uid="existing-first-uid",
            )
        ]

    monkeypatch.setattr(MetaTable, "filter", staticmethod(fake_filter))

    registered = migration.sync_metatable_catalog(timeout=15)

    assert [meta_table.uid for meta_table in registered] == ["first-uid", "second-uid"]
    assert filter_calls == [
        {
            "identifier__in": ["global.first", "global.second"],
            "timeout": 15,
        }
    ]
    assert events == [
        ("bind", "first", "existing-first-uid"),
        ("first", 15),
        ("second", 15),
        ("hook", ["first-uid", "second-uid"]),
    ]


def test_alembic_metatable_migration_registers_missing_platform_managed_model(
    monkeypatch,
):
    class Base(DeclarativeBase):
        metadata = MetaData()

    class ProjectAlembicVersion(AlembicVersionMetaTable):
        __metatable_data_source_uid__ = "data-source-uid"

    class Asset(PlatformManagedMetaTable, Base):
        __metatable_namespace__ = "markets"
        __metatable_identifier__ = "markets.Asset"
        __metatable_data_source_uid__ = "data-source-uid"

        uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)

    migration = AlembicMetaTableMigration(
        package="msm",
        migration_namespace="markets",
        script_location="msm:alembic",
        target_metadata=Base.metadata,
        alembic_registry=ProjectAlembicVersion,
        metatable_models=[Asset],
    )

    filter_calls = []

    def fake_filter(**kwargs):
        filter_calls.append(kwargs)
        return []

    monkeypatch.setattr(MetaTable, "filter", staticmethod(fake_filter))

    captured = {}

    def fake_register(request, *, timeout=None):
        captured["request"] = request
        captured["timeout"] = timeout
        return types.SimpleNamespace(
            uid="asset-meta-table-uid",
            data_source_uid=request.data_source_uid,
            storage_hash=request.storage_hash,
            physical_table_name="mt_asset",
        )

    monkeypatch.setattr(MetaTable, "register", staticmethod(fake_register))

    resolved = migration.resolve_or_register_metatable_models(timeout=20)

    assert [meta_table.uid for meta_table in resolved] == ["asset-meta-table-uid"]
    assert filter_calls == [
        {
            "identifier__in": ["markets.Asset"],
            "timeout": 20,
        }
    ]
    assert captured["request"].identifier == "markets.Asset"
    assert captured["timeout"] == 20
    assert Asset.get_meta_table_uid() == "asset-meta-table-uid"
    assert Asset.get_physical_table_name() == "mt_asset"
    assert Asset.__table__.name == "mt_asset"


def test_alembic_metatable_migration_bulk_resolves_existing_models(monkeypatch):
    class Base(DeclarativeBase):
        metadata = MetaData()

    class ProjectAlembicVersion(AlembicVersionMetaTable):
        __metatable_data_source_uid__ = "data-source-uid"

    class Asset(PlatformManagedMetaTable, Base):
        __metatable_namespace__ = "markets"
        __metatable_identifier__ = "markets.Asset"
        __metatable_data_source_uid__ = "data-source-uid"

        uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)

    class Account(PlatformManagedMetaTable, Base):
        __metatable_namespace__ = "markets"
        __metatable_identifier__ = "markets.Account"
        __metatable_data_source_uid__ = "data-source-uid"

        uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
        account_number: Mapped[int] = mapped_column(Integer)

    migration = AlembicMetaTableMigration(
        package="msm",
        migration_namespace="markets",
        script_location="msm:alembic",
        target_metadata=Base.metadata,
        alembic_registry=ProjectAlembicVersion,
        metatable_models=[Asset, Account],
    )

    filter_calls = []

    def fake_filter(**kwargs):
        filter_calls.append(kwargs)
        return [
            types.SimpleNamespace(
                identifier="markets.Asset",
                uid="asset-meta-table-uid",
                data_source_uid="data-source-uid",
                storage_hash="asset-storage-hash",
                physical_table_name="mt_asset",
            ),
            types.SimpleNamespace(
                identifier="markets.Account",
                uid="account-meta-table-uid",
                data_source_uid="data-source-uid",
                storage_hash="account-storage-hash",
                physical_table_name="mt_account",
            ),
        ]

    monkeypatch.setattr(MetaTable, "filter", staticmethod(fake_filter))
    monkeypatch.setattr(
        MetaTable,
        "register",
        staticmethod(lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError())),
    )

    events = []
    resolved = migration.resolve_or_register_metatable_models(
        timeout=30,
        on_metatable_resolution=lambda model, identifier, status, meta_table: events.append(
            (identifier, status, meta_table.uid)
        ),
    )

    assert filter_calls == [
        {
            "identifier__in": ["markets.Asset", "markets.Account"],
            "timeout": 30,
        }
    ]
    assert [meta_table.uid for meta_table in resolved] == [
        "asset-meta-table-uid",
        "account-meta-table-uid",
    ]
    assert events == [
        ("markets.Asset", "exists", "asset-meta-table-uid"),
        ("markets.Account", "exists", "account-meta-table-uid"),
    ]
    assert Asset.get_meta_table_uid() == "asset-meta-table-uid"
    assert Account.get_meta_table_uid() == "account-meta-table-uid"


def test_alembic_metatable_migration_does_not_call_hook_when_model_registration_fails(
    monkeypatch,
):
    class ProjectAlembicVersion(AlembicVersionMetaTable):
        __metatable_data_source_uid__ = "data-source-uid"

    events = []

    class FirstModel:
        __metatable_identifier__ = "global.first"

        @classmethod
        def register(cls, *, timeout=None):
            events.append(("first", timeout))
            return types.SimpleNamespace(uid="first-uid")

    class FailingModel:
        __metatable_identifier__ = "global.failing"

        @classmethod
        def register(cls, *, timeout=None):
            events.append(("failing", timeout))
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

    monkeypatch.setattr(MetaTable, "filter", staticmethod(lambda **kwargs: []))

    with pytest.raises(RuntimeError, match="registration failed"):
        migration.sync_metatable_catalog(timeout=15)

    assert events == [
        ("first", 15),
        ("failing", 15),
    ]


def test_alembic_metatable_migration_fails_on_duplicate_identifier(monkeypatch):
    class ProjectAlembicVersion(AlembicVersionMetaTable):
        __metatable_data_source_uid__ = "data-source-uid"

    class AssetModel:
        __metatable_identifier__ = "global.asset"

        @classmethod
        def register(cls, *, timeout=None):
            raise AssertionError("register should not run for duplicate identifiers")

    migration = AlembicMetaTableMigration(
        package="msm",
        migration_namespace="markets",
        script_location="msm:alembic",
        target_metadata=MetaData(),
        alembic_registry=ProjectAlembicVersion,
        metatable_models=[AssetModel],
    )

    monkeypatch.setattr(
        MetaTable,
        "filter",
        staticmethod(
            lambda **kwargs: [
                types.SimpleNamespace(uid="first"),
                types.SimpleNamespace(uid="second"),
            ]
        ),
    )

    with pytest.raises(ValueError, match="not globally unique"):
        migration.sync_metatable_catalog(timeout=15)


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
    assert migration.alembic_registry.get_data_source_uid() == "data-source-uid"


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
