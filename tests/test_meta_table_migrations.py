from __future__ import annotations

import importlib
import textwrap
import types
import uuid

import pytest
from sqlalchemy import Column, Index, Integer, MetaData, String, Table, Uuid
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from mainsequence.client.metatables import MetaTable
from mainsequence.meta_tables import (
    MetaTableForeignKey,
    PlatformManagedMetaTable,
)
from mainsequence.meta_tables.migrations import (
    DEFAULT_ALEMBIC_VERSION_COLUMN_NAME,
    DEFAULT_ALEMBIC_VERSION_IDENTIFIER,
    DEFAULT_ALEMBIC_VERSION_NAMESPACE,
    DEFAULT_ALEMBIC_VERSION_SCHEMA,
    DEFAULT_ALEMBIC_VERSION_TABLE_NAME,
    AlembicMetaTableCatalogRefreshContext,
    AlembicMetaTableMigration,
    AlembicVersionMetaTable,
    alembic_config_for_provider,
    apply_mainsequence_migration_role,
    load_alembic_metatable_migration_provider,
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


def test_alembic_version_metatable_uses_session_data_source(monkeypatch):
    import mainsequence.client.metatables as metatables

    class ProjectAlembicVersion(AlembicVersionMetaTable):
        __metatable_data_source_uid__ = None

    monkeypatch.setattr(
        metatables,
        "get_session_data_source",
        lambda: types.SimpleNamespace(uid="session-data-source-uid"),
    )

    request = ProjectAlembicVersion.build_registration_request()

    assert request.data_source_uid == "session-data-source-uid"


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


def test_alembic_metatable_migration_registers_registry_from_session_data_source(
    monkeypatch,
):
    import mainsequence.client.metatables as metatables

    class ProjectAlembicVersion(AlembicVersionMetaTable):
        __metatable_uid__ = None
        __metatable_data_source_uid__ = None

    migration = AlembicMetaTableMigration(
        package="msm",
        migration_namespace="markets",
        script_location="msm:alembic",
        target_metadata=MetaData(),
        alembic_registry=ProjectAlembicVersion,
    )
    captured = {}

    monkeypatch.setattr(
        metatables,
        "get_session_data_source",
        lambda: types.SimpleNamespace(uid="session-data-source-uid"),
    )

    def fake_register(request, *, timeout=None):
        captured["request"] = request
        return types.SimpleNamespace(uid="registry-uid", data_source_uid=request.data_source_uid)

    monkeypatch.setattr(MetaTable, "register", staticmethod(fake_register))

    migration.register_alembic_registry()

    assert captured["request"].data_source_uid == "session-data-source-uid"
    assert ProjectAlembicVersion.get_data_source_uid() == "session-data-source-uid"


def test_ensure_alembic_registry_forces_backend_registration_for_stale_cache(monkeypatch):
    class ProjectAlembicVersion(AlembicVersionMetaTable):
        __metatable_uid__ = "stale-registry-uid"
        __metatable_data_source_uid__ = "data-source-uid"
        __metatable__ = types.SimpleNamespace(
            uid="stale-registry-uid",
            data_source_uid="data-source-uid",
        )

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
        captured["timeout"] = timeout
        return types.SimpleNamespace(
            uid="fresh-registry-uid",
            data_source_uid=request.data_source_uid,
        )

    monkeypatch.setattr(MetaTable, "register", staticmethod(fake_register))

    meta_table = migration.ensure_alembic_registry(timeout=7)

    assert captured["request"].data_source_uid == "data-source-uid"
    assert captured["timeout"] == 7
    assert meta_table.uid == "fresh-registry-uid"
    assert ProjectAlembicVersion.get_meta_table_uid() == "fresh-registry-uid"


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


def test_alembic_metatable_migration_finalizes_catalog_after_alembic(monkeypatch):
    class Base(DeclarativeBase):
        metadata = MetaData()

    class ProjectAlembicVersion(AlembicVersionMetaTable):
        __metatable_data_source_uid__ = "data-source-uid"

    class Asset(PlatformManagedMetaTable, Base):
        __metatable_namespace__ = "markets"
        __metatable_identifier__ = "markets.Asset"
        __metatable_data_source_uid__ = "data-source-uid"

        uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)

    events = []

    def after_register(context):
        events.append(
            (
                "hook",
                context.package,
                context.migration_namespace,
                context.reserved_policy,
                [meta_table.meta_table_uid for meta_table in context.registered_metatables],
            )
        )

    migration = AlembicMetaTableMigration(
        package="msm",
        migration_namespace="markets",
        script_location="msm:alembic",
        target_metadata=Base.metadata,
        alembic_registry=ProjectAlembicVersion,
        metatable_models=[Asset],
        after_register_metatables=after_register,
    )

    captured = {}

    ProjectAlembicVersion._bind_meta_table(
        types.SimpleNamespace(
            uid="registry-meta-table-uid",
            data_source_uid="data-source-uid",
        )
    )

    def fake_finalize(request, *, timeout=None, on_status=None):
        captured["request"] = request
        captured["timeout"] = timeout
        return types.SimpleNamespace(
            ok=True,
            finalized_count=1,
            active_count=1,
            reserved_count=0,
            failed_count=0,
            tables=[
                types.SimpleNamespace(
                    meta_table_uid="asset-meta-table-uid",
                    identifier="markets.Asset",
                    data_source_uid="data-source-uid",
                    storage_hash="mt_asset_hash",
                    physical_table_name="mt_asset",
                    previous_provisioning_status="reserved",
                    provisioning_status="active",
                    table_kind="relational",
                    time_indexed=False,
                    finalized=True,
                    physical_table_exists=True,
                )
            ],
        )

    monkeypatch.setattr(MetaTable, "finalize_managed", staticmethod(fake_finalize))

    response = migration.finalize_metatable_catalog(
        prepared=types.SimpleNamespace(meta_table_uids=["asset-meta-table-uid"]),
        alembic_revision="0001",
        timeout=15,
    )

    assert response.ok is True
    assert captured["timeout"] == 15
    assert captured["request"].meta_table_uids == ["asset-meta-table-uid"]
    assert captured["request"].migration_package == "msm"
    assert captured["request"].migration_namespace == "markets"
    assert captured["request"].alembic_version_meta_table_uid == "registry-meta-table-uid"
    assert captured["request"].alembic_revision == "0001"
    assert Asset.get_meta_table_uid() == "asset-meta-table-uid"
    assert Asset.__table__.name == "mt_asset"
    assert events == [
        (
            "hook",
            "msm",
            "markets",
            "reconcile",
            ["asset-meta-table-uid"],
        )
    ]


def test_alembic_metatable_migration_sync_catalog_hook_has_no_reserved_policy(
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

    events = []

    def after_register(context: AlembicMetaTableCatalogRefreshContext):
        events.append(context.reserved_policy)

    migration = AlembicMetaTableMigration(
        package="msm",
        migration_namespace="markets",
        script_location="msm:alembic",
        target_metadata=Base.metadata,
        alembic_registry=ProjectAlembicVersion,
        metatable_models=[Asset],
        after_register_metatables=after_register,
    )

    def fake_register(request, *, timeout=None):
        return types.SimpleNamespace(
            uid="asset-meta-table-uid",
            data_source_uid=request.data_source_uid,
            storage_hash=request.storage_hash,
            physical_table_name="mt_asset",
        )

    monkeypatch.setattr(MetaTable, "register", staticmethod(fake_register))

    migration.sync_metatable_catalog(timeout=15)

    assert events == [None]


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
    assert not hasattr(migrations, "render_packaged_alembic_migration")
    assert not hasattr(migrations, "render_packaged_alembic_migration_for_provider")


def test_alembic_config_for_provider_uses_scoped_url_and_owner_role():
    class ProjectAlembicVersion(AlembicVersionMetaTable):
        __metatable_data_source_uid__ = "data-source-uid"

    migration = AlembicMetaTableMigration(
        package="sample",
        migration_namespace="markets",
        script_location="sample:migrations",
        target_metadata=MetaData(),
        alembic_registry=ProjectAlembicVersion,
    )

    config = alembic_config_for_provider(
        migration,
        sqlalchemy_url=(
            "postgresql://temporary-secret"
            "?application_name=mainsequence_alembic%3Amsm%3Amarkets"
        ),
        owner_role_name="ms_owner",
    )

    assert config.get_main_option("sqlalchemy.url") == (
        "postgresql://temporary-secret"
        "?application_name=mainsequence_alembic%3Amsm%3Amarkets"
    )
    assert config.get_main_option("version_table") == "alembic_version"
    assert config.get_main_option("version_table_schema") == "public"
    assert config.get_main_option("mainsequence.owner_role_name") == "ms_owner"
    assert config.attributes["mainsequence_migration_provider"] is migration
    assert config.attributes["target_metadata"] is migration.target_metadata
    assert config.attributes["mainsequence_migration_owner_role_name"] == "ms_owner"


def test_apply_mainsequence_migration_role_executes_quoted_set_role():
    class FakeConnection:
        def __init__(self):
            self.statements = []

        def execute(self, statement):
            self.statements.append(str(statement))

    class FakeConfig:
        attributes = {"mainsequence_migration_owner_role_name": 'owner"role'}

        def get_main_option(self, name):
            return None

    connection = FakeConnection()

    apply_mainsequence_migration_role(connection, FakeConfig())

    assert connection.statements == ['SET ROLE "owner""role"']


def test_prepare_for_alembic_reserves_and_binds_backend_names(monkeypatch):
    class Base(DeclarativeBase):
        metadata = MetaData()

    class ProjectAlembicVersion(AlembicVersionMetaTable):
        __metatable_data_source_uid__ = "data-source-uid"

    class Account(PlatformManagedMetaTable, Base):
        __metatable_data_source_uid__ = "data-source-uid"
        __metatable_namespace__ = "example.assets"
        __metatable_identifier__ = "Account"

        uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
        name: Mapped[str] = mapped_column(String(255), nullable=False)

    class Asset(PlatformManagedMetaTable, Base):
        __metatable_data_source_uid__ = "data-source-uid"
        __metatable_namespace__ = "example.assets"
        __metatable_identifier__ = "Asset"
        __table_args__ = (Index(None, "symbol"),)

        uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
        account_uid: Mapped[uuid.UUID] = mapped_column(
            Uuid,
            MetaTableForeignKey(
                Account,
                column="uid",
                name="asset_account_uid_fkey",
                ondelete="CASCADE",
            ),
            nullable=False,
        )
        symbol: Mapped[str] = mapped_column(String(64), nullable=False)

    reserved_payloads = []
    reserved_events = []

    monkeypatch.setattr(MetaTable, "filter", staticmethod(lambda **kwargs: []))

    def fake_reserve_managed(request, *, timeout=None, on_status=None):
        reserved_payloads.extend(request.tables)
        assert [table.identifier for table in request.tables] == ["Account", "Asset"]
        assert (
            request.tables[1].table_contract.foreign_keys[0].target_identifier
            == "Account"
        )
        assert request.tables[1].table_contract.foreign_keys[0].target_meta_table_uid is None

        response_tables = []
        for table in request.tables:
            if table.identifier == "Account":
                uid = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
                physical_name = "mt_account_backend"
                indexes = []
                foreign_keys = []
            else:
                uid = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
                physical_name = "mt_asset_backend"
                indexes = [{"name": "mt_asset_symbol_idx", "columns": ["symbol"], "unique": False}]
                foreign_keys = [
                    {
                        "name": "asset_account_uid_fkey",
                        "source_columns": ["account_uid"],
                        "target_meta_table_uid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                        "target_identifier": "Account",
                        "target_columns": ["uid"],
                        "on_delete": "cascade",
                    }
                ]
            response_tables.append(
                types.SimpleNamespace(
                    identifier=table.identifier,
                    namespace=table.namespace,
                    meta_table_uid=uid,
                    data_source_uid="data-source-uid",
                    management_mode="platform_managed",
                    provisioning_status="reserved",
                    storage_hash=table.storage_hash,
                    physical_table_name=physical_name,
                    table_contract={
                        "version": "relational-table.v1",
                        "physical": {"table_name": physical_name},
                        "columns": [],
                        "indexes": indexes,
                        "foreign_keys": foreign_keys,
                    },
                    existing=False,
                )
            )
        return types.SimpleNamespace(
            tables=response_tables
        )

    monkeypatch.setattr(MetaTable, "reserve_managed", staticmethod(fake_reserve_managed))

    migration = AlembicMetaTableMigration(
        package="sample",
        migration_namespace="markets",
        script_location="sample:migrations",
        target_metadata=Base.metadata,
        alembic_registry=ProjectAlembicVersion,
        metatable_models=[Asset],
    )

    prepared = migration.prepare_for_alembic(
        timeout=5,
        on_metatable_reserved=lambda model, item: reserved_events.append(
            (model.__name__, item.identifier, item.physical_table_name)
        ),
    )

    assert [payload.identifier for payload in reserved_payloads] == ["Account", "Asset"]
    assert reserved_events == [
        ("Account", "Account", "mt_account_backend"),
        ("Asset", "Asset", "mt_asset_backend"),
    ]
    assert prepared.data_source_uid == "data-source-uid"
    assert prepared.meta_table_uids == [
        "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
    ]
    assert Account.__table__.name == "mt_account_backend"
    assert Asset.__table__.name == "mt_asset_backend"
    assert next(iter(Asset.__table__.indexes)).name == "mt_asset_symbol_idx"
    assert next(iter(Asset.__table__.foreign_key_constraints)).name == ("asset_account_uid_fkey")
    assert reserved_payloads[1].table_contract.foreign_keys[0].name == ("asset_account_uid_fkey")
    assert reserved_payloads[0].schema_management.mode == "alembic_managed"
    assert reserved_payloads[0].schema_management.alembic.package == "sample"
    assert reserved_payloads[0].schema_management.alembic.migration_namespace == "markets"
    assert reserved_payloads[0].schema_management.alembic.provider_key == "sample:markets"


def test_prepare_for_alembic_reserves_existing_identifier_to_stamp_schema_management(monkeypatch):
    class Base(DeclarativeBase):
        metadata = MetaData()

    class ProjectAlembicVersion(AlembicVersionMetaTable):
        __metatable_data_source_uid__ = "data-source-uid"

    class Account(PlatformManagedMetaTable, Base):
        __metatable_data_source_uid__ = "data-source-uid"
        __metatable_namespace__ = "example.assets"
        __metatable_identifier__ = "Account"

        uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
        name: Mapped[str] = mapped_column(String(255), nullable=False)

    class Asset(PlatformManagedMetaTable, Base):
        __metatable_data_source_uid__ = "data-source-uid"
        __metatable_namespace__ = "example.assets"
        __metatable_identifier__ = "Asset"

        uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
        account_uid: Mapped[uuid.UUID] = mapped_column(
            Uuid,
            MetaTableForeignKey(Account, column="uid", ondelete="CASCADE"),
            nullable=False,
        )
        symbol: Mapped[str] = mapped_column(String(64), nullable=False)

    filter_calls = []

    def fake_filter(**kwargs):
        filter_calls.append(kwargs)
        assert set(kwargs) == {"timeout", "identifier__in"}
        assert kwargs["identifier__in"] == ["Account", "Asset"]
        return [
            types.SimpleNamespace(
                identifier="Account",
                uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                data_source_uid="data-source-uid",
                provisioning_status="reserved",
                storage_hash="account-storage-hash",
                physical_table_name="mt_account_backend",
                table_contract={
                    "version": "relational-table.v1",
                    "physical": {"table_name": "mt_account_backend"},
                    "columns": [],
                    "indexes": [],
                    "foreign_keys": [],
                },
            )
        ]

    monkeypatch.setattr(MetaTable, "filter", staticmethod(fake_filter))

    reserved_payloads = []

    def fake_reserve_managed(request, *, timeout=None, on_status=None):
        reserved_payloads.extend(request.tables)
        assert [table.identifier for table in request.tables] == ["Account", "Asset"]
        assert (
            request.tables[1].table_contract.foreign_keys[0].target_meta_table_uid
            == "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
        )
        return types.SimpleNamespace(
            tables=[
                types.SimpleNamespace(
                    identifier="Account",
                    namespace="example.assets",
                    meta_table_uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                    data_source_uid="data-source-uid",
                    management_mode="platform_managed",
                    provisioning_status="active",
                    storage_hash="account-storage-hash",
                    physical_table_name="mt_account_backend",
                    table_contract={
                        "version": "relational-table.v1",
                        "physical": {"table_name": "mt_account_backend"},
                        "columns": [],
                        "indexes": [],
                        "foreign_keys": [],
                    },
                ),
                types.SimpleNamespace(
                    identifier="Asset",
                    namespace="example.assets",
                    meta_table_uid="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
                    data_source_uid="data-source-uid",
                    management_mode="platform_managed",
                    provisioning_status="reserved",
                    storage_hash=request.tables[1].storage_hash,
                    physical_table_name="mt_asset_backend",
                    table_contract={
                        "version": "relational-table.v1",
                        "physical": {"table_name": "mt_asset_backend"},
                        "columns": [],
                        "indexes": [],
                        "foreign_keys": [
                            {
                                "name": "asset_account_uid_fkey",
                                "source_columns": ["account_uid"],
                                "target_meta_table_uid": (
                                    "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
                                ),
                                "target_identifier": "Account",
                                "target_columns": ["uid"],
                                "on_delete": "cascade",
                            }
                        ],
                    },
                )
            ]
        )

    monkeypatch.setattr(MetaTable, "reserve_managed", staticmethod(fake_reserve_managed))

    migration = AlembicMetaTableMigration(
        package="sample",
        migration_namespace="markets",
        script_location="sample:migrations",
        target_metadata=Base.metadata,
        alembic_registry=ProjectAlembicVersion,
        metatable_models=[Asset],
    )

    prepared = migration.prepare_for_alembic(timeout=5)

    assert len(filter_calls) == 1
    assert [payload.identifier for payload in reserved_payloads] == ["Account", "Asset"]
    assert reserved_payloads[0].schema_management.mode == "alembic_managed"
    assert prepared.meta_table_uids == [
        "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
    ]
    assert Account.__table__.name == "mt_account_backend"
    assert Asset.__table__.name == "mt_asset_backend"


def test_prepare_for_alembic_skips_already_staged_existing_rows(monkeypatch):
    import mainsequence.meta_tables.migrations as migrations_mod

    class Base(DeclarativeBase):
        metadata = MetaData()

    class ProjectAlembicVersion(AlembicVersionMetaTable):
        __metatable_uid__ = "registry-meta-table-uid"
        __metatable_data_source_uid__ = "data-source-uid"

    class Account(PlatformManagedMetaTable, Base):
        __metatable_data_source_uid__ = "data-source-uid"
        __metatable_namespace__ = "example.assets"
        __metatable_identifier__ = "Account"

        uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
        name: Mapped[str] = mapped_column(String(255), nullable=False)

    class Asset(PlatformManagedMetaTable, Base):
        __metatable_data_source_uid__ = "data-source-uid"
        __metatable_namespace__ = "example.assets"
        __metatable_identifier__ = "Asset"

        uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
        account_uid: Mapped[uuid.UUID] = mapped_column(
            Uuid,
            MetaTableForeignKey(Account, column="uid", ondelete="CASCADE"),
            nullable=False,
        )
        symbol: Mapped[str] = mapped_column(String(64), nullable=False)

    monkeypatch.setattr(migrations_mod, "_contracts_equivalent", lambda left, right: True)

    def fake_filter(**kwargs):
        return [
            types.SimpleNamespace(
                identifier="Account",
                uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                data_source_uid="data-source-uid",
                provisioning_status="active",
                schema_management_mode="alembic_managed",
                migration_provider_key="sample:markets",
                alembic_version_meta_table_uid="registry-meta-table-uid",
                storage_hash="account-storage-hash",
                physical_table_name="mt_account_backend",
                table_contract={
                    "version": "relational-table.v1",
                    "physical": {"table_name": "mt_account_backend"},
                    "columns": [],
                    "indexes": [],
                    "foreign_keys": [],
                },
            ),
            types.SimpleNamespace(
                identifier="Asset",
                uid="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
                data_source_uid="data-source-uid",
                provisioning_status="active",
                schema_management_mode="alembic_managed",
                migration_provider_key="sample:markets",
                alembic_version_meta_table_uid="registry-meta-table-uid",
                storage_hash="asset-storage-hash",
                physical_table_name="mt_asset_backend",
                table_contract={
                    "version": "relational-table.v1",
                    "physical": {"table_name": "mt_asset_backend"},
                    "columns": [],
                    "indexes": [],
                    "foreign_keys": [
                        {
                            "name": "asset_account_uid_fkey",
                            "source_columns": ["account_uid"],
                            "target_meta_table_uid": (
                                "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
                            ),
                            "target_identifier": "Account",
                            "target_columns": ["uid"],
                            "on_delete": "cascade",
                        }
                    ],
                },
            ),
        ]

    monkeypatch.setattr(MetaTable, "filter", staticmethod(fake_filter))
    monkeypatch.setattr(
        MetaTable,
        "reserve_managed",
        staticmethod(lambda *args, **kwargs: pytest.fail("reserve-managed was called")),
    )

    migration = AlembicMetaTableMigration(
        package="sample",
        migration_namespace="markets",
        script_location="sample:migrations",
        target_metadata=Base.metadata,
        alembic_registry=ProjectAlembicVersion,
        metatable_models=[Asset],
    )

    prepared = migration.prepare_for_alembic(timeout=5)

    assert prepared.meta_table_uids == [
        "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
    ]
    assert Account.__table__.name == "mt_account_backend"
    assert Asset.__table__.name == "mt_asset_backend"
    assert next(iter(Asset.__table__.foreign_key_constraints)).name == (
        "asset_account_uid_fkey"
    )


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
