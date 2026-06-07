from __future__ import annotations

import datetime
import importlib
import textwrap
import types
import uuid

import pytest
from sqlalchemy import Column, DateTime, ForeignKey, Index, Integer, MetaData, String, Table, Uuid
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from mainsequence.client.metatables import (
    ManagedMetaTableFinalizeResponse,
    ManagedMetaTableFinalizeTableResult,
    MetaTable,
    TimeIndexMetaTable,
)
from mainsequence.meta_tables import (
    PlatformManagedMetaTable,
    PlatformTimeIndexMetaTable,
    schema_index_name,
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
    PreparedAlembicMetaTableMigration,
    alembic_config_for_provider,
    apply_mainsequence_migration_role,
    build_alembic_version_metatable,
    build_metatable_migration_provider,
    build_metatable_model_registry,
    load_alembic_metatable_migration_provider,
    metadata_for_models,
    namespace_version_location,
    namespace_version_slug,
    resolve_alembic_revision_metadata,
    scaffold_migration_package,
)


def _reserved_metatable(
    resource_cls: type[MetaTable] = MetaTable,
    *,
    uid: str,
    identifier: str,
    physical_table_name: str,
    storage_hash: str,
    namespace: str = "example.assets",
) -> MetaTable:
    return resource_cls.model_construct(
        uid=uid,
        data_source_uid="data-source-uid",
        storage_hash=storage_hash,
        identifier=identifier,
        namespace=namespace,
        management_mode="platform_managed",
        provisioning_status="reserved",
        schema_management_mode="alembic_managed",
        migration_package="sample",
        migration_namespace="markets",
        migration_provider_key="sample:markets",
        alembic_version_meta_table_uid="registry-meta-table-uid",
        physical_table_name=physical_table_name,
        table_contract={
            "version": "relational-table.v1",
            "physical": {"table_name": physical_table_name},
            "columns": [],
        },
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
    assert not hasattr(request.table_contract, "indexes")
    assert not hasattr(request.table_contract, "foreign_keys")
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
        __metatable_uid__ = "registry-meta-table-uid"
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
    assert migration.include_name("asset", "table", {"schema_name": None}) is True
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
        __metatable_uid__ = "registry-meta-table-uid"
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


def test_namespace_version_location_normalizes_and_hashes_long_names():
    assert namespace_version_slug(None) == "default"
    assert namespace_version_slug("MainSequence.Examples") == "mainsequence_examples"
    long_namespace = "mainsequence." + ("very-long-" * 12) + "examples"
    slug = namespace_version_slug(long_namespace)

    assert len(slug) <= 48
    assert slug == namespace_version_slug(long_namespace)
    assert namespace_version_location(
        "MainSequence.Examples",
        prefix="migrations:versions",
    ) == "migrations:versions/mainsequence_examples"


def test_build_metatable_model_registry_filters_dedupes_and_requires_identifiers():
    class Base(DeclarativeBase):
        metadata = MetaData()

    class Asset(PlatformManagedMetaTable, Base):
        __tablename__ = "registry_asset"
        __metatable_namespace__ = "markets"
        __metatable_identifier__ = "markets.Asset"

        uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)

    class NotMetaTable(Base):
        __tablename__ = "not_metatable"

        uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)

    assert build_metatable_model_registry([Asset, Asset, NotMetaTable], base=Base) == [Asset]
    assert build_metatable_model_registry(Asset, base=Base) == [Asset]

    class DuplicateAsset(PlatformManagedMetaTable, Base):
        __tablename__ = "registry_duplicate_asset"
        __metatable_namespace__ = "markets"
        __metatable_identifier__ = "markets.Asset"

        uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)

    with pytest.raises(ValueError, match="Duplicate MetaTable identifier"):
        build_metatable_model_registry([Asset, DuplicateAsset], base=Base)

    class MissingIdentifier(PlatformManagedMetaTable, Base):
        __tablename__ = "registry_missing_identifier"
        __metatable_namespace__ = "markets"

        uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)

    with pytest.raises(ValueError, match="non-empty __metatable_identifier__"):
        build_metatable_model_registry([MissingIdentifier], base=Base)


def test_metadata_for_models_copies_selected_tables_and_indexes():
    class Base(DeclarativeBase):
        metadata = MetaData(naming_convention={"ix": "ix_%(table_name)s_%(column_0_name)s"})

    class Asset(PlatformManagedMetaTable, Base):
        __tablename__ = "metadata_asset"
        __table_args__ = (Index("ix_metadata_asset_name", "name"), {"schema": "markets"})
        __metatable_namespace__ = "markets"
        __metatable_identifier__ = "markets.MetadataAsset"

        uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
        name: Mapped[str] = mapped_column(String)

    class Price(PlatformManagedMetaTable, Base):
        __tablename__ = "metadata_price"
        __metatable_namespace__ = "markets"
        __metatable_identifier__ = "markets.MetadataPrice"

        uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)

    metadata = metadata_for_models([Asset])

    assert sorted(metadata.tables) == ["markets.metadata_asset"]
    copied_table = metadata.tables["markets.metadata_asset"]
    assert copied_table.schema == "markets"
    assert sorted(copied_table.columns.keys()) == ["name", "uid"]
    assert {index.name for index in copied_table.indexes} == {"ix_metadata_asset_name"}
    assert "metadata_price" not in metadata.tables


def test_build_alembic_version_metatable_and_provider_factory():
    class Base(DeclarativeBase):
        metadata = MetaData()

    class Asset(PlatformManagedMetaTable, Base):
        __tablename__ = "provider_asset"
        __metatable_namespace__ = "markets"
        __metatable_identifier__ = "markets.ProviderAsset"

        uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)

    registry = build_alembic_version_metatable(
        class_name="MarketsAlembicVersion",
        namespace="markets",
        identifier="markets.alembic_version",
        schema="markets_schema",
        table_name="markets_alembic_version",
    )

    migration = build_metatable_migration_provider(
        package="msm",
        migration_namespace="MainSequence.Examples",
        script_location="migrations:",
        target_metadata=Base.metadata,
        alembic_registry=registry,
        metatable_models=[Asset],
    )

    assert issubclass(registry, AlembicVersionMetaTable)
    assert registry.__module__ == __name__
    assert registry.__metatable_identifier__ == "markets.alembic_version"
    assert registry.alembic_table_path() == "markets_schema.markets_alembic_version"
    assert migration.version_locations == ["migrations:versions/mainsequence_examples"]
    assert migration.version_path == "migrations:versions/mainsequence_examples"
    assert list(migration.metatable_models) == [Asset]


def test_scaffold_migration_package_creates_idempotent_sdk_skeleton(tmp_path):
    result = scaffold_migration_package(
        project_root=tmp_path,
        module="migrations",
        package="msm",
        namespace="mainsequence.examples",
        metadata_ref="msm.base:MarketsBase.metadata",
        base_ref="msm.base:MarketsBase",
    )

    assert result.root == tmp_path / "src" / "migrations"
    assert {file.action for file in result.files} == {"created"}
    package_init = result.root / "__init__.py"
    package_init_text = package_init.read_text()
    assert "build_metatable_migration_provider" in package_init_text
    assert 'version_location_prefix="migrations:versions"' in package_init_text
    assert "run_mainsequence_alembic_env(default_provider=migration)" in (
        result.root / "env.py"
    ).read_text()
    assert (result.root / "versions" / "mainsequence_examples" / "__init__.py").exists()

    second_result = scaffold_migration_package(
        project_root=tmp_path,
        module="migrations",
        package="msm",
        namespace="mainsequence.examples",
        metadata_ref="msm.base:MarketsBase.metadata",
        base_ref="msm.base:MarketsBase",
    )

    assert {file.action for file in second_result.files} == {"skipped"}

    package_init.write_text("changed", encoding="utf-8")
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        scaffold_migration_package(
            project_root=tmp_path,
            module="migrations",
            package="msm",
            namespace="mainsequence.examples",
            metadata_ref="msm.base:MarketsBase.metadata",
            base_ref="msm.base:MarketsBase",
        )

    forced_result = scaffold_migration_package(
        project_root=tmp_path,
        module="migrations",
        package="msm",
        namespace="mainsequence.examples",
        metadata_ref="msm.base:MarketsBase.metadata",
        base_ref="msm.base:MarketsBase",
        force=True,
    )

    assert any(file.action == "overwritten" for file in forced_result.files)


def test_alembic_metatable_migration_finalizes_catalog_after_alembic(monkeypatch):
    class Base(DeclarativeBase):
        metadata = MetaData()

    class ProjectAlembicVersion(AlembicVersionMetaTable):
        __metatable_data_source_uid__ = "data-source-uid"

    class Asset(PlatformManagedMetaTable, Base):
        __tablename__ = "example_assets__asset"
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
                [meta_table.uid for meta_table in context.registered_metatables],
                [model.__name__ for model in context.metatable_models],
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
        MetaTable.model_construct(
            uid="registry-meta-table-uid",
            data_source_uid="data-source-uid",
        )
    )

    def fake_finalize(request, *, timeout=None, on_status=None):
        captured["request"] = request
        captured["timeout"] = timeout
        return ManagedMetaTableFinalizeResponse.model_construct(
            ok=True,
            finalized_count=1,
            active_count=1,
            reserved_count=0,
            failed_count=0,
            tables=[
                ManagedMetaTableFinalizeTableResult.model_construct(
                    meta_table_uid="asset-meta-table-uid",
                    identifier="markets.Asset",
                    storage_hash="mt_asset_hash",
                    physical_table_name="example_assets__asset",
                    previous_provisioning_status="reserved",
                    provisioning_status="active",
                    table_kind="relational",
                    time_indexed=False,
                    finalized=True,
                    physical_table_exists=True,
                    schema_management_mode="alembic_managed",
                    migration_package="msm",
                    migration_namespace="markets",
                    migration_provider_key="msm:markets",
                    alembic_version_meta_table_uid="registry-meta-table-uid",
                )
            ],
        )

    monkeypatch.setattr(MetaTable, "finalize_managed", staticmethod(fake_finalize))

    response = migration.finalize_metatable_catalog(
        prepared=PreparedAlembicMetaTableMigration(
            data_source_uid="data-source-uid",
            meta_table_uids=["asset-meta-table-uid"],
        ),
        alembic_revision="0001",
        timeout=15,
    )

    assert response.ok is True
    assert captured["timeout"] == 15
    assert captured["request"].meta_table_uids == ["asset-meta-table-uid"]
    assert captured["request"].migration_package == "msm"
    assert captured["request"].migration_namespace == "markets"
    assert captured["request"].migration_provider_key == "msm:markets"
    assert captured["request"].alembic_version_meta_table_uid == "registry-meta-table-uid"
    assert captured["request"].alembic_revision == "0001"
    assert Asset.get_meta_table_uid() == "asset-meta-table-uid"
    assert Asset.__table__.name == "example_assets__asset"
    assert Asset.get_physical_table_name() == "example_assets__asset"
    assert events == [
        (
            "hook",
            "msm",
            "markets",
            "reconcile",
            ["asset-meta-table-uid"],
            ["Asset"],
        )
    ]


def test_finalize_metatable_catalog_passes_full_bound_provider_scope_to_hook(monkeypatch):
    class Base(DeclarativeBase):
        metadata = MetaData()

    class ProjectAlembicVersion(AlembicVersionMetaTable):
        __metatable_data_source_uid__ = "data-source-uid"

    class Asset(PlatformManagedMetaTable, Base):
        __tablename__ = "example_assets__asset"
        __metatable_namespace__ = "markets"
        __metatable_identifier__ = "markets.Asset"
        __metatable_data_source_uid__ = "data-source-uid"

        uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)

    class Price(PlatformManagedMetaTable, Base):
        __tablename__ = "example_assets__price"
        __metatable_namespace__ = "markets"
        __metatable_identifier__ = "markets.Price"
        __metatable_data_source_uid__ = "data-source-uid"

        uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)

    ProjectAlembicVersion._bind_meta_table(
        MetaTable.model_construct(
            uid="registry-meta-table-uid",
            data_source_uid="data-source-uid",
        )
    )
    Price._bind_meta_table(
        MetaTable.model_construct(
            uid="price-meta-table-uid",
            data_source_uid="data-source-uid",
            storage_hash="mt_price_hash",
            identifier="markets.Price",
            physical_table_name="example_assets__price",
            management_mode="platform_managed",
            provisioning_status="active",
        )
    )

    hook_uids = []

    def after_register(context):
        hook_uids.append(
            (
                [meta_table.uid for meta_table in context.registered_metatables],
                [model.__name__ for model in context.metatable_models],
            )
        )

    migration = AlembicMetaTableMigration(
        package="msm",
        migration_namespace="markets",
        script_location="msm:alembic",
        target_metadata=Base.metadata,
        alembic_registry=ProjectAlembicVersion,
        metatable_models=[Asset, Price],
        after_register_metatables=after_register,
    )

    def fake_finalize(request, *, timeout=None, on_status=None):
        return ManagedMetaTableFinalizeResponse.model_construct(
            ok=True,
            finalized_count=0,
            active_count=2,
            reserved_count=0,
            failed_count=0,
            tables=[
                ManagedMetaTableFinalizeTableResult.model_construct(
                    meta_table_uid="asset-meta-table-uid",
                    identifier="markets.Asset",
                    storage_hash="mt_asset_hash",
                    physical_table_name="example_assets__asset",
                    previous_provisioning_status="active",
                    provisioning_status="active",
                    table_kind="relational",
                    time_indexed=False,
                    finalized=False,
                    physical_table_exists=True,
                    schema_management_mode="alembic_managed",
                    migration_package="msm",
                    migration_namespace="markets",
                    migration_provider_key="msm:markets",
                    alembic_version_meta_table_uid="registry-meta-table-uid",
                )
            ],
        )

    monkeypatch.setattr(MetaTable, "finalize_managed", staticmethod(fake_finalize))

    response = migration.finalize_metatable_catalog(
        prepared=PreparedAlembicMetaTableMigration(
            data_source_uid="data-source-uid",
            meta_table_uids=["asset-meta-table-uid", "price-meta-table-uid"],
        ),
        alembic_revision="0001",
    )

    assert response.ok is True
    assert hook_uids == [
        (
            ["asset-meta-table-uid", "price-meta-table-uid"],
            ["Asset", "Price"],
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
        __tablename__ = "example_assets__asset"
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
        return MetaTable.model_construct(
            uid="asset-meta-table-uid",
            data_source_uid=request.data_source_uid,
            storage_hash=request.storage_hash,
            physical_table_name="mt_asset",
            management_mode="platform_managed",
        )

    monkeypatch.setattr(MetaTable, "register", staticmethod(fake_register))

    migration.sync_metatable_catalog(timeout=15)

    assert events == [None]


def test_finalize_metatable_catalog_surfaces_missing_physical_tables(monkeypatch):
    class Base(DeclarativeBase):
        metadata = MetaData()

    class ProjectAlembicVersion(AlembicVersionMetaTable):
        __metatable_data_source_uid__ = "data-source-uid"

    class Asset(PlatformManagedMetaTable, Base):
        __tablename__ = "example_assets__asset"
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
    ProjectAlembicVersion._bind_meta_table(
        MetaTable.model_construct(
            uid="registry-meta-table-uid",
            data_source_uid="data-source-uid",
        )
    )

    def fake_finalize(request, *, timeout=None, on_status=None):
        return ManagedMetaTableFinalizeResponse.model_construct(
            ok=False,
            finalized_count=0,
            active_count=0,
            reserved_count=1,
            failed_count=1,
            tables=[
                ManagedMetaTableFinalizeTableResult.model_construct(
                    meta_table_uid="asset-meta-table-uid",
                    identifier="markets.Asset",
                    storage_hash="mt_asset_hash",
                    physical_table_name="example_assets__asset",
                    previous_provisioning_status="reserved",
                    provisioning_status="reserved",
                    table_kind="relational",
                    time_indexed=False,
                    finalized=False,
                    physical_table_exists=False,
                    schema_management_mode="alembic_managed",
                    migration_package="msm",
                    migration_namespace="markets",
                    migration_provider_key="msm:markets",
                    alembic_version_meta_table_uid="registry-meta-table-uid",
                    error={"code": "physical_table_missing"},
                )
            ],
        )

    monkeypatch.setattr(MetaTable, "finalize_managed", staticmethod(fake_finalize))
    statuses = []

    with pytest.raises(Exception) as exc_info:
        migration.finalize_metatable_catalog(
            prepared=PreparedAlembicMetaTableMigration(
                data_source_uid="data-source-uid",
                meta_table_uids=["asset-meta-table-uid"],
            ),
            alembic_revision="0001",
            timeout=15,
            on_metatable_finalize_status=statuses.append,
        )

    assert "physical_table_exists=False" in str(exc_info.value)
    assert "failed=1" in str(exc_info.value)
    assert statuses == [
        "Finalize-managed response ok=False finalized=0 active=0 reserved=1 failed=1.",
        (
            "Finalize-managed table failed identifier=markets.Asset "
            "physical_table=example_assets__asset provisioning_status=reserved "
            "physical_table_exists=False finalized=False "
            'error={"code": "physical_table_missing"}'
        ),
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
            "postgresql://temporary-secret?application_name=mainsequence_alembic%3Amsm%3Amarkets"
        ),
        owner_role_name="ms_owner",
    )

    assert config.get_main_option("sqlalchemy.url") == (
        "postgresql://temporary-secret?application_name=mainsequence_alembic%3Amsm%3Amarkets"
    )
    assert config.get_main_option("sqlalchemy.echo") == "true"
    assert config.get_main_option("version_table") == "alembic_version"
    assert config.get_main_option("version_table_schema") == "public"
    assert config.get_main_option("mainsequence.owner_role_name") == "ms_owner"
    assert config.attributes["mainsequence_migration_provider"] is migration
    assert config.attributes["target_metadata"] is migration.target_metadata
    assert config.attributes["mainsequence_migration_owner_role_name"] == "ms_owner"
    assert config.attributes["mainsequence_migration_sqlalchemy_url"] == (
        "postgresql://temporary-secret?application_name=mainsequence_alembic%3Amsm%3Amarkets"
    )


def test_alembic_config_for_provider_supports_namespace_version_locations(tmp_path):
    script_root = tmp_path / "migrations"
    namespace_versions = script_root / "versions" / "mainsequence_examples"
    namespace_versions.mkdir(parents=True)
    (script_root / "env.py").write_text("", encoding="utf-8")
    (namespace_versions / "0003_migration.py").write_text(
        "revision = '0003'\n"
        "down_revision = None\n"
        "branch_labels = None\n"
        "depends_on = None\n",
        encoding="utf-8",
    )
    (namespace_versions / "0004_migration.py").write_text(
        "revision = '0004'\n"
        "down_revision = '0003'\n"
        "branch_labels = None\n"
        "depends_on = None\n",
        encoding="utf-8",
    )

    class ProjectAlembicVersion(AlembicVersionMetaTable):
        __metatable_data_source_uid__ = "data-source-uid"

    migration = AlembicMetaTableMigration(
        package="sample",
        migration_namespace="markets",
        script_location=str(script_root),
        version_locations=[str(namespace_versions)],
        version_path=str(namespace_versions),
        target_metadata=MetaData(),
        alembic_registry=ProjectAlembicVersion,
    )

    config = alembic_config_for_provider(migration, sqlalchemy_url="postgresql://example")

    assert config.get_main_option("version_locations") == str(namespace_versions)
    assert config.get_main_option("path_separator") == "newline"
    assert config.attributes["version_locations"] == (str(namespace_versions),)
    assert config.attributes["version_path"] == str(namespace_versions)

    from alembic.script import ScriptDirectory

    script = ScriptDirectory.from_config(config)
    assert script.get_revision("0004").revision == "0004"
    assert resolve_alembic_revision_metadata(
        script_location=str(script_root),
        version_locations=[str(namespace_versions)],
        revision="0004",
    ) == ("0004", "0003")


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


def test_apply_mainsequence_migration_role_commits_implicit_set_role_transaction():
    class FakeConnection:
        def __init__(self):
            self.statements = []
            self.transaction_active = False
            self.commit_count = 0

        def in_transaction(self):
            return self.transaction_active

        def execute(self, statement):
            self.statements.append(str(statement))
            self.transaction_active = True

        def commit(self):
            self.commit_count += 1
            self.transaction_active = False

    class FakeConfig:
        attributes = {"mainsequence_migration_owner_role_name": "owner_role"}

        def get_main_option(self, name):
            return None

    connection = FakeConnection()

    apply_mainsequence_migration_role(connection, FakeConfig())

    assert connection.statements == ['SET ROLE "owner_role"']
    assert connection.commit_count == 1
    assert connection.transaction_active is False


def test_apply_mainsequence_migration_role_preserves_existing_transaction():
    class FakeConnection:
        def __init__(self):
            self.statements = []
            self.transaction_active = True
            self.commit_count = 0

        def in_transaction(self):
            return self.transaction_active

        def execute(self, statement):
            self.statements.append(str(statement))

        def commit(self):
            self.commit_count += 1
            self.transaction_active = False

    class FakeConfig:
        attributes = {"mainsequence_migration_owner_role_name": "owner_role"}

        def get_main_option(self, name):
            return None

    connection = FakeConnection()

    apply_mainsequence_migration_role(connection, FakeConfig())

    assert connection.statements == ['SET ROLE "owner_role"']
    assert connection.commit_count == 0
    assert connection.transaction_active is True


def test_prepare_for_alembic_preserves_authored_table_names(monkeypatch):
    class Base(DeclarativeBase):
        metadata = MetaData()

    class ProjectAlembicVersion(AlembicVersionMetaTable):
        __metatable_uid__ = "registry-meta-table-uid"
        __metatable_data_source_uid__ = "data-source-uid"

    class Account(PlatformManagedMetaTable, Base):
        __tablename__ = "example_assets__account"
        __metatable_data_source_uid__ = "data-source-uid"
        __metatable_namespace__ = "example.assets"
        __metatable_identifier__ = "Account"

        uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
        name: Mapped[str] = mapped_column(String(255), nullable=False)

    class Asset(PlatformManagedMetaTable, Base):
        __tablename__ = "example_assets__asset"
        __metatable_data_source_uid__ = "data-source-uid"
        __metatable_namespace__ = "example.assets"
        __metatable_identifier__ = "Asset"
        __table_args__ = (Index(None, "symbol"),)

        uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
        account_uid: Mapped[uuid.UUID] = mapped_column(
            Uuid,
            ForeignKey(
                "example_assets__account.uid", name="asset_account_uid_fkey", ondelete="CASCADE"
            ),
            nullable=False,
        )
        symbol: Mapped[str] = mapped_column(String(64), nullable=False)

    reserved_payloads = []
    reserved_events = []

    monkeypatch.setattr(MetaTable, "filter_by_body", staticmethod(lambda **kwargs: []))

    def fake_bulk_create(rows, *, timeout=None, on_status=None):
        reserved_payloads.extend(rows)
        assert [table["identifier"] for table in rows] == [
            "example_assets__account",
            "example_assets__asset",
        ]
        assert all(table["migration_package"] == "sample" for table in rows)
        assert all(table["migration_namespace"] == "markets" for table in rows)
        assert all(table["migration_provider_key"] == "sample:markets" for table in rows)
        assert all(
            table["alembic_version_meta_table_uid"] == "registry-meta-table-uid" for table in rows
        )
        assert all(table["is_alembic_managed"] is True for table in rows)
        assert all(table["provisioning_status"] == "reserved" for table in rows)
        assert all(table["management_mode"] == "platform_managed" for table in rows)
        assert all("schema_management" not in table for table in rows)
        assert all(table["protect_from_deletion"] is False for table in rows)
        assert "indexes" not in rows[0]["table_contract"]
        assert "indexes" not in rows[1]["table_contract"]
        assert "foreign_keys" not in rows[0]["table_contract"]
        assert "foreign_keys" not in rows[1]["table_contract"]
        assert "schema" not in rows[0]["table_contract"]["physical"]
        assert "schema" not in rows[1]["table_contract"]["physical"]

        response_tables = []
        for table in rows:
            if table["identifier"] == "example_assets__account":
                uid = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
                physical_name = "example_assets__account"
            else:
                uid = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
                physical_name = "example_assets__asset"
            response_tables.append(
                _reserved_metatable(
                    uid=uid,
                    identifier=table["identifier"],
                    physical_table_name=physical_name,
                    storage_hash=table["storage_hash"],
                )
            )
        return response_tables

    monkeypatch.setattr(MetaTable, "bulk_create", staticmethod(fake_bulk_create))

    migration = AlembicMetaTableMigration(
        package="sample",
        migration_namespace="markets",
        script_location="sample:migrations",
        target_metadata=Base.metadata,
        alembic_registry=ProjectAlembicVersion,
        metatable_models=[Account, Asset],
    )

    prepared = migration.prepare_for_alembic(
        timeout=5,
        on_metatable_reserved=lambda model, item: reserved_events.append(
            (model.__name__, item.identifier, item.physical_table_name)
        ),
    )

    assert [payload["identifier"] for payload in reserved_payloads] == [
        "example_assets__account",
        "example_assets__asset",
    ]
    assert reserved_events == [
        ("Account", "example_assets__account", "example_assets__account"),
        ("Asset", "example_assets__asset", "example_assets__asset"),
    ]
    assert prepared.data_source_uid == "data-source-uid"
    assert prepared.meta_table_uids == [
        "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
    ]
    assert Account.__table__.name == "example_assets__account"
    assert Asset.__table__.name == "example_assets__asset"
    assert (
        reserved_payloads[0]["table_contract"]["physical"]["table_name"]
        == "example_assets__account"
    )
    assert (
        reserved_payloads[1]["table_contract"]["physical"]["table_name"]
        == "example_assets__asset"
    )
    assert "backend" not in str(next(iter(Asset.__table__.indexes)).name)
    assert next(iter(Asset.__table__.foreign_key_constraints)).name == "asset_account_uid_fkey"
    assert "indexes" not in reserved_payloads[1]["table_contract"]
    assert "foreign_keys" not in reserved_payloads[1]["table_contract"]
    assert "schema" not in reserved_payloads[0]["table_contract"]["physical"]
    assert "schema" not in reserved_payloads[1]["table_contract"]["physical"]
    assert "schema_management" not in reserved_payloads[0]
    assert reserved_payloads[0]["protect_from_deletion"] is False


def test_prepare_for_alembic_does_not_resolve_foreign_key_targets(monkeypatch):
    class Base(DeclarativeBase):
        metadata = MetaData()

    class ProjectAlembicVersion(AlembicVersionMetaTable):
        __metatable_data_source_uid__ = "data-source-uid"

    class Account(PlatformManagedMetaTable, Base):
        __tablename__ = "example_assets__account"
        __metatable_data_source_uid__ = "data-source-uid"
        __metatable_namespace__ = "example.assets"
        __metatable_identifier__ = "Account"

        uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)

    class Asset(PlatformManagedMetaTable, Base):
        __tablename__ = "example_assets__asset"
        __metatable_data_source_uid__ = "data-source-uid"
        __metatable_namespace__ = "example.assets"
        __metatable_identifier__ = "Asset"

        uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
        account_uid: Mapped[uuid.UUID] = mapped_column(
            Uuid,
            ForeignKey("example_assets__account.uid", ondelete="CASCADE"),
            nullable=False,
        )

    monkeypatch.setattr(MetaTable, "filter_by_body", staticmethod(lambda **kwargs: []))

    reserved_payloads = []

    def fake_bulk_create(rows, *, timeout=None, on_status=None):
        reserved_payloads.extend(rows)
        assert [table["identifier"] for table in rows] == ["example_assets__asset"]
        assert rows[0]["migration_package"] == "sample"
        assert rows[0]["migration_namespace"] == "markets"
        assert rows[0]["migration_provider_key"] == "sample:markets"
        assert rows[0]["is_alembic_managed"] is True
        assert "indexes" not in rows[0]["table_contract"]
        assert "foreign_keys" not in rows[0]["table_contract"]
        assert "schema" not in rows[0]["table_contract"]["physical"]
        assert "schema_management" not in rows[0]
        return [
            _reserved_metatable(
                uid="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
                identifier="example_assets__asset",
                physical_table_name="example_assets__asset",
                storage_hash=rows[0]["storage_hash"],
            )
        ]

    monkeypatch.setattr(MetaTable, "bulk_create", staticmethod(fake_bulk_create))

    migration = AlembicMetaTableMigration(
        package="sample",
        migration_namespace="markets",
        script_location="sample:migrations",
        target_metadata=Base.metadata,
        alembic_registry=ProjectAlembicVersion,
        metatable_models=[Asset],
    )

    prepared = migration.prepare_for_alembic(timeout=5)

    assert [payload["identifier"] for payload in reserved_payloads] == ["example_assets__asset"]
    assert prepared.meta_table_uids == ["bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"]


def test_prepare_for_alembic_routes_time_indexed_models_to_dynamic_table_bulk_create(
    monkeypatch,
):
    class Base(DeclarativeBase):
        metadata = MetaData()

    class ProjectAlembicVersion(AlembicVersionMetaTable):
        __metatable_uid__ = "registry-meta-table-uid"
        __metatable_data_source_uid__ = "data-source-uid"

    class Prices(PlatformTimeIndexMetaTable, Base):
        __tablename__ = "example_assets__prices"
        __metatable_data_source_uid__ = "data-source-uid"
        __metatable_namespace__ = "example.assets"
        __metatable_identifier__ = "Prices"
        __time_index_name__ = "time_index"
        __index_names__ = ["time_index", "asset_identifier"]

        time_index: Mapped[datetime.datetime] = mapped_column(
            DateTime(timezone=True),
            nullable=False,
        )
        asset_identifier: Mapped[str] = mapped_column(String(255), nullable=False)
        close: Mapped[int] = mapped_column(Integer, nullable=True)

    captured_rows = []
    monkeypatch.setattr(TimeIndexMetaTable, "filter_by_body", staticmethod(lambda **kwargs: []))
    monkeypatch.setattr(
        MetaTable,
        "bulk_create",
        staticmethod(lambda rows, **kwargs: pytest.fail("regular MetaTable endpoint used")),
    )

    def fake_dynamic_bulk_create(rows, *, timeout=None, on_status=None):
        captured_rows.extend(rows)
        return [
            _reserved_metatable(
                TimeIndexMetaTable,
                uid="cccccccc-cccc-4ccc-8ccc-cccccccccccc",
                identifier="example_assets__prices",
                physical_table_name="example_assets__prices",
                storage_hash=rows[0]["storage_hash"],
            )
        ]

    monkeypatch.setattr(
        TimeIndexMetaTable,
        "bulk_create",
        staticmethod(fake_dynamic_bulk_create),
    )

    migration = AlembicMetaTableMigration(
        package="sample",
        migration_namespace="markets",
        script_location="sample:migrations",
        target_metadata=Base.metadata,
        alembic_registry=ProjectAlembicVersion,
        metatable_models=[Prices],
    )

    prepared = migration.prepare_for_alembic(timeout=5)

    assert prepared.meta_table_uids == ["cccccccc-cccc-4ccc-8ccc-cccccccccccc"]
    assert len(captured_rows) == 1
    row = captured_rows[0]
    assert row["identifier"] == "example_assets__prices"
    assert row["physical_table_name"] == "example_assets__prices"
    assert row["is_alembic_managed"] is True
    assert row["provisioning_status"] == "reserved"
    assert row["time_index_name"] == "time_index"
    assert row["partition_strategy"] == "backend_default"
    assert "schema_management" not in row


def test_provider_adds_time_index_grain_index_when_table_cls_is_overridden():
    class Base(DeclarativeBase):
        metadata = MetaData(schema="public")

    class CustomTimeIndexMixin(PlatformTimeIndexMetaTable):
        __abstract__ = True

        @classmethod
        def __table_cls__(cls, *args, **kwargs):
            name, metadata, *table_items = args
            return Table(str(name), metadata, *table_items, **kwargs)

    class ProjectAlembicVersion(AlembicVersionMetaTable):
        __metatable_uid__ = "registry-meta-table-uid"
        __metatable_data_source_uid__ = "data-source-uid"

    class Prices(CustomTimeIndexMixin, Base):
        __tablename__ = "example_assets__prices"
        __metatable_data_source_uid__ = "data-source-uid"
        __metatable_namespace__ = "example.assets"
        __metatable_identifier__ = "Prices"
        __time_index_name__ = "time_index"
        __index_names__ = ["time_index", "asset_identifier"]

        time_index: Mapped[datetime.datetime] = mapped_column(
            DateTime(timezone=True),
            nullable=False,
        )
        asset_identifier: Mapped[str] = mapped_column(String(255), nullable=False)
        close: Mapped[int] = mapped_column(Integer, nullable=True)

    assert not Prices.__table__.indexes
    assert Prices.__table__.schema == "public"
    assert "public.example_assets__prices" in Base.metadata.tables

    AlembicMetaTableMigration(
        package="sample",
        migration_namespace="markets",
        script_location="sample:migrations",
        target_metadata=Base.metadata,
        alembic_registry=ProjectAlembicVersion,
        metatable_models=[Prices],
    )

    assert Prices.__table__.schema is None
    assert "example_assets__prices" in Base.metadata.tables
    assert "public.example_assets__prices" not in Base.metadata.tables

    grain_indexes = [
        index
        for index in Prices.__table__.indexes
        if index.unique
        and [column.name for column in index.columns] == ["time_index", "asset_identifier"]
    ]
    assert len(grain_indexes) == 1
    assert grain_indexes[0].name == schema_index_name(
        "example_assets__prices",
        ["time_index", "asset_identifier"],
        unique=True,
    )


def test_prepare_for_alembic_reuses_existing_reserved_table_name(monkeypatch):
    class Base(DeclarativeBase):
        metadata = MetaData()

    class ProjectAlembicVersion(AlembicVersionMetaTable):
        __metatable_data_source_uid__ = "data-source-uid"

    class Account(PlatformManagedMetaTable, Base):
        __tablename__ = "example_assets__account"
        __metatable_data_source_uid__ = "data-source-uid"
        __metatable_namespace__ = "example.assets"
        __metatable_identifier__ = "Account"

        uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
        name: Mapped[str] = mapped_column(String(255), nullable=False)

    class Asset(PlatformManagedMetaTable, Base):
        __tablename__ = "example_assets__asset"
        __metatable_data_source_uid__ = "data-source-uid"
        __metatable_namespace__ = "example.assets"
        __metatable_identifier__ = "Asset"

        uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
        account_uid: Mapped[uuid.UUID] = mapped_column(
            Uuid,
            ForeignKey("example_assets__account.uid", ondelete="CASCADE"),
            nullable=False,
        )
        symbol: Mapped[str] = mapped_column(String(64), nullable=False)

    filter_calls = []

    def fake_filter_by_body(**kwargs):
        filter_calls.append(kwargs)
        assert set(kwargs) == {"timeout", "physical_table_name__in", "limit"}
        assert kwargs["physical_table_name__in"] == [
            "example_assets__account",
            "example_assets__asset",
        ]
        assert kwargs["limit"] == 2
        return [
            MetaTable.model_construct(
                identifier="example_assets__account",
                uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                data_source_uid="data-source-uid",
                management_mode="platform_managed",
                provisioning_status="reserved",
                storage_hash="account-storage-hash",
                physical_table_name="example_assets__account",
                table_contract={
                    "version": "relational-table.v1",
                    "physical": {"table_name": "example_assets__account"},
                    "columns": [],
                },
            )
        ]

    monkeypatch.setattr(MetaTable, "filter_by_body", staticmethod(fake_filter_by_body))

    reserved_payloads = []

    def fake_bulk_create(rows, *, timeout=None, on_status=None):
        reserved_payloads.extend(rows)
        assert [table["identifier"] for table in rows] == [
            "example_assets__asset",
        ]
        assert all(table["migration_package"] == "sample" for table in rows)
        assert all(table["migration_namespace"] == "markets" for table in rows)
        assert all(table["migration_provider_key"] == "sample:markets" for table in rows)
        assert all(table["is_alembic_managed"] is True for table in rows)
        assert "indexes" not in rows[0]["table_contract"]
        assert "foreign_keys" not in rows[0]["table_contract"]
        assert "schema" not in rows[0]["table_contract"]["physical"]
        assert all("schema_management" not in table for table in rows)
        return [
            _reserved_metatable(
                uid="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
                identifier="example_assets__asset",
                physical_table_name="example_assets__asset",
                storage_hash=rows[0]["storage_hash"],
            ),
        ]

    monkeypatch.setattr(MetaTable, "bulk_create", staticmethod(fake_bulk_create))
    reservation_statuses = []

    migration = AlembicMetaTableMigration(
        package="sample",
        migration_namespace="markets",
        script_location="sample:migrations",
        target_metadata=Base.metadata,
        alembic_registry=ProjectAlembicVersion,
        metatable_models=[Account, Asset],
    )

    prepared = migration.prepare_for_alembic(
        timeout=5,
        on_metatable_reservation_status=reservation_statuses.append,
    )

    assert len(filter_calls) == 1
    assert [payload["identifier"] for payload in reserved_payloads] == ["example_assets__asset"]
    assert "schema_management" not in reserved_payloads[0]
    assert prepared.meta_table_uids == [
        "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
    ]
    assert [item.uid for item in prepared.reserved_tables] == [
        "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
    ]
    assert Account.__table__.name == "example_assets__account"
    assert Asset.__table__.name == "example_assets__asset"
    assert (
        "Reusing existing reserved MetaTable table_name=example_assets__account."
    ) in reservation_statuses


def test_prepare_for_alembic_reserves_already_staged_existing_rows(monkeypatch):
    class Base(DeclarativeBase):
        metadata = MetaData()

    class ProjectAlembicVersion(AlembicVersionMetaTable):
        __metatable_uid__ = "registry-meta-table-uid"
        __metatable_data_source_uid__ = "data-source-uid"

    class Account(PlatformManagedMetaTable, Base):
        __tablename__ = "example_assets__account"
        __metatable_data_source_uid__ = "data-source-uid"
        __metatable_namespace__ = "example.assets"
        __metatable_identifier__ = "Account"

        uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
        name: Mapped[str] = mapped_column(String(255), nullable=False)

    class Asset(PlatformManagedMetaTable, Base):
        __tablename__ = "example_assets__asset"
        __metatable_data_source_uid__ = "data-source-uid"
        __metatable_namespace__ = "example.assets"
        __metatable_identifier__ = "Asset"

        uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
        account_uid: Mapped[uuid.UUID] = mapped_column(
            Uuid,
            ForeignKey("example_assets__account.uid", ondelete="CASCADE"),
            nullable=False,
        )
        symbol: Mapped[str] = mapped_column(String(64), nullable=False)

    def fake_filter_by_body(**kwargs):
        assert set(kwargs) == {"timeout", "physical_table_name__in", "limit"}
        assert kwargs["physical_table_name__in"] == [
            "example_assets__account",
            "example_assets__asset",
        ]
        assert kwargs["limit"] == 2
        return [
            MetaTable.model_construct(
                identifier="example_assets__account",
                uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                data_source_uid="data-source-uid",
                management_mode="platform_managed",
                provisioning_status="active",
                schema_management_mode="alembic_managed",
                migration_provider_key="sample:markets",
                alembic_version_meta_table_uid="registry-meta-table-uid",
                storage_hash="account-storage-hash",
                physical_table_name="example_assets__account",
                table_contract={
                    "version": "relational-table.v1",
                    "physical": {"table_name": "example_assets__account"},
                    "columns": [],
                },
            ),
            MetaTable.model_construct(
                identifier="example_assets__asset",
                uid="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
                data_source_uid="data-source-uid",
                management_mode="platform_managed",
                provisioning_status="active",
                schema_management_mode="alembic_managed",
                migration_provider_key="sample:markets",
                alembic_version_meta_table_uid="registry-meta-table-uid",
                storage_hash="asset-storage-hash",
                physical_table_name="example_assets__asset",
                table_contract={
                    "version": "relational-table.v1",
                    "physical": {"table_name": "example_assets__asset"},
                    "columns": [],
                },
            ),
        ]

    monkeypatch.setattr(MetaTable, "filter_by_body", staticmethod(fake_filter_by_body))

    def fake_bulk_create(rows, *, timeout=None, on_status=None):
        raise AssertionError("ready existing rows must not be collection-created again")

    monkeypatch.setattr(MetaTable, "bulk_create", staticmethod(fake_bulk_create))

    migration = AlembicMetaTableMigration(
        package="sample",
        migration_namespace="markets",
        script_location="sample:migrations",
        target_metadata=Base.metadata,
        alembic_registry=ProjectAlembicVersion,
        metatable_models=[Account, Asset],
    )

    prepared = migration.prepare_for_alembic(timeout=5)

    assert prepared.meta_table_uids == [
        "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
    ]
    assert Account.__table__.name == "example_assets__account"
    assert Asset.__table__.name == "example_assets__asset"
    assert next(iter(Asset.__table__.foreign_key_constraints)).name is None


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
