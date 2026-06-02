from __future__ import annotations

from importlib import import_module

from mainsequence.instrumentation import TracerInstrumentator

_LAZY_IMPORTS = {
    "DEFAULT_PLATFORM_MANAGED_PROVISIONING": (
        ".sqlalchemy_contracts",
        "DEFAULT_PLATFORM_MANAGED_PROVISIONING",
    ),
    "MetaTableForeignKey": (".sqlalchemy_contracts", "MetaTableForeignKey"),
    "AlembicVersionMetaTable": (".migrations", "AlembicVersionMetaTable"),
    "PlatformManagedMetaTable": (".sqlalchemy_contracts", "PlatformManagedMetaTable"),
    "PlatformTimeIndexMetaData": (".sqlalchemy_contracts", "PlatformTimeIndexMetaData"),
    "PackagedAlembicMigrationArtifact": (
        ".migrations",
        "PackagedAlembicMigrationArtifact",
    ),
    "POSTGRES_IDENTIFIER_MAX_LENGTH": (".hashing", "POSTGRES_IDENTIFIER_MAX_LENGTH"),
    "BaseConfiguration": (".data_nodes", "BaseConfiguration"),
    "APIDataNode": (".data_nodes", "APIDataNode"),
    "DataNode": (".data_nodes", "DataNode"),
    "DataNodeConfiguration": (".data_nodes", "DataNodeConfiguration"),
    "build_meta_table_configured_storage_hash": (
        ".hashing",
        "build_meta_table_configured_storage_hash",
    ),
    "build_meta_table_storage_hash": (".hashing", "build_meta_table_storage_hash"),
    "external_registered_registration_request_from_sqlalchemy_model": (
        ".sqlalchemy_contracts",
        "external_registered_registration_request_from_sqlalchemy_model",
    ),
    "metatable_configured_tablename": (
        ".sqlalchemy_contracts",
        "metatable_configured_tablename",
    ),
    "DEFAULT_ALEMBIC_VERSION_IDENTIFIER": (
        ".migrations",
        "DEFAULT_ALEMBIC_VERSION_IDENTIFIER",
    ),
    "DEFAULT_ALEMBIC_VERSION_NAMESPACE": (
        ".migrations",
        "DEFAULT_ALEMBIC_VERSION_NAMESPACE",
    ),
    "DEFAULT_ALEMBIC_VERSION_SCHEMA": (
        ".migrations",
        "DEFAULT_ALEMBIC_VERSION_SCHEMA",
    ),
    "DEFAULT_ALEMBIC_VERSION_TABLE_NAME": (
        ".migrations",
        "DEFAULT_ALEMBIC_VERSION_TABLE_NAME",
    ),
    "DEFAULT_ALEMBIC_VERSION_COLUMN_NAME": (
        ".migrations",
        "DEFAULT_ALEMBIC_VERSION_COLUMN_NAME",
    ),
    "metatable_tablename": (".sqlalchemy_contracts", "metatable_tablename"),
    "platform_managed_registration_request_from_sqlalchemy_model": (
        ".sqlalchemy_contracts",
        "platform_managed_registration_request_from_sqlalchemy_model",
    ),
    "register_external_sqlalchemy_model": (
        ".sqlalchemy_contracts",
        "register_external_sqlalchemy_model",
    ),
    "render_packaged_alembic_migration": (
        ".migrations",
        "render_packaged_alembic_migration",
    ),
    "slugify_identifier": (".hashing", "slugify_identifier"),
    "table_contract_from_sqlalchemy_model": (
        ".sqlalchemy_contracts",
        "table_contract_from_sqlalchemy_model",
    ),
    "time_indexed_registration_request_from_sqlalchemy_model": (
        ".sqlalchemy_contracts",
        "time_indexed_registration_request_from_sqlalchemy_model",
    ),
}

__all__ = ["TracerInstrumentator", *_LAZY_IMPORTS.keys()]


def __getattr__(name: str):
    if name == "TracerInstrumentator":
        return TracerInstrumentator
    if name in _LAZY_IMPORTS:
        module_name, attr_name = _LAZY_IMPORTS[name]
        module = import_module(module_name, __name__)
        value = getattr(module, attr_name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(__all__)
