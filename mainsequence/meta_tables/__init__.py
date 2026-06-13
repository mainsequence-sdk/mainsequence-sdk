from __future__ import annotations

from importlib import import_module

from mainsequence.instrumentation import TracerInstrumentator

_LAZY_IMPORTS = {
    "DEFAULT_PLATFORM_MANAGED_PROVISIONING": (
        ".sqlalchemy_contracts",
        "DEFAULT_PLATFORM_MANAGED_PROVISIONING",
    ),
    "AlembicMetaTableMigration": (".migrations", "AlembicMetaTableMigration"),
    "AlembicVersionMetaTable": (".migrations", "AlembicVersionMetaTable"),
    "PlatformManagedMetaTable": (".sqlalchemy_contracts", "PlatformManagedMetaTable"),
    "PlatformTimeIndexMetaTable": (".sqlalchemy_contracts", "PlatformTimeIndexMetaTable"),
    "compute_metatable_contract_hash": (
        ".sqlalchemy_contracts",
        "compute_metatable_contract_hash",
    ),
    "POSTGRES_IDENTIFIER_MAX_LENGTH": (".hashing", "POSTGRES_IDENTIFIER_MAX_LENGTH"),
    "SchemaTableNameParts": (".schema_names", "SchemaTableNameParts"),
    "BaseConfiguration": (".data_nodes", "BaseConfiguration"),
    "APIDataNode": (".data_nodes", "APIDataNode"),
    "DataNode": (".data_nodes", "DataNode"),
    "DataNodeConfiguration": (".data_nodes", "DataNodeConfiguration"),
    "build_meta_table_configured_storage_hash": (
        ".hashing",
        "build_meta_table_configured_storage_hash",
    ),
    "build_meta_table_storage_hash": (".hashing", "build_meta_table_storage_hash"),
    "bounded_identifier": (".schema_names", "bounded_identifier"),
    "external_registered_registration_request_from_sqlalchemy_model": (
        ".sqlalchemy_contracts",
        "external_registered_registration_request_from_sqlalchemy_model",
    ),
    "normalize_identifier_part": (".schema_names", "normalize_identifier_part"),
    "parse_schema_table_name": (".schema_names", "parse_schema_table_name"),
    "DEFAULT_ALEMBIC_VERSION_IDENTIFIER": (
        ".migrations",
        "DEFAULT_ALEMBIC_VERSION_IDENTIFIER",
    ),
    "DEFAULT_ALEMBIC_PROVIDER_REFERENCE": (
        ".migrations",
        "DEFAULT_ALEMBIC_PROVIDER_REFERENCE",
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
    "platform_managed_registration_request_from_sqlalchemy_model": (
        ".sqlalchemy_contracts",
        "platform_managed_registration_request_from_sqlalchemy_model",
    ),
    "platform_managed_migration_registration_context": (
        ".sqlalchemy_contracts",
        "platform_managed_migration_registration_context",
    ),
    "register_external_sqlalchemy_model": (
        ".sqlalchemy_contracts",
        "register_external_sqlalchemy_model",
    ),
    "resolve_metatable_identifier": (
        ".sqlalchemy_contracts",
        "resolve_metatable_identifier",
    ),
    "alembic_config_for_provider": (".migrations", "alembic_config_for_provider"),
    "apply_mainsequence_migration_role": (
        ".migrations",
        "apply_mainsequence_migration_role",
    ),
    "load_alembic_metatable_migration_provider": (
        ".migrations",
        "load_alembic_metatable_migration_provider",
    ),
    "PreparedAlembicMetaTableMigration": (
        ".migrations",
        "PreparedAlembicMetaTableMigration",
    ),
    "resolve_alembic_revision_metadata": (
        ".migrations",
        "resolve_alembic_revision_metadata",
    ),
    "schema_check_constraint_name": (".schema_names", "schema_check_constraint_name"),
    "schema_foreign_key_name": (".schema_names", "schema_foreign_key_name"),
    "schema_index_name": (".schema_names", "schema_index_name"),
    "schema_primary_key_name": (".schema_names", "schema_primary_key_name"),
    "schema_table_name": (".schema_names", "schema_table_name"),
    "schema_unique_constraint_name": (".schema_names", "schema_unique_constraint_name"),
    "slugify_identifier": (".hashing", "slugify_identifier"),
    "sqlalchemy_naming_convention": (".schema_names", "sqlalchemy_naming_convention"),
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
