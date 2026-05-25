from __future__ import annotations

from importlib import import_module

_LAZY_IMPORTS = {
    "DEFAULT_PLATFORM_MANAGED_PROVISIONING": (
        ".sqlalchemy_contracts",
        "DEFAULT_PLATFORM_MANAGED_PROVISIONING",
    ),
    "PlatformManagedMetaTable": (".sqlalchemy_contracts", "PlatformManagedMetaTable"),
    "POSTGRES_IDENTIFIER_MAX_LENGTH": (".hashing", "POSTGRES_IDENTIFIER_MAX_LENGTH"),
    "build_compiled_sql_v1_operation": (
        ".compiled_sql",
        "build_compiled_sql_v1_operation",
    ),
    "build_meta_table_configured_storage_hash": (
        ".hashing",
        "build_meta_table_configured_storage_hash",
    ),
    "build_meta_table_storage_hash": (".hashing", "build_meta_table_storage_hash"),
    "compile_sqlalchemy_statement": (".compiled_sql", "compile_sqlalchemy_statement"),
    "external_registered_registration_request_from_sqlalchemy_model": (
        ".sqlalchemy_contracts",
        "external_registered_registration_request_from_sqlalchemy_model",
    ),
    "metatable_configured_tablename": (
        ".sqlalchemy_contracts",
        "metatable_configured_tablename",
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
    "register_platform_managed_sqlalchemy_model": (
        ".sqlalchemy_contracts",
        "register_platform_managed_sqlalchemy_model",
    ),
    "slugify_identifier": (".hashing", "slugify_identifier"),
    "table_contract_from_sqlalchemy_model": (
        ".sqlalchemy_contracts",
        "table_contract_from_sqlalchemy_model",
    ),
}

__all__ = list(_LAZY_IMPORTS.keys())


def __getattr__(name: str):
    if name in _LAZY_IMPORTS:
        module_name, attr_name = _LAZY_IMPORTS[name]
        module = import_module(module_name, __name__)
        value = getattr(module, attr_name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(__all__)
