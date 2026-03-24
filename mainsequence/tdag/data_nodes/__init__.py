from __future__ import annotations

from importlib import import_module

_LAZY_IMPORTS = {
    "BaseConfiguration": ("..configuration_models", "BaseConfiguration"),
    "APIDataNode": (".data_nodes", "APIDataNode"),
    "DataNode": (".data_nodes", "DataNode"),
    "WrapperDataNode": (".data_nodes", "WrapperDataNode"),
    "WrapperDataNodeConfig": (".data_nodes", "WrapperDataNodeConfig"),
    "DataNodeConfiguration": (".models", "DataNodeConfiguration"),
    "DataNodeMetaData": (".models", "DataNodeMetaData"),
    "RecordDefinition": (".models", "RecordDefinition"),
    "hash_namespace": (".namespacing", "hash_namespace"),
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
