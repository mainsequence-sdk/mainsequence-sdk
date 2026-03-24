from __future__ import annotations

from importlib import import_module

from mainsequence.instrumentation import TracerInstrumentator

_LAZY_IMPORTS = {
    "TIME_SERIES_SOURCE_TIMESCALE": (".config", "TIME_SERIES_SOURCE_TIMESCALE"),
    "RunningMode": (".config", "RunningMode"),
    "configuration": (".config", "configuration"),
    "ogm": (".config", "ogm"),
    "BaseConfiguration": (".configuration_models", "BaseConfiguration"),
    "APIDataNode": (".data_nodes", "APIDataNode"),
    "DataNode": (".data_nodes", "DataNode"),
    "DataNodeConfiguration": (".data_nodes", "DataNodeConfiguration"),
    "DataNodeMetaData": (".data_nodes", "DataNodeMetaData"),
    "RecordDefinition": (".data_nodes", "RecordDefinition"),
    "WrapperDataNode": (".data_nodes", "WrapperDataNode"),
    "WrapperDataNodeConfig": (".data_nodes", "WrapperDataNodeConfig"),
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
