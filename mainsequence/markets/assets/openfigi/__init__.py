from __future__ import annotations

from importlib import import_module

_EXPORTS = {
    "DEFAULT_MASTER_LIST_DEPENDENCY": ".simple_tables",
    "OpenFigiAssetRows": ".api",
    "OpenFigiDetails": ".simple_tables",
    "OpenFigiDetailsConfiguration": ".simple_tables",
    "OpenFigiDetailsUpdater": ".simple_tables",
    "build_asset_rows_from_openfigi_result": ".api",
    "build_asset_snapshot_frame_from_openfigi_result": ".api",
    "get_open_figi_definitions": ".api",
    "load_openfigi_lists": ".api",
    "normalize_openfigi_result": ".api",
    "query_by_figi": ".api",
    "query_by_isin": ".api",
    "query_figi": ".api",
    "search_figi": ".api",
}


def __getattr__(name: str):
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(_EXPORTS[name], __name__)
    value = getattr(module, name)
    globals()[name] = value
    return value


__all__ = [
    "DEFAULT_MASTER_LIST_DEPENDENCY",
    "OpenFigiAssetRows",
    "OpenFigiDetails",
    "OpenFigiDetailsConfiguration",
    "OpenFigiDetailsUpdater",
    "build_asset_rows_from_openfigi_result",
    "build_asset_snapshot_frame_from_openfigi_result",
    "get_open_figi_definitions",
    "load_openfigi_lists",
    "normalize_openfigi_result",
    "query_by_figi",
    "query_by_isin",
    "query_figi",
    "search_figi",
]
