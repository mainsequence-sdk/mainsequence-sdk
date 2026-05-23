from __future__ import annotations

from importlib import import_module

_EXPORTS = {
    "AssetDataNodeConfiguration": ".data_nodes",
    "AssetPricingDetail": ".data_nodes",
    "AssetCategoryMembershipSimpleTable": ".simple_tables",
    "AssetCategoryMembershipSimpleTableConfiguration": ".simple_tables",
    "AssetCategoryMembershipSimpleTableUpdater": ".simple_tables",
    "AssetCategorySimpleTable": ".simple_tables",
    "AssetCategorySimpleTableConfiguration": ".simple_tables",
    "AssetCategorySimpleTableUpdater": ".simple_tables",
    "AssetSimpleTable": ".simple_tables",
    "AssetSimpleTableConfiguration": ".simple_tables",
    "AssetSimpleTableUpdater": ".simple_tables",
    "AssetSnapshot": ".data_nodes",
    "AssetTimestampedDataNode": ".data_nodes",
    "AssetTimestampedFrameMixin": ".data_nodes",
}


def __getattr__(name: str):
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(_EXPORTS[name], __name__)
    value = getattr(module, name)
    globals()[name] = value
    return value


__all__ = [
    "AssetDataNodeConfiguration",
    "AssetPricingDetail",
    "AssetCategoryMembershipSimpleTable",
    "AssetCategoryMembershipSimpleTableConfiguration",
    "AssetCategoryMembershipSimpleTableUpdater",
    "AssetCategorySimpleTable",
    "AssetCategorySimpleTableConfiguration",
    "AssetCategorySimpleTableUpdater",
    "AssetSimpleTable",
    "AssetSimpleTableConfiguration",
    "AssetSimpleTableUpdater",
    "AssetSnapshot",
    "AssetTimestampedDataNode",
    "AssetTimestampedFrameMixin",
]
