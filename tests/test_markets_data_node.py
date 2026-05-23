from __future__ import annotations

import datetime as dt
import os
from types import SimpleNamespace

import pytest

os.environ.setdefault("MAINSEQUENCE_ACCESS_TOKEN", "test-token")
os.environ.setdefault("MAINSEQUENCE_REFRESH_TOKEN", "test-refresh-token")

from mainsequence.client.markets.models.assets import Asset
from mainsequence.markets.assets.data_nodes import (
    ASSET_DATA_NODE_INDEX_NAMES,
    AssetDataNodeConfiguration,
    AssetSnapshot,
    AssetTimestampedDataNode,
    AssetTimestampedFrameMixin,
)
from mainsequence.markets.markets_data_node import (
    MarketDataNode,
    MarketDataNodeConfiguration,
)
from mainsequence.tdag.data_nodes import DataNode


def _asset(unique_identifier: str) -> Asset:
    return Asset(unique_identifier=unique_identifier)


def test_market_data_node_validates_asset_lists():
    assets = [_asset("asset:one"), _asset("asset:two")]

    assert MarketDataNode.validate_asset_list(assets) == assets
    assert MarketDataNode.validate_asset_list(None) is None
    assert MarketDataNode.validate_asset_list([], allow_empty=True) == []

    with pytest.raises(ValueError, match="cannot be empty"):
        MarketDataNode.validate_asset_list([])

    with pytest.raises(TypeError, match="AssetMixin"):
        MarketDataNode.validate_asset_list([object()])

    with pytest.raises(ValueError, match="non-empty"):
        MarketDataNode.validate_asset_list([_asset(" ")])

    with pytest.raises(ValueError, match="duplicate"):
        MarketDataNode.validate_asset_list([_asset("asset:one"), _asset("asset:one")])


def test_market_data_node_converts_assets_to_dimension_filters():
    assets = [_asset("asset:one"), _asset("asset:two")]

    assert MarketDataNode.asset_unique_identifiers(assets) == ["asset:one", "asset:two"]
    assert MarketDataNode.asset_dimension_filters(assets) == {
        "unique_identifier": ["asset:one", "asset:two"],
    }
    assert MarketDataNode.asset_dimension_filters(None) is None


def test_market_data_node_converts_asset_ranges_to_dimension_ranges():
    start_date = dt.datetime(2026, 1, 1, tzinfo=dt.UTC)
    range_descriptor = {"asset:one": {"start_date": start_date}}

    dimension_range_map = MarketDataNode.asset_range_map_to_dimension_range_map(
        range_descriptor
    )

    assert dimension_range_map == [
        {
            "coordinate": {"unique_identifier": "asset:one"},
            "start_date": start_date,
        }
    ]
    dimension_range_map[0]["start_date"] = dt.datetime(2027, 1, 1, tzinfo=dt.UTC)
    assert range_descriptor["asset:one"]["start_date"] == start_date


def test_asset_timestamped_nodes_use_market_data_node_boundary(monkeypatch):
    monkeypatch.setattr(
        DataNode,
        "set_data_source",
        lambda self, data_source=None: setattr(
            self,
            "_data_source",
            SimpleNamespace(id=1, related_resource_class_type=None),
        ),
    )

    assert issubclass(AssetDataNodeConfiguration, MarketDataNodeConfiguration)
    assert issubclass(AssetTimestampedDataNode, MarketDataNode)
    assert issubclass(AssetSnapshot, AssetTimestampedDataNode)
    assert issubclass(AssetSnapshot, AssetTimestampedFrameMixin)

    node = AssetSnapshot()

    assert isinstance(node, MarketDataNode)
    assert isinstance(node, AssetTimestampedFrameMixin)
    assert node.config.index_names == ASSET_DATA_NODE_INDEX_NAMES
    assert node.dependencies() == {}
