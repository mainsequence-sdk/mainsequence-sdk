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
        MarketDataNode.validate_asset_list(
            [_asset("asset:one"), _asset("asset:one")]
        )


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


def test_market_data_node_asset_data_access_helpers_use_dimension_contract():
    class TestMarketDataNode(MarketDataNode):
        def dependencies(self):
            return {}

        def update(self):
            return None

    class FakePersistManager:
        def __init__(self):
            self.last_calls = []
            self.range_calls = []

        def get_last_observation(self, **kwargs):
            self.last_calls.append(kwargs)
            return "latest"

        def get_df_between_dates(self, **kwargs):
            self.range_calls.append(kwargs)
            return "range"

    manager = FakePersistManager()
    node = object.__new__(TestMarketDataNode)
    node._local_persist_manager = manager
    node.asset_list = [_asset("asset:one"), _asset("asset:two")]
    node._get_data_node_configuration = lambda: None

    assert node.get_last_observation() == "latest"
    assert manager.last_calls == [
        {
            "dimension_filters": {
                "unique_identifier": ["asset:one", "asset:two"],
            }
        }
    ]

    start_date = dt.datetime(2026, 1, 1, tzinfo=dt.UTC)
    range_descriptor = {"asset:one": {"start_date": start_date}}
    assert node.get_ranged_data_per_asset(range_descriptor) == "range"
    assert manager.range_calls[-1]["dimension_range_map"] == [
        {
            "coordinate": {"unique_identifier": "asset:one"},
            "start_date": start_date,
        }
    ]

    assert node.get_ranged_data_per_asset_great_or_equal(range_descriptor) == "range"
    assert manager.range_calls[-1]["dimension_range_map"] == [
        {
            "coordinate": {"unique_identifier": "asset:one"},
            "start_date": start_date,
            "start_date_operand": ">=",
        }
    ]
    assert range_descriptor == {"asset:one": {"start_date": start_date}}


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


def test_requested_market_nodes_use_market_data_node_boundary():
    from mainsequence.markets.accounts.data_nodes import HoldingsDataNode
    from mainsequence.markets.instruments.interest_rates.etl.nodes import (
        DiscountCurvesNode,
        FixingRatesNode,
    )
    from mainsequence.markets.portfolios.contrib.prices.data_nodes import (
        ExternalPrices,
        InterpolatedPrices,
    )
    from mainsequence.markets.portfolios.contrib.signals.external_weights import (
        ExternalWeights,
    )
    from mainsequence.markets.portfolios.contrib.signals.fixed_weights import FixedWeights
    from mainsequence.markets.portfolios.contrib.signals.market_cap import MarketCap
    from mainsequence.markets.portfolios.contrib.signals.portfolio_replicator import (
        ETFReplicator,
    )
    from mainsequence.markets.portfolios.data_nodes import (
        AssetScopedPortfolioCanonicalDataNode,
        PortfoliosDataNode,
        PortfolioWeights,
        SignalWeights,
    )

    assert issubclass(HoldingsDataNode, MarketDataNode)
    assert issubclass(InterpolatedPrices, MarketDataNode)
    assert issubclass(ExternalPrices, MarketDataNode)
    assert issubclass(DiscountCurvesNode, MarketDataNode)
    assert issubclass(FixingRatesNode, MarketDataNode)

    assert issubclass(SignalWeights, AssetScopedPortfolioCanonicalDataNode)
    assert issubclass(PortfolioWeights, AssetScopedPortfolioCanonicalDataNode)
    assert issubclass(PortfoliosDataNode, AssetScopedPortfolioCanonicalDataNode)
    assert issubclass(SignalWeights, MarketDataNode)
    assert issubclass(PortfolioWeights, MarketDataNode)
    assert issubclass(PortfoliosDataNode, MarketDataNode)

    assert issubclass(FixedWeights, MarketDataNode)
    assert issubclass(ExternalWeights, MarketDataNode)
    assert issubclass(MarketCap, MarketDataNode)
    assert issubclass(ETFReplicator, MarketDataNode)


def test_portfolios_data_node_asset_scope_is_the_portfolio_asset():
    from mainsequence.markets.portfolios.data_nodes import PortfoliosDataNode

    imported_node = object.__new__(PortfoliosDataNode)
    assert imported_node.get_asset_list() is None

    explicit_asset = _asset("portfolio:imported")
    imported_node._asset = explicit_asset
    assert imported_node.get_asset_list() == [explicit_asset]

    workflow_asset = _asset("portfolio:workflow")
    workflow_node = object.__new__(PortfoliosDataNode)
    workflow_node.target_portfolio = object()
    workflow_node.index_asset = workflow_asset
    assert workflow_node.get_asset_list() == [workflow_asset]
