from __future__ import annotations

import os
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

os.environ.setdefault("MAINSEQUENCE_ACCESS_TOKEN", "test-token")
os.environ.setdefault("MAINSEQUENCE_REFRESH_TOKEN", "test-refresh-token")

from mainsequence.markets.portfolios.contrib.prices import data_nodes as price_nodes
from mainsequence.markets.portfolios.models import (
    AssetsConfiguration,
    MarketsTimeSeries,
    PricesConfiguration,
)
from mainsequence.tdag.data_nodes import APIDataNode, DataNode


def _patch_data_node_source(monkeypatch):
    monkeypatch.setattr(
        DataNode,
        "set_data_source",
        lambda self, data_source=None: setattr(
            self,
            "_data_source",
            SimpleNamespace(id=1, related_resource_class_type=None),
        ),
    )


def test_interpolated_prices_requires_explicit_source():
    config = price_nodes.InterpolatedPricesConfig(
        bar_frequency_id="1d",
        intraday_bar_interpolation_rule="ffill",
        upsample_frequency_id="1d",
        asset_category_unique_id="crypto",
    )

    with pytest.raises(ValueError, match="requires an explicit source_bars_data_node"):
        price_nodes.InterpolatedPrices(interpolation_config=config)


def test_prices_configuration_rejects_legacy_source_fields():
    with pytest.raises(ValidationError, match="legacy_source_id"):
        PricesConfiguration(legacy_source_id="old-indirect-source")


def test_prices_configuration_markets_time_series_builds_explicit_api_source(monkeypatch):
    _patch_data_node_source(monkeypatch)
    source = APIDataNode(data_source_id=1, storage_hash="normalized_prices_hash")
    calls: list[str] = []

    def build_from_identifier(cls, identifier: str):
        calls.append(identifier)
        return source

    monkeypatch.setattr(
        price_nodes.APIDataNode,
        "build_from_identifier",
        classmethod(build_from_identifier),
    )

    node = price_nodes.get_interpolated_prices_timeseries(
        AssetsConfiguration(
            assets_category_unique_id="crypto",
            prices_configuration=PricesConfiguration(
                markets_time_series=MarketsTimeSeries(
                    unique_identifier="normalized_portfolio_prices"
                ),
            ),
        )
    )

    assert calls == ["normalized_portfolio_prices"]
    assert node.bars_ts is source
