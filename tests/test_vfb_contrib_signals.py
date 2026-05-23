from __future__ import annotations

import inspect
import os
from types import SimpleNamespace

import pandas as pd

os.environ.setdefault("MAINSEQUENCE_ACCESS_TOKEN", "test-token")
os.environ.setdefault("MAINSEQUENCE_REFRESH_TOKEN", "test-refresh-token")

from mainsequence.markets.portfolios import data_nodes, simple_tables
from mainsequence.markets.portfolios.contrib.signals.external_weights import (
    ExternalWeights,
    ExternalWeightsConfig,
)
from mainsequence.markets.portfolios.contrib.signals.fixed_weights import (
    AUIDWeight,
    FixedWeights,
    FixedWeightsConfig,
)
from mainsequence.markets.portfolios.contrib.signals.intraday_trend import (
    IntradayTrend,
    IntradayTrendConfig,
)
from mainsequence.markets.portfolios.contrib.signals.market_cap import MarketCap
from mainsequence.markets.portfolios.contrib.signals.portfolio_replicator import (
    ETFReplicator,
    ETFReplicatorConfig,
    TrackingStrategyConfiguration,
)
from mainsequence.markets.portfolios.data_nodes import (
    SIGNAL_UID,
    SIGNAL_WEIGHTS_INDEX_NAMES,
    SignalWeights,
)
from mainsequence.markets.portfolios.models import (
    AssetsConfiguration,
    PricesConfiguration,
)
from mainsequence.tdag.data_nodes import DataNode


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


def _assets_configuration() -> AssetsConfiguration:
    return AssetsConfiguration(prices_configuration=PricesConfiguration())


def _fixed_weights_config(*asset_uids: str) -> FixedWeightsConfig:
    weight = 1.0 / len(asset_uids)
    return FixedWeightsConfig(
        signal_assets_configuration=_assets_configuration(),
        asset_unique_identifier_weights=[
            AUIDWeight(unique_identifier=asset_uid, weight=weight) for asset_uid in asset_uids
        ],
    )


def test_contrib_market_cap_signals_are_signal_weight_data_nodes():
    assert issubclass(FixedWeights, SignalWeights)
    assert issubclass(MarketCap, SignalWeights)
    assert issubclass(ExternalWeights, SignalWeights)
    assert issubclass(IntradayTrend, SignalWeights)
    assert issubclass(ETFReplicator, SignalWeights)


def test_contrib_signals_use_signal_weights_constructor_contract():
    for signal_cls in (
        FixedWeights,
        MarketCap,
        ExternalWeights,
        IntradayTrend,
        ETFReplicator,
    ):
        signature = inspect.signature(signal_cls.__init__)
        assert "config" in signature.parameters
        assert "weights_config" not in signature.parameters
        assert "trend_config" not in signature.parameters
        assert "replicator_config" not in signature.parameters


def test_old_contrib_signals_use_runtime_signal_configuration(monkeypatch):
    _patch_data_node_source(monkeypatch)
    signal_configs = [
        ExternalWeightsConfig(
            signal_assets_configuration=_assets_configuration(),
            artifact_name="weights.csv",
            bucket_name="research",
        ),
        IntradayTrendConfig(
            signal_assets_configuration=_assets_configuration(),
            calendar="NYSE",
            source_frequency="15m",
        ),
        ETFReplicatorConfig(
            signal_assets_configuration=_assets_configuration(),
            etf_ticker="SPY",
            tracking_strategy_configuration=TrackingStrategyConfiguration(),
        ),
    ]
    signal_classes = [ExternalWeights, IntradayTrend, ETFReplicator]
    canonical_table = SignalWeights(namespace="research")

    for signal_cls, signal_config in zip(signal_classes, signal_configs, strict=True):
        signal = signal_cls.from_signal_configuration(
            signal_config,
            namespace="research",
        )
        assert signal.signal_configuration == signal_config
        assert signal.storage_hash == canonical_table.storage_hash
        assert signal.update_hash == canonical_table.update_hash
        assert signal.signal_uid == data_nodes.compute_signal_uid(signal)


def test_fixed_weights_uses_shared_signal_weights_storage_hash(monkeypatch):
    _patch_data_node_source(monkeypatch)
    first_signal = FixedWeights.from_signal_configuration(
        _fixed_weights_config("asset:btc", "asset:eth"),
        namespace="research",
    )
    second_signal = FixedWeights.from_signal_configuration(
        _fixed_weights_config("asset:btc", "asset:eth"),
        namespace="research",
    )
    canonical_table = SignalWeights(namespace="research")

    assert first_signal.storage_hash == canonical_table.storage_hash
    assert first_signal.update_hash == canonical_table.update_hash
    assert first_signal.update_hash == second_signal.update_hash
    assert first_signal.signal_uid == second_signal.signal_uid


def test_fixed_weights_config_changes_signal_uid_not_signal_weights_identity(
    monkeypatch,
):
    _patch_data_node_source(monkeypatch)

    first_signal = FixedWeights.from_signal_configuration(
        _fixed_weights_config("asset:btc", "asset:eth"),
        namespace="research",
    )
    second_signal = FixedWeights.from_signal_configuration(
        _fixed_weights_config("asset:btc", "asset:sol"),
        namespace="research",
    )

    assert first_signal.signal_uid != second_signal.signal_uid
    assert first_signal.update_hash == second_signal.update_hash
    assert first_signal.storage_hash == second_signal.storage_hash


def test_fixed_weights_calculates_canonical_signal_weight_rows(monkeypatch):
    _patch_data_node_source(monkeypatch)
    monkeypatch.setattr(
        simple_tables,
        "upsert_signal_metadata",
        lambda **kwargs: None,
    )
    monkeypatch.setattr(
        SignalWeights,
        "get_df_between_dates",
        lambda self, **kwargs: pd.DataFrame(),
    )

    signal = FixedWeights.from_signal_configuration(
        _fixed_weights_config("asset:btc", "asset:eth"),
        namespace="research",
    )
    frame = signal.update()

    assert list(frame.index.names) == SIGNAL_WEIGHTS_INDEX_NAMES
    assert frame.index.get_level_values(SIGNAL_UID).unique().tolist() == [signal.signal_uid]
    assert frame["signal_weight"].sum() == 1.0
    assert set(frame.index.get_level_values("unique_identifier")) == {
        "asset:btc",
        "asset:eth",
    }


def test_fixed_weights_returns_empty_canonical_frame_when_signal_already_exists(
    monkeypatch,
):
    _patch_data_node_source(monkeypatch)
    monkeypatch.setattr(
        simple_tables,
        "upsert_signal_metadata",
        lambda **kwargs: None,
    )
    existing = pd.DataFrame(
        {"signal_weight": [1.0]},
        index=pd.MultiIndex.from_tuples(
            [(pd.Timestamp("2024-01-01", tz="UTC"), "existing", "asset:btc")],
            names=SIGNAL_WEIGHTS_INDEX_NAMES,
        ),
    )
    monkeypatch.setattr(
        SignalWeights,
        "get_df_between_dates",
        lambda self, **kwargs: existing,
    )

    signal = FixedWeights.from_signal_configuration(
        _fixed_weights_config("asset:btc", "asset:eth"),
        namespace="research",
    )
    frame = signal.update()

    assert frame.empty
    assert list(frame.index.names) == SIGNAL_WEIGHTS_INDEX_NAMES
