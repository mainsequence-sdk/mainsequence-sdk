from __future__ import annotations

import os
from types import SimpleNamespace

import pandas as pd
import pytest
from pydantic import Field, ValidationError

os.environ.setdefault("MAINSEQUENCE_ACCESS_TOKEN", "test-token")
os.environ.setdefault("MAINSEQUENCE_REFRESH_TOKEN", "test-refresh-token")

from mainsequence.client.models_tdag import (
    LOGICAL_COLUMN_DTYPES_ATTR,
    DataNodeStorage,
    DataNodeUpdate,
)
from mainsequence.markets.portfolios import data_nodes, simple_tables
from mainsequence.markets.portfolios.data_nodes import storage_initialization
from mainsequence.markets.portfolios.portfolio_nodes import PortfolioStrategy
from mainsequence.tdag.data_nodes import DataNode, DataNodeConfiguration, RecordDefinition


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


class _DemoSignalConfig(DataNodeConfiguration):
    lookback: int
    display_name: str | None = Field(
        default=None,
        json_schema_extra={"runtime_only": True},
    )


class _DemoSignal(DataNode):
    def __init__(
        self,
        signal_config: _DemoSignalConfig,
        *args,
        **kwargs,
    ):
        self.signal_config = signal_config
        super().__init__(config=signal_config, *args, **kwargs)

    def dependencies(self):
        return {}

    def get_explanation(self):
        return f"{self.__class__.__name__}: demo signal"

    def update(self):
        return pd.DataFrame()


class _OtherDemoSignal(_DemoSignal):
    pass


class _DemoPortfolioConfig(DataNodeConfiguration):
    name: str
    lookback: int
    display_label: str | None = Field(
        default=None,
        json_schema_extra={"runtime_only": True},
    )


class _FakeSignalMetadataUpdater:
    def __init__(self, rows=None):
        self.upserted = []
        self.filter_calls = []
        self.rows = rows or []

    def upsert(self, record):
        self.upserted.append(record)
        return record

    def execute_filter(self, filter_expr, *, limit=50, offset=0):
        self.filter_calls.append({"filter": filter_expr, "limit": limit, "offset": offset})
        return self.rows


class _DemoRebalanceStrategy:
    def __init__(self, *, lookback: int = 20, description: str | None = None):
        self.lookback = lookback
        self.description = description

    def model_dump(self):
        return {
            "lookback": self.lookback,
            "description": self.description,
        }

    def get_explanation(self):
        return f"{self.__class__.__name__}: demo rebalance strategy"


class _FakeCanonicalSignalWeightsNode:
    def __init__(self, frame):
        self.frame = frame
        self.calls = []
        self.update_statistics = SimpleNamespace(index_progress=None)

    def get_df_between_dates(self, **kwargs):
        self.calls.append(kwargs)
        return self.frame


class _InterpolatingSignal(data_nodes.SignalWeights):
    @property
    def logger(self):
        return SimpleNamespace(
            warning=lambda *args, **kwargs: None,
            info=lambda *args, **kwargs: None,
        )

    @property
    def signal_uid(self):
        return "signal-hash"

    def maximum_forward_fill(self):
        return pd.Timedelta("3D")

    def get_df_between_dates(self, **kwargs):
        raise AssertionError("canonical interpolation should not read legacy signal storage")


def test_portfolio_weights_is_canonical_data_node_contract():
    assert issubclass(data_nodes.PortfolioWeights, DataNode)

    frame = data_nodes.PortfolioWeights.build_schema_bootstrap_frame(
        index_values={
            data_nodes.PORTFOLIO_INDEX_ASSET_UNIQUE_IDENTIFIER: "portfolio:alpha",
            data_nodes.ASSET_UNIQUE_IDENTIFIER: "asset:btc",
        }
    )

    assert list(frame.index.names) == [
        "time_index",
        "portfolio_index_asset_unique_identifier",
        "unique_identifier",
    ]
    assert "extra_details" not in data_nodes.PORTFOLIO_WEIGHTS_COLUMN_DTYPES_MAP
    assert set(data_nodes.PORTFOLIO_WEIGHTS_COLUMN_DTYPES_MAP).issubset(
        set(frame.reset_index().columns)
    )
    assert frame.attrs[LOGICAL_COLUMN_DTYPES_ATTR] == data_nodes.PORTFOLIO_WEIGHTS_COLUMN_DTYPES_MAP
    flat = frame.reset_index()
    assert str(flat[data_nodes.PORTFOLIO_INDEX_ASSET_UNIQUE_IDENTIFIER].dtype) == "string"
    assert str(flat[data_nodes.ASSET_UNIQUE_IDENTIFIER].dtype) == "string"

    (
        _serialized_frame,
        index_names,
        column_dtypes_map,
        time_index_name,
    ) = DataNodeUpdate._break_pandas_dataframe(frame)
    assert time_index_name == "time_index"
    assert list(index_names) == data_nodes.PORTFOLIO_WEIGHTS_INDEX_NAMES
    assert column_dtypes_map == data_nodes.PORTFOLIO_WEIGHTS_COLUMN_DTYPES_MAP


def test_signal_weights_is_canonical_data_node_contract():
    assert issubclass(data_nodes.SignalWeights, DataNode)

    frame = data_nodes.SignalWeights.build_schema_bootstrap_frame(
        index_values={
            data_nodes.SIGNAL_UID: "signal-hash",
            data_nodes.ASSET_UNIQUE_IDENTIFIER: "asset:eth",
        }
    )

    assert list(frame.index.names) == ["time_index", "signal_uid", "unique_identifier"]
    assert "extra_details" not in data_nodes.SIGNAL_WEIGHTS_COLUMN_DTYPES_MAP
    assert frame.attrs[LOGICAL_COLUMN_DTYPES_ATTR] == data_nodes.SIGNAL_WEIGHTS_COLUMN_DTYPES_MAP
    flat = frame.reset_index()
    assert str(flat[data_nodes.SIGNAL_UID].dtype) == "string"
    assert str(flat[data_nodes.ASSET_UNIQUE_IDENTIFIER].dtype) == "string"

    (
        _serialized_frame,
        index_names,
        column_dtypes_map,
        time_index_name,
    ) = DataNodeUpdate._break_pandas_dataframe(frame)
    assert time_index_name == "time_index"
    assert list(index_names) == data_nodes.SIGNAL_WEIGHTS_INDEX_NAMES
    assert column_dtypes_map == data_nodes.SIGNAL_WEIGHTS_COLUMN_DTYPES_MAP


def test_signal_uid_is_stable_for_identical_canonical_signal_configs(monkeypatch):
    _patch_data_node_source(monkeypatch)
    first_signal = _DemoSignal(_DemoSignalConfig(lookback=20, display_name="Research Label"))
    second_signal = _DemoSignal(_DemoSignalConfig(lookback=20, display_name="Production Label"))

    assert data_nodes.compute_signal_uid(first_signal) == data_nodes.compute_signal_uid(
        second_signal
    )
    assert data_nodes.SignalWeights.compute_signal_uid(first_signal) == (
        data_nodes.compute_signal_uid(first_signal)
    )


def test_signal_uid_changes_when_output_changing_signal_config_changes(monkeypatch):
    _patch_data_node_source(monkeypatch)
    first_signal = _DemoSignal(_DemoSignalConfig(lookback=20))
    second_signal = _DemoSignal(_DemoSignalConfig(lookback=40))

    assert data_nodes.compute_signal_uid(first_signal) != data_nodes.compute_signal_uid(
        second_signal
    )


def test_signal_uid_does_not_change_with_namespace_or_backend_identity(monkeypatch):
    _patch_data_node_source(monkeypatch)
    default_signal = _DemoSignal(_DemoSignalConfig(lookback=20))
    namespaced_signal = _DemoSignal(
        _DemoSignalConfig(lookback=20),
        hash_namespace="research",
    )

    assert default_signal.storage_hash != namespaced_signal.storage_hash
    assert data_nodes.compute_signal_uid(default_signal) == data_nodes.compute_signal_uid(
        namespaced_signal
    )
    canonical_payload = data_nodes.canonical_signal_configuration(namespaced_signal)
    assert "hash_namespace" not in canonical_payload
    assert "update_hash" not in canonical_payload
    assert "storage_hash" not in canonical_payload


def test_signal_uid_includes_concrete_signal_class_identity(monkeypatch):
    _patch_data_node_source(monkeypatch)
    first_signal = _DemoSignal(_DemoSignalConfig(lookback=20))
    second_signal = _OtherDemoSignal(_DemoSignalConfig(lookback=20))

    assert data_nodes.compute_signal_uid(first_signal) != data_nodes.compute_signal_uid(
        second_signal
    )


def test_signal_weights_runtime_signal_configuration_does_not_change_node_hash(
    monkeypatch,
):
    _patch_data_node_source(monkeypatch)
    first_signal = _DemoSignalConfig(lookback=20)
    second_signal = _DemoSignalConfig(lookback=40)

    first_node = data_nodes.SignalWeights(
        config=data_nodes.SignalWeights.default_config(signal_configuration=first_signal),
        namespace="research",
    )
    second_node = data_nodes.SignalWeights(
        config=data_nodes.SignalWeights.default_config(signal_configuration=second_signal),
        namespace="research",
    )

    assert first_node.storage_hash == second_node.storage_hash
    assert first_node.update_hash == second_node.update_hash
    assert first_node.signal_uid != second_node.signal_uid


def test_signal_uid_excludes_storage_identity_from_dict_payload():
    first_payload = {
        "time_series_class_import_path": {
            "module": "signals",
            "qualname": "MarketCap",
        },
        "config": {"lookback": 20},
        "update_hash": "signal_update_a",
        "storage_hash": "signal_storage_a",
        "data_node_update_id": 1,
        "data_node_storage_id": 2,
        "portfolio_index_asset_unique_identifier": "portfolio-a",
        "signal_name": "Research label",
    }
    second_payload = {
        **first_payload,
        "update_hash": "signal_update_b",
        "storage_hash": "signal_storage_b",
        "data_node_update_id": 100,
        "data_node_storage_id": 200,
        "portfolio_index_asset_unique_identifier": "portfolio-b",
        "signal_name": "Production label",
    }

    assert data_nodes.compute_signal_uid(first_payload) == data_nodes.compute_signal_uid(
        second_payload
    )


def test_signal_uid_uses_tdag_hash_signature(monkeypatch):
    captured = {}

    def fake_hash_signature(payload):
        captured["payload"] = payload
        return "update-hash", "storage-hash"

    monkeypatch.setattr(
        data_nodes.build_operations,
        "hash_signature",
        fake_hash_signature,
    )

    signal_uid = data_nodes.compute_signal_uid(
        {
            "time_series_class_import_path": {
                "module": "signals",
                "qualname": "MarketCap",
            },
            "config": {"lookback": 20},
            "hash_namespace": "research",
        }
    )

    assert signal_uid == "storage-hash"
    assert "hash_namespace" not in captured["payload"]
    assert captured["payload"]["config"] == {"lookback": 20}


def test_signal_metadata_is_simple_table_with_unique_signal_uid():
    field_spec = simple_tables.SignalMetadata.field_spec(data_nodes.SIGNAL_UID)

    assert issubclass(simple_tables.SignalMetadata, simple_tables.SimpleTable)
    assert field_spec.index is not None
    assert field_spec.index.unique is True
    assert simple_tables.SignalMetadata.field_spec(data_nodes.SIGNAL_DESCRIPTION).nullable is True


def test_build_signal_metadata_uses_signal_uid_and_description(monkeypatch):
    _patch_data_node_source(monkeypatch)

    signal = _DemoSignal(_DemoSignalConfig(lookback=20))

    metadata = simple_tables.build_signal_metadata(signal)

    assert metadata.signal_uid == data_nodes.compute_signal_uid(signal)
    assert signal.__class__.__name__ in metadata.signal_description


def test_signal_description_does_not_change_signal_uid():
    first_payload = {
        "time_series_class_import_path": {
            "module": "signals",
            "qualname": "MarketCap",
        },
        "config": {"lookback": 20},
        "signal_description": "First description",
    }
    second_payload = {
        **first_payload,
        "signal_description": "Second description",
    }

    assert data_nodes.compute_signal_uid(first_payload) == data_nodes.compute_signal_uid(
        second_payload
    )


def test_upsert_and_get_signal_metadata_use_simple_table_updater():
    updater = _FakeSignalMetadataUpdater(
        rows=[
            simple_tables.SignalMetadata(
                signal_uid="signal-hash",
                signal_description="Market cap signal",
            )
        ]
    )

    upserted = simple_tables.upsert_signal_metadata(
        signal_uid="signal-hash",
        signal_description="Market cap signal",
        updater=updater,
    )
    fetched = simple_tables.get_signal_metadata("signal-hash", updater=updater)

    assert upserted.signal_uid == "signal-hash"
    assert upserted.signal_description == "Market cap signal"
    assert updater.upserted == [upserted]
    assert fetched.signal_description == "Market cap signal"
    assert updater.filter_calls[0]["limit"] == 1


def test_rebalance_strategy_metadata_is_simple_table_with_unique_uid():
    field_spec = simple_tables.RebalanceStrategyMetadata.field_spec(
        data_nodes.REBALANCE_STRATEGY_UID
    )

    assert issubclass(simple_tables.RebalanceStrategyMetadata, simple_tables.SimpleTable)
    assert field_spec.index is not None
    assert field_spec.index.unique is True
    assert (
        simple_tables.RebalanceStrategyMetadata.field_spec(
            data_nodes.REBALANCE_STRATEGY_DESCRIPTION
        ).nullable
        is True
    )


def test_build_rebalance_strategy_metadata_uses_uid_and_description():
    strategy = _DemoRebalanceStrategy(lookback=30)

    metadata = simple_tables.build_rebalance_strategy_metadata(strategy)

    assert metadata.rebalance_strategy_uid == simple_tables.compute_rebalance_strategy_uid(
        strategy
    )
    assert strategy.__class__.__name__ in metadata.rebalance_strategy_description


def test_rebalance_strategy_description_does_not_change_uid():
    first_payload = {
        "rebalance_strategy_class_import_path": {
            "module": "strategies",
            "qualname": "ImmediateSignal",
        },
        "config": {"calendar_key": "24/7"},
        "rebalance_strategy_description": "First description",
    }
    second_payload = {
        **first_payload,
        "rebalance_strategy_description": "Second description",
    }

    assert simple_tables.compute_rebalance_strategy_uid(
        first_payload
    ) == simple_tables.compute_rebalance_strategy_uid(second_payload)


def test_upsert_and_get_rebalance_strategy_metadata_use_simple_table_updater():
    updater = _FakeSignalMetadataUpdater(
        rows=[
            simple_tables.RebalanceStrategyMetadata(
                rebalance_strategy_uid="rebalance-hash",
                rebalance_strategy_description="Immediate rebalance",
            )
        ]
    )

    upserted = simple_tables.upsert_rebalance_strategy_metadata(
        rebalance_strategy_uid="rebalance-hash",
        rebalance_strategy_description="Immediate rebalance",
        updater=updater,
    )
    fetched = simple_tables.get_rebalance_strategy_metadata(
        "rebalance-hash",
        updater=updater,
    )

    assert upserted.rebalance_strategy_uid == "rebalance-hash"
    assert upserted.rebalance_strategy_description == "Immediate rebalance"
    assert updater.upserted == [upserted]
    assert fetched.rebalance_strategy_description == "Immediate rebalance"
    assert updater.filter_calls[0]["limit"] == 1


def test_portfolio_metadata_is_simple_table_with_unique_identifier():
    field_spec = simple_tables.PortfolioMetadata.field_spec(
        data_nodes.PORTFOLIO_METADATA_UNIQUE_IDENTIFIER
    )

    assert issubclass(simple_tables.PortfolioMetadata, simple_tables.SimpleTable)
    assert field_spec.index is not None
    assert field_spec.index.unique is True
    assert (
        simple_tables.PortfolioMetadata.field_spec(data_nodes.PORTFOLIO_DESCRIPTION).nullable
        is True
    )


def test_build_portfolio_metadata_uses_index_asset_identifier_and_description():
    portfolio_configuration = SimpleNamespace(
        portfolio_markets_configuration=SimpleNamespace(
            front_end_details=SimpleNamespace(description="Research portfolio")
        )
    )

    metadata = simple_tables.build_portfolio_metadata(
        SimpleNamespace(unique_identifier="portfolio-hash"),
        portfolio_configuration=portfolio_configuration,
    )

    assert metadata.unique_identifier == "portfolio-hash"
    assert metadata.description == "Research portfolio"


def test_upsert_and_get_portfolio_metadata_use_simple_table_updater():
    updater = _FakeSignalMetadataUpdater(
        rows=[
            simple_tables.PortfolioMetadata(
                unique_identifier="portfolio-hash",
                description="Research portfolio",
            )
        ]
    )

    upserted = simple_tables.upsert_portfolio_metadata(
        unique_identifier="portfolio-hash",
        description="Research portfolio",
        updater=updater,
    )
    fetched = simple_tables.get_portfolio_metadata("portfolio-hash", updater=updater)

    assert upserted.unique_identifier == "portfolio-hash"
    assert upserted.description == "Research portfolio"
    assert updater.upserted == [upserted]
    assert fetched.description == "Research portfolio"
    assert updater.filter_calls[0]["limit"] == 1


def test_backfill_portfolio_metadata_from_legacy_portfolios():
    updater = _FakeSignalMetadataUpdater()

    upserted = simple_tables.backfill_portfolio_metadata_from_legacy_portfolios(
        [
            {
                "index_asset": {"unique_identifier": "portfolio-hash"},
                "target_portfolio_about": {"description": "Legacy description"},
            },
            {
                "index_asset": {"unique_identifier": "missing-description"},
                "target_portfolio_about": {},
            },
        ],
        updater=updater,
    )

    assert len(upserted) == 1
    assert upserted[0].unique_identifier == "portfolio-hash"
    assert upserted[0].description == "Legacy description"
    assert updater.upserted == upserted


def test_portfolio_create_from_time_series_uses_metadata_not_about_payload(monkeypatch):
    import mainsequence.client.markets.models.accounts_and_portfolios as portfolio_models

    request_calls = []
    upsert_calls = []

    class _Response:
        status_code = 201

        def json(self):
            portfolio_index_asset = {
                "id": 2,
                "unique_identifier": "portfolio-hash",
                "reference_portfolio": 1,
            }
            return {
                "portfolio": {
                    "id": 1,
                    "data_node_update": {
                        "uid": "dnu-1",
                        "update_hash": "portfolio_update",
                        "build_configuration": {},
                        "data_node_storage": "storage",
                    },
                    "signal_data_node_update": None,
                    "backtest_table_price_column_name": "close",
                    "calendar": {
                        "id": 1,
                        "name": "NYSE",
                        "calendar_dates": None,
                    },
                    "index_asset": portfolio_index_asset,
                    "builds_from_target_weights": True,
                    "builds_from_target_positions": False,
                    "creation_date": None,
                },
                "portfolio_index_asset": portfolio_index_asset,
            }

    def _fake_make_request(**kwargs):
        request_calls.append(kwargs)
        return _Response()

    def _fake_upsert_portfolio_metadata(**kwargs):
        upsert_calls.append(kwargs)
        return simple_tables.PortfolioMetadata(
            unique_identifier=kwargs["portfolio_index_asset"].unique_identifier,
            description=kwargs["description"],
        )

    monkeypatch.setattr(portfolio_models, "make_request", _fake_make_request)
    monkeypatch.setattr(
        simple_tables,
        "upsert_portfolio_metadata",
        _fake_upsert_portfolio_metadata,
    )

    portfolio, index_asset = portfolio_models.Portfolio.create_from_time_series(
        portfolio_name="Research Portfolio",
        data_node_update_id=10,
        signal_data_node_update_id=None,
        calendar_name="NYSE",
        backtest_table_price_column_name="close",
        portfolio_description="Research description",
    )

    payload = request_calls[0]["payload"]["json"]
    assert "target_portfolio_about" not in payload
    assert "portfolio_description" not in payload
    assert portfolio.index_asset.unique_identifier == "portfolio-hash"
    assert index_asset.unique_identifier == "portfolio-hash"
    assert upsert_calls[0]["description"] == "Research description"
    assert upsert_calls[0]["portfolio_index_asset"].unique_identifier == "portfolio-hash"


def test_portfolio_metadata_read_helpers_use_index_asset_identifier():
    import mainsequence.client.markets.models.accounts_and_portfolios as portfolio_models

    updater = _FakeSignalMetadataUpdater(
        rows=[
            simple_tables.PortfolioMetadata(
                unique_identifier="portfolio-hash",
                description="Stored description",
            )
        ]
    )
    portfolio = portfolio_models.Portfolio(
        id=1,
        data_node_update={
            "uid": "dnu-1",
            "update_hash": "portfolio_update",
            "build_configuration": {},
            "data_node_storage": "storage",
        },
        signal_data_node_update=None,
        backtest_table_price_column_name="close",
        calendar={
            "id": 1,
            "name": "NYSE",
            "calendar_dates": None,
        },
        index_asset={
            "id": 2,
            "unique_identifier": "portfolio-hash",
            "reference_portfolio": 1,
        },
        builds_from_target_weights=True,
        builds_from_target_positions=False,
        creation_date=None,
    )

    assert portfolio.get_description(updater=updater) == "Stored description"
    upserted = portfolio.upsert_metadata(
        description="Updated description",
        updater=updater,
    )

    assert updater.filter_calls[0]["limit"] == 1
    assert updater.upserted == [upserted]
    assert upserted.unique_identifier == "portfolio-hash"
    assert upserted.description == "Updated description"


def test_signal_weights_update_upserts_metadata(monkeypatch):
    _patch_data_node_source(monkeypatch)
    updater = _FakeSignalMetadataUpdater()
    node = data_nodes.SignalWeights()
    signal_configuration = _DemoSignalConfig(lookback=20)
    signal_uid = data_nodes.compute_signal_uid(
        {
            "time_series_class_import_path": data_nodes._class_import_path(
                data_nodes.SignalWeights
            ),
            "config": signal_configuration,
        }
    )
    signal_frame = pd.DataFrame(
        {
            "time_index": ["2024-01-01T00:00:00Z"],
            "unique_identifier": ["asset:btc"],
            "signal_weight": [0.7],
        }
    )

    node.set_signal_weights_frame(
        signal_frame,
        signal_configuration=signal_configuration,
        signal_description="Research signal",
        metadata_updater=updater,
    )

    frame = node.update()

    flat = frame.reset_index()
    assert flat.loc[0, "signal_uid"] == signal_uid
    assert updater.upserted[0] == simple_tables.SignalMetadata(
        signal_uid=signal_uid,
        signal_description="Research signal",
    )


def test_signal_weights_interpolate_index_can_read_canonical_signal_weights():
    canonical_frame = pd.DataFrame(
        {
            "time_index": [
                "2024-01-01T00:00:00Z",
                "2024-01-02T00:00:00Z",
            ],
            "signal_uid": ["signal-hash", "signal-hash"],
            "unique_identifier": ["asset:btc", "asset:btc"],
            "signal_weight": [0.6, 0.7],
        }
    ).set_index(["time_index", "signal_uid", "unique_identifier"])
    canonical_node = _FakeCanonicalSignalWeightsNode(canonical_frame)
    signal = object.__new__(_InterpolatingSignal)
    signal.update_statistics = SimpleNamespace(index_progress=None)

    interpolated = signal.interpolate_index(
        pd.DatetimeIndex(
            pd.to_datetime(
                ["2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z"],
                utc=True,
            ),
            name="time_index",
        ),
        signal_weights_node=canonical_node,
    )

    assert canonical_node.calls[0]["dimension_filters"] == {"signal_uid": ["signal-hash"]}
    assert list(interpolated.columns) == ["asset:btc"]
    assert interpolated.loc[pd.Timestamp("2024-01-02T00:00:00Z"), "asset:btc"] == 0.7


def test_portfolios_data_node_is_canonical_data_node_contract():
    assert issubclass(data_nodes.PortfoliosDataNode, DataNode)

    frame = data_nodes.PortfoliosDataNode.build_schema_bootstrap_frame(
        index_values={
            data_nodes.PORTFOLIO_INDEX_ASSET_UNIQUE_IDENTIFIER: "portfolio:alpha",
        }
    )

    assert list(frame.index.names) == [
        "time_index",
        "portfolio_index_asset_unique_identifier",
    ]
    assert "extra_details" not in data_nodes.PORTFOLIOS_COLUMN_DTYPES_MAP
    assert frame.attrs[LOGICAL_COLUMN_DTYPES_ATTR] == data_nodes.PORTFOLIOS_COLUMN_DTYPES_MAP
    assert (
        str(frame.reset_index()[data_nodes.PORTFOLIO_INDEX_ASSET_UNIQUE_IDENTIFIER].dtype)
        == "string"
    )
    assert str(frame.reset_index()["close_time"].dtype) == "datetime64[ns, UTC]"

    (
        _serialized_frame,
        index_names,
        column_dtypes_map,
        time_index_name,
    ) = DataNodeUpdate._break_pandas_dataframe(frame)
    assert time_index_name == "time_index"
    assert list(index_names) == data_nodes.PORTFOLIOS_INDEX_NAMES
    assert column_dtypes_map == data_nodes.PORTFOLIOS_COLUMN_DTYPES_MAP


def test_portfolio_weights_validation_rejects_wrong_index_order():
    frame = data_nodes.PortfolioWeights.build_mock_frame()
    frame = frame.reset_index().set_index(
        ["unique_identifier", "time_index", "portfolio_index_asset_unique_identifier"]
    )

    with pytest.raises(ValueError, match="index_names"):
        data_nodes.PortfolioWeights.validate_frame(frame)


def test_portfolio_weights_validation_rejects_missing_required_column():
    frame = data_nodes.PortfolioWeights.build_mock_frame()
    frame = frame.drop(columns=["price_current"])

    with pytest.raises(ValueError, match="missing required columns"):
        data_nodes.PortfolioWeights.validate_frame(frame)


def test_portfolio_weights_validation_rejects_duplicate_index_tuples():
    flat = data_nodes.PortfolioWeights.build_mock_frame().reset_index()
    duplicate_frame = pd.concat([flat, flat], ignore_index=True)

    with pytest.raises(ValueError, match="duplicate rows"):
        data_nodes.PortfolioWeights.validate_frame(duplicate_frame)


def test_portfolio_weights_validation_normalizes_float_and_time_dtypes():
    raw_frame = data_nodes.PortfolioWeights.build_mock_frame().reset_index()
    raw_frame["time_index"] = ["2024-01-01T00:00:00Z"]
    raw_frame["weight"] = ["0.25"]

    validated_frame = data_nodes.PortfolioWeights.validate_frame(raw_frame)

    flat = validated_frame.reset_index()
    assert str(flat["time_index"].dtype) == "datetime64[ns, UTC]"
    assert str(flat["weight"].dtype) == "float64"
    assert flat.loc[0, "weight"] == 0.25


def test_portfolio_configuration_hash_uses_tdag_hash_signature(monkeypatch):
    captured = {}

    def fake_hash_signature(payload):
        captured["payload"] = payload
        return "update-hash", "portfolio-storage-hash"

    monkeypatch.setattr(
        data_nodes.build_operations,
        "hash_signature",
        fake_hash_signature,
    )

    portfolio_hash = data_nodes.compute_portfolio_configuration_hash(
        {
            "portfolio_configuration": _DemoPortfolioConfig(
                name="Top 100",
                lookback=20,
                display_label="Research label",
            ),
            "hash_namespace": "research",
            "data_node_storage_id": 1,
        }
    )

    assert portfolio_hash == "portfolio-storage-hash"
    assert "hash_namespace" not in captured["payload"]
    assert "data_node_storage_id" not in captured["payload"]
    assert "portfolio_configuration" in captured["payload"]


def test_portfolio_configuration_runtime_metadata_does_not_change_hash(monkeypatch):
    _patch_data_node_source(monkeypatch)

    first_hash = data_nodes.compute_portfolio_configuration_hash(
        _DemoPortfolioConfig(
            name="Top 100",
            lookback=20,
            display_label="Research label",
        )
    )
    second_hash = data_nodes.compute_portfolio_configuration_hash(
        _DemoPortfolioConfig(
            name="Top 100",
            lookback=20,
            display_label="Production label",
        )
    )

    assert first_hash == second_hash


def test_get_or_create_portfolio_index_asset_uses_configuration_hash(monkeypatch):
    captured = {}

    class FakePortfolioResolver:
        @staticmethod
        def get_or_create_from_configuration_hash(**kwargs):
            captured.update(kwargs)
            return (
                SimpleNamespace(id=10),
                SimpleNamespace(unique_identifier="portfolio-hash"),
            )

    portfolio, index_asset = data_nodes.get_or_create_portfolio_index_asset(
        _DemoPortfolioConfig(name="Top 100", lookback=20),
        portfolio_configuration_hash="portfolio-hash",
        portfolio_resolver=FakePortfolioResolver,
    )

    assert portfolio.id == 10
    assert index_asset.unique_identifier == "portfolio-hash"
    assert captured["portfolio_configuration_hash"] == "portfolio-hash"
    assert "portfolio_configuration" in captured


def test_normalize_portfolio_weights_frame_maps_execution_columns():
    raw_frame = pd.DataFrame(
        {
            "time_index": ["2024-01-01T00:00:00Z"],
            "unique_identifier": ["asset:btc"],
            "weights_current": [0.6],
            "weights_before": [0.4],
            "price_current": [100.0],
            "price_before": [90.0],
            "volume_current": [10.0],
            "volume_before": [8.0],
        }
    )

    frame = data_nodes.normalize_portfolio_weights_frame(
        raw_frame,
        portfolio_index_asset_unique_identifier="portfolio-hash",
    )

    flat = frame.reset_index()
    assert list(frame.index.names) == data_nodes.PORTFOLIO_WEIGHTS_INDEX_NAMES
    assert flat.loc[0, "portfolio_index_asset_unique_identifier"] == "portfolio-hash"
    assert flat.loc[0, "weight"] == 0.6
    assert flat.loc[0, "weight_before"] == 0.4


def test_portfolio_weights_update_calls_calculate_weights(monkeypatch):
    _patch_data_node_source(monkeypatch)

    class TestPortfolioWeights(data_nodes.PortfolioWeights):
        def __init__(self, *args, **kwargs):
            self.called = False
            super().__init__(*args, **kwargs)

        def _calculate_weights(self):
            self.called = True
            return pd.DataFrame(
                {
                    "time_index": ["2024-01-01T00:00:00Z"],
                    "portfolio_index_asset_unique_identifier": ["portfolio-hash"],
                    "unique_identifier": ["asset:btc"],
                    "weight": [0.6],
                    "weight_before": [0.4],
                    "price_current": [100.0],
                    "price_before": [90.0],
                    "volume_current": [10.0],
                    "volume_before": [8.0],
                }
            )

    node = TestPortfolioWeights()
    frame = node.update()

    assert node.called is True
    assert frame.reset_index().loc[0, "weight"] == 0.6


def test_portfolio_weights_set_weights_frame_uses_explicit_portfolio_identity(
    monkeypatch,
):
    _patch_data_node_source(monkeypatch)
    node = data_nodes.PortfolioWeights()
    raw_frame = pd.DataFrame(
        {
            "time_index": ["2024-01-01T00:00:00Z"],
            "unique_identifier": ["asset:btc"],
            "weights_current": [0.6],
            "weights_before": [0.4],
            "price_current": [100.0],
            "price_before": [90.0],
            "volume_current": [10.0],
            "volume_before": [8.0],
        }
    )

    node.set_weights_frame(
        raw_frame,
        portfolio_index_asset_unique_identifier="portfolio-hash",
    )

    frame = node.update()

    assert frame.reset_index().loc[0, "portfolio_index_asset_unique_identifier"] == "portfolio-hash"


def test_portfolio_weights_update_upserts_portfolio_metadata(monkeypatch):
    _patch_data_node_source(monkeypatch)
    updater = _FakeSignalMetadataUpdater()
    node = data_nodes.PortfolioWeights()
    raw_frame = pd.DataFrame(
        {
            "time_index": ["2024-01-01T00:00:00Z"],
            "unique_identifier": ["asset:btc"],
            "weights_current": [0.6],
            "weights_before": [0.4],
            "price_current": [100.0],
            "price_before": [90.0],
            "volume_current": [10.0],
            "volume_before": [8.0],
        }
    )

    node.set_weights_frame(
        raw_frame,
        portfolio_index_asset_unique_identifier="portfolio-hash",
        portfolio_description="Research portfolio",
        metadata_updater=updater,
    )

    node.update()

    assert updater.upserted == [
        simple_tables.PortfolioMetadata(
            unique_identifier="portfolio-hash",
            description="Research portfolio",
        )
    ]


def test_portfolio_weights_allows_same_asset_timestamp_for_different_portfolios():
    frame = pd.DataFrame(
        {
            "time_index": [
                "2024-01-01T00:00:00Z",
                "2024-01-01T00:00:00Z",
            ],
            "portfolio_index_asset_unique_identifier": [
                "portfolio-a",
                "portfolio-b",
            ],
            "unique_identifier": ["asset:btc", "asset:btc"],
            "weight": [0.6, 0.2],
            "weight_before": [0.4, 0.1],
            "price_current": [100.0, 100.0],
            "price_before": [90.0, 90.0],
            "volume_current": [10.0, 10.0],
            "volume_before": [8.0, 8.0],
        }
    )

    validated = data_nodes.PortfolioWeights.validate_frame(frame)

    assert len(validated) == 2


def test_normalize_portfolio_values_frame_defaults_calculated_close_and_close_time():
    raw_frame = pd.DataFrame(
        {
            "close": [1.1],
            "return": [0.1],
        },
        index=pd.DatetimeIndex(pd.to_datetime(["2024-01-01T00:00:00Z"], utc=True)),
    )

    frame = data_nodes.normalize_portfolio_values_frame(
        raw_frame,
        portfolio_index_asset_unique_identifier="portfolio-hash",
    )

    flat = frame.reset_index()
    assert list(frame.index.names) == data_nodes.PORTFOLIOS_INDEX_NAMES
    assert flat.loc[0, "portfolio_index_asset_unique_identifier"] == "portfolio-hash"
    assert flat.loc[0, "calculated_close"] == 1.1
    assert str(flat["close_time"].dtype) == "datetime64[ns, UTC]"


def test_portfolios_data_node_update_calls_calculate_portfolio_values(monkeypatch):
    _patch_data_node_source(monkeypatch)

    class TestPortfoliosDataNode(data_nodes.PortfoliosDataNode):
        def __init__(self, *args, **kwargs):
            self.called = False
            super().__init__(*args, **kwargs)

        def _calculate_portfolio_values(self):
            self.called = True
            return pd.DataFrame(
                {
                    "time_index": ["2024-01-01T00:00:00Z"],
                    "portfolio_index_asset_unique_identifier": ["portfolio-hash"],
                    "close": [1.1],
                    "return": [0.1],
                    "calculated_close": [1.1],
                    "close_time": ["2024-01-01T00:00:00Z"],
                }
            )

    node = TestPortfoliosDataNode()
    frame = node.update()

    assert node.called is True
    assert frame.reset_index().loc[0, "close"] == 1.1


def test_portfolios_data_node_set_values_frame_uses_explicit_portfolio_identity(
    monkeypatch,
):
    _patch_data_node_source(monkeypatch)
    node = data_nodes.PortfoliosDataNode()
    raw_frame = pd.DataFrame(
        {
            "time_index": ["2024-01-01T00:00:00Z"],
            "close": [1.1],
            "return": [0.1],
            "calculated_close": [1.1],
            "close_time": ["2024-01-01T00:00:00Z"],
        }
    )

    node.set_portfolio_values_frame(
        raw_frame,
        portfolio_index_asset_unique_identifier="portfolio-hash",
    )

    frame = node.update()

    assert frame.reset_index().loc[0, "portfolio_index_asset_unique_identifier"] == "portfolio-hash"


def test_portfolio_strategy_run_writes_only_canonical_nodes(monkeypatch):
    _patch_data_node_source(monkeypatch)
    dependency_runs = []
    canonical_runs = []

    def fail_legacy_data_node_run(self, *args, **kwargs):
        raise AssertionError("PortfolioStrategy.run must not call DataNode.run on itself")

    monkeypatch.setattr(DataNode, "run", fail_legacy_data_node_run)
    monkeypatch.setattr(
        data_nodes.PortfolioWeights,
        "ensure_storage_ready",
        lambda self: "portfolio-weights-storage",
    )
    monkeypatch.setattr(
        data_nodes.PortfoliosDataNode,
        "ensure_storage_ready",
        lambda self: "portfolio-values-storage",
    )
    monkeypatch.setattr(
        data_nodes.PortfoliosDataNode,
        "get_update_statistics",
        lambda self: SimpleNamespace(max_time_index_value=None, index_progress=None),
    )

    def capture_portfolio_weights_run(self, **kwargs):
        canonical_runs.append(("weights", self, kwargs))
        return ("weights", self._weights_frame)

    def capture_portfolio_values_run(self, **kwargs):
        canonical_runs.append(("values", self, kwargs))
        return ("values", self._portfolio_values_frame)

    monkeypatch.setattr(data_nodes.PortfolioWeights, "run", capture_portfolio_weights_run)
    monkeypatch.setattr(data_nodes.PortfoliosDataNode, "run", capture_portfolio_values_run)

    strategy = object.__new__(PortfolioStrategy)
    strategy._hash_namespace = "research"
    strategy.portfolio_strategy_config = _DemoPortfolioConfig(name="Top 100", lookback=20)
    strategy.portfolio_markets_config = SimpleNamespace(
        front_end_details=SimpleNamespace(description="Research portfolio")
    )
    strategy.signal_weights = SimpleNamespace(
        run=lambda **kwargs: dependency_runs.append(("signal", kwargs))
    )
    strategy.bars_ts = SimpleNamespace(
        run=lambda **kwargs: dependency_runs.append(("bars", kwargs))
    )
    strategy._resolve_portfolio_identity = lambda: (
        SimpleNamespace(id=1),
        SimpleNamespace(unique_identifier="portfolio-hash"),
    )
    strategy.update_statistics = None

    weights_frame = pd.DataFrame(
        {
            "time_index": ["2024-01-01T00:00:00Z"],
            "unique_identifier": ["asset:btc"],
            "weights_current": [0.6],
            "weights_before": [0.4],
            "price_current": [100.0],
            "price_before": [90.0],
            "volume_current": [10.0],
            "volume_before": [8.0],
        }
    )
    portfolio_values_frame = pd.DataFrame(
        {
            "close": [1.1],
            "return": [0.1],
        },
        index=pd.DatetimeIndex(pd.to_datetime(["2024-01-01T00:00:00Z"], utc=True)),
    )

    def calculate_canonical_outputs():
        strategy._last_canonical_weights_frame = weights_frame
        strategy._last_canonical_portfolio_values_frame = portfolio_values_frame
        return portfolio_values_frame

    strategy.update = calculate_canonical_outputs

    results = strategy.run(debug_mode=True, update_tree=True, force_update=True)

    assert [run[0] for run in dependency_runs] == ["signal", "bars"]
    assert [run[0] for run in canonical_runs] == ["weights", "values"]
    assert set(results) == {"portfolio_weights", "portfolio_values"}

    weights_node = canonical_runs[0][1]
    values_node = canonical_runs[1][1]
    assert weights_node._portfolio_index_asset_unique_identifier == "portfolio-hash"
    assert values_node._portfolio_index_asset_unique_identifier == "portfolio-hash"
    assert "rebalance_weights" not in values_node._portfolio_values_frame.columns
    assert not hasattr(PortfolioStrategy, "_add_serialized_weights")


def test_canonical_config_supports_explicit_extra_records_without_extra_details():
    config = data_nodes.PortfolioWeights.default_config(
        identifier="research.portfolio_weights",
        extra_records=[
            RecordDefinition(
                column_name="execution_broker",
                dtype="string",
                description="Execution broker code.",
            )
        ],
    )

    assert config.node_metadata.identifier == "research.portfolio_weights"
    assert "time_index_name" not in config.model_dump()
    assert "extra_details" not in config.column_dtypes_map
    assert config.column_dtypes_map["execution_broker"] == "string"

    frame = data_nodes.PortfolioWeights.build_schema_bootstrap_frame(config=config)

    assert frame.reset_index().loc[0, "execution_broker"] == ""
    assert frame.attrs[LOGICAL_COLUMN_DTYPES_ATTR]["execution_broker"] == "string"


def test_canonical_config_rejects_required_record_dtype_override():
    with pytest.raises(ValueError, match="required dtype"):
        data_nodes.SignalWeights.default_config(
            extra_records=[
                RecordDefinition(
                    column_name="signal_weight",
                    dtype="string",
                )
            ],
        )


def test_canonical_config_rejects_duplicate_records():
    records = data_nodes.SignalWeights.default_config().records
    duplicate_config = data_nodes.VFBCanonicalDataNodeConfiguration(
        index_names=list(data_nodes.SIGNAL_WEIGHTS_INDEX_NAMES),
        records=records + [records[-1]],
    )

    with pytest.raises(ValueError, match="duplicate-free"):
        data_nodes.SignalWeights(config=duplicate_config)


def test_canonical_config_does_not_accept_time_index_name_api():
    with pytest.raises(ValidationError, match="time_index_name"):
        data_nodes.VFBCanonicalDataNodeConfiguration(
            time_index_name="custom_time",
            index_names=list(data_nodes.SIGNAL_WEIGHTS_INDEX_NAMES),
            records=data_nodes.SignalWeights.default_config().records,
        )


def test_canonical_namespace_alias_maps_to_hash_namespace(monkeypatch):
    _patch_data_node_source(monkeypatch)

    namespaced_node = data_nodes.PortfolioWeights(namespace="research")
    hash_namespaced_node = data_nodes.PortfolioWeights(hash_namespace="research")
    default_node = data_nodes.PortfolioWeights()
    explicit_none_node = data_nodes.PortfolioWeights(namespace=None)

    assert namespaced_node.hash_namespace == "research"
    assert namespaced_node.storage_hash == hash_namespaced_node.storage_hash
    assert namespaced_node.update_hash == hash_namespaced_node.update_hash
    assert namespaced_node.storage_hash != default_node.storage_hash
    assert default_node.storage_hash == explicit_none_node.storage_hash
    assert "namespace" not in namespaced_node.build_configuration
    assert namespaced_node.build_configuration["hash_namespace"] == "research"
    assert "hash_namespace" not in default_node.build_configuration


def test_canonical_namespace_rejects_hash_namespace_conflict(monkeypatch):
    _patch_data_node_source(monkeypatch)

    with pytest.raises(ValueError, match="hash_namespace and namespace"):
        data_nodes.PortfolioWeights(
            namespace="research",
            hash_namespace="production",
        )


def test_canonical_namespace_applies_to_all_vfb_canonical_nodes(monkeypatch):
    _patch_data_node_source(monkeypatch)

    portfolio_weights = data_nodes.PortfolioWeights(namespace="research")
    signal_weights = data_nodes.SignalWeights(namespace="research")
    portfolios = data_nodes.PortfoliosDataNode(namespace="research")

    assert portfolio_weights.hash_namespace == "research"
    assert signal_weights.hash_namespace == "research"
    assert portfolios.hash_namespace == "research"
    assert portfolio_weights.config.index_names == data_nodes.PORTFOLIO_WEIGHTS_INDEX_NAMES
    assert signal_weights.config.index_names == data_nodes.SIGNAL_WEIGHTS_INDEX_NAMES
    assert portfolios.config.index_names == data_nodes.PORTFOLIOS_INDEX_NAMES
    assert (
        portfolio_weights.config.column_dtypes_map == data_nodes.PORTFOLIO_WEIGHTS_COLUMN_DTYPES_MAP
    )
    assert signal_weights.config.column_dtypes_map == data_nodes.SIGNAL_WEIGHTS_COLUMN_DTYPES_MAP
    assert portfolios.config.column_dtypes_map == data_nodes.PORTFOLIOS_COLUMN_DTYPES_MAP


def test_canonical_namespace_isolates_storage_without_changing_contract(monkeypatch):
    _patch_data_node_source(monkeypatch)

    research_node = data_nodes.PortfolioWeights(namespace="research")
    production_node = data_nodes.PortfolioWeights(namespace="production")

    assert research_node.storage_hash != production_node.storage_hash
    assert research_node.update_hash != production_node.update_hash
    assert research_node.config.index_names == production_node.config.index_names
    assert research_node.config.column_dtypes_map == production_node.config.column_dtypes_map


def test_canonical_namespace_never_becomes_row_column_or_portfolio_identity(
    monkeypatch,
):
    _patch_data_node_source(monkeypatch)
    node = data_nodes.PortfolioWeights(namespace="research")

    frame = node.build_schema_bootstrap_frame(
        index_values={
            data_nodes.PORTFOLIO_INDEX_ASSET_UNIQUE_IDENTIFIER: "portfolio:alpha",
            data_nodes.ASSET_UNIQUE_IDENTIFIER: "asset:btc",
        }
    )
    flat = frame.reset_index()

    assert "namespace" not in flat.columns
    assert flat.loc[0, data_nodes.PORTFOLIO_INDEX_ASSET_UNIQUE_IDENTIFIER] == "portfolio:alpha"


def test_canonical_storage_readiness_accepts_ready_contract(monkeypatch):
    storage = SimpleNamespace(
        uid="portfolio-weights-uid",
        sourcetableconfiguration=SimpleNamespace(
            time_index_name=data_nodes.VFB_CANONICAL_TIME_INDEX_NAME,
            index_names=list(data_nodes.PORTFOLIO_WEIGHTS_INDEX_NAMES),
            column_dtypes_map=dict(data_nodes.PORTFOLIO_WEIGHTS_COLUMN_DTYPES_MAP),
        ),
    )
    monkeypatch.setattr(
        data_nodes.PortfolioWeights,
        "data_node_storage",
        property(lambda self: storage),
    )
    monkeypatch.setattr(
        data_nodes.PortfolioWeights,
        "run",
        lambda self, **kwargs: pytest.fail("ready storage should not bootstrap"),
    )
    node = object.__new__(data_nodes.PortfolioWeights)

    assert node.ensure_storage_ready() == "portfolio-weights-uid"


def test_canonical_storage_readiness_rejects_wrong_contract(monkeypatch):
    storage = SimpleNamespace(
        uid="portfolio-weights-uid",
        sourcetableconfiguration=SimpleNamespace(
            time_index_name=data_nodes.VFB_CANONICAL_TIME_INDEX_NAME,
            index_names=["time_index", "unique_identifier"],
            column_dtypes_map={
                "time_index": "datetime64[ns, UTC]",
                "unique_identifier": "string",
            },
        ),
    )
    monkeypatch.setattr(
        data_nodes.PortfolioWeights,
        "data_node_storage",
        property(lambda self: storage),
    )
    node = object.__new__(data_nodes.PortfolioWeights)

    with pytest.raises(ValueError, match="incompatible canonical VFB data node"):
        node.ensure_storage_ready()


def test_canonical_storage_readiness_initializes_storage_family_when_config_missing(
    monkeypatch,
):
    calls: list[dict] = []
    storage = SimpleNamespace(uid="portfolio-weights-uid", sourcetableconfiguration=None)

    def initialize_storage_family(**kwargs):
        calls.append(kwargs)
        storage.sourcetableconfiguration = SimpleNamespace(
            time_index_name=data_nodes.VFB_CANONICAL_TIME_INDEX_NAME,
            index_names=list(data_nodes.PORTFOLIO_WEIGHTS_INDEX_NAMES),
            column_dtypes_map=dict(data_nodes.PORTFOLIO_WEIGHTS_COLUMN_DTYPES_MAP),
        )
        return {}

    monkeypatch.setattr(
        storage_initialization,
        "initialize_portfolio_storage_source_tables",
        initialize_storage_family,
    )
    monkeypatch.setattr(
        data_nodes.PortfolioWeights,
        "data_node_storage",
        property(lambda self: storage),
    )
    monkeypatch.setattr(
        data_nodes.PortfolioWeights,
        "run",
        lambda self, **kwargs: pytest.fail("storage family initialization should not bootstrap"),
    )
    node = object.__new__(data_nodes.PortfolioWeights)

    assert node.ensure_storage_ready() == "portfolio-weights-uid"
    assert calls == [{"anchor_node": node}]


def test_canonical_storage_readiness_does_not_bootstrap_when_family_endpoint_missing(
    monkeypatch,
):
    class MissingEndpoint(Exception):
        status_code = 404

    storage = SimpleNamespace(uid="portfolio-weights-uid", sourcetableconfiguration=None)

    def initialize_storage_family(**kwargs):
        raise MissingEndpoint()

    monkeypatch.setattr(
        storage_initialization,
        "initialize_portfolio_storage_source_tables",
        initialize_storage_family,
    )
    monkeypatch.setattr(
        data_nodes.PortfolioWeights,
        "data_node_storage",
        property(lambda self: storage),
    )
    monkeypatch.setattr(
        data_nodes.PortfolioWeights,
        "run",
        lambda self, **kwargs: pytest.fail("storage family initialization should not bootstrap"),
    )
    node = object.__new__(data_nodes.PortfolioWeights)

    with pytest.raises(MissingEndpoint):
        node.ensure_storage_ready()


def test_canonical_storage_readiness_uses_active_config_with_extra_records(
    monkeypatch,
):
    config = data_nodes.PortfolioWeights.default_config(
        extra_records=[
            RecordDefinition(
                column_name="execution_venue",
                dtype="string",
                description="Execution venue used by the canonical writer.",
            )
        ]
    )
    captured: dict[str, dict] = {}
    storage = SimpleNamespace(uid="portfolio-weights-uid", sourcetableconfiguration=None)

    def initialize_storage_family(**kwargs):
        captured.update(kwargs)
        storage.sourcetableconfiguration = SimpleNamespace(
            time_index_name=config.time_index_name,
            index_names=list(config.index_names),
            column_dtypes_map=dict(config.column_dtypes_map),
        )
        return {}

    monkeypatch.setattr(
        storage_initialization,
        "initialize_portfolio_storage_source_tables",
        initialize_storage_family,
    )
    monkeypatch.setattr(
        data_nodes.PortfolioWeights,
        "data_node_storage",
        property(lambda self: storage),
    )
    monkeypatch.setattr(
        data_nodes.PortfolioWeights,
        "run",
        lambda self, **kwargs: pytest.fail("storage family initialization should not bootstrap"),
    )
    node = object.__new__(data_nodes.PortfolioWeights)
    node.config = config

    assert node.ensure_storage_ready() == "portfolio-weights-uid"
    assert captured == {"anchor_node": node}


def test_canonical_storage_readiness_rejects_initialized_schema_missing_extra_record(
    monkeypatch,
):
    config = data_nodes.PortfolioWeights.default_config(
        extra_records=[
            RecordDefinition(
                column_name="execution_venue",
                dtype="string",
            )
        ]
    )
    storage = SimpleNamespace(uid="portfolio-weights-uid", sourcetableconfiguration=None)

    def initialize_storage_family(**kwargs):
        storage.sourcetableconfiguration = SimpleNamespace(
            time_index_name=config.time_index_name,
            index_names=list(config.index_names),
            column_dtypes_map=dict(data_nodes.PORTFOLIO_WEIGHTS_COLUMN_DTYPES_MAP),
        )
        return {}

    monkeypatch.setattr(
        storage_initialization,
        "initialize_portfolio_storage_source_tables",
        initialize_storage_family,
    )
    monkeypatch.setattr(
        data_nodes.PortfolioWeights,
        "data_node_storage",
        property(lambda self: storage),
    )
    node = object.__new__(data_nodes.PortfolioWeights)
    node.config = config

    with pytest.raises(ValueError, match="execution_venue"):
        node.ensure_storage_ready()


def test_initialize_portfolio_storage_source_tables_builds_one_payload_and_validates_extra_records(
    monkeypatch,
):
    extra_record = RecordDefinition(column_name="execution_venue", dtype="string")
    portfolio_weights_config = data_nodes.PortfolioWeights.default_config(
        extra_records=[extra_record],
    )
    storages = {
        data_nodes.PortfolioWeights: SimpleNamespace(
            id=11,
            uid="portfolio-weights-uid",
            sourcetableconfiguration=None,
        ),
        data_nodes.SignalWeights: SimpleNamespace(
            id=22,
            uid="signal-weights-uid",
            sourcetableconfiguration=None,
        ),
        data_nodes.PortfoliosDataNode: SimpleNamespace(
            id=33,
            uid="portfolio-data-uid",
            sourcetableconfiguration=None,
        ),
    }
    for node_cls, storage in storages.items():
        monkeypatch.setattr(
            node_cls,
            "data_node_storage",
            property(lambda self, storage=storage: storage),
        )

    captured: dict[str, Any] = {}

    def initialize_bulk_source_tables(**kwargs):
        captured.update(kwargs)
        return {
            "source_table_configurations": {
                "portfolio_weights": {
                    "time_index_name": data_nodes.VFB_CANONICAL_TIME_INDEX_NAME,
                    "index_names": list(portfolio_weights_config.index_names),
                    "column_dtypes_map": dict(portfolio_weights_config.column_dtypes_map),
                },
                "signal_weights": {
                    "time_index_name": data_nodes.VFB_CANONICAL_TIME_INDEX_NAME,
                    "index_names": list(data_nodes.SIGNAL_WEIGHTS_INDEX_NAMES),
                    "column_dtypes_map": dict(data_nodes.SIGNAL_WEIGHTS_COLUMN_DTYPES_MAP),
                },
                "portfolio_data": {
                    "time_index_name": data_nodes.VFB_CANONICAL_TIME_INDEX_NAME,
                    "index_names": list(data_nodes.PORTFOLIOS_INDEX_NAMES),
                    "column_dtypes_map": dict(data_nodes.PORTFOLIOS_COLUMN_DTYPES_MAP),
                },
            }
        }

    monkeypatch.setattr(
        DataNodeStorage,
        "initialize_portfolio_storage_source_tables",
        staticmethod(initialize_bulk_source_tables),
    )
    portfolio_weights = object.__new__(data_nodes.PortfolioWeights)
    portfolio_weights.config = portfolio_weights_config
    signal_weights = object.__new__(data_nodes.SignalWeights)
    portfolio_data = object.__new__(data_nodes.PortfoliosDataNode)

    storage_initialization.initialize_portfolio_storage_source_tables(
        portfolio_weights=portfolio_weights,
        signal_weights=signal_weights,
        portfolio_data=portfolio_data,
        timeout=30,
    )

    assert captured["timeout"] == 30
    assert captured["portfolio_weights"] == {
        "dynamic_table_metadata_uid": "portfolio-weights-uid",
        "time_index_name": data_nodes.VFB_CANONICAL_TIME_INDEX_NAME,
        "index_names": portfolio_weights_config.index_names,
        "column_dtypes_map": portfolio_weights_config.column_dtypes_map,
    }
    assert captured["signal_weights"]["dynamic_table_metadata_uid"] == "signal-weights-uid"
    assert captured["portfolio_data"]["dynamic_table_metadata_uid"] == "portfolio-data-uid"
    assert (
        storages[data_nodes.PortfolioWeights]
        .sourcetableconfiguration
        .column_dtypes_map["execution_venue"]
        == "string"
    )
