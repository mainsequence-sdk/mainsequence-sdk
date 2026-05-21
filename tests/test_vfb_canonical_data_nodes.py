from __future__ import annotations

import os
from types import SimpleNamespace

import pandas as pd
import pytest
from pydantic import ValidationError

os.environ.setdefault("MAINSEQUENCE_ACCESS_TOKEN", "test-token")
os.environ.setdefault("MAINSEQUENCE_REFRESH_TOKEN", "test-refresh-token")

from mainsequence.client.models_tdag import LOGICAL_COLUMN_DTYPES_ATTR, DataNodeUpdate
from mainsequence.markets.virtualfundbuilder import data_nodes
from mainsequence.tdag.data_nodes import DataNode, RecordDefinition


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
    assert (
        frame.attrs[LOGICAL_COLUMN_DTYPES_ATTR]
        == data_nodes.PORTFOLIO_WEIGHTS_COLUMN_DTYPES_MAP
    )
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
    assert (
        frame.attrs[LOGICAL_COLUMN_DTYPES_ATTR]
        == data_nodes.SIGNAL_WEIGHTS_COLUMN_DTYPES_MAP
    )
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
    assert (
        frame.attrs[LOGICAL_COLUMN_DTYPES_ATTR]
        == data_nodes.PORTFOLIOS_COLUMN_DTYPES_MAP
    )
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
        portfolio_weights.config.column_dtypes_map
        == data_nodes.PORTFOLIO_WEIGHTS_COLUMN_DTYPES_MAP
    )
    assert (
        signal_weights.config.column_dtypes_map
        == data_nodes.SIGNAL_WEIGHTS_COLUMN_DTYPES_MAP
    )
    assert portfolios.config.column_dtypes_map == data_nodes.PORTFOLIOS_COLUMN_DTYPES_MAP


def test_canonical_namespace_isolates_storage_without_changing_contract(monkeypatch):
    _patch_data_node_source(monkeypatch)

    research_node = data_nodes.PortfolioWeights(namespace="research")
    production_node = data_nodes.PortfolioWeights(namespace="production")

    assert research_node.storage_hash != production_node.storage_hash
    assert research_node.update_hash != production_node.update_hash
    assert research_node.config.index_names == production_node.config.index_names
    assert (
        research_node.config.column_dtypes_map
        == production_node.config.column_dtypes_map
    )


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
    assert (
        flat.loc[0, data_nodes.PORTFOLIO_INDEX_ASSET_UNIQUE_IDENTIFIER]
        == "portfolio:alpha"
    )


def test_canonical_storage_readiness_accepts_ready_contract(monkeypatch):
    storage = SimpleNamespace(
        id=77,
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

    assert node.ensure_storage_ready() == 77


def test_canonical_storage_readiness_rejects_wrong_contract(monkeypatch):
    storage = SimpleNamespace(
        id=77,
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


@pytest.mark.parametrize(
    ("node_cls", "index_names", "column_dtypes_map", "initializer_name"),
    [
        (
            data_nodes.PortfolioWeights,
            data_nodes.PORTFOLIO_WEIGHTS_INDEX_NAMES,
            data_nodes.PORTFOLIO_WEIGHTS_COLUMN_DTYPES_MAP,
            "initialize_portfolio_weights_source_table",
        ),
        (
            data_nodes.SignalWeights,
            data_nodes.SIGNAL_WEIGHTS_INDEX_NAMES,
            data_nodes.SIGNAL_WEIGHTS_COLUMN_DTYPES_MAP,
            "initialize_signal_weights_source_table",
        ),
        (
            data_nodes.PortfoliosDataNode,
            data_nodes.PORTFOLIOS_INDEX_NAMES,
            data_nodes.PORTFOLIOS_COLUMN_DTYPES_MAP,
            "initialize_portfolios_source_table",
        ),
    ],
)
def test_canonical_storage_readiness_initializes_domain_schema_when_config_missing(
    monkeypatch,
    node_cls,
    index_names,
    column_dtypes_map,
    initializer_name,
):
    calls: list[dict] = []
    storage = SimpleNamespace(id=77, sourcetableconfiguration=None)

    def initialize_source_table(**kwargs):
        calls.append(kwargs)
        storage.sourcetableconfiguration = SimpleNamespace(
            time_index_name=data_nodes.VFB_CANONICAL_TIME_INDEX_NAME,
            index_names=list(index_names),
            column_dtypes_map=dict(column_dtypes_map),
        )

    setattr(storage, initializer_name, initialize_source_table)
    monkeypatch.setattr(
        node_cls,
        "data_node_storage",
        property(lambda self: storage),
    )
    monkeypatch.setattr(
        node_cls,
        "run",
        lambda self, **kwargs: pytest.fail(
            "domain schema initialization should not run bootstrap"
        ),
    )
    node = object.__new__(node_cls)

    assert node.ensure_storage_ready() == 77
    assert calls == [
        {
            "time_index_name": data_nodes.VFB_CANONICAL_TIME_INDEX_NAME,
            "index_names": index_names,
            "column_dtypes_map": column_dtypes_map,
        }
    ]


def test_canonical_storage_readiness_falls_back_when_domain_endpoint_missing(
    monkeypatch,
):
    class MissingEndpoint(Exception):
        status_code = 404

    calls: list[dict] = []
    storage = SimpleNamespace(id=77, sourcetableconfiguration=None)

    def initialize_portfolio_weights_source_table(**kwargs):
        raise MissingEndpoint()

    def fake_run(self, **kwargs):
        calls.append(kwargs)
        storage.sourcetableconfiguration = SimpleNamespace(
            time_index_name=data_nodes.VFB_CANONICAL_TIME_INDEX_NAME,
            index_names=list(data_nodes.PORTFOLIO_WEIGHTS_INDEX_NAMES),
            column_dtypes_map=dict(data_nodes.PORTFOLIO_WEIGHTS_COLUMN_DTYPES_MAP),
        )

    storage.initialize_portfolio_weights_source_table = (
        initialize_portfolio_weights_source_table
    )
    monkeypatch.setattr(
        data_nodes.PortfolioWeights,
        "data_node_storage",
        property(lambda self: storage),
    )
    monkeypatch.setattr(data_nodes.PortfolioWeights, "run", fake_run)
    node = object.__new__(data_nodes.PortfolioWeights)

    assert node.ensure_storage_ready() == 77
    assert calls == [
        {
            "debug_mode": True,
            "update_tree": False,
            "force_update": True,
        }
    ]


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
    calls: list[dict] = []
    storage = SimpleNamespace(id=77, sourcetableconfiguration=None)

    def initialize_portfolio_weights_source_table(**kwargs):
        calls.append(kwargs)
        storage.sourcetableconfiguration = SimpleNamespace(
            time_index_name=config.time_index_name,
            index_names=list(config.index_names),
            column_dtypes_map=dict(config.column_dtypes_map),
        )

    storage.initialize_portfolio_weights_source_table = (
        initialize_portfolio_weights_source_table
    )
    monkeypatch.setattr(
        data_nodes.PortfolioWeights,
        "data_node_storage",
        property(lambda self: storage),
    )
    monkeypatch.setattr(
        data_nodes.PortfolioWeights,
        "run",
        lambda self, **kwargs: pytest.fail(
            "domain schema initialization should not run bootstrap"
        ),
    )
    node = object.__new__(data_nodes.PortfolioWeights)
    node.config = config

    assert node.ensure_storage_ready() == 77
    assert calls == [
        {
            "time_index_name": data_nodes.VFB_CANONICAL_TIME_INDEX_NAME,
            "index_names": config.index_names,
            "column_dtypes_map": config.column_dtypes_map,
        }
    ]


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
    storage = SimpleNamespace(id=77, sourcetableconfiguration=None)

    def initialize_portfolio_weights_source_table(**kwargs):
        storage.sourcetableconfiguration = SimpleNamespace(
            time_index_name=config.time_index_name,
            index_names=list(config.index_names),
            column_dtypes_map=dict(data_nodes.PORTFOLIO_WEIGHTS_COLUMN_DTYPES_MAP),
        )

    storage.initialize_portfolio_weights_source_table = (
        initialize_portfolio_weights_source_table
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
