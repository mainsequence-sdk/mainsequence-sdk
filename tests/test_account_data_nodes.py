from __future__ import annotations

import datetime as dt
import os
from decimal import Decimal
from types import SimpleNamespace
from uuid import UUID

import numpy as np
import pandas as pd
import pytest

os.environ.setdefault("MAINSEQUENCE_ACCESS_TOKEN", "test-token")
os.environ.setdefault("MAINSEQUENCE_REFRESH_TOKEN", "test-refresh-token")

from mainsequence.client.models_tdag import LOGICAL_COLUMN_DTYPES_ATTR, DataNodeUpdate
from mainsequence.markets.accounts import data_nodes
from mainsequence.tdag.data_nodes import DataNode
from mainsequence.tdag.data_nodes.run_operations import UpdateRunner


def test_account_holdings_is_real_data_node_contract():
    assert issubclass(data_nodes.AccountHoldings, DataNode)

    frame = data_nodes.AccountHoldings.build_schema_bootstrap_account_frame(
        account_uid=UUID("11111111-1111-1111-1111-111111111111"),
    )

    assert list(frame.index.names) == ["time_index", "account_uid", "unique_identifier"]
    assert set(data_nodes.ACCOUNT_HOLDINGS_COLUMN_DTYPES_MAP).issubset(
        set(frame.reset_index().columns)
    )
    assert (
        frame.attrs[LOGICAL_COLUMN_DTYPES_ATTR]
        == data_nodes.ACCOUNT_HOLDINGS_COLUMN_DTYPES_MAP
    )

    (
        _serialized_frame,
        index_names,
        column_dtypes_map,
        time_index_name,
    ) = DataNodeUpdate._break_pandas_dataframe(frame)
    assert time_index_name == "time_index"
    assert list(index_names) == ["time_index", "account_uid", "unique_identifier"]
    assert column_dtypes_map == data_nodes.ACCOUNT_HOLDINGS_COLUMN_DTYPES_MAP


def test_account_holdings_bootstrap_row_is_reserved_and_non_semantic():
    frame = data_nodes.AccountHoldings.build_initialization_frame()
    flat = frame.reset_index()

    assert flat.loc[0, "account_uid"] == str(data_nodes.SCHEMA_BOOTSTRAP_ACCOUNT_UID)
    assert flat.loc[0, "unique_identifier"] == data_nodes.SCHEMA_BOOTSTRAP_ROW_IDENTIFIER
    assert flat.loc[0, "holdings_set_uid"] == str(
        data_nodes.SCHEMA_BOOTSTRAP_HOLDINGS_SET_UID
    )
    assert flat.loc[0, "extra_details"] == {
        "_mainsequence_reserved": "schema_bootstrap",
        "semantic": False,
    }


def test_virtual_fund_holdings_is_real_data_node_contract():
    assert issubclass(data_nodes.VirtualFundHoldings, DataNode)

    frame = data_nodes.VirtualFundHoldings.build_schema_bootstrap_fund_frame(
        fund_uid=UUID("22222222-2222-2222-2222-222222222222"),
    )

    assert list(frame.index.names) == ["time_index", "fund_uid", "unique_identifier"]
    assert set(data_nodes.VIRTUAL_FUND_HOLDINGS_COLUMN_DTYPES_MAP).issubset(
        set(frame.reset_index().columns)
    )
    assert (
        frame.attrs[LOGICAL_COLUMN_DTYPES_ATTR]
        == data_nodes.VIRTUAL_FUND_HOLDINGS_COLUMN_DTYPES_MAP
    )

    (
        _serialized_frame,
        index_names,
        column_dtypes_map,
        time_index_name,
    ) = DataNodeUpdate._break_pandas_dataframe(frame)
    assert time_index_name == "time_index"
    assert list(index_names) == ["time_index", "fund_uid", "unique_identifier"]
    assert column_dtypes_map == data_nodes.VIRTUAL_FUND_HOLDINGS_COLUMN_DTYPES_MAP


def test_account_holdings_validation_accepts_extra_non_index_columns():
    raw_frame = data_nodes.AccountHoldings.build_mock_account_frame().reset_index()
    raw_frame["broker_note"] = "client-specific metadata"

    validated_frame = data_nodes.AccountHoldings.validate_holdings_frame(raw_frame)

    assert list(validated_frame.index.names) == [
        "time_index",
        "account_uid",
        "unique_identifier",
    ]
    assert "broker_note" in validated_frame.columns
    assert (
        validated_frame.attrs[LOGICAL_COLUMN_DTYPES_ATTR]
        == data_nodes.ACCOUNT_HOLDINGS_COLUMN_DTYPES_MAP
    )


def test_account_holdings_validation_rejects_wrong_index_order():
    frame = data_nodes.AccountHoldings.build_mock_account_frame()
    frame = frame.reset_index().set_index(
        ["account_uid", "time_index", "unique_identifier"]
    )

    with pytest.raises(ValueError, match="index_names"):
        data_nodes.AccountHoldings.validate_holdings_frame(frame)


def test_account_holdings_validation_rejects_missing_required_column():
    frame = data_nodes.AccountHoldings.build_mock_account_frame()
    frame = frame.drop(columns=["quantity"])

    with pytest.raises(ValueError, match="missing required columns"):
        data_nodes.AccountHoldings.validate_holdings_frame(frame)


def test_account_holdings_validation_forces_nanosecond_utc_time_index():
    raw_frame = data_nodes.AccountHoldings.build_mock_account_frame().reset_index()
    raw_frame["time_index"] = pd.DatetimeIndex(
        np.array(["1970-01-01T00:00:00.000000"], dtype="datetime64[us]"),
        tz="UTC",
    )

    validated_frame = data_nodes.AccountHoldings.validate_holdings_frame(raw_frame)

    assert str(validated_frame.index.get_level_values("time_index").dtype) == (
        "datetime64[ns, UTC]"
    )
    UpdateRunner.validate_data_frame(validated_frame, storage_class_type="timescale")


def test_users_can_extend_account_holdings_data_node():
    class BrokerAccountHoldings(data_nodes.AccountHoldings):
        @classmethod
        def source_frame(cls) -> pd.DataFrame:
            return cls.build_mock_account_frame(
                account_uid=UUID("33333333-3333-3333-3333-333333333333"),
            )

        def get_holdings_frame(self) -> pd.DataFrame:
            return self.source_frame()

    frame = BrokerAccountHoldings.source_frame()

    assert issubclass(BrokerAccountHoldings, data_nodes.AccountHoldings)
    assert list(frame.index.names) == ["time_index", "account_uid", "unique_identifier"]


def test_users_can_set_account_holdings_data_node_identifier():
    config = data_nodes.AccountHoldings.default_config(
        identifier="broker.accounts.holdings",
        description="Broker account holdings imported from daily files.",
    )

    assert config.node_metadata.identifier == "broker.accounts.holdings"
    assert config.node_metadata.description == (
        "Broker account holdings imported from daily files."
    )


def test_users_can_add_extra_account_holdings_columns_to_config():
    config = data_nodes.AccountHoldings.default_config(
        identifier="broker.account_holdings",
        extra_records=[
            data_nodes.RecordDefinition(
                column_name="broker_account_id",
                dtype="object",
                description="Broker account id from the upstream system.",
            )
        ],
    )

    assert config.index_names == ["time_index", "account_uid", "unique_identifier"]
    assert config.column_dtypes_map["broker_account_id"] == "object"
    assert (
        config.column_dtypes_map["unique_identifier"]
        == data_nodes.ACCOUNT_HOLDINGS_COLUMN_DTYPES_MAP["unique_identifier"]
    )
    frame = data_nodes.AccountHoldings.build_schema_bootstrap_account_frame(
        config=config,
        account_uid=UUID("44444444-4444-4444-4444-444444444444"),
    )
    assert frame.reset_index().loc[0, "broker_account_id"] == ""
    assert frame.attrs[LOGICAL_COLUMN_DTYPES_ATTR]["broker_account_id"] == "object"


def test_account_holdings_config_rejects_missing_required_records():
    bad_records = [
        record
        for record in data_nodes.AccountHoldings.default_config().records
        if record.column_name != "unique_identifier"
    ]
    bad_config = data_nodes.HoldingsDataNodeConfiguration(
        time_index_name=data_nodes.ACCOUNT_HOLDINGS_TIME_INDEX_NAME,
        index_names=list(data_nodes.ACCOUNT_HOLDINGS_INDEX_NAMES),
        records=bad_records,
    )

    with pytest.raises(ValueError, match="required columns"):
        data_nodes.AccountHoldings(config=bad_config)


def test_virtual_fund_holdings_requires_fund_specific_records():
    config = data_nodes.VirtualFundHoldings.default_config()

    assert config.index_names == ["time_index", "fund_uid", "unique_identifier"]
    assert config.column_dtypes_map["fund_uid"] == "uuid"
    assert config.column_dtypes_map["target_weight"] == "decimal"
    assert "account_uid" not in config.column_dtypes_map


def test_logical_dtype_contract_rejects_columns_missing_from_frame():
    frame = pd.DataFrame(
        [{"time_index": pd.Timestamp("2024-01-01", tz="UTC"), "value": 1}]
    ).set_index("time_index")
    frame.attrs[LOGICAL_COLUMN_DTYPES_ATTR] = {"missing_column": "uuid"}

    with pytest.raises(ValueError, match="not present in the DataFrame"):
        DataNodeUpdate._break_pandas_dataframe(frame)


def test_account_can_be_created_before_holdings_data_node(monkeypatch):
    calls: list[tuple[str, dict]] = []

    class FakeAccount:
        @classmethod
        def create(cls, timeout=None, **kwargs):
            calls.append(("create", kwargs))
            return cls()

    def fail_storage_lookup(self):
        raise AssertionError("storage should not be resolved")

    monkeypatch.setattr(data_nodes, "ClientAccount", FakeAccount)
    monkeypatch.setattr(
        data_nodes.AccountHoldings,
        "holdings_data_source_id",
        fail_storage_lookup,
    )
    node = object.__new__(data_nodes.AccountHoldings)

    node.create_account(
        bind_holdings_data_node=False,
        account_name="Broker Account",
    )

    assert calls == [
        (
            "create",
            {
                "account_name": "Broker Account",
            },
        )
    ]


def test_holdings_node_can_get_or_create_and_bind_account(monkeypatch):
    calls: list[tuple[str, dict]] = []

    class FakeAccount:
        def __init__(self, holdings_data_source=None):
            self.uid = "account-1"
            self.holdings_data_source = holdings_data_source

        @classmethod
        def get_or_create(cls, create_without_holdings=False, timeout=None, **kwargs):
            calls.append(
                (
                    "get_or_create",
                    {
                        "create_without_holdings": create_without_holdings,
                        **kwargs,
                    },
                )
            )
            return cls(holdings_data_source=kwargs.get("holdings_data_source"))

        def patch(self, timeout=None, **kwargs):
            calls.append(("patch", kwargs))
            self.holdings_data_source = kwargs["holdings_data_source"]
            return self

    monkeypatch.setattr(data_nodes, "ClientAccount", FakeAccount)
    monkeypatch.setattr(
        data_nodes.AccountHoldings,
        "holdings_data_source_id",
        lambda self: 42,
    )
    node = object.__new__(data_nodes.AccountHoldings)

    account = node.get_or_create_account(
        account_name="Broker Account",
    )

    assert account.holdings_data_source == 42
    assert calls == [
        (
            "get_or_create",
            {
                "create_without_holdings": True,
                "account_name": "Broker Account",
                "holdings_data_source": 42,
            },
        )
    ]


def test_existing_account_get_or_create_is_patched_to_current_holdings_node(monkeypatch):
    calls: list[tuple[str, dict]] = []

    class FakeAccount:
        def __init__(self):
            self.uid = "account-1"
            self.holdings_data_source = None

        @classmethod
        def get_or_create(cls, create_without_holdings=False, timeout=None, **kwargs):
            calls.append(("get_or_create", kwargs))
            return cls()

        def patch(self, timeout=None, **kwargs):
            calls.append(("patch", kwargs))
            self.holdings_data_source = kwargs["holdings_data_source"]
            return self

    monkeypatch.setattr(data_nodes, "ClientAccount", FakeAccount)
    monkeypatch.setattr(
        data_nodes.AccountHoldings,
        "holdings_data_source_id",
        lambda self: 99,
    )
    node = object.__new__(data_nodes.AccountHoldings)

    account = node.get_or_create_account(account_name="Existing")

    assert account.holdings_data_source == 99
    assert calls == [
        (
            "get_or_create",
            {
                "account_name": "Existing",
                "holdings_data_source": 99,
            },
        ),
        ("patch", {"holdings_data_source": 99}),
    ]


def _ready_account_holdings_storage(storage_id=77):
    return SimpleNamespace(
        id=storage_id,
        sourcetableconfiguration=SimpleNamespace(
            time_index_name=data_nodes.ACCOUNT_HOLDINGS_TIME_INDEX_NAME,
            index_names=list(data_nodes.ACCOUNT_HOLDINGS_INDEX_NAMES),
            column_dtypes_map=dict(data_nodes.ACCOUNT_HOLDINGS_COLUMN_DTYPES_MAP),
        ),
    )


def test_account_holdings_ensure_storage_ready_runs_bootstrap_when_storage_missing(
    monkeypatch,
):
    calls: list[dict] = []
    storages = [None]

    def fake_run(self, **kwargs):
        calls.append(kwargs)
        storages.append(_ready_account_holdings_storage(storage_id=77))

    monkeypatch.setattr(
        data_nodes.AccountHoldings,
        "data_node_storage",
        property(lambda self: storages[-1]),
    )
    monkeypatch.setattr(data_nodes.AccountHoldings, "run", fake_run)
    node = object.__new__(data_nodes.AccountHoldings)

    assert node.ensure_storage_ready() == 77
    assert calls == [
        {
            "debug_mode": True,
            "update_tree": False,
            "force_update": True,
        }
    ]


def test_account_holdings_ensure_storage_ready_initializes_schema_when_config_missing(
    monkeypatch,
):
    calls: list[dict] = []
    storage = SimpleNamespace(id=77, sourcetableconfiguration=None)

    def initialize_account_holdings_source_table(**kwargs):
        calls.append(kwargs)
        storage.sourcetableconfiguration = SimpleNamespace(
            time_index_name=data_nodes.ACCOUNT_HOLDINGS_TIME_INDEX_NAME,
            index_names=list(data_nodes.ACCOUNT_HOLDINGS_INDEX_NAMES),
            column_dtypes_map=dict(data_nodes.ACCOUNT_HOLDINGS_COLUMN_DTYPES_MAP),
        )

    storage.initialize_account_holdings_source_table = (
        initialize_account_holdings_source_table
    )
    monkeypatch.setattr(
        data_nodes.AccountHoldings,
        "data_node_storage",
        property(lambda self: storage),
    )
    monkeypatch.setattr(
        data_nodes.AccountHoldings,
        "run",
        lambda self, **kwargs: pytest.fail("schema initialization should not run bootstrap"),
    )
    node = object.__new__(data_nodes.AccountHoldings)

    assert node.ensure_storage_ready() == 77
    assert calls == [
        {
            "time_index_name": data_nodes.ACCOUNT_HOLDINGS_TIME_INDEX_NAME,
            "index_names": data_nodes.ACCOUNT_HOLDINGS_INDEX_NAMES,
            "column_dtypes_map": data_nodes.ACCOUNT_HOLDINGS_COLUMN_DTYPES_MAP,
        }
    ]


def test_account_holdings_ensure_storage_ready_falls_back_when_schema_endpoint_missing(
    monkeypatch,
):
    class MissingEndpoint(Exception):
        status_code = 404

    calls: list[dict] = []
    storage = SimpleNamespace(id=77, sourcetableconfiguration=None)

    def initialize_account_holdings_source_table(**kwargs):
        raise MissingEndpoint()

    def fake_run(self, **kwargs):
        calls.append(kwargs)
        storage.sourcetableconfiguration = SimpleNamespace(
            time_index_name=data_nodes.ACCOUNT_HOLDINGS_TIME_INDEX_NAME,
            index_names=list(data_nodes.ACCOUNT_HOLDINGS_INDEX_NAMES),
            column_dtypes_map=dict(data_nodes.ACCOUNT_HOLDINGS_COLUMN_DTYPES_MAP),
        )

    storage.initialize_account_holdings_source_table = (
        initialize_account_holdings_source_table
    )
    monkeypatch.setattr(
        data_nodes.AccountHoldings,
        "data_node_storage",
        property(lambda self: storage),
    )
    monkeypatch.setattr(data_nodes.AccountHoldings, "run", fake_run)
    node = object.__new__(data_nodes.AccountHoldings)

    assert node.ensure_storage_ready() == 77
    assert calls == [
        {
            "debug_mode": True,
            "update_tree": False,
            "force_update": True,
        }
    ]


def test_account_holdings_ensure_storage_ready_skips_bootstrap_when_contract_ready(
    monkeypatch,
):
    monkeypatch.setattr(
        data_nodes.AccountHoldings,
        "data_node_storage",
        property(lambda self: _ready_account_holdings_storage(storage_id=88)),
    )
    monkeypatch.setattr(
        data_nodes.AccountHoldings,
        "run",
        lambda self, **kwargs: pytest.fail("ready storage should not rerun"),
    )
    node = object.__new__(data_nodes.AccountHoldings)

    assert node.ensure_storage_ready() == 88


def test_account_holdings_ensure_storage_ready_rejects_wrong_contract(monkeypatch):
    bad_storage = SimpleNamespace(
        id=77,
        sourcetableconfiguration=SimpleNamespace(
            time_index_name="time_index",
            index_names=["time_index", "unique_identifier"],
            column_dtypes_map={
                "time_index": "datetime64[ns, UTC]",
                "unique_identifier": "object",
            },
        ),
    )
    monkeypatch.setattr(
        data_nodes.AccountHoldings,
        "data_node_storage",
        property(lambda self: bad_storage),
    )
    node = object.__new__(data_nodes.AccountHoldings)

    with pytest.raises(ValueError, match="incompatible holdings data node"):
        node.ensure_storage_ready()


def test_virtual_fund_holdings_ensure_storage_ready_initializes_schema_when_config_missing(
    monkeypatch,
):
    calls: list[dict] = []
    storage = SimpleNamespace(id=78, sourcetableconfiguration=None)

    def initialize_virtual_fund_holdings_source_table(**kwargs):
        calls.append(kwargs)
        storage.sourcetableconfiguration = SimpleNamespace(
            time_index_name=data_nodes.VIRTUAL_FUND_HOLDINGS_TIME_INDEX_NAME,
            index_names=list(data_nodes.VIRTUAL_FUND_HOLDINGS_INDEX_NAMES),
            column_dtypes_map=dict(data_nodes.VIRTUAL_FUND_HOLDINGS_COLUMN_DTYPES_MAP),
        )

    storage.initialize_virtual_fund_holdings_source_table = (
        initialize_virtual_fund_holdings_source_table
    )
    monkeypatch.setattr(
        data_nodes.VirtualFundHoldings,
        "data_node_storage",
        property(lambda self: storage),
    )
    monkeypatch.setattr(
        data_nodes.VirtualFundHoldings,
        "run",
        lambda self, **kwargs: pytest.fail("schema initialization should not run bootstrap"),
    )
    node = object.__new__(data_nodes.VirtualFundHoldings)

    assert node.ensure_storage_ready() == 78
    assert calls == [
        {
            "time_index_name": data_nodes.VIRTUAL_FUND_HOLDINGS_TIME_INDEX_NAME,
            "index_names": data_nodes.VIRTUAL_FUND_HOLDINGS_INDEX_NAMES,
            "column_dtypes_map": data_nodes.VIRTUAL_FUND_HOLDINGS_COLUMN_DTYPES_MAP,
        }
    ]


def test_account_holdings_facade_writes_through_drf_account_methods(monkeypatch):
    calls: list[tuple[str, dict]] = []

    class FakeAccount:
        def __init__(self):
            self.uid = "account-1"
            self.holdings_data_source = None

        def patch(self, timeout=None, **kwargs):
            calls.append(("patch", kwargs))
            self.holdings_data_source = kwargs["holdings_data_source"]
            return self

        def add_holdings(self, timeout=None, **kwargs):
            calls.append(("add_holdings", kwargs))
            return SimpleNamespace(kind="holdings-response")

    def fail_upsert(*args, **kwargs):
        raise AssertionError("facade must not write directly to the dynamic table")

    class FakeAsset:
        @classmethod
        def filter(cls, timeout=None, **kwargs):
            calls.append(("asset_filter", {"timeout": timeout, **kwargs}))
            return [SimpleNamespace(unique_identifier="asset-1")]

    monkeypatch.setattr(data_nodes, "ClientAccount", FakeAccount)
    monkeypatch.setattr(data_nodes, "Asset", FakeAsset)
    monkeypatch.setattr(DataNodeUpdate, "upsert_data_into_table", fail_upsert)
    monkeypatch.setattr(
        data_nodes.AccountHoldings,
        "ensure_storage_ready",
        lambda self: calls.append(("ensure_storage_ready", {})) or 77,
    )
    node = object.__new__(data_nodes.AccountHoldings)

    response = node.add_account_holdings(
        account=FakeAccount(),
        holdings_date=dt.datetime(2026, 5, 20, tzinfo=dt.UTC),
        positions=[{"unique_identifier": "asset-1", "quantity": Decimal("1")}],
    )

    assert response.kind == "holdings-response"
    assert calls == [
        (
            "asset_filter",
            {"timeout": None, "unique_identifier__in": ["asset-1"]},
        ),
        ("ensure_storage_ready", {}),
        ("patch", {"holdings_data_source": 77}),
        (
            "add_holdings",
            {
                "holdings_date": dt.datetime(2026, 5, 20, tzinfo=dt.UTC),
                "positions": [{"unique_identifier": "asset-1", "quantity": Decimal("1")}],
                "overwrite": False,
            },
        ),
    ]


def test_account_holdings_facade_rejects_unknown_asset_identifiers(monkeypatch):
    calls: list[tuple[str, dict]] = []

    class FakeAccount:
        def __init__(self):
            self.uid = "account-1"
            self.holdings_data_source = None

        def patch(self, timeout=None, **kwargs):
            calls.append(("patch", kwargs))
            return self

        def add_holdings(self, timeout=None, **kwargs):
            calls.append(("add_holdings", kwargs))
            return SimpleNamespace(kind="holdings-response")

    class FakeAsset:
        @classmethod
        def filter(cls, timeout=None, **kwargs):
            calls.append(("asset_filter", {"timeout": timeout, **kwargs}))
            return [SimpleNamespace(unique_identifier="asset-1")]

    monkeypatch.setattr(data_nodes, "ClientAccount", FakeAccount)
    monkeypatch.setattr(data_nodes, "Asset", FakeAsset)
    monkeypatch.setattr(
        data_nodes.AccountHoldings,
        "ensure_storage_ready",
        lambda self: calls.append(("ensure_storage_ready", {})) or 77,
    )
    node = object.__new__(data_nodes.AccountHoldings)

    with pytest.raises(ValueError, match="Missing unique_identifier values: missing"):
        node.add_account_holdings(
            account=FakeAccount(),
            holdings_date=dt.datetime(2026, 5, 20, tzinfo=dt.UTC),
            positions=[
                {"unique_identifier": "asset-1", "quantity": Decimal("1")},
                {"unique_identifier": "missing", "quantity": Decimal("1")},
            ],
        )

    assert calls == [
        (
            "asset_filter",
            {"timeout": None, "unique_identifier__in": ["asset-1", "missing"]},
        )
    ]


def test_account_holdings_facade_rejects_positions_without_identifier(monkeypatch):
    calls: list[tuple[str, dict]] = []

    class FakeAsset:
        @classmethod
        def filter(cls, timeout=None, **kwargs):
            calls.append(("asset_filter", {"timeout": timeout, **kwargs}))
            return []

    monkeypatch.setattr(data_nodes, "Asset", FakeAsset)
    monkeypatch.setattr(
        data_nodes.AccountHoldings,
        "ensure_storage_ready",
        lambda self: calls.append(("ensure_storage_ready", {})) or 77,
    )
    node = object.__new__(data_nodes.AccountHoldings)

    with pytest.raises(ValueError, match="must include unique_identifier"):
        node.add_account_holdings(
            account=SimpleNamespace(),
            holdings_date=dt.datetime(2026, 5, 20, tzinfo=dt.UTC),
            positions=[{"quantity": Decimal("1")}],
        )

    assert calls == []


def test_account_holdings_facade_target_positions_use_drf_account_methods(monkeypatch):
    calls: list[tuple[str, dict]] = []

    class FakeAccount:
        def __init__(self):
            self.uid = "account-1"
            self.holdings_data_source = 5

        def add_target_positions(self, timeout=None, **kwargs):
            calls.append(("add_target_positions", kwargs))
            return SimpleNamespace(kind="target-response")

    monkeypatch.setattr(data_nodes, "ClientAccount", FakeAccount)
    monkeypatch.setattr(
        data_nodes.AccountHoldings,
        "ensure_storage_ready",
        lambda self: calls.append(("ensure_storage_ready", {})) or 5,
    )
    node = object.__new__(data_nodes.AccountHoldings)

    response = node.add_account_target_positions(
        account=FakeAccount(),
        target_positions_date=dt.datetime(2026, 5, 20, tzinfo=dt.UTC),
        positions=[{"unique_identifier": "asset-1", "weight_notional_exposure": "1"}],
        overwrite=True,
    )

    assert response.kind == "target-response"
    assert calls == [
        ("ensure_storage_ready", {}),
        (
            "add_target_positions",
            {
                "target_positions_date": dt.datetime(2026, 5, 20, tzinfo=dt.UTC),
                "positions": [
                    {"unique_identifier": "asset-1", "weight_notional_exposure": "1"}
                ],
                "overwrite": True,
            },
        )
    ]


def test_account_holdings_facade_latest_read_uses_drf_account_method(monkeypatch):
    calls: list[tuple[str, dict]] = []

    class FakeAccount:
        def __init__(self):
            self.uid = "account-1"
            self.holdings_data_source = 7

        def get_latest_holdings(self, timeout=None, **kwargs):
            calls.append(("get_latest_holdings", kwargs))
            return SimpleNamespace(kind="latest")

    monkeypatch.setattr(data_nodes, "ClientAccount", FakeAccount)
    node = object.__new__(data_nodes.AccountHoldings)

    response = node.get_latest_account_holdings(
        account=FakeAccount(),
        include_asset_detail=True,
    )

    assert response.kind == "latest"
    assert calls == [("get_latest_holdings", {"include_asset_detail": True})]
