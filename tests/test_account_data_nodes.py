from __future__ import annotations

import datetime as dt
import os
from decimal import Decimal
from types import SimpleNamespace
from uuid import UUID

import pandas as pd
import pytest

os.environ.setdefault("MAINSEQUENCE_ACCESS_TOKEN", "test-token")
os.environ.setdefault("MAINSEQUENCE_REFRESH_TOKEN", "test-refresh-token")

from mainsequence.client.models_tdag import LOGICAL_COLUMN_DTYPES_ATTR, DataNodeUpdate
from mainsequence.markets.accounts import data_nodes
from mainsequence.tdag.data_nodes import DataNode


def test_account_holdings_is_real_data_node_contract():
    assert issubclass(data_nodes.AccountHoldings, DataNode)

    frame = data_nodes.AccountHoldings.build_mock_account_frame(
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


def test_virtual_fund_holdings_is_real_data_node_contract():
    assert issubclass(data_nodes.VirtualFundHoldings, DataNode)

    frame = data_nodes.VirtualFundHoldings.build_mock_fund_frame(
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
        execution_venue="paper",
    )

    assert calls == [
        (
            "create",
            {
                "account_name": "Broker Account",
                "execution_venue": "paper",
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
        execution_venue="paper",
    )

    assert account.holdings_data_source == 42
    assert calls == [
        (
            "get_or_create",
            {
                "create_without_holdings": True,
                "account_name": "Broker Account",
                "execution_venue": "paper",
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

    account = node.get_or_create_account(account_name="Existing", execution_venue="paper")

    assert account.holdings_data_source == 99
    assert calls == [
        (
            "get_or_create",
            {
                "account_name": "Existing",
                "execution_venue": "paper",
                "holdings_data_source": 99,
            },
        ),
        ("patch", {"holdings_data_source": 99}),
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

    monkeypatch.setattr(data_nodes, "ClientAccount", FakeAccount)
    monkeypatch.setattr(DataNodeUpdate, "upsert_data_into_table", fail_upsert)
    monkeypatch.setattr(
        data_nodes.AccountHoldings,
        "holdings_data_source_id",
        lambda self: 77,
    )
    node = object.__new__(data_nodes.AccountHoldings)

    response = node.add_account_holdings(
        account=FakeAccount(),
        holdings_date=dt.datetime(2026, 5, 20, tzinfo=dt.UTC),
        positions=[{"unique_identifier": "asset-1", "quantity": Decimal("1")}],
    )

    assert response.kind == "holdings-response"
    assert calls == [
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
        "holdings_data_source_id",
        lambda self: 5,
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
