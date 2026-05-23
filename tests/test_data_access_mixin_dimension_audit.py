import datetime
import inspect
import os

import pytest

os.environ.setdefault("MAINSEQUENCE_ACCESS_TOKEN", "test-token")
os.environ.setdefault("MAINSEQUENCE_REFRESH_TOKEN", "test-refresh")

from mainsequence.tdag.data_nodes import data_nodes
from mainsequence.tdag.data_nodes.models import DataNodeConfiguration


def _clear_command_center_env(monkeypatch):
    monkeypatch.delenv("MAINSEQUENCE_COMMAND_CENTER_URL", raising=False)
    monkeypatch.delenv("COMMAND_CENTER_URL", raising=False)


class _FakeAPIPersistManager:
    def __init__(self):
        self.calls = []
        self.last_calls = []

    def get_df_between_dates(self, **kwargs):
        self.calls.append(kwargs)
        return "ok"

    def get_last_observation(self, **kwargs):
        self.last_calls.append(kwargs)
        return "latest"


def _mixin_with_fake_api_manager(monkeypatch):
    monkeypatch.setattr(data_nodes, "APIPersistManager", _FakeAPIPersistManager)
    manager = _FakeAPIPersistManager()
    mixin = data_nodes.DataAccessMixin()
    mixin.local_persist_manager = manager
    return mixin, manager


def test_data_node_repr_omits_detail_url_without_command_center_env(monkeypatch):
    _clear_command_center_env(monkeypatch)
    monkeypatch.setenv("MAINSEQUENCE_ENDPOINT", "http://127.0.0.1:8000")
    mixin = data_nodes.DataAccessMixin()

    assert repr(mixin) == "DataAccessMixin"


def test_data_node_repr_omits_detail_url_with_command_center_env(monkeypatch):
    _clear_command_center_env(monkeypatch)
    monkeypatch.setenv("MAINSEQUENCE_ENDPOINT", "http://127.0.0.1:8000")
    monkeypatch.setenv("COMMAND_CENTER_URL", "http://localhost:5173/")
    mixin = data_nodes.DataAccessMixin()

    assert repr(mixin) == "DataAccessMixin"


def test_api_data_node_repr_omits_detail_url_without_command_center_env(monkeypatch):
    _clear_command_center_env(monkeypatch)
    node = object.__new__(data_nodes.APIDataNode)

    assert repr(node) == "APIDataNode"


def test_api_data_node_repr_omits_detail_url_with_command_center_env(monkeypatch):
    _clear_command_center_env(monkeypatch)
    monkeypatch.setenv("MAINSEQUENCE_COMMAND_CENTER_URL", "http://localhost:5173")
    node = object.__new__(data_nodes.APIDataNode)

    assert repr(node) == "APIDataNode"


def test_data_access_mixin_translates_unique_identifier_list_for_api_manager(monkeypatch):
    mixin, manager = _mixin_with_fake_api_manager(monkeypatch)

    with pytest.warns(FutureWarning, match="unique_identifier_list"):
        result = mixin.get_df_between_dates(unique_identifier_list=["BTC", "ETH"])

    assert result == "ok"
    assert manager.calls == [
        {
            "start_date": None,
            "end_date": None,
            "unique_identifier_list": None,
            "great_or_equal": True,
            "less_or_equal": True,
            "unique_identifier_range_map": None,
            "dimension_filters": {"unique_identifier": ["BTC", "ETH"]},
            "index_coordinates": None,
            "dimension_range_map": None,
            "columns": None,
        }
    ]


def test_data_access_mixin_translates_unique_identifier_range_map_for_api_manager(monkeypatch):
    mixin, manager = _mixin_with_fake_api_manager(monkeypatch)
    start = datetime.datetime(2026, 5, 1, tzinfo=datetime.UTC)

    with pytest.warns(FutureWarning, match="unique_identifier_range_map"):
        result = mixin.get_df_between_dates(
            unique_identifier_range_map={"BTC": {"start_date": start}},
        )

    assert result == "ok"
    assert manager.calls[0]["unique_identifier_range_map"] is None
    assert manager.calls[0]["dimension_range_map"] == [
        {
            "coordinate": {"unique_identifier": "BTC"},
            "start_date": start,
        }
    ]


def test_data_access_mixin_latest_observation_uses_generic_dimensions(monkeypatch):
    mixin, manager = _mixin_with_fake_api_manager(monkeypatch)

    result = mixin.get_last_observation(
        dimension_filters={"account_uid": ["account-a"]},
    )

    assert result == "latest"
    assert manager.last_calls == [
        {
            "dimension_filters": {"account_uid": ["account-a"]},
            "index_coordinates": None,
            "dimension_range_map": None,
        }
    ]


def test_data_access_mixin_no_longer_exposes_asset_specific_helpers():
    assert "asset_list" not in inspect.signature(
        data_nodes.DataAccessMixin.get_last_observation
    ).parameters
    assert not hasattr(data_nodes.DataAccessMixin, "get_ranged_data_per_asset")
    assert not hasattr(data_nodes.DataAccessMixin, "get_ranged_data_per_asset_great_or_equal")
    assert not hasattr(data_nodes.DataAccessMixin, "filter_by_assets_ranges")
    assert "asset_list" not in DataNodeConfiguration.model_fields
    assert not hasattr(data_nodes.DataNode, "get_asset_list")
    assert not hasattr(data_nodes.APIDataNode, "get_earliest_updated_asset_filter")
    assert not hasattr(data_nodes, "get_latest_update_by_assets_filter")
    assert not hasattr(data_nodes, "last_update_per_unique_identifier")
