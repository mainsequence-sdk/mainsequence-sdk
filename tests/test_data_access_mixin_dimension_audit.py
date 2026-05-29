import inspect
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

os.environ.setdefault("MAINSEQUENCE_ACCESS_TOKEN", "test-token")
os.environ.setdefault("MAINSEQUENCE_REFRESH_TOKEN", "test-refresh")

from mainsequence.meta_tables.data_nodes import data_nodes
from mainsequence.meta_tables.data_nodes.models import DataNodeConfiguration


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


def test_api_data_node_constructor_requires_data_source_uid():
    with pytest.raises(ValueError, match="APIDataNode requires data_source_uid"):
        data_nodes.APIDataNode(data_source_uid="", storage_hash="prices_hash")


def test_api_persist_manager_requires_data_source_uid():
    with pytest.raises(ValueError, match="APIPersistManager requires data_source_uid"):
        data_nodes.APIPersistManager(storage_hash="prices_hash", data_source_uid="")


def test_api_data_node_build_from_identifier_uses_data_source_uid(monkeypatch):
    captured = {}
    storage_table = SimpleNamespace(
        storage_hash="prices_hash",
        data_source_uid="data-source-uid",
    )

    def fake_get(**kwargs):
        captured.update(kwargs)
        return storage_table

    monkeypatch.setattr(data_nodes.MetaTable, "get", staticmethod(fake_get))

    node = data_nodes.APIDataNode.build_from_identifier("prices")

    assert captured == {"identifier": "prices"}
    assert node.storage_hash == "prices_hash"
    assert node.data_source_uid == "data-source-uid"
    assert node.storage_table is storage_table
    assert not hasattr(node, "data_source_id")


def test_api_data_node_build_from_table_uid_uses_meta_table_uid(monkeypatch):
    captured = {}
    storage_table = SimpleNamespace(
        storage_hash="prices_hash",
        data_source_uid="data-source-uid",
    )

    def fake_get(**kwargs):
        captured.update(kwargs)
        return storage_table

    monkeypatch.setattr(data_nodes.MetaTable, "get", staticmethod(fake_get))

    node = data_nodes.APIDataNode.build_from_table_uid("table-uid")

    assert captured == {"uid": "table-uid"}
    assert node.storage_hash == "prices_hash"
    assert node.data_source_uid == "data-source-uid"
    assert node.storage_table is storage_table
    assert not hasattr(data_nodes.APIDataNode, "build_from_table_id")


def test_api_data_node_build_from_meta_table_falls_back_to_nested_data_source():
    storage_table = SimpleNamespace(
        storage_hash="prices_hash",
        data_source=SimpleNamespace(uid="data-source-uid"),
    )

    node = data_nodes.APIDataNode.build_from_meta_table(storage_table)

    assert node.storage_hash == "prices_hash"
    assert node.data_source_uid == "data-source-uid"
    assert node.storage_table is storage_table


def test_api_data_node_local_persist_manager_receives_only_data_source_uid(monkeypatch):
    captured = {}

    class FakeAPIPersistManager:
        def __init__(self, **kwargs):
            captured.update(kwargs)
            self.storage_table = object()

    monkeypatch.setattr(data_nodes.APIDataNode, "_verify_local_data_source", lambda self: None)
    monkeypatch.setattr(data_nodes, "APIPersistManager", FakeAPIPersistManager)
    node = data_nodes.APIDataNode(data_source_uid="data-source-uid", storage_hash="prices_hash")

    node._set_local_persist_manager()

    assert captured == {
        "storage_hash": "prices_hash",
        "data_source_uid": "data-source-uid",
    }


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_data_access_mixin_uses_explicit_dimension_filters(monkeypatch):
    mixin, manager = _mixin_with_fake_api_manager(monkeypatch)

    result = mixin.get_df_between_dates(
        dimension_filters={"entity_uid": ["BTC", "ETH"]},
    )

    assert result == "ok"
    assert manager.calls == [
        {
            "start_date": None,
            "end_date": None,
            "great_or_equal": True,
            "less_or_equal": True,
            "dimension_filters": {"entity_uid": ["BTC", "ETH"]},
            "index_coordinates": None,
            "dimension_range_map": None,
            "columns": None,
        }
    ]


def test_data_access_mixin_uses_explicit_dimension_range_map(monkeypatch):
    mixin, manager = _mixin_with_fake_api_manager(monkeypatch)

    dimension_range_map = [
        {"coordinate": {"entity_uid": "BTC"}, "start_date": "2026-05-01T00:00:00Z"}
    ]
    result = mixin.get_df_between_dates(dimension_range_map=dimension_range_map)

    assert result == "ok"
    assert manager.calls[0]["dimension_range_map"] == dimension_range_map


def test_data_access_mixin_rejects_removed_unique_identifier_aliases(monkeypatch):
    mixin, _manager = _mixin_with_fake_api_manager(monkeypatch)

    with pytest.raises(TypeError):
        mixin.get_df_between_dates(unique_identifier_list=["BTC"])

    with pytest.raises(TypeError):
        mixin.get_df_between_dates(unique_identifier_range_map={"BTC": {}})


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
    signature = inspect.signature(data_nodes.DataAccessMixin.get_df_between_dates)
    assert "unique_identifier_list" not in signature.parameters
    assert "unique_identifier_range_map" not in signature.parameters
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



def test_core_tdag_public_api_has_no_domain_asset_compatibility_vocabulary():
    forbidden = (
        "asset_list",
        "get_asset_list",
        "unique_identifier_list",
        "unique_identifier_range_map",
        "filter_by_assets_ranges",
    )
    paths = list((PROJECT_ROOT / "mainsequence" / "meta_tables").rglob("*.py"))
    paths.append(PROJECT_ROOT / "mainsequence" / "client" / "models_tdag.py")

    violations = []
    for path in paths:
        text = path.read_text()
        for token in forbidden:
            if token in text:
                violations.append(f"{path.relative_to(PROJECT_ROOT)} contains {token}")

    assert violations == []
