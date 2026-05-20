import datetime
import os

import pytest

os.environ.setdefault("MAINSEQUENCE_ACCESS_TOKEN", "test-token")
os.environ.setdefault("MAINSEQUENCE_REFRESH_TOKEN", "test-refresh")

from mainsequence.tdag.data_nodes import data_nodes


class _FakeAPIPersistManager:
    def __init__(self):
        self.calls = []

    def get_df_between_dates(self, **kwargs):
        self.calls.append(kwargs)
        return "ok"


def _mixin_with_fake_api_manager(monkeypatch):
    monkeypatch.setattr(data_nodes, "APIPersistManager", _FakeAPIPersistManager)
    manager = _FakeAPIPersistManager()
    mixin = data_nodes.DataAccessMixin()
    mixin.local_persist_manager = manager
    return mixin, manager


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
