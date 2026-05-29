import datetime

import pandas as pd
import pytest
from pydantic import ValidationError

from mainsequence.client.models_metatables import UpdateStatistics
from mainsequence.meta_tables.data_nodes.data_nodes import DataNode


def _dt(hour: int) -> datetime.datetime:
    return datetime.datetime(2026, 5, 1, hour, tzinfo=datetime.UTC)


def test_update_statistics_one_index_global_progress_projects_max_time():
    stats = UpdateStatistics(
        global_index_progress={
            "max": "2026-05-01T03:00:00Z",
            "min": "2026-05-01T00:00:00Z",
        },
        index_progress={},
        index_min={},
        max_time_index_value="2025-01-01T00:00:00Z",
    )

    assert stats.global_index_progress == {
        "max": _dt(3),
        "min": _dt(0),
    }
    assert stats.index_progress == {}
    assert stats.index_min == {}
    assert stats.max_time_index_value == _dt(3)
    assert stats.get_max_time_in_update_statistics() == _dt(3)


def test_update_statistics_public_contract_is_generic():
    assert set(UpdateStatistics.model_fields) == {
        "global_index_progress",
        "index_progress",
        "index_min",
        "max_time_index_value",
        "limit_update_time",
        "multi_index_column_stats",
        "is_backfill",
    }

    with pytest.raises(ValidationError):
        UpdateStatistics(asset_list=[])

    with pytest.raises(ValidationError):
        UpdateStatistics(asset_time_statistics={})


def test_update_statistics_two_index_normalizes_progress():
    stats = UpdateStatistics(
        global_index_progress={
            "max": "2026-05-01 03:00:00+00:00",
            "min": "2026-05-01 00:00:00+00:00",
        },
        index_progress={
            "asset-1": "2026-05-01T02:00:00Z",
            "asset-2": datetime.datetime(2026, 5, 1, 1),
        },
        index_min={
            "asset-1": 1777593600,
            "asset-2": "2026-05-01T01:00:00Z",
        },
    )

    assert stats.index_progress == {
        "asset-1": _dt(2),
        "asset-2": _dt(1),
    }
    assert stats.index_min == {
        "asset-1": _dt(0),
        "asset-2": _dt(1),
    }
    assert stats.identity_values() == ["asset-1", "asset-2"]
    assert stats.get_last_update_for_identity("asset-1") == _dt(2)

    stats["asset-3"] = "2026-05-01T03:00:00Z"
    assert stats.index_progress["asset-3"] == _dt(3)


def test_update_statistics_missing_identity_has_no_last_update():
    stats = UpdateStatistics(index_progress={"asset-1": "2026-05-01T02:00:00Z"})
    stats._initial_fallback_date = _dt(0)

    assert stats.get_last_update_for_identity("asset-2") is None


def test_base_data_node_attaches_update_statistics_without_forcing_fallback_date():
    class ExampleNode(DataNode):
        OFFSET_START = _dt(0)

        def dependencies(self):
            return {}

        def update(self):
            return None

    node = object.__new__(ExampleNode)
    stats = UpdateStatistics(index_progress={})

    returned = node._set_update_statistics(stats)

    assert returned is stats
    assert node.update_statistics is stats
    assert stats._initial_fallback_date is None


def test_base_data_node_update_statistics_prepare_hook_can_set_fallback_date():
    class ExampleNode(DataNode):
        OFFSET_START = _dt(0)

        def prepare_update_statistics(self, update_statistics):
            update_statistics._initial_fallback_date = self.get_offset_start()
            return update_statistics

        def dependencies(self):
            return {}

        def update(self):
            return None

    node = object.__new__(ExampleNode)
    stats = UpdateStatistics(index_progress={})

    returned = node._set_update_statistics(stats)

    assert returned is stats
    assert node.update_statistics is stats
    assert stats._initial_fallback_date == _dt(0)


def test_update_statistics_builds_canonical_dimension_range_map_for_nested_progress():
    stats = UpdateStatistics(
        index_progress={
            "account-a": {"asset-1": "2026-05-01T02:00:00Z"},
            "account-b": {"asset-2": "2026-05-01T03:00:00Z"},
        },
    )

    with pytest.raises(TypeError):
        stats.get_dimension_range_map_great_or_equal()

    assert stats.get_dimension_range_map_great_or_equal(
        identity_dimensions=["account_uid", "unique_identifier"],
    ) == [
        {
            "coordinate": {"account_uid": "account-a", "unique_identifier": "asset-1"},
            "start_date_operand": ">=",
            "start_date": _dt(2),
        },
        {
            "coordinate": {"account_uid": "account-b", "unique_identifier": "asset-2"},
            "start_date_operand": ">=",
            "start_date": _dt(3),
        },
    ]


def test_update_statistics_scopes_by_generic_identity_values():
    stats = UpdateStatistics(
        max_time_index_value=_dt(3),
        index_progress={
            "account-a": "2026-05-01T02:00:00Z",
            "account-b": "2026-05-01T03:00:00Z",
        },
    )

    scoped = stats.update_identity_scope(
        identity_values=["account-a", "account-c"],
        init_fallback_date=_dt(0),
    )

    assert scoped.index_progress == {
        "account-a": _dt(2),
        "account-c": _dt(0),
    }
    assert not hasattr(scoped, "asset_list")
    assert scoped.get_max_time_in_update_statistics() == _dt(2)


def test_update_statistics_has_no_domain_asset_scoping_api():
    assert not hasattr(UpdateStatistics, "update_assets")
    assert not hasattr(UpdateStatistics, "asset_identifier")
    assert not hasattr(UpdateStatistics, "is_any_asset_on_fallback_date")
    assert not hasattr(UpdateStatistics, "are_all_assets_on_fallback_date")
    assert not hasattr(UpdateStatistics, "get_asset_earliest_multiindex_update")
    assert not hasattr(UpdateStatistics, "filter_assets_by_level")
    public_names = {name for name in dir(UpdateStatistics) if not name.startswith("_")}
    assert not any("asset" in name for name in public_names)
    assert not any("unique_identifier" in name for name in public_names)


def test_update_statistics_three_index_normalizes_nested_stats_and_filters_dataframe():
    stats = UpdateStatistics(
        global_index_progress={
            "max": "2026-05-01T03:00:00Z",
            "min": "2026-05-01T00:00:00Z",
        },
        index_progress={
            "account-a": {
                "asset-1": "2026-05-01T02:00:00Z",
            }
        },
        index_min={
            "account-a": {
                "asset-1": "2026-05-01T00:00:00Z",
            }
        },
        multi_index_column_stats={
            "value": {
                "account-a": {
                    "asset-1": {
                        "min": "2026-05-01T00:00:00Z",
                        "max": "2026-05-01T02:00:00Z",
                    }
                }
            }
        },
    )

    assert stats.index_progress == {"account-a": {"asset-1": _dt(2)}}
    assert stats.index_min == {"account-a": {"asset-1": _dt(0)}}
    assert stats.multi_index_column_stats == {
        "value": {
            "account-a": {
                "asset-1": {
                    "min": _dt(0),
                    "max": _dt(2),
                }
            }
        }
    }

    index = pd.MultiIndex.from_tuples(
        [
            (_dt(1), "account-a", "asset-1"),
            (_dt(3), "account-a", "asset-1"),
            (_dt(1), "account-b", "asset-1"),
        ],
        names=["time_index", "account_uid", "unique_identifier"],
    )
    df = pd.DataFrame({"value": [10, 30, 20]}, index=index)

    filtered = stats.filter_df_by_latest_value(df)

    assert filtered["value"].tolist() == [30, 20]
    assert list(filtered.index) == [
        (_dt(3), "account-a", "asset-1"),
        (_dt(1), "account-b", "asset-1"),
    ]
