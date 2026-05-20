import datetime

import pandas as pd
import pytest

from mainsequence.client.models_tdag import UpdateStatistics


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
    assert stats.asset_time_statistics == {}


def test_update_statistics_two_index_normalizes_progress_and_legacy_projection():
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
    assert stats.asset_time_statistics == stats.index_progress
    assert stats.asset_identifier() == ["asset-1", "asset-2"]
    assert stats.get_last_update_index_2d("asset-1") == _dt(2)

    stats["asset-3"] = "2026-05-01T03:00:00Z"
    assert stats.index_progress["asset-3"] == _dt(3)
    assert stats.asset_time_statistics == stats.index_progress


def test_update_statistics_legacy_asset_time_statistics_projects_to_index_progress():
    with pytest.warns(FutureWarning, match="asset_time_statistics"):
        stats = UpdateStatistics(
            asset_time_statistics={
                "asset-1": "2026-05-01T02:00:00Z",
            }
        )

    assert stats.index_progress == {"asset-1": _dt(2)}
    assert stats.asset_time_statistics == stats.index_progress


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
