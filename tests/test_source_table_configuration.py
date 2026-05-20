import datetime

import pytest
from pydantic import ValidationError

from mainsequence.client import models_tdag


def _source_config_payload():
    return {
        "id": 11,
        "related_table": 44,
        "time_index_name": "time_index",
        "index_names": ["time_index", "account_uid", "unique_identifier"],
        "column_dtypes_map": {
            "time_index": "datetime64[ns, UTC]",
            "account_uid": "uuid",
            "unique_identifier": "object",
            "value": "float64",
        },
        "last_time_index_value": "2026-05-01T03:00:00Z",
        "earliest_index_value": "2026-05-01T00:00:00Z",
        "storage_layout": {
            "time_index": "time_index",
            "identity_dimensions": ["account_uid", "unique_identifier"],
        },
        "physical_index_plan": {
            "uniqueness": {"columns": ["time_index", "account_uid", "unique_identifier"]},
            "lookup": {"columns": ["account_uid", "unique_identifier"]},
        },
        "multi_index_stats": {
            "_GLOBAL_": {
                "min": "2026-05-01T00:00:00Z",
                "max": "2026-05-01T03:00:00Z",
            },
            "index_progress": {
                "account-a": {"BTC": "2026-05-01T02:00:00Z"},
            },
            "index_min": {
                "account-a": {"BTC": "2026-05-01T00:00:00Z"},
            },
        },
        "multi_index_column_stats": None,
    }


def test_source_table_configuration_parses_canonical_response_without_table_partition():
    config = models_tdag.SourceTableConfiguration(**_source_config_payload())

    assert "table_partition" not in models_tdag.SourceTableConfiguration.model_fields
    assert not hasattr(config, "table_partition")
    assert config.storage_layout == {
        "time_index": "time_index",
        "identity_dimensions": ["account_uid", "unique_identifier"],
    }
    assert config.physical_index_plan == {
        "uniqueness": {"columns": ["time_index", "account_uid", "unique_identifier"]},
        "lookup": {"columns": ["account_uid", "unique_identifier"]},
    }
    assert config.multi_index_stats["_GLOBAL_"]["max"] == "2026-05-01T03:00:00Z"
    assert config.last_time_index_value == datetime.datetime(
        2026, 5, 1, 3, tzinfo=datetime.UTC
    )
    assert config.earliest_index_value == datetime.datetime(
        2026, 5, 1, tzinfo=datetime.UTC
    )


def test_source_table_configuration_rejects_table_partition_typed_surface():
    payload = _source_config_payload()
    payload["table_partition"] = {"kind": "legacy"}

    with pytest.raises(ValidationError, match="table_partition"):
        models_tdag.SourceTableConfiguration(**payload)


def test_source_table_configuration_get_data_updates_prefers_canonical_stats(monkeypatch):
    captured = {}

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return {
                "multi_index_stats": {
                    "_GLOBAL_": {
                        "min": "2026-05-01T00:00:00Z",
                        "max": "2026-05-01T03:00:00Z",
                    },
                    "index_progress": {
                        "account-a": {"BTC": "2026-05-01T02:00:00Z"},
                    },
                    "index_min": {
                        "account-a": {"BTC": "2026-05-01T00:00:00Z"},
                    },
                    "max_per_asset_symbol": {
                        "legacy-asset": "2026-04-01T00:00:00Z",
                    },
                },
                "multi_index_column_stats": {
                    "value": {
                        "account-a": {
                            "BTC": {
                                "min": "2026-05-01T00:00:00Z",
                                "max": "2026-05-01T02:00:00Z",
                            }
                        }
                    }
                },
            }

    def _fake_make_request(*, s, loaders, r_type, url, accept_gzip=False):
        captured["r_type"] = r_type
        captured["url"] = url
        captured["accept_gzip"] = accept_gzip
        return FakeResponse()

    monkeypatch.setattr(models_tdag, "make_request", _fake_make_request)
    monkeypatch.setattr(
        models_tdag.SourceTableConfiguration,
        "build_session",
        classmethod(lambda cls: object()),
    )

    config = models_tdag.SourceTableConfiguration(**_source_config_payload())

    update_stats = config.get_data_updates()

    assert captured == {
        "r_type": "GET",
        "url": f"{models_tdag.SourceTableConfiguration.get_object_url()}/44/get_stats/",
        "accept_gzip": True,
    }
    assert update_stats.max_time_index_value == datetime.datetime(
        2026, 5, 1, 3, tzinfo=datetime.UTC
    )
    assert update_stats.global_index_progress == {
        "min": datetime.datetime(2026, 5, 1, tzinfo=datetime.UTC),
        "max": datetime.datetime(2026, 5, 1, 3, tzinfo=datetime.UTC),
    }
    assert update_stats.index_progress == {
        "account-a": {"BTC": datetime.datetime(2026, 5, 1, 2, tzinfo=datetime.UTC)}
    }
    assert update_stats.index_min == {
        "account-a": {"BTC": datetime.datetime(2026, 5, 1, tzinfo=datetime.UTC)}
    }
    assert update_stats.asset_time_statistics == update_stats.index_progress
    assert "legacy-asset" not in update_stats.index_progress
    assert update_stats.multi_index_column_stats == {
        "value": {
            "account-a": {
                "BTC": {
                    "min": datetime.datetime(2026, 5, 1, tzinfo=datetime.UTC),
                    "max": datetime.datetime(2026, 5, 1, 2, tzinfo=datetime.UTC),
                }
            }
        }
    }


def test_source_table_configuration_get_data_updates_legacy_max_per_asset_fallback(
    monkeypatch,
):
    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return {
                "multi_index_stats": {
                    "max_per_asset_symbol": {
                        "AAPL": "2026-05-01T01:00:00Z",
                        "MSFT": "2026-05-01T03:00:00Z",
                    },
                    "min_per_asset_symbol": {
                        "AAPL": "2026-05-01T00:00:00Z",
                        "MSFT": "2026-05-01T02:00:00Z",
                    },
                },
                "multi_index_column_stats": None,
            }

    monkeypatch.setattr(
        models_tdag,
        "make_request",
        lambda **_kwargs: FakeResponse(),
    )
    monkeypatch.setattr(
        models_tdag.SourceTableConfiguration,
        "build_session",
        classmethod(lambda cls: object()),
    )

    payload = _source_config_payload()
    payload["index_names"] = ["time_index", "unique_identifier"]
    payload["storage_layout"] = {
        "time_index": "time_index",
        "identity_dimensions": ["unique_identifier"],
    }
    payload["physical_index_plan"] = {
        "uniqueness": {"columns": ["time_index", "unique_identifier"]},
    }
    config = models_tdag.SourceTableConfiguration(**payload)

    update_stats = config.get_data_updates()

    assert update_stats.global_index_progress is None
    assert update_stats.max_time_index_value == datetime.datetime(
        2026, 5, 1, 3, tzinfo=datetime.UTC
    )
    assert update_stats.index_progress == {
        "AAPL": datetime.datetime(2026, 5, 1, 1, tzinfo=datetime.UTC),
        "MSFT": datetime.datetime(2026, 5, 1, 3, tzinfo=datetime.UTC),
    }
    assert update_stats.index_min == {
        "AAPL": datetime.datetime(2026, 5, 1, tzinfo=datetime.UTC),
        "MSFT": datetime.datetime(2026, 5, 1, 2, tzinfo=datetime.UTC),
    }
    assert update_stats.asset_time_statistics == update_stats.index_progress
