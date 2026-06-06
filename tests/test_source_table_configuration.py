import datetime

import pytest
from pydantic import ValidationError

from mainsequence.client import metatables as models_metatables


def _source_config_payload():
    return {
        "related_table_uid": "storage-uid-44",
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


def test_time_indexed_profile_parses_canonical_response():
    config = models_metatables.TimeIndexedProfile(**_source_config_payload())

    assert config.storage_layout == {
        "time_index": "time_index",
        "identity_dimensions": ["account_uid", "unique_identifier"],
    }
    assert config.physical_index_plan == {
        "uniqueness": {"columns": ["time_index", "account_uid", "unique_identifier"]},
        "lookup": {"columns": ["account_uid", "unique_identifier"]},
    }
    assert config.multi_index_stats["_GLOBAL_"]["max"] == "2026-05-01T03:00:00Z"
    assert config.last_time_index_value == datetime.datetime(2026, 5, 1, 3, tzinfo=datetime.UTC)
    assert config.earliest_index_value == datetime.datetime(2026, 5, 1, tzinfo=datetime.UTC)


def test_time_indexed_profile_rejects_foreign_keys():
    payload = _source_config_payload()
    payload["foreign_keys"] = [
        {
            "name": "fk_prices_asset_uid_4f3a2b1c",
            "source_columns": ["account_uid"],
            "target_meta_table_uid": "asset-meta-table-uid",
            "target_columns": ["uid"],
            "on_delete": "restrict",
        }
    ]

    with pytest.raises(ValidationError, match="foreign_keys"):
        models_metatables.TimeIndexedProfile(**payload)


def test_time_index_meta_table_contract_requires_registered_profile_index_names():
    storage = models_metatables.TimeIndexMetaTable.model_construct(
        table_contract={
            "dynamic_table": {
                "time_index_name": "time_index",
                "index_names": ["time_index", "asset_identifier"],
            },
            "columns": [
                {"name": "time_index", "data_type": "datetime64[ns, UTC]"},
                {"name": "asset_identifier", "data_type": "object"},
            ],
        },
        time_indexed_profile=None,
    )

    assert storage.index_names == []
    with pytest.raises(ValueError, match="got no time_indexed_profile"):
        storage._require_time_indexed_table_contract()


def test_time_index_meta_table_contract_does_not_infer_from_storage_layout():
    storage = models_metatables.TimeIndexMetaTable.model_construct(
        time_indexed_profile=models_metatables.TimeIndexedProfile.model_construct(
            time_index_name="time_index",
            index_names=[],
            column_dtypes_map={
                "time_index": "datetime64[ns, UTC]",
                "asset_identifier": "object",
            },
            storage_layout={
                "time_index": "time_index",
                "identity_dimensions": ["asset_identifier"],
                "uniqueness": {"columns": ["time_index", "asset_identifier"]},
            },
        ),
    )

    assert storage.index_names == []
    with pytest.raises(ValueError, match="expected index_names from registered storage profile"):
        storage._require_time_indexed_table_contract()


def test_time_indexed_profile_get_data_updates_prefers_canonical_stats(monkeypatch):
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

    def _fake_make_request(*, s, loaders, r_type, url, time_out=None):
        captured["r_type"] = r_type
        captured["url"] = url
        captured["timeout"] = time_out
        return FakeResponse()

    monkeypatch.setattr(models_metatables, "make_request", _fake_make_request)
    monkeypatch.setattr(
        models_metatables.TimeIndexMetaTable,
        "build_session",
        classmethod(lambda cls: object()),
    )

    config = models_metatables.TimeIndexedProfile(**_source_config_payload())
    storage = models_metatables.TimeIndexMetaTable.model_construct(
        uid="storage-uid-44",
        time_indexed_profile=config,
    )

    update_stats = storage.get_data_updates()

    assert captured == {
        "r_type": "GET",
        "url": f"{models_metatables.TimeIndexMetaTable.get_object_url()}/storage-uid-44/get-stats/",
        "timeout": None,
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


def test_time_indexed_profile_column_metadata_mutation_helper_is_removed():
    assert not hasattr(models_metatables.TimeIndexMetaTable, "set_or_update_columns_metadata")
