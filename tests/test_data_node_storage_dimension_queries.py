import datetime

import pandas as pd

from mainsequence.client import models_metatables


def _source_config(index_names: list[str]) -> models_metatables.TimeIndexedProfile:
    column_dtypes_map = {
        "time_index": "datetime64[ns, UTC]",
        "value": "float64",
    }
    column_dtypes_map.update({name: "object" for name in index_names[1:]})
    return models_metatables.TimeIndexedProfile(
        related_table_uid="714",
        time_index_name="time_index",
        index_names=index_names,
        column_dtypes_map=column_dtypes_map,
        storage_layout={
            "time_index": "time_index",
            "identity_dimensions": index_names[1:],
        },
        physical_index_plan={
            "uniqueness": {"columns": index_names},
        },
    )


def _storage(index_names: list[str]) -> models_metatables.TimeIndexMetaData:
    return models_metatables.TimeIndexMetaData(
        uid="714",
        storage_hash="prices_hash",
        management_mode="platform_managed",
        physical_table_name="prices_hash",
        data_source=1,
        source_class_name="PricesNode",
        creation_date="2026-04-01T00:00:00Z",
        time_indexed_profile=_source_config(index_names),
    )


def test_data_node_storage_has_no_initialize_source_table_method():
    assert not hasattr(models_metatables.TimeIndexMetaData, "initialize_source_table")


def test_get_last_observation_sends_dimension_filters_and_coordinates(monkeypatch):
    captured = {}

    class FakeResponse:
        status_code = 200
        text = ""

        @staticmethod
        def json():
            return [
                {
                    "time_index": "2026-05-01T03:00:00Z",
                    "account_uid": "account-a",
                    "unique_identifier": "BTC",
                    "value": 101.5,
                }
            ]

    def _fake_make_request(*, r_type, url, payload, s, loaders, time_out=None):
        captured["r_type"] = r_type
        captured["url"] = url
        captured["payload"] = payload
        captured["timeout"] = time_out
        return FakeResponse()

    monkeypatch.setattr(models_metatables, "make_request", _fake_make_request)
    monkeypatch.setattr(
        models_metatables.TimeIndexMetaData,
        "build_session",
        classmethod(lambda cls: object()),
    )

    df = _storage(["time_index", "account_uid", "unique_identifier"]).get_last_observation(
        dimension_filters={
            "account_uid": ["account-a"],
            "unique_identifier": ["BTC"],
        },
        index_coordinates=[{"account_uid": "account-a", "unique_identifier": "BTC"}],
        timeout=30,
    )

    assert captured["payload"] == {
        "json": {
            "dimension_filters": {
                "account_uid": ["account-a"],
                "unique_identifier": ["BTC"],
            },
            "index_coordinates": [{"account_uid": "account-a", "unique_identifier": "BTC"}],
        }
    }
    assert captured["timeout"] == 30
    assert isinstance(df.index, pd.MultiIndex)
    assert df.index.names == ["time_index", "account_uid", "unique_identifier"]


def test_get_data_between_dates_from_api_sends_dimension_range_map(monkeypatch):
    captured_payloads = []

    class FakeResponse:
        status_code = 200
        text = ""

        @staticmethod
        def json():
            return {
                "results": [
                    {
                        "time_index": "2026-05-01T03:00:00Z",
                        "account_uid": "account-a",
                        "unique_identifier": "BTC",
                        "value": 101.5,
                    }
                ],
                "next_offset": None,
            }

    def _fake_make_request(*, s, loaders, payload, r_type, url):
        captured_payloads.append(payload["json"])
        return FakeResponse()

    monkeypatch.setattr(models_metatables, "make_request", _fake_make_request)
    monkeypatch.setattr(
        models_metatables.TimeIndexMetaData,
        "build_session",
        classmethod(lambda cls: object()),
    )

    start = datetime.datetime(2026, 5, 1, 0, tzinfo=datetime.UTC)
    end = datetime.datetime(2026, 5, 1, 3, tzinfo=datetime.UTC)
    df = _storage(
        ["time_index", "account_uid", "unique_identifier"]
    ).get_data_between_dates_from_api(
        start_date=start,
        end_date=end,
        dimension_range_map=[
            {
                "coordinate": {
                    "account_uid": "account-a",
                    "unique_identifier": "BTC",
                },
                "start_date": start,
                "end_date": end,
            }
        ],
        columns=["value"],
    )

    assert len(df) == 1
    assert captured_payloads == [
        {
            "start_date": start.timestamp(),
            "end_date": end.timestamp(),
            "great_or_equal": None,
            "less_or_equal": None,
            "columns": ["value"],
            "offset": 0,
            "dimension_range_map": [
                {
                    "coordinate": {
                        "account_uid": "account-a",
                        "unique_identifier": "BTC",
                    },
                    "start_date": int(start.timestamp()),
                    "end_date": int(end.timestamp()),
                }
            ],
        }
    ]


def test_get_data_between_dates_from_node_identifier_sends_canonical_dimensions(monkeypatch):
    captured = {}

    class FakeResponse:
        status_code = 200
        text = ""

        @staticmethod
        def json():
            return {
                "results": [],
                "next_offset": None,
                "storage_node": _storage(
                    ["time_index", "account_uid", "unique_identifier"]
                ).model_dump(mode="json"),
            }

    def _fake_make_request(*, s, loaders, payload, r_type, url):
        captured["payload"] = payload
        captured["url"] = url
        return FakeResponse()

    monkeypatch.setattr(models_metatables, "make_request", _fake_make_request)
    monkeypatch.setattr(
        models_metatables.TimeIndexMetaData,
        "build_session",
        classmethod(lambda cls: object()),
    )

    df, storage = models_metatables.TimeIndexMetaData.get_data_between_dates_from_node_identifier(
        node_identifier="prices-node",
        dimension_filters={"account_uid": ["account-a"]},
        index_coordinates=[{"account_uid": "account-a", "unique_identifier": "BTC"}],
    )

    assert df.empty
    assert storage.uid == "714"
    assert captured["payload"]["json"]["node_identifier"] == "prices-node"
    assert captured["payload"]["json"]["dimension_filters"] == {"account_uid": ["account-a"]}
    assert captured["payload"]["json"]["index_coordinates"] == [
        {"account_uid": "account-a", "unique_identifier": "BTC"}
    ]


def test_delete_after_date_sends_canonical_coordinate_scope(monkeypatch):
    captured = {}

    class FakeResponse:
        status_code = 200
        content = b'{"ok": true}'

        @staticmethod
        def json():
            return {"ok": True, "deleted_count": 3}

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured["payload"] = payload
        captured["timeout"] = time_out
        return FakeResponse()

    monkeypatch.setattr(models_metatables, "make_request", _fake_make_request)
    monkeypatch.setattr(
        models_metatables.TimeIndexMetaData,
        "build_session",
        classmethod(lambda cls: object()),
    )

    result = _storage(["time_index", "account_uid", "unique_identifier"]).delete_after_date(
        "2026-05-01T00:00:00Z",
        index_coordinates=[{"account_uid": "account-a", "unique_identifier": "BTC"}],
        timeout=30,
    )

    assert result == {"ok": True, "deleted_count": 3}
    assert captured == {
        "payload": {
            "json": {
                "after_date": "2026-05-01T00:00:00Z",
                "index_coordinates": [{"account_uid": "account-a", "unique_identifier": "BTC"}],
            }
        },
        "timeout": 30,
    }
