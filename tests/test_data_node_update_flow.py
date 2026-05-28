import base64
import datetime
import gzip
import json
from types import SimpleNamespace

import pandas as pd
import pytest
from pydantic import ValidationError

from mainsequence.client import models_tdag
from mainsequence.tdag.data_nodes.models import RecordDefinition


def _dt(hour: int) -> datetime.datetime:
    return datetime.datetime(2026, 5, 1, hour, tzinfo=datetime.UTC)


def _minimal_update(**kwargs):
    payload = {
        "uid": "data-node-update-77",
        "update_hash": "update-hash",
        "build_configuration": {},
        "ogm_dependencies_linked": False,
        "data_node_storage": "data-node-storage-44",
    }
    payload.update(kwargs)
    return models_tdag.DataNodeUpdate(**payload)


def _source_config(
    *,
    index_names: list[str],
    column_dtypes_map: dict[str, str],
    columns_metadata: list[models_tdag.ColumnMetaData] | None = None,
    foreign_key_projections: list[models_tdag.SourceTableForeignKeyProjection] | None = None,
) -> models_tdag.TimeIndexedProfile:
    return models_tdag.TimeIndexedProfile(
        related_table_uid="data-node-storage-44",
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
        columns_metadata=columns_metadata,
        foreign_key_projections=foreign_key_projections or [],
    )


def _storage_with_source_config(
    *,
    index_names: list[str],
    column_dtypes_map: dict[str, str],
    columns_metadata: list[models_tdag.ColumnMetaData] | None = None,
    foreign_key_projections: list[models_tdag.SourceTableForeignKeyProjection] | None = None,
) -> models_tdag.DataNodeStorage:
    return models_tdag.DataNodeStorage.model_construct(
        uid="data-node-storage-44",
        storage_hash="storage-hash",
        time_indexed_profile=_source_config(
            index_names=index_names,
            column_dtypes_map=column_dtypes_map,
            columns_metadata=columns_metadata,
            foreign_key_projections=foreign_key_projections,
        ),
    )


def _decode_compressed_payload(captured_payload):
    compressed = base64.b64decode(captured_payload["json"]["data"])
    return json.loads(gzip.decompress(compressed).decode("utf-8"))


def test_post_data_frame_in_chunks_serializes_remote_temporal_payload_columns(monkeypatch):
    captured = {}

    class FakeResponse:
        status_code = 200
        text = ""
        content = b'{"ok": true}'

        @staticmethod
        def json():
            return {"ok": True}

    def _fake_make_request(*, s, loaders, payload, r_type, url, time_out=None):
        captured["payload"] = payload
        captured["r_type"] = r_type
        captured["url"] = url
        return FakeResponse()

    monkeypatch.setattr(models_tdag, "make_request", _fake_make_request)
    monkeypatch.setattr(
        models_tdag.DataNodeUpdate,
        "build_session",
        classmethod(lambda cls: object()),
    )

    frame = pd.DataFrame(
        {
            "time_index": [pd.Timestamp("2026-05-01T00:00:00Z")],
            "event_date": [datetime.date(2026, 5, 1)],
            "event_time": [pd.Timestamp("2026-05-01T12:30:00Z")],
        }
    )

    models_tdag.DataNodeUpdate.post_data_frame_in_chunks(
        serialized_data_frame=frame,
        data_node_update=_minimal_update(),
        index_names=["time_index"],
        time_index_name="time_index",
        column_dtypes_map={
            "time_index": "timestamp with time zone",
            "event_date": "date",
            "event_time": "datetime64[ns, UTC]",
        },
    )

    decoded = _decode_compressed_payload(captured["payload"])
    assert decoded == [
        {
            "time_index": "2026-05-01T00:00:00Z",
            "event_date": "2026-05-01",
            "event_time": "2026-05-01T12:30:00Z",
        }
    ]


def test_set_start_of_execution_prefers_canonical_update_stats(monkeypatch):
    class FakeResponse:
        status_code = 201
        text = ""
        content = b'{"ok": true}'

        @staticmethod
        def json():
            return {
                "historical_update": {
                    "uid": "historical-update-15",
                    "related_table_uid": "data-node-update-77",
                    "update_time_start": "2026-05-01T03:00:00Z",
                },
                "global_index_progress": {
                    "min": "2026-05-01T00:00:00Z",
                    "max": "2026-05-01T03:00:00Z",
                },
                "index_progress": {
                    "account-a": {"asset-1": "2026-05-01T02:00:00Z"}
                },
                "index_min": {
                    "account-a": {"asset-1": "2026-05-01T00:00:00Z"}
                },
                "multi_index_column_stats": {},
                "time_index_name": "time_index",
                "index_names": ["time_index", "account_uid", "unique_identifier"],
                "must_update": True,
                "direct_dependency_uids": ["dependency-1", "dependency-2"],
            }

    monkeypatch.setattr(models_tdag, "make_request", lambda **_kwargs: FakeResponse())
    monkeypatch.setattr(
        models_tdag.DataNodeUpdate,
        "build_session",
        classmethod(lambda cls: object()),
    )

    historical_update = _minimal_update().set_start_of_execution()

    stats = historical_update.update_statistics
    assert stats.global_index_progress == {"min": _dt(0), "max": _dt(3)}
    assert stats.max_time_index_value == _dt(3)
    assert stats.index_progress == {"account-a": {"asset-1": _dt(2)}}
    assert stats.index_min == {"account-a": {"asset-1": _dt(0)}}
    assert historical_update.must_update is True
    assert historical_update.direct_dependency_uids == ["dependency-1", "dependency-2"]


def test_last_update_payload_model_accepts_top_level_and_nested_shapes():
    top_level = models_tdag.LastUpdateIndexTimePayload.model_validate(
        {
            "global_index_progress": {
                "max": "2026-05-01 03:00:00+00:00",
                "min": "2026-05-01 00:00:00+00:00",
            },
            "index_progress": {"account-a": {"asset-1": "2026-05-01 02:00:00+00:00"}},
            "index_min": {"account-a": {"asset-1": "2026-05-01 00:00:00+00:00"}},
            "multi_index_column_stats": {},
        }
    )
    assert top_level.to_nested_payload() == {
        "multi_index_stats": {
            "_GLOBAL_": {
                "max": "2026-05-01 03:00:00+00:00",
                "min": "2026-05-01 00:00:00+00:00",
            },
            "index_progress": {"account-a": {"asset-1": "2026-05-01 02:00:00+00:00"}},
            "index_min": {"account-a": {"asset-1": "2026-05-01 00:00:00+00:00"}},
        },
        "multi_index_column_stats": {},
    }

    nested = models_tdag.LastUpdateIndexTimePayload.model_validate(
        {
            "multi_index_stats": {
                "_GLOBAL_": {
                    "max": "2026-05-01 03:00:00+00:00",
                    "min": "2026-05-01 00:00:00+00:00",
                },
                "index_progress": {},
                "index_min": {},
            },
            "multi_index_column_stats": {},
        }
    )
    assert nested.to_nested_payload()["multi_index_stats"]["index_progress"] == {}


def test_last_update_payload_model_rejects_unknown_keys_generically():
    with pytest.raises(ValidationError):
        models_tdag.LastUpdateIndexTimePayload.model_validate(
            {
                "unexpected_key": "2026-05-01 03:00:00+00:00",
                "global_index_progress": {
                    "max": "2026-05-01 03:00:00+00:00",
                    "min": "2026-05-01 00:00:00+00:00",
                },
                "index_progress": {},
                "index_min": {},
            }
        )

    with pytest.raises(ValidationError):
        models_tdag.LastUpdateIndexTimePayload.model_validate(
            {
                "multi_index_stats": {
                    "_GLOBAL_": {
                        "max": "2026-05-01 03:00:00+00:00",
                        "min": "2026-05-01 00:00:00+00:00",
                    },
                    "index_progress": {},
                    "index_min": {},
                    "unexpected_nested_key": {},
                },
                "multi_index_column_stats": {},
            }
        )


def test_set_last_update_index_time_from_update_stats_sends_canonical_payload(monkeypatch):
    captured = {}

    class FakeResponse:
        status_code = 200
        text = ""
        content = b'{"ok": true}'

        @staticmethod
        def json():
            return {
                "uid": "data-node-update-77",
                "update_hash": "update-hash",
                "build_configuration": {},
                "ogm_dependencies_linked": False,
                "data_node_storage": "data-node-storage-44",
            }

    def _fake_make_request(*, s, loaders, payload, r_type, url, time_out=None):
        captured["payload"] = payload
        captured["r_type"] = r_type
        captured["url"] = url
        captured["timeout"] = time_out
        return FakeResponse()

    monkeypatch.setattr(models_tdag, "make_request", _fake_make_request)
    monkeypatch.setattr(
        models_tdag.DataNodeUpdate,
        "build_session",
        classmethod(lambda cls: object()),
    )

    update = _minimal_update()
    update.set_last_update_index_time_from_update_stats(
        global_index_progress={
            "max": "2026-05-01 03:00:00+00:00",
            "min": "2026-05-01 00:00:00+00:00",
        },
        index_progress={"account-a": {"asset-1": "2026-05-01 02:00:00+00:00"}},
        index_min={"account-a": {"asset-1": "2026-05-01 00:00:00+00:00"}},
        multi_index_column_stats={},
        timeout=12,
    )

    decoded = _decode_compressed_payload(captured["payload"])
    assert captured["r_type"] == "POST"
    assert captured["timeout"] == 12
    assert decoded == {
        "multi_index_stats": {
            "_GLOBAL_": {
                "max": "2026-05-01 03:00:00+00:00",
                "min": "2026-05-01 00:00:00+00:00",
            },
            "index_progress": {"account-a": {"asset-1": "2026-05-01 02:00:00+00:00"}},
            "index_min": {"account-a": {"asset-1": "2026-05-01 00:00:00+00:00"}},
        },
        "multi_index_column_stats": {},
    }
    assert "last_time_index_value" not in decoded
    assert "max_per_asset_symbol" not in decoded


def test_get_index_progress_chunk_stats_for_three_index_frame():
    df = pd.DataFrame(
        {
            "time_index": [_dt(0), _dt(2), _dt(1), _dt(3)],
            "account_uid": ["account-a", "account-a", "account-b", "account-b"],
            "unique_identifier": ["asset-1", "asset-1", "asset-1", "asset-2"],
            "value": [1, 2, 3, 4],
        }
    )

    stats, grouped_dates = models_tdag.get_index_progress_chunk_stats(
        df,
        time_index_name="time_index",
        index_names=["time_index", "account_uid", "unique_identifier"],
    )

    assert stats == {
        "_GLOBAL_": {"min": _dt(0), "max": _dt(3)},
        "index_progress": {
            "account-a": {"asset-1": _dt(2)},
            "account-b": {"asset-1": _dt(1), "asset-2": _dt(3)},
        },
        "index_min": {
            "account-a": {"asset-1": _dt(0)},
            "account-b": {"asset-1": _dt(1), "asset-2": _dt(3)},
        },
    }
    assert grouped_dates is not None


def test_set_last_update_index_time_rejects_legacy_per_asset_backend_payload(monkeypatch):
    def _unexpected_make_request(**_kwargs):
        raise AssertionError("legacy backend payload should fail before make_request")

    monkeypatch.setattr(models_tdag, "make_request", _unexpected_make_request)

    update = _minimal_update()
    with pytest.raises(ValidationError):
        update.set_last_update_index_time_from_update_stats(
            multi_index_stats={
                "_GLOBAL_": {
                    "min": "2026-05-01 00:00:00+00:00",
                    "max": "2026-05-01 03:00:00+00:00",
                },
                "_PER_ASSET_": {
                    "asset-1": "2026-05-01 03:00:00+00:00",
                },
            },
            multi_index_column_stats={},
        )


def test_upsert_data_into_table_computes_canonical_stats(monkeypatch):
    calls = {}

    class FakeResource:
        def insert_data_into_table(self, **kwargs):
            calls["insert"] = kwargs

    class FakeStorage:
        def handle_time_indexed_profile_creation(self, **_kwargs):
            raise AssertionError("upsert should not create or validate source-table profile")

    update = models_tdag.DataNodeUpdate.model_construct(
        id=77,
        update_hash="update-hash",
        build_configuration={},
        ogm_dependencies_linked=False,
        data_node_storage=FakeStorage(),
    )

    def _fake_set_last(**kwargs):
        calls["set_last"] = kwargs
        return "updated"

    object.__setattr__(
        update,
        "set_last_update_index_time_from_update_stats",
        _fake_set_last,
    )

    index = pd.MultiIndex.from_tuples(
        [
            (_dt(0), "account-a", "asset-1"),
            (_dt(2), "account-a", "asset-1"),
            (_dt(3), "account-b", "asset-2"),
        ],
        names=["time_index", "account_uid", "unique_identifier"],
    )
    df = pd.DataFrame({"value": [1.0, 2.0, 3.0]}, index=index)

    result = update.upsert_data_into_table(
        df,
        data_source=SimpleNamespace(related_resource=FakeResource()),
        overwrite=True,
    )

    assert result == "updated"
    assert calls["insert"]["index_names"] == [
        "time_index",
        "account_uid",
        "unique_identifier",
    ]
    assert calls["set_last"]["global_index_progress"] == {"min": _dt(0), "max": _dt(3)}
    assert calls["set_last"]["index_progress"] == {
        "account-a": {"asset-1": _dt(2)},
        "account-b": {"asset-2": _dt(3)},
    }
    assert calls["set_last"]["index_min"] == {
        "account-a": {"asset-1": _dt(0)},
        "account-b": {"asset-2": _dt(3)},
    }
    assert calls["set_last"]["multi_index_column_stats"] == {
        "value": {
            "account-a": {"asset-1": {"min": _dt(0), "max": _dt(2)}},
            "account-b": {"asset-2": {"min": _dt(3), "max": _dt(3)}},
        }
    }
    assert "max_per_asset_symbol" not in calls["set_last"]
    assert "last_time_index_value" not in calls["set_last"]


def test_upsert_data_into_table_uses_declared_record_dtype_for_payload_columns():
    calls = {}

    class FakeResource:
        def insert_data_into_table(self, **kwargs):
            calls["insert"] = kwargs

    update = models_tdag.DataNodeUpdate.model_construct(
        id=77,
        update_hash="update-hash",
        build_configuration={},
        ogm_dependencies_linked=False,
        data_node_storage=_storage_with_source_config(
            index_names=["time_index"],
            column_dtypes_map={
                "time_index": "timestamp with time zone",
                "venue_specific_properties": "jsonb",
                "venue_event_time": "timestamp with time zone",
            },
        ),
    )

    object.__setattr__(
        update,
        "set_last_update_index_time_from_update_stats",
        lambda **kwargs: kwargs,
    )

    df = pd.DataFrame(
        {
            "venue_specific_properties": [{"tick_size": "0.01"}],
            "venue_event_time": [pd.Timestamp("2026-05-01T12:30:00Z")],
        },
        index=pd.DatetimeIndex([_dt(0)], name="time_index"),
    )

    update.upsert_data_into_table(
        df,
        data_source=SimpleNamespace(related_resource=FakeResource()),
        overwrite=True,
        records=[
            RecordDefinition(
                column_name="venue_specific_properties",
                dtype="jsonb",
                label="Venue Specific Properties",
                description="JSON payload for exchange-specific metadata.",
            ),
            RecordDefinition(
                column_name="venue_event_time",
                dtype="datetime64[ns, UTC]",
                label="Venue Event Time",
                description="Timezone-aware event timestamp from the venue payload.",
            ),
        ],
    )

    assert calls["insert"]["column_dtypes_map"] == {
        "time_index": "timestamp with time zone",
        "venue_specific_properties": "jsonb",
        "venue_event_time": "timestamp with time zone",
    }


def test_dynamic_table_data_source_delegates_direct_class_type_reads():
    calls = {}
    expected = pd.DataFrame({"value": [1.0]})

    class FakeResource:
        class_type = "direct"

        def get_data_by_time_index(self, *args, **kwargs):
            calls["args"] = args
            calls["kwargs"] = kwargs
            return expected

    data_source = models_tdag.DynamicTableDataSource.model_construct(
        related_resource=FakeResource(),
        related_resource_class_type="direct",
    )
    update = object()
    start_date = _dt(0)

    result = data_source.get_data_by_time_index(
        data_node_update=update,
        start_date=start_date,
        columns=["value"],
    )

    assert result is expected
    assert calls["args"] == ()
    assert calls["kwargs"] == {
        "data_node_update": update,
        "start_date": start_date,
        "columns": ["value"],
    }


def test_upsert_data_into_table_rejects_full_index_duplicates():
    update = models_tdag.DataNodeUpdate.model_construct(
        id=77,
        update_hash="update-hash",
        build_configuration={},
        ogm_dependencies_linked=False,
        data_node_storage=_storage_with_source_config(
            index_names=["time_index", "account_uid", "unique_identifier"],
            column_dtypes_map={
                "time_index": "timestamp with time zone",
                "account_uid": "string",
                "unique_identifier": "string",
                "value": "float64",
            },
        ),
    )
    duplicate_index = pd.MultiIndex.from_tuples(
        [
            (_dt(0), "account-a", "asset-1"),
            (_dt(0), "account-a", "asset-1"),
        ],
        names=["time_index", "account_uid", "unique_identifier"],
    )
    df = pd.DataFrame({"value": [1.0, 2.0]}, index=duplicate_index)

    with pytest.raises(Exception, match="Duplicates found"):
        update.upsert_data_into_table(
            df,
            data_source=SimpleNamespace(related_resource=SimpleNamespace()),
            overwrite=True,
        )
