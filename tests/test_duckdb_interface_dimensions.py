import datetime
import os
from types import SimpleNamespace

import pandas as pd
import pytest

from mainsequence.client import models_metatables, models_tdag
from mainsequence.client.data_sources_interfaces.duckdb import DuckDBInterface

INDEX_NAMES = ["time_index", "account_uid", "asset_uid"]


def _dt(hour: int) -> datetime.datetime:
    return datetime.datetime(2026, 5, 25, hour, tzinfo=datetime.UTC)


def _interface(tmp_path) -> DuckDBInterface:
    os.environ["DO_NOT_CHECK_META_TABLES"] = "true"
    return DuckDBInterface(db_path=tmp_path / "duckdb")


def _seed_n_dimensional_table(interface: DuckDBInterface, table: str = "node") -> None:
    interface.upsert(
        pd.DataFrame(
            {
                "time_index": [_dt(0), _dt(0), _dt(1)],
                "account_uid": ["acct-a", "acct-b", "acct-a"],
                "asset_uid": ["asset-1", "asset-1", "asset-2"],
                "value": [10.0, 20.0, 30.0],
            }
        ),
        table=table,
        index_names=INDEX_NAMES,
        time_index_name="time_index",
    )


def test_duckdb_upsert_preserves_n_dimensional_identity(tmp_path):
    interface = _interface(tmp_path)
    _seed_n_dimensional_table(interface)

    df = interface.read(
        table="node",
        index_names=INDEX_NAMES,
        time_index_name="time_index",
    )

    assert len(df) == 3
    assert set(zip(df["account_uid"], df["asset_uid"], df["value"], strict=False)) == {
        ("acct-a", "asset-1", 10.0),
        ("acct-b", "asset-1", 20.0),
        ("acct-a", "asset-2", 30.0),
    }


def test_duckdb_local_backend_stores_data_without_physical_fk_constraints(tmp_path):
    interface = _interface(tmp_path)
    _seed_n_dimensional_table(interface)

    interface.upsert(
        pd.DataFrame(
            {
                "time_index": [_dt(2)],
                "account_uid": ["acct-a"],
                "asset_uid": ["missing-target-asset"],
                "value": [40.0],
            }
        ),
        table="node",
        index_names=INDEX_NAMES,
        time_index_name="time_index",
    )

    df = interface.read(
        table="node",
        index_names=INDEX_NAMES,
        time_index_name="time_index",
        index_coordinates=[{"account_uid": "acct-a", "asset_uid": "missing-target-asset"}],
    )
    constraints = interface.con.execute(
        "SELECT * FROM duckdb_constraints() WHERE table_name = 'node'"
    ).fetchall()

    assert df["value"].tolist() == [40.0]
    assert constraints == []


def test_duckdb_upsert_rewrites_only_matching_full_index_tuple(tmp_path):
    interface = _interface(tmp_path)
    _seed_n_dimensional_table(interface)

    interface.upsert(
        pd.DataFrame(
            {
                "time_index": [_dt(0)],
                "account_uid": ["acct-a"],
                "asset_uid": ["asset-1"],
                "value": [15.0],
            }
        ),
        table="node",
        index_names=INDEX_NAMES,
        time_index_name="time_index",
    )

    df = interface.read(
        table="node",
        index_names=INDEX_NAMES,
        time_index_name="time_index",
    )

    values = {
        (row.account_uid, row.asset_uid): row.value
        for row in df.itertuples(index=False)
        if row.time_index == pd.Timestamp(_dt(0))
    }
    assert values == {
        ("acct-a", "asset-1"): 15.0,
        ("acct-b", "asset-1"): 20.0,
    }


def test_duckdb_column_subset_read_includes_all_index_columns(tmp_path):
    interface = _interface(tmp_path)
    _seed_n_dimensional_table(interface)

    df = interface.read(
        table="node",
        index_names=INDEX_NAMES,
        time_index_name="time_index",
        columns=["value"],
    )

    assert list(df.columns) == ["time_index", "account_uid", "asset_uid", "value"]


def test_duckdb_read_supports_canonical_dimension_filters(tmp_path):
    interface = _interface(tmp_path)
    _seed_n_dimensional_table(interface)

    df = interface.read(
        table="node",
        index_names=INDEX_NAMES,
        time_index_name="time_index",
        dimension_filters={"account_uid": ["acct-a"]},
    )

    assert set(df["value"]) == {10.0, 30.0}


def test_duckdb_read_supports_index_coordinates(tmp_path):
    interface = _interface(tmp_path)
    _seed_n_dimensional_table(interface)

    df = interface.read(
        table="node",
        index_names=INDEX_NAMES,
        time_index_name="time_index",
        index_coordinates=[{"account_uid": "acct-b", "asset_uid": "asset-1"}],
    )

    assert df["value"].tolist() == [20.0]


def test_duckdb_read_supports_dimension_range_map(tmp_path):
    interface = _interface(tmp_path)
    _seed_n_dimensional_table(interface)

    df = interface.read(
        table="node",
        index_names=INDEX_NAMES,
        time_index_name="time_index",
        dimension_range_map=[
            {
                "coordinate": {"account_uid": "acct-a", "asset_uid": "asset-2"},
                "start_date": _dt(1),
                "start_date_operand": ">=",
            }
        ],
    )

    assert df["value"].tolist() == [30.0]


def test_duckdb_rejects_legacy_unique_identifier_range_map_for_n_dimensional_tables(
    tmp_path,
):
    interface = _interface(tmp_path)
    _seed_n_dimensional_table(interface)

    with pytest.raises(ValueError, match="unique_identifier_range_map"):
        interface.read(
            table="node",
            index_names=INDEX_NAMES,
            time_index_name="time_index",
            unique_identifier_range_map={"asset-1": {}},
        )


def test_duckdb_insert_uses_existing_update_key_metadata_without_storage_config_lookup(
    monkeypatch,
):
    calls = {}

    class FakeDuckDBInterface:
        def upsert(self, **kwargs):
            calls["upsert"] = kwargs

    class Storage:
        storage_hash = "storage-hash"

        @property
        def time_indexed_profile(self):
            raise AssertionError("write hot path should not fetch time-indexed profile")

    monkeypatch.setattr(models_metatables, "_duckdb_interface", lambda: FakeDuckDBInterface())

    data_source = models_tdag.DataSource.model_construct(class_type=models_tdag.DUCK_DB)
    update = SimpleNamespace(data_node_storage=Storage())
    df = pd.DataFrame(
        {
            "time_index": [_dt(0)],
            "account_uid": ["acct-a"],
            "asset_uid": ["asset-1"],
            "value": [10.0],
        }
    )

    data_source.insert_data_into_table(
        serialized_data_frame=df,
        data_node_update=update,
        overwrite=True,
        time_index_name="time_index",
        index_names=INDEX_NAMES,
        grouped_dates={},
    )

    assert calls["upsert"] == {
        "df": df,
        "table": "storage-hash",
        "index_names": INDEX_NAMES,
        "time_index_name": "time_index",
    }


def test_duckdb_read_dispatch_uses_adjusted_constrain_read_outputs(monkeypatch):
    calls = {}
    adjusted_range_map = [
        {
            "coordinate": {"account_uid": "acct-a", "asset_uid": "asset-1"},
            "start_date": _dt(0),
            "end_date": _dt(1),
        }
    ]

    class FakeDuckDBInterface:
        def constrain_read(self, **kwargs):
            calls["constrain_read"] = kwargs
            return _dt(0), _dt(1), adjusted_range_map, {"limited": True}

        def read(self, **kwargs):
            calls["read"] = kwargs
            return pd.DataFrame(
                {
                    "time_index": [_dt(0)],
                    "account_uid": ["acct-a"],
                    "asset_uid": ["asset-1"],
                    "value": [10.0],
                }
            )

    stc = SimpleNamespace(
        index_names=INDEX_NAMES,
        time_index_name="time_index",
        column_dtypes_map={
            "time_index": "datetime64[ns, UTC]",
            "account_uid": "object",
            "asset_uid": "object",
            "value": "float64",
        },
    )
    update = SimpleNamespace(
        update_hash="update-hash",
        data_node_storage=SimpleNamespace(
            storage_hash="storage-hash",
            time_indexed_profile=stc,
        ),
    )
    data_source = models_tdag.DataSource.model_construct(class_type=models_tdag.DUCK_DB)

    monkeypatch.setattr(models_metatables, "_duckdb_interface", lambda: FakeDuckDBInterface())

    original_range_map = [
        {
            "coordinate": {"account_uid": "acct-a", "asset_uid": "asset-1"},
            "start_date": _dt(0),
        }
    ]
    result = data_source.get_data_by_time_index(
        data_node_update=update,
        start_date=_dt(0),
        end_date=_dt(3),
        dimension_range_map=original_range_map,
        columns=["value"],
    )

    assert calls["constrain_read"]["index_names"] == INDEX_NAMES
    assert calls["constrain_read"]["time_index_name"] == "time_index"
    assert calls["constrain_read"]["dimension_range_map"] == original_range_map
    assert calls["read"]["start"] == _dt(0)
    assert calls["read"]["end"] == _dt(1)
    assert calls["read"]["dimension_range_map"] == adjusted_range_map
    assert calls["read"]["index_names"] == INDEX_NAMES
    assert calls["read"]["time_index_name"] == "time_index"
    assert result.index.names == INDEX_NAMES
    assert result["value"].tolist() == [10.0]
