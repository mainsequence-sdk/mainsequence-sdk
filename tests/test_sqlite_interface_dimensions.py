import datetime
import os
from types import SimpleNamespace

import pandas as pd
import pytest

from mainsequence.client import models_tdag
from mainsequence.client.data_sources_interfaces.sqlite import SQLiteInterface

INDEX_NAMES = ["time_index", "account_uid", "asset_uid"]


def _dt(hour: int) -> datetime.datetime:
    return datetime.datetime(2026, 5, 25, hour, tzinfo=datetime.UTC)


def _interface(tmp_path) -> SQLiteInterface:
    os.environ["DO_NOT_CHECK_TDAG"] = "true"
    return SQLiteInterface(db_path=tmp_path / "sqlite")


def _seed_n_dimensional_table(interface: SQLiteInterface, table: str = "node") -> None:
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


def test_sqlite_upsert_preserves_n_dimensional_identity(tmp_path):
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


def test_sqlite_local_backend_has_physical_unique_index_but_no_fk_constraints(tmp_path):
    interface = _interface(tmp_path)
    _seed_n_dimensional_table(interface)

    indexes = interface.con.execute('PRAGMA index_list("node")').fetchall()
    unique_index_names = [row["name"] for row in indexes if row["unique"]]

    assert unique_index_names
    assert any(
        [
            row["name"]
            for row in interface.con.execute(f'PRAGMA index_info("{index_name}")').fetchall()
        ]
        == INDEX_NAMES
        for index_name in unique_index_names
    )
    assert interface.con.execute('PRAGMA foreign_key_list("node")').fetchall() == []


def test_sqlite_upsert_rewrites_only_matching_full_index_tuple(tmp_path):
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


def test_sqlite_column_subset_read_includes_all_index_columns(tmp_path):
    interface = _interface(tmp_path)
    _seed_n_dimensional_table(interface)

    df = interface.read(
        table="node",
        index_names=INDEX_NAMES,
        time_index_name="time_index",
        columns=["value"],
    )

    assert list(df.columns) == ["time_index", "account_uid", "asset_uid", "value"]


def test_sqlite_read_supports_canonical_dimension_filters(tmp_path):
    interface = _interface(tmp_path)
    _seed_n_dimensional_table(interface)

    df = interface.read(
        table="node",
        index_names=INDEX_NAMES,
        time_index_name="time_index",
        dimension_filters={"account_uid": ["acct-a"]},
    )

    assert set(df["value"]) == {10.0, 30.0}


def test_sqlite_read_supports_index_coordinates(tmp_path):
    interface = _interface(tmp_path)
    _seed_n_dimensional_table(interface)

    df = interface.read(
        table="node",
        index_names=INDEX_NAMES,
        time_index_name="time_index",
        index_coordinates=[{"account_uid": "acct-b", "asset_uid": "asset-1"}],
    )

    assert df["value"].tolist() == [20.0]


def test_sqlite_read_supports_dimension_range_map(tmp_path):
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


def test_sqlite_rejects_legacy_unique_identifier_range_map_for_n_dimensional_tables(
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


def test_sqlite_time_index_minima_groups_by_identity_coordinates(tmp_path):
    interface = _interface(tmp_path)
    _seed_n_dimensional_table(interface)

    global_min, per_coordinate = interface.time_index_minima(
        table="node",
        index_names=INDEX_NAMES,
        time_index_name="time_index",
    )

    assert global_min == pd.Timestamp(_dt(0))
    assert per_coordinate == {
        ("acct-a", "asset-1"): pd.Timestamp(_dt(0)),
        ("acct-b", "asset-1"): pd.Timestamp(_dt(0)),
        ("acct-a", "asset-2"): pd.Timestamp(_dt(1)),
    }


def test_sqlite_data_source_dispatch_uses_local_interface(monkeypatch):
    calls = {}

    class FakeSQLiteInterface:
        def upsert(self, **kwargs):
            calls["upsert"] = kwargs

    monkeypatch.setattr(models_tdag, "_sqlite_interface", lambda: FakeSQLiteInterface())

    data_source = models_tdag.DataSource.model_construct(class_type=models_tdag.SQLITE)
    update = SimpleNamespace(data_node_storage=SimpleNamespace(storage_hash="storage-hash"))
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


def test_sqlite_read_dispatch_uses_adjusted_constrain_read_outputs(monkeypatch):
    calls = {}
    adjusted_range_map = [
        {
            "coordinate": {"account_uid": "acct-a", "asset_uid": "asset-1"},
            "start_date": _dt(0),
            "end_date": _dt(1),
        }
    ]

    class FakeSQLiteInterface:
        def constrain_read(self, **kwargs):
            calls["constrain_read"] = kwargs
            return _dt(0), _dt(1), adjusted_range_map, {"limited": False}

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
            sourcetableconfiguration=stc,
        ),
    )
    data_source = models_tdag.DataSource.model_construct(class_type=models_tdag.SQLITE)

    monkeypatch.setattr(models_tdag, "_sqlite_interface", lambda: FakeSQLiteInterface())

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
