import pandas as pd
import pytest

from mainsequence.client import models_tdag
from mainsequence.tdag.data_nodes.filters import JoinKey, JoinSpec, SearchRequest


def _source_config(index_names: list[str]) -> models_tdag.SourceTableConfiguration:
    column_dtypes_map = {
        "time_index": "datetime64[ns, UTC]",
        "value": "float64",
    }
    column_dtypes_map.update({name: "object" for name in index_names[1:]})
    return models_tdag.SourceTableConfiguration(
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


def _storage(index_names: list[str], *, storage_hash: str = "prices_hash") -> models_tdag.DataNodeStorage:
    return models_tdag.DataNodeStorage(
        uid="714",
        storage_hash=storage_hash,
        data_source=1,
        source_class_name="PricesNode",
        creation_date="2026-04-01T00:00:00Z",
        organization_owner=1,
        sourcetableconfiguration=_source_config(index_names),
    )


def _search_request(index_names: list[str]) -> SearchRequest:
    return SearchRequest(
        node_unique_identifier="base",
        joins=[
            JoinSpec(
                name="joined",
                node_unique_identifier="joined",
                on=index_names,
            )
        ],
    )


def _joined_response_frame(index_names: list[str]) -> pd.DataFrame:
    data = {
        "time_index": ["2026-05-01T03:00:00Z"],
        "base__value": ["101.5"],
        "joined__value": ["99.25"],
    }
    if "account_uid" in index_names:
        data["account_uid"] = ["account-a"]
    if "unique_identifier" in index_names:
        data["unique_identifier"] = ["BTC"]
    return pd.DataFrame(data, dtype=object)


def test_search_request_allows_three_index_join_keys():
    req = SearchRequest(
        node_unique_identifier="base",
        joins=[
            JoinSpec(
                name="joined",
                node_unique_identifier="joined",
                on=[JoinKey.time_index, "account_uid", "entity_uid"],
            )
        ],
    )

    assert req.joins[0].on == ["time_index", "account_uid", "entity_uid"]
    assert req.model_dump(mode="json", exclude_none=True)["joins"][0]["on"] == [
        "time_index",
        "account_uid",
        "entity_uid",
    ]


def test_search_request_allows_configured_time_index_name():
    req = SearchRequest(
        node_unique_identifier="base",
        joins=[
            JoinSpec(
                name="joined",
                node_unique_identifier="joined",
                on=["timestamp", "account_uid"],
            )
        ],
    )

    assert req.joins[0].on == ["timestamp", "account_uid"]


@pytest.mark.parametrize(
    ("join_on", "match"),
    [
        (["time_index", "account_uid", "account_uid"], "duplicate"),
        (["time_index", ""], "empty"),
        ([], "time-first index vector"),
    ],
)
def test_search_request_rejects_invalid_join_index_vectors(join_on, match):
    with pytest.raises(ValueError, match=match):
        SearchRequest(
            node_unique_identifier="base",
            joins=[
                JoinSpec(
                    name="joined",
                    node_unique_identifier="joined",
                    on=join_on,
                )
            ],
        )


def test_apply_dtypes_from_meta_restores_two_index_join_response():
    index_names = ["time_index", "unique_identifier"]

    df = models_tdag.DataNodeStorage._apply_dtypes_from_meta(
        _joined_response_frame(index_names),
        data_node_storage_map={
            "base": _storage(index_names, storage_hash="base_hash"),
            "joined": _storage(index_names, storage_hash="joined_hash"),
        },
        filter_request=_search_request(index_names),
        response_index_names=index_names,
    )

    assert list(df.index.names) == index_names
    assert isinstance(df.index.get_level_values("time_index").dtype, pd.DatetimeTZDtype)
    assert df.index.get_level_values("unique_identifier")[0] == "BTC"
    assert pd.api.types.is_float_dtype(df["base__value"].dtype)
    assert pd.api.types.is_float_dtype(df["joined__value"].dtype)


def test_apply_dtypes_from_meta_restores_three_index_join_response():
    index_names = ["time_index", "account_uid", "unique_identifier"]

    df = models_tdag.DataNodeStorage._apply_dtypes_from_meta(
        _joined_response_frame(index_names),
        data_node_storage_map={
            "base": _storage(index_names, storage_hash="base_hash"),
            "joined": _storage(index_names, storage_hash="joined_hash"),
        },
        filter_request=_search_request(index_names),
        response_index_names=index_names,
    )

    assert list(df.index.names) == index_names
    assert isinstance(df.index.get_level_values("time_index").dtype, pd.DatetimeTZDtype)
    assert df.index.get_level_values("account_uid")[0] == "account-a"
    assert df.index.get_level_values("unique_identifier")[0] == "BTC"
    assert pd.api.types.is_float_dtype(df["base__value"].dtype)
    assert pd.api.types.is_float_dtype(df["joined__value"].dtype)


def test_get_data_from_filter_uses_server_returned_index_names(monkeypatch):
    index_names = ["time_index", "account_uid", "unique_identifier"]
    captured_payloads = []

    class FakeResponse:
        status_code = 200
        text = ""

        @staticmethod
        def json():
            return {
                "data_node_storage_map": {
                    "base": _storage(index_names, storage_hash="base_hash").model_dump(mode="json")
                },
                "index_names": {"0": "time_index", "1": "account_uid", "2": "unique_identifier"},
                "results": [
                    {
                        "time_index": "2026-05-01T03:00:00Z",
                        "account_uid": "account-a",
                        "unique_identifier": "BTC",
                        "base__value": "101.5",
                    }
                ],
                "next_offset": None,
            }

    def _fake_make_request(*, s, loaders, payload, r_type, url):
        captured_payloads.append(payload["json"])
        return FakeResponse()

    monkeypatch.setattr(models_tdag, "make_request", _fake_make_request)
    monkeypatch.setattr(
        models_tdag.DataNodeStorage,
        "build_session",
        classmethod(lambda cls: object()),
    )

    df = models_tdag.DataNodeStorage.get_data_from_filter(
        SearchRequest(node_unique_identifier="base"),
        batch_limit=10,
    )

    assert captured_payloads[0]["limit"] == 10
    assert list(df.index.names) == index_names
    assert df.index.get_level_values("account_uid")[0] == "account-a"
    assert df.index.get_level_values("unique_identifier")[0] == "BTC"
