import datetime as dt
import re

import pandas as pd
import pandas.testing as pdt

from mainsequence.client.data_filters import (
    F,
    JoinKey,
    JoinSpec,
    JoinType,
    SearchRequest,
    and_,
)  # tests/test_full_outer_join_filters.py
from mainsequence.client.models_tdag import DataNodeStorage

UTC = getattr(dt, "UTC", dt.UTC)

# August 2025 UTC window
START = dt.datetime(2025, 8, 1, 0, 0, 0, tzinfo=UTC)
END = dt.datetime(2025, 8, 31, 23, 59, 59, tzinfo=UTC)

# This FIGI appears in both screenshots you shared:
# - alpaca_1d_bars includes BBG000L4M7F1
# - polygon_historical_marketcap includes BBG000L4M7F1
FIGI_SAMPLE = "BBG000L4M7F1"

_FIGI_RE = re.compile(r"^[A-Z0-9]{12}$")


def build_full_outer_request(*, figi: str | None = None, join_side_only: bool = False) -> SearchRequest:
    """
    Build the canonical FULL OUTER JOIN request:
      alpaca_1d_bars FULL OUTER JOIN polygon_historical_marketcap
      ON time_index + unique_identifier
    """
    filters = [F.between("time_index", START, END)]

    if figi:
        if join_side_only:
            # Filter on the join-side field specifically (alias-prefixed filter field)
            filters.append(F.eq("polygon_historical_marketcap.unique_identifier", figi))
        else:
            filters.append(F.eq("unique_identifier", figi))

    return SearchRequest(
        node_unique_identifier="alpaca_1d_bars",
        joins=[
            JoinSpec(
                name="polygon_historical_marketcap",
                node_unique_identifier="polygon_historical_marketcap",
                type=JoinType.full,
                on=[JoinKey.time_index, JoinKey.unique_identifier],
            )
        ],
        filter=and_(*filters),
        offset=0,
    )


def test_request_serialization_iso_datetimes():
    """
    Pure client-side unit sanity:
    datetimes must serialize as ISO strings in the request payload.
    """
    req = build_full_outer_request(figi=FIGI_SAMPLE)
    payload = req.model_dump(mode="json", exclude_none=True)

    assert payload["node_unique_identifier"] == "alpaca_1d_bars"
    assert payload["joins"][0]["name"] == "polygon_historical_marketcap"
    assert payload["joins"][0]["type"] == "full"
    assert payload["joins"][0]["on"] == ["time_index", "unique_identifier"]

    # Make sure our between values are ISO strings
    expr = payload["filter"]
    assert expr["type"] == "group"
    between = expr["items"][0]
    assert between["op"] == "between"
    assert isinstance(between["value"][0], str)
    assert isinstance(between["value"][1], str)


def test_full_outer_join_schema_and_index():
    req = build_full_outer_request()
    df = DataNodeStorage.get_data_from_filter(req, batch_limit=14000)

    assert isinstance(df, pd.DataFrame)
    assert not df.empty

    # Keys are expected to be the index (backend tells client it's unambiguous)
    assert list(df.index.names) == ["time_index", "unique_identifier"]

    # Must NOT have prefixed key columns anymore
    forbidden = {
        "base__time_index",
        "base__unique_identifier",
        "polygon_historical_marketcap__time_index",
        "polygon_historical_marketcap__unique_identifier",
    }
    assert forbidden.isdisjoint(set(df.columns))

    # Must have some base columns + some join columns
    # (exact set depends on your table schemas, but these are shown in your screenshots)
    expected_some = [
        "base__open_time",
        "base__open",
        "base__high",
        "base__low",
        "base__close",
        "base__volume",
        "polygon_historical_marketcap__market_cap",
        "polygon_historical_marketcap__price",
        "polygon_historical_marketcap__volume",
    ]
    missing = [c for c in expected_some if c not in df.columns]
    assert not missing, f"Missing expected columns: {missing}"


def test_dtype_parsing_for_known_columns():
    req = build_full_outer_request(figi=FIGI_SAMPLE)
    df = DataNodeStorage.get_data_from_filter(req, batch_limit=14000)

    assert not df.empty
    assert list(df.index.names) == ["time_index", "unique_identifier"]

    # Index dtype sanity
    time_level = df.index.get_level_values("time_index")
    uid_level = df.index.get_level_values("unique_identifier")

    assert pd.api.types.is_datetime64tz_dtype(time_level.dtype), f"time_index dtype was {time_level.dtype}"
    # MultiIndex stores strings as object sometimes; ensure actual values look like FIGIs
    assert all((x is pd.NA) or (x is None) or bool(_FIGI_RE.match(str(x))) for x in uid_level[:200])

    # Column dtype sanity (nullable dtypes expected because FULL OUTER JOIN introduces NULLs)
    assert pd.api.types.is_integer_dtype(df["base__open_time"].dtype), df["base__open_time"].dtype
    assert pd.api.types.is_float_dtype(df["base__open"].dtype), df["base__open"].dtype
    assert pd.api.types.is_float_dtype(df["base__high"].dtype), df["base__high"].dtype
    assert pd.api.types.is_float_dtype(df["base__low"].dtype), df["base__low"].dtype
    assert pd.api.types.is_float_dtype(df["base__close"].dtype), df["base__close"].dtype

    # Volume might be integer or float depending on upstream; prefer integer, but accept float if you chose it
    assert (
        pd.api.types.is_integer_dtype(df["base__volume"].dtype)
        or pd.api.types.is_float_dtype(df["base__volume"].dtype)
    ), df["base__volume"].dtype

    assert pd.api.types.is_float_dtype(df["polygon_historical_marketcap__market_cap"].dtype)
    assert pd.api.types.is_float_dtype(df["polygon_historical_marketcap__price"].dtype)
    assert (
        pd.api.types.is_integer_dtype(df["polygon_historical_marketcap__volume"].dtype)
        or pd.api.types.is_float_dtype(df["polygon_historical_marketcap__volume"].dtype)
    )


def test_full_outer_join_contains_rows_from_both_sides_for_figi():
    """
    Because alpaca bars use time_index like 20:00:00Z and polygon marketcap uses 00:00:00Z,
    joining on exact (time_index, unique_identifier) should yield:
      - rows where base columns exist but join columns are NULL
      - rows where join columns exist but base columns are NULL
    """
    req = build_full_outer_request(figi=FIGI_SAMPLE)
    df = DataNodeStorage.get_data_from_filter(req, batch_limit=14000)

    assert not df.empty

    base_has = df["base__open_time"].notna()
    join_has = df["polygon_historical_marketcap__market_cap"].notna()

    assert base_has.any(), "Expected at least one alpaca_1d_bars row for this FIGI in Aug 2025."
    assert join_has.any(), "Expected at least one polygon_historical_marketcap row for this FIGI in Aug 2025."

    assert (base_has & ~join_has).any(), "Expected at least one base-only row (join NULLs)."
    assert (~base_has & join_has).any(), "Expected at least one join-only row (base NULLs)."


def test_filter_on_join_side_only_returns_join_rows():
    """
    If we filter on polygon_historical_marketcap.unique_identifier,
    we should get rows where join-side data exists (market_cap not null).
    """
    req = build_full_outer_request(figi=FIGI_SAMPLE, join_side_only=True)
    df = DataNodeStorage.get_data_from_filter(req, batch_limit=14000)

    assert not df.empty
    assert df["polygon_historical_marketcap__market_cap"].notna().all()


def test_streaming_pagination_consistency_small_vs_large_batch_limit():
    """
    Ensures client streaming pagination produces identical data regardless of batch_limit.
    """
    req = build_full_outer_request(figi=FIGI_SAMPLE)

    df_small = DataNodeStorage.get_data_from_filter(req, batch_limit=10).sort_index()
    df_large = DataNodeStorage.get_data_from_filter(req, batch_limit=14000).sort_index()

    # Same shape and same data. We allow column order differences (check_like=True).
    pdt.assert_frame_equal(df_small, df_large, check_like=True)


# August 2025 UTC window
start = dt.datetime(2025, 8, 1, 0, 0, 0, tzinfo=dt.UTC)
end   = dt.datetime(2025, 8, 31, 23, 59, 59, tzinfo=dt.UTC)

req = SearchRequest(
    node_unique_identifier="alpaca_1d_bars",
    joins=[
        JoinSpec(
            name="polygon_historical_marketcap",
            node_unique_identifier="polygon_historical_marketcap",
            type=JoinType.full,
            on=[JoinKey.time_index, JoinKey.unique_identifier],
        )
    ],
    filter=and_(
        F.between("time_index", start, end),
        # optional FIGI filter example:
        # F.eq("unique_identifier", "BBG000BLNNH6"),
    ),
    offset=0,
    # limit can be omitted; the method sets it each request anyway
)

df = DataNodeStorage.get_data_from_filter(req, batch_limit=14000)


a=5
