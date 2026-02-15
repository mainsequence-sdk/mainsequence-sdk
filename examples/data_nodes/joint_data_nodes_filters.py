import datetime as dt

from mainsequence.client.models_tdag import DataNodeStorage
from mainsequence.tdag.data_nodes.data_filters import (
    F,
    JoinKey,
    JoinSpec,
    JoinType,
    SearchRequest,
    and_,
)

# August 2025 UTC window
start = dt.datetime(2025, 8, 1, 0, 0, 0, tzinfo=dt.UTC)
end   = dt.datetime(2025, 8, 31, 23, 59, 59, tzinfo=dt.UTC)

req = SearchRequest(
    node_unique_identifier="alpaca_1d_bars",
    joins=[
        JoinSpec(
            name="polygon_historical_marketcap",
            node_unique_identifier="polygon_historical_marketcap",
            type=JoinType.inner,
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
