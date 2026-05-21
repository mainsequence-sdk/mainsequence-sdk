# DataNode Migrations

This guide covers the DataNode update and query migration to the multidimensional
index contract.

The core rule is simple:

```python
index_names = [time_index_name, *identity_dimensions]
identity_dimensions = index_names[1:]
```

Only the first index is special. It is the UTC time index. Every remaining
index is an identity dimension.

## What changed

DataNodes are no longer limited to:

```python
["time_index"]
["time_index", "unique_identifier"]
```

They can now use higher-dimensional identity keys, for example:

```python
["time_index", "account_uid", "unique_identifier"]
```

The full index tuple is the uniqueness key. For account holdings, one row is
identified by time, account, and asset.

## Method changes to make

Update existing call sites to use dimension-aware names:

| Area | Use now |
| --- | --- |
| table reads | `dimension_filters`, `index_coordinates`, or `dimension_range_map` |
| latest reads | `dimension_filters` or `index_coordinates` |
| tail deletes | `dimension_filters` or `index_coordinates` |
| update statistics | `global_index_progress`, `index_progress`, and `index_min` |
| joins and search | full `JoinSpec.on` index vectors |

For asset tables, this read:

```python
df = node.get_df_between_dates(
    dimension_filters={"unique_identifier": ["BTC", "ETH"]},
)
```

is the two-index version of the same contract used by three-index tables:

```python
df = node.get_df_between_dates(
    dimension_filters={
        "account_uid": ["account-a"],
        "unique_identifier": ["BTC", "ETH"],
    },
)
```

## Source configuration

Creation payloads should still send the minimal table intent:

```python
{
    "time_index_name": "time_index",
    "index_names": ["time_index", "account_uid", "unique_identifier"],
    "column_dtypes_map": {
        "time_index": "datetime64[ns, UTC]",
        "account_uid": "uuid",
        "unique_identifier": "object",
        "quantity": "decimal",
    },
}
```

The backend derives `storage_layout` and `physical_index_plan`. Client code
should read those fields from source-configuration responses when it needs the
persisted layout or physical indexes.

Do not build new code around `table_partition`.

## Update statistics

Use canonical update-statistics fields:

```python
stats.global_index_progress
stats.index_progress
stats.index_min
stats.max_time_index_value
```

`max_time_index_value` remains a scalar projection of
`global_index_progress["max"]`.

Update-statistics payload builders should emit only the canonical keys accepted
by the backend:

```python
{
    "global_index_progress": {
        "max": "2026-05-01 03:00:00+00:00",
        "min": "2026-05-01 00:00:00+00:00",
    },
    "index_progress": {
        "account-a": {
            "BTC": "2026-05-01 02:00:00+00:00",
        }
    },
    "index_min": {
        "account-a": {
            "BTC": "2026-05-01 00:00:00+00:00",
        }
    },
    "multi_index_column_stats": {},
}
```

The equivalent compressed form is:

```python
{
    "multi_index_stats": {
        "_GLOBAL_": {
            "max": "2026-05-01 03:00:00+00:00",
            "min": "2026-05-01 00:00:00+00:00",
        },
        "index_progress": {
            "account-a": {
                "BTC": "2026-05-01 02:00:00+00:00",
            }
        },
        "index_min": {
            "account-a": {
                "BTC": "2026-05-01 00:00:00+00:00",
            }
        },
    },
    "multi_index_column_stats": {},
}
```

For a two-index asset table, progress is keyed by asset:

```python
{
    "BTC": "2026-05-01T02:00:00Z",
    "ETH": "2026-05-01T03:00:00Z",
}
```

For a three-index account holdings table, progress is nested by the identity
dimensions:

```python
{
    "account-a": {
        "BTC": "2026-05-01T02:00:00Z",
    }
}
```

Build incremental range reads from the identity dimensions:

```python
range_map = update_statistics.get_dimension_range_map_great_or_equal(
    identity_dimensions=["account_uid", "unique_identifier"],
)

previous_rows = self.get_df_between_dates(dimension_range_map=range_map)
```

## Reads

Use `dimension_filters` for ordinary dimension filtering:

```python
df = node.get_df_between_dates(
    start_date="2026-05-01T00:00:00Z",
    end_date="2026-05-20T00:00:00Z",
    dimension_filters={
        "account_uid": ["account-a"],
        "unique_identifier": ["BTC", "ETH"],
    },
)
```

Use `index_coordinates` when you want exact identity coordinates:

```python
latest = storage.get_last_observation(
    index_coordinates=[
        {"account_uid": "account-a", "unique_identifier": "BTC"},
    ],
)
```

Use `dimension_range_map` when each coordinate needs its own time window:

```python
df = node.get_df_between_dates(
    dimension_range_map=[
        {
            "coordinate": {"account_uid": "account-a", "unique_identifier": "BTC"},
            "start_date": "2026-05-01T00:00:00Z",
            "end_date": "2026-05-20T00:00:00Z",
        }
    ],
)
```

## Tail delete

Use the same canonical dimension shape when deleting the tail of a table.

```python
result = storage.delete_after_date(
    "2026-05-01T00:00:00Z",
    index_coordinates=[
        {"account_uid": "account-a", "unique_identifier": "BTC"},
    ],
)
```

Use `dimension_filters` when the same cutoff applies to a broader slice:

```python
result = storage.delete_after_date(
    "2026-05-01T00:00:00Z",
    dimension_filters={
        "account_uid": ["account-a"],
        "unique_identifier": ["BTC", "ETH"],
    },
)
```

## Search and joins

When joining DataNode tables, `JoinSpec.on` should use the full index vector
shared by the tables.

```python
from mainsequence.tdag.data_nodes.filters import JoinSpec, SearchRequest

request = SearchRequest(
    node_unique_identifier="account_holdings",
    joins=[
        JoinSpec(
            name="other_holdings",
            node_unique_identifier="other_account_holdings",
            on=["time_index", "account_uid", "unique_identifier"],
        )
    ],
)
```

The client restores the response index from the join keys returned by the
server.

## Account holdings example

The account holdings DataNodes in `mainsequence.markets.accounts.data_nodes`
are the concrete three-index example.

```python
from mainsequence.markets.accounts.data_nodes import (
    ACCOUNT_HOLDINGS_INDEX_NAMES,
    AccountHoldings,
)

config = AccountHoldings.default_config(
    identifier="broker.account_holdings",
    description="Broker account holdings imported from daily files.",
)
assert config.index_names == ACCOUNT_HOLDINGS_INDEX_NAMES == [
    "time_index",
    "account_uid",
    "unique_identifier",
]


class BrokerAccountHoldings(AccountHoldings):
    def get_holdings_frame(self):
        return self.build_schema_bootstrap_account_frame(config=self.config)


node = BrokerAccountHoldings(config=config)
frame = node.update()
assert list(frame.index.names) == config.index_names
```

This table is keyed by observation time, account, and asset. That shape is the
normal multidimensional DataNode contract.

Use `AccountHoldings.default_config(...)` to set the public DataNode identifier
or description. If the account holdings import needs extra provider-specific
columns, add them through `extra_records` and make sure `get_holdings_frame()`
returns those columns in addition to the required holdings schema.
