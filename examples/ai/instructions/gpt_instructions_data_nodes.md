# DataNodes — Authoring Guide (Complete & Correct)

> This is the canonical guide for building **DataNodes** in MainSequence with the mainsequence library.  
> It clearly separates **MUST** vs **SHOULD**, supports **single-index** *or* **MultiIndex** tables, and includes a practical, copy‑pasteable template.  
> **Note:** the base `DataNode` already performs **index/column validation and sanitization after `update()`** — do **not** call your own validators or sanitizers inside `update()`.

---

## Importing a DataNode

```python
from mainsequence.tdag import DataNode
```

## What is a DataNode?

A **DataNode** is a unit of computation in the MainSequence DAG. Each node:

- Declares its **dependencies()** on other nodes.
- Computes and returns a **`pandas.DataFrame`** in **`update()`**.
- Optionally exposes **metadata** for better discovery & documentation.
- Follows strict index/column rules so storage and downstream nodes stay consistent.

---

## MUST (Hard Requirements)

1. **Implement `dependencies()` and `update()`**.  
   If there are no dependencies, **return `{}`** (do **not** use `pass`).

2. **Output index** (pick one):  
   - **Single-index**: `pandas.DatetimeIndex` named **`time_index`** (timezone-aware **UTC**).  
   - **MultiIndex**: first level **`time_index`** (UTC), second level **`unique_identifier`**; any deeper levels are allowed but **must be named**.

3. **No datetime columns** in the DataFrame (dates live only in the index) for date information store them as timestamps.

4. **Columns** must be **lowercase**, **≤ 63 characters**, and reasonably stable (avoid renaming).

5. **Deterministic identity & hashing**:  
   - Constructor args define the node’s identity (storage/update hashes).  
   - Use **`_ARGS_IGNORE_IN_STORAGE_HASH`** for args that should **not** affect the storage hash (e.g., transient inputs).
   - When a DataNode configuration is extensive and make sense the `__init__` constructor can also accept pydantic models. It is recommended to follow this path to build most of the data nodes.
   - To separate args that should **not** affect the storage hash while using pydantic models as arguments user can set the property ` ignore_from_storage_hash` bellow is an example
   ```python
   from pydantic import BaseModel
   from pydantic import BaseModel, Field
   from mainsequence.tdag import DataNode
   class VolatilityConfig(BaseModel):
        ...
   class RandomDataNodeConfig(BaseModel):
    mean: float = Field(..., ignore_from_storage_hash=False, title="Mean",
                        description="Mean for the random normal distribution generator")
    std: VolatilityConfig = Field(VolatilityConfig(center=1, skew=True), ignore_from_storage_hash=True,
                                  title="Vol Config",
                                  description="Vol Configuration")
   class DailyRandomNumber(DataNode):
    """
    Example Data Node that generates one random number every day
    """

    def __init__(self, node_configuration: RandomDataNodeConfig, *args, **kwargs):
        ...
   ```
   - Try to maximize the usage of the Field model by always include a title, description and examples if possible.

6. **Do not validate/sanitize manually** inside `update()`; the base class does this automatically when your DataFrame returns.

7. The name of the first index level must always be `time_index`, and it is strongly recommended that it represents the observation time of the time series. For example, if the DataFrame stores time bars, `time_index` should represent the moment the bar is observed, not when the bar started.

---

## SHOULD (Best Practices)

- **Docstrings & typing**: Give every class/method a clear docstring; add type hints for public APIs.
- **Idempotent `update()`**: For the same input window, produce the same output (seed randomness if needed).
- **Incremental updates**: Use `UpdateStatistics` to compute only the new slice (examples below).
- **Early exits**: If nothing to do (e.g., `start > end` or empty ranges), return an **empty DataFrame**.
- **Logging**: Log high-level progress only (avoid noisy per-row logs).
- **Small helpers**: Use small private helpers for repetitive fetch/transform logic.
- **Stable schemas**: Prefer additive changes (new columns) over breaking changes (renames/drops).
- **Pre-filter when >2 levels**: If your MultiIndex has deeper levels, call `update_statistics.filter_assets_by_level(...)` at the **start** of `update()` to keep updates consistent across levels.
- **Batch IO**: When possible, fetch dependency data **once** using a **range descriptor**, not in per-asset loops.

---

## `UpdateStatistics` — How to Use It (Field & Method Guide)

`self.update_statistics` describes **what** to update (assets, levels) and **from when**. You will use it to compute only the necessary slice.

**Common fields**
- `max_time_index_value` — last stored timestamp for **single-index** outputs. Use `last + 1` as the next start.
- `asset_list` — the assets included in this update batch (for MultiIndex tables).
- `asset_time_statistics` — “last time updated” mapping per asset (and deeper levels if present).

**Handy methods**
- `get_update_range_map_great_or_equal()`  
  Returns a **range descriptor** suitable for fetching all prior observations per asset in **one call** (e.g., everything with `>= last_seen`).
- `get_asset_earliest_multiindex_update(asset)`  
  Returns the last timestamp for that asset; often you’ll start at `last + 1 hour` or `last + 1 day`.
- `filter_assets_by_level(level, filters)`  
  For MultiIndex with **>2 levels**, restrict the update to a consistent subset at a deeper level (e.g., feature configs). Call this **at the beginning** of `update()`.

---

## `UpdateStatistics` — Patterns by Example

Below are distilled patterns adapted from the examples. Adjust as needed.

### A) Single-index series (daily) — start from `last + 1 day`

Use `max_time_index_value` to compute the next start. Generate through “yesterday 00:00 UTC”. Return a **single** `DatetimeIndex` named `time_index`.

```python
class SingleIndexTS(DataNode):
    """A simple daily time series with a single DatetimeIndex named 'time_index'."""
    OFFSET_START = datetime.datetime(2024, 1, 1, tzinfo=pytz.utc)

    def dependencies(self) -> Dict[str, Union["DataNode", "APIDataNode"]]:
        return {}

    def update(self) -> pd.DataFrame:
        import numpy as np
        today_utc = datetime.datetime.now(tz=pytz.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        us = self.update_statistics

        start_date = self.OFFSET_START if us.max_time_index_value is None else us.max_time_index_value + datetime.timedelta(days=1)
        if start_date > today_utc:
            return pd.DataFrame()

        num_days = (today_utc - start_date).days + 1
        idx = pd.date_range(start=start_date, periods=num_days, freq="D", tz=pytz.utc, name="time_index")
        return pd.DataFrame({"random_number": np.random.rand(num_days)}, index=idx)
```

### B) MultiIndex (2 levels) — per-asset simulation using prior observations

* Use get_update_range_map_great_or_equal() to fetch prior observations once.
* For each asset in asset_list, start at get_asset_earliest_multiindex_update(asset) + 1h and generate through “yesterday 00:00 UTC”.
* Reshape to long and set index to `("time_index","unique_identifier")`.
* **Important** it is never guaranteed that every asset in update_statistics.asset_list has the same last update time. Therefore,it is encouraged to always loop per asset by user the already fitlered update_statistics.asset_list.

```python
class SimulatedPricesManager:
    """Helper/manager that performs the per-asset simulation for a DataNode owner."""
    def __init__(self, owner: DataNode):
        self.owner = owner

    @staticmethod
    def _get_last_price(obs_df: pd.DataFrame, unique_id: str, fallback: float) -> float:
        """Return last close for `unique_id` from obs_df (time_index, unique_identifier), or fallback."""
        if obs_df is None or obs_df.empty:
            return fallback
        try:
            series = (
                obs_df
                .reset_index()
                .sort_values(["unique_identifier", "time_index"])
                .set_index(["time_index", "unique_identifier"])["close"]
            )
            last = series.xs(unique_id, level="unique_identifier").dropna()
            if len(last) == 0:
                return fallback
            return float(last.iloc[-1])
        except Exception:
            return fallback

    def update(self) -> pd.DataFrame:
        import numpy as np, pytz
        us = self.owner.update_statistics

        # 1) One-shot fetch of prior observations
        range_descriptor = us.get_update_range_map_great_or_equal()
        last_obs = self.owner.get_ranged_data_per_asset(range_descriptor=range_descriptor)

        # 2) Target end (yesterday 00:00 UTC)
        yday = datetime.datetime.now(pytz.utc).replace(hour=0, minute=0, second=0, microsecond=0) - datetime.timedelta(days=1)

        # 3) Simulate per asset
        frames = []
        for asset in us.asset_list:
            start_time = us.get_asset_earliest_multiindex_update(asset=asset) + datetime.timedelta(hours=1)
            if start_time > yday:
                continue

            idx = pd.date_range(start=start_time, end=yday, freq="D")
            if len(idx) == 0:
                continue

            seed_price = self._get_last_price(last_obs, unique_id=asset.unique_identifier, fallback=100.0)
            steps = np.random.lognormal(mean=0.0, sigma=0.01, size=len(idx))
            path = seed_price * np.cumprod(steps)

            tmp = pd.DataFrame({asset.unique_identifier: path}, index=idx)
            tmp.index.name = "time_index"
            frames.append(tmp)

        if not frames:
            return pd.DataFrame()

        # 4) Long-form with MultiIndex
        wide = pd.concat(frames, axis=1)
        long = wide.melt(ignore_index=False, var_name="unique_identifier", value_name="close")
        long = long.set_index("unique_identifier", append=True)  # -> index: (time_index, unique_identifier)
        return long

```

### C) MultiIndex (>2 levels) — pre-filter deeper level, compute TA features
Call filter_assets_by_level(level=2, filters=[...]) at the start of update() to restrict to a consistent set of deeper-level keys (e.g., TA configs, JSON-serialized).

Build a per-asset asset_range_descriptor with start_date = last_update - lookback.

Fetch all prices once with get_ranged_data_per_asset(range_descriptor=asset_range_descriptor).

Pivot to wide, compute TA (e.g., with pandas_ta), then reshape back to long.

```python
class FeatureStoreTA(DataNode):
    """Derives TA features from a dependent price series (MultiIndex with >2 levels)."""

    def __init__(self, asset_list: List[ms_client.Asset], ta_feature_config: List[dict], *args, **kwargs):
        self.asset_list = asset_list
        self.ta_feature_config = ta_feature_config  # e.g., [{"kind":"SMA","length":28}, {"kind":"RSI","length":21}]
        self.prices_time_serie = SimulatedPrices(asset_list=asset_list, *args, **kwargs)
        super().__init__(*args, **kwargs)

    def dependencies(self) -> Dict[str, Union["DataNode", "APIDataNode"]]:
        return {"prices_time_serie": self.prices_time_serie}

    def update(self) -> pd.DataFrame:
        import numpy as np, json, copy, pandas_ta as ta

        # (0) REQUIRED for >2 levels: pre-filter deeper level (e.g., by encoded TA config)
        self.update_statistics.filter_assets_by_level(
            level=2,
            filters=[json.dumps(c) for c in self.ta_feature_config]
        )

        us = self.update_statistics
        SIM_OFFSET = datetime.timedelta(days=50)

        # (1) Lookback window large enough for all TA configs
        max_len = int(np.max([c["length"] for c in self.ta_feature_config]).item())
        rolling_window = datetime.timedelta(days=max_len + 1)

        # (2) Build per-asset range descriptor
        asset_range_descriptor = {}
        for asset in us.asset_list:
            last_update = us.get_asset_earliest_multiindex_update(asset) - SIM_OFFSET
            start_date = last_update - rolling_window
            asset_range_descriptor[asset.unique_identifier] = {
                "start_date": start_date,
                "start_date_operand": ">=",
            }

        # (3) Fetch base prices in a single call
        prices_df = self.prices_time_serie.get_ranged_data_per_asset(range_descriptor=asset_range_descriptor)
        if prices_df is None or prices_df.empty:
            return pd.DataFrame()

        # (4) Wide for TA → then long features
        pivot = prices_df.reset_index().pivot(index="time_index", columns="unique_identifier", values="close")
        features = []
        for conf in self.ta_feature_config:
            conf_copy = copy.deepcopy(conf)          # keep original intact
            kind = conf_copy.pop("kind").lower()     # e.g., "sma", "rsi"
            func = getattr(ta, kind)                 # pandas_ta.sma, pandas_ta.rsi, ...
            wide = pivot.apply(lambda col: func(col, **conf_copy))

            # long: (time, uid) → one feature column keyed by the config
            feature_name = json.dumps(conf, sort_keys=True)
            long = pd.DataFrame({feature_name: wide.stack(dropna=False)})
            features.append(long)

        out = pd.concat(features, axis=1)  # columns are encoded feature configs
        # Base class will sanitize names & validate index. Return long or pivot to wide as your contract requires.
        return out

```

## Dependencies
Only nodes returned by dependencies() are traversed & updated as part of the DAG:

* Return a dict mapping short aliases to the dependency instances, e.g.:

  * return {"prices_time_serie": self.prices_time_serie}

  * If none, return {}.

* **Always** declare the dependency nodes first in the constructor. **Never** pass other DataNodes as arguments.
Below is an example of how to create a dependency. 
```python
class PricesConfig:
    asset_list: List[ms_client.Asset]
class PricesDataNode(DataNode):
    def __init__(self, prices_config:PricesConfig, *args, **kwargs):
        self.prices_config = prices_config
        super().__init__(*args, **kwargs)

class MyDataNode(DataNode):
    def __init__(self, prices_nodes_config:PricesConfig,*args, **kwargs):
        self.prices_ts = PRicesDataNode(prices_config=prices_nodes_config)
        super().__init__(*args, **kwargs)
    def dependencies(self) -> Dict[str, Union["DataNode", "APIDataNode"]]:
        return {"prices_ts": self.prices_ts}
```

## Optional Hooks

Override these to enrich metadata or control the asset universe:

* get_table_metadata(self) -> Optional[ms_client.TableMetaData]
Provide a globally unique identifier, data_frequency_id, and a concise description.

* get_asset_list(self) -> List[ms_client.Asset]
Define the assets dynamically (e.g., pull from a category or registry).

* get_column_metadata(self) -> List[ColumnMetaData]
Describe columns (dtype, label, description) for UI and docs.

* run_post_update_routines(self, error_on_last_update: bool)
Optional post-update side effects (e.g., building translation tables, artifacts).

## Hashing & Constructor Notes
Constructor arguments determine the identity of a node instance (its storage/update hash).

Put any non-identity or transient args in _ARGS_IGNORE_IN_STORAGE_HASH to avoid unnecessary duplication or by setting the Field property `ignore_in_storage_hash=True`.
Example: SimulatedPrices ignores "asset_list" in its storage hash.

## Canonical Template (copy/paste)
Minimal working skeleton. Choose whether you output single-index or MultiIndex in update().

```python
from typing import Dict, Union, List, Optional
import datetime, pytz, pandas as pd
import mainsequence.client as ms_client
from mainsequence.tdag import DataNode, APIDataNode
from mainsequence.client.models_tdag import UpdateStatistics, ColumnMetaData

UTC = pytz.utc

class MyDataNode(DataNode):
    """One-line purpose. Longer description of inputs/outputs & assumptions.

    Output:
        Either:
          - Single-index: DatetimeIndex named "time_index" (UTC), or
          - MultiIndex: ("time_index","unique_identifier", [level_2, ...])
        Columns are lowercase (≤63 chars). No datetime columns.
    """

    _ARGS_IGNORE_IN_STORAGE_HASH = ["asset_list"]  # adjust as needed

    def __init__(self, asset_list: Optional[List[ms_client.Asset]] = None, *args, **kwargs):
        self.asset_list = asset_list or []
        super().__init__(*args, **kwargs)

    def dependencies(self) -> Dict[str, Union["DataNode", "APIDataNode"]]:
        return {}  # e.g., {"prices": self.prices_ts}

    def get_asset_list(self) -> List[ms_client.Asset]:
        # Optional: dynamically supply the asset universe
        return self.asset_list

    def get_column_metadata(self) -> List[ColumnMetaData]:
        return [ColumnMetaData(
            column_name="my_value",
            dtype="float",
            label="My Value",
            description="Example metric"
        )]

    def get_table_metadata(self) -> Optional[ms_client.TableMetaData]:
        return ms_client.TableMetaData(
            identifier="my_timeseries_identifier",
            data_frequency_id=ms_client.DataFrequency.one_d,
            description="Explain what this series represents"
        )

    def update(self) -> pd.DataFrame:
        us: UpdateStatistics = self.update_statistics

        # 1) Compute the update window
        start = (us.max_time_index_value or datetime.datetime(2024, 1, 1, tzinfo=UTC)).replace(tzinfo=UTC)
        start = (start + datetime.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        end = datetime.datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0) - datetime.timedelta(days=1)
        if start > end:
            return pd.DataFrame()

        # 2) Example: MultiIndex output for assets in this batch
        idx = pd.date_range(start=start, end=end, freq="D", tz=UTC, name="time_index")
        frames = []
        for asset in us.asset_list:
            tmp = pd.DataFrame({"my_value": 0.0}, index=idx)
            tmp["unique_identifier"] = asset.unique_identifier
            frames.append(tmp.set_index("unique_identifier", append=True))

        return pd.concat(frames) if frames else pd.DataFrame()

```

## Runing Data Nodes

When prompted to test or run a data node, the best practice is to create a launcher file only for this node  in `/scripts` or `/tests
for example

```python
from src.data_nodes.example_nodes import DailyRandomNumber, RandomDataNodeConfig

def main():
    daily_node = DailyRandomNumber(node_configuration=RandomDataNodeConfig(mean=0.0))
    daily_node.run()

if __name__ == "__main__":
    main()
```

*When prompted to create a node be sure that all the constructor arguments are provided

## Common Pitfalls
* Wrong index names/order: For MultiIndex, the first two must be ("time_index","unique_identifier").

* Naive datetimes: The time_index must be timezone-aware UTC.

* Datetime columns: Don’t put datetimes in columns — only in the index.

* dependencies() using pass: If there are no dependencies, return {}.

* Empty updates: If there’s nothing to compute, return an empty DataFrame (don’t raise or write partials).

* Randomness: Seed or derive from deterministic inputs if you need reproducibility.

* Always be strict import libraries from mainsequence errors should be raised if the libary is not present
