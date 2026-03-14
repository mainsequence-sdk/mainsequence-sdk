# Part 3: Multi-Index Columns and Working with Assets

In Part 2, you built a basic `DataNode`. In this part, you will build an asset-based `DataNode` that stores simulated security prices in a two-level `MultiIndex` DataFrame.

You can reuse this pattern for prices, signals, news, or any other asset-centric dataset.

For the broader design rules behind this tutorial, see the [Data Nodes knowledge guide](../knowledge/data_nodes.md).

## What you will build

In this part you will:

- create a `SimulatedPrices` `DataNode` that returns a `(time_index, unique_identifier)` `MultiIndex`
- expose the asset universe through `get_asset_list()`
- run two updater jobs that write to the same underlying table
- understand why `asset_list` should usually affect `update_hash`, not `storage_hash`

## Mental model first: table identity vs updater identity

Before writing code, keep these concepts separate:

- `storage_hash` identifies the dataset contract, meaning the table that downstream users read.
- `update_hash` identifies the updater job, meaning one process that writes into that table.

That distinction is what allows multiple jobs to write safely into the same dataset.

For this tutorial:

- the table identifier in `get_table_metadata()` names the dataset
- the `unique_identifier` in the `MultiIndex` names each asset row
- `asset_list` is updater scope, so it should usually be ignored from `storage_hash`

## Create `src/data_nodes/prices_nodes.py`

Create a file at `src\data_nodes\prices_nodes.py` (Windows) or `src/data_nodes/prices_nodes.py` (macOS/Linux) and add the following code:

```python
import datetime
from typing import Union

import numpy as np
import pandas as pd
import pytz
from pydantic import BaseModel, Field

import mainsequence.client as msc
from mainsequence.tdag import APIDataNode, DataNode

SIMULATED_PRICES_IDENTIFIER = "simulated_prices_tutorial"


class SimulatedPricesManager:
    def __init__(self, owner: DataNode):
        self.owner = owner

    @staticmethod
    def _get_last_price(obs_df: pd.DataFrame, unique_id: str, fallback: float) -> float:
        if obs_df.empty:
            return fallback

        try:
            slice_df = obs_df.xs(unique_id, level="unique_identifier")["close"]
            return float(slice_df.iloc[-1])
        except (KeyError, IndexError):
            return fallback

    def update(self) -> pd.DataFrame:
        initial_price = 100.0
        mu = 0.0
        sigma = 0.01

        update_statistics = self.owner.update_statistics
        asset_list = update_statistics.asset_list or self.owner.get_asset_list() or []
        if not asset_list:
            return pd.DataFrame()

        range_descriptor = update_statistics.get_update_range_map_great_or_equal()
        last_observation = self.owner.get_ranged_data_per_asset(
            range_descriptor=range_descriptor
        )

        yesterday_midnight = datetime.datetime.now(pytz.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        ) - datetime.timedelta(days=1)

        df_list: list[pd.DataFrame] = []
        for asset in asset_list:
            last_update = update_statistics.get_asset_earliest_multiindex_update(asset=asset)
            start_time = (last_update + datetime.timedelta(days=1)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            if start_time > yesterday_midnight:
                continue

            time_range = pd.date_range(
                start=start_time,
                end=yesterday_midnight,
                freq="D",
                tz=pytz.utc,
            )
            if time_range.empty:
                continue

            last_price = self._get_last_price(
                obs_df=last_observation,
                unique_id=asset.unique_identifier,
                fallback=initial_price,
            )
            random_returns = np.random.lognormal(mean=mu, sigma=sigma, size=len(time_range))
            simulated_prices = last_price * np.cumprod(random_returns)

            df_asset = pd.DataFrame(
                {
                    "close": simulated_prices,
                    "unique_identifier": asset.unique_identifier,
                },
                index=time_range,
            )
            df_list.append(df_asset)

        if not df_list:
            return pd.DataFrame()

        data = pd.concat(df_list).sort_index()
        data.index.name = "time_index"
        data = data.set_index("unique_identifier", append=True)

        self.owner.logger.info(
            f"simulated_prices rows={len(data)} assets={len(df_list)} end={yesterday_midnight.isoformat()}"
        )
        return data


class PriceSimulConfig(BaseModel):
    asset_list: list[msc.AssetMixin] = Field(
        ...,
        title="Asset List",
        description="List of assets to simulate",
        json_schema_extra={"ignore_from_storage_hash": True},
    )


class SimulatedPrices(DataNode):
    """Simulates daily close prices for a fixed batch of assets."""

    OFFSET_START = datetime.datetime(2024, 1, 1, tzinfo=pytz.utc)

    def __init__(self, simulation_config: PriceSimulConfig, *args, **kwargs):
        self.asset_list = simulation_config.asset_list
        super().__init__(*args, **kwargs)

    def dependencies(self) -> dict[str, Union["DataNode", "APIDataNode"]]:
        return {}

    def update(self) -> pd.DataFrame:
        return SimulatedPricesManager(self).update()

    def get_asset_list(self):
        return self.asset_list

    def get_column_metadata(self):
        return [
            msc.ColumnMetaData(
                column_name="close",
                dtype="float",
                label="Close",
                description="Simulated daily close price",
            )
        ]

    def get_table_metadata(self) -> msc.TableMetaData:
        return msc.TableMetaData(
            identifier=SIMULATED_PRICES_IDENTIFIER,
            data_frequency_id=msc.DataFrequency.one_d,
            description="Simulated daily close prices for tutorial assets.",
        )
```

You can also compare against the full SDK example here:
[Simulated Prices Example](https://github.com/mainsequence-sdk/mainsequence-sdk/blob/main/examples/data_nodes/simple_simulated_prices.py)

## Why this code is written this way

### `asset_list` is scope, not table meaning

We ignore `asset_list` in `storage_hash` because the asset batch defines which updater job writes data, not what the dataset means.

That is why this field uses:

```python
Field(..., json_schema_extra={"ignore_from_storage_hash": True})
```

This keeps multiple update processes pointed at the same table while still allowing each updater to have its own `update_hash`.

### `get_asset_list()` is not just a formality

When a node works with assets, `get_asset_list()` tells the platform which assets belong to that updater. This supports per-asset update statistics and makes incremental updates possible.

If your updater produces asset identifiers that do not already exist in Main Sequence, resolve or register them idempotently inside `get_asset_list()` before returning them.

### `get_table_metadata()` and `get_column_metadata()` make the table usable

Production-quality nodes should describe the table and its columns. Other users, dashboards, and agents may not have code access, so metadata is part of the dataset contract.

### `update()` should be incremental

The important pattern in `update()` is:

- compute the per-asset start from `UpdateStatistics`
- fetch prior observations once with `get_ranged_data_per_asset(...)`
- return only new rows
- keep the index sorted and stable

## Output shape rules for asset DataNodes

For a standard asset table, the output should follow these rules:

- first index level: UTC-aware `time_index`
- second index level: `unique_identifier`
- no duplicate `(time_index, unique_identifier)` pairs
- lowercase, stable column names
- consistent dtypes across runs
- sorted index whenever possible

Those rules are the minimum needed to make the table predictable for downstream users and jobs.

## Choosing a table identifier safely

Use a stable snake_case identifier that describes the dataset, for example:

- `simulated_prices_tutorial`
- `simulated_prices_research_demo`

If someone in your organization already created the same table identifier, choose a new stable identifier with a project-specific suffix. Do not rename it on every run.

## Launcher script

Create `scripts\simulated_prices_launcher.py` (Windows) or `scripts/simulated_prices_launcher.py` (macOS/Linux) and add the following code to run two updater jobs that write to the same prices table:

```python
from mainsequence.client import Asset

from src.data_nodes.prices_nodes import PriceSimulConfig, SimulatedPrices

assets = Asset.filter(ticker__in=["NVDA", "AAPL"])
config = PriceSimulConfig(asset_list=assets)

batch_2_assets = Asset.filter(ticker__in=["JPM", "GS"])
config_2 = PriceSimulConfig(asset_list=batch_2_assets)

node_1 = SimulatedPrices(simulation_config=config)
node_1.run(debug_mode=True, force_update=True)

node_2 = SimulatedPrices(simulation_config=config_2)
node_2.run(debug_mode=True, force_update=True)
```

This launches two update processes with different asset batches, but both write into the same underlying table because the dataset meaning is the same.

If ticker filters are ambiguous in your environment, prefer more specific filters or use `unique_identifier__in=[...]` instead.

## Run from VS Code

Add a new debug configuration for this launcher.

**Windows (`.vscode\launch.json`)**

```json
{
    "name": "Debug simulated_prices_launcher",
    "type": "debugpy",
    "request": "launch",
    "program": "${workspaceFolder}\\scripts\\simulated_prices_launcher.py",
    "console": "integratedTerminal",
    "envFile": "${workspaceFolder}\\.env",
    "env": {
        "PYTHONPATH": "${workspaceFolder}"
    },
    "python": "${workspaceFolder}\\.venv\\Scripts\\python.exe",
    "justMyCode": true
}
```

**macOS/Linux (`.vscode/launch.json`)**

```json
{
    "name": "Debug simulated_prices_launcher",
    "type": "debugpy",
    "request": "launch",
    "program": "${workspaceFolder}/scripts/simulated_prices_launcher.py",
    "console": "integratedTerminal",
    "envFile": "${workspaceFolder}/.env",
    "env": {
        "PYTHONPATH": "${workspaceFolder}"
    },
    "python": "${workspaceFolder}/.venv/bin/python",
    "justMyCode": true
}
```

Then run `Debug simulated_prices_launcher` from VS Code.

## Testing or experimenting safely on a shared backend

If you are experimenting in a shared organization backend, isolate your test tables instead of colliding with production-like identifiers.

```python
from mainsequence.tdag.data_nodes import hash_namespace

with hash_namespace("tutorial_alice"):
    node = SimulatedPrices(simulation_config=config)
    node.run(debug_mode=True, force_update=True)
```

This is especially useful for tests and tutorial experimentation.

## What success looks like

If the launcher succeeds, you should see two updater jobs writing into the same dataset.

Search for the identifier you chose, for example:

[Search for `simulated_prices_tutorial`](https://main-sequence.app/dynamic-table-metadatas/?search=simulated_prices_tutorial&storage_hash=&identifier=)

If you chose a different identifier, search for that one instead.

## Common issues

- **Identifier already exists**: choose a new stable identifier with a project-specific suffix.
- **No new rows returned**: you may already be up to date through yesterday 00:00 UTC.
- **Assets not found**: check your ticker filter, venue filter, or use `unique_identifier` directly.
- **Auth or environment issues**: make sure your `.env` and Main Sequence login state are valid before running the launcher.
