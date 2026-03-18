# Part 3: Multi-Index Columns and Working with Assets

## Quick Summary

In this part, you will:

- build an asset-based DataNode that writes a `(time_index, unique_identifier)` MultiIndex table
- use `get_asset_list()` to scope updates by asset
- run multiple updaters that write to the same dataset
- understand table identity (`storage_hash`) vs updater identity (`update_hash`)

DataNodes created in this part: **`SimulatedPrices`**.

In Part 2, you built a basic `DataNode`. In this part, you will build an asset-based `DataNode` that stores simulated security prices in a two-level `MultiIndex` DataFrame.

You can reuse this pattern for prices, signals, news, or any other asset-centric dataset.

For the broader design rules behind this tutorial, see the [Data Nodes knowledge guide](../knowledge/data_nodes.md). For a deeper explanation of asset identity, custom assets, and when to use `filter()` versus `query()`, see [Assets](../knowledge/markets/assets.md).

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

- the table `identifier` in `get_table_metadata()` names the dataset and must be unique across your organization
- the `unique_identifier` in the `MultiIndex` names each asset row and must be unique for the asset it represents
- `asset_list` is updater scope, so it should usually be ignored from `storage_hash`

## Create `src/data_nodes/prices_nodes.py`

Create a file at `src\data_nodes\prices_nodes.py` (Windows) or `src/data_nodes/prices_nodes.py` (macOS/Linux) and add the following code:

```python
import datetime
import os
from typing import Union

import numpy as np
import pandas as pd
import pytz
from pydantic import BaseModel, Field

import mainsequence.client as msc
from mainsequence.tdag import APIDataNode, DataNode

PROJECT_ID = os.getenv("MAIN_SEQUENCE_PROJECT_ID", "local").strip() or "local"
SIMULATED_PRICES_IDENTIFIER = f"simulated_prices_tutorial_{PROJECT_ID}"


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

- `simulated_prices_tutorial_130`
- `simulated_prices_research_demo_130`

The safest tutorial pattern is to include `MAIN_SEQUENCE_PROJECT_ID` from your `.env` file, as shown in the code example above. This matters because tutorial identifiers are reused by many people, and someone else in your organization has probably already run this chapter. Using the project id keeps the identifier stable for your project and avoids those collisions.

If someone in your organization already created the same table identifier, choose a new stable identifier with a project-specific suffix. Do not rename it on every run.

If you want to inspect the organization-visible DataNode table identifiers first, run:

```bash
mainsequence data-node org-unique-identifiers
```

This command helps you check existing table names before publishing a new one. It lists DataNode identifiers, not asset `unique_identifier` values.

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

If you are experimenting in a shared organization backend, keep the table identifier stable and isolate the run with `hash_namespace(...)` instead of inventing a new identifier for every test.

Why this is the safer pattern:

- `identifier` still describes the dataset meaning
- `hash_namespace(...)` is test-only isolation plumbing
- a non-empty namespace changes both `storage_hash` and `update_hash`
- your experiment writes into isolated tables and updater records instead of colliding with production-like runs

Use a short namespace you can recognize later, for example:

- `tutorial_alice`
- `tutorial_alice_fix_123`
- `pytest_simulated_prices_smoke`

Preferred pattern:

```python
from mainsequence.tdag.data_nodes import hash_namespace

with hash_namespace("tutorial_alice"):
    node = SimulatedPrices(simulation_config=config)
    err, df = node.run(debug_mode=True, force_update=True)
```

Shortcut form:

```python
node = SimulatedPrices(simulation_config=config, test_node=True)
err, df = node.run(debug_mode=True, force_update=True)
```

Prefer the explicit namespace form when multiple people or parallel tests may run at the same time.

After the run, inspect the updater records from the CLI:

```bash
mainsequence project data-node-updates list
```

If your local project auth has expired, refresh it first:

```bash
mainsequence project refresh_token --path .
```

Example CLI output:

```text
Project Data Node Updates
ID   Update Hash                        Data Node Storage                  Update Details
--   ---------------------------------  ---------------------------------  --------------
410  0f0a8c2c6b9a4b6b8d7d2e9b5b6f2a1     7b6d7a7a65f34d7f9a8d8c3e9f8a7b1     901
411  4b7c27f5f8a9447eaaf3c9f37df0f5ab    0c2f0e32cf14462f8d54b9c1f8a31f73    902
Total updates: 2
```

What to expect from that output:

- the exact IDs and hashes will differ in your environment
- a namespaced run will have a different `Data Node Storage` value from a non-namespaced run
- the `Update Hash` will also differ, because the updater identity is isolated too
- that difference is expected even when both runs use the same `SIMULATED_PRICES_IDENTIFIER`

This is especially useful for tests, smoke runs, and short-lived tutorial experimentation on a shared backend.

## Example test in the `tests/` folder

For real projects, keep your tests under `tests/`. For this tutorial, a good example would be:

- `tests/test_simulated_prices.py`

One useful testing pattern is:

1. create a test-only subclass of `SimulatedPrices`
2. override `OFFSET_START` so the first run stays small
3. run the node inside a namespace so the test hashes do not collide with shared tables

Example:

```python
import datetime

import pytz

import mainsequence.client as msc
from mainsequence.tdag.data_nodes import hash_namespace

from src.data_nodes.prices_nodes import PriceSimulConfig, SimulatedPrices

UTC = pytz.utc


class TestSimulatedPrices(SimulatedPrices):
    OFFSET_START = datetime.datetime(2025, 1, 1, tzinfo=UTC)


def test_simulated_prices_smoke():
    assets = msc.Asset.batch_get_or_register_custom_assets(
        [
            {"unique_identifier": "TEST_SIM_A", "snapshot": {"name": "TEST_SIM_A", "ticker": "TEST_SIM_A"}},
            {"unique_identifier": "TEST_SIM_B", "snapshot": {"name": "TEST_SIM_B", "ticker": "TEST_SIM_B"}},
        ]
    )

    config = PriceSimulConfig(asset_list=assets)

    with hash_namespace("pytest_simulated_prices_smoke"):
        node = TestSimulatedPrices(simulation_config=config)
        err, df = node.run(debug_mode=True, force_update=True)

    assert err is False
    assert df is not None
    assert not df.empty
    assert df.index.names == ["time_index", "unique_identifier"]
```

Why this is the recommended shape:

- the test lives in the normal `tests/` folder
- `hash_namespace(...)` isolates both `storage_hash` and `update_hash`
- overriding `OFFSET_START` keeps the first-run backfill small and fast
- the production `SimulatedPrices` class stays unchanged

## What success looks like

If the launcher succeeds, you should see two updater jobs writing into the same dataset.

Search for the identifier you chose, for example `simulated_prices_tutorial_<your_project_id>`:

[Open Dynamic Table Metadatas](https://main-sequence.app/dynamic-table-metadatas/)

If you chose a different identifier, search for that one instead.

## Common issues

- **Identifier already exists**: the table `identifier` must be unique across your organization. Reuse `MAIN_SEQUENCE_PROJECT_ID` or another stable project-specific suffix.
- **No new rows returned**: you may already be up to date through yesterday 00:00 UTC.
- **Assets not found**: check your ticker filter, venue filter, or use `unique_identifier` directly.
- **Auth or environment issues**: make sure your `.env` and Main Sequence login state are valid before running the launcher.
