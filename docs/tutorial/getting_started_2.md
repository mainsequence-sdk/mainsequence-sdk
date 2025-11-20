# Getting Started (Part 2): Multi‑Index Columns — Working with Assets

In Part 1, you created a project and built a basic `DataNode`. Here, you'll build a `DataNode` designed for **financial workflows**: one that stores **security prices**. The same pattern also works for signals, news, or other asset‑centric datasets.

Create a file at `src\data_nodes\prices_nodes.py` (Windows) or `src/data_nodes/prices_nodes.py` (macOS/Linux) and add the following data node. 

`prices_nodes.py`
```python

from pydantic import BaseModel, Field
from typing import Union
import datetime
import pytz
import pandas as pd


import mainsequence.client as msc
from mainsequence.tdag import DataNode,APIDataNode


class SimulatedPricesManager:

    def __init__(self, owner: DataNode):
        self.owner = owner

    @staticmethod
    def _get_last_price(obs_df: pd.DataFrame, unique_id: str, fallback: float) -> float:
        """
        Helper method to retrieve the last price for a given unique_id or return 'fallback'
        if unavailable.

        Args:
            obs_df (pd.DataFrame): A DataFrame with multi-index (time_index, unique_identifier).
            unique_id (str): Asset identifier to look up.
            fallback (float): Value to return if the last price cannot be retrieved.

        Returns:
            float: Last observed price or the fallback value.
        """
        # If there's no historical data at all, return fallback immediately
        if obs_df.empty:
            return fallback

        # Try to slice for this asset and get the last 'close' value
        try:
            slice_df = obs_df.xs(unique_id, level="unique_identifier")["close"]
            return slice_df.iloc[-1]
        except (KeyError, IndexError):
            # KeyError if unique_id not present, IndexError if slice is empty
            return fallback

    def update(self) -> pd.DataFrame:
        """
        Mocks price updates for assets with stochastic lognormal returns.
        For each asset, simulate new data starting one hour after its last update
         until yesterday at 00:00 UTC, using the last observed price as the seed.
         The last observation is not duplicated.
         Returns:
             pd.DataFrame: A DataFrame with a multi-index (time_index, unique_identifier)
                           and a single column 'close' containing the simulated prices.
        """
        import numpy as np

        initial_price = 100.0
        mu = 0.0  # drift component for lognormal returns
        sigma = 0.01  # volatility component for lognormal returns

        df_list = []
        update_statistics = self.owner.update_statistics
        # Get the latest historical observations; assumed to be a DataFrame with a multi-index:
        # (time_index, unique_identifier) and a column "close" for the last observed price.
        range_descriptor = update_statistics.get_update_range_map_great_or_equal()
        last_observation = self.owner.get_ranged_data_per_asset(range_descriptor=range_descriptor)
        # Define simulation end: yesterday at midnight (UTC)
        yesterday_midnight = datetime.datetime.now(pytz.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        ) - datetime.timedelta(days=1)
        # Loop over each unique identifier and its last update timestamp.
        for asset in update_statistics.asset_list:
            # Simulation starts one hour after the last update.
            start_time = update_statistics.get_asset_earliest_multiindex_update(
                asset=asset
            ) + datetime.timedelta(hours=1)
            if start_time > yesterday_midnight:
                continue  # Skip if no simulation period is available.
            time_range = pd.date_range(start=start_time, end=yesterday_midnight, freq="D")
            if len(time_range) == 0:

                continue
            # Use the last observed price for the asset as the starting price (or fallback).
            last_price = self._get_last_price(
                obs_df=last_observation, unique_id=asset.unique_identifier, fallback=initial_price
            )

            random_returns = np.random.lognormal(mean=mu, sigma=sigma, size=len(time_range))
            simulated_prices = last_price * np.cumprod(random_returns)
            df_asset = pd.DataFrame({asset.unique_identifier: simulated_prices}, index=time_range)
            df_list.append(df_asset)

        if df_list:
            data = pd.concat(df_list, axis=1)
        else:
            return pd.DataFrame()

        # Reshape the DataFrame into long format with a multi-index.
        data.index.name = "time_index"
        data = data.melt(ignore_index=False, var_name="unique_identifier", value_name="close")
        data = data.set_index("unique_identifier", append=True)
        return data

    def get_column_metadata(self):
        from mainsequence.client.models_tdag import ColumnMetaData

        columns_metadata = [
            msc.ColumnMetaData(
                column_name="close",
                dtype="float",
                label="Close ",
                description=("Simulated close price"),
            ),# type: ignore pylance wrongly infers Field(None) as required
        ]
        return columns_metadata

class PriceSimulConfig(BaseModel):

    asset_list: list[msc.AssetMixin] = Field(
        ...,
        title="Asset List",
        description="List of assets to simulate",
        ignore_from_storage_hash=True
    )# type: ignore pylance wrongly expctes ignore_from_storage_hash to be described paramtenrs but is not the case in pydantic

class SimulatedPrices(DataNode):
    """
    Simulates price updates for a specific list of assets provided at initialization.
    """

    OFFSET_START = datetime.datetime(2024, 1, 1, tzinfo=pytz.utc)

    def __init__(self, simulation_config: PriceSimulConfig, *args, **kwargs):
        """
        Args:
            simulation_config (PriceSimulConfig): Configuration containing the asset list.
            *args: Additional positional arguments.
            **kwargs: Additional keyword arguments.
        """
        self.asset_list = simulation_config.asset_list
        self.asset_symbols_filter = [a.unique_identifier for a in self.asset_list]
        super().__init__(*args, **kwargs)

    def dependencies(self) -> dict[str, Union["DataNode", "APIDataNode"]]:
        return {}

    def update(self):
        update_manager = SimulatedPricesManager(self)
        df = update_manager.update()
        return df

    def get_asset_list(self):
        return self.asset_list

    def get_column_metadata(self):
        """
        Add MetaData information to the DataNode Table
        Returns:

        """
       

        columns_metadata = [
            msc.ColumnMetaData(
                column_name="close",
                dtype="float",
                label="Close",
                description=("Simulated Close Price"),
            ), # type: ignore pylance wrongly infers Field(None) as required
        ]
        return columns_metadata

    def get_table_metadata(self) -> msc.TableMetaData:
        """
        REturns the market time serie unique identifier, assets to append , or asset to overwrite
        Returns:

        """

        mts = msc.TableMetaData(
            identifier="simulated_prices",
            data_frequency_id=msc.DataFrequency.one_d,
            description="This is a simulated prices time serie from asset category",
        )

        return mts
```
>**(IMPORTANT) Do not forget to include the correct imports in this file for this DataNode.**
Visual Studio Code will usually help you auto-import missing dependencies and underline with red missing ones. You can reference the full working example here and find what and where to import and copy to ensure that DataNode above works correctly: https://github.com/mainsequence-sdk/mainsequence-sdk/blob/main/examples/data_nodes/simple_simulated_prices.py

Notice that we **ignore** `asset_list` when computing the **storage hash** with `ignore_from_storage_hash=True`. This is intentional: you often want **all prices**—even from different update processes—to be stored in the **same table**.

>**(IMPORTANT) There can only be one DataNode per unique_identifier** 
If someone in your organization has already done the tutorial or created a DataNode "simulated_prices" you should see an error. You can just rename the unique_identifier by adding a unique suffix 


---
## Lets dive deeper into the DataNode code:

### Two Important Methods

### 1) `get_table_metadata`

Use this method to assign a **human‑readable unique identifier** to the table (and optional metadata like data frequency). This makes it easy to reference the table across projects and keep naming consistent.

```python
def get_table_metadata(self) -> msc.TableMetaData:
    """
    Returns the market time series unique identifier, assets to append, or asset to overwrite
    Returns:

    """

    mts = msc.TableMetaData(
        identifier="simulated_prices",
        data_frequency_id=msc.DataFrequency.one_d,
        description="This is a simulated prices time series from asset category",
    )

    return mts
```

### 2) `get_column_metadata`

Provide descriptive metadata for the columns your `DataNode` writes. This helps other users—and automation—understand the data without reading code.

```python
def get_column_metadata(self):
    """
    Add MetaData information to the DataNode Table
    Returns:

    """
    from mainsequence.client.models_tdag import ColumnMetaData
    columns_metadata = [
        ColumnMetaData(
            column_name="close",
            dtype="float",
            label="Close",
            description="Simulated Close Price"
        ),
    ]
    return columns_metadata
```

---

## Exposing the Asset List

When a node works with assets, we need to implement `get_asset_list`. Sometimes you won't pass assets explicitly; you might pass filters or an asset category name. Returning the resolved list lets the platform automatically maintain **update‑process statistics** and context.

```python
def get_asset_list(self):
    return self.asset_list
```

---

## Why All This Metadata Matters

As your data system grows, metadata becomes crucial. Many users won't have access to the code, so clear table and column metadata helps them understand what's stored beyond the raw `type`. It's also extremely helpful for **agentic workflows**, giving agents better context about the data they're interacting with.

---

## Add `dependencies` and `update` Methods

Finally, we have implemented `dependencies` and `update` that are required for the `DataNode` to function properly. Here, we simulate prices for the specified assets. 

```python
def dependencies(self) -> Dict[str, Union["DataNode", "APIDataNode"]]:
        return {}

def update(self):
    update_manager=SimulatedPricesManager(self)
    df=update_manager.update()
    return df
```

**Next step is to implement a simple manager class to handle the price simulation logic - `SimulatedPricesManager`, you can copy it from the full example linked here:
[Simulated Prices Example - class SimulatedPricesManager](https://github.com/mainsequence-sdk/mainsequence-sdk/blob/16d121a3dfcbaae0b06ab8ecd873efcc23f1d28f/examples/data_nodes/simple_simulated_prices.py#L24)** and place it before the `SimulatedPrices` class in the same file.

---

## Launcher Script and Multi‑Index Output

Create `scripts\simulated_prices_launcher.py` (Windows) or `scripts/simulated_prices_launcher.py` (macOS/Linux) and add the following code to run two separate update processes that write to the **same** prices table:

```python
from src.data_nodes.prices_nodes import SimulatedPrices, PriceSimulConfig
from mainsequence.client import Asset

assets = Asset.filter(ticker__in=["NVDA", "APPL"])
config = PriceSimulConfig(asset_list=assets)

batch_2_assets = Asset.filter(ticker__in=["JPM", "GS"])
config_2 = PriceSimulConfig(asset_list=batch_2_assets)

ts = SimulatedPrices(simulation_config=config)
ts.run(debug_mode=True, force_update=True)

ts_2 = SimulatedPrices(simulation_config=config_2)
ts_2.run(debug_mode=True, force_update=True)
```

This runs two update processes—each updating a different set of tickers—while persisting to the **same underlying table**.

### Running the Launcher

Now add a new debug configuration for this launcher.

**Add to `.vscode\launch.json` (Windows) to `configurations` array:**
```json
{
    "name": "Debug simulated_prices_launcher",
    "type": "debugpy",
    "request": "launch",
    "program": "${workspaceFolder}\\scripts\\simulated_prices_launcher.py",
    "console": "integratedTerminal",
    "env": {
        "PYTHONPATH": "${workspaceFolder}"
    },
    "python": "${workspaceFolder}\\.venv\\Scripts\\python.exe"
}
```

**Add to `.vscode/launch.json` (macOS/Linux) to `configurations` array:**
```json
{
    "name": "Debug simulated_prices_launcher",
    "type": "debugpy",
    "request": "launch",
    "program": "${workspaceFolder}/scripts/simulated_prices_launcher.py",
    "console": "integratedTerminal",
    "env": {
        "PYTHONPATH": "${workspaceFolder}"
    },
    "python": "${workspaceFolder}/.venv/bin/python"
}
```

And finally return to `simulated_prices_launcher.py` and run the debugger using the new configuration you just created, same way you done in the previous Part: at the top right of VS Code, select "Python Debugger: debug using launch.json", then select "Debug simulated_prices_launcher" configuration.

You will see output in the terminal indicating that the two update processes have run successfully.

> **Note on output shape:** In the `update` method, return a DataFrame with a **two‑level index**: `time_index` and `unique_identifier`. Those two indices are the only prerequisites for working with assets in a `DataNode`.

Congratulations! You can see our new table with two processes in:

https://main-sequence.app/dynamic-table-metadatas/?search=simulatedprices&storage_hash=&identifier=