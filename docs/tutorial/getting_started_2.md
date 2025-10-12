# Getting Started (Part 2): Multi‑Index Columns — Working with Assets

In Part 1, you created a project and built a basic `DataNode`. Here, you'll build a `DataNode` designed for **financial workflows**: one that stores **security prices**. The same pattern also works for signals, news, or other asset‑centric datasets.

Create a file at `src\data_nodes\prices_nodes.py` (Windows) or `src/data_nodes/prices_nodes.py` (macOS/Linux) and add the following data node. **Do not forget to include the correct imports for your project setup.** You can reference the full working example here:

https://github.com/mainsequence-sdk/mainsequence-sdk/blob/main/examples/data_nodes/simple_simulated_prices.py

```python

class PriceSimulConfig(BaseModel):

    asset_list: List[msc.AssetMixin] = Field(
        ...,
        title="Asset List",
        description="List of assets to simulate",
        ignore_from_storage_hash=True
    )

class SimulatedPrices(DataNode):
    """
    Simulates price updates for a specific list of assets provided at initialization.
    """
    OFFSET_START = datetime.datetime(2024, 1, 1, tzinfo=pytz.utc)
    
    def __init__(self, simulation_config: PriceSimulConfig, *args, **kwargs):
        """
        Args:
            simulation_config: Configuration containing the asset list
            *args: Additional positional arguments.
            **kwargs: Additional keyword arguments.
        """
        self.asset_list = simulation_config.asset_list
        self.asset_symbols_filter = [a.unique_identifier for a in self.asset_list]
        super().__init__(*args, **kwargs)

```

Notice that we **ignore** `asset_list` when computing the **storage hash**. This is intentional: you often want **all prices**—even from different update processes—to be stored in the **same table**.

---

## Two Important Methods

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

When a node works with assets, implement `get_asset_list`. Sometimes you won't pass assets explicitly; you might pass filters or an asset category name. Returning the resolved list lets the platform automatically maintain **update‑process statistics** and context.

```python
def get_asset_list(self):
    return self.asset_list
```

---

## Why All This Metadata Matters

As your data system grows, metadata becomes crucial. Many users won't have access to the code, so clear table and column metadata helps them understand what's stored beyond the raw `type`. It's also extremely helpful for **agentic workflows**, giving agents better context about the data they're interacting with.

---

## Add `dependencies` and `update` Methods

Finally, implement `dependencies` and `update` that are required for the `DataNode` to function properly. Here, we simulate prices for the specified assets. 

 Add required imports to the top of your nodes file:
```python
from typing import Dict, Union
from mainsequence.tdag.data_nodes import APIDataNode
```

Then add the following methods to your `SimulatedPrices` class:
```python
def dependencies(self) -> Dict[str, Union["DataNode", "APIDataNode"]]:
        return {}

def update(self):
    update_manager=SimulatedPricesManager(self)
    df=update_manager.update()
    return df
```

**Next step is to implement a simple manager class to handle the price simulation logic - `SimulatedPricesManager`, you can copy it from the full example linked here:
[Simulated Prices Example - class SimulatedPricesManager](https://github.com/mainsequence-sdk/mainsequence-sdk/blob/16d121a3dfcbaae0b06ab8ecd873efcc23f1d28f/examples/data_nodes/simple_simulated_prices.py#L24)**

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

You can run this launcher directly or set up a debug configuration in VS Code.

**Add to `.vscode\launch.json` (Windows):**
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

**Add to `.vscode/launch.json` (macOS/Linux):**
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

> **Note on output shape:** In the `update` method, return a DataFrame with a **two‑level index**: `time_index` and `unique_identifier`. Those two indices are the only prerequisites for working with assets in a `DataNode`.

We can see our new table in:

https://main-sequence.app/dynamic-table-metadatas/?search=simulatedprices&storage_hash=&identifier=