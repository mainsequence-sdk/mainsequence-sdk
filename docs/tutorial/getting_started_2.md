# Getting Started (Part 2): Multi‑Index Columns — Working with Assets

In Part 1, you created a project and built a basic `DataNode`. Here, you'll build a `DataNode` designed for **financial workflows**: one that stores **security prices**. The same pattern also works for signals, news, or other asset‑centric datasets.

Create a file at `src\data_nodes\prices_nodes.py` (Windows) or `src/data_nodes/prices_nodes.py` (macOS/Linux) and add the following data node. 

```python
class PriceSimulConfig(BaseModel):

    asset_list: list[msc.AssetMixin] = Field(
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
            asset_list (ModelList): List of asset objects.
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
        from mainsequence.client.models_tdag import ColumnMetaData

        columns_metadata = [
            ColumnMetaData(
                column_name="close",
                dtype="float",
                label="Close",
                description=("Simulated Close Price"),
            ),
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

---
## Lets Dive Deeper into the  DataNode code:

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