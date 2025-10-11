
# Getting Started 4: Hydrating the Platform

After you sign up, the Main Sequence platform starts **empty**. Its power comes from building **data workflows at scale**, so one of the first tasks is to **integrate your data sources**.

In this part of the tutorial, you’ll implement a few data nodes to start building data and financial applications.

## Assets in Main Sequence

Before moving forward, let’s recap the concept of an **asset**. Main Sequence is built not only to unify data sources, but also to unify how data relates to **financial instruments**.

The platform is already hydrated with information about many publicly traded assets. You can review the full list of asset attributes here:

https://github.com/mainsequence-sdk/mainsequence-sdk/blob/f3b42bd4f4478574b7375508b1b7441ca5c8d297/mainsequence/client/models_vam.py#L197

For this tutorial, focus on the most important fields when integrating with data nodes:

```python
class AssetMixin(BaseObjectOrm, BasePydanticModel):
    id: Optional[int] = None

    # Immutable identifiers
    unique_identifier: constr(max_length=255)
    is_custom_by_organization: bool = Field(
        default=False,
        description="Flag indicating if this asset was custom-created by the organization"
    )
```
As you saw earlier, `unique_identifier` lets you identify an asset as a **secondary index** in your tables.
For most assets this will be the FIGI. However, some assets won’t exist on https://www.openfigi.com.
In those cases, use the `is_custom_by_organization` flag to register a custom asset in the platform.

You’re not limited to classic “assets.” You can also create assets that represent **indices**,
**interest-rate curves**, or any other **unique identifier** for time‑series data.

## Example: Hydrating with U.S. Treasury Constant-Maturity Yields

To demonstrate, we’ll build a data node that hydrates the platform with
**interest‑rate curves** you can use later in fixed‑income applications (pricing, 
stress analysis, and more).

First, create a data node to store **constant‑maturity U.S. Treasury yields**.
We’ll source the data from **polygon.io**.

A complete `DataNode` implementation lives in the public data connectors repo:  
https://github.com/mainsequence-sdk/data-connectors  
Look under `data_connectors/prices/polygon/data_nodes.py`.

```python
class PolygonUSTCMTYields(PolygonEconomyNode):
    """
    Daily U.S. Treasury constant-maturity yields by tenor from Polygon Economy API.
    Identifiers: UST_<tenor}_CMT (e.g., UST_2Y_CMT)
    Columns:
      - days_to_maturity (int)
      - par_yield (decimal)
    """
```
### Creating or Resolving Assets in Code

In earlier examples, `get_asset_list` simply returned assets that already existed. 
Here, you’ll **create the assets on the fly** if they’re missing.
We also attach optional properties such as `security_market_sector`, 
`security_type`, and `security_type_2` to enable richer filtering on the platform.

```python
def build_assets(self) -> List[msc.Asset]:
    payload = []
    for tenor in UST_CMT_FIELD_BY_TENOR:
        identifier = f"UST_{tenor}_CMT"
        snapshot = {"name": identifier, "ticker": identifier, "exchange_code": "US_TREASURY"}
        payload.append({
            "unique_identifier": identifier,
            "snapshot": snapshot,
            "security_market_sector": msc.MARKETS_CONSTANTS.FIGI_MARKET_SECTOR_GOVT,
            "security_type": msc.MARKETS_CONSTANTS.FIGI_SECURITY_TYPE_DOMESTIC,
            "security_type_2": msc.MARKETS_CONSTANTS.FIGI_SECURITY_TYPE_2_GOVT,
        })
    asset_list = msc.Asset.batch_get_or_register_custom_assets(payload)
    return asset_list

def get_asset_list(self) -> List[msc.Asset]:
    return self.build_assets()
```
As in the previous tutorial, you can copy code snippets from **data connectors** repository into your tutorial project and adjust the imports making sure they point to the correct locations and create a **run endpoint** to update the data for the first time.

Or even better and simpler - if you not sure that will be able add all needed code and adjust it manually, you can fork Data Connectors project into a new project so you have all connectors available. Than you will be able to follow this  tutorial adding only missing pieces and ensuring everything works smoothly.

To fork the repo into new project:
1. Go to [https://main-sequence.app/projects/](https://main-sequence.app/projects/)
2. Find project with "data_connectors" name and click on it
3. In the top right corner you see three dots menu, open it and  click on "Fork Project" button, assing it name like "my_data_connectors" and click on "Fork".
4. Than you can set up new project locally as you done before in prevbious part of tutorial and work with this new project afterwards.

Now create a new runner file in `scripts` folder with a name `run_ust_cmt_yields.py` and add this code to it:

```python
from data_connectors.prices.polygon.data_nodes import PolygonUSTCMTYields
from mainsequence.client import Constant as _C

data_node = PolygonUSTCMTYields()
data_node.run(debug_mode=True, force_update=True)
```


Now you need is to get your API key from polygon.io and add it as environment variable `POLYGON_API_KEY` in the `.env` file in the root of your project.

Register and request your API key here [https://polygon.io/](https://polygon.io/)

```env
POLYGON_API_KEY="your_polygon_api_key_here"
```


After that you can add a new entry to your `.vscode\launch.json` file in `configurations` list:

(Windows):
```json
{
    "name": "Debug ust_cmt_yields",
    "type": "debugpy",
    "request": "launch",
    "program": "${workspaceFolder}\\scripts\\run_ust_cmt_yields.py",
    "console": "integratedTerminal",
    "env": {
        "PYTHONPATH": "${workspaceFolder}"
    },
    "python": "${workspaceFolder}\\.venv\\Scripts\\python.exe"
}
```
(macOS/Linux):
```json
{
    "name": "Debug ust_cmt_yields",
    "type": "debugpy",
    "request": "launch",
    "program": "${workspaceFolder}/scripts/run_ust_cmt_yields.py",
    "console": "integratedTerminal",
    "env": {
        "PYTHONPATH": "${workspaceFolder}"
    },
    "python": "${workspaceFolder}/.venv/bin/python"
}
```

Then run it from the Run and Debug tab in VS Code.

After running the node, you should see the table in the platform: [https://main-sequence.app/dynamic-table-metadatas/?search=polygonustcmtyields_&storage_hash=&identifier=](https://main-sequence.app/dynamic-table-metadatas/?search=polygonustcmtyields_&storage_hash=&identifier=)


## From CMT Yields to a Zero Curve

Next, build a **zero curve** and store it in a `zero_curves` data node. 
The idea is to provide a backend data node from which you can retrieve different curves
and use them to value fixed‑income instruments (e.g., custom swaps or floating‑rate coupon bonds).

We provide a **curve registry factory** that can serve as a blueprint for your projects.
You can keep ours or implement your own; to move quickly, we recommend using ours initially.

The zero‑curve `DataNode` can be found at `data_connectors/interest_rates/nodes.py`:

```python
class DiscountCurves(DataNode):
    ...

    def update(self):
        # Download CSV from source
        df = DISCOUNT_CURVE_BUILD_REGISTRY[self.curve_config.unique_identifier](
            update_statistics=self.update_statistics,
            curve_unique_identifier=self.curve_config.unique_identifier,
            base_node_curve_points=self.base_node_curve_points,
        )

        # Apply the new compression and encoding function to the 'curve' column.
        df["curve"] = df["curve"].apply(compress_curve_to_string)

        last_update = self.update_statistics.get_last_update_index_2d(self.curve_config.unique_identifier)
        df = df[df.index.get_level_values("time_index") > last_update]

        if df.empty:
            return pd.DataFrame()

        return df
```
The method above is **generic**. What you need is a registry entry that points a **curve identifier** to a **build function**.

Create it in `data_connectors/interest_rates/registries/discount_curves.py` like this:

```python
"""
Signature for each zero‑curve function should be:

def bootstrap_cmt_curve(update_statistics, curve_unique_identifier: str, base_node_curve_points: APIDataNode) -> pd.DataFrame

It must return a DataFrame with:
  - MultiIndex: ("time_index", "unique_identifier")
  - Column "curve": dict[days_to_maturity] -> zero_rate (percent)

Where "unique_identifier" is the name of the zero curve. We recommend exposing a constant in the backend
to retrieve this specific curve.
"""
from mainsequence.client import Constant as _C

_POLYGON_CURVES = {
    _C.get_value(name="ZERO_CURVE__UST_CMT_ZERO_CURVE_UID"): bootstrap_cmt_curve,
}

# ---- Public, aggregated registry ----
DISCOUNT_CURVE_BUILD_REGISTRY: Dict[str, Callable] = _merge_unique(
    _POLYGON_CURVES
)
```
### Constants: A Cross‑Project Contract

Notice the import of `mainsequence.client.Constant`. As you build multiple applications across 
data nodes and projects, it’s useful to fetch **constants** via the API. 
Main Sequence supports both **global constants** (available across the platform) 
and **project‑scoped constants** (limited to a project).

To keep things orderly without adding complexity,
constants can be grouped by **category** using a double underscore `__` as a separator. 
The name of the constant doesn’t change, but the platform groups related constants for easier
filtering and visualization.

**Where are these constants created?** To ensure a curve constant exists, we run a **get‑or‑create** operation when importing the `prices/polygon/__init__.py` module:

```python
constants_to_create = dict(
    POLYGON__UST_CMT_YIELDS_TABLE_UID=UST_CMT_YIELDS_TABLE_UID,
    ZERO_CURVE__UST_CMT_ZERO_CURVE_UID="polygon_ust_cmt_zero_curve_usd",
)

_C.create_constants_if_not_exist(constants_to_create)
```

## Interest rate fixing DataNode

So far, we’ve used the `zero_curve` registry helper from `data_connectors` to model future cash flows. However, with most pricing libraries — including QuantLib — we also need past fixing dates to price cash flows whose fixings occurred in the past.

Below, let’s look at `data_connectors/prices/fred/data_nodes.py`. Here we have a data node designed to integrate economic data from the Federal Reserve Bank of St. Louis (FRED).

Take the code below and run it in your tutorial project.
create a new runner file in `scripts` folder with a name `run_fred_fixings.py` and add this code to it:

```python
from src.data_nodes.interest_rates.nodes import FixingRatesNode, FixingRateConfig, RateConfig
from mainsequence.client import Constant as _C

USD_SOFR = _C.get_value(name="REFERENCE_RATE__USD_SOFR")
USD_EFFR = _C.get_value(name="REFERENCE_RATE__USD_EFFR")
USD_OBFR = _C.get_value(name="REFERENCE_RATE__USD_OBFR")
fixing_config = FixingRateConfig(rates_config_list=[
RateConfig(unique_identifier=USD_SOFR,
            name=f"Secured Overnight Financing Rate "),
RateConfig(unique_identifier=USD_EFFR,
            name=f"Effective Federal Funds Rate "),
RateConfig(unique_identifier=USD_OBFR,
            name=f"Overnight Bank Funding Rate"),
    ])
ts = FixingRatesNode(rates_config=fixing_config)
ts.run(debug_mode=True, force_update=True)

ts = FixingRatesNode(rates_config=fixing_config)
ts.run(debug_mode=True, force_update=True)
```

>**Important: Don’t forget to copy the interest_rate folder from data_connectors if you not forked the project completely.**

Now you can add a new entry to your `.vscode\launch.json` file in `configurations` list:

(Windows):
```json
{
    "name": "Debug fred_fixings",
    "type": "debugpy",
    "request": "launch",
    "program": "${workspaceFolder}\\scripts\\run_fred_fixings.py",
    "console": "integratedTerminal",
    "env": {
        "PYTHONPATH": "${workspaceFolder}"
    },
    "python": "${workspaceFolder}\\.venv\\Scripts\\python.exe"
}
```
(macOS/Linux):
```json
{
    "name": "Debug fred_fixings",
    "type": "debugpy",
    "request": "launch",
    "program": "${workspaceFolder}/scripts/run_fred_fixings.py",
    "console": "integratedTerminal",
    "env": {
        "PYTHONPATH": "${workspaceFolder}"
    },
    "python": "${workspaceFolder}/.venv/bin/python"
}
```

Before you be able to run this you need to get an API key from FRED and add it as environment variable `FRED_API_KEY` in the `.env` file in the root of your project.
```env
FRED_API_KEY="your_fred_api_key_here"
```

Register and request your API key here:

[https://fredaccount.stlouisfed.org/apikeys](https://fredaccount.stlouisfed.org/apikeys)



Then run it from the Run and Debug tab in VS Code.


## One‑Shot Runner

Here’s how everything looks if you want to run your data nodes at once:


```python
from data_connectors.prices.polygon.data_nodes import PolygonUSTCMTYields
from mainsequence.client import Constant as _C

data_node = PolygonUSTCMTYields()
data_node.run(debug_mode=True, force_update=True)

from data_connectors.prices.polygon.settings import UST_CMT_YIELDS_TABLE_UID
from data_connectors.interest_rates.nodes import (DiscountCurves, CurveConfig,)
config = CurveConfig(
    unique_identifier=_C.get_value("ZERO_CURVE__UST_CMT_ZERO_CURVE_UID"),
    name="Discount Curve UST Bootstrapped",
    curve_points_dependecy_data_node_uid=UST_CMT_YIELDS_TABLE_UID,
)
node = DiscountCurves(curve_config=config)
node.run(debug_mode=True, force_update=True)
```

Add this code to new file `run_ust_cmt_and_zero_curve.py` in the `scripts` folder in your project.

Dont forget about Polygon API key in the `.env` file if you not added it before:
```env
POLYGON_API_KEY="your_polygon_api_key_here"
```

Now you can add a new entry to your `.vscode\launch.json` file in `configurations` list:

(Windows):
```json
{
    "name": "Debug ust_cmt_and_zero_curve",
    "type": "debugpy",
    "request": "launch",
    "program": "${workspaceFolder}\\scripts\\run_ust_cmt_and_zero_curve.py",
    "console": "integratedTerminal",
    "env": {
        "PYTHONPATH": "${workspaceFolder}"
    },
    "python": "${workspaceFolder}\\.venv\\Scripts\\python.exe"
}
```

(macOS/Linux):
```json
{
    "name": "Debug ust_cmt_and_zero_curve",
    "type": "debugpy",
    "request": "launch",
    "program": "${workspaceFolder}/scripts/run_ust_cmt_and_zero_curve.py",
    "console": "integratedTerminal",
    "env": {
        "PYTHONPATH": "${workspaceFolder}"
    },
    "python": "${workspaceFolder}/.venv/bin/python"
}
```

Then run it from the Run and Debug tab in VS Code.