# Getting Started 4: Markets — Portfolios and Virtual Funds

In the previous part, you created and updated **assets** and learned how **Data Nodes** relate to assets. This chapter goes deeper into Main Sequence’s **Markets** features and shows, end‑to‑end, how to create a portfolio that the platform understands.

Main Sequence can store the key objects used in investment operations: **accounts**, **groups of accounts**, **portfolios**, **portfolio groups**, and **virtual funds**. Most of these are straightforward and share the same CRUD operations (filter, retrieve, create). Portfolios are a special case because they are tightly linked to **Data Nodes** in the platform.

Before moving on to dashboards or deeper analysis, we’ll build a concise **end‑to‑end example**: create assets, group them, link them to a price source via a **translation table**, and then build a portfolio.

---

## Process Overview

1. Ensure the assets the portfolio will hold **exist** (and have pricing details).  
2. Create an **asset category** so we can filter them as a group.  
3. Build a **translation table** that maps asset characteristics to the **right Data Node**.  
4. Create a **signal Data Node** with fixed weights.  
5. Use the **Virtual Fund Builder** to assemble the portfolio.

Go to your tutorial project and, under `src/`, create a file named `helpers_mock.py`. We’ll start by building the test portfolio there.

---

## 1) Building Test Assets

As before, we’ll register **custom fixed‑income assets** and attach pricing details so they can be priced later.

```python
import datetime
import pytz
import numpy as np
import pandas as pd
import mainsequence.client as msc
from mainsequence.tdag import DataNode
from mainsequence.client.models_tdag import UpdateStatistics, ColumnMetaData

UTC = pytz.utc


SECURITY_TYPE_MOCK="MOCK_ASSET"
SIMULATED_PRICES_TABLE="simulated_daily_closes_tutorial"
TRANSLATION_TABLE_IDENTIFIER = "prices_translation_table_1d"
# =========================================================
# 1) DRY helper: ensure both test assets exist and have pricing details
# =========================================================
def ensure_test_assets(unique_identifiers=None):
    """
    Ensure the two test bonds exist and have instrument pricing details.
    Returns: List[msc.Asset]
    """
    FLOATING_INDEX_NAME = "SOFR"


    if unique_identifiers is None:
        unique_identifiers = ["TEST_FLOATING_BOND_UST_R", "TEST_FIXED_BOND_USD_R"]

    # Fetch any existing
    existing_assets = msc.Asset.filter(unique_identifier__in=unique_identifiers)  # cookbook filtering
    uid_to_asset = {a.unique_identifier: a for a in existing_assets}

    # Build common dates (UTC)
    now_utc = datetime.datetime.now(UTC)
    time_idx = datetime.datetime(
        now_utc.year, now_utc.month, now_utc.day, now_utc.hour, now_utc.minute, tzinfo=UTC
    )

    # Common instrument kwargs
    common_kwargs = {
        "face_value": 100,
        "coupon_frequency": ql.Period(6, ql.Months),
        "day_count": ql.Actual365Fixed(),
        "calendar": ql.UnitedStates(ql.UnitedStates.GovernmentBond),
        "business_day_convention": ql.Unadjusted,
        "settlement_days": 0,
        "maturity_date": time_idx.date() + datetime.timedelta(days=365 * 10),
        "issue_date": time_idx.date(),
        "benchmark_rate_index_name":FLOATING_INDEX_NAME
    }

    created_assets = []
    for uid in unique_identifiers:
        asset = uid_to_asset.get(uid)
        needs_build = (asset is None) or (getattr(asset, "current_pricing_detail", None) is None)

        if needs_build:
            # Build instrument
            if "FLOATING" in uid:
                instrument = msi.FloatingRateBond(
                    **common_kwargs,
                    floating_rate_index_name=FLOATING_INDEX_NAME,
                )
            else:
                instrument = msi.FixedRateBond(
                    **common_kwargs,
                    coupon_rate=0.05,
                )

            # Minimal registration payload for a custom asset (keeps your approach)
            #We Add this custom security_type so we can use a translation table and point to the right prices

            payload_item = {
                "unique_identifier": uid,
                "security_type":SECURITY_TYPE_MOCK,
                "snapshot": {"name": uid, "ticker": uid},
            }
            # Your environment already uses this utility; keep it DRY.
            registered = msc.Asset.batch_get_or_register_custom_assets([payload_item])
            asset = registered[0]

            # Attach instrument pricing details
            asset.add_instrument_pricing_details_from_ms_instrument(
                instrument=instrument, pricing_details_date=time_idx
            )

        created_assets.append(asset)

    return created_assets
```

You’ve seen most of this already for asset registration. The new piece is the endpoint that writes **instrument pricing details** onto each asset:

```python
asset.add_instrument_pricing_details_from_ms_instrument(
                instrument=instrument, pricing_details_date=time_idx
            )
```

On the Main Sequence platform, the backend lets you attach an arbitrary dictionary as **asset pricing details**. The objective is to tie non‑standard, asset‑class‑specific pricing metadata to the asset.

```python
class AssetPricingDetail(BasePydanticModel):
    instrument_dump: dict
    pricing_details_date: datetime.datetime
```

To simplify pricing, we provide an **instrument wrapper** around QuantLib:

```python
import  mainsequence.instruments as msi
```

In the code above, we build `mainsequence.instruments.FloatingRateBond` and `mainsequence.instruments.FixedRateBond`. Under the hood, these create QuantLib objects that we can serialize and persist on the platform. From now on, when you fetch an asset, it will carry its **pricing detail**.

Another small but important detail: we assign a custom `security_type` (`SECURITY_TYPE_MOCK`) so we can **indirectly** reference a prices table through a translation rule:

```python
payload_item = {
                "unique_identifier": uid,
                "security_type":SECURITY_TYPE_MOCK,
                "snapshot": {"name": uid, "ticker": uid},
            }
```

### 1.2 Simulating Prices

Before we can build a portfolio, we need price data for our test assets.
Since this tutorial doesn’t use live market data, we’ll generate simulated daily prices with a new DataNode.

1. Create a new file: `src/data_nodes/simulated_daily_close_prices.py`

2. Paste the `SimulatedDailyClosePrices` class below.


```python
# SimulatedDailyClosePrices: generates fake daily OHLCV data for assets
import datetime, pytz, numpy as np, pandas as pd
import mainsequence.client as msc
from mainsequence.tdag import DataNode
from mainsequence.client.models_tdag import UpdateStatistics, ColumnMetaData

UTC = pytz.utc

class SimulatedDailyClosePrices(DataNode):
    def __init__(self, asset_list, *args, **kwargs):
        self.asset_list = asset_list
        super().__init__(*args, **kwargs)

    def dependencies(self): return {}
    def get_asset_list(self): return self.asset_list

    def get_table_metadata(self):
        return msc.TableMetaData(
            identifier="simulated_daily_closes_tutorial",
            description="Simulated daily OHLCV bars for tutorial assets",
        )

    def get_column_metadata(self):
        return [
            ColumnMetaData(column_name="close", dtype="float", description="Simulated close price", label="Close Price"),
            ColumnMetaData(column_name="open", dtype="float", description="Simulated open price", label="Open Price"),
            ColumnMetaData(column_name="high", dtype="float", description="Simulated high price", label="High Price"),
            ColumnMetaData(column_name="low", dtype="float", description="Simulated low price", label="Low Price"),
            ColumnMetaData(column_name="volume", dtype="float", description="Simulated volume", label="Volume"),
            ColumnMetaData(column_name="duration", dtype="float", description="Simulated duration", label="Duration"),
            ColumnMetaData(column_name="open_time", dtype="int", description="Simulated open time", label="Open Time"),
            ColumnMetaData(column_name="first_trade_time", dtype="int", description="Simulated first trade time", label="First Trade Time"),
            ColumnMetaData(column_name="last_trade_time", dtype="int", description="Simulated last trade time", label="Last Trade Time"),
        ]

    def update(self) -> pd.DataFrame:
        us: UpdateStatistics = self.update_statistics
        today = datetime.datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
        yday = today - datetime.timedelta(days=1)

        start = (us.max_time_index_value or datetime.datetime(2024, 1, 1, tzinfo=UTC))
        start = start.replace(hour=0, minute=0, second=0, microsecond=0) + datetime.timedelta(days=1)
        if start > yday:
            return pd.DataFrame()

        idx = pd.date_range(start=start, end=yday, freq="D", tz=UTC, name="time_index")
        frames = []
        for asset in self.asset_list:
            base_price = 100.0
            shocks = np.random.lognormal(mean=0, sigma=0.01, size=len(idx))
            prices = base_price * np.cumprod(shocks)
            tmp = pd.DataFrame({"close": prices}, index=idx)
            tmp["unique_identifier"] = asset.unique_identifier
            frames.append(tmp.set_index("unique_identifier", append=True))

        long = pd.concat(frames)
        long["open"] = long["close"]
        long["high"] = long["close"]
        long["low"] = long["close"]
        long["volume"] = 0.0
        long["duration"] = 6.5
        long["open_time"] = long.reset_index()["time_index"].view("int64").values
        long["first_trade_time"] = long["open_time"]
        long["last_trade_time"] = long["open_time"]

        return long
```

This **DataNode**:

 - Accepts a list of assets (`asset_list`).

 - Produces one simulated daily OHLCV record per asset.

 - Stores the result in a table named `simulated_daily_closes_tutorial`.


Now lets create initial script to run this DataNode to populate the prices table.
We will update it all along the tutorial to build the portfolio step by step and finally run everything end to end.

Create a new runner script: `scripts/run_simulated_prices.py` with the following code:

```python
from src.helpers_mock import ensure_test_assets
from src.data_nodes.simulated_daily_close_prices import SimulatedDailyClosePrices

assets = ensure_test_assets() # Ensure test assets exist
# Instantiate and update the DataNode (platform would orchestrate this)
prices_node = SimulatedDailyClosePrices(asset_list=assets)
prices_node.run(debug_mode=True, force_update=True)
```

## 2) Building an Asset Category

Often you’ll need to operate on a set of assets that cannot be captured easily by simple filters—for example, “all assets in the S&P 500” or “all fixed‑income assets allowed by internal policy.” For simpler filtering, create an **asset category**:

```python
class AssetCategory(BaseObjectOrm, BasePydanticModel):
    id: int
    unique_identifier: str
    display_name: str
    assets: list[Union[int, "Asset"]]
    description: str | None = None
```

An `AssetCategory` is simply a **named collection of assets**. Create and populate it like this:
```python
asset_category=msc.AssetCategory.get_or_create(display_name="Mock Category Assets Tutorial",
                                    unique_identifier="mock_category_assets_tutorial",
                                    )
    asset_category.append_assets(assets=assets)
```

Now let’s update our `scripts/run_simulated_prices.py` to also create our portfolio with Asset Category.

```python
from src.helpers_mock import ensure_test_assets
from src.data_nodes.simulated_daily_close_prices import SimulatedDailyClosePrices
import mainsequence.client as msc

assets = ensure_test_assets()
# Instantiate and update the DataNode (platform would orchestrate this)
prices_node = SimulatedDailyClosePrices(asset_list=assets)
prices_node.run(debug_mode=True, force_update=True)

# Get or create the asset category
asset_category=msc.AssetCategory.get_or_create(display_name="Mock Category Assets Tutorial",
                                unique_identifier="mock_category_assets_tutorial",
                                )
#add assets to the category
asset_category.append_assets(assets=assets)
```

Perfect! You learned how to create an asset category and populate it with assets.

---

## 3) Building a Translation Table

Now that you have:

- Assets registered (ensure_test_assets)

- A simulated prices node (SimulatedDailyClosePrices)

- An asset category (mock_category_assets_tutorial)

…you’re ready to build an Asset Translation Table.
This table defines rules that map asset properties → to a DataNode providing prices.

A **translation table**—a set of rules that route assets to the **correct** Data Node based on their characteristics.

Translation tables let you compose backtests from multiple Data Nodes. For example, you might use one source for `security_type=Equity` and a different one for `security_type=Comdty`. While you can hard‑code this in a node’s `dependencies()`, translation tables make it **extensible** and **data‑driven**.

```python
class AssetFilter(BaseModel):
    security_type: str | None = None
    security_market_sector: str | None = None

    def filter_triggered(self, asset: "Asset") -> bool:
        if self.security_type and asset.security_type != self.security_type:
            return False
        if (
            self.security_market_sector
            and asset.security_market_sector != self.security_market_sector
        ):
            return False
        return True


class AssetTranslationRule(BaseModel):
    asset_filter: AssetFilter
    markets_time_serie_unique_identifier: str
    target_exchange_code: str | None = None

    def is_asset_in_rule(self, asset: "Asset") -> bool:
        return self.asset_filter.filter_triggered(asset)


class AssetTranslationTable(BaseObjectOrm, BasePydanticModel):
    """
    Mirrors the Django model 'AssetTranslationTableModel' in the backend.
    """

    id: int = None
    unique_identifier: str
    rules: list[AssetTranslationRule] = Field(default_factory=list)
```

When building dependencies you can use a `WrapperDataNode` initialized with a translation table, so the same code can route to different Data Nodes **dynamically**.

```python
class WrapperDataNode(DataNode):
    """A wrapper class for managing multiple DataNode objects."""

    def __init__(self, translation_table: AssetTranslationTable, *args, **kwargs):
        """
        Initialize the WrapperDataNode.

        Args:
            time_series_dict: Dictionary of DataNode objects.
        """
        super().__init__(*args, **kwargs)
```

Add the following rule **after** creating the asset category:

```python
# Create Translation Table to link assets to pricing table
translation_table = msc.AssetTranslationTable.get_or_create(
    translation_table_identifier=TRANSLATION_TABLE_IDENTIFIER,
    rules=[
            msc.AssetTranslationRule(
                asset_filter=msc.AssetFilter(
                    security_type=SECURITY_TYPE_MOCK,
                ),
                markets_time_serie_unique_identifier=SIMULATED_PRICES_TABLE,
            ),

        ]
)
```

And also update imports at the top of the file:

```python
from src.helpers_mock import ensure_test_assets, SECURITY_TYPE_MOCK, SIMULATED_PRICES_TABLE, TRANSLATION_TABLE_IDENTIFIER

```

Now you have a translation table that maps all assets with `security_type=MOCK_ASSET` to the `simulated_daily_closes_tutorial` prices table.

---

## 4) Building a Portfolio

A portfolio on the platform looks like this:

```python
class Portfolio(BaseObjectOrm, BasePydanticModel):
    id: int | None = None
    data_node_update: Optional["DataNodeUpdate"]
    signal_data_node_update: Optional["DataNodeUpdate"]
    follow_account_rebalance: bool = False
    backtest_table_price_column_name: str | None = Field(None, max_length=20)
    tags: list["PortfolioTags"] | None = None
    calendar: Optional["Calendar"]
    index_asset: PortfolioIndexAsset
```

**Field notes**  
- `data_node_update`: Data Node that stores the backtest (weights and executed prices).  
- `signal_data_node_update`: Data Node that stores the **signal**’s weights.  
- `backtest_table_price_column_name`: Column name in the backtest table that holds prices.  
- `tags`: Optional tags for organization.  
- `calendar`: Calendar used to compute backtest dates.  
- `index_asset`: The portfolio’s index asset.

Although you **can** build a portfolio with only `data_node_update`, we typically split portfolios into **two Data Nodes**: one for the **signal weights** and one for the **backtest** (execution). This decouples weight generation from execution logic.

Example: for a market‑cap strategy, the **signal** may be computed daily, while the portfolio **rebalances quarterly** or uses **Friday VWAP**. Both portfolios share the **same signal** but differ in execution. Splitting them makes this distinction explicit.

Portfolios also become assets automatically, which means you can build further data pipelines—handy for factor analysis and more.

To import a **Fixed‑Weights** portfolio from the Virtual Fund Builder:

```python
from mainsequence.virtualfundbuilder.contrib.data_nodes.market_cap import FixedWeights, AUIDWeight
```

### 4.1 Building Portfolio Signal Nodes (Optional)

You can skip this and just import `FixedWeights` from SDK and use it in your project. If you plan to build your own signals and portfolios, read on.

A signal Data Node follows the usual node pattern and also implements **two additional methods**:

- **`maximum_forward_fill`**: the longest period a portfolio may forward‑fill your signal’s weights (e.g., a weekly signal with a daily backtest may allow seven days).  
- **`get_explanation`** *(optional)*: human‑readable description used in the UI.

For example, here’s a stripped implementation of a fixed‑weights signal:

```python

from mainsequence.virtualfundbuilder.resource_factory.signal_factory import (
    WeightsBase,
    register_signal_class,
)
from mainsequence.tdag.data_nodes import DataNode
from mainsequence.virtualfundbuilder.models import VFBConfigBaseModel
from datetime import timedelta


class AUIDWeight(VFBConfigBaseModel):
    unique_identifier: str
    weight: float


@register_signal_class(register_in_agent=True)
class FixedWeights(WeightsBase, DataNode):

    def __init__(self, asset_unique_identifier_weights: list[AUIDWeight], *args, **kwargs):
        """
        Args:
            asset_symbol_weights (List[SymbolWeight]): List of SymbolWeights that map asset symbols to weights
        """
        super().__init__(*args, **kwargs)
        self.asset_unique_identifier_weights = asset_unique_identifier_weights

    def maximum_forward_fill(self):
        return timedelta(days=200 * 365)  # Always forward-fill to avoid filling the DB

    def get_explanation(self):
        info = f"<p>{self.__class__.__name__}: Signal uses fixed weights with the following weights:</p>"
        return info
```

### 4.2 Virtual Fund Builder Models

The Virtual Fund Builder ships a set of Pydantic models that configure **assets, prices, signals, weights, execution,** and the final **portfolio build**.

All those models will be used to build the portfolio in the next section. Hrere’s a quick overview of the key models.

#### PricesConfiguration

Holds the rules that the portfolio’s interpolation node will apply and references the translation table:
```python
class PricesConfiguration(VFBConfigBaseModel):
    """
    Configuration for price data handling in a portfolio.

    Attributes:
        bar_frequency_id (str): The frequency of price bars.
        upsample_frequency_id (str): Frequency to upsample intraday data to.
        intraday_bar_interpolation_rule (str): Rule for interpolating missing intraday bars.
        is_live (bool): Boolean flag indicating if the price feed is live.
        translation_table_unique_id (str): The unique identifier of the translation table used to identify the price source.
    """

    bar_frequency_id: str = "1d"
    upsample_frequency_id: str = "1d"  # "15m"
    intraday_bar_interpolation_rule: str = "ffill"
    is_live: bool = False
    translation_table_unique_id: str = "prices_translation_table_1d"
    forward_fill_to_now: bool = False
```

#### AssetsConfiguration

Wraps `PricesConfiguration` and assigns an **asset category**, defining how those assets are interpolated:
```python
class AssetsConfiguration(VFBConfigBaseModel):
    """
    Configuration for assets included in a portfolio.

    Attributes:
        assets_category_unique_id (str):
            Unique Identifier of assets category
        price_type (PriceTypeNames): Type of price used for backtesting.
        prices_configuration (PricesConfiguration): Configuration for price data handling.
    """

    assets_category_unique_id: str
    price_type: PriceTypeNames = PriceTypeNames.CLOSE
    prices_configuration: PricesConfiguration
```

#### PortfolioExecutionConfiguration

Defines execution‑related parameters (for now, commission as a percent):
```python
class PortfolioExecutionConfiguration(VFBConfigBaseModel):
    """
    Configuration for portfolio execution.

    Attributes:
        commission_fee (float): Commission fee percentage.
    """

    commission_fee: float = 0.00018
```

#### BacktestingWeightsConfig

Joins the **signal** with a **rebalance strategy**:
```python
class BacktestingWeightsConfig(VFBConfigBaseModel):
    """
    Configuration for backtesting weights.

    Attributes:
        rebalance_strategy_name (str): Strategy used for rebalancing.
        rebalance_strategy_configuration (Dict): Placeholder dict for the rebalance strategy configuration.
        signal_weights_name (str): Type of signal weights strategy.
        signal_weights_configuration (Dict): Placeholder dict for the signal weights configuration.
    Private attrs:
        _rebalance_strategy_instance (RebalanceStrategyBase | None)
        _signal_weights_instance (WeightsBase | None)
    """

    model_config = ConfigDict(
        frozen=True,  # make the model immutable
        extra="forbid",  # reject unknown fields
        populate_by_name=True,
    )
    rebalance_strategy_name: str = "ImmediateSignal"
    rebalance_strategy_configuration: dict[str, Any] = Field(default_factory=dict)

    signal_weights_name: str = "MarketCap"
    signal_weights_configuration: dict[str, Any] = Field(default_factory=dict)

    _rebalance_strategy_instance: RebalanceStrategyBase | None = PrivateAttr(default=None)
    _signal_weights_instance: WeightsBase | None = PrivateAttr(default=None)
```

#### PortfolioBuildConfiguration

Finally, compose the portfolio from assets, weights, and execution parameters:
```python
class PortfolioBuildConfiguration(VFBConfigBaseModel):
    """
    Main class for configuring and building a portfolio.

    This class defines the configuration parameters needed for
    building a portfolio, including asset configurations, backtesting
    weights, and execution parameters.

    Attributes:
        assets_configuration (AssetsConfiguration): Configuration details for assets.
        portfolio_prices_frequency (str): Frequency to upsample portoflio. Optional.
        backtesting_weights_configuration (BacktestingWeightsConfig): Weights configuration used for backtesting.
        execution_configuration (PortfolioExecutionConfiguration): Execution settings for the portfolio.
    """

    assets_configuration: AssetsConfiguration
    portfolio_prices_frequency: str | None = "1d"

    execution_configuration: PortfolioExecutionConfiguration

    backtesting_weights_configuration: BacktestingWeightsConfig
```

### 4.3 Putting It All Together
Now you’re ready to build the portfolio using the models above and all code you’ve written.

Update your `scripts/run_simulated_prices.py` file to look like this:

```python
from src.helpers_mock import (
    ensure_test_assets,
    SECURITY_TYPE_MOCK,
    SIMULATED_PRICES_TABLE,
    TRANSLATION_TABLE_IDENTIFIER
)
from src.data_nodes.simulated_daily_close_prices import SimulatedDailyClosePrices
import mainsequence.client as msc

from mainsequence.virtualfundbuilder.contrib.data_nodes.market_cap import (
    FixedWeights,
    AUIDWeight,
)
from mainsequence.virtualfundbuilder.portfolio_interface import PortfolioInterface
from mainsequence.virtualfundbuilder.models import (
    AssetsConfiguration,
    PricesConfiguration,
    PortfolioBuildConfiguration,
    BacktestingWeightsConfig,
    PortfolioExecutionConfiguration,
    PortfolioMarketsConfig
)
from mainsequence.virtualfundbuilder.data_nodes import PortfolioStrategy
from mainsequence.virtualfundbuilder.contrib.rebalance_strategies import ImmediateSignal


assets = ensure_test_assets()
# Instantiate and update the DataNode (platform would orchestrate this)
prices_node = SimulatedDailyClosePrices(asset_list=assets)
prices_node.run(debug_mode=True, force_update=True)

# Get or create the asset category
asset_category = msc.AssetCategory.get_or_create(
    display_name="Mock Category Assets Tutorial",
    unique_identifier="mock_category_assets_tutorial",
)
#add assets to the category
asset_category.append_assets(assets=assets)

#Create Translation Table to link assets to pricing table
translation_table = msc.AssetTranslationTable.get_or_create(
    translation_table_identifier=TRANSLATION_TABLE_IDENTIFIER,
    rules=[
            msc.AssetTranslationRule(
                asset_filter=msc.AssetFilter(
                    security_type=SECURITY_TYPE_MOCK,
                ),
                markets_time_serie_unique_identifier=SIMULATED_PRICES_TABLE,
            ),
        ]
)

# build Fixed Weights Portfolio Data Node
weights = [.4, .6]
node_weights_input_1, node_weights_input_2 = [], []
for c, a in enumerate(assets):
    node_weights_input_1.append(AUIDWeight(unique_identifier=a.unique_identifier,
                                            weight=weights[c]))
    node_weights_input_2.append(AUIDWeight(unique_identifier=a.unique_identifier,
                                            weight=weights[c]*1.05))

#assets configuration
prices_configuration = PricesConfiguration(
    bar_frequency_id="1d",
    upsample_frequency_id="1d",
    intraday_bar_interpolation_rule="ffill",
    is_live=False,
    translation_table_unique_id=TRANSLATION_TABLE_IDENTIFIER,
    forward_fill_to_now=False
)

assets_configuration = AssetsConfiguration(
    assets_category_unique_id="mock_category_assets_tutorial",
    price_type="close",
    prices_configuration=prices_configuration,
)

signal_weights_node_1 = FixedWeights(
    asset_unique_identifier_weights=node_weights_input_1,
    signal_assets_configuration=assets_configuration,
)
signal_weights_node_2 = FixedWeights(
    asset_unique_identifier_weights=node_weights_input_2,
    signal_assets_configuration=assets_configuration,
)


#portfolio
def build_portfolio(portfolio_name, signal_node):
    portfolio_execution_configuration = PortfolioExecutionConfiguration(commission_fee=0.0)
    rebalance_strategy = ImmediateSignal(calendar="SIFMAUS")  # US bond market (SIFMA) calendar

    backtest_weight_configuration = BacktestingWeightsConfig.build_from_rebalance_strategy_and_signal_node(
        rebalance_strategy=rebalance_strategy,
        signal_weights_node=signal_node,
)

    portfolio_build_configuration = PortfolioBuildConfiguration(
        assets_configuration=assets_configuration,
        portfolio_prices_frequency="1d",
        execution_configuration=portfolio_execution_configuration,
        backtesting_weights_configuration=backtest_weight_configuration
    )

    portfolio_data_node = PortfolioStrategy(portfolio_build_configuration=portfolio_build_configuration,)
    portfolio_markets_config = PortfolioMarketsConfig(portfolio_name=portfolio_name)


    interface = PortfolioInterface.build_from_portfolio_node(portfolio_node=portfolio_data_node, portfolio_markets_config=portfolio_markets_config)

    res = interface.run(
        patch_build_configuration=False,
        debug_mode=True,
        portfolio_tags=None,
        add_portfolio_to_markets_backend=True,
    )

    return interface.target_portfolio

portfolio_1=build_portfolio(
    portfolio_name="Mock Portfolio 1 With Signals Tutorial",
    signal_node=signal_weights_node_1
)

portfolio_2 = build_portfolio(
    portfolio_name="Mock Portfolio 2 With Signals Tutorial",
    signal_node=signal_weights_node_2
)

portfolio_group = msc.PortfolioGroup.get_or_create(
    display_name="Mock Bond Portfolio with Signals Group",
    unique_identifier="mock_portfolio_signal_group",
    portfolio_ids=[portfolio_1.id, portfolio_2.id],
    description="Mock Portfolio Group for Tutorial"
)
```

Finally, we are ready to run the script and build the portfolios.

Add a new entry to the `.vscode/launch.json` to the `configurations` array to run this script:

(Windows):
```json
{
    "name": "Debug simulated_daily_close_prices",
    "type": "debugpy",
    "request": "launch",
    "program": "${workspaceFolder}\\scripts\\run_simulated_prices.py",
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
    "name": "Debug simulated_daily_close_prices",
    "type": "debugpy",
    "request": "launch",
    "program": "${workspaceFolder}/scripts/run_simulated_prices.py",
    "console": "integratedTerminal",
    "env": {
        "PYTHONPATH": "${workspaceFolder}"
    },
    "python": "${workspaceFolder}/.venv/bin/python"
}
```

Then back to `run_simulated_prices.py` file and run it from the Run and Debug dropdown at the top right (near the play button) and use `Debug simulated_daily_close_prices` configuration.

You’re now ready to use these portfolios in dashboards and deeper analyses.
