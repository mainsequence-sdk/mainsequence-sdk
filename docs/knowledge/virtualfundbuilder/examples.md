# Examples

The fastest way to understand VFB is to see a few concrete patterns.

This page turns the source examples into a reader-friendly catalog: what each example is for, when to use it, and what to pay attention to.

## Example 1: Minimal portfolio with built-ins

Use this when:

- you want the smallest realistic VFB portfolio
- you want to understand the normal configuration flow

Pattern:

- signal: `MarketCap`
- rebalancer: `ImmediateSignal`
- config: explicit `PortfolioConfiguration`

```python
from mainsequence.virtualfundbuilder import register_default_strategies
from mainsequence.virtualfundbuilder.contrib.data_nodes import MarketCap
from mainsequence.virtualfundbuilder.contrib.rebalance_strategies import ImmediateSignal
from mainsequence.virtualfundbuilder.models import (
    AssetsConfiguration,
    BacktestingWeightsConfig,
    PortfolioBuildConfiguration,
    PortfolioConfiguration,
    PortfolioExecutionConfiguration,
    PortfolioMarketsConfig,
    PricesConfiguration,
)
from mainsequence.virtualfundbuilder.enums import PriceTypeNames
from mainsequence.virtualfundbuilder.portfolio_nodes import PortfolioStrategy

register_default_strategies()

shared_assets_config = AssetsConfiguration(
    assets_category_unique_id="crypto",
    price_type=PriceTypeNames.CLOSE,
    prices_configuration=PricesConfiguration(
        bar_frequency_id="1d",
        upsample_frequency_id="1d",
        intraday_bar_interpolation_rule="ffill",
        translation_table_unique_id="prices_translation_table_1d",
        forward_fill_to_now=False,
    ),
)

signal_node = MarketCap(
    signal_assets_configuration=shared_assets_config,
    volatility_control_configuration=None,
    num_top_assets=10,
)

portfolio_config = PortfolioConfiguration(
    portfolio_build_configuration=PortfolioBuildConfiguration(
        assets_configuration=shared_assets_config,
        portfolio_prices_frequency="1d",
        execution_configuration=PortfolioExecutionConfiguration(commission_fee=0.00018),
        backtesting_weights_configuration=BacktestingWeightsConfig(
            rebalance_strategy_instance=ImmediateSignal(calendar_key="24/7"),
            signal_weights_instance=signal_node,
        ),
    ),
    portfolio_markets_configuration=PortfolioMarketsConfig(
        portfolio_name="Demo VFB Portfolio - Direct Config"
    ),
)

node = PortfolioStrategy(portfolio_configuration=portfolio_config)
```

Why this example matters:

- it shows the normal VFB shape without extra indirection
- it makes the signal, price config, and rebalance logic all visible in one place

## Example 2: Fixed weights portfolio

Use this when:

- you want a benchmark or tutorial portfolio
- you want to test the portfolio engine with minimal signal complexity

Pattern:

- signal: `FixedWeights`
- rebalancer: `ImmediateSignal`

What it shows:

- how to inject explicit `(asset, weight)` pairs
- how to let the signal define the asset universe directly

Why this example matters:

- it is one of the cleanest smoke tests for VFB

## Example 3: Portfolio from an external DataFrame

Use this when:

- you already have `close` and `return`
- you want to ingest a portfolio instead of building it inside VFB

Pattern:

- subclass `PortfolioFromDF`
- return a correctly shaped DataFrame

```python
import pandas as pd

from mainsequence.virtualfundbuilder.portfolio_nodes import PortfolioFromDF


class MyExternalPortfolio(PortfolioFromDF):
    def get_portfolio_df(self) -> pd.DataFrame:
        idx = pd.to_datetime(
            ["2025-01-31 23:59:59+00:00", "2025-02-28 23:59:59+00:00"]
        )
        return (
            pd.DataFrame(
                index=idx,
                data={
                    "close": [100.0, 101.5],
                    "return": [0.0, 0.015],
                    "last_rebalance_date": ["2025-01-31", "2025-02-28"],
                    "rebalance_weights": [{"BTC": 0.6}, {"BTC": 0.6}],
                    "rebalance_price": [{"BTC": 42000}, {"BTC": 43000}],
                    "volume": [{"BTC": 0}, {"BTC": 0}],
                    "weights_at_last_rebalance": [{"BTC": 0.6}, {"BTC": 0.6}],
                    "price_at_last_rebalance": [{"BTC": 42000}, {"BTC": 43000}],
                    "volume_at_last_rebalance": [{"BTC": 0}, {"BTC": 0}],
                },
            )
            .rename_axis("time_index")
        )
```

Why this example matters:

- it shows the import path clearly
- it helps separate "portfolio engine" use cases from "portfolio ingestion" use cases

## Example 4: External weights from an Artifact

Use this when:

- the weights come from another system
- the natural handoff is a file, not a table or API

Pattern:

- signal: `ExternalWeights`
- weight file stored as an Artifact

Expected file shape:

- `time_index`
- `figi`
- `weight`

What it shows:

- how VFB consumes Artifact-backed operational inputs
- how `figi` values are mapped into platform asset identifiers

Related reading:

- [Artifacts](../infrastructure/artifacts.md)

## Example 5: ETF replication

Use this when:

- your goal is to replicate an ETF return stream with a basket of assets

Pattern:

- signal: `ETFReplicator`
- rebalancer: `ImmediateSignal`

Typical configuration:

- ETF ticker
- regression window
- tracking strategy such as LASSO or ElasticNet

Why this example matters:

- it shows that VFB can host more sophisticated signal-generation logic than fixed weights or simple filters

## Example 6: Intraday trend strategy

Use this when:

- the strategy truly needs intraday behavior

Pattern:

- signal: `IntradayTrend`
- intraday prices such as `30m`
- intraday portfolio frequency such as `30m`

What to pay attention to:

- trading calendar
- raw bar frequency
- target portfolio frequency
- interpolation settings

Why this example matters:

- it shows how much more sensitive intraday VFB workflows are to calendar and price-quality assumptions

## Picking the right example to start from

If you are deciding where to begin, use this sequence.

### Start with `FixedWeights`

Best for:

- learning VFB
- debugging the portfolio pipeline
- reducing signal complexity

### Move to `MarketCap`

Best for:

- learning how a more realistic built-in signal behaves
- working with an asset-category-driven universe

### Use `ExternalWeights` or `PortfolioFromDF` when the upstream system already exists

Best for:

- integration projects
- operational workflows
- migrations

### Leave intraday and replication use cases for later

Best for:

- teams that already understand the daily pipeline

## Practical prerequisites

Most VFB examples assume you already have:

- assets in the platform
- a translation table that points to valid price sources
- price data that matches the expected frequency
- calendars that match the market you are modeling

When those pieces are missing, the example code will look fine but the portfolio will still not behave correctly.

## Related Reading

- [Portfolio Pipeline and Configuration](./portfolio_pipeline.md)
- [Built-in Strategies](./built_in_strategies.md)
- [Implementation Patterns](./implementation_patterns.md)
