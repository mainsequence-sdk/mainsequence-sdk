# Portfolio Pipeline and Configuration

This page explains how a VFB portfolio is built from end to end.

The single most useful way to think about VFB is this:

- a signal says what you want to hold
- a rebalance strategy says how that becomes an actual portfolio
- prices determine what those holdings are worth over time

## The pipeline

At a high level, `PortfolioStrategy` does five things:

1. resolves the asset universe
2. builds or fetches the relevant price series
3. computes or loads signal weights
4. applies rebalance logic
5. computes portfolio `return` and `close`, then stores the result

In code terms, the pipeline looks like this:

- `AssetsConfiguration.prices_configuration` drives the price path
- a `WeightsBase` instance provides signal weights
- a `RebalanceStrategyBase` instance provides execution logic
- `PortfolioExecutionConfiguration` provides the fee model

## The main orchestrator: `PortfolioStrategy`

`PortfolioStrategy` is the normal VFB portfolio node.

It is a `DataNode`, so it:

- participates in the DAG like any other node
- has dependencies
- persists output
- can be run locally or through jobs

You instantiate it with a single top-level config:

```python
portfolio_node = PortfolioStrategy(portfolio_configuration=config)
```

## The top-level config: `PortfolioConfiguration`

This config has two halves:

- `portfolio_build_configuration`
- `portfolio_markets_configuration`

That split is intentional.

The build side describes how the portfolio behaves.
The markets side describes how the resulting portfolio should appear in the backend.

### `PortfolioBuildConfiguration`

This is the core build config.

It contains:

- `assets_configuration`
- `portfolio_prices_frequency`
- `execution_configuration`
- `backtesting_weights_configuration`

#### `AssetsConfiguration`

This defines:

- which assets the portfolio trades
- which price field is used for valuation
- how prices are fetched and interpolated

Important fields:

- `assets_category_unique_id`
- `price_type`
- `prices_configuration`

If `assets_category_unique_id` is present, VFB resolves the portfolio universe from that asset category.

If it is missing, the signal node is expected to define the universe itself through `get_asset_list()`.

#### `PricesConfiguration`

This tells VFB how to get prices into a shape the portfolio engine can use.

Important fields:

- `bar_frequency_id`
- `upsample_frequency_id`
- `intraday_bar_interpolation_rule`
- `translation_table_unique_id`
- `forward_fill_to_now`

This config controls the **price path**, not the economic validity of the signal.

#### `BacktestingWeightsConfig`

This is where you inject:

- `signal_weights_instance`
- `rebalance_strategy_instance`

This is a defining VFB design choice.

VFB uses **direct instance injection**, not string-based runtime lookups.

That gives you:

- explicit dependencies
- clearer type validation
- less ambiguity when reading code

#### `PortfolioExecutionConfiguration`

This contains the fee model.

The main field is:

- `commission_fee`

VFB applies fee drag based on turnover:

- `abs(weights_current - weights_before) * commission_fee`

### `PortfolioMarketsConfig`

This config describes how the portfolio should appear when synced into the Markets backend.

Important fields:

- `portfolio_name`
- `front_end_details`

This is metadata, not portfolio logic.

## Signal strategy vs rebalance strategy

These two roles are different and should stay different in your mind.

### Signal strategy

Usually inherits both:

- `WeightsBase`
- `DataNode`

Its job is to say:

- "At this timestamp, I want these asset weights."

Its required output is:

- index: `(time_index, unique_identifier)`
- column: `signal_weight`

### Rebalance strategy

Inherits `RebalanceStrategyBase`.

It is not a `DataNode`.

Its job is to say:

- "Given the signal and the available prices, this is how the portfolio transitions from previous holdings to current holdings."

The built-in strategy most readers should start with is:

- `ImmediateSignal`

This means:

- rebalance directly into the signal weights

## How the asset universe is resolved

There are two common patterns.

### Pattern 1: category-driven universe

Use `assets_category_unique_id` inside `AssetsConfiguration`.

This is the cleanest pattern when the portfolio should trade a known category of assets.

### Pattern 2: signal-driven universe

Leave `assets_category_unique_id=None` and let the signal node define `get_asset_list()`.

This is useful when:

- the signal selects its own universe
- the universe is dynamic
- the strategy is more self-contained than category-driven

## How the portfolio timeline is constructed

The portfolio does not simply run on the raw source timestamps.

VFB builds a derived timeline.

### Daily portfolios

When `upsample_frequency_id` is daily:

- VFB uses the rebalance strategy calendar
- the effective timestamps are session close timestamps

This is important because the rebalance calendar is part of the portfolio definition.

### Intraday portfolios

When `upsample_frequency_id` is intraday:

- VFB uses a `pd.date_range(...)` style timeline
- calendar alignment is less strict than in daily mode

That works, but it also means intraday portfolios need more care around calendar assumptions.

## A minimal configuration example

This is the smallest realistic VFB pattern worth remembering:

```python
from mainsequence.virtualfundbuilder.contrib.data_nodes.market_cap import AUIDWeight, FixedWeights
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
from mainsequence.virtualfundbuilder.portfolio_nodes import PortfolioStrategy

signal_assets_config = AssetsConfiguration(
    assets_category_unique_id=None,
    prices_configuration=PricesConfiguration(
        bar_frequency_id="1d",
        upsample_frequency_id="1d",
        translation_table_unique_id="prices_translation_table_1d",
    ),
)

signal = FixedWeights(
    signal_assets_configuration=signal_assets_config,
    asset_unique_identifier_weights=[
        AUIDWeight(unique_identifier="BTC", weight=0.6),
        AUIDWeight(unique_identifier="ETH", weight=0.4),
    ],
)

config = PortfolioConfiguration(
    portfolio_build_configuration=PortfolioBuildConfiguration(
        assets_configuration=signal_assets_config,
        portfolio_prices_frequency="1d",
        execution_configuration=PortfolioExecutionConfiguration(commission_fee=0.0),
        backtesting_weights_configuration=BacktestingWeightsConfig(
            signal_weights_instance=signal,
            rebalance_strategy_instance=ImmediateSignal(calendar_key="24/7"),
        ),
    ),
    portfolio_markets_configuration=PortfolioMarketsConfig(
        portfolio_name="Demo Fixed Weights Portfolio",
    ),
)

portfolio_node = PortfolioStrategy(portfolio_configuration=config)
```

That example already shows the VFB design clearly:

- price settings live in one place
- the signal is injected as an object
- the rebalance rule is injected as an object
- the portfolio node becomes the actual persisted output

## What happens when you run the node

`PortfolioStrategy.run(...)` behaves like a normal `DataNode` run, but VFB adds an important option:

- `add_portfolio_to_markets_backend`

When enabled, VFB can create or update a portfolio entity in the Markets backend and attach metadata such as:

- portfolio name
- calendar
- signal description
- rebalance strategy name
- tags

That backend sync is covered in more detail in [Implementation Patterns](./implementation_patterns.md).

## Practical rules

### Keep the signal and the price plumbing aligned

If the signal emits `unique_identifier` values that do not match the assets used by the price translation table, the portfolio will fail in confusing ways.

### Treat the rebalance calendar as part of the strategy definition

A daily portfolio is not fully defined by the signal alone. The rebalance calendar changes how the time series is built.

### Keep build logic separate from backend metadata

`PortfolioBuildConfiguration` should define behavior.
`PortfolioMarketsConfig` should define presentation and backend metadata.

That separation keeps configs easier to reason about.

## Related Reading

- [Built-in Strategies](./built_in_strategies.md)
- [Data Contracts](./data_contracts.md)
- [Prices and Forward Fill](./prices_and_forward_fill.md)

Next: [Built-in Strategies](./built_in_strategies.md)
