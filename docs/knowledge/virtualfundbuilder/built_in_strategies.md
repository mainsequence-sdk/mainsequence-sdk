# Built-in Strategies

VFB ships with a small set of built-in signals, rebalancers, and price helpers.

This page explains what they are for, when to use them, and what to watch out for.

## The main categories

There are three groups to know:

- **signal strategies**
- **rebalance strategies**
- **price nodes**

## Signal strategies

Signal strategies usually inherit both `WeightsBase` and `DataNode`.

Their output is the desired allocation:

- index: `(time_index, unique_identifier)`
- column: `signal_weight`

### `FixedWeights`

Use this when:

- you already know the target asset weights
- you want a benchmark portfolio
- you want the simplest possible tutorial or smoke-test strategy

What it does:

- stores one constant weight vector
- avoids re-storing the same vector repeatedly
- forward-fills those weights for a very long time by design

Why it is useful:

- it removes almost all signal complexity
- it makes it easy to test the rest of the portfolio pipeline

Gotcha:

- once the constant weights are stored, `update()` may return an empty DataFrame because there is nothing new to write

### `MarketCap`

Use this when:

- you want a universe-driven portfolio
- market cap and liquidity should decide the signal weights

What it does:

- selects assets from an asset category
- filters by top-N market cap
- filters by liquidity metrics such as ATVR
- normalizes the remaining market caps into weights

Optional behavior:

- can apply volatility control

This is the built-in signal that feels closest to a real production strategy.

### `ExternalWeights`

Use this when:

- another system already computes the weights
- those weights arrive as a file rather than through code

What it does:

- reads a CSV from an Artifact
- expects columns like `time_index`, `figi`, and `weight`
- maps `figi` values to `unique_identifier` values using the resolved asset list

This is one of the cleanest examples of how Artifacts and VFB work together.

Related reading:

- [Artifacts](../infrastructure/artifacts.md)

### `ETFReplicator`

Use this when:

- you want to replicate an ETF using a basket of assets

What it does:

- fits a rolling regression
- turns the resulting coefficients into signal weights

Typical inputs:

- target ETF ticker
- rolling window length
- tracking strategy configuration such as LASSO or ElasticNet

This is a strategy-specific node rather than a general-purpose starter pattern, but it is valuable when replication is the goal.

### `IntradayTrend`

Use this when:

- your strategy is intraday by design
- your signal should react to intraday returns

What it does:

- builds weights inside the trading day
- tends to increase exposure when returns align with the chosen direction
- typically closes out by end-of-day

Important:

- this pattern is much more sensitive to bar frequency and calendar assumptions than a daily signal

### `MockSignal`

Use this when:

- you want a lightweight internal test signal

What it does:

- alternates in a simple deterministic way across assets

This is useful for quick pipeline tests, but not a production strategy.

## Rebalance strategies

Rebalance strategies are different from signal strategies.

They are Pydantic model objects, not `DataNode`s.

Their job is to take the desired signal weights and decide what the executable portfolio state should look like over time.

### `ImmediateSignal`

This is the default rebalancer most readers should start with.

Use it when:

- you want the portfolio to rebalance directly into the signal
- you do not need execution staging or participation logic

What it does:

- makes the executed portfolio weights follow the signal weights directly

In practice, this is the built-in rebalance strategy you will most often see in examples and tutorials.

### `TimeWeighted` and `VolumeParticipation`

These names exist in the codebase, but you should treat them carefully.

Current status:

- the models and validation may exist
- the execution logic may still raise `NotImplementedError` depending on your branch

That means they are not safe to treat as production-ready unless you have confirmed your branch implements them.

## Price nodes

VFB also ships with price-side helpers, because the portfolio engine needs prices in a stable format.

### `InterpolatedPrices`

Use this when:

- you want VFB to fetch and normalize price bars from an upstream source
- you need gap filling or interpolation before portfolio construction

What it does:

- loads raw bars
- applies interpolation rules
- standardizes the schema
- produces the price table shape VFB expects

This is the normal price node inside a VFB portfolio pipeline.

### `ExternalPrices`

Use this when:

- prices come from an external Artifact-backed file

What it does:

- reads a CSV with columns such as `time_index`, `figi`, and `price`
- maps the file into the long-form asset price schema VFB expects

This is the price-side equivalent of `ExternalWeights`.

### `get_interpolated_prices_timeseries(...)`

This is a convenience factory.

Use it when:

- you want the SDK to build the appropriate `InterpolatedPrices` node from your asset and price config

That helper is part of why the default `PortfolioStrategy` constructor feels compact even though the actual price pipeline is doing meaningful work underneath.

## Choosing the right built-in

Here is the practical shortcut:

- start with `FixedWeights` if you want the simplest working portfolio
- use `MarketCap` if you want a realistic built-in signal
- use `ExternalWeights` if the weights come from another system
- use `ExternalPrices` if the prices come from another system
- use `ETFReplicator` when replication is the problem
- use `IntradayTrend` when the portfolio truly needs intraday behavior
- use `ImmediateSignal` unless you have a clear reason not to

## Common mistakes

### Treating every built-in as equally mature

Some built-ins are safe defaults. Some are more specialized. Some rebalance classes may be placeholders in certain branches.

### Using an external-file strategy without solving asset mapping first

If your file uses `figi`, but your portfolio assets are not aligned cleanly, the strategy will not behave well.

### Reaching for `IntradayTrend` too early

Intraday strategies are more sensitive to calendars, bar quality, and interpolation settings. They are not the best first VFB project unless you actually need them.

## Related Reading

- [Data Contracts](./data_contracts.md)
- [Prices and Forward Fill](./prices_and_forward_fill.md)
- [Examples](./examples.md)

Next: [Data Contracts](./data_contracts.md)
