# Virtual Fund Builder

Virtual Fund Builder, or **VFB**, is the portfolio-construction layer of the Main Sequence SDK.

Its job is simple to describe and important to get right: take prices, take a signal, decide how that signal becomes trades or holdings, and turn the result into a portfolio time series that the rest of the platform can use.

If `DataNode`s are how Main Sequence produces datasets, VFB is how Main Sequence turns those datasets into portfolios.

## What VFB actually does

VFB sits between the data layer and the Markets layer.

In a typical workflow:

1. `DataNode`s produce prices, signals, or supporting market data.
2. VFB reads those inputs and builds a portfolio path.
3. The resulting portfolio can be stored, inspected, and optionally synced to the Markets backend.

The main output is a portfolio time series with fields such as:

- `close`
- `return`
- `last_rebalance_date`
- serialized rebalance metadata

That makes VFB the bridge between "I have data" and "I have a portfolio the platform understands."

## The core mental model

Every VFB portfolio combines four things:

- **prices**
- **signal weights**
- **rebalance logic**
- **execution fees**

At the center of that process is `PortfolioStrategy`, which is itself a `DataNode`.

That means a VFB portfolio is not a special side system. It lives inside the same DAG model as the rest of the SDK.

## Two entrypoints to know

### `PortfolioStrategy`

This is the normal VFB entrypoint.

Use it when you want VFB to compute the portfolio for you from:

- a signal strategy
- a rebalance strategy
- a price pipeline
- a fee model

### `PortfolioFromDF`

This is the import path.

Use it when you already have a portfolio time series from somewhere else and want to:

- store it in the platform
- normalize the metadata columns
- optionally sync it to the Markets backend

## Where readers usually get confused

The same points come up again and again:

- VFB does not replace `DataNode`s. It depends on them.
- Signal strategies and rebalance strategies are different roles.
- Price interpolation and signal validity are different concepts.
- The portfolio timeline is not just "whatever timestamps were in the source bars."
- VFB expects a few strict DataFrame contracts.

This section exists to make those edges clear.

## Recommended reading order

Start here, then continue in this order:

1. [Portfolio Pipeline and Configuration](./portfolio_pipeline.md)
2. [Built-in Strategies](./built_in_strategies.md)
3. [Data Contracts](./data_contracts.md)
4. [Prices and Forward Fill](./prices_and_forward_fill.md)
5. [Implementation Patterns](./implementation_patterns.md)
6. [Examples](./examples.md)

## How this section connects to the tutorial

The tutorial chapter on portfolios introduces VFB from the point of view of a worked example.

This knowledge section does something different:

- it explains the architecture
- it explains the contracts
- it explains the built-ins
- it explains the operational patterns that matter when you build real portfolios

If you came here from the tutorial, this is the right place to slow down and understand the moving parts.

## Related Reading

- [Part 4.3 — Markets — Portfolios and Virtual Funds](../../tutorial/getting_started_4_2.md)
- [Data Nodes](../data_nodes.md)
- [Artifacts](../infrastructure/artifacts.md)
- [Instruments](../instruments/index.md)

Next: [Portfolio Pipeline and Configuration](./portfolio_pipeline.md)
