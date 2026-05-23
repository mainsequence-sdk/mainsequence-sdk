# Implementation Patterns

This page focuses on the practical patterns that matter when you build or extend portfolio code.

It covers:

- writing a new signal strategy
- writing a new rebalance strategy
- importing a portfolio from an external DataFrame
- syncing portfolios to the Markets backend
- keeping strategy configs serializable

## Authoring a new signal strategy

Most signal strategies inherit `SignalWeights`.

Your class needs to do three things well:

1. expose or resolve its asset universe
2. implement `maximum_forward_fill()`
3. return the canonical `signal_weight` schema

### Minimal template

```python
from datetime import timedelta

import pandas as pd

from mainsequence.tdag import DataNodeConfiguration
from mainsequence.markets.portfolios.data_nodes import SignalWeights
from mainsequence.markets.portfolios.utils import TIMEDELTA


class MySignalConfig(DataNodeConfiguration):
    my_param: int = 10


class MySignal(SignalWeights):
    def __init__(self, config: MySignalConfig):
        self.my_param = config.my_param
        super().__init__(config=config)

    def get_asset_list(self):
        ...

    def maximum_forward_fill(self):
        return timedelta(days=1) - TIMEDELTA

    def update(self) -> pd.DataFrame:
        # return index ["time_index", "unique_identifier"]
        # with a column named "signal_weight"
        ...
```

That index shape is the portfolio signal contract because Portfolios signals are keyed by
asset. Other DataNode tables can use additional identity dimensions after
`time_index`.

### Practical rules

- use UTC-aware timestamps
- make sure `unique_identifier` matches the asset master
- do not invent a custom output column name
- keep the forward-fill window economically meaningful

## Authoring a new rebalance strategy

Rebalance strategies are model objects, not `DataNode`s.

They inherit:

- `RebalanceStrategyBase`

Their job is to take a wide matrix of signal weights and a price table and return the wide rebalance schema Portfolios expects.

### Minimal template

```python
import pandas as pd

from mainsequence.markets.portfolios.enums import PriceTypeNames
from mainsequence.markets.portfolios.rebalance_strategy import RebalanceStrategyBase


class MyRebalancer(RebalanceStrategyBase):
    def apply_rebalance_logic(
        self,
        last_rebalance_weights: pd.DataFrame | None,
        start_date,
        end_date,
        signal_weights: pd.DataFrame,
        prices_df: pd.DataFrame,
        price_type: PriceTypeNames,
    ) -> pd.DataFrame:
        ...
```

### Practical rules

- start by copying `ImmediateSignal` if you need a new rebalance style
- return the full required wide schema, not a partial subset
- keep calendar assumptions explicit

## Choosing computed vs imported portfolio values

This is one of the most important design decisions in Portfolios.

### Use computed portfolio values when

- you want Portfolios to compute the portfolio
- you have a signal and a price pipeline
- you want the full rebalance logic inside the SDK

Instantiate `PortfoliosDataNode` with `portfolio_configuration`.

### Use imported portfolio values when

- the portfolio path already exists
- another system computed `close` and `return`
- you want to ingest the series without recreating the full strategy logic

Use `PortfoliosDataNode.set_portfolio_values_frame(...)`.

The DataFrame must match the canonical `PortfoliosDataNode` values schema.

### Minimal example

```python
import pandas as pd

from mainsequence.markets.portfolios.data_nodes import PortfoliosDataNode

idx = pd.to_datetime(
    ["2025-01-31 23:59:59+00:00", "2025-02-28 23:59:59+00:00"]
)
portfolio_values = pd.DataFrame(
    index=idx,
    data={
        "close": [100.0, 101.5],
        "return": [0.0, 0.015],
    },
).rename_axis("time_index")

node = PortfoliosDataNode()
node.set_portfolio_values_frame(
    portfolio_values,
    portfolio_index_asset_unique_identifier="portfolio:external-demo",
)
```

### What Portfolios does for you

`PortfoliosDataNode` adds the portfolio index asset identifier and normalizes
the frame into the canonical portfolio values table.

## Backend sync behavior

Portfolios can sync a computed or imported portfolio into the Markets backend.

This happens when you run:

```python
node.run()
```

### What Portfolios creates or updates

Depending on the case, Portfolios can create or patch:

- a `Portfolio`
- a `PortfolioIndexAsset`

### What metadata it carries

For `PortfoliosDataNode`, backend sync can include:

- the portfolio name
- the rebalance calendar
- tags
- a description
- signal strategy name and explanation
- rebalance strategy name

### Why this linkage matters

Portfolios links the backend portfolio to the `DataNodeUpdate` id.

That is what lets the backend trace which exact computation produced the portfolio.

Practical rule:

- prefer patching an existing synced portfolio rather than deleting and recreating it unnecessarily

## Serialization matters because strategies are injected as instances

Portfolios chooses direct injection for signal and rebalance strategies.

That is good for clarity, but it creates a configuration challenge:

- raw Python objects are not a good portable config format

### Current Portfolios approach

- rebalance strategies serialize naturally because they are Pydantic models
- signal strategies are serialized through `build_configuration_json_schema()`

### What this means for authors

If you want your signal strategy to serialize well:

- use a real typed constructor signature
- avoid hiding everything behind `**kwargs`
- use docstrings that describe arguments clearly
- prefer Pydantic models for nested config

That improves both schema quality and tooling quality.

## Common mistakes

### Returning the wrong signal schema

This is still the most common custom-strategy mistake.

### Using imported portfolio values as if they were free-form data

They still have a contract. `close` and `return` must be meaningful.

### Confusing backend sync with portfolio computation

Backend sync publishes the portfolio object. It does not replace the need to compute the time series correctly.

### Hiding too much in untyped constructors

That makes strategy serialization and tooling much worse.

## Related Reading

- [Data Contracts](./data_contracts.md)
- [Prices and Forward Fill](./prices_and_forward_fill.md)
- [Examples](./examples.md)

Next: [Examples](./examples.md)
