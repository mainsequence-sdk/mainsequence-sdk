# Implementation Patterns

This page focuses on the practical patterns that matter when you build or extend VFB code.

It covers:

- writing a new signal strategy
- writing a new rebalance strategy
- importing a portfolio from an external DataFrame
- syncing portfolios to the Markets backend
- keeping strategy configs serializable

## Authoring a new signal strategy

Most signal strategies inherit:

- `WeightsBase`
- `DataNode`

Your class needs to do three things well:

1. expose or resolve its asset universe
2. implement `maximum_forward_fill()`
3. return the canonical `signal_weight` schema

### Minimal template

```python
from datetime import timedelta

import pandas as pd

from mainsequence.tdag.data_nodes import DataNode
from mainsequence.virtualfundbuilder.resource_factory.signal_factory import WeightsBase
from mainsequence.virtualfundbuilder.utils import TIMEDELTA


class MySignal(WeightsBase, DataNode):
    def __init__(self, my_param: int = 10, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.my_param = my_param

    def get_asset_list(self):
        ...

    def maximum_forward_fill(self):
        return timedelta(days=1) - TIMEDELTA

    def update(self) -> pd.DataFrame:
        # return index ["time_index", "unique_identifier"]
        # with a column named "signal_weight"
        ...
```

### Practical rules

- use UTC-aware timestamps
- make sure `unique_identifier` matches the asset master
- do not invent a custom output column name
- keep the forward-fill window economically meaningful

## Authoring a new rebalance strategy

Rebalance strategies are model objects, not `DataNode`s.

They inherit:

- `RebalanceStrategyBase`

Their job is to take a wide matrix of signal weights and a price table and return the wide rebalance schema VFB expects.

### Minimal template

```python
import pandas as pd

from mainsequence.virtualfundbuilder.enums import PriceTypeNames
from mainsequence.virtualfundbuilder.resource_factory.rebalance_factory import RebalanceStrategyBase


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

## Choosing `PortfolioStrategy` vs `PortfolioFromDF`

This is one of the most important design decisions in VFB.

### Use `PortfolioStrategy` when

- you want VFB to compute the portfolio
- you have a signal and a price pipeline
- you want the full rebalance logic inside the SDK

### Use `PortfolioFromDF` when

- the portfolio path already exists
- another system computed `close` and `return`
- you want to ingest and sync the series without recreating the full strategy logic

## `PortfolioFromDF` in practice

To use `PortfolioFromDF`, subclass it and implement:

```python
def get_portfolio_df(self) -> pd.DataFrame:
    ...
```

The returned DataFrame must match either the weights schema or the positions schema expected by VFB.

### Minimal example

```python
import pandas as pd

from mainsequence.virtualfundbuilder.portfolio_nodes import PortfolioFromDF


class MyExternalPortfolio(PortfolioFromDF):
    def get_portfolio_df(self) -> pd.DataFrame:
        idx = pd.to_datetime(
            ["2025-01-31 23:59:59+00:00", "2025-02-28 23:59:59+00:00"]
        )
        return pd.DataFrame(
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
        ).rename_axis("time_index")
```

### What VFB does for you

`PortfolioFromDF` will normalize dict-like metadata columns into canonical JSON strings.

That means you can pass:

- Python dicts
- JSON strings
- Python-literal dict strings

and VFB will standardize them before storage.

## Backend sync behavior

VFB can sync a computed or imported portfolio into the Markets backend.

This happens when you run:

```python
node.run(add_portfolio_to_markets_backend=True)
```

### What VFB creates or updates

Depending on the case, VFB can create or patch:

- a `Portfolio`
- a `PortfolioIndexAsset`

### What metadata it carries

For `PortfolioStrategy`, backend sync can include:

- the portfolio name
- the rebalance calendar
- tags
- a description
- signal strategy name and explanation
- rebalance strategy name

### Why this linkage matters

VFB links the backend portfolio to the `DataNodeUpdate` id.

That is what lets the backend trace which exact computation produced the portfolio.

Practical rule:

- prefer patching an existing synced portfolio rather than deleting and recreating it unnecessarily

## Serialization matters because strategies are injected as instances

VFB chooses direct injection for signal and rebalance strategies.

That is good for clarity, but it creates a configuration challenge:

- raw Python objects are not a good portable config format

### Current VFB approach

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

### Using `PortfolioFromDF` as if it were a generic free-form importer

It still has a contract. The metadata columns must exist and must be meaningful.

### Confusing backend sync with portfolio computation

Backend sync publishes the portfolio object. It does not replace the need to compute the time series correctly.

### Hiding too much in untyped constructors

That makes strategy serialization and tooling much worse.

## Related Reading

- [Data Contracts](./data_contracts.md)
- [Prices and Forward Fill](./prices_and_forward_fill.md)
- [Examples](./examples.md)

Next: [Examples](./examples.md)
