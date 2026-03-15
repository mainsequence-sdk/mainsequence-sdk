# Data Contracts

VFB is opinionated about DataFrame shape.

That is a good thing. Portfolio code gets brittle quickly when every signal or rebalance strategy invents its own schema.

This page explains the main DataFrame contracts used by VFB in plain English.

## Why these contracts matter

If you are only consuming built-ins, you do not need to memorize every field.

If you are writing a signal, a custom rebalancer, or importing an external portfolio, you do need to match the expected shape.

Most hard-to-diagnose VFB bugs come from one of these problems:

- wrong index shape
- wrong column names
- wrong identifier field
- wrong timestamp semantics

## 1. Signal weights: the basic signal output

This is the required output of a signal strategy.

Expected shape:

- index: MultiIndex `("time_index", "unique_identifier")`
- columns:
  - `signal_weight`

Example:

```text
time_index                  unique_identifier   signal_weight
2025-01-01T00:00:00Z       BTC                0.60
2025-01-01T00:00:00Z       ETH                0.40
2025-01-02T00:00:00Z       BTC                0.55
```

Important rules:

- `time_index` should be UTC-aware
- `unique_identifier` should match `mainsequence.client.Asset.unique_identifier`
- the output column should be named exactly `signal_weight`

## 2. Portfolio-aligned signal weights: the wide form VFB uses internally

After interpolation to the portfolio index, signal weights become a wide table.

Expected shape:

- index: `time_index`
- columns: one level named `unique_identifier`
- values: weights

This form is useful internally because the rebalance logic wants the full cross-section at each timestamp.

Some values may be `NaN` when forward fill is no longer valid.

That is expected behavior, not necessarily bad data.

## 3. Prices: the VFB bar schema

`InterpolatedPrices` outputs long-form asset prices.

Expected shape:

- index: MultiIndex `("time_index", "unique_identifier")`
- columns typically include:
  - `open_time`
  - `open`
  - `high`
  - `low`
  - `close`
  - `volume`
  - `trade_count`
  - `vwap`
  - `interpolated`

Important details:

- `open_time` is stored in an integer representation for consistency
- some fields may be `NaN` when the source does not provide them
- `interpolated` tells you whether a row was filled rather than directly observed

## 4. Rebalance output: the wide execution schema

Rebalance strategies return a wide DataFrame with a two-level column index.

Expected level-0 keys:

- `weights_current`
- `weights_before`
- `price_current`
- `price_before`
- `volume_current`
- `volume_before`

Expected level-1 keys:

- `unique_identifier`

This is the contract expected by `PortfolioStrategy` after rebalancing.

Why it exists:

- VFB needs both the new state and the previous state
- VFB also needs price and volume context for return and execution metadata

## 5. Postprocessed weights: long form again

After the rebalance output is produced, VFB stacks it back into long form.

Expected shape:

- index: MultiIndex `("time_index", "unique_identifier")`
- columns:
  - `weights_current`
  - `weights_before`
  - `price_current`
  - `price_before`
  - `volume_current`
  - `volume_before`

This form is then used to compute the final portfolio time series and serialize rebalance metadata.

## 6. Final portfolio output

This is what `PortfolioStrategy` stores.

Required columns:

- `close`
- `return`
- `last_rebalance_date`
- `rebalance_weights`
- `rebalance_price`
- `volume`
- `weights_at_last_rebalance`
- `price_at_last_rebalance`
- `volume_at_last_rebalance`

Possible optional columns:

- `close_time`
- `calculated_close`

Important:

- the rebalance metadata fields are stored as JSON strings
- they represent `{unique_identifier: value}` style mappings

This is a compact storage format that preserves execution context without exploding the schema into many per-asset columns.

## 7. `PortfolioFromDF` input contracts

`PortfolioFromDF` accepts two broad modes.

### Weights mode

Use this when the external portfolio is expressed in terms of weights.

Expected columns:

- `close`
- `return`
- `last_rebalance_date`
- `rebalance_weights`
- `rebalance_price`
- `volume`
- `weights_at_last_rebalance`
- `price_at_last_rebalance`
- `volume_at_last_rebalance`

### Positions mode

Use this when the external portfolio is expressed in terms of positions.

Expected columns:

- `close`
- `return`
- `last_rebalance_date`
- `rebalance_positions`
- `rebalance_price`
- `volume`
- `positions_at_last_rebalance`
- `price_at_last_rebalance`
- `volume_at_last_rebalance`

`PortfolioFromDF` will normalize dict-like values into canonical JSON strings before storage.

## Practical quality rules

### Use UTC-aware timestamps

This is the safest default everywhere in VFB.

### Keep identifiers aligned with the asset master

Do not let your strategy invent identifiers that the pricing side does not understand.

### Match the exact column names

Close-enough names usually turn into wasted debugging time.

### Do not confuse long form and wide form

VFB uses both.

- signal output starts long
- rebalancer wants wide
- postprocessing returns to long

That shape switching is part of the design.

## Related Reading

- [Portfolio Pipeline and Configuration](./portfolio_pipeline.md)
- [Prices and Forward Fill](./prices_and_forward_fill.md)
- [Implementation Patterns](./implementation_patterns.md)

Next: [Prices and Forward Fill](./prices_and_forward_fill.md)
