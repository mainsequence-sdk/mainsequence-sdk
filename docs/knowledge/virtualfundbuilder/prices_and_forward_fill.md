# Prices and Forward Fill

Price handling is one of the most misunderstood parts of VFB.

This page explains how prices move through the portfolio engine and why "forward fill" in VFB actually means three different things.

## The two-stage price process

VFB does not go straight from raw bars to portfolio returns.

It uses a two-stage process.

### Stage 1: clean and normalize bars

This is handled by `InterpolatedPrices`.

Its job is to:

- fetch raw bars
- build a schedule for each asset
- fill gaps according to the configured interpolation rule
- normalize the output schema

At the end of this stage, VFB has a clean long-form price table.

### Stage 2: align prices to the portfolio timeline

This happens inside `PortfolioStrategy`.

Its job is to:

- build the portfolio timeline
- reindex prices to that timeline
- forward-fill values so each portfolio timestamp has a usable price

This is why price handling is not just a single helper call. The portfolio engine needs prices that are both clean and aligned.

## Where prices come from

In the default setup, prices are usually resolved from:

- an asset category
- a translation table
- an upstream bars source

The translation table matters because the traded asset universe and the upstream price source are not always identical objects.

VFB needs a stable way to say:

- "for this asset, use this upstream time series"

For the routing model in detail, see [Translation Tables](../markets/translation_tables.md).

## `InterpolatedPrices` in practice

`InterpolatedPrices` is the main price node in the VFB pipeline.

It supports two broad raw-bar modes:

- daily bars
- intraday bars

### Daily bars

For daily bars, interpolation is built around daily schedules and session structure.

### Intraday bars

For intraday bars, interpolation fills missing bars inside the trading schedule and can also aggregate bars upward when needed.

That means intraday configurations demand more attention:

- correct calendar
- correct raw bar frequency
- correct target frequency
- realistic gap-fill assumptions

## The three forward-fill systems

This is the part most people mix up.

VFB has three different fill systems:

1. bar interpolation
2. price forward-fill to now
3. signal validity forward-fill

They are separate because they solve different business problems.

## 1. Bar interpolation

Owned by:

- `PricesConfiguration.intraday_bar_interpolation_rule`

Meaning:

- fill missing bars inside the price series

Typical use:

- make the bar series dense enough for downstream portfolio logic

This is about the **quality of the price series itself**.

## 2. Price forward-fill to now

Owned by:

- `PricesConfiguration.forward_fill_to_now`

Meaning:

- extend the price series to the current time and carry the last price forward

Typical use:

- live valuation continuity

This is about **valuation continuity**, not about new market information.

## 3. Signal validity forward-fill

Owned by:

- `maximum_forward_fill()` on the signal strategy

Meaning:

- how long the portfolio is allowed to keep using stale signal weights

Typical use:

- prevent stale weights from silently persisting too long

This is about **economic validity**.

!!! warning "The common mistake"
    `forward_fill_to_now=True` does not make an old signal valid. It only extends the price path.

## Why `maximum_forward_fill()` matters so much

Every signal strategy must decide how long its weights remain meaningful.

Examples:

- daily signal: maybe one day
- weekly signal: maybe seven days
- fixed benchmark weights: effectively forever
- intraday signal: maybe one bar frequency

If this window is wrong, the portfolio can either:

- stop updating too early, or
- keep using stale allocations much longer than intended

## Why VFB subtracts `TIMEDELTA`

You will often see patterns like:

```python
return timedelta(days=1) - TIMEDELTA
```

That small subtraction exists to avoid boundary problems when VFB checks whether a weight is still valid.

Without it, exact timestamp equality can produce surprising invalidation behavior.

## Daily vs intraday timelines

The portfolio timeline is not built the same way in daily and intraday mode.

### Daily

Daily portfolios use the rebalance strategy calendar and effectively anchor to market-close timestamps.

This is why the rebalance calendar is a meaningful part of the portfolio definition.

### Intraday

Intraday portfolios use a generated frequency-based timeline.

That is powerful, but it also means intraday workflows need extra care around:

- trading session assumptions
- schedule clipping
- interpolation expectations

## A practical debugging checklist

When a VFB portfolio looks wrong, check these in order.

### 1. Do prices exist for the relevant assets?

If the upstream bars are missing, nothing else can save the portfolio.

### 2. Is the translation table correct?

If the asset-to-price mapping is wrong, the rest of the pipeline will look broken even when the raw data exists.

### 3. Are bars being interpolated as expected?

Inspect the `interpolated` column. If you see either too many synthetic rows or unexpected gaps, the schedule or interpolation rule may be wrong.

### 4. Is `forward_fill_to_now` masking a deeper issue?

It is useful for valuation continuity, but it can also hide the fact that no genuinely new prices have arrived.

### 5. Are the signal weights still valid?

If the signal has expired according to `maximum_forward_fill()`, the portfolio should stop using it. That is usually correct behavior.

## Practical rules

### Keep price interpolation and signal validity conceptually separate

They solve different business problems.

### Start with daily portfolios unless you truly need intraday logic

Intraday portfolios are more fragile because they depend more heavily on calendars and bar quality.

### Treat translation tables as part of the portfolio plumbing

If they are wrong, the portfolio is wrong even if your signal logic is perfect.

## Related Reading

- [Data Contracts](./data_contracts.md)
- [Implementation Patterns](./implementation_patterns.md)
- [Examples](./examples.md)

Next: [Implementation Patterns](./implementation_patterns.md)
