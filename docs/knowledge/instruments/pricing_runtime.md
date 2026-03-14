# Pricing Runtime

Once curves, fixings, and pricing details are in place, runtime pricing becomes straightforward.

This page explains what actually happens when you call `.price()` on an instrument.

## 1. The runtime path

The runtime usually follows this sequence:

1. Resolve the index UID used by the instrument.
2. Look up its `IndexSpec`.
3. Load the correct curve and fixings from platform storage.
4. Materialize QuantLib objects.
5. Price the instrument or portfolio.

The important point is that pricing is mostly read-only. It consumes what ETL has already written.

## 2. Curves

The main entry point is:

- `build_zero_curve(target_date, index_identifier)`

At a high level, it:

- resolves the `IndexSpec`,
- loads the stored curve through `MSInterface`,
- decompresses the stored payload,
- turns curve points into a QuantLib term structure.

One useful detail: the effective curve date returned by storage may differ from the date you asked for. The runtime uses the effective date of the actual stored observation.

## 3. Fixings

Floating-rate pricing usually needs past fixings as well as a forward curve.

That is why `get_index(..., hydrate_fixings=True)` is important. It loads historical fixings into the QuantLib index so you do not get missing-fixing errors when pricing floaters or swaps.

If your floaters are failing unexpectedly, missing fixings are one of the first things to check.

## 4. Fixed-rate bonds

A fixed-rate bond can usually be priced in two ways:

- from a flat yield,
- or from a curve.

That makes fixed bonds a good starting point for validation because you can compare curve-based and yield-based pricing more easily.

## 5. Floating-rate bonds

A floating-rate bond needs more wiring.

It depends on:

- a registered index UID,
- a working `IndexSpec`,
- accessible fixings,
- accessible curve data.

Once those are in place, `FloatingRateBond.price()` can build the right runtime objects and price off the index curve and fixings.

## 6. Interest-rate swaps

The same general pattern applies to a vanilla interest-rate swap.

The most important field is:

- `float_leg_index_name`

If that UID is not registered through `IndexSpec`, swap pricing fails for the same reason as a floater.

## 7. Positions

`Position` is the portfolio wrapper.

It is useful when you want to:

- aggregate PV across multiple instruments,
- inspect per-line pricing,
- merge future cashflows.

This is usually the layer that maps best to portfolio analytics and MCP-style workflows.

## 8. Practical runtime rules

### Set a valuation date explicitly

Always set valuation date before pricing. Do not rely on incidental defaults.

### Treat valuation date as instance-specific

As a practical rule, use one instrument instance per valuation date. Some runtime objects are cached, and changing the valuation date repeatedly on the same instance can be harder to reason about.

### Know the caches exist

The runtime caches:

- stored curves,
- stored fixings,
- and some instrument-level pricing calculations.

That is useful for performance, but it also means debugging should start with a clear valuation date and clear understanding of which data snapshot you are using.

### Be careful with fallback behavior

If `USE_LAST_OBSERVATION_MS_INSTRUMENT=true`, the runtime may use the last available stored observation when exact data is missing.

That can be operationally useful, but it changes audit semantics. Use it on purpose.

## 9. A good validation habit

After registration, validate the chain early:

- load one curve,
- load one index,
- price one simple instrument.

That catches wiring errors much earlier than waiting for a full portfolio run to fail.

Next: [Examples](./examples.md)
