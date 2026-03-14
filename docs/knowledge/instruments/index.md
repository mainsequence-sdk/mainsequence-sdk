# Instruments

The instruments layer is where stored market data becomes something you can actually price.

In Main Sequence, this is not just a QuantLib wrapper. It is a full path from:

- market data production,
- platform storage,
- index and curve registration,
- asset pricing details,
- runtime pricing of bonds, swaps, and positions.

This section is written from a reader's point of view. The goal is to explain how the pieces fit together, what must be configured, and where teams usually break the chain.

!!! note "Current scope"
    The current stack is strongest around rates instruments: fixed-rate bonds, floating-rate bonds, vanilla interest-rate swaps, and position-level aggregation. The same architecture can later support more instrument types.

## The big picture

There are two worlds in this system.

### 1. The market data world

This world produces and stores market objects:

- discount curves,
- fixing time series,
- and other pricing inputs.

It is powered by `DataNode`s such as `DiscountCurvesNode` and `FixingRatesNode`.

### 2. The pricing world

This world consumes those stored objects and turns them into runtime pricing inputs:

- QuantLib curves,
- QuantLib indices,
- priceable instrument objects,
- portfolio positions.

It is powered by components such as `MSInterface`, `IndexSpec`, and the instrument models under `mainsequence.instruments`.

## The two handshakes that matter

Most implementation problems happen in one of these two bridges.

### Handshake A: market data to pricing indices

You need two registrations:

- ETL registration, which tells the system how to build and store curves or fixings.
- Pricing registration, which tells the system how a given index UID should be interpreted at pricing time.

If ETL registration is missing, curves or fixings never make it into storage.

If pricing registration is missing, pricing fails even if the data exists, because the runtime does not know how to turn an index UID into conventions, curves, and fixings.

### Handshake B: assets to instrument terms

An asset with prices is not automatically a priceable instrument.

To make an asset priceable, you usually need to attach pricing details to it. That stores the instrument terms needed to rebuild the SDK model later: dates, coupon schedule, spread, conventions, index references, and so on.

Without that link, you may have market data, but you do not have a reproducible bond or swap definition.

## Recommended reading order

1. [Market Data and Registration](./market_data_and_registration.md)
2. [Assets and Pricing Details](./assets_and_pricing_details.md)
3. [Pricing Runtime](./pricing_runtime.md)
4. [Examples](./examples.md)
5. [Key Terms](./key_terms.md)

## What you can do with this stack

Today, the most common workflows are:

- store discount curves and fixing series,
- register pricing indices backed by those stored series,
- price fixed-rate and floating-rate bonds,
- price vanilla interest-rate swaps,
- aggregate instruments into positions and inspect portfolio cashflows.

Next: [Market Data and Registration](./market_data_and_registration.md)
