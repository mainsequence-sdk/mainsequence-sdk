# Market Data and Registration

If you only remember one thing from this section, remember this: the instruments library is intentionally explicit.

It does not silently register conventions at import time. It does not guess how a rate index should be built. It does not assume where curves or fixings live.

That design is a good thing. It keeps pricing behavior controlled and auditable. It also means your connector or bootstrap code must wire the pieces together on purpose.

## 1. Platform prerequisites

Before pricing can load anything, the platform needs to know where curves and fixings are stored.

That configuration lives in `InstrumentsConfiguration`.

The two important fields are:

- `discount_curves_storage_node`
- `reference_rates_fixings_storage_node`

These are table ids, not human-readable table names. The pricing runtime uses them to build `APIDataNode` readers behind the scenes.

If these are missing or misconfigured, pricing cannot fetch market data even if the tables already exist.

## 2. The storage contracts

The runtime expects two specific storage shapes.

### Discount curves

Table identifier:

- `discount_curves`

Index:

- `time_index`
- `unique_identifier`

Columns:

- `curve` as a compressed string payload

That payload expands into a simple curve dictionary:

- key: days to maturity
- value: zero rate

### Reference-rate fixings

Table identifier:

- `fixing_rates_1d`

Index:

- `time_index`
- `unique_identifier`

Columns:

- `rate` as a decimal float

## 3. Constant names vs UID values

This distinction matters a lot.

- A constant name is the symbolic name, such as `ZERO_CURVE__VALMER_TIIE_28`.
- A UID value is the resolved identifier string returned by `Constant.get_value(...)`.

The ETL layer and the pricing layer do not use them the same way.

### ETL registration

ETL registries are keyed by constant name.

That means `DiscountCurvesNode` and `FixingRatesNode` stay environment-agnostic. They resolve the actual UID later, at runtime.

### Pricing registration

Pricing registration is keyed by UID value.

That is the identifier you later pass into:

- `get_index(...)`
- `build_zero_curve(...)`
- `FloatingRateBond(floating_rate_index_name=...)`
- `InterestRateSwap(float_leg_index_name=...)`

## 4. The bootstrap sequence

The clean mental model is:

1. Register ETL builders.
2. Run ETL nodes to populate storage.
3. Register pricing indices.
4. Validate the runtime by loading a curve or an index once.

If you skip step 3, pricing usually fails with a missing `IndexSpec` error.

## 5. ETL builders

Two registries drive ETL:

- `DISCOUNT_CURVE_BUILDERS`
- `FIXING_RATE_BUILDERS`

These registries map constant names to builder functions.

### Discount-curve builder contract

A discount-curve builder receives:

- `update_statistics`
- `curve_unique_identifier`
- `base_node_curve_points` when applicable

It should return a DataFrame indexed by:

- `time_index`
- `unique_identifier`

And it should provide one column:

- `curve`

The `curve` column should contain a dictionary before compression. The node handles compression for storage.

### Fixing-rate builder contract

A fixing-rate builder receives:

- `update_statistics`
- `unique_identifier`

It returns a DataFrame indexed by:

- `time_index`
- `unique_identifier`

With one value column:

- `rate`

## 6. The built-in ETL nodes

Two SDK nodes matter most here:

- `DiscountCurvesNode`
- `FixingRatesNode`

They are standard `DataNode`s. Their job is not to price instruments. Their job is to keep the pricing inputs fresh in storage.

If you need a refresher on the producer side, see [Data Nodes](../data_nodes.md).

## 7. Pricing registration with `IndexSpec`

Once curves and fixings exist in storage, the runtime still needs a pricing interpretation for each index UID.

That interpretation lives in `IndexSpec`.

An `IndexSpec` tells the runtime:

- which curve UID to load,
- which fixing UID to load,
- which calendar, day counter, tenor, currency, and settlement rules to use.

That is why pricing registration is connector-owned. The base library stays clean, and your connector owns the business conventions.

## 8. Common failure modes

- Curves were stored, but no `IndexSpec` was registered for the index UID.
- An `IndexSpec` exists, but `InstrumentsConfiguration` points to the wrong storage node ids.
- A builder writes percent values where the runtime expects decimals.
- Teams mix constant names and resolved UID values in the wrong registry.

## 9. Operational advice

- Keep registration explicit in a single `register_all()` style bootstrap.
- Make that bootstrap idempotent.
- Validate registration early by calling `build_zero_curve(...)` once during startup.

Next: [Assets and Pricing Details](./assets_and_pricing_details.md)
