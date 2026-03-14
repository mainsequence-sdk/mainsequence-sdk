# Assets and Pricing Details

Market data alone is not enough to price an instrument.

You can store prices for an asset and still have no way to rebuild the bond or swap terms behind it. That is why pricing details exist.

## 1. What an Asset means here

In this ecosystem, an `Asset` is the platform object that holds a stable identity.

That identity can represent:

- a tradeable instrument, such as a bond,
- or a market object, such as a reference rate or a curve.

The important field is usually:

- `unique_identifier`

That identifier is the thread that connects storage tables, pricing registries, and platform objects.

## 2. Prices do not make an asset priceable

An asset can have:

- historical prices,
- return series,
- fixings,
- or any other time-series data

and still not be priceable by the instruments SDK.

To make the SDK rebuild an instrument later, you need static terms. That is what pricing details store.

## 3. What pricing details contain

The latest pricing terms for an asset are exposed through:

- `asset.current_pricing_detail`

In practice, two fields matter most:

- `instrument_dump`
- `pricing_details_date`

### `instrument_dump`

This is the serialized SDK instrument model.

It contains the information needed to rebuild the bond or swap later:

- dates,
- face value,
- coupon schedule,
- spread or coupon,
- day-count conventions,
- calendar rules,
- benchmark or floating index identifiers.

### `pricing_details_date`

This is the as-of timestamp for those terms.

It matters because instrument terms can change over time. If you care about reproducibility, you need to know which terms were in force on which date.

## 4. The usual connector workflow

A production connector often does three related things:

1. Ensure the asset exists.
2. Ingest market data for that asset.
3. Attach pricing details when the instrument is priceable.

That third step is the bridge between platform assets and the instruments SDK.

## 5. How pricing details get attached

The key method is:

- `asset.add_instrument_pricing_details_from_ms_instrument(...)`

The usual flow is:

- build an SDK instrument model from vendor data,
- attach it to the asset with a pricing details date,
- let downstream workflows rebuild that instrument later from stored terms.

This keeps pricing deterministic. The platform is not trying to infer a bond from raw price history. It is storing the exact terms that were intended.

## 6. Why this matters for indices and curves

A stored instrument model usually references index UIDs.

For example, a floating-rate bond may contain:

- `floating_rate_index_name`
- `benchmark_rate_index_name`

Those identifiers must already be registered in the pricing registry. That creates the full chain:

- asset UID
- pricing details
- instrument model
- index UID
- `IndexSpec`
- curve UID and fixing UID
- storage tables

If any link is missing, pricing breaks later.

## 7. Rebuilding an instrument later

Once pricing details are attached, downstream code can rebuild the SDK instrument from the stored dump.

That is useful for:

- portfolio pricing,
- risk runs,
- analytics dashboards,
- validation against vendor sheets,
- reproducible backtests.

This is one of the biggest advantages of the platform model: the market data and the instrument definition travel together.

## 8. When to update pricing details

Pricing details should be updated intentionally, not on every run.

Typical reasons:

- the asset has no pricing details yet,
- the stored terms are incomplete,
- a meaningful term changed,
- you are intentionally refreshing terms from a trusted source.

That keeps the system stable and avoids unnecessary platform churn.

## 9. Practical rule

If a dataset is meant to support pricing, do not stop at prices. Attach pricing details whenever the SDK has a real instrument model for that asset.

Next: [Pricing Runtime](./pricing_runtime.md)
