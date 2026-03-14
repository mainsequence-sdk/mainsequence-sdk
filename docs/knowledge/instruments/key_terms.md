# Key Terms

This page translates the core instrument terms into plain language.

## Asset

The platform object that gives something a stable identity.

An asset can represent:

- a tradeable instrument, like a bond,
- or a market object, like a curve or a reference rate.

The field you will use most is `unique_identifier`.

## PricingDetail

The platform record that stores the latest known instrument terms for an asset.

This is what makes an asset priceable by the instruments SDK.

## current_pricing_detail

The latest pricing detail attached to an asset.

This is usually where downstream code reads:

- `instrument_dump`
- `pricing_details_date`

## instrument_dump

The serialized SDK instrument definition stored on the platform.

It is the payload you use later to rebuild the bond or swap model.

## pricing_details_date

The as-of date for the stored instrument terms.

This matters whenever you care about reproducibility across time.

## InstrumentsConfiguration

The platform configuration object that tells the SDK where to read:

- discount curves,
- and reference-rate fixings.

Without it, runtime pricing cannot fetch its market data inputs.

## MSInterface

The runtime adapter that reads stored curves and fixings from Main Sequence and turns them into Python-native structures used during pricing.

## DataNode

A producer node that writes data into storage.

In the instruments context, the important examples are:

- `DiscountCurvesNode`
- `FixingRatesNode`

## APIDataNode

A reader object used to query already-stored tables.

In the instruments runtime, it is used behind the scenes to load curves and fixings.

## Constant name

The symbolic name for a market object, such as `ZERO_CURVE__VALMER_TIIE_28`.

This is commonly used on the ETL registration side.

## UID value

The resolved string identifier returned by `Constant.get_value(...)`.

This is what pricing actually uses at runtime.

## Curve UID

The UID stored as `unique_identifier` for a curve asset in the `discount_curves` table.

## Fixings UID

The UID stored as `unique_identifier` for a fixing-rate asset in the `fixing_rates_1d` table.

## IndexSpec

The pricing contract for an index UID.

It tells the runtime:

- which curve to use,
- which fixings to use,
- and which conventions to apply.

## build_zero_curve

The runtime helper that loads stored curve data and builds a QuantLib curve handle from it.

## get_index

The runtime helper that turns an index UID into a QuantLib index, optionally hydrating historical fixings.

## Position

The portfolio wrapper that groups instruments plus units and lets you price them together.

It is the simplest interface for portfolio-level PV and cashflow aggregation.

Back to: [Instruments Overview](./index.md)
