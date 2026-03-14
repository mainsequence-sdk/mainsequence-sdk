# Examples

The source knowledge base includes a good set of example scripts. Read them as a progression, not as isolated snippets.

The sequence below matches how a real implementation usually comes together.

## 1. Bootstrap first

### `00_bootstrap_registration.py`

This is the most important example operationally.

It shows the expected startup pattern:

- your connector exposes a `register_all()` function,
- that function wires ETL builders and pricing index specs,
- pricing only starts after that bootstrap has run.

If you only adopt one pattern from the examples, adopt this one.

## 2. Register ETL builders

### `01_register_etl_curve_builder_valmer.py`

This example shows how to register a discount-curve builder.

Why it matters:

- the ETL registry is keyed by constant name,
- the builder receives the resolved UID value,
- the node later compresses and stores the curve payload.

It is a good reference for anyone building a connector around a rates vendor.

## 3. Publish pricing inputs with DataNodes

### `02_etl_nodes_discount_and_fixings.py`

This example shows the actual nodes that publish:

- discount curves,
- and daily fixing series.

It is the bridge between the data production world and the pricing runtime.

## 4. Register pricing indices

### `03_register_pricing_indices_banxico_style.py`

This is the pricing-side mirror of the ETL registration step.

It shows how to:

- resolve the UID values you will use in pricing,
- register an `IndexSpec`,
- tie an index UID to its curve UID, fixings UID, and conventions.

This is where most "data exists but pricing still fails" issues are resolved.

## 5. Price simple instruments

### `04_price_fixed_and_floating_bond.py`

Use this example to understand the difference between:

- a fixed bond priced with a yield,
- and a floating bond priced from registered curves and fixings.

It is a good smoke test for runtime setup.

### `05_price_tiie_swap.py`

This example does the same for an interest-rate swap.

If bonds work but swaps fail, comparing the two examples often makes the missing link obvious.

## 6. Move to a portfolio view

### `06_portfolio_position.py`

This example introduces `Position`, which is the easiest way to think about portfolios in this stack.

Use it when you want:

- total PV,
- line-by-line contribution,
- combined future cashflows.

## 7. Connect platform assets to pricing

### `07_attach_pricing_details_to_assets.py`

This example is important because it shows how a connector turns an asset into a priceable instrument.

It demonstrates:

- ensuring the asset exists,
- building the SDK instrument model,
- storing pricing details on the asset.

### `08_rebuild_instrument_from_asset_pricing_details.py`

This is the downstream side of the same pattern.

It shows how to:

- fetch an asset,
- rebuild the SDK instrument from `instrument_dump`,
- run pricing or analytics on the reconstructed object.

## Recommended reading order

If you are onboarding someone new, use this order:

1. `00_bootstrap_registration.py`
2. `01_register_etl_curve_builder_valmer.py`
3. `02_etl_nodes_discount_and_fixings.py`
4. `03_register_pricing_indices_banxico_style.py`
5. `04_price_fixed_and_floating_bond.py`
6. `05_price_tiie_swap.py`
7. `07_attach_pricing_details_to_assets.py`
8. `08_rebuild_instrument_from_asset_pricing_details.py`
9. `06_portfolio_position.py`

That order matches how teams usually build the stack in practice.

Next: [Key Terms](./key_terms.md)
