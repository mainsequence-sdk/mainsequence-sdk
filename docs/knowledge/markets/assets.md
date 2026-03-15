# Assets

Assets are the identity layer behind most of the markets examples in this documentation.

When a `DataNode`, portfolio, dashboard, or pricing workflow talks about an instrument, it usually connects back to a platform `Asset`. The field that matters most is usually `unique_identifier`. That is the stable key that lets you connect time-series data, pricing details, portfolios, and downstream analytics.

If you have been following the tutorial, you have already worked with assets several times even if the code was doing different things in each chapter.

## What the tutorial has done with assets so far

### Part 3: asset-based DataNodes

In [Part 3](../../tutorial/multi_index_columns_working_with_assets.md), the `SimulatedPrices` node writes a table indexed by:

- `time_index`
- `unique_identifier`

That chapter introduces the most important rule for asset-based tables:

- if your output uses `unique_identifier`, those identifiers should normally correspond to real platform assets
- `get_asset_list()` tells the platform which assets belong to that updater
- per-asset update statistics depend on that asset list being correct

This is where assets stop being just metadata and become part of the update model itself.

### Part 4.1: public equities from FIGI

In [Part 4.1](../../tutorial/markets_tutorial/markets_equities_with_algoseek.md), the tutorial uses provider security-master data to hydrate public equities.

The key pattern is:

1. Query existing assets by `unique_identifier`
2. Treat the provider FIGI as the platform identity
3. Register missing assets with `msc.Asset.register_asset_from_figi(...)`

That is the public-master flow. You are not inventing the identity yourself. You are aligning to a known market identifier and letting the platform register it.

### Part 4.3: custom fixed-income assets

In [Part 4.3](../../tutorial/virtualfundbuilder/markets_portfolios_and_virtual_funds.md), the flow changes.

Instead of relying on public FIGIs, the tutorial creates custom assets for mock fixed-income instruments:

- register them with `msc.Asset.batch_get_or_register_custom_assets(...)`
- attach pricing details with `asset.add_instrument_pricing_details_from_ms_instrument(...)`
- assign a custom `security_type` so other workflows, such as translation tables, can route them correctly

This is the custom-master flow. The identity is now owned by your organization, so you are responsible for keeping it stable and meaningful.

### Part 5: dashboards reuse the same assets

In the Streamlit tutorials:

- [Part 5.1](../../tutorial/dashboards/streamlit/streamlit_integration_1.md)
- [Part 5.2](../../tutorial/dashboards/streamlit/streamlit_integration_2.md)

the dashboards do not create a new identity model. They reuse the assets and pricing details created earlier so the app can:

- rebuild instruments,
- drill into per-asset details,
- and price positions consistently.

That continuity is important. Assets are not just a setup step. They are the shared object that lets DataNodes, pricing, portfolios, and dashboards talk about the same thing.

## Public assets vs custom assets

The tutorial covers both patterns.

### Public assets

Use public assets when your source already maps cleanly to a market identifier the platform understands, especially FIGI.

Typical example:

- listed equities coming from a vendor security master

Typical flow:

```python
existing_assets = msc.Asset.query(unique_identifier__in=figis, per_page=500)
asset = msc.Asset.register_asset_from_figi(figi=figi)
```

### Custom assets

Use custom assets when the instrument identity belongs to your organization or does not exist in the public master:

- private instruments
- internal baskets
- OTC structures
- tutorial mock bonds

Typical flow:

```python
assets = msc.Asset.batch_get_or_register_custom_assets(
    [
        {
            "unique_identifier": "TEST_FIXED_BOND_USD_R",
            "security_type": "MOCK_ASSET",
            "snapshot": {"name": "TEST_FIXED_BOND_USD_R", "ticker": "TEST_FIXED_BOND_USD_R"},
        }
    ]
)
```

## Pricing details are the next step, not an optional extra

The fixed-income and dashboard chapters also introduce an important distinction:

- an asset can exist without being priceable
- an asset becomes priceable when it has pricing details attached

That is why the tutorial goes beyond asset registration and adds:

```python
asset.add_instrument_pricing_details_from_ms_instrument(
    instrument=instrument,
    pricing_details_date=time_idx,
)
```

If you want the deeper pricing side of this story, continue with [Assets and Pricing Details](../instruments/assets_and_pricing_details.md).

## `Asset.filter()` vs `Asset.query()`

When you call `msc.Asset.query(...)`, you are using `mainsequence.client.models_vam.AssetMixin.query`.

Both `filter()` and `query()` let you search assets with the same field style. They both also translate common shorthand fields such as:

- `ticker`
- `name`
- `exchange_code`
- `asset_ticker_group_id`

into the corresponding `current_snapshot__...` lookups under the hood.

The difference is mainly in how the request is sent and when each method is a better fit.

### Use `filter()` for normal lookups

`filter()` is the simpler path.

- it sends a GET request
- it paginates across all pages for you
- it returns a list of `Asset` objects
- it is a good default for small to medium filters

Example:

```python
assets = msc.Asset.filter(ticker__in=["AAPL", "NVDA"])
```

This is usually what you want when:

- you are filtering a short list of tickers or IDs
- the filter is readable in a URL
- you do not need to think about request size

### Use `query()` for larger filter payloads

`query()` is the safer path when the filter payload is large.

- it sends a POST request to the collection `query/` endpoint
- it keeps the filter in the request body instead of the URL
- it paginates across all pages for you
- it lets you set `per_page`
- it is the better choice for large `__in` lists

Example:

```python
assets = msc.Asset.query(unique_identifier__in=figis, per_page=500)
```

This is why the Algoseek tutorial uses `query()` when looking up many FIGIs at once. A long `unique_identifier__in=[...]` filter can become awkward or fragile as a GET URL, while `query()` is designed for that case.

### Practical rule of thumb

Use:

- `filter()` when the lookup is simple and reasonably small
- `query()` when you are sending a large asset universe or want explicit `per_page` control

Both methods return paginated results accumulated into Python objects, so the main choice is not "single page vs many pages". The real choice is "simple GET filter vs large POST filter payload".

## A practical registration pattern

For tutorial code and production connectors, the safest pattern is usually:

1. Look up existing assets
2. Register only the missing ones
3. Attach pricing details if the asset needs to be priceable
4. Reuse those same asset identities everywhere else

For example:

```python
asset_uids = ["TEST_FLOATING_BOND_UST_R", "TEST_FIXED_BOND_USD_R"]
existing = msc.Asset.filter(unique_identifier__in=asset_uids)
existing_map = {a.unique_identifier: a for a in existing}

for uid in asset_uids:
    asset = existing_map.get(uid)
    if asset is None:
        asset = msc.Asset.batch_get_or_register_custom_assets(
            [{"unique_identifier": uid, "snapshot": {"name": uid, "ticker": uid}}]
        )[0]
```

That flow is idempotent, easy to reason about, and consistent with the rest of the tutorial.

## What to remember

- `unique_identifier` is the key that ties markets workflows together
- public assets and custom assets solve different identity problems
- asset-based DataNodes should work with real platform assets, not arbitrary strings
- pricing details are what make an asset usable in instrument valuation
- `filter()` is the normal lookup tool
- `query()` is the better option for large filter payloads, especially big `__in` searches
