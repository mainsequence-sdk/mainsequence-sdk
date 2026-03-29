# Asset Categories

`AssetCategory` is the reusable grouping layer for assets.

If an `Asset` is the identity of one instrument, an `AssetCategory` is the named universe that collects many assets into one object you can reference elsewhere.

This is useful when you want to say:

- "these are the assets my portfolio trades"
- "these are the assets in this benchmark universe"
- "these are the assets I want to interpolate prices for"

without repeating the full asset list every time.

## What an asset category is

An asset category is a named collection of assets with:

- `unique_identifier`
- `display_name`
- `assets`
- optional `description`

In practice, it is the platform object you use when the asset universe itself should be reusable.

## Why asset categories matter

There are two common ways to define a universe:

1. pass a concrete list of assets directly into code
2. point to a reusable category by identifier

Direct asset lists are fine for:

- small one-off runs
- very local scripts
- experiments where reuse does not matter yet

Asset categories are better when:

- the same universe is reused across several workflows
- a portfolio or signal should reference a stable named universe
- you want the universe to be easy to update without rewriting every consumer
- the asset list is conceptually part of the business definition

## The mental model

Think of asset categories as reusable containers for universes.

Examples:

- `sp500_constituents`
- `us_ig_credit_universe`
- `crypto_core_assets`
- `mock_category_assets_tutorial`

The point is not only grouping. The point is having a stable platform object that other workflows can reference by name.

## How the tutorial uses them

The portfolio tutorial introduces an asset category here:

- [Part 4.3 — Markets — Portfolios and Virtual Funds](../../tutorial/virtualfundbuilder/markets_portfolios_and_virtual_funds.md)

In that chapter, the category is used to collect the mock assets so later price and portfolio configuration can refer to the universe by `unique_identifier` instead of by repeating the list manually.

That is the right intuition:

- assets define what each instrument is
- the asset category defines which instruments belong to the working universe

## Core client operations

The normal client entrypoint is:

```python
import mainsequence.client as msc
```

### Create or fetch a category

```python
asset_category = msc.AssetCategory.get_or_create(
    display_name="Mock Category Assets Tutorial",
    unique_identifier="mock_category_assets_tutorial",
    description="Tutorial asset universe for mock fixed-income examples.",
)
```

### Append assets

```python
asset_category.append_assets(assets=assets)
```

You can also append by id if that is what you already have:

```python
asset_category.append_assets(asset_ids=[101, 102, 103])
```

### Read the assets back

```python
category_assets = asset_category.get_assets()
```

### Remove assets

```python
asset_category.remove_assets(asset_ids=[101, 102])
```

### Replace the universe

```python
asset_category.update_assets(asset_ids=[201, 202, 203])
```

Use that carefully. Replacing the membership changes the effective universe for any workflow that depends on that category.

## How this connects to VFB

In Virtual Fund Builder, the important field is:

- `assets_category_unique_id`

When that field is present, VFB resolves the traded universe from the asset category.

That is the category-driven pattern.

When it is missing, the signal or other node-level logic is expected to define the universe directly through code.

So the choice is:

- category-driven universe: stable, shared, reusable
- signal-driven universe: local, dynamic, code-defined

Neither is universally better. They solve different problems.

## Asset categories vs plain asset lists

This distinction matters.

### Plain asset list

Use this when:

- the list is small
- the list is local to one node or one signal
- reuse is not important yet

### Asset category

Use this when:

- the universe should be named
- multiple workflows should refer to the same set
- you want universe changes to happen in one place
- the asset universe is part of the business definition

## Asset categories vs translation tables

Readers often confuse these because both sit near the universe and price plumbing.

They do different jobs.

### Asset category

Answers:

> which assets are in scope?

### Translation table

Answers:

> for a given asset, which upstream time series should be used?

So:

- category = universe definition
- translation table = routing definition

You often need both in the same workflow, especially in portfolio construction.

## A practical example

This is the tutorial pattern in its simplest form:

```python
assets = ensure_test_assets()

asset_category = msc.AssetCategory.get_or_create(
    display_name="Mock Category Assets Tutorial",
    unique_identifier="mock_category_assets_tutorial",
)
asset_category.append_assets(assets=assets)
```

What this gives you:

- the assets still keep their own identities
- the workflow now also has a named universe object
- later code can reference `mock_category_assets_tutorial` instead of rebuilding the list

## Good practices

### Keep `unique_identifier` stable

Treat the category identifier as a real API-level name, not as a temporary label.

### Use meaningful boundaries

A category should represent a coherent universe:

- one benchmark
- one strategy universe
- one research basket
- one tutorial test universe

### Avoid mixing unrelated universes

If two workflows have different business meanings, they should usually have different categories.

### Be careful with shared categories

If many workflows depend on the same category, changing membership can have broad downstream effects.

## Common mistakes

### 1. Treating a category like a translation table

Categories do not route prices.

They only define membership.

### 2. Treating a category like asset metadata

The category is not where asset identity lives.

Identity still belongs to each `Asset`.

### 3. Using one category for unrelated purposes

This makes the category easy to reuse in the short term and confusing in the long term.

### 4. Rebuilding the same universe everywhere in code

If the same universe keeps appearing in multiple nodes or portfolios, it probably wants to become an asset category.

## Rules of thumb

- use assets for identity
- use asset categories for reusable universes
- use translation tables for routing
- use direct asset lists for local or dynamic cases

## Related reading

- [Assets](./assets.md)
- [Translation Tables](./translation_tables.md)
- [Portfolio Pipeline and Configuration](../virtualfundbuilder/portfolio_pipeline.md)
- [Part 4.3 — Markets — Portfolios and Virtual Funds](../../tutorial/virtualfundbuilder/markets_portfolios_and_virtual_funds.md)
