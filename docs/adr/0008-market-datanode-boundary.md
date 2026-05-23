# ADR 0008: Separate Market Asset Semantics From Core DataNodes

Date: 2026-05-23

Status: Proposed

## Context

Core TDAG DataNodes currently contain market-specific concepts. This creates a
bad dependency direction: generic time-series infrastructure knows about
platform assets.

The main leaks are:

- `mainsequence/tdag/data_nodes/data_nodes.py` imports market asset types through
  `mainsequence.client`.
- `DataAccessMixin.get_last_observation()` accepts `asset_list`.
- Core DataNode helpers expose `get_ranged_data_per_asset()`,
  `get_ranged_data_per_asset_great_or_equal()`, and
  `filter_by_assets_ranges()`.
- Core `DataNode._set_update_statistics()` calls `get_asset_list()` and
  `UpdateStatistics.update_assets(...)`.
- Core `DataNode.get_asset_list()` exists even though most DataNodes are not
  market asset nodes.
- `DataNodeConfiguration.asset_list` is typed as a market `AssetMixin` list.
- Persist managers translate `asset_list` into the hard-coded
  `unique_identifier` dimension.

This conflicts with ADR 0002. The generic TDAG contract is multidimensional:
after `time_index_name`, every index is just an identity dimension. In that
model, `unique_identifier` is not a TDAG primitive. It is the market asset
identity dimension.

## Decision

Move market asset semantics out of core TDAG and into a markets-specific
DataNode extension.

The target file is:

```text
mainsequence/markets/markets_data_node.py
```

Core TDAG must own only generic table/update concepts:

- `time_index_name`
- `index_names`
- identity dimensions
- `dimension_filters`
- `index_coordinates`
- `dimension_range_map`
- generic update progress
- storage and update hashes
- persistence orchestration

The markets layer must own market asset concepts:

- `Asset`
- `AssetMixin`
- asset categories and asset universes
- `asset_list`
- `get_asset_list()`
- `unique_identifier` as the asset identity dimension
- asset-scoped latest-observation helpers
- asset range maps
- validation that asset-scoped nodes are actually backed by market assets

## Target Design

Add a market-specific base class in `mainsequence/markets/markets_data_node.py`.

The expected shape is:

```python
class MarketDataNodeConfiguration(DataNodeConfiguration):
    asset_list: list[AssetMixin] | None


class MarketDataNode(DataNode):
    asset_identity_dimension = "unique_identifier"
```

`MarketDataNode` is not a generic TDAG abstraction. It is the explicit boundary
for DataNodes whose identity dimension represents platform assets.

The market extension should provide:

- validation that `asset_list` contains `AssetMixin` instances
- validation that every asset has a non-empty `unique_identifier`
- validation that duplicate `unique_identifier` values are rejected
- validation that configured asset-scoped tables include `unique_identifier` in
  `index_names` when the config exposes `index_names`
- conversion from `asset_list` to
  `dimension_filters={"unique_identifier": [...]}`
- conversion from legacy `UniqueIdentifierRangeMap` to generic
  `dimension_range_map`
- compatibility helpers for current market callers, including
  `get_ranged_data_per_asset(...)`
- a market override of `_set_update_statistics(...)` that keeps asset scoping
  outside core TDAG

The implementation must not make core TDAG import market asset models.

## Non-Goals

This ADR does not:

- implement the refactor
- change backend APIs
- change persisted table contracts
- change market cap logic
- reintroduce `AssetTranslationTable`
- reintroduce `WrapperDataNode`
- make every `unique_identifier` in the SDK an asset
- make core TDAG depend on `mainsequence.markets`

## Implementation Tasks

The tasks are intentionally ordered. Completed tasks are checked when the
corresponding code and tests exist in the SDK.

1. [x] Create `MarketDataNodeConfiguration` in
   `mainsequence/markets/markets_data_node.py`.
2. [x] Create `MarketDataNode` in
   `mainsequence/markets/markets_data_node.py`.
3. [x] Import market asset models directly from the markets client model module,
   not through broad `mainsequence.client` re-exports.
4. [x] Keep `mainsequence/markets/markets_data_node.py` responsible for asset
   verification and asset-to-dimension translation.
5. [x] Add focused tests for asset validation and dimension-filter conversion.
6. [x] Keep `AssetTimestampedFrameMixin` as a plain frame/config mixin and
   migrate `AssetTimestampedDataNode` to compose `AssetTimestampedFrameMixin`
   with `MarketDataNode`.
7. [ ] Migrate `HoldingsDataNode` to inherit from `MarketDataNode`.
8. [ ] Migrate `PortfolioCanonicalDataNode` to inherit from `MarketDataNode`
   only where the table is asset-scoped.
9. [ ] Migrate `InterpolatedPrices` and `ExternalPrices` to inherit from
   `MarketDataNode`.
10. [ ] Migrate portfolio signal nodes that define `get_asset_list()` to inherit
    from `MarketDataNode`.
11. [ ] Migrate instrument/rate nodes that expose assets through
    `get_asset_list()` to inherit from `MarketDataNode`.
12. [ ] Keep non-asset market tables on plain `DataNode` if their identity
    dimensions are not platform assets.
13. [ ] Move `get_ranged_data_per_asset(...)` behavior into `MarketDataNode`.
14. [ ] Move `get_ranged_data_per_asset_great_or_equal(...)` behavior into
    `MarketDataNode`.
15. [ ] Move asset range map conversion into `MarketDataNode`.
16. [ ] Move asset-scoped latest-observation behavior into `MarketDataNode`.
17. [ ] Keep temporary deprecation shims in core only if needed to avoid breaking
    existing code during migration.
18. [ ] Mark every temporary shim with an explicit cleanup comment.
19. [ ] Remove `asset_list` from core `DataNodeConfiguration`.
20. [ ] Add `asset_list` to `MarketDataNodeConfiguration`.
21. [ ] Update market-specific configs to inherit from
    `MarketDataNodeConfiguration` when they need asset scoping.
22. [ ] Keep generic DataNode configs free of `Asset`, `AssetMixin`, and market
    model imports.
23. [ ] Stop calling `get_asset_list()` from core
    `DataNode._set_update_statistics()`.
24. [ ] Make core `_set_update_statistics()` operate only on generic update
    progress.
25. [ ] Keep asset-scoped update narrowing in
    `MarketDataNode._set_update_statistics()`.
26. [ ] Replace `UpdateStatistics.update_assets(...)` with a generic identity or
    dimension scope API when the compatibility window closes.
27. [ ] Keep `UpdateStatistics.asset_list` only as a transition field until
    market callers no longer require it.
28. [ ] Remove `asset_list` parameters from core persist manager methods.
29. [ ] Use `dimension_filters` for generic scoped reads.
30. [ ] Keep asset-list-to-dimension-filter conversion only in `MarketDataNode`.
31. [ ] Preserve APIDataNode behavior by passing canonical dimensions to the
    backend.
32. [ ] Remove docs language that teaches asset-scoped behavior as generic
    DataNode behavior.
33. [ ] Document market asset scoping under markets docs, not core TDAG docs.
34. [ ] Keep portfolio price examples explicit about their price source.
35. [ ] Do not reference `WrapperDataNode` or `AssetTranslationTable`.
36. [ ] Add tests proving generic DataNodes can run without importing market
    asset models.
37. [ ] Add tests proving market DataNodes reject invalid `asset_list` values.
38. [ ] Add tests proving market DataNodes reject duplicate asset unique
    identifiers.
39. [ ] Add tests proving asset lists become canonical `dimension_filters`.
40. [ ] Add tests proving migrated portfolio price nodes still fetch by
    `unique_identifier`.
41. [ ] Add tests proving core DataNode no longer exposes asset-specific public
    helpers after the compatibility window closes.
42. [ ] Regenerate reference docs after public signatures change.

## Migration Order

1. Add `MarketDataNode` and tests without changing existing market nodes.
2. Migrate the smallest market node first, preferably `AssetTimestampedDataNode`.
3. Migrate portfolio price nodes.
4. Migrate portfolio signal and holdings nodes.
5. Migrate instrument/rate nodes that are asset-scoped.
6. Remove asset helpers from core TDAG after all call sites are migrated.
7. Remove temporary compatibility shims.
8. Regenerate reference docs.

## Risks

- Some market tables use `unique_identifier` together with other identity
  dimensions, such as account or portfolio identifiers. These should still be
  market nodes only when `unique_identifier` means platform asset identity.
- Some market tables may use `unique_identifier` as a generic string. Those
  should not inherit from `MarketDataNode` unless they really represent assets.
- `UpdateStatistics.update_assets(...)` is still asset-specific. It should be
  treated as a transition API until generic dimension update scoping exists.
- Migrating all current subclasses at once would make the refactor hard to
  review. The migration should be staged.

## Final State

- Core TDAG has no imports of market asset models.
- Core DataNode configuration has no `asset_list`.
- Core DataNode has no `get_asset_list()`.
- Core DataNode has no asset-specific ranged-data helpers.
- Market asset-scoped nodes inherit from `MarketDataNode`.
- Asset validation happens in the markets layer.
- `unique_identifier` is treated as a market asset dimension only inside the
  markets layer.
