# ADR 0008: Separate Market Asset Semantics From Core DataNodes

Date: 2026-05-23

Status: Accepted

## Context

Core TDAG DataNodes contained market-specific concepts. This created a bad
dependency direction: generic time-series infrastructure knew about platform
assets.

The main leaks were:

- `mainsequence/tdag/data_nodes/data_nodes.py` imported market asset types through
  `mainsequence.client`.
- `DataAccessMixin.get_last_observation()` accepted `asset_list`.
- Core DataNode helpers exposed `get_ranged_data_per_asset()`,
  `get_ranged_data_per_asset_great_or_equal()`, and
  `filter_by_assets_ranges()`.
- Core `DataNode._set_update_statistics()` called `get_asset_list()` and
  `UpdateStatistics.update_assets(...)`.
- Core `DataNode.get_asset_list()` existed even though most DataNodes are not
  market asset nodes.
- `DataNodeConfiguration.asset_list` was typed as a market `AssetMixin` list.
- Persist managers translated `asset_list` into the hard-coded
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
7. [x] Migrate `HoldingsDataNode` to inherit from `MarketDataNode`.
8. [x] Migrate `PortfolioCanonicalDataNode` to inherit from `MarketDataNode`
   only where the table is asset-scoped.
9. [x] Migrate `InterpolatedPrices` and `ExternalPrices` to inherit from
   `MarketDataNode`.
10. [x] Migrate portfolio signal nodes that define `get_asset_list()` to inherit
    from `MarketDataNode`.
11. [x] Migrate instrument/rate nodes that expose assets through
    `get_asset_list()` to inherit from `MarketDataNode`.
12. [x] Keep non-asset market tables on plain `DataNode` if their identity
    dimensions are not platform assets.
13. [x] Move `get_ranged_data_per_asset(...)` behavior into `MarketDataNode`.
14. [x] Move `get_ranged_data_per_asset_great_or_equal(...)` behavior into
    `MarketDataNode`.
15. [x] Move asset range map conversion into `MarketDataNode`.
16. [x] Move asset-scoped latest-observation behavior into `MarketDataNode`.
17. [x] Keep temporary deprecation shims in core only if needed to avoid breaking
    existing code during migration.
18. [x] Mark every temporary shim with an explicit cleanup comment.
19. [x] Remove `asset_list` from core `DataNodeConfiguration`.
20. [x] Add `asset_list` to `MarketDataNodeConfiguration`.
21. [x] Update market-specific configs to inherit from
    `MarketDataNodeConfiguration` when they need asset scoping.
22. [x] Keep generic DataNode configs free of `Asset`, `AssetMixin`, and market
    model imports.
23. [x] Stop calling `get_asset_list()` from core
    `DataNode._set_update_statistics()`.
24. [x] Make core `_set_update_statistics()` operate only on generic update
    progress.
25. [x] Keep asset-scoped update narrowing in
    `MarketDataNode._set_update_statistics()`.
26. [x] Add `UpdateStatistics.update_identity_scope(...)` as the generic
    identity-scope API and make market callers use it.
27. [x] Keep `UpdateStatistics.asset_list` only as a transition field until
    market callers no longer require it.
28. [x] Remove `asset_list` parameters from core persist manager methods.
29. [x] Use `dimension_filters` for generic scoped reads.
30. [x] Keep asset-list-to-dimension-filter conversion only in `MarketDataNode`.
31. [x] Preserve APIDataNode behavior by passing canonical dimensions to the
    backend.
32. [x] Remove docs language that teaches asset-scoped behavior as generic
    DataNode behavior.
33. [x] Document market asset scoping under markets docs, not core TDAG docs.
34. [x] Keep portfolio price examples explicit about their price source.
35. [x] Do not reference `WrapperDataNode` or `AssetTranslationTable`.
36. [x] Add tests proving generic DataNodes can run without importing market
    asset models.
37. [x] Add tests proving market DataNodes reject invalid `asset_list` values.
38. [x] Add tests proving market DataNodes reject duplicate asset unique
    identifiers.
39. [x] Add tests proving asset lists become canonical `dimension_filters`.
40. [x] Add tests proving migrated portfolio price nodes still fetch by
    `unique_identifier`.
41. [x] Add tests proving core DataNode no longer exposes asset-specific public
    helpers after the compatibility window closes.
42. [x] Regenerate or validate reference docs after public signatures change.

## Migration Order

1. Add `MarketDataNode` and tests without changing existing market nodes.
2. Migrate the smallest market node first, preferably `AssetTimestampedDataNode`.
3. Migrate portfolio price nodes.
4. Migrate portfolio signal and holdings nodes.
5. Migrate instrument/rate nodes that are asset-scoped.
6. Remove asset helpers from core TDAG after all call sites are migrated.
7. Remove temporary compatibility shims.
8. Regenerate reference docs.

## Follow-Up: Remove Market Semantics From UpdateStatistics

The first implementation phase moves asset-scoped DataNode behavior out of core
TDAG, but `UpdateStatistics` still keeps transition APIs for market callers.
The final cleanup must make `UpdateStatistics` completely independent from
market assets and from the `unique_identifier` convention.

Implementation tasks:

1. [x] Define the final `UpdateStatistics` contract as only
   `index_progress`, `index_min`, `global_index_progress`,
   `multi_index_column_stats`, `max_time_index_value`, `limit_update_time`,
   and `is_backfill`.
2. [x] Remove `asset_list` from `UpdateStatistics`.
3. [x] Remove `asset_time_statistics` from `UpdateStatistics`, including the
   legacy projection to and from `index_progress`.
4. [x] Replace `UpdateStatistics.update_assets(...)` with market-layer asset
   scoping, not a blind deletion. The functionality to preserve is:
   validate the current asset universe, convert each asset to its platform
   asset identity, narrow `index_progress` to that identity set, assign the
   fallback date for missing assets, compute `_max_time_in_update_statistics`
   from the scoped progress, and keep columnar update stats aligned with the
   scoped identity set.
5. [x] Remove `_get_update_statistics(... unique_identifier_list ...)` from
   `UpdateStatistics`.
6. [x] Keep `update_identity_scope(identity_values=...)`, but remove asset and
   `unique_identifier` language from its internals.
7. [x] Remove the default `identity_dimensions=["unique_identifier"]` from
   `iter_index_progress_coordinates(...)`.
8. [x] Make `identity_dimensions` explicit anywhere `UpdateStatistics` builds a
   `dimension_range_map`.
9. [x] Rename or delete asset-named helpers in `UpdateStatistics`, including
   `asset_identifier`, `is_any_asset_on_fallback_date`,
   `are_all_assets_on_fallback_date`, `get_asset_earliest_multiindex_update`,
   and `filter_assets_by_level`.
10. [x] Delete helpers that only make sense for market assets instead of
    renaming them generically.
11. [x] Move remaining market-specific behavior into
    `mainsequence/markets/markets_data_node.py`, including asset-to-identity
    conversion, updater asset scope, and any per-asset fallback/range helpers
    still needed by market nodes.
12. [x] Add a `MarketDataNode` asset-scope helper that replaces
    `update_assets(...)` for market nodes. It should validate the current asset
    scope, translate assets to platform identities, scope update statistics and
    columnar stats to those identities, and return a scoped `UpdateStatistics`
    without writing market assets into the `UpdateStatistics` object.
13. [x] Store the active market asset scope on `MarketDataNode`, for example as
    `self.asset_update_scope` or an equivalent private field, so `update()`
    methods can access the scoped assets without reading
    `update_statistics.asset_list`.
14. [x] Keep generic identity helpers on `UpdateStatistics` and add
    market-layer helpers only where asset objects are required. Generic fallback
    checks and range-map creation now use identity terminology; asset-object
    earliest update lookup lives on `MarketDataNode`.
15. [x] Update market callers that read `update_statistics.asset_list` so they
    use `self.get_asset_list()` or a `MarketDataNode` helper instead.
16. [x] Update columnar update-range logic that depends on `self.asset_list`
    inside `UpdateStatistics`; make it generic with explicit identity values or
    move it to markets.
17. [x] Add tests proving `MarketDataNode` preserves the old
    `update_assets(...)` behavior for market nodes: existing assets keep their
    last progress, missing assets receive the fallback date, unrelated progress
    keys are dropped, and `_max_time_in_update_statistics` is computed from the
    scoped market asset progress.
18. [x] Add tests proving `UpdateStatistics` has no public `asset`,
    `asset_list`, `asset_time_statistics`, or `unique_identifier` API.
19. [x] Add tests proving market nodes still scope updates by asset through
    `MarketDataNode`, not through `UpdateStatistics`.
20. [x] Update ADR 0002 after the transition fields are fully removed.
21. [x] Run targeted market, portfolio, and TDAG tests.
22. [x] Verify the `UpdateStatistics` class body with
    `rg "asset|unique_identifier"` and require no matches.

Definition of done: inside `UpdateStatistics`, `asset`,
`asset_time_statistics`, `asset_list`, and `unique_identifier` do not appear in
fields, method names, method parameters, defaults, docstrings, or comments.

## Follow-Up: Remove Remaining TDAG Vocabulary Leaks

`mainsequence.tdag` no longer imports `mainsequence.markets`, but some generic
TDAG files still expose legacy asset-table vocabulary. These are compatibility
paths, not package dependencies. They should be removed in a separate cleanup so
the TDAG public surface is vocabulary-clean.

Removal plan:

1. [x] Remove `DataAccessMixin.get_df_between_dates(...)` parameters
   `unique_identifier_list` and `unique_identifier_range_map` from
   `mainsequence/tdag/data_nodes/data_nodes.py`.
2. [x] Remove
   `_legacy_unique_identifier_range_map_to_dimension_range_map(...)` from
   `mainsequence/tdag/data_nodes/data_nodes.py`.
3. [x] Update every TDAG and market caller still using
   `unique_identifier_list` to pass
   `dimension_filters={<dimension_name>: [...]}` explicitly.
4. [x] Update every TDAG and market caller still using
   `unique_identifier_range_map` to pass `dimension_range_map` with explicit
   coordinate dictionaries.
5. [x] Rewrite `DataAccessMixin.get_df_between_dates(...)` docstring so it only
   documents `dimension_filters`, `index_coordinates`, and
   `dimension_range_map`.
6. [x] Rewrite `DataNode._execute_local_update(...)` docstring in
   `mainsequence/tdag/data_nodes/data_nodes.py` so it describes generic
   multidimensional indexes and does not prescribe `unique_identifier` for
   asset tables.
7. [x] Review `mainsequence/tdag/data_nodes/filters.py` and remove or rename
   `JoinKey.unique_identifier` if it is only a market-table convenience.
   Keep `node_unique_identifier` only if it means TDAG node identity, not market
   asset identity.
8. [x] Review `mainsequence/tdag/data_nodes/build_operations.py` serialization
   of arbitrary objects with `.unique_identifier`. Replace it with a generic
   protocol/name only if it is not intentionally modeling SDK object identity.
9. [x] Remove or rename `filter_by_assets_ranges(...)` compatibility wrappers
   in `mainsequence/client/models_tdag.py` after all callers use canonical
   dimension range APIs.
10. [x] Remove `unique_identifier_list` and `unique_identifier_range_map`
    compatibility parameters from `mainsequence/client/models_tdag.py` methods
    after callers are migrated.
11. [x] Remove `max_per_asset_symbol` and `min_per_asset_symbol` legacy parsing
    from `SourceTableConfiguration.get_data_updates()` once backend responses
    only use `index_progress` and `index_min`.
12. [x] Add tests asserting `mainsequence/tdag` has no imports from
    `mainsequence.markets` or `mainsequence.client.markets`.
13. [x] Add tests or static checks for the vocabulary boundary: generic TDAG
    public APIs should not expose market asset terminology such as `asset_list`,
    `get_asset_list`, `unique_identifier_list`, or
    `unique_identifier_range_map`.
14. [x] Run targeted TDAG, APIDataNode, market, portfolio, and docs-reference
    checks after the compatibility aliases are removed.

Definition of done: `rg "asset_list|get_asset_list|unique_identifier_list|unique_identifier_range_map|filter_by_assets_ranges" mainsequence/tdag mainsequence/client/models_tdag.py`
returns no public TDAG compatibility APIs. Remaining `unique_identifier`
mentions must either be market-layer code or generic object identity code with
an explicit justification.

## Risks

- Some market tables use `unique_identifier` together with other identity
  dimensions, such as account or portfolio identifiers. These should still be
  market nodes only when `unique_identifier` means platform asset identity.
- Some market tables may use `unique_identifier` as a generic string. Those
  should not inherit from `MarketDataNode` unless they really represent assets.
- `UpdateStatistics.update_assets(...)` has been removed. Market callers scope
  updates through `MarketDataNode.scope_update_statistics_to_assets(...)`.
- Future market-node migrations should stay staged when a table uses
  `unique_identifier` for something other than platform asset identity.

## Final State

- Core TDAG has no imports of market asset models.
- Core DataNode configuration has no `asset_list`.
- Core DataNode has no `get_asset_list()`.
- Core DataNode has no asset-specific ranged-data helpers.
- Market asset-scoped nodes inherit from `MarketDataNode`.
- Asset validation happens in the markets layer.
- `unique_identifier` is treated as a market asset dimension only inside the
  markets layer.
