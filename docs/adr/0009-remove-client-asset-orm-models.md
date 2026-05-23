# ADR 0009: Remove Client Asset ORM Models From Market Runtime

Date: 2026-05-23

Status: Proposed

## Related ADRs

- ADR 0004: Move Client Market Models To `mainsequence.client.markets.models`
- ADR 0007: Client-Wide UID Public Identity
- ADR 0008: Separate Market Asset Semantics From Core DataNodes

## Context

`mainsequence/client/markets/models/assets.py` currently mixes several
responsibilities:

- client ORM resource models for backend assets
- nested asset snapshot and pricing-detail DTOs
- asset-category mutation helpers
- portfolio-index asset helpers
- asset serializer/rebuild helpers used by TDAG configuration rebuilding
- public `mainsequence.client` exports for `Asset`, `AssetMixin`,
  `AssetCategory`, and `PortfolioIndexAsset`

That file is now the wrong architectural boundary. Assets are market-domain
data, not generic client model infrastructure. The SDK already has market-owned
asset tables and asset DataNodes under:

```text
mainsequence/markets/assets/
```

The client model package should not be the long-term home for asset catalog
runtime, asset category runtime, portfolio asset identity, or TDAG
serialization registry behavior.

The current models are not all unused. `Asset`, `AssetMixin`,
`AssetCategory`, `resolve_asset`, and `get_model_class` still have live
consumers. Cleanup must therefore be a staged migration, not a blind deletion.

## Decision

Remove `mainsequence.client.markets.models.assets` as a model-bearing runtime
module.

Asset catalog behavior must move to the market asset package. Portfolio code
must use `portfolio_index_asset_unique_identifier` strings and portfolio
metadata tables, not a client-side `PortfolioIndexAsset` model. Account,
portfolio, instrument, and DataNode code must not depend on `AssetMixin` as a
client ORM base class.

After migration, the client model package may keep account, portfolio, order,
and shared market DTOs that are still backed by client APIs, but it must not
own the canonical asset catalog/domain runtime.

No compatibility shim should be added for removed asset model classes unless a
separate ADR explicitly accepts that public compatibility cost.

## Target Ownership

The target ownership is:

- `mainsequence.markets.assets.simple_tables` owns asset catalog rows, asset
  category rows, and asset-category memberships.
- `mainsequence.markets.assets.data_nodes` owns asset snapshot and pricing
  detail time-series DataNodes.
- `mainsequence.markets.portfolios` owns portfolio identity and stores
  portfolio-index asset identity as strings.
- TDAG owns generic serialization/rebuild infrastructure through an explicit
  registry, not through `assets.py`.
- `mainsequence.client.markets.models` must not export asset ORM models after
  the migration is complete.

## Non-Goals

This ADR does not:

- remove backend asset resources
- remove market asset semantics from the SDK
- remove asset simple tables
- remove asset DataNodes
- require core TDAG to understand market assets
- keep deprecated client asset classes as fake compatibility shims

## Implementation And Review Tasks

### Phase 1: Inventory And Contract Review

- [ ] Review every class and function in
      `mainsequence/client/markets/models/assets.py` and classify it as
      migrate, replace, or delete.
- [ ] Review every public export of `Asset`, `AssetMixin`, `AssetCategory`,
      `AssetSnapshot`, `AssetPricingDetail`, `PortfolioIndexAsset`,
      `resolve_asset`, `create_from_serializer_with_class`, and
      `get_model_class`.
- [ ] Review all `mainsequence.client` top-level exports that come from
      `mainsequence.client.markets.models.assets`.
- [ ] Review ADR 0004 and mark the asset-model ownership decision as
      superseded by this ADR.
- [ ] Review backend endpoints currently used by `Asset` and `AssetCategory`
      methods and map each endpoint to the new market asset API/updater owner.
- [ ] Review whether any lightweight asset DTO is still required; if one is
      required, place it under `mainsequence.markets.assets`, not under
      `mainsequence.client.markets.models`.

### Phase 2: Asset Catalog Runtime

- [ ] Promote `AssetSimpleTable`, `AssetSimpleTableUpdater`,
      `AssetCategorySimpleTable`, `AssetCategorySimpleTableUpdater`, and
      `AssetCategoryMembershipSimpleTableUpdater` as the SDK-owned asset
      catalog runtime.
- [ ] Add or finalize a market asset service/updater method that replaces
      `Asset.filter(...)`.
- [ ] Add or finalize a market asset service/updater method that replaces
      `Asset.query(...)`.
- [ ] Add or finalize a market asset service/updater method that replaces
      `Asset.quick_search(...)`.
- [ ] Add or finalize a market asset service/updater method that replaces
      `Asset.filter_with_asset_class(...)`.
- [ ] Add or finalize a market asset service/updater method that replaces
      `Asset.register_asset_from_figi(...)`.
- [ ] Add or finalize a market asset service/updater method that replaces
      `Asset.get_or_register_custom_asset(...)`.
- [ ] Add or finalize a market asset service/updater method that replaces
      `Asset.batch_get_or_register_custom_assets(...)`.
- [ ] Add or finalize asset-category methods that replace
      `AssetCategory.get_or_create(...)`, `append_assets(...)`,
      `remove_assets(...)`, `update_assets(...)`, and `get_assets(...)`.

### Phase 3: Account And Order Model Migration

- [ ] Replace `AssetMixin` annotations in account holding, target-position,
      trade, virtual-fund holding, and order-manager payload models.
- [ ] Replace `resolve_asset(...)` validators in account and virtual-fund
      payload models with normalization that does not instantiate client asset
      ORM models.
- [ ] Review whether nested asset payloads should be preserved as dictionaries,
      converted to market asset table rows, or reduced to `unique_identifier`
      strings.
- [ ] Remove the direct `Asset` import from
      `mainsequence/client/markets/models/accounts_and_portfolios.py`.
- [ ] Add tests proving account and order payload models still accept current
      backend payload shapes after the asset model removal.

### Phase 4: Market DataNode Migration

- [ ] Remove the `AssetMixin` dependency from
      `mainsequence/markets/markets_data_node.py`.
- [ ] Decide whether `MarketDataNodeConfiguration.asset_list` should accept
      `list[str]`, market asset table rows, or structurally typed objects with
      `unique_identifier`.
- [ ] Update market asset-list validation to validate `unique_identifier`
      without requiring an `AssetMixin` instance.
- [ ] Update tests that currently construct `Asset(...)` only to satisfy
      `MarketDataNode` asset-list validation.
- [ ] Confirm ADR 0008 still holds after removing the client asset model from
      the market DataNode boundary.

### Phase 5: Portfolio Migration

- [ ] Replace `msc.Asset` and `Asset.filter_with_asset_class(...)` usage in
      portfolio configuration helpers.
- [ ] Replace `msc.AssetCategory.get(...)` and `msc.Asset.filter(...)` usage in
      `AssetsConfiguration.get_asset_list()`.
- [ ] Replace asset/category usage in first-party portfolio signals and price
      utilities with the market asset simple-table/updater API.
- [ ] Remove any remaining assumption that portfolio identity requires a
      `PortfolioIndexAsset` Python model.
- [ ] Ensure portfolio identity code uses only
      `portfolio_index_asset_unique_identifier` and the portfolio metadata
      simple table.
- [ ] Add tests proving portfolio construction and canonical writes work without
      importing `mainsequence.client.markets.models.assets`.

### Phase 6: Instruments And Asset Registration Migration

- [ ] Replace `msc.Asset.batch_get_or_register_custom_assets(...)` in instrument
      ETL code with the new market asset registration owner.
- [ ] Replace any direct `Asset.get(...)`, `Asset.filter(...)`, or
      `Asset.query(...)` usage in maintained instrument examples.
- [ ] Review pricing-detail ownership and ensure client
      `AssetPricingDetail` is not required for instrument pricing runtime.
- [ ] Confirm asset registration still writes the backend or simple-table data
      needed by downstream pricing and portfolio workflows.

### Phase 7: TDAG Serializer Registry Cleanup

- [ ] Move `get_model_class(...)` out of `assets.py` into an explicit registry
      module that does not import asset ORM models by default.
- [ ] Update `mainsequence.tdag.data_nodes.build_operations` to use the new
      registry.
- [ ] Remove `create_from_serializer_with_class(...)` after all consumers stop
      using `AssetClass` payload reconstruction.
- [ ] Add tests for rebuilding serialized configs that currently depend on
      `orm_class`.
- [ ] Review whether asset catalog rows should be serialized as simple-table
      rows, plain dictionaries, or `unique_identifier` strings.

### Phase 8: Public Import And Documentation Cleanup

- [ ] Remove asset model exports from
      `mainsequence/client/markets/models/__init__.py`.
- [ ] Remove asset model re-exports from `mainsequence/client/__init__.py`.
- [ ] Remove or rewrite docs that teach `mainsequence.client.Asset`,
      `AssetMixin`, `AssetCategory`, or `PortfolioIndexAsset` as public runtime
      models.
- [ ] Update examples that import asset models from `mainsequence.client`.
- [ ] Remove generated reference docs for
      `mainsequence.client.markets.models.assets` after the module is deleted.
- [ ] Update tutorials to use market asset simple tables/updaters or explicit
      `unique_identifier` strings.

### Phase 9: Deletion

- [ ] Delete `AssetSnapshot` from
      `mainsequence/client/markets/models/assets.py` after nested asset payloads
      no longer require it.
- [ ] Delete `AssetPricingDetail` from
      `mainsequence/client/markets/models/assets.py` after pricing-detail
      runtime no longer requires it.
- [ ] Delete `AssetMixin` after account, portfolio, DataNode, and TDAG
      consumers no longer require it.
- [ ] Delete `AssetCategory` after all category behavior is owned by asset
      simple-table updaters.
- [ ] Delete `PortfolioIndexAsset` after portfolio identity uses only strings
      and metadata tables.
- [ ] Delete `Asset` after all asset lookup, search, registration, and pricing
      helper behavior has moved to the market asset owner.
- [ ] Delete `resolve_asset(...)`, `create_from_serializer_with_class(...)`,
      and local `get_model_class(...)` after registry and payload migration is
      complete.
- [ ] Delete `mainsequence/client/markets/models/assets.py` when it contains no
      remaining runtime code.

### Phase 10: Verification Gates

- [ ] Run `rg` for every removed symbol and confirm there are no maintained
      runtime imports left.
- [ ] Run the client model tests.
- [ ] Run the market DataNode tests.
- [ ] Run the account DataNode tests.
- [ ] Run the portfolio canonical DataNode tests.
- [ ] Run the asset simple-table tests.
- [ ] Run import compatibility tests and update them to assert the new public
      contract instead of preserving removed asset model imports.
- [ ] Review generated docs to confirm no removed client asset model appears as
      public API.

