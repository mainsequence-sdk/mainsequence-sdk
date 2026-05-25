# ADR 0010: Move Markets Client Under `mainsequence.markets.client`

Date: 2026-05-25

Status: Proposed

## Related ADRs

- ADR 0003: Move Market Folders Under `mainsequence.markets`
- ADR 0004: Move Client Market Models To `mainsequence.client.markets.models`
- ADR 0008: Separate Market Asset Semantics From Core DataNodes
- ADR 0009: Remove Client Asset ORM Models From Market Runtime

## Context

The SDK currently has two market-related package surfaces:

```text
mainsequence/client/markets/
mainsequence/markets/
```

`mainsequence.client.markets` was introduced as a transitional home for market
client models after moving them out of `mainsequence/client/models_vam.py`.
That move improved the module name but did not create the final application
boundary. Market resource models, market asset semantics, market account DTOs,
portfolio runtime, instruments, and market DataNodes still span both
`mainsequence.client` and `mainsequence.markets`.

This creates the wrong dependency direction:

- importing `mainsequence.client` exports market models through a star import
- `mainsequence.markets` imports `Asset`, `AssetCategory`, or `AssetMixin`
  through `mainsequence.client`
- `mainsequence.client.markets.models.assets` imports market runtime code such
  as `mainsequence.markets.instruments`
- market application code cannot be isolated, packaged, or reasoned about as a
  self-contained dependency of the core SDK

The target architecture is that the markets application is completely under
`mainsequence.markets`. Generic SDK client code may provide shared transport,
base ORM, auth, and request utilities, but it must not own the market
application namespace.

## Decision

Move the market client package from:

```text
mainsequence/client/markets/
```

to:

```text
mainsequence/markets/client/
```

The canonical import path for market client models becomes:

```python
from mainsequence.markets.client.models import Asset
from mainsequence.markets.client.models import Portfolio
from mainsequence.markets.client.models import Account
```

The final package shape should be:

```text
mainsequence/
  client/
    base.py
    client.py
    exceptions.py
    utils.py
    ...
  markets/
    client/
      __init__.py
      models/
        __init__.py
        core.py
        assets.py
        accounts_and_portfolios.py
    accounts/
    assets/
    execution/
    instruments/
    portfolios/
    markets_data_node.py
```

The dependency direction is:

```text
mainsequence.markets -> mainsequence.client shared infrastructure
mainsequence.markets -> mainsequence.tdag shared infrastructure
mainsequence.client -/-> mainsequence.markets
mainsequence.tdag -/-> mainsequence.markets
```

`mainsequence.markets` may depend on core SDK infrastructure. The core
`mainsequence.client` package must not depend on or import the markets
application.

## Supersession

This ADR supersedes ADR 0004's final canonical path decision. ADR 0004 remains
the historical record for the mechanical migration out of
`mainsequence/client/models_vam.py`, but `mainsequence.client.markets.models`
is no longer the target canonical market model path.

ADR 0009 still controls the removal of client-owned asset ORM behavior. This
ADR controls the package boundary and import path movement.

## Non-Goals

This migration must not:

- change backend endpoint URLs
- rename public market model classes as part of the path move
- change account, portfolio, order, virtual fund, asset, or instrument
  semantics
- make generic TDAG aware of market asset semantics
- keep `mainsequence.client` as the long-term owner of market models
- hide unresolved asset ownership problems behind new import aliases
- introduce a hard runtime dependency from generic `mainsequence.client` to
  `mainsequence.markets`

## Compatibility Policy

The canonical path after this migration is:

```python
mainsequence.markets.client.models
```

During the transition, compatibility modules may remain at:

```python
mainsequence.client.markets
mainsequence.client.markets.models
```

Those modules must be thin re-export shims only. They must not contain market
model implementation. They should emit a `DeprecationWarning` that points users
to `mainsequence.markets.client.models`.

The migration should not preserve the broad market star export from
`mainsequence.client` indefinitely. Any temporary compatibility export from
`mainsequence.client` must be explicit, documented, and scheduled for removal.

## Implementation Tasks

### Phase 1: Record The Boundary Decision

- [ ] Add this ADR as the canonical package-boundary decision.
- [ ] Update ADR 0004 with a supersession note that points to this ADR.
- [ ] Update ADR 0009 only if implementation changes alter its asset-model
      ownership decision.
- [ ] Confirm ADR 0008 remains true after market DataNodes stop depending on
      `mainsequence.client.markets.models.assets.AssetMixin`.

### Phase 2: Inventory The Current Import Surface

- [ ] Inventory every source import of `mainsequence.client.markets`.
- [ ] Inventory every source import of `mainsequence.markets`.
- [ ] Inventory every `from mainsequence.client import Asset` and
      `from mainsequence.client import AssetCategory` usage.
- [ ] Inventory every `import mainsequence.client as msc` usage inside
      `mainsequence/markets`.
- [ ] Inventory all CLI string references to market client models, including
      model-reference constants.
- [ ] Inventory documentation and example references to
      `mainsequence.client.markets.models`.
- [ ] Classify each market client symbol as one of:
      `move unchanged`, `move after dependency cleanup`, `replace with markets
      runtime`, or `delete after compatibility window`.

Representative search commands:

```bash
rg "mainsequence\.client\.markets" mainsequence tests examples docs agent_scaffold
rg "from mainsequence\.client import .*Asset|import mainsequence\.client as msc" mainsequence/markets tests examples docs
rg "mainsequence\.markets" mainsequence/client mainsequence/markets tests examples docs
```

### Phase 3: Break Asset ORM Coupling First

- [ ] Replace `AssetMixin` in
      `mainsequence/markets/markets_data_node.py` with a market-local asset
      identity contract.
- [ ] Decide the final `MarketDataNodeConfiguration.asset_list` shape:
      `list[str]`, market asset table rows, or structurally typed objects with
      `unique_identifier`.
- [ ] Update market asset-list validation so it only requires a non-empty
      `unique_identifier` value and rejects duplicates.
- [ ] Update `MarketDataNode.asset_unique_identifiers(...)` and
      `MarketDataNode.asset_dimension_filters(...)` to use the new asset
      identity contract.
- [ ] Update tests that currently instantiate
      `mainsequence.client.markets.models.assets.Asset` only to satisfy
      `MarketDataNode` validation.
- [ ] Remove the direct `AssetMixin` import from
      `mainsequence/markets/markets_data_node.py`.
- [ ] Add tests proving market DataNodes accept the final asset-list shape
      without importing client market asset ORM models.

### Phase 4: Move Asset Runtime Out Of Client Models

- [ ] Keep ADR 0009 as the controlling plan for removing
      `mainsequence.client.markets.models.assets` as a model-bearing runtime
      module.
- [ ] Move or replace asset catalog behavior with
      `mainsequence.markets.assets.simple_tables`.
- [ ] Move or replace asset category behavior with
      `AssetCategorySimpleTable` and
      `AssetCategoryMembershipSimpleTableUpdater`.
- [ ] Move or replace asset snapshot and pricing-detail behavior with
      `mainsequence.markets.assets.data_nodes`.
- [ ] Move FIGI registration and lookup behavior under
      `mainsequence.markets.assets.openfigi`.
- [ ] Remove portfolio-index asset behavior from the client asset ORM path;
      portfolio code should use `portfolio_index_asset_unique_identifier`
      strings and portfolio metadata tables.
- [ ] Remove `Asset`, `AssetMixin`, `AssetCategory`, `AssetSnapshot`,
      `AssetPricingDetail`, `PortfolioIndexAsset`, `resolve_asset`,
      `create_from_serializer_with_class`, and `get_model_class` from the
      generic client export surface after replacements are complete.

### Phase 5: Migrate Account, Portfolio, And Order DTOs

- [ ] Move account, portfolio, virtual fund, trade, execution, order, and shared
      market DTOs from
      `mainsequence/client/markets/models/accounts_and_portfolios.py` to
      `mainsequence/markets/client/models/accounts_and_portfolios.py`.
- [ ] Move shared market client definitions from
      `mainsequence/client/markets/models/core.py` to
      `mainsequence/markets/client/models/core.py`.
- [ ] Update account and order payload models so nested asset dictionaries are
      preserved as dictionaries or reduced to `unique_identifier` strings
      instead of reconstructed as client asset ORM objects.
- [ ] Remove direct `Asset` and `resolve_asset` imports from account and order
      DTO modules.
- [ ] Add tests proving account holdings, target positions, virtual fund
      holdings, and order payloads still parse current backend response shapes.
- [ ] Update market account DataNodes to import DTOs from
      `mainsequence.markets.client.models`.

### Phase 6: Move The Package Path

- [ ] Create `mainsequence/markets/client/`.
- [ ] Create `mainsequence/markets/client/__init__.py`.
- [ ] Create `mainsequence/markets/client/models/__init__.py`.
- [ ] Move implementation modules from:

      ```text
      mainsequence/client/markets/models/
      ```

      to:

      ```text
      mainsequence/markets/client/models/
      ```

- [ ] Update all implementation imports from:

      ```python
      mainsequence.client.markets.models
      ```

      to:

      ```python
      mainsequence.markets.client.models
      ```

- [ ] Use absolute imports in `mainsequence.markets.client.models` modules.
- [ ] Ensure package discovery includes the moved package through existing
      `mainsequence.*` setuptools discovery.
- [ ] Run an import check for the new canonical path.

### Phase 7: Add Transitional Compatibility Shims

- [ ] Replace `mainsequence/client/markets/__init__.py` with a shim that
      re-exports from `mainsequence.markets.client`.
- [ ] Replace `mainsequence/client/markets/models/__init__.py` with a shim that
      re-exports from `mainsequence.markets.client.models`.
- [ ] Replace old per-module paths, if kept during the transition, with shims
      that import from the new modules:

      ```text
      mainsequence/client/markets/models/core.py
      mainsequence/client/markets/models/assets.py
      mainsequence/client/markets/models/accounts_and_portfolios.py
      ```

- [ ] Emit `DeprecationWarning` from each compatibility path.
- [ ] Add tests proving old paths still work during the compatibility window
      and warn with the correct migration target.
- [ ] Add a removal milestone for the compatibility shims.

### Phase 8: Remove Generic Client Market Exports

- [ ] Remove `from mainsequence.client.markets.models import *` from
      `mainsequence/client/__init__.py`.
- [ ] Replace any temporary top-level `mainsequence.client.Asset` and
      `mainsequence.client.AssetCategory` compatibility with explicit imports
      or a documented lazy deprecation layer.
- [ ] Update `mainsequence/client/models_helpers.py` so it does not import
      markets through a star import.
- [ ] Update CLI code that imports `Portfolio` from the old market client path.
- [ ] Update CLI string constants such as
      `mainsequence.client.markets.models.Portfolio` to the new canonical path.
- [ ] Add tests proving `import mainsequence.client` does not import
      `mainsequence.markets`.

### Phase 9: Clean Market Runtime Imports

- [ ] Update `mainsequence/markets/portfolios/models.py` to stop importing
      `Asset` from `mainsequence.client`.
- [ ] Update portfolio signal modules that import `Asset` or `AssetCategory`
      from `mainsequence.client`.
- [ ] Update portfolio utility type annotations that use client `Asset`.
- [ ] Update account DataNodes to import market DTOs from
      `mainsequence.markets.client.models`.
- [ ] Update instrument data-interface modules that import generic constants
      through `mainsequence.client` when a narrower import exists.
- [ ] Remove client-to-markets imports from market client models, especially
      any asset model dependency on `mainsequence.markets.instruments`.
- [ ] Add tests proving `mainsequence.markets` imports without relying on
      `mainsequence.client` broad exports.

### Phase 10: Update Documentation And Examples

- [ ] Update README examples that teach market model imports.
- [ ] Update market tutorials to use
      `mainsequence.markets.client.models` or market runtime modules.
- [ ] Update docs that reference
      `mainsequence.client.markets.models.AssetMixin.query`.
- [ ] Update examples that import `Asset` or `AssetCategory` from
      `mainsequence.client`.
- [ ] Update generated reference source stubs if they are committed.
- [ ] Keep migration documentation that intentionally mentions the old path,
      but mark it as deprecated.

### Phase 11: Packaging Isolation

- [ ] Decide whether markets remains in the same distribution or becomes a
      separately installable distribution such as `mainsequence-markets`.
- [ ] If markets becomes separate, configure namespace package discovery so
      both distributions can contribute to `mainsequence`.
- [ ] Make the markets package depend on the core `mainsequence` package, not
      the reverse.
- [ ] Move market-only optional dependencies into market extras or the markets
      distribution.
- [ ] Ensure a core-only install can import `mainsequence.client` and
      `mainsequence.tdag` without installing market-only dependencies.
- [ ] Ensure a markets install can import `mainsequence.markets.client.models`
      and the market runtime modules.

### Phase 12: Verification

- [ ] Run static import searches and confirm no implementation import uses the
      old canonical path:

      ```bash
      rg "mainsequence\.client\.markets" mainsequence tests examples docs agent_scaffold
      ```

      Remaining matches must be compatibility shims, deprecation tests, or
      migration documentation.

- [ ] Run compile checks for moved modules:

      ```bash
      python -m py_compile mainsequence/markets/client/models/core.py
      python -m py_compile mainsequence/markets/client/models/assets.py
      python -m py_compile mainsequence/markets/client/models/accounts_and_portfolios.py
      ```

- [ ] Run targeted tests:

      ```bash
      pytest tests/test_client_markets_models_compat.py
      pytest tests/test_client_account_models.py
      pytest tests/test_client_asset_models.py
      pytest tests/test_markets_data_node.py
      pytest tests/test_account_data_nodes.py
      pytest tests/test_asset_simple_tables.py
      ```

- [ ] Run import-boundary tests:

      ```bash
      python - <<'PY'
      import sys

      import mainsequence.client
      assert "mainsequence.markets" not in sys.modules

      from mainsequence.markets.client.models import Portfolio
      assert Portfolio.__module__.startswith("mainsequence.markets.client.models")
      print("markets client boundary ok")
      PY
      ```

- [ ] Build a wheel or source distribution and confirm the moved package is
      included.
- [ ] If separate packaging is implemented, test both core-only and
      core-plus-markets installation modes.

## Risks

- Existing users may import market symbols from `mainsequence.client`.
- The old path is heavily referenced in tests, examples, docs, and CLI model
  strings.
- Moving files before removing `AssetMixin` coupling would preserve the current
  boundary violation under a new path.
- Compatibility shims can mask implementation imports that should be migrated.
- Separate packaging can expose hidden imports from core SDK modules into
  markets code.

## Open Questions

- Should `mainsequence.client.Asset` remain as a temporary compatibility export,
  or should it be removed in the same release as the canonical path change?
- Should the final markets distribution be a separate PyPI project or an extra
  inside the current distribution?
- Should `Asset` remain as a market client DTO at all after ADR 0009, or should
  all asset runtime behavior be represented by market simple tables and
  DataNodes?
- What release removes the `mainsequence.client.markets` compatibility shims?
