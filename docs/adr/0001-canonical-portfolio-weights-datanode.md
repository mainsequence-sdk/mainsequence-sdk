# ADR 0001: Canonical Portfolio Weights DataNode

Date: 2026-05-19

Status: Proposed

## Context

Virtual Fund Builder (VFB) currently stores portfolio weights through the portfolio's
own DataNode output. Each `PortfolioStrategy` instance is itself a `DataNode`.
Its configuration is portfolio-specific, so each portfolio receives its own
`storage_hash` and therefore its own persisted table.

The current pipeline is:

1. A signal strategy inherits `WeightsBase` and usually `DataNode`.
2. The signal emits long-form signal weights:
   - index: `time_index`, `unique_identifier`
   - column: `signal_weight`
3. `PortfolioStrategy.update()` interpolates those signal weights to the
   portfolio timeline.
4. A `RebalanceStrategyBase` implementation converts signal weights into
   executed weights.
5. `PortfolioStrategy._postprocess_weights()` returns executed weights in long
   form:
   - index: `time_index`, `unique_identifier`
   - columns: `weights_current`, `weights_before`, `price_current`,
     `price_before`, `volume_current`, `volume_before`
6. `PortfolioStrategy._add_serialized_weights()` serializes the executed
   weights into JSON columns on the portfolio's own output table:
   - `rebalance_weights`
   - `rebalance_price`
   - `volume`
   - `weights_at_last_rebalance`
   - `price_at_last_rebalance`
   - `volume_at_last_rebalance`

When `PortfolioStrategy.run(add_portfolio_to_markets_backend=True)` is used,
VFB also creates or patches a Markets `Portfolio` through
`Portfolio.create_from_time_series(...)`. That portfolio points at:

- `data_node_update`: the portfolio backtest DataNode update
- `signal_data_node_update`: the signal DataNode update
- `index_asset`: the `PortfolioIndexAsset` representing the portfolio as an
  asset

This model has two useful properties:

- The portfolio output is self-contained. A user can inspect one portfolio table
  and see `close`, `return`, and serialized rebalance state.
- Signal generation and execution are separate. The signal DataNode can be
  shared by multiple portfolios that use different rebalance rules.

It also has an important limitation:

- Portfolio weights are not available in one canonical long-form DataNode across
  all portfolios. Cross-portfolio analysis must first discover every portfolio
  table, parse JSON columns, and union the results manually.

The desired canonical storage contract is:

- `time_index`
- `unique_identifier` from the portfolio asset being held
- `weight`
- `portfolio_index_asset_unique_identifier`

This ADR only proposes the architecture and implementation tasks. It does not
implement the change.

## Current Architecture Findings

### VFB already has the right intermediate shape

The best source for canonical weights is not the final serialized JSON columns.
It is the long-form executed weights produced after rebalancing and before
serialization.

The existing `PortfolioStrategy` path already creates this shape:

- `_postprocess_weights(weights)` stacks rebalance output into
  `(time_index, unique_identifier)` rows.
- `weights_current` is the target executed weight to publish.
- `_add_serialized_weights(portfolio, weights)` later pivots and JSON-serializes
  the same information into the per-portfolio table.

That means the canonical table should standardize on the post-rebalance executed
weight semantics, specifically `weights_current`. For live VFB integration, the
cleanest source is the long-form `weights` DataFrame before JSON serialization.
For backfill and decoupled post-run sync, the same information can be
reconstructed from the persisted `rebalance_weights` JSON column.

### Signal weights are not enough

The signal DataNode stores `signal_weight`, not executed portfolio weight.
For `ImmediateSignal`, signal and executed weights are effectively the same.
For other rebalance strategies, they are not guaranteed to match.

Therefore the canonical table must represent executed portfolio weights from the
portfolio output path, not raw signal weights.

### The portfolio index asset is created after backend sync

The canonical table needs `portfolio_index_asset_unique_identifier`. That value
comes from the `PortfolioIndexAsset` attached to the Markets `Portfolio`.

The canonical writer therefore depends on one of these being available:

- the `Portfolio` returned by `Portfolio.create_from_time_series(...)`, or
- a lookup through `PortfolioIndexAsset.get(reference_portfolio__data_node_update__update_hash=...)`

If a portfolio is run without syncing to the Markets backend, the canonical
writer cannot publish the required portfolio identity unless the caller provides
an equivalent portfolio index asset unique identifier.

### Canonical storage must support many portfolios holding the same asset

A canonical table cannot use only `(time_index, unique_identifier)` as its
physical index. Two portfolios can hold the same `unique_identifier` at the same
`time_index`.

The persisted DataFrame should therefore use:

```text
index:   time_index, unique_identifier, portfolio_index_asset_unique_identifier
columns: weight
```

After `reset_index()`, the physical table exposes exactly the desired fields:

```text
time_index
unique_identifier
portfolio_index_asset_unique_identifier
weight
```

This preserves the platform convention that `unique_identifier` is the held
asset identity while preventing collisions between portfolios.

### Namespace should isolate canonical storages, not model business meaning

TDAG `hash_namespace` is an isolation mechanism for tests and experiments. It is
not a business key.

The canonical weights design should create one canonical storage per TDAG hash
namespace:

- production-style runs with no namespace share the production canonical table
- `hash_namespace("pytest_case")` creates an isolated canonical table
- `portfolio_index_asset_unique_identifier` partitions portfolios inside the
  table

The namespace should remain `DataNodeStorage.namespace` metadata, not a row-level
business column.

## Decision

Introduce a canonical VFB portfolio weights DataNode that normalizes executed
portfolio weights from all portfolios into one long-form table per TDAG hash
namespace.

Recommended class shape:

```python
class PortfolioWeightsConfig(DataNodeConfiguration):
    source_portfolio_data_node_update_id: int = Field(
        ...,
        json_schema_extra={"update_only": True},
    )
    portfolio_index_asset_unique_identifier: str | None = Field(
        default=None,
        json_schema_extra={"update_only": True},
    )
```

Recommended DataNode name:

- Class: `CanonicalPortfolioWeights` or `PortfolioWeights`
- Published identifier: `canonical_portfolio_weights`
- Module: `mainsequence.markets.virtualfundbuilder.portfolio_weights`

The final class name should be chosen after checking import/API conflicts with
the existing Markets `HistoricalWeights` endpoint. The published identifier
should be unambiguous even if the class name is shortened to `PortfolioWeights`.

The config fields that identify a source portfolio must be `update_only` because
they define writer scope, not table meaning. This is how multiple portfolio
writer jobs can share the same `storage_hash` while still having different
`update_hash` values.

The canonical DataNode output contract is:

```text
index:
  time_index: datetime64[ns, UTC]
  unique_identifier: string
  portfolio_index_asset_unique_identifier: string

columns:
  weight: float64
```

The table semantics are:

- one row per portfolio, timestamp, and held asset
- `unique_identifier` is the asset held by the portfolio
- `portfolio_index_asset_unique_identifier` is the index asset representing the
  portfolio
- `weight` is the executed current portfolio weight, equivalent to
  `weights_current` in the VFB rebalance output

The canonical table should not replace the current per-portfolio output table in
the first implementation. The existing portfolio table remains the source of
`close`, `return`, and full execution context. The canonical table adds a query
optimized, normalized weights surface.

## Proposed Data Flow

### Normal VFB run

1. `PortfolioStrategy.run(...)` updates the portfolio DataNode as today.
2. If requested, it syncs the portfolio into Markets and obtains the
   `PortfolioIndexAsset`.
3. A canonical weights writer is instantiated for that source portfolio.
4. The canonical writer receives or reconstructs executed weights and writes
   normalized rows into the shared canonical table.

### Canonical writer behavior

For each source portfolio row when reconstructing from persisted portfolio
output:

1. Read `rebalance_weights`.
2. Parse JSON into `{asset_unique_identifier: weight}`.
3. Explode the mapping into rows.
4. Add `portfolio_index_asset_unique_identifier`.
5. Return a DataFrame indexed by:
   - `time_index`
   - `unique_identifier`
   - `portfolio_index_asset_unique_identifier`
6. Use `weight` as the only value column.

### Incremental updates

The writer should avoid full-table rewrites.

Because the canonical table has an additional portfolio index level, the writer
must derive the latest persisted `time_index` for its own
`portfolio_index_asset_unique_identifier`, not just the latest timestamp for an
asset across all portfolios.

Implementation options:

1. Prefer using the canonical table's nested `asset_time_statistics`, which
   already supports extra index levels.
2. Add a small helper on the canonical DataNode to compute:
   `max(time_index where portfolio_index_asset_unique_identifier == X)`.
3. Query the source portfolio table from that timestamp forward.

The writer should be idempotent for a portfolio and timestamp. Re-running the
same source portfolio should either filter out already-persisted rows or
overwrite only that portfolio/time window.

## Consequences

### Positive

- Cross-portfolio weights become a normal DataNode query instead of a manual
  JSON-union process.
- Dashboards, risk jobs, and analytics agents can join weights to prices,
  categories, and asset metadata directly.
- Existing per-portfolio tables remain backward compatible.
- The canonical table follows the SDK DataNode guidance: stable table contract,
  portfolio-specific writer scope.

### Negative

- One additional DataNode update must run per portfolio.
- The canonical table introduces a new multi-index shape with an extra index
  level, so some existing helper APIs that only filter `unique_identifier` may
  need convenience wrappers for portfolio-level queries.
- Backfill must parse historical JSON columns from existing portfolio tables.
- The canonical writer must wait until the portfolio index asset is known.

### Compatibility

This should be an additive change.

Do not remove or rename these existing columns in the per-portfolio output:

- `rebalance_weights`
- `weights_at_last_rebalance`
- `rebalance_price`
- `price_at_last_rebalance`
- `volume`
- `volume_at_last_rebalance`

Existing `Portfolio.get_latest_weights()` can continue using the current backend
endpoint until a canonical-table based replacement is available.

## Alternatives Considered

### Keep weights only inside each portfolio DataNode

Rejected. This is the current state and does not provide a canonical,
cross-portfolio query surface.

### Store canonical rows as a side effect inside `PortfolioStrategy.update()`

Rejected for the first implementation. It would mix two persistence contracts in
one update method and make failures harder to reason about. VFB can still offer a
post-run orchestration option, but the canonical weights table should be owned by
its own DataNode.

### Use `portfolio_index_asset_unique_identifier` as `unique_identifier`

Rejected. The user-facing requirement is that `unique_identifier` should come
from the held portfolio asset. It also aligns with existing VFB contracts where
`unique_identifier` means the asset in the weight vector.

### Keep `portfolio_index_asset_unique_identifier` as a plain column only

Rejected unless the storage layer gains a different uniqueness rule. With many
portfolios, `(time_index, unique_identifier)` is not unique. The portfolio index
asset identifier must participate in the persisted index to prevent duplicate
rows when two portfolios hold the same asset at the same time.

### Replace per-portfolio JSON columns immediately

Rejected. That would be a breaking migration. The canonical table should first
be added as a read-optimized normalized mirror.

## Implementation Tasks

### Phase 1: Canonical DataNode skeleton

1. Add `mainsequence/virtualfundbuilder/portfolio_weights.py`.
2. Add a `PortfolioWeightsConfig` based on `DataNodeConfiguration`.
3. Mark source portfolio identity fields as `update_only`.
4. Add table metadata with identifier `canonical_portfolio_weights`.
5. Add column metadata for `weight`.
6. Implement the output index contract:
   `["time_index", "unique_identifier", "portfolio_index_asset_unique_identifier"]`.

### Phase 2: Source portfolio resolution

1. Resolve the source `Portfolio` from `source_portfolio_data_node_update_id`.
2. Resolve `portfolio_index_asset_unique_identifier` from the source portfolio's
   `index_asset`.
3. Fail clearly when no portfolio index asset can be resolved.
4. Build an `APIDataNode` dependency for the source portfolio output table.
5. Support both `PortfolioStrategy` and `PortfolioFromDF` outputs when
   `rebalance_weights` is present.

### Phase 3: Weight normalization

1. Read source portfolio rows incrementally.
2. Parse `rebalance_weights` from canonical JSON string to a dict.
3. Explode dict entries into rows:
   - `time_index`
   - held asset `unique_identifier`
   - `portfolio_index_asset_unique_identifier`
   - `weight`
4. Drop null weights.
5. Cast:
   - `time_index` to UTC-aware datetime
   - identifiers to string
   - `weight` to float
6. Validate there are no duplicate rows on the full three-level index.

### Phase 4: Incremental and idempotent writes

1. Add a helper to compute the latest canonical timestamp for a source portfolio.
2. Use the helper to read only new source portfolio rows.
3. Confirm that repeated runs for the same source portfolio do not duplicate
   rows.
4. Confirm that two portfolios holding the same asset at the same time persist
   successfully because the portfolio index asset is part of the index.

### Phase 5: VFB orchestration integration

1. Add an explicit orchestration option such as
   `sync_canonical_portfolio_weights=True`.
2. Run the canonical writer only after the portfolio table has been persisted.
3. Require Markets backend sync, or require an explicit
   `portfolio_index_asset_unique_identifier` override.
4. Keep the default behavior backward compatible until rollout is complete.

### Phase 6: Backfill command or utility

1. Add a backfill utility that lists `Portfolio` records with
   `data_node_update`.
2. Instantiate one canonical writer per portfolio.
3. Preserve the source DataNodeStorage namespace when creating the canonical
   writer.
4. Backfill in batches to avoid loading all portfolio history at once.
5. Report skipped portfolios with missing index assets or missing
   `rebalance_weights`.

### Phase 7: Query helpers and docs

1. Add a helper for retrieving canonical weights by
   `portfolio_index_asset_unique_identifier`.
2. Add a helper for retrieving canonical weights by held asset
   `unique_identifier`.
3. Document the canonical table in VFB data contracts.
4. Document the migration relationship between per-portfolio JSON weights and
   canonical long-form weights.
5. Add CLI or SDK examples for cross-portfolio queries.

### Phase 8: Tests

1. Unit test JSON normalization from `rebalance_weights`.
2. Unit test output dtypes and index names.
3. Unit test two portfolios holding the same asset at the same timestamp.
4. Unit test incremental filtering for a single portfolio.
5. Unit test namespace isolation with `hash_namespace(...)`.
6. Integration test `PortfolioStrategy` plus canonical writer.
7. Integration test `PortfolioFromDF` plus canonical writer.

## Open Questions

1. Should the public class name be `PortfolioWeights` or
   `CanonicalPortfolioWeights`?
2. Should canonical sync be opt-in during rollout or enabled automatically when
   `add_portfolio_to_markets_backend=True`?
3. Should `Portfolio.get_latest_weights()` eventually read from the canonical
   table, or should it remain backed by the existing Markets endpoint?
4. Do downstream filters need first-class support for
   `portfolio_index_asset_unique_identifier`, or is `run_query(...)` sufficient
   for the first release?
5. Should the canonical table include additional lineage columns later, such as
   source `portfolio_data_node_update_id` or source `data_node_update_hash`?

## Rollout Plan

1. Add the canonical DataNode behind explicit opt-in.
2. Backfill a small namespace or test tenant first.
3. Compare canonical weights to parsed per-portfolio JSON weights for selected
   portfolios.
4. Enable canonical sync for new portfolio builds.
5. Backfill production portfolios in batches.
6. Add canonical query helpers to dashboards and analytics code.
7. Consider deprecating direct JSON parsing only after canonical coverage is
   complete.

## Acceptance Criteria

The implementation is complete when:

1. A single canonical DataNodeStorage exists per TDAG hash namespace.
2. Multiple source portfolios write to the same canonical storage.
3. The physical rows expose:
   - `time_index`
   - `unique_identifier`
   - `portfolio_index_asset_unique_identifier`
   - `weight`
4. Two portfolios can hold the same asset at the same timestamp without
   duplicate-index failures.
5. Existing per-portfolio VFB outputs remain backward compatible.
6. Backfill can reconstruct canonical rows from existing portfolio tables.
7. VFB documentation explains when to use the canonical table versus the
   per-portfolio output table.
