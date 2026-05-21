# ADR 0005: Unified VFB Weight DataNodes

Date: 2026-05-21

Status: Proposed

Depends on: [ADR 0002](0002-multidimensional-data-node-update-contract.md)

## Context

ADR 0002 changed the architecture. A timestamped DataNode table is no longer
only one of these two shapes:

- `time_index`
- `time_index`, `unique_identifier`

The current contract is:

```text
index_names = [time_index_name, *identity_dimensions]
```

Only `time_index_name` is special. Every index after it is an identity dimension
used for uniqueness, update progress, reads, latest-observation lookup, and tail
delete.

This is exactly what portfolio and signal weights need. The portfolio or signal
identity should be an index dimension, not a reason to create another physical
table.

## Current Architecture Findings

### AccountHoldings is the best current pattern

`mainsequence/markets/accounts/data_nodes.py` now defines a domain-specific
DataNode contract for holdings:

```text
AccountHoldings:
  index_names = ["time_index", "account_uid", "unique_identifier"]

VirtualFundHoldings:
  index_names = ["time_index", "fund_uid", "unique_identifier"]
```

The important design points are:

- the table contract is a domain object, not an incidental subclass output
- the owner identity is an index dimension
- the held asset remains `unique_identifier`
- the table validates its exact `time_index_name`, `index_names`, and
  `column_dtypes_map`
- `ensure_storage_ready()` creates or validates the source table before writes
- domain-specific backend initializers can create lookup indexes without
  requiring a bootstrap row
- operational helpers bind domain objects to the DataNode storage id

Portfolio weights and signal weights should follow the same shape.

### VFB still stores too much by portfolio-specific DataNode identity

`PortfolioStrategy` is still a `DataNode`. Its configuration contains the whole
portfolio build:

- asset universe
- price configuration
- execution configuration
- signal instance
- rebalance strategy
- Markets metadata

Those fields are portfolio-specific. If they remain part of the storage identity,
VFB will keep creating one physical output table per portfolio.

That behavior is useful for legacy self-contained backtests, but it is not the
right storage model for shared operational tables. In the multidimensional
contract, the portfolio-specific parts should be writer/update scope and lineage.
The table meaning is the stable schema:

- "portfolio weights over time"
- "signal weights over time"
- optionally "portfolio values over time"

### VFB already has the right in-memory frames

The live portfolio path should not parse JSON when it can avoid it.

`PortfolioStrategy.update()` already computes:

- interpolated signal weights
- rebalance output
- postprocessed long-form executed weights
- portfolio `close` and `return`

The best live source for canonical portfolio weights is the post-rebalance
long-form `weights` frame before `_add_serialized_weights()` turns it into JSON
columns.

Existing portfolio tables can still be backfilled by parsing `rebalance_weights`,
but that should be a migration path, not the primary architecture.

### Signal DataNodes are also fragmented

Built-in signal strategies such as `FixedWeights`, `MarketCap`,
`ExternalWeights`, `MockSignal`, `IntradayTrend`, and `ETFReplicator` currently
produce:

```text
index:   time_index, unique_identifier
column:  signal_weight
```

Each signal strategy is a DataNode instance with strategy-specific config. That
means signal weights also fragment by DataNode identity.

With ADR 0002, the shared shape should be:

```text
index:   time_index, signal_uid, unique_identifier
column:  signal_weight
```

`signal_uid` is the signal identity dimension. `unique_identifier` remains the
asset being weighted.

### Namespace is table-family selection, not a row dimension

The user needs to be able to pass a namespace.

This namespace should choose which shared table family the run writes to. It
should not be a row-level business column because the table already has domain
identity dimensions:

- portfolio identity for portfolio weights
- signal identity for signal weights
- asset identity through `unique_identifier`

For example:

```python
PortfolioWeights(namespace="research")
SignalWeights(namespace="research")
```

should create or resolve namespaced DataNodeStorage rows whose source-table
contracts are the same as production, but whose storage identity is isolated.

The SDK already has `hash_namespace`. The VFB public API should expose a clearer
domain parameter, likely named `namespace`, and map it to DataNode namespace
plumbing internally.

## Decision

Introduce unified VFB weight DataNodes modeled after `AccountHoldings`.

The first release should add two primary tables:

1. `PortfolioWeights`
2. `SignalWeights`

It should also reserve a compatible shape for a future `PortfolioValues` table
so the whole VFB portfolio output can move away from per-portfolio DataNode
tables over time.

## Table Contracts

### PortfolioWeights

Purpose:

- store executed portfolio weights from all portfolios in one table per
  namespace
- represent the output of rebalance logic, not raw signal intent
- make cross-portfolio risk, allocation, and exposure analysis queryable without
  parsing JSON

Recommended contract:

```text
role: portfolio_weights
identifier: mainsequence.markets.portfolio_weights
time_index_name: time_index
index_names:
  - time_index
  - portfolio_index_asset_unique_identifier
  - unique_identifier
columns:
  weight: float64
  weight_before: float64
  price_current: float64
  price_before: float64
  volume_current: float64
  volume_before: float64
  extra_details: jsonb
```

Minimum required columns for the first implementation:

```text
weight
```

Recommended first implementation columns:

```text
weight
weight_before
price_current
price_before
volume_current
volume_before
extra_details
```

Rationale:

- `weight` maps to VFB `weights_current`
- `weight_before` maps to VFB `weights_before`
- price and volume fields preserve the execution context currently hidden in
  serialized portfolio JSON columns
- `extra_details` gives room for rebalance strategy metadata without schema
  churn

The index order should put portfolio identity before asset identity:

```text
time_index -> portfolio_index_asset_unique_identifier -> unique_identifier
```

That makes the canonical update-progress path:

```text
portfolio_index_asset_unique_identifier -> unique_identifier -> timestamp
```

and lets one portfolio update its own asset rows without being blocked by other
portfolios that hold the same asset.

The physical table still exposes the requested fields:

```text
time_index
portfolio_index_asset_unique_identifier
unique_identifier
weight
```

### SignalWeights

Purpose:

- store signal weights from all VFB signals in one table per namespace
- preserve raw signal intent before execution/rebalance logic
- let different portfolios point to the same signal history without each signal
  requiring a separate physical table

Recommended contract:

```text
role: signal_weights
identifier: mainsequence.markets.signal_weights
time_index_name: time_index
index_names:
  - time_index
  - signal_uid
  - unique_identifier
columns:
  signal_weight: float64
  extra_details: jsonb
```

`signal_uid` should be stable for the logical signal. The recommended order for
resolving it is:

1. explicit `signal_uid` supplied by the user or signal config
2. `DataNodeStorage.identifier` when the signal is already published under a
   stable identifier
3. `DataNodeUpdate.update_hash` as a compatibility fallback

The fallback is acceptable for migration, but not ideal as the long-term public
identity because update hashes can change when updater scope changes.

### PortfolioValues

This ADR does not require `PortfolioValues` in the first implementation, but it
should be planned now because it completes the migration away from one portfolio
DataNode table per portfolio.

Recommended contract:

```text
role: portfolio_values
identifier: mainsequence.markets.portfolio_values
time_index_name: time_index
index_names:
  - time_index
  - portfolio_index_asset_unique_identifier
columns:
  close: float64
  return: float64
  calculated_close: float64
  close_time: datetime64[ns, UTC]
  extra_details: jsonb
```

Once this exists, the legacy per-portfolio VFB table can become optional or
compatibility-only.

## DataNode Class Design

Use the AccountHoldings contract pattern.

Recommended module:

```text
mainsequence/markets/virtualfundbuilder/data_nodes.py
```

Recommended abstractions:

```python
class VFBWeightsDataNodeConfiguration(DataNodeConfiguration):
    pass

@dataclass(frozen=True)
class VFBWeightsDataNodeContract:
    role: str
    schema_version: int
    description: str
    time_index_name: str
    index_names: list[str]
    column_dtypes_map: dict[str, str]
    column_labels: dict[str, str]
    column_descriptions: dict[str, str]

class VFBWeightsDataNode(DataNode):
    WEIGHTS_CONTRACT: ClassVar[VFBWeightsDataNodeContract]
    SOURCE_TABLE_INITIALIZER_METHOD: ClassVar[str] = "initialize_source_table"
```

Concrete classes:

```python
class PortfolioWeights(VFBWeightsDataNode):
    WEIGHTS_CONTRACT = PORTFOLIO_WEIGHTS_CONTRACT
    SOURCE_TABLE_INITIALIZER_METHOD = "initialize_portfolio_weights_source_table"

class SignalWeights(VFBWeightsDataNode):
    WEIGHTS_CONTRACT = SIGNAL_WEIGHTS_CONTRACT
    SOURCE_TABLE_INITIALIZER_METHOD = "initialize_signal_weights_source_table"
```

Each class should provide:

- default config with `DataNodeMetaData(identifier=...)`
- `build_schema_bootstrap_frame()`
- `validate_weights_frame()`
- `ensure_storage_ready()`
- `_initialize_source_table_storage_or_none()`
- `_validate_storage_contract()`

The default `update()` may return a schema bootstrap frame, like holdings, so the
table can be created through the standard DataNode path when the domain endpoint
is not available.

## Namespace API

The user-facing VFB API should accept `namespace`.

Examples:

```python
portfolio_weights = PortfolioWeights(namespace="research")
signal_weights = SignalWeights(namespace="research")
```

and:

```python
portfolio_node.run(
    add_portfolio_to_markets_backend=True,
    sync_weight_tables=True,
    namespace="research",
)
```

Implementation rule:

- `namespace` maps to DataNode hash namespace/storage namespace internally
- namespace is not included as an output column
- namespace is not a portfolio identity
- namespace is not a signal identity
- all canonical VFB weight tables created with the same namespace share the same
  storage family for that namespace

If the SDK keeps `hash_namespace` as the low-level DataNode argument, VFB should
still expose `namespace` to avoid leaking test-oriented language into market
domain code.

## Storage Identity Rules

To actually unify tables, the canonical table class and table config must stay
stable.

Do not let these fields participate in the `storage_hash` for the canonical
tables:

- portfolio id
- portfolio name
- portfolio config
- signal config
- rebalance config
- asset universe
- writer/source DataNode update id
- writer/source storage hash

Those are row identity, update scope, or lineage. They are not table meaning.

The table meaning is:

- `PortfolioWeights`: VFB executed weights indexed by portfolio and asset
- `SignalWeights`: VFB signal weights indexed by signal and asset

If an implementation needs writer-specific configuration, mark it
`update_only` or keep it outside the canonical table DataNode constructor.

## VFB Integration

### PortfolioStrategy live path

`PortfolioStrategy.update()` should keep computing:

- long-form postprocessed `weights`
- `portfolio` values

After `weights = self._postprocess_weights(weights)`, the live path can convert
that frame into `PortfolioWeights` rows:

```text
time_index
portfolio_index_asset_unique_identifier
unique_identifier
weight              <- weights_current
weight_before       <- weights_before
price_current
price_before
volume_current
volume_before
```

The portfolio identity should come from the `PortfolioIndexAsset`.

This means canonical sync must run after the portfolio has a Markets backend
identity, or the user must provide `portfolio_index_asset_unique_identifier`
explicitly.

Recommended first release behavior:

- keep the current per-portfolio DataNode output unchanged
- add opt-in canonical sync
- write canonical `PortfolioWeights` from the in-memory `weights` frame
- use persisted `rebalance_weights` JSON only for backfill

Recommended later behavior:

- add `PortfolioValues`
- make the legacy per-portfolio DataNode output optional
- remove the need to parse serialized rebalance JSON in normal reads

### SignalWeights live path

There are two possible migration paths.

#### Option A: Mirror signal output into SignalWeights

Keep existing signal strategy DataNodes unchanged, but after each signal update,
normalize rows into the shared `SignalWeights` table.

Pros:

- lowest risk
- preserves all existing VFB code
- gives immediate canonical query surface

Cons:

- still computes through per-signal DataNode tables
- requires a mirroring step
- does not fully eliminate signal table fragmentation yet

#### Option B: Make SignalWeights the primary signal storage

Refactor `WeightsBase` and built-in signal DataNodes so their output includes
`signal_uid` as an identity dimension and writes directly to the shared
`SignalWeights` table.

Pros:

- actually unifies signal storage
- removes one layer of mirroring
- aligns fully with ADR 0002

Cons:

- larger refactor
- every signal config must provide or derive stable `signal_uid`
- `WeightsBase.interpolate_index()` must filter by `signal_uid`
- custom user signal DataNodes need a migration story

Recommended path:

1. implement Option A first
2. expose the canonical `SignalWeights` query surface
3. migrate built-in signals to Option B
4. keep Option A as a compatibility bridge for custom signals

## Backend/API Additions

Mirror the AccountHoldings domain endpoints.

Required client methods on `DataNodeStorage`:

```python
initialize_portfolio_weights_source_table(...)
initialize_signal_weights_source_table(...)
```

Recommended backend endpoints:

```text
POST /orm/api/assets/portfolio-weights-data-node/{id}/initialize-source-table/
POST /orm/api/assets/signal-weights-data-node/{id}/initialize-source-table/
```

If `PortfolioValues` is included:

```text
POST /orm/api/assets/portfolio-values-data-node/{id}/initialize-source-table/
```

The backend should derive:

- identity dimensions from `index_names[1:]`
- uniqueness over full `index_names`
- lookup indexes for portfolio/signal and asset dimensions
- update progress by full identity coordinate

## Query Examples

### All weights for one portfolio

```python
df = portfolio_weights.get_df_between_dates(
    start_date=start,
    end_date=end,
    dimension_filters={
        "portfolio_index_asset_unique_identifier": [portfolio_uid],
    },
)
```

### One asset across all portfolios

```python
df = portfolio_weights.get_df_between_dates(
    start_date=start,
    end_date=end,
    dimension_filters={
        "unique_identifier": ["ASSET:BTC"],
    },
)
```

### One signal across its assets

```python
df = signal_weights.get_df_between_dates(
    start_date=start,
    end_date=end,
    dimension_filters={
        "signal_uid": ["market_cap_top_20"],
    },
)
```

### Incremental range for one portfolio and asset

```python
df = portfolio_weights.get_df_between_dates(
    dimension_range_map=[
        {
            "coordinate": {
                "portfolio_index_asset_unique_identifier": portfolio_uid,
                "unique_identifier": asset_uid,
            },
            "start_date": last_seen,
            "start_date_operand": ">",
        }
    ],
)
```

## Consequences

### Positive

- Portfolio weights can be queried across portfolios without discovering many
  portfolio-specific tables.
- Signal weights can be queried across signal producers.
- The design uses ADR 0002's multidimensional update contract directly.
- `unique_identifier` keeps its existing meaning: the held or signaled asset.
- Namespace gives users isolated table families without changing row shape.
- This aligns VFB with the AccountHoldings storage model.

### Negative

- The first implementation needs domain-specific table initialization endpoints.
- Existing VFB signal strategies need a migration path.
- `PortfolioStrategy` still has a legacy per-portfolio output until
  `PortfolioValues` is introduced.
- Stable signal identity is not currently a first-class VFB concept and must be
  added.

### Compatibility

Existing per-portfolio VFB DataNodes should remain readable during migration.

Existing columns such as `rebalance_weights` and `weights_at_last_rebalance`
should not be removed in the first release.

Existing custom signal DataNodes should continue to work through the mirror path
until they opt into direct `SignalWeights` storage.

## Implementation Tasks

### Phase 1: Contracts

1. Add VFB weight DataNode contract classes.
2. Define `PORTFOLIO_WEIGHTS_CONTRACT`.
3. Define `SIGNAL_WEIGHTS_CONTRACT`.
4. Optionally define `PORTFOLIO_VALUES_CONTRACT`.
5. Add validators that enforce exact index names, required columns, dtypes, and
   duplicate-free full index tuples.

### Phase 2: Source table initialization

1. Add `DataNodeStorage.initialize_portfolio_weights_source_table(...)`.
2. Add `DataNodeStorage.initialize_signal_weights_source_table(...)`.
3. Add backend domain endpoints and lookup indexes.
4. Match AccountHoldings fallback behavior: domain initializer first, bootstrap
   `run(force_update=True)` second.

### Phase 3: Namespace plumbing

1. Add `namespace` to VFB-facing constructors/helpers.
2. Internally pass namespace through DataNode namespace plumbing.
3. Ensure `PortfolioWeights(namespace=X)` and `SignalWeights(namespace=X)` use
   stable table contracts inside the same namespace.
4. Add tests that the same table contract with different namespaces produces
   isolated storage.

### Phase 4: PortfolioWeights sync

1. Add a normalizer from postprocessed VFB `weights` frames to
   `PortfolioWeights` rows.
2. Add a normalizer from persisted `rebalance_weights` JSON for backfill.
3. Add opt-in `sync_weight_tables=True` to `PortfolioStrategy.run(...)` or an
   adjacent VFB orchestration helper.
4. Require or resolve `portfolio_index_asset_unique_identifier`.
5. Validate that two portfolios holding the same asset at the same timestamp can
   write to the same table.

### Phase 5: SignalWeights sync

1. Add `signal_uid` to VFB signal configuration or wrapper metadata.
2. Add a mirror path from existing signal DataNodes into `SignalWeights`.
3. Update `WeightsBase.interpolate_index()` to optionally read from
   `SignalWeights` with `dimension_filters={"signal_uid": [...]}`.
4. Migrate built-in signals one by one to direct shared-table storage.

### Phase 6: PortfolioValues

1. Add the optional `PortfolioValues` contract.
2. Write `close`, `return`, and value metadata indexed by
   `portfolio_index_asset_unique_identifier`.
3. Update VFB docs to describe which table owns values and which table owns
   weights.
4. De-emphasize per-portfolio JSON output after canonical coverage is complete.

### Phase 7: Backfill

1. List existing Markets `Portfolio` records with `data_node_update`.
2. Resolve each `PortfolioIndexAsset`.
3. Parse existing portfolio output JSON into `PortfolioWeights`.
4. List existing signal DataNode storages where possible.
5. Mirror signal rows into `SignalWeights`.
6. Run backfill per namespace, preserving source table namespace.
7. Report missing identities and skipped rows explicitly.

### Phase 8: Tests

1. Contract validation for `PortfolioWeights`.
2. Contract validation for `SignalWeights`.
3. Namespace isolation tests.
4. Full-index duplicate detection tests.
5. Dimension-filter query payload tests.
6. Incremental update tests using nested `index_progress`.
7. PortfolioStrategy sync tests.
8. Signal mirror tests.
9. Backfill JSON parsing tests.

## Open Questions

1. Should `signal_uid` be required in every signal config, or can it be generated
   from signal DataNode metadata for one release?
2. Should `namespace` map directly to `hash_namespace`, or should the SDK add a
   market-domain alias that is stored separately but still affects storage
   identity?
3. Should the first implementation include `PortfolioValues`, or should it focus
   only on `PortfolioWeights` and `SignalWeights`?
4. Should `PortfolioWeights` store only `weight`, or also the execution context
   columns currently present in the rebalance output?
5. Should canonical writes happen inside `PortfolioStrategy.run(...)`, or should
   an orchestration object own the sync after portfolio construction?

## Acceptance Criteria

The new architecture is complete when:

1. All portfolios in the same namespace can write to one `PortfolioWeights`
   DataNodeStorage.
2. All signals in the same namespace can write to one `SignalWeights`
   DataNodeStorage.
3. `unique_identifier` always means the held/signaled asset.
4. Portfolio identity is represented by
   `portfolio_index_asset_unique_identifier`.
5. Signal identity is represented by `signal_uid`.
6. The user can pass `namespace` to select an isolated shared table family.
7. The tables use ADR 0002 dimension filters and nested index progress.
8. Existing VFB per-portfolio tables remain compatible during migration.
