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
- "portfolio values over time"

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

`signal_uid` is the signal identity dimension. It must not be a human label such
as `"market_cap_top_100"`. It is the deterministic unique hash of the canonical
signal configuration, matching the identity mechanism that currently makes a
signal retrievable as its own DataNode. `unique_identifier` remains the asset
being weighted.

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

## Workflow Comparison

### Old workflow: portfolio identity is the portfolio DataNode identity

Today the `PortfolioStrategy` DataNode identity is doing two jobs at once:

- identifying the physical output table
- identifying the portfolio configuration

That is why every materially different portfolio configuration produces its own
portfolio DataNode storage.

```text
 PortfolioStrategyConfig
 (assets, prices, execution, signal instance,
  rebalance strategy, Markets metadata)
        |
        | DataNode hashing over full portfolio configuration
        v
 PortfolioStrategy DataNode
 storage_hash/update_hash = hash(PortfolioStrategyConfig)
        |
        +--------------------+
        |                    |
        v                    v
 Signal DataNode(s)      Price / interpolation DataNode(s)
 per signal config       per price config
        |                    |
        +----------+---------+
                   |
                   v
 PortfolioStrategy.update()
 - interpolate signal weights
 - rebalance into executed weights
 - compute close / return
 - serialize rebalance weights into JSON columns
                   |
                   v
 Per-portfolio DynamicTableMetaData
 one physical table per PortfolioStrategyConfig hash
 rows contain portfolio values plus serialized weights
                   |
                   v
 Portfolio.create_from_time_series(data_node_update_id)
                   |
                   v
 PortfolioIndexAsset
 unique_identifier is tied to the backend Portfolio created
 from that portfolio-specific DataNodeUpdate
```

Storage produced by the old workflow:

```text
N portfolio configs
  -> N PortfolioStrategy DataNode storages
  -> N portfolio output tables
  -> signal DataNode storages still fragmented by signal config
  -> price/interpolation DataNode storages by price config
```

### New workflow: table identity is static, portfolio identity is a row dimension

The new workflow separates the shared table identity from the portfolio identity.
The canonical DataNodes identify stable storage contracts. The full portfolio
configuration is still unique, but that uniqueness becomes the identity of the
`PortfolioIndexAsset`, not the identity of the canonical table.

```text
 PortfolioConfiguration
 (assets, prices, execution, signal config,
  rebalance strategy, Markets metadata)
        |
        | deterministic canonical serialization
        v
 portfolio_configuration_hash
        |
        | get/create Portfolio domain object
        v
 PortfolioIndexAsset
 unique_identifier = portfolio_configuration_hash
        |
        v
 PortfolioStrategy.update()
 - interpolate signal weights
 - rebalance into executed weights
 - compute close / return
        |
        +-----------------------------+-----------------------------+
        |                             |                             |
        v                             v                             v
 PortfolioWeights(namespace)     PortfoliosDataNode(namespace)  SignalWeights(namespace)
 static DynamicTableMetaData     static DynamicTableMetaData    static DynamicTableMetaData
        |                             |                             |
        | row identity includes       | row identity includes       | row identity includes
        | portfolio_index_asset_      | portfolio_index_asset_      | signal_uid =
        | unique_identifier =         | unique_identifier =         | hash(signal config)
        | portfolio_configuration_hash| portfolio_configuration_hash|
        v                             v                             v
 one table for all portfolio     one table for all portfolio    one table for all signal
 weights in the namespace        value series in namespace      weights in namespace
```

Storage produced by the new workflow:

```text
N portfolio configs in one namespace
  -> 1 PortfolioWeights DataNodeStorage
  -> 1 PortfoliosDataNode DataNodeStorage
  -> 1 SignalWeights DataNodeStorage
  -> N PortfolioIndexAsset identities, one per portfolio_configuration_hash
```

During migration, the legacy per-portfolio `PortfolioStrategy` output can remain
available for compatibility, but it should not be the canonical identity model.
The canonical model is simpler: one shared table contract, many portfolio row
identities.

## Decision

Introduce unified VFB DataNodes modeled after `AccountHoldings`.

The first release should add three primary tables:

1. `PortfolioWeights`
2. `SignalWeights`
3. `PortfoliosDataNode`

`PortfoliosDataNode` stores the portfolio value series for all portfolios in
the same namespace. `PortfolioWeights` stores the asset-level allocations for
those portfolios. Keeping both canonical tables in the first implementation
lets VFB move the portfolio output away from one physical DataNode table per
portfolio without waiting for a later migration.

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
column_dtypes_map:
  time_index: datetime64[ns, UTC]
  portfolio_index_asset_unique_identifier: string
  unique_identifier: string
  weight: float64
  weight_before: float64
  price_current: float64
  price_before: float64
  volume_current: float64
  volume_before: float64
```

Required canonical columns:

```text
weight
weight_before
price_current
price_before
volume_current
volume_before
```

Rationale:

- `weight` maps to VFB `weights_current`, the executed/current allocation
- `weight_before` maps to VFB `weights_before`, the allocation before execution
- `price_current`, `price_before`, `volume_current`, and `volume_before`
  represent the actual rebalance execution result and are required for
  reconstructing or auditing portfolio price calculations

Do not include a catch-all JSON metadata column in the first canonical schema.
VFB does not currently write a defined payload for one, and it would make the
canonical contract less precise without solving an active requirement. If future
rebalance metadata becomes necessary, add typed canonical columns for queryable
fields or an explicit schema extension with `extra_records`.

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
weight_before
price_current
price_before
volume_current
volume_before
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
column_dtypes_map:
  time_index: datetime64[ns, UTC]
  signal_uid: string
  unique_identifier: string
  signal_weight: float64
```

`signal_uid` must be computed from the canonical signal configuration hash.
Today signal retrievability comes from hashing a signal configuration into its
own DataNode identity. Moving signal rows into a shared table must preserve that
property by carrying the same logical identity as a row dimension.

The hash input should include:

- the concrete signal class/import identity
- the canonical serialized signal `DataNodeConfiguration`
- fields that change the emitted `signal_weight` series

The hash input should exclude:

- namespace
- storage id
- update id
- run-specific timestamps
- portfolio-specific consumers of the signal
- user-facing display names that do not change signal output

Human-readable names can be stored as metadata, but they must not be the
canonical `signal_uid`. During migration, an existing signal DataNode's hash may
be reused only when it is the same canonical configuration hash. Arbitrary
`DataNodeStorage.identifier` values or mutable update metadata should not become
long-term signal identities.

### PortfoliosDataNode

`PortfoliosDataNode` is required in the first implementation. It stores the
portfolio value and return series for all portfolios in one table per namespace.
The asset-level allocations belong in `PortfolioWeights`; this table owns the
portfolio-level time series.

Recommended contract:

```text
role: portfolios
identifier: mainsequence.markets.portfolios
time_index_name: time_index
index_names:
  - time_index
  - portfolio_index_asset_unique_identifier
column_dtypes_map:
  time_index: datetime64[ns, UTC]
  portfolio_index_asset_unique_identifier: string
  close: float64
  return: float64
  calculated_close: float64
  close_time: datetime64[ns, UTC]
```

The canonical table should not store serialized rebalance JSON as its primary
weights representation. Those rows belong in `PortfolioWeights`. Legacy
serialized columns can remain on per-portfolio output during migration and can
be parsed only for backfill.

## DataNode Class Design

Use the current AccountHoldings configuration pattern.

`AccountHoldings` no longer uses a frozen contract object as the primary public
shape. It uses a `HoldingsDataNodeConfiguration` that carries:

- `time_index_name`
- `index_names`
- `records`
- `node_metadata`

The canonical VFB nodes should use the same config-first pattern, with one
important restriction: `time_index_name` is not a configurable field. It is
always `"time_index"` for these canonical tables. Required schema is defined by
the class, optional `extra_records` are merged through the default config, and
runtime validation checks the active config.

Recommended module:

```text
mainsequence/markets/virtualfundbuilder/data_nodes.py
```

Recommended abstractions:

```python
class VFBCanonicalDataNodeConfiguration(DataNodeConfiguration):
    index_names: list[str]
    records: list[RecordDefinition]

class VFBCanonicalDataNode(DataNode):
    def __init__(
        self,
        config: VFBCanonicalDataNodeConfiguration | None = None,
        *args,
        **kwargs,
    ): ...

    @classmethod
    def default_config(
        cls,
        *,
        identifier: str | None = None,
        description: str | None = None,
        extra_records: list[RecordDefinition] | None = None,
    ) -> VFBCanonicalDataNodeConfiguration: ...

    @classmethod
    def _required_index_names(cls) -> list[str]: ...

    @classmethod
    def _required_records(cls) -> list[RecordDefinition]: ...
```

Concrete classes:

```python
class PortfolioWeights(VFBCanonicalDataNode):
    def _initialize_source_table(...):
        storage.initialize_portfolio_weights_source_table(...)

class SignalWeights(VFBCanonicalDataNode):
    def _initialize_source_table(...):
        storage.initialize_signal_weights_source_table(...)

class PortfoliosDataNode(VFBCanonicalDataNode):
    def _initialize_source_table(...):
        storage.initialize_portfolios_source_table(...)
```

Each class should provide:

- default config with `DataNodeMetaData(identifier=...)`
- `build_schema_bootstrap_frame()`
- `validate_frame()` or a domain-specific validator such as
  `validate_weights_frame()`
- `ensure_storage_ready()`
- `_initialize_source_table_storage_or_none()`
- `_validate_storage_contract()`

The `SignalWeights` integration should also provide `compute_signal_uid()` for
signal rows, implemented from the canonical signal configuration hash.

The default `update()` may return a schema bootstrap frame, like holdings, so the
table can be created through the standard DataNode path when the domain endpoint
is not available.

## Namespace API

The user-facing VFB API should accept `namespace`.

Examples:

```python
portfolio_weights = PortfolioWeights(namespace="research")
signal_weights = SignalWeights(namespace="research")
portfolios = PortfoliosDataNode(namespace="research")
```

and:

```python
portfolio_node.run(
    add_portfolio_to_markets_backend=True,
    sync_canonical_tables=True,
    namespace="research",
)
```

Resolved implementation rule:

- `namespace` is the VFB-facing alias for existing DataNode
  `hash_namespace`/storage namespace plumbing
- no separate market-domain namespace identity is added
- namespace is not included as an output column
- namespace is not a portfolio identity
- namespace is not a signal identity
- all canonical VFB weight tables created with the same namespace share the same
  storage family for that namespace

VFB should expose `namespace` to avoid leaking the lower-level `hash_namespace`
name into market-domain code, but the storage identity effect is the same
existing DataNode namespace mechanism.

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
- `PortfoliosDataNode`: VFB portfolio value series indexed by portfolio

Portfolio uniqueness must move out of the canonical table DataNode identity and
into the portfolio row identity. The SDK must compute a deterministic
`portfolio_configuration_hash` from the full canonical `PortfolioConfiguration`
and use that hash to get or create the corresponding `PortfolioIndexAsset`.
Canonical portfolio rows then use:

```text
portfolio_index_asset_unique_identifier =
  PortfolioIndexAsset.unique_identifier =
  portfolio_configuration_hash
```

The hash input must include the fields that change the emitted portfolio weights
or value series, including the asset universe, price configuration, execution
configuration, signal identity/config, rebalance strategy, and relevant Markets
portfolio metadata. It must exclude namespace, storage id, update id, and
run-specific timestamps.

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
- write canonical portfolio value rows into `PortfoliosDataNode` from the
  calculated `portfolio` frame
- use persisted `rebalance_weights` JSON only for backfill

Recommended later behavior:

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
- every signal must expose its canonical configuration hash as `signal_uid`
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
initialize_portfolios_source_table(...)
```

Recommended backend endpoints:

```text
POST /orm/api/assets/portfolio-weights-data-node/{id}/initialize-source-table/
POST /orm/api/assets/signal-weights-data-node/{id}/initialize-source-table/
POST /orm/api/assets/portfolios-data-node/{id}/initialize-source-table/
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

### Portfolio value series for one portfolio

```python
df = portfolios.get_df_between_dates(
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
        "signal_uid": [signal_configuration_hash],
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
- Portfolio value series can be queried across portfolios without discovering
  many portfolio-specific tables.
- Signal weights can be queried across signal producers.
- The design uses ADR 0002's multidimensional update contract directly.
- `unique_identifier` keeps its existing meaning: the held or signaled asset.
- Namespace gives users isolated table families without changing row shape.
- This aligns VFB with the AccountHoldings storage model.

### Negative

- The first implementation needs domain-specific table initialization endpoints.
- Existing VFB signal strategies need a migration path.
- `PortfolioStrategy` still needs to keep the legacy per-portfolio output during
  migration even after canonical `PortfoliosDataNode` coverage exists.
- Stable signal identity is not currently a first-class VFB concept and must be
  added.

### Compatibility

Existing per-portfolio VFB DataNodes should remain readable during migration.

Existing columns such as `rebalance_weights` and `weights_at_last_rebalance`
should not be removed in the first release.

Existing custom signal DataNodes should continue to work through the mirror path
until they opt into direct `SignalWeights` storage.

## Implementation Tasks

### Phase 1: Config-first contracts

- [x] Add `VFBCanonicalDataNodeConfiguration(DataNodeConfiguration)` with
      `index_names` and `records`. Do not expose `time_index_name` as a config
      field; canonical VFB tables always use `"time_index"`.
- [x] Add a `VFBCanonicalDataNode` base class that mirrors the current
      `HoldingsDataNode` lifecycle: `default_config()`, `extra_records` merge,
      `_validate_config()`, schema bootstrap frame, storage readiness, and
      storage contract validation.
- [x] Define required record maps, labels, and descriptions for
      `PortfolioWeights`.
- [x] Define required record maps, labels, and descriptions for `SignalWeights`.
- [x] Define required record maps, labels, and descriptions for
      `PortfoliosDataNode`.
- [x] Attach logical dtype metadata to bootstrap and validation frames.
- [x] Add validators that enforce exact `index_names`, required columns, dtypes,
      fixed `"time_index"` as the first index, and duplicate-free full index
      tuples.

### Phase 2: Source table initialization

- [x] Add `DataNodeStorage.initialize_portfolio_weights_source_table(...)`.
- [x] Add `DataNodeStorage.initialize_signal_weights_source_table(...)`.
- [x] Add `DataNodeStorage.initialize_portfolios_source_table(...)`.
- [x] Target the assumed backend portfolio-domain endpoints and lookup indexes
      for portfolio and signal weight tables and the canonical portfolios
      table.
- [x] Match AccountHoldings fallback behavior: domain initializer first,
      bootstrap `run(force_update=True)` second.
- [x] Validate initialized source-table contracts against the active
      `VFBCanonicalDataNodeConfiguration`, including any `extra_records`.

### Phase 3: Namespace plumbing

- [x] Add `namespace` to VFB-facing constructors/helpers.
- [x] Internally map `namespace` to DataNode namespace/hash namespace plumbing.
- [x] Ensure `PortfolioWeights(namespace=X)`, `SignalWeights(namespace=X)`,
      and `PortfoliosDataNode(namespace=X)` use stable table contracts inside
      the same namespace.
- [x] Ensure namespace never becomes a row column or part of
      `portfolio_index_asset_unique_identifier`.
- [x] Add tests that the same table contract with different namespaces produces
      isolated storage.

### Phase 4: Signal identity hashing

- [ ] Add a canonical signal configuration serialization helper.
- [ ] Compute `signal_uid` as the deterministic hash of the canonical signal
      configuration.
- [ ] Include the concrete signal class/import identity in the hash input.
- [ ] Include normalized config fields that affect emitted `signal_weight`
      values in the hash input.
- [ ] Exclude namespace, storage id, update id, run timestamps, portfolio
      consumers, and display-only names from the hash input.
- [ ] Reuse the existing DataNode configuration hashing semantics where they
      already represent this identity.
- [ ] Add tests proving identical signal configs produce identical
      `signal_uid` values.
- [ ] Add tests proving output-changing config changes produce different
      `signal_uid` values.
- [ ] Add tests proving namespace changes do not change `signal_uid`.

### Phase 5: PortfolioWeights sync

- [ ] Add a canonical portfolio configuration serialization helper.
- [ ] Compute `portfolio_configuration_hash` from the full canonical
      `PortfolioConfiguration`.
- [ ] Use `portfolio_configuration_hash` to get or create the backend
      `Portfolio` and `PortfolioIndexAsset`.
- [ ] Store the resulting `PortfolioIndexAsset.unique_identifier` as
      `portfolio_index_asset_unique_identifier` in canonical portfolio rows.
- [ ] Add a normalizer from postprocessed VFB `weights` frames to
      `PortfolioWeights` rows.
- [ ] Add a normalizer from persisted `rebalance_weights` JSON for backfill.
- [ ] Add opt-in `sync_canonical_tables=True` to `PortfolioStrategy.run(...)` or an
      adjacent VFB orchestration helper.
- [ ] Validate asset `unique_identifier` values before writing canonical rows.
- [ ] Validate that two portfolios holding the same asset at the same timestamp
      can write to the same table.

### Phase 6: SignalWeights sync

- [ ] Add a mirror path from existing signal DataNodes into `SignalWeights`.
- [ ] Populate every mirrored row with the computed `signal_uid`.
- [ ] Update `WeightsBase.interpolate_index()` to optionally read from
      `SignalWeights` with `dimension_filters={"signal_uid": [...]}`.
- [ ] Migrate built-in signals one by one to direct shared-table storage.
- [ ] Keep the mirror path as a compatibility bridge for custom signal
      DataNodes.

### Phase 7: PortfoliosDataNode sync

- [ ] Add the required `PortfoliosDataNode` config-first DataNode contract.
- [ ] Write `close`, `return`, and value metadata indexed by
      `portfolio_index_asset_unique_identifier`.
- [ ] Add a normalizer from the VFB calculated `portfolio` frame to
      `PortfoliosDataNode` rows.
- [ ] Ensure `PortfoliosDataNode` sync uses the same namespace and
      `portfolio_index_asset_unique_identifier` as `PortfolioWeights`.
- [ ] Update VFB docs to describe which table owns values and which table owns
      weights.
- [ ] De-emphasize per-portfolio JSON output after canonical coverage is
      complete.

### Phase 8: Backfill

- [ ] List existing Markets `Portfolio` records with `data_node_update`.
- [ ] Resolve each `PortfolioIndexAsset`.
- [ ] Parse existing portfolio output JSON into `PortfolioWeights`.
- [ ] Parse existing portfolio value rows into `PortfoliosDataNode`.
- [ ] List existing signal DataNode storages where possible.
- [ ] Derive `signal_uid` from each signal's canonical configuration hash.
- [ ] Mirror signal rows into `SignalWeights`.
- [ ] Run backfill per namespace, preserving source table namespace.
- [ ] Report missing identities and skipped rows explicitly.

### Phase 9: Tests

- [x] Contract validation for `PortfolioWeights`.
- [x] Contract validation for `SignalWeights`.
- [x] Contract validation for `PortfoliosDataNode`.
- [x] Namespace isolation tests.
- [ ] Signal configuration hash identity tests.
- [x] Full-index duplicate detection tests.
- [ ] Dimension-filter query payload tests.
- [ ] Incremental update tests using nested `index_progress`.
- [ ] PortfolioStrategy sync tests.
- [ ] Signal mirror tests.
- [ ] Backfill JSON parsing tests.

## Open Questions

1. Should canonical writes happen inside `PortfolioStrategy.run(...)`, or should
   an orchestration object own the sync after portfolio construction?

## Acceptance Criteria

The new architecture is complete when:

1. All portfolios in the same namespace can write to one `PortfolioWeights`
   DataNodeStorage.
2. All portfolio value series in the same namespace can write to one
   `PortfoliosDataNode` DataNodeStorage.
3. All signals in the same namespace can write to one `SignalWeights`
   DataNodeStorage.
4. `unique_identifier` always means the held/signaled asset.
5. Portfolio identity is represented by
   `portfolio_index_asset_unique_identifier`.
6. Signal identity is represented by `signal_uid`, computed as the deterministic
   hash of the canonical signal configuration.
7. The user can pass `namespace` to select an isolated shared table family.
8. The tables use ADR 0002 dimension filters and nested index progress.
9. Existing VFB per-portfolio tables remain compatible during migration.
