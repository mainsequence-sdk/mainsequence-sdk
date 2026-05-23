# ADR 0005: Unified Portfolio Weight DataNodes

Date: 2026-05-21

Status: Accepted

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

The direct AccountHoldings initializer is a one-table pattern:

```text
AccountHoldings DataNodeStorage
        |
        | POST /orm/api/assets/account-holdings-data-node/{uid}/initialize-source-table/
        v
one holdings source table + holdings lookup indexes
```

Portfolios should not copy that as three unrelated per-table calls. The canonical Portfolios
storage surface is a table family: `PortfolioWeights`, `SignalWeights`, and
`PortfoliosDataNode` are initialized together for the same namespace. The
normal DataNode creation path must still create the three
`DataNodeStorage`/`DynamicTableMetaData` records first; the portfolio-domain
endpoint only initializes their source tables and lookup indexes.

### Portfolios still stores too much by portfolio-specific DataNode identity

`PortfoliosDataNode` is still a `DataNode`. Its configuration contains the whole
portfolio build:

- asset universe
- price configuration
- execution configuration
- signal instance
- rebalance strategy
- Markets metadata

Those fields are portfolio-specific. If they remain part of the storage identity,
Portfolios will keep creating one physical output table per portfolio.

That behavior is the legacy storage path being replaced. In the
multidimensional contract, the portfolio-specific parts should be writer/update
scope and lineage. The table meaning is the stable schema:

- "portfolio weights over time"
- "signal weights over time"
- "portfolio values over time"

### Portfolios already has the right in-memory frames

The live portfolio path should not parse JSON when it can avoid it.

`PortfoliosDataNode.update()` already computes:

- interpolated signal weights
- rebalance output
- postprocessed long-form executed weights
- portfolio `close` and `return`

The best source for canonical portfolio weights is the post-rebalance long-form
`weights` frame. New Portfolios DataNodes should return this canonical shape directly
instead of serializing weights into a per-portfolio JSON output table.

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

The previous per-signal DataNode also gave Portfolios a place to keep human-facing
signal metadata such as a signal description. Canonical signal weights should
not make that description part of `SignalWeights`, because it is not part of the
time-series observation and should not affect `signal_uid`. The new workflow
therefore needs a small `Signals` `SimpleTable` registry keyed by `signal_uid` so
descriptions remain queryable even when the signal producer itself is no longer
persisted as a standalone DataNode.

The same applies to rebalance strategies. Rebalance strategy configuration is
part of portfolio construction identity, but human-facing rebalance descriptions
are metadata. They should not be stored in canonical weight rows and should not
affect portfolio table identity. The canonical workflow needs a small
`RebalanceStrategies` `SimpleTable` registry keyed by deterministic
`rebalance_strategy_uid`.

Portfolio descriptions follow the same rule. The portfolio identity in
canonical rows is the `PortfolioIndexAsset.unique_identifier` created from the
full portfolio configuration hash. Human-facing descriptions should live in a
small `Portfolios` metadata `SimpleTable` keyed by `unique_identifier`, where
`unique_identifier` means that `PortfolioIndexAsset.unique_identifier`, not a
held asset.

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

The SDK already has `hash_namespace`. The Portfolios public API should expose a clearer
domain parameter, likely named `namespace`, and map it to DataNode namespace
plumbing internally.

## Workflow Comparison

### Old workflow: portfolio identity is the runtime DataNode identity

Previously the `PortfolioStrategy` DataNode identity did two jobs at once:

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
 PortfoliosDataNode.update()
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
                                                                  |
                                                                  | upsert metadata by signal_uid
                                                                  v
                                                         Signals
                                                         unique key = signal_uid
                                                         stores signal_description
```

Storage produced by the new workflow:

```text
N portfolio configs in one namespace
  -> 1 PortfolioWeights DataNodeStorage
  -> 1 PortfoliosDataNode DataNodeStorage
  -> 1 SignalWeights DataNodeStorage
  -> 1 Signals SimpleTable metadata registry
  -> 1 RebalanceStrategies SimpleTable metadata registry
  -> 1 Portfolios SimpleTable metadata registry
  -> N PortfolioIndexAsset identities, one per portfolio_configuration_hash
```

The legacy per-portfolio `PortfoliosDataNode` output is not part of the new
runtime workflow. The canonical model is simpler: one shared table contract,
many portfolio row identities.

## Decision

Introduce unified Portfolios DataNodes modeled after `AccountHoldings`.

The first release should add three primary time-series tables:

1. `PortfolioWeights`
2. `SignalWeights`
3. `PortfoliosDataNode`

`PortfoliosDataNode` stores the portfolio value series for all portfolios in
the same namespace. `PortfolioWeights` stores the asset-level allocations for
those portfolios. Keeping both canonical tables in the first implementation
lets Portfolios move the portfolio output away from one physical DataNode table per
portfolio immediately.

The first release should also add a `Signals` `SimpleTable` metadata registry
keyed by `signal_uid`. This table is not a time-series DataNode and is not part
of the namespace identity. It preserves human-facing signal descriptions while
`SignalWeights` owns only timestamped weight observations.

The first release should also add a `RebalanceStrategies` `SimpleTable`
metadata registry keyed by `rebalance_strategy_uid`. This table is not a
time-series DataNode and is not part of namespace identity. It preserves
human-facing rebalance strategy descriptions while portfolio weights and values
stay canonical.

The first release should also add a `Portfolios` `SimpleTable` metadata
registry keyed by `PortfolioIndexAsset.unique_identifier` through the
`unique_identifier` column. This table replaces portfolio-description storage
that previously lived in `PortfolioAbout`/`target_portfolio_about`.

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

- `weight` maps to Portfolios `weights_current`, the executed/current allocation
- `weight_before` maps to Portfolios `weights_before`, the allocation before execution
- `price_current`, `price_before`, `volume_current`, and `volume_before`
  represent the actual rebalance execution result and are required for
  reconstructing or auditing portfolio price calculations

Do not include a catch-all JSON metadata column in the first canonical schema.
Portfolios does not currently write a defined payload for one, and it would make the
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

- store signal weights from all Portfolios signals in one table per namespace
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
canonical `signal_uid`. An existing signal DataNode's hash may be reused only
when it is the same canonical configuration hash. Arbitrary
`DataNodeStorage.identifier` values or mutable update metadata should not become
long-term signal identities.

The SDK implementation must use the existing TDAG serialization and hashing
machinery (`Serializer.serialize_init_kwargs(...)` and `hash_signature(...)`) for
this computation. Portfolios should only filter out non-identity fields such as
namespace, backend ids, portfolio consumers, and display-only metadata before
delegating to TDAG hashing.

### Signals SimpleTable

`Signals` is a small `SimpleTable` metadata registry keyed by `signal_uid`.

Purpose:

- preserve the human-facing signal description that used to live on the
  persisted signal DataNode
- make signal metadata retrievable when weights are written into shared
  `SignalWeights` storage
- keep descriptions out of `SignalWeights` so weight observations stay canonical

Recommended contract:

```python
from typing import Annotated

from mainsequence.tdag.simple_tables import Index, SimpleTable


class SignalMetadata(SimpleTable):
    signal_uid: Annotated[str, Index(unique=True)]
    signal_description: str | None = None
```

`signal_uid` remains the deterministic TDAG hash of the canonical signal
configuration. `signal_description` is mutable metadata and must not participate
in the hash. Portfolios should upsert this row whenever a signal participates in a
canonical workflow and a description is available. `namespace` must not be part
of the unique key for this registry. This must use the existing
`SimpleTableUpdater` and `SimpleTablePersistManager` machinery, not a new
DataNode or ad hoc storage path.

### RebalanceStrategies SimpleTable

`RebalanceStrategies` is a small `SimpleTable` metadata registry keyed by
`rebalance_strategy_uid`.

Purpose:

- preserve human-facing rebalance strategy descriptions
- keep rebalance metadata queryable without storing it in canonical weight rows
- keep descriptions out of portfolio and weight table identities

Recommended contract:

```python
from typing import Annotated

from mainsequence.tdag.simple_tables import Index, SimpleTable


class RebalanceStrategyMetadata(SimpleTable):
    rebalance_strategy_uid: Annotated[str, Index(unique=True)]
    rebalance_strategy_description: str | None = None
```

`rebalance_strategy_uid` is the deterministic TDAG hash of the canonical
rebalance strategy configuration and concrete strategy class/import identity.
`rebalance_strategy_description` is mutable metadata and must not participate in
the hash. Portfolios should upsert this row whenever a rebalance strategy participates
in a canonical workflow and a description is available. `namespace` must not be
part of the unique key for this registry. This must use the existing
`SimpleTableUpdater` and `SimpleTablePersistManager` machinery, not a new
DataNode or ad hoc storage path.

### Portfolios SimpleTable metadata

`Portfolios` is a small `SimpleTable` metadata registry keyed by
`unique_identifier`.

Purpose:

- preserve the human-facing portfolio description
- make portfolio metadata retrievable by `PortfolioIndexAsset.unique_identifier`
- keep descriptions out of `PortfolioWeights` and `PortfoliosDataNode` rows
- provide the replacement storage path for the legacy `PortfolioAbout` model

Recommended contract:

```python
from typing import Annotated

from mainsequence.tdag.simple_tables import Index, SimpleTable


class PortfolioMetadata(SimpleTable):
    unique_identifier: Annotated[str, Index(unique=True)]
    description: str | None = None
```

In this table, `unique_identifier` points to the
`PortfolioIndexAsset.unique_identifier` created by the canonical portfolio
configuration hash workflow. It is not the held asset identifier used inside
`PortfolioWeights`. `description` is mutable UI metadata and must not
participate in the portfolio configuration hash or canonical DataNode table
identity. Portfolios should upsert this row whenever a canonical portfolio workflow has
a resolved `PortfolioIndexAsset.unique_identifier` and a description from
`PortfolioConfiguration.portfolio_markets_configuration.front_end_details`.

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

The canonical table should not store serialized rebalance JSON. Those rows
belong in `PortfolioWeights`, and the new runtime should not produce a
per-portfolio JSON DataNode output.

## DataNode Class Design

Use the current AccountHoldings configuration pattern.

`AccountHoldings` no longer uses a frozen contract object as the primary public
shape. It uses a `HoldingsDataNodeConfiguration` that carries:

- `time_index_name`
- `index_names`
- `records`
- `node_metadata`

The canonical Portfolios nodes should use the same config-first pattern, with one
important restriction: `time_index_name` is not a configurable field. It is
always `"time_index"` for these canonical tables. Required schema is defined by
the class, optional `extra_records` are merged through the default config, and
runtime validation checks the active config.

Recommended package:

```text
mainsequence/markets/portfolios/
  data_nodes/
    base.py
    portfolio_weights.py
    signal_weights.py
    portfolios.py
  simple_tables/
    signal_metadata.py
    rebalance_metadata.py
    portfolio_metadata.py
```

This package name is now a historical mismatch. After the canonical table
refactor, this code is no longer mainly a "virtual fund builder"; it is the
portfolio model surface for portfolio identities, weights, values, signals, and
rebalance strategy metadata. The follow-up cleanup should move the package to a
model-oriented namespace:

```text
mainsequence/markets/portfolios/
  models.py
  enums.py
  data_nodes/
    base.py
    portfolio_weights.py
    signal_weights.py
    portfolios.py
    storage_initialization.py
  simple_tables/
    signal_metadata.py
    rebalance_metadata.py
    portfolio_metadata.py
  rebalance_strategy/
    base.py
    immediate_signal.py
    time_weighted.py
    volume_participation.py
  utils/
    helpers.py
    prices/
    signals/
```

`contrib` should not survive the rename. The existing signal and price helper
modules are first-party reusable portfolio components, not external
contribution examples. Signals and price helpers should move under
`mainsequence.markets.portfolios.utils`. Rebalance strategy base and built-in
strategies should live under `mainsequence.markets.portfolios.rebalance_strategy`.
Because the current package already has a `utils.py`, the implementation must
first convert that file into a `utils/` package, for example
`utils/helpers.py`, before moving the remaining `contrib` children underneath it.

The public package should stay oriented around portfolio models and contracts:
configuration models, canonical DataNodes, SimpleTable metadata models,
portfolio strategy runtime, and resource factories. Utility modules should not
own model identity, schema contracts, or canonical storage decisions.

Recommended abstractions:

```python
class PortfolioCanonicalDataNodeConfiguration(DataNodeConfiguration):
    index_names: list[str]
    records: list[RecordDefinition]

class PortfolioCanonicalDataNode(DataNode):
    def __init__(
        self,
        config: PortfolioCanonicalDataNodeConfiguration | None = None,
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
    ) -> PortfolioCanonicalDataNodeConfiguration: ...

    @classmethod
    def _required_index_names(cls) -> list[str]: ...

    @classmethod
    def _required_records(cls) -> list[RecordDefinition]: ...
```

Concrete classes:

```python
class PortfolioWeights(PortfolioCanonicalDataNode):
    def update(self) -> pd.DataFrame:
        return self.validate_frame(self._calculate_weights())

    def _calculate_weights(self) -> pd.DataFrame:
        ...

class SignalWeights(PortfolioCanonicalDataNode):
    def update(self) -> pd.DataFrame:
        return self.validate_frame(self._calculate_signal_weights())

    def _calculate_signal_weights(self) -> pd.DataFrame:
        ...

class PortfoliosDataNode(PortfolioCanonicalDataNode):
    def update(self) -> pd.DataFrame:
        return self.validate_frame(self._calculate_portfolio_values())

    def _calculate_portfolio_values(self) -> pd.DataFrame:
        ...
```

Each class should provide:

- default config with `DataNodeMetaData(identifier=...)`
- `build_schema_bootstrap_frame()`
- `validate_frame()` or a domain-specific validator such as
  `validate_weights_frame()`
- `ensure_storage_ready()`
- `_validate_storage_contract()`

The `SignalWeights` integration should also provide `compute_signal_uid()` for
signal rows, implemented from the canonical signal configuration hash.

Source-table initialization is owned by the Portfolios storage-family initializer, not
by three concrete DataNode-specific initializer methods. A single canonical Portfolios
write setup should resolve the three canonical nodes for the namespace, ensure
their `DataNodeStorage`/`DynamicTableMetaData` UIDs exist through the normal
DataNode creation path, and then call the portfolio-domain bulk initializer
with the three schema contracts.

The base class may keep a schema-bootstrap frame for storage initialization only.
Concrete Portfolios runtime nodes must implement `update()` as the canonical write path:
`update()` calls the protected calculation method, validates the canonical frame,
and returns rows for the shared table. Subclasses extend Portfolios behavior by
overriding `_calculate_weights()`, `_calculate_signal_weights()`, or
`_calculate_portfolio_values()`, not by creating another per-portfolio output
DataNode.

## Namespace API

The user-facing Portfolios API should accept `namespace`.

Examples:

```python
portfolio_weights = PortfolioWeights(namespace="research")
signal_weights = SignalWeights(namespace="research")
portfolios = PortfoliosDataNode(namespace="research")
```

Resolved implementation rule:

- `namespace` is the Portfolios-facing alias for existing DataNode
  `hash_namespace`/storage namespace plumbing
- no separate market-domain namespace identity is added
- namespace is not included as an output column
- namespace is not a portfolio identity
- namespace is not a signal identity
- all canonical Portfolios weight tables created with the same namespace share the same
  storage family for that namespace

Portfolios should expose `namespace` to avoid leaking the lower-level `hash_namespace`
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

- `PortfolioWeights`: Portfolios executed weights indexed by portfolio and asset
- `SignalWeights`: Portfolios signal weights indexed by signal and asset
- `PortfoliosDataNode`: Portfolios portfolio value series indexed by portfolio
- `Signals`: Portfolios signal metadata indexed by `signal_uid`

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

The implementation must reuse the existing TDAG configuration serialization and
hashing machinery (`Serializer.serialize_init_kwargs(...)` and
`hash_signature(...)`) for this hash. Portfolios must not introduce a new portfolio
configuration serializer; any Portfolios helper should only assemble the correct hash
payload and delegate serialization/hashing to TDAG.

If an implementation needs writer-specific configuration, mark it
`update_only` or keep it outside the canonical table DataNode constructor.

## Portfolios Integration

### Canonical PortfolioWeights update path

`PortfolioWeights.update()` is the primary portfolio-weight write path. It must
not call a legacy per-portfolio output DataNode and then mirror that output.
Instead, `update()` calls `_calculate_weights()`, validates the returned frame,
and returns canonical rows directly:

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

The portfolio identity comes from the `PortfolioIndexAsset`, whose
`unique_identifier` is the deterministic `portfolio_configuration_hash`.
`_calculate_weights()` is the extension point for portfolio construction logic.
The default implementation can reuse extracted Portfolios calculation helpers, but it
must return the canonical long-form schema above.

### Canonical PortfoliosDataNode update path

`PortfoliosDataNode.update()` is the primary portfolio-value write path. It
calls `_calculate_portfolio_values()`, validates the returned frame, and returns
canonical rows directly:

```text
time_index
portfolio_index_asset_unique_identifier
close
return
calculated_close
close_time
```

Portfolio weight and value calculations should share a calculation object or
helper so the same portfolio execution does not need to be recomputed
inconsistently. The shared helper is internal orchestration, not a legacy output
DataNode.

### Canonical SignalWeights update path

`SignalWeights.update()` is the primary signal-weight write path. It calls
`_calculate_signal_weights()`, validates the returned frame, and returns
canonical rows directly:

```text
time_index
signal_uid
unique_identifier
signal_weight
```

The signal identity comes from `compute_signal_uid()`, which hashes the canonical
signal configuration with TDAG hashing. `SignalWeights.update()` should also
upsert the `Signals` `SimpleTable` metadata row when a signal description is
available.

Built-in signal implementations should move to this direct canonical path.
Custom extensions should subclass or compose the new canonical signal calculation
hook instead of relying on a per-signal DataNode plus mirror step.

## Backend/API Additions

Use the AccountHoldings source-table initializer pattern, but lift it to the
portfolio storage family.

AccountHoldings initializes one existing DataNodeStorage because it owns one
domain table. Canonical Portfolios owns three coordinated domain tables in the same
namespace, so the SDK should initialize them in one portfolio-domain call after
the normal DataNode creation path has produced the three storage UIDs.

Required SDK helper:

```python
initialize_portfolio_storage_source_tables(
    *,
    portfolio_weights: PortfolioWeights,
    signal_weights: SignalWeights,
    portfolio_data: PortfoliosDataNode,
    timeout: int | None = None,
) -> dict
```

Required metadata SDK helpers live under
`mainsequence.markets.portfolios.simple_tables` and are implemented on
top of `SimpleTable` persistence and query APIs:

```python
upsert_signal_metadata(...)
get_signal_metadata(...)
upsert_rebalance_strategy_metadata(...)
get_rebalance_strategy_metadata(...)
upsert_portfolio_metadata(...)
get_portfolio_metadata(...)
```

Required backend endpoint:

```text
POST /orm/api/assets/portfolio-storage-data-nodes/initialize-source-tables/
```

Payload shape:

```json
{
  "portfolio_weights": {
    "dynamic_table_metadata_uid": "11111111-1111-4111-8111-111111111111",
    "time_index_name": "time_index",
    "index_names": [
      "time_index",
      "portfolio_index_asset_unique_identifier",
      "unique_identifier"
    ],
    "column_dtypes_map": {
      "time_index": "datetime64[ns, UTC]",
      "portfolio_index_asset_unique_identifier": "string",
      "unique_identifier": "string",
      "weight": "float64",
      "weight_before": "float64",
      "price_current": "float64",
      "price_before": "float64",
      "volume_current": "float64",
      "volume_before": "float64"
    }
  },
  "signal_weights": {
    "dynamic_table_metadata_uid": "22222222-2222-4222-8222-222222222222",
    "time_index_name": "time_index",
    "index_names": ["time_index", "signal_uid", "unique_identifier"],
    "column_dtypes_map": {
      "time_index": "datetime64[ns, UTC]",
      "signal_uid": "string",
      "unique_identifier": "string",
      "signal_weight": "float64"
    }
  },
  "portfolio_data": {
    "dynamic_table_metadata_uid": "33333333-3333-4333-8333-333333333333",
    "time_index_name": "time_index",
    "index_names": [
      "time_index",
      "portfolio_index_asset_unique_identifier"
    ],
    "column_dtypes_map": {
      "time_index": "datetime64[ns, UTC]",
      "portfolio_index_asset_unique_identifier": "string",
      "close": "float64",
      "return": "float64",
      "calculated_close": "float64",
      "close_time": "datetime64[ns, UTC]"
    }
  }
}
```

The three `dynamic_table_metadata_uid` values must already exist. They are the
public UIDs of the `DataNodeStorage`/`DynamicTableMetaData` records created by
the normal DataNode creation path for `PortfolioWeights`, `SignalWeights`, and
`PortfoliosDataNode`. This endpoint must not create DataNodes, must not insert
observations, and must not create bootstrap rows. It only initializes or
validates the three source tables and portfolio-domain lookup indexes.

Before the POST, the SDK must derive each object in the payload from the active
canonical DataNode configuration. Any `extra_records` included in that
configuration are folded into `column_dtypes_map`; no schema information should
be inferred from runtime data frames.

The backend should derive:

- identity dimensions from `index_names[1:]`
- uniqueness over full `index_names`
- lookup indexes for portfolio/signal and asset dimensions
- update progress by full identity coordinate

After the POST, the SDK must refresh or inspect the three storage objects and
validate their source-table contracts against the active
`PortfolioCanonicalDataNodeConfiguration`, including `extra_records`.

The `Signals` registry does not need a `DataNodeStorage` initializer or a custom
dynamic-table source endpoint. It should use the existing `SimpleTable`
machinery. The backend must enforce the `Index(unique=True)` constraint on
`signal_uid`; description updates should be upserts, not new signal identities.

The `RebalanceStrategies` registry follows the same rule. It does not need a
`DataNodeStorage` initializer or a custom dynamic-table source endpoint. It
should use the existing `SimpleTable` machinery, enforce a unique
`rebalance_strategy_uid`, and treat description updates as upserts.

The `Portfolios` metadata registry also follows the same rule. It should use
the existing `SimpleTable` machinery, enforce a unique `unique_identifier`, and
treat description updates as upserts keyed by
`PortfolioIndexAsset.unique_identifier`.

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

### Signal description

```python
signal = get_signal_metadata(signal_uid=signal_configuration_hash)
description = signal.signal_description
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
- This aligns Portfolios with the AccountHoldings storage model.

### Negative

- The first implementation needs a portfolio-domain bulk source-table
  initialization endpoint.
- Existing Portfolios portfolio and signal strategies need to be refactored onto the
  canonical update hooks.
- Stable signal identity is not currently a first-class Portfolios concept and must be
  added.
- This is a breaking storage architecture change for Portfolios runtime output.

### Compatibility Boundary

The new runtime does not preserve the per-portfolio Portfolios output DataNode as a
parallel compatibility path. If old persisted tables need import, that is a
separate offline operation, not part of the canonical Portfolios runtime. Runtime
writes should go directly through `PortfolioWeights`, `PortfoliosDataNode`,
`SignalWeights`, and `Signals`.

## Implementation Tasks

### Phase 1: Config-first contracts

- [x] Add `PortfolioCanonicalDataNodeConfiguration(DataNodeConfiguration)` with
      `index_names` and `records`. Do not expose `time_index_name` as a config
      field; canonical Portfolios tables always use `"time_index"`.
- [x] Add a `PortfolioCanonicalDataNode` base class that mirrors the current
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

- [x] Replace the three Portfolios per-table initializer calls with one SDK helper for
      `POST /orm/api/assets/portfolio-storage-data-nodes/initialize-source-tables/`.
- [x] Resolve or create the `PortfolioWeights`, `SignalWeights`, and
      `PortfoliosDataNode` `DataNodeStorage`/`DynamicTableMetaData` rows through
      the normal DataNode creation path before calling the portfolio-domain
      initializer.
- [x] Build the `portfolio_weights`, `signal_weights`, and `portfolio_data`
      payload entries from the active canonical configurations.
- [x] Send `dynamic_table_metadata_uid`, fixed `time_index_name`, exact
      `index_names`, and `column_dtypes_map` for all three canonical tables in
      the same POST body.
- [x] Include `extra_records` in each `column_dtypes_map` before sending the
      payload.
- [x] Ensure the endpoint is treated as source-table initialization only: it
      must not create DataNodes, insert observations, or write bootstrap rows.
- [x] Validate all three initialized source-table contracts against the active
      `PortfolioCanonicalDataNodeConfiguration`, including any `extra_records`.
- [x] Remove the Portfolios target architecture dependency on
      `initialize_portfolio_weights_source_table(...)`,
      `initialize_signal_weights_source_table(...)`, and
      `initialize_portfolios_source_table(...)`.

### Phase 3: Namespace plumbing

- [x] Add `namespace` to Portfolios-facing constructors/helpers.
- [x] Internally map `namespace` to DataNode namespace/hash namespace plumbing.
- [x] Ensure `PortfolioWeights(namespace=X)`, `SignalWeights(namespace=X)`,
      and `PortfoliosDataNode(namespace=X)` use stable table contracts inside
      the same namespace.
- [x] Ensure namespace never becomes a row column or part of
      `portfolio_index_asset_unique_identifier`.
- [x] Add tests that the same table contract with different namespaces produces
      isolated storage.

### Phase 4: Signal identity hashing

- [x] Add a canonical signal configuration serialization helper.
- [x] Compute `signal_uid` as the deterministic hash of the canonical signal
      configuration.
- [x] Include the concrete signal class/import identity in the hash input.
- [x] Include normalized config fields that affect emitted `signal_weight`
      values in the hash input.
- [x] Exclude namespace, storage id, update id, run timestamps, portfolio
      consumers, and display-only names from the hash input.
- [x] Reuse the existing DataNode configuration hashing semantics where they
      already represent this identity.
- [x] Add tests proving identical signal configs produce identical
      `signal_uid` values.
- [x] Add tests proving output-changing config changes produce different
      `signal_uid` values.
- [x] Add tests proving namespace changes do not change `signal_uid`.

### Phase 4b: Signal metadata registry

- [x] Add a `Signals` `SimpleTable` metadata registry with unique
      `signal_uid`.
- [x] Store `signal_description` as nullable string metadata.
- [x] Ensure `signal_description` is excluded from `signal_uid` hashing.
- [x] Add SDK upsert and lookup helpers for `Signals` backed by existing
      `SimpleTable` persistence/query machinery.
- [x] Upsert signal metadata when Portfolios computes a `signal_uid` for canonical
      `SignalWeights`.
- [x] Add tests proving description changes update metadata without changing
      `signal_uid`.
- [x] Move signal metadata SimpleTable code into
      `mainsequence.markets.portfolios.simple_tables`.

### Phase 4c: Rebalance strategy metadata registry

- [x] Add a `RebalanceStrategies` `SimpleTable` metadata registry with unique
      `rebalance_strategy_uid`.
- [x] Store `rebalance_strategy_description` as nullable string metadata.
- [x] Compute `rebalance_strategy_uid` from the concrete strategy class/import
      identity and canonical strategy configuration.
- [x] Ensure `rebalance_strategy_description` is excluded from
      `rebalance_strategy_uid` hashing.
- [x] Add SDK upsert and lookup helpers for `RebalanceStrategies` backed by
      existing `SimpleTable` persistence/query machinery.
- [x] Add tests proving description changes update metadata without changing
      `rebalance_strategy_uid`.
- [x] Move rebalance strategy metadata SimpleTable code into
      `mainsequence.markets.portfolios.simple_tables`.

### Phase 4d: Portfolio metadata registry

- [x] Add a `Portfolios` `SimpleTable` metadata registry with unique
      `unique_identifier`.
- [x] Define `unique_identifier` as the
      `PortfolioIndexAsset.unique_identifier` created by the canonical portfolio
      configuration hash workflow.
- [x] Store `description` as nullable string metadata.
- [x] Keep `description` out of `PortfolioWeights`, `PortfoliosDataNode`, and
      portfolio configuration hashing.
- [x] Add SDK upsert and lookup helpers for `Portfolios` backed by existing
      `SimpleTable` persistence/query machinery.
- [x] Upsert portfolio metadata from canonical portfolio weights/value updates
      when a portfolio description is available.
- [x] Add tests proving the table is keyed by unique
      `PortfolioIndexAsset.unique_identifier`.
- [x] Move portfolio metadata SimpleTable code into
      `mainsequence.markets.portfolios.simple_tables`.

### PortfolioAbout removal plan

- [x] Identify SDK/backend-client read and write paths that still expect
      `PortfolioAbout` or `target_portfolio_about`.
- [x] Replace SDK reads with `PortfolioMetadata` lookup by
      `PortfolioIndexAsset.unique_identifier`.
- [x] Replace SDK portfolio creation/update payloads that write
      `target_portfolio_about.description` with `PortfolioMetadata` upserts.
- [x] Add an SDK backfill helper for existing
      `PortfolioAbout`/`target_portfolio_about` descriptions into the
      `Portfolios` `SimpleTable` keyed by
      `PortfolioIndexAsset.unique_identifier`.
- [x] Remove the SDK `PortfolioAbout` model after SDK readers and writers use
      the simple table.
- [x] Add regression tests asserting portfolio descriptions do not live in
      portfolio creation payloads.

### Phase 5: PortfolioWeights canonical update

- [x] Reuse the existing TDAG serialization helper for canonical
      `PortfolioConfiguration`; do not add a new serializer.
- [x] Compute `portfolio_configuration_hash` from the full canonical
      `PortfolioConfiguration`.
- [x] Use `portfolio_configuration_hash` to get or create the backend
      `Portfolio` and `PortfolioIndexAsset`.
- [x] Store the resulting `PortfolioIndexAsset.unique_identifier` as
      `portfolio_index_asset_unique_identifier` in canonical portfolio rows.
- [x] Implement `PortfolioWeights.update()` as the primary canonical write path.
- [x] Add `_calculate_weights()` as the explicit extension point called by
      `PortfolioWeights.update()`.
- [x] Extract reusable Portfolios portfolio calculation logic so `_calculate_weights()`
      can build canonical rows without producing a legacy per-portfolio DataNode.
- [x] Add a normalizer from postprocessed Portfolios `weights` frames to
      `PortfolioWeights` rows.
- [x] Validate asset `unique_identifier` values before writing canonical rows.
- [x] Validate that two portfolios holding the same asset at the same timestamp
      can write to the same table.

### Phase 6: SignalWeights canonical update

- [x] Implement `SignalWeights.update()` as the primary canonical signal write
      path.
- [x] Add `_calculate_signal_weights()` as the explicit extension point called
      by `SignalWeights.update()`.
- [x] Populate every canonical signal row with the computed `signal_uid`.
- [x] Upsert the signal metadata row with `signal_uid` and
      `signal_description` when available.
- [x] Update `WeightsBase.interpolate_index()` to optionally read from
      `SignalWeights` with `dimension_filters={"signal_uid": [...]}`.
- [x] Enable built-in signals to use the direct shared-table read path through
      `WeightsBase`.
- [x] Document custom signal extension through `_calculate_signal_weights()`,
      not per-signal DataNode mirroring.

### Phase 7: PortfoliosDataNode canonical update

- [x] Add the required `PortfoliosDataNode` config-first DataNode contract.
- [x] Implement `PortfoliosDataNode.update()` as the primary canonical
      portfolio-value write path.
- [x] Add `_calculate_portfolio_values()` as the explicit extension point called
      by `PortfoliosDataNode.update()`.
- [x] Write `close`, `return`, and value metadata indexed by
      `portfolio_index_asset_unique_identifier`.
- [x] Add a normalizer from the Portfolios calculated `portfolio` frame to
      `PortfoliosDataNode` rows.
- [x] Ensure `PortfoliosDataNode` sync uses the same namespace and
      `portfolio_index_asset_unique_identifier` as `PortfolioWeights`.
- [x] Update Portfolios docs to describe which table owns values and which table owns
      weights.

### Phase 8: Remove legacy Portfolios runtime output

- [x] Remove the Portfolios runtime dependency on per-portfolio output DataNodes.
- [x] Stop writing serialized `rebalance_weights` JSON as the portfolio weight
      storage surface.
- [x] Ensure no canonical runtime path mirrors from legacy portfolio or signal
      DataNodes.
- [x] Keep any one-off legacy data import tooling outside the canonical Portfolios
      DataNode runtime.

### Phase 8b: Explicit portfolio price sources

- [x] Remove portfolio-price construction through the legacy indirect routing
      layer.
- [x] Require portfolio price pipelines to use an explicit normalized source
      bars DataNode or `MarketsTimeSeries` identifier.
- [x] Treat price-source namespace normalization as the responsibility of the
      source price node before data reaches `InterpolatedPrices`.
- [x] Keep `InterpolatedPrices` focused on fetching explicit bars,
      interpolation, upsampling, validation, and schema normalization.

### Phase 9: Tests

- [x] Contract validation for `PortfolioWeights`.
- [x] Contract validation for `SignalWeights`.
- [x] Contract validation for `PortfoliosDataNode`.
- [x] Namespace isolation tests.
- [x] Signal configuration hash identity tests.
- [x] Full-index duplicate detection tests.
- [ ] Dimension-filter query payload tests.
- [ ] Incremental update tests using nested `index_progress`.
- [x] `PortfolioWeights.update()` calls `_calculate_weights()` and validates
      canonical rows.
- [x] `SignalWeights.update()` calls `_calculate_signal_weights()` and validates
      canonical rows.
- [x] `PortfoliosDataNode.update()` calls `_calculate_portfolio_values()` and
      validates canonical rows.
- [x] Tests proving canonical runtime does not write per-portfolio JSON output.

### Phase 10: Model-first package cleanup

- [x] Rename `mainsequence.markets.portfolios` to
      `mainsequence.markets.portfolios`.
- [x] Update the ADR title, docs, tests, examples, and package imports to use
      "portfolios" as the canonical name.
- [x] Do not add a compatibility shim package for
      `mainsequence.markets.portfolios`; update callers to the new
      import path.
- [ ] Convert the current `utils.py` module into a `utils/` package so it can
      hold both general helpers and reusable first-party portfolio components.
- [ ] Move current `utils.py` helpers into `portfolios/utils/helpers.py` or an
      equivalent internal helper module.
- [ ] Replace `contrib` with `utils` by moving built-in signals from
      `contrib/signals` to `portfolios/utils/signals`.
- [x] Move `RebalanceStrategyBase` into `rebalance_strategy/base.py`.
- [x] Move built-in rebalance strategies into `rebalance_strategy`.
- [x] Split built-in rebalance strategy implementations into one module per
      strategy class.
- [ ] Move built-in price helpers/nodes from `contrib/prices` to
      `portfolios/utils/prices`.
- [ ] Keep canonical DataNodes, SimpleTables, portfolio identity hashing,
      storage initialization, and public configuration models outside `utils`.
- [ ] Keep the top-level `mainsequence.markets.portfolios` namespace
      model-oriented: export portfolio models, canonical DataNodes, SimpleTable
      metadata models, `PortfoliosDataNode`, and resource factories.
- [ ] Update every internal import, test import, and example import to the new
      package path in one pass.
- [ ] Remove stale `virtualfundbuilder` naming from logger names, generated
      descriptions, documentation, and comments where it refers to the package
      rather than historical ADR context.
- [ ] Run focused canonical DataNode, simple table, contrib-signal replacement,
      and portfolio runtime tests after the rename.

## Open Questions

None for the write path. Canonical writes belong to the canonical DataNode
`update()` methods.

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
9. Canonical Portfolios runtime writes go through the canonical DataNode `update()`
   methods and do not produce per-portfolio JSON output tables.
10. The three canonical source tables are initialized by one portfolio-domain
    endpoint call after their `DataNodeStorage`/`DynamicTableMetaData` UIDs
    already exist.
11. Portfolio descriptions are retrieved from the `Portfolios` `SimpleTable`
    keyed by `PortfolioIndexAsset.unique_identifier`, not from `PortfolioAbout`.
12. The public package for this architecture is
    `mainsequence.markets.portfolios`, not
    `mainsequence.markets.portfolios`.
13. Built-in reusable signals and price helpers live under
    `mainsequence.markets.portfolios.utils`, while rebalance strategy base and
    built-ins live under `mainsequence.markets.portfolios.rebalance_strategy`.
