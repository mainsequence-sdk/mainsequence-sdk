# ADR 0019: Remove Cold Rebuild From DataNode Dependency Execution

Date: 2026-05-29

Status: Accepted and implemented

## Related ADRs

- ADR 0014: DataNode Records and Foreign Keys
- ADR 0015: APIDataNode UID Runtime Identity
- ADR 0017: MetaTable Canonical Storage
- ADR 0018: Remove DataNode Pickling

## Context

ADR 0018 removed pickled DataNode objects from runtime execution. The migration
kept a replacement path named `rebuild_from_configuration(...)`, where the SDK
looks up a `DataNodeUpdate`, reads its stored `build_configuration`, imports the
DataNode class, and constructs a new object with:

```python
TimeSerieClass(**time_serie_config)
```

That was a reasonable transitional substitute for pickle loading, but it no
longer fits the architecture established by ADR 0017.

The current architecture separates responsibilities:

- `PlatformTimeIndexMetaData` / `MetaTable` owns storage identity and table
  contract.
- `DataNode` owns update behavior.
- `DataNodeUpdate` owns backend update-process state.
- `DataNode.dependencies()` owns the executable dependency graph declaration.

Storage is now an explicit runtime constructor argument:

```python
class PricesNode(DataNode):
    def __init__(
        self,
        config: PricesConfig,
        storage_table: type[PlatformTimeIndexMetaData],
    ):
        super().__init__(config=config, storage_table=storage_table)
```

The `DataNode` wrapper intentionally removes `storage_table` from saved build
configuration before hashing and persistence. This is correct: storage identity
is not owned by the updater config.

That means generic cold rebuild from `build_configuration` is structurally
incomplete. `rebuild_from_configuration(...)` fetches
`data_node_update.data_node_storage`, but then does not pass an equivalent
`storage_table` authoring class into the DataNode constructor. Patching that by
injecting storage into saved config would undo the ADR 0017 boundary and make
storage identity part of the update-process rebuild payload again.

## Problem

Dependency execution should not require reconstructing DataNode objects from
backend configuration.

Dependencies are declared in source code:

```python
def dependencies(self):
    return {"prices": self.prices_node}
```

The executable dependency object graph already exists when a head DataNode is
constructed. During `run()`, the SDK calls `set_relation_tree()`, walks that
declared graph, creates missing `DataNodeUpdate` rows, and connects backend
dependency edges.

The current cold rebuild fallback creates several architectural problems:

- It treats backend `build_configuration` as an object factory for executable
  code, even though it is update metadata.
- It creates pressure to persist runtime-only constructor state such as
  `storage_table`.
- It makes stale backend dependency rows look executable even when they are no
  longer declared by the current source graph.
- It makes dependency execution depend on importable historical constructors
  rather than the already-instantiated dependency graph.
- It conflates graph registration with graph execution.
- It preserves the shape of the old pickle-loading model after pickles have
  been removed.

The bug where `rebuild_from_configuration(...)` cannot construct nodes that
require `storage_table` is a symptom. The underlying issue is that the runtime
should not cold-rebuild DataNode instances for dependency execution at all.

## Decision

Remove generic cold DataNode rebuild from normal dependency execution.

The canonical execution model is:

1. User code constructs the head DataNode.
2. DataNode constructors instantiate dependency DataNodes explicitly.
3. `dependencies()` returns the declared dependency instances.
4. `set_relation_tree()` registers backend update nodes and dependency edges
   from those instances.
5. `set_dependencies_df()` asks the backend for execution priority/order.
6. The local debug runner executes dependency instances from the in-memory
   declared graph.
7. If backend dependency metadata contains a node that is not present in the
   declared graph, the SDK treats it as graph drift and raises a clear error.

`rebuild_from_configuration(...)` and `rebuild_and_set_from_update_hash(...)`
are deleted. They are not hidden runtime fallbacks and not diagnostic APIs.

## Runtime Boundary

`build_configuration` remains useful as metadata:

- update identity diagnostics;
- run detail display;
- reproducibility inspection;
- comparison of current and persisted update configuration;
- controlled tooling that explicitly asks to inspect metadata.

It is not the source of executable dependency objects during a normal
DataNode run.

`DataNode.dependencies()` is the source of executable dependency objects.

## Rejected Alternatives

### Inject `storage_table` into `time_serie_config`

This would make the immediate constructor failure go away, but it restores the
wrong boundary. Storage identity would leak back into the persisted update
configuration or require an implicit backend-to-authoring-class conversion.

The storage table is explicit runtime state. It should be supplied by the
source-level DataNode constructor, not recovered through generic backend config
rehydration.

### Keep cold rebuild for stale dependency rows

Executing stale backend rows is unsafe. If a dependency is not returned by the
current source `dependencies()` graph, it should not run just because an older
backend relation still exists.

The correct behavior is to detect graph drift and fail clearly. Silent
reconciliation can be added later only as an explicit graph-management feature,
not as a runtime rebuild fallback.

### Reintroduce object snapshots without pickle

Replacing pickle with another opaque snapshot format would keep the same
problem. DataNode runtime execution should be source-declared and explicit, not
snapshot-rehydrated.

## Implementation Tasks

- [x] Remove the fallback call to
      `build_operations.rebuild_and_set_from_update_hash(...)` from sequential
      dependency execution.
- [x] Make `_execute_sequential_debug_update(...)` require dependency instances
      from the declared graph.
- [x] When the backend priority dataframe contains a dependency missing from
      the declared graph, raise a clear graph drift error.
- [x] Delete `rebuild_from_configuration(...)`.
- [x] Delete `rebuild_and_set_from_update_hash(...)`.
- [x] Update ADR 0018 validation notes that still assume cold dependency
      rebuilds from canonical configuration are desired.
- [x] Add regression tests for dependency execution with no cold rebuild path.

## Validation

Required tests:

- local debug execution uses declared dependency instances only;
- nested dependencies are discovered through recursive `dependencies()` calls;
- stale backend dependency rows trigger a clear error;
- no test asserts that dependency execution can rebuild missing DataNode objects
  from `build_configuration`;
- `storage_table` remains absent from persisted `DataNode` build configuration;
- API dependencies remain table references and are not treated as executable
  DataNode update dependencies.

## Consequences

### Positive

- DataNode execution matches the source-declared object graph.
- Storage remains outside update-process configuration.
- Stale backend dependency rows cannot silently execute old code paths.
- The post-pickle runtime no longer preserves a pickle-shaped rebuild model.
- Dependency execution becomes easier to reason about and test.

### Negative

- A backend-only dependency row can no longer be executed locally unless the
  current source graph declares it.
- Some diagnostic workflows may need explicit tooling if they still need richer
  historical build-configuration inspection.
- Distributed execution will need a deliberate entrypoint model rather than
  relying on generic SDK object reconstruction.

## Migration Notes

The migration deletes the cold rebuild API and changes local debug dependency
execution to use only the in-memory declared dependency graph.

Do not fix the current constructor failure by adding `storage_table` back into
stored DataNode configuration. That solves the symptom while weakening the
architecture.
