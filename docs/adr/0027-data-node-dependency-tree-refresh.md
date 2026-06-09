# ADR 0027: Automatic DataNode Dependency Tree Self-Healing

Date: 2026-06-09

Status: Proposed

## Context

`DataNode.dependencies()` is the SDK/application source of truth for runtime
dependency graphs. The backend stores dependency edges and dependency priority
metadata as derived execution state.

Current SDK refresh logic checks whether the current direct dependencies are
present in the backend dependency tree. That detects missing backend edges, but
it does not detect stale backend edges that remain after `dependencies()` code
changes. When stale backend UIDs remain in the dependency dataframe, the SDK can
later fail because the executable dependency map is built from current Python
code and does not contain the stale UID.

Setting `ogm_dependencies_linked = false` is not sufficient because it clears
cached priority state but does not remove stored dependency edges.

The SDK must not clear and rebuild backend dependency edges before every update
pass. The backend dependency tree exists so normal execution can reuse derived
dependency metadata. Backend graph mutation should happen only when the SDK
detects drift between backend-derived dependency state and the current Python
dependency graph.

This does not mean the runner can execute stale backend dependency rows. When
`update_tree=True`, the runner already builds the executable dependency map from
current Python `dependencies()` declarations. It must compare that map with the
backend dependency dataframe before execution. This validation is not an extra
backend rebuild; it is a safety check over objects the runner already needs for
dependency execution.

The backend exposes:

```text
POST /orm/api/ts_manager/local_time_serie/{uid}/clear-dependencies/
```

This endpoint clears outgoing dependency edges for the selected update node and
marks the dependency tree as unlinked. It does not delete DataNodes, storage
tables, or scheduler membership.

## Decision

The SDK should keep backend dependency metadata as the normal cached execution
state once `ogm_dependencies_linked` is true.

During normal `DataNode.run(update_tree=True)`, the runner must validate the
backend dependency dataframe against the executable dependency map built from
current Python `dependencies()` declarations. If the sets differ, the SDK must
self-heal once before executing dependency rows:

1. call `clear-dependencies/` on the head update node;
2. reconnect edges from current `dependencies()` declarations;
3. mark the relation tree linked;
4. fetch dependency metadata for subsequent execution.

This workflow is not part of every update pass. It runs only when the validation
detects stale backend extras, missing backend rows, or uninitialized current
dependency update nodes.

Stale backend extras are invalid during execution. When the runner sees backend
dependency UIDs that are not present in the current executable dependency map, it
must clear/rebuild the backend relation tree and re-read the dependency
dataframe before executing. If the backend still returns stale or incomplete
metadata after that automatic refresh, the runner must raise a
`DependencyUpdateError` that states the SDK already attempted the repair.

This validation only runs in dependency-update paths (`update_tree=True`). It
does not force graph walking for runs that update only the head node.

## SDK API Shape

Add a thin client wrapper on the update-node client model:

```python
DataNodeUpdate.clear_dependencies(timeout=None)
```

The method should call:

```text
POST /orm/api/ts_manager/local_time_serie/{uid}/clear-dependencies/
```

and return the structured backend response.

Do not add a public dependency-refresh option to `DataNode.run()`. Dependency
tree drift is an SDK consistency problem, not an application workflow parameter.

## Runner Integration

`set_relation_tree()` currently returns immediately when
`ogm_dependencies_linked` is true. After clearing dependencies, the SDK needs an
internal force path used only by automatic self-healing:

```python
set_relation_tree(force_rebuild=True)
```

or equivalent, so the rebuild does not short-circuit.

For full-tree refreshes, the force path must apply recursively to every
non-API `DataNode` reached from current `dependencies()` declarations. Clearing
only the head node is insufficient because stale deeper edges can still leak
back into the backend dependency dataframe.

The normal runner should continue using cached backend dependency metadata. The
automatic refresh path should not ask the backend to infer the replacement
graph; the SDK must reconnect edges from current `dependencies()` declarations
because only application code knows the graph.

Before executing dependency rows, the runner must validate:

```python
backend_uids = set(dependencies_df["update_node_uid"].astype(str))
declared_uids = set(update_map.keys())
```

If either difference is non-empty, the SDK must clear/rebuild once and validate
again. Only the second mismatch is fatal.

## Consequences

Dependency graph changes get a first-class relink path without making every
normal update run rebuild the dependency graph. A changed `dependencies()`
implementation no longer requires manual backend database cleanup or a special
client-side run flag.

Normal dependency execution also becomes safer: stale backend dependency rows are
self-healed before any dependency node is executed. Persistent mismatch after the
automatic rebuild is reported as a backend/SDK consistency failure with concrete
stale and missing update UIDs.
