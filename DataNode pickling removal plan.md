# DataNode Pickling Removal Plan

## Goal

Remove DataNode pickling as a runtime mechanism.

Target state:

- DataNode rebuild and dependency execution do not rely on local `.pickle` files.
- DataNode state is reconstructed from canonical build configuration and backend metadata.
- No runtime path writes or reads `pickled_ts/<data_source_uid>/...`.
- No serializer emits pickle markers such as `is_time_serie_pickled` or `is_api_time_serie_pickled`.
- No runtime component depends on `cloudpickle` for DataNode execution.

This document is a task plan only. It does not implement the removal.

## Current machinery

### DataNode object persistence

Files:

- `mainsequence/tdag/data_nodes/data_nodes.py`
- `mainsequence/tdag/data_nodes/build_operations.py`
- `mainsequence/tdag/config.py`

Current behavior:

- `DataNode.get_pickle_path_from_time_serie()` computes the local pickle path.
- `DataNode.persist_to_pickle()`:
  - updates backend git/code metadata for non-API nodes
  - nulls `_local_persist_manager`
  - writes a sidecar data source pickle
  - atomically writes the node itself with `cloudpickle`
- `DataNode.__getstate__()` calls `_prepare_state_for_pickle(...)`
- `_prepare_state_for_pickle(...)` serializes object state and strips unpicklable fields
- `DataNode.__setstate__()` restores raw object state from pickle

### Serialization markers

Files:

- `mainsequence/tdag/data_nodes/build_operations.py`
- `mainsequence/tdag/data_nodes/data_nodes.py`

Current behavior:

- `serialize_argument(..., pickle_ts=True)` emits:
  - `{"is_time_serie_pickled": True, "update_hash": ..., "data_source_uid": ...}`
  - `{"is_api_time_serie_pickled": True, "update_hash": ..., "data_source_uid": ...}`
- `DataNode` and `APIDataNode` register these handlers with the serializer dispatcher

### Pickle rebuild path

Files:

- `mainsequence/tdag/data_nodes/build_operations.py`
- `mainsequence/tdag/data_nodes/data_nodes.py`

Current behavior:

- `PickleRebuilder` handles:
  - `is_time_serie_pickled`
  - `is_api_time_serie_pickled`
  - Pydantic wrappers
  - ORM wrappers
  - tuple/list wrappers
- `DeserializerManager.deserialize_pickle_state(...)` routes object state through `PickleRebuilder`
- `DataNode._set_state_with_sessions()` deserializes pickled nested state and reconnects persistence/runtime objects
- `load_from_pickle(...)` loads a node from disk and reattaches the data source
- `load_and_set_from_pickle(...)` loads then calls `_set_state_with_sessions(...)`

### Rebuild and execution integration

Files:

- `mainsequence/tdag/data_nodes/build_operations.py`
- `mainsequence/tdag/data_nodes/run_operations.py`

Current behavior:

- `rebuild_and_set_from_update_hash(...)`:
  - ensures a pickle exists
  - rebuilds from configuration if the pickle is missing
  - persists a pickle
  - then loads from pickle
- `rebuild_from_configuration(...)` already supports pure rebuild from canonical build configuration and data source UID
- `run_operations._execute_sequential_debug_update(...)` falls back to `rebuild_and_set_from_update_hash(...)` when a dependency is not already present in memory

### Filesystem contract

Files:

- `mainsequence/tdag/config.py`
- `mainsequence/tdag/data_nodes/build_operations.py`

Current behavior:

- pickle root is `ogm.pickle_storage_path`
- node path is:
  - `pickled_ts/<data_source_uid>/<update_hash>.pickle`
- API node path is:
  - `pickled_ts/<data_source_uid>/api-<update_hash>.pickle`
- sidecar data source path is:
  - `pickled_ts/<data_source_uid>/data_source.pickle`

## Why remove it

- The runtime already has a canonical non-pickle rebuild path through backend build configuration.
- Pickling duplicates runtime state and configuration state.
- Pickled objects are tightly coupled to Python/module layout and `cloudpickle` behavior.
- The sidecar data source pickle is an implicit secondary persistence contract.
- Import-time and runtime state restoration become harder to reason about because execution can depend on previously materialized local files.
- Pickle files add filesystem coordination, atomic write logic, cleanup, and path management that are not core to DataNode semantics.
- The backend git-hash verification hook is already a no-op, which is a signal that parts of the pickle path have outlived their original purpose.

## Blast radius / impact

### Direct code impact

- `mainsequence/tdag/data_nodes/data_nodes.py`
  - `get_pickle_path_from_time_serie()`
  - `persist_to_pickle()`
  - `_atomic_pickle_dump()`
  - `__getstate__()`
  - `__setstate__()`
  - `_prepare_state_for_pickle()`
  - `_set_state_with_sessions()`
- `mainsequence/tdag/data_nodes/build_operations.py`
  - `_serialize_timeserie(...)`
  - `_serialize_api_timeserie(...)`
  - `serialize_argument(..., pickle_ts=True)` call paths
  - `PickleRebuilder`
  - `DeserializerManager.deserialize_pickle_state(...)`
  - `data_source_dir_path(...)`
  - `data_source_pickle_path(...)`
  - `flush_pickle(...)`
  - `load_from_pickle(...)`
  - `load_and_set_from_pickle(...)`
  - `rebuild_and_set_from_update_hash(...)`
  - `load_and_set_from_hash_id(...)`
- `mainsequence/tdag/config.py`
  - `pickle_storage_path`
  - `get_ts_pickle_path(...)`

### Runtime behavior impact

- Sequential debug dependency execution will no longer hydrate dependencies from local pickle cache.
- Any workflow that expects a local file cache of rebuilt nodes will change.
- Node state restoration must come from constructor/configuration + explicit runtime reattachment, not object deserialization.

### Data source impact

- Sidecar persistence of `DynamicTableDataSource` must be removed.
- Runtime rebuild must obtain the data source directly from:
  - backend lookup by UID, or
  - an explicit in-memory object passed through the rebuild path

### Performance impact

- Likely slower cold-path rebuilds if no replacement cache exists.
- Lower local filesystem churn.
- Fewer stale local artifacts and less hidden state.

### Operational impact

- Existing `pickled_ts/` directories become obsolete.
- Local cleanup tooling can be simplified.
- Cross-version pickle incompatibility stops being a runtime concern.

### Test impact

Current tests that explicitly pin pickle behavior must be rewritten or removed:

- `tests/test_pod_project_resolution.py`
  - pickle marker tests
  - pickle path tests
  - legacy marker rejection tests
- Any future tests added around local file-based rebuilds

### Likely hidden impact

- Any downstream code outside this repo that calls:
  - `persist_to_pickle()`
  - `load_from_pickle()`
  - `load_and_set_from_pickle()`
  - `rebuild_and_set_from_update_hash()`
- Any tooling that inspects `pickled_ts/` on disk

## Suggested migration direction

### Principle

Treat canonical build configuration as the only rebuild source of truth.

That means:

- rebuild node from backend/local stored build configuration
- reattach runtime-only collaborators explicitly
- recreate dependencies through constructor/configuration, not pickle state

### Replacement model

Use `rebuild_from_configuration(...)` as the foundation and remove file-based persistence around it.

Replacement shape:

- `rebuild_and_set_from_update_hash(...)`
  - becomes a pure rebuild-and-attach function
  - no `persist_to_pickle()`
  - no `load_from_pickle()`
- `_set_state_with_sessions(...)`
  - should be simplified into an explicit runtime attachment step
  - it should no longer deserialize pickle markers from `self.__dict__`
- serializer path
  - stop emitting pickle markers
  - keep only stable serialized configuration / model wrappers needed for hashing and config reconstruction

## Proposed phases

### Phase 1: Freeze and isolate

- [ ] Mark pickle-specific public helpers as deprecated in docs and docstrings.
- [ ] Add a single internal feature boundary around file-based node persistence.
- [ ] Stop introducing new pickle-based features or tests.

### Phase 2: Pure rebuild path

- [ ] Change `rebuild_and_set_from_update_hash(...)` to rebuild from configuration without persisting a pickle first.
- [ ] Replace `load_and_set_from_pickle(...)` call sites with pure rebuild/attach logic.
- [ ] Update `run_operations._execute_sequential_debug_update(...)` to use the pure rebuild path.

### Phase 3: Runtime state attachment cleanup

- [ ] Simplify `DataNode._set_state_with_sessions(...)` so it does not depend on pickle-state deserialization.
- [ ] Remove `DeserializerManager.deserialize_pickle_state(...)` from DataNode execution.
- [ ] Ensure dependency reconstruction happens through normal constructor/config behavior.

### Phase 4: Remove serialization markers

- [ ] Remove `is_time_serie_pickled` marker generation.
- [ ] Remove `is_api_time_serie_pickled` marker generation.
- [ ] Remove `PickleRebuilder` handlers for those markers.
- [ ] Remove legacy marker tests.

### Phase 5: Remove file persistence

- [ ] Remove `DataNode.persist_to_pickle()`.
- [ ] Remove `_atomic_pickle_dump()`.
- [ ] Remove `get_pickle_path_from_time_serie()`.
- [ ] Remove `load_from_pickle()`.
- [ ] Remove `load_and_set_from_pickle()`.
- [ ] Remove `flush_pickle()`.
- [ ] Remove `data_source_pickle_path(...)` and sidecar data-source pickles.
- [ ] Remove `ogm.pickle_storage_path` if no longer used anywhere.

### Phase 6: Remove pickling hooks from object model

- [ ] Remove `DataNode.__getstate__()`.
- [ ] Remove `DataNode.__setstate__()`.
- [ ] Remove `DataNode._prepare_state_for_pickle()`.
- [ ] Remove `cloudpickle` imports from DataNode runtime code.

## Suggested compatibility policy

- Do not keep long-term backward compatibility for old `pickled_ts/` files.
- During rollout, fail loudly if a deprecated pickle-only path is invoked.
- Prefer a short migration window over dual execution modes.

## Suggested test plan

- [ ] Add tests proving `rebuild_and_set_from_update_hash(...)` works without creating local pickle files.
- [ ] Add tests proving dependency execution rebuilds nodes from canonical configuration only.
- [ ] Add tests proving data source reattachment happens without `data_source.pickle`.
- [ ] Remove tests that assert pickle marker emission and pickle path layout.
- [ ] Add regression tests for cold rebuild performance-sensitive paths where appropriate.

## Open questions

- [ ] Do any external tools or downstream repos depend on `pickled_ts/` on disk?
- [ ] Is an in-memory rebuild cache needed to replace the current local file cache for debug/update workflows?
- [ ] Can `_set_state_with_sessions(...)` be reduced to persistence-manager synchronization only?
- [ ] Is `mainsequence/client/models_metatables.py::persist_to_pickle()` also removable, or should it be handled in a separate follow-up ADR?

## Acceptance criteria

- [ ] No DataNode execution path reads or writes local pickle files.
- [ ] No serializer emits DataNode pickle markers.
- [ ] No runtime rebuild path depends on `cloudpickle`.
- [ ] `run_operations` can rebuild missing dependencies without local pickle files.
- [ ] `pickled_ts/` is no longer a required runtime directory.
