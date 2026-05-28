# ADR 0018: Remove DataNode Pickling

Date: 2026-05-28

Status: Proposed

## Related ADRs

- ADR 0002: Multidimensional DataNode Update Contract
- ADR 0011: DuckDB N-Dimensional Row Identity
- ADR 0014: DataNode Records and Foreign Keys
- ADR 0015: APIDataNode UID Runtime Identity
- ADR 0017: MetaTable Canonical Storage

## Context

The DataNode runtime currently uses local pickling as part of execution and
rebuild.

The active mechanism spans several layers:

- `DataNode.persist_to_pickle()` in
  `mainsequence/tdag/data_nodes/data_nodes.py`
- object-state serialization via `DataNode.__getstate__()` and
  `_prepare_state_for_pickle(...)`
- marker emission from
  `mainsequence/tdag/data_nodes/build_operations.py`:
  - `is_time_serie_pickled`
  - `is_api_time_serie_pickled`
- file loading through:
  - `load_from_pickle(...)`
  - `load_and_set_from_pickle(...)`
  - `rebuild_and_set_from_update_hash(...)`
- runtime state restoration through:
  - `PickleRebuilder`
  - `DeserializerManager.deserialize_pickle_state(...)`
  - `DataNode._set_state_with_sessions(...)`
- sidecar persistence of the data source to:
  - `pickled_ts/<data_source_uid>/data_source.pickle`

There is also a filesystem contract in `mainsequence/tdag/config.py`:

- `ogm.pickle_storage_path`
- default node pickles:
  - `pickled_ts/<data_source_uid>/<update_hash>.pickle`
- API node pickles:
  - `pickled_ts/<data_source_uid>/api-<update_hash>.pickle`

At the same time, the runtime already contains a canonical non-pickle rebuild
path:

- `rebuild_from_configuration(...)`

That function reconstructs a DataNode from stored build configuration and data
source identity. This is a stronger source of truth than local pickled object
state.

The current pickle-based design causes several problems:

- runtime behavior depends on hidden local filesystem state;
- object reconstruction depends on `cloudpickle` compatibility and module layout;
- the sidecar data source pickle is a second implicit persistence contract;
- DataNode execution and DataNode serialization are coupled too tightly;
- debug and dependency execution paths are harder to reason about because they
  can take different code paths depending on whether a local pickle exists;
- cleanup, atomic writes, and path management exist only to support a local
  cache layer rather than core DataNode semantics.

The backend git-hash verification hook for pickles is already a no-op, which is
another signal that the original pickle-based guarantees are no longer central
to the design.

## Decision

Remove DataNode pickling as a runtime mechanism.

The canonical rebuild path will be:

1. resolve backend/local persisted build configuration;
2. rebuild the DataNode from configuration;
3. reattach runtime-only collaborators explicitly;
4. synchronize persistence/session state explicitly.

The runtime will no longer:

- write node `.pickle` files for execution;
- write sidecar data source pickles for execution;
- emit pickle markers in serialized state;
- rebuild dependencies by loading pickled node objects;
- depend on `cloudpickle` for DataNode execution.

## Non-Goals

- Do not preserve long-term compatibility for existing `pickled_ts/` files.
- Do not keep dual rebuild modes where some paths use pickle and some use pure
  rebuild indefinitely.
- Do not replace file-based pickles with another opaque object snapshot format.
- Do not broaden the scope to unrelated persistence helpers unless they are
  directly part of DataNode runtime execution.

## Scope

In scope:

- `mainsequence/tdag/data_nodes/data_nodes.py`
- `mainsequence/tdag/data_nodes/build_operations.py`
- `mainsequence/tdag/data_nodes/run_operations.py`
- `mainsequence/tdag/config.py` pickle path contract

Likely follow-up scope:

- `mainsequence/client/models_metatables.py::persist_to_pickle()` if it is still
  only serving the removed DataNode sidecar path

## Implementation Tasks

- [ ] Deprecate pickle-specific public helpers in docs and docstrings.
- [ ] Change `rebuild_and_set_from_update_hash(...)` to rebuild from canonical
      configuration without first creating a pickle.
- [ ] Replace `load_and_set_from_pickle(...)` call sites with a pure
      rebuild-and-attach flow.
- [ ] Update `run_operations._execute_sequential_debug_update(...)` to use the
      pure rebuild path.
- [ ] Simplify `DataNode._set_state_with_sessions(...)` so it no longer
      deserializes pickle markers from object state.
- [ ] Remove `PickleRebuilder` handling for:
      - `is_time_serie_pickled`
      - `is_api_time_serie_pickled`
- [ ] Remove marker emission from:
      - `_serialize_timeserie(...)`
      - `_serialize_api_timeserie(...)`
- [ ] Remove:
      - `DataNode.persist_to_pickle()`
      - `DataNode._atomic_pickle_dump()`
      - `DataNode.get_pickle_path_from_time_serie()`
      - `load_from_pickle(...)`
      - `load_and_set_from_pickle(...)`
      - `flush_pickle(...)`
- [ ] Remove sidecar data source pickles:
      - `data_source_dir_path(...)`
      - `data_source_pickle_path(...)`
      - `load_data_source_from_pickle(...)`
- [ ] Remove `DataNode.__getstate__()`, `DataNode.__setstate__()`, and
      `_prepare_state_for_pickle(...)`.
- [ ] Remove runtime `cloudpickle` imports from DataNode execution code.
- [ ] Remove or repurpose `ogm.pickle_storage_path` if nothing else requires it.
- [ ] Rewrite tests that currently assert pickle markers, pickle paths, and
      pickle rebuild behavior.

## Consequences

### Positive

- Runtime rebuild becomes explicit and deterministic.
- Local filesystem state stops being part of normal DataNode execution.
- Cross-version pickle compatibility stops being a runtime concern.
- DataNode execution becomes easier to reason about and test.
- The runtime aligns more closely with canonical backend build configuration.

### Negative

- Cold rebuilds may become slower unless an in-memory cache is introduced.
- Existing local `pickled_ts/` artifacts become obsolete.
- Any downstream tooling that depends on local pickle files will need migration.

### Risk

The most sensitive runtime impact is dependency execution:

- `run_operations._execute_sequential_debug_update(...)` currently rebuilds
  missing dependencies through `rebuild_and_set_from_update_hash(...)`
- if the pure rebuild path is not equivalent, dependency execution will regress

The second major risk is runtime reattachment:

- `_set_state_with_sessions(...)` currently does more than one thing
- removal must preserve:
  - persistence-manager initialization
  - update-statistics synchronization
  - dependency traversal depth control

## Validation

- [ ] Add tests proving `rebuild_and_set_from_update_hash(...)` works without
      creating local pickle files.
- [ ] Add tests proving dependency execution rebuilds missing nodes from
      canonical configuration only.
- [ ] Add tests proving no data source sidecar pickle is required.
- [ ] Remove tests that assert pickle marker emission and pickle path layout.
- [ ] Add targeted regression coverage around sequential debug execution and
      cold dependency rebuilds.

## Migration Notes

The recommended execution order is:

1. replace runtime call sites with pure rebuild;
2. simplify runtime attachment/state restoration;
3. remove marker serialization;
4. remove file persistence;
5. remove object pickling hooks;
6. clean up docs, tests, and obsolete filesystem contracts.
