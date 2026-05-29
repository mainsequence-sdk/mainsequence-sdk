# ADR 0018: Remove DataNode Pickling

Date: 2026-05-28

Status: Accepted and implemented

## Related ADRs

- ADR 0002: Multidimensional DataNode Update Contract
- ADR 0011: DuckDB N-Dimensional Row Identity
- ADR 0014: DataNode Records and Foreign Keys
- ADR 0015: APIDataNode UID Runtime Identity
- ADR 0017: MetaTable Canonical Storage
- ADR 0019: Remove Cold Rebuild From DataNode Dependency Execution

## Context

The DataNode runtime currently uses local pickling as part of execution and
rebuild.

The active mechanism spans several layers:

- `DataNode.persist_to_pickle()` in
  `mainsequence/meta_tables/data_nodes/data_nodes.py`
- object-state serialization via `DataNode.__getstate__()` and
  `_prepare_state_for_pickle(...)`
- marker emission from
  `mainsequence/meta_tables/data_nodes/build_operations.py`:
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

There is also a filesystem contract in `mainsequence/meta_tables/config.py`:

- `ogm.pickle_storage_path`
- default node pickles:
  - `pickled_ts/<data_source_uid>/<update_hash>.pickle`
- API node pickles:
  - `pickled_ts/<data_source_uid>/api-<update_hash>.pickle`

During the initial migration, a non-pickle rebuild path,
`rebuild_from_configuration(...)`, was treated as a possible replacement for
pickle loading. ADR 0019 supersedes that direction: dependency execution should
use the source-declared `DataNode.dependencies()` graph, not generic object
reconstruction from backend build configuration.

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

No replacement object snapshot or generic DataNode reconstruction path is part
of runtime execution. Dependency execution after pickle removal is governed by
ADR 0019.

The runtime will no longer:

- write node `.pickle` files for execution;
- write sidecar data source pickles for execution;
- emit pickle markers in serialized state;
- rebuild dependencies by loading pickled node objects;
- depend on `cloudpickle` for DataNode execution.

## Non-Goals

- Do not preserve long-term compatibility for existing `pickled_ts/` files.
- Do not keep dual rebuild modes where some paths use pickle and some paths
  reconstruct objects from backend build configuration indefinitely.
- Do not replace file-based pickles with another opaque object snapshot format.
- Do not broaden the scope to unrelated persistence helpers unless they are
  directly part of DataNode runtime execution.

## Scope

In scope:

- `mainsequence/meta_tables/data_nodes/data_nodes.py`
- `mainsequence/meta_tables/data_nodes/build_operations.py`
- `mainsequence/meta_tables/data_nodes/run_operations.py`
- `mainsequence/meta_tables/config.py` pickle path contract

Also removed:

- `mainsequence/client/models_metatables.py::persist_to_pickle()` because it only
  served the removed DataNode sidecar path

## Implementation Tasks

- [x] Deprecate pickle-specific public helpers in docs and docstrings.
- [x] Remove pickle-backed rebuild from runtime execution.
- [x] Replace `load_and_set_from_pickle(...)` call sites without preserving a
      pickle-shaped object reconstruction model.
- [x] Remove dependency execution paths that load pickled DataNode objects.
- [x] Simplify `DataNode._set_state_with_sessions(...)` so it no longer
      deserializes pickle markers from object state.
- [x] Remove `PickleRebuilder` handling for:
      - `is_time_serie_pickled`
      - `is_api_time_serie_pickled`
- [x] Remove marker emission from:
      - `_serialize_timeserie(...)`
      - `_serialize_api_timeserie(...)`
- [x] Remove:
      - `DataNode.persist_to_pickle()`
      - `DataNode._atomic_pickle_dump()`
      - `DataNode.get_pickle_path_from_time_serie()`
      - `load_from_pickle(...)`
      - `load_and_set_from_pickle(...)`
      - `flush_pickle(...)`
- [x] Remove sidecar data source pickles:
      - `data_source_dir_path(...)`
      - `data_source_pickle_path(...)`
      - `load_data_source_from_pickle(...)`
- [x] Remove `DataNode.__getstate__()`, `DataNode.__setstate__()`, and
      `_prepare_state_for_pickle(...)`.
- [x] Remove runtime `cloudpickle` imports from DataNode execution code.
- [x] Remove or repurpose `ogm.pickle_storage_path` if nothing else requires it.
- [x] Rewrite tests that currently assert pickle markers, pickle paths, and
      pickle rebuild behavior.

## Consequences

### Positive

- Runtime execution no longer depends on opaque object snapshots.
- Local filesystem state stops being part of normal DataNode execution.
- Cross-version pickle compatibility stops being a runtime concern.
- DataNode execution becomes easier to reason about and test.
- Dependency execution can be aligned with source-declared graphs, as described
  in ADR 0019.

### Negative

- Existing local `pickled_ts/` artifacts become obsolete.
- Any downstream tooling that depends on local pickle files will need migration.

### Risk

The most sensitive runtime impact is dependency execution. ADR 0019 resolves
that follow-up by deleting generic cold rebuild and requiring dependencies to be
executed from the source-declared dependency graph.

The second major risk is runtime reattachment:

- `_set_state_with_sessions(...)` currently does more than one thing
- removal must preserve:
  - persistence-manager initialization
  - update-statistics synchronization
  - dependency traversal depth control

## Validation

- [x] Remove tests proving `rebuild_and_set_from_update_hash(...)` works without
      creating local pickle files; that API is deleted by ADR 0019.
- [x] Add tests proving dependency execution does not rebuild missing nodes from
      canonical configuration.
- [x] Add tests proving no data source sidecar pickle is required.
- [x] Remove tests that assert pickle marker emission and pickle path layout.
- [x] Add targeted regression coverage around sequential debug execution and
      rejection of undeclared backend dependency rows.

## Migration Notes

The recommended execution order is:

1. remove pickle-backed runtime call sites;
2. simplify runtime attachment/state restoration;
3. remove marker serialization;
4. remove file persistence;
5. remove object pickling hooks;
6. clean up docs, tests, and obsolete filesystem contracts.
