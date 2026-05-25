# ADR 0006: TDAG Public UID Identifiers

Date: 2026-05-22

Status: Proposed

## Context

The SDK client model contract for selected TDAG resources now uses `uid: UUID`
as the public resource identifier. For those affected resources, `id` should not
be used in SDK-facing model fields, method arguments, runtime joins, CLI inputs,
or documentation examples.

The immediate SDK resources in scope are:

- `DataNodeStorage`
- `DataNodeUpdate`
- `SimpleTableStorage`
- `SimpleTableUpdate`
- Source-table, update-details, scheduler, and dependency relation surfaces that
  refer to those resources

This ADR is not a blind rename. The SDK still contains unrelated `id` concepts,
especially SimpleTable row ids and ids for models outside this TDAG migration.
Those are not part of this decision.

## Decision

Use `uid` as the only SDK-facing public resource identifier for affected TDAG
resources.

For the affected TDAG resources:

- SDK model fields should expose `uid`, not `id`.
- SDK method parameters should use names such as `*_uid`, `update_node_uid`, or
  `storage_uid`.
- SDK runtime joins should use UID-based columns and maps.
- CLI and docs should teach UID-based references.
- Remaining `*_id` usages in these resource paths are implementation defects to
  audit and remove unless they are SimpleTable row ids or unrelated model ids.

## Terminology

`uid` is the UUID-valued public resource identifier for affected TDAG resources.

`id` is not part of the affected TDAG resource identity contract in the SDK.

Valid `id` usages outside this migration include:

- SimpleTable row `id` / `record_id` / `records_ids`
- unrelated model ids, such as users, teams, projects, data sources, jobs, or
  assets
- local variable names inside unrelated code paths

`identifier`, `node_unique_identifier`, `storage_hash`, and `update_hash` are
logical or hash-based identifiers. They are not UIDs and must remain separate.

## Client Model Migration Review

The client model layer has already been partially migrated to public UIDs.

### TDAG client models

Observed migrated surfaces in `mainsequence/client/models_tdag.py`:

- `UUID` is imported and used for TDAG public identifiers.
- `UpdateNodeRef` carries `uid: UUID`.
- `_require_public_uid(obj, object_name)` centralizes the UID-required guard.
- `TableUpdateNode` declares `uid: UUID | None`.
- `TableUpdateNode._public_uid()` returns the public UID through the shared
  guard.
- `DataNodeUpdate.FILTERSET_FIELDS` exposes `uid`.
- `DataNodeUpdate.data_node_storage` is typed as `UUID | DataNodeStorage`.
- `DataNodeUpdate` action URLs use `_public_uid()` for public-resource routes.
- `DataNodeStorage` carries `uid: UUID | None`.
- `DataNodeStorage` action URLs use `_public_uid()` for public-resource routes.
- Scheduler client methods expose `update_node_uids` at the model boundary.
- Historical update payloads include `direct_dependency_uids`.

### SimpleTable client models

Observed migrated surfaces in `mainsequence/client/models_simple_tables.py`:

- `UUID` is imported and used for SimpleTable public identifiers.
- `SimpleTableStorage.FILTERSET_FIELDS` exposes `uid`.
- `SimpleTableStorage.run_query()` uses `_public_uid()` in the route.
- `SimpleTableStorage.insert_records_into_table(simple_table_uid=...)` uses a
  UID parameter.
- `SimpleTableStorage.upsert_records_into_table(simple_table_uid=...)` uses a
  UID parameter.
- `SimpleTableStorage.delete_records_from_table(data_node_storage_uid=...)`
  uses a UID parameter while preserving row `records_ids`.
- `STSourceTableConfiguration` carries `related_table_uid`.
- `SimpleTableUpdateRecord` carries `related_table_uid`.
- `SimpleTableUpdateDetails` carries `related_table_uid`.
- `SimpleTableUpdate.FILTERSET_FIELDS` exposes `uid`.
- `SimpleTableUpdate.remote_table` is typed as `UUID | SimpleTableStorage`.
- `SimpleTableUpdate` action URLs use `_public_uid()` for public-resource
  routes.

## Known SDK Gaps

These are SDK facts observed in the current code and should be fixed or verified
as part of implementation.

### SourceTableConfiguration has stale relation access

`SourceTableConfiguration` declares `related_table_uid`, but methods still
reference `self.related_table`.

Affected methods:

- `get_data_updates()`
- `get_time_scale_extra_table_indices()`
- `set_or_update_columns_metadata()`
- `patch()`

### SimpleTable foreign-key metadata still exposes `target_table`

`SimpleTableForeignKeyPayload.target_table` remains typed as `int` and is
documented as a target simple-table primary key. This must be audited before
changing schema relation generation.

### SimpleTable update upload methods still expose id-named parameters

`SimpleTableUpdate.insert_records_into_table(...)` and
`SimpleTableUpdate.insert_data_into_table(...)` still use a parameter named
`data_node_update_id`.

### BaseObjectOrm still assumes `.id`

`BaseObjectOrm.__hash__()` and `BaseObjectOrm.__repr__()` still fall back to
`.id`. Affected TDAG models that no longer expose `.id` need targeted behavior
or a safe generic identity helper.

Do not change `BaseObjectOrm._coerce_filter_id()` to accept UIDs. It should
remain id-specific for non-TDAG models that still use ids.

## Special Focus: DataNode Update Process Stability

The DataNode update process is the highest-risk migration area. It currently
uses id-named fields as stable join keys across scheduling, dependency
comparison, dependency-tree connection, returned update maps, and persistence.
Those paths need explicit UID migration, not a mechanical rename.

### Current id-dependent update path

`mainsequence/tdag/data_nodes/run_operations.py` still uses ids throughout the
main update lifecycle:

- `_setup_scheduler()` builds the scheduler name from
  `self.ts.data_node_update.id`.
- `_setup_scheduler()` calls `Scheduler.build_and_assign_to_update_nodes(...)`
  with `update_nodes_ids=[self.ts.data_node_update.id]`.
- `_pre_update_routines()` reads `self.ts.depth_df["update_node_id"]` and
  appends `self.ts.data_node_update.id`.
- `_pre_update_routines()` calls `scheduler.in_active_tree_connect(...)` using
  id-based update-node lists.
- `_pre_update_routines()` builds `update_nodes_in_tree` records with an `id`
  key.
- `_pre_update_routines()` appends the head update node with
  `"id": self.ts.data_node_update.id`.
- `_pre_update_routines()` keys `data_node_updates_map` by `m.id`.
- `_verify_tree_is_updated()` derives dependency ids from
  `d.data_node_update.id`.
- `_verify_tree_is_updated()` compares those ids against
  `dependencies_df["update_node_id"]`.

`mainsequence/tdag/data_nodes/data_nodes.py` also treats
`update_node_id`/`.id` as the dependency dataframe key:

- `set_dependencies_df()` filters the head node with
  `depth_df["update_node_id"] != self.data_node_update.id`.

`mainsequence/tdag/base_persist_managers.py` still connects dependencies with
id-named arguments:

- `depends_on_connect(target_time_serie_id=new_ts.data_node_update.id)`
- `depends_on_connect_to_api_table(target_table_id=...data_node_storage.id)`

SimpleTable update persistence has the same issue:

- `mainsequence/tdag/simple_tables/persist_managers.py` calls
  `SimpleTableUpdate.insert_records_into_table(data_node_update_id=self.data_node_update.id, ...)`.
- The same file calls
  `SimpleTableStorage.delete_records_from_table(data_node_storage_id=self.data_node_storage.id, ...)`.
- `mainsequence/tdag/simple_tables/table_nodes.py` resolves foreign-key schema
  targets with `target_storage.id`.

### Update-process migration rule

The DataNode update process should have one canonical SDK key:

```text
update_node_uid
```

Runtime code should normalize dependency dataframes at the boundary, then use
`update_node_uid` internally. Do not allow mixed `id`/`uid` columns to spread
through the update runner.

Target names for SDK-facing arguments and local payloads:

```text
update_node_uid
update_node_uids
target_update_node_uid
target_table_uid
data_node_update_uid
data_node_storage_uid
```

## Implementation Gates

These gates state what must be verified before implementation touches a given
call path. They do not assert unknown server behavior.

- Verify the route or client method accepts UID names before renaming a call
  site.
- Verify response rows include enough UID information before changing runtime
  joins.
- Keep SimpleTable row ids separate from TDAG resource UIDs.
- Keep unrelated model ids out of this migration.
- Do not keep an affected TDAG `*_id` parameter as public compatibility API.

## Implementation Task Checklist

These tasks are intentionally explicit. They are not a global `id` to `uid`
replacement list.

### DataNode update-process tasks

- [x] Verify scheduler assignment supports UID-based arguments.
- [x] Update `UpdateRunner._setup_scheduler()` to build scheduler names from
  `self.ts.data_node_update.uid`.
- [x] Update `UpdateRunner._setup_scheduler()` to call
  `Scheduler.build_and_assign_to_update_nodes(...)` with UID-based arguments.
- [x] Remove `update_nodes_ids` from the SDK call path for affected TDAG update
  resources.
- [x] Verify active-tree connection supports UID-based arguments.
- [x] Update `UpdateRunner._pre_update_routines()` so scheduler tree connection
  builds `all_uids` from dependency data and the head `data_node_update.uid`.
- [x] Replace `scheduler.in_active_tree_connect(local_time_series_ids=...)`
  with a UID-based call.
- [x] Verify `get_all_dependencies_update_priority()` returns UID information
  for every dependency row or add a local lookup before runtime joins.
- [x] Add dependency dataframe normalization that creates one canonical
  `update_node_uid` column.
- [x] Remove runtime dependence on `update_node_id`, `local_time_serie_id`, and
  `data_node_update_id` after normalization.
- [x] Update `DataNode.set_dependencies_df()` to exclude the head node with
  `self.data_node_update.uid`.
- [x] Update `UpdateRunner._pre_update_routines()` to build
  `update_nodes_in_tree` records with a `uid` key.
- [x] Update the head-node append in `_pre_update_routines()` to send
  `uid`, `update_hash`, `remote_table_hash_id`, and `node_type`.
- [x] Verify `get_data_nodes_and_set_updates(...)` accepts UID-keyed
  `update_nodes` records.
- [x] Update `data_node_updates_map` to key returned update objects by
  `str(m.uid)`.
- [x] Update `self.ts.update_details_tree` to use the same UID keys as
  `data_node_updates_map`.
- [x] Update `_verify_tree_is_updated()` to collect dependency UIDs from
  `d.data_node_update.uid`.
- [x] Update `_verify_tree_is_updated()` to compare declared dependency UIDs
  against `dependencies_df["update_node_uid"]`.
- [x] Fail loudly when a DataNode dependency lacks `uid`; do not silently fall
  back to `id`.

### DataNode dependency-connection tasks

- [x] Verify `depends_on_connect` accepts `target_update_node_uid`.
- [x] Update `BasePersistManager.depends_on_connect(...)` for DataNode
  dependencies to send `target_update_node_uid=new_ts.data_node_update.uid`.
- [x] Verify API-table dependency connection accepts `target_table_uid`.
- [x] Update `BasePersistManager.depends_on_connect(...)` for API-table
  dependencies to send
  `target_table_uid=new_ts.local_persist_manager.data_node_storage.uid`.
- [x] Rename SDK method arguments that expose `target_time_serie_id` or
  `target_table_id` in affected TDAG public resource paths.

### DataNode execution endpoint tasks

- [x] Confirm `DataNodeUpdate.set_start_of_execution()` uses `_public_uid()`.
- [x] Confirm `DataNodeUpdate.set_end_of_execution()` uses `_public_uid()`.
- [x] Confirm `DataNodeUpdate.set_last_update_index_time_from_update_stats()`
  uses `_public_uid()`.
- [x] Confirm `DataNodeUpdate.verify_if_direct_dependencies_are_updated()` uses
  `_public_uid()`.
- [x] Confirm `DataNodeUpdate.get_all_dependencies_update_priority()` uses
  `_public_uid()`.
- [x] Audit request bodies for these methods and rename any remaining affected
  TDAG resource `*_id` payload fields to `*_uid`.
- [x] Add missing-UID guard tests for each public update endpoint.

### SourceTableConfiguration tasks

- [x] Replace `SourceTableConfiguration.get_data_updates()` usage of
  `self.related_table`.
- [x] Replace `SourceTableConfiguration.get_time_scale_extra_table_indices()`
  usage of `self.related_table`.
- [x] Replace `SourceTableConfiguration.set_or_update_columns_metadata()`
  usage of `self.related_table`.
- [x] Rewrite `SourceTableConfiguration.patch()` so it no longer assumes
  `related_table` is present or has `.id`.
- [x] Add tests proving SourceTableConfiguration methods route through
  `related_table_uid`.

### SimpleTable update-process tasks

- [x] Rename `SimpleTablePersistManager.persist_records()` call from
  `data_node_update_id=self.data_node_update.id` to a UID-based argument.
- [x] Rename `SimpleTableUpdate.insert_records_into_table(...)` parameter
  `data_node_update_id` to `data_node_update_uid`.
- [x] Rename `SimpleTableUpdate.insert_data_into_table(...)` parameter
  `data_node_update_id` to `data_node_update_uid`.
- [x] Update `SimpleTablePersistManager.delete()` to call
  `SimpleTableStorage.delete_records_from_table(data_node_storage_uid=...)`
  with `self.data_node_storage.uid`.
- [x] Keep `records_ids` unchanged because those are SimpleTable row ids.
- [x] Update tests to prove row deletion still uses row ids while table routing
  uses storage UIDs.

### SimpleTable relation and schema tasks

- [x] Audit whether `SimpleTableForeignKeyPayload.target_table` belongs to SDK
  public resource identity or backend metadata.
- [x] If the SDK schema contract should use public table identity, add
  `target_table_uid`.
- [x] Update `SimpleTableUpdater._resolve_simple_table_schema_dict()` to write
  `target_storage.uid` into the resolved schema if the SDK schema contract
  expects public table UIDs.
- [x] Do not change SimpleTable record `id` fields as part of this migration.
- [x] Add tests for foreign-key schema generation with UID targets if that path
  is migrated.

### Client model consistency tasks

- [x] Add or confirm `uid: UUID | None` on every affected public TDAG resource
  model.
- [x] Ensure public-resource route methods use `_public_uid()` and fail loudly
  when `uid` is missing.
- [x] Do not alias `uid` to `id`.
- [x] Remove affected TDAG resource `id` fields from SDK-facing models and
  method signatures.
- [x] Update `BaseObjectOrm.__repr__()` or affected TDAG model reprs so objects
  without `.id` do not fail.
- [x] Update `BaseObjectOrm.__hash__()` or affected TDAG model hashes so
  objects without `.id` do not fail.
- [x] Do not change `BaseObjectOrm._coerce_filter_id()` to accept UIDs.

### Remaining id/uid ambiguity fixes

- [ ] Update `BaseObjectOrm.patch()` so it resolves the object detail reference
  by `uid` when the instance has a public `uid`, and falls back to `id` only
  for models that still use integer database primary keys as their public
  detail reference.
- [ ] Update `BaseObjectOrm.delete()` with the same identity resolution:
  prefer `uid` when present, otherwise use `id`.
- [ ] Update `DetailActionObjectMixin.get_detail_url()` with the same
  custom object-reference resolution. This is required because
  `LabelableObjectMixin` and `ShareableObjectMixin` build their action URLs
  from `get_detail_url()`.
- [ ] Confirm `delete_simple_table_storage()` works through the generic
  resolution above. It fetches `SimpleTableStorage` by `uid` and then calls
  `storage.delete()`, so it must not require `.id`.
- [ ] Confirm CLI label helpers for affected TDAG storages work through the
  generic resolution above. The CLI fetches `SimpleTableStorage` and
  `DataNodeStorage` by `uid`, then calls label/share instance methods whose
  detail-action URLs must also use `uid`.
- [ ] Update CLI sharing output so affected TDAG resources do not prefer
  `object_id` over `object_uid`. Public display should prefer `object_uid`
  when present and only show `object_id` for unrelated id-based models.
- [ ] Update project data-node update list rendering so nested
  `data_node_storage` display prefers `uid` over `id`. Project itself is not
  the affected identity here; the nested TDAG storage reference is.
- [ ] Fix `SimpleTableUpdater` schema-resolution documentation in
  `table_nodes.py` so it says canonical `SimpleTableStorage.uid`, not
  `SimpleTableStorage.id`.
- [ ] Extend deterministic-hash exclusion lists to remove UID-shaped backend
  identity keys as well as old id-shaped keys. At minimum exclude
  `storage_uid`, `update_uid`, `data_node_storage_uid`, and
  `data_node_update_uid` wherever the corresponding `*_id` keys are already
  excluded.

### CLI and docs tasks

- [x] Update DataNode CLI argument names from `storage_id` to `storage_uid`.
- [x] Remove `int(...)` coercion from CLI paths that now accept public UIDs.
- [x] Update SimpleTable CLI argument names from `storage_id` or
  `simple_table_id` to `storage_uid` or `simple_table_uid`.
- [x] Do not present numeric ids as public compatibility behavior for affected
  TDAG resources.
- [x] Update `docs/knowledge/data_nodes.md` examples from `get(pk=123)` to
  UID-based access.
- [x] Update `docs/knowledge/simple_tables/simple_table.md` examples from
  `get(pk=123)` and `{simple_table_id}` to UID-based access.
- [x] Update tutorial examples to use `<DATA_NODE_STORAGE_UID>`,
  `<SIMPLE_TABLE_UID>`, and `.uid` fields for affected TDAG storage references.
- [x] Update examples under `examples/data_nodes` that pass `.id` from
  DataNodeStorage, DataNodeUpdate, SimpleTableStorage, or SimpleTableUpdate.

### Test tasks

- [x] Add unit tests for UID-based scheduler assignment.
- [x] Add unit tests for UID-based active-tree connection.
- [x] Add unit tests for dependency dataframe normalization.
- [x] Add unit tests for `UpdateRunner._pre_update_routines()` UID-keyed
  `update_nodes_in_tree`.
- [x] Add unit tests for UID-keyed `data_node_updates_map`.
- [x] Add unit tests for `_verify_tree_is_updated()` UID dependency comparison.
- [x] Add unit tests for SimpleTable update insert using update UID.
- [x] Add unit tests for SimpleTable delete routing using storage UID and row
  ids in the body.
- [x] Add CLI tests showing UID strings are accepted without integer coercion.
- [ ] Add docs or snapshot checks if the docs build supports them.

## Non-Goals

This ADR does not:

- replace unrelated model ids
- change SimpleTable row identity
- rename logical identifiers such as `identifier`, `node_unique_identifier`,
  `storage_hash`, or `update_hash`
- preserve affected TDAG `id` references as public SDK compatibility behavior

## Risks

### Risk: DataNode update joins are partially migrated

If scheduler assignment, dependency dataframe filtering, dependency rebuild
checks, and update maps do not use the same UID key, update execution may become
unstable.

### Risk: SimpleTable row ids are confused with table UIDs

SimpleTable row operations use row identifiers. Those should not be renamed as
part of TDAG resource UID migration.

### Risk: Generic ORM identity behavior changes unrelated models

Changing `BaseObjectOrm` globally can affect models outside this migration.
Prefer targeted TDAG behavior unless the generic change is proven safe.

## Consequences

The SDK will expose `uid` as the only stable public reference for affected TDAG
resources. For those resources, `id` is not part of the client SDK contract.

Implementation remains an analysis-driven migration rather than a broad rename.
That avoids corrupting SimpleTable row operations, dependency graph joins, and
unrelated model APIs.

The final public API should make identity explicit:

- use `uid` for affected TDAG resource references
- keep `id` only where it means SimpleTable row identity or unrelated models
- use logical names and hashes for deterministic TDAG configuration identity
