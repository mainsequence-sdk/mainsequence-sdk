# ID to UID Refactor

## Goal

Make the public SDK and CLI contract UID-only.

Target state:

- Public model payloads do not require or advertise numeric `id`.
- Public detail lookups use `uid`.
- Public filters use `uid`-based field names instead of `__id`.
- Public create/update helper payloads use `*_uid` keys instead of `*_id`.
- Local runtime env, local hashing, and CLI rendering do not leak numeric row ids.

This document is an implementation task list. It is not a design note for keeping mixed `id`/`uid` compatibility indefinitely.

## Migration rules

1. Public identifier fields
   - Replace public `id` fields with `uid`.
   - If temporary compatibility is required during rollout, keep `id` optional and undocumented, then remove it in a follow-up cleanup.

2. Detail lookups
   - `BaseObjectOrm.get(pk=...)` should resolve public UID references.
   - Any model overriding `PUBLIC_LOOKUP_FIELD` to `"id"` must be audited and migrated unless the backend contract explicitly remains numeric.

3. Filters
   - Replace `field__id` with `field__uid`.
   - Remove `FILTER_VALUE_NORMALIZERS` entries that normalize `"id"`.

4. Request payloads
   - Replace `project_id`, `related_project_id`, `data_source_id`, `current_project_id`, etc. with `project_uid`, `related_project_uid`, `data_source_uid`, `current_project_uid`, etc.

5. CLI and docs
   - Commands, help text, examples, and rendered tables must show `UID`, not `ID`, when the public object contract is UID-based.

6. Validation
   - Every migration slice needs:
   - model deserialization test for backend public serializer payloads
   - filter normalization test
   - CLI rendering / CLI command test where applicable

## Recommended order

- [ ] Foundation in `mainsequence/client/base.py` (partial: UID detail routing and UID coercion are done; base `id` fallback and remaining family audits are not)
- [x] Shared helper layer in `mainsequence/client/models_helpers.py`
- [ ] Project and data-source family in `mainsequence/client/models_tdag.py` (partial: Project, ProjectImage, TS Manager storage/update, Job-adjacent payloads, and selected infra models are migrated; DynamicResource, remaining runtime internals, CLI, and full tests are not)
- [x] Meta tables in `mainsequence/client/models_metatables.py`
- [x] User/org/team family in `mainsequence/client/models_user.py`
- [x] Agent runtime family in `mainsequence/client/agent_runtime_models.py`
- [ ] Command Center models in `mainsequence/client/command_center/`
- [ ] CLI cleanup after model contracts are explicit

## Implementation tasks

### 1. Foundation: `mainsequence/client/base.py`

Files:

- `mainsequence/client/base.py`

Problems:

- Generic identity fallback still returns `self.id`.
- Generic filter coercion still expects objects with `.id`.
- Base ORM model still exposes `id`.

Tasks:

- [x] Route public detail, patch, delete, label, and share actions through UID-first public reference helpers.
- [ ] Replace generic numeric-id fallback with UID-first identity resolution everywhere, including repr/hash helpers.
- [x] Introduce a generic UID coercer for public object references.
- [ ] Remove or isolate the base `id` field so it is not treated as part of the public contract.
- [x] Audit deprecated helpers like `destroy_by_id` / `patch_by_id` and remove them or move them behind explicit internal-only compatibility shims.

Suggested migration:

- Keep `unique_identifier` and `_public_detail_reference()` as the generic public identity mechanism.
- Add a helper that accepts:
  - string UID
  - object with `.uid`
  - dict with `"uid"`
- Stop teaching the base layer that a public object is identified by `.id`.
- Base repr/hash still allow an `id` fallback for non-migrated models; public detail routing uses `uid` by default.

Validation:

- [x] Add direct tests for UID-only filter normalization and public detail resolution.

### 2. Shared helpers: `mainsequence/client/models_helpers.py`

Files:

- `mainsequence/client/models_helpers.py`

Problems:

- CLI wrappers still need separate cleanup where they expose `job_id`, `job_run_id`, `resource_id`, or `release_id`.
- Public SDK model/helper paths for Job, JobRun, ProjectResource, and ResourceRelease now use UID references.

Tasks:

- [x] Replace public SDK project references in `models_helpers.py` with `project_uid`.
- [x] Replace `project__id` filters with `project__uid`.
- [x] Audit whether backend job-run endpoints already accept UID paths.
- [x] Change JobRun URL construction from `/{self.id}/...` to `/{self.uid}/...`.
- [x] Change Job URL construction from `/{self.id}/...` to `/{self.uid}/...`.
- [x] Replace Job and batch-job payloads with `project_uid` and `related_image_uid`.

Suggested migration:

- Done for SDK models: add `uid` to `Job`, `JobRun`, `ProjectResource`, and related models.
- Done for SDK models: replace `_resolve_project_id(...)` with `_resolve_project_uid(...)`.
- Done for SDK models: rename outgoing payload keys to UID-based names after backend endpoint verification.

Validation:

- [x] Add deserialization tests for UID-based job / job-run payloads.
- [x] Add tests for helper payload keys to confirm no `project_id` leaks remain.

### 3. Project family: `mainsequence/client/models_tdag.py`

Files:

- `mainsequence/client/models_tdag.py`

Problems still present:

- Some TDAG runtime internals still use `data_source_id` for local file-cache paths and in-memory runtime joins; those are not public backend API filters and require a separate runtime migration.
- `DynamicResource` and a few local physical-data-source helpers still expose numeric ids and require backend contract confirmation before changing.

Tasks:

- [x] Remove `id` from `Project` and `ProjectQuickSearchResult`.
- [x] Migrate `GithubOrganization` and `ProjectBaseImage` after backend serializer verification.
- [x] Replace `ProjectImage.related_project__id` filter and `related_project_id` payload with UID equivalents.
- [x] Replace TS Manager storage/update data-source-id filters and payloads with UID equivalents.
- [ ] Replace `DynamicResource.id` with `uid` if backend serializers already support it.

Suggested migration:

- Split the file by object family and migrate one serializer contract at a time:
  - [x] project
  - [x] project images
  - [x] public data-source / data-node-update API paths
  - [x] buckets / artifacts / secrets / constants client model fields
  - [ ] buckets / artifacts / constants serializer-shape tests and CLI cleanup
  - [ ] dynamic resources
- For each family, update:
  - [x] migrated model fields for project, project images, TS Manager storage/update, Job/JobRun, ProjectResource, ResourceRelease, Bucket, Artifact, Secret, and Constant.
  - [x] migrated filter sets for verified project/job/TS Manager storage/update paths.
  - [x] migrated payload builders for verified project/job/TS Manager storage/update paths.
  - [ ] finish docs/docstrings for every project/data-source-family model.
  - [ ] finish tests for every project/data-source-family model.

Validation:

- [x] Add public payload deserialization tests for the migrated project/job groups.
- [x] Add project image model tests proving that no `related_project_id` or `related_project__id` is required.

### 4. Meta tables: `mainsequence/client/models_metatables.py`

Files:

- `mainsequence/client/models_metatables.py`

Problems:

- Done: `MetaTable` no longer supports `data_source__id` filters in the client.

Tasks:

- [x] Remove `data_source__id`.
- [x] Keep only `data_source__uid`.

Suggested migration:

- Update `FILTERSET_FIELDS` and `FILTER_VALUE_NORMALIZERS`.
- Remove numeric-id examples and docs for metatable filtering.

Validation:

- [x] Add filter normalization tests for `data_source__uid`.
- [x] Add a negative test confirming `data_source__id` is rejected.

### 5. User / organization / team family: `mainsequence/client/models_user.py`

Files:

- `mainsequence/client/models_user.py`

Resolved:

- `Organization`, `User`, `UserSummary`, `ShareableTeamSummary`, and `Team` now expose UID as the public identity; numeric row ids are accepted only as hidden/internal fields where the backend still echoes them.
- `Team` no longer overrides `PUBLIC_LOOKUP_FIELD = "id"`; detail actions route by UID.
- `Team` membership management posts `user_uids` to the backend contract.
- Request-bound auth remains on `X-User-ID`; the backend auth status/header contract is not part of this public resource UID migration.

Tasks:

- [x] Migrate public user, organization, and team models to UID-based lookups.
- [x] Remove `PUBLIC_LOOKUP_FIELD = "id"` where backend public routes now require UID.
- [x] Replace any numeric-id filters with UID filters.

Suggested migration:

- Use `/user/api/user/get_user_details/` and `/user/api/user/{user_uid}/` as the source of truth for public user resources.
- Treat numeric user ids only as temporary internal compatibility if backend still emits them somewhere.
- Keep request-bound auth status/header handling on `X-User-ID` until the backend auth contract changes explicitly.

Validation:

- [x] Add deserialization tests for the backend’s current user and full user profile payloads.
- [x] Add UID-based detail lookup tests.

### 6. Agent runtime family: `mainsequence/client/agent_runtime_models.py`

Files:

- `mainsequence/client/agent_runtime_models.py`

Problems:

- Done: `AgentSemanticSearchResult` uses `uid`, not numeric `id`.
- Done: `Agent` exposes no numeric `id`; generic SDK resource lookup, mutation, and detail actions use `uid`.
- Done: `AgentSession` uses `uid` for public routing and `agent_uid` for the related agent.
- Done: `UserOrchestratorAgentService` and `UserProjectExecutorAgentService` use service `uid` and related `agent_uid`.
- Done: `agent_unique_id` remains a separate deterministic agent key and is not confused with `uid`.

Tasks:

- [x] Replace public numeric ids with the backend’s canonical public identifiers.
- [x] For agents, keep `agent_unique_id` only for deterministic create/get_or_create and the explicit `get_by_agent_unique_id(...)` helper.
- [x] For sessions and services, confirm the backend public serializer and migrate to UID-based routing where the client exposes resource lookup.

Suggested migration:

- Confirmed backend contracts:
  - [x] agent
  - [x] agent session
  - [x] orchestrator service
  - [x] executor service
- Agent list/search/detail payloads include `uid`; detail, delete, sharing, latest-session, and A2A CLI resource flows use `agent_uid`.
- Deterministic `agent_unique_id` remains available for create/get_or_create and `Agent.get_by_agent_unique_id(...)` only.
- Agent sessions and runtime-access actions use `AgentSession.uid`.
- A2A allocation sends `caller_agent_session_uid` and reads `agent_session_uid` from the response.
- Orchestrator and project-executor services deserialize `uid` and `agent_uid` payloads.
- Runtime/provider identifiers such as `coding_agent_id` remain unchanged.
- Permission subject ids in share commands remain unchanged until the backend permission subject contract migrates.

Validation:

- [x] Add deserialization tests for search/list/detail payloads.
- [x] Add CLI command tests for the updated identifiers once the client contract is explicit.

### 7. Command Center family: `mainsequence/client/command_center/workspace.py`

Files:

- `mainsequence/client/command_center/workspace.py`

Problems:

- [x] `Workspace` no longer models the backend row `id`; the SDK uses `uid`.
- [x] Widget-scoped comments now describe `widget.id` as the mounted widget instance id, not a workspace/resource id.

Tasks:

- [x] Confirm backend workspace public routes are UID-based.
- [x] If UID-based, migrate `Workspace.id`.
- [x] If UID-based, migrate workspace filters.
- [x] If UID-based, migrate widget reference handling.

Migration result:

- Workspace detail, update, delete, label, and snapshot CLI paths accept `workspace_uid`.
- Workspace filters use `uid`, `uid__in`, and `exclude_uids`; legacy `id` filters are rejected.
- Mounted widgets still use `widget.id` / `widget_instance_id` because that is the backend workspace-document instance key.

Validation:

- [x] Add workspace list/detail deserialization tests with public payloads.
- [x] Add filter tests for UID-based workspace references.

### 8. Command Center connections: `mainsequence/client/command_center/connections.py`

Files:

- `mainsequence/client/command_center/connections.py`

Resolved:

- [x] `ConnectionInstance` now exposes `uid` as the public resource identity and no longer models backend row `id`.
- [x] Connection filters use the backend public query contract: `type_id`, `status`, `workspace_uid`, `is_default`, and `is_active`.

Tasks:

- [x] Confirm the backend public connection-instance identifier is `uid`.
- [x] Replace numeric `id` with the canonical UID-style identifier.
- [x] Keep `ConnectionType.type_id` because that remains the actual public contract.

Migration result:

- Connection instance detail lookup uses `uid`.
- Connection instance list filters send `workspaceUid`, not `workspace_id`.
- `ConnectionType.type_id` remains unchanged because it is a catalog type key, not a backend row id.

Validation:

- [x] Add deserialization tests for connection-type and connection-instance detail payloads.
- [x] Add CLI alignment tests after the client model is updated.

### 9. Secrets / constants / artifacts / buckets cleanup

Files:

- `mainsequence/client/models_tdag.py`
- `mainsequence/cli/api.py`
- `mainsequence/cli/cli.py`

Current state:

- `Secret`, `Constant`, `Artifact`, and `Bucket` expose `uid` as the public resource identity in the client models.
- Constant CLI/API resource lookup has been migrated from `constant_id` to `constant_uid`.
- Secret CLI/API resource lookup has been normalized from `secret_id` naming to `secret_uid` naming where the value is a resource UID.
- `Artifact` and `Bucket` have UID model identity; there are no dedicated artifact/bucket CLI command families in this section to migrate.

Tasks:

- [x] Confirm backend serializers/routes for `Constant`.
- [x] Confirm backend serializers/routes for `Artifact`.
- [x] Confirm backend serializers/routes for `Bucket`.
- [x] Replace public `id` fields and CLI rendering with UID-based identity if backend is already UID-only there too.

Suggested migration:

- Reuse the `Secret` migration shape:
  - [x] model accepts `uid`
  - [x] API wrappers stop coercing resource references to `int`
  - [x] CLI renders `UID`

Validation:

- [x] Add one test per model for UID-based deserialization.
- [x] Add one CLI test per command family after migration.

### 10. Local runtime and persistence payloads

Files:

- `mainsequence/client/models_tdag.py`
- `mainsequence/tdag/data_nodes/build_operations.py`
- related runtime helpers

Current state:

- project runtime env uses `MAIN_SEQUENCE_PROJECT_UID`; `MAIN_SEQUENCE_PROJECT_ID` is not used as a fallback
- TS Manager local update creation sends `current_project_uid` and `data_source_uid`
- backend `LocalTimeSerie.get_or_create` resolves project scope by `current_project_uid`
- local pickle markers and pickle directory paths now use `data_source_uid`
- local runtime logger context now uses `local_hash_uid_data_source`
- old serialized pickle markers with `data_source_id` are read only as backwards compatibility

Tasks:

- [x] Audit all runtime payload builders and hashing helpers for leaked numeric identifiers.
- [x] Replace leaked public project/data-source object ids in the TS Manager update creation path with UID-based keys.
- [x] Replace local DataNode pickle markers from `data_source_id` to `data_source_uid`.
- [x] Replace local DataNode pickle paths from data-source numeric id directories to data-source UID directories.
- [x] Replace dependency update-map runtime keys from `(update_hash, data_source_id)` to `(update_hash, data_source_uid)`.
- [x] Replace `get_upstream_nodes` query parameter from `data_source_id` to `data_source_uid`.

Suggested migration:

- For every runtime payload, decide whether the field is:
  - [x] public object reference -> must be UID
  - [x] internal database row reference -> isolate it behind an internal-only adapter layer

Remaining compatibility notes:

- Legacy pickled markers containing `data_source_id` are still read as a fallback so old local pickles can be deserialized.
- Constructor-level compatibility arguments such as `data_source_id` remain only where they are part of older local/API initialization paths and require a separate compatibility-removal pass.

Validation:

- [x] Add local runtime smoke tests using only UID-based env variables and UID-based payload assertions for the TS Manager get-or-create path.
- [x] Add broader local runtime smoke tests for the migrated pickle marker/path builders.

### 11. CLI alignment after client migrations

Files:

- `mainsequence/cli/api.py`
- `mainsequence/cli/cli.py`

Problems:

- Several CLI command families still render `ID`, accept `int` args, or cast references with `int(...)`.
- Some CLI commands are already partly migrated and now sit on mixed client contracts.
- CLI alignment must follow the completed client-family migration. Do not rename unrelated command families until their client models and API wrappers are UID-based.

Implemented for completed UID-migrated families:

- [x] Aligned command arguments for migrated families: `Constant`, `Secret`, UID-fetched TDAG resources, `Workspace`, and `Agent` resource references.
- [x] Aligned help text for migrated families so public resource references are described as UID references.
- [x] Aligned examples for migrated families, including Secret permission commands that still showed numeric secret examples.
- [x] Aligned delete confirmation text for migrated families so deleted public resources report `uid` instead of public `id`.
- [x] Aligned JSON output labels for migrated families so public resource references use `uid` / `object_uid`.
- [x] Aligned table headers for migrated families so public resource columns render `UID`.

Remaining explicit CLI migration work:

- [ ] Continue CLI alignment after the corresponding client-family migration for families that still intentionally use numeric backend references today, including project images, project resources, resource releases, jobs, job runs, organization teams, and agent runs.
- [ ] Re-audit generic CLI helpers after the remaining client families migrate so `int(...)` coercion is limited to non-resource permission subjects or explicitly internal backend fields.

Validation:

- [ ] Add CLI tests for list/detail/create/delete per object family after each remaining family migration.

### 12. Tests and docs cleanup

Files:

- `tests/`
- `docs/`

Problems:

- Many tests still encode numeric-id assumptions.
- Public docs and examples still reference `id` in several families.

Tasks:

- [ ] Replace stale test fixtures that emit numeric ids where backend now emits UIDs.
- [ ] Add serializer-shape tests for every migrated family.
- [ ] Update docs/tutorials/examples to show the public identifier only.

Suggested migration:

- Treat tests as part of the contract migration, not as cleanup afterward.
- Every time a model changes from `id` to `uid`, update:
  - [ ] model fixture
  - [ ] CLI fixture
  - [ ] docs example

## Suggested rollout strategy

- [ ] Finish model foundation first.
- [ ] Migrate one object family at a time.
- [ ] Update CLI only after the client contract for that family is explicit.
- [ ] Remove fallback `id` support as soon as the corresponding backend family is confirmed UID-only.

## Suggested acceptance criteria

- [ ] No public client model in the migrated family requires or advertises numeric `id`.
- [ ] No public filter in the migrated family uses `__id`.
- [ ] No public helper payload in the migrated family sends `*_id`.
- [ ] CLI for the migrated family does not render `ID` or cast identifiers to `int`.
- [ ] Serializer-shape tests pass for the backend’s current UID payload.
