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

- [x] Foundation in `mainsequence/client/base.py`
- [x] Shared helper layer in `mainsequence/client/models_helpers.py`
- [x] Project and data-source family in `mainsequence/client/models_tdag.py`
- [x] Meta tables in `mainsequence/client/models_metatables.py`
- [ ] User/org/team family in `mainsequence/client/models_user.py`
- [ ] Agent runtime family in `mainsequence/client/agent_runtime_models.py`
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

- [ ] Replace generic numeric-id fallback with UID-first identity resolution.
- [ ] Introduce a generic UID coercer for public object references.
- [ ] Remove or isolate the base `id` field so it is not treated as part of the public contract.
- [ ] Audit deprecated helpers like `destroy_by_id` / `patch_by_id` and remove them or move them behind explicit internal-only compatibility shims.

Suggested migration:

- Keep `unique_identifier` and `_public_detail_reference()` as the generic public identity mechanism.
- Add a helper that accepts:
  - string UID
  - object with `.uid`
  - dict with `"uid"`
- Stop teaching the base layer that a public object is identified by `.id`.

Validation:

- [x] Add direct tests for UID-only filter normalization and public detail resolution.

### 2. Shared helpers: `mainsequence/client/models_helpers.py`

Files:

- `mainsequence/client/models_helpers.py`

Problems:

- CLI wrappers still need separate cleanup where they expose `job_id`, `job_run_id`, `resource_id`, or `release_id`.
- Public SDK model/helper paths for Job, JobRun, ProjectResource, and ResourceRelease now use UID references.

Tasks:

- [x] Replace all public SDK project references with `project_uid`.
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
  - [x] data sources / data node updates
  - [x] buckets / artifacts / constants
  - [ ] dynamic resources
- For each family, update:
  - [x] model fields
  - [x] filter sets
  - [x] payload builders
  - [x] docstrings
  - [x] tests

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

Problems:

- `Organization` still exposes both `id` and `uid`.
- Main user models still carry numeric ids.
- `PUBLIC_LOOKUP_FIELD` is still `"id"` for the main user model.

Tasks:

- [ ] Migrate public user, organization, and team models to UID-based lookups.
- [ ] Remove `PUBLIC_LOOKUP_FIELD = "id"` where backend public routes now require UID.
- [ ] Replace any numeric-id filters with UID filters.

Suggested migration:

- Use `/user/api/user/get_user_details/` and `/user/api/user/{user_uid}/` as the source of truth.
- Treat numeric user ids only as temporary internal compatibility if backend still emits them somewhere.

Validation:

- [ ] Add deserialization tests for the backend’s current user and full user profile payloads.
- [ ] Add UID-based detail lookup tests.

### 6. Agent runtime family: `mainsequence/client/agent_runtime_models.py`

Files:

- `mainsequence/client/agent_runtime_models.py`

Problems:

- `AgentSemanticSearchResult.id`
- `Agent.id`
- `AgentSession.id`
- `UserOrchestratorAgentService.id`
- `UserProjectExecutorAgentService.id`

Tasks:

- [ ] Replace public numeric ids with the backend’s canonical public identifiers.
- [ ] For agents, migrate toward `agent_unique_id` if that is the confirmed public contract.
- [ ] For sessions and services, confirm the backend public serializer and migrate to UID-based routing if available.

Suggested migration:

- Do not guess.
- First confirm the canonical public identifier for:
  - [ ] agent
  - [ ] agent session
  - [ ] orchestrator service
  - [ ] executor service
- Then align model fields, detail routes, and CLI output to those identifiers.

Validation:

- [ ] Add deserialization tests for search/list/detail payloads.
- [ ] Add CLI command tests for the updated identifiers once the client contract is explicit.

### 7. Command Center family: `mainsequence/client/command_center/workspace.py`

Files:

- `mainsequence/client/command_center/workspace.py`

Problems:

- `Workspace` still filters and models on `id`.
- Widget comments still assume widget `id` semantics.

Tasks:

- [ ] Confirm whether workspace public routes are still numeric or already UID-based.
- [ ] If UID-based, migrate `Workspace.id`.
- [ ] If UID-based, migrate workspace filters.
- [ ] If UID-based, migrate widget reference handling.

Suggested migration:

- Keep workspace migration separate from agent-session migration.
- Change only after backend contract is explicit.

Validation:

- [ ] Add workspace list/detail deserialization tests with public payloads.
- [ ] Add filter tests for UID-based workspace references.

### 8. Command Center connections: `mainsequence/client/command_center/connections.py`

Files:

- `mainsequence/client/command_center/connections.py`

Problems:

- `ConnectionInstance.id` is still numeric.
- Connection filters still reflect mixed contracts.

Tasks:

- [ ] Confirm the public connection-instance identifier.
- [ ] Replace numeric `id` with the canonical UID-style identifier.
- [ ] Keep `ConnectionType.type_id` if that remains the actual public contract.

Suggested migration:

- Treat connection types and connection instances separately:
  - [ ] type contract may remain `type_id`
  - [ ] instance contract should move to `uid` if backend already exposes it

Validation:

- [ ] Add deserialization tests for connection-type and connection-instance detail payloads.
- [ ] Add CLI alignment tests after the client model is updated.

### 9. Secrets / constants / artifacts / buckets cleanup

Files:

- `mainsequence/client/models_tdag.py`
- `mainsequence/cli/api.py`
- `mainsequence/cli/cli.py`

Current state:

- `Secret` has already been migrated to `uid` in the source tree.
- `Constant`, `Artifact`, and `Bucket` are still numeric-id based in the client.

Tasks:

- [ ] Confirm backend serializers for `Constant`.
- [ ] Confirm backend serializers for `Artifact`.
- [ ] Confirm backend serializers for `Bucket`.
- [ ] Replace public `id` fields and CLI rendering with UID-based identity if backend is already UID-only there too.

Suggested migration:

- Reuse the `Secret` migration shape:
  - [ ] model accepts `uid`
  - [ ] API wrappers stop coercing to `int`
  - [ ] CLI renders `UID`

Validation:

- [ ] Add one test per model for UID-based deserialization.
- [ ] Add one CLI test per command family after migration.

### 10. Local runtime and persistence payloads

Files:

- `mainsequence/client/models_tdag.py`
- `mainsequence/tdag/data_nodes/build_operations.py`
- related runtime helpers

Current state:

- project runtime env uses `MAIN_SEQUENCE_PROJECT_UID`; `MAIN_SEQUENCE_PROJECT_ID` is not used as a fallback
- TS Manager local update creation sends `current_project_uid` and `data_source_uid`
- backend `LocalTimeSerie.get_or_create` resolves project scope by `current_project_uid`
- there are still lingering `data_source_id`, `object_id`, and internal numeric payload names elsewhere

Tasks:

- [ ] Audit all runtime payload builders and hashing helpers for leaked numeric identifiers.
- [x] Replace leaked public project/data-source object ids in the TS Manager update creation path with UID-based keys.

Suggested migration:

- For every runtime payload, decide whether the field is:
  - [ ] public object reference -> must be UID
  - [ ] internal database row reference -> isolate it behind an internal-only adapter layer

Validation:

- [ ] Add local runtime smoke tests using only UID-based env variables and UID-based payload assertions.

### 11. CLI alignment after client migrations

Files:

- `mainsequence/cli/api.py`
- `mainsequence/cli/cli.py`

Problems:

- Several CLI command families still render `ID`, accept `int` args, or cast references with `int(...)`.
- Some CLI commands are already partly migrated and now sit on mixed client contracts.

Tasks:

- [ ] After each client-family migration, align command arguments.
- [ ] After each client-family migration, align help text.
- [ ] After each client-family migration, align examples.
- [ ] After each client-family migration, align delete confirmation text.
- [ ] After each client-family migration, align JSON output labels.
- [ ] After each client-family migration, align table headers.

Suggested migration:

- Do not blindly rename everything to `UID`.
- Use the canonical public identifier for each object family:
  - [ ] `uid`
  - [ ] `agent_unique_id`
  - [ ] `type_id`
  - [ ] other confirmed public identifiers where applicable

Validation:

- [ ] Add CLI tests for list/detail/create/delete per object family.

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
