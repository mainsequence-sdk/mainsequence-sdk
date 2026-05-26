# ADR 0007: Client-Wide UID Public Identity

## Status

Accepted

## Related ADRs

- ADR 0006: TDAG Public UID Identifiers

## Context

The SDK has been migrating from integer resource identifiers to stable UID-based public identifiers. ADR 0006 covered the TDAG-specific part of that work. This ADR defines the client-wide rule.

The important distinction is semantic, not textual:

- `uid` is the public SDK resource reference.
- `id` is not a public SDK resource reference.
- `id` may still appear only when it means something else, such as a provider identifier, a permission subject identifier that has not migrated, or backend-internal metadata that SDK callers do not use for resource lookup.

The migration must not be implemented as a broad rename. Every use must be classified before it is changed because some `id` fields are valid domain data and some `_id` fields are stale public resource references that must be removed.

## Decision

The SDK public resource identity is `uid`.

Public SDK lookup, patch, delete, label, share, CLI resource arguments, tutorials, and examples must use UID references for SDK resources.

Integer `id` is not a public compatibility path for SDK resources. If an endpoint still requires integer resource ID, that endpoint is not migrated and must be tracked as a blocker or hidden behind an internal adapter. It must not be documented as a public SDK lookup contract.

The final SDK contract is:

- Use `uid` for public SDK resource references.
- Use row `id` only for row-level table data.
- Use provider `id` only for external provider identifiers.
- Use permission subject identifiers only where the backend permission contract explicitly requires them.
- Use deterministic names, hashes, and configuration keys for deterministic TDAG identity.
- Do not infer that any field named `id` is a resource reference.
- Do not infer that any field ending in `_id` can be renamed without payload analysis.

## Non-negotiable implementation rules

- No public resource method should require `.id` on a migrated SDK model.
- No generic detail action should fail only because a migrated SDK model has `uid` and no `id`.
- No CLI resource argument should coerce UID values with `int(...)`.
- No public example should teach users to lookup SDK resources by integer ID.
- No deterministic hash should include backend-generated resource UID fields unless that UID is explicitly part of business configuration.
- No permission subject payload should be changed until the backend subject identity contract is confirmed.

## Scope

In scope:

- `mainsequence/client/base.py`
- maintained `BaseObjectOrm` subclasses
- TDAG resource models
- MetaTable resource models
- project and infrastructure resource models
- command-center resource models
- agent and runtime resource models
- CLI commands that accept or display resource references
- SDK request and response models that expose resource identity
- hand-written docs, tutorials, and examples
- generated reference docs after public signatures change
- tests proving UID-only behavior

Out of scope:

- external provider identifiers
- backend-internal metadata not accepted as SDK lookup input
- permission subject identifiers until their backend contracts migrate
- database primary keys that are only stored or echoed and never used as public SDK lookup arguments

## Required final state

- Every public SDK resource lookup uses UID.
- Every public resource mutation URL is built from UID.
- Every public label and share action URL is built from UID.
- Every public CLI resource option accepts UID strings without integer coercion.
- Every public table and detail view prints UID as the primary resource reference.
- Every UID-migrated response model works when the backend sends UID-only resource identity.
- Every stale `_id` request key is replaced only after confirming it is resource identity.
- Every valid non-resource `id` remains explicitly documented as non-resource identity.
- Every remaining id-based resource endpoint is tracked as a backend blocker, not preserved as public SDK behavior.

## Current implementation already completed

- [x] `BaseObjectOrm` has `PUBLIC_LOOKUP_FIELD = "uid"` as the default public detail identity policy.
- [x] `BaseObjectOrm` has a public detail reference helper.
- [x] Generic instance `patch()` uses the public detail reference helper.
- [x] Generic instance `delete()` uses the public detail reference helper.
- [x] `DetailActionObjectMixin.get_detail_url()` uses the public detail reference helper.
- [x] Base UID-named wrappers exist for `get_by_uid()`, `patch_by_uid()`, and `destroy_by_uid()`.
- [x] Public id-named base aliases are removed; internal-only `_patch_by_id_compat()` and `_destroy_by_id_compat()` shims emit `DeprecationWarning`.
- [x] Id-only migrated resource instances fail before making patch, delete, and generic detail-action requests.
- [x] UID filter normalization exists for resource-reference filters.
- [x] CLI share output prefers `object_uid` over `object_id`.
- [x] Project data-node update list rendering prefers nested storage `uid`.
- [x] MetaTable documentation uses `MetaTable.uid` for registered table references.
- [x] Backend-verified TS Manager storage/update filters use `data_source__uid` and `remote_table__data_source__uid`.
- [x] Local TS Manager update creation uses `current_project_uid` and `data_source_uid`.

## Required implementation tasks

### 1. Base client contract

- [x] Audit direct URL builders in the base helpers and currently migrated model groups for `.id`, `id`, `pk`, `instance_id`, or `object_id` resource lookup.
- [ ] Audit remaining direct URL builders in CLI, user/team, command-center, and agent/runtime models.
- [x] Replace public wording in the base helper docstrings that says primary key or ID lookup with UID lookup.
- [ ] Replace remaining public CLI/docs wording that still teaches ID lookup for resource references.
- [x] Remove public documentation for `patch_by_id()` and `destroy_by_id()` as resource APIs.
- [x] Decide whether id-named methods remain temporarily as UID-taking aliases or are removed.
- [x] Add deprecation notices for internal id-compatibility shims that remain during transition.
- [x] Add a failure path for migrated resource models that have no `uid`.
- [x] Add tests proving id-only migrated resource objects do not silently route by integer ID.

Base decision:

- Public `patch_by_id()` and `destroy_by_id()` are not retained as public base APIs.
- Internal `_patch_by_id_compat()` and `_destroy_by_id_compat()` route through public-reference internals and emit `DeprecationWarning`.
- Normal instance methods use UID helpers and do not call deprecated aliases.
- Remaining model-specific direct URL builders are not accepted as final state; they are tracked in the relevant model-group sections below.

Base audit findings that remain for model-group migration:

- `mainsequence/client/models_helpers.py` no longer builds Job or JobRun detail URLs from `.id`; CLI wrappers still need separate cleanup where their arguments are named `job_id` / `job_run_id`.
- `mainsequence/client/models_tdag.py` still has some local runtime/file-cache paths named `data_source_id`; backend API lookups for migrated TS Manager storage/update resources now use data-source UID.
- `mainsequence/client/agent_runtime_models.py` still has runtime/session helper routes that require classification before renaming.
- `mainsequence/client/models_user.py` still has user detail paths and permission subject identifiers that require separate user/team contract confirmation.

### 2. Model inventory

- [ ] Generate an inventory of every maintained `BaseObjectOrm` subclass.
- [ ] Record the endpoint path for each model.
- [ ] Record whether each model has a `uid` field.
- [ ] Record whether each model supports get, patch, delete, label, or share.
- [ ] Record whether backend detail routes accept UID.
- [ ] Record every field named `id` or ending in `_id`.
- [ ] Classify each `id` usage as resource identity, row identity, provider identity, permission subject identity, or backend metadata.
- [ ] Mark any resource model without UID detail support as a blocker.
- [ ] Fail the inventory if a public migrated resource method requires integer ID.

### 3. TDAG migration

- [x] Confirm UID detail lookup for `DataNodeStorage`.
- [x] Confirm UID detail lookup for `DataNodeUpdate`.
- [x] Confirm UID detail lookup for `DataNodeUpdateDetails`.
- [x] Confirm UID-scoped lookup for `SourceTableConfiguration`.
- [ ] Confirm UID detail lookup for scheduler and run-configuration resources before changing their public arguments.
- [x] Remove public TS Manager storage/update data-source filters named `*_id` where they are resource references.
- [x] Update `DataNodeUpdate.get_or_create()` transport to send `current_project_uid` and `data_source_uid`.
- [x] Replace verified TS Manager public request/filter keys ending in `_id` with UID keys (`current_project_uid`, `data_source_uid`, `remote_table__data_source__uid`).
- [ ] Continue auditing unrelated TDAG runtime/local cache `_id` keys before changing them.
- [x] Keep TDAG logical names and hashes separate from backend UID identity.
- [x] Add a focused test proving `DataNodeUpdate.get_or_create()` uses `current_project_uid` and does not send `current_project_id`.
- [ ] Add tests proving the remaining DataNode update detail/action methods work with `uid` and no `.id`.
- [ ] Add tests proving DataNode patch, delete, label, and share URLs use UID.

### 4. MetaTable migration

- [x] Add `MetaTable` client models and registration contracts.
- [x] Confirm `MetaTable` uses `uid` for registered table references.
- [x] Confirm backend `MetaTableViewSet` uses UID detail lookup.
- [x] Remove `data_source__id` from the client MetaTable filter contract.
- [x] Keep `data_source__uid` as the only public data-source filter for MetaTable.
- [x] Add tests proving `data_source__uid` normalizes and `data_source__id` is rejected.
- [x] Add SQLAlchemy helpers that produce `MetaTableRegistrationRequest` payloads.
- [x] Add governed compiled SQL operation helpers with declared table scope.
- [x] Add tests proving registration and operation payloads use `meta_table_uid`.
- [x] Update tutorials and examples to use backend-managed `MetaTable`s for row-oriented application data.

### 5. Project and infrastructure migration

- [x] Confirm UID detail lookup for `Project`.
- [x] Confirm UID detail lookup for `ProjectImage`.
- [x] Confirm UID detail lookup for `ProjectResource`.
- [x] Confirm UID detail lookup for `ResourceRelease`.
- [x] Confirm UID detail lookup for `Job`.
- [x] Confirm UID detail lookup for `JobRun`.
- [x] Confirm UID detail lookup for `Bucket`.
- [x] Confirm UID detail lookup for `Artifact`.
- [x] Confirm UID detail lookup for `Secret`.
- [x] Confirm UID detail lookup for `Constant`.
- [x] Ensure `Secret`, `Constant`, `Bucket`, and `Artifact` SDK models accept UID identity payloads.
- [x] Replace Constant API wrappers so public fetch, delete, share, and label paths use UID lookup instead of integer ID coercion.
- [x] Replace Constant CLI resource arguments and rendering with UID-based identity.
- [x] Replace SDK model fields and create payloads for `Project`, `ProjectImage`, `ProjectResource`, `ResourceRelease`, `Job`, and `JobRun` with UID-based resource references.
- [x] Remove `MAIN_SEQUENCE_PROJECT_ID` fallback from the local project resolver.
- [x] Add SDK tests proving project/job model payloads and UID-only response shapes for the migrated model subset.
- [ ] Replace public CLI resource parameters such as `project_id`, `resource_id`, `job_id`, and `artifact_id` where they are resource references.
- [ ] Add tests proving project and job commands accept UUID-like values without integer coercion.

### 6. Command-center migration

- [x] Confirm backend UID detail lookup for `Workspace`.
- [x] Confirm backend UID detail lookup for `ConnectionInstance`.
- [ ] Confirm UID detail lookup for every remaining command-center model that inherits generic detail actions.
- [x] Replace public workspace resource arguments with `workspace_uid`; connection resource arguments remain separately tracked.
- [x] Confirm `ConnectionType.type_id` remains a non-UID public type identifier.
- [ ] Keep provider connection IDs unchanged where they are external identifiers.
- [x] Add tests for UID-only workspace response payloads; remaining command-center models stay separately tracked.

### 7. Agent and runtime migration

- [x] Inventory agent and runtime models before changing field names.
- [x] Confirm backend UID detail lookup for `Agent`.
- [x] Confirm backend UID detail lookup for `AgentSession`.
- [x] Confirm backend UID detail lookup for `UserOrchestratorAgentService`.
- [x] Confirm backend UID detail lookup for `UserProjectExecutorAgentService`.
- [x] Replace public id-named arguments only where they are SDK resource references.
- [x] Keep runtime execution IDs and provider IDs unchanged where they are domain identifiers.
- [x] Add tests for UID-only agent and runtime responses.

Agent/runtime decisions:

- `Agent.uid` is the backend detail-route public UID and the generic SDK/CLI resource identifier.
- `Agent.agent_unique_id` is the deterministic organization-scoped key used by create/get_or_create and the explicit `Agent.get_by_agent_unique_id(...)` helper. It is not the same field as `uid` and must not replace generic resource UID lookup.
- `AgentSession.uid` is the public session lookup key.
- `UserOrchestratorAgentService.uid` and `UserProjectExecutorAgentService.uid` are the public service lookup keys; related agents are exposed as `agent_uid`.
- A2A allocation uses `caller_agent_session_uid` in requests and `agent_session_uid` in responses.
- Runtime identifiers such as `coding_agent_id` are not SDK resource identities and are not renamed.
- Permission subject ids in share/access commands remain as-is until the backend permission subject contract changes explicitly.

### 8. User, team, organization, and permission subjects

- [x] Inventory user and organization models separately from resource models.
- [x] Confirm backend `User`, `OrganizationTeam`/`Team`, and `Organization` expose UID detail routes.
- [x] Migrate SDK `Organization`, `User`, and `Team` public lookup/filter contracts to UID.
- [x] Migrate SDK team membership management to send `user_uids`.
- [x] Keep request-bound auth status/header handling on `X-User-ID`; this is not a public resource lookup contract.
- [ ] Do not change permission subject payloads until backend subject identity is confirmed.
- [ ] If permission subjects migrate, replace subject `id` with subject `uid` in share payloads and docs.
- [ ] If permission subjects do not migrate, document them as non-resource subject identifiers.
- [x] Add focused tests for UID user/team/org resource lookups and payload deserialization.
- [ ] Add broader tests that distinguish resource UID from permission subject identifiers.

### 9. CLI migration

- [ ] Replace public CLI resource option names ending in `_id` with `_uid`.
- [x] Replace Constant CLI resource argument names from `constant_id` to `constant_uid`.
- [ ] Stop coercing resource lookup options with `int(...)`.
- [x] Stop coercing Constant resource lookup arguments with `int(...)`.
- [x] Stop coercing generic share/label resource object references with `int(...)` by default.
- [ ] Print `uid` as the primary resource identity in resource tables.
- [x] Print `uid` as the primary resource identity in Constant tables.
- [ ] Print `uid` as the primary resource identity in resource detail views.
- [ ] Remove integer resource IDs from public CLI examples.
- [x] Remove integer Constant resource IDs from Constant CLI examples.
- [ ] Keep non-resource subject IDs only where backend permission contracts require them.
- [ ] Add CLI tests proving UUID-like resource values are accepted.
- [x] Add Constant CLI tests proving UUID-like resource values are accepted.
- [ ] Add CLI tests proving migrated commands do not prefer `object_id` over `object_uid`.
- [x] Add Constant CLI tests proving migrated commands use `object_uid`.

### 10. Request and response typing

- [ ] Audit response models that expose `object_id`.
- [ ] Replace resource identity response fields with `object_uid` or typed `object_reference`.
- [x] Replace Constant share test payloads with `object_uid`.
- [ ] Audit request models that expose `dynamic_table_id`, `storage_id`, `update_id`, or similar keys.
- [x] Replace verified TS Manager/project/job resource request keys with UID keys after backend contract verification.
- [ ] Continue auditing unverified resource request keys before changing them.
- [x] Remove code that assumes UID and ID keys both exist for migrated project/job/TS Manager resource paths.
- [x] Remove code that prefers `id` over `uid` for migrated SDK models and the local project resolver.
- [x] Add tests for UID-only response shapes in the migrated project/job subset.

### 11. Documentation and tutorials

- [ ] Update ADR 0006 to reference this ADR for client-wide identity rules.
- [ ] Update hand-written SDK docs to use UID lookup examples.
- [ ] Update TDAG tutorials to use `storage.uid`, `update.uid`, and table UID references.
- [ ] Update CLI docs to use `--*_uid` options for resource references.
- [ ] Update SDK examples to use `get_by_uid()`, `patch_by_uid()`, and `destroy_by_uid()`.
- [ ] Regenerate reference docs after public method and signature changes.
- [ ] Remove stale generated examples that expose `storage_id` or `dynamic_table_id` as public resource lookup examples.

### 13. Tests and acceptance criteria

- [x] Add base tests for `PUBLIC_LOOKUP_FIELD = "uid"`.
- [x] Add base tests for missing UID errors.
- [x] Add base tests proving patch, delete, and generic share/detail-action URLs use UID.
- [ ] Add explicit base tests proving label action URLs use UID.
- [x] Add tests proving id-only migrated resource objects fail.
- [ ] Add model inventory tests for UID coverage.
- [x] Add endpoint URL tests for the migrated Job/JobRun and TS Manager get-or-create paths.
- [ ] Add CLI tests for UUID-like resource arguments.
- [x] Add response-shape tests for UID-only payloads in the migrated subset.

## Migration sequence

1. Keep the base identity rule UID-first and policy-driven.
2. Build the model inventory and classify every `id` usage.
3. Migrate models group by group.
4. Migrate CLI arguments and output for the same group in the same implementation step.
5. Migrate request and response typing for UID-only backend payloads.
6. Add tests for the migrated group before marking it complete.
7. Update tutorials and hand-written docs after behavior is migrated.
8. Regenerate reference docs after public signatures are final.
9. Track every remaining id-based resource endpoint as a blocker.
10. Remove temporary id-named aliases once downstream usage has migrated.

## Completion gates

A model group is complete only when all of the following are true:

- Public lookup accepts UID.
- Public patch and delete use UID.
- Public label and share actions use UID if the model supports them.
- Public CLI commands accept UID and do not coerce to integer.
- Public docs and tutorials use UID.
- Tests cover UID-only response payloads.
- Tests cover missing `.id` on migrated objects.
- Any remaining `id` fields in the group are documented as non-resource identity.

The ADR is complete when all model groups either satisfy the gates or have explicit blockers with owner, backend endpoint, and removal condition.

## Consequences

The SDK has one public resource identity model: UID.

This intentionally breaks callers that still pass integer database IDs as SDK resource lookup arguments. That break is required because integer resource lookup is not part of the client public contract anymore.

The migration remains analysis-driven because `id` still has valid non-resource meanings. Keeping that distinction explicit prevents damage to permission payloads, provider integrations, and TDAG configuration hashes.

## Risks

- Backend UID detail routes may be incomplete for some resources.
- Under-typed response models may hide assumptions that `id` exists.
- Permission subject payloads may still use identifiers that are not resource UID.
- Generated docs may continue to expose stale signatures until regenerated.
- Id-named compatibility aliases may confuse users unless removed or clearly deprecated.
