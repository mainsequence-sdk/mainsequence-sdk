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
- `id` may still appear only when it means something else, such as a SimpleTable row identifier, a provider identifier, a permission subject identifier that has not migrated, or backend-internal metadata that SDK callers do not use for resource lookup.

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
- Use deterministic names, hashes, and configuration keys for deterministic TDAG or VFB identity.
- Do not infer that any field named `id` is a resource reference.
- Do not infer that any field ending in `_id` can be renamed without payload analysis.

## Non-negotiable implementation rules

- No public resource method should require `.id` on a migrated SDK model.
- No generic detail action should fail only because a migrated SDK model has `uid` and no `id`.
- No CLI resource argument should coerce UID values with `int(...)`.
- No public example should teach users to lookup SDK resources by integer ID.
- No deterministic hash should include backend-generated resource UID fields unless that UID is explicitly part of business configuration.
- No SimpleTable row operation should be broken by replacing row `id` with resource `uid`.
- No permission subject payload should be changed until the backend subject identity contract is confirmed.

## Scope

In scope:

- `mainsequence/client/base.py`
- maintained `BaseObjectOrm` subclasses
- TDAG resource models
- SimpleTable resource models
- project and infrastructure resource models
- command-center resource models
- markets resource models
- agent and runtime resource models
- CLI commands that accept or display resource references
- SDK request and response models that expose resource identity
- hand-written docs, tutorials, and examples
- generated reference docs after public signatures change
- tests proving UID-only behavior

Out of scope:

- SimpleTable row mutation identifiers
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
- [x] Deprecated id-named base aliases remain only as transition shims and emit `DeprecationWarning`.
- [x] Id-only migrated resource instances fail before making patch, delete, label, or share requests.
- [x] CLI share output prefers `object_uid` over `object_id`.
- [x] Project data-node update list rendering prefers nested storage `uid`.
- [x] VFB deterministic hash exclusions include `storage_uid`, `update_uid`, `data_node_storage_uid`, and `data_node_update_uid`.
- [x] SimpleTable source-table configuration documentation refers to `SimpleTableStorage.uid`.

## Required implementation tasks

### 1. Base client contract

- [x] Audit every direct URL builder for `.id`, `id`, `pk`, `instance_id`, or `object_id` resource lookup.
- [x] Replace public wording that says primary key or ID lookup with UID lookup.
- [x] Remove public documentation for `patch_by_id()` and `destroy_by_id()` as resource APIs.
- [x] Decide whether id-named methods remain temporarily as UID-taking aliases or are removed.
- [x] Add deprecation notices for any id-named method that remains only as an alias.
- [x] Add a failure path for migrated resource models that have no `uid`.
- [x] Add tests proving id-only migrated resource objects do not silently route by integer ID.

Base decision:

- `patch_by_id()` and `destroy_by_id()` remain temporarily as deprecated aliases.
- Deprecated aliases route through public-reference internals and emit `DeprecationWarning`.
- Normal instance methods use UID helpers and do not call deprecated aliases.
- Remaining model-specific direct URL builders are not accepted as final state; they are tracked in the relevant model-group sections below.

Base audit findings that remain for model-group migration:

- `mainsequence/client/models_helpers.py` still has job and job-run direct URLs built from `self.id`.
- `mainsequence/client/models_tdag.py` still has project and data-source helper paths that use project or data-source IDs.
- `mainsequence/client/models_simple_tables.py` intentionally still has row-record URL builders that use row `id`; these are out of scope unless they are table resource lookups.
- `mainsequence/client/markets/models/accounts_and_portfolios.py` still has portfolio and portfolio-group helper URLs built from `self.id`.
- `mainsequence/client/data_sources_interfaces/timescale.py` still has a data-node-storage helper URL built from `data_node_storage.id`.
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

- [ ] Confirm UID detail lookup for `DataNodeStorage`.
- [ ] Confirm UID detail lookup for `DataNodeUpdate`.
- [ ] Confirm UID detail lookup for `DataNodeUpdateDetails`.
- [ ] Confirm UID detail lookup for `SourceTableConfiguration`.
- [ ] Confirm UID detail lookup for scheduler and run-configuration resources before changing their public arguments.
- [ ] Remove public TDAG resource arguments named `*_id` where they are resource references.
- [ ] Replace TDAG request keys ending in `_id` only when payload analysis proves they are public resource identity.
- [ ] Keep TDAG logical names and hashes separate from backend UID identity.
- [ ] Add tests proving DataNode update methods work with `uid` and no `.id`.
- [ ] Add tests proving DataNode patch, delete, label, and share URLs use UID.

### 4. SimpleTable migration

- [ ] Confirm UID detail lookup for `SimpleTableStorage`.
- [ ] Confirm UID detail lookup for `SimpleTableUpdate`.
- [ ] Confirm UID detail lookup for `SimpleTableUpdateDetails`.
- [ ] Confirm UID detail lookup for SimpleTable run-configuration resources before changing public arguments.
- [ ] Make `SimpleTableForeignKeyPayload.target_table` hold target table UID.
- [ ] Add `target_table_uid` only if the backend schema requires a separate explicit field.
- [ ] Keep SimpleTable row create, update, and delete row IDs unchanged.
- [ ] Add tests proving foreign-key payloads use target table UID.
- [ ] Add tests proving SimpleTable storage delete uses UID.
- [ ] Add tests proving SimpleTable label and share actions use UID.
- [ ] Add tests proving row `id` behavior remains unchanged.

### 5. Project and infrastructure migration

- [ ] Confirm UID detail lookup for `Project`.
- [ ] Confirm UID detail lookup for `ProjectImage`.
- [ ] Confirm UID detail lookup for `ProjectResource`.
- [ ] Confirm UID detail lookup for `ResourceRelease`.
- [ ] Confirm UID detail lookup for `Job`.
- [ ] Confirm UID detail lookup for `JobRun`.
- [ ] Confirm UID detail lookup for `Bucket`.
- [ ] Confirm UID detail lookup for `Artifact`.
- [ ] Confirm UID detail lookup for `Secret`.
- [ ] Confirm UID detail lookup for `Constant`.
- [ ] Replace public CLI and SDK resource parameters such as `project_id`, `resource_id`, `job_id`, and `artifact_id` where they are resource references.
- [ ] Add tests proving project and job commands accept UUID-like values without integer coercion.

### 6. Command-center migration

- [ ] Confirm UID detail lookup for `Workspace`.
- [ ] Confirm UID detail lookup for `ConnectionInstance`.
- [ ] Confirm UID detail lookup for command-center models that inherit generic detail actions.
- [ ] Replace public `workspace_id` and `connection_id` arguments where they are resource references.
- [ ] Keep provider connection IDs unchanged where they are external identifiers.
- [ ] Add tests for UID-only command-center response payloads.

### 7. Markets migration

- [ ] Inventory markets models before changing field names.
- [ ] Confirm UID detail lookup for `Asset`.
- [ ] Confirm UID detail lookup for `AssetSnapshot` if it is resource-addressed.
- [ ] Confirm UID detail lookup for `AssetCategory`.
- [ ] Confirm UID detail lookup for `AssetTranslationTable`.
- [ ] Confirm UID detail lookup for `Calendar` if it is resource-addressed.
- [ ] Confirm UID detail lookup for `Portfolio`.
- [ ] Confirm UID detail lookup for `PortfolioGroup`.
- [ ] Confirm UID detail lookup for `Trade`.
- [ ] Confirm UID detail lookup for `Order`.
- [ ] Confirm UID detail lookup for `OrderManager`.
- [ ] Confirm UID detail lookup for `VirtualFund`.
- [ ] Keep tickers, exchange codes, broker IDs, and provider IDs outside this migration.
- [ ] Add tests proving backend UID changes do not affect deterministic VFB hashes.

### 8. Agent and runtime migration

- [ ] Inventory agent and runtime models before changing field names.
- [ ] Confirm UID detail lookup for `Agent`.
- [ ] Confirm UID detail lookup for `AgentSession`.
- [ ] Confirm UID detail lookup for `UserOrchestratorAgentService`.
- [ ] Confirm UID detail lookup for `UserProjectExecutorAgentService`.
- [ ] Replace public id-named arguments only where they are SDK resource references.
- [ ] Keep runtime execution IDs and provider IDs unchanged where they are domain identifiers.
- [ ] Add tests for UID-only agent and runtime responses.

### 9. User, team, organization, and permission subjects

- [ ] Inventory user and organization models separately from resource models.
- [ ] Confirm whether `User`, `Team`, and `OrganizationTeam` expose UID detail routes.
- [ ] Do not change permission subject payloads until backend subject identity is confirmed.
- [ ] If permission subjects migrate, replace subject `id` with subject `uid` in share payloads and docs.
- [ ] If permission subjects do not migrate, document them as non-resource subject identifiers.
- [ ] Add tests that distinguish resource UID from permission subject identifiers.

### 10. CLI migration

- [ ] Replace public CLI resource option names ending in `_id` with `_uid`.
- [ ] Stop coercing resource lookup options with `int(...)`.
- [ ] Print `uid` as the primary resource identity in resource tables.
- [ ] Print `uid` as the primary resource identity in resource detail views.
- [ ] Remove integer resource IDs from public CLI examples.
- [ ] Keep non-resource subject IDs only where backend permission contracts require them.
- [ ] Add CLI tests proving UUID-like resource values are accepted.
- [ ] Add CLI tests proving migrated commands do not prefer `object_id` over `object_uid`.

### 11. Request and response typing

- [ ] Audit response models that expose `object_id`.
- [ ] Replace resource identity response fields with `object_uid` or typed `object_reference`.
- [ ] Audit request models that expose `dynamic_table_id`, `simple_table_id`, `storage_id`, `update_id`, or similar keys.
- [ ] Replace resource request keys with UID keys only after confirming backend contract migration.
- [ ] Remove code that assumes UID and ID keys both exist.
- [ ] Remove code that prefers `id` over `uid` for migrated resources.
- [ ] Keep row response `id` fields unchanged for SimpleTable records.
- [ ] Add tests for UID-only response shapes.

### 12. Documentation and tutorials

- [ ] Update ADR 0006 to reference this ADR for client-wide identity rules.
- [ ] Update hand-written SDK docs to use UID lookup examples.
- [ ] Update TDAG tutorials to use `storage.uid`, `update.uid`, and table UID references.
- [ ] Update SimpleTable tutorials to explain row `id` versus table `uid`.
- [ ] Update CLI docs to use `--*_uid` options for resource references.
- [ ] Update SDK examples to use `get_by_uid()`, `patch_by_uid()`, and `destroy_by_uid()`.
- [ ] Regenerate reference docs after public method and signature changes.
- [ ] Remove stale generated examples that expose `storage_id`, `dynamic_table_id`, or `simple_table_id` as public resource lookup examples.

### 13. Tests and acceptance criteria

- [ ] Add base tests for `PUBLIC_LOOKUP_FIELD = "uid"`.
- [x] Add base tests for missing UID errors.
- [ ] Add base tests proving patch, delete, label, and share URLs use UID.
- [x] Add tests proving id-only migrated resource objects fail.
- [ ] Add model inventory tests for UID coverage.
- [ ] Add endpoint URL tests for each migrated model group.
- [ ] Add CLI tests for UUID-like resource arguments.
- [ ] Add response-shape tests for UID-only payloads.
- [ ] Add SimpleTable tests proving row `id` is preserved.
- [ ] Add VFB hash tests proving backend UID changes do not change deterministic hashes.

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

The migration remains analysis-driven because `id` still has valid non-resource meanings. Keeping that distinction explicit prevents damage to SimpleTable row operations, permission payloads, provider integrations, TDAG configuration hashes, and VFB deterministic hashes.

## Risks

- Backend UID detail routes may be incomplete for some resources.
- Under-typed response models may hide assumptions that `id` exists.
- Broad renames can corrupt row-level SimpleTable operations.
- Permission subject payloads may still use identifiers that are not resource UID.
- Generated docs may continue to expose stale signatures until regenerated.
- Id-named compatibility aliases may confuse users unless removed or clearly deprecated.
