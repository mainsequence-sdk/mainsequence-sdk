---
name: mainsequence-data-nodes
description: Use this skill when the task is about producing, changing, validating, or reviewing Main Sequence DataNodes. This skill owns DataNode contracts, hashing, namespaces, update logic, metadata, asset-indexed nodes, and DataNode validation. It does not own SimpleTable row modeling, API route contracts, scheduling, or sharing policy.
---

# Main Sequence Data Nodes

## Overview

Use this skill when the task changes a DataNode producer or the published table contract behind it.

This skill is for producer-side table engineering.

## This Skill Can Do

- create a new `DataNode`
- modify an existing `DataNode`
- review whether a DataNode change is breaking or non-breaking
- define or refactor `DataNodeConfiguration`
- classify config fields into dataset meaning, updater scope, and runtime-only concerns
- implement or review:
  - `dependencies()`
  - `update()`
  - `get_asset_list()`
  - metadata and record definitions
- design single-index or `(time_index, unique_identifier)` MultiIndex outputs
- define namespace-first validation strategy
- write or review DataNode smoke tests
- decide whether a consumer should use `APIDataNode`

## This Skill Must Not Claim

This skill must not claim ownership of:

- SimpleTable row modeling or row-mutation semantics
- HTTP route design or FastAPI response contracts
- workspace/widget layout payloads
- job creation, scheduling, image pinning, or release creation
- RBAC or sharing policy
- portfolio strategy semantics

If the task depends on one of those areas, route it explicitly instead of guessing.

If the user is still in the discovery process and does not yet know what data exists on the platform, use the exploration skill first and return here after discovery is complete.

## Route Adjacent Work

- discovery-only data inventory before DataNode implementation:
  `.agents/skills/mainsequence/data_access/exploration/SKILL.md`
- SimpleTables:
  `.agents/skills/mainsequence/data_publishing/simple_tables/SKILL.md`
- APIs and FastAPI:
  `.agents/skills/mainsequence/application_surfaces/api_surfaces/SKILL.md`
- Command Center workspaces:
  `.agents/skills/mainsequence/command_center/workspace_builder/SKILL.md`
- AppComponents and custom forms:
  `.agents/skills/mainsequence/command_center/app_components/SKILL.md`
- Jobs, images, resources, and releases:
  `.agents/skills/mainsequence/platform_operations/orchestration_and_releases/SKILL.md`
- RBAC and sharing:
  `.agents/skills/mainsequence/platform_operations/access_control_and_sharing/SKILL.md`
- Asset categories and translation tables as standalone market concepts:
  `.agents/skills/mainsequence/markets_platform/assets_and_translation/SKILL.md`
- VFB semantics:
  `.agents/skills/mainsequence/markets_platform/virtualfundbuilder/SKILL.md`

## Read First

1. `docs/tutorial/creating_a_simple_data_node.md`
2. `docs/tutorial/multi_index_columns_working_with_assets.md`
3. `docs/knowledge/data_nodes.md`
4. `docs/knowledge/markets/assets.md` when the node is asset-indexed

## Inputs This Skill Needs

Before changing code, collect or infer:

- dataset meaning
- intended published identifier
- expected index shape
- expected columns and dtypes
- upstream dependencies
- whether the node is asset-indexed
- first-run or backfill bounds
- whether the change must preserve the existing table contract

If one of these is unknown and changes the contract, stop and resolve it before implementation.

## Required Decisions

For every non-trivial DataNode task, make these decisions explicitly:

1. Is this a new dataset or the same dataset?
2. Is the change changing dataset meaning or only updater scope?
3. Is the identifier collision-safe in this organization?
4. Is the node single-index or MultiIndex?
5. Does the first validation run happen in a namespace?

## Build Rules

### 1. Treat the DataNode as a published product

The following are contract-level decisions:

- identifier
- schema
- index shape
- semantic meaning of the table

Do not change them casually.

### 2. Keep meaning separate from scope

- dataset meaning belongs in table identity
- updater scope belongs in updater identity
- runtime-only knobs belong outside hashed identity

Do not mix these.

### 3. `hash_namespace` is isolation only

Use `hash_namespace(...)` or `test_node=True` for:

- namespaced tests
- isolated experimentation
- shared-backend safety

Do not use namespace to encode business meaning.

### 4. `update()` should be incremental by default

Use `UpdateStatistics`.

Do not fetch or return full history every run unless there is a documented reason.

### 5. Dependencies must be deterministic

Dependencies belong in constructor setup and `dependencies()`.

Do not construct dependency graphs dynamically inside `update()`.

### 6. Asset-indexed nodes must behave like asset-indexed nodes

If the node emits `(time_index, unique_identifier)`:

- `unique_identifier` should represent an Asset identity
- `get_asset_list()` must reflect the effective updater asset scope
- missing assets should be resolved or registered when required by the workflow

### 7. Metadata is not optional for production-quality nodes

When the node is not a throwaway example, provide:

- table metadata
- column metadata or record definitions

## Review Rules

When reviewing an existing DataNode, look for:

- identifier collisions
- accidental schema breaks
- wrong meaning/scope/runtime-only split
- misuse of `hash_namespace`
- non-incremental `update()` behavior
- hidden dependency creation inside `update()`
- invalid asset-indexed output shape
- missing metadata on a production node

## Validation Checklist

Do not claim success until you have checked:

- the relevant docs were read first
- the identifier choice is intentional
- config fields are classified correctly
- `dependencies()` is deterministic
- `update()` is incremental
- the DataFrame shape is valid
- the first validation run is namespaced

For asset-indexed nodes, also check:

- `get_asset_list()` is correct
- no duplicate `(time_index, unique_identifier)` rows are emitted
- assets exist or are registered idempotently when needed

## This Skill Must Stop And Escalate When

- the change may break an existing published table contract and the versioning decision is unclear
- the intended identifier is likely to collide and no naming decision was made
- the node needs asset identities but the asset-resolution strategy is unclear
- the task is actually an API, SimpleTable, orchestration, or sharing problem
- docs and code disagree on hashing or runtime behavior

Do not guess through contract changes.
