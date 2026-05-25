---
name: mainsequence-meta-tables
description: Use this skill when the task is about defining, registering, querying, or reviewing Main Sequence MetaTables. This skill owns SQLAlchemy table contracts, backend-managed table registration, external table registration, governed compiled SQL operations, foreign keys, indexes, and validation rules. It does not own DataNode producers, API route contracts, scheduling, or sharing policy.
---

# Main Sequence MetaTables

## Overview

Use this skill when the task changes row-oriented project tables that are not naturally time-series DataNodes.

This skill is for schema-driven application tables registered through TS Manager as `MetaTable` resources.

## This Skill Can Do

- define SQLAlchemy/Core or ORM table models for `MetaTable` registration
- choose `platform_managed` or `external_registered` management mode
- build registration requests from resolved SQLAlchemy metadata
- define indexes and foreign keys in the table contract
- design governed compiled SQL read and write operations
- review table contracts for physical-name, namespace, and identifier issues
- review whether a task should be a `MetaTable` or a `DataNode`

## This Skill Must Not Claim

This skill must not claim ownership of:

- DataNode producer contracts
- FastAPI route contracts
- widget response contracts
- workspace payloads
- job scheduling, image pinning, or releases
- RBAC or sharing policy

If the user is still in the discovery process and does not yet know what data exists on the platform, use the exploration skill first and return here after discovery is complete.

## Route Adjacent Work

- discovery-only data inventory before table implementation:
  `.agents/skills/mainsequence/data_access/exploration/SKILL.md`
- DataNodes:
  `.agents/skills/mainsequence/data_publishing/data_nodes/SKILL.md`
- APIs and FastAPI:
  `.agents/skills/mainsequence/application_surfaces/api_surfaces/SKILL.md`
- Command Center workspaces:
  `.agents/skills/mainsequence/command_center/workspace_builder/SKILL.md`
- AppComponents and custom forms:
  `.agents/skills/mainsequence/command_center/app_components/SKILL.md`
- Jobs, images, resources, and releases:
  `.agents/skills/mainsequence/platform_operations/orchestration_and_releases/SKILL.md`

## Read First

1. `docs/tutorial/working_with_meta_tables.md`
2. `docs/knowledge/meta_tables/index.md`
3. `docs/knowledge/meta_tables/sqlalchemy.md`
4. `docs/knowledge/meta_tables/compiled_sql.md`
5. `docs/knowledge/meta_tables/api.md`

## Inputs This Skill Needs

Before changing code, collect or infer:

- the row entities that should exist
- the business keys
- relation shape between tables
- expected read patterns
- expected mutation patterns
- whether TS Manager should create the physical table
- the target `DynamicTableDataSource` UID

If ownership of the physical table lifecycle is unclear, stop before choosing a management mode.

## Required Decisions

For every non-trivial task, decide:

1. Is this table really row-oriented, or should it be a DataNode?
2. What is the business key?
3. Should the table be `platform_managed` or `external_registered`?
4. What namespace and identifier define the logical table identity?
5. Are foreign-key dependencies aligned with registration order?
6. What governed operations should be allowed by the declared table scope?

## Build Rules

### 1. SQLAlchemy metadata is the authoring source

Keep the application table model as the authoring source for the neutral table contract.

Do not hand-build contract fragments when the SQLAlchemy helper can derive them.

### 2. Use storage-hash physical names for backend-managed tables

For `platform_managed`, use `metatable_tablename(...)` as the SQLAlchemy `__tablename__`.

The backend expects that deterministic physical name to match the registration storage hash.

### 3. Register parent tables before child tables

Foreign-key contracts reference the target `MetaTable` UID.

Register parent tables first, then pass their UIDs when building child table registration requests.

### 4. Governed operations declare scope

Compiled SQL operations must declare the `MetaTable` UID scope and read/write access for every table they touch.

Do not execute unrestricted SQL outside the MetaTable operation contract.

### 5. Physical names come from registered resources

Only use physical schema and table names returned by registered `MetaTable` objects when composing SQL strings.

Do not hardcode platform-managed physical names manually.

## Review Rules

When reviewing an existing MetaTable workflow, look for:

- missing namespace or identifier
- backend-managed models that do not use `metatable_tablename(...)`
- external tables registered with unstable physical names
- foreign keys that do not reference target MetaTable UIDs
- compiled SQL operations without complete table scope
- raw SQL that hardcodes stale physical names
- a table that should really be modeled as a DataNode instead

## Validation Checklist

Do not claim success until you have checked:

- the table contract matches the intended row contract
- indexes are intentional
- foreign keys point to the correct dependency targets
- management mode is correct
- backend-managed physical names match the storage hash
- registration returns a `MetaTable.uid`
- compiled SQL operations declare table scope

For related tables, also check:

- aliases are readable
- parent table UIDs are passed into child contracts
- query results still match the expected response contract

## This Skill Must Stop And Escalate When

- physical table lifecycle ownership is unclear
- the target data source is unknown
- the task really requires a time-series published table
- the workflow requires direct database credentials outside TS Manager governance
- the task is actually an API or orchestration problem

Do not guess through registration or execution semantics.
