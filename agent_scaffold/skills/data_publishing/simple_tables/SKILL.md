---
name: mainsequence-simple-tables
description: Use this skill when the task is about defining, changing, querying, or reviewing Main Sequence SimpleTables and SimpleTableUpdaters. This skill owns row schemas, backend-managed ids, insert versus overwrite behavior, filtering, foreign keys, and validation rules. It does not own DataNode producers, API route contracts, scheduling, or sharing policy.
---

# Main Sequence SimpleTables

## Overview

Use this skill when the task changes row-oriented project tables that are not naturally time-series DataNodes.

This skill is for schema-driven application tables and their updater workflows.

## This Skill Can Do

- create a new `SimpleTable`
- create or modify a `SimpleTableUpdater`
- define indexes, foreign keys, and `Ops(...)`
- design typed filter expressions
- design join filters across simple tables
- decide whether a write should be insert-only or overwrite/upsert
- review simple-table code for invalid `id` usage
- review whether a task should be a SimpleTable or a DataNode

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

- discovery-only data inventory before SimpleTable implementation:
  `.agents/skills/data_access/exploration/SKILL.md`
- DataNodes:
  `.agents/skills/data_publishing/data_nodes/SKILL.md`
- APIs and FastAPI:
  `.agents/skills/application_surfaces/api_surfaces/SKILL.md`
- Command Center workspaces:
  `.agents/skills/command_center/workspace_builder/SKILL.md`
- AppComponents and custom forms:
  `.agents/skills/command_center/app_components/SKILL.md`
- Jobs, images, resources, and releases:
  `.agents/skills/platform_operations/orchestration_and_releases/SKILL.md`

## Read First

1. `docs/tutorial/working_with_simple_tables.md`
2. `docs/knowledge/simple_tables/simple_table.md`
3. `docs/knowledge/simple_tables/filtering.md`

## Inputs This Skill Needs

Before changing code, collect or infer:

- the row entities that should exist
- the business keys
- relation shape between tables
- expected read patterns
- expected mutation patterns
- whether rows already exist or are being seeded for the first time

If the mutation model is unclear, stop before choosing insert-only versus overwrite.

## Required Decisions

For every non-trivial task, decide:

1. Is this table really row-oriented, or should it be a DataNode?
2. What is the business key?
3. Does the backend mutation require row ids?
4. Is this write insert-only or overwrite/upsert?
5. Are foreign-key dependencies aligned with the intended insert/read flow?

## Build Rules

### 1. `SimpleTable` defines schema, `SimpleTableUpdater` owns the real table

Keep that split clear.

Do not mix row schema concerns with backend updater concerns.

### 2. `id` is backend-managed

Users must not declare `id` in a `SimpleTable` subclass.

The normal lifecycle is:

1. insert without `id`
2. read rows back
3. use returned `id` values for sparse updates, upserts, or deletes

### 3. Unique index is not overwrite key

A unique business key is useful for lookup and constraints.

It is not the backend mutation key for overwrite/upsert payloads.

If `update()` returns `(records, True)`, those records must already include backend ids.

### 4. Insert-only is the default when ids do not exist yet

Use insert-only when:

- seeding rows
- loading new records
- the records do not yet carry backend ids

### 5. Filters come from the schema and execute through the updater

Build filter expressions from the schema surface.

Execute them through the updater.

Do not bypass the updater with hand-built backend table requests.

### 6. Foreign keys must reflect actual dependency flow

If one table depends on another:

- parent rows must exist first
- child rows should use the returned backend ids from the parent

## Review Rules

When reviewing an existing SimpleTable workflow, look for:

- user-declared `id`
- treating a business key as the overwrite key
- overwrite/upsert without backend ids
- foreign keys that do not match the real dependency flow
- filters bypassing the typed schema surface
- a table that should really be modeled as a DataNode instead

## Validation Checklist

Do not claim success until you have checked:

- the schema matches the intended row contract
- `id` is not user-declared
- indexes are intentional
- foreign keys point to the correct dependency targets
- insert-only versus overwrite behavior is correct
- overwrite/upsert payloads include backend ids when required
- filters run through the updater

For join filters, also check:

- aliases are readable
- resolved tables are used intentionally
- the base row type returned by the updater is still the expected one

## This Skill Must Stop And Escalate When

- a proposed schema declares `id`
- overwrite/upsert is attempted without backend ids
- the task really requires a time-series published table
- the workflow requires a richer relational model than `SimpleTable` is meant to support
- the task is actually an API or orchestration problem

Do not guess through mutation semantics.
