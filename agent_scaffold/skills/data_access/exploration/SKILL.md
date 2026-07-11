---
name: mainsequence-data-exploration
description: Use this skill when the task is to discover what `DataNode` or `MetaTable` data already exists on the Main Sequence platform before implementation starts.
---

# Main Sequence Data Exploration

## Overview

Use this skill when the task is about discovering what `DataNode` or `MetaTable` data is already available on the platform before implementation starts.

This skill is for discovery only.

It helps answer questions such as:

- what published data already exists on this platform
- whether the right surface is a `DataNode` or `MetaTable`
- which identifier, storage UID, or object UID should be used
- what metadata is available for those objects
- what still needs to be clarified before implementation starts

## This Skill Can Do

- discover published `DataNode` tables through the CLI
- inspect published table identifiers, storage UIDs, and metadata
- search for tables by keyword, description, or column name
- inspect organization-visible table identifiers
- inspect registered `MetaTable` objects through the SDK client
- summarize what data is available, what is missing, and what evidence supports that conclusion

## This Skill Must Not Claim

This skill must not claim ownership of:

- producing or modifying `DataNode` pipelines
- designing `MetaTable` registration or operation behavior
- deciding code-level read patterns for discovered datasets
- building APIs or dashboards
- scheduling jobs, images, or releases
- sharing or RBAC policy

This skill discovers and identifies data. It does not publish, redesign, or define implementation patterns.

## Route Adjacent Work

- project bootstrap, scaffolding, and routing:
  `.agents/skills/mainsequence/project_builder/SKILL.md`
- `DataNode` creation or modification:
  `.agents/skills/mainsequence/data_publishing/data_nodes/SKILL.md`
- `MetaTable` registration or operation work:
  `.agents/skills/mainsequence/data_publishing/meta_tables/SKILL.md`
- FastAPI or application-facing implementation:
  `.agents/skills/mainsequence/application_surfaces/api_surfaces/SKILL.md`
- Streamlit dashboard deployment:
  `.agents/skills/mainsequence/platform_operations/orchestration_and_releases/SKILL.md`
- Command Center surfaces:
  `.agents/skills/mainsequence/command_center/workspace_builder/SKILL.md`
- blocker and failure analysis:
  `.agents/skills/mainsequence/maintenance/bug_auditor/SKILL.md`

Streamlit dashboard implementation is app-owned project code, not a Main Sequence scaffold skill.

## Read First

1. `AGENTS.md`
2. `docs/cli/index.md`
3. `docs/knowledge/data_nodes.md`
4. `docs/knowledge/meta_tables/index.md` when the exploration touches row-oriented storage

## Inputs This Skill Needs

Before exploring, collect or infer:

- the business question being asked
- whether the user is exploring organization-wide data or project-scoped data
- whether the target is a published table or a row-oriented table
- whether the user already knows an identifier, keyword, or object UID
- whether metadata discovery alone answers the question

## Required Decisions

For every non-trivial exploration task, decide:

1. Is the data surface a `DataNode` or `MetaTable`?
2. Do we know the identifier already, or do we need to search first?
3. What metadata or object details are needed to complete the discovery?
4. Which implementation skill should receive the handoff after discovery?

## Build Rules

### 1. Use the CLI as the default discovery surface

For platform discovery, start with the `mainsequence` CLI.

Discovery priority:

- use semantic description search first when the user describes a dataset in natural language
- use list and filters only when the user already knows a structured field to constrain by
- use column lookup only when the user is specifically searching for a schema or column name
- use detail only after you have identified the candidate object you want to inspect

Semantic description discovery searches `MetaTable` metadata rows through:

- `GET /orm/api/ts_manager/meta_table/description-search/?q=<text>`

The same discovery surface applies to `MetaTable` metadata and to published
`DataNode` storage tables, because `DataNode` storage is represented by
`TimeIndexMetaTable` metadata.

Typical semantic discovery commands:

- `mainsequence data-node search "<keyword>"`

Optional ranking knobs for description discovery:

- `--trigram-k 200`
- `--embed-k 200`
- `--w-trgm 0.65`
- `--w-emb 0.35`

The response is paginated metadata:

```json
{
  "count": 2,
  "next": null,
  "previous": null,
  "results": [
    {
      "orm_class": "TimeIndexMetaTable",
      "uid": "0b0f5733-b1ee-4d09-b51f-9e29073007fd",
      "data_source_uid": "864e7c22-482a-464a-8758-0d3408abd77f",
      "identifier": "ms_markets__externalpricests__mainsequence_examples",
      "namespace": "mainsequence.examples",
      "description": "...",
      "physical_table_name": "ms_markets__externalpricests__mainsequence_examples",
      "table_contract": {},
      "columns": [],
      "indexes_meta": [],
      "foreign_keys": [],
      "cadence": "1d",
      "time_indexed_profile": {}
    }
  ]
}
```

Structured filtering is a different path. Use it only when you already know the
field to filter by:

- `mainsequence data-node list`
- `mainsequence data-node list --show-filters`
- `mainsequence data-node list --filter KEY=VALUE`
- `mainsequence data-node detail <DATA_NODE_STORAGE_UID>`

Column lookup is also a different path. Use it only for schema-name discovery,
not as the default dataset discovery flow:

- `mainsequence data-node search "<column-name>" --mode column`

Raw SQL inspection is only for targeted exploration after you have a UID:

- `mainsequence meta-table run_query <META_TABLE_UID> "SELECT * FROM public.some_table LIMIT 100"`
- `mainsequence data-node run_query <DATA_NODE_STORAGE_UID> "SELECT * FROM public.some_table LIMIT 100"`

Use the MetaTable command for `MetaTable` UIDs. Use the DataNode command only
for `DataNode` storage UIDs. Do not confuse the two surfaces.

For direct SDK/backend usage, MetaTable raw SQL is sent as a JSON string body,
not as an object. Do not send `{ "sql": "SELECT ..." }`.

### 2. Stop at discovery boundaries

Use the CLI or SDK client to identify what exists and collect the relevant identifiers, object UIDs, and metadata.

Do not define code-level read patterns here.

For `DataNode` discovery specifically:

- start with `mainsequence data-node search`
- use `mainsequence data-node list` only when you need broader enumeration or structured filters
- use `mainsequence data-node search "<column-name>" --mode column` only for schema-name lookup
- use `mainsequence data-node detail` only after search or list identified the target storage

### 3. Report platform objects with evidence

When you say data exists, include the evidence you used:

- identifier
- storage UID or object UID
- object type
- command used to discover it
- metadata or filter evidence when available

### 4. Hand off explicitly after discovery

Once the discovery is complete, hand off to the correct implementation skill.

Examples:

- if the task is about publishing or modifying time-series data, hand off to `.agents/skills/mainsequence/data_publishing/data_nodes/SKILL.md`
- if the task is about row-oriented operational data, hand off to `.agents/skills/mainsequence/data_publishing/meta_tables/SKILL.md`
- if the task is about APIs or dashboards, hand off to the relevant surface skill after discovery is complete

## Review Rules

When reviewing an exploration result, look for:

- a discovery claim without CLI evidence
- confusion between `DataNode` and `MetaTable`
- jumping into implementation work before discovery is complete
- claiming implementation conclusions from a discovery-only pass

## Validation Checklist

Do not claim exploration success until you have checked:

- the correct platform surface was identified
- CLI discovery was used first
- existing identifiers or object UIDs were captured
- the result clearly distinguishes:
  - what exists
  - what is missing
  - what still needs validation

## This Skill Must Stop And Escalate When

- the relevant platform surface cannot be identified
- discovery requires credentials or access that are not available
- the task has shifted from exploration into publishing or implementation
- the evidence is too weak to claim the data exists or does not exist

Do not guess through platform discovery.
