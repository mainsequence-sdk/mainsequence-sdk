---
name: mainsequence-data-exploration
description: Use this skill when the task is to discover what `DataNode` or `SimpleTable` data already exists on the Main Sequence platform before implementation starts.
---

# Main Sequence Data Exploration

## Overview

Use this skill when the task is about discovering what `DataNode` or `SimpleTable` data is already available on the platform before implementation starts.

This skill is for discovery only.

It helps answer questions such as:

- what published data already exists on this platform
- whether the right surface is a `DataNode` or `SimpleTable`
- which identifier, storage id, or object id should be used
- what metadata is available for those objects
- what still needs to be clarified before implementation starts

## This Skill Can Do

- discover published `DataNode` tables through the CLI
- inspect published table identifiers, storage ids, and metadata
- search for tables by keyword, description, or column name
- inspect organization-visible table identifiers
- inspect `SimpleTable` objects through the CLI
- summarize what data is available, what is missing, and what evidence supports that conclusion

## This Skill Must Not Claim

This skill must not claim ownership of:

- producing or modifying `DataNode` pipelines
- designing `SimpleTable` schemas or mutation behavior
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
- `SimpleTable` schema or updater work:
  `.agents/skills/mainsequence/data_publishing/simple_tables/SKILL.md`
- FastAPI or application-facing implementation:
  `.agents/skills/mainsequence/application_surfaces/api_surfaces/SKILL.md`
- Streamlit dashboards:
  `.agents/skills/mainsequence/dashboards/streamlit/SKILL.md`
- Command Center surfaces:
  `.agents/skills/mainsequence/command_center/workspace_builder/SKILL.md`
- market-specific object discovery:
  `.agents/skills/mainsequence/markets_platform/assets_and_translation/SKILL.md`
- project-state reconciliation after exploration:
  `.agents/skills/mainsequence/maintenance/local_journal/SKILL.md`
- blocker and failure analysis:
  `.agents/skills/mainsequence/maintenance/bug_auditor/SKILL.md`

## Read First

1. `AGENTS.md`
2. `docs/cli/index.md`
3. `docs/knowledge/data_nodes.md`
4. `docs/knowledge/simple_tables/simple_table.md` when the exploration touches row-oriented storage

## Inputs This Skill Needs

Before exploring, collect or infer:

- the business question being asked
- whether the user is exploring organization-wide data or project-scoped data
- whether the target is a published table or a row-oriented table
- whether the user already knows an identifier, keyword, or object id
- whether metadata discovery alone answers the question

## Required Decisions

For every non-trivial exploration task, decide:

1. Is the data surface a `DataNode` or `SimpleTable`?
2. Do we know the identifier already, or do we need to search first?
3. What metadata or object details are needed to complete the discovery?
4. Which implementation skill should receive the handoff after discovery?

## Build Rules

### 1. Use the CLI as the default discovery surface

For platform discovery, start with the `mainsequence` CLI.

Discovery priority:

- use search first when the surface supports search
- use list and filters when the surface does not support search
- use detail only after you have identified the candidate object you want to inspect

Typical commands include:

- `mainsequence data-node search "<keyword>"`
- `mainsequence data-node search "<keyword>" --mode description`
- `mainsequence data-node search "<keyword>" --mode column`
- `mainsequence data-node search "<keyword>" --show-filters`
- `mainsequence data-node search "<keyword>" --filter KEY=VALUE`
- `mainsequence data-node list`
- `mainsequence data-node list --show-filters`
- `mainsequence data-node list --filter KEY=VALUE`
- `mainsequence data-node detail <DATA_NODE_STORAGE_ID>`
- `mainsequence simple_table list`
- `mainsequence simple_table detail <SIMPLE_TABLE_ID>`

### 2. Stop at discovery boundaries

Use the CLI to identify what exists and collect the relevant identifiers, object ids, and metadata.

Do not define code-level read patterns here.

For `DataNode` discovery specifically:

- start with `mainsequence data-node search`
- use `mainsequence data-node list` when you need broader enumeration or structured filters
- use `mainsequence data-node detail` only after search or list identified the target storage

### 3. Report platform objects with evidence

When you say data exists, include the evidence you used:

- identifier
- storage id or object id
- object type
- command used to discover it
- metadata or filter evidence when available

### 4. Hand off explicitly after discovery

Once the discovery is complete, hand off to the correct implementation skill.

Examples:

- if the task is about publishing or modifying time-series data, hand off to `.agents/skills/mainsequence/data_publishing/data_nodes/SKILL.md`
- if the task is about row-oriented operational data, hand off to `.agents/skills/mainsequence/data_publishing/simple_tables/SKILL.md`
- if the task is about APIs or dashboards, hand off to the relevant surface skill after discovery is complete

## Review Rules

When reviewing an exploration result, look for:

- a discovery claim without CLI evidence
- confusion between `DataNode` and `SimpleTable`
- jumping into implementation work before discovery is complete
- claiming implementation conclusions from a discovery-only pass

## Validation Checklist

Do not claim exploration success until you have checked:

- the correct platform surface was identified
- CLI discovery was used first
- existing identifiers or object ids were captured
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
