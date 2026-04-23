---
name: command-center-workspace-design
description: Use this skill when the task is about translating user intent into a Main Sequence Command Center workspace concept. This skill owns workspace design, widget selection, visualization strategy, information architecture, and deciding which widgets best express a business idea. It does not mutate workspace JSON, patch mounted widgets, create FastAPI endpoints, or create project resources/releases.
---

# Command Center Workspace Design

## Overview

Use this skill before building or mutating a Command Center workspace when the question is still:

- what should this workspace communicate?
- which widgets should represent the user's idea?
- what should be a table, chart, KPI, note, action, inspector, or agent surface?
- what layout and information hierarchy makes the workspace understandable?

This skill produces a design plan. It does not execute the workspace mutation.

## This Skill Must Not Claim

This skill must not claim ownership of:

- creating or updating workspace JSON
- exporting current workspace versions
- patching, moving, or deleting mounted widgets
- writing widget instance payloads
- FastAPI implementation
- AppComponent form contracts
- DataNode or SimpleTable producer design
- project resource or release creation

## Route Adjacent Work

- Workspace JSON creation, export/versioning, and widget mutation:
  `.agents/skills/command_center/workspace_builder/SKILL.md`
- AppComponent custom forms and widget input contracts:
  `.agents/skills/command_center/app_components/SKILL.md`
- APIs and FastAPI:
  `.agents/skills/application_surfaces/api_surfaces/SKILL.md`
- DataNodes:
  `.agents/skills/data_publishing/data_nodes/SKILL.md`
- SimpleTables:
  `.agents/skills/data_publishing/simple_tables/SKILL.md`
- Jobs, images, resources, and releases:
  `.agents/skills/platform_operations/orchestration_and_releases/SKILL.md`
- Streamlit dashboards:
  `.agents/skills/dashboards/streamlit/SKILL.md`

## Read First

1. Discover available widgets from the registry:
   - `mainsequence cc registered_widget_type list --json`
   - `mainsequence cc registered_widget_type detail <WIDGET_ID> --json`
2. `docs/knowledge/command_center/workspaces.md`
3. `docs/knowledge/command_center/widget_data_contracts.md`
4. `docs/knowledge/command_center/forms.md` when the design may need an AppComponent
5. `.agents/skills/command_center/workspace_builder/SKILL.md` only after the design is ready for execution

The widget registry is the source of truth for which widget types are actually available.

## Inputs This Skill Needs

Before producing a workspace design, collect or infer:

- the business question or workflow the workspace should support
- the intended users and their decisions
- the primary data objects:
  - DataNodes
  - SimpleTables
  - assets
  - portfolios
  - jobs
  - APIs
  - agents
- whether the workspace is for:
  - monitoring
  - exploration
  - execution
  - diagnostics
  - reporting
  - debugging
- whether the user needs a quick visual answer or a durable product surface
- the available registered widget types and their capabilities
- whether any widget requires a backend API or AppComponent flow

If the intent is unclear, ask for clarification before designing the workspace.

## Required Decisions

For every workspace design task, decide:

1. What is the primary user decision or workflow?
2. What is the workspace narrative from top to bottom?
3. Which widget types should be used, and why?
4. Which widgets are primary, supporting, diagnostic, or action-oriented?
5. Which views should be tables versus charts versus KPIs?
6. Which parts need markdown context or annotations?
7. Which interactions require an AppComponent or API-backed widget?
8. Which data already exists, and which data must be built first?
9. Does any non-standard visualization require `echarts-spec`?
10. What should be handed to `workspace_builder` for execution?

## Build Rules

### 1. Start From User Intent, Not Widget Availability

Do not choose widgets just because they exist.

Map the user intent into workspace roles first:

- explain
- monitor
- compare
- inspect
- drill down
- trigger action
- debug
- collaborate with an agent

Then select widget types that serve those roles.

### 2. Verify Widget Availability Through The Registry

Before recommending a widget type, verify it exists:

```bash
mainsequence cc registered_widget_type list --json
mainsequence cc registered_widget_type detail <WIDGET_ID> --json
```

Use `detail` to understand:

- what the widget is for
- configuration modes
- IO shape
- supported capabilities
- runtime ownership
- examples and authoring hints

Do not invent widget ids.

### 3. Use The Right Widget Role

Think in concepts first, then map the concept to a registered widget.

Always verify through the CLI that the widget exists and inspect its widget-detail internals before treating the mapping as valid:

```bash
mainsequence cc registered_widget_type list --json
mainsequence cc registered_widget_type detail <WIDGET_ID> --json
```

Structure and explanation:

- use `markdown-note` for explanatory text, instructions, assumptions, annotations, and section headers
- use `workspace-row` for grouping, layout, and visual separation

Data inspection:

- use `main-sequence-data-node` Stores a reusable Main Sequence DataNode. This widget should be use as the standard and canonical widget to request
data within the Main Sequence Data Platform that is a DataNode, This widget exposers the  long format of the DataNode this widget should be prefered when a visualization of the DataNode. 
requires simple transformations defined in the widget internals  which are:
- none: publishes the input dataset as-is, except optional final projection.
aggregate: groups by selected key fields and reduces each group with first, last, sum, mean, min, or max.
pivot: uses key fields as row dimensions, turns values from one categorical field into output columns, and fills them from a selected value field using the aggregate mode.
unpivot: melts selected wide value columns into long-form rows, preserving selected key fields and writing output field names like series and value.
project columns: optional final step that keeps only selected published columns after the main transform.
The documented transform order is: input dataset -> one transform mode -> projection -> published dataset. It does not support chaining multiple transform modes inside one widget; for that, chain multiple Data Node widgets


- use `data-node-table-visualizer` when the user needs to inspect rows, columns, filters, or tabular output


Visualization and summaries:

- use `main-sequence-data-node-visualizer` for chart or graph-oriented DataNode exploration
- use `main-sequence-data-node-statistic` for KPIs, single-value summaries, or compact status metrics
- use `echarts-spec` when the visualization is non-standard or too complex for a standard chart widget

Markets and portfolio views:

- use market widgets such as price, positions, curve, and zero-curve widgets when the workspace is about market data, portfolio inspection, or instrument analytics
- verify the exact market widget ids and contracts from the registry before proposing them

Infrastructure and lineage:

- use `main-sequence-dependency-graph` to explain DataNode dependencies or lineage
- use `main-sequence-project-infra-graph` to explain project infrastructure and platform object relationships

Interaction and workflows:

- use `app-component` when the workspace needs a form-driven action, custom workflow, or domain-specific interactive operation
- route AppComponent form and input-contract design to the AppComponents skill

Agent and debugging surfaces:

- use `main-sequence-ai-agent-terminal` when the workspace needs an agent interaction surface
- use `main-sequence-ai-upstream-inspector` when the workspace is explicitly testing or debugging upstream widget wiring and payload behavior

These examples are guidance only. Always confirm actual availability and contract details from the registry.

### 4. Keep The Workspace Narrative Coherent

A good Command Center workspace usually has:

1. context:
   what this workspace is for
2. controls:
   what the user can change
3. primary view:
   the main table, chart, KPI, or action
4. supporting views:
   breakdowns, comparisons, or diagnostics
5. explanation:
   notes, assumptions, or next steps

Avoid dumping unrelated widgets into one workspace without a clear story.

### 5. Separate Product Surfaces From Debug Surfaces

Use debugging widgets intentionally.

For example:

- use `main-sequence-ai-upstream-inspector` to test and inspect upstream payloads
- do not leave it as the primary product surface unless the workspace is explicitly a diagnostic workspace

### 6. Use `echarts-spec` For Complex Custom Visualizations

If the user needs a non-standard visualization, do not improvise chart props.

Use `echarts-spec` when the visualization needs:

- multiple axes
- complex series composition
- custom encodings
- non-standard interactions
- specialized chart semantics

First extract the relevant widget requirements from:

```bash
mainsequence cc registered_widget_type detail <WIDGET_ID> --json
```

Then design the `echarts-spec` visualization around the verified widget contract.

### 7. Identify Required Backend Work

If the design needs data or behavior that does not exist yet, route it explicitly:

- new or refreshed time-series/data pipeline:
  DataNode skill
- operational/application records:
  SimpleTable skill
- widget-facing backend endpoint:
  API surfaces skill
- custom form or interaction:
  AppComponents skill
- resource or release:
  orchestration-and-releases skill

Do not make the workspace design pretend missing backend capabilities already exist.

## Review Rules

When reviewing a workspace design, look for:

- widget choices that do not match the user intent
- unverified widget ids
- product workflows mixed with debugging widgets without explanation
- too many unrelated widgets in one workspace
- missing context or explanatory markdown
- charts used where a table or KPI would be clearer
- tables used where a chart would better show trend, comparison, or distribution
- AppComponent proposed where default API/widget behavior is enough
- custom visualization proposed without a reason to use `echarts-spec`
- missing handoff to the workspace-builder skill

## Validation Checklist

Do not claim the design is ready until you have checked:

- the user intent is stated clearly
- the workspace narrative is clear
- each proposed widget has a reason
- each proposed widget id was verified through the registry
- the design identifies required data/API/AppComponent dependencies
- complex visualizations justify `echarts-spec`
- debugging widgets are marked as diagnostic, not product surfaces, unless that is the user's intent
- the handoff to `workspace_builder` includes:
  - workspace purpose
  - target widgets
  - widget ids
  - layout intent
  - required data/resources
  - open questions

## This Skill Must Stop And Escalate When

- the user intent is too vague to select widgets responsibly
- the target widget cannot be found in the registry
- widget registry detail contradicts the desired use
- required backend data or APIs do not exist and the user has not approved building them
- the task has moved from design into workspace JSON mutation

Do not mutate workspace JSON from this skill.
