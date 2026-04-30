---
name: command-center-workspace-design
description: Use this skill when the task is about translating user intent into a Main Sequence Command Center workspace concept. This skill owns workspace design, widget selection, visualization strategy, information architecture, connection-first dataflow planning, and deciding which widgets best express a business idea. It does not mutate workspace JSON, patch mounted widgets, create connections, create FastAPI endpoints, or create project resources/releases.
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
- tabular or operational data producer design
- project resource or release creation

## Route Adjacent Work

- Workspace JSON creation, export/versioning, and widget mutation:
  `.agents/skills/mainsequence/command_center/workspace_builder/SKILL.md`
- AppComponent custom forms and widget input contracts:
  `.agents/skills/mainsequence/command_center/app_components/SKILL.md`
- Connection-backed data access and query contract selection:
  `.agents/skills/mainsequence/command_center/connections/SKILL.md`
- APIs and FastAPI:
  `.agents/skills/mainsequence/application_surfaces/api_surfaces/SKILL.md`
- SimpleTables:
  `.agents/skills/mainsequence/data_publishing/simple_tables/SKILL.md`
- Jobs, images, resources, and releases:
  `.agents/skills/mainsequence/platform_operations/orchestration_and_releases/SKILL.md`
- Streamlit dashboards:
  `.agents/skills/mainsequence/dashboards/streamlit/SKILL.md`

## Read First

1. Discover available widgets from the registry:
   - `mainsequence cc registered_widget_type list --json`
   - `mainsequence cc registered_widget_type detail <WIDGET_ID> --json`
2. `.agents/skills/mainsequence/command_center/connections/SKILL.md` when the workspace needs backend-owned data
3. `docs/knowledge/command_center/workspaces.md`
4. `docs/knowledge/command_center/widget_data_contracts.md`
5. `docs/knowledge/command_center/forms.md` when the design may need an AppComponent
6. `.agents/skills/mainsequence/command_center/workspace_builder/SKILL.md` only after the design is ready for execution

The widget registry is the source of truth for which widget types are actually available.

## Inputs This Skill Needs

Before producing a workspace design, collect or infer:

- the business question or workflow the workspace should support
- the intended users and their decisions
- the primary data objects:
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
- the required connection instances and query models for backend-owned data
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
8. Which backend-owned data must come through a connection instance and query model?
9. Which data already exists, and which data must be built first?
10. Does any non-standard visualization require `echarts-spec`?
11. What should be handed to `workspace_builder` for execution?

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

Always verify through the CLI that the widget exists, and inspect the widget-detail response before treating the mapping as valid. Read the response usage guidance carefully and treat it as canonical.

```bash
mainsequence cc registered_widget_type list --json
mainsequence cc registered_widget_type detail <WIDGET_ID> --json
```

#### Structure and explanation:

- use `markdown-note` for explanatory text, instructions, assumptions, annotations, and section headers
- use `workspace-row` for grouping, layout, and visual separation

#### Connection-backed data:

- use a Connection Query source widget when the workspace needs a reusable backend-owned dataset
- select the connection instance, query model, typed query payload, and output contract before choosing consumers
- use a Tabular Transform widget when the workspace needs aggregate, pivot, unpivot, projection, or other inspectable tabular reshaping
- bind table, chart, statistic, curve, and similar consumers to an upstream `dataset` output
- require generic tabular consumers to receive `core.tabular_frame@v1`
- when a project API or AppComponent is meant to produce a full canonical tabular frame, require
  the SDK `TabularFrameResponse` model from `mainsequence.client.command_center.data_models`
- keep source widgets in the sidebar rail when they are execution/data owners; visible canvas widgets should own presentation
- do not let consumer widgets query connection instances directly

#### Data inspection:

- use the registered table widget when the user needs row, column, filter, or tabular inspection of a bound tabular dataset
- when it improves readability, use richer per-column visual options such as gradients or gauges


#### Visualization and summaries:

- use the registered graph/chart widget for chart-oriented exploration of a bound tabular dataset
- use the registered statistic widget for KPIs, single-value summaries, status metrics, and compact numerical cards derived from a bound tabular dataset
- use statistic widgets when the user needs an at-a-glance answer rather than a full chart or table
- use `echarts-spec` when the visualization is non-standard, needs richer chart semantics, and a general ECharts chart is the right fit; this chart normally needs an AppComponent or API-backed upstream binding
- use `lightweight-chart-ts-spec` when the visualization is non-standard, time-series heavy, or financial-market oriented; this chart normally needs an AppComponent or API-backed upstream binding

#### Markets and portfolio views:

- use market widgets such as price, positions, curve, and zero-curve widgets when the workspace is about market data, portfolio inspection, or instrument analytics
- verify the exact market widget ids and contracts from the registry before proposing them

#### Infrastructure and lineage:

- use `main-sequence-dependency-graph` to explain data dependency or lineage relationships
- use `main-sequence-project-infra-graph` to explain project infrastructure and platform object relationships

#### Interaction and workflows:

- use `app-component` when the workspace needs a form-driven action, custom workflow, or domain-specific interactive operation; an AppComponent is a full interaction surface between the workspace and an API
- route AppComponent form and input-contract design to the AppComponents skill

#### Agent and debugging surfaces:

- use `main-sequence-ai-agent-terminal` when the workspace needs an agent interaction surface
- if you add an agent terminal to interpret a graph, table, or related group of tables, include a clear prompt and verify with the user that the agent surface is actually required
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
  data publishing skill
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
- connection-backed data dependencies identify connection instances, query models, and output contracts
- complex visualizations justify `echarts-spec`
- debugging widgets are marked as diagnostic, not product surfaces, unless that is the user's intent
- the handoff to `workspace_builder` includes:
  - workspace purpose
  - target widgets
  - widget ids
  - layout intent
  - required connection instances and query models
  - required data/resources
  - open questions

## This Skill Must Stop And Escalate When

- the user intent is too vague to select widgets responsibly
- the target widget cannot be found in the registry
- widget registry detail contradicts the desired use
- required backend data or APIs do not exist and the user has not approved building them
- the task has moved from design into workspace JSON mutation

Do not mutate workspace JSON from this skill.
