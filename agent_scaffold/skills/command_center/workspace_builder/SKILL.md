---
name: command-center-workspace-builder
description: Use this skill when the task is about creating, updating, validating, or reviewing Main Sequence Command Center workspaces. This skill owns workspace documents, widget instance payload resolution, layout decisions, shared versus user state, and widget-scoped mutation. The primary workflow is CLI registry verification first, then local payload-source inspection only when registry metadata is not enough. It does not own AppComponent form contracts, API implementation, or Streamlit dashboards.
---

# Command Center Workspace Builder

## Overview

Use this skill when the task is about a Command Center workspace document or a mounted widget inside that workspace.

This skill is for workspace structure, widget payload resolution, and safe workspace mutation.

## This Skill Can Do

- create a new Command Center workspace
- update an existing workspace document
- add widgets to a workspace
- verify widget types in the CLI registry before mounting or mutating them
- resolve widget instance payloads from local models or examples after registry verification
- decide between full workspace update and widget-scoped mutation
- patch one widget safely
- move or delete a widget safely
- verify shared workspace data versus current-user state
- review a workspace payload for guessed or invalid widget configuration

## This Skill Must Not Claim

This skill must not claim ownership of:

- AppComponent custom input forms
- widget-facing API response contracts
- generic FastAPI or backend API implementation
- Streamlit dashboard implementation
- DataNode producer design
- SimpleTable schema design

## Route Adjacent Work

- AppComponents and custom forms:
  `.agents/skills/command_center/app_components/SKILL.md`
- APIs and FastAPI:
  `.agents/skills/application_surfaces/api_surfaces/SKILL.md`
- Streamlit dashboards:
  `.agents/skills/dashboards/streamlit/SKILL.md`
- DataNodes:
  `.agents/skills/data_publishing/data_nodes/SKILL.md`
- SimpleTables:
  `.agents/skills/data_publishing/simple_tables/SKILL.md`

## Read First

1. Verify the widget catalog through the CLI:
   - `mainsequence --json cc registered_widget_type list`
   - identify the target `widget_id`
   - `mainsequence --json cc registered_widget_type detail <WIDGET_ID>`
2. `docs/knowledge/command_center/workspaces.md`
3. the local client models, frontend definitions, normalized props models, or examples that define the widget payloads being mounted
4. the current CLI docs if the task uses CLI workflow

If the workspace contains AppComponent widgets, also read:

5. `docs/knowledge/command_center/forms.md`
6. `docs/knowledge/command_center/widget_data_contracts.md`

## Inputs This Skill Needs

Before writing or mutating a workspace, collect or infer:

- workspace metadata:
  title, description, labels, category, source
- layout mode:
  custom grid or auto-grid
- desired widgets:
  one item per widget with concrete intent
- verified `widget_id` values from the CLI registry
- registry detail payloads for those widget ids
- widget instance ids
- external resource ids required by those widgets
- whether the task is:
  - full workspace create/update
  - one-widget mutation

Only inspect local payload source after registry verification and only to resolve mounted widget shape.

If a widget lacks both registry verification and a concrete payload source, stop before building the workspace.

## Required Decisions

For every non-trivial workspace task, decide:

1. Is this a full workspace change or a one-widget mutation?
2. Has the widget type been verified through `registered_widget_type list/detail`?
3. What local source defines the widget instance shape after registry verification?
4. Which fields are shared workspace state versus current-user state?
5. Are bindings and external resource ids fully resolved?

## Build Rules

### 1. Verify the widget type in the CLI registry first

Before mutating or mounting a workspace widget:

1. run `mainsequence --json cc registered_widget_type list`
2. identify the target `widget_id`
3. run `mainsequence --json cc registered_widget_type detail <WIDGET_ID>`

Do not start with local frontend source inspection. The registry is the first source of truth for widget existence and catalog metadata.

### 2. Do not invent widget payloads

Resolve widget instance payloads from:

- local typed models
- local payload builders
- local examples
- local schemas
- local frontend widget `definition.ts` or normalized props models when those exist

If the payload shape is not defined locally and cannot be verified, stop.

### 2.1 Use local source only to refine payload shape

A registered widget type proves the widget exists and gives catalog metadata.

It does not always fully define the mounted widget instance payload.

So the concrete sequence is:

1. verify widget type in the CLI registry
2. inspect registry detail payload
3. inspect local payload source
4. only then mount or update the workspace widget

Use local source to refine:

- `props`
- layout
- bindings
- runtime state shape when relevant

### 3. Shared workspace state and current-user state are different

Treat current-user view state separately from shared workspace structure.

Do not assume that runtime or view-state fields belong in the shared workspace document just because they can appear in a write surface.

### 4. Prefer widget-scoped mutation for one-widget changes

If the user wants to change one mounted widget:

- do not default to rewriting the full workspace
- prefer:
  - `patch_workspace_widget(...)`
  - `delete_workspace_widget(...)`
  - `move_workspace_widget(...)`

Only use a full workspace update when the change is truly workspace-wide or coordinated across multiple widgets.

### 5. File-based workspace workflow is the safe default

Prefer:

1. write the intended workspace document to a local JSON or YAML file
2. create or update through the CLI using that file
3. verify through CLI after creation or mutation

## Review Rules

When reviewing a workspace task, look for:

- guessed widget payloads
- widget work that skipped CLI registry verification
- unknown or unverified `widgetId` values
- missing widget instance ids
- workspace-wide rewrites for one-widget changes
- shared state mixed incorrectly with current-user runtime state
- unresolved external resource ids
- widget trees using nested structures not supported by the local source models

## Validation Checklist

Do not claim success until you have checked:

- the workspace id is correct
- `widgetId` was verified via:
  - `mainsequence --json cc registered_widget_type list`
  - `mainsequence --json cc registered_widget_type detail <WIDGET_ID>`
- widget ids and widget instance ids are correct
- local payload shape was reconciled against the verified widget id
- each widget payload comes from a verified local source
- the chosen mutation mode is correct:
  - full workspace update
  - widget-scoped mutation
- external resource ids are resolved
- the resulting workspace exists and matches the intended structure

If the task is one-widget mutation, also check:

- the correct widget instance id was targeted
- unrelated widgets were not rewritten

## This Skill Must Stop And Escalate When

- a widget type does not exist in the registry
- the widget id is being inferred without registry verification
- a widget exists but no local model or example defines the payload shape
- the task needs AppComponent input or output contracts that are not yet defined
- the target widget instance id is ambiguous
- the workspace mutation is actually an API or dashboard task rather than a Command Center workspace task

Do not guess through widget payloads.
