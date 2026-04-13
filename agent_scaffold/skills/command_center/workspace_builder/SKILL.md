---
name: command-center-workspace-builder
description: Use this skill when the task is about creating, updating, validating, or reviewing Main Sequence Command Center workspaces. This skill owns workspace documents, widget instance payload resolution, layout decisions, shared versus user state, and widget-scoped mutation. It does not own AppComponent form contracts, API implementation, or Streamlit dashboards.
---

# Command Center Workspace Builder

## Overview

Use this skill when the task is about a Command Center workspace document or a mounted widget inside that workspace.

This skill is for workspace structure, widget payload resolution, and safe workspace mutation.

## This Skill Can Do

- create a new Command Center workspace
- update an existing workspace document
- add widgets to a workspace
- resolve widget instance payloads from local models or examples
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

1. `docs/knowledge/command_center/workspaces.md`
2. the local client models or examples that define the widget payloads being mounted
3. the current CLI docs if the task uses CLI workflow

If the workspace contains AppComponent widgets, also read:

4. `docs/knowledge/command_center/forms.md`
5. `docs/knowledge/command_center/widget_data_contracts.md`

## Inputs This Skill Needs

Before writing or mutating a workspace, collect or infer:

- workspace metadata:
  title, description, labels, category, source
- layout mode:
  custom grid or auto-grid
- desired widgets:
  one item per widget with concrete intent
- widget instance ids
- external resource ids required by those widgets
- whether the task is:
  - full workspace create/update
  - one-widget mutation

If a widget lacks a concrete payload source, stop before building the workspace.

## Required Decisions

For every non-trivial workspace task, decide:

1. Is this a full workspace change or a one-widget mutation?
2. Is the widget type registered and known?
3. What local source defines the widget instance shape?
4. Which fields are shared workspace state versus current-user state?
5. Are bindings and external resource ids fully resolved?

## Build Rules

### 1. Do not invent widget payloads

Resolve widget instance payloads from:

- local typed models
- local payload builders
- local examples
- local schemas

If the payload shape is not defined locally and cannot be verified, stop.

### 2. Registered widget types are necessary but not sufficient

A registered widget type proves the widget exists.

It does not fully define the mounted widget instance payload.

You still need a local source of truth for:

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
- unknown or unverified `widgetId` values
- missing widget instance ids
- workspace-wide rewrites for one-widget changes
- shared state mixed incorrectly with current-user runtime state
- unresolved external resource ids
- widget trees using nested structures not supported by the local source models

## Validation Checklist

Do not claim success until you have checked:

- the workspace id is correct
- widget ids and widget instance ids are correct
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
- a widget exists but no local model or example defines the payload shape
- the task needs AppComponent input or output contracts that are not yet defined
- the target widget instance id is ambiguous
- the workspace mutation is actually an API or dashboard task rather than a Command Center workspace task

Do not guess through widget payloads.
