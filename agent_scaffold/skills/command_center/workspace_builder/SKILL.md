---
name: command-center-workspace-builder
description: Use this skill when the task is about creating, updating, validating, or reviewing Main Sequence Command Center workspaces. This skill owns workspace documents, widget instance payload resolution, layout decisions, shared versus user state, widget-scoped mutation, and grounding those decisions against the richer widget-type registry contract plus the SDK command_center client models. Source order is strict: registry detail first, SDK client models second, local Main Sequence repository docs/models third only when the first two still leave instance payload questions unresolved. Main Sequence is platform-first: if a mounted widget or AppComponent depends on a project API, that API must already exist as a FastAPI project resource and have a corresponding FastAPI ResourceRelease before the workspace flow is considered usable. Resource and release creation belong to the orchestration-and-releases skill. It does not own AppComponent form contracts, API implementation, or Streamlit dashboards.
---

# Command Center Workspace Builder

## Overview

Use this skill when the task is about a Command Center workspace document or a mounted widget inside that workspace.

This skill is for workspace structure, widget payload resolution, safe workspace mutation, and grounding those decisions against the actual `mainsequence.client.command_center` client models.

## This Skill Can Do

- create a new Command Center workspace
- update an existing workspace document
- add widgets to a workspace
- verify widget types in the CLI registry before mounting or mutating them
- inspect the richer widget detail contract before opening repository source
- inspect SDK client models in `mainsequence/client/command_center/` before opening frontend implementation files
- decide whether widget detail plus SDK models are already enough to build the widget safely
- resolve widget instance payloads from Main Sequence docs/models in this repository only after registry verification and widget-detail review
- decide between full workspace update and widget-scoped mutation
- patch one widget safely
- move or delete a widget safely
- verify shared workspace data versus current-user state
- verify runtime ownership semantics such as `execution-owner` versus `consumer`
- review a workspace payload for guessed or invalid widget configuration
- keep versioned workspace and widget JSON snapshots under `workspaces/` until the user accepts the change

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
- predeployment mock API contract validation:
  `.agents/skills/command_center/api_mock_prototyping/SKILL.md`
- APIs and FastAPI:
  `.agents/skills/application_surfaces/api_surfaces/SKILL.md`
- Jobs, images, resources, and releases:
  `.agents/skills/platform_operations/orchestration_and_releases/SKILL.md`
- Streamlit dashboards:
  `.agents/skills/dashboards/streamlit/SKILL.md`
- DataNodes:
  `.agents/skills/data_publishing/data_nodes/SKILL.md`
- SimpleTables:
  `.agents/skills/data_publishing/simple_tables/SKILL.md`

## Read First

1. Verify the widget catalog through the CLI:
   - `mainsequence cc registered_widget_type list --json`
   - identify the target `widget_id`
   - `mainsequence cc registered_widget_type detail <WIDGET_ID> --json`
2. The SDK client models in `mainsequence/client/command_center/`:
   - `workspace.py`
   - `data_models.py`
   - `app_component.py` when the workspace contains AppComponent widgets or editable form payloads
3. `docs/knowledge/command_center/workspaces.md`
4. the local Main Sequence docs/models/examples in this repository that define the widget payloads being mounted
5. the current CLI docs if the task uses CLI workflow

If the workspace contains AppComponent widgets, also read:

6. `docs/knowledge/command_center/forms.md`
7. `docs/knowledge/command_center/widget_data_contracts.md`
8. `.agents/skills/platform_operations/orchestration_and_releases/SKILL.md` when mounted widgets depend on project APIs that must be usable from Command Center
9. `.agents/skills/command_center/api_mock_prototyping/SKILL.md` when the workspace should validate an AppComponent/API contract in `mock-json` mode before deployment

## Command Center Mental Model

Think in terms of these objects:

- workspace:
  shared document that stores mounted widgets, layout, shared controls, and workspace metadata
- widget type:
  registered capability identified by `widget_id` and described by widget registry detail
- widget instance:
  one mounted configured object inside a workspace document, identified by its own instance id
- workspace user state:
  current-user runtime/view state stored separately from shared workspace content
- saved widget or saved group:
  import-layer snapshot/template, not a live linked widget instance inside the workspace

## Inputs This Skill Needs

Before writing or mutating a workspace, collect or infer:

- workspace metadata:
  title, description, labels, category, source
- layout mode:
  custom grid or auto-grid
- desired widgets:
  one item per widget with concrete intent
- verified `widget_id` values from the CLI registry
- widget detail payloads for those widget ids
- relevant SDK model sources in `mainsequence/client/command_center/`
- widget instance ids
- external resource ids required by those widgets
- whether any mounted widget depends on a project API that already exists as a FastAPI project resource with a FastAPI `ResourceRelease`
- whether the task is:
  - full workspace create/update
  - one-widget mutation

Use this source order strictly:

1. Registry detail first
   - `mainsequence cc registered_widget_type detail <WIDGET_ID> --json`
   - this is the first contract source
2. SDK client models second
   - `mainsequence/client/command_center/workspace.py`
   - `mainsequence/client/command_center/data_models.py`
   - `mainsequence/client/command_center/app_component.py` when relevant
   - use these before any broader repository exploration
3. Repository docs/models third
   - local docs, examples, payload builders, and typed models
   - only if registry detail plus SDK client models still leave instance-level payload questions unresolved

Only inspect Main Sequence docs/models/examples in this repository after registry verification and SDK model review, and only to resolve mounted widget shape that remains unclear.

If a widget lacks registry verification, usable widget detail, or a concrete payload source, stop before building the workspace.

## Required Decisions

For every non-trivial workspace task, decide:

1. Is this a full workspace change or a one-widget mutation?
2. Has the widget type been verified through `registered_widget_type list/detail`?
3. Is widget detail plus the relevant SDK model already enough to author the widget safely?
4. If not, what Main Sequence repository source defines the remaining instance-level payload shape?
5. Which fields are shared workspace state versus current-user state?
6. Is the widget a runtime `execution-owner`, `consumer`, or `local-ui` widget?
7. Are bindings and external resource ids fully resolved?
8. If a mounted widget depends on a project API, does that API already exist as a FastAPI project resource with a FastAPI `ResourceRelease`?
9. Does the workspace require a non-standard or more complex visualization that should be authored through `echarts-spec` instead of improvised chart props?
10. If the workspace uses echarts-spec, prefer theme-aware colors and chart palettes over hardcoded hex values: use semantic theme tokens like "$theme.primary", "$theme.secondary", "$theme.warning", "$theme.success", "$theme.positive", "$theme.negative" for emphasis, use text tokens like "$theme.foreground" and "$theme.muted-foreground" for labels and annotations, and when a chart needs multiple series or scales, use the theme chart palettes for categorical, sequential, and diverging color assignment instead of improvising custom color arrays.
## Build Rules

### 1. Verify the widget type in the CLI registry first

Before mutating or mounting a workspace widget:

1. run `mainsequence cc registered_widget_type list --json`
2. identify the target `widget_id`
3. run `mainsequence cc registered_widget_type detail <WIDGET_ID> --json`

Do not start with secondary repository source inspection. The registry is the first source of truth for widget existence and catalog metadata. Widget detail is now expected to show enough contract detail to guide exploration before falling back to repository source.

### 1.1 Use widget detail as a build contract, not only an existence check

During widget exploration, do not treat `registered_widget_type detail` as a shallow existence check.

Use widget detail first to answer:

- what the widget is for
- the widget `widgetVersion`
- whether it is a workspace `execution-owner`, `consumer`, or `local-ui` widget
- which configuration modes and major fields exist
- whether inputs/outputs are static, dynamic, or consumer-only
- what capability modes the widget supports
- what authoring steps an agent must follow before the widget is usable
- which examples already match the requested workspace change

If widget detail already answers the authoring question safely, do not open implementation files just to “look around”.

### 1.2 API-backed widget flows require a FastAPI resource release

Main Sequence is platform-first.

If a mounted widget or AppComponent depends on a project API, that API must already exist as:

- a FastAPI project resource
- a corresponding FastAPI `ResourceRelease`

before the workspace flow is considered usable from Command Center.

Do not treat a local dev server, a local-only API route, or a discovered-but-unreleased FastAPI resource as enough for workspace readiness.

This skill does not create resources or releases. Route that work to:

- `.agents/skills/platform_operations/orchestration_and_releases/SKILL.md`

### 2. Read the SDK client models before frontend implementation files

After registry verification, inspect the relevant `mainsequence.client.command_center` model before opening frontend implementation files.

Use:

- `workspace.py` for shared workspace shape and widget-scoped mutation methods
- `data_models.py` for Data Node-family tabular contracts, field provenance, and date/range payload rules
- `app_component.py` for editable form structures relevant to workspace-mounted AppComponent workflows

Treat these SDK models as the first concrete client interaction surface.

### 2.1 Use `echarts-spec` for non-standard or complex visualizations

If a workspace requires a visualization that is not standard and needs more complex charting guidance, use `echarts-spec`.

Do not improvise a complex chart configuration directly from guesswork.

First extract the chart requirements from `registered_widget_type detail <WIDGET_ID> --json`, including:

- widget capabilities
- supported modes
- configuration fields
- IO shape
- examples and authoring hints when present

Then use `echarts-spec` to author the visualization in a way that stays grounded in the actual widget contract.

### 2.2 Use local source only to refine unresolved instance payload shape

A registered widget type proves the widget exists and gives catalog metadata.

Widget detail should now also expose the machine-readable widget contract, including at least:

- `widgetVersion`
- configuration summary and fields
- runtime ownership and execution semantics
- IO mode, static inputs/outputs, or dynamic IO summary
- capability summaries and supported modes
- agent authoring hints and examples

The SDK client models should then tell you what the client can actually read, write, or mutate safely.

It still does not always fully define the mounted widget instance payload.

So the concrete sequence is:

1. verify widget type in the CLI registry
2. inspect widget detail and extract the richer contract
3. inspect the relevant SDK model
4. decide whether that contract is enough to author the widget safely
5. inspect local payload source only for unresolved instance-level questions
6. only then mount or update the workspace widget

Use Main Sequence docs/models/examples in this repository only after registry detail and SDK client models still leave unresolved questions. Then use them to refine:

- `props`
- layout
- bindings
- runtime state shape when relevant

If widget detail and repository source disagree, or if SDK model and repository source disagree, stop and surface that mismatch instead of guessing.

### 3. Do not invent widget payloads

Resolve widget instance payloads from:

- widget registry detail
- SDK client models
- local typed models
- local payload builders
- local examples
- local schemas

If the payload shape is not defined by those sources and cannot be verified, stop.

### 4. Shared workspace state and current-user state are different

Treat current-user view state separately from shared workspace structure.

Do not assume that runtime or view-state fields belong in the shared workspace document just because they can appear in a write surface.

As a default rule:

- workspace JSON owns shared structure, widget props, bindings, layout, and shared controls
- workspace user state owns current-user runtime/view state such as selected control values and widget runtime state when the platform separates them

### 5. Respect runtime ownership

When reasoning about mounted widgets, classify them correctly:

- `execution-owner`:
  owns canonical runtime execution or data fetching for that widget surface
- `consumer`:
  reads published upstream data and must not invent its own canonical runtime fetch path
- `local-ui`:
  local rendering utility with no shared runtime execution ownership

Do not design workspace changes that violate the registered runtime ownership model.

### 6. Saved widgets and groups are import-layer snapshots

Treat saved widgets and saved groups as import templates, not as live-linked mounted widgets.

When importing from the saved-widget layer:

- widget type identity is preserved
- mounted widget instance ids are new
- imports are not assumed to live-resync from the saved library unless the system explicitly supports that

Do not assume a saved-widget record id is the same thing as a mounted workspace widget instance id.

### 7. Prefer widget-scoped mutation for one-widget changes

If the user wants to change one mounted widget:

- do not default to rewriting the full workspace
- prefer:
  - `patch_workspace_widget(...)`
  - `delete_workspace_widget(...)`
  - `move_workspace_widget(...)`

Only use a full workspace update when the change is truly workspace-wide or coordinated across multiple widgets.

### 8. Export and version the current workspace before mutation

Before modifying an existing workspace:

1. export the current workspace through the CLI:
   - `mainsequence cc workspace detail <WORKSPACE_ID> --json`
2. save that exported JSON under a repository folder:
   - `workspaces/`
3. keep versioned JSON files there until the user explicitly accepts the workspace change

When you create or revise widget payloads during the task:

1. save those widget JSON drafts under:
   - `workspaces/widgets/`
2. keep versioned widget JSON files there until the user explicitly accepts the change

This rule exists to guarantee recoverability and make workspace mutation reviewable.

Do not treat an in-memory payload or a one-off CLI mutation as sufficient change control for workspace editing.

### 9. File-based workspace workflow is the safe default

Prefer:

1. write the intended workspace document to a local JSON or YAML file
2. create or update through the CLI using that file
3. verify through CLI after creation or mutation

## Review Rules

When reviewing a workspace task, look for:

- guessed widget payloads
- widget work that skipped CLI registry verification
- widget work that skipped SDK client model review when one exists
- unknown or unverified `widgetId` values
- missing widget instance ids
- workspace mutation attempted without first exporting the current workspace JSON
- widget payloads changed without saving versioned JSON drafts under `workspaces/widgets/`
- workspace-wide rewrites for one-widget changes
- shared state mixed incorrectly with current-user runtime state
- runtime ownership violations such as consumer widgets inventing canonical fetch paths
- unresolved external resource ids
- widget trees using structures not supported by the Main Sequence repository source models

## Validation Checklist

Do not claim success until you have checked:

- the workspace id is correct
- `widgetId` was verified via:
  - `mainsequence cc registered_widget_type list --json`
  - `mainsequence cc registered_widget_type detail <WIDGET_ID> --json`
- for existing workspaces, the pre-change workspace export was captured through:
  - `mainsequence cc workspace detail <WORKSPACE_ID> --json`
- the current workspace JSON was saved under `workspaces/`
- widget JSON drafts created during the task were saved under `workspaces/widgets/`
- versioned workspace/widget JSON files were preserved pending user acceptance
- widget detail was reviewed for `widgetVersion`, configuration, runtime, IO, capabilities, agent hints, and examples
- the relevant SDK client model was reviewed when one exists
- widget ids and widget instance ids are correct
- any mounted widget that depends on a project API points to a FastAPI project resource that already exists
- any mounted widget that depends on a project API points to a FastAPI `ResourceRelease` that already exists
- local payload shape was reconciled against the verified widget id, widget-detail contract, and SDK model
- the chosen mutation mode is correct:
  - full workspace update
  - widget-scoped mutation
- shared workspace state and current-user state were not mixed incorrectly
- runtime ownership semantics were respected
- external resource ids are resolved
- the resulting workspace exists and matches the intended structure

If the task is one-widget mutation, also check:

- the correct widget instance id was targeted
- unrelated widgets were not rewritten

If the task involves saved-widget import, also check:

- mounted widget instance ids are newly assigned
- widget type identity was preserved correctly
- no live-link behavior was assumed unless explicitly supported

## This Skill Must Stop And Escalate When

- a widget type does not exist in the registry
- the widget id is being inferred without registry verification
- widget detail, SDK model, and repository source disagree materially
- a widget exists but no SDK or Main Sequence repository source defines the payload shape safely enough to proceed
- the task needs AppComponent input or output contracts that are not yet defined
- the target widget instance id is ambiguous
- the workspace mutation is actually an API or dashboard task rather than a Command Center workspace task

Do not guess through widget payloads.
