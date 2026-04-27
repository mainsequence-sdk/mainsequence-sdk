---
name: command-center-workspace-builder
description: Use this skill when the task is about creating, updating, validating, or reviewing Main Sequence Command Center workspaces after the workspace design is known. This skill owns workspace documents, widget instance payload resolution, safe mutation, shared versus user state, widget-scoped mutation, connection-backed source widget mounting, and grounding those decisions against the richer widget-type registry contract plus the SDK command_center client models. Use workspace_design first when the task is still about deciding which widgets, layout, narrative, or visualization strategy should express the user intent; use command-center-connections first when source data or query contracts are unresolved. Source order is strict: registry detail first, SDK client models second, local Main Sequence repository docs/models third only when the first two still leave instance payload questions unresolved. Main Sequence is platform-first: if a mounted widget or AppComponent depends on a project API, that API must already exist as a FastAPI project resource and have a corresponding FastAPI ResourceRelease before the workspace flow is considered usable. Resource and release creation belong to the orchestration-and-releases skill. It does not own AppComponent form contracts, API implementation, workspace design, or Streamlit dashboards.
---

# Command Center Workspace Builder

## Overview

Use this skill when the task is about a Command Center workspace document or a mounted widget inside that workspace, and the high-level workspace design is already known.

This skill is for workspace structure execution, widget payload resolution, safe workspace mutation, and grounding those decisions against the actual `mainsequence.client.command_center` client models.

If the task is still deciding what the workspace should communicate, which widgets should be used, or how the workspace should be organized, use `.agents/skills/command_center/workspace_design/SKILL.md` first.

## This Skill Can Do

- create a new Command Center workspace
- update an existing workspace document
- add widgets to a workspace
- execute a known workspace design
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

- deciding the workspace narrative or information architecture
- choosing widgets from user intent when the design is not settled
- deciding whether a table, chart, KPI, note, AppComponent, or diagnostic widget best expresses the business idea
- AppComponent custom input forms
- widget-facing API response contracts
- generic FastAPI or backend API implementation
- Streamlit dashboard implementation
- tabular or operational data producer design
- SimpleTable schema design

## Route Adjacent Work

- Workspace design, widget selection, and visualization strategy:
  `.agents/skills/command_center/workspace_design/SKILL.md`
- AppComponents and custom forms:
  `.agents/skills/command_center/app_components/SKILL.md`
- Connection-backed data access and query contract selection:
  `.agents/skills/command_center/connections/SKILL.md`
- predeployment mock API contract validation:
  `.agents/skills/command_center/api_mock_prototyping/SKILL.md`
- APIs and FastAPI:
  `.agents/skills/application_surfaces/api_surfaces/SKILL.md`
- Jobs, images, resources, and releases:
  `.agents/skills/platform_operations/orchestration_and_releases/SKILL.md`
- Streamlit dashboards:
  `.agents/skills/dashboards/streamlit/SKILL.md`
- SimpleTables:
  `.agents/skills/data_publishing/simple_tables/SKILL.md`

## Read First

0. If widget selection, layout narrative, or visualization strategy is not already decided, use:
   - `.agents/skills/command_center/workspace_design/SKILL.md`
1. Verify the widget catalog through the CLI:
   - `mainsequence cc registered_widget_type list --json`
   - identify the target `widget_id`
   - `mainsequence cc registered_widget_type detail <WIDGET_ID> --json`
2. The SDK client models in `mainsequence/client/command_center/`:
   - `workspace.py`
   - `connections.py` when source widgets depend on backend-owned connections
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
- connection instance, connection type, query model, and output contract for each connection-backed source widget
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
8. Does every connection-backed source widget have a resolved connection instance, query model, typed query payload, and output contract?
9. Do generic table, chart, statistic, curve, or agent-facing consumers receive `core.tabular_frame@v1` from a source or transform widget?
10. If a mounted widget depends on a project API, does that API already exist as a FastAPI project resource with a FastAPI `ResourceRelease`?
11. Does the workspace require a non-standard or more complex visualization that should be authored through `echarts-spec` instead of improvised chart props?
12. If the workspace uses echarts-spec, prefer theme-aware colors and chart palettes over hardcoded hex values: use semantic theme tokens like "$theme.primary", "$theme.secondary", "$theme.warning", "$theme.success", "$theme.positive", "$theme.negative" for emphasis, use text tokens like "$theme.foreground" and "$theme.muted-foreground" for labels and annotations, and when a chart needs multiple series or scales, use the theme chart palettes for categorical, sequential, and diverging color assignment instead of improvising custom color arrays.

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
- `connections.py` for connection type and connection instance discovery
- `data_models.py` for `TabularFrameResponse`, the SDK canonical `core.tabular_frame@v1` model,
  plus field schema, frame metadata, and source context rules
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

### 3.1 Resolve connection-backed data before mounting consumers

Before mounting table, chart, statistic, curve, or similar data consumers:

1. identify the source Connection Query widget
2. verify the selected connection instance exists and is usable
3. verify the connection type and `queryModelId`
4. verify the typed query payload
5. verify the published output contract
6. add a Tabular Transform widget if analytical reshaping is required
7. bind consumers to the source or transform `dataset` output

Do not store endpoint URLs, credentials, backend route fragments, provider ids, or mutable
connection display labels as authoritative widget props.

Generic tabular consumers must receive `core.tabular_frame@v1`. If the upstream API or connection
returns raw arrays, paginated JSON, nested provider payloads, or other ad hoc records, normalize
through an Adapter from API connection first, then use an explicit transform when analytical
reshaping is still required.

When a project API or AppComponent legitimately returns a full canonical frame, ground the contract
against `mainsequence.client.command_center.data_models.TabularFrameResponse`. Source-specific
runtime details belong in `source.context`, not top-level widget payload fields.

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

## Bindings

Bindings are canonical instance-level graph edges. They live on the target widget instance in
`bindings`, not in `props`, and not in widget-type metadata.

One binding is always defined as:

- target widget instance + target input id
- source widget instance + source output id
- optional ordered transform steps

Persisted shape:

- `bindings[inputId] = WidgetPortBinding`
- `bindings[inputId] = WidgetPortBinding[]` when the target input declares `cardinality: "many"`

A `WidgetPortBinding` minimally contains:

- `sourceWidgetId`
- `sourceOutputId`
- optional `transformSteps`

Treat `transformSteps` as canonical. Legacy `transformId`, `transformPath`, and
`transformContractId` may still appear only as backward-compatible mirrors for older persisted
workspaces.

Do not describe bindings loosely as “widget A uses widget B”. In this platform, bindings are always
port-to-port:

- source widget output port -> target widget input port

Resolve bindings from the target widget outward:

1. Resolve the target widget IO surface first.
   - use static `io` when present
   - use instance-level `resolveIo(...)` when the widget exposes dynamic ports
   - if the widget has no resolved inputs yet, stop; the widget is not ready for binding authoring
2. For each target input, record:
   - `inputId`
   - accepted contracts
   - `required`
   - `cardinality`
   - `effects`
3. Choose a source widget instance that actually publishes the required output id.
4. If the source output is structured JSON or an array, resolve binding transforms before contract
   validation.
   - supported transform steps:
     - `select-array-item`
     - `extract-path`
5. Validate the transformed contract against the target input `accepts`.
6. Treat the binding as usable only when the resolved status is `valid`.

A binding can exist in workspace JSON and still be unusable at runtime. The runtime may resolve a
binding as:

- `unbound`
- `missing-source`
- `missing-output`
- `contract-mismatch`
- `self-reference-blocked`
- `transform-invalid`
- `valid`

Input `effects` explain what the binding actually changes:

- `drives-value`: upstream value becomes an effective widget or generated-field value
- `drives-default`: upstream value changes a default
- `drives-options`: upstream value changes available options
- `drives-validation`: upstream value changes validation rules
- `drives-render`: upstream value changes rendered output

Runtime and persistence rules:

- store graph edges in `widget.bindings`, never in ad hoc raw props
- binding changes clear that target widget's `runtimeState`
- the widget settings `Bindings` tab and the workspace graph editor edit the same canonical binding model
- for `cardinality: "many"` inputs, preserve order and store an array of bindings for that input id

Dynamic-IO rule:

- some widgets cannot be bound safely until instance configuration has materialized their ports
- `AppComponent` is the clearest example: request inputs and response outputs are generated from the
  saved operation or binding spec, so do not guess ports before `resolveIo(...)` exposes them

Concrete examples:

- a Connection Query widget publishes `dataset`; a Tabular Transform widget can consume and republish `dataset`
- `main-sequence-ai-agent-terminal` accepts one input with `cardinality: "many"`, so several
  upstream widget contexts can feed one terminal
- widgets that implement `buildAgentSnapshot(...)` may also publish a synthetic `agent-context`
  output; that output is platform-generated and still counts as a normal bindable source

Review rule:

- for every binding in a proposed workspace, verify the full tuple:
  - target widget instance id
  - target input id
  - source widget instance id
  - source output id
  - transform steps, if any
  - final resolved contract
  - final resolved status


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
- connection-backed consumers missing a source or transform `dataset` binding
- generic tabular consumers bound to raw JSON instead of `core.tabular_frame@v1`
- full canonical tabular payloads that drift from `TabularFrameResponse`
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
- connection-backed source widgets have verified connection instance, connection type, query model, typed query payload, and output contract
- generic tabular consumers receive `core.tabular_frame@v1`
- full canonical tabular frames match `TabularFrameResponse`
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
