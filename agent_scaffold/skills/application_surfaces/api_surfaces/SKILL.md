---
name: mainsequence-api-surfaces
description: Use this skill when the task is about building or changing APIs in a Main Sequence repository. In a Main Sequence project, project APIs should be implemented as FastAPI project resources. Unless the user explicitly says the API is standalone or for a non-Command Center client, assume the API is meant to be Command Center integrated, load the related Command Center skills, and use Command Center SDK response models whenever the endpoint can reasonably match them. Main Sequence is platform-first: a Command Center-facing project API is not considered usable until it exists as a FastAPI project resource and has a corresponding FastAPI ResourceRelease. Resource and release creation belong to the orchestration-and-releases skill. This skill owns FastAPI structure, request and response contracts, request user binding, APIDataNode and SimpleTable consumption inside APIs, and exact widget-facing API response contracts. It does not own producer-side DataNode or SimpleTable design, workspace payloads, or scheduling and release workflows.
---

# Main Sequence API Surfaces

## Overview

Use this skill when the task is about an application-facing HTTP surface in a Main Sequence project.

In this environment, project APIs should be built as FastAPI.

Unless the user explicitly says otherwise, assume the API is intended to be Command Center integrated and to serve Command Center widgets, AppComponents, or other Command Center flows.

This skill is for FastAPI structure, request/response contracts, request user context, and API-side consumption of project data.

## This Skill Can Do

- create or modify a FastAPI application
- structure the API as a Main Sequence FastAPI project resource
- define request and response models
- keep route handlers thin and contract-driven
- use `APIDataNode` to read published DataNode tables
- use `SimpleTableUpdater.execute_filter(...)` to read simple-table rows
- add `LoggedUserContextMiddleware` when request-local user context is needed
- assume Command Center is the default API consumer unless the user clearly says otherwise
- load the related Command Center skills when the API feeds widgets, AppComponents, or workspaces
- define exact widget-facing API contracts using SDK response models whenever the endpoint can reasonably match those contracts
- review whether an API route is rebuilding producer logic incorrectly
- requiring Command Center-facing APIs to be treated as deployed FastAPI resources/releases, not just local servers

## This Skill Must Not Claim

This skill must not claim ownership of:

- DataNode producer design
- SimpleTable schema design
- workspace document creation or mutation
- AppComponent custom form design
- job creation, scheduling, image pinning, or releases
- Streamlit dashboard implementation
- non-FastAPI API framework choices for Main Sequence project APIs

## Route Adjacent Work

- DataNodes:
  `.agents/skills/data_publishing/data_nodes/SKILL.md`
- SimpleTables:
  `.agents/skills/data_publishing/simple_tables/SKILL.md`
- Command Center workspaces:
  `.agents/skills/command_center/workspace_builder/SKILL.md`
- AppComponents and custom forms:
  `.agents/skills/command_center/app_components/SKILL.md`
- Adapter from API provider-side Command Center connection endpoints:
  `.agents/skills/command_center/adapter_from_api/SKILL.md`
- predeployment mock API contract validation:
  `.agents/skills/command_center/api_mock_prototyping/SKILL.md`
- Jobs, images, resources, and releases:
  `.agents/skills/platform_operations/orchestration_and_releases/SKILL.md`
- Streamlit dashboards:
  `.agents/skills/dashboards/streamlit/SKILL.md`

## Read First

1. `docs/tutorial/create_your_first_api.md`
2. `docs/tutorial/fastapi_tutorial/index.md`
3. `docs/tutorial/fastapi_tutorial/implementation_details.md`
4. `docs/knowledge/fastapi/index.md`
5. `docs/knowledge/command_center/widget_data_contracts.md`
6. `docs/knowledge/command_center/forms.md`
7. `.agents/skills/command_center/app_components/SKILL.md`

Also load:

8. `.agents/skills/command_center/workspace_builder/SKILL.md` when the API is tied to mounted widgets, workspace payloads, or workspace mutation
9. `.agents/skills/command_center/adapter_from_api/SKILL.md` when the API must be consumed through a Command Center Adapter from API connection
10. `.agents/skills/command_center/api_mock_prototyping/SKILL.md` when the contract should be validated in AppComponent mock mode before backend deployment
11. `.agents/skills/platform_operations/orchestration_and_releases/SKILL.md` when the API must become usable from Command Center or an AppComponent

Do not wait for the user to say "Command Center" explicitly if the API is being built as a platform UI surface. That is the default assumption in Main Sequence projects.

## Inputs This Skill Needs

Before changing code, collect or infer:

- the routes that should exist
- the intended clients of those routes
- the input parameters and validation rules
- the response contract
- the upstream data sources
- whether the endpoint is generic or widget-facing
- whether route handlers need the logged Main Sequence user
- whether the API already exists as a FastAPI project resource with a FastAPI `ResourceRelease`

Default assumption:

- the intended client is Command Center unless the user clearly specifies another consumer
- if that assumption is wrong for this task, make that explicit before building the contract

If the upstream producer contract is unclear, stop and resolve that first.

## Required Decisions

For every non-trivial API task, decide:

1. Is this route exposing application logic or should the data stay as a producer table?
2. Should the route read from `APIDataNode`, `SimpleTable`, or something else?
3. Does the route need request-local user context?
4. Is this API serving Command Center by default, or did the user explicitly ask for a different consumer?
5. Does this task require the AppComponents skill, the workspace-builder skill, or both?
6. Which Command Center SDK response model fits this endpoint, and if none fits, why is a generic business contract justified?
7. Should the route own composition only, or is it incorrectly rebuilding producer logic?
8. Must this API already be usable from Command Center or an AppComponent, and if so, does the FastAPI resource plus FastAPI `ResourceRelease` already exist?
9. Is this endpoint specifically designed to serve workspace visualizations, and if so, should it live under `/workspace` to keep concerns properly separated?
10. Should this API be consumed through Adapter from API, and if so, does it expose the well-known Command Center connection contract?

## Build Rules

### 1. An API is a consumer surface, not a data producer

The API should expose:

- request validation
- route logic
- response contracts

It should consume producer resources rather than silently re-implementing them.

### 1.1 Default to Command Center as the consumer

In a Main Sequence project, assume API work is for Command Center unless the user explicitly says the API is:

- purely external
- purely backend-to-backend
- a standalone non-Command-Center integration

That means:

- prefer widget-facing contracts by default
- load `.agents/skills/command_center/app_components/SKILL.md`
- also load `.agents/skills/command_center/workspace_builder/SKILL.md` when the API is coupled to mounted workspace widgets or workspace payloads
- load `.agents/skills/command_center/adapter_from_api/SKILL.md` when the API will be consumed by connection-first workspace dataflow
- try to use the existing Command Center SDK response model before inventing a new response shape

Do not default to a generic standalone API mindset in this repository.

### 1.2 In Main Sequence projects, APIs are FastAPI

When a user asks to build an API in a Main Sequence project, the default and expected implementation is FastAPI.

Do not propose or scaffold another framework unless:

- the repository already documents a different framework, and
- the task explicitly requires preserving that framework

### 1.3 The API is a project resource

Treat the API as a deployable Main Sequence project resource, not just a local dev server.

That means the implementation should be compatible with:

- project sync
- project resource discovery
- image-based deployment
- release creation

### 1.4 Command Center-facing APIs must have a FastAPI release

Main Sequence is platform-first.

If a project API is meant to be used from Command Center, AppComponents, or other platform UI surfaces, it is not considered usable until:

- the FastAPI project resource exists
- the corresponding FastAPI `ResourceRelease` exists

Do not present a local dev server, a local-only route, or an undiscovered API file as a finished platform API.

This skill owns the API contract and implementation. It does not own resource or release creation. Route that work to:

- `.agents/skills/platform_operations/orchestration_and_releases/SKILL.md`

### 1.5 Keep workspace-visualization routes under `/workspace`

When building endpoints that are specifically designed to serve workspace visualizations, place them under a dedicated `/workspace` route prefix.

Examples:

- `/workspace/...`
- `/workspace/charts/...`
- `/workspace/widgets/...`

This keeps workspace-facing visualization concerns separated from:

- generic business API routes
- external integration routes
- producer-side publishing logic

Do not scatter workspace-visualization endpoints across unrelated route groups when they are clearly meant to support Command Center workspace rendering.

### 2. Keep route handlers thin

A route should mainly:

1. accept validated input
2. call a helper or service
3. return a stable response model

### 3. Use the right read path

When reading published data:

- use `APIDataNode` for published DataNode tables
- use `SimpleTableUpdater.execute_filter(...)` for simple-table rows

Do not rebuild producer logic just because the API needs the result.

### 4. Bind request user context only when needed

Use:

```python
app.add_middleware(LoggedUserContextMiddleware)
```

only when route handlers or helpers actually need the resolved Main Sequence user on `request.state.user`.

This is request-local context, not authentication policy.

Do not add this middleware just because the API is Command Center-facing.

Many Command Center APIs do not need request-local user context and should avoid the middleware entirely when they:

- do not read `request.state.user`
- do not pass request-local user context into helpers or services
- do not need user-scoped binding or user-specific data resolution at request time

Use the middleware only when the route really depends on request-local user context. Otherwise keep the API surface free of that coupling.

### 5. Response models are part of the boundary

Use explicit request and response models.

This keeps:

- OpenAPI clear
- validation close to the boundary
- drift easier to catch

For Main Sequence project APIs:

- always declare a `response_model`
- do not leave route output as loose untyped dictionaries unless the route truly returns no structured payload
- if the route has a structured request body, model that request explicitly too

### 6. Widget-facing APIs must use exact SDK contracts

In Main Sequence projects, prefer widget-facing response contracts by default.

If an endpoint can reasonably serve a Main Sequence widget, AppComponent, or workspace surface, use the SDK response model for that contract first.

Do not handcraft loose JSON and hope the widget accepts it.

For generic Command Center tabular consumers, use:

```python
from mainsequence.client.command_center.data_models import TabularFrameResponse
```

Declare `response_model=TabularFrameResponse` when the route returns the full
`core.tabular_frame@v1` payload.

Only fall back to a generic business response model when:

- the user explicitly asks for a non-widget API contract, or
- the client is clearly not a Main Sequence widget and the route should remain UI-agnostic
- no existing Command Center SDK response model fits the endpoint, and that mismatch is explicit in the task reasoning

If the endpoint participates in an AppComponent flow, also use the AppComponents skill to validate the input-form side of the contract.

### 6.1 Use notification response contracts for immediate client feedback

When an API action needs immediate user-facing acknowledgment, prefer a response contract carrying:

- `"x-ui-role": "notification"`

Use the SDK `NotificationDefinition` model for this immediate feedback path instead of returning a loose acknowledgment dictionary.

This is the default response-side feedback contract for:

- AppComponent-triggered actions
- Command Center actions that should acknowledge success, warning, validation failure, or initiation of work

Do not force this onto read-only data endpoints or widget data endpoints whose job is to return structured business data rather than action feedback.

### 6.2 Use platform notifications for long-running asynchronous updates

If the work continues after the request returns, spans subprocesses, or becomes a long-running background workflow, do not rely on the immediate HTTP response as the ongoing user-feedback channel.

Use `mainsequence.client.Notification` for asynchronous user updates in those cases.

The immediate API response may still acknowledge that work started, but ongoing or delayed user feedback belongs in platform notifications rather than a one-shot HTTP response model.

### 7. Keep FastAPI documentation rich

For FastAPI routes in a Main Sequence project:

- route summaries should be explicit
- route descriptions should be explicit
- router parameters should always be explicitly typed
- router parameters should always have rich FastAPI metadata such as `description`
- use `Query(...)`, `Path(...)`, `Body(...)`, and similar parameter helpers instead of leaving route parameters undocumented
- add examples, bounds, enums, and other validation metadata when they clarify the contract
- request and response models should be documented
- route examples should be added when they materially clarify the contract
- the API folder should have a `README.md` explaining what the API does

## Review Rules

When reviewing an API change, look for:

- a Command Center-facing API being treated like a generic standalone API
- missing use of an existing Command Center SDK response model when the endpoint could have matched it
- missing load/use of the AppComponents skill when the endpoint is part of an AppComponent flow
- missing load/use of the workspace-builder skill when the endpoint is tied to a mounted workspace widget
- missing load/use of the Adapter from API skill when the endpoint is meant for connection-first Command Center data consumption
- a non-FastAPI proposal without an explicit repository reason
- workspace-visualization endpoints placed outside a dedicated `/workspace` route group without a clear repository reason
- route handlers doing too much
- missing `response_model`
- `LoggedUserContextMiddleware` added even though the route does not consume `request.state.user`
- route parameters that are untyped or weakly documented
- generic response models where a widget-facing contract should have been used
- tabular Command Center data routes returning a full frame without `TabularFrameResponse`
- action endpoints returning loose dict acknowledgments where `NotificationDefinition` should have been used
- long-running or spanning subprocess workflows trying to use a one-shot HTTP response instead of `mainsequence.client.Notification` for asynchronous user updates
- producer logic duplicated inside routes
- missing request-user middleware when request-local user access is required
- widget-facing payloads built without the SDK contract model
- API routes that really belong in workspace or AppComponent flows instead
- Command Center-facing APIs being treated as complete before a FastAPI resource and FastAPI `ResourceRelease` exist

## Validation Checklist

Do not claim success until you have checked:

- the API is being implemented as FastAPI
- the intended consumer is explicit, and Command Center was used as the default assumption unless the user overrode it
- an existing Command Center SDK response model was used whenever the endpoint could reasonably match it
- full `core.tabular_frame@v1` routes use `TabularFrameResponse`
- route inputs are intentionally typed
- route parameters are described richly
- every structured route has an explicit `response_model`
- route handlers are thin
- `APIDataNode` is used when the route reads a published DataNode
- `SimpleTableUpdater.execute_filter(...)` is used when the route reads simple-table rows
- middleware is present when request-local user context is required
- middleware is absent when the route does not consume `request.state.user`
- endpoints built specifically for workspace visualizations live under `/workspace`
- immediate client feedback endpoints use `NotificationDefinition` with `"x-ui-role": "notification"` when the route is acknowledging an action rather than returning business data
- long-running or subprocess-spanning workflows use `mainsequence.client.Notification` for asynchronous user updates instead of relying on a one-shot HTTP response
- widget-facing endpoints use exact SDK response contracts
- APIs consumed through Adapter from API expose the well-known Command Center connection contract
- local API docs or route behavior reflect the intended contract
- any Command Center-facing API that is meant to be usable now already exists as a FastAPI project resource
- any Command Center-facing API that is meant to be usable now already has a FastAPI `ResourceRelease`

If the endpoint feeds a widget directly, also check:

- the correct SDK response model is used
- the row shape matches the widget contract
- the payload is validated through the FastAPI boundary

If the endpoint is part of an AppComponent or workspace flow, also check:

- the related Command Center skill was loaded
- the API contract does not drift from the AppComponent or workspace expectations

## This Skill Must Stop And Escalate When

- the proposed API framework is not FastAPI and the repository does not explicitly justify that choice
- the likely consumer is Command Center but the task is being treated as a generic API without loading the relevant Command Center skill
- a route is being left without a `response_model`
- `LoggedUserContextMiddleware` is being added without any concrete route-level need for `request.state.user`
- workspace-visualization endpoints are being mixed into unrelated route groups without a clear reason
- route parameters are being left effectively undocumented
- the API route is duplicating producer logic instead of consuming published resources
- the endpoint is supposed to feed a widget but no exact contract model is available
- request-local user context is required but the request binding path is unclear
- the task is actually about workspace mutation or AppComponent form design
- docs and code disagree on the boundary contract
- the API is expected to be usable from Command Center now, but the FastAPI resource or FastAPI `ResourceRelease` does not exist yet

Do not guess through API contracts.
