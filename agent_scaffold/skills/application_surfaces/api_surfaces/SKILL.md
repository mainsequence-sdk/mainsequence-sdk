---
name: mainsequence-api-surfaces
description: Use this skill when the task is about building or changing APIs in a Main Sequence repository. In a Main Sequence project, project APIs should be implemented as FastAPI project resources. This skill owns FastAPI structure, request and response contracts, request user binding, APIDataNode and SimpleTable consumption inside APIs, and exact widget-facing API response contracts. It does not own producer-side DataNode or SimpleTable design, workspace payloads, or scheduling and release workflows.
---

# Main Sequence API Surfaces

## Overview

Use this skill when the task is about an application-facing HTTP surface in a Main Sequence project.

In this environment, project APIs should be built as FastAPI.

This skill is for FastAPI structure, request/response contracts, request user context, and API-side consumption of project data.

## This Skill Can Do

- create or modify a FastAPI application
- structure the API as a Main Sequence FastAPI project resource
- define request and response models
- keep route handlers thin and contract-driven
- use `APIDataNode` to read published DataNode tables
- use `SimpleTableUpdater.execute_filter(...)` to read simple-table rows
- add `LoggedUserContextMiddleware` when request-local user context is needed
- define exact widget-facing API contracts using SDK response models
- review whether an API route is rebuilding producer logic incorrectly

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
  `agent_scaffold/skills/data_publishing/data_nodes/SKILL.md`
- SimpleTables:
  `agent_scaffold/skills/data_publishing/simple_tables/SKILL.md`
- Command Center workspaces:
  `agent_scaffold/skills/command_center/workspace_builder/SKILL.md`
- AppComponents and custom forms:
  `agent_scaffold/skills/command_center/app_components/SKILL.md`
- Jobs, images, resources, and releases:
  `agent_scaffold/skills/platform_operations/orchestration_and_releases/SKILL.md`
- Streamlit dashboards:
  `agent_scaffold/skills/dashboards/streamlit/SKILL.md`

## Read First

1. `docs/tutorial/create_your_first_api.md`
2. `docs/tutorial/fastapi_tutorial/index.md`
3. `docs/tutorial/fastapi_tutorial/implementation_details.md`
4. `docs/knowledge/fastapi/index.md`
5. `docs/knowledge/command_center/widget_data_contracts.md` when the API feeds a widget directly
6. `docs/knowledge/command_center/forms.md` when the API is part of an AppComponent flow

## Inputs This Skill Needs

Before changing code, collect or infer:

- the routes that should exist
- the intended clients of those routes
- the input parameters and validation rules
- the response contract
- the upstream data sources
- whether the endpoint is generic or widget-facing
- whether route handlers need the logged Main Sequence user

If the upstream producer contract is unclear, stop and resolve that first.

## Required Decisions

For every non-trivial API task, decide:

1. Is this route exposing application logic or should the data stay as a producer table?
2. Should the route read from `APIDataNode`, `SimpleTable`, or something else?
3. Does the route need request-local user context?
4. Is the response a generic business contract or an exact widget contract?
5. Should the route own composition only, or is it incorrectly rebuilding producer logic?

## Build Rules

### 1. An API is a consumer surface, not a data producer

The API should expose:

- request validation
- route logic
- response contracts

It should consume producer resources rather than silently re-implementing them.

### 1.1 In Main Sequence projects, APIs are FastAPI

When a user asks to build an API in a Main Sequence project, the default and expected implementation is FastAPI.

Do not propose or scaffold another framework unless:

- the repository already documents a different framework, and
- the task explicitly requires preserving that framework

### 1.2 The API is a project resource

Treat the API as a deployable Main Sequence project resource, not just a local dev server.

That means the implementation should be compatible with:

- project sync
- project resource discovery
- image-based deployment
- release creation

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

when route handlers or helpers need the resolved Main Sequence user on `request.state`.

This is request-local context, not authentication policy.

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

If an endpoint can reasonably serve a Main Sequence widget, use the SDK response model for that widget contract first.

Do not handcraft loose JSON and hope the widget accepts it.

Only fall back to a generic business response model when:

- the user explicitly asks for a non-widget API contract, or
- the client is clearly not a Main Sequence widget and the route should remain UI-agnostic

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

- a non-FastAPI proposal without an explicit repository reason
- route handlers doing too much
- missing `response_model`
- route parameters that are untyped or weakly documented
- generic response models where a widget-facing contract should have been used
- producer logic duplicated inside routes
- missing request-user middleware when request-local user access is required
- widget-facing payloads built without the SDK contract model
- API routes that really belong in workspace or AppComponent flows instead

## Validation Checklist

Do not claim success until you have checked:

- the API is being implemented as FastAPI
- route inputs are intentionally typed
- route parameters are described richly
- every structured route has an explicit `response_model`
- route handlers are thin
- `APIDataNode` is used when the route reads a published DataNode
- `SimpleTableUpdater.execute_filter(...)` is used when the route reads simple-table rows
- middleware is present when request-local user context is required
- widget-facing endpoints use exact SDK response contracts
- local API docs or route behavior reflect the intended contract

If the endpoint feeds a widget directly, also check:

- the correct SDK response model is used
- the row shape matches the widget contract
- the payload is validated through the FastAPI boundary

## This Skill Must Stop And Escalate When

- the proposed API framework is not FastAPI and the repository does not explicitly justify that choice
- a route is being left without a `response_model`
- route parameters are being left effectively undocumented
- the API route is duplicating producer logic instead of consuming published resources
- the endpoint is supposed to feed a widget but no exact contract model is available
- request-local user context is required but the request binding path is unclear
- the task is actually about workspace mutation or AppComponent form design
- docs and code disagree on the boundary contract

Do not guess through API contracts.
