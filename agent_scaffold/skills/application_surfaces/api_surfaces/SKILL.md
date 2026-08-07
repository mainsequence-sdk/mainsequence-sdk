---
name: mainsequence-api-surfaces
description: Use this skill when building or changing a FastAPI application in a Main Sequence repository. It owns API structure, request and response contracts, request-user binding, APIDataNode and MetaTable consumption, and local validation. It does not own producer-side DataNode or MetaTable design, job scheduling, image builds, resource discovery, releases, access policy, or Streamlit implementation.
---

# Main Sequence API Surfaces

## Overview

Use this skill for application-facing HTTP APIs in a Main Sequence project.
Project APIs should use FastAPI unless the repository and task explicitly
require preserving another framework.

This skill covers API structure, request and response contracts, request-user
context, and API-side consumption of published project data. Deployment remains
a separate platform workflow.

## This Skill Can Do

- create or modify a FastAPI application
- structure an API for project resource discovery
- define typed request and response models
- keep route handlers thin and contract-driven
- use `APIDataNode` to read published DataNode tables
- use `MetaTable.execute_operation(...)` to read governed MetaTable rows
- add `LoggedUserContextMiddleware` when request-local user context is required
- review whether routes incorrectly duplicate producer logic
- validate routes locally and inspect generated OpenAPI

## This Skill Must Not Claim

This skill does not own:

- DataNode producer design
- MetaTable schema design
- job creation or scheduling
- image creation or pinning
- project resource or release creation
- RBAC or sharing policy
- Streamlit dashboard design or implementation

## Route Adjacent Work

- DataNodes:
  `.agents/skills/mainsequence/data_publishing/data_nodes/SKILL.md`
- MetaTables:
  `.agents/skills/mainsequence/data_publishing/meta_tables/SKILL.md`
- Jobs, images, resources, and releases:
  `.agents/skills/mainsequence/platform_operations/orchestration_and_releases/SKILL.md`
- RBAC and sharing:
  `.agents/skills/mainsequence/platform_operations/access_control_and_sharing/SKILL.md`

Streamlit implementation is project-owned code. Route only its platform
deployment and release verification to orchestration and releases.

## Read First

1. `docs/tutorial/create_your_first_api.md`
2. `docs/tutorial/fastapi_tutorial/index.md`
3. `docs/tutorial/fastapi_tutorial/implementation_details.md`
4. `docs/knowledge/fastapi/index.md`
5. `.agents/skills/mainsequence/platform_operations/orchestration_and_releases/SKILL.md`
   when the API must be deployed

## Inputs This Skill Needs

Before changing code, collect or infer:

- the routes that should exist
- the intended clients
- input parameters and validation rules
- response contracts
- upstream data sources
- whether request-local user context is required
- whether the API already exists as a project resource and release

If the producer contract is unclear, resolve that before implementing the API.

## Required Decisions

For every non-trivial API task, decide:

1. Is the route exposing application logic, or should the data remain a table?
2. Should the route read from `APIDataNode`, `MetaTable`, or another source?
3. Does it require request-local user context?
4. Which request and response models define the public boundary?
5. Is the route composing published data or rebuilding producer logic?
6. Does the API need a project resource, image, and release now?

## Build Rules

### 1. Treat the API as a consumer surface

An API should expose request validation, route-level composition, and stable
response contracts. It should consume published resources instead of silently
reimplementing their producer logic.

### 2. Use FastAPI for project APIs

Do not introduce another framework unless the repository already uses it and
the task explicitly requires preserving that choice.

### 3. Keep deployment compatibility

The implementation should remain compatible with project sync, resource
discovery, image-based deployment, and release creation. Do not describe a local
development server as a completed deployment.

### 4. Keep route handlers thin

A route should primarily:

1. accept validated input
2. call a helper or service
3. return a typed response

Move reusable business logic into project modules instead of duplicating it in
route handlers.

### 5. Use the correct read path

When reading published data:

- use `APIDataNode` for published DataNode tables
- use `MetaTable.execute_operation(...)` for governed MetaTable rows

Do not rebuild producer logic merely because an API needs the result.

### 6. Bind request-user context only when required

Use:

```python
app.add_middleware(LoggedUserContextMiddleware)
```

only when route handlers or helpers actually consume the resolved user through
`request.state.user`. The middleware provides request-local context; it is not
an authorization policy by itself.

### 7. Make models part of the boundary

Use explicit request and response models:

- declare `response_model` for every structured route
- model structured request bodies explicitly
- avoid loose dictionaries for stable public payloads
- document fields and validation constraints

Typed boundaries keep OpenAPI useful and make contract drift visible.

### 8. Use platform notifications for asynchronous work

If work continues after the request returns or spans subprocesses, do not rely
on the immediate HTTP response as the ongoing feedback channel. Use
`mainsequence.client.Notification` for delayed user updates.

### 9. Keep FastAPI documentation complete

For every project API:

- provide explicit route summaries and descriptions
- type every route parameter
- use `Query(...)`, `Path(...)`, `Body(...)`, and related helpers where useful
- add examples, bounds, and enums when they clarify the contract
- document request and response models
- keep an API-local `README.md`

## Review Rules

When reviewing an API change, look for:

- a non-FastAPI implementation without an explicit repository reason
- route handlers containing reusable business logic
- producer logic duplicated inside the API
- missing request or response models
- undocumented or weakly typed route parameters
- middleware added without a concrete use of `request.state.user`
- missing middleware when request-local identity is required
- long-running work using only a one-shot HTTP response for feedback
- code presented as deployed before resource and release verification
- documentation that disagrees with the implemented contract

## Validation Checklist

Do not claim success until you have checked:

- the intended client and route semantics are explicit
- route inputs are intentionally typed
- every structured route has an explicit `response_model`
- route handlers remain thin
- published DataNodes are read through `APIDataNode`
- governed MetaTables are read through `MetaTable.execute_operation(...)`
- request-user middleware is present only when required
- local API tests pass
- generated OpenAPI reflects the intended contract
- deployment claims are backed by resource and release verification

## This Skill Must Stop And Escalate When

- the proposed framework is not FastAPI and the repository does not justify it
- the producer contract needed by a route is unknown
- a stable response contract cannot be determined
- request-local identity is required but its binding path is unclear
- the route would duplicate unpublished producer logic
- documentation and code disagree on the API boundary
- deployment is required but the project resource or release path is unavailable

Do not guess through API contracts.
