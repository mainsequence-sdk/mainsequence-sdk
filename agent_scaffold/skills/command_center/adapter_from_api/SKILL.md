---
name: command-center-adapter-from-api
description: Use when building or changing an API so Command Center can consume it through an Adapter from API connection. This skill defines the provider-side API contract standards required for Command Center connection discovery, health checks, operation selection, config fields, secret injection metadata, canonical `TabularFrameResponse` outputs, and tabular response mappings. It does not define general API architecture and does not imply the API may only serve these endpoints.
---

# Command Center Adapter From API

## Overview

Use this skill when an API must be consumable by Command Center through an Adapter from API
connection.

This skill defines the Command Center-facing contract that the API must expose. It does not own the
whole API architecture. An API may serve other clients and routes, but if it is consumed by Command
Center through this adapter, it must also expose the discovery, health, operation, config, secret,
and response-mapping metadata described here.

The runtime flow is:

```text
API with Command Center contract endpoints
  -> Adapter from API connection instance
  -> Connection Query widget
  -> optional Tabular Transform widget
  -> generic tabular consumer widgets
```

Generic table, chart, statistic, curve, transform, and agent-facing data consumers must consume
`core.tabular_frame@v1`. API responses that are provider-native, paginated, nested, or domain
specific need an exact response mapping before they are consumed by generic tabular widgets.

## Scope

This skill owns:

- the provider-side Command Center discovery contract
- the required well-known endpoint
- the required health operation metadata
- operation metadata needed by Connection Query
- public config variable definitions
- secret variable definitions and backend injection metadata
- response mappings for canonical tabular consumption
- API-side validation and maintenance rules for this contract

This skill does not own:

- general FastAPI project structure
- unrelated API routes for non-Command Center clients
- Command Center connection instance creation
- workspace layout or widget mounting
- backend adapter runtime internals
- resource release or deployment orchestration

Route those concerns to the adjacent skills below.

## Route Adjacent Work

- General Main Sequence API creation:
  `.agents/skills/mainsequence/application_surfaces/api_surfaces/SKILL.md`
- Command Center connection instance creation and selection:
  `.agents/skills/mainsequence/command_center/connections/SKILL.md`
- Workspace widgets and binding:
  `.agents/skills/mainsequence/command_center/workspace_builder/SKILL.md`
- AppComponent form-driven actions:
  `.agents/skills/mainsequence/command_center/app_components/SKILL.md`
- API mock/prototype validation before deployment:
  `.agents/skills/mainsequence/command_center/api_mock_prototyping/SKILL.md`
- FastAPI resources, images, jobs, and releases:
  `.agents/skills/mainsequence/platform_operations/orchestration_and_releases/SKILL.md`

## Required Endpoints

An API consumable by Adapter from API must expose these Command Center endpoints in addition to any
other routes it already serves.

### 1. Well-Known Command Center Contract

Required:

```text
GET /.well-known/command-center/connection-contract
```

This endpoint is the discovery source of truth for Command Center. Do not treat `/openapi.json` as
the adapter discovery contract.

The response must be a strict JSON document with at least:

- `contractVersion`
- `adapter`
- `openapi`
- `configVariables`
- `secretVariables`
- `availableOperations`
- `health`

### 2. OpenAPI Document

Required for FastAPI and useful for documentation:

```text
GET /openapi.json
```

This is supplementary metadata. The well-known Command Center contract decides what operations,
config fields, secrets, and response mappings Command Center may use.

### 3. Dedicated Health Endpoint

Required:

```text
GET /health
```

or an equivalent zero-argument route referenced by `health.operationId`.

The health operation should be trivial, fast, read-only, and independent of business query
parameters. Do not use a parameterized data endpoint as a fake health check.

### 4. Business Operation Endpoints

Each queryable API operation must have a stable `operationId` in the well-known contract and a
matching route in the API.

Operation endpoints may return provider-native JSON. For generic tabular consumers, the contract
must declare exactly how that response becomes `core.tabular_frame@v1`.

## Well-Known Contract Shape

Use strict Pydantic models for the contract endpoint. Unknown fields should be rejected during API
implementation unless the contract version explicitly allows them.

Minimum shape:

```json
{
  "contractVersion": 1,
  "adapter": {
    "type": "adapter-from-api",
    "id": "provider.stable-id",
    "title": "Provider Name",
    "description": "Provider contract description."
  },
  "openapi": {
    "url": "https://api.example.com/openapi.json",
    "version": "3.1.0",
    "checksum": "optional-stable-checksum"
  },
  "configVariables": [],
  "secretVariables": [],
  "availableOperations": [],
  "health": {
    "operationId": "health",
    "expectedStatus": 200,
    "timeoutMs": 5000
  }
}
```

Contract field rules:

- `contractVersion` must be explicit so future contract changes can be versioned.
- `adapter.type` must be `adapter-from-api`.
- `adapter.id` must be stable and provider scoped.
- `openapi.url` should point at the API OpenAPI document.
- `configVariables` contains public connection configuration fields.
- `secretVariables` contains credential fields and injection metadata.
- `availableOperations` is the allowlist of operations Command Center may call.
- `health.operationId` must reference an operation declared in `availableOperations`.

## Operation Standards

Every operation listed in `availableOperations` must define:

- `operationId`
- `label`
- `description`
- `method`
- `path`
- `kind`
- `capabilities`
- `requiresTimeRange`
- `supportsVariables`
- `supportsMaxRows`
- `parameters`
- optional `requestBody`
- `responseMappings`
- `cache`

Rules:

- `operationId` must be stable across releases. Renaming it breaks saved connection queries.
- `path` must be relative to the configured API root.
- Queryable operations must use `kind: "query"` or include `query` in `capabilities`.
- Mutating operations must not be exposed as query-capable operations.
- Parameter metadata must include location-specific fields for `path`, `query`, and safe
  user-configurable `headers`.
- Request body metadata must be explicit when the operation requires a body.
- Cache metadata must be explicit so the backend can apply predictable dedupe and TTL behavior.
- Operation definitions are an allowlist. Command Center must not call arbitrary API routes.

## Config And Secrets

Public config fields belong in `configVariables`.

Supported field types:

- `string`
- `number`
- `boolean`
- `select`
- `json`

Secret fields belong in `secretVariables`.

Secret metadata must declare how the backend adapter injects the secret, for example:

```json
{
  "key": "apiToken",
  "label": "API token",
  "type": "secret",
  "required": true,
  "injection": {
    "type": "bearer",
    "name": "Authorization"
  }
}
```

Rules:

- Secret values are stored by the backend, not the browser.
- Secret values must not appear in query payloads, public config, logs, error bodies, cache keys, or
  returned connection instance JSON.
- Query parameters and request bodies should reference logical inputs, not raw credentials.
- Optional secrets are allowed only when the operation can work without them.

## Tabular Consumption Contract

For generic table, chart, statistic, curve, transform, or agent-facing data consumers, the Command
Center consumption contract is:

```text
core.tabular_frame@v1
```

If an operation returns a full canonical frame, the returned JSON must contain:

- `status`
- `columns`
- `rows`
- optional `fields`
- optional `meta`
- optional `source`

For FastAPI providers in Main Sequence repos, full canonical frame endpoints must use:

```python
from mainsequence.client.command_center.data_models import TabularFrameResponse
```

Declare `response_model=TabularFrameResponse` instead of recreating the canonical frame shape
locally.

If an operation returns provider-native JSON, declare a `responseMappings` entry that exposes the
operation as `core.tabular_frame@v1`:

```json
{
  "id": "points_table",
  "label": "Points table",
  "contract": "core.tabular_frame@v1",
  "statusCode": "200",
  "contentType": "application/json",
  "rowsPath": "$.points",
  "fieldTypes": {
    "x": "number",
    "y": "number"
  },
  "timeSeries": {
    "xField": "x",
    "yField": "y",
    "seriesKind": "line"
  }
}
```

Rules:

- `contract` must be `core.tabular_frame@v1` for generic tabular consumption.
- `rowsPath` must point to the array of row objects inside the operation response.
- `fieldTypes` must cover the fields that consumers need for formatting and inference.
- Time-series metadata should be present when the rows are meant for charts, curves, or time-aware
  transforms.
- If the response shape changes, update the response mapping in the same change.
- Do not bind provider-native JSON directly to generic tabular consumers.

## Canonical SDK Model

The SDK canonical model for a full `core.tabular_frame@v1` response is:

```python
mainsequence.client.command_center.data_models.TabularFrameResponse
```

Related SDK models:

- `TabularFrameFieldResponse`
- `TabularFrameMetaResponse`
- `TabularFrameSourceResponse`
- `TabularTimeSeriesMetaResponse`

Use these models when the API operation returns the full canonical frame. If the provider operation
returns provider-native JSON instead, keep the provider response model explicit and declare an exact
`responseMappings` entry that maps it into `core.tabular_frame@v1`.

## Required Decisions

Before implementing or revising an API for Adapter from API consumption, decide:

1. What is the API root URL that a connection instance will configure?
2. Does the API expose `/.well-known/command-center/connection-contract`?
3. Does the API expose a dedicated health operation referenced by `health.operationId`?
4. Which operations are safe for Command Center to call?
5. Which operation IDs must remain stable?
6. Which public config fields does the connection need?
7. Which secrets does the connection need, and how does the backend inject them?
8. Which operations produce generic tabular consumption?
9. For each tabular operation, does the API return a full `core.tabular_frame@v1` document or a
   provider-native response with an exact `responseMappings` entry?
10. If the operation returns a full canonical frame, does it use `TabularFrameResponse` as the
    FastAPI `response_model`?

## Example Implementation Pattern

For a FastAPI provider:

- define strict Pydantic models for the Command Center contract
- set explicit `operation_id` values on routes
- expose `GET /.well-known/command-center/connection-contract`
- expose a zero-argument health route
- expose query operation routes with documented request parameters
- declare every Command Center-callable operation in `availableOperations`
- declare `core.tabular_frame@v1` response mappings for tabular consumption
- use `TabularFrameResponse` when an operation returns the full canonical frame
- keep the API root, well-known contract, and OpenAPI document internally consistent

## Review Rules

When reviewing an Adapter from API provider change, look for:

- missing well-known Command Center contract endpoint
- `/openapi.json` being treated as the discovery contract
- health checks that call parameterized business routes
- unstable or auto-generated operation IDs
- operations missing from `availableOperations`
- operations exposed to Command Center without explicit allowlist metadata
- secret values represented as public config or query payload fields
- tabular responses without `core.tabular_frame@v1` response mappings
- response mappings whose `rowsPath` no longer matches the API response
- missing `fieldTypes` for fields consumed by widgets
- API endpoints returning provider-native JSON directly to generic tabular consumers
- full canonical frame endpoints that do not use `TabularFrameResponse`
- docs or README examples that disagree with the actual contract endpoint

## Validation Checklist

Do not claim the API is consumable by Adapter from API until:

- `GET /.well-known/command-center/connection-contract` exists
- `GET /openapi.json` exists for FastAPI documentation
- the well-known contract returns `contractVersion`, `adapter`, `openapi`, `configVariables`,
  `secretVariables`, `availableOperations`, and `health`
- `adapter.type` is `adapter-from-api`
- `health.operationId` references a declared operation
- health uses a zero-argument or otherwise trivial route
- every Command Center-callable operation has a stable `operationId`
- query-capable operations are explicitly marked as query-capable
- public config and secret fields are separated
- secret injection rules are backend-owned and explicit
- each generic tabular operation declares `core.tabular_frame@v1`
- each full canonical frame operation uses `TabularFrameResponse`
- each provider-native tabular response has an exact `rowsPath`
- field types and time-series hints are present where consumers need them
- the local README documents the Command Center contract endpoints
- any general FastAPI/API work has also followed `application_surfaces/api_surfaces`

## Stop Conditions

Stop and surface the missing backend task when:

- the API does not expose the required well-known contract endpoint
- the API has no dedicated health operation
- operation IDs cannot be made stable
- auth requirements are unknown
- secret injection cannot be described without exposing secret values
- no exact response mapping exists for generic tabular consumption
- the API response shape cannot be mapped to `core.tabular_frame@v1`
- a full canonical frame endpoint cannot import or use `TabularFrameResponse`
- backend Adapter from API runtime support is required but not implemented

Do not create workspace widgets that consume this API until a connection instance can call the API
through the Adapter from API contract.
