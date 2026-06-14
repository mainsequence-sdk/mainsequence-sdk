---
name: command-center-adapter-from-api
description: Use when building or changing an API so Command Center can consume it through an Adapter from API connection. This skill defines the provider-side API contract standards required for Command Center connection discovery, health checks, operation selection, config fields, secret injection metadata, canonical `TabularFrameResponse` outputs, and optional response-mapping metadata for frontend/editor context or future explicit transforms. It does not define general API architecture and does not imply the API may only serve these endpoints.
---

# Command Center Adapter From API

## Overview

Use this skill when an API must be consumable by Command Center through an Adapter from API
connection.

This skill defines the Command Center-facing contract that the API must expose. It does not own the
whole API architecture. An API may serve other clients and routes, but if it is consumed by Command
Center through this adapter, it must also expose the discovery, health, operation, config, secret,
and any optional response-mapping metadata described here.

The runtime flow is:

```text
API with Command Center contract endpoints
  -> Adapter from API connection instance
  -> Connection Query widget
  -> optional Tabular Transform widget
  -> generic tabular consumer widgets
```

Generic table, chart, statistic, curve, transform, and agent-facing data consumers must consume
an actual `core.tabular_frame@v1` payload at the consumption boundary. API responses that are
provider-native, paginated, nested, or domain specific are still provider-native even when the
contract includes `responseMappings`. A mapping may describe how a response could be interpreted by
frontend/editor tooling or a future explicit transform, but the current backend adapter must not
hot-path validate, extract JSONPath rows, coerce schemas, or reshape provider responses because a
mapping exists.

## Connection CLI And Transport Modes

Use the Main Sequence CLI to create or update the Adapter from API connection instance that points
Command Center at the provider API.

Backend/deployed API mode:

```bash
mainsequence cc connection create-adapter-from-api \
  --name "Provider API" \
  --api-base-url https://api.example.com \
  --workspace-uid <workspace-uid> \
  --default
```

Direct local/tunnel development mode:

```bash
cloudflared tunnel --url http://127.0.0.1:8021

mainsequence cc connection create-adapter-from-api \
  --name "Provider API local" \
  --debug-api-base-url https://example.trycloudflare.com \
  --workspace-uid <workspace-uid> \
  --default
```

Patch an existing Adapter from API connection:

```bash
mainsequence cc connection patch-adapter-from-api <connection-uid> \
  --api-base-url https://api.example.com

mainsequence cc connection patch-adapter-from-api <connection-uid> \
  --debug-api-base-url https://example.trycloudflare.com
```

Inspect connection state:

```bash
mainsequence cc connection list --filter type_id=command_center.adapter_from_api
mainsequence cc connection detail <connection-uid>
```

CLI characteristics:

- `--api-base-url` creates backend mode public config with `apiBaseUrl`.
- `--debug-api-base-url` creates direct mode public config with `transportMode=direct`,
  `debugApiBaseUrl`, `compiledContractSource=direct`, and derived contract/OpenAPI URLs.
- `--public-config-json` and `--public-config-file` accept a full publicConfig object when URL
  options are not enough.
- `--config-json` and `--config-file` set public non-secret `configValues`.
- `--secure-config-json` and `--secure-config-file` send secret values as `secureConfig`; returned
  connection details expose only `secureFields`, not the raw secret values.
- `patch-adapter-from-api` must target an existing `command_center.adapter_from_api` connection; do
  not use it for other connection types.
- Do not include `applicationBindings` in Adapter from API public config. The
  `AdapterFromApiConnectionPublicConfig` model rejects it.

Direct mode is critical during development. It lets a workspace test the connection against a local
API before any resource release or API deployment. Run the local API, expose it with a Cloudflare
tunnel, create or patch the connection with `--debug-api-base-url`, and validate the workspace
connection/query flow directly. This avoids constant API deployments while iterating on the
contract, operation metadata, response shape, and widget behavior. Switch to `--api-base-url` only
when the deployed API is ready to be the stable backend target.

## Scope

This skill owns:

- the provider-side Command Center discovery contract
- the required well-known endpoint
- the required health operation metadata
- operation metadata needed by Connection Query
- public config variable definitions
- secret variable definitions and backend injection metadata
- optional response-mapping metadata for frontend/editor context and future explicit transforms
- API-side validation and maintenance rules for this contract

This skill does not own:

- general FastAPI project structure
- unrelated API routes for non-Command Center clients
- general Command Center connection management beyond the Adapter from API CLI workflow above
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
config fields, secrets, and optional response-mapping metadata Command Center may use.

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

Operation endpoints may return a full canonical `core.tabular_frame@v1` payload or provider-native
JSON. `responseMappings` may document an intended tabular interpretation, but they are not a
runtime guarantee that provider-native JSON becomes `core.tabular_frame@v1`.

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
- optional `responseMappings`
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

If an operation returns provider-native JSON, it may declare a `responseMappings` entry that
describes a tabular interpretation for frontend/editor metadata or future explicit transforms:

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

- `responseMappings` are optional metadata in the current Adapter from API flow.
- A mapping does not make provider-native JSON a `core.tabular_frame@v1` runtime payload.
- The backend must not hot-path validate the upstream body against `responseMappings`.
- The backend must not extract JSONPath rows, coerce schemas, or reshape the response merely
  because a mapping exists.
- If a mapping describes tabular interpretation, `contract` should be `core.tabular_frame@v1`.
- If `rowsPath` is present, it should point to the array of row objects inside the operation
  response.
- If `fieldTypes` is present, it should cover the fields that frontend/editor tooling or future
  transforms need for formatting and inference.
- Time-series metadata is useful when the mapped rows are meant for charts, curves, or time-aware
  transforms.
- If the response shape changes, update the response mapping in the same change.
- Do not claim provider-native JSON is safe for generic tabular consumers just because a
  `responseMappings` entry exists.
- If generic tabular widgets need to consume the operation directly today, return
  `TabularFrameResponse` from the API operation.

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
returns provider-native JSON instead, keep the provider response model explicit. Add
`responseMappings` only as metadata; do not treat it as runtime conversion into
`core.tabular_frame@v1`.

## Required Decisions

Before implementing or revising an API for Adapter from API consumption, decide:

1. What is the API root URL that a connection instance will configure?
2. Does the API expose `/.well-known/command-center/connection-contract`?
3. Does the API expose a dedicated health operation referenced by `health.operationId`?
4. Which operations are safe for Command Center to call?
5. Which operation IDs must remain stable?
6. Which public config fields does the connection need?
7. Which secrets does the connection need, and how does the backend inject them?
8. Which operations directly return a full `core.tabular_frame@v1` payload for generic tabular
   consumption?
9. For provider-native responses, is optional `responseMappings` metadata useful, and what explicit
   transform or future feature would consume it?
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
- declare `responseMappings` only as optional metadata when useful; do not rely on them for runtime
  normalization
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
- provider-native responses being treated as `core.tabular_frame@v1` because a mapping exists
- response mappings whose `rowsPath` no longer matches the API response when the mapping is present
- missing `fieldTypes` for fields that declared mappings or future transforms need
- API endpoints returning provider-native JSON directly to generic tabular consumers without an
  explicit transform path
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
- each operation that must feed generic tabular consumers directly returns `core.tabular_frame@v1`
- each full canonical frame operation uses `TabularFrameResponse`
- provider-native responses are not represented as direct generic tabular outputs solely through
  `responseMappings`
- optional response mappings, when present, have accurate `rowsPath`, `fieldTypes`, and time-series
  hints for their intended metadata or transform use
- the local README documents the Command Center contract endpoints
- any general FastAPI/API work has also followed `application_surfaces/api_surfaces`

## Stop Conditions

Stop and surface the missing backend task when:

- the API does not expose the required well-known contract endpoint
- the API has no dedicated health operation
- operation IDs cannot be made stable
- auth requirements are unknown
- secret injection cannot be described without exposing secret values
- a provider-native response is being treated as generic tabular output only because
  `responseMappings` exists
- the API response shape must feed generic tabular consumers directly but cannot return
  `TabularFrameResponse`
- a full canonical frame endpoint cannot import or use `TabularFrameResponse`
- backend Adapter from API runtime support is required but not implemented

Do not create workspace widgets that consume this API until a connection instance can call the API
through the Adapter from API contract.
