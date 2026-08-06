---
name: command-center-resource-api
description: Use this skill when a Python or FastAPI application must expose the canonical Command Center resource contracts for paginated collections, user-visible controls, caller-available bulk actions, strict explicit or all-matching selection, preflight, and entity summaries. It owns use of the Command Center SDK Pydantic models and FastAPI helpers. It does not own domain authorization, queryset filtering, destructive side effects, generic FastAPI architecture, workspace layout, or Adapter from API provider discovery.
---

# Command Center Resource API

## Overview

Use this skill when a Python API must provide the same resource-list and
custom-action wire contracts consumed by Command Center and implemented by the
canonical DRF application.

The Python SDK projects those contracts through strict Pydantic models and
optional FastAPI helpers. It does not become a second authorization, filtering,
or domain-behavior authority. The application still owns the complete
authorized queryset, semantic filters, stable ordering, side effects, and
execution-time permission checks.

## This Skill Can Do

- model a canonical paginated resource collection
- publish user-visible search, filter, and ordering controls
- publish only the bulk actions available to the authenticated caller
- validate explicit and all-matching bulk-action selections
- reuse one strict list-filter model for list and all-matching action queries
- model action options, confirmation metadata, and optional preflight routes
- return canonical or domain-specific preflight responses
- model canonical entity summaries
- apply the canonical private discovery cache policy
- generate accurate FastAPI OpenAPI schemas for these contracts

## This Skill Must Not Claim

This skill does not own:

- authentication or authorization
- domain queryset construction
- filtering, searching, counting, or ordering implementation
- destructive-operation policy or side effects
- action-specific success responses
- generic FastAPI project structure
- Adapter from API connection discovery
- workspace documents, widget mounting, or visualization design
- DRF serializer or ViewSet behavior

DRF remains canonical for platform endpoints. SDK models must project the
accepted wire contract without redefining its semantics.

## Route Adjacent Work

- General FastAPI structure and project API implementation:
  `.agents/skills/mainsequence/application_surfaces/api_surfaces/SKILL.md`
- Adapter from API provider discovery and connection operations:
  `.agents/skills/mainsequence/client/command_center/api_contracts/adapter_from_api/SKILL.md`
- Workspace creation and mounted-widget mutation:
  `.agents/skills/mainsequence/client/command_center/workspaces/builder/SKILL.md`
- AppComponent request and response contracts:
  `.agents/skills/mainsequence/client/command_center/workspaces/widgets/app_components/SKILL.md`
- Table and `core.tabular_frame@v1` contracts:
  `.agents/skills/mainsequence/client/command_center/workspaces/widgets/tables/SKILL.md`
- FastAPI resources, images, releases, and deployment:
  `.agents/skills/mainsequence/platform_operations/orchestration_and_releases/SKILL.md`

## Read First

1. `docs/knowledge/command_center/resource_sdk.md`
2. `mainsequence/client/command_center/sdk/resource/collections.py`
3. `mainsequence/client/command_center/sdk/resource/controls.py`
4. `mainsequence/client/command_center/sdk/resource/actions.py`
5. `mainsequence/client/command_center/sdk/resource/summary.py`
6. `mainsequence/client/command_center/sdk/fastapi/resource.py`
7. `mainsequence/client/command_center/sdk/fastapi/actions.py`
8. the owning application's list filters, authorization path, action behavior,
   and summary contract

## Canonical Collection Contract

Every resource collection governed by this contract returns all of these keys:

```json
{
  "count": 125,
  "next": null,
  "previous": null,
  "results": [],
  "controls": {
    "search": null,
    "filters": [],
    "ordering": []
  },
  "actions": []
}
```

Use `CanonicalResourceCollection[RowModel]` and
`build_fastapi_resource_collection(...)` for FastAPI routes.

The application must perform operations in this order:

1. start from the caller-authorized dataset;
2. apply every active host scope, search term, and semantic filter;
3. calculate `count` from that complete filtered dataset;
4. apply deterministic stable ordering;
5. slice with `limit` and `offset`;
6. build the envelope with the current request URL.

Never count an unfiltered dataset and never paginate before filtering.
`next` and `previous` must preserve the active query and remain present as
`null` when unavailable. Do not configure FastAPI to exclude `None` fields on
collection responses.

Use the `resource_limit_offset` dependency for the canonical validated page
parameters. A domain may set a different accepted default only when that public
contract is deliberate and documented.

## Controls

`controls` describes what a generic Command Center client should render. It is
not a dump of every ORM or internal query parameter.

- `search.placeholder` explains the actual searchable concepts.
- `search.fields` documents the backend fields covered by free-text search.
- `filters` contains only user-visible text, boolean, or select controls.
- `ordering` contains only supported user-visible ordering fields.

Hidden host scope, such as the active Project UID, stays outside rendered
controls. It must still be applied by the list route and preserved when an
all-matching action resolves targets.

## Action Discovery

Selection-based collection actions use:

```text
GET <collection>/bulk-actions/
```

Return `ResourceBulkActionDiscoveryResponse`, or include the same action
definitions in the collection envelope. Use
`build_fastapi_bulk_action_discovery(...)` for a standalone FastAPI discovery
route.

Each action definition contains its stable id, label, safe relative POST
endpoint, supported selection modes, options, and optional confirmation,
tone, and preflight endpoint. Action endpoints must be same-origin relative
paths ending in `/`.

Discovery is caller-contextual. Include only actions currently available to
the authenticated caller, then reauthorize again during preflight and
execution. Discovery metadata never grants permission.

Collection and standalone action discovery must use:

```text
Cache-Control: private, no-store
Vary: Authorization, Cookie
```

Use the SDK FastAPI builders so these headers are applied consistently.

## Selection And Execution Request

Use one strict `ResourceBulkActionRequest[OptionsModel]` body for preflight and
execution:

```json
{
  "selection": {
    "mode": "explicit",
    "uids": ["11111111-1111-4111-8111-111111111111"]
  },
  "options": {}
}
```

or:

```json
{
  "selection": {
    "mode": "all_matching",
    "query": {
      "search": "risk",
      "filters": {"kind": "service"}
    }
  },
  "options": {}
}
```

Rules:

- `explicit` contains one or more public UUIDs and deduplicates them.
- `all_matching` means every authorized row matching the complete active
  semantic query, not every row in the unfiltered collection.
- `all_matching.query.filters` must pass through the exact same strict filter
  model and filtering function used by the list route.
- Search semantics must be identical between list and action resolution.
- Pagination and presentation keys such as `limit`, `offset`, `ordering`,
  `page`, and `page_size` are not selection filters.
- Hidden host scope must be reapplied by trusted route context; do not rely on
  the client to submit it as a visible control.
- Use `ResourceBulkActionEmptyOptions` when an action has no options.
- Reject undeclared action options and filters.
- Do not accept legacy `selected_uids`, `select_all`, or `current_url` keys.

Resolve and reauthorize every target at execution time. Never execute directly
from a stale discovery response or preflight result.

## Preflight

An optional preflight route receives the exact same selection and options body
as execution.

Use `ResourceBulkActionPreflightResponse` for the common advisory shape:

- required `allowed`
- required `matched_count`
- optional `detail`
- optional `blockers`
- `warnings`

Use `build_fastapi_bulk_action_preflight_response(...)` so an allowed result
returns HTTP 200 and a blocked result returns HTTP 409. Declare both response
schemas in OpenAPI.

A domain may define a stricter action-specific preflight model when it needs
structured per-item impact or blockers. Preserve that structure; do not flatten
it into the generic response merely for uniformity. It must still use the same
selection/options request and the same 200/409 meaning.

Preflight is advisory. Execution repeats target resolution, authorization, and
validation.

## Summary

Use `EntitySummary` for:

```text
GET <collection>/{uid}/summary/
```

It supports entity identity, badges, inline and highlighted fields, stats,
helpers, label management, extensions, and an optional warning. Do not restore
legacy top-level `labels` or `labelable` fields.

For FastAPI summary routes, exclude absent optional values so the response
matches canonical DRF omission semantics.

## OpenAPI Rules

- Set concrete generic response models such as
  `CanonicalResourceCollection[ProjectRow]`.
- Set the exact generic request type such as
  `ResourceBulkActionRequest[DeleteOptions]` on preflight and execution.
- Document HTTP 409 with the preflight response model.
- Keep required envelope fields required in the generated schema.
- Do not use untyped dictionaries where a canonical or domain-specific model
  exists.
- Keep successful execution responses action-specific rather than inventing a
  universal action-result envelope.

## Validation

Before reporting completion, verify:

1. collection results, count, and links use the complete filtered dataset;
2. ordering is deterministic;
3. controls contain only renderable user controls;
4. hidden scope is retained for list and all-matching resolution;
5. action discovery is caller-specific and privately cached;
6. explicit and all-matching requests reject legacy and unknown fields;
7. preflight and execution have identical request schemas;
8. preflight documents and returns 200 or 409 correctly;
9. execution reauthorizes every selected target;
10. summaries use the canonical strict model;
11. OpenAPI exposes the actual request and response models.

## Stop Conditions

Stop and request direction when:

- the canonical DRF list, summary, or action contract is ambiguous;
- all-matching selection cannot reuse the list filtering function;
- the owning application lacks execution-time authorization;
- a requested field would expose hidden scope, credentials, or secret values;
- a new action category is not selection-based and has no approved contract;
- implementation would require changing the canonical DRF wire contract.
