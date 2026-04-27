---
name: command-center-connections
description: Use this skill when the task is about selecting, creating, validating, or documenting Command Center connections before workspace widgets consume data. This skill owns connection type discovery, connection instance readiness, query model/output-contract checks, connection-first workspace dataflow, and routing backend adapter gaps. Use this before creating data-consuming widgets. It does not own workspace layout, widget JSON mutation, AppComponent form contracts, or generic FastAPI implementation.
---

# Command Center Connections

## Overview

Use this skill when a Command Center workspace needs backend-owned data access.

Connections are resources, not presentation widgets. A workspace should resolve data through:

```text
Connection instance
  -> Connection Query widget
  -> Tabular Transform widget when reshaping is needed
  -> table, chart, statistic, curve, or other consumer widget
```

Do not create table, chart, statistic, curve, or other data-consuming widgets until the required
connection instance and query model are known.

## This Skill Can Do

- discover connection types and instances through `mainsequence.client.command_center.connections`
- decide whether an existing connection instance can support the requested workspace data
- inspect `ConnectionType.queryModels` and `usageGuidance`
- verify that a query model can publish the required output contract
- require generic tabular consumers to receive `core.tabular_frame@v1`
- identify missing backend adapter, query model, permission, cache, or response-normalization work
- route workspace mutation to `workspace_builder` after connection requirements are resolved

## This Skill Must Not Claim

This skill must not claim ownership of:

- workspace layout or narrative design
- writing workspace JSON or mounted widget payloads
- AppComponent custom forms or action contracts
- generic FastAPI endpoint design
- backend resource and release creation
- Streamlit dashboards

## Route Adjacent Work

- Workspace design and widget selection:
  `.agents/skills/command_center/workspace_design/SKILL.md`
- Workspace JSON creation and widget mutation:
  `.agents/skills/command_center/workspace_builder/SKILL.md`
- Adapter from API connection workflow:
  `.agents/skills/command_center/adapter_from_api/SKILL.md`
- AppComponent forms and action API contracts:
  `.agents/skills/command_center/app_components/SKILL.md`
- API implementation:
  `.agents/skills/application_surfaces/api_surfaces/SKILL.md`
- Jobs, images, resources, and releases:
  `.agents/skills/platform_operations/orchestration_and_releases/SKILL.md`

## Read First

1. SDK connection models:
   - `mainsequence/client/command_center/connections.py`
   - `mainsequence/client/command_center/data_models.py` when validating `core.tabular_frame@v1`
     output shape
2. Widget registry detail for the source and consumer widgets:
   - `mainsequence cc registered_widget_type list --json`
   - `mainsequence cc registered_widget_type detail <WIDGET_ID> --json`
3. Connection type catalog data:
   - `ConnectionType`
   - `ConnectionInstance`
   - connection `usageGuidance`
   - connection `queryModels`
   - connection examples and required permissions
4. Command Center workspace docs only when the task is ready to mount widgets.

## Inputs This Skill Needs

Before recommending or using a connection, collect or infer:

- target data source or backend system
- existing `ConnectionInstance` id or lookup filters
- target `ConnectionType.type_id`
- connection status and scope
- relevant `queryModelId`
- typed query payload shape
- advertised output contracts for the query model
- whether a time range, variables, or row limit are supported
- whether downstream consumers need raw records, a canonical tabular frame, or a specialized widget contract

## Required Decisions

For every connection-backed workspace task, decide:

1. Does an appropriate connection instance already exist?
2. Which connection type and query model owns the requested data access?
3. Is the connection status usable, or does health/configuration need attention first?
4. Does the query model publish `core.tabular_frame@v1` for generic tabular consumers?
5. Is a Tabular Transform widget needed for aggregate, pivot, unpivot, projection, or other analytical reshaping?
6. Which downstream widgets consume the source or transform `dataset` output?
7. Is any backend adapter, permission, cache, or response-normalization work missing?

## Build Rules

### 1. Connection first

Before creating a data-consuming widget, identify the connection path:

1. connection type
2. connection instance
3. query model
4. typed query payload
5. output contract

Do not store endpoint URLs, credentials, route fragments, provider ids, or mutable display labels as
authoritative widget props.

### 2. Use the connection query source widget for workspace data

The workspace source node should be a Connection Query widget. It should persist stable connection
and query references, then publish the selected response frame as `dataset`.

Downstream table, chart, statistic, curve, and similar widgets must bind to a published dataset.
They should not query connection instances directly.

### 3. Use Tabular Transform for analytical reshaping

Use a Tabular Transform widget when the workspace needs:

- aggregate
- pivot
- unpivot
- projection
- other inspectable tabular reshaping

Do not hide analytical reshaping inside binding transforms or consumer widgets.

### 4. Generic consumers need the canonical tabular frame

For generic table, chart, statistic, curve, and agent-facing data consumers, the final bound value
must match `core.tabular_frame@v1`:

- `status`
- `columns`
- `rows`
- optional `fields`
- optional `meta`
- optional `source`

If an API returns raw arrays, paginated JSON, nested provider payloads, or other ad hoc records,
first create, or select an existing, connection instance of type Adapter from API. That adapter owns
normalization into `core.tabular_frame@v1` before any generic tabular consumer is bound.

When an API operation already returns the full canonical frame, validate it against
`mainsequence.client.command_center.data_models.TabularFrameResponse`. If it returns
provider-native JSON, the Adapter from API contract must declare the exact response mapping into
`core.tabular_frame@v1`.

### 5. Treat `usageGuidance` as agent-facing contract

When choosing a connection, read the connection type `usageGuidance`. It should explain:

- when to use the connection
- when not to use it
- public and secure configuration fields
- query models and payloads
- output contracts
- backend adapter ownership
- important constraints and permissions

If `usageGuidance` is vague or contradicts `queryModels`, stop and surface the mismatch.

## Review Rules

When reviewing connection-backed workspace work, look for:

- data-consuming widgets created before a connection instance was resolved
- consumer widgets querying backend systems directly
- endpoint URLs, tokens, route fragments, or provider ids stored in widget props
- missing `ConnectionType.queryModels` validation
- missing `usageGuidance` review
- raw JSON bound into generic tabular consumers
- API operations claiming canonical tabular output without matching `TabularFrameResponse`
- analytical reshaping hidden in binding transforms
- missing backend adapter or response normalization work

## Validation Checklist

Do not claim the connection flow is ready until you have checked:

- connection type exists
- connection instance exists and is in a usable status
- connection type `usageGuidance` and `queryModels` were reviewed
- selected `queryModelId` is valid for the connection type
- typed query payload matches the selected query model
- output contract is compatible with downstream widgets
- generic tabular consumers receive `core.tabular_frame@v1`
- full canonical tabular frames match `TabularFrameResponse`
- missing backend adapter work is explicitly documented
- workspace handoff identifies the Connection Query widget, optional Tabular Transform widget, and downstream consumers

## This Skill Must Stop And Escalate When

- no suitable connection instance exists
- the required query model does not exist
- the query model output contract does not match downstream needs
- connection metadata and registry details disagree
- backend adapter support is required but unavailable
- the task has moved from connection selection into workspace JSON mutation

Do not guess through connection boundaries.
