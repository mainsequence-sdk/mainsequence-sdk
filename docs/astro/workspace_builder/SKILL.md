---
name: command-center-workspace-builder
description: Use this skill when you need to create or update a Main Sequence Command Center workspace from a client-side CLI,
  especially when the agent can inspect local client models, SDK types, generated schemas,
  or local examples but cannot rely on server source code. 
  This skill covers discovering registered widget types, resolving widget-specific payload shapes from local models,
  building workspace and user-state payloads, creating the workspace through the API,
  and verifying the shared-vs-user-state split.
---

# Command Center Workspace Builder

## Overview


Use this skill when the task is to:

- create a new Command Center workspace
- add widgets to a workspace
- update widget configuration
- convert local client models into valid Command Center API payloads
- verify that shared workspace data and current-user state were persisted correctly

## Non-Negotiable Rules

- Do not assume access to backend source code.
- Treat local client models, SDK types, generated schemas, and local examples as the source of truth.
- Do not invent widget instance payloads.
- Do not send a workspace create or update request until every widget has a known payload contract.
- Treat unknown `widgetId` values as a blocker unless the environment explicitly documents an escape hatch and the user asks to use it.
- Prefer local typed models or local examples over freehand JSON.
- When the user asks to mutate one specific widget, prefer widget-scoped workspace mutation methods over rewriting the full workspace document.
- If the target widget instance id is ambiguous, confirm the correct widget id before mutating anything.

## Required Inputs

Collect or infer these before writing anything:


- workspace metadata:
  title, description, labels, category, source
- target layout mode:
  custom grid or auto-grid
- desired widgets:
  one item per widget with business intent
- external resource identifiers required by the widgets:
  for example asset ids, node ids, portfolio ids, table ids, or job ids

If a widget lacks a concrete configuration source, stop and ask for it.

## ClI commands

Always  refer to the canonical CLI document in the https://github.com/mainsequence-sdk/mainsequence-sdk/tree/main/docs/cli
Some workflows may involve also usage of the cli and the full mainsequence SDK use also the canonical documentation

## Workflow



### 1. Discover candidate widget types

List candidate widget types, then fetch each selected widget type in detail.

For each chosen `widgetId`, record:

- `widgetId`
- `title`
- `kind`
- `source`
- `requiredPermissions`
- `schema`
- `io`
- `defaultPresentation`

This catalog metadata validates the widget type, but it does not fully define the widget instance payload.

### 2. Resolve the widget instance contract locally

For each widget you intend to mount, locate a client-side source that defines the instance shape.

Acceptable sources include:

- a local Pydantic model
- a local dataclass
- a typed payload builder
- a local JSON schema
- a local example payload
- a widget factory in the client codebase

For each widget, resolve:

- instance `id`
- `widgetId`
- display title if used
- `props`
- `layout`
- `position` if applicable
- `bindings`
- `companions`
- whether it may appear inside `row.children`
- any supported `runtimeState`

If the local client model does not define these fields clearly, do not guess.

### 3. Separate shared workspace data from current-user state

The workspace write surface accepts some current-user view state inside the workspace payload, but the backend stores that data in per-user workspace state.

Treat these as user-state fields:

- `controls.timeRange.selectedRange`
- `controls.timeRange.customStartMs`
- `controls.timeRange.customEndMs`
- `controls.refresh.selectedIntervalMs`
- `widgets[*].runtimeState`

Treat these as shared workspace fields:

- workspace metadata
- `requiredPermissions`
- `grid`
- `layoutKind`
- `autoGrid`
- `companions`
- shared `controls` config
- shared `widgets` definitions

You may include current-user view state during create if the local client contract expects it, but verify the created workspace through the CLI after creation.

### 4. Build the workspace payload

Build one complete workspace payload with:

- valid workspace metadata
- valid layout configuration
- only registered `widgetId` values
- concrete widget instance ids
- fully resolved widget props from local models
- explicit bindings when widgets depend on each other
- no guessed fields

Use nested `row.children` only when the local client model or examples confirm that the row or container pattern is valid.

### 5. Create, verify, and correct through the CLI

Use the current Command Center CLI surface instead of calling workspace endpoints directly:

1. create the workspace with `mainsequence cc workspace create ...`
2. verify the result with `mainsequence cc workspace detail <id>` and `mainsequence cc workspace list`
3. confirm that:
   the new workspace exists
   the title and description match the intended document
   the workspace was created in the expected category or layout mode
   the stored `widgets`, `controls`, `grid`, and `autoGrid` match the intended document

Prefer the file-based workspace workflow:

1. write the intended workspace document to a local JSON or YAML file
2. create it with `mainsequence cc workspace create --file workspace.json`
3. if correction is needed, patch it with `mainsequence cc workspace update <id> --file workspace.json`

If the created workspace is wrong and cannot be corrected with `workspace update`, delete it with `mainsequence cc workspace delete <id>` and recreate it.

### 6. Prefer widget-scoped mutation for one-widget changes

If the user asks to change one specific widget, do not default to rewriting the full workspace JSON.

Prefer these SDK methods on `Workspace`:

- `patch_workspace_widget(...)`
- `delete_workspace_widget(...)`
- `move_workspace_widget(...)`

Use them when the task is:

- changing one widget title, props, layout, or runtime state
- deleting one widget instance
- moving one widget inside the workspace

Only fall back to full workspace update when the change is truly workspace-wide or spans multiple widgets as one coordinated document mutation.

Before mutating a specific widget:

1. identify the exact workspace id
2. identify the exact widget instance id
3. if the widget id is ambiguous, confirm it with the user before making the change

## Minimum Widget Checklist

Do not create a widget instance until all items below are known:

- valid `widgetId`
- instance `id`
- widget props source
- layout source
- any required bindings
- any required external ids
- any required permissions

## Failure Conditions

Stop and report a blocker when any of these is true:

- the widget type does not exist in the registry
- the widget type exists but no local model or example defines the instance payload
- a widget needs external resource ids that are not available
- the user has not provided enough intent to select the right widget
- layout semantics are unclear
- permissions required by the widget or workspace are unknown

## Output Requirements

When you complete a workspace creation task, report:

- created workspace id
- workspace title
- widget ids mounted in the workspace
- whether any current-user view-state fields were stored separately
- any assumptions used
- any remaining manual follow-up, if applicable

## Payload Skeletons

Use these only as scaffolding. Replace placeholders from local models.

### Workspace create payload

```json
{
  "title": "Rates Desk",
  "description": "Workspace created from the CLI",
  "labels": ["rates", "cli"],
  "category": "Custom",
  "source": "user",
  "schemaVersion": 1,
  "requiredPermissions": null,
  "grid": {},
  "layoutKind": "custom",
  "autoGrid": {},
  "companions": [],
  "controls": {},
  "widgets": [
    {
      "id": "widget-1",
      "widgetId": "replace-with-registered-widget-id"
    }
  ]
}
```

## Widget Creation Examples

## Widget Build Examples

Always do this first for every widget:

1. Query the widget type through the CLI.
2. Confirm the widget exists and inspect its backend metadata.
3. Only then build the widget instance payload.

Use:

```bash
mainsequence cc registered_widget_type list --show-filters
mainsequence cc registered_widget_type list --filter widget_id=main-sequence-data-node
mainsequence cc registered_widget_type list --filter widget_id=data-node-table-visualizer
mainsequence cc registered_widget_type list --filter widget_id=main-sequence-data-node-visualizer
mainsequence cc registered_widget_type list --filter widget_id=app-component
```
If the requested widget type is not returned by the backend, stop.

When a widget needs a Main Sequence DataNode, resolve the DataNode first through the CLI:
```shell
mainsequence data-node list
mainsequence data-node list --show-filters
mainsequence data-node search "rates"
mainsequence data-node search "close" --mode column
mainsequence data-node search "pricing" --mode description
mainsequence data-node detail 714
```
If a widget needs an AppComponent operation, do not invent the operation schema or bindingSpec. Get it from the backend/CLI flow available in the environment, or stop and ask for the operation contract.


Example 1: Data Node widget using a Main Sequence DataNode
Use this when the widget should point directly to a known DataNode id.
```
{
  "id": "rates-source-node",
  "widgetId": "main-sequence-data-node",
  "title": "Rates Source",
  "props": {
    "chromeMode": "minimal",
    "dataNodeId": 714,
    "dateRangeMode": "dashboard",
    "showHeader": false
  },
  "presentation": {
    "placementMode": "sidebar"
  },
  "layout": {
    "cols": 8,
    "rows": 4
  },
  "position": {
    "x": 0,
    "y": 0
  }
}
```

Example 2: Do not write a fake "Data Node widget from AppComponent" example
That is not a valid default example.

If the user asks for that exact flow, the skill should say this:

```json
{
  "blocker": "No valid direct request",
  "requestedWidgetId": "main-sequence-data-node",
  "requestedUpstreamWidgetId": "app-component",
  "reason": "The Data Node widget expects a real Main Sequence DataNode or a compatible tabular source. An AppComponent response is not a valid direct substitute unless the environment explicitly exposes a compatible tabular contract."
}

```

If the real goal is to connect the two, the supported example is usually the other direction: AppComponent consuming a field derived from another widget.

Example 3: AppComponent widget
Only use this after resolving the widget type and the operation contract through the backend/CLI flow.

```json
{
  "id": "swap-pricer",
  "widgetId": "app-component",
  "title": "Swap Pricer",
  "props": {
    "apiBaseUrl": "https://example-api.internal",
    "authMode": "session-jwt",
    "method": "post",
    "path": "/price/swap",
    "requestBodyContentType": "application/json",
    "showHeader": true,
    "refreshOnDashboardRefresh": true,
    "bindingSpec": {
      "version": 1,
      "operationKey": "post /price/swap",
      "requestPorts": [
        {
          "id": "body:date",
          "fieldKey": "body:date",
          "location": "body",
          "required": true,
          "kind": "date"
        },
        {
          "id": "body:rate",
          "fieldKey": "body:rate",
          "location": "body",
          "required": true,
          "kind": "number"
        }
      ],
      "responsePorts": [
        {
          "id": "response:$",
          "kind": "json",
          "contract": "core.value.json@v1"
        },
        {
          "id": "response:date",
          "kind": "string",
          "contract": "core.value.string@v1"
        },
        {
          "id": "response:rate",
          "kind": "number",
          "contract": "core.value.number@v1"
        },
        {
          "id": "response:price",
          "kind": "number",
          "contract": "core.value.number@v1"
        }
      ]
    }
  },
  "layout": {
    "cols": 8,
    "rows": 8
  },
  "position": {
    "x": 0,
    "y": 4
  }
}
```

Example 4: AppComponent connected to a DataNode-derived value
This is the valid connection example to show when the user wants to connect a DataNode-related widget to an AppComponent.

```json
{
  "id": "swap-pricer",
  "widgetId": "app-component",
  "title": "Swap Pricer",
  "props": {
    "apiBaseUrl": "https://example-api.internal",
    "authMode": "session-jwt",
    "method": "get",
    "path": "/price/by-node",
    "showHeader": true,
    "refreshOnDashboardRefresh": true,
    "bindingSpec": {
      "version": 1,
      "operationKey": "get /price/by-node",
      "requestPorts": [
        {
          "id": "query:data_node_id",
          "fieldKey": "query:data_node_id",
          "location": "query",
          "required": true,
          "kind": "integer"
        }
      ],
      "responsePorts": [
        {
          "id": "response:$",
          "kind": "json",
          "contract": "core.value.json@v1"
        }
      ]
    }
  },
  "bindings": {
    "query:data_node_id": {
      "sourceWidgetId": "rates-source-node",
      "sourceOutputId": "dataset",
      "transformId": "extract-path",
      "transformPath": ["source", "id"]
    }
  },
  "layout": {
    "cols": 8,
    "rows": 8
  },
  "position": {
    "x": 8,
    "y": 0
  }
}
```

Example 5: Table widget
Use this when the table should render the dataset produced by another widget.

```json
{
  "id": "rates-table",
  "widgetId": "data-node-table-visualizer",
  "title": "Rates Table",
  "props": {
    "sourceMode": "filter_widget",
    "sourceWidgetId": "rates-source-node",
    "dateRangeMode": "dashboard",
    "density": "comfortable",
    "showToolbar": true,
    "showSearch": true,
    "zebraRows": true,
    "pagination": true,
    "pageSize": 10,
    "schema": [],
    "columnOverrides": {},
    "valueLabels": [],
    "conditionalRules": []
  },
  "bindings": {
    "sourceData": {
      "sourceWidgetId": "rates-source-node",
      "sourceOutputId": "dataset"
    }
  },
  "layout": {
    "cols": 8,
    "rows": 6
  },
  "position": {
    "x": 0,
    "y": 4
  }
}
```

Example 6: Graph widget
Use this when the graph should render the dataset produced by another widget.

```json
{
  "id": "rates-graph",
  "widgetId": "main-sequence-data-node-visualizer",
  "title": "Rates Graph",
  "props": {
    "sourceMode": "filter_widget",
    "sourceWidgetId": "rates-source-node",
    "provider": "tradingview",
    "chartType": "line",
    "dateRangeMode": "dashboard",
    "xField": "time_index",
    "yField": "close",
    "groupField": "ticker",
    "groupSelectionMode": "include",
    "selectedGroupValues": ["NVDA", "TTWO"],
    "seriesAxisMode": "shared"
  },
  "bindings": {
    "sourceData": {
      "sourceWidgetId": "rates-source-node",
      "sourceOutputId": "dataset"
    }
  },
  "layout": {
    "cols": 8,
    "rows": 6
  },
  "position": {
    "x": 8,
    "y": 4
  }
}
```

Minimal pattern the skill should follow
Query the widget type with mainsequence cc registered_widget_type list --filter widget_id=...
If the widget needs a DataNode, find it with mainsequence data-node search ... and confirm it with mainsequence data-node detail ...
If the widget needs an AppComponent operation contract, resolve it from the backend/CLI flow and do not invent bindingSpec
Build the widget payload
Mount it into the workspace payload
