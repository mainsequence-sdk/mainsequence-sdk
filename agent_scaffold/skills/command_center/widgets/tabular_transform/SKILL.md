---
name: command-center-tabular-transform
description: Use this skill when the task is about the Command Center `core__tabular-transform` widget, including tabular transform widget props, projection, filter, aggregate, pivot, unpivot, computed columns, latest-row merge behavior, seed/live update bindings, or wiring transformed `core.tabular_frame@v1` datasets into downstream table, chart, statistic, curve, or agent-facing widgets. This skill is grounded in `mainsequence.client.command_center.widgets.tabular_transform` and does not own connection creation, API provider contracts, table widget presentation props, workspace layout design, or generic FastAPI implementation.
---

# Command Center Tabular Transform

## Overview

Use this skill when a workspace needs the `core__tabular-transform` widget or its SDK payload
helpers.

The SDK model for this widget lives in:

```python
mainsequence.client.command_center.widgets.tabular_transform
```

The binding helpers for transform inputs live in:

```python
mainsequence.client.command_center.widgets.bindings
```

## This Skill Can Do

- build or review `core__tabular-transform` mounted widget payloads
- build or review `TabularTransformProps`
- choose one of the SDK-supported transform modes:
  - `none`
  - `filter`
  - `aggregate`
  - `pivot`
  - `unpivot`
- configure projection through `projectFields`
- configure computed columns through `computedColumns`
- configure latest-row merge through `rowMergeMode = "latest"`
- configure row merge keys through `rowMergeKeyFields` or `rowMergeKeyMappings`
- bind seed data to the `seedData` input
- bind live updates to the `liveUpdates` input
- wire the transform `dataset` output into downstream consumers
- use the SDK tests as executable examples for valid payload shape

## This Skill Must Not Claim

This skill must not claim ownership of:

- deciding whether the workspace should use a transform widget
- creating or selecting connection instances
- Adapter from API provider discovery contracts
- table/pro-table presentation props
- AppComponent forms or action contracts
- generic FastAPI route implementation
- MetaTable schema, DataNode storage, migrations, or governed operations
- project resources, releases, images, jobs, RBAC, or sharing

## Route Adjacent Work

- Workspace design and deciding whether a transform is needed:
  `.agents/skills/mainsequence/command_center/workspace_design/SKILL.md`
- Workspace JSON creation, transform mounting, and widget mutation:
  `.agents/skills/mainsequence/command_center/workspace_builder/SKILL.md`
- Connection-backed source data and query model selection:
  `.agents/skills/mainsequence/command_center/connections/SKILL.md`
- Table/pro-table props and `meta.tableVisuals`:
  `.agents/skills/mainsequence/command_center/widgets/tables/SKILL.md`
- Adapter from API provider contracts:
  `.agents/skills/mainsequence/command_center/adapter_from_api/SKILL.md`

## Read First

1. Verify the widget catalog through the CLI:
   - `mainsequence cc registered_widget_type list --json`
   - identify `core__tabular-transform`
   - `mainsequence cc registered_widget_type detail core__tabular-transform --json`
2. SDK widget models and helpers:
   - `mainsequence/client/command_center/widgets/tabular_transform.py`
   - `mainsequence/client/command_center/widgets/bindings.py`
   - `mainsequence/client/command_center/workspaces/mounted_widgets.py`
3. Executable examples:
   - `tests/test_command_center_tabular_transform.py`
4. When the transform consumes connection-backed data:
   - `.agents/skills/mainsequence/command_center/connections/SKILL.md`
   - `mainsequence/client/command_center/widgets/connection_query.py`

Use these files before opening frontend implementation files.

## Inputs This Skill Needs

Before changing a tabular transform payload, collect or infer:

- source widget uid
- whether the source output is retained/base data, live updates, or both
- source output id, usually `dataset` or `updates`
- target transform mode
- fields needed downstream
- computed column keys and formula expressions
- filter rules and whether they combine with `all` or `any`
- aggregate key fields and aggregate mode
- pivot field and pivot value field
- unpivot value fields and output field names
- latest-row merge keys or seed/live key mappings
- downstream widget inputs that consume the transform `dataset`

## Required Decisions

For every tabular transform task, decide:

1. Is the input a canonical `core.tabular_frame@v1` dataset?
2. Is this just projection/computed columns, or does it need `filter`, `aggregate`, `pivot`, or
   `unpivot`?
3. Should the transform also collapse rows with `rowMergeMode = "latest"`?
4. If live updates exist, are they bound to `liveUpdates` instead of `seedData`?
5. Which output should downstream widgets consume: `dataset` or `updates`?
6. Are computed column keys unique?
7. Are the required mode-specific fields present?
8. Has `core__tabular-transform` been verified in the widget registry?

## Build Rules

### 1. Use The SDK Builders

Use the helper that matches the intended mode:

```python
from mainsequence.client.command_center.widgets.tabular_transform import (
    make_aggregate_transform,
    make_filter_transform,
    make_latest_row_transform,
    make_passthrough_transform,
    make_pivot_transform,
    make_projection_transform,
    make_tabular_transform_widget_payload,
    make_unpivot_transform,
)
```

Do not hand-build transform props unless the helper cannot represent the required payload.

### 2. Use Binding Helpers For Transform Inputs

Use `bind_tabular_transform_seed` for retained/base rows:

```python
from mainsequence.client.command_center.widgets.bindings import bind_tabular_transform_seed
```

Use `bind_tabular_transform_live_updates` for incremental updates:

```python
from mainsequence.client.command_center.widgets.bindings import (
    bind_tabular_transform_live_updates,
)
```

The SDK constants are:

- input ids:
  - `seedData`
  - `liveUpdates`
- output ids:
  - `dataset`
  - `updates`

### 3. Respect Mode Requirements

The SDK validates these requirements:

- `aggregate` requires at least one `keyFields` entry
- `filter` requires at least one `filterRules` entry
- `pivot` requires `pivotField` and `pivotValueField`
- `unpivot` requires at least one `unpivotValueFields` entry
- `rowMergeMode = "latest"` requires `rowMergeKeyFields` or `rowMergeKeyMappings`
- computed column keys must be unique

Do not claim a transform payload is valid until it passes the SDK model.

### 4. Keep Analytical Reshaping In The Transform Widget

Use `core__tabular-transform` for projection, computed columns, filtering, aggregate, pivot,
unpivot, and latest-row merge behavior.

Do not hide those analytical transformations inside binding `transformSteps` or downstream
consumer widget props.

### 5. Keep Table Presentation In The Table Skill

This widget prepares tabular data. It does not own table/pro-table presentation props such as
column overrides, formulas on table widgets, selection behavior, or `meta.tableVisuals`.

Route those to:

```text
.agents/skills/mainsequence/command_center/widgets/tables/SKILL.md
```

## Review Rules

When reviewing tabular transform work, look for:

- raw provider JSON used instead of `core.tabular_frame@v1`
- analytical reshaping hidden in binding transforms
- missing widget registry verification for `core__tabular-transform`
- transform mode missing required fields
- duplicate computed column keys
- live updates bound to `seedData` instead of `liveUpdates`
- latest-row merge configured without stable key fields or key mappings
- downstream consumers bound to the wrong transform output
- table presentation props placed on the transform widget

## Validation Checklist

Do not claim the transform is ready until:

- `core__tabular-transform` was verified in the registry
- props serialize through `make_tabular_transform_widget_payload` or `dump_tabular_transform_props`
- mode-specific validation passes
- computed column keys are unique
- seed data and live update bindings target the correct inputs
- downstream consumers bind to the intended output
- focused tests such as `tests/test_command_center_tabular_transform.py` pass when code changed

## Stop Conditions

Stop and route to the adjacent skill when:

- the task is deciding if a transform widget is needed at all
- the source connection/query model is unresolved
- the upstream data is not `core.tabular_frame@v1`
- the task is table/pro-table presentation rather than data reshaping
- the widget registry contradicts the SDK helper model
- stable merge keys cannot be identified for latest-row behavior
