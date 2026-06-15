---
name: command-center-tables
description: Use this skill when the task is about Command Center table or pro-table widgets, canonical `core.tabular_frame@v1` responses, table-specific frame visual metadata, table widget column schema, formula columns, conditional formatting, selection outputs, or live merge key mappings. This skill owns the boundary between source-owned tabular frame contracts and persisted table widget props. It does not own workspace layout, connection creation, generic API architecture, AppComponent forms, MetaTable schema design, or DataNode storage design.
---

# Command Center Tables

## Overview

Use this skill when a Command Center table surface needs exact contract work.

There are two table-related surfaces that must stay separate:

- source-owned data contract:
  `core.tabular_frame@v1` plus optional `meta.tableVisuals`
- widget-owned presentation contract:
  `core__table` or `core__pro-table` mounted widget props

Do not mix these contracts. A data producer should return a canonical tabular frame. A workspace
table widget should define persisted presentation choices, bindings, formulas, selection behavior,
and live update mappings.

## This Skill Can Do

- design or review a `core.tabular_frame@v1` response intended for table consumption
- use `make_tabular_frame`, `make_tabular_field`, and related helpers
- add or review source-owned `meta.tableVisuals` metadata
- decide whether display semantics belong in `meta.tableVisuals` or table widget props
- build or review `core__table` mounted widget payloads
- build or review `core__pro-table` mounted widget payloads
- define `TableWidgetColumnSchema`
- define `TableWidgetColumnOverride`
- define conditional rules and value labels
- use formula helpers and `make_formula_column` for pro-table only
- define selection mode and selection key fields
- define live merge key mappings for incremental table updates
- verify table/pro-table widget registry details before mounting or mutating a table widget

## This Skill Must Not Claim

This skill must not claim ownership of:

- workspace narrative, layout, or widget selection strategy
- workspace JSON mutation or widget-scoped patch execution
- connection type selection or connection instance creation
- Adapter from API provider discovery contracts outside table-specific frame output decisions
- AppComponent request forms, editable forms, or notification responses
- generic FastAPI route architecture
- MetaTable schema, DataNode storage, migrations, or governed operations
- resource releases, jobs, images, RBAC, or sharing

## Route Adjacent Work

- Workspace design and deciding whether the view should be a table:
  `.agents/skills/mainsequence/command_center/workspace_design/SKILL.md`
- Workspace JSON creation, table mounting, and widget mutation:
  `.agents/skills/mainsequence/command_center/workspace_builder/SKILL.md`
- Connection-backed source data and query model selection:
  `.agents/skills/mainsequence/command_center/connections/SKILL.md`
- Adapter from API provider contracts:
  `.agents/skills/mainsequence/command_center/adapter_from_api/SKILL.md`
- Tabular transform widgets that reshape data before table consumption:
  `.agents/skills/mainsequence/command_center/widgets/tabular_transform/SKILL.md`
- AppComponent custom forms and action contracts:
  `.agents/skills/mainsequence/command_center/widgets/app_components/SKILL.md`
- APIs and FastAPI:
  `.agents/skills/mainsequence/application_surfaces/api_surfaces/SKILL.md`
- MetaTables:
  `.agents/skills/mainsequence/data_publishing/meta_tables/SKILL.md`
- DataNodes:
  `.agents/skills/mainsequence/data_publishing/data_nodes/SKILL.md`

## Read First

1. Verify the table widget catalog through the CLI:
   - `mainsequence cc registered_widget_type list --json`
   - identify `core__table` or `core__pro-table`
   - `mainsequence cc registered_widget_type detail <WIDGET_ID> --json`
2. `docs/knowledge/command_center/widget_data_contracts.md`
3. SDK table and tabular models:
   - `mainsequence/client/command_center/contracts/tabular.py`
   - `mainsequence/client/command_center/contracts/table_visuals.py`
   - `mainsequence/client/command_center/widgets/table.py`
   - `mainsequence/client/command_center/data_models.py`
4. If the table consumes connection-backed data:
   - `.agents/skills/mainsequence/command_center/connections/SKILL.md`
   - `mainsequence/client/command_center/widgets/connection_query.py`
   - `mainsequence/client/command_center/widgets/bindings.py`
5. If the table consumes transformed data:
   - `.agents/skills/mainsequence/command_center/widgets/tabular_transform/SKILL.md`
   - `mainsequence/client/command_center/widgets/tabular_transform.py`
6. If mounting or mutating a table widget:
   - `.agents/skills/mainsequence/command_center/workspace_builder/SKILL.md`
   - `mainsequence/client/command_center/workspaces/mounted_widgets.py`

Use the SDK helper files above before opening frontend implementation files.

## Inputs This Skill Needs

Before changing a table contract or payload, collect or infer:

- target widget id: `core__table` or `core__pro-table`
- registry detail for that widget id
- source mode: bound, connection, connection-stream, or manual
- source frame shape: columns, rows, fields, source, and meta
- whether the source emits a full `core.tabular_frame@v1` response
- which fields are stable identity keys
- which columns should be visible, hidden, pinned, aligned, formatted, or resized
- which formatting is source-owned default metadata versus workspace-specific override
- whether formulas are needed
- whether row/cell selection is needed and which outputs downstream widgets consume
- whether live updates need merge mappings from seed fields to live-update fields

## Required Decisions

For every non-trivial table task, decide:

1. Should the data be represented as `core.tabular_frame@v1` before it reaches the table?
2. Does the table need `core__table` or `core__pro-table`?
3. Are formula columns required? If yes, use `core__pro-table`.
4. Which display semantics should be emitted by the source in `meta.tableVisuals`?
5. Which display choices should remain widget-owned props or user/workspace overrides?
6. Should column descriptions live in frame `fields`, table widget `schema`, or both?
7. Does selection need stable `selectionKeyFields` and published selection outputs?
8. Does incremental/live updating need `liveMergeKeyMappings`?
9. Is a Tabular Transform widget needed before the table?
10. Has the exact widget registry detail been checked before authoring payloads?

## Build Rules

### 1. Use Canonical Tabular Frames At The Consumption Boundary

Table and pro-table widgets should consume `core.tabular_frame@v1` for generic tabular data.

Use:

```python
from mainsequence.client.command_center.contracts.tabular import (
    make_tabular_field,
    make_tabular_frame,
)
```

Do not bind raw arrays, paginated provider JSON, nested objects, or provider-native payloads
directly into generic table consumers unless a dedicated widget contract explicitly supports that
shape.

### 2. Keep Source-Owned Table Defaults In `meta.tableVisuals`

Use `meta.tableVisuals` for source-owned display metadata that should travel with the frame:

```python
from mainsequence.client.command_center.contracts.table_visuals import (
    make_table_visual_column,
    make_table_visuals,
)
from mainsequence.client.command_center.contracts.tabular import make_tabular_frame

frame = make_tabular_frame(
    rows=[{"asset": "BTC", "price": 101.25, "change": 0.015}],
    columns=["asset", "price", "change"],
    table_visuals=make_table_visuals(
        {
            "price": make_table_visual_column(format="currency", decimals=2),
            "change": make_table_visual_column(format="percent", decimals=2),
        }
    ),
)
```

Use `meta.tableVisuals` for stable source semantics such as:

- source-recommended labels
- numeric, currency, percent, bps, or formula result format hints
- default decimals
- default heatmap, bar, gradient, gauge, or threshold hints
- inline series encoding hints for sparkline-like columns

Do not put user-specific layout, workspace-local overrides, credentials, connection ids, or runtime
view state into `meta.tableVisuals`.

### 3. Keep Widget-Owned Presentation In Table Props

Use table widget props for persisted workspace presentation:

```python
from mainsequence.client.command_center.widgets.table import (
    TableWidgetColumnOverride,
    TableWidgetColumnSchema,
    make_table_widget_payload,
)

payload = make_table_widget_payload(
    title="Prices",
    schema=[
        TableWidgetColumnSchema(key="asset", label="Asset", format="text"),
        TableWidgetColumnSchema(key="price", label="Price", format="currency"),
    ],
    column_overrides={
        "price": TableWidgetColumnOverride(format="currency", decimals=2, align="right")
    },
)
```

Use widget props for:

- table source mode
- column schema and persistent column overrides
- toolbar/search/filter/pagination options
- value labels and conditional rules
- selection mode and selection output behavior
- live merge key mappings
- workspace-specific formatting choices

### 4. Formula Columns Require Pro Table

Normal table payloads reject formula columns. If the table needs formula columns, use
`core__pro-table` and `make_pro_table_widget_payload`.

```python
from mainsequence.client.command_center.widgets.table import (
    formula_percent_change,
    make_formula_column,
    make_pro_table_widget_payload,
)

payload = make_pro_table_widget_payload(
    title="Returns",
    schema=[
        make_formula_column(
            key="return_pct",
            label="Return %",
            expression=formula_percent_change("price", "previous_price"),
            result_format="percent",
        )
    ],
)
```

Do not force formula props into `core__table`.

### 5. Do Not Confuse Frame Fields With Widget Schema

Frame `fields` describe the data contract:

- key
- label
- type
- nullable
- provenance

Table widget `schema` describes persisted table presentation and table runtime behavior:

- format
- formula expression
- min width, flex, pinning
- compactness
- heatmap eligibility

It is valid to use both, but they serve different owners. Do not replace a canonical frame field
description with widget props, and do not put table runtime-only props into frame fields.

### 6. Connection-Backed Tables Must Stay Connection-First

If a table consumes backend-owned data:

1. resolve the connection instance and query model
2. use a Connection Query source widget
3. bind the table input to the source widget output
4. use a Tabular Transform widget before the table when reshaping is needed

Do not store endpoint URLs, credentials, raw provider ids, or route fragments in table widget props.

### 7. Live Merge Keys Must Be Stable

Use `TableLiveMergeKeyMapping` only when seed-frame rows and live-update rows have stable fields
that identify the same logical row.

Do not configure live merge mappings from display labels, mutable names, timestamps alone, or
non-unique values.

### 8. Selection Outputs Need Stable Keys

When the table should drive downstream widgets, define:

- `selectionMode`
- `selectionKeyFields`
- `publishSelectionOutputs`

Selection key fields must identify rows stably enough for downstream consumers.

## Review Rules

When reviewing table work, look for:

- raw provider JSON bound into a generic table
- table widget props carrying credentials, endpoint URLs, or backend route fragments
- display defaults that should be source-owned but were hardcoded as workspace overrides
- workspace-local overrides pushed into `meta.tableVisuals`
- formula columns mounted on `core__table` instead of `core__pro-table`
- duplicate table schema column keys
- conditional rules referencing missing schema columns
- selection outputs without stable key fields
- live merge mappings based on unstable or non-unique fields
- frame `fields` being used as a substitute for widget runtime schema, or the reverse
- table payloads authored before registry detail was checked

## Validation Checklist

Do not claim the table contract is ready until:

- the target widget id was verified through registry detail
- data consumed by generic table widgets is `core.tabular_frame@v1`
- frame columns and row keys are aligned
- important field types are represented in `fields`
- source-specific runtime details live in `source.context`
- source-owned display defaults, if any, live under `meta.tableVisuals`
- table widget props serialize through `make_table_widget_payload` or `make_pro_table_widget_payload`
- formula columns use `core__pro-table`
- selection and live merge keys are stable when configured
- the workspace handoff identifies bindings from source outputs to table inputs

## Stop Conditions

Stop and route to the adjacent skill when:

- the task is deciding whether a table is the right widget
- the table needs a connection instance or query model that does not exist
- the API cannot emit `core.tabular_frame@v1` but the table needs generic tabular data
- table widget registry detail is unavailable or contradicts the SDK model
- formulas are required but `core__pro-table` is unavailable
- stable selection or live merge keys cannot be identified
- the task has moved into workspace JSON mutation or backend API implementation
