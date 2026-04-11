---
name: command-center-app-components
description: Use this skill when the task is about AppComponent widgets in a Main Sequence project. This skill owns AppComponent input contracts, custom forms, form sections and field definitions, and the boundary between AppComponent input contracts and widget-facing output contracts. It does not own workspace layout, generic FastAPI design, or Streamlit dashboards.
---

# Command Center AppComponents

## Overview

Use this skill when the task is about the backend contract behind a Command Center AppComponent widget.

This skill is for:

- AppComponent input contracts
- custom forms
- field and section definitions
- deciding when default argument resolution is enough
- deciding when a widget-facing API response must use an exact SDK contract

## This Skill Can Do

- decide whether an AppComponent should rely on the default generated form
- create a custom `EditableFormDefinition`
- define `FormSectionDefinition`
- define `FormFieldDefinition`
- choose the correct `FormFieldKind`
- define stable `token` values for fields
- review whether an AppComponent form is too thin or too custom
- separate input contracts from output contracts
- decide when the API behind an AppComponent must return exact widget-facing response models

## This Skill Must Not Claim

This skill must not claim ownership of:

- workspace document creation or widget layout
- generic FastAPI route architecture
- DataNode producer design
- SimpleTable schema design
- Streamlit dashboard implementation
- scheduling, image pinning, resources, or releases

## Route Adjacent Work

- Command Center workspaces:
  `agent_scaffold/skills/command_center/workspace_builder/SKILL.md`
- APIs and FastAPI:
  `agent_scaffold/skills/application_surfaces/api_surfaces/SKILL.md`
- DataNodes:
  `agent_scaffold/skills/data_publishing/data_nodes/SKILL.md`
- SimpleTables:
  `agent_scaffold/skills/data_publishing/simple_tables/SKILL.md`
- Streamlit dashboards:
  `agent_scaffold/skills/dashboards/streamlit/SKILL.md`

## Read First

1. `docs/knowledge/command_center/forms.md`
2. `docs/knowledge/command_center/widget_data_contracts.md`
3. `mainsequence/client/command_center/app_component.py`

If the AppComponent is backed by project APIs, also read:

4. `agent_scaffold/skills/application_surfaces/api_surfaces/SKILL.md`

## Inputs This Skill Needs

Before changing an AppComponent backend contract, collect or infer:

- what the widget is trying to collect from the user
- whether the default generated form is already sufficient
- the fields, sections, and labels the form should expose
- whether the output is:
  - a generic API contract
  - an exact widget-facing contract
- whether stable field tokens are needed for downstream bindings or draft state

If the input contract or output contract is unclear, stop before building the form.

## Required Decisions

For every non-trivial AppComponent task, decide:

1. Is the default generated form sufficient?
2. If not, what requires a custom `EditableFormDefinition`?
3. What are the stable field tokens?
4. What field kinds should the frontend render?
5. Is the output generic, or does it need an exact widget-facing response contract?

## Build Rules

### 1. Default generated form first

Do not jump to a custom form by default.

If the operation only exposes simple flat arguments with a straightforward shape, let Command Center resolve the form automatically.

Typical cases where default generation is enough:

- a few scalar query parameters
- a small request body with standard primitive fields
- enums, booleans, dates, and numbers with no specialized layout requirement

### 2. Use `EditableFormDefinition` only when the form needs to be specialized

Use a custom form when the widget needs:

- grouped sections
- domain-specific labels
- field-level formatter hints
- more control over editable vs read-only behavior
- stable custom tokens for bindings or draft state

Do not use a custom form just to restate a trivial flat contract.

### 3. Treat tokens as stable identities

`token` is not cosmetic.

It is the stable field identity for:

- draft state
- bindings
- app-specific coordination

Do not rename tokens casually once a compatible interaction flow exists.

### 4. Use field kinds that reflect business meaning

Choose `FormFieldKind` based on intended UI semantics, not just transport type.

Example:

- `PERCENT` is better than `NUMBER` for a funding rate when the UI should behave like a percent input

### 5. Do not mix input contracts and output contracts

Keep the boundary clear:

- `EditableFormDefinition` and related form objects describe what the widget should collect
- widget data models in `mainsequence.client.command_center.data_models` describe what some widgets expect back from the API

If the AppComponent both collects input and returns widget-facing data, validate both sides explicitly.

### 6. Widget-facing outputs must use exact SDK models

If the AppComponent backend is supposed to feed a Main Sequence widget directly, use the SDK response model for that widget contract.

Do not return loose dictionaries for a widget boundary when an exact contract model exists.

## Review Rules

When reviewing an AppComponent task, look for:

- a custom form that should have been auto-generated
- a flat autogenerated form that should have been specialized
- unstable or poorly named field tokens
- wrong `FormFieldKind` choices
- generic API output being used where an exact widget-facing contract should have been returned
- confusion between workspace concerns and AppComponent contract concerns

## Validation Checklist

Do not claim success until you have checked:

- the choice between autogenerated form and custom form is intentional
- custom forms use `EditableFormDefinition`
- sections and fields are explicit where needed
- field tokens are stable and meaningful
- field kinds reflect business meaning
- input and output contracts are not mixed together
- widget-facing outputs use exact SDK response models when applicable

## This Skill Must Stop And Escalate When

- the task is really about workspace structure instead of AppComponent contracts
- the form contract is unclear but a custom form is being forced anyway
- the widget-facing output contract is unclear and no exact SDK model is available
- the task is really a generic API design problem rather than an AppComponent problem

Do not guess through AppComponent boundaries.
