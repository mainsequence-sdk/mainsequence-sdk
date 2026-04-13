---
name: command-center-app-components
description: Use this skill when the task is about AppComponent widgets in a Main Sequence project. This skill owns AppComponent input contracts, custom forms, form sections and field definitions, and the boundary between AppComponent input contracts and widget-facing output contracts. Before changing AppComponent payloads or contracts, verify the target widget in the Command Center registry through the CLI. Stay within Main Sequence-accessible sources of truth: CLI registry, SDK models, and documentation in this repository. It does not own workspace layout, generic FastAPI design, or Streamlit dashboards.
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
- verify the target widget type in the CLI registry before changing payload or contract logic

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
  `.agents/skills/command_center/workspace_builder/SKILL.md`
- APIs and FastAPI:
  `.agents/skills/application_surfaces/api_surfaces/SKILL.md`
- DataNodes:
  `.agents/skills/data_publishing/data_nodes/SKILL.md`
- SimpleTables:
  `.agents/skills/data_publishing/simple_tables/SKILL.md`
- Streamlit dashboards:
  `.agents/skills/dashboards/streamlit/SKILL.md`

## Read First

1. Verify the widget catalog through the CLI:
   - `mainsequence --json cc registered_widget_type list`
   - identify the target `widget_id`
   - `mainsequence --json cc registered_widget_type detail <WIDGET_ID>`
2. `docs/knowledge/command_center/forms.md`
3. `docs/knowledge/command_center/widget_data_contracts.md`
4. `mainsequence/client/command_center/app_component.py`

If the AppComponent is backed by project APIs, also read:

5. `.agents/skills/application_surfaces/api_surfaces/SKILL.md`

## Inputs This Skill Needs

Before changing an AppComponent backend contract, collect or infer:

- verified `widget_id`
- registry detail payload from:
  - `mainsequence --json cc registered_widget_type detail <WIDGET_ID>`
- what the widget is trying to collect from the user
- whether the default generated form is already sufficient
- the fields, sections, and labels the form should expose
- whether the output is:
  - a generic API contract
  - an exact widget-facing contract
- whether stable field tokens are needed for downstream bindings or draft state

If registry detail is not sufficient, only use Main Sequence-accessible sources in this repository:

- `docs/knowledge/command_center/forms.md`
- `docs/knowledge/command_center/widget_data_contracts.md`
- `mainsequence/client/command_center/app_component.py`
- `mainsequence/client/command_center/data_models.py`

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

### 1.1 Registry verification is mandatory before AppComponent payload work

Before changing an AppComponent payload, form contract, or widget-facing output:

1. run `mainsequence --json cc registered_widget_type list`
2. identify the target `widget_id`
3. run `mainsequence --json cc registered_widget_type detail <WIDGET_ID>`

Do not infer `widget_id`, mounted widget identity, or widget-facing behavior without checking the registered widget catalog first.

If registry inspection is unavailable or does not provide enough contract detail, stay within Main Sequence-accessible sources in this repository. If the contract is still unclear after checking the registry plus local SDK docs/models, stop and escalate.

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

- inferred or guessed `widget_id` values
- AppComponent work that skipped `registered_widget_type list/detail`
- a custom form that should have been auto-generated
- a flat autogenerated form that should have been specialized
- unstable or poorly named field tokens
- wrong `FormFieldKind` choices
- generic API output being used where an exact widget-facing contract should have been returned
- confusion between workspace concerns and AppComponent contract concerns

## Validation Checklist

Do not claim success until you have checked:

- the target `widget_id` was verified through:
  - `mainsequence --json cc registered_widget_type list`
  - `mainsequence --json cc registered_widget_type detail <WIDGET_ID>`
- registry detail was used as the first source of truth
- the choice between autogenerated form and custom form is intentional
- custom forms use `EditableFormDefinition`
- sections and fields are explicit where needed
- field tokens are stable and meaningful
- field kinds reflect business meaning
- input and output contracts are not mixed together
- widget-facing outputs use exact SDK response models when applicable
- only Main Sequence-accessible repository sources were used after registry detail when refinement was needed

## This Skill Must Stop And Escalate When

- the target widget cannot be identified in the registered widget catalog
- the task depends on guessed widget behavior without registry verification
- registry detail is insufficient and the required contract cannot be resolved from Main Sequence docs/models in this repository
- the task is really about workspace structure instead of AppComponent contracts
- the form contract is unclear but a custom form is being forced anyway
- the widget-facing output contract is unclear and no exact SDK model is available
- the task is really a generic API design problem rather than an AppComponent problem

Do not guess through AppComponent boundaries.
