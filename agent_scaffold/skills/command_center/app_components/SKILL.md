---
name: command-center-app-components
description: Use this skill when the task is about AppComponent widgets in a Main Sequence project. This skill owns AppComponent input contracts, custom forms, form sections and field definitions, and the boundary between AppComponent input contracts and widget-facing output contracts. Before changing AppComponent payloads or contracts, verify the target widget in the Command Center registry through the CLI. Source order is strict: registry detail first, SDK client models second, local Main Sequence repository docs/models third only if the first two still leave something unresolved. Main Sequence is platform-first: if an AppComponent depends on a project API, that API must already exist as a FastAPI project resource and have a corresponding FastAPI ResourceRelease before the AppComponent is considered usable from Command Center. Resource and release creation belong to the orchestration-and-releases skill. It does not own workspace layout, generic FastAPI design, or Streamlit dashboards.
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
- requiring API-backed AppComponents to depend on deployed FastAPI resources/releases, not local-only API code

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
- predeployment mock API contract validation:
  `.agents/skills/command_center/api_mock_prototyping/SKILL.md`
- APIs and FastAPI:
  `.agents/skills/application_surfaces/api_surfaces/SKILL.md`
- DataNodes:
  `.agents/skills/data_publishing/data_nodes/SKILL.md`
- SimpleTables:
  `.agents/skills/data_publishing/simple_tables/SKILL.md`
- Jobs, images, resources, and releases:
  `.agents/skills/platform_operations/orchestration_and_releases/SKILL.md`
- Streamlit dashboards:
  `.agents/skills/dashboards/streamlit/SKILL.md`

## Read First

1. Verify the widget catalog through the CLI:
   - `mainsequence cc registered_widget_type list --json`
   - identify the target `widget_id`
   - `mainsequence cc registered_widget_type detail <WIDGET_ID> --json`
2. `docs/knowledge/command_center/forms.md`
3. `docs/knowledge/command_center/widget_data_contracts.md`
4. `mainsequence/client/command_center/app_component.py`

If the AppComponent is backed by project APIs, also read:

5. `.agents/skills/application_surfaces/api_surfaces/SKILL.md`
6. `.agents/skills/platform_operations/orchestration_and_releases/SKILL.md`

If the task is about validating the API contract before deployment, also read:

7. `.agents/skills/command_center/api_mock_prototyping/SKILL.md`

## Inputs This Skill Needs

Before changing an AppComponent backend contract, collect or infer:

- verified `widget_id`
- registry detail payload from:
  - `mainsequence cc registered_widget_type detail <WIDGET_ID> --json`
- what the widget is trying to collect from the user
- whether the default generated form is already sufficient
- the fields, sections, and labels the form should expose
- whether the output is:
  - a generic API contract
  - an exact widget-facing contract
- whether stable field tokens are needed for downstream bindings or draft state
- whether the backing API already exists as a FastAPI project resource with a FastAPI `ResourceRelease`

Use this source order strictly:

1. Registry detail first
   - `mainsequence cc registered_widget_type detail <WIDGET_ID> --json`
   - this is the first contract source
2. SDK client models second
   - `mainsequence/client/command_center/app_component.py`
   - `mainsequence/client/command_center/data_models.py`
   - use these before any broader repository exploration
3. Repository docs/models third
   - local docs, examples, payload builders, and typed models
   - only if registry detail plus SDK client models still leave something unresolved

If registry detail is not sufficient, and only after checking the SDK client models, use local Main Sequence repository sources such as:

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
6. If the AppComponent depends on a project API, does that API already exist as a FastAPI project resource with a FastAPI `ResourceRelease`?

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

1. run `mainsequence cc registered_widget_type list --json`
2. identify the target `widget_id`
3. run `mainsequence cc registered_widget_type detail <WIDGET_ID> --json`

Do not infer `widget_id`, mounted widget identity, or widget-facing behavior without checking the registered widget catalog first.

If registry inspection is unavailable or does not provide enough contract detail, do not jump directly into general repository exploration. Check the SDK client models next. Only then use local docs/models/examples if the contract is still unresolved. If the contract is still unclear after registry detail plus SDK client models, stop and escalate.

### 1.2 API-backed AppComponents require a FastAPI resource release

Main Sequence is platform-first.

If an AppComponent depends on a project API, that API must already exist as:

- a FastAPI project resource
- a corresponding FastAPI `ResourceRelease`

before the AppComponent is considered usable from Command Center.

Do not treat a local dev server, an unregistered API file, or a discovered-but-unreleased FastAPI resource as "good enough" for AppComponent readiness.

This skill does not create resources or releases. If the FastAPI project resource or FastAPI release does not exist yet, route that work to:

- `.agents/skills/platform_operations/orchestration_and_releases/SKILL.md`

Only return to AppComponent contract work once the backing API deployment surface is real on the platform.

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

### 6.1 `x-ui-role` is what makes the contract render as richer UI

For AppComponent contracts, prefer SDK models whose OpenAPI schema carries the explicit UI role markers.

The AppComponent should always try to implement:

- `"x-ui-role": "editable-form"` for input-side contracts
- `"x-ui-role": "notification"` for response-side contracts

These markers are not cosmetic. They are what tell Command Center to treat the payload as a richer UI contract instead of generic JSON.

That means:

- use `EditableFormDefinition` and related input models when the AppComponent needs a specialized input form
- use `NotificationDefinition` for response-side user feedback when the backend should return a banner-style notification
- do not handcraft loose dictionaries for these cases when the SDK model already exists
- keep input and response contracts separate instead of overloading one model to do both jobs

## Review Rules

When reviewing an AppComponent task, look for:

- inferred or guessed `widget_id` values
- AppComponent work that skipped `registered_widget_type list/detail`
- a custom form that should have been auto-generated
- a flat autogenerated form that should have been specialized
- unstable or poorly named field tokens
- wrong `FormFieldKind` choices
- generic API output being used where an exact widget-facing contract should have been returned
- AppComponent contracts that should be richer UI surfaces but do not use the SDK model carrying the correct `x-ui-role`
- confusion between workspace concerns and AppComponent contract concerns
- AppComponent work assuming a project API is usable before a FastAPI resource and FastAPI `ResourceRelease` exist

## Validation Checklist

Do not claim success until you have checked:

- the target `widget_id` was verified through:
  - `mainsequence cc registered_widget_type list --json`
  - `mainsequence cc registered_widget_type detail <WIDGET_ID> --json`
- registry detail was used as the first source of truth
- the choice between autogenerated form and custom form is intentional
- custom forms use `EditableFormDefinition`
- sections and fields are explicit where needed
- field tokens are stable and meaningful
- field kinds reflect business meaning
- input and output contracts are not mixed together
- input-side AppComponent contracts use `"x-ui-role": "editable-form"` when a specialized form is intended
- response-side AppComponent contracts use `"x-ui-role": "notification"` when the API is returning user-facing banner feedback
- widget-facing outputs use exact SDK response models when applicable
- registry detail was used first
- SDK client models were used second
- local repository docs/models/examples were used only after the first two sources still left unresolved contract questions
- any API-backed AppComponent depends on a FastAPI project resource that already exists
- any API-backed AppComponent depends on a FastAPI `ResourceRelease` that already exists

## This Skill Must Stop And Escalate When

- the target widget cannot be identified in the registered widget catalog
- the task depends on guessed widget behavior without registry verification
- registry detail is insufficient and the required contract cannot be resolved from Main Sequence docs/models in this repository
- the task is really about workspace structure instead of AppComponent contracts
- the form contract is unclear but a custom form is being forced anyway
- the widget-facing output contract is unclear and no exact SDK model is available
- the task is really a generic API design problem rather than an AppComponent problem
- the AppComponent depends on a project API that does not yet exist as a FastAPI resource and FastAPI `ResourceRelease`

Do not guess through AppComponent boundaries.
