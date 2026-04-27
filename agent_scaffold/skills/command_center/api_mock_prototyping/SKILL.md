---
name: command-center-api-mock-prototyping
description: Use this skill when the task is about validating a planned Command Center AppComponent API contract before backend deployment. This skill owns `apiTargetMode: "mock-json"`, synthetic request and response contract authoring, request-side versus response-side UI metadata separation, and testing downstream bindings in a workspace before any FastAPI image build or ResourceRelease, including checking that generic tabular consumers receive `core.tabular_frame@v1`. It does not own FastAPI implementation, workspace layout design beyond the test harness, or release creation.
---

# Command Center API Mock Prototyping

## Overview

Use this skill when the task is about testing a planned API contract in Command Center before building an image or creating a FastAPI `ResourceRelease`.

This skill is for:

- `apiTargetMode: "mock-json"`
- synthetic API contract prototyping for AppComponents
- request-side and response-side UI metadata
- mock response rendering
- published output and downstream binding validation
- canonical `core.tabular_frame@v1` widget-consumption contract when generic tabular consumers are involved
- deciding when the contract is stable enough to hand off to real FastAPI implementation and release work

## This Skill Can Do

- define a mock AppComponent API contract before backend deployment
- encode method, path, request schema, and response schema into `mockJson`
- separate request-side UI metadata from response-side UI metadata correctly
- prototype banner-style notification responses
- prototype editable-form responses
- prototype supported request-side custom UI such as async select search
- validate downstream widget bindings using mock mode
- decide when a contract is stable enough to hand off to the real API and release workflow

## This Skill Must Not Claim

This skill must not claim ownership of:

- FastAPI route implementation
- Pydantic backend request or response model implementation
- workspace layout design beyond what is required to test the widget contract
- image creation
- project resource creation
- `ResourceRelease` creation

## Route Adjacent Work

- AppComponents and custom forms:
  `.agents/skills/command_center/app_components/SKILL.md`
- Command Center workspaces and widget mutation:
  `.agents/skills/command_center/workspace_builder/SKILL.md`
- APIs and FastAPI:
  `.agents/skills/application_surfaces/api_surfaces/SKILL.md`
- Jobs, images, resources, and releases:
  `.agents/skills/platform_operations/orchestration_and_releases/SKILL.md`

## Read First

1. Verify the widget catalog through the CLI:
   - `mainsequence cc registered_widget_type list --json`
   - identify the target `widget_id`
   - `mainsequence cc registered_widget_type detail <WIDGET_ID> --json`
2. The SDK client models:
   - `mainsequence/client/command_center/app_component.py`
   - `mainsequence/client/command_center/workspace.py`
3. `.agents/skills/command_center/app_components/SKILL.md`
4. `.agents/skills/command_center/workspace_builder/SKILL.md`

Only after the mock contract is stable:

5. `.agents/skills/application_surfaces/api_surfaces/SKILL.md`
6. `.agents/skills/platform_operations/orchestration_and_releases/SKILL.md`

## Inputs This Skill Needs

Before prototyping a mock API contract, collect or infer:

- verified `widget_id`
- target workspace and widget instance
- intended endpoint:
  - method
  - path
- request contract:
  - query/path/header parameters
  - request body content type
  - request body schema
  - request-side UI intent
- response contract:
  - status
  - content type
  - response body shape
  - response-side UI intent
- whether downstream bindings must be validated
- whether any downstream generic tabular consumer requires `core.tabular_frame@v1`
- the intended real target mode after mock validation:
  - `manual`
  - `main-sequence-resource-release`

If the intended API contract is still moving, do not deploy yet. Stay in mock mode.

## Required Decisions

For every non-trivial mock prototyping task, decide:

1. Which AppComponent widget instance should host the mock contract?
2. What is the final intended HTTP method and path?
3. What request-side UI should the widget render?
4. What response-side UI should the widget render?
5. Does the response represent immediate client feedback or business data?
6. Which downstream bindings must be tested before backend deployment?
7. Is the contract stable enough to implement for real, or should the workspace continue iterating in mock mode?

## Build Rules

### 1. `mock-json` is the default predeployment workflow

Do not build and release a FastAPI resource just to test AppComponent UX.

Default sequence:

1. define the planned API contract
2. encode it into `mockJson`
3. inject `apiTargetMode: "mock-json"` into the widget instance
4. validate request rendering, response rendering, published outputs, and downstream bindings
5. iterate until stable
6. only then implement the real FastAPI route
7. only then build the image, create the resource release, and switch the widget away from mock mode

### 2. Mock mode is fully local

In mock mode, the widget does not call a deployed API.

Use:

- `apiTargetMode: "mock-json"`
- `showResponse: true`

The widget should behave as if the API exists, but the source of truth is the injected `mockJson` contract.

### 3. Use this base injection shape

```json
{
  "apiTargetMode": "mock-json",
  "method": "post",
  "path": "/my-endpoint",
  "requestBodyContentType": "application/json",
  "showResponse": true,
  "mockJson": {
    "version": 1,
    "operation": {
      "method": "post",
      "path": "/my-endpoint",
      "summary": "Prototype endpoint",
      "description": "Synthetic API used before deployment."
    },
    "request": {
      "parameters": [],
      "bodyContentType": "application/json",
      "bodyRequired": false,
      "bodySchema": {}
    },
    "response": {
      "status": 200,
      "contentType": "application/json",
      "body": {},
      "ui": {
        "role": "notification",
        "widget": "banner-v1"
      }
    }
  }
}
```

### 4. Never mix request UI and response UI

Keep the separation strict:

- request-side UI metadata belongs in `mockJson.operation.ui`
- response-side UI metadata belongs in `mockJson.response.ui`

Do not put response UI metadata under `operation.ui`.
Do not put request UI metadata under `response.ui`.

### 5. Prefer notification response UI for immediate feedback

If the response is acknowledging an action rather than returning business data, use:

- `response.ui.role = "notification"`
- `response.ui.widget = "banner-v1"`

Expected response body shape:

```json
{
  "title": "Action completed",
  "message": "Prototype response from mock API.",
  "tone": "success",
  "details": "This is a local mock."
}
```

Valid notification tones:

- `success`
- `primary`
- `info`
- `warning`
- `error`

### 6. Use editable-form response UI only when the response itself is a form contract

If the response should render a specialized form, use:

- `response.ui.role = "editable-form"`
- `response.ui.widget = "definition-v1"`

The response body should match the intended editable-form payload closely.

### 7. Request-side custom UI must be explicit

If the request form should render as a custom async select input, declare it in `operation.ui` and also declare the matching request helper parameters.

Supported pattern:

```json
{
  "role": "async-select-search",
  "widget": "select2",
  "selectionType": "single",
  "searchParam": "country_search",
  "searchParamAliases": ["country_query"],
  "itemsPath": "items",
  "itemValueField": "code",
  "itemLabelField": "label"
}
```

If this UI is used, the request contract must also include matching helper params in `request.parameters`.

### 8. Validate bindings before backend deployment

Use the mock widget to validate:

- request form rendering
- response rendering
- published outputs
- downstream bindings to other widgets
- `core.tabular_frame@v1` shape when downstream table, chart, statistic, curve, or agent-facing data consumers are involved

Do not treat card-only rendering as enough when the workspace depends on downstream output behavior.

## Review Rules

When reviewing a mock prototyping task, look for:

- a push toward image build or release before the AppComponent contract is stable
- `apiTargetMode` not set to `mock-json`
- missing `showResponse: true`
- request-side and response-side UI metadata mixed together
- mock response body not matching the intended final API contract
- banner-style user feedback returned as loose JSON without `response.ui.role = "notification"`
- async select request UI declared without the matching helper parameters
- downstream bindings not tested even though the widget publishes outputs
- generic tabular consumers bound to a response that does not match `core.tabular_frame@v1`

## Validation Checklist

Do not claim success until you have checked:

- the widget type was verified through:
  - `mainsequence cc registered_widget_type list --json`
  - `mainsequence cc registered_widget_type detail <WIDGET_ID> --json`
- the widget instance uses `apiTargetMode: "mock-json"`
- `showResponse` is enabled
- `mockJson.operation.method` and `mockJson.operation.path` match the intended final API
- request-side UI metadata lives only in `mockJson.operation.ui`
- response-side UI metadata lives only in `mockJson.response.ui`
- immediate feedback responses use `role: "notification"` when the route is acknowledging an action
- editable-form responses use `role: "editable-form"` only when the response is actually a form contract
- the mock response body closely matches the intended final API contract
- published outputs and downstream bindings were validated before leaving mock mode
- generic tabular consumers receive `core.tabular_frame@v1`
- the task only hands off to real FastAPI implementation and release work after the contract is stable

## This Skill Must Stop And Escalate When

- the target widget cannot be verified in the registry
- the request or response contract is still unclear
- request-side UI and response-side UI are being mixed
- someone proposes building an image or creating a release just to test AppComponent UX
- downstream bindings matter but were not tested in mock mode
- the task is actually real FastAPI implementation rather than contract prototyping

Do not skip directly from idea to deployment when the AppComponent contract can be validated locally in mock mode first.
