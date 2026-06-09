# Command Center Forms

When an AppComponent widget needs a specialized editable response experience, the right object to return is `EditableFormDefinition`.

This is a response-side UI contract. Command Center renders it after the selected operation returns successfully and the OpenAPI response schema advertises:

- `"x-ui-role": "editable-form"`
- `"x-ui-widget": "definition-v1"`

It is not the contract for the pre-submit request form. Request inputs are generated from OpenAPI path, query, header, and JSON body metadata.

Use it when you need things like:

- grouped sections
- domain-specific labels
- richer field kinds such as percent
- explicit defaults
- custom field tokens for downstream binding or draft state

## When You Do Not Need An Editable Response Form

For simple function arguments, Command Center can resolve the default request form directly from the operation contract.

That means if your endpoint or function only exposes normal flat request arguments with a straightforward shape, you usually do **not** need to return an editable form definition.

Typical examples:

- a few scalar query parameters
- a small request body with standard primitive fields
- simple enums, booleans, dates, and numbers

In those cases, let Command Center generate the form automatically.

## When You Should Return `EditableFormDefinition`

Use `EditableFormDefinition` when you want the operation response to render as a stateful editable form with grouped sections and explicit field definitions.

This is the right path when:

- the generic response viewer is too flat
- you want a product-specific language instead of raw parameter names
- you need sectioned editable response fields
- you want more control over editable vs read-only fields
- you want percent or formatter hints
- you need stable field tokens for app-specific behavior

## The Core Objects

The SDK models are:

- `EditableFormDefinition`
- `FormSectionDefinition`
- `FormFieldDefinition`
- `FormFieldKind`

These live in:

- `mainsequence.client.command_center`

The important mental model is:

- one form
- one or more sections
- each section contains fields
- each field has a stable token, a user-facing label, a kind, and editability rules

## Example

```python
from fastapi import APIRouter

from mainsequence.client.command_center import (
    EditableFormDefinition,
    FormFieldDefinition,
    FormFieldKind,
    FormSectionDefinition,
)

router = APIRouter()


@router.get(
    "/pricing-context/{product_id}",
    response_model=EditableFormDefinition,
)
def get_pricing_context_form(product_id: str) -> EditableFormDefinition:
    return EditableFormDefinition(
        form_id=f"pricing-context:{product_id}:v1",
        title="NOTE",
        description="custom note based on DoubleNoTouchMXN with additional funding parameter.",
        sections=[
            FormSectionDefinition(
                id="custom_fields",
                title="Custom Fields",
                fields=[
                    FormFieldDefinition(
                        token="custom::funding_rate",
                        name="funding_rate",
                        label="Funding Rate",
                        kind=FormFieldKind.PERCENT,
                        editable=True,
                        required=True,
                        value=None,
                        default_value=None,
                        formatter="percent",
                    ),
                    FormFieldDefinition(
                        token="custom::notional",
                        name="notional",
                        label="Notional",
                        kind=FormFieldKind.NUMBER,
                        editable=True,
                        required=False,
                        value=100,
                        default_value=100,
                    ),
                ],
            )
        ],
    )
```

## What This Example Is Doing

- `form_id` gives the form a stable identity for reset/reload semantics
- `title` and `description` control the form header
- `sections` let you group fields into meaningful blocks
- `token` is the stable field identity used for draft state and downstream bindings
- `name` is the backend field identifier
- `label` is what the user sees
- `kind` tells Command Center how to render the field
- `formatter` gives the frontend an extra presentation hint

## Why `token` Matters

The `token` field is not cosmetic.

It is the stable global identifier for the field in the Command Center interaction model. That is what makes it useful for:

- draft state
- bindings
- app-specific coordination

If you are defining an editable response form, keep tokens stable across compatible versions.

## Choosing The Right `FormFieldKind`

The built-in field kinds are:

- `STRING`
- `NUMBER`
- `INTEGER`
- `BOOLEAN`
- `DATE`
- `DATETIME`
- `PERCENT`
- `ENUM`
- `JSON`

Use the kind that reflects the business meaning, not just the raw transport type.

Example:

- `PERCENT` is better than `NUMBER` for a funding rate if the UI should treat it as a percentage input

## A Practical Rule

Start with the default generated request form and generic response renderer.

Only introduce `EditableFormDefinition` when you actually need:

- sectioned layout
- specialized labels
- richer formatting
- explicit token control

That keeps the Command Center integration simpler and makes the editable response form a deliberate choice instead of a default habit.

For request-side custom UI, keep using OpenAPI request metadata. The current supported request-side enhancement is operation-level `x-ui-widget: select2` with `x-ui-role: async-select-search` for query-parameter search helpers.

## Relationship To AppComponent Widgets

This page is specifically relevant for AppComponent widgets.

The usual pattern is:

1. the widget resolves an operation contract
2. Command Center builds the request form automatically from OpenAPI parameters and request body schema
3. the operation executes
4. if the response advertises editable-form UI metadata, the app returns `EditableFormDefinition`

So the editable form is a response renderer for richer UX, not a requirement for every AppComponent.

It is also important not to mix this with widget output contracts.

- OpenAPI parameters and request body schemas describe how the widget should collect pre-submit input
- `EditableFormDefinition` describes a response-side editable form session
- the models in `mainsequence.client.command_center.data_models` describe the exact output shape expected by some widgets when your API feeds them directly

If your API is powering a widget end-to-end, you may use both:

- an OpenAPI request contract for the widget input
- an editable-form response contract for server-driven editable response UI
- a widget data contract for the widget output

## Related Pages

- [Command Center Overview](index.md)
- [Command Center Workspaces](workspaces.md)
- [Widget Data Contracts](widget_data_contracts.md)
- [CLI Overview](../../cli/index.md)
