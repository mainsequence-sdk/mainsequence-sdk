# Command Center Widget Data Contracts

Some Main Sequence widgets do not want arbitrary JSON. They expect a specific frontend contract.

If you are building an API that should feed a widget directly, use the SDK models in:

```python
mainsequence.client.command_center.data_models
```

These contracts are the exact validated structure expected at the widget boundary.

Right now this file is focused on the Command Center `data-node-table-visualizer` widget. That matters because this is not a generic "any widget" JSON layer. It is a concrete, versioned widget contract.

## Why This Matters

Without these models, it is too easy to drift away from what the widget actually expects.

These Pydantic contracts give you:

- explicit field names
- validation of required structure
- stable timestamp behavior
- a clear split between data and presentation
- early rejection of payload drift through `extra="forbid"`

That is especially useful when your API is not just returning generic business JSON, but is trying to feed a Main Sequence widget directly.

## The Important Rule

If the endpoint is intended to feed a Main Sequence widget directly, treat the response model as part of the widget contract, not as a convenience type.

That means:

- declare the response with the SDK model
- build the payload against the model, not against loose dictionaries
- let FastAPI and Pydantic validate the shape before it reaches the frontend

## The Main Example: Data Node Table Widget

The file currently documents the contract for the Command Center `data-node-table-visualizer` widget.

The important mental model is that there are two layers:

1. `DataNodeTableSourceInputResponse`
   This answers: "What dataset should the widget display?"

2. `DataNodeTableWidgetPropsResponse`
   This answers: "How should the widget display that dataset?"

If you keep those two layers separate, the contract becomes much easier to reason about.

## 1. The Upstream Data Contract

Use `DataNodeTableSourceInputResponse` for the incoming tabular dataset.

Core fields:

- `status`
- `error`
- `columns`
- `rows`
- `fields`
- `source`
- `dataNodeId`
- `limit`
- `rangeStartMs`
- `rangeEndMs`
- `uniqueIdentifierList`
- `updatedAtMs`

Important shape detail:

- `columns` is an ordered list of column keys
- `rows` is a list of JSON-style row objects keyed by column name

So this layer looks like:

```python
{
    "status": "ready",
    "columns": ["customer_code", "name", "region"],
    "rows": [
        {"customer_code": "ACME", "name": "Acme Capital", "region": "US"},
        {"customer_code": "BETA", "name": "Beta Treasury", "region": "EU"},
    ],
}
```

This is the right contract when your API is publishing table data for the widget to consume.

## 2. The Presentation Contract

Use `DataNodeTableWidgetPropsResponse` for widget-owned presentation state.

This layer includes:

- binding/source metadata
- local adapted frame
- schema
- column overrides
- toolbar and pagination settings
- value labels
- conditional formatting rules

Important shape detail:

- `columns` is still ordered
- `rows` is now positional, not keyed
- each row must align with `columns` by index

So this layer looks more like:

```python
{
    "columns": ["customer_code", "balance_usd", "status"],
    "rows": [
        ["ACME", 150000.0, "healthy"],
        ["BETA", 91250.0, "warning"],
    ],
}
```

That difference is easy to miss.

`DataNodeTableSourceInputResponse.rows`:
- row objects keyed by column name

`DataNodeTableWidgetPropsResponse.rows`:
- positional arrays aligned with `columns`

## 3. Schema, Labels, And Rules

The data-model file also makes the presentation pieces explicit:

- `TableFieldResponse`
  upstream schema metadata
- `SourceMetadataResponse`
  provenance and source context
- `DataNodeTableColumnSchemaResponse`
  local base schema for the widget
- `DataNodeTableColumnOverrideResponse`
  per-column UI overrides
- `DataNodeTableValueLabelResponse`
  value-to-label mappings for chip-like rendering
- `DataNodeTableConditionalRuleResponse`
  numeric conditional formatting rules

This is the contract you want when the widget should do more than just dump a plain table.

Examples:

- rename a header
- render a numeric column as currency
- pin a column left
- map `healthy` to a green semantic label
- color negative values with a warning tone

## 4. Binding And Source Context

The widget-props contract also carries the binding and source-context layer the frontend needs in order to understand what this table instance is attached to.

Important fields include:

- `sourceMode`
- `sourceWidgetId`
- `dataNodeId`
- `dateRangeMode`
- `fixedStartMs`
- `fixedEndMs`
- `uniqueIdentifierList`
- `limit`

The practical meaning is:

- the source payload tells the widget what data is available
- the widget-props payload tells the widget how this specific instance is bound and configured

This distinction matters when one upstream dataset can be rendered in different ways by different widget instances.

## 5. Timestamp Behavior

All `*Ms` fields in these contracts serialize as JavaScript epoch timestamps in milliseconds.

Python-side validation accepts:

- timezone-aware `datetime`
- ISO 8601 strings with timezone
- JavaScript epoch milliseconds

Serialization always emits:

- JavaScript epoch milliseconds

This matters because it lets Python services work with normal `datetime` values while still satisfying the frontend contract.

## 6. Example: Return A Widget-Ready Source Payload

```python
from fastapi import APIRouter

from mainsequence.client.command_center.data_models import (
    DataNodeTableSourceInputResponse,
    SourceMetadataResponse,
    TableFieldResponse,
)

router = APIRouter()


@router.get(
    "/widgets/customers/source",
    response_model=DataNodeTableSourceInputResponse,
)
def get_customers_table_source() -> DataNodeTableSourceInputResponse:
    rows = [
        {"customer_code": "ACME", "name": "Acme Capital", "region": "US"},
        {"customer_code": "BETA", "name": "Beta Treasury", "region": "EU"},
    ]

    return DataNodeTableSourceInputResponse(
        status="ready",
        columns=["customer_code", "name", "region"],
        rows=rows,
        fields=[
            TableFieldResponse(
                key="customer_code",
                label="Customer Code",
                type="string",
                provenance="manual",
            ),
            TableFieldResponse(
                key="name",
                label="Name",
                type="string",
                provenance="manual",
            ),
            TableFieldResponse(
                key="region",
                label="Region",
                type="string",
                provenance="manual",
            ),
        ],
        source=SourceMetadataResponse(
            kind="custom-api",
            label="Tutorial Customers API",
        ),
    )
```

This is the cleanest way to say:

- here is the dataset
- here is its schema
- here is the source metadata the widget can use

## 7. Example: Return Widget-Owned Presentation State

When the frontend also needs table-specific UI state, return `DataNodeTableWidgetPropsResponse`.

Example:

```python
from fastapi import APIRouter

from mainsequence.client.command_center.data_models import (
    DataNodeTableColumnOverrideResponse,
    DataNodeTableColumnSchemaResponse,
    DataNodeTableConditionalRuleResponse,
    DataNodeTableValueLabelResponse,
    DataNodeTableWidgetPropsResponse,
)

router = APIRouter()


@router.get(
    "/widgets/customers/props",
    response_model=DataNodeTableWidgetPropsResponse,
)
def get_customers_table_props() -> DataNodeTableWidgetPropsResponse:
    return DataNodeTableWidgetPropsResponse(
        sourceMode="manual",
        columns=["customer_code", "balance_usd", "status"],
        rows=[
            ["ACME", 150000.0, "healthy"],
            ["BETA", 91250.0, "warning"],
        ],
        schema=[
            DataNodeTableColumnSchemaResponse(
                key="customer_code",
                label="Customer Code",
                format="text",
            ),
            DataNodeTableColumnSchemaResponse(
                key="balance_usd",
                label="Balance",
                format="currency",
                prefix="$",
                decimals=2,
                heatmapEligible=True,
            ),
            DataNodeTableColumnSchemaResponse(
                key="status",
                label="Status",
                format="text",
                categorical=True,
            ),
        ],
        columnOverrides={
            "balance_usd": DataNodeTableColumnOverrideResponse(
                heatmap=True,
                visualRangeMode="fixed",
                visualMin=0,
                visualMax=200000,
            )
        },
        valueLabels=[
            DataNodeTableValueLabelResponse(
                columnKey="status",
                value="healthy",
                label="Healthy",
                tone="success",
            ),
            DataNodeTableValueLabelResponse(
                columnKey="status",
                value="warning",
                label="Warning",
                tone="warning",
            ),
        ],
        conditionalRules=[
            DataNodeTableConditionalRuleResponse(
                id="balance-alert",
                columnKey="balance_usd",
                operator="lt",
                value=100000,
                tone="warning",
            )
        ],
        showToolbar=True,
        showSearch=True,
        zebraRows=True,
    )
```

## 8. When To Use These Contracts

Use them when:

- your API is meant to feed a Main Sequence widget directly
- you want the response contract to match the frontend exactly
- you want FastAPI/OpenAPI to document the real widget payload

Do not use them when:

- the endpoint is just a generic business API
- the client is not a Main Sequence widget
- the route should stay independent from Command Center-specific UI concepts

In that case, return your own application models instead.
- here is where it came from

## 6. Example: Return Widget Props

```python
from fastapi import APIRouter

from mainsequence.client.command_center.data_models import (
    DataNodeTableColumnOverrideResponse,
    DataNodeTableColumnSchemaResponse,
    DataNodeTableConditionalRuleResponse,
    DataNodeTableValueLabelResponse,
    DataNodeTableWidgetPropsResponse,
)

router = APIRouter()


@router.get(
    "/widgets/customers/props",
    response_model=DataNodeTableWidgetPropsResponse,
)
def get_customers_table_props() -> DataNodeTableWidgetPropsResponse:
    return DataNodeTableWidgetPropsResponse(
        sourceMode="direct",
        columns=["customer_code", "balance_usd", "status"],
        rows=[
            ["ACME", 150000.0, "healthy"],
            ["BETA", 91250.0, "warning"],
        ],
        schema=[
            DataNodeTableColumnSchemaResponse(
                key="customer_code",
                label="Customer Code",
                format="text",
                pinned="left",
            ),
            DataNodeTableColumnSchemaResponse(
                key="balance_usd",
                label="Balance",
                format="currency",
                decimals=0,
            ),
            DataNodeTableColumnSchemaResponse(
                key="status",
                label="Status",
                format="text",
            ),
        ],
        showToolbar=True,
        showSearch=True,
        zebraRows=True,
        pagination=True,
        pageSize=25,
        columnOverrides={
            "balance_usd": DataNodeTableColumnOverrideResponse(
                heatmap=True,
                format="currency",
                visualRangeMode="fixed",
                visualMin=0,
                visualMax=200000,
            )
        },
        valueLabels=[
            DataNodeTableValueLabelResponse(
                columnKey="status",
                value="healthy",
                label="Healthy",
                tone="success",
            ),
            DataNodeTableValueLabelResponse(
                columnKey="status",
                value="warning",
                label="Warning",
                tone="warning",
            ),
        ],
        conditionalRules=[
            DataNodeTableConditionalRuleResponse(
                id="balance-warning",
                columnKey="balance_usd",
                operator="lt",
                value=100000.0,
                tone="warning",
            )
        ],
    )
```

This is the right contract when you want to control the widget's rendering behavior, not just deliver rows.

## 7. Practical Rule

If your API is supposed to feed a Main Sequence widget directly:

- do not handcraft loose dictionaries
- do not guess the row shape
- do not guess timestamp formats
- build the response from the SDK contracts

That gives you the exact structure expected by the widget.

## 8. Relationship To AppComponent Forms

This page is about output contracts for widgets.

The forms page is about input contracts for AppComponent widgets.

The practical split is:

- [Forms](forms.md): how the user should fill in specialized inputs
- this page: what structured payload a widget expects back from your API

## Related Pages

- [Command Center Overview](index.md)
- [Command Center Workspaces](workspaces.md)
- [Command Center Forms](forms.md)
- [Part 3.2 — Create Your First API](../../tutorial/create_your_first_api.md)
- [FastAPI Implementation Details](../../tutorial/fastapi_tutorial/implementation_details.md)
