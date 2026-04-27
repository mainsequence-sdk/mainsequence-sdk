# Command Center Tabular Data Contracts

Command Center generic table, chart, statistic, curve, transform, and agent-facing data consumers
consume a canonical tabular frame:

```text
core.tabular_frame@v1
```

If you are building a FastAPI route that returns generic tabular data for Command Center, use the
SDK models in:

```python
mainsequence.client.command_center.data_models
```

The primary response model is:

```python
TabularFrameResponse
```

This model is the SDK boundary for `core.tabular_frame@v1`. It is not tied to one widget
implementation.

## Canonical Frame Shape

`TabularFrameResponse` contains:

- `status`
- `error`
- `columns`
- `rows`
- `fields`
- `meta`
- `source`

Required fields:

- `status`
- `columns`
- `rows`

Optional fields:

- `error`
- `fields`
- `meta`
- `source`

Top-level fields should stay generic. Source-specific runtime details belong under
`source.context`, not beside `status`, `columns`, and `rows`.

## SDK Models

Use these generic SDK models for canonical tabular output:

- `TabularFrameResponse`
- `TabularFrameFieldResponse`
- `TabularFrameMetaResponse`
- `TabularFrameSourceResponse`
- `TabularTimeSeriesMetaResponse`

These models keep the API boundary explicit and let FastAPI validate the response before Command
Center consumes it.

## Minimal Example

```python
from fastapi import APIRouter

from mainsequence.client.command_center.data_models import (
    TabularFrameFieldResponse,
    TabularFrameResponse,
    TabularFrameSourceResponse,
)

router = APIRouter()


@router.get(
    "/workspace/customers",
    response_model=TabularFrameResponse,
)
def get_customers_frame() -> TabularFrameResponse:
    rows = [
        {"customer_code": "ACME", "name": "Acme Capital", "region": "US"},
        {"customer_code": "BETA", "name": "Beta Treasury", "region": "EU"},
    ]

    return TabularFrameResponse(
        status="ready",
        columns=["customer_code", "name", "region"],
        rows=rows,
        fields=[
            TabularFrameFieldResponse(
                key="customer_code",
                label="Customer Code",
                type="string",
                provenance="manual",
            ),
            TabularFrameFieldResponse(
                key="name",
                label="Name",
                type="string",
                provenance="manual",
            ),
            TabularFrameFieldResponse(
                key="region",
                label="Region",
                type="string",
                provenance="manual",
            ),
        ],
        source=TabularFrameSourceResponse(
            kind="api",
            label="Customers API",
        ),
    )
```

## Source Context

Keep source-specific details in `source.context`.

```python
from mainsequence.client.command_center.data_models import (
    TabularFrameResponse,
    TabularFrameSourceResponse,
)


frame = TabularFrameResponse(
    status="ready",
    columns=["x", "y"],
    rows=[{"x": 1, "y": 2.5}],
    source=TabularFrameSourceResponse(
        kind="connection-query",
        id="customers-query",
        label="Customers query",
        context={
            "limit": 100,
            "rangeStartMs": 1710000000000,
            "rangeEndMs": 1710086400000,
        },
    ),
)
```

This keeps the canonical frame stable while preserving source metadata that some workflows need.

## Time-Series Metadata

When a tabular frame is meant for chart, curve, or time-aware consumers, include `meta.timeSeries`.

```python
from mainsequence.client.command_center.data_models import (
    TabularFrameMetaResponse,
    TabularFrameResponse,
    TabularTimeSeriesMetaResponse,
)


frame = TabularFrameResponse(
    status="ready",
    columns=["timestamp", "price"],
    rows=[
        {"timestamp": 1710000000000, "price": 101.4},
        {"timestamp": 1710086400000, "price": 102.1},
    ],
    meta=TabularFrameMetaResponse(
        timeSeries=TabularTimeSeriesMetaResponse(
            shape="long",
            timeField="timestamp",
            sorted=True,
            valueField="price",
        ),
    ),
)
```

`timeUnit` defaults to `ms` and `timezone` defaults to `UTC`.

## Adapter From API

Adapter from API providers can either:

- return `TabularFrameResponse` directly from an operation, or
- return provider-native JSON and declare a `core.tabular_frame@v1` response mapping in the
  well-known Command Center contract

Generic Command Center consumers should not be bound to provider-native JSON without an exact
mapping into `core.tabular_frame@v1`.

## Validation Rules

Before claiming an API returns canonical Command Center tabular data, verify:

- the route declares `response_model=TabularFrameResponse`
- `columns` contains the ordered field keys consumers need
- every row is an object keyed by column name
- `fields` describes important schema and formatting details
- `meta.timeSeries` is present for time-aware consumers
- source-specific fields live in `source.context`
- no widget-specific or source-specific fields are added at the top level
