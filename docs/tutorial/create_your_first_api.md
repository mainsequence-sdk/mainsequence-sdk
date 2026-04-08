# Part 3.2: Create Your First API
**Expose tutorial data through a small application-facing service**

## Quick Summary

In this tutorial, you will:

- create a minimal FastAPI application inside the project
- expose a health endpoint
- read tutorial `SimpleTable` rows from an API endpoint
- read tutorial `DataNode` output through `APIDataNode`
- understand how to return exact Command Center widget contracts when the API should feed a Main Sequence widget directly
- run the API locally
- understand how API deployment fits the same image/resource/release model used elsewhere in Main Sequence

This chapter belongs here for a reason.

Up to this point, the tutorial has shown you how to publish data with `DataNode`s and how to model application-facing 
rows with `SimpleTable`s. An API is the next layer: it lets you turn those project resources into a request/response surface with your own validation, routing, and response shape.

## 1. When To Build an API Instead of Another Table

Use an API when you need application logic, not just persisted data.

Typical reasons:

- combine several `DataNode`s or tables in one endpoint
- validate request parameters before querying the backend
- expose a simpler contract to downstream applications
- keep frontend or agent clients away from the raw storage/query details

If all you need is a reusable dataset, keep using `DataNode`s.

If all you need is small relational application data, keep using `SimpleTable`.

If you need an application surface on top of those building blocks, add an API.

## 2. Add the API Dependencies

This tutorial uses `FastAPI` and `uvicorn`.

From the project root:

```bash
uv add fastapi uvicorn
```

That keeps the dependency change inside the same project workflow you already used for the rest of the tutorial.

## 3. Create the API Module

Create a small API package inside the repository:

```text
src/apis/tutorial_api/main.py
```

If you want the more detailed implementation walkthrough, use the dedicated FastAPI tutorial section:

- [FastAPI Tutorial Overview](fastapi_tutorial/index.md)
- [FastAPI Implementation Details](fastapi_tutorial/implementation_details.md)

Example:

```python
import datetime as dt
import os

from fastapi import FastAPI, Query, Request

from mainsequence.client.fastapi import LoggedUserContextMiddleware
from mainsequence.tdag import APIDataNode

from examples.data_nodes.simple_tables import (
    CustomerRecord,
    CustomersUpdater,
    CustomersUpdaterConfiguration,
)


PROJECT_ID = os.getenv("MAIN_SEQUENCE_PROJECT_ID", "local").strip() or "local"
RANDOM_NODE_IDENTIFIER = f"example_random_number_{PROJECT_ID}_0.0"

app = FastAPI(
    title="Tutorial API",
    version="0.1.0",
    description="Small API that exposes tutorial SimpleTable and DataNode data.",
)

app.add_middleware(LoggedUserContextMiddleware)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/me")
def get_authenticated_user(request: Request) -> dict[str, object]:
    user = request.state.user
    return {
        "id": request.state.user_id,
        "username": user.username if user else None,
        "email": user.email if user else None,
    }


@app.get("/customers")
def list_customers(
    region: str | None = None,
    limit: int = Query(50, ge=1, le=500),
) -> list[dict[str, object]]:
    customers_updater = CustomersUpdater(
        configuration=CustomersUpdaterConfiguration()
    )
    filter_expr = CustomerRecord.filters.region.eq(region) if region else None
    rows = customers_updater.execute_filter(filter_expr, limit=limit)
    return [row.model_dump(mode="json") for row in rows]


@app.get("/random-numbers")
def list_random_numbers(
    start_date: dt.date,
    end_date: dt.date,
) -> dict[str, object]:
    node = APIDataNode.build_from_identifier(identifier=RANDOM_NODE_IDENTIFIER)
    start_dt = dt.datetime.combine(start_date, dt.time.min, tzinfo=dt.UTC)
    end_dt = dt.datetime.combine(end_date, dt.time.max, tzinfo=dt.UTC)
    df = node.get_df_between_dates(start_date=start_dt, end_date=end_dt)

    return {
        "identifier": RANDOM_NODE_IDENTIFIER,
        "rows": df.reset_index().to_dict(orient="records"),
    }
```

Why this is a good tutorial example:

- `/me` shows how to read the currently authenticated Main Sequence user inside FastAPI
- `/customers` uses the `SimpleTable` objects you just created
- `/random-numbers` uses `APIDataNode.build_from_identifier(...)` to read a published table
- the API layer is thin and explicit

Important:

- if you want the resolved Main Sequence user attached to `request.state`, add `app.add_middleware(LoggedUserContextMiddleware)`
- that middleware binds request headers into the SDK auth context and populates `request.state.user` when a user can be resolved
- it does not authenticate requests or return `401` by itself

## 4. Why `APIDataNode` Fits Naturally in an API

Inside an API handler, `APIDataNode` is usually the right reader when:

- you already know the published table identifier
- you want a narrow read path
- you want the route logic to stay readable

That is the same reason it was introduced earlier for dashboard readers. The API layer is not rebuilding the `DataNode`; it is consuming the published table contract.

## 5. If The API Should Feed A Main Sequence Widget Directly

Sometimes the API is not meant for a generic frontend client. It is meant to feed a Main Sequence widget directly.

That is a different situation.

In that case, do not hand-build arbitrary JSON and hope the widget accepts it. Use the Command Center contracts in:

```python
mainsequence.client.command_center.data_models
```

For the Data Node Table widget, the two important models are:

- `DataNodeTableSourceInputResponse`
- `DataNodeTableWidgetPropsResponse`

The practical split is:

- `DataNodeTableSourceInputResponse`: the dataset the widget should display
- `DataNodeTableWidgetPropsResponse`: how the widget should display it

Declare these as the FastAPI `response_model` for widget-facing endpoints. That way the contract is validated in Python before the widget sees it.

Example:

```python
from mainsequence.client.command_center.data_models import (
    DataNodeTableSourceInputResponse,
    SourceMetadataResponse,
    TableFieldResponse,
)


@app.get(
    "/widgets/customers/source",
    response_model=DataNodeTableSourceInputResponse,
)
def get_customers_widget_source() -> DataNodeTableSourceInputResponse:
    rows = list_customers(region=None, limit=50)
    return DataNodeTableSourceInputResponse(
        status="ready",
        columns=["id", "customer_code", "name", "region"],
        rows=rows,
        fields=[
            TableFieldResponse(key="id", label="Id", type="integer", provenance="manual"),
            TableFieldResponse(key="customer_code", label="Customer Code", type="string", provenance="manual"),
            TableFieldResponse(key="name", label="Name", type="string", provenance="manual"),
            TableFieldResponse(key="region", label="Region", type="string", provenance="manual"),
        ],
        source=SourceMetadataResponse(
            kind="custom-api",
            label="Tutorial Customers API",
        ),
    )
```

This matters because these models give you the exact structure expected by the widget.

For the full contract breakdown, including the distinction between source rows and widget-props rows, see:

- [Command Center Widget Data Contracts](../knowledge/command_center/widget_data_contracts.md)

## 6. Run the API Locally

From the project root:

```bash
uv run uvicorn src.apis.tutorial_api.main:app --reload
```

Then open:

- `http://127.0.0.1:8000/health`
- `http://127.0.0.1:8000/me`
- `http://127.0.0.1:8000/docs`

The automatic FastAPI docs are useful here because they let you verify the request and response shape before you think about deployment.

## 7. Test It Against the Tutorial Data

The `/customers` route expects that you already created the tutorial simple-table data.

The `/me` route expects that the request is coming through a Main Sequence-authenticated context, because the middleware resolves the current platform user from request headers.

If you have not done that yet, run the example from the previous chapter:

```bash
uv run python examples/data_nodes/simple_tables.py
```

The `/random-numbers` route expects that the first `DataNode` tutorial has already been built and updated.

If needed, run the tutorial launcher again:

```bash
uv run python scripts/random_number_launcher.py
```

Now you can test the API endpoints:

```bash
curl "http://127.0.0.1:8000/customers?region=US"
curl "http://127.0.0.1:8000/random-numbers?start_date=2026-03-01&end_date=2026-03-31"
```

## 8. How API Deployment Fits the Platform Model

The deployment model is the same one you later use for dashboards:

1. push the API code with `mainsequence project sync`
2. build or select a project image for that pushed commit
3. let the platform discover project resources from that commit
4. create a release from the discovered resource

That is the important continuity point:

- `DataNode`s publish data contracts
- `SimpleTable`s publish application-facing rows
- APIs, dashboards, and agents are deployment-facing project resources built on top of that data layer

You can deploy the discovered FastAPI project resource through the same CLI surface used for other releases:

```bash
mainsequence project sync "Add tutorial API"
mainsequence project images create
mainsequence project project_resource list --filter resource_type=fastapi
mainsequence project project_resource create_fastapi
```

The CLI uses the same deployment model as dashboards and agents:

- pushed code
- matching project image
- discovered project resource
- release created from that resource

## 9. What To Keep Stable

Treat the API as a contract just like you treat a `DataNode` identifier or a `SimpleTable` schema as a contract.

Keep these stable unless you mean to introduce a breaking change:

- route paths
- request parameter names
- response field names
- the meaning of each endpoint

If the API is going to be consumed across projects, dashboards, or agents, stability matters just as much here as it does for tables.

## 10. Further Reading

For the data layer behind this chapter, see:

- [Data Nodes](../knowledge/data_nodes.md)
- [Command Center Widget Data Contracts](../knowledge/command_center/widget_data_contracts.md)
- [Simple Tables Overview](../knowledge/simple_tables/simple_table.md)
- [Simple Tables Filtering](../knowledge/simple_tables/filtering.md)
- [FastAPI Tutorial Overview](fastapi_tutorial/index.md)
- [FastAPI Implementation Details](fastapi_tutorial/implementation_details.md)

For the deployment model that already exists in the tutorial for project resources and releases, see:

- [Part 5.2 — Streamlit Integration II](dashboards/streamlit/streamlit_integration_2.md)
- [CLI Overview](../cli/index.md)
