# FastAPI Implementation Details

## Quick Summary

In this guide, you will go deeper into the FastAPI example from [Part 3.2 — Create Your First API](../create_your_first_api.md).

The focus here is implementation quality:

- how to structure the app
- how to keep routes thin
- how to read from `SimpleTable` and `APIDataNode` cleanly
- how to think about request validation and response contracts
- how FastAPI resources are discovered and deployed by the platform

## 1. Recommended Project Layout

For a small API, a simple layout is enough:

```text
src/
  apis/
    tutorial_api/
      main.py
      schemas.py
      services.py
      dependencies.py
```

Suggested responsibility split:

- `main.py`: create the FastAPI app and register routes
- `schemas.py`: request and response models
- `services.py`: data access and business logic
- `dependencies.py`: shared helpers such as updater factories

This matters because APIs become hard to maintain very quickly if route functions also contain all the data-access logic.

## 2. Keep Route Functions Thin

A route should mainly do three things:

1. accept validated input
2. call a service/helper
3. return a stable response shape

Example:

```python
from fastapi import APIRouter, Query

from .services import get_customers


router = APIRouter()


@router.get("/customers")
def list_customers(
    region: str | None = None,
    limit: int = Query(50, ge=1, le=500),
) -> list[dict[str, object]]:
    return get_customers(region=region, limit=limit)
```

The service layer then holds the actual query logic.

## 3. Reading From `SimpleTable`

For application-facing rows, `SimpleTableUpdater.execute_filter(...)` is usually the cleanest read path.

Example service:

```python
from examples.data_nodes.simple_tables import (
    CustomerRecord,
    CustomersUpdater,
    CustomersUpdaterConfiguration,
)


def get_customers(*, region: str | None, limit: int) -> list[dict[str, object]]:
    updater = CustomersUpdater(configuration=CustomersUpdaterConfiguration())
    filter_expr = CustomerRecord.filters.region.eq(region) if region else None
    rows = updater.execute_filter(filter_expr, limit=limit)
    return [row.model_dump(mode="json") for row in rows]
```

Why this is a good fit:

- it keeps the table schema as the source of truth
- it reuses the typed filter DSL
- it avoids hand-written HTTP request code in the route layer

## 4. Reading From `DataNode`

For published time-series tables, prefer `APIDataNode`.

Example service:

```python
import datetime as dt
import os

from mainsequence.tdag import APIDataNode


PROJECT_ID = os.getenv("MAIN_SEQUENCE_PROJECT_ID", "local").strip() or "local"
RANDOM_NODE_IDENTIFIER = f"example_random_number_{PROJECT_ID}_0.0"


def get_random_numbers(*, start_date: dt.date, end_date: dt.date) -> dict[str, object]:
    node = APIDataNode.build_from_identifier(identifier=RANDOM_NODE_IDENTIFIER)
    start_dt = dt.datetime.combine(start_date, dt.time.min, tzinfo=dt.UTC)
    end_dt = dt.datetime.combine(end_date, dt.time.max, tzinfo=dt.UTC)
    df = node.get_df_between_dates(start_date=start_dt, end_date=end_dt)
    return {
        "identifier": RANDOM_NODE_IDENTIFIER,
        "rows": df.reset_index().to_dict(orient="records"),
    }
```

That keeps the contract explicit:

- the API route knows which published table it is reading
- the `DataNode` itself remains the producer
- the API is only the consumer layer

## 5. Prefer Explicit Response Models

Returning raw dictionaries is fine for the first tutorial step, but real APIs should use response models.

Example:

```python
from pydantic import BaseModel


class CustomerOut(BaseModel):
    id: int
    customer_code: str
    name: str
    region: str
```

Then use:

```python
@router.get("/customers", response_model=list[CustomerOut])
def list_customers(...):
    ...
```

Why this is worth it:

- OpenAPI output is clearer
- clients get a stable contract
- accidental response-shape drift becomes easier to catch

## 6. Validate Inputs Early

FastAPI already validates query parameters, path parameters, and body payloads.

Use that instead of delaying validation until deep inside the business logic.

Examples:

- `limit: int = Query(50, ge=1, le=500)`
- explicit enums for mode switches
- date inputs typed as `datetime.date`

That keeps bad requests out of the data access layer.

## 7. Think In Contracts, Not Just Routes

Treat each endpoint as a product surface.

Keep stable:

- route names
- response model fields
- parameter names
- semantics of filters and default values

This is the same discipline already used for:

- `DataNode` identifiers
- `SimpleTable` schemas
- dashboard and agent interfaces

## 8. Local Development Flow

A practical local loop is:

```bash
uv add fastapi uvicorn
uv run python examples/data_nodes/simple_tables.py
uv run python scripts/random_number_launcher.py
uv run uvicorn src.apis.tutorial_api.main:app --reload
```

Then test:

```bash
curl "http://127.0.0.1:8000/health"
curl "http://127.0.0.1:8000/customers?region=US"
curl "http://127.0.0.1:8000/random-numbers?start_date=2026-03-01&end_date=2026-03-31"
```

## 9. Deployment Model For FastAPI Resources

FastAPI follows the same project-resource model used elsewhere in the platform.

The normal flow is:

1. push the API code
2. create or select a project image
3. list discovered project resources
4. create the FastAPI release

CLI flow:

```bash
mainsequence project sync -m "Add tutorial API"
mainsequence project images create
mainsequence project project_resource list --filter resource_type=fastapi
mainsequence project project_resource create_fastapi
```

Important continuity point:

- the image defines the runtime environment
- the resource identifies the FastAPI source file discovered from the pushed commit
- the release is the deployed unit

This is the same deployment pattern you already see with Streamlit dashboards and agents.

## 10. Common Mistakes

- putting all business logic directly in route functions
- returning unstable ad-hoc dictionaries as responses
- rebuilding producer logic in the API instead of consuming published `DataNode` outputs
- querying by business meaning but forgetting to keep the endpoint contract stable
- trying to deploy code that has not been pushed yet

## 11. Related Pages

- [Part 3.2 — Create Your First API](../create_your_first_api.md)
- [Part 3.1 — Working With Simple Tables](../working_with_simple_tables.md)
- [Data Nodes](../../knowledge/data_nodes.md)
- [Simple Tables Overview](../../knowledge/simple_tables/simple_table.md)
- [Part 5.2 — Streamlit Integration II](../dashboards/streamlit/streamlit_integration_2.md)
