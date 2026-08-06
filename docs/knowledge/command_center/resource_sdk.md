# Command Center Resource SDK For FastAPI

Use `mainsequence.client.command_center.sdk.resource` when a Python API must expose the same
collection, action, pagination, and summary contracts consumed by the standalone Command Center
SDK.

Django REST Framework remains the canonical platform contract. These Pydantic models project that
wire format for APIs authored with FastAPI; they do not replace backend authorization, filtering,
or domain services.

Install the optional FastAPI integration when using the route helpers:

```bash
pip install "mainsequence[fastapi]"
```

## Canonical Collection

Every new SDK-backed collection returns:

```json
{
  "count": 125,
  "next": null,
  "previous": null,
  "results": [],
  "controls": {
    "search": null,
    "filters": [],
    "ordering": []
  },
  "actions": []
}
```

Use `CanonicalResourceCollection[YourRowModel]` as the FastAPI response model. The application must
apply authorization, filters, and stable ordering before counting and slicing the dataset.

```python
from typing import Annotated

from fastapi import APIRouter, Depends, Request

from mainsequence.client.command_center.sdk.data_models import ContractBaseModel
from mainsequence.client.command_center.sdk.fastapi import (
    build_fastapi_resource_collection,
    resource_limit_offset,
)
from mainsequence.client.command_center.sdk.resource import (
    CanonicalResourceCollection,
    ResourceCollectionControls,
    ResourceLimitOffset,
)


class ProjectRow(ContractBaseModel):
    uid: str
    name: str


router = APIRouter(prefix="/projects")
controls = ResourceCollectionControls(
    search={
        "placeholder": "Search by project name or UID",
        "fields": ["name", "uid"],
    },
    filters=[],
    ordering=["name"],
)


@router.get("/", response_model=CanonicalResourceCollection[ProjectRow])
def list_projects(
    request: Request,
    pagination: Annotated[ResourceLimitOffset, Depends(resource_limit_offset)],
) -> CanonicalResourceCollection[ProjectRow]:
    filtered_projects = load_authorized_filtered_projects()
    count = filtered_projects.count()
    results = filtered_projects.order_by("name", "uid")[
        pagination.offset : pagination.offset + pagination.limit
    ]
    return build_fastapi_resource_collection(
        request=request,
        results=results,
        count=count,
        pagination=pagination,
        controls=controls,
        actions=actions_available_to_current_user(),
    )
```

`controls.filters` contains only controls the generic SDK should render. Hidden host scope remains
part of the actual list query and must also be preserved for all-matching actions.

Do not set `response_model_exclude_none=True` on collection routes. `next` and `previous` are
required envelope keys and must remain present when their values are `null`.

## Discovered Selection Actions

Selection-based collection actions use:

- `GET <collection>/bulk-actions/` returning `ResourceBulkActionDiscoveryResponse`;
- the strict `ResourceBulkActionRequest[OptionsModel]` execution body; and
- the same body for optional preflight.

```python
from mainsequence.client.command_center.sdk.resource import (
    ResourceBulkActionDiscoveryResponse,
    ResourceBulkActionPreflightResponse,
    ResourceBulkActionRequest,
)


class DeleteOptions(ContractBaseModel):
    delete_from_provider: bool = False


DeleteRequest = ResourceBulkActionRequest[DeleteOptions]


@router.get("/bulk-actions/", response_model=ResourceBulkActionDiscoveryResponse)
def discover_project_actions() -> ResourceBulkActionDiscoveryResponse:
    return ResourceBulkActionDiscoveryResponse(
        actions=actions_available_to_current_user()
    )


@router.post(
    "/bulk-delete/preflight/",
    response_model=ResourceBulkActionPreflightResponse,
)
def preflight_project_deletion(
    payload: DeleteRequest,
) -> ResourceBulkActionPreflightResponse:
    targets = reauthorize_and_resolve_targets(payload.selection)
    return ResourceBulkActionPreflightResponse(
        allowed=True,
        warnings=build_project_deletion_warnings(targets, payload.options),
        matched_count=len(targets),
    )


@router.post("/bulk-delete/")
def bulk_delete_projects(payload: DeleteRequest):
    targets = reauthorize_and_resolve_targets(payload.selection)
    return execute_project_deletion(targets, payload.options)
```

`explicit` selection accepts public UUIDs. `all_matching` contains semantic `search` and `filters`
only; pagination and ordering are rejected. Execution must reauthorize and resolve every target.
Legacy fields such as `selected_uids`, `select_all`, and `current_url` are not accepted.

Successful action responses remain action-specific. Do not wrap every action result in a new
universal response model.

## Summary

Use `EntitySummary` for `GET <collection>/{uid}/summary/`. It mirrors the canonical DRF summary
shape, including typed helpers and optional `label_management`. Legacy top-level `labels` and
`labelable` fields are rejected.

When optional summary fields are absent, declare `response_model_exclude_none=True` on the FastAPI
route so the wire response matches DRF omission semantics.

## Contract Boundary

The SDK supplies validation, serialization, page-link generation, and OpenAPI response models. The
application still owns:

- authentication and authorization;
- the complete filtered queryset;
- stable ordering;
- authoritative filtered counts;
- exact all-matching filter reuse;
- preflight and execution-time reauthorization; and
- action-specific success and error responses.

The discovered-action contract currently covers selection-based collection actions only. Ordinary
row, detail, global, and multi-step workflow actions need their own explicit contracts.
