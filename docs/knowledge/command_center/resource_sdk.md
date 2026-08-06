# Command Center Resource SDK For FastAPI

Use `mainsequence.command_center.sdk.resource` when a Python API must expose the same
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

from fastapi import APIRouter, Depends, Request, Response

from mainsequence.command_center.sdk.data_models import ContractBaseModel
from mainsequence.command_center.sdk.fastapi import (
    build_fastapi_resource_collection,
    resource_limit_offset,
)
from mainsequence.command_center.sdk.resource import (
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
    response: Response,
    pagination: Annotated[ResourceLimitOffset, Depends(resource_limit_offset)],
) -> CanonicalResourceCollection[ProjectRow]:
    filtered_projects = load_authorized_filtered_projects()
    count = filtered_projects.count()
    results = filtered_projects.order_by("name", "uid")[
        pagination.offset : pagination.offset + pagination.limit
    ]
    return build_fastapi_resource_collection(
        request=request,
        response=response,
        results=results,
        count=count,
        pagination=pagination,
        controls=controls,
        actions=actions_available_to_current_user(),
    )
```

`controls.filters` contains only controls the generic SDK should render. Hidden host scope remains
part of the actual list query and must also be preserved for all-matching actions.

`build_fastapi_resource_collection` applies `Cache-Control: private, no-store` and merges
`Authorization, Cookie` into `Vary`. Collection action availability is caller-contextual and must
not be stored in a shared cache.

Do not set `response_model_exclude_none=True` on collection routes. `next` and `previous` are
required envelope keys and must remain present when their values are `null`. Optional fields inside
action definitions are omitted by the action model itself, including an absent `preflight_endpoint`.

## Discovered Selection Actions

Selection-based collection actions use:

- `GET <collection>/bulk-actions/` returning `ResourceBulkActionDiscoveryResponse`;
- the strict `ResourceBulkActionRequest[OptionsModel]` execution body; and
- the same body for optional preflight.

```python
from typing import Literal
from uuid import UUID

from fastapi import Response

from mainsequence.command_center.sdk.data_models import ContractBaseModel
from mainsequence.command_center.sdk.resource import (
    ResourceBulkActionDiscoveryResponse,
    ResourceBulkActionEmptyOptions,
    ResourceBulkActionPreflightResponse,
    ResourceBulkActionRequest,
    validate_resource_bulk_action_filters,
)
from mainsequence.command_center.sdk.fastapi import (
    build_fastapi_bulk_action_discovery,
    build_fastapi_bulk_action_preflight_response,
)


class DeleteOptions(ContractBaseModel):
    delete_from_provider: bool = False


class ProjectListFilters(ContractBaseModel):
    project_uid: UUID | None = None
    kind: Literal["service", "job"] | None = None


DeleteRequest = ResourceBulkActionRequest[DeleteOptions]


@router.get("/bulk-actions/", response_model=ResourceBulkActionDiscoveryResponse)
def discover_project_actions(response: Response) -> ResourceBulkActionDiscoveryResponse:
    return build_fastapi_bulk_action_discovery(
        response=response,
        actions=actions_available_to_current_user()
    )


@router.post(
    "/bulk-delete/preflight/",
    response_model=ResourceBulkActionPreflightResponse,
    responses={409: {"model": ResourceBulkActionPreflightResponse}},
)
def preflight_project_deletion(
    payload: DeleteRequest,
) -> Response:
    if payload.selection.mode == "all_matching":
        filters = validate_resource_bulk_action_filters(
            payload.selection.query,
            ProjectListFilters,
        )
    else:
        filters = None
    targets = reauthorize_and_resolve_targets(payload.selection, filters=filters)
    preflight = ResourceBulkActionPreflightResponse(
        allowed=deletion_is_allowed(targets, payload.options),
        blockers=build_project_deletion_blockers(targets, payload.options),
        warnings=build_project_deletion_warnings(targets, payload.options),
        matched_count=len(targets),
    )
    return build_fastapi_bulk_action_preflight_response(preflight)


@router.post("/bulk-delete/")
def bulk_delete_projects(payload: DeleteRequest):
    if payload.selection.mode == "all_matching":
        filters = validate_resource_bulk_action_filters(
            payload.selection.query,
            ProjectListFilters,
        )
    else:
        filters = None
    targets = reauthorize_and_resolve_targets(payload.selection, filters=filters)
    return execute_project_deletion(targets, payload.options)
```

Use `ResourceBulkActionEmptyOptions` instead of an untyped dictionary when an action has no user
options:

```python
DeleteRequest = ResourceBulkActionRequest[ResourceBulkActionEmptyOptions]
```

`explicit` selection accepts public UUIDs. `all_matching` contains semantic `search` and `filters`
only; pagination and ordering are rejected. Pass its filters through the same strict filter model
and application filtering function used by the list route. This rejects undeclared filters and
keeps hidden host scope aligned. Execution must reauthorize and resolve every target. Legacy fields
such as `selected_uids`, `select_all`, and `current_url` are not accepted.

The reusable preflight response supports required `allowed` and `matched_count`, optional `detail`
and string `blockers`, and string `warnings`. The FastAPI helper returns HTTP 200 when allowed and
HTTP 409 when blocked, matching the canonical destructive-action preflight behavior. Declare the
409 response model on the route so OpenAPI documents both outcomes. A domain with structured
per-item impacts should define its own strict response model instead of flattening or discarding
those action-specific fields; it must still receive the exact same selection/options request as
execution.

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
