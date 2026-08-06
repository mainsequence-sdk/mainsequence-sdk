from __future__ import annotations

from collections.abc import Sequence
from typing import Annotated

from fastapi import Query, Request

from ..resource import (
    CanonicalResourceCollection,
    ResourceBulkActionDefinition,
    ResourceCollectionControls,
    ResourceLimitOffset,
    build_canonical_resource_collection,
)


def resource_limit_offset(
    limit: Annotated[int, Query(ge=1, le=5000)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> ResourceLimitOffset:
    """FastAPI dependency matching the canonical DRF limit/offset defaults."""

    return ResourceLimitOffset(limit=limit, offset=offset)


def build_fastapi_resource_collection[ItemT](
    *,
    request: Request,
    results: Sequence[ItemT],
    count: int,
    pagination: ResourceLimitOffset,
    controls: ResourceCollectionControls,
    actions: Sequence[ResourceBulkActionDefinition] = (),
) -> CanonicalResourceCollection[ItemT]:
    """Build a canonical collection using the active FastAPI request URL."""

    return build_canonical_resource_collection(
        request_url=str(request.url),
        results=results,
        count=count,
        pagination=pagination,
        controls=controls,
        actions=actions,
    )


__all__ = [
    "build_fastapi_resource_collection",
    "resource_limit_offset",
]
