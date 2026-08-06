from __future__ import annotations

from collections.abc import Sequence
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from pydantic import Field

from ..data_models import ContractBaseModel
from .actions import ResourceBulkActionDefinition
from .controls import ResourceCollectionControls


class ResourceLimitOffset(ContractBaseModel):
    limit: int = Field(default=100, ge=1, le=5000)
    offset: int = Field(default=0, ge=0)


class CanonicalResourceCollection[ItemT](ContractBaseModel):
    count: int = Field(ge=0)
    next: str | None
    previous: str | None
    results: list[ItemT]
    controls: ResourceCollectionControls
    actions: list[ResourceBulkActionDefinition]


def _replace_page_query(
    request_url: str,
    *,
    limit: int,
    offset: int,
) -> str:
    parsed = urlsplit(request_url)
    query = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if key not in {"limit", "offset"}
    ]
    query.append(("limit", str(limit)))
    if offset:
        query.append(("offset", str(offset)))
    return urlunsplit(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            urlencode(query, doseq=True),
            parsed.fragment,
        )
    )


def build_canonical_resource_collection[ItemT](
    *,
    request_url: str,
    results: Sequence[ItemT],
    count: int,
    pagination: ResourceLimitOffset,
    controls: ResourceCollectionControls,
    actions: Sequence[ResourceBulkActionDefinition] = (),
) -> CanonicalResourceCollection[ItemT]:
    """Build the canonical Command Center collection envelope.

    The caller must filter, authorize, count, and stably order the complete
    collection before passing the current page results to this helper.
    """

    next_url = None
    if pagination.offset + pagination.limit < count:
        next_url = _replace_page_query(
            request_url,
            limit=pagination.limit,
            offset=pagination.offset + pagination.limit,
        )

    previous_url = None
    if pagination.offset > 0:
        previous_url = _replace_page_query(
            request_url,
            limit=pagination.limit,
            offset=max(pagination.offset - pagination.limit, 0),
        )

    return CanonicalResourceCollection[ItemT](
        count=count,
        next=next_url,
        previous=previous_url,
        results=list(results),
        controls=controls,
        actions=list(actions),
    )


__all__ = [
    "CanonicalResourceCollection",
    "ResourceLimitOffset",
    "build_canonical_resource_collection",
]
