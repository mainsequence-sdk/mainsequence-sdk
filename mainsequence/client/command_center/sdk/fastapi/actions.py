from __future__ import annotations

from collections.abc import Sequence

from fastapi import Response, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from ..resource import (
    ResourceBulkActionDefinition,
    ResourceBulkActionDiscoveryResponse,
)


def apply_resource_discovery_headers(response: Response) -> None:
    """Apply the private caller-context cache policy used by canonical DRF."""

    response.headers["Cache-Control"] = "private, no-store"

    vary_values = [
        value.strip()
        for value in response.headers.get("Vary", "").split(",")
        if value.strip()
    ]
    if "*" in vary_values:
        response.headers["Vary"] = "*"
        return

    existing = {value.casefold() for value in vary_values}
    for value in ("Authorization", "Cookie"):
        if value.casefold() not in existing:
            vary_values.append(value)
            existing.add(value.casefold())
    response.headers["Vary"] = ", ".join(vary_values)


def build_fastapi_bulk_action_discovery(
    *,
    response: Response,
    actions: Sequence[ResourceBulkActionDefinition],
) -> ResourceBulkActionDiscoveryResponse:
    """Build caller-contextual discovery and apply its canonical cache policy."""

    apply_resource_discovery_headers(response)
    return ResourceBulkActionDiscoveryResponse(actions=list(actions))


def build_fastapi_bulk_action_preflight_response(
    preflight: BaseModel,
) -> JSONResponse:
    """Return a typed preflight payload, using 409 when execution is blocked."""

    allowed = getattr(preflight, "allowed", None)
    if not isinstance(allowed, bool):
        raise TypeError("Bulk-action preflight models must expose a boolean 'allowed' field.")

    return JSONResponse(
        content=preflight.model_dump(mode="json", exclude_none=True),
        status_code=(
            status.HTTP_200_OK
            if allowed
            else status.HTTP_409_CONFLICT
        ),
    )


__all__ = [
    "apply_resource_discovery_headers",
    "build_fastapi_bulk_action_discovery",
    "build_fastapi_bulk_action_preflight_response",
]
