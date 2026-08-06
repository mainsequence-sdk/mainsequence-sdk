"""Optional FastAPI bindings for Command Center SDK wire contracts."""

from .actions import (
    apply_resource_discovery_headers,
    build_fastapi_bulk_action_discovery,
    build_fastapi_bulk_action_preflight_response,
)
from .resource import build_fastapi_resource_collection, resource_limit_offset

__all__ = [
    "apply_resource_discovery_headers",
    "build_fastapi_bulk_action_discovery",
    "build_fastapi_bulk_action_preflight_response",
    "build_fastapi_resource_collection",
    "resource_limit_offset",
]
