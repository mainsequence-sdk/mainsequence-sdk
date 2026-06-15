from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from ..contracts.adapter_from_api import (
    AdapterFromApiConfigVariable,
    AdapterFromApiConnectionContract,
    AdapterFromApiOperation,
    AdapterFromApiOperationCache,
    AdapterFromApiParameter,
    AdapterFromApiRequestBody,
    AdapterFromApiSecretVariable,
    make_adapter_from_api_contract,
)
from ..contracts.response_mapping import AdapterResponseMapping


def make_health_operation(
    *,
    operation_id: str = "health",
    path: str = "/health",
    label: str = "Health",
    description: str = "Provider health check.",
) -> AdapterFromApiOperation:
    """Build a zero-argument health operation for the provider contract."""

    return AdapterFromApiOperation(
        operationId=operation_id,
        label=label,
        description=description,
        method="GET",
        path=path,
        kind="health",
        capabilities=[],
        requiresTimeRange=False,
        supportsVariables=False,
        supportsMaxRows=False,
        parameters=[],
        cache=AdapterFromApiOperationCache(enabled=False, dedupe=False),
    )


def make_query_operation(
    *,
    operation_id: str,
    label: str,
    path: str,
    method: str = "GET",
    description: str = "",
    capabilities: Sequence[str] | None = None,
    parameters: Sequence[AdapterFromApiParameter | Mapping[str, Any]] | None = None,
    request_body: AdapterFromApiRequestBody | Mapping[str, Any] | None = None,
    response_mappings: Sequence[AdapterResponseMapping | Mapping[str, Any]] | None = None,
    requires_time_range: bool = False,
    supports_variables: bool = True,
    supports_max_rows: bool = True,
    cache: AdapterFromApiOperationCache | Mapping[str, Any] | None = None,
) -> AdapterFromApiOperation:
    """Build a query-capable operation allowlisted for Adapter from API."""

    normalized_parameters = [
        parameter
        if isinstance(parameter, AdapterFromApiParameter)
        else AdapterFromApiParameter(**dict(parameter))
        for parameter in (parameters or [])
    ]
    normalized_response_mappings = [
        mapping if isinstance(mapping, AdapterResponseMapping) else AdapterResponseMapping(**dict(mapping))
        for mapping in (response_mappings or [])
    ]
    normalized_request_body = None
    if request_body is not None:
        normalized_request_body = (
            request_body
            if isinstance(request_body, AdapterFromApiRequestBody)
            else AdapterFromApiRequestBody(**dict(request_body))
        )
    normalized_cache = None
    if cache is not None:
        normalized_cache = (
            cache if isinstance(cache, AdapterFromApiOperationCache) else AdapterFromApiOperationCache(**dict(cache))
        )

    return AdapterFromApiOperation(
        operationId=operation_id,
        label=label,
        description=description,
        method=method,
        path=path,
        kind="query",
        capabilities=list(capabilities or ["query"]),
        requiresTimeRange=requires_time_range,
        supportsVariables=supports_variables,
        supportsMaxRows=supports_max_rows,
        parameters=normalized_parameters,
        requestBody=normalized_request_body,
        responseMappings=normalized_response_mappings,
        cache=normalized_cache or AdapterFromApiOperationCache(),
    )


def make_provider_contract(
    *,
    adapter_id: str,
    title: str,
    openapi_url: str,
    query_operations: Sequence[AdapterFromApiOperation],
    health_operation: AdapterFromApiOperation | None = None,
    description: str | None = None,
    config_variables: Sequence[AdapterFromApiConfigVariable] | None = None,
    secret_variables: Sequence[AdapterFromApiSecretVariable] | None = None,
) -> AdapterFromApiConnectionContract:
    """Build a provider contract with a declared health operation."""

    health = health_operation or make_health_operation()
    operations = [health, *query_operations]
    return make_adapter_from_api_contract(
        adapter_id=adapter_id,
        title=title,
        description=description,
        openapi_url=openapi_url,
        operations=operations,
        health_operation_id=health.operationId,
        config_variables=list(config_variables or []),
        secret_variables=list(secret_variables or []),
    )


__all__ = [
    "make_adapter_from_api_contract",
    "make_health_operation",
    "make_provider_contract",
    "make_query_operation",
]
