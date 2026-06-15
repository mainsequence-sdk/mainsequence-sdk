from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ..connections import CONNECTION_TYPE_ADAPTER_FROM_API
from ..contracts.tabular import CORE_TABULAR_FRAME_CONTRACT


def make_connection_ref(
    *,
    connection_id: str | int,
    connection_type_id: str = CONNECTION_TYPE_ADAPTER_FROM_API,
) -> dict[str, Any]:
    """Build the stable connection reference used by connection-query widgets."""

    return {
        "id": connection_id,
        "typeId": connection_type_id,
    }


def make_api_operation_parameters(
    *,
    path: Mapping[str, Any] | None = None,
    query: Mapping[str, Any] | None = None,
    headers: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the parameter block for an Adapter from API operation query."""

    return {
        "path": dict(path or {}),
        "query": dict(query or {}),
        "headers": dict(headers or {}),
    }


def make_api_operation_query(
    *,
    operation_id: str,
    query: Mapping[str, Any] | None = None,
    path: Mapping[str, Any] | None = None,
    headers: Mapping[str, Any] | None = None,
    body: Any = None,
) -> dict[str, Any]:
    """Build a serializable api-operation query payload."""

    return {
        "kind": "api-operation",
        "operationId": operation_id,
        "parameters": make_api_operation_parameters(
            path=path,
            query=query,
            headers=headers,
        ),
        "body": body,
    }


def make_connection_query_props(
    *,
    connection_id: str | int,
    operation_id: str,
    query: Mapping[str, Any] | None = None,
    connection_type_id: str = CONNECTION_TYPE_ADAPTER_FROM_API,
    query_model_id: str = "api-operation",
    time_range_mode: str = "none",
    max_rows: int | None = None,
    body: Any = None,
) -> dict[str, Any]:
    """Build generic props for a connection-query source widget."""

    props: dict[str, Any] = {
        "connectionRef": make_connection_ref(
            connection_id=connection_id,
            connection_type_id=connection_type_id,
        ),
        "queryModelId": query_model_id,
        "query": make_api_operation_query(
            operation_id=operation_id,
            query=query,
            body=body,
        ),
        "timeRangeMode": time_range_mode,
    }
    if max_rows is not None:
        props["maxRows"] = max_rows
    return props


def make_connection_query_payload(
    *,
    connection_uid: str,
    operation_id: str,
    query: Mapping[str, Any] | None = None,
    query_model_id: str | None = None,
    connection_type_id: str = CONNECTION_TYPE_ADAPTER_FROM_API,
    output_contract: str = CORE_TABULAR_FRAME_CONTRACT,
) -> dict[str, Any]:
    """Build a compact connection-query descriptor for agents and tools."""

    payload: dict[str, Any] = {
        "connectionUid": connection_uid,
        "connectionTypeId": connection_type_id,
        "operationId": operation_id,
        "query": dict(query or {}),
        "outputContract": output_contract,
    }
    if query_model_id is not None:
        payload["queryModelId"] = query_model_id
    return payload


__all__ = [
    "make_api_operation_parameters",
    "make_api_operation_query",
    "make_connection_query_payload",
    "make_connection_query_props",
    "make_connection_ref",
]
