from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ..connections import CONNECTION_TYPE_ADAPTER_FROM_API
from ..contracts.tabular import CORE_TABULAR_FRAME_CONTRACT


def make_connection_query_payload(
    *,
    connection_uid: str,
    operation_id: str,
    query: Mapping[str, Any] | None = None,
    query_model_id: str | None = None,
    connection_type_id: str = CONNECTION_TYPE_ADAPTER_FROM_API,
    output_contract: str = CORE_TABULAR_FRAME_CONTRACT,
) -> dict[str, Any]:
    """Build a generic connection-query payload for a Command Center source widget."""

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


__all__ = ["make_connection_query_payload"]
