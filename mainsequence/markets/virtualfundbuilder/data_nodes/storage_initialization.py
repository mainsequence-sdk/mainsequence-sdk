from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from mainsequence.client.models_tdag import DataNodeStorage

from .base import VFBCanonicalDataNode, _storage_source_config


def initialize_portfolio_storage_source_tables(
    *,
    portfolio_weights: VFBCanonicalDataNode | None = None,
    signal_weights: VFBCanonicalDataNode | None = None,
    portfolio_data: VFBCanonicalDataNode | None = None,
    anchor_node: VFBCanonicalDataNode | None = None,
    timeout: int | None = None,
) -> dict[str, Any]:
    """Initialize the canonical VFB storage family through one backend call."""
    family = _resolve_storage_family(
        portfolio_weights=portfolio_weights,
        signal_weights=signal_weights,
        portfolio_data=portfolio_data,
        anchor_node=anchor_node,
    )
    storages = {
        payload_key: _ensure_storage_metadata(node, timeout=timeout)
        for payload_key, node in family.items()
    }
    payload = {
        payload_key: _source_table_payload(
            storage=storages[payload_key],
            node=family[payload_key],
        )
        for payload_key in family
    }

    result = DataNodeStorage.initialize_portfolio_storage_source_tables(
        portfolio_weights=payload["portfolio_weights"],
        signal_weights=payload["signal_weights"],
        portfolio_data=payload["portfolio_data"],
        timeout=timeout,
    )

    for payload_key, node in family.items():
        storage = storages[payload_key]
        source_config = _source_config_from_response(result, payload_key)
        if source_config is not None:
            storage.sourcetableconfiguration = source_config

        source_config = _storage_source_config(storage)
        if source_config is None:
            refreshed_storage = _refresh_storage(storage, timeout=timeout)
            if refreshed_storage is not None:
                _set_node_storage(node, refreshed_storage)
                storage = refreshed_storage
                source_config = _storage_source_config(storage)

        if source_config is None:
            raise RuntimeError(
                f"Portfolio storage initializer did not return or expose a "
                f"source-table configuration for {payload_key}."
            )
        node._validate_storage_contract(source_config)

    return result


def _resolve_storage_family(
    *,
    portfolio_weights: VFBCanonicalDataNode | None,
    signal_weights: VFBCanonicalDataNode | None,
    portfolio_data: VFBCanonicalDataNode | None,
    anchor_node: VFBCanonicalDataNode | None,
) -> dict[str, VFBCanonicalDataNode]:
    from .portfolio_weights import PortfolioWeights
    from .portfolios import PortfoliosDataNode
    from .signal_weights import SignalWeights

    namespace = _node_namespace(
        anchor_node or portfolio_weights or signal_weights or portfolio_data
    )
    if anchor_node is not None:
        if isinstance(anchor_node, PortfolioWeights):
            portfolio_weights = portfolio_weights or anchor_node
        elif isinstance(anchor_node, SignalWeights):
            signal_weights = signal_weights or anchor_node
        elif isinstance(anchor_node, PortfoliosDataNode):
            portfolio_data = portfolio_data or anchor_node
        else:
            raise TypeError(
                "anchor_node must be PortfolioWeights, SignalWeights, or "
                "PortfoliosDataNode."
            )

    return {
        "portfolio_weights": portfolio_weights or PortfolioWeights(namespace=namespace),
        "signal_weights": signal_weights or SignalWeights(namespace=namespace),
        "portfolio_data": portfolio_data or PortfoliosDataNode(namespace=namespace),
    }


def _node_namespace(node: VFBCanonicalDataNode | None) -> str | None:
    if node is None:
        return None
    namespace = getattr(node, "hash_namespace", "") or ""
    return namespace or None


def _ensure_storage_metadata(
    node: VFBCanonicalDataNode,
    *,
    timeout: int | None,
) -> Any:
    storage = getattr(node, "data_node_storage", None)
    if _coerce_optional_uid(storage) is None:
        node.verify_and_build_remote_objects()
        storage = getattr(node, "data_node_storage", None)
    if _coerce_optional_uid(storage) is None:
        raise RuntimeError(
            f"{node.__class__.__name__} must have a DataNodeStorage uid before "
            "the portfolio storage source tables can be initialized."
        )
    return storage


def _source_table_payload(
    *,
    storage: Any,
    node: VFBCanonicalDataNode,
) -> dict[str, Any]:
    config = node._canonical_config()
    return {
        "dynamic_table_metadata_uid": _coerce_required_uid(storage),
        "time_index_name": config.time_index_name,
        "index_names": config.index_names,
        "column_dtypes_map": config.column_dtypes_map,
    }


def _source_config_from_response(
    response: dict[str, Any] | None,
    payload_key: str,
) -> Any | None:
    if not isinstance(response, dict):
        return None

    candidates = []
    payload_response = response.get(payload_key)
    if isinstance(payload_response, dict):
        candidates.extend(
            [
                payload_response.get("source_table_configuration"),
                payload_response.get("sourcetableconfiguration"),
                payload_response.get("source_table_config"),
            ]
        )
    for container_key in (
        "source_table_configurations",
        "sourcetableconfigurations",
        "source_table_configs",
    ):
        container = response.get(container_key)
        if isinstance(container, dict):
            candidates.append(container.get(payload_key))

    for candidate in candidates:
        if candidate is not None:
            return _as_source_config(candidate)
    return None


def _as_source_config(value: Any) -> Any:
    if isinstance(value, dict):
        return SimpleNamespace(**value)
    return value


def _refresh_storage(storage: Any, *, timeout: int | None) -> Any | None:
    storage_uid = _coerce_optional_uid(storage)
    if storage_uid is None:
        return None
    return DataNodeStorage.get(
        uid=storage_uid,
        include_relations_detail=True,
        timeout=timeout,
    )


def _set_node_storage(node: VFBCanonicalDataNode, storage: Any) -> None:
    try:
        node.local_persist_manager.data_node_storage = storage
    except Exception:
        pass


def _coerce_required_uid(value: Any) -> str:
    uid = _coerce_optional_uid(value)
    if uid is None:
        raise ValueError("DataNodeStorage must expose a public uid.")
    return uid


def _coerce_optional_uid(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value or None
    if isinstance(value, dict):
        uid = value.get("uid")
    else:
        uid = getattr(value, "uid", None)
    if uid in (None, ""):
        return None
    return str(uid)
