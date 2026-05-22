from __future__ import annotations

from typing import Any

from pydantic import BaseModel

import mainsequence.tdag.data_nodes.build_operations as build_operations

from .base import _drop_excluded_keys
from .constants import PORTFOLIO_CONFIGURATION_HASH_EXCLUDED_KEYS


def canonical_portfolio_configuration(
    portfolio_configuration: Any,
) -> dict[str, Any]:
    """Return the canonical hash payload for a VFB portfolio configuration."""
    payload = _portfolio_configuration_payload(portfolio_configuration)
    serialized_payload = build_operations.Serializer().serialize_init_kwargs(payload)
    return _drop_excluded_keys(
        dict(serialized_payload),
        excluded_keys=PORTFOLIO_CONFIGURATION_HASH_EXCLUDED_KEYS,
    )


def compute_portfolio_configuration_hash(portfolio_configuration: Any) -> str:
    """Compute the deterministic identity hash for a VFB portfolio config."""
    payload = canonical_portfolio_configuration(portfolio_configuration)
    _update_hash, storage_hash = build_operations.hash_signature(payload)
    return storage_hash


def get_or_create_portfolio_index_asset(
    portfolio_configuration: Any,
    *,
    portfolio_configuration_hash: str | None = None,
    portfolio_resolver: Any | None = None,
    timeout: int | float | tuple[float, float] | None = None,
) -> tuple[Any, Any]:
    """Resolve the backend Portfolio and PortfolioIndexAsset for a config hash."""
    resolved_hash = portfolio_configuration_hash or compute_portfolio_configuration_hash(
        portfolio_configuration
    )
    resolver = portfolio_resolver
    if resolver is None:
        import mainsequence.client as msc

        resolver = msc.Portfolio

    get_or_create = getattr(resolver, "get_or_create_from_configuration_hash", None)
    if not callable(get_or_create):
        raise AttributeError(
            "Portfolio resolver must expose " "get_or_create_from_configuration_hash(...)."
        )

    return _coerce_portfolio_resolution_result(
        get_or_create(
            portfolio_configuration_hash=resolved_hash,
            portfolio_configuration=canonical_portfolio_configuration(portfolio_configuration),
            timeout=timeout,
        )
    )


def _portfolio_configuration_payload(portfolio_configuration: Any) -> dict[str, Any]:
    build_configuration = getattr(portfolio_configuration, "build_configuration", None)
    if callable(build_configuration):
        build_configuration = build_configuration()
    if isinstance(build_configuration, dict):
        extracted = _extract_portfolio_configuration_from_mapping(build_configuration)
        return {"portfolio_configuration": extracted}

    nested_portfolio_configuration = getattr(
        portfolio_configuration,
        "portfolio_configuration",
        None,
    )
    if nested_portfolio_configuration is not None:
        return {"portfolio_configuration": nested_portfolio_configuration}

    if isinstance(portfolio_configuration, dict):
        extracted = _extract_portfolio_configuration_from_mapping(portfolio_configuration)
        return {"portfolio_configuration": extracted}

    if isinstance(portfolio_configuration, BaseModel):
        return {"portfolio_configuration": portfolio_configuration}

    raise TypeError(
        "portfolio_configuration must be a PortfolioConfiguration-like Pydantic "
        "model, a PortfolioStrategyConfig-like object, a DataNode-like object "
        "with build_configuration, or a dict payload."
    )


def _extract_portfolio_configuration_from_mapping(
    value: dict[str, Any],
) -> Any:
    if "portfolio_configuration" in value:
        return value["portfolio_configuration"]

    strategy_config = value.get("portfolio_strategy_config")
    if isinstance(strategy_config, dict):
        if "portfolio_configuration" in strategy_config:
            return strategy_config["portfolio_configuration"]
        serialized_model = strategy_config.get("serialized_model")
        if isinstance(serialized_model, dict) and "portfolio_configuration" in serialized_model:
            return serialized_model["portfolio_configuration"]
    if getattr(strategy_config, "portfolio_configuration", None) is not None:
        return strategy_config.portfolio_configuration

    return value


def _coerce_portfolio_resolution_result(result: Any) -> tuple[Any, Any]:
    if isinstance(result, tuple) and len(result) == 2:
        return result
    if isinstance(result, dict):
        portfolio = result.get("portfolio")
        index_asset = (
            result.get("portfolio_index_asset") or result.get("index_asset") or result.get("asset")
        )
        if portfolio is not None and index_asset is not None:
            return portfolio, index_asset
    portfolio = getattr(result, "portfolio", None)
    index_asset = (
        getattr(result, "portfolio_index_asset", None)
        or getattr(result, "index_asset", None)
        or getattr(result, "asset", None)
    )
    if portfolio is not None and index_asset is not None:
        return portfolio, index_asset

    raise TypeError(
        "Portfolio configuration resolver must return "
        "(portfolio, portfolio_index_asset) or an object/dict with those fields."
    )
