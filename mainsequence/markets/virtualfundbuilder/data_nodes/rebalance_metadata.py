from __future__ import annotations

from typing import Annotated, Any

import mainsequence.tdag.data_nodes.build_operations as build_operations
from mainsequence.tdag.simple_tables import (
    Index,
    SimpleTable,
    SimpleTableUpdater,
    SimpleTableUpdaterConfiguration,
)

from .base import _class_import_path, _drop_excluded_keys
from .constants import (
    REBALANCE_STRATEGY_DESCRIPTION,
    REBALANCE_STRATEGY_UID_EXCLUDED_CONFIGURATION_KEYS,
)


class RebalanceStrategyMetadata(SimpleTable):
    """Metadata registry row for a canonical VFB rebalance strategy."""

    rebalance_strategy_uid: Annotated[str, Index(unique=True)]
    rebalance_strategy_description: str | None = None


class RebalanceStrategiesMetadataConfiguration(SimpleTableUpdaterConfiguration):
    """Configuration for the canonical VFB rebalance strategy registry."""

    identifier: str = "mainsequence.markets.rebalance_strategies"


class RebalanceStrategiesMetadataUpdater(SimpleTableUpdater):
    """SimpleTable updater for canonical VFB rebalance strategy metadata."""

    SIMPLE_TABLE_SCHEMA = RebalanceStrategyMetadata

    def update(self) -> list[RebalanceStrategyMetadata]:
        return []


def default_rebalance_strategy_metadata_updater() -> RebalanceStrategiesMetadataUpdater:
    return RebalanceStrategiesMetadataUpdater(
        configuration=RebalanceStrategiesMetadataConfiguration()
    )


def canonical_rebalance_strategy_configuration(
    rebalance_strategy: Any,
) -> dict[str, Any]:
    """Return the canonical hash payload for a VFB rebalance strategy."""
    payload = _rebalance_strategy_payload(rebalance_strategy)
    serialized_payload = build_operations.Serializer().serialize_init_kwargs(payload)
    return _drop_excluded_keys(
        dict(serialized_payload),
        excluded_keys=REBALANCE_STRATEGY_UID_EXCLUDED_CONFIGURATION_KEYS,
    )


def compute_rebalance_strategy_uid(rebalance_strategy: Any) -> str:
    """Compute the deterministic VFB rebalance_strategy_uid."""
    payload = canonical_rebalance_strategy_configuration(rebalance_strategy)
    _update_hash, storage_hash = build_operations.hash_signature(payload)
    return storage_hash


def build_rebalance_strategy_metadata(
    rebalance_strategy: Any | None = None,
    *,
    rebalance_strategy_uid: str | None = None,
    rebalance_strategy_description: str | None = None,
) -> RebalanceStrategyMetadata:
    if rebalance_strategy_uid is None and rebalance_strategy is not None:
        resolved_strategy_uid = compute_rebalance_strategy_uid(rebalance_strategy)
    else:
        resolved_strategy_uid = rebalance_strategy_uid
    if not resolved_strategy_uid:
        raise ValueError(
            "rebalance_strategy_uid or rebalance_strategy is required to build "
            "RebalanceStrategyMetadata."
        )

    resolved_description = rebalance_strategy_description
    if resolved_description is None:
        resolved_description = _extract_rebalance_strategy_description(rebalance_strategy)

    return RebalanceStrategyMetadata(
        rebalance_strategy_uid=str(resolved_strategy_uid),
        rebalance_strategy_description=(
            None if resolved_description is None else str(resolved_description)
        ),
    )


def upsert_rebalance_strategy_metadata(
    rebalance_strategy: Any | None = None,
    *,
    rebalance_strategy_uid: str | None = None,
    rebalance_strategy_description: str | None = None,
    updater: Any | None = None,
) -> RebalanceStrategyMetadata:
    metadata = build_rebalance_strategy_metadata(
        rebalance_strategy,
        rebalance_strategy_uid=rebalance_strategy_uid,
        rebalance_strategy_description=rebalance_strategy_description,
    )
    resolved_updater = updater or default_rebalance_strategy_metadata_updater()
    return resolved_updater.upsert(metadata)


def get_rebalance_strategy_metadata(
    rebalance_strategy_uid: str,
    *,
    updater: Any | None = None,
) -> RebalanceStrategyMetadata | None:
    resolved_updater = updater or default_rebalance_strategy_metadata_updater()
    rows = resolved_updater.execute_filter(
        RebalanceStrategyMetadata.filters.rebalance_strategy_uid.eq(
            rebalance_strategy_uid
        ),
        limit=1,
    )
    return rows[0] if rows else None


def _rebalance_strategy_payload(rebalance_strategy: Any) -> dict[str, Any]:
    if isinstance(rebalance_strategy, dict):
        return rebalance_strategy

    model_dump = getattr(rebalance_strategy, "model_dump", None)
    config = model_dump() if callable(model_dump) else rebalance_strategy
    return {
        "rebalance_strategy_class_import_path": _class_import_path(
            rebalance_strategy.__class__
        ),
        "config": config,
    }


def _extract_rebalance_strategy_description(
    rebalance_strategy: Any | None,
) -> str | None:
    if rebalance_strategy is None:
        return None
    if isinstance(rebalance_strategy, dict):
        return (
            rebalance_strategy.get(REBALANCE_STRATEGY_DESCRIPTION)
            or rebalance_strategy.get("description")
        )

    for attr_name in (REBALANCE_STRATEGY_DESCRIPTION, "description"):
        description = getattr(rebalance_strategy, attr_name, None)
        if description is not None:
            return str(description)

    get_explanation = getattr(rebalance_strategy, "get_explanation", None)
    if callable(get_explanation):
        description = get_explanation()
        return None if description is None else str(description)

    return None
