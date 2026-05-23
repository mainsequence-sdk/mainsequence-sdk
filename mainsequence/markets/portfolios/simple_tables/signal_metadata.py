from __future__ import annotations

from typing import Annotated, Any

from mainsequence.tdag.simple_tables import (
    Index,
    SimpleTable,
    SimpleTableUpdater,
    SimpleTableUpdaterConfiguration,
)

from ..data_nodes.constants import SIGNAL_DESCRIPTION


class SignalMetadata(SimpleTable):
    """Metadata registry row for a canonical VFB signal."""

    signal_uid: Annotated[str, Index(unique=True)]
    signal_description: str | None = None


class SignalsMetadataConfiguration(SimpleTableUpdaterConfiguration):
    """Configuration for the canonical VFB signal metadata registry."""

    identifier: str = "mainsequence.markets.signals"


class SignalsMetadataUpdater(SimpleTableUpdater):
    """SimpleTable updater for canonical VFB signal metadata."""

    SIMPLE_TABLE_SCHEMA = SignalMetadata

    def update(self) -> list[SignalMetadata]:
        return []


def default_signal_metadata_updater() -> SignalsMetadataUpdater:
    return SignalsMetadataUpdater(configuration=SignalsMetadataConfiguration())


def build_signal_metadata(
    signal: Any | None = None,
    *,
    signal_uid: str | None = None,
    signal_description: str | None = None,
) -> SignalMetadata:
    if signal_uid is None and signal is not None:
        from ..data_nodes.signal_weights import compute_signal_uid

        resolved_signal_uid = compute_signal_uid(signal)
    else:
        resolved_signal_uid = signal_uid
    if not resolved_signal_uid:
        raise ValueError("signal_uid or signal is required to build SignalMetadata.")

    resolved_description = signal_description
    if resolved_description is None:
        resolved_description = _extract_signal_description(signal)

    return SignalMetadata(
        signal_uid=str(resolved_signal_uid),
        signal_description=(None if resolved_description is None else str(resolved_description)),
    )


def upsert_signal_metadata(
    signal: Any | None = None,
    *,
    signal_uid: str | None = None,
    signal_description: str | None = None,
    updater: Any | None = None,
) -> SignalMetadata:
    metadata = build_signal_metadata(
        signal,
        signal_uid=signal_uid,
        signal_description=signal_description,
    )
    resolved_updater = updater or default_signal_metadata_updater()
    return resolved_updater.upsert(metadata)


def get_signal_metadata(
    signal_uid: str,
    *,
    updater: Any | None = None,
) -> SignalMetadata | None:
    resolved_updater = updater or default_signal_metadata_updater()
    rows = resolved_updater.execute_filter(
        SignalMetadata.filters.signal_uid.eq(signal_uid),
        limit=1,
    )
    return rows[0] if rows else None


def _extract_signal_description(signal: Any | None) -> str | None:
    if signal is None:
        return None
    if isinstance(signal, dict):
        return signal.get(SIGNAL_DESCRIPTION) or signal.get("description")

    for attr_name in (SIGNAL_DESCRIPTION, "description"):
        description = getattr(signal, attr_name, None)
        if description is not None:
            return str(description)

    get_explanation = getattr(signal, "get_explanation", None)
    if callable(get_explanation):
        description = get_explanation()
        return None if description is None else str(description)

    return None
