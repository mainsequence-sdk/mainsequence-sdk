from __future__ import annotations

from typing import Annotated, Any

from mainsequence.tdag.simple_tables import (
    Index,
    SimpleTable,
    SimpleTableUpdater,
    SimpleTableUpdaterConfiguration,
)

from .constants import (
    PORTFOLIO_DESCRIPTION,
    PORTFOLIO_METADATA_UNIQUE_IDENTIFIER,
)


class PortfolioMetadata(SimpleTable):
    """Metadata registry row for a canonical VFB portfolio."""

    unique_identifier: Annotated[str, Index(unique=True)]
    description: str | None = None


class PortfoliosMetadataConfiguration(SimpleTableUpdaterConfiguration):
    """Configuration for the canonical VFB portfolio metadata registry."""

    identifier: str = "mainsequence.markets.portfolios_metadata"


class PortfoliosMetadataUpdater(SimpleTableUpdater):
    """SimpleTable updater for canonical VFB portfolio metadata."""

    SIMPLE_TABLE_SCHEMA = PortfolioMetadata

    def update(self) -> list[PortfolioMetadata]:
        return []


def default_portfolio_metadata_updater() -> PortfoliosMetadataUpdater:
    return PortfoliosMetadataUpdater(configuration=PortfoliosMetadataConfiguration())


def build_portfolio_metadata(
    portfolio_index_asset: Any | None = None,
    *,
    unique_identifier: str | None = None,
    description: str | None = None,
    portfolio_configuration: Any | None = None,
) -> PortfolioMetadata:
    resolved_unique_identifier = unique_identifier
    if resolved_unique_identifier is None and portfolio_index_asset is not None:
        resolved_unique_identifier = _extract_unique_identifier(portfolio_index_asset)
    if not resolved_unique_identifier:
        raise ValueError(
            "unique_identifier or portfolio_index_asset is required to build "
            "PortfolioMetadata."
        )

    resolved_description = description
    if resolved_description is None:
        resolved_description = _extract_portfolio_description(portfolio_configuration)

    return PortfolioMetadata(
        unique_identifier=str(resolved_unique_identifier),
        description=(None if resolved_description is None else str(resolved_description)),
    )


def upsert_portfolio_metadata(
    portfolio_index_asset: Any | None = None,
    *,
    unique_identifier: str | None = None,
    description: str | None = None,
    portfolio_configuration: Any | None = None,
    updater: Any | None = None,
) -> PortfolioMetadata:
    metadata = build_portfolio_metadata(
        portfolio_index_asset,
        unique_identifier=unique_identifier,
        description=description,
        portfolio_configuration=portfolio_configuration,
    )
    resolved_updater = updater or default_portfolio_metadata_updater()
    return resolved_updater.upsert(metadata)


def get_portfolio_metadata(
    unique_identifier: str,
    *,
    updater: Any | None = None,
) -> PortfolioMetadata | None:
    resolved_updater = updater or default_portfolio_metadata_updater()
    rows = resolved_updater.execute_filter(
        PortfolioMetadata.filters.unique_identifier.eq(unique_identifier),
        limit=1,
    )
    return rows[0] if rows else None


def _extract_unique_identifier(value: Any) -> str | None:
    if isinstance(value, dict):
        unique_identifier = value.get(PORTFOLIO_METADATA_UNIQUE_IDENTIFIER)
    else:
        unique_identifier = getattr(value, PORTFOLIO_METADATA_UNIQUE_IDENTIFIER, None)
    return None if unique_identifier in (None, "") else str(unique_identifier)


def _extract_portfolio_description(value: Any | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, dict):
        description = value.get(PORTFOLIO_DESCRIPTION) or value.get("portfolio_description")
        if description is not None:
            return str(description)
        front_end_details = value.get("front_end_details")
        if front_end_details is not None:
            return _extract_portfolio_description(front_end_details)
        return _extract_portfolio_description(
            value.get("portfolio_markets_configuration")
            or value.get("portfolio_markets_config")
        )

    for attr_name in (PORTFOLIO_DESCRIPTION, "portfolio_description"):
        description = getattr(value, attr_name, None)
        if description is not None:
            return str(description)

    front_end_details = getattr(value, "front_end_details", None)
    if front_end_details is not None:
        return _extract_portfolio_description(front_end_details)

    markets_config = (
        getattr(value, "portfolio_markets_configuration", None)
        or getattr(value, "portfolio_markets_config", None)
    )
    if markets_config is not None:
        return _extract_portfolio_description(markets_config)

    return None
