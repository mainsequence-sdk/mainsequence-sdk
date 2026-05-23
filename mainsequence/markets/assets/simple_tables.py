from __future__ import annotations

from typing import Annotated, Any

from pydantic import Field, constr

from mainsequence.tdag.simple_tables import (
    ForeignKey,
    Index,
    SimpleTable,
    SimpleTableUpdater,
    SimpleTableUpdaterConfiguration,
)

DEFAULT_MASTER_LIST_DEPENDENCY = "default_master_list"


class AssetSimpleTable(SimpleTable):
    """Base asset row for the SimpleTable-backed asset catalog."""

    unique_identifier: Annotated[constr(max_length=255), Index(unique=True)] = Field(
        ...,
        description="Stable asset business identifier within the selected asset master list.",
    )


class OpenFigiDetails(SimpleTable):
    """OpenFIGI identifier and classification details for an asset row."""

    asset_id: Annotated[
        int,
        ForeignKey(DEFAULT_MASTER_LIST_DEPENDENCY, on_delete="cascade"),
        Index(unique=True),
    ] = Field(..., description="Backend-managed row id from AssetSimpleTable.")
    figi: constr(max_length=12) | None = Field(
        None,
        description="FIGI identifier unique to a specific instrument on a market/exchange.",
    )
    composite: constr(max_length=12) | None = Field(
        None,
        description="Composite FIGI identifier aggregating local listings within one market.",
    )
    share_class: constr(max_length=12) | None = Field(
        None,
        description="Share class designation as reported by OpenFIGI.",
    )
    isin: constr(max_length=12) | None = Field(
        None,
        description="International Securities Identification Number.",
    )
    ticker: constr(max_length=50) | None = Field(
        None,
        description="OpenFIGI ticker field.",
    )
    name: constr(max_length=255) | None = Field(
        None,
        description="OpenFIGI instrument name.",
    )
    exchange_code: constr(max_length=50) | None = Field(
        None,
        description="OpenFIGI exchange code from exchCode.",
    )
    security_type: constr(max_length=50) | None = Field(
        None,
        description="Instrument type as reported by OpenFIGI.",
    )
    security_type_2: constr(max_length=50) | None = Field(
        None,
        description="OpenFIGI Security Type 2.",
    )
    security_market_sector: constr(max_length=50) | None = Field(
        None,
        description="High-level sector classification as reported by OpenFIGI.",
    )
    security_description: constr(max_length=255) | None = Field(
        None,
        description="OpenFIGI securityDescription field.",
    )
    metadata: str | None = Field(
        None,
        description="OpenFIGI metadata field used when non-FIGI fields cannot be shown.",
    )
    raw_payload: dict[str, Any] | None = Field(
        None,
        description="Raw OpenFIGI result payload for forward-compatible provider fields.",
    )


class AssetSimpleTableConfiguration(SimpleTableUpdaterConfiguration):
    """Configuration for the canonical asset base table."""

    identifier: str = Field(
        "mainsequence.markets.assets",
        description="Stable SimpleTable identifier for the canonical asset base table.",
    )


class OpenFigiDetailsConfiguration(SimpleTableUpdaterConfiguration):
    """Configuration for the canonical OpenFIGI asset-detail table."""

    identifier: str = Field(
        "mainsequence.markets.open_figi_details",
        description="Stable SimpleTable identifier for OpenFIGI asset detail rows.",
    )


class AssetSimpleTableUpdater(SimpleTableUpdater):
    """SimpleTable updater for canonical asset base rows."""

    SIMPLE_TABLE_SCHEMA = AssetSimpleTable

    def update(self) -> list[AssetSimpleTable]:
        return []


class OpenFigiDetailsUpdater(SimpleTableUpdater):
    """SimpleTable updater for one-to-one OpenFIGI asset details."""

    SIMPLE_TABLE_SCHEMA = OpenFigiDetails

    def __init__(
        self,
        configuration: OpenFigiDetailsConfiguration,
        *,
        asset_updater: AssetSimpleTableUpdater,
        **kwargs: Any,
    ):
        self.asset_updater = asset_updater
        super().__init__(configuration=configuration, **kwargs)

    def dependencies(self) -> dict[str, SimpleTableUpdater]:
        return {DEFAULT_MASTER_LIST_DEPENDENCY: self.asset_updater}

    def update(self) -> list[OpenFigiDetails]:
        return []


__all__ = [
    "DEFAULT_MASTER_LIST_DEPENDENCY",
    "AssetSimpleTable",
    "AssetSimpleTableConfiguration",
    "AssetSimpleTableUpdater",
    "OpenFigiDetails",
    "OpenFigiDetailsConfiguration",
    "OpenFigiDetailsUpdater",
]
