from __future__ import annotations

from typing import Annotated

from pydantic import Field, constr

from mainsequence.tdag.simple_tables import (
    Index,
    SimpleTable,
    SimpleTableUpdater,
    SimpleTableUpdaterConfiguration,
)


class AssetSimpleTable(SimpleTable):
    """Base asset row for the SimpleTable-backed asset catalog."""

    unique_identifier: Annotated[constr(max_length=255), Index(unique=True)] = Field(
        ...,
        description="Stable asset business identifier within the selected asset master list.",
    )


class AssetSimpleTableConfiguration(SimpleTableUpdaterConfiguration):
    """Configuration for the canonical asset base table."""

    identifier: str = Field(
        "mainsequence.markets.assets",
        description="Stable SimpleTable identifier for the canonical asset base table.",
    )


class AssetSimpleTableUpdater(SimpleTableUpdater):
    """SimpleTable updater for canonical asset base rows."""

    SIMPLE_TABLE_SCHEMA = AssetSimpleTable

    def update(self) -> list[AssetSimpleTable]:
        return []


__all__ = [
    "AssetSimpleTable",
    "AssetSimpleTableConfiguration",
    "AssetSimpleTableUpdater",
]
