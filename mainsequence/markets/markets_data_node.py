from __future__ import annotations

import copy
from collections.abc import Iterable
from typing import Any

import pandas as pd
from pydantic import Field

from mainsequence.client.markets.models.assets import AssetMixin
from mainsequence.client.models_tdag import UniqueIdentifierRangeMap, UpdateStatistics
from mainsequence.tdag.data_nodes import DataNode, DataNodeConfiguration

ASSET_UNIQUE_IDENTIFIER_DIMENSION = "unique_identifier"


class MarketDataNodeConfiguration(DataNodeConfiguration):
    """Base configuration for DataNodes scoped to platform market assets."""

    asset_list: list[AssetMixin] | None = Field(
        default=None,
        description=(
            "Optional platform asset scope for updater partitioning. Asset "
            "semantics are owned by the markets layer, not core TDAG."
        ),
        json_schema_extra={"update_only": True},
    )


class MarketDataNode(DataNode):
    """
    DataNode boundary for market datasets whose identity dimension is an asset.

    Core TDAG works with generic dimensions. This class owns the market-specific
    contract that platform assets are addressed by ``unique_identifier``.
    """

    asset_identity_dimension = ASSET_UNIQUE_IDENTIFIER_DIMENSION

    @classmethod
    def validate_asset_list(
        cls,
        asset_list: Iterable[AssetMixin] | None,
        *,
        allow_empty: bool = False,
    ) -> list[AssetMixin] | None:
        """Validate and normalize an explicit market asset scope."""
        if asset_list is None:
            return None

        assets = list(asset_list)
        if not assets and not allow_empty:
            raise ValueError("asset_list cannot be empty for an asset-scoped MarketDataNode.")

        seen_unique_identifiers: set[str] = set()
        for position, asset in enumerate(assets):
            if not isinstance(asset, AssetMixin):
                raise TypeError(
                    "MarketDataNode asset scopes must contain AssetMixin instances; "
                    f"item {position} has type {type(asset).__name__}."
                )

            unique_identifier = getattr(asset, cls.asset_identity_dimension, None)
            if not isinstance(unique_identifier, str) or not unique_identifier.strip():
                raise ValueError(
                    "MarketDataNode asset scopes require a non-empty "
                    f"{cls.asset_identity_dimension!r}; item {position} is invalid."
                )

            if unique_identifier in seen_unique_identifiers:
                raise ValueError(
                    "MarketDataNode asset scopes cannot contain duplicate "
                    f"{cls.asset_identity_dimension!r}: {unique_identifier!r}."
                )
            seen_unique_identifiers.add(unique_identifier)

        return assets

    @classmethod
    def asset_unique_identifiers(
        cls,
        asset_list: Iterable[AssetMixin] | None,
    ) -> list[str] | None:
        """Return validated asset unique identifiers."""
        assets = cls.validate_asset_list(asset_list)
        if assets is None:
            return None

        return [getattr(asset, cls.asset_identity_dimension) for asset in assets]

    @classmethod
    def asset_dimension_filters(
        cls,
        asset_list: Iterable[AssetMixin] | None,
    ) -> dict[str, list[str]] | None:
        """Translate an asset scope into canonical DataNode dimension filters."""
        unique_identifiers = cls.asset_unique_identifiers(asset_list)
        if unique_identifiers is None:
            return None

        return {cls.asset_identity_dimension: unique_identifiers}

    @classmethod
    def asset_range_map_to_dimension_range_map(
        cls,
        range_descriptor: UniqueIdentifierRangeMap | None,
    ) -> list[dict[str, Any]] | None:
        """Translate an asset unique-identifier range map to generic dimensions."""
        if range_descriptor is None:
            return None

        return [
            {
                "coordinate": {cls.asset_identity_dimension: unique_identifier},
                **copy.deepcopy(date_info),
            }
            for unique_identifier, date_info in range_descriptor.items()
        ]

    def get_asset_list(self) -> list[AssetMixin] | None:
        """
        Return and validate the asset scope for this market DataNode.

        Subclasses may override this to resolve assets from an asset category,
        account, portfolio, or instrument source.
        """
        asset_list = getattr(self, "asset_list", None)
        if asset_list is None:
            config = self._get_data_node_configuration()
            asset_list = getattr(config, "asset_list", None) if config is not None else None

        return self.validate_asset_list(asset_list)

    def get_asset_dimension_filters(self) -> dict[str, list[str]] | None:
        """Return this node's validated asset scope as dimension filters."""
        return self.asset_dimension_filters(self.get_asset_list())

    def assert_asset_index_contract(self) -> None:
        """Validate that configured asset tables include the asset dimension."""
        config = self._get_data_node_configuration()
        index_names = getattr(config, "index_names", None) if config is not None else None
        if index_names is None:
            return

        if self.asset_identity_dimension not in index_names:
            raise ValueError(
                f"{self.__class__.__name__} is asset-scoped but index_names does not "
                f"include {self.asset_identity_dimension!r}: {index_names!r}."
            )

    def _set_update_statistics(self, update_statistics: UpdateStatistics) -> UpdateStatistics:
        """Narrow update statistics with market asset scope, outside core TDAG."""
        self.assert_asset_index_contract()
        asset_list = self.get_asset_list()
        self._setted_asset_list = asset_list

        scoped_update_statistics = update_statistics.update_assets(
            asset_list,
            init_fallback_date=self.get_offset_start(),
        )
        self.update_statistics = scoped_update_statistics
        return scoped_update_statistics

    def get_last_observation(
        self,
        asset_list: Iterable[AssetMixin] | None = None,
    ) -> pd.DataFrame:
        """Return the latest observation, optionally scoped to market assets."""
        assets = self.get_asset_list() if asset_list is None else self.validate_asset_list(asset_list)
        return super().get_last_observation(asset_list=assets)

    def get_ranged_data_per_asset(
        self,
        range_descriptor: UniqueIdentifierRangeMap | None,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        """Fetch market data using an asset unique-identifier range map."""
        return self.get_df_between_dates(
            dimension_range_map=self.asset_range_map_to_dimension_range_map(range_descriptor),
            columns=columns,
        )

    def get_ranged_data_per_asset_great_or_equal(
        self,
        range_descriptor: UniqueIdentifierRangeMap | None,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        """Fetch market data using inclusive asset lower bounds."""
        if range_descriptor is None:
            return self.get_ranged_data_per_asset(range_descriptor=None, columns=columns)

        inclusive_descriptor = copy.deepcopy(range_descriptor)
        for date_info in inclusive_descriptor.values():
            date_info["start_date_operand"] = "=>"

        return self.get_ranged_data_per_asset(
            range_descriptor=inclusive_descriptor,
            columns=columns,
        )

    def filter_by_assets_ranges(self, asset_ranges_map: dict[str, Any]) -> pd.DataFrame:
        """Compatibility wrapper for older market callers using asset range maps."""
        return self.local_persist_manager.filter_by_assets_ranges(asset_ranges_map)


__all__ = [
    "ASSET_UNIQUE_IDENTIFIER_DIMENSION",
    "MarketDataNode",
    "MarketDataNodeConfiguration",
]
