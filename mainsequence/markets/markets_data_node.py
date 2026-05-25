from __future__ import annotations

import copy
import datetime
from collections.abc import Iterable, Mapping
from typing import Any

import pandas as pd
from pydantic import Field

from mainsequence.client.models_tdag import UniqueIdentifierRangeMap, UpdateStatistics
from mainsequence.tdag.data_nodes import DataNode, DataNodeConfiguration

ASSET_UNIQUE_IDENTIFIER_DIMENSION = "unique_identifier"
MarketAssetScopeItem = str | Mapping[str, Any] | Any


class MarketDataNodeConfiguration(DataNodeConfiguration):
    """Base configuration for DataNodes scoped to platform market assets."""

    asset_list: list[MarketAssetScopeItem] | None = Field(
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
    def _asset_unique_identifier(cls, asset: MarketAssetScopeItem) -> str:
        if isinstance(asset, str):
            return asset
        if isinstance(asset, Mapping):
            value = asset.get(cls.asset_identity_dimension)
        else:
            value = getattr(asset, cls.asset_identity_dimension, None)

        if not isinstance(value, str):
            raise TypeError(
                "MarketDataNode asset scopes must contain strings, mappings, "
                f"or objects with {cls.asset_identity_dimension!r}."
            )
        return value

    @classmethod
    def validate_asset_list(
        cls,
        asset_list: Iterable[MarketAssetScopeItem] | None,
        *,
        allow_empty: bool = False,
    ) -> list[MarketAssetScopeItem] | None:
        """Validate and normalize an explicit market asset scope."""
        if asset_list is None:
            return None

        assets = list(asset_list)
        if not assets and not allow_empty:
            raise ValueError("asset_list cannot be empty for an asset-scoped MarketDataNode.")

        seen_unique_identifiers: set[str] = set()
        for position, asset in enumerate(assets):
            try:
                unique_identifier = cls._asset_unique_identifier(asset)
            except TypeError as exc:
                raise TypeError(f"{exc} Item {position} has type {type(asset).__name__}.") from exc

            if not unique_identifier.strip():
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
        asset_list: Iterable[MarketAssetScopeItem] | None,
    ) -> list[str] | None:
        """Return validated asset unique identifiers."""
        assets = cls.validate_asset_list(asset_list)
        if assets is None:
            return None

        return [cls._asset_unique_identifier(asset) for asset in assets]

    @classmethod
    def asset_dimension_filters(
        cls,
        asset_list: Iterable[MarketAssetScopeItem] | None,
    ) -> dict[str, list[str]] | None:
        """Translate an asset scope into canonical DataNode dimension filters."""
        unique_identifiers = cls.asset_unique_identifiers(asset_list)
        if unique_identifiers is None:
            return None

        return {cls.asset_identity_dimension: unique_identifiers}

    @classmethod
    def _is_min_max_bounds(cls, value: Any) -> bool:
        return isinstance(value, dict) and ("min" in value or "max" in value)

    @classmethod
    def _missing_identity_value(
        cls,
        current_level: dict[str, Any],
        fallback_value: Any,
        *,
        bounds_fallback: bool = False,
    ) -> Any:
        if bounds_fallback:
            for value in current_level.values():
                if cls._is_min_max_bounds(value):
                    return {"min": fallback_value, "max": fallback_value}

        return fallback_value

    @classmethod
    def _scope_tree_to_identity_values(
        cls,
        tree: Any,
        *,
        identity_values: list[str] | None,
        fallback_value: Any,
        target_depth: int,
        bounds_fallback: bool = False,
    ) -> Any:
        if identity_values is None:
            return copy.deepcopy(tree)

        if target_depth == 0:
            current_level = tree if isinstance(tree, dict) else {}
            missing_value = cls._missing_identity_value(
                current_level,
                fallback_value,
                bounds_fallback=bounds_fallback,
            )
            return {
                identity_value: (
                    copy.deepcopy(current_level[identity_value])
                    if identity_value in current_level
                    else copy.deepcopy(missing_value)
                )
                for identity_value in identity_values
            }

        if not isinstance(tree, dict):
            return {}

        return {
            key: cls._scope_tree_to_identity_values(
                value,
                identity_values=identity_values,
                fallback_value=fallback_value,
                target_depth=target_depth - 1,
                bounds_fallback=bounds_fallback,
            )
            for key, value in tree.items()
        }

    @classmethod
    def _max_in_nested_values(cls, value: Any) -> Any:
        if isinstance(value, dict):
            max_value = None
            for child in value.values():
                child_max = cls._max_in_nested_values(child)
                if child_max is not None and (max_value is None or child_max > max_value):
                    max_value = child_max
            return max_value

        return value

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

    def get_asset_list(self) -> list[MarketAssetScopeItem] | None:
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

    def get_update_asset_list(self) -> list[MarketAssetScopeItem] | None:
        """Return the asset scope selected for the current update cycle."""
        asset_list = getattr(self, "_setted_asset_list", None)
        if asset_list is not None:
            return self.validate_asset_list(asset_list)

        return self.get_asset_list()

    def get_asset_earliest_update(self, asset: MarketAssetScopeItem) -> Any:
        """Return the earliest scoped update timestamp for a platform asset."""
        self.validate_asset_list([asset])
        return self.update_statistics.get_earliest_update_for_identity(
            self._asset_unique_identifier(asset)
        )

    def get_asset_update_range_map_great_or_equal(
        self,
        extra_time_delta: datetime.timedelta | None = None,
    ) -> UniqueIdentifierRangeMap:
        """Return per-asset update ranges keyed by platform asset identity."""
        if self._asset_identity_depth() != 0:
            raise ValueError(
                f"{self.__class__.__name__} cannot build a flat asset range map "
                "because the asset identity is not the first identity dimension. "
                "Use get_asset_dimension_range_map_great_or_equal() instead."
            )

        return self.update_statistics.get_identity_range_map_great_or_equal(
            extra_time_delta=extra_time_delta
        )

    def get_asset_dimension_range_map_great_or_equal(
        self,
        extra_time_delta: datetime.timedelta | None = None,
    ) -> list[dict[str, Any]]:
        """Return generic dimension ranges using this market node's asset dimension."""
        return self.update_statistics.get_dimension_range_map_great_or_equal(
            identity_dimensions=self._asset_identity_dimensions(),
            extra_time_delta=extra_time_delta,
        )

    def get_asset_columnar_update_range_map_great_or_equal(
        self,
        *,
        extra_time_delta: datetime.timedelta | None = None,
        column_filter: list[str] | None = None,
    ) -> dict[str, UniqueIdentifierRangeMap]:
        """Return columnar per-asset ranges keyed by platform asset identity."""
        if self._asset_identity_depth() != 0:
            raise ValueError(
                f"{self.__class__.__name__} cannot build a flat columnar asset "
                "range map because the asset identity is not the first identity "
                "dimension."
            )

        return self.update_statistics.get_columnar_identity_range_map_great_or_equal(
            extra_time_delta=extra_time_delta,
            column_filter=column_filter,
        )

    def get_asset_dimension_filters(self) -> dict[str, list[str]] | None:
        """Return this node's validated asset scope as dimension filters."""
        return self.asset_dimension_filters(self.get_asset_list())

    def _asset_identity_dimensions(self) -> list[str]:
        config = self._get_data_node_configuration()
        index_names = getattr(config, "index_names", None) if config is not None else None
        if index_names is None:
            return [self.asset_identity_dimension]

        return list(index_names[1:])

    def _asset_identity_depth(self) -> int:
        identity_dimensions = self._asset_identity_dimensions()
        if self.asset_identity_dimension not in identity_dimensions:
            raise ValueError(
                f"{self.__class__.__name__} is asset-scoped but identity dimensions do "
                f"not include {self.asset_identity_dimension!r}: {identity_dimensions!r}."
            )

        return identity_dimensions.index(self.asset_identity_dimension)

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

    def scope_update_statistics_to_assets(
        self,
        update_statistics: UpdateStatistics,
    ) -> UpdateStatistics:
        """Return update statistics narrowed to this node's market asset scope."""
        self.assert_asset_index_contract()
        asset_list = self.get_asset_list()
        self._setted_asset_list = asset_list

        asset_unique_identifiers = self.asset_unique_identifiers(asset_list)
        fallback_date = self.get_offset_start()
        asset_depth = self._asset_identity_depth()

        scoped_index_progress = self._scope_tree_to_identity_values(
            update_statistics.index_progress,
            identity_values=asset_unique_identifiers,
            fallback_value=fallback_date,
            target_depth=asset_depth,
        )
        scoped_index_min = self._scope_tree_to_identity_values(
            update_statistics.index_min,
            identity_values=asset_unique_identifiers,
            fallback_value=fallback_date,
            target_depth=asset_depth,
        )

        if update_statistics.multi_index_column_stats is None:
            scoped_column_stats = None
        elif asset_unique_identifiers is None:
            scoped_column_stats = copy.deepcopy(update_statistics.multi_index_column_stats)
        else:
            scoped_column_stats = {
                column_name: self._scope_tree_to_identity_values(
                    column_stats,
                    identity_values=asset_unique_identifiers,
                    fallback_value=fallback_date,
                    target_depth=asset_depth,
                    bounds_fallback=True,
                )
                for column_name, column_stats in update_statistics.multi_index_column_stats.items()
            }

        scoped_update_statistics = UpdateStatistics(
            global_index_progress=copy.deepcopy(update_statistics.global_index_progress),
            index_progress=scoped_index_progress,
            index_min=scoped_index_min,
            max_time_index_value=update_statistics.max_time_index_value,
            limit_update_time=update_statistics.limit_update_time,
            multi_index_column_stats=scoped_column_stats,
            is_backfill=update_statistics.is_backfill,
        )
        scoped_max = self._max_in_nested_values(scoped_index_progress)
        scoped_update_statistics._max_time_in_update_statistics = (
            scoped_max
            if scoped_max is not None
            else update_statistics.max_time_index_value or fallback_date
        )
        scoped_update_statistics._initial_fallback_date = fallback_date
        return scoped_update_statistics

    def _set_update_statistics(self, update_statistics: UpdateStatistics) -> UpdateStatistics:
        """Narrow update statistics with market asset scope, outside core TDAG."""
        scoped_update_statistics = self.scope_update_statistics_to_assets(update_statistics)
        self.update_statistics = scoped_update_statistics
        return scoped_update_statistics

    def get_last_observation(
        self,
        asset_list: Iterable[MarketAssetScopeItem] | None = None,
    ) -> pd.DataFrame:
        """Return the latest observation, optionally scoped to market assets."""
        assets = (
            self.get_asset_list() if asset_list is None else self.validate_asset_list(asset_list)
        )
        return self.local_persist_manager.get_last_observation(
            dimension_filters=self.asset_dimension_filters(assets),
        )

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
            date_info["start_date_operand"] = ">="

        return self.get_ranged_data_per_asset(
            range_descriptor=inclusive_descriptor,
            columns=columns,
        )


__all__ = [
    "ASSET_UNIQUE_IDENTIFIER_DIMENSION",
    "MarketAssetScopeItem",
    "MarketDataNode",
    "MarketDataNodeConfiguration",
]
