from __future__ import annotations

import datetime as dt
from collections.abc import Callable
from typing import Annotated, Any

from pydantic import BaseModel, Field, constr, model_validator

from mainsequence.markets.assets.simple_tables import (
    DEFAULT_MASTER_LIST_DEPENDENCY,
    AssetSimpleTable,
    AssetSimpleTableUpdater,
)
from mainsequence.markets.portfolios.data_nodes.constants import (
    ASSET_UNIQUE_IDENTIFIER,
    PORTFOLIO_INDEX_ASSET_UNIQUE_IDENTIFIER,
)
from mainsequence.tdag.simple_tables import (
    ForeignKey,
    Index,
    SimpleTable,
    SimpleTableUpdater,
    SimpleTableUpdaterConfiguration,
    and_,
)
from mainsequence.tdag.simple_tables.filters import FilterExpr

PORTFOLIO_ASSET_TYPE = "portfolio"
DependentDeleteCallback = Callable[[list[str]], Any]


def _split_values(value: Any) -> list[Any]:
    if value in (None, ""):
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, (list, tuple, set)):
        return [item for item in value if item not in (None, "")]
    return [value]


def _unique_strings(value: Any) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for item in _split_values(value):
        item_value = str(item or "").strip()
        if not item_value or item_value in seen:
            continue
        seen.add(item_value)
        normalized.append(item_value)
    return normalized


def _combine_filters(filters: list[FilterExpr | None]) -> FilterExpr | None:
    items = [filter_expr for filter_expr in filters if filter_expr is not None]
    if not items:
        return None
    if len(items) == 1:
        return items[0]
    return and_(*items)


def _record_id(record_or_id: Any, label: str) -> int:
    if isinstance(record_or_id, int):
        return record_or_id
    record_id = getattr(record_or_id, "id", None)
    if record_id is None:
        raise ValueError(f"{label} must be a row id or a row with an id.")
    return int(record_id)


def _asset_filter_expr(
    asset_ref: Any,
    *,
    asset_unique_identifier: str | None = None,
    asset_unique_identifier__in: list[str] | tuple[str, ...] | str | None = None,
    asset_unique_identifier__contains: str | None = None,
    asset_type: str | None = PORTFOLIO_ASSET_TYPE,
    search: str | None = None,
) -> FilterExpr | None:
    filters: list[FilterExpr | None] = []
    if asset_type not in (None, ""):
        filters.append(asset_ref.f.asset_type.eq(str(asset_type)))
    if asset_unique_identifier not in (None, ""):
        filters.append(asset_ref.f.unique_identifier.eq(str(asset_unique_identifier)))

    unique_identifiers = _split_values(asset_unique_identifier__in)
    if unique_identifiers:
        filters.append(
            asset_ref.f.unique_identifier.in_(
                [str(unique_identifier) for unique_identifier in unique_identifiers]
            )
        )

    if asset_unique_identifier__contains not in (None, ""):
        filters.append(
            asset_ref.f.unique_identifier.contains(str(asset_unique_identifier__contains))
        )

    normalized_search = str(search or "").strip()
    if normalized_search:
        filters.append(asset_ref.f.unique_identifier.contains(normalized_search))

    return _combine_filters(filters)


def _dimension_filters_with_value(
    dimension_filters: dict[str, list[Any]] | None,
    key: str,
    value: Any,
) -> dict[str, list[Any]]:
    resolved: dict[str, list[Any]] = {
        filter_key: list(filter_values)
        for filter_key, filter_values in (dimension_filters or {}).items()
    }
    values = resolved.get(key)
    if values is None:
        resolved[key] = [value]
        return resolved
    if value not in values:
        raise ValueError(
            f"dimension_filters[{key!r}] conflicts with portfolio asset "
            f"unique_identifier {value!r}."
        )
    return resolved


class PortfolioAssetDeleteResult(BaseModel):
    """Result summary for portfolio detail delete helpers."""

    requested_identifiers: list[str] = Field(
        default_factory=list,
        description="Portfolio asset unique_identifiers requested for deletion.",
    )
    deleted_detail_identifiers: list[str] = Field(
        default_factory=list,
        description="Portfolio asset unique_identifiers whose detail rows were deleted.",
    )
    deleted_asset_identifiers: list[str] = Field(
        default_factory=list,
        description="Portfolio asset unique_identifiers whose AssetSimpleTable rows were deleted.",
    )
    missing_identifiers: list[str] = Field(
        default_factory=list,
        description="Requested portfolio asset unique_identifiers that were not found.",
    )
    dependent_delete_results: list[Any] = Field(
        default_factory=list,
        description="Results returned by explicitly approved dependent delete callbacks.",
    )

    @property
    def deleted_detail_count(self) -> int:
        return len(self.deleted_detail_identifiers)

    @property
    def deleted_asset_count(self) -> int:
        return len(self.deleted_asset_identifiers)


class Portfolio(SimpleTable):
    """Client-owned portfolio row linked to one AssetSimpleTable portfolio asset."""

    asset_id: Annotated[
        int,
        ForeignKey(DEFAULT_MASTER_LIST_DEPENDENCY, on_delete="cascade"),
        Index(unique=True),
    ] = Field(..., description="Backend-managed row id from AssetSimpleTable.")
    calendar_name: constr(max_length=255) | None = Field(
        None,
        description="Trading or business calendar name used by the portfolio.",
    )
    portfolio_weights_data_node_uid: constr(max_length=36) | None = Field(
        None,
        description="UID of the DataNode containing portfolio constituent weights.",
    )
    signal_weights_data_node_uid: constr(max_length=36) | None = Field(
        None,
        description="UID of the DataNode containing signal weights.",
    )
    portfolio_data_node_uid: constr(max_length=36) | None = Field(
        None,
        description="UID of the DataNode containing portfolio price or return data.",
    )
    backtest_table_price_column_name: constr(max_length=20) = Field(
        "close",
        description="Column name used as the reference price series for backtests.",
    )
    stats_json: str | None = Field(
        None,
        description="Optional JSON-encoded operational summary stats.",
    )
    builds_from_target_weights: bool = Field(
        True,
        description="Whether the portfolio is constructed from target weights.",
    )
    builds_from_predictions: bool = Field(
        False,
        description="Whether the portfolio is constructed from prediction signals.",
    )
    builds_from_target_positions: bool = Field(
        False,
        description="Whether the portfolio is constructed from target positions.",
    )
    tracking_funds_expected_exposure_from_latest_holdings: bool = Field(
        False,
        description=(
            "Whether tracking funds derive expected exposure from latest holdings "
            "instead of target weights."
        ),
    )

    @model_validator(mode="after")
    def validate_portfolio_configuration(self) -> Portfolio:
        build_flags = [
            self.builds_from_target_weights,
            self.builds_from_predictions,
            self.builds_from_target_positions,
        ]
        if sum(bool(flag) for flag in build_flags) != 1:
            raise ValueError(
                "Exactly one of builds_from_target_weights, builds_from_predictions, "
                "or builds_from_target_positions must be true."
            )
        return self

    @classmethod
    def filter_expr(
        cls,
        *,
        asset_id: int | None = None,
        asset_id__in: list[int] | tuple[int, ...] | str | None = None,
        calendar_name: str | None = None,
        portfolio_weights_data_node_uid: str | None = None,
        signal_weights_data_node_uid: str | None = None,
        portfolio_data_node_uid: str | None = None,
        builds_from_target_weights: bool | None = None,
        builds_from_predictions: bool | None = None,
        builds_from_target_positions: bool | None = None,
    ) -> FilterExpr | None:
        """Build the canonical filter expression for portfolio asset detail rows."""

        filters: list[FilterExpr | None] = []
        if asset_id is not None:
            filters.append(cls.f.asset_id.eq(int(asset_id)))

        asset_ids = _split_values(asset_id__in)
        if asset_ids:
            filters.append(cls.f.asset_id.in_([int(value) for value in asset_ids]))

        if calendar_name not in (None, ""):
            filters.append(cls.f.calendar_name.eq(str(calendar_name)))
        if portfolio_weights_data_node_uid not in (None, ""):
            filters.append(
                cls.f.portfolio_weights_data_node_uid.eq(
                    str(portfolio_weights_data_node_uid)
                )
            )
        if signal_weights_data_node_uid not in (None, ""):
            filters.append(
                cls.f.signal_weights_data_node_uid.eq(str(signal_weights_data_node_uid))
            )
        if portfolio_data_node_uid not in (None, ""):
            filters.append(cls.f.portfolio_data_node_uid.eq(str(portfolio_data_node_uid)))

        if builds_from_target_weights is not None:
            filters.append(cls.f.builds_from_target_weights.eq(builds_from_target_weights))
        if builds_from_predictions is not None:
            filters.append(cls.f.builds_from_predictions.eq(builds_from_predictions))
        if builds_from_target_positions is not None:
            filters.append(
                cls.f.builds_from_target_positions.eq(builds_from_target_positions)
            )

        return _combine_filters(filters)

    @classmethod
    def create_or_select_portfolio_asset(
        cls,
        *,
        asset_updater: AssetSimpleTableUpdater,
        unique_identifier: str,
    ) -> AssetSimpleTable:
        """Create or return the AssetSimpleTable row that owns a portfolio detail."""

        normalized_unique_identifier = str(unique_identifier or "").strip()
        if not normalized_unique_identifier:
            raise ValueError("unique_identifier is required.")

        asset = asset_updater.get_by_unique_identifier(normalized_unique_identifier)
        if asset is None:
            asset_updater.insert(
                AssetSimpleTable(
                    unique_identifier=normalized_unique_identifier,
                    asset_type=PORTFOLIO_ASSET_TYPE,
                )
            )
            return asset_updater.require_by_unique_identifier(
                normalized_unique_identifier
            )

        if asset.asset_type not in (None, "", PORTFOLIO_ASSET_TYPE):
            raise ValueError(
                "Asset "
                f"'{normalized_unique_identifier}' has asset_type={asset.asset_type!r}; "
                f"expected {PORTFOLIO_ASSET_TYPE!r}."
            )

        if asset.asset_type != PORTFOLIO_ASSET_TYPE:
            asset_updater.upsert(
                asset.model_copy(update={"asset_type": PORTFOLIO_ASSET_TYPE})
            )
            asset = asset_updater.require_by_unique_identifier(
                normalized_unique_identifier
            )

        return asset

    @classmethod
    def create_or_update_for_asset(
        cls,
        *,
        updater: PortfolioAssetDetailUpdater,
        asset: AssetSimpleTable | int | None = None,
        asset_id: int | None = None,
        asset_unique_identifier: str | None = None,
        calendar_name: str | None = None,
        portfolio_weights_data_node_uid: str | None = None,
        signal_weights_data_node_uid: str | None = None,
        portfolio_data_node_uid: str | None = None,
        backtest_table_price_column_name: str = "close",
        stats_json: str | None = None,
        builds_from_target_weights: bool = True,
        builds_from_predictions: bool = False,
        builds_from_target_positions: bool = False,
        tracking_funds_expected_exposure_from_latest_holdings: bool = False,
    ) -> Portfolio:
        """Create or replace the detail row for a portfolio asset."""

        if asset is not None:
            resolved_asset_id = _record_id(asset, "asset")
        elif asset_id is not None:
            resolved_asset_id = int(asset_id)
        elif asset_unique_identifier not in (None, ""):
            resolved_asset_id = _record_id(
                cls.create_or_select_portfolio_asset(
                    asset_updater=updater.asset_updater,
                    unique_identifier=str(asset_unique_identifier),
                ),
                "asset",
            )
        else:
            raise ValueError(
                "asset, asset_id, or asset_unique_identifier is required."
            )

        existing = updater.get_by_asset_id(resolved_asset_id)
        detail = cls(
            id=getattr(existing, "id", None),
            asset_id=resolved_asset_id,
            calendar_name=calendar_name,
            portfolio_weights_data_node_uid=portfolio_weights_data_node_uid,
            signal_weights_data_node_uid=signal_weights_data_node_uid,
            portfolio_data_node_uid=portfolio_data_node_uid,
            backtest_table_price_column_name=backtest_table_price_column_name,
            stats_json=stats_json,
            builds_from_target_weights=builds_from_target_weights,
            builds_from_predictions=builds_from_predictions,
            builds_from_target_positions=builds_from_target_positions,
            tracking_funds_expected_exposure_from_latest_holdings=(
                tracking_funds_expected_exposure_from_latest_holdings
            ),
        )
        updater.upsert(detail)
        return updater.get_by_asset_id(resolved_asset_id) or detail

    @classmethod
    def search_portfolios(
        cls,
        *,
        updater: PortfolioAssetDetailUpdater,
        asset: AssetSimpleTable | int | None = None,
        asset_id: int | None = None,
        asset_ids: list[int] | tuple[int, ...] | None = None,
        asset_unique_identifier: str | None = None,
        asset_unique_identifier__in: list[str] | tuple[str, ...] | str | None = None,
        asset_unique_identifier__contains: str | None = None,
        asset_type: str | None = PORTFOLIO_ASSET_TYPE,
        search: str | None = None,
        calendar_name: str | None = None,
        portfolio_weights_data_node_uid: str | None = None,
        signal_weights_data_node_uid: str | None = None,
        portfolio_data_node_uid: str | None = None,
        builds_from_target_weights: bool | None = None,
        builds_from_predictions: bool | None = None,
        builds_from_target_positions: bool | None = None,
        limit: int = 500,
        offset: int = 0,
    ) -> list[Portfolio]:
        """Search portfolio details through their linked AssetSimpleTable row."""

        return updater.filter_portfolio_asset_details(
            asset=asset,
            asset_id=asset_id,
            asset_ids=asset_ids,
            asset_unique_identifier=asset_unique_identifier,
            asset_unique_identifier__in=asset_unique_identifier__in,
            asset_unique_identifier__contains=asset_unique_identifier__contains,
            asset_type=asset_type,
            search=search,
            calendar_name=calendar_name,
            portfolio_weights_data_node_uid=portfolio_weights_data_node_uid,
            signal_weights_data_node_uid=signal_weights_data_node_uid,
            portfolio_data_node_uid=portfolio_data_node_uid,
            builds_from_target_weights=builds_from_target_weights,
            builds_from_predictions=builds_from_predictions,
            builds_from_target_positions=builds_from_target_positions,
            limit=limit,
            offset=offset,
        )

    @classmethod
    def get_portfolio_detail(
        cls,
        *,
        updater: PortfolioAssetDetailUpdater,
        asset: AssetSimpleTable | int | None = None,
        asset_id: int | None = None,
        asset_unique_identifier: str | None = None,
    ) -> Portfolio | None:
        rows = cls.search_portfolios(
            updater=updater,
            asset=asset,
            asset_id=asset_id,
            asset_unique_identifier=asset_unique_identifier,
            limit=1,
        )
        return rows[0] if rows else None

    @classmethod
    def require_portfolio_detail(
        cls,
        *,
        updater: PortfolioAssetDetailUpdater,
        asset: AssetSimpleTable | int | None = None,
        asset_id: int | None = None,
        asset_unique_identifier: str | None = None,
    ) -> Portfolio:
        detail = cls.get_portfolio_detail(
            updater=updater,
            asset=asset,
            asset_id=asset_id,
            asset_unique_identifier=asset_unique_identifier,
        )
        if detail is None:
            raise ValueError("Portfolio asset detail was not found.")
        return detail

    @classmethod
    def search_portfolios_with_assets(
        cls,
        *,
        updater: PortfolioAssetDetailUpdater,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        details = cls.search_portfolios(updater=updater, **kwargs)
        assets_by_id = updater.assets_by_id_for_details(details)
        return [
            detail.to_display_dict(asset=assets_by_id.get(detail.asset_id))
            for detail in details
        ]

    @classmethod
    def delete_for_asset(
        cls,
        *,
        updater: PortfolioAssetDetailUpdater,
        asset_unique_identifier: str,
        delete_asset_row: bool = False,
        approved_dependent_delete_callbacks: list[DependentDeleteCallback]
        | tuple[DependentDeleteCallback, ...]
        | None = None,
    ) -> PortfolioAssetDeleteResult:
        """Delete one portfolio detail row by portfolio asset unique_identifier."""

        return cls.bulk_delete(
            updater=updater,
            asset_unique_identifiers=[asset_unique_identifier],
            delete_asset_rows=delete_asset_row,
            approved_dependent_delete_callbacks=approved_dependent_delete_callbacks,
        )

    @classmethod
    def bulk_delete(
        cls,
        *,
        updater: PortfolioAssetDetailUpdater,
        asset_unique_identifiers: list[str] | tuple[str, ...] | str,
        delete_asset_rows: bool = False,
        approved_dependent_delete_callbacks: list[DependentDeleteCallback]
        | tuple[DependentDeleteCallback, ...]
        | None = None,
    ) -> PortfolioAssetDeleteResult:
        """Delete selected portfolio details with explicit dependent cleanup hooks."""

        return updater.delete_portfolio_asset_details(
            asset_unique_identifiers=asset_unique_identifiers,
            delete_asset_rows=delete_asset_rows,
            approved_dependent_delete_callbacks=approved_dependent_delete_callbacks,
        )

    @classmethod
    def get_portfolios_latest_weights(
        cls,
        *,
        updater: PortfolioAssetDetailUpdater,
        portfolio_weights_node: Any,
        asset_unique_identifiers: list[str] | tuple[str, ...] | str,
        dimension_filters: dict[str, list[Any]] | None = None,
    ) -> dict[str, Any]:
        """Return latest weights keyed by portfolio asset unique_identifier."""

        return updater.get_portfolios_latest_weights(
            portfolio_weights_node=portfolio_weights_node,
            asset_unique_identifiers=asset_unique_identifiers,
            dimension_filters=dimension_filters,
        )

    def get_portfolio_asset(
        self,
        *,
        asset_updater: AssetSimpleTableUpdater,
    ) -> AssetSimpleTable:
        rows = asset_updater.execute_filter(
            AssetSimpleTable.f.id.eq(self.asset_id),
            limit=1,
        )
        if not rows:
            raise ValueError(f"AssetSimpleTable row id={self.asset_id} was not found.")
        return rows[0]

    def get_asset_unique_identifier(
        self,
        *,
        asset: AssetSimpleTable | None = None,
        asset_updater: AssetSimpleTableUpdater | None = None,
    ) -> str:
        resolved_asset = asset
        if resolved_asset is None:
            if asset_updater is None:
                raise ValueError("asset or asset_updater is required.")
            resolved_asset = self.get_portfolio_asset(asset_updater=asset_updater)
        return str(resolved_asset.unique_identifier)

    def get_latest_weights(
        self,
        *,
        portfolio_weights_node: Any,
        asset: AssetSimpleTable | None = None,
        asset_updater: AssetSimpleTableUpdater | None = None,
        dimension_filters: dict[str, list[Any]] | None = None,
    ) -> Any:
        unique_identifier = self.get_asset_unique_identifier(
            asset=asset,
            asset_updater=asset_updater,
        )
        return portfolio_weights_node.get_last_observation(
            dimension_filters=_dimension_filters_with_value(
                dimension_filters,
                PORTFOLIO_INDEX_ASSET_UNIQUE_IDENTIFIER,
                unique_identifier,
            )
        )

    def get_portfolio_price_history(
        self,
        *,
        portfolio_data_node: Any,
        asset: AssetSimpleTable | None = None,
        asset_updater: AssetSimpleTableUpdater | None = None,
        start_date: dt.datetime | None = None,
        end_date: dt.datetime | None = None,
        great_or_equal: bool = True,
        less_or_equal: bool = True,
        dimension_filters: dict[str, list[Any]] | None = None,
        columns: list[str] | None = None,
    ) -> Any:
        unique_identifier = self.get_asset_unique_identifier(
            asset=asset,
            asset_updater=asset_updater,
        )
        return portfolio_data_node.get_df_between_dates(
            start_date=start_date,
            end_date=end_date,
            great_or_equal=great_or_equal,
            less_or_equal=less_or_equal,
            dimension_filters=_dimension_filters_with_value(
                dimension_filters,
                ASSET_UNIQUE_IDENTIFIER,
                unique_identifier,
            ),
            columns=columns,
        )

    def get_latest_portfolio_value(
        self,
        *,
        portfolio_data_node: Any,
        asset: AssetSimpleTable | None = None,
        asset_updater: AssetSimpleTableUpdater | None = None,
        dimension_filters: dict[str, list[Any]] | None = None,
    ) -> Any:
        unique_identifier = self.get_asset_unique_identifier(
            asset=asset,
            asset_updater=asset_updater,
        )
        return portfolio_data_node.get_last_observation(
            dimension_filters=_dimension_filters_with_value(
                dimension_filters,
                ASSET_UNIQUE_IDENTIFIER,
                unique_identifier,
            )
        )

    def to_display_dict(
        self,
        *,
        asset: AssetSimpleTable | None = None,
    ) -> dict[str, Any]:
        payload = self.model_dump(mode="python")
        payload["asset"] = None if asset is None else asset.model_dump(mode="python")
        payload["asset_unique_identifier"] = (
            None if asset is None else asset.unique_identifier
        )
        payload["asset_type"] = None if asset is None else asset.asset_type
        return payload

    def build_summary(
        self,
        *,
        asset: AssetSimpleTable | None = None,
        asset_updater: AssetSimpleTableUpdater | None = None,
        portfolio_weights_node: Any | None = None,
        portfolio_data_node: Any | None = None,
        include_latest_weights: bool = True,
        include_latest_portfolio_value: bool = True,
    ) -> dict[str, Any]:
        resolved_asset = asset
        if resolved_asset is None and asset_updater is not None:
            resolved_asset = self.get_portfolio_asset(asset_updater=asset_updater)

        summary = self.to_display_dict(asset=resolved_asset)
        if include_latest_weights and portfolio_weights_node is not None:
            summary["latest_weights"] = self.get_latest_weights(
                portfolio_weights_node=portfolio_weights_node,
                asset=resolved_asset,
                asset_updater=asset_updater,
            )
        if include_latest_portfolio_value and portfolio_data_node is not None:
            summary["latest_portfolio_value"] = self.get_latest_portfolio_value(
                portfolio_data_node=portfolio_data_node,
                asset=resolved_asset,
                asset_updater=asset_updater,
            )
        return summary


class PortfolioAssetDetailConfiguration(SimpleTableUpdaterConfiguration):
    """Configuration for the canonical portfolio asset detail table."""

    identifier: str = Field(
        "mainsequence.markets.portfolio_asset_details",
        description="Stable SimpleTable identifier for portfolio asset details.",
    )


class PortfolioAssetDetailUpdater(SimpleTableUpdater):
    """SimpleTable updater for portfolio asset detail rows."""

    SIMPLE_TABLE_SCHEMA = Portfolio

    def __init__(
        self,
        configuration: PortfolioAssetDetailConfiguration,
        *,
        asset_updater: AssetSimpleTableUpdater,
        **kwargs: Any,
    ):
        self.asset_updater = asset_updater
        super().__init__(configuration=configuration, **kwargs)

    def dependencies(self) -> dict[str, SimpleTableUpdater]:
        return {DEFAULT_MASTER_LIST_DEPENDENCY: self.asset_updater}

    def update(self) -> list[Portfolio]:
        return []

    def filter_portfolio_asset_details(
        self,
        *,
        asset: AssetSimpleTable | int | None = None,
        asset_id: int | None = None,
        asset_unique_identifier: str | None = None,
        asset_unique_identifier__in: list[str] | tuple[str, ...] | str | None = None,
        asset_unique_identifier__contains: str | None = None,
        asset_type: str | None = None,
        asset_ids: list[int] | tuple[int, ...] | None = None,
        search: str | None = None,
        calendar_name: str | None = None,
        portfolio_weights_data_node_uid: str | None = None,
        signal_weights_data_node_uid: str | None = None,
        portfolio_data_node_uid: str | None = None,
        builds_from_target_weights: bool | None = None,
        builds_from_predictions: bool | None = None,
        builds_from_target_positions: bool | None = None,
        limit: int = 500,
        offset: int = 0,
    ) -> list[Portfolio]:
        resolved_asset_ids: list[int] = []
        if asset is not None:
            resolved_asset_ids.append(_record_id(asset, "asset"))
        if asset_id is not None:
            resolved_asset_ids.append(int(asset_id))
        for value in asset_ids or ():
            resolved_asset_ids.append(int(value))

        unique_asset_ids = sorted(set(resolved_asset_ids))
        detail_filter_expr = Portfolio.filter_expr(
            asset_id=unique_asset_ids[0] if len(unique_asset_ids) == 1 else None,
            asset_id__in=unique_asset_ids if len(unique_asset_ids) > 1 else None,
            calendar_name=calendar_name,
            portfolio_weights_data_node_uid=portfolio_weights_data_node_uid,
            signal_weights_data_node_uid=signal_weights_data_node_uid,
            portfolio_data_node_uid=portfolio_data_node_uid,
            builds_from_target_weights=builds_from_target_weights,
            builds_from_predictions=builds_from_predictions,
            builds_from_target_positions=builds_from_target_positions,
        )

        asset_alias = "portfolio_asset"
        asset_ref = AssetSimpleTable.ref(asset_alias)
        asset_filter_expr = _asset_filter_expr(
            asset_ref,
            asset_unique_identifier=asset_unique_identifier,
            asset_unique_identifier__in=asset_unique_identifier__in,
            asset_unique_identifier__contains=asset_unique_identifier__contains,
            asset_type=asset_type,
            search=search,
        )
        joins = None
        if asset_filter_expr is not None:
            joins = [
                AssetSimpleTable.join(
                    asset_alias,
                    storage_hash=self.asset_updater.storage_hash,
                )
            ]

        return self.execute_filter(
            _combine_filters([detail_filter_expr, asset_filter_expr]),
            joins=joins,
            limit=limit,
            offset=offset,
        )

    def assets_by_id_for_details(
        self,
        details: list[Portfolio] | tuple[Portfolio, ...],
    ) -> dict[int, AssetSimpleTable]:
        asset_ids = sorted({detail.asset_id for detail in details})
        if not asset_ids:
            return {}
        assets = self.asset_updater.execute_filter(
            AssetSimpleTable.f.id.in_(asset_ids),
            limit=len(asset_ids),
        )
        return {
            asset_id: asset
            for asset in assets
            if (asset_id := getattr(asset, "id", None)) is not None
        }

    def get_by_asset_id(self, asset_id: int) -> Portfolio | None:
        rows = self.filter_portfolio_asset_details(asset_id=asset_id, limit=1)
        return rows[0] if rows else None

    def get_by_asset_unique_identifier(
        self,
        asset_unique_identifier: str,
    ) -> Portfolio | None:
        rows = self.filter_portfolio_asset_details(
            asset_unique_identifier=asset_unique_identifier,
            limit=1,
        )
        return rows[0] if rows else None

    def require_by_asset_unique_identifier(
        self,
        asset_unique_identifier: str,
    ) -> Portfolio:
        row = self.get_by_asset_unique_identifier(asset_unique_identifier)
        if row is None:
            raise ValueError(
                f"Portfolio asset detail for asset '{asset_unique_identifier}' was not found."
            )
        return row

    def delete_portfolio_asset_details(
        self,
        *,
        asset_unique_identifiers: list[str] | tuple[str, ...] | str,
        delete_asset_rows: bool = False,
        approved_dependent_delete_callbacks: list[DependentDeleteCallback]
        | tuple[DependentDeleteCallback, ...]
        | None = None,
    ) -> PortfolioAssetDeleteResult:
        requested_identifiers = _unique_strings(asset_unique_identifiers)
        if not requested_identifiers:
            raise ValueError("asset_unique_identifiers is required.")

        details = self.filter_portfolio_asset_details(
            asset_unique_identifier__in=requested_identifiers,
            limit=len(requested_identifiers),
        )
        assets_by_id = self.assets_by_id_for_details(details)
        detail_by_identifier: dict[str, Portfolio] = {}
        asset_by_identifier: dict[str, AssetSimpleTable] = {}
        for detail in details:
            asset = assets_by_id.get(detail.asset_id)
            if asset is None:
                continue
            identifier = str(asset.unique_identifier)
            detail_by_identifier[identifier] = detail
            asset_by_identifier[identifier] = asset

        found_identifiers = [
            identifier
            for identifier in requested_identifiers
            if identifier in detail_by_identifier
        ]
        missing_identifiers = [
            identifier
            for identifier in requested_identifiers
            if identifier not in detail_by_identifier
        ]

        dependent_delete_results: list[Any] = []
        if found_identifiers:
            for callback in approved_dependent_delete_callbacks or ():
                dependent_delete_results.append(callback(found_identifiers))

        deleted_detail_identifiers: list[str] = []
        for identifier in found_identifiers:
            self.delete(detail_by_identifier[identifier])
            deleted_detail_identifiers.append(identifier)

        deleted_asset_identifiers: list[str] = []
        if delete_asset_rows:
            for identifier in found_identifiers:
                asset = asset_by_identifier.get(identifier)
                if asset is None:
                    continue
                self.asset_updater.delete(asset)
                deleted_asset_identifiers.append(identifier)

        return PortfolioAssetDeleteResult(
            requested_identifiers=requested_identifiers,
            deleted_detail_identifiers=deleted_detail_identifiers,
            deleted_asset_identifiers=deleted_asset_identifiers,
            missing_identifiers=missing_identifiers,
            dependent_delete_results=dependent_delete_results,
        )

    def get_portfolios_latest_weights(
        self,
        *,
        portfolio_weights_node: Any,
        asset_unique_identifiers: list[str] | tuple[str, ...] | str,
        dimension_filters: dict[str, list[Any]] | None = None,
    ) -> dict[str, Any]:
        requested_identifiers = _unique_strings(asset_unique_identifiers)
        if not requested_identifiers:
            raise ValueError("asset_unique_identifiers is required.")

        details = self.filter_portfolio_asset_details(
            asset_unique_identifier__in=requested_identifiers,
            limit=len(requested_identifiers),
        )
        assets_by_id = self.assets_by_id_for_details(details)
        detail_by_identifier: dict[str, Portfolio] = {}
        asset_by_identifier: dict[str, AssetSimpleTable] = {}
        for detail in details:
            asset = assets_by_id.get(detail.asset_id)
            if asset is None:
                continue
            identifier = str(asset.unique_identifier)
            detail_by_identifier[identifier] = detail
            asset_by_identifier[identifier] = asset

        latest_weights: dict[str, Any] = {}
        for identifier in requested_identifiers:
            detail = detail_by_identifier.get(identifier)
            if detail is None:
                latest_weights[identifier] = None
                continue
            latest_weights[identifier] = detail.get_latest_weights(
                portfolio_weights_node=portfolio_weights_node,
                asset=asset_by_identifier.get(identifier),
                asset_updater=self.asset_updater,
                dimension_filters=dimension_filters,
            )
        return latest_weights


__all__ = [
    "PORTFOLIO_ASSET_TYPE",
    "PortfolioAssetDeleteResult",
    "Portfolio",
    "PortfolioAssetDetailConfiguration",
    "PortfolioAssetDetailUpdater",
]
