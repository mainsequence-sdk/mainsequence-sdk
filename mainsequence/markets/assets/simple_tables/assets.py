from __future__ import annotations

from typing import Annotated, Any

from pydantic import Field, constr

from mainsequence.tdag.simple_tables import (
    Index,
    SimpleTable,
    SimpleTableUpdater,
    SimpleTableUpdaterConfiguration,
    and_,
)
from mainsequence.tdag.simple_tables.filters import FilterExpr

DEFAULT_MASTER_LIST_DEPENDENCY = "default_master_list"


def _split_values(value: Any) -> list[Any]:
    if value in (None, ""):
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, (list, tuple, set)):
        return [item for item in value if item not in (None, "")]
    return [value]


def _combine_filters(filters: list[FilterExpr | None]) -> FilterExpr | None:
    items = [filter_expr for filter_expr in filters if filter_expr is not None]
    if not items:
        return None
    if len(items) == 1:
        return items[0]
    return and_(*items)


class AssetSimpleTable(SimpleTable):
    """Base asset row for the SimpleTable-backed asset catalog."""

    unique_identifier: Annotated[constr(max_length=255), Index(unique=True)] = Field(
        ...,
        description="Stable asset business identifier within the selected asset master list.",
    )

    @classmethod
    def filter_expr(
        cls,
        *,
        unique_identifier: str | None = None,
        unique_identifier__in: list[str] | tuple[str, ...] | str | None = None,
        unique_identifier__contains: str | None = None,
        search: str | None = None,
    ) -> FilterExpr | None:
        """Build the canonical filter expression for AssetSimpleTable rows."""

        filters: list[FilterExpr | None] = []
        if unique_identifier not in (None, ""):
            filters.append(cls.f.unique_identifier.eq(str(unique_identifier)))

        unique_identifiers = _split_values(unique_identifier__in)
        if unique_identifiers:
            filters.append(
                cls.f.unique_identifier.in_(
                    [str(identifier) for identifier in unique_identifiers]
                )
            )

        if unique_identifier__contains not in (None, ""):
            filters.append(cls.f.unique_identifier.contains(str(unique_identifier__contains)))

        normalized_search = str(search or "").strip()
        if normalized_search:
            filters.append(cls.f.unique_identifier.contains(normalized_search))

        return _combine_filters(filters)

    def get_categories(
        self,
        membership_updater: Any,
        *,
        limit: int = 500,
        offset: int = 0,
    ) -> list[Any]:
        """Return categories linked to this asset through the membership table."""

        return membership_updater.list_categories_for_asset(
            asset=self,
            limit=limit,
            offset=offset,
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

    def filter_assets(
        self,
        *,
        membership_updater: Any | None = None,
        category: Any | None = None,
        category_id: int | None = None,
        category_ids: list[int] | tuple[int, ...] | None = None,
        category_uid: str | None = None,
        category_uid__in: list[str] | tuple[str, ...] | str | None = None,
        categories__uid: str | None = None,
        categories__uid__in: list[str] | tuple[str, ...] | str | None = None,
        category_unique_identifier: str | None = None,
        category_unique_identifier__in: list[str] | tuple[str, ...] | str | None = None,
        unique_identifier: str | None = None,
        unique_identifier__in: list[str] | tuple[str, ...] | str | None = None,
        unique_identifier__contains: str | None = None,
        search: str | None = None,
        limit: int = 500,
        offset: int = 0,
    ) -> list[AssetSimpleTable]:
        resolved_category_uid = category_uid or categories__uid
        resolved_category_uid__in = category_uid__in or categories__uid__in
        has_category_filter = any(
            value not in (None, "")
            for value in (
                category,
                category_id,
                category_ids,
                resolved_category_uid,
                resolved_category_uid__in,
                category_unique_identifier,
                category_unique_identifier__in,
            )
        )
        if has_category_filter:
            if membership_updater is None:
                raise ValueError(
                    "membership_updater is required when filtering assets by category."
                )
            return membership_updater.list_assets_for_category(
                category=category,
                category_id=category_id,
                category_ids=category_ids,
                category_uid=resolved_category_uid,
                category_uid__in=resolved_category_uid__in,
                category_unique_identifier=category_unique_identifier,
                category_unique_identifier__in=category_unique_identifier__in,
                unique_identifier=unique_identifier,
                unique_identifier__in=unique_identifier__in,
                unique_identifier__contains=unique_identifier__contains,
                search=search,
                limit=limit,
                offset=offset,
            )

        return self.execute_filter(
            AssetSimpleTable.filter_expr(
                unique_identifier=unique_identifier,
                unique_identifier__in=unique_identifier__in,
                unique_identifier__contains=unique_identifier__contains,
                search=search,
            ),
            limit=limit,
            offset=offset,
        )

    def get_by_unique_identifier(
        self,
        unique_identifier: str,
    ) -> AssetSimpleTable | None:
        rows = self.filter_assets(unique_identifier=unique_identifier, limit=1)
        return rows[0] if rows else None

    def require_by_unique_identifier(
        self,
        unique_identifier: str,
    ) -> AssetSimpleTable:
        row = self.get_by_unique_identifier(unique_identifier)
        if row is None:
            raise ValueError(f"Asset '{unique_identifier}' was not found.")
        return row


__all__ = [
    "DEFAULT_MASTER_LIST_DEPENDENCY",
    "AssetSimpleTable",
    "AssetSimpleTableConfiguration",
    "AssetSimpleTableUpdater",
]
