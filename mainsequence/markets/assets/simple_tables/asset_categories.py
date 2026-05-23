from __future__ import annotations

import uuid
from typing import Annotated, Any

from pydantic import Field, constr

from mainsequence.tdag.simple_tables import (
    Index,
    SimpleTable,
    SimpleTableUpdater,
    SimpleTableUpdaterConfiguration,
    and_,
    or_,
)
from mainsequence.tdag.simple_tables.filters import FilterExpr

ASSET_CATEGORY_TABLE_DEPENDENCY = "asset_category_table"


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


class AssetCategorySimpleTable(SimpleTable):
    """Client-owned asset category row."""

    uid: Annotated[constr(max_length=36), Index(unique=True)] = Field(
        ...,
        description="Client-generated public UID for the category row.",
    )
    unique_identifier: Annotated[constr(max_length=255), Index(unique=True)] = Field(
        ...,
        description="Stable category business identifier within the selected asset catalog.",
    )
    display_name: constr(max_length=255) = Field(
        ...,
        description="Human-readable category name.",
    )
    description: str | None = Field(
        None,
        description="Optional category description.",
    )
    metadata_json: str | None = Field(
        None,
        description="Optional JSON-encoded category metadata owned by the client.",
    )

    @classmethod
    def filter_expr(
        cls,
        *,
        uid: str | None = None,
        uid__in: list[str] | tuple[str, ...] | str | None = None,
        unique_identifier: str | None = None,
        unique_identifier__in: list[str] | tuple[str, ...] | str | None = None,
        unique_identifier__contains: str | None = None,
        display_name: str | None = None,
        display_name__in: list[str] | tuple[str, ...] | str | None = None,
        display_name__contains: str | None = None,
        description: str | None = None,
        description__contains: str | None = None,
        search: str | None = None,
    ) -> FilterExpr | None:
        """Build the canonical filter expression for category rows."""

        filters: list[FilterExpr | None] = []
        if uid not in (None, ""):
            filters.append(cls.f.uid.eq(str(uid)))

        uids = _split_values(uid__in)
        if uids:
            filters.append(cls.f.uid.in_([str(value) for value in uids]))

        if unique_identifier not in (None, ""):
            filters.append(cls.f.unique_identifier.eq(str(unique_identifier)))

        unique_identifiers = _split_values(unique_identifier__in)
        if unique_identifiers:
            filters.append(
                cls.f.unique_identifier.in_(
                    [str(value) for value in unique_identifiers]
                )
            )

        if unique_identifier__contains not in (None, ""):
            filters.append(cls.f.unique_identifier.contains(str(unique_identifier__contains)))

        if display_name not in (None, ""):
            filters.append(cls.f.display_name.eq(str(display_name)))

        display_names = _split_values(display_name__in)
        if display_names:
            filters.append(cls.f.display_name.in_([str(value) for value in display_names]))

        if display_name__contains not in (None, ""):
            filters.append(cls.f.display_name.contains(str(display_name__contains)))

        if description not in (None, ""):
            filters.append(cls.f.description.eq(str(description)))

        if description__contains not in (None, ""):
            filters.append(cls.f.description.contains(str(description__contains)))

        normalized_search = str(search or "").strip()
        if normalized_search:
            filters.append(
                or_(
                    cls.f.display_name.contains(normalized_search),
                    cls.f.unique_identifier.contains(normalized_search),
                    cls.f.description.contains(normalized_search),
                )
            )

        return _combine_filters(filters)

    def get_assets(
        self,
        membership_updater: Any,
        *,
        limit: int = 500,
        offset: int = 0,
    ) -> list[Any]:
        """Return assets linked to this category through the membership table."""

        return membership_updater.list_assets_for_category(
            category=self,
            limit=limit,
            offset=offset,
        )

    def append_assets(
        self,
        membership_updater: Any,
        *,
        assets: list[Any] | tuple[Any, ...] | None = None,
        asset_ids: list[int] | tuple[int, ...] | None = None,
        asset_unique_identifiers: list[str] | tuple[str, ...] | None = None,
    ) -> list[Any]:
        """Append assets to this category without removing existing memberships."""

        return membership_updater.append_assets(
            category=self,
            assets=assets,
            asset_ids=asset_ids,
            asset_unique_identifiers=asset_unique_identifiers,
        )

    def remove_assets(
        self,
        membership_updater: Any,
        *,
        assets: list[Any] | tuple[Any, ...] | None = None,
        asset_ids: list[int] | tuple[int, ...] | None = None,
        asset_unique_identifiers: list[str] | tuple[str, ...] | None = None,
    ) -> list[Any]:
        """Remove assets from this category."""

        return membership_updater.remove_assets(
            category=self,
            assets=assets,
            asset_ids=asset_ids,
            asset_unique_identifiers=asset_unique_identifiers,
        )

    def set_assets(
        self,
        membership_updater: Any,
        *,
        assets: list[Any] | tuple[Any, ...] | None = None,
        asset_ids: list[int] | tuple[int, ...] | None = None,
        asset_unique_identifiers: list[str] | tuple[str, ...] | None = None,
    ) -> list[Any]:
        """Replace the category membership set with the provided assets."""

        return membership_updater.set_assets(
            category=self,
            assets=assets,
            asset_ids=asset_ids,
            asset_unique_identifiers=asset_unique_identifiers,
        )


class AssetCategorySimpleTableConfiguration(SimpleTableUpdaterConfiguration):
    """Configuration for the canonical asset category table."""

    identifier: str = Field(
        "mainsequence.markets.asset_categories",
        description="Stable SimpleTable identifier for asset category rows.",
    )


class AssetCategorySimpleTableUpdater(SimpleTableUpdater):
    """SimpleTable updater for asset category rows."""

    SIMPLE_TABLE_SCHEMA = AssetCategorySimpleTable

    def update(self) -> list[AssetCategorySimpleTable]:
        return []

    def filter_categories(
        self,
        *,
        uid: str | None = None,
        uid__in: list[str] | tuple[str, ...] | str | None = None,
        unique_identifier: str | None = None,
        unique_identifier__in: list[str] | tuple[str, ...] | str | None = None,
        unique_identifier__contains: str | None = None,
        display_name: str | None = None,
        display_name__in: list[str] | tuple[str, ...] | str | None = None,
        display_name__contains: str | None = None,
        description: str | None = None,
        description__contains: str | None = None,
        search: str | None = None,
        limit: int = 500,
        offset: int = 0,
    ) -> list[AssetCategorySimpleTable]:
        return self.execute_filter(
            AssetCategorySimpleTable.filter_expr(
                uid=uid,
                uid__in=uid__in,
                unique_identifier=unique_identifier,
                unique_identifier__in=unique_identifier__in,
                unique_identifier__contains=unique_identifier__contains,
                display_name=display_name,
                display_name__in=display_name__in,
                display_name__contains=display_name__contains,
                description=description,
                description__contains=description__contains,
                search=search,
            ),
            limit=limit,
            offset=offset,
        )

    def get_by_uid(self, uid: str) -> AssetCategorySimpleTable | None:
        rows = self.filter_categories(uid=uid, limit=1)
        return rows[0] if rows else None

    def get_by_unique_identifier(
        self,
        unique_identifier: str,
    ) -> AssetCategorySimpleTable | None:
        rows = self.filter_categories(unique_identifier=unique_identifier, limit=1)
        return rows[0] if rows else None

    def require_by_uid(self, uid: str) -> AssetCategorySimpleTable:
        row = self.get_by_uid(uid)
        if row is None:
            raise ValueError(f"Asset category UID '{uid}' was not found.")
        return row

    def require_by_unique_identifier(
        self,
        unique_identifier: str,
    ) -> AssetCategorySimpleTable:
        row = self.get_by_unique_identifier(unique_identifier)
        if row is None:
            raise ValueError(f"Asset category '{unique_identifier}' was not found.")
        return row

    def get_or_create(
        self,
        *,
        unique_identifier: str,
        display_name: str | None = None,
        uid: str | None = None,
        description: str | None = None,
        metadata_json: str | None = None,
    ) -> AssetCategorySimpleTable:
        existing = self.get_by_unique_identifier(unique_identifier)
        if existing is not None:
            return existing

        category = AssetCategorySimpleTable(
            uid=str(uid or uuid.uuid4()),
            unique_identifier=unique_identifier,
            display_name=display_name or unique_identifier,
            description=description,
            metadata_json=metadata_json,
        )
        self.insert(category)
        return self.require_by_unique_identifier(unique_identifier)


__all__ = [
    "ASSET_CATEGORY_TABLE_DEPENDENCY",
    "AssetCategorySimpleTable",
    "AssetCategorySimpleTableConfiguration",
    "AssetCategorySimpleTableUpdater",
]
