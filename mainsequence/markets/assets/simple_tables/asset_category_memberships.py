from __future__ import annotations

from typing import Annotated, Any

from pydantic import Field

from mainsequence.tdag.simple_tables import (
    ForeignKey,
    Index,
    SimpleTable,
    SimpleTableUpdater,
    SimpleTableUpdaterConfiguration,
    and_,
)
from mainsequence.tdag.simple_tables.filters import FilterExpr

from .asset_categories import (
    ASSET_CATEGORY_TABLE_DEPENDENCY,
    AssetCategorySimpleTable,
    AssetCategorySimpleTableUpdater,
)
from .assets import DEFAULT_MASTER_LIST_DEPENDENCY, AssetSimpleTable, AssetSimpleTableUpdater


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


def _record_id(record_or_id: Any, label: str) -> int:
    if isinstance(record_or_id, int):
        return record_or_id
    record_id = getattr(record_or_id, "id", None)
    if record_id is None:
        raise ValueError(f"{label} must be a row id or a row with an id.")
    return int(record_id)


def _unique_ints(values: list[int]) -> list[int]:
    seen: set[int] = set()
    unique_values: list[int] = []
    for value in values:
        normalized = int(value)
        if normalized in seen:
            continue
        seen.add(normalized)
        unique_values.append(normalized)
    return unique_values


class AssetCategoryMembershipSimpleTable(SimpleTable):
    """Client-owned join row between one category and one asset."""

    category: Annotated[
        int,
        ForeignKey(ASSET_CATEGORY_TABLE_DEPENDENCY, on_delete="cascade"),
        Index(),
    ] = Field(..., description="Backend-managed row id from AssetCategorySimpleTable.")
    asset: Annotated[
        int,
        ForeignKey(DEFAULT_MASTER_LIST_DEPENDENCY, on_delete="cascade"),
        Index(),
    ] = Field(..., description="Backend-managed row id from AssetSimpleTable.")

    @classmethod
    def filter_expr(
        cls,
        *,
        category: int | None = None,
        category__in: list[int] | tuple[int, ...] | str | None = None,
        asset: int | None = None,
        asset__in: list[int] | tuple[int, ...] | str | None = None,
    ) -> FilterExpr | None:
        """Build the canonical filter expression for membership rows."""

        filters: list[FilterExpr | None] = []
        if category is not None:
            filters.append(cls.f.category.eq(int(category)))

        category_ids = _split_values(category__in)
        if category_ids:
            filters.append(cls.f.category.in_([int(value) for value in category_ids]))

        if asset is not None:
            filters.append(cls.f.asset.eq(int(asset)))

        asset_ids = _split_values(asset__in)
        if asset_ids:
            filters.append(cls.f.asset.in_([int(value) for value in asset_ids]))

        return _combine_filters(filters)


class AssetCategoryMembershipSimpleTableConfiguration(SimpleTableUpdaterConfiguration):
    """Configuration for the canonical asset/category membership table."""

    identifier: str = Field(
        "mainsequence.markets.asset_category_memberships",
        description="Stable SimpleTable identifier for asset/category membership rows.",
    )


class AssetCategoryMembershipSimpleTableUpdater(SimpleTableUpdater):
    """SimpleTable updater for asset/category membership rows."""

    SIMPLE_TABLE_SCHEMA = AssetCategoryMembershipSimpleTable

    def __init__(
        self,
        configuration: AssetCategoryMembershipSimpleTableConfiguration,
        *,
        asset_updater: AssetSimpleTableUpdater,
        category_updater: AssetCategorySimpleTableUpdater,
        **kwargs: Any,
    ):
        self.asset_updater = asset_updater
        self.category_updater = category_updater
        super().__init__(configuration=configuration, **kwargs)

    def dependencies(self) -> dict[str, SimpleTableUpdater]:
        return {
            DEFAULT_MASTER_LIST_DEPENDENCY: self.asset_updater,
            ASSET_CATEGORY_TABLE_DEPENDENCY: self.category_updater,
        }

    def update(self) -> list[AssetCategoryMembershipSimpleTable]:
        return []

    def resolve_category_id(
        self,
        *,
        category: AssetCategorySimpleTable | int | None = None,
        category_id: int | None = None,
        category_uid: str | None = None,
        category_unique_identifier: str | None = None,
    ) -> int:
        if category is not None:
            return _record_id(category, "category")
        if category_id is not None:
            return int(category_id)
        if category_uid not in (None, ""):
            return _record_id(
                self.category_updater.require_by_uid(str(category_uid)),
                "category",
            )
        if category_unique_identifier not in (None, ""):
            return _record_id(
                self.category_updater.require_by_unique_identifier(
                    str(category_unique_identifier)
                ),
                "category",
            )
        raise ValueError(
            "category, category_id, category_uid, or "
            "category_unique_identifier is required."
        )

    def resolve_category_ids(
        self,
        *,
        category: AssetCategorySimpleTable | int | None = None,
        category_id: int | None = None,
        category_ids: list[int] | tuple[int, ...] | None = None,
        category_uid: str | None = None,
        category_uid__in: list[str] | tuple[str, ...] | str | None = None,
        category_unique_identifier: str | None = None,
        category_unique_identifier__in: list[str] | tuple[str, ...] | str | None = None,
        require_non_empty: bool = True,
    ) -> list[int]:
        resolved_ids: list[int] = []
        if any(
            value not in (None, "")
            for value in (category, category_id, category_uid, category_unique_identifier)
        ):
            resolved_ids.append(
                self.resolve_category_id(
                    category=category,
                    category_id=category_id,
                    category_uid=category_uid,
                    category_unique_identifier=category_unique_identifier,
                )
            )

        for value in category_ids or ():
            resolved_ids.append(int(value))

        category_uids = [str(value) for value in _split_values(category_uid__in)]
        if category_uids:
            rows = self.category_updater.filter_categories(
                uid__in=category_uids,
                limit=500,
            )
            rows_by_uid = {row.uid: row for row in rows}
            missing = [uid for uid in category_uids if uid not in rows_by_uid]
            if missing:
                raise ValueError(
                    "Some asset category UIDs were not found: "
                    + ", ".join(sorted(missing))
                )
            resolved_ids.extend(_record_id(rows_by_uid[uid], "category") for uid in category_uids)

        category_unique_identifiers = [
            str(value) for value in _split_values(category_unique_identifier__in)
        ]
        if category_unique_identifiers:
            rows = self.category_updater.filter_categories(
                unique_identifier__in=category_unique_identifiers,
                limit=500,
            )
            rows_by_unique_identifier = {row.unique_identifier: row for row in rows}
            missing = [
                unique_identifier
                for unique_identifier in category_unique_identifiers
                if unique_identifier not in rows_by_unique_identifier
            ]
            if missing:
                raise ValueError(
                    "Some asset category unique identifiers were not found: "
                    + ", ".join(sorted(missing))
                )
            resolved_ids.extend(
                _record_id(rows_by_unique_identifier[unique_identifier], "category")
                for unique_identifier in category_unique_identifiers
            )

        resolved_ids = _unique_ints(resolved_ids)
        if require_non_empty and not resolved_ids:
            raise ValueError(
                "category, category_id, category_ids, category_uid, "
                "category_uid__in, category_unique_identifier, or "
                "category_unique_identifier__in is required."
            )
        return resolved_ids

    def resolve_asset_ids(
        self,
        *,
        assets: list[AssetSimpleTable | int] | tuple[AssetSimpleTable | int, ...] | None = None,
        asset_ids: list[int] | tuple[int, ...] | None = None,
        asset_unique_identifiers: list[str] | tuple[str, ...] | str | None = None,
        require_non_empty: bool = True,
    ) -> list[int]:
        resolved_ids: list[int] = []
        for asset in assets or ():
            resolved_ids.append(_record_id(asset, "asset"))
        for asset_id in asset_ids or ():
            resolved_ids.append(int(asset_id))

        unique_identifiers = [str(value) for value in _split_values(asset_unique_identifiers)]
        if unique_identifiers:
            rows = self.asset_updater.filter_assets(
                unique_identifier__in=unique_identifiers,
                limit=500,
            )
            rows_by_unique_identifier = {row.unique_identifier: row for row in rows}
            missing = [
                unique_identifier
                for unique_identifier in unique_identifiers
                if unique_identifier not in rows_by_unique_identifier
            ]
            if missing:
                raise ValueError(
                    "Some asset unique identifiers were not found: "
                    + ", ".join(sorted(missing))
                )
            resolved_ids.extend(
                _record_id(rows_by_unique_identifier[unique_identifier], "asset")
                for unique_identifier in unique_identifiers
            )

        resolved_ids = _unique_ints(resolved_ids)
        if require_non_empty and not resolved_ids:
            raise ValueError(
                "assets, asset_ids, or asset_unique_identifiers must be provided."
            )
        return resolved_ids

    def list_memberships(
        self,
        *,
        category: AssetCategorySimpleTable | int | None = None,
        category_id: int | None = None,
        category_ids: list[int] | tuple[int, ...] | None = None,
        category_uid: str | None = None,
        category_uid__in: list[str] | tuple[str, ...] | str | None = None,
        category_unique_identifier: str | None = None,
        category_unique_identifier__in: list[str] | tuple[str, ...] | str | None = None,
        asset: AssetSimpleTable | int | None = None,
        asset_id: int | None = None,
        asset_unique_identifier: str | None = None,
        asset_ids: list[int] | tuple[int, ...] | None = None,
        limit: int = 500,
        offset: int = 0,
    ) -> list[AssetCategoryMembershipSimpleTable]:
        if any(
            value not in (None, "")
            for value in (
                category,
                category_id,
                category_ids,
                category_uid,
                category_uid__in,
                category_unique_identifier,
                category_unique_identifier__in,
            )
        ):
            resolved_category_ids = self.resolve_category_ids(
                category=category,
                category_id=category_id,
                category_ids=category_ids,
                category_uid=category_uid,
                category_uid__in=category_uid__in,
                category_unique_identifier=category_unique_identifier,
                category_unique_identifier__in=category_unique_identifier__in,
            )
        else:
            resolved_category_ids = []

        resolved_asset_ids: list[int] = []
        if asset is not None:
            resolved_asset_ids.append(_record_id(asset, "asset"))
        if asset_id is not None:
            resolved_asset_ids.append(int(asset_id))
        if asset_ids is not None:
            resolved_asset_ids.extend(int(value) for value in asset_ids)
        if asset_unique_identifier not in (None, ""):
            resolved_asset_ids.extend(
                self.resolve_asset_ids(
                    asset_unique_identifiers=[str(asset_unique_identifier)],
                )
            )
        resolved_asset_ids = _unique_ints(resolved_asset_ids)

        filter_expr = AssetCategoryMembershipSimpleTable.filter_expr(
            category=resolved_category_ids[0] if len(resolved_category_ids) == 1 else None,
            category__in=resolved_category_ids if len(resolved_category_ids) > 1 else None,
            asset__in=resolved_asset_ids or None,
        )
        return self.execute_filter(filter_expr, limit=limit, offset=offset)

    def list_assets_for_category(
        self,
        *,
        category: AssetCategorySimpleTable | int | None = None,
        category_id: int | None = None,
        category_ids: list[int] | tuple[int, ...] | None = None,
        category_uid: str | None = None,
        category_uid__in: list[str] | tuple[str, ...] | str | None = None,
        category_unique_identifier: str | None = None,
        category_unique_identifier__in: list[str] | tuple[str, ...] | str | None = None,
        unique_identifier: str | None = None,
        unique_identifier__in: list[str] | tuple[str, ...] | str | None = None,
        unique_identifier__contains: str | None = None,
        search: str | None = None,
        limit: int = 500,
        offset: int = 0,
    ) -> list[AssetSimpleTable]:
        resolved_category_ids = self.resolve_category_ids(
            category=category,
            category_id=category_id,
            category_ids=category_ids,
            category_uid=category_uid,
            category_uid__in=category_uid__in,
            category_unique_identifier=category_unique_identifier,
            category_unique_identifier__in=category_unique_identifier__in,
        )
        self._ensure_remote_objects_ready(require_update=False)

        membership_alias = "membership"
        membership = AssetCategoryMembershipSimpleTable.ref(membership_alias)
        membership_join = AssetCategoryMembershipSimpleTable.join(
            membership_alias,
            storage_hash=self.storage_hash,
        )
        filter_expr = _combine_filters(
            [
                membership.f.category.eq(resolved_category_ids[0])
                if len(resolved_category_ids) == 1
                else membership.f.category.in_(resolved_category_ids),
                AssetSimpleTable.filter_expr(
                    unique_identifier=unique_identifier,
                    unique_identifier__in=unique_identifier__in,
                    unique_identifier__contains=unique_identifier__contains,
                    search=search,
                ),
            ]
        )
        return self.asset_updater.execute_filter(
            filter_expr,
            joins=[membership_join],
            limit=limit,
            offset=offset,
        )

    def list_categories_for_asset(
        self,
        *,
        asset: AssetSimpleTable | int | None = None,
        asset_id: int | None = None,
        asset_unique_identifier: str | None = None,
        uid: str | None = None,
        uid__in: list[str] | tuple[str, ...] | str | None = None,
        unique_identifier: str | None = None,
        unique_identifier__in: list[str] | tuple[str, ...] | str | None = None,
        unique_identifier__contains: str | None = None,
        display_name: str | None = None,
        display_name__contains: str | None = None,
        search: str | None = None,
        limit: int = 500,
        offset: int = 0,
    ) -> list[AssetCategorySimpleTable]:
        resolved_asset_ids = []
        if asset is not None:
            resolved_asset_ids.append(_record_id(asset, "asset"))
        if asset_id is not None:
            resolved_asset_ids.append(int(asset_id))
        if asset_unique_identifier not in (None, ""):
            resolved_asset_ids.extend(
                self.resolve_asset_ids(
                    asset_unique_identifiers=[str(asset_unique_identifier)],
                )
            )
        resolved_asset_ids = _unique_ints(resolved_asset_ids)
        if not resolved_asset_ids:
            raise ValueError("asset, asset_id, or asset_unique_identifier is required.")

        self._ensure_remote_objects_ready(require_update=False)

        membership_alias = "membership"
        membership = AssetCategoryMembershipSimpleTable.ref(membership_alias)
        membership_join = AssetCategoryMembershipSimpleTable.join(
            membership_alias,
            storage_hash=self.storage_hash,
        )
        filter_expr = _combine_filters(
            [
                membership.f.asset.in_(resolved_asset_ids),
                AssetCategorySimpleTable.filter_expr(
                    uid=uid,
                    uid__in=uid__in,
                    unique_identifier=unique_identifier,
                    unique_identifier__in=unique_identifier__in,
                    unique_identifier__contains=unique_identifier__contains,
                    display_name=display_name,
                    display_name__contains=display_name__contains,
                    search=search,
                ),
            ]
        )
        return self.category_updater.execute_filter(
            filter_expr,
            joins=[membership_join],
            limit=limit,
            offset=offset,
        )

    def append_assets(
        self,
        *,
        category: AssetCategorySimpleTable | int | None = None,
        category_id: int | None = None,
        category_uid: str | None = None,
        category_unique_identifier: str | None = None,
        assets: list[AssetSimpleTable | int] | tuple[AssetSimpleTable | int, ...] | None = None,
        asset_ids: list[int] | tuple[int, ...] | None = None,
        asset_unique_identifiers: list[str] | tuple[str, ...] | str | None = None,
    ) -> list[AssetSimpleTable]:
        resolved_category_id = self.resolve_category_id(
            category=category,
            category_id=category_id,
            category_uid=category_uid,
            category_unique_identifier=category_unique_identifier,
        )
        resolved_asset_ids = self.resolve_asset_ids(
            assets=assets,
            asset_ids=asset_ids,
            asset_unique_identifiers=asset_unique_identifiers,
        )
        existing_memberships = self.list_memberships(
            category_id=resolved_category_id,
            asset_ids=resolved_asset_ids,
            limit=500,
        )
        existing_asset_ids = {membership.asset for membership in existing_memberships}
        new_memberships = [
            AssetCategoryMembershipSimpleTable(
                category=resolved_category_id,
                asset=asset_id,
            )
            for asset_id in resolved_asset_ids
            if asset_id not in existing_asset_ids
        ]
        if new_memberships:
            self.insert_records(new_memberships)
        return self.list_assets_for_category(category_id=resolved_category_id)

    def remove_assets(
        self,
        *,
        category: AssetCategorySimpleTable | int | None = None,
        category_id: int | None = None,
        category_uid: str | None = None,
        category_unique_identifier: str | None = None,
        assets: list[AssetSimpleTable | int] | tuple[AssetSimpleTable | int, ...] | None = None,
        asset_ids: list[int] | tuple[int, ...] | None = None,
        asset_unique_identifiers: list[str] | tuple[str, ...] | str | None = None,
    ) -> list[AssetSimpleTable]:
        resolved_category_id = self.resolve_category_id(
            category=category,
            category_id=category_id,
            category_uid=category_uid,
            category_unique_identifier=category_unique_identifier,
        )
        resolved_asset_ids = self.resolve_asset_ids(
            assets=assets,
            asset_ids=asset_ids,
            asset_unique_identifiers=asset_unique_identifiers,
        )
        memberships = self.list_memberships(
            category_id=resolved_category_id,
            asset_ids=resolved_asset_ids,
            limit=500,
        )
        for membership in memberships:
            self.delete(membership)
        return self.list_assets_for_category(category_id=resolved_category_id)

    def set_assets(
        self,
        *,
        category: AssetCategorySimpleTable | int | None = None,
        category_id: int | None = None,
        category_uid: str | None = None,
        category_unique_identifier: str | None = None,
        assets: list[AssetSimpleTable | int] | tuple[AssetSimpleTable | int, ...] | None = None,
        asset_ids: list[int] | tuple[int, ...] | None = None,
        asset_unique_identifiers: list[str] | tuple[str, ...] | str | None = None,
    ) -> list[AssetSimpleTable]:
        resolved_category_id = self.resolve_category_id(
            category=category,
            category_id=category_id,
            category_uid=category_uid,
            category_unique_identifier=category_unique_identifier,
        )
        target_asset_ids = set(
            self.resolve_asset_ids(
                assets=assets,
                asset_ids=asset_ids,
                asset_unique_identifiers=asset_unique_identifiers,
                require_non_empty=False,
            )
        )
        current_memberships = self.list_memberships(
            category_id=resolved_category_id,
            limit=500,
        )
        current_asset_ids = {membership.asset for membership in current_memberships}

        for membership in current_memberships:
            if membership.asset not in target_asset_ids:
                self.delete(membership)

        missing_asset_ids = sorted(target_asset_ids - current_asset_ids)
        if missing_asset_ids:
            self.insert_records(
                [
                    AssetCategoryMembershipSimpleTable(
                        category=resolved_category_id,
                        asset=asset_id,
                    )
                    for asset_id in missing_asset_ids
                ]
            )
        return self.list_assets_for_category(category_id=resolved_category_id)


__all__ = [
    "AssetCategoryMembershipSimpleTable",
    "AssetCategoryMembershipSimpleTableConfiguration",
    "AssetCategoryMembershipSimpleTableUpdater",
]
