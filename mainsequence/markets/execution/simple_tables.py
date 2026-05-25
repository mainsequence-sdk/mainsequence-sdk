from __future__ import annotations

from collections.abc import Callable
from typing import Annotated, Any

from pydantic import BaseModel, Field, constr

from mainsequence.markets.accounts.simple_tables import (
    ACCOUNT_TABLE_DEPENDENCY,
    Account,
    AccountUpdater,
)
from mainsequence.tdag.simple_tables import (
    ForeignKey,
    Index,
    SimpleTable,
    SimpleTableUpdater,
    SimpleTableUpdaterConfiguration,
    and_,
    or_,
)
from mainsequence.tdag.simple_tables.filters import FilterExpr

ORDER_MANAGER_TABLE_DEPENDENCY = "order_managers"
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


class ExecutionDeleteResult(BaseModel):
    """Result summary for execution SimpleTable delete helpers."""

    requested_identifiers: list[str] = Field(default_factory=list)
    deleted_identifiers: list[str] = Field(default_factory=list)
    missing_identifiers: list[str] = Field(default_factory=list)
    dependent_delete_results: list[Any] = Field(default_factory=list)

    @property
    def deleted_count(self) -> int:
        return len(self.deleted_identifiers)


class ExecutionDependentDeleteResult(BaseModel):
    """Result summary for dependent execution-row delete helpers."""

    requested_identifiers: list[str] = Field(default_factory=list)
    deleted_count: int = 0
    missing_identifiers: list[str] = Field(default_factory=list)


class OrderManager(SimpleTable):
    """Client-owned order manager / rebalance batch row."""

    unique_identifier: Annotated[constr(max_length=255), Index(unique=True)] = Field(
        ...,
        description="Stable order-manager business identifier.",
    )
    target_account_id: Annotated[
        int,
        ForeignKey(ACCOUNT_TABLE_DEPENDENCY, on_delete="cascade"),
        Index(),
    ] = Field(..., description="Backend-managed row id from Account(SimpleTable).")
    target_time: constr(max_length=64) = Field(
        ...,
        description="Target execution timestamp encoded as an ISO-8601 string.",
    )
    order_received_time: constr(max_length=64) | None = Field(
        None,
        description="Order-received timestamp encoded as an ISO-8601 string.",
    )
    execution_end: constr(max_length=64) | None = Field(
        None,
        description="Execution-end timestamp encoded as an ISO-8601 string.",
    )
    status: constr(max_length=64) | None = Field(
        None,
        description="Client-owned order-manager lifecycle status.",
    )
    metadata_json: str | None = Field(
        None,
        description="Optional JSON-encoded order-manager metadata.",
    )

    @classmethod
    def filter_expr(
        cls,
        *,
        unique_identifier: str | None = None,
        unique_identifier__in: list[str] | tuple[str, ...] | str | None = None,
        unique_identifier__contains: str | None = None,
        target_account_id: int | None = None,
        target_time: str | None = None,
        status: str | None = None,
        search: str | None = None,
    ) -> FilterExpr | None:
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
        if target_account_id is not None:
            filters.append(cls.f.target_account_id.eq(int(target_account_id)))
        if target_time not in (None, ""):
            filters.append(cls.f.target_time.eq(str(target_time)))
        if status not in (None, ""):
            filters.append(cls.f.status.eq(str(status)))
        normalized_search = str(search or "").strip()
        if normalized_search:
            filters.append(
                or_(
                    cls.f.unique_identifier.contains(normalized_search),
                    cls.f.status.contains(normalized_search),
                )
            )
        return _combine_filters(filters)


class OrderManagerConfiguration(SimpleTableUpdaterConfiguration):
    """Configuration for the client-owned order-manager table."""

    identifier: str = Field(
        "mainsequence.markets.execution.order_managers",
        description="Stable SimpleTable identifier for execution order managers.",
    )


class OrderManagerUpdater(SimpleTableUpdater):
    """SimpleTable updater for order-manager rows."""

    SIMPLE_TABLE_SCHEMA = OrderManager

    def __init__(
        self,
        configuration: OrderManagerConfiguration,
        *,
        account_updater: AccountUpdater,
        **kwargs: Any,
    ):
        self.account_updater = account_updater
        super().__init__(configuration=configuration, **kwargs)

    def dependencies(self) -> dict[str, SimpleTableUpdater]:
        return {ACCOUNT_TABLE_DEPENDENCY: self.account_updater}

    def update(self) -> list[OrderManager]:
        return []

    def filter_order_managers(
        self,
        *,
        unique_identifier: str | None = None,
        unique_identifier__in: list[str] | tuple[str, ...] | str | None = None,
        unique_identifier__contains: str | None = None,
        target_account: Account | int | None = None,
        target_account_id: int | None = None,
        target_account_unique_identifier: str | None = None,
        target_time: str | None = None,
        status: str | None = None,
        search: str | None = None,
        limit: int = 500,
        offset: int = 0,
    ) -> list[OrderManager]:
        resolved_account_id = _resolve_account_id(
            account_updater=self.account_updater,
            account=target_account,
            account_id=target_account_id,
            account_unique_identifier=target_account_unique_identifier,
            required=False,
        )
        return self.execute_filter(
            OrderManager.filter_expr(
                unique_identifier=unique_identifier,
                unique_identifier__in=unique_identifier__in,
                unique_identifier__contains=unique_identifier__contains,
                target_account_id=resolved_account_id,
                target_time=target_time,
                status=status,
                search=search,
            ),
            limit=limit,
            offset=offset,
        )

    def get_by_unique_identifier(self, unique_identifier: str) -> OrderManager | None:
        rows = self.filter_order_managers(unique_identifier=unique_identifier, limit=1)
        return rows[0] if rows else None

    def require_by_unique_identifier(self, unique_identifier: str) -> OrderManager:
        row = self.get_by_unique_identifier(unique_identifier)
        if row is None:
            raise ValueError(f"OrderManager '{unique_identifier}' was not found.")
        return row

    def create_or_update_order_manager(
        self,
        *,
        unique_identifier: str,
        target_account: Account | int | None = None,
        target_account_id: int | None = None,
        target_account_unique_identifier: str | None = None,
        target_time: str,
        order_received_time: str | None = None,
        execution_end: str | None = None,
        status: str | None = None,
        metadata_json: str | None = None,
    ) -> OrderManager:
        resolved_account_id = _resolve_account_id(
            account_updater=self.account_updater,
            account=target_account,
            account_id=target_account_id,
            account_unique_identifier=target_account_unique_identifier,
            required=True,
        )
        existing = self.get_by_unique_identifier(unique_identifier)
        row = OrderManager(
            id=getattr(existing, "id", None),
            unique_identifier=str(unique_identifier),
            target_account_id=resolved_account_id,
            target_time=str(target_time),
            order_received_time=order_received_time,
            execution_end=execution_end,
            status=status,
            metadata_json=metadata_json,
        )
        self.upsert(row)
        return self.get_by_unique_identifier(unique_identifier) or row

    def delete_order_managers(
        self,
        *,
        unique_identifiers: list[str] | tuple[str, ...] | str,
        approved_dependent_delete_callbacks: list[DependentDeleteCallback]
        | tuple[DependentDeleteCallback, ...]
        | None = None,
    ) -> ExecutionDeleteResult:
        return _delete_rows_by_unique_identifier(
            updater=self,
            row_getter=self.filter_order_managers,
            unique_identifiers=unique_identifiers,
            approved_dependent_delete_callbacks=approved_dependent_delete_callbacks,
        )


class OrderTargetQuantity(SimpleTable):
    """Client-owned target quantity row for one order-manager batch."""

    order_manager_id: Annotated[
        int,
        ForeignKey(ORDER_MANAGER_TABLE_DEPENDENCY, on_delete="cascade"),
        Index(),
    ] = Field(..., description="Backend-managed row id from OrderManager(SimpleTable).")
    asset_unique_identifier: Annotated[constr(max_length=255), Index()] = Field(
        ...,
        description="Asset unique_identifier targeted by this order-manager row.",
    )
    quantity: float = Field(
        ...,
        description="Requested target quantity for the asset.",
    )

    @classmethod
    def filter_expr(
        cls,
        *,
        order_manager_id: int | None = None,
        order_manager_id__in: list[int] | tuple[int, ...] | str | None = None,
        asset_unique_identifier: str | None = None,
        asset_unique_identifier__in: list[str] | tuple[str, ...] | str | None = None,
    ) -> FilterExpr | None:
        filters: list[FilterExpr | None] = []
        if order_manager_id is not None:
            filters.append(cls.f.order_manager_id.eq(int(order_manager_id)))
        order_manager_ids = _split_values(order_manager_id__in)
        if order_manager_ids:
            filters.append(
                cls.f.order_manager_id.in_([int(value) for value in order_manager_ids])
            )
        if asset_unique_identifier not in (None, ""):
            filters.append(cls.f.asset_unique_identifier.eq(str(asset_unique_identifier)))
        asset_identifiers = _split_values(asset_unique_identifier__in)
        if asset_identifiers:
            filters.append(
                cls.f.asset_unique_identifier.in_(
                    [str(identifier) for identifier in asset_identifiers]
                )
            )
        return _combine_filters(filters)


class OrderTargetQuantityConfiguration(SimpleTableUpdaterConfiguration):
    """Configuration for the client-owned order target quantity table."""

    identifier: str = Field(
        "mainsequence.markets.execution.order_target_quantities",
        description="Stable SimpleTable identifier for execution target quantities.",
    )


class OrderTargetQuantityUpdater(SimpleTableUpdater):
    """SimpleTable updater for order target quantity rows."""

    SIMPLE_TABLE_SCHEMA = OrderTargetQuantity

    def __init__(
        self,
        configuration: OrderTargetQuantityConfiguration,
        *,
        order_manager_updater: OrderManagerUpdater,
        **kwargs: Any,
    ):
        self.order_manager_updater = order_manager_updater
        super().__init__(configuration=configuration, **kwargs)

    def dependencies(self) -> dict[str, SimpleTableUpdater]:
        return {ORDER_MANAGER_TABLE_DEPENDENCY: self.order_manager_updater}

    def update(self) -> list[OrderTargetQuantity]:
        return []

    def filter_target_quantities(
        self,
        *,
        order_manager: OrderManager | int | None = None,
        order_manager_id: int | None = None,
        order_manager_id__in: list[int] | tuple[int, ...] | str | None = None,
        order_manager_unique_identifier: str | None = None,
        asset_unique_identifier: str | None = None,
        asset_unique_identifier__in: list[str] | tuple[str, ...] | str | None = None,
        limit: int = 500,
        offset: int = 0,
    ) -> list[OrderTargetQuantity]:
        resolved_order_manager_id = _resolve_order_manager_id(
            order_manager_updater=self.order_manager_updater,
            order_manager=order_manager,
            order_manager_id=order_manager_id,
            order_manager_unique_identifier=order_manager_unique_identifier,
            required=False,
        )
        return self.execute_filter(
            OrderTargetQuantity.filter_expr(
                order_manager_id=resolved_order_manager_id,
                order_manager_id__in=order_manager_id__in,
                asset_unique_identifier=asset_unique_identifier,
                asset_unique_identifier__in=asset_unique_identifier__in,
            ),
            limit=limit,
            offset=offset,
        )

    def create_or_update_target_quantity(
        self,
        *,
        order_manager: OrderManager | int | None = None,
        order_manager_id: int | None = None,
        order_manager_unique_identifier: str | None = None,
        asset_unique_identifier: str,
        quantity: float,
    ) -> OrderTargetQuantity:
        resolved_order_manager_id = _resolve_order_manager_id(
            order_manager_updater=self.order_manager_updater,
            order_manager=order_manager,
            order_manager_id=order_manager_id,
            order_manager_unique_identifier=order_manager_unique_identifier,
            required=True,
        )
        existing_rows = self.filter_target_quantities(
            order_manager_id=resolved_order_manager_id,
            asset_unique_identifier=asset_unique_identifier,
            limit=1,
        )
        existing = existing_rows[0] if existing_rows else None
        row = OrderTargetQuantity(
            id=getattr(existing, "id", None),
            order_manager_id=resolved_order_manager_id,
            asset_unique_identifier=str(asset_unique_identifier),
            quantity=float(quantity),
        )
        self.upsert(row)
        rows = self.filter_target_quantities(
            order_manager_id=resolved_order_manager_id,
            asset_unique_identifier=asset_unique_identifier,
            limit=1,
        )
        return rows[0] if rows else row

    def delete_for_order_managers(
        self,
        order_manager_unique_identifiers: list[str] | tuple[str, ...] | str,
    ) -> ExecutionDependentDeleteResult:
        requested_identifiers = _unique_strings(order_manager_unique_identifiers)
        if not requested_identifiers:
            raise ValueError("order_manager_unique_identifiers is required.")

        managers = self.order_manager_updater.filter_order_managers(
            unique_identifier__in=requested_identifiers,
            limit=len(requested_identifiers),
        )
        manager_by_identifier = {
            str(manager.unique_identifier): manager
            for manager in managers
        }
        found_identifiers = [
            identifier
            for identifier in requested_identifiers
            if identifier in manager_by_identifier
        ]
        missing_identifiers = [
            identifier
            for identifier in requested_identifiers
            if identifier not in manager_by_identifier
        ]
        manager_ids = [
            _record_id(manager_by_identifier[identifier], "order_manager")
            for identifier in found_identifiers
        ]
        rows = self.filter_target_quantities(
            order_manager_id__in=manager_ids,
            limit=max(len(manager_ids), 1) * 500,
        )
        deleted_count = 0
        for row in rows:
            self.delete(row)
            deleted_count += 1

        return ExecutionDependentDeleteResult(
            requested_identifiers=requested_identifiers,
            deleted_count=deleted_count,
            missing_identifiers=missing_identifiers,
        )


def _resolve_account_id(
    *,
    account_updater: AccountUpdater,
    account: Account | int | None = None,
    account_id: int | None = None,
    account_unique_identifier: str | None = None,
    required: bool,
) -> int | None:
    if account is not None:
        return _record_id(account, "account")
    if account_id is not None:
        return int(account_id)
    if account_unique_identifier not in (None, ""):
        return _record_id(
            account_updater.require_by_unique_identifier(str(account_unique_identifier)),
            "account",
        )
    if required:
        raise ValueError(
            "account, account_id, or account_unique_identifier is required."
        )
    return None


def _resolve_order_manager_id(
    *,
    order_manager_updater: OrderManagerUpdater,
    order_manager: OrderManager | int | None = None,
    order_manager_id: int | None = None,
    order_manager_unique_identifier: str | None = None,
    required: bool,
) -> int | None:
    if order_manager is not None:
        return _record_id(order_manager, "order_manager")
    if order_manager_id is not None:
        return int(order_manager_id)
    if order_manager_unique_identifier not in (None, ""):
        return _record_id(
            order_manager_updater.require_by_unique_identifier(
                str(order_manager_unique_identifier)
            ),
            "order_manager",
        )
    if required:
        raise ValueError(
            "order_manager, order_manager_id, or "
            "order_manager_unique_identifier is required."
        )
    return None


def _delete_rows_by_unique_identifier(
    *,
    updater: SimpleTableUpdater,
    row_getter: Callable[..., list[Any]],
    unique_identifiers: list[str] | tuple[str, ...] | str,
    approved_dependent_delete_callbacks: list[DependentDeleteCallback]
    | tuple[DependentDeleteCallback, ...]
    | None = None,
) -> ExecutionDeleteResult:
    requested_identifiers = _unique_strings(unique_identifiers)
    if not requested_identifiers:
        raise ValueError("unique_identifiers is required.")

    rows = row_getter(
        unique_identifier__in=requested_identifiers,
        limit=len(requested_identifiers),
    )
    row_by_identifier = {str(row.unique_identifier): row for row in rows}
    found_identifiers = [
        identifier
        for identifier in requested_identifiers
        if identifier in row_by_identifier
    ]
    missing_identifiers = [
        identifier
        for identifier in requested_identifiers
        if identifier not in row_by_identifier
    ]

    dependent_delete_results: list[Any] = []
    if found_identifiers:
        for callback in approved_dependent_delete_callbacks or ():
            dependent_delete_results.append(callback(found_identifiers))

    deleted_identifiers: list[str] = []
    for identifier in found_identifiers:
        updater.delete(row_by_identifier[identifier])
        deleted_identifiers.append(identifier)

    return ExecutionDeleteResult(
        requested_identifiers=requested_identifiers,
        deleted_identifiers=deleted_identifiers,
        missing_identifiers=missing_identifiers,
        dependent_delete_results=dependent_delete_results,
    )


__all__ = [
    "ORDER_MANAGER_TABLE_DEPENDENCY",
    "ExecutionDependentDeleteResult",
    "ExecutionDeleteResult",
    "OrderManager",
    "OrderManagerConfiguration",
    "OrderManagerUpdater",
    "OrderTargetQuantity",
    "OrderTargetQuantityConfiguration",
    "OrderTargetQuantityUpdater",
]
