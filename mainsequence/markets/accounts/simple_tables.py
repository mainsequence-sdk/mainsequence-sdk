from __future__ import annotations

from collections.abc import Callable
from typing import Annotated, Any

from pydantic import BaseModel, Field, constr

from mainsequence.markets.portfolios.simple_tables import (
    Portfolio,
    PortfolioAssetDetailUpdater,
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

ACCOUNT_TABLE_DEPENDENCY = "accounts"
PORTFOLIO_TABLE_DEPENDENCY = "portfolios"
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


class SimpleTableDeleteResult(BaseModel):
    """Result summary for account and fund delete helpers."""

    requested_identifiers: list[str] = Field(default_factory=list)
    deleted_identifiers: list[str] = Field(default_factory=list)
    missing_identifiers: list[str] = Field(default_factory=list)
    dependent_delete_results: list[Any] = Field(default_factory=list)

    @property
    def deleted_count(self) -> int:
        return len(self.deleted_identifiers)


class Account(SimpleTable):
    """Client-owned account identity row."""

    unique_identifier: Annotated[constr(max_length=255), Index(unique=True)] = Field(
        ...,
        description="Stable account business identifier in the selected account table.",
    )
    account_name: constr(max_length=255) = Field(
        ...,
        description="Human-readable account display name.",
    )
    is_paper: bool = Field(
        True,
        description="Whether this account represents a paper/test account.",
    )
    account_is_active: bool = Field(
        False,
        description="Client-owned active account flag.",
    )
    holdings_data_node_uid: constr(max_length=36) | None = Field(
        None,
        description="UID of the account holdings DataNode selected for this account.",
    )
    metadata_json: str | None = Field(
        None,
        description="Optional JSON-encoded account metadata owned by the client.",
    )

    @classmethod
    def filter_expr(
        cls,
        *,
        unique_identifier: str | None = None,
        unique_identifier__in: list[str] | tuple[str, ...] | str | None = None,
        unique_identifier__contains: str | None = None,
        account_name: str | None = None,
        account_name__contains: str | None = None,
        is_paper: bool | None = None,
        account_is_active: bool | None = None,
        holdings_data_node_uid: str | None = None,
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
        if account_name not in (None, ""):
            filters.append(cls.f.account_name.eq(str(account_name)))
        if account_name__contains not in (None, ""):
            filters.append(cls.f.account_name.contains(str(account_name__contains)))
        if is_paper is not None:
            filters.append(cls.f.is_paper.eq(bool(is_paper)))
        if account_is_active is not None:
            filters.append(cls.f.account_is_active.eq(bool(account_is_active)))
        if holdings_data_node_uid not in (None, ""):
            filters.append(cls.f.holdings_data_node_uid.eq(str(holdings_data_node_uid)))

        normalized_search = str(search or "").strip()
        if normalized_search:
            filters.append(
                or_(
                    cls.f.unique_identifier.contains(normalized_search),
                    cls.f.account_name.contains(normalized_search),
                )
            )

        return _combine_filters(filters)

    @classmethod
    def create_or_update(
        cls,
        *,
        updater: AccountUpdater,
        unique_identifier: str,
        account_name: str,
        is_paper: bool = True,
        account_is_active: bool = False,
        holdings_data_node_uid: str | None = None,
        metadata_json: str | None = None,
    ) -> Account:
        return updater.create_or_update_account(
            unique_identifier=unique_identifier,
            account_name=account_name,
            is_paper=is_paper,
            account_is_active=account_is_active,
            holdings_data_node_uid=holdings_data_node_uid,
            metadata_json=metadata_json,
        )

    @classmethod
    def get_or_create(
        cls,
        *,
        updater: AccountUpdater,
        unique_identifier: str,
        account_name: str | None = None,
        **kwargs: Any,
    ) -> Account:
        existing = updater.get_by_unique_identifier(unique_identifier)
        if existing is not None:
            return existing
        return updater.create_or_update_account(
            unique_identifier=unique_identifier,
            account_name=account_name or unique_identifier,
            **kwargs,
        )

    def holdings_dimension_filters(
        self,
        *,
        dimension_name: str = "account_unique_identifier",
    ) -> dict[str, list[str]]:
        return {dimension_name: [str(self.unique_identifier)]}

    def latest_holdings(
        self,
        holdings_data_node: Any,
        *,
        dimension_name: str = "account_unique_identifier",
        dimension_filters: dict[str, list[Any]] | None = None,
    ) -> Any:
        filters = _dimension_filters_with_identifier(
            dimension_filters,
            dimension_name,
            self.unique_identifier,
        )
        return holdings_data_node.get_last_observation(dimension_filters=filters)

    def holdings_history(
        self,
        holdings_data_node: Any,
        *,
        start_date: Any = None,
        end_date: Any = None,
        great_or_equal: bool = True,
        less_or_equal: bool = True,
        dimension_name: str = "account_unique_identifier",
        dimension_filters: dict[str, list[Any]] | None = None,
        columns: list[str] | None = None,
    ) -> Any:
        filters = _dimension_filters_with_identifier(
            dimension_filters,
            dimension_name,
            self.unique_identifier,
        )
        return holdings_data_node.get_df_between_dates(
            start_date=start_date,
            end_date=end_date,
            great_or_equal=great_or_equal,
            less_or_equal=less_or_equal,
            dimension_filters=filters,
            columns=columns,
        )


class AccountConfiguration(SimpleTableUpdaterConfiguration):
    """Configuration for the client-owned account table."""

    identifier: str = Field(
        "mainsequence.markets.accounts",
        description="Stable SimpleTable identifier for client-owned accounts.",
    )


class AccountUpdater(SimpleTableUpdater):
    """SimpleTable updater for client-owned accounts."""

    SIMPLE_TABLE_SCHEMA = Account

    def update(self) -> list[Account]:
        return []

    def filter_accounts(
        self,
        *,
        unique_identifier: str | None = None,
        unique_identifier__in: list[str] | tuple[str, ...] | str | None = None,
        unique_identifier__contains: str | None = None,
        account_name: str | None = None,
        account_name__contains: str | None = None,
        is_paper: bool | None = None,
        account_is_active: bool | None = None,
        holdings_data_node_uid: str | None = None,
        search: str | None = None,
        limit: int = 500,
        offset: int = 0,
    ) -> list[Account]:
        return self.execute_filter(
            Account.filter_expr(
                unique_identifier=unique_identifier,
                unique_identifier__in=unique_identifier__in,
                unique_identifier__contains=unique_identifier__contains,
                account_name=account_name,
                account_name__contains=account_name__contains,
                is_paper=is_paper,
                account_is_active=account_is_active,
                holdings_data_node_uid=holdings_data_node_uid,
                search=search,
            ),
            limit=limit,
            offset=offset,
        )

    def get_by_unique_identifier(self, unique_identifier: str) -> Account | None:
        rows = self.filter_accounts(unique_identifier=unique_identifier, limit=1)
        return rows[0] if rows else None

    def require_by_unique_identifier(self, unique_identifier: str) -> Account:
        row = self.get_by_unique_identifier(unique_identifier)
        if row is None:
            raise ValueError(f"Account '{unique_identifier}' was not found.")
        return row

    def create_or_update_account(
        self,
        *,
        unique_identifier: str,
        account_name: str,
        is_paper: bool = True,
        account_is_active: bool = False,
        holdings_data_node_uid: str | None = None,
        metadata_json: str | None = None,
    ) -> Account:
        existing = self.get_by_unique_identifier(unique_identifier)
        account = Account(
            id=getattr(existing, "id", None),
            unique_identifier=str(unique_identifier),
            account_name=account_name,
            is_paper=is_paper,
            account_is_active=account_is_active,
            holdings_data_node_uid=holdings_data_node_uid,
            metadata_json=metadata_json,
        )
        self.upsert(account)
        return self.get_by_unique_identifier(unique_identifier) or account

    def delete_accounts(
        self,
        *,
        unique_identifiers: list[str] | tuple[str, ...] | str,
        approved_dependent_delete_callbacks: list[DependentDeleteCallback]
        | tuple[DependentDeleteCallback, ...]
        | None = None,
    ) -> SimpleTableDeleteResult:
        return _delete_rows_by_unique_identifier(
            updater=self,
            row_getter=self.filter_accounts,
            unique_identifiers=unique_identifiers,
            approved_dependent_delete_callbacks=approved_dependent_delete_callbacks,
        )


class AccountTargetPositionAssignment(SimpleTable):
    """Client-owned binding from account to a reusable target-position set."""

    account_id: Annotated[
        int,
        ForeignKey(ACCOUNT_TABLE_DEPENDENCY, on_delete="cascade"),
        Index(),
    ] = Field(..., description="Backend-managed row id from Account(SimpleTable).")
    target_positions_time: Annotated[constr(max_length=64), Index()] = Field(
        ...,
        description="Target positions timestamp encoded as an ISO-8601 string.",
    )
    position_set_uid: constr(max_length=36) = Field(
        ...,
        description="Reusable target-position set UID.",
    )

    @classmethod
    def filter_expr(
        cls,
        *,
        account_id: int | None = None,
        account_id__in: list[int] | tuple[int, ...] | str | None = None,
        target_positions_time: str | None = None,
        position_set_uid: str | None = None,
    ) -> FilterExpr | None:
        filters: list[FilterExpr | None] = []
        if account_id is not None:
            filters.append(cls.f.account_id.eq(int(account_id)))
        account_ids = _split_values(account_id__in)
        if account_ids:
            filters.append(cls.f.account_id.in_([int(value) for value in account_ids]))
        if target_positions_time not in (None, ""):
            filters.append(cls.f.target_positions_time.eq(str(target_positions_time)))
        if position_set_uid not in (None, ""):
            filters.append(cls.f.position_set_uid.eq(str(position_set_uid)))
        return _combine_filters(filters)


class AccountTargetPositionAssignmentConfiguration(SimpleTableUpdaterConfiguration):
    """Configuration for client-owned account target-position assignments."""

    identifier: str = Field(
        "mainsequence.markets.account_target_position_assignments",
        description=(
            "Stable SimpleTable identifier for account target-position assignments."
        ),
    )


class AccountTargetPositionAssignmentUpdater(SimpleTableUpdater):
    """SimpleTable updater for account target-position assignment rows."""

    SIMPLE_TABLE_SCHEMA = AccountTargetPositionAssignment

    def __init__(
        self,
        configuration: AccountTargetPositionAssignmentConfiguration,
        *,
        account_updater: AccountUpdater,
        **kwargs: Any,
    ):
        self.account_updater = account_updater
        super().__init__(configuration=configuration, **kwargs)

    def dependencies(self) -> dict[str, SimpleTableUpdater]:
        return {ACCOUNT_TABLE_DEPENDENCY: self.account_updater}

    def update(self) -> list[AccountTargetPositionAssignment]:
        return []

    def filter_assignments(
        self,
        *,
        account: Account | int | None = None,
        account_id: int | None = None,
        account_unique_identifier: str | None = None,
        target_positions_time: str | None = None,
        position_set_uid: str | None = None,
        limit: int = 500,
        offset: int = 0,
    ) -> list[AccountTargetPositionAssignment]:
        resolved_account_id = _resolve_account_id(
            account_updater=self.account_updater,
            account=account,
            account_id=account_id,
            account_unique_identifier=account_unique_identifier,
            required=False,
        )
        return self.execute_filter(
            AccountTargetPositionAssignment.filter_expr(
                account_id=resolved_account_id,
                target_positions_time=target_positions_time,
                position_set_uid=position_set_uid,
            ),
            limit=limit,
            offset=offset,
        )

    def create_or_update_assignment(
        self,
        *,
        account: Account | int | None = None,
        account_id: int | None = None,
        account_unique_identifier: str | None = None,
        target_positions_time: str,
        position_set_uid: str,
    ) -> AccountTargetPositionAssignment:
        resolved_account_id = _resolve_account_id(
            account_updater=self.account_updater,
            account=account,
            account_id=account_id,
            account_unique_identifier=account_unique_identifier,
            required=True,
        )
        existing_rows = self.filter_assignments(
            account_id=resolved_account_id,
            target_positions_time=target_positions_time,
            limit=1,
        )
        existing = existing_rows[0] if existing_rows else None
        row = AccountTargetPositionAssignment(
            id=getattr(existing, "id", None),
            account_id=resolved_account_id,
            target_positions_time=str(target_positions_time),
            position_set_uid=str(position_set_uid),
        )
        self.upsert(row)
        rows = self.filter_assignments(
            account_id=resolved_account_id,
            target_positions_time=target_positions_time,
            limit=1,
        )
        return rows[0] if rows else row


class VirtualFund(SimpleTable):
    """Client-owned virtual fund row."""

    unique_identifier: Annotated[constr(max_length=255), Index(unique=True)] = Field(
        ...,
        description="Stable virtual-fund business identifier.",
    )
    target_account_id: Annotated[
        int,
        ForeignKey(ACCOUNT_TABLE_DEPENDENCY, on_delete="cascade"),
        Index(),
    ] = Field(..., description="Backend-managed row id from Account(SimpleTable).")
    target_portfolio_id: Annotated[
        int,
        ForeignKey(PORTFOLIO_TABLE_DEPENDENCY, on_delete="cascade"),
        Index(),
    ] = Field(..., description="Backend-managed row id from Portfolio(SimpleTable).")
    requires_nav_adjustment: bool = Field(
        False,
        description="Whether fund holdings need NAV adjustment.",
    )
    metadata_json: str | None = Field(
        None,
        description="Optional JSON-encoded virtual-fund metadata owned by the client.",
    )

    @classmethod
    def filter_expr(
        cls,
        *,
        unique_identifier: str | None = None,
        unique_identifier__in: list[str] | tuple[str, ...] | str | None = None,
        unique_identifier__contains: str | None = None,
        target_account_id: int | None = None,
        target_account_id__in: list[int] | tuple[int, ...] | str | None = None,
        target_portfolio_id: int | None = None,
        target_portfolio_id__in: list[int] | tuple[int, ...] | str | None = None,
        requires_nav_adjustment: bool | None = None,
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
        account_ids = _split_values(target_account_id__in)
        if account_ids:
            filters.append(
                cls.f.target_account_id.in_([int(value) for value in account_ids])
            )
        if target_portfolio_id is not None:
            filters.append(cls.f.target_portfolio_id.eq(int(target_portfolio_id)))
        portfolio_ids = _split_values(target_portfolio_id__in)
        if portfolio_ids:
            filters.append(
                cls.f.target_portfolio_id.in_([int(value) for value in portfolio_ids])
            )
        if requires_nav_adjustment is not None:
            filters.append(cls.f.requires_nav_adjustment.eq(bool(requires_nav_adjustment)))
        normalized_search = str(search or "").strip()
        if normalized_search:
            filters.append(cls.f.unique_identifier.contains(normalized_search))
        return _combine_filters(filters)

    def holdings_dimension_filters(
        self,
        *,
        dimension_name: str = "fund_unique_identifier",
    ) -> dict[str, list[str]]:
        return {dimension_name: [str(self.unique_identifier)]}

    def latest_holdings(
        self,
        holdings_data_node: Any,
        *,
        dimension_name: str = "fund_unique_identifier",
        dimension_filters: dict[str, list[Any]] | None = None,
    ) -> Any:
        filters = _dimension_filters_with_identifier(
            dimension_filters,
            dimension_name,
            self.unique_identifier,
        )
        return holdings_data_node.get_last_observation(dimension_filters=filters)

    def holdings_history(
        self,
        holdings_data_node: Any,
        *,
        start_date: Any = None,
        end_date: Any = None,
        great_or_equal: bool = True,
        less_or_equal: bool = True,
        dimension_name: str = "fund_unique_identifier",
        dimension_filters: dict[str, list[Any]] | None = None,
        columns: list[str] | None = None,
    ) -> Any:
        filters = _dimension_filters_with_identifier(
            dimension_filters,
            dimension_name,
            self.unique_identifier,
        )
        return holdings_data_node.get_df_between_dates(
            start_date=start_date,
            end_date=end_date,
            great_or_equal=great_or_equal,
            less_or_equal=less_or_equal,
            dimension_filters=filters,
            columns=columns,
        )


class VirtualFundConfiguration(SimpleTableUpdaterConfiguration):
    """Configuration for the client-owned virtual fund table."""

    identifier: str = Field(
        "mainsequence.markets.virtual_funds",
        description="Stable SimpleTable identifier for client-owned virtual funds.",
    )


class VirtualFundUpdater(SimpleTableUpdater):
    """SimpleTable updater for client-owned virtual funds."""

    SIMPLE_TABLE_SCHEMA = VirtualFund

    def __init__(
        self,
        configuration: VirtualFundConfiguration,
        *,
        account_updater: AccountUpdater,
        portfolio_updater: PortfolioAssetDetailUpdater,
        **kwargs: Any,
    ):
        self.account_updater = account_updater
        self.portfolio_updater = portfolio_updater
        super().__init__(configuration=configuration, **kwargs)

    def dependencies(self) -> dict[str, SimpleTableUpdater]:
        return {
            ACCOUNT_TABLE_DEPENDENCY: self.account_updater,
            PORTFOLIO_TABLE_DEPENDENCY: self.portfolio_updater,
        }

    def update(self) -> list[VirtualFund]:
        return []

    def filter_virtual_funds(
        self,
        *,
        unique_identifier: str | None = None,
        unique_identifier__in: list[str] | tuple[str, ...] | str | None = None,
        unique_identifier__contains: str | None = None,
        target_account: Account | int | None = None,
        target_account_id: int | None = None,
        target_account_unique_identifier: str | None = None,
        target_portfolio: Portfolio | int | None = None,
        target_portfolio_id: int | None = None,
        target_portfolio_unique_identifier: str | None = None,
        requires_nav_adjustment: bool | None = None,
        search: str | None = None,
        limit: int = 500,
        offset: int = 0,
    ) -> list[VirtualFund]:
        resolved_account_id = _resolve_account_id(
            account_updater=self.account_updater,
            account=target_account,
            account_id=target_account_id,
            account_unique_identifier=target_account_unique_identifier,
            required=False,
        )
        resolved_portfolio_id = _resolve_portfolio_id(
            portfolio_updater=self.portfolio_updater,
            portfolio=target_portfolio,
            portfolio_id=target_portfolio_id,
            portfolio_unique_identifier=target_portfolio_unique_identifier,
            required=False,
        )
        return self.execute_filter(
            VirtualFund.filter_expr(
                unique_identifier=unique_identifier,
                unique_identifier__in=unique_identifier__in,
                unique_identifier__contains=unique_identifier__contains,
                target_account_id=resolved_account_id,
                target_portfolio_id=resolved_portfolio_id,
                requires_nav_adjustment=requires_nav_adjustment,
                search=search,
            ),
            limit=limit,
            offset=offset,
        )

    def get_by_unique_identifier(self, unique_identifier: str) -> VirtualFund | None:
        rows = self.filter_virtual_funds(unique_identifier=unique_identifier, limit=1)
        return rows[0] if rows else None

    def require_by_unique_identifier(self, unique_identifier: str) -> VirtualFund:
        row = self.get_by_unique_identifier(unique_identifier)
        if row is None:
            raise ValueError(f"VirtualFund '{unique_identifier}' was not found.")
        return row

    def create_or_update_virtual_fund(
        self,
        *,
        unique_identifier: str,
        target_account: Account | int | None = None,
        target_account_id: int | None = None,
        target_account_unique_identifier: str | None = None,
        target_portfolio: Portfolio | int | None = None,
        target_portfolio_id: int | None = None,
        target_portfolio_unique_identifier: str | None = None,
        requires_nav_adjustment: bool = False,
        metadata_json: str | None = None,
    ) -> VirtualFund:
        resolved_account_id = _resolve_account_id(
            account_updater=self.account_updater,
            account=target_account,
            account_id=target_account_id,
            account_unique_identifier=target_account_unique_identifier,
            required=True,
        )
        resolved_portfolio_id = _resolve_portfolio_id(
            portfolio_updater=self.portfolio_updater,
            portfolio=target_portfolio,
            portfolio_id=target_portfolio_id,
            portfolio_unique_identifier=target_portfolio_unique_identifier,
            required=True,
        )
        existing = self.get_by_unique_identifier(unique_identifier)
        row = VirtualFund(
            id=getattr(existing, "id", None),
            unique_identifier=str(unique_identifier),
            target_account_id=resolved_account_id,
            target_portfolio_id=resolved_portfolio_id,
            requires_nav_adjustment=requires_nav_adjustment,
            metadata_json=metadata_json,
        )
        self.upsert(row)
        return self.get_by_unique_identifier(unique_identifier) or row

    def delete_virtual_funds(
        self,
        *,
        unique_identifiers: list[str] | tuple[str, ...] | str,
        approved_dependent_delete_callbacks: list[DependentDeleteCallback]
        | tuple[DependentDeleteCallback, ...]
        | None = None,
    ) -> SimpleTableDeleteResult:
        return _delete_rows_by_unique_identifier(
            updater=self,
            row_getter=self.filter_virtual_funds,
            unique_identifiers=unique_identifiers,
            approved_dependent_delete_callbacks=approved_dependent_delete_callbacks,
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


def _resolve_portfolio_id(
    *,
    portfolio_updater: PortfolioAssetDetailUpdater,
    portfolio: Portfolio | int | None = None,
    portfolio_id: int | None = None,
    portfolio_unique_identifier: str | None = None,
    required: bool,
) -> int | None:
    if portfolio is not None:
        return _record_id(portfolio, "portfolio")
    if portfolio_id is not None:
        return int(portfolio_id)
    if portfolio_unique_identifier not in (None, ""):
        return _record_id(
            portfolio_updater.require_by_asset_unique_identifier(
                str(portfolio_unique_identifier)
            ),
            "portfolio",
        )
    if required:
        raise ValueError(
            "portfolio, portfolio_id, or portfolio_unique_identifier is required."
        )
    return None


def _dimension_filters_with_identifier(
    dimension_filters: dict[str, list[Any]] | None,
    key: str,
    value: Any,
) -> dict[str, list[Any]]:
    resolved: dict[str, list[Any]] = {
        filter_key: list(filter_values)
        for filter_key, filter_values in (dimension_filters or {}).items()
    }
    identifier = str(value)
    values = resolved.get(key)
    if values is None:
        resolved[key] = [identifier]
        return resolved
    if identifier not in [str(item) for item in values]:
        raise ValueError(
            f"dimension_filters[{key!r}] conflicts with identifier {identifier!r}."
        )
    return resolved


def _delete_rows_by_unique_identifier(
    *,
    updater: SimpleTableUpdater,
    row_getter: Callable[..., list[Any]],
    unique_identifiers: list[str] | tuple[str, ...] | str,
    approved_dependent_delete_callbacks: list[DependentDeleteCallback]
    | tuple[DependentDeleteCallback, ...]
    | None = None,
) -> SimpleTableDeleteResult:
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

    return SimpleTableDeleteResult(
        requested_identifiers=requested_identifiers,
        deleted_identifiers=deleted_identifiers,
        missing_identifiers=missing_identifiers,
        dependent_delete_results=dependent_delete_results,
    )


__all__ = [
    "ACCOUNT_TABLE_DEPENDENCY",
    "PORTFOLIO_TABLE_DEPENDENCY",
    "Account",
    "AccountConfiguration",
    "AccountTargetPositionAssignment",
    "AccountTargetPositionAssignmentConfiguration",
    "AccountTargetPositionAssignmentUpdater",
    "AccountUpdater",
    "SimpleTableDeleteResult",
    "VirtualFund",
    "VirtualFundConfiguration",
    "VirtualFundUpdater",
]
