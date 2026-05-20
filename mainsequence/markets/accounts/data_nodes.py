from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any, ClassVar
from uuid import UUID

import pandas as pd

from mainsequence.client.markets.models.accounts_and_portfolios import (
    Account as ClientAccount,
)
from mainsequence.client.markets.models.accounts_and_portfolios import (
    AccountHoldingsSnapshot as ClientAccountHoldingsSnapshot,
)
from mainsequence.client.markets.models.accounts_and_portfolios import (
    AccountHoldingsWritePosition as ClientAccountHoldingsWritePosition,
)
from mainsequence.client.markets.models.accounts_and_portfolios import (
    AccountHoldingsWriteResponse as ClientAccountHoldingsWriteResponse,
)
from mainsequence.client.markets.models.accounts_and_portfolios import (
    AccountTargetPosition as ClientAccountTargetPosition,
)
from mainsequence.client.markets.models.accounts_and_portfolios import (
    AccountTargetPositionsSnapshot as ClientAccountTargetPositionsSnapshot,
)
from mainsequence.client.markets.models.accounts_and_portfolios import (
    AccountTargetPositionsWriteResponse as ClientAccountTargetPositionsWriteResponse,
)
from mainsequence.client.models_tdag import LOGICAL_COLUMN_DTYPES_ATTR
from mainsequence.tdag.data_nodes import (
    DataNode,
    DataNodeConfiguration,
    DataNodeMetaData,
    RecordDefinition,
)

ACCOUNT_HOLDINGS_TIME_INDEX_NAME = "time_index"
ACCOUNT_HOLDINGS_INDEX_NAMES = ["time_index", "account_uid", "unique_identifier"]
ACCOUNT_HOLDINGS_COLUMN_DTYPES_MAP = {
    "time_index": "datetime64[ns, UTC]",
    "account_uid": "uuid",
    "unique_identifier": "object",
    "holdings_set_uid": "uuid",
    "is_trade_snapshot": "bool",
    "quantity": "decimal",
    "target_trade_time": "datetime64[ns, UTC]",
    "extra_details": "jsonb",
}
ACCOUNT_HOLDINGS_COLUMN_LABELS = {
    "time_index": "Time Index",
    "account_uid": "Account UID",
    "unique_identifier": "Unique Identifier",
    "holdings_set_uid": "Holdings Set UID",
    "is_trade_snapshot": "Is Trade Snapshot",
    "quantity": "Quantity",
    "target_trade_time": "Target Trade Time",
    "extra_details": "Extra Details",
}
ACCOUNT_HOLDINGS_COLUMN_DESCRIPTIONS = {
    "time_index": "Timestamp for the stamped account holdings row.",
    "account_uid": "Stable public Account UID that owns the holdings row.",
    "unique_identifier": "Row-grain unique identifier inside the account timestamp.",
    "holdings_set_uid": "Stable UUID shared by rows in one account holdings set.",
    "is_trade_snapshot": "Whether this holdings row is tied to an execution snapshot.",
    "quantity": "Submitted position quantity for the holdings row.",
    "target_trade_time": "Per-row target execution time when provided.",
    "extra_details": "Variable JSON payload for extra holdings attributes.",
}

VIRTUAL_FUND_HOLDINGS_TIME_INDEX_NAME = "time_index"
VIRTUAL_FUND_HOLDINGS_INDEX_NAMES = ["time_index", "fund_uid", "unique_identifier"]
VIRTUAL_FUND_HOLDINGS_COLUMN_DTYPES_MAP = {
    "time_index": "datetime64[ns, UTC]",
    "fund_uid": "uuid",
    "unique_identifier": "object",
    "holdings_set_uid": "uuid",
    "is_trade_snapshot": "bool",
    "quantity": "decimal",
    "target_weight": "decimal",
    "target_trade_time": "datetime64[ns, UTC]",
    "extra_details": "jsonb",
}
VIRTUAL_FUND_HOLDINGS_COLUMN_LABELS = {
    "time_index": "Time Index",
    "fund_uid": "Virtual Fund UID",
    "unique_identifier": "Unique Identifier",
    "holdings_set_uid": "Holdings Set UID",
    "is_trade_snapshot": "Is Trade Snapshot",
    "quantity": "Quantity",
    "target_weight": "Target Weight",
    "target_trade_time": "Target Trade Time",
    "extra_details": "Extra Details",
}
VIRTUAL_FUND_HOLDINGS_COLUMN_DESCRIPTIONS = {
    "time_index": "Timestamp for the stamped virtual-fund holdings row.",
    "fund_uid": "Stable public VirtualFund UID that owns the holdings row.",
    "unique_identifier": "Row-grain unique identifier inside the fund timestamp.",
    "holdings_set_uid": "Stable UUID shared by rows in one virtual-fund holdings set.",
    "is_trade_snapshot": "Whether this holdings row is tied to an execution snapshot.",
    "quantity": "Submitted position quantity for the holdings row.",
    "target_weight": "Submitted target portfolio weight for the holdings row when available.",
    "target_trade_time": "Per-row target execution time when provided.",
    "extra_details": "Variable JSON payload for extra holdings attributes.",
}

MOCK_ACCOUNT_UID = UUID("00000000-0000-0000-0000-000000000001")
MOCK_FUND_UID = UUID("00000000-0000-0000-0000-000000000002")
MOCK_HOLDINGS_SET_UID = UUID("00000000-0000-0000-0000-000000000100")
MOCK_UNIQUE_IDENTIFIER = "__schema_bootstrap__"
MOCK_TIME_INDEX = dt.datetime(1970, 1, 1, tzinfo=dt.UTC)


class HoldingsDataNodeConfiguration(DataNodeConfiguration):
    """Configuration base for SDK-created holdings data nodes."""


@dataclass(frozen=True)
class HoldingsDataNodeContract:
    role: str
    schema_version: int
    description: str
    time_index_name: str
    index_names: list[str]
    column_dtypes_map: dict[str, str]
    column_labels: dict[str, str] = field(default_factory=dict)
    column_descriptions: dict[str, str] = field(default_factory=dict)

    @property
    def owner_index_name(self) -> str:
        return self.index_names[1]

    def default_config(self) -> HoldingsDataNodeConfiguration:
        return HoldingsDataNodeConfiguration(
            records=[
                RecordDefinition(
                    column_name=column_name,
                    dtype=dtype,
                    label=self.column_labels.get(column_name, column_name),
                    description=self.column_descriptions.get(column_name),
                )
                for column_name, dtype in self.column_dtypes_map.items()
            ],
            node_metadata=DataNodeMetaData(
                identifier=f"mainsequence.markets.{self.role}",
                description=self.description,
            ),
        )


ACCOUNT_HOLDINGS_CONTRACT = HoldingsDataNodeContract(
    role="account_historical_holdings",
    schema_version=4,
    description="Account historical holdings rows table.",
    time_index_name=ACCOUNT_HOLDINGS_TIME_INDEX_NAME,
    index_names=ACCOUNT_HOLDINGS_INDEX_NAMES,
    column_dtypes_map=ACCOUNT_HOLDINGS_COLUMN_DTYPES_MAP,
    column_labels=ACCOUNT_HOLDINGS_COLUMN_LABELS,
    column_descriptions=ACCOUNT_HOLDINGS_COLUMN_DESCRIPTIONS,
)

VIRTUAL_FUND_HOLDINGS_CONTRACT = HoldingsDataNodeContract(
    role="virtual_fund_historical_holdings",
    schema_version=3,
    description="Virtual fund historical holdings rows table.",
    time_index_name=VIRTUAL_FUND_HOLDINGS_TIME_INDEX_NAME,
    index_names=VIRTUAL_FUND_HOLDINGS_INDEX_NAMES,
    column_dtypes_map=VIRTUAL_FUND_HOLDINGS_COLUMN_DTYPES_MAP,
    column_labels=VIRTUAL_FUND_HOLDINGS_COLUMN_LABELS,
    column_descriptions=VIRTUAL_FUND_HOLDINGS_COLUMN_DESCRIPTIONS,
)


class HoldingsDataNode(DataNode):
    """Base class for holdings tables created through the standard DataNode path."""

    HOLDINGS_CONTRACT: ClassVar[HoldingsDataNodeContract]
    MOCK_OWNER_UID: ClassVar[UUID]

    def __init__(
        self,
        config: HoldingsDataNodeConfiguration | None = None,
        *args,
        **kwargs,
    ):
        resolved_config = config or self.HOLDINGS_CONTRACT.default_config()
        super().__init__(resolved_config, *args, **kwargs)

    def dependencies(self) -> dict[str, DataNode]:
        return {}

    def update(self) -> pd.DataFrame:
        return self.validate_holdings_frame(self.get_holdings_frame())

    def get_holdings_frame(self) -> pd.DataFrame:
        return self.build_initialization_frame()

    @classmethod
    def build_initialization_frame(cls, **kwargs) -> pd.DataFrame:
        return cls.build_mock_frame(**kwargs)

    @classmethod
    def build_mock_frame(
        cls,
        *,
        owner_uid: UUID | str | None = None,
        unique_identifier: str = MOCK_UNIQUE_IDENTIFIER,
        holdings_set_uid: UUID | str = MOCK_HOLDINGS_SET_UID,
        time_index: dt.datetime | pd.Timestamp = MOCK_TIME_INDEX,
    ) -> pd.DataFrame:
        owner_uid = owner_uid or cls.MOCK_OWNER_UID
        contract = cls.HOLDINGS_CONTRACT
        row: dict[str, Any] = {
            contract.time_index_name: time_index,
            contract.owner_index_name: owner_uid,
            "unique_identifier": unique_identifier,
            "holdings_set_uid": holdings_set_uid,
            "is_trade_snapshot": False,
            "quantity": "0",
            "target_trade_time": pd.Timestamp(time_index).isoformat(),
            "extra_details": {},
        }
        if "target_weight" in contract.column_dtypes_map:
            row["target_weight"] = "0"
        frame = pd.DataFrame([row])
        frame = frame.set_index(contract.index_names)
        return cls.validate_holdings_frame(frame)

    @classmethod
    def validate_holdings_frame(cls, data_frame: pd.DataFrame) -> pd.DataFrame:
        contract = cls.HOLDINGS_CONTRACT
        frame = _ensure_contract_index(data_frame, contract=contract)
        flat = frame.reset_index()
        missing_columns = [
            column_name
            for column_name in contract.column_dtypes_map
            if column_name not in flat.columns
        ]
        if missing_columns:
            raise ValueError(
                "Holdings frame is missing required columns: "
                f"{', '.join(missing_columns)}."
            )

        flat = _normalize_contract_values(flat, contract=contract)
        frame = flat.set_index(contract.index_names).sort_index()
        if frame.index.has_duplicates:
            raise ValueError(
                "Holdings frame contains duplicate rows for index contract "
                f"{contract.index_names}."
            )
        return _attach_logical_dtype_contract(frame, contract=contract)


class AccountHoldings(HoldingsDataNode):
    """DataNode users can subclass to import account holdings."""

    HOLDINGS_CONTRACT = ACCOUNT_HOLDINGS_CONTRACT
    MOCK_OWNER_UID = MOCK_ACCOUNT_UID

    @classmethod
    def build_mock_account_frame(
        cls,
        *,
        account_uid: UUID | str | None = None,
        **kwargs,
    ) -> pd.DataFrame:
        return cls.build_mock_frame(owner_uid=account_uid, **kwargs)

    def holdings_data_source_id(self) -> int:
        return _coerce_required_id(
            self.data_node_storage,
            field_name="data_node_storage",
        )

    def create_account(
        self,
        *,
        bind_holdings_data_node: bool = True,
        timeout=None,
        **account_kwargs,
    ) -> ClientAccount:
        payload = self._account_payload(
            account_kwargs,
            bind_holdings_data_node=bind_holdings_data_node,
        )
        account = ClientAccount.create(timeout=timeout, **payload)
        if bind_holdings_data_node:
            return self.bind_account(account, timeout=timeout)
        return account

    def get_or_create_account(
        self,
        *,
        bind_holdings_data_node: bool = True,
        create_without_holdings: bool = True,
        timeout=None,
        **account_kwargs,
    ) -> ClientAccount:
        payload = self._account_payload(
            account_kwargs,
            bind_holdings_data_node=bind_holdings_data_node,
        )
        account = ClientAccount.get_or_create(
            create_without_holdings=create_without_holdings,
            timeout=timeout,
            **payload,
        )
        if bind_holdings_data_node:
            return self.bind_account(account, timeout=timeout)
        return account

    def bind_account(
        self,
        account: ClientAccount | str | UUID,
        *,
        timeout=None,
    ) -> ClientAccount:
        holdings_data_source_id = self.holdings_data_source_id()
        account_obj = self._resolve_account(account, timeout=timeout)
        if _coerce_optional_id(
            getattr(account_obj, "holdings_data_source", None),
            field_name="holdings_data_source",
        ) == holdings_data_source_id:
            return account_obj

        if hasattr(account_obj, "patch"):
            return account_obj.patch(
                timeout=timeout,
                holdings_data_source=holdings_data_source_id,
            )
        return ClientAccount.patch_by_id(
            str(account),
            timeout=timeout,
            holdings_data_source=holdings_data_source_id,
        )

    def add_account_holdings(
        self,
        *,
        account: ClientAccount | str | UUID,
        holdings_date: dt.datetime | str,
        positions: list[ClientAccountHoldingsWritePosition | dict[str, Any]],
        overwrite: bool = False,
        bind_if_needed: bool = True,
        timeout=None,
    ) -> ClientAccountHoldingsWriteResponse:
        account_obj = self._account_for_operational_write(
            account,
            bind_if_needed=bind_if_needed,
            timeout=timeout,
        )
        return account_obj.add_holdings(
            holdings_date=holdings_date,
            positions=positions,
            overwrite=overwrite,
            timeout=timeout,
        )

    def add_account_target_positions(
        self,
        *,
        account: ClientAccount | str | UUID,
        target_positions_date: dt.datetime | str,
        positions: list[ClientAccountTargetPosition | dict[str, Any]],
        overwrite: bool = False,
        bind_if_needed: bool = True,
        timeout=None,
    ) -> ClientAccountTargetPositionsWriteResponse:
        account_obj = self._account_for_operational_write(
            account,
            bind_if_needed=bind_if_needed,
            timeout=timeout,
        )
        return account_obj.add_target_positions(
            target_positions_date=target_positions_date,
            positions=positions,
            overwrite=overwrite,
            timeout=timeout,
        )

    def get_account_holdings(
        self,
        *,
        account: ClientAccount | str | UUID,
        holdings_date: dt.datetime | str | None = None,
        order: str = "desc",
        limit: int | None = None,
        include_asset_detail: bool = False,
        bind_if_needed: bool = False,
        timeout=None,
    ) -> list[ClientAccountHoldingsSnapshot]:
        account_obj = self._account_for_operational_write(
            account,
            bind_if_needed=bind_if_needed,
            timeout=timeout,
        )
        return account_obj.get_holdings(
            holdings_date=holdings_date,
            order=order,
            limit=limit,
            include_asset_detail=include_asset_detail,
            timeout=timeout,
        )

    def get_latest_account_holdings(
        self,
        *,
        account: ClientAccount | str | UUID,
        include_asset_detail: bool = False,
        bind_if_needed: bool = False,
        timeout=None,
    ) -> ClientAccountHoldingsSnapshot | None:
        account_obj = self._account_for_operational_write(
            account,
            bind_if_needed=bind_if_needed,
            timeout=timeout,
        )
        return account_obj.get_latest_holdings(
            include_asset_detail=include_asset_detail,
            timeout=timeout,
        )

    def get_account_target_positions(
        self,
        *,
        account: ClientAccount | str | UUID,
        target_positions_date: dt.datetime | str | None = None,
        order: str = "desc",
        limit: int | None = None,
        include_asset_detail: bool = False,
        bind_if_needed: bool = False,
        timeout=None,
    ) -> list[ClientAccountTargetPositionsSnapshot]:
        account_obj = self._account_for_operational_write(
            account,
            bind_if_needed=bind_if_needed,
            timeout=timeout,
        )
        return account_obj.get_target_positions(
            target_positions_date=target_positions_date,
            order=order,
            limit=limit,
            include_asset_detail=include_asset_detail,
            timeout=timeout,
        )

    def get_latest_account_target_positions(
        self,
        *,
        account: ClientAccount | str | UUID,
        include_asset_detail: bool = False,
        bind_if_needed: bool = False,
        timeout=None,
    ) -> ClientAccountTargetPositionsSnapshot | None:
        account_obj = self._account_for_operational_write(
            account,
            bind_if_needed=bind_if_needed,
            timeout=timeout,
        )
        return account_obj.get_latest_target_positions(
            include_asset_detail=include_asset_detail,
            timeout=timeout,
        )

    def _account_payload(
        self,
        account_kwargs: dict[str, Any],
        *,
        bind_holdings_data_node: bool,
    ) -> dict[str, Any]:
        payload = dict(account_kwargs)
        if not bind_holdings_data_node:
            return payload

        holdings_data_source_id = self.holdings_data_source_id()
        requested_data_source_id = _coerce_optional_id(
            payload.get("holdings_data_source"),
            field_name="holdings_data_source",
        )
        if (
            requested_data_source_id is not None
            and requested_data_source_id != holdings_data_source_id
        ):
            raise ValueError(
                "AccountHoldings can only bind accounts to its own holdings "
                "data node."
            )
        payload["holdings_data_source"] = holdings_data_source_id
        return payload

    def _resolve_account(
        self,
        account: ClientAccount | str | UUID,
        *,
        timeout=None,
    ) -> ClientAccount:
        if isinstance(account, ClientAccount):
            return account
        uid = getattr(account, "uid", None)
        if uid is not None:
            return account
        return ClientAccount.get(uid=str(account), timeout=timeout)

    def _account_for_operational_write(
        self,
        account: ClientAccount | str | UUID,
        *,
        bind_if_needed: bool,
        timeout=None,
    ) -> ClientAccount:
        account_obj = self._resolve_account(account, timeout=timeout)
        if bind_if_needed:
            return self.bind_account(account_obj, timeout=timeout)
        return account_obj


class VirtualFundHoldings(HoldingsDataNode):
    """DataNode users can subclass to import virtual-fund holdings."""

    HOLDINGS_CONTRACT = VIRTUAL_FUND_HOLDINGS_CONTRACT
    MOCK_OWNER_UID = MOCK_FUND_UID

    @classmethod
    def build_mock_fund_frame(
        cls,
        *,
        fund_uid: UUID | str | None = None,
        **kwargs,
    ) -> pd.DataFrame:
        return cls.build_mock_frame(owner_uid=fund_uid, **kwargs)


def _ensure_contract_index(
    data_frame: pd.DataFrame,
    *,
    contract: HoldingsDataNodeContract,
) -> pd.DataFrame:
    expected_index_names = list(contract.index_names)
    frame = data_frame.copy()
    if list(frame.index.names) == expected_index_names:
        return frame
    if all(index_name in frame.columns for index_name in expected_index_names):
        return frame.set_index(expected_index_names)
    raise ValueError(
        "Holdings frame must use index_names "
        f"{expected_index_names} or include those columns before validation."
    )


def _normalize_contract_values(
    frame: pd.DataFrame,
    *,
    contract: HoldingsDataNodeContract,
) -> pd.DataFrame:
    normalized = frame.copy()
    for column_name, dtype in contract.column_dtypes_map.items():
        values = normalized[column_name]
        if column_name == contract.time_index_name:
            normalized[column_name] = pd.to_datetime(values, utc=True)
        elif dtype == "uuid":
            normalized[column_name] = values.map(_normalize_uuid)
        elif dtype == "decimal":
            normalized[column_name] = values.map(_normalize_decimal)
        elif dtype == "bool":
            normalized[column_name] = values.map(_normalize_bool)
        elif dtype == "jsonb":
            normalized[column_name] = values.map(_normalize_jsonb)
        elif dtype == "datetime64[ns, UTC]":
            normalized[column_name] = values.map(_normalize_datetime_payload)
        elif dtype == "object":
            normalized[column_name] = values.map(str)
        else:
            raise ValueError(f"Unsupported holdings dtype {dtype!r} for {column_name!r}.")
    return normalized


def _normalize_uuid(value: Any) -> str:
    if pd.isna(value):
        raise ValueError("UUID holdings columns cannot contain null values.")
    return str(UUID(str(value)))


def _normalize_decimal(value: Any) -> str:
    if pd.isna(value):
        return "0"
    try:
        return str(Decimal(str(value)))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"Invalid decimal holdings value {value!r}.") from exc


def _normalize_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.lower() in {"true", "false"}:
        return value.lower() == "true"
    raise ValueError(f"Invalid boolean holdings value {value!r}.")


def _normalize_jsonb(value: Any) -> dict[str, Any] | list[Any]:
    if value is None:
        return {}
    if isinstance(value, (dict, list)):
        return value
    raise ValueError(f"Invalid jsonb holdings value {value!r}.")


def _normalize_datetime_payload(value: Any) -> str:
    if value is None or pd.isna(value):
        return pd.Timestamp(MOCK_TIME_INDEX).isoformat()
    return pd.to_datetime(value, utc=True).isoformat()


def _attach_logical_dtype_contract(
    frame: pd.DataFrame,
    *,
    contract: HoldingsDataNodeContract,
) -> pd.DataFrame:
    frame.attrs[LOGICAL_COLUMN_DTYPES_ATTR] = dict(contract.column_dtypes_map)
    return frame


def _coerce_required_id(value: Any, *, field_name: str) -> int:
    value_id = _coerce_optional_id(value, field_name=field_name)
    if value_id is None:
        raise ValueError(f"{field_name} must expose an integer id.")
    return value_id


def _coerce_optional_id(value: Any, *, field_name: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    value_id = getattr(value, "id", None)
    if value_id is not None:
        return int(value_id)
    if isinstance(value, dict) and value.get("id") is not None:
        return int(value["id"])
    raise TypeError(f"{field_name} must be an int id or an object with .id.")


__all__ = [
    "ACCOUNT_HOLDINGS_COLUMN_DTYPES_MAP",
    "ACCOUNT_HOLDINGS_CONTRACT",
    "ACCOUNT_HOLDINGS_INDEX_NAMES",
    "ACCOUNT_HOLDINGS_TIME_INDEX_NAME",
    "AccountHoldings",
    "HoldingsDataNode",
    "HoldingsDataNodeConfiguration",
    "HoldingsDataNodeContract",
    "MOCK_ACCOUNT_UID",
    "MOCK_FUND_UID",
    "MOCK_HOLDINGS_SET_UID",
    "MOCK_TIME_INDEX",
    "MOCK_UNIQUE_IDENTIFIER",
    "VIRTUAL_FUND_HOLDINGS_COLUMN_DTYPES_MAP",
    "VIRTUAL_FUND_HOLDINGS_CONTRACT",
    "VIRTUAL_FUND_HOLDINGS_INDEX_NAMES",
    "VIRTUAL_FUND_HOLDINGS_TIME_INDEX_NAME",
    "VirtualFundHoldings",
]
