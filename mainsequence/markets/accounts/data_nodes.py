from __future__ import annotations

import datetime as dt
from decimal import Decimal, InvalidOperation
from typing import Any
from uuid import UUID

import pandas as pd

from mainsequence.client import Asset
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
    "time_index": (
        "UTC timestamp for the account holdings snapshot. Rows with the same "
        "account_uid and time_index belong to the same account observation."
    ),
    "account_uid": (
        "Stable public Account UID that owns the holdings row. This dimension "
        "scopes holdings history to one account."
    ),
    "unique_identifier": (
        "Asset unique identifier for the held instrument at this account "
        "timestamp. Asset details can be resolved through joined asset queries "
        "using this identifier."
    ),
    "holdings_set_uid": (
        "Stable UUID shared by rows written together as one account holdings "
        "set."
    ),
    "is_trade_snapshot": (
        "Whether the holdings row belongs to an execution or trade snapshot "
        "instead of a regular holdings observation."
    ),
    "quantity": "Position quantity held for this asset in the account snapshot.",
    "target_trade_time": (
        "Requested or expected execution time for this row when provided."
    ),
    "extra_details": (
        "JSONB payload for provider-specific or workflow-specific holdings "
        "attributes that are not part of the canonical schema."
    ),
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
    "time_index": (
        "UTC timestamp for the virtual-fund holdings snapshot. Rows with the "
        "same fund_uid and time_index belong to the same fund observation."
    ),
    "fund_uid": (
        "Stable public VirtualFund UID that owns the holdings row. This "
        "dimension scopes holdings history to one virtual fund."
    ),
    "unique_identifier": (
        "Asset unique identifier for the held instrument at this fund "
        "timestamp. Asset details can be resolved through joined asset queries "
        "using this identifier."
    ),
    "holdings_set_uid": (
        "Stable UUID shared by rows written together as one virtual-fund "
        "holdings set."
    ),
    "is_trade_snapshot": (
        "Whether the holdings row belongs to an execution or trade snapshot "
        "instead of a regular holdings observation."
    ),
    "quantity": "Position quantity held for this asset in the fund snapshot.",
    "target_weight": (
        "Target portfolio weight for this asset in the virtual-fund snapshot "
        "when available."
    ),
    "target_trade_time": (
        "Requested or expected execution time for this row when provided."
    ),
    "extra_details": (
        "JSONB payload for provider-specific or workflow-specific holdings "
        "attributes that are not part of the canonical schema."
    ),
}

SCHEMA_BOOTSTRAP_ACCOUNT_UID = UUID("00000000-0000-0000-0000-000000000001")
SCHEMA_BOOTSTRAP_FUND_UID = UUID("00000000-0000-0000-0000-000000000002")
SCHEMA_BOOTSTRAP_HOLDINGS_SET_UID = UUID("00000000-0000-0000-0000-000000000100")
SCHEMA_BOOTSTRAP_ROW_IDENTIFIER = "__schema_bootstrap__"
SCHEMA_BOOTSTRAP_TIME_INDEX = dt.datetime(1970, 1, 1, tzinfo=dt.UTC)


class HoldingsDataNodeConfiguration(DataNodeConfiguration):
    """Configuration base for SDK-created holdings data nodes."""

    time_index_name: str
    index_names: list[str]
    records: list[RecordDefinition]

    @property
    def owner_index_name(self) -> str:
        return self.index_names[1]

    @property
    def row_identifier_index_name(self) -> str:
        return self.index_names[-1]

    @property
    def column_dtypes_map(self) -> dict[str, str]:
        return {
            record.column_name: record.dtype
            for record in self.records
        }


class HoldingsDataNode(DataNode):
    """Base class for holdings tables created through the standard DataNode path."""

    def __init__(
        self,
        config: HoldingsDataNodeConfiguration | None = None,
        *args,
        **kwargs,
    ):
        resolved_config = self._validate_config(config or self.default_config())
        super().__init__(resolved_config, *args, **kwargs)

    def dependencies(self) -> dict[str, DataNode]:
        return {}

    @classmethod
    def default_config(
        cls,
        *,
        identifier: str | None = None,
        description: str | None = None,
        extra_records: list[RecordDefinition] | None = None,
    ) -> HoldingsDataNodeConfiguration:
        return cls._validate_config(
            HoldingsDataNodeConfiguration(
                time_index_name=cls._required_time_index_name(),
                index_names=cls._required_index_names(),
                records=cls._records_with_extra(extra_records=extra_records),
                node_metadata=DataNodeMetaData(
                    identifier=identifier or cls._default_identifier(),
                    description=description or cls._default_description(),
                ),
            )
        )

    @classmethod
    def _records_with_extra(
        cls,
        *,
        extra_records: list[RecordDefinition] | None = None,
    ) -> list[RecordDefinition]:
        required_records = cls._required_records()
        resolved_records = (
            _merge_records(required_records, extra_records)
            if extra_records
            else list(required_records)
        )
        _validate_required_records(
            records=resolved_records,
            required_records=required_records,
        )
        return resolved_records

    @classmethod
    def _validate_config(
        cls,
        config: HoldingsDataNodeConfiguration,
    ) -> HoldingsDataNodeConfiguration:
        if not isinstance(config, HoldingsDataNodeConfiguration):
            raise TypeError(
                f"{cls.__name__} requires a HoldingsDataNodeConfiguration."
            )
        if config.time_index_name != cls._required_time_index_name():
            raise ValueError(
                f"{cls.__name__} requires time_index_name "
                f"{cls._required_time_index_name()!r}."
            )
        if config.index_names != cls._required_index_names():
            raise ValueError(
                f"{cls.__name__} requires index_names "
                f"{cls._required_index_names()!r}."
            )
        _validate_required_records(
            records=list(config.records),
            required_records=cls._required_records(),
        )
        return config

    @classmethod
    def _default_identifier(cls) -> str:
        raise NotImplementedError

    @classmethod
    def _default_description(cls) -> str:
        raise NotImplementedError

    @classmethod
    def _required_time_index_name(cls) -> str:
        raise NotImplementedError

    @classmethod
    def _required_index_names(cls) -> list[str]:
        raise NotImplementedError

    @classmethod
    def _required_records(cls) -> list[RecordDefinition]:
        raise NotImplementedError

    @classmethod
    def _schema_bootstrap_owner_uid(cls) -> UUID:
        raise NotImplementedError

    def _holdings_config(self) -> HoldingsDataNodeConfiguration:
        return self.__class__._validate_config(
            getattr(self, "config", None) or self.default_config()
        )

    def update(self) -> pd.DataFrame:
        return _validate_holdings_frame(
            self.get_holdings_frame(),
            config=self._holdings_config(),
        )

    def get_holdings_frame(self) -> pd.DataFrame:
        return self.build_schema_bootstrap_frame(config=self._holdings_config())

    @classmethod
    def build_initialization_frame(cls, **kwargs) -> pd.DataFrame:
        return cls.build_schema_bootstrap_frame(**kwargs)

    @classmethod
    def build_schema_bootstrap_frame(
        cls,
        *,
        config: HoldingsDataNodeConfiguration | None = None,
        owner_uid: UUID | str | None = None,
        row_identifier: str = SCHEMA_BOOTSTRAP_ROW_IDENTIFIER,
        holdings_set_uid: UUID | str = SCHEMA_BOOTSTRAP_HOLDINGS_SET_UID,
        time_index: dt.datetime | pd.Timestamp = SCHEMA_BOOTSTRAP_TIME_INDEX,
    ) -> pd.DataFrame:
        owner_uid = owner_uid or cls._schema_bootstrap_owner_uid()
        config = config or cls.default_config()
        row: dict[str, Any] = {
            config.time_index_name: time_index,
            config.owner_index_name: owner_uid,
            config.row_identifier_index_name: row_identifier,
            "holdings_set_uid": holdings_set_uid,
            "is_trade_snapshot": False,
            "quantity": "0",
            "target_trade_time": pd.Timestamp(time_index).isoformat(),
            "extra_details": {
                "_mainsequence_reserved": "schema_bootstrap",
                "semantic": False,
            },
        }
        for record in config.records or []:
            if record.column_name not in row:
                row[record.column_name] = _schema_bootstrap_value(
                    dtype=record.dtype,
                    time_index=time_index,
                )
        frame = pd.DataFrame([row])
        frame = frame.set_index(config.index_names)
        return _validate_holdings_frame(frame, config=config)

    @classmethod
    def build_mock_frame(cls, **kwargs) -> pd.DataFrame:
        return cls.build_schema_bootstrap_frame(**kwargs)

    @classmethod
    def validate_holdings_frame(cls, data_frame: pd.DataFrame) -> pd.DataFrame:
        return _validate_holdings_frame(
            data_frame,
            config=cls.default_config(),
        )

    def holdings_data_source_id(self) -> int:
        return self.ensure_storage_ready()

    def ensure_storage_ready(self, *, force_update: bool = False) -> int:
        storage = None if force_update else self._ready_storage_or_none()
        if storage is None and not force_update:
            storage = self._initialize_source_table_storage_or_none()
        if storage is None:
            self.run(debug_mode=True, update_tree=False, force_update=True)
            storage = self._ready_storage_or_none()

        if storage is None:
            raise RuntimeError(
                f"{self.__class__.__name__} did not create a ready holdings "
                "data node. Run the DataNode bootstrap path before writing "
                "holdings."
            )
        return _coerce_required_id(storage, field_name="data_node_storage")

    def _initialize_source_table_storage_or_none(self):
        storage = self.data_node_storage
        if _coerce_optional_id(storage, field_name="data_node_storage") is None:
            return None

        config = self._holdings_config()
        try:
            self._initialize_source_table(storage=storage, config=config)
        except Exception as exc:
            status_code = getattr(exc, "status_code", None)
            if status_code in {404, 405}:
                return None
            raise

        source_config = _storage_source_config(storage)
        if source_config is None:
            return None
        self._validate_storage_contract(source_config)
        return storage

    def _initialize_source_table(
        self,
        *,
        storage: Any,
        config: HoldingsDataNodeConfiguration,
    ) -> None:
        raise NotImplementedError

    def _ready_storage_or_none(self):
        storage = self.data_node_storage
        if _coerce_optional_id(storage, field_name="data_node_storage") is None:
            return None

        source_config = _storage_source_config(storage)
        if source_config is None:
            return None

        self._validate_storage_contract(source_config)
        return storage

    def _validate_storage_contract(self, source_config: Any) -> None:
        config = self._holdings_config()
        errors: list[str] = []

        time_index_name = _get_mapping_or_attr(source_config, "time_index_name")
        if time_index_name != config.time_index_name:
            errors.append(
                "time_index_name "
                f"{time_index_name!r} does not match {config.time_index_name!r}"
            )

        index_names = list(_get_mapping_or_attr(source_config, "index_names") or [])
        if index_names != config.index_names:
            errors.append(
                f"index_names {index_names!r} do not match {config.index_names!r}"
            )

        column_dtypes_map = dict(
            _get_mapping_or_attr(source_config, "column_dtypes_map") or {}
        )
        for column_name, expected_dtype in config.column_dtypes_map.items():
            actual_dtype = column_dtypes_map.get(column_name)
            if actual_dtype != expected_dtype:
                errors.append(
                    f"{column_name!r} dtype {actual_dtype!r} does not match "
                    f"{expected_dtype!r}"
                )

        if errors:
            raise ValueError(
                f"{self.__class__.__name__} is bound to an incompatible "
                "holdings data node: "
                + "; ".join(errors)
            )


class AccountHoldings(HoldingsDataNode):
    """DataNode users can subclass to import account holdings."""

    @classmethod
    def _default_identifier(cls) -> str:
        return "mainsequence.markets.account_historical_holdings"

    @classmethod
    def _default_description(cls) -> str:
        return (
            "Timestamped account holdings DataNode. The table is indexed by "
            "time_index, account_uid, and the asset unique_identifier so account "
            "position history can be queried over time and enriched through joined "
            "asset metadata queries."
        )

    @classmethod
    def _required_time_index_name(cls) -> str:
        return ACCOUNT_HOLDINGS_TIME_INDEX_NAME

    @classmethod
    def _required_index_names(cls) -> list[str]:
        return list(ACCOUNT_HOLDINGS_INDEX_NAMES)

    @classmethod
    def _required_records(cls) -> list[RecordDefinition]:
        return _record_definitions_from_dtype_map(
            ACCOUNT_HOLDINGS_COLUMN_DTYPES_MAP,
            labels=ACCOUNT_HOLDINGS_COLUMN_LABELS,
            descriptions=ACCOUNT_HOLDINGS_COLUMN_DESCRIPTIONS,
        )

    @classmethod
    def _schema_bootstrap_owner_uid(cls) -> UUID:
        return SCHEMA_BOOTSTRAP_ACCOUNT_UID

    @classmethod
    def build_schema_bootstrap_account_frame(
        cls,
        *,
        account_uid: UUID | str | None = None,
        **kwargs,
    ) -> pd.DataFrame:
        return cls.build_schema_bootstrap_frame(owner_uid=account_uid, **kwargs)

    @classmethod
    def build_mock_account_frame(cls, **kwargs) -> pd.DataFrame:
        return cls.build_schema_bootstrap_account_frame(**kwargs)

    def _initialize_source_table(
        self,
        *,
        storage: Any,
        config: HoldingsDataNodeConfiguration,
    ) -> None:
        storage.initialize_account_holdings_source_table(
            time_index_name=config.time_index_name,
            index_names=config.index_names,
            column_dtypes_map=config.column_dtypes_map,
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
        holdings_data_source_id: int | None = None,
        timeout=None,
    ) -> ClientAccount:
        holdings_data_source_id = (
            holdings_data_source_id
            if holdings_data_source_id is not None
            else self.holdings_data_source_id()
        )
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
        _validate_position_asset_unique_identifiers(positions, timeout=timeout)
        account_obj = self._account_for_holdings_write(
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
        account_obj = self._account_for_holdings_write(
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

    def _account_for_holdings_write(
        self,
        account: ClientAccount | str | UUID,
        *,
        bind_if_needed: bool,
        timeout=None,
    ) -> ClientAccount:
        holdings_data_source_id = self.ensure_storage_ready()
        account_obj = self._resolve_account(account, timeout=timeout)
        if bind_if_needed:
            return self.bind_account(
                account_obj,
                holdings_data_source_id=holdings_data_source_id,
                timeout=timeout,
            )
        return account_obj


class VirtualFundHoldings(HoldingsDataNode):
    """DataNode users can subclass to import virtual-fund holdings."""

    @classmethod
    def _default_identifier(cls) -> str:
        return "mainsequence.markets.virtual_fund_historical_holdings"

    @classmethod
    def _default_description(cls) -> str:
        return (
            "Timestamped virtual-fund holdings DataNode. The table is indexed by "
            "time_index, fund_uid, and the asset unique_identifier so fund position "
            "history can be queried over time and enriched through joined asset "
            "metadata queries."
        )

    @classmethod
    def _required_time_index_name(cls) -> str:
        return VIRTUAL_FUND_HOLDINGS_TIME_INDEX_NAME

    @classmethod
    def _required_index_names(cls) -> list[str]:
        return list(VIRTUAL_FUND_HOLDINGS_INDEX_NAMES)

    @classmethod
    def _required_records(cls) -> list[RecordDefinition]:
        return _record_definitions_from_dtype_map(
            VIRTUAL_FUND_HOLDINGS_COLUMN_DTYPES_MAP,
            labels=VIRTUAL_FUND_HOLDINGS_COLUMN_LABELS,
            descriptions=VIRTUAL_FUND_HOLDINGS_COLUMN_DESCRIPTIONS,
        )

    @classmethod
    def _schema_bootstrap_owner_uid(cls) -> UUID:
        return SCHEMA_BOOTSTRAP_FUND_UID

    @classmethod
    def build_schema_bootstrap_fund_frame(
        cls,
        *,
        fund_uid: UUID | str | None = None,
        **kwargs,
    ) -> pd.DataFrame:
        return cls.build_schema_bootstrap_frame(owner_uid=fund_uid, **kwargs)

    @classmethod
    def build_mock_fund_frame(cls, **kwargs) -> pd.DataFrame:
        return cls.build_schema_bootstrap_fund_frame(**kwargs)

    def _initialize_source_table(
        self,
        *,
        storage: Any,
        config: HoldingsDataNodeConfiguration,
    ) -> None:
        storage.initialize_virtual_fund_holdings_source_table(
            time_index_name=config.time_index_name,
            index_names=config.index_names,
            column_dtypes_map=config.column_dtypes_map,
        )


def _record_definitions_from_dtype_map(
    column_dtypes_map: dict[str, str],
    *,
    labels: dict[str, str],
    descriptions: dict[str, str],
) -> list[RecordDefinition]:
    return [
        RecordDefinition(
            column_name=column_name,
            dtype=dtype,
            label=labels.get(column_name, column_name),
            description=descriptions.get(column_name),
        )
        for column_name, dtype in column_dtypes_map.items()
    ]


def _merge_records(
    required_records: list[RecordDefinition],
    extra_records: list[RecordDefinition],
) -> list[RecordDefinition]:
    merged_records = list(required_records)
    existing_dtypes = {
        record.column_name: record.dtype
        for record in required_records
    }
    for record in extra_records:
        existing_dtype = existing_dtypes.get(record.column_name)
        if existing_dtype is not None:
            if existing_dtype != record.dtype:
                raise ValueError(
                    f"Extra record {record.column_name!r} has dtype "
                    f"{record.dtype!r}, but the required dtype is "
                    f"{existing_dtype!r}."
                )
            continue
        merged_records.append(record)
        existing_dtypes[record.column_name] = record.dtype
    return merged_records


def _validate_required_records(
    *,
    records: list[RecordDefinition],
    required_records: list[RecordDefinition],
) -> None:
    dtype_by_column = {
        record.column_name: record.dtype
        for record in records
    }
    errors = []
    for required_record in required_records:
        actual_dtype = dtype_by_column.get(required_record.column_name)
        if actual_dtype != required_record.dtype:
            errors.append(
                f"{required_record.column_name!r} dtype {actual_dtype!r} "
                f"does not match required dtype {required_record.dtype!r}"
            )
    if errors:
        raise ValueError(
            "Holdings records must include the required columns: "
            + "; ".join(errors)
        )


def _schema_bootstrap_value(
    *,
    dtype: str,
    time_index: dt.datetime | pd.Timestamp,
) -> Any:
    if dtype == "uuid":
        return SCHEMA_BOOTSTRAP_HOLDINGS_SET_UID
    if dtype == "decimal":
        return "0"
    if dtype == "bool":
        return False
    if dtype == "jsonb":
        return {}
    if dtype == "datetime64[ns, UTC]":
        return pd.Timestamp(time_index).isoformat()
    return ""


def _validate_holdings_frame(
    data_frame: pd.DataFrame,
    *,
    config: HoldingsDataNodeConfiguration,
) -> pd.DataFrame:
    frame = _ensure_config_index(data_frame, config=config)
    flat = frame.reset_index()
    missing_columns = [
        column_name
        for column_name in config.column_dtypes_map
        if column_name not in flat.columns
    ]
    if missing_columns:
        raise ValueError(
            "Holdings frame is missing required columns: "
            f"{', '.join(missing_columns)}."
        )

    flat = _normalize_config_values(flat, config=config)
    frame = flat.set_index(config.index_names).sort_index()
    if frame.index.has_duplicates:
        raise ValueError(
            "Holdings frame contains duplicate rows for index contract "
            f"{config.index_names}."
        )
    return _attach_logical_dtype_contract(frame, config=config)


def _ensure_config_index(
    data_frame: pd.DataFrame,
    *,
    config: HoldingsDataNodeConfiguration,
) -> pd.DataFrame:
    expected_index_names = list(config.index_names)
    frame = data_frame.copy()
    if list(frame.index.names) == expected_index_names:
        return frame
    if all(index_name in frame.columns for index_name in expected_index_names):
        return frame.set_index(expected_index_names)
    raise ValueError(
        "Holdings frame must use index_names "
        f"{expected_index_names} or include those columns before validation."
    )


def _normalize_config_values(
    frame: pd.DataFrame,
    *,
    config: HoldingsDataNodeConfiguration,
) -> pd.DataFrame:
    normalized = frame.copy()
    for column_name, dtype in config.column_dtypes_map.items():
        values = normalized[column_name]
        if column_name == config.time_index_name:
            normalized[column_name] = _normalize_time_index(values)
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
        return pd.Timestamp(SCHEMA_BOOTSTRAP_TIME_INDEX).isoformat()
    return pd.to_datetime(value, utc=True).isoformat()


def _normalize_time_index(values: Any) -> pd.Series:
    return pd.to_datetime(values, utc=True).astype("datetime64[ns, UTC]")


def _attach_logical_dtype_contract(
    frame: pd.DataFrame,
    *,
    config: HoldingsDataNodeConfiguration,
) -> pd.DataFrame:
    frame.attrs[LOGICAL_COLUMN_DTYPES_ATTR] = dict(config.column_dtypes_map)
    return frame


def _storage_source_config(storage: Any) -> Any | None:
    return (
        _get_mapping_or_attr(storage, "sourcetableconfiguration")
        or _get_mapping_or_attr(storage, "source_table_configuration")
        or _get_mapping_or_attr(storage, "source_table_config")
    )


def _get_mapping_or_attr(value: Any, field_name: str) -> Any:
    if isinstance(value, dict):
        return value.get(field_name)
    return getattr(value, field_name, None)


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


def _validate_position_asset_unique_identifiers(
    positions: list[ClientAccountHoldingsWritePosition | dict[str, Any]],
    *,
    timeout=None,
) -> None:
    unique_identifiers = [
        unique_identifier
        for unique_identifier in (
            _position_unique_identifier(position)
            for position in positions
        )
        if unique_identifier is not None
    ]
    if len(unique_identifiers) != len(positions):
        raise ValueError(
            "Every account holdings position must include unique_identifier."
        )
    if not unique_identifiers:
        return

    requested_identifiers = sorted(set(unique_identifiers))
    assets = Asset.filter(
        unique_identifier__in=requested_identifiers,
        timeout=timeout,
    )
    found_identifiers = {
        unique_identifier
        for unique_identifier in (
            _asset_unique_identifier(asset)
            for asset in assets
        )
        if unique_identifier is not None
    }
    missing_identifiers = [
        unique_identifier
        for unique_identifier in requested_identifiers
        if unique_identifier not in found_identifiers
    ]
    if missing_identifiers:
        raise ValueError(
            "All account holdings positions must reference existing "
            "MainSequence assets. Missing unique_identifier values: "
            + ", ".join(missing_identifiers)
            + "."
        )


def _position_unique_identifier(
    position: ClientAccountHoldingsWritePosition | dict[str, Any],
) -> str | None:
    if isinstance(position, dict):
        value = position.get("unique_identifier")
    else:
        value = getattr(position, "unique_identifier", None)
    if value is None:
        return None
    return str(value)


def _asset_unique_identifier(asset: Any) -> str | None:
    if isinstance(asset, dict):
        value = asset.get("unique_identifier")
    else:
        value = getattr(asset, "unique_identifier", None)
    if value is None:
        return None
    return str(value)


__all__ = [
    "ACCOUNT_HOLDINGS_COLUMN_DTYPES_MAP",
    "ACCOUNT_HOLDINGS_INDEX_NAMES",
    "ACCOUNT_HOLDINGS_TIME_INDEX_NAME",
    "AccountHoldings",
    "HoldingsDataNode",
    "HoldingsDataNodeConfiguration",
    "SCHEMA_BOOTSTRAP_ACCOUNT_UID",
    "SCHEMA_BOOTSTRAP_FUND_UID",
    "SCHEMA_BOOTSTRAP_HOLDINGS_SET_UID",
    "SCHEMA_BOOTSTRAP_ROW_IDENTIFIER",
    "SCHEMA_BOOTSTRAP_TIME_INDEX",
    "VIRTUAL_FUND_HOLDINGS_COLUMN_DTYPES_MAP",
    "VIRTUAL_FUND_HOLDINGS_INDEX_NAMES",
    "VIRTUAL_FUND_HOLDINGS_TIME_INDEX_NAME",
    "VirtualFundHoldings",
]
