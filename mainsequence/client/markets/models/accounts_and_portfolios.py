import copy
import datetime
import json
from decimal import Decimal
from enum import Enum, IntEnum
from typing import Any, ClassVar, Optional, Union

import pytz
from pydantic import BaseModel, Field, model_validator

from mainsequence.client.base import BaseObjectOrm, BasePydanticModel
from mainsequence.client.exceptions import raise_for_response
from mainsequence.client.markets.models.assets import (
    Asset,
    AssetMixin,
    PortfolioIndexAsset,
    resolve_asset,
)
from mainsequence.client.markets.models.core import Calendar
from mainsequence.client.models_tdag import DataNodeUpdate
from mainsequence.client.utils import DATE_FORMAT, make_request


class AccountHoldingPosition(BasePydanticModel):
    time_index: datetime.datetime
    unique_identifier: str
    quantity: Decimal
    target_trade_time: datetime.datetime | None = None
    extra_details: dict[str, Any] = Field(default_factory=dict)
    asset: AssetMixin | None = None

    @model_validator(mode="before")
    def resolve_asset(cls, values):
        asset = values.get("asset")
        if isinstance(asset, dict):
            values["asset"] = resolve_asset(asset)
        return values


class AccountHoldingsWritePosition(BasePydanticModel):
    unique_identifier: str
    quantity: Decimal
    target_trade_time: datetime.datetime
    extra_details: dict[str, Any] = Field(default_factory=dict)


class AccountLatestHoldings(BasePydanticModel):
    holdings_set_uid: str
    is_trade_snapshot: bool = False
    target_trade_time: datetime.datetime | None = None
    comments: str | None = None
    holdings: list[AccountHoldingPosition]
    holdings_date: datetime.datetime


class AccountHoldingsSnapshot(AccountLatestHoldings):
    snapshot_uid: str
    related_account_uid: str


class AccountTargetPosition(BasePydanticModel):
    unique_identifier: str
    weight_notional_exposure: Decimal | None = None
    constant_notional_exposure: Decimal | None = None
    single_asset_quantity: Decimal | None = None
    asset: AssetMixin | None = None

    @model_validator(mode="before")
    def resolve_asset(cls, values):
        asset = values.get("asset")
        if isinstance(asset, dict):
            values["asset"] = resolve_asset(asset)
        return values

    @model_validator(mode="after")
    def validate_exposure_shape(self):
        provided_fields = [
            field_name
            for field_name in (
                "weight_notional_exposure",
                "constant_notional_exposure",
                "single_asset_quantity",
            )
            if getattr(self, field_name) is not None
        ]
        if len(provided_fields) != 1:
            raise ValueError(
                "Each target position must provide exactly one of "
                "`weight_notional_exposure`, `constant_notional_exposure`, "
                "or `single_asset_quantity`."
            )
        return self


class AccountTargetPositionsSnapshot(BasePydanticModel):
    related_account_uid: str
    target_positions_date: datetime.datetime
    position_set_uid: str
    positions: list[AccountTargetPosition]


class AccountHoldingsWriteResponse(BasePydanticModel):
    related_account_uid: str
    holdings_date: datetime.datetime
    holdings_set_uid: str
    comments: str | None = None
    positions: list[AccountHoldingsWritePosition]


class AccountTargetPositionsWriteResponse(BasePydanticModel):
    related_account_uid: str
    target_positions_date: datetime.datetime
    position_set_uid: str
    positions: list[AccountTargetPosition]


class AccountMixin(BasePydanticModel):
    uid: str
    account_is_active: bool
    account_name: str | None = None
    holdings_data_source: dict[str, Any] | None = None
    labels: list[str] = Field(default_factory=list)
    latest_holdings: AccountLatestHoldings | None = None
    is_paper: bool

    @classmethod
    def _coerce_optional_uid(cls, value: Any, *, field_name: str) -> str | None:
        if value is None:
            return None
        if isinstance(value, str):
            return value
        if hasattr(value, "uid") and value.uid is not None:
            return str(value.uid)
        if isinstance(value, dict) and value.get("uid") is not None:
            return str(value["uid"])
        raise TypeError(f"{field_name} must be a uid or an object with .uid.")

    @classmethod
    def _normalize_write_kwargs(cls, kwargs: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(kwargs)
        if "holdings_data_source" in normalized:
            raise TypeError(
                "Use holdings_data_source_uid for account storage binding. "
                "holdings_data_source is a read field."
            )
        if "holdings_data_source_uid" in normalized:
            normalized["holdings_data_source_uid"] = cls._coerce_optional_uid(
                normalized["holdings_data_source_uid"],
                field_name="holdings_data_source_uid",
            )
        return normalized

    @classmethod
    def get(cls, pk=None, timeout=None, **filters):
        uid = filters.pop("uid", None)
        if pk is None and uid is not None:
            pk = uid
        return super().get(pk=pk, timeout=timeout, **filters)

    def _detail_url(self) -> str:
        base_url = type(self).get_object_url().rstrip("/")
        return f"{base_url}/{self.uid}/"

    def get_holdings(
        self,
        *,
        holdings_date: datetime.datetime | str | None = None,
        order: str = "desc",
        limit: int | None = None,
        include_asset_detail: bool = False,
        timeout=None,
    ) -> list[AccountHoldingsSnapshot]:
        params: dict[str, Any] = {
            "order": order,
            "include_asset_detail": str(include_asset_detail).lower(),
        }
        if holdings_date is not None:
            params["holdings_date"] = (
                holdings_date.isoformat()
                if isinstance(holdings_date, datetime.datetime)
                else str(holdings_date)
            )
        if limit is not None:
            params["limit"] = int(limit)

        r = make_request(
            s=self.build_session(),
            loaders=self.LOADERS,
            r_type="GET",
            url=f"{self._detail_url()}holdings/",
            payload={"params": params},
            time_out=timeout,
        )
        if r.status_code != 200:
            raise_for_response(r)
        return [AccountHoldingsSnapshot(**item) for item in r.json()]

    def get_latest_holdings(
        self,
        *,
        include_asset_detail: bool = False,
        timeout=None,
    ) -> AccountHoldingsSnapshot | None:
        snapshots = self.get_holdings(
            order="desc",
            limit=1,
            include_asset_detail=include_asset_detail,
            timeout=timeout,
        )
        return snapshots[0] if snapshots else None

    def add_holdings(
        self,
        *,
        holdings_date: datetime.datetime | str,
        positions: list[AccountHoldingsWritePosition | dict[str, Any]],
        overwrite: bool = False,
        timeout=None,
    ) -> AccountHoldingsWriteResponse:
        payload = {
            "holdings_date": holdings_date,
            "overwrite": overwrite,
            "positions": [
                position.model_dump()
                if isinstance(position, BaseModel)
                else position
                for position in positions
            ],
        }
        r = make_request(
            s=self.build_session(),
            loaders=self.LOADERS,
            r_type="POST",
            url=f"{self._detail_url()}add-holdings/",
            payload={"json": self.serialize_for_json(payload)},
            time_out=timeout,
        )
        if r.status_code not in (200, 201):
            raise_for_response(r)
        return AccountHoldingsWriteResponse(**r.json())

    def get_target_positions(
        self,
        *,
        target_positions_date: datetime.datetime | str | None = None,
        order: str = "desc",
        limit: int | None = None,
        include_asset_detail: bool = False,
        timeout=None,
    ) -> list[AccountTargetPositionsSnapshot]:
        params: dict[str, Any] = {
            "order": order,
            "include_asset_detail": str(include_asset_detail).lower(),
        }
        if target_positions_date is not None:
            params["target_positions_date"] = (
                target_positions_date.isoformat()
                if isinstance(target_positions_date, datetime.datetime)
                else str(target_positions_date)
            )
        if limit is not None:
            params["limit"] = int(limit)

        r = make_request(
            s=self.build_session(),
            loaders=self.LOADERS,
            r_type="GET",
            url=f"{self._detail_url()}target-positions/",
            payload={"params": params},
            time_out=timeout,
        )
        if r.status_code != 200:
            raise_for_response(r)
        return [AccountTargetPositionsSnapshot(**item) for item in r.json()]

    def get_latest_target_positions(
        self,
        *,
        include_asset_detail: bool = False,
        timeout=None,
    ) -> AccountTargetPositionsSnapshot | None:
        snapshots = self.get_target_positions(
            order="desc",
            limit=1,
            include_asset_detail=include_asset_detail,
            timeout=timeout,
        )
        return snapshots[0] if snapshots else None

    def add_target_positions(
        self,
        *,
        target_positions_date: datetime.datetime | str,
        positions: list[AccountTargetPosition | dict[str, Any]],
        overwrite: bool = False,
        timeout=None,
    ) -> AccountTargetPositionsWriteResponse:
        payload = {
            "target_positions_date": target_positions_date,
            "overwrite": overwrite,
            "positions": [
                position.model_dump(exclude={"asset"})
                if isinstance(position, BaseModel)
                else position
                for position in positions
            ],
        }
        r = make_request(
            s=self.build_session(),
            loaders=self.LOADERS,
            r_type="POST",
            url=f"{self._detail_url()}add-target-positions/",
            payload={"json": self.serialize_for_json(payload)},
            time_out=timeout,
        )
        if r.status_code not in (200, 201):
            raise_for_response(r)
        return AccountTargetPositionsWriteResponse(**r.json())


class Account(AccountMixin, BaseObjectOrm, BasePydanticModel):
    @classmethod
    def create(cls, timeout=None, **kwargs):
        normalized = cls._normalize_write_kwargs(kwargs)
        return super().create(timeout=timeout, **normalized)

    @classmethod
    def get_or_create(
        cls,
        create_without_holdings: bool = False,
        timeout=None,
        **kwargs,
    ):
        base_url = cls.get_object_url()
        url = f"{base_url}/get-or-create/"
        payload_kwargs = cls._normalize_write_kwargs(kwargs)
        payload_kwargs["create_without_holdings"] = create_without_holdings
        payload = {"json": cls.serialize_for_json(payload_kwargs)}

        r = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="POST",
            url=url,
            payload=payload,
            time_out=timeout,
        )
        if r.status_code not in [200, 201]:
            raise_for_response(r)
        return cls(**r.json())

    @classmethod
    def patch_by_id(cls, instance_id, *args, _into=None, timeout=None, **kwargs):
        url = f"{cls.get_object_url().rstrip('/')}/{instance_id}/"
        data = cls.serialize_for_json(cls._normalize_write_kwargs(kwargs))
        payload = {"json": data}

        r = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="PATCH",
            url=url,
            payload=payload,
            time_out=timeout,
        )
        if r.status_code != 200:
            raise_for_response(r)
        body = r.json()
        updated = cls(**body)
        if _into is not None:
            for field_name in type(updated).model_fields:
                setattr(_into, field_name, getattr(updated, field_name))
            return _into
        return updated

    @classmethod
    def destroy_by_id(cls, instance_id, *args, timeout=None, **kwargs):
        r = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="DELETE",
            url=f"{cls.get_object_url().rstrip('/')}/{instance_id}/",
            payload={},
            time_out=timeout,
        )
        if r.status_code != 204:
            raise_for_response(r)

    def patch(self, *args, timeout=None, **kwargs):
        updated = type(self).patch_by_id(self.uid, timeout=timeout, **kwargs)
        for field_name in type(updated).model_fields:
            setattr(self, field_name, getattr(updated, field_name))
        return self

    def delete(self, *args, timeout=None, **kwargs):
        return self.__class__.destroy_by_id(self.uid, *args, timeout=timeout, **kwargs)


class WeightPosition(BaseObjectOrm, BasePydanticModel):
    # id: Optional[int] = None
    # parent_weights: int
    asset: AssetMixin | int
    weight_notional_exposure: float

    @property
    def asset_id(self):
        return self.asset if isinstance(self.asset, int) else self.asset.id

    @model_validator(mode="before")
    def resolve_assets(cls, values):
        # Check if 'asset' is a dict and determine its type
        if isinstance(values.get("asset"), dict):
            asset = values.get("asset")
            asset = resolve_asset(asset_dict=asset)
            values["asset"] = asset

        return values


class TradeSide(IntEnum):
    SELL = -1
    BUY = 1


class Trade(BaseObjectOrm, BasePydanticModel):
    id: int | None = None

    # Use a default_factory to set the default trade_time to now (with UTC timezone)
    trade_time: datetime.datetime
    trade_side: TradeSide
    asset: AssetMixin | int | None
    quantity: float
    price: float
    commission: float | None
    commission_asset: AssetMixin | int | None

    related_fund: Union["VirtualFund", int] | None
    related_account: Account | int | None
    related_order: Union["Order", int] | None

    settlement_cost: float | None
    settlement_asset: AssetMixin | int | None

    comments: str | None
    venue_specific_properties: dict | None

    @classmethod
    def create_or_update(cls, trade_kwargs, timeout=None) -> None:
        url = f"{cls.get_object_url()}/create_or_update/"
        data = cls.serialize_for_json(trade_kwargs)
        payload = {"json": data}
        r = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="POST",
            url=url,
            payload=payload,
            time_out=timeout,
        )
        if r.status_code !=200:
            raise_for_response(r)
        return cls(**r.json())


class OrdersExecutionConfiguration(BaseModel):
    broker_class: str
    broker_configuration: dict


class PortfolioTags(BasePydanticModel):
    id: int | None = None
    name: str
    color: str





class Portfolio(BaseObjectOrm, BasePydanticModel):
    FILTERSET_FIELDS: ClassVar[dict[str, list[str]]] = {
        "index_asset__unique_identifier": ["in", "exact", "contains"],
        "data_node_update__id": ["in", "exact"],
        "data_node_update": ["isnull"],
        "signal_data_node_update": ["isnull"],
        "id": ["in", "exact"],
    }
    FILTER_VALUE_NORMALIZERS: ClassVar[dict[str, str]] = {
        "data_node_update__id": "id",
        "data_node_update__isnull": "bool",
        "signal_data_node_update__isnull": "bool",
        "id": "id",
    }

    id: int | None = Field(
        None,
        title="Portfolio ID",
        description="Unique identifier of the portfolio record.",
        examples=[42],
        json_schema_extra={"label": "Portfolio ID"},
    )
    data_node_update: Optional["DataNodeUpdate"] = Field(
        ...,
        title="Data Node Update",
        description="Primary data node update backing the portfolio holdings, weights, or positions time series.",
        examples=[{"id": 901, "update_hash": "portfolio_weights_daily"}],
        json_schema_extra={"label": "Data Node Update"},
    )
    signal_data_node_update: Optional["DataNodeUpdate"] = Field(
        ...,
        title="Signal Data Node Update",
        description="Signal-generating data node update associated with this portfolio.",
        examples=[{"id": 902, "update_hash": "rebalance_signal_daily"}],
        json_schema_extra={"label": "Signal Data Node Update"},
    )
    backtest_table_price_column_name: str | None = Field(
        None,
        max_length=20,
        title="Backtest Price Column",
        description="Column name used as the reference price series when backtesting the portfolio.",
        examples=["close"],
        json_schema_extra={"label": "Backtest Price Column"},
    )

    calendar: Optional["Calendar"] = Field(
        ...,
        title="Calendar",
        description="Trading or business calendar used to interpret portfolio dates and rebalance schedules.",
        examples=[{"id": 1, "name": "NYSE"}],
        json_schema_extra={"label": "Calendar"},
    )
    index_asset: PortfolioIndexAsset = Field(
        ...,
        title="Index Asset",
        description="Index asset representing the portfolio instrument and its identity metadata.",
        examples=[{"id": 77}],
        json_schema_extra={"label": "Index Asset"},
    )
    builds_from_target_weights: bool = Field(
        True,
        title="Builds From Target Weights",
        description="Whether the portfolio is constructed from target weights.",
        examples=[True],
        json_schema_extra={"label": "Builds From Target Weights"},
    )
    builds_from_target_positions: bool = Field(
        False,
        title="Builds From Target Positions",
        description="Whether the portfolio is constructed from target positions instead of target weights.",
        examples=[False],
        json_schema_extra={"label": "Builds From Target Positions"},
    )
    creation_date:str | None = Field()

    def pretty_print(self) -> str:
        def format_field(name, value):
            if isinstance(value, list):
                val = ", ".join(str(v) for v in value)
            elif hasattr(value, "__str__"):
                val = str(value)
            else:
                val = repr(value)
            return f"{name:35}: {val}"

        fields = self.__fields__
        lines = [format_field(name, getattr(self, name, None)) for name in fields]
        return "\n".join(lines)

    @classmethod
    def create_from_time_series(
        cls,
        portfolio_name: str,
        data_node_update_id: int,
        signal_data_node_update_id: int | None,
        calendar_name: str,
        backtest_table_price_column_name: str,
        portfolio_description: str | None = None,
        timeout=None,
        metadata_updater: Any | None = None,
    ) -> tuple["Portfolio", PortfolioIndexAsset]:
        url = f"{cls.get_object_url()}/create_from_time_series/"
        # Build the payload with the required arguments.
        payload_data = {
            "portfolio_name": portfolio_name,
            "data_node_update_id": data_node_update_id,
            "signal_data_node_update_id": signal_data_node_update_id,
            # Using the same ID for local_signal_time_serie_id as specified.
            "calendar_name": calendar_name,
            "backtest_table_price_column_name": backtest_table_price_column_name,
        }

        r = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="POST",
            url=url,
            payload={"json": payload_data},
            time_out=timeout,
        )
        if r.status_code not in [201]:
            raise_for_response(r)
        response = r.json()

        portfolio = cls(**response["portfolio"])
        PortfolioIndexAsset.model_rebuild(_types_namespace={"Portfolio": cls})
        portfolio_index_asset = PortfolioIndexAsset(**response["portfolio_index_asset"])
        if portfolio_description is not None:
            from mainsequence.markets.portfolios.simple_tables import (
                upsert_portfolio_metadata,
            )

            upsert_portfolio_metadata(
                portfolio_index_asset=portfolio_index_asset,
                description=portfolio_description,
                updater=metadata_updater,
            )
        return portfolio, portfolio_index_asset

    @classmethod
    def get_or_create_from_configuration_hash(
        cls,
        portfolio_configuration_hash: str,
        portfolio_configuration: dict[str, Any],
        timeout=None,
    ) -> tuple["Portfolio", PortfolioIndexAsset]:
        url = f"{cls.get_object_url()}/get_or_create_from_configuration_hash/"
        payload_data = {
            "portfolio_configuration_hash": portfolio_configuration_hash,
            "portfolio_configuration": portfolio_configuration,
        }

        r = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="POST",
            url=url,
            payload={"json": payload_data},
            time_out=timeout,
        )
        if r.status_code not in [200, 201]:
            raise_for_response(r)
        response = r.json()

        PortfolioIndexAsset.model_rebuild(_types_namespace={"Portfolio": cls})
        return cls(**response["portfolio"]), PortfolioIndexAsset(
            **response["portfolio_index_asset"]
        )

    @property
    def portfolio_name(self) -> str:
        return self.index_asset.current_snapshot.name

    @property
    def portfolio_ticker(self) -> str:
        return self.index_asset.current_snapshot.ticker

    def _portfolio_index_asset_unique_identifier(self) -> str:
        unique_identifier = getattr(self.index_asset, "unique_identifier", None)
        if not unique_identifier:
            raise ValueError("Portfolio.index_asset must expose unique_identifier.")
        return str(unique_identifier)

    def get_metadata(self, *, updater: Any | None = None):
        from mainsequence.markets.portfolios.simple_tables import (
            get_portfolio_metadata,
        )

        return get_portfolio_metadata(
            self._portfolio_index_asset_unique_identifier(),
            updater=updater,
        )

    def get_description(self, *, updater: Any | None = None) -> str | None:
        metadata = self.get_metadata(updater=updater)
        return None if metadata is None else metadata.description

    def upsert_metadata(
        self,
        *,
        description: str | None = None,
        updater: Any | None = None,
    ):
        from mainsequence.markets.portfolios.simple_tables import (
            upsert_portfolio_metadata,
        )

        return upsert_portfolio_metadata(
            portfolio_index_asset=self.index_asset,
            description=description,
            updater=updater,
        )

    def add_venue(self, venue_id) -> None:
        url = f"{self.get_object_url()}/{self.id}/add_venue/"
        payload = {"json": {"venue_id": venue_id}}
        r = make_request(
            s=self.build_session(), loaders=self.LOADERS, r_type="PATCH", url=url, payload=payload
        )
        if r.status_code != 200:
            raise RuntimeError(f"PATCH {url} failed: {r.status_code} {r.text}")




    def get_latest_weights(self, timeout=None) -> dict[str, float]:
        url = f"{self.get_object_url()}/{self.id}/get_latest_weights/"
        r = make_request(
            s=self.build_session(), loaders=self.LOADERS, r_type="GET", url=url, time_out=timeout
        )
        if r.status_code != 200:
            raise_for_response(r)
        results = r.json()
        return results["weights"], datetime.datetime.utcfromtimestamp(
            results["weights_date"]
        ).replace(tzinfo=pytz.utc)



class PortfolioGroup(BaseObjectOrm, BasePydanticModel):
    id: int
    unique_identifier: str
    display_name: str
    source: str
    portfolios: list[Union[int, "Portfolio"]]
    description: str | None = None

    def __repr__(self):
        return f"{self.display_name} ({self.unique_identifier}), {len(self.portfolios)} portfolios"

    @classmethod
    def get_or_create(
        cls,
        unique_identifier: str,
        display_name: str,
        portfolio_ids: list[int],
        source: str | None = None,
        description: str | None = None,
        timeout=None,
    ):
        url = f"{cls.get_object_url()}/get_or_create/"
        payload = {
            "json": {
                "display_name": display_name,
                "source": source,
                "unique_identifier": unique_identifier,
                "portfolios": portfolio_ids,
                "description": description,
            }
        }
        r = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="POST",
            url=url,
            payload=payload,
            time_out=timeout,
        )
        if r.status_code not in [201, 200]:
            raise_for_response(r)

        return cls(**r.json())

    def append_portfolios(self, portfolio_ids: list[int]) -> "PortfolioGroup":
        """
        Appends portfolios to the group by calling the custom API action.

        Args:
            portfolio_ids: A list of portfolio primary keys to add to the group.

        Returns:
            The updated PortfolioGroup instance.
        """
        if not self.id:
            raise ValueError("Cannot append portfolios to an unsaved PortfolioGroup.")

        url = f"{self.get_object_url()}/{self.id}/append-portfolios/"
        payload = {"portfolios": portfolio_ids}

        r = make_request(
            s=self.build_session(),
            loaders=self.LOADERS,
            r_type="POST",
            url=url,
            payload={"json": payload},
        )

        if r.status_code != 200:
            raise_for_response(r)

        # Update the current instance in-place with the response from the server
        updated_data = r.json()
        for key, value in updated_data.items():
            setattr(self, key, value)

        return self

    def remove_portfolios(self, portfolio_ids: list[int]) -> "PortfolioGroup":
        """
        Removes portfolios from the group by calling the custom API action.

        Args:
            portfolio_ids: A list of portfolio primary keys to remove from the group.

        Returns:
            The updated PortfolioGroup instance.
        """
        if not self.id:
            raise ValueError("Cannot remove portfolios from an unsaved PortfolioGroup.")

        url = f"{self.get_object_url()}/{self.id}/remove-portfolios/"
        payload = {"portfolios": portfolio_ids}

        r = make_request(
            s=self.build_session(),
            loaders=self.LOADERS,
            r_type="POST",
            url=url,
            payload={"json": payload},
        )

        if r.status_code != 200:
            raise_for_response(r)

        # Update the current instance in-place with the response from the server
        updated_data = r.json()
        for key, value in updated_data.items():
            setattr(self, key, value)

        return self


class VirtualFundHoldingPosition(BasePydanticModel):
    time_index: datetime.datetime
    unique_identifier: str
    quantity: Decimal
    target_weight: Decimal | None = None
    target_trade_time: datetime.datetime | None = None
    extra_details: dict[str, Any] = Field(default_factory=dict)
    asset: AssetMixin | None = None

    @model_validator(mode="before")
    def resolve_asset(cls, values):
        asset = values.get("asset")
        if isinstance(asset, dict):
            values["asset"] = resolve_asset(asset)
        return values


class VirtualFundLatestHoldings(BasePydanticModel):
    holdings_set_uid: str
    is_trade_snapshot: bool = False
    target_trade_time: datetime.datetime | None = None
    holdings: list[VirtualFundHoldingPosition]
    holdings_date: datetime.datetime


class VirtualFundHistoricalHoldings(VirtualFundLatestHoldings):
    snapshot_uid: str
    related_fund_uid: str


class InstrumentsConfiguration(BaseObjectOrm,BasePydanticModel):
    discount_curves_storage_node:int | None
    reference_rates_fixings_storage_node:int | None

class VirtualFund(BaseObjectOrm, BasePydanticModel):
    id: int | None = None
    uid: str
    target_portfolio: Union[int, "Portfolio"]
    target_account: Account
    latest_holdings: VirtualFundLatestHoldings | None = None
    requires_nav_adjustment: bool = Field(default=False)
    execution_mode: str | None = None

    @classmethod
    def _coerce_target_account_uid(cls, value: Any) -> str:
        if isinstance(value, str):
            return value.strip()
        account_uid = getattr(value, "uid", None)
        if account_uid is not None:
            return str(account_uid)
        if isinstance(value, dict) and value.get("uid") is not None:
            return str(value["uid"])
        raise TypeError("target_account must be an Account or account uid.")

    @classmethod
    def _coerce_target_portfolio_id(cls, value: Any) -> int:
        if isinstance(value, int):
            return value
        portfolio_id = getattr(value, "id", None)
        if portfolio_id is not None:
            return int(portfolio_id)
        if isinstance(value, dict) and value.get("id") is not None:
            return int(value["id"])
        raise TypeError("target_portfolio must be a Portfolio or portfolio id.")

    @classmethod
    def _normalize_write_kwargs(cls, kwargs: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(kwargs)
        if "target_account" in normalized:
            normalized["target_account"] = cls._coerce_target_account_uid(
                normalized["target_account"]
            )
        if "target_portfolio" in normalized:
            normalized["target_portfolio"] = cls._coerce_target_portfolio_id(
                normalized["target_portfolio"]
            )
        return normalized

    @classmethod
    def get(cls, pk=None, timeout=None, **filters):
        uid = filters.pop("uid", None)
        if pk is None and uid is not None:
            pk = uid
        return super().get(pk=pk, timeout=timeout, **filters)

    @classmethod
    def create(cls, timeout=None, **kwargs):
        normalized = cls._normalize_write_kwargs(kwargs)
        return super().create(timeout=timeout, **normalized)

    @classmethod
    def patch_by_id(cls, instance_id, *args, _into=None, timeout=None, **kwargs):
        url = f"{cls.get_object_url().rstrip('/')}/{instance_id}/"
        payload = {"json": cls.serialize_for_json(cls._normalize_write_kwargs(kwargs))}
        r = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="PATCH",
            url=url,
            payload=payload,
            time_out=timeout,
        )
        if r.status_code != 200:
            raise_for_response(r)
        updated = cls(**r.json())
        if _into is not None:
            for field_name in type(updated).model_fields:
                setattr(_into, field_name, getattr(updated, field_name))
            return _into
        return updated

    @classmethod
    def destroy_by_id(cls, instance_id, *args, timeout=None, **kwargs):
        r = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="DELETE",
            url=f"{cls.get_object_url().rstrip('/')}/{instance_id}/",
            payload={},
            time_out=timeout,
        )
        if r.status_code != 204:
            raise_for_response(r)

    def patch(self, *args, timeout=None, **kwargs):
        updated = type(self).patch_by_id(self.uid, timeout=timeout, **kwargs)
        for field_name in type(updated).model_fields:
            setattr(self, field_name, getattr(updated, field_name))
        return self

    def delete(self, *args, timeout=None, **kwargs):
        return self.__class__.destroy_by_id(self.uid, *args, timeout=timeout, **kwargs)


class OrderStatus(str, Enum):
    LIVE = "live"
    FILLED = "filled"
    PARTIALLY_FILLED = "partially_filled"
    CANCELED = "canceled"
    NOT_PLACED = "not_placed"


class OrderTimeInForce(str, Enum):
    GOOD_TILL_CANCELED = "gtc"


class OrderSide(IntEnum):
    SELL = -1
    BUY = 1


class OrderType(str, Enum):
    MARKET = "market"
    LIMIT = "limit"
    NOT_PLACED = "not_placed"


class Order(BaseObjectOrm, BasePydanticModel):
    id: int | None = Field(None, primary_key=True)
    order_remote_id: str
    client_order_id: str
    order_type: OrderType
    order_time: datetime.datetime
    expires_time: datetime.datetime | None = None
    order_side: OrderSide  # Use int for choices (-1: SELL, 1: BUY)
    quantity: float
    status: OrderStatus = OrderStatus.NOT_PLACED
    filled_quantity: float | None = 0.0
    filled_price: float | None = None
    order_manager: Union[int, "OrderManager"] = None  # Assuming foreign key ID is used
    asset: int  # Assuming foreign key ID is used
    related_fund: int | None = None  # Assuming foreign key ID is used
    related_account: int  # Assuming foreign key ID is used
    time_in_force: str
    comments: str | None = None

    class Config:
        use_enum_values = True  # This allows using enum values directly

    @classmethod
    def create_or_update(cls, order_time_stamp: float, *args, **kwargs):
        """

        Args:
            order_time: timestamp
            *args:
            **kwargs:

        Returns:

        """
        url = f"{cls.get_object_url()}/create_or_update/"
        kwargs["order_time"] = order_time_stamp
        payload = {"json": kwargs}

        r = make_request(
            s=cls.build_session(), loaders=cls.LOADERS, r_type="POST", url=url, payload=payload
        )

        if r.status_code not in [200, 201]:
            raise_for_response(r)

        return cls(**r.json())


class MarketOrder(Order):
    pass


class LimitOrder(Order):
    limit_price: float


class OrderManagerTargetQuantity(BaseModel):
    asset: int | Asset
    quantity: Decimal


class OrderManager(BaseObjectOrm, BasePydanticModel):
    id: int | None = None
    target_time: datetime.datetime
    target_rebalance: list[OrderManagerTargetQuantity]
    order_received_time: datetime.datetime | None = None
    execution_end: datetime.datetime | None = None
    related_account: Account | int  # Representing the ForeignKey field with the related account ID

    @staticmethod
    def serialize_for_json(kwargs):
        new_data = {}
        for key, value in kwargs.items():
            new_value = copy.deepcopy(value)
            if isinstance(value, datetime.datetime):
                new_value = str(value)
            elif key == "target_rebalance":
                new_value = [json.loads(c.model_dump_json()) for c in value]
            new_data[key] = new_value
        return new_data

    @classmethod
    def destroy_before_date(cls, target_date: datetime.datetime):
        base_url = cls.get_object_url()
        payload = {
            "json": {
                "target_date": target_date.strftime(DATE_FORMAT),
            },
        }

        r = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="POST",
            url=f"{base_url}/destroy_before_date/",
            payload=payload,
        )

        if r.status_code != 204:
            raise_for_response(r)
