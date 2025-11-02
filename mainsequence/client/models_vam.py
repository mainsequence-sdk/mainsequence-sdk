import copy
import datetime
import json
from decimal import Decimal
from enum import Enum, IntEnum
from typing import Any, Optional, TypedDict, Union
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import pandas as pd
import pytz
from pydantic import BaseModel, Field, constr, model_validator, root_validator, validator

from mainsequence.logconf import logger

from .base import (
    TDAG_ENDPOINT,
    BaseObjectOrm,
    BasePydanticModel,
    HtmlSaveException,
)
from .models_tdag import DataNodeUpdate
from .utils import DATE_FORMAT, DoesNotExist, make_request
from .utils import MARKETS_CONSTANTS as CONSTANTS

CRYPTO_EXCHANGE_CODE = [
    "abts",
    "acxi",
    "alcn",
    "bbit",
    "bbox",
    "bbsp",
    "bcex",
    "bequ",
    "bfly",
    "bfnx",
    "bfrx",
    "bgon",
    "binc",
    "bitc",
    "bitz",
    "bjex",
    "bl3p",
    "blc2",
    "blcr",
    "bnbd",
    "bnce",
    "bndx",
    "bnf8",
    "bnus",
    "bopt",
    "bpnd",
    "bt38",
    "btba",
    "btbu",
    "btby",
    "btca",
    "btcb",
    "btcc",
    "bthb",
    "btma",
    "btmx",
    "btrk",
    "btrx",
    "btsh",
    "btso",
    "bull",
    "bxth",
    "bybt",
    "cbse",
    "ccck",
    "ccex",
    "cexi",
    "cflr",
    "cflx",
    "cnex",
    "cngg",
    "cnhd",
    "cnmt",
    "cone",
    "crco",
    "crfl",
    "crtw",
    "crv2",
    "cucy",
    "curv",
    "delt",
    "drbt",
    "dydx",
    "eris",
    "ethx",
    "etrx",
    "exxa",
    "ftxu",
    "ftxx",
    "gacn",
    "gate",
    "gmni",
    "hbdm",
    "hitb",
    "huob",
    "inch",
    "indr",
    "itbi",
    "kcon",
    "korb",
    "krkn",
    "lclb",
    "lgom",
    "lmax",
    "merc",
    "mexc",
    "mtgx",
    "ngcs",
    "nova",
    "nvdx",
    "okcn",
    "okex",
    "oslx",
    "pksp",
    "polo",
    "qsp2",
    "qsp3",
    "quon",
    "sghd",
    "stmp",
    "sush",
]

COMPOSITE_TO_ISO = {
    "AR": "XBUE",
    "AU": "XASX",
    "BZ": "BVMF",
    "CN": "XTSE",
    "CB": "XBOG",
    "CH": "XSHG",
    "CI": "XSGO",
    "CP": "XPRA",
    "DC": "XCSE",
    "FH": "XHEL",
    "FP": "XPAR",
    "GA": "ASEX",
    "GR": "XFRA",
    "HK": "XHKG",
    "IE": "XDUB",
    "IM": "XMIL",
    "IN": "XBOM",
    "IT": "XTAE",
    "JP": "XTKS",
    "KS": "XKRX",
    "KZ": "AIXK",
    "LN": "XLON",
    "MM": "XMEX",
    "MK": "XKLS",
    "NA": "XAMS",
    "PL": "XLIS",
    "PM": "XPHS",
    "PW": "XWAR",
    "RO": "XBSE",
    "SA": "XSAU",
    "SM": "XMAD",
    "SS": "XSTO",
    "SW": "XSWX",
    "TH": "XBKK",
    "TI": "XIST",
    "TT": "XTAI",
    "US": "XNYS",
    "AT": "XWBO",
    "BB": "XBRU",
}


def validator_for_string(value):
    if isinstance(value, str):
        # Parse the string to a datetime object
        try:
            return datetime.datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
        except ValueError as err:
            raise ValueError(
                f"Invalid datetime format: {value!r}. Expected format is 'YYYY-MM-DDTHH:MM:SSZ'."
            ) from err


def get_model_class(model_class: str):
    """
    Reverse look from model class by name
    """
    MODEL_CLASS_MAP = {
        "Asset": Asset,
        "AssetCurrencyPair": AssetCurrencyPair,
        "AssetFutureUSDM": AssetFutureUSDM,
        "PortfolioIndexAsset": PortfolioIndexAsset,
        "Calendar": Calendar,
        "ExecutionVenue": ExecutionVenue,
        "PortfolioGroup": PortfolioGroup,
    }
    return MODEL_CLASS_MAP[model_class]


def create_from_serializer_with_class(asset_list: list[dict]):
    new_list = []
    for a in asset_list:
        AssetClass = get_model_class(a["AssetClass"])
        a.pop("AssetClass")
        new_list.append(AssetClass(**a))
    return new_list


def resolve_asset(asset_dict: dict):
    asset = create_from_serializer_with_class([asset_dict])[0]
    return asset


class Calendar(BaseObjectOrm, BasePydanticModel):
    id: int | None = None
    name: str
    calendar_dates: dict | None = None

    def __str__(self):
        return self.name

    def __repr__(self) -> str:
        return self.name


class Organization(BaseModel):
    id: int
    uid: str
    name: str
    url: str | None  # URL can be None


class Group(BaseModel):
    id: int
    name: str
    permissions: list[Any]  # Adjust the type for permissions as needed


class User(BaseObjectOrm, BasePydanticModel):

    first_name: str
    last_name: str
    is_active: bool
    date_joined: datetime.datetime
    role: str
    username: str
    email: str
    last_login: datetime.datetime
    api_request_limit: int
    mfa_enabled: bool
    organization: Organization
    plan: Any | None  # Use a specific model if plan details are available
    groups: list[Group]
    user_permissions: list[Any]  # Adjust as necessary for permission structure
    phone_number: str | None = None

    @classmethod
    def get_object_url(cls):
        # TODO should be also orm/api
        url = f"{cls.ROOT_URL.replace('orm/api', 'user/api')}/{cls.END_POINTS[cls.class_name()]}"
        return url

    @classmethod
    def get_authenticated_user_details(cls):
        url = f"{cls.get_object_url()}/get_user_details/"
        r = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="GET",
            url=url,
        )
        if r.status_code not in [200, 201]:
            raise Exception(f" {r.text()}")

        return cls(**r.json())


class AssetSnapshot(BaseObjectOrm, BasePydanticModel):
    id: int | None = None
    asset: Union["AssetMixin", int]

    # Validity window
    effective_from: datetime.datetime = Field(
        description="Date at which this snapshot became effective"
    )
    effective_to: datetime.datetime | None = Field(
        None, description="Date at which this snapshot was superseded (null if current)"
    )

    # Mutable fields
    name: constr(max_length=255) = Field(
        ..., description="Security name as recorded in the FIGI database"
    )
    ticker: constr(max_length=50) | None = Field(
        None, description="FIGI ticker field (often shorter symbol used by OpenFIGI)"
    )
    exchange_code: constr(max_length=50) | None = Field(
        None, description="Exchange/market MIC code (e.g. XNYS, XNAS) or composite code"
    )
    asset_ticker_group_id: constr(max_length=12) | None = Field(
        None, description="Highest aggregation level for share class grouping"
    )
    venue_specific_properties: dict[str, Any] | None = Field(
        None, description="Exchange-specific metadata"
    )


def _set_query_param_on_url(url: str, key: str, value) -> str:
    """
    Add or replace a query parameter in a URL without disturbing others (e.g., offset/page).
    Works with absolute or relative URLs.
    """
    parts = urlsplit(url)
    q = dict(parse_qsl(parts.query, keep_blank_values=True))
    q[key] = str(value)
    new_query = urlencode(q, doseq=True)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, parts.fragment))


class AssetPricingDetail(BasePydanticModel):
    instrument_dump: dict
    pricing_details_date: datetime.datetime


class AssetMixin(BaseObjectOrm, BasePydanticModel):
    id: int | None = None

    # Immutable identifiers
    unique_identifier: constr(max_length=255)
    figi: constr(max_length=12) | None = Field(
        None,
        description="FIGI identifier (unique to a specific instrument on a particular market/exchange)",
    )
    composite: constr(max_length=12) | None = Field(
        None,
        description="Composite FIGI identifier (aggregates multiple local listings within one market)",
    )
    share_class: constr(max_length=12) | None = Field(
        None,
        description="Share class designation (e.g. 'Common', 'Class A', 'Preferred') as per FIGI",
    )

    isin: constr(max_length=12) | None = Field(
        None, description="International Securities Identification Number"
    )

    security_type: constr(max_length=50) | None = Field(
        None, description="Instrument type (e.g. 'CS' for common stock, 'PS' for preferred)"
    )
    security_type_2: constr(max_length=50) | None = Field(
        None, description="OpenFIGI Security Type 2"
    )
    security_market_sector: constr(max_length=50) | None = Field(
        None,
        description="High-level sector classification (e.g. 'Equity', 'Corporate Bond') as per FIGI",
    )

    is_custom_by_organization: bool = Field(
        default=False,
        description="Flag indicating if this asset was custom-created by the organization",
    )

    # Snapshot relationship
    current_snapshot: AssetSnapshot | None = Field(
        None, description="Latest active snapshot (effective_to is null)"
    )
    current_pricing_detail: AssetPricingDetail | None = Field(
        None, description="details for instrument pricing"
    )

    def __repr__(self) -> str:
        return f"{self.class_name()}: {self.unique_identifier}"

    @model_validator(mode="after")
    def _inject_main_sequence_asset_id(self) -> "AssetMixin":
        """
        After model construction, if instrument_pricing_detail is present,
        ensure it contains {'main_sequence_asset_id': self.id}.
        """
        ipd = self.current_pricing_detail
        if ipd is not None:
            # Be tolerant: coerce to a dict if necessary.
            try:
                ipd.instrument_dump["instrument"]["main_sequence_asset_id"] = self.id
            except Exception as e:
                self.clear_asset_pricing_details()
                raise e
            self.current_pricing_detail = ipd
        return self

    @property
    def ticker(self):
        return self.current_snapshot.ticker

    @property
    def name(self):
        return self.current_snapshot.name

    @property
    def exchange_code(self):
        return self.current_snapshot.exchange_code

    @property
    def asset_ticker_group_id(self):
        return self.current_snapshot.asset_ticker_group_id

    @classmethod
    def _translate_query_params(cls, query_params: dict[str, Any]):
        translation_map = {
            "ticker": "current_snapshot__ticker",
            "name": "current_snapshot__name",
            "exchange_code": "current_snapshot__exchange_code",
            "asset_ticker_group_id": "current_snapshot__asset_ticker_group_id",
        }

        translated_params = {}
        for key, value in query_params.items():
            # django search uses '__' for nested objects
            full_query = key.split("__")
            asset_query = full_query[0]

            if asset_query in translation_map:
                # Reconstruct the key using the translated base and the original suffix
                translated_base = translation_map[asset_query]
                # Join the translated base with the rest of the query parts
                new_key_parts = [translated_base] + full_query[1:]
                new_key = "__".join(new_key_parts)
                translated_params[new_key] = value
            else:
                # If no translation is needed, use the original key
                translated_params[key] = value

        return translated_params

    @classmethod
    def query(cls, timeout=None, per_page: int = None, **kwargs):
        """
        POST-based filtering for large requests that don't fit in the URL.

        - per_page: desired number of items per page (client-side).


        Follows DRF pagination and accumulates ALL pages. Returns raw dict items.
        """
        base_url = cls.get_object_url()  # e.g. "https://api.example.com/assets"
        body = cls._parse_parameters_filter(kwargs)  # same filters as GET
        accumulated = []

        # Start at the collection action
        next_url = f"{base_url}/query/"

        # Choose which page-size param(s) to set
        # If not specified, we try the common ones in order.
        page_size_params = ["limit", "page_size"]

        only_fields = "fields" in body  # your existing flag

        while next_url:
            # Inject per_page into the URL (NOT the JSON body), preserving offset/page/cursor.
            if per_page:
                for pname in page_size_params:
                    if pname:  # skip None if passed
                        next_url = _set_query_param_on_url(next_url, pname, per_page)

            r = make_request(
                s=cls.build_session(),
                loaders=cls.LOADERS,
                r_type="POST",
                url=next_url,
                payload={"json": body},  # filters stay in body
                time_out=timeout,
            )

            if r.status_code != 200:
                if r.status_code == 401:
                    raise Exception("Unauthorized. Please add credentials to environment.")
                elif r.status_code == 500:
                    raise Exception("Server Error.")
                elif r.status_code == 404:
                    raise DoesNotExist("Not Found.")
                elif r.status_code == 405:
                    raise Exception("Method Not Allowed. Ensure the 'query' endpoint accepts POST.")
                else:
                    raise Exception(f"{r.status_code} - {r.text}")

            data = r.json()
            next_url = data.get("next")  # DRF-provided next URL (may be relative or absolute)

            # Collect results
            for item in data.get("results", []):
                if only_fields:
                    accumulated.append(item)
                else:
                    item["orm_class"] = cls.__name__
                    try:

                        accumulated.append(
                            cls(**item) if issubclass(cls, BasePydanticModel) else item
                        )
                    except Exception as e:
                        print(item)
                        print(cls)
                        print(cls(**item))
                        import traceback

                        traceback.print_exc()
                        raise e

        return accumulated

    @classmethod
    def filter(cls, *args, **kwargs):
        """
        Overrides the default filter to remap 'ticker' and 'name' lookup keys
        to the corresponding fields on the related current_snapshot.
        """
        transformed_kwargs = cls._translate_query_params(kwargs)
        return super().filter(*args, **transformed_kwargs)

    @classmethod
    def get(cls, *args, **kwargs):
        """
        Overrides the default get to remap lookup keys
        to the corresponding fields on the related current_snapshot.
        """
        transformed_kwargs = cls._translate_query_params(kwargs)
        return super().get(*args, **transformed_kwargs)

    def get_calendar(self):
        if self.current_snapshot.exchange_code in COMPOSITE_TO_ISO.keys():
            return Calendar(name=COMPOSITE_TO_ISO[self.current_snapshot.exchange_code])
        elif self.security_type == CONSTANTS.FIGI_SECURITY_TYPE_CRYPTO:
            return Calendar(name="24/7")
        elif self.security_type_2 == CONSTANTS.FIGI_SECURITY_TYPE_2_CRYPTO:
            return Calendar(name="24/7")
        elif self.security_type_2 == CONSTANTS.FIGI_SECURITY_TYPE_2_PERPETUAL:
            return Calendar(name="24/7")
        else:
            return Calendar(name="XNYS")

    def pretty_print(self) -> None:
        """
        Print all asset properties in a neat, aligned table.
        """
        # Gather (field_name, value) pairs
        rows = []
        for field_name in self.__fields__:
            value = getattr(self, field_name)
            rows.append((field_name, value))

        # Compute column widths
        max_name_len = max(len(name) for name, _ in rows)
        max_val_len = max(len(str(val)) for _, val in rows)

        # Header
        header = f"{'Property':<{max_name_len}} | {'Value':<{max_val_len}}"
        separator = "-" * len(header)
        print(header)
        print(separator)

        # Rows
        for name, val in rows:
            print(f"{name:<{max_name_len}} | {val}")

    @classmethod
    def register_asset_from_figi(cls, figi: str, timeout=None):
        base_url = cls.get_object_url() + "/register_asset_from_figi/"
        payload = {"json": {"figi": figi}}
        s = cls.build_session()

        r = make_request(
            s=s, loaders=cls.LOADERS, r_type="POST", url=base_url, payload=payload, time_out=timeout
        )

        if r.status_code not in [200, 201]:
            raise Exception(r.text)

        return cls(**r.json())

    @classmethod
    def filter_with_asset_class(
        cls, timeout=None, include_relationship_details_depth=None, *args, **kwargs
    ):
        """
        Filters assets and returns instances with their correct asset class,
        """

        from .models_helpers import create_from_serializer_with_class

        base_url = cls.get_object_url()
        # Convert `kwargs` to query parameters
        # kwargs["include_relationship_details_depth"]=include_details
        transformed_kwargs = cls._translate_query_params(kwargs)
        params = cls._parse_parameters_filter(parameters=transformed_kwargs)

        # We'll call the custom action endpoint
        url = f"{base_url}/list_with_asset_class/"
        all_results = []

        # Build a single requests session
        s = cls.build_session()

        while url:
            # Make the request to the current page URL
            request_kwargs = {"params": params} if params else {}
            r = make_request(
                s=s,
                loaders=cls.LOADERS,
                r_type="GET",
                url=url,
                payload=request_kwargs,
                time_out=timeout,
            )

            if r.status_code != 200:
                raise Exception(f"Error getting assets (status code: {r.status_code})")

            data = r.json()

            # Check if it's a DRF paginated response by looking for "results"
            if isinstance(data, dict) and "results" in data:
                # Paginated response
                results = data["results"]
                next_url = data["next"]
            else:
                # Either not paginated or no "results" key
                # It's possible your endpoint returns a plain list or other structure
                # Adjust accordingly if needed
                results = data
                next_url = None

            # Accumulate the results
            all_results.extend(results)

            # Prepare for the next loop iteration
            url = next_url
            # After the first request, DRF's `next` link is a full URL that already includes
            # appropriate query params, so we set `params=None` to avoid conflicts.
            params = None

        # Convert the accumulated raw data into asset instances with correct classes
        return create_from_serializer_with_class(all_results)

    def clear_asset_pricing_details(self, timeout=None):
        base_url = self.get_object_url()  # e.g., https://api.example.com/assets
        url = f"{base_url}/{self.id}/clear-asset-pricing-details/"
        r = make_request(
            s=self.build_session(),
            loaders=self.LOADERS,
            r_type="PATCH",
            url=url,
            time_out=timeout,
        )

        if r.status_code not in (200, 201):
            raise Exception(r.text)

    def add_instrument_pricing_details_from_ms_instrument(
        self, instrument, pricing_details_date: datetime.datetime, timeout=None
    ):

        data = instrument.serialize_for_backend()
        data = json.loads(data)
        data["instrument"]["main_sequence_asset_id"] = self.id
        data["pricing_details_date"] = pricing_details_date.timestamp()

        return self.add_instrument_pricing_details(instrument_pricing_details=data, timeout=timeout)

    def add_instrument_pricing_details(
        self,
        instrument_pricing_details: dict[str, Any],
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """
        POST /assets/{self.id}/set-asset-pricing-detail/

        Sends the pricing details as a RAW JSON object (no wrapper keys).
        The backend action treats the entire body as the pricing dump and
        associates it to (asset, organization_owner).

        Args:
            instrument_pricing_details: JSON object to store.
            timeout: optional request timeout (seconds).

        Returns:
            The server's JSON response (dict).
        """
        if not getattr(self, "id", None):
            raise ValueError("This object has no 'id'; cannot POST to detail action.")
        if not isinstance(instrument_pricing_details, dict):
            raise ValueError("instrument_pricing_details must be a JSON object (dict).")

        base_url = self.get_object_url()  # e.g., https://api.example.com/assets
        url = f"{base_url}/{self.id}/set-asset-pricing-detail/"

        r = make_request(
            s=self.build_session(),
            loaders=self.LOADERS,
            r_type="POST",
            url=url,
            payload={
                "json": instrument_pricing_details
            },  # raw body (no 'dump', no 'organization_id')
            time_out=timeout,
        )

        if r.status_code not in (200, 201):
            if r.status_code == 401:
                raise Exception("Unauthorized. Please add credentials to environment.")
            elif r.status_code == 404:
                raise DoesNotExist("Asset not found.")
            elif r.status_code == 405:
                raise Exception("Method Not Allowed. Ensure the custom action is enabled.")
            elif r.status_code == 413:
                raise Exception("Payload Too Large. Consider compressing or splitting.")
            elif r.status_code >= 500:
                raise Exception("Server Error.")
            else:
                raise Exception(f"{r.status_code} - {r.text}")

        data = r.json()

        data.get("instrument_pricing_detail")
        when = data["pricing_details_date"]
        self.current_pricing_detail = AssetPricingDetail(
            instrument_dump=data["instrument_dump"],
            pricing_details_date=datetime.datetime.utcfromtimestamp(when).replace(tzinfo=pytz.utc),
        )


class AssetCategory(BaseObjectOrm, BasePydanticModel):
    id: int
    unique_identifier: str
    display_name: str
    assets: list[Union[int, "Asset"]]
    description: str | None = None

    def __repr__(self):
        return f"{self.display_name} source: {self.source}, {len(self.assets)} assets"

    def get_assets(self):
        if not self.assets:
            raise ValueError(f"No assets in Asset Category {self.display_name}")
        return Asset.filter(id__in=self.assets)

    def update_assets(self, asset_ids: list[int]):
        self.remove_assets(self.assets)
        self.append_assets(asset_ids)

    def append_assets(
        self, asset_ids: list[int] | None = None, assets: AssetMixin | None = None
    ) -> "AssetCategory":
        """
        Append the given asset IDs to this category.
        Expects a payload: {"assets": [<asset_id1>, <asset_id2>, ...]}
        """
        assert asset_ids is not None or assets is not None, "asset_ids or assets must be provided"

        url = f"{self.get_object_url()}/{self.id}/append-assets/"
        if assets is not None:
            asset_ids = [a.id for a in assets]
        payload = {"assets": asset_ids}
        r = make_request(
            s=self.build_session(),
            loaders=self.LOADERS,
            r_type="POST",
            url=url,
            payload={"json": payload},
        )
        if r.status_code not in [200, 201]:
            raise Exception(f"Error appending assets: {r.text()}")
        # Return a new instance of AssetCategory built from the response JSON.
        cat = AssetCategory(**r.json())
        self.assets = cat.assets

    def remove_assets(self, asset_ids: list[int]) -> "AssetCategory":
        """
        Remove the given asset IDs from this category.
        Expects a payload: {"assets": [<asset_id1>, <asset_id2>, ...]}
        """
        url = f"{self.get_object_url()}/{self.id}/remove-assets/"
        payload = {"assets": asset_ids}
        r = make_request(
            s=self.build_session(),
            loaders=self.LOADERS,
            r_type="POST",
            url=url,
            payload={"json": payload},
        )
        if r.status_code not in [200, 201]:
            raise Exception(f"Error removing assets: {r.text()}")
        # Return a new instance of AssetCategory built from the response JSON.
        return AssetCategory(**r.json())

    @classmethod
    def get_or_create(cls, *args, **kwargs):
        url = f"{cls.get_object_url()}/get-or-create/"
        payload = {"json": kwargs}
        r = make_request(
            s=cls.build_session(), loaders=cls.LOADERS, r_type="POST", url=url, payload=payload
        )
        if r.status_code not in [200, 201]:
            raise Exception(f"Error appending creating: {r.text}")
        # Return a new instance of AssetCategory built from the response JSON.
        return AssetCategory(**r.json())


class TranslationError(RuntimeError):
    """Raised when no translation rule (or more than one) matches an asset."""


class AssetFilter(BaseModel):
    security_type: str | None = None
    security_market_sector: str | None = None

    def filter_triggered(self, asset: "Asset") -> bool:
        if self.security_type and asset.security_type != self.security_type:
            return False
        if (
            self.security_market_sector
            and asset.security_market_sector != self.security_market_sector
        ):
            return False
        return True


class AssetTranslationRule(BaseModel):
    asset_filter: AssetFilter
    markets_time_serie_unique_identifier: str
    target_exchange_code: str | None = None

    def is_asset_in_rule(self, asset: "Asset") -> bool:
        return self.asset_filter.filter_triggered(asset)


class AssetTranslationTable(BaseObjectOrm, BasePydanticModel):
    """
    Mirrors the Django model 'AssetTranslationTableModel' in the backend.
    """

    id: int = None
    unique_identifier: str
    rules: list[AssetTranslationRule] = Field(default_factory=list)

    @classmethod
    def get_or_create(
        cls,
        translation_table_identifier,
        rules,
    ):
        translation_table = cls.get_or_none(unique_identifier=translation_table_identifier)
        rules_serialized = [r.model_dump() for r in rules]

        if translation_table is None:
            translation_table = AssetTranslationTable.create(
                unique_identifier=translation_table_identifier,
                rules=rules_serialized,
            )
        else:
            translation_table.add_rules(rules)

    def evaluate_asset(self, asset):
        for rule in self.rules:
            if rule.is_asset_in_rule(asset):
                return {
                    "markets_time_serie_unique_identifier": rule.markets_time_serie_unique_identifier,
                    "exchange_code": rule.target_exchange_code,
                }

        raise TranslationError(f"No rules for asset {asset} found")

    def add_rules(self, rules: list[AssetTranslationRule], open_for_everyone=False) -> None:
        """
        Add each rule to the translation table by calling the backend's 'add_rule' endpoint.
        Prevents local duplication. If the server also rejects a duplicate,
        it returns an error which we silently ignore.
        """
        base_url = self.get_object_url()
        for new_rule in rules:
            # 1) Check for local duplicates
            if any(
                r.asset_filter == new_rule.asset_filter
                and r.markets_time_serie_unique_identifier
                == new_rule.markets_time_serie_unique_identifier
                and r.target_exchange_code == new_rule.target_exchange_code
                for r in self.rules
            ):
                # Already in local table, skip adding
                logger.debug(f"Rule {new_rule} already present - skipping")
                continue

            # 2) Post to backend's "add_rule"
            url = f"{base_url}/{self.id}/add_rule/"
            payload = new_rule.model_dump()
            if open_for_everyone:
                payload["open_for_everyone"] = True
                payload["asset_filter"]["open_for_everyone"] = True

            r = make_request(
                s=self.build_session(),
                loaders=self.LOADERS,
                r_type="POST",
                url=url,
                payload={"json": payload},
            )

            if r.status_code == 201:
                # Successfully created on server. Append locally
                self.rules.append(new_rule)
            elif r.status_code not in (200, 201):
                raise Exception(f"Error adding rule: {r.text}")

    def remove_rules(self, rules: list[AssetTranslationRule]) -> None:
        """
        Remove each rule from the translation table by calling the backend's 'remove_rule' endpoint.
        Once successfully removed on the server, remove it from the local list `self.rules`.
        If a rule is not found on the server, we skip silently.
        """
        base_url = self.get_object_url()
        for rule_to_remove in rules:
            # 1) Check if we even have it locally
            matching_local = [
                r
                for r in self.rules
                if r.asset_filter == rule_to_remove.asset_filter
                and r.markets_time_serie_unique_identifier
                == rule_to_remove.markets_time_serie_unique_identifier
                and r.target_exchange_code == rule_to_remove.target_exchange_code
            ]
            if not matching_local:
                # Not in local rules, skip
                continue

            # 2) Post to backend's "remove_rule"
            url = f"{base_url}/{self.id}/remove_rule/"
            payload = rule_to_remove.model_dump()
            r = make_request(
                s=self.build_session(),
                loaders=self.LOADERS,
                r_type="POST",
                url=url,
                payload={"json": payload},
            )

            if r.status_code == 200:
                # Successfully removed from server => remove from local
                for matched in matching_local:
                    self.rules.remove(matched)
            elif r.status_code not in (200, 204):
                raise Exception(f"Error removing rule: {r.text()}")


class Asset(AssetMixin, BaseObjectOrm):

    def get_spot_reference_asset_unique_identifier(self):
        return self.unique_identifier

    @classmethod
    def create_or_update_index_asset_from_portfolios(
        cls, reference_portfolio: int, timeout=None
    ) -> "PortfolioIndexAsset":
        url = f"{cls.get_object_url()}/create_or_update_index_asset_from_portfolios/"
        payload = {
            "json": dict(
                reference_portfolio=reference_portfolio,
            )
        }
        r = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="POST",
            url=url,
            payload=payload,
            time_out=timeout,
        )
        if r.status_code not in [200, 201]:
            raise Exception(f"{r.text}")

        return PortfolioIndexAsset(**r.json())

    @classmethod
    def get_or_register_from_isin(
        cls,
        isin: str,
        exchange_code: str,
        timeout=None,
    ) -> "Asset":

        base_url = cls.get_object_url() + "/get_or_register_from_isin/"
        payload = {"json": {"isin": isin, "exchange_code": exchange_code}}
        s = cls.build_session()

        r = make_request(
            s=s, loaders=cls.LOADERS, r_type="POST", url=base_url, payload=payload, time_out=timeout
        )
        if r.status_code not in (200, 201):
            raise Exception(f"Error registering asset: {r.text}")
        return cls(**r.json())

    @classmethod
    def get_or_register_custom_asset(
        cls,
        timeout=None,
        **kwargs,
    ):
        base_url = cls.get_object_url() + "/get_or_register_custom_asset/"
        payload = {"json": kwargs}
        s = cls.build_session()

        r = make_request(
            s=s, loaders=cls.LOADERS, r_type="POST", url=base_url, payload=payload, time_out=timeout
        )
        if r.status_code not in (200, 201):
            raise Exception(f"Error registering asset: {r.text}")
        return cls(**r.json())

    @classmethod
    def batch_get_or_register_custom_assets(
        cls, assets_data: list[dict], timeout=None
    ) -> list["Asset"]:
        """
        Calls the batch endpoint to get or register multiple custom assets.

        Args:
            assets_data: A list of dictionaries, where each dictionary
                         represents the data for one asset.
            timeout: Optional request timeout in seconds.

        Returns:
            A list of Asset objects.
        """
        base_url = cls.get_object_url() + "/batch_get_or_register_custom_assets/"
        payload = {"json": assets_data}
        s = cls.build_session()

        r = make_request(
            s=s, loaders=cls.LOADERS, r_type="POST", url=base_url, payload=payload, time_out=timeout
        )

        if r.status_code != 200:
            raise Exception(f"Error in batch asset registration: {r.text}")

        return [cls(**data) for data in r.json()]


class PortfolioIndexAsset(Asset):
    reference_portfolio: Union["Portfolio", int]

    @property
    def reference_portfolio_details_url(self):
        return f"{TDAG_ENDPOINT}/dashboards/portfolio-detail/?target_portfolio_id={self.reference_portfolios.id}"


class AssetCurrencyPair(AssetMixin, BasePydanticModel):
    base_asset: AssetMixin | int
    quote_asset: AssetMixin | int

    def get_spot_reference_asset_unique_identifier(self):
        return self.base_asset.unique_identifier

    def get_ms_share_class(self):
        return self.base_asset.get_ms_share_class()


class FutureUSDMMixin(AssetMixin, BasePydanticModel):
    maturity_code: str = Field(..., max_length=50)
    last_trade_time: datetime.datetime | None = None
    currency_pair: AssetCurrencyPair

    def get_spot_reference_asset_unique_identifier(self):

        base_asset_symbol = self.currency_pair.base_asset.unique_identifier
        if self.execution_venue_symbol == CONSTANTS.BINANCE_FUTURES_EV_SYMBOL:
            # replace() will do nothing if “1000SHIB” isn’t present
            return base_asset_symbol.replace("1000SHIB", "SHIB")
        return base_asset_symbol


class AssetFutureUSDM(FutureUSDMMixin, BaseObjectOrm):
    pass


class AccountPortfolioScheduledRebalance(BaseObjectOrm, BasePydanticModel):
    id: int
    target_account_portfolio: dict | None = None
    scheduled_time: str = None
    received_in_execution_engine: bool = False
    executed: bool = False
    execution_start: str | None = None
    execution_end: datetime.datetime | None = None
    execution_message: str | None = None


class AccountExecutionConfiguration(BasePydanticModel):
    related_account: int  # Assuming related_account is represented by its ID
    rebalance_tolerance_percent: float = Field(0.02, ge=0)
    minimum_notional_for_a_rebalance: float = Field(15.00, ge=0)
    max_latency_in_cdc_seconds: float = Field(60.00, ge=0)
    force_market_order_on_execution_remaining_balances: bool = Field(False)
    orders_execution_configuration: dict[str, Any]
    cooldown_configuration: dict[str, Any]


class AccountPortfolioPosition(BasePydanticModel):
    id: int | None
    parent_positions: int | None
    target_asset: int
    weight_notional_exposure: float | None = 0.0
    constant_notional_exposure: float | None = 0.0
    single_asset_quantity: float | None = 0.0


class AccountPortfolioHistoricalPositions(BaseObjectOrm, BasePydanticModel):
    id: int | None
    positions_date: datetime.datetime
    comments: str | None
    positions: list[AccountPortfolioPosition]


class AccountPortfolio(BaseObjectOrm, BasePydanticModel):
    id: int
    related_account: int | None
    latest_positions: AccountPortfolioHistoricalPositions | None = None
    model_portfolio_name: str | None = None
    model_portfolio_description: str | None = None

    @property
    def unique_identifier(self):
        return self.related_account_id


class AccountMixin(BasePydanticModel):
    id: int | None = None
    uuid: str
    execution_venue: Union["ExecutionVenue", int]
    account_is_active: bool
    account_name: str | None = None
    is_paper: bool
    account_target_portfolio: AccountPortfolio | None = (
        None  # can be none on creation without holdings
    )
    latest_holdings: Union["AccountLatestHoldings", None] = None

    def build_rebalance(
        self,
        latest_holdings: "AccountHistoricalHoldings",
        tolerance: float,
        change_cash_asset_to_currency_asset: Asset | None = None,
    ):
        nav = self.get_nav()
        nav, _ = nav["nav"], nav["nav_date"]
        related_expected_asset_exposure_df = latest_holdings.related_expected_asset_exposure_df
        # extract Target Rebalance

        # extract expected holdings
        try:
            implicit_holdings_df = (
                related_expected_asset_exposure_df.groupby("aid")
                .aggregate({"holding": "sum", "price": "last", "expected_holding_in_fund": "sum"})
                .rename(columns={"expected_holding_in_fund": "expected_holding"})
            )
        except Exception as e:
            raise e
        implicit_holdings_df["difference"] = (
            implicit_holdings_df["expected_holding"] - implicit_holdings_df["holding"]
        )
        implicit_holdings_df["relative_w"] = (
            implicit_holdings_df["difference"] * implicit_holdings_df["price"]
        ) / nav
        implicit_holdings_df["tolerance_flag"] = implicit_holdings_df["relative_w"].apply(
            lambda x: 1 if x >= tolerance else 0
        )
        implicit_holdings_df["difference"] = (
            implicit_holdings_df["difference"] * implicit_holdings_df["tolerance_flag"]
        )
        implicit_holdings_df["expected_holding"] = (
            implicit_holdings_df["holding"] + implicit_holdings_df["difference"]
        )

        implicit_holdings = (
            implicit_holdings_df[["expected_holding", "price"]]
            .rename(columns={"expected_holding": "holding"})
            .T.to_dict()
        )

        implicit_holdings_df["reference_notional"] = (
            implicit_holdings_df["price"] * implicit_holdings_df["difference"]
        )
        rebalance = (
            implicit_holdings_df[["difference", "reference_notional", "price"]]
            .rename(columns={"difference": "quantity", "price": "reference_price"})
            .T.to_dict()
        )

        all_assets = implicit_holdings.keys()
        new_rebalance, new_implicit_holdings = {}, {}
        # build_asset_switch
        asset_switch_map = Asset.switch_cash_in_asset_list(
            asset_id_list=[c for c in all_assets if c != change_cash_asset_to_currency_asset.id],
            target_currency_asset_id=int(change_cash_asset_to_currency_asset.id),
        )
        asset_switch_map[str(change_cash_asset_to_currency_asset.id)] = (
            change_cash_asset_to_currency_asset.serialized_config
        )

        for a_id in all_assets:
            try:
                new_a = Asset(**asset_switch_map[str(a_id)])
            except Exception as e:
                raise e
            if rebalance[a_id]["quantity"] != 0.0:
                new_rebalance[new_a.id] = {"rebalance": rebalance[a_id], "asset": new_a}
            try:
                new_implicit_holdings[new_a.id] = implicit_holdings[a_id]
            except Exception as e:
                raise e
        not_rebalanced_by_tolerance = implicit_holdings_df[implicit_holdings_df["difference"] != 0]
        not_rebalanced_by_tolerance = not_rebalanced_by_tolerance[
            not_rebalanced_by_tolerance["tolerance_flag"] == 0
        ]["relative_w"]
        not_rebalanced_by_tolerance = {"tolerance": not_rebalanced_by_tolerance.to_dict()}
        return new_rebalance, new_implicit_holdings, not_rebalanced_by_tolerance

    def get_latest_holdings(self):
        base_url = self.get_object_url()
        url = f"{base_url}/{self.id}/latest_holdings/"
        r = make_request(s=self.build_session(), loaders=self.LOADERS, r_type="GET", url=url)
        if r.status_code != 200:
            raise Exception("Error Syncing funds in account")
        return AccountHistoricalHoldings(**r.json())

    def get_missing_assets_in_exposure(self, asset_list_ids, timeout=None) -> list[Asset]:
        base_url = self.get_object_url()
        url = f"{base_url}/{self.id}/get_missing_assets_in_exposure/"
        payload = {
            "json": {
                "asset_list_ids": asset_list_ids,
            }
        }

        r = make_request(
            s=self.build_session(),
            payload=payload,
            loaders=self.LOADERS,
            r_type="GET",
            url=url,
            time_out=timeout,
        )
        if r.status_code != 200:
            raise Exception(r.text)

        asset_list = []
        for a in r.json():
            asset_list.append(resolve_asset(a))

        return asset_list


class RebalanceTargetPosition(BasePydanticModel):
    target_portfolio_id: int
    weight_notional_exposure: float


class Account(AccountMixin, BaseObjectOrm, BasePydanticModel):

    @classmethod
    def get_or_create(
        cls,
        create_without_holdings=False,
        timeout=None,
        **kwargs,
    ):
        base_url = cls.get_object_url()
        url = f"{base_url}/get-or-create/"
        kwargs["create_without_holdings"] = create_without_holdings
        payload = {"json": kwargs}

        r = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="POST",
            url=url,
            payload=payload,
            time_out=timeout,
        )
        if r.status_code not in [200, 201]:
            raise Exception(f"Error geting or creating account {r.text}")
        return cls(**r.json())

    def set_account_target_portfolio_from_asset_holdings(self, timeout=None):
        base_url = self.get_object_url()
        url = f"{base_url}/{self.id}/set_account_target_portfolio_from_asset_holdings/"
        r = make_request(
            s=self.build_session(), loaders=self.LOADERS, r_type="GET", url=url, time_out=timeout
        )
        if r.status_code != 200:
            raise Exception(
                f"Error set_account_target_portfolio_from_asset_holdings in account {r.text}"
            )

    def snapshot_account(self, timeout=None):

        base_url = self.get_object_url()
        url = f"{base_url}/{self.id}/snapshot_account/"
        r = make_request(
            s=self.build_session(), loaders=self.LOADERS, r_type="GET", url=url, time_out=timeout
        )
        if r.status_code != 200:
            raise Exception(f"Error Getting NAV in account {r.text}")

    def get_tracking_error_details(self, timeout=None):

        base_url = self.get_object_url()
        url = f"{base_url}/{self.id}/get_tracking_error_details/"
        r = make_request(
            s=self.build_session(), loaders=self.LOADERS, r_type="GET", url=url, time_out=timeout
        )
        if r.status_code != 200:
            raise Exception(f"Error Getting NAV in account {r.text}")
        result = r.json()
        return result["fund_summary"], result["account_tracking_error"]

    def rebalance(
        self,
        target_positions: list[RebalanceTargetPosition],
        scheduled_time: datetime.datetime | None = None,
        timeout=None,
    ) -> AccountPortfolioScheduledRebalance:

        parsed_target_positions = {}
        for target_position in target_positions:
            if target_position.target_portfolio_id in parsed_target_positions:
                raise ValueError(
                    f"Duplicate target portfolio id: {target_position.target_portfolio_id} not allowed"
                )

            parsed_target_positions[target_position.target_portfolio_id] = {
                "weight_notional_exposure": target_position.weight_notional_exposure,
            }

        return AccountPortfolioScheduledRebalance.create(
            timeout=timeout,
            target_positions=parsed_target_positions,
            target_account_portfolio=self.id,
            scheduled_time=scheduled_time,
        )

    def get_historical_holdings(
        self,
        start_date: datetime.datetime | None = None,
        end_date: datetime.datetime | None = None,
        timeout=None,
    ) -> pd.DataFrame:
        """
        Retrieves historical holdings data for the account over a specified date range.

        Args:
            start_date_timestamp (datetime, optional): The start datetime (UTC) for filtering holdings.
            end_date_timestamp (datetime, optional): The end datetime (UTC) for filtering holdings.
            timeout (int, optional): Optional timeout parameter for the query (currently unused).

        Returns:
            pd.DataFrame: A DataFrame indexed by a multi-index of `time_index` (UTC datetime) and `asset_id` (int),
            containing the following columns:

            - **missing_price (bool)**: Indicates whether the price for the asset was missing on that date.
            - **price (float)**: The recorded price of the asset at the time of the holding.
            - **quantity (float)**: The quantity of the asset held at that time.

            If no holdings are found within the specified range, an empty DataFrame is returned.

        Example Output:
                                                        missing_price  price   quantity
        time_index                     asset_id
        2025-06-23 17:59:57+00:00      62376               False        1.0   1000000.0
        2025-05-30 09:43:19+00:00      62376               False        1.0   1000000.0
        2025-05-30 09:43:26+00:00      62376               False        1.0   1000000.0
        """

        filter_search = dict(related_account__id=self.id)
        if start_date is not None:
            if isinstance(start_date, datetime.datetime):
                if start_date.tzinfo is None:
                    start_date_timestamp = start_date.replace(tzinfo=pytz.utc)
                filter_search["holdings_date__gte"] = start_date_timestamp.isoformat()

        if end_date is not None:
            if isinstance(end_date, datetime.datetime):
                if end_date.tzinfo is None:
                    end_date = end_date.replace(tzinfo=pytz.utc)
            filter_search["holdings_date__lte"] = end_date.isoformat()

        holdings = AccountHistoricalHoldings.filter(**filter_search)
        if len(holdings) == 0:
            return pd.DataFrame()
        positions_df = []
        for holding in holdings:
            holding_date = holding.holdings_date
            for position in holding.holdings:
                pos = position.model_dump()
                pos.pop("orm_class", None)
                pos.pop("parents_holdings", None)
                pos.pop("id", None)
                pos.pop("extra_details", None)
                pos["time_index"] = holding_date

                positions_df.append(pos)

        positions_df = (
            pd.DataFrame(positions_df)
            .rename(columns={"asset": "asset_id"})
            .set_index(["time_index", "asset_id"])
        )
        return positions_df


class AccountPositionDetail(BaseObjectOrm, BasePydanticModel):
    id: int | None = None
    asset: Asset | int = None
    missing_price: bool = False
    price: float
    quantity: float
    parents_holdings: int | None = None
    extra_details: dict | None = None


class AccountHistoricalHoldingsMixin:
    id: int | None = Field(None, primary_key=True)
    holdings_date: datetime.datetime
    comments: str | None = Field(None, max_length=150)
    nav: float | None = None

    is_trade_snapshot: bool = Field(default=False)
    target_trade_time: datetime.datetime | None = None
    related_expected_asset_exposure_df: list[dict[str, Any]] | None = None

    holdings: list[AccountPositionDetail]

    def get_nav(self):
        base_url = self.get_object_url()
        url = f"{base_url}/{self.id}/get_nav/"
        r = make_request(s=self.build_session(), loaders=self.LOADERS, r_type="GET", url=url)
        if r.status_code != 200:
            raise Exception(f"Error Getting NAV in account {r.text}")
        return r.json()


class AccountLatestHoldings(AccountHistoricalHoldingsMixin, BaseObjectOrm, BasePydanticModel):
    """
    Same as Account HistoricalHoldings but Does not include related account

    """

    ...


class AccountHistoricalHoldings(AccountHistoricalHoldingsMixin, BaseObjectOrm, BasePydanticModel):

    related_account: Union[int, "Account"]

    @classmethod
    def destroy_holdings_before_date(
        cls, target_date: datetime.datetime, keep_trade_snapshots: bool
    ):
        base_url = cls.get_object_url()
        payload = {
            "json": {
                "target_date": target_date.strftime(DATE_FORMAT),
                "keep_trade_snapshots": keep_trade_snapshots,
            }
        }

        r = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="POST",
            url=f"{base_url}/destroy_holdings_before_date/",
            payload=payload,
        )
        if r.status_code != 204:
            raise Exception(r.text)

    @classmethod
    def create_with_holdings(
        cls,
        position_list: list[AccountPositionDetail],
        holdings_date: int,
        related_account: int,
        extra_details: dict = None,
        timeout=None,
    ):

        base_url = cls.get_object_url()
        payload = {
            "json": {
                "position_list": [
                    {
                        k: v
                        for k, v in p.model_dump().items()
                        if k not in ["orm_class", "id", "parents_holdings"]
                    }
                    for p in position_list
                ],
                "holdings_date": holdings_date,
                "related_account": related_account,
            }
        }

        r = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="POST",
            url=f"{base_url}/create_with_holdings/",
            payload=payload,
            time_out=timeout,
        )
        if r.status_code != 201:
            raise Exception(r.text)
        return cls(**r.json())


class AccountRiskFactors(BaseObjectOrm, BasePydanticModel):
    related_holdings: int | AccountHistoricalHoldings
    account_balance: float


class FundingFeeTransaction(BaseObjectOrm):
    pass


class AccountPortfolioHistoricalWeights(BaseObjectOrm):
    pass


class WeightPosition(BaseObjectOrm, BasePydanticModel):
    # id: Optional[int] = None
    # parent_weights: int
    asset: AssetMixin | int
    weight_notional_exposure: float

    @property
    def asset_id(self):
        return self.asset if isinstance(self.asset, int) else self.asset.id

    @root_validator(pre=True)
    def resolve_assets(cls, values):
        # Check if 'asset' is a dict and determine its type
        if isinstance(values.get("asset"), dict):
            asset = values.get("asset")
            asset = resolve_asset(asset_dict=asset)
            values["asset"] = asset

        return values


class ExecutionVenue(BaseObjectOrm, BasePydanticModel):
    id: int | None = None
    symbol: str
    name: str

    @property
    def unique_identifier(self):
        return f"{self.symbol}"


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
            raise Exception(f" {r.text()}")
        return cls(**r.json())


class OrdersExecutionConfiguration(BaseModel):
    broker_class: str
    broker_configuration: dict


class PortfolioTags(BasePydanticModel):
    id: int | None = None
    name: str
    color: str





class PortfolioAbout(TypedDict):
    description: str
    signal_name: str
    signal_description: str
    rebalance_strategy_name: str



class Portfolio(BaseObjectOrm, BasePydanticModel):
    id: int | None = None
    data_node_update: Optional["DataNodeUpdate"]
    signal_data_node_update: Optional["DataNodeUpdate"]
    backtest_table_price_column_name: str | None = Field(None, max_length=20)
    tags: list["PortfolioTags"] | None = None
    calendar: Optional["Calendar"]
    index_asset: PortfolioIndexAsset

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
        signal_data_node_update_id: int,
        calendar_name: str,
        target_portfolio_about: PortfolioAbout,
        backtest_table_price_column_name: str,
        tags: list | None = None,
        timeout=None,
    ) -> "Portfolio":
        url = f"{cls.get_object_url()}/create_from_time_series/"
        # Build the payload with the required arguments.
        payload_data = {
            "portfolio_name": portfolio_name,
            "data_node_update_id": data_node_update_id,
            "signal_data_node_update_id": signal_data_node_update_id,
            # Using the same ID for local_signal_time_serie_id as specified.
            "calendar_name": calendar_name,
            "target_portfolio_about": target_portfolio_about,
            "backtest_table_price_column_name": backtest_table_price_column_name,
            "tags": tags,
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
            raise Exception(f" {r.text}")
        response = r.json()

        return cls(**response["portfolio"]), PortfolioIndexAsset(
            **response["portfolio_index_asset"]
        )

    @property
    def portfolio_name(self) -> str:
        return self.index_asset.current_snapshot.name

    @property
    def portfolio_ticker(self) -> str:
        return self.index_asset.current_snapshot.ticker

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
        if r.status_code  !=200:
            raise Exception(f" {r.text}")
        results = r.json()
        return results["weights"], datetime.datetime.utcfromtimestamp(
            results["weights_date"]
        ).replace(tzinfo=pytz.utc)

    def get_historical_weights(
        self, start_date_timestamp: float, end_date_timestamp: float, timeout=None
    ) -> dict[str, float]:
        if self.data_node_update is None:
            print("this portfolio does not have a weights table")

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
            raise Exception(f" {r.text}")
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
            raise Exception(f"Error appending portfolios: {r.text}")

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
            raise Exception(f"Error removing portfolios: {r.text}")

        # Update the current instance in-place with the response from the server
        updated_data = r.json()
        for key, value in updated_data.items():
            setattr(self, key, value)

        return self


class VirtualFundPositionDetail(BaseObjectOrm, BasePydanticModel):
    id: int | None = None
    asset: Asset | AssetFutureUSDM | int
    price: float
    quantity: float
    parents_holdings: Union[int, "VirtualFundHistoricalHoldings"]

    @property
    def asset_id(self):
        return self.asset if isinstance(self.asset, int) else self.asset.id

    @root_validator(pre=True)
    def resolve_assets(cls, values):
        # Check if 'asset' is a dict and determine its type
        if isinstance(values.get("asset"), dict):
            asset = values.get("asset")
            asset = resolve_asset(asset_dict=asset)
            values["asset"] = asset

        return values


class VirtualFundHistoricalHoldings(BaseObjectOrm, BasePydanticModel):
    related_fund: Union["VirtualFund", int]  # assuming VirtualFund is another Pydantic model
    target_trade_time: datetime.datetime | None = None
    target_weights: dict | None = Field(default=None)
    is_trade_snapshot: bool = Field(default=False)
    fund_account_target_exposure: float = Field(default=0)
    fund_account_units_exposure: float | None = Field(default=None)
    holdings: list[VirtualFundPositionDetail]


class ExecutionQuantity(BaseModel):
    asset: Asset | AssetFutureUSDM | int
    quantity: float
    reference_price: None | float

    def __repr__(self):
        return f"{self.__class__.__name__}(asset={self.asset}, quantity={self.quantity})"

    @root_validator(pre=True)
    def resolve_assets(cls, values):
        # Check if 'asset' is a dict and determine its type
        if isinstance(values.get("asset"), dict):
            asset = values.get("asset")
            asset = resolve_asset(asset_dict=asset)
            values["asset"] = asset

        return values


class TargetRebalance(BaseModel):
    # target_execution_positions: ExecutionPositions
    execution_target: list[ExecutionQuantity]

    @property
    def rebalance_asset_map(self):
        return {e.asset.id: e.asset for e in self.execution_target}

class InstrumentsConfiguration(BaseObjectOrm,BasePydanticModel):
    discount_curves_storage_node:Optional[int]
    reference_rates_fixings_storage_node:Optional[int]

class VirtualFund(BaseObjectOrm, BasePydanticModel):
    id: float | None = None
    target_portfolio: Union[int, "Portfolio"]
    target_account: AccountMixin
    notional_exposure_in_account: float
    latest_holdings: "VirtualFundHistoricalHoldings" = None
    latest_rebalance: datetime.datetime | None = None
    fund_nav: float = Field(default=0)
    fund_nav_date: datetime.datetime | None = None
    requires_nav_adjustment: bool = Field(default=False)
    target_portfolio_weight_in_account: float | None = None
    last_trade_time: datetime.datetime | None = None

    # def sanitize_target_weights_for_execution_venue(self,target_weights:dict):
    #     """
    #     This functions switches assets from main net to test net to guarante consistency in the recording
    #     of trades and orders
    #     Args:
    #         target_weights:{asset_id:WeightExecutionPosition}
    #
    #     Returns:
    #
    #     """
    #     if self.target_account.execution_venue.symbol == CONSTANTS.BINANCE_TESTNET_FUTURES_EV_SYMBOL:
    #         target_ev=CONSTANTS.BINANCE_TESTNET_FUTURES_EV_SYMBOL
    #         new_target_weights={}
    #         for _, position in target_weights.items():
    #             AssetClass = position.asset.__class__
    #             asset,_ = AssetClass.filter(symbol=position.asset.unique_symbol, execution_venue__symbol=target_ev,
    #                                     asset_type=position.asset.asset_type,
    #                                     )
    #             asset = asset[0]
    #             new_position = copy.deepcopy(position)
    #             new_position.asset=asset
    #             new_target_weights[asset.id] = new_position
    #             # todo create in DB an execution position
    #     else:
    #         new_target_weights = target_weights
    #
    #     return new_target_weights

    # def build_rebalance_from_target_weights(
    #         self,
    #         target_execution_postitions: ExecutionPositions,
    #         positions_prices: dict(),
    #         absolute_rebalance_weight_limit=.02
    # ) -> TargetRebalance:
    #     actual_positions = {}
    #     target_weights = {p.asset_id: p for p in target_execution_postitions.positions}
    #     #substitute target weights in case of testnets
    #     target_weights = self.sanitize_target_weights_for_execution_venue(target_weights)
    #
    #     positions_to_rebalance = []
    #     if self.latest_holdings is not None:
    #         actual_positions = {p.asset_id : p for p in self.latest_holdings.holdings}
    #
    #         # positions to unwind first
    #         positions_to_unwind=[]
    #         for position in self.latest_holdings.holdings:
    #             if position.quantity == 0.0:
    #                 continue
    #             if position.asset_id not in target_weights.keys():
    #                 positions_to_unwind.append(
    #                     ExecutionQuantity(
    #                         asset=position.asset,
    #                         reference_price=None,
    #                         quantity=-position.quantity
    #                     )
    #                 )
    #
    #         positions_to_rebalance.extend(positions_to_unwind)
    #
    #     for target_position in target_execution_postitions.positions:
    #         price = positions_prices[target_position.asset_id]
    #
    #         current_weight, current_position = 0, 0
    #         if target_position.asset_id in actual_positions.keys():
    #             current_weight = actual_positions[target_position.asset_id].quantity * price / self.notional_exposure_in_account
    #             current_position = actual_positions[target_position.asset_id].quantity
    #         target_weight = target_position.weight_notional_exposure
    #         if abs(target_weight - current_weight) <= absolute_rebalance_weight_limit:
    #             continue
    #         target_quantity = self.notional_exposure_in_account * target_position.weight_notional_exposure / price
    #         rebalance_quantity = target_quantity - current_position
    #         positions_to_rebalance.append(ExecutionQuantity(asset=target_position.asset,
    #                                                         quantity=rebalance_quantity,
    #                                                         reference_price=price
    #                                                         ))
    #
    #     target_rebalance = TargetRebalance(target_execution_positions=target_execution_postitions,
    #                                        execution_target=positions_to_rebalance
    #                                        )
    #     return target_rebalance

    @validator("last_trade_time", pre=True, always=True)
    def parse_last_trade_time(cls, value):
        value = validator_for_string(value)
        return value

    @validator("fund_nav_date", pre=True, always=True)
    def parse_fund_nav_date(cls, value):
        value = validator_for_string(value)
        return value

    @validator("latest_rebalance", pre=True, always=True)
    def parse_latest_rebalance(cls, value):
        value = validator_for_string(value)
        return value

    def get_account(self):
        a, r = Account.get(id=self.target_account)
        return a

    def get_latest_trade_snapshot_holdings(self):
        url = f"{self.get_object_url()}/{int(self.id)}/get_latest_trade_snapshot_holdings/"
        r = make_request(s=self.build_session(), loaders=self.LOADERS, r_type="GET", url=url)

        if r.status_code != 200:
            raise HtmlSaveException(r.text)
        if len(r.json()) == 0:
            return None
        return VirtualFundHistoricalHoldings(**r.json())


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
            raise r.text
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
            raise Exception(r.text)


# ------------------------------
# ALPACA
# ------------------------------


class AlpacaAccountRiskFactors(AccountRiskFactors):
    total_initial_margin: float
    total_maintenance_margin: float
    last_equity: float
    buying_power: float
    cash: float
    last_maintenance_margin: float
    long_market_value: float
    non_marginable_buying_power: float
    options_buying_power: float
    portfolio_value: float
    regt_buying_power: float
    sma: float


class AlpacaAccount(
    AccountMixin,
):
    api_key: str
    secret_key: str

    account_number: str
    id_hex: str
    account_blocked: bool
    multiplier: float
    options_approved_level: int
    options_trading_level: int
    pattern_day_trader: bool
    trade_suspended_by_user: bool
    trading_blocked: bool
    transfers_blocked: bool
    shorting_enabled: bool


# ------------------------------
# BINANCE
# ------------------------------


class BinanceFuturesAccountRiskFactors(AccountRiskFactors):
    total_initial_margin: float
    total_maintenance_margin: float
    total_margin_balance: float
    total_unrealized_profit: float
    total_cross_wallet_balance: float
    total_cross_unrealized_pnl: float
    available_balance: float
    max_withdraw_amount: float


class BaseFuturesAccount(Account):
    api_key: str
    secret_key: str

    multi_assets_margin: bool = False
    fee_burn: bool = False
    can_deposit: bool = False
    can_withdraw: bool = False


