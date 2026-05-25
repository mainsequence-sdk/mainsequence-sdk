import datetime
import json
from typing import TYPE_CHECKING, Any, ClassVar, Union

import pytz
from pydantic import (
    Field,
    constr,
    model_validator,
)

from mainsequence.client.base import BaseObjectOrm, BasePydanticModel
from mainsequence.client.exceptions import raise_for_response
from mainsequence.client.utils import (
    MAINSEQUENCE_ENDPOINT,
    DoesNotExist,
    make_request,
)
from mainsequence.client.utils import MARKETS_CONSTANTS as CONSTANTS
from mainsequence.logconf import logger
from mainsequence.markets.client.models.core import (
    COMPOSITE_TO_ISO,
    Calendar,
    _set_query_param_on_url,
)

if TYPE_CHECKING:
    from mainsequence.markets.client.models.accounts_and_portfolios import Portfolio


def get_model_class(model_class: str):
    """
    Reverse look from model class by name
    """
    from mainsequence.markets.client.models.accounts_and_portfolios import PortfolioGroup

    MODEL_CLASS_MAP = {
        "Asset": Asset,
        "PortfolioIndexAsset": PortfolioIndexAsset,
        "Calendar": Calendar,
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


class AssetPricingDetail(BasePydanticModel):
    instrument_dump: dict
    pricing_details_date: datetime.datetime


class AssetMixin(BaseObjectOrm, BasePydanticModel):
    FILTERSET_FIELDS: ClassVar[dict[str, list[str]]] = {
        "is_custom_by_organization": ["exact"],
        "id": ["in", "exact"],
        "unique_identifier": ["in", "exact", "contains"],
        "current_snapshot__ticker": ["in", "exact"],
        "current_snapshot__name": ["in", "exact", "contains"],
        "current_snapshot__exchange_code": ["in", "exact"],
        "current_snapshot__asset_ticker_group_id": ["in", "exact"],
        "figi": ["in", "exact"],
        "security_type": ["in", "exact"],
        "security_type_2": ["in", "exact"],
        "security_market_sector": ["in", "exact"],
        "isin": ["in", "exact"],
    }
    FILTER_VALUE_NORMALIZERS: ClassVar[dict[str, str]] = {
        "is_custom_by_organization": "bool",
        "id": "id",
    }

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

    is_tradable: bool = Field(
        default=True, description="Flag indicating if this asset is tradable "
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
        transformed_kwargs = cls._translate_query_params(kwargs)
        transformed_kwargs = cls._normalize_filter_kwargs(transformed_kwargs)
        body = cls._parse_parameters_filter(transformed_kwargs)  # same filters as GET
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
                raise_for_response(r)

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
                        logger.debug(item)
                        logger.debug(cls)
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
        transformed_kwargs = cls._normalize_filter_kwargs(transformed_kwargs)
        return super().get(*args, **transformed_kwargs)

    @property
    def ms_instrument(self):
        if hasattr(self, "_ms_instrument"):
            return self._ms_instrument
        self.set_ms_instrument()
        return self._ms_instrument

    def set_ms_instrument(self):
        """
        Delicate function that mixes functionality it only works with pricing details from
        Main Sequence
        Returns
        -------

        """
        import mainsequence.markets.instruments as msi

        if self.current_pricing_detail:
            if hasattr(self.current_pricing_detail, "instrument_dump"):
                self._ms_instrument = msi.Instrument.rebuild(
                    self.current_pricing_detail.instrument_dump
                )
                return None
        raise Exception("Instrument does not have Main Sequence Current Pricing Details")

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
            raise_for_response(r)

        return cls(**r.json())

    @classmethod
    def filter_with_asset_class(
        cls, timeout=None, include_relationship_details_depth=None, *args, **kwargs
    ):
        """
        Filters assets and returns instances with their correct asset class,
        """

        base_url = cls.get_object_url()
        # Convert `kwargs` to query parameters
        # kwargs["include_relationship_details_depth"]=include_details
        transformed_kwargs = cls._translate_query_params(kwargs)
        transformed_kwargs = cls._normalize_filter_kwargs(transformed_kwargs)
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
                raise_for_response(r)

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
            raise_for_response(r)

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
    labels: list[str] = Field(
        default_factory=list,
        description=(
            "Organizational labels  "
            "These are helpers for grouping and discovery only and do not change runtime behavior or functionality."
        ),
    )

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
            raise_for_response(r)
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
            raise_for_response(r)
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
            raise_for_response(r)
        # Return a new instance of AssetCategory built from the response JSON.
        return AssetCategory(**r.json())


class Asset(AssetMixin, BaseObjectOrm):

    @classmethod
    def quick_search(
        cls,
        q: str,
        *,
        limit: int = 20,
        timeout: int | None = None,
    ) -> list[dict[str, Any]]:
        """
        Return lightweight asset matches for a name, ticker, or unique identifier.

        This hits the frontend-list serializer path:
            GET /orm/api/assets/asset/?response_format=frontend_list&search=...
        """
        normalized_query = (q or "").strip()
        if not normalized_query:
            raise ValueError("Asset search query cannot be empty.")

        limit = int(limit)
        if limit < 1 or limit > 100:
            raise ValueError("limit must be between 1 and 100")

        response = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="GET",
            url=f"{cls.get_object_url()}/",
            payload={
                "params": {
                    "response_format": "frontend_list",
                    "search": normalized_query,
                    "limit": limit,
                }
            },
            time_out=timeout,
        )
        raise_for_response(response)

        if not response.content:
            return []

        payload = response.json()
        if isinstance(payload, dict):
            results = payload.get("results") or []
        elif isinstance(payload, list):
            results = payload
        else:
            raise ValueError(
                f"Unexpected response type for Asset.quick_search(): {type(payload)!r}"
            )
        if not isinstance(results, list):
            raise ValueError(f"Unexpected results type for Asset.quick_search(): {type(results)!r}")
        return [dict(item) for item in results]

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
            raise_for_response(r)
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
            raise_for_response(r)
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
            raise_for_response(r)

        return [cls(**data) for data in r.json()]


class PortfolioIndexAsset(Asset):
    FILTERSET_FIELDS: ClassVar[dict[str, list[str]]] = {
        **AssetMixin.FILTERSET_FIELDS,
        "reference_portfolio__id": ["in", "exact"],
        "reference_portfolio__data_node_update__update_hash": ["in", "exact"],
    }
    FILTER_VALUE_NORMALIZERS: ClassVar[dict[str, str]] = {
        **AssetMixin.FILTER_VALUE_NORMALIZERS,
        "reference_portfolio__id": "id",
    }

    reference_portfolio: Union["Portfolio", int]

    @property
    def reference_portfolio_details_url(self):
        return f"{MAINSEQUENCE_ENDPOINT}/dashboards/portfolio-detail/?target_portfolio_id={self.reference_portfolios.id}"
