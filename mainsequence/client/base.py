import inspect
import os
from datetime import datetime

from pydantic import BaseModel, ConfigDict

from mainsequence import logger

from .exceptions import ApiError, raise_for_response
from .utils import (
    API_ENDPOINT,
    DATE_FORMAT,
    DoesNotExist,
    loaders,  # shared singleton
    make_request,
    request_to_datetime,
    serialize_to_json,
    session,  # shared singleton
)


class HtmlSaveException(Exception):
    def __init__(self, message):
        super().__init__(message)
        self.message = message
        self.file_path = None

        if "html" in message.lower():
            self.file_path = self.save_as_html_file()

    def save_as_html_file(self):
        # Get the name of the method that raised the exception
        caller_method = inspect.stack()[2].function

        # Get the current timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Create the directory to save HTML files if it doesn't exist
        folder_path = "html_exceptions"
        os.makedirs(folder_path, exist_ok=True)

        # Create the filename
        filename = f"{caller_method}_{timestamp}.html"
        file_path = os.path.join(folder_path, filename)

        # Save the message as an HTML file
        with open(file_path, "w") as file:
            file.write(self.message)

        return file_path

    def __str__(self):
        if self.file_path:
            return f"HTML content saved to {self.file_path}"
        else:
            return self.message


class BasePydanticModel(BaseModel):
    model_config = ConfigDict(extra="forbid")  # Forbid extra fields in v2
    orm_class: str = None  # This will be set to the class that inherits

    @classmethod
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        # Set orm_class to the class itself
        cls.orm_class = cls.__name__


class BaseObjectOrm:
    END_POINTS = {
        "User": "user",
        # VAM
        "Portfolio": "assets/target_portfolio",
        "PortfolioGroup": "assets/portfolio_group",
        "Asset": "assets/asset",
        "IndexAsset": "assets/index_asset",
        "AssetFutureUSDM": "assets/asset_future_usdm",
        "AssetCurrencyPair": "assets/asset_currency_pair",
        "VirtualFund": "assets/virtualfund",
        "OrderManager": "assets/order_manager",
        "ExecutionVenue": "assets/execution_venue",
        "Order": "assets/order",
        "MarketOrder": "assets/market_order",
        "LimitOrder": "assets/limit_order",
        "OrderEvent": "assets/order_event",
        "Account": "assets/account",
        "Trade": "assets/trade",
        "VirtualFundHistoricalHoldings": "assets/historical_holdings",
        "AccountHistoricalHoldings": "assets/account_historical_holdings",
        "AccountLatestHoldings": "assets/account_historical_holdings",
        "AccountRiskFactors": "assets/account_risk_factors",
        "AccountPortfolioScheduledRebalance": "assets/account_portfolio_scheduled_rebalance",
        "AccountPortfolioHistoricalPositions": "assets/account_portfolio_historical_positions",
        "ExecutionPrediction": "assets/execution_predictions",
        "ExecutionPositions": "assets/execution_positions",
        "AccountCoolDown": "assets/account_cooldown",
        "HistoricalWeights": "assets/portfolio_weights",
        "PortfolioIndexAsset": "assets/portfolio_index_asset",
        "HistoricalBarsSource": "data_sources/historical-bars-source",
        "MarketsTimeSeriesDetails": "data_sources/markets-time-series-details",
        "AssetCategory": "assets/asset-category",
        "AssetTranslationTable": "assets/asset-translation-tables",
        "InstrumentsConfiguration":"assets/instruments-configuration",
        # TDAG
        "Scheduler": "ts_manager/scheduler",
        "MultiIndexMetadata": "orm/multi_index_metadata",
        "ContinuousAggMultiIndex": "ts_manager/cont_agg_multi_ind",
        "DataNodeStorage": "ts_manager/dynamic_table",
        # "LocalTimeSerieNodesMethods": "ogm/local_time_serie",
        "LocalTimeSerieNodesMethods": "ts_manager/local_time_serie",
        "DataNodeUpdate": "ts_manager/local_time_serie",
        "DataNodeUpdateDetails": "ts_manager/local_time_serie_update_details",
        "LocalTimeSerieHistoricalUpdate": "ts_manager/lts_historical_update",
        "DynamicTableDataSource": "ts_manager/dynamic_table_data_source",
        "DataSource": "pods/data_source",
        "Project": "pods/projects",
        "SourceTableConfiguration": "ts_manager/source_table_config",
        "Artifact": "pods/artifact",
        "Job": "pods/job",
        "JobRun": "pods/job-run",
        "Constant": "pods/constant",
        "Secret": "pods/secret",
        "ProjectBaseImage":"pods/project-base-image",
        "ProjectImage":"pods/project-image",
        "GithubOrganization": "pods/github-organization",
        "JobApi":"pods/job-api",



        #AI
        "ResourceRelease": "pods/resource-release",

    }
    ROOT_URL = API_ENDPOINT
    LOADERS = loaders

    @staticmethod
    def request_to_datetime(string_date: str):
        return request_to_datetime(string_date=string_date)

    @staticmethod
    def date_to_string(target_date: datetime):
        return target_date.strftime(DATE_FORMAT)

    @classmethod
    def class_name(cls):
        if hasattr(cls, "CLASS_NAME"):
            return cls.CLASS_NAME
        return cls.__name__

    @classmethod
    def build_session(cls):
        s = session
        return s

    @property
    def s(self):
        s = self.build_session()
        return s

    def __hash__(self):
        if hasattr(self, "unique_identifier"):
            return hash(self.unique_identifier)
        return hash(self.id)

    def __repr__(self):
        object_id = self.id if hasattr(self, "id") else None
        return f"{self.class_name()}: {object_id}"

    @classmethod
    def get_object_url(cls, custom_endpoint_name=None):
        # 1) Preferred: model-local endpoint
        if custom_endpoint_name is None:
            endpoint = getattr(cls, "ENDPOINT", None)
            if endpoint:
                return f"{cls.ROOT_URL.rstrip('/')}/{endpoint.lstrip('/')}"

        # 2) Backwards-compatible: central mapping
        endpoint_name = custom_endpoint_name or cls.class_name()

        try:
            endpoint = cls.END_POINTS[endpoint_name]
        except KeyError as e:
            keys = sorted(cls.END_POINTS.keys())
            preview = ", ".join(keys[:20]) + ("..." if len(keys) > 20 else "")
            raise ValueError(
                f"Endpoint mapping missing for '{endpoint_name}'. "
                f"Fix: set {cls.__name__}.ENDPOINT = '<path>' "
                "or add it to BaseObjectOrm.END_POINTS. "
                f"Known keys (preview): {preview}"
            ) from e

        return f"{cls.ROOT_URL.rstrip('/')}/{endpoint.lstrip('/')}"

    @staticmethod
    def _parse_parameters_filter(parameters):
        for key, value in parameters.items():
            if "__in" in key:
                assert isinstance(value, list)
                value = [str(v) for v in value]
                parameters[key] = ",".join(value)
        return parameters

    @classmethod
    def iter_filter(cls, timeout=None, max_items: int | None = None, **kwargs):
        """
        Generator variant: yields objects across all pages without accumulating into memory.
        """
        base_url = cls.get_object_url()
        params = cls._parse_parameters_filter(kwargs)

        next_url = f"{base_url}/"
        yielded = 0

        while next_url:
            req_payload = {"params": params} if params else {}
            r = make_request(
                s=cls.build_session(),
                loaders=cls.LOADERS,
                r_type="GET",
                url=next_url,
                payload=req_payload,
                time_out=timeout,
            )
            raise_for_response(r, payload=req_payload)

            data = r.json()

            # DRF paginated: {"results": [...], "next": "..."}
            if isinstance(data, dict) and "results" in data:
                results = data.get("results") or []
                next_url = data.get("next")
            else:
                # Non-paginated endpoint: assume list payload
                results = data
                next_url = None

            for item in results:
                if isinstance(item, dict):
                    item.setdefault("orm_class", cls.__name__)
                    try:
                        obj = cls(**item) if issubclass(cls, BasePydanticModel) else item
                    except Exception as e:
                        raise e
                else:
                    obj = item

                yield obj
                yielded += 1
                if max_items is not None and yielded >= max_items:
                    return

            # Important: only send params on the first request; DRF `next` already contains querystring
            params = None

    @classmethod
    def filter(cls, timeout=None, **kwargs):
        """
        List-returning variant (backwards compatible).
        """
        return list(cls.iter_filter(timeout=timeout, **kwargs))

    @classmethod
    def get(cls, pk=None, timeout=None, **filters):
        """
        Retrieves exactly one object by primary key: GET /base_url/<pk>/
        Raises `DoesNotExist` if 404 or the response is empty.
        Raises Exception if multiple or unexpected data is returned.
        """
        if pk is not None:
            base_url = cls.get_object_url()
            detail_url = f"{base_url}/{pk}/"

            r = make_request(
                s=cls.build_session(),
                loaders=cls.LOADERS,
                r_type="GET",
                url=detail_url,
                payload={"params": filters},  # neede to pass special serializer
                time_out=timeout,
            )
            raise_for_response(r)


            data = r.json()
            data["orm_class"] = cls.__name__
            return cls(**data)

        # Otherwise, do the filter approach
        candidates = cls.filter(timeout=timeout, **filters)
        if not candidates:
            raise DoesNotExist(f"No {cls.class_name()} found matching {filters}")

        if len(candidates) > 1:
            raise ApiError(f"Multiple objects returned for {cls.__name__} with filters={filters}")


        return candidates[0]

    @classmethod
    def get_or_none(cls, *arg, **kwargs):
        try:
            return cls.get(*arg, **kwargs)
        except DoesNotExist:
            return None

    @staticmethod
    def serialize_for_json(kwargs):


        return serialize_to_json(kwargs)

    @classmethod
    def create(cls, timeout=None, files=None, *args, **kwargs):
        base_url = cls.get_object_url()
        data = cls.serialize_for_json(kwargs)
        payload = {"json": data}
        if files:
            payload["files"] = files
        r = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="POST",
            url=f"{base_url}/",
            payload=payload,
            time_out=timeout,
        )
        if r.status_code not in (200, 201):
            raise_for_response(r, payload=payload)
        return cls(**r.json())

    @classmethod
    def update_or_create(cls, timeout=None, *args, **kwargs):
        url = f"{cls.get_object_url()}/update_or_create/"
        data = cls.serialize_for_json(kwargs)
        payload = {"json": data}

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

    @classmethod
    def destroy_by_id(cls, instance_id, *args, **kwargs):
        base_url = cls.get_object_url()
        data = cls.serialize_for_json(kwargs)
        payload = {"json": data}
        r = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="DELETE",
            url=f"{base_url}/{instance_id}/",
            payload=payload,
        )
        if r.status_code != 204:
            raise_for_response(r)

    @classmethod
    def patch_by_id(cls, instance_id, *args, _into=None, **kwargs):
        base_url = cls.get_object_url()
        url = f"{base_url}/{instance_id}/"
        data = cls.serialize_for_json(kwargs)
        payload = {"json": data}

        r = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="PATCH",
            url=url,
            payload=payload,
        )
        if r.status_code != 200:
            raise_for_response(r)

        body = r.json()

        def recursive_update(obj, update_dict):
            for k, v in update_dict.items():
                # Get the existing nested object, defaulting to None if it doesn't exist
                nested_obj = getattr(obj, k, None)

                # Only recurse if the update value is a dict AND the existing
                # attribute is an instance of a Pydantic model.
                if isinstance(v, dict) and isinstance(nested_obj, BaseModel):
                    recursive_update(nested_obj, v)
                else:
                    # Otherwise, just set the value directly.
                    try:
                        setattr(obj, k, v)
                    except Exception as e:
                        logger.exception(e)

            return obj

        # If an instance was provided, update it in place
        if _into is not None:
            recursive_update(_into, body)
            return _into

        # Otherwise return a new instance
        return cls(**body)

    def patch(self, *args, **kwargs):
        return type(self).patch_by_id(self.id, _into=self, **kwargs)

    def delete(self, *args, **kwargs):
        return self.__class__.destroy_by_id(self.id)

    def get_app_label(self):
        return self.END_POINTS[self.orm_class].split("/")[0]
