import copy
import inspect
import os
from datetime import datetime

import requests
from pydantic import BaseModel, ConfigDict

from .utils import (
    DATE_FORMAT,
    AuthLoaders,
    DoesNotExist,
    make_request,
    request_to_datetime,
)

TDAG_ENDPOINT = os.environ.get("TDAG_ENDPOINT", "https://main-sequence.app")
API_ENDPOINT = f"{TDAG_ENDPOINT}/orm/api"

loaders = AuthLoaders()


def build_session(loaders):
    from requests.adapters import HTTPAdapter, Retry

    s = requests.Session()
    s.headers.update(loaders.auth_headers)
    retries = Retry(
        total=2,
        backoff_factor=2,
    )
    s.mount("http://", HTTPAdapter(max_retries=retries))
    s.headers["Accept-Encoding"] = "gzip"
    return s


session = build_session(loaders=loaders)


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
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

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
        "DynamicResource": "tdag-gpt/dynamic_resource",
        "Artifact": "pods/artifact",
        "Job": "pods/job",
        "Constant": "pods/constant",
        # ReportBuilder
        "Presentation": "reports/presentations",
        "Folder": "reports/folder",
        "Slide": "reports/slides",
        "Theme": "reports/themes",
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

    def ___hash__(self):
        if hasattr(self, "unique_identifier"):
            return self.unique_identifier
        return self.id

    def __repr__(self):
        object_id = self.id if hasattr(self, "id") else None
        return f"{self.class_name()}: {object_id}"

    @classmethod
    def get_object_url(cls, custom_endpoint_name=None):
        endpoint_name = custom_endpoint_name or cls.class_name()
        return f"{cls.ROOT_URL}/{cls.END_POINTS[endpoint_name]}"

    @staticmethod
    def _parse_parameters_filter(parameters):
        for key, value in parameters.items():
            if "__in" in key:
                assert isinstance(value, list)
                value = [str(v) for v in value]
                parameters[key] = ",".join(value)
        return parameters

    @classmethod
    def filter(cls, timeout=None, **kwargs):
        """
        Fetches *all pages* from a DRF-paginated endpoint.
        Accumulates results from each page until 'next' is None.

        Returns a list of `cls` objects (not just one page).

        DRF's typical paginated response looks like:
            {
              "count": <int>,
              "next": <str or null>,
              "previous": <str or null>,
              "results": [ ...items... ]
            }
        """
        base_url = cls.get_object_url()  # e.g. "https://api.example.com/assets"
        params = cls._parse_parameters_filter(kwargs)

        # We'll handle pagination by following the 'next' links from DRF.
        accumulated = []
        next_url = f"{base_url}/"  # Start with the main endpoint (list)

        while next_url:
            # For each page, do a GET request
            r = make_request(
                s=cls.build_session(),
                loaders=cls.LOADERS,
                r_type="GET",
                url=next_url,  # next_url changes each iteration
                payload={"params": params},
                time_out=timeout,
            )

            if r.status_code != 200:
                # Handle errors or break out
                if r.status_code == 401:
                    raise Exception("Unauthorized. Please add credentials to environment.")
                elif r.status_code == 500:
                    raise Exception("Server Error.")
                elif r.status_code == 404:
                    raise DoesNotExist("Not Found.")
                else:
                    raise Exception(f"{r.status_code} - {r.text}")

            data = r.json()
            # data should be a dict with "count", "next", "previous", and "results".

            # DRF returns the next page URL in `data["next"]`
            next_url = data["next"]  # either a URL string or None

            # data["results"] should be a list of objects
            for item in data["results"]:
                # Insert "orm_class" if you still need that
                item["orm_class"] = cls.__name__
                try:
                    accumulated.append(cls(**item) if issubclass(cls, BasePydanticModel) else item)
                except Exception as e:
                    print(item)
                    print(cls)
                    print(cls(**item))
                    import traceback

                    traceback.print_exc()
                    raise e

            # We set `params = None` (or empty) after the first loop to avoid appending repeatedly
            # but only if DRF's `next` doesn't contain the query parameters.
            # Usually, DRF includes them, so you don't need to do anything special here.
            params = None

        return accumulated

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

            if r.status_code == 404:
                raise DoesNotExist(f"No object found for pk={pk}")
            elif r.status_code == 401:
                raise Exception("Unauthorized. Please add credentials to environment.")
            elif r.status_code == 500:
                raise Exception("Server Error")
            elif r.status_code != 200:
                raise Exception(f"Unexpected status code: {r.status_code}")

            data = r.json()
            data["orm_class"] = cls.__name__
            return cls(**data)

        # Otherwise, do the filter approach
        candidates = cls.filter(timeout=timeout, **filters)
        if not candidates:
            raise DoesNotExist(f"No {cls.class_name()} found matching {filters}")

        if len(candidates) > 1:
            raise Exception(
                f"Multiple {cls.class_name()} objects found for filters {filters}. "
                f"Expected exactly one."
            )

        return candidates[0]

    @classmethod
    def get_or_none(cls, *arg, **kwargs):
        try:
            return cls.get(*arg, **kwargs)
        except DoesNotExist:
            return None

    @staticmethod
    def serialize_for_json(kwargs):
        new_data = {}
        for key, value in kwargs.items():
            new_value = copy.deepcopy(value)
            if isinstance(value, datetime):
                new_value = str(value)
            new_data[key] = new_value
        return new_data

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
        if r.status_code not in [201]:
            raise Exception(r.text)
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
            raise Exception(r.text)
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
            raise Exception(r.text)

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
            raise Exception(r.text)

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
                        print(e)

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
