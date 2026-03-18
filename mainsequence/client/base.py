import inspect
import os
from collections.abc import Callable, Iterable
from datetime import datetime
from typing import Any, ClassVar

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
    FILTERSET_FIELDS: ClassVar[dict[str, list[str]] | None] = None
    FILTER_VALUE_NORMALIZERS: ClassVar[dict[str, str | Callable[..., Any]]] = {}
    READ_QUERY_PARAMS: ClassVar[dict[str, str | Callable[..., Any]] | None] = None
    READ_QUERY_PARAM_DESCRIPTIONS: ClassVar[dict[str, str] | None] = None
    DESTROY_QUERY_PARAMS: ClassVar[dict[str, str | Callable[..., Any]] | None] = None
    DESTROY_QUERY_PARAM_DESCRIPTIONS: ClassVar[dict[str, str] | None] = None

    END_POINTS = {
        "User": "user",
        # VAM
        "Portfolio": "assets/target_portfolio",
        "PortfolioGroup": "assets/portfolio_group",
        "Asset": "assets/asset",
        "IndexAsset": "assets/index_asset",
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
        "ProjectResource":"pods/project-resource",
        "ResourceRelease":"pods/resource-release",



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

    @staticmethod
    def _coerce_filter_id(value: Any, *, field_name: str) -> int:
        if isinstance(value, int):
            return value
        if hasattr(value, "id") and value.id is not None:
            return int(value.id)
        if isinstance(value, dict) and value.get("id") is not None:
            return int(value["id"])
        raise TypeError(
            f"{field_name} must be an int id or an object with .id. Got: {type(value)!r}"
        )

    @staticmethod
    def _coerce_filter_bool(value: Any, *, field_name: str) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"1", "true", "yes"}:
                return True
            if normalized in {"0", "false", "no"}:
                return False
        raise TypeError(f"{field_name} must be a boolean value. Got: {value!r}")

    @classmethod
    def _filter_param_name(cls, field_name: str, lookup: str) -> str:
        return field_name if lookup == "exact" else f"{field_name}__{lookup}"

    @classmethod
    def _build_filter_param_specs(cls) -> dict[str, tuple[str, str]]:
        filterset_fields = getattr(cls, "FILTERSET_FIELDS", None)
        if not filterset_fields:
            return {}

        specs: dict[str, tuple[str, str]] = {}
        for field_name, lookups in filterset_fields.items():
            for lookup in lookups:
                specs[cls._filter_param_name(field_name, lookup)] = (field_name, lookup)
        return specs

    @classmethod
    def _resolve_filter_normalizer(
        cls,
        *,
        filter_key: str,
        field_name: str,
        lookup: str,
    ) -> str | Callable[..., Any] | None:
        normalizers = getattr(cls, "FILTER_VALUE_NORMALIZERS", {}) or {}
        return (
            normalizers.get(filter_key)
            or normalizers.get(field_name)
            or ("bool" if lookup == "isnull" else None)
        )

    @classmethod
    def _apply_filter_normalizer(
        cls,
        value: Any,
        *,
        filter_key: str,
        field_name: str,
        lookup: str,
    ) -> Any:
        normalizer = cls._resolve_filter_normalizer(
            filter_key=filter_key,
            field_name=field_name,
            lookup=lookup,
        )

        return cls._apply_declared_normalizer(
            value,
            field_name=filter_key,
            normalizer=normalizer,
        )

    @classmethod
    def _apply_declared_normalizer(
        cls,
        value: Any,
        *,
        field_name: str,
        normalizer: str | Callable[..., Any] | None,
        bool_as_query_string: bool = False,
    ) -> Any:
        if normalizer == "id":
            return cls._coerce_filter_id(value, field_name=field_name)
        if normalizer == "bool":
            normalized_bool = cls._coerce_filter_bool(value, field_name=field_name)
            if bool_as_query_string:
                return str(normalized_bool).lower()
            return normalized_bool
        if normalizer == "str":
            return str(value).strip()
        if callable(normalizer):
            try:
                return normalizer(value, field_name=field_name)
            except TypeError:
                return normalizer(value)
        if isinstance(value, str):
            return value.strip()
        return value

    @classmethod
    def _normalize_filter_kwargs(cls, kwargs: dict[str, Any]) -> dict[str, Any]:
        specs = cls._build_filter_param_specs()
        if not specs:
            return dict(kwargs)

        unexpected = sorted(key for key in kwargs.keys() if key not in specs)
        if unexpected:
            allowed_filters = ", ".join(sorted(specs.keys()))
            raise ValueError(
                f"Unsupported {cls.__name__} filter(s): {', '.join(unexpected)}. "
                f"Allowed filters: {allowed_filters}."
            )

        normalized: dict[str, Any] = {}
        for filter_key, value in kwargs.items():
            if value is None:
                continue

            field_name, lookup = specs[filter_key]
            if lookup == "in":
                if isinstance(value, Iterable) and not isinstance(value, (str, bytes, dict)):
                    normalized[filter_key] = [
                        cls._apply_filter_normalizer(
                            item,
                            filter_key=filter_key,
                            field_name=field_name,
                            lookup=lookup,
                        )
                        for item in value
                    ]
                else:
                    normalized[filter_key] = [
                        cls._apply_filter_normalizer(
                            value,
                            filter_key=filter_key,
                            field_name=field_name,
                            lookup=lookup,
                        )
                    ]
                continue

            normalized[filter_key] = cls._apply_filter_normalizer(
                value,
                filter_key=filter_key,
                field_name=field_name,
                lookup=lookup,
            )

        return normalized

    @classmethod
    def _normalize_declared_query_kwargs(
        cls,
        kwargs: dict[str, Any],
        *,
        declared_params: dict[str, str | Callable[..., Any]] | None,
        label: str,
    ) -> dict[str, Any]:
        if not declared_params:
            return dict(kwargs)

        unexpected = sorted(key for key in kwargs.keys() if key not in declared_params)
        if unexpected:
            allowed_params = ", ".join(sorted(declared_params.keys()))
            raise ValueError(
                f"Unsupported {cls.__name__} {label}(s): {', '.join(unexpected)}. "
                f"Allowed parameters: {allowed_params}."
            )

        normalized: dict[str, Any] = {}
        for param_name, value in kwargs.items():
            if value is None:
                continue

            normalized[param_name] = cls._apply_declared_normalizer(
                value,
                field_name=param_name,
                normalizer=declared_params.get(param_name),
                bool_as_query_string=True,
            )

        return normalized

    @classmethod
    def _normalize_read_query_kwargs(cls, kwargs: dict[str, Any]) -> dict[str, Any]:
        return cls._normalize_declared_query_kwargs(
            kwargs,
            declared_params=getattr(cls, "READ_QUERY_PARAMS", None),
            label="read parameter",
        )

    @classmethod
    def _normalize_destroy_kwargs(cls, kwargs: dict[str, Any]) -> dict[str, Any]:
        return cls._normalize_declared_query_kwargs(
            kwargs,
            declared_params=getattr(cls, "DESTROY_QUERY_PARAMS", None),
            label="delete parameter",
        )

    @classmethod
    def _split_filter_and_read_query_kwargs(
        cls,
        kwargs: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        read_query_params = getattr(cls, "READ_QUERY_PARAMS", None) or {}
        if not read_query_params:
            return dict(kwargs), {}

        filter_kwargs: dict[str, Any] = {}
        read_query_kwargs: dict[str, Any] = {}
        for key, value in kwargs.items():
            if key in read_query_params:
                read_query_kwargs[key] = value
            else:
                filter_kwargs[key] = value
        return filter_kwargs, read_query_kwargs

    @classmethod
    def iter_filter(cls, timeout=None, max_items: int | None = None, **kwargs):
        """
        Generator variant: yields objects across all pages without accumulating into memory.
        """
        filter_kwargs, read_query_kwargs = cls._split_filter_and_read_query_kwargs(kwargs)
        normalized_filters = cls._normalize_filter_kwargs(filter_kwargs)
        normalized_read_query = cls._normalize_read_query_kwargs(read_query_kwargs)
        base_url = cls.get_object_url()
        params = cls._parse_parameters_filter({**normalized_filters, **normalized_read_query})

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
            _, read_query_kwargs = cls._split_filter_and_read_query_kwargs(filters)
            normalized_read_query = cls._normalize_read_query_kwargs(read_query_kwargs)
            extra_params = {
                key: value for key, value in filters.items() if key not in read_query_kwargs
            }
            params = {**extra_params, **normalized_read_query}

            r = make_request(
                s=cls.build_session(),
                loaders=cls.LOADERS,
                r_type="GET",
                url=detail_url,
                payload={"params": params},  # neede to pass special serializer
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
    def destroy_by_id(cls, instance_id, *args, timeout=None, **kwargs):
        base_url = cls.get_object_url()
        payload: dict[str, Any] = {}

        if getattr(cls, "DESTROY_QUERY_PARAMS", None):
            normalized_params = cls._normalize_destroy_kwargs(kwargs)
            if normalized_params:
                payload["params"] = normalized_params
        else:
            data = cls.serialize_for_json(kwargs)
            if data:
                payload["json"] = data

        r = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="DELETE",
            url=f"{base_url}/{instance_id}/",
            payload=payload,
            time_out=timeout,
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
        return self.__class__.destroy_by_id(self.id, *args, **kwargs)

    def get_app_label(self):
        return self.END_POINTS[self.orm_class].split("/")[0]


class ShareableObjectMixin:
    SHARING_ACTION_PATHS: ClassVar[dict[str, str]] = {
        "add_to_view": "add-to-view",
        "add_to_edit": "add-to-edit",
        "remove_from_edit": "remove-from-edit",
        "remove_from_view": "remove-from-view",
        "can_view": "can-view",
        "can_edit": "can-edit",
        "users_can_view": "can-view",
        "users_can_edit": "can-edit",
    }

    def get_detail_url(self) -> str:
        object_id = getattr(self, "id", None)
        if object_id is None:
            raise ValueError(f"{type(self).__name__} must have an id before calling detail actions.")

        base = type(self).get_object_url().rstrip("/")
        return f"{base}/{object_id}/"

    def get_action_url(self, action_name: str) -> str:
        return f"{self.get_detail_url().rstrip('/')}/{action_name.strip('/')}/"

    def _post_sharing_action(
        self,
        action_name: str,
        *,
        user_id: Any,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> dict[str, Any]:
        normalized_user_id = type(self)._coerce_filter_id(user_id, field_name="user_id")
        payload = {"json": {"user_id": normalized_user_id}}

        response = make_request(
            s=type(self).build_session(),
            loaders=type(self).LOADERS,
            r_type="POST",
            url=self.get_action_url(action_name),
            payload=payload,
            time_out=timeout,
        )
        if response.status_code not in (200, 201, 202):
            raise_for_response(response, payload=payload)

        if not getattr(response, "content", b""):
            return {}

        return response.json()

    def _get_sharing_state(
        self,
        action_name: str,
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> Any:
        response = make_request(
            s=type(self).build_session(),
            loaders=type(self).LOADERS,
            r_type="GET",
            url=self.get_action_url(action_name),
            payload={},
            time_out=timeout,
        )
        if response.status_code != 200:
            raise_for_response(response)

        data = response.json()
        if not isinstance(data, dict):
            raise ApiError(
                f"Unexpected {type(self).__name__} sharing response for action "
                f"{action_name!r}: {type(data)!r}"
            )

        from .models_user import ShareableAccessState

        return ShareableAccessState.model_validate(data)

    def add_to_view(
        self,
        user_id: Any,
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> dict[str, Any]:
        return self._post_sharing_action(
            self.SHARING_ACTION_PATHS["add_to_view"],
            user_id=user_id,
            timeout=timeout,
        )

    def add_to_edit(
        self,
        user_id: Any,
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> dict[str, Any]:
        return self._post_sharing_action(
            self.SHARING_ACTION_PATHS["add_to_edit"],
            user_id=user_id,
            timeout=timeout,
        )

    def remove_from_edit(
        self,
        user_id: Any,
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> dict[str, Any]:
        return self._post_sharing_action(
            self.SHARING_ACTION_PATHS["remove_from_edit"],
            user_id=user_id,
            timeout=timeout,
        )

    def remove_from_view(
        self,
        user_id: Any,
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> dict[str, Any]:
        return self._post_sharing_action(
            self.SHARING_ACTION_PATHS["remove_from_view"],
            user_id=user_id,
            timeout=timeout,
        )

    def can_view(
        self,
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> Any:
        return self._get_sharing_state(
            self.SHARING_ACTION_PATHS["can_view"],
            timeout=timeout,
        )

    def users_can_view(
        self,
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> Any:
        return self.can_view(timeout=timeout)

    def list_users_can_view(
        self,
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> Any:
        return self.can_view(timeout=timeout)

    def can_edit(
        self,
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> Any:
        return self._get_sharing_state(
            self.SHARING_ACTION_PATHS["can_edit"],
            timeout=timeout,
        )

    def users_can_edit(
        self,
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> Any:
        return self.can_edit(timeout=timeout)

    def list_users_can_edit(
        self,
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> Any:
        return self.can_edit(timeout=timeout)
