import datetime
import inspect
import json
import logging
import os
import warnings
from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import asdict
from functools import wraps
from typing import Any, Union

import pandas as pd
import pytz
import structlog.contextvars as cvars

import mainsequence.tdag.data_nodes.build_operations as build_operations
import mainsequence.tdag.data_nodes.run_operations as run_operations
from mainsequence.client.models_metatables import MetaTable
from mainsequence.client.models_tdag import (
    BaseUpdateStatistics,
    DataNodeUpdate,
    DataSource,
    DynamicTableDataSource,
    Scheduler,
    SessionDataSource,
    UpdateStatistics,
)
from mainsequence.client.utils import TDAG_CONSTANTS as CONSTANTS
from mainsequence.instrumentation import tracer
from mainsequence.logconf import logger
from mainsequence.tdag.data_nodes.persist_managers import APIPersistManager, PersistManager
from mainsequence.tdag.meta_tables import PlatformTimeIndexMetaData

from .models import BaseConfiguration
from .namespacing import current_hash_namespace
from .namespacing import hash_namespace as _hash_namespace_cm

LocalUpdateResult = None | pd.DataFrame | Sequence[Any]


class DependencyUpdateError(Exception):
    pass


class DataAccessMixin:
    """A mixin for classes that provide access to time series data."""

    def __repr__(self) -> str:
        return self.__class__.__name__

    def get_last_observation(
        self,
        *,
        dimension_filters: dict[str, list[Any]] | None = None,
        index_coordinates: list[dict[str, Any]] | None = None,
        dimension_range_map: list[dict[str, Any]] | None = None,
    ):
        """Return the latest observation using generic TDAG dimensions."""
        return self.local_persist_manager.get_last_observation(
            dimension_filters=dimension_filters,
            index_coordinates=index_coordinates,
            dimension_range_map=dimension_range_map,
        )

    def get_logger_context_variables(self) -> dict[str, Any]:
        return dict(
            update_hash=self.update_hash,
            local_hash_uid_data_source=self.data_source_uid,
            api_time_series=self.__class__.__name__ == "APIDataNode",
        )

    @property
    def logger(self) -> logging.Logger:
        """Gets a logger instance with bound context variables."""
        # import structlog.contextvars as cvars
        # cvars.bind_contextvars(update_hash=self.update_hash,
        #                      data_source_uid=self.data_source_uid,
        #                      api_time_series=True,)
        global logger
        if not hasattr(self, "_logger"):
            cvars.bind_contextvars(**self.get_logger_context_variables())
            self._logger = logger

        return self._logger

    @staticmethod
    def set_context_in_logger(logger_context: dict[str, Any]) -> None:
        """
        Binds context variables to the global logger.

        Args:
            logger_context: A dictionary of context variables.
        """
        global logger
        for key, value in logger_context.items():
            logger.bind(**{key: value})

    def unbind_context_variables_from_logger(self) -> None:
        cvars.unbind_contextvars(*self.get_logger_context_variables().keys())

    def get_df_between_dates(
        self,
        start_date: datetime.datetime | None = None,
        end_date: datetime.datetime | None = None,
        great_or_equal: bool = True,
        less_or_equal: bool = True,
        dimension_filters: dict[str, list] | None = None,
        index_coordinates: list[dict] | None = None,
        dimension_range_map: list[dict] | None = None,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        """Retrieve rows using time bounds and explicit TDAG dimensions.

        `dimension_filters`, `index_coordinates`, and `dimension_range_map`
        are the only identity-scoping inputs accepted by core TDAG. Domain
        specific helpers should live outside the TDAG core.
        """
        return self.local_persist_manager.get_df_between_dates(
            start_date=start_date,
            end_date=end_date,
            great_or_equal=great_or_equal,
            less_or_equal=less_or_equal,
            dimension_filters=dimension_filters,
            index_coordinates=index_coordinates,
            dimension_range_map=dimension_range_map,
            columns=columns,
        )


class APIDataNode(DataAccessMixin):
    @staticmethod
    def _require_data_source_uid(data_source: Any, *, context: str) -> str:
        if isinstance(data_source, dict):
            data_source_uid = data_source.get("uid")
        else:
            data_source_uid = getattr(data_source, "uid", None)
        if data_source_uid in (None, ""):
            raise ValueError(f"{context} requires data_source.uid.")
        return str(data_source_uid)

    @classmethod
    def _require_storage_table_data_source_uid(
        cls,
        storage_table: Any,
        *,
        context: str,
    ) -> str:
        if isinstance(storage_table, dict):
            data_source_uid = storage_table.get("data_source_uid")
            data_source = storage_table.get("data_source")
        else:
            data_source_uid = getattr(storage_table, "data_source_uid", None)
            data_source = getattr(storage_table, "data_source", None)

        if data_source_uid not in (None, ""):
            return str(data_source_uid)
        return cls._require_data_source_uid(data_source, context=context)

    @staticmethod
    def _require_storage_hash(storage_table: Any, *, context: str) -> str:
        if isinstance(storage_table, dict):
            storage_hash = storage_table.get("storage_hash")
        else:
            storage_hash = getattr(storage_table, "storage_hash", None)
        if storage_hash in (None, ""):
            raise ValueError(f"{context} requires storage_table.storage_hash.")
        return str(storage_hash)

    @classmethod
    def build_from_local_time_serie(cls, source_table: "DataNodeUpdate") -> "APIDataNode":
        return cls(
            data_source_uid=cls._require_data_source_uid(
                source_table.data_source,
                context="APIDataNode.build_from_local_time_serie",
            ),
            storage_hash=source_table.storage_hash,
        )

    @classmethod
    def build_from_table_uid(cls, meta_table_uid: str) -> "APIDataNode":
        storage_table = MetaTable.get(uid=meta_table_uid)
        return cls.build_from_meta_table(storage_table)

    @classmethod
    def build_from_meta_table(cls, storage_table: MetaTable) -> "APIDataNode":
        context = "APIDataNode.build_from_meta_table"
        return cls(
            data_source_uid=cls._require_storage_table_data_source_uid(
                storage_table,
                context=context,
            ),
            storage_hash=cls._require_storage_hash(storage_table, context=context),
            storage_table=storage_table,
        )

    @classmethod
    def build_from_identifier(cls, identifier: str) -> "APIDataNode":
        storage_table = MetaTable.get(identifier=identifier)
        return cls.build_from_meta_table(storage_table)

    def __init__(
        self,
        data_source_uid: str,
        storage_hash: str,
        data_source_local_lake: DataSource | None = None,
        storage_table: MetaTable | None = None,
    ):
        """
        Initializes an APIDataNode.

        Args:
            data_source_uid: The UID of the data source.
            storage_hash: The storage hash of the data node table.
            data_source_local_lake: Optional local data source for the lake.
            storage_table: Optional resolved MetaTable backing this read wrapper.
        """
        if data_source_local_lake is not None:
            assert (
                data_source_local_lake.data_type in CONSTANTS.DATA_SOURCE_TYPE_LOCAL_DISK_LAKE
            ), "data_source_local_lake should be of type CONSTANTS.DATA_SOURCE_TYPE_LOCAL_DISK_LAKE"

        if data_source_uid in (None, ""):
            raise ValueError("APIDataNode requires data_source_uid.")
        self.data_source_uid = str(data_source_uid)
        self.storage_hash = storage_hash
        self.storage_table = storage_table
        self.data_source = data_source_local_lake
        self._local_persist_manager: APIPersistManager = None
        self.update_statistics = None

    def __repr__(self) -> str:
        return self.__class__.__name__

    @property
    def is_api(self):
        return True

    @staticmethod
    def _get_update_hash(storage_hash):
        return "API_" + f"{storage_hash}"

    @property
    def update_hash(self):
        return self._get_update_hash(storage_hash=self.storage_hash)

    @property
    def local_persist_manager(self) -> Any:
        """Gets the local persistence manager, initializing it if necessary."""
        if self._local_persist_manager is None:
            self._set_local_persist_manager()
            self.logger.debug(f"Setting local persist manager for {self.storage_hash}")
        return self._local_persist_manager

    def set_relation_tree(self) -> None:
        pass  # do nothing  for API Time Series

    def _verify_local_data_source(self) -> None:
        """Verifies and sets the local data source from environment variables if available."""
        pod_source = os.environ.get("POD_DEFAULT_DATA_SOURCE", None)
        if pod_source is not None:
            from mainsequence.client import models as models

            pod_source = json.loads(pod_source)
            ModelClass = pod_source["tdag_orm_class"]
            pod_source.pop("tdag_orm_class", None)
            ModelClass = getattr(models, ModelClass)
            pod_source = ModelClass(**pod_source)
            self.data_source = pod_source

    def build_data_source_from_configuration(self, data_config: dict[str, Any]) -> DataSource:
        """
        Builds a data source object from a configuration dictionary.

        Args:
            data_config: The data source configuration.

        Returns:
            A DataSource object.
        """
        data_source_uid = data_config.get("uid")
        if data_source_uid in (None, ""):
            raise ValueError("Data source configuration requires uid.")
        ModelClass = DynamicTableDataSource.get_class(data_config["data_type"])
        pod_source = ModelClass.get(uid=data_source_uid)
        return pod_source

    def _set_local_persist_manager(self) -> None:
        self._verify_local_data_source()
        self._local_persist_manager = APIPersistManager(
            storage_hash=self.storage_hash,
            data_source_uid=self.data_source_uid,
        )
        storage_table = self._local_persist_manager.storage_table

        assert storage_table is not None, f"Verify that the table {self.storage_hash} exists "

    def get_update_statistics(self):
        """
        Gets update statistics from the database.
        """
        return self.local_persist_manager.storage_table.get_data_updates()

    def update(self, *args, **kwargs) -> pd.DataFrame:
        self.logger.info("Not updating series")
        pass


class DataNode(DataAccessMixin, ABC):
    """
    Base class for building and maintaining datasets in Main Sequence.

    A ``DataNode`` is the update process recipe:

    - configuration for update behavior,
    - dependencies,
    - update logic.

    Two identities matter:

    - ``storage_table``: the first-class PlatformTimeIndexMetaData storage contract.
    - ``update_hash``: identifies the updater job writing to that table.

    This separation lets you run different updater jobs (for example, asset shards)
    while writing into the same table safely.

    Hash namespace
    --------------
    ``DataNode`` also supports ``hash_namespace`` for test and experiment isolation.

    Resolution order is:

    1. explicit ``hash_namespace="..."`` passed at construction,
    2. subclass-declared aliases such as ``namespace="..."``,
    3. the active ``with hash_namespace("...")`` context manager,
    4. otherwise, no namespace.

    A non-empty namespace is injected into the build configuration, which changes
    ``update_hash``. Storage identity stays on the explicit ``storage_table``.

    During ``run()``, the active namespace is re-applied around the full run so
    dependencies created inside ``dependencies()`` inherit the same isolation.

    Subclass checklist:

    - Keep constructor args stable and serializable (Pydantic config is recommended).
    - Build dependencies in ``__init__`` and return them in ``dependencies()``.
    - Use ``self.update_statistics`` in ``update()`` and return only incremental rows.
    - Provide table/column metadata for production datasets.
    """

    OFFSET_START = datetime.datetime(2018, 1, 1, tzinfo=pytz.utc)
    DATA_NODE_UPDATE_CLASS = DataNodeUpdate

    def __init__(
        self,
        config: BaseConfiguration,
        storage_table: type[PlatformTimeIndexMetaData],
        *,
        hash_namespace: str | None = None,
    ):
        """
        Initialize framework-level state for the node.

        Preferred pattern:
        subclasses build a concrete ``DataNodeConfiguration`` and pass it as
        ``super().__init__(config=...)``.

        The initial fallback start date for first-run updates is ``OFFSET_START``.

        Parameters
        ----------
        config : BaseConfiguration
            Canonical node configuration for this node.
        storage_table : type[PlatformTimeIndexMetaData]
            Explicit time-indexed platform storage model where this update
            process writes. This is runtime state, not part of the build
            configuration payload.
        hash_namespace : str | None
            Optional hash isolation namespace.
        """
        if not isinstance(config, BaseConfiguration):
            raise TypeError(
                f"{self.__class__.__name__} expected config to be a BaseConfiguration; "
                f"got {type(config).__name__}."
            )
        if not issubclass(storage_table, PlatformTimeIndexMetaData):
            raise TypeError(
                f"{self.__class__.__name__} expected storage_table to be a subclass of "
                f"PlatformTimeIndexMetaData; got {storage_table}."
            )

        self.pre_load_routines_run = False
        self._local_persist_manager: PersistManager | None = None
        self.storage_table = storage_table

        self._scheduler_tree_connected = False
        self.update_statistics = None
        self.config = config
        self._framework_initialized = True

        if hash_namespace is not None:
            self._hash_namespace = (hash_namespace or "").strip()

    def __init_subclass__(cls, **kwargs):
        """
        Wrap subclass construction so DataNode can capture config, compute hashes,
        and apply namespace/test controls consistently.

        The wrapper consumes ``hash_namespace`` before the subclass ``__init__`` runs:

        - ``hash_namespace="..."``: explicit hash isolation namespace

        Namespace precedence is:

        1. explicit ``hash_namespace``
        2. subclass-declared aliases such as ``namespace``
        3. active namespacing context manager

        Only a non-empty namespace is injected into the hashed build configuration.
        That preserves backward compatibility for normal runs while allowing isolated
        test tables when needed.
        """
        super().__init_subclass__(**kwargs)

        if "_ARGS_IGNORE_IN_STORAGE_HASH" in cls.__dict__:
            raise TypeError(
                f"{cls.__name__} uses removed class attribute _ARGS_IGNORE_IN_STORAGE_HASH; "
                "move those fields into DataNodeConfiguration. Configuration fields "
                "participate in update hashing by default."
            )

        # Get the original __init__ from the new subclass
        original_init = cls.__init__

        @wraps(original_init)
        def wrapped_init(self, *args, **kwargs):
            # ---- hashing namespace controls (never forwarded to user __init__) ----
            if "test_node" in kwargs:
                raise TypeError(
                    "test_node has been removed from DataNode construction; use "
                    'hash_namespace="..." or with hash_namespace("...") instead.'
                )
            explicit_namespace = kwargs.pop("hash_namespace", None)
            namespace_aliases = tuple(
                getattr(cls, "_HASH_NAMESPACE_ALIASES", ()) or ()
            )
            provided_namespace_aliases = [
                alias for alias in namespace_aliases if alias in kwargs
            ]
            alias_namespace = None
            alias_namespace_provided = bool(provided_namespace_aliases)
            if len(provided_namespace_aliases) > 1:
                raise ValueError(
                    f"{cls.__name__} received multiple hash namespace aliases: "
                    f"{provided_namespace_aliases!r}."
                )
            if alias_namespace_provided:
                alias_namespace = kwargs.pop(provided_namespace_aliases[0])
            if (
                explicit_namespace is not None
                and alias_namespace_provided
                and (explicit_namespace or "").strip()
                != (alias_namespace or "").strip()
            ):
                raise ValueError(
                    f"{cls.__name__} received both hash_namespace and "
                    f"{provided_namespace_aliases[0]} with different values."
                )
            if "init_meta" in kwargs:
                raise TypeError(
                    "init_meta has been removed from DataNode construction; remove this keyword argument."
                )

            # Determine namespace:
            # 1) explicit hash_namespace kwarg wins
            # 2) subclass-declared alias kwarg, such as namespace
            # 3) else: context manager namespace
            if explicit_namespace is not None:
                namespace = explicit_namespace
            elif alias_namespace_provided:
                namespace = alias_namespace
            else:
                namespace = current_hash_namespace()

            namespace = (namespace or "").strip()
            # 1. Call the original __init__ of the subclass first
            if namespace:
                with _hash_namespace_cm(namespace):
                    original_init(self, *args, **kwargs)
            else:
                original_init(self, *args, **kwargs)

            if not getattr(self, "_framework_initialized", False):
                raise TypeError(
                    f"{self.__class__.__name__} must call super().__init__(config=..., "
                    "hash_namespace=...) from its constructor."
                )
            if not isinstance(getattr(self, "config", None), BaseConfiguration):
                raise TypeError(
                    f"{self.__class__.__name__} must pass a BaseConfiguration to super().__init__(config=...)."
                )

            # 2. Capture all arguments from __init__ methods in the MRO up to DataNode
            final_kwargs = {}
            mro = self.__class__.mro()

            try:
                # We want to inspect from parent to child to ensure subclass arguments override.
                # The MRO is ordered from child to parent, so we find DataNode and reverse the part before it.
                data_node_index = mro.index(DataNode)
                classes_to_inspect = reversed(mro[:data_node_index])
            except ValueError:
                # Fallback if DataNode is not in the MRO.
                classes_to_inspect = [self.__class__]

            def _bind_supported_arguments(sig: inspect.Signature) -> inspect.BoundArguments:
                remaining_args = list(args)
                positional_args: list[Any] = []
                consumed_positionally: set[str] = set()
                accepts_var_keyword = False

                for param in sig.parameters.values():
                    if param.name == "self":
                        continue

                    if param.kind in (
                        inspect.Parameter.POSITIONAL_ONLY,
                        inspect.Parameter.POSITIONAL_OR_KEYWORD,
                    ):
                        if remaining_args:
                            positional_args.append(remaining_args.pop(0))
                            consumed_positionally.add(param.name)
                    elif param.kind == inspect.Parameter.VAR_POSITIONAL:
                        positional_args.extend(remaining_args)
                        remaining_args.clear()
                    elif param.kind == inspect.Parameter.VAR_KEYWORD:
                        accepts_var_keyword = True

                filtered_kwargs: dict[str, Any] = {}
                extra_kwargs: dict[str, Any] = {}

                for key, value in kwargs.items():
                    param = sig.parameters.get(key)

                    if param is None:
                        if accepts_var_keyword:
                            extra_kwargs[key] = value
                        continue

                    if key in consumed_positionally:
                        continue

                    if param.kind in (
                        inspect.Parameter.POSITIONAL_OR_KEYWORD,
                        inspect.Parameter.KEYWORD_ONLY,
                    ):
                        filtered_kwargs[key] = value
                    elif param.kind == inspect.Parameter.VAR_KEYWORD:
                        extra_kwargs[key] = value

                if accepts_var_keyword:
                    filtered_kwargs.update(extra_kwargs)

                return sig.bind_partial(self, *positional_args, **filtered_kwargs)

            for cls_to_inspect in classes_to_inspect:
                # Only inspect the __init__ defined on the class itself.
                if "__init__" in cls_to_inspect.__dict__:
                    sig = inspect.signature(cls_to_inspect.__init__)
                    try:
                        bound_args = _bind_supported_arguments(sig)
                        bound_args.apply_defaults()

                        current_args = dict(bound_args.arguments)
                        current_args.pop("self", None)

                        for param in sig.parameters.values():
                            if param.kind == inspect.Parameter.VAR_POSITIONAL:
                                current_args.pop(param.name, None)
                            elif param.kind == inspect.Parameter.VAR_KEYWORD:
                                final_kwargs.update(current_args.pop(param.name, {}))

                        # Update the final arguments. Overwrites parent args with child args.
                        final_kwargs.update(current_args)
                    except TypeError as exc:
                        logger.warning(
                            f"Could not bind filtered arguments for "
                            f"{cls_to_inspect.__name__}.__init__; skipping for config. "
                            f"Error: {exc}"
                        )
                        continue

            # Remove `args` as it collects un-named positional arguments which are not part of the config hash.
            final_kwargs.pop("args", None)
            for namespace_alias in namespace_aliases:
                final_kwargs.pop(namespace_alias, None)
            storage_table = final_kwargs.pop("storage_table", None)
            if storage_table is not None:
                self.storage_table = storage_table
            if final_kwargs.get("config") is None:
                final_kwargs.pop("config", None)
            if not final_kwargs:
                final_kwargs["config"] = self.config

            # ---- the surgical part: only change hashes when namespace is non-empty ----
            # Backward compatibility guarantee:
            # - if no namespace => NOTHING added => hashes identical to normal behavior
            self._hash_namespace = namespace
            if self._hash_namespace:
                final_kwargs["hash_namespace"] = self._hash_namespace
                logger.debug(f"Running on namespace {self._hash_namespace}")
            # 3. Run the post-initialization routines
            self.build_configuration = final_kwargs
            logger.debug(f"Running post-init routines for {self.__class__.__name__}")
            self._initialize_configuration(init_kwargs=final_kwargs)

            logger.bind(update_hash=self.update_hash)

            self.run_after_post_init_routines()

            # requirements for graph update
            self.dependencies_df: pd.DataFrame | None = None
            self.depth_df: pd.DataFrame | None = None

            self.scheduler: Scheduler | None = None
            self.update_details_tree: dict[str, Any] | None = None

            logger.debug(f"Post-init routines for {self.__class__.__name__} complete.")

        # Replace the subclass's __init__ with our new wrapped version
        cls.__init__ = wrapped_init

    @property
    def storage_table(self) -> type[PlatformTimeIndexMetaData]:
        try:
            return self._storage_table
        except AttributeError as exc:
            raise AttributeError(
                "DataNode storage_table has not been initialized. "
                "Pass storage_table=... to DataNode.__init__."
            ) from exc

    @storage_table.setter
    def storage_table(self, value: type[PlatformTimeIndexMetaData]) -> None:
        if value is None:
            raise TypeError(
                "DataNode storage_table is required and must be a "
                "PlatformTimeIndexMetaData model class."
            )
        is_time_index_model_class = (
            isinstance(value, type)
            and issubclass(value, PlatformTimeIndexMetaData)
        )
        if not is_time_index_model_class:
            raise TypeError(
                "DataNode storage_table must be a PlatformTimeIndexMetaData model class; "
                f"got {type(value).__name__}."
            )
        if value.get_time_index_metadata() is None:
            raise ValueError(
                "DataNode storage_table must be registered or bound before construction."
            )
        if value.get_meta_table_uid() in (None, ""):
            raise ValueError("DataNode storage_table must provide a MetaTable UID.")
        if value.get_data_source_uid() in (None, ""):
            raise ValueError("DataNode storage_table must provide a data-source UID.")
        self._storage_table = value

    @property
    def storage_metadata(self) -> Any:
        storage_metadata = self.storage_table.get_time_index_metadata()
        if storage_metadata is None:
            raise ValueError("DataNode storage_table must be registered or bound before use.")
        return storage_metadata

    def _initialize_configuration(self, init_kwargs: dict) -> None:
        """Creates config from init args and sets them as instance attributes."""
        logger.debug(f"Creating configuration for {self.__class__.__name__}")

        init_kwargs["time_series_class_import_path"] = {
            "module": self.__class__.__module__,
            "qualname": self.__class__.__qualname__,
        }

        config = build_operations.create_config(
            kwargs=init_kwargs,
            ts_class_name=self.__class__.__name__,
        )

        for field_name, value in asdict(config).items():
            if field_name == "storage_hash":
                continue
            setattr(self, field_name, value)

    def _get_data_node_configuration(self) -> BaseConfiguration | None:
        config = getattr(self, "config", None)
        return config if isinstance(config, BaseConfiguration) else None

    @property
    def hash_namespace(self) -> str:
        """
        Return the active hash namespace for this node.

        An empty string means "no namespace", which keeps hashing identical to the
        normal production-style behavior. A non-empty value means this node was
        constructed in an isolated namespace and its hashes include that namespace.
        """
        return getattr(self, "_hash_namespace", "") or ""

    def get_offset_start(self) -> datetime.datetime:
        """
        Return the configured first-run fallback start date.
        """
        config = self._get_data_node_configuration()
        offset_start = getattr(config, "offset_start", None) if config is not None else None
        if offset_start is not None:
            return config.offset_start
        return self.OFFSET_START

    @property
    def is_api(self):
        return False

    @property
    def data_source_uid(self) -> str:
        data_source_uid = self.storage_table.get_data_source_uid()
        if data_source_uid in (None, ""):
            raise ValueError("DataNode data_source_uid requires storage_table data-source identity.")
        return str(data_source_uid)

    @property
    def data_node_update(self) -> DataNodeUpdate:
        """The local time series metadata object."""
        return self.local_persist_manager.data_node_update

    @property
    def local_persist_manager(self) -> PersistManager:
        if self._local_persist_manager is None:
            self.logger.debug(f"Setting local persist manager for {self.update_hash}")
            self._set_local_persist_manager(update_hash=self.update_hash)
        return self._local_persist_manager

    @property
    def data_source(self) -> Any:
        data_source = getattr(self.storage_metadata, "data_source", None)
        if data_source is not None:
            return data_source
        return self.local_persist_manager.data_source

    # --- Persistence & Backend Methods ---

    @tracer.start_as_current_span("TS: set_state_with_sessions")
    def _set_state_with_sessions(
        self,
        include_client_objects: bool = True,
        graph_depth_limit: int = 1000,
        graph_depth: int = 0,
    ) -> None:
        """
        Attaches runtime sessions and synchronizes backend metadata after rebuild.

        Args:
            include_client_objects: Whether to include nested client objects.
            graph_depth_limit: The depth limit for graph traversal.
            graph_depth: The current depth in the graph.
        """
        if graph_depth_limit == -1:
            graph_depth_limit = 1e6

        minimum_required_depth_for_update = self.get_minimum_required_depth_for_update()

        if graph_depth_limit < minimum_required_depth_for_update and graph_depth == 0:
            graph_depth_limit = minimum_required_depth_for_update
            self.logger.warning(
                f"Graph depth limit overwritten to {minimum_required_depth_for_update}"
            )

        storage_data_source = getattr(self.storage_metadata, "data_source", None)
        if isinstance(storage_data_source, dict):
            relation_type = storage_data_source.get("related_resource_class_type")
        else:
            relation_type = getattr(storage_data_source, "related_resource_class_type", None)

        if graph_depth <= graph_depth_limit and relation_type:
            self._set_local_persist_manager(
                update_hash=self.update_hash,
                data_node_update=None,
            )

        self.local_persist_manager.synchronize_data_node_update(data_node_update=None)

    def _set_local_persist_manager(
        self,
        update_hash: str,
        data_node_update: None | dict = None,
    ) -> None:
        """
        Initializes the local persistence manager for the time series. It sets up
        the necessary configurations and checks for existing metadata. If the metadata doesn't
        exist or is incomplete, it sets up the initial configuration and builds the update details.

        Args:
           update_hash : str
               The local hash ID for the time series.
           data_node_update : Union[None, dict], optional
               Local metadata for the time series, if available.
        """
        self._local_persist_manager = PersistManager.get_from_storage_table(
            storage_table=self.storage_table,
            update_hash=update_hash,
            class_name=self.__class__.__name__,
            data_node_update=data_node_update,
        )

    def verify_and_build_remote_objects(self) -> None:
        """
        Verifies and builds remote objects by calling the persistence layer.
        This logic is now correctly located within the BuildManager.
        """
        self.local_persist_manager.local_persist_exist_set_config(
            local_configuration=self.local_initial_configuration,
        )

    def set_relation_tree(self):
        """Sets the node relationships in the backend by calling the dependencies() method."""

        if self.local_persist_manager.data_node_update is None:
            self.verify_and_build_remote_objects()  #
        if self.local_persist_manager.is_local_relation_tree_set():
            return
        declared_dependencies = self.dependencies() or {}

        for name, dependency_ts in declared_dependencies.items():
            self.logger.debug(f"Connecting dependency '{name}'...")

            # Ensure the dependency itself is properly initialized
            is_api = dependency_ts.is_api
            if not is_api:
                dependency_ts.verify_and_build_remote_objects()

            self.local_persist_manager.depends_on_connect(dependency_ts, is_api=is_api)

            # Recursively set the relation tree for the dependency
            dependency_ts.set_relation_tree()

        self.local_persist_manager.set_ogm_dependencies_linked()

    def set_dependencies_df(self):
        depth_df = self.local_persist_manager.get_all_dependencies_update_priority()
        self.depth_df = depth_df
        if not depth_df.empty:
            if "update_node_uid" not in depth_df.columns:
                raise ValueError("Dependency dataframe must include 'update_node_uid'.")
            if self.data_node_update.uid is None:
                raise ValueError("DataNodeUpdate must have uid before filtering dependencies.")
            self.dependencies_df = depth_df[
                depth_df["update_node_uid"].astype(str) != str(self.data_node_update.uid)
            ].copy()


        else:
            self.dependencies_df = pd.DataFrame()

    def get_update_statistics(self):
        """
        This method always queries last state
        """
        return self.local_persist_manager.storage_metadata.get_data_updates()

    def prepare_update_statistics(self, update_statistics: UpdateStatistics) -> UpdateStatistics:
        """Hook for subclasses to scope or enrich update statistics before update()."""
        return update_statistics

    def _set_update_statistics(self, update_statistics: UpdateStatistics) -> UpdateStatistics:
        """Attach generic update statistics."""
        update_statistics = self.prepare_update_statistics(update_statistics)
        self.update_statistics = update_statistics
        return update_statistics

    def run(
        self,
        debug_mode: bool=True,
        *,
        update_tree: bool = True,
        force_update: bool = False,
        update_only_tree: bool = False,
        remote_scheduler: object | None = None,
        override_update_stats: BaseUpdateStatistics | None = None,
    ):
        """
        Run one update cycle for this node.

        By default, this also updates dependencies first, validates output, persists rows,
        and runs metadata/post-update hooks.

        Namespace behavior:
        if this node has a non-empty ``hash_namespace``, ``run()`` activates that
        namespace around the full run. That ensures dependencies instantiated inside
        ``dependencies()`` inherit the same namespace instead of accidentally writing
        into the non-namespaced tables.

        Parameters
        ----------
        debug_mode : bool, default=True
            Enables debug-friendly run behavior.
        update_tree : bool, default=True
            If ``True``, update dependencies before this node.
        force_update : bool, default=False
            If ``True``, run even when no new range is detected.
        update_only_tree : bool, default=False
            If ``True``, update dependencies only (skip this node update).
        remote_scheduler : object | None, optional
            Optional scheduler context.
        override_update_stats : BaseUpdateStatistics | None, optional
            Optional explicit update-state object (useful in tests or controlled runs).

        Returns
        -------
        Any
            Result returned by ``UpdateRunner.run()``.
        """

        debug_mode=True # Todo: onle enterpsie distributed has the distribured node update.

        def _do_run():
            update_runner = run_operations.UpdateRunner(
                time_serie=self,
                debug_mode=debug_mode,
                force_update=force_update,
                update_tree=update_tree,
                update_only_tree=update_only_tree,
                remote_scheduler=remote_scheduler,
                override_update_stats=override_update_stats,
            )
            return update_runner.run()

        # IMPORTANT:
        # If this node is namespaced, make that namespace active for the full run.
        # That ensures dependencies() calls also create namespaced DataNodes automatically.
        if self.hash_namespace:
            with _hash_namespace_cm(self.hash_namespace):
                return _do_run()

        return _do_run()

    # --- Optional Hooks for Customization ---
    def run_after_post_init_routines(self) -> None:
        pass

    def get_minimum_required_depth_for_update(self) -> int:
        """
        Controls the minimum depth that needs to be rebuilt.
        """
        return 0

    def run_post_update_routines(
        self,
        error_on_last_update: bool,
    ) -> None:
        """Should be overwritten by subclass"""
        pass

    def _resolve_latest_persisted_time_index(
        self,
        historical_update: Any,
    ) -> datetime.datetime | None:
        if hasattr(self, "latest_persisted_time_index"):
            return self.latest_persisted_time_index
        if hasattr(self, "overwrite_latest_value"):
            return self.overwrite_latest_value
        update_statistics = getattr(historical_update, "update_statistics", None)
        max_time_index_value = getattr(update_statistics, "max_time_index_value", None)
        if max_time_index_value is not None:
            return max_time_index_value

        legacy_last_time_index_value = getattr(historical_update, "last_time_index_value", None)
        if legacy_last_time_index_value is not None:
            # LEGACY_COMPAT: historical_update.last_time_index_value is a scalar
            # projection kept for older SDK surfaces. Runtime logic should prefer
            # update_statistics.max_time_index_value from global_index_progress.
            warnings.warn(
                "Deprecated TDAG compatibility path: "
                "historical_update.last_time_index_value was read. Use "
                "historical_update.update_statistics.max_time_index_value instead.",
                FutureWarning,
                stacklevel=2,
            )
        return legacy_last_time_index_value

    def _validate_update_output(self, temp_df: pd.DataFrame) -> None:
        storage_data_source = getattr(self.storage_metadata, "data_source", None)
        if storage_data_source is None:
            storage_data_source = self.local_persist_manager.data_source
        if isinstance(storage_data_source, dict):
            related_resource = storage_data_source.get("related_resource")
            if isinstance(related_resource, dict):
                class_type = related_resource.get("class_type")
            else:
                class_type = getattr(related_resource, "class_type", None)
        else:
            class_type = getattr(
                getattr(storage_data_source, "related_resource", None),
                "class_type",
                None,
            )

        run_operations.UpdateRunner.validate_data_frame(
            temp_df,
            class_type,
            meta_table=self.storage_metadata,
        )

    def _execute_local_update(
        self,
        historical_update: Any,
    ) -> LocalUpdateResult:
        self.logger.debug(f"Calculating update for {self}...")

        temp_df = self.update()
        latest_persisted_time_index = self._resolve_latest_persisted_time_index(historical_update)

        if temp_df is None:
            raise Exception(f" {self} update(...) method needs to return a data frame")

        if temp_df.empty:
            self.logger.warning(f"{self} produced no new data in this update round.")
            return temp_df

        if (
            latest_persisted_time_index is None
            and not SessionDataSource.is_local_db
        ):
            temp_df = self.update_statistics.filter_df_by_latest_value(temp_df)

        if temp_df.empty:
            self.logger.warning(f"No new data to persist for {self} after filtering.")
            return temp_df

        self._validate_update_output(temp_df)

        self.logger.info(f"Persisting {len(temp_df)} new rows for {self}.")
        self.local_persist_manager.persist_updated_data(
            temp_df=temp_df,
            overwrite=(latest_persisted_time_index is not None),
        )
        self.logger.info(f"Successfully updated {self}.")
        return temp_df



        

    @abstractmethod
    def dependencies(self) -> dict[str, Union["DataNode", "APIDataNode"]]:
        """
        Return direct upstream nodes required by this node.

        Keep keys short and descriptive (for example ``"prices"`` or ``"raw"``),
        and prefer creating dependency instances in ``__init__``.

        Returns:
            A dictionary where keys are descriptive names and values are the DataNode dependency instances.
        """
        raise NotImplementedError

    @abstractmethod
    def update(self) -> LocalUpdateResult:
        """
        Build and return the update payload for this run.

        Default ``DataNode`` execution expects a ``pd.DataFrame`` and will use
        ``self.update_statistics`` to compute an incremental window before
        persistence.

        Specialized subclasses that override ``_execute_local_update(...)``
        may return a different ``LocalUpdateResult`` shape instead.

        For the default DataFrame-based path, the expected output shape is:

        - index starts with ``time_index`` (UTC-aware datetimes),
        - column names are lowercase and schema-stable,
        - no duplicate index keys,
        - temporal payload columns are allowed when declared with an explicit
          dtype such as ``date`` or ``datetime64[ns, UTC]``.

        MultiIndex tables should use the configured time-first index vector.
        Any identity dimensions after ``time_index`` must match the table's
        source-table configuration.

        Returns
        -------
        LocalUpdateResult
            The payload produced for this run. For the default DataFrame-based
            path, return an empty ``pd.DataFrame`` when there is nothing new to
            persist. Returning ``None`` is invalid and will raise at runtime.
        """
        raise NotImplementedError


build_operations.serialize_argument.register(DataNode, build_operations._serialize_timeserie)
build_operations.serialize_argument.register(APIDataNode, build_operations._serialize_api_timeserie)
