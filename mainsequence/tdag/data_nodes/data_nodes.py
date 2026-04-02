import copy
import datetime
import inspect
import json
import logging
import os
import tempfile
from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import asdict
from functools import wraps
from typing import Any, Union

import cloudpickle
import numpy as np
import pandas as pd
import pytz
import structlog.contextvars as cvars

import mainsequence.client as ms_client
import mainsequence.tdag.data_nodes.build_operations as build_operations
import mainsequence.tdag.data_nodes.run_operations as run_operations
from mainsequence.client import (
    CONSTANTS,
    AssetTranslationTable,
    DataNodeUpdate,
    DynamicTableDataSource,
    Scheduler,
)
from mainsequence.client.models_tdag import (
    BaseUpdateStatistics,
    ColumnMetaData,
    DataSource,
    UniqueIdentifierRangeMap,
    UpdateStatistics,
)
from mainsequence.instrumentation import tracer
from mainsequence.logconf import logger
from mainsequence.tdag.base_persist_managers import get_data_node_source_code
from mainsequence.tdag.config import ogm
from mainsequence.tdag.data_nodes.persist_managers import APIPersistManager, PersistManager

from ..configuration_models import BaseConfiguration
from .models import DataNodeConfiguration
from .namespacing import current_hash_namespace
from .namespacing import hash_namespace as _hash_namespace_cm


def get_data_source_from_orm() -> Any:
    from mainsequence.client import SessionDataSource

    if SessionDataSource.data_source.related_resource is None:
        raise Exception("This Pod does not have a default data source")
    return SessionDataSource.data_source


LocalUpdateResult = None | pd.DataFrame | Sequence[Any]


def get_latest_update_by_assets_filter(
    asset_symbols: list | None, last_update_per_asset: dict
) -> datetime.datetime:
    """
    Gets the latest update timestamp for a list of asset symbols.

    Args:
        asset_symbols: A list of asset symbols.
        last_update_per_asset: A dictionary mapping assets to their last update time.

    Returns:
        The latest update timestamp.
    """
    if asset_symbols is not None:
        last_update_in_table = np.max(
            [
                timestamp
                for unique_identifier, timestamp in last_update_per_asset.items()
                if unique_identifier in asset_symbols
            ]
        )
    else:
        last_update_in_table = np.max(last_update_per_asset.values)
    return last_update_in_table


def last_update_per_unique_identifier(
    unique_identifier_list: list | None, last_update_per_asset: dict
) -> datetime.datetime:
    """
    Gets the earliest last update time for a list of unique identifiers.

    Args:
        unique_identifier_list: A list of unique identifiers.
        last_update_per_asset: A dictionary mapping assets to their last update times.

    Returns:
        The earliest last update timestamp.
    """
    if unique_identifier_list is not None:
        last_update_in_table = min(
            [
                t
                for a in last_update_per_asset.values()
                for t in a.values()
                if a in unique_identifier_list
            ]
        )
    else:
        last_update_in_table = min([t for a in last_update_per_asset.values() for t in a.values()])
    return last_update_in_table


class DependencyUpdateError(Exception):
    pass


class DataAccessMixin:
    """A mixin for classes that provide access to time series data."""

    def __repr__(self) -> str:
        try:
            local_id = self.data_node_update.id
        except:
            local_id = 0
        repr = (
            self.__class__.__name__
            + f" {os.environ['TDAG_ENDPOINT']}/local-time-series/details/?local_time_serie_id={local_id}"
        )
        return repr

    def get_last_observation(self, asset_list: list[ms_client.AssetMixin] | None=None):
        # update_statistics = self.get_update_statistics()
        # if asset_list is not None:
        #     update_statistics = update_statistics.update_assets(asset_list=asset_list)
        # update_range_map = update_statistics.get_update_range_map_great_or_equal()
        # last_observation = self.get_ranged_data_per_asset(update_range_map)
        # return last_observation
        return self.local_persist_manager.get_last_observation(
            asset_list=asset_list,
        )


    def get_pickle_path_from_time_serie(self) -> str:
        path = build_operations.get_pickle_path(
            update_hash=self.update_hash, data_source_id=self.data_source_id, is_api=self.is_api
        )
        return path

    def persist_to_pickle(self, overwrite: bool = False) -> tuple[str, str]:
        """
        Persists the DataNode object to a pickle file using an atomic write.

        Uses a single method to determine the pickle path and dispatches to
        type-specific logic only where necessary.

        Args:
            overwrite: If True, overwrites any existing pickle file.

        Returns:
            A tuple containing the full path and the relative path of the pickle file.
        """
        # 1. Common Logic: Determine the pickle path for both types
        path = self.get_pickle_path_from_time_serie()

        # 2. Type-Specific Logic: Run pre-dump actions only for standard DataNode
        if not self.is_api:
            self.logger.debug(f"Patching source code and git hash for {self.storage_hash}")
            self.local_persist_manager.update_git_and_code_in_backend(
                time_serie_class=self.__class__
            )
            # Prepare for pickling by removing the unpicklable ThreadLock
            self._local_persist_manager = None

        # 3. Common Logic: Persist the data source if needed
        data_source_id = getattr(self.data_source, "id", self.data_source_id)
        data_source_path = build_operations.data_source_pickle_path(data_source_id)
        if not os.path.isfile(data_source_path) or overwrite:
            self.data_source.persist_to_pickle(data_source_path)

        # 4. Common Logic: Atomically write the main pickle file
        if os.path.isfile(path) and not overwrite:
            self.logger.debug(f"Pickle file already exists at {path}. Skipping.")
        else:
            if overwrite:
                self.logger.warning(f"Overwriting pickle file at {path}")
            self._atomic_pickle_dump(path)

        # 5. Common Logic: Return the full and relative paths
        return path, path.replace(ogm.pickle_storage_path + "/", "")

    def _atomic_pickle_dump(self, path: str) -> None:
        """
        Private helper to atomically dump the object to a pickle file.
        This prevents file corruption if the process is interrupted.
        """
        dir_, fname = os.path.split(path)
        # Ensure the target directory exists
        os.makedirs(dir_, exist_ok=True)

        fd, tmp_path = tempfile.mkstemp(prefix=f"{fname}~", dir=dir_)
        os.close(fd)
        try:
            with open(tmp_path, "wb") as handle:
                cloudpickle.dump(self, handle)
            # Atomic replace is safer than a direct write
            os.replace(tmp_path, path)
            self.logger.debug(f"Successfully persisted pickle to {path}")
        except Exception:
            # Clean up the temporary file on error to avoid clutter
            try:
                os.remove(tmp_path)
            except OSError:
                pass
            raise

    def get_logger_context_variables(self) -> dict[str, Any]:
        return dict(
            update_hash=self.update_hash,
            local_hash_id_data_source=self.data_source_id,
            api_time_series=self.__class__.__name__ == "APIDataNode",
        )

    @property
    def logger(self) -> logging.Logger:
        """Gets a logger instance with bound context variables."""
        # import structlog.contextvars as cvars
        # cvars.bind_contextvars(update_hash=self.update_hash,
        #                      update_hash=self.data_source_id,
        #                      api_time_series=True,)
        global logger
        if hasattr(self, "_logger") == False:
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
            logger.bind(**dict(key=value))

    def unbind_context_variables_from_logger(self) -> None:
        cvars.unbind_contextvars(*self.get_logger_context_variables().keys())

    def get_df_between_dates(
        self,
        start_date: datetime.datetime | None = None,
        end_date: datetime.datetime | None = None,
        unique_identifier_list: list | None = None,
        great_or_equal: bool = True,
        less_or_equal: bool = True,
        unique_identifier_range_map: UniqueIdentifierRangeMap | None = None,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        """
        Retrieve rows from this DataNode whose `time_index` (and optional `unique_identifier`) fall within the specified date ranges.

        **Note:** If `unique_identifier_range_map` is provided, **all** other filters
        (`start_date`, `end_date`, `unique_identifier_list`, `great_or_equal`, `less_or_equal`)
        are ignored, and only the per-identifier ranges in `unique_identifier_range_map` apply.

        Filtering logic (when `unique_identifier_range_map` is None):
          - If `start_date` is provided, include rows where
            `time_index > start_date` (if `great_or_equal=False`)
            or `time_index >= start_date` (if `great_or_equal=True`).
          - If `end_date` is provided, include rows where
            `time_index < end_date` (if `less_or_equal=False`)
            or `time_index <= end_date` (if `less_or_equal=True`).
          - If `unique_identifier_list` is provided, only include rows whose
            `unique_identifier` is in that list.

        Filtering logic (when `unique_identifier_range_map` is provided):
          - For each `unique_identifier`, apply its own `start_date`/`end_date`
            filters using the specified operands (`">"`, `">="`, `"<"`, `"<="`):
            {
              <uid>: {
                "start_date": datetime,
                "start_date_operand": ">=" or ">",
                "end_date": datetime,
                "end_date_operand": "<=" or "<"
              },
              ...
            }

        Parameters
        ----------
        start_date : datetime.datetime or None
            Global lower bound for `time_index`. Ignored if `unique_identifier_range_map` is provided.
        end_date : datetime.datetime or None
            Global upper bound for `time_index`. Ignored if `unique_identifier_range_map` is provided.
        unique_identifier_list : list or None
            If provided, only include rows matching these IDs. Ignored if `unique_identifier_range_map` is provided.
        great_or_equal : bool, default True
            If True, use `>=` when filtering by `start_date`; otherwise use `>`. Ignored if `unique_identifier_range_map` is provided.
        less_or_equal : bool, default True
            If True, use `<=` when filtering by `end_date`; otherwise use `<`. Ignored if `unique_identifier_range_map` is provided.
        unique_identifier_range_map : UniqueIdentifierRangeMap or None
            Mapping of specific `unique_identifier` keys to their own sub-filters. When provided, this is the sole filter applied.

        Returns
        -------
        pd.DataFrame
            A DataFrame containing rows that satisfy the combined time and identifier filters.
        """
        return self.local_persist_manager.get_df_between_dates(
            start_date=start_date,
            end_date=end_date,
            unique_identifier_list=unique_identifier_list,
            great_or_equal=great_or_equal,
            less_or_equal=less_or_equal,
            unique_identifier_range_map=unique_identifier_range_map,
            columns=columns,
        )

    def get_ranged_data_per_asset(
        self,
        range_descriptor: UniqueIdentifierRangeMap | None,
        columns=None,
    ) -> pd.DataFrame:
        """
        Gets data based on a range descriptor.

        Args:
            range_descriptor: A UniqueIdentifierRangeMap object.

        Returns:
            A DataFrame with the ranged data.
        """
        return self.get_df_between_dates(
            unique_identifier_range_map=range_descriptor,
            columns=columns,
        )

    def get_ranged_data_per_asset_great_or_equal(
        self,
        range_descriptor: UniqueIdentifierRangeMap | None,
        columns=None,
    ) -> pd.DataFrame:
        """
        Gets data based on a range descriptor.

        Args:
            range_descriptor: A UniqueIdentifierRangeMap object.

        Returns:
            A DataFrame with the ranged data.
        """

        for k, v in range_descriptor.items():
            v["start_date_operand"] = "=>"
        return self.get_df_between_dates(
            unique_identifier_range_map=range_descriptor,
            columns=columns,
        )

    def filter_by_assets_ranges(self, asset_ranges_map: dict) -> pd.DataFrame:
        """
        Filters data by asset ranges.

        Args:
            asset_ranges_map: A dictionary mapping assets to their date ranges.

        Returns:
            A DataFrame with the filtered data.
        """
        return self.local_persist_manager.filter_by_assets_ranges(asset_ranges_map)


class APIDataNode(DataAccessMixin):

    @classmethod
    def build_from_local_time_serie(cls, source_table: "DataNodeUpdate") -> "APIDataNode":
        return cls(
            data_source_id=source_table.data_source.id, storage_hash=source_table.storage_hash
        )

    @classmethod
    def build_from_table_id(cls, table_id: str) -> "APIDataNode":
        table = ms_client.DataNodeStorage.get(id=table_id)
        ts = cls(data_source_id=table.data_source.id, storage_hash=table.storage_hash)
        return ts


    @classmethod
    def build_from_identifier(cls, identifier: str) -> "APIDataNode":

        table = ms_client.DataNodeStorage.get(identifier=identifier)
        ts = cls(data_source_id=table.data_source.id, storage_hash=table.storage_hash)
        return ts

    def __init__(
        self,
        data_source_id: int,
        storage_hash: str,
        data_source_local_lake: DataSource | None = None,
    ):
        """
        Initializes an APIDataNode.

        Args:
            data_source_id: The ID of the data source.
            update_hash: The local hash ID of the time series.
            data_source_local_lake: Optional local data source for the lake.
        """
        if data_source_local_lake is not None:
            assert (
                data_source_local_lake.data_type in CONSTANTS.DATA_SOURCE_TYPE_LOCAL_DISK_LAKE
            ), "data_source_local_lake should be of type CONSTANTS.DATA_SOURCE_TYPE_LOCAL_DISK_LAKE"

        assert isinstance(data_source_id, int)
        self.data_source_id = data_source_id
        self.storage_hash = storage_hash
        self.data_source = data_source_local_lake
        self._local_persist_manager: APIPersistManager = None
        self.update_statistics = None

    def __repr__(self) -> str:


        try:

            repr = (
                self.__class__.__name__
                + f" {os.environ['TDAG_ENDPOINT']}/dynamic-table-metadatas/details/?dynamic_table_id={self._local_persist_manager.data_node_storage.id}"
            )
        except  Exception:
            logger.exception("DataNode has not resolved yet an storage node")
        return repr

    @property
    def is_api(self):
        return True

    @staticmethod
    def _get_update_hash(storage_hash):
        return "API_" + f"{storage_hash}"

    @property
    def update_hash(self):
        return self._get_update_hash(storage_hash=self.storage_hash)

    def __getstate__(self) -> dict[str, Any]:
        """Prepares the state for pickling."""
        state = self.__dict__.copy()
        # Remove unpicklable/transient state specific to APIDataNode
        names_to_remove = [
            "_local_persist_manager",  # APIPersistManager instance
        ]
        cleaned_state = {k: v for k, v in state.items() if k not in names_to_remove}
        return cleaned_state

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
        if pod_source != None:
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
        ModelClass = DynamicTableDataSource.get_class(data_config["data_type"])
        pod_source = ModelClass.get(data_config["id"])
        return pod_source

    def _set_local_persist_manager(self) -> None:
        self._verify_local_data_source()
        self._local_persist_manager = APIPersistManager(
            storage_hash=self.storage_hash, data_source_id=self.data_source_id
        )
        data_node_storage = self._local_persist_manager.data_node_storage

        assert data_node_storage is not None, f"Verify that the table {self.storage_hash} exists "

    def get_update_statistics(
        self, asset_symbols: list | None = None
    ) -> tuple[datetime.datetime | None, dict[str, datetime.datetime] | None]:
        """
        Gets update statistics from the database.

        Args:
            asset_symbols: An optional list of asset symbols to filter by.

        Returns:
            A tuple containing the last update time for the table and a dictionary of last update times per asset.
        """

        return (
            self.local_persist_manager.data_node_storage.sourcetableconfiguration.get_data_updates()
        )

    def get_earliest_updated_asset_filter(
        self, unique_identifier_list: list, last_update_per_asset: dict
    ) -> datetime.datetime:
        """
        Gets the earliest last update time for a list of unique identifiers.

        Args:
            unique_identifier_list: A list of unique identifiers.
            last_update_per_asset: A dictionary mapping assets to their last update times.

        Returns:
            The earliest last update timestamp.
        """
        if unique_identifier_list is not None:
            last_update_in_table = min(
                [
                    t
                    for a in last_update_per_asset.values()
                    for t in a.values()
                    if a in unique_identifier_list
                ]
            )
        else:
            last_update_in_table = min(
                [t for a in last_update_per_asset.values() for t in a.values()]
            )
        return last_update_in_table

    def update(self, *args, **kwargs) -> pd.DataFrame:
        self.logger.info("Not updating series")
        pass


class DataNode(DataAccessMixin, ABC):
    """
    Base class for building and maintaining datasets in Main Sequence.

    A ``DataNode`` is both:

    - the recipe to produce data (config + dependencies + update logic), and
    - the data product contract used by downstream users (identifier + schema + metadata).

    Two identities matter:

    - ``storage_hash``: identifies the table/dataset contract.
    - ``update_hash``: identifies the updater job writing to that table.

    This separation lets you run different updater jobs (for example, asset shards)
    while writing into the same table safely.

    Hash namespace
    --------------
    ``DataNode`` also supports ``hash_namespace`` for test and experiment isolation.

    Resolution order is:

    1. explicit ``hash_namespace="..."`` passed at construction,
    2. ``test_node=True`` which becomes the namespace ``"test"``,
    3. the active ``with hash_namespace("...")`` context manager,
    4. otherwise, no namespace.

    A non-empty namespace is injected into the build configuration, which changes
    both ``storage_hash`` and ``update_hash``. An empty namespace changes nothing.

    During ``run()``, the active namespace is re-applied around the full run so
    dependencies created inside ``dependencies()`` inherit the same isolation.

    Subclass checklist:

    - Keep constructor args stable and serializable (Pydantic config is recommended).
    - Build dependencies in ``__init__`` and return them in ``dependencies()``.
    - Use ``self.update_statistics`` in ``update()`` and return only incremental rows.
    - Provide table/column metadata for production datasets.
    """

    OFFSET_START = datetime.datetime(2018, 1, 1, tzinfo=pytz.utc)
    OPEN_TO_PUBLIC=False # flag for enterprise data providers that want to open their data nmodes
    DATA_NODE_UPDATE_CLASS = ms_client.DataNodeUpdate

    # --- Dunder & Serialization Methods ---

    def __setstate__(self, state: dict[str, Any]) -> None:
        # Restore instance attributes (i.e., filename and lineno).
        self.__dict__.update(state)

    def __getstate__(self) -> dict[str, Any]:
        # Copy the object's state from self.__dict__ which contains
        # all our instance attributes. Always use the dict.copy()
        # method to avoid modifying the original state.
        state = self._prepare_state_for_pickle(state=self.__dict__)

        # Remove the unpicklable entries.
        return state

    def __init__(
        self,
        config: BaseConfiguration,
        *,
        hash_namespace: str | None = None,
        test_node: bool = False,
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
        hash_namespace : str | None
            Optional hash isolation namespace.
        test_node : bool
            Convenience flag for the ``"test"`` namespace.
        """
        if not isinstance(config, BaseConfiguration):
            raise TypeError(
                f"{self.__class__.__name__} expected config to be a BaseConfiguration; "
                f"got {type(config).__name__}."
            )

        self.pre_load_routines_run = False
        self._data_source: DynamicTableDataSource | None = None  # is set later
        self._local_persist_manager: PersistManager | None = None

        self._scheduler_tree_connected = False
        self.update_statistics = None
        self.config = config
        self._framework_initialized = True

        explicit_namespace = hash_namespace
        if explicit_namespace is None and test_node:
            explicit_namespace = "test"
        if explicit_namespace is not None:
            self._hash_namespace = (explicit_namespace or "").strip()

    def __init_subclass__(cls, **kwargs):
        """
        Wrap subclass construction so DataNode can capture config, compute hashes,
        and apply namespace/test controls consistently.

        The wrapper consumes two special kwargs before the subclass ``__init__`` runs:

        - ``hash_namespace="..."``: explicit hash isolation namespace
        - ``test_node=True``: shortcut for ``hash_namespace="test"``

        Namespace precedence is:

        1. explicit ``hash_namespace``
        2. ``test_node=True``
        3. active namespacing context manager

        Only a non-empty namespace is injected into the hashed build configuration.
        That preserves backward compatibility for normal runs while allowing isolated
        test tables when needed.
        """
        super().__init_subclass__(**kwargs)

        if "_ARGS_IGNORE_IN_STORAGE_HASH" in cls.__dict__:
            raise TypeError(
                f"{cls.__name__} uses removed class attribute _ARGS_IGNORE_IN_STORAGE_HASH; "
                "move those fields into DataNodeConfiguration and mark them with "
                'json_schema_extra={"update_only": True}.'
            )

        # Get the original __init__ from the new subclass
        original_init = cls.__init__

        @wraps(original_init)
        def wrapped_init(self, *args, **kwargs):
            # ---- tests-only hashing controls (never forwarded to user __init__) ----
            test_node_flag = bool(kwargs.pop("test_node", False))
            explicit_namespace = kwargs.pop("hash_namespace", None)
            if "init_meta" in kwargs:
                raise TypeError(
                    "init_meta has been removed from DataNode construction; remove this keyword argument."
                )

            # Determine namespace:
            # 1) explicit hash_namespace kwarg wins
            # 2) test_node=True => "test"
            # 3) else: context manager namespace (tests)
            if explicit_namespace is not None:
                namespace = explicit_namespace
            elif test_node_flag:
                namespace = "test"
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
                    "hash_namespace=..., test_node=...) from its constructor."
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
            if final_kwargs.get("config") is None:
                final_kwargs.pop("config", None)
            if not final_kwargs:
                final_kwargs["config"] = self.config

            # ---- the surgical part: only change hashes when namespace is non-empty ----
            # Backward compatibility guarantee:
            # - if no test_node + no context => namespace == "" => NOTHING added => hashes identical to old behavior
            self._hash_namespace = namespace
            if self._hash_namespace:
                final_kwargs["hash_namespace"] = self._hash_namespace
                logger.debug(f"Running on namespace {self._hash_namespace}")
            # 3. Run the post-initialization routines
            self.build_configuration = final_kwargs
            logger.debug(f"Running post-init routines for {self.__class__.__name__}")
            self._initialize_configuration(init_kwargs=final_kwargs)

            # 7. Final setup
            self.set_data_source()
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
        # Works for old pickles too (attribute may not exist)
        return getattr(self, "_hash_namespace", "") or ""

    @property
    def test_node(self) -> bool:
        """
        Return ``True`` when this node is running with any non-empty hash namespace.

        In current behavior, ``test_node`` is just a convenience view over
        ``bool(self.hash_namespace)``.
        """
        # “test node” = any non-empty namespace
        return bool(self.hash_namespace)

    def get_offset_start(self) -> datetime.datetime:
        """
        Hook to allow test nodes to change OFFSET_START without forking update logic.
        Backward compatible: prod returns OFFSET_START exactly as before.
        """
        if self.test_node and hasattr(self, "TEST_OFFSET_START"):
            return self.TEST_OFFSET_START
        config = self._get_data_node_configuration()
        offset_start = getattr(config, "offset_start", None) if config is not None else None
        if offset_start is not None:
            return config.offset_start
        return self.OFFSET_START

    def get_open_to_public(self) -> bool:
        """
        Return the publication flag for the node.

        Resolution order:
        1. explicit ``DataNodeConfiguration.open_to_public`` when set on a config object
        2. legacy class attribute ``OPEN_TO_PUBLIC``
        """
        config = self._get_data_node_configuration()
        if config is not None and "open_to_public" in getattr(config, "model_fields_set", set()):
            return config.open_to_public
        return self.OPEN_TO_PUBLIC

    @property
    def is_api(self):
        return False

    @property
    def data_source_id(self) -> int:
        return self.data_source.id

    @property
    def data_node_update(self) -> DataNodeUpdate:
        """The local time series metadata object."""
        return self.local_persist_manager.data_node_update

    @property
    def data_node_storage(self) -> "DataNodeStorage":
        return self.local_persist_manager.data_node_storage

    @property
    def local_persist_manager(self) -> PersistManager:
        if self._local_persist_manager is None:
            self.logger.debug(f"Setting local persist manager for {self.storage_hash}")
            self._set_local_persist_manager(update_hash=self.update_hash)
        return self._local_persist_manager

    @property
    def data_source(self) -> Any:
        if self._data_source is not None:
            return self._data_source
        else:
            raise Exception("Data source has not been set")

    # --- Persistence & Backend Methods ---

    @tracer.start_as_current_span("TS: set_state_with_sessions")
    def _set_state_with_sessions(
        self,
        include_vam_client_objects: bool = True,
        graph_depth_limit: int = 1000,
        graph_depth: int = 0,
    ) -> None:
        """
        Sets the state of the DataNode after loading from pickle, including sessions.

        Args:
            include_vam_client_objects: Whether to include VAM client objects.
            graph_depth_limit: The depth limit for graph traversal.
            graph_depth: The current depth in the graph.
        """
        if graph_depth_limit == -1:
            graph_depth_limit = 1e6

        minimum_required_depth_for_update = self.get_minimum_required_depth_for_update()

        state = self.__dict__

        if graph_depth_limit < minimum_required_depth_for_update and graph_depth == 0:
            graph_depth_limit = minimum_required_depth_for_update
            self.logger.warning(
                f"Graph depth limit overwritten to {minimum_required_depth_for_update}"
            )

        # if the data source is not local then the de-serialization needs to happend after setting the local persist manager
        # to guranteed a proper patch in the back-end
        if graph_depth <= graph_depth_limit and self.data_source.related_resource_class_type:
            self._set_local_persist_manager(
                update_hash=self.update_hash,
                data_node_update=None,
            )

        deserializer = build_operations.DeserializerManager()
        state = deserializer.deserialize_pickle_state(
            state=state,
            data_source_id=self.data_source.id,
            include_vam_client_objects=include_vam_client_objects,
            graph_depth_limit=graph_depth_limit,
            graph_depth=graph_depth + 1,
        )

        self.__dict__.update(state)

        self.local_persist_manager.synchronize_data_node_update(data_node_update=None)

    def _prepare_state_for_pickle(self, state: dict[str, Any]) -> dict[str, Any]:
        """
        Prepares the object's state for pickling by serializing and removing unpicklable entries.

        Args:
            state: The object's __dict__.

        Returns:
            A pickle-safe dictionary representing the object's state.
        """
        properties = state
        serializer = build_operations.Serializer()
        properties = serializer.serialize_for_pickle(properties)
        names_to_remove = []
        for name, attr in properties.items():
            if name in [
                "local_persist_manager",
                "logger",
                "_data_node_update_future",
                "_data_node_update_lock",
                "_local_persist_manager",
                "update_tracker",
            ]:
                names_to_remove.append(name)
                continue

            try:
                cloudpickle.dumps(attr)
            except Exception as e:
                logger.exception(f"Cant Pickle property {name}")
                raise e

        for n in names_to_remove:
            properties.pop(n, None)

        return properties

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
           storage_hash : str
               The remote table hash name for the time series.
           data_node_update : Union[None, dict], optional
               Local metadata for the time series, if available.
        """
        self._local_persist_manager = PersistManager.get_from_data_type(
            update_hash=update_hash,
            class_name=self.__class__.__name__,
            data_node_update=data_node_update,
            data_source=self.data_source,
        )

    def set_data_source(self, data_source: object | None = None) -> None:
        """
        Sets the data source for the time series.

        Args:
            data_source: The data source object. If None, the default is fetched from the ORM.
        """
        if data_source is None:
            self._data_source = get_data_source_from_orm()
        else:
            self._data_source = data_source

    def verify_and_build_remote_objects(self) -> None:
        """
        Verifies and builds remote objects by calling the persistence layer.
        This logic is now correctly located within the BuildManager.
        """
        # Use self.owner to get properties from the DataNode instance
        owner_class = self.__class__
        time_serie_source_code_git_hash = build_operations.get_data_node_source_code_git_hash(
            owner_class
        )
        time_serie_source_code = get_data_node_source_code(owner_class)

        # The call to the low-level persist manager is encapsulated here
        self.local_persist_manager.local_persist_exist_set_config(
            storage_hash=self.storage_hash,
            local_configuration=self.local_initial_configuration,
            remote_configuration=self.remote_initial_configuration,
            time_serie_source_code_git_hash=time_serie_source_code_git_hash,
            time_serie_source_code=time_serie_source_code,
            data_source=self.data_source,
            build_configuration_json_schema=self.build_configuration_json_schema,
            open_to_public=self.get_open_to_public(),
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
            if is_api == False:
                dependency_ts.verify_and_build_remote_objects()

            self.local_persist_manager.depends_on_connect(dependency_ts, is_api=is_api)

            # Recursively set the relation tree for the dependency
            dependency_ts.set_relation_tree()

        self.local_persist_manager.set_ogm_dependencies_linked()

    def set_dependencies_df(self):
        depth_df = self.local_persist_manager.get_all_dependencies_update_priority()
        self.depth_df = depth_df
        if not depth_df.empty:
            self.dependencies_df = depth_df[
                depth_df["update_node_id"] != self.data_node_update.id
            ].copy()


        else:
            self.dependencies_df = pd.DataFrame()

    def get_update_statistics(self):
        """
        This method always queries last state
        """
        return self.data_node_storage.sourcetableconfiguration.get_data_updates()

    def _set_update_statistics(self, update_statistics: UpdateStatistics) -> UpdateStatistics:
        """
         UpdateStatistics provides the last-ingested positions:
          - For a single-index series (time_index only), `update_statistics.max_time` is either:
              - None: no prior data—fetch all available rows.
              - a datetime: fetch rows where `time_index > max_time`.
          - For a dual-index series (time_index, unique_identifier), `update_statistics.max_time_per_id` is either:
              - None: single-index behavior applies.
              - dict[str, datetime]: for each `unique_identifier` (matching `Asset.unique_identifier`), fetch rows where
                `time_index > max_time_per_id[unique_identifier]`.

        Default method to narrow down update statistics un local time series,
        the method will filter using asset_list if the attribute exists as well as the init fallback date
        :param update_statistics:

        :return:
        """
        # Filter update_statistics to include only assets in self.asset_list.

        asset_list = self.get_asset_list()
        self._setted_asset_list = asset_list

        update_statistics = update_statistics.update_assets(
            asset_list, init_fallback_date=self.get_offset_start()
        )

        self.update_statistics = update_statistics

    # --- Public API ---

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

    def get_table_metadata(
        self,
    ) -> ms_client.TableMetaData | None:
        """
        Return metadata that describes the table as a dataset.

        Base behavior:
        - if the node instance carries a ``DataNodeConfiguration`` with
          ``node_metadata``, build ``ms_client.TableMetaData`` from it.
        - otherwise return ``None``.

        Subclasses can still override this for custom behavior.

        Returns
        -------
        ms_client.TableMetaData | None
            Table metadata, or ``None`` when not provided.
        """
        config = self._get_data_node_configuration()
        node_metadata = getattr(config, "node_metadata", None) if config is not None else None
        if node_metadata is None:
            return None

        return ms_client.TableMetaData(
            identifier=node_metadata.identifier,
            description=node_metadata.description,
            data_frequency_id=node_metadata.data_frequency_id,
        )

    def get_column_metadata(self) -> list[ColumnMetaData] | None:
        """
        Return metadata for output columns.

        Base behavior:
        - if the node instance carries a ``DataNodeConfiguration`` with
          ``records``, build ``ColumnMetaData`` from those definitions.
        - otherwise return ``None``.

        Subclasses can still override this for custom behavior.

        Returns:
            A list of ColumnMetaData objects, or None.
        """
        config = self._get_data_node_configuration()
        records = getattr(config, "records", None) if config is not None else None
        if not records:
            return None

        return [
            ColumnMetaData(
                column_name=record.column_name,
                dtype=record.dtype,
                label=record.label or record.column_name,
                description=record.description or "",
            )
            for record in records
        ]

    def get_asset_list(self) -> list["Asset"] | None:
        """
        Return the assets this updater should consider.

        For ``(time_index, unique_identifier)`` tables, this is usually the best place
        to resolve/register assets idempotently before running updates.

        Default behavior:
        - returns ``self.asset_list`` when that attribute exists,
        - otherwise returns ``None`` (no explicit asset filtering).

        Returns
        -------
        list["Asset"] | None
            Asset list used by ``UpdateStatistics.update_assets(...)`` or ``None``.
        """
        if hasattr(self, "asset_list"):
            return self.asset_list

        return None

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
        return getattr(historical_update, "last_time_index_value", None)

    def _validate_update_output(self, temp_df: pd.DataFrame) -> None:
        run_operations.UpdateRunner.validate_data_frame(
            temp_df,
            self.data_source.related_resource.class_type,
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
            and ms_client.SessionDataSource.is_local_duck_db == False
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
        - no datetime payload columns (time should live in the index).

        For asset MultiIndex tables, the second index level should be
        ``unique_identifier`` and should map to platform assets.

        Returns
        -------
        LocalUpdateResult
            The payload produced for this run. Return ``None`` or an empty
            result when there is nothing new to persist.
        """
        raise NotImplementedError


class WrapperDataNodeConfig(DataNodeConfiguration):
    translation_table: AssetTranslationTable


class WrapperDataNode(DataNode):
    """
    Composite/virtual time series that routes per-asset queries to different underlying MarketTimeSeries.

    Core idea
    ---------
    WrapperTimeSeries uses an AssetTranslationTable as a routing configuration.

    For each requested source asset unique_identifier:
      1) Evaluate AssetTranslationTable.evaluate_asset(asset) to obtain:
         - markets_time_serie_unique_identifier: which backend MarketsTimeSeries to query
         - exchange_code (optional): which share-class listing to target
         - default_column_name_from_rule: preferred value column (e.g. close/vwap)
      2) Group assets by (markets_time_serie_unique_identifier, exchange_code) to minimize backend calls.
      3) Translate source assets -> target listing assets via asset_ticker_group_id (+ exchange_code constraint).
      4) Query the corresponding APIDataNode using target unique_identifiers.
      5) Rename results back into the source unique_identifier namespace.

    Determinism requirements
    ------------------------
    AssetTranslationTable must match exactly one rule per asset.
    - 0 matches => TranslationError
    - >1 matches => TranslationError

    Share-class translation requirements
    ------------------------------------
    If a ticker group has multiple listings, exchange_code must be specified in the matching rule,
    otherwise target listing resolution becomes ambiguous and the wrapper raises.

    Mutability / snapshot behavior
    ------------------------------
    WrapperTimeSeries deep-copies the translation table in __init__ to prevent accidental external mutation.
    """

    def __init__(
        self,
        config: WrapperDataNodeConfig,
        *,
        hash_namespace: str | None = None,
        test_node: bool = False,
    ):
        """
        Initialize the WrapperDataNode.

        Args:
            config: Wrapper configuration containing the asset translation table.
        """
        super().__init__(
            config=config,
            hash_namespace=hash_namespace,
            test_node=test_node,
        )

        def get_time_serie_from_markets_unique_id(table_identifier: str) -> DataNode:
            """
            Returns the appropriate bar time series based on the asset list and source.
            """
            from mainsequence.client import DoesNotExist

            try:
                metadata = ms_client.DataNodeStorage.get(identifier=table_identifier)

            except DoesNotExist as e:
                raise e
            api_ts = APIDataNode(
                data_source_id=metadata.data_source.id, storage_hash=metadata.storage_hash
            )
            return api_ts

        translation_table = copy.deepcopy(config.translation_table)

        self.api_ts_map = {}
        for rule in translation_table.rules:
            if rule.markets_time_serie_unique_identifier not in self.api_ts_map:
                self.api_ts_map[rule.markets_time_serie_unique_identifier] = (
                    get_time_serie_from_markets_unique_id(
                        table_identifier=rule.markets_time_serie_unique_identifier
                    )
                )

        self.translation_table = translation_table

    def dependencies(self) -> dict[str, Union["DataNode", "APIDataNode"]]:
        return self.api_ts_map

    def get_ranged_data_per_asset(
        self, range_descriptor: UniqueIdentifierRangeMap | None
    ) -> pd.DataFrame:
        """
        Gets data based on a range descriptor.

        Args:
            range_descriptor: A UniqueIdentifierRangeMap object.

        Returns:
            A DataFrame with the ranged data.
        """
        return self.get_df_between_dates(unique_identifier_range_map=range_descriptor)

    def get_df_between_dates(
        self,
        start_date: datetime.datetime | None = None,
        end_date: datetime.datetime | None = None,
        unique_identifier_list: list | None = None,
        great_or_equal: bool = True,
        less_or_equal: bool = True,
        unique_identifier_range_map: UniqueIdentifierRangeMap | None = None,
    ) -> pd.DataFrame:
        """
        Retrieves a DataFrame of time series data between specified dates, handling asset translation.

        Args:
            start_date: The start date of the data range.
            end_date: The end date of the data range.
            unique_identifier_list: An optional list of unique identifiers to filter by.
            great_or_equal: Whether to include the start date.
            less_or_equal: Whether to include the end date.
            unique_identifier_range_map: An optional map of ranges for unique identifiers.

        Returns:
            A pandas DataFrame with the requested data.
        """
        if (unique_identifier_list is None) == (unique_identifier_range_map is None):
            raise ValueError(
                "Pass **either** unique_identifier_list **or** unique_identifier_range_map, but not both."
            )

        if unique_identifier_list is not None:
            wanted_src_uids = set(unique_identifier_list)
        else:  # range‑map path
            wanted_src_uids = set(unique_identifier_range_map.keys())

        if not wanted_src_uids:
            return pd.DataFrame()

        # evaluate the rules for each asset
        from mainsequence.client import Asset

        assets = Asset.filter(unique_identifier__in=list(wanted_src_uids))
        # assets that i want to get pricces

        asset_translation_dict = {}
        for asset in assets:
            asset_translation_dict[asset.unique_identifier] = self.translation_table.evaluate_asset(
                asset
            )

        # we grouped the assets for the same rules together and now query all assets that have the same target
        translation_df = pd.DataFrame.from_dict(asset_translation_dict, orient="index")
        try:
            grouped = translation_df.groupby(
                ["markets_time_serie_unique_identifier", "exchange_code"], dropna=False
            )
        except Exception as e:
            raise e

        data_df = []
        for (mkt_ts_id, target_exchange_code), group_df in grouped:
            # get the correct DataNode instance from our pre-built map
            api_ts = self.api_ts_map[mkt_ts_id]

            # figure out which assets belong to this group
            grouped_unique_ids = group_df.index.tolist()
            source_assets = [
                a for a in assets if a.unique_identifier in grouped_unique_ids
            ]  # source the ones we want to have

            # get correct target assets based on the share classes
            asset_ticker_group_ides = [a.asset_ticker_group_id for a in source_assets] #assets?
            asset_query = dict(asset_ticker_group_id__in=asset_ticker_group_ides)
            if not pd.isna(target_exchange_code):
                asset_query["exchange_code"] = target_exchange_code

            target_assets = Asset.filter(**asset_query)  # the assets that have the same group

            target_asset_unique_ids = [a.asset_ticker_group_id for a in target_assets]
            if len(asset_ticker_group_ides) > len(target_asset_unique_ids):
                raise Exception(
                    f"Not all assets were found in backend for translation table: {set(asset_ticker_group_ides) - set(target_asset_unique_ids)}"
                )

            if len(asset_ticker_group_ides) < len(target_asset_unique_ids):
                # this will blow the proper selection of assets
                raise Exception(
                    f"Too many assets were found in backend for translation table: {set(target_asset_unique_ids) - set(asset_ticker_group_ides)}"
                )

            # create the source-target mapping
            ticker_group_to_uid_map = {}
            for a in source_assets:
                if a.asset_ticker_group_id in ticker_group_to_uid_map:
                    raise ValueError(f"Share class {a.asset_ticker_group_id} cannot be duplicated")
                ticker_group_to_uid_map[a.asset_ticker_group_id] = a.unique_identifier

            source_target_map = {}
            for a in target_assets:
                asset_ticker_group_id = a.asset_ticker_group_id
                source_unique_identifier = ticker_group_to_uid_map[asset_ticker_group_id]
                source_target_map[source_unique_identifier] = a.unique_identifier

            target_source_map = {v: k for k, v in source_target_map.items()}
            if unique_identifier_range_map is not None:
                # create the correct unique identifier range map
                unique_identifier_range_map_target = {}
                for a_unique_identifier, asset_range in unique_identifier_range_map.items():
                    if a_unique_identifier not in source_target_map.keys():
                        continue
                    target_key = source_target_map[a_unique_identifier]
                    unique_identifier_range_map_target[target_key] = asset_range

                if not unique_identifier_range_map_target:
                    self.logger.warning(
                        f"Unique identifier map is empty for group assets {source_assets} and unique_identifier_range_map {unique_identifier_range_map}"
                    )
                    continue

                tmp_data = api_ts.get_df_between_dates(
                    unique_identifier_range_map=unique_identifier_range_map_target,
                    start_date=start_date,
                    end_date=end_date,
                    great_or_equal=great_or_equal,
                    less_or_equal=less_or_equal,
                )
            else:
                tmp_data = api_ts.get_df_between_dates(
                    start_date=start_date,
                    end_date=end_date,
                    unique_identifier_list=list(target_source_map.keys()),
                    great_or_equal=great_or_equal,
                    less_or_equal=less_or_equal,
                )

            if tmp_data.empty:
                continue

            tmp_data = tmp_data.rename(index=target_source_map, level="unique_identifier")
            data_df.append(tmp_data)

        if not data_df:
            return pd.DataFrame()

        data_df = pd.concat(data_df, axis=0)
        return data_df

    def update(self, update_statistics):
        """WrapperTimeSeries does not update"""
        pass


build_operations.serialize_argument.register(DataNode, build_operations._serialize_timeserie)
build_operations.serialize_argument.register(APIDataNode, build_operations._serialize_api_timeserie)
