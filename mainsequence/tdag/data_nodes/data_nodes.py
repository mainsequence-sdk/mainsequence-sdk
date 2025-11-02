import copy
import datetime
import inspect
import json
import logging
import os
import tempfile
from abc import ABC, abstractmethod
from dataclasses import asdict
from functools import wraps
from typing import Any, Union,Optional

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
    ColumnMetaData,
    DataSource,
    UniqueIdentifierRangeMap,
    UpdateStatistics,
)
from mainsequence.instrumentation import tracer
from mainsequence.logconf import logger
from mainsequence.tdag.config import ogm
from mainsequence.tdag.data_nodes.persist_managers import APIPersistManager, PersistManager

from .persist_managers import get_data_node_source_code


def get_data_source_from_orm() -> Any:
    from mainsequence.client import SessionDataSource

    if SessionDataSource.data_source.related_resource is None:
        raise Exception("This Pod does not have a default data source")
    return SessionDataSource.data_source


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

    def get_last_observation(self, asset_list: Optional[list[ms_client.AssetMixin]]=None):
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

        repr = (
            self.__class__.__name__
            + f" {os.environ['TDAG_ENDPOINT']}/dynamic-table-metadatas/details/?dynamic_table_id={self.data_source_id}"
        )
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
    Base DataNode class
    """

    OFFSET_START = datetime.datetime(2018, 1, 1, tzinfo=pytz.utc)
    OPEN_TO_PUBLIC=False # flag for enterprise data providers that want to open their data nmodes
    _ARGS_IGNORE_IN_STORAGE_HASH = []

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
        init_meta: build_operations.TimeSerieInitMeta | None = None,
        build_meta_data: dict | None = None,
        *args,
        **kwargs,
    ):
        """
        Initializes the DataNode object with the provided data_node_storage and configurations. For extension of the method

        This method sets up the time series object, loading the necessary configurations
        and metadata.

        Each DataNode instance will create a table in the Main Sequence Data Engine by uniquely hashing
        the arguments with exception of:

        - init_meta
        - build_meta_data

        Each DataNode instance will create a update_hash and a DataNodeUpdate instance in the Data Engine by uniquely hashing
        the same arguments as the table but excluding the arguments inside _LOCAL_KWARGS_TO_IGNORE


        allowed type of arguments can only be str,list, int or  Pydantic objects inlcuding lists of Pydantic Objects.

        The OFFSET_START property can be overridend and markts the minimum date value where the table will insert data

        Parameters
        ----------
        init_meta : dict, optional
            Metadata for initializing the time series instance.
        build_meta_data : dict, optional
            Metadata related to the building process of the time series.
        *args : tuple
            Additional arguments.
        **kwargs : dict
            Additional keyword arguments.
        """

        self.init_meta = init_meta

        self.build_meta_data = build_meta_data or {}
        self.build_meta_data.setdefault("initialize_with_default_partitions", True)

        self.build_meta_data = build_meta_data

        self.pre_load_routines_run = False
        self._data_source: DynamicTableDataSource | None = None  # is set later
        self._local_persist_manager: PersistManager | None = None

        self._scheduler_tree_connected = False
        self.update_statistics = None

    def __init_subclass__(cls, **kwargs):
        """
        This special method is called when DataNode is subclassed.
        It automatically wraps the subclass's __init__ method to add post-init routines.
        """
        super().__init_subclass__(**kwargs)

        # Get the original __init__ from the new subclass
        original_init = cls.__init__

        @wraps(original_init)
        def wrapped_init(self, *args, **kwargs):
            # 1. Call the original __init__ of the subclass first
            original_init(self, *args, **kwargs)

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

            for cls_to_inspect in classes_to_inspect:
                # Only inspect the __init__ defined on the class itself.
                if "__init__" in cls_to_inspect.__dict__:
                    sig = inspect.signature(cls_to_inspect.__init__)
                    try:
                        # Use bind_partial as the full set of args might not match this specific signature.
                        bound_args = sig.bind_partial(self, *args, **kwargs)
                        bound_args.apply_defaults()

                        current_args = bound_args.arguments
                        current_args.pop("self", None)

                        # If the signature has **kwargs, it collects extraneous arguments. Unpack them.
                        if "kwargs" in current_args:
                            final_kwargs.update(current_args.pop("kwargs"))

                        # Update the final arguments. Overwrites parent args with child args.
                        final_kwargs.update(current_args)
                    except TypeError:
                        logger.warning(
                            f"Could not bind arguments for {cls_to_inspect.__name__}.__init__; skipping for config."
                        )
                        continue

            # Remove `args` as it collects un-named positional arguments which are not part of the config hash.
            final_kwargs.pop("args", None)

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

            self._patch_build_from_env()
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
            arguments_to_ignore_from_storage_hash=self._ARGS_IGNORE_IN_STORAGE_HASH,
            kwargs=init_kwargs,
            ts_class_name=self.__class__.__name__,
        )

        for field_name, value in asdict(config).items():
            setattr(self, field_name, value)

    def _patch_build_from_env(self) -> None:
        """
        Checks for the PATCH_BUILD_CONFIGURATION environment variable and,
        if set, flushes the pickle and patches the build configuration.
        """
        patch_build = os.environ.get("PATCH_BUILD_CONFIGURATION", "false").lower() in ["true", "1"]
        if patch_build:
            self.logger.warning(f"Patching build configuration for {self.storage_hash}")

            # Ensure dependencies are initialized
            self.local_persist_manager
            self.verify_and_build_remote_objects()

            pickle_path = self.get_pickle_path_from_time_serie()
            build_operations.flush_pickle(pickle_path=pickle_path)

            self.local_persist_manager.patch_build_configuration(
                local_configuration=self.local_initial_configuration,
                remote_configuration=self.remote_initial_configuration,
                remote_build_metadata=self.remote_build_metadata,
            )

    # --- Core Properties ---

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
                "init_meta",
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
            open_to_public=self.OPEN_TO_PUBLIC
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
                depth_df["data_node_update_id"] != self.data_node_update.id
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
            asset_list, init_fallback_date=self.OFFSET_START
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
        override_update_stats: UpdateStatistics | None = None,
    ):

        update_runner = run_operations.UpdateRunner(
            time_serie=self,
            debug_mode=debug_mode,
            force_update=force_update,
            update_tree=update_tree,
            update_only_tree=update_only_tree,
            remote_scheduler=remote_scheduler,
            override_update_stats=override_update_stats,
        )
        error_on_last_update, updated_df = update_runner.run()

        return error_on_last_update, updated_df

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
        """Provides the metadata configuration for a market time series."""

        return None

    def get_column_metadata(self) -> list[ColumnMetaData] | None:
        """
        This Method should return a list for ColumnMetaData to add extra context to each time series
        Examples:
            from mainsequence.client.models_tdag import ColumnMetaData
        columns_metadata = [ColumnMetaData(column_name="instrument",
                                          dtype="str",
                                          label="Instrument",
                                          description=(
                                              "Unique identifier provided by Valmer; it’s a composition of the "
                                              "columns `tv_emisora_serie`, and is also used as a ticker for custom "
                                              "assets in Valmer."
                                          )
                                          ),
                            ColumnMetaData(column_name="currency",
                                           dtype="str",
                                           label="Currency",
                                           description=(
                                               "Corresponds to  code for curries be aware this may not match Figi Currency assets"
                                           )
                                           ),

                            ]
        Returns:
            A list of ColumnMetaData objects, or None.
        """
        return None

    def get_asset_list(self) -> list["Asset"] | None:
        """
        Provide the list of assets that this DataNode should include when updating.

        By default, this method returns `self.asset_list` if defined.
        Subclasses _must_ override this method when no `asset_list` attribute was set
        during initialization, to supply a dynamic list of assets for update_statistics.

        Use Case:
          - For category-based series, return all Asset unique_identifiers in a given category
            (e.g., `AssetCategory(unique_identifier="investable_assets")`), so that only those
            assets are updated in this DataNode.

        Returns
        -------
        list or None
            - A list of asset unique_identifiers to include in the update.
            - `None` if no filtering by asset is required (update all assets by default).
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

    @abstractmethod
    def dependencies(self) -> dict[str, Union["DataNode", "APIDataNode"]]:
        """
        Subclasses must implement this method to explicitly declare their upstream dependencies.

        Returns:
            A dictionary where keys are descriptive names and values are the DataNode dependency instances.
        """
        raise NotImplementedError

    @abstractmethod
    def update(self) -> pd.DataFrame:
        """
        Fetch and ingest only the new rows for this DataNode based on prior update checkpoints.



        Requirements:
          - `time_index` **must** be a `datetime.datetime` instance with UTC timezone.
          - Column names **must** be all lowercase.
          - No column values may be Python `datetime` objects; if date/time storage is needed, convert to integer
            timestamps (e.g., UNIX epoch in seconds or milliseconds).

        After retrieving the incremental rows, this method inserts or upserts them into the Main Sequence Data Engine.

        Parameters
        ----------
        update_statistics : UpdateStatistics
            Object capturing the previous update state. Must expose:
              - `max_time` (datetime | None)
              - `max_time_per_id` (dict[str, datetime] | None)

        Returns
        -------
        pd.DataFrame
            A DataFrame containing only the newly added or updated records.
        """
        raise NotImplementedError


class WrapperDataNode(DataNode):
    """A wrapper class for managing multiple DataNode objects."""

    def __init__(self, translation_table: AssetTranslationTable, *args, **kwargs):
        """
        Initialize the WrapperDataNode.

        Args:
            time_series_dict: Dictionary of DataNode objects.
        """
        super().__init__(*args, **kwargs)

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

        translation_table = copy.deepcopy(translation_table)

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
            asset_ticker_group_ides = [a.asset_ticker_group_id for a in assets]
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
