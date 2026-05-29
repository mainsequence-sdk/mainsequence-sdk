# Standard Library Imports
from __future__ import annotations

import datetime
import gc
import json
import time
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any
from uuid import UUID

# Third-Party Library Imports
import numpy as np
import pandas as pd
import structlog.contextvars as cvars
from opentelemetry.trace import Status, StatusCode

# Client and ORM Models
import mainsequence.client as ms_client
from mainsequence.client import BaseUpdateStatistics
from mainsequence.client.dtype_codec import (
    DATE,
    LOCAL_DATETIME_NAIVE,
    TIMESTAMP_TZ,
    normalize_dtype_token,
    pandas_dtype_to_token,
    serialize_remote_value,
    sqlalchemy_type_to_token,
)

# Instrumentation and Logging
from mainsequence.instrumentation import TracerInstrumentator, tracer

if TYPE_CHECKING:
    from .data_nodes import DataNode


# Custom Exceptions
class DependencyUpdateError(Exception):
    pass


LocalUpdateResult = None | pd.DataFrame | Sequence[Any]


def _is_nullish(value: Any) -> bool:
    if value is None:
        return True
    try:
        is_na = pd.isna(value)
    except (TypeError, ValueError):
        return False
    if isinstance(is_na, (bool, np.bool_)):
        return bool(is_na)
    return False


def _validate_json_compatible_values(column_name: str, values: Sequence[Any]) -> None:
    for value in values:
        if _is_nullish(value):
            continue
        try:
            json.dumps(value, allow_nan=False)
        except (TypeError, ValueError) as exc:
            raise TypeError(
                f"Column '{column_name}' is declared as json/jsonb but contains "
                f"a non-JSON-serializable value: {value!r}"
            ) from exc


def _validate_uuid_compatible_values(column_name: str, values: Sequence[Any]) -> None:
    for value in values:
        if _is_nullish(value) or isinstance(value, UUID):
            continue
        try:
            UUID(str(value))
        except (TypeError, ValueError, AttributeError) as exc:
            raise TypeError(
                f"Column '{column_name}' is declared as uuid but contains "
                f"a non-UUID value: {value!r}"
            ) from exc


def _validate_string_compatible_values(column_name: str, values: Sequence[Any]) -> None:
    for value in values:
        if _is_nullish(value):
            continue
        if not isinstance(value, str):
            raise TypeError(
                f"Column '{column_name}' is declared as string but contains "
                f"a non-string value: {value!r}"
            )


def _validate_declared_record_dtype(
    *,
    column_name: str,
    declared_dtype: str,
    actual_dtype: Any,
    values: Sequence[Any],
    remote_dtypes: bool = True,
    allow_naive_datetime: bool = False,
) -> None:
    normalized_declared_dtype = normalize_dtype_token(
        declared_dtype,
        remote=remote_dtypes,
        allow_naive_datetime=allow_naive_datetime,
    )
    if normalized_declared_dtype in {"json", "jsonb"}:
        _validate_json_compatible_values(column_name, values)
        return
    if normalized_declared_dtype == "uuid":
        _validate_uuid_compatible_values(column_name, values)
        return
    if normalized_declared_dtype in {DATE, TIMESTAMP_TZ}:
        for value in values:
            try:
                serialize_remote_value(value, normalized_declared_dtype)
            except Exception as exc:
                raise TypeError(
                    f"Column '{column_name}' is declared as {declared_dtype} "
                    f"but contains an incompatible temporal value: {value!r}"
                ) from exc
        return

    try:
        normalized_actual_dtype = pandas_dtype_to_token(
            actual_dtype,
            remote=remote_dtypes,
            allow_naive_datetime=allow_naive_datetime,
        )
    except ValueError as exc:
        raise TypeError(
            f"Column '{column_name}' is declared as {declared_dtype} "
            f"but DataFrame dtype is {actual_dtype}"
        ) from exc

    if normalized_declared_dtype == "string":
        _validate_string_compatible_values(column_name, values)
        return
    if normalized_declared_dtype == LOCAL_DATETIME_NAIVE:
        if remote_dtypes:
            raise TypeError(
                f"Column '{column_name}' is declared as {declared_dtype}, "
                "but timezone-naive datetime is local-backend-only."
            )
        if normalized_actual_dtype != LOCAL_DATETIME_NAIVE:
            raise TypeError(
                f"Column '{column_name}' is declared as {declared_dtype} "
                f"but DataFrame dtype is {actual_dtype}"
            )
        return

    if normalized_actual_dtype != normalized_declared_dtype:
        raise TypeError(
            f"Column '{column_name}' is declared as {declared_dtype} "
            f"but DataFrame dtype is {actual_dtype}"
        )


def _column_attr(column: Any, *names: str) -> Any:
    for name in names:
        if isinstance(column, Mapping):
            value = column.get(name)
        else:
            value = getattr(column, name, None)
        if value not in (None, ""):
            return value
    return None


def _metatable_contract_column_dtypes_map(meta_table: Any) -> dict[str, str]:
    if meta_table is None:
        return {}

    table_contract = getattr(meta_table, "table_contract", None)
    if isinstance(table_contract, Mapping):
        contract_columns = table_contract.get("columns") or []
    else:
        contract_columns = getattr(table_contract, "columns", []) or []

    if not contract_columns:
        contract_columns = getattr(meta_table, "columns", []) or []

    if not contract_columns:
        table = getattr(meta_table, "__table__", None)
        contract_columns = list(getattr(table, "columns", []) or [])

    column_dtypes: dict[str, str] = {}
    for column in contract_columns:
        column_name = _column_attr(column, "name", "column_name")
        data_type = _column_attr(column, "data_type", "dtype")
        if data_type in (None, "") and getattr(column, "type", None) is not None:
            data_type = sqlalchemy_type_to_token(column.type, remote=True)
        if column_name in (None, "") or data_type in (None, ""):
            continue
        column_dtypes[str(column_name)] = str(data_type)
    return column_dtypes


def _storage_hash(storage_table: Any) -> str | None:
    if storage_table is None:
        return None
    if isinstance(storage_table, Mapping):
        storage_hash = storage_table.get("storage_hash") or storage_table.get("physical_table_name")
    else:
        hash_getter = getattr(storage_table, "get_storage_hash", None)
        try:
            storage_hash = hash_getter() if callable(hash_getter) else None
        except Exception:
            storage_hash = None
        storage_hash = storage_hash or getattr(storage_table, "storage_hash", None)
        storage_hash = storage_hash or getattr(storage_table, "physical_table_name", None)
        if storage_hash in (None, ""):
            table = getattr(storage_table, "__table__", None)
            storage_hash = getattr(table, "name", None)
    return str(storage_hash) if isinstance(storage_hash, str) and storage_hash else None


def _require_uid(obj: Any, object_name: str) -> str:
    uid = getattr(obj, "uid", None)
    if uid in (None, ""):
        raise ValueError(f"{object_name} must have a uid.")
    return str(uid)


class UpdateRunner:
    """
    Orchestrates the entire update process for a DataNode instance.
    It handles scheduling, dependency resolution, execution, and error handling.
    """

    def __init__(
        self,
        time_serie: DataNode,
        debug_mode: bool = False,
        force_update: bool = False,
        update_tree: bool = True,
        update_only_tree: bool = False,
        remote_scheduler: ms_client.Scheduler | None = None,
        override_update_stats: BaseUpdateStatistics | None = None,
    ):
        self.ts = time_serie
        self.logger = self.ts.logger
        self.debug_mode = debug_mode
        self.force_update = force_update
        self.update_tree = update_tree
        self.update_only_tree = update_only_tree
        if self.update_tree:
            self.update_only_tree = False

        self.remote_scheduler = remote_scheduler
        self.scheduler: ms_client.Scheduler | None = None
        self.override_update_stats = override_update_stats

    def _setup_scheduler(self) -> None:
        """Initializes or retrieves the scheduler and starts its heartbeat."""
        if self.remote_scheduler:
            self.scheduler = self.remote_scheduler
            return

        name_prefix = "DEBUG_" if self.debug_mode else ""
        update_uid = _require_uid(self.ts.data_node_update, "DataNodeUpdate")
        self.scheduler = ms_client.Scheduler.build_and_assign_to_update_nodes(
            scheduler_name=f"{name_prefix}{update_uid}",
            update_node_uids=[update_uid],
            remove_from_other_schedulers=True,
            running_in_debug_mode=self.debug_mode,
        )
        self.scheduler.start_heart_beat()

    def _pre_update_routines(
        self, data_node_update: dict | None = None
    ) -> tuple[dict[str, Any], Any]:
        """
        Synchronize the head updater, ensure the dependency graph is registered,
        and fetch the latest typed update objects for the full dependency tree.

        This method:

        - synchronizes the head updater through the local persist manager,
        - ensures dependency relations and dependency dataframes are loaded,
        - connects the active scheduler to the dependency tree when needed,
        - fetches the latest backend update objects using
          ``self.ts.DATA_NODE_UPDATE_CLASS.get_data_nodes_and_set_updates(...)``,
        - stores the returned run-configuration snapshots on
          ``self.ts.update_details_tree``.

        Args:
            data_node_update: Optional already-fetched backend payload for the
                head updater, used to synchronize local state before loading the
                full tree.

        Returns:
            A tuple containing:

            - a mapping of updater uid -> typed update object for the full tree
            - the backend ``state_data`` payload returned by the update class
        """
        # 1. Synchronize the head node and load its dependency structure.
        self.ts.local_persist_manager.synchronize_data_node_update(
            data_node_update=data_node_update
        )
        self.ts.set_relation_tree()

        # The `load_dependencies` logic is now integrated here.
        if self.ts.dependencies_df is None:
            self.ts.set_dependencies_df()

        # 2. Connect the dependency tree to the scheduler if it hasn't been already.
        if not self.ts._scheduler_tree_connected and self.update_tree:
            self.logger.debug("Connecting dependency tree to scheduler...")
            if not self.ts.depth_df.empty:
                all_uids = self.ts.depth_df["update_node_uid"].astype(str).to_list() + [
                    _require_uid(self.ts.data_node_update, "DataNodeUpdate")
                ]
                self.scheduler.in_active_tree_connect(update_node_uids=all_uids)
            self.ts._scheduler_tree_connected = True

        # 3. Collect all UIDs in the dependency graph to fetch their metadata.
        # This correctly initializes the list, fixing the original bug.
        if not self.ts.depth_df.empty:
            update_nodes_in_tree = self.ts.depth_df[
                ["update_node_uid", "node_type", "update_hash", "remote_table_hash_id"]
            ].to_dict("records")
            for update_node in update_nodes_in_tree:
                update_node["uid"] = str(update_node.pop("update_node_uid"))
        else:
            update_nodes_in_tree = []

        # Always include the head node itself.
        update_nodes_in_tree.append(
            {
                "uid": _require_uid(self.ts.data_node_update, "DataNodeUpdate"),
                "update_hash": self.ts.data_node_update.update_hash,
                "remote_table_hash_id": _storage_hash(
                    self.ts.local_persist_manager.storage_metadata
                ),
                "node_type": self.ts.data_node_update.NODE_TYPE,
            }
        )

        # 4. Fetch the latest metadata for the entire tree from the backend.
        update_details_batch = dict(
            error_on_last_update=False,
            active_update_scheduler_uid=_require_uid(self.scheduler, "Scheduler"),
            active_update_status="Q",  # Assuming queue status is always set here
        )

        update_class = self.ts.DATA_NODE_UPDATE_CLASS
        all_metadatas_response = update_class.get_data_nodes_and_set_updates(
            update_nodes=update_nodes_in_tree,
            update_details_kwargs=update_details_batch,
            update_priority_dict=None,
        )

        # 5. Process and return the results.
        state_data = all_metadatas_response.state_data
        data_node_updates_list = all_metadatas_response.data_node_updates
        data_node_updates_map = {
            _require_uid(m, m.__class__.__name__): m for m in data_node_updates_list
        }

        self.ts.scheduler = self.scheduler
        self.ts.update_details_tree = {
            key: v.run_configuration for key, v in data_node_updates_map.items()
        }

        return data_node_updates_map, state_data

    def _setup_execution_environment(self) -> dict[str, Any]:
        data_node_updates, state_data = self._pre_update_routines()
        return data_node_updates

    def _start_update(
        self,
        override_update_stats: BaseUpdateStatistics | None = None,
    ) -> tuple[bool, LocalUpdateResult]:
        """Orchestrates a single DataNode update, including pre/post routines."""
        historical_update = self.ts.local_persist_manager.data_node_update.set_start_of_execution(
            active_update_scheduler_uid=_require_uid(self.scheduler, "Scheduler")
        )

        must_update = historical_update.must_update or self.force_update

        # Ensure metadata is fully loaded with relationship details before proceeding.
        self.ts.local_persist_manager.set_data_node_update_lazy(include_relations_detail=True)

        if override_update_stats is not None:
            self.ts.update_statistics = override_update_stats
        else:
            update_statistics = historical_update.update_statistics
            # The DataNode defines how to scope its statistics
            self.ts._set_update_statistics(update_statistics)

        update_result: LocalUpdateResult = None
        error_on_last_update = False
        try:
            if must_update:
                self.logger.debug(f"Update required for {self.ts}.")
                update_result = self._update_local(
                    historical_update=historical_update,
                )
            else:
                self.logger.debug(f"Already up-to-date. Skipping update for {self.ts}.")
        except Exception as e:
            error_on_last_update = True
            raise e
        finally:
            self.ts.local_persist_manager.data_node_update.set_end_of_execution(
                historical_update_uid=historical_update.uid, error_on_update=error_on_last_update
            )

            # Always set last relations details after the run completes.
            self.ts.local_persist_manager.set_data_node_update_lazy(include_relations_detail=True)

            self.ts.run_post_update_routines(error_on_last_update=error_on_last_update)

        return error_on_last_update, update_result

    @staticmethod
    def validate_data_frame(
        df: pd.DataFrame,
        storage_class_type,
        meta_table: Any | None = None,
    ) -> None:
        """
        Performs a series of critical checks on the DataFrame before persistence.

        Args:
            df: The DataFrame returned from the DataNode's update method.

        Raises:
            AssertionError or Exception if any validation check fails.
        """
        # Check for infinite values
        df.replace([np.inf, -np.inf], np.nan, inplace=True)

        # Check that the time index is a UTC datetime
        time_index = df.index.get_level_values(0)
        if not pd.api.types.is_datetime64_ns_dtype(time_index) or str(time_index.tz) != str(
            datetime.UTC
        ):
            raise TypeError(f"Time index must be datetime64[ns, UTC], but found {time_index.dtype}")

        # Enforce backend-safe physical column names for non-local storage.
        if storage_class_type not in ms_client.LOCAL_DATA_SOURCE_CLASS_TYPES:
            for col, _dtype in df.dtypes.items():
                if not isinstance(col, str) or not col.islower():
                    raise ValueError(f"Column name '{col}' must be a lowercase string.")
                if len(col) > 63:
                    raise ValueError(f"Column name '{col}' must be 63 characters or fewer.")
        is_local_storage = storage_class_type in ms_client.LOCAL_DATA_SOURCE_CLASS_TYPES
        contract_column_dtypes_map = _metatable_contract_column_dtypes_map(meta_table)
        if contract_column_dtypes_map:
            column_dtypes_map = {
                column_name: normalize_dtype_token(
                    dtype,
                    remote=not is_local_storage,
                    allow_naive_datetime=is_local_storage,
                )
                for column_name, dtype in contract_column_dtypes_map.items()
            }
        else:
            column_dtypes_map = {}

        if not column_dtypes_map:
            return

        frame_columns = {str(column_name) for column_name in df.columns}
        frame_column_lookup = {str(column_name): column_name for column_name in df.columns}
        index_names = {str(index_name) for index_name in df.index.names if index_name is not None}
        missing_record_columns = [
            column_name
            for column_name in column_dtypes_map
            if column_name not in frame_columns and column_name not in index_names
        ]
        if missing_record_columns:
            raise ValueError(
                "MetaTable contract declares columns not present in the DataFrame: "
                f"{missing_record_columns}"
            )

        for column_name, declared_dtype in column_dtypes_map.items():
            if column_name in frame_columns:
                frame_column_name = frame_column_lookup[column_name]
                values = df[frame_column_name].tolist()
                actual_dtype = df[frame_column_name].dtype
            else:
                index_values = df.index.get_level_values(column_name)
                values = index_values.tolist()
                actual_dtype = index_values.dtype

            _validate_declared_record_dtype(
                column_name=column_name,
                declared_dtype=declared_dtype,
                actual_dtype=actual_dtype,
                values=values,
                remote_dtypes=not is_local_storage,
                allow_naive_datetime=is_local_storage,
            )

    @tracer.start_as_current_span("UpdateRunner._update_local")
    def _update_local(
        self,
        historical_update: Any,
    ) -> LocalUpdateResult:
        """
        Calculates, validates, and persists the node update result.

        Args:
            historical_update: Backend historical update record returned by
                ``set_start_of_execution()`` for this run. The node-specific
                ``_execute_local_update(...)`` implementation is responsible for
                interpreting any fields on this object.
            Dependencies are executed from the source-declared in-memory graph.
        """
        # 1. Handle dependency tree update first
        if self.update_tree:
            self._verify_tree_is_updated()
            if self.update_only_tree:
                self.logger.info(
                    f"Dependency tree for {self.ts} updated. Halting run as requested."
                )
                return None

        # 2. Execute the core data calculation
        with tracer.start_as_current_span("Update Calculation") as update_span:
            try:
                update_result = self.ts._execute_local_update(
                    historical_update=historical_update,
                )
                update_span.set_status(Status(StatusCode.OK))
                return update_result

            except Exception as e:
                self.logger.exception("Failed during update calculation or persistence.")
                update_span.set_status(Status(StatusCode.ERROR, description=str(e)))
                raise e
            finally:
                self.ts.local_persist_manager.synchronize_data_node_update(None)
                us = self.ts.local_persist_manager.get_update_statistics_for_table()
                self.ts.update_statistics = us

    @tracer.start_as_current_span("UpdateRunner._verify_tree_is_updated")
    def _verify_tree_is_updated(self) -> None:
        """
        Ensures all dependencies in the tree are updated before the head node.

        This method checks if the dependency graph is defined in the backend and
        then delegates the update execution to either a sequential (debug) or
        parallel (production) helper method.

        Dependencies are executed from the currently declared DataNode graph.
        Backend dependency metadata is ordering/state only; it is not used to
        cold-rebuild executable DataNode instances.
        """
        # 1. Ensure the dependency graph is built in the backend
        declared_dependencies = self.ts.dependencies() or {}
        deps_uids = [
            (
                _require_uid(d.data_node_update, "DataNodeUpdate")
                if (not d.is_api and d.data_node_update is not None)
                else None
            )
            for d in declared_dependencies.values()
        ]

        # 2. Get the list of dependencies to update
        dependencies_df = self.ts.dependencies_df
        if (
            dependencies_df is not None
            and not dependencies_df.empty
            and "update_node_uid" not in dependencies_df.columns
        ):
            raise ValueError("Dependency dataframe must include 'update_node_uid'.")
        dependency_uids_in_tree = (
            dependencies_df["update_node_uid"].astype(str).to_list()
            if dependencies_df is not None and not dependencies_df.empty
            else []
        )

        if any([a is None for a in deps_uids]) or any(
            [d not in dependency_uids_in_tree for d in deps_uids]
        ):
            # Datanode not update set
            self.ts.local_persist_manager.data_node_update.patch(ogm_dependencies_linked=False)

        if not self.ts.local_persist_manager.data_node_update.ogm_dependencies_linked:
            self.logger.info("Dependency tree not set. Building now...")
            start_time = time.time()
            self.ts.set_relation_tree()
            self.logger.debug(f"Tree build took {time.time() - start_time:.2f}s.")
            self.ts.set_dependencies_df()
            dependencies_df = self.ts.dependencies_df

        if dependencies_df.empty:
            self.logger.debug("No dependencies to update.")
            return

        # 3. Build the executable dependency map from source declarations.
        update_map = self._get_update_map(declared_dependencies, logger=self.logger)

        # 4. Delegate to the appropriate execution method
        self.logger.debug(f"Starting update for {len(dependencies_df)} dependencies...")

        if self.debug_mode:
            self._execute_sequential_debug_update(
                dependencies_df,
                update_map,
            )
        else:
            # self._execute_parallel_distributed_update(dependencies_df) Todo: Impplement
            self._execute_sequential_debug_update(
                dependencies_df,
                update_map,
            )

        self.logger.debug(f"Dependency tree evaluation complete for {self.ts}.")

    def _get_update_map(
        self,
        declared_dependencies: dict[str, DataNode],
        logger: object,
        dependecy_map: dict | None = None,
    ) -> dict[str, dict[str, Any]]:
        """
        Obtains all DataNode objects in the dependency graph by recursively
        calling the dependencies() method.

        This approach is more robust than introspecting class members as it relies
        on an explicit declaration of dependencies.

        Args:
            time_serie_instance: The DataNode instance from which to start the dependency traversal.
            dependecy_map: An optional dictionary to store the dependency map, used for recursion.

        Returns:
            A dictionary mapping update node uid to DataNode info.
        """
        # Initialize the map on the first call
        if dependecy_map is None:
            dependecy_map = {}

        # Get the explicitly declared dependencies, just like set_relation_tree

        for name, dependency_ts in declared_dependencies.items():
            if dependency_ts.is_api:
                continue

            # Ensure the dependency is initialized in the persistence layer.
            _ = dependency_ts.local_persist_manager
            key = _require_uid(dependency_ts.data_node_update, "DataNodeUpdate")

            # If we have already processed this node, skip it to prevent infinite loops
            if key in dependecy_map:
                continue

            logger.debug(f"Adding dependency '{name}' to update map.")
            dependecy_map[key] = {"ts": dependency_ts}
            declared_dependencies = dependency_ts.dependencies() or {}
            # Recursively call get_update_map on the dependency to traverse the entire graph
            self._get_update_map(
                declared_dependencies=declared_dependencies,
                logger=logger,
                dependecy_map=dependecy_map,
            )

        return dependecy_map

    def _execute_sequential_debug_update(
        self,
        dependencies_df: pd.DataFrame,
        update_map: dict[str, dict],
    ) -> None:
        """Runs dependency updates sequentially in the same process for debugging."""
        self.logger.info("Executing dependency updates in sequential debug mode.")
        # Sort by priority to respect the DAG execution order
        sorted_priorities = sorted(dependencies_df["update_priority"].unique())

        def refresh_update_statistics_of_deps(ts):
            for _, ts_dep in ts.dependencies().items():
                if ts_dep.is_api:
                    continue  # No need to update statistics for API dependencies
                ts_dep.update_statistics = (
                    ts_dep.local_persist_manager.get_update_statistics_for_table()
                )

        for priority in sorted_priorities:
            priority_df = dependencies_df[dependencies_df["update_priority"] == priority]
            # Sort by number of upstreams to potentially optimize within a priority level
            sorted_deps = priority_df.sort_values("number_of_upstreams", ascending=False)

            for _, ts_row in sorted_deps.iterrows():
                update_node_uid = str(ts_row["update_node_uid"])
                try:
                    if update_node_uid not in update_map:
                        raise DependencyUpdateError(
                            "Backend dependency metadata includes an update node that "
                            "is not declared by the current DataNode.dependencies() graph: "
                            f"update_node_uid={update_node_uid!r}."
                        )

                    ts_to_update = update_map[update_node_uid]["ts"]
                    refresh_update_statistics_of_deps(ts_to_update)

                    self.logger.debug(
                        f"Running debug update for dependency: {ts_to_update.update_hash}"
                    )
                    # Each dependency gets its own clean runner.
                    dep_runner = UpdateRunner(
                        time_serie=ts_to_update,
                        debug_mode=True,
                        update_tree=False,
                        force_update=self.force_update,
                        remote_scheduler=self.scheduler,
                    )
                    dep_runner._setup_scheduler()

                    dep_runner._start_update()
                except Exception as e:
                    self.logger.exception(f"Failed to update dependency {update_node_uid}")
                    raise e  # Re-raise to halt the entire process on failure

        # refresh update statistics of direct dependencies

        refresh_update_statistics_of_deps(self.ts)

    # This code is a method within the UpdateRunner class.
    # Assumes 'ms_client', 'tracer_instrumentator', and 'DependencyUpdateError' are imported.

    @tracer.start_as_current_span("UpdateRunner._execute_parallel_distributed_update")
    def _execute_parallel_distributed_update(
        self,
        dependencies_df: pd.DataFrame,
    ) -> None:
        """ """
        # 1. Prepare tasks, prioritizing any pre-loaded time series

        raise Exception(
            "This is an Enterprise feature available only in the Main Sequence Platform"
        )

    def run(self) -> None:
        """
        Executes the full update lifecycle for the time series.

        This is the main entry point for the runner. It orchestrates the setup
        of scheduling and the execution environment, triggers the core update
        process, and handles all error reporting and cleanup.
        """
        # Initialize tracing and set initial flags
        tracer_instrumentator = TracerInstrumentator()
        tracer = tracer_instrumentator.build_tracer()
        error_to_raise = None

        # 1. Set up the scheduler for this run
        try:
            self.ts.verify_and_build_remote_objects()  # needed to start sch
            self._setup_scheduler()
            cvars.bind_contextvars(
                scheduler_name=self.scheduler.name, head_local_ts_hash_id=self.ts.update_hash
            )

            # 2. Start the main execution block with tracing
            with tracer.start_as_current_span(
                f"Scheduler Head Update: {self.ts.update_hash}"
            ) as span:
                span.set_attribute("time_serie_update_hash", self.ts.update_hash)
                storage_hash = _storage_hash(self.ts.local_persist_manager.storage_metadata)
                if storage_hash is not None:
                    span.set_attribute("storage_hash", storage_hash)
                span.set_attribute("head_scheduler", self.scheduler.name)

                # 3. Prepare the execution environment (Ray actors, dependency metadata)
                _ = self._setup_execution_environment()
                self.logger.debug("Execution environment and dependency metadata are set.")

                # 4. Wait for the scheduled update time, if not forcing an immediate run
                if not self.force_update:
                    self.ts.data_node_update.wait_for_update_time()

                # 5. Trigger the core update process
                error_on_last_update, update_result = self._start_update(
                    override_update_stats=self.override_update_stats,
                )

                return error_on_last_update, update_result

        except DependencyUpdateError as de:
            self.logger.error("A dependency failed to update, halting the run.", error=de)
            error_to_raise = de
        except TimeoutError as te:
            self.logger.error("The update process timed out.", error=te)
            error_to_raise = te
        except Exception as e:
            self.logger.exception("An unexpected error occurred during the update run.")
            error_to_raise = e
        finally:
            # 6. Clean up resources
            # Stop the scheduler heartbeat if it was created by this runner
            if self.remote_scheduler is None and self.scheduler:
                self.scheduler.stop_heart_beat()

            # Clean up temporary attributes on the DataNode instance
            if hasattr(self.ts, "update_tracker"):
                del self.ts.update_tracker

            gc.collect()

        # 7. Re-raise any captured exception after cleanup
        if error_to_raise:
            raise error_to_raise
