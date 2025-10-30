# Standard Library Imports
import datetime
import gc
import time
from typing import Any

# Third-Party Library Imports
import numpy as np
import pandas as pd
import structlog.contextvars as cvars
from opentelemetry.trace import Status, StatusCode

# Client and ORM Models
import mainsequence.client as ms_client
from mainsequence.client import UpdateStatistics

# Instrumentation and Logging
from mainsequence.instrumentation import TracerInstrumentator, tracer

# TDAG Core Components and Helpers
from mainsequence.tdag.data_nodes import build_operations


# Custom Exceptions
class DependencyUpdateError(Exception):
    pass


class UpdateRunner:
    """
    Orchestrates the entire update process for a DataNode instance.
    It handles scheduling, dependency resolution, execution, and error handling.
    """

    def __init__(
        self,
        time_serie: "DataNode",
        debug_mode: bool = False,
        force_update: bool = False,
        update_tree: bool = True,
        update_only_tree: bool = False,
        remote_scheduler: ms_client.Scheduler | None = None,
        override_update_stats: UpdateStatistics | None = None,
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
        self.scheduler = ms_client.Scheduler.build_and_assign_to_ts(
            scheduler_name=f"{name_prefix}{self.ts.data_node_update.id}",
            time_serie_ids=[self.ts.data_node_update.id],
            remove_from_other_schedulers=True,
            running_in_debug_mode=self.debug_mode,
        )
        self.scheduler.start_heart_beat()

    def _pre_update_routines(
        self, data_node_update: dict | None = None
    ) -> tuple[dict[int, ms_client.DataNodeUpdate], Any]:
        """
        Prepares the DataNode and its dependencies for an update by fetching the
        latest metadata for the entire dependency graph.

        Args:
            data_node_update: Optional dictionary with metadata for the head node,
                            used to synchronize before fetching the full tree.

        Returns:
            A tuple containing a dictionary of all local metadata objects in the
            tree (keyed by ID) and the corresponding state data.
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
                all_ids = self.ts.depth_df["data_node_update_id"].to_list() + [
                    self.ts.data_node_update.id
                ]
                self.scheduler.in_active_tree_connect(local_time_series_ids=all_ids)
            self.ts._scheduler_tree_connected = True

        # 3. Collect all IDs in the dependency graph to fetch their metadata.
        # This correctly initializes the list, fixing the original bug.
        if not self.ts.depth_df.empty:
            all_ids_in_tree = self.ts.depth_df["data_node_update_id"].to_list()
        else:
            all_ids_in_tree = []

        # Always include the head node itself.
        all_ids_in_tree.append(self.ts.data_node_update.id)

        # 4. Fetch the latest metadata for the entire tree from the backend.
        update_details_batch = dict(
            error_on_last_update=False,
            active_update_scheduler_id=self.scheduler.id,
            active_update_status="Q",  # Assuming queue status is always set here
        )

        all_metadatas_response = ms_client.DataNodeUpdate.get_data_nodes_and_set_updates(
            local_time_series_ids=all_ids_in_tree,
            update_details_kwargs=update_details_batch,
            update_priority_dict=None,
        )

        # 5. Process and return the results.
        state_data = all_metadatas_response["state_data"]
        data_node_updates_list = all_metadatas_response["data_node_updates"]
        data_node_updates_map = {m.id: m for m in data_node_updates_list}

        self.ts.scheduler = self.scheduler
        self.ts.update_details_tree = {
            key: v.run_configuration for key, v in data_node_updates_map.items()
        }

        return data_node_updates_map, state_data

    def _setup_execution_environment(self) -> dict[int, ms_client.DataNodeUpdate]:
        data_node_updates, state_data = self._pre_update_routines()
        return data_node_updates

    def _start_update(
        self, use_state_for_update: bool, override_update_stats: UpdateStatistics | None = None
    ) -> [bool, pd.DataFrame]:
        """Orchestrates a single DataNode update, including pre/post routines."""
        historical_update = self.ts.local_persist_manager.data_node_update.set_start_of_execution(
            active_update_scheduler_id=self.scheduler.id
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

        updated_df = pd.DataFrame()
        error_on_last_update = False
        try:
            if must_update:
                self.logger.debug(f"Update required for {self.ts}.")
                updated_df = self._update_local(
                    overwrite_latest_value=historical_update.last_time_index_value,
                    use_state_for_update=use_state_for_update,
                )
            else:
                self.logger.debug(f"Already up-to-date. Skipping update for {self.ts}.")
        except Exception as e:
            error_on_last_update = True
            raise e
        finally:
            self.ts.local_persist_manager.data_node_update.set_end_of_execution(
                historical_update_id=historical_update.id, error_on_update=error_on_last_update
            )

            # Always set last relations details after the run completes.
            self.ts.local_persist_manager.set_data_node_update_lazy(include_relations_detail=True)

            self.ts.run_post_update_routines(error_on_last_update=error_on_last_update)
            self.ts.local_persist_manager.set_column_metadata(
                columns_metadata=self.ts.get_column_metadata()
            )
            table_metadata = self.ts.get_table_metadata()

            if self.ts.data_source.related_resource.class_type != ms_client.DUCK_DB:
                self.ts.local_persist_manager.set_table_metadata(table_metadata=table_metadata)

        return error_on_last_update, updated_df

    def _validate_update_dataframe(self, df: pd.DataFrame) -> None:
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
            datetime.timezone.utc
        ):
            raise TypeError(f"Time index must be datetime64[ns, UTC], but found {time_index.dtype}")

        # Check for forbidden data types and enforce lowercase columns
        if self.ts.data_source.related_resource.class_type != ms_client.DUCK_DB:

            for col, dtype in df.dtypes.items():
                if not isinstance(col, str) or not col.islower():
                    raise ValueError(f"Column name '{col}' must be a lowercase string.")
                if "datetime64" in str(dtype):
                    raise TypeError(f"Column '{col}' has a forbidden datetime64 dtype.")

    @tracer.start_as_current_span("UpdateRunner._update_local")
    def _update_local(
        self,
        overwrite_latest_value: datetime.datetime | None,
        use_state_for_update: bool,
    ) -> pd.DataFrame:
        """
        Calculates, validates, and persists the data update for the time series.
        """
        tmp_df = pd.DataFrame()
        # 1. Handle dependency tree update first
        if self.update_tree:
            self._verify_tree_is_updated(use_state_for_update)
            if self.update_only_tree:
                self.logger.info(
                    f"Dependency tree for {self.ts} updated. Halting run as requested."
                )
                return tmp_df

        # 2. Execute the core data calculation
        with tracer.start_as_current_span("Update Calculation") as update_span:

            # Add specific log message for the initial run
            if not self.ts.update_statistics:
                self.logger.debug(f"Performing first-time update for {self.ts}...")
            else:
                self.logger.debug(f"Calculating update for {self.ts}...")

            try:
                # Call the business logic defined on the DataNode class
                temp_df = self.ts.update()
                overwrite_latest_value =overwrite_latest_value if not hasattr(self.ts,"overwrite_latest_value") else self.ts.overwrite_latest_value
                if temp_df is None:
                    raise Exception(f" {self.ts} update(...) method needs to return a data frame")

                # If the update method returns no data, we're done.
                if temp_df.empty:
                    self.logger.warning(f"No new data returned from update for {self.ts}.")
                    return temp_df

                # In a normal run, filter out data we already have.
                if (
                    overwrite_latest_value is None
                    and ms_client.SessionDataSource.is_local_duck_db == False
                ):
                    temp_df = self.ts.update_statistics.filter_df_by_latest_value(temp_df)

                # If filtering left nothing, we're done.
                if temp_df.empty:
                    self.logger.warning(f"No new data to persist for {self.ts} after filtering.")
                    return temp_df

                # Validate the structure and content of the DataFrame
                self._validate_update_dataframe(temp_df)

                # Persist the validated data
                self.logger.info(f"Persisting {len(temp_df)} new rows for {self.ts}.")
                persisted = self.ts.local_persist_manager.persist_updated_data(
                    temp_df=temp_df, overwrite=(overwrite_latest_value is not None)
                )
                update_span.set_status(Status(StatusCode.OK))
                self.logger.info(f"Successfully updated {self.ts}.")
                return temp_df

            except Exception as e:
                self.logger.exception("Failed during update calculation or persistence.")
                update_span.set_status(Status(StatusCode.ERROR, description=str(e)))
                raise e
        return tmp_df

    @tracer.start_as_current_span("UpdateRunner._verify_tree_is_updated")
    def _verify_tree_is_updated(
        self,
        use_state_for_update: bool,
    ) -> None:
        """
        Ensures all dependencies in the tree are updated before the head node.

        This method checks if the dependency graph is defined in the backend and
        then delegates the update execution to either a sequential (debug) or
        parallel (production) helper method.

        Args:
            use_state_for_update: If True, uses the current state for the update.
        """
        # 1. Ensure the dependency graph is built in the backend
        declared_dependencies = self.ts.dependencies() or {}
        deps_ids = [
            (
                d.data_node_update.id
                if (d.is_api == False and d.data_node_update is not None)
                else None
            )
            for d in declared_dependencies.values()
        ]

        # 2. Get the list of dependencies to update
        dependencies_df = self.ts.dependencies_df

        if any([a is None for a in deps_ids]) or any(
            [d not in dependencies_df["data_node_update_id"].to_list() for d in deps_ids]
        ):
            # Datanode not update set
            self.ts.local_persist_manager.data_node_update.patch(ogm_dependencies_linked=False)

        if self.ts.local_persist_manager.data_node_update.ogm_dependencies_linked == False:
            self.logger.info("Dependency tree not set. Building now...")
            start_time = time.time()
            self.ts.set_relation_tree()
            self.logger.debug(f"Tree build took {time.time() - start_time:.2f}s.")
            self.ts.set_dependencies_df()
            dependencies_df = self.ts.dependencies_df

        if dependencies_df.empty:
            self.logger.debug("No dependencies to update.")
            return

        # 3. Build a map of dependency instances if needed for debug mode
        update_map = {}
        if self.debug_mode and use_state_for_update:
            update_map = self._get_update_map(declared_dependencies, logger=self.logger)

        # 4. Delegate to the appropriate execution method
        self.logger.debug(f"Starting update for {len(dependencies_df)} dependencies...")

        dependencies_df = dependencies_df[dependencies_df["source_class_name"] != "WrapperDataNode"]
        if dependencies_df.empty:
            return
        if self.debug_mode:
            self._execute_sequential_debug_update(
                dependencies_df,
                update_map,
            )
        else:
            self._execute_parallel_distributed_update(dependencies_df)

        self.logger.debug(f"Dependency tree evaluation complete for {self.ts}.")

    def _get_update_map(
        self,
        declared_dependencies: dict[str, "DataNode"],
        logger: object,
        dependecy_map: dict | None = None,
    ) -> dict[tuple[str, int], dict[str, Any]]:
        """
        Obtains all DataNode objects in the dependency graph by recursively
        calling the dependencies() method.

        This approach is more robust than introspecting class members as it relies
        on an explicit declaration of dependencies.

        Args:
            time_serie_instance: The DataNode instance from which to start the dependency traversal.
            dependecy_map: An optional dictionary to store the dependency map, used for recursion.

        Returns:
            A dictionary mapping (update_hash, data_source_id) to DataNode info.
        """
        # Initialize the map on the first call
        if dependecy_map is None:
            dependecy_map = {}

        # Get the explicitly declared dependencies, just like set_relation_tree

        for name, dependency_ts in declared_dependencies.items():
            key = (dependency_ts.update_hash, dependency_ts.data_source_id)

            # If we have already processed this node, skip it to prevent infinite loops
            if key in dependecy_map:
                continue
            if dependency_ts.is_api == True:
                continue

            # Ensure the dependency is initialized in the persistence layer
            dependency_ts.local_persist_manager

            logger.debug(f"Adding dependency '{name}' to update map.")
            dependecy_map[key] = {"is_pickle": False, "ts": dependency_ts}
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
        update_map: dict[tuple[str, int], dict],
    ) -> None:
        """Runs dependency updates sequentially in the same process for debugging."""
        self.logger.info("Executing dependency updates in sequential debug mode.")
        # Sort by priority to respect the DAG execution order
        sorted_priorities = sorted(dependencies_df["update_priority"].unique())

        def refresh_update_statistics_of_deps(ts):
            for _, ts_dep in ts.dependencies().items():
                ts_dep.update_statistics = (
                    ts_dep.local_persist_manager.get_update_statistics_for_table()
                )

        for priority in sorted_priorities:
            priority_df = dependencies_df[dependencies_df["update_priority"] == priority]
            # Sort by number of upstreams to potentially optimize within a priority level
            sorted_deps = priority_df.sort_values("number_of_upstreams", ascending=False)

            for _, ts_row in sorted_deps.iterrows():
                key = (ts_row["update_hash"], ts_row["data_source_id"])
                ts_to_update = None
                try:
                    if key in update_map:
                        ts_to_update = update_map[key]["ts"]

                        # update the update_statistics of the dependencies
                        refresh_update_statistics_of_deps(ts_to_update)

                    else:
                        # If not in the map, it must be rebuilt from storage
                        ts_to_update, _ = build_operations.rebuild_and_set_from_update_hash(
                            update_hash=key[0], data_source_id=key[1]
                        )

                    if ts_to_update:
                        self.logger.debug(
                            f"Running debug update for dependency: {ts_to_update.update_hash}"
                        )
                        # Each dependency gets its own clean runner
                        dep_runner = UpdateRunner(
                            time_serie=ts_to_update,
                            debug_mode=True,
                            update_tree=False,  # We only update one node at a time
                            force_update=self.force_update,
                            remote_scheduler=self.scheduler,
                        )
                        dep_runner._setup_scheduler()

                        dep_runner._start_update(
                            use_state_for_update=False,
                        )
                except Exception as e:
                    self.logger.exception(f"Failed to update dependency {key[0]}")
                    raise e  # Re-raise to halt the entire process on failure

        # refresh update statistics of direct dependencies
        # for edge case of multicolumn self update
        self.ts.local_persist_manager.synchronize_data_node_update(None)
        us = self.ts.local_persist_manager.get_update_statistics_for_table()
        self.ts.update_statistics = us

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
                span.set_attribute("storage_hash", self.ts.storage_hash)
                span.set_attribute("head_scheduler", self.scheduler.name)

                # 3. Prepare the execution environment (Ray actors, dependency metadata)
                _ = self._setup_execution_environment()
                self.logger.debug("Execution environment and dependency metadata are set.")

                # 4. Wait for the scheduled update time, if not forcing an immediate run
                if not self.force_update:
                    self.ts.data_node_update.wait_for_update_time()

                # 5. Trigger the core update process
                error_on_last_update, updated_df = self._start_update(
                    use_state_for_update=True, override_update_stats=self.override_update_stats
                )

                return error_on_last_update, updated_df

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
