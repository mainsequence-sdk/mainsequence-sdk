# Standard Library Imports
from __future__ import annotations

import datetime
import gc
import json
import time
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid4

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
    from .updaters import TimeIndexTableUpdater


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


def _physical_table_name(output_table: Any) -> str | None:
    if output_table is None:
        return None
    if isinstance(output_table, Mapping):
        table_name = output_table.get("physical_table_name")
    else:
        table_name = getattr(output_table, "physical_table_name", None)
        if table_name in (None, ""):
            table = getattr(output_table, "__table__", None)
            table_name = getattr(table, "name", None)
    return str(table_name) if isinstance(table_name, str) and table_name else None


def _require_uid(obj: Any, object_name: str) -> str:
    uid = getattr(obj, "uid", None)
    if uid in (None, ""):
        raise ValueError(f"{object_name} must have a uid.")
    return str(uid)


class UpdateRunner:
    """
    Executes the update lifecycle for a TimeIndexTableUpdater instance.

    It handles dependency resolution, execution, and error reporting in the
    current process.
    """

    def __init__(
        self,
        table_updater: TimeIndexTableUpdater,
        update_tree: bool = True,
        update_only_tree: bool = False,
        override_update_stats: BaseUpdateStatistics | None = None,
        execution_trace_id: str | None = None,
    ):
        self.updater = table_updater
        self.logger = self.updater.logger
        self.update_tree = update_tree
        self.update_only_tree = update_only_tree
        if self.update_tree:
            self.update_only_tree = False

        self.override_update_stats = override_update_stats
        self.execution_trace_id = execution_trace_id or str(uuid4())

    def _pre_update_routines(self, table_update: dict | None = None) -> tuple[dict[str, Any], Any]:
        """
        Synchronize the head updater, ensure the dependency graph is registered,
        and fetch the latest typed update objects for the full dependency tree.

        This method:

        - synchronizes the head updater through its update manager,
        - ensures dependency relations and dependency dataframes are loaded,
        - fetches the latest backend update objects using
          ``self.updater.TABLE_UPDATE_CLASS.get_table_updates_and_set_updates(...)``.

        Args:
            table_update: Optional already-fetched backend payload for the
                head updater, used to synchronize local state before loading the
                full tree.

        Returns:
            A tuple containing:

            - a mapping of updater uid -> typed update object for the full tree
            - the backend ``state_data`` payload returned by the update class
        """
        # 1. Synchronize the root updater and load its dependency structure.
        self.updater.update_manager.synchronize_table_update(table_update=table_update)
        self.updater.set_relation_tree()

        # The `load_dependencies` logic is now integrated here.
        if self.updater.dependencies_df is None:
            self.updater.set_dependencies_df()

        if self.update_tree:
            self._ensure_dependency_tree_matches_current_declarations()

        # 2. Collect all UIDs in the dependency graph to fetch their metadata.
        # This correctly initializes the list, fixing the original bug.
        if not self.updater.depth_df.empty:
            dependency_columns = ["update_node_uid", "node_type", "update_hash"]
            if "physical_table_name" in self.updater.depth_df.columns:
                dependency_columns.append("physical_table_name")
            update_nodes_in_tree = self.updater.depth_df[dependency_columns].to_dict("records")
            for update_node in update_nodes_in_tree:
                update_node["uid"] = str(update_node.pop("update_node_uid"))
        else:
            update_nodes_in_tree = []

        # Always include the root updater itself.
        update_nodes_in_tree.append(
            {
                "uid": _require_uid(self.updater.table_update, "TimeIndexTableUpdate"),
                "update_hash": self.updater.table_update.update_hash,
                "physical_table_name": _physical_table_name(
                    self.updater.update_manager.output_metadata
                ),
                "node_type": self.updater.table_update.NODE_TYPE,
            }
        )

        # 3. Fetch the latest metadata for the entire tree from the backend.
        update_details_batch = dict(
            error_on_last_update=False,
            active_update_status="Q",  # Assuming queue status is always set here
        )

        update_class = self.updater.TABLE_UPDATE_CLASS
        batch_response = update_class.get_table_updates_and_set_updates(
            update_nodes=update_nodes_in_tree,
            update_details_kwargs=update_details_batch,
            update_priority_dict=None,
        )

        # 4. Process and return the results.
        state_data = batch_response.state_data
        table_updates_list = batch_response.table_updates
        table_updates_map = {_require_uid(m, m.__class__.__name__): m for m in table_updates_list}

        return table_updates_map, state_data

    def _setup_execution_environment(self) -> dict[str, Any]:
        table_updates, state_data = self._pre_update_routines()
        return table_updates

    def _start_update(
        self,
        override_update_stats: BaseUpdateStatistics | None = None,
    ) -> tuple[bool, LocalUpdateResult]:
        """Execute one TimeIndexTableUpdater update, including pre/post routines."""
        table_update_run = self.updater.update_manager.table_update.set_start_of_execution(
            trace_id=self.execution_trace_id,
        )

        # Ensure metadata is fully loaded with relationship details before proceeding.
        self.updater.update_manager.set_table_update_lazy(include_relations_detail=True)

        if override_update_stats is not None:
            self.updater.update_statistics = override_update_stats
        else:
            update_statistics = table_update_run.update_statistics
            # The TimeIndexTableUpdater defines how to scope its statistics
            self.updater._set_update_statistics(update_statistics)

        update_result: LocalUpdateResult = None
        error_on_last_update = False
        try:
            self.logger.debug(f"Executing update for {self.updater}.")
            update_result = self._update_local(
                table_update_run=table_update_run,
            )
        except Exception as e:
            error_on_last_update = True
            raise e
        finally:
            self.updater.update_manager.table_update.set_end_of_execution(
                table_update_run_uid=table_update_run.uid,
                error_on_update=error_on_last_update,
            )

            # Always set last relations details after the run completes.
            self.updater.update_manager.set_table_update_lazy(include_relations_detail=True)

            self.updater.run_post_update_routines(error_on_last_update=error_on_last_update)

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
            df: The DataFrame returned from the TimeIndexTableUpdater's update method.

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
        table_update_run: Any,
    ) -> LocalUpdateResult:
        """
        Calculates, validates, and persists the updater result.

        Args:
            table_update_run: Backend execution record returned by
                ``set_start_of_execution()`` for this run. The updater-specific
                ``_execute_local_update(...)`` implementation is responsible for
                interpreting any fields on this object.
            Dependencies are executed from the source-declared in-memory graph.
        """
        # 1. Handle dependency tree update first
        if self.update_tree:
            self._verify_tree_is_updated()
            if self.update_only_tree:
                self.logger.info(
                    f"Dependency tree for {self.updater} updated. Halting run as requested."
                )
                return None

        # 2. Execute the core data calculation
        with tracer.start_as_current_span("Update Calculation") as update_span:
            try:
                update_result = self.updater._execute_local_update(
                    table_update_run=table_update_run,
                )
                update_span.set_status(Status(StatusCode.OK))
                return update_result

            except Exception as e:
                self.logger.exception("Failed during update calculation or persistence.")
                update_span.set_status(Status(StatusCode.ERROR, description=str(e)))
                raise e
            finally:
                self.updater.update_manager.synchronize_table_update(None)
                us = self.updater.update_manager.get_update_statistics_for_table()
                self.updater.update_statistics = us

    @tracer.start_as_current_span("UpdateRunner._verify_tree_is_updated")
    def _verify_tree_is_updated(self) -> None:
        """
        Ensures all executable dependencies are updated before the root updater.

        This method checks if the dependency graph is defined in the backend and
        then executes dependencies sequentially in priority order.

        Dependencies are executed from the currently declared TimeIndexTableUpdater graph.
        Backend dependency metadata is ordering/state only; it is not used to
        cold-rebuild executable TimeIndexTableUpdater instances.
        """
        _, dependencies_df, update_map = self._ensure_dependency_tree_matches_current_declarations()

        if dependencies_df.empty:
            self.logger.debug("No dependencies to update.")
            return

        self.logger.debug(f"Starting update for {len(dependencies_df)} dependencies...")
        self._execute_dependencies_sequentially(
            dependencies_df,
            update_map,
        )

        self.logger.debug(f"Dependency tree evaluation complete for {self.updater}.")

    def _ensure_dependency_tree_matches_current_declarations(
        self,
    ) -> tuple[dict[str, TimeIndexTableUpdater], pd.DataFrame, dict[str, dict[str, Any]]]:
        declared_dependencies = self.updater.dependencies() or {}

        if self._has_uninitialized_update_dependencies(declared_dependencies):
            self.updater.update_manager.set_ogm_dependencies_unlinked()
            dependencies_df = self._rebuild_dependency_tree_from_current_declarations(
                force_rebuild=True,
                reason="Dependency declarations include uninitialized update nodes.",
            )
            declared_dependencies = self.updater.dependencies() or {}
        elif not self.updater.update_manager.table_update.ogm_dependencies_linked:
            dependencies_df = self._rebuild_dependency_tree_from_current_declarations(
                force_rebuild=False,
                reason="Dependency tree not set. Building now.",
            )
            declared_dependencies = self.updater.dependencies() or {}
        else:
            dependencies_df = self._dependencies_df_or_empty(self.updater.dependencies_df)

        update_map = self._get_update_map(declared_dependencies, logger=self.logger)
        mismatch = self._dependency_tree_mismatch(
            dependencies_df=dependencies_df,
            update_map=update_map,
        )
        if self._mismatch_is_empty(mismatch):
            return declared_dependencies, dependencies_df, update_map

        self._log_dependency_tree_rebuild(mismatch)
        dependencies_df = self._rebuild_dependency_tree_from_current_declarations(
            force_rebuild=True,
            reason="Backend dependency tree drift detected.",
        )
        declared_dependencies = self.updater.dependencies() or {}
        update_map = self._get_update_map(declared_dependencies, logger=self.logger)
        mismatch = self._dependency_tree_mismatch(
            dependencies_df=dependencies_df,
            update_map=update_map,
        )
        if not self._mismatch_is_empty(mismatch):
            raise DependencyUpdateError(
                self._dependency_tree_mismatch_message(
                    mismatch,
                    after_automatic_refresh=True,
                )
            )

        return declared_dependencies, dependencies_df, update_map

    @staticmethod
    def _has_uninitialized_update_dependencies(
        declared_dependencies: dict[str, Any],
    ) -> bool:
        return any(
            getattr(dependency, "dependency_kind", None) == "table_update"
            and getattr(dependency, "table_update", None) is None
            for dependency in declared_dependencies.values()
        )

    def _rebuild_dependency_tree_from_current_declarations(
        self,
        *,
        force_rebuild: bool,
        reason: str,
    ) -> pd.DataFrame:
        self.logger.info(reason)
        start_time = time.time()
        self.updater.set_relation_tree(force_rebuild=force_rebuild)
        self.logger.debug(f"Tree build took {time.time() - start_time:.2f}s.")
        self.updater.set_dependencies_df()
        return self._dependencies_df_or_empty(self.updater.dependencies_df)

    @staticmethod
    def _dependencies_df_or_empty(dependencies_df: pd.DataFrame | None) -> pd.DataFrame:
        if dependencies_df is None:
            return pd.DataFrame()
        if not dependencies_df.empty and "update_node_uid" not in dependencies_df.columns:
            raise ValueError("Dependency dataframe must include 'update_node_uid'.")
        return dependencies_df

    @staticmethod
    def _dependency_tree_mismatch(
        *,
        dependencies_df: pd.DataFrame,
        update_map: dict[str, dict[str, Any]],
    ) -> dict[str, list[str]]:
        if not dependencies_df.empty and "update_node_uid" not in dependencies_df.columns:
            raise ValueError("Dependency dataframe must include 'update_node_uid'.")
        backend_uids = (
            set(dependencies_df["update_node_uid"].astype(str).to_list())
            if not dependencies_df.empty
            else set()
        )
        declared_uids = set(update_map)
        return {
            "stale_backend_uids": sorted(backend_uids - declared_uids),
            "missing_backend_uids": sorted(declared_uids - backend_uids),
        }

    @staticmethod
    def _mismatch_is_empty(mismatch: dict[str, list[str]]) -> bool:
        return not mismatch["stale_backend_uids"] and not mismatch["missing_backend_uids"]

    def _log_dependency_tree_rebuild(self, mismatch: dict[str, list[str]]) -> None:
        message = self._dependency_tree_mismatch_message(
            mismatch,
            after_automatic_refresh=False,
        )
        warning = getattr(self.logger, "warning", None)
        if callable(warning):
            warning(message)
        else:
            self.logger.info(message)

    @staticmethod
    def _dependency_tree_mismatch_message(
        mismatch: dict[str, list[str]],
        *,
        after_automatic_refresh: bool,
    ) -> str:
        stale_backend_uids = mismatch["stale_backend_uids"]
        missing_backend_uids = mismatch["missing_backend_uids"]
        message_parts = [
            "Backend dependency tree is out of sync with the current "
            "TimeIndexTableUpdater.dependencies() graph."
        ]
        if stale_backend_uids:
            message_parts.append(
                "Backend contains stale dependency update UIDs that current Python "
                f"code does not declare: {stale_backend_uids!r}."
            )
        if missing_backend_uids:
            message_parts.append(
                "Backend is missing dependency update UIDs declared by current "
                f"Python code: {missing_backend_uids!r}."
            )
        if after_automatic_refresh:
            message_parts.append(
                "The SDK already cleared and rebuilt the dependency tree once, but "
                "the backend response still does not match the current graph."
            )
        else:
            message_parts.append(
                "The SDK will clear and rebuild backend dependency edges from the "
                "current declarations before executing dependency updates."
            )
        return " ".join(message_parts)

    @staticmethod
    def _validate_dependency_tree_matches_update_map(
        *,
        dependencies_df: pd.DataFrame,
        update_map: dict[str, dict[str, Any]],
    ) -> None:
        mismatch = UpdateRunner._dependency_tree_mismatch(
            dependencies_df=dependencies_df,
            update_map=update_map,
        )
        if UpdateRunner._mismatch_is_empty(mismatch):
            return
        raise DependencyUpdateError(
            UpdateRunner._dependency_tree_mismatch_message(
                mismatch,
                after_automatic_refresh=True,
            )
        )

    def _get_update_map(
        self,
        declared_dependencies: dict[str, Any],
        logger: object,
        dependency_map: dict | None = None,
    ) -> dict[str, dict[str, Any]]:
        """
        Obtains all TimeIndexTableUpdater objects in the dependency graph by recursively
        calling the dependencies() method.

        This approach is more robust than introspecting class members as it relies
        on an explicit declaration of dependencies.

        Args:
            declared_dependencies: The explicit dependencies from which to start traversal.
            logger: Logger used for dependency traversal diagnostics.
            dependency_map: An optional dictionary to store the dependency map, used for recursion.

        Returns:
            A dictionary mapping update-process uid to TimeIndexTableUpdater info.
        """
        # Initialize the map on the first call
        if dependency_map is None:
            dependency_map = {}

        # Get the explicitly declared dependencies, just like set_relation_tree

        for name, dependency in declared_dependencies.items():
            dependency_kind = getattr(dependency, "dependency_kind", None)
            if dependency_kind == "time_index_table_ref":
                continue
            if dependency_kind != "table_update":
                raise TypeError(
                    f"Dependency {name!r} has unsupported dependency_kind {dependency_kind!r}."
                )

            # Ensure the dependency is initialized in the persistence layer.
            _ = dependency.update_manager
            key = _require_uid(dependency.table_update, "TimeIndexTableUpdate")

            # If we have already processed this node, skip it to prevent infinite loops
            if key in dependency_map:
                continue

            logger.debug(f"Adding dependency '{name}' to update map.")
            dependency_map[key] = {"updater": dependency}
            declared_dependencies = dependency.dependencies() or {}
            # Recursively call get_update_map on the dependency to traverse the entire graph
            self._get_update_map(
                declared_dependencies=declared_dependencies,
                logger=logger,
                dependency_map=dependency_map,
            )

        return dependency_map

    def _execute_dependencies_sequentially(
        self,
        dependencies_df: pd.DataFrame,
        update_map: dict[str, dict],
    ) -> None:
        """Run executable dependencies sequentially in the current process."""
        self.logger.info("Executing dependency updates sequentially.")
        # Sort by priority to respect the DAG execution order
        sorted_priorities = sorted(dependencies_df["update_priority"].unique())

        def refresh_update_statistics_of_deps(updater):
            for _, dependency in updater.dependencies().items():
                if dependency.dependency_kind == "time_index_table_ref":
                    continue
                dependency.update_statistics = (
                    dependency.update_manager.get_update_statistics_for_table()
                )

        for priority in sorted_priorities:
            priority_df = dependencies_df[dependencies_df["update_priority"] == priority]
            # Sort by number of upstreams to potentially optimize within a priority level
            sorted_deps = priority_df.sort_values("number_of_upstreams", ascending=False)

            for _, dependency_row in sorted_deps.iterrows():
                update_node_uid = str(dependency_row["update_node_uid"])
                try:
                    if update_node_uid not in update_map:
                        raise DependencyUpdateError(
                            "Backend dependency metadata includes an update node that "
                            "is not declared by the current TimeIndexTableUpdater.dependencies() graph: "
                            f"update_node_uid={update_node_uid!r}."
                        )

                    updater_to_update = update_map[update_node_uid]["updater"]
                    refresh_update_statistics_of_deps(updater_to_update)

                    self.logger.debug(
                        f"Running debug update for dependency: {updater_to_update.update_hash}"
                    )
                    # Each dependency gets its own clean runner.
                    dep_runner = UpdateRunner(
                        table_updater=updater_to_update,
                        update_tree=False,
                        execution_trace_id=self.execution_trace_id,
                    )
                    dep_runner._start_update()
                except Exception as e:
                    self.logger.exception(f"Failed to update dependency {update_node_uid}")
                    raise e  # Re-raise to halt the entire process on failure

        # refresh update statistics of direct dependencies

        refresh_update_statistics_of_deps(self.updater)

    def run(self) -> tuple[bool, LocalUpdateResult]:
        """
        Executes the full time-index table update lifecycle.

        This is the main entry point for the runner. It prepares the execution
        environment, triggers the core update process, and handles error reporting
        and cleanup.
        """
        # Initialize tracing and set initial flags
        tracer_instrumentator = TracerInstrumentator()
        tracer = tracer_instrumentator.build_tracer()
        error_to_raise = None

        try:
            self.updater.verify_and_build_remote_objects()
            cvars.bind_contextvars(
                table_update_trace_id=self.execution_trace_id,
                head_table_update_hash=self.updater.update_hash,
            )

            with tracer.start_as_current_span(
                f"TimeIndexTableUpdater Run: {self.updater.update_hash}"
            ) as span:
                span.set_attribute("table_update_hash", self.updater.update_hash)
                span.set_attribute("table_update_trace_id", self.execution_trace_id)
                physical_table_name = _physical_table_name(
                    self.updater.update_manager.output_metadata
                )
                if physical_table_name is not None:
                    span.set_attribute("physical_table_name", physical_table_name)
                # 1. Prepare the execution environment and dependency metadata.
                _ = self._setup_execution_environment()
                self.logger.debug("Execution environment and dependency metadata are set.")

                # 2. Trigger the core update process
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
            # Clean up temporary attributes on the TimeIndexTableUpdater instance
            if hasattr(self.updater, "update_tracker"):
                del self.updater.update_tracker

            gc.collect()

        # Re-raise any captured exception after cleanup
        if error_to_raise:
            raise error_to_raise
