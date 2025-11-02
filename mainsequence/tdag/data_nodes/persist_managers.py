import hashlib
import inspect
import json
import threading
from concurrent.futures import Future
from typing import Optional, List
import pandas as pd

import mainsequence.client as ms_client
from mainsequence.client import TDAG_CONSTANTS as CONSTANTS
from mainsequence.client import (
    DataNodeStorage,
    DataNodeUpdate,
    DynamicTableDataSource,
)
from mainsequence.client.models_tdag import DataNodeUpdateDetails
from mainsequence.instrumentation import tracer
from mainsequence.logconf import logger

from .. import future_registry


def get_data_node_source_code(DataNodeClass: "DataNode") -> str:
    """
    Gets the source code of a DataNode class.

    Args:
        DataNodeClass: The class to get the source code for.

    Returns:
        The source code as a string.
    """
    global logger
    try:
        # First try the standard approach.
        source = inspect.getsource(DataNodeClass)
        if source.strip():
            return source
    except Exception:
        logger.warning(
            "Your TimeSeries is not in a python module this will likely bring exceptions when running in a pipeline"
        )
    from IPython import get_ipython

    # Fallback: Scan IPython's input history.
    ip = get_ipython()  # Get the current IPython instance.
    if ip is not None:
        # Retrieve the full history as a single string.
        history = "\n".join(code for _, _, code in ip.history_manager.get_range())
        marker = f"class {DataNodeClass.__name__}"
        idx = history.find(marker)
        if idx != -1:
            return history[idx:]
    return "Source code unavailable."


def get_data_node_source_code_git_hash(DataNodeClass: "DataNode") -> str:
    """
    Hashes the source code of a DataNode class using SHA-1 (Git style).

    Args:
        DataNodeClass: The class to hash.

    Returns:
        The Git-style hash of the source code.
    """
    data_node_class_source_code = get_data_node_source_code(DataNodeClass)
    # Prepare the content for Git-style hashing
    # Git hashing format: "blob <size_of_content>\0<content>"
    content = f"blob {len(data_node_class_source_code)}\0{data_node_class_source_code}"
    # Compute the SHA-1 hash (Git hash)
    hash_object = hashlib.sha1(content.encode("utf-8"))
    git_hash = hash_object.hexdigest()
    return git_hash


class APIPersistManager:
    """
    Manages persistence for time series data accessed via an API.
    It handles asynchronous fetching of data_node_storage to avoid blocking operations.
    """

    def __init__(self, data_source_id: int, storage_hash: str):
        """
        Initializes the APIPersistManager.

        Args:
            data_source_id: The ID of the data source.
            update_hash: The local hash identifier for the time series.
        """
        self.data_source_id: int = data_source_id
        self.storage_hash: str = storage_hash

        logger.debug(f"Initializing Time Serie {self.storage_hash}  as APIDataNode")

        # Create a Future to hold the local metadata when ready.
        self._data_node_storage_future = Future()
        # Register the future globally.
        future_registry.add_future(self._data_node_storage_future)
        # Launch the REST request in a separate, non-daemon thread.
        thread = threading.Thread(
            target=self._init_data_node_storage,
            name=f"ApiDataNodeStorageThread-{self.storage_hash}",
            daemon=False,
        )
        thread.start()

    @property
    def data_node_storage(self) -> DataNodeStorage:
        """Lazily block and cache the result if needed."""
        if not hasattr(self, "_data_node_storage_cached"):
            # This call blocks until the future is resolved.
            self._data_node_storage_cached = self._data_node_storage_future.result()
        return self._data_node_storage_cached

    def _init_data_node_storage(self) -> None:
        """
        Performs the REST request to fetch local data_node_storage asynchronously.
        Sets the result or exception on the future object.
        """
        try:
            result = DataNodeStorage.get_or_none(
                storage_hash=self.storage_hash,
                data_source__id=self.data_source_id,
                include_relations_detail=True,
            )
            self._data_node_storage_future.set_result(result)
        except Exception as exc:
            self._data_node_storage_future.set_exception(exc)
        finally:
            # Remove the future from the global registry once done.
            future_registry.remove_future(self._data_node_storage_future)


    def get_last_observation(self,asset_list:Optional[List["Asset"]]):
        unique_identifier_list=[]
        if asset_list is not None:
            unique_identifier_list=[a.unique_identifier for a in asset_list]
        last_observation=self.data_node_storage.get_last_observation(unique_identifier_list=unique_identifier_list)
        return last_observation

    def get_df_between_dates(self, *args, **kwargs) -> pd.DataFrame:
        """
        Retrieves a DataFrame from the API between specified dates.

        Returns:
            A pandas DataFrame with the requested data.
        """
        filtered_data = self.data_node_storage.get_data_between_dates_from_api(*args, **kwargs)
        if filtered_data.empty:
            return filtered_data

        # fix types
        stc = self.data_node_storage.sourcetableconfiguration
        filtered_data[stc.time_index_name] = pd.to_datetime(
            filtered_data[stc.time_index_name], utc=True
        )
        column_filter = kwargs.get("columns") or stc.column_dtypes_map.keys()
        for c in column_filter:
            c_type = stc.column_dtypes_map[c]
            if c != stc.time_index_name:
                if c_type == "object":
                    c_type = "str"
                filtered_data[c] = filtered_data[c].astype(c_type)
        filtered_data = filtered_data.set_index(stc.index_names)

        return filtered_data


class PersistManager:
    def __init__(
        self,
        data_source: DynamicTableDataSource,
        update_hash: str,
        description: str | None = None,
        class_name: str | None = None,
        data_node_storage: dict | None = None,
        data_node_update: DataNodeUpdate | None = None,
    ):
        """
        Initializes the PersistManager.

        Args:
            data_source: The data source for the time series.
            update_hash: The local hash identifier for the time series.
            description: An optional description for the time series.
            class_name: The name of the DataNode class.
            data_node_storage: Optional remote data_node_storage dictionary.
            data_node_update: Optional local data_node_storage object.
        """
        self.data_source: DynamicTableDataSource = data_source
        self.update_hash: str = update_hash
        if data_node_update is not None and data_node_storage is None:
            # query remote storage_hash
            data_node_storage = data_node_update.data_node_storage
        self.description: str | None = description
        self.logger = logger

        self.table_model_loaded: bool = False
        self.class_name: str | None = class_name

        # Private members for managing lazy asynchronous retrieval.
        self._data_node_update_future: Future | None = None
        self._data_node_update_cached: DataNodeUpdate | None = None
        self._data_node_update_lock = threading.Lock()
        self._data_node_storage_cached: DataNodeStorage | None = None

        if self.update_hash is not None:
            self.synchronize_data_node_update(data_node_update=data_node_update)

    def synchronize_data_node_update(self, data_node_update: DataNodeUpdate | None) -> None:
        if data_node_update is not None:
            self.set_data_node_update(data_node_update)
        else:
            self.set_data_node_update_lazy(force_registry=True, include_relations_detail=True)

    @classmethod
    def get_from_data_type(
        cls, data_source: DynamicTableDataSource, *args, **kwargs
    ) -> "PersistManager":
        """
        Factory method to get the correct PersistManager based on data source type.

        Args:
            data_source: The data source object.

        Returns:
            An instance of a PersistManager subclass.
        """
        data_type = data_source.related_resource_class_type
        if data_type in CONSTANTS.DATA_SOURCE_TYPE_TIMESCALEDB:
            return TimeScaleLocalPersistManager(data_source=data_source, *args, **kwargs)
        else:
            return TimeScaleLocalPersistManager(data_source=data_source, *args, **kwargs)

    def set_data_node_update(self, data_node_update: DataNodeUpdate) -> None:
        """
        Caches the local data_node_storage object for lazy queries

        Args:
            data_node_update: The DataNodeUpdate object to cache.
        """
        self._data_node_update_cached = data_node_update

    @property
    def data_node_update(self) -> DataNodeUpdate:
        """Lazily block and retrieve the local metadata, caching the result."""
        with self._data_node_update_lock:
            if self._data_node_update_cached is None:
                if self._data_node_update_future is None:
                    # If no future is running, start one.
                    self.set_data_node_update_lazy(force_registry=True)
                # Block until the future completes and cache its result.
                data_node_update = self._data_node_update_future.result()
                self.set_data_node_update(data_node_update)
            return self._data_node_update_cached

            # Define a callback that will launch set_local_data_node_lazy after the remote update is complete.

    @property
    def data_node_storage(self) -> DataNodeStorage | None:
        """
        Lazily retrieves and returns the remote data_node_storage.
        """
        if self.data_node_update is None:
            return None
        if self.data_node_update.data_node_storage is not None:
            if self.data_node_update.data_node_storage.sourcetableconfiguration is not None:
                if (
                    self.data_node_update.data_node_storage.build_meta_data.get(
                        "initialize_with_default_partitions", True
                    )
                    == False
                ):
                    if (
                        self.data_node_update.data_node_storage.data_source.related_resource_class_type
                        in CONSTANTS.DATA_SOURCE_TYPE_TIMESCALEDB
                    ):
                        self.logger.warning("Default Partitions will not be initialized ")

        return self.data_node_update.data_node_storage

    @property
    def local_build_configuration(self) -> dict:
        return self.data_node_update.build_configuration

    @property
    def local_build_metadata(self) -> dict:
        return self.data_node_update.build_meta_data

    def set_data_node_update_lazy_callback(self, fut: Future) -> None:
        """
        Callback to handle the result of an asynchronous task and trigger a metadata refresh.
        """
        try:
            # This will re-raise any exception that occurred in _update_task.
            fut.result()
        except Exception as exc:
            # Optionally, handle or log the error if needed.
            # For example: logger.error("Remote build update failed: %s", exc)
            raise exc
        # Launch the local metadata update regardless of the outcome.
        self.set_data_node_update_lazy(force_registry=True)

    def set_data_node_update_lazy(
        self, force_registry: bool = True, include_relations_detail: bool = True
    ) -> None:
        """
        Initiates a lazy, asynchronous fetch of the local data_node_update.

        Args:
            force_registry: If True, forces a refresh even if cached data exists.
            include_relations_detail: If True, includes relationship details in the fetch.
        """
        with self._data_node_update_lock:
            if force_registry:
                self._data_node_update_cached = None
            # Capture the new future in a local variable.
            new_future = Future()
            self._data_node_update_future = new_future
            # Register the new future.
            future_registry.add_future(new_future)

        def _get_or_none_data_node_update():
            """Perform the REST request asynchronously."""
            try:
                result = DataNodeUpdate.get_or_none(
                    update_hash=self.update_hash,
                    remote_table__data_source__id=self.data_source.id,
                    include_relations_detail=include_relations_detail,
                )
                if result is None:
                    self.logger.warning(
                        f"TimeSeries {self.update_hash} with data source {self.data_source.id} not found in backend"
                    )
                new_future.set_result(result)
            except Exception as exc:
                new_future.set_exception(exc)
            finally:
                # Remove the future from the global registry once done.
                future_registry.remove_future(new_future)

        thread = threading.Thread(
            target=_get_or_none_data_node_update,
            name=f"LocalDataNodeStorageThreadPM-{self.update_hash}",
            daemon=False,
        )
        thread.start()

    def depends_on_connect(self, new_ts: "DataNode", is_api: bool) -> None:
        """
        Connects a time series as a relationship in the DB.

        Args:
            new_ts: The target DataNode to connect to.
            is_api: True if the target is an APIDataNode
        """
        if not is_api:
            self.data_node_update.depends_on_connect(
                target_time_serie_id=new_ts.data_node_update.id
            )
        else:
            try:
                self.data_node_update.depends_on_connect_to_api_table(
                    target_table_id=new_ts.local_persist_manager.data_node_storage.id
                )
            except Exception as exc:
                raise exc

    def display_mermaid_dependency_diagram(self) -> str:
        """
        Generates and returns an HTML string for a Mermaid dependency diagram.

        Returns:
            An HTML string containing the Mermaid diagram and supporting Javascript.
        """

        response = ms_client.TimeSerieLocalUpdate.get_mermaid_dependency_diagram(
            update_hash=self.update_hash, data_source_id=self.data_source.id
        )
        mermaid_chart = response.get("mermaid_chart")
        metadata = response.get("metadata")
        # Render Mermaid.js diagram with metadata display
        html_template = f"""
           <div class="mermaid">
           {mermaid_chart}
           </div>
           <div id="metadata-display" style="margin-top: 20px; font-size: 16px; color: #333;"></div>
           <script>
               // Initialize Mermaid.js
               if (typeof mermaid !== 'undefined') {{
                   mermaid.initialize({{ startOnLoad: true }});
               }}

               // Metadata dictionary
               const metadata = {metadata};

               // Attach click listeners to nodes
               document.addEventListener('click', function(event) {{
                   const target = event.target.closest('div[data-graph-id]');
                   if (target) {{
                       const nodeId = target.dataset.graphId;
                       const metadataDisplay = document.getElementById('metadata-display');
                       if (metadata[nodeId]) {{
                           metadataDisplay.innerHTML = "<strong>Node Metadata:</strong> " + metadata[nodeId];
                       }} else {{
                           metadataDisplay.innerHTML = "<strong>No metadata available for this node.</strong>";
                       }}
                   }}
               }});
           </script>
           """

        return mermaid_chart

    def get_mermaid_dependency_diagram(self) -> str:
        """
        Displays a Mermaid.js dependency diagram in a Jupyter environment.

        Returns:
            The Mermaid diagram string.
        """
        from IPython.display import HTML, display

        mermaid_diagram = self.display_mermaid_dependency_diagram()

        # Mermaid.js initialization script (only run once)
        if not hasattr(display, "_mermaid_initialized"):
            mermaid_initialize = """
                   <script src="https://cdn.jsdelivr.net/npm/mermaid/dist/mermaid.min.js"></script>
                   <script>
                       function initializeMermaid() {
                           if (typeof mermaid !== 'undefined') {
                               console.log('Initializing Mermaid.js...');
                               const mermaidDivs = document.querySelectorAll('.mermaid');
                               mermaidDivs.forEach(mermaidDiv => {
                                   mermaid.init(undefined, mermaidDiv);
                               });
                           } else {
                               console.error('Mermaid.js is not loaded.');
                           }
                       }
                   </script>
                   """
            display(HTML(mermaid_initialize))
            display._mermaid_initialized = True

        # HTML template for rendering the Mermaid diagram
        html_template = f"""
               <div class="mermaid">
               {mermaid_diagram}
               </div>
               <script>
                   initializeMermaid();
               </script>
               """

        # Display the Mermaid diagram in the notebook
        display(HTML(html_template))

        # Optionally return the raw diagram code for further use
        return mermaid_diagram

    def get_all_dependencies_update_priority(self) -> pd.DataFrame:
        """
        Retrieves a DataFrame of all dependencies with their update priority.

        Returns:
            A pandas DataFrame with dependency and priority information.
        """
        depth_df = self.data_node_update.get_all_dependencies_update_priority()
        return depth_df

    def set_ogm_dependencies_linked(self) -> None:
        self.data_node_update.patch(ogm_dependencies_linked=True)

    @property
    def update_details(self) -> DataNodeUpdateDetails | None:
        """Returns the update details associated with the local time series."""
        return self.data_node_update.update_details

    @property
    def run_configuration(self) -> dict | None:
        """Returns the run configuration from the local metadata."""
        return self.data_node_update.run_configuration

    @property
    def source_table_configuration(self) -> dict | None:
        """Returns the source table configuration from the remote metadata."""
        if "sourcetableconfiguration" in self.data_node_storage.keys():
            return self.data_node_storage["sourcetableconfiguration"]
        return None

    def update_source_informmation(self, git_hash_id: str, source_code: str) -> None:
        """
        Updates the source code and git hash for the remote table.
        """
        self.data_node_update.data_node_storage = self.data_node_storage.patch(
            time_serie_source_code_git_hash=git_hash_id,
            time_serie_source_code=source_code,
        )

    def add_tags(self, tags: list[str]) -> None:
        """Adds tags to the local time series metadata if they don't already exist."""
        if any([t not in self.data_node_update.tags for t in tags]) == True:
            self.data_node_update.add_tags(tags=tags)

    @property
    def persist_size(self) -> int:
        """Returns the size of the persisted table, or 0 if not available."""
        try:
            return self.data_node_storage["table_size"]
        except KeyError:
            return 0

    def time_serie_exist(self) -> bool:
        """Checks if the remote metadata for the time series exists."""
        if hasattr(self, "data_node_storage"):
            return True
        return False

    def patch_build_configuration(
        self, local_configuration: dict, remote_configuration: dict, remote_build_metadata: dict
    ) -> None:
        """
        Asynchronously patches the build configuration for the remote and local tables.

        Args:
            local_configuration: The build configuration for the local time series.
            remote_configuration: The build configuration for the remote table.
            remote_build_metadata: The build metadata for the remote table.
        """
        # This ensures that later accesses to data_node_update will block for the new value.
        with self._data_node_update_lock:
            self._data_node_update_future = Future()
            future_registry.add_future(self._data_node_update_future)

        kwargs = dict(
            build_configuration=remote_configuration,
        )

        data_node_update_kwargs = dict(
            update_hash=self.update_hash,
            build_configuration=local_configuration,
        )

        patch_future = Future()
        future_registry.add_future(patch_future)

        # Define the inner helper function.
        def _patch_build_configuration():
            """Helper function for patching build configuration asynchronously."""
            try:
                # Execute the patch operation; this method is expected to return a DataNodeUpdate-like instance.
                result = DataNodeStorage.patch_build_configuration(
                    remote_table_patch=kwargs,
                    data_source_id=self.data_source.id,
                    build_meta_data=remote_build_metadata,
                    local_table_patch=data_node_update_kwargs,
                )
                patch_future.set_result(True)  # success
            except Exception as exc:
                patch_future.set_exception(exc)
            finally:
                # Once the operation is complete (or errors out), remove the future from the global registry.
                future_registry.remove_future(result)

        thread = threading.Thread(
            target=_patch_build_configuration,
            name=f"PatchBuildConfigThread-{self.update_hash}",
            daemon=False,
        )
        thread.start()

        patch_future.add_done_callback(self.set_data_node_update_lazy_callback)

    def local_persist_exist_set_config(
        self,
        storage_hash: str,
        local_configuration: dict,
        remote_configuration: dict,
        data_source: DynamicTableDataSource,
        time_serie_source_code_git_hash: str,
        time_serie_source_code: str,
        build_configuration_json_schema: dict,
            open_to_public:bool
    ) -> None:
        """
        Ensures local and remote persistence objects exist and sets their configurations.
        This runs on DataNode initialization.
        """
        remote_build_configuration = None
        if hasattr(self, "remote_build_configuration"):
            remote_build_configuration = self.remote_build_configuration

        if remote_build_configuration is None:
            logger.debug(f"remote table {storage_hash} does not exist creating")
            # create remote table

            try:

                # table may not exist but
                remote_build_metadata = (
                    remote_configuration["build_meta_data"]
                    if "build_meta_data" in remote_configuration.keys()
                    else {}
                )
                remote_configuration.pop("build_meta_data", None)
                kwargs = dict(
                    storage_hash=storage_hash,
                    time_serie_source_code_git_hash=time_serie_source_code_git_hash,
                    time_serie_source_code=time_serie_source_code,
                    build_configuration=remote_configuration,
                    data_source=data_source.model_dump(),
                    build_meta_data=remote_build_metadata,
                    build_configuration_json_schema=build_configuration_json_schema,
                    open_to_public=open_to_public
                )

                dtd_metadata = DataNodeStorage.get_or_create(**kwargs)
                storage_hash = dtd_metadata.storage_hash
            except Exception as e:
                self.logger.exception(f"{storage_hash} Could not set meta data in DB for P")
                raise e
        else:
            self.set_data_node_update_lazy(force_registry=True, include_relations_detail=True)
            storage_hash = self.metadata.storage_hash

        local_table_exist = self._verify_local_ts_exists(
            storage_hash=storage_hash, local_configuration=local_configuration
        )

    def _verify_local_ts_exists(
        self, storage_hash: str, local_configuration: dict | None = None
    ) -> None:
        """
        Verifies that the local time series exists in the ORM, creating it if necessary.
        """
        local_build_configuration = None
        if self.data_node_update is not None:
            local_build_configuration, local_build_metadata = (
                self.local_build_configuration,
                self.local_build_metadata,
            )
        if local_build_configuration is None:

            logger.debug(f"data_node_update {self.update_hash} does not exist creating")
            local_update = DataNodeUpdate.get_or_none(
                update_hash=self.update_hash, remote_table__data_source__id=self.data_source.id
            )
            if local_update is None:
                local_build_metadata = (
                    local_configuration["build_meta_data"]
                    if "build_meta_data" in local_configuration.keys()
                    else {}
                )
                local_configuration.pop("build_meta_data", None)
                metadata_kwargs = dict(
                    update_hash=self.update_hash,
                    build_configuration=local_configuration,
                    remote_table__hash_id=storage_hash,
                    data_source_id=self.data_source.id,
                )

                data_node_update = DataNodeUpdate.get_or_create(
                    **metadata_kwargs,
                )
            else:
                data_node_update = local_update

            self.set_data_node_update(data_node_update=data_node_update)

    def _verify_insertion_format(self, temp_df: pd.DataFrame) -> None:
        """
        Verifies that a DataFrame is properly configured for insertion.
        """
        if isinstance(temp_df.index, pd.MultiIndex) == True:
            assert temp_df.index.names == ["time_index", "asset_symbol"] or temp_df.index.names == [
                "time_index",
                "asset_symbol",
                "execution_venue_symbol",
            ]

    def build_update_details(self, source_class_name: str) -> None:
        """
        Asynchronously builds or updates the update details for the time series.
        """
        update_kwargs = dict(
            source_class_name=source_class_name,
            local_metadata=json.loads(self.data_node_update.model_dump_json()),
        )
        # This ensures that later accesses to data_node_update will block for the new value.
        with self._data_node_update_lock:
            self._data_node_update_future = Future()
            future_registry.add_future(self._data_node_update_future)

        # Create a future for the remote update task and register it.
        future = Future()
        future_registry.add_future(future)

        def _update_task():
            try:
                # Run the remote build/update details task.
                self.data_node_update.data_node_storage.build_or_update_update_details(
                    **update_kwargs
                )
                future.set_result(True)  # Signal success
            except Exception as exc:
                future.set_exception(exc)
            finally:
                # Unregister the future once the task completes.
                future_registry.remove_future(future)

        thread = threading.Thread(
            target=_update_task, name=f"BuildUpdateDetailsThread-{self.update_hash}", daemon=False
        )
        thread.start()

        # Attach the callback to the future.
        future.add_done_callback(self.set_data_node_update_lazy_callback)

    def patch_table(self, **kwargs) -> None:
        """Patches the remote metadata table with the given keyword arguments."""
        self.data_node_storage.patch(**kwargs)

    def protect_from_deletion(self, protect_from_deletion: bool = True) -> None:
        """Sets the 'protect_from_deletion' flag on the remote metadata."""
        self.data_node_storage.patch(protect_from_deletion=protect_from_deletion)

    def open_for_everyone(self, open_for_everyone: bool = True) -> None:
        """Sets the 'open_for_everyone' flag on local, remote, and source table configurations."""
        if not self.data_node_update.open_for_everyone:
            self.data_node_update.patch(open_for_everyone=open_for_everyone)

        if not self.data_node_storage.open_for_everyone:
            self.data_node_storage.patch(open_for_everyone=open_for_everyone)

        if not self.data_node_storage.sourcetableconfiguration.open_for_everyone:
            self.data_node_storage.sourcetableconfiguration.patch(open_for_everyone=open_for_everyone)

    def get_df_between_dates(self, *args, **kwargs) -> pd.DataFrame:
        """
        Retrieves a DataFrame from the data source between specified dates.
        """
        filtered_data = self.data_source.get_data_by_time_index(
            data_node_update=self.data_node_update, *args, **kwargs
        )
        return filtered_data
    def get_last_observation(self,asset_list:Optional[List["Asset"]]):
        unique_identifier_list=[]
        if asset_list is not None:
            unique_identifier_list=[a.unique_identifier for a in asset_list]
        last_observation=self.data_node_storage.get_last_observation(unique_identifier_list=unique_identifier_list)
        return last_observation


    def set_column_metadata(self, columns_metadata: list[ms_client.ColumnMetaData] | None) -> None:
        if self.data_node_storage:
            if self.data_node_storage.sourcetableconfiguration != None:
                if self.data_node_storage.sourcetableconfiguration.columns_metadata is not None:
                    if columns_metadata is None:
                        self.logger.info("get_column_metadata method not implemented")
                        return

                    self.data_node_storage.sourcetableconfiguration.set_or_update_columns_metadata(
                        columns_metadata=columns_metadata
                    )

    def set_table_metadata(
        self,
        table_metadata: ms_client.TableMetaData,
    ):
        """
        Creates or updates the MarketsTimeSeriesDetails metadata in the backend.

        This method orchestrates the synchronization of the time series metadata,
        including its description, frequency, and associated assets, based on the
        configuration returned by `_get_time_series_meta_details`.
        """
        if not (self.data_node_storage):
            self.logger.warning("metadata not set")
            return

        # 1. Get the user-defined metadata configuration for the time series.
        if table_metadata is None:
            return

        # 2. Get or create the MarketsTimeSeriesDetails object in the backend.
        source_table_id = self.data_node_storage.patch(**table_metadata.model_dump())

    def delete_table(self) -> None:
        if self.data_source.related_resource.class_type == "duck_db":
            from mainsequence.client.data_sources_interfaces.duckdb import DuckDBInterface

            db_interface = DuckDBInterface()
            db_interface.drop_table(self.data_node_storage.storage_hash)

        self.data_node_storage.delete()

    @tracer.start_as_current_span("TS: Persist Data")
    def persist_updated_data(self, temp_df: pd.DataFrame, overwrite: bool = False) -> bool:
        """
        Persists the updated data to the database.

        Args:
            temp_df: The DataFrame with updated data.
            update_tracker: The update tracker object.
            overwrite: If True, overwrites existing data.

        Returns:
            True if data was persisted, False otherwise.
        """
        persisted = False
        if not temp_df.empty:
            if overwrite == True:
                self.logger.warning("Values will be overwritten")

            self._data_node_update_cached = self.data_node_update.upsert_data_into_table(
                data=temp_df,
                data_source=self.data_source,overwrite=overwrite
            )

            persisted = True
        return persisted

    def get_update_statistics_for_table(self) -> ms_client.UpdateStatistics:
        """
        Gets the latest update statistics from the database.

        Args:
            unique_identifier_list: An optional list of unique identifiers to filter by.

        Returns:
            A UpdateStatistics object with the latest statistics.
        """
        if isinstance(self.data_node_storage, int):
            self.set_data_node_update_lazy(force_registry=True, include_relations_detail=True)

        if self.data_node_storage.sourcetableconfiguration is None:
            return ms_client.UpdateStatistics()

        update_stats = self.data_node_storage.sourcetableconfiguration.get_data_updates()
        return update_stats

    def is_local_relation_tree_set(self) -> bool:
        return self.data_node_update.ogm_dependencies_linked

    def update_git_and_code_in_backend(self, time_serie_class) -> None:
        """Updates the source code and git hash information in the backend."""
        self.update_source_informmation(
            git_hash_id=get_data_node_source_code_git_hash(time_serie_class),
            source_code=get_data_node_source_code(time_serie_class),
        )


class TimeScaleLocalPersistManager(PersistManager):
    """
    Main Controler to interacti with backend
    """

    def get_table_schema(self, table_name):
        return self.data_node_storage["sourcetableconfiguration"]["column_dtypes_map"]
