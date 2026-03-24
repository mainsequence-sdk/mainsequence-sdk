from __future__ import annotations

import hashlib
import inspect
import json
import threading
from concurrent.futures import Future
from typing import Any, ClassVar

import pandas as pd

import mainsequence.client as ms_client
from mainsequence.instrumentation import tracer
from mainsequence.logconf import logger

from . import future_registry


def get_data_node_source_code(DataNodeClass: type[Any]) -> str:
    """
    Gets the source code of a DataNode class.

    Args:
        DataNodeClass: The class to get the source code for.

    Returns:
        The source code as a string.
    """
    global logger
    try:
        source = inspect.getsource(DataNodeClass)
        if source.strip():
            return source
    except Exception:
        logger.warning(
            "Your TimeSeries is not in a python module this will likely bring exceptions when running in a pipeline"
        )
    from IPython import get_ipython

    ip = get_ipython()
    if ip is not None:
        history = "\n".join(code for _, _, code in ip.history_manager.get_range())
        marker = f"class {DataNodeClass.__name__}"
        idx = history.find(marker)
        if idx != -1:
            return history[idx:]
    return "Source code unavailable."


def get_data_node_source_code_git_hash(DataNodeClass: type[Any]) -> str:
    """
    Hashes the source code of a DataNode class using SHA-1 (Git style).

    Args:
        DataNodeClass: The class to hash.

    Returns:
        The Git-style hash of the source code.
    """
    data_node_class_source_code = get_data_node_source_code(DataNodeClass)
    content = f"blob {len(data_node_class_source_code)}\0{data_node_class_source_code}"
    hash_object = hashlib.sha1(content.encode("utf-8"))
    return hash_object.hexdigest()


class BasePersistManager:
    STORAGE_CLASS: ClassVar[type[Any] | None] = None
    UPDATE_CLASS: ClassVar[type[Any] | None] = None
    UPDATE_DETAILS_CLASS: ClassVar[type[Any] | None] = None

    UPDATE_GET_OR_NONE_DATASOURCE_LOOKUP: ClassVar[str] = "remote_table__data_source__id"
    UPDATE_CREATE_STORAGE_LOOKUP: ClassVar[str] = "remote_table__hash_id"
    SOURCE_TABLE_CONFIGURATION_ATTR: ClassVar[str] = "sourcetableconfiguration"

    def __init__(
        self,
        data_source: ms_client.DynamicTableDataSource,
        update_hash: str,
        description: str | None = None,
        class_name: str | None = None,
        data_node_storage: dict | None = None,
        data_node_update: Any | None = None,
    ):
        self.data_source: ms_client.DynamicTableDataSource = data_source
        self.update_hash: str = update_hash
        if data_node_update is not None and data_node_storage is None:
            data_node_storage = self._extract_storage_from_update(data_node_update)
        self.description: str | None = description
        self.logger = logger

        self.table_model_loaded: bool = False
        self.class_name: str | None = class_name

        self._data_node_update_future: Future | None = None
        self._data_node_update_cached: Any | None = None
        self._data_node_update_lock = threading.Lock()
        self._data_node_storage_cached: Any | None = data_node_storage

        if self.update_hash is not None:
            self.synchronize_data_node_update(data_node_update=data_node_update)

    def _extract_storage_from_update(self, data_node_update: Any) -> Any:
        return data_node_update.data_node_storage

    def _get_source_table_configuration(self) -> Any | None:
        data_node_storage = self.data_node_storage
        if data_node_storage is None or isinstance(data_node_storage, int):
            return None
        if isinstance(data_node_storage, dict):
            return data_node_storage.get(self.SOURCE_TABLE_CONFIGURATION_ATTR)
        return getattr(data_node_storage, self.SOURCE_TABLE_CONFIGURATION_ATTR, None)

    def _get_storage_hash(self) -> str | None:
        data_node_storage = self.data_node_storage
        if data_node_storage is None:
            return None
        if isinstance(data_node_storage, dict):
            return data_node_storage.get("storage_hash")
        return getattr(data_node_storage, "storage_hash", None)

    def _build_update_get_or_none_kwargs(
        self,
        *,
        include_relations_detail: bool,
    ) -> dict[str, Any]:
        kwargs = {
            "update_hash": self.update_hash,
            "include_relations_detail": include_relations_detail,
        }
        data_source_id = getattr(self.data_source, "id", None)
        if data_source_id is not None:
            kwargs[self.UPDATE_GET_OR_NONE_DATASOURCE_LOOKUP] = data_source_id
        return kwargs

    def _build_storage_get_or_create_kwargs(
        self,
        *,
        storage_hash: str,
        remote_configuration: dict,
        data_source: ms_client.DynamicTableDataSource,
        time_serie_source_code_git_hash: str,
        time_serie_source_code: str,
        build_configuration_json_schema: dict,
        open_to_public: bool,
    ) -> dict[str, Any]:
        return dict(
            storage_hash=storage_hash,
            time_serie_source_code_git_hash=time_serie_source_code_git_hash,
            time_serie_source_code=time_serie_source_code,
            build_configuration=remote_configuration,
            data_source=data_source.model_dump(),
            build_configuration_json_schema=build_configuration_json_schema,
            open_to_public=open_to_public,
        )

    def _build_update_get_or_create_kwargs(
        self,
        *,
        storage_hash: str,
        local_configuration: dict | None = None,
        open_to_public: bool = False,
    ) -> dict[str, Any]:
        kwargs = dict(
            update_hash=self.update_hash,
            build_configuration=local_configuration,
            data_source_id=self.data_source.id,
        )
        kwargs[self.UPDATE_CREATE_STORAGE_LOOKUP] = storage_hash
        return kwargs

    def _should_refresh_update_when_remote_exists(self) -> bool:
        return True

    @property
    def metadata(self) -> Any | None:
        return self.data_node_storage

    @property
    def remote_build_configuration(self) -> dict | None:
        data_node_storage = self.data_node_storage
        if data_node_storage is None or isinstance(data_node_storage, int):
            return None
        if isinstance(data_node_storage, dict):
            return data_node_storage.get("build_configuration")
        return getattr(data_node_storage, "build_configuration", None)

    def synchronize_data_node_update(self, data_node_update: Any | None) -> None:
        if data_node_update is not None:
            self.set_data_node_update(data_node_update)
        else:
            self.set_data_node_update_lazy(force_registry=True, include_relations_detail=True)

    def set_data_node_update(self, data_node_update: Any) -> None:
        self._data_node_update_cached = data_node_update
        try:
            self._data_node_storage_cached = self._extract_storage_from_update(data_node_update)
        except Exception:
            self._data_node_storage_cached = None

    @property
    def data_node_update(self) -> Any:
        with self._data_node_update_lock:
            if self._data_node_update_cached is None:
                if self._data_node_update_future is None:
                    self.set_data_node_update_lazy(force_registry=True)
                data_node_update = self._data_node_update_future.result()
                if data_node_update is not None:
                    self.set_data_node_update(data_node_update)
            return self._data_node_update_cached

    @property
    def data_node_storage(self) -> Any | None:
        if self._data_node_storage_cached is not None:
            return self._data_node_storage_cached
        if self.data_node_update is None:
            return None
        self._data_node_storage_cached = self._extract_storage_from_update(self.data_node_update)
        return self._data_node_storage_cached

    @data_node_storage.setter
    def data_node_storage(self, value: Any | None) -> None:
        self._data_node_storage_cached = value

    @property
    def local_build_configuration(self) -> dict:
        return self.data_node_update.build_configuration

    def set_data_node_update_lazy_callback(self, fut: Future) -> None:
        try:
            fut.result()
        except Exception as exc:
            raise exc
        self.set_data_node_update_lazy(force_registry=True)

    def set_data_node_update_lazy(
        self, force_registry: bool = True, include_relations_detail: bool = True
    ) -> None:
        with self._data_node_update_lock:
            if force_registry:
                self._data_node_update_cached = None
                self._data_node_storage_cached = None
            new_future = Future()
            self._data_node_update_future = new_future
            future_registry.add_future(new_future)

        def _get_or_none_data_node_update():
            try:
                result = self.UPDATE_CLASS.get_or_none(
                    **self._build_update_get_or_none_kwargs(
                        include_relations_detail=include_relations_detail
                    )
                )
                if result is None:
                    self.logger.warning(
                        f"TimeSeries {self.update_hash} with data source {self.data_source.id} not found in backend"
                    )
                new_future.set_result(result)
            except Exception as exc:
                new_future.set_exception(exc)
            finally:
                future_registry.remove_future(new_future)

        thread = threading.Thread(
            target=_get_or_none_data_node_update,
            name=f"LocalDataNodeStorageThreadPM-{self.update_hash}",
            daemon=False,
        )
        thread.start()

    def depends_on_connect(self, new_ts: Any, is_api: bool) -> None:
        if not is_api:
            self.data_node_update.depends_on_connect(
                target_time_serie_id=new_ts.data_node_update.id
            )
        else:
            self.data_node_update.depends_on_connect_to_api_table(
                target_table_id=new_ts.local_persist_manager.data_node_storage.id
            )

    def get_all_dependencies_update_priority(self) -> pd.DataFrame:
        return self.data_node_update.get_all_dependencies_update_priority()

    def set_ogm_dependencies_linked(self) -> None:
        self.data_node_update.patch(ogm_dependencies_linked=True)

    @property
    def update_details(self) -> Any | None:
        return self.data_node_update.update_details

    @property
    def run_configuration(self) -> dict | None:
        return self.data_node_update.run_configuration

    @property
    def source_table_configuration(self) -> Any | None:
        return self._get_source_table_configuration()

    def update_source_informmation(self, git_hash_id: str, source_code: str) -> None:
        self.data_node_update.data_node_storage = self.data_node_storage.patch(
            time_serie_source_code_git_hash=git_hash_id,
            time_serie_source_code=source_code,
        )

    def add_tags(self, tags: list[str]) -> None:
        if any(t not in self.data_node_update.tags for t in tags):
            self.data_node_update.add_tags(tags=tags)

    @property
    def persist_size(self) -> int:
        data_node_storage = self.data_node_storage
        if data_node_storage is None:
            return 0
        if isinstance(data_node_storage, dict):
            return data_node_storage.get("table_size", 0)
        return getattr(data_node_storage, "table_size", 0)

    def time_serie_exist(self) -> bool:
        return self.data_node_storage is not None

    def local_persist_exist_set_config(
        self,
        storage_hash: str,
        local_configuration: dict,
        remote_configuration: dict,
        data_source: ms_client.DynamicTableDataSource,
        time_serie_source_code_git_hash: str,
        time_serie_source_code: str,
        build_configuration_json_schema: dict,
        open_to_public: bool,
    ) -> None:
        remote_build_configuration = self.remote_build_configuration

        if remote_build_configuration is None:
            logger.debug(f"remote table {storage_hash} does not exist creating")

            try:
                kwargs = self._build_storage_get_or_create_kwargs(
                    storage_hash=storage_hash,
                    remote_configuration=remote_configuration,
                    data_source=data_source,
                    time_serie_source_code_git_hash=time_serie_source_code_git_hash,
                    time_serie_source_code=time_serie_source_code,
                    build_configuration_json_schema=build_configuration_json_schema,
                    open_to_public=open_to_public,
                )

                dtd_metadata = self.STORAGE_CLASS.get_or_create(**kwargs)
                self.data_node_storage = dtd_metadata
                storage_hash = dtd_metadata.storage_hash
            except Exception as e:
                self.logger.exception(f"{storage_hash} Could not set meta data in DB for P")
                raise e
        else:
            if self._should_refresh_update_when_remote_exists():
                self.set_data_node_update_lazy(
                    force_registry=True, include_relations_detail=True
                )
            storage_hash = self._get_storage_hash()

        self._verify_local_ts_exists(
            storage_hash=storage_hash,
            local_configuration=local_configuration,
            open_to_public=open_to_public,
        )

    def _verify_local_ts_exists(
        self,
        storage_hash: str,
        local_configuration: dict | None = None,
        open_to_public: bool = False,
    ) -> None:
        local_build_configuration = None
        if self.data_node_update is not None:
            local_build_configuration = self.local_build_configuration
        if local_build_configuration is None:
            logger.debug(f"data_node_update {self.update_hash} does not exist creating")
            local_update = self.UPDATE_CLASS.get_or_none(
                **self._build_update_get_or_none_kwargs(include_relations_detail=False)
            )
            if local_update is None:
                data_node_update = self.UPDATE_CLASS.get_or_create(
                    **self._build_update_get_or_create_kwargs(
                        storage_hash=storage_hash,
                        local_configuration=local_configuration,
                        open_to_public=open_to_public,
                    ),
                )
            else:
                data_node_update = local_update

            self.set_data_node_update(data_node_update=data_node_update)

    def _verify_insertion_format(self, temp_df: pd.DataFrame) -> None:
        if isinstance(temp_df.index, pd.MultiIndex) is True:
            assert temp_df.index.names == ["time_index", "asset_symbol"] or temp_df.index.names == [
                "time_index",
                "asset_symbol",
                "execution_venue_symbol",
            ]

    def build_update_details(self, source_class_name: str) -> None:
        update_kwargs = dict(
            source_class_name=source_class_name,
            local_metadata=json.loads(self.data_node_update.model_dump_json()),
        )
        with self._data_node_update_lock:
            self._data_node_update_future = Future()
            future_registry.add_future(self._data_node_update_future)

        future = Future()
        future_registry.add_future(future)

        def _update_task():
            try:
                self.data_node_update.data_node_storage.build_or_update_update_details(
                    **update_kwargs
                )
                future.set_result(True)
            except Exception as exc:
                future.set_exception(exc)
            finally:
                future_registry.remove_future(future)

        thread = threading.Thread(
            target=_update_task, name=f"BuildUpdateDetailsThread-{self.update_hash}", daemon=False
        )
        thread.start()
        future.add_done_callback(self.set_data_node_update_lazy_callback)

    def patch_table(self, **kwargs) -> None:
        self.data_node_storage.patch(**kwargs)

    def protect_from_deletion(self, protect_from_deletion: bool = True) -> None:
        self.data_node_storage.patch(protect_from_deletion=protect_from_deletion)

    def open_for_everyone(self, open_for_everyone: bool = True) -> None:
        if not self.data_node_update.open_for_everyone:
            self.data_node_update.patch(open_for_everyone=open_for_everyone)

        if not self.data_node_storage.open_for_everyone:
            self.data_node_storage.patch(open_for_everyone=open_for_everyone)

        source_table_configuration = self._get_source_table_configuration()
        if (
            source_table_configuration is not None
            and not source_table_configuration.open_for_everyone
        ):
            source_table_configuration.patch(open_for_everyone=open_for_everyone)

    def get_df_between_dates(self, *args, **kwargs) -> pd.DataFrame:
        return self.data_source.get_data_by_time_index(
            data_node_update=self.data_node_update, *args, **kwargs
        )

    def get_last_observation(self, asset_list: list[Asset] | None):
        unique_identifier_list = []
        if asset_list is not None:
            unique_identifier_list = [a.unique_identifier for a in asset_list]
        return self.data_node_storage.get_last_observation(
            unique_identifier_list=unique_identifier_list
        )

    def set_column_metadata(self, columns_metadata: list[ms_client.ColumnMetaData] | None) -> None:
        source_table_configuration = self._get_source_table_configuration()
        if source_table_configuration is not None:
            if source_table_configuration.columns_metadata is not None:
                if columns_metadata is None:
                    self.logger.info("get_column_metadata method not implemented")
                    return

                source_table_configuration.set_or_update_columns_metadata(
                    columns_metadata=columns_metadata
                )

    def set_table_metadata(
        self,
        table_metadata: ms_client.TableMetaData,
    ):
        if not self.data_node_storage:
            self.logger.warning("metadata not set")
            return

        if table_metadata is None:
            return

        self.data_node_storage.patch(**table_metadata.model_dump())

    def delete_table(self) -> None:
        if self.data_source.related_resource.class_type == "duck_db":
            from mainsequence.client.data_sources_interfaces.duckdb import DuckDBInterface

            db_interface = DuckDBInterface()
            db_interface.drop_table(self.data_node_storage.storage_hash)

        self.data_node_storage.delete()

    @tracer.start_as_current_span("TS: Persist Data")
    def persist_updated_data(self, temp_df: pd.DataFrame, overwrite: bool = False) -> bool:
        persisted = False
        if not temp_df.empty:
            if overwrite is True:
                self.logger.warning("Values will be overwritten")

            self._data_node_update_cached = self.data_node_update.upsert_data_into_table(
                data=temp_df,
                data_source=self.data_source,
                overwrite=overwrite,
            )

            persisted = True
        return persisted

    def get_update_statistics_for_table(self) -> ms_client.UpdateStatistics:
        if isinstance(self.data_node_storage, int):
            self.set_data_node_update_lazy(force_registry=True, include_relations_detail=True)

        source_table_configuration = self._get_source_table_configuration()
        if source_table_configuration is None:
            return ms_client.UpdateStatistics()

        return source_table_configuration.get_data_updates()

    def is_local_relation_tree_set(self) -> bool:
        return self.data_node_update.ogm_dependencies_linked

    def update_git_and_code_in_backend(self, time_serie_class) -> None:
        self.update_source_informmation(
            git_hash_id=get_data_node_source_code_git_hash(time_serie_class),
            source_code=get_data_node_source_code(time_serie_class),
        )
