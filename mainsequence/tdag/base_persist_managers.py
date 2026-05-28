from __future__ import annotations

import hashlib
import inspect
import json
import threading
from concurrent.futures import Future
from typing import Any, ClassVar
from uuid import UUID

import pandas as pd

from mainsequence.client.models_tdag import (
    DUCK_DB,
    LOCAL_DATA_SOURCE_CLASS_TYPES,
    SQLITE,
    DynamicTableDataSource,
    UpdateStatistics,
)
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
    UPDATE_CLASS: ClassVar[type[Any] | None] = None
    UPDATE_DETAILS_CLASS: ClassVar[type[Any] | None] = None

    UPDATE_GET_OR_NONE_DATASOURCE_LOOKUP: ClassVar[str] = "remote_table__data_source__uid"
    UPDATE_CREATE_STORAGE_LOOKUP: ClassVar[str] = "meta_table_uid"
    TIME_INDEXED_PROFILE_ATTR: ClassVar[str] = "time_indexed_profile"

    def __init__(
        self,
        update_hash: str,
        description: str | None = None,
        class_name: str | None = None,
        storage_table: Any | None = None,
        data_node_update: Any | None = None,
    ):
        self.update_hash: str = update_hash
        if data_node_update is not None and storage_table is None:
            storage_table = self._extract_storage_from_update(data_node_update)
        self.description: str | None = description
        self.logger = logger

        self.table_model_loaded: bool = False
        self.class_name: str | None = class_name

        self._data_node_update_future: Future | None = None
        self._data_node_update_cached: Any | None = None
        self._data_node_update_lock = threading.Lock()
        self._storage_table_cached: Any | None = storage_table
        self._explicit_storage_table = storage_table is not None
        self._data_source_cached: DynamicTableDataSource | Any | None = None

        if self.update_hash is not None and (
            data_node_update is not None or storage_table is not None
        ):
            self.synchronize_data_node_update(data_node_update=data_node_update)

    def _extract_storage_from_update(self, data_node_update: Any) -> Any:
        return data_node_update.data_node_storage

    @staticmethod
    def _storage_data_source_object(storage: Any) -> Any | None:
        if storage is None:
            return None
        if isinstance(storage, dict):
            data_source = storage.get("data_source")
        else:
            data_source = getattr(storage, "data_source", None)
        if isinstance(data_source, int | str | UUID):
            return None
        return data_source

    @property
    def data_source(self) -> DynamicTableDataSource | Any:
        if self._data_source_cached is not None:
            return self._data_source_cached

        storage = self.storage_table
        data_source = self._storage_data_source_object(storage)
        if data_source is None:
            data_source_uid = self._storage_data_source_uid(storage)
            if data_source_uid in (None, ""):
                raise ValueError("PersistManager requires storage_table.data_source_uid.")
            data_source = DynamicTableDataSource.get_by_uid(data_source_uid)

        self._data_source_cached = data_source
        return data_source

    def _get_time_indexed_profile(self) -> Any | None:
        storage_table = self.storage_table
        if storage_table is None or isinstance(storage_table, int):
            return None
        if isinstance(storage_table, dict):
            return storage_table.get(self.TIME_INDEXED_PROFILE_ATTR)
        return getattr(storage_table, self.TIME_INDEXED_PROFILE_ATTR, None)

    def _build_update_get_or_none_kwargs(
        self,
        *,
        include_relations_detail: bool,
    ) -> dict[str, Any]:
        kwargs = {
            "update_hash": self.update_hash,
            "include_relations_detail": include_relations_detail,
        }
        data_source_uid = self._storage_data_source_uid(self._storage_table_cached)
        if data_source_uid in (None, ""):
            raise ValueError("DataNode update lookup requires storage_table.data_source_uid.")
        kwargs[self.UPDATE_GET_OR_NONE_DATASOURCE_LOOKUP] = str(data_source_uid)
        return kwargs

    @staticmethod
    def _storage_uid(storage: Any) -> str | None:
        if isinstance(storage, dict):
            uid = storage.get("uid")
        else:
            uid = getattr(storage, "uid", None)
        if isinstance(uid, UUID):
            return str(uid)
        return str(uid) if uid not in (None, "") else None

    @staticmethod
    def _storage_data_source_uid(storage: Any) -> str | None:
        if isinstance(storage, dict):
            data_source_uid = storage.get("data_source_uid")
            data_source = storage.get("data_source")
        else:
            data_source_uid = getattr(storage, "data_source_uid", None)
            data_source = getattr(storage, "data_source", None)

        if data_source_uid not in (None, ""):
            return str(data_source_uid)
        if isinstance(data_source, dict):
            nested_uid = data_source.get("uid")
        else:
            nested_uid = getattr(data_source, "uid", None)
        return str(nested_uid) if nested_uid not in (None, "") else None

    def _require_existing_storage_table(self) -> Any:
        storage = self._storage_table_cached
        if storage is None:
            raise ValueError(
                "PersistManager requires an explicit storage_table. Create or "
                "register the MetaTable before constructing the DataNode."
            )
        if isinstance(storage, int | str | UUID):
            raise ValueError(
                "PersistManager requires a resolved storage_table object, not "
                "only a storage uid."
            )

        storage_uid = self._storage_uid(storage)
        if storage_uid in (None, ""):
            raise ValueError("PersistManager requires storage_table.uid.")

        storage_data_source_uid = self._storage_data_source_uid(storage)
        if storage_data_source_uid in (None, ""):
            raise ValueError("PersistManager requires storage_table.data_source_uid.")
        return storage

    def _build_update_get_or_create_kwargs(
        self,
        *,
        storage: Any,
        local_configuration: dict | None = None,
    ) -> dict[str, Any]:
        storage_data_source_uid = self._storage_data_source_uid(storage)
        if storage_data_source_uid in (None, ""):
            raise ValueError("DataNode update creation requires storage_table.data_source_uid.")
        storage_uid = self._storage_uid(storage)
        if storage_uid in (None, ""):
            raise ValueError("DataNode update creation requires storage_table.uid.")
        kwargs = dict(
            update_hash=self.update_hash,
            build_configuration=local_configuration,
            data_source_uid=str(storage_data_source_uid),
        )
        kwargs[self.UPDATE_CREATE_STORAGE_LOOKUP] = storage_uid
        return kwargs

    def _should_refresh_update_when_remote_exists(self) -> bool:
        return True

    @property
    def metadata(self) -> Any | None:
        return self.storage_table

    @property
    def remote_build_configuration(self) -> dict | None:
        data_node_update = self.data_node_update
        if data_node_update is None:
            return None
        if isinstance(data_node_update, dict):
            return data_node_update.get("build_configuration")
        return getattr(data_node_update, "build_configuration", None)

    def synchronize_data_node_update(self, data_node_update: Any | None) -> None:
        if data_node_update is not None:
            self.set_data_node_update(data_node_update)
        else:
            self.set_data_node_update_lazy(force_registry=True, include_relations_detail=True)

    def set_data_node_update(self, data_node_update: Any) -> None:
        previous_storage = self._storage_table_cached
        self._data_node_update_cached = data_node_update
        try:
            extracted_storage = self._extract_storage_from_update(data_node_update)
        except Exception:
            extracted_storage = None

        if extracted_storage is None:
            self._storage_table_cached = previous_storage
        elif previous_storage is not None and isinstance(extracted_storage, int | str | UUID):
            self._storage_table_cached = previous_storage
        else:
            self._storage_table_cached = extracted_storage

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
    def storage_table(self) -> Any | None:
        if self._storage_table_cached is not None:
            return self._storage_table_cached
        if self.data_node_update is None:
            return None
        self._storage_table_cached = self._extract_storage_from_update(self.data_node_update)
        return self._storage_table_cached

    @storage_table.setter
    def storage_table(self, value: Any | None) -> None:
        self._storage_table_cached = value
        self._explicit_storage_table = value is not None

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
                if not self._explicit_storage_table:
                    self._storage_table_cached = None
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
                    data_source_uid = self._storage_data_source_uid(self._storage_table_cached)
                    self.logger.warning(
                        f"TimeSeries {self.update_hash} with data source {data_source_uid} not found in backend"
                    )
                new_future.set_result(result)
            except Exception as exc:
                new_future.set_exception(exc)
            finally:
                future_registry.remove_future(new_future)

        thread = threading.Thread(
            target=_get_or_none_data_node_update,
            name=f"LocalStorageTableThreadPM-{self.update_hash}",
            daemon=False,
        )
        thread.start()

    def depends_on_connect(self, new_ts: Any, is_api: bool) -> None:
        if not is_api:
            self.data_node_update.depends_on_connect(
                target_update_node_uid=new_ts.data_node_update.uid
            )
        else:
            self.data_node_update.depends_on_connect_to_api_table(
                target_table_uid=new_ts.local_persist_manager.storage_table.uid
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
    def time_indexed_profile(self) -> Any | None:
        return self._get_time_indexed_profile()

    def update_source_informmation(self, git_hash_id: str, source_code: str) -> None:
        logger.debug(
            "Skipping storage-table source-code patch because backend storage "
            "metadata no longer stores source code fields."
        )

    def add_tags(self, tags: list[str]) -> None:
        if any(t not in self.data_node_update.tags for t in tags):
            self.data_node_update.add_tags(tags=tags)

    @property
    def persist_size(self) -> int:
        storage_table = self.storage_table
        if storage_table is None:
            return 0
        if isinstance(storage_table, dict):
            return storage_table.get("table_size", 0)
        return getattr(storage_table, "table_size", 0)

    def time_serie_exist(self) -> bool:
        return self.storage_table is not None

    def local_persist_exist_set_config(
        self,
        local_configuration: dict,
    ) -> None:
        storage = self._require_existing_storage_table()
        if self._should_refresh_update_when_remote_exists():
            self.set_data_node_update_lazy(force_registry=True, include_relations_detail=True)

        self._verify_local_ts_exists(
            storage=storage,
            local_configuration=local_configuration,
        )

    def _verify_local_ts_exists(
        self,
        storage: Any,
        local_configuration: dict | None = None,
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
                        storage=storage,
                        local_configuration=local_configuration,
                    ),
                )
            else:
                data_node_update = local_update

            self.set_data_node_update(data_node_update=data_node_update)

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
                self.storage_table.build_or_update_update_details(**update_kwargs)
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
        self.storage_table.patch(**kwargs)

    def protect_from_deletion(self, protect_from_deletion: bool = True) -> None:
        self.storage_table.patch(protect_from_deletion=protect_from_deletion)

    def open_for_everyone(self, open_for_everyone: bool = True) -> None:
        if not self.data_node_update.open_for_everyone:
            self.data_node_update.patch(open_for_everyone=open_for_everyone)

        if not self.storage_table.open_for_everyone:
            self.storage_table.patch(open_for_everyone=open_for_everyone)

    def get_df_between_dates(self, *args, **kwargs) -> pd.DataFrame:
        return self.data_source.get_data_by_time_index(
            *args,
            data_node_update=self.data_node_update,
            **kwargs,
        )

    def get_last_observation(
        self,
        *,
        dimension_filters: dict[str, list[Any]] | None = None,
        index_coordinates: list[dict[str, Any]] | None = None,
        dimension_range_map: list[dict[str, Any]] | None = None,
    ):
        return self.storage_table.get_last_observation(
            dimension_filters=dimension_filters,
            index_coordinates=index_coordinates,
            dimension_range_map=dimension_range_map,
        )

    def delete_table(self) -> None:
        class_type = self.data_source.related_resource.class_type
        if class_type in LOCAL_DATA_SOURCE_CLASS_TYPES:
            from mainsequence.client.data_sources_interfaces import (
                get_duckdb_interface_class,
                get_sqlite_interface_class,
            )

            if class_type == DUCK_DB:
                db_interface = get_duckdb_interface_class()()
            elif class_type == SQLITE:
                db_interface = get_sqlite_interface_class()()
            else:
                raise ValueError(f"Unsupported local DataSource class_type: {class_type!r}")
            db_interface.drop_table(self.storage_table.storage_hash)

        self.storage_table.delete()

    @tracer.start_as_current_span("TS: Persist Data")
    def persist_updated_data(
        self,
        temp_df: pd.DataFrame,
        overwrite: bool = False,
    ) -> bool:
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

    def get_update_statistics_for_table(self) -> UpdateStatistics:
        if isinstance(self.storage_table, int):
            self.set_data_node_update_lazy(force_registry=True, include_relations_detail=True)

        if self.storage_table is None or isinstance(self.storage_table, int):
            return UpdateStatistics()

        return self.storage_table.get_data_updates()

    def is_local_relation_tree_set(self) -> bool:
        return self.data_node_update.ogm_dependencies_linked

    def update_git_and_code_in_backend(self, time_serie_class) -> None:
        logger.debug(
            "Skipping storage-table source-code patch because backend storage "
            "metadata no longer stores source code fields."
        )
