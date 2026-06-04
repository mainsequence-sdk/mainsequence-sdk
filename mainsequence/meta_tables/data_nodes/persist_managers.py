from __future__ import annotations

import hashlib
import inspect
import threading
from concurrent.futures import Future
from typing import Any, ClassVar

import pandas as pd

from mainsequence.client.dtype_codec import (
    TIMESTAMP_TZ,
    sqlalchemy_type_to_token,
    token_to_pandas_series,
)
from mainsequence.client.metatables import (
    DUCK_DB,
    LOCAL_DATA_SOURCE_CLASS_TYPES,
    SQLITE,
    DataNodeUpdate,
    DataNodeUpdateDetails,
    DynamicTableDataSource,
    TimeIndexMetaTable,
    UpdateStatistics,
)
from mainsequence.instrumentation import tracer
from mainsequence.logconf import logger
from mainsequence.meta_tables import PlatformTimeIndexMetaTable

from .. import future_registry


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


def ensure_registered_storage_table(
    storage_table: type[PlatformTimeIndexMetaTable],
    *,
    context: str,
) -> type[PlatformTimeIndexMetaTable]:
    if storage_table is None:
        raise TypeError(
            f"{context} storage_table is required and must be a "
            "PlatformTimeIndexMetaTable model class."
        )
    if not isinstance(storage_table, type) or not issubclass(
        storage_table,
        PlatformTimeIndexMetaTable,
    ):
        raise TypeError(
            f"{context} storage_table must be a PlatformTimeIndexMetaTable "
            f"model class; got {type(storage_table).__name__}."
        )

    if storage_table.get_time_index_meta_table() is None:
        _bind_registered_storage_table(storage_table)

    storage_metadata = storage_table.get_time_index_meta_table()
    if storage_metadata is None:
        raise ValueError(
            f"{context} storage_table class is not bound to backend "
            "TimeIndexMetaTable "
            "metadata in this Python process. The backend table may already exist; "
            "the SDK could not resolve a unique TimeIndexMetaTable row for "
            f"{_storage_table_lookup_label(storage_table)}."
        )
    if not isinstance(storage_metadata, TimeIndexMetaTable):
        raise TypeError(
            f"{context} storage_table must bind TimeIndexMetaTable metadata; "
            f"got {type(storage_metadata).__name__}."
        )
    if storage_table.get_meta_table_uid() in (None, ""):
        raise ValueError(f"{context} storage_table must provide a MetaTable UID.")
    if storage_table.get_data_source_uid() in (None, ""):
        raise ValueError(f"{context} storage_table must provide a data-source UID.")
    return storage_table


def _bind_registered_storage_table(storage_table: type[PlatformTimeIndexMetaTable]) -> None:
    matches = _registered_storage_table_matches(storage_table)
    if len(matches) == 1:
        storage_table._bind_meta_table(matches[0])


def _registered_storage_table_matches(
    storage_table: type[PlatformTimeIndexMetaTable],
) -> list[TimeIndexMetaTable]:
    table_name = _storage_table_physical_table_name(storage_table)
    if table_name:
        matches = TimeIndexMetaTable.filter_by_body(
            physical_table_name__in=[table_name],
            limit=1,
        )
        if matches:
            return matches

    return []


def _storage_table_physical_table_name(
    storage_table: type[PlatformTimeIndexMetaTable],
) -> str | None:
    physical_table_name = storage_table.get_physical_table_name()
    if physical_table_name not in (None, ""):
        return str(physical_table_name)
    table = getattr(storage_table, "__table__", None)
    table_name = getattr(table, "name", None)
    if table_name not in (None, ""):
        return str(table_name)
    return None


def _storage_table_lookup_label(storage_table: type[PlatformTimeIndexMetaTable]) -> str:
    table_name = _storage_table_physical_table_name(storage_table) or "<unknown-table>"
    return f"{storage_table.__name__}(table={table_name})"


class BasePersistManager:
    UPDATE_CLASS: ClassVar[type[Any] | None] = None
    UPDATE_DETAILS_CLASS: ClassVar[type[Any] | None] = None

    UPDATE_GET_OR_NONE_STORAGE_LOOKUP: ClassVar[str] = "remote_table__uid"
    UPDATE_CREATE_STORAGE_LOOKUP: ClassVar[str] = "meta_table_uid"
    TIME_INDEXED_PROFILE_ATTR: ClassVar[str] = "time_indexed_profile"

    def __init__(
        self,
        update_hash: str,
        storage_table: type[PlatformTimeIndexMetaTable],
        description: str | None = None,
        class_name: str | None = None,
        data_node_update: Any | None = None,
    ):
        self.update_hash: str = update_hash
        self.description: str | None = description
        self.logger = logger

        self.table_model_loaded: bool = False
        self.class_name: str | None = class_name

        self._data_node_update_future: Future | None = None
        self._data_node_update_cached: Any | None = None
        self._data_node_update_lock = threading.Lock()
        self.storage_table: type[PlatformTimeIndexMetaTable] = self._validate_storage_table(
            storage_table
        )

        if self.update_hash is not None:
            self.synchronize_data_node_update(data_node_update=data_node_update)

    @staticmethod
    def _validate_storage_table(
        storage_table: type[PlatformTimeIndexMetaTable],
    ) -> type[PlatformTimeIndexMetaTable]:
        return ensure_registered_storage_table(storage_table, context="PersistManager")

    @property
    def storage_metadata(self) -> Any:
        storage_metadata = self.storage_table.get_time_index_meta_table()
        if storage_metadata is None:
            raise ValueError(
                "PersistManager storage_table registration metadata is unavailable after register()."
            )
        return storage_metadata

    @property
    def data_source(self) -> DynamicTableDataSource | Any:
        data_source = getattr(self.storage_metadata, "data_source", None)
        if data_source not in (None, "") and not isinstance(data_source, int | str):
            return data_source
        return DynamicTableDataSource.get_by_uid(self.storage_table.get_data_source_uid())

    def _get_time_indexed_profile(self) -> Any | None:
        return getattr(self.storage_metadata, self.TIME_INDEXED_PROFILE_ATTR, None)

    def _build_update_get_or_none_kwargs(
        self,
        *,
        include_relations_detail: bool,
    ) -> dict[str, Any]:
        kwargs = {
            "update_hash": self.update_hash,
            "include_relations_detail": include_relations_detail,
        }
        kwargs[self.UPDATE_GET_OR_NONE_STORAGE_LOOKUP] = self.storage_table.get_meta_table_uid()
        return kwargs

    def _build_update_get_or_create_kwargs(
        self,
        *,
        local_configuration: dict | None = None,
    ) -> dict[str, Any]:
        kwargs = dict(
            update_hash=self.update_hash,
            build_configuration=local_configuration,
        )
        kwargs[self.UPDATE_CREATE_STORAGE_LOOKUP] = self.storage_table.get_meta_table_uid()
        return kwargs

    def _should_refresh_update_when_remote_exists(self) -> bool:
        return True

    @property
    def metadata(self) -> Any | None:
        return self.storage_metadata

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
        self._data_node_update_cached = data_node_update

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
                        f"TimeSeries {self.update_hash} with data source "
                        f"{self.storage_table.get_data_source_uid()} not found in backend"
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
        return getattr(self.storage_metadata, "table_size", 0)

    def time_serie_exist(self) -> bool:
        return self.storage_metadata is not None

    def local_persist_exist_set_config(
        self,
        local_configuration: dict,
    ) -> None:
        if self._should_refresh_update_when_remote_exists():
            self.set_data_node_update_lazy(force_registry=True, include_relations_detail=True)

        self._verify_local_ts_exists(
            local_configuration=local_configuration,
        )

    def _verify_local_ts_exists(
        self,
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
                        local_configuration=local_configuration,
                    ),
                )
            else:
                data_node_update = local_update

            self.set_data_node_update(data_node_update=data_node_update)

    def build_update_details(self, source_class_name: str) -> None:
        if self.UPDATE_DETAILS_CLASS is None:
            raise ValueError("UPDATE_DETAILS_CLASS must be configured to patch update details.")
        data_node_update_uid = getattr(self.data_node_update, "uid", None)
        if data_node_update_uid in (None, ""):
            raise ValueError("DataNodeUpdate uid is required to patch update details.")

        with self._data_node_update_lock:
            self._data_node_update_future = Future()
            future_registry.add_future(self._data_node_update_future)

        future = Future()
        future_registry.add_future(future)

        def _update_task():
            try:
                self.UPDATE_DETAILS_CLASS.patch_for_data_node_update_uid(
                    data_node_update_uid,
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
        self.storage_metadata.patch(**kwargs)

    def protect_from_deletion(self, protect_from_deletion: bool = True) -> None:
        self.storage_metadata.patch(protect_from_deletion=protect_from_deletion)

    def open_for_everyone(self, open_for_everyone: bool = True) -> None:
        if not self.data_node_update.open_for_everyone:
            self.data_node_update.patch(open_for_everyone=open_for_everyone)

        if not self.storage_metadata.open_for_everyone:
            self.storage_metadata.patch(open_for_everyone=open_for_everyone)

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
        return self.storage_metadata.get_last_observation(
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
            db_interface.drop_table(self.storage_metadata.storage_hash)

        self.storage_metadata.delete()

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
                source_table_schema=self._source_table_schema(),
            )

            persisted = True
        return persisted

    def _source_table_schema(self) -> dict[str, Any]:
        time_index_name, index_names, column_dtypes_map = (
            self.storage_metadata._require_time_indexed_table_contract()
        )
        return {
            "time_index_name": time_index_name,
            "index_names": list(index_names),
            "column_dtypes_map": dict(column_dtypes_map),
        }

    def get_update_statistics_for_table(self) -> UpdateStatistics:
        return self.storage_metadata.get_data_updates()

    def is_local_relation_tree_set(self) -> bool:
        return self.data_node_update.ogm_dependencies_linked

    def update_git_and_code_in_backend(self, time_serie_class) -> None:
        logger.debug(
            "Skipping storage-table source-code patch because backend storage "
            "metadata no longer stores source code fields."
        )


class APIPersistManager:
    """
    Manages persistence for time series data accessed via an API.
    It handles asynchronous fetching of the storage table to avoid blocking operations.
    """

    def __init__(
        self,
        *,
        storage_hash: str | None = None,
        data_source_uid: str,
    ):
        if data_source_uid in (None, ""):
            raise ValueError("APIPersistManager requires data_source_uid.")
        self.data_source_uid: str = str(data_source_uid)
        self.storage_hash: str = storage_hash

        logger.debug(f"Initializing Time Serie {self.storage_hash}  as APIDataNode")

        self._storage_table_future = Future()
        future_registry.add_future(self._storage_table_future)
        thread = threading.Thread(
            target=self._init_storage_table,
            name=f"ApiStorageTableThread-{self.storage_hash}",
            daemon=False,
        )
        thread.start()

    @property
    def storage_table(self) -> TimeIndexMetaTable:
        if not hasattr(self, "_storage_table_cached"):
            self._storage_table_cached = self._storage_table_future.result()
        return self._storage_table_cached

    def _init_storage_table(self) -> None:
        try:
            result = TimeIndexMetaTable.get_or_none(
                physical_table_name=self.storage_hash,
                data_source__uid=self.data_source_uid,
                include_relations_detail=True,
            )
            self._storage_table_future.set_result(result)
        except Exception as exc:
            self._storage_table_future.set_exception(exc)
        finally:
            future_registry.remove_future(self._storage_table_future)

    def get_last_observation(
        self,
        *,
        dimension_filters: dict[str, list[Any]] | None = None,
        index_coordinates: list[dict[str, Any]] | None = None,
        dimension_range_map: list[dict[str, Any]] | None = None,
    ):
        last_observation = self.storage_table.get_last_observation(
            dimension_filters=dimension_filters,
            index_coordinates=index_coordinates,
            dimension_range_map=dimension_range_map,
        )
        return last_observation

    def get_df_between_dates(self, *args, **kwargs) -> pd.DataFrame:
        filtered_data = self.storage_table.get_data_between_dates_from_api(*args, **kwargs)
        if filtered_data.empty:
            return filtered_data

        time_index_name, index_names, column_dtypes_map = (
            self.storage_table._require_time_indexed_table_contract()
        )
        filtered_data[time_index_name] = token_to_pandas_series(
            filtered_data[time_index_name],
            TIMESTAMP_TZ,
            is_time_index=True,
        )
        column_filter = kwargs.get("columns") or column_dtypes_map.keys()
        for c in column_filter:
            c_type = column_dtypes_map[c]
            if c in filtered_data.columns:
                filtered_data[c] = token_to_pandas_series(
                    filtered_data[c],
                    c_type,
                    is_time_index=c == time_index_name,
                )
        filtered_data = filtered_data.set_index(index_names)

        return filtered_data


class PersistManager(BasePersistManager):
    UPDATE_CLASS = DataNodeUpdate
    UPDATE_DETAILS_CLASS = DataNodeUpdateDetails
    UPDATE_GET_OR_NONE_STORAGE_LOOKUP = "remote_table__uid"
    UPDATE_CREATE_STORAGE_LOOKUP = "meta_table_uid"

    @classmethod
    def get_from_storage_table(cls, storage_table: Any, *args, **kwargs) -> PersistManager:
        return TimeScaleLocalPersistManager(storage_table=storage_table, *args, **kwargs)


class TimeScaleLocalPersistManager(PersistManager):
    """
    Main Controller to interact with backend-backed table storage.
    """

    def get_table_schema(self, _):
        storage_metadata = self.storage_metadata
        profile = getattr(storage_metadata, "time_indexed_profile", None)
        if isinstance(profile, dict) and profile.get("column_dtypes_map"):
            return profile["column_dtypes_map"]

        column_dtypes_map = getattr(storage_metadata, "column_dtypes_map", None)
        if column_dtypes_map is not None:
            return column_dtypes_map

        table_contract = getattr(storage_metadata, "table_contract", None)
        contract_columns = []
        if isinstance(table_contract, dict):
            contract_columns = table_contract.get("columns") or []
        elif table_contract is not None:
            contract_columns = getattr(table_contract, "columns", []) or []

        if not contract_columns:
            table = getattr(self.storage_table, "__table__", None)
            contract_columns = list(getattr(table, "columns", []) or [])

        schema = {}
        for column in contract_columns:
            if isinstance(column, dict):
                column_name = column.get("name") or column.get("column_name")
                data_type = column.get("data_type") or column.get("dtype")
            else:
                column_name = getattr(column, "name", None)
                data_type = getattr(column, "data_type", None) or getattr(
                    column,
                    "dtype",
                    None,
                )
                if data_type in (None, "") and getattr(column, "type", None) is not None:
                    data_type = sqlalchemy_type_to_token(column.type, remote=True)
            if column_name not in (None, "") and data_type not in (None, ""):
                schema[str(column_name)] = str(data_type)
        return schema
