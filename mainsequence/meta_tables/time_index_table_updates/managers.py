from __future__ import annotations

import hashlib
import inspect
import threading
from concurrent.futures import Future
from dataclasses import dataclass
from typing import Any, ClassVar

import pandas as pd

from mainsequence.client.dtype_codec import sqlalchemy_type_to_token
from mainsequence.client.metatables import (
    DUCK_DB,
    LOCAL_DATA_SOURCE_CLASS_TYPES,
    SQLITE,
    DataSource,
    TimeIndexMetaTable,
    TimeIndexTableUpdate,
    TimeIndexTableUpdateDetails,
    UpdateStatistics,
    get_session_data_source,
)
from mainsequence.instrumentation import tracer
from mainsequence.logconf import logger
from mainsequence.meta_tables import PlatformTimeIndexMetaTable, compute_metatable_contract_hash

from .. import future_registry
from .configuration import validate_canonical_configuration

_OUTPUT_TABLE_LOOKUP_LIMIT = 20
_DEFAULT_POSTGRES_SCHEMA = "public"


@dataclass(frozen=True)
class _OutputTableLookupResult:
    data_source_uid: str | None
    physical_schema: str | None
    table_name: str | None
    filters: dict[str, Any]
    matches: list[TimeIndexMetaTable]
    data_source_resolution_error: str | None = None
    data_source_mismatch: tuple[str, str] | None = None


def get_table_updater_source_code(UpdaterClass: type[Any]) -> str:
    """
    Gets the source code of a TimeIndexTableUpdater class.

    Args:
        UpdaterClass: The class to get the source code for.

    Returns:
        The source code as a string.
    """
    global logger
    try:
        source = inspect.getsource(UpdaterClass)
        if source.strip():
            return source
    except Exception:
        logger.warning(
            "Your TimeIndexTableUpdater is not in a Python module; pipeline "
            "source provenance will be unavailable."
        )
    from IPython import get_ipython

    ip = get_ipython()
    if ip is not None:
        history = "\n".join(code for _, _, code in ip.history_manager.get_range())
        marker = f"class {UpdaterClass.__name__}"
        idx = history.find(marker)
        if idx != -1:
            return history[idx:]
    return "Source code unavailable."


def get_table_updater_source_code_git_hash(UpdaterClass: type[Any]) -> str:
    """
    Hashes the source code of a TimeIndexTableUpdater class using SHA-1 (Git style).

    Args:
        UpdaterClass: The class to hash.

    Returns:
        The Git-style hash of the source code.
    """
    updater_class_source_code = get_table_updater_source_code(UpdaterClass)
    content = f"blob {len(updater_class_source_code)}\0{updater_class_source_code}"
    hash_object = hashlib.sha1(content.encode("utf-8"))
    return hash_object.hexdigest()


def ensure_registered_output_table(
    output_table: type[PlatformTimeIndexMetaTable],
    *,
    context: str,
) -> type[PlatformTimeIndexMetaTable]:
    if output_table is None:
        raise TypeError(
            f"{context} output_table is required and must be a "
            "PlatformTimeIndexMetaTable model class."
        )
    if not isinstance(output_table, type) or not issubclass(
        output_table,
        PlatformTimeIndexMetaTable,
    ):
        raise TypeError(
            f"{context} output_table must be a PlatformTimeIndexMetaTable "
            f"model class; got {type(output_table).__name__}."
        )

    lookup_result: _OutputTableLookupResult | None = None
    if output_table.get_time_index_meta_table() is None:
        lookup_result = _bind_registered_output_table(output_table)

    output_metadata = output_table.get_time_index_meta_table()
    if output_metadata is None:
        raise ValueError(
            _unbound_output_table_message(
                output_table,
                context=context,
                lookup_result=lookup_result,
            )
        )
    if not isinstance(output_metadata, TimeIndexMetaTable):
        raise TypeError(
            f"{context} output_table must bind TimeIndexMetaTable metadata; "
            f"got {type(output_metadata).__name__}."
        )
    if output_table.get_meta_table_uid() in (None, ""):
        raise ValueError(f"{context} output_table must provide a MetaTable UID.")
    if output_table.get_data_source_uid() in (None, ""):
        raise ValueError(f"{context} output_table must provide a data-source UID.")
    return output_table


def _bind_registered_output_table(
    output_table: type[PlatformTimeIndexMetaTable],
) -> _OutputTableLookupResult:
    lookup_result = _registered_output_table_lookup(output_table)
    if len(lookup_result.matches) == 1:
        output_table._bind_meta_table(lookup_result.matches[0])
    return lookup_result


def _registered_output_table_lookup(
    output_table: type[PlatformTimeIndexMetaTable],
) -> _OutputTableLookupResult:
    table_name = _output_table_physical_table_name(output_table)
    physical_schema = _output_table_physical_schema(output_table)
    (
        data_source_uid,
        data_source_resolution_error,
        data_source_mismatch,
    ) = _output_table_lookup_data_source_uid(output_table)

    if data_source_mismatch is not None:
        return _OutputTableLookupResult(
            data_source_uid=data_source_uid,
            physical_schema=physical_schema,
            table_name=table_name,
            filters={},
            matches=[],
            data_source_resolution_error=data_source_resolution_error,
            data_source_mismatch=data_source_mismatch,
        )

    if table_name and physical_schema and data_source_uid:
        filters = {
            "data_source__uid": data_source_uid,
            "physical_schema__in": [physical_schema],
            "physical_table_name__in": [table_name],
            "limit": _OUTPUT_TABLE_LOOKUP_LIMIT,
        }
        raw_matches = TimeIndexMetaTable.filter_by_body(**filters)
        matches = [
            match
            for match in raw_matches
            if _time_index_meta_table_identity(match)
            == (data_source_uid, physical_schema, table_name)
            and _time_index_meta_table_is_active(match)
        ]
        return _OutputTableLookupResult(
            data_source_uid=data_source_uid,
            physical_schema=physical_schema,
            table_name=table_name,
            filters=filters,
            matches=matches,
            data_source_resolution_error=data_source_resolution_error,
        )

    return _OutputTableLookupResult(
        data_source_uid=data_source_uid,
        physical_schema=physical_schema,
        table_name=table_name,
        filters={},
        matches=[],
        data_source_resolution_error=data_source_resolution_error,
    )


def _output_table_lookup_data_source_uid(
    output_table: type[PlatformTimeIndexMetaTable],
) -> tuple[str | None, str | None, tuple[str, str] | None]:
    explicit_uid = output_table.get_data_source_uid()
    explicit_uid = str(explicit_uid) if explicit_uid not in (None, "") else None

    session_uid: str | None = None
    resolution_error: str | None = None
    try:
        session_data_source = get_session_data_source()
    except Exception as exc:
        resolution_error = str(exc)
    else:
        resolved_uid = getattr(session_data_source, "uid", None) or getattr(
            session_data_source,
            "data_source_uid",
            None,
        )
        if resolved_uid not in (None, ""):
            session_uid = str(resolved_uid)

    if explicit_uid is not None:
        if session_uid is not None and explicit_uid != session_uid:
            return explicit_uid, resolution_error, (explicit_uid, session_uid)
        return explicit_uid, resolution_error, None
    return session_uid, resolution_error, None


def _time_index_meta_table_is_active(meta_table: TimeIndexMetaTable) -> bool:
    status = getattr(meta_table, "provisioning_status", None)
    if status in (None, ""):
        return True
    return str(status) == "active"


def _output_table_physical_schema(
    output_table: type[PlatformTimeIndexMetaTable],
) -> str | None:
    schema_getter = getattr(output_table, "get_physical_schema", None)
    physical_schema = schema_getter() if callable(schema_getter) else None
    if physical_schema not in (None, ""):
        return str(physical_schema)
    table = getattr(output_table, "__table__", None)
    table_schema = getattr(table, "schema", None)
    if table_schema not in (None, ""):
        return str(table_schema)
    return _DEFAULT_POSTGRES_SCHEMA


def _output_table_physical_table_name(
    output_table: type[PlatformTimeIndexMetaTable],
) -> str | None:
    physical_table_name = output_table.get_physical_table_name()
    if physical_table_name not in (None, ""):
        return str(physical_table_name)
    table = getattr(output_table, "__table__", None)
    table_name = getattr(table, "name", None)
    if table_name not in (None, ""):
        return str(table_name)
    return None


def _output_table_lookup_label(output_table: type[PlatformTimeIndexMetaTable]) -> str:
    physical_schema = _output_table_physical_schema(output_table) or "<unknown-schema>"
    table_name = _output_table_physical_table_name(output_table) or "<unknown-table>"
    return f"{output_table.__name__}(schema={physical_schema}, table={table_name})"


def _unbound_output_table_message(
    output_table: type[PlatformTimeIndexMetaTable],
    *,
    context: str,
    lookup_result: _OutputTableLookupResult | None,
) -> str:
    label = _output_table_lookup_label(output_table)
    identity = _output_table_identity_summary(output_table)
    message = (
        f"{context} output_table class is not bound to backend TimeIndexMetaTable "
        f"metadata in this Python process for {label}. {identity} "
        "Expected exactly one backend TimeIndexMetaTable catalog row before "
        "constructing a TimeIndexTableUpdater."
    )
    if lookup_result is None:
        return (
            f"{message} The SDK did not run a backend lookup because the model "
            "already appeared to be bound before validation failed. Check the "
            "bound MetaTable UID and data-source UID on the storage model."
        )
    if lookup_result.table_name is None:
        return (
            f"{message} The SDK could not determine a physical table name from "
            "the storage model, so no backend catalog lookup was possible."
        )
    if lookup_result.physical_schema is None:
        return (
            f"{message} The SDK could not determine a physical schema from "
            "the storage model, so no backend catalog lookup was possible."
        )
    if lookup_result.data_source_uid is None:
        resolution_detail = ""
        if lookup_result.data_source_resolution_error:
            resolution_detail = (
                " CodeRepositoryBranch/session default data-source resolution failed: "
                f"{lookup_result.data_source_resolution_error}."
            )
        return (
            f"{message} The SDK could not determine a data-source UID from "
            "the storage model or the current CodeRepositoryBranch/session default, so no "
            f"backend catalog lookup was possible.{resolution_detail}"
        )
    if lookup_result.data_source_mismatch is not None:
        explicit_uid, session_uid = lookup_result.data_source_mismatch
        return (
            f"{message} Storage model data-source UID {explicit_uid!r} does "
            "not match current CodeRepositoryBranch/session default data-source UID "
            f"{session_uid!r}; refusing to bind this storage model."
        )

    filters = _format_lookup_filters(lookup_result.filters)
    match_count = len(lookup_result.matches)
    if match_count == 0:
        return (
            f"{message} Lookup used TimeIndexMetaTable.filter_by_body({filters}) "
            "and found no backend TimeIndexMetaTable catalog row. This usually "
            "means the exact storage model/table has not been reserved and "
            "finalized by its migration provider. If the SQL table already exists "
            "without a TimeIndexMetaTable catalog row, it is still unusable by "
            "time-index table updaters until migration finalization creates that row. "
            "Add this exact storage model to the relevant migration provider "
            "or dynamic scoped provider and run the provider upgrade before "
            "constructing the TimeIndexTableUpdater."
        )

    candidates = "; ".join(
        _time_index_meta_table_candidate_summary(match)
        for match in lookup_result.matches[:_OUTPUT_TABLE_LOOKUP_LIMIT]
    )
    qualifier = "at least " if match_count >= _OUTPUT_TABLE_LOOKUP_LIMIT else ""
    return (
        f"{message} Lookup used TimeIndexMetaTable.filter_by_body({filters}) "
        f"and found {qualifier}{match_count} matching backend TimeIndexMetaTable "
        "catalog rows, so the SDK refused to guess. Matching rows: "
        f"{candidates}. Remove or repair the duplicate catalog state so exactly "
        "one row owns this physical table."
    )


def _output_table_identity_summary(output_table: type[PlatformTimeIndexMetaTable]) -> str:
    parts = []
    identifier = getattr(output_table, "__metatable_identifier__", None)
    if identifier not in (None, ""):
        parts.append(f"identifier={identifier!r}")
    try:
        contract_hash = compute_metatable_contract_hash(output_table)
    except Exception:
        contract_hash = None
    if contract_hash not in (None, ""):
        parts.append(f"contract_hash={contract_hash!r}")
    data_source_uid = output_table.get_data_source_uid()
    if data_source_uid not in (None, ""):
        parts.append(f"model_data_source_uid={data_source_uid!r}")
    physical_schema = _output_table_physical_schema(output_table)
    if physical_schema not in (None, ""):
        parts.append(f"physical_schema={physical_schema!r}")
    table_name = _output_table_physical_table_name(output_table)
    if table_name not in (None, ""):
        parts.append(f"physical_table_name={table_name!r}")
    if not parts:
        return "Storage identity is incomplete."
    return "Storage identity: " + ", ".join(parts) + "."


def _format_lookup_filters(filters: dict[str, Any]) -> str:
    return ", ".join(f"{key}={value!r}" for key, value in filters.items())


def _time_index_meta_table_candidate_summary(meta_table: TimeIndexMetaTable) -> str:
    parts = []
    for attr in (
        "uid",
        "data_source_uid",
        "physical_schema",
        "identifier",
        "physical_table_name",
        "provisioning_status",
    ):
        value = getattr(meta_table, attr, None)
        if value not in (None, ""):
            parts.append(f"{attr}={value}")
    return "{" + ", ".join(parts) + "}" if parts else "{unidentified TimeIndexMetaTable}"


def _time_index_meta_table_identity(
    meta_table: TimeIndexMetaTable,
) -> tuple[str, str, str] | None:
    data_source_uid = getattr(meta_table, "data_source_uid", None)
    if data_source_uid in (None, ""):
        data_source = getattr(meta_table, "data_source", None)
        if isinstance(data_source, dict):
            data_source_uid = data_source.get("uid") or data_source.get("data_source_uid")
        else:
            data_source_uid = getattr(data_source, "uid", None) or getattr(
                data_source,
                "data_source_uid",
                None,
            )
    physical_schema = getattr(meta_table, "physical_schema", None)
    if physical_schema in (None, ""):
        table_contract = getattr(meta_table, "table_contract", None)
        if isinstance(table_contract, dict):
            physical = table_contract.get("physical") or {}
            if isinstance(physical, dict):
                physical_schema = physical.get("schema") or physical.get("schema_")
    if physical_schema in (None, ""):
        physical_schema = _DEFAULT_POSTGRES_SCHEMA
    table_name = getattr(meta_table, "physical_table_name", None)
    if data_source_uid in (None, "") or table_name in (None, ""):
        return None
    return str(data_source_uid), str(physical_schema), str(table_name)


class BaseTimeIndexTableUpdateManager:
    UPDATE_CLASS: ClassVar[type[Any] | None] = None
    UPDATE_DETAILS_CLASS: ClassVar[type[Any] | None] = None

    UPDATE_GET_OR_NONE_OUTPUT_LOOKUP: ClassVar[str] = "output_table__uid"
    UPDATE_CREATE_OUTPUT_LOOKUP: ClassVar[str] = "output_table_uid"
    TIME_INDEXED_PROFILE_ATTR: ClassVar[str] = "time_indexed_profile"

    def __init__(
        self,
        update_hash: str,
        output_table: type[PlatformTimeIndexMetaTable],
        description: str | None = None,
        class_name: str | None = None,
        table_update: Any | None = None,
    ):
        self.update_hash: str = update_hash
        self.description: str | None = description
        self.logger = logger

        self.table_model_loaded: bool = False
        self.class_name: str | None = class_name

        self._table_update_future: Future | None = None
        self._table_update_cached: Any | None = None
        self._table_update_lock = threading.Lock()
        self.output_table: type[PlatformTimeIndexMetaTable] = self._validate_output_table(
            output_table
        )

        if self.update_hash is not None:
            self.synchronize_table_update(table_update=table_update)

    @staticmethod
    def _validate_output_table(
        output_table: type[PlatformTimeIndexMetaTable],
    ) -> type[PlatformTimeIndexMetaTable]:
        return ensure_registered_output_table(output_table, context="TimeIndexTableUpdateManager")

    @property
    def output_metadata(self) -> Any:
        output_metadata = self.output_table.get_time_index_meta_table()
        if output_metadata is None:
            raise ValueError(
                "TimeIndexTableUpdateManager output_table registration metadata is unavailable after register()."
            )
        return output_metadata

    @property
    def data_source(self) -> DataSource:
        data_source = getattr(self.output_metadata, "data_source", None)
        if data_source not in (None, "") and not isinstance(data_source, int | str):
            return data_source
        return DataSource.get_by_uid(self.output_table.get_data_source_uid())

    def _get_time_indexed_profile(self) -> Any | None:
        return getattr(self.output_metadata, self.TIME_INDEXED_PROFILE_ATTR, None)

    def _build_update_get_or_none_kwargs(
        self,
        *,
        include_relations_detail: bool,
    ) -> dict[str, Any]:
        kwargs = {
            "update_hash": self.update_hash,
            "include_relations_detail": include_relations_detail,
        }
        kwargs[self.UPDATE_GET_OR_NONE_OUTPUT_LOOKUP] = self.output_table.get_meta_table_uid()
        return kwargs

    def _build_update_get_or_create_kwargs(
        self,
        *,
        local_configuration: dict,
    ) -> dict[str, Any]:
        validate_canonical_configuration(local_configuration)
        kwargs = dict(
            update_hash=self.update_hash,
            build_configuration=local_configuration,
        )
        kwargs[self.UPDATE_CREATE_OUTPUT_LOOKUP] = self.output_table.get_meta_table_uid()
        return kwargs

    def _should_refresh_update_when_remote_exists(self) -> bool:
        return True

    @property
    def metadata(self) -> Any | None:
        return self.output_metadata

    @property
    def remote_build_configuration(self) -> dict | None:
        table_update = self.table_update
        if table_update is None:
            return None
        if isinstance(table_update, dict):
            return table_update.get("build_configuration")
        return getattr(table_update, "build_configuration", None)

    def synchronize_table_update(self, table_update: Any | None) -> None:
        if table_update is not None:
            self.set_table_update(table_update)
        else:
            self.set_table_update_lazy(force_registry=True, include_relations_detail=True)

    def set_table_update(self, table_update: Any) -> None:
        self._table_update_cached = table_update

    @property
    def table_update(self) -> Any:
        with self._table_update_lock:
            if self._table_update_cached is None:
                if self._table_update_future is None:
                    self.set_table_update_lazy(force_registry=True)
                table_update = self._table_update_future.result()
                if table_update is not None:
                    self.set_table_update(table_update)
            return self._table_update_cached

    @property
    def local_build_configuration(self) -> dict:
        return self.table_update.build_configuration

    def set_table_update_lazy_callback(self, fut: Future) -> None:
        try:
            fut.result()
        except Exception as exc:
            raise exc
        self.set_table_update_lazy(force_registry=True)

    def set_table_update_lazy(
        self, force_registry: bool = True, include_relations_detail: bool = True
    ) -> None:
        with self._table_update_lock:
            if force_registry:
                self._table_update_cached = None
            new_future = Future()
            self._table_update_future = new_future
            future_registry.add_future(new_future)

        def _get_or_none_table_update():
            try:
                result = self.UPDATE_CLASS.get_or_none(
                    **self._build_update_get_or_none_kwargs(
                        include_relations_detail=include_relations_detail
                    )
                )
                if result is None:
                    meta_table_uid = self.output_table.get_meta_table_uid()
                    physical_schema = _output_table_physical_schema(self.output_table)
                    physical_table_name = _output_table_physical_table_name(self.output_table)
                    self.logger.warning(
                        f"TimeIndexTableUpdate {self.update_hash} for MetaTable {meta_table_uid} "
                        f"(physical_schema={physical_schema}, "
                        f"physical_table_name={physical_table_name}) not found in backend"
                    )
                new_future.set_result(result)
            except Exception as exc:
                new_future.set_exception(exc)
            finally:
                future_registry.remove_future(new_future)

        thread = threading.Thread(
            target=_get_or_none_table_update,
            name=f"TableUpdateLookup-{self.update_hash}",
            daemon=False,
        )
        thread.start()

    def connect_update_dependency(self, dependency: Any) -> None:
        dependency_uid = getattr(dependency.table_update, "uid", None)
        if dependency_uid in (None, ""):
            raise ValueError("Upstream TimeIndexTableUpdate must have a uid.")
        self.table_update.connect_update_dependency(
            upstream_update_uid=str(dependency_uid),
        )

    def connect_table_dependency(self, dependency: Any) -> None:
        self.table_update.connect_table_dependency(
            time_index_meta_table_uid=dependency.output_table_uid,
        )

    def get_all_dependencies_update_priority(self) -> pd.DataFrame:
        return self.table_update.get_all_dependencies_update_priority()

    def clear_dependencies(self) -> Any:
        result = self.table_update.clear_dependencies()
        self.set_table_update_lazy(force_registry=True, include_relations_detail=True)
        return result

    def set_ogm_dependencies_unlinked(self) -> None:
        self.set_table_update(self.table_update.patch(ogm_dependencies_linked=False))

    def set_ogm_dependencies_linked(self) -> None:
        self.set_table_update(self.table_update.patch(ogm_dependencies_linked=True))

    @property
    def update_details(self) -> Any | None:
        return self.table_update.update_details

    @property
    def time_indexed_profile(self) -> Any | None:
        return self._get_time_indexed_profile()

    @property
    def persist_size(self) -> int:
        return getattr(self.output_metadata, "table_size", 0)

    def table_update_exists(self) -> bool:
        return self.output_metadata is not None

    def ensure_table_update(
        self,
        local_configuration: dict,
    ) -> None:
        validate_canonical_configuration(local_configuration)
        if self._should_refresh_update_when_remote_exists():
            self.set_table_update_lazy(force_registry=True, include_relations_detail=True)

        self._ensure_table_update_exists(
            local_configuration=local_configuration,
        )

    def _ensure_table_update_exists(
        self,
        local_configuration: dict,
    ) -> None:
        local_build_configuration = None
        if self.table_update is not None:
            local_build_configuration = self.local_build_configuration
        if local_build_configuration is None:
            logger.debug(f"table_update {self.update_hash} does not exist creating")
            local_update = self.UPDATE_CLASS.get_or_none(
                **self._build_update_get_or_none_kwargs(include_relations_detail=False)
            )
            if local_update is None:
                table_update = self.UPDATE_CLASS.get_or_create(
                    **self._build_update_get_or_create_kwargs(
                        local_configuration=local_configuration,
                    ),
                )
            else:
                table_update = local_update

            self.set_table_update(table_update=table_update)

    def build_update_details(self, source_class_name: str) -> None:
        if self.UPDATE_DETAILS_CLASS is None:
            raise ValueError("UPDATE_DETAILS_CLASS must be configured to patch update details.")
        table_update_uid = getattr(self.table_update, "uid", None)
        if table_update_uid in (None, ""):
            raise ValueError("TimeIndexTableUpdate uid is required to patch update details.")

        with self._table_update_lock:
            self._table_update_future = Future()
            future_registry.add_future(self._table_update_future)

        future = Future()
        future_registry.add_future(future)

        def _update_task():
            try:
                self.UPDATE_DETAILS_CLASS.patch_for_table_update_uid(
                    table_update_uid,
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
        future.add_done_callback(self.set_table_update_lazy_callback)

    def patch_table(self, **kwargs) -> None:
        self.output_metadata.patch(**kwargs)

    def protect_from_deletion(self, protect_from_deletion: bool = True) -> None:
        self.output_metadata.patch(protect_from_deletion=protect_from_deletion)

    def get_df_between_dates(self, *args, **kwargs) -> pd.DataFrame:
        return self.data_source.get_data_by_time_index(
            *args,
            table_update=self.table_update,
            **kwargs,
        )

    def get_last_observation(
        self,
        *,
        dimension_filters: dict[str, list[Any]] | None = None,
        index_coordinates: list[dict[str, Any]] | None = None,
        dimension_range_map: list[dict[str, Any]] | None = None,
    ):
        return self.output_metadata.get_last_observation(
            dimension_filters=dimension_filters,
            index_coordinates=index_coordinates,
            dimension_range_map=dimension_range_map,
        )

    def delete_table(self) -> None:
        class_type = self.data_source.class_type
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
            db_interface.drop_table(self.output_metadata.physical_table_name)

        self.output_metadata.delete()

    @tracer.start_as_current_span("TableUpdate: Persist Data")
    def persist_updated_data(
        self,
        temp_df: pd.DataFrame,
        overwrite: bool = False,
    ) -> bool:
        persisted = False
        if not temp_df.empty:
            if overwrite is True:
                self.logger.warning("Values will be overwritten")

            self._table_update_cached = self.table_update.upsert_data_into_table(
                data=temp_df,
                data_source=self.data_source,
                overwrite=overwrite,
            )

            persisted = True
        return persisted

    def get_update_statistics_for_table(self) -> UpdateStatistics:
        return self.output_metadata.get_data_updates()

    def is_local_relation_tree_set(self) -> bool:
        return self.table_update.ogm_dependencies_linked


class TimeIndexTableUpdateManager(BaseTimeIndexTableUpdateManager):
    UPDATE_CLASS = TimeIndexTableUpdate
    UPDATE_DETAILS_CLASS = TimeIndexTableUpdateDetails
    UPDATE_GET_OR_NONE_OUTPUT_LOOKUP = "output_table__uid"
    UPDATE_CREATE_OUTPUT_LOOKUP = "output_table_uid"

    @classmethod
    def get_from_output_table(
        cls, output_table: Any, *args, **kwargs
    ) -> TimeIndexTableUpdateManager:
        return TimeScaleTimeIndexTableUpdateManager(*args, output_table=output_table, **kwargs)


class TimeScaleTimeIndexTableUpdateManager(TimeIndexTableUpdateManager):
    """
    Main Controller to interact with backend-backed table storage.
    """

    def get_table_schema(self, _):
        output_metadata = self.output_metadata
        profile = getattr(output_metadata, "time_indexed_profile", None)
        if isinstance(profile, dict) and profile.get("column_dtypes_map"):
            return profile["column_dtypes_map"]

        column_dtypes_map = getattr(output_metadata, "column_dtypes_map", None)
        if column_dtypes_map is not None:
            return column_dtypes_map

        table_contract = getattr(output_metadata, "table_contract", None)
        contract_columns = []
        if isinstance(table_contract, dict):
            contract_columns = table_contract.get("columns") or []
        elif table_contract is not None:
            contract_columns = getattr(table_contract, "columns", []) or []

        if not contract_columns:
            table = getattr(self.output_table, "__table__", None)
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
