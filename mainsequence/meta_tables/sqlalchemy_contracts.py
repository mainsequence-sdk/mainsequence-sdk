from __future__ import annotations

import contextlib
import contextvars
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, ClassVar
from uuid import UUID

from sqlalchemy import Table as _SQLAlchemyTable
from sqlalchemy import inspect as _sqlalchemy_inspect
from sqlalchemy.orm import declared_attr as _sqlalchemy_declared_attr
from sqlalchemy.schema import BLANK_SCHEMA as _SQLALCHEMY_BLANK_SCHEMA

from mainsequence.client.dtype_codec import (
    is_temporal_token,
    sqlalchemy_backend_type,
    sqlalchemy_type_to_token,
)
from mainsequence.client.metatables import (
    DynamicTableDataSource,
    MetaTable,
    MetaTableColumnContract,
    MetaTableContract,
    MetaTablePhysicalContract,
    MetaTableRegistrationRequest,
    TimeIndexMetaTable,
)

from .hashing import build_meta_table_configured_storage_hash
from .schema_names import schema_index_name

DEFAULT_PLATFORM_MANAGED_PROVISIONING = {
    "create_table": True,
    "if_not_exists": True,
}
SERVER_GENERATED_UUID_DEFAULT = "gen_random_uuid()"
DEFAULT_POSTGRES_SCHEMA = "public"
_PLATFORM_MANAGED_MIGRATION_REGISTRATION_CONTEXT: contextvars.ContextVar[bool] = (
    contextvars.ContextVar(
        "mainsequence_platform_managed_migration_registration_context",
        default=False,
    )
)


@dataclass
class _MetaTableRegistrationState:
    status: str
    model: type[Any]
    meta_table: MetaTable | None = None
    stack: tuple[str, ...] = ()


_METATABLE_REGISTRATION_REGISTRY: dict[str, _MetaTableRegistrationState] = {}


@contextlib.contextmanager
def platform_managed_migration_registration_context() -> Any:
    token = _PLATFORM_MANAGED_MIGRATION_REGISTRATION_CONTEXT.set(True)
    try:
        yield
    finally:
        _PLATFORM_MANAGED_MIGRATION_REGISTRATION_CONTEXT.reset(token)


def _require_platform_managed_migration_registration_context(model: type[Any]) -> None:
    if _PLATFORM_MANAGED_MIGRATION_REGISTRATION_CONTEXT.get():
        return
    model_name = getattr(model, "__qualname__", repr(model))
    try:
        identifier = resolve_metatable_identifier(model) or model_name
    except Exception:
        identifier = model_name
    raise ValueError(
        "Platform-managed MetaTable "
        f"{identifier!r} is migration-managed and cannot be registered directly. "
        "Run `mainsequence migrations upgrade --provider <provider> head`."
    )


def _configured_storage_hash_for_model(
    model_or_table: Any,
    *,
    namespace: str | None = None,
    schema: str | None = None,
    hash_namespace: str | None = None,
    extra_hash_components: Mapping[str, Any] | None = None,
) -> str:
    table = _resolve_table(model_or_table)
    resolved_schema = _resolve_schema(table, schema=schema)
    resolved_namespace = _resolve_namespace(model_or_table, namespace=namespace)
    resolved_hash_namespace = _resolve_hash_namespace(
        model_or_table,
        hash_namespace=hash_namespace,
    )
    resolved_extra_hash_components = _resolve_extra_hash_components(
        model_or_table,
        extra_hash_components=extra_hash_components,
    )
    return _build_configured_storage_hash(
        namespace=resolved_namespace,
        schema=resolved_schema,
        table_storage_identity=_configured_table_storage_identity(model_or_table, table=table),
        hash_namespace=resolved_hash_namespace,
        extra_hash_components=resolved_extra_hash_components,
    )


def _time_index_mapper_args(cls: type[Any]) -> dict[str, list[Any]]:
    table = getattr(cls, "__table__", None)
    columns = getattr(table, "c", None)
    index_names = getattr(cls, "__index_names__", None)
    time_index_name = getattr(cls, "__time_index_name__", None) or "time_index"
    resolved_index_names = list(index_names or [time_index_name])
    if columns is None or not resolved_index_names:
        return {}

    mapper_primary_key = []
    for name in resolved_index_names:
        if name not in columns:
            return {}
        mapper_primary_key.append(columns[name])
    return {"primary_key": mapper_primary_key}


class PlatformManagedMetaTable:
    """SQLAlchemy declarative base mixin for platform-managed MetaTables.

    The class derives the logical storage hash from storage-relevant metadata
    plus the SQLAlchemy table storage shape, builds the platform registration
    request, and registers the MetaTable through TS Manager. Binding records the
    returned MetaTable UID and storage metadata while preserving the authored
    SQLAlchemy table name.
    """

    __metatable__: ClassVar[MetaTable | None] = None
    __metatable_uid__: ClassVar[str | None] = None
    __metatable_data_source_uid__: ClassVar[str | None] = None
    __metatable_storage_hash__: ClassVar[str | None] = None
    __metatable_physical_table_name__: ClassVar[str | None] = None
    __metatable_description__: ClassVar[str | None] = None
    __metatable_labels__: ClassVar[Sequence[str] | None] = None
    __metatable_provisioning__: ClassVar[Mapping[str, Any] | None] = None
    __metatable_protect_from_deletion__: ClassVar[bool | None] = None
    __metatable_introspect__: ClassVar[bool | None] = None
    __metatable_hash_namespace__: ClassVar[str | None] = None
    __metatable_extra_hash_components__: ClassVar[Mapping[str, Any] | None] = None

    @classmethod
    def __table_cls__(cls, *args: Any, **kwargs: Any) -> Any:
        if len(args) < 2:
            raise TypeError("SQLAlchemy __table_cls__ expected name, metadata, and columns.")

        name, metadata, *table_items = args
        kwargs = dict(kwargs)
        schema = _normalize_sqlalchemy_schema(
            kwargs.get("schema")
            if "schema" in kwargs
            else _resolve_class_schema(cls, metadata=metadata)
        )
        if schema is None:
            kwargs["schema"] = _SQLALCHEMY_BLANK_SCHEMA
        else:
            kwargs["schema"] = schema

        from sqlalchemy import Table

        return Table(str(name), metadata, *table_items, **kwargs)

    @classmethod
    def build_registration_request(
        cls,
        *,
        data_source: DynamicTableDataSource | None = None,
        data_source_uid: str | None = None,
        identifier: str | None = None,
        namespace: str | None = None,
        description: str | None = None,
        labels: Sequence[str] | None = None,
        protect_from_deletion: bool | None = None,
        provisioning: Mapping[str, Any] | None = None,
        introspect: bool | None = None,
        hash_namespace: str | None = None,
        extra_hash_components: Mapping[str, Any] | None = None,
        enforce_storage_hash_name: bool = True,
    ) -> MetaTableRegistrationRequest:
        resolved_data_source_uid = _resolve_model_data_source_uid(
            cls,
            data_source=data_source,
            data_source_uid=data_source_uid,
        )
        return platform_managed_registration_request_from_sqlalchemy_model(
            cls,
            data_source_uid=resolved_data_source_uid,
            identifier=identifier,
            namespace=namespace,
            description=description,
            labels=labels,
            protect_from_deletion=protect_from_deletion,
            provisioning=provisioning,
            introspect=introspect,
            hash_namespace=hash_namespace,
            extra_hash_components=extra_hash_components,
            enforce_storage_hash_name=enforce_storage_hash_name,
        )

    @classmethod
    def register(
        cls,
        *,
        data_source: DynamicTableDataSource | None = None,
        data_source_uid: str | None = None,
        timeout: int | float | tuple[float, float] | None = None,
        _registration_stack: tuple[str, ...] = (),
    ) -> MetaTable:
        _require_platform_managed_migration_registration_context(cls)
        storage_hash = cls.get_storage_hash()
        registry_meta_table = _begin_local_registration(
            cls,
            storage_hash=storage_hash,
            stack=_registration_stack,
        )
        if registry_meta_table is not None:
            return registry_meta_table

        try:
            request = cls.build_registration_request(
                data_source=data_source,
                data_source_uid=data_source_uid,
            )
            meta_table = MetaTable.register(request, timeout=timeout)
            cls._bind_meta_table(meta_table)
            _complete_local_registration(
                storage_hash=storage_hash, model=cls, meta_table=meta_table
            )
            return meta_table
        except Exception:
            _clear_failed_local_registration(storage_hash=storage_hash, model=cls)
            raise

    @classmethod
    def _bind_meta_table(cls, meta_table: MetaTable) -> MetaTable:
        """Attach an already-created backend MetaTable resource to this authoring model."""
        cls.__metatable__ = meta_table
        cls.__metatable_uid__ = _required_meta_table_uid(meta_table)
        cls.__metatable_storage_hash__ = _meta_table_storage_hash(meta_table)
        cls.__metatable_physical_table_name__ = _bound_physical_table_name(cls, meta_table)
        cls.__metatable_data_source_uid__ = _required_meta_table_data_source_uid(meta_table)
        return meta_table

    @classmethod
    def get_meta_table(cls) -> MetaTable | None:
        return getattr(cls, "__metatable__", None)

    @classmethod
    def get_meta_table_uid(cls) -> str | None:
        uid = getattr(cls, "__metatable_uid__", None) or getattr(cls, "meta_table_uid", None)
        return _coerce_optional_uid(uid)

    @classmethod
    def get_data_source_uid(cls) -> str | None:
        data_source_uid = getattr(cls, "__metatable_data_source_uid__", None) or getattr(
            cls, "data_source_uid", None
        )
        return _coerce_optional_uid(data_source_uid)

    @classmethod
    def get_storage_hash(cls) -> str:
        storage_hash = getattr(cls, "__metatable_storage_hash__", None)
        if storage_hash not in (None, ""):
            return str(storage_hash)
        return _configured_storage_hash_for_model(cls)

    @classmethod
    def get_physical_table_name(cls) -> str | None:
        physical_table_name = getattr(cls, "__metatable_physical_table_name__", None)
        if physical_table_name not in (None, ""):
            return str(physical_table_name)
        return None


class PlatformTimeIndexMetaTable(PlatformManagedMetaTable):
    """SQLAlchemy declarative base mixin for platform-managed TimeIndexMetaTable.

    This is the SDK authoring surface for time-indexed DataNode storage. It
    reuses the MetaTable column/type projection, but registers through the
    time-indexed MetaTable endpoint and validates the opinionated table shape:
    the first index must be the time index, and any extra index dimensions are
    ordinary non-null table columns.
    """

    __time_index_metadata__: ClassVar[TimeIndexMetaTable | None] = None

    __mapper_args__ = _sqlalchemy_declared_attr.directive(_time_index_mapper_args)

    @classmethod
    def _bind_meta_table(cls, meta_table: TimeIndexMetaTable) -> TimeIndexMetaTable:
        if not isinstance(meta_table, TimeIndexMetaTable):
            raise TypeError(
                "PlatformTimeIndexMetaTable._bind_meta_table requires TimeIndexMetaTable."
            )
        super()._bind_meta_table(meta_table)
        cls.__time_index_metadata__ = meta_table
        return meta_table

    @classmethod
    def get_time_index_metadata(cls) -> TimeIndexMetaTable | None:
        return getattr(cls, "__time_index_metadata__", None)

    @classmethod
    def __table_cls__(cls, *args: Any, **kwargs: Any) -> Any:
        if len(args) < 2:
            raise TypeError("SQLAlchemy __table_cls__ expected name, metadata, and columns.")

        name, metadata, *table_items = args
        kwargs = dict(kwargs)
        schema = _normalize_sqlalchemy_schema(
            kwargs.get("schema")
            if "schema" in kwargs
            else _resolve_class_schema(cls, metadata=metadata)
        )
        if schema is None:
            kwargs["schema"] = _SQLALCHEMY_BLANK_SCHEMA
        else:
            kwargs["schema"] = schema

        columns = [item for item in table_items if _looks_like_column(item)]
        time_index_name = _resolve_time_index_name(cls)
        index_names = _resolve_time_index_names(cls, time_index_name=time_index_name)
        _validate_time_index_contract(
            columns=columns,
            time_index_name=time_index_name,
            index_names=index_names,
        )

        from sqlalchemy import Table

        table = Table(str(name), metadata, *table_items, **kwargs)
        _ensure_time_index_unique_grain_index(table=table, index_names=index_names)
        return table

    @classmethod
    def build_registration_request(
        cls,
        *,
        data_source: DynamicTableDataSource | None = None,
        data_source_uid: str | None = None,
        identifier: str | None = None,
        namespace: str | None = None,
        description: str | None = None,
        labels: Sequence[str] | None = None,
        protect_from_deletion: bool | None = None,
        provisioning: Mapping[str, Any] | None = None,
        hash_namespace: str | None = None,
        extra_hash_components: Mapping[str, Any] | None = None,
        enforce_storage_hash_name: bool = True,
        time_index_name: str | None = None,
        index_names: Sequence[str] | None = None,
        storage_layout: Mapping[str, Any] | None = None,
    ) -> Any:
        resolved_data_source_uid = _resolve_model_data_source_uid(
            cls,
            data_source=data_source,
            data_source_uid=data_source_uid,
        )
        return time_indexed_registration_request_from_sqlalchemy_model(
            cls,
            data_source_uid=resolved_data_source_uid,
            identifier=identifier,
            namespace=namespace,
            description=description,
            labels=labels,
            protect_from_deletion=protect_from_deletion,
            provisioning=provisioning,
            hash_namespace=hash_namespace,
            extra_hash_components=extra_hash_components,
            enforce_storage_hash_name=enforce_storage_hash_name,
            time_index_name=time_index_name,
            index_names=index_names,
            storage_layout=storage_layout,
        )

    @classmethod
    def register(
        cls,
        *,
        data_source: DynamicTableDataSource | None = None,
        data_source_uid: str | None = None,
        timeout: int | float | tuple[float, float] | None = None,
        _registration_stack: tuple[str, ...] = (),
    ) -> Any:
        from mainsequence.client.metatables import TimeIndexMetaTable

        _require_platform_managed_migration_registration_context(cls)
        storage_hash = cls.get_storage_hash()
        registry_meta_table = _begin_local_registration(
            cls,
            storage_hash=storage_hash,
            stack=_registration_stack,
        )
        if registry_meta_table is not None:
            return registry_meta_table

        try:
            request = cls.build_registration_request(
                data_source=data_source,
                data_source_uid=data_source_uid,
            )
            time_index_metadata = TimeIndexMetaTable.register(request, timeout=timeout)
            cls._bind_meta_table(time_index_metadata)
            _complete_local_registration(
                storage_hash=storage_hash,
                model=cls,
                meta_table=time_index_metadata,
            )
            return time_index_metadata
        except Exception:
            _clear_failed_local_registration(storage_hash=storage_hash, model=cls)
            raise


def table_contract_from_sqlalchemy_model(
    model_or_table: Any,
    *,
    table_model_module: str | None = None,
    table_model_qualname: str | None = None,
    schema: str | None = None,
    include_physical_table_name: bool = True,
) -> MetaTableContract:
    table = _resolve_table(model_or_table)
    resolved_schema = _resolve_schema(table, schema=schema)
    module, qualname = _resolve_model_path(
        model_or_table,
        table_model_module=table_model_module,
        table_model_qualname=table_model_qualname,
    )

    return MetaTableContract(
        version="relational-table.v1",
        authoring={
            "table_model": {
                "kind": "sqlalchemy",
                "module": module,
                "qualname": qualname,
            }
        },
        physical=MetaTablePhysicalContract(
            schema_=resolved_schema,
            table_name=_table_name(table) if include_physical_table_name else None,
        ),
        columns=[
            _column_contract(column, ordinal_position=position)
            for position, column in enumerate(_iter_columns(table))
        ],
        constraints=[],
    )


def time_indexed_registration_request_from_sqlalchemy_model(
    model_or_table: Any,
    *,
    data_source: DynamicTableDataSource | None = None,
    data_source_uid: str | None = None,
    identifier: str | None = None,
    namespace: str | None = None,
    description: str | None = None,
    labels: Sequence[str] | None = None,
    protect_from_deletion: bool | None = None,
    provisioning: Mapping[str, Any] | None = None,
    schema: str | None = None,
    hash_namespace: str | None = None,
    extra_hash_components: Mapping[str, Any] | None = None,
    enforce_storage_hash_name: bool = True,
    time_index_name: str | None = None,
    index_names: Sequence[str] | None = None,
    storage_layout: Mapping[str, Any] | None = None,
) -> Any:
    from mainsequence.client.metatables import TimeIndexMetaTableRegistrationRequest

    table = _resolve_table(model_or_table)
    resolved_schema = _resolve_schema(table, schema=schema)
    resolved_identifier = resolve_metatable_identifier(model_or_table, identifier=identifier)
    resolved_namespace = _resolve_namespace(model_or_table, namespace=namespace)
    resolved_description = _resolve_description(model_or_table, description=description)
    resolved_hash_namespace = _resolve_hash_namespace(
        model_or_table,
        hash_namespace=hash_namespace,
    )
    resolved_extra_hash_components = _resolve_extra_hash_components(
        model_or_table,
        extra_hash_components=extra_hash_components,
    )
    resolved_time_index_name = _resolve_time_index_name(
        model_or_table,
        time_index_name=time_index_name,
    )
    resolved_index_names = _resolve_time_index_names(
        model_or_table,
        time_index_name=resolved_time_index_name,
        index_names=index_names,
    )
    resolved_storage_layout = _resolve_time_index_storage_layout(
        model_or_table,
        storage_layout=storage_layout,
    )
    columns = _iter_columns(table)
    _validate_time_index_contract(
        columns=columns,
        time_index_name=resolved_time_index_name,
        index_names=resolved_index_names,
    )

    configured_storage_hash = _build_configured_storage_hash(
        namespace=resolved_namespace,
        schema=resolved_schema,
        table_storage_identity=_time_index_table_storage_identity(
            table,
            time_index_name=resolved_time_index_name,
            index_names=resolved_index_names,
            storage_layout=resolved_storage_layout,
        ),
        hash_namespace=resolved_hash_namespace,
        extra_hash_components=resolved_extra_hash_components,
    )
    storage_hash = _model_bound_storage_hash(model_or_table) or configured_storage_hash

    column_contracts = [
        _column_contract(column, ordinal_position=position).model_dump(
            mode="json",
            exclude_none=True,
        )
        for position, column in enumerate(columns)
    ]
    module, qualname = _resolve_model_path(
        model_or_table,
        table_model_module=None,
        table_model_qualname=None,
    )

    return TimeIndexMetaTableRegistrationRequest(
        data_source_uid=_resolve_model_data_source_uid(
            model_or_table,
            data_source=data_source,
            data_source_uid=data_source_uid,
        ),
        storage_hash=storage_hash,
        identifier=resolved_identifier,
        namespace=resolved_namespace,
        description=resolved_description,
        protect_from_deletion=_resolve_bool_metadata(
            model_or_table,
            value=protect_from_deletion,
            attr_name="__metatable_protect_from_deletion__",
            info_key="protect_from_deletion",
            default=False,
        ),
        labels=_resolve_labels(model_or_table, labels=labels),
        provisioning=_resolve_provisioning(model_or_table, provisioning=provisioning),
        time_index_name=resolved_time_index_name,
        table_contract={
            "version": "relational-table.v1",
            "table_kind": "time_indexed",
            "authoring": {
                "table_model": {
                    "kind": "sqlalchemy",
                    "module": module,
                    "qualname": qualname,
                },
                "time_indexed": {
                    "time_index_name": resolved_time_index_name,
                    "index_names": resolved_index_names,
                    **(
                        {"storage_layout": dict(resolved_storage_layout)}
                        if resolved_storage_layout
                        else {}
                    ),
                },
            },
            "physical": {
                "schema": resolved_schema,
                "table_name": _table_name(table),
            },
            "columns": column_contracts,
        },
    )


def platform_managed_registration_request_from_sqlalchemy_model(
    model_or_table: Any,
    *,
    data_source: DynamicTableDataSource | None = None,
    data_source_uid: str | None = None,
    identifier: str | None = None,
    namespace: str | None = None,
    description: str | None = None,
    labels: Sequence[str] | None = None,
    protect_from_deletion: bool | None = None,
    provisioning: Mapping[str, Any] | None = None,
    introspect: bool | None = None,
    schema: str | None = None,
    hash_namespace: str | None = None,
    extra_hash_components: Mapping[str, Any] | None = None,
    enforce_storage_hash_name: bool = True,
) -> MetaTableRegistrationRequest:
    table = _resolve_table(model_or_table)
    resolved_schema = _resolve_schema(table, schema=schema)
    resolved_identifier = resolve_metatable_identifier(model_or_table, identifier=identifier)
    resolved_namespace = _resolve_namespace(model_or_table, namespace=namespace)
    resolved_description = _resolve_description(model_or_table, description=description)
    resolved_hash_namespace = _resolve_hash_namespace(
        model_or_table,
        hash_namespace=hash_namespace,
    )
    resolved_extra_hash_components = _resolve_extra_hash_components(
        model_or_table,
        extra_hash_components=extra_hash_components,
    )
    configured_storage_hash = _build_configured_storage_hash(
        namespace=resolved_namespace,
        schema=resolved_schema,
        table_storage_identity=_table_storage_identity(table),
        hash_namespace=resolved_hash_namespace,
        extra_hash_components=resolved_extra_hash_components,
    )
    storage_hash = _model_bound_storage_hash(model_or_table) or configured_storage_hash

    table_contract = table_contract_from_sqlalchemy_model(
        model_or_table,
        schema=resolved_schema,
        include_physical_table_name=True,
    )
    return MetaTableRegistrationRequest(
        data_source_uid=_resolve_model_data_source_uid(
            model_or_table,
            data_source=data_source,
            data_source_uid=data_source_uid,
        ),
        management_mode="platform_managed",
        storage_hash=storage_hash,
        identifier=resolved_identifier,
        namespace=resolved_namespace,
        description=resolved_description,
        protect_from_deletion=_resolve_bool_metadata(
            model_or_table,
            value=protect_from_deletion,
            attr_name="__metatable_protect_from_deletion__",
            info_key="protect_from_deletion",
            default=False,
        ),
        labels=_resolve_labels(model_or_table, labels=labels),
        provisioning=_resolve_provisioning(model_or_table, provisioning=provisioning),
        introspect=_resolve_bool_metadata(
            model_or_table,
            value=introspect,
            attr_name="__metatable_introspect__",
            info_key="introspect",
            default=False,
        ),
        table_contract=table_contract,
    )


def external_registered_registration_request_from_sqlalchemy_model(
    model_or_table: Any,
    *,
    data_source_uid: str,
    storage_hash: str | None = None,
    identifier: str | None = None,
    namespace: str | None = None,
    description: str | None = None,
    labels: Sequence[str] | None = None,
    protect_from_deletion: bool = False,
    introspect: bool = True,
    schema: str | None = None,
    hash_namespace: str | None = None,
    extra_hash_components: Mapping[str, Any] | None = None,
) -> MetaTableRegistrationRequest:
    table = _resolve_table(model_or_table)
    resolved_schema = _resolve_schema(table, schema=schema)
    resolved_identifier = resolve_metatable_identifier(model_or_table, identifier=identifier)
    resolved_namespace = _resolve_namespace(model_or_table, namespace=namespace)
    resolved_description = _resolve_description(model_or_table, description=description)
    resolved_hash_namespace = _resolve_hash_namespace(
        model_or_table,
        hash_namespace=hash_namespace,
    )
    resolved_extra_hash_components = _resolve_extra_hash_components(
        model_or_table,
        extra_hash_components=extra_hash_components,
    )
    resolved_storage_hash = storage_hash or _build_configured_storage_hash(
        namespace=resolved_namespace,
        schema=resolved_schema,
        table_storage_identity=_table_storage_identity(table),
        hash_namespace=resolved_hash_namespace,
        extra_hash_components=resolved_extra_hash_components,
    )

    return MetaTableRegistrationRequest(
        data_source_uid=str(data_source_uid),
        management_mode="external_registered",
        storage_hash=resolved_storage_hash,
        identifier=resolved_identifier,
        namespace=resolved_namespace,
        description=resolved_description,
        protect_from_deletion=protect_from_deletion,
        labels=list(labels or []),
        introspect=introspect,
        table_contract=table_contract_from_sqlalchemy_model(
            model_or_table,
            schema=resolved_schema,
        ),
    )


def register_external_sqlalchemy_model(
    model_or_table: Any,
    *,
    data_source_uid: str,
    timeout: int | float | tuple[float, float] | None = None,
    **kwargs: Any,
) -> MetaTable:
    request = external_registered_registration_request_from_sqlalchemy_model(
        model_or_table,
        data_source_uid=data_source_uid,
        **kwargs,
    )
    return MetaTable.register(request, timeout=timeout)


def _resolve_table(model_or_table: Any) -> Any:
    table = _optional_sqlalchemy_table(model_or_table)
    if table is not None:
        return table
    return getattr(model_or_table, "__table__", model_or_table)


def _optional_sqlalchemy_table(model_or_table: Any) -> Any | None:
    inspected = _sqlalchemy_inspect(model_or_table, raiseerr=False)
    table = _table_from_sqlalchemy_inspection(inspected)
    if table is not None:
        return table
    table = getattr(model_or_table, "__table__", None)
    if table is not None and hasattr(table, "columns"):
        return table
    return None


def _table_from_sqlalchemy_inspection(inspected: Any | None) -> Any | None:
    if inspected is None:
        return None
    if isinstance(inspected, _SQLAlchemyTable):
        return inspected
    local_table = getattr(inspected, "local_table", None)
    if local_table is not None:
        return local_table
    persist_selectable = getattr(inspected, "persist_selectable", None)
    if isinstance(persist_selectable, _SQLAlchemyTable):
        return persist_selectable
    return None


def _table_name(table: Any) -> str:
    name = getattr(table, "name", None)
    if not name:
        raise ValueError("SQLAlchemy table metadata must expose a non-empty table name.")
    return str(name)


def _resolve_schema(table: Any, *, schema: str | None = None) -> str:
    resolved_schema = schema or getattr(table, "schema", None)
    if not resolved_schema:
        return DEFAULT_POSTGRES_SCHEMA
    return str(resolved_schema)


def _resolve_class_namespace(cls: type[Any]) -> str:
    resolved_namespace = getattr(cls, "__metatable_namespace__", None)
    if not resolved_namespace:
        raise ValueError("PlatformManagedMetaTable requires __metatable_namespace__.")
    return str(resolved_namespace)


def _resolve_class_schema(cls: type[Any], *, metadata: Any | None = None) -> str | None:
    table_args = getattr(cls, "__table_args__", None)
    if isinstance(table_args, Mapping):
        schema = table_args.get("schema")
    elif isinstance(table_args, tuple) and table_args and isinstance(table_args[-1], Mapping):
        schema = table_args[-1].get("schema")
    else:
        schema = None
    if schema is None and metadata is not None:
        schema = getattr(metadata, "schema", None)
    return _normalize_sqlalchemy_schema(schema)


def _normalize_sqlalchemy_schema(schema: Any | None) -> str | None:
    if schema is None:
        return None
    schema_name = str(schema).strip()
    if schema_name in ("", DEFAULT_POSTGRES_SCHEMA):
        return None
    return schema_name


def _normalize_table_default_schema(table: Any) -> None:
    if getattr(table, "schema", None) != DEFAULT_POSTGRES_SCHEMA:
        return

    metadata = getattr(table, "metadata", None)
    if metadata is not None:
        metadata._remove_table(table.name, table.schema)
    table.schema = None
    table.fullname = _table_name(table)
    if metadata is not None:
        metadata._add_table(table.name, table.schema, table)


def _resolve_data_source_uid(
    *,
    data_source: DynamicTableDataSource | None = None,
    data_source_uid: str | None = None,
) -> str:
    if data_source is not None and not isinstance(data_source, DynamicTableDataSource):
        raise TypeError(
            "data_source must be a DynamicTableDataSource. Pass data_source_uid=... "
            "when only the uid is available."
        )
    if data_source_uid:
        return str(data_source_uid)

    resolved_data_source = data_source
    if resolved_data_source is None:
        try:
            from mainsequence.client.metatables import get_session_data_source
        except ImportError as exc:  # pragma: no cover - defensive import guard.
            raise RuntimeError("Could not import the session data source resolver.") from exc
        resolved_data_source = get_session_data_source()

    uid = getattr(resolved_data_source, "uid", None)
    if not uid:
        raise ValueError(
            "Could not resolve a time-indexed data source uid. Run inside a configured "
            "Main Sequence project/session, or pass data_source=... / data_source_uid=... "
            "explicitly."
        )
    return str(uid)


def _resolve_model_data_source_uid(
    model_or_table: Any,
    *,
    data_source: DynamicTableDataSource | None = None,
    data_source_uid: str | None = None,
) -> str:
    resolved_data_source_uid = (
        data_source_uid
        or getattr(model_or_table, "__metatable_data_source_uid__", None)
        or getattr(model_or_table, "data_source_uid", None)
        or _table_info_value(model_or_table, "data_source_uid")
    )
    return _resolve_data_source_uid(
        data_source=data_source,
        data_source_uid=_coerce_optional_uid(resolved_data_source_uid),
    )


def _begin_local_registration(
    model: type[PlatformManagedMetaTable],
    *,
    storage_hash: str,
    stack: tuple[str, ...],
) -> MetaTable | None:
    state = _METATABLE_REGISTRATION_REGISTRY.get(storage_hash)
    if state is not None:
        if state.status == "registered" and state.meta_table is not None:
            model._bind_meta_table(state.meta_table)
            return state.meta_table
        if state.status == "in_progress":
            cycle_path = (*state.stack, _registration_stack_label(model, storage_hash))
            raise ValueError(
                "MetaTable recursive registration cycle detected: " + " -> ".join(cycle_path)
            )

    bound_meta_table = model.get_meta_table()
    if bound_meta_table is not None and model.get_meta_table_uid() is not None:
        _METATABLE_REGISTRATION_REGISTRY[storage_hash] = _MetaTableRegistrationState(
            status="registered",
            model=model,
            meta_table=bound_meta_table,
            stack=stack,
        )
        return bound_meta_table

    current_stack = (*stack, _registration_stack_label(model, storage_hash))
    _METATABLE_REGISTRATION_REGISTRY[storage_hash] = _MetaTableRegistrationState(
        status="in_progress",
        model=model,
        meta_table=None,
        stack=current_stack,
    )
    return None


def _complete_local_registration(
    *,
    storage_hash: str,
    model: type[PlatformManagedMetaTable],
    meta_table: MetaTable,
) -> None:
    state = _METATABLE_REGISTRATION_REGISTRY.get(storage_hash)
    stack = state.stack if state is not None else ()
    _METATABLE_REGISTRATION_REGISTRY[storage_hash] = _MetaTableRegistrationState(
        status="registered",
        model=model,
        meta_table=meta_table,
        stack=stack,
    )


def _clear_failed_local_registration(
    *,
    storage_hash: str,
    model: type[PlatformManagedMetaTable],
) -> None:
    state = _METATABLE_REGISTRATION_REGISTRY.get(storage_hash)
    if state is not None and state.status == "in_progress" and state.model is model:
        _METATABLE_REGISTRATION_REGISTRY.pop(storage_hash, None)


def _registration_stack_label(model: type[Any], storage_hash: str) -> str:
    model_name = getattr(model, "__qualname__", getattr(model, "__name__", repr(model)))
    return f"{model_name}({storage_hash})"


def _coerce_optional_uid(value: Any) -> str | None:
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, str) and value:
        return value
    return None


def _meta_table_uid(meta_table: MetaTable) -> str | None:
    uid = getattr(meta_table, "uid", None)
    return _coerce_optional_uid(uid)


def _required_meta_table_uid(meta_table: MetaTable) -> str:
    uid = _meta_table_uid(meta_table)
    if uid in (None, ""):
        raise ValueError("PlatformManagedMetaTable._bind_meta_table requires meta_table.uid.")
    return uid


def _meta_table_storage_hash(meta_table: MetaTable) -> str | None:
    storage_hash = getattr(meta_table, "storage_hash", None)
    if storage_hash in (None, ""):
        return None
    return str(storage_hash)


def _required_meta_table_storage_hash(meta_table: MetaTable) -> str:
    storage_hash = _meta_table_storage_hash(meta_table)
    if storage_hash in (None, ""):
        raise ValueError(
            "PlatformManagedMetaTable._bind_meta_table requires meta_table.storage_hash."
        )
    return storage_hash


def _required_meta_table_data_source_uid(meta_table: MetaTable) -> str:
    data_source_uid = _coerce_optional_uid(meta_table.data_source_uid)
    if data_source_uid in (None, ""):
        raise ValueError(
            "PlatformManagedMetaTable._bind_meta_table requires meta_table.data_source_uid."
        )
    return data_source_uid


def _meta_table_physical_table_name(meta_table: MetaTable) -> str | None:
    physical_table_name = getattr(meta_table, "physical_table_name", None)
    if physical_table_name in (None, ""):
        return None
    return str(physical_table_name)


def _bound_physical_table_name(cls: type[Any], meta_table: MetaTable) -> str | None:
    table = _optional_sqlalchemy_table(cls)
    if table is not None:
        return _table_name(table)
    return _meta_table_physical_table_name(meta_table)


def _model_bound_storage_hash(model_or_table: Any) -> str | None:
    storage_hash = getattr(model_or_table, "__metatable_storage_hash__", None)
    if storage_hash not in (None, ""):
        return str(storage_hash)
    return None


def resolve_metatable_identifier(
    model_or_table: Any,
    *,
    identifier: str | None = None,
) -> str | None:
    """Resolve the optional globally stable MetaTable identifier for a SQLAlchemy model."""

    resolved_identifier = (
        identifier
        or getattr(model_or_table, "__metatable_identifier__", None)
        or _table_info_value(model_or_table, "identifier")
    )
    if resolved_identifier in (None, ""):
        return None
    return str(resolved_identifier)


def _resolve_namespace(model_or_table: Any, *, namespace: str | None) -> str:
    resolved_namespace = (
        namespace
        or getattr(model_or_table, "__metatable_namespace__", None)
        or _table_info_value(model_or_table, "namespace")
    )
    if not resolved_namespace:
        raise ValueError(
            "MetaTable SQLAlchemy contracts require namespace or __metatable_namespace__."
        )
    return str(resolved_namespace)


def _resolve_description(model_or_table: Any, *, description: str | None) -> str | None:
    if description is not None:
        return description
    resolved_description = getattr(
        model_or_table,
        "__metatable_description__",
        None,
    ) or _table_info_value(model_or_table, "description")
    if resolved_description in (None, ""):
        return None
    return str(resolved_description)


def _resolve_labels(
    model_or_table: Any,
    *,
    labels: Sequence[str] | None,
) -> list[str]:
    resolved_labels = (
        labels
        if labels is not None
        else getattr(model_or_table, "__metatable_labels__", None)
        or _table_info_value(model_or_table, "labels")
    )
    if resolved_labels in (None, ""):
        return []
    if isinstance(resolved_labels, str):
        return [resolved_labels]
    return [str(label) for label in list(resolved_labels)]


def _resolve_provisioning(
    model_or_table: Any,
    *,
    provisioning: Mapping[str, Any] | None,
) -> dict[str, Any]:
    resolved_provisioning = (
        provisioning
        if provisioning is not None
        else getattr(model_or_table, "__metatable_provisioning__", None)
        or _table_info_value(model_or_table, "provisioning")
    )
    if resolved_provisioning is None:
        return dict(DEFAULT_PLATFORM_MANAGED_PROVISIONING)
    if not isinstance(resolved_provisioning, Mapping):
        raise ValueError("MetaTable provisioning metadata must be a mapping when provided.")
    return dict(resolved_provisioning)


def _resolve_bool_metadata(
    model_or_table: Any,
    *,
    value: bool | None,
    attr_name: str,
    info_key: str,
    default: bool,
) -> bool:
    if value is not None:
        return bool(value)
    resolved_value = getattr(model_or_table, attr_name, None)
    if resolved_value is None:
        resolved_value = _table_info_value(model_or_table, info_key)
    if resolved_value is None:
        return default
    return bool(resolved_value)


def _resolve_hash_namespace(model_or_table: Any, *, hash_namespace: str | None) -> str | None:
    if hash_namespace is not None:
        return hash_namespace
    resolved_hash_namespace = getattr(model_or_table, "__metatable_hash_namespace__", None)
    if resolved_hash_namespace in (None, ""):
        return None
    return str(resolved_hash_namespace)


def _resolve_extra_hash_components(
    model_or_table: Any,
    *,
    extra_hash_components: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    resolved: dict[str, Any] = {}
    class_components = getattr(model_or_table, "__metatable_extra_hash_components__", None)
    if isinstance(class_components, Mapping):
        resolved.update(dict(class_components))
    if isinstance(extra_hash_components, Mapping):
        resolved.update(dict(extra_hash_components))
    return resolved or None


def _resolve_model_path(
    model_or_table: Any,
    *,
    table_model_module: str | None,
    table_model_qualname: str | None,
) -> tuple[str, str]:
    if table_model_module and table_model_qualname:
        return table_model_module, table_model_qualname
    if hasattr(model_or_table, "__table__"):
        return (
            table_model_module or getattr(model_or_table, "__module__", "__main__"),
            table_model_qualname
            or getattr(
                model_or_table, "__qualname__", getattr(model_or_table, "__name__", "Table")
            ),
        )
    table = _resolve_table(model_or_table)
    return table_model_module or "sqlalchemy", table_model_qualname or _table_name(table)


def _table_info_value(model_or_table: Any, key: str) -> Any:
    try:
        table = _resolve_table(model_or_table)
    except TypeError:
        return None
    info = getattr(table, "info", None)
    if isinstance(info, Mapping):
        return info.get(key)
    return None


def _iter_columns(table: Any) -> list[Any]:
    columns = getattr(table, "columns", None)
    if columns is None:
        return []
    return list(columns)


def _column_contract(column: Any, *, ordinal_position: int) -> MetaTableColumnContract:
    type_contract = _column_type_contract(column)
    info = getattr(column, "info", None)
    info = info if isinstance(info, Mapping) else {}
    server_default = (
        SERVER_GENERATED_UUID_DEFAULT
        if _is_server_generated_uuid_primary_key(
            column,
            data_type=type_contract["data_type"],
        )
        else None
    )
    return MetaTableColumnContract(
        name=str(column.name),
        data_type=type_contract["data_type"],
        backend_type=type_contract.get("backend_type"),
        nullable=bool(getattr(column, "nullable", True)),
        primary_key=bool(getattr(column, "primary_key", False)),
        unique=bool(getattr(column, "unique", False)),
        server_default=server_default,
        description=info.get("description"),
        label=info.get("label"),
        logical_name=info.get("logical_name"),
    )


def _column_type_contract(column: Any) -> dict[str, str]:
    column_type = getattr(column, "type", None)
    if column_type is None:
        raise ValueError(f"Column {column.name!r} does not expose a SQLAlchemy type.")

    data_type = sqlalchemy_type_to_token(column_type, remote=True)
    backend_type = sqlalchemy_backend_type(column_type, data_type)
    return {
        "data_type": data_type,
        "backend_type": backend_type or data_type,
    }


def _is_server_generated_uuid_primary_key(column: Any, *, data_type: str) -> bool:
    return bool(getattr(column, "primary_key", False)) and data_type == "uuid"


def _build_configured_storage_hash(
    *,
    namespace: str,
    schema: str,
    table_storage_identity: Mapping[str, Any],
    hash_namespace: str | None = None,
    extra_hash_components: Mapping[str, Any] | None = None,
) -> str:
    return build_meta_table_configured_storage_hash(
        namespace=namespace,
        schema=schema,
        table_storage_identity=table_storage_identity,
        hash_namespace=hash_namespace,
        extra_hash_components=extra_hash_components,
    )


def _table_storage_identity(table: Any) -> dict[str, Any]:
    return _storage_identity_from_parts(
        columns=_iter_columns(table),
        indexes=_iter_indexes(table),
        foreign_keys=_iter_foreign_key_constraints(table),
    )


def _configured_table_storage_identity(model_or_table: Any, *, table: Any) -> dict[str, Any]:
    if not _has_time_index_contract(model_or_table):
        return _table_storage_identity(table)

    time_index_name = _resolve_time_index_name(model_or_table)
    index_names = _resolve_time_index_names(
        model_or_table,
        time_index_name=time_index_name,
    )
    _validate_time_index_contract(
        columns=_iter_columns(table),
        time_index_name=time_index_name,
        index_names=index_names,
    )
    return _time_index_table_storage_identity(
        table,
        time_index_name=time_index_name,
        index_names=index_names,
        storage_layout=_resolve_time_index_storage_layout(model_or_table),
    )


def _table_items_storage_identity(table_items: Sequence[Any]) -> dict[str, Any]:
    columns = [item for item in table_items if _looks_like_column(item)]
    return _storage_identity_from_parts(columns=columns)


def _time_index_table_storage_identity(
    table: Any,
    *,
    time_index_name: str,
    index_names: Sequence[str],
    storage_layout: Mapping[str, Any] | None,
) -> dict[str, Any]:
    return _time_index_storage_identity(
        table_storage_identity=_table_storage_identity(table),
        time_index_name=time_index_name,
        index_names=index_names,
        storage_layout=storage_layout,
    )


def _time_index_table_items_storage_identity(
    table_items: Sequence[Any],
    *,
    time_index_name: str,
    index_names: Sequence[str],
    storage_layout: Mapping[str, Any] | None,
) -> dict[str, Any]:
    return _time_index_storage_identity(
        table_storage_identity=_table_items_storage_identity(table_items),
        time_index_name=time_index_name,
        index_names=index_names,
        storage_layout=storage_layout,
    )


def _time_index_storage_identity(
    *,
    table_storage_identity: Mapping[str, Any],
    time_index_name: str,
    index_names: Sequence[str],
    storage_layout: Mapping[str, Any] | None,
) -> dict[str, Any]:
    profile: dict[str, Any] = {
        "kind": "time_indexed",
        "time_index_name": str(time_index_name),
        "index_names": [str(name) for name in index_names],
    }
    if storage_layout:
        profile["storage_layout"] = dict(storage_layout)
    return {
        "relational_table": dict(table_storage_identity),
        "time_indexed_profile": profile,
    }


def _has_time_index_contract(model_or_table: Any) -> bool:
    if getattr(model_or_table, "__time_index_name__", None) is not None:
        return True
    if getattr(model_or_table, "__dynamic_table_time_index_name__", None) is not None:
        return True
    if getattr(model_or_table, "__index_names__", None) is not None:
        return True
    if getattr(model_or_table, "__dynamic_table_index_names__", None) is not None:
        return True

    time_index_cls = globals().get("PlatformTimeIndexMetaTable")
    if isinstance(time_index_cls, type) and isinstance(model_or_table, type):
        try:
            if issubclass(model_or_table, time_index_cls):
                return True
        except TypeError:
            pass

    table = _resolve_table(model_or_table)
    info = getattr(table, "info", None)
    return isinstance(info, Mapping) and (
        info.get("time_index_name") is not None
        or info.get("index_names") is not None
        or info.get("dynamic_table") is not None
    )


def _resolve_time_index_name(
    model_or_table: Any,
    *,
    time_index_name: str | None = None,
) -> str:
    resolved = (
        time_index_name
        or getattr(model_or_table, "__time_index_name__", None)
        or getattr(model_or_table, "__dynamic_table_time_index_name__", None)
        or _table_info_value(model_or_table, "time_index_name")
        or _dynamic_table_info_value(model_or_table, "time_index_name")
        or "time_index"
    )
    resolved = str(resolved)
    if not resolved:
        raise ValueError("PlatformTimeIndexMetaTable requires a non-empty time_index_name.")
    return resolved


def _resolve_time_index_names(
    model_or_table: Any,
    *,
    time_index_name: str,
    index_names: Sequence[str] | None = None,
) -> list[str]:
    resolved = (
        index_names
        or getattr(model_or_table, "__index_names__", None)
        or getattr(model_or_table, "__dynamic_table_index_names__", None)
        or _table_info_value(model_or_table, "index_names")
        or _dynamic_table_info_value(model_or_table, "index_names")
    )
    if resolved is None:
        identity_dimensions = (
            getattr(model_or_table, "__identity_dimensions__", None)
            or getattr(model_or_table, "__dynamic_table_identity_dimensions__", None)
            or _table_info_value(model_or_table, "identity_dimensions")
            or _dynamic_table_info_value(model_or_table, "identity_dimensions")
        )
        if identity_dimensions is None:
            return [time_index_name]
        resolved = [time_index_name, *list(identity_dimensions)]

    names = [str(name) for name in list(resolved or [])]
    if not names:
        raise ValueError(
            "PlatformTimeIndexMetaTable requires at least the time index in index_names."
        )
    return names


def _resolve_time_index_storage_layout(
    model_or_table: Any,
    *,
    storage_layout: Mapping[str, Any] | None = None,
) -> Mapping[str, Any] | None:
    resolved = (
        storage_layout
        or getattr(model_or_table, "__storage_layout__", None)
        or getattr(model_or_table, "__dynamic_table_storage_layout__", None)
        or _table_info_value(model_or_table, "storage_layout")
        or _dynamic_table_info_value(model_or_table, "storage_layout")
    )
    if resolved is None:
        return None
    if not isinstance(resolved, Mapping):
        raise ValueError(
            "PlatformTimeIndexMetaTable storage_layout must be a mapping when provided."
        )
    return resolved


def _dynamic_table_info_value(model_or_table: Any, key: str) -> Any:
    dynamic_table = _table_info_value(model_or_table, "dynamic_table")
    if isinstance(dynamic_table, Mapping):
        return dynamic_table.get(key)
    return None


def _validate_time_index_contract(
    *,
    columns: Sequence[Any],
    time_index_name: str,
    index_names: Sequence[str],
) -> None:
    names = [str(name) for name in index_names]
    if names[0] != time_index_name:
        raise ValueError(
            "PlatformTimeIndexMetaTable index_names must start with the time_index_name. "
            f"Expected {time_index_name!r}, got {names[0]!r}."
        )
    if len(set(names)) != len(names):
        raise ValueError("PlatformTimeIndexMetaTable index_names cannot contain duplicates.")

    columns_by_name = {str(column.name): column for column in columns}
    missing = [name for name in names if name not in columns_by_name]
    if missing:
        raise ValueError(
            "PlatformTimeIndexMetaTable index_names must all exist as table columns. "
            f"Missing columns: {missing!r}."
        )

    time_column = columns_by_name[time_index_name]
    time_type = _column_type_contract(time_column)["data_type"]
    if not is_temporal_token(time_type):
        raise ValueError(
            "PlatformTimeIndexMetaTable time_index_name must reference a temporal column. "
            f"Column {time_index_name!r} has type {time_type!r}."
        )

    nullable_index_columns = [
        name for name in names if bool(getattr(columns_by_name[name], "nullable", True))
    ]
    if nullable_index_columns:
        raise ValueError(
            "PlatformTimeIndexMetaTable index columns must be non-nullable. "
            f"Nullable index columns: {nullable_index_columns!r}."
        )


def _ensure_time_index_unique_grain_index(
    *,
    table: Any,
    index_names: Sequence[str],
) -> None:
    grain_names = [str(name) for name in index_names]
    if _has_unique_grain_enforcement(table=table, index_names=grain_names):
        return

    from sqlalchemy import Index

    Index(
        schema_index_name(_table_name(table), grain_names, unique=True),
        *(table.c[name] for name in grain_names),
        unique=True,
    )


def _has_unique_grain_enforcement(
    *,
    table: Any,
    index_names: Sequence[str],
) -> bool:
    for index in _iter_indexes(table):
        if bool(getattr(index, "unique", False)) and _grain_columns_match(
            _index_column_names(index),
            index_names,
        ):
            return True

    for constraint in _iter_unique_constraints(table):
        if _grain_columns_match(_constraint_column_names(constraint), index_names):
            return True

    return False


def _grain_columns_match(existing_names: Sequence[str], index_names: Sequence[str]) -> bool:
    existing = [str(name) for name in existing_names]
    expected = [str(name) for name in index_names]
    if len(existing) != len(expected):
        return False
    return set(existing) == set(expected)


def _index_column_names(index: Any) -> list[str]:
    return [_storage_item_name(item) for item in _iter_index_items(index)]


def _iter_unique_constraints(table: Any) -> list[Any]:
    from sqlalchemy import UniqueConstraint

    constraints = getattr(table, "constraints", None)
    if constraints is None:
        return []
    return [
        constraint
        for constraint in constraints
        if isinstance(constraint, UniqueConstraint)
    ]


def _constraint_column_names(constraint: Any) -> list[str]:
    columns = getattr(constraint, "columns", None)
    if columns is None:
        return []
    return [_storage_item_name(column) for column in columns]


def _storage_identity_from_parts(
    *,
    columns: Sequence[Any],
    indexes: Sequence[Any] | None = None,
    foreign_keys: Sequence[Any] | None = None,
) -> dict[str, Any]:
    identity = {
        "columns": [
            _column_storage_identity(column, ordinal_position=position)
            for position, column in enumerate(columns)
        ],
    }
    index_identity = [_index_storage_identity(index) for index in indexes or []]
    if index_identity:
        identity["indexes"] = sorted(index_identity, key=_storage_identity_sort_key)

    foreign_key_identity = [
        _foreign_key_storage_identity(foreign_key) for foreign_key in foreign_keys or []
    ]
    if foreign_key_identity:
        identity["foreign_keys"] = sorted(
            foreign_key_identity,
            key=_storage_identity_sort_key,
        )
    return identity


def _column_storage_identity(column: Any, *, ordinal_position: int) -> dict[str, Any]:
    type_contract = _column_type_contract(column)
    identity = {
        "ordinal_position": ordinal_position,
        "name": str(column.name),
        "data_type": type_contract["data_type"],
        "backend_type": type_contract.get("backend_type"),
        "nullable": bool(getattr(column, "nullable", True)),
        "primary_key": bool(getattr(column, "primary_key", False)),
        "unique": bool(getattr(column, "unique", False)),
    }
    if _is_server_generated_uuid_primary_key(
        column,
        data_type=type_contract["data_type"],
    ):
        identity["server_default"] = SERVER_GENERATED_UUID_DEFAULT
    return identity


def _iter_indexes(table: Any) -> list[Any]:
    indexes = getattr(table, "indexes", None)
    if indexes is None:
        return []
    return list(indexes)


def _iter_foreign_key_constraints(table: Any) -> list[Any]:
    foreign_keys = getattr(table, "foreign_key_constraints", None)
    if foreign_keys is None:
        return []
    return list(foreign_keys)


def _index_storage_identity(index: Any) -> dict[str, Any]:
    identity = {
        "columns": [_storage_item_name(item) for item in _iter_index_items(index)],
        "unique": bool(getattr(index, "unique", False)),
    }
    dialect_kwargs = _index_dialect_kwargs(index)
    if dialect_kwargs:
        identity["dialect_kwargs"] = dialect_kwargs
    return identity


def _iter_index_items(index: Any) -> list[Any]:
    expressions = getattr(index, "expressions", None)
    if expressions is not None:
        return list(expressions)
    columns = getattr(index, "columns", None)
    if columns is None:
        return []
    return list(columns)


def _index_dialect_kwargs(index: Any) -> dict[str, Any]:
    dialect_kwargs = getattr(index, "dialect_kwargs", None)
    if isinstance(dialect_kwargs, Mapping):
        return {
            str(key): _storage_value(value)
            for key, value in sorted(dialect_kwargs.items())
            if _storage_value_should_be_included(value)
        }

    dialect_options = getattr(index, "dialect_options", None)
    if not isinstance(dialect_options, Mapping):
        return {}

    resolved: dict[str, Any] = {}
    for dialect, values in sorted(dialect_options.items()):
        if not isinstance(values, Mapping):
            continue
        for key, value in sorted(values.items()):
            if _storage_value_should_be_included(value):
                resolved[f"{dialect}_{key}"] = _storage_value(value)
    return resolved


def _foreign_key_storage_identity(foreign_key: Any) -> dict[str, Any]:
    elements = [
        _foreign_key_element_storage_identity(element)
        for element in _iter_foreign_key_elements(foreign_key)
    ]
    identity: dict[str, Any] = {"elements": elements}
    for attr_name in ("ondelete", "onupdate", "deferrable", "initially", "match"):
        attr_value = getattr(foreign_key, attr_name, None)
        if attr_value not in (None, ""):
            identity[attr_name] = _storage_value(attr_value)
    return identity


def _iter_foreign_key_elements(foreign_key: Any) -> list[Any]:
    elements = getattr(foreign_key, "elements", None)
    if elements is None:
        return []
    return list(elements)


def _foreign_key_element_storage_identity(element: Any) -> dict[str, Any]:
    source_column = getattr(element, "parent", None)
    target_column = _foreign_key_target_column(element)
    target_schema, target_table, target_column_name = _foreign_key_target_identity(
        element,
        target_column=target_column,
    )
    identity = {
        "source_column": _storage_item_name(source_column),
        "target_schema": target_schema,
        "target_table": target_table,
        "target_column": target_column_name,
    }
    for attr_name in ("ondelete", "onupdate", "deferrable", "initially", "match"):
        attr_value = getattr(element, attr_name, None)
        if attr_value not in (None, ""):
            identity[attr_name] = _storage_value(attr_value)
    return identity


def _foreign_key_target_column(element: Any) -> Any | None:
    try:
        return getattr(element, "column", None)
    except Exception:
        return None


def _foreign_key_target_identity(
    element: Any,
    *,
    target_column: Any | None,
) -> tuple[str | None, str | None, str]:
    if target_column is not None:
        return (
            _storage_table_schema(target_column),
            _storage_table_name(target_column),
            _storage_item_name(target_column),
        )

    colspec = getattr(element, "_colspec", None)
    if colspec in (None, ""):
        return None, None, ""

    parts = str(colspec).split(".")
    if len(parts) >= 2:
        target_column_name = parts[-1]
        target_table = parts[-2]
        target_schema = ".".join(parts[:-2]) or None
        return target_schema, target_table, target_column_name
    return None, None, str(colspec)


def _storage_table_schema(column: Any) -> str | None:
    table = getattr(column, "table", None)
    schema = getattr(table, "schema", None)
    if schema in (None, ""):
        return None
    return str(schema)


def _storage_table_name(column: Any) -> str | None:
    table = getattr(column, "table", None)
    table_name = getattr(table, "name", None)
    if table_name in (None, ""):
        return None
    return str(table_name)


def _storage_item_name(item: Any) -> str:
    name = getattr(item, "name", None)
    if name not in (None, ""):
        return str(name)
    return str(item)


def _storage_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _storage_value(item)
            for key, item in sorted(value.items())
            if _storage_value_should_be_included(item)
        }
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_storage_value(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _storage_value_should_be_included(value: Any) -> bool:
    return value not in (None, "", {}, [], ())


def _storage_identity_sort_key(value: Mapping[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, default=str)


def _looks_like_column(value: Any) -> bool:
    return getattr(value, "name", None) not in (None, "") and hasattr(value, "type")


__all__ = [
    "DEFAULT_PLATFORM_MANAGED_PROVISIONING",
    "PlatformManagedMetaTable",
    "PlatformTimeIndexMetaTable",
    "external_registered_registration_request_from_sqlalchemy_model",
    "platform_managed_migration_registration_context",
    "platform_managed_registration_request_from_sqlalchemy_model",
    "register_external_sqlalchemy_model",
    "resolve_metatable_identifier",
    "table_contract_from_sqlalchemy_model",
    "time_indexed_registration_request_from_sqlalchemy_model",
]
