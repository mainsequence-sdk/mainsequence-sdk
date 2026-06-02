from __future__ import annotations

import pathlib
import sys
from collections.abc import Mapping, MutableMapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar
from uuid import UUID

from sqlalchemy import ForeignKey
from sqlalchemy.orm import declared_attr as _sqlalchemy_declared_attr

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
    MetaTableForeignKeyContract,
    MetaTableIndexContract,
    MetaTablePhysicalContract,
    MetaTableRegistrationRequest,
)

from .hashing import build_meta_table_configured_storage_hash, build_meta_table_storage_hash

if TYPE_CHECKING:
    from mainsequence.client.metatables import TimeIndexMetaData

DEFAULT_PLATFORM_MANAGED_PROVISIONING = {
    "create_table": True,
    "if_not_exists": True,
}
SERVER_GENERATED_UUID_DEFAULT = "gen_random_uuid()"
POSTGRESQL_MAX_IDENTIFIER_LENGTH = 63
_BOUND_PHYSICAL_TO_LOGICAL_FULLNAMES: dict[str, str] = {}
_METATABLE_FOREIGN_KEY_INFO_KEY = "mainsequence_metatable_foreign_key"


@dataclass
class _MetaTableRegistrationState:
    status: str
    model: type[Any]
    meta_table: Any | None = None
    stack: tuple[str, ...] = ()


_METATABLE_REGISTRATION_REGISTRY: dict[str, _MetaTableRegistrationState] = {}


def _truncate_postgresql_identifier(value: str) -> str:
    return str(value)[:POSTGRESQL_MAX_IDENTIFIER_LENGTH]


def _default_time_indexed_meta_table_indexes(index_names: Sequence[str]) -> list[dict[str, Any]]:
    normalized_index_names = [str(name) for name in index_names or []]
    if not normalized_index_names:
        return []

    time_index_name, *identity_dimensions = normalized_index_names
    indexes: list[dict[str, Any]] = [
        {
            "name": "time_idx",
            "columns": [time_index_name],
            "unique": False,
            "method": "brin",
            "expression": None,
        }
    ]
    if identity_dimensions:
        indexes.append(
            {
                "name": _truncate_postgresql_identifier(f"{'_'.join(identity_dimensions)}_idx"),
                "columns": identity_dimensions,
                "unique": False,
                "method": "btree",
                "expression": None,
            }
        )
    indexes.append(
        {
            "name": "time_identifier_ev_idx",
            "columns": normalized_index_names,
            "unique": True,
            "method": "btree",
            "expression": None,
        }
    )
    return indexes


def metatable_tablename(
    *,
    namespace: str,
    identifier: str,
    schema: str = "public",
    hash_namespace: str | None = None,
    extra_hash_components: Mapping[str, Any] | None = None,
) -> str:
    return build_meta_table_storage_hash(
        namespace=namespace,
        identifier=identifier,
        schema=schema,
        hash_namespace=hash_namespace,
        extra_hash_components=extra_hash_components,
    )


class MetaTableForeignKey:
    """Factory for SQLAlchemy foreign keys that target MetaTable model classes."""

    def __new__(
        cls,
        target_model: type[PlatformManagedMetaTable],
        *,
        column: str = "uid",
        name: str | None = None,
        onupdate: str | None = None,
        ondelete: str | None = None,
        deferrable: bool | None = None,
        initially: str | None = None,
        link_to_name: bool = False,
        use_alter: bool = False,
        match: str | None = None,
        info: Mapping[str, Any] | None = None,
        comment: str | None = None,
        **dialect_kw: Any,
    ) -> Any:
        if name is not None:
            raise ValueError(
                "MetaTableForeignKey does not accept foreign-key names for "
                "platform-managed MetaTables. The backend generates physical "
                "constraint names."
            )
        target_column = _resolve_metatable_foreign_key_target_column(
            target_model,
            column=column,
        )
        foreign_key_info = dict(info or {})
        foreign_key_info[_METATABLE_FOREIGN_KEY_INFO_KEY] = {
            "target_model": target_model,
            "target_column": str(column),
        }
        return ForeignKey(
            target_column,
            name=name,
            onupdate=onupdate,
            ondelete=ondelete,
            deferrable=deferrable,
            initially=initially,
            link_to_name=link_to_name,
            use_alter=use_alter,
            match=match,
            info=foreign_key_info,
            comment=comment,
            **dialect_kw,
        )


def _resolve_metatable_foreign_key_target_column(
    target_model: type[PlatformManagedMetaTable],
    *,
    column: str,
) -> Any:
    model_name = getattr(target_model, "__qualname__", repr(target_model))
    if not _is_platform_managed_meta_table_model(target_model):
        raise ValueError(
            "MetaTableForeignKey target must be a PlatformManagedMetaTable authoring "
            f"model class; got {model_name}."
        )

    target_table = getattr(target_model, "__table__", None)
    if target_table is None:
        raise ValueError(
            "MetaTableForeignKey target must be a SQLAlchemy MetaTable model "
            f"class with __table__; got {model_name}."
        )

    columns = getattr(target_table, "c", None)
    if columns is None:
        raise ValueError(f"MetaTableForeignKey target {model_name} does not expose table columns.")

    column_name = str(column)
    try:
        return columns[column_name]
    except (KeyError, TypeError, IndexError):
        raise ValueError(
            f"MetaTableForeignKey target {model_name} has no column {column_name!r}."
        ) from None


def _is_platform_managed_meta_table_model(value: Any) -> bool:
    return isinstance(value, type) and issubclass(value, PlatformManagedMetaTable)


def metatable_configured_tablename(
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


def _metatable_declared_tablename(cls: type[Any]) -> str:
    return f"metatable_{cls.__name__.lower()}"


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
    request, and registers the MetaTable through TS Manager. Registration is the
    only public path that binds the SQLAlchemy table to the backend-owned
    physical table name returned by TS Manager.
    """

    __metatable_use_configured_table_name__ = True
    __metatable__: ClassVar[MetaTable | None] = None
    __metatable_uid__: ClassVar[str | None] = None
    __metatable_data_source_uid__: ClassVar[str | None] = None
    __metatable_storage_hash__: ClassVar[str | None] = None
    __metatable_physical_table_name__: ClassVar[str | None] = None
    __metatable_description__: ClassVar[str | None] = None
    __metatable_labels__: ClassVar[Sequence[str] | None] = None
    __metatable_provisioning__: ClassVar[Mapping[str, Any] | None] = None
    __metatable_protect_from_deletion__: ClassVar[bool | None] = None
    __metatable_open_for_everyone__: ClassVar[bool | None] = None
    __metatable_introspect__: ClassVar[bool | None] = None
    __metatable_hash_namespace__: ClassVar[str | None] = None
    __metatable_extra_hash_components__: ClassVar[Mapping[str, Any] | None] = None

    __tablename__ = _sqlalchemy_declared_attr.directive(_metatable_declared_tablename)

    @classmethod
    def __table_cls__(cls, *args: Any, **kwargs: Any) -> Any:
        if len(args) < 2:
            raise TypeError("SQLAlchemy __table_cls__ expected name, metadata, and columns.")

        _name, metadata, *table_items = args
        kwargs = dict(kwargs)
        schema = str(kwargs.get("schema") or _resolve_class_schema(cls, metadata=metadata))
        if not kwargs.get("schema"):
            kwargs["schema"] = schema
        table_name = _build_configured_storage_hash(
            namespace=_resolve_class_namespace(cls),
            schema=schema,
            table_storage_identity=_table_items_storage_identity(table_items),
            hash_namespace=getattr(cls, "__metatable_hash_namespace__", None),
            extra_hash_components=getattr(cls, "__metatable_extra_hash_components__", None),
        )

        from sqlalchemy import Table

        return Table(table_name, metadata, *table_items, **kwargs)

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
        open_for_everyone: bool | None = None,
        provisioning: Mapping[str, Any] | None = None,
        introspect: bool | None = None,
        _target_meta_tables: Mapping[Any, Any] | None = None,
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
            open_for_everyone=open_for_everyone,
            provisioning=provisioning,
            introspect=introspect,
            _target_meta_tables=_target_meta_tables,
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
        storage_hash = cls.get_storage_hash()
        registry_meta_table = _begin_local_registration(
            cls,
            storage_hash=storage_hash,
            stack=_registration_stack,
        )
        if registry_meta_table is not None:
            return registry_meta_table

        try:
            target_meta_tables = _register_metatable_foreign_key_targets(
                cls,
                data_source=data_source,
                data_source_uid=data_source_uid,
                timeout=timeout,
                stack=_METATABLE_REGISTRATION_REGISTRY[storage_hash].stack,
            )
            request = cls.build_registration_request(
                data_source=data_source,
                data_source_uid=data_source_uid,
                _target_meta_tables=target_meta_tables,
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
    def resolve_foreign_key_targets(
        cls,
        *,
        target_meta_tables: Mapping[Any, Any] | None = None,
    ) -> dict[type[Any], str]:
        return _resolve_target_meta_table_uid_by_model(
            target_meta_tables=target_meta_tables,
            model_or_table=cls,
        )

    @classmethod
    def _bind_meta_table(cls, meta_table: Any) -> Any:
        """Attach an already-created backend MetaTable resource to this authoring model."""
        meta_table_uid = _meta_table_uid(meta_table)
        if meta_table_uid in (None, ""):
            raise ValueError("PlatformManagedMetaTable._bind_meta_table requires meta_table.uid.")

        cls.__metatable__ = meta_table
        cls.__metatable_uid__ = str(meta_table_uid)

        storage_hash = _meta_table_storage_hash(meta_table)
        if storage_hash in (None, ""):
            try:
                storage_hash = _table_name(_resolve_table(cls))
            except TypeError:
                storage_hash = None
        if storage_hash not in (None, ""):
            cls.__metatable_storage_hash__ = str(storage_hash)

        physical_table_name = _meta_table_physical_table_name(meta_table)
        if physical_table_name not in (None, ""):
            cls.__metatable_physical_table_name__ = str(physical_table_name)
            try:
                table = _resolve_table(cls)
            except TypeError:
                table = None
            if table is not None:
                _bind_table_physical_name(
                    table,
                    physical_table_name=str(physical_table_name),
                    storage_hash=str(storage_hash) if storage_hash not in (None, "") else None,
                )

        data_source_uid = _meta_table_data_source_uid(meta_table)
        if data_source_uid not in (None, ""):
            cls.__metatable_data_source_uid__ = str(data_source_uid)
        return meta_table

    @classmethod
    def get_meta_table(cls) -> Any | None:
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
        return _table_name(_resolve_table(cls))

    @classmethod
    def get_physical_table_name(cls) -> str | None:
        physical_table_name = getattr(cls, "__metatable_physical_table_name__", None)
        if physical_table_name not in (None, ""):
            return str(physical_table_name)
        return None


class PlatformTimeIndexMetaData(PlatformManagedMetaTable):
    """SQLAlchemy declarative base mixin for platform-managed TimeIndexMetaData.

    This is the SDK authoring surface for time-indexed DataNode storage. It
    reuses the MetaTable column/type/FK projection, but registers through the
    time-indexed MetaTable endpoint and validates the opinionated table shape:
    the first index must be the time index, and any extra index dimensions are
    ordinary non-null table columns.
    """

    __time_index_metadata__: ClassVar[TimeIndexMetaData | None] = None

    __mapper_args__ = _sqlalchemy_declared_attr.directive(_time_index_mapper_args)

    @classmethod
    def _bind_meta_table(cls, meta_table: TimeIndexMetaData) -> TimeIndexMetaData:
        bound = super()._bind_meta_table(meta_table)
        cls.__time_index_metadata__ = bound
        return bound

    @classmethod
    def get_time_index_metadata(cls) -> TimeIndexMetaData | None:
        return getattr(cls, "__time_index_metadata__", None)

    @classmethod
    def __table_cls__(cls, *args: Any, **kwargs: Any) -> Any:
        if len(args) < 2:
            raise TypeError("SQLAlchemy __table_cls__ expected name, metadata, and columns.")

        _name, metadata, *table_items = args
        kwargs = dict(kwargs)
        schema = str(kwargs.get("schema") or _resolve_class_schema(cls, metadata=metadata))
        if not kwargs.get("schema"):
            kwargs["schema"] = schema

        columns = [item for item in table_items if _looks_like_column(item)]
        time_index_name = _resolve_time_index_name(cls)
        index_names = _resolve_time_index_names(cls, time_index_name=time_index_name)
        _validate_time_index_contract(
            columns=columns,
            time_index_name=time_index_name,
            index_names=index_names,
        )

        table_name = _build_configured_storage_hash(
            namespace=_resolve_class_namespace(cls),
            schema=schema,
            table_storage_identity=_time_index_table_items_storage_identity(
                table_items,
                time_index_name=time_index_name,
                index_names=index_names,
                storage_layout=_resolve_time_index_storage_layout(cls),
            ),
            hash_namespace=getattr(cls, "__metatable_hash_namespace__", None),
            extra_hash_components=getattr(cls, "__metatable_extra_hash_components__", None),
        )

        from sqlalchemy import Table

        return Table(table_name, metadata, *table_items, **kwargs)

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
        _target_meta_tables: Mapping[Any, Any] | None = None,
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
            _target_meta_tables=_target_meta_tables,
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
        from mainsequence.client.metatables import TimeIndexMetaData

        storage_hash = cls.get_storage_hash()
        registry_meta_table = _begin_local_registration(
            cls,
            storage_hash=storage_hash,
            stack=_registration_stack,
        )
        if registry_meta_table is not None:
            return registry_meta_table

        try:
            target_meta_tables = _register_metatable_foreign_key_targets(
                cls,
                data_source=data_source,
                data_source_uid=data_source_uid,
                timeout=timeout,
                stack=_METATABLE_REGISTRATION_REGISTRY[storage_hash].stack,
            )
            request = cls.build_registration_request(
                data_source=data_source,
                data_source_uid=data_source_uid,
                _target_meta_tables=target_meta_tables,
            )
            time_index_metadata = TimeIndexMetaData.register(request, timeout=timeout)
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
    target_meta_tables: Mapping[Any, Any] | None = None,
    target_meta_table_uid_by_fullname: Mapping[str, Any] | None = None,
    schema: str | None = None,
    include_physical_table_name: bool = True,
    require_metatable_foreign_keys: bool = False,
) -> MetaTableContract:
    table = _resolve_table(model_or_table)
    _resolve_schema(table, schema=schema)
    resolved_targets = _resolve_target_meta_table_uid_by_fullname(
        target_meta_tables=target_meta_tables,
        target_meta_table_uid_by_fullname=target_meta_table_uid_by_fullname,
    )
    resolved_target_models = _resolve_target_meta_table_uid_by_model(
        target_meta_tables=target_meta_tables,
        model_or_table=model_or_table,
    )
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
            table_name=_table_name(table) if include_physical_table_name else None,
        ),
        columns=[
            _column_contract(column, ordinal_position=position)
            for position, column in enumerate(_iter_columns(table))
        ],
        constraints=[],
        indexes=[
            _index_contract(index)
            for index in sorted(_iter_indexes(table), key=lambda item: item.name or "")
        ],
        foreign_keys=[
            _foreign_key_contract(
                foreign_key_constraint,
                target_meta_table_uid_by_model=resolved_target_models,
                target_meta_table_uid_by_fullname=resolved_targets,
                require_metatable_foreign_keys=require_metatable_foreign_keys,
            )
            for foreign_key_constraint in sorted(
                _iter_foreign_key_constraints(table),
                key=lambda item: item.name or "",
            )
        ],
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
    _target_meta_tables: Mapping[Any, Any] | None = None,
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
    resolved_identifier = _resolve_identifier(model_or_table, identifier=identifier)
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
    resolved_target_models = _resolve_target_meta_table_uid_by_model(
        target_meta_tables=_target_meta_tables,
        model_or_table=model_or_table,
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
    bound_storage_hash = _model_bound_storage_hash(model_or_table)
    table_name = bound_storage_hash or _table_name(table)
    if (
        enforce_storage_hash_name
        and bound_storage_hash is None
        and table_name != configured_storage_hash
    ):
        raise ValueError(
            "Platform-managed time-indexed SQLAlchemy tables must use the configured "
            "time-indexed MetaTable storage hash as their initial SQLAlchemy table "
            "name. Use PlatformTimeIndexMetaData or metatable_configured_tablename(...) "
            "for __tablename__; registration will rebind it to the backend physical name. "
            f"Expected {configured_storage_hash!r}, got {table_name!r}."
        )

    column_contracts = [
        _column_contract(column, ordinal_position=position).model_dump(
            mode="json",
            exclude_none=True,
        )
        for position, column in enumerate(columns)
    ]
    foreign_key_contracts = [
        _time_indexed_meta_table_foreign_key_contract(
            foreign_key_constraint,
            target_meta_table_uid_by_model=resolved_target_models,
            target_meta_table_uid_by_fullname={},
            require_metatable_foreign_keys=True,
        ).model_dump(mode="json", exclude_none=True)
        for foreign_key_constraint in sorted(
            _iter_foreign_key_constraints(table),
            key=lambda item: item.name or "",
        )
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
        storage_hash=table_name,
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
            "physical": {},
            "columns": column_contracts,
            "indexes": _default_time_indexed_meta_table_indexes(resolved_index_names),
            "foreign_keys": foreign_key_contracts,
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
    open_for_everyone: bool | None = None,
    provisioning: Mapping[str, Any] | None = None,
    introspect: bool | None = None,
    _target_meta_tables: Mapping[Any, Any] | None = None,
    schema: str | None = None,
    hash_namespace: str | None = None,
    extra_hash_components: Mapping[str, Any] | None = None,
    enforce_storage_hash_name: bool = True,
) -> MetaTableRegistrationRequest:
    table = _resolve_table(model_or_table)
    resolved_schema = _resolve_schema(table, schema=schema)
    resolved_identifier = _resolve_identifier(model_or_table, identifier=identifier)
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
    expected_storage_hash = build_meta_table_storage_hash(
        namespace=resolved_namespace,
        identifier=resolved_identifier,
        schema=resolved_schema,
        hash_namespace=resolved_hash_namespace,
        extra_hash_components=resolved_extra_hash_components,
    )
    configured_storage_hash = _build_configured_storage_hash(
        namespace=resolved_namespace,
        schema=resolved_schema,
        table_storage_identity=_table_storage_identity(table),
        hash_namespace=resolved_hash_namespace,
        extra_hash_components=resolved_extra_hash_components,
    )
    bound_storage_hash = _model_bound_storage_hash(model_or_table)
    table_name = bound_storage_hash or _table_name(table)
    if (
        enforce_storage_hash_name
        and table_name
        not in {
            expected_storage_hash,
            configured_storage_hash,
        }
        and bound_storage_hash is None
    ):
        raise ValueError(
            "Platform-managed SQLAlchemy tables must use the MetaTable storage hash as "
            "their initial SQLAlchemy table name. Use PlatformManagedMetaTable or "
            "metatable_tablename(...) for __tablename__; registration will rebind it "
            "to the backend physical name. "
            f"Expected {configured_storage_hash!r} or {expected_storage_hash!r}, "
            f"got {table_name!r}."
        )

    table_contract = table_contract_from_sqlalchemy_model(
        model_or_table,
        target_meta_tables=_target_meta_tables,
        schema=resolved_schema,
        include_physical_table_name=False,
        require_metatable_foreign_keys=True,
    )
    return MetaTableRegistrationRequest(
        data_source_uid=_resolve_model_data_source_uid(
            model_or_table,
            data_source=data_source,
            data_source_uid=data_source_uid,
        ),
        management_mode="platform_managed",
        storage_hash=table_name,
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
        open_for_everyone=_resolve_bool_metadata(
            model_or_table,
            value=open_for_everyone,
            attr_name="__metatable_open_for_everyone__",
            info_key="open_for_everyone",
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
    open_for_everyone: bool = False,
    introspect: bool = True,
    target_meta_tables: Mapping[Any, Any] | None = None,
    target_meta_table_uid_by_fullname: Mapping[str, Any] | None = None,
    schema: str | None = None,
    hash_namespace: str | None = None,
    extra_hash_components: Mapping[str, Any] | None = None,
) -> MetaTableRegistrationRequest:
    table = _resolve_table(model_or_table)
    resolved_schema = _resolve_schema(table, schema=schema)
    resolved_identifier = _resolve_identifier(model_or_table, identifier=identifier)
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
    resolved_targets = _resolve_target_meta_table_uid_by_fullname(
        target_meta_tables=target_meta_tables,
        target_meta_table_uid_by_fullname=target_meta_table_uid_by_fullname,
    )
    resolved_storage_hash = storage_hash or build_meta_table_storage_hash(
        namespace=resolved_namespace,
        identifier=resolved_identifier,
        schema=resolved_schema,
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
        open_for_everyone=open_for_everyone,
        labels=list(labels or []),
        introspect=introspect,
        table_contract=table_contract_from_sqlalchemy_model(
            model_or_table,
            target_meta_tables=target_meta_tables,
            target_meta_table_uid_by_fullname=resolved_targets,
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
    table = getattr(model_or_table, "__table__", model_or_table)
    if not hasattr(table, "columns"):
        raise TypeError("Expected a SQLAlchemy model class/instance or Table-like object.")
    return table


def _table_name(table: Any) -> str:
    name = getattr(table, "name", None)
    if not name:
        raise ValueError("SQLAlchemy table metadata must expose a non-empty table name.")
    return str(name)


def _resolve_schema(table: Any, *, schema: str | None = None) -> str:
    resolved_schema = schema or getattr(table, "schema", None)
    if not resolved_schema:
        raise ValueError("MetaTable SQLAlchemy contracts require a SQLAlchemy table schema.")
    return str(resolved_schema)


def _resolve_class_namespace(cls: type[Any]) -> str:
    resolved_namespace = getattr(cls, "__metatable_namespace__", None)
    if not resolved_namespace:
        raise ValueError("PlatformManagedMetaTable requires __metatable_namespace__.")
    return str(resolved_namespace)


def _resolve_class_schema(cls: type[Any], *, metadata: Any | None = None) -> str:
    table_args = getattr(cls, "__table_args__", None)
    if isinstance(table_args, Mapping):
        schema = table_args.get("schema")
    elif isinstance(table_args, tuple) and table_args and isinstance(table_args[-1], Mapping):
        schema = table_args[-1].get("schema")
    else:
        schema = None
    if schema is None and metadata is not None:
        schema = getattr(metadata, "schema", None)
    return str(schema or "public")


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


def _resolve_target_meta_table_uid_by_fullname(
    *,
    target_meta_tables: Mapping[Any, Any] | None = None,
    target_meta_table_uid_by_fullname: Mapping[str, Any] | None = None,
) -> dict[str, str]:
    resolved: dict[str, str] = {}

    for target, meta_table in dict(target_meta_tables or {}).items():
        target_fullname = _target_table_fullname(target)
        target_uid = _target_meta_table_uid(meta_table)
        resolved[target_fullname] = target_uid

    for target_fullname, target_uid in dict(target_meta_table_uid_by_fullname or {}).items():
        target_fullname = str(target_fullname)
        target_uid = _target_meta_table_uid(target_uid)
        if target_fullname in resolved and resolved[target_fullname] != target_uid:
            raise ValueError(
                f"Conflicting target MetaTable UIDs for foreign key target {target_fullname!r}."
            )
        resolved[target_fullname] = target_uid

    return resolved


def _resolve_target_meta_table_uid_by_model(
    *,
    target_meta_tables: Mapping[Any, Any] | None = None,
    model_or_table: Any | None = None,
) -> dict[type[Any], str]:
    resolved: dict[type[Any], str] = {}
    for target, meta_table in dict(target_meta_tables or {}).items():
        if _is_platform_managed_meta_table_model(target):
            resolved[target] = _target_meta_table_uid(meta_table)
    if model_or_table is not None:
        for target_model in _metatable_foreign_key_target_models(model_or_table):
            if target_model in resolved:
                continue
            target_uid = target_model.get_meta_table_uid()
            if target_uid not in (None, ""):
                resolved[target_model] = str(target_uid)
    return resolved


def _begin_local_registration(
    model: type[PlatformManagedMetaTable],
    *,
    storage_hash: str,
    stack: tuple[str, ...],
) -> Any | None:
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
    meta_table: Any,
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


def _register_metatable_foreign_key_targets(
    model_or_table: Any,
    *,
    data_source: DynamicTableDataSource | None = None,
    data_source_uid: str | None = None,
    timeout: int | float | tuple[float, float] | None,
    stack: tuple[str, ...],
) -> dict[type[Any], Any]:
    target_meta_tables: dict[type[Any], Any] = {}
    for target_model in _metatable_foreign_key_target_models(model_or_table):
        target_meta_table = target_model.register(
            data_source=data_source,
            data_source_uid=data_source_uid,
            timeout=timeout,
            _registration_stack=stack,
        )
        target_meta_tables[target_model] = target_meta_table
    return target_meta_tables


def _metatable_foreign_key_target_models(
    model_or_table: Any,
) -> list[type[PlatformManagedMetaTable]]:
    table = _resolve_table(model_or_table)
    targets: list[type[PlatformManagedMetaTable]] = []
    seen: set[type[PlatformManagedMetaTable]] = set()
    for foreign_key_constraint in _iter_foreign_key_constraints(table):
        for element in list(getattr(foreign_key_constraint, "elements", []) or []):
            metadata = _metatable_foreign_key_metadata(element)
            if metadata is None:
                continue
            target_model = metadata.get("target_model")
            if not _is_platform_managed_meta_table_model(target_model):
                raise ValueError("MetaTableForeignKey metadata must include a target model class.")
            if target_model in seen:
                continue
            seen.add(target_model)
            targets.append(target_model)
    return targets


def _target_table_fullname(target: Any) -> str:
    if isinstance(target, str):
        return target

    table = _resolve_table(target)
    fullname = _table_reference_fullname(table)
    if fullname:
        return str(fullname)

    schema = getattr(table, "schema", None)
    name = _table_name(table)
    return f"{schema}.{name}" if schema else name


def _target_meta_table_uid(meta_table: Any) -> str:
    uid = getattr(meta_table, "uid", meta_table)
    if uid in (None, ""):
        raise ValueError("Target MetaTable must be a uid string or an object with a non-empty uid.")
    return str(uid)


def _coerce_optional_uid(value: Any) -> str | None:
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, str) and value:
        return value
    return None


def _meta_table_uid(meta_table: Any) -> str | None:
    if isinstance(meta_table, Mapping):
        uid = meta_table.get("meta_table_uid") or meta_table.get("uid")
    else:
        uid = (
            getattr(meta_table, "meta_table_uid", None)
            or getattr(meta_table, "__metatable_uid__", None)
            or getattr(meta_table, "uid", None)
        )
    return _coerce_optional_uid(uid)


def _meta_table_storage_hash(meta_table: Any) -> str | None:
    if isinstance(meta_table, Mapping):
        storage_hash = meta_table.get("storage_hash")
    else:
        storage_hash = getattr(meta_table, "storage_hash", None)
    if storage_hash in (None, ""):
        return None
    return str(storage_hash)


def _meta_table_physical_table_name(meta_table: Any) -> str | None:
    if isinstance(meta_table, Mapping):
        physical_table_name = meta_table.get("physical_table_name")
    else:
        physical_table_name = getattr(meta_table, "physical_table_name", None)
    if physical_table_name in (None, ""):
        return None
    return str(physical_table_name)


def _model_bound_storage_hash(model_or_table: Any) -> str | None:
    storage_hash = getattr(model_or_table, "__metatable_storage_hash__", None)
    if storage_hash not in (None, ""):
        return str(storage_hash)
    return None


def _bind_table_physical_name(
    table: Any,
    *,
    physical_table_name: str,
    storage_hash: str | None = None,
) -> None:
    current_name = _table_name(table)
    _bind_table_logical_identity(table, storage_hash=storage_hash or current_name)
    _remember_table_physical_to_logical_fullname(
        table,
        physical_table_name=physical_table_name,
    )
    if current_name == physical_table_name:
        _set_table_fullname(table, physical_table_name)
        return

    metadata = getattr(table, "metadata", None)
    schema = getattr(table, "schema", None)
    remove_table = getattr(metadata, "_remove_table", None)
    add_table = getattr(metadata, "_add_table", None)
    if callable(remove_table) and callable(add_table):
        remove_table(current_name, schema)
        table.name = physical_table_name
        _set_table_fullname(table, physical_table_name)
        add_table(physical_table_name, schema, table)
        return

    table.name = physical_table_name
    _set_table_fullname(table, physical_table_name)


def _bind_table_logical_identity(table: Any, *, storage_hash: str) -> None:
    schema = getattr(table, "schema", None)
    logical_fullname = _compose_table_fullname(schema=schema, table_name=storage_hash)
    table._mainsequence_storage_hash = str(storage_hash)
    table._mainsequence_logical_fullname = logical_fullname

    info = getattr(table, "info", None)
    if isinstance(info, MutableMapping):
        info["mainsequence_storage_hash"] = str(storage_hash)
        info["mainsequence_logical_fullname"] = logical_fullname


def _remember_table_physical_to_logical_fullname(
    table: Any,
    *,
    physical_table_name: str,
) -> None:
    logical_fullname = _table_reference_fullname(table)
    if logical_fullname in (None, ""):
        return

    schema = getattr(table, "schema", None)
    physical_fullname = _compose_table_fullname(
        schema=schema,
        table_name=physical_table_name,
    )
    _BOUND_PHYSICAL_TO_LOGICAL_FULLNAMES[physical_fullname] = str(logical_fullname)
    _BOUND_PHYSICAL_TO_LOGICAL_FULLNAMES[str(physical_table_name)] = str(logical_fullname)


def _set_table_fullname(table: Any, table_name: str) -> None:
    fullname = _compose_table_fullname(schema=getattr(table, "schema", None), table_name=table_name)
    try:
        table.fullname = fullname
    except Exception:
        return


def _compose_table_fullname(*, schema: str | None, table_name: str) -> str:
    return f"{schema}.{table_name}" if schema else str(table_name)


def _table_reference_fullname(table: Any) -> str | None:
    if table is None:
        return None
    logical_fullname = getattr(table, "_mainsequence_logical_fullname", None)
    if logical_fullname not in (None, ""):
        return str(logical_fullname)
    info = getattr(table, "info", None)
    if isinstance(info, Mapping):
        logical_fullname = info.get("mainsequence_logical_fullname")
        if logical_fullname not in (None, ""):
            return str(logical_fullname)

    fullname = getattr(table, "fullname", None)
    if fullname not in (None, ""):
        return str(fullname)

    name = getattr(table, "name", None)
    if name in (None, ""):
        return None
    return _compose_table_fullname(schema=getattr(table, "schema", None), table_name=str(name))


def _logical_fullname_for_target_fullname(table_fullname: str | None) -> str | None:
    if table_fullname in (None, ""):
        return None
    table_fullname = str(table_fullname)
    return _BOUND_PHYSICAL_TO_LOGICAL_FULLNAMES.get(table_fullname, table_fullname)


def _meta_table_data_source_uid(meta_table: Any) -> str | None:
    if isinstance(meta_table, Mapping):
        data_source_uid = meta_table.get("data_source_uid")
        data_source = meta_table.get("data_source")
    else:
        data_source_uid = getattr(meta_table, "data_source_uid", None) or getattr(
            meta_table, "__metatable_data_source_uid__", None
        )
        data_source = getattr(meta_table, "data_source", None)

    coerced_data_source_uid = _coerce_optional_uid(data_source_uid)
    if coerced_data_source_uid is not None:
        return coerced_data_source_uid
    if isinstance(data_source, Mapping):
        nested_uid = data_source.get("uid")
    else:
        nested_uid = getattr(data_source, "uid", None)
    return _coerce_optional_uid(nested_uid)


def _resolve_identifier(model_or_table: Any, *, identifier: str | None) -> str:
    resolved_identifier = (
        identifier
        or getattr(model_or_table, "__metatable_identifier__", None)
        or _table_info_value(model_or_table, "identifier")
    )
    if resolved_identifier in (None, ""):
        return _default_metatable_identifier(model_or_table)
    return str(resolved_identifier)


def resolve_metatable_identifier(
    model_or_table: Any,
    *,
    identifier: str | None = None,
) -> str:
    """Resolve the globally stable MetaTable identifier for a SQLAlchemy model."""

    return _resolve_identifier(model_or_table, identifier=identifier)


def _default_metatable_identifier(model_or_table: Any) -> str:
    module, qualname = _resolve_model_path(
        model_or_table,
        table_model_module=None,
        table_model_qualname=None,
    )
    if module == "sqlalchemy":
        raise ValueError(
            "MetaTable SQLAlchemy contracts require __metatable_identifier__ for "
            "plain SQLAlchemy Table objects."
        )

    project_name = _resolve_pyproject_project_name(model_or_table)
    if project_name in (None, ""):
        raise ValueError(
            "MetaTable SQLAlchemy contracts require __metatable_identifier__ when "
            "[project].name cannot be resolved from pyproject.toml."
        )
    return f"{project_name}:{module}.{qualname}"


def _resolve_pyproject_project_name(model_or_table: Any) -> str | None:
    candidate_dirs: list[pathlib.Path] = []
    module_name = getattr(model_or_table, "__module__", None)
    if module_name:
        module = sys.modules.get(str(module_name))
        module_file = getattr(module, "__file__", None) if module is not None else None
        if module_file:
            candidate_dirs.append(pathlib.Path(module_file).resolve().parent)
    candidate_dirs.append(pathlib.Path.cwd().resolve())

    seen: set[pathlib.Path] = set()
    for start_dir in candidate_dirs:
        for path in (start_dir, *start_dir.parents):
            if path in seen:
                continue
            seen.add(path)
            pyproject_path = path / "pyproject.toml"
            if not pyproject_path.exists():
                continue
            project_name = _read_pyproject_project_name(pyproject_path)
            if project_name not in (None, ""):
                return project_name
    return None


def _read_pyproject_project_name(pyproject_path: pathlib.Path) -> str | None:
    try:
        import tomllib
    except ModuleNotFoundError:  # pragma: no cover - Python < 3.11 fallback.
        return None
    try:
        pyproject = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    project_name = pyproject.get("project", {}).get("name")
    if project_name in (None, ""):
        return None
    return str(project_name).strip() or None


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


def _iter_indexes(table: Any) -> list[Any]:
    indexes = getattr(table, "indexes", None) or []
    return list(indexes)


def _iter_foreign_key_constraints(table: Any) -> list[Any]:
    constraints = getattr(table, "foreign_key_constraints", None) or []
    return list(constraints)


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
        foreign_key_constraints=_iter_foreign_key_constraints(table),
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
    indexes = [item for item in table_items if _looks_like_index(item)]
    foreign_key_constraints = [item for item in table_items if _looks_like_foreign_key(item)]
    return _storage_identity_from_parts(
        columns=columns,
        indexes=indexes,
        foreign_key_constraints=foreign_key_constraints,
    )


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

    time_index_cls = globals().get("PlatformTimeIndexMetaData")
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
        raise ValueError("PlatformTimeIndexMetaData requires a non-empty time_index_name.")
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
            "PlatformTimeIndexMetaData requires at least the time index in index_names."
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
            "PlatformTimeIndexMetaData storage_layout must be a mapping when provided."
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
            "PlatformTimeIndexMetaData index_names must start with the time_index_name. "
            f"Expected {time_index_name!r}, got {names[0]!r}."
        )
    if len(set(names)) != len(names):
        raise ValueError("PlatformTimeIndexMetaData index_names cannot contain duplicates.")

    columns_by_name = {str(column.name): column for column in columns}
    missing = [name for name in names if name not in columns_by_name]
    if missing:
        raise ValueError(
            "PlatformTimeIndexMetaData index_names must all exist as table columns. "
            f"Missing columns: {missing!r}."
        )

    time_column = columns_by_name[time_index_name]
    time_type = _column_type_contract(time_column)["data_type"]
    if not is_temporal_token(time_type):
        raise ValueError(
            "PlatformTimeIndexMetaData time_index_name must reference a temporal column. "
            f"Column {time_index_name!r} has type {time_type!r}."
        )

    nullable_index_columns = [
        name for name in names if bool(getattr(columns_by_name[name], "nullable", True))
    ]
    if nullable_index_columns:
        raise ValueError(
            "PlatformTimeIndexMetaData index columns must be non-nullable. "
            f"Nullable index columns: {nullable_index_columns!r}."
        )


def _storage_identity_from_parts(
    *,
    columns: Sequence[Any],
    indexes: Sequence[Any],
    foreign_key_constraints: Sequence[Any],
) -> dict[str, Any]:
    return {
        "columns": [
            _column_storage_identity(column, ordinal_position=position)
            for position, column in enumerate(columns)
        ],
        "indexes": sorted(
            (_index_storage_identity(index) for index in indexes),
            key=lambda item: repr(item),
        ),
        "foreign_keys": sorted(
            _foreign_key_storage_identities(
                columns=columns,
                foreign_key_constraints=foreign_key_constraints,
            ),
            key=lambda item: repr(item),
        ),
    }


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


def _looks_like_column(value: Any) -> bool:
    return getattr(value, "name", None) not in (None, "") and hasattr(value, "type")


def _looks_like_index(value: Any) -> bool:
    return hasattr(value, "columns") and hasattr(value, "unique") and not hasattr(value, "type")


def _looks_like_foreign_key(value: Any) -> bool:
    return hasattr(value, "elements") and not hasattr(value, "type")


def _index_storage_identity(index: Any) -> dict[str, Any]:
    columns = _column_names(getattr(index, "columns", []))
    if not columns:
        columns = _column_names(getattr(index, "expressions", []))
    return {
        "columns": columns,
        "unique": bool(getattr(index, "unique", False)),
        "method": _index_method(index),
        "expression": None if columns else str(index),
    }


def _foreign_key_storage_identities(
    *,
    columns: Sequence[Any],
    foreign_key_constraints: Sequence[Any],
) -> list[dict[str, Any]]:
    identities = [
        _foreign_key_storage_identity(foreign_key_constraint)
        for foreign_key_constraint in foreign_key_constraints
    ]
    if not identities:
        identities = _column_foreign_key_storage_identities(columns)
    return _deduplicate_identity_dicts(identities)


def _foreign_key_storage_identity(foreign_key_constraint: Any) -> dict[str, Any]:
    elements = list(getattr(foreign_key_constraint, "elements", []) or [])
    if not elements:
        return {
            "source_columns": [],
            "target_table": None,
            "target_columns": [],
            "on_delete": "restrict",
        }
    target_table = _foreign_key_element_target_table(elements[0])
    on_delete = getattr(elements[0], "ondelete", None) or getattr(
        foreign_key_constraint,
        "ondelete",
        None,
    )
    return {
        "source_columns": [str(element.parent.name) for element in elements],
        "target_table": target_table,
        "target_columns": [str(element.column.name) for element in elements],
        "on_delete": str(on_delete or "restrict").lower(),
    }


def _column_foreign_key_storage_identities(columns: Sequence[Any]) -> list[dict[str, Any]]:
    identities: list[dict[str, Any]] = []
    for column in columns:
        for foreign_key in sorted(
            list(getattr(column, "foreign_keys", []) or []),
            key=lambda item: str(item),
        ):
            target_table, target_column = _foreign_key_target_parts(foreign_key)
            identities.append(
                {
                    "source_columns": [str(column.name)],
                    "target_table": target_table,
                    "target_columns": [target_column] if target_column else [],
                    "on_delete": str(getattr(foreign_key, "ondelete", None) or "restrict").lower(),
                }
            )
    return identities


def _foreign_key_element_target_table(element: Any) -> str | None:
    try:
        target_column = getattr(element, "column", None)
    except Exception:
        target_column = None
    target_table = getattr(target_column, "table", None)
    fullname = _table_reference_fullname(target_table)
    if fullname:
        return str(fullname)
    target_fullname = getattr(element, "target_fullname", None)
    if target_fullname:
        target_table, _target_column = _split_target_fullname(str(target_fullname))
        return _logical_fullname_for_target_fullname(target_table)
    return None


def _foreign_key_target_parts(foreign_key: Any) -> tuple[str | None, str | None]:
    try:
        target_column = getattr(foreign_key, "column", None)
    except Exception:
        target_column = None
    target_table = getattr(target_column, "table", None)
    target_table_fullname = _table_reference_fullname(target_table)
    target_column_name = getattr(target_column, "name", None)
    if target_table_fullname:
        return (
            str(target_table_fullname),
            str(target_column_name) if target_column_name else None,
        )

    target_fullname = getattr(foreign_key, "target_fullname", None) or getattr(
        foreign_key,
        "_colspec",
        None,
    )
    if target_fullname:
        return _split_target_fullname(str(target_fullname))
    return None, str(target_column_name) if target_column_name else None


def _split_target_fullname(target_fullname: str) -> tuple[str | None, str | None]:
    if "." not in target_fullname:
        return None, target_fullname or None
    target_table, target_column = target_fullname.rsplit(".", 1)
    return _logical_fullname_for_target_fullname(target_table), target_column or None


def _deduplicate_identity_dicts(items: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    deduplicated: dict[str, dict[str, Any]] = {}
    for item in items:
        deduplicated[repr(sorted(item.items()))] = item
    return list(deduplicated.values())


def _index_contract(index: Any) -> MetaTableIndexContract:
    if not getattr(index, "name", None):
        raise ValueError("MetaTable SQLAlchemy indexes must be explicitly named.")
    columns = _column_names(getattr(index, "columns", []))
    expression = None if columns else str(index)
    return MetaTableIndexContract(
        name=str(index.name),
        columns=columns,
        unique=bool(getattr(index, "unique", False)),
        method=_index_method(index),
        expression=expression,
    )


def _index_method(index: Any) -> str | None:
    dialect_options = getattr(index, "dialect_options", None)
    if isinstance(dialect_options, Mapping):
        postgresql_options = dialect_options.get("postgresql")
        if isinstance(postgresql_options, Mapping):
            method = postgresql_options.get("using")
            if method:
                return str(method)
    return None


def _foreign_key_contract(
    foreign_key_constraint: Any,
    *,
    target_meta_table_uid_by_model: Mapping[type[Any], Any],
    target_meta_table_uid_by_fullname: Mapping[str, Any],
    require_metatable_foreign_keys: bool = False,
) -> MetaTableForeignKeyContract:
    elements = list(getattr(foreign_key_constraint, "elements", []) or [])
    if not elements:
        raise ValueError(f"Foreign key {foreign_key_constraint.name!r} does not expose elements.")

    sdk_target = _metatable_foreign_key_contract_target(
        elements,
        target_meta_table_uid_by_model=target_meta_table_uid_by_model,
    )
    if sdk_target is None:
        if require_metatable_foreign_keys:
            raise ValueError(
                "Platform-managed MetaTable foreign keys must use "
                "MetaTableForeignKey(TargetModel, column=...)."
            )
        if not getattr(foreign_key_constraint, "name", None):
            raise ValueError("MetaTable SQLAlchemy foreign keys must be explicitly named.")
        target_tables = {
            _table_reference_fullname(element.column.table)
            for element in elements
            if _table_reference_fullname(element.column.table)
        }
        if len(target_tables) != 1:
            raise ValueError("Composite foreign keys must target one table.")
        target_table_fullname = next(iter(target_tables))
        target_meta_table_uid = _lookup_target_meta_table_uid(
            target_table_fullname,
            target_meta_table_uid_by_fullname=target_meta_table_uid_by_fullname,
        )
        target_columns = [str(element.column.name) for element in elements]
    else:
        target_meta_table_uid, target_columns = sdk_target

    foreign_key_name = None
    if not require_metatable_foreign_keys:
        foreign_key_name = getattr(foreign_key_constraint, "name", None) or (
            _derive_metatable_foreign_key_name(elements) if sdk_target is not None else None
        )
        if not foreign_key_name:
            raise ValueError("MetaTable SQLAlchemy foreign keys must be explicitly named.")

    on_delete = getattr(elements[0], "ondelete", None) or getattr(
        foreign_key_constraint,
        "ondelete",
        None,
    )
    return MetaTableForeignKeyContract(
        name=str(foreign_key_name) if foreign_key_name else None,
        source_columns=[str(element.parent.name) for element in elements],
        target_meta_table_uid=target_meta_table_uid,
        target_columns=target_columns,
        on_delete=str(on_delete or "restrict").lower(),
    )


def _time_indexed_meta_table_foreign_key_contract(
    foreign_key_constraint: Any,
    *,
    target_meta_table_uid_by_model: Mapping[type[Any], Any],
    target_meta_table_uid_by_fullname: Mapping[str, Any],
    require_metatable_foreign_keys: bool = False,
) -> MetaTableForeignKeyContract:
    elements = list(getattr(foreign_key_constraint, "elements", []) or [])
    if not elements:
        raise ValueError("Time-indexed MetaTable SQLAlchemy foreign keys must expose elements.")

    sdk_target = _metatable_foreign_key_contract_target(
        elements,
        target_meta_table_uid_by_model=target_meta_table_uid_by_model,
    )
    if sdk_target is None:
        if require_metatable_foreign_keys:
            raise ValueError(
                "Platform-managed MetaTable foreign keys must use "
                "MetaTableForeignKey(TargetModel, column=...)."
            )
        target_tables = {
            _foreign_key_element_target_table(element)
            for element in elements
            if _foreign_key_element_target_table(element)
        }
        if len(target_tables) != 1:
            raise ValueError("Composite foreign keys must target one table.")

        target_table_fullname = next(iter(target_tables))
        target_meta_table_uid = _lookup_target_meta_table_uid(
            target_table_fullname,
            target_meta_table_uid_by_fullname=target_meta_table_uid_by_fullname,
        )
        target_columns = [str(element.column.name) for element in elements]
    else:
        target_meta_table_uid, target_columns = sdk_target

    on_delete = getattr(elements[0], "ondelete", None) or getattr(
        foreign_key_constraint,
        "ondelete",
        None,
    )
    return MetaTableForeignKeyContract(
        source_columns=[str(element.parent.name) for element in elements],
        target_meta_table_uid=target_meta_table_uid,
        target_columns=target_columns,
        on_delete=str(on_delete or "restrict").lower(),
    )


def _metatable_foreign_key_contract_target(
    elements: Sequence[Any],
    *,
    target_meta_table_uid_by_model: Mapping[type[Any], Any],
) -> tuple[str, list[str]] | None:
    metadata_by_element = [_metatable_foreign_key_metadata(element) for element in elements]
    if not any(metadata_by_element):
        return None
    if not all(metadata_by_element):
        raise ValueError(
            "Composite MetaTableForeignKey constraints must use MetaTableForeignKey "
            "for every target element."
        )

    target_models: list[type[PlatformManagedMetaTable]] = []
    target_columns: list[str] = []
    for metadata in metadata_by_element:
        if metadata is None:
            continue
        target_model = metadata.get("target_model")
        target_column = metadata.get("target_column")
        if not _is_platform_managed_meta_table_model(target_model):
            raise ValueError("MetaTableForeignKey metadata must include a target model class.")
        if target_column in (None, ""):
            raise ValueError("MetaTableForeignKey metadata must include a target column name.")
        target_models.append(target_model)
        target_columns.append(str(target_column))

    if len(set(target_models)) != 1:
        raise ValueError("Composite MetaTableForeignKey constraints must target one model.")

    target_model = target_models[0]
    target_meta_table_uid = _lookup_target_meta_table_uid_by_model(
        target_model,
        target_meta_table_uid_by_model=target_meta_table_uid_by_model,
    )
    return target_meta_table_uid, target_columns


def _metatable_foreign_key_metadata(element: Any) -> Mapping[str, Any] | None:
    info = getattr(element, "info", None)
    if not isinstance(info, Mapping):
        return None
    metadata = info.get(_METATABLE_FOREIGN_KEY_INFO_KEY)
    if metadata is None:
        return None
    if not isinstance(metadata, Mapping):
        raise ValueError("MetaTableForeignKey metadata must be a mapping.")
    return metadata


def _derive_metatable_foreign_key_name(elements: Sequence[Any]) -> str:
    source_columns = [str(element.parent.name) for element in elements]
    source_table = None
    if elements:
        source_table = getattr(getattr(elements[0], "parent", None), "table", None)
    source_table_name = None
    if source_table is not None:
        source_table_name = _table_name(source_table)
    name_parts = [part for part in [source_table_name, "_".join(source_columns), "fkey"] if part]
    return _truncate_postgresql_identifier("_".join(name_parts))


def _lookup_target_meta_table_uid_by_model(
    target_model: type[PlatformManagedMetaTable],
    *,
    target_meta_table_uid_by_model: Mapping[type[Any], Any],
) -> str:
    target_uid = target_meta_table_uid_by_model.get(target_model)
    if target_uid is not None:
        return _target_meta_table_uid(target_uid)

    get_meta_table_uid = getattr(target_model, "get_meta_table_uid", None)
    if callable(get_meta_table_uid):
        target_uid = get_meta_table_uid()
        if target_uid not in (None, ""):
            return str(target_uid)

    model_name = getattr(target_model, "__qualname__", repr(target_model))
    raise ValueError(
        "Missing registered target MetaTable UID for MetaTableForeignKey target "
        f"{model_name}. Register the target model through register() before building "
        "a request directly."
    )


def _lookup_target_meta_table_uid(
    target_table_fullname: str,
    *,
    target_meta_table_uid_by_fullname: Mapping[str, Any],
) -> str:
    target_uid = target_meta_table_uid_by_fullname.get(target_table_fullname)
    if target_uid is None:
        raise ValueError(
            "Missing registered target MetaTable UID for foreign key target "
            f"{target_table_fullname!r}."
        )
    if hasattr(target_uid, "uid"):
        target_uid = target_uid.uid
    return str(target_uid)


def _column_names(columns: Any) -> list[str]:
    return [
        str(column if isinstance(column, str) else column.name)
        for column in list(columns or [])
        if isinstance(column, str) or getattr(column, "name", None) not in (None, "")
    ]


__all__ = [
    "DEFAULT_PLATFORM_MANAGED_PROVISIONING",
    "MetaTableForeignKey",
    "PlatformManagedMetaTable",
    "PlatformTimeIndexMetaData",
    "external_registered_registration_request_from_sqlalchemy_model",
    "metatable_configured_tablename",
    "metatable_tablename",
    "platform_managed_registration_request_from_sqlalchemy_model",
    "register_external_sqlalchemy_model",
    "resolve_metatable_identifier",
    "table_contract_from_sqlalchemy_model",
    "time_indexed_registration_request_from_sqlalchemy_model",
]
