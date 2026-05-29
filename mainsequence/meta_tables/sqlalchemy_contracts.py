from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, ClassVar
from uuid import UUID

from mainsequence.client.dtype_codec import (
    is_temporal_token,
    sqlalchemy_backend_type,
    sqlalchemy_type_to_token,
)
from mainsequence.client.models_metatables import (
    MetaTable,
    MetaTableColumnContract,
    MetaTableContract,
    MetaTableForeignKeyContract,
    MetaTableIndexContract,
    MetaTablePhysicalContract,
    MetaTableRegistrationRequest,
)

from .hashing import build_meta_table_configured_storage_hash, build_meta_table_storage_hash

DEFAULT_PLATFORM_MANAGED_PROVISIONING = {
    "create_table": True,
    "if_not_exists": True,
}
SERVER_GENERATED_UUID_DEFAULT = "gen_random_uuid()"
POSTGRESQL_MAX_IDENTIFIER_LENGTH = 63


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


try:
    from sqlalchemy.orm import declared_attr as _sqlalchemy_declared_attr
except ImportError:  # pragma: no cover - SQLAlchemy is optional for the SDK.
    _sqlalchemy_declared_attr = None


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
    return _build_configured_storage_hash(
        namespace=resolved_namespace,
        schema=resolved_schema,
        table_storage_identity=_configured_table_storage_identity(model_or_table, table=table),
        hash_namespace=hash_namespace,
        extra_hash_components=extra_hash_components,
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

    The class derives the final physical table name from storage-relevant
    metadata plus the SQLAlchemy table storage shape, builds the platform
    registration request, and registers the MetaTable through TS Manager.
    The logical MetaTable identifier is registration metadata and does not
    rotate this configured name.
    """

    __metatable_use_configured_table_name__ = True
    __metatable__: ClassVar[Any | None] = None
    __metatable_uid__: ClassVar[str | None] = None
    __metatable_data_source_uid__: ClassVar[str | None] = None

    if _sqlalchemy_declared_attr is not None:
        __tablename__ = _sqlalchemy_declared_attr.directive(_metatable_declared_tablename)

    @classmethod
    def __table_cls__(cls, *args: Any, **kwargs: Any) -> Any:
        if _sqlalchemy_declared_attr is None:
            raise ImportError(
                "PlatformManagedMetaTable requires SQLAlchemy. Install SQLAlchemy in the "
                "application environment before using this mixin."
            )
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
        data_source: Any | None = None,
        data_source_uid: str | None = None,
        identifier: str | None = None,
        namespace: str | None = None,
        description: str | None = None,
        labels: Sequence[str] | None = None,
        protect_from_deletion: bool = False,
        open_for_everyone: bool = False,
        provisioning: Mapping[str, Any] | None = None,
        introspect: bool = False,
        target_meta_tables: Mapping[Any, Any] | None = None,
        target_meta_table_uid_by_fullname: Mapping[str, Any] | None = None,
        foreign_key_lookup_timeout: int | float | tuple[float, float] | None = None,
        hash_namespace: str | None = None,
        extra_hash_components: Mapping[str, Any] | None = None,
        enforce_storage_hash_name: bool = True,
    ) -> MetaTableRegistrationRequest:
        resolved_data_source_uid = _resolve_data_source_uid(
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
            target_meta_table_uid_by_fullname=cls.resolve_foreign_key_targets(
                data_source_uid=resolved_data_source_uid,
                target_meta_tables=target_meta_tables,
                target_meta_table_uid_by_fullname=target_meta_table_uid_by_fullname,
                timeout=foreign_key_lookup_timeout,
            ),
            hash_namespace=hash_namespace,
            extra_hash_components=extra_hash_components,
            enforce_storage_hash_name=enforce_storage_hash_name,
        )

    @classmethod
    def register(
        cls,
        *,
        timeout: int | float | tuple[float, float] | None = None,
        data_source: Any | None = None,
        data_source_uid: str | None = None,
        identifier: str | None = None,
        namespace: str | None = None,
        description: str | None = None,
        labels: Sequence[str] | None = None,
        protect_from_deletion: bool = False,
        open_for_everyone: bool = False,
        provisioning: Mapping[str, Any] | None = None,
        introspect: bool = False,
        target_meta_tables: Mapping[Any, Any] | None = None,
        target_meta_table_uid_by_fullname: Mapping[str, Any] | None = None,
        foreign_key_lookup_timeout: int | float | tuple[float, float] | None = None,
        hash_namespace: str | None = None,
        extra_hash_components: Mapping[str, Any] | None = None,
        enforce_storage_hash_name: bool = True,
    ) -> MetaTable:
        request = cls.build_registration_request(
            data_source=data_source,
            data_source_uid=data_source_uid,
            identifier=identifier,
            namespace=namespace,
            description=description,
            labels=labels,
            protect_from_deletion=protect_from_deletion,
            open_for_everyone=open_for_everyone,
            provisioning=provisioning,
            introspect=introspect,
            target_meta_tables=target_meta_tables,
            target_meta_table_uid_by_fullname=target_meta_table_uid_by_fullname,
            foreign_key_lookup_timeout=(
                timeout if foreign_key_lookup_timeout is None else foreign_key_lookup_timeout
            ),
            hash_namespace=hash_namespace,
            extra_hash_components=extra_hash_components,
            enforce_storage_hash_name=enforce_storage_hash_name,
        )
        meta_table = MetaTable.register(request, timeout=timeout)
        cls.bind_meta_table(meta_table)
        return meta_table

    @classmethod
    def bind_meta_table(cls, meta_table: Any) -> Any:
        """Attach an already-created backend MetaTable resource to this authoring model."""
        meta_table_uid = _meta_table_uid(meta_table)
        if meta_table_uid in (None, ""):
            raise ValueError("PlatformManagedMetaTable.bind_meta_table requires meta_table.uid.")

        cls.__metatable__ = meta_table
        cls.__metatable_uid__ = str(meta_table_uid)

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
        return _table_name(_resolve_table(cls))

    @classmethod
    def resolve_foreign_key_targets(
        cls,
        *,
        data_source: Any | None = None,
        data_source_uid: str | None = None,
        target_meta_tables: Mapping[Any, Any] | None = None,
        target_meta_table_uid_by_fullname: Mapping[str, Any] | None = None,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> dict[str, str]:
        resolved_data_source_uid = _resolve_data_source_uid(
            data_source=data_source,
            data_source_uid=data_source_uid,
        )
        explicit_targets = _resolve_target_meta_table_uid_by_fullname(
            target_meta_tables=target_meta_tables,
            target_meta_table_uid_by_fullname=target_meta_table_uid_by_fullname,
        )
        return _resolve_registered_foreign_key_targets(
            cls,
            data_source_uid=resolved_data_source_uid,
            resolved_targets=explicit_targets,
            timeout=timeout,
        )


class PlatformTimeIndexMetaData(PlatformManagedMetaTable):
    """SQLAlchemy declarative base mixin for platform-managed DynamicTableMetaData.

    This is the SDK authoring surface for time-indexed DataNode storage. It
    reuses the MetaTable column/type/FK projection, but registers through the
    DynamicTable endpoint and validates the opinionated table shape:
    the first index must be the time index, and any extra index dimensions are
    ordinary non-null table columns.
    """

    __time_index_metadata__: ClassVar[Any | None] = None

    if _sqlalchemy_declared_attr is not None:
        __mapper_args__ = _sqlalchemy_declared_attr.directive(_time_index_mapper_args)

    @classmethod
    def bind_meta_table(cls, meta_table: Any) -> Any:
        bound = super().bind_meta_table(meta_table)
        cls.__time_index_metadata__ = bound
        return bound

    @classmethod
    def get_time_index_metadata(cls) -> Any | None:
        return getattr(cls, "__time_index_metadata__", None)

    @classmethod
    def __table_cls__(cls, *args: Any, **kwargs: Any) -> Any:
        if _sqlalchemy_declared_attr is None:
            raise ImportError(
                "PlatformTimeIndexMetaData requires SQLAlchemy. Install SQLAlchemy in the "
                "application environment before using this mixin."
            )
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
        data_source: Any | None = None,
        data_source_uid: str | None = None,
        identifier: str | None = None,
        namespace: str | None = None,
        description: str | None = None,
        labels: Sequence[str] | None = None,
        protect_from_deletion: bool = False,
        provisioning: Mapping[str, Any] | None = None,
        target_meta_tables: Mapping[Any, Any] | None = None,
        target_meta_table_uid_by_fullname: Mapping[str, Any] | None = None,
        foreign_key_lookup_timeout: int | float | tuple[float, float] | None = None,
        hash_namespace: str | None = None,
        extra_hash_components: Mapping[str, Any] | None = None,
        enforce_storage_hash_name: bool = True,
        time_index_name: str | None = None,
        index_names: Sequence[str] | None = None,
        storage_layout: Mapping[str, Any] | None = None,
    ) -> Any:
        resolved_data_source_uid = _resolve_data_source_uid(
            data_source=data_source,
            data_source_uid=data_source_uid,
        )
        resolved_targets = cls.resolve_foreign_key_targets(
            data_source_uid=resolved_data_source_uid,
            target_meta_tables=target_meta_tables,
            target_meta_table_uid_by_fullname=target_meta_table_uid_by_fullname,
            timeout=foreign_key_lookup_timeout,
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
            target_meta_table_uid_by_fullname=resolved_targets,
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
        timeout: int | float | tuple[float, float] | None = None,
        data_source: Any | None = None,
        data_source_uid: str | None = None,
        identifier: str | None = None,
        namespace: str | None = None,
        description: str | None = None,
        labels: Sequence[str] | None = None,
        protect_from_deletion: bool = False,
        provisioning: Mapping[str, Any] | None = None,
        target_meta_tables: Mapping[Any, Any] | None = None,
        target_meta_table_uid_by_fullname: Mapping[str, Any] | None = None,
        foreign_key_lookup_timeout: int | float | tuple[float, float] | None = None,
        hash_namespace: str | None = None,
        extra_hash_components: Mapping[str, Any] | None = None,
        enforce_storage_hash_name: bool = True,
        time_index_name: str | None = None,
        index_names: Sequence[str] | None = None,
        storage_layout: Mapping[str, Any] | None = None,
    ) -> Any:
        request = cls.build_registration_request(
            data_source=data_source,
            data_source_uid=data_source_uid,
            identifier=identifier,
            namespace=namespace,
            description=description,
            labels=labels,
            protect_from_deletion=protect_from_deletion,
            provisioning=provisioning,
            target_meta_tables=target_meta_tables,
            target_meta_table_uid_by_fullname=target_meta_table_uid_by_fullname,
            foreign_key_lookup_timeout=(
                timeout if foreign_key_lookup_timeout is None else foreign_key_lookup_timeout
            ),
            hash_namespace=hash_namespace,
            extra_hash_components=extra_hash_components,
            enforce_storage_hash_name=enforce_storage_hash_name,
            time_index_name=time_index_name,
            index_names=index_names,
            storage_layout=storage_layout,
        )

        from mainsequence.client.models_tdag import TimeIndexMetaData

        time_index_metadata = TimeIndexMetaData.register(request, timeout=timeout)
        cls.bind_meta_table(time_index_metadata)
        return time_index_metadata


def table_contract_from_sqlalchemy_model(
    model_or_table: Any,
    *,
    table_model_module: str | None = None,
    table_model_qualname: str | None = None,
    target_meta_table_uid_by_fullname: Mapping[str, Any] | None = None,
    schema: str | None = None,
) -> MetaTableContract:
    table = _resolve_table(model_or_table)
    _resolve_schema(table, schema=schema)
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
            table_name=_table_name(table),
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
                target_meta_table_uid_by_fullname=target_meta_table_uid_by_fullname or {},
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
    data_source: Any | None = None,
    data_source_uid: str | None = None,
    identifier: str | None = None,
    namespace: str | None = None,
    description: str | None = None,
    labels: Sequence[str] | None = None,
    protect_from_deletion: bool = False,
    provisioning: Mapping[str, Any] | None = None,
    target_meta_table_uid_by_fullname: Mapping[str, Any] | None = None,
    schema: str | None = None,
    hash_namespace: str | None = None,
    extra_hash_components: Mapping[str, Any] | None = None,
    enforce_storage_hash_name: bool = True,
    time_index_name: str | None = None,
    index_names: Sequence[str] | None = None,
    storage_layout: Mapping[str, Any] | None = None,
) -> Any:
    from mainsequence.client.models_tdag import TimeIndexMetaTableRegistrationRequest

    table = _resolve_table(model_or_table)
    resolved_schema = _resolve_schema(table, schema=schema)
    resolved_identifier = _resolve_identifier(model_or_table, identifier=identifier)
    resolved_namespace = _resolve_namespace(model_or_table, namespace=namespace)
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
        hash_namespace=hash_namespace,
        extra_hash_components=extra_hash_components,
    )
    table_name = _table_name(table)
    if enforce_storage_hash_name and table_name != configured_storage_hash:
        raise ValueError(
            "Platform-managed time-indexed SQLAlchemy tables must use the configured "
            "time-indexed MetaTable storage hash as their physical table name. Use PlatformTimeIndexMetaData "
            "or metatable_configured_tablename(...) for __tablename__. "
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
        _source_table_foreign_key_contract(
            foreign_key_constraint,
            target_meta_table_uid_by_fullname=target_meta_table_uid_by_fullname or {},
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
        data_source_uid=_resolve_data_source_uid(
            data_source=data_source,
            data_source_uid=data_source_uid,
        ),
        storage_hash=table_name,
        identifier=resolved_identifier,
        namespace=resolved_namespace,
        description=description,
        protect_from_deletion=protect_from_deletion,
        labels=list(labels or []),
        provisioning=dict(provisioning or DEFAULT_PLATFORM_MANAGED_PROVISIONING),
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
    data_source: Any | None = None,
    data_source_uid: str | None = None,
    identifier: str | None = None,
    namespace: str | None = None,
    description: str | None = None,
    labels: Sequence[str] | None = None,
    protect_from_deletion: bool = False,
    open_for_everyone: bool = False,
    provisioning: Mapping[str, Any] | None = None,
    introspect: bool = False,
    target_meta_table_uid_by_fullname: Mapping[str, Any] | None = None,
    schema: str | None = None,
    hash_namespace: str | None = None,
    extra_hash_components: Mapping[str, Any] | None = None,
    enforce_storage_hash_name: bool = True,
) -> MetaTableRegistrationRequest:
    table = _resolve_table(model_or_table)
    resolved_schema = _resolve_schema(table, schema=schema)
    resolved_identifier = _resolve_identifier(model_or_table, identifier=identifier)
    resolved_namespace = _resolve_namespace(model_or_table, namespace=namespace)
    expected_storage_hash = build_meta_table_storage_hash(
        namespace=resolved_namespace,
        identifier=resolved_identifier,
        schema=resolved_schema,
        hash_namespace=hash_namespace,
        extra_hash_components=extra_hash_components,
    )
    configured_storage_hash = _build_configured_storage_hash(
        namespace=resolved_namespace,
        schema=resolved_schema,
        table_storage_identity=_table_storage_identity(table),
        hash_namespace=hash_namespace,
        extra_hash_components=extra_hash_components,
    )
    table_name = _table_name(table)
    if enforce_storage_hash_name and table_name not in {
        expected_storage_hash,
        configured_storage_hash,
    }:
        raise ValueError(
            "Platform-managed SQLAlchemy tables must use the MetaTable storage hash as "
            "their physical table name. Use PlatformManagedMetaTable or metatable_tablename(...) "
            "for __tablename__. "
            f"Expected {configured_storage_hash!r} or {expected_storage_hash!r}, "
            f"got {table_name!r}."
        )

    table_contract = table_contract_from_sqlalchemy_model(
        model_or_table,
        target_meta_table_uid_by_fullname=target_meta_table_uid_by_fullname,
        schema=resolved_schema,
    )
    return MetaTableRegistrationRequest(
        data_source_uid=_resolve_data_source_uid(
            data_source=data_source,
            data_source_uid=data_source_uid,
        ),
        management_mode="platform_managed",
        storage_hash=table_name,
        identifier=resolved_identifier,
        namespace=resolved_namespace,
        description=description,
        protect_from_deletion=protect_from_deletion,
        open_for_everyone=open_for_everyone,
        labels=list(labels or []),
        provisioning=dict(provisioning or DEFAULT_PLATFORM_MANAGED_PROVISIONING),
        introspect=introspect,
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
    target_meta_table_uid_by_fullname: Mapping[str, Any] | None = None,
    schema: str | None = None,
    hash_namespace: str | None = None,
    extra_hash_components: Mapping[str, Any] | None = None,
) -> MetaTableRegistrationRequest:
    table = _resolve_table(model_or_table)
    resolved_schema = _resolve_schema(table, schema=schema)
    resolved_identifier = _resolve_identifier(model_or_table, identifier=identifier)
    resolved_namespace = _resolve_namespace(model_or_table, namespace=namespace)
    resolved_storage_hash = storage_hash or build_meta_table_storage_hash(
        namespace=resolved_namespace,
        identifier=resolved_identifier,
        schema=resolved_schema,
        hash_namespace=hash_namespace,
        extra_hash_components=extra_hash_components,
    )

    return MetaTableRegistrationRequest(
        data_source_uid=str(data_source_uid),
        management_mode="external_registered",
        storage_hash=resolved_storage_hash,
        identifier=resolved_identifier,
        namespace=resolved_namespace,
        description=description,
        protect_from_deletion=protect_from_deletion,
        open_for_everyone=open_for_everyone,
        labels=list(labels or []),
        introspect=introspect,
        table_contract=table_contract_from_sqlalchemy_model(
            model_or_table,
            target_meta_table_uid_by_fullname=target_meta_table_uid_by_fullname,
            schema=resolved_schema,
        ),
    )


def register_platform_managed_sqlalchemy_model(
    model_or_table: Any,
    *,
    data_source: Any | None = None,
    data_source_uid: str | None = None,
    timeout: int | float | tuple[float, float] | None = None,
    **kwargs: Any,
) -> MetaTable:
    request = platform_managed_registration_request_from_sqlalchemy_model(
        model_or_table,
        data_source=data_source,
        data_source_uid=data_source_uid,
        **kwargs,
    )
    meta_table = MetaTable.register(request, timeout=timeout)
    binder = getattr(model_or_table, "bind_meta_table", None)
    if callable(binder):
        binder(meta_table)
    return meta_table


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
    data_source: Any | None = None,
    data_source_uid: str | None = None,
) -> str:
    if data_source_uid:
        return str(data_source_uid)

    resolved_data_source = data_source
    if resolved_data_source is None:
        try:
            from mainsequence.client.models_tdag import get_session_data_source
        except ImportError as exc:  # pragma: no cover - defensive import guard.
            raise RuntimeError("Could not import the session data source resolver.") from exc
        resolved_data_source = get_session_data_source()

    uid = getattr(resolved_data_source, "uid", None)
    if not uid:
        raise ValueError(
            "Could not resolve a DynamicTableDataSource uid. Run inside a configured "
            "Main Sequence project/session, or pass data_source=... / data_source_uid=... "
            "explicitly."
        )
    return str(uid)


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


def _resolve_registered_foreign_key_targets(
    model_or_table: Any,
    *,
    data_source_uid: str,
    resolved_targets: Mapping[str, str],
    timeout: int | float | tuple[float, float] | None = None,
) -> dict[str, str]:
    resolved = dict(resolved_targets)
    for target_fullname in sorted(_foreign_key_target_fullnames(_resolve_table(model_or_table))):
        if target_fullname in resolved:
            continue

        _schema, table_name = _split_table_fullname(target_fullname)
        target_meta_table = _lookup_registered_platform_meta_table(
            data_source_uid=data_source_uid,
            physical_table_name=table_name,
            target_fullname=target_fullname,
            timeout=timeout,
        )
        resolved[target_fullname] = _target_meta_table_uid(target_meta_table)
    return resolved


def _foreign_key_target_fullnames(table: Any) -> set[str]:
    target_fullnames: set[str] = set()
    for foreign_key_constraint in _iter_foreign_key_constraints(table):
        elements = list(getattr(foreign_key_constraint, "elements", []) or [])
        for element in elements:
            target_fullname = _foreign_key_element_target_table(element)
            if target_fullname:
                target_fullnames.add(target_fullname)

    if target_fullnames:
        return target_fullnames

    for column in _iter_columns(table):
        for foreign_key in list(getattr(column, "foreign_keys", []) or []):
            target_table, _target_column = _foreign_key_target_parts(foreign_key)
            if target_table:
                target_fullnames.add(target_table)
    return target_fullnames


def _lookup_registered_platform_meta_table(
    *,
    data_source_uid: str,
    physical_table_name: str,
    target_fullname: str,
    timeout: int | float | tuple[float, float] | None,
) -> MetaTable:
    filters: dict[str, Any] = {
        "data_source__uid": data_source_uid,
        "physical_table_name": physical_table_name,
        "management_mode": "platform_managed",
    }

    matches = MetaTable.filter(timeout=timeout, **filters)
    if not matches:
        raise ValueError(
            "Could not resolve registered platform-managed MetaTable for foreign key "
            f"target {target_fullname!r}. Register the target table first, or pass an "
            "explicit target_meta_tables mapping."
        )
    if len(matches) > 1:
        raise ValueError(
            "Multiple registered platform-managed MetaTables matched foreign key target "
            f"{target_fullname!r} with filters {filters!r}. Pass an explicit "
            "target_meta_tables mapping."
        )

    return matches[0]


def _split_table_fullname(table_fullname: str) -> tuple[str | None, str]:
    if "." not in table_fullname:
        return None, table_fullname
    schema, table_name = table_fullname.rsplit(".", 1)
    return schema or None, table_name


def _target_table_fullname(target: Any) -> str:
    if isinstance(target, str):
        return target

    table = _resolve_table(target)
    fullname = getattr(table, "fullname", None)
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
        or getattr(model_or_table, "__name__", None)
        or _table_name(_resolve_table(model_or_table))
    )
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
    target_column = getattr(element, "column", None)
    target_table = getattr(target_column, "table", None)
    fullname = getattr(target_table, "fullname", None)
    if fullname:
        return str(fullname)
    target_fullname = getattr(element, "target_fullname", None)
    if target_fullname:
        target_table, _target_column = _split_target_fullname(str(target_fullname))
        return target_table
    return None


def _foreign_key_target_parts(foreign_key: Any) -> tuple[str | None, str | None]:
    target_fullname = getattr(foreign_key, "target_fullname", None) or getattr(
        foreign_key,
        "_colspec",
        None,
    )
    if target_fullname:
        return _split_target_fullname(str(target_fullname))
    target_column = getattr(foreign_key, "column", None)
    target_table = getattr(target_column, "table", None)
    target_table_fullname = getattr(target_table, "fullname", None)
    target_column_name = getattr(target_column, "name", None)
    return (
        str(target_table_fullname) if target_table_fullname else None,
        str(target_column_name) if target_column_name else None,
    )


def _split_target_fullname(target_fullname: str) -> tuple[str | None, str | None]:
    if "." not in target_fullname:
        return None, target_fullname or None
    target_table, target_column = target_fullname.rsplit(".", 1)
    return target_table or None, target_column or None


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
    target_meta_table_uid_by_fullname: Mapping[str, Any],
) -> MetaTableForeignKeyContract:
    if not getattr(foreign_key_constraint, "name", None):
        raise ValueError("MetaTable SQLAlchemy foreign keys must be explicitly named.")
    elements = list(getattr(foreign_key_constraint, "elements", []) or [])
    if not elements:
        raise ValueError(f"Foreign key {foreign_key_constraint.name!r} does not expose elements.")

    target_tables = {element.column.table.fullname for element in elements}
    if len(target_tables) != 1:
        raise ValueError("Composite foreign keys must target one table.")
    target_table_fullname = next(iter(target_tables))
    target_meta_table_uid = _lookup_target_meta_table_uid(
        target_table_fullname,
        target_meta_table_uid_by_fullname=target_meta_table_uid_by_fullname,
    )
    on_delete = getattr(elements[0], "ondelete", None) or getattr(
        foreign_key_constraint,
        "ondelete",
        None,
    )
    return MetaTableForeignKeyContract(
        name=str(foreign_key_constraint.name),
        source_columns=[str(element.parent.name) for element in elements],
        target_meta_table_uid=target_meta_table_uid,
        target_columns=[str(element.column.name) for element in elements],
        on_delete=str(on_delete or "restrict").lower(),
    )


def _source_table_foreign_key_contract(
    foreign_key_constraint: Any,
    *,
    target_meta_table_uid_by_fullname: Mapping[str, Any],
) -> MetaTableForeignKeyContract:
    elements = list(getattr(foreign_key_constraint, "elements", []) or [])
    if not elements:
        raise ValueError("DynamicTable SQLAlchemy foreign keys must expose elements.")

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
    on_delete = getattr(elements[0], "ondelete", None) or getattr(
        foreign_key_constraint,
        "ondelete",
        None,
    )
    return MetaTableForeignKeyContract(
        source_columns=[str(element.parent.name) for element in elements],
        target_meta_table_uid=target_meta_table_uid,
        target_columns=[str(element.column.name) for element in elements],
        on_delete=str(on_delete or "restrict").lower(),
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
    "PlatformManagedMetaTable",
    "PlatformTimeIndexMetaData",
    "external_registered_registration_request_from_sqlalchemy_model",
    "metatable_configured_tablename",
    "metatable_tablename",
    "platform_managed_registration_request_from_sqlalchemy_model",
    "register_external_sqlalchemy_model",
    "register_platform_managed_sqlalchemy_model",
    "table_contract_from_sqlalchemy_model",
    "time_indexed_registration_request_from_sqlalchemy_model",
]
