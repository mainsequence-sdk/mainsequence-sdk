from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from mainsequence.client.models_metatables import (
    MetaTable,
    MetaTableColumnContract,
    MetaTableContract,
    MetaTableForeignKeyContract,
    MetaTableIndexContract,
    MetaTablePhysicalContract,
    MetaTableRegistrationRequest,
)

from .hashing import build_meta_table_storage_hash

DEFAULT_PLATFORM_MANAGED_PROVISIONING = {
    "create_table": True,
    "if_not_exists": True,
}


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


def table_contract_from_sqlalchemy_model(
    model_or_table: Any,
    *,
    table_model_module: str | None = None,
    table_model_qualname: str | None = None,
    target_meta_table_uid_by_fullname: Mapping[str, Any] | None = None,
    schema: str | None = None,
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
            schema=resolved_schema,
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


def platform_managed_registration_request_from_sqlalchemy_model(
    model_or_table: Any,
    *,
    data_source_uid: str,
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
    table_name = _table_name(table)
    if enforce_storage_hash_name and table_name != expected_storage_hash:
        raise ValueError(
            "Platform-managed SQLAlchemy tables must use the MetaTable storage hash as "
            "their physical table name. Use metatable_tablename(...) for __tablename__. "
            f"Expected {expected_storage_hash!r}, got {table_name!r}."
        )

    table_contract = table_contract_from_sqlalchemy_model(
        model_or_table,
        target_meta_table_uid_by_fullname=target_meta_table_uid_by_fullname,
        schema=resolved_schema,
    )
    return MetaTableRegistrationRequest(
        data_source_uid=str(data_source_uid),
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
    data_source_uid: str,
    timeout: int | float | tuple[float, float] | None = None,
    **kwargs: Any,
) -> MetaTable:
    request = platform_managed_registration_request_from_sqlalchemy_model(
        model_or_table,
        data_source_uid=data_source_uid,
        **kwargs,
    )
    return MetaTable.register(request, timeout=timeout)


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
        raise ValueError("MetaTable SQLAlchemy contracts require a physical schema.")
    return str(resolved_schema)


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
    table = _resolve_table(model_or_table)
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
    return MetaTableColumnContract(
        name=str(column.name),
        data_type=type_contract["data_type"],
        backend_type=type_contract.get("backend_type"),
        nullable=bool(getattr(column, "nullable", True)),
        primary_key=bool(getattr(column, "primary_key", False)),
        unique=bool(getattr(column, "unique", False)),
        description=info.get("description"),
        label=info.get("label"),
        logical_name=info.get("logical_name"),
    )


def _column_type_contract(column: Any) -> dict[str, str]:
    column_type = getattr(column, "type", None)
    if column_type is None:
        raise ValueError(f"Column {column.name!r} does not expose a SQLAlchemy type.")

    type_name = type(column_type).__name__.lower()
    backend_type = str(column_type).upper()
    data_type = _logical_data_type(type_name=type_name, backend_type=backend_type)
    return {
        "data_type": data_type,
        "backend_type": backend_type or data_type,
    }


def _logical_data_type(*, type_name: str, backend_type: str) -> str:
    normalized_backend = backend_type.lower()
    if type_name in {"uuid", "uuidtype", "uuid_", "postgresqluuid"} or normalized_backend == "uuid":
        return "uuid"
    if type_name in {"string", "unicode", "text", "varchar", "char"}:
        return "str"
    if "varchar" in normalized_backend or normalized_backend in {"text", "char"}:
        return "str"
    if type_name in {"integer", "biginteger"}:
        return "int64"
    if type_name in {"smallinteger"}:
        return "int16"
    if normalized_backend in {"integer", "int", "int4"}:
        return "int32"
    if normalized_backend in {"bigint", "int8"}:
        return "int64"
    if normalized_backend in {"smallint", "int2"}:
        return "int16"
    if type_name in {"float", "double", "doubleprecision"}:
        return "float64"
    if normalized_backend in {"real", "float4"}:
        return "float32"
    if "double" in normalized_backend or normalized_backend in {"float", "float8"}:
        return "float64"
    if type_name in {"numeric", "decimal"} or normalized_backend.startswith("numeric"):
        return "numeric"
    if type_name in {"boolean"} or normalized_backend in {"bool", "boolean"}:
        return "bool"
    if type_name in {"datetime"} or "timestamp" in normalized_backend:
        return "datetime"
    if type_name in {"date"} or normalized_backend == "date":
        return "date"
    if type_name in {"json", "jsonb"} or normalized_backend in {"json", "jsonb"}:
        return "jsonb" if "jsonb" in normalized_backend or type_name == "jsonb" else "json"
    raise ValueError(f"Unsupported SQLAlchemy column type {backend_type!r}.")


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
        str(column.name)
        for column in list(columns or [])
        if getattr(column, "name", None) not in (None, "")
    ]


__all__ = [
    "DEFAULT_PLATFORM_MANAGED_PROVISIONING",
    "external_registered_registration_request_from_sqlalchemy_model",
    "metatable_tablename",
    "platform_managed_registration_request_from_sqlalchemy_model",
    "register_external_sqlalchemy_model",
    "register_platform_managed_sqlalchemy_model",
    "table_contract_from_sqlalchemy_model",
]
