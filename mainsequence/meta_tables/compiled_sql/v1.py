from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from sqlalchemy.dialects import postgresql

from mainsequence.client.dtype_codec import DATE, TIMESTAMP_TZ, sqlalchemy_type_to_token
from mainsequence.client.metatables import (
    COMPILED_SQL_V1,
    MetaTableCompiledSQLDialect,
    MetaTableCompiledSQLOperation,
    MetaTableCompiledSQLParamstyle,
    MetaTableOperation,
    MetaTableOperationLimits,
    MetaTableOperationScope,
    MetaTableOperationScopeTable,
    MetaTableStatementPayload,
)


def build_operation(
    *,
    operation: MetaTableOperation,
    sql: str,
    parameters: Mapping[str, Any] | Sequence[Any] | None = None,
    parameter_types: Mapping[str, str] | None = None,
    scope: MetaTableOperationScope | Mapping[str, Any],
    dialect: MetaTableCompiledSQLDialect = "postgresql",
    paramstyle: MetaTableCompiledSQLParamstyle = "pyformat",
    limits: MetaTableOperationLimits | Mapping[str, Any] | None = None,
) -> MetaTableCompiledSQLOperation:
    """
    Build and validate the TS Manager compiled-sql.v1 operation contract.

    This is the client-side protocol object. It is intentionally plain SQL plus
    bound parameters, an explicit execution data source, and declared MetaTable
    scope, not a serialized SQLAlchemy object.
    """

    if parameters is None:
        statement_parameters: dict[str, Any] | list[Any] = {}
    elif isinstance(parameters, Mapping):
        statement_parameters = dict(parameters)
    else:
        statement_parameters = list(parameters)

    return MetaTableCompiledSQLOperation(
        operation=operation,
        version=COMPILED_SQL_V1,
        dialect=dialect,
        statement=MetaTableStatementPayload(
            sql=sql,
            parameters=statement_parameters,
            parameter_types=dict(parameter_types) if parameter_types is not None else None,
            paramstyle=paramstyle,
        ),
        scope=(
            scope
            if isinstance(scope, MetaTableOperationScope)
            else MetaTableOperationScope(**scope)
        ),
        limits=(
            limits
            if limits is None or isinstance(limits, MetaTableOperationLimits)
            else MetaTableOperationLimits(**limits)
        ),
    )


def compile_sqlalchemy_statement(
    statement: Any,
    *,
    operation: MetaTableOperation,
    data_source_uid: str | None = None,
    scope_tables: Sequence[MetaTableOperationScopeTable | Mapping[str, Any]],
    limits: MetaTableOperationLimits | Mapping[str, Any] | None = None,
    dialect: MetaTableCompiledSQLDialect = "postgresql",
    paramstyle: MetaTableCompiledSQLParamstyle = "pyformat",
    compile_kwargs: Mapping[str, Any] | None = None,
) -> MetaTableCompiledSQLOperation:
    """
    Compile a SQLAlchemy/Core statement into the TS Manager compiled-sql.v1 payload.

    ``data_source_uid`` selects the DynamicTableDataSource connection. If it is
    omitted, the SDK resolves the configured project/session default data source.
    ``scope_tables`` remains the declared MetaTable permission scope for the
    operation.
    """

    if dialect != "postgresql":
        raise ValueError("Only the postgresql compiled-sql.v1 dialect is supported.")
    if paramstyle != "pyformat":
        raise ValueError("Only pyformat compiled-sql.v1 parameters are supported.")

    resolved_compile_kwargs = {"render_postcompile": True}
    if compile_kwargs:
        resolved_compile_kwargs.update(dict(compile_kwargs))

    compiled = statement.compile(
        dialect=postgresql.dialect(paramstyle=paramstyle),
        compile_kwargs=resolved_compile_kwargs,
    )
    parameter_types = _compiled_sqlalchemy_parameter_types(compiled)
    scope = MetaTableOperationScope(
        data_source_uid=data_source_uid,
        tables=[
            (
                table
                if isinstance(table, MetaTableOperationScopeTable)
                else MetaTableOperationScopeTable(**table)
            )
            for table in scope_tables
        ]
    )
    return build_operation(
        operation=operation,
        sql=str(compiled),
        parameters=dict(compiled.params),
        parameter_types=parameter_types,
        scope=scope,
        dialect=dialect,
        paramstyle=paramstyle,
        limits=limits,
    )


def _compiled_sqlalchemy_parameter_types(compiled: Any) -> dict[str, str]:
    parameter_types: dict[str, str] = {}
    bind_names = getattr(compiled, "bind_names", {}) or {}
    parameters = getattr(compiled, "params", {}) or {}
    for bind_parameter, rendered_name in bind_names.items():
        column_type = getattr(bind_parameter, "type", None)
        if column_type is None:
            continue
        token = sqlalchemy_type_to_token(column_type, remote=True)
        if token in {DATE, TIMESTAMP_TZ}:
            for parameter_name in _rendered_sqlalchemy_parameter_names(
                rendered_name,
                parameters,
            ):
                parameter_types[parameter_name] = token
    return parameter_types


def _rendered_sqlalchemy_parameter_names(
    rendered_name: Any,
    parameters: Mapping[str, Any],
) -> list[str]:
    rendered_name = str(rendered_name)
    if rendered_name in parameters:
        return [rendered_name]

    expanded_prefix = f"{rendered_name}_"
    return [
        str(parameter_name)
        for parameter_name in parameters
        if str(parameter_name).startswith(expanded_prefix)
    ]


__all__ = [
    "build_operation",
    "compile_sqlalchemy_statement",
]
