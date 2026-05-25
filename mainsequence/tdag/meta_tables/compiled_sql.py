from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from mainsequence.client.models_metatables import (
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


def build_compiled_sql_v1_operation(
    *,
    operation: MetaTableOperation,
    sql: str,
    parameters: Mapping[str, Any] | Sequence[Any] | None = None,
    scope: MetaTableOperationScope | Mapping[str, Any],
    dialect: MetaTableCompiledSQLDialect = "postgresql",
    paramstyle: MetaTableCompiledSQLParamstyle = "pyformat",
    limits: MetaTableOperationLimits | Mapping[str, Any] | None = None,
) -> MetaTableCompiledSQLOperation:
    """
    Build and validate the TS Manager compiled-sql.v1 operation contract.

    This is the client-side protocol object. It is intentionally plain SQL plus
    bound parameters and declared MetaTable scope, not a serialized SQLAlchemy
    object.
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
    scope_tables: Sequence[MetaTableOperationScopeTable | Mapping[str, Any]],
    limits: MetaTableOperationLimits | Mapping[str, Any] | None = None,
    dialect: MetaTableCompiledSQLDialect = "postgresql",
    paramstyle: MetaTableCompiledSQLParamstyle = "pyformat",
    compile_kwargs: Mapping[str, Any] | None = None,
) -> MetaTableCompiledSQLOperation:
    """
    Compile a SQLAlchemy/Core statement into the TS Manager compiled-sql.v1 payload.

    SQLAlchemy is an optional caller-side dependency. The SDK imports it only
    when this helper is used.
    """

    if dialect != "postgresql":
        raise ValueError("Only the postgresql compiled-sql.v1 dialect is supported.")
    if paramstyle != "pyformat":
        raise ValueError("Only pyformat compiled-sql.v1 parameters are supported.")

    try:
        from sqlalchemy.dialects import postgresql
    except ImportError as exc:
        raise ImportError(
            "compile_sqlalchemy_statement requires SQLAlchemy to be installed in the "
            "client application environment."
        ) from exc

    resolved_compile_kwargs = {"render_postcompile": True}
    if compile_kwargs:
        resolved_compile_kwargs.update(dict(compile_kwargs))

    compiled = statement.compile(
        dialect=postgresql.dialect(paramstyle=paramstyle),
        compile_kwargs=resolved_compile_kwargs,
    )
    scope = MetaTableOperationScope(
        tables=[
            (
                table
                if isinstance(table, MetaTableOperationScopeTable)
                else MetaTableOperationScopeTable(**table)
            )
            for table in scope_tables
        ]
    )
    return build_compiled_sql_v1_operation(
        operation=operation,
        sql=str(compiled),
        parameters=dict(compiled.params),
        scope=scope,
        dialect=dialect,
        paramstyle=paramstyle,
        limits=limits,
    )


__all__ = [
    "build_compiled_sql_v1_operation",
    "compile_sqlalchemy_statement",
]
