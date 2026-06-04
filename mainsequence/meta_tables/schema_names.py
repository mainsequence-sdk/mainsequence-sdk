from __future__ import annotations

import hashlib
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from .hashing import POSTGRES_IDENTIFIER_MAX_LENGTH, slugify_identifier

_IDENTIFIER_HASH_LENGTH = 10
_PART_SEPARATOR = "__"


@dataclass(frozen=True)
class SchemaTableNameParts:
    app: str
    concept: str
    suffix: str | None = None


def normalize_identifier_part(value: str, *, field_name: str = "identifier part") -> str:
    raw_value = str(value).strip()
    if not raw_value:
        raise ValueError(f"Schema {field_name} is required.")
    if not any(character.isalnum() for character in raw_value):
        raise ValueError(f"Schema {field_name} must contain letters or digits.")
    return slugify_identifier(raw_value)


def bounded_identifier(
    *parts: str,
    max_length: int = POSTGRES_IDENTIFIER_MAX_LENGTH,
) -> str:
    normalized_parts = [
        _normalize_identifier_payload(part, field_name="identifier part") for part in parts
    ]
    candidate = _PART_SEPARATOR.join(normalized_parts)
    if len(candidate) <= max_length:
        return candidate

    digest = hashlib.md5(candidate.encode()).hexdigest()[:_IDENTIFIER_HASH_LENGTH]
    digest_marker = f"_{digest}"
    head_budget = max_length - len(digest_marker)
    if head_budget < 1:
        raise ValueError(f"Identifier max_length is too small: {max_length!r}.")

    head = candidate[:head_budget].rstrip("_") or candidate[:1]
    return f"{head}{digest_marker}"


def schema_table_name(
    app: str,
    concept: str,
    suffix: str | None = None,
) -> str:
    """Return a PostgreSQL-safe table name using the ``app__concept`` convention."""

    app_name = normalize_identifier_part(app, field_name="table name app")
    concept_name = normalize_identifier_part(concept, field_name="table name concept")
    suffix_name = None
    if suffix is not None:
        suffix_name = normalize_identifier_part(suffix, field_name="table name suffix")

    if suffix_name is None:
        candidate = f"{app_name}{_PART_SEPARATOR}{concept_name}"
    else:
        candidate = f"{app_name}{_PART_SEPARATOR}{concept_name}{_PART_SEPARATOR}{suffix_name}"

    if len(candidate) <= POSTGRES_IDENTIFIER_MAX_LENGTH:
        return candidate
    return _truncate_table_name(
        app=app_name,
        concept=concept_name,
        suffix=suffix_name,
        candidate=candidate,
    )


def parse_schema_table_name(table_name: str) -> SchemaTableNameParts:
    raw_table_name = str(table_name).strip()
    if not raw_table_name:
        raise ValueError("Schema table name is required.")
    parts = [
        normalize_identifier_part(part, field_name="table name part")
        for part in raw_table_name.split(_PART_SEPARATOR)
    ]
    if len(parts) < 2:
        raise ValueError("Schema table names must use the app__concept convention.")

    suffix = _PART_SEPARATOR.join(parts[2:]) if len(parts) > 2 else None
    return SchemaTableNameParts(
        app=parts[0],
        concept=parts[1],
        suffix=suffix or None,
    )


def schema_index_name(
    table_name: str,
    columns: Sequence[str],
    *,
    unique: bool = False,
) -> str:
    prefix = "uix" if unique else "ix"
    return bounded_identifier(prefix, table_name, *_required_columns(columns))


def schema_foreign_key_name(
    table_name: str,
    columns: Sequence[str],
    target_table: str,
    target_columns: Sequence[str] = (),
) -> str:
    return bounded_identifier(
        "fk",
        table_name,
        *_required_columns(columns),
        target_table,
        *_required_columns(target_columns, required=False),
    )


def schema_primary_key_name(table_name: str) -> str:
    return bounded_identifier("pk", table_name)


def schema_unique_constraint_name(table_name: str, columns: Sequence[str]) -> str:
    return bounded_identifier("uq", table_name, *_required_columns(columns))


def schema_check_constraint_name(table_name: str, constraint_name: str | None = None) -> str:
    if constraint_name:
        return bounded_identifier("ck", table_name, constraint_name)
    return bounded_identifier("ck", table_name)


def sqlalchemy_naming_convention() -> dict[str, Any]:
    return {
        "schema_index_name": _sqlalchemy_index_name,
        "schema_foreign_key_name": _sqlalchemy_fk_name,
        "schema_primary_key_name": _sqlalchemy_pk_name,
        "schema_unique_constraint_name": _sqlalchemy_uq_name,
        "schema_check_constraint_name": _sqlalchemy_ck_name,
        "ix": "%(schema_index_name)s",
        "fk": "%(schema_foreign_key_name)s",
        "pk": "%(schema_primary_key_name)s",
        "uq": "%(schema_unique_constraint_name)s",
        "ck": "%(schema_check_constraint_name)s",
    }


def _truncate_table_name(
    *,
    app: str,
    concept: str,
    suffix: str | None,
    candidate: str,
) -> str:
    digest = hashlib.md5(candidate.encode()).hexdigest()[:_IDENTIFIER_HASH_LENGTH]
    digest_marker = f"_{digest}"
    app_prefix = f"{app}{_PART_SEPARATOR}"

    if suffix is None:
        concept_budget = POSTGRES_IDENTIFIER_MAX_LENGTH - len(app_prefix) - len(digest_marker)
        if concept_budget < 1:
            raise ValueError(f"Schema table name app is too long: {app!r}.")
        concept_part = concept[:concept_budget].rstrip("_") or concept[:1]
        return f"{app_prefix}{concept_part}{digest_marker}"

    suffix_separator = _PART_SEPARATOR
    remaining = (
        POSTGRES_IDENTIFIER_MAX_LENGTH
        - len(app_prefix)
        - len(digest_marker)
        - len(suffix_separator)
    )
    if remaining < 2:
        raise ValueError(f"Schema table name app is too long: {app!r}.")

    suffix_budget = min(len(suffix), max(1, remaining // 2))
    concept_budget = remaining - suffix_budget

    if concept_budget > len(concept):
        suffix_budget = min(len(suffix), suffix_budget + concept_budget - len(concept))
        concept_budget = len(concept)
    if suffix_budget > len(suffix):
        concept_budget = min(len(concept), concept_budget + suffix_budget - len(suffix))
        suffix_budget = len(suffix)

    concept_part = concept[:concept_budget].rstrip("_") or concept[:1]
    suffix_part = suffix[-suffix_budget:].strip("_") or suffix[-1]
    return f"{app_prefix}{concept_part}{digest_marker}{suffix_separator}{suffix_part}"


def _required_columns(
    columns: Sequence[str],
    *,
    required: bool = True,
) -> list[str]:
    if required and not columns:
        raise ValueError("At least one column is required for schema names.")
    return [normalize_identifier_part(column, field_name="column name") for column in columns]


def _normalize_identifier_payload(value: str, *, field_name: str) -> str:
    raw_value = str(value).strip()
    if not raw_value:
        raise ValueError(f"Schema {field_name} is required.")
    return _PART_SEPARATOR.join(
        normalize_identifier_part(part, field_name=field_name)
        for part in raw_value.split(_PART_SEPARATOR)
    )


def _sqlalchemy_index_name(index: Any, table: Any) -> str:
    return schema_index_name(
        str(table.name),
        _sqlalchemy_expression_names(index),
        unique=bool(getattr(index, "unique", False)),
    )


def _sqlalchemy_fk_name(constraint: Any, table: Any) -> str:
    target_table, target_columns = _foreign_key_target(constraint)
    return schema_foreign_key_name(
        str(table.name),
        _sqlalchemy_constraint_column_names(constraint),
        target_table,
        target_columns,
    )


def _sqlalchemy_pk_name(constraint: Any, table: Any) -> str:
    return schema_primary_key_name(str(table.name))


def _sqlalchemy_uq_name(constraint: Any, table: Any) -> str:
    return schema_unique_constraint_name(
        str(table.name),
        _sqlalchemy_constraint_column_names(constraint),
    )


def _sqlalchemy_ck_name(constraint: Any, table: Any) -> str:
    return schema_check_constraint_name(str(table.name), getattr(constraint, "name", None))


def _sqlalchemy_constraint_column_names(constraint: Any) -> list[str]:
    return [str(column.name) for column in constraint.columns]


def _sqlalchemy_expression_names(index: Any) -> list[str]:
    names: list[str] = []
    for expression in index.expressions:
        name = getattr(expression, "name", None)
        if name in (None, ""):
            name = str(expression)
        names.append(str(name))
    return names


def _foreign_key_target(constraint: Any) -> tuple[str, list[str]]:
    elements = list(constraint.elements)
    if not elements:
        raise ValueError("Foreign-key naming requires at least one target element.")

    target_columns: list[str] = []
    target_table = None
    for element in elements:
        try:
            target_column = element.column
        except Exception:
            target_column = None

        if target_column is not None:
            target_columns.append(str(target_column.name))
            target_table = str(target_column.table.name)
            continue

        target_fullname = str(element.target_fullname)
        target_parts = target_fullname.split(".")
        if len(target_parts) >= 2:
            target_table = target_parts[-2]
            target_columns.append(target_parts[-1])

    if target_table in (None, ""):
        raise ValueError("Foreign-key naming requires a target table.")

    return target_table, target_columns


__all__ = [
    "SchemaTableNameParts",
    "bounded_identifier",
    "normalize_identifier_part",
    "parse_schema_table_name",
    "schema_check_constraint_name",
    "schema_foreign_key_name",
    "schema_index_name",
    "schema_primary_key_name",
    "schema_table_name",
    "schema_unique_constraint_name",
    "sqlalchemy_naming_convention",
]
