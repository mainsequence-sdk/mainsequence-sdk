"""Public simple-table model surface.

This module keeps the declarative table model API compact:

- schema declarations and normalized schema types live in ``schema.py``
- filter/query helpers live in ``filters.py``
- row serialization/validation helpers live on ``SimpleTable`` itself

`SimpleTable` composes those layers into the public user-facing base class while
preserving the existing import surface from `mainsequence.tdag.simple_tables`.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict

from .filters import (
    Condition,
    F,
    FieldRef,
    FilterExpr,
    Group,
    JoinHandle,
    JoinKey,
    JoinSpec,
    JoinType,
    Logic,
    Op,
    SearchRequest,
    SimpleTableQueryMixin,
    TableRef,
    and_,
    classproperty,
    or_,
)
from .schema import (
    ForeignKey,
    Index,
    Ops,
    SimpleTableSchemaMixin,
    TableFieldSpec,
    TableSchema,
)


class SimpleTable(SimpleTableSchemaMixin, SimpleTableQueryMixin, BaseModel):
    """Base class for user-facing logical table declarations."""

    model_config = ConfigDict(extra="forbid", ignored_types=(classproperty,))

    @classmethod
    def serialize_record_payload(
        cls,
        record: SimpleTable | dict[str, Any] | BaseModel,
    ) -> dict[str, Any]:
        if isinstance(record, cls):
            return record.model_dump(mode="python")
        if isinstance(record, BaseModel):
            return record.model_dump(mode="python")
        if isinstance(record, dict):
            return record
        raise TypeError(f"Unsupported record type for {cls.__name__}: {type(record)!r}")

    @classmethod
    def validate_record_response_payload(cls, payload: Any) -> list[SimpleTable]:
        if isinstance(payload, dict):
            if isinstance(payload.get("results"), list):
                payload = payload["results"]
            elif isinstance(payload.get("records"), list):
                payload = payload["records"]
            else:
                payload = [payload]

        if not isinstance(payload, list):
            raise TypeError(
                f"Expected list or dict response for {cls.__name__} records, got {type(payload)!r}."
            )

        return [cls.model_validate(item) for item in payload]


__all__ = [
    "Condition",
    "F",
    "FieldRef",
    "FilterExpr",
    "ForeignKey",
    "Group",
    "Index",
    "JoinHandle",
    "JoinKey",
    "JoinSpec",
    "JoinType",
    "Logic",
    "Op",
    "Ops",
    "SearchRequest",
    "SimpleTable",
    "TableFieldSpec",
    "TableRef",
    "TableSchema",
    "and_",
    "or_",
]
