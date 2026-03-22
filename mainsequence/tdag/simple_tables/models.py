"""Public simple-table model surface.

This module now keeps the declarative table model API compact:

- schema declarations and normalized schema types live in ``schema.py``
- filter/query helpers live in ``filters.py``
- client CRUD methods live in ``mainsequence.client.models_simple_tables``

`SimpleTable` composes those layers into the public user-facing base class while
preserving the existing import surface from `mainsequence.tdag.simple_tables`.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict

from mainsequence.client.models_simple_tables import SimpleTableClientMixin

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


class SimpleTable(SimpleTableClientMixin, SimpleTableSchemaMixin, SimpleTableQueryMixin, BaseModel):
    """Base class for user-facing logical table declarations."""

    model_config = ConfigDict(extra="forbid", ignored_types=(classproperty,))


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
