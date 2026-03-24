from __future__ import annotations

import datetime as dt
from enum import Enum
from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field, field_serializer, model_validator


class Logic(str, Enum):
    """Logical operator used by filter groups."""

    and_ = "and"
    or_ = "or"


class Op(str, Enum):
    """Operators supported by a single filter condition."""

    eq = "eq"
    ne = "ne"
    lt = "lt"
    lte = "lte"
    gt = "gt"
    gte = "gte"
    in_ = "in"
    not_in = "not_in"
    contains = "contains"
    starts_with = "starts_with"
    ends_with = "ends_with"
    is_null = "is_null"
    between = "between"


def _jsonify_value(v: Any) -> Any:
    """Convert datetime/date values, including nested containers, into JSON-friendly values."""

    if isinstance(v, (dt.datetime, dt.date)):
        return v.isoformat()
    if isinstance(v, (list, tuple)):
        return [_jsonify_value(x) for x in v]
    if isinstance(v, dict):
        return {str(k): _jsonify_value(val) for k, val in v.items()}
    return v


class Condition(BaseModel):
    """A single filter clause."""

    type: Literal["condition"] = "condition"
    field: str
    op: Op
    value: Any = None

    @model_validator(mode="after")
    def _shape_validate(self):
        if self.op in (Op.in_, Op.not_in) and not isinstance(self.value, (list, tuple)):
            raise ValueError(f"{self.op.value} requires a list value.")
        if self.op == Op.between and not (
            isinstance(self.value, (list, tuple)) and len(self.value) == 2
        ):
            raise ValueError("between requires a 2-item list [start, end].")
        return self

    @field_serializer("value", when_used="json")
    def _serialize_value(self, v: Any):
        return _jsonify_value(v)


class Group(BaseModel):
    """A boolean combination of filter expressions."""

    type: Literal["group"] = "group"
    logic: Logic = Logic.and_
    items: list[FilterExpr] = Field(min_length=1)


FilterExpr = Annotated[Condition | Group, Field(discriminator="type")]
Group.model_rebuild()


class JoinType(str, Enum):
    """Join type."""

    inner = "inner"
    left = "left"
    full = "full"


class JoinSpec(BaseModel):
    """Generic join declaration shared across TDAG filtering domains."""

    name: str
    node_unique_identifier: str | None = None
    on: list[str] | None = None
    storage_hash: str | None = None
    type: JoinType = JoinType.inner


BaseJoinSpec = JoinSpec


class BaseSearchRequest(BaseModel):
    """Generic storage-hash based search request wrapper."""

    storage_hash: str | None = None
    joins: list[JoinSpec] = Field(default_factory=list)
    filter: FilterExpr | None = None
    limit: int = Field(default=50, ge=1, le=500)
    offset: int = Field(default=0, ge=0)

    @model_validator(mode="after")
    def _validate_join_names(self):
        names = [j.name for j in self.joins]
        if len(names) != len(set(names)):
            raise ValueError("Join names must be unique within a request.")
        return self


def and_(*items: FilterExpr) -> Group:
    """Build an AND group."""

    return Group(logic=Logic.and_, items=list(items))


def or_(*items: FilterExpr) -> Group:
    """Build an OR group."""

    return Group(logic=Logic.or_, items=list(items))


class F:
    """Helper constructors that return Condition instances."""

    @staticmethod
    def eq(field: str, value: Any) -> Condition:
        return Condition(field=field, op=Op.eq, value=value)

    @staticmethod
    def ne(field: str, value: Any) -> Condition:
        return Condition(field=field, op=Op.ne, value=value)

    @staticmethod
    def lt(field: str, value: Any) -> Condition:
        return Condition(field=field, op=Op.lt, value=value)

    @staticmethod
    def lte(field: str, value: Any) -> Condition:
        return Condition(field=field, op=Op.lte, value=value)

    @staticmethod
    def gt(field: str, value: Any) -> Condition:
        return Condition(field=field, op=Op.gt, value=value)

    @staticmethod
    def gte(field: str, value: Any) -> Condition:
        return Condition(field=field, op=Op.gte, value=value)

    @staticmethod
    def in_(field: str, values: list[Any]) -> Condition:
        return Condition(field=field, op=Op.in_, value=values)

    @staticmethod
    def not_in(field: str, values: list[Any]) -> Condition:
        return Condition(field=field, op=Op.not_in, value=values)

    @staticmethod
    def contains(field: str, value: str) -> Condition:
        return Condition(field=field, op=Op.contains, value=value)

    @staticmethod
    def starts_with(field: str, value: str) -> Condition:
        return Condition(field=field, op=Op.starts_with, value=value)

    @staticmethod
    def ends_with(field: str, value: str) -> Condition:
        return Condition(field=field, op=Op.ends_with, value=value)

    @staticmethod
    def is_null(field: str, value: bool | None = True) -> Condition:
        return Condition(field=field, op=Op.is_null, value=value)

    @staticmethod
    def between(field: str, start: Any, end: Any) -> Condition:
        return Condition(field=field, op=Op.between, value=[start, end])


__all__ = [
    "BaseJoinSpec",
    "BaseSearchRequest",
    "Condition",
    "F",
    "FilterExpr",
    "Group",
    "JoinSpec",
    "JoinType",
    "Logic",
    "Op",
    "and_",
    "or_",
]
