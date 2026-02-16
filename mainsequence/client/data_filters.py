# data_filters.py
"""
Filter and join request payload models.

Contents
--------
- Logic, Op: enums for boolean combinations and operators
- Condition, Group: filter expression objects
- JoinType, JoinKey, JoinSpec: join declarations
- SearchRequest: request wrapper:
    - base selector: exactly one of storage_hash or node_unique_identifier
    - joins: list of JoinSpec
    - filter: optional filter expression
    - limit/offset: pagination controls (server may override)

Datetime values
---------------
Condition.value may include datetime/date objects. When serialized with
`model_dump(mode="json")`, datetime/date values are converted to ISO-8601 strings.

Field naming
------------
- Base fields: "time_index", "unique_identifier"
- Join fields use the join name prefix:
    "<join_name>.time_index"
    "<join_name>.unique_identifier"

Example: join daily_prices with fundamentals on time_index + unique_identifier
-----------------------------------------------------------------------------

    >>> import datetime as dt
    >>> from mainsequence.client.data_filters import SearchRequest, JoinSpec, JoinType, JoinKey, F, and_
    >>> req = SearchRequest(
    ...     node_unique_identifier="daily_prices",
    ...     joins=[
    ...         JoinSpec(
    ...             name="fundamentals",
    ...             node_unique_identifier="fundamentals",
    ...             type=JoinType.inner,
    ...             on=[JoinKey.time_index, JoinKey.unique_identifier],
    ...         )
    ...     ],
    ...     filter=and_(
    ...         F.between(
    ...             "time_index",
    ...             dt.datetime(2026, 2, 1, tzinfo=dt.timezone.utc),
    ...             dt.datetime(2026, 2, 15, tzinfo=dt.timezone.utc),
    ...         ),
    ...         F.eq("unique_identifier", "BBG000BLNNH6"),
    ...     ),
    ...     limit=500,
    ...     offset=0,
    ... )
    >>> payload = req.model_dump(mode="json")
    >>> payload["node_unique_identifier"]
    'daily_prices'
    >>> payload["joins"][0]["name"]
    'fundamentals'
    >>> payload["joins"][0]["on"]
    ['time_index', 'unique_identifier']
"""

from __future__ import annotations

import datetime as dt
from enum import Enum
from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field, field_serializer, model_validator


class Logic(str, Enum):
    """Logical operator used by Group."""
    and_ = "and"
    or_ = "or"


class Op(str, Enum):
    """
    Operators supported by Condition.

    Shape rules
    ----------
    - between: value must be [start, end]
    - in / not_in: value must be a list
    - is_null: value should be true/false (or None meaning true)
    """
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
    """Convert datetime/date (including nested lists/tuples/dicts) into JSON-friendly values."""
    if isinstance(v, (dt.datetime, dt.date)):
        return v.isoformat()
    if isinstance(v, (list, tuple)):
        return [_jsonify_value(x) for x in v]
    if isinstance(v, dict):
        return {str(k): _jsonify_value(val) for k, val in v.items()}
    return v


class Condition(BaseModel):
    """
    A single filter clause.

    Fields
    ------
    field:
        Public field name (e.g. "time_index", "unique_identifier",
        or "<join_name>.unique_identifier").
    op:
        Operator (e.g. "eq", "between", "in").
    value:
        Operator-specific value. May include datetime objects.
    """
    type: Literal["condition"] = "condition"
    field: str
    op: Op
    value: Any = None

    @model_validator(mode="after")
    def _shape_validate(self):
        if self.op in (Op.in_, Op.not_in):
            if not isinstance(self.value, (list, tuple)):
                raise ValueError(f"{self.op.value} requires a list value.")
        if self.op == Op.between:
            if not (isinstance(self.value, (list, tuple)) and len(self.value) == 2):
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


class JoinKey(str, Enum):
    """Allowed join keys."""
    time_index = "time_index"
    unique_identifier = "unique_identifier"


class JoinSpec(BaseModel):
    """
    Join declaration.

    Fields
    ------
    name:
        Join label used for field prefixes and output naming (e.g. "fundamentals").
    storage_hash / node_unique_identifier:
        Exactly one must be set to identify the join target.
    type:
        Join type ("inner" or "left").
    on:
        Join keys. Must be exactly ["time_index", "unique_identifier"] for this version.
    """
    name: str
    storage_hash: str | None = None
    node_unique_identifier: str | None = None
    type: JoinType = JoinType.inner
    on: list[JoinKey] = Field(default_factory=lambda: [JoinKey.time_index, JoinKey.unique_identifier])

    @model_validator(mode="after")
    def _xor_target(self):
        a = bool(self.storage_hash and self.storage_hash.strip())
        b = bool(self.node_unique_identifier and self.node_unique_identifier.strip())
        if a == b:
            raise ValueError("JoinSpec requires exactly one of storage_hash or node_unique_identifier.")
        # enforce exactly the two allowed join keys for now
        if set(self.on) != {JoinKey.time_index, JoinKey.unique_identifier}:
            raise ValueError("JoinSpec.on must be exactly ['time_index', 'unique_identifier'].")
        return self


class SearchRequest(BaseModel):
    """
    Request wrapper.

    Exactly one of:
    - storage_hash
    - node_unique_identifier
    must be set.
    """
    storage_hash: str | None = None
    node_unique_identifier: str | None = None

    joins: list[JoinSpec] = Field(default_factory=list)
    filter: FilterExpr | None = None
    limit: int = Field(default=50, ge=1, le=500)
    offset: int = Field(default=0, ge=0)

    @model_validator(mode="after")
    def _xor_target_and_unique_join_names(self):
        a = bool(self.storage_hash and self.storage_hash.strip())
        b = bool(self.node_unique_identifier and self.node_unique_identifier.strip())
        if a == b:
            raise ValueError("Exactly one of storage_hash or node_unique_identifier must be provided.")

        names = [j.name for j in self.joins]
        if len(names) != len(set(names)):
            raise ValueError("Join names must be unique within a request.")
        return self


def and_(*items: FilterExpr) -> Group:
    """Build an AND Group."""
    return Group(logic=Logic.and_, items=list(items))


def or_(*items: FilterExpr) -> Group:
    """Build an OR Group."""
    return Group(logic=Logic.or_, items=list(items))


class F:
    """Helper constructors that return Condition."""

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
