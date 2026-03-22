from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from mainsequence.client.data_filters import (
    Condition,
    F,
    FilterExpr,
    Group,
    JoinKey,
    JoinSpec,
    JoinType,
    Logic,
    Op,
    SearchRequest,
    and_,
    or_,
)

from .schema import TableFieldSpec

if TYPE_CHECKING:
    from .models import SimpleTable


class classproperty:  # noqa: N801
    def __init__(self, func):
        self.func = func

    def __get__(self, instance, owner):
        return self.func(owner)


@dataclass(frozen=True)
class FieldRef:
    table: type[SimpleTable]
    spec: TableFieldSpec
    qualifier: str | None = None

    @property
    def name(self) -> str:
        if self.qualifier:
            return f"{self.qualifier}.{self.spec.name}"
        return self.spec.name

    def _ensure_filterable(self) -> None:
        if not self.spec.ops.filter:
            raise ValueError(
                f"Field '{self.spec.name}' on {self.table.__name__} is not allowed "
                "in declarative backend filters (Ops.filter=False)."
            )

    def _condition(self, op: Op, value: Any = None) -> Condition:
        self._ensure_filterable()
        return Condition(field=self.name, op=op, value=value)

    def eq(self, value: Any) -> Condition:
        return self._condition(Op.eq, value)

    def ne(self, value: Any) -> Condition:
        return self._condition(Op.ne, value)

    def lt(self, value: Any) -> Condition:
        return self._condition(Op.lt, value)

    def lte(self, value: Any) -> Condition:
        return self._condition(Op.lte, value)

    def gt(self, value: Any) -> Condition:
        return self._condition(Op.gt, value)

    def gte(self, value: Any) -> Condition:
        return self._condition(Op.gte, value)

    def in_(self, values: Sequence[Any]) -> Condition:
        return self._condition(Op.in_, list(values))

    def not_in(self, values: Sequence[Any]) -> Condition:
        return self._condition(Op.not_in, list(values))

    def contains(self, value: str) -> Condition:
        return self._condition(Op.contains, value)

    def starts_with(self, value: str) -> Condition:
        return self._condition(Op.starts_with, value)

    def ends_with(self, value: str) -> Condition:
        return self._condition(Op.ends_with, value)

    def is_null(self, value: bool | None = True) -> Condition:
        return self._condition(Op.is_null, value)

    def between(self, start: Any, end: Any) -> Condition:
        return self._condition(Op.between, [start, end])

    def order_key(self, *, descending: bool = False) -> str:
        if not self.spec.ops.order:
            raise ValueError(
                f"Field '{self.spec.name}' on {self.table.__name__} is not allowed "
                "for ordering (Ops.order=False)."
            )
        return f"-{self.name}" if descending else self.name


class FieldNamespace:
    def __init__(self, table: type[SimpleTable], qualifier: str | None = None):
        self._table = table
        self._qualifier = qualifier

    def __getattr__(self, field_name: str) -> FieldRef:
        if field_name.startswith("_"):
            raise AttributeError(field_name)
        spec = self._table.field_spec(field_name)
        return FieldRef(table=self._table, spec=spec, qualifier=self._qualifier)

    def __getitem__(self, field_name: str) -> FieldRef:
        return getattr(self, field_name)

    def __dir__(self) -> list[str]:
        return sorted(self._table.model_fields.keys())


@dataclass(frozen=True)
class TableRef:
    table: type[SimpleTable]
    qualifier: str | None = None

    @property
    def filters(self) -> FieldNamespace:
        return FieldNamespace(self.table, qualifier=self.qualifier)

    @property
    def f(self) -> FieldNamespace:
        return self.filters


@dataclass(frozen=True)
class JoinHandle:
    table: type[SimpleTable]
    spec: JoinSpec

    @property
    def name(self) -> str:
        return self.spec.name

    @property
    def ref(self) -> TableRef:
        return TableRef(self.table, qualifier=self.name)

    @property
    def filters(self) -> FieldNamespace:
        return self.ref.filters

    @property
    def f(self) -> FieldNamespace:
        return self.filters


class SimpleTableQueryMixin:
    @classproperty
    def filters(cls) -> FieldNamespace:
        return FieldNamespace(cls)

    @classproperty
    def f(cls) -> FieldNamespace:
        return cls.filters

    @classmethod
    def ref(cls, qualifier: str) -> TableRef:
        return TableRef(cls, qualifier=qualifier)

    @classmethod
    def join(
        cls,
        name: str,
        *,
        storage_hash: str | None = None,
        node_unique_identifier: str | None = None,
        type: JoinType = JoinType.inner,
        on: Sequence[JoinKey] | None = None,
    ) -> JoinHandle:
        spec = JoinSpec(
            name=name,
            storage_hash=storage_hash,
            node_unique_identifier=node_unique_identifier,
            type=type,
            on=list(on) if on is not None else [JoinKey.time_index, JoinKey.unique_identifier],
        )
        return JoinHandle(table=cls, spec=spec)

    @classmethod
    def request(
        cls,
        *,
        storage_hash: str | None = None,
        node_unique_identifier: str | None = None,
        joins: Sequence[JoinSpec | JoinHandle] | None = None,
        filter: FilterExpr | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> SearchRequest:
        normalized_joins: list[JoinSpec] = []
        for join in joins or ():
            if isinstance(join, JoinHandle):
                normalized_joins.append(join.spec)
            elif isinstance(join, JoinSpec):
                normalized_joins.append(join)
            else:
                raise TypeError(
                    "joins must contain JoinSpec or JoinHandle instances. "
                    f"Got {type(join)!r}."
                )

        return SearchRequest(
            storage_hash=storage_hash,
            node_unique_identifier=node_unique_identifier,
            joins=normalized_joins,
            filter=filter,
            limit=limit,
            offset=offset,
        )


__all__ = [
    "Condition",
    "F",
    "FieldRef",
    "FieldNamespace",
    "FilterExpr",
    "Group",
    "JoinHandle",
    "JoinKey",
    "JoinSpec",
    "JoinType",
    "Logic",
    "Op",
    "SearchRequest",
    "SimpleTableQueryMixin",
    "TableRef",
    "and_",
    "or_",
]
