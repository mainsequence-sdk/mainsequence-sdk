# mainsequence/tdag/simple_tables/filters.py
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from pydantic import Field, model_validator

from ..filters import (
    BaseSearchRequest,
    Condition,
    F,
    FilterExpr,
    Group,
    JoinType,
    Logic,
    Op,
    and_,
    or_,
)
from ..filters import (
    JoinSpec as BaseJoinSpec,
)
from .schema import TableFieldSpec

if TYPE_CHECKING:
    from .models import SimpleTable


class classproperty:  # noqa: N801
    def __init__(self, func):
        self.func = func

    def __get__(self, instance, owner):
        return self.func(owner)


class JoinSpec(BaseJoinSpec):
    @model_validator(mode="after")
    def _require_storage_hash(self):
        if not (self.storage_hash and self.storage_hash.strip()):
            raise ValueError("Simple-table joins require a storage_hash target.")
        if self.node_unique_identifier:
            raise ValueError("Simple-table joins do not support node_unique_identifier targets.")
        return self


class SearchRequest(BaseSearchRequest):
    joins: list[JoinSpec] = Field(default_factory=list)

    @model_validator(mode="after")
    def _require_storage_hash(self):
        if not (self.storage_hash and self.storage_hash.strip()):
            raise ValueError("Simple-table requests require a storage_hash target.")
        super()._validate_join_names()
        return self


def _field_namespace(table: type[SimpleTable], qualifier: str | None = None) -> FieldNamespace:
    return FieldNamespace(table, qualifier=qualifier)


class _FieldNamespaceAccessorMixin:
    table: type[SimpleTable]
    qualifier: str | None

    @property
    def filters(self) -> FieldNamespace:
        return _field_namespace(self.table, qualifier=self.qualifier)

    @property
    def f(self) -> FieldNamespace:
        return self.filters


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

    def _delegate_condition(self, builder, *args: Any) -> Condition:
        self._ensure_filterable()
        return builder(self.name, *args)

    def eq(self, value: Any) -> Condition:
        return self._delegate_condition(F.eq, value)

    def ne(self, value: Any) -> Condition:
        return self._delegate_condition(F.ne, value)

    def lt(self, value: Any) -> Condition:
        return self._delegate_condition(F.lt, value)

    def lte(self, value: Any) -> Condition:
        return self._delegate_condition(F.lte, value)

    def gt(self, value: Any) -> Condition:
        return self._delegate_condition(F.gt, value)

    def gte(self, value: Any) -> Condition:
        return self._delegate_condition(F.gte, value)

    def in_(self, values: Sequence[Any]) -> Condition:
        return self._delegate_condition(F.in_, list(values))

    def not_in(self, values: Sequence[Any]) -> Condition:
        return self._delegate_condition(F.not_in, list(values))

    def contains(self, value: str) -> Condition:
        return self._delegate_condition(F.contains, value)

    def starts_with(self, value: str) -> Condition:
        return self._delegate_condition(F.starts_with, value)

    def ends_with(self, value: str) -> Condition:
        return self._delegate_condition(F.ends_with, value)

    def is_null(self, value: bool | None = True) -> Condition:
        return self._delegate_condition(F.is_null, value)

    def between(self, start: Any, end: Any) -> Condition:
        return self._delegate_condition(F.between, start, end)

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
class TableRef(_FieldNamespaceAccessorMixin):
    table: type[SimpleTable]
    qualifier: str | None = None
    physical_table_name: str | None = None

    def bind(self, physical_table_name: str) -> TableRef:
        return TableRef(
            self.table,
            qualifier=self.qualifier,
            physical_table_name=physical_table_name,
        )

    def ref(self, qualifier: str) -> TableRef:
        return TableRef(
            self.table,
            qualifier=qualifier,
            physical_table_name=self.physical_table_name,
        )

    def join(
        self,
        name: str,
        *,
        target: TableRef | None = None,
        storage_hash: str | None = None,
        type: JoinType = JoinType.inner,
    ) -> JoinHandle:
        resolved_storage_hash = storage_hash
        join_table = self.table
        if resolved_storage_hash is None and target is not None:
            resolved_storage_hash = target.physical_table_name
        if target is not None:
            join_table = target.table
        return JoinHandle(
            table=join_table,
            spec=JoinSpec(
                name=name,
                storage_hash=resolved_storage_hash,
                type=type,
            ),
        )

    def request(
        self,
        *,
        storage_hash: str | None = None,
        joins: Sequence[JoinSpec | JoinHandle] | None = None,
        filter: FilterExpr | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> SearchRequest:
        return self.table.request(
            storage_hash=storage_hash or self.physical_table_name,
            joins=joins,
            filter=filter,
            limit=limit,
            offset=offset,
        )


@dataclass(frozen=True)
class JoinHandle(_FieldNamespaceAccessorMixin):
    table: type[SimpleTable]
    spec: JoinSpec

    @property
    def name(self) -> str:
        return self.spec.name

    @property
    def qualifier(self) -> str:
        return self.name

    @property
    def ref(self) -> TableRef:
        return TableRef(
            self.table,
            qualifier=self.name,
            physical_table_name=self.spec.storage_hash,
        )


class SimpleTableQueryMixin:
    @classproperty
    def filters(cls) -> FieldNamespace:
        return _field_namespace(cls)

    @classproperty
    def f(cls) -> FieldNamespace:
        return cls.filters

    @classmethod
    def ref(cls, qualifier: str, physical_table_name: str | None = None) -> TableRef:
        return TableRef(cls, qualifier=qualifier, physical_table_name=physical_table_name)

    @classmethod
    def bind(cls, physical_table_name: str, qualifier: str | None = None) -> TableRef:
        return TableRef(cls, qualifier=qualifier, physical_table_name=physical_table_name)

    @classmethod
    def join(
        cls,
        name: str,
        *,
        storage_hash: str | None = None,
        type: JoinType = JoinType.inner,
    ) -> JoinHandle:
        spec = JoinSpec(
            name=name,
            storage_hash=storage_hash,
            type=type,
        )
        return JoinHandle(table=cls, spec=spec)

    @classmethod
    def request(
        cls,
        *,
        storage_hash: str | None = None,
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
