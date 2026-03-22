"""mainsequence.tdag.data_nodes.simple_tables.models
=================================================

Friendly table declarations for the mini-ORM layer.

This module keeps the existing declarative filter payloads untouched and builds
an ergonomic layer on top of them. Users declare tables by subclassing
:class:`SimpleTable` and attaching metadata with ``Annotated[...]``.

The key goals are:

1. **User-friendly schema declarations**
   - define fields with normal Pydantic annotations
   - attach relationship/index/runtime-operation metadata with
     :class:`ForeignKey`, :class:`Index`, and :class:`Ops`

2. **Typed declarative filters**
   - build backend filter payloads without writing raw field strings
   - produce the exact same ``Condition`` / ``Group`` / ``SearchRequest``
     objects already defined in ``data_filters.py``

3. **Compiler-friendly introspection**
   - expose normalized schema information for your schema compiler
   - force the ``id`` field to be the primary key whenever it exists

Backward compatibility
----------------------
This module does **not** change the wire format or runtime behavior of the
existing data-filter payloads. It only adds a more ergonomic construction layer
on top of them.

Examples
--------
Basic table declarations::

    from decimal import Decimal
    from typing import Annotated
    from pydantic import Field

    class Client(SimpleTable):
        id: int
        name: Annotated[str, Index(), Ops(filter=True, order=True)] = Field(max_length=200)

    class ClientPurchase(SimpleTable):
        id: int
        client: Annotated[
            int,
            ForeignKey(Client, on_delete="cascade"),
            Index(),
            Ops(filter=True),
        ]
        total: Annotated[Decimal, Ops(filter=True, order=True)]

Build filter payloads without raw string field names::

    from decimal import Decimal

    base_filter = Client.filters.name.eq("ACME")
    joined = ClientPurchase.join(
        "purchases",
        node_unique_identifier="client_purchases",
    )

    request = Client.request(
        node_unique_identifier="clients",
        joins=[joined],
        filter=and_(
            Client.filters.name.eq("ACME"),
            joined.filters.total.gte(Decimal("100.00")),
        ),
        limit=100,
    )

    payload = request.model_dump(mode="json")
    assert payload["filter"]["items"][0]["field"] == "name"
    assert payload["filter"]["items"][1]["field"] == "purchases.total"

Expose compiler-facing metadata::

    schema = ClientPurchase.schema()
    assert schema.primary_key_name == "id"
    assert schema.fields[1].foreign_key.target is Client
    assert schema.physical_name.startswith("t_")
"""

from __future__ import annotations

import hashlib
import json
import types
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from typing import Any, Union, get_args, get_origin

from pydantic import BaseModel, ConfigDict

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

_ALLOWED_ON_DELETE = {"cascade", "restrict", "set_null"}


class classproperty:  # noqa: N801 - intentionally lower-case to mirror @property
    """Minimal descriptor for class-level computed properties.

    The returned object is computed from the owning class, not from an
    instance. This keeps the public API compact and allows a natural syntax such
    as ``Client.filters.name.eq("ACME")``.
    """

    def __init__(self, func):
        self.func = func

    def __get__(self, instance, owner):
        return self.func(owner)


@dataclass(frozen=True)
class ForeignKey:
    """Logical foreign-key declaration attached through ``Annotated`` metadata.

    Parameters
    ----------
    target:
        The target :class:`SimpleTable` subclass.
    on_delete:
        Backend delete behavior. Supported values are ``"cascade"``,
        ``"restrict"``, and ``"set_null"``.

    Notes
    -----
    The logical field name stays user-friendly (for example ``client``), while
    the physical database compiler is free to emit a storage column such as
    ``client_id``.
    """

    target: type[SimpleTable]
    on_delete: str = "restrict"

    def __post_init__(self) -> None:
        if not issubclass(self.target, SimpleTable):
            raise TypeError("ForeignKey.target must be a SimpleTable subclass.")
        if self.on_delete not in _ALLOWED_ON_DELETE:
            raise ValueError(
                "ForeignKey.on_delete must be one of "
                f"{sorted(_ALLOWED_ON_DELETE)}."
            )


@dataclass(frozen=True)
class Index:
    """Index declaration attached through ``Annotated`` metadata.

    Parameters
    ----------
    unique:
        Whether the index is unique.
    name:
        Optional backend-facing index name override.
    """

    unique: bool = False
    name: str | None = None


@dataclass(frozen=True)
class Ops:
    """Runtime operation policy for a field.

    Parameters
    ----------
    insert:
        Whether the field may be set during create/insert operations.
    update:
        Whether the field may be updated after creation.
    filter:
        Whether the field may appear in backend declarative filters.
    order:
        Whether the field may be used as an ordering key.

    Why this matters
    ----------------
    ``Ops`` is not a storage concern; it is a runtime contract. It lets the
    schema declare what the API or mini-ORM is allowed to do with a field.
    This module uses ``Ops.filter`` immediately to prevent construction of
    invalid backend filter payloads.
    """

    insert: bool = True
    update: bool = True
    filter: bool = True
    order: bool = False


@dataclass(frozen=True)
class TableFieldSpec:
    """Normalized field information extracted from a ``SimpleTable`` subclass.

    Attributes
    ----------
    name:
        Logical field name as written by the user.
    annotation:
        The Python annotation after optionality unwrapping.
    nullable:
        Whether the field allows ``None``.
    required:
        Whether Pydantic requires the field on construction.
    default:
        The field default value when one exists.
    primary_key:
        ``True`` when the field is the forced primary key.
    foreign_key:
        Optional foreign-key metadata.
    index:
        Optional index metadata.
    ops:
        Runtime operation policy for the field.
    physical_column_name:
        Suggested backend physical column name. Foreign keys default to
        ``<name>_id`` while scalar fields keep their logical names.
    """

    name: str
    annotation: Any
    nullable: bool
    required: bool
    default: Any
    primary_key: bool
    foreign_key: ForeignKey | None
    index: Index | None
    ops: Ops

    @property
    def physical_column_name(self) -> str:
        """Return the compiler-friendly physical column name for this field."""
        return f"{self.name}_id" if self.foreign_key is not None else self.name


@dataclass(frozen=True)
class TableSchema:
    """Compiler-facing normalized schema for a :class:`SimpleTable` subclass."""

    model: type[SimpleTable]
    fields: tuple[TableFieldSpec, ...]

    @property
    def model_name(self) -> str:
        """Fully-qualified logical model name used in schema hashing."""
        return f"{self.model.__module__}.{self.model.__qualname__}"

    @property
    def primary_key_name(self) -> str | None:
        """Return the primary-key field name, if one exists."""
        for field in self.fields:
            if field.primary_key:
                return field.name
        return None

    def to_canonical_dict(self) -> dict[str, Any]:
        """Return a stable dictionary representation for hashing and persistence."""
        return {
            "model": self.model_name,
            "fields": [
                {
                    "name": field.name,
                    "annotation": _annotation_name(field.annotation),
                    "nullable": field.nullable,
                    "required": field.required,
                    "default": None if field.required else _stable_default_repr(field.default),
                    "primary_key": field.primary_key,
                    "foreign_key": None
                    if field.foreign_key is None
                    else {
                        "target": (
                            f"{field.foreign_key.target.__module__}."
                            f"{field.foreign_key.target.__qualname__}"
                        ),
                        "on_delete": field.foreign_key.on_delete,
                    },
                    "index": None if field.index is None else asdict(field.index),
                    "ops": asdict(field.ops),
                }
                for field in self.fields
            ],
        }

    @property
    def fingerprint(self) -> str:
        """Stable content hash of the normalized table schema."""
        canonical = json.dumps(self.to_canonical_dict(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    @property
    def physical_name(self) -> str:
        """Deterministic backend table name derived from the schema fingerprint."""
        return f"t_{self.fingerprint[:24]}"


@dataclass(frozen=True)
class FieldRef:
    """Reference to a filterable field on a table or join alias.

    ``FieldRef`` does not invent a new filter payload format. Each comparison
    method returns the existing :class:`Condition` model from ``data_filters``.
    """

    table: type[SimpleTable]
    spec: TableFieldSpec
    qualifier: str | None = None

    @property
    def name(self) -> str:
        """Return the logical backend field name, qualified when joined."""
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
        """Return ``field == value``."""
        return self._condition(Op.eq, value)

    def ne(self, value: Any) -> Condition:
        """Return ``field != value``."""
        return self._condition(Op.ne, value)

    def lt(self, value: Any) -> Condition:
        """Return ``field < value``."""
        return self._condition(Op.lt, value)

    def lte(self, value: Any) -> Condition:
        """Return ``field <= value``."""
        return self._condition(Op.lte, value)

    def gt(self, value: Any) -> Condition:
        """Return ``field > value``."""
        return self._condition(Op.gt, value)

    def gte(self, value: Any) -> Condition:
        """Return ``field >= value``."""
        return self._condition(Op.gte, value)

    def in_(self, values: Sequence[Any]) -> Condition:
        """Return ``field IN values``."""
        return self._condition(Op.in_, list(values))

    def not_in(self, values: Sequence[Any]) -> Condition:
        """Return ``field NOT IN values``."""
        return self._condition(Op.not_in, list(values))

    def contains(self, value: str) -> Condition:
        """Return a substring containment condition for string-like fields."""
        return self._condition(Op.contains, value)

    def starts_with(self, value: str) -> Condition:
        """Return a prefix-match condition for string-like fields."""
        return self._condition(Op.starts_with, value)

    def ends_with(self, value: str) -> Condition:
        """Return a suffix-match condition for string-like fields."""
        return self._condition(Op.ends_with, value)

    def is_null(self, value: bool | None = True) -> Condition:
        """Return a null-check condition."""
        return self._condition(Op.is_null, value)

    def between(self, start: Any, end: Any) -> Condition:
        """Return a closed-range condition represented as ``[start, end]``."""
        return self._condition(Op.between, [start, end])

    def order_key(self, *, descending: bool = False) -> str:
        """Return a validated ordering key for future order-by payloads.

        This module does not define an order payload because ``data_filters``
        currently has no ordering model, but the method centralizes
        ``Ops.order`` enforcement so the caller can safely pass the returned
        field name to any external ordering request wrapper.
        """
        if not self.spec.ops.order:
            raise ValueError(
                f"Field '{self.spec.name}' on {self.table.__name__} is not allowed "
                "for ordering (Ops.order=False)."
            )
        return f"-{self.name}" if descending else self.name


class FieldNamespace:
    """Attribute-based access to :class:`FieldRef` objects.

    Instances are usually obtained from ``Client.filters`` or from a join handle:

    - ``Client.filters.name``
    - ``joined.filters.total``
    """

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
    """Qualified reference to a table in base or joined filter expressions."""

    table: type[SimpleTable]
    qualifier: str | None = None

    @property
    def filters(self) -> FieldNamespace:
        """Return the attribute-based filter namespace for this table reference."""
        return FieldNamespace(self.table, qualifier=self.qualifier)

    @property
    def f(self) -> FieldNamespace:
        """Short alias for :attr:`filters`."""
        return self.filters


@dataclass(frozen=True)
class JoinHandle:
    """Join helper that bundles a :class:`JoinSpec` and alias-qualified fields.

    Use ``JoinHandle.spec`` inside :class:`SearchRequest.joins`, and use
    ``JoinHandle.filters`` / ``JoinHandle.f`` to build alias-qualified filter
    conditions such as ``purchases.total``.
    """

    table: type[SimpleTable]
    spec: JoinSpec

    @property
    def name(self) -> str:
        """Return the join alias used in backend field prefixes."""
        return self.spec.name

    @property
    def ref(self) -> TableRef:
        """Return a qualified table reference for this join alias."""
        return TableRef(self.table, qualifier=self.name)

    @property
    def filters(self) -> FieldNamespace:
        """Return alias-qualified filter fields for the join target."""
        return self.ref.filters

    @property
    def f(self) -> FieldNamespace:
        """Short alias for :attr:`filters`."""
        return self.filters


class SimpleTable(BaseModel):
    """Base class for user-facing logical table declarations.

    Subclasses are plain Pydantic models plus extra metadata stored in
    ``Annotated[...]``. The class provides:

    - schema introspection for the compiler
    - typed filter construction for the backend declarative filter API
    - join helpers that still produce the existing ``JoinSpec`` payloads
    """

    model_config = ConfigDict(extra="forbid", ignored_types=(classproperty,))

    @classproperty
    def filters(cls) -> FieldNamespace:
        """Class-level filter namespace.

        Examples
        --------
        ``Client.filters.name.eq("ACME")``
        ``Client.filters.id.in_([1, 2, 3])``
        """
        return FieldNamespace(cls)

    @classproperty
    def f(cls) -> FieldNamespace:
        """Short alias for :attr:`filters`."""
        return cls.filters

    @classmethod
    def ref(cls, qualifier: str) -> TableRef:
        """Return a qualified table reference for an existing join alias."""
        return TableRef(cls, qualifier=qualifier)

    @classmethod
    def field_spec(cls, field_name: str) -> TableFieldSpec:
        """Return normalized metadata for one declared field.

        Raises
        ------
        KeyError
            If the field does not exist on the table declaration.
        """
        schema = cls.schema()
        for spec in schema.fields:
            if spec.name == field_name:
                return spec
        valid = ", ".join(sorted(cls.model_fields.keys()))
        raise KeyError(f"Unknown field '{field_name}' for {cls.__name__}. Valid fields: {valid}")

    @classmethod
    def field_specs(cls) -> tuple[TableFieldSpec, ...]:
        """Return all normalized field specifications."""
        return cls.schema().fields

    @classmethod
    def schema(cls) -> TableSchema:
        """Return compiler-facing normalized schema information for the table."""
        return _build_table_schema(cls)

    @classmethod
    def schema_fingerprint(cls) -> str:
        """Return the stable hash of the normalized schema."""
        return cls.schema().fingerprint

    @classmethod
    def physical_name(cls) -> str:
        """Return the deterministic backend table name for this schema."""
        return cls.schema().physical_name

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
        """Create a join declaration plus alias-qualified field helpers.

        Parameters
        ----------
        name:
            Join alias used both in the backend ``JoinSpec`` and in qualified
            filter fields (for example ``"purchases.total"``).
        storage_hash / node_unique_identifier:
            The existing backend selector used by ``JoinSpec``. Exactly one must
            be provided, matching ``data_filters`` semantics.
        type:
            Join type passed through unchanged to ``JoinSpec``.
        on:
            Optional join keys. Defaults to the same pair used by
            ``JoinSpec`` in ``data_filters``.
        """
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
        """Build a ``SearchRequest`` using this table's declarative filters.

        The resulting object is the exact same ``SearchRequest`` model defined
        in ``data_filters.py``.
        """
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


def _build_table_schema(model: type[SimpleTable]) -> TableSchema:
    """Build and cache a normalized :class:`TableSchema` for ``model``."""
    cache = getattr(model, "__simple_table_schema_cache__", None)
    if cache is not None:
        return cache

    fields: list[TableFieldSpec] = []
    for name, field_info in model.model_fields.items():
        metadata = list(field_info.metadata)
        foreign_key = _extract_single(metadata, ForeignKey, field_name=name, model=model)
        index = _extract_single(metadata, Index, field_name=name, model=model)
        ops = _extract_single(metadata, Ops, field_name=name, model=model)

        annotation, nullable = _unwrap_optional(field_info.annotation)
        primary_key = name == "id"
        effective_ops = _effective_ops(name=name, explicit_ops=ops)

        fields.append(
            TableFieldSpec(
                name=name,
                annotation=annotation,
                nullable=False if primary_key else nullable,
                required=field_info.is_required(),
                default=None if field_info.is_required() else field_info.default,
                primary_key=primary_key,
                foreign_key=foreign_key,
                index=index,
                ops=effective_ops,
            )
        )

    schema = TableSchema(model=model, fields=tuple(fields))
    model.__simple_table_schema_cache__ = schema
    return schema


def _extract_single(
    metadata: Sequence[Any],
    kind: type[Any],
    *,
    field_name: str,
    model: type[SimpleTable],
) -> Any | None:
    """Return a single metadata object of ``kind`` or raise on duplicates."""
    matches = [item for item in metadata if isinstance(item, kind)]
    if len(matches) > 1:
        raise ValueError(
            f"Field '{field_name}' on {model.__name__} declares multiple {kind.__name__} metadata entries."
        )
    return matches[0] if matches else None


def _effective_ops(*, name: str, explicit_ops: Ops | None) -> Ops:
    """Return the runtime operation policy for a field.

    The special case for ``id`` reflects its typical role as an immutable primary
    key: it is filterable and orderable by default, but not updateable unless the
    caller explicitly overrides the behavior.
    """
    if explicit_ops is not None:
        return explicit_ops
    if name == "id":
        return Ops(insert=True, update=False, filter=True, order=True)
    return Ops()


def _unwrap_optional(annotation: Any) -> tuple[Any, bool]:
    """Return ``(inner_annotation, nullable)`` for optional union types."""
    origin = get_origin(annotation)
    if origin in (Union, types.UnionType):
        args = get_args(annotation)
        if type(None) in args:
            non_none_args = tuple(arg for arg in args if arg is not type(None))
            if len(non_none_args) == 1:
                return non_none_args[0], True
            return Union[non_none_args], True
    return annotation, False


def _annotation_name(annotation: Any) -> str:
    """Return a stable human-readable name for an annotation."""
    if hasattr(annotation, "__name__"):
        return annotation.__name__
    return repr(annotation)


def _stable_default_repr(value: Any) -> Any:
    """Return a JSON-safe representation for schema hashing.

    The goal is deterministic hashing, not round-trip serialization.
    """
    try:
        json.dumps(value)
        return value
    except TypeError:
        return repr(value)


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
