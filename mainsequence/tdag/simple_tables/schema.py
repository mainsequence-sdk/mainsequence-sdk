from __future__ import annotations

import json
import types
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any, Union, get_args, get_origin

if TYPE_CHECKING:
    from .models import SimpleTable


_ALLOWED_ON_DELETE = {"cascade", "restrict", "set_null"}


@dataclass(frozen=True)
class ForeignKey:
    """Logical foreign-key declaration attached through ``Annotated`` metadata."""

    target: type[SimpleTable]
    on_delete: str = "restrict"

    def __post_init__(self) -> None:
        from .models import SimpleTable

        if not issubclass(self.target, SimpleTable):
            raise TypeError("ForeignKey.target must be a SimpleTable subclass.")
        if self.on_delete not in _ALLOWED_ON_DELETE:
            raise ValueError(
                "ForeignKey.on_delete must be one of "
                f"{sorted(_ALLOWED_ON_DELETE)}."
            )


@dataclass(frozen=True)
class Index:
    """Index declaration attached through ``Annotated`` metadata."""

    unique: bool = False
    name: str | None = None


@dataclass(frozen=True)
class Ops:
    """Runtime operation policy for a field."""

    insert: bool = True
    update: bool = True
    filter: bool = True
    order: bool = False


@dataclass(frozen=True)
class TableFieldSpec:
    """Normalized field information extracted from a ``SimpleTable`` subclass."""

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
        return f"{self.name}_id" if self.foreign_key is not None else self.name


@dataclass(frozen=True)
class TableSchema:
    """Compiler-facing normalized schema for a :class:`SimpleTable` subclass."""

    model: type[SimpleTable]
    fields: tuple[TableFieldSpec, ...]

    @property
    def model_name(self) -> str:
        return f"{self.model.__module__}.{self.model.__qualname__}"

    @property
    def primary_key_name(self) -> str | None:
        for field in self.fields:
            if field.primary_key:
                return field.name
        return None

    def to_canonical_dict(self) -> dict[str, Any]:
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

class SimpleTableSchemaMixin:
    @classmethod
    def field_spec(cls, field_name: str) -> TableFieldSpec:
        schema = cls.schema()
        for spec in schema.fields:
            if spec.name == field_name:
                return spec
        valid = ", ".join(sorted(cls.model_fields.keys()))
        raise KeyError(f"Unknown field '{field_name}' for {cls.__name__}. Valid fields: {valid}")

    @classmethod
    def field_specs(cls) -> tuple[TableFieldSpec, ...]:
        return cls.schema().fields

    @classmethod
    def schema(cls) -> TableSchema:
        return _build_table_schema(cls)


def _build_table_schema(model: type[SimpleTable]) -> TableSchema:
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
    matches = [item for item in metadata if isinstance(item, kind)]
    if len(matches) > 1:
        raise ValueError(
            f"Field '{field_name}' on {model.__name__} declares multiple {kind.__name__} metadata entries."
        )
    return matches[0] if matches else None


def _effective_ops(*, name: str, explicit_ops: Ops | None) -> Ops:
    if explicit_ops is not None:
        return explicit_ops
    if name == "id":
        return Ops(insert=True, update=False, filter=True, order=True)
    return Ops()


def _unwrap_optional(annotation: Any) -> tuple[Any, bool]:
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
    if hasattr(annotation, "__name__"):
        return annotation.__name__
    return repr(annotation)


def _stable_default_repr(value: Any) -> Any:
    try:
        json.dumps(value)
        return value
    except TypeError:
        return repr(value)


__all__ = [
    "ForeignKey",
    "Index",
    "Ops",
    "SimpleTableSchemaMixin",
    "TableFieldSpec",
    "TableSchema",
]
