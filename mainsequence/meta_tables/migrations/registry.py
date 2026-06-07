from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import Any

from mainsequence.meta_tables.sqlalchemy_contracts import (
    PlatformManagedMetaTable,
    PlatformTimeIndexMetaTable,
)


def build_metatable_model_registry(
    *sources: type[Any] | Iterable[type[Any]] | Callable[[], Iterable[type[Any]]],
    base: type[Any] | None = None,
) -> list[type[Any]]:
    """Build a deterministic provider-scoped MetaTable model registry."""

    registry: list[type[Any]] = []
    seen_models: set[type[Any]] = set()
    identifiers: dict[str, type[Any]] = {}
    for model in _iter_registry_sources(sources):
        if not _is_supported_metatable_model(model):
            continue
        if base is not None and not issubclass(model, base):
            continue
        identifier = _model_identifier(model)
        existing = identifiers.get(identifier)
        if existing is not None and existing is not model:
            raise ValueError(
                "Duplicate MetaTable identifier in migration registry "
                f"{identifier!r}: {existing.__module__}.{existing.__qualname__} and "
                f"{model.__module__}.{model.__qualname__}."
            )
        identifiers[identifier] = model
        if model in seen_models:
            continue
        seen_models.add(model)
        registry.append(model)
    return registry


def _iter_registry_sources(
    sources: tuple[
        type[Any] | Iterable[type[Any]] | Callable[[], Iterable[type[Any]]],
        ...,
    ],
) -> Iterable[type[Any]]:
    for source in sources:
        if _is_supported_metatable_model(source):
            yield source
            continue
        models = source() if callable(source) else source
        if models is None:
            continue
        yield from models


def _is_supported_metatable_model(model: Any) -> bool:
    return isinstance(model, type) and issubclass(
        model,
        (PlatformManagedMetaTable, PlatformTimeIndexMetaTable),
    )


def _model_identifier(model: type[Any]) -> str:
    identifier = getattr(model, "__metatable_identifier__", None)
    if identifier in (None, ""):
        raise ValueError(
            "Migration-managed MetaTable models must define a non-empty "
            f"__metatable_identifier__: {model.__module__}.{model.__qualname__}."
        )
    return str(identifier)


__all__ = ["build_metatable_model_registry"]
