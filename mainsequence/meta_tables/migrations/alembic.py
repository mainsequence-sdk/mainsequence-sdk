from __future__ import annotations

import hashlib
import re
import sys
from collections.abc import Callable, Sequence
from typing import Any

from sqlalchemy import MetaData

from mainsequence.meta_tables.sqlalchemy_contracts import _resolve_table

from .registry import build_metatable_model_registry

_MAX_NAMESPACE_SLUG_LENGTH = 48
_NAMESPACE_HASH_LENGTH = 10


def namespace_version_slug(namespace: str | None) -> str:
    """Return a stable filesystem-safe Alembic version-directory slug."""

    if namespace is None or str(namespace).strip() == "":
        return "default"
    raw_namespace = str(namespace).strip()
    slug = re.sub(r"[^0-9A-Za-z]+", "_", raw_namespace.lower())
    slug = re.sub(r"_+", "_", slug).strip("_")
    if not slug:
        return "default"
    if len(slug) <= _MAX_NAMESPACE_SLUG_LENGTH:
        return slug
    digest = hashlib.sha1(raw_namespace.encode("utf-8")).hexdigest()[
        :_NAMESPACE_HASH_LENGTH
    ]
    prefix_length = _MAX_NAMESPACE_SLUG_LENGTH - _NAMESPACE_HASH_LENGTH - 1
    prefix = slug[:prefix_length].rstrip("_")
    return f"{prefix}_{digest}"


def namespace_version_location(
    namespace: str | None,
    *,
    prefix: str = "migrations:versions",
) -> str:
    """Return Alembic's resource location for a namespace-specific versions dir."""

    normalized_prefix = str(prefix).strip().rstrip("/")
    if not normalized_prefix:
        raise ValueError("namespace_version_location prefix cannot be empty.")
    return f"{normalized_prefix}/{namespace_version_slug(namespace)}"


def build_alembic_version_metatable(
    *,
    class_name: str = "ProjectAlembicVersion",
    namespace: str,
    identifier: str,
    schema: str | None,
    table_name: str,
    column_name: str = "version_num",
) -> type[Any]:
    """Create an Alembic version MetaTable binding class."""

    if not class_name.isidentifier():
        raise ValueError(f"Alembic version MetaTable class_name is invalid: {class_name!r}.")
    if not namespace:
        raise ValueError("Alembic version MetaTable namespace cannot be empty.")
    if not identifier:
        raise ValueError("Alembic version MetaTable identifier cannot be empty.")
    if not table_name:
        raise ValueError("Alembic version MetaTable table_name cannot be empty.")
    if not column_name:
        raise ValueError("Alembic version MetaTable column_name cannot be empty.")

    from mainsequence.meta_tables.migrations import AlembicVersionMetaTable

    caller_module = sys._getframe(1).f_globals.get("__name__", __name__)
    return type(
        class_name,
        (AlembicVersionMetaTable,),
        {
            "__module__": caller_module,
            "__metatable_namespace__": namespace,
            "__metatable_identifier__": identifier,
            "__alembic_version_schema__": schema,
            "__alembic_version_table_name__": table_name,
            "__alembic_version_column_name__": column_name,
        },
    )


def build_metatable_migration_provider(
    *,
    package: str,
    migration_namespace: str,
    target_metadata: Any,
    alembic_registry: type[Any],
    metatable_models: Sequence[type[Any]],
    script_location: str = "migrations:",
    version_location_prefix: str = "migrations:versions",
    after_register_metatables: Callable[[Any], Any] | None = None,
    include_name_hook: Any | None = None,
    include_object_hook: Any | None = None,
) -> Any:
    """Build an SDK MetaTable Alembic migration provider."""

    from mainsequence.meta_tables.migrations import AlembicMetaTableMigration

    version_location = namespace_version_location(
        migration_namespace,
        prefix=version_location_prefix,
    )
    return AlembicMetaTableMigration(
        package=package,
        migration_namespace=migration_namespace,
        script_location=script_location,
        target_metadata=target_metadata,
        alembic_registry=alembic_registry,
        version_locations=[version_location],
        version_path=version_location,
        metatable_models=build_metatable_model_registry(metatable_models),
        after_register_metatables=after_register_metatables,
        include_name_hook=include_name_hook,
        include_object_hook=include_object_hook,
    )


def metadata_for_models(models: Sequence[type[Any]]) -> MetaData:
    """Copy the SQLAlchemy tables for selected MetaTable models into new metadata."""

    target = MetaData(naming_convention=_naming_convention_for_models(models))
    seen: set[tuple[str | None, str]] = set()
    for model in models:
        table = _resolve_table(model)
        table_name = getattr(table, "name", None)
        if not table_name:
            raise ValueError(
                "MetaTable migration model has no SQLAlchemy table name: "
                f"{getattr(model, '__module__', type(model).__module__)}."
                f"{getattr(model, '__qualname__', repr(model))}."
            )
        key = (getattr(table, "schema", None), str(table_name))
        if key in seen:
            continue
        seen.add(key)
        table.to_metadata(target)
    return target


def _naming_convention_for_models(models: Sequence[type[Any]]) -> dict[str, str] | None:
    for model in models:
        table = _resolve_table(model)
        metadata = getattr(table, "metadata", None)
        naming_convention = getattr(metadata, "naming_convention", None)
        if naming_convention:
            return dict(naming_convention)
    return None


__all__ = [
    "build_alembic_version_metatable",
    "build_metatable_migration_provider",
    "metadata_for_models",
    "namespace_version_location",
    "namespace_version_slug",
]
