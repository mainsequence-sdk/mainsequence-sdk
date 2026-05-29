from __future__ import annotations

from collections.abc import Callable
from typing import Any

from pydantic import BaseModel


def serialize_pydantic_model(
    value: BaseModel,
    *,
    serialize_field: Callable[[Any], Any],
) -> dict[str, Any]:
    import_path = {"module": value.__class__.__module__, "qualname": value.__class__.__qualname__}
    serialized_model = {
        field_name: serialize_field(getattr(value, field_name))
        for field_name in value.__class__.model_fields
    }
    hash_excluded_fields: list[str] = []

    for field_name, field_info in value.__class__.model_fields.items():
        extra = field_info.json_schema_extra or {}
        if "ignore_from_storage_hash" in extra:
            raise ValueError(
                f"{value.__class__.__name__}.{field_name} uses removed metadata "
                "'ignore_from_storage_hash'. All configuration fields participate in "
                'update hashing by default; use json_schema_extra={"hash_excluded": True} '
                "only for fields that should not affect update identity."
            )
        if "update_only" in extra:
            raise ValueError(
                f"{value.__class__.__name__}.{field_name} uses removed metadata "
                "'update_only'. All configuration fields are update-scoped by default."
            )
        if "runtime_only" in extra:
            raise ValueError(
                f"{value.__class__.__name__}.{field_name} uses removed metadata "
                "'runtime_only'; use json_schema_extra={\"hash_excluded\": True} instead."
            )

        is_hash_excluded = extra.get("hash_excluded", False)
        if not isinstance(is_hash_excluded, bool):
            raise ValueError(
                f"{value.__class__.__name__}.{field_name} metadata 'hash_excluded' must be bool"
            )
        if is_hash_excluded:
            hash_excluded_fields.append(field_name)

    return {
        "pydantic_model_import_path": import_path,
        "serialized_model": serialized_model,
        "hash_excluded": sorted(hash_excluded_fields),
    }


def is_serialized_pydantic_model(value: Any) -> bool:
    if not isinstance(value, dict):
        return False

    expected_keys = {"pydantic_model_import_path", "serialized_model", "hash_excluded"}
    if not {"pydantic_model_import_path", "serialized_model"}.issubset(value):
        return False
    if not set(value).issubset(expected_keys):
        return False

    import_path = value["pydantic_model_import_path"]
    if not isinstance(import_path, dict):
        return False
    if set(import_path) != {"module", "qualname"}:
        return False
    if not all(isinstance(import_path[key], str) for key in ("module", "qualname")):
        return False

    if not isinstance(value["serialized_model"], dict):
        return False

    meta_value = value.get("hash_excluded", [])
    if not isinstance(meta_value, list) or not all(isinstance(item, str) for item in meta_value):
        return False

    return True


def strip_pydantic_hash_exclusions(value: Any, *, for_storage_hash: bool) -> Any:
    if isinstance(value, list):
        return [
            strip_pydantic_hash_exclusions(item, for_storage_hash=for_storage_hash)
            for item in value
        ]

    if isinstance(value, tuple):
        return tuple(
            strip_pydantic_hash_exclusions(item, for_storage_hash=for_storage_hash)
            for item in value
        )

    if not isinstance(value, dict):
        return value

    if is_serialized_pydantic_model(value):
        fields_to_remove = set(value.get("hash_excluded", []))

        serialized_model = {
            key: strip_pydantic_hash_exclusions(item, for_storage_hash=for_storage_hash)
            for key, item in value.get("serialized_model", {}).items()
            if key not in fields_to_remove
        }

        return {
            "pydantic_model_import_path": value["pydantic_model_import_path"],
            "serialized_model": serialized_model,
        }

    return {
        key: strip_pydantic_hash_exclusions(item, for_storage_hash=for_storage_hash)
        for key, item in value.items()
    }
