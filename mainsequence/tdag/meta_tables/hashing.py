from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from typing import Any

from mainsequence.tdag.pydantic_metadata import strip_pydantic_hash_exclusions

POSTGRES_IDENTIFIER_MAX_LENGTH = 63
_HASH_SUFFIX_LENGTH = 33


def slugify_identifier(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", value).strip("_").lower()
    if not slug:
        return "table"
    if slug[0].isdigit():
        return f"t_{slug}"
    return slug


def build_meta_table_storage_hash(
    *,
    namespace: str,
    identifier: str,
    schema: str = "public",
    hash_namespace: str | None = None,
    extra_hash_components: Mapping[str, Any] | None = None,
    max_length: int = POSTGRES_IDENTIFIER_MAX_LENGTH,
) -> str:
    """
    Build a PostgreSQL-safe MetaTable storage hash using the DataNode hash path.

    The table name prefix keeps the identifier readable while the hash suffix is
    produced by ``mainsequence.tdag.data_nodes.build_operations.create_config``.
    """

    if max_length <= _HASH_SUFFIX_LENGTH:
        raise ValueError("max_length must leave room for the DataNode hash suffix.")

    namespace = namespace.strip()
    identifier = identifier.strip()
    schema = schema.strip()
    if not namespace:
        raise ValueError("namespace is required to build a MetaTable storage hash.")
    if not identifier:
        raise ValueError("identifier is required to build a MetaTable storage hash.")
    if not schema:
        raise ValueError("schema is required to build a MetaTable storage hash.")

    prefix_base = slugify_identifier(f"mt_{namespace}_{identifier}")
    max_prefix_length = max_length - _HASH_SUFFIX_LENGTH
    prefix = prefix_base[:max_prefix_length].rstrip("_") or "mt"

    hash_payload: dict[str, Any] = {
        "namespace": namespace,
        "identifier": identifier,
        "schema": schema,
    }
    if hash_namespace:
        hash_payload["hash_namespace"] = hash_namespace.strip()
    if extra_hash_components:
        hash_payload.update(dict(extra_hash_components))

    storage_hash = _build_storage_hash_with_data_node_machinery(
        prefix=prefix,
        hash_payload=hash_payload,
    )
    if len(storage_hash) > max_length:
        raise ValueError(
            f"Generated MetaTable storage hash exceeds {max_length} characters: "
            f"{storage_hash!r}."
        )
    return storage_hash


def _build_storage_hash_with_data_node_machinery(
    *,
    prefix: str,
    hash_payload: Mapping[str, Any],
) -> str:
    try:
        from mainsequence.tdag.data_nodes.build_operations import create_config
    except (AssertionError, ImportError):
        return _build_storage_hash_without_tdag_config(prefix=prefix, hash_payload=hash_payload)

    try:
        config = create_config(
            ts_class_name="MetaTable",
            kwargs=dict(hash_payload),
            storage_hash_prefix=prefix,
        )
    except AssertionError:
        return _build_storage_hash_without_tdag_config(prefix=prefix, hash_payload=hash_payload)
    return config.storage_hash


def _build_storage_hash_without_tdag_config(
    *,
    prefix: str,
    hash_payload: Mapping[str, Any],
) -> str:
    storage_payload = strip_pydantic_hash_exclusions(
        dict(hash_payload),
        for_storage_hash=True,
    )
    encoded = json.dumps(storage_payload, sort_keys=True).encode()
    digest = hashlib.md5(encoded).hexdigest()
    return f"{prefix}_{digest}".lower()


__all__ = [
    "POSTGRES_IDENTIFIER_MAX_LENGTH",
    "build_meta_table_storage_hash",
    "slugify_identifier",
]
