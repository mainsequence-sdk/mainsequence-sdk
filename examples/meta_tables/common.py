from __future__ import annotations

import json
import os
from typing import Any

DEFAULT_NAMESPACE = "examples.meta_tables"
DEFAULT_SCHEMA = os.getenv("MAINSEQUENCE_META_TABLE_SCHEMA", "public").strip() or "public"
DEFAULT_TIMEOUT = int(os.getenv("MAINSEQUENCE_META_TABLE_TIMEOUT", "120"))

PLACEHOLDER_ACCOUNT_META_TABLE_UID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
PLACEHOLDER_ASSET_META_TABLE_UID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
PLACEHOLDER_ACCOUNT_ROW_UID = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
POSTGRES_IDENTIFIER_MAX_LENGTH = 63


def env_flag(name: str, *, default: bool = False) -> bool:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    return raw_value.strip().lower() in {"1", "true", "yes", "on"}


def required_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        raise RuntimeError(f"Set {name} before running this example.")
    return value.strip()


def optional_env(name: str, default: str) -> str:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return value.strip()


def print_json(title: str, payload: Any) -> None:
    if hasattr(payload, "model_dump"):
        payload = payload.model_dump(mode="json", by_alias=True, exclude_none=True)
    print(f"\n{title}")
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))


def derived_postgres_identifier(*, table_name: str, suffix: str) -> str:
    separator_length = 1
    prefix_max_length = POSTGRES_IDENTIFIER_MAX_LENGTH - len(suffix) - separator_length
    if prefix_max_length < 1:
        raise ValueError("suffix is too long for a PostgreSQL identifier.")
    return f"{table_name[:prefix_max_length]}_{suffix}"
