from __future__ import annotations

import importlib
from dataclasses import dataclass
from functools import cache
from typing import Any


@dataclass(frozen=True)
class CliModelFilterSpec:
    key: str
    field_name: str
    lookup: str
    normalizer: str | None

    @property
    def value_format(self) -> str:
        if self.normalizer == "id":
            base = "integer ID"
        elif self.normalizer == "bool":
            base = "true/false"
        elif self.normalizer == "str":
            base = "text"
        else:
            base = "value"

        if self.lookup == "in":
            if base == "integer ID":
                return "comma-separated integer IDs"
            if base == "true/false":
                return "comma-separated true/false values"
            if base == "text":
                return "comma-separated text values"
            return "comma-separated values"
        return base

    @property
    def normalizer_label(self) -> str:
        return self.normalizer or "-"


def _split_model_ref(model_ref: str) -> tuple[str, str]:
    module_name, _, class_name = model_ref.rpartition(".")
    if not module_name or not class_name:
        raise ValueError(f"Invalid model reference: {model_ref!r}")
    return module_name, class_name


def _resolve_model_cls(model_ref: type | str) -> type:
    if isinstance(model_ref, str):
        module_name, class_name = _split_model_ref(model_ref)
        module = importlib.import_module(module_name)
        return getattr(module, class_name)
    return model_ref


def _filter_param_name(field_name: str, lookup: str) -> str:
    return field_name if lookup == "exact" else f"{field_name}__{lookup}"


@cache
def get_cli_model_filter_specs(model_ref: type | str | None) -> tuple[CliModelFilterSpec, ...]:
    if model_ref is None:
        return ()

    model_cls = _resolve_model_cls(model_ref)
    filterset_fields = getattr(model_cls, "FILTERSET_FIELDS", None) or {}
    normalizers = getattr(model_cls, "FILTER_VALUE_NORMALIZERS", {}) or {}

    specs: list[CliModelFilterSpec] = []
    for field_name, lookups in filterset_fields.items():
        for lookup in lookups:
            key = _filter_param_name(field_name, lookup)
            normalizer = (
                normalizers.get(key)
                or normalizers.get(field_name)
                or ("bool" if lookup == "isnull" else None)
            )
            specs.append(
                CliModelFilterSpec(
                    key=key,
                    field_name=field_name,
                    lookup=lookup,
                    normalizer=normalizer if isinstance(normalizer, str) else None,
                )
            )

    return tuple(sorted(specs, key=lambda spec: spec.key))


def build_cli_model_filter_rows(model_ref: type | str | None) -> list[list[str]]:
    rows: list[list[str]] = []
    for spec in get_cli_model_filter_specs(model_ref):
        rows.append([spec.key, spec.lookup, spec.value_format, spec.normalizer_label])
    return rows


def parse_cli_model_filters(
    model_ref: type | str | None,
    filter_entries: list[str] | tuple[str, ...] | None,
) -> dict[str, Any]:
    entries = [entry.strip() for entry in (filter_entries or []) if str(entry).strip()]

    if not entries:
        return {}

    specs = {spec.key: spec for spec in get_cli_model_filter_specs(model_ref)}

    if not specs:
        raise ValueError("This command does not expose additional model filters.")

    parsed: dict[str, Any] = {}
    for entry in entries:
        if "=" not in entry:
            raise ValueError(f"Invalid filter {entry!r}. Use KEY=VALUE.")

        key, raw_value = entry.split("=", 1)
        key = key.strip()
        value = raw_value.strip()

        if not key:
            raise ValueError(f"Invalid filter {entry!r}. Filter key cannot be empty.")
        if key not in specs:
            allowed = ", ".join(sorted(specs))
            raise ValueError(
                f"Unsupported filter {key!r}. Allowed filters: {allowed}."
            )
        if value == "":
            raise ValueError(f"Invalid filter {entry!r}. Filter value cannot be empty.")

        spec = specs[key]
        if spec.lookup == "in":
            items = [item.strip() for item in value.split(",") if item.strip()]
            if not items:
                raise ValueError(f"Invalid filter {entry!r}. Filter value cannot be empty.")
            parsed.setdefault(key, [])
            parsed[key].extend(items)
            continue

        if key in parsed:
            raise ValueError(f"Filter {key!r} may only be provided once.")
        parsed[key] = value

    return parsed
