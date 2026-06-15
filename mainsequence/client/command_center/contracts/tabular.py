from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, datetime
from typing import Any

from ..data_models import (
    CORE_TABULAR_FRAME_CONTRACT,
    ContractBaseModel,
    TabularFrameFieldProvenance,
    TabularFrameFieldResponse,
    TabularFrameFieldType,
    TabularFrameMetaResponse,
    TabularFrameResponse,
    TabularFrameSourceResponse,
    TabularFrameStatus,
    TabularTimeSeriesMetaResponse,
)

TabularRow = Mapping[str, Any]
TabularFieldInput = TabularFrameFieldResponse | Mapping[str, Any]


def normalize_tabular_columns(
    rows: Sequence[TabularRow],
    columns: Sequence[str] | None = None,
) -> list[str]:
    """Return stable frame columns without adding domain-specific fields."""

    if columns is not None:
        return [str(column) for column in columns]

    ordered: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row.keys():
            column = str(key)
            if column not in seen:
                seen.add(column)
                ordered.append(column)
    return ordered


def infer_tabular_field_type(value: Any) -> TabularFrameFieldType:
    """Infer a generic Command Center tabular field type from a Python value."""

    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int) and not isinstance(value, bool):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, datetime):
        return "datetime"
    if isinstance(value, date):
        return "date"
    if isinstance(value, (dict, list, tuple)):
        return "json"
    return "string"


def make_tabular_field(
    key: str,
    *,
    label: str | None = None,
    type: TabularFrameFieldType | str = "string",
    description: str | None = None,
    nullable: bool | None = True,
    native_type: str | None = None,
    provenance: TabularFrameFieldProvenance | str | None = None,
    derived_from: Sequence[str] | None = None,
) -> TabularFrameFieldResponse:
    """Build a generic field descriptor for core.tabular_frame@v1."""

    return TabularFrameFieldResponse(
        key=key,
        label=label,
        description=description,
        type=type,
        nullable=nullable,
        nativeType=native_type,
        provenance=provenance,
        derivedFrom=list(derived_from) if derived_from is not None else None,
    )


def build_tabular_field(
    *,
    key: str,
    field_type: TabularFrameFieldType | str,
    label: str | None = None,
    description: str | None = None,
    nullable: bool | None = None,
    native_type: str | None = None,
    provenance: TabularFrameFieldProvenance | str | None = "backend",
    derived_from: Sequence[str] | None = None,
) -> TabularFrameFieldResponse:
    """Compatibility alias using the historical field_type argument name."""

    return make_tabular_field(
        key,
        label=label,
        type=field_type,
        description=description,
        nullable=nullable,
        native_type=native_type,
        provenance=provenance,
        derived_from=derived_from,
    )


def infer_tabular_fields(
    rows: Sequence[TabularRow],
    columns: Sequence[str] | None = None,
) -> list[TabularFrameFieldResponse]:
    """Infer field descriptors from rows using only generic value types."""

    normalized_columns = normalize_tabular_columns(rows, columns)
    fields: list[TabularFrameFieldResponse] = []
    for column in normalized_columns:
        sample = None
        for row in rows:
            value = row.get(column)
            if value is not None:
                sample = value
                break
        fields.append(
            make_tabular_field(
                column,
                type=infer_tabular_field_type(sample),
                provenance="inferred",
            )
        )
    return fields


def make_tabular_source(
    kind: str,
    *,
    id: str | None = None,
    label: str | None = None,
    context: Mapping[str, Any] | None = None,
) -> TabularFrameSourceResponse:
    """Build a source descriptor while keeping runtime details in context."""

    return TabularFrameSourceResponse(
        kind=kind,
        id=id,
        label=label,
        context=dict(context) if context is not None else None,
    )


def make_time_series_meta(
    *,
    shape: str = "long",
    time_field: str,
    value_field: str | None = None,
    entity_field: str | None = None,
    sorted: bool | None = None,
    time_unit: str | None = None,
    timezone: str | None = None,
) -> TabularTimeSeriesMetaResponse:
    """Build the time-series metadata block used by time-aware tabular consumers."""

    payload: dict[str, Any] = {
        "shape": shape,
        "timeField": time_field,
    }
    if value_field is not None:
        payload["valueField"] = value_field
    if entity_field is not None:
        payload["entityField"] = entity_field
    if sorted is not None:
        payload["sorted"] = sorted
    if time_unit is not None:
        payload["timeUnit"] = time_unit
    if timezone is not None:
        payload["timezone"] = timezone
    return TabularTimeSeriesMetaResponse(**payload)


def make_tabular_frame(
    *,
    rows: Sequence[TabularRow],
    columns: Sequence[str] | None = None,
    fields: Sequence[TabularFieldInput] | None = None,
    status: TabularFrameStatus | str = "ready",
    error: str | None = None,
    meta: TabularFrameMetaResponse | Mapping[str, Any] | None = None,
    source: TabularFrameSourceResponse | Mapping[str, Any] | None = None,
    infer_fields: bool = False,
) -> TabularFrameResponse:
    """Build a canonical core.tabular_frame@v1 response.

    This helper is intentionally domain-neutral: it never adds identity, symbol,
    ticker, or widget-specific columns.
    """

    row_dicts = [dict(row) for row in rows]
    normalized_columns = normalize_tabular_columns(row_dicts, columns)

    normalized_fields: list[TabularFrameFieldResponse] | None = None
    if fields is not None:
        normalized_fields = [
            field
            if isinstance(field, TabularFrameFieldResponse)
            else TabularFrameFieldResponse.model_validate(field)
            for field in fields
        ]
    elif infer_fields:
        normalized_fields = infer_tabular_fields(row_dicts, normalized_columns)

    normalized_meta = None
    if meta is not None:
        normalized_meta = (
            meta if isinstance(meta, TabularFrameMetaResponse) else TabularFrameMetaResponse.model_validate(meta)
        )

    normalized_source = None
    if source is not None:
        normalized_source = (
            source
            if isinstance(source, TabularFrameSourceResponse)
            else TabularFrameSourceResponse.model_validate(source)
        )

    return TabularFrameResponse(
        status=status,
        error=error,
        columns=normalized_columns,
        rows=row_dicts,
        fields=normalized_fields,
        meta=normalized_meta,
        source=normalized_source,
    )


def build_tabular_frame(
    *,
    columns: Sequence[str],
    rows: Sequence[TabularRow],
    fields: Sequence[TabularFieldInput] | None = None,
    meta: TabularFrameMetaResponse | Mapping[str, Any] | None = None,
    source: TabularFrameSourceResponse | Mapping[str, Any] | None = None,
    status: TabularFrameStatus | str = "ready",
    error: str | None = None,
) -> TabularFrameResponse:
    """Compatibility alias for project helpers that used build_* names."""

    return make_tabular_frame(
        columns=columns,
        rows=rows,
        fields=fields,
        meta=meta,
        source=source,
        status=status,
        error=error,
    )


__all__ = [
    "CORE_TABULAR_FRAME_CONTRACT",
    "ContractBaseModel",
    "TabularFieldInput",
    "TabularFrameFieldProvenance",
    "TabularFrameFieldResponse",
    "TabularFrameFieldType",
    "TabularFrameMetaResponse",
    "TabularFrameResponse",
    "TabularFrameSourceResponse",
    "TabularFrameStatus",
    "TabularRow",
    "TabularTimeSeriesMetaResponse",
    "build_tabular_field",
    "build_tabular_frame",
    "infer_tabular_field_type",
    "infer_tabular_fields",
    "make_tabular_field",
    "make_tabular_frame",
    "make_tabular_source",
    "make_time_series_meta",
    "normalize_tabular_columns",
]
