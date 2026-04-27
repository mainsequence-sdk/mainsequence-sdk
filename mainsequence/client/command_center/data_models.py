"""
Canonical Pydantic contracts for Command Center tabular data consumption.

This module defines the SDK representation of `core.tabular_frame@v1`, the generic tabular frame
consumed by Command Center table, chart, statistic, curve, transform, and agent-facing data
widgets.

The canonical top-level frame contains only shared tabular contract fields:

- `status`
- `error`
- `columns`
- `rows`
- `fields`
- `meta`
- `source`

Source-specific runtime details belong in `source.context` or another explicit metadata object, not
as top-level frame fields.

Timestamp note
--------------
All `*Ms` fields serialize to JavaScript epoch milliseconds for the frontend.

Python-side validation accepts:

- timezone-aware `datetime`
- ISO 8601 strings with timezone
- JavaScript epoch milliseconds

Serialization always emits JavaScript epoch milliseconds.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated, Any, Literal

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    PlainSerializer,
    WithJsonSchema,
)

CORE_TABULAR_FRAME_CONTRACT = "core.tabular_frame@v1"


def _parse_js_epoch_ms_datetime(value: Any) -> datetime:
    """Normalize timestamp inputs into timezone-aware UTC datetimes."""
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError("Timestamp datetimes must be timezone-aware.")
        return value.astimezone(UTC)

    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value) / 1000.0, tz=UTC)

    if isinstance(value, str):
        text = value.strip()
        if not text:
            raise ValueError("Timestamp string cannot be empty.")

        if text.isdigit() or (text.startswith("-") and text[1:].isdigit()):
            return datetime.fromtimestamp(int(text) / 1000.0, tz=UTC)

        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(
                "Timestamp must be a JavaScript epoch milliseconds integer or an ISO 8601 datetime string."
            ) from exc

        if parsed.tzinfo is None:
            raise ValueError("ISO datetime timestamps must include timezone information.")

        return parsed.astimezone(UTC)

    raise TypeError(
        "Timestamp must be provided as a timezone-aware datetime, ISO 8601 string, or JavaScript epoch milliseconds."
    )


def _serialize_js_epoch_ms_datetime(value: datetime) -> int:
    """Serialize UTC-aware datetimes into JavaScript epoch milliseconds."""
    if value.tzinfo is None:
        raise ValueError("Timestamp datetimes must be timezone-aware before serialization.")
    return int(value.astimezone(UTC).timestamp() * 1000)


JavaScriptEpochMsDateTime = Annotated[
    datetime,
    BeforeValidator(_parse_js_epoch_ms_datetime),
    PlainSerializer(_serialize_js_epoch_ms_datetime, return_type=int),
    WithJsonSchema(
        {
            "oneOf": [
                {"type": "integer", "description": "JavaScript epoch timestamp in milliseconds."},
                {"type": "string", "format": "date-time", "description": "ISO 8601 datetime string with timezone."},
            ]
        },
        mode="validation",
    ),
    WithJsonSchema(
        {
            "type": "integer",
            "description": "Serialized as a JavaScript epoch timestamp in milliseconds.",
        },
        mode="serialization",
    ),
]


class ContractBaseModel(BaseModel):
    """Strict base model for Command Center SDK contracts."""

    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=True,
    )


TabularFrameStatus = Literal["idle", "loading", "ready", "error"]
TabularFrameFieldType = Literal[
    "string",
    "number",
    "integer",
    "boolean",
    "datetime",
    "date",
    "time",
    "json",
    "unknown",
]
TabularFrameFieldProvenance = Literal["backend", "manual", "inferred", "derived"]
TabularTimeSeriesShape = Literal["long", "wide"]
TabularTimeSeriesGapPolicy = Literal["preserve_nulls", "drop_nulls"]
TabularTimeSeriesDuplicatePolicy = Literal[
    "error",
    "first",
    "latest",
    "aggregate",
    "preserve",
]


class TabularFrameFieldResponse(ContractBaseModel):
    """Field-level schema metadata for a canonical tabular frame."""

    key: str = Field(
        ...,
        description="Stable field key present in `columns` and row objects.",
    )
    label: str | None = Field(
        default=None,
        description="Optional display label for the field.",
    )
    description: str | None = Field(
        default=None,
        description="Optional human-readable field description.",
    )
    type: TabularFrameFieldType = Field(
        ...,
        description="Normalized frontend field type used for formatting and schema inference.",
    )
    nullable: bool | None = Field(
        default=None,
        description="Whether the field may contain null values.",
    )
    nativeType: str | None = Field(
        default=None,
        description="Optional backend/native storage type.",
    )
    provenance: TabularFrameFieldProvenance | None = Field(
        default=None,
        description="Origin of this field schema: backend, manual, inferred, or derived.",
    )
    reason: str | None = Field(
        default=None,
        description="Optional explanation of how this field schema was produced.",
    )
    derivedFrom: list[str] | None = Field(
        default=None,
        description="Optional source field keys this field was derived from.",
    )
    warnings: list[str] | None = Field(
        default=None,
        description="Optional non-fatal warnings attached to the field schema.",
    )


class TabularFrameSourceResponse(ContractBaseModel):
    """Generic metadata describing where a canonical tabular frame came from."""

    kind: str = Field(
        ...,
        description="Source descriptor kind, such as `connection-query`, `api`, or another stable producer kind.",
    )
    id: str | int | None = Field(
        default=None,
        description="Optional source identifier.",
    )
    label: str | None = Field(
        default=None,
        description="Optional human-readable source label.",
    )
    updatedAtMs: JavaScriptEpochMsDateTime | None = Field(
        default=None,
        description="Optional source freshness timestamp. Accepts datetime, ISO 8601 string, or JavaScript epoch milliseconds; serializes as epoch milliseconds.",
    )
    context: dict[str, Any] | None = Field(
        default=None,
        description="Optional source-specific metadata. Keep source-specific fields here instead of the top-level frame.",
    )


class TabularTimeSeriesMetaResponse(ContractBaseModel):
    """Time-series metadata for tabular frames consumed by chart and curve widgets."""

    shape: TabularTimeSeriesShape = Field(
        ...,
        description="Whether time-series rows are encoded in long or wide shape.",
    )
    timeField: str = Field(
        ...,
        description="Field key that contains the timestamp or time index.",
    )
    timeUnit: Literal["ms"] = Field(
        default="ms",
        description="Time unit used by the frontend. Command Center expects milliseconds.",
    )
    timezone: Literal["UTC"] = Field(
        default="UTC",
        description="Timezone used for time-field interpretation.",
    )
    sorted: bool = Field(
        ...,
        description="Whether rows are sorted by the time field.",
    )
    valueField: str | None = Field(
        default=None,
        description="Value field for long-shaped single-series data.",
    )
    seriesField: str | None = Field(
        default=None,
        description="Field that identifies the series in long-shaped multi-series data.",
    )
    seriesLabelFields: list[str] | None = Field(
        default=None,
        description="Optional fields used to build display labels for each series.",
    )
    valueFields: list[str] | None = Field(
        default=None,
        description="Value fields for wide-shaped data.",
    )
    frequency: str | None = Field(
        default=None,
        description="Optional declared sampling frequency.",
    )
    calendar: str | None = Field(
        default=None,
        description="Optional calendar identifier for market or business-time data.",
    )
    gapPolicy: TabularTimeSeriesGapPolicy | None = Field(
        default=None,
        description="How missing values should be handled by consumers.",
    )
    duplicatePolicy: TabularTimeSeriesDuplicatePolicy | None = Field(
        default=None,
        description="How duplicate time keys should be handled by consumers.",
    )
    unitByField: dict[str, str] | None = Field(
        default=None,
        description="Optional display unit metadata keyed by field.",
    )


class TabularFrameMetaResponse(ContractBaseModel):
    """Frame-level metadata for `core.tabular_frame@v1`."""

    model_config = ConfigDict(
        extra="allow",
        populate_by_name=True,
    )

    timeSeries: TabularTimeSeriesMetaResponse | None = Field(
        default=None,
        description="Optional time-series hints for chart, curve, and time-aware consumers.",
    )


class TabularFrameResponse(ContractBaseModel):
    """Canonical SDK response model for `core.tabular_frame@v1`."""

    status: TabularFrameStatus = Field(
        ...,
        description="Loading state for the tabular frame.",
    )
    error: str | None = Field(
        default=None,
        description="Optional error message. Usually meaningful only when `status` is `error`.",
    )
    columns: list[str] = Field(
        ...,
        description="Ordered field keys available in the frame.",
    )
    rows: list[dict[str, Any]] = Field(
        ...,
        description="Tabular rows as JSON-compatible objects keyed by column name.",
    )
    fields: list[TabularFrameFieldResponse] | None = Field(
        default=None,
        description="Optional normalized field schema for the frame.",
    )
    meta: TabularFrameMetaResponse | None = Field(
        default=None,
        description="Optional frame-level metadata, including time-series hints.",
    )
    source: TabularFrameSourceResponse | None = Field(
        default=None,
        description="Optional generic source metadata for the frame.",
    )
