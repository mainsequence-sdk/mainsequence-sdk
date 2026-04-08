from __future__ import annotations

"""
Pydantic contracts for Main Sequence `Data Node`-family widgets in Command Center.

This module defines Python-side models that mirror the frontend runtime contracts
used by the following widgets inside Command Center:

- `main-sequence-data-node` (`Data Node`)
- `data-node-table-visualizer` (`Data Node Table`)

Why this file exists
--------------------
These widgets share the same upstream dataset contract but use different widget
instance props:

1. Shared upstream dataset contract
   Any compatible producer, including an App Component, can publish a tabular
   dataset into the widget graph as long as it matches the shared Data Node-family
   input shape.

2. Widget-specific props contract
   Each widget also has its own local configuration:
   - `Data Node` owns source settings, transforms, and publication behavior.
   - `Data Node Table` owns table formatting and presentation behavior.

This file is intentionally DRY:
- shared upstream types live once
- Data Node and Data Node Table reuse them
- widget-specific props are split into separate models

Command Center mental model
---------------------------
Agents should think in terms of two layers:

- `DataNodeWidgetInputResponse`
  "What dataset is being published into the widget graph?"

- `DataNodeWidgetPropsResponse`
  "How should the Data Node widget source/transform/publish that dataset?"

- `DataNodeTableWidgetPropsResponse`
  "How should the Data Node Table widget render that dataset?"

Important timestamp note
------------------------
All `*Ms` fields serialize to JavaScript epoch milliseconds for the frontend.

Python-side validation accepts:
- timezone-aware `datetime`
- ISO 8601 strings with timezone
- JavaScript epoch milliseconds

Serialization always emits:
- JavaScript epoch milliseconds
"""

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
    """Shared strict base model for Command Center widget contracts."""

    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=True,
    )


TableStatus = Literal["idle", "loading", "ready", "error"]
FieldType = Literal[
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
FieldProvenance = Literal["backend", "manual", "inferred", "derived"]
DateRangeMode = Literal["dashboard", "fixed"]


class TableFieldResponse(ContractBaseModel):
    """Field-level schema metadata for a published tabular dataset."""

    key: str = Field(
        ...,
        description="Stable field key present in `columns` and in each row object.",
    )
    label: str | None = Field(
        default=None,
        description="Optional display label for the field.",
    )
    description: str | None = Field(
        default=None,
        description="Optional human-readable field description.",
    )
    type: FieldType = Field(
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
    provenance: FieldProvenance | None = Field(
        default=None,
        description="Origin of this field schema: backend, manual, inferred, or derived.",
    )
    reason: str | None = Field(
        default=None,
        description="Optional explanation of how this field schema was produced.",
    )
    derivedFrom: list[str] | None = Field(
        default=None,
        description="Optional list of source field keys this field was derived from.",
    )
    warnings: list[str] | None = Field(
        default=None,
        description="Optional non-fatal warnings attached to the field schema.",
    )


class SourceContextResponse(ContractBaseModel):
    """Optional Main Sequence-specific metadata attached to the published source."""

    dateRangeMode: DateRangeMode | None = Field(
        default=None,
        description="How the upstream source range is resolved: dashboard-driven or fixed.",
    )
    fixedStartMs: JavaScriptEpochMsDateTime | None = Field(
        default=None,
        description="Optional fixed range start timestamp. Accepts datetime, ISO 8601 string, or JavaScript epoch milliseconds; serializes as epoch milliseconds.",
    )
    fixedEndMs: JavaScriptEpochMsDateTime | None = Field(
        default=None,
        description="Optional fixed range end timestamp. Accepts datetime, ISO 8601 string, or JavaScript epoch milliseconds; serializes as epoch milliseconds.",
    )
    uniqueIdentifierList: list[str] | None = Field(
        default=None,
        description="Optional list of upstream unique_identifier values used to filter the source dataset.",
    )
    limit: int | None = Field(
        default=None,
        ge=1,
        description="Optional upstream effective row limit.",
    )


class SourceMetadataResponse(ContractBaseModel):
    """Metadata describing where a published dataset came from."""

    kind: str = Field(
        ...,
        description="Source descriptor kind. For Data Node-family sources this is typically `main-sequence-data-node`.",
    )
    id: str | int | None = Field(
        default=None,
        description="Optional source identifier, usually the originating Data Node id.",
    )
    label: str | None = Field(
        default=None,
        description="Optional human-readable source label.",
    )
    updatedAtMs: JavaScriptEpochMsDateTime | None = Field(
        default=None,
        description="Optional source freshness timestamp. Accepts datetime, ISO 8601 string, or JavaScript epoch milliseconds; serializes as epoch milliseconds.",
    )
    context: dict[str, Any] | SourceContextResponse | None = Field(
        default=None,
        description="Optional source-specific metadata.",
    )


class DataNodeWidgetInputResponse(ContractBaseModel):
    """
    Shared upstream dataset contract accepted by Data Node-family widgets.

    This is the generic tabular-frame payload that can feed:
    - `Data Node`
    - `Data Node Table`
    - `Data Node Graph`
    - other compatible Main Sequence consumers
    """

    status: TableStatus = Field(
        ...,
        description="Loading state for the incoming tabular frame.",
    )
    error: str | None = Field(
        default=None,
        description="Optional source-scoped error message. Usually meaningful only when `status` is `error`.",
    )
    columns: list[str] = Field(
        default_factory=list,
        description="Ordered source column keys available in the dataset.",
    )
    rows: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Incoming source rows as JSON-compatible objects keyed by column name.",
    )
    fields: list[TableFieldResponse] | None = Field(
        default=None,
        description="Optional normalized field schema for the incoming dataset.",
    )
    source: SourceMetadataResponse | None = Field(
        default=None,
        description="Optional metadata describing the source of the incoming dataset.",
    )
    dataNodeId: int | None = Field(
        default=None,
        ge=1,
        description="Optional Main Sequence Data Node id associated with the incoming dataset.",
    )
    limit: int | None = Field(
        default=None,
        ge=1,
        description="Optional effective row limit attached to the incoming dataset.",
    )
    rangeStartMs: JavaScriptEpochMsDateTime | None = Field(
        default=None,
        description="Optional resolved start timestamp for the incoming dataset. Accepts datetime, ISO 8601 string, or JavaScript epoch milliseconds; serializes as epoch milliseconds.",
    )
    rangeEndMs: JavaScriptEpochMsDateTime | None = Field(
        default=None,
        description="Optional resolved end timestamp for the incoming dataset. Accepts datetime, ISO 8601 string, or JavaScript epoch milliseconds; serializes as epoch milliseconds.",
    )
    uniqueIdentifierList: list[str] | None = Field(
        default=None,
        description="Optional effective unique_identifier filter list carried with the incoming dataset.",
    )
    updatedAtMs: JavaScriptEpochMsDateTime | None = Field(
        default=None,
        description="Optional last-materialized timestamp for the incoming dataset. Accepts datetime, ISO 8601 string, or JavaScript epoch milliseconds; serializes as epoch milliseconds.",
    )


class DataNodeTableSourceInputResponse(DataNodeWidgetInputResponse):
    """
    Table-specific alias for the shared Data Node-family input contract.

    This adds no fields. It exists only so table-oriented code can reference a
    more specific name while reusing the generic `DataNodeWidgetInputResponse`.
    """

    pass


#
# Shared widget-source props
#

DataNodeWidgetSourceMode = Literal["direct", "filter_widget", "manual"]


class ManualDataNodeColumnDefinitionResponse(ContractBaseModel):
    """Manual column definition used when a Data Node is authored in manual-table mode."""

    key: str = Field(
        ...,
        description="Stable column key for a manually authored Data Node table.",
    )
    type: FieldType = Field(
        ...,
        description="Declared field type for the manually authored column.",
    )


#
# Data Node widget props contract
#

DataNodeGroupAggregateMode = Literal["first", "last", "sum", "mean", "min", "max"]
DataNodeTransformMode = Literal["none", "aggregate", "pivot", "unpivot"]
DataNodeFilterChromeMode = Literal["default", "minimal"]


class DataNodeWidgetPropsResponse(ContractBaseModel):
    """
    Full widget-instance configuration for the `Data Node` widget.

    This model represents the configuration layer of the executable Data Node
    widget itself. Unlike `Data Node Table`, this widget owns source selection,
    transforms, and publication of the canonical dataset used by downstream
    consumers in Command Center.
    """

    sourceMode: DataNodeWidgetSourceMode | None = Field(
        default=None,
        description="How this Data Node resolves its source: direct backend query, another Data Node widget, or an authored manual table.",
    )
    sourceWidgetId: str | None = Field(
        default=None,
        description="Optional upstream widget instance id when `sourceMode` is `filter_widget`.",
    )
    dataNodeId: int | None = Field(
        default=None,
        ge=1,
        description="Selected backend Data Node id when this widget is configured for direct query mode.",
    )
    dateRangeMode: DateRangeMode | None = Field(
        default=None,
        description="Whether the Data Node follows the dashboard date range or stores its own fixed range.",
    )
    fixedStartMs: JavaScriptEpochMsDateTime | None = Field(
        default=None,
        description="Optional fixed range start used when `dateRangeMode` is `fixed`.",
    )
    fixedEndMs: JavaScriptEpochMsDateTime | None = Field(
        default=None,
        description="Optional fixed range end used when `dateRangeMode` is `fixed`.",
    )
    manualColumns: list[ManualDataNodeColumnDefinitionResponse] = Field(
        default_factory=list,
        description="Manual table column definitions used when `sourceMode` is `manual`.",
    )
    manualRows: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Manual table rows used when `sourceMode` is `manual`.",
    )
    uniqueIdentifierList: list[str] | None = Field(
        default=None,
        description="Optional `unique_identifier` filter applied to the selected source.",
    )
    aggregateMode: DataNodeGroupAggregateMode | None = Field(
        default=None,
        description="Aggregate strategy used when `transformMode` is `aggregate` or `pivot`.",
    )
    chromeMode: DataNodeFilterChromeMode | None = Field(
        default=None,
        description="Visual chrome mode for the mounted Data Node token.",
    )
    keyFields: list[str] | None = Field(
        default=None,
        description="Grouping or key fields used by aggregate, pivot, and unpivot transforms.",
    )
    limit: int | None = Field(
        default=None,
        ge=1,
        description="Maximum number of source rows requested or published by this widget.",
    )
    pivotField: str | None = Field(
        default=None,
        description="Categorical field expanded into columns when `transformMode` is `pivot`.",
    )
    pivotValueField: str | None = Field(
        default=None,
        description="Value field used to fill generated pivot columns.",
    )
    projectFields: list[str] | None = Field(
        default=None,
        description="Optional final output column projection applied after transform resolution.",
    )
    showHeader: bool | None = Field(
        default=None,
        description="Whether the Data Node widget renders its header in mounted mode.",
    )
    transformMode: DataNodeTransformMode | None = Field(
        default=None,
        description="Transform mode applied before the dataset is republished downstream.",
    )
    unpivotFieldName: str | None = Field(
        default=None,
        description="Output field name that receives melted source column names in `unpivot` mode.",
    )
    unpivotValueFieldName: str | None = Field(
        default=None,
        description="Output field name that receives melted source values in `unpivot` mode.",
    )
    unpivotValueFields: list[str] | None = Field(
        default=None,
        description="Wide source columns selected for melting in `unpivot` mode.",
    )


#
# Data Node Table widget props contract
#

DataNodeTableVisualizerDateRangeMode = Literal["dashboard", "fixed"]
DataNodeTableVisualizerColumnFormat = Literal["auto", "text", "number", "currency", "percent", "bps"]
DataNodeTableVisualizerDensity = Literal["compact", "comfortable"]
DataNodeTableVisualizerBarMode = Literal["none", "fill"]
DataNodeTableVisualizerGradientMode = Literal["none", "fill"]
DataNodeTableVisualizerHeatmapPalette = Literal[
    "auto",
    "viridis",
    "plasma",
    "inferno",
    "magma",
    "turbo",
    "jet",
    "blue-white-red",
    "red-yellow-green",
]
DataNodeTableVisualizerGaugeMode = Literal["none", "ring"]
DataNodeTableVisualizerRangeMode = Literal["auto", "fixed"]
DataNodeTableVisualizerAlign = Literal["auto", "left", "center", "right"]
DataNodeTableVisualizerPinned = Literal["none", "left", "right"]
DataNodeTableVisualizerOperator = Literal["gt", "gte", "lt", "lte", "eq"]
DataNodeTableVisualizerTone = Literal["neutral", "primary", "success", "warning", "danger"]
DataNodeTableVisualizerCellValue = str | int | float | bool | None


class DataNodeTableColumnSchemaResponse(ContractBaseModel):
    """Base per-column schema used by the Data Node Table widget instance."""

    key: str = Field(
        ...,
        description="Stable column key for this table column definition.",
    )
    label: str = Field(
        ...,
        description="Display label used in the table header.",
    )
    description: str | None = Field(
        default=None,
        description="Optional description for the column.",
    )
    format: Literal["text", "number", "currency", "percent", "bps"] = Field(
        ...,
        description="Base display format for this column after schema resolution.",
    )
    minWidth: int | None = Field(
        default=None,
        ge=60,
        description="Optional minimum AG Grid column width in pixels.",
    )
    flex: float | None = Field(
        default=None,
        ge=0,
        description="Optional AG Grid flex sizing weight.",
    )
    pinned: Literal["left", "right"] | None = Field(
        default=None,
        description="Optional pinned position for the column.",
    )
    decimals: int | None = Field(
        default=None,
        ge=0,
        le=6,
        description="Optional decimal precision override for numeric display.",
    )
    prefix: str | None = Field(
        default=None,
        description="Optional string prefix added when rendering values in this column.",
    )
    suffix: str | None = Field(
        default=None,
        description="Optional string suffix added when rendering values in this column.",
    )
    categorical: bool | None = Field(
        default=None,
        description="Whether the column should be treated as categorical text-like data.",
    )
    heatmapEligible: bool | None = Field(
        default=None,
        description="Whether this column is eligible for heatmap-like numeric visuals.",
    )
    compact: bool | None = Field(
        default=None,
        description="Whether compact-number formatting should be used for this column by default.",
    )


class DataNodeTableColumnOverrideResponse(ContractBaseModel):
    """Per-column presentation override layer for the Data Node Table widget."""

    visible: bool | None = Field(
        default=None,
        description="Optional visibility override for this column.",
    )
    label: str | None = Field(
        default=None,
        description="Optional display-label override for this column.",
    )
    format: DataNodeTableVisualizerColumnFormat | None = Field(
        default=None,
        description="Optional display-format override. `auto` means fall back to schema/default inference.",
    )
    decimals: int | None = Field(
        default=None,
        ge=0,
        le=6,
        description="Optional decimal precision override for numeric rendering.",
    )
    prefix: str | None = Field(
        default=None,
        description="Optional string prefix override for cell display.",
    )
    suffix: str | None = Field(
        default=None,
        description="Optional string suffix override for cell display.",
    )
    heatmap: bool | None = Field(
        default=None,
        description="Whether heatmap rendering is enabled for this column.",
    )
    compact: bool | None = Field(
        default=None,
        description="Whether compact-number formatting is enabled for this column.",
    )
    barMode: DataNodeTableVisualizerBarMode | None = Field(
        default=None,
        description="Inline data-bar rendering mode for numeric cells.",
    )
    gradientMode: DataNodeTableVisualizerGradientMode | None = Field(
        default=None,
        description="Legacy visual fill mode used together with heatmap-style cell backgrounds.",
    )
    heatmapPalette: DataNodeTableVisualizerHeatmapPalette | None = Field(
        default=None,
        description="Heatmap palette applied when heatmap rendering is enabled.",
    )
    gaugeMode: DataNodeTableVisualizerGaugeMode | None = Field(
        default=None,
        description="Gauge visualization mode for numeric cells.",
    )
    visualRangeMode: DataNodeTableVisualizerRangeMode | None = Field(
        default=None,
        description="How numeric visual bounds are resolved: automatically from live values or from fixed limits.",
    )
    visualMin: float | None = Field(
        default=None,
        description="Optional fixed lower bound for numeric visuals when `visualRangeMode` is `fixed`.",
    )
    visualMax: float | None = Field(
        default=None,
        description="Optional fixed upper bound for numeric visuals when `visualRangeMode` is `fixed`.",
    )
    align: DataNodeTableVisualizerAlign | None = Field(
        default=None,
        description="Optional cell alignment override.",
    )
    pinned: DataNodeTableVisualizerPinned | None = Field(
        default=None,
        description="Optional pinning override for this column.",
    )


class DataNodeTableValueLabelResponse(ContractBaseModel):
    """Explicit value-to-chip mapping for categorical or text cell rendering."""

    columnKey: str = Field(
        ...,
        description="Column key this explicit value-label mapping applies to.",
    )
    value: str = Field(
        ...,
        description="Exact raw cell value that triggers this label mapping.",
    )
    label: str | None = Field(
        default=None,
        description="Optional replacement label rendered in the chip. If omitted, the raw `value` is used.",
    )
    tone: DataNodeTableVisualizerTone | None = Field(
        default=None,
        description="Optional semantic tone used for the rendered chip.",
    )
    textColor: str | None = Field(
        default=None,
        description="Optional explicit hex text color override, for example `#ffffff`.",
    )
    backgroundColor: str | None = Field(
        default=None,
        description="Optional explicit hex background color override, for example `#1f2937`.",
    )


class DataNodeTableConditionalRuleResponse(ContractBaseModel):
    """Numeric conditional formatting rule for the Data Node Table widget."""

    id: str = Field(
        ...,
        description="Stable client-side id for the rule.",
    )
    columnKey: str = Field(
        ...,
        description="Column key the rule applies to.",
    )
    operator: DataNodeTableVisualizerOperator = Field(
        ...,
        description="Comparison operator used to evaluate the numeric cell value.",
    )
    value: float = Field(
        ...,
        description="Numeric threshold compared against the cell value.",
    )
    tone: DataNodeTableVisualizerTone | None = Field(
        default=None,
        description="Optional semantic tone applied when the rule matches.",
    )
    textColor: str | None = Field(
        default=None,
        description="Optional explicit hex text color override when the rule matches.",
    )
    backgroundColor: str | None = Field(
        default=None,
        description="Optional explicit hex background color override when the rule matches.",
    )


class DataNodeTableWidgetPropsResponse(ContractBaseModel):
    """
    Full widget-instance configuration for the Command Center Data Node Table.

    This is the table's local presentation/configuration layer. It does not own
    the source dataset; it only controls how that dataset is rendered.
    """

    sourceMode: DataNodeWidgetSourceMode | None = Field(
        default=None,
        description="Optional source mode metadata. In current practice the table is a bound consumer and typically uses `filter_widget`.",
    )
    sourceWidgetId: str | None = Field(
        default=None,
        description="Optional bound upstream widget instance id. This is how the table identifies the Data Node it consumes.",
    )
    dataNodeId: int | None = Field(
        default=None,
        ge=1,
        description="Optional associated Data Node id. Usually derived from the bound source rather than authored directly.",
    )
    dateRangeMode: DataNodeTableVisualizerDateRangeMode | None = Field(
        default=None,
        description="Optional date range mode metadata. Usually inherited from the bound source.",
    )
    fixedStartMs: JavaScriptEpochMsDateTime | None = Field(
        default=None,
        description="Optional fixed range start metadata. Accepts datetime, ISO 8601 string, or JavaScript epoch milliseconds; serializes as epoch milliseconds.",
    )
    fixedEndMs: JavaScriptEpochMsDateTime | None = Field(
        default=None,
        description="Optional fixed range end metadata. Accepts datetime, ISO 8601 string, or JavaScript epoch milliseconds; serializes as epoch milliseconds.",
    )
    uniqueIdentifierList: list[str] | None = Field(
        default=None,
        description="Optional identifier filter metadata associated with the bound source.",
    )
    limit: int | None = Field(
        default=None,
        ge=1,
        description="Optional effective source row limit metadata.",
    )
    columns: list[str] = Field(
        default_factory=list,
        description="Local table frame column order after adapting the incoming source dataset.",
    )
    rows: list[list[DataNodeTableVisualizerCellValue]] = Field(
        default_factory=list,
        description="Local table frame rows. Each row is positional and must align with `columns` by index.",
    )
    schema: list[DataNodeTableColumnSchemaResponse] = Field(
        default_factory=list,
        description="Per-instance base schema used to describe and format table columns.",
    )
    density: DataNodeTableVisualizerDensity | None = Field(
        default=None,
        description="Table density setting for row height and spacing.",
    )
    showToolbar: bool | None = Field(
        default=None,
        description="Whether the table toolbar is shown.",
    )
    showSearch: bool | None = Field(
        default=None,
        description="Whether the quick-search input is shown in the toolbar.",
    )
    zebraRows: bool | None = Field(
        default=None,
        description="Whether alternating row backgrounds are enabled.",
    )
    pagination: bool | None = Field(
        default=None,
        description="Whether AG Grid pagination is enabled.",
    )
    pageSize: int | None = Field(
        default=None,
        ge=5,
        le=200,
        description="Page size used when pagination is enabled.",
    )
    columnOverrides: dict[str, DataNodeTableColumnOverrideResponse] = Field(
        default_factory=dict,
        description="Per-column presentation overrides keyed by column key.",
    )
    valueLabels: list[DataNodeTableValueLabelResponse] = Field(
        default_factory=list,
        description="Explicit value-to-chip-label mappings for categorical/text cells.",
    )
    conditionalRules: list[DataNodeTableConditionalRuleResponse] = Field(
        default_factory=list,
        description="Numeric conditional formatting rules evaluated against cell values.",
    )
