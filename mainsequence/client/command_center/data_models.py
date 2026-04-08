from __future__ import annotations

"""
Pydantic contracts for the Command Center `Data Node Table` widget.

This module describes the validated Python-side models that mirror the frontend
runtime contract used by the Main Sequence `data-node-table-visualizer` widget
inside Command Center.

How this fits into Command Center
---------------------------------
The Data Node Table widget is not a raw backend query interface. It is a UI
consumer widget that expects two layers of information:

1. An upstream tabular dataset
   This is the data published by a Data Node or any compatible widget that
   emits the shared Main Sequence tabular-frame contract. In this file, that
   upstream layer is represented by `DataNodeTableSourceInputResponse`.

2. Widget instance configuration
   This is the table-specific UI state that controls how the data should be
   rendered in Command Center: schema, labels, value formatting, heatmaps,
   pagination, rules, and other visual behaviors. In this file, that layer is
   represented by `DataNodeTableWidgetPropsResponse`.

When agents should use this file
--------------------------------
Use these models when an agent, backend service, or integration needs to:

- construct a valid payload for the Command Center Data Node Table widget
- validate incoming or outgoing widget state before sending it to the frontend
- document the exact shape of the data consumed by the table widget
- serialize Python-native datetimes into the JavaScript timestamp format that
  Command Center expects

Important timestamp note
------------------------
All `*Ms` fields in this contract are serialized for the frontend as JavaScript
epoch timestamps in milliseconds.

Python-side validation accepts:
- timezone-aware `datetime`
- ISO 8601 strings with timezone
- JavaScript epoch milliseconds

Serialization always emits:
- JavaScript epoch milliseconds

Practical mental model for agents
---------------------------------
Think of this file as two contract layers:

- `DataNodeTableSourceInputResponse`
  "What data should the table display?"

- `DataNodeTableWidgetPropsResponse`
  "How should the table display it?"

This module intentionally models the widget/runtime boundary, not the lower-level
backend fetch request used to query raw Data Node rows from the server.
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

#
# Shared timestamp adapter
# Accept:
# - timezone-aware datetime
# - ISO 8601 string with timezone
# - JavaScript epoch milliseconds
# Serialize:
# - JavaScript epoch milliseconds
#


def _parse_js_epoch_ms_datetime(value: Any) -> datetime:
    """
    Normalize incoming timestamp values into timezone-aware UTC datetimes.

    This adapter exists because Command Center expects timestamp fields in JSON
    as JavaScript epoch milliseconds, while Python producers often work with
    `datetime` instances or ISO 8601 strings.

    Accepted inputs:
    - timezone-aware `datetime`
    - ISO 8601 string with timezone
    - integer/float JavaScript epoch milliseconds

    Returns:
        A timezone-aware UTC `datetime` suitable for internal validation.
    """
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
    """
    Serialize a validated UTC-aware Python datetime into JavaScript epoch milliseconds.

    Command Center expects `*Ms` fields in the widget contract to be numeric
    millisecond timestamps, not ISO strings.
    """
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
    """
    Shared base model for Command Center widget contracts.

    Rules enforced here:
    - forbid unexpected fields so payload drift is caught early
    - allow explicit field-name population in a stable Python-friendly way

    Agents and services should inherit from this base whenever they are modeling
    frontend widget contracts for Command Center.
    """

    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=True,
    )


#
# Shared upstream source contract
#

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
    """
    Field-level schema metadata for a tabular dataset consumed by Command Center.

    This is the schema layer attached to upstream tabular data. The Data Node
    Table widget uses these field definitions to make better formatting and UI
    decisions before falling back to row-sample inference.

    In Command Center this metadata is especially useful for:
    - choosing numeric vs text formatting
    - identifying temporal fields
    - preserving backend or derived schema lineage
    - surfacing field warnings to the user
    """

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
    """
    Optional Main Sequence-specific source context attached to a published dataset.

    This is not the dataset itself. It is metadata about how that dataset was
    produced or scoped.

    Command Center uses this context to preserve source semantics such as:
    - whether the source follows the dashboard range or a fixed range
    - what fixed timestamps were used
    - what `unique_identifier` filter was applied
    - what effective row limit was used
    """

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
    """
    Metadata describing where an upstream dataset came from.

    The Data Node Table widget does not require this block to render rows, but
    Command Center can use it to preserve provenance, display source labels, and
    recover source-specific context when the dataset originated from a Main
    Sequence Data Node.

    In practice, for Data Node-family widgets:
    - `kind` is often `main-sequence-data-node`
    - `id` is often the Data Node id
    - `context` may include range mode, fixed timestamps, filters, and limit
    """

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


class DataNodeTableSourceInputResponse(ContractBaseModel):
    """
    Upstream tabular dataset consumed by the Command Center Data Node Table widget.

    This is the runtime data payload that feeds the table. It mirrors the shared
    Main Sequence tabular-frame contract published by a Data Node or another
    compatible source widget.

    This model answers:
    - what rows are available right now
    - what columns exist
    - what the current loading/error state is
    - what optional schema metadata and source provenance are attached

    In Command Center this is the primary incoming data layer for
    `data-node-table-visualizer`.
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


#
# Data Node Table widget props contract
#

DataNodeWidgetSourceMode = Literal["direct", "filter_widget", "manual"]
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
    """
    Base per-column schema used by the Data Node Table widget instance.

    This is the table's local schema layer, not the upstream dataset schema.
    Command Center uses it to define how each visible column should be presented
    once the upstream data has already been adapted into the table frame.

    This model is where the widget stores decisions such as:
    - display label
    - base format
    - width/flex behavior
    - pinning
    - numeric formatting defaults
    """

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
    """
    Per-column presentation override layer for the Data Node Table widget.

    This model sits on top of the base schema and lets Command Center persist
    instance-specific UI choices without rewriting the whole schema definition.

    Use this for widget-level presentation controls such as:
    - visibility and relabeling
    - display format override
    - heatmap/data-bar/gauge behavior
    - fixed visual ranges
    - alignment and pinning tweaks
    """

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
    """
    Explicit value-to-chip mapping for categorical or text cell rendering.

    Command Center uses these mappings to replace raw values with labeled chips
    and optional semantic coloring. This is useful for states such as:
    - Active / Watch / Critical
    - Long / Short
    - Healthy / Warning / Risk

    These mappings are instance-owned formatting rules, not upstream data.
    """

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
    """
    Numeric conditional formatting rule for the Data Node Table widget.

    Command Center evaluates these rules against numeric cell values at render
    time and applies semantic tones or explicit colors when a rule matches.

    These rules are typically used for table-level threshold semantics such as:
    - values greater than 0
    - drawdown below -5
    - utilization above 80
    """

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

    This model represents the table widget's local UI state and formatter
    settings. It is the non-data half of the contract that Command Center needs
    in order to render the table correctly once the upstream dataset has been
    resolved.

    This model includes:
    - source/binding metadata
    - legacy source-derived metadata preserved on the instance
    - the locally adapted table frame
    - schema and column overrides
    - toolbar, pagination, density, and zebra settings
    - value labels and conditional formatting rules

    Practical agent guidance
    ------------------------
    Use this model when you want to control presentation without changing the
    upstream data itself.

    Examples:
    - hide a column
    - rename a header
    - apply heatmap formatting
    - add chip labels for categorical values
    - add threshold-based warning rules
    """

    #
    # Source / binding metadata
    #
    sourceMode: DataNodeWidgetSourceMode | None = Field(
        default=None,
        description="Optional source mode metadata. In current practice the table is a bound consumer and typically uses `filter_widget`.",
    )
    sourceWidgetId: str | None = Field(
        default=None,
        description="Optional bound upstream widget instance id. This is how the table identifies the Data Node it consumes.",
    )

    #
    # Legacy / source-derived metadata
    #
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

    #
    # Local adapted frame
    #
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

    #
    # Surface controls
    #
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

    #
    # Column presentation overrides
    #
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
