from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Literal

from pydantic import Field

from ..data_models import ContractBaseModel

TABLE_VISUALS_META_KEY = "tableVisuals"

TableFrameInlineSeriesEncoding = Literal["csv-number", "json-number-array", "number-array"]
TableFrameSeriesOrder = Literal["oldest-to-newest", "newest-to-oldest"]
TableFrameVisualTone = Literal["neutral", "primary", "success", "warning", "danger"]
TableFrameVisualOperator = Literal["gt", "gte", "lt", "lte", "eq"]
TableFrameVisualBarMode = Literal["none", "fill"]
TableFrameVisualGradientMode = Literal["none", "fill"]
TableFrameVisualHeatmapPalette = Literal[
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
TableFrameVisualGaugeMode = Literal["none", "ring"]
TableFrameVisualRangeMode = Literal["auto", "fixed"]
TableFrameVisualFormulaResultFormat = Literal[
    "text",
    "datetime",
    "number",
    "currency",
    "percent",
    "bps",
]
TableFrameVisualColumnFormat = Literal[
    "number",
    "price",
    "percent",
    "volume",
    "currency",
    "datetime",
    "formula",
]
TableFrameVisualColumnKind = Literal["sparkline", "bar", "heatmap"]


class TableFrameColorScaleMetadata(ContractBaseModel):
    """Tone metadata used by table heatmap and threshold rendering."""

    negative: str | None = None
    neutral: str | None = None
    positive: str | None = None


class TableFrameRangeMetadata(ContractBaseModel):
    """Optional numeric range metadata for table visuals."""

    min: float | None = None
    max: float | None = None
    midpoint: float | None = None
    clamp: bool | None = None


class TableFrameThresholdRuleMetadata(ContractBaseModel):
    """One source-owned conditional formatting rule for a table column."""

    background_color: str | None = Field(default=None, alias="backgroundColor")
    id: str | None = None
    operator: TableFrameVisualOperator
    text_color: str | None = Field(default=None, alias="textColor")
    tone: TableFrameVisualTone | None = None
    value: float


class TableFrameVisualColumnMetadata(ContractBaseModel):
    """Source-owned display metadata for one tabular frame column."""

    label: str | None = None
    format: TableFrameVisualColumnFormat | None = None
    formula_expression: str | None = Field(default=None, alias="formulaExpression")
    formula_result_format: TableFrameVisualFormulaResultFormat | None = Field(
        default=None,
        alias="formulaResultFormat",
    )
    date_time_input_format: str | None = Field(default=None, alias="dateTimeInputFormat")
    date_time_output_format: str | None = Field(default=None, alias="dateTimeOutputFormat")
    decimals: int | None = None
    visible: bool | None = None
    color_scale: TableFrameColorScaleMetadata | None = Field(default=None, alias="colorScale")
    range: TableFrameRangeMetadata | None = None
    thresholds: list[TableFrameThresholdRuleMetadata] | None = None
    heatmap: bool | None = None
    bar_mode: TableFrameVisualBarMode | None = Field(default=None, alias="barMode")
    gradient_mode: TableFrameVisualGradientMode | None = Field(default=None, alias="gradientMode")
    heatmap_palette: TableFrameVisualHeatmapPalette | None = Field(
        default=None,
        alias="heatmapPalette",
    )
    gauge_mode: TableFrameVisualGaugeMode | None = Field(default=None, alias="gaugeMode")
    visual_range_mode: TableFrameVisualRangeMode | None = Field(
        default=None,
        alias="visualRangeMode",
    )
    visual_min: float | None = Field(default=None, alias="visualMin")
    visual_max: float | None = Field(default=None, alias="visualMax")
    kind: TableFrameVisualColumnKind | None = None
    encoding: TableFrameInlineSeriesEncoding | None = None
    order: TableFrameSeriesOrder | None = None
    width: int | None = None


class TableFrameVisualsMetadata(ContractBaseModel):
    """Frame-level source-owned table visuals metadata."""

    columns: dict[str, TableFrameVisualColumnMetadata] | None = None


TableVisualColumnInput = TableFrameVisualColumnMetadata | Mapping[str, Any]


def make_table_visual_column(**kwargs: Any) -> TableFrameVisualColumnMetadata:
    """Build source-owned display metadata for one table column."""

    return TableFrameVisualColumnMetadata(**kwargs)


def make_table_visuals(
    columns: Mapping[str, TableVisualColumnInput] | None = None,
) -> TableFrameVisualsMetadata:
    """Build the `meta.tableVisuals` metadata block consumed by table widgets."""

    normalized_columns = None
    if columns is not None:
        normalized_columns = {
            key: value
            if isinstance(value, TableFrameVisualColumnMetadata)
            else TableFrameVisualColumnMetadata(**dict(value))
            for key, value in columns.items()
        }
    return TableFrameVisualsMetadata(columns=normalized_columns)


def dump_table_visuals(
    table_visuals: TableFrameVisualsMetadata | Mapping[str, Any],
) -> dict[str, Any]:
    """Serialize table visuals to the frontend `meta.tableVisuals` shape."""

    normalized = (
        table_visuals
        if isinstance(table_visuals, TableFrameVisualsMetadata)
        else TableFrameVisualsMetadata(**dict(table_visuals))
    )
    return normalized.model_dump(mode="json", by_alias=True, exclude_none=True)


__all__ = [
    "TABLE_VISUALS_META_KEY",
    "TableFrameColorScaleMetadata",
    "TableFrameInlineSeriesEncoding",
    "TableFrameRangeMetadata",
    "TableFrameSeriesOrder",
    "TableFrameThresholdRuleMetadata",
    "TableFrameVisualBarMode",
    "TableFrameVisualColumnFormat",
    "TableFrameVisualColumnKind",
    "TableFrameVisualColumnMetadata",
    "TableFrameVisualFormulaResultFormat",
    "TableFrameVisualGaugeMode",
    "TableFrameVisualGradientMode",
    "TableFrameVisualHeatmapPalette",
    "TableFrameVisualOperator",
    "TableFrameVisualRangeMode",
    "TableFrameVisualTone",
    "TableFrameVisualsMetadata",
    "TableVisualColumnInput",
    "dump_table_visuals",
    "make_table_visual_column",
    "make_table_visuals",
]
