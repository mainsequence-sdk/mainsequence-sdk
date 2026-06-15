from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Literal

from pydantic import ConfigDict, Field, model_validator

from ..data_models import ContractBaseModel
from ..workspaces.mounted_widgets import make_mounted_widget_payload

CORE_TABLE_WIDGET_ID = "core__table"
CORE_PRO_TABLE_WIDGET_ID = "core__pro-table"

TableWidgetDateRangeMode = Literal["dashboard", "fixed"]
TableWidgetSourceMode = Literal["bound", "connection", "connection-stream", "manual"]
TableWidgetColumnFormat = Literal[
    "auto",
    "text",
    "datetime",
    "number",
    "currency",
    "percent",
    "bps",
    "formula",
]
TableWidgetColumnSchemaFormat = Literal[
    "text",
    "datetime",
    "number",
    "currency",
    "percent",
    "bps",
    "formula",
]
TableWidgetFormulaResultFormat = Literal["text", "datetime", "number", "currency", "percent", "bps"]
TableWidgetDensity = Literal["compact", "comfortable"]
TableWidgetBarMode = Literal["none", "fill"]
TableWidgetGradientMode = Literal["none", "fill"]
TableWidgetHeatmapPalette = Literal[
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
TableWidgetGaugeMode = Literal["none", "ring"]
TableWidgetRangeMode = Literal["auto", "fixed"]
TableWidgetAlign = Literal["auto", "left", "center", "right"]
TableWidgetPinned = Literal["none", "left", "right"]
TableWidgetPinnedColumn = Literal["left", "right"]
TableWidgetOperator = Literal["gt", "gte", "lt", "lte", "eq"]
TableWidgetTone = Literal["neutral", "primary", "success", "warning", "danger"]
TableWidgetSelectionMode = Literal["none", "single-row", "multi-row", "cell"]


class TableWidgetColumnSchema(ContractBaseModel):
    """Persisted schema configuration for one table widget column."""

    key: str
    label: str
    description: str | None = None
    format: TableWidgetColumnSchemaFormat
    formula_expression: str | None = Field(default=None, alias="formulaExpression")
    formula_result_format: TableWidgetFormulaResultFormat | None = Field(
        default=None,
        alias="formulaResultFormat",
    )
    min_width: int | None = Field(default=None, alias="minWidth")
    flex: int | None = None
    pinned: TableWidgetPinnedColumn | None = None
    decimals: int | None = None
    prefix: str | None = None
    suffix: str | None = None
    date_time_input_format: str | None = Field(default=None, alias="dateTimeInputFormat")
    date_time_output_format: str | None = Field(default=None, alias="dateTimeOutputFormat")
    categorical: bool | None = None
    heatmap_eligible: bool | None = Field(default=None, alias="heatmapEligible")
    compact: bool | None = None

    @model_validator(mode="after")
    def _validate_formula_column(self) -> TableWidgetColumnSchema:
        if self.format == "formula" and not self.formula_expression:
            raise ValueError("Formula columns require formula_expression.")
        if self.formula_expression and self.format != "formula":
            raise ValueError("formula_expression is only valid when format='formula'.")
        return self


class TableWidgetColumnOverride(ContractBaseModel):
    """Persisted user/display overrides for one table widget column."""

    visible: bool | None = None
    label: str | None = None
    format: TableWidgetColumnFormat | None = None
    decimals: int | None = None
    prefix: str | None = None
    suffix: str | None = None
    date_time_input_format: str | None = Field(default=None, alias="dateTimeInputFormat")
    date_time_output_format: str | None = Field(default=None, alias="dateTimeOutputFormat")
    heatmap: bool | None = None
    compact: bool | None = None
    bar_mode: TableWidgetBarMode | None = Field(default=None, alias="barMode")
    gradient_mode: TableWidgetGradientMode | None = Field(default=None, alias="gradientMode")
    heatmap_palette: TableWidgetHeatmapPalette | None = Field(default=None, alias="heatmapPalette")
    gauge_mode: TableWidgetGaugeMode | None = Field(default=None, alias="gaugeMode")
    visual_range_mode: TableWidgetRangeMode | None = Field(default=None, alias="visualRangeMode")
    visual_min: float | None = Field(default=None, alias="visualMin")
    visual_max: float | None = Field(default=None, alias="visualMax")
    align: TableWidgetAlign | None = None
    pinned: TableWidgetPinned | None = None


class TableWidgetValueLabel(ContractBaseModel):
    """Display label replacement for a discrete table value."""

    column_key: str = Field(alias="columnKey")
    value: str
    label: str | None = None
    tone: TableWidgetTone | None = None
    text_color: str | None = Field(default=None, alias="textColor")
    background_color: str | None = Field(default=None, alias="backgroundColor")


class TableWidgetConditionalRule(ContractBaseModel):
    """One persisted conditional formatting rule for a table widget."""

    id: str
    column_key: str = Field(alias="columnKey")
    operator: TableWidgetOperator
    value: float
    tone: TableWidgetTone | None = None
    text_color: str | None = Field(default=None, alias="textColor")
    background_color: str | None = Field(default=None, alias="backgroundColor")


class TableLiveMergeKeyMapping(ContractBaseModel):
    """Maps seed-frame fields to live-update fields for incremental table updates."""

    seed_field: str = Field(alias="seedField")
    live_field: str = Field(alias="liveField")

    @model_validator(mode="after")
    def _validate_fields(self) -> TableLiveMergeKeyMapping:
        if not self.seed_field.strip() or not self.live_field.strip():
            raise ValueError("Live merge mappings require non-empty seed_field and live_field.")
        return self


class TableWidgetProps(ContractBaseModel):
    """Persisted props for the shared Command Center table/pro-table runtime."""

    model_config = ConfigDict(
        extra="allow",
        populate_by_name=True,
    )

    table_source_mode: TableWidgetSourceMode | None = Field(default=None, alias="tableSourceMode")
    embedded_connection_presentation: dict[str, Any] | None = Field(
        default=None,
        alias="embeddedConnectionPresentation",
    )
    embedded_connection_query: dict[str, Any] | None = Field(
        default=None,
        alias="embeddedConnectionQuery",
    )
    source_id: int | None = Field(default=None, alias="sourceId")
    date_range_mode: TableWidgetDateRangeMode | None = Field(default=None, alias="dateRangeMode")
    fixed_end_ms: int | None = Field(default=None, alias="fixedEndMs")
    fixed_start_ms: int | None = Field(default=None, alias="fixedStartMs")
    unique_identifier_list: list[str] | None = Field(default=None, alias="uniqueIdentifierList")
    manual_columns: list[dict[str, Any]] | None = Field(default=None, alias="manualColumns")
    manual_rows: list[dict[str, Any]] | None = Field(default=None, alias="manualRows")
    limit: int | None = None
    columns: list[str] | None = None
    rows: list[list[Any]] | None = None
    column_schema: list[TableWidgetColumnSchema] | None = Field(default=None, alias="schema")
    density: TableWidgetDensity | None = None
    group_by: str | None = Field(default=None, alias="groupBy")
    show_toolbar: bool | None = Field(default=None, alias="showToolbar")
    show_search: bool | None = Field(default=None, alias="showSearch")
    show_column_filters: bool | None = Field(default=None, alias="showColumnFilters")
    zebra_rows: bool | None = Field(default=None, alias="zebraRows")
    pagination: bool | None = None
    page_size: int | None = Field(default=None, alias="pageSize")
    column_overrides: dict[str, TableWidgetColumnOverride] | None = Field(
        default=None,
        alias="columnOverrides",
    )
    value_labels: list[TableWidgetValueLabel] | None = Field(default=None, alias="valueLabels")
    conditional_rules: list[TableWidgetConditionalRule] | None = Field(
        default=None,
        alias="conditionalRules",
    )
    formulas_enabled: bool | None = Field(default=None, alias="formulasEnabled")
    live_merge_key_mappings: list[TableLiveMergeKeyMapping] | None = Field(
        default=None,
        alias="liveMergeKeyMappings",
    )
    selection_mode: TableWidgetSelectionMode | None = Field(default=None, alias="selectionMode")
    selection_key_fields: list[str] | None = Field(default=None, alias="selectionKeyFields")
    publish_selection_outputs: bool | None = Field(default=None, alias="publishSelectionOutputs")

    @model_validator(mode="after")
    def _validate_table_props(self) -> TableWidgetProps:
        if self.column_schema is not None:
            schema_keys = [column.key for column in self.column_schema]
            if len(schema_keys) != len(set(schema_keys)):
                raise ValueError("Table schema column keys must be unique.")

        if self.conditional_rules is not None:
            rule_ids = [rule.id for rule in self.conditional_rules]
            if len(rule_ids) != len(set(rule_ids)):
                raise ValueError("Table conditional rule ids must be unique.")

        if self.column_schema is not None and self.conditional_rules is not None:
            schema_keys = {column.key for column in self.column_schema}
            missing_rule_columns = [
                rule.column_key
                for rule in self.conditional_rules
                if rule.column_key not in schema_keys
            ]
            if missing_rule_columns:
                missing = ", ".join(sorted(set(missing_rule_columns)))
                raise ValueError(f"Conditional rules reference unknown schema columns: {missing}.")

        return self


TableWidgetPropsInput = TableWidgetProps | Mapping[str, Any]


def formula_expression_field(field: str) -> str:
    """Return frontend table formula syntax for one field reference."""

    normalized = field.strip()
    if not normalized:
        raise ValueError("Formula field names cannot be empty.")
    return f"[{normalized}]"


def formula_percent_change(current_field: str, reference_field: str) -> str:
    return f"PERCENT_CHANGE({formula_expression_field(current_field)}, {formula_expression_field(reference_field)})"


def formula_difference(left_field: str, right_field: str) -> str:
    return f"DIFFERENCE({formula_expression_field(left_field)}, {formula_expression_field(right_field)})"


def formula_ratio(numerator_field: str, denominator_field: str) -> str:
    return f"RATIO({formula_expression_field(numerator_field)}, {formula_expression_field(denominator_field)})"


def formula_add(*fields: str) -> str:
    if not fields:
        raise ValueError("ADD formulas require at least one field.")
    return f"ADD({', '.join(formula_expression_field(field) for field in fields)})"


def formula_multiply(*fields: str) -> str:
    if not fields:
        raise ValueError("MULTIPLY formulas require at least one field.")
    return f"MULTIPLY({', '.join(formula_expression_field(field) for field in fields)})"


def make_formula_column(
    *,
    key: str,
    label: str,
    expression: str,
    result_format: TableWidgetFormulaResultFormat = "number",
    decimals: int | None = None,
    suffix: str | None = None,
    prefix: str | None = None,
) -> TableWidgetColumnSchema:
    """Build a pro-table formula column schema entry."""

    return TableWidgetColumnSchema(
        key=key,
        label=label,
        format="formula",
        formula_expression=expression,
        formula_result_format=result_format,
        decimals=decimals,
        suffix=suffix,
        prefix=prefix,
    )


def make_table_widget_props(
    props: TableWidgetPropsInput | None = None, **kwargs: Any
) -> TableWidgetProps:
    """Normalize typed or mapping-based table props into the SDK model."""

    if props is not None and kwargs:
        raise ValueError("Pass either props or keyword table props, not both.")
    if isinstance(props, TableWidgetProps):
        return props
    return TableWidgetProps(**dict(props or kwargs))


def dump_table_widget_props(
    props: TableWidgetPropsInput | None = None, **kwargs: Any
) -> dict[str, Any]:
    """Serialize table props to the frontend widget prop shape."""

    normalized = make_table_widget_props(props, **kwargs)
    return normalized.model_dump(mode="json", by_alias=True, exclude_none=True)


def _props_have_formula_columns(props: TableWidgetProps) -> bool:
    return any(
        column.format == "formula" or bool(column.formula_expression)
        for column in (props.column_schema or [])
    )


def _validate_table_edition(props: TableWidgetProps, *, pro: bool) -> None:
    has_formula_columns = _props_have_formula_columns(props)
    if has_formula_columns and not pro:
        raise ValueError("Formula columns require the pro table widget.")
    if props.formulas_enabled and not pro:
        raise ValueError("formulas_enabled=True requires the pro table widget.")
    if has_formula_columns and props.formulas_enabled is False:
        raise ValueError("Formula columns require formulas_enabled to be true or unset.")


def _make_table_payload(
    *,
    widget_type_id: str,
    pro: bool,
    widget_uid: str | None = None,
    title: str | None = None,
    props: TableWidgetPropsInput | None = None,
    layout: Mapping[str, Any] | None = None,
    bindings: Mapping[str, Any] | list[Mapping[str, Any]] | None = None,
    metadata: Mapping[str, Any] | None = None,
    **props_kwargs: Any,
) -> dict[str, Any]:
    normalized_props = make_table_widget_props(props, **props_kwargs)
    if pro and normalized_props.formulas_enabled is None:
        normalized_props.formulas_enabled = True
    elif not pro and normalized_props.formulas_enabled is None:
        normalized_props.formulas_enabled = False

    _validate_table_edition(normalized_props, pro=pro)

    return make_mounted_widget_payload(
        widget_type_id=widget_type_id,
        widget_uid=widget_uid,
        title=title,
        props=normalized_props.model_dump(mode="json", by_alias=True, exclude_none=True),
        layout=layout,
        bindings=bindings,
        metadata=metadata,
    )


def make_table_widget_payload(
    *,
    widget_uid: str | None = None,
    title: str | None = None,
    props: TableWidgetPropsInput | None = None,
    layout: Mapping[str, Any] | None = None,
    bindings: Mapping[str, Any] | list[Mapping[str, Any]] | None = None,
    metadata: Mapping[str, Any] | None = None,
    **props_kwargs: Any,
) -> dict[str, Any]:
    """Build a mounted normal-table widget payload.

    Normal table supports formatting, selection, and display props. Formula
    columns are intentionally rejected so users do not create a widget that the
    community table runtime cannot execute.
    """

    return _make_table_payload(
        widget_type_id=CORE_TABLE_WIDGET_ID,
        pro=False,
        widget_uid=widget_uid,
        title=title,
        props=props,
        layout=layout,
        bindings=bindings,
        metadata=metadata,
        **props_kwargs,
    )


def make_pro_table_widget_payload(
    *,
    widget_uid: str | None = None,
    title: str | None = None,
    props: TableWidgetPropsInput | None = None,
    layout: Mapping[str, Any] | None = None,
    bindings: Mapping[str, Any] | list[Mapping[str, Any]] | None = None,
    metadata: Mapping[str, Any] | None = None,
    **props_kwargs: Any,
) -> dict[str, Any]:
    """Build a mounted pro-table widget payload with formula support enabled."""

    return _make_table_payload(
        widget_type_id=CORE_PRO_TABLE_WIDGET_ID,
        pro=True,
        widget_uid=widget_uid,
        title=title,
        props=props,
        layout=layout,
        bindings=bindings,
        metadata=metadata,
        **props_kwargs,
    )


__all__ = [
    "CORE_PRO_TABLE_WIDGET_ID",
    "CORE_TABLE_WIDGET_ID",
    "TableLiveMergeKeyMapping",
    "TableWidgetAlign",
    "TableWidgetBarMode",
    "TableWidgetColumnFormat",
    "TableWidgetColumnOverride",
    "TableWidgetColumnSchema",
    "TableWidgetColumnSchemaFormat",
    "TableWidgetConditionalRule",
    "TableWidgetDateRangeMode",
    "TableWidgetDensity",
    "TableWidgetFormulaResultFormat",
    "TableWidgetGaugeMode",
    "TableWidgetGradientMode",
    "TableWidgetHeatmapPalette",
    "TableWidgetOperator",
    "TableWidgetPinned",
    "TableWidgetPinnedColumn",
    "TableWidgetProps",
    "TableWidgetPropsInput",
    "TableWidgetRangeMode",
    "TableWidgetSelectionMode",
    "TableWidgetSourceMode",
    "TableWidgetTone",
    "TableWidgetValueLabel",
    "dump_table_widget_props",
    "formula_add",
    "formula_difference",
    "formula_expression_field",
    "formula_multiply",
    "formula_percent_change",
    "formula_ratio",
    "make_formula_column",
    "make_pro_table_widget_payload",
    "make_table_widget_payload",
    "make_table_widget_props",
]
