from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Literal

from pydantic import ConfigDict, Field, model_validator

from ..data_models import ContractBaseModel
from ..workspaces.mounted_widgets import make_mounted_widget_payload
from .table import TableLiveMergeKeyMapping

CORE_TABULAR_TRANSFORM_WIDGET_ID = "core__tabular-transform"

TABULAR_TRANSFORM_SEED_INPUT_ID = "seedData"
TABULAR_TRANSFORM_LIVE_UPDATES_INPUT_ID = "liveUpdates"

TABULAR_TRANSFORM_DATASET_OUTPUT_ID = "dataset"
TABULAR_TRANSFORM_UPDATES_OUTPUT_ID = "updates"

TabularTransformMode = Literal["none", "filter", "aggregate", "pivot", "unpivot"]
TabularAggregateMode = Literal["first", "last", "sum", "mean", "min", "max"]
TabularFilterCombineMode = Literal["all", "any"]
TabularTransformRowMergeMode = Literal["passthrough", "latest"]
TabularTransformComputedColumnType = Literal["number", "string", "boolean", "json"]
TabularFilterOperator = Literal[
    "equals",
    "not-equals",
    "in",
    "not-in",
    "gt",
    "gte",
    "lt",
    "lte",
    "is-empty",
    "is-not-empty",
]
TabularFilterRuleValue = str | int | float | bool | None


class TabularFilterRule(ContractBaseModel):
    """One lightweight row predicate for the tabular transform widget."""

    field: str
    operator: TabularFilterOperator
    value: TabularFilterRuleValue | list[TabularFilterRuleValue] | None = None

    @model_validator(mode="after")
    def _validate_rule(self) -> TabularFilterRule:
        if not self.field.strip():
            raise ValueError("Filter rules require a non-empty field.")
        if self.operator in {"in", "not-in"}:
            if not isinstance(self.value, list) or not self.value:
                raise ValueError(f"{self.operator} filter rules require a non-empty list value.")
        elif self.operator not in {"is-empty", "is-not-empty"} and self.value is None:
            raise ValueError(f"{self.operator} filter rules require a value.")
        return self


class TabularTransformComputedColumn(ContractBaseModel):
    """One computed output column materialized by the tabular transform."""

    key: str
    label: str | None = None
    type: TabularTransformComputedColumnType = "number"
    formula_expression: str = Field(alias="formulaExpression")

    @model_validator(mode="after")
    def _validate_computed_column(self) -> TabularTransformComputedColumn:
        if not self.key.strip():
            raise ValueError("Computed columns require a non-empty key.")
        if not self.formula_expression.strip():
            raise ValueError("Computed columns require a non-empty formula_expression.")
        return self


class TabularTransformProps(ContractBaseModel):
    """Persisted props for the core tabular-transform widget."""

    model_config = ConfigDict(
        extra="allow",
        populate_by_name=True,
    )

    transform_mode: TabularTransformMode = Field(default="none", alias="transformMode")
    aggregate_mode: TabularAggregateMode = Field(default="last", alias="aggregateMode")
    computed_columns: list[TabularTransformComputedColumn] | None = Field(
        default=None,
        alias="computedColumns",
    )
    filter_combine_mode: TabularFilterCombineMode = Field(default="all", alias="filterCombineMode")
    filter_rules: list[TabularFilterRule] | None = Field(default=None, alias="filterRules")
    key_fields: list[str] | None = Field(default=None, alias="keyFields")
    pivot_field: str | None = Field(default=None, alias="pivotField")
    pivot_value_field: str | None = Field(default=None, alias="pivotValueField")
    project_fields: list[str] | None = Field(default=None, alias="projectFields")
    row_merge_key_fields: list[str] | None = Field(default=None, alias="rowMergeKeyFields")
    row_merge_key_mappings: list[TableLiveMergeKeyMapping] | None = Field(
        default=None,
        alias="rowMergeKeyMappings",
    )
    row_merge_mode: TabularTransformRowMergeMode = Field(
        default="passthrough",
        alias="rowMergeMode",
    )
    unpivot_field_name: str = Field(default="series", alias="unpivotFieldName")
    unpivot_value_field_name: str = Field(default="value", alias="unpivotValueFieldName")
    unpivot_value_fields: list[str] | None = Field(default=None, alias="unpivotValueFields")

    @model_validator(mode="after")
    def _validate_props(self) -> TabularTransformProps:
        if self.computed_columns is not None:
            keys = [column.key for column in self.computed_columns]
            if len(keys) != len(set(keys)):
                raise ValueError("Computed column keys must be unique.")

        if self.transform_mode == "aggregate" and not self.key_fields:
            raise ValueError("Aggregate transforms require at least one key field.")

        if self.transform_mode == "filter" and not self.filter_rules:
            raise ValueError("Filter transforms require at least one filter rule.")

        if self.transform_mode == "pivot":
            if not self.pivot_field:
                raise ValueError("Pivot transforms require pivot_field.")
            if not self.pivot_value_field:
                raise ValueError("Pivot transforms require pivot_value_field.")

        if self.transform_mode == "unpivot" and not self.unpivot_value_fields:
            raise ValueError("Unpivot transforms require at least one unpivot value field.")

        if self.row_merge_mode == "latest":
            has_key_fields = bool(self.row_merge_key_fields)
            has_key_mappings = bool(self.row_merge_key_mappings)
            if not has_key_fields and not has_key_mappings:
                raise ValueError(
                    "Latest-row merge requires row_merge_key_fields or row_merge_key_mappings."
                )

        return self


TabularTransformPropsInput = TabularTransformProps | Mapping[str, Any]


def make_tabular_transform_computed_column(
    *,
    key: str,
    formula_expression: str,
    label: str | None = None,
    type: TabularTransformComputedColumnType = "number",
) -> TabularTransformComputedColumn:
    """Build one computed column config using the shared table formula language."""

    return TabularTransformComputedColumn(
        key=key,
        label=label,
        type=type,
        formula_expression=formula_expression,
    )


def make_tabular_filter_rule(
    *,
    field: str,
    operator: TabularFilterOperator,
    value: TabularFilterRuleValue | list[TabularFilterRuleValue] | None = None,
) -> TabularFilterRule:
    """Build one tabular transform filter rule."""

    return TabularFilterRule(field=field, operator=operator, value=value)


def make_tabular_transform_props(
    props: TabularTransformPropsInput | None = None,
    **kwargs: Any,
) -> TabularTransformProps:
    """Normalize typed or mapping-based tabular transform props into the SDK model."""

    if props is not None and kwargs:
        raise ValueError("Pass either props or keyword tabular transform props, not both.")
    if isinstance(props, TabularTransformProps):
        return props
    return TabularTransformProps(**dict(props or kwargs))


def dump_tabular_transform_props(
    props: TabularTransformPropsInput | None = None,
    **kwargs: Any,
) -> dict[str, Any]:
    """Serialize tabular transform props to the frontend widget prop shape."""

    normalized = make_tabular_transform_props(props, **kwargs)
    return normalized.model_dump(mode="json", by_alias=True, exclude_none=True)


def make_tabular_transform_widget_payload(
    *,
    widget_uid: str | None = None,
    title: str | None = None,
    props: TabularTransformPropsInput | None = None,
    layout: Mapping[str, Any] | None = None,
    bindings: Mapping[str, Any] | list[Mapping[str, Any]] | None = None,
    metadata: Mapping[str, Any] | None = None,
    **props_kwargs: Any,
) -> dict[str, Any]:
    """Build a mounted core tabular-transform widget payload."""

    normalized_props = make_tabular_transform_props(props, **props_kwargs)
    return make_mounted_widget_payload(
        widget_type_id=CORE_TABULAR_TRANSFORM_WIDGET_ID,
        widget_uid=widget_uid,
        title=title,
        props=normalized_props.model_dump(mode="json", by_alias=True, exclude_none=True),
        layout=layout,
        bindings=bindings,
        metadata=metadata,
    )


def make_passthrough_transform(
    *,
    project_fields: Sequence[str] | None = None,
    computed_columns: Sequence[TabularTransformComputedColumn | Mapping[str, Any]] | None = None,
) -> TabularTransformProps:
    """Build neutral transform props with optional projection/computed columns."""

    return make_tabular_transform_props(
        transform_mode="none",
        project_fields=list(project_fields) if project_fields is not None else None,
        computed_columns=_normalize_computed_columns(computed_columns),
    )


def make_projection_transform(
    *,
    project_fields: Sequence[str],
    computed_columns: Sequence[TabularTransformComputedColumn | Mapping[str, Any]] | None = None,
) -> TabularTransformProps:
    """Build projection-only transform props."""

    return make_passthrough_transform(
        project_fields=project_fields,
        computed_columns=computed_columns,
    )


def make_filter_transform(
    *,
    filter_rules: Sequence[TabularFilterRule | Mapping[str, Any]],
    filter_combine_mode: TabularFilterCombineMode = "all",
    project_fields: Sequence[str] | None = None,
    computed_columns: Sequence[TabularTransformComputedColumn | Mapping[str, Any]] | None = None,
) -> TabularTransformProps:
    """Build filter transform props."""

    return make_tabular_transform_props(
        transform_mode="filter",
        filter_combine_mode=filter_combine_mode,
        filter_rules=_normalize_filter_rules(filter_rules),
        project_fields=list(project_fields) if project_fields is not None else None,
        computed_columns=_normalize_computed_columns(computed_columns),
    )


def make_aggregate_transform(
    *,
    key_fields: Sequence[str],
    aggregate_mode: TabularAggregateMode = "last",
    project_fields: Sequence[str] | None = None,
    computed_columns: Sequence[TabularTransformComputedColumn | Mapping[str, Any]] | None = None,
) -> TabularTransformProps:
    """Build aggregate transform props."""

    return make_tabular_transform_props(
        transform_mode="aggregate",
        aggregate_mode=aggregate_mode,
        key_fields=list(key_fields),
        project_fields=list(project_fields) if project_fields is not None else None,
        computed_columns=_normalize_computed_columns(computed_columns),
    )


def make_pivot_transform(
    *,
    pivot_field: str,
    pivot_value_field: str,
    key_fields: Sequence[str] | None = None,
    project_fields: Sequence[str] | None = None,
    computed_columns: Sequence[TabularTransformComputedColumn | Mapping[str, Any]] | None = None,
) -> TabularTransformProps:
    """Build pivot transform props."""

    return make_tabular_transform_props(
        transform_mode="pivot",
        pivot_field=pivot_field,
        pivot_value_field=pivot_value_field,
        key_fields=list(key_fields) if key_fields is not None else None,
        project_fields=list(project_fields) if project_fields is not None else None,
        computed_columns=_normalize_computed_columns(computed_columns),
    )


def make_unpivot_transform(
    *,
    unpivot_value_fields: Sequence[str],
    unpivot_field_name: str = "series",
    unpivot_value_field_name: str = "value",
    key_fields: Sequence[str] | None = None,
    project_fields: Sequence[str] | None = None,
    computed_columns: Sequence[TabularTransformComputedColumn | Mapping[str, Any]] | None = None,
) -> TabularTransformProps:
    """Build unpivot transform props."""

    return make_tabular_transform_props(
        transform_mode="unpivot",
        unpivot_value_fields=list(unpivot_value_fields),
        unpivot_field_name=unpivot_field_name,
        unpivot_value_field_name=unpivot_value_field_name,
        key_fields=list(key_fields) if key_fields is not None else None,
        project_fields=list(project_fields) if project_fields is not None else None,
        computed_columns=_normalize_computed_columns(computed_columns),
    )


def make_latest_row_transform(
    *,
    row_merge_key_fields: Sequence[str] | None = None,
    row_merge_key_mappings: Sequence[TableLiveMergeKeyMapping | Mapping[str, Any]] | None = None,
    project_fields: Sequence[str] | None = None,
    computed_columns: Sequence[TabularTransformComputedColumn | Mapping[str, Any]] | None = None,
) -> TabularTransformProps:
    """Build passthrough transform props that collapse output to latest row per key."""

    return make_tabular_transform_props(
        transform_mode="none",
        row_merge_mode="latest",
        row_merge_key_fields=list(row_merge_key_fields)
        if row_merge_key_fields is not None
        else None,
        row_merge_key_mappings=_normalize_merge_key_mappings(row_merge_key_mappings),
        project_fields=list(project_fields) if project_fields is not None else None,
        computed_columns=_normalize_computed_columns(computed_columns),
    )


def _normalize_computed_columns(
    computed_columns: Sequence[TabularTransformComputedColumn | Mapping[str, Any]] | None,
) -> list[TabularTransformComputedColumn] | None:
    if computed_columns is None:
        return None
    return [
        column
        if isinstance(column, TabularTransformComputedColumn)
        else TabularTransformComputedColumn(**dict(column))
        for column in computed_columns
    ]


def _normalize_filter_rules(
    filter_rules: Sequence[TabularFilterRule | Mapping[str, Any]],
) -> list[TabularFilterRule]:
    return [
        rule if isinstance(rule, TabularFilterRule) else TabularFilterRule(**dict(rule))
        for rule in filter_rules
    ]


def _normalize_merge_key_mappings(
    mappings: Sequence[TableLiveMergeKeyMapping | Mapping[str, Any]] | None,
) -> list[TableLiveMergeKeyMapping] | None:
    if mappings is None:
        return None
    return [
        mapping
        if isinstance(mapping, TableLiveMergeKeyMapping)
        else TableLiveMergeKeyMapping(**dict(mapping))
        for mapping in mappings
    ]


__all__ = [
    "CORE_TABULAR_TRANSFORM_WIDGET_ID",
    "TABULAR_TRANSFORM_DATASET_OUTPUT_ID",
    "TABULAR_TRANSFORM_LIVE_UPDATES_INPUT_ID",
    "TABULAR_TRANSFORM_SEED_INPUT_ID",
    "TABULAR_TRANSFORM_UPDATES_OUTPUT_ID",
    "TabularAggregateMode",
    "TabularFilterCombineMode",
    "TabularFilterOperator",
    "TabularFilterRule",
    "TabularFilterRuleValue",
    "TabularTransformComputedColumn",
    "TabularTransformComputedColumnType",
    "TabularTransformMode",
    "TabularTransformProps",
    "TabularTransformPropsInput",
    "TabularTransformRowMergeMode",
    "dump_tabular_transform_props",
    "make_aggregate_transform",
    "make_filter_transform",
    "make_latest_row_transform",
    "make_passthrough_transform",
    "make_pivot_transform",
    "make_projection_transform",
    "make_tabular_filter_rule",
    "make_tabular_transform_computed_column",
    "make_tabular_transform_props",
    "make_tabular_transform_widget_payload",
    "make_unpivot_transform",
]
