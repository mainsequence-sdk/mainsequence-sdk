import pytest

from mainsequence.client.command_center import (
    CORE_TABULAR_FRAME_CONTRACT,
    CORE_TABULAR_TRANSFORM_WIDGET_ID,
    TABULAR_TRANSFORM_LIVE_UPDATES_INPUT_ID,
    TABULAR_TRANSFORM_UPDATES_OUTPUT_ID,
    TableLiveMergeKeyMapping,
    bind_tabular_transform_live_updates,
    dump_tabular_transform_props,
    formula_difference,
    formula_percent_change,
    make_aggregate_transform,
    make_filter_transform,
    make_latest_row_transform,
    make_passthrough_transform,
    make_pivot_transform,
    make_tabular_filter_rule,
    make_tabular_transform_computed_column,
    make_tabular_transform_props,
    make_tabular_transform_widget_payload,
    make_unpivot_transform,
)


def test_tabular_transform_payload_serializes_passthrough_props():
    payload = make_tabular_transform_widget_payload(
        title="Transform",
        props=make_passthrough_transform(
            project_fields=["symbol", "last_price"],
            computed_columns=[
                make_tabular_transform_computed_column(
                    key="net_change",
                    label="Net Chg",
                    formula_expression=formula_difference("last_price", "previous_close"),
                )
            ],
        ),
    )

    assert payload["widgetTypeId"] == CORE_TABULAR_TRANSFORM_WIDGET_ID
    assert payload["title"] == "Transform"
    assert payload["props"] == {
        "transformMode": "none",
        "aggregateMode": "last",
        "computedColumns": [
            {
                "key": "net_change",
                "label": "Net Chg",
                "type": "number",
                "formulaExpression": "DIFFERENCE([last_price], [previous_close])",
            }
        ],
        "filterCombineMode": "all",
        "projectFields": ["symbol", "last_price"],
        "rowMergeMode": "passthrough",
        "unpivotFieldName": "series",
        "unpivotValueFieldName": "value",
    }


def test_aggregate_transform_payload_uses_frontend_keys():
    payload = make_tabular_transform_widget_payload(
        props=make_aggregate_transform(
            key_fields=["symbol"],
            aggregate_mode="mean",
            project_fields=["symbol", "price"],
        )
    )

    assert payload["props"]["transformMode"] == "aggregate"
    assert payload["props"]["aggregateMode"] == "mean"
    assert payload["props"]["keyFields"] == ["symbol"]
    assert payload["props"]["projectFields"] == ["symbol", "price"]


def test_filter_transform_payload_serializes_rules():
    payload = make_tabular_transform_widget_payload(
        props=make_filter_transform(
            filter_combine_mode="any",
            filter_rules=[
                make_tabular_filter_rule(field="symbol", operator="in", value=["BTC", "ETH"]),
                {"field": "price", "operator": "gt", "value": 0},
            ],
        )
    )

    assert payload["props"]["transformMode"] == "filter"
    assert payload["props"]["filterCombineMode"] == "any"
    assert payload["props"]["filterRules"] == [
        {"field": "symbol", "operator": "in", "value": ["BTC", "ETH"]},
        {"field": "price", "operator": "gt", "value": 0},
    ]


def test_pivot_and_unpivot_builders_validate_required_fields():
    pivot = make_pivot_transform(pivot_field="tenor", pivot_value_field="rate", key_fields=["date"])
    unpivot = make_unpivot_transform(
        unpivot_value_fields=["bid", "ask"],
        unpivot_field_name="side",
        unpivot_value_field_name="price",
    )

    assert pivot.transform_mode == "pivot"
    assert pivot.pivot_field == "tenor"
    assert unpivot.transform_mode == "unpivot"
    assert unpivot.unpivot_field_name == "side"


def test_latest_row_transform_serializes_merge_mappings():
    payload = make_tabular_transform_widget_payload(
        props=make_latest_row_transform(
            row_merge_key_mappings=[
                TableLiveMergeKeyMapping(seed_field="symbol", live_field="ticker")
            ],
            computed_columns=[
                make_tabular_transform_computed_column(
                    key="return_pct",
                    formula_expression=formula_percent_change("last_price", "previous_close"),
                )
            ],
        )
    )

    assert payload["props"]["rowMergeMode"] == "latest"
    assert payload["props"]["rowMergeKeyMappings"] == [
        {"seedField": "symbol", "liveField": "ticker"}
    ]
    assert payload["props"]["computedColumns"][0]["formulaExpression"] == (
        "PERCENT_CHANGE([last_price], [previous_close])"
    )


def test_tabular_transform_validation_rejects_missing_mode_requirements():
    with pytest.raises(ValueError, match="Aggregate transforms require"):
        make_tabular_transform_props(transform_mode="aggregate")

    with pytest.raises(ValueError, match="Filter transforms require"):
        make_tabular_transform_props(transform_mode="filter")

    with pytest.raises(ValueError, match="Pivot transforms require pivot_field"):
        make_tabular_transform_props(transform_mode="pivot", pivot_value_field="price")

    with pytest.raises(ValueError, match="Unpivot transforms require"):
        make_tabular_transform_props(transform_mode="unpivot")

    with pytest.raises(ValueError, match="Latest-row merge requires"):
        make_tabular_transform_props(row_merge_mode="latest")


def test_tabular_transform_validation_rejects_duplicate_computed_column_keys():
    with pytest.raises(ValueError, match="Computed column keys must be unique"):
        make_tabular_transform_props(
            computed_columns=[
                make_tabular_transform_computed_column(
                    key="return_pct",
                    formula_expression="[price] - [open]",
                ),
                make_tabular_transform_computed_column(
                    key="return_pct",
                    formula_expression="[close] - [open]",
                ),
            ]
        )


def test_live_update_binding_targets_transform_live_input():
    binding = bind_tabular_transform_live_updates(source_widget_uid="source-widget")

    assert binding == {
        "input": TABULAR_TRANSFORM_LIVE_UPDATES_INPUT_ID,
        "source": {
            "widgetUid": "source-widget",
            "output": TABULAR_TRANSFORM_UPDATES_OUTPUT_ID,
            "contract": CORE_TABULAR_FRAME_CONTRACT,
        },
    }


def test_transform_python_fields_are_snake_case_and_dump_frontend_camel_case():
    props = dump_tabular_transform_props(
        transform_mode="pivot",
        aggregate_mode="last",
        filter_combine_mode="all",
        key_fields=["date"],
        pivot_field="tenor",
        pivot_value_field="rate",
        project_fields=["date", "1m", "3m"],
        row_merge_key_fields=["date"],
        row_merge_mode="latest",
        unpivot_field_name="series",
        unpivot_value_field_name="value",
        computed_columns=[
            make_tabular_transform_computed_column(
                key="spread",
                formula_expression=formula_difference("ask", "bid"),
            )
        ],
    )

    assert props["transformMode"] == "pivot"
    assert props["aggregateMode"] == "last"
    assert props["filterCombineMode"] == "all"
    assert props["keyFields"] == ["date"]
    assert props["pivotField"] == "tenor"
    assert props["pivotValueField"] == "rate"
    assert props["projectFields"] == ["date", "1m", "3m"]
    assert props["rowMergeKeyFields"] == ["date"]
    assert props["rowMergeMode"] == "latest"
    assert props["unpivotFieldName"] == "series"
    assert props["unpivotValueFieldName"] == "value"
    assert props["computedColumns"][0]["formulaExpression"] == "DIFFERENCE([ask], [bid])"
