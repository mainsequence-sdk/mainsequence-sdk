import pytest

from mainsequence.client.command_center import (
    CORE_PRO_TABLE_WIDGET_ID,
    CORE_TABLE_WIDGET_ID,
    TableLiveMergeKeyMapping,
    TableWidgetColumnOverride,
    TableWidgetColumnSchema,
    dump_table_widget_props,
    formula_percent_change,
    make_formula_column,
    make_pro_table_widget_payload,
    make_table_visual_column,
    make_table_visuals,
    make_table_widget_payload,
    make_tabular_frame,
)


def test_tabular_frame_accepts_typed_table_visuals_metadata():
    frame = make_tabular_frame(
        rows=[{"asset": "BTC", "price": 101.25, "change": 0.015}],
        columns=["asset", "price", "change"],
        meta={"provider": "example"},
        table_visuals=make_table_visuals(
            {
                "price": make_table_visual_column(
                    label="Last Price",
                    format="currency",
                    decimals=2,
                    width=140,
                ),
                "change": {
                    "format": "percent",
                    "decimals": 2,
                    "thresholds": [
                        {
                            "operator": "lt",
                            "value": 0,
                            "tone": "danger",
                        }
                    ],
                },
            }
        ),
    )

    payload = frame.model_dump(mode="json", exclude_none=True)

    assert payload["meta"]["provider"] == "example"
    assert payload["meta"]["tableVisuals"] == {
        "columns": {
            "price": {
                "label": "Last Price",
                "format": "currency",
                "decimals": 2,
                "width": 140,
            },
            "change": {
                "format": "percent",
                "decimals": 2,
                "thresholds": [{"operator": "lt", "tone": "danger", "value": 0.0}],
            },
        }
    }


def test_table_widget_payload_serializes_formatting_and_live_merge_mappings():
    payload = make_table_widget_payload(
        title="Prices",
        schema=[
            TableWidgetColumnSchema(key="asset", label="Asset", format="text"),
            TableWidgetColumnSchema(key="price", label="Price", format="currency"),
        ],
        column_overrides={
            "price": TableWidgetColumnOverride(
                format="currency",
                decimals=2,
                prefix="$",
                align="right",
            )
        },
        live_merge_key_mappings=[TableLiveMergeKeyMapping(seed_field="asset", live_field="symbol")],
        selection_mode="single-row",
    )

    assert payload["widgetTypeId"] == CORE_TABLE_WIDGET_ID
    assert payload["title"] == "Prices"
    assert payload["props"]["formulasEnabled"] is False
    assert payload["props"]["columnOverrides"] == {
        "price": {
            "format": "currency",
            "decimals": 2,
            "prefix": "$",
            "align": "right",
        }
    }
    assert payload["props"]["liveMergeKeyMappings"] == [
        {"seedField": "asset", "liveField": "symbol"}
    ]


def test_normal_table_rejects_formula_columns():
    formula_column = make_formula_column(
        key="return_pct",
        label="Return %",
        expression=formula_percent_change("price", "previous_price"),
        result_format="percent",
    )

    with pytest.raises(ValueError, match="Formula columns require the pro table widget"):
        make_table_widget_payload(schema=[formula_column])


def test_pro_table_payload_enables_formula_columns():
    payload = make_pro_table_widget_payload(
        title="Returns",
        schema=[
            TableWidgetColumnSchema(key="asset", label="Asset", format="text"),
            make_formula_column(
                key="return_pct",
                label="Return %",
                expression=formula_percent_change("price", "previous_price"),
                result_format="percent",
                decimals=2,
                suffix="%",
            ),
        ],
        show_search=True,
    )

    assert payload["widgetTypeId"] == CORE_PRO_TABLE_WIDGET_ID
    assert payload["props"]["formulasEnabled"] is True
    assert payload["props"]["schema"][1] == {
        "key": "return_pct",
        "label": "Return %",
        "format": "formula",
        "formulaExpression": "PERCENT_CHANGE([price], [previous_price])",
        "formulaResultFormat": "percent",
        "decimals": 2,
        "suffix": "%",
    }


def test_table_python_fields_are_snake_case_and_dump_frontend_camel_case():
    props = dump_table_widget_props(
        schema=[
            TableWidgetColumnSchema(
                key="event_time",
                label="Event Time",
                format="datetime",
                date_time_input_format="iso",
                date_time_output_format="MMM d, yyyy",
            )
        ],
        column_overrides={
            "price": TableWidgetColumnOverride(
                bar_mode="fill",
                gradient_mode="fill",
                visual_range_mode="fixed",
                visual_min=0,
                visual_max=100,
            )
        },
        live_merge_key_mappings=[
            TableLiveMergeKeyMapping(seed_field="symbol", live_field="ticker")
        ],
    )

    assert props["schema"][0]["dateTimeInputFormat"] == "iso"
    assert props["schema"][0]["dateTimeOutputFormat"] == "MMM d, yyyy"
    assert props["columnOverrides"]["price"]["barMode"] == "fill"
    assert props["columnOverrides"]["price"]["gradientMode"] == "fill"
    assert props["columnOverrides"]["price"]["visualRangeMode"] == "fixed"
    assert props["columnOverrides"]["price"]["visualMin"] == 0
    assert props["columnOverrides"]["price"]["visualMax"] == 100
    assert props["liveMergeKeyMappings"] == [{"seedField": "symbol", "liveField": "ticker"}]
