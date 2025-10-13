import copy
from pathlib import Path

from mainsequence.reportbuilder.model import (
    FontWeight,
    GridCell,
    GridLayout,
    HorizontalAlign,
    HtmlElement,
    ImageElement,
    Presentation,
    Size,
    Slide,
    StyleSettings,
    TextElement,
    TextH2,
    ThemeMode,
    VerticalAlign,
)
from mainsequence.reportbuilder.slide_templates import (
    generic_plotly_bar_chart,
    generic_plotly_grouped_bar_chart,
    generic_plotly_pie_chart,
    generic_plotly_table,
)

styles = StyleSettings(mode=ThemeMode.light)
styles.logo_url = "https://cdn.prod.website-files.com/67d166ea95c73519badbdabd/67d166ea95c73519badbdc60_Asset%25202%25404x-8-p-800.png"

report_main_color = "#003049"  # mainsequence.reportbuilder.model.main_sequence_blue
report_text_color_dark = "#333333"
report_text_color_light_gray = "#555555"

fixed_income_local_headers = [
    "INSTRUMENT",
    "UNITS",
    "PRICE",
    "AMOUNT",
    "% TOTAL",
    "DURATION",
    "YIELD",
    "DxV",
]
fixed_income_local_rows = [
    ["Alpha Bond 2025", "350,000", "$99.50", "$34,825,000.00", "7.50%", "0.25", "9.05%", "90"],
    ["Beta Note 2026", "160,000", "$99.80", "$15,968,000.00", "3.60%", "1.30", "9.15%", "530"],
    ["Gamma Security 2026", "250,000", "$99.90", "$24,975,000.00", "5.60%", "1.50", "9.20%", "600"],
    ["Delta Issue 2027", "245,000", "$100.10", "$24,524,500.00", "5.40%", "1.60", "9.25%", "630"],
    ["Epsilon Paper 2026", "200,000", "$98.50", "$19,700,000.00", "4.40%", "0.80", "8.30%", "300"],
    ["Zeta Bond 2029", "170,000", "$102.50", "$17,425,000.00", "3.90%", "3.30", "8.60%", "1,500"],
    [
        "Eta Security 2030",
        "180,000",
        "$100.00",
        "$18,000,000.00",
        "4.00%",
        "3.80",
        "8.80%",
        "1,700",
    ],
    ["Theta Note 2034", "110,000", "$93.00", "$10,230,000.00", "2.30%", "6.30", "9.30%", "3,500"],
    ["Iota UDI 2028", "40,000", "$98.00", "$33,600,000.00", "7.90%", "3.20", "4.90%", "1,300"],
    ["Kappa C-Bill 2026A", "2,500,000", "$9.20", "$23,000,000.00", "5.10%", "0.85", "8.40%", "340"],
    [
        "Lambda C-Bill 2026B",
        "3,300,000",
        "$8.80",
        "$29,040,000.00",
        "6.70%",
        "1.25",
        "8.50%",
        "520",
    ],
    ["TOTAL", "", "", "$251,287,500.00", "56.70%", "1.60", "8.55%", "480"],
]

fixed_income_usd_headers = [
    "INSTRUMENT",
    "UNITS",
    "PRICE",
    "AMOUNT",
    "% TOTAL",
    "DURATION",
    "YIELD",
    "DxV",
]
fixed_income_usd_rows = [
    ["Global Note A", "16,000", "$2,900.00", "$46,400,000.00", "10.00%", "8.50", "4.30%", "3,100"],
    [
        "International Bond X",
        "40,000",
        "$2,200.00",
        "$88,000,000.00",
        "20.00%",
        "1.90",
        "3.90%",
        "700",
    ],
    ["TOTAL", "", "", "$134,400,000.00", "30.00%", "4.10", "4.00%", "1,500"],
]

liquidity_headers = [
    "INSTRUMENT",
    "UNITS",
    "PRICE",
    "AMOUNT",
    "% TOTAL",
    "DURATION",
    "YIELD",
    "DxV",
]
liquidity_rows = [
    ["Repo Agreement", "", "", "$55,000,000.00", "12.50%", "0.01", "9.50%", "5"],
    ["Cash Equiv. (Local)", "", "", "$150.00", "0.00%", "", "", ""],
    ["Cash Equiv. (USD)", "50,000", "", "$1,000,000.00", "0.20%", "", "", ""],
    ["TOTAL", "", "", "$56,000,150.00", "12.70%", "", "", ""],
]


def title_slide(footer_text: str, slide_styles: StyleSettings) -> Slide:
    cover_slide_main_title_text = "Strategic Portfolio Insights"
    cover_slide_subtitle_text = "Monthly Review & Performance Outlook"
    cover_slide_tagline_text = "Navigating Your Financial Future with Precision"
    enlarged_logo_height_str = "75px"

    el_main_title = TextElement(
        text=cover_slide_main_title_text,
        element_type="h1",
        font_weight=FontWeight.bold,
        h_align=HorizontalAlign.left,
        color=slide_styles.heading_color,  # Use theme color
    )
    el_subtitle = TextElement(
        text=cover_slide_subtitle_text,
        element_type="h2",
        h_align=HorizontalAlign.left,
        color=slide_styles.heading_color,
    )
    el_logo = ImageElement(
        src=slide_styles.logo_url,
        alt="Main Sequence Logo",
        size=Size(height=enlarged_logo_height_str, width="auto"),
    )
    el_tagline = TextElement(
        text=cover_slide_tagline_text,
        element_type="h5",
        h_align=HorizontalAlign.left,
        color=slide_styles.paragraph_color,
    )

    # Use cover_slide_element_left_indent from StyleSettings if available, else default
    left_indent = getattr(slide_styles, "cover_slide_element_left_indent", "8%")

    cover_layout = GridLayout(
        row_definitions=["20%", "auto", "auto", "auto", "auto", "1fr"],
        col_definitions=[left_indent, "auto", "1fr"],
        gap=12,
        cells=[
            GridCell(
                row=2,
                col=2,
                element=el_main_title,
                justify_self=HorizontalAlign.left,
                align_self=VerticalAlign.bottom,
            ),
            GridCell(
                row=3,
                col=2,
                element=el_subtitle,
                justify_self=HorizontalAlign.left,
                align_self=VerticalAlign.top,
            ),
            GridCell(
                row=4,
                col=2,
                element=el_logo,
                justify_self=HorizontalAlign.left,
                align_self=VerticalAlign.center,
                padding="20px 0 0 0",
            ),
            GridCell(
                row=5,
                col=2,
                element=el_tagline,
                justify_self=HorizontalAlign.left,
                align_self=VerticalAlign.top,
            ),
        ],
        width="100%",
        height="100%",
    )
    return Slide(
        title="Main Cover", layout=cover_layout, footer_info=footer_text, style_theme=slide_styles
    )


def portfolio_detail_slide(footer_text: str, slide_styles: StyleSettings) -> Slide:
    shared_column_widths = [1.8, 1, 1, 1.5, 0.7, 0.8, 0.8, 0.7]
    shared_cell_align: str | list[str] = [
        "left",
        "right",
        "right",
        "right",
        "right",
        "right",
        "right",
        "right",
    ]
    table_figure_width = 900

    # Use paragraph font family for charts from the theme
    chart_font_family = slide_styles.font_family_paragraphs
    chart_label_font_size = slide_styles.chart_label_font_size

    fi_local_html = generic_plotly_table(
        headers=fixed_income_local_headers,
        rows=fixed_income_local_rows,
        column_widths=shared_column_widths,
        cell_align=shared_cell_align,
        fig_width=table_figure_width,
        header_font_dict=dict(
            color=slide_styles.background_color, size=10, family=chart_font_family
        ),
        cell_font_dict=dict(
            size=chart_label_font_size, family=chart_font_family, color=slide_styles.paragraph_color
        ),
        theme_mode=slide_styles.mode,
    )
    fi_usd_html = generic_plotly_table(
        headers=fixed_income_usd_headers,
        rows=fixed_income_usd_rows,
        column_widths=shared_column_widths,
        cell_align=shared_cell_align,
        fig_width=table_figure_width,
        header_font_dict=dict(
            color=slide_styles.background_color, size=10, family=chart_font_family
        ),
        cell_font_dict=dict(
            size=chart_label_font_size, family=chart_font_family, color=slide_styles.paragraph_color
        ),
        theme_mode=slide_styles.mode,
    )
    liquidity_html = generic_plotly_table(
        headers=liquidity_headers,
        rows=liquidity_rows,
        column_widths=shared_column_widths,
        cell_align=shared_cell_align,
        fig_width=table_figure_width,
        header_font_dict=dict(
            color=slide_styles.background_color, size=10, family=chart_font_family
        ),
        cell_font_dict=dict(
            size=chart_label_font_size, family=chart_font_family, color=slide_styles.paragraph_color
        ),
        theme_mode=slide_styles.mode,
    )

    total_general_rows = [["TOTAL", "", "", "$441,687,650.00", "100.00%", "", "", ""]]
    total_table_html = generic_plotly_table(
        headers=liquidity_headers,
        rows=total_general_rows,
        fig_width=table_figure_width,
        column_widths=shared_column_widths,
        cell_align=["left", "right", "right", "right", "right", "right", "right", "right"],
        cell_fill_color="rgba(0,0,0,0)",
        line_color="rgba(0,0,0,0)",
        header_fill_color="rgba(0,0,0,0)",  # header_font_color default will make it invisible too
        margin_dict=dict(l=0, r=0, t=5, b=0),
        cell_font_dict=dict(
            size=12, family=chart_font_family, color=slide_styles.paragraph_color
        ),  # Ensure total row text is visible
        theme_mode=slide_styles.mode,
    )

    slide_layout = GridLayout(
        row_definitions=["auto", "auto", "auto", "auto", "auto"],
        col_definitions=[slide_styles.title_column_width, "1fr"],
        gap=0,
        cells=[
            GridCell(
                row=1,
                col=1,
                col_span=2,
                element=TextElement(
                    text="Portfolio",
                    element_type="h3",
                    font_weight=FontWeight.bold,
                    h_align=HorizontalAlign.left,
                    color=report_main_color,
                ),
                padding="0 0 3px 0",
            ),
            GridCell(
                row=2,
                col=1,
                element=TextElement(
                    text="Fixed Income (Local Currency)",
                    element_type="p",
                    font_weight=FontWeight.bold,
                    color=report_main_color,
                    h_align=HorizontalAlign.left,
                    v_align=VerticalAlign.center,
                ),
                padding="2px 10px 2px 0",
                align_self="start",
            ),
            GridCell(row=2, col=2, element=HtmlElement(html=fi_local_html), padding="2px 0 2px 0"),
            GridCell(
                row=3,
                col=1,
                element=TextElement(
                    text="Fixed Income (USD)",
                    element_type="p",
                    font_weight=FontWeight.bold,
                    color=report_main_color,
                    h_align=HorizontalAlign.left,
                    v_align=VerticalAlign.center,
                ),
                padding="2px 10px 2px 0",
                align_self="start",
            ),
            GridCell(row=3, col=2, element=HtmlElement(html=fi_usd_html), padding="2px 0 2px 0"),
            GridCell(
                row=4,
                col=1,
                element=TextElement(
                    text="Liquidity",
                    element_type="p",
                    font_weight=FontWeight.bold,
                    color=report_main_color,
                    h_align=HorizontalAlign.left,
                    v_align=VerticalAlign.center,
                ),
                padding="2px 10px 2px 0",
                align_self="start",
            ),
            GridCell(row=4, col=2, element=HtmlElement(html=liquidity_html), padding="2px 0 2px 0"),
            GridCell(
                row=5, col=2, element=HtmlElement(html=total_table_html), padding="2px 0 2px 0"
            ),
        ],
    )
    return Slide(
        title="Portfolio Detail",
        layout=slide_layout,
        footer_info=footer_text,
        style_theme=slide_styles,
    )


def pie_chart_bars_slide(footer_text: str, slide_styles: StyleSettings) -> Slide:
    pie_chart_slice_colors = slide_styles.chart_palette_categorical[:3]  # Use theme palette
    chart_font_family = slide_styles.font_family_paragraphs
    chart_label_font_size = slide_styles.chart_label_font_size

    asset_table_headers_raw = ["ASSET CLASS", "%"]
    asset_table_rows_data = [
        ["Domestic Equities", "57%"],
        ["International Bonds", "30%"],
        ["Cash & Equivalents", "13%"],
    ]
    asset_class_table_html = generic_plotly_table(
        headers=asset_table_headers_raw,
        rows=asset_table_rows_data,
        table_height=125,
        fig_width=300,
        column_widths=[0.3, 0.2],
        header_font_dict=dict(
            color=slide_styles.background_color, size=10, family=chart_font_family
        ),
        cell_font_dict=dict(
            size=chart_label_font_size, family=chart_font_family, color=slide_styles.paragraph_color
        ),
        cell_align=["left", "right"],
        theme_mode=slide_styles.mode,
    )
    asset_class_table_el = HtmlElement(html=asset_class_table_html)

    asset_pie_labels = ["Domestic Equities", "Cash & Equivalents", "International Bonds"]
    asset_pie_values = [57, 13, 30]
    asset_pie_html = generic_plotly_pie_chart(
        labels=asset_pie_labels,
        values=asset_pie_values,
        colors=pie_chart_slice_colors,
        height=400,
        width=430,
        textinfo="percent",
        legend_dict=dict(
            font=dict(size=chart_label_font_size, family=chart_font_family),
            orientation="h",
            yanchor="top",
            y=-0.1,
            xanchor="center",
            x=0.5,
            bgcolor="rgba(0,0,0,0)",
        ),
        font_dict=dict(family=chart_font_family, color=slide_styles.paragraph_color),
        theme_mode=slide_styles.mode,
    )
    asset_pie_el = HtmlElement(html=asset_pie_html)

    duration_bar_labels = [
        ">7 Years",
        "3-7 Years",
        "2-3 Years",
        "1-2 Years",
        "0-1 Years",
        "Short-Term/Cash",
    ]
    duration_bar_values = [10.05, 12.07, 0.20, 41.52, 12.16, 12.76]
    max_val = max(duration_bar_values) if duration_bar_values else 1
    custom_xaxis_bar_dict = dict(
        showgrid=False,
        zeroline=False,
        showline=False,
        showticklabels=True,
        ticksuffix="%",
        side="bottom",
        tickfont=dict(
            size=chart_label_font_size, color=report_text_color_dark, family=chart_font_family
        ),
        range=[0, max_val * 1.115],
    )
    custom_yaxis_bar_dict = dict(
        showgrid=False,
        zeroline=False,
        showline=False,
        tickfont=dict(
            size=chart_label_font_size, color=report_text_color_dark, family=chart_font_family
        ),
        autorange="reversed",
    )
    duration_bar_html = generic_plotly_bar_chart(
        y_values=duration_bar_labels,
        x_values=duration_bar_values,
        orientation="h",
        bar_color=report_main_color,
        width=450,
        height=450,
        xaxis_dict=custom_xaxis_bar_dict,
        yaxis_dict=custom_yaxis_bar_dict,
        font_dict=dict(family=chart_font_family, color=slide_styles.paragraph_color),
        textfont_dict=dict(
            size=chart_label_font_size, family=chart_font_family, color=slide_styles.paragraph_color
        ),
        theme_mode=slide_styles.mode,
    )
    duration_bar_el = HtmlElement(html=duration_bar_html)

    slide_layout = GridLayout(
        row_definitions=["auto", "auto", "1fr"],
        col_definitions=["1fr", "1fr"],
        gap=10,
        cells=[
            GridCell(
                row=1,
                col=1,
                element=TextElement(
                    text="Asset Allocation",
                    element_type="p",
                    font_weight=FontWeight.bold,
                    color=report_main_color,
                    h_align=HorizontalAlign.left,
                ),
                align_self=VerticalAlign.bottom,
                padding="0 0 2px 0",
            ),
            GridCell(
                row=2,
                col=1,
                element=asset_class_table_el,
                align_self=VerticalAlign.top,
                justify_self=HorizontalAlign.center,
                padding="5px 0 0 0",
            ),
            GridCell(
                row=3,
                col=1,
                element=asset_pie_el,
                align_self=VerticalAlign.center,
                justify_self=HorizontalAlign.center,
                padding="5px 0 0 0",
            ),
            GridCell(
                row=1,
                col=2,
                element=TextElement(
                    text="Duration Breakdown",
                    element_type="p",
                    font_weight=FontWeight.bold,
                    color=report_main_color,
                    h_align=HorizontalAlign.left,
                ),
                align_self=VerticalAlign.bottom,
                padding="0 0 2px 0",
            ),
            GridCell(
                row=2,
                col=2,
                row_span=2,
                element=duration_bar_el,
                align_self=VerticalAlign.center,
                justify_self=HorizontalAlign.left,
                padding="5px 0 0 0",
            ),
        ],
    )
    return Slide(
        title="Portfolio Snapshot",
        layout=slide_layout,
        footer_info=footer_text,
        style_theme=slide_styles,
    )


def generic_table_and_grouped_bar_chart_slide(
    footer_text: str, slide_styles: StyleSettings
) -> Slide:
    slide_main_title_text = "Component Analysis: Multi-Series Data"
    chart_font_family = slide_styles.font_family_paragraphs
    chart_label_font_size = slide_styles.chart_label_font_size

    table_headers = ["Type", "Value Set 1", "Value Set 2", "Difference (VS1-VS2)"]
    raw_table_rows_data = [
        ["Alpha Group", 0.60, 0.40],
        ["Beta Group", 0.45, 0.50],
        ["Gamma Group", 0.70, 0.55],
        ["Delta Group", 0.25, 0.25],
        ["Epsilon Group", 0.50, 0.30],
        ["Zeta Group", 0.10, 0.15],
    ]
    table_rows_processed, total_vs1, total_vs2, total_diff = [], 0, 0, 0
    for row_data in raw_table_rows_data:
        category, vs1, vs2 = row_data
        diff = round(vs1 - vs2, 2)
        table_rows_processed.append([category, f"{vs1:.2f}", f"{vs2:.2f}", f"{diff:.2f}"])
        total_vs1 += vs1
        total_vs2 += vs2
        total_diff += diff
    table_rows_processed.append(
        [
            "<b>TOTAL</b>",
            f"<b>{total_vs1:.2f}</b>",
            f"<b>{total_vs2:.2f}</b>",
            f"<b>{total_diff:.2f}</b>",
        ]
    )

    generic_table_html = generic_plotly_table(
        headers=table_headers,
        rows=table_rows_processed,
        column_widths=[0.7, 0.3, 0.3, 0.3],
        cell_align=["left", "right", "right", "right"],
        header_font_dict=dict(
            color=slide_styles.background_color, size=10, family=chart_font_family
        ),
        cell_font_dict=dict(
            size=chart_label_font_size, family=chart_font_family, color=slide_styles.paragraph_color
        ),
        fig_width=700,
        theme_mode=slide_styles.mode,
    )

    chart_categories = [f"Point {i + 1}" for i in range(11)]
    series_a_values = [0.50, 0.45, 0.60, 0.30, 0.75, 0.20, 0.55, 0.40, 0.65, 0.35, 0.70]
    series_b_values = [0.40, 0.50, 0.55, 0.35, 0.60, 0.25, 0.60, 0.30, 0.70, 0.25, 0.65]
    series_data_for_chart = [
        {"name": "Data Series A", "y_values": series_a_values, "color": report_main_color},
        {"name": "Data Series B", "y_values": series_b_values, "color": "rgb(200,200,200)"},
        {
            "name": "Difference (A-B)",
            "y_values": [
                round(a - b, 2) for a, b in zip(series_a_values, series_b_values, strict=False)
            ],
            "color": "rgb(160,120,70)",
        },
    ]
    custom_legend = dict(
        orientation="h",
        yanchor="top",
        y=-0.15,
        xanchor="center",
        x=0.5,
        font=dict(size=chart_label_font_size, family=chart_font_family),
        bgcolor="rgba(0,0,0,0)",
    )
    generic_bar_chart_html = generic_plotly_grouped_bar_chart(
        x_values=chart_categories,
        series_data=series_data_for_chart,
        chart_title="",
        height=380,
        width=800,
        y_axis_tick_format=".2f",
        xaxis_tickangle=-45,
        legend_dict=custom_legend,
        margin_dict=dict(l=30, r=20, t=10, b=120),
        theme_mode=slide_styles.mode,
    )

    slide_layout = GridLayout(
        row_definitions=["auto", "auto", "auto"],
        col_definitions=["1fr"],
        gap=10,
        cells=[
            GridCell(
                row=1,
                col=1,
                element=TextElement(
                    text=slide_main_title_text,
                    element_type="h5",
                    font_weight=FontWeight.bold,
                    color=report_main_color,
                    h_align=HorizontalAlign.center,
                ),
                padding="0 0 2px 0",
            ),
            GridCell(
                row=2,
                col=1,
                element=HtmlElement(html=generic_table_html),
                justify_self=HorizontalAlign.center,
                align_self=VerticalAlign.center,
            ),
            GridCell(
                row=3,
                col=1,
                element=HtmlElement(html=generic_bar_chart_html),
                justify_self=HorizontalAlign.center,
                padding="5px 0 0 0",
            ),
        ],
    )
    return Slide(
        title="Generic Grouped Bar Chart and Table",
        layout=slide_layout,
        footer_info=footer_text,
        style_theme=slide_styles,
    )


def portfolio_performance_slide(footer_text: str, slide_styles: StyleSettings) -> Slide:
    portfolio_bar_color = report_main_color
    benchmark_bar_color = "#CDA434"

    monthly_categories = ["JAN", "FEB", "MAR", "APR"]
    monthly_series_data = [
        {"name": "Portfolio", "y_values": [0.83, 1.13, 1.72, 1.08], "color": portfolio_bar_color},
        {"name": "Benchmark", "y_values": [1.38, 1.47, 1.31, 1.02], "color": benchmark_bar_color},
    ]
    monthly_chart_html = generic_plotly_grouped_bar_chart(
        x_values=monthly_categories,
        series_data=monthly_series_data,
        chart_title="Monthly Performance (LCY)",
        height=280,
        width=750,
        y_axis_tick_format=".2f",
        bar_text_template="%{y:.2f}%",
        legend_dict=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin_dict=dict(l=40, r=20, t=50, b=40),
        theme_mode=slide_styles.mode,
    )

    annual_categories = ["2023", "2024"]
    annual_series_data = [
        {"name": "Portfolio", "y_values": [4.84, 11.45], "color": portfolio_bar_color},
        {"name": "Benchmark", "y_values": [5.28, 9.91], "color": benchmark_bar_color},
    ]
    annual_chart_html = generic_plotly_grouped_bar_chart(
        x_values=annual_categories,
        series_data=annual_series_data,
        chart_title="Annual Performance (LCY)",
        height=240,
        width=750,
        y_axis_tick_format=".2f",
        bar_text_template="%{y:.2f}%",
        legend_dict=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin_dict=dict(l=40, r=20, t=50, b=40),
        theme_mode=slide_styles.mode,
    )

    slide_layout = GridLayout(
        row_definitions=["auto", "auto", "auto"],
        col_definitions=["1fr"],
        gap=10,
        cells=[
            GridCell(
                row=1,
                col=1,
                element=TextElement(
                    text="Portfolio Performance in Local Currency",
                    element_type="h4",
                    font_weight=FontWeight.bold,
                    color=report_main_color,
                    h_align=HorizontalAlign.left,
                ),
                padding="0 0 5px 0",
            ),
            GridCell(
                row=2,
                col=1,
                element=HtmlElement(html=monthly_chart_html),
                justify_self=HorizontalAlign.center,
            ),
            GridCell(
                row=3,
                col=1,
                element=HtmlElement(html=annual_chart_html),
                justify_self=HorizontalAlign.center,
            ),
        ],
    )
    return Slide(
        title="Financial Performance Summary",
        layout=slide_layout,
        footer_info=footer_text,
        style_theme=slide_styles,
    )


def rate_sensitivity_analysis_slide(footer_text: str, slide_styles: StyleSettings) -> Slide:
    slide_main_title_text = "Rate Sensitivity Analysis: USD Portfolio"
    chart_font_family = slide_styles.font_family_paragraphs
    chart_label_font_size = slide_styles.chart_label_font_size

    table1_headers, table1_rows = ["Factor", "Portfolio", "Benchmark", "Active"], [
        ["Factor A", "60%", "100%", "-40%"],
        ["Factor B", "30%", "0%", "30%"],
        ["Factor C", "10%", "0%", "10%"],
    ]
    table1_html = generic_plotly_table(
        headers=table1_headers,
        rows=table1_rows,
        fig_width=350,
        table_height=120,
        column_widths=[0.8, 0.5, 0.5, 0.5],
        cell_align=["left", "center", "center", "center"],
        header_font_dict=dict(
            color=slide_styles.background_color, size=10, family=chart_font_family
        ),
        cell_font_dict=dict(
            size=chart_label_font_size, family=chart_font_family, color=slide_styles.paragraph_color
        ),
        header_fill_color=report_main_color,
        cell_fill_color="rgb(245,245,245)",
        line_color="rgb(220,220,220)",
        theme_mode=slide_styles.mode,
    )
    table2_headers, table2_rows = ["Factor", "Portfolio", "Benchmark", "Active"], [
        ["Factor A", "1.30", "1.90", "-0.60"],
        ["Factor B", "2.80", "-", "2.80"],
        ["Factor C", "-", "-", "-"],
        ["Total", "4.10", "1.90", "2.20"],
    ]
    table2_html = generic_plotly_table(
        headers=table2_headers,
        rows=table2_rows,
        fig_width=350,
        table_height=145,
        column_widths=[0.8, 0.5, 0.5, 0.5],
        cell_align=["left", "right", "right", "right"],
        header_font_dict=dict(
            color=slide_styles.background_color, size=10, family=chart_font_family
        ),
        cell_font_dict=dict(
            size=chart_label_font_size, family=chart_font_family, color=slide_styles.paragraph_color
        ),
        header_fill_color=report_main_color,
        cell_fill_color="rgb(245,245,245)",
        line_color="rgb(220,220,220)",
        theme_mode=slide_styles.mode,
    )
    bar_chart_categories = ["Factor A", "Factor B"]
    bar_chart_series_data = [
        {"name": "Portfolio", "y_values": [1.30, 2.80], "color": report_main_color},
        {"name": "Benchmark", "y_values": [1.90, 0.00], "color": "rgb(173, 216, 230)"},
        {"name": "Active", "y_values": [-0.60, 2.80], "color": "rgb(205, 164, 52)"},
    ]
    bar_chart_html = generic_plotly_grouped_bar_chart(
        x_values=bar_chart_categories,
        series_data=bar_chart_series_data,
        chart_title="",
        height=350,
        width=720,
        y_axis_tick_format=".2f",
        bar_text_template=None,
        legend_dict=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="center",
            x=0.5,
            font=dict(size=chart_label_font_size, family=chart_font_family),
            bgcolor="rgba(0,0,0,0)",
        ),
        margin_dict=dict(l=40, r=20, t=30, b=40),
        theme_mode=slide_styles.mode,
    )
    slide_layout = GridLayout(
        row_definitions=["auto", "auto", "1fr"],
        col_definitions=["1fr", "1fr"],
        gap=15,
        cells=[
            GridCell(
                row=1,
                col=1,
                col_span=2,
                element=TextElement(
                    text=slide_main_title_text,
                    element_type="h4",
                    font_weight=FontWeight.bold,
                    color=report_main_color,
                    h_align=HorizontalAlign.left,
                ),
                padding="0 0 10px 0",
            ),
            GridCell(
                row=2,
                col=1,
                element=HtmlElement(html=table1_html),
                justify_self=HorizontalAlign.center,
                align_self=VerticalAlign.top,
            ),
            GridCell(
                row=2,
                col=2,
                element=HtmlElement(html=table2_html),
                justify_self=HorizontalAlign.center,
                align_self=VerticalAlign.top,
            ),
            GridCell(
                row=3,
                col=1,
                col_span=2,
                element=HtmlElement(html=bar_chart_html),
                justify_self=HorizontalAlign.center,
                align_self=VerticalAlign.center,
                padding="20px 0 0 0",
            ),
        ],
    )
    return Slide(
        title="Rate Sensitivity Analysis",
        layout=slide_layout,
        footer_info=footer_text,
        style_theme=slide_styles,
    )


def issuer_performance_table_slide(footer_text: str, slide_styles: StyleSettings) -> Slide:
    slide_content_main_title = "Performance Analysis: Key Segments"
    table_super_header1_text, table_super_header2_text = "PORTFOLIOS", "SUB-CATEGORY DETAILS"
    table_headers = ["Category", "Weight (%)", "Return (%)", "Impact (%)"]
    table_rows_data = [
        ["Alpha Product Series 1", "-67.0%", "0.69%", "0.09%"],
        ["Beta Service Line 2", "66.3%", "0.92%", "0.30%"],
        ["Gamma Component Type 3", "0.7%", "-4.05%", "-0.03%"],
    ]
    chart_font_family = slide_styles.font_family_paragraphs
    chart_label_font_size = slide_styles.chart_label_font_size

    issuer_table_html = generic_plotly_table(
        headers=table_headers,
        rows=table_rows_data,
        fig_width=800,
        table_height=150,
        column_widths=[2, 1, 1, 1],
        cell_align=["left", "right", "right", "right"],
        header_align=["center", "center", "center", "center"],
        header_font_dict=dict(
            color=slide_styles.background_color,
            size=chart_label_font_size + 1,
            family=chart_font_family,
        ),
        cell_font_dict=dict(
            size=chart_label_font_size, family=chart_font_family, color=report_text_color_dark
        ),
        header_fill_color=report_main_color,
        cell_fill_color="white",
        line_color="rgb(150,150,150)",
        header_height=25,
        cell_height=25,
        margin_dict=dict(l=5, r=5, t=5, b=5),
        theme_mode=slide_styles.mode,
    )
    slide_layout = GridLayout(
        row_definitions=["auto", "auto", "auto", "auto", "1fr"],
        col_definitions=["1fr"],
        gap=0,
        cells=[
            GridCell(
                row=1,
                col=1,
                element=TextElement(
                    text=slide_content_main_title,
                    element_type="h4",
                    font_weight=FontWeight.bold,
                    color=report_main_color,
                    h_align=HorizontalAlign.left,
                ),
                padding="0 0 20px 0",
            ),
            GridCell(
                row=2,
                col=1,
                element=TextElement(
                    text=table_super_header1_text,
                    element_type="p",
                    font_weight=FontWeight.bold,
                    color=report_text_color_dark,
                    h_align=HorizontalAlign.center,
                ),
                padding="5px 0 0 0",
                background_color="white",
            ),
            GridCell(
                row=3,
                col=1,
                element=TextElement(
                    text=table_super_header2_text,
                    element_type="p",
                    font_weight=FontWeight.bold,
                    color=report_text_color_dark,
                    h_align=HorizontalAlign.center,
                ),
                padding="0 0 0 0",
                background_color="white",
            ),
            GridCell(
                row=4,
                col=1,
                element=HtmlElement(html=issuer_table_html),
                justify_self=HorizontalAlign.center,
                align_self=VerticalAlign.top,
                padding="0 0 0 0",
            ),
        ],
    )
    return Slide(
        title="Category Performance Summary",
        layout=slide_layout,
        footer_info=footer_text,
        style_theme=slide_styles,
    )


def comparative_performance_charts_slide(footer_text: str, slide_styles: StyleSettings) -> Slide:
    slide_content_main_title = "Overall Portfolio Performance: USD"
    portfolio_color, benchmark_color = report_main_color, "rgb(189, 149, 58)"
    chart_label_font_size = slide_styles.chart_label_font_size
    chart_font_family = slide_styles.font_family_paragraphs

    monthly_chart_title, monthly_categories = "Monthly USD Returns", ["Jan", "Feb", "Mar", "Apr"]
    monthly_series_data = [
        {"name": "Portfolio", "y_values": [0.2, 1.62, 0.6, 1.31], "color": portfolio_color},
        {"name": "Benchmark", "y_values": [0.1, 0.32, 0.89, 0.12], "color": benchmark_color},
    ]
    monthly_legend_config = dict(
        orientation="v",
        yanchor="top",
        y=1,
        xanchor="left",
        x=1.01,
        font=dict(size=chart_label_font_size - 1, family=chart_font_family),
        bgcolor="rgba(0,0,0,0)",
    )
    monthly_chart_html = generic_plotly_grouped_bar_chart(
        x_values=monthly_categories,
        series_data=monthly_series_data,
        chart_title=monthly_chart_title,
        height=300,
        width=700,
        y_axis_tick_format=".2f",
        bar_text_template="%{y:.2f}%",
        bar_text_position="outside",
        legend_dict=monthly_legend_config,
        margin_dict=dict(l=50, r=100, t=50, b=40),
        title_x_position=0.5,
        theme_mode=slide_styles.mode,
    )
    annual_chart_title, annual_categories = "Annual Returns", ["Prior Year", "Current Year"]
    annual_series_data = [
        {"name": "Portfolio", "y_values": [2.49, 5.78], "color": portfolio_color},
        {"name": "Benchmark", "y_values": [3.30, 5.18], "color": benchmark_color},
    ]
    annual_legend_config = dict(
        orientation="v",
        yanchor="top",
        y=1,
        xanchor="left",
        x=1.01,
        font=dict(size=chart_label_font_size - 1, family=chart_font_family),
        bgcolor="rgba(0,0,0,0)",
    )
    annual_chart_html = generic_plotly_grouped_bar_chart(
        x_values=annual_categories,
        series_data=annual_series_data,
        chart_title=annual_chart_title,
        height=330,
        width=700,
        y_axis_tick_format=".2f",
        bar_text_template="%{y:.2f}%",
        bar_text_position="outside",
        legend_dict=annual_legend_config,
        margin_dict=dict(l=50, r=100, t=60, b=40),
        title_x_position=0.5,
        theme_mode=slide_styles.mode,
    )
    slide_layout = GridLayout(
        row_definitions=["auto", "auto", "auto", "auto"],
        col_definitions=["1fr"],
        gap=5,
        cells=[
            GridCell(
                row=1,
                col=1,
                element=TextElement(
                    text=slide_content_main_title,
                    element_type="h4",
                    font_weight=FontWeight.normal,
                    color=report_main_color,
                    h_align=HorizontalAlign.left,
                ),
                padding="0 0 15px 0",
            ),
            GridCell(
                row=2,
                col=1,
                element=HtmlElement(html=monthly_chart_html),
                justify_self=HorizontalAlign.center,
                align_self=VerticalAlign.center,
            ),
            GridCell(
                row=3,
                col=1,
                element=HtmlElement(html=annual_chart_html),
                justify_self=HorizontalAlign.center,
                align_self=VerticalAlign.center,
                padding="15px 0 0 0",
            ),
        ],
    )
    return Slide(
        title="Portfolio Performance Summary",
        layout=slide_layout,
        footer_info=footer_text,
        style_theme=slide_styles,
    )


def data_table_summary_slide(footer_text: str, slide_styles: StyleSettings) -> Slide:
    slide_content_title = "Transactions Summary"
    table_headers = ["Date", "Deposits", "Withdrawals", "Net Amount"]
    example_table_rows = [
        ["05/01/2025", "", "1,200,000.50", "-1,200,000.50"],
        ["10/01/2025", "", "500,000.00", "-500,000.00"],
        ["15/01/2025", "", "7,000,000.00", "-7,000,000.00"],
        ["16/01/2025", "6,500,000.00", "6,500,000.00", "0.00"],
        ["02/02/2025", "10,000,000.00", "", "10,000,000.00"],
        ["08/02/2025", "", "10,000,000.00", "-10,000,000.00"],
        ["12/02/2025", "", "100,000.75", "-100,000.75"],
        ["25/02/2025", "2,000,000.00", "3,500,000.00", "-1,500,000.00"],
        ["10/03/2025", "", "250,000.00", "-250,000.00"],
        ["20/03/2025", "", "1,500,000.00", "-1,500,000.00"],
        ["<b>Total</b>", "<b>18,500,000.00</b>", "<b>40,550,000.25</b>", "<b>-22,050,000.25</b>"],
    ]
    table_html = generic_plotly_table(
        headers=table_headers,
        rows=example_table_rows,
        column_widths=[1, 1.5, 1.5, 1.5],
        cell_align=["left", "right", "right", "right"],
        header_align="center",
        table_height=400,
        theme_mode=slide_styles.mode,  # Added theme_mode
    )
    slide_layout = GridLayout(
        row_definitions=["auto", "1fr"],
        col_definitions=["1fr"],
        gap=10,
        cells=[
            GridCell(
                row=1,
                col=1,
                element=TextElement(
                    text=slide_content_title,
                    element_type="h4",
                    font_weight=FontWeight.normal,
                    color=report_main_color,
                    h_align=HorizontalAlign.left,
                ),
                padding="0 0 15px 0",
            ),
            GridCell(
                row=2,
                col=1,
                element=HtmlElement(html=table_html),
                justify_self=HorizontalAlign.center,
                align_self=VerticalAlign.center,
                padding="0 0 0 0",
            ),
        ],
    )
    return Slide(
        title="Data Table Summary",
        layout=slide_layout,
        footer_info=footer_text,
        style_theme=slide_styles,
    )


def contact_information_slide(footer_text: str, slide_styles: StyleSettings) -> Slide:
    # For the specific #d6e2ff background:
    contact_slide_specific_styles = copy.deepcopy(slide_styles)
    contact_slide_specific_styles.background_color = "#d6e2ff"
    # Adjust text colors if needed for contrast with the new background
    contact_slide_specific_styles.heading_color = "#000000"
    contact_slide_specific_styles.paragraph_color = "#000000"

    el_main_sequence = TextElement(
        text="Main Sequence",
        element_type="h1",
        font_weight=FontWeight.bold,
        h_align=HorizontalAlign.left,
        color=contact_slide_specific_styles.heading_color,
    )
    el_asset_management = TextElement(
        text="Asset Management",
        element_type="h3",
        font_weight=FontWeight.normal,
        h_align=HorizontalAlign.left,
        color=contact_slide_specific_styles.heading_color,
    )
    el_address1 = TextElement(
        text="Karlsplatz 3",
        element_type="h5",
        font_weight=FontWeight.normal,
        h_align=HorizontalAlign.left,
        color=contact_slide_specific_styles.paragraph_color,
    )
    el_address2 = TextElement(
        text="1010 Vienna, Austria",
        element_type="h5",
        font_weight=FontWeight.normal,
        h_align=HorizontalAlign.left,
        color=contact_slide_specific_styles.paragraph_color,
    )
    slide_layout = GridLayout(
        row_definitions=["18%", "auto", "auto", "auto", "auto", "1fr"],
        col_definitions=["8%", "auto", "1fr"],
        gap=6,
        cells=[
            GridCell(
                row=2,
                col=2,
                element=el_main_sequence,
                justify_self=HorizontalAlign.left,
                align_self=VerticalAlign.bottom,
            ),
            GridCell(
                row=3,
                col=2,
                element=el_asset_management,
                justify_self=HorizontalAlign.left,
                align_self=VerticalAlign.top,
            ),
            GridCell(
                row=4,
                col=2,
                element=el_address1,
                justify_self=HorizontalAlign.left,
                align_self=VerticalAlign.bottom,
                padding="15px 0 0 0",
            ),
            GridCell(
                row=5,
                col=2,
                element=el_address2,
                justify_self=HorizontalAlign.left,
                align_self=VerticalAlign.top,
            ),
        ],
    )
    return Slide(
        title="Contact Information",
        layout=slide_layout,
        footer_info=footer_text,
        style_theme=contact_slide_specific_styles,
    )


def contact_information_slide() -> Slide:
    slide_title = "Contact Details"
    page_main_title_text = "Main Sequence | Asset Management"

    contact_1_details = (
        "<strong>CONTACT NAME 1</strong><br>contact1@example.com<br>Tel. (555) 100-1001"
    )
    contact_2_details = "<strong>CONTACT NAME 2</strong><br>contact2@example.com<br>Ext. 1002"
    contact_3_details = "<strong>CONTACT NAME 3</strong><br>contact3@example.com<br>Ext. 1003"
    contact_4_details = "<strong>CONTACT NAME 4</strong><br>contact4@example.com<br>Ext. 1004"
    contact_5_details = "<strong>CONTACT NAME 5</strong><br>contact5@example.com<br>Ext. 1005"

    address_details = (
        "Placeholder Company Name<br>123 Anonymous Street, Placeholder City, ST 98765, Floor X."
    )

    contact_slide_cells = [
        GridCell(
            row=1,
            col=1,
            col_span=12,
            element=TextH2(text=page_main_title_text, h_align=HorizontalAlign.center),
            padding="5px 0 15px 0",
        ),
        GridCell(
            row=2,
            col=4,
            col_span=3,
            element=TextElement(
                text=contact_1_details, h_align=HorizontalAlign.center, v_align=VerticalAlign.top
            ),
            padding="10px",
        ),
        GridCell(
            row=2,
            col=7,
            col_span=3,
            element=TextElement(
                text=contact_2_details, h_align=HorizontalAlign.center, v_align=VerticalAlign.top
            ),
            padding="10px",
        ),
        GridCell(
            row=3,
            col=3,
            col_span=2,
            element=TextElement(
                text=contact_3_details, h_align=HorizontalAlign.center, v_align=VerticalAlign.top
            ),
            padding="10px",
        ),
        GridCell(
            row=3,
            col=6,
            col_span=2,
            element=TextElement(
                text=contact_4_details, h_align=HorizontalAlign.center, v_align=VerticalAlign.top
            ),
            padding="10px",
        ),
        GridCell(
            row=3,
            col=9,
            col_span=2,
            element=TextElement(
                text=contact_5_details, h_align=HorizontalAlign.center, v_align=VerticalAlign.top
            ),
            padding="10px",
        ),
        GridCell(
            row=4,
            col=1,
            col_span=12,
            element=TextElement(
                text=address_details, h_align=HorizontalAlign.center, element_type="p"
            ),
            padding="15px 0 5px 0",
        ),
    ]

    contacts_layout = GridLayout(
        col_definitions=[
            "1fr",
            "1fr",
            "1fr",
            "1fr",
            "1fr",
            "1fr",
            "1fr",
            "1fr",
            "1fr",
            "1fr",
            "1fr",
            "1fr",
        ],
        row_definitions=["auto", "auto", "auto", "auto"],
        cells=contact_slide_cells,
        gap="5px",
        width="100%",
        height="auto",
    )

    contact_slide = Slide(title=slide_title, layout=contacts_layout, footer_info="Confidential")
    return contact_slide


def create_full_presentation() -> Presentation:
    # Use the global `styles` object configured at the top for the presentation's base theme.
    presentation_styles = styles

    current_date_footer_text = "May 2025"

    contact_slide_styles = copy.deepcopy(presentation_styles)
    contact_slide_styles.background_color = "#d6e2ff"
    contact_slide_styles.heading_color = "#000000"  # Ensure text is readable on light blue
    contact_slide_styles.paragraph_color = "#000000"
    contact_slide_styles.light_paragraph_color = "#333333"  # For footers etc. on this slide

    presentation_slides = [
        title_slide(footer_text=current_date_footer_text, slide_styles=presentation_styles),
        portfolio_detail_slide(
            footer_text=current_date_footer_text, slide_styles=presentation_styles
        ),
        pie_chart_bars_slide(
            footer_text=current_date_footer_text, slide_styles=presentation_styles
        ),
        generic_table_and_grouped_bar_chart_slide(
            footer_text=current_date_footer_text, slide_styles=presentation_styles
        ),
        portfolio_performance_slide(
            footer_text=current_date_footer_text, slide_styles=presentation_styles
        ),
        rate_sensitivity_analysis_slide(
            footer_text=current_date_footer_text, slide_styles=presentation_styles
        ),
        issuer_performance_table_slide(
            footer_text=current_date_footer_text, slide_styles=presentation_styles
        ),
        comparative_performance_charts_slide(
            footer_text=current_date_footer_text, slide_styles=presentation_styles
        ),
        data_table_summary_slide(
            footer_text=current_date_footer_text, slide_styles=presentation_styles
        ),
        contact_information_slide(),
    ]

    return Presentation(
        title="Main Sequence Monthly Report (Example)",
        subtitle="May 2025",
        slides=presentation_slides,
    )


if __name__ == "__main__":
    script_dir = Path(__file__).resolve().parent
    output_html_path = (
        script_dir / "ms_template_report.html"
    )  # Output to the same directory as the script
    final_presentation = create_full_presentation()
    try:
        html_content = final_presentation.render()
        with open(output_html_path, "w", encoding="utf-8") as f:
            f.write(html_content)
        print(f"Presentation rendered successfully to {output_html_path.resolve()}")
    except Exception as e:
        print(f"An error occurred during rendering: {e}")
        import traceback

        traceback.print_exc()
