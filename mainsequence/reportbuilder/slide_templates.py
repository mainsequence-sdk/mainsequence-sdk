from typing import Any

import pandas as pd
import plotly.graph_objects as go

from mainsequence.reportbuilder.model import (
    ThemeMode,
    get_theme_settings,
)


def _transpose_for_plotly(data_rows: list[list[Any]], num_columns: int) -> list[list[Any]]:
    if not data_rows:
        return [[] for _ in range(num_columns)]
    transposed = list(map(list, zip(*data_rows, strict=False)))
    return transposed


def generic_plotly_table(
    headers: list[str],
    rows: list[list[Any]],
    table_height: int | None = None,  # MODIFIED: Made optional for auto-sizing
    fig_width: int | None = None,
    column_widths: list[int | float] | None = None,
    cell_align: str | list[str] = "left",
    header_align: str = "center",
    cell_font_dict: dict[str, Any] | None = None,
    header_font_dict: dict[str, Any] | None = None,
    header_fill_color: str | None = None,
    cell_fill_color: str = "#F5F5F5",
    line_color: str = "#DCDCDC",
    header_height: int = 22,
    cell_height: int = 20,
    margin_dict: dict[str, int] | None = None,
    paper_bgcolor: str = "rgba(0,0,0,0)",
    plot_bgcolor: str = "rgba(0,0,0,0)",
    responsive: bool = True,
    display_mode_bar: bool = False,
    include_plotlyjs: str = False,
    full_html: bool = False,
    column_formats: list[str] | None = None,
    theme_mode: ThemeMode = ThemeMode.light,
) -> str:
    settings = get_theme_settings(theme_mode)

    effective_margin_dict = margin_dict if margin_dict is not None else dict(l=5, r=5, t=2, b=2)

    if header_fill_color is None:
        header_fill_color = settings.primary_color
    if cell_font_dict is None:
        cell_font_dict = dict(size=12)
    if header_font_dict is None:
        header_font_dict = dict(
            color=settings.background_color,
            size=14,
        )

    plotly_column_data = _transpose_for_plotly(rows, len(headers))
    # Build cell properties, injecting formats if provided
    cell_props = dict(
        values=plotly_column_data,
        fill_color=cell_fill_color,
        font=cell_font_dict,
        align=cell_align,
        line_color=line_color,
        height=cell_height,
    )
    if column_formats:
        cell_props["format"] = column_formats

    fig = go.Figure(
        data=[
            go.Table(
                header=dict(
                    values=headers,
                    fill_color=header_fill_color,
                    font=header_font_dict,
                    align=header_align,
                    line_color=line_color,
                    height=header_height if headers else 0,
                ),
                cells=cell_props,
                columnwidth=column_widths if column_widths else [],
            )
        ]
    )

    determined_fig_height: int

    if table_height is None:
        content_actual_height = (header_height if headers else 0) + (len(rows) * cell_height)
        # Figure height needs to include its own top/bottom margins
        determined_fig_height = (
            content_actual_height
            + effective_margin_dict.get("t", 0)
            + effective_margin_dict.get("b", 0)
            + 4
        )  # Small buffer for any internal Plotly paddings
    else:
        determined_fig_height = table_height

    layout_args = {
        "height": determined_fig_height,
        "margin": effective_margin_dict,
        "paper_bgcolor": paper_bgcolor,
        "plot_bgcolor": plot_bgcolor,
    }
    if fig_width:
        layout_args["width"] = fig_width

    fig.update_layout(**layout_args)
    html = fig.to_html(
        include_plotlyjs=include_plotlyjs,
        full_html=full_html,
        config={"responsive": responsive, "displayModeBar": display_mode_bar},
    )
    html = html.replace(
        'style="height:100%; width:100%;"', 'style="width:100%;"'
    )  # drop 100 % height
    return html


def generic_plotly_pie_chart(
    labels: list[str],
    values: list[int | float],
    height: int = 400,
    width: int = 450,
    title: str | None = None,
    colors: list[str] | None = None,
    textinfo: str = "percent+label",
    textfont_dict: dict[str, Any] | None = None,
    hoverinfo: str = "label+percent+value",
    showlegend: bool = True,
    legend_dict: dict[str, Any] | None = None,
    margin_dict: dict[str, int] | None = None,
    paper_bgcolor: str = "rgba(0,0,0,0)",
    plot_bgcolor: str = "rgba(0,0,0,0)",
    font_dict: dict[str, Any] | None = None,
    sort_traces: bool = False,
    responsive: bool = True,
    display_mode_bar: bool = False,
    include_plotlyjs: str = False,
    full_html: bool = False,
    theme_mode: ThemeMode = ThemeMode.light,
) -> str:

    settings = get_theme_settings(theme_mode)
    if textfont_dict is None:
        textfont_dict = dict(size=11)
    if legend_dict is None:
        legend_dict = dict(
            font=dict(size=9, family="Lato, Arial, Helvetica, sans-serif"),
            orientation="h",
            yanchor="top",
            y=-0.1,
            xanchor="center",
            x=0.5,
            bgcolor="rgba(0,0,0,0)",
        )
    if margin_dict is None:
        margin_dict = dict(l=10, r=10, t=10, b=100 if showlegend else 20)
    if font_dict is None:
        font_dict = dict(family="Lato, Arial, Helvetica, sans-serif")
    if colors is None:
        colors = settings.chart_palette_categorical

    fig = go.Figure(
        data=[
            go.Pie(
                labels=labels,
                values=values,
                marker_colors=colors,
                textinfo=textinfo,
                textfont=textfont_dict,
                hoverinfo=hoverinfo,
                sort=sort_traces,
                showlegend=showlegend,
            )
        ]
    )

    fig.update_layout(
        title_text=title,
        height=height,
        width=width,
        margin=margin_dict,
        paper_bgcolor=paper_bgcolor,
        plot_bgcolor=plot_bgcolor,
        font=font_dict,
        legend=legend_dict,
    )
    return fig.to_html(
        include_plotlyjs=include_plotlyjs,
        full_html=full_html,
        config={"responsive": responsive, "displayModeBar": display_mode_bar},
    )


def generic_plotly_bar_chart(
    y_values: list[str | int | float],
    x_values: list[int | float],
    orientation: str = "h",
    height: int = 400,
    width: int = 450,
    title: str | None = None,
    bar_color: str | list[str] = None,
    text_template: str | None = None,
    textposition: str = "outside",
    textfont_dict: dict[str, Any] | None = None,
    hoverinfo: str = "x+y",  # Default, will be adapted by plotly if orientation changes
    margin_dict: dict[str, int] | None = None,
    paper_bgcolor: str = "rgba(0,0,0,0)",
    plot_bgcolor: str = "rgba(0,0,0,0)",
    xaxis_dict: dict[str, Any] | None = None,
    yaxis_dict: dict[str, Any] | None = None,
    bargap: float = 0.2,  # Slightly reduced default bargap for a tighter look
    font_dict: dict[str, Any] | None = None,
    responsive: bool = True,
    display_mode_bar: bool = False,
    include_plotlyjs: bool = False,
    full_html: bool = False,
    theme_mode: ThemeMode = ThemeMode.light,
) -> str:
    # 1) theme & defaults
    settings = get_theme_settings(theme_mode)
    if bar_color is None:
        bar_color = settings.chart_palette_categorical
    if textfont_dict is None:
        textfont_dict = dict(size=9)
    if margin_dict is None:
        # extra left margin for horizontal labels
        margin_dict = dict(l=150 if orientation == "h" else 40, r=20, t=5, b=20)
    if font_dict is None:
        font_dict = dict(family="Lato, Arial, Helvetica, sans-serif")

    # 2) axis defaults (no autorange reversal)
    default_axis = dict(
        showgrid=False, zeroline=False, showline=False, tickfont=dict(size=9, color="#333333")
    )
    xaxis_cfg = xaxis_dict.copy() if xaxis_dict else default_axis.copy()
    yaxis_cfg = yaxis_dict.copy() if yaxis_dict else default_axis.copy()

    # 3) build data_params correctly
    if orientation == "h":
        # horizontal: numeric→x, categories→y
        data_params = dict(x=x_values, y=y_values)
        if text_template is None:
            text_template = "%{x:.2f}"
    elif orientation == "v":
        # vertical: categories→x, numeric→y
        data_params = dict(x=y_values, y=x_values)
        if text_template is None:
            text_template = "%{y:.2f}"
    else:
        raise ValueError("Orientation must be 'h' or 'v'")

    # 4) build the figure
    fig = go.Figure(
        go.Bar(
            **data_params,
            orientation=orientation,
            marker_color=bar_color,
            texttemplate=text_template,
            textposition=textposition,
            textfont=textfont_dict,
            hoverinfo=hoverinfo,
        )
    )

    fig.update_layout(
        title_text=title,
        height=height,
        width=width,
        margin=margin_dict,
        paper_bgcolor=paper_bgcolor,
        plot_bgcolor=plot_bgcolor,
        xaxis=xaxis_cfg,
        yaxis=yaxis_cfg,
        bargap=bargap,
        font=font_dict,
    )
    return fig.to_html(
        include_plotlyjs=include_plotlyjs,
        full_html=full_html,
        config={"responsive": responsive, "displayModeBar": display_mode_bar},
    )


def generic_plotly_grouped_bar_chart(
    x_values: list[str],
    series_data: list[dict[str, Any]],
    height: int,
    chart_title: str = "",
    width: int | None = None,
    y_axis_tick_format: str | None = ".2f",
    bar_text_template: str | None = "%{y:.2f}",
    bar_text_position: str = "outside",
    bar_text_font_size_factor: float = 1.0,
    barmode: str = "group",
    legend_dict: dict[str, Any] | None = None,
    margin_dict: dict[str, int] | None = None,
    title_x_position: float = 0.05,
    xaxis_tickangle: float | None = None,
    paper_bgcolor: str = "rgba(0,0,0,0)",
    plot_bgcolor: str = "rgba(0,0,0,0)",
    include_plotlyjs: bool = False,
    full_html: bool = False,
    display_mode_bar: bool = False,
    responsive: bool = True,
    theme_mode: ThemeMode = ThemeMode.light,
) -> str:
    fig = go.Figure()
    styles = get_theme_settings(theme_mode)

    for counter, series in enumerate(series_data):
        marker_color = series.get("color", None)
        if marker_color is None:
            marker_color = styles.chart_palette_categorical[
                counter % len(styles.chart_palette_categorical)
            ]
        trace = go.Bar(
            name=series["name"], x=x_values, y=series["y_values"], marker_color=marker_color
        )
        if bar_text_template:
            trace.texttemplate = bar_text_template
            trace.textposition = bar_text_position
            trace.textfont = dict(
                size=int(styles.chart_label_font_size * bar_text_font_size_factor),
                family=styles.font_family_paragraphs,
                color=styles.paragraph_color,
            )
        fig.add_trace(trace)

    all_y = [y for series in series_data for y in series["y_values"]]
    y_max = max(all_y) * 1.1  # 10% above the tallest bar
    y_min = min(all_y) * 1.1 if min(all_y) < 0 else min(all_y) * 0.9

    default_legend_config = dict(
        font=dict(size=styles.chart_label_font_size, family=styles.font_family_paragraphs),
        orientation="h",
        yanchor="bottom",
        y=1.02,
        xanchor="right",
        x=1,
        bgcolor="rgba(0,0,0,0)",
    )
    if legend_dict is not None:
        default_legend_config.update(legend_dict)

    final_margin_dict = (
        margin_dict
        if margin_dict is not None
        else dict(l=40, r=20, t=50 if chart_title else 50, b=50)
    )
    if xaxis_tickangle is not None and xaxis_tickangle != 0:
        final_margin_dict["b"] = max(
            final_margin_dict.get("b", 30), 70 + abs(xaxis_tickangle) // 10 * 5
        )

    fig.update_layout(
        title_text=chart_title,
        title_font=dict(
            size=styles.font_size_h4,
            family=styles.font_family_paragraphs,
            color=styles.heading_color,
        ),
        title_x=title_x_position,
        height=height,
        width=width,
        barmode=barmode,
        xaxis_tickfont_size=styles.chart_label_font_size,
        yaxis_tickfont_size=styles.chart_label_font_size,
        yaxis_tickformat=y_axis_tick_format,
        yaxis=dict(range=[y_min, y_max]),  # <-- set min/max here
        legend=default_legend_config,
        margin=final_margin_dict,
        paper_bgcolor=paper_bgcolor,
        plot_bgcolor=plot_bgcolor,
        font=dict(family=styles.font_family_paragraphs),
    )

    if xaxis_tickangle is not None:
        fig.update_xaxes(tickangle=xaxis_tickangle)

    return fig.to_html(
        include_plotlyjs=include_plotlyjs,
        full_html=full_html,
        config={"responsive": responsive, "displayModeBar": display_mode_bar},
    )


def _build_traces(
    x_values: list, series_data: list[dict[str, Any]], styles: Any
) -> list[go.Scatter]:
    traces = []
    palette = styles.chart_palette_categorical
    for i, series in enumerate(series_data):
        traces.append(
            go.Scatter(
                name=series.get("name", f"Series {i + 1}"),
                x=x_values,
                y=series["y_values"],
                mode="lines",
                line=dict(color=series.get("color", palette[i % len(palette)]), width=2),
                hoverinfo="x+y+name",
            )
        )
    return traces


def _build_layout(
    styles: Any,
    chart_title: str,
    y_axis_title: str,
    y_axis_tick_format: str | None,
    legend_dict: dict[str, Any] | None,
    margin_dict: dict[str, int] | None,
    theme_mode: Any,
    height: int | None,
    width: int | None,
) -> dict[str, Any]:
    # legend
    default_legend = dict(
        font=dict(size=styles.chart_label_font_size, family=styles.font_family_paragraphs),
        orientation="h",
        yanchor="bottom",
        y=1.02,
        xanchor="right",
        x=1,
        bgcolor="rgba(0,0,0,0)",
    )
    if legend_dict:
        default_legend.update(legend_dict)

    # margin
    final_margin = margin_dict or dict(l=60, r=40, t=60 if chart_title else 20, b=50)

    layout = dict(
        title_text=chart_title,
        title_font=dict(
            size=styles.font_size_h4, family=styles.font_family_headings, color=styles.heading_color
        ),
        title_x=0.5,
        legend=default_legend,
        margin=final_margin,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family=styles.font_family_paragraphs, color=styles.paragraph_color),
        xaxis=dict(
            showgrid=True,
            gridcolor=styles.light_paragraph_color if theme_mode == ThemeMode.light else "#444",
            gridwidth=0.5,
            zeroline=False,
        ),
        yaxis=dict(
            title=y_axis_title,
            tickformat=y_axis_tick_format,
            showgrid=True,
            gridcolor=styles.light_paragraph_color if theme_mode == ThemeMode.light else "#444",
            gridwidth=0.5,
            zeroline=False,
        ),
    )
    if height:
        layout["height"] = height
    if width:
        layout["width"] = width

    return layout


def generic_plotly_line_chart(
    x_values: list,
    series_data: list[dict[str, Any]],
    # height and width are now optional for autosizing
    height: int | None = None,
    width: int | None = None,
    chart_title: str = "",
    y_axis_title: str = "",
    y_axis_tick_format: str | None = ".2f",
    legend_dict: dict[str, Any] | None = None,
    margin_dict: dict[str, int] | None = None,
    theme_mode: ThemeMode = ThemeMode.light,
    include_plotlyjs: str = "cdn",
    full_html: bool = True,
    display_mode_bar: bool = False,
    responsive: bool = True,
) -> str:
    """
    Responsive multi-series line chart.
    """
    styles = get_theme_settings(theme_mode)
    fig = go.Figure()

    # add traces
    for trace in _build_traces(x_values, series_data, styles):
        fig.add_trace(trace)

    # apply layout
    layout_args = _build_layout(
        styles,
        chart_title,
        y_axis_title,
        y_axis_tick_format,
        legend_dict,
        margin_dict,
        theme_mode,
        height,
        width,
    )
    fig.update_layout(**layout_args)

    return fig.to_html(
        include_plotlyjs=include_plotlyjs,
        full_html=full_html,
        config={"responsive": responsive, "displayModeBar": display_mode_bar},
    )


def plot_dataframe_line_chart(
    df: pd.DataFrame, x_column: str | None = None, columns: list[str] | None = None, **plot_kwargs
) -> str:
    """
    Plot all specified columns from df against the x_column.
    Any generic_plotly_line_chart kwargs can be passed through plot_kwargs.
    """
    index_name = df.index.name
    df = df.reset_index()
    x_column = x_column or index_name
    columns = columns or [c for c in df.columns if c != index_name]
    series_data = [
        {
            "name": col,
            "y_values": df[col].tolist(),
            # optionally add 'color': ... here if you want fixed colors per column
        }
        for col in columns
    ]
    x_values = df[x_column].tolist()
    # delegate to generic
    return generic_plotly_line_chart(x_values=x_values, series_data=series_data, **plot_kwargs)
