from enum import Enum

from pydantic import BaseModel

from mainsequence.client import Portfolio
from mainsequence.reportbuilder.model import StyleSettings, ThemeMode
from mainsequence.reportbuilder.slide_templates import generic_plotly_table
from mainsequence.virtualfundbuilder.resource_factory.app_factory import (
    HtmlApp,
    regiester_agent_tool,
)
from mainsequence.virtualfundbuilder.utils import get_vfb_logger

logger = get_vfb_logger()

portfolio_ids = [
    portfolio.id for portfolio in Portfolio.filter(signal_data_node_update__isnull=False)
]


class ReportType(Enum):
    FIXED_INCOME = "fixed_income"
    LIQUIDITY = "liquidity"


class PortfolioTableConfiguration(BaseModel):
    report_title: str = "Portfolio Table"
    report_type: ReportType = ReportType.FIXED_INCOME
    portfolio_ids: list[int] = portfolio_ids
    report_days: int = 365 * 5


@regiester_agent_tool()
class PortfolioTable(HtmlApp):
    configuration_class = PortfolioTableConfiguration

    def run(self) -> str:
        style = StyleSettings(mode=ThemeMode.light)
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
        chart_font_family = style.font_family_paragraphs
        chart_label_font_size = style.chart_label_font_size

        if self.configuration.report_type == ReportType.FIXED_INCOME:
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
                [
                    "Alpha Bond 2025",
                    "350,000",
                    "$99.50",
                    "$34,825,000.00",
                    "7.50%",
                    "0.25",
                    "9.05%",
                    "90",
                ],
                [
                    "Beta Note 2026",
                    "160,000",
                    "$99.80",
                    "$15,968,000.00",
                    "3.60%",
                    "1.30",
                    "9.15%",
                    "530",
                ],
                [
                    "Gamma Security 2026",
                    "250,000",
                    "$99.90",
                    "$24,975,000.00",
                    "5.60%",
                    "1.50",
                    "9.20%",
                    "600",
                ],
                [
                    "Delta Issue 2027",
                    "245,000",
                    "$100.10",
                    "$24,524,500.00",
                    "5.40%",
                    "1.60",
                    "9.25%",
                    "630",
                ],
                [
                    "Epsilon Paper 2026",
                    "200,000",
                    "$98.50",
                    "$19,700,000.00",
                    "4.40%",
                    "0.80",
                    "8.30%",
                    "300",
                ],
                [
                    "Zeta Bond 2029",
                    "170,000",
                    "$102.50",
                    "$17,425,000.00",
                    "3.90%",
                    "3.30",
                    "8.60%",
                    "1,500",
                ],
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
                [
                    "Theta Note 2034",
                    "110,000",
                    "$93.00",
                    "$10,230,000.00",
                    "2.30%",
                    "6.30",
                    "9.30%",
                    "3,500",
                ],
                [
                    "Iota UDI 2028",
                    "40,000",
                    "$98.00",
                    "$33,600,000.00",
                    "7.90%",
                    "3.20",
                    "4.90%",
                    "1,300",
                ],
                [
                    "Kappa C-Bill 2026A",
                    "2,500,000",
                    "$9.20",
                    "$23,000,000.00",
                    "5.10%",
                    "0.85",
                    "8.40%",
                    "340",
                ],
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

            html_table = generic_plotly_table(
                headers=fixed_income_local_headers,
                rows=fixed_income_local_rows,
                column_widths=shared_column_widths,
                cell_align=shared_cell_align,
                fig_width=table_figure_width,
                header_font_dict=dict(
                    color=style.background_color, size=10, family=chart_font_family
                ),
                cell_font_dict=dict(
                    size=chart_label_font_size,
                    family=chart_font_family,
                    color=style.paragraph_color,
                ),
                theme_mode=style.mode,
                full_html=False,
                include_plotlyjs="cdn",
            )
        else:
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
                # ["Cash Equiv. (USD)", "50,000", "", "$1,000,000.00", "0.20%", "", "", ""],
                # ["TOTAL", "", "", "$56,000,150.00", "12.70%", "", "", ""]
            ]

            html_table = generic_plotly_table(
                headers=liquidity_headers,
                rows=liquidity_rows,
                column_widths=shared_column_widths,
                cell_align=shared_cell_align,
                fig_width=table_figure_width,
                header_font_dict=dict(
                    color=style.background_color, size=10, family=chart_font_family
                ),
                cell_font_dict=dict(
                    size=chart_label_font_size,
                    family=chart_font_family,
                    color=style.paragraph_color,
                ),
                theme_mode=style.mode,
                full_html=False,
                include_plotlyjs="cdn",
            )
        return html_table


if __name__ == "__main__":
    cfg = PortfolioTableConfiguration(
        report_type=ReportType.LIQUIDITY, portfolio_ids=portfolio_ids[:1]
    )
    PortfolioTable(cfg).run()
