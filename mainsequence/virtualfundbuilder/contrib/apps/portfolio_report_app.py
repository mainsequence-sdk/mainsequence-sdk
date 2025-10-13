import datetime

import pandas as pd
from pydantic import BaseModel

from mainsequence.client import Portfolio
from mainsequence.reportbuilder.model import StyleSettings, ThemeMode
from mainsequence.reportbuilder.slide_templates import generic_plotly_line_chart
from mainsequence.virtualfundbuilder.resource_factory.app_factory import (
    HtmlApp,
    regiester_agent_tool,
)
from mainsequence.virtualfundbuilder.utils import get_vfb_logger

logger = get_vfb_logger()

portfolio_ids = [
    portfolio.id for portfolio in Portfolio.filter(signal_data_node_update__isnull=False)
]


class PortfolioReportConfiguration(BaseModel):
    report_title: str = "Portfolio Report"
    portfolio_ids: list[int] = portfolio_ids
    report_days: int = 365 * 5


@regiester_agent_tool()
class PortfolioReport(HtmlApp):
    configuration_class = PortfolioReportConfiguration

    def run(self) -> str:
        styles = StyleSettings(mode=ThemeMode.light)
        start_date = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
            days=self.configuration.report_days
        )

        series_data = []
        all_dates = pd.Index([])

        portfolio_data_map = {}
        for portfolio_id in self.configuration.portfolio_ids:
            try:
                portfolio = Portfolio.get(id=portfolio_id)
                data = portfolio.data_node_update.get_data_between_dates_from_api()
                data["time_index"] = pd.to_datetime(data["time_index"])
                report_data = (
                    data[data["time_index"] >= start_date].copy().sort_values("time_index")
                )

                if not report_data.empty:
                    portfolio_data_map[portfolio_id] = report_data
                    all_dates = all_dates.union(report_data["time_index"])

            except Exception as e:
                logger.error(f"Could not process portfolio {portfolio_id}. Error: {e}")

        # Second loop: process and normalize data
        for portfolio_id in self.configuration.portfolio_ids:
            if portfolio_id in portfolio_data_map:
                report_data = portfolio_data_map[portfolio_id]
                portfolio = Portfolio.get(id=portfolio_id)

                # Reindex to common date range and forward-fill missing values
                processed_data = (
                    report_data.set_index("time_index").reindex(all_dates).ffill().reset_index()
                )

                # Normalize to 100 at the start of the common date range
                first_price = processed_data["close"].iloc[0]
                normalized_close = (processed_data["close"] / first_price) * 100

                series_data.append(
                    {
                        "name": portfolio.portfolio_name,
                        "y_values": normalized_close,
                        "color": styles.chart_palette_categorical[
                            len(series_data) % len(styles.chart_palette_categorical)
                        ],
                    }
                )

        # Final check if any data was processed
        if not series_data:
            return "<html><body><h1>No data available for the selected portfolios and date range.</h1></body></html>"

        # Call the generic function
        html_chart = generic_plotly_line_chart(
            x_values=list(all_dates),
            series_data=series_data,
            y_axis_title="Indexed Performance (Start = 100)",
            theme_mode=styles.mode,
            full_html=False,
            include_plotlyjs="cdn",
        )
        return html_chart


if __name__ == "__main__":
    configuration = PortfolioReportConfiguration(portfolio_ids=portfolio_ids[:1])
    PortfolioReport(configuration).run()
