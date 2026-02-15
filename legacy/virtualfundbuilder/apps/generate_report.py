#!/usr/bin/env python3

import base64
import os
from datetime import datetime
from io import BytesIO

import plotly.express as px
import plotly.graph_objs as go
from jinja2 import Environment, FileSystemLoader
from pydantic import BaseModel

from mainsequence.client import AssetCategory, DoesNotExist
from mainsequence.client.models_tdag import Artifact
from mainsequence.tdag import APIDataNode
from mainsequence.virtualfundbuilder.resource_factory.app_factory import (
    BaseAgentTool,
    regiester_agent_tool,
)
from mainsequence.virtualfundbuilder.utils import get_vfb_logger

logger = get_vfb_logger()


def example_data(assets):
    """
    Fetch real data from the 'api_ts.get_df_between_dates()' call, then:
      1) Build a time-series chart of 'Revenue' vs. time for each asset (ticker).
      2) Build a correlation heatmap of 'Revenue' vs. 'EPS' for the latest time period.
      3) Return both figures as Base64-encoded PNGs.
    """

    # ------------------------------------------------------------------------------
    # 1) GET THE REAL DATA
    market_time_serie_unique_identifier = "polygon_historical_fundamentals"
    try:
        from mainsequence.client import MarketsTimeSeriesDetails

        hbs = MarketsTimeSeriesDetails.get(unique_identifier=market_time_serie_unique_identifier)
    except DoesNotExist as e:
        logger.exception(
            f"HistoricalBarsSource does not exist for {market_time_serie_unique_identifier}"
        )
        raise e

    api_ts = APIDataNode(
        data_source_id=hbs.related_local_time_serie.data_source_id,
        update_hash=hbs.related_local_time_serie.update_hash,
    )

    # This returns a DataFrame, indexed by (time_index, unique_identifier)
    # with columns like: 'is_revenues', 'is_basic_earnings_per_share', etc.
    data = api_ts.get_df_between_dates()

    # Move the multi-index to columns for easier manipulation
    df = data.reset_index()
    # Example: 'unique_identifier' might be "AAPL_ms_share_class_123xyz"
    # We'll extract the first underscore-delimited part as the 'asset_id'.
    df["asset_id"] = df["unique_identifier"].str.split("_").str[0]

    df = df[df["unique_identifier"].isin([a.unique_identifier for a in assets])]

    # Rename columns to something more readable in the charts
    df["Revenue"] = df["is_revenues"]
    df["EPS"] = df["is_basic_earnings_per_share"]

    # OPTIONAL: If you want to drop rows that have no revenue or EPS data
    # df.dropna(subset=["Revenue", "EPS"], how="all", inplace=True)

    # ------------------------------------------------------------------------------
    # 2) TIME-SERIES LINE CHART: Revenue over time, color by ticker
    fig_line = px.line(
        df, x="time_index", y="Revenue", color="asset_id", title="Revenue Over Time by Asset"
    )
    fig_line.update_layout(xaxis_title="Date", yaxis_title="Revenue")

    # ------------------------------------------------------------------------------
    # 3) CORRELATION HEATMAP
    latest_date = (
        df.groupby("unique_identifier")["quarter"].max().min()
    )  # latest date where all values are present
    df_latest = df[df["quarter"] == latest_date].copy()

    # Pivot so each row is an asset and columns are the fundamental metrics
    # (Here, "Revenue" and "EPS").
    df_pivot = df_latest.pivot_table(
        index="asset_id",
        values=["Revenue", "EPS"],
        aggfunc="mean",  # or 'first' if each (asset, time_index) is unique
    )

    corr_matrix = df_pivot.corr()
    fig_heatmap = go.Figure(
        data=go.Heatmap(
            z=corr_matrix.values, x=corr_matrix.columns, y=corr_matrix.index, colorscale="Blues"
        )
    )
    fig_heatmap.update_layout(title=f"Correlation of Fundamentals on {latest_date}")

    # ------------------------------------------------------------------------------
    # 4) CONVERT PLOTS TO BASE64 STRINGS
    import base64
    from io import BytesIO

    def fig_to_base64(fig):
        """Render a Plotly figure to a PNG and return a base64 string."""
        buf = BytesIO()
        fig.write_image(buf, format="png")
        buf.seek(0)
        return base64.b64encode(buf.read()).decode("utf-8")

    chart1_base64 = fig_to_base64(fig_line)
    chart2_base64 = fig_to_base64(fig_heatmap)

    return chart1_base64, chart2_base64


class ExampleReportConfig(BaseModel):
    """Pydantic model defining the parameters for report generation."""

    report_id: str = "MC-2025"
    report_title: str = "Global Strategy Views: Diversify to Amplify"
    bucket_name: str = "Reports"
    authors: str = "Main Sequence AI"
    sector: str = "US Equities"
    region: str = "USA"
    topics: list[str] = ["Diversification", "Equities", "Fundamentals"]
    asset_category_unique_identifier: str = "magnificent_7"
    summary: str = (
        "We are entering a more benign phase of the economic cycle characterized by "
        "sustained economic growth and declining policy interest rates. Historically, "
        "such an environment supports equities but also highlights the increasing "
        "importance of broad diversification across regions and sectors."
    )


@regiester_agent_tool()
class ExampleReportApp(BaseAgentTool):
    """
    Minimal example of a 'ReportApp' that can:
    1) Generate dummy data and create charts (line + heatmap).
    2) Embed those charts into an HTML template.
    3) Optionally export the HTML to PDF using WeasyPrint.
    """

    configuration_class = ExampleReportConfig

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        category = AssetCategory.get(
            unique_identifier=self.configuration.asset_category_unique_identifier
        )
        self.assets = category.get_assets()
        self.category_name = category.display_name

    def _fig_to_base64(self, fig) -> str:
        """
        Render a Plotly figure to PNG and return a Base64 string.
        """
        buf = BytesIO()
        fig.write_image(buf, format="png")
        buf.seek(0)
        return base64.b64encode(buf.read()).decode("utf-8")

    def run(self):
        """
        Generates an HTML report (and optional PDF) in a minimal, self-contained way.
        """
        print(f"Running tool with configuration {self.configuration}")

        # 1) Retrieve the chart images:
        chart1_base64, chart2_base64 = example_data(self.assets)

        # Build context from config
        template_context = {
            "current_date": datetime.now().strftime("%Y-%m-%d"),
            "current_year": datetime.now().year,
            "logo_location": "https://main-sequence.app/static/media/logos/MS_logo_long_white.png",
            # Pulling fields from our pydantic config:
            "report_id": self.configuration.report_id,
            "authors": self.configuration.authors,
            "sector": self.configuration.sector,
            "region": self.configuration.region,
            "topics": self.configuration.topics,
            "report_title": self.configuration.report_title,
            "summary": self.configuration.summary,
            "report_content": f"""
                <h2>Overview</h2>
                <p>
                    Longer-term interest rates are expected to remain elevated, driven by rising government deficits
                    and persistent term premiums. However, the reduced likelihood of a near-term recession presents
                    opportunities for positive equity returns, notably in sectors like technology and select value-oriented
                    areas such as financials.
                </p>
                <p>
                    This evolving landscape emphasizes the necessity of expanding our investment horizon beyond traditional
                    focuses—such as large-cap US technology—to include regional markets, mid-cap companies, and "Ex-Tech Compounders."
                    Such diversification aims to enhance risk-adjusted returns as global growth trajectories become more aligned.
                </p>
                <h2>Key Takeaways</h2>
                <ul>
                    <li>
                        <strong>Diversification enhances return potential:</strong> Capturing alpha in the upcoming cycle
                        will likely depend on a diversified approach across multiple regions and investment factors.
                    </li>
                    <li>
                        <strong>Technology remains essential:</strong> Rising demand for physical infrastructure, such as
                        data centers and AI-supportive hardware, will benefit traditional industrial sectors, creating
                        new investment opportunities.
                    </li>
                    <li>
                        <strong>Divergent interest rate dynamics:</strong> Central banks have started easing policies, but
                        persistent high bond yields imply limitations on further equity valuation expansions.
                    </li>
                </ul>

                <!-- Page break before next section if printing to PDF -->
                <div style="page-break-after: always;"></div>

                <h2>Fundamental Trends and Correlation Analysis</h2>
                <p>
                    The following charts illustrate recent fundamental trends among selected US equities, focusing specifically
                    on revenue performance over recent reporting periods. This analysis leverages data obtained via the internal
                    "pylong" API, clearly highlighting the evolving top-line dynamics across multiple companies.
                </p>
                <p style="text-align:center;">
                    <img alt="Revenue Over Time"
                         src="data:image/png;base64,{chart1_base64}"
                         style="max-width:600px; width:100%;">
                </p>

                <p>
                    Further, the correlation heatmap below illustrates the relationships between key fundamental indicators—such
                    as Revenue and Earnings Per Share (EPS)—across companies at the latest reporting date. This visualization
                    provides strategic insights into how closely fundamental metrics move together, enabling more informed
                    portfolio allocation decisions.
                </p>
                <p style="text-align:center;">
                    <img alt="Correlation Heatmap"
                         src="data:image/png;base64,{chart2_base64}"
                         style="max-width:600px; width:100%;">
                </p>

                <p>
                    As the macroeconomic environment evolves, shifts in these fundamental correlations may offer additional
                    opportunities for strategic repositioning and optimized sector exposures.
                </p>
            """,
        }

        """
        Renders a static HTML report from Jinja2 templates, embedding two charts as Base64 images,
        and (optionally) saves it as PDF using WeasyPrint.
        """
        # 2) Setup the Jinja2 environment: (point to the templates directory)
        template_dir = os.path.join(os.path.dirname(__file__), "templates")
        env = Environment(loader=FileSystemLoader(template_dir), autoescape=False)

        # 3) Load the derived template (which should reference placeholders in Jinja syntax).
        template = env.get_template("report.html")

        # 5) Render the HTML:
        rendered_html = template.render(template_context)

        # 6) Write the rendered HTML to a file
        output_html_path = os.path.join(os.path.dirname(__file__), "output_report.html")
        with open(output_html_path, "w", encoding="utf-8") as f:
            f.write(rendered_html)

        print(f"HTML report generated: {output_html_path}")

        print("Generated HTML:", output_html_path)

        # from weasyprint import HTML
        # pdf_path = "/tmp/report.pdf"
        # HTML(string=rendered_html).write_pdf(pdf_path)
        # print(f"PDF generated: {pdf_path}")
        # pdf_artifact = Artifact.upload_file(filepath=pdf_path, name="Report PDF", created_by_resource_name=self.__class__.__name__, bucket_name="Reports")
        html_artifact = Artifact.upload_file(
            filepath=output_html_path,
            name=self.configuration.report_id,
            created_by_resource_name=self.__class__.__name__,
            bucket_name=self.configuration.bucket_name,
        )


if __name__ == "__main__":
    # Example usage:
    config = ExampleReportConfig()  # Or override fields as needed
    app = ExampleReportApp(config)
    html_artifact = app.run()  # Creates output_report.html and weasy_output_report.pdf
