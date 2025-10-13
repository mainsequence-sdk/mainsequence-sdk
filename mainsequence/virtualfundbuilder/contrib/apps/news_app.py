#!/usr/bin/env python3
import base64
import os
from datetime import datetime, timedelta
from io import BytesIO

import pandas as pd
import plotly.graph_objs as go
from jinja2 import Environment, FileSystemLoader
from pydantic import BaseModel

from mainsequence.client import AssetCategory
from mainsequence.client.models_tdag import Artifact

# Assuming TDAGAgent is correctly set up and accessible in the execution environment
from mainsequence.virtualfundbuilder.agent_interface import TDAGAgent
from mainsequence.virtualfundbuilder.resource_factory.app_factory import (
    BaseAgentTool,
    regiester_agent_tool,
)
from mainsequence.virtualfundbuilder.utils import get_vfb_logger

logger = get_vfb_logger()

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")


class SentimentReportConfig(BaseModel):
    """Pydantic model defining parameters for the Sentiment Report."""

    asset_category_unique_identifier: str = "magnificent_7"
    report_days: int = 14
    report_title: str = "Multi-Ticker News Sentiment & Headlines Report"
    bucket_name: str = "SentimentReports"  # Optional: For artifact storage
    authors: str = "Automated Analysis (Main Sequence AI)"
    sector: str = "Technology Focus"
    region: str = "Global"
    news_items_per_day_limit: int = 5
    report_id: str | None = "MS_SentimentReport"


@regiester_agent_tool()
class SentimentReport(BaseAgentTool):
    """
    Generates an HTML report summarizing news sentiment and headlines
    for a list of stock tickers using data from Polygon.io.
    Additionally, fetches the first 100 words of each article (if possible)
    and generates a single combined summary displayed below the combined chart.
    """

    configuration_class = SentimentReportConfig

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        from polygon import RESTClient

        if not POLYGON_API_KEY:
            raise ValueError(
                "Warning: POLYGON_API_KEY environment variable not set. Data fetching will fail."
            )

        self.tdag_agent = TDAGAgent()

        logger.info(
            f"Initializing Sentiment Report with configuration {self.configuration.model_dump()}"
        )

        end_date_dt = datetime.now()
        start_date_dt = end_date_dt - timedelta(days=self.configuration.report_days - 1)
        self.start_date = start_date_dt.strftime("%Y-%m-%d")
        self.end_date = end_date_dt.strftime("%Y-%m-%d")

        category = AssetCategory.get(
            unique_identifier=self.configuration.asset_category_unique_identifier
        )
        self.tickers = [a.ticker for a in category.get_assets()]
        self.category_name = category.display_name

        # Initialize Polygon client once if API key exists
        self.polygon_client = RESTClient(POLYGON_API_KEY) if POLYGON_API_KEY else None

        # Setup Jinja2 environment once
        self._setup_jinja()

    def _setup_jinja(self):
        """Initializes the Jinja2 environment."""
        template_dir = os.path.join(os.path.dirname(__file__), "templates")
        if not os.path.isdir(template_dir):
            raise FileNotFoundError(f"Jinja2 template directory not found: {template_dir}")
        report_template_path = os.path.join(template_dir, "report.html")
        if not os.path.isfile(report_template_path):
            raise FileNotFoundError(f"Jinja2 report template not found: {report_template_path}")
        self.jinja_env = Environment(loader=FileSystemLoader(template_dir), autoescape=True)

    def _fetch_data(self) -> (dict[str, pd.DataFrame], dict[str, dict[str, list[dict]]]):
        """
        Fetches sentiment counts and news headlines for configured tickers and date range.
        Returns:
            Tuple[Dict[str, pd.DataFrame], Dict[str, Dict[str, List[Dict]]]]:
                - Sentiment data per ticker (date-indexed DataFrame).
                - News items (title, url) per ticker per date.
        """
        if not self.polygon_client:
            logger.info("Error: Polygon API key not configured. Cannot fetch data.")
            empty_sentiment = {ticker: pd.DataFrame() for ticker in self.tickers}
            empty_news = {ticker: {} for ticker in self.tickers}
            return empty_sentiment, empty_news

        tickers = self.tickers
        start_date = self.start_date
        end_date = self.end_date

        results = {}
        all_news = {}
        date_range = pd.date_range(start=start_date, end=end_date)

        logger.info(f"Fetching data for tickers: {tickers} from {start_date} to {end_date}")

        for ticker in tickers:
            logger.info(f" -> Fetching for {ticker}...")
            sentiment_count = []
            ticker_news_by_date = {}

            for day in date_range:
                day_str = day.strftime("%Y-%m-%d")
                try:
                    # Fetch news for the day
                    daily_news_response = list(
                        self.polygon_client.list_ticker_news(
                            ticker=ticker,
                            published_utc=day_str,
                            limit=100,  # Limit news fetched per day
                        )
                    )
                except Exception as e:
                    logger.info(f"    Error fetching news for {ticker} on {day_str}: {e}")
                    daily_news_response = []

                daily_sentiment = {"date": day, "positive": 0, "negative": 0, "neutral": 0}
                daily_news_items_for_report = []  # Store dicts with 'title' & 'url'

                for article in daily_news_response:
                    # Extract headline and URL for the report list
                    if hasattr(article, "title") and hasattr(article, "article_url"):
                        daily_news_items_for_report.append(
                            {"title": article.title, "url": article.article_url}
                        )

                    # Extract sentiment from insights
                    if hasattr(article, "insights") and article.insights:
                        for insight in article.insights:
                            if hasattr(insight, "sentiment"):
                                sentiment = insight.sentiment
                                if sentiment == "positive":
                                    daily_sentiment["positive"] += 1
                                elif sentiment == "negative":
                                    daily_sentiment["negative"] += 1
                                elif sentiment == "neutral":
                                    daily_sentiment["neutral"] += 1

                sentiment_count.append(daily_sentiment)

                if daily_news_items_for_report:
                    ticker_news_by_date[day_str] = daily_news_items_for_report

            # Prepare the sentiment DataFrame for this ticker
            if sentiment_count:
                df_sentiment = pd.DataFrame(sentiment_count)
                df_sentiment["date"] = pd.to_datetime(df_sentiment["date"])
                df_sentiment.set_index("date", inplace=True)
                # Ensure all dates in the range are present
                df_sentiment = df_sentiment.reindex(date_range, fill_value=0)
                results[ticker] = df_sentiment
                all_news[ticker] = ticker_news_by_date
            else:
                # No sentiment data found
                logger.info(f"    No sentiment data found for {ticker} in the date range.")
                results[ticker] = pd.DataFrame(
                    index=date_range, columns=["positive", "negative", "neutral"]
                ).fillna(0)
                all_news[ticker] = {}

        return results, all_news

    def _download_article_previews(self, all_news_data, words_per_article=50, articles_per_day=2):
        from newspaper import Article

        article_snippets = []
        if self.polygon_client and Article is not None:
            logger.info("\nGathering first 50 words from each article...")
            for ticker, date_dict in all_news_data.items():
                for date_str, articles in date_dict.items():
                    # restrict to 2 items per day
                    for article_info in articles[:articles_per_day]:
                        url = article_info.get("url")
                        if not url:
                            continue
                        try:
                            art = Article(url)
                            art.download()
                            art.parse()
                            # Grab first 100 words
                            words = art.text.split()
                            snippet = " ".join(words[:words_per_article])
                            if snippet:
                                article_snippets.append(snippet)
                        except Exception as e:
                            logger.info(
                                f"    Could not retrieve/parse text from {url} for {ticker} due to: {e}"
                            )
        else:
            logger.info(
                "\nSkipping article text retrieval (Polygon client or newspaper not available)."
            )

        return article_snippets

    def _generate_plot(self, df_sentiment: pd.DataFrame, chart_title: str) -> str | None:
        """
        Generates a Plotly sentiment chart and returns it as a Base64 encoded PNG string.
        Returns None if no data to plot or if image generation fails.
        """
        if df_sentiment.empty or (
            df_sentiment[["positive", "negative", "neutral"]].sum().sum() == 0
        ):
            logger.info(f"    No data to plot for '{chart_title}'. Skipping chart generation.")
            return None

        x_axis_data = df_sentiment.index.to_pydatetime()

        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=x_axis_data,
                y=df_sentiment["positive"],
                mode="lines+markers",
                name="Positive",
                line=dict(color="green"),
                marker=dict(size=5),
            )
        )
        fig.add_trace(
            go.Scatter(
                x=x_axis_data,
                y=df_sentiment["negative"],
                mode="lines+markers",
                name="Negative",
                line=dict(color="red"),
                marker=dict(size=5),
            )
        )
        fig.add_trace(
            go.Scatter(
                x=x_axis_data,
                y=df_sentiment["neutral"],
                mode="lines+markers",
                name="Neutral",
                line=dict(color="gray", dash="dash"),
                marker=dict(size=5),
            )
        )

        fig.update_layout(
            title=f"{chart_title} News Sentiment Over Time",
            xaxis_title="Date",
            yaxis_title="Sentiment Count",
            legend_title="Sentiment",
            width=850,
            height=450,
            margin=dict(l=40, r=40, t=60, b=40),
            xaxis=dict(type="date", tickformat="%Y-%m-%d"),
        )

        buf = BytesIO()
        try:
            fig.write_image(buf, format="png", scale=2)
            buf.seek(0)
            encoded_plot = base64.b64encode(buf.read()).decode("utf-8")
            return encoded_plot
        except Exception as e:
            logger.info(f"    Error generating PNG for '{chart_title}': {e}")
            return None

    def _format_ticker_sections(self, all_sentiment_data, all_news_data):
        ticker_sections_html = ""
        logger.info("\nGenerating individual ticker sections...")
        for ticker in self.tickers:
            df_sentiment = all_sentiment_data.get(ticker)
            ticker_news = all_news_data.get(ticker, {})
            ticker_html = f"<h2>{ticker} Sentiment & News</h2>\n"
            logger.info(f" -> Processing {ticker}")

            # Plot for this ticker
            chart_base64 = self._generate_plot(df_sentiment, ticker)
            if chart_base64:
                ticker_html += f"""
                     <div style="text-align: center; margin-bottom: 20px;">
                         <img alt="{ticker} Sentiment Chart" src="data:image/png;base64,{chart_base64}"
                              style="max-width:850px; width:100%; display: block; margin:auto;">
                     </div>"""
            else:
                ticker_html += f"<p>No plottable sentiment data available for {ticker}.</p>\n"

            # Recent News Headlines (Details)
            ticker_html += "<h5>News Headlines</h5>\n"
            if ticker_news:
                sorted_dates = sorted(ticker_news.keys(), reverse=True)
                news_list_html = ""
                for date_str in sorted_dates:
                    news_items = ticker_news[date_str]
                    if news_items:
                        items_to_show = news_items[: self.configuration.news_items_per_day_limit]
                        if items_to_show:
                            news_list_html += f"{date_str}\n<ul class='list-unstyled'>\n"
                            for item in items_to_show:
                                safe_title = (
                                    item.get("title", "No Title")
                                    .replace("<", "&lt;")
                                    .replace(">", "&gt;")
                                )
                                url = item.get("url", "#")
                                news_list_html += (
                                    f"  <li><a href='{url}' target='_blank' rel='noopener noreferrer'>"
                                    f"{safe_title}</a></li>\n"
                                )
                            news_list_html += "</ul>\n"
                ticker_html += (
                    news_list_html
                    if news_list_html
                    else "<p>No recent news headlines found based on limits.</p>\n"
                )
            else:
                ticker_html += "<p>No news headlines found for this period.</p>\n"

            ticker_html += '<hr style="margin: 30px 0;">\n'
            ticker_sections_html += ticker_html
        return ticker_sections_html

    def run(self) -> str:
        """
        Orchestrates the report generation process:
          1. Fetch data,
          2. Create plots,
          3. Attempt to retrieve article text (first 100 words) for all articles,
          4. Generate a single combined summary from those snippets,
          5. Render HTML,
          6. Upload artifact.
        """
        logger.info(
            f"Running Sentiment Report with configuration: {self.configuration.model_dump()}"
        )

        # Step 1: Fetch sentiment and news data
        all_sentiment_data, all_news_data = self._fetch_data()

        # Step 2: Create a combined (all tickers) sentiment chart
        valid_dfs = [df for df in all_sentiment_data.values() if not df.empty]
        combined_chart_base64 = None
        if valid_dfs:
            combined_df = pd.concat(valid_dfs).groupby(level=0).sum()
            combined_chart_base64 = self._generate_plot(combined_df, "All Tickers (Combined)")

        if combined_chart_base64:
            combined_chart_html = f"""
                <h2>Combined Sentiment Across All Tickers</h2>
                <p style="text-align:center;">
                    <img alt="All Tickers Combined Sentiment Chart"
                         src="data:image/png;base64,{combined_chart_base64}"
                         style="max-width:850px; width:100%; display: block; margin:auto;">
                </p><hr style="margin: 30px 0;">"""
        else:
            combined_chart_html = "<h2>Combined Sentiment</h2><p>No combined sentiment data available.</p><hr style='margin: 30px 0;'>"

        # Step 3: Attempt to retrieve the first 100 words from each article and accumulate
        article_snippets = self._download_article_previews(all_news_data=all_news_data)

        # Step 4: Generate one single combined summary from all article snippets
        combined_article_snippets_summary_html = ""
        if article_snippets:
            # Combine all snippets into one string
            combined_text = "\n".join(article_snippets)
            summary_prompt = (
                f"Please summarize the following text in about 150 words, focus on the assets {self.tickers}:\n\n"
                f"{combined_text}"
            )
            logger.info("\nGenerating combined summary of article snippets...")
            try:
                combined_summary_text = self.tdag_agent.query_agent(summary_prompt)
                combined_article_snippets_summary_html = f"""
                    <h3>Summary (AI-Generated)</h3>
                    <p>{combined_summary_text}</p>
                    <hr style="margin: 30px 0;">"""
            except Exception as e:
                logger.info(f"    Error generating combined summary: {e}")
                combined_article_snippets_summary_html = (
                    "<h3>Summary (AI-Generated)</h3>" f"<p>Error generating summary: {e}</p><hr>"
                )
        else:
            combined_article_snippets_summary_html = (
                "<h3>Summary (AI-Generated)</h3>"
                "<p>No article snippets found to summarize.</p>"
                "<hr>"
            )

        # Step 5: Build up the per-ticker sections
        ticker_sections_html = self._format_ticker_sections(all_sentiment_data, all_news_data)

        # Construct the overall report HTML
        report_content_html = f"""
            <h2>Overview</h2>
            <p>This report summarizes daily sentiment counts (positive/negative/neutral)
            derived from Polygon.io news article insights for each requested ticker,
            within the date range {self.start_date} to {self.end_date}.</p>

            {combined_chart_html}
            {combined_article_snippets_summary_html}

            {ticker_sections_html}
        """

        template_context = {
            "report_title": self.configuration.report_title,
            "report_id": self.configuration.report_id,
            "current_date": datetime.now().strftime("%Y-%m-%d"),
            "authors": self.configuration.authors,
            "sector": self.configuration.sector,
            "region": self.configuration.region,
            "topics": ["Sentiment Analysis", "News Aggregation", "Market Data", "Equities"],
            "current_year": datetime.now().year,
            "summary": (
                f"Daily sentiment analysis, plus combined and per-ticker summaries, "
                f"for the {self.category_name} category from {self.start_date} to {self.end_date}."
            ),
            "report_content": report_content_html,
            "logo_location": "https://main-sequence.app/static/media/logos/MS_logo_long_white.png",
        }

        template = self.jinja_env.get_template("report.html")
        rendered_html = template.render(template_context)

        output_html_path = os.path.join(
            os.path.dirname(__file__), "multi_ticker_sentiment_report.html"
        )
        try:
            with open(output_html_path, "w", encoding="utf-8") as f:
                f.write(rendered_html)
            logger.info(f"\nHTML report generated successfully: {output_html_path}")
            logger.info(f"View the report at: file://{os.path.abspath(output_html_path)}")
        except Exception as e:
            logger.info(f"\nError writing HTML report to file: {e}")

        html_artifact = None
        try:
            html_artifact = Artifact.upload_file(
                filepath=output_html_path,
                name=self.configuration.report_id + f"_{self.category_name}.html",
                created_by_resource_name=self.__class__.__name__,
                bucket_name=self.configuration.bucket_name,
            )
            logger.info(
                f"Artifact uploaded successfully: {html_artifact.id if html_artifact else 'Failed'}"
            )
        except Exception as e:
            logger.info(f"Error uploading artifact: {e}")

        self.add_output(html_artifact)
        return html_artifact


# --- Main Execution Guard ---
if __name__ == "__main__":
    try:
        import kaleido
    except ImportError:
        logger.info("Warning: 'kaleido' package not found. Plotly image export might fail.")
        logger.info("Consider installing it: pip install kaleido")

    # Example configuration
    config = SentimentReportConfig(
        asset_category_unique_identifier="magnificent_7",
        report_days=7,
        report_title="Magnificent 7 News Sentiment & Headlines Report (Last 7 Days)",
        report_id="Mag7_SentimentReport_7d",
    )

    # Create the App instance with config
    app = SentimentReport(config)
    # Run the report
    generated_artifact = app.run()
    if generated_artifact:
        logger.info(f"\nReport generation complete. Artifact ID: {generated_artifact.id}")
    else:
        logger.info("\nReport generation completed, but artifact upload failed or was skipped.")
