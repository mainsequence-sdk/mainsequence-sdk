import datetime

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pydantic import BaseModel

from mainsequence.client import Asset
from mainsequence.virtualfundbuilder.contrib.data_nodes import (
    TrackingStrategy,
    TrackingStrategyConfiguration,
)
from mainsequence.virtualfundbuilder.portfolio_interface import PortfolioInterface
from mainsequence.virtualfundbuilder.resource_factory.app_factory import (
    HtmlApp,
    regiester_agent_tool,
)
from mainsequence.virtualfundbuilder.utils import get_vfb_logger

logger = get_vfb_logger()


class ETFReplicatorConfiguration(BaseModel):
    source_asset_category_identifier: str = "magnificent_7"
    etf_to_replicate: str = "XLF"
    in_window: int = 60
    tracking_strategy: TrackingStrategy = TrackingStrategy.LASSO
    tracking_strategy_configuration: TrackingStrategyConfiguration


@regiester_agent_tool()
class ETFReplicatorApp(HtmlApp):
    configuration_class = ETFReplicatorConfiguration

    def _build_portfolio_config(self) -> dict:
        """
        Loads a portfolio configuration template and customizes it for ETF replication.
        """
        portfolio_config = PortfolioInterface.load_configuration(configuration_name="market_cap")
        signal_weights_configuration = {
            "etf_ticker": self.configuration.etf_to_replicate,
            "in_window": self.configuration.in_window,
            "tracking_strategy": self.configuration.tracking_strategy,
            "tracking_strategy_configuration": self.configuration.tracking_strategy_configuration,
            "signal_assets_configuration": portfolio_config.portfolio_build_configuration.backtesting_weights_configuration.signal_weights_configuration[
                "signal_assets_configuration"
            ],
        }
        signal_weights_configuration["signal_assets_configuration"].assets_category_unique_id = (
            self.configuration.source_asset_category_identifier
        )

        portfolio_config.portfolio_build_configuration.backtesting_weights_configuration.signal_weights_configuration = (
            signal_weights_configuration
        )
        portfolio_config.portfolio_build_configuration.backtesting_weights_configuration.signal_weights_name = (
            "ETFReplicator"
        )
        portfolio_config.portfolio_markets_configuration.portfolio_name = f"ETFReplicator Portfolio for {self.configuration.etf_to_replicate} using {self.configuration.source_asset_category_identifier}"

        return portfolio_config.model_dump()

    def _create_plot(self, df_plot_normalized: pd.DataFrame, weights_pivot: pd.DataFrame) -> str:
        """
        Creates a combined Plotly figure with performance and asset weight subplots.
        """
        fig = make_subplots(
            rows=2,
            cols=1,
            shared_xaxes=True,
            vertical_spacing=0.1,
            subplot_titles=("ETF Replication Performance", "Calculated Asset Weights"),
        )

        # Add performance traces to the first subplot
        fig.add_trace(
            go.Scatter(
                x=df_plot_normalized.index,
                y=df_plot_normalized[self.configuration.etf_to_replicate],
                mode="lines",
                name=f"Original: {self.configuration.etf_to_replicate}",
            ),
            row=1,
            col=1,
        )

        fig.add_trace(
            go.Scatter(
                x=df_plot_normalized.index,
                y=df_plot_normalized[f"Replicated_{self.configuration.etf_to_replicate}"],
                mode="lines",
                name="Replicated Portfolio",
            ),
            row=1,
            col=1,
        )

        # Add weights traces to the second subplot as a stacked area chart
        for asset in weights_pivot.columns:
            fig.add_trace(
                go.Scatter(
                    x=weights_pivot.index,
                    y=weights_pivot[asset],
                    mode="lines",
                    name=asset,
                    showlegend=True,
                ),
                row=2,
                col=1,
            )

        fig.update_layout(
            title_text=f"ETF Replication Analysis: {self.configuration.etf_to_replicate} vs. Replicated Portfolio",
            legend_title="Series",
            height=800,  # Increase height to accommodate both plots
        )

        fig.update_yaxes(title_text="Normalized Performance (Indexed to 100)", row=1, col=1)
        fig.update_yaxes(title_text="Asset Weight", row=2, col=1)

        return fig.to_html(full_html=False, include_plotlyjs="cdn")

    def run(self) -> str:
        # Build Portfolio Configuration
        portfolio_config_dump = self._build_portfolio_config()
        portfolio = PortfolioInterface(portfolio_config_template=portfolio_config_dump)

        # Run Portfolio
        portfolio.run(add_portfolio_to_markets_backend=True)
        self.add_output(output=portfolio.target_portfolio)

        # Fetch Portfolio Results in batches to avoid timeouts
        loop_start_date = portfolio.portfolio_strategy_data_node.OFFSET_START
        final_end_date = datetime.datetime.now(datetime.timezone.utc)
        all_results = []
        current_start = loop_start_date
        while current_start < final_end_date:
            current_end = current_start + pd.DateOffset(months=6)
            logger.info(
                f"Fetching data from {current_start.date()} to {min(current_end, final_end_date).date()}"
            )
            results_chunk = portfolio.portfolio_strategy_data_node.get_df_between_dates(
                start_date=current_start, end_date=current_end
            )
            all_results.append(results_chunk)
            current_start = current_end

        results = pd.concat(all_results).drop_duplicates()
        if results.empty:
            return "<html><body><h1>No data available to generate ETF replication report.</h1></body></html>"

        # Fetch and Process Data for Plotting
        etf_replicator_signal = portfolio.portfolio_strategy_data_node.signal_weights
        etf_asset = etf_replicator_signal.etf_asset
        etf_data = etf_replicator_signal.etf_bars_ts.get_df_between_dates(
            start_date=results.index.min(),
            end_date=results.index.max(),
            unique_identifier_list=[etf_asset.unique_identifier],
        )

        replicated_df = (
            results.sort_index()
            .reset_index()[["time_index", "close"]]
            .rename(columns={"close": f"Replicated_{self.configuration.etf_to_replicate}"})
        )
        original_df = (
            etf_data.sort_index()
            .reset_index()[["time_index", "close"]]
            .rename(columns={"close": self.configuration.etf_to_replicate})
        )
        df_plot = pd.concat([original_df, replicated_df]).sort_values("time_index").ffill()
        df_plot = df_plot.set_index("time_index").dropna()
        if df_plot.empty:
            return "<html><body><h1>Could not align original and replicated portfolio data.</h1></body></html>"

        df_plot_normalized = (df_plot / df_plot.iloc[0]) * 100

        weights_df = etf_replicator_signal.get_df_between_dates(
            start_date=results.index.min(), end_date=results.index.max()
        )
        weights_df = weights_df.reset_index()
        weight_assets = Asset.filter(
            unique_identifier__in=list(weights_df["unique_identifier"].unique())
        )
        translation_map = {asset.unique_identifier: asset.ticker for asset in weight_assets}
        weights_df["ticker"] = weights_df["unique_identifier"].map(translation_map)

        weights_pivot = weights_df.pivot(
            index="time_index", columns="ticker", values="signal_weight"
        ).fillna(0)

        # reduce size of plot
        weights_pivot = weights_pivot.resample("W").last().fillna(0)  # only use weekly weights
        weights_pivot = weights_pivot.loc[
            :, (weights_pivot > 0.01).any(axis=0)
        ]  # filter out assets with very small weights

        return self._create_plot(df_plot_normalized, weights_pivot)


if __name__ == "__main__":
    cfg = ETFReplicatorConfiguration(
        tracking_strategy_configuration=TrackingStrategyConfiguration(),
        source_asset_category_identifier="s&p500_constitutents",
    )
    ETFReplicatorApp(cfg).run()
