import pandas as pd
from pydantic import BaseModel

from mainsequence.client import CONSTANTS, Asset, AssetCategory
from mainsequence.client.models_tdag import Artifact
from mainsequence.virtualfundbuilder.contrib.prices.data_nodes import ExternalPrices
from mainsequence.virtualfundbuilder.portfolio_interface import PortfolioInterface
from mainsequence.virtualfundbuilder.resource_factory.app_factory import (
    BaseAgentTool,
    regiester_agent_tool,
)
from mainsequence.virtualfundbuilder.utils import get_vfb_logger

logger = get_vfb_logger()


class LoadExternalPortfolioConfig(BaseModel):
    bucket_name: str
    artifact_name: str
    portfolio_name: str
    created_asset_category_name: str


@regiester_agent_tool()
class LoadExternalPortfolio(BaseAgentTool):
    configuration_class = LoadExternalPortfolioConfig

    def run(self):

        # get the data and store it on local storage
        source_artifact = Artifact.get(
            bucket__name=self.configuration.bucket_name, name=self.configuration.artifact_name
        )
        weights_source = pd.read_csv(source_artifact.content)

        # validate data
        expected_cols = ["time_index", "figi", "weight", "price"]
        if set(weights_source.columns) != set(expected_cols):
            raise ValueError(
                f"Invalid CSV format: expected columns {expected_cols!r} "
                f"but got {list(weights_source.columns)!r}"
            )

        weights_source["time_index"] = pd.to_datetime(weights_source["time_index"], utc=True)

        # create assets from figi in the backend
        assets = []
        for figi in weights_source["figi"].unique():
            asset = Asset.get_or_none(figi=figi)
            if asset is None:
                try:
                    asset = Asset.register_figi_as_asset_in_main_sequence_venue(
                        figi=figi,
                        execution_venue__symbol=CONSTANTS.MAIN_SEQUENCE_EV,
                    )
                except Exception as e:
                    print(f"Could not register asset with figi {figi}, error {e}")
                    continue
            assets.append(asset)

        # create asset category
        portfolio_category = AssetCategory.get_or_create(
            display_name=self.configuration.created_asset_category_name,
            source="external",
            description=f"This category contains the assets for the external portfolio {self.configuration.portfolio_name}",
            unique_identifier=self.configuration.created_asset_category_name.replace(
                " ", "_"
            ).lower(),
        )
        portfolio_category.append_assets([a.id for a in assets])

        # insert prices
        external_prices_source = ExternalPrices(
            bucket_name=self.configuration.bucket_name,
            artifact_name=self.configuration.artifact_name,
            asset_category_unique_id=portfolio_category.unique_identifier,
        ).run(debug_mode=True, force_update=True)

        # adapt portfolio configuration
        portfolio = PortfolioInterface.load_from_configuration("external_portfolio_template")
        current_template_dict = portfolio.portfolio_config_template

        sw_config = current_template_dict["portfolio_build_configuration"][
            "backtesting_weights_configuration"
        ]["signal_weights_configuration"]
        sw_config["bucket_name"] = self.configuration.bucket_name
        sw_config["artifact_name"] = self.configuration.artifact_name
        sw_config["assets_category_unique_id"] = portfolio_category.unique_identifier
        sw_config["signal_assets_configuration"][
            "assets_category_unique_id"
        ] = portfolio_category.unique_identifier
        current_template_dict["portfolio_markets_configuration"][
            "portfolio_name"
        ] = self.configuration.portfolio_name

        portfolio = PortfolioInterface(current_template_dict)

        # Switch out the prices source to get our external prices
        portfolio._initialize_nodes()
        portfolio.portfolio_strategy_data_node.bars_ts = external_prices_source

        # Run the portfolio
        res = portfolio.run(add_portfolio_to_markets_backend=True)
        logger.info(f"Portfolio integrated successfully with results {res.head()}")


if __name__ == "__main__":
    app_config = LoadExternalPortfolioConfig(
        bucket_name="Sample Data",
        artifact_name="portfolio_weights_mag7.csv",
        portfolio_name="Mag 7 External Portfolio",
        created_asset_category_name="external_magnificent_7",
    )

    LoadExternalPortfolio(app_config).run()
