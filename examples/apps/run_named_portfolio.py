from mainsequence.virtualfundbuilder.models import PortfolioConfiguration
from mainsequence.virtualfundbuilder.utils import get_vfb_logger

from pydantic import BaseModel
from mainsequence.client.models_tdag import Artifact
from mainsequence.virtualfundbuilder.resource_factory.app_factory import BaseApp, register_app

logger = get_vfb_logger()

class PortfolioRunParameters(BaseModel):
    add_portfolio_to_markets_backend: bool = True
    update_tree: bool = True

class NamedPortfolioConfiguration(BaseModel):
    portfolio_name: str = "market_cap_example"
    portfolio_run_parameters: PortfolioRunParameters

@register_app()
class RunNamedPortfolio(BaseApp):
    configuration_class = NamedPortfolioConfiguration

    def __init__(self, configuration: NamedPortfolioConfiguration):
        logger.info(f"Run Named Timeseries Configuration {configuration}")
        self.configuration = configuration

    def run(self) -> None:
        from mainsequence.virtualfundbuilder.portfolio_interface import PortfolioInterface
        portfolio = PortfolioInterface.load_from_configuration(self.configuration.portfolio_name)
        res = portfolio.run(**self.configuration.portfolio_run_parameters.model_dump())
        logger.info(f"Portfolio Run successful with results {res.head()}")

if __name__ == "__main__":
    configuration = NamedPortfolioConfiguration(
        portfolio_run_parameters=PortfolioRunParameters()
    )
    RunNamedPortfolio(configuration).run()
