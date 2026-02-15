from pydantic import BaseModel

from mainsequence.virtualfundbuilder.models import PortfolioConfiguration
from mainsequence.virtualfundbuilder.portfolio_interface import PortfolioInterface
from mainsequence.virtualfundbuilder.resource_factory.app_factory import (
    BaseAgentTool,
    regiester_agent_tool,
)
from mainsequence.virtualfundbuilder.utils import get_vfb_logger

logger = get_vfb_logger()


class PortfolioRunParameters(BaseModel):
    add_portfolio_to_markets_backend: bool = True
    update_tree: bool = True


class RunPortfolioConfiguration(BaseModel):
    portfolio_configuration: PortfolioConfiguration
    portfolio_run_parameters: PortfolioRunParameters


@regiester_agent_tool()
class RunPortfolio(BaseAgentTool):
    configuration_class = RunPortfolioConfiguration

    def __init__(self, configuration: RunPortfolioConfiguration):
        self.configuration = configuration

    def run(self) -> None:
        portfolio = PortfolioInterface(
            portfolio_config_template=self.configuration.portfolio_configuration.model_dump()
        )
        res = portfolio.run(**self.configuration.portfolio_run_parameters.model_dump())
        logger.info(f"Portfolio Run successful with results {res.head()}")
        self.add_output(output=portfolio.target_portfolio)


if __name__ == "__main__":
    portfolio_configuration = PortfolioInterface.load_from_configuration(
        "market_cap"
    ).portfolio_config
    run_portfolio_configuration = RunPortfolioConfiguration(
        portfolio_configuration=portfolio_configuration,
        portfolio_run_parameters=PortfolioRunParameters(),
    )
    RunPortfolio(run_portfolio_configuration).run()
