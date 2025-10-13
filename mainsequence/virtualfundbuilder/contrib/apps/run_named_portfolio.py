from enum import Enum

from pydantic import BaseModel

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


PortfolioNameEnum = Enum(
    "PortfolioNameEnum",
    {name: name for name in PortfolioInterface.list_configurations()},
    type=str,  # make each member a `str`, so validation works as before
)


class NamedPortfolioConfiguration(BaseModel):
    portfolio_name: PortfolioNameEnum
    portfolio_run_parameters: PortfolioRunParameters


@regiester_agent_tool()
class RunNamedPortfolio(BaseAgentTool):
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
        portfolio_run_parameters=PortfolioRunParameters(), portfolio_name="market_cap"
    )
    RunNamedPortfolio(configuration).run()
