
import os
from pathlib import Path

from mainsequence.client.models_tdag import DynamicResource, Artifact
from mainsequence.virtualfundbuilder.__main__ import VirtualFundLauncher

os.environ["VFB_PROJECT_PATH"] = str(Path(__file__).parent.absolute())

#comment out for local testing out of Main Sequence Platform
import dotenv
dotenv.load_dotenv('../.env.dev')

from mainsequence.virtualfundbuilder.portfolio_interface import PortfolioInterface
from mainsequence.virtualfundbuilder.agent_interface import TDAGAgent

tdag_agent = TDAGAgent()

from mainsequence.virtualfundbuilder.contrib.time_series import MarketCap
portfolio = tdag_agent.generate_portfolio(MarketCap, signal_description="Create me a market cap portfolio using the mag 7 assets")
res = portfolio.run()
# res.head()

