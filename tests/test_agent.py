# comment out for local testing out of Main Sequence Platform
import dotenv

dotenv.load_dotenv("../.env.dev")

from mainsequence.virtualfundbuilder.agent_interface import TDAGAgent

tdag_agent = TDAGAgent()

from mainsequence.virtualfundbuilder.contrib.data_nodes import MarketCap

portfolio = tdag_agent.generate_portfolio(
    MarketCap, signal_description="Create me a market cap portfolio using the mag 7 assets"
)
res = portfolio.run()
# res.head()
