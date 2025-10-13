if __name__ == "__main__":
    from mainsequence.virtualfundbuilder.agent_interface import TDAGAgent
    from mainsequence.virtualfundbuilder.contrib.data_nodes import MarketCap

    tdag_agent = TDAGAgent()
    portfolio = tdag_agent.generate_portfolio(
        MarketCap, signal_description="Create me a market cap portfolio using AAPL and GOOG"
    )
    res = portfolio.run()
    assert len(res) > 0
