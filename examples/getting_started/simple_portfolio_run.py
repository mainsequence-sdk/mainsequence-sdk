import mainsequence.client as ms_client
import datetime
portfolio_ticker = "portfo446B" # An example ticker that should exist
portfolios = ms_client.Portfolio.filter()

# portfolio=[p for p in portfolios if p.data_node_update is not None][0]


#
# historical_weights=portfolio.get_historical_weights(start_date_timestamp=datetime.datetime(2020,1,1).timestamp(),
#                                  end_date_timestamp=datetime.datetime(2025,1,2).timestamp(),
#                                  )
#
# a=5
