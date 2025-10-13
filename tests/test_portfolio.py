# comment out for local testing out of Main Sequence Platform
import dotenv

dotenv.load_dotenv("../.env.dev")

from mainsequence.virtualfundbuilder.portfolio_interface import PortfolioInterface

portfolio = PortfolioInterface.load_from_configuration(
    configuration_name=None,
    config_file="/home/jose/code/MainSequenceClientSide/mainsequence-sdk/examples/configurations/market_cap_vol_control.yaml",
)

# SessionDataSource.set_local_db()


res = portfolio.run(add_portfolio_to_markets_backend=True)
print(res)
# bars_ts = get_interpolated_prices_timeseries(portfolio.portfolio_build_configuration.assets_configuration)

# bars_ts.run(debug_mode=True, force_update=True)
# res.head()
