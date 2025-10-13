from mainsequence import MARKETS_CONSTANTS
from mainsequence.tdag.data_nodes import APIDataNode

if __name__ == "__main__":
    api_ts = APIDataNode.build_from_unique_identifier(
        MARKETS_CONSTANTS.data_sources_constants.HISTORICAL_MARKET_CAP
    )
    data = api_ts.get_df_between_dates()
    assert len(data) > 0
