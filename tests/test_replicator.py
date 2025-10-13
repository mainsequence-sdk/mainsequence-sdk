from mainsequence.virtualfundbuilder.contrib.data_nodes.portfolio_replicator import (
    ETFReplicator,
    TrackingStrategyConfiguration,
)
from mainsequence.virtualfundbuilder.models import AssetsConfiguration, PricesConfiguration

asset_configuration = AssetsConfiguration(
    prices_configuration=PricesConfiguration(),
    assets_category_unique_id="magnificent_7",
)

ts = ETFReplicator(
    etf_ticker="XLF",
    signal_assets_configuration=asset_configuration,
    tracking_strategy_configuration=TrackingStrategyConfiguration(),
)

ts.run(debug_mode=True, force_update=True, update_tree=True)
