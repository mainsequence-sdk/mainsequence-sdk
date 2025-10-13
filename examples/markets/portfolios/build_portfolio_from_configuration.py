import os
import tempfile

import yaml

from mainsequence.client import MARKETS_CONSTANTS, Asset, AssetCategory, AssetCurrencyPair
from mainsequence.logconf import logger
from mainsequence.virtualfundbuilder.contrib.data_nodes import AssetMistMatch
from mainsequence.virtualfundbuilder.portfolio_interface import (
    PortfolioConfiguration,
    PortfolioInterface,
)

top_100_cryptos = AssetCategory.get(unique_identifier="top_100_crypto_market_cap")


# swithc to binance categories
spot_assets = Asset.filter(id__in=top_100_cryptos.assets)
# get them through main sequence figi class and exchange
binance_currency_pairs = AssetCurrencyPair.filter(
    base_asset__asset_ticker_group_id__in=[a.asset_ticker_group_id for a in spot_assets],
    execution_venue__symbol=MARKETS_CONSTANTS.BINANCE_EV_SYMBOL,
    quote_asset__ticker="USDT",
    include_base_quote_detail=False,
)


top_100_cryptos_binance = AssetCategory.get_or_create(
    unique_identifier=top_100_cryptos.unique_identifier + "_BINANCE",
    display_name=top_100_cryptos.display_name + "_BINANCE",
    description=top_100_cryptos.description + " That trade in Binance Exchange",
    source=top_100_cryptos.source,
)

top_100_cryptos_binance.patch(assets=[a.id for a in binance_currency_pairs])

all_categories = [top_100_cryptos_binance, top_100_cryptos]


DAILY_CRYPTO_TS_UNIQUE_ID = "binance_1d_bars"
MARKET_CAP_TS_ID = "coingecko_market_cap"


for category in all_categories:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    yaml_file_path = os.path.join(script_dir, "base_market_cap_spot.yaml")

    # 1. Read the template file as a string
    with open(yaml_file_path) as f:
        template_str = f.read()

    # 2. Perform string replacements
    #    - Replace placeholders with the actual values
    replaced_str = template_str.replace("CATEGORY_ID", category.unique_identifier)
    replaced_str = replaced_str.replace("CATEGORY_SOURCE", category.source)

    assets = Asset.filter(id__in=category.assets)

    valuation_asset = Asset.get(
        ticker="USDT",
        execution_venue__symbol=assets[0].execution_venue.symbol,
        security_type_2=MARKETS_CONSTANTS.FIGI_SECURITY_TYPE_2_CRYPTO,
    )
    replaced_str = replaced_str.replace("VALUATION_ASSET", valuation_asset.unique_identifier)
    # 3. Optionally, load it into a YAML structure (if you want to manipulate further)
    updated_config = yaml.safe_load(replaced_str)
    # 4. Save the updated YAML to a temporary file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as tmp_file:
        yaml.dump(updated_config, tmp_file)
        tmp_file_path = tmp_file.name

    # 5. Read the modified YAML into PortfolioConfiguration
    try:
        portfolio_config = PortfolioConfiguration.read_portfolio_configuration_from_yaml(
            tmp_file_path
        )

        # no need to build TS if assets mistmacthc
        # assets=Asset.filter_with_asset_class(id__in=category.assets) #expensive

        if len(assets) == 0:
            logger.warning(f"No assets found in category {category.unique_identifier}")
            continue
        if (
            len(assets)
            < portfolio_config["portfolio_build_configuration"][
                "backtesting_weights_configuration"
            ]["signal_weights_configuration"]["min_number_of_assets"]
        ):
            logger.warning(f"No minimum assets found in category {category.name}")
            continue

        portfolio_interface = PortfolioInterface(portfolio_config)

        # Use portfolio_interface as needed
    finally:
        # 6. Clean up the temporary file
        if os.path.exists(tmp_file_path):
            os.remove(tmp_file_path)
    try:

        portfolio_interface.run(
            portfolio_tags=["Thematic", "Crypto"],
            add_portfolio_to_markets_backend=True,
            patch_build_configuration=False,
        )
    except AssetMistMatch:
        portfolio_interface.delete_portfolio()
    except Exception:
        logger.exception(f"error building category {category.unique_identifier}")
