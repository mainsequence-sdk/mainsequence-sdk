"""

This script is designed to be able to load and snap accounts when no trading is recorded
and we just want to update the holdings of an account


"""

import pandas as pd

# Create sample data for 3 dates, 2 instruments per account each date
dates = pd.date_range(end=pd.Timestamp.utcnow(), periods=3, freq="T")  # current time in UTC
rows = []
isin_map = {
    "Account A": ["US0378331005", "US4642874576"],
    "Account B": ["US0378331005", "US4642874576"],
}

import random

for date in dates:
    for account, isins in isin_map.items():
        for isin in isins:
            rows.append(
                {
                    "time_index": date,
                    "account_name": account,
                    "isin": isin,
                    "quantity": random.randint(50, 300),
                    "price": round(random.uniform(5, 200), 2),
                }
            )

df = pd.DataFrame(rows).set_index("time_index")

import mainsequence.client as msc
from mainsequence.client import (
    MARKETS_CONSTANTS,
    Account,
    AccountHistoricalHoldings,
    AccountPositionDetail,
    Asset,
    ExecutionVenue,
)

execution_venue = ExecutionVenue.get(symbol=MARKETS_CONSTANTS.MAIN_SEQUENCE_EV)

translations_table = msc.AssetTranslationTable.get(unique_identifier="prices_translation_table_1d")


for (account_name, positions_date), account_df in df.groupby(["account_name", "time_index"]):

    # get or create the account
    target_account = Account.get_or_create(
        account_name=account_name,
        execution_venue=execution_venue.id,
        account_valuation_table_id=translations_table.id,
        create_without_holdings=True,
    )
    # get assets for positions
    asset_map = {}
    for isin in account_df["isin"].to_list():
        asset = Asset.get_or_register_from_isin(
            isin=isin, exchange_code="MM"
        )  # Bolsa Mexican de Valores)
        asset_map[asset.isin] = asset.id

    # map asset id to isin
    account_df["asset"] = account_df["isin"].map(asset_map)

    assert account_df["asset"].isnull().sum() == 0, "Some ISIN does not exist"

    # build  account positions
    positions = [
        AccountPositionDetail(**p)
        for p in account_df[["quantity", "price", "asset"]].to_dict("records")
    ]

    holdings = AccountHistoricalHoldings.create_with_holdings(
        position_list=positions,
        holdings_date=positions_date.timestamp(),
        related_account=target_account.id,
    )

    # set account target portfolio to match exactly
    target_account.set_account_target_portfolio_from_asset_holdings()
    # snapshot account after new holdings to build tracking error
    target_account.snapshot_account()
