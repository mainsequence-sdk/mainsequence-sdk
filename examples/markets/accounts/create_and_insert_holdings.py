from __future__ import annotations

import datetime as dt
import os
from decimal import Decimal

from mainsequence.client.markets.models.accounts_and_portfolios import (
    AccountHoldingsWritePosition,
)
from mainsequence.markets.accounts import AccountHoldings

ACCOUNT_NAME = os.getenv("MAINSEQUENCE_EXAMPLE_ACCOUNT_NAME", "SDK Example Account")
EXECUTION_VENUE_UID = os.getenv("MAINSEQUENCE_EXAMPLE_EXECUTION_VENUE_UID", "paper")
HASH_NAMESPACE = os.getenv("MAINSEQUENCE_EXAMPLE_HASH_NAMESPACE", "account_holdings_example")
UNIQUE_IDENTIFIER = os.getenv(
    "MAINSEQUENCE_EXAMPLE_UNIQUE_IDENTIFIER",
    "example-account-holdings-asset",
)
TIMEOUT = int(os.getenv("MAINSEQUENCE_EXAMPLE_TIMEOUT", "120"))


def main() -> None:
    holdings_node = AccountHoldings(hash_namespace=HASH_NAMESPACE)

    # This creates the DataNodeStorage/SourceTableConfiguration through the
    # normal governed DataNode path. Account writes below still go through DRF.
    holdings_node.run(debug_mode=True, force_update=True)

    account = holdings_node.get_or_create_account(
        account_name=ACCOUNT_NAME,
        execution_venue=EXECUTION_VENUE_UID,
        labels=["sdk-example"],
        is_paper=True,
        timeout=TIMEOUT,
    )

    holdings_date = dt.datetime.now(dt.UTC).replace(microsecond=0)
    position = AccountHoldingsWritePosition(
        unique_identifier=UNIQUE_IDENTIFIER,
        quantity=Decimal("10.0"),
        target_trade_time=holdings_date,
        extra_details={"source": "sdk-example"},
    )

    response = holdings_node.add_account_holdings(
        account=account,
        holdings_date=holdings_date,
        positions=[position],
        overwrite=True,
        timeout=TIMEOUT,
    )

    latest = holdings_node.get_latest_account_holdings(
        account=account,
        timeout=TIMEOUT,
    )

    print("Account UID:", account.uid)
    print("Holdings data node id:", holdings_node.holdings_data_source_id())
    print("Inserted holdings set:", response.holdings_set_uid)
    print("Latest holdings date:", None if latest is None else latest.holdings_date)


if __name__ == "__main__":
    main()
