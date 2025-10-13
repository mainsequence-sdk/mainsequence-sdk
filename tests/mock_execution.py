import os
from datetime import datetime
from pathlib import Path

from mainsequence.client import (
    Account,
    AccountHistoricalHoldings,
    Asset,
    MarketOrder,
    OrderManager,
    OrderManagerTargetQuantity,
)

os.environ["VFB_PROJECT_PATH"] = str(Path(__file__).parent.absolute())

from mainsequence import MARKETS_CONSTANTS

account = Account.get(account_name="Default MainSequence Portfolios Account")

btc_asset = Asset.get(symbol="BTC", execution_venue__symbol=MARKETS_CONSTANTS.BINANCE_EV_SYMBOL)

# Create Order Manager
target_order = OrderManagerTargetQuantity(
    asset=btc_asset.id,
    quantity=0.2,
)

order_manager = OrderManager.create(
    target_time=datetime.now(),
    order_received_time=datetime.now(),
    execution_end=datetime.now(),
    related_account=account.id,
    target_rebalance=[target_order.dict()],
)

# TODO!
# create holding
start_date = datetime(2025, 2, 25)
end_date = datetime(2025, 3, 1)
historical_holdings = AccountHistoricalHoldings.filter(
    holdings_date__gte=start_date, holdings_date__lte=end_date, related_account__id=account.id
)

# Create Order
MarketOrder.update_or_create(
    order_remote_id="some-unique-remote-id",  # Required, no default
    client_order_id="some-client-id",  # Required, no default
    order_type="market",  # For MarketOrder
    order_time=datetime.now(),  # Required
    order_side=1,  # Must be either 1 (BUY) or -1 (SELL)
    quantity=100.0,  # Required, no default
    status="NOT_PLACED",  # Must be in OrderStatus.choices
    order_manager=order_manager.id,  # ForeignKey, must reference a valid OrderManager
    asset=btc_asset.id,  # ForeignKey, must reference a valid Asset
    related_account=account.id,  # ForeignKey, must reference a valid Account
    # (Optionally, you can include other nullable or optional fields as desired)
)


# Make Trades
