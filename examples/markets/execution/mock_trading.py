"""
This script is used to Mock the Process of integrating and execution engine into the Main Sequence

"""

import datetime
from decimal import Decimal

import numpy as np
import pandas as pd
import pytz

## GEt
from mainsequence.client import (
    Account,
    AssetOnlyPortfolio,
    Order,
    OrderManager,
    OrderManagerTargetQuantity,
    OrderSide,
    OrderStatus,
    OrderTimeInForce,
    OrderType,
    Portfolio,
    RebalanceTargetPosition,
    Trade,
    TradeSide,
)

# %% Configuration
PORTFOLIO_TICKER = "portfo53B"
ACCOUNT_NAME = "Default MainSequence Portfolios Account"


# %%

portfolio_to_mock_trading = Portfolio.get(portfolio_ticker=PORTFOLIO_TICKER)


# %% Get Account to rebalance the portfolio
account = Account.get(account_name=ACCOUNT_NAME, timeout=10000000)
asset_only_portfolio = AssetOnlyPortfolio.get(tracking_asset__id=account.cash_asset.id)
print("Account Latest Holdings")
print(account.latest_holdings)
print("Account Target Positions")
print(account.account_target_portfolio.latest_positions)

make_rebalance = True
for p in account.account_target_portfolio.latest_positions.positions:
    if p.target_portfolio == portfolio_to_mock_trading.id:
        make_rebalance = False
        break

if make_rebalance:
    rebalance_target_position_port = RebalanceTargetPosition(
        target_portfolio_id=portfolio_to_mock_trading.id, weight_notional_exposure=0.2
    )
    rebalance_target_position_cash = RebalanceTargetPosition(
        target_portfolio_id=asset_only_portfolio.id, weight_notional_exposure=0.8
    )
    scheduled_rebalance = account.rebalance(
        target_positions=[rebalance_target_position_port, rebalance_target_position_cash],
        scheduled_time=None,
    )

# A this point we should have a new target portfolio set and a scheduled rebalance that should have happened

# we are going to mock an execution engine that is always forcing to match  the tracking error of our portfolio with
# first lets snapshot the account to guaranteed las valuation
account.snapshot_account(timeout=1000000)

# second lets get the tracking error detail
fund_summary, account_tracking_error = account.get_tracking_error_details(timeout=1000000)
fund_summary = pd.DataFrame(fund_summary["data"])
print(pd.DataFrame(fund_summary))  # give us the state of the fund

rebalance_df = pd.DataFrame(account_tracking_error["data"])

# %%
# Mock Execution
target_rebalance = []
for _, row in rebalance_df.iterrows():
    tmp_quantity = OrderManagerTargetQuantity(
        asset=row["asset_id"],
        quantity=Decimal(row["asset_required_net_rebalance"]),
    )
    target_rebalance.append(tmp_quantity)


# Create Order Manager
order_manager = OrderManager.create(
    target_time=datetime.datetime.now(pytz.utc).replace(tzinfo=pytz.utc),  # target for now
    order_received_time=datetime.datetime.now(pytz.utc).replace(
        tzinfo=pytz.utc
    ),  # Order just received now
    related_account=account.id,
    target_rebalance=target_rebalance,
    timeout=100000,
)

# build the orders
import random

all_orders = []
for target_position in order_manager.target_rebalance:
    asset = target_position.asset

    order_remote_id = str(datetime.datetime.now(pytz.utc).replace(tzinfo=pytz.utc).timestamp())
    if np.random.rand() > 0.9999:  # send as market order
        tmp_order = Order.create_or_update(
            order_type=OrderType.MARKET,
            order_manager=order_manager.id,
            order_remote_id=order_remote_id,
            client_order_id=f"{order_manager.id}_{asset.id}",
            order_time_stamp=datetime.datetime.now(pytz.utc).replace(tzinfo=pytz.utc).timestamp(),
            expires_time=None,
            order_side=OrderSide.BUY if target_position.quantity > 0 else OrderSide.SELL,
            quantity=float(target_position.quantity),
            status=OrderStatus.LIVE,
            asset=asset.id,
            related_account=account.id,
            time_in_force=OrderTimeInForce.GOOD_TILL_CANCELED,
            comments="Mock Order",
        )
    else:
        tmp_order = Order.create_or_update(
            order_type=OrderType.LIMIT,
            order_manager=order_manager.id,
            order_remote_id=order_remote_id,
            client_order_id=f"{order_manager.id}_{asset.id}",
            order_time_stamp=datetime.datetime.now(pytz.utc).replace(tzinfo=pytz.utc).timestamp(),
            expires_time=None,
            order_side=OrderSide.BUY if target_position.quantity > 0 else OrderSide.SELL,
            quantity=float(target_position.quantity),
            limit_price=np.floor(float(row["price"]) * 100) / 100,
            status=OrderStatus.LIVE,
            asset=asset.id,
            related_account=account.id,
            time_in_force=OrderTimeInForce.GOOD_TILL_CANCELED,
            comments="Mock Order",
        )
    # mock a trade
    trade_q = float(target_position.quantity) * random.uniform(0.95, 1.05)
    price = rebalance_df[rebalance_df.asset_id == asset.id]["price"].iloc[0]
    new_trade = Trade.create(
        trade_time=datetime.datetime.now(pytz.utc).replace(tzinfo=pytz.utc),
        trade_side=TradeSide.BUY if target_position.quantity > 0 else TradeSide.SELL,
        asset=asset.id,
        quantity=trade_q,
        price=price,
        commission=0,
        commission_asset=account.cash_asset.id,
        settlement_asset=account.cash_asset.id,
        settlement_cost=float(target_position.quantity) * float(price),
        related_account=account.id,
        related_order=tmp_order.id,
        comments="Mock Trade",
        timeout=10000000,
    )
    # update order
    tmp_order.patch(
        status=OrderStatus.PARTIALLY_FILLED, filled_price=price, filled_quantity=trade_q
    )

    all_orders.append(tmp_order)
