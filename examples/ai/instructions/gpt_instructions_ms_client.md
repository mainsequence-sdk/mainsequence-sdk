### LLM Assistant Guide for `mainsequence.client`

This guide provides instructions and code examples for interacting with the Main Sequence platform using the `ms_client` Python library.

---

### **1. Asset Management**

This section covers how to find, register, and categorize assets.

#### **Querying Assets**

To find existing assets, use the `ms_client.Asset.filter()` method. You can filter by any attribute of the `Asset` model, such as `ticker`, or `security_type`.

* **Method:** `ms_client.Asset.filter(**kwargs)`
* **Example:** Find all assets with tickers 'BTCUSDT' or 'ETHUSDT' on the Binance execution venue.
    ```python
    import mainsequence.client as ms_client

    crypto_assets = ms_client.Asset.filter(
        ticker__in=["BTCUSDT", "ETHUSDT"],
    )

    for asset in crypto_assets:
        print(f"Found: {asset.ticker} (ID: {asset.id})")
    ```

#### **Registering New Assets**

If an asset does not exist in the system, you can add it using its FIGI identifier.

* **Method:** `ms_client.Asset.register_asset_from_figi(figi)`
* **Example:** Register an asset using its FIGI.
    ```python
    import mainsequence.client as ms_client

    # This FIGI corresponds to NVIDIA on the Toronto Stock Exchange.
    nvda_figi = "BBG014T46NC0"

    try:
        registered_asset = ms_client.Asset.register_asset_from_figi(figi=nvda_figi)
        print(f"Successfully registered asset: {registered_asset.name}")
        registered_asset.pretty_print()
    except Exception as e:
        print(f"Could not register asset (it might already exist): {e}")
    ```

#### **Managing Asset Categories**

Group assets into categories for easier management and analysis.

* **Method:** `ms_client.AssetCategory.get_or_create(display_name, source, assets)`
* **Parameters:**
    * `display_name`: A human-readable name for the category.
    * `source`: The origin of the category (e.g., 'user\_defined').
    * `assets`: A list of integer asset IDs to include in the category.
* **Example:** Create a category named "My Favorite Cryptos".
    ```python
    import mainsequence.client as ms_client

    # Prerequisite: Get the IDs of the assets you want to categorize.
    btc_asset = ms_client.Asset.get(ticker="BTCUSDT", exchange_code=ms_client.MARKETS_CONSTANTS.BINANCE_EV_SYMBOL)
    eth_asset = ms_client.Asset.get(ticker="ETHUSDT", exchange_code=ms_client.MARKETS_CONSTANTS.BINANCE_EV_SYMBOL)
    asset_ids = [btc_asset.id, eth_asset.id]

    fav_crypto_category = ms_client.AssetCategory.get_or_create(
        display_name="My Favorite Cryptos",
        source="user_defined",
        assets=asset_ids
    )
    print(f"Category '{fav_crypto_category.display_name}' now contains {len(fav_crypto_category.assets)} assets.")
    ```

---

### **2. Account Management**

This section explains how to manage user accounts, including creating them, managing their holdings, and analyzing their performance.

#### **Querying Accounts**

Use `ms_client.Account.filter()` to find accounts based on specific criteria.

* **Method:** `ms_client.Account.filter(**kwargs)`
* **Example:** Find all active paper trading accounts.
    ```python
    import mainsequence.client as ms_client

    active_paper_accounts = ms_client.Account.filter(
        account_is_active=True,
        is_paper=True
    )
    print(f"Found {len(active_paper_accounts)} active paper accounts.")
    ```

#### **Creating and Retrieving Accounts**

Use `ms_client.Account.get_or_create()` to create a new account or retrieve an existing one safely.

* **Method:** `ms_client.Account.get_or_create(account_name, execution_venue, cash_asset, **kwargs)`
* **Example:** Create a paper account on the Main Sequence execution venue with USD as the cash asset.
    ```python
    import mainsequence.client as ms_client

    # Prerequisites: Get the execution venue and cash asset objects.
    venue = ms_client.ExecutionVenue.get(symbol=ms_client.MARKETS_CONSTANTS.MAIN_SEQUENCE_EV)
    cash = ms_client.Asset.get(ticker="USD", execution_venue=venue.id)

    my_account = ms_client.Account.get_or_create(
        account_name="My LLM Paper Account",
        execution_venue=venue.id,
        cash_asset=cash.id,
        is_paper=True,
        account_is_active=True
    )
    print(f"Successfully retrieved/created account: '{my_account.account_name}'")
    ```

#### **Creating Holdings Snapshots**

To load an account's state at a specific point in time, use the `ms_client.AccountHistoricalHoldings.create_with_holdings` method. This is essential for initializing an account's positions.

* **Method:** `ms_client.AccountHistoricalHoldings.create_with_holdings(position_list, holdings_date, related_account)`
* **Parameters:**
    * `position_list`: A list of `ms_client.AccountPositionDetail` objects. Each object must contain an `asset` (as an integer ID), `quantity`, and `price`.
    * `holdings_date`: A UNIX timestamp representing the date of the snapshot.
    * `related_account`: The integer ID of the account to which the holdings belong.
* **Example:**
    ```python
    import mainsequence.client as ms_client
    import datetime

    # Prerequisite: You must have the integer IDs for your assets and the target account.
    asset_id_1 = 12345  # Placeholder ID
    asset_id_2 = 67890  # Placeholder ID
    target_account_id = 101 # Placeholder ID

    positions = [
        ms_client.AccountPositionDetail(asset=asset_id_1, quantity=100, price=175.50),
        ms_client.AccountPositionDetail(asset=asset_id_2, quantity=50, price=250.00)
    ]

    snapshot_timestamp = datetime.datetime.now(datetime.timezone.utc).timestamp()

    new_holdings = ms_client.AccountHistoricalHoldings.create_with_holdings(
        position_list=positions,
        holdings_date=snapshot_timestamp,
        related_account=target_account_id
    )
    print(f"Created holdings snapshot with ID: {new_holdings.id}")
    ```

#### **Syncing and Snapshotting an Account**

After loading holdings, you can align the account's target composition with its current state and create a snapshot for analysis.

* **Methods:**
    * `my_account.set_account_target_portfolio_from_asset_holdings()`: Sets the account's target portfolio to match the most recent holdings.
    * `my_account.snapshot_account()`: Creates a snapshot for risk and tracking error analysis.
* **Example:**
    ```python
    # Prerequisite: `my_account` is an existing ms_client.Account object.
    my_account.set_account_target_portfolio_from_asset_holdings()
    my_account.snapshot_account()
    print("Account target synced and snapshot created.")
    ```

#### **Retrieving Historical Holdings as a DataFrame**

For analysis, you can fetch an account's holdings over a specified date range as a pandas DataFrame. If no holdings are found for the range, an empty DataFrame is returned.

* **Method:** `my_account.get_historical_holdings(start_date=None, end_date=None)`
* **Parameters:**
    * `start_date` (`datetime`, optional): The start datetime (UTC) for filtering holdings. If omitted, the query starts from the earliest available data.
    * `end_date` (`datetime`, optional): The end datetime (UTC) for filtering holdings. If omitted, the query includes data up to the most recent record.
* **Returns:** A pandas DataFrame indexed by a multi-index of `time_index` (UTC datetime) and `asset_id` (int). It contains the following columns:
    * `price` (float): The price of the asset at the time of the holding.
    * `quantity` (float): The quantity of the asset held.
    * `missing_price` (bool): `True` if the price for the asset was missing on that date.
* **Example:** Get holdings since June 1, 2025, and add asset names to the DataFrame.
    ```python
    import mainsequence.client as ms_client
    import datetime

    # Prerequisite: `my_account` is an existing ms_client.Account object.
    # It's best practice to use timezone-aware datetimes for queries.
    start_of_june = datetime.datetime(2025, 6, 1, tzinfo=datetime.timezone.utc)

    holdings_df = my_account.get_historical_holdings(start_date=start_of_june)

    if not holdings_df.empty:
        # The DataFrame index contains asset IDs. We can fetch asset details in bulk.
        asset_ids = holdings_df.index.get_level_values("asset_id").unique().tolist()
        assets_in_holdings = ms_client.Asset.filter(id__in=asset_ids)
        
        # Create a mapping from asset ID to name for easy lookup.
        id_to_name_map = {asset.id: asset.name for asset in assets_in_holdings}
        holdings_df["asset_name"] = holdings_df.index.get_level_values("asset_id").map(id_to_name_map)
        
        print("Enriched Historical Holdings DataFrame:")
        print(holdings_df.head())
    else:
        print("No holdings found for the specified period.")
    ```

---

### **3. Portfolio Management**

This section covers how to query portfolios and inspect their composition.

#### **Querying Portfolios**

Use `ms_client.Portfolio.filter()` to find portfolios.

* **Method:** `ms_client.Portfolio.filter(**kwargs)`
* **Example:** Find a portfolio by its `portfolio_ticker`.
    ```python
    import mainsequence.client as ms_client

    # Use a ticker for a portfolio you know exists.
    portfolio_ticker = "portfo446B" 
    portfolios = ms_client.Portfolio.filter(portfolio_ticker=portfolio_ticker)

    if portfolios:
        my_portfolio = portfolios[0]
        print(f"Found portfolio: '{my_portfolio.portfolio_name}'")
    ```

#### **Getting Latest Portfolio Weights**

Retrieve the most recent weights of a portfolio's assets.

* **Method:** `my_portfolio.get_latest_weights()`
* **Returns:** A dictionary of `{asset_unique_identifier: weight}`.
* **Example:**
    ```python
    # Prerequisite: `my_portfolio` is an existing ms_client.Portfolio object.
    latest_weights = my_portfolio.get_latest_weights()

    if latest_weights:
        print("Top 5 portfolio weights:")
        for i, (asset_uid, weight) in enumerate(latest_weights.items()):
            if i >= 5: break
            print(f"  - {asset_uid}: {weight:.4f}")
    ```