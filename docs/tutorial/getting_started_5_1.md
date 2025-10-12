# Getting Started 5: From Data to Dashboards I

## Introduction

Once your data is unified, you can start doing more interesting things with it. Let’s build your first dashboard.

To help you build and deploy dashboards quickly, the Main Sequence platform integrates **Streamlit**, an open‑source Python library for interactive apps.

Streamlit on the Main Sequence platform lets you turn data into interactive tools. Coupled with your project’s compute engine, you can ship a **production‑ready** dashboard in minutes.

In this example, we’ll build a simple app to **stress‑test a fixed‑income portfolio** against movements in the benchmark yield curve.

We’ll use the **`mainsequence.instrument`** library, which is a template wrapper you can use to integrate any instrument‑pricing engine. For high‑quality pricing, we wrap the excellent **QuantLib** project: https://www.quantlib.org/.

QuantLib is a free, open‑source library for modeling, trading, and risk management. It’s written in C++ and exported to languages like C#, Java, Python, and R.

We’ll use `mainsequence.instrument` to price a portfolio of floating‑ and fixed‑rate bonds and use QuantLib to estimate KPIs such as expected carry and mark‑to‑market impact.

If you want to see the end result, check the example repository: https://github.com/mainsequence-sdk/ExampleDashboards. We still recommend that you continue building in your isolated tutorial project.

## Building a Dashboard

For the platform to detect your dashboards, place them inside the `dashboards/` folder in your repo, and name the file that initializes the app **`app.py`**. The platform will discover and deploy it automatically.

## Interest‑Rate Portfolio Exposure Example

We’ll build a dashboard that shows how changes in the yield curve affect a portfolio. It will include:

1) A search input to look up the portfolio.
2) Controls to choose how the yield curve should shift.
3) A chart comparing the original vs. shifted curve.
4) A table showing overall and per‑instrument impact.

## Pre‑work Making A Portfolio

We don’t yet have portfolios in the platform, so let’s create one. Any portfolio in Main Sequence has the following properties (see the examples under **Markets** for more context: https://github.com/mainsequence-sdk/mainsequence-sdk/tree/main/examples/markets).

```python
class PortfolioMixin:
    id: Optional[int] = None
    is_active: bool = False
    data_node_update: Optional['DataNodeUpdate']
    signal_data_node_update: Optional['DataNodeUpdate']
    follow_account_rebalance: bool = False
    comparable_portfolios: Optional[List[int]] = None
    backtest_table_price_column_name: Optional[str] = Field(None, max_length=20)
    tags: Optional[List['PortfolioTags']] = None
    calendar: Optional['Calendar']
    index_asset: PortfolioIndexAsset
```

The most important fields are **`data_node_update`** and **`signal_data_node_update`**. On Main Sequence, a portfolio is typically composed of a **signal** (which generates weights) and a **portfolio process** (which represents the historical performance and configuration). For instance, a market‑cap strategy might compute weights daily, but the portfolio may only rebalance **quarterly**.

For this tutorial dashboard, we’ll build a **mock portfolio** with **no signal**—just a `data_node_update`.

You can find the code under `dashboards/helpers/mock.py`:

```python
class TestFixedIncomePortfolio(PortfolioFromDF):
    def get_portfolio_df(self):

        time_idx = datetime.datetime.now()
        time_idx = datetime.datetime(time_idx.year, time_idx.month, time_idx.day, time_idx.hour,
                                     time_idx.minute, tzinfo=pytz.utc, )

        unique_identifiers = ["TEST_FLOATING_BOND_UST","TEST_FIXED_BOND_USD"]
        existing_assets = msc.Asset.query(unique_identifier__in=unique_identifiers)
        existing_uids={a.unique_identifier:a for a in existing_assets}
        for uid in unique_identifiers:
            build_uid=False
            if uid not in existing_uids.keys():
                build_uid=True
            else:
                if existing_uids[uid].current_pricing_detail is None:
                    build_uid=True

            if build_uid:
                common_kwargs = {
                    "face_value": 100,
                    "coupon_frequency": ql.Period(6, ql.Months),
                    "day_count": ql.Actual365Fixed(),
                    "calendar": ql.UnitedStates(ql.UnitedStates.GovernmentBond),
                    "business_day_convention": ql.Unadjusted,
                    "settlement_days": 0,
                    "maturity_date" : time_idx.date() + datetime.timedelta(days=365 * 10),
                    "issue_date": time_idx.date()
                }

                # --- Conditionally create the bond, adding only the specific arguments ---
                if "FLOATING" in uid:
                    bond = msi.FloatingRateBond(
                        **common_kwargs,  # Unpack the common arguments
                        floating_rate_index_name="UST",
                        issue_date=time_idx.date(),

                    )
                else:  # Implies a FixedRateBond
                    bond = msi.FixedRateBond(
                        **common_kwargs,  # Unpack the common arguments
                        coupon_rate=0.05
                    )
                snapshot = {
                    "name": uid,
                    "ticker": uid
                }
                payload_item = {
                    "unique_identifier": uid,
                    "snapshot": snapshot,
                }
                assets = msc.Asset.batch_get_or_register_custom_assets([payload_item])
                asset=assets[0]
                #registed the instrument pricing details
                asset.add_instrument_pricing_details_from_ms_instrument(
                    instrument=bond,pricing_details_date=time_idx
                    )

        # ----- build dict-valued columns -----
        keys = unique_identifiers
        n = len(keys)

        # random weights that sum to 1
        import numpy as np
        w = np.random.rand(n)
        w = w / w.sum()
        weights_dict = json.dumps({k: float(v) for k, v in zip(keys, w)})

        # everything else set to 1 per asset
        ones_dict = json.dumps({k: 1 for k in keys})



        # Map logical fields to actual DF columns
        col_weights_current = "rebalance_weights"  # "weights_current"
        col_price_current = "rebalance_price"  # "price_current"
        col_vol_current = "volume"  # "volume_current"
        col_weights_before = "weights_at_last_rebalance"  # "weights_before"
        col_price_before = "price_at_last_rebalance"  # "price_before"
        col_vol_before = "volume_at_last_rebalance"  # "volume_before"

        row = {
            "time_index": time_idx,
            "close": 1,
            "return": 0,
            "last_rebalance_date": time_idx.timestamp(),
            col_weights_current: weights_dict,
            col_weights_before: weights_dict,  # same as current
            col_price_current: ones_dict,
            col_price_before: ones_dict,
            col_vol_current: ones_dict,
            col_vol_before: ones_dict,
        }

        # one-row DataFrame
        portoflio_df = pd.DataFrame([row])
        portoflio_df = portoflio_df.set_index("time_index")
        if self.update_statistics.max_time_index_value is not None:
            portoflio_df = portoflio_df[portoflio_df.index > self.update_statistics.max_time_index_value]
        return portoflio_df


def build_test_portfolio(portfolio_name:str):
    node = TestFixedIncomePortfolio(portfolio_name=portfolio_name, calendar_name="24/7",
                         target_portfolio_about="Test")

    PortfolioInterface.build_and_run_portfolio_from_df(portfolio_node=node,
                                                       add_portfolio_to_markets_backend=True)
```

Key pieces to notice:

```python
assets = msc.Asset.batch_get_or_register_custom_assets([payload_item])
```

This ensures the custom assets you want are **registered** on the platform.

```python
asset.add_instrument_pricing_details_from_ms_instrument(
                    instrument=bond,pricing_details_date=time_idx
                    )
```

This attaches **instrument pricing details** to the asset.

```python
node = TestFixedIncomePortfolio(portfolio_name=portfolio_name, calendar_name="24/7",
                         target_portfolio_about="Test")

PortfolioInterface.build_and_run_portfolio_from_df(portfolio_node=node,
                                                       add_portfolio_to_markets_backend=True)
```

Here we use the `PortfolioInterface` to **build and run** the portfolio. This differs slightly from running a plain `DataNode`: the interface populates portfolio‑specific objects in the platform (for example, creating a `PortfolioIndexAsset` linked to this portfolio).

After running the node through the `PortfolioInterface`, you’ll see a few things in the platform:

1) **Target Portfolios**:
   `https://www.main-sequence.app/target-portfolios/` — you should see the new portfolio with the name you provided.

![img.png](img.png)

2) **Tables** (portfolio details stored by the data node):
   `https://www.main-sequence.app/dynamic-table-metadatas/?search=testfixed&storage_hash=&identifier=`

3) **Assets** (the two newly created assets):
   `https://www.main-sequence.app/asset/?search=TEST_&unique_identifier=&figi=&security_type=&security_market_sector=&ticker=`
   Click either asset to explore the **current pricing details** created from the snapshot.

![img.png](img.png)

## Pre‑work Simulating Asset Prices Closes, 

For our dashboards we want also to include the latest price for each asset. We will need a data_node containing closing
prices. You can reuse your price simulator from the previoys tutorial





Now you have pricing details for these assets that you can use to rebuild the instruments with QuantLib—unlocking deeper analysis and richer dashboards.

With the platform **hydrated** (portfolio + assets), you’re ready to move on and build the dashboard UI.
