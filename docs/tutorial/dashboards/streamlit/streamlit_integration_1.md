# Part 5.1: Streamlit Integration I

!!! warning "IMPORTANT"
    Each dashboard folder must include a `README.md` file in the same directory as `app.py`.
    The SDK does not provide Streamlit UI scaffolding. Keep page setup, styling, and shared UI helpers inside the dashboard project.

## Introduction

In this chapter, we will build a small tutorial dashboard that reads normalized time-series objects:

- mock fixed-income assets with pricing details
- the simulated daily prices table
- the direct API read path for those normalized prices

The goal is to keep the first dashboard chapter small and understandable. Instead of copying a large external example application, we will create a minimal multipage Streamlit app directly inside the tutorial project.

## Project structure

Create the following structure under your project:

```text
dashboards/
  └─ tutorial_fixed_income_dashboard/
          ├─ README.md
          ├─ common.py
          ├─ app.py
          └─ pages/
              ├─ 01_prices_and_assets.py
              └─ 02_metadata.py
```

This layout is enough for Streamlit discovery on the platform and for a clean local development flow.

## 1) Shared helpers

Create `dashboards/tutorial_fixed_income_dashboard/common.py`.

This helper module should:

1. reuse the tutorial helpers from `src/helpers_mock.py`
2. reuse the simulated prices node from `src/data_nodes/simulated_daily_close_prices.py`
3. expose a `bootstrap_tutorial_data()` helper that rebuilds the tutorial state on demand
4. expose read helpers for assets, table metadata, and recent price history

The central piece is reading the simulated prices table through `APIDataNode.build_from_identifier(...)`:

```python
api_node = APIDataNode.build_from_identifier(SIMULATED_PRICES_TABLE)
```

This is the cleanest way to read a published table when you already know its identifier. It keeps the dashboard aligned with the same data contract used by the tutorial runner scripts, without manually resolving `DataNodeStorage` first.

### Why use `APIDataNode.build_from_identifier(...)` here?

Use this helper when your code is consuming an existing table, not creating one.

!!! warning "IMPORTANT"
    This pattern also turns a published table into a reusable API-style data source for other projects.
    Once a table exists in the platform and you know its identifier, another project can resolve it with
    `APIDataNode.build_from_identifier(...)` and read it without rebuilding the original DataNode locally.
    That is one of the main ways to share data products across projects while keeping one published table as
    the source of truth.

It is a good fit for dashboards because:

- the dashboard usually knows the table identifier ahead of time
- it keeps the read path short and readable
- it returns an `APIDataNode` ready for methods such as `get_df_between_dates(...)`

In other words, this is the "I know which table I want, now give me a reader" entry point.

### When should you switch to `mainsequence.tdag.data_nodes.filters`?

`APIDataNode.build_from_identifier(...)` is the right tool for straightforward reads of one table.

When the dashboard starts building ad-hoc filters dynamically, or when it needs joins across dynamic tables, move to the structured filter DSL from `mainsequence.tdag.data_nodes.filters`.

For example:

```python
import datetime as dt

from mainsequence.tdag.data_nodes.filters import F, SearchRequest, and_

request = SearchRequest(
    node_unique_identifier=SIMULATED_PRICES_TABLE,
    filter=and_(
        F.between("time_index", start_date, end_date),
        F.in_("unique_identifier", unique_identifiers),
    ),
)

history = msc.DataNodeStorage.get_data_from_filter(request)
```

That is the better path when the UI is assembling filters at runtime, because it gives you a safe, structured request format instead of hard-coding one read shape into dashboard code.

## 2) Landing page

Create `dashboards/tutorial_fixed_income_dashboard/app.py`.

Use plain Streamlit page setup in the app:

```python
import streamlit as st
```

Then initialize the page:

```python
st.set_page_config(
    page_title="Tutorial Fixed-Income Dashboard",
    layout="wide",
)
st.title("Tutorial Fixed-Income Dashboard")
```

The landing page should:

- explain what the app reads from the platform
- show a few current status metrics
- expose a `Build or Refresh Tutorial Data` button
- link to the dashboard sub-pages with `st.page_link(...)`

The refresh button is useful in a tutorial because it lets the reader recover the demo state directly from the app:

```python
if st.button("Build or Refresh Tutorial Data"):
    status = bootstrap_tutorial_data()
```

## 3) Prices and assets page

Create `dashboards/tutorial_fixed_income_dashboard/pages/01_prices_and_assets.py`.

This page should:

- start with `st.set_page_config(...)`
- offer a sidebar asset selector
- offer a sidebar lookback window
- load recent price history from the simulated prices table
- render:
  - a close-price chart
  - a latest-bars table
  - an asset summary table
  - a raw-history table

The central read path is:

```python
history = api_node.get_df_between_dates(
    start_date=start_date,
    dimension_filters={"unique_identifier": unique_identifiers},
    columns=["open", "high", "low", "close", "volume", "trade_count", "vwap"],
)
```

That is enough for a useful first dashboard page without introducing a second pricing stack.

## 4) Metadata page

Create `dashboards/tutorial_fixed_income_dashboard/pages/02_metadata.py`.

This page should:

- fetch table metadata for the simulated prices source
- show the key platform objects as JSON
- explain that consumers should reference the normalized price source explicitly by identifier

## 5) Local validation

From the project root:

```bash
source .venv/bin/activate
streamlit run dashboards/tutorial_fixed_income_dashboard/app.py
```

If the tutorial data does not exist yet, use the landing-page button to create the assets and simulated prices.

## Why this chapter is structured this way

This tutorial chapter stays intentionally small:

- it does not re-teach domain construction
- it does not require cloning an external dashboard repository
- it uses plain Streamlit app code owned by the dashboard project
- it reuses the data products already created in earlier tutorial chapters

By the end of Part 5.1, you have a working tutorial dashboard in your own project, and Part 5.2 can focus only on deployment.

For the current dashboard guidance, see [Streamlit Dashboards](../../../knowledge/dashboards/streamlit/index.md).
