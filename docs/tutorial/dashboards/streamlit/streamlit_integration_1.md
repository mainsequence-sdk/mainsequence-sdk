# Part 5.1: Streamlit Integration I

!!! warning "IMPORTANT"
    Each dashboard folder must include a `README.md` file in the same directory as `app.py`.
    The current Streamlit scaffold can create `.streamlit/config.toml` automatically on first run, so you do not need to commit that file unless you want to override the packaged theme.

## Introduction

In this chapter, we will build a small tutorial dashboard that reads the same objects you created in the Markets chapters:

- mock fixed-income assets with pricing details
- the simulated daily prices table
- the translation table that maps those assets to prices

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
          └─ 02_metadata_and_rules.py
```

This layout is enough for Streamlit discovery on the platform and for a clean local development flow.

## 1) Shared helpers

Create `dashboards/tutorial_fixed_income_dashboard/common.py`.

This helper module should:

1. reuse the tutorial helpers from `src/helpers_mock.py`
2. reuse the simulated prices node from `src/data_nodes/simulated_daily_close_prices.py`
3. expose a `bootstrap_tutorial_data()` helper that rebuilds the tutorial state on demand
4. expose read helpers for assets, table metadata, translation-table rules, and recent price history

Two pieces matter most.

First, keep the translation-table path aligned with the Markets tutorial:

```python
return msc.AssetTranslationTable.get_or_create(
    translation_table_identifier=TRANSLATION_TABLE_IDENTIFIER,
    rules=rules,
)
```

Second, read the simulated prices table through `DataNodeStorage` plus `APIDataNode`:

```python
storage = msc.DataNodeStorage.get(identifier=SIMULATED_PRICES_TABLE)
api_node = APIDataNode(
    data_source_id=storage.data_source.id,
    storage_hash=storage.storage_hash,
)
```

That keeps the dashboard aligned with the same data contract used by the tutorial runner scripts.

### Why use `Asset.query(...)` in a dashboard?

For the dashboard asset loader, prefer:

```python
assets = msc.Asset.query(unique_identifier__in=DEFAULT_TEST_ASSET_UIDS)
```

This comes from `mainsequence.client.models_vam.AssetMixin.query`.

Why use it here:

- it is POST-based, so it is safer for dashboard searches that may grow beyond a short URL
- it accepts the same filter syntax as normal asset filters
- it follows pagination and accumulates all pages
- it also supports friendly aliases such as `ticker`, `name`, and `exchange_code`

For very small one-off filters, `filter(...)` is still fine. But in dashboards, `query(...)` is a better default because UI-driven filters tend to grow over time.

## 2) Landing page

Create `dashboards/tutorial_fixed_income_dashboard/app.py`.

Use the current Streamlit scaffold from the SDK:

```python
from mainsequence.dashboards.streamlit.scaffold import PageConfig, run_page
```

Then initialize the page:

```python
run_page(
    PageConfig(
        title="Tutorial Fixed-Income Dashboard",
        use_wide_layout=True,
        inject_theme_css=True,
    )
)
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

- start with `run_page(PageConfig(...))`
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
    unique_identifier_list=unique_identifiers,
    columns=["open", "high", "low", "close", "volume", "trade_count", "vwap"],
)
```

That is enough for a useful first dashboard page without introducing a second pricing stack.

## 4) Metadata and rules page

Create `dashboards/tutorial_fixed_income_dashboard/pages/02_metadata_and_rules.py`.

This page should:

- fetch the project-specific translation table
- flatten its rules into a table
- show the key platform objects as JSON
- explain the fail-fast behavior of shared translation tables

That last point is important. `WrapperDataNode` validates every referenced target time series during initialization, so a shared translation table is not only a matching ruleset. It is also a dependency manifest.

If a table contains:

- Rule A: `security_type=MOCK_ASSET_TUTORIAL_135 -> simulated_daily_closes_tutorial_135`
- Rule B: `security_type=SOMETHING_ELSE -> old_deleted_table`

then wrapper construction is expected to fail if `old_deleted_table` does not exist, even when the current assets would only match Rule A.

## 5) Local validation

From the project root:

```bash
source .venv/bin/activate
streamlit run dashboards/tutorial_fixed_income_dashboard/app.py
```

If the tutorial data does not exist yet, use the landing-page button to create the assets, translation table, and simulated prices.

## Why this chapter is structured this way

This tutorial chapter stays intentionally small:

- it does not re-teach portfolio construction
- it does not require cloning an external dashboard repository
- it uses the current SDK Streamlit scaffold
- it reuses the data products already created in earlier tutorial chapters

By the end of Part 5.1, you have a working tutorial dashboard in your own project, and Part 5.2 can focus only on deployment.
