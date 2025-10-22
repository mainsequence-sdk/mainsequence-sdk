# Getting Started 5: From Data to Dashboards II
**Build the Fixed‑Income “Curve, Stats & Positions” dashboard on the Main Sequence platform**

> Updated to reflect your current repository state for the *Fixed Income Position Dashboard* (multipage Streamlit app with curve bumps, portfolio/group selection, z‑spreads, and per‑asset drill‑downs).

---

## What you’ll build (now aligned to your latest code)

A production‑ready, multi‑page Streamlit app that runs on the **Main Sequence platform** and lets you:

1) Load one or more **platform portfolios** and convert their weights into **priced Positions**.  
2) Build and **bump yield curves** (parallel & key‑rate) and reuse the **same curves** across selected portfolios.  
3) Compute **z‑spreads from dirty prices** so ΔNPV reflects only the curve move.  
4) View a **par‑curve chart** and a **paginated NPV table** with base/bumped metrics per instrument.  
5) Inspect **data‑node dependencies** and **per‑asset cashflows** (rebuilt from the platform’s instrument dump).

Everything below uses the **Main Sequence SDK/API** against objects in your tenant.

---

## Repository layout (current)

You need to recreate this structure under your project’s `dashboards/` folder:

```
dashboards/
  └─ bond_portfolio_analysis/
    ├─ .streamlit/config.toml
    ├─ app.py
    ├─ settings.py
    └─ pages/
        ├─ 01_curve_and_positions.py       # “Curve, Stats & Positions”
        ├─ 02_data_nodes_dependencies.py    # Data‑node graph (Mermaid)
        └─ 99_asset_detail.py               # Per‑asset JSON + cashflows
```

**IMPORTANT** Beside this - you need to bring other required folders from `dashboards/` folder in repo to `dashboards/` folder in your project: `components/`, `core/`, `plots/`, `services/` to recreate same structure in your project. Best way is to clone whole repo somewhere on your computer and copy required folders to your project.


> **Quick‑link note:** `app.py` creates buttons linking to `pages/01_Curve_Stats_Positions` and `pages/02_Data_Nodes_Graph`. Your actual files are `01_curve_and_positions.py` and `02_data_nodes_dependencies.py`. Either rename the files to match the links **or** adjust the two link targets in `app.py`. Streamlit will still show the pages in the sidebar either way.

---

## Code walkthrough
Explore the key parts of each file below and print this code to your dashboard app from example repo: https://github.com/mainsequence-sdk/ExampleDashboards/tree/main/dashboards/apps/bond_portfolio_analysis

## Let’s dive in!

## 1) Theme & app shell

- **`.streamlit/config.toml`** sets your dark theme (primary/background/text colors, font) and enables headless/XSFR protection.  
- **`app.py`** registers the theme, sets the page config (`page_title="Fixed Income Position Dashboard"`), and provides quick links to the app pages. It also explains that all pages live under `pages/` and that no registry is required (native Streamlit multipage routing). This file has custom theme injection from `dashboards/core/theme.py`.
---

## 2) Tell the app which **prices** table to read

**File:** `settings.py`

```python
from dashboards.helpers.mock import SIMULATED_PRICES_TABLE


PRICES_TABLE_NAME=SIMULATED_PRICES_TABLE
```

The page `01_curve_and_positions.py` calls a small bootstrap (`_ensure_data_nodes()`) that registers this table under the app’s data‑node registry so the services can retrieve each asset’s **dirty price** from the platform.

---

## 3) Page “Curve, Stats & Positions” (end‑to‑end flow)

**File:** `pages/01_curve_and_positions.py`

### 3.1 Sidebar: valuation date & notional

- A **valuation date** control (`date_selector`) writes the date to `st.session_state` and sets QuantLib’s evaluation date.  
- A **portfolio notional** is used to translate weights into integer **units** later on.

> The page uses `run_page(PageConfig(...))` from the platform’s Streamlit scaffold and clears any stale “render once” flags.

### 3.2 Optional: Build the mock portfolio in‑app

- Click **“Build mock portfolios”** to run `dashboards.helpers.mock.build_test_portfolio("mock_portfolio_floating_dashboard")`.  
- This populates a demo *target portfolio*, two assets, and platform **instrument pricing details** so downstream pages and drill‑downs work.

### 3.3 Pick portfolios (and groups), then dedupe

- Use `sidebar_portfolio_multi_select` to search/select portfolios from the platform.  
- Optionally toggle **“Include portfolio groups”**; group selections are flattened into portfolios.  
- The list is **deduplicated by id** before building positions.

### 3.4 Build positions from platform data

When the selection or notional changes, the page:

1) Invokes `PortfoliosOperations(...).get_all_portfolios_as_positions(portfolio_notional=...)` to build a **Position** per portfolio.  
2) Persists to session state:  
   - `instrument_hash_to_asset` (for UID display/links),  
   - `position_templates_mem` (`{portfolio_id → Position template}`),  
   - `_merged_template` (all lines merged) to drive a **single shared curve control**,  
   - `_active_portfolio_ids`, `_active_notional`, and a mapping of ids → portfolio objects.  
3) Calls `st.rerun()` to refresh the page with the new state.

### 3.5 Curves: one control & chart for all portfolios

- `st_curve_bumps(...)` (from `dashboards.components.curve_bumps_and_stats`) takes the **merged position** and the valuation date, builds **base curves** and **bumped curves** by family, and renders the **par‑curve chart**.  
- The function returns `base_curves` and `bumped_curves` so we can reuse the same curves across all selected portfolios.

### 3.6 Per‑portfolio tabs: instantiate, z‑spreads, metrics

For each selected portfolio (one **tab** per id):

- Instantiate `PositionOperations` from the position template and **set the base/bumped curves**.  
- Compute **z‑spreads** from platform dirty prices so that PV(base+z) matches each asset’s price; mirror the same `z` to the bumped position.  
- Render a **paginated NPV table** via `PortfoliosOperations.st_position_npv_table_paginated(...)`, with optional search and CSV download. The table shows base/bumped NPV per line and summary stats (carry to cutoff, etc.).

---

## 4) Page “Data Nodes — Dependencies”

**File:** `pages/02_data_nodes_dependencies.py`

- Builds a **Mermaid** graph from a dependency payload (currently `_mock_fetch_dependencies()` in the file).  
- Lets users switch layout (TD/LR/BT/RL), view the raw payload, and **download** the `.mmd` snapshot.  
- To connect it to live metadata, replace the mock with a call to your platform’s TDAG/data‑catalog endpoint.

---

## 5) Page “Asset Detail”

**File:** `pages/99_asset_detail.py`

- Accepts `?id=<int>` or `?unique_identifier=<str>` as query parameters.  
- Loads the `msc.Asset` from the platform; shows the asset JSON and **instrument dump**.  
- Rebuilds the instrument with `mainsequence.instruments` and displays **cashflows**, with a CSV download button.

---

## 6) Run it on the platform

1) Commit and push to the branch your project deploys from using `mainsequence project open-signed-terminal <PROJECT_ID>` and run: 
```bash
git add .
git commit -m "Add Fixed Income Position Dashboard"
git push
```
3) In the platform UI, open your project and in **Dashboards → bond_portfolio_analysis → View**.  
4) In the sidebar pick a **Valuation date**, click on **Build mock portfolios**, and in the **Portfolio groups** search by `Mock Bond Portfolio with Signals Group` then in dropdown select it and click "Load Selected".  
5) Bump the curve and inspect **ΔNPV**/**carry**; drill down into assets as needed.

---

## 7) Troubleshooting

- **Quick links don’t open** → Fix the slug mismatch between `app.py` links and page filenames.  
- **No prices for assets** → Confirm `settings.PRICES_TABLE_NAME` matches your simulator’s table and that it has recent closes.  
- **No instrument dump** → Ensure the assets’ `current_pricing_detail.instrument_dump` exists (the mock builder sets this up).


### Appendix A — Minimal interface sketches (copy/paste shells)

> **Only if you need quick stubs to unblock the app locally.** In production, use the real implementations that read/write to the **Main Sequence platform**.

```python
# dashboards/core/theme.py
def register_theme():
    # Optional: set Plotly templates, etc. Safe no-op.
    return None
```

```python
# dashboards/core/ql.py
import QuantLib as ql
def qld(d): 
    return ql.Date(d.day, d.month, d.year)
```

```python
# dashboards/core/formatters.py
def fmt_ccy(x): 
    try: return f"{float(x):,.2f}"
    except: return "—"
```

```python
# dashboards/core/data_nodes.py
class _Deps:
    def __init__(self): self._m = {}
    def get(self, k): return self._m[k]
    def register(self, **kwargs): self._m.update(kwargs)
_deps = _Deps()
def get_app_data_nodes(): return _deps
```

*(Build your actual Main Sequence integrations as needed.)*

---

**You now have a complete, multipage, curve‑aware fixed‑income dashboard running on the Main Sequence platform.**  
Push your repo, open the dashboard, and try a few curve bump scenarios!

