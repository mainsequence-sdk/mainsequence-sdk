# Getting Started 5: From Data to Dashboards II
**Build the Fixed‑Income “Curve, Stats & Positions” dashboard on the Main Sequence platform**  
*(Beginner‑friendly, step‑by‑step; uses Streamlit on the Main Sequence platform at every stage.)*

> This Part II continues from **Getting Started 5: From Data to Dashboards I** (Part I). If you haven’t finished the pre‑work (mock portfolio + price node), please complete it first. fileciteturn0file0

---

## What you’ll build (on the Main Sequence platform)

A production‑ready, multi‑page Streamlit app that runs **on the Main Sequence platform** and lets you:

1) **Load a portfolio from the Main Sequence platform** and convert it into a priced **Position**.  
2) **Build and bump yield curves** (by family) using QuantLib **inside the Main Sequence platform**.  
3) **Compute z‑spreads from dirty prices** so mark‑to‑market is consistent with **Main Sequence platform** prices.  
4) Visualize **par yield curves** with **position yield overlays** and view **NPV/carry stats** — all computed against data served by the **Main Sequence platform**.  
5) Inspect **data‑node dependencies** and **per‑asset cashflows**, using assets and instrument dumps coming **from the Main Sequence platform**.

Everything below uses the **Main Sequence platform** explicitly. When you see `msc.*` or “Data Node” or “Portfolio”, that’s the Main Sequence SDK/API operating on platform objects.

---

## Repository layout used in this guide

We will work with the exact files you provided; keep this structure in your repo so the **Main Sequence platform** can discover and deploy Streamlit apps automatically.

```
dashboards/
└─ apps/
   └─ floating_portfolio_analysis/
      ├─ .streamlit/config.toml
      ├─ app.py
      ├─ settings.py
      └─ pages/
         ├─ 01_curve_and_positions.py       # “Curve, Stats & Positions”
         ├─ 02_data_nodes_dependencies.py    # Data‑node graph (Mermaid)
         └─ 99_asset_detail.py               # Per‑asset JSON + cashflows
└─ services/
   ├─ curves.py
   ├─ portfolios.py
   └─ positions.py
```

> **Heads‑up (quick‑link slug):** `app.py` links to `pages/01_Curve_Stats_Positions`, while the file is named `01_curve_and_positions.py`. Either rename the file to `01_Curve_Stats_Positions.py` **or** update the link targets in `app.py` so the quick links work. This only affects the links; Streamlit will still show the page in the sidebar.

---

## 0) Prerequisites (from Part I)

You should already have, **on the Main Sequence platform**:

- A **mock fixed‑income portfolio** with two assets (created in Part I).  
- A **price data node** that provides closes for your assets (Part I asked you to reuse a simulator).  
- The two assets registered with **instrument pricing details** so you can rebuild instruments in Python.  
- Your repository connected to the **Main Sequence platform** deployer for dashboards. fileciteturn0file0

---

## 1) Theme & App shell (Main Sequence platform discovers this automatically)

**File:** `.streamlit/config.toml`  
This sets a dark theme for Streamlit. When you push this repo, the **Main Sequence platform** packages and serves the same visual theme in your dashboard.

**File:** `app.py`  
- Calls `register_theme()` → injects custom theme (see “Fill‑me references” below).  
- `st.set_page_config(...)` → Streamlit page setup used by the **Main Sequence platform** app runner.  
- Shows quick links to the two pages.

> **Main Sequence note:** The **Main Sequence platform** detects apps under `dashboards/` and runs them with Streamlit as your app server. No extra wiring is needed once the repo is synced.

---

## 2) Configure which price table to read on the Main Sequence platform

**File:** `settings.py`

```python
PRICES_TABLE_NAME = "simulated_daily_closes_f1_v1"  # vector_de_precios_valmer
```

This tells the app which **Main Sequence platform** table to read for **instrument dirty prices** (via a registered data node). If your simulator wrote to a different identifier, set it here. The app (page “Curve, Stats & Positions”) will register this table in the **Main Sequence platform** data‑node registry so the services can pull prices.

**Where it’s used (Main Sequence platform):**

- `pages/01_curve_and_positions.py` → `_ensure_data_nodes()` calls `get_app_data_nodes()` and registers `instrument_pricing_table_id=PRICES_TABLE_NAME`.  
- Later, `PortfoliosOperations` uses this registered node to query price closes from the **Main Sequence platform** for each asset when building a position.

---

## 3) Page “Curve, Stats & Positions” — end‑to‑end workflow on the platform

**File:** `pages/01_curve_and_positions.py`

This page is the core of the tutorial. Here is the user flow, each step happening **on the Main Sequence platform**:

### 3.1 Sidebar: valuation date & notional (platform context)

- **Valuation date** selector (`date_selector`) persists to `session_state`. This date is used to set QuantLib’s evaluation date and to build curves. This action configures how your pricing will run **on the Main Sequence platform** runtime.  
- **Portfolio notional** converts weights into integer holdings (units) later on.

> **Fill‑me reference:** `dashboards.components.date_selector.date_selector` (see Fill‑me section).

### 3.2 (Optional) Build the mock portfolio **on the Main Sequence platform**

- Clicking **“Build mock portfolio”** calls `dashboards.helpers.mock.build_test_portfolio("mock_portfolio_floating_dashboard")`.  
- This uses the **Main Sequence platform** `PortfolioInterface` (see Part I) to create a new target portfolio, assets, and associated instrument pricing details in your tenant. fileciteturn0file0

> If the helper is not present, the button shows an error. You can paste the Part I helper (`dashboards/helpers/mock.py`) into your repo. fileciteturn0file0

### 3.3 Pick a portfolio from the **Main Sequence platform**

- The sidebar search (`sidebar_portfolio_multi_select`) queries portfolios **on the Main Sequence platform** so users can select one.  
- When you select a portfolio, the code calls `PortfoliosOperations.get_all_portfolios_as_positions(...)` to **materialize a Position object** from the latest portfolio weights retrieved from the **Main Sequence platform**.

> **Fill‑me reference:** `dashboards.components.portfolio_select.sidebar_portfolio_multi_select` (see Fill‑me section).

### 3.4 How the Position is built (platform data → priced instruments)

`PortfoliosOperations._build_position_from_portfolio(...)` (in `services/portfolios.py`) performs these steps **on the Main Sequence platform**:

1) Fetch latest weights via `portfolio.get_latest_weights()` (platform call).  
2) Resolve `msc.Asset` objects by `unique_identifier` (platform asset registry).  
3) Read **dirty prices** for these assets from the price **Data Node** registered earlier (`instrument_pricing_table_id`), via `data_node.get_last_observation(...)`.  
4) Assemble a `msi.Position` with **integer units** so that `units × dirty_price ≈ (weight × notional)`.  
5) Return the position **and** a `instrument_hash_to_asset` map so the UI can show **Main Sequence platform** identifiers (UIDs) in tables.

> If your price node returns nothing, the service raises `Exception("No price for portfolio assets")`. Ensure your simulator on the **Main Sequence platform** produced recent closes for the assets.

### 3.5 Curves on the platform: base vs bumped

- The page introspects which **floating‑rate indices** your position uses; it builds a per‑family bump UI (TIIE, CETE, etc.).  
- `services/curves.build_curves_for_ui(...)` uses `mainsequence.instruments.pricing_models.indices.build_zero_curve` under the hood to obtain a base `YieldTermStructure` for each index **on the Main Sequence platform** runtime.  
- A **BumpSpec** (parallel + key‑rate bp) is applied to construct a bumped curve. Both base and bumped curves are returned as QuantLib handles.

> **Fill‑me reference:** `dashboards.components.curve_bump.curve_bump_controls_ex` (UI widget that writes a `{family: {parallel_bp, keyrate_bp{...}}}` spec to session state).

### 3.6 Position instantiation & z‑spreads (platform prices honored)

- `PositionOperations.from_template(...)` deep‑copies the template position and **resets each instrument’s curve** to the base (then bumped) curve *by its floating index name*.
- `PositionOperations.compute_and_apply_z_spreads_from_dirty_price(...)` reads the **dirty price** we fetched from the **Main Sequence platform** and solves a **constant z‑spread** so `dirty_price == PV(curve+z)`. The same `z` is mirrored to the bumped position so your ΔNPV reflects only the curve move.

### 3.7 Par‑curve chart + position yield overlay

- `plot_par_yield_curves_by_family(...)` plots base vs bumped par curves.  
- `st_position_yield_overlay(...)` adds your instruments’ **current yields** as points on the chart to connect position reality with the curve. Those yields normally come from your **Main Sequence platform** pricing details / extra metadata.

> **Fill‑me references:** `dashboards.plots.curves.plot_par_yield_curves_by_family` and `dashboards.components.position_yield_overlay.st_position_yield_overlay` (see Fill‑me section).

### 3.8 Portfolio metrics computed on the platform

- **NPV (base/bumped)** and **ΔNPV**  
- **Carry** to a user‑selected cutoff (computed by aggregating projected cashflows)

All metrics use the QuantLib instruments and curves that run **on the Main Sequence platform** runtime.

> **Fill‑me reference:** `dashboards.core.formatters.fmt_ccy` (for currency formatting in Streamlit metrics).

### 3.9 Positions table (paginated) with platform links

- `PortfoliosOperations.st_position_npv_table_paginated(...)` renders a searchable, paginated table with base/bumped price and NPVs per line.  
- The **“details”** link navigates to `pages/99_asset_detail.py` with a query param so users can drill down into a single asset **on the Main Sequence platform**.

---

## 4) Page “Data Nodes — Dependencies” (platform view)

**File:** `pages/02_data_nodes_dependencies.py`

- Generates a Mermaid graph from a **dependency payload**. The file currently uses `_mock_fetch_dependencies()` for a static example.
- On the **Main Sequence platform**, replace `_mock_fetch_dependencies()` with a call to your data‑catalog/TDAG endpoint to fetch node/edge metadata for the selected table or node.

The page also offers a **“Download .mmd”** button so users can export the diagram generated from platform metadata.

---

## 5) Page “Asset Detail” (platform drill‑down)

**File:** `pages/99_asset_detail.py`

- Accepts `?id=<int>` **or** `?unique_identifier=<str>` as query params.  
- Looks up the asset via `msc.Asset.get_or_none(...)` **on the Main Sequence platform**.  
- Displays the **asset JSON** and the **instrument dump** provided by the platform’s pricing detail (`asset.current_pricing_detail.instrument_dump`).  
- Rebuilds the instrument with `mainsequence.instruments` and shows **cashflows**, downloadable as CSV.

This page is invaluable for validating the **Main Sequence platform** object <→ instrument parity.

---

## 6) How the provided `services/` modules work (plain‑English overview)

All three modules are pure Python helpers that run inside your Streamlit app hosted by the **Main Sequence platform**.

### `services/curves.py` (curve families, bumps, par rates)

- **`curve_family_key()` / `KEYRATE_GRID_BY_FAMILY`**: Map index IDs (e.g., TIIE, CETE) to curve families and define the tenors for key‑rate bumps.
- **`BumpSpec`**: Dataclass holding parallel and key‑rate bump sizes (in bps) and the per‑index key‑rate grid.
- **`build_curves_for_ui(calc_date, spec, index_identifier)`**: Uses the **Main Sequence platform** instrument/indices helpers to build a **base QuantLib term structure** and a **bumped** copy from `BumpSpec`; returns both plus node samples for plotting.
- **`zspread_from_dirty_ccy(...)`**: Solves the constant z‑spread (continuous comp) such that PV equals the platform dirty price.
- **Par‑curve helpers** (`par_curve`, `par_nodes_from_tenors`): compute par rates to plot base/bumped curves.

### `services/portfolios.py` (turn platform portfolios into positions)

- **Signals & prices**: Consolidates signals (weights) and price history for the selected portfolios **from the Main Sequence platform**.  
- **`_make_weighted_lines(...)`**: Uses the **platform price Data Node** to fetch the last **dirty price per asset** and translates weights×notional into integer **units** per instrument line.  
- **`get_all_portfolios_as_positions(...)`**: For each selected **Main Sequence platform** portfolio, return `(instrument_hash_to_asset, Position)` so the UI can price and display UIDs.  
- Extras: Rolling leaders plots and joint portfolio simulations are included for broader analytics (also operating on platform data).

### `services/positions.py` (instantiate, z‑spreads, stats)

- **`PositionOperations.from_template(...)`**: Deep‑copy the template, set valuation date, and **reset curves** [base/bumped] by floating index name.  
- **`compute_and_apply_z_spreads_from_dirty_price(...)`**: Read each line’s `extra_market_info["dirty_price"]` (from the **Main Sequence platform**), compute `z`, and attach it to base & bumped lines.  
- **`portfolio_style_stats(...)`**: Return NPV/carry and deltas.  
- **`st_position_npv_table_paginated(...)`** (in `portfolios.py`): Render a searchable Streamlit table with optional CSV export and a link to asset details.

---

## 7) Fill‑me references (imports you didn’t include here)

The app imports a few utilities not shown in your snippet. Create these modules with the minimal interfaces below (or copy your real implementations) so the app runs end‑to‑end **on the Main Sequence platform**.

> You can paste these filenames under `dashboards/` and fill the bodies later. The descriptions explain what each piece should do.

| Module path | What it should provide | Why it matters (on the Main Sequence platform) |
|---|---|---|
| `dashboards/core/theme.py` → `register_theme()` | Optional: apply Plotly/Streamlit theme defaults (fonts, grid, hover). Safe to no‑op. | Harmonizes visuals of charts served by the platform. |
| `dashboards/components/date_selector.py` → `date_selector(label, session_cfg_key, cfg_field, key, help)` | A small widget that writes `session_state[session_cfg_key][cfg_field] = date_iso`. | Controls the **platform** valuation/pricing date. |
| `dashboards/components/portfolio_select.py` → `sidebar_portfolio_multi_select(title, key_prefix, min_chars)` | A sidebar search on **Main Sequence platform** portfolios that returns a list of objects with `.reference_portfolio`. | Lets users load platform portfolios. |
| `dashboards/components/curve_bump.py` → `curve_bump_controls_ex(available_tenors, default_bumps, default_parallel_bp, header, container, key)` | UI to input a parallel bump and per‑tenor key‑rate bumps; return a spec (dict or `BumpSpec`). | Builds the bump spec that feeds the curve builder. |
| `dashboards/components/position_yield_overlay.py` → `st_position_yield_overlay(position, valuation_date, key)` | Return a list of Plotly `Scatter` traces to overlay instrument yields on the curve chart. | Shows where **platform** instruments sit vs curve. |
| `dashboards/plots/curves.py` → `plot_par_yield_curves_by_family(base_curves, bumped_curves, max_years, step_months, title)` | Return a Plotly `Figure` with par curves (base vs bumped) per family. | Visualizes curve scenarios built on the platform. |
| `dashboards/core/formatters.py` → `fmt_ccy(x)` | Simple number→currency string (e.g., `1_234.56 → "1,234.56"`). | Human‑readable metrics in platform dashboards. |
| `dashboards/core/ql.py` → `qld(py_date)` | Convert `datetime.date` → `QuantLib.Date`. | Aligns Python dates and QuantLib inside the platform. |
| `dashboards/core/data_nodes.py` → `get_app_data_nodes()` | A registry object (with `.get()` and `.register(...)`) that stores handles to **Main Sequence platform** data nodes (e.g., your prices table). | Lets services discover which platform table to query. |
| `dashboards/helpers/mock.py` → `build_test_portfolio(name)` | The Part I helper that creates the mock portfolio and registers assets + pricing details **on the Main Sequence platform**. | One‑click demo data on the platform. fileciteturn0file0 |

> **Tip:** For local experiments, you can stub any of these to return static data; on the **Main Sequence platform** you’ll point them at your tenant’s APIs (e.g., via `mainsequence.client` / TDAG endpoints).

---

## 8) Running the app on the Main Sequence platform

1) **Commit & push** this repo (with the `dashboards/` folder) to the branch your **Main Sequence platform** project deploys from.  
2) In the platform UI, open the **Dashboards** section and launch the app. The platform will start Streamlit and serve it behind your project URL.  
3) On first run, set a **Valuation date** (sidebar), click **Build mock portfolio** (optional), and **search for your portfolio** created in Part I.  
4) Adjust **Curve bumps** (parallel/key‑rate) and observe **ΔNPV** and **Carry** on the **Main Sequence platform** runtime instantly.  
5) Click a position row’s **Details** link to open **Asset Detail** and download cashflows computed from your platform instrument dump.

---

## 9) Troubleshooting (platform‑specific)

- **No prices found for portfolio assets** → Verify `settings.PRICES_TABLE_NAME` matches your simulator’s table **on the Main Sequence platform** and that it has recent `close` values for the UIDs in your portfolio.  
- **Curve quick links don’t open** → Fix the filename/link mismatch noted at the top.  
- **QuantLib date errors** → Make sure the **Valuation date** is set and `ql.Settings.instance().evaluationDate` is in sync (the page already sets it).  
- **Mock portfolio button fails** → Copy the Part I helper into `dashboards/helpers/mock.py` (see Fill‑me references). fileciteturn0file0  
- **Asset Detail shows “no current_pricing_detail”** → Confirm your assets on the **Main Sequence platform** have `instrument_dump` attached (Part I). fileciteturn0file0

---

## 10) What happens under the hood (data flow recap on the Main Sequence platform)

1) **Portfolio → weights** (platform): `msc.Portfolio` → latest weights.  
2) **Assets → prices** (platform): price **Data Node** → last dirty price per UID.  
3) **Weights × Notional → Position units** (local in app).  
4) **Curves (base/bumped)** (platform runtime): built with `build_zero_curve` + `BumpSpec`.  
5) **Price & z‑spreads** (platform runtime): QuantLib instruments priced; z solved to match platform dirty price.  
6) **KPIs & charts** (platform runtime): ΔNPV, carry, and par curves rendered in Streamlit served by the **Main Sequence platform**.

---

## 11) Next steps

- Replace the static dependency graph with a real **Main Sequence platform** metadata call.  
- Add more curve families and assets (CETE, UST, etc.) and display cross‑currency risk.  
- Wire real position yield overlays by reading yields from the **Main Sequence platform** pricing details.

---

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

