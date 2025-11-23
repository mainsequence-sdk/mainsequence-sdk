# MainSequence × Streamlit — General Dashboard Authoring Guide (Scaffold‑Aligned)

> **Scope**: This guide is intentionally **general**. It describes how to author Streamlit dashboards that integrate with the MainSequence platform **without prescribing page‑specific controls or domain logic**. It focuses on structure, imports, scaffold usage, state, caching, and safe platform access. It follows the same scaffold and import patterns recommended for MainSequence dashboards.  

---

## 0) Core principles (apply to every page)


1. **Use the MainSequence client** everywhere platform data is touched:
   ```python
   import mainsequence.client as msc
   ```
2. **Boot every file through the Streamlit scaffold** to get unified theming and context:
   ```python
   from mainsequence.dashboards.streamlit.scaffold import PageConfig, run_page
   _ = run_page(PageConfig(title="<PAGE TITLE>", use_wide_layout=True, inject_theme_css=True))
   ```
3. **Register required data dependencies once per session** (idempotent) *before* querying the platform. Keep identifiers in a settings module; do **not** hard‑code them in pages.

These three rules keep pages consistent, portable, and safe to evolve across environments.

4. **Use the proper folder structure** for each dashboard. This means that each dashboard should be inside `dashboards/apps`, and the name of the dashboard will be the name of the folder. For example, `dashboards/apps/my_dashboard` will create a `my_dashboard` dashboard. All dashboards require an `app.py` file, which is the root of the dashboard.

When prompted to create a dashboard, separate the logic between Streamlit components (visualizations) and services. Place the Streamlit components inside a folder called `components` and Mainsequence services inside a folder called `services`. For example, if the user asks for a dropdown to select assets, create all the UI logic inside the `components` folder and import it inside the `app.py` file. Implement the request logic that imports from `mainsequence.client as msc` and uses `msc.Assets` inside the `services` folder.



---

## 1) Project layout (multi‑page Streamlit)

```
your_dashboard/
  app.py
  pages/
    01_<feature_a>.py
    02_<feature_b>.py
    ...
  settings.py   # external identifiers only; no secrets
```

**Conventions**

- Use numeric prefixes (`01_`, `02_`, …) so Streamlit orders pages deterministically.
- Keep **each page self‑contained**: imports, scaffold boot, and an idempotent dependency bootstrap.

---

## 2) Shared page bootstrap (copy pattern)

All Streamlit files—including `app.py` and any `pages/NN_*.py`—should start with the same minimal scaffold and dependency guard. Keep **identifiers** in `settings.py` and import them (do not hard‑code).

```python
from __future__ import annotations

import streamlit as st
import mainsequence.client as msc
from mainsequence.dashboards.streamlit.scaffold import PageConfig, run_page
from dashboards.core.data_nodes import get_app_data_nodes  # your registry helper

# Optional: centralize identifiers in your app settings:
# from your_app.settings import SOME_IDENTIFIER, ANOTHER_IDENTIFIER

# 1) Boot the page (theme, context, layout)
ctx = run_page(PageConfig(
    title="<PAGE TITLE>",
    use_wide_layout=True,
    inject_theme_css=True,
))

# 2) Register data dependencies once (idempotent)
def _ensure_data_nodes_once() -> None:
    if st.session_state.get("_deps_bootstrapped"):  # simple once-flag
        return
    deps = get_app_data_nodes()

    # Example pattern: for each required key, ensure a value is registered.
    # Replace <KEY> / <IDENTIFIER> with names read from your settings module.
    # try:
    #     deps.get("<KEY>")
    # except KeyError:
    #     deps.register(**{"<KEY>": <IDENTIFIER>})

    st.session_state["_deps_bootstrapped"] = True

_ensure_data_nodes_once()

# Optional: clear single-run guards for components you want to re-render on navigation
# st.session_state.pop("<SOME_RENDER_ONCE_FLAG>", None)
```

> **Why**: The scaffold centralizes theming and context; the idempotent dependency guard ensures downstream components can resolve the identifiers they expect, without hard‑coding them in page code.

---

## 3) Minimal root app (`app.py`) pattern

A minimal root file that participates in the same conventions; body content is up to the application.

```python
# app.py
from __future__ import annotations
import streamlit as st
import mainsequence.client as msc
from mainsequence.dashboards.streamlit.scaffold import PageConfig, run_page
from dashboards.core.data_nodes import get_app_data_nodes

ctx = run_page(PageConfig(
    title="<APP TITLE>",
    use_wide_layout=True,
    inject_theme_css=True,
))

def _ensure_data_nodes_once() -> None:
    if st.session_state.get("_deps_bootstrapped_root"):
        return
    deps = get_app_data_nodes()

    # Ensure your pages’ expected keys are registered, using identifiers from settings
    # e.g., deps.register(**{"<KEY>": settings.<IDENTIFIER>})
    st.session_state["_deps_bootstrapped_root"] = True

_ensure_data_nodes_once()

# --- Your app-level content goes here (navigation, overview, etc.) ---
st.write("Welcome.")
```

---

## 4) New page template (`pages/NN_<feature>.py`)

Keep templates **generic** and avoid embedding domain rules in the boilerplate. Add your page body below the guard.

```python
# pages/01_Generic_Page.py
from __future__ import annotations
import streamlit as st
import mainsequence.client as msc
from mainsequence.dashboards.streamlit.scaffold import PageConfig, run_page
from dashboards.core.data_nodes import get_app_data_nodes

ctx = run_page(PageConfig(
    title="<PAGE TITLE>",
    use_wide_layout=True,
    inject_theme_css=True,
))

def _ensure_data_nodes_once() -> None:
    if st.session_state.get("_deps_bootstrapped_01"):
        return
    deps = get_app_data_nodes()

    # Register only the keys this page expects, using identifiers from settings
    # try: deps.get("<KEY>")
    # except KeyError: deps.register(**{"<KEY>": settings.<IDENTIFIER>})

    st.session_state["_deps_bootstrapped_01"] = True

_ensure_data_nodes_once()

# --- Page body (your logic, components, visualizations) ---
```

---

## 5) Platform access via `msc` (generic patterns)

Use the MainSequence client **consistently**. Keep calls cached when they are expensive, and guard optional fields defensively.

```python
# Generic cached query (invalidate by passing a new reload_token from the UI if needed)
@st.cache_data(show_spinner=True)
def fetch_anything(*, query: dict | None = None, reload_token: int = 0):
    # Replace <ModelClass> with a real model class available in your environment
    # return msc.<ModelClass>.filter(**(query or {}))
    return []

# Safe single-item access
# item = msc.<ModelClass>.get_or_none(id=<some_id>)

# Defensive attribute access pattern
# value = getattr(item, "some_field", None)
# nested = getattr(getattr(item, "nested_obj", None), "name", None)
```

**Guidelines**

- Prefer `get_or_none` over `get` when absence is valid.
- Use `@st.cache_data` for pure data fetches; pass an integer `reload_token` to invalidate when the user clicks “Refresh” (no domain assumptions).
- Never hard‑code secrets, URLs, or environment toggles inside pages; rely on MainSequence client configuration.

---

## 6) State, caching, and refresh (agnostic)

- Persist cross‑page/session state with `st.session_state`.
- Cache computationally expensive or repeated fetches with `@st.cache_data(show_spinner=True)`.
- Use a **reload token** in session (e.g., `st.session_state["<token>"] += 1`) to invalidate cached loaders *without* changing their signatures or introducing domain‑specific UI.

Example cache‑invalidated loader:

```python
@st.cache_data(show_spinner=True)
def load_resource(reload_token: int = 0):
    # return msc.<ModelClass>.filter(...)
    return []
```

---

## 7) Tables, charts, and typed columns (generic UI patterns)

- Use `st.dataframe` for large tables; prefer typed `column_config` (e.g., `DatetimeColumn`, `LinkColumn`) when relevant to your data’s schema.
- For quick activity visuals, simple `st.line_chart`/`st.bar_chart` are fine; advanced plotting libraries are optional and page‑specific.
- Avoid hard‑coding labels and units in templates—compute and inject them from your data where possible.

```python
# Example typed display (schema-agnostic)
# st.dataframe(
#     df,
#     hide_index=True,
#     use_container_width=True,
#     column_config={
#         "created_at": st.column_config.DatetimeColumn("Created", format="YYYY-MM-DD HH:mm"),
#         "link": st.column_config.LinkColumn("Link"),
#     },
# )
```

---

## 8) Generic artifact/resource operations (no domain assumptions)

When working with listable resources (files, artifacts, records), the pattern is:

1. **List** resources.
2. **Normalize** essential fields to a DataFrame for display/plots.
3. **Preview** a small recent slice.
4. **Optional batch operation** (e.g., delete), guarded by a confirmation input.

```python
# Listing pattern
# q = msc.<Resource>.filter(**filters)
# items = list(q) if not isinstance(q, list) else q
# rows = [{"id": getattr(x, "id", None), "name": getattr(x, "name", None)} for x in items]
# df = pd.DataFrame(rows)

# Cautious destructive pattern (instance method first, fallback to client call)
# ok = False
# try:
#     if hasattr(item, "delete"): item.delete(); ok = True
# except Exception: pass
# if not ok:
#     try: msc.<Resource>.delete(id=getattr(item, "id", None)); ok = True
#     except Exception: pass
```

Keep confirmations explicit and report successes/failures clearly; never assume permissions.

---

## 9) Settings module contract (centralize identifiers)

Create a `settings.py` for **external identifiers and knobs** only (e.g., table names, snapshot ids, bucket names, feature flags). Pages **import** from this module instead of embedding identifiers.

```python
# settings.py (example structure, not values)
# TABLE_A = "..."        # identifier only
# SNAPSHOT_X = "..."     # identifier only
# FEATURE_FLAG_Y = True  # non-secret config
```

This keeps pages environment‑agnostic and simplifies promotion across dev/test/prod.

---

## 10) UX & error‑handling guidelines (neutral)

- Wrap long operations with `st.spinner("…")` and provide **actionable** messages with `st.info`, `st.warning`, `st.error`.
- Use **safe accessors** and short‑circuit gracefully when prerequisites are missing.
- Prefer **typed inputs** and **clear units** only where your page logic knows them—do not bake them into the template.

---

## 11) Page authoring checklist (copy into every new file)

- [ ] `from __future__ import annotations` at the top.  
- [ ] `import mainsequence.client as msc` and boot with `run_page(PageConfig(...))`.  
- [ ] Idempotent data dependency registration via a helper (e.g., `get_app_data_nodes`).  
- [ ] No hard‑coded identifiers—import from `settings.py`.  
- [ ] Use `@st.cache_data` for pure fetches; add a `reload_token` for invalidation when needed.  
- [ ] Persist cross‑page state in `st.session_state` only.  
- [ ] Defensive platform access (`get_or_none`, `getattr(..., default)`).  
- [ ] Keep templates free of domain logic; put business rules in page‑specific code **below** the bootstrap.  

---

## 12) Notes on parity with the MainSequence scaffold

- The **imports**, **scaffold call**, and **idempotent dependency registration** here mirror the recommended patterns for MainSequence dashboards, keeping your app consistent with the packaged theme and helpers.  
- Ready‑made components (selectors, status HUDs, etc.) may be used where appropriate, but **they are not required** by this template and are deliberately not specified here.

---

*End of general guide.*
