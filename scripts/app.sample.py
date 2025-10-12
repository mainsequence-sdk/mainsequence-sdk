# dashboards/apps/minimal_app/app.py
from __future__ import annotations
import sys
from pathlib import Path
import streamlit as st
from mainsequence.dashboards.streamlit.scaffold import PageConfig, run_page


# Ensure repo root is importable (same as before)
ROOT = Path(__file__).resolve().parent
if str(ROOT.parent.parent) not in sys.path:
    sys.path.insert(0, str(ROOT.parent.parent))


ctx = run_page(PageConfig(
    title="Main Sequence Demo App",
    use_wide_layout=True,
    inject_theme_css=True,
))

st.caption("Create a new page in the `pages/` folder ")
