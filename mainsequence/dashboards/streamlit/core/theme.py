from __future__ import annotations
import streamlit as st

def inject_css_for_dark_accents():
    st.markdown(
        """
        <style>
        /* Subtle tweaks; Streamlit theme itself comes from .streamlit/config.toml */
        .stMetric > div { background: rgba(255,255,255,0.04); border-radius: 6px; padding: .5rem .75rem; }
        div[data-testid="stMetricDelta"] svg { display: none; } /* clean deltas, hide the arrow */
        </style>
        """,
        unsafe_allow_html=True
    )

def explain_theming():
    st.info(
        "Theme colors come from `.streamlit/config.toml`. "
        "You can’t switch Streamlit’s theme at runtime, but you can tune Plotly’s colors and inject light CSS."
    )

def override_spinners(spinner_img_url_or_path: str, hide_deploy_button: bool = False) -> None:
    img = spinner_img_url_or_path
    st.markdown(f"""
    <style>
      [data-testid="stSpinner"] svg {{ display: none !important; }}
      [data-testid="stSpinner"]::before {{
        content: ""; display: inline-block; width: 1.25rem; height: 1.25rem;
        margin-right: .35rem; vertical-align: middle;
        background: url("{img}") no-repeat center / contain;
      }}
      [data-testid="stStatusWidget"] svg {{ display: none !important; }}
      [data-testid="stStatusWidget"]::before {{
        content: ""; display: inline-block; width: 16px; height: 16px;
        background: url("{img}") no-repeat center / contain;
      }}
      {"div[data-testid='stToolbar'] { display:none !important; }" if hide_deploy_button else ""}
    </style>
    """, unsafe_allow_html=True)
