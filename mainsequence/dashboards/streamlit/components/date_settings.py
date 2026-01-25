# dashboards/streamlit/components/date_settings.py
from __future__ import annotations

import datetime as dt
import os

import streamlit as st
from dashboards.components.date_selector import date_selector


def sidebar_date_settings(
    *,
    date_label: str = "Valuation date",
    session_cfg_key: str = "position_cfg_mem",
    cfg_field: str = "valuation_date",
    date_key: str = "valuation_date_input",
    date_help: str | None = None,
    use_last_curve_label: str = "Use last available curve",
    use_last_curve_key: str = "use_last_obs_curve",
    use_last_curve_help: str | None = None,
    env_var_key: str = "USE_LAST_OBSERVATION_MS_INSTRUMENT",
) -> tuple[dt.date, bool]:
    """
    Sidebar date settings: valuation date selector + "use last available curve" toggle.
    Persists date in session cfg and toggles env var for downstream curve construction.
    Returns (valuation_date, use_last_curve).
    """
    valuation_date = date_selector(
        label=date_label,
        session_cfg_key=session_cfg_key,
        cfg_field=cfg_field,
        key=date_key,
        help=date_help,
    )

    default_use_last = st.session_state.get(
        use_last_curve_key,
        os.environ.get(env_var_key, "1").lower() in ("1", "true", "yes", "y", "on"),
    )
    use_last_curve = st.checkbox(
        use_last_curve_label,
        value=default_use_last,
        key=use_last_curve_key,
        help=use_last_curve_help,
    )

    # Set the process env var so downstream curve-building respects it in this run.
    os.environ[env_var_key] = "True" if use_last_curve else "False"

    return valuation_date, use_last_curve
