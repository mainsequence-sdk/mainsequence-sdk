from __future__ import annotations

import os
import sys
from collections.abc import Callable, Mapping, MutableMapping
from dataclasses import dataclass
from importlib.resources import files as _pkg_files
from pathlib import Path
from typing import Any

import streamlit as st

from mainsequence.dashboards.streamlit.core.theme import (
    inject_css_for_dark_accents,
    override_spinners,
    remove_deploy_button,
)


def _detect_app_dir() -> Path:
    """
    Best-effort detection of the directory that contains the running Streamlit app.
    Priority:
      1) sys.modules['__main__'].__file__ (how Streamlit executes scripts)
      2) Streamlit script run context (main_script_path) if available
      3) env override MS_APP_DIR / STREAMLIT_APP_DIR
      4) fallback: Path.cwd()
    """
    # 1) __main__.__file__
    try:
        main_mod = sys.modules.get("__main__")
        if main_mod and getattr(main_mod, "__file__", None):
            return Path(main_mod.__file__).resolve().parent
    except Exception:
        pass

    # 2) Streamlit runtime (private API; guarded)
    try:
        from streamlit.runtime.scriptrunner.script_run_context import get_script_run_ctx

        ctx = get_script_run_ctx()
        if ctx and getattr(ctx, "main_script_path", None):
            return Path(ctx.main_script_path).resolve().parent
    except Exception:
        pass

    # 3) env override
    for var in ("MS_APP_DIR", "STREAMLIT_APP_DIR"):
        p = os.environ.get(var)
        if p:
            try:
                return Path(p).resolve()
            except Exception:
                pass

    # 4) fallback
    return Path.cwd()


def _bootstrap_theme_from_package(
    package: str = "mainsequence.dashboards.streamlit",
    resource: str = "assets/config.toml",  # keep this in assets/ (dot-dirs often excluded from wheels)
    target_root: Path | None = None,
) -> Path | None:
    """
    Ensure there's a streamlit/config.toml next to the app script.
    If missing, copy the packaged default once and rerun so theme applies.
    Returns the path to the config file (or None if not created).
    """
    # Read default theme from the package (if present)

    try:
        src = _pkg_files(package).joinpath(resource)

        if not src.is_file():
            return None  # packaged file not present
        default_toml = src.read_text(encoding="utf-8")
    except Exception:

        return None  # no packaged theme; nothing to do

    app_dir = target_root or _detect_app_dir()
    cfg_dir = app_dir / ".streamlit"
    cfg_file = cfg_dir / "config.toml"

    if not cfg_file.exists():
        try:
            cfg_dir.mkdir(parents=True, exist_ok=True)
            cfg_file.write_text(default_toml, encoding="utf-8")
            # Avoid infinite loop: only rerun once
            if not st.session_state.get("_ms_theme_bootstrapped"):
                st.session_state["_ms_theme_bootstrapped"] = True
                st.rerun()
        except Exception:
            # If we cannot write, just skip silently to avoid breaking the app
            return None

    return cfg_file


# --- App configuration contract (provided by the example app) -----------------

HeaderFn = Callable[[Any], None]
RouteFn = Callable[[Mapping[str, Any]], str]
ContextFn = Callable[[MutableMapping[str, Any]], Any]
InitSessionFn = Callable[[MutableMapping[str, Any]], None]
NotFoundFn = Callable[[], None]


@dataclass
class PageConfig:
    title: str
    build_context: ContextFn | None = None  # required

    render_header: HeaderFn | None = None  # if None, minimal header
    init_session: InitSessionFn | None = None  # set defaults in session_state

    # Optional overrides; if None, scaffold uses its bundled defaults.
    logo_path: str | Path | None = None
    page_icon_path: str | Path | None = None

    use_wide_layout: bool = True
    hide_streamlit_multipage_nav: bool = False
    inject_theme_css: bool = True


# --- Internal helpers ---------------------------------------------------------

_HIDE_NATIVE_NAV = """
<style>[data-testid='stSidebarNav']{display:none!important}</style>
"""


def _hide_sidebar() -> None:
    st.markdown(
        """
        <style>
          [data-testid="stSidebar"]{display:none!important;}
          [data-testid="stSidebarCollapseControl"]{display:none!important;}
        </style>
    """,
        unsafe_allow_html=True,
    )


def _minimal_header(title: str) -> None:
    st.title(title)


def _resolve_assets(
    explicit_logo: str | Path | None, explicit_icon: str | Path | None
) -> tuple[str | None, str | None, str | None]:
    """
    Returns a tuple:
      (logo_path_for_st_logo, page_icon_for_set_page_config, icon_path_for_st_logo_param)

    - If no overrides are provided, uses scaffold defaults:
         mainsequence.dashboards.streamlit/assets/logo.png
        mainsequence.dashboards.streamlit/assets/favicon.png
    - If favicon file is missing, falls back to emoji "📊" for set_page_config.
    - st.logo() will only receive icon_image if a real file exists.
    """
    base_assets = Path(__file__).resolve().parent / "assets"
    default_logo = base_assets / "logo.png"
    default_favicon = base_assets / "favicon.png"

    # Pick explicit override or default paths
    logo_path = Path(explicit_logo) if explicit_logo else default_logo
    icon_path = Path(explicit_icon) if explicit_icon else default_favicon

    # Effective values
    logo_for_logo_api: str | None = str(logo_path) if logo_path.exists() else None
    icon_for_page_config: str | None
    icon_for_logo_param: str | None

    if icon_path.exists():
        icon_for_page_config = str(icon_path)
        icon_for_logo_param = str(icon_path)
    else:
        # Streamlit allows emoji for set_page_config, but st.logo needs a file path.
        icon_for_page_config = "📊"
        icon_for_logo_param = None

    return logo_for_logo_api, icon_for_page_config, icon_for_logo_param


# --- Public entrypoint --------------------------------------------------------


# scaffold.py
def run_page(cfg: PageConfig):
    """
    Initialize page-wide look & feel, theme, context, and header.
    Call this at the top of *every* Streamlit page (Home + pages/*).
    Returns a context object (whatever build_context returns).
    """
    # 1) Page config should be the first Streamlit call
    _logo, _page_icon, _icon_for_logo = _resolve_assets(cfg.logo_path, cfg.page_icon_path)
    st.set_page_config(
        page_title=cfg.title,
        page_icon=_page_icon,
        layout="wide" if cfg.use_wide_layout else "centered",
    )

    # 2) Optional: logo + CSS tweaks
    if _logo:
        st.logo(_logo, icon_image=_icon_for_logo)
    if cfg.inject_theme_css:
        inject_css_for_dark_accents()

    # 3) Spinners (pure CSS)
    override_spinners()
    remove_deploy_button()
    # 4) Do NOT hide the native nav unless explicitly asked
    if cfg.hide_streamlit_multipage_nav:
        st.markdown(_HIDE_NATIVE_NAV, unsafe_allow_html=True)

    # 5) Session + context
    if cfg.init_session:
        cfg.init_session(st.session_state)

    ctx = {}
    if cfg.build_context:
        ctx = cfg.build_context(st.session_state)

    # 6) Header
    if cfg.render_header:
        cfg.render_header(ctx)
    else:
        _minimal_header(cfg.title)

    # 7) Create .streamlit/config.toml on first run (reruns once if created)
    from pathlib import Path

    print(Path.cwd())
    _bootstrap_theme_from_package()

    return ctx
