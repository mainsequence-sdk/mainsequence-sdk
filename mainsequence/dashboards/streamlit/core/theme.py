from __future__ import annotations

from pathlib import Path

import streamlit as st

# --------------------- small theme helpers ---------------------


def inject_css_for_dark_accents():
    st.markdown(
        """
        <style>
        /* Subtle tweaks; Streamlit theme itself comes from .streamlit/config.toml */
        .stMetric > div { background: rgba(255,255,255,0.04); border-radius: 6px; padding: .5rem .75rem; }
        div[data-testid="stMetricDelta"] svg { display: none; } /* clean deltas, hide the arrow */
        </style>
        """,
        unsafe_allow_html=True,
    )


def explain_theming():
    st.info(
        "Theme colors come from `.streamlit/config.toml`. "
        "You can’t switch Streamlit’s theme at runtime, but you can tune Plotly’s colors and inject light CSS."
    )


# --------------------- spinner frame loader (runs once on import) ---------------------


def _read_txt(p: Path) -> str:
    return p.read_text(encoding="utf-8").strip()


def _load_spinner_frames_for_this_template() -> list[str]:
    """
    Looks under: <repo>/mainsequence/dashboards/streamlit/assets/

    Order of precedence:
      1) image_1_base64.txt ... image_5_base64.txt
      2) image_base64.txt  (single file, replicated to 5 frames)
      3) spinner_1.txt ... spinner_5.txt
      4) Any *base64.txt (sorted) or *.txt (sorted), up to 5 frames
         - If only one file is found, it is replicated to 5 frames.
         - If 2-4 files are found, the last one is repeated to reach 5.
    On total failure, returns five copies of a 1x1 transparent PNG.
    """
    assets = Path(__file__).resolve().parent.parent / "assets"

    # 1) Named sequence: image_1_base64.txt .. image_5_base64.txt
    seq = [assets / f"image_{i}_base64.txt" for i in range(1, 6)]
    if all(p.exists() for p in seq):
        return [_read_txt(p) for p in seq]

    # 2) Single file replicated
    single = assets / "image_base64.txt"
    if single.exists():
        s = _read_txt(single)
        return [s] * 5

    # 3) Alternate sequence
    alt_seq = [assets / f"spinner_{i}.txt" for i in range(1, 6)]
    if all(p.exists() for p in alt_seq):
        return [_read_txt(p) for p in alt_seq]

    # 4) Any *base64.txt, then any *.txt
    candidates = sorted(assets.glob("*base64.txt")) or sorted(assets.glob("*.txt"))
    frames = [_read_txt(p) for p in candidates[:5]]
    if frames:
        if len(frames) == 1:
            frames = frames * 5
        elif len(frames) < 5:
            frames += [frames[-1]] * (5 - len(frames))
        return frames

    # Fallback: 1x1 transparent PNG
    transparent_png = (
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8"
        "/w8AAn8B9p7u3t8AAAAASUVORK5CYII="
    )
    return [transparent_png] * 5


try:
    _SPINNER_FRAMES_RAW = _load_spinner_frames_for_this_template()
except Exception:
    # Never break import due to spinner assets
    transparent_png = (
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8"
        "/w8AAn8B9p7u3t8AAAAASUVORK5CYII="
    )
    _SPINNER_FRAMES_RAW = [transparent_png] * 5

# Public constants (used only within this module, but left as globals for clarity)
IMAGE_1_B64, IMAGE_2_B64, IMAGE_3_B64, IMAGE_4_B64, IMAGE_5_B64 = _SPINNER_FRAMES_RAW


# --------------------- spinner override (CSS only) ---------------------


def override_spinners(
    hide_deploy_button: bool = False,
    *,
    # Sizes
    top_px: int = 20,  # top-right toolbar spinner size
    inline_px: int = 96,  # inline/status spinner size
    # Timing
    duration_ms: int = 900,
    # Toolbar micro-positioning
    toolbar_nudge_px: int = -2,
    toolbar_gap_left_px: int = 2,
    toolbar_left_offset_px: int = 0,
    # Overlay options (for inline/status)
    center_non_toolbar: bool = True,
    dim_backdrop: bool = False,
    overlay_blur_px: float = 0.0,
    overlay_opacity: float = 0.0,
    overlay_z_index: int = 9990,
) -> None:
    """Replace Streamlit's spinners with a 5‑frame bitmap animation.

    This injects CSS only (no JS). It hides native SVGs and applies the frames
    to the toolbar spinner, inline st.spinner, and st.status icon.
    """

    def as_data_uri(s: str, mime: str = "image/png") -> str:
        s = (s or "").strip()
        return s if s.startswith("data:") else f"data:{mime};base64,{s}"

    i1 = as_data_uri(IMAGE_1_B64)
    i2 = as_data_uri(IMAGE_2_B64)
    i3 = as_data_uri(IMAGE_3_B64)
    i4 = as_data_uri(IMAGE_4_B64)
    i5 = as_data_uri(IMAGE_5_B64)

    st.markdown(
        f"""
<style>
/* ===== 5-frame animation (fixed: do NOT use '0%%') ===== */
@keyframes st-fiveframe {{
  0%  {{ background-image:url("{i1}"); }}
  20% {{ background-image:url("{i2}"); }}
  40% {{ background-image:url("{i3}"); }}
  60% {{ background-image:url("{i4}"); }}
  80% {{ background-image:url("{i5}"); }}
  100%{{ background-image:url("{i5}"); }}
}}

:root {{
  --st-spin-top:{top_px}px;
  --st-spin-inline:{inline_px}px;
  --st-spin-dur:{duration_ms}ms;

  --st-toolbar-nudge:{toolbar_nudge_px}px;
  --st-toolbar-gap:{toolbar_gap_left_px}px;
  --st-toolbar-left:{toolbar_left_offset_px}px;

  --st-overlay-z:{overlay_z_index};
  --st-overlay-bg: rgba(0,0,0,{overlay_opacity});
  --st-overlay-blur:{overlay_blur_px}px;
}}

/* ===== ensure toolbar itself stays clickable above overlays ===== */
div[data-testid="stToolbar"],
[data-testid="stStatusWidget"] {{
  position: relative;
  z-index: calc(var(--st-overlay-z) + 5);
}}

/* ===== hide every built-in spinner glyph (SVG/img) ===== */
[data-testid="stSpinner"] svg,
[data-testid="stSpinnerIcon"] svg,
[data-testid="stStatusWidget"] svg,
header [data-testid="stSpinner"] svg {{
  display: none !important;
}}

/* ===== toolbar spinner (top-right) ===== */
[data-testid="stStatusWidget"] {{
  position:relative;
  padding-left: calc(var(--st-spin-top) + var(--st-toolbar-gap));
}}
[data-testid="stStatusWidget"]::before {{
  content:"";
  position:absolute;
  left: var(--st-toolbar-left);
  top:50%;
  transform: translateY(calc(-50% + var(--st-toolbar-nudge)));
  width:var(--st-spin-top);
  height:var(--st-spin-top);
  background-image:url("{i1}");
  background-repeat:no-repeat;
  background-position:center center;
  background-size:contain;
  animation: st-fiveframe var(--st-spin-dur) steps(1, end) infinite;
}}

/* Optionally hide Deploy/Stop toolbar entirely */
{"div[data-testid='stToolbar']{display:none !important;}" if hide_deploy_button else ""}

/* ===== inline st.spinner ===== */
[data-testid="stSpinner"] {{
  min-height: 0 !important;
}}
{ "[data-testid='stSpinner']::after { content:''; position:fixed; inset:0; background:var(--st-overlay-bg); backdrop-filter: blur(var(--st-overlay-blur)); z-index: var(--st-overlay-z); pointer-events: none; }" if dim_backdrop else "" }
[data-testid="stSpinner"]::before {{
  content:"";
  position: fixed;
  left: 50%;
  top: 50%;
  transform: translate(-50%,-50%);
  width: var(--st-spin-inline);
  height: var(--st-spin-inline);
  background-image:url("{i1}");
  background-repeat:no-repeat;
  background-position:center center;
  background-size:contain;
  animation: st-fiveframe var(--st-spin-dur) steps(1, end) infinite;
  z-index: calc(var(--st-overlay-z) + 1);
}}

/* ===== st.status(...) icon ===== */
[data-testid="stStatus"] [data-testid="stStatusIcon"] svg {{ display:none !important; }}
{ "[data-testid='stStatus']::after { content:''; position:fixed; inset:0; background:var(--st-overlay-bg); backdrop-filter: blur(var(--st-overlay-blur)); z-index: var(--st-overlay-z); pointer-events: none; }" if dim_backdrop else "" }
[data-testid="stStatus"] [data-testid="stStatusIcon"]::before {{
  content:"";
  position: fixed;
  left: 50%;
  top: 50%;
  transform: translate(-50%,-50%);
  width: var(--st-spin-inline);
  height: var(--st-spin-inline);
  background-image:url("{i1}");
  background-repeat:no-repeat;
  background-position:center center;
  background-size:contain;
  animation: st-fiveframe var(--st-spin-dur) steps(1, end) infinite;
  z-index: calc(var(--st-overlay-z) + 1);
}}
</style>
    """,
        unsafe_allow_html=True,
    )


def remove_deploy_button() -> None:
    """
    Hide Streamlit's "Deploy" button while keeping the rest of the toolbar.
    Works across several Streamlit versions/DOM variants.
    """
    st.markdown(
        """
        <style>
        /* Optionally hide only the Deploy button (keep toolbar/menu/spinner) */
        .stDeployButton,
        .stAppDeployButton,
        [data-testid="stDeployButton"],
        [data-testid="stAppDeployButton"],
        /* aria-label variants */
        div[data-testid="stToolbar"] a[aria-label*="Deploy"],
        div[data-testid="stToolbar"] button[aria-label*="Deploy"],
        /* link-based fallbacks seen in older/newer builds */
        div[data-testid="stToolbar"] a[href*="/deploy"],
        div[data-testid="stToolbar"] a[href*="share.streamlit"],
        div[data-testid="stToolbar"] a[href*="streamlit.app/deploy"],
        div[data-testid="stToolbar"] a[href*="streamlit.io/cloud"] {
            display: none !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
