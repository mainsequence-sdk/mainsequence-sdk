# settings.py
from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace


HERE = Path(__file__).resolve().parent
# ---------------- App identity & app dirs ----------------
APP_VENDOR = "mainsequence"
APP_NAME = "instruments"
APP_ID = f"{APP_VENDOR}/{APP_NAME}"

# All environment variables use this prefix now.
ENV_PREFIX = "MSI"                    # e.g., MSI_CONFIG_FILE, MSI_DATA_BACKEND
ENV_CONFIG_FILE = f"{ENV_PREFIX}_CONFIG_FILE"
ENV_DEFAULT_TOML_FILE = f"{ENV_PREFIX}_DEFAULT_TOML_FILE"


def _user_config_root() -> Path:
    if sys.platform == "win32":
        base = Path(os.environ.get("APPDATA", Path.home() / "AppData" / "Roaming"))
    elif sys.platform == "darwin":
        base = Path.home() / "Library" / "Application Support"
    else:
        base = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config"))
    return (base / APP_VENDOR / APP_NAME).resolve()

APP_ROOT = _user_config_root()
# (No POSITIONS_DIR / BUILDS_DIR / DATA_DIR and no bulk mkdir here.)

# ---------------- tiny config loader (stdlib only) ----------------
def _load_toml(text: str) -> dict:
    try:
        import tomllib  # py311+
        return tomllib.loads(text)
    except Exception:
        return {}

def _load_file_config() -> dict:
    candidates: list[Path] = []
    # 1) explicit path via MSI_CONFIG_FILE
    env_cfg = os.getenv(ENV_CONFIG_FILE)
    if env_cfg:
        candidates.append(Path(env_cfg).expanduser())

    # 2) project-local
    candidates += [Path("./instruments.toml"), Path("./instruments.json")]

    # 3) user config root
    candidates += [APP_ROOT / "config.toml", APP_ROOT / "config.json"]

    for p in candidates:
        try:
            if not p.exists():
                continue
            s = p.read_text(encoding="utf-8")
            if p.suffix.lower() == ".toml":
                return _load_toml(s) or {}
            if p.suffix.lower() == ".json":
                return json.loads(s)
        except Exception:
            pass
    return {}

# ---------------- default TOML (read from a real file next to this module) -----
def _read_default_toml_text() -> str | None:
    """
    Returns the text of the default TOML configuration.
    Order of precedence:
      1) Path from MSI_DEFAULT_TOML_FILE (if set)
      2) instruments.default.toml next to this file
    Returns None if no default file is found/readable.
    """
    candidate = Path(os.getenv(ENV_DEFAULT_TOML_FILE, HERE / "instruments.default.toml")).expanduser()
    try:
        if candidate.exists():
            return candidate.read_text(encoding="utf-8")
    except Exception:
        pass
    return None

def _existing_config_path() -> Path | None:
    env_cfg = os.getenv(ENV_CONFIG_FILE)
    if env_cfg:
        p = Path(env_cfg).expanduser()
        if p.exists():
            return p
    for p in (
        Path("./instruments.toml"),
        Path("./instruments.json"),
        APP_ROOT / "config.toml",
        APP_ROOT / "config.json",
    ):
        if p.exists():
            return p
    return None

def _ensure_default_config_file() -> Path | None:
    """If no config exists anywhere, create one. Never overwrites existing."""
    if _existing_config_path() is not None:
        return None
    default_text = _read_default_toml_text()
    if default_text is None:
        return None

    target = Path(os.getenv(ENV_CONFIG_FILE, APP_ROOT / "config.toml")).expanduser()
    try:
        target.parent.mkdir(parents=True, exist_ok=True)  # ensure parent dir only
        if not target.exists():
            target.write_text(default_text, encoding="utf-8")
    except Exception:
        return None
    return target

# Create a default config file if none is present anywhere.
_ensure_default_config_file()

# Now load the config (env still overrides)
_CFG = _load_file_config()

def _get(key: str, default: str) -> str:
    # Env overrides config file (MSI_<KEY>)
    v = os.getenv(f"{ENV_PREFIX}_{key}")
    if v is not None:
        return v
    try:
        section, leaf = key.lower().split(".", 1)
        return _CFG.get(section, {}).get(leaf, default)
    except Exception:
        return _CFG.get(key, default)

# ---------------- Your existing constants (with overrides) ----------------
# Tables
DISCOUNT_CURVES_TABLE         = _get("DISCOUNT_CURVES_TABLE", "discount_curves")
REFERENCE_RATES_FIXING_TABLE  = _get("REFERENCE_RATES_FIXING_TABLE", "fixing_rates_1d")

# Curve identifiers
TIIE_28_ZERO_CURVE = _get("TIIE_28_ZERO_CURVE", "F_TIIE_28_VALMER")
M_BONOS_ZERO_CURVE = _get("M_BONOS_ZERO_CURVE", "M_BONOS_ZERO_OTR")

# Index UIDs
TIIE_28_UID        = _get("TIIE_28_UID", "TIIE_28")
TIIE_91_UID        = _get("TIIE_91_UID", "TIIE_91")
TIIE_182_UID       = _get("TIIE_182_UID", "TIIE_182")
TIIE_OVERNIGHT_UID = _get("TIIE_OVERNIGHT_UID", "TIIE_OVERNIGHT")

CETE_28_UID        = _get("CETE_28_UID", "CETE_28")
CETE_91_UID        = _get("CETE_91_UID", "CETE_91")
CETE_182_UID       = _get("CETE_182_UID", "CETE_182")

# Optional file locations (let your code decide how to use them)
TIIE_ZERO_CSV      = (_CFG.get("files", {}) or {}).get("tiie_zero_csv")
TIIE28_FIXINGS_CSV = (_CFG.get("files", {}) or {}).get("tiie28_fixings_csv")

# ---------------- Convenience namespaces for legacy import sites ------------
indices = SimpleNamespace(
    TIIE_28_UID=TIIE_28_UID,
    TIIE_91_UID=TIIE_91_UID,
    TIIE_182_UID=TIIE_182_UID,
    TIIE_OVERNIGHT_UID=TIIE_OVERNIGHT_UID,
    CETE_28_UID=CETE_28_UID,
    CETE_91_UID=CETE_91_UID,
    CETE_182_UID=CETE_182_UID,
)
curves = SimpleNamespace(
    TIIE_28_ZERO_CURVE=TIIE_28_ZERO_CURVE,
    M_BONOS_ZERO_CURVE=M_BONOS_ZERO_CURVE,
)
DATA_BACKEND = os.getenv(f"{ENV_PREFIX}_DATA_BACKEND", (_CFG.get("data", {}) or {}).get("backend", "mainsequence"))
data = SimpleNamespace(backend=DATA_BACKEND)
