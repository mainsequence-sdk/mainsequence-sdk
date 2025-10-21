from __future__ import annotations

import json
import os
import pathlib
import sys
import time

APP_NAME = "MainSequenceCLI"


def _config_dir() -> pathlib.Path:
    home = pathlib.Path.home()
    if sys.platform == "win32":
        base = pathlib.Path(os.environ.get("APPDATA", home))
        return base / APP_NAME
    elif sys.platform == "darwin":
        return home / "Library" / "Application Support" / APP_NAME
    else:
        return home / ".config" / "mainsequence"


CFG_DIR = _config_dir()
CFG_DIR.mkdir(parents=True, exist_ok=True)

CONFIG_JSON = CFG_DIR / "config.json"
TOKENS_JSON = CFG_DIR / "token.json"  # {username, access, refresh, ts}

DEFAULTS = {
    "backend_url": os.environ.get("MAIN_SEQUENCE_BACKEND_URL", "https://main-sequence.app/"),
    "mainsequence_path": str(pathlib.Path.home() / "mainsequence"),
    "version": 1,
}


def read_json(path: pathlib.Path, default):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        return default


def write_json(path: pathlib.Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2), encoding="utf-8")
    # atomic on POSIX and Windows (moves/overwrites safely)
    os.replace(tmp, path)

def get_config() -> dict:
    cfg = DEFAULTS | read_json(CONFIG_JSON, {})
    # ensure base exists
    pathlib.Path(cfg["mainsequence_path"]).mkdir(parents=True, exist_ok=True)
    return cfg


def set_config(updates: dict) -> dict:
    cfg = get_config() | (updates or {})
    cfg["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    write_json(CONFIG_JSON, cfg)
    return cfg





def get_tokens() -> dict:
    return read_json(TOKENS_JSON, {})


def save_tokens(username: str, access: str, refresh: str) -> None:
    write_json(
        TOKENS_JSON,
        {"username": username, "access": access, "refresh": refresh, "ts": int(time.time())},
    )


def set_env_access(access: str) -> None:
    # For the current process (and children). Parent shell can't be set from here.
    os.environ["MAIN_SEQUENCE_USER_TOKEN"] = access


def backend_url() -> str:
    cfg = get_config()
    url = (cfg.get("backend_url") or DEFAULTS["backend_url"]).rstrip("/")

    if os.environ.get("MAIN_SEQUENCE_BACKEND_URL") is not None:
        url = os.environ.get("MAIN_SEQUENCE_BACKEND_URL")

    return url
