from __future__ import annotations

import os
from pathlib import Path


def local_data_path() -> Path:
    configured = (os.getenv("MAINSEQUENCE_LOCAL_DATA_PATH") or "").strip()
    if configured:
        return Path(configured).expanduser()
    return Path.home() / "meta_tables" / "data"


__all__ = ["local_data_path"]
