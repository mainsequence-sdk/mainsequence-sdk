# settings.py
from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace

ENV_PREFIX = "MSI"


DATA_BACKEND = os.getenv(f"{ENV_PREFIX}_DATA_BACKEND","mainsequence")
data = SimpleNamespace(backend=DATA_BACKEND)
