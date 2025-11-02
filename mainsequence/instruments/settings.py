# settings.py
from __future__ import annotations

import os
from types import SimpleNamespace
from mainsequence.client import Constant as _C
ENV_PREFIX = "MSI"


DATA_BACKEND = os.getenv(f"{ENV_PREFIX}_DATA_BACKEND", "mainsequence")
data = SimpleNamespace(backend=DATA_BACKEND)



