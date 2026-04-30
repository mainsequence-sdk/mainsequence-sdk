from __future__ import annotations

import os

STANDARD_BACKEND_URL = "https://api.main-sequence.app"
CANONICAL_BACKEND_ENV = "MAINSEQUENCE_ENDPOINT"


def resolve_backend_endpoint(default: str | None = None) -> str:
    raw = (os.getenv(CANONICAL_BACKEND_ENV) or "").strip()
    if raw:
        return raw.rstrip("/")
    fallback = (default or STANDARD_BACKEND_URL).strip()
    return fallback.rstrip("/")
