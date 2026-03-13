from __future__ import annotations

import os
import pathlib
import re


def _set_if_missing(key: str, value: str | None) -> None:
    if value and not os.environ.get(key):
        os.environ[key] = value


def _read_local_env_values(env_path: pathlib.Path) -> dict[str, str]:
    if not env_path.is_file():
        return {}

    try:
        content = env_path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return {}

    values: dict[str, str] = {}
    for key in ("TDAG_ENDPOINT", "MAINSEQUENCE_ENDPOINT", "MAIN_SEQUENCE_PROJECT_ID"):
        match = re.search(rf"(?m)^{re.escape(key)}=(.+?)\s*$", content)
        if match:
            values[key] = match.group(1).strip()
    return values


def prime_runtime_env() -> None:
    """
    Best-effort bootstrap so CLI/SDK imports can discover runtime config without
    requiring the user to source project `.env` manually.
    """
    local_values = _read_local_env_values(pathlib.Path.cwd() / ".env")

    _set_if_missing("TDAG_ENDPOINT", local_values.get("TDAG_ENDPOINT"))
    _set_if_missing(
        "MAINSEQUENCE_ENDPOINT",
        local_values.get("MAINSEQUENCE_ENDPOINT") or local_values.get("TDAG_ENDPOINT"),
    )
    _set_if_missing("MAIN_SEQUENCE_PROJECT_ID", local_values.get("MAIN_SEQUENCE_PROJECT_ID"))

    try:
        from .cli import config as cli_config
    except Exception:
        return

    try:
        backend = cli_config.backend_url()
    except Exception:
        backend = ""

    _set_if_missing("TDAG_ENDPOINT", backend)
    _set_if_missing("MAINSEQUENCE_ENDPOINT", backend)

    try:
        tokens = cli_config.get_tokens()
    except Exception:
        tokens = {}

    _set_if_missing("MAINSEQUENCE_ACCESS_TOKEN", (tokens.get("access") or "").strip())
    _set_if_missing("MAINSEQUENCE_REFRESH_TOKEN", (tokens.get("refresh") or "").strip())
