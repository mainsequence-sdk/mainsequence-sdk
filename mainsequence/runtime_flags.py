from __future__ import annotations

import os


def is_running_in_pod() -> bool:
    """
    Return whether the current process is running inside a MainSequence pod.

    For now, pod runtime is identified by the presence of the legacy
    `MAINSEQUENCE_TOKEN` environment variable.
    """
    return bool((os.getenv("MAINSEQUENCE_TOKEN") or "").strip())
