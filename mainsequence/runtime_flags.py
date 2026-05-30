from __future__ import annotations

import os


def is_running_in_pod() -> bool:
    """
    Return whether the current process is running inside a MainSequence pod.

    Pod runtime is identified by the job-run UID injected by the platform.
    Local workspaces may also set `MAIN_SEQUENCE_PROJECT_ID`, so that variable
    must not be used as the pod-runtime discriminator.
    """
    return bool((os.getenv("JOB_RUN_UID") or "").strip())
