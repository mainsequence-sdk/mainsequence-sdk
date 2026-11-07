# ruff: noqa: E402
from .bootstrap import prime_runtime_env
from .instrumentation.utils import _attach_environment_context

# Restore CLI credentials before any client module freezes its endpoint/provider.
prime_runtime_env()
# A Job run's TRACEPARENT and BAGGAGE, before its first log line or call (ADR 0040).
_attach_environment_context()

from .logconf import (
    logger as logger,
)
from .logconf import (
    refresh_application_logger_bindings as refresh_application_logger_bindings,
)
from .logconf import (
    set_local_run_app as set_local_run_app,
)
