from __future__ import annotations

import logging
import logging.config
import os
from pathlib import Path

import requests
import structlog
from requests.structures import CaseInsensitiveDict
from structlog.dev import ConsoleRenderer

from .instrumentation import OTelJSONRenderer

logger = None
import inspect
import sys
from collections.abc import Mapping
from typing import Any

from structlog.contextvars import bind_contextvars, unbind_contextvars
from structlog.stdlib import BoundLogger


def ensure_dir(file_path):
    directory = os.path.dirname(file_path)
    Path(directory).mkdir(parents=True, exist_ok=True)


def add_structlog_event_to_record(logger, method_name, event_dict):
    record = event_dict.get("_record")
    if record is not None:
        # Remove '_record' to prevent circular reference
        event_dict.pop("_record", None)
        record.structlog_event = event_dict.copy()
    return event_dict


class CustomConsoleRenderer(ConsoleRenderer):
    def __call__(self, logger, name, event_dict):
        # Extract call site parameters
        lineno = event_dict.pop("lineno", None)
        filename = event_dict.pop("filename", None)
        func_name = event_dict.pop("func_name", None)
        # application_name = event_dict.pop('application_name', None)
        # update_hash=event_dict.pop('update_hash', "")
        # Call the parent renderer
        rendered = super().__call__(logger, name, event_dict)
        # Append the call site information to the rendered output
        if filename and lineno and func_name:
            rendered += f" (at {filename}:{lineno} in {func_name}())"
        elif filename and lineno:
            rendered += f" (at {filename}:{lineno})"
        return rendered


def _request_job_startup_state(*, timeout_s: float = 10.0) -> dict[str, Any]:
    """
    Fetch startup state from backend using current env vars (token, endpoint, command_id).
    Safe to call later after auth (when MAINSEQUENCE_TOKEN becomes available).
    """
    headers = CaseInsensitiveDict()
    headers["Content-Type"] = "application/json"
    headers["Authorization"] = "Token " + os.getenv("MAINSEQUENCE_TOKEN", "INVALID_TOKEN")

    endpoint = f'{os.getenv("TDAG_ENDPOINT")}/orm/api/pods/job/get_job_startup_state/'

    command_id = os.getenv("COMMAND_ID")
    params: dict[str, Any] = {}
    if command_id:
        params["command_id"] = command_id

    resp = requests.get(endpoint, headers=headers, params=params, timeout=timeout_s)
    if resp.status_code != 200:
        # don't crash logger setup; return empty-ish state
        try:
            body = resp.text
        except Exception:
            body = "<unreadable>"
        print(f"Got Status Code {resp.status_code} with response {body}")
        return {}

    try:
        return resp.json()
    except Exception:
        return {}


def _apply_additional_environment(startup_state: Mapping[str, Any]) -> None:
    extra = startup_state.get("additional_environment")
    if isinstance(extra, dict):
        for k, v in extra.items():
            os.environ[str(k)] = str(v)


def _build_backend_bindings(
    startup_state: Mapping[str, Any],
    *,
    command_id: str | None = None,
) -> dict[str, Any]:
    """
    Pure function: compute the keys you want to bind on the logger
    from the backend response + current env.
    """
    bindings: dict[str, Any] = {}

    # prefer backend payload, fall back to env if needed
    project_id = startup_state.get("project_id")
    if project_id is None and "project_id" in os.environ:
        project_id = os.environ.get("project_id")

    if project_id is not None:
        bindings["project_id"] = project_id
        bindings["data_source_id"] = startup_state.get("data_source_id")
        bindings["job_run_id"] = startup_state.get("job_run_id")

        if command_id is None:
            command_id = os.getenv("COMMAND_ID")
        bindings["command_id"] = int(command_id) if command_id else None
    else:
        # your existing behavior: bind job_run_id to user_id in local-ish mode
        if "user_id" in startup_state:
            bindings["job_run_id"] = startup_state.get("user_id")
        else:
            bindings["local_mode"] = "no_app"

    # drop None values
    return {k: v for k, v in bindings.items() if v is not None}


def _bind_runtime(logger_: BoundLogger, **bindings: Any) -> BoundLogger:
    """
    Bind to BOTH:
      - contextvars (so all loggers see it)
      - this logger instance (nice for dumping/restoring)
    """
    clean = {k: v for k, v in bindings.items() if v is not None}
    if not clean:
        return logger_

    # will appear on every log because you have merge_contextvars configured
    structlog.contextvars.bind_contextvars(**clean)
    return logger_.bind(**clean)


def apply_startup_state_bindings(
    logger_: BoundLogger,
    startup_state: Mapping[str, Any],
    *,
    command_id: str | None = None,
) -> BoundLogger:
    """
    Apply env + bindings derived from startup_state to the given logger.
    """
    _apply_additional_environment(startup_state)
    binds = _build_backend_bindings(startup_state, command_id=command_id)
    return _bind_runtime(logger_, **binds)


def build_application_logger(application_name: str = "ms-sdk", **metadata):
    """
    Create a logger that logs to console and file in JSON format.
    This routine also scafoldgs the interaction with the Main Sequence platform by setting environment variables at
    run time
    """

    # do initial request when on logger initialization
    command_id = os.getenv("COMMAND_ID")
    json_response = _request_job_startup_state()

    # set additional args from backend
    _apply_additional_environment(json_response)

    # Get logger path in home directory if no path is set in environemnt
    tdag_base_path = Path(os.getenv("TDAG_ROOT_PATH", Path.home() / ".tdag"))
    default_log_path = tdag_base_path / "logs" / "tdag.log"
    logger_file = os.getenv("LOGGER_FILE_PATH", str(default_log_path))
    logger_name = "tdag"

    # Define the timestamper and pre_chain processors
    timestamper = structlog.processors.TimeStamper(
        fmt="iso",
        utc=True,
    )
    pre_chain = [
        structlog.stdlib.add_log_level,
        structlog.stdlib.ExtraAdder(),
        timestamper,
    ]

    handlers = {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "colored",
            "level": os.getenv("LOG_LEVEL", "DEBUG"),
        },
    }
    if logger_file is not None:
        ensure_dir(logger_file)  # Ensure the directory for the log file exists

        handlers.update(
            {
                "file": {
                    "class": "concurrent_log_handler.ConcurrentRotatingFileHandler",
                    "formatter": "plain",
                    "level": os.getenv("LOG_LEVEL_FILE", "DEBUG"),
                    "filename": logger_file,
                    "mode": "a",
                    "delay": True,
                    "maxBytes": 5 * 1024 * 1024,  # Rotate after 5 MB
                    "backupCount": 5,  # Keep up to 5 backup files
                }
            }
        )

    logging_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "plain": {
                "()": structlog.stdlib.ProcessorFormatter,
                "processor": OTelJSONRenderer(),  # structlog.processors.JSONRenderer(),
                "foreign_pre_chain": pre_chain,
            },
            "colored": {
                "()": structlog.stdlib.ProcessorFormatter,
                "processor": CustomConsoleRenderer(colors=True),
                "foreign_pre_chain": pre_chain,
            },
        },
        "handlers": handlers,
        "loggers": {
            logger_name: {
                "handlers": list(handlers.keys()),
                "level": os.getenv("LOG_LEVEL_STDOUT", "INFO"),
                "propagate": False,
            },
        },
    }
    try:
        logging.config.dictConfig(logging_config)
    except Exception as e:
        raise e
    # Configure structlog
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,  # context that always appears in the logs
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.CallsiteParameterAdder(
                {
                    structlog.processors.CallsiteParameter.FILENAME,
                    structlog.processors.CallsiteParameter.FUNC_NAME,
                    structlog.processors.CallsiteParameter.LINENO,
                }
            ),
            timestamper,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,  # suggested to remove for pretty exceptions
            add_structlog_event_to_record,  # Add this processor before wrap_for_formatter
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.make_filtering_bound_logger(logging.DEBUG),
        cache_logger_on_first_use=True,
    )

    # Create the structlog logger and bind metadata
    logger = structlog.get_logger(logger_name)
    logger = logger.bind(application_name=application_name, **metadata)

    try:

        backend_binds = _build_backend_bindings(json_response, command_id=command_id)
        logger = _bind_runtime(logger, **backend_binds)



    except Exception:

        logger.exception("Logger could not be binded running in local mode")
        logger = logger.bind(local_mode="no_app", **metadata)

    logger = logger.bind()
    return logger

def refresh_application_logger_bindings(*, timeout_s: float = 10.0) -> BoundLogger:
    """
    Importable helper: call this AFTER the user authenticates / token exists.
    It re-fetches backend state and binds the same keys as build_application_logger().
    """
    global logger

    command_id = os.getenv("COMMAND_ID")
    json_response = _request_job_startup_state(timeout_s=timeout_s)

    # set additional args from backend
    _apply_additional_environment(json_response)

    backend_binds = _build_backend_bindings(json_response, command_id=command_id)

    # if somebody calls this super early
    if logger is None:
        logger = build_application_logger()
        return logger

    logger = _bind_runtime(logger, **backend_binds)
    return logger

def dump_structlog_bound_logger(logger: BoundLogger) -> dict[str, Any]:
    """
    Serialize a fully‑initialized structlog BoundLogger into a dict:
      - Global structlog config (as import paths)
      - Underlying stdlib.Logger name & level
      - Bound key/value context

    Returns:
        A dict that can be json-serialized and later reloaded to reconstruct
        the same structlog setup in another process.
    """

    # Helper: get module.QualName for function/class or for an instance's class
    def pathify(obj: Any) -> str:
        target = obj if inspect.isfunction(obj) or inspect.isclass(obj) else obj.__class__
        return f"{target.__module__}.{target.__qualname__}"

    # 1) Global structlog config
    cfg = structlog.get_config()
    structlog_config = {
        "processors": [pathify(p) for p in cfg["processors"]],
        "logger_factory": pathify(cfg["logger_factory"]),
        "wrapper_class": pathify(cfg["wrapper_class"]),
        "context_class": pathify(cfg["context_class"]),
        "cache_logger_on_first_use": cfg["cache_logger_on_first_use"],
    }

    # 2) Underlying stdlib.Logger info
    std = logger._logger
    logger_name = std.name
    logger_level = std.level

    # 3) Bound context
    bound_context = dict(logger._context or {})

    # 4) Assemble and return
    return {
        "structlog_config": structlog_config,
        "logger_name": logger_name,
        "logger_level": logger_level,
        "bound_context": bound_context,
    }


def load_structlog_bound_logger(dump: dict[str, Any]) -> BoundLogger:
    """
    Given the dict from dump_structlog_bound_logger(),
    return a BoundLogger with the same name, level, and context,
    but using the EXISTING global structlog configuration.
    """
    name = dump["logger_name"]
    level = dump["logger_level"]
    bound_context = dump["bound_context"]

    # 1) Grab the already‐configured logger
    base: BoundLogger = structlog.get_logger(name)

    # 2) (Optional) restore its stdlib level
    std = getattr(base, "_logger", None)
    if std is not None:
        std.setLevel(level)

    # 3) Re‐bind the original context
    return base.bind(**bound_context)


logger = build_application_logger()

# create a new system exection hook to also log terminating exceptions
original_hook = sys.excepthook


def set_local_run_app(local_model: str) -> BoundLogger:
    """
    Make `local_model` show up on every log event at runtime.

    Uses contextvars (global/per-async-task) + rebinds the module-level `logger`.
    """
    global logger

    # This will be merged into logs because you already have merge_contextvars configured.
    bind_contextvars(local_model=local_model)

    # Also bind on the module-level logger object (nice for future imports / dump_structlog_bound_logger).
    logger = logger.bind(local_model=local_model)
    return logger


def clear_local_run_app() -> None:
    """Remove the local_model key from the logging context."""
    unbind_contextvars("local_model")


def handle_exception(exc_type, exc_value, exc_traceback):
    """
    A custom exception handler that logs any uncaught exception.
    """
    if issubclass(exc_type, KeyboardInterrupt):
        # Let the user interrupt the program without a traceback
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return

    # Log the exception using our configured logger
    logger.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))
    # Also call the original hook to print the traceback to the console
    original_hook(exc_type, exc_value, exc_traceback)


sys.excepthook = handle_exception
