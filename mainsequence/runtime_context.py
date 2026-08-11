from __future__ import annotations

import os
import threading
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal

RUNTIME_PROJECT_CONTEXT_ENV = {
    "project_uid": "MAIN_SEQUENCE_PROJECT_UID",
    "project_branch_uid": "MAIN_SEQUENCE_PROJECT_BRANCH_UID",
    "repository_branch": "MAINSEQUENCE_REPOSITORY_BRANCH",
    "organization_project_environment_uid": (
        "MAIN_SEQUENCE_ORGANIZATION_PROJECT_ENVIRONMENT_UID"
    ),
}

BackendRuntimeContextSource = Literal[
    "job_run_startup",
    "runtime_credential_exchange",
]


class BackendRuntimeProjectContextError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class BackendRuntimeProjectContext:
    project_uid: str
    project_branch_uid: str
    repository_branch: str
    organization_project_environment_uid: str

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> BackendRuntimeProjectContext:
        normalized = {
            field: str(value.get(field) or "").strip()
            for field in RUNTIME_PROJECT_CONTEXT_ENV
        }
        missing = [field for field, item in normalized.items() if not item]
        if missing:
            raise BackendRuntimeProjectContextError(
                "runtime_project_context is incomplete: "
                + ", ".join(sorted(missing))
            )
        return cls(**normalized)

    def as_dict(self) -> dict[str, str]:
        return {
            field: str(getattr(self, field))
            for field in RUNTIME_PROJECT_CONTEXT_ENV
        }


@dataclass(frozen=True, slots=True)
class BackendRuntimeProjectContextState:
    verified: bool = False
    context: BackendRuntimeProjectContext | None = None
    source: BackendRuntimeContextSource | None = None


_STATE_LOCK = threading.RLock()
_STATE = BackendRuntimeProjectContextState()


def _get_backend_runtime_project_context_state() -> BackendRuntimeProjectContextState:
    with _STATE_LOCK:
        return _STATE


def _install_backend_runtime_project_context(
    payload: Mapping[str, Any],
    *,
    source: BackendRuntimeContextSource,
) -> BackendRuntimeProjectContextState:
    """Install context returned by an authenticated backend operation.

    Reserved process environment is a transport and diagnostics projection. It
    never activates deployed runtime mode on its own.
    """

    global _STATE

    if "runtime_project_context" not in payload:
        raise BackendRuntimeProjectContextError(
            "runtime_project_context is missing from the authenticated backend response."
        )

    raw_context = payload.get("runtime_project_context")
    if raw_context is None:
        context = None
    elif isinstance(raw_context, Mapping):
        context = BackendRuntimeProjectContext.from_mapping(raw_context)
    else:
        raise BackendRuntimeProjectContextError(
            "runtime_project_context must be an object or null."
        )

    with _STATE_LOCK:
        current_state = _STATE
        if current_state.verified and current_state.context != context:
            raise BackendRuntimeProjectContextError(
                "runtime_project_context does not match the context already "
                "verified for this process."
            )

        if context is not None:
            for field, environment_name in RUNTIME_PROJECT_CONTEXT_ENV.items():
                expected = str(getattr(context, field))
                current = str(os.environ.get(environment_name) or "").strip()
                if current and current != expected:
                    raise BackendRuntimeProjectContextError(
                        "runtime_project_context does not match the backend-reserved "
                        f"process environment for {field}."
                    )

            for field, environment_name in RUNTIME_PROJECT_CONTEXT_ENV.items():
                os.environ[environment_name] = str(getattr(context, field))
        else:
            for environment_name in RUNTIME_PROJECT_CONTEXT_ENV.values():
                os.environ.pop(environment_name, None)

        new_state = BackendRuntimeProjectContextState(
            verified=True,
            context=context,
            source=source,
        )
        _STATE = new_state
        return new_state


def _reset_backend_runtime_project_context() -> None:
    """Reset process-local verification state for isolated SDK tests."""

    global _STATE

    with _STATE_LOCK:
        previous_context = _STATE.context
        if previous_context is not None:
            for field, environment_name in RUNTIME_PROJECT_CONTEXT_ENV.items():
                expected = str(getattr(previous_context, field))
                if str(os.environ.get(environment_name) or "").strip() == expected:
                    os.environ.pop(environment_name, None)
        _STATE = BackendRuntimeProjectContextState()
