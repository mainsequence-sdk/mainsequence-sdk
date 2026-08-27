from __future__ import annotations

import os
import pathlib
import re
import subprocess
import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Literal
from urllib.parse import urlsplit

ProjectRuntimeContextStatus = Literal[
    "resolved",
    "project_branch_not_registered",
]
ProjectRuntimeContextSource = Literal["git", "authenticated_runtime"]


class ProjectRuntimeContextError(RuntimeError):
    """Raised when the SDK cannot establish stable Git-native project context."""


class ProjectSourceContextDriftError(ProjectRuntimeContextError):
    """Raised when Git source identity changes after process initialization."""


class ProjectBranchContextRequiredError(ProjectRuntimeContextError):
    """Raised when an operation requires a registered current ProjectBranch."""


class ProjectEnvironmentContextRequiredError(ProjectBranchContextRequiredError):
    """Raised when the current ProjectBranch has no resolved Environment."""


class ProjectDataSourceContextRequiredError(ProjectRuntimeContextError):
    """Raised when project-derived data access has no usable branch DataSource."""


@dataclass(frozen=True, slots=True)
class GitProjectSourceContext:
    repository_root: pathlib.Path
    canonical_repository_identity: str
    repository_branch: str
    repository_ref: str
    commit_sha: str


ProjectBranchContextLoader = Callable[[GitProjectSourceContext], Any]


@dataclass(frozen=True, slots=True)
class ProjectRuntimeContext:
    source_context: GitProjectSourceContext | None
    project_uid: str | None
    project_branch_uid: str | None
    organization_environment_uid: str | None
    metatables_data_source: Any | None
    status: ProjectRuntimeContextStatus
    process_id: int
    project_branch: Any | None
    detail: str = ""
    context_source: ProjectRuntimeContextSource = "git"
    runtime_repository_branch: str | None = None

    @property
    def is_authenticated_runtime(self) -> bool:
        return self.context_source == "authenticated_runtime"

    def _require_git_source_context(self) -> GitProjectSourceContext:
        if self.source_context is None:
            raise ProjectRuntimeContextError(
                "Authenticated deployed runtime context has no local Git source."
            )
        return self.source_context

    @property
    def repository_root(self) -> pathlib.Path:
        return self._require_git_source_context().repository_root

    @property
    def canonical_repository_identity(self) -> str:
        return self._require_git_source_context().canonical_repository_identity

    @property
    def repository_branch(self) -> str:
        if self.source_context is not None:
            return self.source_context.repository_branch
        return str(self.runtime_repository_branch or "")

    @property
    def repository_ref(self) -> str:
        return self._require_git_source_context().repository_ref

    @property
    def commit_sha(self) -> str:
        return self._require_git_source_context().commit_sha


@dataclass(frozen=True, slots=True)
class _ContextState:
    process_id: int
    phase: Literal["uninitialized", "resolving", "resolved", "failed"]
    context: ProjectRuntimeContext | None = None
    error: Exception | None = None


_STATE_CONDITION = threading.Condition(threading.RLock())
_STATE = _ContextState(process_id=os.getpid(), phase="uninitialized")
_AUTHENTICATED_RUNTIME_CONTEXT: tuple[int, dict[str, str]] | None = None


def _normalize_authenticated_runtime_context(
    value: Mapping[str, Any],
) -> dict[str, str]:
    required_fields = (
        "project_uid",
        "project_branch_uid",
        "repository_branch",
        "organization_environment_uid",
    )
    normalized = {field: str(value.get(field) or "").strip() for field in required_fields}
    missing = [field for field, field_value in normalized.items() if not field_value]
    if missing:
        raise ProjectRuntimeContextError(
            "Authenticated runtime project context is incomplete; missing "
            + ", ".join(sorted(missing))
            + "."
        )
    return normalized


def _install_authenticated_runtime_project_context(value: Mapping[str, Any]) -> None:
    """Install only backend-authenticated deployed-runtime ProjectBranch context."""

    global _AUTHENTICATED_RUNTIME_CONTEXT, _STATE

    normalized = _normalize_authenticated_runtime_context(value)
    process_id = os.getpid()
    with _STATE_CONDITION:
        if _STATE.process_id != process_id:
            _STATE = _ContextState(process_id=process_id, phase="uninitialized")
        if _STATE.phase == "resolving":
            installed = _authenticated_runtime_context_for_process()
            if installed is not None and installed != normalized:
                raise ProjectSourceContextDriftError(
                    "Authenticated runtime project context changed while resolving."
                )
        if _STATE.phase == "resolved":
            assert _STATE.context is not None
            existing = _STATE.context
            if (
                not existing.is_authenticated_runtime
                or any(
                    str(getattr(existing, field) or "") != normalized[field]
                    for field in (
                        "project_uid",
                        "project_branch_uid",
                        "organization_environment_uid",
                    )
                )
                or existing.repository_branch != normalized["repository_branch"]
            ):
                raise ProjectSourceContextDriftError(
                    "Authenticated runtime project context changed after process initialization."
                )
        _AUTHENTICATED_RUNTIME_CONTEXT = (process_id, normalized)
        if _STATE.phase == "failed":
            _STATE = _ContextState(process_id=process_id, phase="uninitialized")
        _STATE_CONDITION.notify_all()


def _authenticated_runtime_context_for_process() -> dict[str, str] | None:
    installed = _AUTHENTICATED_RUNTIME_CONTEXT
    if installed is None or installed[0] != os.getpid():
        return None
    return dict(installed[1])


def is_authenticated_runtime_project_context() -> bool:
    if _authenticated_runtime_context_for_process() is not None:
        return True
    with _STATE_CONDITION:
        return bool(
            _STATE.process_id == os.getpid()
            and _STATE.phase == "resolved"
            and _STATE.context is not None
            and _STATE.context.is_authenticated_runtime
        )


def _exchange_authenticated_runtime_context_if_configured() -> None:
    if _authenticated_runtime_context_for_process() is not None:
        return
    if (os.getenv("MAINSEQUENCE_AUTH_MODE") or "").strip().lower() != "runtime_credential":
        return
    if not (os.getenv("MAINSEQUENCE_RUNTIME_CREDENTIAL_ID") or "").strip():
        return
    if not (os.getenv("MAINSEQUENCE_RUNTIME_CREDENTIAL_SECRET") or "").strip():
        return

    from mainsequence.client.utils import AuthError, loaders

    try:
        loaders.refresh_headers(force=True)
    except AuthError as exc:
        raise ProjectRuntimeContextError(
            "Could not exchange the deployed runtime credential for project context."
        ) from exc
    if _authenticated_runtime_context_for_process() is None:
        raise ProjectRuntimeContextError(
            "The authenticated runtime target has no ProjectBranch context."
        )


def _object_value(value: Any, field: str, default: Any = None) -> Any:
    if isinstance(value, Mapping):
        return value.get(field, default)
    return getattr(value, field, default)


def _normalized_value(value: Any, field: str) -> str:
    return str(_object_value(value, field, "") or "").strip()


def normalize_git_repository_identity(value: str) -> str:
    """Normalize a Git remote without retaining credentials or URL spelling."""

    raw = str(value or "").strip()
    if not raw or any(character in raw for character in "\r\n\x00"):
        raise ProjectRuntimeContextError("Git repository identity is missing or invalid.")
    if raw.startswith("-"):
        raise ProjectRuntimeContextError("Git repository identity is invalid.")

    def normalize_path(path: str) -> str:
        normalized = str(path or "").strip().strip("/")
        if not normalized or normalized in {".", ".."}:
            raise ProjectRuntimeContextError("Git repository path is empty.")
        if normalized.lower().endswith(".git"):
            normalized = normalized[:-4]
        return normalized

    def normalize_host(hostname: str, port: int | None) -> str:
        host = str(hostname or "").strip().lower()
        if not host:
            raise ProjectRuntimeContextError("Git repository URL has no hostname.")
        return host if port in (None, 22, 80, 443) else f"{host}:{port}"

    scp_match = re.fullmatch(
        r"(?:(?P<user>[^@/:\s]+)@)?(?P<host>[^:/\s]+):(?P<path>[^\s]+)",
        raw,
    )
    if scp_match is not None and "://" not in raw:
        host = normalize_host(scp_match.group("host"), None)
        path = normalize_path(scp_match.group("path"))
        return f"{host}/{path}"

    if "://" in raw:
        try:
            parsed = urlsplit(raw)
        except ValueError as exc:
            raise ProjectRuntimeContextError("Git repository URL is invalid.") from exc
        scheme = parsed.scheme.lower()
        if not scheme:
            raise ProjectRuntimeContextError("Git repository URL has no scheme.")
        if scheme == "file":
            return f"file:{normalize_path(parsed.path)}"
        host = normalize_host(parsed.hostname or "", parsed.port)
        path = normalize_path(parsed.path)
        return f"{host}/{path}"

    if "@" not in raw:
        return raw.rstrip("/")
    raise ProjectRuntimeContextError(f"Unsupported Git repository identity: {raw!r}.")


def _git_output(project_dir: pathlib.Path, *args: str) -> str:
    try:
        result = subprocess.run(
            ["git", *args],
            cwd=str(project_dir),
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ProjectRuntimeContextError(
            f"Could not inspect Git source context in {project_dir}: {exc}"
        ) from exc
    output = result.stdout.strip() if result.returncode == 0 else ""
    if not output:
        detail = result.stderr.strip() or "Git returned no value."
        raise ProjectRuntimeContextError(
            f"Could not resolve Git source context in {project_dir}: {detail}"
        )
    return output


def _resolve_git_source_context(project_dir: pathlib.Path) -> GitProjectSourceContext:
    repository_root = pathlib.Path(
        _git_output(project_dir, "rev-parse", "--show-toplevel")
    ).resolve()
    repository_ref = _git_output(repository_root, "symbolic-ref", "--quiet", "HEAD")
    if not repository_ref.startswith("refs/heads/"):
        raise ProjectRuntimeContextError(
            f"Git HEAD is not attached to a local branch: {repository_ref!r}."
        )
    repository_branch = repository_ref.removeprefix("refs/heads/").strip()
    if not repository_branch or repository_ref != f"refs/heads/{repository_branch}":
        raise ProjectRuntimeContextError("Git repository branch identity is invalid.")

    commit_sha = _git_output(repository_root, "rev-parse", "--verify", "HEAD^{commit}")
    if len(commit_sha) != 40 or re.fullmatch(r"[0-9a-f]+", commit_sha) is None:
        raise ProjectRuntimeContextError("Git HEAD is not a canonical full commit SHA.")

    remote_name = ""
    branch_remote = subprocess.run(
        ["git", "config", "--get", f"branch.{repository_branch}.remote"],
        cwd=str(repository_root),
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )
    if branch_remote.returncode == 0:
        remote_name = branch_remote.stdout.strip()
    if not remote_name or remote_name == ".":
        remotes = _git_output(repository_root, "remote").splitlines()
        remote_name = "origin" if "origin" in remotes else remotes[0] if len(remotes) == 1 else ""
    if not remote_name:
        raise ProjectRuntimeContextError("Git source context has no unambiguous repository remote.")
    remote_url = _git_output(repository_root, "remote", "get-url", remote_name)

    return GitProjectSourceContext(
        repository_root=repository_root,
        canonical_repository_identity=normalize_git_repository_identity(remote_url),
        repository_branch=repository_branch,
        repository_ref=repository_ref,
        commit_sha=commit_sha,
    )


def _default_project_branch_context_loader(source: GitProjectSourceContext) -> Any:
    from mainsequence.client.models_foundry import ProjectBranch

    return ProjectBranch.resolve_git_context(
        repository_identity=source.canonical_repository_identity,
        repository_branch=source.repository_branch,
        commit_sha=source.commit_sha,
    )


def _load_project_branch_context(
    loader: ProjectBranchContextLoader,
    source: GitProjectSourceContext,
) -> Any | None:
    try:
        return loader(source)
    except Exception as exc:
        if getattr(exc, "status_code", None) == 404:
            return None
        error_name = type(exc).__name__
        if error_name in {"AuthenticationError", "PermissionDeniedError"}:
            raise ProjectRuntimeContextError(
                "Git-native project resolution failed because SDK authentication or "
                f"authorization failed. Backend response: {exc}"
            ) from exc
        raise ProjectRuntimeContextError(
            f"Git-native project resolution backend lookup failed: {exc}"
        ) from exc


def _build_project_runtime_context(
    *,
    project_dir: pathlib.Path,
    project_branch_context_loader: ProjectBranchContextLoader,
) -> ProjectRuntimeContext:
    source = _resolve_git_source_context(project_dir)
    resolution = _load_project_branch_context(project_branch_context_loader, source)
    if resolution is None:
        return ProjectRuntimeContext(
            source_context=source,
            project_uid=None,
            project_branch_uid=None,
            organization_environment_uid=None,
            metatables_data_source=None,
            status="project_branch_not_registered",
            process_id=os.getpid(),
            project_branch=None,
            detail=(
                "No visible ProjectBranch matches Git repository "
                f"{source.canonical_repository_identity!r} and branch "
                f"{source.repository_branch!r}."
            ),
        )

    canonical_repository_identity = _normalized_value(
        resolution,
        "canonical_repository_identity",
    )
    repository_branch = _normalized_value(resolution, "repository_branch")
    repository_ref = _normalized_value(resolution, "repository_ref")
    commit_sha = _normalized_value(resolution, "commit_sha").lower()
    if (
        canonical_repository_identity != source.canonical_repository_identity
        or repository_branch != source.repository_branch
        or repository_ref != source.repository_ref
        or commit_sha != source.commit_sha
    ):
        raise ProjectRuntimeContextError(
            "Backend Git-context resolution does not match the frozen local Git source."
        )

    project_branch = _object_value(resolution, "project_branch")
    project_branch_uid = _normalized_value(project_branch, "uid")
    if not project_branch_uid:
        raise ProjectRuntimeContextError("Git-resolved ProjectBranch has no UID.")
    project_uid = _normalized_value(project_branch, "project_uid")
    if not project_uid:
        raise ProjectRuntimeContextError("Git-resolved ProjectBranch has no Project UID.")
    if _normalized_value(project_branch, "repository_branch") != source.repository_branch:
        raise ProjectRuntimeContextError("Git-resolved ProjectBranch has a mismatched branch.")

    return ProjectRuntimeContext(
        source_context=source,
        project_uid=project_uid,
        project_branch_uid=project_branch_uid,
        organization_environment_uid=(
            _normalized_value(project_branch, "organization_environment_uid") or None
        ),
        metatables_data_source=_object_value(project_branch, "metatables_data_source"),
        status="resolved",
        process_id=os.getpid(),
        project_branch=project_branch,
    )


def _build_authenticated_runtime_project_context(
    authenticated_context: Mapping[str, str],
) -> ProjectRuntimeContext:
    from mainsequence.client.models_foundry import ProjectBranch

    project_branch = ProjectBranch.get_by_uid(authenticated_context["project_branch_uid"])
    expected_fields = {
        "project_uid": authenticated_context["project_uid"],
        "project_branch_uid": authenticated_context["project_branch_uid"],
        "repository_branch": authenticated_context["repository_branch"],
        "organization_environment_uid": authenticated_context["organization_environment_uid"],
    }
    observed_fields = {
        "project_uid": _normalized_value(project_branch, "project_uid"),
        "project_branch_uid": _normalized_value(project_branch, "uid"),
        "repository_branch": _normalized_value(project_branch, "repository_branch"),
        "organization_environment_uid": _normalized_value(
            project_branch,
            "organization_environment_uid",
        ),
    }
    mismatched = sorted(
        field for field, expected in expected_fields.items() if observed_fields[field] != expected
    )
    if mismatched:
        raise ProjectRuntimeContextError(
            "Authenticated runtime project context does not match the target "
            "ProjectBranch: " + ", ".join(mismatched) + "."
        )

    return ProjectRuntimeContext(
        source_context=None,
        project_uid=expected_fields["project_uid"],
        project_branch_uid=expected_fields["project_branch_uid"],
        organization_environment_uid=expected_fields["organization_environment_uid"],
        metatables_data_source=_object_value(
            project_branch,
            "metatables_data_source",
        ),
        status="resolved",
        process_id=os.getpid(),
        project_branch=project_branch,
        context_source="authenticated_runtime",
        runtime_repository_branch=expected_fields["repository_branch"],
        detail=(
            "ProjectBranch context installed from an authenticated runtime credential exchange."
        ),
    )


def get_project_runtime_context(
    *,
    project_dir: str | pathlib.Path | None = None,
    project_uid: str | None = None,
    _project_branch_context_loader: ProjectBranchContextLoader | None = None,
) -> ProjectRuntimeContext:
    """Resolve and freeze authenticated runtime or local Git context once per process."""

    global _STATE

    process_id = os.getpid()
    _exchange_authenticated_runtime_context_if_configured()
    normalized_project_dir = pathlib.Path(project_dir or pathlib.Path.cwd()).resolve()
    with _STATE_CONDITION:
        if _STATE.process_id != process_id:
            _STATE = _ContextState(process_id=process_id, phase="uninitialized")
        while _STATE.phase == "resolving":
            _STATE_CONDITION.wait()
        if _STATE.phase == "resolved":
            assert _STATE.context is not None
            if project_uid and str(project_uid).strip() != _STATE.context.project_uid:
                raise ProjectRuntimeContextError(
                    "Requested Project does not match the Git context locked for this run."
                )
            return _STATE.context
        if _STATE.phase == "failed":
            assert _STATE.error is not None
            raise _STATE.error
        _STATE = _ContextState(process_id=process_id, phase="resolving")

    try:
        authenticated_context = _authenticated_runtime_context_for_process()
        if authenticated_context is not None:
            context = _build_authenticated_runtime_project_context(authenticated_context)
        else:
            context = _build_project_runtime_context(
                project_dir=normalized_project_dir,
                project_branch_context_loader=(
                    _project_branch_context_loader or _default_project_branch_context_loader
                ),
            )
        if project_uid and str(project_uid).strip() != context.project_uid:
            raise ProjectRuntimeContextError(
                "Requested Project does not match the resolved Git repository."
            )
    except Exception as exc:
        with _STATE_CONDITION:
            _STATE = _ContextState(process_id=process_id, phase="failed", error=exc)
            _STATE_CONDITION.notify_all()
        raise

    with _STATE_CONDITION:
        _STATE = _ContextState(process_id=process_id, phase="resolved", context=context)
        _STATE_CONDITION.notify_all()
    return context


def validate_project_source_context(
    *,
    context: ProjectRuntimeContext | None = None,
) -> ProjectRuntimeContext:
    """Fail if the current worktree no longer matches the frozen source context."""

    resolved = context or get_project_runtime_context()
    if resolved.is_authenticated_runtime:
        return resolved
    observed = _resolve_git_source_context(resolved.repository_root)
    if observed != resolved.source_context:
        raise ProjectSourceContextDriftError(
            "Git repository, branch, or HEAD changed after project context was frozen."
        )
    return resolved


def require_project_branch_context(
    operation: str,
    *,
    context: ProjectRuntimeContext | None = None,
) -> ProjectRuntimeContext:
    resolved = context or get_project_runtime_context()
    if resolved.status != "resolved" or not resolved.project_branch_uid:
        raise ProjectBranchContextRequiredError(
            f"{operation} requires a registered active ProjectBranch. {resolved.detail}"
        )
    return resolved


def resolve_project_branch_uid(operation: str, supplied_uid: Any = None) -> str:
    context = require_project_branch_context(operation)
    resolved_uid = str(context.project_branch_uid)
    if supplied_uid not in (None, ""):
        normalized_supplied_uid = str(
            _object_value(supplied_uid, "uid", supplied_uid) or ""
        ).strip()
        if normalized_supplied_uid != resolved_uid:
            raise ProjectBranchContextRequiredError(
                f"{operation} cannot override the ProjectBranch locked for this run."
            )
    return resolved_uid


def resolve_organization_environment_uid(operation: str) -> str:
    """Return the Environment UID derived from the process-frozen ProjectBranch."""

    context = require_project_branch_context(operation)
    environment_uid = str(context.organization_environment_uid or "").strip()
    if not environment_uid:
        raise ProjectEnvironmentContextRequiredError(
            f"{operation} requires an Organization Environment resolved from "
            f"ProjectBranch {context.project_branch_uid!r}, but none was returned."
        )
    return environment_uid


def scope_current_project_branch_filters(
    operation: str,
    filters: Mapping[str, Any],
    *,
    field_name: str = "project_branch_uid",
) -> dict[str, Any]:
    context = require_project_branch_context(operation)
    project_branch_uid = str(context.project_branch_uid)
    scoped = dict(filters)
    exact_value = scoped.get(field_name)
    in_field_name = f"{field_name}__in"
    in_value = scoped.get(in_field_name)
    if context.is_authenticated_runtime:
        if exact_value not in (None, "") or in_value not in (None, ""):
            raise ProjectBranchContextRequiredError(
                f"{operation} cannot select a ProjectBranch in an authenticated runtime."
            )
        return scoped
    if exact_value not in (None, ""):
        normalized = str(_object_value(exact_value, "uid", exact_value) or "").strip()
        if normalized != project_branch_uid:
            raise ProjectBranchContextRequiredError(
                f"{operation} cannot query outside the ProjectBranch locked for this run."
            )
    if in_value not in (None, ""):
        raw_values = in_value if isinstance(in_value, list | tuple | set) else [in_value]
        normalized_values = {
            str(_object_value(value, "uid", value) or "").strip() for value in raw_values
        }
        if normalized_values != {project_branch_uid}:
            raise ProjectBranchContextRequiredError(
                f"{operation} cannot query outside the ProjectBranch locked for this run."
            )
    if exact_value in (None, "") and in_value in (None, ""):
        scoped[field_name] = project_branch_uid
    return scoped


def require_project_metatables_data_source(
    operation: str,
    *,
    context: ProjectRuntimeContext | None = None,
) -> Any:
    resolved = require_project_branch_context(operation, context=context)
    data_source = resolved.metatables_data_source
    if data_source is None:
        raise ProjectDataSourceContextRequiredError(
            f"{operation} requires ProjectBranch.metatables_data_source, but "
            f"ProjectBranch {resolved.project_branch_uid!r} has none configured."
        )
    status = str(_object_value(data_source, "status", "") or "")
    if status != "AVAILABLE":
        raise ProjectDataSourceContextRequiredError(
            f"{operation} requires an AVAILABLE ProjectBranch MetaTables DataSource; "
            f"got status {status or 'unknown'!r}."
        )
    return data_source


def _reset_project_runtime_context() -> None:
    """Reset call-once state for isolated SDK tests."""

    global _AUTHENTICATED_RUNTIME_CONTEXT, _STATE
    with _STATE_CONDITION:
        _AUTHENTICATED_RUNTIME_CONTEXT = None
        _STATE = _ContextState(process_id=os.getpid(), phase="uninitialized")
        _STATE_CONDITION.notify_all()


__all__ = [
    "GitProjectSourceContext",
    "ProjectBranchContextRequiredError",
    "ProjectDataSourceContextRequiredError",
    "ProjectEnvironmentContextRequiredError",
    "ProjectRuntimeContext",
    "ProjectRuntimeContextError",
    "ProjectSourceContextDriftError",
    "get_project_runtime_context",
    "is_authenticated_runtime_project_context",
    "normalize_git_repository_identity",
    "require_project_branch_context",
    "require_project_metatables_data_source",
    "resolve_organization_environment_uid",
    "resolve_project_branch_uid",
    "scope_current_project_branch_filters",
    "validate_project_source_context",
]
