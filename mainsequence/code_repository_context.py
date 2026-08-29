from __future__ import annotations

import os
import pathlib
import re
import subprocess
import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from typing import Any, Literal
from urllib.parse import urlsplit

CodeRepositoryContextStatus = Literal[
    "resolved",
    "code_repository_branch_not_registered",
]
CodeRepositoryContextSource = Literal["git", "authenticated_runtime"]


class CodeRepositoryContextError(RuntimeError):
    """Raised when the SDK cannot establish stable Git-native CodeRepository context."""


class CodeRepositorySourceContextDriftError(CodeRepositoryContextError):
    """Raised when Git source identity changes after process initialization."""


class CodeRepositoryBranchContextRequiredError(CodeRepositoryContextError):
    """Raised when an operation requires a registered current CodeRepositoryBranch."""


class CodeRepositoryEnvironmentContextRequiredError(CodeRepositoryBranchContextRequiredError):
    """Raised when the current CodeRepositoryBranch has no resolved Environment."""


class CodeRepositoryDataSourceContextRequiredError(CodeRepositoryContextError):
    """Raised when branch-derived data access has no usable DataSource."""


@dataclass(frozen=True, slots=True)
class GitCodeRepositorySourceContext:
    repository_root: pathlib.Path
    canonical_repository_identity: str
    repository_branch: str
    repository_ref: str
    commit_sha: str


CodeRepositoryBranchContextLoader = Callable[[GitCodeRepositorySourceContext], Any]


@dataclass(frozen=True, slots=True)
class CodeRepositoryContext:
    source_context: GitCodeRepositorySourceContext
    code_repository_uid: str | None
    code_repository_branch_uid: str | None
    organization_environment_uid: str | None
    metatables_data_source: Any | None
    status: CodeRepositoryContextStatus
    process_id: int
    code_repository_branch: Any | None
    detail: str = ""
    context_source: CodeRepositoryContextSource = "git"

    @property
    def is_authenticated_runtime(self) -> bool:
        return self.context_source == "authenticated_runtime"

    @property
    def repository_root(self) -> pathlib.Path:
        return self.source_context.repository_root

    @property
    def canonical_repository_identity(self) -> str:
        return self.source_context.canonical_repository_identity

    @property
    def repository_branch(self) -> str:
        return self.source_context.repository_branch

    @property
    def repository_ref(self) -> str:
        return self.source_context.repository_ref

    @property
    def commit_sha(self) -> str:
        return self.source_context.commit_sha


@dataclass(frozen=True, slots=True)
class _ContextState:
    process_id: int
    phase: Literal["uninitialized", "resolving", "resolved", "failed"]
    context: CodeRepositoryContext | None = None
    error: Exception | None = None


_STATE_CONDITION = threading.Condition(threading.RLock())
_STATE = _ContextState(process_id=os.getpid(), phase="uninitialized")
_AUTHENTICATED_RUNTIME_CONTEXT: tuple[int, dict[str, str]] | None = None


def _normalize_authenticated_runtime_context(
    value: Mapping[str, Any],
) -> dict[str, str]:
    required_fields = (
        "code_repository_uid",
        "code_repository_branch_uid",
        "repository_branch",
        "organization_environment_uid",
    )
    normalized = {field: str(value.get(field) or "").strip() for field in required_fields}
    missing = [field for field, field_value in normalized.items() if not field_value]
    if missing:
        raise CodeRepositoryContextError(
            "Authenticated runtime CodeRepository context is incomplete; missing "
            + ", ".join(sorted(missing))
            + "."
        )
    return normalized


def _install_authenticated_runtime_code_repository_context(value: Mapping[str, Any]) -> None:
    """Install only backend-authenticated deployed-runtime CodeRepositoryBranch context."""

    global _AUTHENTICATED_RUNTIME_CONTEXT, _STATE

    normalized = _normalize_authenticated_runtime_context(value)
    process_id = os.getpid()
    with _STATE_CONDITION:
        if _STATE.process_id != process_id:
            _STATE = _ContextState(process_id=process_id, phase="uninitialized")
        if _STATE.phase == "resolving":
            installed = _authenticated_runtime_context_for_process()
            if installed is not None and installed != normalized:
                raise CodeRepositorySourceContextDriftError(
                    "Authenticated runtime CodeRepository context changed while resolving."
                )
        if _STATE.phase == "resolved":
            assert _STATE.context is not None
            existing = _STATE.context
            if (
                not existing.is_authenticated_runtime
                or any(
                    str(getattr(existing, field) or "") != normalized[field]
                    for field in (
                        "code_repository_uid",
                        "code_repository_branch_uid",
                        "organization_environment_uid",
                    )
                )
                or existing.repository_branch != normalized["repository_branch"]
            ):
                raise CodeRepositorySourceContextDriftError(
                    "Authenticated runtime CodeRepository context changed after process initialization."
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


def is_authenticated_runtime_code_repository_context() -> bool:
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
        raise CodeRepositoryContextError(
            "Could not exchange the deployed runtime credential for CodeRepository context."
        ) from exc
    if _authenticated_runtime_context_for_process() is None:
        raise CodeRepositoryContextError(
            "The authenticated runtime target has no CodeRepositoryBranch context."
        )


def _object_value(value: Any, field: str, default: Any = None) -> Any:
    if isinstance(value, Mapping):
        return value.get(field, default)
    return getattr(value, field, default)


def _normalized_value(value: Any, field: str) -> str:
    return str(_object_value(value, field, "") or "").strip()


def normalize_github_repository_binding_identity(value: str) -> str:
    """Normalize a Git remote without retaining credentials or URL spelling."""

    raw = str(value or "").strip()
    if not raw or any(character in raw for character in "\r\n\x00"):
        raise CodeRepositoryContextError("Git repository identity is missing or invalid.")
    if raw.startswith("-"):
        raise CodeRepositoryContextError("Git repository identity is invalid.")

    def normalize_path(path: str) -> str:
        normalized = str(path or "").strip().strip("/")
        if not normalized or normalized in {".", ".."}:
            raise CodeRepositoryContextError("Git repository path is empty.")
        if normalized.lower().endswith(".git"):
            normalized = normalized[:-4]
        return normalized

    def normalize_host(hostname: str, port: int | None) -> str:
        host = str(hostname or "").strip().lower()
        if not host:
            raise CodeRepositoryContextError("Git repository URL has no hostname.")
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
            raise CodeRepositoryContextError("Git repository URL is invalid.") from exc
        scheme = parsed.scheme.lower()
        if not scheme:
            raise CodeRepositoryContextError("Git repository URL has no scheme.")
        if scheme == "file":
            return f"file:{normalize_path(parsed.path)}"
        host = normalize_host(parsed.hostname or "", parsed.port)
        path = normalize_path(parsed.path)
        return f"{host}/{path}"

    if "@" not in raw:
        return raw.rstrip("/")
    raise CodeRepositoryContextError(f"Unsupported Git repository identity: {raw!r}.")


def _git_output(code_repository_dir: pathlib.Path, *args: str) -> str:
    try:
        result = subprocess.run(
            ["git", *args],
            cwd=str(code_repository_dir),
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise CodeRepositoryContextError(
            f"Could not inspect Git source context in {code_repository_dir}: {exc}"
        ) from exc
    output = result.stdout.strip() if result.returncode == 0 else ""
    if not output:
        detail = result.stderr.strip() or "Git returned no value."
        raise CodeRepositoryContextError(
            f"Could not resolve Git source context in {code_repository_dir}: {detail}"
        )
    return output


def _resolve_git_source_context(code_repository_dir: pathlib.Path) -> GitCodeRepositorySourceContext:
    repository_root = pathlib.Path(
        _git_output(code_repository_dir, "rev-parse", "--show-toplevel")
    ).resolve()
    repository_ref = _git_output(repository_root, "symbolic-ref", "--quiet", "HEAD")
    if not repository_ref.startswith("refs/heads/"):
        raise CodeRepositoryContextError(
            f"Git HEAD is not attached to a local branch: {repository_ref!r}."
        )
    repository_branch = repository_ref.removeprefix("refs/heads/").strip()
    if not repository_branch or repository_ref != f"refs/heads/{repository_branch}":
        raise CodeRepositoryContextError("Git repository branch identity is invalid.")

    commit_sha = _git_output(repository_root, "rev-parse", "--verify", "HEAD^{commit}")
    if len(commit_sha) != 40 or re.fullmatch(r"[0-9a-f]+", commit_sha) is None:
        raise CodeRepositoryContextError("Git HEAD is not a canonical full commit SHA.")

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
        raise CodeRepositoryContextError("Git source context has no unambiguous repository remote.")
    remote_url = _git_output(repository_root, "remote", "get-url", remote_name)

    return GitCodeRepositorySourceContext(
        repository_root=repository_root,
        canonical_repository_identity=normalize_github_repository_binding_identity(remote_url),
        repository_branch=repository_branch,
        repository_ref=repository_ref,
        commit_sha=commit_sha,
    )


def _default_code_repository_branch_context_loader(source: GitCodeRepositorySourceContext) -> Any:
    from mainsequence.client.models_foundry import CodeRepositoryBranch

    return CodeRepositoryBranch.resolve_git_context(
        repository_identity=source.canonical_repository_identity,
        repository_branch=source.repository_branch,
        commit_sha=source.commit_sha,
    )


def _load_code_repository_branch_context(
    loader: CodeRepositoryBranchContextLoader,
    source: GitCodeRepositorySourceContext,
) -> Any | None:
    try:
        return loader(source)
    except Exception as exc:
        if getattr(exc, "status_code", None) == 404:
            return None
        error_name = type(exc).__name__
        if error_name in {"AuthenticationError", "PermissionDeniedError"}:
            raise CodeRepositoryContextError(
                "Git-native CodeRepository resolution failed because SDK authentication or "
                f"authorization failed. Backend response: {exc}"
            ) from exc
        raise CodeRepositoryContextError(
            f"Git-native CodeRepository resolution backend lookup failed: {exc}"
        ) from exc


def _build_code_repository_context(
    *,
    code_repository_dir: pathlib.Path,
    code_repository_branch_context_loader: CodeRepositoryBranchContextLoader,
) -> CodeRepositoryContext:
    source = _resolve_git_source_context(code_repository_dir)
    resolution = _load_code_repository_branch_context(code_repository_branch_context_loader, source)
    if resolution is None:
        return CodeRepositoryContext(
            source_context=source,
            code_repository_uid=None,
            code_repository_branch_uid=None,
            organization_environment_uid=None,
            metatables_data_source=None,
            status="code_repository_branch_not_registered",
            process_id=os.getpid(),
            code_repository_branch=None,
            detail=(
                "No visible CodeRepositoryBranch matches Git repository "
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
        raise CodeRepositoryContextError(
            "Backend Git-context resolution does not match the frozen local Git source."
        )

    code_repository_branch = _object_value(resolution, "code_repository_branch")
    code_repository_branch_uid = _normalized_value(code_repository_branch, "uid")
    if not code_repository_branch_uid:
        raise CodeRepositoryContextError("Git-resolved CodeRepositoryBranch has no UID.")
    code_repository_uid = _normalized_value(code_repository_branch, "code_repository_uid")
    if not code_repository_uid:
        raise CodeRepositoryContextError("Git-resolved CodeRepositoryBranch has no CodeRepository UID.")
    if _normalized_value(code_repository_branch, "repository_branch") != source.repository_branch:
        raise CodeRepositoryContextError("Git-resolved CodeRepositoryBranch has a mismatched branch.")

    return CodeRepositoryContext(
        source_context=source,
        code_repository_uid=code_repository_uid,
        code_repository_branch_uid=code_repository_branch_uid,
        organization_environment_uid=(
            _normalized_value(code_repository_branch, "organization_environment_uid") or None
        ),
        metatables_data_source=_object_value(code_repository_branch, "metatables_data_source"),
        status="resolved",
        process_id=os.getpid(),
        code_repository_branch=code_repository_branch,
    )


def _verify_authenticated_runtime_code_repository_context(
    context: CodeRepositoryContext,
    authenticated_context: Mapping[str, str],
) -> CodeRepositoryContext:
    expected_fields = {
        "code_repository_uid": authenticated_context["code_repository_uid"],
        "code_repository_branch_uid": authenticated_context["code_repository_branch_uid"],
        "repository_branch": authenticated_context["repository_branch"],
        "organization_environment_uid": authenticated_context["organization_environment_uid"],
    }
    observed_fields = {
        "code_repository_uid": str(context.code_repository_uid or "").strip(),
        "code_repository_branch_uid": str(context.code_repository_branch_uid or "").strip(),
        "repository_branch": context.repository_branch,
        "organization_environment_uid": str(
            context.organization_environment_uid or ""
        ).strip(),
    }
    mismatched = sorted(
        field for field, expected in expected_fields.items() if observed_fields[field] != expected
    )
    if mismatched:
        raise CodeRepositoryContextError(
            "Git-resolved CodeRepositoryBranch does not match the authenticated runtime "
            "target: " + ", ".join(mismatched) + "."
        )

    return replace(
        context,
        context_source="authenticated_runtime",
    )


def get_code_repository_context(
    *,
    code_repository_dir: str | pathlib.Path | None = None,
    code_repository_uid: str | None = None,
    _code_repository_branch_context_loader: CodeRepositoryBranchContextLoader | None = None,
) -> CodeRepositoryContext:
    """Resolve and freeze Git-native context, then verify any runtime target."""

    global _STATE

    process_id = os.getpid()
    _exchange_authenticated_runtime_context_if_configured()
    normalized_code_repository_dir = pathlib.Path(code_repository_dir or pathlib.Path.cwd()).resolve()
    with _STATE_CONDITION:
        if _STATE.process_id != process_id:
            _STATE = _ContextState(process_id=process_id, phase="uninitialized")
        while _STATE.phase == "resolving":
            _STATE_CONDITION.wait()
        if _STATE.phase == "resolved":
            assert _STATE.context is not None
            if code_repository_uid and str(code_repository_uid).strip() != _STATE.context.code_repository_uid:
                raise CodeRepositoryContextError(
                    "Requested CodeRepository does not match the Git context locked for this run."
                )
            return _STATE.context
        if _STATE.phase == "failed":
            assert _STATE.error is not None
            raise _STATE.error
        _STATE = _ContextState(process_id=process_id, phase="resolving")

    try:
        authenticated_context = _authenticated_runtime_context_for_process()
        context = _build_code_repository_context(
            code_repository_dir=normalized_code_repository_dir,
            code_repository_branch_context_loader=(
                _code_repository_branch_context_loader or _default_code_repository_branch_context_loader
            ),
        )
        if authenticated_context is not None:
            context = _verify_authenticated_runtime_code_repository_context(
                context,
                authenticated_context,
            )
        if code_repository_uid and str(code_repository_uid).strip() != context.code_repository_uid:
            raise CodeRepositoryContextError(
                "Requested CodeRepository does not match the resolved Git repository."
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


def validate_code_repository_source_context(
    *,
    context: CodeRepositoryContext | None = None,
) -> CodeRepositoryContext:
    """Fail if the current worktree no longer matches the frozen source context."""

    resolved = context or get_code_repository_context()
    observed = _resolve_git_source_context(resolved.repository_root)
    if observed != resolved.source_context:
        raise CodeRepositorySourceContextDriftError(
            "Git repository, branch, or HEAD changed after CodeRepository context was frozen."
        )
    return resolved


def require_code_repository_branch_context(
    operation: str,
    *,
    context: CodeRepositoryContext | None = None,
) -> CodeRepositoryContext:
    resolved = context or get_code_repository_context()
    if resolved.status != "resolved" or not resolved.code_repository_branch_uid:
        raise CodeRepositoryBranchContextRequiredError(
            f"{operation} requires a registered active CodeRepositoryBranch. {resolved.detail}"
        )
    return resolved


def resolve_code_repository_branch_uid(operation: str, supplied_uid: Any = None) -> str:
    context = require_code_repository_branch_context(operation)
    resolved_uid = str(context.code_repository_branch_uid)
    if supplied_uid not in (None, ""):
        normalized_supplied_uid = str(
            _object_value(supplied_uid, "uid", supplied_uid) or ""
        ).strip()
        if normalized_supplied_uid != resolved_uid:
            raise CodeRepositoryBranchContextRequiredError(
                f"{operation} cannot override the CodeRepositoryBranch locked for this run."
            )
    return resolved_uid


def resolve_organization_environment_uid(operation: str) -> str:
    """Return the Environment UID derived from the process-frozen CodeRepositoryBranch."""

    context = require_code_repository_branch_context(operation)
    environment_uid = str(context.organization_environment_uid or "").strip()
    if not environment_uid:
        raise CodeRepositoryEnvironmentContextRequiredError(
            f"{operation} requires an Organization Environment resolved from "
            f"CodeRepositoryBranch {context.code_repository_branch_uid!r}, but none was returned."
        )
    return environment_uid


def scope_current_code_repository_branch_filters(
    operation: str,
    filters: Mapping[str, Any],
    *,
    field_name: str = "code_repository_branch_uid",
) -> dict[str, Any]:
    context = require_code_repository_branch_context(operation)
    code_repository_branch_uid = str(context.code_repository_branch_uid)
    scoped = dict(filters)
    exact_value = scoped.get(field_name)
    in_field_name = f"{field_name}__in"
    in_value = scoped.get(in_field_name)
    if context.is_authenticated_runtime:
        if exact_value not in (None, "") or in_value not in (None, ""):
            raise CodeRepositoryBranchContextRequiredError(
                f"{operation} cannot select a CodeRepositoryBranch in an authenticated runtime."
            )
        return scoped
    if exact_value not in (None, ""):
        normalized = str(_object_value(exact_value, "uid", exact_value) or "").strip()
        if normalized != code_repository_branch_uid:
            raise CodeRepositoryBranchContextRequiredError(
                f"{operation} cannot query outside the CodeRepositoryBranch locked for this run."
            )
    if in_value not in (None, ""):
        raw_values = in_value if isinstance(in_value, list | tuple | set) else [in_value]
        normalized_values = {
            str(_object_value(value, "uid", value) or "").strip() for value in raw_values
        }
        if normalized_values != {code_repository_branch_uid}:
            raise CodeRepositoryBranchContextRequiredError(
                f"{operation} cannot query outside the CodeRepositoryBranch locked for this run."
            )
    if exact_value in (None, "") and in_value in (None, ""):
        scoped[field_name] = code_repository_branch_uid
    return scoped


def require_code_repository_metatables_data_source(
    operation: str,
    *,
    context: CodeRepositoryContext | None = None,
) -> Any:
    resolved = require_code_repository_branch_context(operation, context=context)
    data_source = resolved.metatables_data_source
    if data_source is None:
        raise CodeRepositoryDataSourceContextRequiredError(
            f"{operation} requires CodeRepositoryBranch.metatables_data_source, but "
            f"CodeRepositoryBranch {resolved.code_repository_branch_uid!r} has none configured."
        )
    status = str(_object_value(data_source, "status", "") or "")
    if status != "AVAILABLE":
        raise CodeRepositoryDataSourceContextRequiredError(
            f"{operation} requires an AVAILABLE CodeRepositoryBranch MetaTables DataSource; "
            f"got status {status or 'unknown'!r}."
        )
    return data_source


def _reset_code_repository_context() -> None:
    """Reset call-once state for isolated SDK tests."""

    global _AUTHENTICATED_RUNTIME_CONTEXT, _STATE
    with _STATE_CONDITION:
        _AUTHENTICATED_RUNTIME_CONTEXT = None
        _STATE = _ContextState(process_id=os.getpid(), phase="uninitialized")
        _STATE_CONDITION.notify_all()


__all__ = [
    "GitCodeRepositorySourceContext",
    "CodeRepositoryBranchContextRequiredError",
    "CodeRepositoryDataSourceContextRequiredError",
    "CodeRepositoryEnvironmentContextRequiredError",
    "CodeRepositoryContext",
    "CodeRepositoryContextError",
    "CodeRepositorySourceContextDriftError",
    "get_code_repository_context",
    "is_authenticated_runtime_code_repository_context",
    "normalize_github_repository_binding_identity",
    "require_code_repository_branch_context",
    "require_code_repository_metatables_data_source",
    "resolve_organization_environment_uid",
    "resolve_code_repository_branch_uid",
    "scope_current_code_repository_branch_filters",
    "validate_code_repository_source_context",
]
