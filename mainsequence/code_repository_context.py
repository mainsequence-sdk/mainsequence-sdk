from __future__ import annotations

import base64
import json
import os
import pathlib
import re
import subprocess
import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from typing import Any, Literal
from urllib.parse import urlsplit
from uuid import UUID

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


class CodeRepositoryEnvironmentContextRequiredError(CodeRepositoryContextError):
    """Raised only when an operation needs an unavailable Environment."""


class OrganizationEnvironmentContextError(CodeRepositoryContextError):
    """Raised when an Environment selection cannot be verified or conflicts with scope."""


class AuthenticatedContextChangedError(CodeRepositoryContextError):
    """Raised when an authenticated principal changes within a context."""


@dataclass(frozen=True, slots=True)
class DevelopmentEnvironmentSelection:
    """Explicit human development scope; access is verified on first resolution."""

    organization_environment_uid: str

    def __post_init__(self) -> None:
        try:
            value = str(UUID(str(self.organization_environment_uid).strip()))
        except (ValueError, TypeError, AttributeError) as exc:
            raise ValueError("organization_environment_uid must be a valid UUID.") from exc
        object.__setattr__(self, "organization_environment_uid", value)


@dataclass(frozen=True, slots=True)
class OrganizationEnvironmentContext:
    organization_environment_uid: str
    principal_uid: str
    source: Literal["authenticated_runtime", "explicit_development", "registered_branch"]
    process_id: int
    api_url: str


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
    status: CodeRepositoryContextStatus
    process_id: int
    code_repository_branch: Any | None
    detail: str = ""
    context_source: CodeRepositoryContextSource = "git"
    principal_uid: str | None = None
    api_url: str | None = None

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
    context: (
        CodeRepositoryContext
        | GitCodeRepositorySourceContext
        | OrganizationEnvironmentContext
        | None
    ) = None
    error: Exception | None = None


_STATE_CONDITION = threading.Condition(threading.RLock())
_STATE = _ContextState(process_id=os.getpid(), phase="uninitialized")
_AUTHENTICATED_RUNTIME_CONTEXT: tuple[int, dict[str, str]] | None = None
_SOURCE_STATE = _ContextState(process_id=os.getpid(), phase="uninitialized")
_ENVIRONMENT_STATE = _ContextState(process_id=os.getpid(), phase="uninitialized")
_DEVELOPMENT_ENVIRONMENT: DevelopmentEnvironmentSelection | None = None


def _clear_context_states() -> None:
    # Caller owns the condition, or is the sole thread after fork.
    global _STATE, _SOURCE_STATE, _ENVIRONMENT_STATE
    global _AUTHENTICATED_RUNTIME_CONTEXT, _DEVELOPMENT_ENVIRONMENT
    _STATE = _ContextState(process_id=os.getpid(), phase="uninitialized")
    _SOURCE_STATE = _ContextState(process_id=os.getpid(), phase="uninitialized")
    _ENVIRONMENT_STATE = _ContextState(process_id=os.getpid(), phase="uninitialized")
    _AUTHENTICATED_RUNTIME_CONTEXT = None
    _DEVELOPMENT_ENVIRONMENT = None


def _ensure_current_process() -> None:
    if _SOURCE_STATE.process_id != os.getpid():
        _clear_context_states()


def _after_fork() -> None:
    # A parent thread may have owned the old lock when fork occurred.
    global _STATE_CONDITION
    _STATE_CONDITION = threading.Condition(threading.RLock())
    _clear_context_states()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_after_fork)


def _runtime_auth_requested() -> bool:
    from mainsequence.client.utils import RuntimeCredentialAuthProvider, loaders

    if (
        os.getenv("MAINSEQUENCE_AUTH_MODE") or ""
    ).strip().lower() == "runtime_credential" or isinstance(
        loaders.provider, RuntimeCredentialAuthProvider
    ):
        return True
    # session_jwt is also used for forwarded human tokens. Token labels only
    # tighten local guards; they never supply identity, UIDs, or permission.
    # Runtime provenance is recorded only after the platform authenticates the
    # request and validates the Git source against the token's runtime target.
    token = (
        getattr(loaders.provider, "access_token", None)
        if loaders.provider is not None
        else os.getenv("MAINSEQUENCE_ACCESS_TOKEN")
    )
    try:
        payload = str(token or "").split(".")[1]
        claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
        if not isinstance(claims, dict):
            return False
        return claims.get("runtime_auth_mode") in (
            "runtime_credential",
            "session_jwt",
        ) or claims.get("scope") in (
            "job_run_runtime",
            "knative_runtime",
        )
    except (ValueError, IndexError, TypeError):
        return False


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
        _ensure_current_process()
        if _DEVELOPMENT_ENVIRONMENT is not None:
            raise OrganizationEnvironmentContextError(
                "A development Environment selection cannot be used in an authenticated runtime."
            )
        if (
            _ENVIRONMENT_STATE.context is not None
            and _ENVIRONMENT_STATE.context.source != "authenticated_runtime"
        ):
            raise AuthenticatedContextChangedError(
                "Cannot switch a development context to runtime authentication; start a fresh process."
            )
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
        if _STATE.phase == "failed" and _STATE.context is None:
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


def _resolve_git_source_context(
    code_repository_dir: pathlib.Path,
) -> GitCodeRepositorySourceContext:
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
    source: GitCodeRepositorySourceContext,
    code_repository_branch_context_loader: CodeRepositoryBranchContextLoader,
) -> CodeRepositoryContext:
    resolution = _load_code_repository_branch_context(code_repository_branch_context_loader, source)
    if resolution is None:
        return CodeRepositoryContext(
            source_context=source,
            code_repository_uid=None,
            code_repository_branch_uid=None,
            organization_environment_uid=None,
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
        raise CodeRepositoryContextError(
            "Git-resolved CodeRepositoryBranch has no CodeRepository UID."
        )
    if _normalized_value(code_repository_branch, "repository_branch") != source.repository_branch:
        raise CodeRepositoryContextError(
            "Git-resolved CodeRepositoryBranch has a mismatched branch."
        )

    return CodeRepositoryContext(
        source_context=source,
        code_repository_uid=code_repository_uid,
        code_repository_branch_uid=code_repository_branch_uid,
        organization_environment_uid=(
            _normalized_value(code_repository_branch, "organization_environment_uid") or None
        ),
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
        "organization_environment_uid": str(context.organization_environment_uid or "").strip(),
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


def get_git_source_context(
    *,
    code_repository_dir: str | pathlib.Path | None = None,
) -> GitCodeRepositorySourceContext:
    """Freeze actual local Git facts without importing authentication or making requests."""

    global _SOURCE_STATE
    directory = pathlib.Path(code_repository_dir or pathlib.Path.cwd()).resolve()
    with _STATE_CONDITION:
        _ensure_current_process()
        while _SOURCE_STATE.phase == "resolving":
            _STATE_CONDITION.wait()
        if _SOURCE_STATE.phase == "resolved":
            source = _SOURCE_STATE.context
            assert isinstance(source, GitCodeRepositorySourceContext)
            if code_repository_dir is not None and not directory.is_relative_to(
                source.repository_root
            ):
                raise CodeRepositorySourceContextDriftError(
                    "Requested directory is outside the frozen repository; reset the development context."
                )
            if code_repository_dir is not None and directory != source.repository_root:
                if _resolve_git_source_context(directory) != source:
                    raise CodeRepositorySourceContextDriftError(
                        "Requested directory does not match the frozen Git source."
                    )
            return source
        if _SOURCE_STATE.phase == "failed":
            assert _SOURCE_STATE.error is not None
            raise _SOURCE_STATE.error
        _SOURCE_STATE = _ContextState(os.getpid(), "resolving")
    try:
        source = _resolve_git_source_context(directory)
    except Exception as exc:
        with _STATE_CONDITION:
            _SOURCE_STATE = _ContextState(os.getpid(), "failed", error=exc)
            _STATE_CONDITION.notify_all()
        raise
    with _STATE_CONDITION:
        _SOURCE_STATE = _ContextState(os.getpid(), "resolved", context=source)
        _STATE_CONDITION.notify_all()
    return source


def validate_git_source_context() -> GitCodeRepositorySourceContext:
    """Validate the frozen source against the checkout, without platform enrichment."""

    source = get_git_source_context()
    if _resolve_git_source_context(source.repository_root) != source:
        raise CodeRepositorySourceContextDriftError(
            "Git repository, branch, or HEAD changed after source context was frozen."
        )
    return source


def get_code_repository_context(
    *,
    code_repository_dir: str | pathlib.Path | None = None,
    code_repository_uid: str | None = None,
    _code_repository_branch_context_loader: CodeRepositoryBranchContextLoader | None = None,
) -> CodeRepositoryContext:
    """Resolve and freeze Git-native context, then verify any runtime target."""

    global _STATE

    source = get_git_source_context(code_repository_dir=code_repository_dir)
    process_id = os.getpid()
    _exchange_authenticated_runtime_context_if_configured()
    with _STATE_CONDITION:
        _ensure_current_process()
        while _STATE.phase == "resolving":
            _STATE_CONDITION.wait()
        if _SOURCE_STATE.context is not source:
            raise CodeRepositoryContextError(
                "Source context was reset during platform resolution; retry in the new context."
            )
        if _STATE.phase == "failed":
            assert _STATE.error is not None
            raise _STATE.error
        previous = _STATE.context
        if (
            previous is not None
            and code_repository_uid
            and str(code_repository_uid).strip() != previous.code_repository_uid
        ):
            raise CodeRepositoryContextError(
                "Requested CodeRepository does not match the Git context locked for this run."
            )
        _STATE = _ContextState(process_id=process_id, phase="resolving", context=previous)

    try:
        authenticated_context = _authenticated_runtime_context_for_process()
        runtime_expected = _runtime_auth_requested() or authenticated_context is not None
        principal_uid, api_url = _authenticated_platform_identity()
        if previous is not None:
            assert isinstance(previous, CodeRepositoryContext)
            if (previous.principal_uid, previous.api_url) != (
                principal_uid,
                api_url,
            ) or previous.is_authenticated_runtime != runtime_expected:
                raise AuthenticatedContextChangedError(
                    "Authenticated principal, endpoint, or authentication mode changed; "
                    "reset the development context or start a fresh runtime process."
                )
            context = previous
        else:
            context = _build_code_repository_context(
                source=source,
                code_repository_branch_context_loader=(
                    _code_repository_branch_context_loader
                    or _default_code_repository_branch_context_loader
                ),
            )
            if authenticated_context is not None:
                context = _verify_authenticated_runtime_code_repository_context(
                    context,
                    authenticated_context,
                )
            if authenticated_context is None and runtime_expected:
                # The platform validates runtime-scoped tokens against their
                # authenticated target and immutable commit on this route.
                if context.status != "resolved":
                    raise CodeRepositoryContextError(
                        "Authenticated runtime has no matching Git target."
                    )
                context = replace(context, context_source="authenticated_runtime")
            context = replace(
                context,
                principal_uid=principal_uid,
                api_url=api_url,
            )
        if code_repository_uid and str(code_repository_uid).strip() != context.code_repository_uid:
            raise CodeRepositoryContextError(
                "Requested CodeRepository does not match the resolved Git repository."
            )
    except Exception as exc:
        with _STATE_CONDITION:
            _STATE = _ContextState(
                process_id=process_id, phase="failed", context=previous, error=exc
            )
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


def configure_development_environment(selection: DevelopmentEnvironmentSelection) -> None:
    """Select human development scope before the first Environment resolution attempt."""

    global _DEVELOPMENT_ENVIRONMENT
    if not isinstance(selection, DevelopmentEnvironmentSelection):
        raise TypeError("Use DevelopmentEnvironmentSelection to configure SDK Environment scope.")
    with _STATE_CONDITION:
        _ensure_current_process()
        if _runtime_auth_requested() or is_authenticated_runtime_code_repository_context():
            raise OrganizationEnvironmentContextError(
                "Development Environment selection is not allowed in an authenticated runtime."
            )
        if _ENVIRONMENT_STATE.phase != "uninitialized":
            raise OrganizationEnvironmentContextError(
                "Configure the Environment before resolving it; reset the development context to change scope."
            )
        _DEVELOPMENT_ENVIRONMENT = selection


def _authenticated_platform_identity() -> tuple[str, str]:
    from mainsequence.client.models_user import User

    user = User.get_authenticated_user_details()
    principal_uid = _normalized_value(user, "uid")
    if not principal_uid:
        raise CodeRepositoryContextError("The authenticated principal must have a public User UID.")
    return principal_uid, User._user_api_root()


def get_organization_environment_context(
    operation: str = "Environment-scoped operation",
) -> OrganizationEnvironmentContext:
    """Resolve one selected Environment through the shared SDK request pipeline.

    Identity is revalidated through users/me on each call. This keeps a token
    refresh for the same principal stable and detects account
    changes without trusting token contents. Resource authorization remains
    server-owned on every subsequent request.
    """

    global _ENVIRONMENT_STATE
    with _STATE_CONDITION:
        _ensure_current_process()
        while _ENVIRONMENT_STATE.phase == "resolving":
            _STATE_CONDITION.wait()
        if _ENVIRONMENT_STATE.phase == "failed":
            assert _ENVIRONMENT_STATE.error is not None
            raise _ENVIRONMENT_STATE.error
        previous = _ENVIRONMENT_STATE.context
        selection = _DEVELOPMENT_ENVIRONMENT
        _ENVIRONMENT_STATE = _ContextState(os.getpid(), "resolving", context=previous)
    try:
        context = get_code_repository_context()
        branch_environment = context.organization_environment_uid
        if context.is_authenticated_runtime:
            if selection is not None:
                raise OrganizationEnvironmentContextError(
                    "Development Environment selection is not allowed in an authenticated runtime."
                )
            environment_uid = branch_environment
            provenance = "authenticated_runtime"
        elif selection is not None:
            environment_uid = selection.organization_environment_uid
            provenance = "explicit_development"
            if branch_environment and branch_environment != environment_uid:
                raise OrganizationEnvironmentContextError(
                    "The selected Environment conflicts with the registered branch Environment."
                )
        else:
            environment_uid = branch_environment
            provenance = "registered_branch"
        if not environment_uid:
            raise CodeRepositoryEnvironmentContextRequiredError(
                f"{operation} requires an authorized Organization Environment. "
                "Configure DevelopmentEnvironmentSelection before use, or use a registered branch "
                "with an Environment. Git source discovery and unrelated SDK calls remain available."
            )
        principal_uid, api_url = (
            context.principal_uid,
            context.api_url,
        )
        if not principal_uid or not api_url:
            raise OrganizationEnvironmentContextError(
                "Environment operations require an authenticated User."
            )
        resolved = OrganizationEnvironmentContext(
            organization_environment_uid=environment_uid,
            principal_uid=principal_uid,
            source=provenance,
            process_id=os.getpid(),
            api_url=api_url,
        )
        if previous is not None:
            if previous != resolved:
                raise AuthenticatedContextChangedError(
                    "Authenticated principal or Environment changed; "
                    "reset the development context or start a fresh runtime process."
                )
            resolved = previous
    except Exception as exc:
        with _STATE_CONDITION:
            _ENVIRONMENT_STATE = _ContextState(os.getpid(), "failed", context=previous, error=exc)
            _STATE_CONDITION.notify_all()
        raise
    with _STATE_CONDITION:
        _ENVIRONMENT_STATE = _ContextState(os.getpid(), "resolved", context=resolved)
        _STATE_CONDITION.notify_all()
    return resolved


def resolve_organization_environment_uid(operation: str) -> str:
    """Return the verified runtime, explicit development, or branch-derived Environment."""

    return get_organization_environment_context(operation).organization_environment_uid


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


def retry_failed_context_resolution() -> None:
    """Clear failed optional lookups; retain frozen source and any resolved scope.

    This does not refresh a resolved unregistered result. To observe a newly
    registered branch, use a fresh development context or process.
    """

    global _STATE, _ENVIRONMENT_STATE
    with _STATE_CONDITION:
        _ensure_current_process()
        _require_idle_context()
        if _STATE.phase == "failed":
            previous = _STATE.context
            _STATE = _ContextState(
                os.getpid(),
                "resolved" if previous is not None else "uninitialized",
                context=previous,
            )
        if _ENVIRONMENT_STATE.phase == "failed":
            previous = _ENVIRONMENT_STATE.context
            _ENVIRONMENT_STATE = _ContextState(
                os.getpid(),
                "resolved" if previous is not None else "uninitialized",
                context=previous,
            )


def _require_idle_context() -> None:
    if any(state.phase == "resolving" for state in (_SOURCE_STATE, _STATE, _ENVIRONMENT_STATE)):
        raise CodeRepositoryContextError(
            "Cannot reset or retry while context resolution is in progress."
        )


def reset_development_context() -> None:
    """Start a new human development context; call only after stopping current work.

    Clears source, enrichment, scope, and the explicit selection, but retains
    SDK authentication. Deployed runtimes must start a fresh process instead.
    """

    with _STATE_CONDITION:
        _ensure_current_process()
        _require_idle_context()
        if _runtime_auth_requested() or is_authenticated_runtime_code_repository_context():
            raise CodeRepositoryContextError(
                "Authenticated runtimes require a fresh process, not a context reset."
            )
        _clear_context_states()


def _reset_code_repository_context() -> None:
    """Unconditionally clear process state for isolated SDK tests."""

    with _STATE_CONDITION:
        _clear_context_states()
        _STATE_CONDITION.notify_all()


__all__ = [
    "AuthenticatedContextChangedError",
    "DevelopmentEnvironmentSelection",
    "OrganizationEnvironmentContext",
    "OrganizationEnvironmentContextError",
    "configure_development_environment",
    "get_git_source_context",
    "get_organization_environment_context",
    "reset_development_context",
    "retry_failed_context_resolution",
    "validate_git_source_context",
    "GitCodeRepositorySourceContext",
    "CodeRepositoryBranchContextRequiredError",
    "CodeRepositoryEnvironmentContextRequiredError",
    "CodeRepositoryContext",
    "CodeRepositoryContextError",
    "CodeRepositorySourceContextDriftError",
    "get_code_repository_context",
    "is_authenticated_runtime_code_repository_context",
    "normalize_github_repository_binding_identity",
    "require_code_repository_branch_context",
    "resolve_organization_environment_uid",
    "resolve_code_repository_branch_uid",
    "scope_current_code_repository_branch_filters",
    "validate_code_repository_source_context",
]
