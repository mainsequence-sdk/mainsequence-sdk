"""Local CodeRepository development commands retained by the SDK."""

from __future__ import annotations

import dataclasses
import importlib
import os
import pathlib
import platform
import re
import shutil
import subprocess
import sys
from typing import Any

import click
import typer
from packaging.specifiers import InvalidSpecifier, SpecifierSet
from packaging.version import InvalidVersion, Version

from ..code_repository_context import (
    CodeRepositoryContextError,
    get_code_repository_context,
    require_code_repository_branch_context,
)
from ..code_repository_skills import (
    CodeRepositorySkillAssemblyError,
    install_dual_source_code_repository_skills,
)
from . import config as cfg
from .api import (
    ApiError,
    NotLoggedIn,
    authed,
    fetch_platform_code_repository_skill_catalog,
    get_current_user_profile,
)
from .local_ops import (
    ensure_uv_installed,
    ensure_venv,
    git_origin,
    normalize_path,
    run_cmd,
    run_uv,
    uv_export_requirements,
    uv_preview_patch_version,
    uv_project_version,
)
from .ssh_utils import (
    ensure_key_for_repo,
    git_ssh_environment,
    open_signed_terminal,
    repository_ssh_key_paths,
    require_ssh_git_origin,
    start_agent_and_add_key,
    verify_git_push_access,
    verify_git_remote_access,
    verify_git_remote_tag_absent,
    verify_git_tag_absent,
)
from .ui import error, info, print_kv, print_table, status, success, warn

code_repository = typer.Typer(help="CodeRepository local development operations")

AGENTS_MD_MANAGED_BLOCK_START_PREFIX = "<!-- mainsequence-agent-scaffold:start"
AGENTS_MD_MANAGED_BLOCK_END = "<!-- mainsequence-agent-scaffold:end -->"
AGENTS_MD_MANAGED_BLOCK_SCHEMA = "1"
AGENTS_MD_MANAGED_BLOCK_START_LINE_RE = re.compile(
    rf"(?m)^[ \t]*{re.escape(AGENTS_MD_MANAGED_BLOCK_START_PREFIX)}\b[^\n]*-->[ \t]*$"
)
AGENTS_MD_MANAGED_BLOCK_END_LINE_RE = re.compile(
    rf"(?m)^[ \t]*{re.escape(AGENTS_MD_MANAGED_BLOCK_END)}[ \t]*$"
)


def _response_payload(response, *, operation: str) -> dict[str, Any]:
    if not response.ok:
        detail = (response.text or "").strip()
        suffix = f": {detail}" if detail else ""
        raise ApiError(f"{operation} failed ({response.status_code}){suffix}")
    try:
        payload = response.json()
    except Exception as exc:
        raise ApiError(f"{operation} returned invalid JSON.") from exc
    if not isinstance(payload, dict):
        raise ApiError(f"{operation} returned an unexpected payload.")
    return payload


def resolve_code_repository(code_repository_uid: str) -> dict[str, Any]:
    normalized_uid = str(code_repository_uid or "").strip()
    if not normalized_uid:
        raise ApiError("CodeRepository UID is required.")
    return _response_payload(
        authed("GET", f"/api/v1/code-repositories/{normalized_uid}/"),
        operation="CodeRepository fetch",
    )


def get_code_repository_branch(branch_uid: str) -> dict[str, Any]:
    normalized_uid = str(branch_uid or "").strip()
    if not normalized_uid:
        raise ApiError("CodeRepositoryBranch UID is required.")
    return _response_payload(
        authed("GET", f"/api/v1/code-repository-branches/{normalized_uid}/"),
        operation="CodeRepositoryBranch fetch",
    )


def get_code_repository_repository(repository_uid: str) -> dict[str, Any]:
    normalized_uid = str(repository_uid or "").strip()
    if not normalized_uid:
        raise ApiError("GitHubRepositoryBinding UID is required.")
    return _response_payload(
        authed("GET", f"/api/v1/github-repository-bindings/{normalized_uid}/"),
        operation="GitHubRepositoryBinding fetch",
    )


def add_deploy_key(code_repository_uid: str, key_title: str, public_key: str) -> None:
    normalized_uid = str(code_repository_uid or "").strip()
    response = authed(
        "POST",
        f"/api/v1/code-repositories/{normalized_uid}/add-deploy-key/",
        {"key_title": key_title, "public_key": public_key},
    )
    if not response.ok:
        detail = (response.text or "").strip()
        raise ApiError(
            f"CodeRepository deploy-key registration failed ({response.status_code})"
            + (f": {detail}" if detail else "")
        )


def render_code_repository_branch_default_redeployment_tag(
    branch_uid: str,
    *,
    version: str,
) -> str:
    normalized_uid = str(branch_uid or "").strip()
    normalized_version = str(version or "").strip()
    if not normalized_uid:
        raise ApiError("CodeRepositoryBranch UID is required.")
    if not normalized_version:
        raise ApiError("CodeRepository version is required.")
    payload = _response_payload(
        authed(
            "POST",
            f"/api/v1/code-repository-branches/{normalized_uid}/default-redeployment-tag/",
            {"version": normalized_version},
        ),
        operation="Default redeployment tag rendering",
    )
    if str(payload.get("version") or "").strip() != normalized_version:
        raise ApiError("Default redeployment tag rendering returned another version.")
    tag_name = str(payload.get("tag_name") or "").strip()
    if not tag_name:
        raise ApiError("Default redeployment tag rendering returned no tag name.")
    return tag_name


def safe_slug(value: str) -> str:
    slug = re.sub(r"[^a-z0-9-_]+", "-", (value or "code-repository").lower()).strip("-")
    return slug[:64] or "code-repository"


def repo_name_from_git_url(url: str | None) -> str | None:
    if not url:
        return None
    candidate = re.sub(r"[?#].*$", "", url.strip())
    leaf = candidate.split("/")[-1] if "/" in candidate else candidate
    if leaf.lower().endswith(".git"):
        leaf = leaf[:-4]
    return re.sub(r"[^A-Za-z0-9._-]+", "-", leaf)


def _emit_json(_payload: object) -> bool:
    return False


def _copy_clipboard(txt: str) -> bool:
    """
    Cross-platform clipboard copy (same spirit as extension).
    Returns True on best-effort success.
    """
    try:
        import shutil

        # Windows
        if sys.platform == "win32":
            for ps in ("powershell.exe", "pwsh.exe"):
                if shutil.which(ps):
                    p = subprocess.run(
                        [
                            ps,
                            "-NoProfile",
                            "-Command",
                            "Set-Clipboard -Value ([Console]::In.ReadToEnd())",
                        ],
                        input=txt,
                        text=True,
                        capture_output=True,
                    )
                    if p.returncode == 0:
                        return True
            if shutil.which("clip.exe"):
                p = subprocess.run(["clip.exe"], input=txt, text=True, capture_output=True)
                return p.returncode == 0
            return False

        # macOS
        if sys.platform == "darwin":
            p = subprocess.run(["pbcopy"], input=txt, text=True, capture_output=True)
            return p.returncode == 0

        # WSL -> Windows clipboard
        if os.environ.get("WSL_DISTRO_NAME") and shutil.which("clip.exe"):
            p = subprocess.run(["clip.exe"], input=txt, text=True, capture_output=True)
            return p.returncode == 0

        wayland = os.environ.get("WAYLAND_DISPLAY")
        x11 = os.environ.get("DISPLAY")

        if wayland and shutil.which("wl-copy"):
            ok1 = (
                subprocess.run(["wl-copy"], input=txt, text=True, capture_output=True).returncode
                == 0
            )
            subprocess.run(["wl-copy", "--primary"], input=txt, text=True, capture_output=True)
            return ok1

        if x11:
            if shutil.which("xclip"):
                for sel in ("clipboard", "primary"):
                    p = subprocess.Popen(
                        ["xclip", "-selection", sel, "-in", "-quiet"],
                        stdin=subprocess.PIPE,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                        text=True,
                        close_fds=True,
                        start_new_session=True,
                    )
                    assert p.stdin is not None
                    p.stdin.write(txt)
                    p.stdin.close()
                return True
            if shutil.which("xsel"):
                for args in (["--clipboard", "--input"], ["--primary", "--input"]):
                    p = subprocess.Popen(
                        ["xsel", *args],
                        stdin=subprocess.PIPE,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                        text=True,
                        close_fds=True,
                        start_new_session=True,
                    )
                    assert p.stdin is not None
                    p.stdin.write(txt)
                    p.stdin.close()
                return True

        return False
    except Exception:
        return False


def _code_repositories_root(base_dir: str, org_slug: str) -> pathlib.Path:
    p = pathlib.Path(base_dir).expanduser()
    return p / org_slug / "code-repositories"


def _org_slug_from_profile() -> str:
    prof = get_current_user_profile()
    name = prof.get("organization") or "default"
    if isinstance(name, dict):
        name = name.get("name") or name.get("slug") or "default"
    if not isinstance(name, str):
        name = str(name or "default")
    return re.sub(r"[^a-z0-9-_]+", "-", name.lower()).strip("-") or "default"


def _resolve_code_repository_repository_ssh_url(code_repository: dict) -> str:
    repository_uid = str(code_repository.get("github_repository_binding_uid") or "").strip()
    if not repository_uid:
        raise ApiError("The CodeRepository has no linked GitHubRepositoryBinding.")

    repository = get_code_repository_repository(repository_uid)
    repository_ssh_url = str(repository.get("git_ssh_url") or "").strip()
    if not repository_ssh_url:
        raise ApiError(f"GitHubRepositoryBinding {repository_uid} has no SSH clone URL.")
    return repository_ssh_url


def _ensure_code_repository_repository_ssh_access(
    *,
    origin: str,
    code_repository_ref: str | None,
    verify_access,
) -> tuple[pathlib.Path, str, dict[str, str]]:
    expected_key_path, expected_public_key_path = repository_ssh_key_paths(origin)
    keypair_existed = expected_key_path.is_file() and expected_public_key_path.is_file()
    key_path, _public_key_path, public_key = ensure_key_for_repo(origin)
    env = git_ssh_environment(key_path)

    def register_key() -> None:
        normalized_code_repository_ref = str(code_repository_ref or "").strip()
        if not normalized_code_repository_ref:
            raise ApiError(
                "The repository SSH key is not authorized and the current Git repository "
                "could not be resolved to a platform CodeRepository for deploy-key registration."
            )
        key_title = str(platform.node() or "").strip()
        if not key_title or "\n" in key_title or "\r" in key_title:
            raise ApiError("Local hostname must contain one non-empty line.")
        try:
            add_deploy_key(normalized_code_repository_ref, key_title, public_key)
        except Exception as exc:
            raise ApiError(f"CodeRepository deploy-key registration failed: {exc}") from exc

    if not keypair_existed:
        register_key()
    else:
        try:
            verify_access(env)
            return key_path, public_key, env
        except RuntimeError:
            register_key()

    try:
        verify_access(env)
    except RuntimeError as exc:
        raise ApiError(str(exc)) from exc
    return key_path, public_key, env


def _code_repository_identity_value(code_repository: dict) -> str:
    """Return the logical CodeRepository public UID."""
    return str(code_repository.get("uid") or "").strip()


def _require_login() -> dict:
    """
    Ensure user is logged in by calling get_current_user_profile().

    Returns:
        profile dict

    Raises typer.Exit(1) with user-friendly message on failure.
    """
    try:
        prof = get_current_user_profile()
        if not prof or not prof.get("username"):
            raise NotLoggedIn("Not logged in.")
        return prof
    except NotLoggedIn as e:
        error("Not logged in. Run: mainsequence login")
        raise typer.Exit(1) from e
    except ApiError as e:
        error("Not logged in. Run: mainsequence login")
        raise typer.Exit(1) from e


def _runtime_credential_mode_enabled() -> bool:
    return (os.environ.get("MAINSEQUENCE_AUTH_MODE") or "").strip().lower() == "runtime_credential"


def _exchange_runtime_credential_for_cli_login(backend_url: str) -> str:
    try:
        from mainsequence.client.utils import RuntimeCredentialAuthProvider
    except Exception as exc:
        raise ApiError(f"Runtime credential auth is unavailable: {exc}") from exc

    token_url = f"{backend_url.rstrip('/')}/api/v1/runtime-credentials/token/"
    try:
        RuntimeCredentialAuthProvider(token_url=token_url).refresh(force=True)
    except Exception as exc:
        raise ApiError(f"Runtime credential exchange failed: {exc}") from exc

    access = (os.environ.get("MAINSEQUENCE_ACCESS_TOKEN") or "").strip()
    if not access:
        raise ApiError("Runtime credential exchange did not produce MAINSEQUENCE_ACCESS_TOKEN.")
    return access


def _resolve_code_repository_dir(code_repository_id: str | None, path: str | None) -> pathlib.Path:
    """
    Resolve the containing Git worktree from an explicit path or the current directory.

    Raises:
        typer.Exit(1) on failure.
    """
    candidate = normalize_path(path) if path else pathlib.Path.cwd()
    if not candidate.exists():
        error(f"Folder does not exist: {candidate}")
        raise typer.Exit(1)
    if (candidate / ".git").exists():
        p = candidate.resolve()
    else:
        try:
            result = subprocess.run(
                ["git", "rev-parse", "--show-toplevel"],
                cwd=str(candidate),
                capture_output=True,
                text=True,
                timeout=5,
                check=False,
            )
        except (OSError, subprocess.SubprocessError) as exc:
            error("Run this command inside a Git CodeRepository checkout or pass --path.")
            raise typer.Exit(1) from exc
        root = result.stdout.strip() if result.returncode == 0 else ""
        if not root:
            error("Run this command inside a Git CodeRepository checkout or pass --path.")
            raise typer.Exit(1)
        p = pathlib.Path(root).resolve()
    if not p.is_dir():
        error(f"Git CodeRepository root is missing: {p}")
        raise typer.Exit(1)
    if code_repository_id:
        try:
            get_code_repository_context(
                code_repository_uid=code_repository_id, code_repository_dir=p
            )
        except CodeRepositoryContextError as exc:
            error(f"CodeRepository UID assertion failed: {exc}")
            raise typer.Exit(1) from exc
    return p


def _resolve_code_repository_branch(
    code_repository: dict,
    *,
    repository_branch: str | None = None,
    prompt_if_ambiguous: bool = False,
) -> dict:
    branches = [
        item for item in list(code_repository.get("branches") or []) if isinstance(item, dict)
    ]
    if not branches:
        raise ApiError("This CodeRepository has no CodeRepositoryBranches.")

    branch_name = (repository_branch or "").strip()
    if branch_name:
        matches = [
            item for item in branches if str(item.get("repository_branch") or "") == branch_name
        ]
        if len(matches) == 1:
            return get_code_repository_branch(str(matches[0]["uid"]))
        raise ApiError(
            f"Git branch {branch_name!r} is not registered as a CodeRepositoryBranch for this CodeRepository."
        )

    if prompt_if_ambiguous:
        names = [str(item.get("repository_branch") or "") for item in branches]
        selected = typer.prompt(
            "Repository branch",
            type=click.Choice(names, case_sensitive=True),
        )
        match = next(item for item in branches if item.get("repository_branch") == selected)
        return get_code_repository_branch(str(match["uid"]))

    raise ApiError(
        "No repository branch was selected. Run the command from the CodeRepository Git checkout "
        "or pass an explicit repository branch."
    )


def _resolve_git_code_repository_branch_context(
    code_repository_ref: str | None = None,
    *,
    code_repository_dir: pathlib.Path | None = None,
) -> tuple[str, str]:
    """Read the process-lifetime context for a current-CodeRepository CLI workflow."""
    try:
        context = get_code_repository_context(
            code_repository_uid=code_repository_ref,
            code_repository_dir=code_repository_dir,
        )
        context = require_code_repository_branch_context(
            "This current-CodeRepository CLI operation",
            context=context,
        )
    except CodeRepositoryContextError as exc:
        raise ApiError(str(exc)) from exc
    return context.repository_branch, str(context.code_repository_branch_uid)


def _code_repository_agent_scaffold_bundle_dir(code_repository_dir: pathlib.Path) -> pathlib.Path:
    """
    Resolve the `agent_scaffold` bundle from the target code repository's local `.venv`.
    """
    try:
        vp = ensure_venv(code_repository_dir)
    except Exception as exc:
        error(f"Could not access the target code repository's .venv: {exc}")
        raise typer.Exit(1) from exc

    lookup = subprocess.run(
        [
            str(vp.python),
            "-c",
            (
                "import sys, agent_scaffold; "
                "paths=list(getattr(agent_scaffold, '__path__', [])); "
                "sys.stdout.write(paths[0] if paths else '')"
            ),
        ],
        cwd=str(code_repository_dir),
        capture_output=True,
        text=True,
    )
    if lookup.returncode != 0:
        detail = (lookup.stderr or lookup.stdout or "").strip()
        message = (
            "Could not locate agent_scaffold in the target code repository's .venv. "
            "Run `mainsequence code-repository build-local-venv` or "
            "`mainsequence code-repository update-sdk --path .` first."
        )
        if detail:
            message = f"{message} ({detail})"
        error(message)
        raise typer.Exit(1)

    bundle_dir = pathlib.Path((lookup.stdout or "").strip()).resolve()
    if not bundle_dir.exists() or not bundle_dir.is_dir():
        error(f"Target code repository .venv resolved an invalid agent_scaffold path: {bundle_dir}")
        raise typer.Exit(1)
    return bundle_dir


def _code_repository_installed_package_version(
    code_repository_dir: pathlib.Path, package_name: str
) -> str:
    """
    Resolve an installed package version from the target code repository's local `.venv`.
    """
    try:
        vp = ensure_venv(code_repository_dir)
    except Exception as exc:
        error(f"Could not access the target code repository's .venv: {exc}")
        raise typer.Exit(1) from exc

    lookup = subprocess.run(
        [
            str(vp.python),
            "-c",
            (
                "import importlib.metadata, sys; "
                "sys.stdout.write(importlib.metadata.version(sys.argv[1]))"
            ),
            package_name,
        ],
        cwd=str(code_repository_dir),
        capture_output=True,
        text=True,
    )
    if lookup.returncode != 0:
        detail = (lookup.stderr or lookup.stdout or "").strip()
        message = (
            f"Could not resolve installed {package_name!r} version from the target "
            "CodeRepository's .venv. Run `mainsequence code-repository update-sdk --path .` first."
        )
        if detail:
            message = f"{message} ({detail})"
        error(message)
        raise typer.Exit(1)

    resolved_version = (lookup.stdout or "").strip()
    if not resolved_version:
        error(
            f"Could not resolve installed {package_name!r} version from the target code repository's .venv."
        )
        raise typer.Exit(1)
    return resolved_version


def _mainsequence_source_checkout_root() -> pathlib.Path | None:
    repo_root = pathlib.Path(__file__).resolve().parents[2]
    if (repo_root / "pyproject.toml").is_file() and (
        repo_root / "agent_scaffold" / "skills"
    ).is_dir():
        return repo_root
    return None


def _installed_agent_scaffold_bundle_dir() -> pathlib.Path:
    """
    Resolve the `agent_scaffold` bundle for the currently running CLI install.
    """
    candidates: list[pathlib.Path] = []
    import_error: Exception | None = None
    try:
        module = importlib.import_module("agent_scaffold")
    except Exception as exc:
        import_error = exc
    else:
        paths = [pathlib.Path(p).resolve() for p in getattr(module, "__path__", [])]
        candidates.extend(p for p in paths if p.exists() and p.is_dir())

    sibling_candidate = pathlib.Path(__file__).resolve().parents[2] / "agent_scaffold"
    if sibling_candidate.exists() and sibling_candidate.is_dir():
        candidates.append(sibling_candidate.resolve())

    deduped_candidates: list[pathlib.Path] = []
    seen: set[pathlib.Path] = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        deduped_candidates.append(candidate)

    candidates = deduped_candidates
    if not candidates:
        if import_error is not None:
            error(f"Could not import installed agent_scaffold bundle: {import_error}")
            raise typer.Exit(1) from import_error
        error("Installed agent_scaffold bundle path could not be resolved.")
        raise typer.Exit(1)
    return candidates[0]


@dataclasses.dataclass(frozen=True)
class AgentsMdManagedBlockUpdate:
    action: str
    changed: bool


def _installed_agent_scaffold_agents_md_file() -> pathlib.Path:
    source = _installed_agent_scaffold_bundle_dir() / "AGENTS.md"
    if not source.is_file():
        error(f"Installed agent_scaffold bundle is missing {source.name}: {source}")
        raise typer.Exit(1)
    return source


def _agents_md_managed_block_line_matches(
    source_content: str,
) -> tuple[list[re.Match[str]], list[re.Match[str]]]:
    start_matches = list(AGENTS_MD_MANAGED_BLOCK_START_LINE_RE.finditer(source_content))
    end_matches = list(AGENTS_MD_MANAGED_BLOCK_END_LINE_RE.finditer(source_content))
    return start_matches, end_matches


def _extract_agents_md_managed_block(source_content: str) -> str:
    start_matches, end_matches = _agents_md_managed_block_line_matches(source_content)
    if len(start_matches) != 1 or len(end_matches) != 1:
        raise ValueError(
            "Installed agent_scaffold AGENTS.md must contain exactly one Main Sequence "
            "managed block."
        )

    start_match = start_matches[0]
    end_match = end_matches[0]
    if end_match.start() < start_match.start():
        raise ValueError(
            "Installed agent_scaffold AGENTS.md contains malformed Main Sequence managed "
            "block markers."
        )

    return source_content[start_match.start() : end_match.end()]


def _load_installed_agents_md_template() -> tuple[pathlib.Path, str, str]:
    source = _installed_agent_scaffold_agents_md_file()
    content = source.read_text(encoding="utf-8")
    managed_block = _extract_agents_md_managed_block(content)
    return source, content, managed_block


def _apply_agents_md_managed_block(
    content: str,
    bootstrap_content: str,
    managed_block: str,
) -> tuple[str, str]:
    start_matches, end_matches = _agents_md_managed_block_line_matches(content)

    if len(start_matches) > 1 or len(end_matches) > 1:
        raise ValueError(
            "AGENTS.md contains multiple Main Sequence managed block markers; resolve it manually."
        )
    if len(start_matches) != len(end_matches):
        raise ValueError(
            "AGENTS.md contains malformed Main Sequence managed block markers; resolve it manually."
        )

    if not start_matches:
        return bootstrap_content, "replaced"

    start_match = start_matches[0]
    end_match = end_matches[0]
    if end_match.start() < start_match.start():
        raise ValueError(
            "AGENTS.md contains malformed Main Sequence managed block markers; resolve it manually."
        )

    updated = (
        f"{content[: start_match.start()]}{managed_block.rstrip()}{content[end_match.end() :]}"
    )
    action = "unchanged" if updated == content else "updated"
    return updated, action


def _update_agents_md_managed_block_file(
    destination: pathlib.Path,
    bootstrap_content: str,
    managed_block: str,
) -> AgentsMdManagedBlockUpdate:
    destination.parent.mkdir(parents=True, exist_ok=True)

    if not destination.exists():
        destination.write_text(bootstrap_content, encoding="utf-8")
        return AgentsMdManagedBlockUpdate(action="created", changed=True)

    original = destination.read_text(encoding="utf-8")
    updated, action = _apply_agents_md_managed_block(original, bootstrap_content, managed_block)
    changed = updated != original
    if changed:
        destination.write_text(updated, encoding="utf-8")
    return AgentsMdManagedBlockUpdate(action=action, changed=changed)


def _normalize_python_version_request(spec: str | None) -> str | None:
    if not spec:
        return None
    cleaned = str(spec).strip()
    if not cleaned:
        return None

    normalized = cleaned
    if cleaned == "*":
        normalized = ">=3.13"
    elif cleaned.startswith("^"):
        try:
            lower = Version(cleaned[1:])
        except InvalidVersion:
            return None
        release = lower.release + (0, 0, 0)
        major, minor, patch = release[:3]
        if major:
            upper = f"{major + 1}.0"
        elif minor:
            upper = f"0.{minor + 1}"
        else:
            upper = f"0.0.{patch + 1}"
        normalized = f">={lower},<{upper}"
    elif cleaned.startswith("~") and not cleaned.startswith("~="):
        raw_version = cleaned[1:]
        try:
            lower = Version(raw_version)
        except InvalidVersion:
            return None
        release = lower.release
        if len(release) <= 1:
            upper = f"{release[0] + 1}.0"
        else:
            upper = f"{release[0]}.{release[1] + 1}"
        normalized = f">={lower},<{upper}"
    elif re.fullmatch(r"\d+(?:\.\d+){0,2}(?:\.\*)?", cleaned):
        if cleaned.endswith(".*"):
            normalized = f"=={cleaned}"
        elif cleaned.count(".") < 2:
            normalized = f"=={cleaned}.*"
        else:
            normalized = f"=={cleaned}"

    try:
        SpecifierSet(normalized)
    except InvalidSpecifier:
        return None
    return normalized


def _extract_python_request_from_pyproject_text(pyproject_text: str) -> str | None:
    """Extract and validate the package's Python interpreter request."""

    try:
        import tomllib

        data = tomllib.loads(pyproject_text)
    except Exception:
        data = {}

    candidates: list[str] = []
    if isinstance(data, dict):
        code_repository_data = data.get("project") or {}
        if isinstance(code_repository_data, dict):
            req = code_repository_data.get("requires-python")
            if req:
                candidates.append(str(req))

        tool_data = data.get("tool") or {}
        if isinstance(tool_data, dict):
            poetry_data = tool_data.get("poetry") or {}
            if isinstance(poetry_data, dict):
                deps = poetry_data.get("dependencies") or {}
                if isinstance(deps, dict):
                    py_spec = deps.get("python")
                    if py_spec:
                        candidates.append(str(py_spec))

    for spec in candidates:
        parsed = _normalize_python_version_request(spec)
        if parsed:
            return parsed

    # Fallback regex parsing for partially-invalid TOML or non-standard formatting.
    req_match = re.search(r'(?im)^\s*requires-python\s*=\s*["\']([^"\']+)["\']\s*$', pyproject_text)
    if req_match:
        parsed = _normalize_python_version_request(req_match.group(1))
        if parsed:
            return parsed

    poetry_section = re.search(
        r"(?is)^\s*\[tool\.poetry\.dependencies\]\s*(.*?)(?:^\s*\[|\Z)",
        pyproject_text,
        re.MULTILINE,
    )
    if poetry_section:
        py_match = re.search(
            r'(?im)^\s*python\s*=\s*["\']([^"\']+)["\']\s*$', poetry_section.group(1)
        )
        if py_match:
            return _normalize_python_version_request(py_match.group(1))

    return None


def _venv_python_executable(venv_path: pathlib.Path) -> pathlib.Path | None:
    candidates = (
        venv_path / "Scripts" / "python.exe",
        venv_path / "bin" / "python",
    )
    return next((candidate for candidate in candidates if candidate.is_file()), None)


def _read_venv_python_version(venv_path: pathlib.Path) -> Version | None:
    python_executable = _venv_python_executable(venv_path)
    if python_executable is None:
        return None

    result = subprocess.run(
        [
            str(python_executable),
            "-c",
            "import platform; print(platform.python_version())",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return None
    try:
        return Version(result.stdout.strip())
    except InvalidVersion:
        return None


def _python_version_matches_request(version: Version, request: str) -> bool:
    return SpecifierSet(request).contains(version, prereleases=True)


def _resolve_uv_runner() -> tuple[list[str], str] | None:
    uv_bin = shutil.which("uv")
    if uv_bin:
        return [uv_bin], "uv"

    probe = subprocess.run(
        [sys.executable, "-m", "uv", "--version"],
        capture_output=True,
        text=True,
    )
    if probe.returncode == 0:
        return [sys.executable, "-m", "uv"], f"{sys.executable} -m uv"

    return None


def _install_uv() -> tuple[bool, str]:
    attempts = [
        [sys.executable, "-m", "pip", "install", "uv"],
        [sys.executable, "-m", "pip", "install", "--user", "uv"],
    ]
    reasons: list[str] = []
    for cmd in attempts:
        r = subprocess.run(cmd, capture_output=True, text=True)
        if r.returncode == 0:
            return True, ""
        out = (r.stderr or r.stdout or "").strip()
        if out:
            reasons.append(out.splitlines()[-1])
    return False, "; ".join(reasons)


def _current_session_jwt_tokens() -> tuple[str, str]:
    """
    Return access/refresh JWTs from the current CLI session.

    Raises:
        RuntimeError: if the CLI session does not currently expose both tokens.
    """
    tokens = cfg.get_tokens()
    access_token = (tokens.get("access") or "").strip()
    refresh_token = (tokens.get("refresh") or "").strip()
    if not access_token or not refresh_token:
        raise RuntimeError("JWT session tokens are missing. Run: mainsequence login")
    return access_token, refresh_token


def _current_code_repository_runtime_auth_env(backend_url: str) -> dict[str, str]:
    """
    Return auth environment entries for local CodeRepository `.env` provisioning.

    The output follows the active auth mode:
    - a backend-injected runtime credential mode preserves its credential keys
      and an exchanged access token
    - default JWT mode writes the current CLI session access/refresh token pair
    """
    if _runtime_credential_mode_enabled():
        credential_id = (os.environ.get("MAINSEQUENCE_RUNTIME_CREDENTIAL_ID") or "").strip()
        credential_secret = (os.environ.get("MAINSEQUENCE_RUNTIME_CREDENTIAL_SECRET") or "").strip()
        if not credential_id or not credential_secret:
            raise RuntimeError(
                "Runtime credential mode requires MAINSEQUENCE_RUNTIME_CREDENTIAL_ID "
                "and MAINSEQUENCE_RUNTIME_CREDENTIAL_SECRET."
            )

        access_token = _exchange_runtime_credential_for_cli_login(backend_url)
        return {
            "MAINSEQUENCE_AUTH_MODE": "runtime_credential",
            "MAINSEQUENCE_ACCESS_TOKEN": access_token,
            "MAINSEQUENCE_RUNTIME_CREDENTIAL_ID": credential_id,
            "MAINSEQUENCE_RUNTIME_CREDENTIAL_SECRET": credential_secret,
        }

    access_token, refresh_token = _current_session_jwt_tokens()
    return {
        "MAINSEQUENCE_ACCESS_TOKEN": access_token,
        "MAINSEQUENCE_REFRESH_TOKEN": refresh_token,
    }


def _render_code_repository_runtime_env_text(
    env_text: str,
    *,
    auth_env: dict[str, str],
    backend_url: str,
) -> str:
    """
    Return `.env` text with managed runtime auth keys refreshed.

    Managed keys are rewritten from scratch to avoid duplicate stale entries.
    Obsolete local CodeRepository aliases are not carried into the rendered file.
    """
    from mainsequence.repository_identity_security import (
        UNSUPPORTED_SOURCE_IDENTITY_ENV_NAMES,
    )

    managed_prefixes = (
        "MAINSEQUENCE_AUTH_MODE=",
        "MAINSEQUENCE_ACCESS_TOKEN=",
        "MAINSEQUENCE_REFRESH_TOKEN=",
        "MAINSEQUENCE_RUNTIME_CREDENTIAL_ID=",
        "MAINSEQUENCE_RUNTIME_CREDENTIAL_SECRET=",
        "MAINSEQUENCE_ENDPOINT=",
        "MAINSEQUENCE_TOKEN=",
    ) + tuple(f"{name}=" for name in UNSUPPORTED_SOURCE_IDENTITY_ENV_NAMES)
    lines = [
        ln
        for ln in (env_text or "").replace("\r", "").splitlines()
        if not any(ln.startswith(prefix) for prefix in managed_prefixes)
    ]

    if lines and lines[-1] != "":
        lines.append("")

    lines.extend(
        [f"{key}={value}" for key, value in auth_env.items() if value]
        + [f"MAINSEQUENCE_ENDPOINT={backend_url}"]
    )

    final_env = "\n".join(lines).replace("\r", "")
    return final_env + ("\n" if not final_env.endswith("\n") else "")


@code_repository.command("set-up-locally")
def code_repository_set_up_locally(
    code_repository_id: str = typer.Argument(..., help="CodeRepository UID from the platform"),
    branch: str | None = typer.Option(
        None,
        "--branch",
        help="Repository branch to check out; prompted when omitted.",
    ),
    base_dir: str | None = typer.Option(
        None, "--base-dir", help="Override base dir (default from settings)"
    ),
    scaffold_docker: bool = typer.Option(
        True,
        "--scaffold-docker/--no-scaffold-docker",
        help="Deprecated compatibility flag. Docker scaffolding is no longer derived during set-up-locally.",
    ),
):
    """
    Clone a CodeRepository locally and provision runtime `.env`.

    Workflow:
    - ensure, register when needed, and verify a repository-specific SSH key,
    - clone the repository into the local CodeRepositories root,
    - build local runtime auth/backend entries for the active auth mode,
    - write/update `.env` with local runtime values.

    Parameters
    ----------
    code_repository_id:
        Platform CodeRepository UID.
    base_dir:
        Override the local CodeRepositories base directory.
    scaffold_docker:
        Deprecated compatibility flag. No effect.

    Examples
    --------
    ```bash
    mainsequence code-repository set-up-locally code-repository-uid-123
    mainsequence code-repository set-up-locally code-repository-uid-123 --base-dir ~/mainsequence
    mainsequence code-repository set-up-locally code-repository-uid-123 --no-scaffold-docker
    ```
    """
    _require_login()

    cfg_obj = cfg.get_config()
    base = base_dir or cfg_obj["mainsequence_path"]
    org_slug = _org_slug_from_profile()

    try:
        p = resolve_code_repository(code_repository_id)
    except ApiError as e:
        error(f"CodeRepository not found/visible: {e}")
        raise typer.Exit(1) from e

    code_repository_uid = _code_repository_identity_value(p) or str(code_repository_id).strip()
    try:
        code_repository_branch = _resolve_code_repository_branch(
            p,
            repository_branch=branch,
            prompt_if_ambiguous=True,
        )
    except ApiError as e:
        error(str(e))
        raise typer.Exit(1) from e

    repository_branch = str(code_repository_branch.get("repository_branch") or "").strip()
    is_initialized = code_repository_branch.get("is_initialized")

    if is_initialized is not True:
        error(
            "CodeRepository has not finished initializing yet. "
            "Wait until is_initialized=true and try again."
        )
        raise typer.Exit(1)

    try:
        repo = _resolve_code_repository_repository_ssh_url(p)
    except ApiError as e:
        error(str(e))
        raise typer.Exit(1) from e

    name = safe_slug(p.get("code_repository_name") or f"code-repository-{code_repository_uid}")
    code_repositories_root = _code_repositories_root(base, org_slug)
    target_dir = code_repositories_root / f"{name}-{code_repository_uid}"
    code_repositories_root.mkdir(parents=True, exist_ok=True)

    if target_dir.exists():
        warn(f"Target already exists: {target_dir}")
        raise typer.Exit(2)

    try:
        key_path, pub, _git_env = _ensure_code_repository_repository_ssh_access(
            origin=repo,
            code_repository_ref=code_repository_uid,
            verify_access=lambda env: verify_git_remote_access(repo, env),
        )
    except (ApiError, RuntimeError, ValueError) as exc:
        error(f"Repository SSH key setup failed: {exc}")
        raise typer.Exit(1) from exc

    copied = _copy_clipboard(pub)
    agent_env = start_agent_and_add_key(key_path)

    env = git_ssh_environment(key_path, base_env=os.environ.copy() | agent_env)

    with status(f"Cloning repo into {target_dir}..."):
        rc = subprocess.call(
            ["git", "clone", "--branch", repository_branch, repo, str(target_dir)],
            env=env,
            cwd=str(code_repositories_root),
        )
    if rc != 0:
        try:
            import shutil

            if target_dir.exists():
                shutil.rmtree(target_dir, ignore_errors=True)
        except Exception:
            pass
        error("git clone failed")
        raise typer.Exit(3)

    backend_url = cfg.backend_url()
    try:
        auth_env = _current_code_repository_runtime_auth_env(backend_url)
    except RuntimeError as e:
        error(str(e))
        raise typer.Exit(1) from e
    except ApiError as e:
        error(str(e))
        raise typer.Exit(1) from e

    final_env = _render_code_repository_runtime_env_text(
        "",
        auth_env=auth_env,
        backend_url=backend_url,
    )
    (target_dir / ".env").write_text(final_env, encoding="utf-8")

    success(f"Local folder: {target_dir}")
    info(f"Repo URL: {repo}")
    info(f"Repository branch: {repository_branch}")
    if copied:
        info("Public key copied to clipboard.")


@code_repository.command("open-signed-terminal")
def code_repository_open_signed_terminal(
    code_repository_id: str | None = typer.Argument(
        None, help="Optional CodeRepository UID assertion against the current Git worktree"
    ),
    path: str | None = typer.Option(
        None, "--path", help="Open in a specific CodeRepository directory"
    ),
):
    """
    Open a terminal with `ssh-agent` and the CodeRepository key preloaded.

    Parameters
    ----------
    code_repository_id:
        Optional CodeRepository UID assertion against the current Git worktree.
    path:
        Explicit local path.

    Examples
    --------
    ```bash
    mainsequence code-repository open-signed-terminal code-repository-uid-123
    mainsequence code-repository open-signed-terminal --path .
    ```
    """
    code_repository_dir = _resolve_code_repository_dir(code_repository_id, path)

    origin = git_origin(code_repository_dir)
    name = repo_name_from_git_url(origin) or code_repository_dir.name
    try:
        context = get_code_repository_context(
            code_repository_uid=code_repository_id, code_repository_dir=code_repository_dir
        )
        code_repository_ref = str(context.code_repository_uid or "").strip()
        key_path, _public_key, _git_env = _ensure_code_repository_repository_ssh_access(
            origin=origin,
            code_repository_ref=code_repository_ref or None,
            verify_access=lambda env: verify_git_remote_access(origin, env),
        )
    except (ApiError, RuntimeError, ValueError) as exc:
        error(f"Repository SSH key setup failed: {exc}")
        raise typer.Exit(1) from exc
    open_signed_terminal(str(code_repository_dir), key_path, name)


@code_repository.command("build-local-venv")
def code_repository_build_local_venv(
    code_repository_id: str | None = typer.Argument(
        None, help="Optional CodeRepository UID assertion against the current Git worktree"
    ),
    path: str | None = typer.Option(None, "--path", help="CodeRepository directory"),
    recreate: bool = typer.Option(
        False,
        "--recreate",
        help="Replace an existing .venv after validating the package Python requirement",
    ),
):
    """
    Build local `.venv` and sync dependencies using `uv`.

    Reads Python requirement from `pyproject.toml`, creates `.venv`, then runs `uv sync`.

    Parameters
    ----------
    code_repository_id:
        Optional CodeRepository UID assertion. The current Git worktree selects the folder.
    path:
        Explicit local path.

    Examples
    --------
    ```bash
    mainsequence code-repository build-local-venv
    mainsequence code-repository build-local-venv code-repository-uid-123
    mainsequence code-repository build-local-venv --path .
    mainsequence code-repository build-local-venv --path . --recreate
    ```
    """
    code_repository_dir = _resolve_code_repository_dir(code_repository_id, path)
    pyproject_path = code_repository_dir / "pyproject.toml"
    if not pyproject_path.is_file():
        error("pyproject.toml not found in the CodeRepository root.")
        raise typer.Exit(1)

    try:
        pyproject_text = pyproject_path.read_text(encoding="utf-8")
    except Exception as e:
        error("Could not read pyproject.toml from the CodeRepository root.")
        raise typer.Exit(1) from e

    python_request = _extract_python_request_from_pyproject_text(pyproject_text)
    if not python_request:
        error(
            "Could not determine a valid Python requirement from pyproject.toml "
            "(requires-python or Poetry python spec)."
        )
        raise typer.Exit(1)

    venv_path = code_repository_dir / ".venv"
    replace_existing_venv = False
    if venv_path.exists():
        existing_version = _read_venv_python_version(venv_path)
        if not recreate:
            if existing_version is not None and _python_version_matches_request(
                existing_version, python_request
            ):
                info(
                    f"Skipped: {venv_path} already uses compatible Python "
                    f"{existing_version} ({python_request})."
                )
                return

            actual = str(existing_version) if existing_version is not None else "unreadable"
            error(
                f"Existing {venv_path} uses Python {actual}, which does not satisfy "
                f"{python_request}."
            )
            info("Re-run with --recreate to replace the incompatible environment.")
            raise typer.Exit(1)

        replace_existing_venv = True

    with status("Building local .venv..."):
        uv_runner = _resolve_uv_runner()
        if not uv_runner:
            info("uv not found. Installing uv...")
            ok, reason = _install_uv()
            if not ok:
                details = f": {reason}" if reason else ""
                error(
                    f"uv is not installed and automatic install failed{details}. Install manually with: pip install uv"
                )
                raise typer.Exit(1)

            uv_runner = _resolve_uv_runner()
            if not uv_runner:
                error(
                    "uv install completed but uv is still not available. Restart your shell and try again."
                )
                raise typer.Exit(1)

        uv_cmd, uv_display = uv_runner

        if replace_existing_venv:
            info(f"Replacing existing {venv_path}.")
            shutil.rmtree(venv_path)

        info(f"Creating .venv with Python requirement {python_request}...")
        venv_result = subprocess.run(
            [*uv_cmd, "venv", ".venv", "--python", python_request],
            cwd=str(code_repository_dir),
            env=os.environ.copy(),
            capture_output=True,
            text=True,
        )
        if venv_result.returncode != 0:
            reason = (venv_result.stderr or venv_result.stdout or "").strip()
            error(
                f"Failed to create local .venv via {uv_display}: {reason or f'exit {venv_result.returncode}'}"
            )
            raise typer.Exit(1)

        info("Running uv sync with .venv...")
        sync_env = os.environ.copy()
        sync_env["UV_PROJECT_ENVIRONMENT"] = ".venv"
        sync_result = subprocess.run(
            [*uv_cmd, "sync"],
            cwd=str(code_repository_dir),
            env=sync_env,
            capture_output=True,
            text=True,
        )
        if sync_result.returncode != 0:
            reason = (sync_result.stderr or sync_result.stdout or "").strip()
            error(
                f"Failed to run uv sync for local .venv via {uv_display}: {reason or f'exit {sync_result.returncode}'}"
            )
            raise typer.Exit(1)

    success(f"Local .venv built for Python requirement {python_request}.")


@code_repository.command("refresh-token")
def code_repository_refresh_token(
    code_repository_id: str | None = typer.Argument(
        None, help="Optional CodeRepository UID assertion against the current Git worktree"
    ),
    path: str | None = typer.Option(None, "--path", help="CodeRepository directory"),
):
    """
    Refresh local CodeRepository auth entries in `.env` from the active auth mode.

    Use this when a CodeRepository has been idle long enough for the previously injected
    auth token to expire. The command preserves the rest of the `.env` file and
    only rewrites the runtime auth keys managed by the CLI.

    Parameters
    ----------
    code_repository_id:
        Optional CodeRepository UID assertion against the current Git worktree.
    path:
        Explicit local path. If omitted, the current directory is used.

    Examples
    --------
    ```bash
    mainsequence code-repository refresh-token
    mainsequence code-repository refresh-token code-repository-uid-123
    mainsequence code-repository refresh-token --path .
    ```
    """
    _require_login()
    code_repository_dir = _resolve_code_repository_dir(code_repository_id, path)
    env_path = code_repository_dir / ".env"
    if not env_path.is_file():
        error(f".env not found in CodeRepository root: {env_path}")
        info(
            "Run: mainsequence code-repository set-up-locally <code_repository_uid> to provision the local runtime first."
        )
        raise typer.Exit(1)

    backend_url = cfg.backend_url()
    try:
        auth_env = _current_code_repository_runtime_auth_env(backend_url)
    except RuntimeError as e:
        error(str(e))
        raise typer.Exit(1) from e
    except ApiError as e:
        error(str(e))
        raise typer.Exit(1) from e

    try:
        env_text = env_path.read_text(encoding="utf-8")
    except Exception as e:
        error(f"Could not read .env: {e}")
        raise typer.Exit(1) from e

    final_env = _render_code_repository_runtime_env_text(
        env_text,
        auth_env=auth_env,
        backend_url=backend_url,
    )
    env_path.write_text(final_env, encoding="utf-8")
    success(f"Refreshed auth entries in: {env_path}")


@code_repository.command("freeze-env")
def code_repository_freeze_env(
    code_repository_id: str | None = typer.Argument(
        None, help="Optional CodeRepository UID assertion against the current Git worktree"
    ),
    path: str | None = typer.Option(None, "--path", help="CodeRepository directory"),
    ensure_uv: bool = typer.Option(
        True,
        "--ensure-uv/--no-ensure-uv",
        help="Allow resolving uv from PATH when it is not present inside .venv.",
    ),
):
    """
    Export locked runtime dependencies into `requirements.txt` using `uv`.

    Development dependency groups are excluded from the runtime export.

    Parameters
    ----------
    code_repository_id:
        Optional CodeRepository UID assertion against the current Git worktree.
    path:
        Explicit local path.
    ensure_uv:
        Allow resolving `uv` from PATH when it is not present inside `.venv`.

    Examples
    --------
    ```bash
    mainsequence code-repository freeze-env code-repository-uid-123
    mainsequence code-repository freeze-env --path .
    mainsequence code-repository freeze-env --path . --no-ensure-uv
    ```
    """
    code_repository_dir = _resolve_code_repository_dir(code_repository_id, path)
    ensure_venv(code_repository_dir)

    uv = (
        ensure_uv_installed(code_repository_dir)
        if ensure_uv
        else (ensure_venv(code_repository_dir).uv or None)
    )
    if not uv:
        error("uv not found in .venv and --no-ensure-uv was used.")
        raise typer.Exit(1)

    with status("Exporting requirements.txt via uv..."):
        uv_export_requirements(
            uv,
            cwd=code_repository_dir,
            locked=True,
            no_dev=True,
            no_hashes=True,
            output_file="requirements.txt",
        )

    success(f"Wrote: {code_repository_dir / 'requirements.txt'}")


@code_repository.command("sync")
def code_repository_sync(
    message: str | None = typer.Argument(None, help="Git commit message"),
    code_repository_id: str | None = typer.Argument(
        None, help="Optional CodeRepository UID assertion against the current Git worktree"
    ),
    path: str | None = typer.Option(None, "--path", help="CodeRepository directory"),
    message_opt: str | None = typer.Option(None, "--message", "-m", help="Git commit message"),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help=(
            "Run read-only preflight and print steps without generating keys or "
            "changing the CodeRepository"
        ),
    ),
):
    """
    Run the end-to-end sync workflow for CodeRepository dependencies and Git state.

    Workflow:
    1. preview the patch version and backend-owned CodeRepositoryBranch tag,
    2. reject local and remote collisions before mutating the CodeRepository,
    3. apply and verify the patch version via `uv version`,
    4. run `uv lock` + `uv sync`,
    5. export locked `requirements.txt`,
    6. commit the changes and create that annotated tag,
    7. atomically push the branch and tag.

    Parameters
    ----------
    message:
        Commit message. Can be passed positionally or via `--message`.
    code_repository_id:
        Optional CodeRepository UID assertion against the current Git worktree.
    path:
        Explicit local path.
    dry_run:
        Run read-only preflight and print the plan without generating keys or
        changing CodeRepository files, dependencies, Git state, or backend state.

    Examples
    --------
    ```bash
    mainsequence code-repository sync "Update environment"
    mainsequence code-repository sync -m "Update environment" --path .
    mainsequence code-repository sync -m "Preview only" --path . --dry-run
    ```
    """
    if message is not None and message_opt is not None:
        error("Pass the commit message either positionally or with --message, not both.")
        raise typer.Exit(2)

    message = message if message is not None else message_opt
    code_repository_dir = _resolve_code_repository_dir(code_repository_id, path)

    safe_message = (
        str(message or "").replace("\r", " ").replace("\n", " ").replace('"', "'").strip()
    )
    if not safe_message:
        error("Commit message is required.")
        raise typer.Exit(1)

    try:
        git_branch, code_repository_branch_uid = _resolve_git_code_repository_branch_context(
            code_repository_id,
            code_repository_dir=code_repository_dir,
        )
        code_repository_ref = str(get_code_repository_context().code_repository_uid or "").strip()
    except ApiError as exc:
        error(f"CodeRepository sync preflight failed: {exc}")
        raise typer.Exit(1) from exc

    info(
        "CodeRepository sync preflight resolved "
        f"Git branch {git_branch!r} to CodeRepositoryBranch {code_repository_branch_uid}."
    )

    origin = git_origin(code_repository_dir)
    repo_name = repo_name_from_git_url(origin) or code_repository_dir.name
    try:
        require_ssh_git_origin(origin)
    except ValueError as exc:
        error(f"CodeRepository sync preflight failed: {exc}")
        raise typer.Exit(1) from exc

    try:
        ensure_venv(code_repository_dir)
        uv = ensure_uv_installed(code_repository_dir)
        current_version = uv_project_version(uv, cwd=code_repository_dir)
        next_version = uv_preview_patch_version(uv, cwd=code_repository_dir)
        tag = render_code_repository_branch_default_redeployment_tag(
            code_repository_branch_uid,
            version=next_version,
        )
        verify_git_tag_absent(code_repository_dir, tag)
    except (ApiError, RuntimeError) as exc:
        error(f"CodeRepository sync tag preflight failed: {exc}")
        raise typer.Exit(1) from exc

    steps = [
        "preview uv patch version",
        "request backend default redeployment tag",
        "verify backend tag does not exist locally",
        "ensure repository SSH key",
        "register a new or inaccessible SSH key through the owning CodeRepository",
        "git push --dry-run --follow-tags origin HEAD:refs/heads/<branch>",
        "verify exact backend tag does not exist remotely",
        "uv version --bump patch",
        "verify bumped version matches the preflight version",
        "uv lock",
        "uv sync",
        "uv export (locked) -> requirements.txt",
        "git add -A",
        f'git commit -m "{safe_message}"',
        "git tag -a <backend tag> -m <backend tag>",
        "git push --atomic --follow-tags origin HEAD:refs/heads/<branch> refs/tags/<backend tag>:refs/tags/<backend tag>",
    ]

    print_kv(
        "Sync release",
        [
            ("Current version", current_version),
            ("Next version", next_version),
            ("Branch tag", tag),
        ],
    )
    print_table("Sync plan", ["Step"], [[s] for s in steps])

    if dry_run:
        warn("Dry run: read-only preflight complete; no changes made.")
        return

    try:
        _key_path, _public_key, env = _ensure_code_repository_repository_ssh_access(
            origin=origin,
            code_repository_ref=code_repository_ref,
            verify_access=lambda ssh_env: verify_git_push_access(
                code_repository_dir,
                git_branch,
                ssh_env,
            ),
        )
    except (ApiError, RuntimeError, ValueError) as exc:
        error(f"CodeRepository sync SSH preflight failed: {exc}")
        raise typer.Exit(1) from exc

    try:
        verify_git_remote_tag_absent(code_repository_dir, tag, env)
    except RuntimeError as exc:
        error(f"CodeRepository sync remote tag preflight failed: {exc}")
        raise typer.Exit(1) from exc

    with status("Running uv + git sync steps..."):
        run_uv(uv, ["version", "--bump", "patch"], cwd=code_repository_dir, env=env)
        version = uv_project_version(uv, cwd=code_repository_dir, env=env)
        if version != next_version:
            error(
                f"CodeRepository sync version verification failed: uv produced {version}; "
                f"preflight expected {next_version}."
            )
            raise typer.Exit(1)
        run_uv(uv, ["lock"], cwd=code_repository_dir, env=env)
        run_uv(uv, ["sync"], cwd=code_repository_dir, env=env)
        # `uv sync` can prune ad hoc packages from `.venv`, including a `uv`
        # executable that was installed there just for this workflow.
        uv = ensure_uv_installed(code_repository_dir)
        uv_export_requirements(
            uv,
            cwd=code_repository_dir,
            locked=True,
            no_dev=True,
            no_hashes=True,
            output_file="requirements.txt",
        )

        run_cmd(["git", "add", "-A"], cwd=code_repository_dir, env=env)
        run_cmd(["git", "commit", "-m", safe_message], cwd=code_repository_dir, env=env)
        run_cmd(["git", "tag", "-a", tag, "-m", tag], cwd=code_repository_dir, env=env)
        run_cmd(
            [
                "git",
                "push",
                "--atomic",
                "--follow-tags",
                "origin",
                f"HEAD:refs/heads/{git_branch}",
                f"refs/tags/{tag}:refs/tags/{tag}",
            ],
            cwd=code_repository_dir,
            env=env,
        )

    success(f"Synced: {repo_name}")


@code_repository.command("update-sdk")
def code_repository_update_sdk(
    code_repository_id: str | None = typer.Argument(
        None, help="Optional CodeRepository UID assertion against the current Git worktree"
    ),
    path: str | None = typer.Option(None, "--path", help="CodeRepository directory"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Print steps but do not execute"),
):
    """
    Upgrade the CodeRepository SDK dependency (`mainsequence`) using `uv`.

    Parameters
    ----------
    code_repository_id:
        Optional CodeRepository UID assertion against the current Git worktree.
    path:
        Explicit local path.
    dry_run:
        Print update plan without executing.

    Examples
    --------
    ```bash
    mainsequence code-repository update-sdk code-repository-uid-123
    mainsequence code-repository update-sdk --path .
    mainsequence code-repository update-sdk --path . --dry-run
    ```
    """
    code_repository_dir = _resolve_code_repository_dir(code_repository_id, path)
    ensure_venv(code_repository_dir)

    steps = [
        "resolve uv executable",
        "uv lock --upgrade-package mainsequence",
        "uv sync",
    ]
    print_table("Update SDK plan", ["Step"], [[s] for s in steps])

    if dry_run:
        warn("Dry run: no commands executed.")
        return

    uv = ensure_uv_installed(code_repository_dir)
    with status("Upgrading mainsequence SDK via uv..."):
        run_uv(uv, ["lock", "--upgrade-package", "mainsequence"], cwd=code_repository_dir)
        run_uv(uv, ["sync"], cwd=code_repository_dir)

    success("SDK update complete.")


@code_repository.command("update")
def code_repository_update_scaffold_target(
    target: str = typer.Argument(
        ..., help="Scaffold target to update. Currently supported: AGENTS.md"
    ),
    code_repository_id: str | None = typer.Option(
        None,
        "--code-repository-uid",
        help="Optional CodeRepository UID assertion against the current Git worktree",
    ),
    path: str | None = typer.Option(None, "--path", help="CodeRepository directory"),
):
    """
    Update a scaffold-managed file in the local CodeRepository root.

    Currently this command supports only `AGENTS.md`.
    If the Main Sequence managed marker is present, only that block is updated.
    If the marker is absent, the whole file is replaced from the installed
    scaffold template.

    Examples
    --------
    ```bash
    mainsequence code-repository update AGENTS.md
    mainsequence code-repository update AGENTS.md --path .
    mainsequence code-repository update AGENTS.md --code-repository-uid code-repository-uid-123
    ```
    """
    if target != "AGENTS.md":
        error(f"Unsupported scaffold update target: {target}. Supported target: AGENTS.md")
        raise typer.Exit(1)

    code_repository_dir = _resolve_code_repository_dir(code_repository_id, path)
    destination = code_repository_dir / "AGENTS.md"

    try:
        source, bootstrap_content, managed_block = _load_installed_agents_md_template()
        update_result = _update_agents_md_managed_block_file(
            destination,
            bootstrap_content,
            managed_block,
        )
    except ValueError as exc:
        error(str(exc))
        raise typer.Exit(1) from exc

    payload = {
        "target": target,
        "code_repository": code_repository_dir,
        "source": source,
        "destination": destination,
        "action": update_result.action,
        "changed": update_result.changed,
        "overwritten": False,
        "managed_block": {
            "start": AGENTS_MD_MANAGED_BLOCK_START_PREFIX,
            "end": AGENTS_MD_MANAGED_BLOCK_END,
        },
    }
    if _emit_json(payload):
        return

    if update_result.action == "unchanged":
        success(f"{target} Main Sequence managed block already current.")
    else:
        success(f"Updated scaffold-managed {target}.")
    print_kv(
        "Scaffold Update",
        [
            ("Target", target),
            ("Action", update_result.action),
            ("CodeRepository", str(code_repository_dir)),
            ("Source", str(source)),
            ("Destination", str(destination)),
        ],
    )


@code_repository.command("update-agent-skills")
def code_repository_update_agent_skills(
    code_repository_id: str | None = typer.Option(
        None,
        "--code-repository-uid",
        help="Optional CodeRepository UID assertion against the current Git worktree",
    ),
    path: str | None = typer.Option(None, "--path", help="CodeRepository directory"),
):
    """
    Update `.agents/skills/mainsequence` from installed SDK and platform sources.

    The existing command copies SDK-owned execution skills from the target
    CodeRepository's installed `agent_scaffold/skills` tree and retrieves
    platform-owned skills from authenticated MCP resources. It validates and
    stages both sources, then replaces only the managed `mainsequence`
    namespace and writes one dual-source `PINNED_FROM.txt`. Bundle-root files
    such as `AGENTS.md` are not copied by this command.

    Examples
    --------
    ```bash
    mainsequence code-repository update-agent-skills
    mainsequence code-repository update-agent-skills --path .
    mainsequence code-repository update-agent-skills --code-repository-uid code-repository-uid-123
    ```
    """
    code_repository_dir = _resolve_code_repository_dir(code_repository_id, path)

    scaffold_bundle_dir = _code_repository_agent_scaffold_bundle_dir(code_repository_dir)
    skills_dir = scaffold_bundle_dir / "skills"
    if not skills_dir.exists() or not skills_dir.is_dir():
        error(f"CodeRepository-installed agent_scaffold bundle is missing skills/: {skills_dir}")
        raise typer.Exit(1)

    pinned_version = _code_repository_installed_package_version(code_repository_dir, "mainsequence")
    source_checkout_root = _mainsequence_source_checkout_root()
    protected_code_repository_roots = (
        (source_checkout_root,) if source_checkout_root is not None else ()
    )
    try:
        platform_catalog = fetch_platform_code_repository_skill_catalog()
        install_result = install_dual_source_code_repository_skills(
            code_repository_dir=code_repository_dir,
            sdk_library_name="mainsequence",
            namespace="mainsequence",
            sdk_skills_path=skills_dir,
            sdk_version=pinned_version,
            platform_catalog=platform_catalog,
            command="mainsequence code-repository update-agent-skills",
            protected_code_repository_roots=protected_code_repository_roots,
        )
    except (
        ApiError,
        CodeRepositorySkillAssemblyError,
        FileNotFoundError,
        OSError,
        ValueError,
    ) as exc:
        error(str(exc))
        raise typer.Exit(1) from exc

    updated = [
        {
            "name": item.name,
            "owner": item.owner,
            "source": item.source,
            "destination": item.destination,
            "content_sha256": item.content_sha256,
        }
        for item in install_result.installed
    ]

    payload = {
        "code_repository": code_repository_dir,
        "library_name": install_result.sdk_library_name,
        "namespace": "mainsequence",
        "skills_path": install_result.sdk_skills_path,
        "destination_root": install_result.destination_root,
        "sentinel_path": install_result.sentinel_path,
        "pinned_version": install_result.sdk_version,
        "sdk": {
            "library_name": install_result.sdk_library_name,
            "version": install_result.sdk_version,
            "skills_path": install_result.sdk_skills_path,
        },
        "platform": {
            "source_url": platform_catalog.source_url,
            "manifest_version": platform_catalog.manifest_version,
            "manifest_sha256": platform_catalog.manifest_sha256,
            "ontology_uri": platform_catalog.ontology_uri,
            "ontology_sha256": platform_catalog.ontology_sha256,
            "resources": [
                {
                    "name": resource.name,
                    "uri": resource.uri,
                    "path": str(resource.resource_path),
                    "content_sha256": resource.content_sha256,
                }
                for resource in platform_catalog.resources
            ],
            "skills": [
                {
                    "name": skill.name,
                    "uri": skill.uri,
                    "path": str(skill.relative_path),
                    "content_sha256": skill.content_sha256,
                }
                for skill in platform_catalog.skills
            ],
        },
        "updated_count": len(updated),
        "updated": updated,
    }
    if _emit_json(payload):
        return

    success("Updated .agents/skills/mainsequence from installed SDK and platform sources.")
    print_kv(
        "CodeRepository Skill Provenance",
        [
            ("SDK Library", install_result.sdk_library_name),
            ("SDK Version", install_result.sdk_version),
            ("Platform Manifest", platform_catalog.manifest_sha256),
            ("Platform Resources", len(platform_catalog.resources)),
            ("Platform Skills", len(platform_catalog.skills)),
            ("Sentinel", str(install_result.sentinel_path)),
        ],
    )
    print_table(
        "Updated CodeRepository Skills",
        ["Skill", "Owner", "Destination"],
        [[item["name"], item["owner"], str(item["destination"])] for item in updated],
    )
