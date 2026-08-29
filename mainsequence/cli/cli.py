"""
mainsequence.cli.cli
====================

MainSequence CLI entrypoint.

Parity with VS Code extension:
- settings set-backend
- logout (clear tokens)
- code-repository freeze-env (compile environment)
- code-repository build_local_venv (create local .venv from pyproject + uv sync)
- code-repository sync (uv bump + lock/sync/export + git commit/push)
- code-repository build-docker-env (docker build + devcontainer config)
- local `.env` provisioning during set-up-locally uses only CLI-managed runtime values
- code-repository current (detect current code repository + venv/python info)
- sdk latest + code-repository sdk-status + code-repository update-sdk
- doctor diagnostics

All commands have docstrings so `--help` is useful.
"""

from __future__ import annotations

import dataclasses
import datetime
import difflib
import importlib
import importlib.metadata
import json
import os
import pathlib
import platform
import re
import shlex
import shutil
import subprocess
import sys
import time
import uuid
from decimal import ROUND_UP, Decimal
from enum import Enum as PyEnum
from textwrap import dedent

import click
import typer
from packaging.specifiers import InvalidSpecifier, SpecifierSet
from packaging.version import InvalidVersion, Version

from ..client.compute_validation import decimal_to_storage, parse_cpu_request, parse_memory_request
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
    add_agent_team_to_edit,
    add_agent_team_to_view,
    add_agent_user_to_edit,
    add_agent_user_to_view,
    add_code_repository_labels,
    add_code_repository_team_to_edit,
    add_code_repository_team_to_view,
    add_code_repository_user_to_edit,
    add_code_repository_user_to_view,
    add_constant_team_to_edit,
    add_constant_team_to_view,
    add_constant_user_to_edit,
    add_constant_user_to_view,
    add_deploy_key,
    add_meta_table_labels,
    add_meta_table_team_to_edit,
    add_meta_table_team_to_view,
    add_meta_table_user_to_edit,
    add_meta_table_user_to_view,
    add_secret_team_to_edit,
    add_secret_team_to_view,
    add_secret_user_to_edit,
    add_secret_user_to_view,
    add_team_user_to_edit,
    add_team_user_to_view,
    add_time_index_table_labels,
    add_time_index_table_team_to_edit,
    add_time_index_table_team_to_view,
    add_time_index_table_user_to_edit,
    add_time_index_table_user_to_view,
    bulk_delete_code_repositories,
    create_agent,
    create_code_repository,
    create_code_repository_image,
    create_code_repository_job,
    create_code_repository_resource_release,
    create_constant,
    create_organization_team,
    create_secret,
    delete_agent,
    delete_code_repository_image,
    delete_constant,
    delete_meta_table,
    delete_organization_team,
    delete_resource_release,
    delete_secret,
    delete_time_index_table,
    fetch_platform_code_repository_skill_catalog,
    get_agent,
    get_agent_logs,
    get_agent_resource_usage,
    get_agent_session,
    get_agent_session_logs,
    get_code_repositories,
    get_code_repository_branch,
    get_code_repository_image,
    get_code_repository_job,
    get_code_repository_job_run_logs,
    get_code_repository_job_run_resource_usage,
    get_code_repository_repository,
    get_code_repository_time_index_table_updates,
    get_constant,
    get_current_user_profile,
    get_logged_user_details,
    get_meta_table,
    get_or_create_agent_session,
    get_organization_team,
    get_resource_release,
    get_resource_release_logs,
    get_resource_release_resource_usage,
    get_secret,
    get_time_index_table,
    list_agent_sessions,
    list_agent_users_can_edit,
    list_agent_users_can_view,
    list_agents,
    list_code_repository_base_images,
    list_code_repository_images,
    list_code_repository_job_runs,
    list_code_repository_jobs,
    list_code_repository_resources,
    list_code_repository_users_can_edit,
    list_code_repository_users_can_view,
    list_constant_users_can_edit,
    list_constant_users_can_view,
    list_constants,
    list_github_organizations,
    list_meta_table_users_can_edit,
    list_meta_table_users_can_view,
    list_meta_tables,
    list_organization_teams,
    list_secret_users_can_edit,
    list_secret_users_can_view,
    list_secrets,
    list_team_users_can_edit,
    list_team_users_can_view,
    list_time_index_table_users_can_edit,
    list_time_index_table_users_can_view,
    list_time_index_tables,
    logout_cli_session,
    refresh_time_index_table_search_index,
    remove_agent_team_from_edit,
    remove_agent_team_from_view,
    remove_agent_user_from_edit,
    remove_agent_user_from_view,
    remove_code_repository_labels,
    remove_code_repository_team_from_edit,
    remove_code_repository_team_from_view,
    remove_code_repository_user_from_edit,
    remove_code_repository_user_from_view,
    remove_constant_team_from_edit,
    remove_constant_team_from_view,
    remove_constant_user_from_edit,
    remove_constant_user_from_view,
    remove_meta_table_labels,
    remove_meta_table_team_from_edit,
    remove_meta_table_team_from_view,
    remove_meta_table_user_from_edit,
    remove_meta_table_user_from_view,
    remove_secret_team_from_edit,
    remove_secret_team_from_view,
    remove_secret_user_from_edit,
    remove_secret_user_from_view,
    remove_team_user_from_edit,
    remove_team_user_from_view,
    remove_time_index_table_labels,
    remove_time_index_table_team_from_edit,
    remove_time_index_table_team_from_view,
    remove_time_index_table_user_from_edit,
    remove_time_index_table_user_from_view,
    render_code_repository_branch_default_redeployment_tag,
    repo_name_from_git_url,
    resolve_code_repository,
    run_code_repository_job,
    run_meta_table_query,
    run_time_index_table_query,
    safe_slug,
    search_code_repositories,
    semantic_search_agents,
    send_agent_session_a2a_message,
    time_index_table_column_search,
    time_index_table_description_search,
    update_organization_team,
    validate_code_repository_name,
)
from .browser_auth import (
    BrowserAuthError,
    login_via_browser,
    login_via_mcp_handoff,
)
from .code_repository_status import detect_current_code_repository
from .docker_utils import (
    build_docker_environment,
    compute_docker_image_ref,
    write_devcontainer_config,
)
from .doctor import run_doctor
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
from .migrations import migrations as migrations_group
from .model_filters import build_cli_model_filter_rows, parse_cli_model_filters
from .pydantic_cli import (
    get_cli_field_metadata,
    pydantic_argument,
    pydantic_option,
    pydantic_prompt_text,
)
from .sdk_utils import fetch_latest_sdk_version, normalize_version, read_local_sdk_version
from .ssh_utils import (
    ensure_key_for_repo,
    git_ssh_environment,
    open_folder,
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

JSON_OUTPUT_CONTEXT_KEY = "json_output"
_JSON_OUTPUT_REQUESTED = False


def _package_version() -> str:
    """
    Return the installed SDK version for CLI display.
    """
    try:
        return importlib.metadata.version("mainsequence")
    except importlib.metadata.PackageNotFoundError:
        return "unknown"


class MainSequenceGroup(typer.core.TyperGroup):
    """
    Typer group that accepts `--json` anywhere in the command line.

    The flag is stripped before the normal Typer/Click parsing path runs and is
    stored on the root context for later rendering decisions.
    """

    def parse_args(self, ctx: click.Context, args: list[str]) -> list[str]:
        global _JSON_OUTPUT_REQUESTED

        filtered_args: list[str] = []
        json_output = False
        for arg in args:
            if arg == "--json":
                json_output = True
                continue
            filtered_args.append(arg)

        ctx.ensure_object(dict)
        _JSON_OUTPUT_REQUESTED = json_output
        if json_output:
            ctx.obj[JSON_OUTPUT_CONTEXT_KEY] = True
        return super().parse_args(ctx, filtered_args)


def _json_output_enabled() -> bool:
    if _JSON_OUTPUT_REQUESTED:
        return True
    ctx = click.get_current_context(silent=True)
    if ctx is None:
        return False
    root = ctx.find_root()
    obj = getattr(root, "obj", None) or {}
    return bool(obj.get(JSON_OUTPUT_CONTEXT_KEY))


def _to_jsonable(value):
    if hasattr(value, "model_dump_json"):
        try:
            return _to_jsonable(json.loads(value.model_dump_json()))
        except Exception:
            pass
    if hasattr(value, "model_dump"):
        try:
            return _to_jsonable(value.model_dump(mode="json"))
        except Exception:
            pass
    if dataclasses.is_dataclass(value):
        return _to_jsonable(dataclasses.asdict(value))
    if isinstance(value, dict):
        return {str(key): _to_jsonable(item) for key, item in value.items() if key != "orm_class"}
    if isinstance(value, list | tuple | set):
        return [_to_jsonable(item) for item in value]
    if isinstance(value, datetime.datetime | datetime.date | datetime.time):
        return value.isoformat()
    if isinstance(value, pathlib.Path):
        return str(value)
    if isinstance(value, PyEnum):
        return value.value
    return value


def _emit_json(payload, *, force: bool = False) -> bool:
    if not force and not _json_output_enabled():
        return False
    typer.echo(json.dumps(_to_jsonable(payload), indent=2, ensure_ascii=False))
    return True


app = typer.Typer(
    help="MainSequence CLI (login + code-repository operations)",
    cls=MainSequenceGroup,
    invoke_without_command=True,
)

agent = typer.Typer(help="Agent commands")
agent_session_group = typer.Typer(help="Agent session commands")
agent_session_a2a_group = typer.Typer(help="Agent session A2A commands")
constants = typer.Typer(help="Constant commands")
secrets = typer.Typer(help="Secret commands")
organization = typer.Typer(help="Organization commands")
organization_teams_group = typer.Typer(help="Organization team commands")
meta_table_group = typer.Typer(help="MetaTable table-storage commands")
time_index_table_group = typer.Typer(help="Time-index table discovery and access commands")
code_repository = typer.Typer(help="CodeRepository commands (remote + local operations)")
code_repository_list_group = typer.Typer(help="List-related CodeRepository commands")
code_repository_resources_group = typer.Typer(help="CodeRepository resource commands")
code_repository_time_index_table_updates_group = typer.Typer(
    help="CodeRepository time-index table update commands"
)
code_repository_images_group = typer.Typer(help="CodeRepository image commands")
code_repository_jobs_group = typer.Typer(help="CodeRepository job commands")
code_repository_job_runs_group = typer.Typer(help="CodeRepository job run commands")
settings = typer.Typer(help="Settings (base folder, backend, etc.)")
sdk = typer.Typer(help="SDK utilities (latest version, status)")
skills = typer.Typer(help="Installed scaffold skill commands")

app.add_typer(agent, name="agent")
agent.add_typer(agent_session_group, name="session")
agent_session_group.add_typer(agent_session_a2a_group, name="a2a")
app.add_typer(constants, name="constants")
app.add_typer(secrets, name="secrets")
app.add_typer(organization, name="organization")
app.add_typer(skills, name="skills")
app.add_typer(meta_table_group, name="meta-table")
app.add_typer(meta_table_group, name="meta_table")
app.add_typer(time_index_table_group, name="time-index-table")
app.add_typer(code_repository, name="code-repository")
code_repository.add_typer(code_repository_list_group, name="list")
code_repository.add_typer(code_repository_resources_group, name="resources")
code_repository.add_typer(code_repository_time_index_table_updates_group, name="time-index-table-updates")
code_repository.add_typer(code_repository_images_group, name="images")
code_repository.add_typer(code_repository_jobs_group, name="jobs")
code_repository_jobs_group.add_typer(code_repository_job_runs_group, name="runs")
app.add_typer(settings, name="settings")
app.add_typer(sdk, name="sdk")
app.add_typer(migrations_group, name="migrations")


@app.callback()
def root_callback(
    version: bool = typer.Option(
        False,
        "--version",
        help="Show installed mainsequence SDK version and exit.",
    ),
):
    """
    MainSequence CLI.
    """
    if version:
        typer.echo(f"mainsequence {_package_version()}")
        raise typer.Exit()


JOB_DEFAULT_CPU_REQUEST = Decimal("0.25")
JOB_DEFAULT_MEMORY_REQUEST = Decimal("0.5")
JOB_MEMORY_PER_CPU_MAX = Decimal("6.5")
JOB_DEFAULT_SPOT = False
JOB_DEFAULT_MAX_RUNTIME_SECONDS = 86400
JOB_ALLOWED_INTERVAL_PERIODS = ("seconds", "minutes", "hours", "days")
AGENT_MODEL_REF = "mainsequence.client.agent_runtime_models.Agent"
AGENT_SESSION_MODEL_REF = "mainsequence.client.agent_runtime_models.AgentSession"
JOB_MODEL_REF = "mainsequence.client.models_helpers.Job"
INTERVAL_SCHEDULE_MODEL_REF = "mainsequence.client.models_helpers.IntervalSchedule"
CRONTAB_SCHEDULE_MODEL_REF = "mainsequence.client.models_helpers.CrontabSchedule"
JOB_RUN_MODEL_REF = "mainsequence.client.models_helpers.JobRun"
CODE_REPOSITORY_IMAGE_MODEL_REF = "mainsequence.client.models_foundry.CodeRepositoryImage"
CODE_REPOSITORY_RESOURCE_MODEL_REF = "mainsequence.client.models_helpers.CodeRepositoryResource"
RESOURCE_RELEASE_MODEL_REF = "mainsequence.client.models_helpers.ResourceRelease"
TIME_INDEX_TABLE_MODEL_REF = "mainsequence.client.metatables.TimeIndexMetaTable"
META_TABLE_MODEL_REF = "mainsequence.client.metatables.MetaTable"
CONSTANT_MODEL_REF = "mainsequence.client.models_foundry.Constant"
SECRET_MODEL_REF = "mainsequence.client.models_foundry.Secret"
TEAM_MODEL_REF = "mainsequence.client.models_user.Team"
JOB_RUN_STATUS_PENDING = "PENDING"
JOB_RUN_STATUS_RUNNING = "RUNNING"
RESOURCE_RELEASE_RESOURCE_TYPE_MAP = {
    "streamlit_dashboard": "dashboard",
    "fastapi": "fastapi",
}
RESOURCE_RELEASE_LABEL_MAP = {
    "streamlit_dashboard": "dashboard release",
    "fastapi": "FastAPI release",
}
LIST_FILTER_OPTION_HELP = (
    "Repeatable filter in KEY=VALUE form. "
    "Use --show-filters to inspect the filters supported by this list command."
)


# ---------- AI instructions utilities (kept) ----------

INSTR_REL_PATH = pathlib.Path("examples") / "ai" / "instructions"


def _mainsequence_ascii_banner() -> str:
    return dedent(
        r"""
         __  __       _        
        |  \/  | __ _(_)_ __   
        | |\/| |/ _` | | '_ \  
        | |  | | (_| | | | | | 
        |_|  |_|\__,_|_|_| |_| 

         ____                                 
        / ___|  ___  __ _ _   _  ___ _ __   ___ ___
        \___ \ / _ \/ _` | | | |/ _ \ '_ \ / __/ _ \
         ___) |  __/ (_| | |_| |  __/ | | | (_|  __/
        |____/ \___|\__, |\__,_|\___|_| |_|\___\___|
                      |_|                           
        """
    ).strip("\n")


def _git_root() -> pathlib.Path | None:
    """Return the git repo root (if any), else None."""
    try:
        out = subprocess.check_output(["git", "rev-parse", "--show-toplevel"], text=True).strip()
        return pathlib.Path(out) if out else None
    except Exception:
        return None


def _find_instructions_dir(
    start: pathlib.Path | None = None,
    rel_path: pathlib.Path = INSTR_REL_PATH,
) -> pathlib.Path | None:
    """
    Starting at CWD (or 'start'), walk upward and return the first
    '<ancestor>/examples/ai/instructions' directory.
    """
    start = start or pathlib.Path.cwd()
    for base in [start] + list(start.parents):
        cand = base / rel_path
        if cand.is_dir():
            return cand
    if start.is_dir() and start.name == rel_path.name:
        return start
    return None


def _natural_key(p: pathlib.Path):
    """Natural sort so '10-...' comes after '2-...'."""
    return [int(s) if s.isdigit() else s.lower() for s in re.split(r"(\d+)", p.name)]


def _collect_markdown_files(d: pathlib.Path, recursive: bool = False) -> list[pathlib.Path]:
    """Collect markdown files with optional recursion."""
    patterns = ["*.md", "*.markdown", "*.mdx"]
    files: list[pathlib.Path] = []
    if recursive:
        for pat in patterns:
            files.extend(d.rglob(pat))
    else:
        for pat in patterns:
            files.extend(d.glob(pat))

    seen: set[pathlib.Path] = set()
    uniq: list[pathlib.Path] = []
    for f in files:
        if f not in seen:
            uniq.append(f)
            seen.add(f)
    return sorted(uniq, key=_natural_key)


def _bundle_markdown(
    files: list[pathlib.Path],
    title: str | None = "AI Instructions Bundle",
    repo_root: pathlib.Path | None = None,
) -> str:
    """Bundle markdown files into one document with section headers."""
    repo_root = repo_root or _git_root()
    now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    parts: list[str] = [f"<!-- Bundle generated {now} -->\n"]
    if title:
        parts.append(f"# {title}\n\n")
    for f in files:
        try:
            rel = f.relative_to(repo_root) if repo_root else f
        except Exception:
            rel = f
        header = "\n\n" + ("-" * 80) + f"\n## {rel}\n" + ("-" * 80) + "\n\n"
        parts.append(header)
        txt = f.read_text(encoding="utf-8", errors="replace")
        parts.append(txt.replace("\r\n", "\n").replace("\r", "\n").rstrip() + "\n")
    return "".join(parts)


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


def copy_instructions_to_clipboard(
    instructions_dir: str | os.PathLike[str] | None = None,
    recursive: bool = False,
    also_write_to: str | None = None,
) -> bool:
    """
    Bundle all markdowns under examples/ai/instructions and copy to clipboard.

    Returns:
        True if clipboard copy succeeded; False otherwise (bundle still written to disk).
    """
    base = (
        pathlib.Path(instructions_dir).expanduser().resolve()
        if instructions_dir
        else _find_instructions_dir()
    )
    if not base or not base.is_dir():
        raise RuntimeError(
            "Instructions folder not found. Pass --dir PATH or run from inside your repo."
        )

    files = _collect_markdown_files(base, recursive=recursive)
    if not files:
        raise RuntimeError(f"No markdown files found in: {base}")

    bundle = _bundle_markdown(files, title="AI Instructions", repo_root=_git_root())
    if also_write_to:
        pathlib.Path(also_write_to).write_text(bundle, encoding="utf-8")

    ok = _copy_clipboard(bundle)
    if not ok:
        alt = pathlib.Path.cwd() / "ai_instructions.txt"
        alt.write_text(bundle, encoding="utf-8")
    return ok


# ---------- helpers ----------


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


def _render_code_repositories_table(items: list[dict]) -> str:
    """Return an aligned table with CodeRepository and branch identity."""

    rows = []
    for p in items:
        public_id = _code_repository_identity_value(p)
        name = p.get("code_repository_name") or "(unnamed)"
        branches = (
            ", ".join(
                str(branch.get("repository_branch") or "-")
                for branch in list(p.get("branches") or [])
                if isinstance(branch, dict)
            )
            or "-"
        )

        rows.append((public_id or "-", name, branches))

    header = ["UID", "CodeRepository", "Branches"]
    if not rows:
        return "No CodeRepositories."

    colw = [max(len(str(r[i])) for r in rows + [tuple(header)]) for i in range(len(header))]
    fmt = "  ".join("{:<" + str(colw[i]) + "}" for i in range(len(header)))
    out = [fmt.format(*header), fmt.format(*["-" * len(h) for h in header])]
    for r in rows:
        out.append(fmt.format(*r))
    return "\n".join(out)


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
            get_code_repository_context(code_repository_uid=code_repository_id, code_repository_dir=p)
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
    branches = [item for item in list(code_repository.get("branches") or []) if isinstance(item, dict)]
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


def _resolve_code_repository_branch_uid_for_command(
    code_repository_ref: str | None = None,
    *,
    code_repository_dir: pathlib.Path | None = None,
) -> str:
    _, uid = _resolve_git_code_repository_branch_context(
        code_repository_ref,
        code_repository_dir=code_repository_dir,
    )
    return uid


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


def _code_repository_installed_package_version(code_repository_dir: pathlib.Path, package_name: str) -> str:
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


def _installed_agent_scaffold_skills_dir() -> pathlib.Path:
    skills_dir = _installed_agent_scaffold_bundle_dir() / "skills"
    if not skills_dir.exists() or not skills_dir.is_dir():
        error(f"Installed agent_scaffold skills directory could not be resolved: {skills_dir}")
        raise typer.Exit(1)
    return skills_dir


def _installed_agent_scaffold_skills() -> list[dict[str, pathlib.Path | str]]:
    bundle_dir = _installed_agent_scaffold_bundle_dir()
    skills_dir = _installed_agent_scaffold_skills_dir()
    rows: list[dict[str, pathlib.Path | str]] = []
    for skill_file in sorted(skills_dir.rglob("SKILL.md")):
        skill_dir = skill_file.parent
        skill_name = skill_dir.relative_to(skills_dir).as_posix()
        rows.append(
            {
                "name": skill_name,
                "bundle_dir": bundle_dir,
                "skills_dir": skills_dir,
                "skill_dir": skill_dir,
                "skill_file": skill_file,
            }
        )
    return rows


def _resolve_installed_agent_scaffold_skill(skill_name: str) -> dict[str, pathlib.Path | str]:
    skills = _installed_agent_scaffold_skills()
    if not skills:
        error("No installed agent_scaffold skills were found.")
        raise typer.Exit(1)

    query = skill_name.strip().replace("\\", "/")
    exact_candidates = [row for row in skills if row["name"] == query]
    if len(exact_candidates) == 1:
        return exact_candidates[0]

    dot_query = query.replace(".", "/")
    exact_candidates = [row for row in skills if row["name"] == dot_query]
    if len(exact_candidates) == 1:
        return exact_candidates[0]

    leaf_candidates = [
        row for row in skills if pathlib.PurePosixPath(str(row["name"])).name == query
    ]
    if len(leaf_candidates) == 1:
        return leaf_candidates[0]
    if len(leaf_candidates) > 1:
        error(
            "Skill name is ambiguous. Use one of: "
            + ", ".join(sorted(str(row["name"]) for row in leaf_candidates))
        )
        raise typer.Exit(1)

    suggestions = difflib.get_close_matches(query, [str(row["name"]) for row in skills], n=3)
    detail = f" Did you mean: {', '.join(suggestions)}?" if suggestions else ""
    error(f"Installed agent_scaffold skill not found: {skill_name}.{detail}")
    raise typer.Exit(1)


AGENTS_MD_MANAGED_BLOCK_START_PREFIX = "<!-- mainsequence-agent-scaffold:start"
AGENTS_MD_MANAGED_BLOCK_END = "<!-- mainsequence-agent-scaffold:end -->"
AGENTS_MD_MANAGED_BLOCK_SCHEMA = "1"
AGENTS_MD_MANAGED_BLOCK_START_LINE_RE = re.compile(
    rf"(?m)^[ \t]*{re.escape(AGENTS_MD_MANAGED_BLOCK_START_PREFIX)}\b[^\n]*-->[ \t]*$"
)
AGENTS_MD_MANAGED_BLOCK_END_LINE_RE = re.compile(
    rf"(?m)^[ \t]*{re.escape(AGENTS_MD_MANAGED_BLOCK_END)}[ \t]*$"
)


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


def _parse_env_var_entries(entries: list[str]) -> dict[str, str]:
    """
    Parse env var entries from repeated KEY=VALUE args and/or comma-separated chunks.
    """
    out: dict[str, str] = {}
    for raw in entries:
        for part in str(raw).split(","):
            item = part.strip()
            if not item:
                continue
            if "=" not in item:
                raise ValueError(f"Invalid env var entry '{item}'. Expected KEY=VALUE.")
            key, value = item.split("=", 1)
            key = key.strip()
            if not key:
                raise ValueError(f"Invalid env var entry '{item}'. Empty key.")
            out[key] = value.strip()
    return out


def _format_cli_filter_value(value: object) -> str:
    if isinstance(value, list):
        return ",".join(str(item) for item in value)
    if isinstance(value, dict):
        return json.dumps(value, sort_keys=True)
    if value is None:
        return "-"
    return str(value)


def _resolve_cli_list_filters(
    *,
    model_ref: type | str | None,
    filter_entries: list[str] | None,
    show_filters: bool,
    command_label: str,
    reserved_filter_descriptions: dict[str, object] | None = None,
) -> dict[str, object]:
    reserved_filter_descriptions = dict(reserved_filter_descriptions or {})

    if show_filters:
        rows = build_cli_model_filter_rows(model_ref)
        if rows:
            print_table(
                f"{command_label} Filters",
                ["Filter", "Lookup", "Value Format", "Normalized As"],
                rows,
            )
        else:
            info(f"No additional model filters exposed by {command_label}.")

        if reserved_filter_descriptions:
            print_table(
                "Always Applied Filters",
                ["Filter", "Value"],
                [
                    [key, _format_cli_filter_value(value)]
                    for key, value in reserved_filter_descriptions.items()
                ],
            )
        raise typer.Exit(0)

    try:
        filters = parse_cli_model_filters(model_ref, filter_entries)
    except ValueError as e:
        error(str(e))
        raise typer.Exit(1) from e

    conflicting = sorted(key for key in filters if key in reserved_filter_descriptions)
    if conflicting:
        error(
            "These filters are already enforced by the command and cannot be overridden: "
            + ", ".join(conflicting)
        )
        raise typer.Exit(1)

    return filters


def _merge_cli_filter_alias(
    filters: dict[str, object],
    *,
    filter_key: str,
    value: object | None,
    option_name: str,
) -> dict[str, object]:
    if value is None:
        return filters

    if filter_key in filters:
        error(f"Do not pass both `--{option_name}` and `--filter {filter_key}=...`. Use only one.")
        raise typer.Exit(1)

    merged = dict(filters)
    merged[filter_key] = str(value)
    return merged


def _require_item_uid(item: dict, *, prompt_label: str) -> str:
    uid = str(item.get("uid") or "").strip()
    if not uid:
        raise RuntimeError(f"Available {prompt_label} option is missing uid.")
    return uid


def _prompt_select_uid(
    *,
    title: str,
    prompt_label: str,
    items: list[dict],
    rows: list[list[str]],
) -> str:
    if not items:
        raise RuntimeError(f"No options available for {prompt_label}.")
    print_table(title, ["UID", "Name", "Details"], rows)
    default_uid = _require_item_uid(items[0], prompt_label=prompt_label)
    picked = typer.prompt(prompt_label, default=default_uid).strip()
    if not picked:
        raise RuntimeError(f"Invalid {prompt_label}: {picked}")
    return picked


def _confirm_delete_action(
    *,
    preview_title: str,
    preview_items: list[tuple[str, str]],
    prompt_text: str,
    yes: bool,
) -> None:
    print_kv(preview_title, preview_items)
    if yes:
        return
    if not typer.confirm(prompt_text, default=False):
        info("Cancelled.")
        raise typer.Exit(0)


def _require_delete_verification(
    *,
    preview_title: str,
    preview_items: list[tuple[str, str]],
    verification_value: str,
    verification_label: str,
) -> None:
    print_kv(preview_title, preview_items)
    typed = typer.prompt(
        f"Type {verification_label} '{verification_value}' to confirm deletion",
        default="",
        show_default=False,
    ).strip()
    if typed != verification_value:
        info("Cancelled.")
        raise typer.Exit(0)


def _format_time_index_table_delete_preview(storage: dict[str, object]) -> list[tuple[str, str]]:
    return [
        ("UID", str(storage.get("uid") or "-")),
        ("Physical Table", str(storage.get("physical_table_name") or "-")),
        ("Identifier", str(storage.get("identifier") or "-")),
        ("Source Class", str(storage.get("source_class_name") or "-")),
        ("Data Source", _format_time_index_table_data_source(storage.get("data_source"))),
        ("Protected", str(storage.get("protect_from_deletion"))),
    ]


def _format_meta_table_delete_preview(meta_table: dict[str, object]) -> list[tuple[str, str]]:
    return [
        ("UID", str(meta_table.get("uid") or "-")),
        ("Identifier", str(meta_table.get("identifier") or "-")),
        ("Namespace", str(meta_table.get("namespace") or "-")),
        ("Physical Table", str(meta_table.get("physical_table_name") or "-")),
        ("Management Mode", str(meta_table.get("management_mode") or "-")),
        ("Data Source", _format_time_index_table_data_source(meta_table.get("data_source"))),
        ("Protected", str(meta_table.get("protect_from_deletion"))),
    ]


def _format_code_repository_image_delete_preview(image: dict[str, object]) -> list[tuple[str, str]]:
    return [
        ("UID", str(image.get("uid") or "-")),
        ("CodeRepository Repo Hash", str(image.get("code_repository_commit_hash") or "-")),
        ("Base Image", _format_base_image_label(image.get("base_image"))),
        ("Is Ready", str(image.get("is_ready")) if image.get("is_ready") is not None else "-"),
    ]


def _format_resource_release_delete_preview(release: dict[str, object]) -> list[tuple[str, str]]:
    return [
        ("UID", str(release.get("uid") or "-")),
        ("Release Kind", str(release.get("release_kind") or "-")),
        ("Resource", str(release.get("resource") or "-")),
        ("Related Image", _format_related_image_label(release.get("related_image"))),
    ]


def _git_run(code_repository_dir: pathlib.Path, args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "-C", str(code_repository_dir), *args],
        capture_output=True,
        text=True,
    )


def _git_upstream_ref(code_repository_dir: pathlib.Path) -> str | None:
    result = _git_run(
        code_repository_dir, ["rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{upstream}"]
    )
    if result.returncode != 0:
        return None
    upstream = (result.stdout or "").strip()
    return upstream or None


def _get_remote_branch_head_commit(code_repository_dir: pathlib.Path) -> tuple[str, str]:
    upstream = _git_upstream_ref(code_repository_dir)
    if not upstream:
        raise RuntimeError(
            "Current branch has no upstream remote branch. Push with --set-upstream before listing code repository resources."
        )

    result = _git_run(code_repository_dir, ["rev-parse", upstream])
    if result.returncode != 0:
        reason = (result.stderr or result.stdout or "").strip() or "git rev-parse failed"
        raise RuntimeError(f"Could not resolve remote branch head commit: {reason}")

    commit_sha = (result.stdout or "").strip()
    if not commit_sha:
        raise RuntimeError("Remote branch head commit is empty.")
    return upstream, commit_sha


def _parse_git_log_rows(stdout: str) -> list[dict[str, str]]:
    commits: list[dict[str, str]] = []
    seen: set[str] = set()
    for line in (stdout or "").splitlines():
        full_hash, short_hash, commit_date, subject = (line.split("\t", 3) + ["", "", "", ""])[:4]
        full_hash = full_hash.strip()
        if not full_hash or full_hash in seen:
            continue
        seen.add(full_hash)
        commits.append(
            {
                "hash": full_hash,
                "short_hash": short_hash.strip(),
                "date": commit_date.strip(),
                "subject": subject.strip(),
            }
        )
    return commits


def _list_pushed_commits(code_repository_dir: pathlib.Path, limit: int = 20) -> list[dict[str, str]]:
    """
    List commits already present on the remote-tracking branch.

    Preference order:
      1. current branch upstream
      2. all remote refs
    """
    refs: list[str] = []
    upstream = _git_upstream_ref(code_repository_dir)
    if upstream:
        refs = [upstream]
    else:
        refs_result = _git_run(
            code_repository_dir, ["for-each-ref", "--format=%(refname:short)", "refs/remotes"]
        )
        if refs_result.returncode == 0:
            refs = [
                line.strip()
                for line in (refs_result.stdout or "").splitlines()
                if line.strip() and not line.strip().endswith("/HEAD")
            ]

    if not refs:
        raise RuntimeError(
            "No pushed commits found. Configure a remote and push at least one commit first."
        )

    result = _git_run(
        code_repository_dir,
        [
            "log",
            f"--max-count={max(int(limit), 1)}",
            "--date=format-local:%Y-%m-%d %H:%M:%S",
            "--format=%H%x09%h%x09%ad%x09%s",
            *refs,
        ],
    )
    if result.returncode != 0:
        reason = (result.stderr or result.stdout or "").strip() or "git log failed"
        raise RuntimeError(f"Could not list pushed commits: {reason}")

    commits = _parse_git_log_rows(result.stdout or "")
    if not commits:
        raise RuntimeError(
            "No pushed commits found. Push at least one commit before creating an image."
        )
    return commits


def _list_unpushed_commits(code_repository_dir: pathlib.Path, limit: int = 10) -> list[dict[str, str]]:
    """
    List local commits reachable from HEAD that are not present on any remote ref.
    """
    result = _git_run(
        code_repository_dir,
        [
            "log",
            f"--max-count={max(int(limit), 1)}",
            "--date=format-local:%Y-%m-%d %H:%M:%S",
            "--format=%H%x09%h%x09%ad%x09%s",
            "HEAD",
            "--not",
            "--remotes",
        ],
    )
    if result.returncode != 0:
        return []
    return _parse_git_log_rows(result.stdout or "")


def _is_pushed_commit(code_repository_dir: pathlib.Path, commit_hash: str) -> bool:
    result = _git_run(code_repository_dir, ["branch", "-r", "--contains", commit_hash])
    if result.returncode != 0:
        return False
    refs = [
        line.strip()
        for line in (result.stdout or "").splitlines()
        if line.strip() and not line.strip().endswith("/HEAD")
    ]
    return bool(refs)


def _resolve_full_commit_hash(code_repository_dir: pathlib.Path, commit_hash: str) -> str:
    normalized = str(commit_hash or "").strip()
    if not normalized:
        raise RuntimeError("code_repository_commit_hash is required.")

    result = _git_run(code_repository_dir, ["rev-parse", "--verify", f"{normalized}^{{commit}}"])
    if result.returncode != 0:
        reason = (result.stderr or result.stdout or "").strip() or "git rev-parse failed"
        raise RuntimeError(f"Could not resolve code_repository_commit_hash to a full commit SHA: {reason}")

    full_hash = (result.stdout or "").strip()
    if not re.fullmatch(r"[0-9a-fA-F]{40}", full_hash):
        raise RuntimeError("Resolved code_repository_commit_hash is not a full 40-character commit SHA.")

    return full_hash.lower()


def _group_code_repository_images_by_hash(images: list[dict]) -> dict[str, list[dict]]:
    grouped: dict[str, list[dict]] = {}
    for image in images:
        commit_hash = str(image.get("code_repository_commit_hash") or "").strip()
        if not commit_hash:
            continue
        grouped.setdefault(commit_hash, []).append(image)
    return grouped


def _format_base_image_label(value) -> str:
    if isinstance(value, dict):
        return str(value.get("title") or value.get("uid") or "-")
    if value is None:
        return "-"
    return str(value)


def _format_image_uids(images: list[dict]) -> str:
    uids = [str(img.get("uid")) for img in images if img.get("uid") is not None]
    return ", ".join(uids) if uids else "-"


def _format_related_image_label(value) -> str:
    if isinstance(value, dict):
        return str(value.get("uid") or value.get("title") or "-")
    if value is None:
        return "-"
    return str(value)


def _format_nested_summary(
    value,
    *,
    preferred_fields: tuple[str, ...],
) -> str:
    if isinstance(value, dict):
        for field_name in preferred_fields:
            field_value = value.get(field_name)
            if field_value not in (None, ""):
                return str(field_value)
        return "-"
    if value is None:
        return "-"
    return str(value)


def _format_time_index_table_data_source(value) -> str:
    if isinstance(value, dict):
        display_name = str(value.get("display_name") or "").strip()
        class_type = str(value.get("class_type") or "").strip()
        if display_name and class_type:
            return f"{display_name} ({class_type})"
        if display_name:
            return display_name
        if class_type:
            return class_type
        if value.get("uid") is not None:
            return str(value.get("uid"))
        return "-"
    if value is None:
        return "-"
    return str(value)


def _format_json_value(value) -> str:
    if value in (None, "", [], {}):
        return "-"
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, indent=2, sort_keys=True)
    except Exception:
        return str(value)


def _find_image_by_uid(images: list[dict], image_uid: str | None) -> dict | None:
    if image_uid is None:
        return None
    return next((img for img in images if str(img.get("uid")) == str(image_uid)), None)


def _format_job_schedule_summary(task_schedule) -> str:
    if task_schedule is None:
        return "-"

    if not isinstance(task_schedule, dict):
        return str(task_schedule)

    name = str(task_schedule.get("name") or "").strip()
    schedule = task_schedule.get("schedule")
    prefix = f"{name}: " if name else ""

    if isinstance(schedule, dict):
        schedule_type = str(schedule.get("type") or "").strip().lower()
        if schedule_type == "crontab":
            expr = str(schedule.get("expression") or "").strip() or "-"
            return f"{prefix}cron {expr}"
        if schedule_type == "interval":
            every = schedule.get("every")
            period = str(schedule.get("period") or "").strip() or "units"
            if every is not None:
                return f"{prefix}every {every} {period}"
            return f"{prefix}interval"

    task_name = str(task_schedule.get("task") or "").strip()
    if prefix or task_name:
        return f"{prefix}{task_name}".strip() or "-"
    return "-"


def _resolve_job_create_defaults(
    *,
    cpu_request: str | None,
    memory_request: str | None,
    spot: bool | None,
    max_runtime_seconds: int | None,
) -> tuple[str, str, bool, int, list[str]]:
    cpu_request, memory_request, resolved_spot, used_defaults = _resolve_compute_defaults(
        cpu_request=cpu_request,
        memory_request=memory_request,
        spot=spot,
    )

    if max_runtime_seconds is None:
        resolved_max_runtime_seconds = JOB_DEFAULT_MAX_RUNTIME_SECONDS
        used_defaults.append("max_runtime_seconds")
    else:
        resolved_max_runtime_seconds = int(max_runtime_seconds)
        if resolved_max_runtime_seconds <= 0:
            raise ValueError("max_runtime_seconds must be a positive integer.")

    return (
        cpu_request,
        memory_request,
        resolved_spot,
        resolved_max_runtime_seconds,
        used_defaults,
    )


def _resolve_compute_defaults(
    *,
    cpu_request: str | None,
    memory_request: str | None,
    spot: bool | None,
) -> tuple[str, str, bool, list[str]]:
    cpu = parse_cpu_request(cpu_request, field_name="cpu_request")
    memory = parse_memory_request(memory_request, field_name="memory_request")
    used_defaults: list[str] = []

    if cpu is None and memory is None:
        cpu = JOB_DEFAULT_CPU_REQUEST
        memory = JOB_DEFAULT_MEMORY_REQUEST
        used_defaults.extend(["cpu_request", "memory_request"])
    elif cpu is None:
        derived_cpu = (memory / JOB_MEMORY_PER_CPU_MAX).quantize(Decimal("0.01"), rounding=ROUND_UP)
        cpu = max(JOB_DEFAULT_CPU_REQUEST, derived_cpu)
        used_defaults.append("cpu_request")
    elif memory is None:
        memory = max(JOB_DEFAULT_MEMORY_REQUEST, cpu)
        used_defaults.append("memory_request")

    resolved_spot = JOB_DEFAULT_SPOT if spot is None else spot
    if spot is None:
        used_defaults.append("spot")

    return (
        decimal_to_storage(cpu),
        decimal_to_storage(memory),
        resolved_spot,
        used_defaults,
    )


def _parse_schedule_start_time(value: str | None) -> datetime.datetime | None:
    if value is None:
        return None
    raw = value.strip()
    if not raw:
        return None
    normalized = raw.replace("Z", "+00:00")
    try:
        return datetime.datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError("schedule_start_time must be a valid ISO datetime.") from exc


def _build_job_task_schedule_payload(
    *,
    schedule_type: str | None,
    schedule_every: int | None,
    schedule_period: str | None,
    schedule_expression: str | None,
    schedule_start_time: str | None,
    schedule_one_off: bool | None,
    prompt_for_missing: bool,
) -> dict[str, object] | None:
    inferred_type = (schedule_type or "").strip().lower() or None
    if inferred_type is None:
        if schedule_expression:
            inferred_type = "crontab"
        elif schedule_every is not None or schedule_period:
            inferred_type = "interval"

    if inferred_type is None and prompt_for_missing:
        if not typer.confirm(
            pydantic_prompt_text(
                JOB_MODEL_REF, "task_schedule", optional=True, extra_hint="create now?"
            ),
            default=False,
        ):
            return None
        inferred_type = (
            typer.prompt(
                pydantic_prompt_text(
                    INTERVAL_SCHEDULE_MODEL_REF,
                    "type",
                    extra_hint="interval/crontab",
                ),
                default="interval",
            )
            .strip()
            .lower()
        )

    if inferred_type is None:
        return None

    if inferred_type not in {"interval", "crontab"}:
        raise ValueError("schedule_type must be either 'interval' or 'crontab'.")

    if inferred_type == "interval":
        if schedule_expression:
            raise ValueError("schedule_expression cannot be used with interval schedules.")
        if schedule_every is None and prompt_for_missing:
            schedule_every = int(
                typer.prompt(
                    pydantic_prompt_text(INTERVAL_SCHEDULE_MODEL_REF, "every"),
                    default="1",
                ).strip()
            )
        if schedule_period is None and prompt_for_missing:
            schedule_period = typer.prompt(
                pydantic_prompt_text(
                    INTERVAL_SCHEDULE_MODEL_REF,
                    "period",
                    extra_hint="seconds/minutes/hours/days",
                ),
                default="hours",
            ).strip()
        if schedule_every is None:
            raise ValueError("schedule_every is required for interval schedules.")
        if schedule_every <= 0:
            raise ValueError("schedule_every must be greater than 0.")
        normalized_period = str(schedule_period or "").strip().lower()
        if normalized_period not in JOB_ALLOWED_INTERVAL_PERIODS:
            raise ValueError(
                "schedule_period must be one of: " + ", ".join(JOB_ALLOWED_INTERVAL_PERIODS) + "."
            )
        schedule_payload: dict[str, object] = {
            "type": "interval",
            "every": int(schedule_every),
            "period": normalized_period,
        }
    else:
        if schedule_every is not None or schedule_period:
            raise ValueError(
                "schedule_every and schedule_period are only valid for interval schedules."
            )
        if schedule_expression is None and prompt_for_missing:
            schedule_expression = typer.prompt(
                pydantic_prompt_text(
                    CRONTAB_SCHEDULE_MODEL_REF,
                    "expression",
                ),
                default="0 * * * *",
            ).strip()
        expression = str(schedule_expression or "").strip()
        if not expression:
            raise ValueError("schedule_expression is required for crontab schedules.")
        if len(expression.split()) != 5:
            raise ValueError(
                "schedule_expression must have 5 crontab fields: minute hour day_of_month month_of_year day_of_week."
            )
        schedule_payload = {
            "type": "crontab",
            "expression": expression,
        }

    if schedule_start_time is None and prompt_for_missing:
        schedule_start_time = (
            typer.prompt(
                pydantic_prompt_text(CRONTAB_SCHEDULE_MODEL_REF, "start_time", optional=True),
                default="",
            ).strip()
            or None
        )

    if schedule_one_off is None and prompt_for_missing:
        schedule_one_off = typer.confirm("Make this a one-off schedule?", default=False)

    payload: dict[str, object] = {"schedule": schedule_payload}

    parsed_start_time = _parse_schedule_start_time(schedule_start_time)
    if parsed_start_time is not None:
        payload["start_time"] = parsed_start_time
    if schedule_one_off is not None:
        payload["one_off"] = bool(schedule_one_off)

    return payload


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


# ---------- top-level commands ----------


@app.command()
def login(
    backend: str | None = typer.Argument(
        None,
        help="Optional backend URL or host[:port], for example 127.0.0.1:8000.",
    ),
    code_repositories_base: str | None = typer.Argument(
        None,
        help="Optional local CodeRepositories base folder, for example mainsequence-dev.",
    ),
    access_token: str | None = typer.Option(None, "--access-token", help="JWT access token."),
    refresh_token: str | None = typer.Option(None, "--refresh-token", help="JWT refresh token."),
    no_open: bool = typer.Option(
        False,
        "--no-open",
        help="Do not auto-open a browser. Print the authorization URL and wait for callback.",
    ),
    mcp: bool = typer.Option(
        False,
        "--mcp",
        help=(
            "Authorize this CLI through the already-authenticated Main Sequence "
            "MCP principal instead of opening a browser."
        ),
    ),
    mcp_timeout_seconds: int = typer.Option(
        300,
        "--mcp-timeout-seconds",
        min=1,
        help="Seconds to wait for the MCP host to authorize the CLI handoff.",
    ),
    backend_option: str | None = typer.Option(
        None,
        "--backend",
        help="Backend URL or host[:port], for example http://127.0.0.1:8000.",
    ),
    code_repositories_base_option: str | None = typer.Option(
        None,
        "--code-repositories-base",
        "--base-folder",
        help="Local CodeRepositories base folder for this terminal session, for example mainsequence-dev.",
    ),
    no_status: bool = typer.Option(
        False,
        "--no-status",
        hidden=True,
        help="Deprecated no-op kept only for backward compatibility.",
    ),
    export: bool = typer.Option(
        False,
        "--export",
        "--export-env",
        help="Print shell export commands for session auth variables.",
    ),
):
    """
    Authenticate to the MainSequence platform.

    Persists auth tokens in the active CLI auth store so subsequent
    CLI invocations can run without re-authentication. Backend/base-folder
    overrides passed to `login` are scoped to the current terminal session.
    When no backend is provided, login uses the currently configured backend.

    Interactive login uses browser-based authentication and finishes with
    standard JWT access/refresh tokens persisted by the CLI.

    When a backend-launched process already has
    `MAINSEQUENCE_AUTH_MODE=runtime_credential`, login exchanges the injected
    runtime credential for a short-lived access token instead of opening the
    browser or persisting CLI JWT tokens. This is not a user branch/runtime
    selection mechanism.

    Parameters
    ----------
    backend:
        Optional positional backend override for backward compatibility.
    code_repositories_base:
        Optional positional CodeRepositories base folder.
    access_token:
        JWT access token for manual token import.
    refresh_token:
        JWT refresh token for manual token import.
    no_open:
        If True, do not auto-open a browser. The CLI prints the auth URL.
    mcp:
        If True, use a backend-issued handoff approved by auth.cli_authorize.
    mcp_timeout_seconds:
        Maximum time to wait for the MCP authorization handoff.
    backend_option:
        Backend override for this terminal session.
    code_repositories_base_option:
        CodeRepositories base-folder override for this terminal session.
    export:
        If True, print shell export lines for auth variables.

    Examples
    --------
    ```bash
    mainsequence login
    mainsequence login 127.0.0.1:8000 mainsequence-dev
    mainsequence login --no-open
    mainsequence login --mcp
    mainsequence login --access-token "$TOKEN" --refresh-token "$REFRESH"
    mainsequence login --access-token "$TOKEN" --refresh-token "$REFRESH" --backend http://127.0.0.1:8000 --code-repositories-base mainsequence-dev
    mainsequence login --export
    ```
    """
    using_jwt = bool((access_token or "").strip() or (refresh_token or "").strip())
    using_runtime_credential = _runtime_credential_mode_enabled()

    if using_runtime_credential and using_jwt:
        error("Runtime credential login cannot be combined with --access-token/--refresh-token.")
        raise typer.Exit(1)
    if using_runtime_credential and mcp:
        error(
            "MCP handoff login cannot be used when "
            "MAINSEQUENCE_AUTH_MODE=runtime_credential. Run `mainsequence login` instead."
        )
        raise typer.Exit(1)

    if mcp and using_jwt:
        error("MCP handoff login cannot be combined with --access-token/--refresh-token.")
        raise typer.Exit(1)
    if mcp and export:
        error("MCP handoff login cannot be combined with --export.")
        raise typer.Exit(1)

    if using_runtime_credential and no_open:
        warn("--no-open is ignored when MAINSEQUENCE_AUTH_MODE=runtime_credential.")
    if mcp and no_open:
        warn("--no-open is ignored during MCP handoff login.")

    if not using_jwt and backend and "@" in backend:
        error(
            "Email/password CLI login was removed. Use `mainsequence login` for browser login "
            "or use --access-token/--refresh-token for manual JWT import."
        )
        raise typer.Exit(1)

    if backend and backend_option:
        if cfg.normalize_backend_url(backend) != cfg.normalize_backend_url(backend_option):
            error("Pass backend either positionally or with --backend, not both.")
            raise typer.Exit(1)
    explicit_backend_input = backend_option if backend_option is not None else backend
    current_backend = cfg.backend_url()
    effective_backend_input = (
        explicit_backend_input if explicit_backend_input is not None else current_backend
    )

    if code_repositories_base and code_repositories_base_option:
        if cfg.normalize_mainsequence_path(code_repositories_base) != cfg.normalize_mainsequence_path(
            code_repositories_base_option
        ):
            error(
                "Pass the CodeRepositories base either positionally or with "
                "--code-repositories-base/--base-folder, not both."
            )
            raise typer.Exit(1)
    effective_code_repositories_base_input = (
        code_repositories_base_option
        if code_repositories_base_option is not None
        else code_repositories_base
    )

    if using_jwt:
        if not (access_token or "").strip() or not (refresh_token or "").strip():
            error("JWT login requires both --access-token and --refresh-token.")
            raise typer.Exit(1)
    elif access_token is not None or refresh_token is not None:
        error("JWT login requires both --access-token and --refresh-token.")
        raise typer.Exit(1)

    normalized_backend = cfg.normalize_backend_url(effective_backend_input)

    if explicit_backend_input is not None and normalized_backend != current_backend:
        if not effective_code_repositories_base_input:
            error(
                "When using a different backend, you must also specify a "
                "CodeRepositories base folder."
            )
            raise typer.Exit(1)

    previous_backend_override = os.environ.get("MAINSEQUENCE_ENDPOINT")
    os.environ["MAINSEQUENCE_ENDPOINT"] = normalized_backend

    try:
        if using_runtime_credential:
            access = _exchange_runtime_credential_for_cli_login(normalized_backend)
            persisted = cfg.save_tokens("", access, "")
            res = {
                "username": "",
                "backend": normalized_backend,
                "access": access,
                "refresh": "",
                "persisted": bool(persisted),
                "auth_mode": "runtime_credential",
            }
        elif using_jwt:
            os.environ.pop(cfg.ENV_USERNAME, None)
            os.environ.pop(cfg.LEGACY_ENV_USERNAME, None)
            persisted = cfg.save_tokens(
                "", (access_token or "").strip(), (refresh_token or "").strip()
            )
            res = {
                "username": "",
                "backend": normalized_backend,
                "access": (access_token or "").strip(),
                "refresh": (refresh_token or "").strip(),
                "persisted": bool(persisted),
                "auth_mode": "jwt",
            }
        elif mcp:

            def _emit_mcp_handoff(handoff: dict) -> None:
                tool_call = {
                    "tool": handoff["mcp_tool"],
                    "arguments": handoff["mcp_arguments"],
                }
                info("Authorize this CLI from the connected Main Sequence MCP session:")
                typer.echo(json.dumps(tool_call, separators=(",", ":")))

            flow = login_via_mcp_handoff(
                timeout_seconds=mcp_timeout_seconds,
                on_handoff=_emit_mcp_handoff,
            )
            access = (flow.get("access") or "").strip()
            refresh = (flow.get("refresh") or "").strip()
            if not access or not refresh:
                raise ApiError("MCP handoff login did not return access and refresh tokens.")

            persisted = cfg.save_tokens("", access, refresh)
            user_payload = flow.get("user")
            username = ""
            if isinstance(user_payload, dict):
                username = str(user_payload.get("username") or "").strip()
            if not username:
                profile = get_current_user_profile()
                if isinstance(profile, dict):
                    username = (profile.get("username") or "").strip()
            if username:
                persisted = bool(cfg.save_tokens(username, access, refresh) and persisted)

            res = {
                "username": username,
                "backend": normalized_backend,
                "access": access,
                "refresh": refresh,
                "persisted": bool(persisted),
                "auth_mode": "jwt",
            }
        else:

            def _emit_auth_url(url: str) -> None:
                info(f"Open this URL to authenticate: {url}")

            flow = login_via_browser(
                no_open=no_open,
                on_authorize_url=_emit_auth_url if no_open else None,
            )
            access = (flow.get("access") or "").strip()
            refresh = (flow.get("refresh") or "").strip()
            if not access or not refresh:
                raise ApiError("Browser login did not return access and refresh tokens.")

            persisted = cfg.save_tokens("", access, refresh)
            username = ""
            profile = get_current_user_profile()
            if isinstance(profile, dict):
                username = (profile.get("username") or "").strip()
            if username:
                persisted = bool(cfg.save_tokens(username, access, refresh) and persisted)

            res = {
                "username": username,
                "backend": normalized_backend,
                "access": access,
                "refresh": refresh,
                "persisted": bool(persisted),
                "auth_mode": "jwt",
            }
    except BrowserAuthError as e:
        login_kind = "MCP handoff" if mcp else "Browser"
        error(f"{login_kind} login failed: {e}")
        raise typer.Exit(1) from e
    except ApiError as e:
        error(f"Login failed: {e}")
        raise typer.Exit(1) from e
    finally:
        if previous_backend_override is None:
            os.environ.pop("MAINSEQUENCE_ENDPOINT", None)
        else:
            os.environ["MAINSEQUENCE_ENDPOINT"] = previous_backend_override

    cfg.set_session_overrides(
        backend_url=normalized_backend,
        mainsequence_path=effective_code_repositories_base_input,
    )

    if export:
        access = (res.get("access") or "").replace('"', '\\"')
        refresh = (res.get("refresh") or "").replace('"', '\\"')
        username = (res.get("username") or "").replace('"', '\\"')
        auth_mode = (res.get("auth_mode") or "").replace('"', '\\"')
        if auth_mode:
            typer.echo(f'export MAINSEQUENCE_AUTH_MODE="{auth_mode}"')
        typer.echo(f'export MAINSEQUENCE_ACCESS_TOKEN="{access}"')
        if refresh:
            typer.echo(f'export MAINSEQUENCE_REFRESH_TOKEN="{refresh}"')
        if username:
            typer.echo(f'export MAINSEQUENCE_USERNAME="{username}"')
        return

    cfg_obj = cfg.get_config()
    base = cfg_obj["mainsequence_path"]
    typer.echo(_mainsequence_ascii_banner())
    typer.echo("MAIN SEQUENCE")
    if res.get("username"):
        success(f"Signed in as {res['username']} (Backend: {res['backend']})")
    elif res.get("auth_mode") == "runtime_credential":
        success(f"Signed in with runtime credential (Backend: {res['backend']})")
    else:
        success(f"Signed in with JWT tokens (Backend: {res['backend']})")
    info(f"CodeRepositories base folder: {base}")
    auth_store_label = cfg.auth_persistence_label()
    if res.get("auth_mode") == "runtime_credential":
        info(
            f"Runtime credential access token is persisted in {auth_store_label}; no CLI JWT refresh token exists."
        )
        info(
            "When the access token expires, CLI will re-exchange the runtime credential automatically."
        )
    elif res.get("persisted", True):
        info(f"Auth tokens are persisted in {auth_store_label} for subsequent CLI commands.")
    else:
        warn(
            f"Could not persist auth tokens in {auth_store_label}. Use --export for shell-based auth."
        )


@app.command("logout")
def logout(
    export: bool = typer.Option(
        False,
        "--export",
        "--export-env",
        help="Print shell unset commands for session auth variables.",
    ),
):
    """
    Log out by clearing stored/session authentication state.

    Parameters
    ----------
    export:
        If True, print shell `unset` lines for auth variables.

    Examples
    --------
    ```bash
    mainsequence logout
    mainsequence logout --export
    ```
    """
    cfg.clear_session_overrides()
    backend_logout_result = {
        "attempted": False,
        "revoked": False,
        "method": "local_only",
        "detail": "",
    }
    try:
        backend_logout_result = logout_cli_session()
    except Exception as exc:
        backend_logout_result = {
            "attempted": True,
            "revoked": False,
            "method": "error",
            "detail": str(exc),
        }

    ok = cfg.clear_tokens()
    if export:
        typer.echo("unset MAINSEQUENCE_ACCESS_TOKEN")
        typer.echo("unset MAINSEQUENCE_REFRESH_TOKEN")
        typer.echo("unset MAINSEQUENCE_USERNAME")
        typer.echo("unset MAIN_SEQUENCE_USER_TOKEN")
        typer.echo("unset MAIN_SEQUENCE_REFRESH_TOKEN")
        typer.echo("unset MAIN_SEQUENCE_USERNAME")
        return

    if ok:
        if backend_logout_result.get("revoked"):
            success("Signed out (backend session revoked, local tokens cleared).")
        elif backend_logout_result.get("attempted"):
            warn(
                "Signed out locally, but backend session revoke could not be confirmed."
                + (
                    f" Detail: {backend_logout_result.get('detail')}"
                    if backend_logout_result.get("detail")
                    else ""
                )
            )
        else:
            success("Signed out (session tokens cleared).")
    else:
        warn("Signed out, but some session auth variables could not be cleared.")


@app.command("doctor")
def doctor():
    """
    Print a diagnostics report for the CLI environment.

    The report includes config paths, backend URL, auth visibility, and
    external tool availability.

    Examples
    --------
    ```bash
    mainsequence doctor
    ```
    """
    run_doctor()


@app.command("user")
def user_show():
    """
    Show the authenticated MainSequence user.

    Uses SDK client `User.get_authenticated_user_details()` as the single source of truth.

    Examples
    --------
    ```bash
    mainsequence user
    ```
    """
    try:
        user = get_logged_user_details()
    except NotLoggedIn as e:
        error("Not logged in. Run: mainsequence login")
        raise typer.Exit(1) from e
    except ApiError as e:
        error(str(e))
        raise typer.Exit(1) from e

    if _emit_json(user):
        return

    organization = user.get("organization")
    if isinstance(organization, dict):
        organization_name = str(organization.get("name") or organization.get("uid") or "-")
    else:
        organization_name = str(organization or "-")

    print_kv(
        "MainSequence User",
        [
            ("UID", str(user.get("uid") or "-")),
            ("Username", str(user.get("username") or "-")),
            ("Email", str(user.get("email") or "-")),
            ("Organization", organization_name),
            ("Active", str(user.get("is_active") if user.get("is_active") is not None else "-")),
            (
                "Verified",
                str(user.get("is_verified") if user.get("is_verified") is not None else "-"),
            ),
            (
                "MFA Enabled",
                str(user.get("mfa_enabled") if user.get("mfa_enabled") is not None else "-"),
            ),
            ("Date Joined", str(user.get("date_joined") or "-")),
            ("Last Login", str(user.get("last_login") or "-")),
        ],
    )


@organization.command("github-organizations")
def organization_github_organizations_cmd():
    """
    List GitHub organizations available to the authenticated user.

    Examples
    --------
    ```bash
    mainsequence organization github-organizations
    mainsequence organization github-organizations --json
    ```
    """
    _require_login()

    try:
        organizations = list_github_organizations()
    except ApiError as e:
        error(f"GitHub organizations fetch failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(organizations):
        return

    if organizations:
        print_table(
            "GitHub Organizations",
            ["UID", "Name", "Login"],
            [
                [
                    str(org.get("uid") or "-"),
                    str(org.get("display_name") or "-"),
                    str(org.get("login") or "-"),
                ]
                for org in organizations
            ],
        )
    else:
        info("No GitHub organizations available.")
    info(f"Total GitHub organizations: {len(organizations)}")


def _organization_teams_list_impl(
    *,
    timeout: int | None,
    filter_entries: list[str] | None,
    show_filters: bool,
) -> None:
    filters = _resolve_cli_list_filters(
        model_ref=TEAM_MODEL_REF,
        filter_entries=filter_entries,
        show_filters=show_filters,
        command_label="Organization Teams",
    )
    _require_login()

    try:
        teams_payload = list_organization_teams(timeout=timeout, filters=filters)
    except ApiError as e:
        error(f"Organization teams fetch failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(teams_payload):
        return

    rows: list[list[str]] = []
    for team in teams_payload:
        member_count = team.get("member_count")
        if member_count in (None, ""):
            members = team.get("members")
            member_count = len(members) if isinstance(members, list) else "-"
        rows.append(
            [
                str(team.get("uid") or "-"),
                str(team.get("name") or "-"),
                str(team.get("description") or "-"),
                str(member_count),
                str(team.get("is_active")) if team.get("is_active") is not None else "-",
            ]
        )

    if rows:
        print_table("Organization Teams", ["UID", "Name", "Description", "Members", "Active"], rows)
    else:
        info("No organization teams.")
    info(f"Total organization teams: {len(teams_payload)}")


def _organization_teams_create_impl(
    *,
    name: str | None,
    description: str | None,
    timeout: int | None,
) -> None:
    _require_login()

    team_name = (name or "").strip() or typer.prompt("Team name").strip()
    if not team_name:
        error("Team name is required.")
        raise typer.Exit(1)

    team_description = description
    if team_description is None:
        team_description = typer.prompt("Team description", default="", show_default=False)

    try:
        created = create_organization_team(
            name=team_name,
            description=team_description or "",
            timeout=timeout,
        )
    except ApiError as e:
        error(f"Organization team creation failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(created):
        return

    success(f"Organization team created: {team_name}")
    print_kv("Created Team", _format_team_preview(created))


def _organization_teams_edit_impl(
    *,
    team_uid: str,
    name: str | None,
    description: str | None,
    is_active: bool | None,
    timeout: int | None,
) -> None:
    _require_login()

    try:
        current = get_organization_team(team_uid, timeout=timeout)
    except ApiError as e:
        error(f"Organization team fetch failed: {e}")
        raise typer.Exit(1) from e

    next_name = name
    next_description = description
    next_active = is_active

    if next_name is None and next_description is None and next_active is None:
        next_name = typer.prompt(
            "Team name", default=str(current.get("name") or ""), show_default=True
        ).strip()
        next_description = typer.prompt(
            "Team description",
            default=str(current.get("description") or ""),
            show_default=True,
        )
        current_active = (
            bool(current.get("is_active")) if current.get("is_active") is not None else True
        )
        next_active = typer.confirm("Team is active?", default=current_active)

    try:
        updated = update_organization_team(
            team_uid,
            name=next_name,
            description=next_description,
            is_active=next_active,
            timeout=timeout,
        )
    except ApiError as e:
        error(f"Organization team update failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(updated):
        return

    success(f"Organization team updated: uid={team_uid}")
    print_kv("Updated Team", _format_team_preview(updated))


def _organization_teams_delete_impl(
    *,
    team_uid: str,
    timeout: int | None,
) -> None:
    _require_login()

    try:
        team = get_organization_team(team_uid, timeout=timeout)
    except ApiError as e:
        error(f"Organization team fetch failed: {e}")
        raise typer.Exit(1) from e

    verification_value = str(team.get("name") or team.get("uid") or team_uid)
    _require_delete_verification(
        preview_title="Organization Team Delete Preview",
        preview_items=_format_team_preview(team),
        verification_value=verification_value,
        verification_label="team name" if team.get("name") else "team UID",
    )

    try:
        deleted = delete_organization_team(team_uid, timeout=timeout)
    except ApiError as e:
        error(f"Organization team deletion failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(deleted):
        return

    success(f"Organization team deleted: uid={team_uid}")
    print_kv("Deleted Team", _format_team_preview(deleted))


@organization_teams_group.command("list")
def organization_teams_list_cmd(
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
    filter_entries: list[str] | None = typer.Option(None, "--filter", help=LIST_FILTER_OPTION_HELP),
    show_filters: bool = typer.Option(
        False, "--show-filters", help="Show supported list filters and exit."
    ),
):
    """
    List organization teams visible to the authenticated user.
    """
    _organization_teams_list_impl(
        timeout=timeout,
        filter_entries=filter_entries,
        show_filters=show_filters,
    )


@organization_teams_group.command("create")
def organization_teams_create_cmd(
    name: str | None = typer.Argument(None, help="Team name."),
    description: str | None = typer.Option(
        None, "--description", help="Optional team description."
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Create one organization team.
    """
    _organization_teams_create_impl(name=name, description=description, timeout=timeout)


@organization_teams_group.command("edit")
def organization_teams_edit_cmd(
    team_uid: str = pydantic_argument(TEAM_MODEL_REF, "uid", ..., help="Team UID."),
    name: str | None = typer.Option(None, "--name", help="New team name."),
    description: str | None = typer.Option(None, "--description", help="New team description."),
    is_active: bool | None = typer.Option(None, "--active/--inactive", help="Set active status."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Edit one organization team.
    """
    _organization_teams_edit_impl(
        team_uid=team_uid,
        name=name,
        description=description,
        is_active=is_active,
        timeout=timeout,
    )


@organization_teams_group.command("delete")
def organization_teams_delete_cmd(
    team_uid: str = pydantic_argument(TEAM_MODEL_REF, "uid", ..., help="Team UID."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Delete one organization team.
    """
    _organization_teams_delete_impl(team_uid=team_uid, timeout=timeout)


@organization_teams_group.command("can_view")
def organization_teams_can_view_cmd(
    team_uid: str = pydantic_argument(TEAM_MODEL_REF, "uid", ..., help="Team UID."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_user_list_impl(
        fetch_fn=list_team_users_can_view,
        object_label="Team",
        access_label="view",
        object_uid=team_uid,
        timeout=timeout,
    )


@organization_teams_group.command("can_edit")
def organization_teams_can_edit_cmd(
    team_uid: str = pydantic_argument(TEAM_MODEL_REF, "uid", ..., help="Team UID."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_user_list_impl(
        fetch_fn=list_team_users_can_edit,
        object_label="Team",
        access_label="edit",
        object_uid=team_uid,
        timeout=timeout,
    )


@organization_teams_group.command("add_to_view")
def organization_teams_add_to_view_cmd(
    team_uid: str = pydantic_argument(TEAM_MODEL_REF, "uid", ..., help="Team UID."),
    user_uid: uuid.UUID = typer.Argument(..., help="User UID to grant view access."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_user_access_update_impl(
        action_fn=add_team_user_to_view,
        object_label="Team",
        action_label="add_to_view",
        object_uid=team_uid,
        user_uid=user_uid,
        timeout=timeout,
    )


@organization_teams_group.command("add_to_edit")
def organization_teams_add_to_edit_cmd(
    team_uid: str = pydantic_argument(TEAM_MODEL_REF, "uid", ..., help="Team UID."),
    user_uid: uuid.UUID = typer.Argument(..., help="User UID to grant edit access."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_user_access_update_impl(
        action_fn=add_team_user_to_edit,
        object_label="Team",
        action_label="add_to_edit",
        object_uid=team_uid,
        user_uid=user_uid,
        timeout=timeout,
    )


@organization_teams_group.command("remove_from_view")
def organization_teams_remove_from_view_cmd(
    team_uid: str = pydantic_argument(TEAM_MODEL_REF, "uid", ..., help="Team UID."),
    user_uid: uuid.UUID = typer.Argument(..., help="User UID to remove explicit view access from."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_user_access_update_impl(
        action_fn=remove_team_user_from_view,
        object_label="Team",
        action_label="remove_from_view",
        object_uid=team_uid,
        user_uid=user_uid,
        timeout=timeout,
    )


@organization_teams_group.command("remove_from_edit")
def organization_teams_remove_from_edit_cmd(
    team_uid: str = pydantic_argument(TEAM_MODEL_REF, "uid", ..., help="Team UID."),
    user_uid: uuid.UUID = typer.Argument(..., help="User UID to remove explicit edit access from."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_user_access_update_impl(
        action_fn=remove_team_user_from_edit,
        object_label="Team",
        action_label="remove_from_edit",
        object_uid=team_uid,
        user_uid=user_uid,
        timeout=timeout,
    )


@app.command("copy-llm-instructions")
def copy_llm_instructions(
    dir: str | None = typer.Option(
        None,
        "--dir",
        "-d",
        help="Path to examples/ai/instructions. If omitted, search upward from CWD.",
    ),
    recursive: bool = typer.Option(False, "--recursive", "-r", help="Include nested subfolders."),
    out: str | None = typer.Option(None, "--out", "-o", help="Also write the bundle to this file."),
    print_: bool = typer.Option(
        False, "--print", help="Print the bundle to stdout instead of copying."
    ),
):
    """
    Bundle markdown instructions and copy them to clipboard or print them.

    Parameters
    ----------
    dir:
        Explicit path to instructions directory.
    recursive:
        Include nested markdown files.
    out:
        Optional output file path for the generated bundle.
    print_:
        Print bundle to stdout instead of copying.

    Examples
    --------
    ```bash
    mainsequence copy-llm-instructions
    mainsequence copy-llm-instructions --recursive
    mainsequence copy-llm-instructions --dir ./examples/ai/instructions --print
    mainsequence copy-llm-instructions --out ./ai_instructions.txt
    ```
    """
    try:
        base = pathlib.Path(dir).expanduser().resolve() if dir else None
        if print_:
            found = base or _find_instructions_dir()
            if not found:
                error(
                    "Instructions folder not found. Pass --dir PATH or run from inside your repo."
                )
                raise typer.Exit(1)
            files = _collect_markdown_files(found, recursive=recursive)
            if not files:
                error(f"No markdown files found in: {found}")
                raise typer.Exit(1)
            bundle = _bundle_markdown(files, title="AI Instructions", repo_root=_git_root())
            if out:
                pathlib.Path(out).write_text(bundle, encoding="utf-8")
                info(f"Wrote bundle to: {out}")
            typer.echo(bundle)
            return

        ok = copy_instructions_to_clipboard(
            instructions_dir=str(base) if base else None,
            recursive=recursive,
            also_write_to=out,
        )
        if ok:
            success("Instructions copied to clipboard.")
        else:
            alt = out or (pathlib.Path.cwd() / "ai_instructions.txt")
            warn(f"Clipboard unavailable. Wrote bundle to: {alt}")
            raise typer.Exit(2)

    except RuntimeError as e:
        error(str(e))
        raise typer.Exit(1) from e


# ---------- settings group ----------


@settings.callback(invoke_without_command=True)
def settings_cb(ctx: typer.Context):
    """
    Settings command group callback.

    When invoked without a subcommand, defaults to `settings show`.

    Examples
    --------
    ```bash
    mainsequence settings
    mainsequence settings show
    ```
    """
    if ctx.invoked_subcommand is None:
        settings_show()
        raise typer.Exit()


@settings.command("show")
def settings_show():
    """
    Show current CLI configuration.

    Prints the backend URL and CodeRepositories base path as JSON.

    Examples
    --------
    ```bash
    mainsequence settings show
    ```
    """
    c = cfg.get_persistent_config()
    typer.echo(
        json.dumps(
            {
                "backend_url": c.get("backend_url"),
                "mainsequence_path": c.get("mainsequence_path"),
            },
            indent=2,
        )
    )


@settings.command("set-base")
def settings_set_base(path: str = typer.Argument(..., help="New CodeRepositories base folder")):
    """
    Set the base folder where CodeRepositories are cloned locally.

    Parameters
    ----------
    path:
        New base path for local CodeRepository folders.

    Examples
    --------
    ```bash
    mainsequence settings set-base ~/mainsequence
    ```
    """
    out = cfg.set_mainsequence_path(path)
    if _emit_json(out):
        return
    success(f"CodeRepositories base folder set to: {out['mainsequence_path']}")


@settings.command("set-backend")
def settings_set_backend(
    url: str = typer.Argument(..., help=f"Backend base URL, e.g. {cfg.STANDARD_BACKEND_URL}"),
):
    """
    Set backend base URL used by CLI API calls.

    Parameters
    ----------
    url:
        Backend base URL.

    Examples
    --------
    ```bash
    mainsequence settings set-backend <backend-url>
    ```
    """
    out = cfg.set_backend_url(url)
    if _emit_json(out):
        return
    success(f"Backend URL set to: {out.get('backend_url')}")


def _settings_reset_impl() -> dict:
    """
    Reset persistent CLI settings to standard defaults and clear session overrides.
    """
    standard_backend = cfg.normalize_backend_url(cfg.STANDARD_BACKEND_URL)
    standard_base = cfg.normalize_mainsequence_path(cfg.DEFAULTS.get("mainsequence_path"))
    pathlib.Path(standard_base).mkdir(parents=True, exist_ok=True)
    out = cfg.set_config(
        {
            "backend_url": standard_backend,
            "mainsequence_path": standard_base,
        }
    )
    cfg.clear_session_overrides()
    return out


@settings.command("reset")
def settings_reset():
    """
    Reset CLI settings to standard defaults.

    Resets backend URL to the standard production backend, base folder to the
    default `~/mainsequence`, and clears current terminal session overrides.

    Examples
    --------
    ```bash
    mainsequence settings reset
    ```
    """
    out = _settings_reset_impl()
    if _emit_json(out):
        return
    success("Settings reset to standard defaults.")
    info(f"Backend URL: {out.get('backend_url')}")
    info(f"CodeRepositories base folder: {out.get('mainsequence_path')}")


@settings.command("refresh")
def settings_refresh():
    """
    Alias for `settings reset`.

    Examples
    --------
    ```bash
    mainsequence settings refresh
    ```
    """
    settings_reset()


# ---------- sdk group ----------


@sdk.command("latest")
def sdk_latest():
    """
    Print latest available MainSequence SDK version from GitHub.

    Examples
    --------
    ```bash
    mainsequence sdk latest
    ```
    """
    with status("Checking GitHub for latest SDK version..."):
        try:
            v = fetch_latest_sdk_version()
        except Exception as e:
            error(f"Failed to fetch latest SDK version: {e}")
            raise typer.Exit(1) from e

    if _emit_json({"latest": v}):
        return

    if v:
        success(f"Latest SDK (GitHub): {v}")
    else:
        warn("Latest SDK version unavailable.")


def _constant_category(name: object) -> str:
    text = str(name or "").strip()
    if "__" not in text:
        return "-"
    return text.split("__", 1)[0].strip() or "-"


def _format_constant_delete_preview(constant: dict[str, object]) -> list[tuple[str, str]]:
    return [
        ("UID", str(constant.get("uid") or "-")),
        ("Category", _constant_category(constant.get("name"))),
        ("Name", str(constant.get("name") or "-")),
        ("Value", _format_json_value(constant.get("value"))),
    ]


def _format_secret_preview(secret: dict[str, object]) -> list[tuple[str, str]]:
    return [
        ("UID", str(secret.get("uid") or "-")),
        ("Name", str(secret.get("name") or "-")),
    ]


def _format_team_preview(team: dict[str, object]) -> list[tuple[str, str]]:
    organization = team.get("organization")
    if isinstance(organization, dict):
        organization_label = str(organization.get("name") or organization.get("id") or "-")
    else:
        organization_label = str(organization or "-")

    member_count = team.get("member_count")
    if member_count in (None, ""):
        members = team.get("members")
        member_count = len(members) if isinstance(members, list) else "-"

    return [
        ("UID", str(team.get("uid") or "-")),
        ("Name", str(team.get("name") or "-")),
        ("Description", str(team.get("description") or "-")),
        ("Organization", organization_label),
        ("Members", str(member_count)),
        ("Active", str(team.get("is_active")) if team.get("is_active") is not None else "-"),
    ]


def _render_shareable_user_name(user: dict[str, object]) -> str:
    first_name = str(user.get("first_name") or "").strip()
    last_name = str(user.get("last_name") or "").strip()
    full_name = " ".join(part for part in (first_name, last_name) if part)
    return full_name or "-"


def _render_shareable_team_name(team: dict[str, object]) -> str:
    return str(team.get("name") or "-")


def _format_shareable_permission_change(payload: dict[str, object]) -> list[tuple[str, str]]:
    user = payload.get("user") if isinstance(payload.get("user"), dict) else {}
    team = payload.get("team") if isinstance(payload.get("team"), dict) else {}
    explicit_view_uids = payload.get("explicit_can_view_user_uids")
    explicit_edit_uids = payload.get("explicit_can_edit_user_uids")
    explicit_view_team_uids = payload.get("explicit_can_view_team_uids")
    explicit_edit_team_uids = payload.get("explicit_can_edit_team_uids")
    return [
        ("Action", str(payload.get("action") or "-")),
        ("Detail", str(payload.get("detail") or "-")),
        ("Object Reference", str(payload.get("object_uid") or "-")),
        ("Object Type", str(payload.get("object_type") or "-")),
        ("User UID", str(user.get("uid") or "-")),
        ("Username", str(user.get("username") or "-")),
        ("Email", str(user.get("email") or "-")),
        ("Name", _render_shareable_user_name(user)),
        ("Team UID", str(team.get("uid") or "-")),
        ("Team Name", _render_shareable_team_name(team)),
        ("Team Description", str(team.get("description") or "-")),
        ("Explicit Can View", str(payload.get("explicit_can_view"))),
        ("Explicit Can Edit", str(payload.get("explicit_can_edit"))),
        (
            "Explicit View User UIDs",
            (
                ", ".join(str(item) for item in explicit_view_uids)
                if isinstance(explicit_view_uids, list)
                else "-"
            ),
        ),
        (
            "Explicit Edit User UIDs",
            (
                ", ".join(str(item) for item in explicit_edit_uids)
                if isinstance(explicit_edit_uids, list)
                else "-"
            ),
        ),
        (
            "Explicit View Team UIDs",
            (
                ", ".join(str(item) for item in explicit_view_team_uids)
                if isinstance(explicit_view_team_uids, list)
                else "-"
            ),
        ),
        (
            "Explicit Edit Team UIDs",
            (
                ", ".join(str(item) for item in explicit_edit_team_uids)
                if isinstance(explicit_edit_team_uids, list)
                else "-"
            ),
        ),
    ]


def _parse_constant_value(raw_value: str) -> object:
    text = raw_value.strip()
    if text == "":
        return ""
    try:
        return json.loads(text)
    except Exception:
        return raw_value


def _parse_json_dict_option(raw_value: str, *, field_label: str) -> dict[str, object]:
    text = (raw_value or "").strip()
    if text == "":
        return {}
    try:
        parsed = json.loads(text)
    except Exception as e:
        raise ValueError(f"{field_label} must be valid JSON: {e}") from e
    if not isinstance(parsed, dict):
        raise ValueError(f"{field_label} must be a JSON object.")
    return parsed


def _parse_text_option_or_file(
    *,
    raw_value: str | None,
    file_path: pathlib.Path | None,
    field_label: str,
) -> str | None:
    if raw_value is not None and file_path is not None:
        raise ValueError(f"Provide either {field_label} text or {field_label} file, not both.")
    if file_path is None:
        return raw_value
    try:
        return file_path.read_text(encoding="utf-8")
    except FileNotFoundError as e:
        raise ValueError(f"{field_label} file not found: {file_path}") from e
    except Exception as e:
        raise ValueError(f"Could not read {field_label} file {file_path}: {e}") from e


def _format_agent_preview(agent_payload: dict[str, object]) -> list[tuple[str, str]]:
    labels = agent_payload.get("labels")
    return [
        ("UID", str(agent_payload.get("uid") or "-")),
        ("Name", str(agent_payload.get("name") or "-")),
        ("Description", str(agent_payload.get("description") or "-")),
        ("Status", str(agent_payload.get("status") or "-")),
        (
            "Labels",
            ", ".join(str(item) for item in labels) if isinstance(labels, list) and labels else "-",
        ),
        ("LLM Provider", str(agent_payload.get("llm_provider") or "-")),
        ("LLM Model", str(agent_payload.get("llm_model") or "-")),
        ("Engine", str(agent_payload.get("engine_name") or "-")),
        ("Last Run At", str(agent_payload.get("last_run_at") or "-")),
    ]


def _format_agent_details(agent_payload: dict[str, object]) -> list[tuple[str, str]]:
    return [
        ("Runtime Config", _format_json_value(agent_payload.get("runtime_config"))),
        ("Configuration", _format_json_value(agent_payload.get("configuration"))),
        ("Metadata", _format_json_value(agent_payload.get("metadata"))),
    ]


def _format_agent_session_preview(
    agent_session_payload: dict[str, object],
) -> list[tuple[str, str]]:
    return [
        ("UID", str(agent_session_payload.get("uid") or "-")),
        ("Agent UID", str(agent_session_payload.get("agent_uid") or "-")),
        ("Status", str(agent_session_payload.get("status") or "-")),
        ("Started At", str(agent_session_payload.get("started_at") or "-")),
        ("Ended At", str(agent_session_payload.get("ended_at") or "-")),
        ("LLM Provider", str(agent_session_payload.get("llm_provider") or "-")),
        ("LLM Model", str(agent_session_payload.get("llm_model") or "-")),
        ("Engine", str(agent_session_payload.get("engine_name") or "-")),
    ]


def _format_agent_session_details(
    agent_session_payload: dict[str, object],
) -> list[tuple[str, str]]:
    return [
        ("Created By User UID", str(agent_session_payload.get("created_by_user_uid") or "-")),
        ("Parent Session UID", str(agent_session_payload.get("parent_session_uid") or "-")),
        ("Spawned By Step", str(agent_session_payload.get("spawned_by_step") or "-")),
        ("External Session ID", str(agent_session_payload.get("external_session_id") or "-")),
        ("Runtime Session ID", str(agent_session_payload.get("runtime_session_id") or "-")),
        ("Thread ID", str(agent_session_payload.get("thread_id") or "-")),
        ("Input Text", str(agent_session_payload.get("input_text") or "-")),
        ("Output Text", str(agent_session_payload.get("output_text") or "-")),
        ("Error Detail", str(agent_session_payload.get("error_detail") or "-")),
        (
            "Runtime Config Snapshot",
            _format_json_value(agent_session_payload.get("runtime_config_snapshot")),
        ),
        ("Usage Summary", _format_json_value(agent_session_payload.get("usage_summary"))),
        ("Session Metadata", _format_json_value(agent_session_payload.get("session_metadata"))),
        ("Bound Handle", _format_json_value(agent_session_payload.get("bound_handle"))),
    ]


def _render_owner_logs(payload: dict[str, object], *, title: str) -> None:
    if _emit_json(payload):
        return
    rows = payload.get("rows") or []
    if not isinstance(rows, list):
        rows = [rows]
    print_kv(
        title,
        [
            ("Environment UID", str(payload.get("organization_environment_uid") or "-")),
            ("Start", str(payload.get("start") or "-")),
            ("End", str(payload.get("end") or "-")),
            ("Truncated", str(bool(payload.get("truncated"))).lower()),
            ("Next Cursor", str(payload.get("next_cursor") or "-")),
        ],
    )
    for row in rows:
        typer.echo(_format_job_run_log_row(row))
    if not rows:
        info("No logs matched the requested window and filters.")


def _render_owner_resource_usage(payload: dict[str, object], *, title: str) -> None:
    if _emit_json(payload):
        return
    summary = payload.get("summary")
    summary = summary if isinstance(summary, dict) else {}
    print_kv(
        title,
        [
            ("Start", str(payload.get("start") or "-")),
            ("End", str(payload.get("end") or "-")),
            ("Step Seconds", str(payload.get("step_seconds") or "-")),
            ("CPU Current", str(summary.get("cpu_cores_current") or "-")),
            ("CPU Peak", str(summary.get("cpu_cores_peak") or "-")),
            ("Memory GiB Current", str(summary.get("memory_gib_current") or "-")),
            ("Memory GiB Peak", str(summary.get("memory_gib_peak") or "-")),
            ("Disk GiB Current", str(summary.get("disk_gib_current") or "-")),
            ("Disk GiB Peak", str(summary.get("disk_gib_peak") or "-")),
        ],
    )
    rows = payload.get("rows") or []
    if isinstance(rows, list) and rows:
        print_table(
            "Usage Samples",
            ["Time", "CPU Cores", "Memory GiB", "Disk GiB"],
            [
                [
                    str(row.get("time") or "-"),
                    str(row.get("cpu_cores") or "-"),
                    str(row.get("memory_gib") or "-"),
                    str(row.get("disk_gib") or "-"),
                ]
                for row in rows
                if isinstance(row, dict)
            ],
        )


def _agent_list_impl(
    organization_environment_uid: str,
    timeout: int | None,
    filter_entries: list[str] | None,
    show_filters: bool,
) -> None:
    filters = _resolve_cli_list_filters(
        model_ref=AGENT_MODEL_REF,
        filter_entries=filter_entries,
        show_filters=show_filters,
        command_label="Agents",
    )
    _require_login()

    try:
        agents = list_agents(
            organization_environment_uid=organization_environment_uid,
            timeout=timeout,
            filters=filters,
        )
    except ApiError as e:
        error(f"Agents fetch failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(agents):
        return

    rows: list[list[str]] = []
    for agent_payload in agents:
        labels = agent_payload.get("labels")
        rows.append(
            [
                str(agent_payload.get("uid") or "-"),
                str(agent_payload.get("name") or "-"),
                str(agent_payload.get("status") or "-"),
                (
                    ", ".join(str(item) for item in labels)
                    if isinstance(labels, list) and labels
                    else "-"
                ),
                str(agent_payload.get("llm_provider") or "-"),
                str(agent_payload.get("llm_model") or "-"),
                str(agent_payload.get("engine_name") or "-"),
                str(agent_payload.get("last_run_at") or "-"),
            ]
        )

    if rows:
        print_table(
            "Agents",
            [
                "UID",
                "Name",
                "Status",
                "Labels",
                "Provider",
                "Model",
                "Engine",
                "Last Run",
            ],
            rows,
        )
    else:
        info("No agents.")
    info(f"Total agents: {len(agents)}")


def _agent_detail_impl(
    *,
    agent_uid: str,
    timeout: int | None,
) -> None:
    _require_login()

    try:
        agent_payload = get_agent(agent_uid, timeout=timeout)
    except ApiError as e:
        error(f"Agent fetch failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(agent_payload):
        return

    print_kv("Agent", _format_agent_preview(agent_payload))
    print_kv("Agent Details", _format_agent_details(agent_payload))


def _agent_search_impl(
    *,
    q: str,
    organization_environment_uid: str,
    limit: int,
    timeout: int | None,
) -> None:
    _require_login()

    try:
        results = semantic_search_agents(
            q,
            organization_environment_uid=organization_environment_uid,
            limit=limit,
            timeout=timeout,
        )
    except ApiError as e:
        error(f"Agent search failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(results):
        return

    rows: list[list[str]] = []
    for result in results:
        rows.append(
            [
                str(result.get("uid") or "-"),
                str(result.get("name") or "-"),
                str(
                    result.get("combined_score")
                    if result.get("combined_score") is not None
                    else "-"
                ),
                str(
                    result.get("semantic_score")
                    if result.get("semantic_score") is not None
                    else "-"
                ),
                str(result.get("text_score") if result.get("text_score") is not None else "-"),
                str(result.get("description") or "-"),
            ]
        )

    if rows:
        print_table(
            "Agent Search Results",
            ["UID", "Name", "Combined", "Semantic", "Text", "Description"],
            rows,
        )
    else:
        info("No agents matched the search.")
    info(f'Agent search matches for "{q}": {len(results)}')


def _agent_create_impl(
    *,
    name: str | None,
    description: str | None,
    status_value: str | None,
    labels: list[str] | None,
    llm_provider: str | None,
    llm_model: str | None,
    engine_name: str | None,
    runtime_config: str | None,
    configuration: str | None,
    metadata: str | None,
    timeout: int | None,
) -> None:
    _require_login()

    agent_name = (name or "").strip() or typer.prompt(
        pydantic_prompt_text(AGENT_MODEL_REF, "name")
    ).strip()
    if not agent_name:
        error("Agent name is required.")
        raise typer.Exit(1)

    try:
        runtime_config_payload = (
            _parse_json_dict_option(runtime_config, field_label="runtime_config")
            if runtime_config is not None
            else None
        )
        configuration_payload = (
            _parse_json_dict_option(configuration, field_label="configuration")
            if configuration is not None
            else None
        )
        metadata_payload = (
            _parse_json_dict_option(metadata, field_label="metadata")
            if metadata is not None
            else None
        )
    except ValueError as e:
        error(str(e))
        raise typer.Exit(1) from e

    try:
        created = create_agent(
            name=agent_name,
            description=description,
            status=status_value,
            labels=_parse_cli_csv_list(labels),
            llm_provider=llm_provider,
            llm_model=llm_model,
            engine_name=engine_name,
            runtime_config=runtime_config_payload,
            configuration=configuration_payload,
            metadata=metadata_payload,
            timeout=timeout,
        )
    except ApiError as e:
        error(f"Agent creation failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(created):
        return

    success(f"Agent created: {agent_name}")
    print_kv("Created Agent", _format_agent_preview(created))


def _agent_delete_impl(
    *,
    agent_uid: str,
    timeout: int | None,
) -> None:
    _require_login()

    try:
        agent_payload = get_agent(agent_uid, timeout=timeout)
    except ApiError as e:
        error(f"Agent fetch failed: {e}")
        raise typer.Exit(1) from e

    verification_value = str(agent_payload.get("name") or agent_payload.get("uid") or agent_uid)
    _require_delete_verification(
        preview_title="Agent Delete Preview",
        preview_items=_format_agent_preview(agent_payload),
        verification_value=verification_value,
        verification_label="agent name" if agent_payload.get("name") else "agent uid",
    )

    try:
        deleted = delete_agent(agent_uid, timeout=timeout)
    except ApiError as e:
        error(f"Agent deletion failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(deleted):
        return

    success(f"Agent deleted: agent_uid={agent_payload.get('uid') or agent_uid}")
    print_kv("Deleted Agent", _format_agent_preview(deleted))


def _extract_standard_a2a_message_text(payload: dict[str, object]) -> str:
    response_message = payload.get("message")
    if not isinstance(response_message, dict):
        return ""
    parts = response_message.get("parts")
    if not isinstance(parts, list):
        return ""
    chunks: list[str] = []
    for part in parts:
        if isinstance(part, dict) and isinstance(part.get("text"), str):
            chunks.append(str(part["text"]))
    return "".join(chunks)


def _looks_like_uuid(value: str) -> bool:
    try:
        uuid.UUID(str(value))
        return True
    except (TypeError, ValueError):
        return False


def _resolve_agent_session_uid_or_handle(
    target: str,
    *,
    target_agent_uid: str | None,
    name: str | None,
    parent_session_uid: str | None,
    timeout: int | None,
) -> str:
    resolved_target = str(target or "").strip()
    if not resolved_target:
        error("agent session UID or handle is required.")
        raise typer.Exit(1)
    if _looks_like_uuid(resolved_target):
        return resolved_target

    resolved_agent_uid = str(target_agent_uid or "").strip()
    if not resolved_agent_uid:
        cached = cfg.get_a2a_handle_cache(resolved_target)
        cached_session_uid = str((cached or {}).get("agent_session_uid") or "").strip()
        if cached_session_uid:
            return cached_session_uid
        error(
            f"No cached A2A session for handle '{resolved_target}'. "
            "Pass --target-agent-uid once to create or resolve it."
        )
        raise typer.Exit(1)

    try:
        agent_session_payload = get_or_create_agent_session(
            resolved_agent_uid,
            handle_unique_id=resolved_target,
            name=name,
            parent_session_uid=parent_session_uid,
            timeout=timeout,
        )
    except ApiError as e:
        error(f"Agent session handle resolution failed: {e}")
        raise typer.Exit(1) from e

    agent_session_uid = str(agent_session_payload.get("uid") or "").strip()
    if not agent_session_uid:
        error("Agent session handle resolution did not return a session UID.")
        raise typer.Exit(1)

    cfg.save_a2a_handle_cache(
        resolved_target,
        agent_uid=resolved_agent_uid,
        agent_session_uid=agent_session_uid,
        name=name,
    )
    return agent_session_uid


def _agent_session_a2a_send_impl(
    *,
    agent_session_uid_or_handle: str,
    target_agent_uid: str | None,
    name: str | None,
    parent_session_uid: str | None,
    message: str | None,
    message_file: pathlib.Path | None,
    files: list[pathlib.Path] | None,
    media_types: list[str] | None,
    strict_dictionary: bool,
    json_repair_attempts: int,
    message_id: str | None,
    response_kind: str,
    timeout: int | None,
) -> None:
    try:
        resolved_message = _parse_text_option_or_file(
            raw_value=message,
            file_path=message_file,
            field_label="message",
        )
    except ValueError as e:
        error(str(e))
        raise typer.Exit(1) from e
    if resolved_message is None or not resolved_message.strip():
        error("message is required.")
        raise typer.Exit(1)
    if json_repair_attempts < 1:
        error("--json-repair-attempts must be greater than 0.")
        raise typer.Exit(1)
    if response_kind not in {"message", "task"}:
        error("--response-kind must be 'message' or 'task'.")
        raise typer.Exit(1)
    attachment_paths = list(files or [])
    attachment_media_types = list(media_types or [])
    if attachment_media_types and len(attachment_media_types) != len(attachment_paths):
        error("--media-type must be provided once per --file when used.")
        raise typer.Exit(1)
    attachments = [
        {
            "path": str(path),
            "media_type": (
                attachment_media_types[index] if attachment_media_types else "application/pdf"
            ),
        }
        for index, path in enumerate(attachment_paths)
    ]
    effective_message_id = str(message_id).strip() if message_id is not None else ""
    if not effective_message_id:
        effective_message_id = f"msg-{uuid.uuid4()}"

    _require_login()
    agent_session_uid = _resolve_agent_session_uid_or_handle(
        agent_session_uid_or_handle,
        target_agent_uid=target_agent_uid,
        name=name,
        parent_session_uid=parent_session_uid,
        timeout=timeout,
    )

    try:
        response_payload = send_agent_session_a2a_message(
            agent_session_uid,
            message=resolved_message,
            files=attachments,
            message_id=effective_message_id,
            strict_dictionary=strict_dictionary,
            json_repair_attempts=json_repair_attempts,
            response_kind=response_kind,
            timeout=timeout,
        )
    except ApiError as e:
        error(str(e))
        error(f"A2A message id for request retry: {effective_message_id}")
        raise typer.Exit(1) from e

    typer.echo(json.dumps(response_payload, indent=2))


def _agent_session_get_or_create_impl(
    *,
    agent_uid: str,
    session_uid: str | None,
    handle_unique_id: str | None,
    name: str | None,
    parent_session_uid: str | None,
    llm_provider: str | None,
    llm_model: str | None,
    llm_thinking: str | None,
    timeout: int | None,
) -> None:
    resolved_session_uid = str(session_uid or "").strip() if session_uid is not None else ""
    resolved_handle_unique_id = (
        str(handle_unique_id or "").strip() if handle_unique_id is not None else ""
    )
    if bool(resolved_session_uid) == bool(resolved_handle_unique_id):
        error("Provide exactly one of --session-uid or --handle-unique-id.")
        raise typer.Exit(1)

    creation_options = {
        "--name": name,
        "--parent-session-uid": parent_session_uid,
        "--llm-provider": llm_provider,
        "--llm-model": llm_model,
        "--llm-thinking": llm_thinking,
    }
    if resolved_session_uid and any(value is not None for value in creation_options.values()):
        provided = ", ".join(key for key, value in creation_options.items() if value is not None)
        error(f"Creation options require --handle-unique-id, not --session-uid: {provided}.")
        raise typer.Exit(1)

    _require_login()

    try:
        agent_session_payload = get_or_create_agent_session(
            agent_uid,
            session_uid=resolved_session_uid or None,
            handle_unique_id=resolved_handle_unique_id or None,
            name=name,
            parent_session_uid=parent_session_uid,
            llm_provider=llm_provider,
            llm_model=llm_model,
            llm_thinking=llm_thinking,
            timeout=timeout,
        )
    except ApiError as e:
        error(f"Agent session get-or-create failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(agent_session_payload):
        return

    success(f"Agent session resolved: uid={agent_session_payload.get('uid') or '-'}")
    print_kv("Agent Session", _format_agent_session_preview(agent_session_payload))
    print_kv("Agent Session Details", _format_agent_session_details(agent_session_payload))


def _agent_session_list_impl(
    *,
    agent_uid: str | None,
    timeout: int | None,
    filter_entries: list[str] | None,
    show_filters: bool,
) -> None:
    filters = _resolve_cli_list_filters(
        model_ref=AGENT_SESSION_MODEL_REF,
        filter_entries=filter_entries,
        show_filters=show_filters,
        command_label="Agent Sessions",
    )
    if agent_uid and any(key in filters for key in ("agent_uid", "agent_uid__in")):
        error("Do not pass `--filter agent_uid=...` with `--agent-uid`. Use only one agent scope.")
        raise typer.Exit(1)

    _require_login()

    try:
        agent_sessions = list_agent_sessions(
            timeout=timeout,
            filters=filters,
            agent_uid=agent_uid,
        )
    except ApiError as e:
        error(f"Agent sessions fetch failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(agent_sessions):
        return

    rows: list[list[str]] = []
    for agent_session_payload in agent_sessions:
        rows.append(
            [
                str(agent_session_payload.get("uid") or "-"),
                str(agent_session_payload.get("agent_uid") or "-"),
                str(
                    agent_session_payload.get("agent_name")
                    or agent_session_payload.get("agent_type")
                    or "-"
                ),
                str(agent_session_payload.get("status") or "-"),
                str(
                    agent_session_payload.get("runtime_state")
                    or agent_session_payload.get("engine_name")
                    or "-"
                ),
                str(agent_session_payload.get("started_at") or "-"),
                str(agent_session_payload.get("ended_at") or "-"),
                str(agent_session_payload.get("name") or "-"),
            ]
        )

    if rows:
        print_table(
            "Agent Sessions",
            ["UID", "Agent UID", "Agent", "Status", "Runtime", "Started At", "Ended At", "Name"],
            rows,
        )
    else:
        info("No agent sessions.")
    info(f"Total agent sessions: {len(agent_sessions)}")


def _agent_session_detail_impl(
    *,
    agent_session_uid: str,
    timeout: int | None,
) -> None:
    _require_login()

    try:
        agent_session_payload = get_agent_session(agent_session_uid, timeout=timeout)
    except ApiError as e:
        error(f"Agent session fetch failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(agent_session_payload):
        return

    print_kv("Agent Session", _format_agent_session_preview(agent_session_payload))
    print_kv("Agent Session Details", _format_agent_session_details(agent_session_payload))


def _constants_list_impl(
    timeout: int | None,
    filter_entries: list[str] | None,
    show_filters: bool,
) -> None:
    filters = _resolve_cli_list_filters(
        model_ref=CONSTANT_MODEL_REF,
        filter_entries=filter_entries,
        show_filters=show_filters,
        command_label="Constants",
    )
    _require_login()

    try:
        constants_payload = list_constants(timeout=timeout, filters=filters)
    except ApiError as e:
        error(f"Constants fetch failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(constants_payload):
        return

    rows: list[list[str]] = []
    for constant in constants_payload:
        rows.append(
            [
                str(constant.get("uid") or "-"),
                _constant_category(constant.get("name")),
                str(constant.get("name") or "-"),
                _format_json_value(constant.get("value")),
            ]
        )

    if rows:
        print_table("Constants", ["UID", "Category", "Name", "Value"], rows)
    else:
        info("No constants.")
    info(f"Total constants: {len(constants_payload)}")


def _constants_create_impl(
    *,
    name: str | None,
    value: str | None,
    timeout: int | None,
) -> None:
    _require_login()

    constant_name = (name or "").strip() or typer.prompt(
        "Constant name (double underscore creates a display category, example: ASSETS__MASTER)"
    ).strip()
    if not constant_name:
        error("Constant name is required.")
        raise typer.Exit(1)

    raw_value = value
    if raw_value is None:
        raw_value = typer.prompt(
            "Constant value (JSON parses when valid; otherwise it is stored as a string)",
            default="",
            show_default=False,
        )
    parsed_value = _parse_constant_value(raw_value)

    try:
        created = create_constant(name=constant_name, value=parsed_value, timeout=timeout)
    except ApiError as e:
        error(f"Constant creation failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(created):
        return

    success(f"Constant created: {constant_name}")
    print_kv("Created Constant", _format_constant_delete_preview(created))


def _constants_delete_impl(
    *,
    constant_uid: str,
    timeout: int | None,
) -> None:
    _require_login()

    try:
        constant = get_constant(constant_uid, timeout=timeout)
    except ApiError as e:
        error(f"Constant fetch failed: {e}")
        raise typer.Exit(1) from e

    verification_value = str(constant.get("name") or constant.get("uid") or constant_uid)
    _require_delete_verification(
        preview_title="Constant Delete Preview",
        preview_items=_format_constant_delete_preview(constant),
        verification_value=verification_value,
        verification_label="constant name" if constant.get("name") else "constant uid",
    )

    try:
        deleted = delete_constant(constant_uid, timeout=timeout)
    except ApiError as e:
        error(f"Constant deletion failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(deleted):
        return

    success(f"Constant deleted: uid={constant_uid}")
    print_kv("Deleted Constant", _format_constant_delete_preview(deleted))


def _shareable_user_list_impl(
    *,
    fetch_fn,
    object_label: str,
    access_label: str,
    object_uid: int | str,
    timeout: int | None,
) -> None:
    _require_login()

    try:
        access_state = fetch_fn(object_uid, timeout=timeout)
    except ApiError as e:
        error(f"{object_label} {access_label} fetch failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(access_state):
        return

    if isinstance(access_state, dict):
        effective_access_label = str(access_state.get("access_level") or access_label)
        users_payload = list(access_state.get("users") or [])
        teams_payload = list(access_state.get("teams") or [])
    else:
        effective_access_label = access_label
        users_payload = list(access_state or [])
        teams_payload = []

    user_rows: list[list[str]] = []
    for user in users_payload:
        user_rows.append(
            [
                str(user.get("uid") or "-"),
                str(user.get("username") or "-"),
                str(user.get("email") or "-"),
                _render_shareable_user_name(user),
            ]
        )

    title = f"{object_label} Users Who Can {effective_access_label.title()}"
    if user_rows:
        print_table(title, ["UID", "Username", "Email", "Name"], user_rows)
    else:
        info(f"No users can {effective_access_label} this {object_label.lower()}.")

    team_rows: list[list[str]] = []
    for team in teams_payload:
        member_count = team.get("member_count")
        if member_count is None:
            members = team.get("members")
            member_count = len(members) if isinstance(members, list) else "-"
        team_rows.append(
            [
                str(team.get("uid") or "-"),
                _render_shareable_team_name(team),
                str(team.get("description") or "-"),
                str(member_count),
            ]
        )

    teams_title = f"{object_label} Teams Who Can {effective_access_label.title()}"
    if team_rows:
        print_table(teams_title, ["UID", "Name", "Description", "Members"], team_rows)
    else:
        info(f"No teams can {effective_access_label} this {object_label.lower()}.")

    info(f"Total users who can {effective_access_label}: {len(users_payload)}")
    info(f"Total teams who can {effective_access_label}: {len(teams_payload)}")


def _shareable_user_access_update_impl(
    *,
    action_fn,
    object_label: str,
    action_label: str,
    object_uid: int | str,
    user_uid: str | uuid.UUID,
    timeout: int | None,
) -> None:
    _require_login()

    try:
        payload = action_fn(object_uid, str(user_uid), timeout=timeout)
    except ApiError as e:
        error(f"{object_label} {action_label} failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(payload):
        return

    success(f"{object_label} {action_label} completed.")
    print_kv(f"{object_label} Sharing Update", _format_shareable_permission_change(payload))


def _shareable_team_access_update_impl(
    *,
    action_fn,
    object_label: str,
    action_label: str,
    object_uid: int | str,
    team_uid: str | uuid.UUID,
    timeout: int | None,
) -> None:
    _require_login()

    try:
        payload = action_fn(object_uid, str(team_uid), timeout=timeout)
    except ApiError as e:
        error(f"{object_label} {action_label} failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(payload):
        return

    success(f"{object_label} {action_label} completed.")
    print_kv(f"{object_label} Sharing Update", _format_shareable_permission_change(payload))


def _format_labelable_label_change(payload: dict[str, object]) -> list[tuple[str, str]]:
    labels_payload = payload.get("labels") if isinstance(payload, dict) else []
    if not isinstance(labels_payload, list):
        labels_payload = []

    label_names: list[str] = []
    for label in labels_payload:
        if isinstance(label, dict):
            value = label.get("name") or label.get("slug") or label.get("id")
        else:
            value = label
        if value is None:
            continue
        label_names.append(str(value))

    return [
        ("Total Labels", str(len(labels_payload))),
        ("Labels", ", ".join(label_names) if label_names else "-"),
    ]


def _labelable_object_labels_update_impl(
    *,
    action_fn,
    object_label: str,
    action_label: str,
    object_uid: int | str,
    labels: list[str] | None,
    timeout: int | None,
) -> None:
    parsed_labels = _parse_cli_csv_list(labels)
    if not parsed_labels:
        error("Provide at least one --label value.")
        raise typer.Exit(2)

    _require_login()

    try:
        payload = action_fn(object_uid, parsed_labels, timeout=timeout)
    except ApiError as e:
        error(f"{object_label} {action_label} failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(payload):
        return

    success(f"{object_label} {action_label} completed.")
    print_kv(f"{object_label} Labels", _format_labelable_label_change(payload))


def _secrets_list_impl(
    timeout: int | None,
    filter_entries: list[str] | None,
    show_filters: bool,
) -> None:
    filters = _resolve_cli_list_filters(
        model_ref=SECRET_MODEL_REF,
        filter_entries=filter_entries,
        show_filters=show_filters,
        command_label="Secrets",
    )
    _require_login()

    try:
        secrets_payload = list_secrets(timeout=timeout, filters=filters)
    except ApiError as e:
        error(f"Secrets fetch failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(secrets_payload):
        return

    rows: list[list[str]] = []
    for secret in secrets_payload:
        rows.append(
            [
                str(secret.get("uid") or "-"),
                str(secret.get("name") or "-"),
            ]
        )

    if rows:
        print_table("Secrets", ["UID", "Name"], rows)
    else:
        info("No secrets.")
    info(f"Total secrets: {len(secrets_payload)}")


def _secrets_create_impl(
    *,
    name: str | None,
    value: str | None,
    timeout: int | None,
) -> None:
    _require_login()

    secret_name = (name or "").strip() or typer.prompt("Secret name").strip()
    if not secret_name:
        error("Secret name is required.")
        raise typer.Exit(1)

    secret_value = value
    if secret_value is None:
        secret_value = typer.prompt(
            "Secret value",
            default="",
            show_default=False,
            hide_input=True,
        )
    if secret_value == "":
        error("Secret value is required.")
        raise typer.Exit(1)

    try:
        created = create_secret(name=secret_name, value=secret_value, timeout=timeout)
    except ApiError as e:
        error(f"Secret creation failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(created):
        return

    success(f"Secret created: {secret_name}")
    print_kv("Created Secret", _format_secret_preview(created))


def _secrets_delete_impl(
    *,
    secret_uid: str,
    timeout: int | None,
) -> None:
    _require_login()

    try:
        secret = get_secret(secret_uid, timeout=timeout)
    except ApiError as e:
        error(f"Secret fetch failed: {e}")
        raise typer.Exit(1) from e

    verification_value = str(secret.get("name") or secret.get("uid") or secret_uid)
    _require_delete_verification(
        preview_title="Secret Delete Preview",
        preview_items=_format_secret_preview(secret),
        verification_value=verification_value,
        verification_label="secret name" if secret.get("name") else "secret uid",
    )

    try:
        deleted = delete_secret(secret_uid, timeout=timeout)
    except ApiError as e:
        error(f"Secret deletion failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(deleted):
        return

    success(f"Secret deleted: uid={secret_uid}")
    print_kv("Deleted Secret", _format_secret_preview(deleted))


def _print_storage_query_payload(title: str, payload: dict[str, object]) -> None:
    print_kv(
        title,
        [
            ("OK", str(payload.get("ok"))),
            ("Query ID", str(payload.get("query_id") or "-")),
            ("MetaTable UID", str(payload.get("meta_table_uid") or "-")),
            (
                "Time Index MetaTable UID",
                str(payload.get("time_index_meta_table_uid") or "-"),
            ),
            ("Row Count", str(payload.get("row_count") or 0)),
            ("Truncated", str(payload.get("truncated"))),
            ("Max Rows", str(payload.get("max_rows") or "-")),
        ],
    )
    print_kv(
        f"{title} Payload",
        [
            ("Results", _format_json_value(payload.get("results"))),
            ("Error", _format_json_value(payload.get("error"))),
        ],
    )


def _parse_cli_csv_list(values: list[str] | None) -> list[str]:
    items: list[str] = []
    for raw in values or []:
        for part in str(raw).split(","):
            value = part.strip()
            if value:
                items.append(value)
    return items


def _time_index_table_list_impl(
    timeout: int | None,
    filter_entries: list[str] | None,
    show_filters: bool,
    data_source_uid: str | None = None,
) -> None:
    filters = _resolve_cli_list_filters(
        model_ref=TIME_INDEX_TABLE_MODEL_REF,
        filter_entries=filter_entries,
        show_filters=show_filters,
        command_label="Time-Index Table",
    )
    filters = _merge_cli_filter_alias(
        filters,
        filter_key="data_source__uid",
        value=data_source_uid,
        option_name="data-source-uid",
    )
    _require_login()

    try:
        storages = list_time_index_tables(timeout=timeout, filters=filters)
    except ApiError as e:
        error(f"Time-index tables fetch failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(storages):
        return

    if storages:
        print_table(
            "Time-Index Tables",
            [
                "UID",
                "Physical Table",
                "Source Class",
                "Identifier",
                "Namespace",
                "Data Source",
            ],
            _build_time_index_table_rows(storages),
        )
    else:
        info("No time-index tables.")
    info(f"Total time-index tables: {len(storages)}")


def _meta_table_list_impl(
    timeout: int | None,
    filter_entries: list[str] | None,
    show_filters: bool,
    data_source_uid: str | None = None,
) -> None:
    filters = _resolve_cli_list_filters(
        model_ref=META_TABLE_MODEL_REF,
        filter_entries=filter_entries,
        show_filters=show_filters,
        command_label="MetaTable",
    )
    filters = _merge_cli_filter_alias(
        filters,
        filter_key="data_source__uid",
        value=data_source_uid,
        option_name="data-source-uid",
    )
    _require_login()

    try:
        meta_tables = list_meta_tables(timeout=timeout, filters=filters)
    except ApiError as e:
        error(f"MetaTables fetch failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(meta_tables):
        return

    if meta_tables:
        print_table(
            "MetaTables",
            [
                "UID",
                "Physical Table",
                "Identifier",
                "Namespace",
                "Mode",
                "Data Source",
            ],
            _build_meta_table_rows(meta_tables),
        )
    else:
        info("No MetaTables.")
    info(f"Total MetaTables: {len(meta_tables)}")


def _build_time_index_table_rows(storages: list[dict[str, object]]) -> list[list[str]]:
    rows: list[list[str]] = []
    for storage in storages:
        rows.append(
            [
                str(storage.get("uid") or "-"),
                str(storage.get("physical_table_name") or "-"),
                str(storage.get("source_class_name") or "-"),
                str(storage.get("identifier") or "-"),
                str(storage.get("namespace") or "-"),
                _format_time_index_table_data_source(storage.get("data_source")),
            ]
        )
    return rows


def _build_meta_table_rows(meta_tables: list[dict[str, object]]) -> list[list[str]]:
    rows: list[list[str]] = []
    for meta_table in meta_tables:
        rows.append(
            [
                str(meta_table.get("uid") or "-"),
                str(meta_table.get("physical_table_name") or "-"),
                str(meta_table.get("identifier") or "-"),
                str(meta_table.get("namespace") or "-"),
                str(meta_table.get("management_mode") or "-"),
                _format_time_index_table_data_source(meta_table.get("data_source")),
            ]
        )
    return rows


def _parse_cli_embedding(value: str | None) -> list[float] | None:
    raw = (value or "").strip()
    if not raw:
        return None

    items = [item.strip() for item in raw.split(",") if item.strip()]
    if not items:
        return None

    try:
        return [float(item) for item in items]
    except ValueError as e:
        error("Invalid --q-embedding value. Use a comma-separated list of floats.")
        raise typer.Exit(1) from e


def _unpack_time_index_table_search_response(
    payload: dict[str, object] | list[dict[str, object]],
) -> tuple[list[dict[str, object]], dict[str, object]]:
    if isinstance(payload, list):
        return payload, {}

    if isinstance(payload, dict) and isinstance(payload.get("results"), list):
        meta = {
            "count": payload.get("count"),
            "next": payload.get("next"),
            "previous": payload.get("previous"),
        }
        return list(payload.get("results") or []), meta

    if isinstance(payload, dict):
        return [payload], {}

    return [], {}


def _time_index_table_search_impl(
    *,
    command_label: str,
    title: str,
    q: str,
    filter_entries: list[str] | None,
    show_filters: bool,
    search_fn,
) -> None:
    filters = _resolve_cli_list_filters(
        model_ref=TIME_INDEX_TABLE_MODEL_REF,
        filter_entries=filter_entries,
        show_filters=show_filters,
        command_label=command_label,
    )
    _require_login()

    try:
        payload = search_fn(filters=filters)
    except ApiError as e:
        error(f"{command_label} failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(payload):
        return

    storages, pagination = _unpack_time_index_table_search_response(payload)
    if storages:
        print_table(
            title,
            [
                "UID",
                "Physical Table",
                "Source Class",
                "Identifier",
                "Namespace",
                "Data Source",
            ],
            _build_time_index_table_rows(storages),
        )
    else:
        info("No time-index tables matched the search.")

    if pagination:
        print_kv(
            "Pagination",
            [
                ("Query", q),
                ("Returned", str(len(storages))),
                ("Count", str(pagination.get("count") or "-")),
                ("Next", str(pagination.get("next") or "-")),
                ("Previous", str(pagination.get("previous") or "-")),
            ],
        )
    else:
        info(f'Returned time-index tables for query "{q}": {len(storages)}')


def _print_time_index_table_search_section(
    *,
    title: str,
    q: str,
    payload: dict[str, object] | list[dict[str, object]],
) -> int:
    storages, pagination = _unpack_time_index_table_search_response(payload)
    if storages:
        print_table(
            title,
            [
                "UID",
                "Physical Table",
                "Source Class",
                "Identifier",
                "Namespace",
                "Data Source",
            ],
            _build_time_index_table_rows(storages),
        )
    else:
        info(f'No time-index tables matched "{q}" for {title.lower()}.')

    if pagination:
        print_kv(
            f"{title} Pagination",
            [
                ("Query", q),
                ("Returned", str(len(storages))),
                ("Count", str(pagination.get("count") or "-")),
                ("Next", str(pagination.get("next") or "-")),
                ("Previous", str(pagination.get("previous") or "-")),
            ],
        )
    else:
        info(f'{title}: {len(storages)} match(es) for "{q}"')

    return len(storages)


@agent.command("list")
def agent_list_cmd(
    organization_environment_uid: uuid.UUID = typer.Option(
        ...,
        "--environment-uid",
        help="Organization Environment UID that scopes Agent discovery.",
    ),
    filter_entries: list[str] | None = typer.Option(None, "--filter", help=LIST_FILTER_OPTION_HELP),
    show_filters: bool = typer.Option(
        False, "--show-filters", help="Show the filters supported by this list command and exit."
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    List agents visible to the authenticated user.
    """
    _agent_list_impl(
        organization_environment_uid=str(organization_environment_uid),
        timeout=timeout,
        filter_entries=filter_entries,
        show_filters=show_filters,
    )


@agent.command("detail")
def agent_detail_cmd(
    agent_uid: str = pydantic_argument(
        AGENT_MODEL_REF,
        "uid",
        ...,
        help="Agent UID.",
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Show one agent in detail.
    """
    _agent_detail_impl(agent_uid=agent_uid, timeout=timeout)


@agent.command("logs")
def agent_logs_cmd(
    agent_uid: str = pydantic_argument(AGENT_MODEL_REF, "uid", ..., help="Agent UID."),
    start: float | None = typer.Option(
        None, "--start", help="Window start as epoch seconds or milliseconds."
    ),
    end: float | None = typer.Option(
        None, "--end", help="Window end as epoch seconds or milliseconds."
    ),
    cursor: str | None = typer.Option(None, "--cursor", help="Opaque continuation cursor."),
    limit: int | None = typer.Option(None, "--limit", min=1, max=500),
    severity: str | None = typer.Option(None, "--severity"),
    request_id: str | None = typer.Option(None, "--request-id"),
    event: str | None = typer.Option(None, "--event"),
    outcome: str | None = typer.Option(None, "--outcome"),
    agent_session_uid: str | None = typer.Option(None, "--agent-session-uid"),
    timeout: int | None = typer.Option(None, "--timeout"),
):
    """Read application logs owned by an Agent."""
    _require_login()
    try:
        payload = get_agent_logs(
            agent_uid,
            start=start,
            end=end,
            cursor=cursor,
            limit=limit,
            severity=severity,
            request_id=request_id,
            event=event,
            outcome=outcome,
            agent_session_uid=agent_session_uid,
            timeout=timeout,
        )
    except ApiError as e:
        error(f"Agent logs fetch failed: {e}")
        raise typer.Exit(1) from e
    _render_owner_logs(payload, title="Agent Logs")


@agent.command("resource-usage")
def agent_resource_usage_cmd(
    agent_uid: str = pydantic_argument(AGENT_MODEL_REF, "uid", ..., help="Agent UID."),
    start: float | None = typer.Option(
        None, "--start", help="Window start as epoch seconds or milliseconds."
    ),
    end: float | None = typer.Option(
        None, "--end", help="Window end as epoch seconds or milliseconds."
    ),
    timeout: int | None = typer.Option(None, "--timeout"),
):
    """Read aggregate CPU, memory, and disk usage owned by an Agent."""
    _require_login()
    try:
        payload = get_agent_resource_usage(agent_uid, start=start, end=end, timeout=timeout)
    except ApiError as e:
        error(f"Agent resource usage fetch failed: {e}")
        raise typer.Exit(1) from e
    _render_owner_resource_usage(payload, title="Agent Resource Usage")


@agent.command("search")
def agent_search_cmd(
    q: str = typer.Argument(..., help="Natural-language query to match against agents."),
    organization_environment_uid: uuid.UUID = typer.Option(
        ...,
        "--environment-uid",
        help="Organization Environment UID that scopes Agent discovery.",
    ),
    limit: int = typer.Option(
        20, "--limit", min=1, max=100, help="Maximum number of ranked agent matches to return."
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Search agents.

    Uses SDK client `Agent.semantic_search()` as the single source of truth.

    Examples
    --------
    ```bash
    mainsequence agent search "data research copilot"
    mainsequence agent search "pricing assistant" --limit 10
    ```
    """
    _agent_search_impl(
        q=q,
        organization_environment_uid=str(organization_environment_uid),
        limit=limit,
        timeout=timeout,
    )


@agent.command("create")
def agent_create_cmd(
    name: str | None = pydantic_argument(AGENT_MODEL_REF, "name", None),
    description: str | None = pydantic_option(
        AGENT_MODEL_REF, "description", None, "--description"
    ),
    status_value: str | None = typer.Option(
        None,
        "--status",
        help="Lifecycle status for the agent. One of: draft, active, archived.",
    ),
    labels: list[str] | None = typer.Option(
        None, "--label", help="Repeatable or comma-separated agent label."
    ),
    llm_provider: str | None = pydantic_option(
        AGENT_MODEL_REF, "llm_provider", None, "--llm-provider"
    ),
    llm_model: str | None = pydantic_option(AGENT_MODEL_REF, "llm_model", None, "--llm-model"),
    engine_name: str | None = typer.Option(
        None,
        "--engine-name",
        help="Optional execution engine name to store on the agent.",
    ),
    runtime_config: str | None = typer.Option(
        None,
        "--runtime-config",
        help="Runtime config JSON object to store on the agent.",
    ),
    configuration: str | None = typer.Option(
        None,
        "--configuration",
        help="Additional configuration JSON object to store on the agent.",
    ),
    metadata: str | None = typer.Option(
        None,
        "--metadata",
        help="Additional metadata JSON object to store on the agent.",
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Create one agent.
    """
    _agent_create_impl(
        name=name,
        description=description,
        status_value=status_value,
        labels=labels,
        llm_provider=llm_provider,
        llm_model=llm_model,
        engine_name=engine_name,
        runtime_config=runtime_config,
        configuration=configuration,
        metadata=metadata,
        timeout=timeout,
    )


@agent.command("delete")
def agent_delete_cmd(
    agent_uid: str = pydantic_argument(
        AGENT_MODEL_REF,
        "uid",
        ...,
        help="Agent UID.",
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Delete one agent.
    """
    _agent_delete_impl(agent_uid=agent_uid, timeout=timeout)


@agent_session_group.command("list")
def agent_session_list_cmd(
    agent_uid: str | None = pydantic_option(
        AGENT_SESSION_MODEL_REF,
        "agent_uid",
        None,
        "--agent-uid",
        help="Agent UID to scope the session list.",
    ),
    filter_entries: list[str] | None = typer.Option(None, "--filter", help=LIST_FILTER_OPTION_HELP),
    show_filters: bool = typer.Option(
        False, "--show-filters", help="Show the filters supported by this list command and exit."
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    List agent sessions, optionally scoped to one agent.

    Uses SDK client `AgentSession.filter()` as the single source of truth.

    Examples
    --------
    ```bash
    mainsequence agent session list
    mainsequence agent session list --agent-uid e0e75693-4110-464c-93e0-82c7fd9c9a23
    mainsequence agent session list --agent-uid e0e75693-4110-464c-93e0-82c7fd9c9a23 --filter status=running
    ```
    """
    _agent_session_list_impl(
        agent_uid=agent_uid,
        timeout=timeout,
        filter_entries=filter_entries,
        show_filters=show_filters,
    )


@agent_session_group.command("get_or_create")
def agent_session_get_or_create_cmd(
    agent_uid: str = pydantic_argument(
        AGENT_MODEL_REF,
        "uid",
        ...,
        help="Agent UID.",
    ),
    session_uid: str | None = pydantic_option(
        AGENT_SESSION_MODEL_REF,
        "uid",
        None,
        "--session-uid",
        help="Existing agent session UID to resolve for this agent.",
    ),
    handle_unique_id: str | None = typer.Option(
        None,
        "--handle-unique-id",
        help="Reusable session handle key to get or create a session.",
    ),
    name: str | None = typer.Option(
        None,
        "--name",
        help="Human-readable session display name used when creating a handle-backed session.",
    ),
    parent_session_uid: str | None = typer.Option(
        None,
        "--parent-session-uid",
        help="Parent or origin agent session UID used when creating a handle-backed session.",
    ),
    llm_provider: str | None = typer.Option(
        None,
        "--llm-provider",
        help="Session LLM provider override used when creating a handle-backed session.",
    ),
    llm_model: str | None = typer.Option(
        None,
        "--llm-model",
        help="Session LLM model override used when creating a handle-backed session.",
    ),
    llm_thinking: str | None = typer.Option(
        None,
        "--llm-thinking",
        help="Session thinking/reasoning override used when creating a handle-backed session.",
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Get an existing agent session by UID, or get/create one by handle.

    Sends exactly one lookup key to the backend: `session_uid` or `handle_unique_id`.

    Examples
    --------
    ```bash
    mainsequence agent session get_or_create e0e75693-4110-464c-93e0-82c7fd9c9a23 --session-uid 3f1cc452-43ec-49cb-b2ba-87dbac164d29
    mainsequence agent session get_or_create e0e75693-4110-464c-93e0-82c7fd9c9a23 --handle-unique-id portfolio-review-q2-2026 --name "Quarterly portfolio review"
    ```
    """
    _agent_session_get_or_create_impl(
        agent_uid=agent_uid,
        session_uid=session_uid,
        handle_unique_id=handle_unique_id,
        name=name,
        parent_session_uid=parent_session_uid,
        llm_provider=llm_provider,
        llm_model=llm_model,
        llm_thinking=llm_thinking,
        timeout=timeout,
    )


@agent_session_a2a_group.command("send")
def agent_session_a2a_send_cmd(
    agent_session_uid_or_handle: str = pydantic_argument(
        AGENT_SESSION_MODEL_REF,
        "uid",
        ...,
        help="Agent session UID, or cached A2A handle.",
    ),
    target_agent_uid: str | None = typer.Option(
        None,
        "--target-agent-uid",
        help="Target agent UID used to create or resolve a handle on first use.",
    ),
    name: str | None = typer.Option(
        None,
        "--name",
        help="Optional session name used only when resolving a handle with --target-agent-uid.",
    ),
    parent_session_uid: str | None = typer.Option(
        None,
        "--parent-session-uid",
        help="Optional parent session UID used only when resolving a handle with --target-agent-uid.",
    ),
    message: str | None = typer.Option(
        None,
        "--message",
        help="Plain text message to send using the standard A2A message protocol.",
    ),
    message_file: pathlib.Path | None = typer.Option(
        None,
        "--message-file",
        help="Path to a UTF-8 text file containing the A2A message.",
    ),
    files: list[pathlib.Path] | None = typer.Option(
        None,
        "--file",
        help="Inline file attachment to send as an A2A raw part.",
    ),
    media_types: list[str] | None = typer.Option(
        None,
        "--media-type",
        help="Media type for each --file. Defaults to application/pdf.",
    ),
    strict_dictionary: bool = typer.Option(
        False,
        "--strict-dictionary",
        help="Request a strict JSON dictionary response through the standard A2A output contract.",
    ),
    json_repair_attempts: int = typer.Option(
        3,
        "--json-repair-attempts",
        help="JSON repair attempts for --strict-dictionary.",
    ),
    message_id: str | None = typer.Option(
        None,
        "--message-id",
        help="Stable A2A message.messageId to preserve request identity across a retry.",
    ),
    response_kind: str = typer.Option(
        "message",
        "--response-kind",
        help="Select the A2A result contract: message or task.",
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Send one standard A2A message to an existing session UID or cached handle.
    """
    _agent_session_a2a_send_impl(
        agent_session_uid_or_handle=agent_session_uid_or_handle,
        target_agent_uid=target_agent_uid,
        name=name,
        parent_session_uid=parent_session_uid,
        message=message,
        message_file=message_file,
        files=files,
        media_types=media_types,
        strict_dictionary=strict_dictionary,
        json_repair_attempts=json_repair_attempts,
        message_id=message_id,
        response_kind=response_kind,
        timeout=timeout,
    )


@agent_session_group.command("detail")
def agent_session_detail_cmd(
    agent_session_uid: str = pydantic_argument(
        AGENT_SESSION_MODEL_REF, "uid", ..., help="Agent session UID."
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Show one agent session in detail by agent session UID.

    Uses SDK client `AgentSession.get()` as the single source of truth.

    Examples
    --------
    ```bash
    mainsequence agent session detail 3f1cc452-43ec-49cb-b2ba-87dbac164d29
    mainsequence agent session detail 3f1cc452-43ec-49cb-b2ba-87dbac164d29 --timeout 60
    ```
    """
    _agent_session_detail_impl(agent_session_uid=agent_session_uid, timeout=timeout)


@agent_session_group.command("logs")
def agent_session_logs_cmd(
    agent_session_uid: str = pydantic_argument(
        AGENT_SESSION_MODEL_REF,
        "uid",
        ...,
        help="Agent session UID.",
    ),
    start: float | None = typer.Option(
        None, "--start", help="Window start as epoch seconds or milliseconds."
    ),
    end: float | None = typer.Option(
        None, "--end", help="Window end as epoch seconds or milliseconds."
    ),
    cursor: str | None = typer.Option(None, "--cursor", help="Opaque continuation cursor."),
    limit: int | None = typer.Option(None, "--limit", min=1, max=500),
    severity: str | None = typer.Option(None, "--severity"),
    request_id: str | None = typer.Option(None, "--request-id"),
    event: str | None = typer.Option(None, "--event"),
    outcome: str | None = typer.Option(None, "--outcome"),
    timeout: int | None = typer.Option(None, "--timeout"),
):
    """Read logs fixed to one AgentSession owner path."""
    _require_login()
    try:
        payload = get_agent_session_logs(
            agent_session_uid,
            start=start,
            end=end,
            cursor=cursor,
            limit=limit,
            severity=severity,
            request_id=request_id,
            event=event,
            outcome=outcome,
            timeout=timeout,
        )
    except ApiError as e:
        error(f"Agent session logs fetch failed: {e}")
        raise typer.Exit(1) from e
    _render_owner_logs(payload, title="Agent Session Logs")


@agent.command("can_view")
def agent_can_view_cmd(
    agent_uid: str = pydantic_argument(AGENT_MODEL_REF, "uid", ..., help="Agent UID."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_user_list_impl(
        fetch_fn=list_agent_users_can_view,
        object_label="Agent",
        access_label="view",
        object_uid=agent_uid,
        timeout=timeout,
    )


@agent.command("can_edit")
def agent_can_edit_cmd(
    agent_uid: str = pydantic_argument(AGENT_MODEL_REF, "uid", ..., help="Agent UID."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_user_list_impl(
        fetch_fn=list_agent_users_can_edit,
        object_label="Agent",
        access_label="edit",
        object_uid=agent_uid,
        timeout=timeout,
    )


@agent.command("add_to_view")
def agent_add_to_view_cmd(
    agent_uid: str = pydantic_argument(AGENT_MODEL_REF, "uid", ..., help="Agent UID."),
    user_uid: uuid.UUID = typer.Argument(..., help="User UID to grant view access."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_user_access_update_impl(
        action_fn=add_agent_user_to_view,
        object_label="Agent",
        action_label="add_to_view",
        object_uid=agent_uid,
        user_uid=user_uid,
        timeout=timeout,
    )


@agent.command("add_to_edit")
def agent_add_to_edit_cmd(
    agent_uid: str = pydantic_argument(AGENT_MODEL_REF, "uid", ..., help="Agent UID."),
    user_uid: uuid.UUID = typer.Argument(..., help="User UID to grant edit access."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_user_access_update_impl(
        action_fn=add_agent_user_to_edit,
        object_label="Agent",
        action_label="add_to_edit",
        object_uid=agent_uid,
        user_uid=user_uid,
        timeout=timeout,
    )


@agent.command("remove_from_view")
def agent_remove_from_view_cmd(
    agent_uid: str = pydantic_argument(AGENT_MODEL_REF, "uid", ..., help="Agent UID."),
    user_uid: uuid.UUID = typer.Argument(..., help="User UID to remove explicit view access from."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_user_access_update_impl(
        action_fn=remove_agent_user_from_view,
        object_label="Agent",
        action_label="remove_from_view",
        object_uid=agent_uid,
        user_uid=user_uid,
        timeout=timeout,
    )


@agent.command("remove_from_edit")
def agent_remove_from_edit_cmd(
    agent_uid: str = pydantic_argument(AGENT_MODEL_REF, "uid", ..., help="Agent UID."),
    user_uid: uuid.UUID = typer.Argument(..., help="User UID to remove explicit edit access from."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_user_access_update_impl(
        action_fn=remove_agent_user_from_edit,
        object_label="Agent",
        action_label="remove_from_edit",
        object_uid=agent_uid,
        user_uid=user_uid,
        timeout=timeout,
    )


@agent.command("add_team_to_view")
def agent_add_team_to_view_cmd(
    agent_uid: str = pydantic_argument(AGENT_MODEL_REF, "uid", ..., help="Agent UID."),
    team_uid: uuid.UUID = typer.Argument(..., help="Team UID to grant view access."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_team_access_update_impl(
        action_fn=add_agent_team_to_view,
        object_label="Agent",
        action_label="add_team_to_view",
        object_uid=agent_uid,
        team_uid=team_uid,
        timeout=timeout,
    )


@agent.command("add_team_to_edit")
def agent_add_team_to_edit_cmd(
    agent_uid: str = pydantic_argument(AGENT_MODEL_REF, "uid", ..., help="Agent UID."),
    team_uid: uuid.UUID = typer.Argument(..., help="Team UID to grant edit access."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_team_access_update_impl(
        action_fn=add_agent_team_to_edit,
        object_label="Agent",
        action_label="add_team_to_edit",
        object_uid=agent_uid,
        team_uid=team_uid,
        timeout=timeout,
    )


@agent.command("remove_team_from_view")
def agent_remove_team_from_view_cmd(
    agent_uid: str = pydantic_argument(AGENT_MODEL_REF, "uid", ..., help="Agent UID."),
    team_uid: uuid.UUID = typer.Argument(..., help="Team UID to remove explicit view access from."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_team_access_update_impl(
        action_fn=remove_agent_team_from_view,
        object_label="Agent",
        action_label="remove_team_from_view",
        object_uid=agent_uid,
        team_uid=team_uid,
        timeout=timeout,
    )


@agent.command("remove_team_from_edit")
def agent_remove_team_from_edit_cmd(
    agent_uid: str = pydantic_argument(AGENT_MODEL_REF, "uid", ..., help="Agent UID."),
    team_uid: uuid.UUID = typer.Argument(..., help="Team UID to remove explicit edit access from."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_team_access_update_impl(
        action_fn=remove_agent_team_from_edit,
        object_label="Agent",
        action_label="remove_team_from_edit",
        object_uid=agent_uid,
        team_uid=team_uid,
        timeout=timeout,
    )


@constants.command("list")
def constants_list_cmd(
    filter_entries: list[str] | None = typer.Option(None, "--filter", help=LIST_FILTER_OPTION_HELP),
    show_filters: bool = typer.Option(
        False, "--show-filters", help="Show the filters supported by this list command and exit."
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    List constants visible to the authenticated user.

    Uses SDK client `Constant.filter()` as the single source of truth.
    Names containing a double underscore display the prefix before `__`
    as the terminal `Category`, for example `ASSETS__MASTER` => `ASSETS`.

    Examples
    --------
    ```bash
    mainsequence constants list
    mainsequence constants list --show-filters
    mainsequence constants list --filter name__in=ASSETS__MASTER,APP__MODE
    ```
    """
    _constants_list_impl(
        timeout=timeout,
        filter_entries=filter_entries,
        show_filters=show_filters,
    )


@constants.command("create")
def constants_create_cmd(
    name: str | None = typer.Argument(None, help="Constant name, for example ASSETS__MASTER."),
    value: str | None = typer.Argument(
        None,
        help="Constant value. JSON is parsed when valid; otherwise it is stored as a string.",
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Create one constant.

    Only `name` and `value` are accepted by this CLI flow.
    Names containing a double underscore display the prefix before `__`
    as the terminal `Category`, for example `ASSETS__MASTER` => `ASSETS`.

    Examples
    --------
    ```bash
    mainsequence constants create APP__MODE production
    mainsequence constants create ASSETS__MASTER '{"dataset":"bloomberg"}'
    ```
    """
    _constants_create_impl(name=name, value=value, timeout=timeout)


@constants.command("delete")
def constants_delete_cmd(
    constant_uid: str = typer.Argument(..., help="Constant UID."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Delete one constant.

    The command always requires typed verification before the delete call is executed.

    Examples
    --------
    ```bash
    mainsequence constants delete 498d499f-b74c-43f7-acf1-2e2955ad0e6b
    ```
    """
    _constants_delete_impl(constant_uid=constant_uid, timeout=timeout)


@constants.command("can_view")
def constants_can_view_cmd(
    constant_uid: str = typer.Argument(..., help="Constant UID."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    List users and teams who can view one constant.

    Uses the SDK `ShareableObjectMixin.can_view()` path through the `Constant` model.

    Examples
    --------
    ```bash
    mainsequence constants can_view 498d499f-b74c-43f7-acf1-2e2955ad0e6b
    ```
    """
    _shareable_user_list_impl(
        fetch_fn=list_constant_users_can_view,
        object_label="Constant",
        access_label="view",
        object_uid=constant_uid,
        timeout=timeout,
    )


@constants.command("can_edit")
def constants_can_edit_cmd(
    constant_uid: str = typer.Argument(..., help="Constant UID."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    List users and teams who can edit one constant.

    Uses the SDK `ShareableObjectMixin.can_edit()` path through the `Constant` model.

    Examples
    --------
    ```bash
    mainsequence constants can_edit 498d499f-b74c-43f7-acf1-2e2955ad0e6b
    ```
    """
    _shareable_user_list_impl(
        fetch_fn=list_constant_users_can_edit,
        object_label="Constant",
        access_label="edit",
        object_uid=constant_uid,
        timeout=timeout,
    )


@constants.command("add_to_view")
def constants_add_to_view_cmd(
    constant_uid: str = typer.Argument(..., help="Constant UID."),
    user_uid: uuid.UUID = typer.Argument(..., help="User UID to grant view access."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Grant explicit view access to one user for one constant.

    Examples
    --------
    ```bash
    mainsequence constants add_to_view 498d499f-b74c-43f7-acf1-2e2955ad0e6b <USER_UID>
    ```
    """
    _shareable_user_access_update_impl(
        action_fn=add_constant_user_to_view,
        object_label="Constant",
        action_label="add_to_view",
        object_uid=constant_uid,
        user_uid=user_uid,
        timeout=timeout,
    )


@constants.command("add_to_edit")
def constants_add_to_edit_cmd(
    constant_uid: str = typer.Argument(..., help="Constant UID."),
    user_uid: uuid.UUID = typer.Argument(..., help="User UID to grant edit access."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Grant explicit edit access to one user for one constant.

    Examples
    --------
    ```bash
    mainsequence constants add_to_edit 498d499f-b74c-43f7-acf1-2e2955ad0e6b <USER_UID>
    ```
    """
    _shareable_user_access_update_impl(
        action_fn=add_constant_user_to_edit,
        object_label="Constant",
        action_label="add_to_edit",
        object_uid=constant_uid,
        user_uid=user_uid,
        timeout=timeout,
    )


@constants.command("remove_from_view")
def constants_remove_from_view_cmd(
    constant_uid: str = typer.Argument(..., help="Constant UID."),
    user_uid: uuid.UUID = typer.Argument(..., help="User UID to remove explicit view access from."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Remove explicit view access from one user for one constant.

    Examples
    --------
    ```bash
    mainsequence constants remove_from_view 498d499f-b74c-43f7-acf1-2e2955ad0e6b <USER_UID>
    ```
    """
    _shareable_user_access_update_impl(
        action_fn=remove_constant_user_from_view,
        object_label="Constant",
        action_label="remove_from_view",
        object_uid=constant_uid,
        user_uid=user_uid,
        timeout=timeout,
    )


@constants.command("remove_from_edit")
def constants_remove_from_edit_cmd(
    constant_uid: str = typer.Argument(..., help="Constant UID."),
    user_uid: uuid.UUID = typer.Argument(..., help="User UID to remove explicit edit access from."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Remove explicit edit access from one user for one constant.

    Examples
    --------
    ```bash
    mainsequence constants remove_from_edit 498d499f-b74c-43f7-acf1-2e2955ad0e6b <USER_UID>
    ```
    """
    _shareable_user_access_update_impl(
        action_fn=remove_constant_user_from_edit,
        object_label="Constant",
        action_label="remove_from_edit",
        object_uid=constant_uid,
        user_uid=user_uid,
        timeout=timeout,
    )


@constants.command("add_team_to_view")
def constants_add_team_to_view_cmd(
    constant_uid: str = typer.Argument(..., help="Constant UID."),
    team_uid: uuid.UUID = typer.Argument(..., help="Team UID to grant view access."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_team_access_update_impl(
        action_fn=add_constant_team_to_view,
        object_label="Constant",
        action_label="add_team_to_view",
        object_uid=constant_uid,
        team_uid=team_uid,
        timeout=timeout,
    )


@constants.command("add_team_to_edit")
def constants_add_team_to_edit_cmd(
    constant_uid: str = typer.Argument(..., help="Constant UID."),
    team_uid: uuid.UUID = typer.Argument(..., help="Team UID to grant edit access."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_team_access_update_impl(
        action_fn=add_constant_team_to_edit,
        object_label="Constant",
        action_label="add_team_to_edit",
        object_uid=constant_uid,
        team_uid=team_uid,
        timeout=timeout,
    )


@constants.command("remove_team_from_view")
def constants_remove_team_from_view_cmd(
    constant_uid: str = typer.Argument(..., help="Constant UID."),
    team_uid: uuid.UUID = typer.Argument(..., help="Team UID to remove explicit view access from."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_team_access_update_impl(
        action_fn=remove_constant_team_from_view,
        object_label="Constant",
        action_label="remove_team_from_view",
        object_uid=constant_uid,
        team_uid=team_uid,
        timeout=timeout,
    )


@constants.command("remove_team_from_edit")
def constants_remove_team_from_edit_cmd(
    constant_uid: str = typer.Argument(..., help="Constant UID."),
    team_uid: uuid.UUID = typer.Argument(..., help="Team UID to remove explicit edit access from."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_team_access_update_impl(
        action_fn=remove_constant_team_from_edit,
        object_label="Constant",
        action_label="remove_team_from_edit",
        object_uid=constant_uid,
        team_uid=team_uid,
        timeout=timeout,
    )


@secrets.command("list")
def secrets_list_cmd(
    filter_entries: list[str] | None = typer.Option(None, "--filter", help=LIST_FILTER_OPTION_HELP),
    show_filters: bool = typer.Option(
        False, "--show-filters", help="Show the filters supported by this list command and exit."
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    List secrets visible to the authenticated user.

    Uses SDK client `Secret.filter()` as the single source of truth.

    Examples
    --------
    ```bash
    mainsequence secrets list
    mainsequence secrets list --show-filters
    mainsequence secrets list --filter name__in=API_KEY,DB_PASSWORD
    ```
    """
    _secrets_list_impl(
        timeout=timeout,
        filter_entries=filter_entries,
        show_filters=show_filters,
    )


@secrets.command("create")
def secrets_create_cmd(
    name: str | None = typer.Argument(None, help="Secret name."),
    value: str | None = typer.Argument(None, help="Secret value."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Create one secret.

    Only `name` and `value` are accepted by this CLI flow.

    Examples
    --------
    ```bash
    mainsequence secrets create API_KEY super-secret-value
    ```
    """
    _secrets_create_impl(name=name, value=value, timeout=timeout)


@secrets.command("delete")
def secrets_delete_cmd(
    secret_uid: str = typer.Argument(..., help="Secret UID."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Delete one secret.

    The command always requires typed verification before the delete call is executed.

    Examples
    --------
    ```bash
    mainsequence secrets delete 498d499f-b74c-43f7-acf1-2e2955ad0e6b
    ```
    """
    _secrets_delete_impl(secret_uid=secret_uid, timeout=timeout)


@secrets.command("can_view")
def secrets_can_view_cmd(
    secret_uid: str = typer.Argument(..., help="Secret UID."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    List users and teams who can view one secret.

    Uses the SDK `ShareableObjectMixin.can_view()` path through the `Secret` model.

    Examples
    --------
    ```bash
    mainsequence secrets can_view 498d499f-b74c-43f7-acf1-2e2955ad0e6b
    ```
    """
    _shareable_user_list_impl(
        fetch_fn=list_secret_users_can_view,
        object_label="Secret",
        access_label="view",
        object_uid=secret_uid,
        timeout=timeout,
    )


@secrets.command("can_edit")
def secrets_can_edit_cmd(
    secret_uid: str = typer.Argument(..., help="Secret UID."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    List users and teams who can edit one secret.

    Uses the SDK `ShareableObjectMixin.can_edit()` path through the `Secret` model.

    Examples
    --------
    ```bash
    mainsequence secrets can_edit 498d499f-b74c-43f7-acf1-2e2955ad0e6b
    ```
    """
    _shareable_user_list_impl(
        fetch_fn=list_secret_users_can_edit,
        object_label="Secret",
        access_label="edit",
        object_uid=secret_uid,
        timeout=timeout,
    )


@secrets.command("add_to_view")
def secrets_add_to_view_cmd(
    secret_uid: str = typer.Argument(..., help="Secret UID."),
    user_uid: uuid.UUID = typer.Argument(..., help="User UID to grant view access."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Grant explicit view access to one user for one secret.

    Examples
    --------
    ```bash
    mainsequence secrets add_to_view 498d499f-b74c-43f7-acf1-2e2955ad0e6b <USER_UID>
    ```
    """
    _shareable_user_access_update_impl(
        action_fn=add_secret_user_to_view,
        object_label="Secret",
        action_label="add_to_view",
        object_uid=secret_uid,
        user_uid=user_uid,
        timeout=timeout,
    )


@secrets.command("add_to_edit")
def secrets_add_to_edit_cmd(
    secret_uid: str = typer.Argument(..., help="Secret UID."),
    user_uid: uuid.UUID = typer.Argument(..., help="User UID to grant edit access."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Grant explicit edit access to one user for one secret.

    Examples
    --------
    ```bash
    mainsequence secrets add_to_edit 498d499f-b74c-43f7-acf1-2e2955ad0e6b <USER_UID>
    ```
    """
    _shareable_user_access_update_impl(
        action_fn=add_secret_user_to_edit,
        object_label="Secret",
        action_label="add_to_edit",
        object_uid=secret_uid,
        user_uid=user_uid,
        timeout=timeout,
    )


@secrets.command("remove_from_view")
def secrets_remove_from_view_cmd(
    secret_uid: str = typer.Argument(..., help="Secret UID."),
    user_uid: uuid.UUID = typer.Argument(..., help="User UID to remove explicit view access from."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Remove explicit view access from one user for one secret.

    Examples
    --------
    ```bash
    mainsequence secrets remove_from_view 498d499f-b74c-43f7-acf1-2e2955ad0e6b <USER_UID>
    ```
    """
    _shareable_user_access_update_impl(
        action_fn=remove_secret_user_from_view,
        object_label="Secret",
        action_label="remove_from_view",
        object_uid=secret_uid,
        user_uid=user_uid,
        timeout=timeout,
    )


@secrets.command("remove_from_edit")
def secrets_remove_from_edit_cmd(
    secret_uid: str = typer.Argument(..., help="Secret UID."),
    user_uid: uuid.UUID = typer.Argument(..., help="User UID to remove explicit edit access from."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Remove explicit edit access from one user for one secret.

    Examples
    --------
    ```bash
    mainsequence secrets remove_from_edit 498d499f-b74c-43f7-acf1-2e2955ad0e6b <USER_UID>
    ```
    """
    _shareable_user_access_update_impl(
        action_fn=remove_secret_user_from_edit,
        object_label="Secret",
        action_label="remove_from_edit",
        object_uid=secret_uid,
        user_uid=user_uid,
        timeout=timeout,
    )


@secrets.command("add_team_to_view")
def secrets_add_team_to_view_cmd(
    secret_uid: str = typer.Argument(..., help="Secret UID."),
    team_uid: uuid.UUID = typer.Argument(..., help="Team UID to grant view access."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_team_access_update_impl(
        action_fn=add_secret_team_to_view,
        object_label="Secret",
        action_label="add_team_to_view",
        object_uid=secret_uid,
        team_uid=team_uid,
        timeout=timeout,
    )


@secrets.command("add_team_to_edit")
def secrets_add_team_to_edit_cmd(
    secret_uid: str = typer.Argument(..., help="Secret UID."),
    team_uid: uuid.UUID = typer.Argument(..., help="Team UID to grant edit access."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_team_access_update_impl(
        action_fn=add_secret_team_to_edit,
        object_label="Secret",
        action_label="add_team_to_edit",
        object_uid=secret_uid,
        team_uid=team_uid,
        timeout=timeout,
    )


@secrets.command("remove_team_from_view")
def secrets_remove_team_from_view_cmd(
    secret_uid: str = typer.Argument(..., help="Secret UID."),
    team_uid: uuid.UUID = typer.Argument(..., help="Team UID to remove explicit view access from."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_team_access_update_impl(
        action_fn=remove_secret_team_from_view,
        object_label="Secret",
        action_label="remove_team_from_view",
        object_uid=secret_uid,
        team_uid=team_uid,
        timeout=timeout,
    )


@secrets.command("remove_team_from_edit")
def secrets_remove_team_from_edit_cmd(
    secret_uid: str = typer.Argument(..., help="Secret UID."),
    team_uid: uuid.UUID = typer.Argument(..., help="Team UID to remove explicit edit access from."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_team_access_update_impl(
        action_fn=remove_secret_team_from_edit,
        object_label="Secret",
        action_label="remove_team_from_edit",
        object_uid=secret_uid,
        team_uid=team_uid,
        timeout=timeout,
    )


def _time_index_table_detail_impl(table_uid: str, timeout: int | None) -> None:
    _require_login()

    try:
        storage = get_time_index_table(table_uid, timeout=timeout)
    except ApiError as e:
        error(f"Time-index table fetch failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(storage):
        return

    time_indexed_profile = storage.get("time_indexed_profile")
    storage_layout = storage.get("storage_layout")
    physical_index_plan = storage.get("physical_index_plan")
    if isinstance(time_indexed_profile, dict):
        storage_layout = time_indexed_profile.get("storage_layout") or storage_layout
        physical_index_plan = time_indexed_profile.get("physical_index_plan") or physical_index_plan

    print_kv(
        "Time-Index Table",
        [
            ("UID", str(storage.get("uid") or table_uid)),
            ("Physical Table", str(storage.get("physical_table_name") or "-")),
            ("Identifier", str(storage.get("identifier") or "-")),
            ("Source Class", str(storage.get("source_class_name") or "-")),
            ("Data Source", _format_time_index_table_data_source(storage.get("data_source"))),
            ("Protected", str(storage.get("protect_from_deletion"))),
            ("Created", str(storage.get("creation_date") or "-")),
            ("Created By", str(storage.get("created_by_user") or "-")),
            ("Organization", str(storage.get("organization_owner") or "-")),
            ("Description", str(storage.get("description") or "-")),
        ],
    )

    print_kv(
        "Time-Index Table Config",
        [
            ("Time Indexed Profile", _format_json_value(time_indexed_profile)),
            ("Storage Layout", _format_json_value(storage_layout)),
            ("Physical Index Plan", _format_json_value(physical_index_plan)),
            ("Table Index Names", _format_json_value(storage.get("table_index_names"))),
            ("Compression Policy", _format_json_value(storage.get("compression_policy_config"))),
            ("Retention Policy", _format_json_value(storage.get("retention_policy_config"))),
        ],
    )


def _meta_table_detail_impl(meta_table_uid: str, timeout: int | None) -> None:
    _require_login()

    try:
        meta_table = get_meta_table(meta_table_uid, timeout=timeout)
    except ApiError as e:
        error(f"MetaTable fetch failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(meta_table):
        return

    print_kv(
        "MetaTable",
        [
            ("UID", str(meta_table.get("uid") or meta_table_uid)),
            ("Physical Table", str(meta_table.get("physical_table_name") or "-")),
            ("Identifier", str(meta_table.get("identifier") or "-")),
            ("Namespace", str(meta_table.get("namespace") or "-")),
            ("Management Mode", str(meta_table.get("management_mode") or "-")),
            ("Data Source", _format_time_index_table_data_source(meta_table.get("data_source"))),
            ("Protected", str(meta_table.get("protect_from_deletion"))),
            ("Created", str(meta_table.get("creation_date") or "-")),
            ("Created By", str(meta_table.get("created_by_user_uid") or "-")),
            ("Organization", str(meta_table.get("organization_owner_uid") or "-")),
            ("Description", str(meta_table.get("description") or "-")),
        ],
    )

    print_kv(
        "MetaTable Contract",
        [
            ("Contract Version", str(meta_table.get("contract_version") or "-")),
            ("Table Contract", _format_json_value(meta_table.get("table_contract"))),
            ("Columns", _format_json_value(meta_table.get("columns"))),
            ("Introspection", _format_json_value(meta_table.get("introspection_snapshot"))),
        ],
    )


def _meta_table_run_query_impl(
    *,
    meta_table_uid: str,
    sql: str,
    timeout: int | None,
) -> None:
    _require_login()

    try:
        payload = run_meta_table_query(meta_table_uid, sql, timeout=timeout)
    except ApiError as e:
        error(f"MetaTable query failed: {e}")
        raise typer.Exit(1) from e

    ok = bool(payload.get("ok"))
    if _emit_json(payload):
        if not ok:
            raise typer.Exit(1)
        return

    if ok:
        success(f"MetaTable query completed: uid={meta_table_uid}")
    else:
        error(f"MetaTable query failed: uid={meta_table_uid}")
    _print_storage_query_payload("MetaTable Query", payload)
    if not ok:
        raise typer.Exit(1)


def _time_index_table_run_query_impl(
    *,
    table_uid: str,
    sql: str,
    timeout: int | None,
) -> None:
    _require_login()

    try:
        payload = run_time_index_table_query(table_uid, sql, timeout=timeout)
    except ApiError as e:
        error(f"Time-index table query failed: {e}")
        raise typer.Exit(1) from e

    ok = bool(payload.get("ok"))
    if _emit_json(payload):
        if not ok:
            raise typer.Exit(1)
        return

    if ok:
        success(f"Time-index table query completed: uid={table_uid}")
    else:
        error(f"Time-index table query failed: uid={table_uid}")
    _print_storage_query_payload("Time-Index Table Query", payload)
    if not ok:
        raise typer.Exit(1)


def _time_index_table_delete_impl(
    *,
    table_uid: str,
    full_delete_selected: bool,
    full_delete_downstream_tables: bool,
    delete_with_no_table: bool,
    override_protection: bool,
    timeout: int | None,
) -> None:
    _require_login()

    try:
        storage = get_time_index_table(table_uid, timeout=timeout)
    except ApiError as e:
        error(f"Time-index table fetch failed: {e}")
        raise typer.Exit(1) from e

    verification_value = str(storage.get("physical_table_name") or storage.get("uid") or table_uid)
    verification_label = (
        "physical table name" if storage.get("physical_table_name") else "storage uid"
    )
    _require_delete_verification(
        preview_title="Time-Index Table Delete Preview",
        preview_items=_format_time_index_table_delete_preview(storage)
        + [
            ("full_delete_selected", str(full_delete_selected).lower()),
            ("full_delete_downstream_tables", str(full_delete_downstream_tables).lower()),
            ("delete_with_no_table", str(delete_with_no_table).lower()),
            ("override_protection", str(override_protection).lower()),
        ],
        verification_value=verification_value,
        verification_label=verification_label,
    )

    try:
        deleted = delete_time_index_table(
            table_uid,
            full_delete_selected=full_delete_selected,
            full_delete_downstream_tables=full_delete_downstream_tables,
            delete_with_no_table=delete_with_no_table,
            override_protection=override_protection,
            timeout=timeout,
        )
    except ApiError as e:
        error(f"Time-index table deletion failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(deleted):
        return

    success(f"Time-index table deleted: uid={table_uid}")
    print_kv("Deleted Time-Index Table", _format_time_index_table_delete_preview(deleted))


def _meta_table_delete_impl(
    *,
    meta_table_uid: str,
    timeout: int | None,
) -> None:
    _require_login()

    try:
        meta_table = get_meta_table(meta_table_uid, timeout=timeout)
    except ApiError as e:
        error(f"MetaTable fetch failed: {e}")
        raise typer.Exit(1) from e

    verification_value = str(
        meta_table.get("physical_table_name") or meta_table.get("uid") or meta_table_uid
    )
    verification_label = (
        "physical table name" if meta_table.get("physical_table_name") else "MetaTable uid"
    )
    _require_delete_verification(
        preview_title="MetaTable Delete Preview",
        preview_items=_format_meta_table_delete_preview(meta_table),
        verification_value=verification_value,
        verification_label=verification_label,
    )

    try:
        deleted = delete_meta_table(meta_table_uid, timeout=timeout)
    except ApiError as e:
        error(f"MetaTable deletion failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(deleted):
        return

    success(f"MetaTable deleted: uid={meta_table_uid}")
    print_kv("Deleted MetaTable", _format_meta_table_delete_preview(deleted))


@meta_table_group.command("list")
def meta_table_list_cmd(
    data_source_uid: str | None = typer.Option(
        None, "--data-source-uid", help="Filter by data source UID."
    ),
    filter_entries: list[str] | None = typer.Option(None, "--filter", help=LIST_FILTER_OPTION_HELP),
    show_filters: bool = typer.Option(
        False, "--show-filters", help="Show the filters supported by this list command and exit."
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    List MetaTables visible to the authenticated user.

    Uses SDK client `MetaTable.filter()` as the canonical table-storage surface.
    """
    _meta_table_list_impl(
        timeout=timeout,
        filter_entries=filter_entries,
        show_filters=show_filters,
        data_source_uid=data_source_uid,
    )


@meta_table_group.command("detail")
def meta_table_detail_cmd(
    meta_table_uid: str = typer.Argument(..., help="MetaTable UID."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Show one MetaTable and render its table contract in the terminal.
    """
    _meta_table_detail_impl(meta_table_uid=meta_table_uid, timeout=timeout)


@meta_table_group.command("run_query")
def meta_table_run_query_cmd(
    meta_table_uid: str = typer.Argument(..., help="MetaTable UID."),
    sql: str = typer.Argument(..., help="Raw SQL query to run."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Run a raw SQL query against one MetaTable.

    Sends the SQL as a JSON string body to the backend, not as an object.
    """
    _meta_table_run_query_impl(meta_table_uid=meta_table_uid, sql=sql, timeout=timeout)


@meta_table_group.command("delete")
def meta_table_delete_cmd(
    meta_table_uid: str = typer.Argument(..., help="MetaTable UID."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Delete one MetaTable through the canonical table-storage model.
    """
    _meta_table_delete_impl(meta_table_uid=meta_table_uid, timeout=timeout)


@meta_table_group.command("can_view")
def meta_table_can_view_cmd(
    meta_table_uid: str = typer.Argument(..., help="MetaTable UID."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """List users who can view one MetaTable."""
    _shareable_user_list_impl(
        fetch_fn=list_meta_table_users_can_view,
        object_label="MetaTable",
        access_label="view",
        object_uid=meta_table_uid,
        timeout=timeout,
    )


@meta_table_group.command("can_edit")
def meta_table_can_edit_cmd(
    meta_table_uid: str = typer.Argument(..., help="MetaTable UID."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """List users who can edit one MetaTable."""
    _shareable_user_list_impl(
        fetch_fn=list_meta_table_users_can_edit,
        object_label="MetaTable",
        access_label="edit",
        object_uid=meta_table_uid,
        timeout=timeout,
    )


@meta_table_group.command("add-label")
def meta_table_add_label_cmd(
    meta_table_uid: str = typer.Argument(..., help="MetaTable UID."),
    labels: list[str] | None = typer.Option(
        None,
        "--label",
        help="Organizational label to add. Repeatable or comma-separated.",
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """Add one or more organizational labels to a MetaTable."""
    _labelable_object_labels_update_impl(
        action_fn=add_meta_table_labels,
        object_label="MetaTable",
        action_label="add-label",
        object_uid=meta_table_uid,
        labels=labels,
        timeout=timeout,
    )


@meta_table_group.command("add_label", hidden=True)
def meta_table_add_label_alias_cmd(
    meta_table_uid: str = typer.Argument(..., help="MetaTable UID."),
    labels: list[str] | None = typer.Option(None, "--label", help="Organizational label to add."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """Backward-compatible alias for `mainsequence meta-table add-label`."""
    meta_table_add_label_cmd(meta_table_uid=meta_table_uid, labels=labels, timeout=timeout)


@meta_table_group.command("remove-label")
def meta_table_remove_label_cmd(
    meta_table_uid: str = typer.Argument(..., help="MetaTable UID."),
    labels: list[str] | None = typer.Option(
        None,
        "--label",
        help="Organizational label to remove. Repeatable or comma-separated.",
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """Remove one or more organizational labels from a MetaTable."""
    _labelable_object_labels_update_impl(
        action_fn=remove_meta_table_labels,
        object_label="MetaTable",
        action_label="remove-label",
        object_uid=meta_table_uid,
        labels=labels,
        timeout=timeout,
    )


@meta_table_group.command("remove_label", hidden=True)
def meta_table_remove_label_alias_cmd(
    meta_table_uid: str = typer.Argument(..., help="MetaTable UID."),
    labels: list[str] | None = typer.Option(
        None, "--label", help="Organizational label to remove."
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """Backward-compatible alias for `mainsequence meta-table remove-label`."""
    meta_table_remove_label_cmd(meta_table_uid=meta_table_uid, labels=labels, timeout=timeout)


@meta_table_group.command("add_to_view")
def meta_table_add_to_view_cmd(
    meta_table_uid: str = typer.Argument(..., help="MetaTable UID."),
    user_uid: uuid.UUID = typer.Argument(..., help="User UID to grant view access."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """Grant explicit view access to one user for one MetaTable."""
    _shareable_user_access_update_impl(
        action_fn=add_meta_table_user_to_view,
        object_label="MetaTable",
        action_label="add_to_view",
        object_uid=meta_table_uid,
        user_uid=user_uid,
        timeout=timeout,
    )


@meta_table_group.command("add_to_edit")
def meta_table_add_to_edit_cmd(
    meta_table_uid: str = typer.Argument(..., help="MetaTable UID."),
    user_uid: uuid.UUID = typer.Argument(..., help="User UID to grant edit access."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """Grant explicit edit access to one user for one MetaTable."""
    _shareable_user_access_update_impl(
        action_fn=add_meta_table_user_to_edit,
        object_label="MetaTable",
        action_label="add_to_edit",
        object_uid=meta_table_uid,
        user_uid=user_uid,
        timeout=timeout,
    )


@meta_table_group.command("remove_from_view")
def meta_table_remove_from_view_cmd(
    meta_table_uid: str = typer.Argument(..., help="MetaTable UID."),
    user_uid: uuid.UUID = typer.Argument(..., help="User UID to remove explicit view access from."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """Remove explicit view access from one user for one MetaTable."""
    _shareable_user_access_update_impl(
        action_fn=remove_meta_table_user_from_view,
        object_label="MetaTable",
        action_label="remove_from_view",
        object_uid=meta_table_uid,
        user_uid=user_uid,
        timeout=timeout,
    )


@meta_table_group.command("remove_from_edit")
def meta_table_remove_from_edit_cmd(
    meta_table_uid: str = typer.Argument(..., help="MetaTable UID."),
    user_uid: uuid.UUID = typer.Argument(..., help="User UID to remove explicit edit access from."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """Remove explicit edit access from one user for one MetaTable."""
    _shareable_user_access_update_impl(
        action_fn=remove_meta_table_user_from_edit,
        object_label="MetaTable",
        action_label="remove_from_edit",
        object_uid=meta_table_uid,
        user_uid=user_uid,
        timeout=timeout,
    )


@meta_table_group.command("add_team_to_view")
def meta_table_add_team_to_view_cmd(
    meta_table_uid: str = typer.Argument(..., help="MetaTable UID."),
    team_uid: uuid.UUID = typer.Argument(..., help="Team UID to grant view access."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_team_access_update_impl(
        action_fn=add_meta_table_team_to_view,
        object_label="MetaTable",
        action_label="add_team_to_view",
        object_uid=meta_table_uid,
        team_uid=team_uid,
        timeout=timeout,
    )


@meta_table_group.command("add_team_to_edit")
def meta_table_add_team_to_edit_cmd(
    meta_table_uid: str = typer.Argument(..., help="MetaTable UID."),
    team_uid: uuid.UUID = typer.Argument(..., help="Team UID to grant edit access."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_team_access_update_impl(
        action_fn=add_meta_table_team_to_edit,
        object_label="MetaTable",
        action_label="add_team_to_edit",
        object_uid=meta_table_uid,
        team_uid=team_uid,
        timeout=timeout,
    )


@meta_table_group.command("remove_team_from_view")
def meta_table_remove_team_from_view_cmd(
    meta_table_uid: str = typer.Argument(..., help="MetaTable UID."),
    team_uid: uuid.UUID = typer.Argument(..., help="Team UID to remove explicit view access from."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_team_access_update_impl(
        action_fn=remove_meta_table_team_from_view,
        object_label="MetaTable",
        action_label="remove_team_from_view",
        object_uid=meta_table_uid,
        team_uid=team_uid,
        timeout=timeout,
    )


@meta_table_group.command("remove_team_from_edit")
def meta_table_remove_team_from_edit_cmd(
    meta_table_uid: str = typer.Argument(..., help="MetaTable UID."),
    team_uid: uuid.UUID = typer.Argument(..., help="Team UID to remove explicit edit access from."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_team_access_update_impl(
        action_fn=remove_meta_table_team_from_edit,
        object_label="MetaTable",
        action_label="remove_team_from_edit",
        object_uid=meta_table_uid,
        team_uid=team_uid,
        timeout=timeout,
    )


@time_index_table_group.command("list")
def time_index_table_list_cmd(
    data_source_uid: str | None = typer.Option(
        None, "--data-source-uid", help="Filter by data source UID."
    ),
    filter_entries: list[str] | None = typer.Option(None, "--filter", help=LIST_FILTER_OPTION_HELP),
    show_filters: bool = typer.Option(
        False, "--show-filters", help="Show the filters supported by this list command and exit."
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    List time-index tables visible to the authenticated user.

    Uses SDK client `TimeIndexMetaTable.filter()` as the single source of truth.

    Parameters
    ----------
    timeout:
        Request timeout in seconds.

    Examples
    --------
    ```bash
    mainsequence time-index-table list
    mainsequence time-index-table list --filter namespace=pytest_alice
    mainsequence time-index-table list --data-source-uid <DATA_SOURCE_UID>
    mainsequence time-index-table list --timeout 60
    ```
    """
    _time_index_table_list_impl(
        timeout=timeout,
        filter_entries=filter_entries,
        show_filters=show_filters,
        data_source_uid=data_source_uid,
    )


@time_index_table_group.command("detail")
def time_index_table_detail_cmd(
    table_uid: str = typer.Argument(..., help="Time-index table UID."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Show one time-index table and render its configuration in the terminal.

    The configuration view includes the server-derived `storage_layout` and
    `physical_index_plan` when the backend exposes them on the source table
    configuration.

    Uses SDK client `TimeIndexMetaTable.get()` as the single source of truth.

    Parameters
    ----------
    table_uid:
        Time-index table UID.
    timeout:
        Request timeout in seconds.

    Examples
    --------
    ```bash
    mainsequence time-index-table detail <TIME_INDEX_TABLE_UID>
    mainsequence time-index-table detail <TIME_INDEX_TABLE_UID> --timeout 60
    ```
    """
    _time_index_table_detail_impl(table_uid=table_uid, timeout=timeout)


@time_index_table_group.command("run_query")
def time_index_table_run_query_cmd(
    table_uid: str = typer.Argument(..., help="Time-index table UID."),
    sql: str = typer.Argument(..., help="Raw SQL query to run."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Run a raw SQL query against one time-index table.
    """
    _time_index_table_run_query_impl(table_uid=table_uid, sql=sql, timeout=timeout)


@time_index_table_group.command("refresh-search-index")
def time_index_table_refresh_search_index_cmd(
    table_uid: str = typer.Argument(..., help="Time-index table UID."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Refresh the semantic search index for one time-index table.

    Uses SDK client `TimeIndexMetaTable.refresh_table_search_index()` as the single source of truth.

    Examples
    --------
    ```bash
    mainsequence time-index-table refresh-search-index <TIME_INDEX_TABLE_UID>
    mainsequence time-index-table refresh-search-index <TIME_INDEX_TABLE_UID> --timeout 60
    ```
    """
    _require_login()

    try:
        payload = refresh_time_index_table_search_index(table_uid, timeout=timeout)
    except ApiError as e:
        error(f"Time-index table search index refresh failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(payload):
        return

    success(f"Time-index table search index refresh requested: uid={table_uid}")
    print_kv(
        "Time-Index Table Search Index Refresh",
        [(str(k), _format_json_value(v)) for k, v in payload.items()],
    )


@time_index_table_group.command("search")
def time_index_table_search_cmd(
    q: str = typer.Argument(
        ..., help="Natural-language query to match against time-index table descriptions."
    ),
    mode: str = typer.Option(
        "description",
        "--mode",
        help=(
            "Search scope. Default is semantic description discovery. "
            "Use column only for schema-name lookup, or both when explicitly needed."
        ),
    ),
    data_source_uid: str | None = typer.Option(
        None, "--data-source-uid", help="Filter by data source UID."
    ),
    q_embedding: str | None = typer.Option(
        None,
        "--q-embedding",
        help="Optional comma-separated embedding vector, for example 0.1,0.2,0.3.",
    ),
    trigram_k: int = typer.Option(200, "--trigram-k", help="Candidate count for trigram search."),
    embed_k: int = typer.Option(200, "--embed-k", help="Candidate count for embedding search."),
    w_trgm: float = typer.Option(0.65, "--w-trgm", help="Weight for trigram ranking."),
    w_emb: float = typer.Option(0.35, "--w-emb", help="Weight for embedding ranking."),
    embedding_model: str = typer.Option(
        "default",
        "--embedding-model",
        help="Embedding model to use when the server generates the query embedding.",
    ),
    filter_entries: list[str] | None = typer.Option(None, "--filter", help=LIST_FILTER_OPTION_HELP),
    show_filters: bool = typer.Option(
        False,
        "--show-filters",
        help="Show structured filters that can narrow this search and exit.",
    ),
):
    """
    Search time-index tables through MetaTable metadata.

    Default search uses `TimeIndexMetaTable.description_search()`, backed by
    `/api/v1/time-index-meta-tables/description-search/`. Column mode is a
    separate schema lookup path and filters narrow results; they are not the
    semantic discovery path itself.

    Examples
    --------
    ```bash
    mainsequence time-index-table search "close price"
    mainsequence time-index-table search "node weights" --data-source-uid <DATA_SOURCE_UID>
    mainsequence time-index-table search "close" --mode column
    mainsequence time-index-table search "node weights" --q-embedding 0.1,0.2,0.3
    ```
    """
    normalized_mode = (mode or "").strip().lower()
    if normalized_mode not in {"both", "description", "column"}:
        error("Invalid --mode. Use one of: both, description, column.")
        raise typer.Exit(1)

    parsed_embedding = _parse_cli_embedding(q_embedding)
    filters = _resolve_cli_list_filters(
        model_ref=TIME_INDEX_TABLE_MODEL_REF,
        filter_entries=filter_entries,
        show_filters=show_filters,
        command_label="Time-Index Table Search",
    )
    filters = _merge_cli_filter_alias(
        filters,
        filter_key="data_source__uid",
        value=data_source_uid,
        option_name="data-source-uid",
    )
    _require_login()

    total_matches = 0
    description_payload = None
    column_payload = None

    if normalized_mode in {"both", "description"}:
        try:
            description_payload = time_index_table_description_search(
                q,
                q_embedding=parsed_embedding,
                trigram_k=trigram_k,
                embed_k=embed_k,
                w_trgm=w_trgm,
                w_emb=w_emb,
                embedding_model=embedding_model,
                filters=filters,
            )
        except ApiError as e:
            error(f"Time-Index Table Search failed: {e}")
            raise typer.Exit(1) from e
        storages, _ = _unpack_time_index_table_search_response(description_payload)
        total_matches += len(storages)

    if normalized_mode in {"both", "column"}:
        try:
            column_payload = time_index_table_column_search(q, filters=filters)
        except ApiError as e:
            error(f"Time-Index Table Search failed: {e}")
            raise typer.Exit(1) from e
        storages, _ = _unpack_time_index_table_search_response(column_payload)
        total_matches += len(storages)

    if _emit_json(
        {
            "query": q,
            "mode": normalized_mode,
            "description": (
                description_payload if normalized_mode in {"both", "description"} else None
            ),
            "column": column_payload if normalized_mode in {"both", "column"} else None,
            "total_matches": total_matches,
        }
    ):
        return

    if normalized_mode in {"both", "description"} and description_payload is not None:
        _print_time_index_table_search_section(
            title="Description Matches",
            q=q,
            payload=description_payload,
        )

    if normalized_mode in {"both", "column"} and column_payload is not None:
        _print_time_index_table_search_section(
            title="Column Matches",
            q=q,
            payload=column_payload,
        )

    info(f'Total search matches for "{q}": {total_matches}')


@time_index_table_group.command("description-search", hidden=True)
def time_index_table_description_search_cmd(
    q: str = typer.Argument(
        ..., help="Natural-language query to match against time-index table descriptions."
    ),
    data_source_uid: str | None = typer.Option(
        None, "--data-source-uid", help="Filter by data source UID."
    ),
    q_embedding: str | None = typer.Option(
        None,
        "--q-embedding",
        help="Optional comma-separated embedding vector, for example 0.1,0.2,0.3.",
    ),
    trigram_k: int = typer.Option(200, "--trigram-k", help="Candidate count for trigram search."),
    embed_k: int = typer.Option(200, "--embed-k", help="Candidate count for embedding search."),
    w_trgm: float = typer.Option(0.65, "--w-trgm", help="Weight for trigram ranking."),
    w_emb: float = typer.Option(0.35, "--w-emb", help="Weight for embedding ranking."),
    embedding_model: str = typer.Option(
        "default",
        "--embedding-model",
        help="Embedding model to use when the server generates the query embedding.",
    ),
    filter_entries: list[str] | None = typer.Option(None, "--filter", help=LIST_FILTER_OPTION_HELP),
    show_filters: bool = typer.Option(
        False,
        "--show-filters",
        help="Show structured filters that can narrow this search and exit.",
    ),
):
    """
    Hidden alias for semantic description discovery.
    """
    parsed_embedding = _parse_cli_embedding(q_embedding)
    filters = _resolve_cli_list_filters(
        model_ref=TIME_INDEX_TABLE_MODEL_REF,
        filter_entries=filter_entries,
        show_filters=show_filters,
        command_label="Time-Index Table Description Search",
    )
    filters = _merge_cli_filter_alias(
        filters,
        filter_key="data_source__uid",
        value=data_source_uid,
        option_name="data-source-uid",
    )
    _require_login()
    try:
        payload = time_index_table_description_search(
            q,
            q_embedding=parsed_embedding,
            trigram_k=trigram_k,
            embed_k=embed_k,
            w_trgm=w_trgm,
            w_emb=w_emb,
            embedding_model=embedding_model,
            filters=filters,
        )
    except ApiError as e:
        error(f"Time-Index Table Description Search failed: {e}")
        raise typer.Exit(1) from e
    if _emit_json(payload):
        return
    _print_time_index_table_search_section(
        title="Description Matches",
        q=q,
        payload=payload,
    )


@time_index_table_group.command("column-search", hidden=True)
def time_index_table_column_search_cmd(
    q: str = typer.Argument(..., help="Column name or term to search in time-index table columns."),
    data_source_uid: str | None = typer.Option(
        None, "--data-source-uid", help="Filter by data source UID."
    ),
    filter_entries: list[str] | None = typer.Option(None, "--filter", help=LIST_FILTER_OPTION_HELP),
    show_filters: bool = typer.Option(
        False,
        "--show-filters",
        help="Show structured filters that can narrow this column lookup and exit.",
    ),
):
    """
    Search time-index tables by column metadata.

    Uses SDK client `TimeIndexMetaTable.column_search()` as the single source of truth.

    Examples
    --------
    ```bash
    mainsequence time-index-table column-search weight
    mainsequence time-index-table column-search close --filter physical_table_name__contains=weights
    ```
    """
    filters = _resolve_cli_list_filters(
        model_ref=TIME_INDEX_TABLE_MODEL_REF,
        filter_entries=filter_entries,
        show_filters=show_filters,
        command_label="Time-Index Table Column Search",
    )
    filters = _merge_cli_filter_alias(
        filters,
        filter_key="data_source__uid",
        value=data_source_uid,
        option_name="data-source-uid",
    )
    _require_login()
    try:
        payload = time_index_table_column_search(q, filters=filters)
    except ApiError as e:
        error(f"Time-Index Table Column Search failed: {e}")
        raise typer.Exit(1) from e
    if _emit_json(payload):
        return
    _print_time_index_table_search_section(
        title="Column Matches",
        q=q,
        payload=payload,
    )


@time_index_table_group.command("delete")
def time_index_table_delete_cmd(
    table_uid: str = typer.Argument(..., help="Time-index table UID."),
    full_delete_selected: bool = typer.Option(
        False,
        "--full-delete-selected/--no-full-delete-selected",
        help="Fully delete the selected TimeIndexMetaTable and its backing table.",
    ),
    full_delete_downstream_tables: bool = typer.Option(
        False,
        "--full-delete-downstream-tables/--no-full-delete-downstream-tables",
        help="Delete downstream tables and dependencies starting from the selected metadata instance.",
    ),
    delete_with_no_table: bool = typer.Option(
        False,
        "--delete-with-no-table/--no-delete-with-no-table",
        help="Scan TimeIndexMetaTable rows and fully delete records whose backing DB table does not exist.",
    ),
    override_protection: bool = typer.Option(
        False,
        "--override-protection/--no-override-protection",
        help="Bypass protect_from_deletion. ORG_ADMIN only. Used with full_delete_selected=true.",
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Delete one time-index table using the SDK client `TimeIndexMetaTable.delete()` path.

    The command always requires typed verification before the delete call is executed.

    Examples
    --------
    ```bash
    mainsequence time-index-table delete <TIME_INDEX_TABLE_UID>
    mainsequence time-index-table delete <TIME_INDEX_TABLE_UID> --full-delete-selected
    mainsequence time-index-table delete <TIME_INDEX_TABLE_UID> --full-delete-selected --override-protection
    ```
    """
    _time_index_table_delete_impl(
        table_uid=table_uid,
        full_delete_selected=full_delete_selected,
        full_delete_downstream_tables=full_delete_downstream_tables,
        delete_with_no_table=delete_with_no_table,
        override_protection=override_protection,
        timeout=timeout,
    )


@time_index_table_group.command("can_view")
def time_index_table_can_view_cmd(
    table_uid: str = typer.Argument(..., help="Time-index table UID."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    List users who can view one time-index table.

    Uses the SDK `ShareableObjectMixin.can_view()` path through the `TimeIndexMetaTable` model.

    Examples
    --------
    ```bash
    mainsequence time-index-table can_view <TIME_INDEX_TABLE_UID>
    ```
    """
    _shareable_user_list_impl(
        fetch_fn=list_time_index_table_users_can_view,
        object_label="Time-Index Table",
        access_label="view",
        object_uid=table_uid,
        timeout=timeout,
    )


@time_index_table_group.command("can_edit")
def time_index_table_can_edit_cmd(
    table_uid: str = typer.Argument(..., help="Time-index table UID."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    List users who can edit one time-index table.

    Uses the SDK `ShareableObjectMixin.can_edit()` path through the `TimeIndexMetaTable` model.

    Examples
    --------
    ```bash
    mainsequence time-index-table can_edit <TIME_INDEX_TABLE_UID>
    ```
    """
    _shareable_user_list_impl(
        fetch_fn=list_time_index_table_users_can_edit,
        object_label="Time-Index Table",
        access_label="edit",
        object_uid=table_uid,
        timeout=timeout,
    )


@time_index_table_group.command("add-label")
def time_index_table_add_label_cmd(
    table_uid: str = typer.Argument(..., help="Time-index table UID."),
    labels: list[str] | None = typer.Option(
        None,
        "--label",
        help="Organizational label to add. Repeatable or comma-separated.",
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Add one or more organizational labels to a time-index table.

    Labels are helpers for grouping and discovery only. They do not affect runtime behavior or functionality.
    """
    _labelable_object_labels_update_impl(
        action_fn=add_time_index_table_labels,
        object_label="Time-Index Table",
        action_label="add-label",
        object_uid=table_uid,
        labels=labels,
        timeout=timeout,
    )


@time_index_table_group.command("remove-label")
def time_index_table_remove_label_cmd(
    table_uid: str = typer.Argument(..., help="Time-index table UID."),
    labels: list[str] | None = typer.Option(
        None,
        "--label",
        help="Organizational label to remove. Repeatable or comma-separated.",
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Remove one or more organizational labels from a time-index table.

    Labels are helpers for grouping and discovery only. They do not affect runtime behavior or functionality.
    """
    _labelable_object_labels_update_impl(
        action_fn=remove_time_index_table_labels,
        object_label="Time-Index Table",
        action_label="remove-label",
        object_uid=table_uid,
        labels=labels,
        timeout=timeout,
    )


@time_index_table_group.command("add_to_view")
def time_index_table_add_to_view_cmd(
    table_uid: str = typer.Argument(..., help="Time-index table UID."),
    user_uid: uuid.UUID = typer.Argument(..., help="User UID to grant view access."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Grant explicit view access to one user for one time-index table.

    Examples
    --------
    ```bash
    mainsequence time-index-table add_to_view <TIME_INDEX_TABLE_UID> <USER_UID>
    ```
    """
    _shareable_user_access_update_impl(
        action_fn=add_time_index_table_user_to_view,
        object_label="Time-Index Table",
        action_label="add_to_view",
        object_uid=table_uid,
        user_uid=user_uid,
        timeout=timeout,
    )


@time_index_table_group.command("add_to_edit")
def time_index_table_add_to_edit_cmd(
    table_uid: str = typer.Argument(..., help="Time-index table UID."),
    user_uid: uuid.UUID = typer.Argument(..., help="User UID to grant edit access."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Grant explicit edit access to one user for one time-index table.

    Examples
    --------
    ```bash
    mainsequence time-index-table add_to_edit <TIME_INDEX_TABLE_UID> <USER_UID>
    ```
    """
    _shareable_user_access_update_impl(
        action_fn=add_time_index_table_user_to_edit,
        object_label="Time-Index Table",
        action_label="add_to_edit",
        object_uid=table_uid,
        user_uid=user_uid,
        timeout=timeout,
    )


@time_index_table_group.command("remove_from_view")
def time_index_table_remove_from_view_cmd(
    table_uid: str = typer.Argument(..., help="Time-index table UID."),
    user_uid: uuid.UUID = typer.Argument(..., help="User UID to remove explicit view access from."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Remove explicit view access from one user for one time-index table.

    Examples
    --------
    ```bash
    mainsequence time-index-table remove_from_view <TIME_INDEX_TABLE_UID> <USER_UID>
    ```
    """
    _shareable_user_access_update_impl(
        action_fn=remove_time_index_table_user_from_view,
        object_label="Time-Index Table",
        action_label="remove_from_view",
        object_uid=table_uid,
        user_uid=user_uid,
        timeout=timeout,
    )


@time_index_table_group.command("remove_from_edit")
def time_index_table_remove_from_edit_cmd(
    table_uid: str = typer.Argument(..., help="Time-index table UID."),
    user_uid: uuid.UUID = typer.Argument(..., help="User UID to remove explicit edit access from."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Remove explicit edit access from one user for one time-index table.

    Examples
    --------
    ```bash
    mainsequence time-index-table remove_from_edit <TIME_INDEX_TABLE_UID> <USER_UID>
    ```
    """
    _shareable_user_access_update_impl(
        action_fn=remove_time_index_table_user_from_edit,
        object_label="Time-Index Table",
        action_label="remove_from_edit",
        object_uid=table_uid,
        user_uid=user_uid,
        timeout=timeout,
    )


@time_index_table_group.command("add_team_to_view")
def time_index_table_add_team_to_view_cmd(
    table_uid: str = typer.Argument(..., help="Time-index table UID."),
    team_uid: uuid.UUID = typer.Argument(..., help="Team UID to grant view access."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_team_access_update_impl(
        action_fn=add_time_index_table_team_to_view,
        object_label="Time-Index Table",
        action_label="add_team_to_view",
        object_uid=table_uid,
        team_uid=team_uid,
        timeout=timeout,
    )


@time_index_table_group.command("add_team_to_edit")
def time_index_table_add_team_to_edit_cmd(
    table_uid: str = typer.Argument(..., help="Time-index table UID."),
    team_uid: uuid.UUID = typer.Argument(..., help="Team UID to grant edit access."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_team_access_update_impl(
        action_fn=add_time_index_table_team_to_edit,
        object_label="Time-Index Table",
        action_label="add_team_to_edit",
        object_uid=table_uid,
        team_uid=team_uid,
        timeout=timeout,
    )


@time_index_table_group.command("remove_team_from_view")
def time_index_table_remove_team_from_view_cmd(
    table_uid: str = typer.Argument(..., help="Time-index table UID."),
    team_uid: uuid.UUID = typer.Argument(..., help="Team UID to remove explicit view access from."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_team_access_update_impl(
        action_fn=remove_time_index_table_team_from_view,
        object_label="Time-Index Table",
        action_label="remove_team_from_view",
        object_uid=table_uid,
        team_uid=team_uid,
        timeout=timeout,
    )


@time_index_table_group.command("remove_team_from_edit")
def time_index_table_remove_team_from_edit_cmd(
    table_uid: str = typer.Argument(..., help="Time-index table UID."),
    team_uid: uuid.UUID = typer.Argument(..., help="Team UID to remove explicit edit access from."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_team_access_update_impl(
        action_fn=remove_time_index_table_team_from_edit,
        object_label="Time-Index Table",
        action_label="remove_team_from_edit",
        object_uid=table_uid,
        team_uid=team_uid,
        timeout=timeout,
    )


# ---------- CodeRepository group ----------


@code_repository_list_group.callback(invoke_without_command=True)
def code_repository_list(
    ctx: typer.Context,
    filter_entries: list[str] | None = typer.Option(None, "--filter", help=LIST_FILTER_OPTION_HELP),
    show_filters: bool = typer.Option(
        False, "--show-filters", help="Show the filters supported by this list command and exit."
    ),
):
    """
    List CodeRepositories visible to the authenticated user.

    The output includes platform CodeRepository identity and registered branches.

    Examples
    --------
    ```bash
    mainsequence code-repository list
    ```
    """
    if ctx.invoked_subcommand is not None:
        return

    _resolve_cli_list_filters(
        model_ref=None,
        filter_entries=filter_entries,
        show_filters=show_filters,
        command_label="CodeRepositories",
    )

    _require_login()
    items = get_code_repositories()
    if _emit_json(items):
        return
    typer.echo(_render_code_repositories_table(items))


def _print_code_repository_time_index_table_updates(
    code_repository_id: str | None = typer.Argument(None, help="CodeRepository UID"),
    filter_entries: list[str] | None = None,
    show_filters: bool = False,
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
) -> None:
    """
    List time-index table updates for a CodeRepository.

    Uses SDK client `CodeRepositoryBranch.get_time_index_table_updates()` as the single source of truth
    for payload parsing and shape handling.

    Parameters
    ----------
    code_repository_id:
        Optional CodeRepository UID assertion. Git repository identity remains authoritative.
    timeout:
        Optional request timeout in seconds.

    Examples
    --------
    ```bash
    mainsequence code-repository time-index-table-updates list
    mainsequence code-repository time-index-table-updates list code-repository-uid-123
    mainsequence code-repository time-index-table-updates list code-repository-uid-123 --timeout 60
    ```
    """
    _resolve_cli_list_filters(
        model_ref=None,
        filter_entries=filter_entries,
        show_filters=show_filters,
        command_label="CodeRepository Time-Index Table Updates",
        reserved_filter_descriptions={
            "code_repository_uid": "resolved from the current Git repository"
        },
    )

    _require_login()
    try:
        code_repository_branch_uid = _resolve_code_repository_branch_uid_for_command(code_repository_id)
        updates = get_code_repository_time_index_table_updates(code_repository_branch_uid, timeout=timeout)
    except NotLoggedIn as e:
        error("Not logged in. Run: mainsequence login")
        raise typer.Exit(1) from e
    except ApiError as e:
        error(str(e))
        raise typer.Exit(1) from e

    if _emit_json(updates):
        return

    if not updates:
        info("No time-index table updates found.")
        return

    rows: list[list[str]] = []
    for u in updates:
        output_table = u.get("output_table")
        if isinstance(output_table, dict):
            output_table_value = (
                output_table.get("physical_table_name") or output_table.get("uid") or "-"
            )
        else:
            output_table_value = output_table if output_table is not None else "-"

        details = u.get("update_details")
        if isinstance(details, dict):
            details_uid = details.get("table_update_uid") or "-"
        else:
            details_uid = details if details is not None else "-"

        rows.append(
            [
                str(u.get("uid") or "-"),
                str(u.get("update_hash") or "-"),
                str(output_table_value),
                str(details_uid),
            ]
        )

    print_table(
        "CodeRepository Time-Index Table Updates",
        ["UID", "Update Hash", "Output Table", "Update Details"],
        rows,
    )
    info(f"Total updates: {len(rows)}")


@code_repository_time_index_table_updates_group.command("list")
def code_repository_time_index_table_updates_list_cmd(
    code_repository_id: str | None = typer.Argument(None, help="CodeRepository UID"),
    filter_entries: list[str] | None = typer.Option(None, "--filter", help=LIST_FILTER_OPTION_HELP),
    show_filters: bool = typer.Option(
        False, "--show-filters", help="Show the filters supported by this list command and exit."
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    List time-index table updates for a CodeRepository.

    Examples
    --------
    ```bash
    mainsequence code-repository time-index-table-updates list
    mainsequence code-repository time-index-table-updates list code-repository-uid-123
    mainsequence code-repository time-index-table-updates list code-repository-uid-123 --timeout 60
    ```
    """
    _print_code_repository_time_index_table_updates(
        code_repository_id=code_repository_id,
        filter_entries=filter_entries,
        show_filters=show_filters,
        timeout=timeout,
    )


@code_repository.command("validate-name")
def code_repository_validate_name_cmd(
    code_repository_name: str = typer.Argument(..., help="CodeRepository name to validate."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Validate whether a code repository name is available for creation on the platform.

    Examples
    --------
    ```bash
    mainsequence code-repository validate-name "Rates Platform"
    mainsequence code-repository validate-name tutorial-repository --timeout 60
    ```
    """
    _require_login()

    normalized_code_repository_name = (code_repository_name or "").strip()
    if not normalized_code_repository_name:
        error("CodeRepository name is required.")
        raise typer.Exit(1)

    try:
        payload = validate_code_repository_name(code_repository_name=normalized_code_repository_name, timeout=timeout)
    except ApiError as e:
        error(f"CodeRepository name validation failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(payload):
        return

    normalized = payload.get("normalized") or {}
    print_kv(
        "CodeRepository Name Validation",
        [
            ("CodeRepository Name", str(payload.get("code_repository_name") or normalized_code_repository_name)),
            ("Available", "yes" if payload.get("available") else "no"),
            ("Reason", str(payload.get("reason") or "-")),
            ("Slugified CodeRepository Name", str(normalized.get("slugified_code_repository_name") or "-")),
            ("CodeRepository Library Name", str(normalized.get("repository_library_name") or "-")),
        ],
    )

    suggestions = [str(item) for item in list(payload.get("suggestions") or []) if item is not None]
    if suggestions:
        print_table("Suggested CodeRepository Names", ["CodeRepository Name"], [[item] for item in suggestions])

    if payload.get("available"):
        success(f"CodeRepository name is available: {normalized_code_repository_name}")
        return

    warn(f"CodeRepository name is not available: {normalized_code_repository_name}")
    raise typer.Exit(1)


@code_repository.command("search")
def code_repository_search_cmd(
    q: str = typer.Argument(..., help="CodeRepository search query. Minimum 3 characters."),
    limit: int = typer.Option(
        20, "--limit", min=1, max=100, help="Maximum number of matches to return."
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Search CodeRepositories visible to the authenticated user.

    Uses SDK client `CodeRepository.quick_search()` as the single source of truth.

    Examples
    --------
    ```bash
    mainsequence code-repository search "tutorial"
    mainsequence code-repository search "rates" --limit 10
    mainsequence code-repository search "161"
    ```
    """
    _require_login()

    normalized_query = (q or "").strip()
    if len(normalized_query) < 3:
        error("CodeRepository search failed: Query must contain at least 3 characters.")
        raise typer.Exit(1)

    try:
        code_repositories = search_code_repositories(normalized_query, limit=limit, timeout=timeout)
    except ApiError as e:
        error(f"CodeRepository search failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(code_repositories):
        return

    if code_repositories:
        print_table(
            "CodeRepository Search Results",
            ["UID", "CodeRepository Name"],
            [
                [
                    _code_repository_identity_value(code_repository) or "-",
                    str(code_repository.get("code_repository_name") or "-"),
                ]
                for code_repository in code_repositories
            ],
        )
    else:
        info("No CodeRepositories matched the search.")
    info(f'CodeRepository search matches for "{normalized_query}": {len(code_repositories)}')


@code_repository.command("create")
def code_repository_create_cmd(
    code_repository_name: str | None = typer.Argument(None, help="CodeRepository name"),
    code_repository_type: str = typer.Option("python", "--code-repository-type", help="CodeRepository type"),
    default_base_image_uid: str | None = typer.Option(
        None, "--default-base-image-uid", help="Default base image UID"
    ),
    github_org_uid: str | None = typer.Option(
        None, "--github-org-uid", "--organization-uid", help="GitHub organization UID"
    ),
    env: list[str] | None = typer.Option(
        None,
        "--env",
        help="Environment variable entry in KEY=VALUE form. Repeatable or comma-separated.",
    ),
):
    """
    Create a CodeRepository on the platform.

    If required values are omitted, the command prompts interactively and applies defaults.
    CodeRepository creation creates the logical CodeRepository and its initial `main` CodeRepositoryBranch.

    Parameters
    ----------
    code_repository_name:
        CodeRepository name. If omitted, prompt is shown.
    code_repository_type:
        Immutable type of the logical CodeRepository, shared by every CodeRepositoryBranch.
    default_base_image_uid:
        Default base image UID.
    github_org_uid:
        GitHub organization UID.
    env:
        Environment variable entries in `KEY=VALUE` format.

    Examples
    --------
    ```bash
    mainsequence code-repository create
    mainsequence code-repository create tutorial-repository
    mainsequence code-repository create tutorial-repository --default-base-image-uid <base_image_uid> --github-org-uid <github_org_uid>
    mainsequence code-repository create tutorial-repository --env FOO=bar --env BAZ=qux
    ```
    """
    _require_login()

    try:
        if not code_repository_name:
            code_repository_name = typer.prompt("CodeRepository name").strip()
        code_repository_name = (code_repository_name or "").strip()
        if not code_repository_name:
            error("CodeRepository name is required.")
            raise typer.Exit(1)

        name_validation = validate_code_repository_name(code_repository_name=code_repository_name)
        if not name_validation.get("available"):
            normalized = name_validation.get("normalized") or {}
            reason = str(name_validation.get("reason") or "CodeRepository name is not available.")
            error(reason)
            print_kv(
                "CodeRepository Name Validation",
                [
                    ("CodeRepository Name", str(name_validation.get("code_repository_name") or code_repository_name)),
                    ("Available", "no"),
                    ("Reason", reason),
                    (
                        "Slugified CodeRepository Name",
                        str(normalized.get("slugified_code_repository_name") or "-"),
                    ),
                    ("CodeRepository Library Name", str(normalized.get("repository_library_name") or "-")),
                ],
            )
            suggestions = [
                str(item)
                for item in list(name_validation.get("suggestions") or [])
                if item is not None
            ]
            if suggestions:
                print_table(
                    "Suggested CodeRepository Names", ["CodeRepository Name"], [[item] for item in suggestions]
                )
            raise typer.Exit(1)

        if default_base_image_uid is None:
            img_items = list_code_repository_base_images()
            img_rows: list[list[str]] = []
            for item in img_items:
                uid = _require_item_uid(item, prompt_label="default base image uid")
                name = item.get("title") or f"image-{uid}"
                details = item.get("description") or item.get("latest_digest") or "-"
                img_rows.append([uid, str(name), str(details)])
            default_base_image_uid = _prompt_select_uid(
                title="Available Base Images",
                prompt_label="Default base image uid",
                items=img_items,
                rows=img_rows,
            )

        if github_org_uid is None:
            org_items = list_github_organizations()
            if org_items:
                org_rows: list[list[str]] = []
                for item in org_items:
                    uid = _require_item_uid(item, prompt_label="github organization uid")
                    name = item.get("display_name") or item.get("login") or f"org-{uid}"
                    details = item.get("login") or "-"
                    org_rows.append([uid, str(name), str(details)])
                github_org_uid = _prompt_select_uid(
                    title="Available GitHub Organizations",
                    prompt_label="GitHub organization uid",
                    items=org_items,
                    rows=org_rows,
                )
            else:
                warn(
                    "No GitHub organizations available. CodeRepository will be created without github_org_uid."
                )

        env_entries = list(env or [])
        if not env_entries:
            env_line = typer.prompt(
                "Environment variables (KEY=VALUE, comma-separated, optional)",
                default="",
            ).strip()
            if env_line:
                env_entries = [env_line]

        env_vars: dict[str, str] | None = None
        if env_entries:
            try:
                env_vars = _parse_env_var_entries(env_entries)
            except ValueError as e:
                error(str(e))
                raise typer.Exit(1) from e

        created = create_code_repository(
            code_repository_name=code_repository_name,
            code_repository_type=code_repository_type,
            default_base_image_uid=default_base_image_uid,
            github_org_uid=github_org_uid,
            env_vars=env_vars,
        )
    except ApiError as e:
        error(f"CodeRepository creation failed: {e}")
        raise typer.Exit(1) from e
    except RuntimeError as e:
        error(str(e))
        raise typer.Exit(1) from e

    code_repository_uid = _code_repository_identity_value(created)
    if _emit_json(created):
        return

    success(
        f"CodeRepository created: {created.get('code_repository_name') or code_repository_name} (uid={code_repository_uid or '-'})"
    )

    branch_names = ", ".join(
        str(item.get("repository_branch") or "-")
        for item in list(created.get("branches") or [])
        if isinstance(item, dict)
    )
    print_kv(
        "CodeRepository",
        [
            ("UID", code_repository_uid or "-"),
            ("CodeRepository Name", str(created.get("code_repository_name") or code_repository_name)),
            ("Branches", branch_names or "-"),
        ],
    )
    if code_repository_uid:
        info(f"Next: mainsequence code-repository set-up-locally {code_repository_uid}")


@code_repository.command("delete")
def code_repository_delete_remote_cmd(
    code_repository_id: str = typer.Argument(..., help="CodeRepository UID"),
    delete_repositories: bool = typer.Option(
        False,
        "--delete-repositories/--no-delete-repositories",
        help="Also delete linked repositories in the backend workflow.",
    ),
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not prompt for confirmation"),
):
    """
    Delete a CodeRepository from the platform.

    This does not delete local files unless you run `code-repository delete-local`.

    Parameters
    ----------
    code_repository_id:
        Platform CodeRepository UID.
    delete_repositories:
        Also delete linked repositories on backend workflow.
    yes:
        Skip interactive confirmation.

    Examples
    --------
    ```bash
    mainsequence code-repository delete code-repository-uid-123
    mainsequence code-repository delete code-repository-uid-123 --yes
    mainsequence code-repository delete code-repository-uid-123 --delete-repositories --yes
    ```
    """
    _require_login()

    code_repository_name = f"code-repository-{code_repository_id}"
    code_repository_uid = str(code_repository_id)
    try:
        found = resolve_code_repository(code_repository_id)
        if found and found.get("code_repository_name"):
            code_repository_name = str(found.get("code_repository_name"))
            code_repository_uid = _code_repository_identity_value(found) or code_repository_uid
    except Exception:
        # Best-effort metadata lookup only.
        pass

    if not yes:
        warning = (
            f"This will permanently delete CodeRepository '{code_repository_name}' "
            f"(uid={code_repository_uid}) from the platform.\n"
            "This action cannot be undone."
        )
        if delete_repositories:
            warning += "\nLinked repositories will also be deleted."
        if not typer.confirm(f"{warning}\n\nContinue?", default=False):
            info("Cancelled.")
            raise typer.Exit(0)

    try:
        resp = bulk_delete_code_repositories(
            uids=[code_repository_uid],
            delete_repositories=delete_repositories,
        )
    except NotLoggedIn as e:
        error("Not logged in. Run: mainsequence login")
        raise typer.Exit(1) from e
    except ApiError as e:
        error(f"CodeRepository deletion failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(resp):
        return

    success(
        f"CodeRepository deleted: {code_repository_name} (uid={code_repository_uid}; "
        f"deleted={resp.get('deleted_count', 0)})"
    )
    if isinstance(resp, dict) and resp:
        detail = resp.get("detail") or resp.get("message")
        if detail:
            info(str(detail))


@code_repository.command("can_view")
def code_repository_can_view_cmd(
    code_repository_id: str = typer.Argument(..., help="CodeRepository UID."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    List users who can view one CodeRepository.

    Uses the SDK `ShareableObjectMixin.users_can_view()` path through the `CodeRepository` model.

    Examples
    --------
    ```bash
    mainsequence code-repository can_view 42
    ```
    """
    _shareable_user_list_impl(
        fetch_fn=list_code_repository_users_can_view,
        object_label="CodeRepository",
        access_label="view",
        object_uid=code_repository_id,
        timeout=timeout,
    )


@code_repository.command("can_edit")
def code_repository_can_edit_cmd(
    code_repository_id: str = typer.Argument(..., help="CodeRepository UID."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    List users who can edit one CodeRepository.

    Uses the SDK `ShareableObjectMixin.users_can_edit()` path through the `CodeRepository` model.

    Examples
    --------
    ```bash
    mainsequence code-repository can_edit 42
    ```
    """
    _shareable_user_list_impl(
        fetch_fn=list_code_repository_users_can_edit,
        object_label="CodeRepository",
        access_label="edit",
        object_uid=code_repository_id,
        timeout=timeout,
    )


@code_repository.command("add-label")
def code_repository_add_label_cmd(
    code_repository_id: str = typer.Argument(..., help="CodeRepository UID."),
    labels: list[str] | None = typer.Option(
        None,
        "--label",
        help="Organizational label to add. Repeatable or comma-separated.",
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Add one or more organizational labels to a CodeRepository.

    Labels are helpers for grouping and discovery only. They do not affect runtime behavior or functionality.
    """
    _labelable_object_labels_update_impl(
        action_fn=add_code_repository_labels,
        object_label="CodeRepository",
        action_label="add-label",
        object_uid=code_repository_id,
        labels=labels,
        timeout=timeout,
    )


@code_repository.command("remove-label")
def code_repository_remove_label_cmd(
    code_repository_id: str = typer.Argument(..., help="CodeRepository UID."),
    labels: list[str] | None = typer.Option(
        None,
        "--label",
        help="Organizational label to remove. Repeatable or comma-separated.",
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Remove one or more organizational labels from a code repository.

    Labels are helpers for grouping and discovery only. They do not affect runtime behavior or functionality.
    """
    _labelable_object_labels_update_impl(
        action_fn=remove_code_repository_labels,
        object_label="CodeRepository",
        action_label="remove-label",
        object_uid=code_repository_id,
        labels=labels,
        timeout=timeout,
    )


@code_repository.command("add_to_view")
def code_repository_add_to_view_cmd(
    code_repository_id: str = typer.Argument(..., help="CodeRepository UID."),
    user_uid: uuid.UUID = typer.Argument(..., help="User UID to grant view access."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Grant explicit view access to one user for one code repository.

    Examples
    --------
    ```bash
    mainsequence code-repository add_to_view <CODE_REPOSITORY_UID> <USER_UID>
    ```
    """
    _shareable_user_access_update_impl(
        action_fn=add_code_repository_user_to_view,
        object_label="CodeRepository",
        action_label="add_to_view",
        object_uid=code_repository_id,
        user_uid=user_uid,
        timeout=timeout,
    )


@code_repository.command("add_to_edit")
def code_repository_add_to_edit_cmd(
    code_repository_id: str = typer.Argument(..., help="CodeRepository UID."),
    user_uid: uuid.UUID = typer.Argument(..., help="User UID to grant edit access."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Grant explicit edit access to one user for one code repository.

    Examples
    --------
    ```bash
    mainsequence code-repository add_to_edit <CODE_REPOSITORY_UID> <USER_UID>
    ```
    """
    _shareable_user_access_update_impl(
        action_fn=add_code_repository_user_to_edit,
        object_label="CodeRepository",
        action_label="add_to_edit",
        object_uid=code_repository_id,
        user_uid=user_uid,
        timeout=timeout,
    )


@code_repository.command("remove_from_view")
def code_repository_remove_from_view_cmd(
    code_repository_id: str = typer.Argument(..., help="CodeRepository UID."),
    user_uid: uuid.UUID = typer.Argument(..., help="User UID to remove explicit view access from."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Remove explicit view access from one user for one code repository.

    Examples
    --------
    ```bash
    mainsequence code-repository remove_from_view <CODE_REPOSITORY_UID> <USER_UID>
    ```
    """
    _shareable_user_access_update_impl(
        action_fn=remove_code_repository_user_from_view,
        object_label="CodeRepository",
        action_label="remove_from_view",
        object_uid=code_repository_id,
        user_uid=user_uid,
        timeout=timeout,
    )


@code_repository.command("remove_from_edit")
def code_repository_remove_from_edit_cmd(
    code_repository_id: str = typer.Argument(..., help="CodeRepository UID."),
    user_uid: uuid.UUID = typer.Argument(..., help="User UID to remove explicit edit access from."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Remove explicit edit access from one user for one code repository.

    Examples
    --------
    ```bash
    mainsequence code-repository remove_from_edit <CODE_REPOSITORY_UID> <USER_UID>
    ```
    """
    _shareable_user_access_update_impl(
        action_fn=remove_code_repository_user_from_edit,
        object_label="CodeRepository",
        action_label="remove_from_edit",
        object_uid=code_repository_id,
        user_uid=user_uid,
        timeout=timeout,
    )


@code_repository.command("add_team_to_view")
def code_repository_add_team_to_view_cmd(
    code_repository_id: str = typer.Argument(..., help="CodeRepository UID."),
    team_uid: uuid.UUID = typer.Argument(..., help="Team UID to grant view access."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_team_access_update_impl(
        action_fn=add_code_repository_team_to_view,
        object_label="CodeRepository",
        action_label="add_team_to_view",
        object_uid=code_repository_id,
        team_uid=team_uid,
        timeout=timeout,
    )


@code_repository.command("add_team_to_edit")
def code_repository_add_team_to_edit_cmd(
    code_repository_id: str = typer.Argument(..., help="CodeRepository UID."),
    team_uid: uuid.UUID = typer.Argument(..., help="Team UID to grant edit access."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_team_access_update_impl(
        action_fn=add_code_repository_team_to_edit,
        object_label="CodeRepository",
        action_label="add_team_to_edit",
        object_uid=code_repository_id,
        team_uid=team_uid,
        timeout=timeout,
    )


@code_repository.command("remove_team_from_view")
def code_repository_remove_team_from_view_cmd(
    code_repository_id: str = typer.Argument(..., help="CodeRepository UID."),
    team_uid: uuid.UUID = typer.Argument(..., help="Team UID to remove explicit view access from."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_team_access_update_impl(
        action_fn=remove_code_repository_team_from_view,
        object_label="CodeRepository",
        action_label="remove_team_from_view",
        object_uid=code_repository_id,
        team_uid=team_uid,
        timeout=timeout,
    )


@code_repository.command("remove_team_from_edit")
def code_repository_remove_team_from_edit_cmd(
    code_repository_id: str = typer.Argument(..., help="CodeRepository UID."),
    team_uid: uuid.UUID = typer.Argument(..., help="Team UID to remove explicit edit access from."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_team_access_update_impl(
        action_fn=remove_code_repository_team_from_edit,
        object_label="CodeRepository",
        action_label="remove_team_from_edit",
        object_uid=code_repository_id,
        team_uid=team_uid,
        timeout=timeout,
    )


def _code_repository_resources_list_impl(
    code_repository_id: str | None,
    path: str | None,
    filter_entries: list[str] | None,
    show_filters: bool,
    timeout: int | None,
) -> None:
    filters = _resolve_cli_list_filters(
        model_ref=CODE_REPOSITORY_RESOURCE_MODEL_REF,
        filter_entries=filter_entries,
        show_filters=show_filters,
        command_label="CodeRepository Resources",
        reserved_filter_descriptions={
            "code_repository_branch_uid": "always set from the selected CodeRepositoryBranch",
            "repo_commit_sha": "always set from the upstream remote branch head commit",
        },
    )

    _require_login()

    code_repository_dir = _resolve_code_repository_dir(code_repository_id, path)
    try:
        upstream, repo_commit_sha = _get_remote_branch_head_commit(code_repository_dir)
    except RuntimeError as e:
        error(str(e))
        raise typer.Exit(1) from e

    try:
        code_repository_branch_uid = _resolve_code_repository_branch_uid_for_command(
            code_repository_id,
            code_repository_dir=code_repository_dir,
        )
        resources = list_code_repository_resources(
            code_repository_branch_uid=code_repository_branch_uid,
            repo_commit_sha=repo_commit_sha,
            filters=filters,
            timeout=timeout,
        )
    except ApiError as e:
        error(f"CodeRepository resources fetch failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(resources):
        return

    info(f"Using repo_commit_sha={repo_commit_sha} from {upstream}.")

    rows: list[list[str]] = []
    for resource in resources:
        rows.append(
            [
                str(resource.get("uid") or "-"),
                str(resource.get("name") or "-"),
                str(resource.get("resource_type") or "-"),
                str(resource.get("path") or "-"),
                str(resource.get("filesize") or "-"),
                str(resource.get("last_modified") or "-"),
            ]
        )

    if rows:
        print_table(
            "CodeRepository Resources",
            ["UID", "Name", "Type", "Path", "File Size", "Last Modified"],
            rows,
        )
    else:
        info("No code repository resources found.")
    info(f"Total code repository resources: {len(resources)}")


@code_repository_resources_group.command("list")
def code_repository_code_repository_resource_list_cmd(
    code_repository_id: str | None = typer.Argument(
        None, help="Optional CodeRepository UID assertion; Git repository identity is authoritative."
    ),
    path: str | None = typer.Option(
        None, "--path", help="CodeRepository repository path (default: current code repository)"
    ),
    filter_entries: list[str] | None = typer.Option(None, "--filter", help=LIST_FILTER_OPTION_HELP),
    show_filters: bool = typer.Option(
        False, "--show-filters", help="Show the filters supported by this list command and exit."
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    List code repository resources for the current code repository at the head commit of the remote branch.

    Uses SDK client `CodeRepositoryResource.filter()` as the single source of truth and always applies
    the standard `repo_commit_sha` filter resolved from the current upstream branch head.

    Parameters
    ----------
    code_repository_id:
        Optional CodeRepository UID assertion.
    path:
        Local repository path used for Git context and remote branch head resolution.
    timeout:
        Request timeout in seconds.

    Examples
    --------
    ```bash
    mainsequence code-repository resources list
    mainsequence code-repository resources list code-repository-uid-123
    mainsequence code-repository resources list --path .
    ```
    """
    _code_repository_resources_list_impl(
        code_repository_id=code_repository_id,
        path=path,
        filter_entries=filter_entries,
        show_filters=show_filters,
        timeout=timeout,
    )


def _code_repository_resource_release_create_impl(
    *,
    release_kind: str,
    code_repository_id: str | None,
    resource_uid: str | None,
    path: str | None,
    related_image_uid: str | None,
    cpu_request: str | None,
    memory_request: str | None,
    gpu_request: str | None,
    gpu_type: str | None,
    spot: bool | None,
    automatic_deployment: bool | None,
    timeout: int | None,
) -> None:
    _require_login()

    code_repository_dir = _resolve_code_repository_dir(code_repository_id, path)
    try:
        code_repository_branch_uid = _resolve_code_repository_branch_uid_for_command(
            code_repository_id,
            code_repository_dir=code_repository_dir,
        )
        code_repository_images = list_code_repository_images(
            related_code_repository_branch_uid=code_repository_branch_uid,
            timeout=timeout,
        )
    except ApiError as e:
        error(f"CodeRepository images fetch failed: {e}")
        raise typer.Exit(1) from e

    if not code_repository_images:
        error("No code repository images are available. Create an image first.")
        raise typer.Exit(1)

    if related_image_uid is None:
        image_rows: list[list[str]] = []
        for image in code_repository_images:
            image_rows.append(
                [
                    str(image.get("uid") or ""),
                    str(image.get("code_repository_commit_hash") or "-"),
                    _format_base_image_label(image.get("base_image")),
                ]
            )
        related_image_uid = _prompt_select_uid(
            title="Available CodeRepository Images",
            prompt_label="Related image UID",
            items=code_repository_images,
            rows=image_rows,
        )

    selected_image = _find_image_by_uid(code_repository_images, related_image_uid)
    if not selected_image:
        error(f"Related image not found: {related_image_uid}")
        raise typer.Exit(1)

    repo_commit_sha = str(selected_image.get("code_repository_commit_hash") or "").strip()
    if not repo_commit_sha:
        error("The selected image does not expose code_repository_commit_hash.")
        raise typer.Exit(1)

    resource_type = RESOURCE_RELEASE_RESOURCE_TYPE_MAP.get(release_kind)
    if not resource_type:
        error(f"Unsupported release kind: {release_kind}")
        raise typer.Exit(1)

    try:
        resources = list_code_repository_resources(
            code_repository_branch_uid=code_repository_branch_uid,
            repo_commit_sha=repo_commit_sha,
            resource_type=resource_type,
            timeout=timeout,
        )
    except ApiError as e:
        error(f"CodeRepository resources fetch failed: {e}")
        raise typer.Exit(1) from e

    if not resources:
        error(
            "No code repository resources match the selected image commit and release type. "
            f"Expected resource_type={resource_type!r} for release_kind={release_kind!r}."
        )
        raise typer.Exit(1)

    if resource_uid is None:
        resource_rows: list[list[str]] = []
        for resource in resources:
            resource_rows.append(
                [
                    str(resource.get("uid") or ""),
                    str(resource.get("name") or "-"),
                    f"{str(resource.get('resource_type') or '-')}: {str(resource.get('path') or '-')}",
                ]
            )
        resource_uid = _prompt_select_uid(
            title="CodeRepository Resources Matching Selected Image and Release Type",
            prompt_label="Resource UID",
            items=resources,
            rows=resource_rows,
        )

    resource_uids = {
        str(resource.get("uid")) for resource in resources if resource.get("uid") is not None
    }
    if str(resource_uid) not in resource_uids:
        error("Selected resource does not match the selected image commit and release type.")
        raise typer.Exit(1)

    try:
        cpu_request, memory_request, spot, used_defaults = _resolve_compute_defaults(
            cpu_request=cpu_request,
            memory_request=memory_request,
            spot=spot,
        )
    except ValueError as e:
        error(str(e))
        raise typer.Exit(1) from e

    if used_defaults:
        default_parts: list[str] = []
        if "cpu_request" in used_defaults:
            default_parts.append(f"cpu_request={cpu_request}")
        if "memory_request" in used_defaults:
            default_parts.append(f"memory_request={memory_request}")
        if "spot" in used_defaults:
            default_parts.append(f"spot={'true' if spot else 'false'}")
        info("Using defaults: " + ", ".join(default_parts) + ".")

    try:
        created = create_code_repository_resource_release(
            release_kind=release_kind,
            resource_uid=resource_uid,
            related_image_uid=related_image_uid,
            cpu_request=cpu_request,
            memory_request=memory_request,
            gpu_request=gpu_request,
            gpu_type=gpu_type,
            spot=spot,
            automatic_deployment=automatic_deployment,
            timeout=timeout,
        )
    except ApiError as e:
        error(f"CodeRepository resource release creation failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(created):
        return

    success(f"CodeRepository resource release created: uid={created.get('uid') or '-'}")
    print_kv(
        "CodeRepository Resource Release",
        [
            ("UID", str(created.get("uid") or "-")),
            ("Release Kind", release_kind),
            ("Resource", str(created.get("resource") or resource_uid)),
            (
                "Related Image",
                _format_related_image_label(created.get("related_image") or related_image_uid),
            ),
            ("CPU Request", str(created.get("cpu_request") or cpu_request)),
            ("Memory Request", str(created.get("memory_request") or memory_request)),
            ("GPU Request", str(created.get("gpu_request") or gpu_request or "-")),
            ("GPU Type", str(created.get("gpu_type") or gpu_type or "-")),
            ("Spot", str(created.get("spot") if created.get("spot") is not None else spot).lower()),
            (
                "Automatic Deployment",
                str(
                    created.get("automatic_deployment")
                    if created.get("automatic_deployment") is not None
                    else bool(automatic_deployment)
                ).lower(),
            ),
        ],
    )


@code_repository_resources_group.command("create_dashboard")
def code_repository_code_repository_resource_create_dashboard_cmd(
    code_repository_id: str | None = typer.Argument(
        None, help="Optional CodeRepository UID assertion; Git repository identity is authoritative."
    ),
    resource_uid: str | None = typer.Option(None, "--resource-uid", help="CodeRepository resource UID."),
    path: str | None = typer.Option(
        None, "--path", help="CodeRepository repository path (default: current code repository)"
    ),
    related_image_uid: str | None = typer.Option(
        None, "--related-image-uid", help="CodeRepository image UID."
    ),
    cpu_request: str | None = typer.Option(
        None, "--cpu-request", help="CPU request (accepts 0.5 or 500m; default: 0.25)."
    ),
    memory_request: str | None = typer.Option(
        None, "--memory-request", help="Memory request (accepts 1 or 1Gi; default: 0.5)."
    ),
    gpu_request: str | None = typer.Option(None, "--gpu-request", help="GPU request count."),
    gpu_type: str | None = typer.Option(None, "--gpu-type", help="GPU accelerator type."),
    spot: bool | None = typer.Option(
        None, "--spot/--no-spot", help="Whether to prefer spot capacity."
    ),
    automatic_deployment: bool | None = typer.Option(
        None,
        "--automatic-deployment/--no-automatic-deployment",
        help="Opt the release into repository-sync CI/CD rotation.",
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Create a Streamlit dashboard release from a code repository resource.

    The command first lets the user select a code repository image and then filters resources so
    only resources with `repo_commit_sha == related_image.code_repository_commit_hash` are eligible.
    """
    _code_repository_resource_release_create_impl(
        release_kind="streamlit_dashboard",
        code_repository_id=code_repository_id,
        resource_uid=resource_uid,
        path=path,
        related_image_uid=related_image_uid,
        cpu_request=cpu_request,
        memory_request=memory_request,
        gpu_request=gpu_request,
        gpu_type=gpu_type,
        spot=spot,
        automatic_deployment=automatic_deployment,
        timeout=timeout,
    )


@code_repository_resources_group.command("create_fastapi")
def code_repository_code_repository_resource_create_fastapi_cmd(
    code_repository_id: str | None = typer.Argument(
        None, help="Optional CodeRepository UID assertion; Git repository identity is authoritative."
    ),
    resource_uid: str | None = typer.Option(None, "--resource-uid", help="CodeRepository resource UID."),
    path: str | None = typer.Option(
        None, "--path", help="CodeRepository repository path (default: current code repository)"
    ),
    related_image_uid: str | None = typer.Option(
        None, "--related-image-uid", help="CodeRepository image UID."
    ),
    cpu_request: str | None = typer.Option(
        None, "--cpu-request", help="CPU request (accepts 0.5 or 500m; default: 0.25)."
    ),
    memory_request: str | None = typer.Option(
        None, "--memory-request", help="Memory request (accepts 1 or 1Gi; default: 0.5)."
    ),
    gpu_request: str | None = typer.Option(None, "--gpu-request", help="GPU request count."),
    gpu_type: str | None = typer.Option(None, "--gpu-type", help="GPU accelerator type."),
    spot: bool | None = typer.Option(
        None, "--spot/--no-spot", help="Whether to prefer spot capacity."
    ),
    automatic_deployment: bool | None = typer.Option(
        None,
        "--automatic-deployment/--no-automatic-deployment",
        help="Opt the release into repository-sync CI/CD rotation.",
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Create a FastAPI release from a code repository resource.

    The command first lets the user select a code repository image and then filters resources so
    only resources with `repo_commit_sha == related_image.code_repository_commit_hash` are eligible.
    """
    _code_repository_resource_release_create_impl(
        release_kind="fastapi",
        code_repository_id=code_repository_id,
        resource_uid=resource_uid,
        path=path,
        related_image_uid=related_image_uid,
        cpu_request=cpu_request,
        memory_request=memory_request,
        gpu_request=gpu_request,
        gpu_type=gpu_type,
        spot=spot,
        automatic_deployment=automatic_deployment,
        timeout=timeout,
    )


def _code_repository_resource_release_delete_impl(
    *,
    release_uid: str,
    expected_release_kind: str,
    yes: bool,
    timeout: int | None,
) -> None:
    _require_login()

    try:
        release = get_resource_release(
            release_uid=release_uid,
            expected_release_kind=expected_release_kind,
            timeout=timeout,
        )
    except ApiError as e:
        error(f"CodeRepository resource release fetch failed: {e}")
        raise typer.Exit(1) from e

    release_label = RESOURCE_RELEASE_LABEL_MAP.get(
        expected_release_kind,
        f"{expected_release_kind} release",
    )
    _confirm_delete_action(
        preview_title="CodeRepository Resource Release Delete Preview",
        preview_items=_format_resource_release_delete_preview(release),
        prompt_text=f"Delete {release_label} {release_uid}?",
        yes=yes,
    )

    try:
        deleted = delete_resource_release(
            release_uid=release_uid,
            expected_release_kind=expected_release_kind,
            timeout=timeout,
        )
    except ApiError as e:
        error(f"CodeRepository resource release deletion failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(deleted):
        return

    success(f"CodeRepository resource release deleted: uid={release_uid}")
    print_kv("Deleted CodeRepository Resource Release", _format_resource_release_delete_preview(deleted))


@code_repository_resources_group.command("delete_dashboard")
def code_repository_code_repository_resource_delete_dashboard_cmd(
    release_uid: str = pydantic_argument(
        RESOURCE_RELEASE_MODEL_REF, "uid", ..., help="Dashboard resource release UID."
    ),
    yes: bool = typer.Option(False, "--yes", help="Delete without confirmation."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Delete a dashboard resource release.

    Examples
    --------
    ```bash
    mainsequence code-repository resources delete_dashboard <RELEASE_UID>
    mainsequence code-repository resources delete_dashboard <RELEASE_UID> --yes
    ```
    """
    _code_repository_resource_release_delete_impl(
        release_uid=release_uid,
        expected_release_kind="streamlit_dashboard",
        yes=yes,
        timeout=timeout,
    )


@code_repository_resources_group.command("delete_fastapi")
def code_repository_code_repository_resource_delete_fastapi_cmd(
    release_uid: str = pydantic_argument(
        RESOURCE_RELEASE_MODEL_REF, "uid", ..., help="FastAPI resource release UID."
    ),
    yes: bool = typer.Option(False, "--yes", help="Delete without confirmation."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Delete a FastAPI resource release.

    Examples
    --------
    ```bash
    mainsequence code-repository resources delete_fastapi <RELEASE_UID>
    mainsequence code-repository resources delete_fastapi <RELEASE_UID> --yes
    ```
    """
    _code_repository_resource_release_delete_impl(
        release_uid=release_uid,
        expected_release_kind="fastapi",
        yes=yes,
        timeout=timeout,
    )


@code_repository_resources_group.command("logs")
def code_repository_resource_logs_cmd(
    release_uid: str = typer.Argument(..., help="ResourceRelease UID."),
    start: float | None = typer.Option(
        None, "--start", help="Window start as epoch seconds or milliseconds."
    ),
    end: float | None = typer.Option(
        None, "--end", help="Window end as epoch seconds or milliseconds."
    ),
    cursor: str | None = typer.Option(None, "--cursor", help="Opaque continuation cursor."),
    limit: int | None = typer.Option(None, "--limit", min=1, max=500),
    severity: str | None = typer.Option(None, "--severity"),
    request_id: str | None = typer.Option(None, "--request-id"),
    event: str | None = typer.Option(None, "--event"),
    outcome: str | None = typer.Option(None, "--outcome"),
    timeout: int | None = typer.Option(None, "--timeout"),
):
    """Read application logs owned by a runtime-backed ResourceRelease."""
    _require_login()
    try:
        payload = get_resource_release_logs(
            release_uid,
            start=start,
            end=end,
            cursor=cursor,
            limit=limit,
            severity=severity,
            request_id=request_id,
            event=event,
            outcome=outcome,
            timeout=timeout,
        )
    except ApiError as e:
        error(f"Resource release logs fetch failed: {e}")
        raise typer.Exit(1) from e
    _render_owner_logs(payload, title="Resource Release Logs")


@code_repository_resources_group.command("resource-usage")
def code_repository_resource_usage_cmd(
    release_uid: str = typer.Argument(..., help="ResourceRelease UID."),
    start: float | None = typer.Option(
        None, "--start", help="Window start as epoch seconds or milliseconds."
    ),
    end: float | None = typer.Option(
        None, "--end", help="Window end as epoch seconds or milliseconds."
    ),
    timeout: int | None = typer.Option(None, "--timeout"),
):
    """Read aggregate runtime usage owned by a ResourceRelease."""
    _require_login()
    try:
        payload = get_resource_release_resource_usage(
            release_uid,
            start=start,
            end=end,
            timeout=timeout,
        )
    except ApiError as e:
        error(f"Resource release usage fetch failed: {e}")
        raise typer.Exit(1) from e
    _render_owner_resource_usage(payload, title="Resource Release Resource Usage")


def _code_repository_images_list_impl(
    code_repository_id: str | None,
    path: str | None,
    filter_entries: list[str] | None,
    show_filters: bool,
    timeout: int | None,
) -> None:
    filters = _resolve_cli_list_filters(
        model_ref=CODE_REPOSITORY_IMAGE_MODEL_REF,
        filter_entries=filter_entries,
        show_filters=show_filters,
        command_label="CodeRepository Images",
        reserved_filter_descriptions={
            "related_code_repository_branch_uid": "always set from the selected CodeRepositoryBranch",
        },
    )

    _require_login()

    code_repository_dir = _resolve_code_repository_dir(code_repository_id, path)
    try:
        code_repository_branch_uid = _resolve_code_repository_branch_uid_for_command(
            code_repository_id,
            code_repository_dir=code_repository_dir,
        )
        images = list_code_repository_images(
            related_code_repository_branch_uid=code_repository_branch_uid,
            filters=filters,
            timeout=timeout,
        )
    except ApiError as e:
        error(f"CodeRepository images fetch failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(images):
        return

    rows: list[list[str]] = []
    for image in images:
        rows.append(
            [
                str(image.get("uid") or "-"),
                str(image.get("code_repository_commit_hash") or "-"),
                _format_base_image_label(image.get("base_image")),
            ]
        )

    if rows:
        print_table("CodeRepository Images", ["UID", "CodeRepository Repo Hash", "Base Image"], rows)
    else:
        info("No code repository images.")
    info(f"Total images: {len(images)}")


@code_repository_images_group.command("list")
def code_repository_images_list_cmd(
    code_repository_id: str | None = typer.Argument(
        None, help="Optional CodeRepository UID assertion; Git repository identity is authoritative."
    ),
    path: str | None = typer.Option(
        None, "--path", help="CodeRepository repository path (default: current code repository)"
    ),
    filter_entries: list[str] | None = typer.Option(None, "--filter", help=LIST_FILTER_OPTION_HELP),
    show_filters: bool = typer.Option(
        False, "--show-filters", help="Show the filters supported by this list command and exit."
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    List images for a CodeRepository.

    Uses SDK client `CodeRepositoryImage.filter()` as the single source of truth.

    Parameters
    ----------
    code_repository_id:
        Optional CodeRepository UID assertion.
    path:
        Local repository path used for Git-native context resolution.
    timeout:
        Request timeout in seconds.

    Examples
    --------
    ```bash
    mainsequence code-repository images list
    mainsequence code-repository images list code-repository-uid-123
    mainsequence code-repository images list code-repository-uid-123 --path .
    ```
    """
    _code_repository_images_list_impl(
        code_repository_id=code_repository_id,
        path=path,
        filter_entries=filter_entries,
        show_filters=show_filters,
        timeout=timeout,
    )


def _code_repository_images_delete_impl(
    *,
    image_uid: str,
    yes: bool,
    timeout: int | None,
) -> None:
    _require_login()

    try:
        image = get_code_repository_image(image_uid=image_uid, timeout=timeout)
    except ApiError as e:
        error(f"CodeRepository image fetch failed: {e}")
        raise typer.Exit(1) from e

    _confirm_delete_action(
        preview_title="CodeRepository Image Delete Preview",
        preview_items=_format_code_repository_image_delete_preview(image),
        prompt_text=f"Delete code repository image {image_uid}?",
        yes=yes,
    )

    try:
        deleted = delete_code_repository_image(image_uid=image_uid, timeout=timeout)
    except ApiError as e:
        error(f"CodeRepository image deletion failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(deleted):
        return

    success(f"CodeRepository image deleted: uid={image_uid}")
    print_kv("Deleted CodeRepository Image", _format_code_repository_image_delete_preview(deleted))


@code_repository_images_group.command("delete")
def code_repository_images_delete_cmd(
    image_uid: str = typer.Argument(..., help="CodeRepository image UID."),
    yes: bool = typer.Option(False, "--yes", help="Delete without confirmation."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Delete a code repository image.

    Examples
    --------
    ```bash
    mainsequence code-repository images delete <IMAGE_UID>
    mainsequence code-repository images delete <IMAGE_UID> --yes
    ```
    """
    _code_repository_images_delete_impl(image_uid=image_uid, yes=yes, timeout=timeout)


def _code_repository_images_create_impl(
    code_repository_id: str | None,
    code_repository_commit_hash: str | None,
    path: str | None,
    base_image_uid: str | None,
    timeout: int,
    poll_interval: int,
) -> None:
    _require_login()

    code_repository_dir = _resolve_code_repository_dir(code_repository_id, path)

    try:
        code_repository_branch_uid = _resolve_code_repository_branch_uid_for_command(
            code_repository_id,
            code_repository_dir=code_repository_dir,
        )
        existing_images = list_code_repository_images(
            related_code_repository_branch_uid=code_repository_branch_uid,
            timeout=timeout,
        )
    except ApiError as e:
        error(f"CodeRepository images fetch failed: {e}")
        raise typer.Exit(1) from e
    images_by_hash = _group_code_repository_images_by_hash(existing_images)

    emit_json = _json_output_enabled()

    pending_commits = _list_unpushed_commits(code_repository_dir)
    if pending_commits:
        pending_hashes = ", ".join(
            c["short_hash"] for c in pending_commits[:3] if c.get("short_hash")
        )
        suffix = f" Pending: {pending_hashes}." if pending_hashes else ""
        warn(
            f"{len(pending_commits)} local commit(s) have not been pushed yet. "
            "Only pushed commits can be used for code_repository_commit_hash."
            f"{suffix}"
        )

    code_repository_commit_hash = (code_repository_commit_hash or "").strip()
    if not code_repository_commit_hash:
        try:
            commits = _list_pushed_commits(code_repository_dir)
        except RuntimeError as e:
            error(str(e))
            raise typer.Exit(1) from e

        rows = [
            [
                c["hash"],
                c["date"],
                c["subject"] or "-",
                _format_image_uids(images_by_hash.get(c["hash"], [])),
            ]
            for c in commits
        ]
        print_table("Pushed Commits", ["Hash", "Date/Time", "Subject", "Image UIDs"], rows)
        code_repository_commit_hash = typer.prompt("code_repository_commit_hash", default=commits[0]["hash"]).strip()

    if not code_repository_commit_hash:
        error("code_repository_commit_hash is required.")
        raise typer.Exit(1)

    try:
        code_repository_commit_hash = _resolve_full_commit_hash(code_repository_dir, code_repository_commit_hash)
    except RuntimeError as e:
        error(str(e))
        raise typer.Exit(1) from e

    if not _is_pushed_commit(code_repository_dir, code_repository_commit_hash):
        error(
            "code_repository_commit_hash must reference a commit that has already been pushed to the remote."
        )
        raise typer.Exit(1)

    existing_for_hash = images_by_hash.get(code_repository_commit_hash, [])
    if existing_for_hash:
        warn("This commit already has code repository image(s): " + _format_image_uids(existing_for_hash))

    try:
        if base_image_uid is None:
            img_items = list_code_repository_base_images()
            img_rows: list[list[str]] = []
            for item in img_items:
                name = item.get("title") or f"image-{item.get('uid')}"
                details = item.get("description") or item.get("latest_digest") or "-"
                img_rows.append([str(item.get("uid", "")), str(name), str(details)])
            base_image_uid = _prompt_select_uid(
                title="Available Base Images",
                prompt_label="Base image UID",
                items=img_items,
                rows=img_rows,
            )

        created = create_code_repository_image(
            code_repository_commit_hash=code_repository_commit_hash,
            related_code_repository_branch_uid=code_repository_branch_uid,
            base_image_uid=base_image_uid,
            timeout=timeout,
        )
    except ApiError as e:
        error(f"CodeRepository image creation failed: {e}")
        raise typer.Exit(1) from e
    except RuntimeError as e:
        error(str(e))
        raise typer.Exit(1) from e

    if not emit_json:
        success(f"CodeRepository image created: uid={created.get('uid') or '-'}")

    image_uid = created.get("uid")
    if image_uid is not None and created.get("is_ready") is False:
        wait_deadline = time.monotonic() + max(int(timeout), 0)
        attempt = 0
        info(
            "CodeRepository image is still building. "
            f"Waiting until is_ready=true (poll every {poll_interval}s, timeout {timeout}s)."
        )
        while time.monotonic() < wait_deadline:
            attempt += 1
            remaining = max(wait_deadline - time.monotonic(), 0.0)
            sleep_for = min(max(int(poll_interval), 1), remaining)
            if sleep_for > 0:
                with status(
                    f"CodeRepository image not ready yet (attempt {attempt}). Next check in {int(sleep_for)}s..."
                ):
                    time.sleep(sleep_for)

            try:
                polled_images = list_code_repository_images(
                    related_code_repository_branch_uid=code_repository_branch_uid,
                    timeout=timeout,
                )
            except ApiError as e:
                warn(f"CodeRepository image status poll failed (attempt {attempt}): {e}")
                continue

            latest = next(
                (img for img in polled_images if str(img.get("uid")) == str(image_uid)), None
            )
            if latest is None:
                warn(f"CodeRepository image {image_uid} was not visible yet on poll attempt {attempt}.")
                continue

            created = latest
            if created.get("is_ready") is True:
                if not emit_json:
                    success("CodeRepository image is ready.")
                break
            if not emit_json:
                info("CodeRepository image still building. Continuing to poll...")
        else:
            if not emit_json:
                warn(
                    f"Timed out after {timeout}s waiting for code repository image {image_uid} to become ready. "
                    "It may still be building on the backend."
                )

    if _emit_json(created):
        return

    base_image_value = created.get("base_image")
    if isinstance(base_image_value, dict):
        base_image_value = base_image_value.get("uid") or base_image_value.get("title") or "-"

    print_kv(
        "CodeRepository Image",
        [
            ("UID", str(created.get("uid") or "-")),
            ("CodeRepository UID", str(code_repository_id)),
            ("CodeRepository Repo Hash", code_repository_commit_hash),
            ("Base Image", str(base_image_value or base_image_uid or "-")),
            (
                "Is Ready",
                str(created.get("is_ready")) if created.get("is_ready") is not None else "-",
            ),
        ],
    )


@code_repository_images_group.command("create")
def code_repository_images_create_cmd(
    code_repository_id: str | None = typer.Argument(
        None, help="Optional CodeRepository UID assertion; Git repository identity is authoritative."
    ),
    code_repository_commit_hash: str | None = typer.Argument(
        None,
        help="Git commit hash for the image build. Must already be pushed to the remote.",
    ),
    path: str | None = typer.Option(
        None, "--path", help="CodeRepository repository path (default: current code repository)"
    ),
    base_image_uid: str | None = typer.Option(
        None, "--base-image-uid", help="CodeRepository base image UID"
    ),
    timeout: int = typer.Option(
        300, "--timeout", help="Maximum wait time in seconds for the image to become ready."
    ),
    poll_interval: int = typer.Option(
        30, "--poll-interval", help="Polling interval in seconds while waiting for is_ready=true."
    ),
):
    """
    Create a code repository image from a pushed git commit.

    The current Git repository and attached branch select the CodeRepositoryBranch.
    If `code_repository_commit_hash` is omitted, the command shows
    only commits already present on the remote and prompts for a selection.

    Parameters
    ----------
    code_repository_id:
        Optional CodeRepository UID assertion.
    code_repository_commit_hash:
        Git commit hash already pushed to remote.
    path:
        Local repository path. Defaults to current code repository folder.
    base_image_uid:
        CodeRepository base image UID. If omitted, prompt from available base images.
    timeout:
        Maximum wait time in seconds for the image to become ready.
    poll_interval:
        Polling interval in seconds while waiting for `is_ready=true`.

    Examples
    --------
    ```bash
    mainsequence code-repository images create
    mainsequence code-repository images create code-repository-uid-123
    mainsequence code-repository images create code-repository-uid-123 4a1b2c3d
    mainsequence code-repository images create code-repository-uid-123 --path .
    mainsequence code-repository images create code-repository-uid-123 --timeout 600 --poll-interval 15
    ```
    """
    _code_repository_images_create_impl(
        code_repository_id=code_repository_id,
        code_repository_commit_hash=code_repository_commit_hash,
        path=path,
        base_image_uid=base_image_uid,
        timeout=timeout,
        poll_interval=poll_interval,
    )


def _code_repository_jobs_list_impl(
    code_repository_id: str | None,
    path: str | None,
    filter_entries: list[str] | None,
    show_filters: bool,
    timeout: int | None,
) -> None:
    filters = _resolve_cli_list_filters(
        model_ref=JOB_MODEL_REF,
        filter_entries=filter_entries,
        show_filters=show_filters,
        command_label="CodeRepository Jobs",
        reserved_filter_descriptions={
            "code_repository_branch_uid": "always scoped to the selected CodeRepositoryBranch",
        },
    )

    _require_login()

    code_repository_dir = _resolve_code_repository_dir(code_repository_id, path)
    try:
        code_repository_branch_uid = _resolve_code_repository_branch_uid_for_command(
            code_repository_id,
            code_repository_dir=code_repository_dir,
        )
        jobs = list_code_repository_jobs(
            code_repository_branch_uid=code_repository_branch_uid,
            filters=filters,
            timeout=timeout,
        )
    except ApiError as e:
        error(f"CodeRepository jobs fetch failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(jobs):
        return

    rows: list[list[str]] = []
    for job in jobs:
        rows.append(
            [
                str(job.get("uid") or "-"),
                str(job.get("name") or "-"),
                str(job.get("code_repository_commit_hash") or "-"),
                str(job.get("execution_path") or "-"),
                str(job.get("app_name") or "-"),
                _format_job_schedule_summary(job.get("task_schedule")),
                _format_related_image_label(job.get("related_image")),
            ]
        )

    if rows:
        print_table(
            "CodeRepository Jobs",
            [
                "UID",
                "Name",
                "Repo Hash",
                "Execution Path",
                "App Name",
                "Schedule",
                "Related Image",
            ],
            rows,
        )

        deployment_rows = [
            [
                str(job.get("uid") or "-"),
                str(job.get("code_repository_commit_hash") or "-"),
                str(job.get("related_image_uid") or job.get("related_image") or "-"),
                str(job.get("image_status") or "-"),
                str(bool(job.get("automatic_deployment"))).lower(),
                str(
                    (job.get("automatic_redeployment_policy") or {}).get("tag_regex")
                    or "every qualifying exact event"
                ),
            ]
            for job in jobs
        ]
        print_table(
            "CodeRepository Job Image And Promotion State",
            [
                "Job UID",
                "Exact Commit",
                "Image UID",
                "Image Status",
                "Automatic Deployment",
                "Promotion Tag Regex",
            ],
            deployment_rows,
        )
    else:
        info("No CodeRepository jobs.")
    info(f"Total jobs: {len(jobs)}")


def _code_repository_job_runs_list_impl(
    job_uid: str,
    filter_entries: list[str] | None,
    show_filters: bool,
    timeout: int | None,
) -> None:
    filters = _resolve_cli_list_filters(
        model_ref=JOB_RUN_MODEL_REF,
        filter_entries=filter_entries,
        show_filters=show_filters,
        command_label="CodeRepository Job Runs",
        reserved_filter_descriptions={"job__uid": "always set from JOB_UID"},
    )

    _require_login()

    try:
        runs = list_code_repository_job_runs(job_uid=job_uid, filters=filters, timeout=timeout)
    except ApiError as e:
        error(f"CodeRepository job runs fetch failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(runs):
        return

    rows: list[list[str]] = []
    for run in runs:
        rows.append(
            [
                str(run.get("uid") or "-"),
                str(run.get("name") or "-"),
                str(run.get("status") or run.get("response_status") or "-"),
                str(run.get("execution_start") or "-"),
                str(run.get("execution_end") or "-"),
                str(run.get("unique_identifier") or "-"),
                str(run.get("commit_hash") or "-"),
            ]
        )

    if rows:
        print_table(
            "CodeRepository Job Runs",
            [
                "UID",
                "Name",
                "Status",
                "Execution Start",
                "Execution End",
                "Unique Identifier",
                "Commit Hash",
            ],
            rows,
        )

        snapshot_rows = [
            [
                str(run.get("uid") or "-"),
                str(run.get("runtime_image_uid") or "-"),
                str(run.get("runtime_image_digest") or "-"),
                str(run.get("commit_hash") or "-"),
            ]
            for run in runs
        ]
        print_table(
            "Immutable Job Run Image Snapshots",
            ["Job Run UID", "Runtime Image UID", "Runtime Image Digest", "Commit Hash"],
            snapshot_rows,
        )
    else:
        info("No job runs.")
    info(f"Total job runs: {len(runs)}")


def _format_job_run_log_row(row) -> str:
    if isinstance(row, str):
        return row
    if not isinstance(row, dict):
        return str(row)

    timestamp = str(row.get("timestamp") or row.get("time") or "").strip()
    level = str(row.get("severity") or row.get("level") or "").strip().upper()
    event = str(row.get("event") or "").strip()
    message = str(row.get("message") or row.get("text") or "").strip()

    parts = [part for part in (timestamp, level, event) if part]
    if parts and message:
        return f"{' | '.join(parts)} | {message}"
    if parts or message:
        return " | ".join([*parts, message] if message else parts)
    return json.dumps(row, default=str, sort_keys=True)


def _print_job_run_logs_rows(rows, *, start_index: int = 0) -> int:
    if not isinstance(rows, list):
        return start_index

    for row in rows[start_index:]:
        typer.echo(_format_job_run_log_row(row))
    return len(rows)


@code_repository_jobs_group.command("list")
def code_repository_jobs_list_cmd(
    code_repository_id: str | None = typer.Argument(
        None, help="Optional CodeRepository UID assertion; Git repository identity is authoritative."
    ),
    path: str | None = typer.Option(
        None, "--path", help="CodeRepository repository path (default: current code repository)"
    ),
    filter_entries: list[str] | None = typer.Option(None, "--filter", help=LIST_FILTER_OPTION_HELP),
    show_filters: bool = typer.Option(
        False, "--show-filters", help="Show the filters supported by this list command and exit."
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    List jobs for a CodeRepository.

    Uses SDK client `Job.filter()` as the single source of truth.

    Examples
    --------
    ```bash
    mainsequence code-repository jobs list
    mainsequence code-repository jobs list code-repository-uid-123
    mainsequence code-repository jobs list code-repository-uid-123 --path .
    ```
    """
    _code_repository_jobs_list_impl(
        code_repository_id=code_repository_id,
        path=path,
        filter_entries=filter_entries,
        show_filters=show_filters,
        timeout=timeout,
    )


@code_repository_jobs_group.command("run")
def code_repository_jobs_run_cmd(
    job_uid: str = pydantic_argument(JOB_MODEL_REF, "uid", ..., help="Job UID to run."),
    passthrough_args: list[str] | None = typer.Argument(
        None,
        help="Additional per-run args after `--`, for example `mainsequence code-repository jobs run <JOB_UID> -- --name demo`.",
    ),
    command_args: list[str] | None = typer.Option(
        None,
        "--arg",
        help="Append one per-run arg to the saved job entrypoint. Repeatable. Does not replace the saved execution_path or app_name.",
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Run a CodeRepository job immediately.

    Uses SDK client `Job.run_job()` as the single source of truth.
    Per-run args are appended to the saved job entrypoint; they do not replace it.

    Examples
    --------
    ```bash
    mainsequence code-repository jobs run <JOB_UID>
    mainsequence code-repository jobs run <JOB_UID> --arg demo-from-cli
    mainsequence code-repository jobs run <JOB_UID> -- --name demo-from-cli
    mainsequence code-repository jobs run <JOB_UID> --timeout 60
    ```
    """
    _require_login()

    merged_command_args = list(command_args or [])
    if passthrough_args:
        merged_command_args.extend(str(arg) for arg in passthrough_args)

    try:
        job_payload = get_code_repository_job(job_uid, timeout=timeout)
    except ApiError as e:
        error(f"CodeRepository job fetch failed: {e}")
        raise typer.Exit(1) from e

    entrypoint = str(job_payload.get("execution_path") or "").strip()
    if not entrypoint:
        app_name = str(job_payload.get("app_name") or "").strip()
        if app_name:
            entrypoint = f"app:{app_name}"

    if entrypoint:
        effective_tokens = [entrypoint, *merged_command_args]
        info(f"Effective run: {shlex.join(effective_tokens)}")

    try:
        payload = run_code_repository_job(
            job_uid=job_uid,
            command_args=merged_command_args or None,
            timeout=timeout,
        )
    except ApiError as e:
        error(f"CodeRepository job run failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(payload):
        return

    success(f"CodeRepository job run requested: job_uid={job_uid}")

    if payload:
        preferred_keys = [
            ("Job UID", str(payload.get("job_uid") or payload.get("job") or job_uid)),
            ("Job Run UID", str(payload.get("uid") or payload.get("job_run_uid") or "-")),
            ("Name", str(payload.get("name") or payload.get("job_name") or "-")),
            ("Unique Identifier", str(payload.get("unique_identifier") or "-")),
            ("Status", str(payload.get("status") or "-")),
        ]
        rows = [(label, value) for label, value in preferred_keys if value != "-"]
        remaining = []
        for key, value in payload.items():
            if key in {
                "job",
                "job_uid",
                "id",
                "uid",
                "job_run_uid",
                "name",
                "job_name",
                "unique_identifier",
                "status",
            }:
                continue
            remaining.append(
                (str(key), json.dumps(value) if isinstance(value, dict | list) else str(value))
            )
        print_kv("Job Run", rows + remaining)


@code_repository_job_runs_group.command("list")
def code_repository_job_runs_list_cmd(
    job_uid: str = pydantic_argument(
        JOB_MODEL_REF, "uid", ..., help="Job UID whose runs will be listed."
    ),
    filter_entries: list[str] | None = typer.Option(None, "--filter", help=LIST_FILTER_OPTION_HELP),
    show_filters: bool = typer.Option(
        False, "--show-filters", help="Show the filters supported by this list command and exit."
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    List runs for a specific job.

    Uses SDK client `JobRun.filter(job__uid=job_uid)` as the single source of truth.

    Examples
    --------
    ```bash
    mainsequence code-repository jobs runs list <JOB_UID>
    mainsequence code-repository jobs runs list <JOB_UID> --timeout 60
    ```
    """
    _code_repository_job_runs_list_impl(
        job_uid=job_uid,
        filter_entries=filter_entries,
        show_filters=show_filters,
        timeout=timeout,
    )


@code_repository_job_runs_group.command("logs")
def code_repository_job_runs_logs_cmd(
    job_run_uid: str = pydantic_argument(
        JOB_RUN_MODEL_REF, "uid", ..., help="Job run UID whose logs will be shown."
    ),
    start: float | None = typer.Option(
        None, "--start", help="Window start as epoch seconds or milliseconds."
    ),
    end: float | None = typer.Option(
        None, "--end", help="Window end as epoch seconds or milliseconds."
    ),
    cursor: str | None = typer.Option(None, "--cursor", help="Opaque continuation cursor."),
    limit: int | None = typer.Option(None, "--limit", min=1, max=500),
    severity: str | None = typer.Option(None, "--severity"),
    request_id: str | None = typer.Option(None, "--request-id"),
    event: str | None = typer.Option(None, "--event"),
    outcome: str | None = typer.Option(None, "--outcome"),
    poll_interval: int = typer.Option(
        30,
        "--poll-interval",
        help="Polling interval in seconds while the job run status is PENDING or RUNNING. Set to 0 to disable polling.",
    ),
    max_wait_seconds: int = typer.Option(
        600,
        "--max-wait-seconds",
        help="Maximum total time in seconds to keep polling while the job run status is PENDING or RUNNING. Set to 0 to disable the overall polling timeout.",
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Show logs for a specific job run.

    Uses SDK client `JobRun.get_logs()` as the single source of truth.
    When the backend reports `PENDING` or `RUNNING`, the CLI polls every 30 seconds by default
    for up to 10 minutes unless `--max-wait-seconds 0` is used.

    Examples
    --------
    ```bash
    mainsequence code-repository jobs runs logs 4c1d77c8-8a42-42b8-a9c1-06be9a336e5d
    mainsequence code-repository jobs runs logs 4c1d77c8-8a42-42b8-a9c1-06be9a336e5d --poll-interval 10
    mainsequence code-repository jobs runs logs 4c1d77c8-8a42-42b8-a9c1-06be9a336e5d --max-wait-seconds 900
    mainsequence code-repository jobs runs logs 4c1d77c8-8a42-42b8-a9c1-06be9a336e5d --poll-interval 0
    ```
    """
    _require_login()

    if max_wait_seconds < 0:
        error("--max-wait-seconds must be >= 0.")
        raise typer.Exit(2)

    seen_rows: set[str] = set()
    request_cursor = cursor
    header_printed = False
    poll_started_at = time.monotonic()

    while True:
        try:
            payload = get_code_repository_job_run_logs(
                job_run_uid=job_run_uid,
                start=start,
                end=end,
                cursor=request_cursor,
                limit=limit,
                severity=severity,
                request_id=request_id,
                event=event,
                outcome=outcome,
                timeout=timeout,
            )
        except ApiError as e:
            error(f"CodeRepository job run logs fetch failed: {e}")
            raise typer.Exit(1) from e

        if _emit_json(payload):
            return

        status_value = str(payload.get("status") or "-")
        rows = payload.get("rows") or []
        if not isinstance(rows, list):
            rows = [rows]

        if not header_printed:
            print_kv(
                "Job Run Logs",
                [
                    (
                        "Job Run UID",
                        str(payload.get("job_run_uid") or payload.get("uid") or job_run_uid),
                    ),
                    ("Status", status_value),
                ],
            )
            header_printed = True
        else:
            info(f"Job run status: {status_value}")

        printed_rows = 0
        for row in rows:
            row_key = json.dumps(row, default=str, sort_keys=True)
            if row_key in seen_rows:
                continue
            seen_rows.add(row_key)
            typer.echo(_format_job_run_log_row(row))
            printed_rows += 1
        if printed_rows == 0 and not seen_rows:
            info("No logs yet.")

        next_cursor = str(payload.get("next_cursor") or "").strip()
        if next_cursor:
            request_cursor = next_cursor
            continue
        request_cursor = None

        if status_value not in {JOB_RUN_STATUS_PENDING, JOB_RUN_STATUS_RUNNING}:
            break
        if poll_interval <= 0:
            break
        if max_wait_seconds > 0:
            elapsed = time.monotonic() - poll_started_at
            remaining = max_wait_seconds - elapsed
            if remaining <= 0:
                warn(
                    f"Stopping log polling after {max_wait_seconds}s while job run is still {status_value}."
                )
                break
            sleep_for = min(float(poll_interval), remaining)
        else:
            sleep_for = float(poll_interval)

        info(f"Job run is still {status_value}. Polling again in {sleep_for:g}s...")
        time.sleep(sleep_for)


@code_repository_job_runs_group.command("resource-usage")
def code_repository_job_runs_resource_usage_cmd(
    job_run_uid: str = pydantic_argument(
        JOB_RUN_MODEL_REF,
        "uid",
        ...,
        help="Job run UID whose aggregate resource usage will be shown.",
    ),
    start: float | None = typer.Option(
        None, "--start", help="Window start as epoch seconds or milliseconds."
    ),
    end: float | None = typer.Option(
        None, "--end", help="Window end as epoch seconds or milliseconds."
    ),
    timeout: int | None = typer.Option(None, "--timeout"),
):
    """Read aggregate CPU, memory, and disk usage owned by a JobRun."""
    _require_login()
    try:
        payload = get_code_repository_job_run_resource_usage(
            job_run_uid,
            start=start,
            end=end,
            timeout=timeout,
        )
    except ApiError as e:
        error(f"CodeRepository job run resource usage fetch failed: {e}")
        raise typer.Exit(1) from e
    _render_owner_resource_usage(payload, title="Job Run Resource Usage")


def _code_repository_jobs_create_impl(
    code_repository_id: str | None,
    name: str | None,
    path: str | None,
    execution_path: str | None,
    app_name: str | None,
    related_image_uid: str | None,
    schedule_type: str | None,
    schedule_every: int | None,
    schedule_period: str | None,
    schedule_expression: str | None,
    schedule_start_time: str | None,
    schedule_one_off: bool | None,
    cpu_request: str | None,
    memory_request: str | None,
    gpu_request: str | None,
    gpu_type: str | None,
    spot: bool | None,
    max_runtime_seconds: int | None,
    automatic_deployment: bool,
    automatic_redeployment_tag_regex: str | None,
    timeout: int | None,
) -> None:
    _require_login()

    code_repository_dir = _resolve_code_repository_dir(code_repository_id, path)

    try:
        code_repository_branch_uid = _resolve_code_repository_branch_uid_for_command(
            code_repository_id,
            code_repository_dir=code_repository_dir,
        )
    except ApiError as e:
        error(str(e))
        raise typer.Exit(1) from e

    name = (name or "").strip() or typer.prompt(pydantic_prompt_text(JOB_MODEL_REF, "name")).strip()
    if not name:
        error("Job name is required.")
        raise typer.Exit(1)

    if automatic_deployment:
        if related_image_uid is not None:
            error(
                "Do not provide --related-image-uid with --automatic-deployment. "
                "The backend derives and prepares the initial exact image."
            )
            raise typer.Exit(1)
        info(
            "Automatic deployment: the backend will derive the initial exact image "
            "from the CodeRepositoryBranch synchronized commit."
        )
    else:
        try:
            code_repository_images = list_code_repository_images(
                related_code_repository_branch_uid=code_repository_branch_uid,
                timeout=timeout,
            )
        except ApiError as e:
            error(f"CodeRepository images fetch failed: {e}")
            raise typer.Exit(1) from e

        if related_image_uid is None and code_repository_images:
            image_rows = [
                [
                    str(img.get("uid") or "-"),
                    str(img.get("code_repository_commit_hash") or "-"),
                    _format_base_image_label(img.get("base_image")),
                ]
                for img in code_repository_images
            ]
            related_image_uid = _prompt_select_uid(
                title="Available CodeRepository Images",
                prompt_label="Related image UID",
                items=code_repository_images,
                rows=image_rows,
            )

        if related_image_uid is None:
            error("related_image_uid is required when automatic deployment is disabled.")
            raise typer.Exit(1)

    if execution_path is None and app_name is None:
        execution_path = (
            typer.prompt(
                pydantic_prompt_text(JOB_MODEL_REF, "execution_path", optional=True),
                default="",
            ).strip()
            or None
        )
        if execution_path is None:
            app_name = (
                typer.prompt(
                    pydantic_prompt_text(JOB_MODEL_REF, "app_name", optional=True),
                    default="",
                ).strip()
                or None
            )

    if execution_path is None and app_name is None:
        error("One of execution_path or app_name is required.")
        raise typer.Exit(1)

    try:
        task_schedule = _build_job_task_schedule_payload(
            schedule_type=schedule_type,
            schedule_every=schedule_every,
            schedule_period=schedule_period,
            schedule_expression=schedule_expression,
            schedule_start_time=schedule_start_time,
            schedule_one_off=schedule_one_off,
            prompt_for_missing=True,
        )
    except ValueError as e:
        error(str(e))
        raise typer.Exit(1) from e

    try:
        cpu_request, memory_request, spot, max_runtime_seconds, used_defaults = (
            _resolve_job_create_defaults(
                cpu_request=cpu_request,
                memory_request=memory_request,
                spot=spot,
                max_runtime_seconds=max_runtime_seconds,
            )
        )
    except ValueError as e:
        error(str(e))
        raise typer.Exit(1) from e

    if used_defaults:
        default_parts: list[str] = []
        if "cpu_request" in used_defaults:
            default_parts.append(f"cpu_request={cpu_request}")
        if "memory_request" in used_defaults:
            default_parts.append(f"memory_request={memory_request}")
        if "spot" in used_defaults:
            default_parts.append(f"spot={'true' if spot else 'false'}")
        if "max_runtime_seconds" in used_defaults:
            default_parts.append(f"max_runtime_seconds={max_runtime_seconds}")
        info("Using defaults: " + ", ".join(default_parts) + ".")

    try:
        created = create_code_repository_job(
            name=name,
            code_repository_branch_uid=code_repository_branch_uid,
            execution_path=execution_path,
            app_name=app_name,
            task_schedule=task_schedule,
            cpu_request=cpu_request,
            memory_request=memory_request,
            gpu_request=gpu_request,
            gpu_type=gpu_type,
            spot=spot,
            max_runtime_seconds=max_runtime_seconds,
            related_image_uid=related_image_uid,
            automatic_deployment=automatic_deployment,
            automatic_redeployment_tag_regex=automatic_redeployment_tag_regex,
            timeout=timeout,
        )
    except ApiError as e:
        error(f"CodeRepository job creation failed: {e}")
        raise typer.Exit(1) from e
    except RuntimeError as e:
        error(str(e))
        raise typer.Exit(1) from e

    if _emit_json(created):
        return

    success(f"CodeRepository job created: uid={created.get('uid') or '-'}")
    print_kv(
        "CodeRepository Job",
        [
            ("UID", str(created.get("uid") or "-")),
            ("Name", str(created.get("name") or name)),
            ("CodeRepository UID", str(code_repository_id)),
            ("Execution Path", str(created.get("execution_path") or execution_path or "-")),
            ("App Name", str(created.get("app_name") or app_name or "-")),
            (
                "Related Image",
                _format_related_image_label(created.get("related_image") or related_image_uid),
            ),
            (
                "Schedule",
                _format_job_schedule_summary(created.get("task_schedule") or task_schedule),
            ),
            ("CPU Request", str(created.get("cpu_request") or cpu_request)),
            ("Memory Request", str(created.get("memory_request") or memory_request)),
            ("Spot", str(created.get("spot") if created.get("spot") is not None else spot).lower()),
            (
                "Max Runtime Seconds",
                str(created.get("max_runtime_seconds") or max_runtime_seconds),
            ),
            ("Image Status", str(created.get("image_status") or "-")),
            ("Exact Commit", str(created.get("code_repository_commit_hash") or "-")),
            (
                "Automatic Deployment",
                str(created.get("automatic_deployment", automatic_deployment)).lower(),
            ),
            (
                "Promotion Tag Regex",
                str(
                    (created.get("automatic_redeployment_policy") or {}).get("tag_regex")
                    or "every qualifying exact event"
                ),
            ),
        ],
    )


@code_repository_jobs_group.command("create")
def code_repository_jobs_create_cmd(
    code_repository_id: str | None = typer.Argument(
        None, help="Optional CodeRepository UID assertion; Git repository identity is authoritative."
    ),
    name: str | None = pydantic_option(JOB_MODEL_REF, "name", None, "--name"),
    path: str | None = typer.Option(
        None, "--path", help="CodeRepository repository path (default: current code repository)"
    ),
    execution_path: str | None = pydantic_option(
        JOB_MODEL_REF,
        "execution_path",
        None,
        "--execution-path",
    ),
    app_name: str | None = pydantic_option(JOB_MODEL_REF, "app_name", None, "--app-name"),
    related_image_uid: str | None = pydantic_option(
        JOB_MODEL_REF,
        "related_image_uid",
        None,
        "--related-image-uid",
        extra_help=(
            "Required for manually pinned Jobs. Omit it with "
            "--automatic-deployment so the backend owns initial image preparation."
        ),
    ),
    schedule_type: str | None = pydantic_option(
        INTERVAL_SCHEDULE_MODEL_REF,
        "type",
        None,
        "--schedule-type",
        extra_help="Use interval or crontab. If omitted, the CLI asks whether to build a schedule.",
    ),
    schedule_every: int | None = pydantic_option(
        INTERVAL_SCHEDULE_MODEL_REF,
        "every",
        None,
        "--schedule-every",
        extra_help="Used with --schedule-type interval.",
    ),
    schedule_period: str | None = pydantic_option(
        INTERVAL_SCHEDULE_MODEL_REF,
        "period",
        None,
        "--schedule-period",
        extra_help="Used with --schedule-type interval.",
    ),
    schedule_expression: str | None = pydantic_option(
        CRONTAB_SCHEDULE_MODEL_REF,
        "expression",
        None,
        "--schedule-expression",
        extra_help="Used with --schedule-type crontab.",
    ),
    schedule_start_time: str | None = pydantic_option(
        CRONTAB_SCHEDULE_MODEL_REF,
        "start_time",
        None,
        "--schedule-start-time",
    ),
    schedule_one_off: bool | None = typer.Option(
        None,
        "--schedule-one-off/--schedule-recurring",
        help="Mark the created schedule as one-off or recurring.",
    ),
    cpu_request: str | None = pydantic_option(
        JOB_MODEL_REF,
        "cpu_request",
        None,
        "--cpu-request",
        extra_help="Defaults to 0.25 when omitted, or is derived from memory_request if only memory is provided.",
    ),
    memory_request: str | None = pydantic_option(
        JOB_MODEL_REF,
        "memory_request",
        None,
        "--memory-request",
        extra_help="Defaults to 0.5 when omitted, or is derived from cpu_request if only CPU is provided.",
    ),
    gpu_request: str | None = pydantic_option(JOB_MODEL_REF, "gpu_request", None, "--gpu-request"),
    gpu_type: str | None = pydantic_option(JOB_MODEL_REF, "gpu_type", None, "--gpu-type"),
    spot: bool | None = typer.Option(
        None,
        "--spot/--no-spot",
        help=get_cli_field_metadata(JOB_MODEL_REF, "spot").build_help(
            extra_help="Defaults to --no-spot.",
            include_examples=False,
        ),
    ),
    max_runtime_seconds: int | None = pydantic_option(
        JOB_MODEL_REF,
        "max_runtime_seconds",
        None,
        "--max-runtime-seconds",
        extra_help="Defaults to 86400 when omitted.",
    ),
    automatic_deployment: bool = typer.Option(
        False,
        "--automatic-deployment/--no-automatic-deployment",
        help=(
            "Let the backend derive and prepare the initial exact image from the "
            "synchronized CodeRepositoryBranch commit, then promote future qualifying exact events."
        ),
    ),
    automatic_redeployment_tag_regex: str | None = typer.Option(
        None,
        "--automatic-redeployment-tag-regex",
        help=(
            "Full-match regex for qualifying immutable Git tags. Omit it to "
            "allow every qualifying exact event while automatic deployment is enabled."
        ),
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Create a job for a CodeRepository.

    Uses SDK client `Job.create()` as the single source of truth. Manual Jobs
    require `--related-image-uid`; automatically deployed Jobs forbid it and
    delegate initial exact-image preparation to the backend.
    When compute settings are omitted, the CLI applies safe defaults:
    `cpu_request=0.25`, `memory_request=0.5`, `spot=false`, `max_runtime_seconds=86400`.
    If schedule arguments are omitted, the CLI asks whether to build an interval or crontab schedule.

    Examples
    --------
    ```bash
    mainsequence code-repository jobs create
    mainsequence code-repository jobs create code-repository-uid-123 --name daily-run --execution-path scripts/test.py --related-image-uid <uid>
    mainsequence code-repository jobs create code-repository-uid-123 --name dashboard --app-name dashboard-api --related-image-uid <uid>
    ```
    """
    _code_repository_jobs_create_impl(
        code_repository_id=code_repository_id,
        name=name,
        path=path,
        execution_path=execution_path,
        app_name=app_name,
        related_image_uid=related_image_uid,
        schedule_type=schedule_type,
        schedule_every=schedule_every,
        schedule_period=schedule_period,
        schedule_expression=schedule_expression,
        schedule_start_time=schedule_start_time,
        schedule_one_off=schedule_one_off,
        cpu_request=cpu_request,
        memory_request=memory_request,
        gpu_request=gpu_request,
        gpu_type=gpu_type,
        spot=spot,
        max_runtime_seconds=max_runtime_seconds,
        automatic_deployment=automatic_deployment,
        automatic_redeployment_tag_regex=automatic_redeployment_tag_regex,
        timeout=timeout,
    )


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

    name = safe_slug(
        p.get("code_repository_name") or f"code-repository-{code_repository_uid}"
    )
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


@code_repository.command("open")
def code_repository_open(
    code_repository_id: str | None = typer.Argument(
        None, help="Optional CodeRepository UID assertion against the current Git worktree"
    ),
    path: str | None = typer.Option(
        None, "--path", help="Open an explicit path instead of resolving by id"
    ),
):
    """
    Open a mapped CodeRepository folder in the OS file manager.

    Parameters
    ----------
    code_repository_id:
        Optional CodeRepository UID assertion against the current Git worktree.
    path:
        Explicit local path to open.

    Examples
    --------
    ```bash
    mainsequence code-repository open code-repository-uid-123
    mainsequence code-repository open --path .
    ```
    """
    p = _resolve_code_repository_dir(code_repository_id, path)
    open_folder(str(p))
    success(f"Opened: {p}")


@code_repository.command("delete-local")
def code_repository_delete_local(
    code_repository_id: str | None = typer.Argument(
        None, help="Optional CodeRepository UID assertion against the current Git worktree"
    ),
    path: str | None = typer.Option(
        None, "--path", help="Delete an explicit path instead of resolving by id"
    ),
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not prompt for confirmation"),
):
    """
    Delete a local CodeRepository folder.

    Safety checks prevent deletion outside the configured CodeRepositories root.

    Parameters
    ----------
    code_repository_id:
        Optional CodeRepository UID assertion against the current Git worktree.
    path:
        Explicit local path to delete.
    yes:
        Skip confirmation prompt.

    Examples
    --------
    ```bash
    mainsequence code-repository delete-local code-repository-uid-123
    mainsequence code-repository delete-local --path ./my-repository --yes
    ```
    """
    p = _resolve_code_repository_dir(code_repository_id, path)

    # Determine the CodeRepositories root for the safety check.
    cfg_obj = cfg.get_config()
    base = cfg_obj["mainsequence_path"]
    org_slug = "default"
    try:
        prof = get_current_user_profile()
        if prof and prof.get("organization"):
            org_slug = _org_slug_from_profile()
    except Exception:
        pass

    code_repositories_root = _code_repositories_root(base, org_slug).resolve()
    try:
        p.resolve().relative_to(code_repositories_root)
    except Exception as e:
        error(f"Refusing to delete outside the CodeRepositories root: {p}")
        raise typer.Exit(1) from e

    warning_text = (
        "This will permanently delete the local CodeRepository folder.\n"
        "Your CodeRepository will remain on the platform.\n"
    )
    if not yes:
        if not typer.confirm(f"{warning_text}\nDelete: {p} ?", default=False):
            info("Cancelled.")
            raise typer.Exit(0)

    import shutil

    shutil.rmtree(str(p), ignore_errors=True)
    warn(f"Deleted: {p}")


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
        context = get_code_repository_context(code_repository_uid=code_repository_id, code_repository_dir=code_repository_dir)
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
    Export pinned dependencies into `requirements.txt` using `uv`.

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

    uv = ensure_uv_installed(code_repository_dir) if ensure_uv else (ensure_venv(code_repository_dir).uv or None)
    if not uv:
        error("uv not found in .venv and --no-ensure-uv was used.")
        raise typer.Exit(1)

    with status("Exporting requirements.txt via uv..."):
        uv_export_requirements(
            uv, cwd=code_repository_dir, locked=False, no_dev=False, output_file="requirements.txt"
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


@code_repository.command("build-docker-env")
def code_repository_build_docker_env(
    code_repository_id: str | None = typer.Argument(
        None, help="Optional CodeRepository UID assertion against the current Git worktree"
    ),
    path: str | None = typer.Option(None, "--path", help="CodeRepository directory"),
    image_ref: str | None = typer.Option(
        None, "--image-ref", help="Docker image ref to build (default: computed)"
    ),
    devcontainer: bool = typer.Option(
        True,
        "--devcontainer/--no-devcontainer",
        help="Write .devcontainer/devcontainer.json",
    ),
):
    """
    Build a Docker image for the CodeRepository and optionally write devcontainer config.

    Parameters
    ----------
    code_repository_id:
        Optional CodeRepository UID assertion against the current Git worktree.
    path:
        Explicit local path.
    image_ref:
        Explicit docker image tag/reference.
    devcontainer:
        Write `.devcontainer/devcontainer.json` after build.

    Examples
    --------
    ```bash
    mainsequence code-repository build-docker-env code-repository-uid-123
    mainsequence code-repository build-docker-env --path . --image-ref ghcr.io/acme/proj:dev
    mainsequence code-repository build-docker-env --path . --no-devcontainer
    ```
    """
    code_repository_dir = _resolve_code_repository_dir(code_repository_id, path)
    dockerfile = code_repository_dir / "Dockerfile"
    if not dockerfile.exists():
        error("Dockerfile not found in the CodeRepository root.")
        raise typer.Exit(1)

    ref = (image_ref or "").strip() or compute_docker_image_ref(code_repository_dir)

    if devcontainer:
        dc_path = write_devcontainer_config(code_repository_dir, ref)
        info(f"Devcontainer updated: {dc_path}")

    with status(f"Building Docker image: {ref}"):
        rc = build_docker_environment(code_repository_dir, ref)

    if rc != 0:
        error(f"Docker build failed (exit {rc}).")
        raise typer.Exit(rc)

    success(f"Docker image built: {ref}")
    info("Next step (VS Code): run 'Dev Containers: Reopen in Container'.")


@code_repository.command("current")
def code_repository_current(
    debug: bool = typer.Option(False, "--debug", help="Show detection debug details"),
):
    """
    Detect and display current code repository context from current directory.

    Includes detected path, logical CodeRepository UID, current Git branch, resolved
    CodeRepositoryBranch UID, virtual environment, Python version, and SDK status when
    available.

    Parameters
    ----------
    debug:
        Include detailed detection diagnostics.

    Examples
    --------
    ```bash
    mainsequence code-repository current
    mainsequence code-repository current --debug
    ```
    """
    cfg_obj = cfg.get_config()
    base = cfg_obj["mainsequence_path"]
    cwd = str(pathlib.Path.cwd())

    code_repository_info, dbg = detect_current_code_repository([cwd], base)
    if not code_repository_info:
        warn(f"No MainSequence code repository detected (reason: {dbg.reason}).")
        if debug and dbg.checks:
            print_kv("Debug", [("checks", json.dumps([c.__dict__ for c in dbg.checks], indent=2))])
        raise typer.Exit(1)

    code_repository_path = pathlib.Path(code_repository_info.path)
    code_repository_uid = ""
    git_branch = None
    code_repository_branch_uid = None
    repository_identity = None
    commit_sha = None
    code_repository_branch_status = "unresolved"
    code_repository_branch_error = None
    try:
        context = get_code_repository_context(code_repository_dir=code_repository_path)
    except CodeRepositoryContextError as exc:
        code_repository_branch_error = str(exc)
    else:
        code_repository_uid = str(context.code_repository_uid or "")
        git_branch = context.repository_branch
        code_repository_branch_uid = context.code_repository_branch_uid
        repository_identity = context.canonical_repository_identity
        commit_sha = context.commit_sha
        code_repository_branch_status = context.status
        code_repository_branch_error = context.detail or None

    current_code_repository_payload = {
        "path": code_repository_info.path,
        "folder": code_repository_info.folder,
        "code_repository_uid": code_repository_uid or None,
        "git_branch": git_branch,
        "github_repository_binding": repository_identity,
        "git_commit": commit_sha,
        "code_repository_branch_uid": code_repository_branch_uid,
        "code_repository_branch_status": code_repository_branch_status,
        "code_repository_branch_error": code_repository_branch_error,
        "venv_path": code_repository_info.venv_path,
        "python_version": code_repository_info.python_version,
    }

    # SDK status (best-effort)
    req = pathlib.Path(code_repository_info.path) / "requirements.txt"
    local = read_local_sdk_version(req)
    latest = None
    try:
        latest = fetch_latest_sdk_version()
    except Exception:
        pass

    sdk_status_payload = None
    if latest or local is not None:
        status_label = "checking"
        if latest and local and local != "unversioned":
            status_label = (
                "match" if normalize_version(local) == normalize_version(latest) else "differs"
            )
        sdk_status_payload = {
            "latest_github": latest or "unavailable",
            "local_requirements_txt": local if local is not None else "not found",
            "status": status_label,
            "hint": "Run: mainsequence code-repository update-sdk --path .  (if differs)",
        }

    debug_payload = None
    if debug and dbg.checks:
        debug_payload = [c.__dict__ for c in dbg.checks]

    if _emit_json(
        {
            "code_repository": current_code_repository_payload,
            "sdk_status": sdk_status_payload,
            "debug": debug_payload,
        }
    ):
        return

    items = [
        ("Path", code_repository_info.path),
        ("Folder", code_repository_info.folder),
        ("CodeRepository UID", code_repository_uid or "-"),
        ("Git Branch", git_branch or "detached/unavailable"),
        ("Git Repository", repository_identity or "-"),
        ("Git Commit", commit_sha or "-"),
        ("CodeRepositoryBranch UID", code_repository_branch_uid or "-"),
        ("Branch Status", code_repository_branch_status),
        ("Venv", code_repository_info.venv_path or "not found"),
        ("Python", code_repository_info.python_version or "unknown"),
    ]
    if code_repository_branch_error:
        items.append(("Branch Detail", code_repository_branch_error))
    print_kv("Current CodeRepository", items)

    if latest or local is not None:
        print_kv(
            "SDK Status",
            [
                ("Latest (GitHub)", sdk_status_payload["latest_github"]),
                ("Local (requirements.txt)", sdk_status_payload["local_requirements_txt"]),
                ("Status", sdk_status_payload["status"]),
                ("Hint", sdk_status_payload["hint"]),
            ],
        )

    if debug and dbg.checks:
        print_kv(
            "Detection Debug", [("details", json.dumps([c.__dict__ for c in dbg.checks], indent=2))]
        )


@code_repository.command("sdk-status")
def code_repository_sdk_status(
    code_repository_id: str | None = typer.Argument(
        None, help="Optional CodeRepository UID assertion against the current Git worktree"
    ),
    path: str | None = typer.Option(None, "--path", help="CodeRepository directory"),
):
    """
    Show the local CodeRepository SDK version versus the latest GitHub release.

    Parameters
    ----------
    code_repository_id:
        Optional CodeRepository UID assertion against the current Git worktree.
    path:
        Explicit local path.

    Examples
    --------
    ```bash
    mainsequence code-repository sdk-status code-repository-uid-123
    mainsequence code-repository sdk-status --path .
    ```
    """
    code_repository_dir = _resolve_code_repository_dir(code_repository_id, path)
    req = code_repository_dir / "requirements.txt"
    local = read_local_sdk_version(req)
    with status("Checking GitHub for latest SDK..."):
        latest = fetch_latest_sdk_version()

    status_label = "unknown"
    if latest and local and local != "unversioned":
        status_label = (
            "match" if normalize_version(local) == normalize_version(latest) else "differs"
        )

    payload = {
        "code_repository": str(code_repository_dir),
        "latest_github": latest or "unavailable",
        "local_requirements_txt": local if local is not None else "not found",
        "status": status_label,
    }

    if _emit_json(payload):
        return

    print_kv(
        "SDK Status",
        [
            ("CodeRepository", payload["code_repository"]),
            ("Latest (GitHub)", payload["latest_github"]),
            ("Local (requirements.txt)", payload["local_requirements_txt"]),
            ("Status", payload["status"]),
        ],
    )


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
    protected_code_repository_roots = (source_checkout_root,) if source_checkout_root is not None else ()
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


@skills.command("list")
def skills_list_cmd():
    """
    List installed scaffold skills from the current CLI installation.

    Examples
    --------
    ```bash
    mainsequence skills list
    mainsequence skills list --json
    ```
    """
    rows = _installed_agent_scaffold_skills()
    payload = [
        {
            "name": row["name"],
            "skill_dir": row["skill_dir"],
            "skill_file": row["skill_file"],
        }
        for row in rows
    ]
    if _emit_json(payload):
        return

    print_table(
        "Installed Skills",
        ["Skill", "SKILL.md"],
        [[str(row["name"]), str(row["skill_file"])] for row in rows],
    )


@skills.command("path")
def skills_path_cmd(
    skill_name: str | None = typer.Argument(
        None,
        help="Optional installed SDK skill name, for example sdk_code_repository_execution or data_publishing/meta_tables",
    ),
):
    """
    Print the installed scaffold skills path or one installed `SKILL.md` path.

    When no skill name is provided, this prints the installed `agent_scaffold/skills`
    directory for the current CLI installation.

    When a skill name is provided, it may be the full relative skill path such as
    `data_publishing/meta_tables` or, when unique, by its leaf folder name.

    Examples
    --------
    ```bash
    mainsequence skills path
    mainsequence skills path sdk_code_repository_execution
    mainsequence skills path data_publishing/meta_tables
    mainsequence skills path builder
    ```
    """
    if skill_name is None:
        skills_dir = _installed_agent_scaffold_skills_dir()
        payload = {"skills_dir": skills_dir}
        if _emit_json(payload):
            return
        typer.echo(str(skills_dir))
        return

    row = _resolve_installed_agent_scaffold_skill(skill_name)
    payload = {
        "name": row["name"],
        "bundle_dir": row["bundle_dir"],
        "skills_dir": row["skills_dir"],
        "skill_dir": row["skill_dir"],
        "skill_file": row["skill_file"],
    }
    if _emit_json(payload):
        return

    typer.echo(str(row["skill_file"]))


organization.add_typer(organization_teams_group, name="teams")
