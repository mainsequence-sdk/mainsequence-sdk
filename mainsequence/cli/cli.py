"""
mainsequence.cli.cli
====================

MainSequence CLI entrypoint.

Parity with VS Code extension:
- settings set-backend
- logout (clear tokens)
- project freeze-env (compile environment)
- project build_local_venv (create local .venv from pyproject + uv sync)
- project sync (uv bump + lock/sync/export + git commit/push)
- project build-docker-env (docker build + devcontainer config)
- local `.env` provisioning during set-up-locally uses only CLI-managed runtime values
- project current (detect current project + venv/python info)
- sdk latest + project sdk-status + project update-sdk
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
import tempfile
import time
import uuid
from decimal import ROUND_UP, Decimal
from enum import Enum as PyEnum
from textwrap import dedent

import click
import typer
import yaml

from ..client.compute_validation import decimal_to_storage, parse_cpu_request, parse_memory_request
from ..scaffold_skills import ScaffoldSkillCopyBlocked, copy_scaffold_skills
from . import config as cfg
from .api import (
    ApiError,
    NotLoggedIn,
    add_agent_team_to_edit,
    add_agent_team_to_view,
    add_agent_user_to_edit,
    add_agent_user_to_view,
    add_constant_team_to_edit,
    add_constant_team_to_view,
    add_constant_user_to_edit,
    add_constant_user_to_view,
    add_data_node_storage_labels,
    add_data_node_storage_team_to_edit,
    add_data_node_storage_team_to_view,
    add_data_node_storage_user_to_edit,
    add_data_node_storage_user_to_view,
    add_deploy_key,
    add_meta_table_labels,
    add_meta_table_team_to_edit,
    add_meta_table_team_to_view,
    add_meta_table_user_to_edit,
    add_meta_table_user_to_view,
    add_project_labels,
    add_project_team_to_edit,
    add_project_team_to_view,
    add_project_user_to_edit,
    add_project_user_to_view,
    add_secret_team_to_edit,
    add_secret_team_to_view,
    add_secret_user_to_edit,
    add_secret_user_to_view,
    add_team_user_to_edit,
    add_team_user_to_view,
    add_workspace_labels,
    create_adapter_from_api_connection,
    create_agent,
    create_constant,
    create_organization_team,
    create_project,
    create_project_image,
    create_project_job,
    create_project_resource_release,
    create_secret,
    create_workspace,
    data_node_storage_column_search,
    data_node_storage_description_search,
    deep_find_repo_url,
    delete_agent,
    delete_constant,
    delete_data_node_storage,
    delete_meta_table,
    delete_organization_team,
    delete_project,
    delete_project_image,
    delete_resource_release,
    delete_secret,
    delete_workspace,
    get_agent,
    get_agent_run,
    get_agent_session,
    get_connection_instance,
    get_connection_type,
    get_constant,
    get_current_user_profile,
    get_data_node_storage,
    get_logged_user_details,
    get_meta_table,
    get_or_create_agent_session,
    get_organization_team,
    get_project,
    get_project_data_node_updates,
    get_project_image,
    get_project_job,
    get_project_job_run_logs,
    get_projects,
    get_registered_widget_type,
    get_resource_release,
    get_secret,
    get_workspace,
    list_agent_runs,
    list_agent_sessions,
    list_agent_users_can_edit,
    list_agent_users_can_view,
    list_agents,
    list_connection_instances,
    list_connection_types,
    list_constant_users_can_edit,
    list_constant_users_can_view,
    list_constants,
    list_data_node_storage_users_can_edit,
    list_data_node_storage_users_can_view,
    list_data_node_storages,
    list_dynamic_table_data_sources,
    list_github_organizations,
    list_meta_table_users_can_edit,
    list_meta_table_users_can_view,
    list_meta_tables,
    list_organization_teams,
    list_project_base_images,
    list_project_images,
    list_project_job_runs,
    list_project_jobs,
    list_project_resources,
    list_project_users_can_edit,
    list_project_users_can_view,
    list_registered_widget_types,
    list_secret_users_can_edit,
    list_secret_users_can_view,
    list_secrets,
    list_team_users_can_edit,
    list_team_users_can_view,
    list_workspaces,
    logout_cli_session,
    patch_adapter_from_api_connection,
    prime_sync_project_after_commit_sdk,
    refresh_data_node_storage_search_index,
    remove_agent_team_from_edit,
    remove_agent_team_from_view,
    remove_agent_user_from_edit,
    remove_agent_user_from_view,
    remove_constant_team_from_edit,
    remove_constant_team_from_view,
    remove_constant_user_from_edit,
    remove_constant_user_from_view,
    remove_data_node_storage_labels,
    remove_data_node_storage_team_from_edit,
    remove_data_node_storage_team_from_view,
    remove_data_node_storage_user_from_edit,
    remove_data_node_storage_user_from_view,
    remove_meta_table_labels,
    remove_meta_table_team_from_edit,
    remove_meta_table_team_from_view,
    remove_meta_table_user_from_edit,
    remove_meta_table_user_from_view,
    remove_project_labels,
    remove_project_team_from_edit,
    remove_project_team_from_view,
    remove_project_user_from_edit,
    remove_project_user_from_view,
    remove_secret_team_from_edit,
    remove_secret_team_from_view,
    remove_secret_user_from_edit,
    remove_secret_user_from_view,
    remove_team_user_from_edit,
    remove_team_user_from_view,
    remove_workspace_labels,
    repo_name_from_git_url,
    resolve_project,
    run_data_node_storage_query,
    run_meta_table_query,
    run_project_job,
    safe_slug,
    schedule_batch_project_jobs,
    search_projects,
    semantic_search_agents,
    send_agent_session_a2a_message,
    sync_project_after_commit,
    update_organization_team,
    update_workspace,
    validate_project_name,
)
from .browser_auth import BrowserAuthError, login_via_browser
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
)
from .migrations import migrations as migrations_group
from .model_filters import build_cli_model_filter_rows, parse_cli_model_filters
from .project_status import detect_current_project
from .pydantic_cli import (
    get_cli_field_metadata,
    pydantic_argument,
    pydantic_option,
    pydantic_prompt_text,
)
from .sdk_utils import fetch_latest_sdk_version, normalize_version, read_local_sdk_version
from .ssh_utils import (
    ensure_key_for_repo,
    open_folder,
    open_signed_terminal,
    start_agent_and_add_key,
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
    help="MainSequence CLI (login + project operations)",
    cls=MainSequenceGroup,
    invoke_without_command=True,
)

agent = typer.Typer(help="Agent commands")
agent_run_group = typer.Typer(help="Agent runtime commands")
agent_session_group = typer.Typer(help="Agent session commands")
agent_session_a2a_group = typer.Typer(help="Agent session A2A commands")
constants = typer.Typer(help="Constant commands")
secrets = typer.Typer(help="Secret commands")
cc = typer.Typer(help="Command Center commands")
workspace = typer.Typer(help="Workspace commands")
registered_widget_type = typer.Typer(help="Registered widget type commands")
connection_type = typer.Typer(help="Connection type commands")
connection = typer.Typer(help="Connection commands")
organization = typer.Typer(help="Organization commands")
organization_teams_group = typer.Typer(help="Organization team commands")
meta_table_group = typer.Typer(help="MetaTable table-storage commands")
data_node_storage_group = typer.Typer(help="DataNode update/read-helper commands")
project = typer.Typer(help="Project commands (remote + local operations)")
project_list_group = typer.Typer(help="List-related project commands")
project_project_resource_group = typer.Typer(help="Project resource commands")
project_data_node_updates_group = typer.Typer(help="Project data node update commands")
project_images_group = typer.Typer(help="Project image commands")
project_jobs_group = typer.Typer(help="Project job commands")
project_job_runs_group = typer.Typer(help="Project job run commands")
settings = typer.Typer(help="Settings (base folder, backend, etc.)")
sdk = typer.Typer(help="SDK utilities (latest version, status)")
skills = typer.Typer(help="Installed scaffold skill commands")

app.add_typer(agent, name="agent")
agent.add_typer(agent_run_group, name="run")
agent.add_typer(agent_session_group, name="session")
agent_session_group.add_typer(agent_session_a2a_group, name="a2a")
app.add_typer(agent_run_group, name="agent_runtime", hidden=True)
app.add_typer(agent_run_group, name="agent-runtime", hidden=True)
app.add_typer(constants, name="constants")
app.add_typer(secrets, name="secrets")
app.add_typer(cc, name="cc")
app.add_typer(cc, name="command_center", hidden=True)
cc.add_typer(workspace, name="workspace")
cc.add_typer(registered_widget_type, name="registered_widget_type")
cc.add_typer(registered_widget_type, name="registered-widget-type", hidden=True)
cc.add_typer(connection_type, name="connection_type")
cc.add_typer(connection_type, name="connection-type", hidden=True)
cc.add_typer(connection, name="connection")
cc.add_typer(connection, name="connections", hidden=True)
app.add_typer(workspace, name="workspace", hidden=True)
app.add_typer(registered_widget_type, name="registered_widget_type", hidden=True)
app.add_typer(registered_widget_type, name="registered-widget-type", hidden=True)
app.add_typer(connection_type, name="connection_type", hidden=True)
app.add_typer(connection_type, name="connection-type", hidden=True)
app.add_typer(connection, name="connection", hidden=True)
app.add_typer(connection, name="connections", hidden=True)
app.add_typer(organization, name="organization")
app.add_typer(skills, name="skills")
app.add_typer(meta_table_group, name="meta-table")
app.add_typer(meta_table_group, name="meta_table")
app.add_typer(data_node_storage_group, name="data-node")
app.add_typer(data_node_storage_group, name="data_node")
app.add_typer(data_node_storage_group, name="data-node-storage", hidden=True)
app.add_typer(data_node_storage_group, name="data_node_storage", hidden=True)
app.add_typer(project, name="project")
project.add_typer(project_list_group, name="list")
project.add_typer(project_project_resource_group, name="project_resource")
project.add_typer(project_data_node_updates_group, name="data-node-updates")
project.add_typer(project_images_group, name="images")
project.add_typer(project_jobs_group, name="jobs")
project_jobs_group.add_typer(project_job_runs_group, name="runs")
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
AGENT_RUN_MODEL_REF = "mainsequence.client.agent_runtime_models.AgentRun"
JOB_MODEL_REF = "mainsequence.client.models_helpers.Job"
INTERVAL_SCHEDULE_MODEL_REF = "mainsequence.client.models_helpers.IntervalSchedule"
CRONTAB_SCHEDULE_MODEL_REF = "mainsequence.client.models_helpers.CrontabSchedule"
JOB_RUN_MODEL_REF = "mainsequence.client.models_helpers.JobRun"
PROJECT_IMAGE_MODEL_REF = "mainsequence.client.models_foundry.ProjectImage"
PROJECT_RESOURCE_MODEL_REF = "mainsequence.client.models_helpers.ProjectResource"
DATA_NODE_STORAGE_MODEL_REF = "mainsequence.client.metatables.TimeIndexMetaTable"
META_TABLE_MODEL_REF = "mainsequence.client.metatables.MetaTable"
CONSTANT_MODEL_REF = "mainsequence.client.models_foundry.Constant"
SECRET_MODEL_REF = "mainsequence.client.models_foundry.Secret"
WORKSPACE_MODEL_REF = "mainsequence.client.command_center.Workspace"
REGISTERED_WIDGET_TYPE_MODEL_REF = "mainsequence.client.command_center.RegisteredWidgetType"
CONNECTION_TYPE_MODEL_REF = "mainsequence.client.command_center.connections.ConnectionType"
CONNECTION_INSTANCE_MODEL_REF = "mainsequence.client.command_center.connections.ConnectionInstance"
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


def _projects_root(base_dir: str, org_slug: str) -> pathlib.Path:
    p = pathlib.Path(base_dir).expanduser()
    return p / org_slug / "projects"


def _org_slug_from_profile() -> str:
    prof = get_current_user_profile()
    name = prof.get("organization") or "default"
    if isinstance(name, dict):
        name = name.get("name") or name.get("slug") or "default"
    if not isinstance(name, str):
        name = str(name or "default")
    return re.sub(r"[^a-z0-9-_]+", "-", name.lower()).strip("-") or "default"


def _determine_repo_url(p: dict) -> str:
    repo = (p.get("git_ssh_url") or "").strip()
    if repo.lower() == "none":
        repo = ""
    if not repo:
        extra = (p.get("data_source") or {}).get("related_resource", {}) or {}
        extra = (
            extra.get("extra_arguments")
            or (p.get("data_source") or {}).get("extra_arguments")
            or {}
        )
        repo = deep_find_repo_url(extra) or ""
    return repo


def _project_identity_value(project: dict, *, prefer_uid: bool = True) -> str:
    """
    Return the public project identifier for display purposes.

    Public CLI surfaces should prefer `uid`, while still tolerating legacy payloads
    that only expose numeric `id`.
    """
    primary = project.get("uid") if prefer_uid else project.get("id")
    fallback = project.get("id") if prefer_uid else project.get("uid")
    value = primary or fallback or ""
    return str(value).strip()


def _project_ref_matches_env(project_dir: pathlib.Path, project_ref: str) -> bool:
    env_path = project_dir / ".env"
    if not env_path.is_file():
        return False
    try:
        content = env_path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return False

    uid_match = re.search(r"(?m)^MAIN_SEQUENCE_PROJECT_UID=(.+?)\s*$", content)
    if uid_match and uid_match.group(1).strip() == project_ref:
        return True

    id_match = re.search(r"(?m)^MAIN_SEQUENCE_PROJECT_ID=(.+?)\s*$", content)
    if id_match and id_match.group(1).strip() == project_ref:
        return True

    return False


def _find_local_project_dir(
    base_dir: str, org_slug: str, project_ref: int | str, project_name: str | None = None
) -> str | None:
    """
    Find local folder for a project reference.

    Preferred matching is by `.env` markers, especially `MAIN_SEQUENCE_PROJECT_UID`.
    Legacy numeric `-<id>` folder suffixes remain supported for older local clones.
    """
    normalized_ref = str(project_ref).strip()
    legacy_suffix = f"-{normalized_ref}" if normalized_ref.isdigit() else None

    root = _projects_root(base_dir, org_slug)
    if root.exists():
        # Prefer CWD hints when the local .env explicitly points at this project.
        try:
            cwd = pathlib.Path.cwd().resolve()
            for parent in [cwd] + list(cwd.parents):
                try:
                    parent.relative_to(root)
                except Exception:
                    continue
                if parent.is_dir() and _project_ref_matches_env(parent, normalized_ref):
                    return str(parent)
        except Exception:
            pass

        # canonical if name provided
        if project_name:
            slug = safe_slug(project_name)
            cand = root / f"{slug}-{normalized_ref}"
            if cand.is_dir():
                return str(cand)

        # scan root for .env markers first
        try:
            for d in root.iterdir():
                if not d.is_dir():
                    continue
                if _project_ref_matches_env(d, normalized_ref):
                    return str(d)
                if legacy_suffix and d.name.endswith(legacy_suffix):
                    return str(d)
        except Exception:
            pass

    # legacy <slug> if name provided
    if project_name:
        legacy = root / safe_slug(project_name)
        if legacy.is_dir():
            return str(legacy)

    return None


def _render_projects_table(items: list[dict], base_dir: str, org_slug: str) -> str:
    """Return an aligned table with project identity, init state, and local mapping."""

    rows = []
    for p in items:
        public_id = _project_identity_value(p)
        local_lookup_id = _project_identity_value(p, prefer_uid=False) or public_id
        name = p.get("project_name") or "(unnamed)"
        initialized = "yes" if p.get("is_initialized") is True else "no"

        local_path = _find_local_project_dir(base_dir, org_slug, local_lookup_id, name)
        local = "Local" if local_path else "-"
        path_col = local_path or "-"
        rows.append((public_id or "-", name, initialized, local, path_col))

    header = ["UID", "Project", "Initialized", "Local", "Path"]
    if not rows:
        return "No projects."

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

    token_url = f"{backend_url.rstrip('/')}/orm/api/pods/runtime-credentials/token/"
    try:
        RuntimeCredentialAuthProvider(token_url=token_url).refresh(force=True)
    except Exception as exc:
        raise ApiError(f"Runtime credential exchange failed: {exc}") from exc

    access = (os.environ.get("MAINSEQUENCE_ACCESS_TOKEN") or "").strip()
    if not access:
        raise ApiError("Runtime credential exchange did not produce MAINSEQUENCE_ACCESS_TOKEN.")
    return access


def _resolve_project_dir(project_id: str | None, path: str | None) -> pathlib.Path:
    """
    Resolve project directory by:
      - explicit --path, or
      - current working directory when local `.env` exposes `MAIN_SEQUENCE_PROJECT_UID`, or
      - scanning base projects root for a matching project ref / legacy numeric suffix

    Raises:
        typer.Exit(1) on failure.
    """
    if path:
        p = normalize_path(path)
        if not p.exists():
            error(f"Folder does not exist: {p}")
            raise typer.Exit(1)
        return p

    if project_id is None:
        return _resolve_current_project_dir_from_env()

    cfg_obj = cfg.get_config()
    base = cfg_obj["mainsequence_path"]

    # If logged in, we can use org_slug from profile; if not, fall back to 'default'
    org_slug = "default"
    try:
        prof = get_current_user_profile()
        if prof and prof.get("organization"):
            org_slug = _org_slug_from_profile()
    except Exception:
        pass

    found = _find_local_project_dir(base, org_slug, project_id, None)
    if not found:
        error(
            "No local folder mapped for this project. Run `mainsequence project set-up-locally <project_uid>` first."
        )
        raise typer.Exit(1)

    p = pathlib.Path(found)
    if not p.exists():
        error(f"Folder missing: {p}")
        raise typer.Exit(1)
    return p


def _read_project_ref_from_env_file(project_dir: pathlib.Path) -> str | None:
    """
    Read the preferred local project reference from `<project_dir>/.env`.

    The public contract is `MAIN_SEQUENCE_PROJECT_UID`.
    Legacy `MAIN_SEQUENCE_PROJECT_ID` remains readable for older local clones.
    """
    env_path = project_dir / ".env"
    if not env_path.is_file():
        return None
    try:
        content = env_path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return None

    uid_match = re.search(r"(?m)^MAIN_SEQUENCE_PROJECT_UID=(.+?)\s*$", content)
    if uid_match:
        return uid_match.group(1).strip() or None

    id_match = re.search(r"(?m)^MAIN_SEQUENCE_PROJECT_ID=(.+?)\s*$", content)
    if id_match:
        return id_match.group(1).strip() or None

    return None


def _resolve_current_project_dir_from_env() -> pathlib.Path:
    """
    Resolve the current working directory as a project folder when local `.env`
    declares `MAIN_SEQUENCE_PROJECT_UID`.
    """
    cwd = pathlib.Path.cwd()
    if _read_project_ref_from_env_file(cwd) is None:
        error(
            "No PROJECT_UID was provided and the current directory does not expose "
            "MAIN_SEQUENCE_PROJECT_UID in .env."
        )
        raise typer.Exit(1)
    return cwd


def _resolve_project_id_from_local_env(path: str | None = None) -> str:
    """
    Resolve the local project reference from `<path>/.env` or `./.env`.
    """
    project_dir = normalize_path(path) if path else pathlib.Path.cwd()
    project_ref = _read_project_ref_from_env_file(project_dir)
    if project_ref is None:
        error(f"Could not determine project uid from {project_dir / '.env'}.")
        raise typer.Exit(1)
    return project_ref


def _project_agent_scaffold_bundle_dir(project_dir: pathlib.Path) -> pathlib.Path:
    """
    Resolve the `agent_scaffold` bundle from the target project's local `.venv`.
    """
    try:
        vp = ensure_venv(project_dir)
    except Exception as exc:
        error(f"Could not access the target project's .venv: {exc}")
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
        cwd=str(project_dir),
        capture_output=True,
        text=True,
    )
    if lookup.returncode != 0:
        detail = (lookup.stderr or lookup.stdout or "").strip()
        message = (
            "Could not locate agent_scaffold in the target project's .venv. "
            "Run `mainsequence project build_local_venv` or "
            "`mainsequence project update-sdk --path .` first."
        )
        if detail:
            message = f"{message} ({detail})"
        error(message)
        raise typer.Exit(1)

    bundle_dir = pathlib.Path((lookup.stdout or "").strip()).resolve()
    if not bundle_dir.exists() or not bundle_dir.is_dir():
        error(f"Target project .venv resolved an invalid agent_scaffold path: {bundle_dir}")
        raise typer.Exit(1)
    return bundle_dir


def _project_installed_package_version(project_dir: pathlib.Path, package_name: str) -> str:
    """
    Resolve an installed package version from the target project's local `.venv`.
    """
    try:
        vp = ensure_venv(project_dir)
    except Exception as exc:
        error(f"Could not access the target project's .venv: {exc}")
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
        cwd=str(project_dir),
        capture_output=True,
        text=True,
    )
    if lookup.returncode != 0:
        detail = (lookup.stderr or lookup.stdout or "").strip()
        message = (
            f"Could not resolve installed {package_name!r} version from the target "
            "project's .venv. Run `mainsequence project update-sdk --path .` first."
        )
        if detail:
            message = f"{message} ({detail})"
        error(message)
        raise typer.Exit(1)

    resolved_version = (lookup.stdout or "").strip()
    if not resolved_version:
        error(
            f"Could not resolve installed {package_name!r} version from the target project's .venv."
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


def _prompt_select_id(
    *,
    title: str,
    prompt_label: str,
    items: list[dict],
    rows: list[list[str]],
) -> int:
    if not items:
        raise RuntimeError(f"No options available for {prompt_label}.")
    print_table(title, ["ID", "Name", "Details"], rows)
    default_id = str(items[0].get("id"))
    picked = typer.prompt(prompt_label, default=default_id).strip()
    try:
        return int(picked)
    except ValueError as e:
        raise RuntimeError(f"Invalid {prompt_label}: {picked}") from e


def _prepare_batch_jobs_file_with_selected_related_image(
    *,
    project_id: str,
    batch_file: pathlib.Path,
    timeout: int | None,
) -> pathlib.Path:
    try:
        raw_config = yaml.safe_load(batch_file.read_text(encoding="utf-8")) or {}
    except Exception:
        return batch_file

    jobs_config = raw_config.get("jobs") if isinstance(raw_config, dict) else None
    if not isinstance(jobs_config, list):
        return batch_file

    if not jobs_config:
        return batch_file
    for raw_job in jobs_config:
        if not isinstance(raw_job, dict):
            return batch_file

    project_images = list_project_images(related_project_id=project_id, timeout=timeout)
    if not project_images:
        raise RuntimeError(
            "No project images are available. Create a project image before scheduling batch jobs."
        )

    rows = [
        [
            str(img.get("id") or "-"),
            str(img.get("project_repo_hash") or "-"),
            _format_base_image_label(img.get("base_image")),
        ]
        for img in project_images
    ]
    warn(
        f"All {len(jobs_config)} job(s) in {batch_file.name} will be scheduled on one project image. "
        "Select the image to use for this batch."
    )
    related_image_id = _prompt_select_id(
        title="Available Project Images",
        prompt_label="Related image ID",
        items=project_images,
        rows=rows,
    )
    selected_image = _find_image_by_id(project_images, related_image_id)
    if selected_image is None:
        raise RuntimeError(f"Related image not found: {related_image_id}")

    patched_config = dict(raw_config)
    patched_jobs: list[dict] = []
    overwritten_count = 0
    for raw_job in jobs_config:
        job_copy = dict(raw_job)
        existing_image_ref = job_copy.get("related_image_id")
        if existing_image_ref in (None, ""):
            existing_image_ref = job_copy.get("related_image")
        if existing_image_ref not in (None, "") and str(existing_image_ref) != str(
            related_image_id
        ):
            overwritten_count += 1
        job_copy.pop("related_image", None)
        job_copy["related_image_id"] = related_image_id
        patched_jobs.append(job_copy)
    patched_config["jobs"] = patched_jobs

    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        suffix=".yaml",
        prefix=f"{batch_file.stem}.",
        delete=False,
    ) as tmp_file:
        yaml.safe_dump(patched_config, tmp_file, sort_keys=False)
        temp_path = pathlib.Path(tmp_file.name)

    if overwritten_count:
        warn(
            f"Overriding related_image_id for {overwritten_count} job(s) so the entire batch uses image {related_image_id}."
        )
    info(f"Using project image {related_image_id} for all {len(jobs_config)} job(s) in this batch.")
    return temp_path


def _confirm_schedule_batch_jobs_submission(batch_file: pathlib.Path) -> bool:
    prompt_text = f"Schedule jobs from {batch_file.name}?"
    try:
        raw_config = yaml.safe_load(batch_file.read_text(encoding="utf-8")) or {}
    except Exception:
        raw_config = {}

    jobs_config = raw_config.get("jobs") if isinstance(raw_config, dict) else None
    if isinstance(jobs_config, list) and jobs_config:
        image_ids: list[str] = []
        for raw_job in jobs_config:
            if not isinstance(raw_job, dict):
                continue
            image_ref = raw_job.get("related_image_id")
            if image_ref in (None, ""):
                image_ref = raw_job.get("related_image")
            if image_ref not in (None, ""):
                image_ids.append(str(image_ref))
        unique_image_ids = sorted(set(image_ids))
        if len(unique_image_ids) == 1:
            prompt_text = (
                f"This will schedule all {len(jobs_config)} job(s) on the same image "
                f"({unique_image_ids[0]}). Continue?"
            )
        else:
            prompt_text = f"This will schedule {len(jobs_config)} job(s). Continue?"

    if not typer.confirm(prompt_text, default=False):
        info("Cancelled.")
        return False
    return True


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


def _format_data_node_storage_delete_preview(storage: dict[str, object]) -> list[tuple[str, str]]:
    return [
        ("UID", str(storage.get("uid") or "-")),
        ("Physical Table", str(storage.get("physical_table_name") or "-")),
        ("Identifier", str(storage.get("identifier") or "-")),
        ("Source Class", str(storage.get("source_class_name") or "-")),
        ("Data Source", _format_data_node_storage_data_source(storage.get("data_source"))),
        ("Protected", str(storage.get("protect_from_deletion"))),
    ]


def _format_meta_table_delete_preview(meta_table: dict[str, object]) -> list[tuple[str, str]]:
    return [
        ("UID", str(meta_table.get("uid") or "-")),
        ("Identifier", str(meta_table.get("identifier") or "-")),
        ("Namespace", str(meta_table.get("namespace") or "-")),
        ("Physical Table", str(meta_table.get("physical_table_name") or "-")),
        ("Management Mode", str(meta_table.get("management_mode") or "-")),
        ("Data Source", _format_data_node_storage_data_source(meta_table.get("data_source"))),
        ("Protected", str(meta_table.get("protect_from_deletion"))),
    ]


def _format_project_image_delete_preview(image: dict[str, object]) -> list[tuple[str, str]]:
    return [
        ("ID", str(image.get("id") or "-")),
        ("Project Repo Hash", str(image.get("project_repo_hash") or "-")),
        ("Base Image", _format_base_image_label(image.get("base_image"))),
        ("Is Ready", str(image.get("is_ready")) if image.get("is_ready") is not None else "-"),
    ]


def _format_resource_release_delete_preview(release: dict[str, object]) -> list[tuple[str, str]]:
    return [
        ("ID", str(release.get("id") or "-")),
        ("Release Kind", str(release.get("release_kind") or "-")),
        ("Subdomain", str(release.get("subdomain") or "-")),
        ("Resource", str(release.get("resource") or "-")),
        ("Related Image", _format_related_image_label(release.get("related_image"))),
    ]


def _git_run(project_dir: pathlib.Path, args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "-C", str(project_dir), *args],
        capture_output=True,
        text=True,
    )


def _git_upstream_ref(project_dir: pathlib.Path) -> str | None:
    result = _git_run(
        project_dir, ["rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{upstream}"]
    )
    if result.returncode != 0:
        return None
    upstream = (result.stdout or "").strip()
    return upstream or None


def _get_remote_branch_head_commit(project_dir: pathlib.Path) -> tuple[str, str]:
    upstream = _git_upstream_ref(project_dir)
    if not upstream:
        raise RuntimeError(
            "Current branch has no upstream remote branch. Push with --set-upstream before listing project resources."
        )

    result = _git_run(project_dir, ["rev-parse", upstream])
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


def _list_pushed_commits(project_dir: pathlib.Path, limit: int = 20) -> list[dict[str, str]]:
    """
    List commits already present on the remote-tracking branch.

    Preference order:
      1. current branch upstream
      2. all remote refs
    """
    refs: list[str] = []
    upstream = _git_upstream_ref(project_dir)
    if upstream:
        refs = [upstream]
    else:
        refs_result = _git_run(
            project_dir, ["for-each-ref", "--format=%(refname:short)", "refs/remotes"]
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
        project_dir,
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


def _list_unpushed_commits(project_dir: pathlib.Path, limit: int = 10) -> list[dict[str, str]]:
    """
    List local commits reachable from HEAD that are not present on any remote ref.
    """
    result = _git_run(
        project_dir,
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


def _is_pushed_commit(project_dir: pathlib.Path, commit_hash: str) -> bool:
    result = _git_run(project_dir, ["branch", "-r", "--contains", commit_hash])
    if result.returncode != 0:
        return False
    refs = [
        line.strip()
        for line in (result.stdout or "").splitlines()
        if line.strip() and not line.strip().endswith("/HEAD")
    ]
    return bool(refs)


def _resolve_full_commit_hash(project_dir: pathlib.Path, commit_hash: str) -> str:
    normalized = str(commit_hash or "").strip()
    if not normalized:
        raise RuntimeError("project_repo_hash is required.")

    result = _git_run(project_dir, ["rev-parse", "--verify", f"{normalized}^{{commit}}"])
    if result.returncode != 0:
        reason = (result.stderr or result.stdout or "").strip() or "git rev-parse failed"
        raise RuntimeError(f"Could not resolve project_repo_hash to a full commit SHA: {reason}")

    full_hash = (result.stdout or "").strip()
    if not re.fullmatch(r"[0-9a-fA-F]{40}", full_hash):
        raise RuntimeError("Resolved project_repo_hash is not a full 40-character commit SHA.")

    return full_hash.lower()


def _group_project_images_by_hash(images: list[dict]) -> dict[str, list[dict]]:
    grouped: dict[str, list[dict]] = {}
    for image in images:
        commit_hash = str(image.get("project_repo_hash") or "").strip()
        if not commit_hash:
            continue
        grouped.setdefault(commit_hash, []).append(image)
    return grouped


def _format_base_image_label(value) -> str:
    if isinstance(value, dict):
        return str(value.get("title") or value.get("id") or "-")
    if value is None:
        return "-"
    return str(value)


def _format_image_ids(images: list[dict]) -> str:
    ids = [str(img.get("id")) for img in images if img.get("id") is not None]
    return ", ".join(ids) if ids else "-"


def _format_related_image_label(value) -> str:
    if isinstance(value, dict):
        return str(value.get("id") or value.get("title") or "-")
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


def _format_data_node_storage_data_source(value) -> str:
    if isinstance(value, dict):
        display_name = str(value.get("display_name") or "").strip()
        class_type = str(value.get("class_type") or "").strip()
        if display_name and class_type:
            return f"{display_name} ({class_type})"
        if display_name:
            return display_name
        if class_type:
            return class_type
        if value.get("id") is not None:
            return str(value.get("id"))
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


def _find_image_by_id(images: list[dict], image_id: int | None) -> dict | None:
    if image_id is None:
        return None
    return next((img for img in images if str(img.get("id")) == str(image_id)), None)


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


def _extract_batch_job_dict(item) -> dict:
    if isinstance(item, dict):
        nested_job = item.get("job")
        if isinstance(nested_job, dict):
            return nested_job
        return item
    return {}


def _format_batch_job_ref(item) -> tuple[str, str, str, str]:
    job = _extract_batch_job_dict(item)
    fallback_id = item.get("id") if isinstance(item, dict) else "-"
    fallback_name = item.get("name") if isinstance(item, dict) else "-"
    job_id = str(job.get("id") or fallback_id or "-")
    name = str(job.get("name") or fallback_name or "-")
    execution_path = str(job.get("execution_path") or "-")
    app_name = str(job.get("app_name") or "-")
    return job_id, name, execution_path, app_name


def _format_batch_job_reason(item) -> str:
    if not isinstance(item, dict):
        return "-"
    for field_name in ("reason", "detail", "message", "error"):
        value = item.get(field_name)
        if value not in (None, ""):
            return str(value)
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


def _extract_python_version_from_spec(spec: str | None) -> str | None:
    if not spec:
        return None
    cleaned = str(spec).strip()
    m = re.search(r"(?<!\d)(\d+)\.(\d+)(?:\.(\d+))?", cleaned)
    if not m:
        return None
    major, minor, patch = m.group(1), m.group(2), m.group(3)
    if patch and re.fullmatch(r"\d+\.\d+\.\d+", cleaned):
        return f"{major}.{minor}.{patch}"
    return f"{major}.{minor}"


def _extract_python_version_from_pyproject_text(pyproject_text: str) -> str | None:
    """
    Extract python version from pyproject.toml text.

    Supported keys:
      - [project].requires-python
      - [tool.poetry.dependencies].python
    """
    try:
        import tomllib

        data = tomllib.loads(pyproject_text)
    except Exception:
        data = {}

    candidates: list[str] = []
    if isinstance(data, dict):
        project_data = data.get("project") or {}
        if isinstance(project_data, dict):
            req = project_data.get("requires-python")
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
        parsed = _extract_python_version_from_spec(spec)
        if parsed:
            return parsed

    # Fallback regex parsing for partially-invalid TOML or non-standard formatting.
    req_match = re.search(r'(?im)^\s*requires-python\s*=\s*["\']([^"\']+)["\']\s*$', pyproject_text)
    if req_match:
        parsed = _extract_python_version_from_spec(req_match.group(1))
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
            return _extract_python_version_from_spec(py_match.group(1))

    return None


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


def _current_project_runtime_auth_env(backend_url: str) -> dict[str, str]:
    """
    Return auth environment entries for local project `.env` provisioning.

    The output follows the active auth mode:
    - runtime credential mode writes runtime credential keys and an exchanged access token
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


def _render_project_runtime_env_text(
    env_text: str,
    *,
    auth_env: dict[str, str],
    backend_url: str,
    project_runtime_uid: str | None = None,
) -> str:
    """
    Return `.env` text with managed runtime auth keys refreshed.

    Managed keys are rewritten from scratch to avoid duplicate stale entries.
    """
    managed_prefixes = (
        "MAINSEQUENCE_AUTH_MODE=",
        "MAINSEQUENCE_ACCESS_TOKEN=",
        "MAINSEQUENCE_REFRESH_TOKEN=",
        "MAINSEQUENCE_RUNTIME_CREDENTIAL_ID=",
        "MAINSEQUENCE_RUNTIME_CREDENTIAL_SECRET=",
        "MAINSEQUENCE_ENDPOINT=",
        "MAIN_SEQUENCE_PROJECT_UID=",
        "MAIN_SEQUENCE_PROJECT_ID=",
    )
    lines = [
        ln
        for ln in (env_text or "").replace("\r", "").splitlines()
        if not any(ln.startswith(prefix) for prefix in managed_prefixes)
    ]

    if lines and lines[-1] != "":
        lines.append("")

    lines.extend(
        [f"{key}={value}" for key, value in auth_env.items() if value]
        + [
            f"MAINSEQUENCE_ENDPOINT={backend_url}",
        ]
        + (
            [f"MAIN_SEQUENCE_PROJECT_UID={project_runtime_uid}"]
            if project_runtime_uid is not None
            else []
        )
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
    projects_base: str | None = typer.Argument(
        None,
        help="Optional local projects base folder, for example mainsequence-dev.",
    ),
    access_token: str | None = typer.Option(None, "--access-token", help="JWT access token."),
    refresh_token: str | None = typer.Option(None, "--refresh-token", help="JWT refresh token."),
    no_open: bool = typer.Option(
        False,
        "--no-open",
        help="Do not auto-open a browser. Print the authorization URL and wait for callback.",
    ),
    backend_option: str | None = typer.Option(
        None,
        "--backend",
        help="Backend URL or host[:port], for example http://127.0.0.1:8000.",
    ),
    projects_base_option: str | None = typer.Option(
        None,
        "--projects-base",
        "--base-folder",
        help="Local projects base folder for this terminal session, for example mainsequence-dev.",
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
    When no backend is provided, login defaults to the standard production backend.

    Interactive login uses browser-based authentication and finishes with
    standard JWT access/refresh tokens persisted by the CLI.

    If `MAINSEQUENCE_AUTH_MODE=runtime_credential`, login exchanges the
    configured runtime credential for a short-lived access token instead of
    opening the browser or persisting CLI JWT tokens.

    Parameters
    ----------
    backend:
        Optional positional backend override for backward compatibility.
    projects_base:
        Optional positional projects base folder for backward compatibility.
    access_token:
        JWT access token for manual token import.
    refresh_token:
        JWT refresh token for manual token import.
    no_open:
        If True, do not auto-open a browser. The CLI prints the auth URL.
    backend_option:
        Backend override for this terminal session.
    projects_base_option:
        Projects base-folder override for this terminal session.
    export:
        If True, print shell export lines for auth variables.

    Examples
    --------
    ```bash
    mainsequence login
    mainsequence login 127.0.0.1:8000 mainsequence-dev
    mainsequence login --no-open
    mainsequence login --access-token "$TOKEN" --refresh-token "$REFRESH"
    mainsequence login --access-token "$TOKEN" --refresh-token "$REFRESH" --backend http://127.0.0.1:8000 --projects-base mainsequence-dev
    mainsequence login --export
    MAINSEQUENCE_AUTH_MODE=runtime_credential mainsequence login
    ```
    """
    using_jwt = bool((access_token or "").strip() or (refresh_token or "").strip())
    using_runtime_credential = _runtime_credential_mode_enabled()

    if using_runtime_credential and using_jwt:
        error("Runtime credential login cannot be combined with --access-token/--refresh-token.")
        raise typer.Exit(1)

    if using_runtime_credential and no_open:
        warn("--no-open is ignored when MAINSEQUENCE_AUTH_MODE=runtime_credential.")

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
    effective_backend_input = (
        explicit_backend_input if explicit_backend_input is not None else cfg.STANDARD_BACKEND_URL
    )

    if projects_base and projects_base_option:
        if cfg.normalize_mainsequence_path(projects_base) != cfg.normalize_mainsequence_path(
            projects_base_option
        ):
            error(
                "Pass projects base either positionally or with --projects-base/--base-folder, not both."
            )
            raise typer.Exit(1)
    effective_projects_base_input = (
        projects_base_option if projects_base_option is not None else projects_base
    )

    if using_jwt:
        if not (access_token or "").strip() or not (refresh_token or "").strip():
            error("JWT login requires both --access-token and --refresh-token.")
            raise typer.Exit(1)
    elif access_token is not None or refresh_token is not None:
        error("JWT login requires both --access-token and --refresh-token.")
        raise typer.Exit(1)

    current_backend = cfg.backend_url()
    normalized_backend = cfg.normalize_backend_url(effective_backend_input)

    if explicit_backend_input is not None and normalized_backend != current_backend:
        if not effective_projects_base_input:
            error("When using a different backend, you must also specify a projects base folder.")
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
        error(f"Browser login failed: {e}")
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
        mainsequence_path=effective_projects_base_input,
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
    info(f"Projects base folder: {base}")
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

    Uses SDK client `User.get_logged_user()` as the single source of truth.

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
            ["ID", "Name", "Login"],
            [
                [
                    str(org.get("id") or "-"),
                    str(org.get("name") or "-"),
                    str(org.get("login") or org.get("slug") or "-"),
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
                str(team.get("id") or "-"),
                str(team.get("name") or "-"),
                str(team.get("description") or "-"),
                str(member_count),
                str(team.get("is_active")) if team.get("is_active") is not None else "-",
            ]
        )

    if rows:
        print_table("Organization Teams", ["ID", "Name", "Description", "Members", "Active"], rows)
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
    team_id: int,
    name: str | None,
    description: str | None,
    is_active: bool | None,
    timeout: int | None,
) -> None:
    _require_login()

    try:
        current = get_organization_team(team_id, timeout=timeout)
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
            team_id,
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

    success(f"Organization team updated: id={team_id}")
    print_kv("Updated Team", _format_team_preview(updated))


def _organization_teams_delete_impl(
    *,
    team_id: int,
    timeout: int | None,
) -> None:
    _require_login()

    try:
        team = get_organization_team(team_id, timeout=timeout)
    except ApiError as e:
        error(f"Organization team fetch failed: {e}")
        raise typer.Exit(1) from e

    verification_value = str(team.get("name") or team.get("id") or team_id)
    _require_delete_verification(
        preview_title="Organization Team Delete Preview",
        preview_items=_format_team_preview(team),
        verification_value=verification_value,
        verification_label="team name" if team.get("name") else "team id",
    )

    try:
        deleted = delete_organization_team(team_id, timeout=timeout)
    except ApiError as e:
        error(f"Organization team deletion failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(deleted):
        return

    success(f"Organization team deleted: id={team_id}")
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
    team_id: int = typer.Argument(..., help="Team ID."),
    name: str | None = typer.Option(None, "--name", help="New team name."),
    description: str | None = typer.Option(None, "--description", help="New team description."),
    is_active: bool | None = typer.Option(None, "--active/--inactive", help="Set active status."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Edit one organization team.
    """
    _organization_teams_edit_impl(
        team_id=team_id,
        name=name,
        description=description,
        is_active=is_active,
        timeout=timeout,
    )


@organization_teams_group.command("delete")
def organization_teams_delete_cmd(
    team_id: int = typer.Argument(..., help="Team ID."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Delete one organization team.
    """
    _organization_teams_delete_impl(team_id=team_id, timeout=timeout)


@organization_teams_group.command("can_view")
def organization_teams_can_view_cmd(
    team_id: int = typer.Argument(..., help="Team ID."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_user_list_impl(
        fetch_fn=list_team_users_can_view,
        object_label="Team",
        access_label="view",
        object_id=team_id,
        timeout=timeout,
    )


@organization_teams_group.command("can_edit")
def organization_teams_can_edit_cmd(
    team_id: int = typer.Argument(..., help="Team ID."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_user_list_impl(
        fetch_fn=list_team_users_can_edit,
        object_label="Team",
        access_label="edit",
        object_id=team_id,
        timeout=timeout,
    )


@organization_teams_group.command("add_to_view")
def organization_teams_add_to_view_cmd(
    team_id: int = typer.Argument(..., help="Team ID."),
    user_id: int = typer.Argument(..., help="User ID to grant view access."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_user_access_update_impl(
        action_fn=add_team_user_to_view,
        object_label="Team",
        action_label="add_to_view",
        object_id=team_id,
        user_id=user_id,
        timeout=timeout,
    )


@organization_teams_group.command("add_to_edit")
def organization_teams_add_to_edit_cmd(
    team_id: int = typer.Argument(..., help="Team ID."),
    user_id: int = typer.Argument(..., help="User ID to grant edit access."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_user_access_update_impl(
        action_fn=add_team_user_to_edit,
        object_label="Team",
        action_label="add_to_edit",
        object_id=team_id,
        user_id=user_id,
        timeout=timeout,
    )


@organization_teams_group.command("remove_from_view")
def organization_teams_remove_from_view_cmd(
    team_id: int = typer.Argument(..., help="Team ID."),
    user_id: int = typer.Argument(..., help="User ID to remove explicit view access from."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_user_access_update_impl(
        action_fn=remove_team_user_from_view,
        object_label="Team",
        action_label="remove_from_view",
        object_id=team_id,
        user_id=user_id,
        timeout=timeout,
    )


@organization_teams_group.command("remove_from_edit")
def organization_teams_remove_from_edit_cmd(
    team_id: int = typer.Argument(..., help="Team ID."),
    user_id: int = typer.Argument(..., help="User ID to remove explicit edit access from."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_user_access_update_impl(
        action_fn=remove_team_user_from_edit,
        object_label="Team",
        action_label="remove_from_edit",
        object_id=team_id,
        user_id=user_id,
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

    Prints backend URL and base projects path as JSON.

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
def settings_set_base(path: str = typer.Argument(..., help="New projects base folder")):
    """
    Set the base folder where projects are cloned locally.

    Parameters
    ----------
    path:
        New base path for local project folders.

    Examples
    --------
    ```bash
    mainsequence settings set-base ~/mainsequence
    ```
    """
    out = cfg.set_mainsequence_path(path)
    if _emit_json(out):
        return
    success(f"Projects base folder set to: {out['mainsequence_path']}")


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
    info(f"Projects base folder: {out.get('mainsequence_path')}")


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
        ("ID", str(team.get("id") or "-")),
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
    explicit_view_ids = payload.get("explicit_can_view_user_ids")
    explicit_edit_ids = payload.get("explicit_can_edit_user_ids")
    explicit_view_team_ids = payload.get("explicit_can_view_team_ids")
    explicit_edit_team_ids = payload.get("explicit_can_edit_team_ids")
    return [
        ("Action", str(payload.get("action") or "-")),
        ("Detail", str(payload.get("detail") or "-")),
        ("Object Reference", str(payload.get("object_uid") or payload.get("object_id") or "-")),
        ("Object Type", str(payload.get("object_type") or "-")),
        ("User ID", str(user.get("id") or "-")),
        ("Username", str(user.get("username") or "-")),
        ("Email", str(user.get("email") or "-")),
        ("Name", _render_shareable_user_name(user)),
        ("Team ID", str(team.get("id") or "-")),
        ("Team Name", _render_shareable_team_name(team)),
        ("Team Description", str(team.get("description") or "-")),
        ("Explicit Can View", str(payload.get("explicit_can_view"))),
        ("Explicit Can Edit", str(payload.get("explicit_can_edit"))),
        (
            "Explicit View User IDs",
            ", ".join(str(item) for item in explicit_view_ids)
            if isinstance(explicit_view_ids, list)
            else "-",
        ),
        (
            "Explicit Edit User IDs",
            ", ".join(str(item) for item in explicit_edit_ids)
            if isinstance(explicit_edit_ids, list)
            else "-",
        ),
        (
            "Explicit View Team IDs",
            ", ".join(str(item) for item in explicit_view_team_ids)
            if isinstance(explicit_view_team_ids, list)
            else "-",
        ),
        (
            "Explicit Edit Team IDs",
            ", ".join(str(item) for item in explicit_edit_team_ids)
            if isinstance(explicit_edit_team_ids, list)
            else "-",
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


def _load_json_yaml_object_file(file_path: pathlib.Path, *, field_label: str) -> dict[str, object]:
    try:
        payload = yaml.safe_load(file_path.read_text(encoding="utf-8"))
    except FileNotFoundError as e:
        raise ValueError(f"{field_label} file not found: {file_path}") from e
    except Exception as e:
        raise ValueError(f"Could not read {field_label} file {file_path}: {e}") from e

    if not isinstance(payload, dict):
        raise ValueError(f"{field_label} file must contain a JSON/YAML object at the top level.")
    return dict(payload)


def _parse_json_dict_option_or_file(
    *,
    raw_value: str | None,
    file_path: pathlib.Path | None,
    field_label: str,
) -> dict[str, object] | None:
    if raw_value is not None and file_path is not None:
        raise ValueError(f"Provide either {field_label} JSON or {field_label} file, not both.")
    if file_path is not None:
        return _load_json_yaml_object_file(file_path, field_label=field_label)
    if raw_value is not None:
        return _parse_json_dict_option(raw_value, field_label=field_label)
    return None


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


def _format_agent_ref_label(agent_ref: object) -> str:
    if isinstance(agent_ref, dict):
        return str(agent_ref.get("name") or agent_ref.get("uid") or "-")
    return str(agent_ref or "-")


def _format_user_summary_label(user_ref: object) -> str:
    if isinstance(user_ref, dict):
        return str(user_ref.get("username") or user_ref.get("email") or user_ref.get("id") or "-")
    return str(user_ref or "-")


def _format_agent_run_preview(agent_run_payload: dict[str, object]) -> list[tuple[str, str]]:
    return [
        ("ID", str(agent_run_payload.get("id") or "-")),
        ("Agent", _format_agent_ref_label(agent_run_payload.get("agent"))),
        ("Status", str(agent_run_payload.get("status") or "-")),
        ("Started At", str(agent_run_payload.get("started_at") or "-")),
        ("Ended At", str(agent_run_payload.get("ended_at") or "-")),
        ("LLM Provider", str(agent_run_payload.get("llm_provider") or "-")),
        ("LLM Model", str(agent_run_payload.get("llm_model") or "-")),
        ("Engine", str(agent_run_payload.get("engine_name") or "-")),
    ]


def _format_agent_run_details(agent_run_payload: dict[str, object]) -> list[tuple[str, str]]:
    return [
        ("Triggered By", _format_user_summary_label(agent_run_payload.get("triggered_by_user"))),
        ("Parent Run", str(agent_run_payload.get("parent_run") or "-")),
        ("Root Run", str(agent_run_payload.get("root_run") or "-")),
        ("Spawned By Step", str(agent_run_payload.get("spawned_by_step") or "-")),
        ("Session ID", str(agent_run_payload.get("session_id") or "-")),
        ("Thread ID", str(agent_run_payload.get("thread_id") or "-")),
        ("External Run ID", str(agent_run_payload.get("external_run_id") or "-")),
        ("Input Text", str(agent_run_payload.get("input_text") or "-")),
        ("Output Text", str(agent_run_payload.get("output_text") or "-")),
        ("Error Detail", str(agent_run_payload.get("error_detail") or "-")),
        (
            "Runtime Config Snapshot",
            _format_json_value(agent_run_payload.get("runtime_config_snapshot")),
        ),
        ("Usage Summary", _format_json_value(agent_run_payload.get("usage_summary"))),
        ("Run Metadata", _format_json_value(agent_run_payload.get("run_metadata"))),
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


def _agent_list_impl(
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
        agents = list_agents(timeout=timeout, filters=filters)
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
                ", ".join(str(item) for item in labels)
                if isinstance(labels, list) and labels
                else "-",
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
    limit: int,
    timeout: int | None,
) -> None:
    _require_login()

    try:
        results = semantic_search_agents(
            q,
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
    return_immediately: bool,
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
    attachment_paths = list(files or [])
    attachment_media_types = list(media_types or [])
    if attachment_media_types and len(attachment_media_types) != len(attachment_paths):
        error("--media-type must be provided once per --file when used.")
        raise typer.Exit(1)
    attachments = [
        {
            "path": str(path),
            "media_type": attachment_media_types[index]
            if attachment_media_types
            else "application/pdf",
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
            return_immediately=return_immediately,
            timeout=timeout,
        )
    except ApiError as e:
        error(str(e))
        error(f"A2A message id for exact retry: {effective_message_id}")
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
        error(
            "Do not pass `--filter agent_uid=...` with `--agent-uid`. "
            "Use only one agent scope."
        )
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


def _agent_run_list_impl(
    timeout: int | None,
    filter_entries: list[str] | None,
    show_filters: bool,
) -> None:
    filters = _resolve_cli_list_filters(
        model_ref=AGENT_RUN_MODEL_REF,
        filter_entries=filter_entries,
        show_filters=show_filters,
        command_label="Agent Runs",
    )
    _require_login()

    try:
        agent_runs = list_agent_runs(timeout=timeout, filters=filters)
    except ApiError as e:
        error(f"Agent runs fetch failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(agent_runs):
        return

    rows: list[list[str]] = []
    for agent_run_payload in agent_runs:
        rows.append(
            [
                str(agent_run_payload.get("id") or "-"),
                _format_agent_ref_label(agent_run_payload.get("agent")),
                str(agent_run_payload.get("status") or "-"),
                str(agent_run_payload.get("started_at") or "-"),
                str(agent_run_payload.get("ended_at") or "-"),
                str(agent_run_payload.get("llm_provider") or "-"),
                str(agent_run_payload.get("llm_model") or "-"),
                str(agent_run_payload.get("engine_name") or "-"),
            ]
        )

    if rows:
        print_table(
            "Agent Runs",
            ["ID", "Agent", "Status", "Started At", "Ended At", "Provider", "Model", "Engine"],
            rows,
        )
    else:
        info("No agent runs.")
    info(f"Total agent runs: {len(agent_runs)}")


def _agent_run_detail_impl(
    *,
    agent_run_id: int,
    timeout: int | None,
) -> None:
    _require_login()

    try:
        agent_run_payload = get_agent_run(agent_run_id, timeout=timeout)
    except ApiError as e:
        error(f"Agent run fetch failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(agent_run_payload):
        return

    print_kv("Agent Run", _format_agent_run_preview(agent_run_payload))
    print_kv("Agent Run Details", _format_agent_run_details(agent_run_payload))


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
    object_id: int | str,
    timeout: int | None,
) -> None:
    _require_login()

    try:
        access_state = fetch_fn(object_id, timeout=timeout)
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
                str(user.get("id") or "-"),
                str(user.get("username") or "-"),
                str(user.get("email") or "-"),
                _render_shareable_user_name(user),
            ]
        )

    title = f"{object_label} Users Who Can {effective_access_label.title()}"
    if user_rows:
        print_table(title, ["ID", "Username", "Email", "Name"], user_rows)
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
                str(team.get("id") or "-"),
                _render_shareable_team_name(team),
                str(team.get("description") or "-"),
                str(member_count),
            ]
        )

    teams_title = f"{object_label} Teams Who Can {effective_access_label.title()}"
    if team_rows:
        print_table(teams_title, ["ID", "Name", "Description", "Members"], team_rows)
    else:
        info(f"No teams can {effective_access_label} this {object_label.lower()}.")

    info(f"Total users who can {effective_access_label}: {len(users_payload)}")
    info(f"Total teams who can {effective_access_label}: {len(teams_payload)}")


def _shareable_user_access_update_impl(
    *,
    action_fn,
    object_label: str,
    action_label: str,
    object_id: int | str,
    user_id: int,
    timeout: int | None,
) -> None:
    _require_login()

    try:
        payload = action_fn(object_id, user_id, timeout=timeout)
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
    object_id: int | str,
    team_id: int,
    timeout: int | None,
) -> None:
    _require_login()

    try:
        payload = action_fn(object_id, team_id, timeout=timeout)
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
    object_id: int | str,
    labels: list[str] | None,
    timeout: int | None,
) -> None:
    parsed_labels = _parse_cli_csv_list(labels)
    if not parsed_labels:
        error("Provide at least one --label value.")
        raise typer.Exit(2)

    _require_login()

    try:
        payload = action_fn(object_id, parsed_labels, timeout=timeout)
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
            ("Dynamic Table UID", str(payload.get("dynamic_table_uid") or "-")),
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


def _format_workspace_preview(workspace_payload: dict[str, object]) -> list[tuple[str, str]]:
    labels = workspace_payload.get("labels")
    return [
        ("UID", str(workspace_payload.get("uid") or "-")),
        ("Title", str(workspace_payload.get("title") or "-")),
        ("Description", str(workspace_payload.get("description") or "-")),
        ("Category", str(workspace_payload.get("category") or "-")),
        ("Source", str(workspace_payload.get("source") or "-")),
        (
            "Layout Kind",
            str(workspace_payload.get("layoutKind") or workspace_payload.get("layout_kind") or "-"),
        ),
        (
            "Labels",
            ", ".join(str(item) for item in labels) if isinstance(labels, list) and labels else "-",
        ),
        (
            "Updated At",
            str(workspace_payload.get("updatedAt") or workspace_payload.get("updated_at") or "-"),
        ),
    ]


WORKSPACE_FILE_OPTION_HELP = "Path to a JSON or YAML workspace document."
WORKSPACE_WRITE_FIELD_ALIASES: dict[str, tuple[str, ...]] = {
    "title": ("title",),
    "description": ("description",),
    "labels": ("labels",),
    "category": ("category",),
    "source": ("source",),
    "schema_version": ("schema_version", "schemaVersion"),
    "required_permissions": ("required_permissions", "requiredPermissions"),
    "grid": ("grid",),
    "layout_kind": ("layout_kind", "layoutKind"),
    "auto_grid": ("auto_grid", "autoGrid"),
    "companions": ("companions",),
    "controls": ("controls",),
    "widgets": ("widgets",),
}


def _load_workspace_payload_file(file_path: pathlib.Path) -> dict[str, object]:
    try:
        payload = yaml.safe_load(file_path.read_text(encoding="utf-8"))
    except FileNotFoundError as e:
        raise ValueError(f"Workspace file not found: {file_path}") from e
    except Exception as e:
        raise ValueError(f"Could not read workspace file {file_path}: {e}") from e

    if not isinstance(payload, dict):
        raise ValueError("Workspace file must contain a JSON/YAML object at the top level.")
    return dict(payload)


def _workspace_write_kwargs_from_payload(
    payload: dict[str, object],
) -> dict[str, object]:
    kwargs: dict[str, object] = {}
    for field_name, aliases in WORKSPACE_WRITE_FIELD_ALIASES.items():
        for alias in aliases:
            if alias in payload:
                kwargs[field_name] = payload[alias]
                break
    return kwargs


def _apply_workspace_cli_overrides(
    workspace_kwargs: dict[str, object],
    *,
    title: str | None,
    description: str | None,
    labels: list[str] | None,
    category: str | None,
    source: str | None,
    layout_kind: str | None,
) -> dict[str, object]:
    if title is not None:
        workspace_kwargs["title"] = title
    if description is not None:
        workspace_kwargs["description"] = description
    if labels is not None:
        workspace_kwargs["labels"] = _parse_cli_csv_list(labels)
    if category is not None:
        workspace_kwargs["category"] = category
    if source is not None:
        workspace_kwargs["source"] = source
    if layout_kind is not None:
        workspace_kwargs["layout_kind"] = layout_kind
    return workspace_kwargs


def _format_workspace_details(workspace_payload: dict[str, object]) -> list[tuple[str, str]]:
    widgets = workspace_payload.get("widgets")
    companions = workspace_payload.get("companions")
    return [
        (
            "Schema Version",
            str(
                workspace_payload.get("schemaVersion")
                or workspace_payload.get("schema_version")
                or "-"
            ),
        ),
        (
            "Required Permissions",
            _format_json_value(
                workspace_payload.get("requiredPermissions")
                or workspace_payload.get("required_permissions")
            ),
        ),
        (
            "Created At",
            str(workspace_payload.get("createdAt") or workspace_payload.get("created_at") or "-"),
        ),
        ("Grid", _format_json_value(workspace_payload.get("grid"))),
        (
            "Auto Grid",
            _format_json_value(
                workspace_payload.get("autoGrid") or workspace_payload.get("auto_grid")
            ),
        ),
        ("Controls", _format_json_value(workspace_payload.get("controls"))),
        ("Widgets", _format_json_value(widgets)),
        ("Widget Count", str(len(widgets)) if isinstance(widgets, list) else "-"),
        ("Companions", _format_json_value(companions)),
        ("Companion Count", str(len(companions)) if isinstance(companions, list) else "-"),
    ]


def _format_registered_widget_type_preview(
    widget_payload: dict[str, object],
) -> list[tuple[str, str]]:
    return [
        (
            "Widget ID",
            str(widget_payload.get("widget_id") or widget_payload.get("widgetId") or "-"),
        ),
        ("Title", str(widget_payload.get("title") or "-")),
        ("Category", str(widget_payload.get("category") or "-")),
        ("Kind", str(widget_payload.get("kind") or "-")),
        ("Source", str(widget_payload.get("source") or "-")),
        (
            "Active",
            str(
                widget_payload.get("is_active")
                if "is_active" in widget_payload
                else widget_payload.get("isActive")
            ),
        ),
        (
            "Registry Version",
            str(
                widget_payload.get("registry_version")
                or widget_payload.get("registryVersion")
                or "-"
            ),
        ),
    ]


def _format_registered_widget_type_details(
    widget_payload: dict[str, object],
) -> list[tuple[str, str]]:
    known_keys = {
        "id",
        "widget_id",
        "widgetId",
        "title",
        "description",
        "category",
        "widget_version",
        "widgetVersion",
        "kind",
        "source",
        "is_active",
        "isActive",
        "registry_version",
        "registryVersion",
        "tags",
        "required_permissions",
        "requiredPermissions",
        "schema",
        "schema_payload",
        "io",
        "default_presentation",
        "defaultPresentation",
        "default_size",
        "defaultSize",
        "responsive",
        "usage_guidance",
        "usageGuidance",
        "capabilities",
        "examples",
        "checksum",
        "last_synced_at",
        "lastSyncedAt",
        "created_at",
        "createdAt",
        "updated_at",
        "updatedAt",
    }
    extra_fields = {key: value for key, value in widget_payload.items() if key not in known_keys}
    return [
        ("Description", str(widget_payload.get("description") or "-")),
        ("Tags", _format_json_value(widget_payload.get("tags"))),
        (
            "Required Permissions",
            _format_json_value(
                widget_payload.get("required_permissions")
                or widget_payload.get("requiredPermissions")
            ),
        ),
        (
            "Schema",
            _format_json_value(
                widget_payload.get("schema_payload") or widget_payload.get("schema")
            ),
        ),
        ("IO", _format_json_value(widget_payload.get("io"))),
        (
            "Default Presentation",
            _format_json_value(
                widget_payload.get("default_presentation")
                or widget_payload.get("defaultPresentation")
            ),
        ),
        (
            "Default Size",
            _format_json_value(
                widget_payload.get("default_size") or widget_payload.get("defaultSize")
            ),
        ),
        ("Responsive", _format_json_value(widget_payload.get("responsive"))),
        (
            "Usage Guidance",
            _format_json_value(
                widget_payload.get("usage_guidance") or widget_payload.get("usageGuidance")
            ),
        ),
        ("Capabilities", _format_json_value(widget_payload.get("capabilities"))),
        ("Examples", _format_json_value(widget_payload.get("examples"))),
        ("Extra Fields", _format_json_value(extra_fields) if extra_fields else "-"),
        ("Checksum", str(widget_payload.get("checksum") or "-")),
        (
            "Last Synced At",
            str(widget_payload.get("last_synced_at") or widget_payload.get("lastSyncedAt") or "-"),
        ),
        (
            "Created At",
            str(widget_payload.get("created_at") or widget_payload.get("createdAt") or "-"),
        ),
        (
            "Updated At",
            str(widget_payload.get("updated_at") or widget_payload.get("updatedAt") or "-"),
        ),
    ]


def _format_connection_type_preview(
    connection_type_payload: dict[str, object],
) -> list[tuple[str, str]]:
    return [
        (
            "Type ID",
            str(connection_type_payload.get("type_id") or connection_type_payload.get("id") or "-"),
        ),
        (
            "Version",
            str(
                connection_type_payload.get("type_version")
                or connection_type_payload.get("version")
                or "-"
            ),
        ),
        ("Title", str(connection_type_payload.get("title") or "-")),
        ("Category", str(connection_type_payload.get("category") or "-")),
        ("Source", str(connection_type_payload.get("source") or "-")),
        (
            "Access Mode",
            str(
                connection_type_payload.get("access_mode")
                or connection_type_payload.get("accessMode")
                or "-"
            ),
        ),
    ]


def _format_connection_type_details(
    connection_type_payload: dict[str, object],
) -> list[tuple[str, str]]:
    return [
        ("Description", str(connection_type_payload.get("description") or "-")),
        ("Tags", _format_json_value(connection_type_payload.get("tags"))),
        ("Capabilities", _format_json_value(connection_type_payload.get("capabilities"))),
        (
            "Public Config Schema",
            _format_json_value(
                connection_type_payload.get("public_config_schema")
                or connection_type_payload.get("publicConfigSchema")
            ),
        ),
        (
            "Secure Config Schema",
            _format_json_value(
                connection_type_payload.get("secure_config_schema")
                or connection_type_payload.get("secureConfigSchema")
            ),
        ),
        (
            "Query Models",
            _format_json_value(
                connection_type_payload.get("query_models")
                or connection_type_payload.get("queryModels")
            ),
        ),
        (
            "Required Permissions",
            _format_json_value(
                connection_type_payload.get("required_permissions")
                or connection_type_payload.get("requiredPermissions")
            ),
        ),
        (
            "Usage Guidance",
            str(
                connection_type_payload.get("usage_guidance")
                or connection_type_payload.get("usageGuidance")
                or "-"
            ),
        ),
        ("Examples", _format_json_value(connection_type_payload.get("examples"))),
    ]


def _format_connection_preview(connection_payload: dict[str, object]) -> list[tuple[str, str]]:
    return [
        ("UID", str(connection_payload.get("uid") or "-")),
        ("Name", str(connection_payload.get("name") or "-")),
        (
            "Type ID",
            str(connection_payload.get("type_id") or connection_payload.get("typeId") or "-"),
        ),
        (
            "Type Version",
            str(
                connection_payload.get("type_version")
                or connection_payload.get("typeVersion")
                or "-"
            ),
        ),
        ("Status", str(connection_payload.get("status") or "-")),
        (
            "Workspace UID",
            str(
                connection_payload.get("workspace_uid")
                or connection_payload.get("workspaceUid")
                or "-"
            ),
        ),
        (
            "Default",
            str(
                connection_payload.get("is_default")
                if "is_default" in connection_payload
                else connection_payload.get("isDefault")
            ),
        ),
        (
            "System",
            str(
                connection_payload.get("is_system")
                if "is_system" in connection_payload
                else connection_payload.get("isSystem")
            ),
        ),
    ]


def _format_connection_details(connection_payload: dict[str, object]) -> list[tuple[str, str]]:
    return [
        ("Description", str(connection_payload.get("description") or "-")),
        (
            "Organization UID",
            str(
                connection_payload.get("organization_uid")
                or connection_payload.get("organizationUid")
                or "-"
            ),
        ),
        (
            "Public Config",
            _format_json_value(
                connection_payload.get("public_config") or connection_payload.get("publicConfig")
            ),
        ),
        (
            "Secure Fields",
            _format_json_value(
                connection_payload.get("secure_fields") or connection_payload.get("secureFields")
            ),
        ),
        (
            "Status Message",
            str(
                connection_payload.get("status_message")
                or connection_payload.get("statusMessage")
                or "-"
            ),
        ),
        (
            "Last Health Check At",
            str(
                connection_payload.get("last_health_check_at")
                or connection_payload.get("lastHealthCheckAt")
                or "-"
            ),
        ),
        ("Tags", _format_json_value(connection_payload.get("tags"))),
        (
            "Created By User UID",
            str(
                connection_payload.get("created_by_user_uid")
                or connection_payload.get("createdByUserUid")
                or "-"
            ),
        ),
        (
            "Created At",
            str(connection_payload.get("created_at") or connection_payload.get("createdAt") or "-"),
        ),
        (
            "Updated At",
            str(connection_payload.get("updated_at") or connection_payload.get("updatedAt") or "-"),
        ),
    ]


def _workspace_list_impl(
    timeout: int | None,
    filter_entries: list[str] | None,
    show_filters: bool,
) -> None:
    filters = _resolve_cli_list_filters(
        model_ref=WORKSPACE_MODEL_REF,
        filter_entries=filter_entries,
        show_filters=show_filters,
        command_label="Workspaces",
    )
    _require_login()

    try:
        workspaces = list_workspaces(timeout=timeout, filters=filters)
    except ApiError as e:
        error(f"Workspaces fetch failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(workspaces):
        return

    rows: list[list[str]] = []
    for workspace_payload in workspaces:
        labels = workspace_payload.get("labels")
        rows.append(
            [
                str(workspace_payload.get("uid") or "-"),
                str(workspace_payload.get("title") or "-"),
                str(workspace_payload.get("category") or "-"),
                str(workspace_payload.get("source") or "-"),
                str(
                    workspace_payload.get("layoutKind")
                    or workspace_payload.get("layout_kind")
                    or "-"
                ),
                str(len(labels)) if isinstance(labels, list) else "-",
                str(
                    workspace_payload.get("updatedAt") or workspace_payload.get("updated_at") or "-"
                ),
            ]
        )

    if rows:
        print_table(
            "Workspaces",
            ["UID", "Title", "Category", "Source", "Layout", "Labels", "Updated At"],
            rows,
        )
    else:
        info("No workspaces.")
    info(f"Total workspaces: {len(workspaces)}")


def _workspace_create_impl(
    *,
    title: str | None,
    file_path: pathlib.Path | None,
    description: str | None,
    labels: list[str] | None,
    category: str | None,
    source: str | None,
    layout_kind: str | None,
    timeout: int | None,
) -> None:
    _require_login()

    workspace_kwargs: dict[str, object]
    if file_path is not None:
        try:
            workspace_kwargs = _workspace_write_kwargs_from_payload(
                _load_workspace_payload_file(file_path)
            )
        except ValueError as e:
            error(str(e))
            raise typer.Exit(1) from e
    else:
        workspace_title = (title or "").strip() or typer.prompt("Workspace title").strip()
        if not workspace_title:
            error("Workspace title is required.")
            raise typer.Exit(1)

        workspace_description = description
        if workspace_description is None:
            workspace_description = typer.prompt(
                "Workspace description", default="", show_default=False
            )

        workspace_kwargs = {
            "title": workspace_title,
            "description": workspace_description or "",
            "labels": _parse_cli_csv_list(labels),
            "category": category or "Custom",
            "source": source or "user",
            "layout_kind": layout_kind or "custom",
        }

    workspace_kwargs = _apply_workspace_cli_overrides(
        workspace_kwargs,
        title=(title.strip() if title is not None else None),
        description=description,
        labels=labels,
        category=category,
        source=source,
        layout_kind=layout_kind,
    )

    workspace_title = str(workspace_kwargs.get("title") or "").strip()
    if not workspace_title:
        error("Workspace title is required.")
        raise typer.Exit(1)

    try:
        created = create_workspace(timeout=timeout, **workspace_kwargs)
    except ApiError as e:
        error(f"Workspace creation failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(created):
        return

    success(f"Workspace created: {workspace_title}")
    print_kv("Created Workspace", _format_workspace_preview(created))


def _workspace_detail_impl(
    *,
    workspace_uid: str,
    timeout: int | None,
) -> None:
    _require_login()

    try:
        workspace_payload = get_workspace(workspace_uid, timeout=timeout)
    except ApiError as e:
        error(f"Workspace fetch failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(workspace_payload):
        return

    print_kv("Workspace", _format_workspace_preview(workspace_payload))
    print_kv("Workspace Details", _format_workspace_details(workspace_payload))


def _workspace_snapshot_impl(
    *,
    workspace_uid: str,
    output_path: pathlib.Path | None,
) -> None:
    _require_login()

    try:
        from mainsequence.client.command_center.workspace_snapshot import (
            _build_snapshot_url,
            _capture_workspace_snapshot,
            _resolve_command_center_url,
            _WorkspaceSnapshotError,
        )

        snapshot_url = _build_snapshot_url(_resolve_command_center_url(), workspace_uid)
        typer.echo(f"Snapshot URL: {snapshot_url}")
        archive_bytes, extracted_dir = _capture_workspace_snapshot(
            workspace_uid,
            output_path=output_path,
        )
    except _WorkspaceSnapshotError as e:
        error(f"Workspace snapshot failed: {e}")
        raise typer.Exit(1) from e

    extracted_dir_text = str(extracted_dir.expanduser())
    result = {
        "workspace_uid": workspace_uid,
        "archive_size_bytes": len(archive_bytes),
        "output_path": extracted_dir_text,
        "extracted_dir": extracted_dir_text,
    }
    if _emit_json(result):
        return

    success(f"Workspace snapshot captured: uid={workspace_uid}")
    print_kv(
        "Workspace Snapshot",
        [
            ("Workspace UID", str(workspace_uid)),
            ("Archive Size Bytes", str(len(archive_bytes))),
            ("Output Directory", extracted_dir_text),
        ],
    )


def _workspace_update_impl(
    *,
    workspace_uid: str,
    file_path: pathlib.Path,
    timeout: int | None,
) -> None:
    _require_login()

    try:
        workspace_kwargs = _workspace_write_kwargs_from_payload(
            _load_workspace_payload_file(file_path)
        )
    except ValueError as e:
        error(str(e))
        raise typer.Exit(1) from e

    if not workspace_kwargs:
        error("Workspace update payload does not include any writable fields.")
        raise typer.Exit(1)

    try:
        updated = update_workspace(workspace_uid, timeout=timeout, **workspace_kwargs)
    except ApiError as e:
        error(f"Workspace update failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(updated):
        return

    success(f"Workspace updated: uid={workspace_uid}")
    print_kv("Updated Workspace", _format_workspace_preview(updated))
    print_kv("Workspace Details", _format_workspace_details(updated))


def _workspace_delete_impl(
    *,
    workspace_uid: str,
    timeout: int | None,
) -> None:
    _require_login()

    try:
        workspace_payload = get_workspace(workspace_uid, timeout=timeout)
    except ApiError as e:
        error(f"Workspace fetch failed: {e}")
        raise typer.Exit(1) from e

    verification_value = str(
        workspace_payload.get("title") or workspace_payload.get("uid") or workspace_uid
    )
    _require_delete_verification(
        preview_title="Workspace Delete Preview",
        preview_items=_format_workspace_preview(workspace_payload),
        verification_value=verification_value,
        verification_label="workspace title" if workspace_payload.get("title") else "workspace uid",
    )

    try:
        deleted = delete_workspace(workspace_uid, timeout=timeout)
    except ApiError as e:
        error(f"Workspace deletion failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(deleted):
        return

    success(f"Workspace deleted: uid={workspace_uid}")
    print_kv("Deleted Workspace", _format_workspace_preview(deleted))


def _registered_widget_type_list_impl(
    timeout: int | None,
    filter_entries: list[str] | None,
    show_filters: bool,
) -> None:
    filters = _resolve_cli_list_filters(
        model_ref=REGISTERED_WIDGET_TYPE_MODEL_REF,
        filter_entries=filter_entries,
        show_filters=show_filters,
        command_label="Registered Widget Types",
    )
    _require_login()

    try:
        widgets = list_registered_widget_types(timeout=timeout, filters=filters)
    except ApiError as e:
        error(f"Registered widget types fetch failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(widgets):
        return

    rows: list[list[str]] = []
    for widget in widgets:
        rows.append(
            [
                str(widget.get("widget_id") or "-"),
                str(widget.get("title") or "-"),
                str(widget.get("category") or "-"),
                str(widget.get("kind") or "-"),
                str(widget.get("source") or "-"),
                str(widget.get("is_active")),
                str(widget.get("registry_version") or "-"),
            ]
        )

    if rows:
        print_table(
            "Registered Widget Types",
            ["Widget ID", "Title", "Category", "Kind", "Source", "Active", "Registry Version"],
            rows,
        )
    else:
        info("No registered widget types.")
    info(f"Total registered widget types: {len(widgets)}")


def _registered_widget_type_detail_impl(
    *,
    widget_id: str,
    timeout: int | None,
) -> None:
    _require_login()

    try:
        widget_payload = get_registered_widget_type(widget_id, timeout=timeout)
    except ApiError as e:
        error(f"Registered widget type fetch failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(widget_payload):
        return

    print_kv("Registered Widget Type", _format_registered_widget_type_preview(widget_payload))
    print_kv(
        "Registered Widget Type Details", _format_registered_widget_type_details(widget_payload)
    )


def _connection_type_list_impl(
    timeout: int | None,
    filter_entries: list[str] | None,
    show_filters: bool,
) -> None:
    filters = _resolve_cli_list_filters(
        model_ref=CONNECTION_TYPE_MODEL_REF,
        filter_entries=filter_entries,
        show_filters=show_filters,
        command_label="Connection Types",
    )
    _require_login()

    try:
        connection_types = list_connection_types(timeout=timeout, filters=filters)
    except ApiError as e:
        error(f"Connection types fetch failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(connection_types):
        return

    rows: list[list[str]] = []
    for connection_type_payload in connection_types:
        rows.append(
            [
                str(
                    connection_type_payload.get("type_id")
                    or connection_type_payload.get("id")
                    or "-"
                ),
                str(
                    connection_type_payload.get("type_version")
                    or connection_type_payload.get("version")
                    or "-"
                ),
                str(connection_type_payload.get("title") or "-"),
                str(connection_type_payload.get("category") or "-"),
                str(connection_type_payload.get("source") or "-"),
                str(
                    connection_type_payload.get("access_mode")
                    or connection_type_payload.get("accessMode")
                    or "-"
                ),
            ]
        )

    if rows:
        print_table(
            "Connection Types",
            ["Type ID", "Version", "Title", "Category", "Source", "Access Mode"],
            rows,
        )
    else:
        info("No connection types.")
    info(f"Total connection types: {len(connection_types)}")


def _connection_type_detail_impl(
    *,
    type_id: str,
    timeout: int | None,
) -> None:
    _require_login()

    try:
        connection_type_payload = get_connection_type(type_id, timeout=timeout)
    except ApiError as e:
        error(f"Connection type fetch failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(connection_type_payload):
        return

    print_kv("Connection Type", _format_connection_type_preview(connection_type_payload))
    print_kv("Connection Type Details", _format_connection_type_details(connection_type_payload))


def _connection_list_impl(
    timeout: int | None,
    filter_entries: list[str] | None,
    show_filters: bool,
) -> None:
    filters = _resolve_cli_list_filters(
        model_ref=CONNECTION_INSTANCE_MODEL_REF,
        filter_entries=filter_entries,
        show_filters=show_filters,
        command_label="Connections",
    )
    _require_login()

    try:
        connections_payload = list_connection_instances(timeout=timeout, filters=filters)
    except ApiError as e:
        error(f"Connections fetch failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(connections_payload):
        return

    rows: list[list[str]] = []
    for connection_payload in connections_payload:
        rows.append(
            [
                str(connection_payload.get("uid") or "-"),
                str(connection_payload.get("name") or "-"),
                str(connection_payload.get("type_id") or connection_payload.get("typeId") or "-"),
                str(connection_payload.get("status") or "-"),
                str(
                    connection_payload.get("workspace_uid")
                    or connection_payload.get("workspaceUid")
                    or "-"
                ),
                str(
                    connection_payload.get("is_default")
                    if "is_default" in connection_payload
                    else connection_payload.get("isDefault")
                ),
                str(
                    connection_payload.get("is_system")
                    if "is_system" in connection_payload
                    else connection_payload.get("isSystem")
                ),
            ]
        )

    if rows:
        print_table(
            "Connections",
            ["UID", "Name", "Type ID", "Status", "Workspace UID", "Default", "System"],
            rows,
        )
    else:
        info("No connections.")
    info(f"Total connections: {len(connections_payload)}")


def _connection_detail_impl(
    *,
    connection_uid: str,
    timeout: int | None,
) -> None:
    _require_login()

    try:
        connection_payload = get_connection_instance(connection_uid, timeout=timeout)
    except ApiError as e:
        error(f"Connection fetch failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(connection_payload):
        return

    print_kv("Connection", _format_connection_preview(connection_payload))
    print_kv("Connection Details", _format_connection_details(connection_payload))


def _parse_adapter_from_api_cli_objects(
    *,
    public_config_json: str | None,
    public_config_file: pathlib.Path | None,
    config_json: str | None,
    config_file: pathlib.Path | None,
    secure_config_json: str | None,
    secure_config_file: pathlib.Path | None,
    compiled_contract_json: str | None,
    compiled_contract_file: pathlib.Path | None,
) -> tuple[
    dict[str, object] | None,
    dict[str, object] | None,
    dict[str, object] | None,
    dict[str, object] | None,
]:
    public_config = _parse_json_dict_option_or_file(
        raw_value=public_config_json,
        file_path=public_config_file,
        field_label="public_config",
    )
    config_values = _parse_json_dict_option_or_file(
        raw_value=config_json,
        file_path=config_file,
        field_label="config_values",
    )
    secure_config = _parse_json_dict_option_or_file(
        raw_value=secure_config_json,
        file_path=secure_config_file,
        field_label="secure_config",
    )
    compiled_contract = _parse_json_dict_option_or_file(
        raw_value=compiled_contract_json,
        file_path=compiled_contract_file,
        field_label="compiled_contract",
    )
    return public_config, config_values, secure_config, compiled_contract


def _connection_create_adapter_from_api_impl(
    *,
    name: str,
    description: str | None,
    api_base_url: str | None,
    debug_api_base_url: str | None,
    public_config_json: str | None,
    public_config_file: pathlib.Path | None,
    config_json: str | None,
    config_file: pathlib.Path | None,
    secure_config_json: str | None,
    secure_config_file: pathlib.Path | None,
    compiled_contract_json: str | None,
    compiled_contract_file: pathlib.Path | None,
    workspace_uid: str | None,
    is_default: bool | None,
    tags: list[str] | None,
    contract_version: str | None,
    request_timeout_ms: int | None,
    query_cache_policy: str | None,
    query_cache_ttl_ms: int | None,
    dedupe_in_flight: bool | None,
    timeout: int | None,
) -> None:
    _require_login()
    connection_name = name.strip()
    if not connection_name:
        error("Connection name is required.")
        raise typer.Exit(1)

    try:
        public_config, config_values, secure_config, compiled_contract = (
            _parse_adapter_from_api_cli_objects(
                public_config_json=public_config_json,
                public_config_file=public_config_file,
                config_json=config_json,
                config_file=config_file,
                secure_config_json=secure_config_json,
                secure_config_file=secure_config_file,
                compiled_contract_json=compiled_contract_json,
                compiled_contract_file=compiled_contract_file,
            )
        )
        created = create_adapter_from_api_connection(
            name=connection_name,
            description=description or "",
            public_config=public_config,
            api_base_url=api_base_url,
            debug_api_base_url=debug_api_base_url,
            secure_config=secure_config,
            workspace_uid=workspace_uid,
            is_default=is_default,
            tags=_parse_cli_csv_list(tags) if tags else None,
            config_values=config_values,
            compiled_contract=compiled_contract,
            contract_version=contract_version,
            request_timeout_ms=request_timeout_ms,
            query_cache_policy=query_cache_policy,
            query_cache_ttl_ms=query_cache_ttl_ms,
            dedupe_in_flight=dedupe_in_flight,
            timeout=timeout,
        )
    except ValueError as e:
        error(str(e))
        raise typer.Exit(1) from e
    except ApiError as e:
        error(f"Adapter from API connection creation failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(created):
        return

    success(f"Adapter from API connection created: {connection_name}")
    print_kv("Connection", _format_connection_preview(created))
    print_kv("Connection Details", _format_connection_details(created))


def _connection_patch_adapter_from_api_impl(
    *,
    connection_uid: str,
    name: str | None,
    description: str | None,
    api_base_url: str | None,
    debug_api_base_url: str | None,
    public_config_json: str | None,
    public_config_file: pathlib.Path | None,
    config_json: str | None,
    config_file: pathlib.Path | None,
    secure_config_json: str | None,
    secure_config_file: pathlib.Path | None,
    compiled_contract_json: str | None,
    compiled_contract_file: pathlib.Path | None,
    workspace_uid: str | None,
    is_default: bool | None,
    tags: list[str] | None,
    contract_version: str | None,
    request_timeout_ms: int | None,
    query_cache_policy: str | None,
    query_cache_ttl_ms: int | None,
    dedupe_in_flight: bool | None,
    timeout: int | None,
) -> None:
    _require_login()
    connection_name = name.strip() if name is not None else None
    if name is not None and not connection_name:
        error("Connection name cannot be empty.")
        raise typer.Exit(1)

    try:
        public_config, config_values, secure_config, compiled_contract = (
            _parse_adapter_from_api_cli_objects(
                public_config_json=public_config_json,
                public_config_file=public_config_file,
                config_json=config_json,
                config_file=config_file,
                secure_config_json=secure_config_json,
                secure_config_file=secure_config_file,
                compiled_contract_json=compiled_contract_json,
                compiled_contract_file=compiled_contract_file,
            )
        )
        updated = patch_adapter_from_api_connection(
            connection_uid,
            name=connection_name,
            description=description,
            public_config=public_config,
            api_base_url=api_base_url,
            debug_api_base_url=debug_api_base_url,
            secure_config=secure_config,
            workspace_uid=workspace_uid,
            is_default=is_default,
            tags=_parse_cli_csv_list(tags) if tags else None,
            config_values=config_values,
            compiled_contract=compiled_contract,
            contract_version=contract_version,
            request_timeout_ms=request_timeout_ms,
            query_cache_policy=query_cache_policy,
            query_cache_ttl_ms=query_cache_ttl_ms,
            dedupe_in_flight=dedupe_in_flight,
            timeout=timeout,
        )
    except ValueError as e:
        error(str(e))
        raise typer.Exit(1) from e
    except ApiError as e:
        error(f"Adapter from API connection patch failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(updated):
        return

    success(f"Adapter from API connection updated: uid={connection_uid}")
    print_kv("Connection", _format_connection_preview(updated))
    print_kv("Connection Details", _format_connection_details(updated))


def _data_node_storage_list_impl(
    timeout: int | None,
    filter_entries: list[str] | None,
    show_filters: bool,
    data_source_id: int | None = None,
) -> None:
    filters = _resolve_cli_list_filters(
        model_ref=DATA_NODE_STORAGE_MODEL_REF,
        filter_entries=filter_entries,
        show_filters=show_filters,
        command_label="Data Node Storage",
    )
    filters = _merge_cli_filter_alias(
        filters,
        filter_key="data_source__id",
        value=data_source_id,
        option_name="data-source-id",
    )
    _require_login()

    try:
        storages = list_data_node_storages(timeout=timeout, filters=filters)
    except ApiError as e:
        error(f"Data node storages fetch failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(storages):
        return

    if storages:
        print_table(
            "Data Node Storages",
            [
                "UID",
                "Physical Table",
                "Source Class",
                "Identifier",
                "Namespace",
                "Data Source",
            ],
            _build_data_node_storage_rows(storages),
        )
    else:
        info("No data node storages.")
    info(f"Total data node storages: {len(storages)}")


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


def _build_data_node_storage_rows(storages: list[dict[str, object]]) -> list[list[str]]:
    rows: list[list[str]] = []
    for storage in storages:
        rows.append(
            [
                str(storage.get("uid") or "-"),
                str(storage.get("physical_table_name") or "-"),
                str(storage.get("source_class_name") or "-"),
                str(storage.get("identifier") or "-"),
                str(storage.get("namespace") or "-"),
                _format_data_node_storage_data_source(storage.get("data_source")),
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
                _format_data_node_storage_data_source(meta_table.get("data_source")),
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


def _unpack_data_node_storage_search_response(
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


def _data_node_storage_search_impl(
    *,
    command_label: str,
    title: str,
    q: str,
    filter_entries: list[str] | None,
    show_filters: bool,
    search_fn,
) -> None:
    filters = _resolve_cli_list_filters(
        model_ref=DATA_NODE_STORAGE_MODEL_REF,
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

    storages, pagination = _unpack_data_node_storage_search_response(payload)
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
            _build_data_node_storage_rows(storages),
        )
    else:
        info("No data node storages matched the search.")

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
        info(f'Returned data node storages for query "{q}": {len(storages)}')


def _print_data_node_storage_search_section(
    *,
    title: str,
    q: str,
    payload: dict[str, object] | list[dict[str, object]],
) -> int:
    storages, pagination = _unpack_data_node_storage_search_response(payload)
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
            _build_data_node_storage_rows(storages),
        )
    else:
        info(f'No data node storages matched "{q}" for {title.lower()}.')

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


@workspace.command("list")
def workspace_list_cmd(
    filter_entries: list[str] | None = typer.Option(None, "--filter", help=LIST_FILTER_OPTION_HELP),
    show_filters: bool = typer.Option(
        False, "--show-filters", help="Show the filters supported by this list command and exit."
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    List command-center workspaces visible to the authenticated user.
    """
    _workspace_list_impl(
        timeout=timeout,
        filter_entries=filter_entries,
        show_filters=show_filters,
    )


@workspace.command("create")
def workspace_create_cmd(
    title: str | None = typer.Argument(None, help="Workspace title."),
    file_path: pathlib.Path | None = typer.Option(
        None,
        "--file",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        resolve_path=True,
        help=WORKSPACE_FILE_OPTION_HELP,
    ),
    description: str | None = typer.Option(
        None, "--description", help="Optional workspace description."
    ),
    labels: list[str] | None = typer.Option(
        None, "--label", help="Repeatable or comma-separated workspace label."
    ),
    category: str | None = typer.Option(None, "--category", help="Workspace category."),
    source: str | None = typer.Option(None, "--source", help="Workspace source."),
    layout_kind: str | None = typer.Option(None, "--layout-kind", help="Workspace layout kind."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Create one command-center workspace.
    """
    _workspace_create_impl(
        title=title,
        file_path=file_path,
        description=description,
        labels=labels,
        category=category,
        source=source,
        layout_kind=layout_kind,
        timeout=timeout,
    )


@workspace.command("detail")
def workspace_detail_cmd(
    workspace_uid: str = typer.Argument(..., help="Workspace UID."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Show one command-center workspace in detail.
    """
    _workspace_detail_impl(workspace_uid=workspace_uid, timeout=timeout)


@workspace.command("snapshot")
def workspace_snapshot_cmd(
    workspace_uid: str = typer.Argument(..., help="Workspace UID."),
    output_path: pathlib.Path | None = typer.Option(
        None,
        "--output-path",
        "-o",
        help="Optional output directory path. If you pass a file-like path such as snapshot.zip, its stem is used as the output directory. Defaults to ~/mainsequence/workspaces/workspace-<workspace_uid>-<timestamp>/.",
    ),
):
    """
    Capture one command-center workspace snapshot and expand it into a directory.
    """
    _workspace_snapshot_impl(workspace_uid=workspace_uid, output_path=output_path)


@workspace.command("update")
def workspace_update_cmd(
    workspace_uid: str = typer.Argument(..., help="Workspace UID."),
    file_path: pathlib.Path = typer.Option(
        ...,
        "--file",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        resolve_path=True,
        help=WORKSPACE_FILE_OPTION_HELP,
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Update one command-center workspace from a JSON or YAML document.
    """
    _workspace_update_impl(
        workspace_uid=workspace_uid,
        file_path=file_path,
        timeout=timeout,
    )


@workspace.command("delete")
def workspace_delete_cmd(
    workspace_uid: str = typer.Argument(..., help="Workspace UID."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Delete one command-center workspace.
    """
    _workspace_delete_impl(workspace_uid=workspace_uid, timeout=timeout)


@workspace.command("add-label")
def workspace_add_label_cmd(
    workspace_uid: str = typer.Argument(..., help="Workspace UID."),
    labels: list[str] | None = typer.Option(
        None,
        "--label",
        help="Organizational label to add. Repeatable or comma-separated.",
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Add one or more organizational labels to a workspace.

    Labels are helpers for grouping and discovery only. They do not affect runtime behavior or functionality.
    """
    _labelable_object_labels_update_impl(
        action_fn=add_workspace_labels,
        object_label="Workspace",
        action_label="add-label",
        object_id=workspace_uid,
        labels=labels,
        timeout=timeout,
    )


@workspace.command("add_label", hidden=True)
def workspace_add_label_alias_cmd(
    workspace_uid: str = typer.Argument(..., help="Workspace UID."),
    labels: list[str] | None = typer.Option(None, "--label", help="Organizational label to add."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """Backward-compatible alias for `mainsequence cc workspace add-label`."""
    workspace_add_label_cmd(workspace_uid=workspace_uid, labels=labels, timeout=timeout)


@workspace.command("remove-label")
def workspace_remove_label_cmd(
    workspace_uid: str = typer.Argument(..., help="Workspace UID."),
    labels: list[str] | None = typer.Option(
        None,
        "--label",
        help="Organizational label to remove. Repeatable or comma-separated.",
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Remove one or more organizational labels from a workspace.

    Labels are helpers for grouping and discovery only. They do not affect runtime behavior or functionality.
    """
    _labelable_object_labels_update_impl(
        action_fn=remove_workspace_labels,
        object_label="Workspace",
        action_label="remove-label",
        object_id=workspace_uid,
        labels=labels,
        timeout=timeout,
    )


@workspace.command("remove_label", hidden=True)
def workspace_remove_label_alias_cmd(
    workspace_uid: str = typer.Argument(..., help="Workspace UID."),
    labels: list[str] | None = typer.Option(
        None, "--label", help="Organizational label to remove."
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """Backward-compatible alias for `mainsequence cc workspace remove-label`."""
    workspace_remove_label_cmd(workspace_uid=workspace_uid, labels=labels, timeout=timeout)


@registered_widget_type.command("list")
def registered_widget_type_list_cmd(
    filter_entries: list[str] | None = typer.Option(None, "--filter", help=LIST_FILTER_OPTION_HELP),
    show_filters: bool = typer.Option(
        False, "--show-filters", help="Show the filters supported by this list command and exit."
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    List registered widget types visible to the authenticated user.
    """
    _registered_widget_type_list_impl(
        timeout=timeout,
        filter_entries=filter_entries,
        show_filters=show_filters,
    )


@registered_widget_type.command("detail")
def registered_widget_type_detail_cmd(
    widget_id: str = typer.Argument(
        ..., help="Registered widget type unique identifier (`widget_id`)."
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Show one registered widget type in detail.
    """
    _registered_widget_type_detail_impl(widget_id=widget_id, timeout=timeout)


@connection_type.command("list")
def connection_type_list_cmd(
    filter_entries: list[str] | None = typer.Option(None, "--filter", help=LIST_FILTER_OPTION_HELP),
    show_filters: bool = typer.Option(
        False, "--show-filters", help="Show the filters supported by this list command and exit."
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    List Command Center connection types visible to the authenticated user.
    """
    _connection_type_list_impl(
        timeout=timeout,
        filter_entries=filter_entries,
        show_filters=show_filters,
    )


@connection_type.command("detail")
def connection_type_detail_cmd(
    type_id: str = typer.Argument(..., help="Connection type unique identifier (`type_id`)."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Show one Command Center connection type in detail.
    """
    _connection_type_detail_impl(type_id=type_id, timeout=timeout)


@connection.command("list")
def connection_list_cmd(
    filter_entries: list[str] | None = typer.Option(None, "--filter", help=LIST_FILTER_OPTION_HELP),
    show_filters: bool = typer.Option(
        False, "--show-filters", help="Show the filters supported by this list command and exit."
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    List Command Center connection instances visible to the authenticated user.
    """
    _connection_list_impl(
        timeout=timeout,
        filter_entries=filter_entries,
        show_filters=show_filters,
    )


@connection.command("detail")
def connection_detail_cmd(
    connection_uid: str = typer.Argument(
        ..., help="Connection instance stable unique identifier (`uid`)."
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Show one Command Center connection instance in detail.
    """
    _connection_detail_impl(connection_uid=connection_uid, timeout=timeout)


@connection.command("create-adapter-from-api")
@connection.command("create_adapter_from_api", hidden=True)
def connection_create_adapter_from_api_cmd(
    name: str = typer.Option(..., "--name", help="Connection display name."),
    description: str | None = typer.Option(
        None, "--description", help="Optional connection description."
    ),
    api_base_url: str | None = typer.Option(
        None, "--api-base-url", help="Backend-mode upstream API base URL."
    ),
    debug_api_base_url: str | None = typer.Option(
        None,
        "--debug-api-base-url",
        help="Direct-mode browser API base URL for local/tunnel development.",
    ),
    public_config_json: str | None = typer.Option(
        None, "--public-config-json", help="Full publicConfig JSON object."
    ),
    public_config_file: pathlib.Path | None = typer.Option(
        None,
        "--public-config-file",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        resolve_path=True,
        help="Path to a JSON/YAML publicConfig object.",
    ),
    config_json: str | None = typer.Option(
        None,
        "--config-json",
        help="JSON object for non-secret configValues declared by the API contract.",
    ),
    config_file: pathlib.Path | None = typer.Option(
        None,
        "--config-file",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        resolve_path=True,
        help="Path to a JSON/YAML configValues object.",
    ),
    secure_config_json: str | None = typer.Option(
        None,
        "--secure-config-json",
        help="JSON object for secureConfig secret values. Values are not returned by detail.",
    ),
    secure_config_file: pathlib.Path | None = typer.Option(
        None,
        "--secure-config-file",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        resolve_path=True,
        help="Path to a JSON/YAML secureConfig object.",
    ),
    compiled_contract_json: str | None = typer.Option(
        None, "--compiled-contract-json", help="JSON object for compiledContract."
    ),
    compiled_contract_file: pathlib.Path | None = typer.Option(
        None,
        "--compiled-contract-file",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        resolve_path=True,
        help="Path to a JSON/YAML compiledContract object.",
    ),
    workspace_uid: str | None = typer.Option(
        None, "--workspace-uid", help="Optional workspace scope UID."
    ),
    is_default: bool | None = typer.Option(
        None, "--default/--no-default", help="Set whether this is the default connection."
    ),
    tags: list[str] | None = typer.Option(
        None, "--tag", help="Repeatable or comma-separated connection tag."
    ),
    contract_version: str | None = typer.Option(
        None, "--contract-version", help="Optional API contract version pin."
    ),
    request_timeout_ms: int | None = typer.Option(
        None, "--request-timeout-ms", help="Adapter upstream request timeout in milliseconds."
    ),
    query_cache_policy: str | None = typer.Option(
        None, "--query-cache-policy", help="Adapter query cache policy: safe or disabled."
    ),
    query_cache_ttl_ms: int | None = typer.Option(
        None, "--query-cache-ttl-ms", help="Adapter query cache TTL in milliseconds."
    ),
    dedupe_in_flight: bool | None = typer.Option(
        None,
        "--dedupe-in-flight/--no-dedupe-in-flight",
        help="Enable or disable identical in-flight operation deduplication.",
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Create a command_center.adapter_from_api connection instance.
    """
    _connection_create_adapter_from_api_impl(
        name=name,
        description=description,
        api_base_url=api_base_url,
        debug_api_base_url=debug_api_base_url,
        public_config_json=public_config_json,
        public_config_file=public_config_file,
        config_json=config_json,
        config_file=config_file,
        secure_config_json=secure_config_json,
        secure_config_file=secure_config_file,
        compiled_contract_json=compiled_contract_json,
        compiled_contract_file=compiled_contract_file,
        workspace_uid=workspace_uid,
        is_default=is_default,
        tags=tags,
        contract_version=contract_version,
        request_timeout_ms=request_timeout_ms,
        query_cache_policy=query_cache_policy,
        query_cache_ttl_ms=query_cache_ttl_ms,
        dedupe_in_flight=dedupe_in_flight,
        timeout=timeout,
    )


@connection.command("patch-adapter-from-api")
@connection.command("patch_adapter_from_api", hidden=True)
def connection_patch_adapter_from_api_cmd(
    connection_uid: str = typer.Argument(
        ..., help="Adapter from API connection stable unique identifier (`uid`)."
    ),
    name: str | None = typer.Option(None, "--name", help="New connection display name."),
    description: str | None = typer.Option(None, "--description", help="New description."),
    api_base_url: str | None = typer.Option(
        None, "--api-base-url", help="Replace publicConfig with backend-mode API base URL."
    ),
    debug_api_base_url: str | None = typer.Option(
        None,
        "--debug-api-base-url",
        help="Replace publicConfig with direct-mode browser API base URL.",
    ),
    public_config_json: str | None = typer.Option(
        None, "--public-config-json", help="Replacement publicConfig JSON object."
    ),
    public_config_file: pathlib.Path | None = typer.Option(
        None,
        "--public-config-file",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        resolve_path=True,
        help="Path to a replacement JSON/YAML publicConfig object.",
    ),
    config_json: str | None = typer.Option(
        None,
        "--config-json",
        help="JSON object for configValues. Without URL options this patches existing publicConfig.",
    ),
    config_file: pathlib.Path | None = typer.Option(
        None,
        "--config-file",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        resolve_path=True,
        help="Path to a JSON/YAML configValues object.",
    ),
    secure_config_json: str | None = typer.Option(
        None, "--secure-config-json", help="Replacement secureConfig JSON object."
    ),
    secure_config_file: pathlib.Path | None = typer.Option(
        None,
        "--secure-config-file",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        resolve_path=True,
        help="Path to a replacement JSON/YAML secureConfig object.",
    ),
    compiled_contract_json: str | None = typer.Option(
        None, "--compiled-contract-json", help="JSON object for compiledContract."
    ),
    compiled_contract_file: pathlib.Path | None = typer.Option(
        None,
        "--compiled-contract-file",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        resolve_path=True,
        help="Path to a JSON/YAML compiledContract object.",
    ),
    workspace_uid: str | None = typer.Option(
        None, "--workspace-uid", help="New workspace scope UID."
    ),
    is_default: bool | None = typer.Option(
        None, "--default/--no-default", help="Set whether this is the default connection."
    ),
    tags: list[str] | None = typer.Option(
        None, "--tag", help="Repeatable or comma-separated replacement tag."
    ),
    contract_version: str | None = typer.Option(
        None, "--contract-version", help="API contract version pin."
    ),
    request_timeout_ms: int | None = typer.Option(
        None, "--request-timeout-ms", help="Adapter upstream request timeout in milliseconds."
    ),
    query_cache_policy: str | None = typer.Option(
        None, "--query-cache-policy", help="Adapter query cache policy: safe or disabled."
    ),
    query_cache_ttl_ms: int | None = typer.Option(
        None, "--query-cache-ttl-ms", help="Adapter query cache TTL in milliseconds."
    ),
    dedupe_in_flight: bool | None = typer.Option(
        None,
        "--dedupe-in-flight/--no-dedupe-in-flight",
        help="Enable or disable identical in-flight operation deduplication.",
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Patch a command_center.adapter_from_api connection instance.
    """
    _connection_patch_adapter_from_api_impl(
        connection_uid=connection_uid,
        name=name,
        description=description,
        api_base_url=api_base_url,
        debug_api_base_url=debug_api_base_url,
        public_config_json=public_config_json,
        public_config_file=public_config_file,
        config_json=config_json,
        config_file=config_file,
        secure_config_json=secure_config_json,
        secure_config_file=secure_config_file,
        compiled_contract_json=compiled_contract_json,
        compiled_contract_file=compiled_contract_file,
        workspace_uid=workspace_uid,
        is_default=is_default,
        tags=tags,
        contract_version=contract_version,
        request_timeout_ms=request_timeout_ms,
        query_cache_policy=query_cache_policy,
        query_cache_ttl_ms=query_cache_ttl_ms,
        dedupe_in_flight=dedupe_in_flight,
        timeout=timeout,
    )


@agent.command("list")
def agent_list_cmd(
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


@agent.command("search")
def agent_search_cmd(
    q: str = typer.Argument(..., help="Natural-language query to match against agents."),
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
        help="Stable A2A message.messageId to reuse for an exact retry.",
    ),
    return_immediately: bool = typer.Option(
        False,
        "--return-immediately",
        help="Ask the runtime to return immediately instead of waiting for final output.",
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
        return_immediately=return_immediately,
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


@agent.command("can_view")
def agent_can_view_cmd(
    agent_uid: str = pydantic_argument(AGENT_MODEL_REF, "uid", ..., help="Agent UID."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_user_list_impl(
        fetch_fn=list_agent_users_can_view,
        object_label="Agent",
        access_label="view",
        object_id=agent_uid,
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
        object_id=agent_uid,
        timeout=timeout,
    )


@agent.command("add_to_view")
def agent_add_to_view_cmd(
    agent_uid: str = pydantic_argument(AGENT_MODEL_REF, "uid", ..., help="Agent UID."),
    user_id: int = typer.Argument(..., help="User ID to grant view access."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_user_access_update_impl(
        action_fn=add_agent_user_to_view,
        object_label="Agent",
        action_label="add_to_view",
        object_id=agent_uid,
        user_id=user_id,
        timeout=timeout,
    )


@agent.command("add_to_edit")
def agent_add_to_edit_cmd(
    agent_uid: str = pydantic_argument(AGENT_MODEL_REF, "uid", ..., help="Agent UID."),
    user_id: int = typer.Argument(..., help="User ID to grant edit access."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_user_access_update_impl(
        action_fn=add_agent_user_to_edit,
        object_label="Agent",
        action_label="add_to_edit",
        object_id=agent_uid,
        user_id=user_id,
        timeout=timeout,
    )


@agent.command("remove_from_view")
def agent_remove_from_view_cmd(
    agent_uid: str = pydantic_argument(AGENT_MODEL_REF, "uid", ..., help="Agent UID."),
    user_id: int = typer.Argument(..., help="User ID to remove explicit view access from."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_user_access_update_impl(
        action_fn=remove_agent_user_from_view,
        object_label="Agent",
        action_label="remove_from_view",
        object_id=agent_uid,
        user_id=user_id,
        timeout=timeout,
    )


@agent.command("remove_from_edit")
def agent_remove_from_edit_cmd(
    agent_uid: str = pydantic_argument(AGENT_MODEL_REF, "uid", ..., help="Agent UID."),
    user_id: int = typer.Argument(..., help="User ID to remove explicit edit access from."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_user_access_update_impl(
        action_fn=remove_agent_user_from_edit,
        object_label="Agent",
        action_label="remove_from_edit",
        object_id=agent_uid,
        user_id=user_id,
        timeout=timeout,
    )


@agent.command("add_team_to_view")
def agent_add_team_to_view_cmd(
    agent_uid: str = pydantic_argument(AGENT_MODEL_REF, "uid", ..., help="Agent UID."),
    team_id: int = typer.Argument(..., help="Team ID to grant view access."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_team_access_update_impl(
        action_fn=add_agent_team_to_view,
        object_label="Agent",
        action_label="add_team_to_view",
        object_id=agent_uid,
        team_id=team_id,
        timeout=timeout,
    )


@agent.command("add_team_to_edit")
def agent_add_team_to_edit_cmd(
    agent_uid: str = pydantic_argument(AGENT_MODEL_REF, "uid", ..., help="Agent UID."),
    team_id: int = typer.Argument(..., help="Team ID to grant edit access."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_team_access_update_impl(
        action_fn=add_agent_team_to_edit,
        object_label="Agent",
        action_label="add_team_to_edit",
        object_id=agent_uid,
        team_id=team_id,
        timeout=timeout,
    )


@agent.command("remove_team_from_view")
def agent_remove_team_from_view_cmd(
    agent_uid: str = pydantic_argument(AGENT_MODEL_REF, "uid", ..., help="Agent UID."),
    team_id: int = typer.Argument(..., help="Team ID to remove explicit view access from."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_team_access_update_impl(
        action_fn=remove_agent_team_from_view,
        object_label="Agent",
        action_label="remove_team_from_view",
        object_id=agent_uid,
        team_id=team_id,
        timeout=timeout,
    )


@agent.command("remove_team_from_edit")
def agent_remove_team_from_edit_cmd(
    agent_uid: str = pydantic_argument(AGENT_MODEL_REF, "uid", ..., help="Agent UID."),
    team_id: int = typer.Argument(..., help="Team ID to remove explicit edit access from."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_team_access_update_impl(
        action_fn=remove_agent_team_from_edit,
        object_label="Agent",
        action_label="remove_team_from_edit",
        object_id=agent_uid,
        team_id=team_id,
        timeout=timeout,
    )


@agent_run_group.command("list")
def agent_run_list_cmd(
    filter_entries: list[str] | None = typer.Option(None, "--filter", help=LIST_FILTER_OPTION_HELP),
    show_filters: bool = typer.Option(
        False, "--show-filters", help="Show the filters supported by this list command and exit."
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    List agent runtime records.
    """
    _agent_run_list_impl(
        timeout=timeout,
        filter_entries=filter_entries,
        show_filters=show_filters,
    )


@agent_run_group.command("detail")
def agent_run_detail_cmd(
    agent_run_id: int = pydantic_argument(AGENT_RUN_MODEL_REF, "id", ..., help="Agent run ID."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Show one agent runtime record in detail.
    """
    _agent_run_detail_impl(agent_run_id=agent_run_id, timeout=timeout)


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
        object_id=constant_uid,
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
        object_id=constant_uid,
        timeout=timeout,
    )


@constants.command("add_to_view")
def constants_add_to_view_cmd(
    constant_uid: str = typer.Argument(..., help="Constant UID."),
    user_id: int = typer.Argument(..., help="User ID to grant view access."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Grant explicit view access to one user for one constant.

    Examples
    --------
    ```bash
    mainsequence constants add_to_view 498d499f-b74c-43f7-acf1-2e2955ad0e6b 7
    ```
    """
    _shareable_user_access_update_impl(
        action_fn=add_constant_user_to_view,
        object_label="Constant",
        action_label="add_to_view",
        object_id=constant_uid,
        user_id=user_id,
        timeout=timeout,
    )


@constants.command("add_to_edit")
def constants_add_to_edit_cmd(
    constant_uid: str = typer.Argument(..., help="Constant UID."),
    user_id: int = typer.Argument(..., help="User ID to grant edit access."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Grant explicit edit access to one user for one constant.

    Examples
    --------
    ```bash
    mainsequence constants add_to_edit 498d499f-b74c-43f7-acf1-2e2955ad0e6b 7
    ```
    """
    _shareable_user_access_update_impl(
        action_fn=add_constant_user_to_edit,
        object_label="Constant",
        action_label="add_to_edit",
        object_id=constant_uid,
        user_id=user_id,
        timeout=timeout,
    )


@constants.command("remove_from_view")
def constants_remove_from_view_cmd(
    constant_uid: str = typer.Argument(..., help="Constant UID."),
    user_id: int = typer.Argument(..., help="User ID to remove explicit view access from."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Remove explicit view access from one user for one constant.

    Examples
    --------
    ```bash
    mainsequence constants remove_from_view 498d499f-b74c-43f7-acf1-2e2955ad0e6b 7
    ```
    """
    _shareable_user_access_update_impl(
        action_fn=remove_constant_user_from_view,
        object_label="Constant",
        action_label="remove_from_view",
        object_id=constant_uid,
        user_id=user_id,
        timeout=timeout,
    )


@constants.command("remove_from_edit")
def constants_remove_from_edit_cmd(
    constant_uid: str = typer.Argument(..., help="Constant UID."),
    user_id: int = typer.Argument(..., help="User ID to remove explicit edit access from."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Remove explicit edit access from one user for one constant.

    Examples
    --------
    ```bash
    mainsequence constants remove_from_edit 498d499f-b74c-43f7-acf1-2e2955ad0e6b 7
    ```
    """
    _shareable_user_access_update_impl(
        action_fn=remove_constant_user_from_edit,
        object_label="Constant",
        action_label="remove_from_edit",
        object_id=constant_uid,
        user_id=user_id,
        timeout=timeout,
    )


@constants.command("add_team_to_view")
def constants_add_team_to_view_cmd(
    constant_uid: str = typer.Argument(..., help="Constant UID."),
    team_id: int = typer.Argument(..., help="Team ID to grant view access."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_team_access_update_impl(
        action_fn=add_constant_team_to_view,
        object_label="Constant",
        action_label="add_team_to_view",
        object_id=constant_uid,
        team_id=team_id,
        timeout=timeout,
    )


@constants.command("add_team_to_edit")
def constants_add_team_to_edit_cmd(
    constant_uid: str = typer.Argument(..., help="Constant UID."),
    team_id: int = typer.Argument(..., help="Team ID to grant edit access."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_team_access_update_impl(
        action_fn=add_constant_team_to_edit,
        object_label="Constant",
        action_label="add_team_to_edit",
        object_id=constant_uid,
        team_id=team_id,
        timeout=timeout,
    )


@constants.command("remove_team_from_view")
def constants_remove_team_from_view_cmd(
    constant_uid: str = typer.Argument(..., help="Constant UID."),
    team_id: int = typer.Argument(..., help="Team ID to remove explicit view access from."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_team_access_update_impl(
        action_fn=remove_constant_team_from_view,
        object_label="Constant",
        action_label="remove_team_from_view",
        object_id=constant_uid,
        team_id=team_id,
        timeout=timeout,
    )


@constants.command("remove_team_from_edit")
def constants_remove_team_from_edit_cmd(
    constant_uid: str = typer.Argument(..., help="Constant UID."),
    team_id: int = typer.Argument(..., help="Team ID to remove explicit edit access from."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_team_access_update_impl(
        action_fn=remove_constant_team_from_edit,
        object_label="Constant",
        action_label="remove_team_from_edit",
        object_id=constant_uid,
        team_id=team_id,
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
        object_id=secret_uid,
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
        object_id=secret_uid,
        timeout=timeout,
    )


@secrets.command("add_to_view")
def secrets_add_to_view_cmd(
    secret_uid: str = typer.Argument(..., help="Secret UID."),
    user_id: int = typer.Argument(..., help="User ID to grant view access."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Grant explicit view access to one user for one secret.

    Examples
    --------
    ```bash
    mainsequence secrets add_to_view 498d499f-b74c-43f7-acf1-2e2955ad0e6b 7
    ```
    """
    _shareable_user_access_update_impl(
        action_fn=add_secret_user_to_view,
        object_label="Secret",
        action_label="add_to_view",
        object_id=secret_uid,
        user_id=user_id,
        timeout=timeout,
    )


@secrets.command("add_to_edit")
def secrets_add_to_edit_cmd(
    secret_uid: str = typer.Argument(..., help="Secret UID."),
    user_id: int = typer.Argument(..., help="User ID to grant edit access."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Grant explicit edit access to one user for one secret.

    Examples
    --------
    ```bash
    mainsequence secrets add_to_edit 498d499f-b74c-43f7-acf1-2e2955ad0e6b 7
    ```
    """
    _shareable_user_access_update_impl(
        action_fn=add_secret_user_to_edit,
        object_label="Secret",
        action_label="add_to_edit",
        object_id=secret_uid,
        user_id=user_id,
        timeout=timeout,
    )


@secrets.command("remove_from_view")
def secrets_remove_from_view_cmd(
    secret_uid: str = typer.Argument(..., help="Secret UID."),
    user_id: int = typer.Argument(..., help="User ID to remove explicit view access from."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Remove explicit view access from one user for one secret.

    Examples
    --------
    ```bash
    mainsequence secrets remove_from_view 498d499f-b74c-43f7-acf1-2e2955ad0e6b 7
    ```
    """
    _shareable_user_access_update_impl(
        action_fn=remove_secret_user_from_view,
        object_label="Secret",
        action_label="remove_from_view",
        object_id=secret_uid,
        user_id=user_id,
        timeout=timeout,
    )


@secrets.command("remove_from_edit")
def secrets_remove_from_edit_cmd(
    secret_uid: str = typer.Argument(..., help="Secret UID."),
    user_id: int = typer.Argument(..., help="User ID to remove explicit edit access from."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Remove explicit edit access from one user for one secret.

    Examples
    --------
    ```bash
    mainsequence secrets remove_from_edit 498d499f-b74c-43f7-acf1-2e2955ad0e6b 7
    ```
    """
    _shareable_user_access_update_impl(
        action_fn=remove_secret_user_from_edit,
        object_label="Secret",
        action_label="remove_from_edit",
        object_id=secret_uid,
        user_id=user_id,
        timeout=timeout,
    )


@secrets.command("add_team_to_view")
def secrets_add_team_to_view_cmd(
    secret_uid: str = typer.Argument(..., help="Secret UID."),
    team_id: int = typer.Argument(..., help="Team ID to grant view access."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_team_access_update_impl(
        action_fn=add_secret_team_to_view,
        object_label="Secret",
        action_label="add_team_to_view",
        object_id=secret_uid,
        team_id=team_id,
        timeout=timeout,
    )


@secrets.command("add_team_to_edit")
def secrets_add_team_to_edit_cmd(
    secret_uid: str = typer.Argument(..., help="Secret UID."),
    team_id: int = typer.Argument(..., help="Team ID to grant edit access."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_team_access_update_impl(
        action_fn=add_secret_team_to_edit,
        object_label="Secret",
        action_label="add_team_to_edit",
        object_id=secret_uid,
        team_id=team_id,
        timeout=timeout,
    )


@secrets.command("remove_team_from_view")
def secrets_remove_team_from_view_cmd(
    secret_uid: str = typer.Argument(..., help="Secret UID."),
    team_id: int = typer.Argument(..., help="Team ID to remove explicit view access from."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_team_access_update_impl(
        action_fn=remove_secret_team_from_view,
        object_label="Secret",
        action_label="remove_team_from_view",
        object_id=secret_uid,
        team_id=team_id,
        timeout=timeout,
    )


@secrets.command("remove_team_from_edit")
def secrets_remove_team_from_edit_cmd(
    secret_uid: str = typer.Argument(..., help="Secret UID."),
    team_id: int = typer.Argument(..., help="Team ID to remove explicit edit access from."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_team_access_update_impl(
        action_fn=remove_secret_team_from_edit,
        object_label="Secret",
        action_label="remove_team_from_edit",
        object_id=secret_uid,
        team_id=team_id,
        timeout=timeout,
    )


def _data_node_storage_detail_impl(storage_uid: str, timeout: int | None) -> None:
    _require_login()

    try:
        storage = get_data_node_storage(storage_uid, timeout=timeout)
    except ApiError as e:
        error(f"Data node storage fetch failed: {e}")
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
        "Data Node Storage",
        [
            ("UID", str(storage.get("uid") or storage_uid)),
            ("Physical Table", str(storage.get("physical_table_name") or "-")),
            ("Identifier", str(storage.get("identifier") or "-")),
            ("Source Class", str(storage.get("source_class_name") or "-")),
            ("Data Source", _format_data_node_storage_data_source(storage.get("data_source"))),
            ("Protected", str(storage.get("protect_from_deletion"))),
            ("Created", str(storage.get("creation_date") or "-")),
            ("Created By", str(storage.get("created_by_user") or "-")),
            ("Organization", str(storage.get("organization_owner") or "-")),
            ("Description", str(storage.get("description") or "-")),
        ],
    )

    print_kv(
        "Data Node Storage Config",
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
            ("Data Source", _format_data_node_storage_data_source(meta_table.get("data_source"))),
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


def _data_node_storage_run_query_impl(
    *,
    storage_uid: str,
    sql: str,
    timeout: int | None,
) -> None:
    _require_login()

    try:
        payload = run_data_node_storage_query(storage_uid, sql, timeout=timeout)
    except ApiError as e:
        error(f"Data node query failed: {e}")
        raise typer.Exit(1) from e

    ok = bool(payload.get("ok"))
    if _emit_json(payload):
        if not ok:
            raise typer.Exit(1)
        return

    if ok:
        success(f"Data node query completed: uid={storage_uid}")
    else:
        error(f"Data node query failed: uid={storage_uid}")
    _print_storage_query_payload("Data Node Query", payload)
    if not ok:
        raise typer.Exit(1)


def _data_node_storage_delete_impl(
    *,
    storage_uid: str,
    full_delete_selected: bool,
    full_delete_downstream_tables: bool,
    delete_with_no_table: bool,
    override_protection: bool,
    timeout: int | None,
) -> None:
    _require_login()

    try:
        storage = get_data_node_storage(storage_uid, timeout=timeout)
    except ApiError as e:
        error(f"Data node storage fetch failed: {e}")
        raise typer.Exit(1) from e

    verification_value = str(
        storage.get("physical_table_name") or storage.get("uid") or storage_uid
    )
    verification_label = (
        "physical table name" if storage.get("physical_table_name") else "storage uid"
    )
    _require_delete_verification(
        preview_title="Data Node Storage Delete Preview",
        preview_items=_format_data_node_storage_delete_preview(storage)
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
        deleted = delete_data_node_storage(
            storage_uid,
            full_delete_selected=full_delete_selected,
            full_delete_downstream_tables=full_delete_downstream_tables,
            delete_with_no_table=delete_with_no_table,
            override_protection=override_protection,
            timeout=timeout,
        )
    except ApiError as e:
        error(f"Data node storage deletion failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(deleted):
        return

    success(f"Data node storage deleted: uid={storage_uid}")
    print_kv("Deleted Data Node Storage", _format_data_node_storage_delete_preview(deleted))


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
        object_id=meta_table_uid,
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
        object_id=meta_table_uid,
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
        object_id=meta_table_uid,
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
        object_id=meta_table_uid,
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
    user_id: int = typer.Argument(..., help="User ID to grant view access."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """Grant explicit view access to one user for one MetaTable."""
    _shareable_user_access_update_impl(
        action_fn=add_meta_table_user_to_view,
        object_label="MetaTable",
        action_label="add_to_view",
        object_id=meta_table_uid,
        user_id=user_id,
        timeout=timeout,
    )


@meta_table_group.command("add_to_edit")
def meta_table_add_to_edit_cmd(
    meta_table_uid: str = typer.Argument(..., help="MetaTable UID."),
    user_id: int = typer.Argument(..., help="User ID to grant edit access."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """Grant explicit edit access to one user for one MetaTable."""
    _shareable_user_access_update_impl(
        action_fn=add_meta_table_user_to_edit,
        object_label="MetaTable",
        action_label="add_to_edit",
        object_id=meta_table_uid,
        user_id=user_id,
        timeout=timeout,
    )


@meta_table_group.command("remove_from_view")
def meta_table_remove_from_view_cmd(
    meta_table_uid: str = typer.Argument(..., help="MetaTable UID."),
    user_id: int = typer.Argument(..., help="User ID to remove explicit view access from."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """Remove explicit view access from one user for one MetaTable."""
    _shareable_user_access_update_impl(
        action_fn=remove_meta_table_user_from_view,
        object_label="MetaTable",
        action_label="remove_from_view",
        object_id=meta_table_uid,
        user_id=user_id,
        timeout=timeout,
    )


@meta_table_group.command("remove_from_edit")
def meta_table_remove_from_edit_cmd(
    meta_table_uid: str = typer.Argument(..., help="MetaTable UID."),
    user_id: int = typer.Argument(..., help="User ID to remove explicit edit access from."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """Remove explicit edit access from one user for one MetaTable."""
    _shareable_user_access_update_impl(
        action_fn=remove_meta_table_user_from_edit,
        object_label="MetaTable",
        action_label="remove_from_edit",
        object_id=meta_table_uid,
        user_id=user_id,
        timeout=timeout,
    )


@meta_table_group.command("add_team_to_view")
def meta_table_add_team_to_view_cmd(
    meta_table_uid: str = typer.Argument(..., help="MetaTable UID."),
    team_id: int = typer.Argument(..., help="Team ID to grant view access."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_team_access_update_impl(
        action_fn=add_meta_table_team_to_view,
        object_label="MetaTable",
        action_label="add_team_to_view",
        object_id=meta_table_uid,
        team_id=team_id,
        timeout=timeout,
    )


@meta_table_group.command("add_team_to_edit")
def meta_table_add_team_to_edit_cmd(
    meta_table_uid: str = typer.Argument(..., help="MetaTable UID."),
    team_id: int = typer.Argument(..., help="Team ID to grant edit access."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_team_access_update_impl(
        action_fn=add_meta_table_team_to_edit,
        object_label="MetaTable",
        action_label="add_team_to_edit",
        object_id=meta_table_uid,
        team_id=team_id,
        timeout=timeout,
    )


@meta_table_group.command("remove_team_from_view")
def meta_table_remove_team_from_view_cmd(
    meta_table_uid: str = typer.Argument(..., help="MetaTable UID."),
    team_id: int = typer.Argument(..., help="Team ID to remove explicit view access from."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_team_access_update_impl(
        action_fn=remove_meta_table_team_from_view,
        object_label="MetaTable",
        action_label="remove_team_from_view",
        object_id=meta_table_uid,
        team_id=team_id,
        timeout=timeout,
    )


@meta_table_group.command("remove_team_from_edit")
def meta_table_remove_team_from_edit_cmd(
    meta_table_uid: str = typer.Argument(..., help="MetaTable UID."),
    team_id: int = typer.Argument(..., help="Team ID to remove explicit edit access from."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_team_access_update_impl(
        action_fn=remove_meta_table_team_from_edit,
        object_label="MetaTable",
        action_label="remove_team_from_edit",
        object_id=meta_table_uid,
        team_id=team_id,
        timeout=timeout,
    )


@data_node_storage_group.command("list")
def data_node_storage_list_cmd(
    data_source_id: int | None = typer.Option(
        None, "--data-source-id", help="Filter by data source ID."
    ),
    filter_entries: list[str] | None = typer.Option(None, "--filter", help=LIST_FILTER_OPTION_HELP),
    show_filters: bool = typer.Option(
        False, "--show-filters", help="Show the filters supported by this list command and exit."
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    List data node storages visible to the authenticated user.

    Uses SDK client `TimeIndexMetaTable.filter()` as the single source of truth.

    Parameters
    ----------
    timeout:
        Request timeout in seconds.

    Examples
    --------
    ```bash
    mainsequence data-node list
    mainsequence data_node list
    mainsequence data-node list --filter namespace=pytest_alice
    mainsequence data-node list --data-source-id 2
    mainsequence data-node list --timeout 60
    ```
    """
    _data_node_storage_list_impl(
        timeout=timeout,
        filter_entries=filter_entries,
        show_filters=show_filters,
        data_source_id=data_source_id,
    )


@data_node_storage_group.command("detail")
def data_node_storage_detail_cmd(
    storage_uid: str = typer.Argument(..., help="Data node storage UID."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Show one data node storage and render its configuration in the terminal.

    The configuration view includes the server-derived `storage_layout` and
    `physical_index_plan` when the backend exposes them on the source table
    configuration.

    Uses SDK client `TimeIndexMetaTable.get()` as the single source of truth.

    Parameters
    ----------
    storage_uid:
        Data node storage UID.
    timeout:
        Request timeout in seconds.

    Examples
    --------
    ```bash
    mainsequence data-node detail <DATA_NODE_STORAGE_UID>
    mainsequence data_node detail <DATA_NODE_STORAGE_UID>
    mainsequence data-node detail <DATA_NODE_STORAGE_UID> --timeout 60
    ```
    """
    _data_node_storage_detail_impl(storage_uid=storage_uid, timeout=timeout)


@data_node_storage_group.command("run_query")
def data_node_storage_run_query_cmd(
    storage_uid: str = typer.Argument(..., help="Data node storage UID."),
    sql: str = typer.Argument(..., help="Raw SQL query to run."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Run a raw SQL query against one data node storage.
    """
    _data_node_storage_run_query_impl(storage_uid=storage_uid, sql=sql, timeout=timeout)


@data_node_storage_group.command("refresh-search-index")
def data_node_storage_refresh_search_index_cmd(
    storage_uid: str = typer.Argument(..., help="Data node storage UID."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Refresh the semantic search index for one data node storage.

    Uses SDK client `TimeIndexMetaTable.refresh_table_search_index()` as the single source of truth.

    Examples
    --------
    ```bash
    mainsequence data-node refresh-search-index <DATA_NODE_STORAGE_UID>
    mainsequence data_node refresh-search-index <DATA_NODE_STORAGE_UID>
    mainsequence data-node refresh-search-index <DATA_NODE_STORAGE_UID> --timeout 60
    ```
    """
    _require_login()

    try:
        payload = refresh_data_node_storage_search_index(storage_uid, timeout=timeout)
    except ApiError as e:
        error(f"Data node search index refresh failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(payload):
        return

    success(f"Data node search index refresh requested: uid={storage_uid}")
    print_kv(
        "Data Node Search Index Refresh",
        [(str(k), _format_json_value(v)) for k, v in payload.items()],
    )


@data_node_storage_group.command("refresh_search_index", hidden=True)
def data_node_storage_refresh_search_index_alias_cmd(
    storage_uid: str = typer.Argument(..., help="Data node storage UID."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Backward-compatible alias for `mainsequence data-node refresh-search-index`.
    """
    data_node_storage_refresh_search_index_cmd(storage_uid=storage_uid, timeout=timeout)


@data_node_storage_group.command("search")
def data_node_storage_search_cmd(
    q: str = typer.Argument(
        ..., help="Natural-language query to match against data node descriptions."
    ),
    mode: str = typer.Option(
        "description",
        "--mode",
        help=(
            "Search scope. Default is semantic description discovery. "
            "Use column only for schema-name lookup, or both when explicitly needed."
        ),
    ),
    data_source_id: int | None = typer.Option(
        None, "--data-source-id", help="Filter by data source ID."
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
    Search data node storages through MetaTable metadata.

    Default search uses `TimeIndexMetaTable.description_search()`, backed by
    `/orm/api/ts_manager/meta_table/description-search/`. Column mode is a
    separate schema lookup path and filters narrow results; they are not the
    semantic discovery path itself.

    Examples
    --------
    ```bash
    mainsequence data_node search "close price"
    mainsequence data-node search "node weights" --data-source-id 2
    mainsequence data-node search "close" --mode column
    mainsequence data-node search "node weights" --q-embedding 0.1,0.2,0.3
    ```
    """
    normalized_mode = (mode or "").strip().lower()
    if normalized_mode not in {"both", "description", "column"}:
        error("Invalid --mode. Use one of: both, description, column.")
        raise typer.Exit(1)

    parsed_embedding = _parse_cli_embedding(q_embedding)
    filters = _resolve_cli_list_filters(
        model_ref=DATA_NODE_STORAGE_MODEL_REF,
        filter_entries=filter_entries,
        show_filters=show_filters,
        command_label="Data Node Search",
    )
    filters = _merge_cli_filter_alias(
        filters,
        filter_key="data_source__id",
        value=data_source_id,
        option_name="data-source-id",
    )
    _require_login()

    total_matches = 0
    description_payload = None
    column_payload = None

    if normalized_mode in {"both", "description"}:
        try:
            description_payload = data_node_storage_description_search(
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
            error(f"Data Node Search failed: {e}")
            raise typer.Exit(1) from e
        storages, _ = _unpack_data_node_storage_search_response(description_payload)
        total_matches += len(storages)

    if normalized_mode in {"both", "column"}:
        try:
            column_payload = data_node_storage_column_search(q, filters=filters)
        except ApiError as e:
            error(f"Data Node Search failed: {e}")
            raise typer.Exit(1) from e
        storages, _ = _unpack_data_node_storage_search_response(column_payload)
        total_matches += len(storages)

    if _emit_json(
        {
            "query": q,
            "mode": normalized_mode,
            "description": description_payload
            if normalized_mode in {"both", "description"}
            else None,
            "column": column_payload if normalized_mode in {"both", "column"} else None,
            "total_matches": total_matches,
        }
    ):
        return

    if normalized_mode in {"both", "description"} and description_payload is not None:
        _print_data_node_storage_search_section(
            title="Description Matches",
            q=q,
            payload=description_payload,
        )

    if normalized_mode in {"both", "column"} and column_payload is not None:
        _print_data_node_storage_search_section(
            title="Column Matches",
            q=q,
            payload=column_payload,
        )

    info(f'Total search matches for "{q}": {total_matches}')


@data_node_storage_group.command("description-search", hidden=True)
def data_node_storage_description_search_cmd(
    q: str = typer.Argument(
        ..., help="Natural-language query to match against data node descriptions."
    ),
    data_source_id: int | None = typer.Option(
        None, "--data-source-id", help="Filter by data source ID."
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
        model_ref=DATA_NODE_STORAGE_MODEL_REF,
        filter_entries=filter_entries,
        show_filters=show_filters,
        command_label="Data Node Description Search",
    )
    filters = _merge_cli_filter_alias(
        filters,
        filter_key="data_source__id",
        value=data_source_id,
        option_name="data-source-id",
    )
    _require_login()
    try:
        payload = data_node_storage_description_search(
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
        error(f"Data Node Description Search failed: {e}")
        raise typer.Exit(1) from e
    if _emit_json(payload):
        return
    _print_data_node_storage_search_section(
        title="Description Matches",
        q=q,
        payload=payload,
    )


@data_node_storage_group.command("column-search", hidden=True)
def data_node_storage_column_search_cmd(
    q: str = typer.Argument(..., help="Column name or term to search in data node columns."),
    data_source_id: int | None = typer.Option(
        None, "--data-source-id", help="Filter by data source ID."
    ),
    filter_entries: list[str] | None = typer.Option(None, "--filter", help=LIST_FILTER_OPTION_HELP),
    show_filters: bool = typer.Option(
        False,
        "--show-filters",
        help="Show structured filters that can narrow this column lookup and exit.",
    ),
):
    """
    Search data node storages by column metadata.

    Uses SDK client `TimeIndexMetaTable.column_search()` as the single source of truth.

    Examples
    --------
    ```bash
    mainsequence data-node column-search weight
    mainsequence data-node column-search close --filter physical_table_name__contains=weights
    ```
    """
    filters = _resolve_cli_list_filters(
        model_ref=DATA_NODE_STORAGE_MODEL_REF,
        filter_entries=filter_entries,
        show_filters=show_filters,
        command_label="Data Node Column Search",
    )
    filters = _merge_cli_filter_alias(
        filters,
        filter_key="data_source__id",
        value=data_source_id,
        option_name="data-source-id",
    )
    _require_login()
    try:
        payload = data_node_storage_column_search(q, filters=filters)
    except ApiError as e:
        error(f"Data Node Column Search failed: {e}")
        raise typer.Exit(1) from e
    if _emit_json(payload):
        return
    _print_data_node_storage_search_section(
        title="Column Matches",
        q=q,
        payload=payload,
    )


@data_node_storage_group.command("delete")
def data_node_storage_delete_cmd(
    storage_uid: str = typer.Argument(..., help="Data node storage UID."),
    full_delete_selected: bool = typer.Option(
        False,
        "--full-delete-selected/--no-full-delete-selected",
        help="Fully delete the selected DataNode instance.",
    ),
    full_delete_downstream_tables: bool = typer.Option(
        False,
        "--full-delete-downstream-tables/--no-full-delete-downstream-tables",
        help="Delete downstream tables and dependencies starting from the selected metadata instance.",
    ),
    delete_with_no_table: bool = typer.Option(
        False,
        "--delete-with-no-table/--no-delete-with-no-table",
        help="Scan DataNode rows and fully delete records whose backing DB table does not exist.",
    ),
    override_protection: bool = typer.Option(
        False,
        "--override-protection/--no-override-protection",
        help="Bypass protect_from_deletion. ORG_ADMIN only. Used with full_delete_selected=true.",
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Delete one data node storage using the SDK client `TimeIndexMetaTable.delete()` path.

    The command always requires typed verification before the delete call is executed.

    Examples
    --------
    ```bash
    mainsequence data-node delete <DATA_NODE_STORAGE_UID>
    mainsequence data-node delete <DATA_NODE_STORAGE_UID> --full-delete-selected
    mainsequence data-node delete <DATA_NODE_STORAGE_UID> --full-delete-selected --override-protection
    ```
    """
    _data_node_storage_delete_impl(
        storage_uid=storage_uid,
        full_delete_selected=full_delete_selected,
        full_delete_downstream_tables=full_delete_downstream_tables,
        delete_with_no_table=delete_with_no_table,
        override_protection=override_protection,
        timeout=timeout,
    )


@data_node_storage_group.command("can_view")
def data_node_storage_can_view_cmd(
    storage_uid: str = typer.Argument(..., help="Data node storage UID."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    List users who can view one data node storage.

    Uses the SDK `ShareableObjectMixin.can_view()` path through the `TimeIndexMetaTable` model.

    Examples
    --------
    ```bash
    mainsequence data-node can_view <DATA_NODE_STORAGE_UID>
    mainsequence data_node can_view <DATA_NODE_STORAGE_UID>
    ```
    """
    _shareable_user_list_impl(
        fetch_fn=list_data_node_storage_users_can_view,
        object_label="Data Node",
        access_label="view",
        object_id=storage_uid,
        timeout=timeout,
    )


@data_node_storage_group.command("can_edit")
def data_node_storage_can_edit_cmd(
    storage_uid: str = typer.Argument(..., help="Data node storage UID."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    List users who can edit one data node storage.

    Uses the SDK `ShareableObjectMixin.can_edit()` path through the `TimeIndexMetaTable` model.

    Examples
    --------
    ```bash
    mainsequence data-node can_edit <DATA_NODE_STORAGE_UID>
    mainsequence data_node can_edit <DATA_NODE_STORAGE_UID>
    ```
    """
    _shareable_user_list_impl(
        fetch_fn=list_data_node_storage_users_can_edit,
        object_label="Data Node",
        access_label="edit",
        object_id=storage_uid,
        timeout=timeout,
    )


@data_node_storage_group.command("add-label")
def data_node_storage_add_label_cmd(
    storage_uid: str = typer.Argument(..., help="Data node storage UID."),
    labels: list[str] | None = typer.Option(
        None,
        "--label",
        help="Organizational label to add. Repeatable or comma-separated.",
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Add one or more organizational labels to a data node storage.

    Labels are helpers for grouping and discovery only. They do not affect runtime behavior or functionality.
    """
    _labelable_object_labels_update_impl(
        action_fn=add_data_node_storage_labels,
        object_label="Data Node",
        action_label="add-label",
        object_id=storage_uid,
        labels=labels,
        timeout=timeout,
    )


@data_node_storage_group.command("add_label", hidden=True)
def data_node_storage_add_label_alias_cmd(
    storage_uid: str = typer.Argument(..., help="Data node storage UID."),
    labels: list[str] | None = typer.Option(None, "--label", help="Organizational label to add."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """Backward-compatible alias for `mainsequence data-node add-label`."""
    data_node_storage_add_label_cmd(storage_uid=storage_uid, labels=labels, timeout=timeout)


@data_node_storage_group.command("remove-label")
def data_node_storage_remove_label_cmd(
    storage_uid: str = typer.Argument(..., help="Data node storage UID."),
    labels: list[str] | None = typer.Option(
        None,
        "--label",
        help="Organizational label to remove. Repeatable or comma-separated.",
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Remove one or more organizational labels from a data node storage.

    Labels are helpers for grouping and discovery only. They do not affect runtime behavior or functionality.
    """
    _labelable_object_labels_update_impl(
        action_fn=remove_data_node_storage_labels,
        object_label="Data Node",
        action_label="remove-label",
        object_id=storage_uid,
        labels=labels,
        timeout=timeout,
    )


@data_node_storage_group.command("remove_label", hidden=True)
def data_node_storage_remove_label_alias_cmd(
    storage_uid: str = typer.Argument(..., help="Data node storage UID."),
    labels: list[str] | None = typer.Option(
        None, "--label", help="Organizational label to remove."
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """Backward-compatible alias for `mainsequence data-node remove-label`."""
    data_node_storage_remove_label_cmd(storage_uid=storage_uid, labels=labels, timeout=timeout)


@data_node_storage_group.command("add_to_view")
def data_node_storage_add_to_view_cmd(
    storage_uid: str = typer.Argument(..., help="Data node storage UID."),
    user_id: int = typer.Argument(..., help="User ID to grant view access."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Grant explicit view access to one user for one data node storage.

    Examples
    --------
    ```bash
    mainsequence data-node add_to_view <DATA_NODE_STORAGE_UID> 7
    mainsequence data_node add_to_view <DATA_NODE_STORAGE_UID> 7
    ```
    """
    _shareable_user_access_update_impl(
        action_fn=add_data_node_storage_user_to_view,
        object_label="Data Node",
        action_label="add_to_view",
        object_id=storage_uid,
        user_id=user_id,
        timeout=timeout,
    )


@data_node_storage_group.command("add_to_edit")
def data_node_storage_add_to_edit_cmd(
    storage_uid: str = typer.Argument(..., help="Data node storage UID."),
    user_id: int = typer.Argument(..., help="User ID to grant edit access."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Grant explicit edit access to one user for one data node storage.

    Examples
    --------
    ```bash
    mainsequence data-node add_to_edit <DATA_NODE_STORAGE_UID> 7
    mainsequence data_node add_to_edit <DATA_NODE_STORAGE_UID> 7
    ```
    """
    _shareable_user_access_update_impl(
        action_fn=add_data_node_storage_user_to_edit,
        object_label="Data Node",
        action_label="add_to_edit",
        object_id=storage_uid,
        user_id=user_id,
        timeout=timeout,
    )


@data_node_storage_group.command("remove_from_view")
def data_node_storage_remove_from_view_cmd(
    storage_uid: str = typer.Argument(..., help="Data node storage UID."),
    user_id: int = typer.Argument(..., help="User ID to remove explicit view access from."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Remove explicit view access from one user for one data node storage.

    Examples
    --------
    ```bash
    mainsequence data-node remove_from_view <DATA_NODE_STORAGE_UID> 7
    mainsequence data_node remove_from_view <DATA_NODE_STORAGE_UID> 7
    ```
    """
    _shareable_user_access_update_impl(
        action_fn=remove_data_node_storage_user_from_view,
        object_label="Data Node",
        action_label="remove_from_view",
        object_id=storage_uid,
        user_id=user_id,
        timeout=timeout,
    )


@data_node_storage_group.command("remove_from_edit")
def data_node_storage_remove_from_edit_cmd(
    storage_uid: str = typer.Argument(..., help="Data node storage UID."),
    user_id: int = typer.Argument(..., help="User ID to remove explicit edit access from."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Remove explicit edit access from one user for one data node storage.

    Examples
    --------
    ```bash
    mainsequence data-node remove_from_edit <DATA_NODE_STORAGE_UID> 7
    mainsequence data_node remove_from_edit <DATA_NODE_STORAGE_UID> 7
    ```
    """
    _shareable_user_access_update_impl(
        action_fn=remove_data_node_storage_user_from_edit,
        object_label="Data Node",
        action_label="remove_from_edit",
        object_id=storage_uid,
        user_id=user_id,
        timeout=timeout,
    )


@data_node_storage_group.command("add_team_to_view")
def data_node_storage_add_team_to_view_cmd(
    storage_uid: str = typer.Argument(..., help="Data node storage UID."),
    team_id: int = typer.Argument(..., help="Team ID to grant view access."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_team_access_update_impl(
        action_fn=add_data_node_storage_team_to_view,
        object_label="Data Node",
        action_label="add_team_to_view",
        object_id=storage_uid,
        team_id=team_id,
        timeout=timeout,
    )


@data_node_storage_group.command("add_team_to_edit")
def data_node_storage_add_team_to_edit_cmd(
    storage_uid: str = typer.Argument(..., help="Data node storage UID."),
    team_id: int = typer.Argument(..., help="Team ID to grant edit access."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_team_access_update_impl(
        action_fn=add_data_node_storage_team_to_edit,
        object_label="Data Node",
        action_label="add_team_to_edit",
        object_id=storage_uid,
        team_id=team_id,
        timeout=timeout,
    )


@data_node_storage_group.command("remove_team_from_view")
def data_node_storage_remove_team_from_view_cmd(
    storage_uid: str = typer.Argument(..., help="Data node storage UID."),
    team_id: int = typer.Argument(..., help="Team ID to remove explicit view access from."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_team_access_update_impl(
        action_fn=remove_data_node_storage_team_from_view,
        object_label="Data Node",
        action_label="remove_team_from_view",
        object_id=storage_uid,
        team_id=team_id,
        timeout=timeout,
    )


@data_node_storage_group.command("remove_team_from_edit")
def data_node_storage_remove_team_from_edit_cmd(
    storage_uid: str = typer.Argument(..., help="Data node storage UID."),
    team_id: int = typer.Argument(..., help="Team ID to remove explicit edit access from."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_team_access_update_impl(
        action_fn=remove_data_node_storage_team_from_edit,
        object_label="Data Node",
        action_label="remove_team_from_edit",
        object_id=storage_uid,
        team_id=team_id,
        timeout=timeout,
    )


# ---------- project group ----------


@project_list_group.callback(invoke_without_command=True)
def project_list(
    ctx: typer.Context,
    filter_entries: list[str] | None = typer.Option(None, "--filter", help=LIST_FILTER_OPTION_HELP),
    show_filters: bool = typer.Option(
        False, "--show-filters", help="Show the filters supported by this list command and exit."
    ),
):
    """
    List projects visible to the authenticated user.

    The output includes project identity, initialization state, and local mapping status.

    Examples
    --------
    ```bash
    mainsequence project list
    ```
    """
    if ctx.invoked_subcommand is not None:
        return

    _resolve_cli_list_filters(
        model_ref=None,
        filter_entries=filter_entries,
        show_filters=show_filters,
        command_label="Projects",
    )

    _require_login()
    cfg_obj = cfg.get_config()
    base = cfg_obj["mainsequence_path"]
    org_slug = _org_slug_from_profile()
    items = get_projects()
    if _emit_json(items):
        return
    typer.echo(_render_projects_table(items, base, org_slug))


def _print_project_data_node_updates(
    project_id: str | None = typer.Argument(None, help="Project UID"),
    filter_entries: list[str] | None = None,
    show_filters: bool = False,
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
) -> None:
    """
    List data node updates for a project.

    Uses SDK client `Project.get_data_nodes_updates()` as the single source of truth
    for payload parsing and shape handling.

    Parameters
    ----------
    project_id:
        Platform project UID. If omitted, resolve it from `MAIN_SEQUENCE_PROJECT_UID` in `./.env`.
    timeout:
        Optional request timeout in seconds.

    Examples
    --------
    ```bash
    mainsequence project data-node-updates list
    mainsequence project data-node-updates list project-uid-123
    mainsequence project data-node-updates list project-uid-123 --timeout 60
    ```
    """
    _resolve_cli_list_filters(
        model_ref=None,
        filter_entries=filter_entries,
        show_filters=show_filters,
        command_label="Project Data Node Updates",
        reserved_filter_descriptions={"project_id": "always set from PROJECT_UID or local .env"},
    )

    if project_id is None:
        project_id = _resolve_project_id_from_local_env()

    _require_login()
    try:
        updates = get_project_data_node_updates(project_id, timeout=timeout)
    except NotLoggedIn as e:
        error("Not logged in. Run: mainsequence login")
        raise typer.Exit(1) from e
    except ApiError as e:
        error(str(e))
        raise typer.Exit(1) from e

    if _emit_json(updates):
        return

    if not updates:
        info("No data node updates found.")
        return

    rows: list[list[str]] = []
    for u in updates:
        storage = u.get("data_node_storage")
        if isinstance(storage, dict):
            storage_value = (
                storage.get("physical_table_name") or storage.get("uid") or storage.get("id") or "-"
            )
        else:
            storage_value = storage if storage is not None else "-"

        details = u.get("update_details")
        if isinstance(details, dict):
            details_id = details.get("id") or "-"
        else:
            details_id = details if details is not None else "-"

        rows.append(
            [
                str(u.get("id") or "-"),
                str(u.get("update_hash") or "-"),
                str(storage_value),
                str(details_id),
            ]
        )

    print_table(
        "Project Data Node Updates",
        ["ID", "Update Hash", "Data Node Storage", "Update Details"],
        rows,
    )
    info(f"Total updates: {len(rows)}")


@project_data_node_updates_group.command("list")
def project_data_node_updates_list_cmd(
    project_id: str | None = typer.Argument(None, help="Project UID"),
    filter_entries: list[str] | None = typer.Option(None, "--filter", help=LIST_FILTER_OPTION_HELP),
    show_filters: bool = typer.Option(
        False, "--show-filters", help="Show the filters supported by this list command and exit."
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    List data node updates for a project.

    Examples
    --------
    ```bash
    mainsequence project data-node-updates list
    mainsequence project data-node-updates list project-uid-123
    mainsequence project data-node-updates list project-uid-123 --timeout 60
    ```
    """
    _print_project_data_node_updates(
        project_id=project_id,
        filter_entries=filter_entries,
        show_filters=show_filters,
        timeout=timeout,
    )


@project_list_group.command("data_nodes_updates", hidden=True)
def project_list_data_nodes_updates_cmd(
    project_id: str | None = typer.Argument(None, help="Project UID"),
    filter_entries: list[str] | None = typer.Option(None, "--filter", help=LIST_FILTER_OPTION_HELP),
    show_filters: bool = typer.Option(
        False, "--show-filters", help="Show the filters supported by this list command and exit."
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Backward-compatible alias for `mainsequence project data-node-updates list`.
    """
    _print_project_data_node_updates(
        project_id=project_id,
        filter_entries=filter_entries,
        show_filters=show_filters,
        timeout=timeout,
    )


@project.command("get-data-node-updates", hidden=True)
def project_get_data_node_updates_cmd(
    project_id: str | None = typer.Argument(None, help="Project UID"),
    filter_entries: list[str] | None = typer.Option(None, "--filter", help=LIST_FILTER_OPTION_HELP),
    show_filters: bool = typer.Option(
        False, "--show-filters", help="Show the filters supported by this list command and exit."
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Backward-compatible alias for `mainsequence project data-node-updates list`.
    """
    _print_project_data_node_updates(
        project_id=project_id,
        filter_entries=filter_entries,
        show_filters=show_filters,
        timeout=timeout,
    )


@project.command("validate-name")
def project_validate_name_cmd(
    project_name: str = typer.Argument(..., help="Project name to validate."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Validate whether a project name is available for creation on the platform.

    Examples
    --------
    ```bash
    mainsequence project validate-name "Rates Platform"
    mainsequence project validate-name tutorial-project --timeout 60
    ```
    """
    _require_login()

    normalized_project_name = (project_name or "").strip()
    if not normalized_project_name:
        error("Project name is required.")
        raise typer.Exit(1)

    try:
        payload = validate_project_name(project_name=normalized_project_name, timeout=timeout)
    except ApiError as e:
        error(f"Project name validation failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(payload):
        return

    normalized = payload.get("normalized") or {}
    print_kv(
        "Project Name Validation",
        [
            ("Project Name", str(payload.get("project_name") or normalized_project_name)),
            ("Available", "yes" if payload.get("available") else "no"),
            ("Reason", str(payload.get("reason") or "-")),
            ("Slugified Project Name", str(normalized.get("slugified_project_name") or "-")),
            ("Project Library Name", str(normalized.get("project_library_name") or "-")),
        ],
    )

    suggestions = [str(item) for item in list(payload.get("suggestions") or []) if item is not None]
    if suggestions:
        print_table("Suggested Project Names", ["Project Name"], [[item] for item in suggestions])

    if payload.get("available"):
        success(f"Project name is available: {normalized_project_name}")
        return

    warn(f"Project name is not available: {normalized_project_name}")
    raise typer.Exit(1)


@project.command("search")
def project_search_cmd(
    q: str = typer.Argument(..., help="Project search query. Minimum 3 characters."),
    limit: int = typer.Option(
        20, "--limit", min=1, max=100, help="Maximum number of matches to return."
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Search projects visible to the authenticated user.

    Uses SDK client `Project.quick_search()` as the single source of truth.

    Examples
    --------
    ```bash
    mainsequence project search "tutorial"
    mainsequence project search "rates" --limit 10
    mainsequence project search "161"
    ```
    """
    _require_login()

    normalized_query = (q or "").strip()
    if len(normalized_query) < 3:
        error("Project search failed: Query must contain at least 3 characters.")
        raise typer.Exit(1)

    try:
        projects = search_projects(normalized_query, limit=limit, timeout=timeout)
    except ApiError as e:
        error(f"Project search failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(projects):
        return

    if projects:
        print_table(
            "Project Search Results",
            ["UID", "Project Name", "Repository Branch", "Cluster ID"],
            [
                [
                    _project_identity_value(project) or "-",
                    str(project.get("project_name") or "-"),
                    str(project.get("repository_branch") or "-"),
                    str(project.get("cluster_id") or "-"),
                ]
                for project in projects
            ],
        )
    else:
        info("No projects matched the search.")
    info(f'Project search matches for "{normalized_query}": {len(projects)}')


@project.command("validate_name", hidden=True)
def project_validate_name_alias_cmd(
    project_name: str = typer.Argument(..., help="Project name to validate."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Backward-compatible alias for `mainsequence project validate-name`.
    """
    project_validate_name_cmd(project_name=project_name, timeout=timeout)


@project.command("create")
def project_create_cmd(
    project_name: str | None = typer.Argument(None, help="Project name"),
    data_source_id: int | None = typer.Option(
        None, "--data-source-id", help="Dynamic table data source ID"
    ),
    default_base_image_id: int | None = typer.Option(
        None, "--default-base-image-id", help="Default base image ID"
    ),
    github_org_id: int | None = typer.Option(
        None, "--github-org-id", "--organization-id", help="GitHub organization ID"
    ),
    branch: str | None = typer.Option(None, "--branch", help="Repository branch (default: main)"),
    env: list[str] | None = typer.Option(
        None,
        "--env",
        help="Environment variable entry in KEY=VALUE form. Repeatable or comma-separated.",
    ),
):
    """
    Create a project on the platform.

    If required values are omitted, the command prompts interactively and applies defaults.
    After creation, it polls project status until `is_initialized=true`.

    Parameters
    ----------
    project_name:
        Project name. If omitted, prompt is shown.
    data_source_id:
        Dynamic table data source ID.
    default_base_image_id:
        Default base image ID.
    github_org_id:
        GitHub organization ID.
    branch:
        Repository branch (default: `main`).
    env:
        Environment variable entries in `KEY=VALUE` format.

    Examples
    --------
    ```bash
    mainsequence project create
    mainsequence project create tutorial-project
    mainsequence project create tutorial-project --branch main --env FOO=bar --env BAZ=qux
    ```
    """
    _require_login()

    try:
        if not project_name:
            project_name = typer.prompt("Project name").strip()
        project_name = (project_name or "").strip()
        if not project_name:
            error("Project name is required.")
            raise typer.Exit(1)

        name_validation = validate_project_name(project_name=project_name)
        if not name_validation.get("available"):
            normalized = name_validation.get("normalized") or {}
            reason = str(name_validation.get("reason") or "Project name is not available.")
            error(reason)
            print_kv(
                "Project Name Validation",
                [
                    ("Project Name", str(name_validation.get("project_name") or project_name)),
                    ("Available", "no"),
                    ("Reason", reason),
                    (
                        "Slugified Project Name",
                        str(normalized.get("slugified_project_name") or "-"),
                    ),
                    ("Project Library Name", str(normalized.get("project_library_name") or "-")),
                ],
            )
            suggestions = [
                str(item)
                for item in list(name_validation.get("suggestions") or [])
                if item is not None
            ]
            if suggestions:
                print_table(
                    "Suggested Project Names", ["Project Name"], [[item] for item in suggestions]
                )
            raise typer.Exit(1)

        if data_source_id is None:
            ds_items = list_dynamic_table_data_sources(status="AVAILABLE")
            ds_rows: list[list[str]] = []
            for item in ds_items:
                rr = item.get("related_resource") or {}
                ds_name = (
                    rr.get("display_name")
                    or item.get("related_resource_class_type")
                    or rr.get("class_type")
                    or f"data-source-{item.get('id')}"
                )
                ds_details = (
                    f"class={rr.get('class_type') or '-'}, status={rr.get('status') or '-'}"
                )
                ds_rows.append([str(item.get("id", "")), str(ds_name), str(ds_details)])
            data_source_id = _prompt_select_id(
                title="Available Data Sources",
                prompt_label="Data source id",
                items=ds_items,
                rows=ds_rows,
            )

        if default_base_image_id is None:
            img_items = list_project_base_images()
            img_rows: list[list[str]] = []
            for item in img_items:
                name = item.get("title") or f"image-{item.get('id')}"
                details = item.get("description") or item.get("latest_digest") or "-"
                img_rows.append([str(item.get("id", "")), str(name), str(details)])
            default_base_image_id = _prompt_select_id(
                title="Available Base Images",
                prompt_label="Default base image id",
                items=img_items,
                rows=img_rows,
            )

        if github_org_id is None:
            org_items = list_github_organizations()
            if org_items:
                org_rows: list[list[str]] = []
                for item in org_items:
                    name = item.get("display_name") or item.get("login") or f"org-{item.get('id')}"
                    details = item.get("login") or "-"
                    org_rows.append([str(item.get("id", "")), str(name), str(details)])
                github_org_id = _prompt_select_id(
                    title="Available GitHub Organizations",
                    prompt_label="GitHub organization id",
                    items=org_items,
                    rows=org_rows,
                )
            else:
                warn(
                    "No GitHub organizations available. Project will be created without github_org_id."
                )

        branch = (
            (branch or "").strip()
            or typer.prompt("Repository branch", default="main").strip()
            or "main"
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

        created = create_project(
            project_name=project_name,
            data_source_id=data_source_id,
            default_base_image_id=default_base_image_id,
            github_org_id=github_org_id,
            repository_branch=branch,
            env_vars=env_vars,
        )
    except ApiError as e:
        error(f"Project creation failed: {e}")
        raise typer.Exit(1) from e
    except RuntimeError as e:
        error(str(e))
        raise typer.Exit(1) from e

    project_uid = _project_identity_value(created)
    project_poll_ref = project_uid or str(created.get("id") or "").strip() or None

    if _emit_json(created):
        return

    success(
        f"Project created: {created.get('project_name') or project_name} (uid={project_uid or '-'})"
    )

    # A freshly created project can take several minutes to initialize on backend.
    # Keep polling until API reports is_initialized=True.
    if project_poll_ref and created.get("is_initialized") is False:
        info("Project is still initializing. Waiting until is_initialized=true (poll every 30s).")
        attempt = 0
        try:
            while True:
                attempt += 1
                with status(f"Project not ready yet (attempt {attempt}). Next check in 30s..."):
                    time.sleep(30)
                try:
                    latest = get_project(project_poll_ref)
                except ApiError as e:
                    warn(f"Project status poll failed (attempt {attempt}): {e}")
                    continue

                created = latest
                if created.get("is_initialized") is True:
                    success("Project is initialized and ready.")
                    break

                info("Project still initializing. Continuing to poll...")
        except KeyboardInterrupt:
            warn("Stopped waiting for project initialization. Project may still be initializing.")

    print_kv(
        "Project",
        [
            ("UID", project_uid or "-"),
            ("Project Name", str(created.get("project_name") or project_name)),
            ("Git SSH URL", str(created.get("git_ssh_url") or "-")),
            ("Branch", branch),
        ],
    )
    if project_uid:
        info(f"Next: mainsequence project set-up-locally {project_uid}")


@project.command("delete")
def project_delete_remote_cmd(
    project_id: str = typer.Argument(..., help="Project UID"),
    delete_repositories: bool = typer.Option(
        False,
        "--delete-repositories/--no-delete-repositories",
        help="Also delete linked repositories in the backend workflow.",
    ),
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not prompt for confirmation"),
):
    """
    Delete a project from the platform (remote deletion).

    This does not delete local files unless you run `project delete-local`.

    Parameters
    ----------
    project_id:
        Platform project UID.
    delete_repositories:
        Also delete linked repositories on backend workflow.
    yes:
        Skip interactive confirmation.

    Examples
    --------
    ```bash
    mainsequence project delete project-uid-123
    mainsequence project delete project-uid-123 --yes
    mainsequence project delete project-uid-123 --delete-repositories --yes
    ```
    """
    _require_login()

    project_name = f"project-{project_id}"
    project_uid = str(project_id)
    try:
        found = resolve_project(project_id)
        if found and found.get("project_name"):
            project_name = str(found.get("project_name"))
            project_uid = _project_identity_value(found) or project_uid
    except Exception:
        # Best-effort metadata lookup only.
        pass

    if not yes:
        warning = (
            f"This will permanently delete project '{project_name}' (uid={project_uid}) from the platform.\n"
            "This action cannot be undone."
        )
        if delete_repositories:
            warning += "\nLinked repositories will also be deleted."
        if not typer.confirm(f"{warning}\n\nContinue?", default=False):
            info("Cancelled.")
            raise typer.Exit(0)

    try:
        resp = delete_project(project_id, delete_repositories=delete_repositories)
    except NotLoggedIn as e:
        error("Not logged in. Run: mainsequence login")
        raise typer.Exit(1) from e
    except ApiError as e:
        error(f"Project deletion failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(resp):
        return

    success(f"Project deleted: {project_name} (uid={project_uid})")
    if isinstance(resp, dict) and resp:
        detail = resp.get("detail") or resp.get("message")
        if detail:
            info(str(detail))


@project.command("can_view")
def project_can_view_cmd(
    project_id: str = typer.Argument(..., help="Project UID."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    List users who can view one project.

    Uses the SDK `ShareableObjectMixin.users_can_view()` path through the `Project` model.

    Examples
    --------
    ```bash
    mainsequence project can_view 42
    ```
    """
    _shareable_user_list_impl(
        fetch_fn=list_project_users_can_view,
        object_label="Project",
        access_label="view",
        object_id=project_id,
        timeout=timeout,
    )


@project.command("can_edit")
def project_can_edit_cmd(
    project_id: str = typer.Argument(..., help="Project UID."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    List users who can edit one project.

    Uses the SDK `ShareableObjectMixin.users_can_edit()` path through the `Project` model.

    Examples
    --------
    ```bash
    mainsequence project can_edit 42
    ```
    """
    _shareable_user_list_impl(
        fetch_fn=list_project_users_can_edit,
        object_label="Project",
        access_label="edit",
        object_id=project_id,
        timeout=timeout,
    )


@project.command("add-label")
def project_add_label_cmd(
    project_id: str = typer.Argument(..., help="Project UID."),
    labels: list[str] | None = typer.Option(
        None,
        "--label",
        help="Organizational label to add. Repeatable or comma-separated.",
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Add one or more organizational labels to a project.

    Labels are helpers for grouping and discovery only. They do not affect runtime behavior or functionality.
    """
    _labelable_object_labels_update_impl(
        action_fn=add_project_labels,
        object_label="Project",
        action_label="add-label",
        object_id=project_id,
        labels=labels,
        timeout=timeout,
    )


@project.command("add_label", hidden=True)
def project_add_label_alias_cmd(
    project_id: str = typer.Argument(..., help="Project UID."),
    labels: list[str] | None = typer.Option(None, "--label", help="Organizational label to add."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """Backward-compatible alias for `mainsequence project add-label`."""
    project_add_label_cmd(project_id=project_id, labels=labels, timeout=timeout)


@project.command("remove-label")
def project_remove_label_cmd(
    project_id: str = typer.Argument(..., help="Project UID."),
    labels: list[str] | None = typer.Option(
        None,
        "--label",
        help="Organizational label to remove. Repeatable or comma-separated.",
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Remove one or more organizational labels from a project.

    Labels are helpers for grouping and discovery only. They do not affect runtime behavior or functionality.
    """
    _labelable_object_labels_update_impl(
        action_fn=remove_project_labels,
        object_label="Project",
        action_label="remove-label",
        object_id=project_id,
        labels=labels,
        timeout=timeout,
    )


@project.command("remove_label", hidden=True)
def project_remove_label_alias_cmd(
    project_id: str = typer.Argument(..., help="Project UID."),
    labels: list[str] | None = typer.Option(
        None, "--label", help="Organizational label to remove."
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """Backward-compatible alias for `mainsequence project remove-label`."""
    project_remove_label_cmd(project_id=project_id, labels=labels, timeout=timeout)


@project.command("add_to_view")
def project_add_to_view_cmd(
    project_id: str = typer.Argument(..., help="Project UID."),
    user_id: int = typer.Argument(..., help="User ID to grant view access."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Grant explicit view access to one user for one project.

    Examples
    --------
    ```bash
    mainsequence project add_to_view 42 7
    ```
    """
    _shareable_user_access_update_impl(
        action_fn=add_project_user_to_view,
        object_label="Project",
        action_label="add_to_view",
        object_id=project_id,
        user_id=user_id,
        timeout=timeout,
    )


@project.command("add_to_edit")
def project_add_to_edit_cmd(
    project_id: str = typer.Argument(..., help="Project UID."),
    user_id: int = typer.Argument(..., help="User ID to grant edit access."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Grant explicit edit access to one user for one project.

    Examples
    --------
    ```bash
    mainsequence project add_to_edit 42 7
    ```
    """
    _shareable_user_access_update_impl(
        action_fn=add_project_user_to_edit,
        object_label="Project",
        action_label="add_to_edit",
        object_id=project_id,
        user_id=user_id,
        timeout=timeout,
    )


@project.command("remove_from_view")
def project_remove_from_view_cmd(
    project_id: str = typer.Argument(..., help="Project UID."),
    user_id: int = typer.Argument(..., help="User ID to remove explicit view access from."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Remove explicit view access from one user for one project.

    Examples
    --------
    ```bash
    mainsequence project remove_from_view 42 7
    ```
    """
    _shareable_user_access_update_impl(
        action_fn=remove_project_user_from_view,
        object_label="Project",
        action_label="remove_from_view",
        object_id=project_id,
        user_id=user_id,
        timeout=timeout,
    )


@project.command("remove_from_edit")
def project_remove_from_edit_cmd(
    project_id: str = typer.Argument(..., help="Project UID."),
    user_id: int = typer.Argument(..., help="User ID to remove explicit edit access from."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Remove explicit edit access from one user for one project.

    Examples
    --------
    ```bash
    mainsequence project remove_from_edit 42 7
    ```
    """
    _shareable_user_access_update_impl(
        action_fn=remove_project_user_from_edit,
        object_label="Project",
        action_label="remove_from_edit",
        object_id=project_id,
        user_id=user_id,
        timeout=timeout,
    )


@project.command("add_team_to_view")
def project_add_team_to_view_cmd(
    project_id: str = typer.Argument(..., help="Project UID."),
    team_id: int = typer.Argument(..., help="Team ID to grant view access."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_team_access_update_impl(
        action_fn=add_project_team_to_view,
        object_label="Project",
        action_label="add_team_to_view",
        object_id=project_id,
        team_id=team_id,
        timeout=timeout,
    )


@project.command("add_team_to_edit")
def project_add_team_to_edit_cmd(
    project_id: str = typer.Argument(..., help="Project UID."),
    team_id: int = typer.Argument(..., help="Team ID to grant edit access."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_team_access_update_impl(
        action_fn=add_project_team_to_edit,
        object_label="Project",
        action_label="add_team_to_edit",
        object_id=project_id,
        team_id=team_id,
        timeout=timeout,
    )


@project.command("remove_team_from_view")
def project_remove_team_from_view_cmd(
    project_id: str = typer.Argument(..., help="Project UID."),
    team_id: int = typer.Argument(..., help="Team ID to remove explicit view access from."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_team_access_update_impl(
        action_fn=remove_project_team_from_view,
        object_label="Project",
        action_label="remove_team_from_view",
        object_id=project_id,
        team_id=team_id,
        timeout=timeout,
    )


@project.command("remove_team_from_edit")
def project_remove_team_from_edit_cmd(
    project_id: str = typer.Argument(..., help="Project UID."),
    team_id: int = typer.Argument(..., help="Team ID to remove explicit edit access from."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    _shareable_team_access_update_impl(
        action_fn=remove_project_team_from_edit,
        object_label="Project",
        action_label="remove_team_from_edit",
        object_id=project_id,
        team_id=team_id,
        timeout=timeout,
    )


def _project_resources_list_impl(
    project_id: str | None,
    path: str | None,
    filter_entries: list[str] | None,
    show_filters: bool,
    timeout: int | None,
) -> None:
    filters = _resolve_cli_list_filters(
        model_ref=PROJECT_RESOURCE_MODEL_REF,
        filter_entries=filter_entries,
        show_filters=show_filters,
        command_label="Project Resources",
        reserved_filter_descriptions={
            "project__id": "always set from the selected project",
            "repo_commit_sha": "always set from the upstream remote branch head commit",
        },
    )

    _require_login()

    project_dir = _resolve_project_dir(project_id, path)
    if project_id is None:
        project_id = _resolve_project_id_from_local_env(str(project_dir))

    try:
        upstream, repo_commit_sha = _get_remote_branch_head_commit(project_dir)
    except RuntimeError as e:
        error(str(e))
        raise typer.Exit(1) from e

    try:
        resources = list_project_resources(
            project_id=project_id,
            repo_commit_sha=repo_commit_sha,
            filters=filters,
            timeout=timeout,
        )
    except ApiError as e:
        error(f"Project resources fetch failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(resources):
        return

    info(f"Using repo_commit_sha={repo_commit_sha} from {upstream}.")

    rows: list[list[str]] = []
    for resource in resources:
        rows.append(
            [
                str(resource.get("id") or "-"),
                str(resource.get("name") or "-"),
                str(resource.get("resource_type") or "-"),
                str(resource.get("path") or "-"),
                str(resource.get("filesize") or "-"),
                str(resource.get("last_modified") or "-"),
            ]
        )

    if rows:
        print_table(
            "Project Resources",
            ["ID", "Name", "Type", "Path", "File Size", "Last Modified"],
            rows,
        )
    else:
        info("No project resources found.")
    info(f"Total project resources: {len(resources)}")


@project_project_resource_group.command("list")
def project_project_resource_list_cmd(
    project_id: str | None = typer.Argument(
        None, help="Project UID. Defaults to local .env when omitted."
    ),
    path: str | None = typer.Option(
        None, "--path", help="Project repository path (default: current project)"
    ),
    filter_entries: list[str] | None = typer.Option(None, "--filter", help=LIST_FILTER_OPTION_HELP),
    show_filters: bool = typer.Option(
        False, "--show-filters", help="Show the filters supported by this list command and exit."
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    List project resources for the current project at the head commit of the remote branch.

    Uses SDK client `ProjectResource.filter()` as the single source of truth and always applies
    the standard `repo_commit_sha` filter resolved from the current upstream branch head.

    Parameters
    ----------
    project_id:
        Platform project UID. Defaults to local `.env`.
    path:
        Local project path. Used when resolving project uid and remote branch head commit.
    timeout:
        Request timeout in seconds.

    Examples
    --------
    ```bash
    mainsequence project project_resource list
    mainsequence project project_resource list project-uid-123
    mainsequence project project_resource list --path .
    ```
    """
    _project_resources_list_impl(
        project_id=project_id,
        path=path,
        filter_entries=filter_entries,
        show_filters=show_filters,
        timeout=timeout,
    )


def _project_resource_release_create_impl(
    *,
    release_kind: str,
    project_id: str | None,
    resource_id: int | None,
    path: str | None,
    related_image_id: int | None,
    readme_resource_id: int | None,
    cpu_request: str | None,
    memory_request: str | None,
    gpu_request: str | None,
    gpu_type: str | None,
    spot: bool | None,
    timeout: int | None,
) -> None:
    _require_login()

    project_dir = _resolve_project_dir(project_id, path)
    if project_id is None:
        project_id = _resolve_project_id_from_local_env(str(project_dir))

    try:
        project_images = list_project_images(related_project_id=project_id, timeout=timeout)
    except ApiError as e:
        error(f"Project images fetch failed: {e}")
        raise typer.Exit(1) from e

    if not project_images:
        error("No project images are available. Create an image first.")
        raise typer.Exit(1)

    if related_image_id is None:
        image_rows: list[list[str]] = []
        for image in project_images:
            image_rows.append(
                [
                    str(image.get("id") or ""),
                    str(image.get("project_repo_hash") or "-"),
                    _format_base_image_label(image.get("base_image")),
                ]
            )
        related_image_id = _prompt_select_id(
            title="Available Project Images",
            prompt_label="Related image id",
            items=project_images,
            rows=image_rows,
        )

    selected_image = _find_image_by_id(project_images, related_image_id)
    if not selected_image:
        error(f"Related image not found: {related_image_id}")
        raise typer.Exit(1)

    repo_commit_sha = str(selected_image.get("project_repo_hash") or "").strip()
    if not repo_commit_sha:
        error("The selected image does not expose project_repo_hash.")
        raise typer.Exit(1)

    resource_type = RESOURCE_RELEASE_RESOURCE_TYPE_MAP.get(release_kind)
    if not resource_type:
        error(f"Unsupported release kind: {release_kind}")
        raise typer.Exit(1)

    try:
        resources = list_project_resources(
            project_id=project_id,
            repo_commit_sha=repo_commit_sha,
            resource_type=resource_type,
            timeout=timeout,
        )
    except ApiError as e:
        error(f"Project resources fetch failed: {e}")
        raise typer.Exit(1) from e

    if not resources:
        error(
            "No project resources match the selected image commit and release type. "
            f"Expected resource_type={resource_type!r} for release_kind={release_kind!r}."
        )
        raise typer.Exit(1)

    if resource_id is None:
        resource_rows: list[list[str]] = []
        for resource in resources:
            resource_rows.append(
                [
                    str(resource.get("id") or ""),
                    str(resource.get("name") or "-"),
                    f"{str(resource.get('resource_type') or '-')}: {str(resource.get('path') or '-')}",
                ]
            )
        resource_id = _prompt_select_id(
            title="Project Resources Matching Selected Image and Release Type",
            prompt_label="Resource id",
            items=resources,
            rows=resource_rows,
        )

    resource_ids = {
        str(resource.get("id")) for resource in resources if resource.get("id") is not None
    }
    if str(resource_id) not in resource_ids:
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
        created = create_project_resource_release(
            release_kind=release_kind,
            resource_id=resource_id,
            related_image_id=related_image_id,
            readme_resource_id=readme_resource_id,
            cpu_request=cpu_request,
            memory_request=memory_request,
            gpu_request=gpu_request,
            gpu_type=gpu_type,
            spot=spot,
            timeout=timeout,
        )
    except ApiError as e:
        error(f"Project resource release creation failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(created):
        return

    success(f"Project resource release created: id={created.get('id') or '-'}")
    print_kv(
        "Project Resource Release",
        [
            ("ID", str(created.get("id") or "-")),
            ("Release Kind", release_kind),
            ("Resource", str(created.get("resource") or resource_id)),
            (
                "Related Image",
                _format_related_image_label(created.get("related_image") or related_image_id),
            ),
            ("CPU Request", str(created.get("cpu_request") or cpu_request)),
            ("Memory Request", str(created.get("memory_request") or memory_request)),
            ("GPU Request", str(created.get("gpu_request") or gpu_request or "-")),
            ("GPU Type", str(created.get("gpu_type") or gpu_type or "-")),
            ("Spot", str(created.get("spot") if created.get("spot") is not None else spot).lower()),
        ],
    )


@project_project_resource_group.command("create_dashboard")
def project_project_resource_create_dashboard_cmd(
    project_id: str | None = typer.Argument(
        None, help="Project UID. Defaults to local .env when omitted."
    ),
    resource_id: int | None = typer.Option(None, "--resource-id", help="Project resource ID."),
    path: str | None = typer.Option(
        None, "--path", help="Project repository path (default: current project)"
    ),
    related_image_id: int | None = typer.Option(
        None, "--related-image-id", help="Project image ID."
    ),
    readme_resource_id: int | None = typer.Option(
        None, "--readme-resource-id", help="Optional README resource ID."
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
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Create a Streamlit dashboard release from a project resource.

    The command first lets the user select a project image and then filters resources so
    only resources with `repo_commit_sha == related_image.project_repo_hash` are eligible.
    """
    _project_resource_release_create_impl(
        release_kind="streamlit_dashboard",
        project_id=project_id,
        resource_id=resource_id,
        path=path,
        related_image_id=related_image_id,
        readme_resource_id=readme_resource_id,
        cpu_request=cpu_request,
        memory_request=memory_request,
        gpu_request=gpu_request,
        gpu_type=gpu_type,
        spot=spot,
        timeout=timeout,
    )


@project_project_resource_group.command("create_fastapi")
def project_project_resource_create_fastapi_cmd(
    project_id: str | None = typer.Argument(
        None, help="Project UID. Defaults to local .env when omitted."
    ),
    resource_id: int | None = typer.Option(None, "--resource-id", help="Project resource ID."),
    path: str | None = typer.Option(
        None, "--path", help="Project repository path (default: current project)"
    ),
    related_image_id: int | None = typer.Option(
        None, "--related-image-id", help="Project image ID."
    ),
    readme_resource_id: int | None = typer.Option(
        None, "--readme-resource-id", help="Optional README resource ID."
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
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Create a FastAPI release from a project resource.

    The command first lets the user select a project image and then filters resources so
    only resources with `repo_commit_sha == related_image.project_repo_hash` are eligible.
    """
    _project_resource_release_create_impl(
        release_kind="fastapi",
        project_id=project_id,
        resource_id=resource_id,
        path=path,
        related_image_id=related_image_id,
        readme_resource_id=readme_resource_id,
        cpu_request=cpu_request,
        memory_request=memory_request,
        gpu_request=gpu_request,
        gpu_type=gpu_type,
        spot=spot,
        timeout=timeout,
    )


def _project_resource_release_delete_impl(
    *,
    release_id: int,
    expected_release_kind: str,
    yes: bool,
    timeout: int | None,
) -> None:
    _require_login()

    try:
        release = get_resource_release(
            release_id=release_id,
            expected_release_kind=expected_release_kind,
            timeout=timeout,
        )
    except ApiError as e:
        error(f"Project resource release fetch failed: {e}")
        raise typer.Exit(1) from e

    release_label = RESOURCE_RELEASE_LABEL_MAP.get(
        expected_release_kind,
        f"{expected_release_kind} release",
    )
    _confirm_delete_action(
        preview_title="Project Resource Release Delete Preview",
        preview_items=_format_resource_release_delete_preview(release),
        prompt_text=f"Delete {release_label} {release_id}?",
        yes=yes,
    )

    try:
        deleted = delete_resource_release(
            release_id=release_id,
            expected_release_kind=expected_release_kind,
            timeout=timeout,
        )
    except ApiError as e:
        error(f"Project resource release deletion failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(deleted):
        return

    success(f"Project resource release deleted: id={release_id}")
    print_kv("Deleted Project Resource Release", _format_resource_release_delete_preview(deleted))


@project_project_resource_group.command("delete_dashboard")
def project_project_resource_delete_dashboard_cmd(
    release_id: int = typer.Argument(..., help="Dashboard resource release ID."),
    yes: bool = typer.Option(False, "--yes", help="Delete without confirmation."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Delete a dashboard resource release.

    Examples
    --------
    ```bash
    mainsequence project project_resource delete_dashboard 501
    mainsequence project project_resource delete_dashboard 501 --yes
    ```
    """
    _project_resource_release_delete_impl(
        release_id=release_id,
        expected_release_kind="streamlit_dashboard",
        yes=yes,
        timeout=timeout,
    )


@project_project_resource_group.command("delete_fastapi")
def project_project_resource_delete_fastapi_cmd(
    release_id: int = typer.Argument(..., help="FastAPI resource release ID."),
    yes: bool = typer.Option(False, "--yes", help="Delete without confirmation."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Delete a FastAPI resource release.

    Examples
    --------
    ```bash
    mainsequence project project_resource delete_fastapi 701
    mainsequence project project_resource delete_fastapi 701 --yes
    ```
    """
    _project_resource_release_delete_impl(
        release_id=release_id,
        expected_release_kind="fastapi",
        yes=yes,
        timeout=timeout,
    )


def _project_images_list_impl(
    project_id: str | None,
    path: str | None,
    filter_entries: list[str] | None,
    show_filters: bool,
    timeout: int | None,
) -> None:
    filters = _resolve_cli_list_filters(
        model_ref=PROJECT_IMAGE_MODEL_REF,
        filter_entries=filter_entries,
        show_filters=show_filters,
        command_label="Project Images",
        reserved_filter_descriptions={
            "related_project__id__in": "always set from the selected project",
        },
    )

    _require_login()

    if project_id is None:
        project_id = _resolve_project_id_from_local_env(path)

    try:
        images = list_project_images(
            related_project_id=project_id, filters=filters, timeout=timeout
        )
    except ApiError as e:
        error(f"Project images fetch failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(images):
        return

    rows: list[list[str]] = []
    for image in images:
        rows.append(
            [
                str(image.get("id") or "-"),
                str(image.get("project_repo_hash") or "-"),
                _format_base_image_label(image.get("base_image")),
            ]
        )

    if rows:
        print_table("Project Images", ["ID", "Project Repo Hash", "Base Image"], rows)
    else:
        info("No project images.")
    info(f"Total images: {len(images)}")


@project_images_group.command("list")
def project_images_list_cmd(
    project_id: str | None = typer.Argument(
        None, help="Project UID. Defaults to local .env when omitted."
    ),
    path: str | None = typer.Option(
        None, "--path", help="Project repository path (default: current project)"
    ),
    filter_entries: list[str] | None = typer.Option(None, "--filter", help=LIST_FILTER_OPTION_HELP),
    show_filters: bool = typer.Option(
        False, "--show-filters", help="Show the filters supported by this list command and exit."
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    List project images for a project.

    Uses SDK client `ProjectImage.filter()` as the single source of truth.

    Parameters
    ----------
    project_id:
        Platform project UID. Defaults to local `.env`.
    path:
        Local project path. Used when resolving project uid from `.env`.
    timeout:
        Request timeout in seconds.

    Examples
    --------
    ```bash
    mainsequence project images list
    mainsequence project images list project-uid-123
    mainsequence project images list project-uid-123 --path .
    ```
    """
    _project_images_list_impl(
        project_id=project_id,
        path=path,
        filter_entries=filter_entries,
        show_filters=show_filters,
        timeout=timeout,
    )


def _project_images_delete_impl(
    *,
    image_id: int,
    yes: bool,
    timeout: int | None,
) -> None:
    _require_login()

    try:
        image = get_project_image(image_id=image_id, timeout=timeout)
    except ApiError as e:
        error(f"Project image fetch failed: {e}")
        raise typer.Exit(1) from e

    _confirm_delete_action(
        preview_title="Project Image Delete Preview",
        preview_items=_format_project_image_delete_preview(image),
        prompt_text=f"Delete project image {image_id}?",
        yes=yes,
    )

    try:
        deleted = delete_project_image(image_id=image_id, timeout=timeout)
    except ApiError as e:
        error(f"Project image deletion failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(deleted):
        return

    success(f"Project image deleted: id={image_id}")
    print_kv("Deleted Project Image", _format_project_image_delete_preview(deleted))


@project_images_group.command("delete")
def project_images_delete_cmd(
    image_id: int = typer.Argument(..., help="Project image ID."),
    yes: bool = typer.Option(False, "--yes", help="Delete without confirmation."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Delete a project image.

    Examples
    --------
    ```bash
    mainsequence project images delete 94
    mainsequence project images delete 94 --yes
    ```
    """
    _project_images_delete_impl(image_id=image_id, yes=yes, timeout=timeout)


def _project_images_create_impl(
    project_id: str | None,
    project_repo_hash: str | None,
    path: str | None,
    base_image_id: int | None,
    timeout: int,
    poll_interval: int,
) -> None:
    _require_login()

    project_dir = (
        _resolve_project_dir(project_id, path)
        if (project_id is not None or path)
        else _resolve_current_project_dir_from_env()
    )
    if project_id is None:
        project_id = _resolve_project_id_from_local_env(str(project_dir))

    try:
        existing_images = list_project_images(related_project_id=project_id, timeout=timeout)
    except ApiError as e:
        error(f"Project images fetch failed: {e}")
        raise typer.Exit(1) from e
    images_by_hash = _group_project_images_by_hash(existing_images)

    emit_json = _json_output_enabled()

    pending_commits = _list_unpushed_commits(project_dir)
    if pending_commits:
        pending_hashes = ", ".join(
            c["short_hash"] for c in pending_commits[:3] if c.get("short_hash")
        )
        suffix = f" Pending: {pending_hashes}." if pending_hashes else ""
        warn(
            f"{len(pending_commits)} local commit(s) have not been pushed yet. "
            "Only pushed commits can be used for project_repo_hash."
            f"{suffix}"
        )

    project_repo_hash = (project_repo_hash or "").strip()
    if not project_repo_hash:
        try:
            commits = _list_pushed_commits(project_dir)
        except RuntimeError as e:
            error(str(e))
            raise typer.Exit(1) from e

        rows = [
            [
                c["hash"],
                c["date"],
                c["subject"] or "-",
                _format_image_ids(images_by_hash.get(c["hash"], [])),
            ]
            for c in commits
        ]
        print_table("Pushed Commits", ["Hash", "Date/Time", "Subject", "Image IDs"], rows)
        project_repo_hash = typer.prompt("project_repo_hash", default=commits[0]["hash"]).strip()

    if not project_repo_hash:
        error("project_repo_hash is required.")
        raise typer.Exit(1)

    try:
        project_repo_hash = _resolve_full_commit_hash(project_dir, project_repo_hash)
    except RuntimeError as e:
        error(str(e))
        raise typer.Exit(1) from e

    if not _is_pushed_commit(project_dir, project_repo_hash):
        error(
            "project_repo_hash must reference a commit that has already been pushed to the remote."
        )
        raise typer.Exit(1)

    existing_for_hash = images_by_hash.get(project_repo_hash, [])
    if existing_for_hash:
        warn("This commit already has project image(s): " + _format_image_ids(existing_for_hash))

    try:
        if base_image_id is None:
            img_items = list_project_base_images()
            img_rows: list[list[str]] = []
            for item in img_items:
                name = item.get("title") or f"image-{item.get('id')}"
                details = item.get("description") or item.get("latest_digest") or "-"
                img_rows.append([str(item.get("id", "")), str(name), str(details)])
            base_image_id = _prompt_select_id(
                title="Available Base Images",
                prompt_label="Base image id",
                items=img_items,
                rows=img_rows,
            )

        created = create_project_image(
            project_repo_hash=project_repo_hash,
            related_project_id=project_id,
            base_image_id=base_image_id,
            timeout=timeout,
        )
    except ApiError as e:
        error(f"Project image creation failed: {e}")
        raise typer.Exit(1) from e
    except RuntimeError as e:
        error(str(e))
        raise typer.Exit(1) from e

    if not emit_json:
        success(f"Project image created: id={created.get('id') or '-'}")

    image_id = created.get("id")
    if image_id is not None and created.get("is_ready") is False:
        wait_deadline = time.monotonic() + max(int(timeout), 0)
        attempt = 0
        info(
            "Project image is still building. "
            f"Waiting until is_ready=true (poll every {poll_interval}s, timeout {timeout}s)."
        )
        while time.monotonic() < wait_deadline:
            attempt += 1
            remaining = max(wait_deadline - time.monotonic(), 0.0)
            sleep_for = min(max(int(poll_interval), 1), remaining)
            if sleep_for > 0:
                with status(
                    f"Project image not ready yet (attempt {attempt}). Next check in {int(sleep_for)}s..."
                ):
                    time.sleep(sleep_for)

            try:
                polled_images = list_project_images(related_project_id=project_id, timeout=timeout)
            except ApiError as e:
                warn(f"Project image status poll failed (attempt {attempt}): {e}")
                continue

            latest = next(
                (img for img in polled_images if str(img.get("id")) == str(image_id)), None
            )
            if latest is None:
                warn(f"Project image {image_id} was not visible yet on poll attempt {attempt}.")
                continue

            created = latest
            if created.get("is_ready") is True:
                if not emit_json:
                    success("Project image is ready.")
                break
            if not emit_json:
                info("Project image still building. Continuing to poll...")
        else:
            if not emit_json:
                warn(
                    f"Timed out after {timeout}s waiting for project image {image_id} to become ready. "
                    "It may still be building on the backend."
                )

    if _emit_json(created):
        return

    base_image_value = created.get("base_image")
    if isinstance(base_image_value, dict):
        base_image_value = base_image_value.get("id") or base_image_value.get("title") or "-"

    print_kv(
        "Project Image",
        [
            ("ID", str(created.get("id") or "-")),
            ("Project UID", str(project_id)),
            ("Project Repo Hash", project_repo_hash),
            ("Base Image", str(base_image_value or base_image_id or "-")),
            (
                "Is Ready",
                str(created.get("is_ready")) if created.get("is_ready") is not None else "-",
            ),
        ],
    )


@project_images_group.command("create")
def project_images_create_cmd(
    project_id: str | None = typer.Argument(
        None, help="Project UID. Defaults to local .env when omitted."
    ),
    project_repo_hash: str | None = typer.Argument(
        None,
        help="Git commit hash for the image build. Must already be pushed to the remote.",
    ),
    path: str | None = typer.Option(
        None, "--path", help="Project repository path (default: current project)"
    ),
    base_image_id: int | None = typer.Option(None, "--base-image-id", help="Project base image ID"),
    timeout: int = typer.Option(
        300, "--timeout", help="Maximum wait time in seconds for the image to become ready."
    ),
    poll_interval: int = typer.Option(
        30, "--poll-interval", help="Polling interval in seconds while waiting for is_ready=true."
    ),
):
    """
    Create a project image from a pushed git commit.

    If `project_id` is omitted, the command reads `MAIN_SEQUENCE_PROJECT_UID`
    from the local project `.env`. If `project_repo_hash` is omitted, it shows
    only commits already present on the remote and prompts for a selection.

    Parameters
    ----------
    project_id:
        Platform project UID. Defaults to local `.env`.
    project_repo_hash:
        Git commit hash already pushed to remote.
    path:
        Local repository path. Defaults to current project folder.
    base_image_id:
        Project base image ID. If omitted, prompt from available base images.
    timeout:
        Maximum wait time in seconds for the image to become ready.
    poll_interval:
        Polling interval in seconds while waiting for `is_ready=true`.

    Examples
    --------
    ```bash
    mainsequence project images create
    mainsequence project images create project-uid-123
    mainsequence project images create project-uid-123 4a1b2c3d
    mainsequence project images create project-uid-123 --path .
    mainsequence project images create project-uid-123 --timeout 600 --poll-interval 15
    ```
    """
    _project_images_create_impl(
        project_id=project_id,
        project_repo_hash=project_repo_hash,
        path=path,
        base_image_id=base_image_id,
        timeout=timeout,
        poll_interval=poll_interval,
    )


@project.command("create_image", hidden=True)
def project_create_image_cmd(
    project_id: str | None = typer.Argument(
        None, help="Project UID. Defaults to local .env when omitted."
    ),
    project_repo_hash: str | None = typer.Argument(
        None,
        help="Git commit hash for the image build. Must already be pushed to the remote.",
    ),
    path: str | None = typer.Option(
        None, "--path", help="Project repository path (default: current project)"
    ),
    base_image_id: int | None = typer.Option(None, "--base-image-id", help="Project base image ID"),
    timeout: int = typer.Option(
        300, "--timeout", help="Maximum wait time in seconds for the image to become ready."
    ),
    poll_interval: int = typer.Option(
        30, "--poll-interval", help="Polling interval in seconds while waiting for is_ready=true."
    ),
):
    """
    Backward-compatible alias for `mainsequence project images create`.
    """
    _project_images_create_impl(
        project_id=project_id,
        project_repo_hash=project_repo_hash,
        path=path,
        base_image_id=base_image_id,
        timeout=timeout,
        poll_interval=poll_interval,
    )


def _project_jobs_list_impl(
    project_id: str | None,
    path: str | None,
    filter_entries: list[str] | None,
    show_filters: bool,
    timeout: int | None,
) -> None:
    filters = _resolve_cli_list_filters(
        model_ref=JOB_MODEL_REF,
        filter_entries=filter_entries,
        show_filters=show_filters,
        command_label="Project Jobs",
        reserved_filter_descriptions={
            "project": "always scoped to the selected project",
            "project__id__in": "always scoped to the selected project",
        },
    )

    _require_login()

    if project_id is None:
        project_id = _resolve_project_id_from_local_env(path)

    try:
        jobs = list_project_jobs(project_id=project_id, filters=filters, timeout=timeout)
    except ApiError as e:
        error(f"Project jobs fetch failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(jobs):
        return

    rows: list[list[str]] = []
    for job in jobs:
        rows.append(
            [
                str(job.get("id") or "-"),
                str(job.get("name") or "-"),
                str(job.get("project_repo_hash") or "-"),
                str(job.get("execution_path") or "-"),
                str(job.get("app_name") or "-"),
                _format_job_schedule_summary(job.get("task_schedule")),
                _format_related_image_label(job.get("related_image")),
            ]
        )

    if rows:
        print_table(
            "Project Jobs",
            ["ID", "Name", "Repo Hash", "Execution Path", "App Name", "Schedule", "Related Image"],
            rows,
        )
    else:
        info("No project jobs.")
    info(f"Total jobs: {len(jobs)}")


def _project_job_runs_list_impl(
    job_id: int,
    filter_entries: list[str] | None,
    show_filters: bool,
    timeout: int | None,
) -> None:
    filters = _resolve_cli_list_filters(
        model_ref=JOB_RUN_MODEL_REF,
        filter_entries=filter_entries,
        show_filters=show_filters,
        command_label="Project Job Runs",
        reserved_filter_descriptions={"job__id": "always set from JOB_ID"},
    )

    _require_login()

    try:
        runs = list_project_job_runs(job_id=job_id, filters=filters, timeout=timeout)
    except ApiError as e:
        error(f"Project job runs fetch failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(runs):
        return

    rows: list[list[str]] = []
    for run in runs:
        rows.append(
            [
                str(run.get("id") or "-"),
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
            "Project Job Runs",
            [
                "ID",
                "Name",
                "Status",
                "Execution Start",
                "Execution End",
                "Unique Identifier",
                "Commit Hash",
            ],
            rows,
        )
    else:
        info("No job runs.")
    info(f"Total job runs: {len(runs)}")


def _format_job_run_log_row(row) -> str:
    if isinstance(row, str):
        return row
    if not isinstance(row, dict):
        return str(row)

    timestamp = str(row.get("timestamp") or "").strip()
    level = str(row.get("level") or "").strip().upper()
    event = str(row.get("event") or "").strip()

    parts = [part for part in (timestamp, level, event) if part]
    if parts:
        return " | ".join(parts)
    return json.dumps(row, default=str, sort_keys=True)


def _print_job_run_logs_rows(rows, *, start_index: int = 0) -> int:
    if not isinstance(rows, list):
        return start_index

    for row in rows[start_index:]:
        typer.echo(_format_job_run_log_row(row))
    return len(rows)


@project_jobs_group.command("list")
def project_jobs_list_cmd(
    project_id: str | None = typer.Argument(
        None, help="Project UID. Defaults to local .env when omitted."
    ),
    path: str | None = typer.Option(
        None, "--path", help="Project repository path (default: current project)"
    ),
    filter_entries: list[str] | None = typer.Option(None, "--filter", help=LIST_FILTER_OPTION_HELP),
    show_filters: bool = typer.Option(
        False, "--show-filters", help="Show the filters supported by this list command and exit."
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    List jobs for a project.

    Uses SDK client `Job.filter()` as the single source of truth.

    Examples
    --------
    ```bash
    mainsequence project jobs list
    mainsequence project jobs list project-uid-123
    mainsequence project jobs list project-uid-123 --path .
    ```
    """
    _project_jobs_list_impl(
        project_id=project_id,
        path=path,
        filter_entries=filter_entries,
        show_filters=show_filters,
        timeout=timeout,
    )


@project_jobs_group.command("run")
def project_jobs_run_cmd(
    job_id: int = pydantic_argument(JOB_MODEL_REF, "id", ..., help="Job ID to run."),
    passthrough_args: list[str] | None = typer.Argument(
        None,
        help="Additional per-run args after `--`, for example `mainsequence project jobs run 91 -- --name demo`.",
    ),
    command_args: list[str] | None = typer.Option(
        None,
        "--arg",
        help="Append one per-run arg to the saved job entrypoint. Repeatable. Does not replace the saved execution_path or app_name.",
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Run a project job immediately.

    Uses SDK client `Job.run_job()` as the single source of truth.
    Per-run args are appended to the saved job entrypoint; they do not replace it.

    Examples
    --------
    ```bash
    mainsequence project jobs run 91
    mainsequence project jobs run 91 --arg demo-from-cli
    mainsequence project jobs run 91 -- --name demo-from-cli
    mainsequence project jobs run 91 --timeout 60
    ```
    """
    _require_login()

    merged_command_args = list(command_args or [])
    if passthrough_args:
        merged_command_args.extend(str(arg) for arg in passthrough_args)

    try:
        job_payload = get_project_job(job_id, timeout=timeout)
    except ApiError as e:
        error(f"Project job fetch failed: {e}")
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
        payload = run_project_job(
            job_id=job_id,
            command_args=merged_command_args or None,
            timeout=timeout,
        )
    except ApiError as e:
        error(f"Project job run failed: {e}")
        raise typer.Exit(1) from e

    if _emit_json(payload):
        return

    success(f"Project job run requested: job_id={job_id}")

    if payload:
        preferred_keys = [
            ("Job ID", str(payload.get("job") or payload.get("job_id") or job_id)),
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
                "job_id",
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


@project_job_runs_group.command("list")
def project_job_runs_list_cmd(
    job_id: int = pydantic_argument(
        JOB_MODEL_REF, "id", ..., help="Job ID whose runs will be listed."
    ),
    filter_entries: list[str] | None = typer.Option(None, "--filter", help=LIST_FILTER_OPTION_HELP),
    show_filters: bool = typer.Option(
        False, "--show-filters", help="Show the filters supported by this list command and exit."
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    List runs for a specific job.

    Uses SDK client `JobRun.filter(job__id=[job_id])` as the single source of truth.

    Examples
    --------
    ```bash
    mainsequence project jobs runs list 91
    mainsequence project jobs runs list 91 --timeout 60
    ```
    """
    _project_job_runs_list_impl(
        job_id=job_id,
        filter_entries=filter_entries,
        show_filters=show_filters,
        timeout=timeout,
    )


@project_job_runs_group.command("logs")
def project_job_runs_logs_cmd(
    job_run_uid: str = pydantic_argument(
        JOB_RUN_MODEL_REF, "uid", ..., help="Job run UID whose logs will be shown."
    ),
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
    mainsequence project jobs runs logs 4c1d77c8-8a42-42b8-a9c1-06be9a336e5d
    mainsequence project jobs runs logs 4c1d77c8-8a42-42b8-a9c1-06be9a336e5d --poll-interval 10
    mainsequence project jobs runs logs 4c1d77c8-8a42-42b8-a9c1-06be9a336e5d --max-wait-seconds 900
    mainsequence project jobs runs logs 4c1d77c8-8a42-42b8-a9c1-06be9a336e5d --poll-interval 0
    ```
    """
    _require_login()

    if max_wait_seconds < 0:
        error("--max-wait-seconds must be >= 0.")
        raise typer.Exit(2)

    shown_rows = 0
    poll_started_at = time.monotonic()

    while True:
        try:
            payload = get_project_job_run_logs(job_run_uid=job_run_uid, timeout=timeout)
        except ApiError as e:
            error(f"Project job run logs fetch failed: {e}")
            raise typer.Exit(1) from e

        if _emit_json(payload):
            return

        status_value = str(payload.get("status") or "-")
        rows = payload.get("rows") or []
        if not isinstance(rows, list):
            rows = [rows]

        if shown_rows == 0:
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
        else:
            info(f"Job run status: {status_value}")

        if len(rows) < shown_rows:
            warn("Log stream was reset by the backend. Reprinting from the beginning.")
            shown_rows = 0

        shown_rows = _print_job_run_logs_rows(rows, start_index=shown_rows)
        if shown_rows == 0:
            info("No logs yet.")

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


def _project_jobs_create_impl(
    project_id: str | None,
    name: str | None,
    path: str | None,
    execution_path: str | None,
    app_name: str | None,
    related_image_id: int | None,
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
    timeout: int | None,
) -> None:
    _require_login()

    project_dir = (
        _resolve_project_dir(project_id, path)
        if (project_id is not None or path)
        else _resolve_current_project_dir_from_env()
    )
    if project_id is None:
        project_id = _resolve_project_id_from_local_env(str(project_dir))

    name = (name or "").strip() or typer.prompt(pydantic_prompt_text(JOB_MODEL_REF, "name")).strip()
    if not name:
        error("Job name is required.")
        raise typer.Exit(1)

    try:
        project_images = list_project_images(related_project_id=project_id, timeout=timeout)
    except ApiError as e:
        error(f"Project images fetch failed: {e}")
        raise typer.Exit(1) from e

    if related_image_id is None and project_images:
        image_rows = [
            [
                str(img.get("id") or "-"),
                str(img.get("project_repo_hash") or "-"),
                _format_base_image_label(img.get("base_image")),
            ]
            for img in project_images
        ]
        related_image_id = _prompt_select_id(
            title="Available Project Images",
            prompt_label="Related image ID",
            items=project_images,
            rows=image_rows,
        )

    if related_image_id is None:
        error("related_image_id is required for jobs.")
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
        created = create_project_job(
            name=name,
            project_id=project_id,
            execution_path=execution_path,
            app_name=app_name,
            task_schedule=task_schedule,
            cpu_request=cpu_request,
            memory_request=memory_request,
            gpu_request=gpu_request,
            gpu_type=gpu_type,
            spot=spot,
            max_runtime_seconds=max_runtime_seconds,
            related_image_id=related_image_id,
            timeout=timeout,
        )
    except ApiError as e:
        error(f"Project job creation failed: {e}")
        raise typer.Exit(1) from e
    except RuntimeError as e:
        error(str(e))
        raise typer.Exit(1) from e

    if _emit_json(created):
        return

    success(f"Project job created: id={created.get('id') or '-'}")
    print_kv(
        "Project Job",
        [
            ("ID", str(created.get("id") or "-")),
            ("Name", str(created.get("name") or name)),
            ("Project UID", str(project_id)),
            ("Execution Path", str(created.get("execution_path") or execution_path or "-")),
            ("App Name", str(created.get("app_name") or app_name or "-")),
            (
                "Related Image",
                _format_related_image_label(created.get("related_image") or related_image_id),
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
        ],
    )


@project_jobs_group.command("create")
def project_jobs_create_cmd(
    project_id: str | None = typer.Argument(
        None, help="Project UID. Defaults to local .env when omitted."
    ),
    name: str | None = pydantic_option(JOB_MODEL_REF, "name", None, "--name"),
    path: str | None = typer.Option(
        None, "--path", help="Project repository path (default: current project)"
    ),
    execution_path: str | None = pydantic_option(
        JOB_MODEL_REF,
        "execution_path",
        None,
        "--execution-path",
    ),
    app_name: str | None = pydantic_option(JOB_MODEL_REF, "app_name", None, "--app-name"),
    related_image_id: int | None = pydantic_option(
        JOB_MODEL_REF,
        "related_image_uid",
        None,
        "--related-image-id",
        extra_help="Use the numeric project image ID.",
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
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Create a job for a project.

    Uses SDK client `Job.create()` as the single source of truth.
    When compute settings are omitted, the CLI applies safe defaults:
    `cpu_request=0.25`, `memory_request=0.5`, `spot=false`, `max_runtime_seconds=86400`.
    If schedule arguments are omitted, the CLI asks whether to build an interval or crontab schedule.

    Examples
    --------
    ```bash
    mainsequence project jobs create
    mainsequence project jobs create project-uid-123 --name daily-run --execution-path scripts/test.py --related-image-id 77
    mainsequence project jobs create project-uid-123 --name dashboard --app-name dashboard-api --related-image-id 77
    mainsequence project jobs create project-uid-123 --name hourly-run --execution-path scripts/test.py --related-image-id 77 --schedule-type interval --schedule-every 1 --schedule-period hours
    mainsequence project jobs create project-uid-123 --name nightly-run --execution-path scripts/test.py --related-image-id 77 --schedule-type crontab --schedule-expression "0 0 * * *"
    ```
    """
    _project_jobs_create_impl(
        project_id=project_id,
        name=name,
        path=path,
        execution_path=execution_path,
        app_name=app_name,
        related_image_id=related_image_id,
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
        timeout=timeout,
    )


@project.command("schedule_batch_jobs")
def project_schedule_batch_jobs_cmd(
    file_path: str = typer.Argument(..., help="Path to the scheduled jobs YAML file."),
    project_id: str | None = typer.Argument(
        None, help="Project UID. Defaults to local .env when omitted."
    ),
    path: str | None = typer.Option(
        None, "--path", help="Project repository path used to resolve project uid."
    ),
    strict: bool = typer.Option(
        False,
        "--strict/--no-strict",
        help=(
            "If enabled, jobs that exist remotely but are not listed in the file may be removed. "
            "Jobs linked to dashboards or resource releases are protected."
        ),
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Validate and submit a batch of jobs from a YAML file.

    Uses SDK client `Job.bulk_get_or_create()` as the single source of truth.
    In strict mode, jobs linked to dashboards or resource releases are not deleted.

    Examples
    --------
    ```bash
    mainsequence project schedule_batch_jobs scheduled_jobs.yaml
    mainsequence project schedule_batch_jobs scheduled_jobs.yaml project-uid-123
    mainsequence project schedule_batch_jobs scheduled_jobs.yaml --strict
    mainsequence project schedule_batch_jobs configs/scheduled_jobs.yaml --path .
    ```
    """
    _require_login()

    project_dir = normalize_path(path) if path else pathlib.Path.cwd()
    if not project_dir.exists():
        error(f"Project folder does not exist: {project_dir}")
        raise typer.Exit(1)

    if project_id is None:
        project_id = _resolve_project_id_from_local_env(str(project_dir))

    batch_file = pathlib.Path(file_path).expanduser()
    if not batch_file.is_absolute():
        batch_file = (project_dir / batch_file).resolve()

    if not batch_file.is_file():
        error(f"Jobs file not found: {batch_file}")
        raise typer.Exit(1)

        prepared_batch_file = batch_file
    try:
        prepared_batch_file = _prepare_batch_jobs_file_with_selected_related_image(
            project_id=project_id,
            batch_file=batch_file,
            timeout=timeout,
        )
        if not _confirm_schedule_batch_jobs_submission(prepared_batch_file):
            return
        created = schedule_batch_project_jobs(
            file_path=str(prepared_batch_file),
            project_id=project_id,
            strict=strict,
            timeout=timeout,
        )
    except ApiError as e:
        error(f"Batch job scheduling failed: {e}")
        raise typer.Exit(1) from e
    except RuntimeError as e:
        error(str(e))
        raise typer.Exit(1) from e
    finally:
        if prepared_batch_file != batch_file:
            try:
                prepared_batch_file.unlink(missing_ok=True)
            except Exception:
                pass

    if _emit_json(created):
        return

    if isinstance(created, list):
        success(f"Scheduled {len(created)} jobs from {batch_file.name}.")
        rows = [
            [
                str(item.get("id") or "-"),
                str(item.get("name") or "-"),
                str(item.get("execution_path") or "-"),
                str(item.get("app_name") or "-"),
                _format_job_schedule_summary(item.get("task_schedule")),
            ]
            for item in created
        ]
        print_table(
            "Scheduled Jobs",
            ["ID", "Name", "Execution Path", "App Name", "Schedule"],
            rows,
        )
        return

    success(f"Scheduled jobs from {batch_file.name}.")
    summary_items = [
        ("Project UID", str(project_id)),
        ("File", str(batch_file)),
        ("Strict", str(bool(created.get("strict", strict))).lower()),
        ("Created", str(created.get("created_count", 0))),
        ("Existing", str(created.get("existing_count", 0))),
        ("Deleted", str(created.get("deleted_count", 0))),
        ("Not Deleted", str(created.get("not_deleted_count", 0))),
    ]
    print_kv("Batch Scheduling Summary", summary_items)

    results = created.get("results")
    if isinstance(results, list) and results:
        rows: list[list[str]] = []
        for item in results:
            job = item.get("job") if isinstance(item, dict) else {}
            status_label = "created" if bool(item.get("created")) else "existing"
            rows.append(
                [
                    status_label,
                    str(job.get("id") or "-"),
                    str(job.get("name") or "-"),
                    str(job.get("execution_path") or "-"),
                    str(job.get("app_name") or "-"),
                    _format_job_schedule_summary(job.get("task_schedule")),
                ]
            )
        print_table(
            "Batch Job Results",
            ["Status", "ID", "Name", "Execution Path", "App Name", "Schedule"],
            rows,
        )

    deleted_items = created.get("deleted")
    if isinstance(deleted_items, list) and deleted_items:
        rows = []
        for item in deleted_items:
            job_id, name, execution_path, app_name = _format_batch_job_ref(item)
            rows.append([job_id, name, execution_path, app_name])
        print_table(
            "Deleted Jobs",
            ["ID", "Name", "Execution Path", "App Name"],
            rows,
        )

    not_deleted_items = created.get("not_deleted")
    if isinstance(not_deleted_items, list) and not_deleted_items:
        rows = []
        for item in not_deleted_items:
            job_id, name, execution_path, app_name = _format_batch_job_ref(item)
            rows.append([job_id, name, execution_path, app_name, _format_batch_job_reason(item)])
        print_table(
            "Not Deleted Jobs",
            ["ID", "Name", "Execution Path", "App Name", "Reason"],
            rows,
        )
        warn(
            "Strict mode will not delete jobs that are still linked to dashboards or resource releases."
        )


@project.command("set-up-locally")
def project_set_up_locally(
    project_id: str = typer.Argument(..., help="Project UID from the platform"),
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
    Clone a project locally and provision runtime `.env`.

    Workflow:
    - ensure SSH key and optionally register deploy key,
    - clone repository into local projects root,
    - build local runtime auth/backend entries for the active auth mode,
    - write/update `.env` with local runtime values.

    Parameters
    ----------
    project_id:
        Platform project UID.
    base_dir:
        Override local projects base directory.
    scaffold_docker:
        Deprecated compatibility flag. No effect.

    Examples
    --------
    ```bash
    mainsequence project set-up-locally project-uid-123
    mainsequence project set-up-locally project-uid-123 --base-dir ~/mainsequence
    mainsequence project set-up-locally project-uid-123 --no-scaffold-docker
    ```
    """
    _require_login()

    cfg_obj = cfg.get_config()
    base = base_dir or cfg_obj["mainsequence_path"]
    org_slug = _org_slug_from_profile()

    try:
        p = resolve_project(project_id)
    except ApiError as e:
        error(f"Project not found/visible: {e}")
        raise typer.Exit(1) from e

    project_uid = _project_identity_value(p) or str(project_id).strip()
    project_row_id = str(p.get("id") or "").strip()

    is_initialized = p.get("is_initialized")
    if is_initialized is None:
        try:
            p = get_project(project_uid)
        except ApiError as e:
            error(f"Could not verify project initialization status: {e}")
            raise typer.Exit(1) from e
        is_initialized = p.get("is_initialized")

    if is_initialized is not True:
        error(
            "Project has not finished initializing yet. "
            "Wait until is_initialized=true and try again."
        )
        raise typer.Exit(1)

    repo = _determine_repo_url(p)
    if not repo:
        error("No repository URL found for this project.")
        raise typer.Exit(1)

    name = safe_slug(p.get("project_name") or f"project-{project_uid}")
    projects_root = _projects_root(base, org_slug)
    target_dir = projects_root / f"{name}-{project_uid}"
    projects_root.mkdir(parents=True, exist_ok=True)

    key_path, _pub_path, pub = ensure_key_for_repo(repo)
    copied = _copy_clipboard(pub)

    # Best-effort deploy key (do not fail set-up-locally on this)
    try:
        host = platform.node()
        add_deploy_key(project_uid, host, pub)
    except Exception as e:
        warn(f"Could not add deploy key automatically (continuing): {e}")

    agent_env = start_agent_and_add_key(key_path)

    if target_dir.exists():
        warn(f"Target already exists: {target_dir}")
        raise typer.Exit(2)

    env = os.environ.copy() | agent_env
    env["GIT_SSH_COMMAND"] = f'ssh -i "{str(key_path)}" -o IdentitiesOnly=yes'

    with status(f"Cloning repo into {target_dir}..."):
        rc = subprocess.call(
            ["git", "clone", repo, str(target_dir)], env=env, cwd=str(projects_root)
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
        auth_env = _current_project_runtime_auth_env(backend_url)
    except RuntimeError as e:
        error(str(e))
        raise typer.Exit(1) from e
    except ApiError as e:
        error(str(e))
        raise typer.Exit(1) from e

    final_env = _render_project_runtime_env_text(
        "",
        auth_env=auth_env,
        backend_url=backend_url,
        project_runtime_uid=project_uid,
    )
    (target_dir / ".env").write_text(final_env, encoding="utf-8")

    success(f"Local folder: {target_dir}")
    info(f"Repo URL: {repo}")
    if project_row_id:
        info(f"Resolved backend row id: {project_row_id}")
    if copied:
        info("Public key copied to clipboard.")


@project.command("open")
def project_open(
    project_id: str | None = typer.Argument(None, help="Project UID"),
    path: str | None = typer.Option(
        None, "--path", help="Open an explicit path instead of resolving by id"
    ),
):
    """
    Open a mapped project folder in the OS file manager.

    Parameters
    ----------
    project_id:
        Project UID to resolve local folder.
    path:
        Explicit local path to open.

    Examples
    --------
    ```bash
    mainsequence project open project-uid-123
    mainsequence project open --path .
    ```
    """
    p = _resolve_project_dir(project_id, path)
    open_folder(str(p))
    success(f"Opened: {p}")


@project.command("delete-local")
def project_delete_local(
    project_id: str | None = typer.Argument(None, help="Project UID"),
    path: str | None = typer.Option(
        None, "--path", help="Delete an explicit path instead of resolving by id"
    ),
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not prompt for confirmation"),
):
    """
    Delete a local project folder.

    Safety checks prevent deletion outside configured projects root.

    Parameters
    ----------
    project_id:
        Project UID to resolve local path.
    path:
        Explicit local path to delete.
    yes:
        Skip confirmation prompt.

    Examples
    --------
    ```bash
    mainsequence project delete-local project-uid-123
    mainsequence project delete-local --path ./my-project --yes
    ```
    """
    p = _resolve_project_dir(project_id, path)

    # Determine projects root for safety check
    cfg_obj = cfg.get_config()
    base = cfg_obj["mainsequence_path"]
    org_slug = "default"
    try:
        prof = get_current_user_profile()
        if prof and prof.get("organization"):
            org_slug = _org_slug_from_profile()
    except Exception:
        pass

    projects_root = _projects_root(base, org_slug).resolve()
    try:
        p.resolve().relative_to(projects_root)
    except Exception as e:
        error(f"Refusing to delete outside projects root: {p}")
        raise typer.Exit(1) from e

    warning_text = (
        "This will permanently delete the local project folder.\n"
        "Your project will remain on the platform.\n"
    )
    if not yes:
        if not typer.confirm(f"{warning_text}\nDelete: {p} ?", default=False):
            info("Cancelled.")
            raise typer.Exit(0)

    import shutil

    shutil.rmtree(str(p), ignore_errors=True)
    warn(f"Deleted: {p}")


@project.command("open-signed-terminal")
def project_open_signed_terminal(
    project_id: str | None = typer.Argument(None, help="Project UID"),
    path: str | None = typer.Option(None, "--path", help="Open in a specific project directory"),
):
    """
    Open terminal with `ssh-agent` and project key preloaded.

    Parameters
    ----------
    project_id:
        Project UID to resolve local folder.
    path:
        Explicit local path.

    Examples
    --------
    ```bash
    mainsequence project open-signed-terminal project-uid-123
    mainsequence project open-signed-terminal --path .
    ```
    """
    project_dir = _resolve_project_dir(project_id, path)

    origin = git_origin(project_dir)
    name = repo_name_from_git_url(origin) or project_dir.name
    key_path, _, _ = ensure_key_for_repo(origin)  # creates if missing
    open_signed_terminal(str(project_dir), key_path, name)


@project.command("build_local_venv")
def project_build_local_venv(
    project_id: str | None = typer.Argument(None, help="Project UID"),
    path: str | None = typer.Option(None, "--path", help="Project directory"),
):
    """
    Build local `.venv` and sync dependencies using `uv`.

    Reads Python requirement from `pyproject.toml`, creates `.venv`, then runs `uv sync`.

    Parameters
    ----------
    project_id:
        Project UID to resolve local folder. If omitted, the current directory is used when
        `MAIN_SEQUENCE_PROJECT_UID` is present in `./.env`.
    path:
        Explicit local path.

    Examples
    --------
    ```bash
    mainsequence project build_local_venv
    mainsequence project build_local_venv project-uid-123
    mainsequence project build_local_venv --path .
    ```
    """
    project_dir = (
        _resolve_project_dir(project_id, path)
        if (project_id is not None or path)
        else _resolve_current_project_dir_from_env()
    )
    venv_path = project_dir / ".venv"
    if venv_path.exists():
        info(f"Skipped: {venv_path} already exists.")
        return

    pyproject_path = project_dir / "pyproject.toml"
    if not pyproject_path.is_file():
        error("pyproject.toml not found in the project root.")
        raise typer.Exit(1)

    try:
        pyproject_text = pyproject_path.read_text(encoding="utf-8")
    except Exception as e:
        error("Could not read pyproject.toml from the project root.")
        raise typer.Exit(1) from e

    python_version = _extract_python_version_from_pyproject_text(pyproject_text)
    if not python_version:
        error(
            "Could not determine Python version from pyproject.toml (requires-python or Poetry python spec)."
        )
        raise typer.Exit(1)

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

        info(f"Creating .venv with Python {python_version}...")
        venv_result = subprocess.run(
            [*uv_cmd, "venv", ".venv", "--python", python_version],
            cwd=str(project_dir),
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
            cwd=str(project_dir),
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

    success(f"Local .venv built with Python {python_version}.")


@project.command("refresh_token")
def project_refresh_token(
    project_id: str | None = typer.Argument(None, help="Project UID"),
    path: str | None = typer.Option(None, "--path", help="Project directory"),
):
    """
    Refresh local project auth entries in `.env` from the active auth mode.

    Use this when a project has been idle long enough for the previously injected
    auth token to expire. The command preserves the rest of the `.env` file and
    only rewrites the runtime auth keys managed by the CLI.

    Parameters
    ----------
    project_id:
        Project UID to resolve local folder. If omitted, the current directory is used.
    path:
        Explicit local path. If omitted, the current directory is used.

    Examples
    --------
    ```bash
    mainsequence project refresh_token
    mainsequence project refresh_token project-uid-123
    mainsequence project refresh_token --path .
    ```
    """
    _require_login()
    project_dir = (
        _resolve_project_dir(project_id, path)
        if (project_id is not None or path)
        else pathlib.Path.cwd()
    )
    env_path = project_dir / ".env"
    if not env_path.is_file():
        error(f".env not found in project root: {env_path}")
        info(
            "Run: mainsequence project set-up-locally <project_uid> to provision the local runtime first."
        )
        raise typer.Exit(1)

    backend_url = cfg.backend_url()
    try:
        auth_env = _current_project_runtime_auth_env(backend_url)
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

    inferred_project_id = (
        str(project_id) if project_id is not None else _read_project_ref_from_env_file(project_dir)
    )

    final_env = _render_project_runtime_env_text(
        env_text,
        auth_env=auth_env,
        backend_url=backend_url,
        project_runtime_uid=inferred_project_id,
    )
    env_path.write_text(final_env, encoding="utf-8")
    success(f"Refreshed auth entries in: {env_path}")


@project.command("freeze-env")
def project_freeze_env(
    project_id: str | None = typer.Argument(None, help="Project UID"),
    path: str | None = typer.Option(None, "--path", help="Project directory"),
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
    project_id:
        Project UID to resolve local folder.
    path:
        Explicit local path.
    ensure_uv:
        Allow resolving `uv` from PATH when it is not present inside `.venv`.

    Examples
    --------
    ```bash
    mainsequence project freeze-env project-uid-123
    mainsequence project freeze-env --path .
    mainsequence project freeze-env --path . --no-ensure-uv
    ```
    """
    project_dir = _resolve_project_dir(project_id, path)
    ensure_venv(project_dir)

    uv = ensure_uv_installed(project_dir) if ensure_uv else (ensure_venv(project_dir).uv or None)
    if not uv:
        error("uv not found in .venv and --no-ensure-uv was used.")
        raise typer.Exit(1)

    with status("Exporting requirements.txt via uv..."):
        uv_export_requirements(
            uv, cwd=project_dir, locked=False, no_dev=False, output_file="requirements.txt"
        )

    success(f"Wrote: {project_dir / 'requirements.txt'}")


@project.command("sync")
def project_sync(
    message: str | None = typer.Argument(None, help="Git commit message"),
    project_id: str | None = typer.Argument(None, help="Project UID"),
    path: str | None = typer.Option(None, "--path", help="Project directory"),
    message_opt: str | None = typer.Option(None, "--message", "-m", help="Git commit message"),
    bump: str = typer.Option(
        "patch", "--bump", help="uv version bump: patch|minor|major (default: patch)"
    ),
    no_push: bool = typer.Option(False, "--no-push", help="Do not git push"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Print steps but do not execute"),
):
    """
    Run end-to-end sync workflow for project dependencies and git state.

    Workflow:
    1. bump package version via `uv version`,
    2. run `uv lock` + `uv sync`,
    3. export locked `requirements.txt`,
    4. commit and push git changes.

    Parameters
    ----------
    message:
        Commit message. Can be passed positionally or via `--message`.
    project_id:
        Project UID to resolve local folder.
    path:
        Explicit local path.
    bump:
        Version bump strategy (`patch`, `minor`, `major`).
    no_push:
        Skip git push.
    dry_run:
        Print plan without executing commands.

    Examples
    --------
    ```bash
    mainsequence project sync "Update environment"
    mainsequence project sync -m "Update environment" --path .
    mainsequence project sync -m "Bump minor" --bump minor --path .
    mainsequence project sync -m "Preview only" --path . --dry-run
    ```
    """
    if message is not None and message_opt is not None:
        error("Pass the commit message either positionally or with --message, not both.")
        raise typer.Exit(2)

    message = message if message is not None else message_opt
    project_dir = _resolve_project_dir(project_id, path)
    resolved_project_id = (
        project_id if project_id is not None else _read_project_ref_from_env_file(project_dir)
    )
    if not dry_run and not no_push:
        _require_login()
        if resolved_project_id is None:
            error(
                "Could not determine project uid from local .env. "
                "Pass PROJECT_UID or ensure MAIN_SEQUENCE_PROJECT_UID is present before syncing."
            )
            raise typer.Exit(1)
        try:
            prime_sync_project_after_commit_sdk()
        except ApiError as e:
            error(f"Could not load SDK post-commit sync path before uv sync: {e}")
            raise typer.Exit(1) from e
    ensure_venv(project_dir)

    origin = git_origin(project_dir)
    repo_name = repo_name_from_git_url(origin) or project_dir.name
    key_path, _, _ = ensure_key_for_repo(origin)

    safe_message = (
        str(message or "").replace("\r", " ").replace("\n", " ").replace('"', "'").strip()
    )
    if not safe_message:
        error("Commit message is required.")
        raise typer.Exit(1)

    steps = [
        "resolve uv executable",
        f"uv version --bump {bump}",
        "uv lock",
        "uv sync",
        "uv export (locked) -> requirements.txt",
        "git add -A",
        f'git commit -m "{safe_message}"',
        "git push" if not no_push else "(skip git push)",
    ]

    print_table("Sync plan", ["Step"], [[s] for s in steps])

    if dry_run:
        warn("Dry run: no commands executed.")
        return

    env = os.environ.copy()
    env["GIT_SSH_COMMAND"] = f'ssh -i "{str(key_path)}" -o IdentitiesOnly=yes'

    uv = ensure_uv_installed(project_dir)
    with status("Running uv + git sync steps..."):
        run_uv(uv, ["version", "--bump", bump], cwd=project_dir, env=env)
        run_uv(uv, ["lock"], cwd=project_dir, env=env)
        run_uv(uv, ["sync"], cwd=project_dir, env=env)
        # `uv sync` can prune ad hoc packages from `.venv`, including a `uv`
        # executable that was installed there just for this workflow.
        uv = ensure_uv_installed(project_dir)
        uv_export_requirements(
            uv,
            cwd=project_dir,
            locked=True,
            no_dev=True,
            no_hashes=True,
            output_file="requirements.txt",
        )

        run_cmd(["git", "add", "-A"], cwd=project_dir, env=env)
        run_cmd(["git", "commit", "-m", safe_message], cwd=project_dir, env=env)
        if not no_push:
            run_cmd(["git", "push"], cwd=project_dir, env=env)

    if not dry_run and not no_push and resolved_project_id is not None:
        with status("Triggering backend sync_project_after_commit..."):
            try:
                sync_project_after_commit(resolved_project_id)
            except ApiError as e:
                error(f"Backend post-commit sync failed: {e}")
                raise typer.Exit(1) from e
        info(f"Triggered backend sync for project {resolved_project_id}.")

    success(f"Synced: {repo_name}")


@project.command("sync-after-commit")
def project_sync_after_commit(
    project_uid: str | None = typer.Argument(
        None,
        help="Project UID. If omitted, read MAIN_SEQUENCE_PROJECT_UID from local .env.",
    ),
    path: str | None = typer.Option(
        None,
        "--path",
        help="Project directory used to resolve MAIN_SEQUENCE_PROJECT_UID when PROJECT_UID is omitted.",
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds."),
):
    """
    Trigger backend post-commit project sync.

    This directly calls:

    POST /orm/api/pods/projects/<project_uid>/sync_project_after_commit/

    Examples
    --------
    ```bash
    mainsequence project sync-after-commit project-uid-123
    mainsequence project sync-after-commit --path .
    ```
    """
    _require_login()

    resolved_project_uid = project_uid
    if resolved_project_uid is None:
        project_dir = normalize_path(path) if path else pathlib.Path.cwd()
        if path and not project_dir.exists():
            error(f"Folder does not exist: {project_dir}")
            raise typer.Exit(1)
        resolved_project_uid = _read_project_ref_from_env_file(project_dir)

    if resolved_project_uid is None:
        error(
            "Could not determine project uid. Pass PROJECT_UID or ensure "
            "MAIN_SEQUENCE_PROJECT_UID is present in local .env."
        )
        raise typer.Exit(1)

    try:
        payload = sync_project_after_commit(resolved_project_uid, timeout=timeout)
    except ApiError as e:
        error(f"Backend post-commit sync failed: {e}")
        raise typer.Exit(1) from e

    result = payload or {
        "project_uid": resolved_project_uid,
        "detail": "sync_project_after_commit triggered",
    }
    if _emit_json(result):
        return

    success(f"Triggered backend sync for project {resolved_project_uid}.")


@project.command("sync_project", hidden=True)
def project_sync_project(
    message: str = typer.Argument(..., help="Git commit message"),
    project_id: str | None = typer.Argument(None, help="Project UID"),
    path: str | None = typer.Option(None, "--path", help="Project directory"),
):
    """
    Backward-compatible hidden alias for `mainsequence project sync`.
    """
    project_sync(
        message=message,
        project_id=project_id,
        path=path,
        message_opt=None,
        bump="patch",
        no_push=False,
        dry_run=False,
    )


@project.command("build-docker-env")
def project_build_docker_env(
    project_id: str | None = typer.Argument(None, help="Project UID"),
    path: str | None = typer.Option(None, "--path", help="Project directory"),
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
    Build Docker image for project and optionally write devcontainer config.

    Parameters
    ----------
    project_id:
        Project UID to resolve local folder.
    path:
        Explicit local path.
    image_ref:
        Explicit docker image tag/reference.
    devcontainer:
        Write `.devcontainer/devcontainer.json` after build.

    Examples
    --------
    ```bash
    mainsequence project build-docker-env project-uid-123
    mainsequence project build-docker-env --path . --image-ref ghcr.io/acme/proj:dev
    mainsequence project build-docker-env --path . --no-devcontainer
    ```
    """
    project_dir = _resolve_project_dir(project_id, path)
    dockerfile = project_dir / "Dockerfile"
    if not dockerfile.exists():
        error("Dockerfile not found in the project root.")
        raise typer.Exit(1)

    ref = (image_ref or "").strip() or compute_docker_image_ref(project_dir)

    if devcontainer:
        dc_path = write_devcontainer_config(project_dir, ref)
        info(f"Devcontainer updated: {dc_path}")

    with status(f"Building Docker image: {ref}"):
        rc = build_docker_environment(project_dir, ref)

    if rc != 0:
        error(f"Docker build failed (exit {rc}).")
        raise typer.Exit(rc)

    success(f"Docker image built: {ref}")
    info("Next step (VS Code): run 'Dev Containers: Reopen in Container'.")


@project.command("current")
def project_current(
    debug: bool = typer.Option(False, "--debug", help="Show detection debug details"),
):
    """
    Detect and display current project context from current directory.

    Includes detected path, project uid, virtual environment, Python version,
    and SDK status when available.

    Parameters
    ----------
    debug:
        Include detailed detection diagnostics.

    Examples
    --------
    ```bash
    mainsequence project current
    mainsequence project current --debug
    ```
    """
    cfg_obj = cfg.get_config()
    base = cfg_obj["mainsequence_path"]
    cwd = str(pathlib.Path.cwd())

    project_info, dbg = detect_current_project([cwd], base)
    if not project_info:
        warn(f"No MainSequence project detected (reason: {dbg.reason}).")
        if debug and dbg.checks:
            print_kv("Debug", [("checks", json.dumps([c.__dict__ for c in dbg.checks], indent=2))])
        raise typer.Exit(1)

    current_project_payload = {
        "path": project_info.path,
        "folder": project_info.folder,
        "project_uid": project_info.project_uid,
        "legacy_project_id": project_info.project_id,
        "venv_path": project_info.venv_path,
        "python_version": project_info.python_version,
    }

    # SDK status (best-effort)
    req = pathlib.Path(project_info.path) / "requirements.txt"
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
            "hint": "Run: mainsequence project update-sdk --path .  (if differs)",
        }

    debug_payload = None
    if debug and dbg.checks:
        debug_payload = [c.__dict__ for c in dbg.checks]

    if _emit_json(
        {
            "project": current_project_payload,
            "sdk_status": sdk_status_payload,
            "debug": debug_payload,
        }
    ):
        return

    items = [
        ("Path", project_info.path),
        ("Folder", project_info.folder),
        ("Project UID", project_info.project_uid or project_info.project_id or "-"),
        ("Venv", project_info.venv_path or "not found"),
        ("Python", project_info.python_version or "unknown"),
    ]
    print_kv("Current Project", items)

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


@project.command("sdk-status")
def project_sdk_status(
    project_id: str | None = typer.Argument(None, help="Project UID"),
    path: str | None = typer.Option(None, "--path", help="Project directory"),
):
    """
    Show local project SDK version versus latest GitHub release.

    Parameters
    ----------
    project_id:
        Project UID to resolve local folder.
    path:
        Explicit local path.

    Examples
    --------
    ```bash
    mainsequence project sdk-status project-uid-123
    mainsequence project sdk-status --path .
    ```
    """
    project_dir = _resolve_project_dir(project_id, path)
    req = project_dir / "requirements.txt"
    local = read_local_sdk_version(req)
    with status("Checking GitHub for latest SDK..."):
        latest = fetch_latest_sdk_version()

    status_label = "unknown"
    if latest and local and local != "unversioned":
        status_label = (
            "match" if normalize_version(local) == normalize_version(latest) else "differs"
        )

    payload = {
        "project": str(project_dir),
        "latest_github": latest or "unavailable",
        "local_requirements_txt": local if local is not None else "not found",
        "status": status_label,
    }

    if _emit_json(payload):
        return

    print_kv(
        "SDK Status",
        [
            ("Project", payload["project"]),
            ("Latest (GitHub)", payload["latest_github"]),
            ("Local (requirements.txt)", payload["local_requirements_txt"]),
            ("Status", payload["status"]),
        ],
    )


@project.command("update-sdk")
def project_update_sdk(
    project_id: str | None = typer.Argument(None, help="Project UID"),
    path: str | None = typer.Option(None, "--path", help="Project directory"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Print steps but do not execute"),
):
    """
    Upgrade project SDK dependency (`mainsequence`) using `uv`.

    Parameters
    ----------
    project_id:
        Project UID to resolve local folder.
    path:
        Explicit local path.
    dry_run:
        Print update plan without executing.

    Examples
    --------
    ```bash
    mainsequence project update-sdk project-uid-123
    mainsequence project update-sdk --path .
    mainsequence project update-sdk --path . --dry-run
    ```
    """
    project_dir = _resolve_project_dir(project_id, path)
    ensure_venv(project_dir)

    steps = [
        "resolve uv executable",
        "uv lock --upgrade-package mainsequence",
        "uv sync",
    ]
    print_table("Update SDK plan", ["Step"], [[s] for s in steps])

    if dry_run:
        warn("Dry run: no commands executed.")
        return

    uv = ensure_uv_installed(project_dir)
    with status("Upgrading mainsequence SDK via uv..."):
        run_uv(uv, ["lock", "--upgrade-package", "mainsequence"], cwd=project_dir)
        run_uv(uv, ["sync"], cwd=project_dir)

    success("SDK update complete.")


@project.command("update")
def project_update_scaffold_target(
    target: str = typer.Argument(
        ..., help="Scaffold target to update. Currently supported: AGENTS.md"
    ),
    project_id: str | None = typer.Option(
        None, "--project-uid", "--project-id", help="Project UID to resolve local folder"
    ),
    path: str | None = typer.Option(None, "--path", help="Project directory"),
):
    """
    Update a scaffold-managed file in the local project root.

    Currently this command supports only `AGENTS.md`.
    If the Main Sequence managed marker is present, only that block is updated.
    If the marker is absent, the whole file is replaced from the installed
    scaffold template.

    Examples
    --------
    ```bash
    mainsequence project update AGENTS.md
    mainsequence project update AGENTS.md --path .
    mainsequence project update AGENTS.md --project-uid project-uid-123
    ```
    """
    if target != "AGENTS.md":
        error(f"Unsupported scaffold update target: {target}. Supported target: AGENTS.md")
        raise typer.Exit(1)

    project_dir = _resolve_project_dir(project_id, path)
    destination = project_dir / "AGENTS.md"

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
        "project": project_dir,
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
            ("Project", str(project_dir)),
            ("Source", str(source)),
            ("Destination", str(destination)),
        ],
    )


@project.command("update_agent_skills")
@project.command("update-agent-skills", hidden=True)
def project_update_agent_skills(
    project_id: str | None = typer.Option(
        None, "--project-uid", "--project-id", help="Project UID to resolve local folder"
    ),
    path: str | None = typer.Option(None, "--path", help="Project directory"),
):
    """
    Update `.agents/skills/mainsequence` from the installed `agent_scaffold/skills` bundle subtree.

    This copies every top-level scaffold skill folder from `agent_scaffold/skills/`
    into `.agents/skills/mainsequence/`, overwriting any folders with the same name
    under that namespace. Bundle-root files such as `AGENTS.md` are not copied by
    this command.

    Examples
    --------
    ```bash
    mainsequence project update_agent_skills
    mainsequence project update_agent_skills --path .
    mainsequence project update_agent_skills --project-uid project-uid-123
    ```
    """
    project_dir = _resolve_project_dir(project_id, path)

    scaffold_bundle_dir = _project_agent_scaffold_bundle_dir(project_dir)
    skills_dir = scaffold_bundle_dir / "skills"
    if not skills_dir.exists() or not skills_dir.is_dir():
        error(f"Project-installed agent_scaffold bundle is missing skills/: {skills_dir}")
        raise typer.Exit(1)

    pinned_version = _project_installed_package_version(project_dir, "mainsequence")
    source_checkout_root = _mainsequence_source_checkout_root()
    protected_project_roots = (source_checkout_root,) if source_checkout_root is not None else ()
    try:
        copy_result = copy_scaffold_skills(
            project_dir=project_dir,
            library_name="mainsequence",
            namespace="mainsequence",
            skills_path=skills_dir,
            pinned_version=pinned_version,
            command="mainsequence project update_agent_skills",
            protected_project_roots=protected_project_roots,
        )
    except (ScaffoldSkillCopyBlocked, FileNotFoundError, ValueError) as exc:
        error(str(exc))
        raise typer.Exit(1) from exc

    updated = [
        {
            "name": item.name,
            "source": item.source,
            "destination": item.destination,
        }
        for item in copy_result.copied
    ]

    payload = {
        "project": project_dir,
        "library_name": copy_result.library_name,
        "namespace": copy_result.namespace,
        "skills_path": copy_result.skills_path,
        "destination_root": copy_result.destination_root,
        "sentinel_path": copy_result.sentinel_path,
        "pinned_version": copy_result.pinned_version,
        "updated_count": len(updated),
        "updated": updated,
    }
    if _emit_json(payload):
        return

    success("Updated .agents/skills/mainsequence from installed agent_scaffold bundle.")
    print_kv(
        "Scaffold Skill Pin",
        [
            ("Library", copy_result.library_name),
            ("Namespace", copy_result.namespace),
            ("Pinned Version", copy_result.pinned_version),
            ("Sentinel", str(copy_result.sentinel_path)),
        ],
    )
    print_table(
        "Updated Scaffold Skills",
        ["Skill Folder", "Destination"],
        [[item["name"], str(item["destination"])] for item in updated],
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
        help="Optional installed skill name, for example project_builder or command_center/workspace_builder",
    ),
):
    """
    Print the installed scaffold skills path or one installed `SKILL.md` path.

    When no skill name is provided, this prints the installed `agent_scaffold/skills`
    directory for the current CLI installation.

    When a skill name is provided, it may be the full relative skill path such as
    `command_center/workspace_builder` or, when unique, by its leaf folder name.

    Examples
    --------
    ```bash
    mainsequence skills path
    mainsequence skills path project_builder
    mainsequence skills path command_center/workspace_builder
    mainsequence skills path workspace_builder
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
