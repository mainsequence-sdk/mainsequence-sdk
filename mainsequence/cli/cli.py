from __future__ import annotations

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
- docker scaffold creation during set-up-locally when DEFAULT_BASE_IMAGE exists
- project current (detect current project + venv/python info)
- sdk latest + project sdk-status + project update-sdk
- doctor diagnostics

All commands have docstrings so `--help` is useful.
"""

import datetime
import json
import os
import pathlib
import platform
import re
import shutil
import subprocess
import sys
import tempfile
import time
from decimal import ROUND_UP, Decimal
from textwrap import dedent

import typer
import yaml

from ..compute_validation import decimal_to_storage, parse_cpu_request, parse_memory_request
from . import config as cfg
from .api import (
    ApiError,
    NotLoggedIn,
    add_deploy_key,
    create_project,
    create_project_image,
    create_project_job,
    create_project_resource_release,
    deep_find_repo_url,
    delete_project,
    delete_project_image,
    delete_resource_release,
    fetch_project_env_text,
    get_current_user_profile,
    get_logged_user_details,
    get_market_asset_translation_table,
    get_project,
    get_project_data_node_updates,
    get_project_image,
    get_project_job_run_logs,
    get_projects,
    get_resource_release,
    list_dynamic_table_data_sources,
    list_github_organizations,
    list_market_asset_translation_tables,
    list_market_portfolios,
    list_project_base_images,
    list_project_images,
    list_project_job_runs,
    list_project_jobs,
    list_project_resources,
    repo_name_from_git_url,
    run_project_job,
    safe_slug,
    schedule_batch_project_jobs,
    sync_project_after_commit,
)
from .api import login as api_login
from .docker_utils import (
    build_docker_environment,
    compute_docker_image_ref,
    ensure_docker_scaffold,
    extract_env_value,
    resolve_base_image,
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

app = typer.Typer(help="MainSequence CLI (login + project operations)")

markets = typer.Typer(help="Markets commands")
markets_portfolios_group = typer.Typer(help="Markets portfolio commands")
markets_asset_translation_table_group = typer.Typer(help="Markets asset translation table commands")
project = typer.Typer(help="Project commands (remote + local operations)")
project_list_group = typer.Typer(help="List-related project commands")
project_project_resource_group = typer.Typer(help="Project resource commands")
project_data_node_updates_group = typer.Typer(help="Project data node update commands")
project_images_group = typer.Typer(help="Project image commands")
project_jobs_group = typer.Typer(help="Project job commands")
project_job_runs_group = typer.Typer(help="Project job run commands")
settings = typer.Typer(help="Settings (base folder, backend, etc.)")
sdk = typer.Typer(help="SDK utilities (latest version, status)")

app.add_typer(markets, name="markets")
markets.add_typer(markets_portfolios_group, name="portfolios")
markets.add_typer(markets_asset_translation_table_group, name="asset-translation-table")
app.add_typer(project, name="project")
project.add_typer(project_list_group, name="list")
project.add_typer(project_project_resource_group, name="project_resource")
project.add_typer(project_data_node_updates_group, name="data-node-updates")
project.add_typer(project_images_group, name="images")
project.add_typer(project_jobs_group, name="jobs")
project_jobs_group.add_typer(project_job_runs_group, name="runs")
app.add_typer(settings, name="settings")
app.add_typer(sdk, name="sdk")

JOB_DEFAULT_CPU_REQUEST = Decimal("0.25")
JOB_DEFAULT_MEMORY_REQUEST = Decimal("0.5")
JOB_MEMORY_PER_CPU_MAX = Decimal("6.5")
JOB_DEFAULT_SPOT = False
JOB_DEFAULT_MAX_RUNTIME_SECONDS = 86400
JOB_ALLOWED_INTERVAL_PERIODS = ("seconds", "minutes", "hours", "days")
JOB_MODEL_REF = "mainsequence.client.models_helpers.Job"
INTERVAL_SCHEDULE_MODEL_REF = "mainsequence.client.models_helpers.IntervalSchedule"
CRONTAB_SCHEDULE_MODEL_REF = "mainsequence.client.models_helpers.CrontabSchedule"
JOB_RUN_MODEL_REF = "mainsequence.client.models_helpers.JobRun"
JOB_RUN_STATUS_PENDING = "PENDING"
JOB_RUN_STATUS_RUNNING = "RUNNING"
RESOURCE_RELEASE_RESOURCE_TYPE_MAP = {
    "streamlit_dashboard": "dashboard",
    "agent": "agent",
}


# ---------- AI instructions utilities (kept) ----------

INSTR_REL_PATH = pathlib.Path("examples") / "ai" / "instructions"


def _mainsequence_ascii_banner() -> str:
    return dedent(
        r"""
         __  __       _         ____                                 
        |  \/  | __ _(_)_ __   / ___|  ___  __ _ _   _  ___ _ __   ___
        | |\/| |/ _` | | '_ \  \___ \ / _ \/ _` | | | |/ _ \ '_ \ / __|
        | |  | | (_| | | | | |  ___) |  __/ (_| | |_| |  __/ | | | (__ 
        |_|  |_|\__,_|_|_| |_| |____/ \___|\__, |\__,_|\___|_| |_|\___|
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
            ok1 = subprocess.run(["wl-copy"], input=txt, text=True, capture_output=True).returncode == 0
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
    base = pathlib.Path(instructions_dir).expanduser().resolve() if instructions_dir else _find_instructions_dir()
    if not base or not base.is_dir():
        raise RuntimeError("Instructions folder not found. Pass --dir PATH or run from inside your repo.")

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
    return re.sub(r"[^a-z0-9-_]+", "-", name.lower()).strip("-") or "default"


def _determine_repo_url(p: dict) -> str:
    repo = (p.get("git_ssh_url") or "").strip()
    if repo.lower() == "none":
        repo = ""
    if not repo:
        extra = (p.get("data_source") or {}).get("related_resource", {}) or {}
        extra = extra.get("extra_arguments") or (p.get("data_source") or {}).get("extra_arguments") or {}
        repo = deep_find_repo_url(extra) or ""
    return repo


def _find_local_dir_by_id(base_dir: str, org_slug: str, project_id: int | str, project_name: str | None = None) -> str | None:
    """
    Find local folder for a project id.

    Parity note:
    VS Code extension considers folders ending in '-<id>' as local even if .env is missing.
    We match that behavior (and still keep a legacy fallback).
    """
    pid = str(project_id).strip()
    try:
        pid = str(int(pid))
    except Exception:
        pass
    suffix = f"-{pid}"

    root = _projects_root(base_dir, org_slug)
    if root.exists():
        # Prefer CWD hints
        try:
            cwd = pathlib.Path.cwd().resolve()
            for parent in [cwd] + list(cwd.parents):
                try:
                    parent.relative_to(root)
                except Exception:
                    continue
                if parent.is_dir() and parent.name.endswith(suffix):
                    return str(parent)
        except Exception:
            pass

        # canonical if name provided
        if project_name:
            slug = safe_slug(project_name)
            cand = root / f"{slug}-{pid}"
            if cand.is_dir():
                return str(cand)

        # scan root
        try:
            for d in root.iterdir():
                if d.is_dir() and d.name.endswith(suffix):
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
    """Return an aligned table with Local status + path."""

    def ds(obj, path, default=""):
        try:
            for k in path.split("."):
                obj = obj.get(k, {})
            return obj or default
        except Exception:
            return default

    rows = []
    for p in items:
        pid = str(p.get("id", ""))
        name = p.get("project_name") or "(unnamed)"
        dname = ds(p, "data_source.related_resource.display_name", "")
        klass = ds(
            p,
            "data_source.related_resource.class_type",
            ds(p, "data_source.related_resource_class_type", ""),
        )
        status_ = ds(p, "data_source.related_resource.status", "")

        local_path = _find_local_dir_by_id(base_dir, org_slug, pid, name)
        local = "Local" if local_path else "-"
        path_col = local_path or "-"
        rows.append((pid, name, dname, klass, status_, local, path_col))

    header = ["ID", "Project", "Data Source", "Class", "Status", "Local", "Path"]
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
    except NotLoggedIn:
        error("Not logged in. Run: mainsequence login <email>")
        raise typer.Exit(1)
    except ApiError:
        error("Not logged in. Run: mainsequence login <email>")
        raise typer.Exit(1)


def _resolve_project_dir(project_id: int | None, path: str | None) -> pathlib.Path:
    """
    Resolve project directory by:
      - explicit --path, or
      - current working directory when local `.env` exposes `MAIN_SEQUENCE_PROJECT_ID`, or
      - scanning base projects root for '-<id>' suffix

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

    found = _find_local_dir_by_id(base, org_slug, project_id, None)
    if not found:
        error("No local folder mapped for this project. Run `mainsequence project set-up-locally <id>` first.")
        raise typer.Exit(1)

    p = pathlib.Path(found)
    if not p.exists():
        error(f"Folder missing: {p}")
        raise typer.Exit(1)
    return p


def _read_project_id_from_env_file(project_dir: pathlib.Path) -> int | None:
    """
    Read `MAIN_SEQUENCE_PROJECT_ID` from `<project_dir>/.env` when available.
    """
    env_path = project_dir / ".env"
    if not env_path.is_file():
        return None
    try:
        content = env_path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return None

    match = re.search(r"(?m)^MAIN_SEQUENCE_PROJECT_ID=(\d+)\s*$", content)
    if not match:
        return None

    try:
        return int(match.group(1))
    except ValueError:
        return None


def _resolve_current_project_dir_from_env() -> pathlib.Path:
    """
    Resolve the current working directory as a project folder when local `.env`
    declares `MAIN_SEQUENCE_PROJECT_ID`.
    """
    cwd = pathlib.Path.cwd()
    if _read_project_id_from_env_file(cwd) is None:
        error(
            "No PROJECT_ID was provided and the current directory does not expose "
            "MAIN_SEQUENCE_PROJECT_ID in .env."
        )
        raise typer.Exit(1)
    return cwd


def _resolve_project_id_from_local_env(path: str | None = None) -> int:
    """
    Resolve project id from `<path>/.env` or `./.env`.
    """
    project_dir = normalize_path(path) if path else pathlib.Path.cwd()
    project_id = _read_project_id_from_env_file(project_dir)
    if project_id is None:
        error(f"Could not determine project id from {project_dir / '.env'}.")
        raise typer.Exit(1)
    return project_id


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
    project_id: int,
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
        if existing_image_ref not in (None, "") and str(existing_image_ref) != str(related_image_id):
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
    info(
        f"Using project image {related_image_id} for all {len(jobs_config)} job(s) in this batch."
    )
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
    result = _git_run(project_dir, ["rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{upstream}"])
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
        refs_result = _git_run(project_dir, ["for-each-ref", "--format=%(refname:short)", "refs/remotes"])
        if refs_result.returncode == 0:
            refs = [
                line.strip()
                for line in (refs_result.stdout or "").splitlines()
                if line.strip() and not line.strip().endswith("/HEAD")
            ]

    if not refs:
        raise RuntimeError("No pushed commits found. Configure a remote and push at least one commit first.")

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
        raise RuntimeError("No pushed commits found. Push at least one commit before creating an image.")
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


def _format_portfolio_label(portfolio: dict) -> str:
    index_asset = portfolio.get("index_asset")
    if isinstance(index_asset, dict):
        unique_identifier = str(index_asset.get("unique_identifier") or "").strip()
        name = str(index_asset.get("name") or "").strip()
        if name and unique_identifier and name != unique_identifier:
            return f"{name} ({unique_identifier})"
        if name:
            return name
        if unique_identifier:
            return unique_identifier
        if index_asset.get("id") is not None:
            return str(index_asset.get("id"))
    elif index_asset is not None:
        return str(index_asset)

    return str(portfolio.get("id") or "-")


def _format_asset_filter_summary(asset_filter) -> str:
    if not isinstance(asset_filter, dict):
        return str(asset_filter or "All assets")

    parts: list[str] = []
    security_type = str(asset_filter.get("security_type") or "").strip()
    if security_type:
        parts.append(f"security_type={security_type}")

    market_sector = str(asset_filter.get("security_market_sector") or "").strip()
    if market_sector:
        parts.append(f"market_sector={market_sector}")

    if asset_filter.get("open_for_everyone") is True:
        parts.append("open_for_everyone=true")

    return ", ".join(parts) if parts else "All assets"


def _format_asset_translation_target(rule: dict) -> str:
    target = str(rule.get("markets_time_serie_unique_identifier") or "-")
    exchange = str(rule.get("target_exchange_code") or "").strip()
    column = str(rule.get("default_column_name") or "").strip()

    if exchange:
        target = f"{target} @ {exchange}"
    if column:
        target = f"{target} ({column})"
    return target


def _format_asset_translation_rules_preview(rules, *, limit: int = 2) -> str:
    if not isinstance(rules, list) or not rules:
        return "-"

    previews: list[str] = []
    for rule in rules[:limit]:
        if not isinstance(rule, dict):
            previews.append(str(rule))
            continue
        previews.append(
            f"{_format_asset_filter_summary(rule.get('asset_filter'))} => "
            f"{_format_asset_translation_target(rule)}"
        )

    if len(rules) > limit:
        previews.append(f"+{len(rules) - limit} more")

    return "; ".join(previews)


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
            pydantic_prompt_text(JOB_MODEL_REF, "task_schedule", optional=True, extra_hint="create now?"),
            default=False,
        ):
            return None
        inferred_type = typer.prompt(
            pydantic_prompt_text(
                INTERVAL_SCHEDULE_MODEL_REF,
                "type",
                extra_hint="interval/crontab",
            ),
            default="interval",
        ).strip().lower()

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
            raise ValueError("schedule_every and schedule_period are only valid for interval schedules.")
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
        schedule_start_time = typer.prompt(
            pydantic_prompt_text(CRONTAB_SCHEDULE_MODEL_REF, "start_time", optional=True),
            default="",
        ).strip() or None

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

    poetry_section = re.search(r"(?is)^\s*\[tool\.poetry\.dependencies\]\s*(.*?)(?:^\s*\[|\Z)", pyproject_text, re.MULTILINE)
    if poetry_section:
        py_match = re.search(r'(?im)^\s*python\s*=\s*["\']([^"\']+)["\']\s*$', poetry_section.group(1))
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
        raise RuntimeError("JWT session tokens are missing. Run: mainsequence login <email>")
    return access_token, refresh_token


def _render_project_runtime_env_text(
    env_text: str,
    *,
    access_token: str,
    refresh_token: str,
    backend_url: str,
    project_runtime_id: str | None = None,
) -> str:
    """
    Return `.env` text with managed runtime auth keys refreshed.

    Managed keys are rewritten from scratch to avoid duplicate stale entries.
    """
    managed_prefixes = (
        "MAINSEQUENCE_TOKEN=",
        "MAINSEQUENCE_ACCESS_TOKEN=",
        "MAINSEQUENCE_REFRESH_TOKEN=",
        "TDAG_ENDPOINT=",
    ) + (("MAIN_SEQUENCE_PROJECT_ID=",) if project_runtime_id is not None else ())
    lines = [
        ln
        for ln in (env_text or "").replace("\r", "").splitlines()
        if not any(ln.startswith(prefix) for prefix in managed_prefixes)
    ]

    if lines and lines[-1] != "":
        lines.append("")

    lines.extend(
        [
            f"MAINSEQUENCE_ACCESS_TOKEN={access_token}",
            f"MAINSEQUENCE_REFRESH_TOKEN={refresh_token}",
            f"TDAG_ENDPOINT={backend_url}",
        ]
        + ([f"MAIN_SEQUENCE_PROJECT_ID={project_runtime_id}"] if project_runtime_id is not None else [])
    )

    final_env = "\n".join(lines).replace("\r", "")
    return final_env + ("\n" if not final_env.endswith("\n") else "")


# ---------- top-level commands ----------


@app.command()
def login(
    email: str = typer.Argument(..., help="Email/username (server expects 'email' field)"),
    backend: str | None = typer.Argument(
        None,
        help="Optional backend URL or host[:port], for example 127.0.0.1:8000",
    ),
    projects_base: str | None = typer.Argument(
        None,
        help="Optional local projects base folder, for example mainsequence-dev",
    ),
    password: str | None = typer.Option(None, prompt=True, hide_input=True, help="Password"),
    no_status: bool = typer.Option(False, "--no-status", help="Do not print projects table after login"),
    export: bool = typer.Option(
        False,
        "--export",
        "--export-env",
        help="Print shell export commands for session auth variables.",
    ),
):
    """
    Authenticate to the MainSequence platform.

    Persists auth tokens in secure OS storage (when available) so subsequent
    CLI invocations can run without re-authentication. Backend/base-folder
    overrides passed to `login` are scoped to the current terminal session.

    Parameters
    ----------
    email:
        Login email.
    backend:
        Optional backend override for this login. It applies only to the current terminal session.
    projects_base:
        Optional projects base folder for the current terminal session. A bare
        name like `mainsequence-dev` maps to `~/mainsequence-dev`.
    password:
        Password (prompted if omitted).
    no_status:
        If True, skip printing the project table after login.
    export:
        If True, print shell export lines instead of storing auth state.

    Examples
    --------
    ```bash
    mainsequence login you@company.com
    mainsequence login you@company.com 127.0.0.1:8000 mainsequence-dev
    mainsequence login you@company.com --no-status
    mainsequence login you@company.com --export
    ```
    """
    current_backend = cfg.backend_url()
    current_projects_base = cfg.normalize_mainsequence_path(cfg.get_config().get("mainsequence_path"))
    normalized_backend = cfg.normalize_backend_url(backend) if backend else None
    normalized_projects_base = cfg.normalize_mainsequence_path(projects_base) if projects_base else None

    if normalized_backend and normalized_backend != current_backend:
        if not projects_base:
            error("When using a different backend, you must also specify a different projects base folder.")
            raise typer.Exit(1)
        if normalized_projects_base == current_projects_base:
            error("When using a different backend, the projects base folder must differ from the current one.")
            raise typer.Exit(1)

    previous_backend_override = os.environ.get("MAIN_SEQUENCE_BACKEND_URL")
    if normalized_backend:
        os.environ["MAIN_SEQUENCE_BACKEND_URL"] = normalized_backend

    try:
        res = api_login(email, password)
    except ApiError as e:
        error(f"Login failed: {e}")
        raise typer.Exit(1)
    finally:
        if normalized_backend:
            if previous_backend_override is None:
                os.environ.pop("MAIN_SEQUENCE_BACKEND_URL", None)
            else:
                os.environ["MAIN_SEQUENCE_BACKEND_URL"] = previous_backend_override

    if normalized_backend or projects_base:
        cfg.set_session_overrides(
            backend_url=normalized_backend,
            mainsequence_path=projects_base,
        )
    else:
        cfg.clear_session_overrides()

    if export:
        access = (res.get("access") or "").replace('"', '\\"')
        refresh = (res.get("refresh") or "").replace('"', '\\"')
        username = (res.get("username") or "").replace('"', '\\"')
        typer.echo(f'export MAINSEQUENCE_ACCESS_TOKEN="{access}"')
        typer.echo(f'export MAINSEQUENCE_REFRESH_TOKEN="{refresh}"')
        typer.echo(f'export MAINSEQUENCE_USERNAME="{username}"')
        return

    cfg_obj = cfg.get_config()
    base = cfg_obj["mainsequence_path"]
    typer.echo(_mainsequence_ascii_banner())
    typer.echo("MAIN SEQUENCE")
    success(f"Signed in as {res['username']} (Backend: {res['backend']})")
    info(f"Projects base folder: {base}")
    if cfg.secure_store_available():
        if res.get("persisted", True):
            info("Auth tokens are persisted in secure OS storage for subsequent CLI commands.")
        else:
            warn("Could not persist auth tokens in secure OS storage. Use --export for shell-based auth.")
    else:
        warn("Secure token storage is unavailable on this platform. Use --export for shell-based auth.")

    if not no_status:
        try:
            items = get_projects()
            org_slug = _org_slug_from_profile()
            typer.echo("\nProjects:")
            typer.echo(_render_projects_table(items, base, org_slug))
        except NotLoggedIn:
            error("Not logged in. Run: mainsequence login <email>")


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
    except NotLoggedIn:
        error("Not logged in. Run: mainsequence login <email>")
        raise typer.Exit(1)
    except ApiError as e:
        error(str(e))
        raise typer.Exit(1)

    organization = user.get("organization")
    if isinstance(organization, dict):
        organization_name = str(organization.get("name") or organization.get("id") or "-")
    else:
        organization_name = str(organization or "-")

    print_kv(
        "MainSequence User",
        [
            ("ID", str(user.get("id") or "-")),
            ("Username", str(user.get("username") or "-")),
            ("Email", str(user.get("email") or "-")),
            ("Organization", organization_name),
            ("Active", str(user.get("is_active") if user.get("is_active") is not None else "-")),
            ("Verified", str(user.get("is_verified") if user.get("is_verified") is not None else "-")),
            ("MFA Enabled", str(user.get("mfa_enabled") if user.get("mfa_enabled") is not None else "-")),
            ("Date Joined", str(user.get("date_joined") or "-")),
            ("Last Login", str(user.get("last_login") or "-")),
        ],
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
    print_: bool = typer.Option(False, "--print", help="Print the bundle to stdout instead of copying."),
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
                error("Instructions folder not found. Pass --dir PATH or run from inside your repo.")
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
        raise typer.Exit(1)


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
    success(f"Projects base folder set to: {out['mainsequence_path']}")


@settings.command("set-backend")
def settings_set_backend(
    url: str = typer.Argument(..., help="Backend base URL, e.g. https://main-sequence.app")
):
    """
    Set backend base URL used by CLI API calls.

    Parameters
    ----------
    url:
        Backend base URL (for example `https://main-sequence.app`).

    Examples
    --------
    ```bash
    mainsequence settings set-backend https://main-sequence.app
    ```
    """
    out = cfg.set_backend_url(url)
    success(f"Backend URL set to: {out.get('backend_url')}")


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
            raise typer.Exit(1)

    if v:
        success(f"Latest SDK (GitHub): {v}")
    else:
        warn("Latest SDK version unavailable.")


# ---------- markets group ----------


def _markets_portfolios_list_impl(timeout: int | None) -> None:
    _require_login()

    try:
        portfolios = list_market_portfolios(timeout=timeout)
    except ApiError as e:
        error(f"Markets portfolios fetch failed: {e}")
        raise typer.Exit(1)

    rows: list[list[str]] = []
    for portfolio in portfolios:
        rows.append(
            [
                str(portfolio.get("id") or "-"),
                _format_portfolio_label(portfolio),
                _format_nested_summary(
                    portfolio.get("calendar"),
                    preferred_fields=("name", "display_name", "id"),
                ),
                _format_nested_summary(
                    portfolio.get("data_node_update"),
                    preferred_fields=("update_hash", "id"),
                ),
                _format_nested_summary(
                    portfolio.get("signal_data_node_update"),
                    preferred_fields=("update_hash", "id"),
                ),
            ]
        )

    if rows:
        print_table(
            "Markets Portfolios",
            ["ID", "Portfolio", "Calendar", "Data Node Update", "Signal Update"],
            rows,
        )
    else:
        info("No markets portfolios.")
    info(f"Total portfolios: {len(portfolios)}")


@markets_portfolios_group.command("list")
def markets_portfolios_list_cmd(
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    List markets portfolios visible to the authenticated user.

    Uses SDK client `Portfolio.filter()` as the single source of truth.

    Parameters
    ----------
    timeout:
        Request timeout in seconds.

    Examples
    --------
    ```bash
    mainsequence markets portfolios list
    mainsequence markets portfolios list --timeout 60
    ```
    """
    _markets_portfolios_list_impl(timeout=timeout)


def _markets_asset_translation_table_list_impl(timeout: int | None) -> None:
    _require_login()

    try:
        tables = list_market_asset_translation_tables(timeout=timeout)
    except ApiError as e:
        error(f"Markets asset translation tables fetch failed: {e}")
        raise typer.Exit(1)

    rows: list[list[str]] = []
    for table in tables:
        rules = table.get("rules")
        rule_count = len(rules) if isinstance(rules, list) else 0
        rows.append(
            [
                str(table.get("id") or "-"),
                str(table.get("unique_identifier") or "-"),
                str(rule_count),
                _format_asset_translation_rules_preview(rules),
            ]
        )

    if rows:
        print_table(
            "Markets Asset Translation Tables",
            ["ID", "Unique Identifier", "Rules", "Mappings"],
            rows,
        )
    else:
        info("No markets asset translation tables.")
    info(f"Total asset translation tables: {len(tables)}")


def _markets_asset_translation_table_detail_impl(table_id: int, timeout: int | None) -> None:
    _require_login()

    try:
        table = get_market_asset_translation_table(table_id, timeout=timeout)
    except ApiError as e:
        error(f"Markets asset translation table fetch failed: {e}")
        raise typer.Exit(1)

    rules = table.get("rules")
    rule_count = len(rules) if isinstance(rules, list) else 0
    print_kv(
        "Markets Asset Translation Table",
        [
            ("ID", str(table.get("id") or table_id)),
            ("Unique Identifier", str(table.get("unique_identifier") or "-")),
            ("Rules", str(rule_count)),
        ],
    )

    if not isinstance(rules, list) or not rules:
        info("No translation rules.")
        return

    rows: list[list[str]] = []
    for rule in rules:
        rule_dict = rule if isinstance(rule, dict) else {}
        rows.append(
            [
                str(rule_dict.get("id") or "-"),
                _format_asset_filter_summary(rule_dict.get("asset_filter")),
                _format_asset_translation_target(rule_dict),
                str(rule_dict.get("target_exchange_code") or "-"),
                str(rule_dict.get("default_column_name") or "-"),
            ]
        )

    print_table(
        "Rules",
        ["Rule ID", "Matches", "Maps To", "Exchange", "Column"],
        rows,
    )


@markets_asset_translation_table_group.command("list")
def markets_asset_translation_table_list_cmd(
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    List markets asset translation tables visible to the authenticated user.

    Uses SDK client `AssetTranslationTable.filter()` as the single source of truth.

    Parameters
    ----------
    timeout:
        Request timeout in seconds.

    Examples
    --------
    ```bash
    mainsequence markets asset-translation-table list
    mainsequence markets asset-translation-table list --timeout 60
    ```
    """
    _markets_asset_translation_table_list_impl(timeout=timeout)


@markets_asset_translation_table_group.command("detail")
def markets_asset_translation_table_detail_cmd(
    table_id: int = typer.Argument(..., help="Asset translation table ID."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Show one markets asset translation table and render its rules in a readable terminal table.

    Uses SDK client `AssetTranslationTable.get()` as the single source of truth.

    Parameters
    ----------
    table_id:
        Asset translation table ID.
    timeout:
        Request timeout in seconds.

    Examples
    --------
    ```bash
    mainsequence markets asset-translation-table detail 12
    mainsequence markets asset-translation-table detail 12 --timeout 60
    ```
    """
    _markets_asset_translation_table_detail_impl(table_id=table_id, timeout=timeout)


# ---------- project group ----------


@project_list_group.callback(invoke_without_command=True)
def project_list(ctx: typer.Context):
    """
    List projects visible to the authenticated user.

    The output includes remote metadata and local mapping status.

    Examples
    --------
    ```bash
    mainsequence project list
    ```
    """
    if ctx.invoked_subcommand is not None:
        return

    _require_login()
    cfg_obj = cfg.get_config()
    base = cfg_obj["mainsequence_path"]
    org_slug = _org_slug_from_profile()
    items = get_projects()
    typer.echo(_render_projects_table(items, base, org_slug))


def _print_project_data_node_updates(
    project_id: int | None = typer.Argument(None, help="Project ID"),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
) -> None:
    """
    List data node updates for a project.

    Uses SDK client `Project.get_data_nodes_updates()` as the single source of truth
    for payload parsing and shape handling.

    Parameters
    ----------
    project_id:
        Platform project ID. If omitted, resolve it from `MAIN_SEQUENCE_PROJECT_ID` in `./.env`.
    timeout:
        Optional request timeout in seconds.

    Examples
    --------
    ```bash
    mainsequence project data-node-updates list
    mainsequence project data-node-updates list 123
    mainsequence project data-node-updates list 123 --timeout 60
    ```
    """
    if project_id is None:
        project_id = _resolve_project_id_from_local_env()

    _require_login()
    try:
        updates = get_project_data_node_updates(project_id, timeout=timeout)
    except NotLoggedIn:
        error("Not logged in. Run: mainsequence login <email>")
        raise typer.Exit(1)
    except ApiError as e:
        error(str(e))
        raise typer.Exit(1)

    if not updates:
        info("No data node updates found.")
        return

    rows: list[list[str]] = []
    for u in updates:
        storage = u.get("data_node_storage")
        if isinstance(storage, dict):
            storage_value = storage.get("storage_hash") or storage.get("id") or "-"
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
    project_id: int | None = typer.Argument(None, help="Project ID"),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    List data node updates for a project.

    Examples
    --------
    ```bash
    mainsequence project data-node-updates list
    mainsequence project data-node-updates list 123
    mainsequence project data-node-updates list 123 --timeout 60
    ```
    """
    _print_project_data_node_updates(project_id=project_id, timeout=timeout)


@project_list_group.command("data_nodes_updates", hidden=True)
def project_list_data_nodes_updates_cmd(
    project_id: int | None = typer.Argument(None, help="Project ID"),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Backward-compatible alias for `mainsequence project data-node-updates list`.
    """
    _print_project_data_node_updates(project_id=project_id, timeout=timeout)


@project.command("get-data-node-updates", hidden=True)
def project_get_data_node_updates_cmd(
    project_id: int | None = typer.Argument(None, help="Project ID"),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Backward-compatible alias for `mainsequence project data-node-updates list`.
    """
    _print_project_data_node_updates(project_id=project_id, timeout=timeout)


@project.command("create")
def project_create_cmd(
    project_name: str | None = typer.Argument(None, help="Project name"),
    data_source_id: int | None = typer.Option(None, "--data-source-id", help="Dynamic table data source ID"),
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
                ds_details = f"class={rr.get('class_type') or '-'}, status={rr.get('status') or '-'}"
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
                warn("No GitHub organizations available. Project will be created without github_org_id.")

        branch = (branch or "").strip() or typer.prompt("Repository branch", default="main").strip() or "main"

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
                raise typer.Exit(1)

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
        raise typer.Exit(1)
    except RuntimeError as e:
        error(str(e))
        raise typer.Exit(1)

    pid = created.get("id")
    success(f"Project created: {created.get('project_name') or project_name} (id={pid})")

    # A freshly created project can take several minutes to initialize on backend.
    # Keep polling until API reports is_initialized=True.
    if pid is not None and created.get("is_initialized") is False:
        info("Project is still initializing. Waiting until is_initialized=true (poll every 30s).")
        attempt = 0
        try:
            while True:
                attempt += 1
                with status(f"Project not ready yet (attempt {attempt}). Next check in 30s..."):
                    time.sleep(30)
                try:
                    latest = get_project(pid)
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
            ("ID", str(created.get("id", "-"))),
            ("Project Name", str(created.get("project_name") or project_name)),
            ("Git SSH URL", str(created.get("git_ssh_url") or "-")),
            ("Branch", branch),
        ],
    )
    if pid is not None:
        info(f"Next: mainsequence project set-up-locally {pid}")


@project.command("delete")
def project_delete_remote_cmd(
    project_id: int = typer.Argument(..., help="Project ID"),
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
        Platform project ID.
    delete_repositories:
        Also delete linked repositories on backend workflow.
    yes:
        Skip interactive confirmation.

    Examples
    --------
    ```bash
    mainsequence project delete 123
    mainsequence project delete 123 --yes
    mainsequence project delete 123 --delete-repositories --yes
    ```
    """
    _require_login()

    project_name = f"project-{project_id}"
    try:
        items = get_projects()
        found = next((x for x in items if str(x.get("id")) == str(project_id)), None)
        if found and found.get("project_name"):
            project_name = str(found.get("project_name"))
    except Exception:
        # Best-effort metadata lookup only.
        pass

    if not yes:
        warning = (
            f"This will permanently delete project '{project_name}' (id={project_id}) from the platform.\n"
            "This action cannot be undone."
        )
        if delete_repositories:
            warning += "\nLinked repositories will also be deleted."
        if not typer.confirm(f"{warning}\n\nContinue?", default=False):
            info("Cancelled.")
            raise typer.Exit(0)

    try:
        resp = delete_project(project_id, delete_repositories=delete_repositories)
    except NotLoggedIn:
        error("Not logged in. Run: mainsequence login <email>")
        raise typer.Exit(1)
    except ApiError as e:
        error(f"Project deletion failed: {e}")
        raise typer.Exit(1)

    success(f"Project deleted: {project_name} (id={project_id})")
    if isinstance(resp, dict) and resp:
        detail = resp.get("detail") or resp.get("message")
        if detail:
            info(str(detail))


def _project_resources_list_impl(
    project_id: int | None,
    path: str | None,
    timeout: int | None,
) -> None:
    _require_login()

    project_dir = _resolve_project_dir(project_id, path)
    if project_id is None:
        project_id = _resolve_project_id_from_local_env(str(project_dir))

    try:
        upstream, repo_commit_sha = _get_remote_branch_head_commit(project_dir)
    except RuntimeError as e:
        error(str(e))
        raise typer.Exit(1)

    try:
        resources = list_project_resources(
            project_id=project_id,
            repo_commit_sha=repo_commit_sha,
            timeout=timeout,
        )
    except ApiError as e:
        error(f"Project resources fetch failed: {e}")
        raise typer.Exit(1)

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
    project_id: int | None = typer.Argument(None, help="Project ID. Defaults to local .env when omitted."),
    path: str | None = typer.Option(None, "--path", help="Project repository path (default: current project)"),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    List project resources for the current project at the head commit of the remote branch.

    Uses SDK client `ProjectResource.filter()` as the single source of truth and always applies
    the standard `repo_commit_sha` filter resolved from the current upstream branch head.

    Parameters
    ----------
    project_id:
        Platform project ID. Defaults to local `.env`.
    path:
        Local project path. Used when resolving project id and remote branch head commit.
    timeout:
        Request timeout in seconds.

    Examples
    --------
    ```bash
    mainsequence project project_resource list
    mainsequence project project_resource list 123
    mainsequence project project_resource list --path .
    ```
    """
    _project_resources_list_impl(project_id=project_id, path=path, timeout=timeout)


def _project_resource_release_create_impl(
    *,
    release_kind: str,
    project_id: int | None,
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
        raise typer.Exit(1)

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
        raise typer.Exit(1)

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

    resource_ids = {str(resource.get("id")) for resource in resources if resource.get("id") is not None}
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
        raise typer.Exit(1)

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
        raise typer.Exit(1)

    success(f"Project resource release created: id={created.get('id') or '-'}")
    print_kv(
        "Project Resource Release",
        [
            ("ID", str(created.get("id") or "-")),
            ("Release Kind", release_kind),
            ("Resource", str(created.get("resource") or resource_id)),
            ("Related Image", _format_related_image_label(created.get("related_image") or related_image_id)),
            ("CPU Request", str(created.get("cpu_request") or cpu_request)),
            ("Memory Request", str(created.get("memory_request") or memory_request)),
            ("GPU Request", str(created.get("gpu_request") or gpu_request or "-")),
            ("GPU Type", str(created.get("gpu_type") or gpu_type or "-")),
            ("Spot", str(created.get("spot") if created.get("spot") is not None else spot).lower()),
        ],
    )


@project_project_resource_group.command("create_dashboard")
def project_project_resource_create_dashboard_cmd(
    project_id: int | None = typer.Argument(None, help="Project ID. Defaults to local .env when omitted."),
    resource_id: int | None = typer.Option(None, "--resource-id", help="Project resource ID."),
    path: str | None = typer.Option(None, "--path", help="Project repository path (default: current project)"),
    related_image_id: int | None = typer.Option(None, "--related-image-id", help="Project image ID."),
    readme_resource_id: int | None = typer.Option(None, "--readme-resource-id", help="Optional README resource ID."),
    cpu_request: str | None = typer.Option(None, "--cpu-request", help="CPU request (accepts 0.5 or 500m; default: 0.25)."),
    memory_request: str | None = typer.Option(None, "--memory-request", help="Memory request (accepts 1 or 1Gi; default: 0.5)."),
    gpu_request: str | None = typer.Option(None, "--gpu-request", help="GPU request count."),
    gpu_type: str | None = typer.Option(None, "--gpu-type", help="GPU accelerator type."),
    spot: bool | None = typer.Option(None, "--spot/--no-spot", help="Whether to prefer spot capacity."),
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


@project_project_resource_group.command("create_agent")
def project_project_resource_create_agent_cmd(
    project_id: int | None = typer.Argument(None, help="Project ID. Defaults to local .env when omitted."),
    resource_id: int | None = typer.Option(None, "--resource-id", help="Project resource ID."),
    path: str | None = typer.Option(None, "--path", help="Project repository path (default: current project)"),
    related_image_id: int | None = typer.Option(None, "--related-image-id", help="Project image ID."),
    readme_resource_id: int | None = typer.Option(None, "--readme-resource-id", help="Optional README resource ID."),
    cpu_request: str | None = typer.Option(None, "--cpu-request", help="CPU request (accepts 0.5 or 500m; default: 0.25)."),
    memory_request: str | None = typer.Option(None, "--memory-request", help="Memory request (accepts 1 or 1Gi; default: 0.5)."),
    gpu_request: str | None = typer.Option(None, "--gpu-request", help="GPU request count."),
    gpu_type: str | None = typer.Option(None, "--gpu-type", help="GPU accelerator type."),
    spot: bool | None = typer.Option(None, "--spot/--no-spot", help="Whether to prefer spot capacity."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Create an agent release from a project resource.

    The command first lets the user select a project image and then filters resources so
    only resources with `repo_commit_sha == related_image.project_repo_hash` are eligible.
    """
    _project_resource_release_create_impl(
        release_kind="agent",
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
        raise typer.Exit(1)

    release_label = "dashboard release" if expected_release_kind == "streamlit_dashboard" else "agent release"
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
        raise typer.Exit(1)

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


@project_project_resource_group.command("delete_agent")
def project_project_resource_delete_agent_cmd(
    release_id: int = typer.Argument(..., help="Agent resource release ID."),
    yes: bool = typer.Option(False, "--yes", help="Delete without confirmation."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Delete an agent resource release.

    Examples
    --------
    ```bash
    mainsequence project project_resource delete_agent 601
    mainsequence project project_resource delete_agent 601 --yes
    ```
    """
    _project_resource_release_delete_impl(
        release_id=release_id,
        expected_release_kind="agent",
        yes=yes,
        timeout=timeout,
    )


def _project_images_list_impl(
    project_id: int | None,
    path: str | None,
    timeout: int | None,
) -> None:
    _require_login()

    if project_id is None:
        project_id = _resolve_project_id_from_local_env(path)

    try:
        images = list_project_images(related_project_id=project_id, timeout=timeout)
    except ApiError as e:
        error(f"Project images fetch failed: {e}")
        raise typer.Exit(1)

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
    project_id: int | None = typer.Argument(None, help="Project ID. Defaults to local .env when omitted."),
    path: str | None = typer.Option(None, "--path", help="Project repository path (default: current project)"),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    List project images for a project.

    Uses SDK client `ProjectImage.filter()` as the single source of truth.

    Parameters
    ----------
    project_id:
        Platform project ID. Defaults to local `.env`.
    path:
        Local project path. Used when resolving project id from `.env`.
    timeout:
        Request timeout in seconds.

    Examples
    --------
    ```bash
    mainsequence project images list
    mainsequence project images list 123
    mainsequence project images list 123 --path .
    ```
    """
    _project_images_list_impl(project_id=project_id, path=path, timeout=timeout)


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
        raise typer.Exit(1)

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
        raise typer.Exit(1)

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
    project_id: int | None,
    project_repo_hash: str | None,
    path: str | None,
    base_image_id: int | None,
    timeout: int,
    poll_interval: int,
) -> None:
    _require_login()

    project_dir = _resolve_project_dir(project_id, path) if (project_id is not None or path) else _resolve_current_project_dir_from_env()
    if project_id is None:
        project_id = _resolve_project_id_from_local_env(str(project_dir))

    try:
        existing_images = list_project_images(related_project_id=project_id, timeout=timeout)
    except ApiError as e:
        error(f"Project images fetch failed: {e}")
        raise typer.Exit(1)
    images_by_hash = _group_project_images_by_hash(existing_images)

    pending_commits = _list_unpushed_commits(project_dir)
    if pending_commits:
        pending_hashes = ", ".join(c["short_hash"] for c in pending_commits[:3] if c.get("short_hash"))
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
            raise typer.Exit(1)

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

    if not _is_pushed_commit(project_dir, project_repo_hash):
        error("project_repo_hash must reference a commit that has already been pushed to the remote.")
        raise typer.Exit(1)

    existing_for_hash = images_by_hash.get(project_repo_hash, [])
    if existing_for_hash:
        warn(
            "This commit already has project image(s): "
            + _format_image_ids(existing_for_hash)
        )

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
        raise typer.Exit(1)
    except RuntimeError as e:
        error(str(e))
        raise typer.Exit(1)

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
                with status(f"Project image not ready yet (attempt {attempt}). Next check in {int(sleep_for)}s..."):
                    time.sleep(sleep_for)

            try:
                polled_images = list_project_images(related_project_id=project_id, timeout=timeout)
            except ApiError as e:
                warn(f"Project image status poll failed (attempt {attempt}): {e}")
                continue

            latest = next((img for img in polled_images if str(img.get("id")) == str(image_id)), None)
            if latest is None:
                warn(f"Project image {image_id} was not visible yet on poll attempt {attempt}.")
                continue

            created = latest
            if created.get("is_ready") is True:
                success("Project image is ready.")
                break
            info("Project image still building. Continuing to poll...")
        else:
            warn(
                f"Timed out after {timeout}s waiting for project image {image_id} to become ready. "
                "It may still be building on the backend."
            )

    base_image_value = created.get("base_image")
    if isinstance(base_image_value, dict):
        base_image_value = base_image_value.get("id") or base_image_value.get("title") or "-"

    print_kv(
        "Project Image",
        [
            ("ID", str(created.get("id") or "-")),
            ("Project ID", str(project_id)),
            ("Project Repo Hash", project_repo_hash),
            ("Base Image", str(base_image_value or base_image_id or "-")),
            ("Is Ready", str(created.get("is_ready")) if created.get("is_ready") is not None else "-"),
        ],
    )


@project_images_group.command("create")
def project_images_create_cmd(
    project_id: int | None = typer.Argument(None, help="Project ID. Defaults to local .env when omitted."),
    project_repo_hash: str | None = typer.Argument(
        None,
        help="Git commit hash for the image build. Must already be pushed to the remote.",
    ),
    path: str | None = typer.Option(None, "--path", help="Project repository path (default: current project)"),
    base_image_id: int | None = typer.Option(None, "--base-image-id", help="Project base image ID"),
    timeout: int = typer.Option(300, "--timeout", help="Maximum wait time in seconds for the image to become ready."),
    poll_interval: int = typer.Option(30, "--poll-interval", help="Polling interval in seconds while waiting for is_ready=true."),
):
    """
    Create a project image from a pushed git commit.

    If `project_id` is omitted, the command reads `MAIN_SEQUENCE_PROJECT_ID`
    from the local project `.env`. If `project_repo_hash` is omitted, it shows
    only commits already present on the remote and prompts for a selection.

    Parameters
    ----------
    project_id:
        Platform project ID. Defaults to local `.env`.
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
    mainsequence project images create 123
    mainsequence project images create 123 4a1b2c3d
    mainsequence project images create 123 --path .
    mainsequence project images create 123 --timeout 600 --poll-interval 15
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
    project_id: int | None = typer.Argument(None, help="Project ID. Defaults to local .env when omitted."),
    project_repo_hash: str | None = typer.Argument(
        None,
        help="Git commit hash for the image build. Must already be pushed to the remote.",
    ),
    path: str | None = typer.Option(None, "--path", help="Project repository path (default: current project)"),
    base_image_id: int | None = typer.Option(None, "--base-image-id", help="Project base image ID"),
    timeout: int = typer.Option(300, "--timeout", help="Maximum wait time in seconds for the image to become ready."),
    poll_interval: int = typer.Option(30, "--poll-interval", help="Polling interval in seconds while waiting for is_ready=true."),
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
    project_id: int | None,
    path: str | None,
    timeout: int | None,
) -> None:
    _require_login()

    if project_id is None:
        project_id = _resolve_project_id_from_local_env(path)

    try:
        jobs = list_project_jobs(project_id=project_id, timeout=timeout)
    except ApiError as e:
        error(f"Project jobs fetch failed: {e}")
        raise typer.Exit(1)

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
    timeout: int | None,
) -> None:
    _require_login()

    try:
        runs = list_project_job_runs(job_id=job_id, timeout=timeout)
    except ApiError as e:
        error(f"Project job runs fetch failed: {e}")
        raise typer.Exit(1)

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
            ["ID", "Name", "Status", "Execution Start", "Execution End", "Unique Identifier", "Commit Hash"],
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
    project_id: int | None = typer.Argument(None, help="Project ID. Defaults to local .env when omitted."),
    path: str | None = typer.Option(None, "--path", help="Project repository path (default: current project)"),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    List jobs for a project.

    Uses SDK client `Job.filter()` as the single source of truth.

    Examples
    --------
    ```bash
    mainsequence project jobs list
    mainsequence project jobs list 123
    mainsequence project jobs list 123 --path .
    ```
    """
    _project_jobs_list_impl(project_id=project_id, path=path, timeout=timeout)


@project_jobs_group.command("run")
def project_jobs_run_cmd(
    job_id: int = pydantic_argument(JOB_MODEL_REF, "id", ..., help="Job ID to run."),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Run a project job immediately.

    Uses SDK client `Job.run_job()` as the single source of truth.

    Examples
    --------
    ```bash
    mainsequence project jobs run 91
    mainsequence project jobs run 91 --timeout 60
    ```
    """
    _require_login()

    try:
        payload = run_project_job(job_id=job_id, timeout=timeout)
    except ApiError as e:
        error(f"Project job run failed: {e}")
        raise typer.Exit(1)

    success(f"Project job run requested: job_id={job_id}")

    if payload:
        preferred_keys = [
            ("Job ID", str(payload.get("job") or payload.get("job_id") or job_id)),
            ("Job Run ID", str(payload.get("id") or payload.get("job_run_id") or "-")),
            ("Name", str(payload.get("name") or payload.get("job_name") or "-")),
            ("Unique Identifier", str(payload.get("unique_identifier") or "-")),
            ("Status", str(payload.get("status") or "-")),
        ]
        rows = [(label, value) for label, value in preferred_keys if value != "-"]
        remaining = []
        for key, value in payload.items():
            if key in {"job", "job_id", "id", "job_run_id", "name", "job_name", "unique_identifier", "status"}:
                continue
            remaining.append((str(key), json.dumps(value) if isinstance(value, (dict, list)) else str(value)))
        print_kv("Job Run", rows + remaining)


@project_job_runs_group.command("list")
def project_job_runs_list_cmd(
    job_id: int = pydantic_argument(JOB_MODEL_REF, "id", ..., help="Job ID whose runs will be listed."),
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
    _project_job_runs_list_impl(job_id=job_id, timeout=timeout)


@project_job_runs_group.command("logs")
def project_job_runs_logs_cmd(
    job_run_id: int = pydantic_argument(JOB_RUN_MODEL_REF, "id", ..., help="Job run ID whose logs will be shown."),
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
    mainsequence project jobs runs logs 501
    mainsequence project jobs runs logs 501 --poll-interval 10
    mainsequence project jobs runs logs 501 --max-wait-seconds 900
    mainsequence project jobs runs logs 501 --poll-interval 0
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
            payload = get_project_job_run_logs(job_run_id=job_run_id, timeout=timeout)
        except ApiError as e:
            error(f"Project job run logs fetch failed: {e}")
            raise typer.Exit(1)

        status_value = str(payload.get("status") or "-")
        rows = payload.get("rows") or []
        if not isinstance(rows, list):
            rows = [rows]

        if shown_rows == 0:
            print_kv(
                "Job Run Logs",
                [
                    ("Job Run ID", str(payload.get("job_run_id") or job_run_id)),
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
    project_id: int | None,
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

    project_dir = _resolve_project_dir(project_id, path) if (project_id is not None or path) else _resolve_current_project_dir_from_env()
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
        raise typer.Exit(1)

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
            app_name = typer.prompt(
                pydantic_prompt_text(JOB_MODEL_REF, "app_name", optional=True),
                default="",
            ).strip() or None

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
        raise typer.Exit(1)

    try:
        cpu_request, memory_request, spot, max_runtime_seconds, used_defaults = _resolve_job_create_defaults(
            cpu_request=cpu_request,
            memory_request=memory_request,
            spot=spot,
            max_runtime_seconds=max_runtime_seconds,
        )
    except ValueError as e:
        error(str(e))
        raise typer.Exit(1)

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
        info(
            "Using defaults: "
            + ", ".join(default_parts)
            + "."
        )

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
        raise typer.Exit(1)
    except RuntimeError as e:
        error(str(e))
        raise typer.Exit(1)

    success(f"Project job created: id={created.get('id') or '-'}")
    print_kv(
        "Project Job",
        [
            ("ID", str(created.get("id") or "-")),
            ("Name", str(created.get("name") or name)),
            ("Project ID", str(project_id)),
            ("Execution Path", str(created.get("execution_path") or execution_path or "-")),
            ("App Name", str(created.get("app_name") or app_name or "-")),
            ("Related Image", _format_related_image_label(created.get("related_image") or related_image_id)),
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
    project_id: int | None = typer.Argument(None, help="Project ID. Defaults to local .env when omitted."),
    name: str | None = pydantic_option(JOB_MODEL_REF, "name", None, "--name"),
    path: str | None = typer.Option(None, "--path", help="Project repository path (default: current project)"),
    execution_path: str | None = pydantic_option(
        JOB_MODEL_REF,
        "execution_path",
        None,
        "--execution-path",
    ),
    app_name: str | None = pydantic_option(JOB_MODEL_REF, "app_name", None, "--app-name"),
    related_image_id: int | None = pydantic_option(
        JOB_MODEL_REF,
        "related_image",
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
    mainsequence project jobs create 123 --name daily-run --execution-path scripts/test.py --related-image-id 77
    mainsequence project jobs create 123 --name dashboard --app-name dashboard-api --related-image-id 77
    mainsequence project jobs create 123 --name hourly-run --execution-path scripts/test.py --related-image-id 77 --schedule-type interval --schedule-every 1 --schedule-period hours
    mainsequence project jobs create 123 --name nightly-run --execution-path scripts/test.py --related-image-id 77 --schedule-type crontab --schedule-expression "0 0 * * *"
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
    project_id: int | None = typer.Argument(None, help="Project ID. Defaults to local .env when omitted."),
    path: str | None = typer.Option(None, "--path", help="Project repository path used to resolve project id."),
    strict: bool = typer.Option(
        False,
        "--strict/--no-strict",
        help="If enabled, jobs that exist remotely but are not listed in the file may be removed.",
    ),
    timeout: int | None = typer.Option(None, "--timeout", help="Request timeout in seconds"),
):
    """
    Validate and submit a batch of jobs from a YAML file.

    Uses SDK client `Job.bulk_get_or_create()` as the single source of truth.

    Examples
    --------
    ```bash
    mainsequence project schedule_batch_jobs scheduled_jobs.yaml
    mainsequence project schedule_batch_jobs scheduled_jobs.yaml 123
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
            project_id=int(project_id),
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
        raise typer.Exit(1)
    except RuntimeError as e:
        error(str(e))
        raise typer.Exit(1)
    finally:
        if prepared_batch_file != batch_file:
            try:
                prepared_batch_file.unlink(missing_ok=True)
            except Exception:
                pass

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
    print_kv(
        "Batch Scheduling",
        [
            ("Project ID", str(project_id)),
            ("File", str(batch_file)),
            ("Strict", str(bool(strict)).lower()),
            ("Result", json.dumps(created) if isinstance(created, (dict, list)) else str(created)),
        ],
    )


@project.command("set-up-locally")
def project_set_up_locally(
    project_id: int = typer.Argument(..., help="Project ID from the platform"),
    base_dir: str | None = typer.Option(None, "--base-dir", help="Override base dir (default from settings)"),
    scaffold_docker: bool = typer.Option(
        True,
        "--scaffold-docker/--no-scaffold-docker",
        help="Create Dockerfile/.dockerignore when DEFAULT_BASE_IMAGE is present",
    ),
):
    """
    Clone a project locally and provision runtime `.env`.

    Workflow:
    - ensure SSH key and optionally register deploy key,
    - clone repository into local projects root,
    - fetch remote environment and inject current session JWTs,
    - write/update `.env` with local runtime values,
    - optionally scaffold Docker files from default base image.

    Parameters
    ----------
    project_id:
        Platform project ID.
    base_dir:
        Override local projects base directory.
    scaffold_docker:
        Enable Dockerfile/.dockerignore scaffold when base image is available.

    Examples
    --------
    ```bash
    mainsequence project set-up-locally 123
    mainsequence project set-up-locally 123 --base-dir ~/mainsequence
    mainsequence project set-up-locally 123 --no-scaffold-docker
    ```
    """
    _require_login()

    cfg_obj = cfg.get_config()
    base = base_dir or cfg_obj["mainsequence_path"]
    org_slug = _org_slug_from_profile()

    items = get_projects()
    p = next((x for x in items if int(x.get("id", -1)) == project_id), None)
    if not p:
        error("Project not found/visible.")
        raise typer.Exit(1)

    repo = _determine_repo_url(p)
    if not repo:
        error("No repository URL found for this project.")
        raise typer.Exit(1)

    name = safe_slug(p.get("project_name") or f"project-{project_id}")
    projects_root = _projects_root(base, org_slug)
    target_dir = projects_root / f"{name}-{project_id}"
    projects_root.mkdir(parents=True, exist_ok=True)

    key_path, _pub_path, pub = ensure_key_for_repo(repo)
    copied = _copy_clipboard(pub)

    # Best-effort deploy key (do not fail set-up-locally on this)
    try:
        host = platform.node()
        add_deploy_key(project_id, host, pub)
    except Exception as e:
        warn(f"Could not add deploy key automatically (continuing): {e}")

    agent_env = start_agent_and_add_key(key_path)

    if target_dir.exists():
        warn(f"Target already exists: {target_dir}")
        raise typer.Exit(2)

    env = os.environ.copy() | agent_env
    env["GIT_SSH_COMMAND"] = f'ssh -i "{str(key_path)}" -o IdentitiesOnly=yes'

    with status(f"Cloning repo into {target_dir}..."):
        rc = subprocess.call(["git", "clone", repo, str(target_dir)], env=env, cwd=str(projects_root))
    if rc != 0:
        try:
            import shutil

            if target_dir.exists():
                shutil.rmtree(target_dir, ignore_errors=True)
        except Exception:
            pass
        error("git clone failed")
        raise typer.Exit(3)

    # Fetch env text (keep original for DEFAULT_BASE_IMAGE extraction)
    orig_env_text = ""
    try:
        orig_env_text = fetch_project_env_text(project_id)
    except Exception:
        orig_env_text = ""
    env_text = (orig_env_text or "").replace("\r", "")

    try:
        access_token, refresh_token = _current_session_jwt_tokens()
    except RuntimeError as e:
        error(str(e))
        raise typer.Exit(1)

    final_env = _render_project_runtime_env_text(
        env_text,
        access_token=access_token,
        refresh_token=refresh_token,
        backend_url=cfg.backend_url(),
        project_runtime_id=str(project_id),
    )
    (target_dir / ".env").write_text(final_env, encoding="utf-8")

    # Docker scaffold parity
    if scaffold_docker:
        default_base_image = extract_env_value(orig_env_text or "", "DEFAULT_BASE_IMAGE")
        if default_base_image is not None:
            img, warnings_ = resolve_base_image(default_base_image)
            for w in warnings_:
                warn(w)
            _changed, msgs = ensure_docker_scaffold(target_dir, img)
            for m in msgs:
                info(m)

    success(f"Local folder: {target_dir}")
    info(f"Repo URL: {repo}")
    if copied:
        info("Public key copied to clipboard.")


@project.command("open")
def project_open(
    project_id: int | None = typer.Argument(None, help="Project ID"),
    path: str | None = typer.Option(None, "--path", help="Open an explicit path instead of resolving by id"),
):
    """
    Open a mapped project folder in the OS file manager.

    Parameters
    ----------
    project_id:
        Project ID to resolve local folder.
    path:
        Explicit local path to open.

    Examples
    --------
    ```bash
    mainsequence project open 123
    mainsequence project open --path .
    ```
    """
    p = _resolve_project_dir(project_id, path)
    open_folder(str(p))
    success(f"Opened: {p}")


@project.command("delete-local")
def project_delete_local(
    project_id: int | None = typer.Argument(None, help="Project ID"),
    path: str | None = typer.Option(None, "--path", help="Delete an explicit path instead of resolving by id"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not prompt for confirmation"),
):
    """
    Delete a local project folder.

    Safety checks prevent deletion outside configured projects root.

    Parameters
    ----------
    project_id:
        Project ID to resolve local path.
    path:
        Explicit local path to delete.
    yes:
        Skip confirmation prompt.

    Examples
    --------
    ```bash
    mainsequence project delete-local 123
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
    except Exception:
        error(f"Refusing to delete outside projects root: {p}")
        raise typer.Exit(1)

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
    project_id: int | None = typer.Argument(None, help="Project ID"),
    path: str | None = typer.Option(None, "--path", help="Open in a specific project directory"),
):
    """
    Open terminal with `ssh-agent` and project key preloaded.

    Parameters
    ----------
    project_id:
        Project ID to resolve local folder.
    path:
        Explicit local path.

    Examples
    --------
    ```bash
    mainsequence project open-signed-terminal 123
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
    project_id: int | None = typer.Argument(None, help="Project ID"),
    path: str | None = typer.Option(None, "--path", help="Project directory"),
):
    """
    Build local `.venv` and sync dependencies using `uv`.

    Reads Python requirement from `pyproject.toml`, creates `.venv`, then runs `uv sync`.

    Parameters
    ----------
    project_id:
        Project ID to resolve local folder. If omitted, the current directory is used when
        `MAIN_SEQUENCE_PROJECT_ID` is present in `./.env`.
    path:
        Explicit local path.

    Examples
    --------
    ```bash
    mainsequence project build_local_venv
    mainsequence project build_local_venv 123
    mainsequence project build_local_venv --path .
    ```
    """
    project_dir = _resolve_project_dir(project_id, path) if (project_id is not None or path) else _resolve_current_project_dir_from_env()
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
    except Exception:
        error("Could not read pyproject.toml from the project root.")
        raise typer.Exit(1)

    python_version = _extract_python_version_from_pyproject_text(pyproject_text)
    if not python_version:
        error("Could not determine Python version from pyproject.toml (requires-python or Poetry python spec).")
        raise typer.Exit(1)

    with status("Building local .venv..."):
        uv_runner = _resolve_uv_runner()
        if not uv_runner:
            info("uv not found. Installing uv...")
            ok, reason = _install_uv()
            if not ok:
                details = f": {reason}" if reason else ""
                error(f"uv is not installed and automatic install failed{details}. Install manually with: pip install uv")
                raise typer.Exit(1)

            uv_runner = _resolve_uv_runner()
            if not uv_runner:
                error("uv install completed but uv is still not available. Restart your shell and try again.")
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
            error(f"Failed to create local .venv via {uv_display}: {reason or f'exit {venv_result.returncode}'}")
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
            error(f"Failed to run uv sync for local .venv via {uv_display}: {reason or f'exit {sync_result.returncode}'}")
            raise typer.Exit(1)

    success(f"Local .venv built with Python {python_version}.")


@project.command("refresh_token")
def project_refresh_token(
    project_id: int | None = typer.Argument(None, help="Project ID"),
    path: str | None = typer.Option(None, "--path", help="Project directory"),
):
    """
    Refresh local project JWTs in `.env` from the current CLI session.

    Use this when a project has been idle long enough for the previously injected
    JWTs to expire. The command preserves the rest of the `.env` file and only
    rewrites the runtime auth keys managed by the CLI.

    Parameters
    ----------
    project_id:
        Project ID to resolve local folder. If omitted, the current directory is used.
    path:
        Explicit local path. If omitted, the current directory is used.

    Examples
    --------
    ```bash
    mainsequence project refresh_token
    mainsequence project refresh_token 123
    mainsequence project refresh_token --path .
    ```
    """
    _require_login()
    project_dir = _resolve_project_dir(project_id, path) if (project_id is not None or path) else pathlib.Path.cwd()
    env_path = project_dir / ".env"
    if not env_path.is_file():
        error(f".env not found in project root: {env_path}")
        info("Run: mainsequence project set-up-locally <id> to provision the local runtime first.")
        raise typer.Exit(1)

    try:
        access_token, refresh_token = _current_session_jwt_tokens()
    except RuntimeError as e:
        error(str(e))
        raise typer.Exit(1)

    try:
        env_text = env_path.read_text(encoding="utf-8")
    except Exception as e:
        error(f"Could not read .env: {e}")
        raise typer.Exit(1)

    inferred_project_id = str(project_id) if project_id is not None else None
    if inferred_project_id is None:
        match = re.search(r"-(\d+)$", project_dir.name)
        if match:
            inferred_project_id = match.group(1)

    final_env = _render_project_runtime_env_text(
        env_text,
        access_token=access_token,
        refresh_token=refresh_token,
        backend_url=cfg.backend_url(),
        project_runtime_id=inferred_project_id,
    )
    env_path.write_text(final_env, encoding="utf-8")
    success(f"Refreshed JWT tokens in: {env_path}")


@project.command("freeze-env")
def project_freeze_env(
    project_id: int | None = typer.Argument(None, help="Project ID"),
    path: str | None = typer.Option(None, "--path", help="Project directory"),
    ensure_uv: bool = typer.Option(True, "--ensure-uv/--no-ensure-uv", help="Install uv into .venv if missing"),
):
    """
    Export pinned dependencies into `requirements.txt` using `uv`.

    Parameters
    ----------
    project_id:
        Project ID to resolve local folder.
    path:
        Explicit local path.
    ensure_uv:
        Install `uv` in `.venv` if missing.

    Examples
    --------
    ```bash
    mainsequence project freeze-env 123
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
        uv_export_requirements(uv, cwd=project_dir, locked=False, no_dev=False, output_file="requirements.txt")

    success(f"Wrote: {project_dir / 'requirements.txt'}")


@project.command("sync")
def project_sync(
    message: str | None = typer.Argument(None, help="Git commit message"),
    project_id: int | None = typer.Argument(None, help="Project ID"),
    path: str | None = typer.Option(None, "--path", help="Project directory"),
    message_opt: str | None = typer.Option(None, "--message", "-m", help="Git commit message"),
    bump: str = typer.Option("patch", "--bump", help="uv version bump: patch|minor|major (default: patch)"),
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
        Project ID to resolve local folder.
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
    resolved_project_id = project_id if project_id is not None else _read_project_id_from_env_file(project_dir)
    if not dry_run and not no_push:
        _require_login()
        if resolved_project_id is None:
            error(
                "Could not determine project id from local .env. "
                "Pass PROJECT_ID or ensure MAIN_SEQUENCE_PROJECT_ID is present before syncing."
            )
            raise typer.Exit(1)
    ensure_venv(project_dir)

    origin = git_origin(project_dir)
    repo_name = repo_name_from_git_url(origin) or project_dir.name
    key_path, _, _ = ensure_key_for_repo(origin)

    safe_message = str(message or "").replace("\r", " ").replace("\n", " ").replace('"', "'").strip()
    if not safe_message:
        error("Commit message is required.")
        raise typer.Exit(1)

    steps = [
        "pip install uv (in .venv)",
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
                raise typer.Exit(1)
        info(f"Triggered backend sync for project {resolved_project_id}.")

    success(f"Synced: {repo_name}")


@project.command("sync_project", hidden=True)
def project_sync_project(
    message: str = typer.Argument(..., help="Git commit message"),
    project_id: int | None = typer.Argument(None, help="Project ID"),
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
    project_id: int | None = typer.Argument(None, help="Project ID"),
    path: str | None = typer.Option(None, "--path", help="Project directory"),
    image_ref: str | None = typer.Option(None, "--image-ref", help="Docker image ref to build (default: computed)"),
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
        Project ID to resolve local folder.
    path:
        Explicit local path.
    image_ref:
        Explicit docker image tag/reference.
    devcontainer:
        Write `.devcontainer/devcontainer.json` after build.

    Examples
    --------
    ```bash
    mainsequence project build-docker-env 123
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
def project_current(debug: bool = typer.Option(False, "--debug", help="Show detection debug details")):
    """
    Detect and display current project context from current directory.

    Includes detected path, project id, virtual environment, Python version,
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

    items = [
        ("Path", project_info.path),
        ("Folder", project_info.folder),
        ("Project ID", project_info.project_id or "-"),
        ("Venv", project_info.venv_path or "not found"),
        ("Python", project_info.python_version or "unknown"),
    ]
    print_kv("Current Project", items)

    # SDK status (best-effort)
    req = pathlib.Path(project_info.path) / "requirements.txt"
    local = read_local_sdk_version(req)
    latest = None
    try:
        latest = fetch_latest_sdk_version()
    except Exception:
        pass

    if latest or local is not None:
        status_label = "checking"
        if latest and local and local != "unversioned":
            status_label = "match" if normalize_version(local) == normalize_version(latest) else "differs"
        print_kv(
            "SDK Status",
            [
                ("Latest (GitHub)", latest or "unavailable"),
                ("Local (requirements.txt)", local if local is not None else "not found"),
                ("Status", status_label),
                ("Hint", "Run: mainsequence project update-sdk --path .  (if differs)"),
            ],
        )

    if debug and dbg.checks:
        print_kv("Detection Debug", [("details", json.dumps([c.__dict__ for c in dbg.checks], indent=2))])


@project.command("sdk-status")
def project_sdk_status(
    project_id: int | None = typer.Argument(None, help="Project ID"),
    path: str | None = typer.Option(None, "--path", help="Project directory"),
):
    """
    Show local project SDK version versus latest GitHub release.

    Parameters
    ----------
    project_id:
        Project ID to resolve local folder.
    path:
        Explicit local path.

    Examples
    --------
    ```bash
    mainsequence project sdk-status 123
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
        status_label = "match" if normalize_version(local) == normalize_version(latest) else "differs"

    print_kv(
        "SDK Status",
        [
            ("Project", str(project_dir)),
            ("Latest (GitHub)", latest or "unavailable"),
            ("Local (requirements.txt)", local if local is not None else "not found"),
            ("Status", status_label),
        ],
    )


@project.command("update-sdk")
def project_update_sdk(
    project_id: int | None = typer.Argument(None, help="Project ID"),
    path: str | None = typer.Option(None, "--path", help="Project directory"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Print steps but do not execute"),
):
    """
    Upgrade project SDK dependency (`mainsequence`) using `uv`.

    Parameters
    ----------
    project_id:
        Project ID to resolve local folder.
    path:
        Explicit local path.
    dry_run:
        Print update plan without executing.

    Examples
    --------
    ```bash
    mainsequence project update-sdk 123
    mainsequence project update-sdk --path .
    mainsequence project update-sdk --path . --dry-run
    ```
    """
    project_dir = _resolve_project_dir(project_id, path)
    ensure_venv(project_dir)

    steps = [
        "pip install uv (in .venv)",
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
