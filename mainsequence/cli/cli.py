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

import json
import os
import pathlib
import platform
import re
import shutil
import subprocess
import sys
import time

import typer

from . import config as cfg
from .api import (
    ApiError,
    NotLoggedIn,
    add_deploy_key,
    create_project,
    deep_find_repo_url,
    delete_project,
    fetch_project_env_text,
    get_current_user_profile,
    get_project,
    get_project_token,
    get_projects,
    list_dynamic_table_data_sources,
    list_github_organizations,
    list_project_base_images,
    repo_name_from_git_url,
    safe_slug,
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
from .sdk_utils import fetch_latest_sdk_version, normalize_version, read_local_sdk_version
from .ssh_utils import (
    ensure_key_for_repo,
    open_folder,
    open_signed_terminal,
    start_agent_and_add_key,
)
from .ui import error, info, print_kv, print_table, status, success, warn

app = typer.Typer(help="MainSequence CLI (login + project operations)")

project = typer.Typer(help="Project commands (remote + local operations)")
settings = typer.Typer(help="Settings (base folder, backend, etc.)")
sdk = typer.Typer(help="SDK utilities (latest version, status)")

app.add_typer(project, name="project")
app.add_typer(settings, name="settings")
app.add_typer(sdk, name="sdk")


# ---------- AI instructions utilities (kept) ----------

INSTR_REL_PATH = pathlib.Path("examples") / "ai" / "instructions"


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
        error("You must pass either PROJECT_ID or --path.")
        raise typer.Exit(1)

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


# ---------- top-level commands ----------


@app.command()
def login(
    email: str = typer.Argument(..., help="Email/username (server expects 'email' field)"),
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
    CLI invocations can run without re-authentication.

    Parameters
    ----------
    email:
        Login email.
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
    mainsequence login you@company.com --no-status
    mainsequence login you@company.com --export
    ```
    """
    try:
        res = api_login(email, password)
    except ApiError as e:
        error(f"Login failed: {e}")
        raise typer.Exit(1)

    if export:
        access = (res.get("access") or "").replace('"', '\\"')
        refresh = (res.get("refresh") or "").replace('"', '\\"')
        username = (res.get("username") or "").replace('"', '\\"')
        typer.echo(f'export MAIN_SEQUENCE_USER_TOKEN="{access}"')
        typer.echo(f'export MAIN_SEQUENCE_REFRESH_TOKEN="{refresh}"')
        typer.echo(f'export MAIN_SEQUENCE_USERNAME="{username}"')
        return

    cfg_obj = cfg.get_config()
    base = cfg_obj["mainsequence_path"]
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
    ok = cfg.clear_tokens()
    if export:
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
    c = cfg.get_config()
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
    out = cfg.set_config({"mainsequence_path": path})
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


# ---------- project group ----------


@project.command("list")
def project_list():
    """
    List projects visible to the authenticated user.

    The output includes remote metadata and local mapping status.

    Examples
    --------
    ```bash
    mainsequence project list
    ```
    """
    _require_login()
    cfg_obj = cfg.get_config()
    base = cfg_obj["mainsequence_path"]
    org_slug = _org_slug_from_profile()
    items = get_projects()
    typer.echo(_render_projects_table(items, base, org_slug))


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
    - fetch remote environment and project token,
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

    # Project token
    try:
        project_token = get_project_token(project_id)
    except NotLoggedIn:
        error("Session expired or refresh failed. Run: mainsequence login <email>")
        raise typer.Exit(1)
    except ApiError as e:
        error(f"Could not fetch project token: {e}")
        raise typer.Exit(1)

    # Write .env (upsert logic mirrors extension)
    lines = env_text.splitlines()

    def upsert(prefix: str, value: str):
        nonlocal lines
        key = prefix + "="
        for i, ln in enumerate(lines):
            if ln.startswith(key):
                lines[i] = f"{prefix}={value}"
                return
        if lines and lines[-1] != "":
            lines.append("")
        lines.append(f"{prefix}={value}")

    upsert("MAINSEQUENCE_TOKEN", project_token)
    upsert("TDAG_ENDPOINT", cfg.backend_url())
    upsert("INGORE_MS_AGENT", "true")

    final_env = "\n".join(lines).replace("\r", "")
    (target_dir / ".env").write_text(final_env + ("\n" if not final_env.endswith("\n") else ""), encoding="utf-8")

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
        Project ID to resolve local folder.
    path:
        Explicit local path.

    Examples
    --------
    ```bash
    mainsequence project build_local_venv 123
    mainsequence project build_local_venv --path .
    ```
    """
    project_dir = _resolve_project_dir(project_id, path)
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
    message: str = typer.Option(..., "--message", "-m", help="Git commit message"),
    project_id: int | None = typer.Argument(None, help="Project ID"),
    path: str | None = typer.Option(None, "--path", help="Project directory"),
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
        Commit message.
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
    mainsequence project sync -m "Update environment" --path .
    mainsequence project sync -m "Bump minor" --bump minor --path .
    mainsequence project sync -m "Preview only" --path . --dry-run
    ```
    """
    project_dir = _resolve_project_dir(project_id, path)
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
        uv_export_requirements(uv, cwd=project_dir, locked=True, no_dev=True, output_file="requirements.txt")

        run_cmd(["git", "add", "-A"], cwd=project_dir, env=env)
        run_cmd(["git", "commit", "-m", safe_message], cwd=project_dir, env=env)
        if not no_push:
            run_cmd(["git", "push"], cwd=project_dir, env=env)

    success(f"Synced: {repo_name}")


@project.command("sync_project")
def project_sync_project(
    message: str = typer.Argument(..., help="Git commit message"),
    project_id: int | None = typer.Argument(None, help="Project ID"),
    path: str | None = typer.Option(None, "--path", help="Project directory"),
):
    """
    Run the standard project sync chain with a commit message.

    This helper executes:
    1. `uv version --bump patch`
    2. `uv lock`
    3. `uv sync`
    4. `uv export --locked --no-dev --no-hashes --format requirements.txt --output-file requirements.txt`
    5. `git add -A`
    6. `git commit -m "<message>"`
    7. `git push`

    Notes
    -----
    `.venv` activation is handled implicitly by using the `uv` binary inside `.venv`.

    Parameters
    ----------
    message:
        Git commit message.
    project_id:
        Project ID to resolve local folder.
    path:
        Explicit local path.

    Examples
    --------
    ```bash
    mainsequence project sync_project "Update dependencies" --path .
    mainsequence project sync_project "Sync patch bump" 123
    ```
    """
    project_dir = _resolve_project_dir(project_id, path)
    ensure_venv(project_dir)

    origin = git_origin(project_dir)
    repo_name = repo_name_from_git_url(origin) or project_dir.name
    key_path, _, _ = ensure_key_for_repo(origin)

    safe_message = str(message or "").replace("\r", " ").replace("\n", " ").replace('"', "'").strip()
    if not safe_message:
        error("Commit message is required.")
        raise typer.Exit(1)

    env = os.environ.copy()
    env["GIT_SSH_COMMAND"] = f'ssh -i "{str(key_path)}" -o IdentitiesOnly=yes'

    uv = ensure_uv_installed(project_dir)
    with status("Running sync_project steps..."):
        run_uv(uv, ["version", "--bump", "patch"], cwd=project_dir, env=env)
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
        run_cmd(["git", "push"], cwd=project_dir, env=env)

    success(f"Project synced: {repo_name}")


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
