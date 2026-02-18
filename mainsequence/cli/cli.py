from __future__ import annotations

"""
mainsequence.cli.cli
====================

MainSequence CLI entrypoint.

Parity with VS Code extension:
- settings set-backend
- logout (clear tokens)
- project freeze-env (compile environment)
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
import subprocess
import sys
import time

import typer

from . import config as cfg
from .api import (
    ApiError,
    NotLoggedIn,
    add_deploy_key,
    deep_find_repo_url,
    fetch_project_env_text,
    get_current_user_profile,
    get_project_token,
    get_projects,
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


# ---------- top-level commands ----------


@app.command()
def login(
    email: str = typer.Argument(..., help="Email/username (server expects 'email' field)"),
    password: str | None = typer.Option(None, prompt=True, hide_input=True, help="Password"),
    no_status: bool = typer.Option(False, "--no-status", help="Do not print projects table after login"),
):
    """
    Login to the MainSequence platform.

    This stores access/refresh tokens in the CLI config folder and enables project operations.
    """
    try:
        res = api_login(email, password)
    except ApiError as e:
        error(f"Login failed: {e}")
        raise typer.Exit(1)

    cfg_obj = cfg.get_config()
    base = cfg_obj["mainsequence_path"]
    success(f"Signed in as {res['username']} (Backend: {res['backend']})")
    info(f"Projects base folder: {base}")

    if not no_status:
        try:
            items = get_projects()
            org_slug = _org_slug_from_profile()
            typer.echo("\nProjects:")
            typer.echo(_render_projects_table(items, base, org_slug))
        except NotLoggedIn:
            error("Not logged in.")


@app.command("logout")
def logout():
    """
    Log out by deleting stored tokens (token.json).

    Mirrors VS Code extension "Log out" behavior.
    """
    ok = cfg.clear_tokens()
    if ok:
        success("Signed out (tokens cleared).")
    else:
        warn("Signed out (tokens cleared), but some files could not be removed.")


@app.command("doctor")
def doctor():
    """
    Print a diagnostics report (config paths, backend URL, external dependencies).
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
    Bundle all markdown files in examples/ai/instructions and copy to the clipboard.
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
    """`mainsequence settings` defaults to `show`."""
    if ctx.invoked_subcommand is None:
        settings_show()
        raise typer.Exit()


@settings.command("show")
def settings_show():
    """Show current configuration (backend_url + mainsequence_path)."""
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
    """Set the projects base folder (where projects are cloned)."""
    out = cfg.set_config({"mainsequence_path": path})
    success(f"Projects base folder set to: {out['mainsequence_path']}")


@settings.command("set-backend")
def settings_set_backend(
    url: str = typer.Argument(..., help="Backend base URL, e.g. https://main-sequence.app")
):
    """Persist backend_url to config.json (parity with VS Code extension settings UI)."""
    out = cfg.set_backend_url(url)
    success(f"Backend URL set to: {out.get('backend_url')}")


# ---------- sdk group ----------


@sdk.command("latest")
def sdk_latest():
    """
    Print the latest SDK version on GitHub (same resolution logic as VS Code extension).
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
    List projects visible to your account (requires login).

    Shows Local status by scanning your projects base folder for folders ending in '-<id>'.
    """
    _require_login()
    cfg_obj = cfg.get_config()
    base = cfg_obj["mainsequence_path"]
    org_slug = _org_slug_from_profile()
    items = get_projects()
    typer.echo(_render_projects_table(items, base, org_slug))


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
    Clone a MainSequence project locally and create a .env file with tokens + backend.

    Parity with VS Code extension:
      - ensure SSH key and copy public key to clipboard
      - best-effort add deploy key
      - git clone
      - fetch environment + project token
      - write .env: MAINSEQUENCE_TOKEN, TDAG_ENDPOINT, INGORE_MS_AGENT=true
      - (optional) scaffold Dockerfile + .dockerignore if DEFAULT_BASE_IMAGE exists
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
    Open the local folder in the OS file manager.

    (CLI equivalent to VS Code "Open Folder", but uses OS file manager)
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
    Delete the local folder for a project.

    Safety:
      - only deletes within <base>/<org>/projects
      - prompts unless --yes is provided
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
    Open a terminal with ssh-agent started and project SSH key added.

    Parity:
      - VS Code opens an integrated terminal. CLI opens an external terminal window.
    """
    project_dir = _resolve_project_dir(project_id, path)

    origin = git_origin(project_dir)
    name = repo_name_from_git_url(origin) or project_dir.name
    key_path, _, _ = ensure_key_for_repo(origin)  # creates if missing
    open_signed_terminal(str(project_dir), key_path, name)


@project.command("freeze-env")
def project_freeze_env(
    project_id: int | None = typer.Argument(None, help="Project ID"),
    path: str | None = typer.Option(None, "--path", help="Project directory"),
    ensure_uv: bool = typer.Option(True, "--ensure-uv/--no-ensure-uv", help="Install uv into .venv if missing"),
):
    """
    Compile / freeze environment into requirements.txt using uv export.

    VS Code parity:
      - requires .venv
      - runs uv export -> requirements.txt
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
    Sync a project: bump version, resolve env, export requirements, commit and push.

    Mirrors VS Code "Sync Project" workflow:
      1) Ensure uv is installed
      2) uv version --bump <patch>
      3) uv lock, uv sync
      4) uv export locked requirements.txt
      5) git add -A; git commit -m; git push
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
    Build Docker environment for a project and optionally create devcontainer config.

    VS Code parity:
      - requires Dockerfile
      - docker buildx build --platform linux/amd64 --load
      - writes .devcontainer/devcontainer.json
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
    Detect and display the current project based on the current working directory.

    Mirrors VS Code extension "Current Project" detection:
      - finds /projects/<folder>
      - project id from suffix -<digits>
      - fallback detection via .env markers
      - shows .venv and python version if available
      - (bonus) shows SDK status if requirements.txt exists
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
    Show local vs latest SDK versions for a project.
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
    Update the project's SDK to the latest by upgrading mainsequence via uv.

    Mirrors VS Code extension:
      - activate .venv (implicitly by using venv uv)
      - pip install uv
      - uv lock --upgrade-package mainsequence
      - uv sync
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
