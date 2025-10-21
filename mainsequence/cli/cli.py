# mainsequence/cli/cli.py
from __future__ import annotations

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
    deep_find_repo_url,
    fetch_project_env_text,
    get_current_user_profile,
    get_project_token,
    get_projects,
    repo_name_from_git_url,
    safe_slug,
)
from .api import login as api_login
from .ssh_utils import (
    ensure_key_for_repo,
    open_folder,
    open_signed_terminal,
    start_agent_and_add_key,
)

app = typer.Typer(help="MainSequence CLI (login + project operations)")

project = typer.Typer(help="Project commands (set up locally, signed terminal, etc.)")
settings = typer.Typer(help="Settings (base folder, backend, etc.)")

app.add_typer(project, name="project")
app.add_typer(settings, name="settings")



# ---------- AI instructions utilities ----------

INSTR_REL_PATH = pathlib.Path("examples") / "ai" / "instructions"


def _git_root() -> pathlib.Path | None:
    """Return the git repo root (if any), else None."""
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "--show-toplevel"], text=True
        ).strip()
        if out:
            return pathlib.Path(out)
    except Exception:
        pass
    return None


def _find_instructions_dir(
    start: pathlib.Path | None = None,
    rel_path: pathlib.Path = INSTR_REL_PATH,
) -> pathlib.Path | None:
    """
    Starting at CWD (or 'start'), walk upward and return the first '<ancestor>/examples/ai/instructions'.
    """
    start = start or pathlib.Path.cwd()
    for base in [start] + list(start.parents):
        cand = base / rel_path
        if cand.is_dir():
            return cand
    # If the caller passed the folder directly
    if start.is_dir() and start.name == rel_path.name:
        return start
    return None


def _natural_key(p: pathlib.Path):
    # Natural sort so "10-..." comes after "2-..."
    return [int(s) if s.isdigit() else s.lower() for s in re.split(r"(\d+)", p.name)]


def _collect_markdown_files(
    d: pathlib.Path, recursive: bool = False
) -> list[pathlib.Path]:
    patterns = ["*.md", "*.markdown", "*.mdx"]
    files: list[pathlib.Path] = []
    if recursive:
        for pat in patterns:
            files.extend(d.rglob(pat))
    else:
        for pat in patterns:
            files.extend(d.glob(pat))
    # Dedupe + natural order
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
        try:
            txt = f.read_text(encoding="utf-8")
        except Exception:
            txt = f.read_text(encoding="utf-8", errors="replace")
        # normalize newlines, avoid trailing blank bloat
        parts.append(txt.replace("\r\n", "\n").replace("\r", "\n").rstrip() + "\n")
    return "".join(parts)


def copy_instructions_to_clipboard(
    instructions_dir: str | os.PathLike[str] | None = None,
    recursive: bool = False,
    also_write_to: str | None = None,
) -> bool:
    """
    Collect all markdown under 'examples/ai/instructions' (or a provided folder),
    bundle them with clear file headers, copy to clipboard, and optionally write to disk.

    Returns True if the clipboard copy succeeded; False otherwise.
    """
    base = pathlib.Path(instructions_dir).expanduser().resolve() if instructions_dir else _find_instructions_dir()
    if not base or not base.is_dir():
        raise RuntimeError(
            "Instructions folder not found. Pass a valid path or run from inside your repo."
        )

    files = _collect_markdown_files(base, recursive=recursive)
    if not files:
        raise RuntimeError(f"No markdown files found in: {base}")

    bundle = _bundle_markdown(files, title="AI Instructions", repo_root=_git_root())

    if also_write_to:
        pathlib.Path(also_write_to).write_text(bundle, encoding="utf-8")

    ok = _copy_clipboard(bundle)
    if not ok:
        # Provide a useful fallback
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
        extra = (
            extra.get("extra_arguments")
            or (p.get("data_source") or {}).get("extra_arguments")
            or {}
        )
        repo = deep_find_repo_url(extra) or ""
    return repo


def _copy_clipboard(txt: str) -> bool:
    """
    Cross‑platform clipboard copy with robust Linux handling:

      - Windows:   PowerShell Set-Clipboard (preferred) or clip.exe
      - macOS:     pbcopy
      - Wayland:   wl-copy (also sets --primary)
      - X11:       xclip/xsel; write to BOTH CLIPBOARD and PRIMARY
                   and keep the helper alive in background so paste works
      - WSL:       use Windows clip.exe

    Returns True iff a backend was invoked and did not immediately fail.
    """
    try:
        # --- Windows ---
        if sys.platform == "win32":
            for ps in ("powershell.exe", "pwsh.exe"):
                if shutil.which(ps):
                    p = subprocess.run(
                        [ps, "-NoProfile", "-Command",
                         "Set-Clipboard -Value ([Console]::In.ReadToEnd())"],
                        input=txt, text=True, capture_output=True
                    )
                    if p.returncode == 0:
                        return True
            if shutil.which("clip.exe"):
                p = subprocess.run(["clip.exe"], input=txt, text=True, capture_output=True)
                return p.returncode == 0
            return False

        # --- macOS ---
        if sys.platform == "darwin":
            p = subprocess.run(["pbcopy"], input=txt, text=True, capture_output=True)
            return p.returncode == 0

        # --- Linux / *nix (including WSL) ---
        # WSL → Windows clipboard
        if os.environ.get("WSL_DISTRO_NAME") and shutil.which("clip.exe"):
            p = subprocess.run(["clip.exe"], input=txt, text=True, capture_output=True)
            return p.returncode == 0

        wayland = os.environ.get("WAYLAND_DISPLAY")
        x11 = os.environ.get("DISPLAY")

        # Wayland (only if a Wayland display exists)
        if wayland and shutil.which("wl-copy"):
            ok1 = subprocess.run(["wl-copy"], input=txt, text=True, capture_output=True).returncode == 0
            # Also set primary selection (best-effort)
            if shutil.which("wl-copy"):
                subprocess.run(["wl-copy", "--primary"], input=txt, text=True, capture_output=True)
            return ok1

        # X11
        if x11:
            # Prefer xclip if available
            if shutil.which("xclip"):
                try:
                    procs = []
                    for sel in ("clipboard", "primary"):
                        proc = subprocess.Popen(
                            ["xclip", "-selection", sel, "-in", "-quiet"],
                            stdin=subprocess.PIPE,
                            stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL,
                            text=True,
                            close_fds=True,
                            start_new_session=True,
                        )
                        assert proc.stdin is not None
                        proc.stdin.write(txt)
                        proc.stdin.close()
                        procs.append(proc)
                    # Give xclip a moment to claim ownership; detect immediate failure
                    time.sleep(0.05)
                    immediate_fail = all(p.poll() is not None and p.returncode != 0 for p in procs)
                    return not immediate_fail
                except Exception:
                    pass

            # Fallback: xsel
            if shutil.which("xsel"):
                try:
                    procs = []
                    for args in (["--clipboard", "--input"], ["--primary", "--input"]):
                        proc = subprocess.Popen(
                            ["xsel", *args],
                            stdin=subprocess.PIPE,
                            stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL,
                            text=True,
                            close_fds=True,
                            start_new_session=True,
                        )
                        assert proc.stdin is not None
                        proc.stdin.write(txt)
                        proc.stdin.close()
                        procs.append(proc)
                    time.sleep(0.05)
                    immediate_fail = all(p.poll() is not None and p.returncode != 0 for p in procs)
                    return not immediate_fail
                except Exception:
                    pass

        # Nothing suitable
        return False

    except Exception:
        return False




def _canonical_project_dir(base_dir: str, org_slug: str, project_id: int | str, project_name: str) -> pathlib.Path:
    slug = safe_slug(project_name or "project")
    return _projects_root(base_dir, org_slug) / f"{slug}-{project_id}"


def _legacy_project_dir(base_dir: str, org_slug: str, project_name: str) -> pathlib.Path:
    slug = safe_slug(project_name or "project")
    return _projects_root(base_dir, org_slug) / slug


def _find_local_dir_by_id(
    base_dir: str,
    org_slug: str,
    project_id: int | str,
    project_name: str | None = None,
) -> str | None:
    """
    Find a local directory for a project id by folder structure only.
    Preference order:
      0) Hints:
         - Current working directory (or any parent) under the projects root whose
           name ends with '-<id>'.
         - $VFB_PROJECT_PATH if it points under the projects root and ends with '-<id>'.
      1) <slug>-<id> (canonical)
      1b) Scan projects root for any folder ending with '-<id>' (in case name changed)
      2) <slug> (legacy fallback; only used if #1 missing and name is provided)
    """

    # --- Normalize id to a clean string like "57"
    def _clean_id(val: int | str) -> str:
        s = str(val).strip()
        try:
            # drop leading zeros / spaces, handle accidental string ids
            s = str(int(s))
        except Exception:
            # keep best-effort string if it can't be int-cast
            pass
        return s

    pid = _clean_id(project_id)
    suffix = f"-{pid}"
    root = _projects_root(base_dir, org_slug)

    if root.exists():
        # 0a) Environment hint
        env_path = os.environ.get("VFB_PROJECT_PATH", "").strip()
        if env_path:
            try:
                p = pathlib.Path(env_path).expanduser().resolve()
                # only trust paths under our projects root
                p.relative_to(root)
                if p.is_dir() and p.name.endswith(suffix):
                    return str(p)
            except Exception:
                pass

        # 0b) CWD hint: if we're inside the project folder, prefer that
        try:
            cwd = pathlib.Path.cwd().resolve()
            for parent in [cwd] + list(cwd.parents):
                try:
                    parent.relative_to(root)
                except Exception:
                    # walked above projects root
                    continue
                if parent.is_dir() and parent.name.endswith(suffix):
                    return str(parent)
        except Exception:
            # Don't let a filesystem quirk break normal resolution
            pass

        # 1) Canonical <slug>-<id>, if we know the name
        if project_name:
            cand = _canonical_project_dir(base_dir, org_slug, pid, project_name)
            if cand.exists():
                return str(cand)

        # 1b) Fallback: single scan in case name changed or we didn't have it
        try:
            for d in root.iterdir():
                if d.is_dir() and d.name.endswith(suffix):
                    return str(d)
        except FileNotFoundError:
            pass

    # 2) Legacy <slug> fallback (requires a name)
    if project_name:
        legacy = _legacy_project_dir(base_dir, org_slug, project_name)
        if legacy.exists():
            return str(legacy)

    return None

def _render_projects_table(items: list[dict], base_dir: str, org_slug: str) -> str:
    """Return an aligned table with Local status + path (map or default folder guess)."""

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
        status = ds(p, "data_source.related_resource.status", "")

        local_path = _find_local_dir_by_id(base_dir, org_slug, pid, name)
        local = "Local" if local_path else "—"
        path_col = local_path or "—"
        rows.append((pid, name, dname, klass, status, local, path_col))

    header = ["ID", "Project", "Data Source", "Class", "Status", "Local", "Path"]
    if not rows:
        return "No projects."

    colw = [max(len(r[i]) for r in rows + [tuple(header)]) for i in range(len(header))]
    fmt = "  ".join("{:<" + str(colw[i]) + "}" for i in range(len(header)))
    out = [fmt.format(*header), fmt.format(*["-" * len(h) for h in header])]
    for r in rows:
        out.append(fmt.format(*r))
    return "\n".join(out)


# ---------- top-level commands ----------


@app.command()
def login(
    email: str = typer.Argument(..., help="Email/username (server expects 'email' field)"),
    password: str | None = typer.Option(None, prompt=True, hide_input=True, help="Password"),
    no_status: bool = typer.Option(
        False, "--no-status", help="Do not print projects table after login"
    ),
):
    """
    Login to the Main Sequence platform to  set up projects locally. to login: mainsequence login <email>
    """
    try:
        res = api_login(email, password)
    except ApiError as e:
        typer.secho(f"Login failed: {e}", fg=typer.colors.RED)
        raise typer.Exit(1)

    cfg_obj = cfg.get_config()
    base = cfg_obj["mainsequence_path"]
    typer.secho(
        f"Signed in as {res['username']} (Backend: {res['backend']})", fg=typer.colors.GREEN
    )
    typer.echo(f"Projects base folder: {base}")

    if not no_status:
        try:
            items = get_projects()
            org_slug = _org_slug_from_profile()
            typer.echo("\nProjects:")
            typer.echo(_render_projects_table(items, base, org_slug))
        except NotLoggedIn:
            typer.secho("Not logged in.", fg=typer.colors.RED)


# ---------- settings group ----------


@settings.callback(invoke_without_command=True)
def settings_cb(ctx: typer.Context):
    """`mainsequence settings` defaults to `show`."""
    if ctx.invoked_subcommand is None:
        settings_show()
        raise typer.Exit()


@settings.command("show")
def settings_show():
    c = cfg.get_config()
    typer.echo(
        json.dumps(
            {"backend_url": c.get("backend_url"), "mainsequence_path": c.get("mainsequence_path")},
            indent=2,
        )
    )


@settings.command("set-base")
def settings_set_base(path: str = typer.Argument(..., help="New projects base folder")):
    out = cfg.set_config({"mainsequence_path": path})
    typer.secho(f"Projects base folder set to: {out['mainsequence_path']}", fg=typer.colors.GREEN)


# ---------- project group (require login) ----------


@project.callback()
def project_guard():
    try:
        prof = get_current_user_profile()
        if not prof or not prof.get("username"):
            raise NotLoggedIn("Not logged in.")
    except NotLoggedIn:
        typer.secho("Not logged in. Run: mainsequence login <email>", fg=typer.colors.RED)
        raise typer.Exit(1)
    except ApiError:
        typer.secho("Not logged in. Run: mainsequence login <email>", fg=typer.colors.RED)
        raise typer.Exit(1)


@project.command("list")
def project_list():
    """List projects with Local status and path."""
    cfg_obj = cfg.get_config()
    base = cfg_obj["mainsequence_path"]
    org_slug = _org_slug_from_profile()

    items = get_projects()

    typer.echo(_render_projects_table(items, base, org_slug))


@project.command("open")
def project_open(project_id: int):
    """Open the local folder in the OS file manager."""
    cfg_obj = cfg.get_config()
    base = cfg_obj["mainsequence_path"]
    org_slug = _org_slug_from_profile()
    items = get_projects()
    p = next((x for x in items if str(x.get("id")) == str(project_id)), None)
    path = _find_local_dir_by_id(base, org_slug, project_id, p.get("project_name") if p else None)
    if not path or not pathlib.Path(path).exists():
        typer.secho(
            "No local folder mapped for this project. Run `set-up-locally` first.",
            fg=typer.colors.RED,
        )
        raise typer.Exit(1)
    open_folder(path)
    typer.echo(f"Opened: {path}")


@project.command("delete-local")
def project_delete_local(
    project_id: int,
):
    """Delete the local folder for this project id based on folder structure."""
    cfg_obj = cfg.get_config()
    base = cfg_obj["mainsequence_path"]
    org_slug = _org_slug_from_profile()

    # Pure folder-structure resolution: <slug>-<id>, or legacy <slug> if needed
    items = get_projects()
    pinfo = next((x for x in items if str(x.get("id")) == str(project_id)), None)
    project_name = pinfo.get("project_name") if pinfo else None
    found = _find_local_dir_by_id(base, org_slug, project_id, project_name)
    if not found:
        typer.echo("No local folder found for this project.")
        return
    p = pathlib.Path(found)

    # Safety: delete only inside projects root
    projects_root = _projects_root(base, org_slug).resolve()
    try:
        p.resolve().relative_to(projects_root)
    except Exception:
        typer.secho(f"Refusing to delete outside projects root: {p}", fg=typer.colors.RED)
        return
    if p.exists():
        import shutil

        shutil.rmtree(str(p), ignore_errors=True)
        typer.secho(f"Deleted: {str(p)}", fg=typer.colors.YELLOW)

    else:
        typer.echo("Folder already absent.")


@project.command("open-signed-terminal")
def project_open_signed_terminal(project_id: int):
    """Open a terminal window in the project directory with ssh-agent started and the repo's key added."""
    cfg_obj = cfg.get_config()
    base = cfg_obj["mainsequence_path"]
    org_slug = _org_slug_from_profile()
    items = get_projects()
    p = next((x for x in items if str(x.get("id")) == str(project_id)), None)
    dir_ = _find_local_dir_by_id(base, org_slug, project_id, p.get("project_name") if p else None)

    if not dir_ or not pathlib.Path(dir_).exists():
        typer.secho(
            "No local folder mapped for this project. Run `set-up-locally` first.",
            fg=typer.colors.RED,
        )
        raise typer.Exit(1)

    proc = subprocess.run(
        ["git", "-C", dir_, "remote", "get-url", "origin"], text=True, capture_output=True
    )
    origin = (proc.stdout or "").strip().splitlines()[-1] if proc.returncode == 0 else ""
    name = repo_name_from_git_url(origin) or pathlib.Path(dir_).name
    key_path = pathlib.Path.home() / ".ssh" / name
    open_signed_terminal(dir_, key_path, name)


@project.command("set-up-locally")
def project_set_up_locally(
    project_id: int,
    base_dir: str | None = typer.Option(
        None, "--base-dir", help="Override base dir (default from settings)"
    ),
):
    cfg_obj = cfg.get_config()
    base = base_dir or cfg_obj["mainsequence_path"]

    org_slug = _org_slug_from_profile()

    items = get_projects()
    p = next((x for x in items if int(x.get("id", -1)) == project_id), None)
    if not p:
        typer.secho("Project not found/visible.", fg=typer.colors.RED)
        raise typer.Exit(1)

    repo = _determine_repo_url(p)
    if not repo:
        typer.secho("No repository URL found for this project.", fg=typer.colors.RED)
        raise typer.Exit(1)

    name = safe_slug(p.get("project_name") or f"project-{project_id}")
    projects_root = _projects_root(base, org_slug)
    # canonical path avoids collisions (no mapping file)
    target_dir = projects_root / f"{name}-{project_id}"
    projects_root.mkdir(parents=True, exist_ok=True)

    key_path, pub_path, pub = ensure_key_for_repo(repo)
    copied = _copy_clipboard(pub)

    try:
        host = platform.node()
        add_deploy_key(project_id, host, pub)
    except Exception:
        pass

    agent_env = start_agent_and_add_key(key_path)

    if target_dir.exists():
        typer.secho(f"Target already exists: {target_dir}", fg=typer.colors.RED)
        raise typer.Exit(2)

    env = os.environ.copy() | agent_env
    env["GIT_SSH_COMMAND"] = f'ssh -i "{str(key_path)}" -o IdentitiesOnly=yes'
    rc = subprocess.call(["git", "clone", repo, str(target_dir)], env=env, cwd=str(projects_root))
    if rc != 0:
        try:
            if target_dir.exists():
                import shutil

                shutil.rmtree(target_dir, ignore_errors=True)
        except Exception:
            pass
        typer.secho("git clone failed", fg=typer.colors.RED)
        raise typer.Exit(3)

    env_text = ""
    try:
        env_text = fetch_project_env_text(project_id)
    except Exception:
        env_text = ""
    env_text = (env_text or "").replace("\r", "")
    if any(line.startswith("VFB_PROJECT_PATH=") for line in env_text.splitlines()):
        lines = [
            f"VFB_PROJECT_PATH={str(target_dir)}" if line.startswith("VFB_PROJECT_PATH=") else line
            for line in env_text.splitlines()
        ]
        env_text = "\n".join(lines)
    else:
        if env_text and not env_text.endswith("\n"):
            env_text += "\n"
        env_text += f"VFB_PROJECT_PATH={str(target_dir)}\n"

    try:
        project_token = get_project_token(project_id)
    except NotLoggedIn:
        typer.secho(
            "Session expired or refresh failed. Run: mainsequence login <email>",
            fg=typer.colors.RED,
        )
        raise typer.Exit(1)
    except ApiError as e:
        typer.secho(f"Could not fetch project token: {e}", fg=typer.colors.RED)
        raise typer.Exit(1)

    lines = env_text.splitlines()
    if any(line.startswith("MAINSEQUENCE_TOKEN=") for line in lines):
        lines = [
            (
                f"MAINSEQUENCE_TOKEN={project_token}"
                if line.startswith("MAINSEQUENCE_TOKEN=")
                else line
            )
            for line in lines
        ]
        env_text = "\n".join(lines)
    else:
        if env_text and not env_text.endswith("\n"):
            env_text += "\n"
        env_text += f"MAINSEQUENCE_TOKEN={project_token}\n"

    # ---  ensure TDAG_ENDPOINT points at the current backend URL ---
    backend = cfg.backend_url()
    lines = env_text.splitlines()
    if any(line.startswith("TDAG_ENDPOINT=") for line in lines):
        env_text = "\n".join(
            (f"TDAG_ENDPOINT={backend}" if line.startswith("TDAG_ENDPOINT=") else line)
            for line in lines
        )
    else:
        if env_text and not env_text.endswith("\n"):
            env_text += "\n"
        env_text += f"TDAG_ENDPOINT={backend}\n"

    # --- ensure INGORE_MS_AGENT flag is present (default: true) ---
    lines = env_text.splitlines()
    if any(line.startswith("INGORE_MS_AGENT=") for line in lines):
        env_text = "\n".join(
            ("INGORE_MS_AGENT=true" if line.startswith("INGORE_MS_AGENT=") else line)
            for line in lines
        )
    else:
        if env_text and not env_text.endswith("\n"):
            env_text += "\n"
        env_text += "INGORE_MS_AGENT=true\n"

    # write final .env with both vars present
    (target_dir / ".env").write_text(env_text, encoding="utf-8")

    typer.secho(f"Local folder: {target_dir}", fg=typer.colors.GREEN)
    typer.echo(f"Repo URL: {repo}")
    if copied:
        typer.echo("Public key copied to clipboard.")


@app.command("build_and_run")
def build_and_run(
    dockerfile: str | None = typer.Argument(
        None, help="Path to Dockerfile to build & run. If omitted, only lock & export requirements."
    )
):
    """
    - uv lock
    - uv export --format requirements --no-dev --hashes > requirements.txt
    - If DOCKERFILE argument is given: docker build -f DOCKERFILE . && docker run IMAGE
    """

    # ----- sanity checks for uv + project files -----
    if shutil.which("uv") is None:
        typer.secho("uv is not installed. Install it with: pip install uv", fg=typer.colors.RED)
        raise typer.Exit(1)

    if not pathlib.Path("pyproject.toml").exists():
        typer.secho(f"pyproject.toml not found in {pathlib.Path.cwd()}", fg=typer.colors.RED)
        raise typer.Exit(1)

    # ----- 1) solve and lock -----
    typer.secho("Running: uv lock", fg=typer.colors.BLUE)
    p = subprocess.run(["uv", "lock"])
    if p.returncode != 0:
        typer.secho("uv lock failed.", fg=typer.colors.RED)
        raise typer.Exit(p.returncode)

    # ----- 2) export pinned, hashed requirements -----
    typer.secho("Exporting hashed requirements to requirements.txt", fg=typer.colors.BLUE)
    p = subprocess.run(
        ["uv", "export", "--format", "requirements", "--no-dev", "--hashes"],
        capture_output=True,
        text=True,
    )
    if p.returncode != 0:
        typer.secho("uv export failed:", fg=typer.colors.RED)
        if p.stderr:
            typer.echo(p.stderr.strip())
        raise typer.Exit(p.returncode)

    pathlib.Path("requirements.txt").write_text(p.stdout, encoding="utf-8")
    typer.secho("requirements.txt written.", fg=typer.colors.GREEN)

    # ----- 3) optional Docker build + run -----
    if dockerfile is None:
        typer.secho("No Dockerfile provided; skipping Docker build/run.", fg=typer.colors.BLUE)
        return

    df_path = pathlib.Path(dockerfile)
    if not df_path.exists():
        typer.secho(f"Dockerfile not found: {dockerfile}", fg=typer.colors.RED)
        raise typer.Exit(1)

    if shutil.which("docker") is None:
        typer.secho("Docker CLI is not installed or not on PATH.", fg=typer.colors.RED)
        raise typer.Exit(1)

    # Image name: directory-name + '-img' (overridable via env IMAGE_NAME)
    cwd_name = pathlib.Path.cwd().name
    safe_name = re.sub(r"[^a-z0-9_.-]+", "-", cwd_name.lower())
    image_name = os.environ.get("IMAGE_NAME", f"{safe_name}-img")

    # Tag: short git sha if available, else timestamp (overridable via env TAG)
    tag = os.environ.get("TAG")
    if not tag:
        try:
            tag = subprocess.check_output(
                ["git", "rev-parse", "--short", "HEAD"], text=True
            ).strip()
        except Exception:
            tag = time.strftime("%Y%m%d%H%M%S")

    image_ref = f"{image_name}:{tag}"

    typer.secho(f"Building Docker image: {image_ref}", fg=typer.colors.BLUE)
    build = subprocess.run(["docker", "build", "-f", str(df_path), "-t", image_ref, "."])
    if build.returncode != 0:
        typer.secho("docker build failed.", fg=typer.colors.RED)
        raise typer.Exit(build.returncode)

    typer.secho(f"Running container: {image_ref}", fg=typer.colors.BLUE)
    try:
        # interactive by default; relies on your ENTRYPOINT
        subprocess.check_call(["docker", "run", "--rm", "-it", image_ref])
    except subprocess.CalledProcessError as e:
        typer.secho(f"docker run failed (exit {e.returncode}).", fg=typer.colors.RED)
        raise typer.Exit(e.returncode)


@app.command("copy-llm-instructions")
def copy_llm_instructions(
    dir: str | None = typer.Option(
        None,
        "--dir",
        "-d",
        help="Path to the 'examples/ai/instructions' folder. If omitted, we search upward from CWD.",
    ),
    recursive: bool = typer.Option(
        False, "--recursive", "-r", help="Include nested subfolders."
    ),
    out: str | None = typer.Option(
        None, "--out", "-o", help="Also write the bundle to this file."
    ),
    print_: bool = typer.Option(
        False, "--print", help="Print the bundle to stdout instead of copying."
    ),
):
    """
    Collect all markdowns in examples/ai/instructions and copy the full bundle to the clipboard.
    """
    try:
        base = pathlib.Path(dir).expanduser().resolve() if dir else None
        if print_:
            # If printing, just emit to stdout
            found = base or _find_instructions_dir()
            if not found:
                typer.secho(
                    "Instructions folder not found. Pass --dir PATH or run from inside your repo.",
                    fg=typer.colors.RED,
                )
                raise typer.Exit(1)
            files = _collect_markdown_files(found, recursive=recursive)
            if not files:
                typer.secho(f"No markdown files found in: {found}", fg=typer.colors.RED)
                raise typer.Exit(1)
            bundle = _bundle_markdown(files, title="AI Instructions", repo_root=_git_root())
            if out:
                pathlib.Path(out).write_text(bundle, encoding="utf-8")
                typer.echo(f"Wrote bundle to: {out}")
            typer.echo(bundle)
            return

        ok = copy_instructions_to_clipboard(
            instructions_dir=str(base) if base else None,
            recursive=recursive,
            also_write_to=out,
        )
        if ok:
            typer.secho("Instructions copied to clipboard.", fg=typer.colors.GREEN)
        else:
            alt = out or (pathlib.Path.cwd() / "ai_instructions.txt")
            typer.secho(
                f"Clipboard unavailable. Wrote bundle to: {alt}",
                fg=typer.colors.YELLOW,
            )
            raise typer.Exit(2)
    except RuntimeError as e:
        typer.secho(str(e), fg=typer.colors.RED)
        raise typer.Exit(1)