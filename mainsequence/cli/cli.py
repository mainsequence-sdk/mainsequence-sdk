# mainsequence/cli/cli.py
from __future__ import annotations

import json
import os
import pathlib
import platform
import re
import shlex
import shutil
import subprocess
import sys
from typing import Optional

import typer

from . import config as cfg
from .api import (
    ApiError,
    NotLoggedIn,
    add_deploy_key,
    deep_find_repo_url,
    fetch_project_env_text,
    get_current_user_profile,
    get_projects,
    login as api_login,
    repo_name_from_git_url,
    safe_slug,
get_project_token
)
from .ssh_utils import (
    ensure_key_for_repo,
    open_folder,
    open_signed_terminal,
    start_agent_and_add_key,
)
import time

app = typer.Typer(help="MainSequence CLI (login + project operations)")

project = typer.Typer(help="Project commands (set up locally, signed terminal, etc.)")
settings = typer.Typer(help="Settings (base folder, backend, etc.)")

app.add_typer(project, name="project")
app.add_typer(settings, name="settings")

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

def _copy_clipboard(txt: str) -> bool:
    try:
        if sys.platform == "darwin":
            p = subprocess.run(["pbcopy"], input=txt, text=True)
            return p.returncode == 0
        elif shutil.which("wl-copy"):
            p = subprocess.run(["wl-copy"], input=txt, text=True)
            return p.returncode == 0
        elif shutil.which("xclip"):
            p = subprocess.run(["xclip", "-selection", "clipboard"], input=txt, text=True)
            return p.returncode == 0
    except Exception:
        pass
    return False

def _render_projects_table(items: list[dict], links: dict, base_dir: str, org_slug: str) -> str:
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
        pid   = str(p.get("id", ""))
        name  = p.get("project_name") or "(unnamed)"
        dname = ds(p, "data_source.related_resource.display_name", "")
        klass = ds(p, "data_source.related_resource.class_type",
                   ds(p, "data_source.related_resource_class_type", ""))
        status = ds(p, "data_source.related_resource.status", "")

        # 1) mapping file
        mapped = links.get(pid)
        local_path = mapped if mapped and pathlib.Path(mapped).exists() else None
        # 2) guess default location if mapping is absent
        if not local_path:
            guess = _projects_root(base_dir, org_slug) / safe_slug(name)
            if guess.exists():
                local_path = str(guess)

        local = "Local" if local_path else "—"
        path_col = local_path or "—"
        rows.append((pid, name, dname, klass, status, local, path_col))

    header = ["ID","Project","Data Source","Class","Status","Local","Path"]
    if not rows:
        return "No projects."

    colw = [max(len(r[i]) for r in rows + [tuple(header)]) for i in range(len(header))]
    fmt = "  ".join("{:<" + str(colw[i]) + "}" for i in range(len(header)))
    out = [fmt.format(*header), fmt.format(*["-"*len(h) for h in header])]
    for r in rows:
        out.append(fmt.format(*r))
    return "\n".join(out)

# ---------- top-level commands ----------

@app.command()
def login(
    email: str = typer.Argument(..., help="Email/username (server expects 'email' field)"),
    password: Optional[str] = typer.Option(None, prompt=True, hide_input=True, help="Password"),
    no_status: bool = typer.Option(False, "--no-status", help="Do not print projects table after login")
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
    typer.secho(f"Signed in as {res['username']} (Backend: {res['backend']})", fg=typer.colors.GREEN)
    typer.echo(f"Projects base folder: {base}")



    if not no_status:
        try:
            items = get_projects()
            links = cfg.get_links()
            org_slug = _org_slug_from_profile()
            typer.echo("\nProjects:")
            typer.echo(_render_projects_table(items, links, base, org_slug))
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
    typer.echo(json.dumps({
        "backend_url": c.get("backend_url"),
        "mainsequence_path": c.get("mainsequence_path")
    }, indent=2))

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
    links = cfg.get_links()
    typer.echo(_render_projects_table(items, links, base, org_slug))

@project.command("open")
def project_open(project_id: int):
    """Open the local folder in the OS file manager."""
    links = cfg.get_links()
    path = links.get(str(project_id))
    if not path or not pathlib.Path(path).exists():
        # also try default guess
        cfg_obj = cfg.get_config()
        base = cfg_obj["mainsequence_path"]
        org_slug = _org_slug_from_profile()
        items = get_projects()
        p = next((x for x in items if str(x.get("id")) == str(project_id)), None)
        if p:
            guess = _projects_root(base, org_slug) / safe_slug(p.get("project_name") or "")
            if guess.exists():
                path = str(guess)
    if not path or not pathlib.Path(path).exists():
        typer.secho("No local folder mapped for this project. Run `set-up-locally` first.", fg=typer.colors.RED)
        raise typer.Exit(1)
    open_folder(path)
    typer.echo(f"Opened: {path}")

@project.command("delete-local")
def project_delete_local(
    project_id: int,
):
    """Unlink the mapped folder, optionally delete it."""
    mapped = cfg.remove_link(project_id)
    if not mapped:
        typer.echo("No mapping found.")
        return
    p = pathlib.Path(mapped)
    if p.exists():
        import shutil
        shutil.rmtree(mapped, ignore_errors=True)
        typer.secho(f"Deleted: {mapped}", fg=typer.colors.YELLOW)

    else:
        typer.echo("Mapping removed; folder already absent.")

@project.command("open-signed-terminal")
def project_open_signed_terminal(project_id: int):
    """Open a terminal window in the project directory with ssh-agent started and the repo's key added."""
    links = cfg.get_links()
    dir_ = links.get(str(project_id))

    if not dir_ or not pathlib.Path(dir_).exists():
        # also try default guess
        cfg_obj = cfg.get_config()
        base = cfg_obj["mainsequence_path"]
        org_slug = _org_slug_from_profile()
        items = get_projects()
        p = next((x for x in items if str(x.get("id")) == str(project_id)), None)
        if p:
            guess = _projects_root(base, org_slug) / safe_slug(p.get("project_name") or "")
            if guess.exists():
                dir_ = str(guess)

    if not dir_ or not pathlib.Path(dir_).exists():
        typer.secho("No local folder mapped for this project. Run `set-up-locally` first.", fg=typer.colors.RED)
        raise typer.Exit(1)

    proc = subprocess.run(["git", "-C", dir_, "remote", "get-url", "origin"], text=True, capture_output=True)
    origin = (proc.stdout or "").strip().splitlines()[-1] if proc.returncode == 0 else ""
    name = repo_name_from_git_url(origin) or pathlib.Path(dir_).name
    key_path = pathlib.Path.home() / ".ssh" / name
    open_signed_terminal(dir_, key_path, name)

@project.command("set-up-locally")
def project_set_up_locally(
    project_id: int,
    base_dir: Optional[str] = typer.Option(None, "--base-dir", help="Override base dir (default from settings)")
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
    target_dir = projects_root / name
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
        lines = [f"VFB_PROJECT_PATH={str(target_dir)}" if line.startswith("VFB_PROJECT_PATH=") else line
                 for line in env_text.splitlines()]
        env_text = "\n".join(lines)
    else:
        if env_text and not env_text.endswith("\n"): env_text += "\n"
        env_text += f"VFB_PROJECT_PATH={str(target_dir)}\n"

    try:
        project_token = get_project_token(project_id)
    except NotLoggedIn:
        typer.secho("Session expired or refresh failed. Run: mainsequence login <email>", fg=typer.colors.RED)
        raise typer.Exit(1)
    except ApiError as e:
        typer.secho(f"Could not fetch project token: {e}", fg=typer.colors.RED)
        raise typer.Exit(1)

    lines = env_text.splitlines()
    if any(line.startswith("MAINSEQUENCE_TOKEN=") for line in lines):
        lines = [
            (f"MAINSEQUENCE_TOKEN={project_token}" if line.startswith("MAINSEQUENCE_TOKEN=") else line)
            for line in lines
        ]
        env_text = "\n".join(lines)
    else:
        if env_text and not env_text.endswith("\n"): env_text += "\n"
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
        if env_text and not env_text.endswith("\n"): env_text += "\n"
        env_text += f"TDAG_ENDPOINT={backend}\n"



    # write final .env with both vars present
    (target_dir / ".env").write_text(env_text, encoding="utf-8")
    cfg.set_link(project_id, str(target_dir))

    typer.secho(f"Local folder: {target_dir}", fg=typer.colors.GREEN)
    typer.echo(f"Repo URL: {repo}")
    if copied:
        typer.echo("Public key copied to clipboard.")

@app.command("build_and_run")
def build_and_run(dockerfile: Optional[str] = typer.Argument(
    None,
    help="Path to Dockerfile to build & run. If omitted, only lock & export requirements."
)):
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
        capture_output=True, text=True
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
            tag = subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], text=True).strip()
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

