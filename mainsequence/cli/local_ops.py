from __future__ import annotations

"""
mainsequence.cli.local_ops
==========================

Local operations shared by several commands:

- Resolve project path from id or --path
- Ensure .venv exists and locate venv python/uv
- Run uv and git commands with nice error messages
"""

import os
import pathlib
import subprocess
import sys
from dataclasses import dataclass


@dataclass(frozen=True)
class VenvPaths:
    venv_dir: pathlib.Path
    python: pathlib.Path
    uv: pathlib.Path | None


def normalize_path(p: str | os.PathLike[str]) -> pathlib.Path:
    return pathlib.Path(p).expanduser().resolve()


def venv_paths(project_dir: pathlib.Path) -> VenvPaths:
    venv = project_dir / ".venv"
    if sys.platform == "win32":
        py = venv / "Scripts" / "python.exe"
        uv = venv / "Scripts" / "uv.exe"
        if not uv.exists():
            alt = venv / "Scripts" / "uv"
            uv = alt if alt.exists() else None
    else:
        py = venv / "bin" / "python"
        uv = venv / "bin" / "uv"
        if not uv.exists():
            uv = None
    return VenvPaths(venv_dir=venv, python=py, uv=uv)


def ensure_venv(project_dir: pathlib.Path) -> VenvPaths:
    """
    Ensure .venv exists and python executable is present.

    Raises:
        RuntimeError: if missing
    """
    vp = venv_paths(project_dir)
    if not vp.venv_dir.exists() or not vp.venv_dir.is_dir():
        raise RuntimeError("A virtual environment needs to be set first (.venv not found).")
    if not vp.python.exists():
        raise RuntimeError("Virtual environment python not found inside .venv.")
    return vp


def ensure_uv_installed(project_dir: pathlib.Path, upgrade: bool = True) -> pathlib.Path:
    """
    Ensure uv is installed inside the project's .venv.

    Returns:
        Path to uv executable
    """
    vp = ensure_venv(project_dir)
    if vp.uv and vp.uv.exists():
        return vp.uv

    args = [str(vp.python), "-m", "pip", "install"]
    if upgrade:
        args.append("--upgrade")
    args.append("uv")
    r = subprocess.run(args, cwd=str(project_dir))
    if r.returncode != 0:
        raise RuntimeError("Failed to install uv into the virtual environment.")

    vp = venv_paths(project_dir)
    if not vp.uv or not vp.uv.exists():
        raise RuntimeError("uv installed but executable not found in .venv.")
    return vp.uv


def run_cmd(cmd: list[str], cwd: pathlib.Path, env: dict[str, str] | None = None) -> subprocess.CompletedProcess:
    """
    Run a command, streaming output.

    Raises:
        RuntimeError: if return code is non-zero
    """
    r = subprocess.run(cmd, cwd=str(cwd), env=env)
    if r.returncode != 0:
        raise RuntimeError(f"Command failed ({r.returncode}): {' '.join(cmd)}")
    return r


def run_uv(uv_path: pathlib.Path, args: list[str], cwd: pathlib.Path, env: dict[str, str] | None = None) -> None:
    """Run uv with args, raising on failure."""
    run_cmd([str(uv_path), *args], cwd=cwd, env=env)


def git_origin(project_dir: pathlib.Path) -> str:
    """
    Return git remote origin URL.

    Raises:
        RuntimeError if missing.
    """
    r = subprocess.run(
        ["git", "-C", str(project_dir), "remote", "get-url", "origin"],
        capture_output=True,
        text=True,
    )
    origin = (r.stdout or "").strip().splitlines()
    if r.returncode == 0 and origin:
        return origin[-1]
    raise RuntimeError('Could not find git remote "origin" for this project.')


def uv_export_requirements(
    uv_path: pathlib.Path,
    cwd: pathlib.Path,
    locked: bool = False,
    no_dev: bool = False,
    no_hashes: bool = True,
    output_file: str = "requirements.txt",
) -> None:
    """
    Export requirements using uv with compatibility fallbacks.

    The VS Code extension uses slightly different flags in different places; we try:
      1) modern: --format requirements-txt -o requirements.txt
      2) fallback: --format requirements.txt --output-file requirements.txt
    """
    base = ["export"]
    if locked:
        base.append("--locked")
    if no_dev:
        base.append("--no-dev")
    if no_hashes:
        base.append("--no-hashes")

    # Try modern format
    try:
        run_uv(uv_path, [*base, "--format", "requirements-txt", "-o", output_file], cwd=cwd)
        return
    except Exception:
        # fallback to alternate flags used by the extension
        run_uv(uv_path, [*base, "--format", "requirements.txt", "--output-file", output_file], cwd=cwd)
