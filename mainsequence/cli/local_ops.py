"""
mainsequence.cli.local_ops
==========================

Local operations shared by several commands:

- Resolve CodeRepository path from UID or --path
- Ensure .venv exists and locate venv python/uv
- Run uv and git commands with nice error messages
"""

from __future__ import annotations

import json
import os
import pathlib
import shutil
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


def venv_paths(code_repository_dir: pathlib.Path) -> VenvPaths:
    venv = code_repository_dir / ".venv"
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


def ensure_venv(code_repository_dir: pathlib.Path) -> VenvPaths:
    """
    Ensure .venv exists and python executable is present.

    Raises:
        RuntimeError: if missing
    """
    vp = venv_paths(code_repository_dir)
    if not vp.venv_dir.exists() or not vp.venv_dir.is_dir():
        raise RuntimeError("A virtual environment needs to be set first (.venv not found).")
    if not vp.python.exists():
        raise RuntimeError("Virtual environment python not found inside .venv.")
    return vp


def ensure_uv_installed(code_repository_dir: pathlib.Path, upgrade: bool = True) -> pathlib.Path:
    """
    Resolve a usable uv executable for the code-repository workflow.

    Returns:
        Path to uv executable
    """
    vp = ensure_venv(code_repository_dir)
    if vp.uv and vp.uv.exists():
        return vp.uv

    uv_bin = shutil.which("uv")
    if uv_bin:
        return pathlib.Path(uv_bin)

    raise RuntimeError("uv executable not found. Install uv and ensure it is available in PATH.")


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


def uv_project_version(
    uv_path: pathlib.Path,
    cwd: pathlib.Path,
    env: dict[str, str] | None = None,
) -> str:
    """Return the current package version reported by ``uv version --short``."""
    result = subprocess.run(
        [str(uv_path), "version", "--short"],
        cwd=str(cwd),
        env=env,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()
        suffix = f": {detail}" if detail else ""
        raise RuntimeError(
            f"Command failed ({result.returncode}): {uv_path} version --short{suffix}"
        )
    version = (result.stdout or "").strip()
    if not version:
        raise RuntimeError("uv version --short returned an empty package version.")
    return version.splitlines()[-1].strip()


def uv_preview_patch_version(
    uv_path: pathlib.Path,
    cwd: pathlib.Path,
    env: dict[str, str] | None = None,
) -> str:
    """Return the version ``uv version --bump patch`` would produce without mutation."""
    result = subprocess.run(
        [
            str(uv_path),
            "version",
            "--bump",
            "patch",
            "--dry-run",
            "--output-format",
            "json",
        ],
        cwd=str(cwd),
        env=env,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()
        suffix = f": {detail}" if detail else ""
        raise RuntimeError(
            f"Command failed ({result.returncode}): {uv_path} version --bump patch "
            f"--dry-run --output-format json{suffix}"
        )
    try:
        payload = json.loads(result.stdout or "")
    except json.JSONDecodeError as exc:
        raise RuntimeError("uv version patch preview returned invalid JSON.") from exc
    version = payload.get("version") if isinstance(payload, dict) else None
    if not isinstance(version, str) or not version.strip():
        raise RuntimeError("uv version patch preview returned no package version.")
    return version.strip()


def git_origin(code_repository_dir: pathlib.Path) -> str:
    """
    Return git remote origin URL.

    Raises:
        RuntimeError if missing.
    """
    r = subprocess.run(
        ["git", "-C", str(code_repository_dir), "remote", "get-url", "origin"],
        capture_output=True,
        text=True,
    )
    origin = (r.stdout or "").strip().splitlines()
    if r.returncode == 0 and origin:
        return origin[-1]
    raise RuntimeError('Could not find Git remote "origin" for this CodeRepository.')


def uv_export_requirements(
    uv_path: pathlib.Path,
    cwd: pathlib.Path,
    locked: bool = True,
    no_dev: bool = True,
    no_hashes: bool = True,
    output_file: str = "requirements.txt",
) -> None:
    """
    Export the locked runtime dependency closure using uv.

    Development dependencies are excluded by default. For uv output flag
    compatibility, try:
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
