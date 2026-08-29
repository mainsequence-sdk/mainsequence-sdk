"""
mainsequence.cli.project_status
===============================

Current project detection from the containing Git worktree.
"""

from __future__ import annotations

import os
import pathlib
import re
import subprocess
from collections.abc import Iterable
from dataclasses import dataclass


@dataclass
class CurrentCodeRepositoryInfo:
    path: str
    folder: str
    venv_path: str | None = None
    python_version: str | None = None


@dataclass
class WorkspaceCheckDebug:
    workspace_dir: str | None = None
    resolved_workspace: str | None = None
    exists: bool | None = None
    base_dir: str | None = None
    resolved_base: str | None = None
    base_exists: bool | None = None
    within_base: bool | None = None
    projects_segment_index: int | None = None
    projects_folder: str | None = None
    git_root: str | None = None
    reason: str | None = None


@dataclass
class CurrentCodeRepositoryDebug:
    workspaces: list[str]
    selected_workspace: str | None = None
    base_dir: str | None = None
    reason: str | None = None
    checks: list[WorkspaceCheckDebug] | None = None


def _normalize_path_for_compare(p: pathlib.Path) -> str:
    resolved = str(p.resolve())
    root = str(p.resolve().anchor)
    if len(resolved) > len(root):
        resolved = resolved.rstrip("\\/")

    if os.name == "nt":
        resolved = resolved.lower()
    return resolved


def is_path_within(base: pathlib.Path, target: pathlib.Path) -> bool:
    """
    Return True if target is inside base (or equal), with cross-platform normalization.
    """
    base_norm = _normalize_path_for_compare(base)
    target_norm = _normalize_path_for_compare(target)
    if target_norm == base_norm:
        return True
    if not target_norm.startswith(base_norm):
        return False
    sep = "\\" if os.name == "nt" else "/"
    return base_norm.endswith(sep) or (
        len(target_norm) > len(base_norm) and target_norm[len(base_norm)] == sep
    )


def detect_current_code_repository(
    workspaces: Iterable[str],
    base_dir: str,
) -> tuple[CurrentCodeRepositoryInfo | None, CurrentCodeRepositoryDebug]:
    """
    Detect the current MainSequence project from a list of workspace directories.

    Args:
        workspaces: list of candidate dirs (in CLI typically [cwd])
        base_dir: configured base folder (mainsequence_path)

    Returns:
        (project_info | None, debug)
    """
    workspaces = list(workspaces or [])
    debug = CurrentCodeRepositoryDebug(workspaces=workspaces, base_dir=base_dir, checks=[])

    if not workspaces:
        debug.reason = "no-workspace"
        return None, debug

    for w in workspaces:
        code_repository, check = _analyze_workspace(w, base_dir)
        debug.checks.append(check)
        if code_repository:
            debug.selected_workspace = w
            debug.reason = check.reason or "detected"
            return code_repository, debug

    debug.reason = "no-matches"
    return None, debug


def _analyze_workspace(
    workspace_dir: str, base_dir: str
) -> tuple[CurrentCodeRepositoryInfo | None, WorkspaceCheckDebug]:
    check = WorkspaceCheckDebug(workspace_dir=workspace_dir, base_dir=base_dir)

    try:
        resolved_workspace = pathlib.Path(workspace_dir).resolve()
        check.resolved_workspace = str(resolved_workspace)
        check.exists = resolved_workspace.exists()

        resolved_base = pathlib.Path(base_dir).resolve() if base_dir else None
        if resolved_base:
            check.resolved_base = str(resolved_base)
            check.base_exists = resolved_base.exists()
            check.within_base = is_path_within(resolved_base, resolved_workspace)

        git_root_result = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            cwd=str(resolved_workspace),
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        git_root_text = git_root_result.stdout.strip() if git_root_result.returncode == 0 else ""
        if not git_root_text:
            check.reason = "not-in-git-worktree"
            return None, check
        git_root = pathlib.Path(git_root_text).resolve()
        check.git_root = str(git_root)
        folder = git_root.name
        check.projects_folder = folder
        venv_path, pyver = _detect_venv_info(git_root)
        code_repository = CurrentCodeRepositoryInfo(
            path=str(git_root),
            folder=folder,
            venv_path=venv_path,
            python_version=pyver,
        )
        check.reason = "detected-git-worktree"
        return code_repository, check

    except Exception as e:
        check.reason = f"error:{e}"
        return None, check


def _detect_venv_info(workspace: pathlib.Path) -> tuple[str | None, str | None]:
    venv = workspace / ".venv"
    if not (venv.exists() and venv.is_dir()):
        return None, None

    pyver = _read_python_version_from_pyvenv(venv) or _probe_python_version(venv)
    return str(venv), pyver


def _read_python_version_from_pyvenv(venv: pathlib.Path) -> str | None:
    cfg = venv / "pyvenv.cfg"
    if not cfg.exists():
        return None
    try:
        txt = cfg.read_text(encoding="utf-8", errors="replace")
        m = re.search(r"^\s*version(?:_info)?\s*=\s*(.+)\s*$", txt, flags=re.I | re.M)
        return m.group(1).strip() if m else None
    except Exception:
        return None


def _probe_python_version(venv: pathlib.Path) -> str | None:
    try:
        py = venv / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
        if not py.exists():
            return None
        r = subprocess.run(
            [str(py), "-c", 'import sys; print(".".join(map(str, sys.version_info[:3])))'],
            capture_output=True,
            text=True,
            timeout=2,
        )
        if r.returncode == 0 and r.stdout.strip():
            return r.stdout.strip()
    except Exception:
        pass
    return None
