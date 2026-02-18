from __future__ import annotations

"""
mainsequence.cli.project_status
===============================

Current project detection aligned with VS Code extension logic:

- Detect a project by path structure containing ".../projects/<folder>"
- Extract project id from "<slug>-<digits>" folder suffix
- If outside configured base, allow detection via .env markers:
    MAINSEQUENCE_TOKEN, TDAG_ENDPOINT
- Detect .venv and infer Python version
"""

import os
import pathlib
import re
import subprocess
from collections.abc import Iterable
from dataclasses import dataclass


@dataclass
class CurrentProjectInfo:
    path: str
    folder: str
    project_id: str | None = None
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
    project_id: str | None = None
    env_path: str | None = None
    env_exists: bool | None = None
    env_markers: list[str] | None = None
    env_has_markers: bool | None = None
    reason: str | None = None


@dataclass
class CurrentProjectDebug:
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
    return base_norm.endswith(sep) or (len(target_norm) > len(base_norm) and target_norm[len(base_norm)] == sep)


def detect_current_project(
    workspaces: Iterable[str],
    base_dir: str,
) -> tuple[CurrentProjectInfo | None, CurrentProjectDebug]:
    """
    Detect the current MainSequence project from a list of workspace directories.

    Args:
        workspaces: list of candidate dirs (in CLI typically [cwd])
        base_dir: configured base folder (mainsequence_path)

    Returns:
        (project_info | None, debug)
    """
    workspaces = list(workspaces or [])
    debug = CurrentProjectDebug(workspaces=workspaces, base_dir=base_dir, checks=[])

    if not workspaces:
        debug.reason = "no-workspace"
        return None, debug

    for w in workspaces:
        project, check = _analyze_workspace(w, base_dir)
        debug.checks.append(check)
        if project:
            debug.selected_workspace = w
            debug.reason = check.reason or "detected"
            return project, debug

    debug.reason = "no-matches"
    return None, debug


def _analyze_workspace(workspace_dir: str, base_dir: str) -> tuple[CurrentProjectInfo | None, WorkspaceCheckDebug]:
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

        parts = list(resolved_workspace.parts)
        parts_cmp = [p.lower() for p in parts] if os.name == "nt" else parts
        idx = len(parts_cmp) - 1 - list(reversed(parts_cmp)).index("projects") if "projects" in parts_cmp else -1
        check.projects_segment_index = idx

        if idx == -1 or idx >= len(parts) - 1:
            check.reason = "missing-projects-segment"
            return None, check

        folder = parts[idx + 1]
        check.projects_folder = folder

        m = re.search(r"-(\d+)$", folder)
        pid = m.group(1) if m else None
        check.project_id = pid

        venv_path, pyver = _detect_venv_info(resolved_workspace)
        project = CurrentProjectInfo(
            path=str(resolved_workspace),
            folder=folder,
            project_id=pid,
            venv_path=venv_path,
            python_version=pyver,
        )

        if check.within_base:
            check.reason = "detected-in-base"
            return project, check

        env_path, env_exists, markers, has_markers = _read_env_markers(resolved_workspace)
        check.env_path = env_path
        check.env_exists = env_exists
        check.env_markers = markers
        check.env_has_markers = has_markers

        if has_markers:
            check.reason = "detected-via-env"
            return project, check

        check.reason = "outside-base-and-no-env" if base_dir else "no-base-and-no-env"
        return None, check

    except Exception as e:
        check.reason = f"error:{e}"
        return None, check


def _read_env_markers(workspace: pathlib.Path) -> tuple[str, bool, list[str], bool]:
    env_path = str(workspace / ".env")
    markers = ["MAINSEQUENCE_TOKEN", "TDAG_ENDPOINT"]
    try:
        p = workspace / ".env"
        if not p.exists():
            return env_path, False, [], False
        content = p.read_text(encoding="utf-8", errors="replace")
        found = [m for m in markers if re.search(rf"^{re.escape(m)}=", content, flags=re.M)]
        return env_path, True, found, bool(found)
    except Exception:
        return env_path, False, [], False


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
