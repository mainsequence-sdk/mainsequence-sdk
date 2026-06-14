"""Reusable helpers for copying packaged scaffold skills into projects.

Extension libraries can use this module to copy their own packaged skill bundle
into `.agents/skills/<namespace>/` without depending on the Main Sequence CLI
implementation. Successful copies always write a `PINNED_FROM.txt` sentinel that
records the installed library version that supplied the copied skills.
"""

from __future__ import annotations

import datetime as _datetime
import re
import shutil
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path

PINNED_FROM_FILENAME = "PINNED_FROM.txt"
PINNED_FROM_SCHEMA = "1"
_NAMESPACE_RE = re.compile(r"^[a-z0-9_]+$")
_UNKNOWN_VERSIONS = {"", "unknown", "none", "null"}


class ScaffoldSkillCopyBlocked(RuntimeError):
    """Raised before copying when a source/destination guard blocks the operation."""


@dataclass(frozen=True)
class CopiedScaffoldSkill:
    """One skill directory selected for copying."""

    name: str
    source: Path
    destination: Path


@dataclass(frozen=True)
class ScaffoldSkillCopyResult:
    """Result payload returned by `copy_scaffold_skills`."""

    library_name: str
    namespace: str
    project_dir: Path
    skills_path: Path
    destination_root: Path
    sentinel_path: Path
    pinned_version: str
    dry_run: bool
    copied: list[CopiedScaffoldSkill]


def copy_scaffold_skills(
    *,
    project_dir: Path,
    library_name: str,
    skills_path: Path,
    pinned_version: str,
    command: str | None = None,
    namespace: str | None = None,
    dry_run: bool = False,
    protected_project_roots: Sequence[Path] = (),
    project_guard: Callable[[Path], str | None] | None = None,
) -> ScaffoldSkillCopyResult:
    """Copy packaged scaffold skills into a managed project skill namespace.

    The helper copies each immediate child skill directory from `skills_path` to
    `<project_dir>/.agents/skills/<namespace>/`, overwriting only matching
    managed skill folders. Files, hidden folders, and folders starting with
    `__` are skipped.

    `pinned_version` is required and must be the installed version of the
    library that owns `skills_path`. On successful non-dry-run copies, the
    helper writes `PINNED_FROM.txt` in the destination namespace using that
    version.

    The helper refuses to copy when the source and destination overlap, when
    `project_dir` is inside a protected source checkout, or when `project_guard`
    returns a block reason.
    """

    resolved_library_name = _validate_library_name(library_name)
    resolved_namespace = normalize_scaffold_skill_namespace(namespace or resolved_library_name)
    resolved_pinned_version = _validate_pinned_version(pinned_version)
    resolved_project_dir = project_dir.expanduser().resolve(strict=False)
    resolved_skills_path = skills_path.expanduser().resolve(strict=False)
    if not resolved_skills_path.is_dir():
        raise FileNotFoundError(f"Scaffold skill source directory does not exist: {skills_path}")

    destination_root = (
        resolved_project_dir / ".agents" / "skills" / resolved_namespace
    ).resolve(strict=False)
    sentinel_path = destination_root / PINNED_FROM_FILENAME

    _check_project_guards(
        project_dir=resolved_project_dir,
        protected_project_roots=protected_project_roots,
        project_guard=project_guard,
    )
    _check_path_overlap(
        source=resolved_skills_path,
        destination=destination_root,
        source_label="skills_path",
        destination_label="destination_root",
    )

    copied = [
        CopiedScaffoldSkill(
            name=source_dir.name,
            source=source_dir,
            destination=destination_root / source_dir.name,
        )
        for source_dir in _iter_skill_roots(resolved_skills_path)
    ]
    for item in copied:
        _check_path_overlap(
            source=item.source,
            destination=item.destination,
            source_label=f"source skill {item.name!r}",
            destination_label=f"destination skill {item.name!r}",
        )

    result = ScaffoldSkillCopyResult(
        library_name=resolved_library_name,
        namespace=resolved_namespace,
        project_dir=resolved_project_dir,
        skills_path=resolved_skills_path,
        destination_root=destination_root,
        sentinel_path=sentinel_path,
        pinned_version=resolved_pinned_version,
        dry_run=dry_run,
        copied=copied,
    )

    if dry_run:
        return result

    for item in copied:
        _copy_tree_overwrite(item.source, item.destination)
    _write_pin_sentinel(
        result,
        command=command,
        copied_at_utc=_utc_timestamp(),
    )
    return result


def normalize_scaffold_skill_namespace(value: str) -> str:
    """Normalize and validate a library-owned scaffold skill namespace."""

    namespace = value.strip().lower().replace("-", "_").replace(".", "_")
    if not namespace or not _NAMESPACE_RE.fullmatch(namespace):
        raise ValueError(
            "Scaffold skill namespace must contain only lowercase ASCII letters, "
            "numbers, and underscores."
        )
    return namespace


def _validate_library_name(value: str) -> str:
    if not isinstance(value, str):
        raise ValueError("library_name is required.")
    library_name = value.strip()
    if not library_name:
        raise ValueError("library_name is required.")
    return library_name


def _validate_pinned_version(value: str) -> str:
    if not isinstance(value, str):
        raise ValueError("pinned_version must be a resolved installed library version.")
    pinned_version = value.strip()
    if pinned_version.lower() in _UNKNOWN_VERSIONS:
        raise ValueError("pinned_version must be a resolved installed library version.")
    return pinned_version


def _iter_skill_roots(skills_path: Path) -> list[Path]:
    return [
        item
        for item in sorted(skills_path.iterdir(), key=lambda child: child.name)
        if item.is_dir() and not item.name.startswith(".") and not item.name.startswith("__")
    ]


def _copy_tree_overwrite(source: Path, destination: Path) -> None:
    if destination.exists():
        shutil.rmtree(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(source, destination)


def _write_pin_sentinel(
    result: ScaffoldSkillCopyResult,
    *,
    command: str | None,
    copied_at_utc: str,
) -> None:
    result.destination_root.mkdir(parents=True, exist_ok=True)
    content = "\n".join(
        [
            f"schema={PINNED_FROM_SCHEMA}",
            f"library_name={result.library_name}",
            f"namespace={result.namespace}",
            f"pinned_version={result.pinned_version}",
            f"skills_path={result.skills_path}",
            f"copied_at_utc={copied_at_utc}",
            f"command={command or ''}",
            "",
        ]
    )
    result.sentinel_path.write_text(content, encoding="utf-8")


def _utc_timestamp() -> str:
    now = _datetime.datetime.now(tz=_datetime.UTC)
    return now.isoformat(timespec="seconds").replace("+00:00", "Z")


def _check_project_guards(
    *,
    project_dir: Path,
    protected_project_roots: Sequence[Path],
    project_guard: Callable[[Path], str | None] | None,
) -> None:
    for protected_root in protected_project_roots:
        resolved_root = protected_root.expanduser().resolve(strict=False)
        if _same_or_inside(project_dir, resolved_root):
            raise ScaffoldSkillCopyBlocked(
                "Blocked: target project is inside a protected library source checkout "
                f"({resolved_root})."
            )

    if project_guard is None:
        return
    reason = project_guard(project_dir)
    if reason:
        raise ScaffoldSkillCopyBlocked(reason)


def _check_path_overlap(
    *,
    source: Path,
    destination: Path,
    source_label: str,
    destination_label: str,
) -> None:
    resolved_source = source.expanduser().resolve(strict=False)
    resolved_destination = destination.expanduser().resolve(strict=False)
    if _same_or_inside(resolved_destination, resolved_source) or _same_or_inside(
        resolved_source,
        resolved_destination,
    ):
        raise ScaffoldSkillCopyBlocked(
            "Blocked: scaffold skill copy source and destination overlap "
            f"({source_label}={resolved_source}, {destination_label}={resolved_destination})."
        )


def _same_or_inside(path: Path, possible_parent: Path) -> bool:
    return path == possible_parent or path.is_relative_to(possible_parent)
