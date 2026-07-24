"""Dual-source Main Sequence project skill assembly.

The existing ``mainsequence project update_agent_skills`` command has two
canonical inputs:

* SDK-owned execution skills from the target project's installed
  ``agent_scaffold/skills`` tree.
* Platform-owned skills retrieved from the authenticated backend.

Platform content is validated in memory and written only to the target
project. It is never persisted in the installed SDK package.
"""

from __future__ import annotations

import hashlib
import os
import re
import shutil
import tempfile
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from .scaffold_skills import (
    PINNED_FROM_FILENAME,
    CopiedScaffoldSkill,
    copy_scaffold_skills,
)

DUAL_SOURCE_SENTINEL_SCHEMA = "2"
PLATFORM_CATALOG_SYNC_SOURCE = "platform_manifest"
PLATFORM_CAPABILITY_SOURCE_PREFIX = "mainsequence:platform-capability:"
PLATFORM_ONTOLOGY_URI = "mainsequence://platform/ontology"

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_PLATFORM_SKILL_NAME_RE = re.compile(r"^[a-z0-9_]+$")


class ProjectSkillAssemblyError(RuntimeError):
    """The combined SDK/platform skill update cannot be completed safely."""


@dataclass(frozen=True)
class PlatformProjectSkill:
    """One validated platform-owned skill returned by the backend."""

    uid: str
    name: str
    source_ref: str
    relative_path: PurePosixPath
    content: str
    content_sha256: str
    content_mime_type: str
    content_size: int


@dataclass(frozen=True)
class PlatformProjectSkillCatalog:
    """One complete validated platform catalog revision."""

    source_url: str
    manifest_version: int
    manifest_sha256: str
    ontology_uri: str
    ontology_sha256: str
    capabilities: tuple[PlatformProjectSkill, ...]


@dataclass(frozen=True)
class InstalledProjectSkill:
    """One skill written to the managed project namespace."""

    name: str
    owner: str
    source: str
    destination: Path
    content_sha256: str | None = None


@dataclass(frozen=True)
class DualSourceProjectSkillInstallResult:
    """Result of one completed dual-source project skill update."""

    project_dir: Path
    destination_root: Path
    sentinel_path: Path
    sdk_library_name: str
    sdk_version: str
    sdk_skills_path: Path
    platform_catalog: PlatformProjectSkillCatalog
    installed: tuple[InstalledProjectSkill, ...]


def parse_platform_project_skill_catalog(
    rows: Sequence[dict[str, Any]],
    *,
    source_url: str,
) -> PlatformProjectSkillCatalog:
    """Validate DRF platform capability rows and their attached content responses."""

    if not rows:
        raise ProjectSkillAssemblyError(
            "The platform returned no fixed platform skills."
        )

    normalized_source_url = _single_line_text(
        source_url,
        label="platform source URL",
    ).rstrip("/")
    if not normalized_source_url:
        raise ProjectSkillAssemblyError("platform source URL is required.")

    expected_manifest_version: int | None = None
    expected_manifest_sha256: str | None = None
    expected_capability_count: int | None = None
    expected_ontology_uri: str | None = None
    expected_ontology_sha256: str | None = None
    capabilities: list[PlatformProjectSkill] = []
    seen_names: set[str] = set()
    seen_source_refs: set[str] = set()
    seen_paths: set[PurePosixPath] = set()

    for index, raw_row in enumerate(rows):
        row = _mapping(raw_row, label=f"platform capability row {index}")
        metadata = _mapping(
            row.get("metadata"),
            label=f"platform capability row {index}.metadata",
        )
        catalog_metadata = _mapping(
            metadata.get("platform_catalog"),
            label=f"platform capability row {index}.metadata.platform_catalog",
        )
        if catalog_metadata.get("sync_source") != PLATFORM_CATALOG_SYNC_SOURCE:
            raise ProjectSkillAssemblyError(
                f"platform capability row {index} is not manifest-owned."
            )

        manifest_version = catalog_metadata.get("manifest_version")
        if not isinstance(manifest_version, int) or manifest_version < 1:
            raise ProjectSkillAssemblyError(
                f"platform capability row {index} has an invalid manifest version."
            )
        manifest_sha256 = _sha256(
            catalog_metadata.get("manifest_sha256"),
            label=f"platform capability row {index} manifest hash",
        )
        capability_count = catalog_metadata.get("capability_count")
        if not isinstance(capability_count, int) or capability_count < 1:
            raise ProjectSkillAssemblyError(
                f"platform capability row {index} has an invalid capability count."
            )
        ontology_uri = _single_line_text(
            catalog_metadata.get("ontology_uri"),
            label=f"platform capability row {index} ontology URI",
        )
        if ontology_uri != PLATFORM_ONTOLOGY_URI:
            raise ProjectSkillAssemblyError(
                f"platform capability row {index} has an unexpected ontology URI."
            )
        ontology_sha256 = _sha256(
            catalog_metadata.get("ontology_sha256"),
            label=f"platform capability row {index} ontology hash",
        )

        manifest_identity = (
            manifest_version,
            manifest_sha256,
            capability_count,
            ontology_uri,
            ontology_sha256,
        )
        expected_identity = (
            expected_manifest_version,
            expected_manifest_sha256,
            expected_capability_count,
            expected_ontology_uri,
            expected_ontology_sha256,
        )
        if expected_manifest_version is None:
            (
                expected_manifest_version,
                expected_manifest_sha256,
                expected_capability_count,
                expected_ontology_uri,
                expected_ontology_sha256,
            ) = manifest_identity
        elif manifest_identity != expected_identity:
            raise ProjectSkillAssemblyError(
                "Platform capability rows do not describe one catalog revision."
            )

        if row.get("kind") != "skill":
            raise ProjectSkillAssemblyError(
                f"platform capability row {index} is not a skill."
            )
        if row.get("source_type") != "registry" or row.get("is_editable") is not False:
            raise ProjectSkillAssemblyError(
                f"platform capability row {index} is not a fixed registry capability."
            )

        uid = _single_line_text(
            row.get("uid"),
            label=f"platform capability row {index} UID",
        )
        name = _single_line_text(
            row.get("name"),
            label=f"platform capability row {index} name",
        )
        if not _PLATFORM_SKILL_NAME_RE.fullmatch(name):
            raise ProjectSkillAssemblyError(
                f"platform capability name {name!r} is not a safe skill folder."
            )
        source_ref = _single_line_text(
            row.get("source_ref"),
            label=f"platform capability row {index} source reference",
        )
        if not source_ref.startswith(PLATFORM_CAPABILITY_SOURCE_PREFIX):
            raise ProjectSkillAssemblyError(
                f"platform capability {name!r} has an invalid source reference."
            )
        capability_path = _safe_capability_path(
            row.get("capability_path"),
            name=name,
        )
        if catalog_metadata.get("source_path") != str(capability_path):
            raise ProjectSkillAssemblyError(
                f"platform capability {name!r} path does not match manifest metadata."
            )
        relative_path = PurePosixPath(*capability_path.parts[1:])

        declared_sha256 = _sha256(
            row.get("content_sha256"),
            label=f"platform capability {name!r} content hash",
        )
        declared_mime_type = _single_line_text(
            row.get("content_mime_type"),
            label=f"platform capability {name!r} content MIME type",
        )
        if declared_mime_type != "text/markdown":
            raise ProjectSkillAssemblyError(
                f"platform capability {name!r} is not UTF-8 markdown."
            )
        declared_size = row.get("content_size")
        if not isinstance(declared_size, int) or declared_size < 0:
            raise ProjectSkillAssemblyError(
                f"platform capability {name!r} has an invalid content size."
            )

        content_response = _mapping(
            row.get("_content"),
            label=f"platform capability {name!r} content response",
        )
        content = content_response.get("content")
        if not isinstance(content, str):
            raise ProjectSkillAssemblyError(
                f"platform capability {name!r} content must be UTF-8 text."
            )
        content_bytes = content.encode("utf-8")
        actual_sha256 = hashlib.sha256(content_bytes).hexdigest()
        response_sha256 = _sha256(
            content_response.get("content_sha256"),
            label=f"platform capability {name!r} response hash",
        )
        if actual_sha256 != declared_sha256 or response_sha256 != declared_sha256:
            raise ProjectSkillAssemblyError(
                f"platform capability {name!r} content hash mismatch."
            )
        if content_response.get("content_mime_type") != declared_mime_type:
            raise ProjectSkillAssemblyError(
                f"platform capability {name!r} content MIME type mismatch."
            )
        if content_response.get("content_size") != declared_size:
            raise ProjectSkillAssemblyError(
                f"platform capability {name!r} content size metadata mismatch."
            )
        if len(content_bytes) != declared_size:
            raise ProjectSkillAssemblyError(
                f"platform capability {name!r} content byte-size mismatch."
            )

        if name in seen_names or source_ref in seen_source_refs or relative_path in seen_paths:
            raise ProjectSkillAssemblyError(
                "Platform capability names, source references, and paths must be unique."
            )
        seen_names.add(name)
        seen_source_refs.add(source_ref)
        seen_paths.add(relative_path)
        capabilities.append(
            PlatformProjectSkill(
                uid=uid,
                name=name,
                source_ref=source_ref,
                relative_path=relative_path,
                content=content,
                content_sha256=declared_sha256,
                content_mime_type=declared_mime_type,
                content_size=declared_size,
            )
        )

    if expected_capability_count != len(capabilities):
        raise ProjectSkillAssemblyError(
            "The platform capability response is incomplete: "
            f"expected {expected_capability_count}, received {len(capabilities)}."
        )
    if [capability.name for capability in capabilities] != sorted(seen_names):
        raise ProjectSkillAssemblyError(
            "Platform capability rows must use deterministic name ordering."
        )

    assert expected_manifest_version is not None
    assert expected_manifest_sha256 is not None
    assert expected_ontology_uri is not None
    assert expected_ontology_sha256 is not None
    return PlatformProjectSkillCatalog(
        source_url=normalized_source_url,
        manifest_version=expected_manifest_version,
        manifest_sha256=expected_manifest_sha256,
        ontology_uri=expected_ontology_uri,
        ontology_sha256=expected_ontology_sha256,
        capabilities=tuple(capabilities),
    )


def install_dual_source_project_skills(
    *,
    project_dir: Path,
    sdk_library_name: str,
    sdk_skills_path: Path,
    sdk_version: str,
    platform_catalog: PlatformProjectSkillCatalog,
    namespace: str = "mainsequence",
    command: str = "mainsequence project update_agent_skills",
    protected_project_roots: Sequence[Path] = (),
) -> DualSourceProjectSkillInstallResult:
    """Install SDK-owned and platform-owned skills as one validated project tree."""

    sdk_plan = copy_scaffold_skills(
        project_dir=project_dir,
        library_name=sdk_library_name,
        namespace=namespace,
        skills_path=sdk_skills_path,
        pinned_version=sdk_version,
        command=command,
        dry_run=True,
        protected_project_roots=protected_project_roots,
    )
    _validate_source_ownership(
        sdk_skills=sdk_plan.copied,
        platform_capabilities=platform_catalog.capabilities,
    )

    destination_root = sdk_plan.destination_root
    destination_parent = destination_root.parent
    destination_parent.mkdir(parents=True, exist_ok=True)
    staging_root = Path(
        tempfile.mkdtemp(
            prefix=f".{destination_root.name}.staging-",
            dir=destination_parent,
        )
    )

    installed: list[InstalledProjectSkill] = []
    try:
        for item in sdk_plan.copied:
            staged_destination = staging_root / item.name
            shutil.copytree(item.source, staged_destination)
            installed.append(
                InstalledProjectSkill(
                    name=item.name,
                    owner="sdk",
                    source=str(item.source),
                    destination=destination_root / item.name,
                )
            )

        for capability in platform_catalog.capabilities:
            staged_destination = staging_root.joinpath(*capability.relative_path.parts)
            staged_destination.parent.mkdir(parents=True, exist_ok=True)
            staged_destination.write_text(capability.content, encoding="utf-8")
            installed.append(
                InstalledProjectSkill(
                    name=capability.name,
                    owner="platform",
                    source=capability.source_ref,
                    destination=destination_root.joinpath(
                        *capability.relative_path.parts
                    ),
                    content_sha256=capability.content_sha256,
                )
            )

        sentinel_path = staging_root / PINNED_FROM_FILENAME
        sentinel_path.write_text(
            render_dual_source_sentinel(
                sdk_library_name=sdk_library_name,
                namespace=namespace,
                sdk_version=sdk_version,
                sdk_skills_path=sdk_plan.skills_path,
                platform_catalog=platform_catalog,
                command=command,
            ),
            encoding="utf-8",
        )
        _replace_managed_tree(
            staging_root=staging_root,
            destination_root=destination_root,
        )
    except Exception:
        if staging_root.exists():
            shutil.rmtree(staging_root, ignore_errors=True)
        raise

    return DualSourceProjectSkillInstallResult(
        project_dir=sdk_plan.project_dir,
        destination_root=destination_root,
        sentinel_path=destination_root / PINNED_FROM_FILENAME,
        sdk_library_name=sdk_library_name,
        sdk_version=sdk_version,
        sdk_skills_path=sdk_plan.skills_path,
        platform_catalog=platform_catalog,
        installed=tuple(installed),
    )


def render_dual_source_sentinel(
    *,
    sdk_library_name: str,
    namespace: str,
    sdk_version: str,
    sdk_skills_path: Path,
    platform_catalog: PlatformProjectSkillCatalog,
    command: str,
) -> str:
    """Render the backward-readable schema-2 dual-source provenance record."""

    installed_at_utc = _utc_timestamp()
    lines = [
        f"schema={DUAL_SOURCE_SENTINEL_SCHEMA}",
        f"library_name={_sentinel_value(sdk_library_name)}",
        f"namespace={_sentinel_value(namespace)}",
        f"pinned_version={_sentinel_value(sdk_version)}",
        f"skills_path={_sentinel_value(str(sdk_skills_path.resolve()))}",
        f"copied_at_utc={installed_at_utc}",
        f"installed_at_utc={installed_at_utc}",
        f"sdk_library_name={_sentinel_value(sdk_library_name)}",
        f"sdk_version={_sentinel_value(sdk_version)}",
        f"sdk_skills_path={_sentinel_value(str(sdk_skills_path.resolve()))}",
        f"platform_source_url={_sentinel_value(platform_catalog.source_url)}",
        f"platform_retrieved_at_utc={installed_at_utc}",
        f"platform_manifest_version={platform_catalog.manifest_version}",
        f"platform_manifest_sha256={platform_catalog.manifest_sha256}",
        f"platform_ontology_uri={platform_catalog.ontology_uri}",
        f"platform_ontology_sha256={platform_catalog.ontology_sha256}",
        f"platform_capability_count={len(platform_catalog.capabilities)}",
    ]
    for capability in platform_catalog.capabilities:
        prefix = f"platform_capability.{capability.name}"
        lines.extend(
            [
                f"{prefix}.source_ref={_sentinel_value(capability.source_ref)}",
                f"{prefix}.path={_sentinel_value(str(capability.relative_path))}",
                f"{prefix}.content_sha256={capability.content_sha256}",
            ]
        )
    lines.extend(
        [
            f"command={_sentinel_value(command)}",
            "",
        ]
    )
    return "\n".join(lines)


def _validate_source_ownership(
    *,
    sdk_skills: Sequence[CopiedScaffoldSkill],
    platform_capabilities: Sequence[PlatformProjectSkill],
) -> None:
    sdk_roots = {item.name for item in sdk_skills}
    collisions = sorted(
        {
            capability.relative_path.parts[0]
            for capability in platform_capabilities
            if capability.relative_path.parts[0] in sdk_roots
        }
    )
    if collisions:
        raise ProjectSkillAssemblyError(
            "SDK/platform skill path ownership collision: "
            + ", ".join(collisions)
            + ". Refactor the SDK-owned skill path before updating the project."
        )


def _replace_managed_tree(*, staging_root: Path, destination_root: Path) -> None:
    backup_root = destination_root.parent / (
        f".{destination_root.name}.backup-{uuid.uuid4().hex}"
    )
    destination_existed = destination_root.exists()
    if destination_existed:
        os.replace(destination_root, backup_root)
    try:
        os.replace(staging_root, destination_root)
    except Exception as install_exc:
        try:
            if destination_existed and backup_root.exists():
                os.replace(backup_root, destination_root)
        except Exception as rollback_exc:
            raise ProjectSkillAssemblyError(
                "Project skill installation failed and rollback also failed: "
                f"{rollback_exc}. The previous managed tree remains at "
                f"{backup_root} for manual recovery."
            ) from install_exc
        raise
    if backup_root.exists():
        shutil.rmtree(backup_root, ignore_errors=True)


def _safe_capability_path(value: Any, *, name: str) -> PurePosixPath:
    raw_path = _single_line_text(
        value,
        label=f"platform capability {name!r} path",
    )
    path = PurePosixPath(raw_path)
    if (
        path.is_absolute()
        or ".." in path.parts
        or "." in path.parts
        or len(path.parts) != 3
        or path.parts[0] != "skills"
        or path.parts[1] != name
        or path.parts[2] not in {"SKILL.md", "SKILL.markdown"}
    ):
        raise ProjectSkillAssemblyError(
            f"platform capability {name!r} has an unsafe or unexpected path."
        )
    return path


def _mapping(value: Any, *, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ProjectSkillAssemblyError(f"{label} must be a JSON object.")
    return value


def _sha256(value: Any, *, label: str) -> str:
    normalized = _single_line_text(value, label=label).lower()
    if not _SHA256_RE.fullmatch(normalized):
        raise ProjectSkillAssemblyError(f"{label} must be a lowercase SHA-256.")
    return normalized


def _single_line_text(value: Any, *, label: str) -> str:
    if not isinstance(value, str):
        raise ProjectSkillAssemblyError(f"{label} must be text.")
    normalized = value.strip()
    if not normalized or "\n" in normalized or "\r" in normalized:
        raise ProjectSkillAssemblyError(f"{label} must be non-empty single-line text.")
    return normalized


def _sentinel_value(value: str) -> str:
    return _single_line_text(value, label="sentinel value")


def _utc_timestamp() -> str:
    import datetime as _datetime

    now = _datetime.datetime.now(tz=_datetime.UTC)
    return now.isoformat(timespec="seconds").replace("+00:00", "Z")
