"""Dual-source Main Sequence code repository skill assembly.

The existing ``mainsequence code-repository update-agent-skills`` command has two
canonical inputs:

* SDK-owned execution skills from the target code repository's installed
  ``agent_scaffold/skills`` tree.
* Platform-owned skills retrieved from authenticated MCP resources.

Platform content is validated in memory and written only to the target code
repository. It is never persisted in the installed SDK package.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

import yaml

from .scaffold_skills import (
    PINNED_FROM_FILENAME,
    CopiedScaffoldSkill,
    copy_scaffold_skills,
)

DUAL_SOURCE_SENTINEL_SCHEMA = "2"
PLATFORM_RESOURCE_NAMESPACE = "mainsequence://platform/"
PLATFORM_ONTOLOGY_URI = "mainsequence://platform/ontology"
PLATFORM_SKILL_URI_PREFIX = f"{PLATFORM_RESOURCE_NAMESPACE}skills/"
SUPPORTED_PLATFORM_MANIFEST_VERSIONS = frozenset({2})

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_PLATFORM_SKILL_NAME_RE = re.compile(r"^[a-z0-9]+(?:_[a-z0-9]+)*$")
_PLATFORM_SKILL_SLUG_RE = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
_PLATFORM_SKILL_FILENAMES = frozenset({"SKILL.md", "SKILL.markdown"})
_PLATFORM_ONTOLOGY_NAME = "Main Sequence platform ontology"
_PLATFORM_ONTOLOGY_PATH = PurePosixPath("ontology/platform.json")


class CodeRepositorySkillAssemblyError(RuntimeError):
    """The combined SDK/platform skill update cannot be completed safely."""


@dataclass(frozen=True)
class PlatformCodeRepositorySkillDeclaration:
    """One platform skill identity declared by the platform ontology."""

    name: str
    uri: str
    front_matter_name: str


@dataclass(frozen=True)
class _ValidatedPlatformResourcePayload:
    """One resource after list/read integrity validation."""

    listed_name: str
    uri: str
    resource_path: PurePosixPath
    content: str
    content_sha256: str
    content_mime_type: str
    content_size: int
    manifest_version: int
    manifest_sha256: str


@dataclass(frozen=True)
class PlatformCodeRepositoryResource:
    """One validated server-owned platform resource returned by MCP."""

    name: str
    uri: str
    resource_path: PurePosixPath
    content: str
    content_sha256: str
    content_mime_type: str
    content_size: int


@dataclass(frozen=True)
class PlatformCodeRepositorySkill(PlatformCodeRepositoryResource):
    """One platform resource installed as a managed CodeRepository skill."""

    relative_path: PurePosixPath


@dataclass(frozen=True)
class PlatformCodeRepositorySkillCatalog:
    """One complete validated server-owned resource manifest revision."""

    source_url: str
    manifest_version: int
    manifest_sha256: str
    ontology: PlatformCodeRepositoryResource
    skills: tuple[PlatformCodeRepositorySkill, ...]

    @property
    def ontology_uri(self) -> str:
        return self.ontology.uri

    @property
    def ontology_sha256(self) -> str:
        return self.ontology.content_sha256

    @property
    def resources(self) -> tuple[PlatformCodeRepositoryResource, ...]:
        return (self.ontology, *self.skills)


@dataclass(frozen=True)
class InstalledCodeRepositorySkill:
    """One skill written to the managed CodeRepository namespace."""

    name: str
    owner: str
    source: str
    destination: Path
    content_sha256: str | None = None


@dataclass(frozen=True)
class DualSourceCodeRepositorySkillInstallResult:
    """Result of one completed dual-source CodeRepository skill update."""

    code_repository_dir: Path
    destination_root: Path
    sentinel_path: Path
    sdk_library_name: str
    sdk_version: str
    sdk_skills_path: Path
    platform_catalog: PlatformCodeRepositorySkillCatalog
    installed: tuple[InstalledCodeRepositorySkill, ...]


def parse_platform_code_repository_skill_catalog(
    rows: Sequence[dict[str, Any]],
    *,
    source_url: str,
) -> PlatformCodeRepositorySkillCatalog:
    """Validate one complete server-owned MCP skill catalog."""

    if not rows:
        raise CodeRepositorySkillAssemblyError("The platform returned no MCP skill resources.")

    normalized_source_url = _single_line_text(
        source_url,
        label="platform source URL",
    )
    if not normalized_source_url:
        raise CodeRepositorySkillAssemblyError("platform source URL is required.")

    raw_rows_by_uri: dict[str, dict[str, Any]] = {}
    for index, raw_row in enumerate(rows):
        row = _mapping(raw_row, label=f"platform resource row {index}")
        uri = _single_line_text(
            row.get("uri"),
            label=f"platform resource row {index} URI",
        )
        if uri in raw_rows_by_uri:
            raise CodeRepositorySkillAssemblyError(
                f"The platform MCP resource catalog returned duplicate URI {uri!r}."
            )
        if uri != PLATFORM_ONTOLOGY_URI and not uri.startswith(PLATFORM_SKILL_URI_PREFIX):
            raise CodeRepositorySkillAssemblyError(f"Unsupported platform skill resource URI {uri!r}.")
        raw_rows_by_uri[uri] = row

    if PLATFORM_ONTOLOGY_URI not in raw_rows_by_uri:
        raise CodeRepositorySkillAssemblyError(
            f"The platform MCP resource catalog is missing {PLATFORM_ONTOLOGY_URI!r}."
        )

    validated_by_uri = {
        uri: _validate_platform_resource_payload(raw_row, uri=uri)
        for uri, raw_row in raw_rows_by_uri.items()
    }
    ontology_payload = validated_by_uri[PLATFORM_ONTOLOGY_URI]
    if ontology_payload.listed_name != _PLATFORM_ONTOLOGY_NAME:
        raise CodeRepositorySkillAssemblyError("The platform ontology resource has an unexpected name.")
    if ontology_payload.resource_path != _PLATFORM_ONTOLOGY_PATH:
        raise CodeRepositorySkillAssemblyError(
            "The platform ontology resource has an unexpected resource path."
        )
    if ontology_payload.content_mime_type != "application/json":
        raise CodeRepositorySkillAssemblyError("The platform ontology resource must use application/json.")

    declarations = parse_platform_code_repository_skill_declarations(ontology_payload.content)
    listed_skill_uris = set(validated_by_uri) - {PLATFORM_ONTOLOGY_URI}
    validate_platform_code_repository_skill_membership(
        declarations,
        listed_skill_uris=listed_skill_uris,
    )

    manifest_identity = (
        ontology_payload.manifest_version,
        ontology_payload.manifest_sha256,
    )
    for uri, payload in validated_by_uri.items():
        if (payload.manifest_version, payload.manifest_sha256) != manifest_identity:
            raise CodeRepositorySkillAssemblyError(
                f"Platform resource {uri!r} does not describe the ontology manifest revision."
            )

    ontology = PlatformCodeRepositoryResource(
        name="ontology",
        uri=ontology_payload.uri,
        resource_path=ontology_payload.resource_path,
        content=ontology_payload.content,
        content_sha256=ontology_payload.content_sha256,
        content_mime_type=ontology_payload.content_mime_type,
        content_size=ontology_payload.content_size,
    )
    skills: list[PlatformCodeRepositorySkill] = []
    destination_paths: set[PurePosixPath] = set()
    for declaration in declarations:
        payload = validated_by_uri[declaration.uri]
        label = f"platform resource {declaration.uri!r}"
        if payload.listed_name != declaration.name:
            raise CodeRepositorySkillAssemblyError(
                f"{label} listed name does not match ontology.skill_resources."
            )
        if payload.content_mime_type != "text/markdown":
            raise CodeRepositorySkillAssemblyError(f"{label} must use text/markdown.")
        _validate_platform_skill_path(
            payload.resource_path,
            label=label,
        )
        _validate_skill_front_matter(
            payload.content,
            expected_name=declaration.front_matter_name,
            label=label,
        )
        relative_path = PurePosixPath(*payload.resource_path.parts[1:])
        if relative_path in destination_paths:
            raise CodeRepositorySkillAssemblyError(
                f"{label} resolves to duplicate CodeRepository destination {relative_path!s}."
            )
        destination_paths.add(relative_path)
        skills.append(
            PlatformCodeRepositorySkill(
                name=declaration.name,
                uri=payload.uri,
                resource_path=payload.resource_path,
                relative_path=relative_path,
                content=payload.content,
                content_sha256=payload.content_sha256,
                content_mime_type=payload.content_mime_type,
                content_size=payload.content_size,
            )
        )

    return PlatformCodeRepositorySkillCatalog(
        source_url=normalized_source_url,
        manifest_version=ontology_payload.manifest_version,
        manifest_sha256=ontology_payload.manifest_sha256,
        ontology=ontology,
        skills=tuple(skills),
    )


def parse_platform_code_repository_skill_declarations(
    ontology_content: Any,
) -> tuple[PlatformCodeRepositorySkillDeclaration, ...]:
    """Return the validated, deterministic skill index from platform ontology JSON."""

    if not isinstance(ontology_content, str):
        raise CodeRepositorySkillAssemblyError("The platform ontology resource must contain UTF-8 text.")
    try:
        ontology = json.loads(ontology_content)
    except json.JSONDecodeError as exc:
        raise CodeRepositorySkillAssemblyError(
            "The platform ontology resource must contain valid JSON."
        ) from exc
    ontology = _mapping(ontology, label="platform ontology payload")
    raw_declarations = ontology.get("skill_resources")
    if not isinstance(raw_declarations, list):
        raise CodeRepositorySkillAssemblyError(
            "The platform ontology must contain a skill_resources array."
        )

    declarations: list[PlatformCodeRepositorySkillDeclaration] = []
    names: set[str] = set()
    uris: set[str] = set()
    for index, raw_declaration in enumerate(raw_declarations):
        label = f"platform ontology skill_resources[{index}]"
        declaration = _mapping(raw_declaration, label=label)
        name = _single_line_text(
            declaration.get("name"),
            label=f"{label} name",
        )
        if not _PLATFORM_SKILL_NAME_RE.fullmatch(name):
            raise CodeRepositorySkillAssemblyError(f"{label} name must use safe lowercase snake case.")
        uri = _single_line_text(
            declaration.get("uri"),
            label=f"{label} URI",
        )
        front_matter_name = _platform_skill_slug(uri, label=label)
        if name != front_matter_name.replace("-", "_"):
            raise CodeRepositorySkillAssemblyError(f"{label} name does not match its platform skill URI.")
        if name in names:
            raise CodeRepositorySkillAssemblyError(
                f"The platform ontology declares duplicate skill name {name!r}."
            )
        if uri in uris:
            raise CodeRepositorySkillAssemblyError(
                f"The platform ontology declares duplicate skill URI {uri!r}."
            )
        names.add(name)
        uris.add(uri)
        declarations.append(
            PlatformCodeRepositorySkillDeclaration(
                name=name,
                uri=uri,
                front_matter_name=front_matter_name,
            )
        )

    return tuple(sorted(declarations, key=lambda item: (item.name, item.uri)))


def validate_platform_code_repository_skill_membership(
    declarations: Sequence[PlatformCodeRepositorySkillDeclaration],
    *,
    listed_skill_uris: Sequence[str] | set[str],
) -> None:
    """Require listed platform skills to match the ontology-owned declaration."""

    declared_uris = {declaration.uri for declaration in declarations}
    listed_uris = set(listed_skill_uris)
    if listed_uris == declared_uris:
        return

    missing = sorted(declared_uris - listed_uris)
    undeclared = sorted(listed_uris - declared_uris)
    details = []
    if missing:
        details.append("missing declared skills: " + ", ".join(missing))
    if undeclared:
        details.append("undeclared listed skills: " + ", ".join(undeclared))
    raise CodeRepositorySkillAssemblyError(
        "The platform MCP skill resources do not match "
        "ontology.skill_resources (" + "; ".join(details) + ")."
    )


def _validate_platform_resource_payload(
    raw_row: dict[str, Any],
    *,
    uri: str,
) -> _ValidatedPlatformResourcePayload:
    label = f"platform resource {uri!r}"
    row = _mapping(raw_row, label=f"{label} list row")
    metadata = _mapping(row.get("_meta"), label=f"{label} list _meta")
    if metadata.get("owner_application") != "mcp_gateway":
        raise CodeRepositorySkillAssemblyError(f"{label} is not owned by mcp_gateway.")

    manifest_version = metadata.get("manifest_version")
    if not isinstance(manifest_version, int) or isinstance(manifest_version, bool):
        raise CodeRepositorySkillAssemblyError(f"{label} has an invalid manifest version.")
    if manifest_version not in SUPPORTED_PLATFORM_MANIFEST_VERSIONS:
        supported = ", ".join(
            str(version) for version in sorted(SUPPORTED_PLATFORM_MANIFEST_VERSIONS)
        )
        raise CodeRepositorySkillAssemblyError(
            f"{label} uses unsupported platform manifest version {manifest_version}; "
            f"this SDK supports {supported}."
        )
    manifest_sha256 = _sha256(
        metadata.get("manifest_sha256"),
        label=f"{label} manifest hash",
    )
    listed_name = _single_line_text(
        row.get("name"),
        label=f"{label} listed name",
    )
    resource_path = _safe_platform_resource_path(
        metadata.get("resource_path"),
        label=label,
    )
    declared_mime_type = _single_line_text(
        row.get("mimeType"),
        label=f"{label} MIME type",
    )
    declared_size = row.get("size")
    if not isinstance(declared_size, int) or isinstance(declared_size, bool) or declared_size < 0:
        raise CodeRepositorySkillAssemblyError(f"{label} has an invalid content size.")
    declared_sha256 = _sha256(
        metadata.get("content_sha256"),
        label=f"{label} content hash",
    )

    content_response = _mapping(
        row.get("_content"),
        label=f"{label} read response",
    )
    if content_response.get("uri") != uri:
        raise CodeRepositorySkillAssemblyError(f"{label} read response returned a different URI.")
    if content_response.get("mimeType") != declared_mime_type:
        raise CodeRepositorySkillAssemblyError(f"{label} read response MIME type mismatch.")
    content = content_response.get("text")
    if not isinstance(content, str):
        raise CodeRepositorySkillAssemblyError(f"{label} content must be UTF-8 text.")
    content_bytes = content.encode("utf-8")
    actual_sha256 = hashlib.sha256(content_bytes).hexdigest()
    content_metadata = _mapping(
        content_response.get("_meta"),
        label=f"{label} read _meta",
    )
    response_sha256 = _sha256(
        content_metadata.get("content_sha256"),
        label=f"{label} read hash",
    )
    if actual_sha256 != declared_sha256 or response_sha256 != declared_sha256:
        raise CodeRepositorySkillAssemblyError(f"{label} content hash mismatch.")
    if content_metadata.get("owner_application") != "mcp_gateway":
        raise CodeRepositorySkillAssemblyError(f"{label} read response is not owned by mcp_gateway.")
    if (
        content_metadata.get("manifest_version") != manifest_version
        or content_metadata.get("manifest_sha256") != manifest_sha256
    ):
        raise CodeRepositorySkillAssemblyError(f"{label} read response manifest identity mismatch.")
    if content_metadata.get("resource_path") != str(resource_path):
        raise CodeRepositorySkillAssemblyError(f"{label} read response resource path mismatch.")
    if content_metadata.get("resource_name") != listed_name:
        raise CodeRepositorySkillAssemblyError(f"{label} read response resource name mismatch.")
    if content_metadata.get("content_size") != declared_size or len(content_bytes) != declared_size:
        raise CodeRepositorySkillAssemblyError(f"{label} content byte-size mismatch.")

    return _ValidatedPlatformResourcePayload(
        listed_name=listed_name,
        uri=uri,
        resource_path=resource_path,
        content=content,
        content_sha256=declared_sha256,
        content_mime_type=declared_mime_type,
        content_size=declared_size,
        manifest_version=manifest_version,
        manifest_sha256=manifest_sha256,
    )


def install_dual_source_code_repository_skills(
    *,
    code_repository_dir: Path,
    sdk_library_name: str,
    sdk_skills_path: Path,
    sdk_version: str,
    platform_catalog: PlatformCodeRepositorySkillCatalog,
    namespace: str = "mainsequence",
    command: str = "mainsequence code-repository update-agent-skills",
    protected_code_repository_roots: Sequence[Path] = (),
) -> DualSourceCodeRepositorySkillInstallResult:
    """Install SDK-owned and platform-owned skills as one validated CodeRepository tree."""

    sdk_plan = copy_scaffold_skills(
        code_repository_dir=code_repository_dir,
        library_name=sdk_library_name,
        namespace=namespace,
        skills_path=sdk_skills_path,
        pinned_version=sdk_version,
        command=command,
        dry_run=True,
        protected_code_repository_roots=protected_code_repository_roots,
    )
    _validate_source_ownership(
        sdk_skills=sdk_plan.copied,
        platform_skills=platform_catalog.skills,
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

    installed: list[InstalledCodeRepositorySkill] = []
    try:
        for item in sdk_plan.copied:
            staged_destination = staging_root / item.name
            shutil.copytree(item.source, staged_destination)
            installed.append(
                InstalledCodeRepositorySkill(
                    name=item.name,
                    owner="sdk",
                    source=str(item.source),
                    destination=destination_root / item.name,
                )
            )

        for skill in platform_catalog.skills:
            staged_destination = staging_root.joinpath(*skill.relative_path.parts)
            staged_destination.parent.mkdir(parents=True, exist_ok=True)
            staged_destination.write_text(skill.content, encoding="utf-8")
            installed.append(
                InstalledCodeRepositorySkill(
                    name=skill.name,
                    owner="platform",
                    source=skill.uri,
                    destination=destination_root.joinpath(*skill.relative_path.parts),
                    content_sha256=skill.content_sha256,
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

    return DualSourceCodeRepositorySkillInstallResult(
        code_repository_dir=sdk_plan.code_repository_dir,
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
    platform_catalog: PlatformCodeRepositorySkillCatalog,
    command: str,
) -> str:
    """Render the schema-2 dual-source provenance record."""

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
        f"platform_resource_count={len(platform_catalog.resources)}",
        f"platform_skill_count={len(platform_catalog.skills)}",
    ]
    for resource in platform_catalog.resources:
        prefix = f"platform_resource.{resource.name}"
        lines.extend(
            [
                f"{prefix}.uri={_sentinel_value(resource.uri)}",
                f"{prefix}.path={_sentinel_value(str(resource.resource_path))}",
                f"{prefix}.content_sha256={resource.content_sha256}",
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
    platform_skills: Sequence[PlatformCodeRepositorySkill],
) -> None:
    sdk_roots = {item.name for item in sdk_skills}
    collisions = sorted(
        {
            skill.relative_path.parts[0]
            for skill in platform_skills
            if skill.relative_path.parts[0] in sdk_roots
        }
    )
    if collisions:
        raise CodeRepositorySkillAssemblyError(
            "SDK/platform skill path ownership collision: "
            + ", ".join(collisions)
            + ". Refactor the SDK-owned skill path before updating the CodeRepository."
        )


def _replace_managed_tree(*, staging_root: Path, destination_root: Path) -> None:
    backup_root = destination_root.parent / (f".{destination_root.name}.backup-{uuid.uuid4().hex}")
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
            raise CodeRepositorySkillAssemblyError(
                "CodeRepository skill installation failed and rollback also failed: "
                f"{rollback_exc}. The previous managed tree remains at "
                f"{backup_root} for manual recovery."
            ) from install_exc
        raise
    if backup_root.exists():
        shutil.rmtree(backup_root, ignore_errors=True)


def _safe_platform_resource_path(
    value: Any,
    *,
    label: str,
) -> PurePosixPath:
    raw_path = _single_line_text(
        value,
        label=f"{label} path",
    )
    if "\\" in raw_path:
        raise CodeRepositorySkillAssemblyError(f"{label} has an unsafe resource path.")
    path = PurePosixPath(raw_path)
    if path.is_absolute() or ".." in path.parts or raw_path != path.as_posix():
        raise CodeRepositorySkillAssemblyError(f"{label} has an unsafe resource path.")
    return path


def _platform_skill_slug(uri: str, *, label: str) -> str:
    if not uri.startswith(PLATFORM_SKILL_URI_PREFIX):
        raise CodeRepositorySkillAssemblyError(f"{label} URI must use {PLATFORM_SKILL_URI_PREFIX}.")
    slug = uri.removeprefix(PLATFORM_SKILL_URI_PREFIX)
    if not _PLATFORM_SKILL_SLUG_RE.fullmatch(slug):
        raise CodeRepositorySkillAssemblyError(
            f"{label} URI must end with a safe lowercase kebab-case skill name."
        )
    return slug


def _validate_platform_skill_path(
    path: PurePosixPath,
    *,
    label: str,
) -> None:
    if len(path.parts) < 3 or path.parts[0] != "skills":
        raise CodeRepositorySkillAssemblyError(
            f"{label} path must be rooted under skills/ and contain at least "
            "one skill directory."
        )
    if path.parts[-1] not in _PLATFORM_SKILL_FILENAMES:
        raise CodeRepositorySkillAssemblyError(
            f"{label} path must end with SKILL.md or SKILL.markdown."
        )
    if any(not _PLATFORM_SKILL_NAME_RE.fullmatch(part) for part in path.parts[1:-1]):
        raise CodeRepositorySkillAssemblyError(
            f"{label} path directories must use safe lowercase snake case."
        )


def _validate_skill_front_matter(
    content: str,
    *,
    expected_name: str,
    label: str,
) -> None:
    lines = content.splitlines()
    if not lines or lines[0].strip() != "---":
        raise CodeRepositorySkillAssemblyError(f"{label} must start with YAML front matter.")
    try:
        closing_index = next(
            index for index, line in enumerate(lines[1:], start=1) if line.strip() == "---"
        )
    except StopIteration as exc:
        raise CodeRepositorySkillAssemblyError(f"{label} front matter is not closed.") from exc

    try:
        front_matter = yaml.safe_load("\n".join(lines[1:closing_index]))
    except yaml.YAMLError as exc:
        raise CodeRepositorySkillAssemblyError(f"{label} front matter must contain valid YAML.") from exc
    front_matter = _mapping(front_matter, label=f"{label} front matter")
    name = _single_line_text(
        front_matter.get("name"),
        label=f"{label} front matter name",
    )
    if name != expected_name:
        raise CodeRepositorySkillAssemblyError(f"{label} front matter name must be {expected_name!r}.")
    _single_line_text(
        front_matter.get("description"),
        label=f"{label} front matter description",
    )


def _mapping(value: Any, *, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise CodeRepositorySkillAssemblyError(f"{label} must be an object mapping.")
    return value


def _sha256(value: Any, *, label: str) -> str:
    normalized = _single_line_text(value, label=label).lower()
    if not _SHA256_RE.fullmatch(normalized):
        raise CodeRepositorySkillAssemblyError(f"{label} must be a lowercase SHA-256.")
    return normalized


def _single_line_text(value: Any, *, label: str) -> str:
    if not isinstance(value, str):
        raise CodeRepositorySkillAssemblyError(f"{label} must be text.")
    normalized = value.strip()
    if not normalized or "\n" in normalized or "\r" in normalized:
        raise CodeRepositorySkillAssemblyError(f"{label} must be non-empty single-line text.")
    return normalized


def _sentinel_value(value: str) -> str:
    return _single_line_text(value, label="sentinel value")


def _utc_timestamp() -> str:
    import datetime as _datetime

    now = _datetime.datetime.now(tz=_datetime.UTC)
    return now.isoformat(timespec="seconds").replace("+00:00", "Z")
