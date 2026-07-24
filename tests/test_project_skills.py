from __future__ import annotations

import hashlib
import os
from pathlib import Path

import pytest

from mainsequence import project_skills
from mainsequence.cli import api as cli_api
from mainsequence.cli.api import ApiError
from mainsequence.project_skills import (
    PlatformProjectSkillCatalog,
    ProjectSkillAssemblyError,
    install_dual_source_project_skills,
    parse_platform_project_skill_catalog,
)


def _platform_row(
    name: str,
    content: str,
    *,
    capability_count: int = 2,
    manifest_sha256: str = "a" * 64,
) -> dict:
    content_bytes = content.encode("utf-8")
    content_sha256 = hashlib.sha256(content_bytes).hexdigest()
    return {
        "uid": f"{name}-uid",
        "name": name,
        "kind": "skill",
        "source_type": "registry",
        "source_ref": f"mainsequence:platform-capability:{name}",
        "capability_path": f"skills/{name}/SKILL.md",
        "is_editable": False,
        "metadata": {
            "platform_catalog": {
                "sync_source": "platform_manifest",
                "manifest_version": 1,
                "manifest_sha256": manifest_sha256,
                "capability_count": capability_count,
                "source_path": f"skills/{name}/SKILL.md",
                "ontology_uri": "mainsequence://platform/ontology",
                "ontology_sha256": "b" * 64,
            }
        },
        "content_sha256": content_sha256,
        "content_mime_type": "text/markdown",
        "content_size": len(content_bytes),
        "_content": {
            "content": content,
            "content_sha256": content_sha256,
            "content_mime_type": "text/markdown",
            "content_size": len(content_bytes),
        },
    }


def _platform_catalog() -> PlatformProjectSkillCatalog:
    return parse_platform_project_skill_catalog(
        [
            _platform_row(
                "a2a_communication",
                "---\nname: a2a-communication\ndescription: A2A\n---\n",
            ),
            _platform_row(
                "project_builder",
                "---\nname: project-builder\ndescription: Builder\n---\n",
            ),
        ],
        source_url="https://platform.example.test/",
    )


def test_parse_platform_catalog_validates_one_complete_revision():
    catalog = _platform_catalog()

    assert catalog.manifest_version == 1
    assert catalog.manifest_sha256 == "a" * 64
    assert catalog.ontology_sha256 == "b" * 64
    assert [item.name for item in catalog.capabilities] == [
        "a2a_communication",
        "project_builder",
    ]
    assert catalog.capabilities[0].relative_path.as_posix() == (
        "a2a_communication/SKILL.md"
    )


def test_parse_platform_catalog_rejects_incomplete_or_drifted_content():
    with pytest.raises(ProjectSkillAssemblyError, match="incomplete"):
        parse_platform_project_skill_catalog(
            [
                _platform_row(
                    "project_builder",
                    "builder",
                    capability_count=2,
                )
            ],
            source_url="https://platform.example.test",
        )

    row = _platform_row("project_builder", "builder", capability_count=1)
    row["_content"]["content"] = "drift"
    with pytest.raises(ProjectSkillAssemblyError, match="hash mismatch"):
        parse_platform_project_skill_catalog(
            [row],
            source_url="https://platform.example.test",
        )


def test_dual_source_install_replaces_only_managed_tree_and_records_both_sources(
    tmp_path,
):
    sdk_skills = tmp_path / "installed-sdk" / "agent_scaffold" / "skills"
    sdk_skill = sdk_skills / "sdk_project_execution"
    sdk_skill.mkdir(parents=True)
    (sdk_skill / "SKILL.md").write_text("sdk execution", encoding="utf-8")

    project_dir = tmp_path / "project"
    managed_root = project_dir / ".agents" / "skills" / "mainsequence"
    stale_skill = managed_root / "stale"
    stale_skill.mkdir(parents=True)
    (stale_skill / "SKILL.md").write_text("stale", encoding="utf-8")
    project_owned = project_dir / ".agents" / "skills" / "project_owned"
    project_owned.mkdir(parents=True)
    (project_owned / "SKILL.md").write_text("keep", encoding="utf-8")

    result = install_dual_source_project_skills(
        project_dir=project_dir,
        sdk_library_name="mainsequence",
        sdk_skills_path=sdk_skills,
        sdk_version="4.4.34",
        platform_catalog=_platform_catalog(),
    )

    assert not stale_skill.exists()
    assert (managed_root / "sdk_project_execution" / "SKILL.md").read_text(
        encoding="utf-8"
    ) == "sdk execution"
    assert (managed_root / "project_builder" / "SKILL.md").is_file()
    assert (managed_root / "a2a_communication" / "SKILL.md").is_file()
    assert (project_owned / "SKILL.md").read_text(encoding="utf-8") == "keep"
    assert [item.owner for item in result.installed] == [
        "sdk",
        "platform",
        "platform",
    ]

    sentinel = result.sentinel_path.read_text(encoding="utf-8")
    assert "schema=2" in sentinel
    assert "pinned_version=4.4.34" in sentinel
    assert "copied_at_utc=" in sentinel
    assert "installed_at_utc=" in sentinel
    assert "sdk_version=4.4.34" in sentinel
    assert "platform_retrieved_at_utc=" in sentinel
    assert "platform_manifest_version=1" in sentinel
    assert f"platform_manifest_sha256={'a' * 64}" in sentinel
    assert "platform_capability_count=2" in sentinel
    assert (
        "platform_capability.project_builder.source_ref="
        "mainsequence:platform-capability:project_builder"
    ) in sentinel


def test_dual_source_install_rejects_sdk_platform_path_collision_without_writes(
    tmp_path,
):
    sdk_skills = tmp_path / "installed-sdk" / "agent_scaffold" / "skills"
    colliding_skill = sdk_skills / "project_builder"
    colliding_skill.mkdir(parents=True)
    (colliding_skill / "SKILL.md").write_text("sdk collision", encoding="utf-8")
    project_dir = tmp_path / "project"
    existing = (
        project_dir
        / ".agents"
        / "skills"
        / "mainsequence"
        / "existing"
        / "SKILL.md"
    )
    existing.parent.mkdir(parents=True)
    existing.write_text("keep", encoding="utf-8")

    with pytest.raises(ProjectSkillAssemblyError, match="collision"):
        install_dual_source_project_skills(
            project_dir=project_dir,
            sdk_library_name="mainsequence",
            sdk_skills_path=sdk_skills,
            sdk_version="4.4.34",
            platform_catalog=_platform_catalog(),
        )

    assert existing.read_text(encoding="utf-8") == "keep"


def test_dual_source_install_restores_previous_tree_when_final_swap_fails(
    monkeypatch,
    tmp_path,
):
    sdk_skills = tmp_path / "installed-sdk" / "agent_scaffold" / "skills"
    sdk_skill = sdk_skills / "sdk_project_execution"
    sdk_skill.mkdir(parents=True)
    (sdk_skill / "SKILL.md").write_text("sdk execution", encoding="utf-8")

    project_dir = tmp_path / "project"
    managed_root = project_dir / ".agents" / "skills" / "mainsequence"
    previous = managed_root / "previous" / "SKILL.md"
    previous.parent.mkdir(parents=True)
    previous.write_text("previous valid tree", encoding="utf-8")
    previous_sentinel = managed_root / "PINNED_FROM.txt"
    previous_sentinel.write_text(
        "schema=2\nsdk_version=previous\n",
        encoding="utf-8",
    )

    real_replace = os.replace
    replace_calls = 0

    def _fail_staging_swap(source, destination):
        nonlocal replace_calls
        replace_calls += 1
        if replace_calls == 2:
            raise OSError("simulated final swap failure")
        return real_replace(source, destination)

    monkeypatch.setattr(project_skills.os, "replace", _fail_staging_swap)

    with pytest.raises(OSError, match="simulated final swap failure"):
        install_dual_source_project_skills(
            project_dir=project_dir,
            sdk_library_name="mainsequence",
            sdk_skills_path=sdk_skills,
            sdk_version="4.4.34",
            platform_catalog=_platform_catalog(),
        )

    assert previous.read_text(encoding="utf-8") == "previous valid tree"
    assert previous_sentinel.read_text(encoding="utf-8") == (
        "schema=2\nsdk_version=previous\n"
    )
    assert not list(managed_root.parent.glob(".mainsequence.backup-*"))
    assert not list(managed_root.parent.glob(".mainsequence.staging-*"))


def test_cli_fetch_uses_existing_authenticated_capability_routes(monkeypatch):
    rows = [
        _platform_row(
            "a2a_communication",
            "platform a2a",
        ),
        _platform_row(
            "project_builder",
            "platform builder",
        ),
    ]
    list_rows = []
    content_by_uid = {}
    for row in rows:
        content_by_uid[row["uid"]] = row["_content"]
        list_rows.append({key: value for key, value in row.items() if key != "_content"})

    class _Response:
        def __init__(self, payload, *, status_code=200):
            self._payload = payload
            self.status_code = status_code
            self.ok = 200 <= status_code < 300
            self.headers = {"content-type": "application/json"}

        def json(self):
            return self._payload

    calls = []

    def _authed(method, path, body=None):
        calls.append((method, path, body))
        if path.startswith("/orm/api/agents/v1/capabilities/?"):
            return _Response({"count": 2, "results": list_rows})
        capability_uid = path.split("/")[-3]
        return _Response(content_by_uid[capability_uid])

    monkeypatch.setattr(cli_api, "authed", _authed)
    monkeypatch.setattr(
        cli_api,
        "backend_url",
        lambda: "https://platform.example.test/",
    )

    catalog = cli_api.fetch_platform_project_skill_catalog()

    assert catalog.source_url == "https://platform.example.test"
    assert [item.name for item in catalog.capabilities] == [
        "a2a_communication",
        "project_builder",
    ]
    assert calls[0][0] == "GET"
    assert "source_type=registry" in calls[0][1]
    assert calls[1][1].endswith("/a2a_communication-uid/content/")
    assert calls[2][1].endswith("/project_builder-uid/content/")


def test_cli_fetch_rejects_catalog_or_content_transport_failures(monkeypatch):
    class _Response:
        def __init__(self, payload, *, status_code):
            self._payload = payload
            self.status_code = status_code
            self.ok = 200 <= status_code < 300
            self.headers = {"content-type": "application/json"}

        def json(self):
            return self._payload

    monkeypatch.setattr(
        cli_api,
        "authed",
        lambda method, path: _Response({}, status_code=401),
    )
    with pytest.raises(ApiError, match="catalog fetch failed \\(401\\)"):
        cli_api.fetch_platform_project_skill_catalog()

    row = _platform_row(
        "project_builder",
        "builder",
        capability_count=1,
    )
    list_row = {key: value for key, value in row.items() if key != "_content"}

    def _content_failure(method, path):
        if path.startswith("/orm/api/agents/v1/capabilities/?"):
            return _Response(
                {"count": 1, "results": [list_row]},
                status_code=200,
            )
        return _Response({}, status_code=503)

    monkeypatch.setattr(cli_api, "authed", _content_failure)
    with pytest.raises(ApiError, match="content fetch failed.*\\(503\\)"):
        cli_api.fetch_platform_project_skill_catalog()


def test_sdk_source_tree_does_not_vendor_platform_owned_content():
    sdk_root = Path(__file__).resolve().parents[1]

    forbidden_paths = [
        sdk_root / "agent_scaffold" / "platform_capabilities",
        sdk_root / "agent_scaffold" / "SDK_CAPABILITY_ASSEMBLY.json",
        sdk_root / "agent_scaffold" / "skills" / "project_builder",
        sdk_root / "agent_scaffold" / "skills" / "a2a_communication",
        sdk_root / "mainsequence" / "platform_capability_snapshot.py",
    ]

    assert not [path for path in forbidden_paths if path.exists()]
