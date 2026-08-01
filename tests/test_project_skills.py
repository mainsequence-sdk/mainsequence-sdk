from __future__ import annotations

import hashlib
import os
import re
from pathlib import Path

import pytest

from mainsequence import project_skills
from mainsequence.cli import api as cli_api
from mainsequence.cli.api import ApiError
from mainsequence.project_skills import (
    PLATFORM_A2A_SKILL_URI,
    PLATFORM_ONTOLOGY_URI,
    PLATFORM_PROJECT_DESIGN_SKILL_URI,
    PLATFORM_PROJECT_TO_AGENT_SKILL_URI,
    PlatformProjectSkillCatalog,
    ProjectSkillAssemblyError,
    install_dual_source_project_skills,
    parse_platform_project_skill_catalog,
)


def _platform_resource_row(
    *,
    uri: str,
    name: str,
    path: str,
    mime_type: str,
    content: str,
    manifest_sha256: str = "a" * 64,
) -> dict:
    content_bytes = content.encode("utf-8")
    content_sha256 = hashlib.sha256(content_bytes).hexdigest()
    return {
        "uri": uri,
        "name": name,
        "mimeType": mime_type,
        "size": len(content_bytes),
        "_meta": {
            "owner_application": "mcp_gateway",
            "manifest_version": 2,
            "manifest_sha256": manifest_sha256,
            "resource_path": path,
            "content_sha256": content_sha256,
        },
        "_content": {
            "uri": uri,
            "mimeType": mime_type,
            "text": content,
            "_meta": {
                "resource_name": name,
                "resource_path": path,
                "owner_application": "mcp_gateway",
                "manifest_version": 2,
                "manifest_sha256": manifest_sha256,
                "content_sha256": content_sha256,
                "content_size": len(content_bytes),
            },
        },
    }


def _platform_rows(*, manifest_sha256: str = "a" * 64) -> list[dict]:
    return [
        _platform_resource_row(
            uri=PLATFORM_ONTOLOGY_URI,
            name="Main Sequence platform ontology",
            path="ontology/platform.json",
            mime_type="application/json",
            content='{"title":"Main Sequence","description":"Platform ontology"}\n',
            manifest_sha256=manifest_sha256,
        ),
        _platform_resource_row(
            uri=PLATFORM_A2A_SKILL_URI,
            name="a2a_communication",
            path="skills/a2a_communication/SKILL.md",
            mime_type="text/markdown",
            content=("---\nname: a2a-communication\ndescription: A2A\n---\n"),
            manifest_sha256=manifest_sha256,
        ),
        _platform_resource_row(
            uri=PLATFORM_PROJECT_DESIGN_SKILL_URI,
            name="project_design",
            path="skills/project_design/SKILL.md",
            mime_type="text/markdown",
            content=("---\nname: project-design\ndescription: Builder\n---\n"),
            manifest_sha256=manifest_sha256,
        ),
        _platform_resource_row(
            uri=PLATFORM_PROJECT_TO_AGENT_SKILL_URI,
            name="project_to_agent",
            path="skills/project_to_agent/SKILL.md",
            mime_type="text/markdown",
            content=("---\nname: project-to-agent\ndescription: Project agent preparation\n---\n"),
            manifest_sha256=manifest_sha256,
        ),
    ]


def _platform_catalog() -> PlatformProjectSkillCatalog:
    return parse_platform_project_skill_catalog(
        _platform_rows(),
        source_url="https://platform.example.test/mcp",
    )


def test_parse_platform_catalog_validates_one_complete_revision():
    catalog = _platform_catalog()

    assert catalog.manifest_version == 2
    assert catalog.manifest_sha256 == "a" * 64
    assert catalog.ontology_uri == PLATFORM_ONTOLOGY_URI
    assert [item.name for item in catalog.skills] == [
        "a2a_communication",
        "project_design",
        "project_to_agent",
    ]
    assert catalog.skills[0].relative_path.as_posix() == ("a2a_communication/SKILL.md")


def test_parse_platform_catalog_rejects_incomplete_or_drifted_content():
    with pytest.raises(ProjectSkillAssemblyError, match="approved fixed resource"):
        parse_platform_project_skill_catalog(
            _platform_rows()[:-1],
            source_url="https://platform.example.test/mcp",
        )

    rows = _platform_rows()
    rows[-1]["_content"]["text"] = "drift"
    with pytest.raises(ProjectSkillAssemblyError, match="hash mismatch"):
        parse_platform_project_skill_catalog(
            rows,
            source_url="https://platform.example.test/mcp",
        )


def test_dual_source_install_replaces_only_managed_tree_and_records_both_sources(
    tmp_path,
):
    sdk_skills = tmp_path / "installed-sdk" / "agent_scaffold" / "skills"
    sdk_skill = sdk_skills / "sdk_project_execution"
    sdk_skill.mkdir(parents=True)
    (sdk_skill / "SKILL.md").write_text("sdk execution", encoding="utf-8")
    maintenance_skill = sdk_skills / "maintenance" / "project-maintenance"
    maintenance_skill.mkdir(parents=True)
    (maintenance_skill / "SKILL.md").write_text(
        "sdk project maintenance",
        encoding="utf-8",
    )

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
    assert (managed_root / "maintenance" / "project-maintenance" / "SKILL.md").read_text(
        encoding="utf-8"
    ) == "sdk project maintenance"
    assert (managed_root / "project_design" / "SKILL.md").is_file()
    assert (managed_root / "a2a_communication" / "SKILL.md").is_file()
    assert (managed_root / "project_to_agent" / "SKILL.md").is_file()
    assert (project_owned / "SKILL.md").read_text(encoding="utf-8") == "keep"
    assert [(item.name, item.owner) for item in result.installed] == [
        ("maintenance", "sdk"),
        ("sdk_project_execution", "sdk"),
        ("a2a_communication", "platform"),
        ("project_design", "platform"),
        ("project_to_agent", "platform"),
    ]

    sentinel = result.sentinel_path.read_text(encoding="utf-8")
    assert "schema=2" in sentinel
    assert "pinned_version=4.4.34" in sentinel
    assert "copied_at_utc=" in sentinel
    assert "installed_at_utc=" in sentinel
    assert "sdk_version=4.4.34" in sentinel
    assert "platform_retrieved_at_utc=" in sentinel
    assert "platform_manifest_version=2" in sentinel
    assert f"platform_manifest_sha256={'a' * 64}" in sentinel
    assert "platform_resource_count=4" in sentinel
    assert "platform_skill_count=3" in sentinel
    assert (
        "platform_resource.project_design.uri=mainsequence://platform/skills/project-design"
    ) in sentinel


def test_dual_source_install_rejects_sdk_platform_path_collision_without_writes(
    tmp_path,
):
    sdk_skills = tmp_path / "installed-sdk" / "agent_scaffold" / "skills"
    colliding_skill = sdk_skills / "project_design"
    colliding_skill.mkdir(parents=True)
    (colliding_skill / "SKILL.md").write_text("sdk collision", encoding="utf-8")
    project_dir = tmp_path / "project"
    existing = project_dir / ".agents" / "skills" / "mainsequence" / "existing" / "SKILL.md"
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
    assert previous_sentinel.read_text(encoding="utf-8") == ("schema=2\nsdk_version=previous\n")
    assert not list(managed_root.parent.glob(".mainsequence.backup-*"))
    assert not list(managed_root.parent.glob(".mainsequence.staging-*"))


def test_cli_fetch_uses_authenticated_mcp_resources(monkeypatch):
    rows = _platform_rows()
    listed_resources = [
        {key: value for key, value in row.items() if key != "_content"} for row in rows
    ]
    content_by_uri = {row["uri"]: row["_content"] for row in rows}
    calls = []

    def _mcp_json_rpc(
        method,
        params,
        *,
        request_id,
        protocol_version=None,
    ):
        calls.append((method, params, request_id, protocol_version))
        if method == "initialize":
            return {
                "protocolVersion": cli_api.MCP_PROTOCOL_VERSION,
                "capabilities": {"resources": {}},
            }
        if method == "resources/list":
            return {"resources": listed_resources}
        return {"contents": [content_by_uri[params["uri"]]]}

    monkeypatch.setattr(cli_api, "_mcp_json_rpc", _mcp_json_rpc)
    monkeypatch.setattr(
        cli_api,
        "backend_url",
        lambda: "https://platform.example.test",
    )

    catalog = cli_api.fetch_platform_project_skill_catalog()

    assert catalog.source_url == "https://platform.example.test/mcp"
    assert [item.name for item in catalog.skills] == [
        "a2a_communication",
        "project_design",
        "project_to_agent",
    ]
    assert [call[0] for call in calls] == [
        "initialize",
        "resources/list",
        "resources/read",
        "resources/read",
        "resources/read",
        "resources/read",
    ]
    assert calls[0][3] is None
    assert all(call[3] == cli_api.MCP_PROTOCOL_VERSION for call in calls[1:])


def test_mcp_json_rpc_uses_existing_jwt_and_refreshes_once(monkeypatch):
    class _Response:
        def __init__(self, payload, *, status_code=200):
            self._payload = payload
            self.status_code = status_code
            self.ok = 200 <= status_code < 300
            self.headers = {"content-type": "application/json"}

        def json(self):
            return self._payload

    responses = [
        _Response({}, status_code=401),
        _Response(
            {
                "jsonrpc": "2.0",
                "id": 7,
                "result": {"resources": []},
            }
        ),
    ]
    calls = []

    def _request(method, url, *, headers, data):
        calls.append((method, url, headers, data))
        return responses.pop(0)

    monkeypatch.setattr(cli_api, "_access_token", lambda: "existing-jwt")
    monkeypatch.setattr(cli_api, "refresh_access", lambda: "refreshed-jwt")
    monkeypatch.setattr(cli_api.S, "request", _request)
    monkeypatch.setattr(
        cli_api,
        "backend_url",
        lambda: "https://platform.example.test",
    )

    result = cli_api._mcp_json_rpc(
        "resources/list",
        {},
        request_id=7,
        protocol_version=cli_api.MCP_PROTOCOL_VERSION,
    )

    assert result == {"resources": []}
    assert calls[0][0:2] == (
        "POST",
        "https://platform.example.test/mcp",
    )
    assert calls[0][2]["Authorization"] == "Bearer existing-jwt"
    assert calls[1][2]["Authorization"] == "Bearer refreshed-jwt"
    assert calls[1][2]["MCP-Protocol-Version"] == cli_api.MCP_PROTOCOL_VERSION


def test_cli_fetch_rejects_mcp_or_resource_contract_failures(monkeypatch):
    monkeypatch.setattr(
        cli_api,
        "_mcp_json_rpc",
        lambda *args, **kwargs: {
            "protocolVersion": "1900-01-01",
            "capabilities": {"resources": {}},
        },
    )
    with pytest.raises(ApiError, match="unsupported protocol version"):
        cli_api.fetch_platform_project_skill_catalog()

    rows = _platform_rows()
    listed_resources = [
        {key: value for key, value in row.items() if key != "_content"} for row in rows
    ]
    rows[-1]["_content"]["text"] = "drift"
    content_by_uri = {row["uri"]: row["_content"] for row in rows}

    def _drifted_mcp(method, params, **_kwargs):
        if method == "initialize":
            return {
                "protocolVersion": cli_api.MCP_PROTOCOL_VERSION,
                "capabilities": {"resources": {}},
            }
        if method == "resources/list":
            return {"resources": listed_resources}
        return {"contents": [content_by_uri[params["uri"]]]}

    monkeypatch.setattr(cli_api, "_mcp_json_rpc", _drifted_mcp)
    with pytest.raises(ApiError, match="content hash mismatch"):
        cli_api.fetch_platform_project_skill_catalog()


def test_sdk_source_tree_does_not_vendor_platform_owned_content():
    sdk_root = Path(__file__).resolve().parents[1]

    forbidden_paths = [
        sdk_root / "agent_scaffold" / "platform_capabilities",
        sdk_root / "agent_scaffold" / "SDK_CAPABILITY_ASSEMBLY.json",
        sdk_root / "agent_scaffold" / "skills" / "project_design",
        sdk_root / "agent_scaffold" / "skills" / "a2a_communication",
        sdk_root / "agent_scaffold" / "skills" / "project_to_agent",
        sdk_root / "mainsequence" / "platform_capability_snapshot.py",
    ]

    assert not [path for path in forbidden_paths if path.exists()]


def test_sdk_source_tree_does_not_vendor_extension_library_routing_skills():
    sdk_root = Path(__file__).resolve().parents[1]

    assert not (sdk_root / "agent_scaffold" / "skills" / "ms-markets").exists()


def test_project_maintenance_is_sdk_owned_and_uses_canonical_cli_workflows():
    sdk_root = Path(__file__).resolve().parents[1]
    skill_path = (
        sdk_root / "agent_scaffold" / "skills" / "maintenance" / "project-maintenance" / "SKILL.md"
    )
    content = skill_path.read_text(encoding="utf-8")

    assert "name: project-maintenance" in content
    assert "mainsequence project build_local_venv --path ." in content
    assert "mainsequence project refresh_token --path ." in content
    assert "mainsequence project update-sdk --path ." in content
    assert "mainsequence project sync --path . -m" in content
    assert content.index("mainsequence project update_agent_skills --path .") < (
        content.index("mainsequence project update AGENTS.md --path .")
    )
    assert "Do not call `sync-after-commit`" in content
    assert "mainsequence://" not in content


def test_docs_do_not_reference_unsupported_agent_state_files():
    sdk_root = Path(__file__).resolve().parents[1]
    ignored_parts = {
        ".git",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
        ".tox",
        ".venv",
        "build",
        "dist",
        "node_modules",
        "site",
        "venv",
    }
    forbidden_patterns = (
        re.compile(
            r"\.agents(?:/|\.)"
            r"(?:tasks?|status|record|journal|local[_-]?journal)(?:\.md)?\b",
            re.IGNORECASE,
        ),
        re.compile(r"\bproject[- ]state (?:files?|records?)\b", re.IGNORECASE),
    )
    violations: list[str] = []

    for markdown_path in sdk_root.rglob("*.md"):
        if ignored_parts.intersection(markdown_path.relative_to(sdk_root).parts):
            continue
        content = markdown_path.read_text(encoding="utf-8")
        for pattern in forbidden_patterns:
            if pattern.search(content):
                violations.append(str(markdown_path.relative_to(sdk_root)))
                break

    assert violations == []
