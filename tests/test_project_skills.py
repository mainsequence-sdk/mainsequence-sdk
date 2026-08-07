from __future__ import annotations

import hashlib
import json
import os
import re
from pathlib import Path

import pytest

from mainsequence import project_skills
from mainsequence.cli import api as cli_api
from mainsequence.cli.api import ApiError
from mainsequence.project_skills import (
    PLATFORM_ONTOLOGY_URI,
    PLATFORM_SKILL_URI_PREFIX,
    PlatformProjectSkillCatalog,
    ProjectSkillAssemblyError,
    install_dual_source_project_skills,
    parse_platform_project_skill_catalog,
)

_DEFAULT_TEST_SKILLS = ("alpha_skill", "beta_skill", "gamma_skill")
_CURRENT_BACKEND_SKILL_PATHS = {
    "a2a_communication": "skills/agents/a2a_communication/SKILL.md",
    "command_center": "skills/command_center/command_center/SKILL.md",
    "command_center_connections": "skills/command_center/connections/SKILL.md",
    "project_design": "skills/platform/project_design/SKILL.md",
    "project_local_setup": "skills/pod_manager/project_local_setup/SKILL.md",
    "project_to_agent": "skills/agents/project_to_agent/SKILL.md",
    "static_site": "skills/pod_manager/static_site/SKILL.md",
}


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


def _platform_rows(
    *,
    manifest_sha256: str = "a" * 64,
    skill_names: tuple[str, ...] = _DEFAULT_TEST_SKILLS,
    skill_paths: dict[str, str] | None = None,
) -> list[dict]:
    skill_paths = skill_paths or {}
    skill_resources = [
        {
            "name": name,
            "uri": f"{PLATFORM_SKILL_URI_PREFIX}{name.replace('_', '-')}",
        }
        for name in skill_names
    ]
    ontology_content = (
        json.dumps(
            {
                "title": "Main Sequence",
                "description": "Platform ontology",
                "skill_resources": skill_resources,
            },
            separators=(",", ":"),
        )
        + "\n"
    )
    rows = [
        _platform_resource_row(
            uri=PLATFORM_ONTOLOGY_URI,
            name="Main Sequence platform ontology",
            path="ontology/platform.json",
            mime_type="application/json",
            content=ontology_content,
            manifest_sha256=manifest_sha256,
        )
    ]
    rows.extend(
        _platform_resource_row(
            uri=resource["uri"],
            name=resource["name"],
            path=skill_paths.get(
                resource["name"],
                f"skills/{resource['name']}/SKILL.md",
            ),
            mime_type="text/markdown",
            content=(
                "---\n"
                f"name: {resource['name'].replace('_', '-')}\n"
                f"description: {resource['name']} guidance\n"
                "---\n"
            ),
            manifest_sha256=manifest_sha256,
        )
        for resource in skill_resources
    )
    return rows


def _replace_resource_content(row: dict, content: str) -> None:
    content_bytes = content.encode("utf-8")
    content_sha256 = hashlib.sha256(content_bytes).hexdigest()
    row["size"] = len(content_bytes)
    row["_meta"]["content_sha256"] = content_sha256
    row["_content"]["text"] = content
    row["_content"]["_meta"]["content_sha256"] = content_sha256
    row["_content"]["_meta"]["content_size"] = len(content_bytes)


def _platform_catalog() -> PlatformProjectSkillCatalog:
    return parse_platform_project_skill_catalog(
        _platform_rows(),
        source_url="https://platform.example.test/mcp",
    )


def _current_backend_catalog() -> PlatformProjectSkillCatalog:
    return parse_platform_project_skill_catalog(
        _platform_rows(
            skill_names=tuple(_CURRENT_BACKEND_SKILL_PATHS),
            skill_paths=_CURRENT_BACKEND_SKILL_PATHS,
        ),
        source_url="https://platform.example.test/mcp",
    )


def test_parse_platform_catalog_validates_one_complete_revision():
    catalog = _platform_catalog()

    assert catalog.manifest_version == 2
    assert catalog.manifest_sha256 == "a" * 64
    assert catalog.ontology_uri == PLATFORM_ONTOLOGY_URI
    assert [item.name for item in catalog.skills] == list(_DEFAULT_TEST_SKILLS)
    assert catalog.skills[0].relative_path.as_posix() == "alpha_skill/SKILL.md"


def test_parse_platform_catalog_rejects_incomplete_or_drifted_content():
    with pytest.raises(ProjectSkillAssemblyError, match="missing declared skills"):
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


def test_parse_platform_catalog_accepts_additive_skills_and_ignores_order():
    rows = _platform_rows(skill_names=("gamma_skill", "new_capability", "alpha_skill"))

    catalog = parse_platform_project_skill_catalog(
        list(reversed(rows)),
        source_url="https://platform.example.test/mcp",
    )

    assert [skill.name for skill in catalog.skills] == [
        "alpha_skill",
        "gamma_skill",
        "new_capability",
    ]


def test_parse_platform_catalog_accepts_current_backend_skill_membership():
    catalog = _current_backend_catalog()

    assert len(catalog.resources) == 8
    assert [skill.name for skill in catalog.skills] == [
        "a2a_communication",
        "command_center",
        "command_center_connections",
        "project_design",
        "project_local_setup",
        "project_to_agent",
        "static_site",
    ]
    assert {skill.name: skill.relative_path.as_posix() for skill in catalog.skills} == {
        name: path.removeprefix("skills/")
        for name, path in _CURRENT_BACKEND_SKILL_PATHS.items()
    }


def test_dual_source_install_preserves_current_backend_skill_hierarchy(tmp_path):
    sdk_skills = tmp_path / "installed-sdk" / "agent_scaffold" / "skills"
    sdk_skill = sdk_skills / "sdk_project_execution"
    sdk_skill.mkdir(parents=True)
    (sdk_skill / "SKILL.md").write_text("sdk execution", encoding="utf-8")
    project_dir = tmp_path / "project"

    install_dual_source_project_skills(
        project_dir=project_dir,
        sdk_library_name="mainsequence",
        sdk_skills_path=sdk_skills,
        sdk_version="6.0.4",
        platform_catalog=_current_backend_catalog(),
    )

    managed_root = project_dir / ".agents" / "skills" / "mainsequence"
    for resource_path in _CURRENT_BACKEND_SKILL_PATHS.values():
        assert (managed_root / resource_path.removeprefix("skills/")).is_file()


def test_parse_platform_catalog_rejects_undeclared_duplicate_and_unsafe_skills():
    rows = _platform_rows()
    rows.append(_platform_rows(skill_names=("undeclared_skill",))[1])
    with pytest.raises(ProjectSkillAssemblyError, match="undeclared listed skills"):
        parse_platform_project_skill_catalog(
            rows,
            source_url="https://platform.example.test/mcp",
        )

    rows = _platform_rows()
    rows.append(rows[-1])
    with pytest.raises(ProjectSkillAssemblyError, match="duplicate URI"):
        parse_platform_project_skill_catalog(
            rows,
            source_url="https://platform.example.test/mcp",
        )

    rows = _platform_rows()
    rows[1]["_meta"]["resource_path"] = "skills/../escape/SKILL.md"
    rows[1]["_content"]["_meta"]["resource_path"] = "skills/../escape/SKILL.md"
    with pytest.raises(ProjectSkillAssemblyError, match="unsafe resource path"):
        parse_platform_project_skill_catalog(
            rows,
            source_url="https://platform.example.test/mcp",
        )

    rows = _platform_rows(
        skill_paths={"alpha_skill": "skills/invalid-group/alpha_skill/SKILL.md"}
    )
    with pytest.raises(ProjectSkillAssemblyError, match="safe lowercase snake case"):
        parse_platform_project_skill_catalog(
            rows,
            source_url="https://platform.example.test/mcp",
        )

    rows = _platform_rows(skill_paths={"alpha_skill": "skills/SKILL.md"})
    with pytest.raises(ProjectSkillAssemblyError, match="at least one skill directory"):
        parse_platform_project_skill_catalog(
            rows,
            source_url="https://platform.example.test/mcp",
        )


def test_parse_platform_catalog_rejects_invalid_front_matter_and_schema_version():
    rows = _platform_rows()
    _replace_resource_content(
        rows[1],
        "---\nname: alpha-skill\n---\n",
    )
    with pytest.raises(ProjectSkillAssemblyError, match="front matter description"):
        parse_platform_project_skill_catalog(
            rows,
            source_url="https://platform.example.test/mcp",
        )

    rows = _platform_rows()
    for row in rows:
        row["_meta"]["manifest_version"] = 999
        row["_content"]["_meta"]["manifest_version"] = 999
    with pytest.raises(ProjectSkillAssemblyError, match="unsupported platform manifest version"):
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
    assert (managed_root / "alpha_skill" / "SKILL.md").is_file()
    assert (managed_root / "beta_skill" / "SKILL.md").is_file()
    assert (managed_root / "gamma_skill" / "SKILL.md").is_file()
    assert (project_owned / "SKILL.md").read_text(encoding="utf-8") == "keep"
    assert [(item.name, item.owner) for item in result.installed] == [
        ("maintenance", "sdk"),
        ("sdk_project_execution", "sdk"),
        ("alpha_skill", "platform"),
        ("beta_skill", "platform"),
        ("gamma_skill", "platform"),
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
        "platform_resource.beta_skill.uri=mainsequence://platform/skills/beta-skill"
    ) in sentinel


def test_dual_source_install_rejects_sdk_platform_path_collision_without_writes(
    tmp_path,
):
    sdk_skills = tmp_path / "installed-sdk" / "agent_scaffold" / "skills"
    colliding_skill = sdk_skills / "beta_skill"
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
    listed_resources.insert(
        1,
        {
            "uri": "mainsequence://documents/release-notes",
            "name": "release_notes",
        },
    )
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
    monkeypatch.setattr(cli_api, "_mcp_client_version", lambda: "5.0.5")
    monkeypatch.setattr(
        cli_api,
        "backend_url",
        lambda: "https://platform.example.test",
    )

    catalog = cli_api.fetch_platform_project_skill_catalog()

    assert catalog.source_url == "https://platform.example.test/mcp"
    assert [item.name for item in catalog.skills] == list(_DEFAULT_TEST_SKILLS)
    assert [call[0] for call in calls] == [
        "initialize",
        "resources/list",
        "resources/read",
        "resources/read",
        "resources/read",
        "resources/read",
    ]
    assert calls[0][3] is None
    assert calls[0][1]["clientInfo"]["version"] == "5.0.5"
    assert all(call[3] == cli_api.MCP_PROTOCOL_VERSION for call in calls[1:])
    assert all(call[1].get("uri") != "mainsequence://documents/release-notes" for call in calls)


def test_cli_fetch_follows_resource_pagination_and_normalizes_order(monkeypatch):
    rows = _platform_rows(skill_names=("gamma_skill", "alpha_skill", "new_capability"))
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
            if not params:
                return {
                    "resources": list(reversed(listed_resources[:2])),
                    "nextCursor": "page-2",
                }
            assert params == {"cursor": "page-2"}
            return {"resources": list(reversed(listed_resources[2:]))}
        return {"contents": [content_by_uri[params["uri"]]]}

    monkeypatch.setattr(cli_api, "_mcp_json_rpc", _mcp_json_rpc)
    monkeypatch.setattr(
        cli_api,
        "backend_url",
        lambda: "https://platform.example.test",
    )

    catalog = cli_api.fetch_platform_project_skill_catalog()

    assert [skill.name for skill in catalog.skills] == [
        "alpha_skill",
        "gamma_skill",
        "new_capability",
    ]
    assert [call[0] for call in calls[:3]] == [
        "initialize",
        "resources/list",
        "resources/list",
    ]
    assert calls[1][2] == 2
    assert calls[2][2] == 3


def test_cli_fetch_rejects_repeated_resource_pagination_cursor(monkeypatch):
    def _mcp_json_rpc(method, _params, **_kwargs):
        if method == "initialize":
            return {
                "protocolVersion": cli_api.MCP_PROTOCOL_VERSION,
                "capabilities": {"resources": {}},
            }
        return {"resources": [], "nextCursor": "repeat"}

    monkeypatch.setattr(cli_api, "_mcp_json_rpc", _mcp_json_rpc)

    with pytest.raises(ApiError, match="repeated a pagination cursor"):
        cli_api.fetch_platform_project_skill_catalog()


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
        sdk_root / "agent_scaffold" / "skills" / "project_local_setup",
        sdk_root / "agent_scaffold" / "skills" / "project_to_agent",
        sdk_root / "agent_scaffold" / "skills" / "static_site",
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
