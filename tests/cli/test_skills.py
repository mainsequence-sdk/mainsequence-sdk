from __future__ import annotations

import json
import pathlib

import pytest

from tests._support import REPOSITORY_ROOT
from tests.cli.support import _cli_platform_skill_catalog, _write_installed_agent_scaffold_bundle


def test_skills_list(cli_mod, runner, monkeypatch, tmp_path):
    bundle_dir = tmp_path / "agent_scaffold"
    (bundle_dir / "skills" / "code_repository_design").mkdir(parents=True)
    (bundle_dir / "skills" / "code_repository_design" / "SKILL.md").write_text(
        "CodeRepository design", encoding="utf-8"
    )
    (bundle_dir / "skills" / "data_publishing" / "meta_tables").mkdir(parents=True)
    (bundle_dir / "skills" / "data_publishing" / "meta_tables" / "SKILL.md").write_text(
        "MetaTable publishing", encoding="utf-8"
    )

    monkeypatch.setattr(cli_mod, "_installed_agent_scaffold_bundle_dir", lambda: bundle_dir)

    result = runner.invoke(cli_mod.app, ["skills", "list"])
    assert result.exit_code == 0
    assert "code_repository_design" in result.output
    assert "data_publishing/meta_tables" in result.output


def test_skills_path(cli_mod, runner, monkeypatch, tmp_path):
    bundle_dir = tmp_path / "agent_scaffold"
    (bundle_dir / "skills" / "data_publishing" / "meta_tables").mkdir(parents=True)
    expected = bundle_dir / "skills" / "data_publishing" / "meta_tables" / "SKILL.md"
    expected.write_text("MetaTable publishing", encoding="utf-8")

    monkeypatch.setattr(cli_mod, "_installed_agent_scaffold_bundle_dir", lambda: bundle_dir)

    result = runner.invoke(cli_mod.app, ["skills", "path", "data_publishing/meta_tables"])
    assert result.exit_code == 0
    assert result.output.strip() == str(expected)


def test_skills_path_bundle_root(cli_mod, runner, monkeypatch, tmp_path):
    bundle_dir = tmp_path / "agent_scaffold"
    (bundle_dir / "skills").mkdir(parents=True)

    monkeypatch.setattr(cli_mod, "_installed_agent_scaffold_bundle_dir", lambda: bundle_dir)

    result = runner.invoke(cli_mod.app, ["skills", "path"])
    assert result.exit_code == 0
    assert result.output.strip() == str(bundle_dir / "skills")


def test_skills_path_unique_leaf_name(cli_mod, runner, monkeypatch, tmp_path):
    bundle_dir = tmp_path / "agent_scaffold"
    (bundle_dir / "skills" / "data_publishing" / "meta_tables").mkdir(parents=True)
    expected = bundle_dir / "skills" / "data_publishing" / "meta_tables" / "SKILL.md"
    expected.write_text("MetaTable publishing", encoding="utf-8")

    monkeypatch.setattr(cli_mod, "_installed_agent_scaffold_bundle_dir", lambda: bundle_dir)

    result = runner.invoke(cli_mod.app, ["skills", "path", "meta_tables"])
    assert result.exit_code == 0
    assert result.output.strip() == str(expected)


def test_copy_llm_instructions_print(cli_mod, runner, tmp_path):
    docs_dir = tmp_path / "examples" / "ai" / "instructions"
    docs_dir.mkdir(parents=True, exist_ok=True)
    (docs_dir / "01.md").write_text("# test\nhello\n", encoding="utf-8")
    result = runner.invoke(
        cli_mod.app,
        ["copy-llm-instructions", "--dir", str(docs_dir), "--print"],
    )
    assert result.exit_code == 0
    assert "AI Instructions" in result.output
    assert "hello" in result.output


def test_code_repository_update_agents_md_replaces_custom_unmarked_file(
    cli_mod,
    runner,
    monkeypatch,
    tmp_path,
):
    agents_md = _write_installed_agent_scaffold_bundle(tmp_path / "bundle")
    target = tmp_path / "code-repository"
    target.mkdir()
    original = "# CodeRepository Agents\n\nKeep this repository-specific instruction.\n"
    (target / "AGENTS.md").write_text(original, encoding="utf-8")
    monkeypatch.setattr(cli_mod, "_installed_agent_scaffold_bundle_dir", lambda: agents_md.parent)

    result = runner.invoke(
        cli_mod.app,
        ["code-repository", "update", "AGENTS.md", "--path", str(target)],
    )
    assert result.exit_code == 0
    content = (target / "AGENTS.md").read_text(encoding="utf-8")
    assert content == agents_md.read_text(encoding="utf-8")
    assert "Installed managed block." in cli_mod._extract_agents_md_managed_block(content)
    assert "Installed managed block." in content
    assert "Keep this repository-specific instruction." not in content
    assert "Action" in result.output
    assert "replaced" in result.output


def test_code_repository_update_agents_md_creates_file_when_missing(
    cli_mod,
    runner,
    monkeypatch,
    tmp_path,
):
    agents_md = _write_installed_agent_scaffold_bundle(tmp_path / "bundle")
    target = tmp_path / "code-repository"
    target.mkdir()
    monkeypatch.setattr(cli_mod, "_installed_agent_scaffold_bundle_dir", lambda: agents_md.parent)

    result = runner.invoke(
        cli_mod.app,
        ["code-repository", "update", "AGENTS.md", "--path", str(target)],
    )
    assert result.exit_code == 0
    content = (target / "AGENTS.md").read_text(encoding="utf-8")
    assert content == agents_md.read_text(encoding="utf-8")
    assert cli_mod.AGENTS_MD_MANAGED_BLOCK_START_PREFIX in content
    assert "Action" in result.output
    assert "created" in result.output


def test_code_repository_update_agents_md_replaces_existing_managed_block(
    cli_mod,
    runner,
    monkeypatch,
    tmp_path,
):
    agents_md = _write_installed_agent_scaffold_bundle(tmp_path / "bundle")
    target = tmp_path / "code-repository"
    target.mkdir()
    old_block = "\n".join(
        [
            f"{cli_mod.AGENTS_MD_MANAGED_BLOCK_START_PREFIX} schema=old source=old -->",
            "old managed content",
            cli_mod.AGENTS_MD_MANAGED_BLOCK_END,
        ]
    )
    (target / "AGENTS.md").write_text(
        f"# CodeRepository Agents\n\n{old_block}\n\nCustom tail.\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(cli_mod, "_installed_agent_scaffold_bundle_dir", lambda: agents_md.parent)

    result = runner.invoke(
        cli_mod.app,
        ["code-repository", "update", "AGENTS.md", "--path", str(target)],
    )
    assert result.exit_code == 0
    content = (target / "AGENTS.md").read_text(encoding="utf-8")
    assert "old managed content" not in content
    assert "Installed managed block." in content
    assert "Custom tail.\n" in content
    assert "Installed managed block." in cli_mod._extract_agents_md_managed_block(content)
    assert "updated" in result.output


def test_code_repository_update_agents_md_json_reports_action(
    cli_mod,
    runner,
    monkeypatch,
    tmp_path,
):
    agents_md = _write_installed_agent_scaffold_bundle(tmp_path / "bundle")
    target = tmp_path / "code-repository"
    target.mkdir()
    monkeypatch.setattr(cli_mod, "_installed_agent_scaffold_bundle_dir", lambda: agents_md.parent)

    result = runner.invoke(
        cli_mod.app,
        ["code-repository", "update", "AGENTS.md", "--path", str(target), "--json"],
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["target"] == "AGENTS.md"
    assert payload["code_repository"] == str(target)
    assert payload["source"] == str(agents_md)
    assert payload["destination"] == str(target / "AGENTS.md")
    assert payload["action"] == "created"
    assert payload["changed"] is True
    assert payload["overwritten"] is False


def test_code_repository_update_agents_md_keeps_existing_scaffold_shaped_file_current(
    cli_mod,
    runner,
    monkeypatch,
    tmp_path,
):
    agents_md = _write_installed_agent_scaffold_bundle(tmp_path / "bundle")
    target = tmp_path / "code-repository"
    target.mkdir()
    (target / "AGENTS.md").write_text(agents_md.read_text(encoding="utf-8"), encoding="utf-8")
    monkeypatch.setattr(cli_mod, "_installed_agent_scaffold_bundle_dir", lambda: agents_md.parent)

    result = runner.invoke(
        cli_mod.app,
        ["code-repository", "update", "AGENTS.md", "--path", str(target)],
    )
    assert result.exit_code == 0
    assert (target / "AGENTS.md").read_text(encoding="utf-8") == agents_md.read_text(
        encoding="utf-8"
    )
    assert "already current" in result.output


def test_code_repository_update_agents_md_rejects_malformed_managed_block(
    cli_mod,
    runner,
    monkeypatch,
    tmp_path,
):
    agents_md = _write_installed_agent_scaffold_bundle(tmp_path / "bundle")
    target = tmp_path / "code-repository"
    target.mkdir()
    start_marker = (
        f"{cli_mod.AGENTS_MD_MANAGED_BLOCK_START_PREFIX} schema=1 source=agent_scaffold -->"
    )
    (target / "AGENTS.md").write_text(
        f"# CodeRepository Agents\n\n{start_marker}\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(cli_mod, "_installed_agent_scaffold_bundle_dir", lambda: agents_md.parent)

    result = runner.invoke(
        cli_mod.app,
        ["code-repository", "update", "AGENTS.md", "--path", str(target)],
    )
    assert result.exit_code == 1
    assert "malformed" in result.output


def test_code_repository_update_agents_md_rejects_duplicate_managed_blocks(
    cli_mod,
    runner,
    monkeypatch,
    tmp_path,
):
    agents_md = _write_installed_agent_scaffold_bundle(tmp_path / "bundle")
    target = tmp_path / "code-repository"
    target.mkdir()
    block = "\n".join(
        [
            f"{cli_mod.AGENTS_MD_MANAGED_BLOCK_START_PREFIX} schema=1 source=agent_scaffold -->",
            "managed content",
            cli_mod.AGENTS_MD_MANAGED_BLOCK_END,
        ]
    )
    (target / "AGENTS.md").write_text(f"{block}\n\n{block}\n", encoding="utf-8")
    monkeypatch.setattr(cli_mod, "_installed_agent_scaffold_bundle_dir", lambda: agents_md.parent)

    result = runner.invoke(
        cli_mod.app,
        ["code-repository", "update", "AGENTS.md", "--path", str(target)],
    )
    assert result.exit_code == 1
    assert "multiple" in result.output


def test_code_repository_update_agents_md_fails_when_installed_bundle_missing_agents_md(
    cli_mod,
    runner,
    monkeypatch,
    tmp_path,
):
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir()
    target = tmp_path / "code-repository"
    target.mkdir()
    monkeypatch.setattr(cli_mod, "_installed_agent_scaffold_bundle_dir", lambda: bundle_dir)

    result = runner.invoke(
        cli_mod.app,
        ["code-repository", "update", "AGENTS.md", "--path", str(target)],
    )
    assert result.exit_code == 1
    assert "Installed agent_scaffold bundle is missing AGENTS.md" in result.output


def test_installed_agent_scaffold_bundle_dir_falls_back_to_sibling_package(cli_mod, monkeypatch):
    original_import_module = cli_mod.importlib.import_module

    def _import_module(name, package=None):
        if name == "agent_scaffold":
            raise ModuleNotFoundError("No module named 'agent_scaffold'")
        return original_import_module(name, package)

    monkeypatch.setattr(cli_mod.importlib, "import_module", _import_module)

    resolved = cli_mod._installed_agent_scaffold_bundle_dir()
    expected = pathlib.Path(cli_mod.__file__).resolve().parents[2] / "agent_scaffold"
    assert resolved == expected.resolve()


def test_code_repository_update_agent_skills_overwrites_matching_folders(
    cli_mod, runner, monkeypatch, tmp_path
):
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir()
    (bundle_dir / "AGENTS.md").write_text("bundle agents", encoding="utf-8")
    (bundle_dir / "skills" / "data_publishing").mkdir(parents=True)
    (bundle_dir / "skills" / "data_publishing" / "SKILL.md").write_text(
        "new data skill", encoding="utf-8"
    )
    (bundle_dir / "skills" / "maintenance").mkdir(parents=True)
    (bundle_dir / "skills" / "maintenance" / "SKILL.md").write_text(
        "new maintenance skill", encoding="utf-8"
    )
    (bundle_dir / "skills" / "__pycache__").mkdir()
    (bundle_dir / "skills" / "__pycache__" / "ignored.txt").write_text(
        "ignore me", encoding="utf-8"
    )

    target = tmp_path / "code-repository"
    stale_top_level = target / ".agents" / "skills" / "data_publishing"
    stale_top_level.mkdir(parents=True)
    (stale_top_level / "old.txt").write_text("stale but preserved", encoding="utf-8")
    existing_mainsequence = target / ".agents" / "skills" / "mainsequence" / "data_publishing"
    existing_mainsequence.mkdir(parents=True)
    (existing_mainsequence / "old.txt").write_text("stale mainsequence skill", encoding="utf-8")

    monkeypatch.setattr(
        cli_mod,
        "_code_repository_agent_scaffold_bundle_dir",
        lambda code_repository_dir: bundle_dir,
    )
    monkeypatch.setattr(
        cli_mod,
        "_code_repository_installed_package_version",
        lambda code_repository_dir, package_name: "4.4.3",
    )
    monkeypatch.setattr(
        cli_mod,
        "fetch_platform_code_repository_skill_catalog",
        _cli_platform_skill_catalog,
    )

    result = runner.invoke(
        cli_mod.app, ["code-repository", "update-agent-skills", "--path", str(target)]
    )
    assert result.exit_code == 0
    assert (
        target / ".agents" / "skills" / "mainsequence" / "data_publishing" / "SKILL.md"
    ).read_text(encoding="utf-8") == "new data skill"
    assert not (
        target / ".agents" / "skills" / "mainsequence" / "data_publishing" / "old.txt"
    ).exists()
    assert (target / ".agents" / "skills" / "data_publishing" / "old.txt").exists()
    assert (target / ".agents" / "skills" / "mainsequence" / "maintenance" / "SKILL.md").read_text(
        encoding="utf-8"
    ) == "new maintenance skill"
    assert not (target / ".agents" / "skills" / "mainsequence" / "__pycache__").exists()
    sentinel = target / ".agents" / "skills" / "mainsequence" / "PINNED_FROM.txt"
    sentinel_content = sentinel.read_text(encoding="utf-8")
    assert "library_name=mainsequence" in sentinel_content
    assert "namespace=mainsequence" in sentinel_content
    assert "pinned_version=4.4.3" in sentinel_content
    assert f"skills_path={(bundle_dir / 'skills').resolve()}" in sentinel_content
    assert "platform_manifest_sha256" not in sentinel_content
    assert "Updated CodeRepository Skills" in result.output
    assert "SDK Version" in result.output
    assert {item.name for item in sentinel.parent.iterdir()} == {
        "data_publishing",
        "maintenance",
        "PINNED_FROM.txt",
    }


def test_code_repository_update_agent_skills_json_reports_pin_sentinel(
    cli_mod, runner, monkeypatch, tmp_path
):
    bundle_dir = tmp_path / "bundle"
    (bundle_dir / "skills" / "data_publishing").mkdir(parents=True)
    (bundle_dir / "skills" / "data_publishing" / "SKILL.md").write_text(
        "new data skill", encoding="utf-8"
    )
    target = tmp_path / "code-repository"
    target.mkdir()

    monkeypatch.setattr(
        cli_mod,
        "_code_repository_agent_scaffold_bundle_dir",
        lambda code_repository_dir: bundle_dir,
    )
    monkeypatch.setattr(
        cli_mod,
        "_code_repository_installed_package_version",
        lambda code_repository_dir, package_name: "4.4.3",
    )
    monkeypatch.setattr(
        cli_mod,
        "fetch_platform_code_repository_skill_catalog",
        _cli_platform_skill_catalog,
    )

    result = runner.invoke(
        cli_mod.app,
        ["code-repository", "update-agent-skills", "--path", str(target), "--json"],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    sentinel = target / ".agents" / "skills" / "mainsequence" / "PINNED_FROM.txt"
    assert payload["library_name"] == "mainsequence"
    assert payload["namespace"] == "mainsequence"
    assert payload["pinned_version"] == "4.4.3"
    assert payload["sentinel_path"] == str(sentinel.resolve())
    assert payload["destination_root"] == str(
        (target / ".agents" / "skills" / "mainsequence").resolve()
    )
    assert payload["updated_count"] == 1
    assert [item["name"] for item in payload["updated"]] == [
        "data_publishing",
    ]
    assert payload["sdk"]["version"] == "4.4.3"
    assert "platform" not in payload
    assert "pinned_version=4.4.3" in sentinel.read_text(encoding="utf-8")


def test_sdk_skills_and_agents_refresh_without_authentication(
    cli_mod, runner, monkeypatch, tmp_path
):
    bundle = REPOSITORY_ROOT / "agent_scaffold"
    target = tmp_path / "repository"
    stale = target / ".agents" / "skills" / "mainsequence" / "data_publishing" / "SKILL.md"
    stale.parent.mkdir(parents=True)
    stale.write_text("retired SDK table instructions", encoding="utf-8")
    (stale.parent.parent / "PINNED_FROM.txt").write_text(
        "schema=2\nsdk_version=8.1.25\n", encoding="utf-8"
    )
    platform = target / ".agents" / "skills" / "mainsequence_platform" / "SKILL.md"
    platform.parent.mkdir(parents=True)
    platform.write_text("platform instructions", encoding="utf-8")
    extension = target / ".agents" / "skills" / "metatables" / "SKILL.md"
    extension.parent.mkdir(parents=True)
    extension.write_text("domain package instructions", encoding="utf-8")
    agents = target / "AGENTS.md"
    agents.write_text(
        "Repository preface.\n<!-- mainsequence-agent-scaffold:start schema=1 -->\n"
        "Old managed routing.\n<!-- mainsequence-agent-scaffold:end -->\nRepository suffix.\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(cli_mod, "_code_repository_agent_scaffold_bundle_dir", lambda *_: bundle)
    monkeypatch.setattr(cli_mod, "_code_repository_installed_package_version", lambda *_: "9.0.2")
    monkeypatch.setattr(cli_mod, "_installed_agent_scaffold_bundle_dir", lambda: bundle)
    for name in (
        "fetch_platform_code_repository_skill_catalog",
        "_require_login",
        "get_code_repository_context",
    ):
        monkeypatch.setattr(
            cli_mod, name, lambda *args, **kwargs: pytest.fail("Local copy reached the backend")
        )

    result = runner.invoke(
        cli_mod.app, ["code-repository", "update-agent-skills", "--path", str(target), "--json"]
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["sdk"]["version"] == "9.0.2"
    assert "platform" not in payload
    assert not stale.exists()
    destination = pathlib.Path(payload["destination_root"])
    expected = {
        p.relative_to(bundle / "skills"): p.read_bytes()
        for p in (bundle / "skills").rglob("*")
        if p.is_file()
    }
    actual = {
        p.relative_to(destination): p.read_bytes()
        for p in destination.rglob("*")
        if p.is_file() and p.name != "PINNED_FROM.txt"
    }
    assert actual == expected
    assert platform.read_text() == "platform instructions"
    assert extension.read_text() == "domain package instructions"

    result = runner.invoke(
        cli_mod.app, ["code-repository", "update", "AGENTS.md", "--path", str(target)]
    )
    assert result.exit_code == 0, result.output
    content = agents.read_text(encoding="utf-8")
    assert cli_mod._extract_agents_md_managed_block((bundle / "AGENTS.md").read_text()) in content
    assert content.startswith("Repository preface.\n")
    assert content.endswith("Repository suffix.\n")
