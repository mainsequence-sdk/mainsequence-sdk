from __future__ import annotations

import json
import pathlib

import pytest

from tests._support import REPOSITORY_ROOT
from tests.cli.support import _cli_platform_skill_catalog


def test_skills_list(cli_mod, runner, monkeypatch, tmp_path):
    bundle_dir = tmp_path / "agent_scaffold"
    (bundle_dir / "skills" / "sdk_code_repository_execution").mkdir(parents=True)
    (bundle_dir / "skills" / "sdk_code_repository_execution" / "SKILL.md").write_text(
        "SDK CodeRepository execution", encoding="utf-8"
    )
    (bundle_dir / "skills" / "data_publishing" / "meta_tables").mkdir(parents=True)
    (bundle_dir / "skills" / "data_publishing" / "meta_tables" / "SKILL.md").write_text(
        "MetaTable publishing", encoding="utf-8"
    )

    monkeypatch.setattr(cli_mod, "_installed_agent_scaffold_bundle_dir", lambda: bundle_dir)

    result = runner.invoke(cli_mod.app, ["skills", "list"])
    assert result.exit_code == 0
    assert "sdk_code_repository_execution" in result.output
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
    cli_mod, runner, monkeypatch, tmp_path, git_checkout
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

    target = git_checkout()
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
    assert not (target / "AGENTS.md").exists()


def test_code_repository_update_agent_skills_json_reports_pin_sentinel(
    cli_mod, runner, monkeypatch, tmp_path, git_checkout
):
    bundle_dir = tmp_path / "bundle"
    (bundle_dir / "skills" / "data_publishing").mkdir(parents=True)
    (bundle_dir / "skills" / "data_publishing" / "SKILL.md").write_text(
        "new data skill", encoding="utf-8"
    )
    target = git_checkout()

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


@pytest.mark.parametrize(
    "agents_content",
    [
        None,
        b"# Repository instructions\r\n\r\nKeep this file exactly as written.\r\n",
        b"Repository preface.\n<!-- mainsequence-agent-scaffold:start schema=1 -->\n"
        b"Old managed routing.\n<!-- mainsequence-agent-scaffold:end -->\nRepository suffix.\n",
    ],
    ids=["missing", "repository-owned", "previously-scaffolded"],
)
def test_sdk_skill_refresh_preserves_repository_instructions_without_authentication(
    cli_mod, runner, monkeypatch, git_checkout, agents_content
):
    bundle = REPOSITORY_ROOT / "agent_scaffold"
    target = git_checkout("repository")
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
    if agents_content is not None:
        agents.write_bytes(agents_content)
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

    if agents_content is None:
        assert not agents.exists()
    else:
        assert agents.read_bytes() == agents_content


def test_removed_agents_update_command_preserves_repository_instructions(
    cli_mod, runner, git_checkout
):
    target = git_checkout()
    agents = target / "AGENTS.md"
    content = b"# Repository-owned instructions\r\n"
    agents.write_bytes(content)

    result = runner.invoke(
        cli_mod.app, ["code-repository", "update", "AGENTS.md", "--path", str(target)]
    )

    assert result.exit_code != 0
    assert agents.read_bytes() == content
