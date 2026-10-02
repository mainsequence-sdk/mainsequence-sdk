from __future__ import annotations

import os
from pathlib import Path

import pytest

from mainsequence.scaffold_skills import (
    ScaffoldSkillCopyBlocked,
    copy_scaffold_skills,
    normalize_scaffold_skill_namespace,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SCAFFOLD_ROOT = REPOSITORY_ROOT / "agent_scaffold"


def _write_skill(skills_root, name: str, content: str = "skill") -> None:
    skill_dir = skills_root / name
    skill_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(content, encoding="utf-8")


def test_copy_scaffold_skills_copies_namespace_and_writes_pin_sentinel(tmp_path):
    skills_root = tmp_path / "package" / "scaffold_skills"
    _write_skill(skills_root, "data_publishing", "new data skill")
    _write_skill(skills_root, "maintenance", "new maintenance skill")
    (skills_root / "README.md").write_text("not a skill", encoding="utf-8")
    _write_skill(skills_root, ".hidden", "hidden")
    _write_skill(skills_root, "__pycache__", "cache")

    code_repository_dir = tmp_path / "code-repository"
    code_repository_owned = code_repository_dir / ".agents" / "skills" / "data_publishing"
    code_repository_owned.mkdir(parents=True)
    (code_repository_owned / "old.txt").write_text("preserved", encoding="utf-8")
    existing_managed = code_repository_dir / ".agents" / "skills" / "ms_markets" / "data_publishing"
    existing_managed.mkdir(parents=True)
    (existing_managed / "old.txt").write_text("removed", encoding="utf-8")
    retired = existing_managed.parent / "retired_skill"
    retired.mkdir()
    (retired / "SKILL.md").write_text("obsolete", encoding="utf-8")

    result = copy_scaffold_skills(
        code_repository_dir=code_repository_dir,
        library_name="ms-markets",
        namespace="ms_markets",
        skills_path=skills_root,
        pinned_version="0.8.1",
        command="msm copy-msm-skills",
    )

    destination_root = code_repository_dir / ".agents" / "skills" / "ms_markets"
    assert result.destination_root == destination_root.resolve()
    assert [item.name for item in result.copied] == ["data_publishing", "maintenance"]
    assert (destination_root / "data_publishing" / "SKILL.md").read_text(
        encoding="utf-8"
    ) == "new data skill"
    assert not (destination_root / "data_publishing" / "old.txt").exists()
    assert not retired.exists()
    assert {item.name for item in destination_root.iterdir()} == {
        "data_publishing",
        "maintenance",
        "PINNED_FROM.txt",
    }
    assert (code_repository_owned / "old.txt").exists()
    assert not (destination_root / ".hidden").exists()
    assert not (destination_root / "__pycache__").exists()

    sentinel = destination_root / "PINNED_FROM.txt"
    sentinel_content = sentinel.read_text(encoding="utf-8")
    assert "schema=1" in sentinel_content
    assert "library_name=ms-markets" in sentinel_content
    assert "namespace=ms_markets" in sentinel_content
    assert "pinned_version=0.8.1" in sentinel_content
    assert f"skills_path={skills_root.resolve()}" in sentinel_content
    assert "command=msm copy-msm-skills" in sentinel_content


def test_copy_scaffold_skills_dry_run_writes_nothing(tmp_path):
    skills_root = tmp_path / "package" / "scaffold_skills"
    _write_skill(skills_root, "data_publishing")
    code_repository_dir = tmp_path / "code-repository"

    result = copy_scaffold_skills(
        code_repository_dir=code_repository_dir,
        library_name="mainsequence",
        skills_path=skills_root,
        pinned_version="4.4.3",
        dry_run=True,
    )

    assert result.dry_run is True
    assert [item.name for item in result.copied] == ["data_publishing"]
    assert not (code_repository_dir / ".agents").exists()


def test_copy_scaffold_skills_rolls_back_failed_namespace_replacement(tmp_path, monkeypatch):
    source = tmp_path / "library-skills"
    _write_skill(source, "new_skill")
    repository = tmp_path / "repository"
    destination = repository / ".agents" / "skills" / "mainsequence"
    _write_skill(destination, "retired_skill", "keep until replacement succeeds")
    sentinel = destination / "PINNED_FROM.txt"
    sentinel.write_text("pinned_version=old\n", encoding="utf-8")
    original = os.replace

    def fail_staging_swap(source, target):
        if ".staging-" in str(source):
            raise OSError("simulated swap failure")
        return original(source, target)

    monkeypatch.setattr(os, "replace", fail_staging_swap)
    with pytest.raises(OSError, match="swap failure"):
        copy_scaffold_skills(
            code_repository_dir=repository,
            library_name="mainsequence",
            skills_path=source,
            pinned_version="9.0.2",
        )
    assert (
        destination / "retired_skill" / "SKILL.md"
    ).read_text() == "keep until replacement succeeds"
    assert sentinel.read_text() == "pinned_version=old\n"
    assert not list(destination.parent.glob(".mainsequence.staging-*"))
    assert not list(destination.parent.glob(".mainsequence.backup-*"))


@pytest.mark.parametrize("version", [None, "", " ", "unknown", "none", "null"])
def test_copy_scaffold_skills_requires_resolved_pinned_version(tmp_path, version):
    skills_root = tmp_path / "package" / "scaffold_skills"
    _write_skill(skills_root, "data_publishing")

    with pytest.raises(ValueError, match="pinned_version"):
        copy_scaffold_skills(
            code_repository_dir=tmp_path / "code-repository",
            library_name="mainsequence",
            skills_path=skills_root,
            pinned_version=version,
        )


def test_copy_scaffold_skills_cannot_replace_another_namespace_through_a_symlink(tmp_path):
    source = tmp_path / "library-skills"
    _write_skill(source, "new_skill")
    repository = tmp_path / "repository"
    sibling = repository / ".agents" / "skills" / "metatables"
    _write_skill(sibling, "owned_skill", "keep")
    (sibling.parent / "mainsequence").symlink_to(sibling, target_is_directory=True)
    with pytest.raises(ScaffoldSkillCopyBlocked, match="symlink"):
        copy_scaffold_skills(
            code_repository_dir=repository,
            library_name="mainsequence",
            skills_path=source,
            pinned_version="9.0.2",
        )
    assert (sibling / "owned_skill" / "SKILL.md").read_text() == "keep"


def test_copy_scaffold_skills_blocks_destination_source_overlap(tmp_path):
    code_repository_dir = tmp_path / "code-repository"
    skills_root = code_repository_dir / ".agents" / "skills" / "ms_markets"
    _write_skill(skills_root, "data_publishing")

    with pytest.raises(ScaffoldSkillCopyBlocked, match="overlap"):
        copy_scaffold_skills(
            code_repository_dir=code_repository_dir,
            library_name="ms-markets",
            namespace="ms_markets",
            skills_path=skills_root,
            pinned_version="0.8.1",
        )


def test_copy_scaffold_skills_blocks_destination_inside_source(tmp_path):
    code_repository_dir = tmp_path / "code-repository"
    skills_root = code_repository_dir / ".agents"
    _write_skill(skills_root, "data_publishing")

    with pytest.raises(ScaffoldSkillCopyBlocked, match="overlap"):
        copy_scaffold_skills(
            code_repository_dir=code_repository_dir,
            library_name="ms-markets",
            namespace="ms_markets",
            skills_path=skills_root,
            pinned_version="0.8.1",
        )


def test_copy_scaffold_skills_blocks_source_inside_destination(tmp_path):
    code_repository_dir = tmp_path / "code-repository"
    skills_root = code_repository_dir / ".agents" / "skills" / "ms_markets" / "source"
    _write_skill(skills_root, "data_publishing")

    with pytest.raises(ScaffoldSkillCopyBlocked, match="overlap"):
        copy_scaffold_skills(
            code_repository_dir=code_repository_dir,
            library_name="ms-markets",
            namespace="ms_markets",
            skills_path=skills_root,
            pinned_version="0.8.1",
        )


def test_copy_scaffold_skills_blocks_protected_code_repository_root(tmp_path):
    skills_root = tmp_path / "package" / "scaffold_skills"
    _write_skill(skills_root, "data_publishing")
    code_repository_dir = tmp_path / "ms-markets"

    with pytest.raises(ScaffoldSkillCopyBlocked, match="protected"):
        copy_scaffold_skills(
            code_repository_dir=code_repository_dir,
            library_name="ms-markets",
            namespace="ms_markets",
            skills_path=skills_root,
            pinned_version="0.8.1",
            protected_code_repository_roots=(code_repository_dir,),
        )


def test_copy_scaffold_skills_blocks_code_repository_guard_reason(tmp_path):
    skills_root = tmp_path / "package" / "scaffold_skills"
    _write_skill(skills_root, "data_publishing")

    with pytest.raises(ScaffoldSkillCopyBlocked, match="source checkout"):
        copy_scaffold_skills(
            code_repository_dir=tmp_path / "code-repository",
            library_name="ms-markets",
            namespace="ms_markets",
            skills_path=skills_root,
            pinned_version="0.8.1",
            code_repository_guard=lambda _code_repository_dir: "source checkout",
        )


@pytest.mark.parametrize("namespace", ["", "bad/name", "bad name", "../bad"])
def test_normalize_scaffold_skill_namespace_rejects_invalid_names(namespace):
    with pytest.raises(ValueError):
        normalize_scaffold_skill_namespace(namespace)


def test_normalize_scaffold_skill_namespace_derives_package_style_name():
    assert normalize_scaffold_skill_namespace("ms-markets") == "ms_markets"


def test_packaged_scaffold_keeps_retained_sdk_skills():
    skill_paths = {
        path.relative_to(SCAFFOLD_ROOT / "skills").as_posix()
        for path in (SCAFFOLD_ROOT / "skills").rglob("SKILL.md")
    }

    assert skill_paths == {
        "a2a_sdk_execution/SKILL.md",
        "mainsequence-inference/SKILL.md",
        "application_surfaces/api_surfaces/SKILL.md",
        "maintenance/bug_auditor/SKILL.md",
        "maintenance/code_repository_maintenance/SKILL.md",
        "maintenance/metatables_transition/SKILL.md",
        "platform_operations/access_control_and_sharing/SKILL.md",
        "platform_operations/orchestration_and_releases/SKILL.md",
        "sdk_code_repository_execution/SKILL.md",
    }
    assert (SCAFFOLD_ROOT / "AGENTS.md").is_file()
