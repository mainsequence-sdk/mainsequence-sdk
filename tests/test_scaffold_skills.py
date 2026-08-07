from __future__ import annotations

import pytest

from mainsequence.scaffold_skills import (
    ScaffoldSkillCopyBlocked,
    copy_scaffold_skills,
    normalize_scaffold_skill_namespace,
)


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

    project_dir = tmp_path / "project"
    project_owned = project_dir / ".agents" / "skills" / "data_publishing"
    project_owned.mkdir(parents=True)
    (project_owned / "old.txt").write_text("preserved", encoding="utf-8")
    existing_managed = project_dir / ".agents" / "skills" / "ms_markets" / "data_publishing"
    existing_managed.mkdir(parents=True)
    (existing_managed / "old.txt").write_text("removed", encoding="utf-8")

    result = copy_scaffold_skills(
        project_dir=project_dir,
        library_name="ms-markets",
        namespace="ms_markets",
        skills_path=skills_root,
        pinned_version="0.8.1",
        command="msm copy-msm-skills",
    )

    destination_root = project_dir / ".agents" / "skills" / "ms_markets"
    assert result.destination_root == destination_root.resolve()
    assert [item.name for item in result.copied] == ["data_publishing", "maintenance"]
    assert (destination_root / "data_publishing" / "SKILL.md").read_text(
        encoding="utf-8"
    ) == "new data skill"
    assert not (destination_root / "data_publishing" / "old.txt").exists()
    assert (project_owned / "old.txt").exists()
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
    project_dir = tmp_path / "project"

    result = copy_scaffold_skills(
        project_dir=project_dir,
        library_name="mainsequence",
        skills_path=skills_root,
        pinned_version="4.4.3",
        dry_run=True,
    )

    assert result.dry_run is True
    assert [item.name for item in result.copied] == ["data_publishing"]
    assert not (project_dir / ".agents").exists()


@pytest.mark.parametrize("version", [None, "", " ", "unknown", "none", "null"])
def test_copy_scaffold_skills_requires_resolved_pinned_version(tmp_path, version):
    skills_root = tmp_path / "package" / "scaffold_skills"
    _write_skill(skills_root, "data_publishing")

    with pytest.raises(ValueError, match="pinned_version"):
        copy_scaffold_skills(
            project_dir=tmp_path / "project",
            library_name="mainsequence",
            skills_path=skills_root,
            pinned_version=version,
        )


def test_copy_scaffold_skills_blocks_destination_source_overlap(tmp_path):
    project_dir = tmp_path / "project"
    skills_root = project_dir / ".agents" / "skills" / "ms_markets"
    _write_skill(skills_root, "data_publishing")

    with pytest.raises(ScaffoldSkillCopyBlocked, match="overlap"):
        copy_scaffold_skills(
            project_dir=project_dir,
            library_name="ms-markets",
            namespace="ms_markets",
            skills_path=skills_root,
            pinned_version="0.8.1",
        )


def test_copy_scaffold_skills_blocks_destination_inside_source(tmp_path):
    project_dir = tmp_path / "project"
    skills_root = project_dir / ".agents"
    _write_skill(skills_root, "data_publishing")

    with pytest.raises(ScaffoldSkillCopyBlocked, match="overlap"):
        copy_scaffold_skills(
            project_dir=project_dir,
            library_name="ms-markets",
            namespace="ms_markets",
            skills_path=skills_root,
            pinned_version="0.8.1",
        )


def test_copy_scaffold_skills_blocks_source_inside_destination(tmp_path):
    project_dir = tmp_path / "project"
    skills_root = project_dir / ".agents" / "skills" / "ms_markets" / "source"
    _write_skill(skills_root, "data_publishing")

    with pytest.raises(ScaffoldSkillCopyBlocked, match="overlap"):
        copy_scaffold_skills(
            project_dir=project_dir,
            library_name="ms-markets",
            namespace="ms_markets",
            skills_path=skills_root,
            pinned_version="0.8.1",
        )


def test_copy_scaffold_skills_blocks_protected_project_root(tmp_path):
    skills_root = tmp_path / "package" / "scaffold_skills"
    _write_skill(skills_root, "data_publishing")
    project_dir = tmp_path / "ms-markets"

    with pytest.raises(ScaffoldSkillCopyBlocked, match="protected"):
        copy_scaffold_skills(
            project_dir=project_dir,
            library_name="ms-markets",
            namespace="ms_markets",
            skills_path=skills_root,
            pinned_version="0.8.1",
            protected_project_roots=(project_dir,),
        )


def test_copy_scaffold_skills_blocks_project_guard_reason(tmp_path):
    skills_root = tmp_path / "package" / "scaffold_skills"
    _write_skill(skills_root, "data_publishing")

    with pytest.raises(ScaffoldSkillCopyBlocked, match="source checkout"):
        copy_scaffold_skills(
            project_dir=tmp_path / "project",
            library_name="ms-markets",
            namespace="ms_markets",
            skills_path=skills_root,
            pinned_version="0.8.1",
            project_guard=lambda _project_dir: "source checkout",
        )


@pytest.mark.parametrize("namespace", ["", "bad/name", "bad name", "../bad"])
def test_normalize_scaffold_skill_namespace_rejects_invalid_names(namespace):
    with pytest.raises(ValueError):
        normalize_scaffold_skill_namespace(namespace)


def test_normalize_scaffold_skill_namespace_derives_package_style_name():
    assert normalize_scaffold_skill_namespace("ms-markets") == "ms_markets"
