from __future__ import annotations

import pathlib
import tomllib

from packaging.requirements import Requirement


def test_streamlit_is_absent_from_dependency_contracts() -> None:
    repo_root = pathlib.Path(__file__).resolve().parents[1]
    project = tomllib.loads((repo_root / "pyproject.toml").read_text(encoding="utf-8"))
    declared_dependencies = list(project["project"]["dependencies"])
    for group_dependencies in project.get("dependency-groups", {}).values():
        declared_dependencies.extend(group_dependencies)

    assert not any(
        str(dependency).partition(";")[0].strip().lower().startswith("streamlit")
        for dependency in declared_dependencies
    )
    assert '\nname = "streamlit"\n' not in (repo_root / "uv.lock").read_text(encoding="utf-8")
    assert "streamlit" not in (repo_root / "requirements.txt").read_text(encoding="utf-8").lower()


def test_caller_verification_dependencies_are_required() -> None:
    repo_root = pathlib.Path(__file__).resolve().parents[1]
    project = tomllib.loads((repo_root / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    dependencies = {
        requirement.name.lower(): requirement
        for requirement in map(Requirement, project["dependencies"])
    }

    assert "pyjwt" in dependencies
    assert dependencies["pyjwt"].marker is None
    assert "crypto" in dependencies["pyjwt"].extras
    assert "server" not in project.get("optional-dependencies", {})

    lock = tomllib.loads((repo_root / "uv.lock").read_text(encoding="utf-8"))
    sdk = next(package for package in lock["package"] if package["name"] == "mainsequence")
    jwt_dependency = next(item for item in sdk["dependencies"] if item["name"] == "pyjwt")
    assert "crypto" in jwt_dependency["extra"]
    assert "server" not in sdk.get("optional-dependencies", {})
    assert "server" not in sdk["metadata"].get("provides-extras", [])

    requirements = {
        requirement.name.lower(): requirement
        for line in (repo_root / "requirements.txt").read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith(("#", "-"))
        for requirement in [Requirement(line)]
    }
    for name in ("pyjwt", "cryptography"):
        assert name in requirements
        assert requirements[name].marker is None
