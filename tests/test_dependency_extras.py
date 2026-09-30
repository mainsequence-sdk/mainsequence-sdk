from __future__ import annotations

import pathlib
import sys
import tomllib
import types

import pytest


def _snapshot_mainsequence_modules() -> dict[str, types.ModuleType]:
    return {
        name: module
        for name, module in sys.modules.items()
        if name == "mainsequence" or name.startswith("mainsequence.")
    }


def _reset_mainsequence_modules() -> None:
    for name in tuple(sys.modules):
        if name == "mainsequence" or name.startswith("mainsequence."):
            sys.modules.pop(name, None)


@pytest.fixture(autouse=True)
def _restore_mainsequence_modules_after_import_test():
    snapshot = _snapshot_mainsequence_modules()
    yield
    _reset_mainsequence_modules()
    sys.modules.update(snapshot)


def _seed_mainsequence_packages() -> None:
    repo_root = pathlib.Path(__file__).resolve().parents[1]
    pkg_root = repo_root / "mainsequence"
    client_pkg_root = pkg_root / "client"

    pkg = types.ModuleType("mainsequence")
    pkg.__path__ = [str(pkg_root)]
    sys.modules["mainsequence"] = pkg

    client_pkg = types.ModuleType("mainsequence.client")
    client_pkg.__path__ = [str(client_pkg_root)]
    sys.modules["mainsequence.client"] = client_pkg


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
