from __future__ import annotations

import pathlib
import shutil
import subprocess

import pytest

from mainsequence.cli import local_ops


def _write_uv_fixture_package(path: pathlib.Path, name: str) -> None:
    path.mkdir()
    (path / "pyproject.toml").write_text(
        "\n".join(
            [
                "[project]",
                f'name = "{name}"',
                'version = "1.0.0"',
                'requires-python = ">=3.13"',
                "",
            ]
        ),
        encoding="utf-8",
    )


def test_uv_export_requirements_excludes_dev_group_from_locked_export(monkeypatch, tmp_path):
    uv_binary = shutil.which("uv")
    if uv_binary is None:
        pytest.skip("uv is required for the locked export integration test")

    _write_uv_fixture_package(tmp_path / "runtime-package", "runtime-package")
    _write_uv_fixture_package(tmp_path / "dev-package", "dev-package")
    (tmp_path / "pyproject.toml").write_text(
        """\
[project]
name = "export-fixture"
version = "1.0.0"
requires-python = ">=3.13"
dependencies = ["runtime-package"]

[dependency-groups]
dev = ["dev-package"]

[tool.uv]
package = false

[tool.uv.sources]
runtime-package = { path = "runtime-package" }
dev-package = { path = "dev-package" }
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("UV_OFFLINE", "1")
    subprocess.run(
        [uv_binary, "lock", "--offline"],
        cwd=tmp_path,
        check=True,
        capture_output=True,
        text=True,
    )

    local_ops.uv_export_requirements(pathlib.Path(uv_binary), tmp_path)

    requirements = (tmp_path / "requirements.txt").read_text(encoding="utf-8")
    command_header = next(line for line in requirements.splitlines() if "uv export" in line)
    assert "--locked" in command_header
    assert "--no-dev" in command_header
    assert "--no-hashes" in command_header
    assert "runtime-package" in requirements
    assert "dev-package" not in requirements
