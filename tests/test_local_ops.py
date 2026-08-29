from __future__ import annotations

import json
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


def test_uv_preview_patch_version_uses_read_only_json_command(monkeypatch, tmp_path):
    calls = []
    uv_path = pathlib.Path("/tools/uv")
    env = {"UV_CACHE_DIR": "/cache"}

    def fake_run(command, **kwargs):
        calls.append((command, kwargs))
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=json.dumps({"package_name": "demo", "version": "1.2.4"}),
            stderr="",
        )

    monkeypatch.setattr(local_ops.subprocess, "run", fake_run)

    assert local_ops.uv_preview_patch_version(uv_path, tmp_path, env) == "1.2.4"
    assert calls == [
        (
            [
                "/tools/uv",
                "version",
                "--bump",
                "patch",
                "--dry-run",
                "--output-format",
                "json",
            ],
            {
                "cwd": str(tmp_path),
                "env": env,
                "capture_output": True,
                "text": True,
            },
        )
    ]


@pytest.mark.parametrize("stdout", ["not-json", "{}", "[]"])
def test_uv_preview_patch_version_rejects_invalid_output(monkeypatch, tmp_path, stdout):
    monkeypatch.setattr(
        local_ops.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(args[0], 0, stdout=stdout, stderr=""),
    )

    with pytest.raises(RuntimeError, match="preview returned"):
        local_ops.uv_preview_patch_version(pathlib.Path("uv"), tmp_path)
