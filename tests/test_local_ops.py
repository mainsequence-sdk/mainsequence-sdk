from __future__ import annotations

import json
import pathlib
import subprocess

import pytest

from mainsequence.cli import local_ops


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
