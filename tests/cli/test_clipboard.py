from __future__ import annotations

import os
import signal
import sys
import time

import pytest

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="needs a POSIX shell")


def test_wayland_copy_returns_while_wl_copy_keeps_serving(cli_mod, monkeypatch, tmp_path):
    """wl-copy leaves a background process holding its output; the copy must not wait on it."""
    copied = tmp_path / "copied.txt"
    pids = tmp_path / "pids.txt"
    fake = tmp_path / "bin" / "wl-copy"
    fake.parent.mkdir()
    # Like the real wl-copy: read the text, fork a clipboard server that inherits
    # stdout and stderr, and exit.
    fake.write_text(f'#!/bin/sh\ncat >> "{copied}"\nsleep 30 &\necho $! >> "{pids}"\nexit 0\n')
    fake.chmod(0o755)

    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setenv("PATH", f"{fake.parent}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("WAYLAND_DISPLAY", "wayland-0")
    monkeypatch.delenv("DISPLAY", raising=False)
    monkeypatch.delenv("WSL_DISTRO_NAME", raising=False)

    started = time.monotonic()
    try:
        ok = cli_mod._copy_clipboard("ssh-ed25519 AAA test")
        elapsed = time.monotonic() - started
    finally:
        for pid in pids.read_text().split() if pids.exists() else []:
            try:
                os.kill(int(pid), signal.SIGTERM)
            except ProcessLookupError:
                pass

    assert ok is True
    assert elapsed < 5
    # Once for the clipboard, once for the primary selection.
    assert copied.read_text() == "ssh-ed25519 AAA test" * 2
