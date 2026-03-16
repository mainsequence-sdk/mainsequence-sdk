from __future__ import annotations

import importlib
import pathlib
import sys
import types

import requests


def _load_mainsequence_submodule(module_name: str):
    repo_root = pathlib.Path(__file__).resolve().parents[1]
    pkg_root = repo_root / "mainsequence"

    for name in tuple(sys.modules):
        if name == "mainsequence" or name.startswith("mainsequence."):
            sys.modules.pop(name, None)

    pkg = types.ModuleType("mainsequence")
    pkg.__path__ = [str(pkg_root)]
    sys.modules["mainsequence"] = pkg

    return importlib.import_module(module_name)


def test_is_running_in_pod_uses_mainsequence_token(monkeypatch):
    runtime_flags = _load_mainsequence_submodule("mainsequence.runtime_flags")

    monkeypatch.delenv("MAINSEQUENCE_TOKEN", raising=False)
    assert runtime_flags.is_running_in_pod() is False

    monkeypatch.setenv("MAINSEQUENCE_TOKEN", "token-value")
    assert runtime_flags.is_running_in_pod() is True


def test_logconf_import_skips_job_startup_state_request_outside_pod(monkeypatch):
    monkeypatch.delenv("MAINSEQUENCE_TOKEN", raising=False)
    monkeypatch.delenv("MAINSEQUENCE_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("MAINSEQUENCE_REFRESH_TOKEN", raising=False)

    calls: list[tuple[tuple, dict]] = []

    def _fake_get(*args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("requests.get should not be called outside pod runtime")

    monkeypatch.setattr(requests, "get", _fake_get)

    logconf = _load_mainsequence_submodule("mainsequence.logconf")

    assert calls == []
    assert logconf._request_job_startup_state() == {}
