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


def test_is_running_in_pod_uses_execution_markers(monkeypatch):
    runtime_flags = _load_mainsequence_submodule("mainsequence.runtime_flags")

    monkeypatch.delenv("COMMAND_ID", raising=False)
    monkeypatch.delenv("JOB_RUN_ID", raising=False)
    assert runtime_flags.is_running_in_pod() is False

    monkeypatch.setenv("COMMAND_ID", "12")
    assert runtime_flags.is_running_in_pod() is True

    monkeypatch.delenv("COMMAND_ID", raising=False)
    monkeypatch.setenv("JOB_RUN_ID", "34")
    assert runtime_flags.is_running_in_pod() is True


def test_logconf_import_skips_job_startup_state_request_outside_pod(monkeypatch):
    monkeypatch.delenv("MAINSEQUENCE_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("MAINSEQUENCE_REFRESH_TOKEN", raising=False)
    monkeypatch.delenv("COMMAND_ID", raising=False)
    monkeypatch.delenv("JOB_RUN_ID", raising=False)
    monkeypatch.setenv("MAIN_SEQUENCE_PROJECT_ID", "123")

    calls: list[tuple[tuple, dict]] = []

    def _fake_get(*args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("requests.get should not be called outside pod runtime")

    monkeypatch.setattr(requests, "get", _fake_get)

    logconf = _load_mainsequence_submodule("mainsequence.logconf")

    assert calls == []
    assert logconf._request_job_startup_state() == {}


def test_logconf_import_skips_job_startup_state_request_without_job_run_id(monkeypatch):
    monkeypatch.delenv("MAINSEQUENCE_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("MAINSEQUENCE_REFRESH_TOKEN", raising=False)
    monkeypatch.setenv("COMMAND_ID", "12")
    monkeypatch.delenv("JOB_RUN_ID", raising=False)

    calls: list[tuple[tuple, dict]] = []

    def _fake_get(*args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("requests.get should not be called without JOB_RUN_ID")

    monkeypatch.setattr(requests, "get", _fake_get)

    logconf = _load_mainsequence_submodule("mainsequence.logconf")

    assert calls == []
    assert logconf._request_job_startup_state() == {}


def test_logconf_import_requests_job_run_detail_startup_state(monkeypatch):
    monkeypatch.setenv("MAINSEQUENCE_ACCESS_TOKEN", "access-token")
    monkeypatch.delenv("MAINSEQUENCE_REFRESH_TOKEN", raising=False)
    monkeypatch.setenv("JOB_RUN_ID", "34")
    monkeypatch.setenv("COMMAND_ID", "12")
    monkeypatch.setenv("TDAG_ENDPOINT", "https://backend.example")

    captured: list[dict[str, object]] = []

    class _FakeResponse:
        status_code = 200
        text = "{\"job_run_id\": 34}"

        def json(self):
            return {"job_run_id": 34, "project_id": 123, "data_source_id": 456}

    def _fake_get(url, *, headers, params, timeout):
        captured.append(
            {
                "url": url,
                "headers": dict(headers),
                "params": params,
                "timeout": timeout,
            }
        )
        return _FakeResponse()

    monkeypatch.setattr(requests, "get", _fake_get)

    _load_mainsequence_submodule("mainsequence.logconf")

    assert captured
    assert captured[0]["url"] == "https://backend.example/orm/api/pods/job-run/34/startup-state/"
    assert captured[0]["params"] == {"command_id": "12"}
    assert captured[0]["headers"]["Authorization"] == "Bearer access-token"
