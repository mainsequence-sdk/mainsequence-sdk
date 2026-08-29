from __future__ import annotations

import importlib
import os
import pathlib
import sys
import types

import pytest
import requests

_RUNTIME_CONTEXT_ENV_NAMES = (
    "MAIN_SEQUENCE_PROJECT_UID",
    "MAIN_SEQUENCE_PROJECT_BRANCH_UID",
    "MAINSEQUENCE_REPOSITORY_BRANCH",
    "MAIN_SEQUENCE_ORGANIZATION_PROJECT_ENVIRONMENT_UID",
)


@pytest.fixture(autouse=True)
def _clear_runtime_context_environment():
    original_modules = {
        name: module
        for name, module in sys.modules.items()
        if name == "mainsequence" or name.startswith("mainsequence.")
    }
    for name in _RUNTIME_CONTEXT_ENV_NAMES:
        os.environ.pop(name, None)
    yield
    for name in _RUNTIME_CONTEXT_ENV_NAMES:
        os.environ.pop(name, None)
    for name in tuple(sys.modules):
        if name == "mainsequence" or name.startswith("mainsequence."):
            sys.modules.pop(name, None)
    sys.modules.update(original_modules)


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


def test_is_running_in_pod_uses_job_run_uid(monkeypatch):
    runtime_flags = _load_mainsequence_submodule("mainsequence.runtime_flags")

    monkeypatch.delenv("JOB_RUN_UID", raising=False)
    assert runtime_flags.is_running_in_pod() is False

    monkeypatch.setenv("JOB_RUN_UID", "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
    assert runtime_flags.is_running_in_pod() is True


def test_logconf_import_skips_job_startup_state_request_outside_pod(monkeypatch):
    monkeypatch.delenv("MAINSEQUENCE_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("MAINSEQUENCE_REFRESH_TOKEN", raising=False)
    monkeypatch.delenv("JOB_RUN_UID", raising=False)
    monkeypatch.setenv("MAIN_SEQUENCE_PROJECT_ID", "123")

    calls: list[tuple[tuple, dict]] = []

    def _fake_get(*args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("requests.get should not be called outside pod runtime")

    monkeypatch.setattr(requests, "get", _fake_get)

    logconf = _load_mainsequence_submodule("mainsequence.logconf")

    assert calls == []
    assert logconf._request_job_startup_state() == {}


def test_logconf_binds_sdk_version(monkeypatch):
    monkeypatch.delenv("MAINSEQUENCE_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("MAINSEQUENCE_REFRESH_TOKEN", raising=False)
    monkeypatch.delenv("JOB_RUN_UID", raising=False)

    logconf = _load_mainsequence_submodule("mainsequence.logconf")

    bound_context = logconf.dump_structlog_bound_logger(logconf.logger)["bound_context"]

    assert bound_context["application_name"] == "ms-sdk"
    assert bound_context["sdk_version"] == logconf._get_sdk_version()


def test_startup_additional_environment_cannot_inject_code_repository_source_identity(monkeypatch):
    logconf = _load_mainsequence_submodule("mainsequence.logconf")

    logconf._apply_additional_environment(
        {
            "additional_environment": {
                "UNRELATED_SETTING": "kept",
                "MAIN_SEQUENCE_PROJECT_UID": "ignored-project",
                "MAIN_SEQUENCE_PROJECT_BRANCH_UID": "ignored-branch",
                "MAINSEQUENCE_REPOSITORY_BRANCH": "ignored-name",
                "MAIN_SEQUENCE_ORGANIZATION_PROJECT_ENVIRONMENT_UID": ("ignored-environment"),
            }
        }
    )

    assert os.environ["UNRELATED_SETTING"] == "kept"
    assert not any(name in os.environ for name in _RUNTIME_CONTEXT_ENV_NAMES)
    monkeypatch.delenv("UNRELATED_SETTING", raising=False)


def test_logconf_import_skips_job_startup_state_request_without_job_run_uid(monkeypatch):
    monkeypatch.delenv("MAINSEQUENCE_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("MAINSEQUENCE_REFRESH_TOKEN", raising=False)
    monkeypatch.delenv("JOB_RUN_UID", raising=False)

    calls: list[tuple[tuple, dict]] = []

    def _fake_get(*args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("requests.get should not be called without JOB_RUN_UID")

    monkeypatch.setattr(requests, "get", _fake_get)

    logconf = _load_mainsequence_submodule("mainsequence.logconf")

    assert calls == []
    assert logconf._request_job_startup_state() == {}


def test_logconf_import_requests_job_run_detail_startup_state(monkeypatch):
    monkeypatch.setenv("MAINSEQUENCE_ACCESS_TOKEN", "access-token")
    monkeypatch.delenv("MAINSEQUENCE_REFRESH_TOKEN", raising=False)
    monkeypatch.setenv("JOB_RUN_UID", "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
    monkeypatch.setenv("COMMAND_ID", "12")
    monkeypatch.setenv("MAINSEQUENCE_ENDPOINT", "https://backend.example")

    captured: list[dict[str, object]] = []

    class _FakeResponse:
        status_code = 200
        text = '{"job_run_uid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}'

        def json(self):
            return {
                "job_run_uid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                "command_id": 12,
            }

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

    logconf = _load_mainsequence_submodule("mainsequence.logconf")

    assert captured
    assert (
        captured[0]["url"]
        == "https://backend.example/api/v1/job-runs/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa/startup-state/"
    )
    assert captured[0]["params"] == {}
    assert captured[0]["headers"]["Authorization"] == "Bearer access-token"
    bindings = logconf._build_backend_bindings(_FakeResponse().json())
    assert bindings["job_run_uid"] == "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    assert "project_id" not in bindings
    assert "data_source_id" not in bindings
