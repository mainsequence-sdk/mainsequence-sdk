from __future__ import annotations

import os

import pytest
import requests

from tests._support import RETIRED_RUNTIME_CONTEXT_ENV_NAMES as _RUNTIME_CONTEXT_ENV_NAMES
from tests._support import load_sdk_submodule as _load_mainsequence_submodule

pytestmark = pytest.mark.usefixtures("isolated_sdk_imports", "clean_runtime_context_environment")


_REMOVED_DOMAIN_TOKEN = "PRO" + "JECT"
_UNSUPPORTED_REPOSITORY_UID_ENV = "MAIN_SEQUENCE_" + _REMOVED_DOMAIN_TOKEN + "_UID"
_UNSUPPORTED_REPOSITORY_BRANCH_UID_ENV = "MAIN_SEQUENCE_" + _REMOVED_DOMAIN_TOKEN + "_BRANCH_UID"
_UNSUPPORTED_REPOSITORY_NUMERIC_ID_ENV = "MAIN_SEQUENCE_" + _REMOVED_DOMAIN_TOKEN + "_ID"
_UNSUPPORTED_ENVIRONMENT_UID_ENV = (
    "MAIN_SEQUENCE_ORGANIZATION_" + _REMOVED_DOMAIN_TOKEN + "_ENVIRONMENT_UID"
)


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
    monkeypatch.setenv(_UNSUPPORTED_REPOSITORY_NUMERIC_ID_ENV, "123")

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
                _UNSUPPORTED_REPOSITORY_UID_ENV: "ignored-project",
                _UNSUPPORTED_REPOSITORY_BRANCH_UID_ENV: "ignored-branch",
                "MAINSEQUENCE_REPOSITORY_BRANCH": "ignored-name",
                _UNSUPPORTED_ENVIRONMENT_UID_ENV: ("ignored-environment"),
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
