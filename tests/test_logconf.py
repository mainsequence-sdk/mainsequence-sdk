from __future__ import annotations

import os
import sys
import time

import pytest
import requests
from urllib3.util import Timeout

from tests._http_server import answer_after, answer_json, answer_sequence, never_answer, serve
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
        raise AssertionError("the startup state should not be read outside pod runtime")

    monkeypatch.setattr(requests.Session, "get", lambda _s, *a, **kw: _fake_get(*a, **kw))

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
        raise AssertionError("the startup state should not be read without JOB_RUN_UID")

    monkeypatch.setattr(requests.Session, "get", lambda _s, *a, **kw: _fake_get(*a, **kw))

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

    monkeypatch.setattr(requests.Session, "get", lambda _s, *a, **kw: _fake_get(*a, **kw))

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


# The Job startup-state read (#215) crosses the real HTTP boundary with a local
# server. A short stand-in for the SDK's default request budget keeps it fast.

JOB_RUN_UID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
STARTUP_STATE_PATH = f"/api/v1/job-runs/{JOB_RUN_UID}/startup-state/"
REFRESH_PATH = "/auth/jwt-token/token/refresh/"


@pytest.fixture
def job_logconf(monkeypatch):
    """Load logconf outside a Job, then return it with the Job's environment set."""
    for name in ("MAINSEQUENCE_ACCESS_TOKEN", "MAINSEQUENCE_REFRESH_TOKEN", "JOB_RUN_UID"):
        monkeypatch.delenv(name, raising=False)
    logconf = _load_mainsequence_submodule("mainsequence.logconf")
    transport = sys.modules["mainsequence._transport"]

    def start(base_url, *, auth_mode="session_jwt", default_timeout=(0.2, 0.3)):
        monkeypatch.setattr(transport, "DEFAULT_TIMEOUT", default_timeout)
        monkeypatch.setenv("MAINSEQUENCE_ENDPOINT", base_url.rstrip("/"))
        monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", auth_mode)
        monkeypatch.setenv("MAINSEQUENCE_ACCESS_TOKEN", "secret-access-token")
        monkeypatch.setenv("JOB_RUN_UID", JOB_RUN_UID)
        return logconf

    return start


def _startup_gets(calls):
    return [call for call in calls if call[:2] == ("GET", STARTUP_STATE_PATH)]


def test_logger_builds_when_the_startup_state_read_times_out(job_logconf, capsys):
    with serve({("GET", STARTUP_STATE_PATH): never_answer()}) as (url, calls):
        logconf = job_logconf(url)
        started = time.monotonic()
        logger = logconf.build_application_logger()
        elapsed = time.monotonic() - started

    assert elapsed < 0.5 + 0.3
    assert len(_startup_gets(calls)) == 1
    bound = logconf.dump_structlog_bound_logger(logger)["bound_context"]
    assert "job_run_uid" not in bound
    err = capsys.readouterr().err
    assert "the Job startup state could not be read (ReadTimeout)" in err
    assert "secret-access-token" not in err


def test_logger_builds_when_the_backend_cannot_be_reached(job_logconf, capsys):
    with serve({}) as (url, _calls):
        pass  # the port is closed again once the server stops
    logconf = job_logconf(url)

    logger = logconf.build_application_logger()

    assert logger is not None
    assert "could not be read (ConnectionError)" in capsys.readouterr().err


def test_startup_state_read_retries_a_failed_safe_read_within_its_budget(job_logconf):
    state = {"job_run_uid": JOB_RUN_UID, "command_id": 12}
    route = answer_sequence(
        answer_after(1.0, 200, {}), answer_json(503, {}), answer_json(200, state)
    )
    with serve({("GET", STARTUP_STATE_PATH): route}) as (url, calls):
        logconf = job_logconf(url, default_timeout=Timeout(connect=0.3, read=0.3, total=3.0))
        logger = logconf.build_application_logger()

    assert len(_startup_gets(calls)) == 3
    bound = logconf.dump_structlog_bound_logger(logger)["bound_context"]
    assert bound["job_run_uid"] == JOB_RUN_UID
    assert bound["command_id"] == 12


def test_denied_startup_state_is_not_treated_as_granted(job_logconf, capsys):
    route = answer_json(403, {"detail": "denied"})
    with serve({("GET", STARTUP_STATE_PATH): route}) as (url, calls):
        logconf = job_logconf(url)
        assert logconf._request_job_startup_state() == {}

    assert len(_startup_gets(calls)) == 1
    assert capsys.readouterr().err == ""


def test_failed_token_renewal_after_a_denial_shares_the_budget(job_logconf, monkeypatch):
    routes = {
        ("GET", STARTUP_STATE_PATH): answer_json(401, {}),
        ("POST", REFRESH_PATH): never_answer(),
    }
    with serve(routes) as (url, calls):
        logconf = job_logconf(url, auth_mode="jwt")
        monkeypatch.setenv("MAINSEQUENCE_REFRESH_TOKEN", "secret-refresh-token")
        started = time.monotonic()
        assert logconf._request_job_startup_state() == {}
        elapsed = time.monotonic() - started

    assert elapsed < 0.5 + 0.3
    assert [call[:2] for call in calls] == [("GET", STARTUP_STATE_PATH), ("POST", REFRESH_PATH)]


def test_refreshing_logger_bindings_honours_an_explicit_total_timeout(job_logconf, capsys):
    with serve({("GET", STARTUP_STATE_PATH): never_answer()}) as (url, _calls):
        logconf = job_logconf(url, default_timeout=(5.0, 120.0))
        started = time.monotonic()
        logger = logconf.refresh_application_logger_bindings(timeout_s=0.3)
        elapsed = time.monotonic() - started

    assert logger is not None
    assert elapsed < 0.3 + 0.3
    assert "could not be read (ReadTimeout)" in capsys.readouterr().err
