import asyncio
import datetime

import pytest
import requests

import mainsequence.client.models_helpers as models_helpers_mod
import mainsequence.client.release_calls as release_calls_mod
from mainsequence._request_deadline import DEADLINE_HEADER, request_deadline
from mainsequence.client import (
    ReleaseColdStartTimeoutError,
    ReleaseDeadlineTooShortError,
    ReleaseNotDeployedError,
    ReleaseReadinessTimeoutError,
    ReleaseRequestTimeoutError,
    ReleaseUnavailableError,
    ResourceRelease,
    prewarm_releases,
    release_runtime_cache,
)
from mainsequence.client.exceptions import TransportError
from tests.client.support import _runtime_access_payload

RELEASE_UID = "2f4c4c3d-5669-4da5-9d86-b84633c1e6ed"


def _ready(**scaling):
    payload = _runtime_access_payload(state="ready", can_request=True)
    if scaling:
        payload["scaling"] = scaling
    return payload


def _waking(retry_after_ms=0, **scaling):
    payload = _runtime_access_payload(
        state="waking", can_request=False, retry_after_ms=retry_after_ms
    )
    if scaling:
        payload["scaling"] = scaling
    return payload


def _refused(retry_after_ms=None, phase="serving", code="runtime_unavailable", message=None):
    """An ``unavailable`` answer, with or without the platform's retry hint."""
    payload = _runtime_access_payload(
        state="unavailable", can_request=False, retry_after_ms=retry_after_ms
    )
    payload["runtime_access"]["notice"] = {
        "code": code,
        "severity": "info" if retry_after_ms is not None else "warning",
        "title": "Deployed runtime unavailable",
        "message": message or "The deployed runtime is unavailable.",
    }
    payload["runtime_presence"]["phase"] = phase
    return payload


def _evidence_pending(retry_after_ms=1):
    """The platform's answer while it records the runtime's evidence (tdag-django#1309)."""
    return _refused(
        retry_after_ms=retry_after_ms,
        phase="redeploying",
        code="runtime_evidence_pending",
        message="Checking the deployed runtime.",
    )


def _response(status=200, headers=None):
    response = requests.Response()
    response.status_code = status
    response._content = b"{}"
    response.headers.update(headers or {})
    return response


class Platform:
    """The platform's answers and the target's responses, in the order they are asked for."""

    def __init__(self, monkeypatch, answers, responses=()):
        self.answers = list(answers)
        self.responses = list(responses)
        self.asked = 0
        self.sent = []
        monkeypatch.setattr(
            models_helpers_mod.ResourceRelease,
            "resolve_runtime_access",
            lambda release, **kwargs: self._resolve(),
        )
        monkeypatch.setattr(release_calls_mod, "request_within_budget", self._send)

    def _resolve(self):
        self.asked += 1
        answer = self.answers.pop(0) if len(self.answers) > 1 else self.answers[0]
        if isinstance(answer, BaseException):
            raise answer
        return models_helpers_mod.ResourceReleaseRuntimeAccess.model_validate(answer)

    def _send(self, method, url, *, timeout, **kwargs):
        self.sent.append({"method": method, "url": url, "timeout": timeout, **kwargs})
        outcome = self.responses.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


@pytest.fixture(autouse=True)
def empty_cache(monkeypatch):
    release_runtime_cache.clear()
    monkeypatch.delenv(release_calls_mod.READY_TIMEOUT_ENV, raising=False)
    monkeypatch.delenv(release_calls_mod.IDLE_TIMEOUT_ENV, raising=False)
    monkeypatch.setattr(release_calls_mod, "MIN_RETRY_SECONDS", 0.001)
    yield
    release_runtime_cache.clear()


def _release():
    return ResourceRelease(uid=RELEASE_UID, release_kind="fastapi")


def test_request_waits_for_a_starting_release_before_its_timeout_starts(monkeypatch):
    platform = Platform(monkeypatch, [_waking(), _waking(), _ready()], [_response()])

    response = _release().request("POST", "/v1/prices", timeout=60, json={"a": 1})

    assert response.status_code == 200
    assert platform.asked == 3
    (sent,) = platform.sent
    assert sent["method"] == "POST"
    assert sent["url"] == "https://runtime.example.test/v1/prices"
    assert sent["timeout"] == 60
    assert sent["json"] == {"a": 1}
    assert sent["allow_redirects"] is False
    assert sent["headers"]["Authorization"] == "Bearer runtime-token"
    assert sent["headers"][DEADLINE_HEADER] == "60000"
    # The release's address came from the platform: the call is a release span (ADR 0042).
    assert sent["dependency"].fields == {
        "dependency_type": "release",
        "dependency_name": RELEASE_UID,
        "http_method": "POST",
        "route": "/v1/prices",
    }
    status = _release().runtime_status()
    assert status.state == "warm"
    assert status.last_cold_start_seconds is not None
    assert status.last_cold_start_at == datetime.datetime(2026, 9, 17, 12, 0, tzinfo=datetime.UTC)


def test_a_warm_release_is_called_without_asking_the_platform(monkeypatch):
    platform = Platform(monkeypatch, [_ready()], [_response(), _response()])
    release = _release()

    release.request("GET", "/v1/prices", timeout=5)
    release.request("GET", "/v1/prices", timeout=5)

    assert platform.asked == 1
    assert len(platform.sent) == 2


def test_the_platform_is_asked_again_once_the_idle_window_may_have_passed(monkeypatch):
    platform = Platform(monkeypatch, [_ready()], [_response(), _response()])
    release = _release()
    release.request("GET", "/v1/prices", timeout=5)

    # 50 seconds of a 60-second default window, less its 10-second margin, are used up.
    release_runtime_cache._entries[RELEASE_UID].served -= 50

    assert release.runtime_status().state == "unknown"
    release.request("GET", "/v1/prices", timeout=5)
    assert platform.asked == 2


def test_the_platforms_idle_window_and_minimum_instances_decide_warmth(monkeypatch):
    platform = Platform(
        monkeypatch,
        [_ready(min_instances=0, idle_timeout_seconds=600, expected_cold_start_seconds=120)],
        [_response(), _response()],
    )
    release = _release()
    release.request("GET", "/v1/prices", timeout=5)
    release_runtime_cache._entries[RELEASE_UID].served -= 400

    release.request("GET", "/v1/prices", timeout=5)

    assert platform.asked == 1
    status = release.runtime_status()
    assert (status.min_instances, status.idle_timeout_seconds) == (0, 600)
    assert status.expected_cold_start_seconds == 120

    release_runtime_cache.clear()
    platform.answers = [_ready(min_instances=1)]
    platform.responses = [_response(), _response()]
    release.request("GET", "/v1/prices", timeout=5)
    release_runtime_cache._entries[RELEASE_UID].served -= 100_000
    release.request("GET", "/v1/prices", timeout=5)
    assert platform.asked == 2


def test_an_expiring_token_is_not_reused(monkeypatch):
    answer = _ready()
    answer["access"]["expires_at"] = (
        datetime.datetime.now(datetime.UTC) + datetime.timedelta(seconds=20)
    ).isoformat()
    platform = Platform(monkeypatch, [answer], [_response(), _response()])
    release = _release()

    release.request("GET", "/v1/prices", timeout=5)
    release.request("GET", "/v1/prices", timeout=5)

    assert platform.asked == 2


def test_readiness_has_its_own_limit_and_sends_nothing(monkeypatch):
    platform = Platform(monkeypatch, [_waking(retry_after_ms=10)])

    with pytest.raises(ReleaseReadinessTimeoutError) as raised:
        _release().request("GET", "/v1/prices", timeout=60, ready_timeout=0.05)

    error = raised.value
    assert isinstance(error, TimeoutError)
    assert (error.phase, error.timeout_seconds) == ("readiness", 0.05)
    assert (error.runtime_state, error.runtime_phase) == ("waking", "starting")
    assert error.wake_requested_at is not None
    assert platform.sent == []
    assert _release().runtime_status().state == "cold"


def test_the_default_readiness_limit_follows_the_expected_start(monkeypatch):
    assert release_calls_mod._ready_limit(None, None) == 300
    assert release_calls_mod._ready_limit(None, 200) == 400
    assert release_calls_mod._ready_limit(None, 5) == 30
    assert release_calls_mod._ready_limit(12, 200) == 12
    monkeypatch.setenv(release_calls_mod.READY_TIMEOUT_ENV, "45")
    assert release_calls_mod._ready_limit(None, None) == 45


def test_an_unavailable_release_ends_the_call_at_once(monkeypatch):
    platform = Platform(
        monkeypatch, [_runtime_access_payload(state="unavailable", can_request=False)]
    )

    with pytest.raises(ReleaseUnavailableError) as raised:
        _release().request("GET", "/v1/prices", timeout=60)

    assert raised.value.runtime_state == "unavailable"
    assert platform.asked == 1
    assert platform.sent == []


def test_an_answer_with_a_retry_hint_is_asked_again_whatever_its_state(monkeypatch):
    platform = Platform(monkeypatch, [_evidence_pending(), _ready()], [_response()])

    response = _release().request("GET", "/v1/prices", timeout=60)

    assert response.status_code == 200
    assert platform.asked == 2
    assert len(platform.sent) == 1


def test_redeploying_without_a_retry_hint_ends_the_call_at_once(monkeypatch):
    platform = Platform(monkeypatch, [_refused(phase="redeploying")])

    with pytest.raises(ReleaseUnavailableError) as raised:
        _release().request("GET", "/v1/prices", timeout=60)

    assert not isinstance(raised.value, ReleaseNotDeployedError)
    assert (raised.value.runtime_state, raised.value.runtime_phase) == (
        "unavailable",
        "redeploying",
    )
    assert platform.asked == 1
    assert platform.sent == []


def test_a_retry_hint_that_never_ends_stops_at_the_readiness_limit(monkeypatch):
    platform = Platform(monkeypatch, [_evidence_pending(retry_after_ms=10)])

    with pytest.raises(ReleaseReadinessTimeoutError) as raised:
        _release().request("GET", "/v1/prices", timeout=60, ready_timeout=0.05)

    error = raised.value
    assert (error.runtime_state, error.runtime_phase) == ("unavailable", "redeploying")
    assert "Checking the deployed runtime." in str(error)
    assert platform.asked > 1
    assert platform.sent == []


def test_a_release_without_an_active_revision_is_not_deployed(monkeypatch):
    answer = _refused(phase="not_deployed")
    answer["routing"] = {"state": "unavailable", "active_revision_uid": None}
    platform = Platform(monkeypatch, [answer])

    with pytest.raises(ReleaseNotDeployedError) as raised:
        _release().request("GET", "/v1/prices", timeout=60)

    assert isinstance(raised.value, ReleaseUnavailableError)
    assert "not been deployed" in str(raised.value)
    assert platform.asked == 1
    assert platform.sent == []


def test_a_failed_platform_ask_is_made_again(monkeypatch):
    monkeypatch.setattr(release_calls_mod, "DEFAULT_RETRY_SECONDS", 0.001)
    platform = Platform(
        monkeypatch, [TransportError("platform unreachable"), _ready()], [_response()]
    )

    response = _release().request("GET", "/v1/prices", timeout=60)

    assert response.status_code == 200
    assert platform.asked == 2


def test_three_failed_platform_asks_in_a_row_end_the_wait(monkeypatch):
    monkeypatch.setattr(release_calls_mod, "DEFAULT_RETRY_SECONDS", 0.001)
    platform = Platform(monkeypatch, [TransportError("platform unreachable")])

    with pytest.raises(TransportError):
        _release().request("GET", "/v1/prices", timeout=60)

    assert platform.asked == release_calls_mod.MAX_FAILED_PLATFORM_ASKS == 3
    assert platform.sent == []


def test_a_timeout_on_a_serving_release_is_the_applications(monkeypatch):
    platform = Platform(monkeypatch, [_ready()], [requests.Timeout("slow")])

    with pytest.raises(ReleaseRequestTimeoutError) as raised:
        _release().request("POST", "/v1/orders", timeout=7, idempotent=True)

    assert (raised.value.phase, raised.value.timeout_seconds) == ("request", 7)
    assert raised.value.runtime_phase == "serving"
    assert len(platform.sent) == 1


def test_a_timeout_while_the_release_starts_is_a_cold_start(monkeypatch):
    platform = Platform(monkeypatch, [_ready(), _waking(), _ready()], [requests.Timeout("held")])

    with pytest.raises(ReleaseColdStartTimeoutError) as raised:
        _release().request("POST", "/v1/orders", timeout=7)

    assert raised.value.runtime_state == "waking"
    assert len(platform.sent) == 1


def test_a_replica_that_began_serving_after_the_send_held_the_request(monkeypatch):
    later = (datetime.datetime.now(datetime.UTC) + datetime.timedelta(minutes=1)).isoformat()
    platform = Platform(
        monkeypatch, [_ready(), _ready(serving_since=later)], [requests.Timeout("held")]
    )

    with pytest.raises(ReleaseColdStartTimeoutError):
        _release().request("POST", "/v1/orders", timeout=7)

    assert len(platform.sent) == 1


def test_an_idempotent_call_is_sent_once_more_after_a_cold_start(monkeypatch):
    platform = Platform(
        monkeypatch,
        [_ready(), _waking(), _waking(), _ready()],
        [requests.Timeout("held"), _response(201)],
    )

    response = _release().request("PUT", "/v1/orders/1", timeout=7, idempotent=True)

    assert response.status_code == 201
    assert len(platform.sent) == 2


def test_an_idempotent_call_is_never_sent_a_third_time(monkeypatch):
    platform = Platform(
        monkeypatch,
        [_ready(), _waking(), _ready(), _waking()],
        [requests.Timeout("held"), requests.Timeout("held")],
    )

    with pytest.raises(ReleaseColdStartTimeoutError):
        _release().request("PUT", "/v1/orders/1", timeout=7, idempotent=True)

    assert len(platform.sent) == 2


def test_a_refused_kept_token_is_replaced_once(monkeypatch):
    platform = Platform(monkeypatch, [_ready()], [_response(), _response(401), _response(401)])
    release = _release()
    release.request("GET", "/v1/prices", timeout=5)

    response = release.request("GET", "/v1/prices", timeout=5)

    assert response.status_code == 401
    assert platform.asked == 2
    assert len(platform.sent) == 3


def test_a_server_error_does_not_count_as_served(monkeypatch):
    Platform(monkeypatch, [_ready()], [_response(503)])
    release = _release()

    assert release.request("GET", "/v1/prices", timeout=5).status_code == 503
    assert release.runtime_status().last_response_at is None


def test_the_target_reports_when_its_process_started(monkeypatch):
    headers = {
        release_calls_mod.PROCESS_STARTED_AT_HEADER: "2026-11-10T08:00:00Z",
        release_calls_mod.COLD_START_HEADER: "true",
    }
    Platform(monkeypatch, [_ready()], [_response(headers=headers)])
    release = _release()

    release.request("GET", "/v1/prices", timeout=5)

    status = release.runtime_status()
    assert status.process_started_at == datetime.datetime(2026, 11, 10, 8, tzinfo=datetime.UTC)
    assert status.last_response_at is not None


def test_the_callers_remaining_time_bounds_the_request_and_is_sent_on(monkeypatch):
    platform = Platform(monkeypatch, [_ready()], [_response()])

    with request_deadline(5):
        _release().request("GET", "/v1/prices", timeout=60)

    assert 4000 < int(platform.sent[0]["headers"][DEADLINE_HEADER]) <= 5000


def test_a_deadline_shorter_than_the_expected_start_fails_before_sending(monkeypatch):
    platform = Platform(monkeypatch, [_waking(expected_cold_start_seconds=300)])

    with request_deadline(60), pytest.raises(ReleaseDeadlineTooShortError) as raised:
        _release().request("GET", "/v1/prices", timeout=30)

    error = raised.value
    assert error.expected_cold_start_seconds == 300
    assert 59 < error.remaining_seconds <= 60
    assert RELEASE_UID in str(error)
    assert platform.asked == 1
    assert platform.sent == []


def test_a_spent_deadline_sends_nothing(monkeypatch):
    platform = Platform(monkeypatch, [_ready()], [_response()])
    release = _release()
    release.request("GET", "/v1/prices", timeout=5)

    with request_deadline(0.001), pytest.raises(ReleaseDeadlineTooShortError):
        import time

        time.sleep(0.01)
        release.request("GET", "/v1/prices", timeout=5)

    assert len(platform.sent) == 1


@pytest.mark.parametrize("path", ["v1/prices", "//other.example/v1", "https://other.example/"])
def test_only_a_path_on_the_release_is_called(monkeypatch, path):
    platform = Platform(monkeypatch, [_ready()], [_response()])

    with pytest.raises(ValueError):
        _release().request("GET", path, timeout=5)

    assert platform.asked == 0


@pytest.mark.parametrize("name", ["Authorization", "authorization", DEADLINE_HEADER])
def test_the_sdk_sets_the_token_and_the_deadline(monkeypatch, name):
    Platform(monkeypatch, [_ready()], [_response()])

    with pytest.raises(ValueError):
        _release().request("GET", "/v1/prices", timeout=5, headers={name: "x"})


def test_the_cache_never_shows_its_token(monkeypatch):
    Platform(monkeypatch, [_ready()], [_response()])
    _release().request("GET", "/v1/prices", timeout=5)

    assert "runtime-token" not in repr(release_runtime_cache._entries[RELEASE_UID])


def test_prewarm_asks_for_each_release_and_never_raises(monkeypatch):
    asked = []

    def resolve(release, **kwargs):
        asked.append(release.uid)
        if release.uid == "broken":
            raise RuntimeError("no such release")
        return models_helpers_mod.ResourceReleaseRuntimeAccess.model_validate(_waking())

    monkeypatch.setattr(models_helpers_mod.ResourceRelease, "resolve_runtime_access", resolve)

    prewarm_releases([RELEASE_UID, _release(), "broken"], wait=True)

    assert sorted(asked) == [RELEASE_UID, RELEASE_UID, "broken"]
    assert _release().runtime_status().state == "cold"


def test_arequest_runs_the_same_call(monkeypatch):
    platform = Platform(monkeypatch, [_ready()], [_response()])

    response = asyncio.run(_release().arequest("GET", "/v1/prices", timeout=5))

    assert response.status_code == 200
    assert platform.sent[0]["url"] == "https://runtime.example.test/v1/prices"


def test_runtime_status_of_a_release_never_called_is_unknown():
    status = _release().runtime_status()

    assert status.state == "unknown"
    assert status.observed_at is None


def test_a_real_request_reaches_the_release_and_a_hung_one_ends_at_its_timeout(monkeypatch):
    from tests._http_server import answer_json, never_answer, serve

    routes = {
        ("POST", "/v1/prices"): answer_json(200, {"ok": True}),
        ("GET", "/v1/hang"): never_answer(),
    }
    with serve(routes) as (base_url, calls):
        answer = _ready()
        answer["access"]["rpc_url"] = base_url
        monkeypatch.setattr(
            models_helpers_mod.ResourceRelease,
            "resolve_runtime_access",
            lambda release, **kwargs: models_helpers_mod.ResourceReleaseRuntimeAccess.model_validate(
                answer
            ),
        )
        release = _release()

        response = release.request("POST", "/v1/prices", timeout=5, json={"symbol": "A"})
        with pytest.raises(ReleaseRequestTimeoutError):
            release.request("GET", "/v1/hang", timeout=0.2)

    assert response.json() == {"ok": True}
    assert calls[0] == ("POST", "/v1/prices", b'{"symbol": "A"}')
    assert [call[:2] for call in calls] == [("POST", "/v1/prices"), ("GET", "/v1/hang")]


def test_a_release_is_called_by_uid_without_fetching_it(monkeypatch):
    import mainsequence.client.base as base_mod

    monkeypatch.setattr(
        base_mod, "make_request", lambda **kwargs: pytest.fail("by_uid must not fetch")
    )
    platform = Platform(monkeypatch, [_ready()], [_response(), _response()])

    release = ResourceRelease.by_uid(RELEASE_UID)
    release.request("GET", "/v1/prices", timeout=5)
    ResourceRelease.by_uid(RELEASE_UID).request("GET", "/v1/prices", timeout=5)

    assert release.uid == RELEASE_UID
    assert platform.asked == 1
    assert len(platform.sent) == 2
    with pytest.raises(ValueError):
        ResourceRelease.by_uid("")


def test_wait_for_runtime_access_uses_the_same_wait_and_fills_the_cache(monkeypatch):
    platform = Platform(monkeypatch, [_waking(), _ready()], [_response()])
    release = _release()

    access = release.wait_for_runtime_access(wait_timeout_seconds=5)

    assert access.runtime_access.can_request is True
    assert release.runtime_status().state == "warm"
    assert release.runtime_status().last_cold_start_seconds is not None
    release.request("GET", "/v1/prices", timeout=5)
    assert platform.asked == 2


def test_wait_for_runtime_access_keeps_its_exception_types(monkeypatch):
    Platform(monkeypatch, [_waking(retry_after_ms=10)])
    with pytest.raises(TimeoutError) as raised:
        _release().wait_for_runtime_access(wait_timeout_seconds=0.05)
    assert isinstance(raised.value, ReleaseReadinessTimeoutError)

    Platform(monkeypatch, [_runtime_access_payload(state="unavailable", can_request=False)])
    with pytest.raises(RuntimeError) as raised:
        _release().wait_for_runtime_access(wait_timeout_seconds=5)
    assert isinstance(raised.value, ReleaseUnavailableError)


def test_a_static_sites_access_is_never_kept_as_the_releases(monkeypatch):
    asked = []

    def resolve(release, **kwargs):
        asked.append(kwargs)
        return models_helpers_mod.ResourceReleaseRuntimeAccess.model_validate(_ready())

    monkeypatch.setattr(models_helpers_mod.ResourceRelease, "resolve_runtime_access", resolve)
    release = _release()

    release.wait_for_runtime_access(static_site_release_uid="site-1", timeout=7)

    assert asked == [{"static_site_release_uid": "site-1", "timeout": 7}]
    assert release.runtime_status().state == "unknown"


def test_concurrent_first_calls_to_a_cold_release_share_one_wait(monkeypatch):
    import threading
    import time

    asked = []

    def resolve(release, **kwargs):
        asked.append(time.monotonic())
        time.sleep(0.05)
        answer = _waking(retry_after_ms=50) if len(asked) < 3 else _ready()
        return models_helpers_mod.ResourceReleaseRuntimeAccess.model_validate(answer)

    monkeypatch.setattr(models_helpers_mod.ResourceRelease, "resolve_runtime_access", resolve)
    sent = []
    monkeypatch.setattr(
        release_calls_mod,
        "request_within_budget",
        lambda method, url, **kwargs: sent.append(url) or _response(),
    )
    threads = [
        threading.Thread(
            target=_release().request, args=("GET", "/v1/prices"), kwargs={"timeout": 5}
        )
        for _ in range(6)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    assert len(sent) == 6
    # One caller polled through the start; the others used what it learned.
    assert len(asked) == 3


# A request a library's own HTTP client sends (ResourceRelease.call, issue #265)


class _Lines:
    """Stands in for the SDK logger and keeps the dependency-call lines."""

    def __init__(self):
        self.lines = []

    def info(self, event, **fields):
        self.lines.append((event, fields))

    warning = info

    def __getattr__(self, name):
        return lambda *args, **kwargs: None


@pytest.fixture
def lines(monkeypatch):
    from mainsequence import logconf

    recorder = _Lines()
    monkeypatch.setattr(logconf, "logger", recorder)
    return recorder


def test_a_library_call_is_prepared_as_request_prepares_its_own(monkeypatch, lines):
    platform = Platform(monkeypatch, [_waking(), _ready()])

    with _release().call("post", "/v1/prices?symbol=ABC", timeout=60) as call:
        assert call.url == "https://runtime.example.test/v1/prices?symbol=ABC"
        assert call.timeout == 60
        assert call.headers["Authorization"] == "Bearer runtime-token"
        assert call.headers[DEADLINE_HEADER] == "60000"
        _, trace_id, span_id, _ = call.headers["traceparent"].split("-")
        assert call.reused_access is False
        call.record(_response())

    # It waited for the start, and the SDK sent nothing itself.
    assert platform.asked == 2
    assert platform.sent == []
    assert _release().runtime_status().state == "warm"
    [(event, fields)] = [line for line in lines.lines if line[0].startswith("dependency.")]
    assert event == "dependency.call.completed"
    assert (fields["trace_id"], fields["span_id"]) == (trace_id, span_id)
    assert (fields["dependency_type"], fields["dependency_name"]) == ("release", RELEASE_UID)
    assert (fields["http_method"], fields["route"], fields["status_code"]) == (
        "POST",
        "/v1/prices",
        200,
    )


def test_a_library_call_is_bounded_by_the_callers_deadline_and_passes_it_on(monkeypatch):
    Platform(monkeypatch, [_ready()])

    with request_deadline(5):
        with _release().call("GET", "/v1/prices", timeout=60) as call:
            call.record(_response())

    assert 4 < call.timeout <= 5
    assert 4000 < int(call.headers[DEADLINE_HEADER]) <= 5000


def test_an_applications_narrower_deadline_bounds_its_release_calls(monkeypatch):
    from mainsequence.client import remaining_request_seconds
    from mainsequence.client import request_deadline as public_request_deadline

    platform = Platform(monkeypatch, [_ready()], [_response()])
    release = _release()

    with public_request_deadline(5):
        with public_request_deadline(2):
            assert 1 < remaining_request_seconds() <= 2
            with release.call("GET", "/v1/prices", timeout=60) as call:
                call.record(_response())
            release.request("GET", "/v1/prices", timeout=60)
        with public_request_deadline(60):
            assert 4 < remaining_request_seconds() <= 5

    assert 1 < call.timeout <= 2
    assert int(call.headers[DEADLINE_HEADER]) <= 2000
    assert int(platform.sent[0]["headers"][DEADLINE_HEADER]) <= 2000
    assert remaining_request_seconds() is None


def test_a_library_call_with_a_spent_deadline_is_refused_before_sending(monkeypatch):
    Platform(monkeypatch, [_ready()])
    release = _release()
    with release.call("GET", "/v1/prices", timeout=5) as call:
        call.record(_response())

    with request_deadline(0.001), pytest.raises(ReleaseDeadlineTooShortError):
        import time

        time.sleep(0.01)
        with release.call("GET", "/v1/prices", timeout=5):
            pytest.fail("nothing may be sent")


def test_library_calls_reuse_the_warm_access_and_ask_again_after_the_idle_window(monkeypatch):
    platform = Platform(monkeypatch, [_ready()])
    release = _release()

    for _ in range(2):
        with release.call("GET", "/v1/prices", timeout=5) as call:
            call.record(_response())
    assert platform.asked == 1
    assert call.reused_access is True

    # 50 seconds of a 60-second default window, less its 10-second margin, are used up.
    release_runtime_cache._entries[RELEASE_UID].served -= 50
    with release.call("GET", "/v1/prices", timeout=5) as call:
        call.record(_response())
    assert platform.asked == 2
    assert call.reused_access is False


def test_a_refused_kept_token_makes_the_next_library_call_ask_again(monkeypatch):
    platform = Platform(monkeypatch, [_ready()])
    release = _release()
    with release.call("GET", "/v1/prices", timeout=5) as call:
        call.record(_response())

    with release.call("GET", "/v1/prices", timeout=5) as call:
        call.record(_response(status=401))
    assert call.reused_access is True
    assert platform.asked == 1

    with release.call("GET", "/v1/prices", timeout=5) as call:
        call.record(_response())
    assert platform.asked == 2


@pytest.mark.parametrize("ending", ["raises", "records nothing"])
def test_a_library_call_without_an_answer_makes_the_next_one_ask_again(monkeypatch, lines, ending):
    platform = Platform(monkeypatch, [_ready()])
    release = _release()
    with release.call("GET", "/v1/prices", timeout=5) as call:
        call.record(_response())

    if ending == "raises":
        with pytest.raises(requests.Timeout):
            with release.call("GET", "/v1/prices", timeout=5):
                raise requests.Timeout("the library's own client timed out")
    else:
        with release.call("GET", "/v1/prices", timeout=5):
            pass
    assert platform.asked == 1

    with release.call("GET", "/v1/prices", timeout=5) as call:
        call.record(_response())
    assert platform.asked == 2
    assert call.reused_access is False
    failed = [fields for event, fields in lines.lines if event == "dependency.call.failed"]
    assert len(failed) == 1


def test_a_server_error_to_a_library_call_is_no_sign_of_serving(monkeypatch):
    Platform(monkeypatch, [_ready()])
    release = _release()
    with release.call("GET", "/v1/prices", timeout=5) as call:
        call.record(_response())
    answered = release.runtime_status().last_response_at

    with release.call("GET", "/v1/prices", timeout=5) as call:
        call.record(_response(status=503))

    assert release.runtime_status().last_response_at == answered


def test_runtime_status_and_request_see_a_library_s_answers(monkeypatch):
    platform = Platform(monkeypatch, [_ready()], [_response()])
    release = _release()
    started = {release_calls_mod.PROCESS_STARTED_AT_HEADER: "2026-11-10T08:00:00Z"}

    with release.call("GET", "/v1/prices", timeout=5) as call:
        call.record(_response(headers=started))

    status = release.runtime_status()
    assert status.state == "warm"
    assert status.process_started_at == datetime.datetime(2026, 11, 10, 8, tzinfo=datetime.UTC)
    release.request("GET", "/v1/prices", timeout=5)
    assert platform.asked == 1
    assert len(platform.sent) == 1


def test_a_library_call_works_with_async_with_and_keeps_the_deadline(monkeypatch):
    platform = Platform(monkeypatch, [_ready()])

    async def send():
        with request_deadline(5):
            async with _release().call("GET", "/v1/prices", timeout=60) as call:
                call.record(_response())
        return call

    call = asyncio.run(send())

    assert platform.asked == 1
    assert call.url == "https://runtime.example.test/v1/prices"
    assert 4000 < int(call.headers[DEADLINE_HEADER]) <= 5000


def test_a_library_s_own_client_keeps_one_connection_and_receives_the_headers(monkeypatch):
    from tests._http_server import serve_keeping_connections

    with serve_keeping_connections() as (base, seen):
        answer = _ready()
        answer["access"]["rpc_url"] = base + "/"
        Platform(monkeypatch, [answer])
        release = _release()
        with requests.Session() as session:
            for _ in range(2):
                with release.call("GET", "/v1/prices", timeout=5) as call:
                    call.record(session.get(call.url, headers=call.headers, timeout=call.timeout))

    assert len(seen) == 2
    assert seen[0][0] == seen[1][0]  # one connection
    for _, headers in seen:
        assert headers["authorization"] == "Bearer runtime-token"
        assert 0 < int(headers[DEADLINE_HEADER.lower()]) <= 5000
        assert headers["traceparent"].startswith("00-")


@pytest.mark.parametrize(
    ("method", "path", "timeout", "message"),
    [
        ("GET", "v1/prices", 5, "path"),
        ("GET", "//evil.example/x", 5, "path"),
        ("", "/v1/prices", 5, "method"),
        ("GET", "/v1/prices", (5, 30), "timeout"),
        ("GET", "/v1/prices", 0, "timeout"),
    ],
)
def test_a_library_call_refuses_what_request_refuses(method, path, timeout, message):
    with pytest.raises(ValueError, match=message):
        _release().call(method, path, timeout=timeout)


def test_a_library_call_prepares_one_send(monkeypatch):
    from mainsequence.client import ReleaseCall

    Platform(monkeypatch, [_ready()])
    call = _release().call("GET", "/v1/prices", timeout=5)
    assert isinstance(call, ReleaseCall)
    with call:
        call.record(_response())

    with pytest.raises(RuntimeError, match="one send"):
        with call:
            pass
