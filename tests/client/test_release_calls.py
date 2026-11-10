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
    ReleaseReadinessTimeoutError,
    ReleaseRequestTimeoutError,
    ReleaseUnavailableError,
    ResourceRelease,
    prewarm_releases,
    release_runtime_cache,
)
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
