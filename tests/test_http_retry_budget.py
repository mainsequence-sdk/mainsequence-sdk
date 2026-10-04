import threading
import time
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import Mock, PropertyMock

import pytest
import requests

from mainsequence.client import _transport as transport
from mainsequence.client import utils
from mainsequence.client.client import MainSequenceClient, MainSequenceClientConfig
from mainsequence.client.exceptions import (
    AuthenticationError,
    PermissionDeniedError,
    TransportError,
)
from mainsequence.client.models_helpers import ResourceRelease


def answer(status=200, headers=None):
    response = requests.Response()
    response.status_code = status
    response.headers.update(headers or {})
    response._content = b'{"ok": true}'
    response._content_consumed = True
    return response


@pytest.fixture
def clock(monkeypatch):
    class Clock:
        now = 0.0
        sleeps = []

        def sleep(self, delay):
            self.sleeps.append(delay)
            self.now += delay

    clock = Clock()
    monkeypatch.setattr(transport.time, "monotonic", lambda: clock.now)
    monkeypatch.setattr(transport.time, "sleep", clock.sleep)
    return clock


@pytest.mark.parametrize("error", [requests.ReadTimeout, requests.ConnectionError])
def test_write_transport_failure_is_not_replayed(monkeypatch, error):
    session = Mock(headers=requests.structures.CaseInsensitiveDict())
    request = session.post
    request.side_effect = error("uncertain result")
    monkeypatch.setattr(utils.time, "sleep", lambda _: None)

    response = utils.make_request(session, "POST", "https://example.test/", None, time_out=1)

    assert response.code == "expired"
    assert request.call_count == 1


def test_only_read_only_methods_are_automatically_retried():
    assert utils.DEFAULT_ALLOWED_METHODS == frozenset({"GET", "HEAD", "OPTIONS"})


def test_programming_error_is_not_retried(monkeypatch):
    session = Mock(headers=requests.structures.CaseInsensitiveDict())
    session.get.side_effect = ValueError("invalid request")
    monkeypatch.setattr(utils.time, "sleep", lambda _: None)

    with pytest.raises(ValueError, match="invalid request"):
        utils.make_request(session, "GET", "https://example.test/", None, time_out=1)

    assert session.get.call_count == 1


@pytest.mark.parametrize(
    ("method", "status"),
    [("POST", 429), ("POST", 503), ("PUT", 503), ("PATCH", 503), ("DELETE", 503)],
)
def test_write_status_is_not_retried_by_adapter(monkeypatch, clock, method, status):
    send = Mock(return_value=answer(status, {"Retry-After": "60"}))
    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", send)
    with utils.build_session() as session:
        response = session.request(method, "https://example.test/", timeout=1)

    assert response.status_code == status
    assert send.call_count == 1
    assert clock.sleeps == []


@pytest.mark.parametrize(
    "error", [requests.ReadTimeout, requests.ConnectTimeout, requests.ConnectionError]
)
def test_adapter_never_retries_write_exceptions(monkeypatch, clock, error):
    send = Mock(side_effect=error("failed"))
    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", send)
    with utils.build_session() as session, pytest.raises(error):
        session.post("https://example.test/", timeout=1)

    assert send.call_count == 1
    assert clock.sleeps == []


@pytest.mark.parametrize("method", ["GET", "HEAD", "OPTIONS"])
def test_safe_retries_are_bounded_and_backed_off(monkeypatch, clock, method):
    responses = [answer(503), answer(503), answer(503), answer(200)]
    send = Mock(side_effect=responses)
    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", send)
    with utils.build_session() as session:
        response = session.request(method, "https://example.test/", timeout=10)

    assert response.status_code == 200
    assert send.call_count == 4
    assert clock.sleeps == [0.5, 1.0, 2.0]
    assert [call.kwargs["timeout"].total for call in send.call_args_list] == [10, 9.5, 8.5, 6.5]


@pytest.mark.parametrize("failure", ["status", "read", "connect", "body"])
def test_deadline_caps_all_read_retries(monkeypatch, clock, failure):
    timeouts = []

    def send(self, request, **kwargs):
        timeouts.append(kwargs["timeout"].total)
        clock.now += 0.2
        if failure == "read":
            raise requests.ReadTimeout("slow response")
        if failure == "connect":
            raise requests.ConnectTimeout("slow connection")
        if failure == "body":
            response = Mock(spec=requests.Response)
            type(response).content = PropertyMock(
                side_effect=requests.ConnectionError("body failed")
            )
            return response
        return answer(503)

    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", send)
    with utils.build_session(retries=10, backoff_factor=0.1) as session:
        if failure == "status":
            response = session.get("https://example.test/", timeout=1)
            assert response.status_code == 503
        else:
            with pytest.raises(requests.RequestException):
                session.get("https://example.test/", timeout=1)

    assert timeouts == pytest.approx([1, 0.7, 0.3])
    assert clock.sleeps == [0.1, 0.2]
    assert clock.now == pytest.approx(0.9)


def test_retry_after_cannot_outlive_budget(monkeypatch, clock):
    send = Mock(return_value=answer(429, {"Retry-After": "60"}))
    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", send)
    with utils.build_session() as session:
        assert session.get("https://example.test/", timeout=1).status_code == 429
    assert send.call_count == 1
    assert clock.sleeps == []


def test_retry_after_is_respected_when_it_fits(monkeypatch, clock):
    send = Mock(side_effect=[answer(429, {"Retry-After": "1"}), answer()])
    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", send)
    with utils.build_session() as session:
        assert session.get("https://example.test/", timeout=2).status_code == 200
    assert clock.sleeps == [1]
    assert send.call_args.kwargs["timeout"].total == 1


def test_outer_request_does_not_multiply_adapter_attempts(monkeypatch, clock):
    send = Mock(side_effect=requests.ConnectionError("offline"))
    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", send)
    with utils.build_session(backoff_factor=0.1) as session:
        response = utils.make_request(session, "GET", "https://example.test/", None, time_out=5)
    assert response.code == "expired"
    assert send.call_count == 4
    assert clock.sleeps == [0.1, 0.2, 0.4]


def test_zero_retry_configuration_is_honored(monkeypatch, clock):
    send = Mock(side_effect=requests.ReadTimeout("failed"))
    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", send)
    with utils.build_session(retries=0) as session:
        response = utils.make_request(session, "GET", "https://example.test/", None, time_out=5)
    assert response.code == "expired"
    assert send.call_count == 1


def test_certificate_failure_is_not_retried(monkeypatch, clock):
    send = Mock(side_effect=requests.exceptions.SSLError("untrusted certificate"))
    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", send)
    with utils.build_session() as session, pytest.raises(requests.exceptions.SSLError):
        session.get("https://example.test/", timeout=1)
    assert send.call_count == 1


def test_invalid_retry_after_uses_bounded_backoff(monkeypatch, clock):
    send = Mock(side_effect=[answer(503, {"Retry-After": "invalid"}), answer()])
    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", send)
    with utils.build_session() as session:
        assert session.get("https://example.test/", timeout=1).status_code == 200
    assert clock.sleeps == [0.5]


@pytest.mark.parametrize("timeout", [0, -1, float("inf"), float("nan"), (1, 0)])
def test_timeout_must_be_positive_and_finite(timeout):
    session = Mock(headers=requests.structures.CaseInsensitiveDict())
    with pytest.raises(ValueError, match="positive, finite"):
        utils.make_request(session, "GET", "https://example.test/", None, time_out=timeout)
    session.get.assert_not_called()


def test_nested_budgets_do_not_reset_deadline(clock):
    with transport.request_budget(1) as outer:
        clock.now = 0.2
        with transport.request_budget(10) as inner:
            assert inner.remaining == pytest.approx(0.8)
        assert transport._ACTIVE_BUDGET.get() is outer
    assert transport._ACTIVE_BUDGET.get() is None


def test_tuple_timeout_has_one_connect_plus_read_budget(clock):
    budget = transport.RequestBudget((0.3, 0.7))
    assert budget.remaining == 1
    clock.now = 0.8
    timeout = budget.timeout()
    assert timeout.total == pytest.approx(0.2)
    assert timeout.connect_timeout == pytest.approx(0.2)
    assert timeout.read_timeout == pytest.approx(0.2)
    clock.now = 1
    with pytest.raises(requests.Timeout):
        budget.timeout()


def test_auth_refresh_and_read_replay_share_deadline(monkeypatch, clock):
    session = Mock(headers=requests.structures.CaseInsensitiveDict())
    loaders = Mock()
    timeouts = []

    def refresh(**kwargs):
        clock.now += 0.2
        return {"Authorization": "test"}

    def get(url, **kwargs):
        timeouts.append(kwargs["timeout"].total)
        clock.now += 0.1
        return answer(401 if len(timeouts) == 1 else 200)

    session.get.side_effect = get
    loaders.refresh_headers.side_effect = refresh
    response = utils.make_request(session, "GET", "https://example.test/", loaders, time_out=1)
    assert response.status_code == 200
    assert timeouts == pytest.approx([0.8, 0.5])
    assert [call.kwargs["force"] for call in loaders.refresh_headers.call_args_list] == [
        False,
        True,
    ]


def test_no_dispatch_after_auth_exhausts_budget(clock):
    session = Mock(headers=requests.structures.CaseInsensitiveDict())
    loaders = Mock()

    def refresh(**kwargs):
        clock.now += 1
        return {"Authorization": "test"}

    loaders.refresh_headers.side_effect = refresh
    response = utils.make_request(session, "GET", "https://example.test/", loaders, time_out=1)
    assert response.code == "expired"
    session.get.assert_not_called()


@pytest.mark.parametrize("method", ["POST", "PUT", "PATCH", "DELETE"])
def test_write_401_is_not_replayed(clock, method):
    session = Mock(headers=requests.structures.CaseInsensitiveDict())
    request = getattr(session, method.lower())
    request.return_value = answer(401)
    loaders = Mock()
    loaders.refresh_headers.return_value = {"Authorization": "test"}
    response = utils.make_request(session, method, "https://example.test/", loaders, time_out=1)
    assert response.status_code == 401
    assert request.call_count == 1
    assert loaders.refresh_headers.call_count == 1


@pytest.mark.parametrize("status,error", [(401, AuthenticationError), (403, PermissionDeniedError)])
def test_direct_client_does_not_replay_rejected_writes(status, error):
    session = Mock(headers=requests.structures.CaseInsensitiveDict())
    session.post.return_value = answer(status)
    loaders = Mock()
    loaders.auth_headers = {"Authorization": "test"}
    loaders.refresh_headers.return_value = loaders.auth_headers
    client = MainSequenceClient(MainSequenceClientConfig(), loaders=loaders, session=session)
    with pytest.raises(error):
        client.request("POST", "example/", timeout=1)
    assert session.post.call_count == 1


def test_direct_client_reads_use_shared_transport_budget(monkeypatch, clock):
    send = Mock(side_effect=[answer(503), answer()])
    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", send)
    loaders = Mock()
    loaders.refresh_headers.return_value = {"Authorization": "test"}
    client = MainSequenceClient(MainSequenceClientConfig(), loaders=loaders)
    try:
        assert client.request("GET", "example/", timeout=1) == {"ok": True}
        assert [call.kwargs["timeout"].total for call in send.call_args_list] == [1, 0.5]
    finally:
        client.session.close()


def test_upload_is_sent_once_and_preserves_query_payload(clock):
    session = Mock(
        headers=requests.structures.CaseInsensitiveDict({"Content-Type": "application/json"})
    )
    session.post.return_value = answer()
    payload = {"json": {"name": "upload"}, "params": {"scope": "one"}, "files": {"file": b"data"}}
    response = utils.make_request(
        session, "POST", "https://example.test/", None, payload=payload, time_out=1
    )
    assert response.status_code == 200
    assert session.post.call_count == 1
    kwargs = session.post.call_args.kwargs
    assert kwargs["data"] == payload["json"]
    assert kwargs["files"] == payload["files"]
    assert kwargs["params"] == payload["params"]
    assert kwargs["allow_redirects"] is False
    assert "json" in payload
    assert "Content-Type" not in session.headers


@pytest.mark.parametrize("provider_kind", ["jwt", "runtime"])
def test_credential_renewal_receives_only_remaining_budget(monkeypatch, clock, provider_kind):
    monkeypatch.setenv("MAINSEQUENCE_ACCESS_TOKEN", "old")
    monkeypatch.setenv("MAINSEQUENCE_REFRESH_TOKEN", "refresh")
    response = Mock(status_code=200)
    response.json.return_value = {"access": "fresh", "expires_in": 300}
    http = Mock()
    http.post.return_value = response
    if provider_kind == "jwt":
        provider = utils.JWTAuthProvider(access_token="old", refresh_token="refresh")
    else:
        provider = utils.RuntimeCredentialAuthProvider(
            credential_id="id", credential_secret="secret"
        )
        monkeypatch.setattr(requests, "post", http.post)
    with transport.request_budget(1):
        clock.now = 0.6
        provider.refresh(force=True, session=http)
    assert http.post.call_count == 1
    assert http.post.call_args.kwargs["timeout"].total == pytest.approx(0.4)


def test_reinstalling_adapters_preserves_single_retry_policy():
    with utils.build_session() as session:
        utils._install_retry_adapters_in_place(session, retries=1, backoff_factor=0.1)
        for prefix in ("http://", "https://"):
            adapter = session.get_adapter(prefix)
            assert isinstance(adapter, transport.DeadlineHTTPAdapter)
            assert adapter.retries == 1
            assert adapter.max_retries.total == 0


@contextmanager
def slow_server(delay=0.15, status=200):
    calls = []

    class Handler(BaseHTTPRequestHandler):
        def handle_request(self):
            calls.append(self.command)
            time.sleep(delay)
            try:
                self.send_response(status)
                self.send_header("Content-Length", "2")
                self.end_headers()
                self.wfile.write(b"{}")
            except (BrokenPipeError, ConnectionResetError):
                pass

        do_GET = do_POST = do_PUT = do_PATCH = do_DELETE = handle_request

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01})
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/", calls
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


def test_slow_get_uses_one_total_budget():
    with (
        slow_server(delay=0.04, status=503) as (url, calls),
        utils.build_session(backoff_factor=0) as session,
    ):
        started = time.monotonic()
        response = utils.make_request(session, "GET", url, None, time_out=0.1)
        elapsed = time.monotonic() - started
        assert response.code == "expired"
        assert 1 < len(calls) <= 4
        assert elapsed < 0.2
        time.sleep(0.16)
        assert len(calls) <= 4


def test_release_runtime_access_does_not_amplify_slow_post(monkeypatch):
    release = ResourceRelease(uid="2f4c4c3d-5669-4da5-9d86-b84633c1e6ed", release_kind="fastapi")
    with slow_server() as (url, calls), utils.build_session() as session:
        monkeypatch.setattr(ResourceRelease, "build_session", classmethod(lambda cls: session))
        monkeypatch.setattr(ResourceRelease, "LOADERS", None)
        monkeypatch.setattr(ResourceRelease, "get_detail_url", lambda self: url)
        started = time.monotonic()
        with pytest.raises(TransportError):
            release.resolve_runtime_access(timeout=0.05)
        assert time.monotonic() - started < 0.15
        assert calls == ["POST"]
        time.sleep(0.16)
        assert calls == ["POST"]
