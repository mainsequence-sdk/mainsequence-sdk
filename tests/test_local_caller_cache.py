"""Local-mode callers reused across requests (#251)."""

import asyncio
import hashlib
import hmac
import logging
import threading
import time
from unittest.mock import Mock
from uuid import uuid4

import jwt
import pytest
import requests
from fastapi import FastAPI
from fastapi.testclient import TestClient

import mainsequence._local_callers as local_callers
from mainsequence._local_callers import LocalCallerCache
from mainsequence.client import User
from mainsequence.server.fastapi import _RequestIdentityMiddleware, install_request_identity

USER = str(uuid4())
OTHER = str(uuid4())
ENDPOINT = "https://platform.example.test"


@pytest.fixture(autouse=True)
def local_mode(monkeypatch):
    for name in (
        "APP_NAME",
        "FASTAPI_PUBLIC_BASE_URL",
        "MAINSEQUENCE_CALLER_AUTH_MODE",
        "MAINSEQUENCE_CALLER_ASSERTION_ISSUER",
        "MAINSEQUENCE_CALLER_ASSERTION_JWKS_URL",
        "FASTAPI_PUBLIC_INGRESS",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("MAINSEQUENCE_ENDPOINT", ENDPOINT)


@pytest.fixture
def clock(monkeypatch):
    """The cache's monotonic clock, moved by the test."""
    now = [1_000.0]
    monkeypatch.setattr(local_callers, "_now", lambda: now[0])
    return now


def answering(*statuses, uid=USER):
    """A fake ``requests.get`` for users/me answering ``statuses`` in turn, then 200."""
    queue = list(statuses)

    def get(url, **kwargs):
        status = queue.pop(0) if queue else 200
        if isinstance(status, BaseException):
            raise status
        response = Mock(status_code=status)
        response.json.return_value = {"uid": uid, "username": "local"}
        return response

    return Mock(side_effect=get)


def app_for(**options):
    app = FastAPI()

    @app.get("/me")
    def me():
        return {"uid": User.get_logged_user().uid}

    install_request_identity(app, **options)
    return app


def bearer(token="local-token"):
    return {"Authorization": "Bearer " + token}


def test_repeated_requests_with_one_token_ask_the_platform_once(monkeypatch):
    get = answering()
    monkeypatch.setattr("mainsequence.server.fastapi.requests.get", get)

    with TestClient(app_for(local_caller_cache_seconds=60)) as client:
        answers = [client.get("/me", headers=bearer()).json() for _ in range(5)]

    assert answers == [{"uid": USER}] * 5
    assert get.call_count == 1
    assert get.call_args.kwargs["headers"] == bearer()


def test_without_a_period_every_request_asks_the_platform(monkeypatch):
    get = answering()
    monkeypatch.setattr("mainsequence.server.fastapi.requests.get", get)

    with TestClient(app_for()) as client:
        for _ in range(3):
            assert client.get("/me", headers=bearer()).json() == {"uid": USER}

    assert get.call_count == 3


def _concurrent_first_requests(monkeypatch, *, count, outcome, period=60, gate=None):
    """``count`` simultaneous first requests with one token.

    Each platform check runs ``gate`` first; by default it ends once every other
    request waits for it.
    """
    runtime = None
    calls = 0

    def all_others_wait():
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if sum(check.waiters for check in runtime.local_callers._checks.values()) == count - 1:
                return
            time.sleep(0.005)

    def get(url, **kwargs):
        nonlocal calls
        calls += 1
        (gate or all_others_wait)()
        if isinstance(outcome, BaseException):
            raise outcome
        response = Mock(status_code=outcome)
        response.json.return_value = {"uid": USER, "username": "local"}
        return response

    monkeypatch.setattr("mainsequence.server.fastapi.requests.get", get)

    async def application(scope, receive, send):
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": User.get_logged_user().uid.encode()})

    runtime = _RequestIdentityMiddleware(
        application,
        mode="local",
        verifier=None,
        public_ingress=(),
        routes=(),
        local_caller_cache_seconds=period,
    )

    async def request():
        sent = []
        scope = {
            "type": "http",
            "method": "GET",
            "path": "/",
            "headers": [(b"authorization", b"Bearer shared-token")],
        }

        async def receive():
            return {"type": "http.request", "body": b""}

        async def send(message):
            sent.append(message)

        await runtime(scope, receive, send)
        return sent[0]["status"], sent[1]["body"]

    async def all_at_once():
        return await asyncio.gather(*(request() for _ in range(count)))

    return asyncio.run(all_at_once()), calls, runtime


def test_concurrent_first_requests_share_one_check(monkeypatch):
    answers, calls, _ = _concurrent_first_requests(monkeypatch, count=6, outcome=200)

    assert calls == 1
    assert answers == [(200, USER.encode())] * 6


def test_without_a_period_concurrent_requests_each_ask_the_platform(monkeypatch):
    together = threading.Barrier(6, timeout=5)

    def all_checks_at_once():
        try:
            together.wait()
        except threading.BrokenBarrierError:
            pass

    answers, calls, _ = _concurrent_first_requests(
        monkeypatch, count=6, outcome=200, period=0, gate=all_checks_at_once
    )

    assert calls == 6
    assert answers == [(200, USER.encode())] * 6


def test_concurrent_first_requests_share_a_failed_check_and_the_next_asks_again(monkeypatch):
    answers, calls, runtime = _concurrent_first_requests(
        monkeypatch, count=6, outcome=requests.Timeout("users/me timed out")
    )

    assert calls == 1
    assert [status for status, _ in answers] == [503] * 6
    assert len(runtime.local_callers) == 0


@pytest.mark.parametrize(
    "refusal, status",
    [
        (401, 401),
        (500, 503),
        (requests.Timeout("timed out"), 503),
        (requests.ConnectionError("unreachable"), 503),
    ],
    ids=["refused", "server-error", "timeout", "network-error"],
)
def test_a_refusal_is_not_kept(monkeypatch, refusal, status):
    get = answering(refusal)
    monkeypatch.setattr("mainsequence.server.fastapi.requests.get", get)

    with TestClient(app_for(local_caller_cache_seconds=60)) as client:
        assert client.get("/me", headers=bearer()).status_code == status
        assert client.get("/me", headers=bearer()).json() == {"uid": USER}
        assert client.get("/me", headers=bearer()).json() == {"uid": USER}

    assert get.call_count == 2


def test_an_answer_expires_after_the_period(monkeypatch, clock):
    get = answering()
    monkeypatch.setattr("mainsequence.server.fastapi.requests.get", get)

    with TestClient(app_for(local_caller_cache_seconds=60)) as client:
        client.get("/me", headers=bearer())
        clock[0] += 59
        client.get("/me", headers=bearer())
        assert get.call_count == 1
        clock[0] += 1
        client.get("/me", headers=bearer())

    assert get.call_count == 2


def test_an_answer_never_outlives_the_token(monkeypatch, clock):
    get = answering()
    monkeypatch.setattr("mainsequence.server.fastapi.requests.get", get)
    key = "k" * 32  # the cache reads exp without verifying the signature
    soon = jwt.encode({"exp": int(time.time()) + 10}, key, algorithm="HS256")
    expired = jwt.encode({"exp": int(time.time()) - 1}, key, algorithm="HS256")

    with TestClient(app_for(local_caller_cache_seconds=60)) as client:
        client.get("/me", headers=bearer(soon))
        clock[0] += 5
        client.get("/me", headers=bearer(soon))
        assert get.call_count == 1
        clock[0] += 6  # past the token's exp, well within the period
        client.get("/me", headers=bearer(soon))
        assert get.call_count == 2
        # A token already past its exp is never kept.
        client.get("/me", headers=bearer(expired))
        client.get("/me", headers=bearer(expired))

    assert get.call_count == 4


def test_each_token_and_endpoint_is_its_own_entry(monkeypatch):
    get = answering()
    monkeypatch.setattr("mainsequence.server.fastapi.requests.get", get)
    app = app_for(local_caller_cache_seconds=60)

    with TestClient(app) as client:
        client.get("/me", headers=bearer("first-token"))
        client.get("/me", headers=bearer("second-token"))
        client.get("/me", headers=bearer("first-token"))
        assert get.call_count == 2
        monkeypatch.setenv("MAINSEQUENCE_ENDPOINT", "https://other.example.test")
        client.get("/me", headers=bearer("first-token"))

    assert get.call_count == 3
    assert get.call_args.args[0] == "https://other.example.test/api/v1/users/me/"


def test_the_identity_header_is_checked_on_every_request(monkeypatch):
    get = answering()
    monkeypatch.setattr("mainsequence.server.fastapi.requests.get", get)

    with TestClient(app_for(local_caller_cache_seconds=60)) as client:
        assert client.get("/me", headers={**bearer(), "X-User-UID": USER}).status_code == 200
        conflicting = client.get("/me", headers={**bearer(), "X-User-UID": OTHER})
        assert client.get("/me", headers=bearer()).status_code == 200

    assert conflicting.status_code == 401
    assert get.call_count == 1


def test_no_token_key_or_hash_is_logged_or_shown(monkeypatch, caplog):
    caplog.set_level(logging.DEBUG)
    token = "secret-local-token"
    monkeypatch.setattr(
        "mainsequence.server.fastapi.requests.get", answering(requests.Timeout("timed out"))
    )
    app = app_for(local_caller_cache_seconds=60)

    with TestClient(app) as client:
        refused = client.get("/me", headers=bearer(token))
        accepted = client.get("/me", headers=bearer(token))
        conflicting = client.get("/me", headers={**bearer(token), "X-User-UID": OTHER})
        cache = app.middleware_stack
        while not isinstance(cache, _RequestIdentityMiddleware):
            cache = cache.app
        cache = cache.local_callers

    digest = hmac.new(cache._hmac_key, token.encode(), hashlib.sha256)
    secrets = (token, cache._hmac_key.hex(), digest.hexdigest())
    shown = [caplog.text, repr(cache), refused.text, accepted.text, conflicting.text]
    assert repr(cache) == "LocalCallerCache(1 callers)"
    assert not [secret for secret in secrets for text in shown if secret in text]


def test_the_cache_keeps_the_newest_callers():
    cache = LocalCallerCache(max_callers=2)
    checks = []

    def check(token):
        checks.append(token)
        return token

    for token in ("a", "b", "c", "a"):
        cache.caller(ENDPOINT, token, lifetime=60, check=lambda token=token: check(token))

    # "a" was the oldest when "c" arrived, so it was asked again.
    assert checks == ["a", "b", "c", "a"]
    assert len(cache) == 2


def _wait_until(condition):
    deadline = time.monotonic() + 5
    while not condition():
        assert time.monotonic() < deadline, "timed out"
        time.sleep(0.005)


def test_a_waiter_gets_the_check_s_failure():
    cache = LocalCallerCache()
    release = threading.Event()
    results = {}

    def failing():
        release.wait(5)
        raise RuntimeError("refused")

    def ask(name, check):
        try:
            results[name] = cache.caller(ENDPOINT, "token", lifetime=60, check=check)
        except RuntimeError as exc:
            results[name] = f"raised {exc}"

    leader = threading.Thread(target=ask, args=("leader", failing))
    leader.start()
    _wait_until(lambda: cache._checks)
    waiter = threading.Thread(target=ask, args=("waiter", Mock(return_value="never")))
    waiter.start()
    _wait_until(lambda: any(check.waiters for check in cache._checks.values()))
    release.set()
    leader.join(5)
    waiter.join(5)

    assert results == {"leader": "raised refused", "waiter": "raised refused"}
    assert len(cache) == 0


@pytest.mark.parametrize("value", [-1, float("inf"), float("nan"), True, "60"])
def test_the_period_must_be_a_non_negative_number(value):
    with pytest.raises(ValueError, match="local_caller_cache_seconds"):
        install_request_identity(FastAPI(), local_caller_cache_seconds=value)
