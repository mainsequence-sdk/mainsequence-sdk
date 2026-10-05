"""Runtime credential exchange: the projected identity token, the secret, and retries."""

from __future__ import annotations

import importlib
import logging
import os
import time

import pytest
import requests
from requests.structures import CaseInsensitiveDict

from tests._support import load_sdk_submodule

pytestmark = pytest.mark.usefixtures("isolated_sdk_imports", "clean_runtime_context_environment")

TOKEN_URL = "https://backend.example/api/v1/runtime-credentials/token/"
ID_ENV = "MAINSEQUENCE_RUNTIME_CREDENTIAL_ID"
SECRET_ENV = "MAINSEQUENCE_RUNTIME_CREDENTIAL_SECRET"
TOKEN_FILE_ENV = "MAINSEQUENCE_RUNTIME_IDENTITY_TOKEN_FILE"
SECRET = "bootstrap-secret-that-must-not-be-sent"
JOB_RUN_UID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"


class _Answer:
    def __init__(self, status_code=200, payload=None, headers=None):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.headers = CaseInsensitiveDict(headers or {})
        self.closed = False

    def json(self):
        return self._payload

    def close(self):
        self.closed = True


def _granted(access="runtime-access"):
    return _Answer(200, {"access": access, "token_type": "Bearer", "expires_in": 900})


class _Exchange:
    """Answer exchange requests from a script, and record them and every wait."""

    def __init__(self, monkeypatch, *answers):
        self.answers = list(answers)
        self.requests = []
        self.sleeps = []
        monkeypatch.setattr(requests, "post", self._post)
        monkeypatch.setattr(time, "sleep", self.sleeps.append)

    def _post(self, url, **kwargs):
        self.requests.append({"url": url, **kwargs})
        if not self.answers:
            raise AssertionError("unexpected runtime credential exchange request")
        return self.answers.pop(0)

    @property
    def bodies(self):
        return [request["json"] for request in self.requests]


@pytest.fixture
def runtime(monkeypatch, tmp_path):
    for name in (
        "MAINSEQUENCE_ACCESS_TOKEN",
        "MAINSEQUENCE_REFRESH_TOKEN",
        "JOB_RUN_UID",
        SECRET_ENV,
        TOKEN_FILE_ENV,
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "runtime_credential")
    monkeypatch.setenv("MAINSEQUENCE_ENDPOINT", "https://backend.example")
    monkeypatch.setenv(ID_ENV, "cred-id")
    return tmp_path


def _use_token_file(monkeypatch, directory, token="projected-token-1"):
    path = directory / "token"
    path.write_text(token, encoding="utf-8")
    monkeypatch.setenv(TOKEN_FILE_ENV, str(path))
    return path


@pytest.mark.parametrize("mode", ["identity_token", "bootstrap_secret"])
def test_each_mode_sends_the_credential_id_and_exactly_one_proof(monkeypatch, runtime, mode):
    if mode == "identity_token":
        _use_token_file(monkeypatch, runtime)
        monkeypatch.setenv(SECRET_ENV, SECRET)
        proof = {"workload_identity_token": "projected-token-1"}
    else:
        monkeypatch.setenv(SECRET_ENV, "cred-secret")
        proof = {"credential_secret": "cred-secret"}
    utils = load_sdk_submodule("mainsequence.client.utils")
    exchange = _Exchange(monkeypatch, _granted())

    headers = utils.RuntimeCredentialAuthProvider().get_headers()

    assert headers["Authorization"] == "Bearer runtime-access"
    assert exchange.bodies == [{"credential_id": "cred-id", **proof}]
    assert exchange.requests[0]["url"] == TOKEN_URL
    assert exchange.requests[0]["headers"] == {"Content-Type": "application/json"}
    assert exchange.requests[0]["allow_redirects"] is False
    assert os.environ["MAINSEQUENCE_ACCESS_TOKEN"] == "runtime-access"


def test_identity_token_mode_never_reads_the_secret(monkeypatch, runtime):
    _use_token_file(monkeypatch, runtime)
    monkeypatch.setenv(SECRET_ENV, SECRET)
    utils = load_sdk_submodule("mainsequence.client.utils")
    exchange = _Exchange(monkeypatch, _granted())
    read = []
    getenv = os.getenv
    monkeypatch.setattr(
        os, "getenv", lambda name, default=None: read.append(name) or getenv(name, default)
    )

    provider = utils.RuntimeCredentialAuthProvider()
    provider.refresh(force=True)

    assert TOKEN_FILE_ENV in read
    assert SECRET_ENV not in read
    assert provider.credential_secret is None
    assert SECRET not in repr(exchange.requests)


def test_identity_token_file_is_read_for_every_exchange_and_not_at_start_up(monkeypatch, runtime):
    token_file = runtime / "token"
    monkeypatch.setenv(TOKEN_FILE_ENV, str(token_file))
    utils = load_sdk_submodule("mainsequence.client.utils")
    exchange = _Exchange(monkeypatch, _granted("access-1"), _granted("access-2"))

    provider = utils.RuntimeCredentialAuthProvider()
    token_file.write_text("projected-token-1", encoding="utf-8")
    provider.refresh(force=True)
    token_file.write_text("projected-token-2", encoding="utf-8")
    provider.refresh(force=True)

    assert [body["workload_identity_token"] for body in exchange.bodies] == [
        "projected-token-1",
        "projected-token-2",
    ]
    assert os.environ["MAINSEQUENCE_ACCESS_TOKEN"] == "access-2"
    assert "projected-token" not in repr(provider)


@pytest.mark.parametrize(
    ("prepare", "reason"),
    [
        (lambda path: None, "does not exist"),
        (lambda path: path.write_text("", encoding="utf-8"), "is empty"),
        (lambda path: path.write_text(" \n", encoding="utf-8"), "is empty"),
        (lambda path: path.mkdir(), "could not be read (IsADirectoryError)"),
        (lambda path: path.write_bytes(b"\xff\xfeprojected"), "does not hold a token"),
    ],
    ids=["missing", "empty", "blank", "unreadable", "not-text"],
)
def test_unusable_identity_token_file_fails_without_falling_back_to_the_secret(
    monkeypatch, runtime, prepare, reason
):
    token_file = runtime / "token"
    prepare(token_file)
    monkeypatch.setenv(TOKEN_FILE_ENV, str(token_file))
    monkeypatch.setenv(SECRET_ENV, SECRET)
    utils = load_sdk_submodule("mainsequence.client.utils")
    exchange = _Exchange(monkeypatch)

    with pytest.raises(utils.AuthError) as raised:
        utils.RuntimeCredentialAuthProvider().get_headers()

    message = str(raised.value)
    assert reason in message
    assert str(token_file) in message
    assert TOKEN_FILE_ENV in message
    assert SECRET not in message
    assert exchange.requests == []
    chained = raised.value.__context__
    while chained is not None:
        assert not isinstance(chained, UnicodeDecodeError)
        chained = chained.__context__


@pytest.mark.parametrize("mode", ["identity_token", "bootstrap_secret"])
@pytest.mark.parametrize("status", [429, 503])
def test_throttled_or_unavailable_exchange_is_retried_honoring_retry_after(
    monkeypatch, runtime, status, mode
):
    if mode == "identity_token":
        _use_token_file(monkeypatch, runtime)
        proof = {"workload_identity_token": "projected-token-1"}
    else:
        monkeypatch.setenv(SECRET_ENV, "cred-secret")
        proof = {"credential_secret": "cred-secret"}
    utils = load_sdk_submodule("mainsequence.client.utils")
    first = _Answer(status, {"detail": "Try again later."}, {"Retry-After": "7"})
    exchange = _Exchange(monkeypatch, first, _Answer(status), _granted())

    utils.RuntimeCredentialAuthProvider().refresh(force=True)

    # Retry-After (7 s) is longer than the first backoff (0.5 s); the second answer
    # has none, so the second backoff (1 s) applies.
    assert exchange.sleeps == [7, 1]
    assert exchange.bodies == [{"credential_id": "cred-id", **proof}] * 3
    assert first.closed is True
    assert os.environ["MAINSEQUENCE_ACCESS_TOKEN"] == "runtime-access"


def test_exchange_retries_are_bounded(monkeypatch, runtime):
    _use_token_file(monkeypatch, runtime)
    utils = load_sdk_submodule("mainsequence.client.utils")
    exchange = _Exchange(
        monkeypatch, *(_Answer(503, headers={"Retry-After": "1"}) for _ in range(8))
    )

    with pytest.raises(utils.AuthError, match="failed with status 503"):
        utils.RuntimeCredentialAuthProvider().refresh(force=True)

    assert len(exchange.requests) == 4
    assert exchange.sleeps == [1, 1, 2]


def test_each_attempt_receives_only_the_remaining_budget(monkeypatch, runtime):
    _use_token_file(monkeypatch, runtime)
    utils = load_sdk_submodule("mainsequence.client.utils")
    transport = importlib.import_module("mainsequence.client._transport")
    exchange = _Exchange(monkeypatch, _Answer(503, headers={"Retry-After": "4"}), _granted())
    now = [100.0]

    def _sleep(delay):
        exchange.sleeps.append(delay)
        now[0] += delay

    monkeypatch.setattr(time, "monotonic", lambda: now[0])
    monkeypatch.setattr(time, "sleep", _sleep)

    with transport.request_budget(10):
        utils.RuntimeCredentialAuthProvider().refresh(force=True)

    assert exchange.sleeps == [4]
    assert [request["timeout"].total for request in exchange.requests] == [10, 6]


def test_retry_after_beyond_the_timeout_budget_ends_the_retries(monkeypatch, runtime):
    _use_token_file(monkeypatch, runtime)
    utils = load_sdk_submodule("mainsequence.client.utils")
    exchange = _Exchange(monkeypatch, _Answer(429, headers={"Retry-After": "3600"}))

    with pytest.raises(utils.AuthError, match="failed with status 429"):
        utils.RuntimeCredentialAuthProvider().refresh(force=True)

    assert len(exchange.requests) == 1
    assert exchange.sleeps == []


@pytest.mark.parametrize("mode", ["identity_token", "bootstrap_secret"])
def test_rejected_exchange_fails_at_once_without_switching_proof(monkeypatch, runtime, mode):
    if mode == "identity_token":
        _use_token_file(monkeypatch, runtime)
        proof = {"workload_identity_token": "projected-token-1"}
    else:
        proof = {"credential_secret": "cred-secret"}
    monkeypatch.setenv(SECRET_ENV, "cred-secret")
    utils = load_sdk_submodule("mainsequence.client.utils")
    rejected = _Answer(
        401,
        {"detail": "Runtime credential exchange denied."},
        {"WWW-Authenticate": 'Bearer realm="runtime-credential-exchange"'},
    )
    exchange = _Exchange(monkeypatch, rejected)

    with pytest.raises(utils.AuthError, match="failed with status 401"):
        utils.RuntimeCredentialAuthProvider().refresh(force=True)

    assert exchange.bodies == [{"credential_id": "cred-id", **proof}]
    assert exchange.sleeps == []


def test_identity_token_stays_out_of_logs_errors_and_the_environment(
    monkeypatch, runtime, capfd, caplog
):
    token = "projected-token-that-stays-in-the-exchange"
    _use_token_file(monkeypatch, runtime, token)
    utils = load_sdk_submodule("mainsequence.client.utils")
    logged = []

    class _Logger:
        def __getattr__(self, level):
            return lambda message, *args, **kwargs: logged.append(
                f"{level} {message} {args!r} {kwargs!r}"
            )

    monkeypatch.setattr(utils, "logger", _Logger())
    caplog.set_level(logging.DEBUG)
    exchange = _Exchange(
        monkeypatch,
        _Answer(503, {"detail": "Verification is unavailable."}, {"Retry-After": "1"}),
        _Answer(401, {"detail": "Runtime credential exchange denied."}),
        _Answer(401, {"detail": "Runtime credential exchange denied."}),
    )

    class _Session:
        headers = CaseInsensitiveDict()

        def get(self, url, **kwargs):
            raise AssertionError("the request must not be sent without an access token")

    loaders = utils.AuthLoaders()
    response = utils.make_request(_Session(), "GET", "https://backend.example/api/v1/x/", loaders)
    with pytest.raises(utils.AuthError) as raised:
        loaders.provider.refresh(force=True)

    assert response.status_code == 401
    assert [body["workload_identity_token"] for body in exchange.bodies] == [token] * 3
    assert any("retrying in 1.0 s" in line for line in logged)
    assert any("Auth error" in line for line in logged)
    captured = capfd.readouterr()
    for text in (
        *logged,
        caplog.text,
        captured.out,
        captured.err,
        response.content.decode("utf-8"),
        str(raised.value),
        repr(raised.value),
        repr(loaders.provider),
        repr(vars(loaders.provider)),
    ):
        assert token not in text
    assert all(token not in value for value in os.environ.values())


def test_runtime_credential_is_configured_by_an_identity_token_file_or_a_secret(
    monkeypatch, runtime
):
    exchange = load_sdk_submodule("mainsequence.runtime_credential_exchange")

    assert exchange.runtime_credential_configured() is False
    monkeypatch.setenv(TOKEN_FILE_ENV, str(runtime / "token"))
    # Presence is enough: the exchange reads the file and reports a missing one.
    assert exchange.runtime_credential_configured() is True
    monkeypatch.delenv(TOKEN_FILE_ENV)
    monkeypatch.setenv(SECRET_ENV, "cred-secret")
    assert exchange.runtime_credential_configured() is True
    monkeypatch.delenv(ID_ENV)
    assert exchange.runtime_credential_configured() is False


def test_logger_start_up_exchange_sends_the_identity_token_and_retries(monkeypatch, runtime):
    _use_token_file(monkeypatch, runtime)
    monkeypatch.setenv(SECRET_ENV, SECRET)
    monkeypatch.setenv("JOB_RUN_UID", JOB_RUN_UID)
    exchange = _Exchange(monkeypatch, _Answer(503, headers={"Retry-After": "1"}), _granted())
    authorizations = []

    def _get(url, **kwargs):
        authorizations.append(kwargs["headers"].get("Authorization"))
        return _Answer(200, {"job_run_uid": JOB_RUN_UID})

    monkeypatch.setattr(requests, "get", _get)

    load_sdk_submodule("mainsequence.logconf")

    proof = {"credential_id": "cred-id", "workload_identity_token": "projected-token-1"}
    assert exchange.bodies == [proof, proof]
    assert exchange.sleeps == [1]
    assert all(request["allow_redirects"] is False for request in exchange.requests)
    assert authorizations == ["Bearer runtime-access"]


def test_logger_start_up_exchange_never_falls_back_to_the_secret(monkeypatch, runtime):
    monkeypatch.setenv(TOKEN_FILE_ENV, str(runtime / "missing-token"))
    monkeypatch.setenv(SECRET_ENV, SECRET)
    monkeypatch.setenv("JOB_RUN_UID", JOB_RUN_UID)
    exchange = _Exchange(monkeypatch)
    monkeypatch.setattr(requests, "get", lambda url, **kwargs: _Answer(401))

    load_sdk_submodule("mainsequence.logconf")

    assert exchange.requests == []
    assert "MAINSEQUENCE_ACCESS_TOKEN" not in os.environ
