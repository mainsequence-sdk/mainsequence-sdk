"""Workload identity: a subject token from the program's cloud identity, exchanged per RFC 8693."""

from __future__ import annotations

import json
import logging
import os
import time
from urllib.parse import parse_qs, urlsplit

import pytest
import requests
from requests.structures import CaseInsensitiveDict

from tests._http_server import answer_sequence, serve
from tests._support import load_sdk_submodule

pytestmark = pytest.mark.usefixtures("isolated_sdk_imports", "clean_runtime_context_environment")

EXCHANGE_PATH = "/api/v1/workload-identity/token/"
SUBJECT_TOKEN = "subject-token-from-the-cloud-identity"
GRANT_TYPE = "urn:ietf:params:oauth:grant-type:token-exchange"
JWT_TOKEN_TYPE = "urn:ietf:params:oauth:token-type:jwt"
WORKLOAD_ENV = (
    "MAINSEQUENCE_WORKLOAD_IDENTITY_SOURCE",
    "MAINSEQUENCE_WORKLOAD_IDENTITY_TOKEN",
    "MAINSEQUENCE_WORKLOAD_IDENTITY_TOKEN_FILE",
    "MAINSEQUENCE_WORKLOAD_IDENTITY_AUDIENCE",
    "MAINSEQUENCE_WORKLOAD_IDENTITY_AZURE_CLIENT_ID",
    "IDENTITY_ENDPOINT",
    "IDENTITY_HEADER",
)


def _answer(status, payload=None, headers=None):
    """Answer at once with ``payload`` as JSON and the given headers."""
    data = json.dumps(payload if payload is not None else {}).encode("utf-8")

    def route(handler, stop):
        handler.send_response(status)
        handler.send_header("Content-Type", "application/json")
        handler.send_header("Content-Length", str(len(data)))
        for name, value in (headers or {}).items():
            handler.send_header(name, value)
        handler.end_headers()
        handler.wfile.write(data)

    return route


def _granted(access="platform-access-1", expires_in=900):
    return _answer(
        200,
        {
            "access_token": access,
            "issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
            "token_type": "Bearer",
            "expires_in": expires_in,
        },
    )


def _form(body: bytes) -> dict[str, list[str]]:
    return parse_qs(body.decode("utf-8"), strict_parsing=True)


@pytest.fixture
def workload(monkeypatch):
    for name in (
        "MAINSEQUENCE_ACCESS_TOKEN",
        "MAINSEQUENCE_REFRESH_TOKEN",
        "JOB_RUN_UID",
        *WORKLOAD_ENV,
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "workload_identity")
    monkeypatch.setenv("MAINSEQUENCE_ENDPOINT", "https://backend.example")
    monkeypatch.setenv("MAINSEQUENCE_WORKLOAD_IDENTITY_SOURCE", "environment")
    monkeypatch.setenv("MAINSEQUENCE_WORKLOAD_IDENTITY_TOKEN", SUBJECT_TOKEN)
    return load_sdk_submodule("mainsequence.client.utils")


def _provider(utils, base_url, **kwargs):
    return utils.WorkloadIdentityAuthProvider(
        token_url=base_url.rstrip("/") + EXCHANGE_PATH, **kwargs
    )


# ---- The exchange ----


def test_exchange_is_an_rfc_8693_token_exchange_and_its_access_token_is_the_bearer(workload):
    utils = workload
    with serve({("POST", EXCHANGE_PATH): _granted()}) as (base_url, calls):
        headers = _provider(utils, base_url).get_headers()

    assert headers["Authorization"] == "Bearer platform-access-1"
    assert [(method, path) for method, path, _ in calls] == [("POST", EXCHANGE_PATH)]
    assert _form(calls[0][2]) == {
        "grant_type": [GRANT_TYPE],
        "subject_token": [SUBJECT_TOKEN],
        "subject_token_type": [JWT_TOKEN_TYPE],
    }


def test_default_token_url_is_the_workload_identity_route(workload, monkeypatch):
    utils = workload
    monkeypatch.setattr(utils, "API_ENDPOINT", "https://backend.example/api/v1")

    provider = utils.WorkloadIdentityAuthProvider()

    assert provider.token_url == "https://backend.example/api/v1/workload-identity/token/"


def test_access_token_is_reused_and_exchanged_again_before_it_expires(workload, monkeypatch):
    utils = workload
    now = [1_000_000.0]
    monkeypatch.setattr(time, "time", lambda: now[0])
    routes = {
        ("POST", EXCHANGE_PATH): answer_sequence(
            _granted("platform-access-1"), _granted("platform-access-2")
        )
    }
    with serve(routes) as (base_url, calls):
        provider = _provider(utils, base_url)
        first = provider.get_headers()["Authorization"]
        now[0] += 900 - provider.refresh_skew_seconds - 1
        still = provider.get_headers()["Authorization"]
        exchanges_before_expiry = len(calls)
        now[0] += 1
        renewed = provider.get_headers()["Authorization"]

    assert first == still == "Bearer platform-access-1"
    assert exchanges_before_expiry == 1
    assert renewed == "Bearer platform-access-2"
    assert len(calls) == 2


def test_each_exchange_asks_the_source_for_a_subject_token(workload):
    utils = workload
    issued = iter(["subject-1", "subject-2"])
    routes = {("POST", EXCHANGE_PATH): answer_sequence(_granted("a"), _granted("b"))}
    with serve(routes) as (base_url, calls):
        provider = _provider(utils, base_url, subject_token_source=lambda: next(issued))
        provider.refresh(force=True)
        provider.refresh(force=True)

    assert [_form(body)["subject_token"] for _, _, body in calls] == [["subject-1"], ["subject-2"]]


def test_access_token_is_not_written_to_the_process_environment(workload):
    utils = workload
    with serve({("POST", EXCHANGE_PATH): _granted("platform-access-kept-in-memory")}) as (
        base_url,
        _,
    ):
        _provider(utils, base_url).get_headers()

    assert "MAINSEQUENCE_ACCESS_TOKEN" not in os.environ
    assert all("platform-access-kept-in-memory" not in value for value in os.environ.values())


def test_refused_exchange_fails_at_once_without_another_proof(workload):
    utils = workload
    asked = []

    def source():
        asked.append(1)
        return SUBJECT_TOKEN

    refused = _answer(
        401,
        {"detail": "Subject token not accepted."},
        {"WWW-Authenticate": 'Bearer realm="workload-identity-exchange"'},
    )
    with serve({("POST", EXCHANGE_PATH): refused}) as (base_url, calls):
        provider = _provider(utils, base_url, subject_token_source=source)
        with pytest.raises(utils.AuthError, match=r"HTTP 401"):
            provider.get_headers()

    assert len(calls) == 1
    assert asked == [1]


def test_unexpected_exchange_answer_reports_its_status(workload):
    utils = workload
    with serve({("POST", EXCHANGE_PATH): _answer(400, {"detail": "bad"})}) as (base_url, _):
        with pytest.raises(utils.AuthError, match="failed with status 400"):
            _provider(utils, base_url).refresh(force=True)


def test_exchange_answer_without_access_token_is_an_error(workload):
    utils = workload
    with serve({("POST", EXCHANGE_PATH): _answer(200, {"token_type": "Bearer"})}) as (
        base_url,
        _,
    ):
        with pytest.raises(utils.AuthError, match="did not include access_token"):
            _provider(utils, base_url).refresh(force=True)


# ---- Retries ----


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


def _scripted_granted():
    return _Answer(200, {"access_token": "platform-access", "expires_in": 900})


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
            raise AssertionError("unexpected workload identity exchange request")
        return self.answers.pop(0)


@pytest.mark.parametrize("status", [429, 503])
def test_throttled_or_unavailable_exchange_is_retried_honoring_retry_after(
    workload, monkeypatch, status
):
    utils = workload
    first = _Answer(status, {"detail": "Try again later."}, {"Retry-After": "7"})
    exchange = _Exchange(monkeypatch, first, _Answer(status), _scripted_granted())
    asked = []

    def source():
        asked.append(1)
        return SUBJECT_TOKEN

    provider = utils.WorkloadIdentityAuthProvider(subject_token_source=source)
    provider.refresh(force=True)

    # Retry-After (7 s) is longer than the first backoff (0.5 s); the second answer
    # has none, so the second backoff (1 s) applies.
    assert exchange.sleeps == [7, 1]
    assert [request["data"]["subject_token"] for request in exchange.requests] == [
        SUBJECT_TOKEN
    ] * 3
    assert asked == [1]
    assert first.closed is True
    assert provider.get_headers()["Authorization"] == "Bearer platform-access"


def test_exchange_retries_are_bounded(workload, monkeypatch):
    utils = workload
    exchange = _Exchange(
        monkeypatch, *(_Answer(503, headers={"Retry-After": "1"}) for _ in range(8))
    )

    with pytest.raises(utils.AuthError, match="failed with status 503"):
        utils.WorkloadIdentityAuthProvider().refresh(force=True)

    assert len(exchange.requests) == 4
    assert exchange.sleeps == [1, 1, 2]


def test_retry_after_beyond_the_timeout_budget_ends_the_retries(workload, monkeypatch):
    utils = workload
    exchange = _Exchange(monkeypatch, _Answer(429, headers={"Retry-After": "3600"}))

    with pytest.raises(utils.AuthError, match="failed with status 429"):
        utils.WorkloadIdentityAuthProvider().refresh(force=True)

    assert len(exchange.requests) == 1
    assert exchange.sleeps == []


def test_refused_exchange_is_not_retried(workload, monkeypatch):
    utils = workload
    exchange = _Exchange(monkeypatch, _Answer(401, {"detail": "Subject token not accepted."}))

    with pytest.raises(utils.AuthError, match="HTTP 401"):
        utils.WorkloadIdentityAuthProvider().refresh(force=True)

    assert len(exchange.requests) == 1
    assert exchange.sleeps == []


# ---- Requests through the shared client ----


def test_read_rejected_with_401_exchanges_again_and_replays_once(workload):
    utils = workload
    routes = {
        ("POST", EXCHANGE_PATH): answer_sequence(
            _granted("platform-access-1"), _granted("platform-access-2")
        ),
        ("GET", "/api/v1/things/"): answer_sequence(
            _answer(401, {"detail": "Credential disabled."}), _answer(200, {"ok": True})
        ),
    }
    with serve(routes) as (base_url, calls):
        loaders = utils.AuthLoaders(provider=_provider(utils, base_url))
        session = utils.build_session(loaders=loaders)
        response = utils.make_request(
            session, "GET", base_url.rstrip("/") + "/api/v1/things/", loaders
        )

    assert response.status_code == 200
    assert [(method, path) for method, path, _ in calls] == [
        ("POST", EXCHANGE_PATH),
        ("GET", "/api/v1/things/"),
        ("POST", EXCHANGE_PATH),
        ("GET", "/api/v1/things/"),
    ]
    assert session.headers["Authorization"] == "Bearer platform-access-2"


def test_subject_token_and_access_token_stay_out_of_logs_errors_and_the_environment(
    workload, monkeypatch, capfd, caplog
):
    utils = workload
    logged = []

    class _Logger:
        def __getattr__(self, level):
            return lambda message, *args, **kwargs: logged.append(
                f"{level} {message} {args!r} {kwargs!r}"
            )

    monkeypatch.setattr(utils, "logger", _Logger())
    caplog.set_level(logging.DEBUG)
    # A custom source: the environment source holds its token in the environment by design.
    monkeypatch.delenv("MAINSEQUENCE_WORKLOAD_IDENTITY_TOKEN")
    access = "platform-access-that-must-not-be-logged"
    routes = {
        ("POST", EXCHANGE_PATH): answer_sequence(
            _answer(503, {"detail": "Keys unavailable."}, {"Retry-After": "0"}),
            _granted(access),
            _answer(401, {"detail": "Subject token not accepted."}),
        ),
        ("GET", "/api/v1/things/"): _answer(401, {"detail": "Credential disabled."}),
    }
    with serve(routes) as (base_url, _):
        provider = _provider(utils, base_url, subject_token_source=lambda: SUBJECT_TOKEN)
        loaders = utils.AuthLoaders(provider=provider)
        session = utils.build_session(loaders=loaders)
        response = utils.make_request(
            session, "GET", base_url.rstrip("/") + "/api/v1/things/", loaders
        )

    assert response.status_code == 401
    assert any("retrying in" in line for line in logged)
    assert any("Auth error" in line for line in logged)
    captured = capfd.readouterr()
    for text in (
        *logged,
        caplog.text,
        captured.out,
        captured.err,
        response.content.decode("utf-8"),
        repr(loaders.provider),
        str(loaders.provider),
    ):
        assert SUBJECT_TOKEN not in text
        assert access not in text
    assert all(SUBJECT_TOKEN not in value for value in os.environ.values())
    assert all(access not in value for value in os.environ.values())


# ---- Mode selection ----


def test_mode_selects_workload_identity_over_tokens_in_the_environment(workload, monkeypatch):
    utils = workload
    monkeypatch.setenv("MAINSEQUENCE_ACCESS_TOKEN", "a-persons-access")
    monkeypatch.setenv("MAINSEQUENCE_REFRESH_TOKEN", "a-persons-refresh")

    assert isinstance(utils.build_default_auth_provider(), utils.WorkloadIdentityAuthProvider)
    assert isinstance(utils.AuthLoaders()._provider(), utils.WorkloadIdentityAuthProvider)


def test_jwt_stays_the_default_mode(workload, monkeypatch):
    utils = workload
    monkeypatch.delenv("MAINSEQUENCE_AUTH_MODE")
    monkeypatch.setenv("MAINSEQUENCE_ACCESS_TOKEN", "a-persons-access")

    assert isinstance(utils.AuthLoaders()._provider(), utils.JWTAuthProvider)


def test_use_workload_identity_installs_a_custom_source(workload):
    utils = workload
    loaders = utils.AuthLoaders()
    source = lambda: "custom-subject-token"  # noqa: E731

    loaders.use_workload_identity(source)

    assert loaders._provider().subject_token_source is source


def test_use_workload_identity_requires_the_mode(workload, monkeypatch):
    utils = workload
    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "jwt")

    with pytest.raises(utils.AuthError, match="MAINSEQUENCE_AUTH_MODE=workload_identity"):
        utils.AuthLoaders().use_workload_identity(lambda: SUBJECT_TOKEN)


def test_saved_cli_session_is_not_loaded_into_a_workload(monkeypatch, tmp_path):
    from mainsequence.bootstrap import prime_runtime_env
    from mainsequence.cli import config

    monkeypatch.chdir(tmp_path)
    for key in ("MAINSEQUENCE_ENDPOINT", "MAINSEQUENCE_ACCESS_TOKEN", "MAINSEQUENCE_REFRESH_TOKEN"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "workload_identity")
    monkeypatch.setattr(config, "backend_url", lambda: "https://saved.example.test")
    monkeypatch.setattr(
        config, "get_tokens", lambda: {"access": "saved-access", "refresh": "saved-refresh"}
    )

    prime_runtime_env()

    assert os.environ["MAINSEQUENCE_ENDPOINT"] == "https://saved.example.test"
    assert "MAINSEQUENCE_ACCESS_TOKEN" not in os.environ
    assert "MAINSEQUENCE_REFRESH_TOKEN" not in os.environ


# ---- Subject token sources ----


@pytest.fixture
def sources(workload):
    return load_sdk_submodule("mainsequence.workload_identity")


def test_environment_source_reads_the_variable_for_every_exchange(sources, monkeypatch):
    source = sources.subject_token_source_from_environment()
    first = source()
    monkeypatch.setenv("MAINSEQUENCE_WORKLOAD_IDENTITY_TOKEN", " rotated-token \n")

    assert (first, source()) == (SUBJECT_TOKEN, "rotated-token")


@pytest.mark.parametrize("value", [None, "  "])
def test_environment_source_without_a_token_names_the_variable(sources, monkeypatch, value):
    if value is None:
        monkeypatch.delenv("MAINSEQUENCE_WORKLOAD_IDENTITY_TOKEN")
    else:
        monkeypatch.setenv("MAINSEQUENCE_WORKLOAD_IDENTITY_TOKEN", value)

    with pytest.raises(sources.SubjectTokenError, match="MAINSEQUENCE_WORKLOAD_IDENTITY_TOKEN"):
        sources.subject_token_source_from_environment()()


def test_file_source_reads_the_file_for_every_exchange(sources, monkeypatch, tmp_path):
    path = tmp_path / "token"
    path.write_text("file-token-1\n", encoding="utf-8")
    monkeypatch.setenv("MAINSEQUENCE_WORKLOAD_IDENTITY_SOURCE", "file")
    monkeypatch.setenv("MAINSEQUENCE_WORKLOAD_IDENTITY_TOKEN_FILE", str(path))
    source = sources.subject_token_source_from_environment()

    first = source()
    path.write_text("file-token-2", encoding="utf-8")

    assert (first, source()) == ("file-token-1", "file-token-2")


@pytest.mark.parametrize(
    ("content", "message"),
    [(None, "does not exist"), (b"", "is empty"), (b"\xff\xfe", "does not hold a token")],
)
def test_unusable_token_file_is_an_error_without_its_content(
    sources, monkeypatch, tmp_path, content, message
):
    path = tmp_path / "token"
    if content is not None:
        path.write_bytes(content)
    monkeypatch.setenv("MAINSEQUENCE_WORKLOAD_IDENTITY_SOURCE", "file")
    monkeypatch.setenv("MAINSEQUENCE_WORKLOAD_IDENTITY_TOKEN_FILE", str(path))

    with pytest.raises(sources.SubjectTokenError, match=message):
        sources.subject_token_source_from_environment()()


@pytest.mark.parametrize(
    ("settings", "message"),
    [
        ({}, "MAINSEQUENCE_WORKLOAD_IDENTITY_SOURCE is required"),
        ({"MAINSEQUENCE_WORKLOAD_IDENTITY_SOURCE": "gcp"}, "is not a subject token source"),
        (
            {"MAINSEQUENCE_WORKLOAD_IDENTITY_SOURCE": "file"},
            "MAINSEQUENCE_WORKLOAD_IDENTITY_TOKEN_FILE is required",
        ),
        (
            {"MAINSEQUENCE_WORKLOAD_IDENTITY_SOURCE": "azure_managed_identity"},
            "MAINSEQUENCE_WORKLOAD_IDENTITY_AUDIENCE is required",
        ),
    ],
)
def test_incomplete_source_configuration_names_the_missing_setting(
    sources, monkeypatch, settings, message
):
    monkeypatch.delenv("MAINSEQUENCE_WORKLOAD_IDENTITY_SOURCE")
    for name, value in settings.items():
        monkeypatch.setenv(name, value)

    with pytest.raises(sources.SubjectTokenError, match=message):
        sources.subject_token_source_from_environment()


def test_incomplete_configuration_fails_the_request_before_anything_is_sent(workload, monkeypatch):
    utils = workload
    monkeypatch.delenv("MAINSEQUENCE_WORKLOAD_IDENTITY_TOKEN")
    exchange = _Exchange(monkeypatch)

    with pytest.raises(utils.AuthError, match="No workload identity subject token"):
        utils.WorkloadIdentityAuthProvider().get_headers()

    assert exchange.requests == []


def _azure_token(token="azure-managed-identity-token"):
    return _answer(
        200,
        {
            "access_token": token,
            "expires_on": "1893456000",
            "resource": "api://main-sequence",
            "token_type": "Bearer",
        },
    )


def _query(path):
    return parse_qs(urlsplit(path).query)


def _azure_settings(monkeypatch, client_id=None):
    monkeypatch.setenv("MAINSEQUENCE_WORKLOAD_IDENTITY_SOURCE", "azure_managed_identity")
    monkeypatch.setenv("MAINSEQUENCE_WORKLOAD_IDENTITY_AUDIENCE", "api://main-sequence")
    if client_id:
        monkeypatch.setenv("MAINSEQUENCE_WORKLOAD_IDENTITY_AZURE_CLIENT_ID", client_id)


def _serve_any_query(routes):
    """Serve routes keyed by path without the query, recording the full path."""
    from tests import _http_server

    class _ByPath(dict):
        def get(self, key, default=None):
            method, path = key
            return super().get((method, urlsplit(path).path), default)

    return _http_server.serve(_ByPath(routes))


def test_azure_source_asks_the_container_apps_identity_endpoint_for_the_audience(
    sources, monkeypatch
):
    _azure_settings(monkeypatch, client_id="user-assigned-client-id")
    monkeypatch.setenv("IDENTITY_HEADER", "identity-header-value")
    seen_headers = []

    def endpoint(handler, stop):
        seen_headers.append(dict(handler.headers))
        _azure_token()(handler, stop)

    with _serve_any_query({("GET", "/msi/token"): endpoint}) as (base_url, calls):
        monkeypatch.setenv("IDENTITY_ENDPOINT", base_url.rstrip("/") + "/msi/token")
        token = sources.subject_token_source_from_environment()()

    assert token == "azure-managed-identity-token"
    assert _query(calls[0][1]) == {
        "resource": ["api://main-sequence"],
        "api-version": ["2019-08-01"],
        "client_id": ["user-assigned-client-id"],
    }
    assert seen_headers[0]["X-IDENTITY-HEADER"] == "identity-header-value"


def test_azure_source_without_the_identity_endpoint_asks_the_instance_metadata_service(
    sources, monkeypatch
):
    _azure_settings(monkeypatch)
    seen_headers = []

    def imds(handler, stop):
        seen_headers.append(dict(handler.headers))
        _azure_token("imds-token")(handler, stop)

    with _serve_any_query({("GET", "/metadata/identity/oauth2/token"): imds}) as (base_url, calls):
        monkeypatch.setattr(
            sources,
            "AZURE_IMDS_TOKEN_URL",
            base_url.rstrip("/") + "/metadata/identity/oauth2/token",
        )
        token = sources.subject_token_source_from_environment()()

    assert token == "imds-token"
    assert _query(calls[0][1]) == {
        "resource": ["api://main-sequence"],
        "api-version": ["2018-02-01"],
    }
    assert seen_headers[0]["Metadata"] == "true"


def test_azure_source_failure_reports_the_status_without_the_answer(sources, monkeypatch):
    _azure_settings(monkeypatch)
    monkeypatch.setenv("IDENTITY_HEADER", "identity-header-value")
    refused = _answer(400, {"error": "invalid_resource", "secret-looking": "do-not-echo"})

    with _serve_any_query({("GET", "/msi/token"): refused}) as (base_url, _):
        monkeypatch.setenv("IDENTITY_ENDPOINT", base_url.rstrip("/") + "/msi/token")
        with pytest.raises(sources.SubjectTokenError) as raised:
            sources.subject_token_source_from_environment()()

    assert "HTTP 400" in str(raised.value)
    assert "do-not-echo" not in str(raised.value)
    assert "identity-header-value" not in str(raised.value)


def test_azure_source_feeds_the_exchange(workload, monkeypatch):
    utils = workload
    _azure_settings(monkeypatch)
    monkeypatch.setenv("IDENTITY_HEADER", "identity-header-value")
    routes = {("GET", "/msi/token"): _azure_token(), ("POST", EXCHANGE_PATH): _granted()}

    with _serve_any_query(routes) as (base_url, calls):
        monkeypatch.setenv("IDENTITY_ENDPOINT", base_url.rstrip("/") + "/msi/token")
        headers = _provider(utils, base_url).get_headers()

    assert headers["Authorization"] == "Bearer platform-access-1"
    assert [method for method, _, _ in calls] == ["GET", "POST"]
    assert _form(calls[1][2])["subject_token"] == ["azure-managed-identity-token"]
