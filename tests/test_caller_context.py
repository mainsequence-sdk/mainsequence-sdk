"""Caller context: W3C ``traceparent`` and ``baggage`` into and out of the SDK (ADR 0040).

Two invented applications, ``advisor`` and ``ledger``, choose their own keys;
the SDK carries every key the same way and gives none a meaning.
"""

import asyncio
import contextvars
import json
import logging
import os
import subprocess
import sys
import time
from contextlib import nullcontext
from pathlib import Path
from unittest.mock import Mock
from uuid import uuid4

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import ed25519
from fastapi import FastAPI, WebSocket
from fastapi.testclient import TestClient
from mcp.server.mcpserver import MCPServer
from mcp.server.transport_security import TransportSecuritySettings
from opentelemetry import baggage, trace
from opentelemetry import context as otel_context
from opentelemetry.context import Context

from mainsequence._transport import request_within_budget
from mainsequence.client import User, inference
from mainsequence.client import utils as client_utils
from mainsequence.instrumentation import tracer_instrumentator
from mainsequence.instrumentation.utils import (
    _add_log_baggage,
    _attach_environment_context,
    _request_context,
    _with_baggage_header,
)
from mainsequence.server.caller_assertions import (
    ASSERTION_HEADER,
    ASSERTION_TYPE,
    CallerAssertionVerifier,
)
from mainsequence.server.fastapi import (
    _RequestIdentityMiddleware,
    install_mcp,
    install_request_identity,
)
from tests._http_server import answer_json, serve

USER = str(uuid4())
RELEASE = str(uuid4())
ENVIRONMENT = str(uuid4())
ISSUER = "https://platform.example.test"
TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736"


@pytest.fixture(autouse=True)
def environment(monkeypatch):
    for name in (
        "APP_NAME",
        "FASTAPI_PUBLIC_BASE_URL",
        "MAINSEQUENCE_CALLER_AUTH_MODE",
        "MAINSEQUENCE_CALLER_ASSERTION_ISSUER",
        "MAINSEQUENCE_CALLER_ASSERTION_JWKS_URL",
        "FASTAPI_PUBLIC_INGRESS",
        "MAINSEQUENCE_ENDPOINT",
    ):
        monkeypatch.delenv(name, raising=False)


@pytest.fixture
def signed(monkeypatch):
    """Caller assertions as the platform signs them, and the verifier that checks them."""
    key = ed25519.Ed25519PrivateKey.generate()
    public = jwt.algorithms.OKPAlgorithm.to_jwk(key.public_key(), as_dict=True)
    jwk = {name: public[name] for name in ("kty", "crv", "x")}
    verifier = CallerAssertionVerifier(
        issuer=ISSUER,
        jwks_url=ISSUER + "/fastapi/caller-keys/",
        release_uid=RELEASE,
        environment_uid=ENVIRONMENT,
        fetch_jwks=Mock(
            return_value={"keys": [{"kid": "test", "alg": "EdDSA", "use": "sig", **jwk}]}
        ),
    )
    monkeypatch.setenv("MAINSEQUENCE_CALLER_AUTH_MODE", "assertion")
    monkeypatch.setattr(CallerAssertionVerifier, "from_environment", lambda: verifier)

    def token():
        now = int(time.time())
        claims = dict(
            iss=ISSUER,
            aud="urn:mainsequence:fapi:" + RELEASE,
            sub=USER,
            resource_release_uid=RELEASE,
            organization_environment_uid=ENVIRONMENT,
            iat=now,
            nbf=now,
            exp=now + 120,
            is_organization_admin=False,
        )
        return jwt.encode(
            claims, key, algorithm="EdDSA", headers={"kid": "test", "typ": ASSERTION_TYPE}
        )

    return token, verifier


def app_for(route, path="/me"):
    app = FastAPI()
    app.get(path)(route)
    install_request_identity(app)
    return app


def _trace_id():
    return format(trace.get_current_span().get_span_context().trace_id, "032x")


def _in_own_context(function):
    """Run ``function`` in an empty context, so what it attaches stays there."""
    return contextvars.Context().run(function)


def _recording(seen):
    """A route that records the request's headers and answers ``{}``."""

    def route(handler, stop):
        seen.append({key.lower(): value for key, value in handler.headers.items()})
        answer_json(200, {})(handler, stop)

    return route


# Release requests


def test_a_signed_request_carries_its_caller_context_next_to_the_caller(signed):
    token, _ = signed

    def handler():
        return {
            "caller": User.get_logged_user().uid,
            "baggage": dict(baggage.get_all()),
            "trace_id": _trace_id(),
            "log": _add_log_baggage(None, "info", {"event": "handled"}),
        }

    with TestClient(app_for(handler)) as client:
        response = client.get(
            "/me",
            headers={
                ASSERTION_HEADER: token(),
                "traceparent": TRACEPARENT,
                "baggage": (
                    "mainsequence.advisor.person=p-7f3a;source=web, "
                    "mainsequence.ledger.note=a%2Cb%3Bc,vendor.key=kept"
                ),
            },
        )

    # Properties are dropped, values are decoded as the platform decodes them,
    # and every key is carried alike. Logs carry only the mainsequence. keys.
    assert response.json() == {
        "caller": USER,
        "baggage": {
            "mainsequence.advisor.person": "p-7f3a",
            "mainsequence.ledger.note": "a,b;c",
            "vendor.key": "kept",
        },
        "trace_id": TRACE_ID,
        "log": {
            "event": "handled",
            "baggage": {
                "mainsequence.advisor.person": "p-7f3a",
                "mainsequence.ledger.note": "a,b;c",
            },
        },
    }


def test_calls_made_while_handling_a_request_pass_its_context_on(signed):
    token, _ = signed
    seen = []

    with serve({("GET", "/api/v1/ping/"): _recording(seen)}) as (base, _):

        def handler():
            client_utils.make_request(
                s=client_utils.build_session(),
                r_type="GET",
                url=base + "api/v1/ping/",
                loaders=None,
            )
            return {}

        with TestClient(app_for(handler)) as client:
            client.get(
                "/me",
                headers={
                    ASSERTION_HEADER: token(),
                    "traceparent": TRACEPARENT,
                    "baggage": "mainsequence.advisor.person=p-7f3a",
                },
            )

    assert seen[0]["baggage"] == "mainsequence.advisor.person=p-7f3a"
    assert seen[0]["traceparent"] == TRACEPARENT


def test_each_request_has_exactly_its_own_baggage_never_the_process_s(signed):
    token, verifier = signed
    assertion = (ASSERTION_HEADER.lower().encode(), token().encode())
    inside, after = {}, {}
    entered = asyncio.Event()
    waiting = 0

    async def application(scope, receive, send):
        nonlocal waiting
        waiting += 1
        if waiting == 2:
            entered.set()
        await entered.wait()  # both requests are inside before either reads
        inside[scope["path"]] = dict(baggage.get_all())
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    runtime = _RequestIdentityMiddleware(
        application, mode="assertion", verifier=verifier, public_ingress=(), routes=()
    )

    async def request(path, *headers):
        scope = {"type": "http", "method": "GET", "path": path, "headers": [assertion, *headers]}

        async def receive():
            return {"type": "http.request", "body": b""}

        async def send(message):
            pass

        await runtime(scope, receive, send)
        after[path] = dict(baggage.get_all())

    async def both():
        await asyncio.gather(
            request("/without"),
            request("/with", (b"baggage", b"mainsequence.advisor.person=p-1")),
        )

    def run():
        otel_context.attach(baggage.set_baggage("mainsequence.advisor.person", "process"))
        asyncio.run(both())

    _in_own_context(run)

    assert inside == {"/without": {}, "/with": {"mainsequence.advisor.person": "p-1"}}
    # The request's context ends with the request.
    assert after == {
        "/without": {"mainsequence.advisor.person": "process"},
        "/with": {"mainsequence.advisor.person": "process"},
    }


def test_mcp_tools_read_the_request_s_caller_context(signed):
    token, _ = signed
    server = MCPServer("context")

    @server.tool()
    async def context() -> str:
        return json.dumps(dict(baggage.get_all()))

    app = FastAPI()
    install_request_identity(app)
    install_mcp(
        app,
        server.streamable_http_app(
            stateless_http=True,
            json_response=True,
            transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False),
        ),
        lifespan=server.session_manager.run,
    )
    call = {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "context"}}

    with TestClient(app) as client:
        response = client.post(
            "/mcp",
            json=call,
            headers={
                "accept": "application/json, text/event-stream",
                "content-type": "application/json",
                "mcp-protocol-version": "2025-11-25",
                ASSERTION_HEADER: token(),
                "baggage": "mainsequence.advisor.person=p-1",
            },
        )

    text = response.json()["result"]["content"][0]["text"]
    assert json.loads(text) == {"mainsequence.advisor.person": "p-1"}


def test_a_public_request_has_no_caller_context(signed, monkeypatch):
    monkeypatch.setenv(
        "FASTAPI_PUBLIC_INGRESS", json.dumps([{"method": "GET", "path": "/callback"}])
    )

    def handler():
        return {"baggage": dict(baggage.get_all()), "trace_id": _trace_id()}

    with TestClient(app_for(handler, path="/callback")) as client:
        response = client.get(
            "/callback",
            headers={
                "X-Public-Ingress": "1",
                "traceparent": TRACEPARENT,
                "baggage": "mainsequence.advisor.person=p-1",
            },
        )

    # No verified caller can vouch for it, so the request has none.
    assert response.json() == {"baggage": {}, "trace_id": TRACE_ID}


def test_a_websocket_carries_its_caller_context():
    app = FastAPI()

    @app.websocket("/ws")
    async def ws(websocket: WebSocket):
        await websocket.accept()
        await websocket.send_json(dict(baggage.get_all()))
        await websocket.close()

    install_request_identity(app)
    with TestClient(app).websocket_connect(
        "/ws", headers={"X-User-UID": USER, "baggage": "mainsequence.advisor.person=p-1"}
    ) as connection:
        assert connection.receive_json() == {"mainsequence.advisor.person": "p-1"}


@pytest.mark.parametrize(
    "header",
    [
        "mainsequence.advisor.person",
        "=;,,=x,mainsequence.advisor.person=a\\b",
        "mainsequence.advisor.person=" + "p" * 5000,
        "mainsequence.advisor.person=p," + "x" * 9000,
    ],
    ids=["no-value", "malformed", "member-too-long", "header-too-long"],
)
def test_bad_baggage_never_fails_a_request_and_is_never_logged(signed, caplog, header):
    token, _ = signed
    caplog.set_level(logging.DEBUG)

    def handler():
        return dict(baggage.get_all())

    with TestClient(app_for(handler)) as client:
        response = client.get("/me", headers={ASSERTION_HEADER: token(), "baggage": header})

    assert response.status_code == 200
    assert response.json() == {}
    assert not [r for r in caplog.records if r.name.startswith("opentelemetry.baggage")]


def test_the_parser_limits_are_the_contract():
    def members(header):
        return dict(baggage.get_all(_with_baggage_header(header, Context())))

    # A header over 8192 bytes is ignored whole.
    long_header = ",".join(f"mainsequence.advisor.k{i}={'v' * 90}" for i in range(90))
    assert len(long_header.encode()) > 8192
    assert members(long_header) == {}
    # A member over 4096 bytes is skipped.
    header = f"mainsequence.advisor.long={'v' * 4096},mainsequence.advisor.short=s"
    assert members(header) == {"mainsequence.advisor.short": "s"}
    # At most 180 members are read.
    many = ",".join(f"mainsequence.advisor.k{i}=v" for i in range(200))
    assert len(many.encode()) <= 8192
    assert list(members(many)) == [f"mainsequence.advisor.k{i}" for i in range(180)]


# Outbound calls


def test_the_telemetry_carrier_holds_both_headers_of_the_current_context():
    def run():
        empty = tracer_instrumentator.get_telemetry_carrier()
        otel_context.attach(
            _request_context(
                {"traceparent": TRACEPARENT, "baggage": "mainsequence.advisor.person=p%201"},
                with_baggage=True,
            )
        )
        return empty, tracer_instrumentator.get_telemetry_carrier()

    empty, carrier = _in_own_context(run)

    assert empty == {}
    assert carrier == {"traceparent": TRACEPARENT, "baggage": "mainsequence.advisor.person=p+1"}


def test_platform_calls_carry_the_context_and_a_header_the_caller_sets_wins():
    seen = []

    with serve({("GET", "/api/v1/ping/"): _recording(seen)}) as (base, _):

        def call(**payload):
            client_utils.make_request(
                s=client_utils.build_session(),
                r_type="GET",
                url=base + "api/v1/ping/",
                loaders=None,
                payload=payload,
            )

        def run():
            call()
            otel_context.attach(
                _request_context(
                    {"traceparent": TRACEPARENT, "baggage": "mainsequence.advisor.person=p-1"},
                    with_baggage=True,
                )
            )
            call()
            call(headers={"Baggage": "mainsequence.advisor.person=explicit"})

        _in_own_context(run)

    without, with_context, explicit = seen
    # Without context the call is exactly as before.
    assert "baggage" not in without and "traceparent" not in without
    assert with_context["baggage"] == "mainsequence.advisor.person=p-1"
    assert with_context["traceparent"] == TRACEPARENT
    assert explicit["baggage"] == "mainsequence.advisor.person=explicit"
    assert explicit["traceparent"] == TRACEPARENT


def test_calls_to_applications_carry_the_context():
    seen = []

    with serve({("POST", "/a2a/message:send"): _recording(seen)}) as (base, _):

        def run():
            otel_context.attach(
                _request_context(
                    {"traceparent": TRACEPARENT, "baggage": "mainsequence.advisor.person=p-1"},
                    with_baggage=True,
                )
            )
            request_within_budget(
                "POST",
                base + "a2a/message:send",
                headers={"Authorization": "Bearer runtime"},
                timeout=5,
            )

        _in_own_context(run)

    assert seen[0]["baggage"] == "mainsequence.advisor.person=p-1"
    assert seen[0]["traceparent"] == TRACEPARENT
    assert seen[0]["authorization"] == "Bearer runtime"


def test_inference_calls_carry_the_context(monkeypatch):
    session = Mock()
    session.request.return_value = Mock(status_code=200)
    session.request.return_value.json.return_value = {
        "conversation_uid": str(uuid4()),
        "request_uid": str(uuid4()),
        "sequence": 1,
        "provider": "openai",
        "model": "configured-model",
        "status": "completed",
        "error_code": "",
        "content_available": True,
        "usage": {"complete": False},
    }
    monkeypatch.setattr(inference.utils, "build_session", Mock(return_value=nullcontext(session)))
    monkeypatch.setattr(
        inference.utils.loaders, "refresh_headers", Mock(return_value={"Authorization": "auth"})
    )
    monkeypatch.setattr(inference.utils, "API_ENDPOINT", "https://mainsequence.test/api/v1")

    def run():
        otel_context.attach(
            _request_context({"baggage": "mainsequence.advisor.person=p-1"}, with_baggage=True)
        )
        inference.InferenceClient().complete(
            provider="openai", model="configured-model", messages=[], idempotency_key="one"
        )

    _in_own_context(run)

    headers = session.request.call_args.kwargs["headers"]
    assert headers["baggage"] == "mainsequence.advisor.person=p-1"
    assert headers["Authorization"] == "auth"


# Process start


def test_a_run_starts_with_its_environment_carriers(monkeypatch):
    monkeypatch.setenv("TRACEPARENT", TRACEPARENT)
    monkeypatch.setenv("BAGGAGE", "mainsequence.advisor.person=p-7f3a")

    def run():
        _attach_environment_context()
        return dict(baggage.get_all()), tracer_instrumentator.get_telemetry_carrier()

    assert _in_own_context(run) == (
        {"mainsequence.advisor.person": "p-7f3a"},
        {"traceparent": TRACEPARENT, "baggage": "mainsequence.advisor.person=p-7f3a"},
    )


def test_without_carriers_the_process_context_is_unchanged(monkeypatch):
    monkeypatch.delenv("TRACEPARENT", raising=False)
    monkeypatch.delenv("BAGGAGE", raising=False)

    def run():
        before = otel_context.get_current()
        _attach_environment_context()
        return otel_context.get_current() is before

    assert _in_own_context(run)


def test_importing_the_sdk_restores_the_run_context_for_calls_and_logs(tmp_path):
    script = """
import json
import sys
import types
config = types.ModuleType("mainsequence.cli.config")
config.backend_url = lambda: ""
config.get_tokens = lambda: {}
cli = types.ModuleType("mainsequence.cli")
cli.__path__ = []
cli.config = config
sys.modules[cli.__name__] = cli
sys.modules[config.__name__] = config
from opentelemetry import baggage
from mainsequence import logger
from mainsequence.instrumentation import tracer_instrumentator
logger.info("run started")
print(json.dumps({
    "baggage": dict(baggage.get_all()),
    "carrier": tracer_instrumentator.get_telemetry_carrier(),
}))
"""
    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("MAINSEQUENCE_", "MAIN_SEQUENCE_", "JOB_RUN_UID", "OTEL_"))
    }
    environment.update(
        PYTHONPATH=str(Path(__file__).resolve().parents[1]),
        LOGGER_FILE_PATH="/dev/stdout",
        TDAG_ROOT_PATH=str(tmp_path),
        TRACEPARENT=TRACEPARENT,
        BAGGAGE="mainsequence.advisor.person=p-7f3a,vendor.key=kept",
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr

    lines = [json.loads(line) for line in result.stdout.splitlines() if line.startswith("{")]
    log_line = next(line for line in lines if line.get("event") == "run started")
    assert log_line["baggage"] == {"mainsequence.advisor.person": "p-7f3a"}
    assert lines[-1] == {
        "baggage": {"mainsequence.advisor.person": "p-7f3a", "vendor.key": "kept"},
        "carrier": {
            "traceparent": TRACEPARENT,
            "baggage": "mainsequence.advisor.person=p-7f3a,vendor.key=kept",
        },
    }


# Logs


def test_log_lines_carry_only_the_mainsequence_keys():
    def run():
        nothing = _add_log_baggage(None, "info", {"event": "a"})
        otel_context.attach(
            _request_context(
                {"baggage": "mainsequence.advisor.person=p-1,vendor.key=v"}, with_baggage=True
            )
        )
        return (
            nothing,
            _add_log_baggage(None, "info", {"event": "b"}),
            _add_log_baggage(None, "info", {"event": "c", "baggage": "the caller's own"}),
        )

    nothing, carried, own = _in_own_context(run)

    assert nothing == {"event": "a"}
    assert carried == {"event": "b", "baggage": {"mainsequence.advisor.person": "p-1"}}
    assert own == {"event": "c", "baggage": "the caller's own"}
