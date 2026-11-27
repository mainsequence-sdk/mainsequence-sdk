"""The SDK's hops in a platform trace (ADR 0042).

A release request's span is the one the platform's launcher names; each call
the SDK makes to the platform is a child span, written as one log event, and so
is a call a library sends itself through ``dependency_call``; log lines carry
the ids at the top level; the carrier goes only to the platform.
"""

import contextvars
import json
from contextlib import contextmanager
from uuid import uuid4

import pytest
import requests
from fastapi import FastAPI
from fastapi.testclient import TestClient
from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider

from mainsequence import logconf
from mainsequence._transport import request_within_budget
from mainsequence.client import utils as client_utils
from mainsequence.client.observability import OwnerLogMixin
from mainsequence.instrumentation import OTelJSONRenderer, dependency_call
from mainsequence.instrumentation.utils import (
    DependencyCall,
    _request_context,
    add_otel_trace_context,
)
from mainsequence.server.fastapi import install_request_identity
from tests._http_server import answer_json, serve, serve_keeping_connections

TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736"
CALLER_SPAN = "00f067aa0ba902b7"
TRACEPARENT = f"00-{TRACE_ID}-{CALLER_SPAN}-01"
LAUNCHER_SPAN = "b7ad6b7169203331"
UID = str(uuid4())


@pytest.fixture(autouse=True)
def environment(monkeypatch):
    for name in ("MAINSEQUENCE_CALLER_AUTH_MODE", "FASTAPI_PUBLIC_INGRESS"):
        monkeypatch.delenv(name, raising=False)


class _Events:
    """Stands in for the SDK logger and keeps the dependency events it is given."""

    def __init__(self):
        self.lines = []

    def info(self, event, **fields):
        self.lines.append(("info", event, fields))

    def warning(self, event, **fields):
        self.lines.append(("warning", event, fields))

    def __getattr__(self, name):
        return lambda *args, **kwargs: None


@pytest.fixture
def events(monkeypatch):
    recorder = _Events()
    monkeypatch.setattr(logconf, "logger", recorder)
    return recorder


@pytest.fixture
def platform(monkeypatch):
    """Serve ``routes`` as the platform's API: the origin of MAINSEQUENCE_ENDPOINT."""

    @contextmanager
    def serving(routes):
        with serve(routes) as (base, calls):
            monkeypatch.setattr(client_utils, "MAINSEQUENCE_ENDPOINT", base.rstrip("/"))
            yield base, calls

    return serving


def _recording(seen, status=200):
    def route(handler, stop):
        seen.append({key.lower(): value for key, value in handler.headers.items()})
        answer_json(status, {})(handler, stop)

    return route


def _in_own_context(function):
    return contextvars.Context().run(function)


def _ids():
    context = trace.get_current_span().get_span_context()
    return format(context.trace_id, "032x"), format(context.span_id, "016x")


def _launcher(app, **state):
    """The platform's launcher, as the application sees it: ids in ``scope["state"]``."""

    async def asgi(scope, receive, send):
        if scope["type"] == "http":
            scope.setdefault("state", {}).update(state)
        await app(scope, receive, send)

    return asgi


def _app(handler):
    app = FastAPI()
    app.get("/work")(handler)
    install_request_identity(app)
    return app


# 2. The current span is the hop's own span


def test_a_release_request_s_span_is_the_one_the_launcher_names(monkeypatch, platform, events):
    monkeypatch.setattr(
        "mainsequence.server.fastapi._local_user",
        lambda *args: type("Caller", (), {"uid": UID})(),
    )
    seen = []

    with platform({("GET", "/api/v1/ping/"): _recording(seen)}) as (base, _):

        def handler():
            ids = _ids()
            client_utils.make_request(
                s=client_utils.build_session(),
                r_type="GET",
                url=base + "api/v1/ping/",
                loaders=None,
            )
            return {"ids": ids, "recording": trace.get_current_span().is_recording()}

        app = _launcher(_app(handler), trace_id=TRACE_ID, span_id=LAUNCHER_SPAN)
        with TestClient(app) as client:
            answer = client.get(
                "/work",
                headers={
                    "Authorization": "Bearer t",
                    "traceparent": TRACEPARENT,
                    "baggage": "mainsequence.advisor.person=p-1",
                },
            ).json()

    assert answer == {"ids": [TRACE_ID, LAUNCHER_SPAN], "recording": False}
    # The call is a child of the release's span, not of its caller's.
    _, trace_id, span_id, flags = seen[0]["traceparent"].split("-")
    assert (trace_id, flags) == (TRACE_ID, "01")
    assert span_id not in {LAUNCHER_SPAN, CALLER_SPAN}
    # Baggage is ADR 0040's, unchanged.
    assert seen[0]["baggage"] == "mainsequence.advisor.person=p-1"
    [(_, event, fields)] = [line for line in events.lines if line[1].startswith("dependency.")]
    assert event == "dependency.call.completed"
    assert (fields["trace_id"], fields["span_id"], fields["parent_span_id"]) == (
        TRACE_ID,
        span_id,
        LAUNCHER_SPAN,
    )


@pytest.mark.parametrize(
    "state",
    [
        {},
        {"trace_id": TRACE_ID.upper(), "span_id": LAUNCHER_SPAN},
        {"trace_id": TRACE_ID, "span_id": "0" * 16},
        {"trace_id": "0" * 32, "span_id": LAUNCHER_SPAN},
        {"trace_id": TRACE_ID[:-1], "span_id": LAUNCHER_SPAN},
        {"trace_id": TRACE_ID, "span_id": None},
    ],
)
def test_without_valid_launcher_ids_the_inbound_traceparent_is_the_parent(state):
    def run():
        otel_context.attach(
            _request_context({"traceparent": TRACEPARENT}, with_baggage=False, state=state)
        )
        return _ids(), trace.get_current_span().get_span_context().is_remote

    assert _in_own_context(run) == ((TRACE_ID, CALLER_SPAN), True)


def test_launcher_ids_in_another_trace_than_the_inbound_one_start_their_own_flags():
    own_trace = "0af7651916cd43dd8448eb211c80319c"

    def run():
        otel_context.attach(
            _request_context(
                {"traceparent": f"00-{TRACE_ID}-{CALLER_SPAN}-00"},
                with_baggage=False,
                state={"trace_id": own_trace, "span_id": LAUNCHER_SPAN},
            )
        )
        context = trace.get_current_span().get_span_context()
        return _ids(), context.is_remote, int(context.trace_flags)

    assert _in_own_context(run) == ((own_trace, LAUNCHER_SPAN), False, 1)


def test_a_span_the_application_already_records_wins():
    tracer = TracerProvider().get_tracer("application")

    def run():
        with tracer.start_as_current_span("request") as span:
            otel_context.attach(
                _request_context(
                    {"traceparent": TRACEPARENT},
                    with_baggage=False,
                    state={"trace_id": TRACE_ID, "span_id": LAUNCHER_SPAN},
                )
            )
            return trace.get_current_span() is span

    assert _in_own_context(run)


# 4. Log lines carry the ids at the top level


def _rendered(**fields):
    return json.loads(OTelJSONRenderer()(None, "info", {"event": "e", **fields}))


def test_log_lines_carry_the_ids_of_a_non_recording_span():
    def run():
        otel_context.attach(
            _request_context(
                {},
                with_baggage=False,
                state={"trace_id": TRACE_ID, "span_id": LAUNCHER_SPAN},
            )
        )
        return _rendered()

    line = _in_own_context(run)
    assert (line["trace_id"], line["span_id"], line["parent_span_id"]) == (
        TRACE_ID,
        LAUNCHER_SPAN,
        None,
    )
    assert line["span"] is None


def test_log_lines_without_a_context_carry_null_ids():
    line = _in_own_context(_rendered)
    assert (line["trace_id"], line["span_id"], line["parent_span_id"], line["span"]) == (
        None,
        None,
        None,
        None,
    )


def test_log_lines_of_a_recording_span_carry_its_parent_and_keep_the_nested_object():
    tracer = TracerProvider().get_tracer("application")

    def run():
        with tracer.start_as_current_span("outer") as outer:
            with tracer.start_as_current_span("inner") as inner:
                return outer.get_span_context(), inner.get_span_context(), _rendered()

    outer, inner, line = _in_own_context(run)
    ids = {
        "trace_id": format(inner.trace_id, "032x"),
        "span_id": format(inner.span_id, "016x"),
        "parent_span_id": format(outer.span_id, "016x"),
    }
    assert {key: line[key] for key in ids} == ids
    assert line["span"] == ids


def test_a_field_the_log_call_sets_wins():
    line = _in_own_context(lambda: add_otel_trace_context(None, "info", {"trace_id": "own"}))
    assert line["trace_id"] == "own"


# 3. A call to the platform is a span


def test_a_platform_call_event_names_its_route_never_its_url(platform, events):
    seen = []
    path = f"/api/v1/job-runs/{UID}/logs/"

    with platform({("GET", f"{path}?secret=s"): _recording(seen, status=404)}) as (base, _):
        _in_own_context(
            lambda: client_utils.make_request(
                s=client_utils.build_session(),
                r_type="get",
                url=f"{base.rstrip('/')}{path}?secret=s",
                loaders=None,
                payload={"headers": {"X-Private": "p"}},
            )
        )

    [(level, event, fields)] = events.lines
    assert (level, event) == ("info", "dependency.call.completed")
    span_id = seen[0]["traceparent"].split("-")[2]
    assert fields.pop("duration_ms") >= 0
    assert fields.pop("trace_id") == seen[0]["traceparent"].split("-")[1]
    assert fields == {
        "span_id": span_id,
        # Made outside any context: the root of a new trace.
        "parent_span_id": None,
        "dependency_type": "api",
        "dependency_name": "mainsequence_backend",
        "http_method": "GET",
        "route": "/api/v1/job-runs/{uid}/logs/",
        "status_code": 404,
        "outcome": "rejected",
    }


def test_a_platform_call_without_an_answer_is_a_failed_event(platform, events):
    with platform({}) as (base, _):
        url = base + "api/v1/ping/"
    # The server is gone: the call gets no answer.
    response = client_utils.make_request(
        s=client_utils.build_session(retries=0), r_type="POST", url=url, loaders=None
    )

    assert response.status_code == 500
    [(level, event, fields)] = events.lines
    assert (level, event) == ("warning", "dependency.call.failed")
    assert fields["status_code"] is None
    assert fields["outcome"] == "failed"
    assert fields["error_type"] == "ConnectionError"


def test_a_release_call_names_its_target_and_its_path_on_the_release(events):
    with serve({("POST", "/v1/orders"): _recording([])}) as (base, _):
        request_within_budget(
            "POST",
            base + "v1/orders",
            timeout=5,
            dependency=DependencyCall("release", "POST", "/v1/orders?account=1", name=UID),
        )

    [(_, event, fields)] = events.lines
    assert event == "dependency.call.completed"
    assert fields["dependency_type"] == "release"
    assert fields["dependency_name"] == UID
    assert fields["route"] == "/v1/orders"
    assert fields["outcome"] == "success"
    assert not {"dependency_kind", "target_release_uid", "target_agent_session_uid"} & set(fields)


def test_a_call_that_raises_is_a_failed_event(events):
    with serve({}) as (base, _):
        url = base + "a2a/message:send"
    with pytest.raises(requests.ConnectionError):
        request_within_budget(
            "POST", url, timeout=1, dependency=DependencyCall("agent", "POST", "/a2a/x")
        )

    [(level, event, fields)] = events.lines
    assert (level, event, fields["error_type"]) == (
        "warning",
        "dependency.call.failed",
        "ConnectionError",
    )


# 3, for libraries: a call a library sends with its own HTTP client (issue #263)


def _in_a_request(function):
    """Run ``function`` as a release's handler runs: in the request's own span, with its baggage."""

    def run():
        otel_context.attach(
            _request_context(
                {"traceparent": TRACEPARENT, "baggage": "mainsequence.advisor.person=p-1"},
                with_baggage=True,
                state={"trace_id": TRACE_ID, "span_id": LAUNCHER_SPAN},
            )
        )
        return function()

    return _in_own_context(run)


def test_a_library_s_own_call_is_a_child_span_written_as_one_event(events):
    path = f"/v1/prices/{UID}?symbol=ABC"

    with serve_keeping_connections() as (rpc_url, seen):

        def call():
            with requests.Session() as session:
                with dependency_call("release", "GET", path, name=UID) as span:
                    response = session.get(
                        rpc_url + path, headers={**span.headers(), "Authorization": "Bearer t"}
                    )
                    span.status_code = response.status_code

        _in_a_request(call)

    [(_, headers)] = seen
    _, trace_id, span_id, flags = headers["traceparent"].split("-")
    # A child of the request's span, in its trace.
    assert (trace_id, flags) == (TRACE_ID, "01")
    assert span_id not in {LAUNCHER_SPAN, CALLER_SPAN}
    assert headers["baggage"] == "mainsequence.advisor.person=p-1"
    assert headers["authorization"] == "Bearer t"
    [(level, event, fields)] = events.lines
    assert (level, event) == ("info", "dependency.call.completed")
    assert fields.pop("duration_ms") >= 0
    assert fields == {
        "trace_id": TRACE_ID,
        "span_id": span_id,
        "parent_span_id": LAUNCHER_SPAN,
        "dependency_type": "release",
        "dependency_name": UID,
        "http_method": "GET",
        "route": "/v1/prices/{uid}",
        "status_code": 200,
        "outcome": "success",
    }


def test_a_library_s_calls_keep_its_own_connection_pool(events):
    with serve_keeping_connections() as (rpc_url, seen):

        def calls():
            with requests.Session() as session:
                for _ in range(2):
                    with dependency_call("release", "GET", "/v1/prices", name=UID) as span:
                        response = session.get(rpc_url + "/v1/prices", headers=span.headers())
                        span.status_code = response.status_code

        _in_a_request(calls)

    assert len(seen) == 2
    # Both requests came over one connection.
    assert seen[0][0] == seen[1][0]
    # One span for each call, each written once.
    sent = [headers["traceparent"].split("-")[2] for _, headers in seen]
    assert len(set(sent)) == 2
    assert [fields["span_id"] for _, _, fields in events.lines] == sent
    assert {fields["parent_span_id"] for _, _, fields in events.lines} == {LAUNCHER_SPAN}


def test_a_library_call_that_raises_or_gets_no_answer_is_a_failed_event(events):
    with serve({}) as (base, _):
        url = base + "v1/prices"
    # The server is gone: the call gets no answer.
    with pytest.raises(requests.ConnectionError):
        with dependency_call("release", "GET", "/v1/prices", name=UID) as span:
            requests.get(url, headers=span.headers(), timeout=1)
    with dependency_call("agent", "POST", "/a2a/message:send", name=UID):
        pass  # no status was set

    [(level, event, fields), (_, second, unanswered)] = events.lines
    assert (level, event, fields["error_type"], fields["outcome"]) == (
        "warning",
        "dependency.call.failed",
        "ConnectionError",
        "failed",
    )
    assert (second, unanswered["error_type"], unanswered["status_code"]) == (
        "dependency.call.failed",
        "NoResponse",
        None,
    )


@pytest.mark.parametrize(
    ("args", "kwargs", "message"),
    [
        (("database", "GET", "/x"), {}, "dependency_type"),
        (("release", "GET", "/x"), {}, "needs name"),
        (("agent", "POST", "/x"), {"name": ""}, "needs name"),
        (("api", " ", "/api/v1/x/"), {}, "method"),
        (("api", "GET", ""), {}, "path"),
    ],
)
def test_dependency_call_refuses_a_call_the_platform_could_not_name(args, kwargs, message):
    with pytest.raises(ValueError, match=message):
        dependency_call(*args, **kwargs)


def test_the_call_span_is_public_api(events):
    import mainsequence.instrumentation as instrumentation

    assert {"DEPENDENCY_TYPES", "DependencyCall", "dependency_call"} <= set(instrumentation.__all__)
    assert instrumentation.DEPENDENCY_TYPES == ("api", "release", "agent", "inference")
    with instrumentation.dependency_call("api", "get", "/api/v1/users/me/") as call:
        call.status_code = 200
    [(_, _, fields)] = events.lines
    assert (fields["dependency_name"], fields["http_method"]) == ("mainsequence_backend", "GET")


# 5. The carrier goes only to the platform


def test_a_call_to_another_origin_carries_no_context_and_writes_no_event(monkeypatch, events):
    seen = []
    monkeypatch.setattr(client_utils, "MAINSEQUENCE_ENDPOINT", "https://platform.invalid")

    with serve(
        {("GET", "/elsewhere"): _recording(seen), ("POST", "/direct"): _recording(seen)}
    ) as (base, _):

        def run():
            otel_context.attach(
                _request_context(
                    {"traceparent": TRACEPARENT, "baggage": "mainsequence.advisor.person=p-1"},
                    with_baggage=True,
                )
            )
            client_utils.make_request(
                s=client_utils.build_session(), r_type="GET", url=base + "elsewhere", loaders=None
            )
            request_within_budget("POST", base + "direct", timeout=5)

        _in_own_context(run)

    assert len(seen) == 2
    for headers in seen:
        assert "traceparent" not in headers and "baggage" not in headers
    assert events.lines == []


@pytest.mark.parametrize(
    ("url", "platform"),
    [
        ("https://api.example.test/api/v1/x/", True),
        ("https://API.example.test:443/api/v1/x/", True),
        ("http://api.example.test/api/v1/x/", False),
        ("https://api.example.test:8443/api/v1/x/", False),
        ("https://api.example.test.evil/api/v1/x/", False),
        ("https://github.com/x", False),
        ("https://api.example.test:bad/", False),
    ],
)
def test_the_platform_is_the_origin_of_the_endpoint(monkeypatch, url, platform):
    monkeypatch.setattr(client_utils, "MAINSEQUENCE_ENDPOINT", "https://api.example.test")
    assert client_utils.is_platform_url(url) is platform


# 7. Reading traces


class _Owner(OwnerLogMixin):
    ROOT_URL = "https://api.example.test/api/v1"
    LOADERS = None

    def __init__(self):
        self.observability = type(
            "Links",
            (),
            {
                "application_logs_url": (
                    "/api/v1/job-runs/x/logs/?organization_environment_uid=" + UID
                )
            },
        )()

    @classmethod
    def build_session(cls):
        return None


def test_owner_logs_pass_an_exact_trace_id(monkeypatch):
    sent = []
    page = {"organization_environment_uid": UID, "start": 0, "end": 1, "truncated": False}

    def make_request(**kwargs):
        sent.append(kwargs["payload"]["params"])
        return type("R", (), {"status_code": 200, "json": lambda self: page})()

    monkeypatch.setattr("mainsequence.client.observability.make_request", make_request)

    _Owner().get_logs(trace_id=TRACE_ID)

    assert sent[0]["trace_id"] == TRACE_ID


@pytest.mark.parametrize("trace_id", [TRACE_ID.upper(), TRACE_ID[:-1], f"{TRACE_ID}0", "", 7])
def test_a_trace_id_that_is_not_32_lowercase_hex_characters_is_refused(trace_id):
    with pytest.raises(ValueError, match="trace_id"):
        _Owner().get_logs(trace_id=trace_id)


@pytest.mark.parametrize("model", ["JobRun", "ResourceRelease", "Agent", "AgentSession"])
def test_collection_log_search_passes_an_exact_trace_id(monkeypatch, model):
    import mainsequence.client.agent_runtime_models as agents
    import mainsequence.client.models_helpers as helpers
    import mainsequence.client.observability as observability

    sent = []

    def make_request(**kwargs):
        sent.append((kwargs["url"], kwargs["payload"]["params"]))
        return type("R", (), {"status_code": 500})()

    def refuse(response, payload=None):
        raise RuntimeError("stop")

    monkeypatch.setattr(observability, "make_request", make_request)
    monkeypatch.setattr(observability, "raise_for_response", refuse)
    search = getattr(agents if model.startswith("Agent") else helpers, model).search_logs
    window = {
        "organization_environment_uid": UID,
        "start_time": "2026-11-26T00:00:00Z",
        "end_time": "2026-11-26T01:00:00Z",
    }

    with pytest.raises(RuntimeError, match="stop"):
        search(**window, trace_id=TRACE_ID)
    with pytest.raises(ValueError, match="trace_id"):
        search(**window, trace_id=TRACE_ID.upper())

    [(url, params)] = sent
    assert url.endswith("/logs/")
    assert params["trace_id"] == TRACE_ID


@pytest.mark.parametrize(
    ("status", "outcome"),
    [(200, "success"), (302, "success"), (400, "rejected"), (499, "rejected"), (500, "failed")],
)
def test_the_outcome_is_the_platform_s(events, status, outcome):
    with serve({("GET", "/x"): _recording([], status=status)}) as (base, _):
        request_within_budget(
            "GET", base + "x", timeout=5, dependency=DependencyCall("inference", "GET", "/x")
        )

    [(_, event, fields)] = events.lines
    assert (event, fields["outcome"], fields["dependency_name"]) == (
        "dependency.call.completed",
        outcome,
        "mainsequence_backend",
    )


def test_log_rows_type_the_dependency_fields():
    from mainsequence.client.observability import EnvironmentLogSearchRow, OwnerLogRow

    fields = {
        "dependency_type": "release",
        "dependency_name": UID,
        "dependency_operation": "GET /v1/prices",
        "dependency_attempt": 1,
        "dependency_duration_ms": 12.5,
        "dependency_outcome": "success",
        "dependency_status_code": 200,
        "dependency_error_type": None,
        "circuit_breaker_state": "closed",
    }
    row = OwnerLogRow.model_validate(fields)
    assert {name: getattr(row, name) for name in fields} == fields
    assert set(fields) <= set(OwnerLogRow.model_fields)
    assert set(fields) <= set(EnvironmentLogSearchRow.model_fields)
