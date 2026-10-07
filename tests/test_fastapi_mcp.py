"""An application-owned MCP server served by install_mcp (ADR 0038)."""

import asyncio
import json
from contextlib import asynccontextmanager
from contextvars import copy_context
from unittest.mock import Mock
from uuid import uuid4

import anyio
import httpx
import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings

import tests.test_fastapi_request_identity as identity_tests
from mainsequence.client import RequestIdentityError, User
from mainsequence.server.caller_assertions import ASSERTION_HEADER
from mainsequence.server.fastapi import install_mcp, install_request_identity

USER, OTHER, ISSUER = identity_tests.USER, identity_tests.OTHER, identity_tests.ISSUER
clean_environment = identity_tests.clean_environment
signed = identity_tests.signed
requester_claim = identity_tests.requester_claim

# A calling Agent's workload identity, and two people it can work for.
WORKLOAD, PERSON, OTHER_PERSON = USER, identity_tests.PERSON, OTHER

MCP_HEADERS = {
    "accept": "application/json, text/event-stream",
    "content-type": "application/json",
    "mcp-protocol-version": "2025-11-25",
}


@pytest.fixture(autouse=True)
def no_cors_origins(monkeypatch):
    monkeypatch.delenv("FASTAPI_CORS_ALLOW_ORIGINS", raising=False)


def _call(name, **arguments):
    return {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {"name": name, "arguments": arguments},
    }


def _text(response):
    assert response.status_code == 200, response.text
    result = response.json()["result"]
    assert result.get("isError") is not True, result
    return result["content"][0]["text"]


def _refused(response):
    """A tool that raised: MCP answers 200 with an error result."""
    assert response.status_code == 200, response.text
    return response.json()["result"].get("isError") is True


def _seen():
    """The identity the current tool call sees: its caller, and the person it works for."""
    caller = User.get_logged_user()
    person = User.get_requester()
    return {
        "caller": [caller.uid, caller.is_organization_admin],
        "person": None if person is None else [person.uid, person.is_organization_admin],
    }


def _reachable():
    """Whether ``get_logged_user()`` and ``get_requester()`` still answer here."""
    answers = []
    for getter in (User.get_logged_user, User.get_requester):
        try:
            getter()
        except RequestIdentityError:
            answers.append(False)
        else:
            answers.append(True)
    return answers


def _server():
    """A stateless JSON server; the SDK checks Origin and the platform routes Host."""
    return FastMCP(
        "identity",
        stateless_http=True,
        json_response=True,
        transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False),
    )


def _identity_server():
    server = _server()

    @server.tool()
    async def caller() -> str:
        return User.get_logged_user().uid

    @server.tool()
    async def caller_in_thread() -> str:
        return await anyio.to_thread.run_sync(lambda: User.get_logged_user().uid)

    @server.tool()
    async def requester() -> str:
        person = User.get_requester()
        return json.dumps(None if person is None else [person.uid, person.is_organization_admin])

    return server


def _install(app, server):
    install_request_identity(app)
    install_mcp(app, server.streamable_http_app(), lifespan=server.session_manager.run)
    return app


async def _fake_mcp(scope, receive, send):
    await send(
        {
            "type": "http.response.start",
            "status": 200,
            "headers": [(b"content-type", b"application/json")],
        }
    )
    headers = {key.decode("latin-1"): value.decode("latin-1") for key, value in scope["headers"]}
    await send({"type": "http.response.body", "body": json.dumps(headers).encode()})


@asynccontextmanager
async def _no_lifespan():
    yield


def test_tools_read_the_verified_caller_and_rest_routes_keep_working(signed):
    token, _, _ = signed
    app = FastAPI()

    @app.get("/prices")
    def prices():
        return {"caller": User.get_logged_user().uid}

    _install(app, _identity_server())
    proof = {**MCP_HEADERS, ASSERTION_HEADER: token()}
    requester_bound = {
        **MCP_HEADERS,
        ASSERTION_HEADER: token(requester=requester_claim(OTHER, is_organization_admin=True)),
    }

    with TestClient(app) as client:
        assert _text(client.post("/mcp", json=_call("caller"), headers=proof)) == USER
        assert _text(client.post("/mcp", json=_call("caller_in_thread"), headers=proof)) == USER
        nobody = client.post("/mcp", json=_call("requester"), headers=proof)
        person = client.post("/mcp", json=_call("requester"), headers=requester_bound)
        assert json.loads(_text(nobody)) is None
        # The person's own admin flag reaches the tool.
        assert json.loads(_text(person)) == [OTHER, True]
        assert client.get("/prices", headers={ASSERTION_HEADER: token()}).json() == {"caller": USER}
        missing = client.post("/mcp", json=_call("caller"), headers=MCP_HEADERS)
        forged = client.post(
            "/mcp",
            json=_call("caller"),
            headers={**MCP_HEADERS, ASSERTION_HEADER: token(aud="urn:other:" + str(uuid4()))},
        )

    assert (missing.status_code, forged.status_code) == (401, 401)
    assert "/mcp" not in app.openapi()["paths"]


def test_requester_bound_write_checks_the_person_without_workload_fallback(signed):
    """A tool that needs a person fails with its own error when there is none."""
    token, _, _ = signed
    server = _server()
    report_editors = {USER, OTHER}
    writes = []

    @server.tool()
    async def publish_report() -> str:
        requester = User.get_requester()
        if requester is None or requester.uid not in report_editors:
            raise PermissionError("The requester cannot publish this report.")
        writes.append((User.get_logged_user().uid, requester.uid))
        return requester.uid

    app = _install(FastAPI(), server)
    with TestClient(app) as client:
        allowed = client.post(
            "/mcp",
            json=_call("publish_report"),
            headers={**MCP_HEADERS, ASSERTION_HEADER: token(requester=requester_claim(OTHER))},
        )
        assert _text(allowed) == OTHER

        for proof in (token(requester=requester_claim(str(uuid4()))), token()):
            refused = client.post(
                "/mcp",
                json=_call("publish_report"),
                headers={**MCP_HEADERS, ASSERTION_HEADER: proof},
            )
            assert _refused(refused)

    assert writes == [(USER, OTHER)]


def test_a_tool_with_an_optional_person_never_combines_grants(signed):
    """With a person, only the person's grants count; without one, only the caller's."""
    token, _, _ = signed
    server = _server()
    # The person may publish one report and the calling workload the other.
    publishers = {"person_report": {PERSON}, "workload_report": {WORKLOAD}}
    published = []

    @server.tool()
    async def publish(report: str) -> str:
        actor = User.get_requester() or User.get_logged_user()
        if actor.uid not in publishers[report]:
            raise PermissionError(f"{actor.uid} cannot publish {report}.")
        published.append((report, actor.uid))
        return actor.uid

    app = _install(FastAPI(), server)
    callers = {
        "for_the_person": token(WORKLOAD, requester=requester_claim(PERSON)),
        "as_the_workload": token(WORKLOAD),
    }
    with TestClient(app) as client:
        refused = {
            (caller, report): _refused(
                client.post(
                    "/mcp",
                    json=_call("publish", report=report),
                    headers={**MCP_HEADERS, ASSERTION_HEADER: proof},
                )
            )
            for caller, proof in callers.items()
            for report in publishers
        }

    assert refused == {
        ("for_the_person", "person_report"): False,
        # The workload's own grant is never added to the person's.
        ("for_the_person", "workload_report"): True,
        ("as_the_workload", "workload_report"): False,
        # Without the person, the person's grant does not apply.
        ("as_the_workload", "person_report"): True,
    }
    assert published == [("person_report", PERSON), ("workload_report", WORKLOAD)]


REJECTED_ASSERTIONS = {
    "missing": None,
    "two_field_requester": dict(requester={"sub": PERSON, "team_uids": []}),
    "requester_missing_team_uids": dict(requester={"sub": PERSON, "is_organization_admin": False}),
    "requester_extra_key": dict(requester={**requester_claim(PERSON), "username": "person"}),
    "requester_integer_admin_flag": dict(
        requester=requester_claim(PERSON, is_organization_admin=1)
    ),
    "without_caller_facts": dict(omit=("team_uids", "is_organization_admin")),
    "requester_bound_without_caller_facts": dict(
        requester=requester_claim(PERSON), omit=("team_uids", "is_organization_admin")
    ),
}


@pytest.mark.parametrize("case", list(REJECTED_ASSERTIONS))
def test_rejected_assertions_never_reach_a_tool(signed, monkeypatch, case):
    """Refused before any tool runs, never as the workload or the developer instead."""
    token, _, _ = signed
    monkeypatch.setenv("MAINSEQUENCE_ACCESS_TOKEN", "workload-session")
    monkeypatch.setattr(
        "mainsequence.server.fastapi.requests.get",
        Mock(side_effect=AssertionError("A refused assertion resolved another account.")),
    )
    server = _server()
    ran = []

    @server.tool()
    async def anything() -> str:
        ran.append(_seen())
        return "ran"

    app = _install(FastAPI(), server)
    headers = {**MCP_HEADERS, "Authorization": "Bearer developer-token"}
    if REJECTED_ASSERTIONS[case] is not None:
        headers[ASSERTION_HEADER] = token(WORKLOAD, **REJECTED_ASSERTIONS[case])
    with TestClient(app) as client:
        response = client.post("/mcp", json=_call("anything"), headers=headers)

    assert response.status_code == 401
    assert ran == []


def test_concurrent_calls_keep_their_own_identity_in_tasks_and_threads(signed):
    """People calling directly, an Agent working for each of them, and the Agent alone."""
    token, _, _ = signed
    server = _server()
    calls = {
        "person": token(PERSON, is_organization_admin=True),
        "other_person": token(OTHER_PERSON),
        "agent_for_person": token(
            WORKLOAD, requester=requester_claim(PERSON, is_organization_admin=True)
        ),
        "agent_for_other_person": token(WORKLOAD, requester=requester_claim(OTHER_PERSON)),
        "agent_alone": token(WORKLOAD),
    }
    all_inside = asyncio.Barrier(len(calls))

    @server.tool()
    async def who_is_asking() -> str:
        await asyncio.wait_for(all_inside.wait(), 5)
        seen = {"handler": _seen()}

        async def in_a_task():
            await asyncio.sleep(0)
            return _seen()

        seen["asyncio_task"] = await asyncio.create_task(in_a_task())
        in_group = []

        async def in_a_task_group():
            await anyio.sleep(0)
            in_group.append(_seen())

        async with anyio.create_task_group() as group:
            group.start_soon(in_a_task_group)
        seen["anyio_task_group"] = in_group[0]
        seen["asyncio_thread"] = await asyncio.to_thread(_seen)
        seen["anyio_worker_thread"] = await anyio.to_thread.run_sync(_seen)
        # Every call has used the shared tasks and threads before any reads again.
        await asyncio.wait_for(all_inside.wait(), 5)
        seen["after_the_others"] = _seen()
        # A thread that gets no copy of the context has no identity at all.
        without_context = await asyncio.get_running_loop().run_in_executor(None, _reachable)
        return json.dumps({"seen": seen, "without_context": without_context})

    app = _install(FastAPI(), server)

    async def run():
        async with app.router.lifespan_context(app):
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(
                transport=transport, base_url="http://testserver"
            ) as client:

                async def call(proof):
                    headers = {**MCP_HEADERS, ASSERTION_HEADER: proof}
                    response = await client.post(
                        "/mcp", json=_call("who_is_asking"), headers=headers
                    )
                    return json.loads(_text(response))

                answers = await asyncio.gather(*(call(proof) for proof in calls.values()))
                # The pooled worker threads keep no identity once the calls end.
                leftovers = [
                    await asyncio.to_thread(_reachable),
                    await anyio.to_thread.run_sync(_reachable),
                ]
                return dict(zip(calls, answers, strict=True)), leftovers

    answers, leftovers = asyncio.run(run())

    expected = {
        "person": {"caller": [PERSON, True], "person": None},
        "other_person": {"caller": [OTHER_PERSON, False], "person": None},
        "agent_for_person": {"caller": [WORKLOAD, False], "person": [PERSON, True]},
        "agent_for_other_person": {"caller": [WORKLOAD, False], "person": [OTHER_PERSON, False]},
        "agent_alone": {"caller": [WORKLOAD, False], "person": None},
    }
    places = (
        "handler",
        "asyncio_task",
        "anyio_task_group",
        "asyncio_thread",
        "anyio_worker_thread",
        "after_the_others",
    )
    for name, answer in answers.items():
        assert answer == {
            "seen": dict.fromkeys(places, expected[name]),
            "without_context": [False, False],
        }, name
    assert leftovers == [[False, False], [False, False]]


@pytest.mark.parametrize("ending", ["completion", "failure", "cancellation"])
def test_a_tool_call_leaves_no_identity_behind(signed, ending):
    """Its end resets the identity, also for what it started, and spares a concurrent call."""
    token, _, _ = signed
    server = _server()
    inside, peer_inside, ended = asyncio.Event(), asyncio.Event(), asyncio.Event()
    copied, started = [], []

    async def after_the_call():
        await ended.wait()
        return _reachable()

    @server.tool()
    async def ends() -> str:
        copied.append(copy_context())
        started.append(asyncio.create_task(after_the_call()))
        assert _reachable() == [True, True]
        await asyncio.wait_for(peer_inside.wait(), 5)
        inside.set()
        if ending == "failure":
            raise RuntimeError("The tool failed.")
        if ending == "cancellation":
            await asyncio.Event().wait()  # until its request is cancelled
        return User.get_requester().uid

    @server.tool()
    async def peer() -> str:
        peer_inside.set()
        await asyncio.wait_for(ended.wait(), 5)
        return json.dumps(_seen())

    app = _install(FastAPI(), server)
    for_person = token(WORKLOAD, requester=requester_claim(PERSON, is_organization_admin=True))
    for_other_person = token(WORKLOAD, requester=requester_claim(OTHER_PERSON))

    async def run():
        async with app.router.lifespan_context(app):
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(
                transport=transport, base_url="http://testserver"
            ) as client:

                def post(name, proof):
                    headers = {**MCP_HEADERS, ASSERTION_HEADER: proof}
                    return asyncio.create_task(
                        client.post("/mcp", json=_call(name), headers=headers)
                    )

                peer_call = post("peer", for_other_person)
                call = post("ends", for_person)
                await asyncio.wait_for(inside.wait(), 5)
                if ending == "cancellation":
                    call.cancel()
                (outcome,) = await asyncio.gather(call, return_exceptions=True)
                ended.set()
                return outcome, await peer_call, await asyncio.gather(*started)

    outcome, peer_response, after = asyncio.run(run())

    if ending == "completion":
        assert _text(outcome) == PERSON
    elif ending == "failure":
        assert _refused(outcome)
    else:
        assert isinstance(outcome, asyncio.CancelledError)
    # A task the call started, and its copied context, keep no identity afterwards.
    assert after == [[False, False]]
    assert [context.run(_reachable) for context in copied] == [[False, False]]
    # The concurrent call kept its own identity throughout.
    assert json.loads(_text(peer_response)) == {
        "caller": [WORKLOAD, False],
        "person": [OTHER_PERSON, False],
    }


def test_endpoint_declines_streams_sessions_and_foreign_origins(signed, monkeypatch):
    token, _, _ = signed
    monkeypatch.setenv("FASTAPI_PUBLIC_BASE_URL", "https://prices.example.test/")
    monkeypatch.setenv(
        "FASTAPI_CORS_ALLOW_ORIGINS", "https://app.example.test, https://*.tenant.example.test"
    )
    app = FastAPI()
    install_request_identity(app)
    install_mcp(app, _fake_mcp, lifespan=_no_lifespan)
    proof = {ASSERTION_HEADER: token()}

    with TestClient(app) as client:
        stream = client.get("/mcp", headers=proof)
        delete = client.delete("/mcp", headers=proof)
        allowed = [
            client.post("/mcp", json={}, headers={**proof, "origin": origin}).status_code
            for origin in (
                "https://prices.example.test",
                "https://app.example.test",
                "https://east.tenant.example.test",
            )
        ]
        refused = [
            client.post("/mcp", json={}, headers={**proof, "origin": origin}).status_code
            for origin in (
                "https://evil.example.test",
                "https://a.b.tenant.example.test",
                "null",
            )
        ]
        duplicated = client.post(
            "/mcp",
            json={},
            headers=[
                (ASSERTION_HEADER, proof[ASSERTION_HEADER]),
                ("origin", "https://app.example.test"),
                ("origin", "https://evil.example.test"),
            ],
        )
        with_session = client.post("/mcp", json={}, headers={**proof, "mcp-session-id": "abc"})

    assert (stream.status_code, stream.headers["allow"]) == (405, "POST")
    assert delete.status_code == 405
    assert allowed == [200, 200, 200]
    assert refused == [403, 403, 403]
    assert duplicated.status_code == 403
    assert with_session.status_code == 200
    assert "mcp-session-id" not in with_session.json()


def test_installation_rejects_unsafe_or_conflicting_setups(signed, monkeypatch):
    with pytest.raises(RuntimeError, match="request identity"):
        install_mcp(FastAPI(), _fake_mcp, lifespan=_no_lifespan)

    routed = FastAPI()
    routed.get("/mcp/tools")(lambda: {})
    install_request_identity(routed)
    with pytest.raises(RuntimeError, match="conflicts"):
        install_mcp(routed, _fake_mcp, lifespan=_no_lifespan)

    mounted = FastAPI()
    mounted.mount("/mcp", _fake_mcp)
    install_request_identity(mounted)
    with pytest.raises(RuntimeError, match="conflicts"):
        install_mcp(mounted, _fake_mcp, lifespan=_no_lifespan)

    app = FastAPI()
    install_request_identity(app)
    with pytest.raises(TypeError):
        install_mcp(app, _fake_mcp, lifespan=None)
    install_mcp(app, _fake_mcp, lifespan=_no_lifespan)
    with pytest.raises(RuntimeError, match="already installed"):
        install_mcp(app, _fake_mcp, lifespan=_no_lifespan)
    assert app.state.mainsequence_mcp == {
        "installed": True,
        "path": "/mcp",
        "transport": "streamable-http",
        "stateless": True,
        "request_identity": True,
    }
    assert app.state.mainsequence_request_identity["installed"] is True

    app.get("/mcp/late")(lambda: {})
    with pytest.raises(RuntimeError, match="conflicts"):
        with TestClient(app):
            pass

    monkeypatch.setenv("FASTAPI_PUBLIC_INGRESS", json.dumps([{"method": "POST", "path": "/mcp"}]))
    public = FastAPI()
    install_request_identity(public)
    with pytest.raises(RuntimeError, match="public ingress"):
        install_mcp(public, _fake_mcp, lifespan=_no_lifespan)


def test_lifespans_start_once_in_order_and_unwind_on_failure(signed):
    token, _, _ = signed
    events = []

    @asynccontextmanager
    async def application_lifespan(app):
        events.append("app start")
        try:
            yield {"greeting": "hello"}
        finally:
            events.append("app stop")

    @asynccontextmanager
    async def mcp_lifespan():
        events.append("mcp start")
        yield
        events.append("mcp stop")

    app = FastAPI(lifespan=application_lifespan)

    @app.get("/greeting")
    def greeting(request: Request):
        return {"greeting": request.state.greeting}

    install_request_identity(app)
    install_mcp(app, _fake_mcp, lifespan=mcp_lifespan)
    with TestClient(app) as client:
        assert client.get("/greeting", headers={ASSERTION_HEADER: token()}).json() == {
            "greeting": "hello"
        }
    assert events == ["app start", "mcp start", "mcp stop", "app stop"]
    with pytest.raises(RuntimeError, match="once"):
        with TestClient(app):
            pass

    @asynccontextmanager
    async def failing_mcp_lifespan():
        raise RuntimeError("MCP server failed to start")
        yield

    events.clear()
    broken = FastAPI(lifespan=application_lifespan)
    install_request_identity(broken)
    install_mcp(broken, _fake_mcp, lifespan=failing_mcp_lifespan)
    with pytest.raises(RuntimeError, match="failed to start"):
        with TestClient(broken):
            pass
    assert events == ["app start", "app stop"]


def test_local_mode_admits_mcp_through_the_same_bearer_validation(monkeypatch):
    monkeypatch.setenv("MAINSEQUENCE_ENDPOINT", ISSUER)

    def users_me(url, **kwargs):
        if kwargs["headers"] == {"Authorization": "Bearer developer-token"}:
            response = Mock(status_code=200)
            response.json.return_value = {"uid": USER, "username": "developer"}
            return response
        # The platform refuses a token issued for another resource, such as an MCP URL.
        return Mock(status_code=401)

    monkeypatch.setattr("mainsequence.server.fastapi.requests.get", users_me)
    app = _install(FastAPI(), _identity_server())

    with TestClient(app) as client:
        developer = client.post(
            "/mcp",
            json=_call("caller"),
            headers={**MCP_HEADERS, "Authorization": "Bearer developer-token"},
        )
        other_resource = client.post(
            "/mcp",
            json=_call("caller"),
            headers={**MCP_HEADERS, "Authorization": "Bearer token-for-a-project-mcp-url"},
        )

    assert _text(developer) == USER
    assert other_resource.status_code == 401
