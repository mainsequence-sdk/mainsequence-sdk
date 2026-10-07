"""An application-owned MCP server served by install_mcp (ADR 0038)."""

import asyncio
import json
from contextlib import asynccontextmanager
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
from mainsequence.client import User
from mainsequence.server.caller_assertions import ASSERTION_HEADER
from mainsequence.server.fastapi import install_mcp, install_request_identity

USER, OTHER, ISSUER = identity_tests.USER, identity_tests.OTHER, identity_tests.ISSUER
clean_environment = identity_tests.clean_environment
signed = identity_tests.signed

MCP_HEADERS = {
    "accept": "application/json, text/event-stream",
    "content-type": "application/json",
    "mcp-protocol-version": "2025-11-25",
}


@pytest.fixture(autouse=True)
def no_cors_origins(monkeypatch):
    monkeypatch.delenv("FASTAPI_CORS_ALLOW_ORIGINS", raising=False)


def _call(name):
    return {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {"name": name, "arguments": {}},
    }


def _text(response):
    assert response.status_code == 200, response.text
    result = response.json()["result"]
    assert result.get("isError") is not True, result
    return result["content"][0]["text"]


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
        return person.uid if person is not None else "none"

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
        ASSERTION_HEADER: token(requester={"sub": OTHER, "team_uids": []}),
    }

    with TestClient(app) as client:
        assert _text(client.post("/mcp", json=_call("caller"), headers=proof)) == USER
        assert _text(client.post("/mcp", json=_call("caller_in_thread"), headers=proof)) == USER
        assert _text(client.post("/mcp", json=_call("requester"), headers=proof)) == "none"
        assert _text(client.post("/mcp", json=_call("requester"), headers=requester_bound)) == OTHER
        assert client.get("/prices", headers={ASSERTION_HEADER: token()}).json() == {"caller": USER}
        missing = client.post("/mcp", json=_call("caller"), headers=MCP_HEADERS)
        forged = client.post(
            "/mcp",
            json=_call("caller"),
            headers={**MCP_HEADERS, ASSERTION_HEADER: token(aud="urn:other:" + str(uuid4()))},
        )

    assert (missing.status_code, forged.status_code) == (401, 401)
    assert "/mcp" not in app.openapi()["paths"]


def test_concurrent_callers_keep_their_own_identity(signed):
    token, _, _ = signed
    server = _server()
    both_inside = asyncio.Barrier(2)

    @server.tool()
    async def caller_after_peer() -> str:
        await asyncio.wait_for(both_inside.wait(), 5)
        return User.get_logged_user().uid

    app = _install(FastAPI(), server)

    async def run():
        async with app.router.lifespan_context(app):
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(
                transport=transport, base_url="http://testserver"
            ) as client:

                async def call(user):
                    headers = {**MCP_HEADERS, ASSERTION_HEADER: token(user)}
                    return _text(
                        await client.post("/mcp", json=_call("caller_after_peer"), headers=headers)
                    )

                return await asyncio.gather(call(USER), call(OTHER))

    assert asyncio.run(run()) == [USER, OTHER]


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
