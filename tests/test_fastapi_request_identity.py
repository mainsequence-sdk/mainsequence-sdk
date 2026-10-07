import asyncio
import json
import time
from contextvars import copy_context
from unittest.mock import Mock
from uuid import uuid4

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import ed25519
from fastapi import FastAPI, Request, WebSocket
from fastapi.testclient import TestClient

import mainsequence.client.base as base_mod
from mainsequence.client import RequestIdentityError, User
from mainsequence.server.caller_assertions import (
    ASSERTION_HEADER,
    ASSERTION_TYPE,
    CallerAssertionVerifier,
)
from mainsequence.server.fastapi import install_request_identity, reads_as_caller

USER = str(uuid4())
OTHER = str(uuid4())
RELEASE = str(uuid4())
ENVIRONMENT = str(uuid4())
ISSUER = "https://platform.example.test"


@pytest.fixture(autouse=True)
def clean_environment(monkeypatch):
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
    key = ed25519.Ed25519PrivateKey.generate()
    public = jwt.algorithms.OKPAlgorithm.to_jwk(key.public_key(), as_dict=True)
    jwk = {name: public[name] for name in ("kty", "crv", "x")}
    fetch = Mock(return_value={"keys": [{"kid": "test", "alg": "EdDSA", "use": "sig", **jwk}]})
    verifier = CallerAssertionVerifier(
        issuer=ISSUER,
        jwks_url=ISSUER + "/fastapi/caller-keys/",
        release_uid=RELEASE,
        environment_uid=ENVIRONMENT,
        fetch_jwks=fetch,
    )
    monkeypatch.setenv("MAINSEQUENCE_CALLER_AUTH_MODE", "assertion")
    monkeypatch.setattr(CallerAssertionVerifier, "from_environment", lambda: verifier)

    def token(user=USER, *, omit=(), **changes):
        """An assertion as the platform signs it, with the caller's facts; ``omit`` drops claims."""
        now = int(time.time())
        claims = dict(
            iss=ISSUER,
            aud="urn:mainsequence:fapi:" + RELEASE,
            sub=user,
            resource_release_uid=RELEASE,
            organization_environment_uid=ENVIRONMENT,
            iat=now,
            nbf=now,
            exp=now + 120,
            team_uids=[],
            is_organization_admin=False,
        )
        claims.update(changes)
        for name in omit:
            del claims[name]
        return jwt.encode(
            claims, key, algorithm="EdDSA", headers={"kid": "test", "typ": ASSERTION_TYPE}
        )

    return token, fetch, verifier


def app_for(route, method="get", path="/me"):
    app = FastAPI()
    getattr(app, method)(path)(route)
    install_request_identity(app)
    return app


def test_sync_getter_signed_proof_once_and_process_credentials_untouched(signed, monkeypatch):
    token, fetch, verifier = signed
    calls = Mock(wraps=verifier.verify)
    monkeypatch.setattr(verifier, "verify", calls)
    monkeypatch.setenv("MAINSEQUENCE_ACCESS_TOKEN", "workload-session")

    def handler(request: Request):
        user = User.get_logged_user()
        assert User.get_logged_user() is user
        assert request.state.user is user
        return {"uid": user.uid, "username": user.username}

    with TestClient(app_for(handler)) as client:
        response = client.get("/me", headers={ASSERTION_HEADER: token(), "X-User-UID": OTHER})
    assert response.json() == {"uid": USER, "username": None}
    assert calls.call_count == fetch.call_count == 1
    import os

    assert os.environ["MAINSEQUENCE_ACCESS_TOKEN"] == "workload-session"
    with pytest.raises(RequestIdentityError):
        User.get_logged_user()


def test_signed_caller_facts_reach_the_logged_user(signed):
    token, _, _ = signed
    teams = sorted([str(uuid4()), str(uuid4())])

    def handler():
        user = User.get_logged_user()
        return {"teams": list(user.team_uids), "admin": user.is_organization_admin}

    with TestClient(app_for(handler)) as client:
        admin = client.get(
            "/me",
            headers={ASSERTION_HEADER: token(team_uids=teams, is_organization_admin=True)},
        )
        member_of_none = client.get("/me", headers={ASSERTION_HEADER: token()})

    assert admin.json() == {"teams": teams, "admin": True}
    assert member_of_none.json() == {"teams": [], "admin": False}


@pytest.mark.parametrize(
    "case",
    [
        "missing",
        "invalid",
        "expired",
        "wrong_target",
        "duplicate",
        "without_caller_facts",
        "without_team_uids",
        "without_admin_flag",
    ],
)
def test_invalid_proof_never_reaches_route(signed, case, monkeypatch):
    """A refused assertion is final: never the workload's or the developer's account instead."""
    token, _, _ = signed
    monkeypatch.setenv("MAINSEQUENCE_ACCESS_TOKEN", "workload-session")
    monkeypatch.setattr(
        "mainsequence.server.fastapi.requests.get",
        Mock(side_effect=AssertionError("A refused assertion resolved another account.")),
    )

    def handler():
        pytest.fail("Rejected request reached handler")

    headers = {"X-User-UID": USER, "Authorization": "Bearer developer-token"}
    if case == "invalid":
        headers[ASSERTION_HEADER] = "bad"
    elif case == "expired":
        headers[ASSERTION_HEADER] = token(exp=1)
    elif case == "wrong_target":
        headers[ASSERTION_HEADER] = token(resource_release_uid=OTHER)
    elif case == "duplicate":
        headers = [(ASSERTION_HEADER, token()), (ASSERTION_HEADER, token())]
    elif case == "without_caller_facts":
        headers[ASSERTION_HEADER] = token(omit=("team_uids", "is_organization_admin"))
    elif case == "without_team_uids":
        headers[ASSERTION_HEADER] = token(omit=("team_uids",))
    elif case == "without_admin_flag":
        headers[ASSERTION_HEADER] = token(omit=("is_organization_admin",))
    assert TestClient(app_for(handler)).get("/me", headers=headers).status_code == 401


def test_discovery_outage_maps_to_503(signed):
    token, fetch, _ = signed
    fetch.side_effect = RuntimeError("private detail")
    response = TestClient(app_for(lambda: None)).get("/me", headers={ASSERTION_HEADER: token()})
    assert response.status_code == 503
    assert "private" not in response.text


def test_local_bearer_validation_inside_same_getter(monkeypatch):
    monkeypatch.setenv("MAINSEQUENCE_ENDPOINT", ISSUER)
    response = Mock(status_code=200)
    response.json.return_value = {"uid": USER, "username": "local"}
    get = Mock(return_value=response)
    monkeypatch.setattr("mainsequence.server.fastapi.requests.get", get)

    def handler(request: Request):
        assert request.headers.get("authorization") is None
        return {"uid": User.get_logged_user().uid}

    client = TestClient(app_for(handler))
    assert client.get("/me", headers={"X-User-UID": USER}).status_code == 401
    assert client.get("/me", headers={"Authorization": "Bearer local-token"}).json() == {
        "uid": USER
    }
    assert get.call_args.kwargs["headers"] == {"Authorization": "Bearer local-token"}


def test_hosted_configuration_cannot_select_local(monkeypatch):
    monkeypatch.setenv("APP_NAME", RELEASE)
    monkeypatch.setenv("MAINSEQUENCE_CALLER_AUTH_MODE", "local")
    with pytest.raises(RuntimeError, match="mode"):
        install_request_identity(FastAPI())


def test_missing_hosted_configuration_fails_startup(monkeypatch):
    monkeypatch.setenv("APP_NAME", RELEASE)
    with pytest.raises(RuntimeError):
        install_request_identity(FastAPI())


def test_public_and_options_are_explicitly_anonymous(signed, monkeypatch):
    monkeypatch.setenv(
        "FASTAPI_PUBLIC_INGRESS", json.dumps([{"method": "GET", "path": "/callback"}])
    )

    def handler(request: Request):
        with pytest.raises(RequestIdentityError):
            User.get_logged_user()
        assert request.state.user_uid is None
        assert request.headers.get(ASSERTION_HEADER) is None
        return {"anonymous": True}

    app = app_for(handler, path="/callback")
    app.options("/preflight")(handler)
    client = TestClient(app)
    assert client.get("/callback", headers={"X-User-UID": USER}).status_code == 401
    assert (
        client.get(
            "/callback", headers={"X-Public-Ingress": "1", ASSERTION_HEADER: "spoof"}
        ).status_code
        == 200
    )
    assert client.options("/preflight").status_code == 200
    assert client.get("/me", headers={"X-Public-Ingress": "1"}).status_code == 401


def test_websocket_keeps_ticket_header_contract_and_getter(monkeypatch):
    app = FastAPI()

    @app.websocket("/ws")
    async def ws(websocket: WebSocket):
        await websocket.accept()
        assert websocket.headers.get("authorization") is None
        await websocket.send_json({"uid": User.get_logged_user().uid})
        await websocket.close()

    install_request_identity(app)
    with TestClient(app).websocket_connect(
        "/ws", headers={"X-User-UID": USER, "Authorization": "Bearer remove"}
    ) as connection:
        assert connection.receive_json() == {"uid": USER}


def test_duplicate_installation_is_rejected():
    app = FastAPI()
    install_request_identity(app)
    with pytest.raises(RuntimeError, match="already installed"):
        install_request_identity(app)


def test_launcher_declaration_states_the_installed_wiring(signed, monkeypatch):
    """The launcher checks this before serving; a REST-only application declares no MCP."""
    monkeypatch.setenv(
        "FASTAPI_PUBLIC_INGRESS", json.dumps([{"method": "GET", "path": "/callback"}])
    )
    hosted = app_for(lambda: {"public": True}, path="/callback")
    monkeypatch.delenv("FASTAPI_PUBLIC_INGRESS")
    monkeypatch.delenv("MAINSEQUENCE_CALLER_AUTH_MODE")
    local = app_for(lambda: {"local": True})

    assert hosted.state.mainsequence_request_identity == {
        "installed": True,
        "mode": "assertion",
        "public_ingress": (("GET", "/callback"),),
    }
    assert local.state.mainsequence_request_identity == {
        "installed": True,
        "mode": "local",
        "public_ingress": (),
    }
    for app in (hosted, local):
        assert getattr(app.state, "mainsequence_mcp", None) is None
    with TestClient(hosted) as client:
        assert client.get("/callback", headers={"X-Public-Ingress": "1"}).json() == {"public": True}


def test_concurrency_streaming_and_copied_context_cleanup(signed):
    from mainsequence.server.fastapi import _RequestIdentityMiddleware

    token, _, verifier = signed
    observed = []
    copied = []
    entered = 0
    release = asyncio.Event()

    async def application(scope, receive, send):
        nonlocal entered
        current = User.get_logged_user().uid
        copied.append(copy_context())
        entered += 1
        if entered == 2:
            release.set()
        await release.wait()
        assert User.get_logged_user().uid == current
        observed.append(current)
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"first", "more_body": True})
        await asyncio.sleep(0)
        assert User.get_logged_user().uid == current
        await send({"type": "http.response.body", "body": b"last"})

    runtime = _RequestIdentityMiddleware(
        application, mode="assertion", verifier=verifier, public_ingress=(), routes=()
    )

    async def run():
        async def request(user):
            scope = {
                "type": "http",
                "method": "GET",
                "path": "/",
                "headers": [(ASSERTION_HEADER.lower().encode(), token(user).encode())],
            }

            async def receive():
                return {"type": "http.request", "body": b""}

            async def send(message):
                pass

            await runtime(scope, receive, send)

        await asyncio.gather(request(USER), request(OTHER))

    asyncio.run(run())
    assert set(observed) == {USER, OTHER}
    for context in copied:
        with pytest.raises(RequestIdentityError):
            context.run(User.get_logged_user)


@pytest.mark.parametrize("failure", [RuntimeError, asyncio.CancelledError])
def test_exception_and_cancellation_invalidate_context(signed, failure):
    from mainsequence.server.fastapi import _RequestIdentityMiddleware

    token, _, verifier = signed
    copied = []

    async def app(scope, receive, send):
        copied.append(copy_context())
        assert User.get_logged_user().uid == USER
        raise failure()

    runtime = _RequestIdentityMiddleware(
        app, mode="assertion", verifier=verifier, public_ingress=(), routes=()
    )

    async def run():
        async def unused(*args):
            pass

        await runtime(
            {
                "type": "http",
                "method": "GET",
                "path": "/",
                "headers": [(ASSERTION_HEADER.lower().encode(), token().encode())],
            },
            unused,
            unused,
        )

    with pytest.raises(failure):
        asyncio.run(run())
    with pytest.raises(RequestIdentityError):
        copied[0].run(User.get_logged_user)


@pytest.mark.parametrize(
    "headers",
    [
        {},
        {"X-User-UID": "not-a-uuid"},
        {"X-User-UID": USER, "X-User-ID": "42"},
        [("X-User-UID", USER), ("x-user-uid", OTHER)],
    ],
)
@pytest.mark.parametrize("denial_extension", [False, True])
def test_websocket_invalid_identity_cannot_reach_app(headers, denial_extension):
    from mainsequence.server.fastapi import _RequestIdentityMiddleware

    async def application(scope, receive, send):
        pytest.fail("Unauthenticated WebSocket reached the app")

    pairs = headers.items() if isinstance(headers, dict) else headers
    scope = {
        "type": "websocket",
        "path": "/ws",
        "headers": [(k.lower().encode(), v.encode()) for k, v in pairs],
        "extensions": {"websocket.http.response": {}} if denial_extension else {},
    }
    messages = []

    async def run():
        async def receive():
            return {"type": "websocket.connect"}

        async def send(message):
            messages.append(message)

        await _RequestIdentityMiddleware(
            application, mode="local", verifier=None, public_ingress=(), routes=()
        )(scope, receive, send)

    asyncio.run(run())
    assert messages[0]["type"] == (
        "websocket.http.response.start" if denial_extension else "websocket.close"
    )
    if denial_extension:
        assert messages[0]["status"] in {401, 403}
    else:
        assert messages[0]["code"] == 1008
    with pytest.raises(RequestIdentityError):
        User.get_logged_user()


class _Page:
    status_code = 200

    def __init__(self, results=()):
        self._results = list(results)

    def json(self):
        return {"results": self._results, "next": None}


def test_sync_route_reads_as_its_caller(signed, monkeypatch):
    token, _, _ = signed
    proof = token()
    workload = str(uuid4())
    sent = []

    def _fake_make_request(*, s, loaders, r_type, url, payload=None, time_out=None):
        sent.append(payload)
        row = {
            "uid": workload,
            "identity_type": "workload",
            "is_active": True,
            "managed_by_caller": True,
        }
        return _Page([row])

    monkeypatch.setattr(base_mod, "make_request", _fake_make_request)

    def handler(q: str):
        with reads_as_caller():
            rows = User.filter(identity_type="workload", search=q)
        return [{"uid": row.uid, "managed": row.managed_by_caller} for row in rows]

    with TestClient(app_for(handler, path="/candidates")) as client:
        response = client.get(
            "/candidates", params={"q": "prices"}, headers={ASSERTION_HEADER: proof}
        )

    assert response.json() == [{"uid": workload, "managed": True}]
    assert sent == [
        {
            "params": {"identity_type": "workload", "search": "prices"},
            "headers": {ASSERTION_HEADER: proof},
        }
    ]


def test_concurrent_requests_present_their_own_assertions(signed, monkeypatch):
    from mainsequence.server.fastapi import _RequestIdentityMiddleware

    token, _, verifier = signed
    proofs = {USER: token(USER), OTHER: token(OTHER)}
    presented = {USER: [], OTHER: []}
    entered = 0
    release = asyncio.Event()

    def _fake_make_request(*, s, loaders, r_type, url, payload=None, time_out=None):
        presented[User.get_logged_user().uid].append((payload or {}).get("headers"))
        return _Page()

    monkeypatch.setattr(base_mod, "make_request", _fake_make_request)

    async def application(scope, receive, send):
        nonlocal entered
        entered += 1
        if entered == 2:
            release.set()
        await release.wait()
        with reads_as_caller():
            User.filter(search="prices")
            await asyncio.sleep(0)  # the other request reads while this one is inside
            User.filter(search="prices")
        User.filter(search="prices")
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    runtime = _RequestIdentityMiddleware(
        application, mode="assertion", verifier=verifier, public_ingress=(), routes=()
    )

    async def run():
        async def request(user):
            scope = {
                "type": "http",
                "method": "GET",
                "path": "/",
                "headers": [(ASSERTION_HEADER.lower().encode(), proofs[user].encode())],
            }

            async def receive():
                return {"type": "http.request", "body": b""}

            async def send(message):
                pass

            await runtime(scope, receive, send)

        await asyncio.gather(request(USER), request(OTHER))

    asyncio.run(run())
    for user in (USER, OTHER):
        assert presented[user] == [{ASSERTION_HEADER: proofs[user]}] * 2 + [None]


def test_reads_as_caller_needs_a_signed_http_request(monkeypatch):
    monkeypatch.setenv("MAINSEQUENCE_ENDPOINT", ISSUER)
    response = Mock(status_code=200)
    response.json.return_value = {"uid": USER, "username": "local"}
    monkeypatch.setattr("mainsequence.server.fastapi.requests.get", Mock(return_value=response))

    def handler():
        with pytest.raises(RequestIdentityError, match="no caller assertion"):
            with reads_as_caller():
                pass
        return {"uid": User.get_logged_user().uid}

    async def ws(websocket: WebSocket):
        await websocket.accept()
        with pytest.raises(RequestIdentityError, match="no caller assertion"):
            with reads_as_caller():
                pass
        await websocket.send_json({"uid": User.get_logged_user().uid})
        await websocket.close()

    app = FastAPI()
    app.get("/me")(handler)
    app.websocket("/ws")(ws)
    install_request_identity(app)
    client = TestClient(app)

    local = client.get("/me", headers={"Authorization": "Bearer local-token"})
    assert local.json() == {"uid": USER}
    with client.websocket_connect("/ws", headers={"X-User-UID": USER}) as connection:
        assert connection.receive_json() == {"uid": USER}
    with pytest.raises(RequestIdentityError, match="authenticated request"):
        with reads_as_caller():
            pass


# --- Requester-bound calls (ADR-0036) ----------------------------------------------
#
# Another application calls while it works for a person, the requester. The caller
# stays that application; User.get_requester() returns the person the platform
# signed into the assertion, with the person's own facts.

PERSON = "5b0f9a8e-3c2d-4e1f-9a7b-6c5d4e3f2a1b"


def requester_claim(sub=PERSON, team_uids=(), is_organization_admin=False):
    """The requester claim as the platform signs it."""
    return {
        "sub": sub,
        "team_uids": list(team_uids),
        "is_organization_admin": is_organization_admin,
    }


def _identities():
    caller = User.get_logged_user()
    requester = User.get_requester()
    return {
        "caller": [caller.uid, list(caller.team_uids), caller.is_organization_admin],
        "requester": (
            None
            if requester is None
            else [requester.uid, list(requester.team_uids), requester.is_organization_admin]
        ),
    }


@pytest.mark.parametrize(
    ("caller_admin", "person_admin"),
    [(False, True), (True, False)],
    ids=["admin_person", "admin_caller"],
)
def test_requester_bound_call_binds_the_requester_beside_the_caller(
    signed, caller_admin, person_admin
):
    token, _, _ = signed
    caller_teams = sorted([str(uuid4()), str(uuid4())])
    requester_teams = sorted([str(uuid4()), str(uuid4())])
    # Each identity keeps its own facts: neither admin flag reaches the other.
    facts = dict(team_uids=caller_teams, is_organization_admin=caller_admin)
    person = requester_claim(team_uids=requester_teams, is_organization_admin=person_admin)

    with TestClient(app_for(_identities)) as client:
        bound = client.get("/me", headers={ASSERTION_HEADER: token(**facts, requester=person)})
        # A person UID in a header or the query names no requester.
        direct = client.get(
            "/me",
            params={"requester": PERSON},
            headers={ASSERTION_HEADER: token(**facts), "X-User-UID": PERSON},
        )

    assert bound.json() == {
        "caller": [USER, caller_teams, caller_admin],
        "requester": [PERSON, requester_teams, person_admin],
    }
    assert direct.json() == {"caller": [USER, caller_teams, caller_admin], "requester": None}
    with pytest.raises(RequestIdentityError, match=r"User\.get_requester\(\)"):
        User.get_requester()


@pytest.mark.parametrize(
    ("requester", "omit"),
    [
        (None, ()),
        ({"sub": PERSON, "team_uids": []}, ()),
        ({"sub": PERSON, "is_organization_admin": True}, ()),
        ({**requester_claim(), "username": "person"}, ()),
        (requester_claim(is_organization_admin=1), ()),
        (requester_claim(is_organization_admin="true"), ()),
        (requester_claim(is_organization_admin=None), ()),
        (requester_claim(sub=PERSON.upper()), ()),
        (requester_claim(), ("team_uids", "is_organization_admin")),
    ],
    ids=[
        "null",
        "two_field_without_admin_flag",
        "missing_team_uids",
        "extra_key",
        "integer_admin_flag",
        "string_admin_flag",
        "null_admin_flag",
        "non_canonical_sub",
        "without_caller_facts",
    ],
)
def test_malformed_requester_never_reaches_route(signed, requester, omit):
    token, _, _ = signed

    def handler():
        pytest.fail("Rejected request reached handler")

    response = TestClient(app_for(handler)).get(
        "/me", headers={ASSERTION_HEADER: token(requester=requester, omit=omit)}
    )
    assert response.status_code == 401


def test_requester_is_none_in_local_mode_and_on_websockets(monkeypatch):
    monkeypatch.setenv("MAINSEQUENCE_ENDPOINT", ISSUER)
    response = Mock(status_code=200)
    response.json.return_value = {"uid": USER, "username": "local"}
    monkeypatch.setattr("mainsequence.server.fastapi.requests.get", Mock(return_value=response))

    async def ws(websocket: WebSocket):
        await websocket.accept()
        await websocket.send_json(_identities())
        await websocket.close()

    app = FastAPI()
    app.get("/me")(_identities)
    app.websocket("/ws")(ws)
    install_request_identity(app)
    client = TestClient(app)

    expected = {"caller": [USER, [], False], "requester": None}
    assert client.get("/me", headers={"Authorization": "Bearer local-token"}).json() == expected
    with client.websocket_connect("/ws", headers={"X-User-UID": USER}) as connection:
        assert connection.receive_json() == expected


def test_public_route_has_no_requester(signed, monkeypatch):
    token, _, _ = signed
    monkeypatch.setenv(
        "FASTAPI_PUBLIC_INGRESS", json.dumps([{"method": "GET", "path": "/callback"}])
    )

    def handler():
        with pytest.raises(RequestIdentityError):
            User.get_requester()
        return {"anonymous": True}

    response = TestClient(app_for(handler, path="/callback")).get(
        "/callback",
        headers={
            "X-Public-Ingress": "1",
            ASSERTION_HEADER: token(requester=requester_claim()),
        },
    )
    assert response.json() == {"anonymous": True}


def test_requester_expires_with_its_request(signed):
    from mainsequence.server.fastapi import _RequestIdentityMiddleware

    token, _, verifier = signed
    copied = []

    async def application(scope, receive, send):
        assert User.get_requester().uid == PERSON
        copied.append(copy_context())
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    runtime = _RequestIdentityMiddleware(
        application, mode="assertion", verifier=verifier, public_ingress=(), routes=()
    )
    proof = token(requester=requester_claim())

    async def run():
        async def receive():
            return {"type": "http.request", "body": b""}

        async def send(message):
            pass

        await runtime(
            {
                "type": "http",
                "method": "GET",
                "path": "/",
                "headers": [(ASSERTION_HEADER.lower().encode(), proof.encode())],
            },
            receive,
            send,
        )

    asyncio.run(run())
    assert len(copied) == 1
    with pytest.raises(RequestIdentityError):
        copied[0].run(User.get_requester)


def test_reads_as_caller_refuses_a_requester_bound_request(signed, monkeypatch):
    token, _, _ = signed
    sent = []
    monkeypatch.setattr(base_mod, "make_request", lambda **kwargs: sent.append(kwargs))

    def handler():
        with pytest.raises(RequestIdentityError, match="requester-bound"):
            with reads_as_caller():
                User.filter(search="prices")
        return _identities()

    # Unchanged by the person's admin flag: the helper still forwards nothing.
    person = requester_claim(is_organization_admin=True)
    with TestClient(app_for(handler)) as client:
        response = client.get("/me", headers={ASSERTION_HEADER: token(requester=person)})

    assert response.json() == {"caller": [USER, [], False], "requester": [PERSON, [], True]}
    assert sent == []
