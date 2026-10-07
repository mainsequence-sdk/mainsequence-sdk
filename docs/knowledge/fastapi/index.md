# FastAPI requesting-user identity

Install the SDK in the application's environment. Caller verification dependencies
are included in the standard installation:

```bash
pip install mainsequence
```

Install request identity once when creating the application:

```python
from fastapi import FastAPI
from mainsequence.client import User
from mainsequence.server.fastapi import install_request_identity

app = FastAPI()
install_request_identity(app)

@app.get("/me")
def me():
    user = User.get_logged_user()
    return {"uid": user.uid}
```

`User.get_logged_user()` is the single application caller lookup. The integration verifies the caller before the handler, binds one isolated request scope, and resets it on completion, exceptions, or cancellation. Both synchronous and asynchronous handlers and their service functions use the same getter. It performs no extra lookup each time.

The result contains a canonical User `uid` and optional `username`. Signed HTTP proofs contain no username, so it is null. Outside an authenticated request, including public routes, the getter raises `RequestIdentityError`. A copied task context cannot retain the user after its owning request ends.

For a signed HTTP request the result also carries what the platform states about the caller: `team_uids`, the canonical UIDs of the caller's active teams, and `is_organization_admin`, whether the caller is an admin of the application's Organization. An application can evaluate its own policies from them without calling the platform; workload callers are covered through the teams they belong to. The platform signs both into every assertion, and an assertion without them is rejected with 401. Local development and WebSocket requests carry no such facts, so `team_uids` is empty and `is_organization_admin` is false.

```python
from mainsequence.client import User


def can_publish(team_uid: str) -> bool:
    caller = User.get_logged_user()
    return caller.is_organization_admin or team_uid in caller.team_uids
```

SDK releases before these facts reject every assertion that carries them, so hosted applications on an older SDK must be rebuilt on a current one.

The caller is a person or a workload identity, the User a deployed Job, FastAPI release or Agent runs as. To tell which, read the caller's User with `User.get_by_uid(caller.uid)`: a workload identity has `identity_type` `"workload"` and the UID of its workload in `job_uid`, `resource_release_uid` or `agent_uid`. See [Workload identities](../infrastructure/users_and_access.md#workload-identities).

## Reading the directory as the caller

The application calls the platform as its own workload identity, which usually has no teams and no grants. To show its caller the people, Teams and workloads that caller can see, for example to share one of the application's objects, read the directory as the caller:

```python
from mainsequence.client import User
from mainsequence.server.fastapi import reads_as_caller


@app.get("/share-candidates")
def share_candidates(q: str):
    with reads_as_caller():
        people = User.filter(search=q)
        workloads = User.filter(identity_type="workload", search=q)
    return {
        "people": [person.uid for person in people],
        "workloads": [
            {
                "uid": workload.uid,
                "name": workload.workload_name,
                "managed": workload.managed_by_caller,
            }
            for workload in workloads
        ],
    }
```

Inside the block, `User.filter`, `User.get_by_uid`, `Team.filter` and `Team.get_by_uid` present the caller assertion the request arrived with, and the platform answers with what the caller may see. `search` matches a person's email or name, or a workload's Job, release or Agent name. A workload row carries `managed_by_caller`, true when the caller manages that workload. No other call presents the assertion, and nothing presents it outside the block.

The block needs a signed HTTP request. Outside an authenticated request, in local mode and on WebSockets it raises `RequestIdentityError`; it never reads as the application instead. In local mode the SDK session already belongs to the developer, so reads outside the block answer as that person. The assertion lives at most five minutes and is not renewed, so a read after it expires fails. See [ADR-0036](../../adr/0036-request-scoped-logged-user.md#reads-as-the-caller).

## Requester-bound calls

Another application, for example an Agent, can call this application while it works for a person: the **requester**. The platform then signs the requester into the caller assertion next to the caller. `User.get_logged_user()` still returns the caller, the acting application. `User.get_requester()` returns the requester, the checked delegation, or `None` when the request carries no delegation.

The requester is a `RequestUserIdentity` with the person's own `uid`, `team_uids` and `is_organization_admin`, as the platform signed them. A requester-bound call carries the person's ordinary permissions, including administrative ones. The SDK verifies the delegation before any handler runs: an invalid one rejects the request with 401 and never falls back to the caller. `get_requester()` returns `None` in local mode, on WebSockets and when the assertion names no requester. Outside an authenticated request, including public routes, it raises `RequestIdentityError`, as `get_logged_user()` does.

Each handler decides whether it needs a person. One that needs a person fails with its own error when there is none:

```python
from fastapi import HTTPException
from mainsequence.client import User


def team_report(team_uid: str) -> dict:
    requester = User.get_requester()
    if requester is None:
        # This operation answers for a person: without a requester, refuse.
        raise HTTPException(status_code=403, detail="This operation needs a requester.")
    if not (requester.is_organization_admin or team_uid in requester.team_uids):
        raise HTTPException(status_code=403, detail="The requester cannot read this report.")
    return {"team_uid": team_uid, "requested_by": requester.uid}
```

One where the person is optional authorizes against the person when present, and against the caller otherwise:

```python
def can_publish(team_uid: str) -> bool:
    actor = User.get_requester() or User.get_logged_user()
    return actor.is_organization_admin or team_uid in actor.team_uids
```

- Authorize against one identity: the person on a requester-bound call, the caller on any other. Never combine the caller's grants with the person's, and never fall back to what the acting application may do when the person may not.
- For a write, check that identity's permission for the exact object and action before mutating anything. A read grant or team membership alone does not establish edit, run, share or delete permission. The SDK verifies identity; the application owns these checks.
- Never parse the assertion, headers or MCP `_meta` yourself, and never trust a person's UID taken from a request body, header or query parameter. Only the signed assertion names the requester, and the SDK has verified that it is addressed to this release.
- Inside a requester-bound request, `reads_as_caller()` raises `RequestIdentityError`: the receiving application cannot forward a requester-bearing assertion to the platform's directory endpoints. Platform-authorized Agent delegation is a separate platform-owned path; this helper cannot create a binding or forward write authority.

The platform checks access on every call and limits requester-bound work to at most 24 hours after the person's request, including across Agent delegation. A model-driven Agent can be steered by prompt injection, so write tools need explicit operation-specific checks. The SDK does not select the person, enable platform endpoints or extend the lifetime of an inbound request scope.

SDK releases up to 9.0.17 reject the requester claim the platform now signs, with its `is_organization_admin`, so requester-bound calls to an application on such a release fail with 401 while its other calls are unchanged. This release rejects the earlier two-field requester and any assertion without the caller's `team_uids` and `is_organization_admin`, so it works only with the updated platform. Which applications may act for their requester, and what that access covers, is described in [Applications that act for their requester](../infrastructure/users_and_access.md#applications-that-act-for-their-requester). See [ADR-0036](../../adr/0036-request-scoped-logged-user.md#requester-bound-calls).

## Serving MCP

An application can serve its own MCP server at `/mcp`, next to its HTTP API. Install the `mcp` extra, `pip install "mainsequence[mcp]"`, and pass the server's ASGI app and lifespan to `install_mcp` after `install_request_identity`:

```python
from fastapi import FastAPI
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings

from mainsequence.client import User
from mainsequence.server.fastapi import install_mcp, install_request_identity

mcp = FastMCP(
    "prices",
    stateless_http=True,
    json_response=True,
    transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False),
)


@mcp.tool()
async def latest_price(symbol: str) -> float:
    caller = User.get_logged_user()
    ...  # check what this caller may read, then answer


app = FastAPI()
install_request_identity(app)
install_mcp(app, mcp.streamable_http_app(), lifespan=mcp.session_manager.run)
```

Tools read the caller with `User.get_logged_user()` and, on a requester-bound call, the person it works for with `User.get_requester()`, as REST handlers do. Admission to the release does not authorize every tool: each tool checks what its caller may do.

Each tool decides whether it needs a person, as in
[Requester-bound calls](#requester-bound-calls). A tool that needs one fails
with its own error when `User.get_requester()` is `None`. A tool where the
person is optional authorizes against `User.get_requester()` when present, and
against `User.get_logged_user()` otherwise. A write tool checks that identity's
permission for the target object and action before calling the mutating
service; on a requester-bound call it can neither use the calling Agent's
grants instead of the person's nor add them to the person's. Tools never read
the assertion, headers or MCP `_meta` to find a person. MCP tool annotations, a
successful connection and permission to read the same object do not authorize
writes. Keep any required mutation approval in the application's policy, not in
a model's claim that the user approved it.

The integration:

- serves exactly `/mcp` inside the request identity: only authenticated POSTs reach the server, and a request without a valid caller answers 401 as on any route;
- keeps each call's identity in the tasks its tool starts and in the worker threads it uses through `asyncio.to_thread` or anyio's `to_thread.run_sync`, and ends it with the call, by completion, failure or cancellation; a thread that receives no copy of the context, such as one from `loop.run_in_executor`, has no identity;
- serves statelessly: other methods answer 405, and an `Mcp-Session-Id` header is removed, so no request reuses another request's server state;
- answers 403 to a request whose `Origin` is neither the release's public origin nor one of its CORS origins; clients that send no `Origin`, such as Codex, are unaffected;
- starts the MCP lifespan once, after the application's, and stops it first; a failed MCP start unwinds the application's and fails startup;
- refuses a second installation, an existing route under `/mcp` and a public-ingress entry for `/mcp`.

Configure the server as above: stateless, JSON responses, and FastMCP's DNS-rebinding protection off, because its default admits only localhost hosts while the integration checks `Origin` and the platform routes the host. Keep its default path, `/mcp`.

The release advertises its endpoint once its workflow declares `spec.mcp_enabled: true` (workflow API `2.3.0`) and a deployment with it succeeds. Clients connect to that URL and sign in with the platform; the application receives the caller assertion, never the client's token. `ResourceRelease.mcp_connection` gives the URL, and `ResourceRelease.filter(mcp_available=True)` lists releases that advertise one. See [ADR 0038](../../adr/0038-project-mcp-on-fastapi-releases.md).

## Platform and SDK responsibilities

Django authenticates and signs. The gateway forwards the proof. The application's SDK integration verifies it using deployment-owned trust configuration. PodDeploymentOrchestrator validates that the integration is installed and serves the app without importing or depending on the SDK.

The existing `request.state.user` and `request.state.user_uid` fields are compatibility projections of the same identity. New code uses the getter. There is one resolver and no UID-header fallback for hosted HTTP.

`User.get_authenticated_user_details()` identifies the account associated with the process's SDK authentication. Its workload credential is unchanged by incoming callers and is never a fallback caller.

## Local development

Without hosted deployment markers, the integration uses a direct incoming Bearer token and validates it against `MAINSEQUENCE_ENDPOINT/api/v1/users/me/`. The handler still calls the same getter. Local mode cannot override a deployed target or replace a failed assertion. Missing local identity returns 401; an unavailable authenticator returns 503.

MCP behaves the same way locally. `/mcp` takes the same Bearer token as your routes, and there is no sign-in challenge locally, so configure the client, for example Codex, to send your platform token as a fixed `Authorization: Bearer` header.

The `Origin` check reads `FASTAPI_PUBLIC_BASE_URL` and `FASTAPI_CORS_ALLOW_ORIGINS` once, when `install_mcp` runs. In a deployment the platform sets both: the release's URL and its allowed origins. Locally they are usually unset, so the list is empty. A browser, which always sends `Origin`, then gets 403, while Codex, curl and scripts, which send none, are unaffected. To try a browser-based MCP client locally, allow its origin in the environment the app starts with:

```bash
export FASTAPI_CORS_ALLOW_ORIGINS=http://localhost:3000
```

## Public ingress, preflight and WebSockets

Exact public method/path pairs require the backend-owned `FASTAPI_PUBLIC_INGRESS` declaration and the gateway's matching admission marker. Anonymous requests have no logged user, and credential/identity headers are stripped. Unlisted routes and methods require authentication. Applications still validate provider callback state or webhook signatures.

OPTIONS receives an anonymous context. Platform health is handled by the launcher's external health wrapper. WebSockets preserve the existing gateway ticket/header contract, use the same getter while the connection is active, and reset identity on disconnect; HTTP caller assertions do not replace their transport contract.

## Migration

Existing application factories must add the installer before adopting the corresponding launcher. Replace direct verifier calls, private ContextVar manipulation, or caller lookups through process authentication with `User.get_logged_user()`. The launcher rejects missing/mismatched integration at startup. Keep Django signing, key discovery and gateway forwarding available before deploying proof-required applications.

The identity answers who called. Application resource policy stays in the application. See [server verification](../server/caller_assertions.md) and [ADR-0036](../../adr/0036-request-scoped-logged-user.md).
