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

For a signed HTTP request the result also carries what the platform states about the caller: `team_uids`, the canonical UIDs of the caller's active teams, and `is_organization_admin`, whether the caller is an admin of the application's Organization. An application can evaluate its own policies from them without calling the platform; workload callers are covered through the teams they belong to. Local development and WebSocket requests carry no such facts, so `team_uids` is empty and `is_organization_admin` is false, as for an assertion from a platform that does not send them yet.

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
            {"uid": workload.uid, "managed": workload.managed_by_caller}
            for workload in workloads
        ],
    }
```

Inside the block, `User.filter`, `User.get_by_uid`, `Team.filter` and `Team.get_by_uid` present the caller assertion the request arrived with, and the platform answers with what the caller may see. `search` matches a person's email or name, or a workload's Job, release or Agent name. A workload row carries `managed_by_caller`, true when the caller manages that workload. No other call presents the assertion, and nothing presents it outside the block.

The block needs a signed HTTP request. Outside an authenticated request, in local mode and on WebSockets it raises `RequestIdentityError`; it never reads as the application instead. In local mode the SDK session already belongs to the developer, so reads outside the block answer as that person. The assertion lives at most five minutes and is not renewed, so a read after it expires fails. See [ADR-0036](../../adr/0036-request-scoped-logged-user.md#reads-as-the-caller).

## Platform and SDK responsibilities

Django authenticates and signs. The gateway forwards the proof. The application's SDK integration verifies it using deployment-owned trust configuration. PodDeploymentOrchestrator validates that the integration is installed and serves the app without importing or depending on the SDK.

The existing `request.state.user` and `request.state.user_uid` fields are compatibility projections of the same identity. New code uses the getter. There is one resolver and no UID-header fallback for hosted HTTP.

`User.get_authenticated_user_details()` identifies the account associated with the process's SDK authentication. Its workload credential is unchanged by incoming callers and is never a fallback caller.

## Local development

Without hosted deployment markers, the integration uses a direct incoming Bearer token and validates it against `MAINSEQUENCE_ENDPOINT/api/v1/users/me/`. The handler still calls the same getter. Local mode cannot override a deployed target or replace a failed assertion. Missing local identity returns 401; an unavailable authenticator returns 503.

## Public ingress, preflight and WebSockets

Exact public method/path pairs require the backend-owned `FASTAPI_PUBLIC_INGRESS` declaration and the gateway's matching admission marker. Anonymous requests have no logged user, and credential/identity headers are stripped. Unlisted routes and methods require authentication. Applications still validate provider callback state or webhook signatures.

OPTIONS receives an anonymous context. Platform health is handled by the launcher's external health wrapper. WebSockets preserve the existing gateway ticket/header contract, use the same getter while the connection is active, and reset identity on disconnect; HTTP caller assertions do not replace their transport contract.

## Migration

Existing application factories must add the installer before adopting the corresponding launcher. Replace direct verifier calls, private ContextVar manipulation, or caller lookups through process authentication with `User.get_logged_user()`. The launcher rejects missing/mismatched integration at startup. Keep Django signing, key discovery and gateway forwarding available before deploying proof-required applications.

The identity answers who called. Application resource policy stays in the application. See [server verification](../server/caller_assertions.md) and [ADR-0036](../../adr/0036-request-scoped-logged-user.md).
