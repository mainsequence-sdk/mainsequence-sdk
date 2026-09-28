# FastAPI requesting-user identity

Install the optional server dependencies in the application's environment:

```bash
pip install 'mainsequence[server]'
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
