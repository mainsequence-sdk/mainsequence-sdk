# FastAPI Request User Context

FastAPI applications that need the authenticated human making the current HTTP
request must install `LoggedUserContextMiddleware` at application startup.
Middleware installation remains application-owned; the deployment orchestrator
does not add it automatically.

Install the FastAPI extra when the application does not already depend on
FastAPI:

```bash
uv add 'mainsequence[fastapi]'
```

## Application Setup

```python
from fastapi import FastAPI, Request

from mainsequence.client.fastapi import LoggedUserContextMiddleware


app = FastAPI(title="My API", version="0.1.0")
app.add_middleware(LoggedUserContextMiddleware)


@app.get("/me")
def get_me(request: Request) -> dict[str, str | None]:
    request_user = request.state.user
    return {
        "uid": request.state.user_uid,
        "username": request_user.username,
    }
```

After successful resolution, the middleware exposes:

- `request.state.user`: a `RequestUserIdentity` containing `uid` and optional
  `username`
- `request.state.user_uid`: the same canonical user UUID as a string

There is no `request.state.user_id`. Request identity never uses a numeric
database ID.

`RequestUserIdentity` describes the human making this request. It is not the
release creator, deployment owner, runtime workload principal, ProjectBranch,
ResourceRelease, or hostname-selected runtime target. It is intentionally not a
full account profile and has no email, organization, plan, or permission fields.

## Using The Helper

Route handlers should use `request.state.user` when they already receive a
`Request`. Shared code called inside that request may use:

```python
from mainsequence.client import User


def current_request_user_uid() -> str:
    return User.get_logged_user().uid
```

`User.get_logged_user()` returns `RequestUserIdentity`, not `User`. The
middleware binds and clears that identity for each request, including failures
and concurrent requests.

Use `User.get_authenticated_user_details()` instead when a standalone CLI,
notebook, or script needs the full user profile associated with its SDK login.
That method calls `/api/v1/users/me/` using the process authentication session;
it does not mean "the human calling this FastAPI endpoint."

## Resolution Modes

In a deployed runtime, the authenticated platform gateway supplies a trusted
`X-User-UID` and optional `X-Username`. The caller's bearer credential is
removed before the request reaches application code. The SDK validates the UID
locally and does not make a redundant user lookup.

In direct local development, the request carries
`Authorization: Bearer <token>` and no gateway user header. The SDK validates
that token through `/api/v1/users/me/` and forwards only the Authorization
header. Cookies, host headers, and caller-provided identity headers are not sent
to the backend lookup. This mode supports testing a local API through a
Cloudflare tunnel before deploying it.

When both a bearer token and `X-User-UID` are present, the SDK validates the
token and requires both UIDs to match. The removed `X-User-ID` header is rejected
in every mode. `X-User-Email` is ignored because it is not authenticated request
identity metadata.

Missing, malformed, mismatched, or backend-rejected identity returns HTTP 401
before the route handler runs. Unauthenticated `OPTIONS` requests pass through
so application CORS middleware can answer browser preflight requests.

## Authorization Boundary

`LoggedUserContextMiddleware` authenticates and exposes request identity. It
does not decide whether that user may read, mutate, or administer a resource.
Every protected endpoint must still perform its resource-level authorization
using `request.state.user_uid` and the authoritative application/backend policy.

Do not install this middleware on an endpoint that intentionally supports
anonymous callers unless the application separates that route into an ASGI
surface without the middleware.
