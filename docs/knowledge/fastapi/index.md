# FastAPI Request User Context

FastAPI applications receive the authenticated human making the current HTTP
request through state injected by the Main Sequence platform. No SDK
authentication setup is required in application code.

Route code does not detect whether it is running locally or deployed, parse
authentication headers, or resolve the user itself. It consumes the same
platform-injected request state in every environment.

In both cases, the result identifies the human making the current request. It
does not identify the deployment owner, release creator, or runtime workload
credential.

## Route Usage

```python
from fastapi import FastAPI, Request

app = FastAPI(title="My API", version="0.1.0")


@app.get("/me")
def get_me(request: Request) -> dict[str, str | None]:
    request_user = request.state.user
    return {
        "uid": request.state.user_uid,
        "username": request_user.username,
    }
```

The Main Sequence platform injects:

- `request.state.user`: a minimal runtime identity containing `uid` and optional
  `username`
- `request.state.user_uid`: the same canonical user UUID as a string

Protected routes use these values identically in local and deployed execution.
They must not inspect authentication headers to select a mode.

There is no `request.state.user_id`. Request identity never uses a numeric
database ID.

The request-user projection describes the human making this request. It is not the
release creator, deployment owner, runtime workload principal, CodeRepositoryBranch,
ResourceRelease, or hostname-selected runtime target. It is intentionally not a
full account profile and has no email, organization, plan, or permission fields.

## Passing Identity To Shared Code

FastAPI route handlers use `request.state.user` or
`request.state.user_uid`. Pass that value explicitly to shared application
services that need the caller. `User.get_logged_user()` is not the FastAPI
entry point, and the SDK no longer installs or exports FastAPI middleware.

Use `User.get_authenticated_user_details()` instead when a standalone CLI,
notebook, or script needs the full user profile associated with its SDK login.
That method calls `/api/v1/users/me/` using the process authentication session;
it does not mean "the human calling this FastAPI endpoint."

## Authorization Boundary

The Main Sequence platform injects authenticated request identity. That
identity alone does not decide whether the user may read, mutate, or administer
a resource.
Every protected endpoint must still perform its resource-level authorization
using `request.state.user_uid` and the authoritative application/backend policy.
